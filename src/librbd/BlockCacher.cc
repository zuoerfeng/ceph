// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2015 Haomai Wang <haomaiwang@gmail.com>
 *
 * LGPL2.1 (see COPYING-LGPL2.1) or later
 */

#include <stdlib.h>
#include <string.h>
#include <vector>
#include "include/buffer.h"
#include "common/dout.h"
#include "common/errno.h"
#include "librbd/ImageCtx.h"
#include "BlockCacher.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "BlockCacher: "

namespace librbd {

const string BlockCacher::name = "BlockCacher::BlockCacher";

void BlockCacherCompletion::complete_request(int r)
{
  lock.lock();
  if (rval >= 0) {
    if (r < 0 && r != -EEXIST)
      rval = r;
    else if (r > 0)
      rval += r;
  }
  lock.unlock();
  if (!count.dec()) {
    ctxt->complete(rval);
    delete this;
  }
}

void C_AioRead2::finish(int r) {
  m_completion->rval = r;
  m_completion->complete();
  m_completion->put_unlock();
}

void CARState::adjust_lru_limit(Page *cur_page, int hit_ghost_history) {
  Mutex::Locker l(lock);
  if (hit_ghost_history == ARC_LRU) {
    /* cache directory hit */
    arc_lru_limit = MIN(arc_lru_limit + arc_list_size[ARC_LRU_GHOST]/arc_list_size[ARC_LFU_GHOST], data_pages);
    cur_page->arc_idx = ARC_LFU;
  } else if (hit_ghost_history == ARC_LFU) {
    /* cache directory hit */
    uint32_t difference = arc_list_size[ARC_LRU_GHOST]/arc_list_size[ARC_LFU_GHOST];
    arc_lru_limit = arc_lru_limit > difference ? arc_lru_limit - difference : 0;
    cur_page->arc_idx = ARC_LFU;
  } else {
    /* cache directory miss */
    cur_page->arc_idx = ARC_LRU;
  }
    ldout(cct, 10) << __func__ << " adjust new lru limit to " << arc_lru_limit << dendl;
}

int BlockCacher::reg_region(uint64_t size)
{
  assert(tree_lock.is_locked());
  assert(size >= CEPH_PAGE_SIZE);
  ldout(cct, 10) << __func__ << " size=" << size << dendl;
  Region region;
  int r = ::posix_memalign(&region.addr, CEPH_PAGE_SIZE, size);
  if (r < 0) {
    lderr(cct) << __func__ << " failed to alloc memory(" << size << "): "
        << cpp_strerror(errno) << dendl;
  } else {
    region.length = size;
    Page *p;
    char *data = static_cast<char*>(region.addr);
    for (uint32_t i = 0; i < region_maxpages; ++i) {
      p = free_pages_head;
      free_pages_head = free_pages_head->arc_next;
      p->addr = data;
      p->arc_next = free_data_pages_head;
      free_data_pages_head = p;
      data += page_length;
    }
  }
  remain_data_pages -= size;
  regions.push_back(region);
  ldout(cct, 10) << __func__ << " r=" << r << dendl;
  return r;
}

// https://dl.dropboxusercontent.com/u/91714474/Papers/clockfast.pdf
int BlockCacher::get_pages(PageRBTree *tree, PageRBTree *ghost_tree, Page **pages, bool hit[],
                           size_t num_pages, size_t align_offset)
{
  ldout(cct, 10) << __func__ << " " << num_pages << " pages, align_offset=" << align_offset << dendl;

  while (num_pages >= total_pages / 2 - inflight_pages) {
    ldout(cct, 0) << __func__ << " can't provide with enough pages" << dendl;
    inflight_page_wait = true;
    tree_cond.Wait(tree_lock);
  }

  Page *cur_page = NULL;
  uint64_t end_offset = align_offset + num_pages * page_length;
  size_t idx = 0;
  RBTree::Iterator end = tree->end();
  memset(hit, 0, sizeof(bool)*num_pages);
  for (RBTree::Iterator ictx_it = tree->lower_bound(align_offset);
       ictx_it != end; ++ictx_it) {
    cur_page = ictx_it->get_container<Page>(offsetof(Page, rb));
    while (cur_page->inflight) {
      ldout(cct, 1) << __func__ << " " << *cur_page << " is inflight, queue me" << dendl;
      inflight_page_wait = true;
      tree_cond.Wait(tree_lock);
    }
    if (cur_page->offset < end_offset) {
      car_state.hit_page(cur_page);
      idx = (cur_page->offset-align_offset)/page_length;
      pages[idx] = cur_page;
      hit[idx] = true;
      ldout(cct, 20) << __func__ << " hit cache page " << *cur_page << dendl;
    } else {
      break;
    }
  }

  RBTree::Iterator ghost_it = ghost_tree->lower_bound(align_offset);
  Page *ghost_page = ghost_it != end ? ghost_it->get_container<Page>(offsetof(Page, rb)) : NULL;
  int hit_ghost_history;  // 0 is hit LRU_GHOST, 1 is hit LFU_GHOST, 2 is not hit
  idx = 0;
  for (size_t pos = align_offset; idx < num_pages; pos += page_length, idx++) {
    if (hit[idx])
      continue;

    // cache miss
    hit[idx] = false;
    hit_ghost_history = 2;
    if (ghost_it != end) {
      if (ghost_page->offset == pos) {
        ldout(cct, 20) << __func__ << " hit history " << *ghost_page << dendl;
        hit_ghost_history = ghost_page->arc_idx;
        ++ghost_it;
      }
      assert(ghost_page->offset > pos);
    }

    if (free_data_pages_head) {
      if (!remain_data_pages) {
        // Cache is full
        cur_page = car_state.get_ghost_page(hit_ghost_history != 2 ? ghost_page : NULL);
        ldout(cct, 20) << __func__ << " ghost_page=" << cur_page << dendl;
        if (cur_page) {
          ghost_tree->erase(cur_page);
        } else {
          cur_page = free_pages_head;
          free_pages_head = free_pages_head->arc_next;
        }
        /* cache full, got a page data from cache */
        ghost_tree->insert(car_state.evict_data(cur_page));
        assert(cur_page && cur_page->addr);
        tree->erase(cur_page);
      } else {
        uint32_t rps = MIN(remain_data_pages, region_maxpages);
        ldout(cct, 20) << __func__ << " no free data page, try to alloc a page region("
            << rps << " pages)" << dendl;
        assert(reg_region(rps) == 0);
        cur_page = free_data_pages_head;
        free_data_pages_head = free_data_pages_head->arc_next;
      }
    } else {
      cur_page = free_data_pages_head;
      free_data_pages_head = free_data_pages_head->arc_next;
    }

    assert(cur_page->addr);
    car_state.adjust_lru_limit(cur_page, hit_ghost_history);
    tree->insert(cur_page);

    cur_page->offset = pos;
    pages[idx] = cur_page;
  }

  return 0;
}

void BlockCacher::complete_read(C_BlockCacheRead *bc_read_comp, int r)
{
  ldout(cct, 20) << __func__ << " r=" << r << dendl;

  if (r < 0 && r != -ENOENT) {
    ldout(cct, 1) << __func__ << " got r=" << r << " for ctxt=" << bc_read_comp->comp
                  << " oustanding reads=" << bc_read_comp->comp << dendl;
  } else { // this was a sparse_read operation
    // reads from the parent don't populate the m_ext_map and the overlap
    // may not be the full buffer.  compensate here by filling in m_ext_map
    // with the read extent when it is empty.
    AioRead *req = bc_read_comp->req;
    if (req->m_ext_map.empty())
      req->m_ext_map[req->get_object_off()] = req->data().length();

    vector<Page*>::iterator page_it = bc_read_comp->extent.page_extents.begin(), page_end;
    size_t num_pages = bc_read_comp->extent.page_extents.size(), i = 0;
    Page *page = bc_read_comp->extent.page_extents[0];
    bufferlist::iterator bliter = req->data().begin();
    uint64_t page_left = page_length;
    uint64_t page_int_offset = 0;
    uint64_t copy_size, tlen, padding;
    bool is_zero[num_pages];
    for (map<uint64_t, uint64_t>::iterator ext_it = req->m_ext_map.begin();
         ext_it != req->m_ext_map.end(); ++ext_it) {
      ldout(cct, 20) << __func__ << " ext_it = (" << ext_it->first << ", " << ext_it->second
                     << ") page left offset " << page_int_offset << dendl;
      // |-----------------<left ext_it>----------|
      // |---------<page>-------------------------|
      while (ext_it->first >= page->offset + page_int_offset + page_left) {
        memset(page->addr + page_int_offset, 0, page_left);
        page_int_offset = 0;
        page_left = page_length;
        ++page;
        is_zero[i++] = true;
      }

      // |--------------<padding><left ext_it>----------|
      // |--------------<      page      >--------------------|
      padding = ext_it->first - page->offset;
      if (padding) {
        memset(page->addr + page_int_offset, 0, padding);
        page_int_offset += padding;
        page_left -= padding;
      }

      tlen = ext_it->second;
      while (tlen) {
        // |----------------<left ext_it>---<next ext>-----|
        // |--------------<page-1>-------------------------|
        // |--------------<       page-2      >------------|
        // |--------------<     page-3    >----------------|
        copy_size = MIN(page_left, tlen);
        bliter.copy(copy_size, page->addr + page_int_offset);
        tlen -= copy_size;
        if (page_left == copy_size) {
          ++page;
          is_zero[i++] = false;
          page_left = page_length;
          page_int_offset = 0;
        } else {
          page_left -= copy_size;
          page_int_offset += copy_size;
        }
      }
    }
    ldout(cct, 20) << __func__ << " page left length " << page_left << dendl;
    if (page_left) {
      memset(page->addr + page_int_offset, 0, page_left);
      ++page;
      is_zero[i++] = true;
    }
    page_end = bc_read_comp->extent.page_extents.end();
    while (i != num_pages) {
      memset(page->addr, 0, page_length);
      ++page;
      is_zero[i++] = true;
    }

    char *buf = bc_read_comp->buf;
    uint64_t start_padding, end_len;
    page = bc_read_comp->extent.page_extents[0];
    Mutex::Locker l(tree_lock);
    start_padding = bc_read_comp->start > page->offset ?
        bc_read_comp->start - page->offset : 0;
    if (num_pages == 1) {
      end_len = bc_read_comp->end < page->offset + page_length ?
          bc_read_comp->end - bc_read_comp->start : page_length;
      ldout(cct, 20) << __func__ << " start offset=" << page->offset + start_padding
                     << " length is " << end_len << dendl;
      memcpy(buf, page->addr + start_padding, end_len);
    } else {
      copy_size = page_length - start_padding;
      memcpy(buf, page->addr + start_padding, copy_size);
      buf += copy_size;
      ++page;
      for (size_t i = 1; i < num_pages - 1; ++i, ++page) {
        if (is_zero[i])
          memset(buf, 0, page_length);
        else
          memcpy(buf, page->addr, page_length);
        buf += page_length;
      }
      end_len = bc_read_comp->end < page->offset + page_length ?
          bc_read_comp->end - page->offset : page_length;
      memcpy(buf, page->addr, end_len);
    }
    unflight_pages(&bc_read_comp->extent.page_extents[0],
                   bc_read_comp->extent.page_extents.size());
    r = req->get_object_len();
  }
  bc_read_comp->comp->complete_request(r);
}

void BlockCacher::complete_write(C_BlockCacheWrite *bc_write_comp, int r, bool noretry)
{
  ldout(cct, 20) << __func__ << " r=" << r << dendl;

  if (r < 0 && !noretry) {
    ldout(cct, 10) << __func__ << " marking dirty again due to error "
                   << " r = " << r << " " << cpp_strerror(-r) << dendl;
    Mutex::Locker l(flush_lock);
    flush_retry_writes.push_back(bc_write_comp);
    flush_cond.Signal();
    return ;
  }

  flush_lock.Lock();
  if (!(--flush_commits[bc_write_comp->flush_id].first) && flush_id > bc_write_comp->flush_id) {
    ldout(cct, 5) << __func__ << " complete flush_id=" << bc_write_comp->flush_id << dendl;
    flush_commits[bc_write_comp->flush_id].second->complete(0);
    flush_commits.erase(bc_write_comp->flush_id);
  }
  flush_lock.Unlock();
  tree_lock.Lock();
  unflight_pages(&bc_write_comp->extent.page_extents[0],
                 bc_write_comp->extent.page_extents.size());
  dirty_page_state.dirty_writing_page.sub(bc_write_comp->extent.page_extents.size());
  tree_lock.Unlock();
  bc_write_comp->comp->complete_request(r);
}

int BlockCacher::read_object_extents(ImageCtx *ictx, uint64_t offset, size_t len,
                                     map<object_t, vector<ObjectPage> > &object_extents,
                                     char *buf, BlockCacherCompletion *c, uint64_t snap_id)
{
  ldout(cct, 20) << __func__ << dendl;

  int r;
  vector<pair<uint64_t,uint64_t> > buffer_extents;

  for (map<object_t, vector<ObjectPage> >::iterator it = object_extents.begin();
       it != object_extents.end(); ++it) {
    for (vector<ObjectPage>::iterator p = it->second.begin(); p != it->second.end(); ++p) {
      C_BlockCacheRead *bc_read_comp = new C_BlockCacheRead(this, c, *p, offset, len, buf);
      ldout(cct, 20) << " oid " << p->oid << " " << p->offset << "~"
                     << p->length << " from " << p->page_extents << dendl;

      AioRead *req = new AioRead(ictx, p->oid.name, p->objectno, p->offset, p->length,
                                 buffer_extents, snap_id, true, bc_read_comp, 0);
      bc_read_comp->req = req;
      if (mock)
        mock_thread->queue_read(req);
      else
        req->send();
      if (r == -ENOENT)
        r = 0;
      if (r < 0) {
        bc_read_comp->complete(r);
        return r;
      }
    }
  }
  return 0;
}

void BlockCacher::prepare_continuous_pages(ImageCtx *ictx, map<uint64_t, Page*> &pages,
                                           map<object_t, vector<ObjectPage> > &object_extents)
{
  ldout(cct, 10) << __func__ << " " << pages.size() << " pages" << dendl;
  uint64_t last_offset = 0;
  vector<Page*> continuous_pages;
  for (map<uint64_t, Page*>::iterator it = pages.begin();
       it != pages.end(); ++it) {
    if (it->first != last_offset + page_length) {
      if (!continuous_pages.empty()) {
        Striper::file_to_pages(cct, ictx->format_string, &ictx->layout,
                               continuous_pages[0]->offset, continuous_pages.size()*page_length, 0,
                               &continuous_pages[0], page_length, object_extents);
        continuous_pages.clear();
      }
    }

    continuous_pages.push_back(it->second);
  }
}

void BlockCacher::flush_object_extent(ImageCtx *ictx, map<object_t, vector<ObjectPage> > &object_extents,
                                      BlockCacherCompletion *c, ::SnapContext &snapc)
{
  ldout(cct, 10) << __func__ << dendl;
  for (map<object_t, vector<ObjectPage> >::iterator it = object_extents.begin();
       it != object_extents.end(); ++it) {
    for (vector<ObjectPage>::iterator p = it->second.begin(); p != it->second.end(); ++p) {
      C_BlockCacheWrite *bc_write_comp = new C_BlockCacheWrite(this, c, ictx, *p, flush_id);
      for (vector<Page*>::iterator q = p->page_extents.begin();
           q != p->page_extents.end(); ++q)
        bc_write_comp->data.append(static_cast<const char*>((*q)->addr), page_length);

      bc_write_comp->send_by_bc_write_comp(snapc);
      Mutex::Locker l(flush_lock);
      flush_commits[flush_id].first++;
    }
  }
}

void BlockCacher::flush_pages(uint32_t num, Context *c)
{
  ldout(cct, 10) << __func__ << " flush_pages=" << num << dendl;
  map<uint16_t, map<uint64_t, Page*> > sorted_flush;
  Mutex::Locker l(dirty_page_lock);
  dirty_page_state.writeback_pages(sorted_flush, num);
  ImageCtx *ictx;
  for (map<uint16_t, map<uint64_t, Page*> >::iterator it = sorted_flush.begin();
       it != sorted_flush.end(); ++it) {
    {
      RWLock::RLocker l(ictx_management_lock);
      ictx = registered_ictx[it->first];
    }
    map<object_t, vector<ObjectPage> > object_extents;
    prepare_continuous_pages(ictx, it->second, object_extents);
    ictx->snap_lock.get_read();
    ::SnapContext snapc = ictx->snapc;
    ictx->snap_lock.put_read();
    BlockCacherCompletion *comp = new BlockCacherCompletion(c);
    ldout(cct, 10) << __func__ << " object=" << it->first << dendl;
    flush_object_extent(ictx, object_extents, comp, snapc);
  }
}

void BlockCacher::flusher_entry()
{
  ldout(cct, 10) << __func__ << " start" << dendl;
  bool recheck = false;
  uint32_t num_flush;
  flush_lock.Lock();
  while (!flusher_stop) {
    if (recheck)
      recheck = false;
    else
      flush_cond.WaitInterval(cct, flush_lock, utime_t(1, 0));

    while (!flush_retry_writes.empty()) {
      ldout(cct, 10) << __func__ << " exist " << flush_retry_writes.size() << " retry writes" << dendl;
      C_BlockCacheWrite *bc_write_comp = flush_retry_writes.front();
      flush_retry_writes.pop_front();
      bc_write_comp->ictx->snap_lock.get_read();
      ::SnapContext snapc = bc_write_comp->ictx->snapc;
      bc_write_comp->ictx->snap_lock.put_read();
      bc_write_comp->send_by_bc_write_comp(snapc);
    }

    {
      Mutex::Locker l(dirty_page_lock);
      num_flush = dirty_page_state.writeback_pages();
    }
    // Note: do we need to limit inflight dirty write? Since we already limit
    // inflight pages in "get_pages"
    if (num_flush) {
      // flush some dirty pages
      ldout(cct, 10) << __func__ << " flush_page=" << num_flush << dendl;
      C_FlushWrite *c = new C_FlushWrite(this, NULL);
      flush_pages(num_flush, c);
    }

    while (!wait_writeback.empty()) {
      std::list<Context*> process;
      process.swap(wait_writeback);
      recheck = true;
      flush_lock.Unlock();
      for (std::list<Context*>::iterator it = process.begin(); it != process.end(); ++it)
        (*it)->complete(0);
      flush_lock.Lock();
    }
  }

  while (!flush_retry_writes.empty()) {
    C_BlockCacheWrite *w = flush_retry_writes.front();
    ldout(cct, 1) << __func__ << " still has retry write request " << w << dendl;
    flush_retry_writes.pop_front();
    flush_lock.Unlock();
    complete_write(w, -EAGAIN);
    flush_lock.Lock();
  }
  flush_lock.Unlock();

  /* Wait for reads/writes to finish. This is only possible if handling
   * -ENOENT made some read completions finish before their rados read
   * came back. If we don't wait for them, and destroy the cache, when
   * the rados reads do come back their callback will try to access the
   * no-longer-valid BlockCacher.
   */
  Mutex::Locker l(tree_lock);
  while (inflight_pages > 0) {
    ldout(cct, 10) << __func__ << " waiting for all pages to complete. Number left: "
                   << inflight_pages << dendl;
    inflight_page_wait = true;
    tree_cond.Wait(tree_lock);
  }

  ldout(cct, 10) << __func__ << " finish" << dendl;
}

int BlockCacher::write_buffer(uint64_t ictx_id, uint64_t off, size_t len, const char *buf,
                              Context *c, int op_flags, ::SnapContext &snapc)
{
  ldout(cct, 20) << __func__ << " ictx=" << ictx_id << " off=" << off << " len=" << len << dendl;
  if (len == 0)
    return 0;
  uint64_t align_offset = off - off % page_length;
  uint64_t num_pages = (len + off - align_offset) / page_length;
  if ((off + len) % page_length)
    ++num_pages;
  Page *pages[num_pages];
  bool hit[num_pages];

  ictx_management_lock.get_read();
  PageRBTree *tree = registered_tree[ictx_id], *ghost_tree = ghost_trees[ictx_id];
  ImageCtx *ictx = registered_ictx[ictx_id];
  ictx_management_lock.put_read();

  tree_lock.Lock();
  int r = get_pages(tree, ghost_tree, pages, hit, num_pages, align_offset);
  assert(r == 0);
  {
    Mutex::Locker l(dirty_page_lock);
    for (uint64_t i = 0; i < num_pages; ++i, buf += page_length) {
      memcpy(pages[i]->addr, buf, page_length);
      dirty_page_state.mark_dirty(pages[i]);
      pages[i]->inflight = 1;
    }
  }
  tree_lock.Unlock();

  if (dirty_page_state.writethrough()) {
    // write-thru!  flush what we just wrote.
    ldout(cct, 20) << __func__ << " writethrough " << dendl;
    map<object_t, vector<ObjectPage> > object_extents;
    BlockCacherCompletion *comp = new BlockCacherCompletion(c);

    Striper::file_to_pages(cct, ictx->format_string, &ictx->layout,
                           pages[0]->offset, num_pages*page_length, 0,
                           pages, page_length, object_extents);
    flush_object_extent(ictx, object_extents, comp, snapc);
  } else if (dirty_page_state.need_writeback()) {
    ldout(cct, 10) << __func__ << " exceed max dirty pages, need wait for write back" << dendl;
    Mutex::Locker l(flush_lock);
    wait_writeback.push_back(c);
    flush_cond.Signal();
  } else {
    c->complete(0);
  }
  return 0;
}

int BlockCacher::read_buffer(uint64_t ictx_id, uint64_t offset, size_t len,
                             char *buf, Context *c, uint64_t snap_id, int op_flags)
{
  ldout(cct, 20) << __func__ << " ictx_id=" << ictx_id << " completion " << c << " offset=" << offset
                 << " op_flags=" << op_flags << dendl;

  map<uint64_t, Page*> need_read;
  int r;

  uint64_t align_offset = offset - offset % page_length;
  uint32_t num_pages = (len + offset - align_offset) / page_length;
  if ((offset + len) % page_length)
    ++num_pages;

  Page *pages[num_pages];
  bool hit[num_pages];

  ictx_management_lock.get_read();
  PageRBTree *tree = registered_tree[ictx_id], *ghost_tree = ghost_trees[ictx_id];
  ImageCtx *ictx = registered_ictx[ictx_id];
  ictx_management_lock.put_read();

  tree_lock.Lock();
  r = get_pages(tree, ghost_tree, pages, hit, num_pages, align_offset);
  assert(r == 0);
  for (uint64_t i = num_pages; i < num_pages; ++i) {
    if (hit[i]) {
      memcpy(buf + i * page_length, pages[i]->addr, page_length);
    } else {
      need_read[pages[i]->offset] = pages[i];
      pages[i]->inflight = 1;
      ++inflight_pages;
    }
  }
  tree_lock.Unlock();

  if (!need_read.empty()) {
    BlockCacherCompletion *comp = new BlockCacherCompletion(c);
    map<object_t, vector<ObjectPage> > object_extents;
    prepare_continuous_pages(ictx, need_read, object_extents);
    r = read_object_extents(ictx, offset, len, object_extents, buf, comp, snap_id);
    if (r < 0)
      return r;
  } else {
    c->complete(len);
  }
  return 0;
}

void BlockCacher::user_flush(Context *c)
{
  ldout(cct, 20) << __func__ << " ctxt=" << c << dendl;
  dirty_page_state.set_writeback();
  flush_pages(0, c);
  Mutex::Locker l(flush_lock);
  if (!flush_commits.empty())
    flush_commits[flush_id++].second = c;
}
}
