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

#define dout_subsys ceph_subsys_blockcacher
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
    ctxt = NULL;
    delete this;
  }
}

void C_AioRead2::finish(int r) {
  m_completion->rval = r;
  m_completion->complete(m_cct);
  m_completion->put_unlock();
}

inline Page* CARState::evict_data()
{
  Page *p;
  bool reversal = false;
  Mutex::Locker l(lock);
  while (true) {
    if (arc_list_size[ARC_LRU] >= arc_lru_limit || reversal) {
      ldout(cct, 20) << __func__ << " pop lru" << dendl;
      p = _pop_head_page(ARC_LRU);
      if (p) {
        if (p->reference) {
          _append_page(p, ARC_LFU);
        } else {
          _append_page(p, ARC_LRU_GHOST);
          break;
        }
        continue;
      }
    }
    ldout(cct, 20) << __func__ << " pop lfu" << dendl;
    p = _pop_head_page(ARC_LFU);
    if (!p) {
      reversal = true;
    } else {
      if (p->reference) {
        _append_page(p, ARC_LFU);
      } else {
        _append_page(p, ARC_LFU_GHOST);
        break;
      }
    }
  };
  return p;
}

inline void CARState::adjust_and_hold(Page *cur_page, int hit_ghost_history)
{
  Mutex::Locker l(lock);
  if (hit_ghost_history == ARC_LRU_GHOST) {
    /* cache directory hit */
    if (arc_list_size[ARC_LRU_GHOST])
      arc_lru_limit = MIN(arc_lru_limit + arc_list_size[ARC_LFU_GHOST]/arc_list_size[ARC_LRU_GHOST], data_pages);
    cur_page->arc_idx = ARC_LFU;
  } else if (hit_ghost_history == ARC_LFU_GHOST) {
    /* cache directory hit */
    if (arc_list_size[ARC_LFU_GHOST]) {
      uint32_t difference = arc_list_size[ARC_LRU_GHOST]/arc_list_size[ARC_LFU_GHOST];
      arc_lru_limit = arc_lru_limit > difference ? arc_lru_limit - difference : 0;
    }
    cur_page->arc_idx = ARC_LFU;
  } else {
    /* cache directory miss */
    cur_page->arc_idx = ARC_LRU;
  }
  ++arc_list_size[cur_page->arc_idx];
  ldout(cct, 10) << __func__ << " adjust new lru limit to " << arc_lru_limit << dendl;
}

int BlockCacher::reg_region(uint64_t num_pages)
{
  assert(tree_lock.is_locked());
  assert(num_pages);
  ldout(cct, 10) << __func__ << " page_length=" << page_length << " num_pages=" << num_pages << dendl;
  size_t size = num_pages * page_length;
  Region region;
  int r = ::posix_memalign(&region.addr, CEPH_PAGE_SIZE, size);
  if (r < 0) {
    lderr(cct) << __func__ << " failed to alloc memory(" << size << "): "
        << cpp_strerror(errno) << dendl;
  } else {
    region.length = size;
    Page *p;
    char *data = static_cast<char*>(region.addr);
    for (uint32_t i = 0; i < num_pages; ++i) {
      p = free_pages_head;
      free_pages_head = free_pages_head->page_next;
      ++num_free_data_pages;
      p->addr = data;
      p->page_next = free_data_pages_head;
      free_data_pages_head = p;
      data += page_length;
    }
  }
  remain_data_pages -= num_pages;
  regions.push_back(region);
  ldout(cct, 10) << __func__ << " r=" << r << dendl;
  return r;
}

// https://dl.dropboxusercontent.com/u/91714474/Papers/clockfast.pdf
int BlockCacher::get_pages(uint16_t ictx_id, PageRBTree *tree, PageRBTree *ghost_tree, Page **pages, bool hit[],
                           size_t num_pages, size_t align_offset, bool only_hit)
{
  ldout(cct, 10) << __func__ << " " << num_pages << " pages, align_offset=" << align_offset << dendl;

  while (num_pages + dirty_page_state.dirty_pages.read() + inflight_reading_pages >= total_half_pages
         && !only_hit) {
    ldout(cct, 0) << __func__ << " can't provide with enough pages" << dendl;
    get_page_wait = true;
    tree_cond.Wait(tree_lock);
  }

  Page *cur_page = NULL;
  uint64_t end_offset = align_offset + num_pages * page_length;
  size_t idx = 0;
  size_t hits = 0;
  RBTree::Iterator end = tree->end();
  memset(hit, 0, sizeof(bool)*num_pages);
  for (RBTree::Iterator ictx_it = tree->lower_bound(align_offset);
       ictx_it != end; ++ictx_it) {
    cur_page = ictx_it->get_container<Page>(offsetof(Page, rb));
    if (cur_page->offset < end_offset) {
      car_state.hit_page(cur_page);
      idx = (cur_page->offset-align_offset)/page_length;
      pages[idx] = cur_page;
      hit[idx] = true;
      assert(idx < num_pages);
      ldout(cct, 20) << __func__ << " hit cache page offset=" << cur_page->offset << dendl;
      hits++;
    } else {
      break;
    }
  }
  if (only_hit)
    return 0;

  assert(ghost_tree);
  RBTree::Iterator ghost_it = ghost_tree->lower_bound(align_offset);
  Page *ghost_page = ghost_it != end ? ghost_it->get_container<Page>(offsetof(Page, rb)) : NULL;
  int hit_ghost_history;  // 0 is not hit
  idx = 0;
  for (size_t pos = align_offset; idx < num_pages; pos += page_length, idx++) {
    if (hit[idx])
      continue;

    // cache miss
    hit[idx] = false;
    hit_ghost_history = 0;
    if (ghost_it != end) {
      if (ghost_page->offset == pos) {
        ldout(cct, 20) << __func__ << " hit history " << *ghost_page << dendl;
        hit_ghost_history = ghost_page->arc_idx;
        ++ghost_it;
      } else {
        while (ghost_page->offset < pos && ghost_it != end)
          ++ghost_it;
      }
    }

    if (!free_data_pages_head) {
      if (!remain_data_pages) {
        /* cache full, got a page data from cache */
        Page *p = car_state.evict_data();
        ldout(cct, 20) << __func__ << " evicted page " << *p << dendl;
        tree->erase(p);
        ghost_tree->insert(p);
        cur_page = car_state.get_ghost_page(hit_ghost_history ? ghost_page : NULL); 
        if (cur_page) {
          ghost_tree->erase(cur_page);
        } else {
          cur_page = free_pages_head;
          free_pages_head = free_pages_head->page_next;
          cur_page->page_next = NULL;
        }
        cur_page->addr = p->addr;
      } else {
        uint32_t rps = MIN(remain_data_pages, region_maxpages);
        ldout(cct, 20) << __func__ << " no free data page, try to alloc a page region("
                       << rps << " pages)" << dendl;
        assert(reg_region(rps) == 0);
        cur_page = free_data_pages_head;
        free_data_pages_head = free_data_pages_head->page_next;
        cur_page->page_next = NULL;
        --num_free_data_pages;
      }
    } else {
      cur_page = free_data_pages_head;
      free_data_pages_head = free_data_pages_head->page_next;
      cur_page->page_next = NULL;
      --num_free_data_pages;
    }

    assert(cur_page->addr);
    car_state.adjust_and_hold(cur_page, hit_ghost_history);

    cur_page->ictx_id = ictx_id;
    cur_page->offset = pos;
    cur_page->version = 0;
    tree->insert(cur_page);
    pages[idx] = cur_page;

    ghost_page = ghost_it != end ? ghost_it->get_container<Page>(offsetof(Page, rb)) : NULL;
    if (ghost_page)
      assert(ghost_page->offset >= pos);
  }

  return 0;
}

void BlockCacher::complete_read(C_BlockCacheRead *bc_read_comp, int r)
{
  ldout(cct, 20) << __func__ << " r=" << r << dendl;

  vector<Page*>::iterator page_it = bc_read_comp->extent.page_extents.begin();
  if (r < 0 && r != -ENOENT) {
    ldout(cct, 1) << __func__ << " got r=" << r << " for ctxt=" << bc_read_comp->comp
                  << " oustanding reads=" << bc_read_comp->comp << dendl;
    for (size_t i = 0; i < bc_read_comp->extent.page_extents.size(); ++i, ++page_it) {
      car_state.insert_page(*page_it);
    }
  } else { // this was a sparse_read operation
    // reads from the parent don't populate the m_ext_map and the overlap
    // may not be the full buffer.  compensate here by filling in m_ext_map
    // with the read extent when it is empty.
    map<uint64_t, uint64_t> image_ext_map;
    AioRead *req = bc_read_comp->req;
    if (req->m_ext_map.empty())
      req->m_ext_map[req->get_object_off()] = req->data().length();

    uint64_t object_offset = bc_read_comp->extent.offset;
    size_t num_pages = bc_read_comp->extent.page_extents.size(), i = 0;
    bufferlist::iterator bliter = req->data().begin();
    uint64_t page_left = page_length;
    uint64_t page_int_offset = 0;
    uint64_t copy_size, tlen, padding;
    bool is_zero[num_pages];
    data_lock.Lock();
    for (map<uint64_t, uint64_t>::iterator ext_it = req->m_ext_map.begin();
         ext_it != req->m_ext_map.end(); ++ext_it) {
      ldout(cct, 20) << __func__ << " ext_it = (" << ext_it->first << ", " << ext_it->second
                     << ") page left offset " << page_int_offset << dendl;
      // |-----------------<left ext_it>----------|
      // |---------<page>-------------------------|
      while (ext_it->first >= object_offset + page_int_offset + page_left) {
        if (bc_read_comp->version > (*page_it)->version) {
          is_zero[i++] = true;
          memset((*page_it)->addr + page_int_offset, 0, page_left);
        } else {
          is_zero[i++] = false;
        }
        page_int_offset = 0;
        page_left = page_length;
        ++page_it;
        object_offset += page_length;
      }

      // |--------------<padding><left ext_it>----------|
      // |--------------<      page      >--------------------|
      padding = ext_it->first - object_offset;
      if (padding) {
        if (bc_read_comp->version > (*page_it)->version)
          memset((*page_it)->addr + page_int_offset, 0, padding);

        page_int_offset += padding;
        page_left -= padding;
        object_offset += padding;
      }

      tlen = ext_it->second;
      while (tlen) {
        // |----------------<left ext_it>---<next ext>-----|
        // |--------------<page-1>-------------------------|
        // |--------------<       page-2      >------------|
        // |--------------<     page-3    >----------------|
        copy_size = MIN(page_left, tlen);
        if (bc_read_comp->version > (*page_it)->version) {
          ldout(cct, 20) << __func__ << " data copy to page offset=" << (*page_it)->offset << dendl;
          bliter.copy(copy_size, (*page_it)->addr + page_int_offset);
        }
        tlen -= copy_size;
        if (page_left == copy_size) {
          ++page_it;
          is_zero[i++] = false;
          page_left = page_length;
          page_int_offset = 0;
          object_offset += page_length;
        } else {
          page_left -= copy_size;
          page_int_offset += copy_size;
          object_offset += copy_size;
        }
      }
    }
    if (page_it != bc_read_comp->extent.page_extents.end()) {
      ldout(cct, 20) << __func__ << " page left length " << page_left << dendl;
      if (page_left) {
        if (bc_read_comp->version > (*page_it)->version) {
          is_zero[i++] = true;
          memset((*page_it)->addr + page_int_offset, 0, page_left);
        } else {
          is_zero[i++] = false;
        }
        ++page_it;
      }
      while (i != num_pages) {
        if (bc_read_comp->version > (*page_it)->version) {
          memset((*page_it)->addr, 0, page_length);
          is_zero[i++] = true;
        } else {
          is_zero[i++] = false;
        }
        ++page_it;
      }
    }
    assert(page_it == bc_read_comp->extent.page_extents.end());
    assert(i == bc_read_comp->extent.page_extents.size());

    char *buf = bc_read_comp->start_buf;
    uint64_t start_padding, end_len;
    page_it = bc_read_comp->extent.page_extents.begin();
    start_padding = bc_read_comp->start > (*page_it)->offset ?
        bc_read_comp->start - (*page_it)->offset : 0;
    if (num_pages == 1) {
      end_len = bc_read_comp->end < (*page_it)->offset + page_length ?
          bc_read_comp->end - (*page_it)->offset : page_length;
      ldout(cct, 20) << __func__ << " start offset=" << (*page_it)->offset + start_padding
                     << " length is " << end_len << dendl;
      memcpy(start_padding ? buf : buf + ((*page_it)->offset - bc_read_comp->start),
             (*page_it)->addr + start_padding, end_len);
    } else {
      copy_size = page_length - start_padding;
      memcpy(start_padding ? buf : buf + ((*page_it)->offset - bc_read_comp->start),
             (*page_it)->addr + start_padding, copy_size);
      car_state.insert_page(*page_it);
      ++page_it;
      for (size_t i = 1; i < num_pages - 1; ++i, ++page_it) {
        if (is_zero[i])
          memset(buf, 0, page_length);
        else
          memcpy(buf + ((*page_it)->offset - bc_read_comp->start),
                 (*page_it)->addr, page_length);
        ldout(cct, 20) << __func__ << " data copy from page offset=" << (*page_it)->offset << dendl;
        car_state.insert_page(*page_it);
      }
      end_len = bc_read_comp->end < (*page_it)->offset + page_length ?
          bc_read_comp->end - (*page_it)->offset : page_length;
      memcpy(buf + ((*page_it)->offset - bc_read_comp->start),
             (*page_it)->addr, end_len);
    }
    car_state.insert_page(*page_it);
    r = req->get_object_len();
    data_lock.Unlock();
  }
  bc_read_comp->comp->complete_request(r);

  Mutex::Locker l(tree_lock);
  inflight_reading_pages -= bc_read_comp->extent.page_extents.size();
  if (get_page_wait) {
    get_page_wait = false;
    tree_cond.Signal();
  }
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
  ldout(cct, 20) << __func__ << " page size=" << bc_write_comp->extent.page_extents.size()
                 << " inflight_writing_pages=" << inflight_writing_pages << dendl;
  inflight_writing_pages -= bc_write_comp->extent.page_extents.size();
  if (!(--flush_commits[bc_write_comp->flush_id].first) && flush_id > bc_write_comp->flush_id) {
    ldout(cct, 5) << __func__ << " complete flush_id=" << bc_write_comp->flush_id << dendl;
    flush_commits[bc_write_comp->flush_id].second->complete(1);
    flush_commits.erase(bc_write_comp->flush_id);
  }
  if (max_writing_wait) {
    max_writing_wait = false;
    flush_cond.Signal();
  }
  flush_lock.Unlock();
  ldout(cct, 20) << __func__ << " " << this << " complete request comp=" << bc_write_comp->comp << dendl;
  bc_write_comp->comp->complete_request(r);
}

void BlockCacher::read_object_extents(ImageCtx *ictx, uint64_t offset, size_t len,
                                     map<object_t, vector<ObjectPage> > &object_extents,
                                     char *buf, BlockCacherCompletion *c, uint64_t snap_id, uint64_t v)
{
  ldout(cct, 20) << __func__ << dendl;

  vector<pair<uint64_t,uint64_t> > buffer_extents;

  for (map<object_t, vector<ObjectPage> >::iterator it = object_extents.begin();
       it != object_extents.end(); ++it) {
    for (vector<ObjectPage>::iterator p = it->second.begin(); p != it->second.end(); ++p) {
      C_BlockCacheRead *bc_read_comp = new C_BlockCacheRead(this, c, *p, offset, len, buf, v);
      ldout(cct, 15) << " oid " << p->oid << " " << p->offset << "~"
                     << p->length << " read_comp=" << bc_read_comp << " from " << p->page_extents << dendl;

      AioRead *req = new AioRead(ictx, p->oid.name, p->objectno, p->offset, p->length,
                                 buffer_extents, snap_id, true, bc_read_comp, 0);
      bc_read_comp->req = req;
      if (mock_thread)
        mock_thread->queue_read(req, p->oid.name);
      else
        req->send();
    }
  }
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
  if (!continuous_pages.empty()) {
    Striper::file_to_pages(cct, ictx->format_string, &ictx->layout,
                           continuous_pages[0]->offset, continuous_pages.size()*page_length, 0,
                           &continuous_pages[0], page_length, object_extents);
    continuous_pages.clear();
  }
}

void BlockCacher::flush_object_extent(ImageCtx *ictx, map<object_t, vector<ObjectPage> > &object_extents,
                                      BlockCacherCompletion *c, ::SnapContext &snapc, uint64_t v)
{
  assert(data_lock.is_locked());
  Page *page;
  ldout(cct, 10) << __func__ << dendl;
  for (map<object_t, vector<ObjectPage> >::iterator it = object_extents.begin();
       it != object_extents.end(); ++it) {
    for (vector<ObjectPage>::iterator p = it->second.begin(); p != it->second.end(); ++p) {
      C_BlockCacheWrite *bc_write_comp = new C_BlockCacheWrite(this, c, ictx, *p, flush_id, v);
      for (vector<Page*>::iterator q = p->page_extents.begin();
           q != p->page_extents.end(); ++q) {
        page = *q;
        page->version = v;
        page->dirty = 0;
        car_state.insert_page(page);
        bc_write_comp->data.append(static_cast<const char*>(page->addr), page_length);
      }

      {
        Mutex::Locker l(flush_lock);
        ldout(cct, 20) << __func__ << " page size=" << bc_write_comp->extent.page_extents.size()
                       << " inflight_writing_pages=" << inflight_writing_pages << dendl;
        inflight_writing_pages += bc_write_comp->extent.page_extents.size();
        flush_commits[flush_id].first++;
      }
      bc_write_comp->send_by_bc_write_comp(snapc);
    }
  }
}

void BlockCacher::flush_pages(uint32_t num, Context *c)
{
  map<uint16_t, map<uint64_t, Page*> > sorted_flush;
  dirty_page_lock.Lock();
  uint32_t pages = dirty_page_state.writeback_pages(sorted_flush, num);
  ldout(cct, 20) << __func__ << " num=" << num << " flush_page=" << pages << dendl;
  if (!pages) {
    dirty_page_lock.Unlock();
    c->complete(0);
    return ;
  }
  ImageCtx *ictx;
  for (map<uint16_t, map<uint64_t, Page*> >::iterator it = sorted_flush.begin();
       it != sorted_flush.end(); ++it) {
    {
      RWLock::RLocker l(ictx_management_lock);
      ictx = registered_ictx[it->first];
    }
    if (!ictx) {
      ldout(cct, 1) << __func__ << " ictx_id=" << it->first << " already unregistered,"
                    << " discard dirty pages!" << dendl;
      for (map<uint64_t, Page*>::iterator page_it = it->second.begin();
           page_it != it->second.end(); ++page_it) {
        Page *p = page_it->second;
        car_state.insert_page(p);
      }
    } else {
      map<object_t, vector<ObjectPage> > object_extents;
      prepare_continuous_pages(ictx, it->second, object_extents);
      ictx->snap_lock.get_read();
      ::SnapContext snapc = ictx->snapc;
      ictx->snap_lock.put_read();
      BlockCacherCompletion *comp = new BlockCacherCompletion(c);
      ldout(cct, 10) << __func__ << " object=" << it->first << dendl;
      Mutex::Locker l2(data_lock);
      flush_object_extent(ictx, object_extents, comp, snapc, global_version);
      comp->finish_adding_requests();
    }
  }
  dirty_page_state.dirty_pages.sub(pages);
  dirty_page_lock.Unlock();
  {
    Mutex::Locker l(tree_lock);
    if (get_page_wait) {
      get_page_wait = false;
      tree_cond.Signal();
    }
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
      data_lock.Lock();
      Page *page;
      uint64_t relative_offset = 0;
      for (vector<Page*>::iterator it = bc_write_comp->extent.page_extents.begin();
           it != bc_write_comp->extent.page_extents.end(); ++it, relative_offset+= page_length) {
        page = *it;
        if(page->version > bc_write_comp->version) {
          ldout(cct, 20) << __func__ << " comp_write's version=" << bc_write_comp->version
                         << " page's version=" << page->version << dendl;
          bc_write_comp->data.copy_in(relative_offset, page_length, page->addr);
        } else {
          assert(page->version == bc_write_comp->version);
        }
      }
      bc_write_comp->send_by_bc_write_comp(snapc);
      data_lock.Unlock();
    }

    if (inflight_writing_pages >= max_writing_pages) {
      ldout(cct, 5) << __func__ << " " << inflight_writing_pages << " >= " << max_writing_pages
                    << "exceed max inflight writeback pages" << dendl;
      max_writing_wait = true;
      continue;
    }
    std::list<Context*> process;
    if (!wait_writeback.empty()) {
      process.swap(wait_writeback);
      recheck = true;
    }

    num_flush = dirty_page_state.need_writeback_pages();
    // Note: do we need to limit inflight dirty write? Since we already limit
    // inflight pages in "get_pages"
    if (num_flush) {
      // flush some dirty pages
      ldout(cct, 10) << __func__ << " flush_page=" << num_flush << dendl;
      C_FlushWrite *c = new C_FlushWrite(this, NULL);
      flush_lock.Unlock();
      flush_pages(num_flush, c);
      flush_lock.Lock();
      recheck = true;
    }

    if (!process.empty()) {
      flush_lock.Unlock();
      for (std::list<Context*>::iterator it = process.begin(); it != process.end(); ++it)
        (*it)->complete(0);
      flush_lock.Lock();
    }
  }

  /* Wait for reads/writes to finish. This is only possible if handling
   * -ENOENT made some read completions finish before their rados read
   * came back. If we don't wait for them, and destroy the cache, when
   * the rados reads do come back their callback will try to access the
   * no-longer-valid BlockCacher.
   */
  while (inflight_writing_pages > 0) {
    ldout(cct, 10) << __func__ << " waiting for writing pages to complete. Number left: "
                   << inflight_writing_pages << dendl;
    max_writing_wait = true;
    flush_cond.Wait(flush_lock);
  }

  while (!flush_retry_writes.empty()) {
    C_BlockCacheWrite *w = flush_retry_writes.front();
    ldout(cct, 1) << __func__ << " still has retry write request " << w << dendl;
    flush_retry_writes.pop_front();
    flush_lock.Unlock();
    complete_write(w, -EAGAIN, true);
    flush_lock.Lock();
  }

  uint64_t last_flush_count = flush_commits[flush_id].first;
  assert(!last_flush_count);
  flush_commits.erase(flush_id);

  flush_lock.Unlock();

  Mutex::Locker l(tree_lock);
  while (inflight_reading_pages > 0) {
    ldout(cct, 10) << __func__ << " waiting for all pages to complete. Number left: "
                   << inflight_reading_pages << dendl;
    get_page_wait = true;
    tree_cond.Wait(tree_lock);
  }

  ldout(cct, 10) << __func__ << " finish" << dendl;
}

int BlockCacher::write_buffer(uint64_t ictx_id, uint64_t off, size_t len, const char *buffer,
                              Context *c, int op_flags, ::SnapContext &snapc)
{
  ldout(cct, 20) << __func__ << " ictx=" << ictx_id << " off=" << off << " len=" << len << dendl;
  if (len == 0)
    return 0;
  uint64_t align_offset = off - off % page_length;
  uint64_t num_pages = (len + off - align_offset) / page_length;
  uint64_t v;
  if ((off + len) % page_length)
    ++num_pages;

  ictx_management_lock.get_read();
  PageRBTree *tree = registered_tree[ictx_id], *ghost_tree = ghost_trees[ictx_id];
  ImageCtx *ictx = registered_ictx[ictx_id];
  ictx_management_lock.put_read();
  if (!ictx) {
    ldout(cct, 1) << __func__ << " ictx is NULL, discard this write op" << dendl;
    c->complete(-ENOENT);
    return -ENOENT;
  }
  bool wt = dirty_page_state.writethrough();
  Page *pages[num_pages];
  bool hit[num_pages];

  {
    const char *buf = buffer;
    Mutex::Locker l1(tree_lock);
    int r = get_pages(ictx_id, tree, ghost_tree, pages, hit, num_pages, align_offset);
    assert(r == 0);
    uint64_t start_padding, end_len, end = off + len;
    size_t copy_size;
    size_t i = 0;
    start_padding = off > pages[i]->offset ? off - pages[i]->offset : 0;
    ldout(cct, 20) << __func__ << " start_padding=" << start_padding << dendl;
    Mutex::Locker l2(dirty_page_lock);
    data_lock.Lock();
    v = global_version++;
    if (num_pages == 1) {
      end_len = end < pages[i]->offset + page_length ? end - off : page_length;
      ldout(cct, 15) << __func__ << " start offset=" << pages[i]->offset + start_padding
                     << " length is " << end_len << dendl;
      memcpy(pages[i]->addr + start_padding, buf, end_len);
      assert(buf >= buffer && (buf + end_len) <= buffer + len);
    } else {
      if (hit[i] && !pages[i]->dirty) {
        ldout(cct, 20) << __func__ << " clean page=" << pages[i] << " dirtied" << dendl;
        car_state.make_dirty(pages[i]);
      }
      if (!wt)
        dirty_page_state.mark_dirty(pages[i]);
      copy_size = page_length - start_padding;
      memcpy(pages[i]->addr + start_padding, buf, copy_size);
      assert(buf >= buffer && (buf + copy_size) <= buffer + len);
      buf += copy_size;
      pages[i]->version = v;
      for (i = 1; i < num_pages - 1; ++i, buf += page_length) {
        if (hit[i] && !pages[i]->dirty) {
          ldout(cct, 20) << __func__ << " clean page=" << pages[i] << " dirtied" << dendl;
          car_state.make_dirty(pages[i]);
        }
        memcpy(pages[i]->addr, buf, page_length);
        assert(buf >= buffer && (buf + page_length) <= buffer + len);
        if (!wt)
          dirty_page_state.mark_dirty(pages[i]);
        pages[i]->version = v;
      }
      end_len = end < pages[i]->offset + page_length ? end - pages[i]->offset : page_length;
      memcpy(pages[i]->addr, buf, end_len);
      assert(buf >= buffer && (buf + end_len) <= buffer + len);
    }
    if (hit[i] && !pages[i]->dirty) {
      ldout(cct, 20) << __func__ << " clean page=" << pages[i] << " dirtied" << dendl;
      car_state.make_dirty(pages[i]);
    }
    if (!wt)
      dirty_page_state.mark_dirty(pages[i]);
    pages[i]->version = v;
    // the pages outside of car_state will be back when flushing
  }

  if (wt) {
    // write-thru!  flush what we just wrote.
    ldout(cct, 20) << __func__ << " writethrough " << dendl;
    map<object_t, vector<ObjectPage> > object_extents;
    BlockCacherCompletion *comp = new BlockCacherCompletion(c);

    Striper::file_to_pages(cct, ictx->format_string, &ictx->layout,
                           pages[0]->offset, num_pages*page_length, 0,
                           pages, page_length, object_extents);
    flush_object_extent(ictx, object_extents, comp, snapc, v);
    comp->finish_adding_requests();
    data_lock.Unlock();
  } else if (dirty_page_state.need_writeback()) {
    ldout(cct, 10) << __func__ << " exceed max dirty pages, need wait for write back" << dendl;
    data_lock.Unlock();
    Mutex::Locker l(flush_lock);
    wait_writeback.push_back(c);
    flush_cond.Signal();
  } else {
    data_lock.Unlock();
    c->complete(0);
  }
  return 0;
}

void BlockCacher::read_buffer(uint64_t ictx_id, uint64_t offset, size_t len,
                              char *buffer, Context *c, uint64_t snap_id, int op_flags)
{
  ldout(cct, 10) << __func__ << " ictx_id=" << ictx_id << " completion " << c << " offset=" << offset
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

  if (!ictx) {
    ldout(cct, 1) << __func__ << " ictx is NULL, discard this write op" << dendl;
    c->complete(-ENOENT);
    return ;
  }

  {
    Mutex::Locker l(tree_lock);
    r = get_pages(ictx_id, tree, ghost_tree, pages, hit, num_pages, align_offset);
    assert(r == 0);
    uint64_t start_padding, copy_size, end = offset + len;
    size_t i = 0;
    char *buf = buffer;
    start_padding = offset > pages[i]->offset ? offset - pages[i]->offset : 0;
    data_lock.Lock();
    if (num_pages == 1) {
      if (hit[i]) {
        copy_size = end < pages[i]->offset + page_length ? end - offset : page_length;
        memcpy(buf, pages[i]->addr + start_padding, copy_size);
      } else {
        need_read[pages[i]->offset] = pages[i];
      }
    } else {
      copy_size = page_length - start_padding;
      if (hit[i]) {
        memcpy(buf, pages[i]->addr + start_padding, copy_size);
      } else {
        need_read[pages[i]->offset] = pages[i];
      }
      buf += copy_size;
      for (i = 1; i < num_pages - 1; ++i, buf += page_length) {
        if (hit[i]) {
          memcpy(buf, pages[i]->addr, page_length);
        } else {
          need_read[pages[i]->offset] = pages[i];
        }
      }
      if (hit[i]) {
        copy_size = end < pages[i]->offset + page_length ? end - pages[i]->offset : page_length;
        memcpy(buf, pages[i]->addr, copy_size);
      } else {
        need_read[pages[i]->offset] = pages[i];
      }
    }
    inflight_reading_pages += need_read.size();
  }

  if (!need_read.empty()) {
    BlockCacherCompletion *comp = new BlockCacherCompletion(c);
    ldout(cct, 10) << __func__ << " do block request " << comp << " using rbd completion " << c << dendl;
    map<object_t, vector<ObjectPage> > object_extents;
    prepare_continuous_pages(ictx, need_read, object_extents);
    read_object_extents(ictx, offset, len, object_extents, buffer, comp, snap_id, global_version);
    comp->finish_adding_requests();
    data_lock.Unlock();
  } else {
    data_lock.Unlock();
    c->complete(len);
    ldout(cct, 10) << __func__ << " finish completion inline " << c << dendl;
  }
}

// TODO: we may want to only flush dirty pages for a specified image
void BlockCacher::user_flush(Context *ctx)
{
  ldout(cct, 20) << __func__ << " ctxt=" << ctx << dendl;
  dirty_page_state.set_writeback();
  C_FlushWrite *c = new C_FlushWrite(this, NULL);
  flush_pages(0, c);
  Mutex::Locker l(flush_lock);
  uint64_t count = flush_commits[flush_id].first;
  if (!count) {
    ldout(cct, 10) << __func__ << " no existing flush_id=" << flush_id << dendl;
    flush_commits.erase(flush_id);
    if (flush_commits.empty()) {
      ctx->complete(0);
      return ;
    }
  }
  flush_commits[flush_id++].second = ctx;
}

void BlockCacher::discard(uint64_t ictx_id, uint64_t offset, size_t len)
{
  ldout(cct, 20) << __func__ << " ictx=" << ictx_id << " offset=" << offset << " len=" << len << dendl;

  ictx_management_lock.get_read();
  PageRBTree *tree = registered_tree[ictx_id];
  ictx_management_lock.put_read();

  if (!tree) {
    ldout(cct, 1) << __func__ << " ictx is NULL, discard this write op" << dendl;
    return ;
  }

  uint64_t start_padding = offset % page_length;
  uint64_t align_offset = offset - start_padding;
  size_t zeroed = 0, end_len = len + start_padding, copied;
  uint32_t num_pages = (len + offset - align_offset) / page_length;
  if ((offset + len) % page_length)
    ++num_pages;

  Page *pages[num_pages];
  bool hit[num_pages];
  Mutex::Locker l1(tree_lock);
  int r = get_pages(ictx_id, tree, NULL, pages, hit, num_pages, align_offset, true);
  assert(r == 0);
  Mutex::Locker l2(data_lock);
  uint64_t v = global_version++;
  for (uint64_t i = 0; i < num_pages; ++i) {
    if (hit[i]) {
      copied = MIN(end_len - zeroed, page_length);
      memset(pages[i]->addr + start_padding, 0, copied);
      pages[i]->version = v;
      ldout(cct, 20) << __func__ << " zero(" << pages[i]->offset + start_padding << ", "
                     << copied << ")" << dendl;
      zeroed += copied;
    } else {
      zeroed += page_length;
    }
    if (zeroed == end_len)
      break;
    start_padding = 0;
  }
}

void BlockCacher::purge(uint64_t ictx_id)
{
  ldout(cct, 20) << __func__ << " ictx=" << ictx_id << dendl;

  // Don't need to clear car_state's page
  ictx_management_lock.get_read();
  PageRBTree *tree = registered_tree[ictx_id];
  ictx_management_lock.put_read();
  Mutex::Locker l(tree_lock);
  if (tree)
    tree->clear();
}
}
