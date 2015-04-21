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
#include "common/dout.h"
#include "common/errno.h"
#include "BlockCacher.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "BlockCacher: "

int BlockCacher::reg_region(uint64_t size)
{
  assert(size >= CEPH_PAGE_SIZE);
  ldout(cct, 10) << __func__ << " size=" << size << dendl;
  regions.resize(regions.size()+1);
  Region &region = regions.back();
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
  ldout(cct, 10) << __func__ << " r=" << r << dendl;
  return r;
}

void BlockCacher::ARCState::evict_page(Page *page)
{
  ldout(cct, 10) << __func__ << dendl;
  bool found = false;
  Page *p;
  do {
    if (arc_list_size[ARC_LRU] >= arc_lru_limit) {
      p = arc_list_foot[ARC_LRU];
      if (p->reference) {
        move_page(p, ARC_LFU);
      } else if (p->flags & PAGE_PINED) {
        // refresh
        // TODO: perf counter
        move_page(p, ARC_LFU);
      } else {
        found = true;
        page->addr = p->addr;
        move_page(p, ARC_LRU_GHOST);
      }
    } else {
      p = arc_list_foot[ARC_LFU];
      if (p->reference) {
        move_page(p, ARC_LFU);
      } else if (p->flags & PAGE_PINED) {
        // refresh
        // TODO: perf counter
        move_page(p, ARC_LRU);
      } else {
        found = true;
        page->addr = p->addr;
        move_page(p, ARC_LFU_GHOST);
      }
    }
  } while (!found);

  ldout(cct, 10) << __func__ << " " << *p << dendl;
}

BlockCacher::BlockCacher(CephContext *c, uint64_t cache_size, uint64_t unit, uint64_t region_units):
    cct(c), total_pages(NULL), free_pages_head(NULL), free_data_pages_head(NULL), pin_pages(0),
    arc_state(c, cache_size/unit),
    region_maxpages(region_units), page_length(unit), can_alloc_pages(cache_size/unit), max_pages(cache_size/unit)
{
  int r = ::posix_memalign((void**)&total_pages, CEPH_PAGE_SIZE, max_pages*2*page_length);
  assert(r);
  memset(total_pages, 0, max_pages*page_length*2);
  Page *p = total_pages;
  for (uint64_t i = 0; i < max_pages*2; ++i) {
    p->arc_next = free_pages_head;
    free_pages_head = p;
    ++p;
  }
}

BlockCacher::~BlockCacher()
{
  for (vector<Region>::iterator it = regions.begin(); it != regions.end(); ++it)
    free(it->addr);
  regions.clear();
  free(total_pages);
}

// https://dl.dropboxusercontent.com/u/91714474/Papers/clockfast.pdf
int BlockCacher::get_pages(uint64_t ictx_id, Page **pages, size_t page_size, size_t align_offset)
{
  ldout(cct, 10) << __func__ << " " << page_size << " pages, align_offset=" << align_offset << dendl;

  if (page_size >= can_alloc_pages + max_pages - pin_pages) {
    ldout(cct, 0) << __func__ << " can't provide with enough pages" << dendl;
    return -EAGAIN;
  }

  PageRBTree *tree = registered_ictx[ictx_id];
  RBTree::Iterator ictx_it = tree->lower_bound(align_offset), end = tree->end(), ictx_prev = ictx_it;
  RBTree::Iterator ghost_it = arc_state.ghost_trees[ictx_id]->lower_bound(align_offset);
  int hit_ghost_history;  // 0 is hit LRU_GHOST, 1 is hit LFU_GHOST, 2 is not hit
  bool move_rbnode;
  Page *cur_page, *ghost_page;
  if (ictx_it == end)
    ictx_prev = tree->last();
  else
    --ictx_prev;
  for (size_t idx = 0, pos = align_offset; idx < page_size; pos += page_length, idx++) {
    if (ictx_it != end) {
      cur_page = ictx_it->get_container<Page>(offsetof(Page, rb));
      if (cur_page->offset == pos) {
        // Yeah, fast path, we hit page!
        hit_page(cur_page);
        ictx_prev = ictx_it;
        ++ictx_it;
        goto got_page;
      }
      assert(cur_page->offset > pos);
    }

    // cache miss
    hit_ghost_history = 2;
    move_rbnode = true;
    if (ghost_it != end) {
      ghost_page = ghost_it->get_container<Page>(offsetof(Page, rb));
      if (ghost_page->offset == pos) {
        ldout(cct, 20) << __func__ << " hit history " << *ghost_page << dendl;
        hit_ghost_history = ghost_page->arc_idx;
        ++ghost_it;
      }
      assert(ghost_page->offset > pos);
    }

    if (free_data_pages_head) {
      if (!can_alloc_pages) {
        // Cache is full
        if (hit_ghost_history == 2) {
          cur_page = free_pages_head;
          free_pages_head = free_pages_head->arc_next;
        } else if (arc_state.arc_list_size[ARC_LRU] + arc_state.arc_list_size[ARC_LRU_GHOST] == max_pages) {
          cur_page = arc_state.arc_list_foot[ARC_LRU_GHOST];
          arc_state.remove_page(cur_page);
        } else if (arc_state.arc_list_size[ARC_LRU] + arc_state.arc_list_size[ARC_LRU_GHOST] +
                   arc_state.arc_list_size[ARC_LFU] + arc_state.arc_list_size[ARC_LFU_GHOST] == max_pages * 2) {
          cur_page = arc_state.arc_list_foot[ARC_LFU_GHOST];
          arc_state.remove_page(cur_page);
        } else {
          assert(0);
        }
        assert(cur_page && !cur_page->addr);
        /* cache full, got a page data from cache */
        arc_state.evict_page(cur_page);
        // If the evicted page is the previous node of ictx_it, we don't need to
        // move node
        if (cur_page->offset == ictx_prev->get_container<Page>(offsetof(Page, rb))->offset)
          move_rbnode = false;
        else
          tree->erase(cur_page);
      } else {
        uint32_t rps = MIN(can_alloc_pages, region_maxpages);
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
    if (hit_ghost_history == ARC_LRU) {
      /* cache directory hit */
      arc_state.arc_lru_limit = MIN(arc_state.arc_lru_limit + arc_state.arc_list_size[ARC_LRU_GHOST]/arc_state.arc_list_size[ARC_LFU_GHOST], max_pages);
      arc_state.move_page(cur_page, ARC_LFU);
    } else if (hit_ghost_history == ARC_LFU) {
      /* cache directory hit */
      uint32_t difference = arc_state.arc_list_size[ARC_LRU_GHOST]/arc_state.arc_list_size[ARC_LFU_GHOST];
      arc_state.arc_lru_limit = arc_state.arc_lru_limit > difference ? arc_state.arc_lru_limit - difference : 0;
      arc_state.move_page(cur_page, ARC_LFU);
    } else {
      /* cache directory miss */
      arc_state.move_page(cur_page, ARC_LRU);
    }

    if (move_rbnode)
      tree->insert(cur_page);

 got_page:
    cur_page->flags = 0;
    cur_page->offset = pos;
    pin_page(cur_page);
    pages[idx] = cur_page;
  }

  return 0;
}

// Cache Hit
void BlockCacher::hit_page(Page *page)
{
  ldout(cct, 20) << __func__ << " " << *page << dendl;
  page->reference = 1;
}
