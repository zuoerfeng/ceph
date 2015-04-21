// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2015 Haomai Wang <haomaiwang@gmail.com>
 *
 * LGPL2.1 (see COPYING-LGPL2.1) or later
 */

#ifndef CEPH_LIBRBD_BLOCKCACHE_H
#define CEPH_LIBRBD_BLOCKCACHE_H

#include <iostream>
#include <list>
#include "common/RBTree.h"

#define PAGE_PINED 1 << 0

struct Page {
  RBNode rb;
  uint64_t offset;
  uint16_t flags;
  // The following fields are managed by ARCState. When becoming
  // *_GHOST Page, NULL. is assigned. When translate from *_GHOST Page, data
  // address is assigned(only replace)
  uint8_t reference;
  uint8_t arc_idx;
  void *addr;
  Page *arc_prev, *arc_next;
};

inline std::ostream& operator<<(std::ostream& out, const Page& page) {
  out << " Page(state=" << page.arc_idx << ", offset=" << page.offset << ", reference=" << page.reference << std::endl;
  return out;
}

struct PageRBTree {
  RBTree root;

  RBTree::Iterator lower_bound(uint64_t offset) {
    RBNode *node = root.rb_node, *parent = NULL;
    Page *page = NULL;

    while (node) {
      parent = node;
      page = node->get_container<Page>(offsetof(Page, rb));

      if (offset < page->offset)
        node = node->rb_left;
      else if (offset > page->offset)
        node = node->rb_right;
      else
        return RBTree::Iterator(node);
    }

    RBTree::Iterator it(parent);
    while (page && offset > page->offset) {
      ++it;
      page = it->get_container<Page>(offsetof(Page, rb));
    }
    return it;
  }
  RBTree::Iterator end() {
    return root.end();
  }
  RBTree::Iterator last() {
    return root.last();
  }

  void insert(Page *page)
  {
    RBNode **n = &root.rb_node, *parent = NULL;
    uint64_t key = page->offset;

    while (*n) {
      parent = *n;
      if (key < parent->get_container<Page>(offsetof(Page, rb))->offset)
        n = &parent->rb_left;
      else
        n = &parent->rb_right;
    }

    root.rb_link_node(&page->rb, parent, n);
    root.insert_color(&page->rb);
  }

  void erase(Page *page) {
    root.erase(&page->rb);
  }
};

class BlockCacher {
  struct Region {
    void *addr;
    uint64_t length;
  };

  CephContext *cct;
  vector<PageRBTree*> registered_ictx;
  vector<Region> regions;
  Page *total_pages;
  Page *free_pages_head;
  Page *free_data_pages_head;
  uint64_t pin_pages;

  struct ARCState {
    CephContext *cct;
#define ARC_LRU 0
#define ARC_LFU 1
#define ARC_LRU_GHOST 2
#define ARC_LFU_GHOST 3
#define ARC_COUNT 4
    Page* arc_list_head[ARC_COUNT];
    Page* arc_list_foot[ARC_COUNT];
    uint32_t arc_list_size[ARC_COUNT];
    uint32_t arc_lru_limit;
    vector<PageRBTree*> ghost_trees;

    void evict_page(Page *page);
    void remove_page(Page *page) {
      if (page->arc_prev)
        page->arc_prev->arc_next = page->arc_next;
      else
        arc_list_head[page->arc_idx] = page->arc_next;
      if (page->arc_next)
        page->arc_next->arc_prev = page->arc_prev;
      else
        arc_list_foot[page->arc_idx] = page->arc_prev;
      arc_list_size[page->arc_idx]--;
      page->arc_idx = ARC_COUNT;
    }
    void move_page(Page *page, int dst) {
      remove_page(page);
      if (dst >= ARC_LRU_GHOST) {
        page->addr = NULL;
        // remove page from rb tree
      }

      page->arc_idx = dst;
      page->arc_next = arc_list_head[dst];
      page->arc_prev = NULL;
      page->reference = 0;
      arc_list_head[dst] = page;
    }

    ARCState(CephContext *c, uint32_t max_pages): cct(c) {
      for (int i = ARC_LRU; i < ARC_COUNT; ++i) {
        arc_list_head[i] = NULL;
        arc_list_foot[i] = NULL;
        arc_list_size[i] = 0;
        arc_lru_limit = max_pages / 2;
      }
    }
  } arc_state;

  // Constant
  uint32_t region_maxpages;
  uint32_t page_length;
  uint32_t can_alloc_pages;
  uint32_t max_pages;

  void hit_page(Page *page);
  int reg_region(uint64_t size);

 public:
  BlockCacher(CephContext *c, uint64_t cache_size, uint64_t unit, uint64_t region_units);
  BlockCacher(uint64_t unit);
  ~BlockCacher();

  int get_pages(uint64_t ictx_id, Page **pages, size_t page_size, size_t align_offset);
  void pin_page(Page *page) {
    page->flags &= PAGE_PINED;
    ++pin_pages;
  }
  void unpin_page(Page *page) {
    page->flags &= ~PAGE_PINED;
    --pin_pages;
  }
};

#endif
