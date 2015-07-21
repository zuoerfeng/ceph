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
#include <stdlib.h>
#include <string.h>
#include "include/Spinlock.h"
#include "common/RBTree.h"
#include "common/Mutex.h"
#include "librbd/AioCompletion.h"
#include "librbd/AioRequest.h"

#define POSITION_NO 0
#define POSITION_FREE 1
#define POSITION_ARC 2
#define POSITION_DIRTY 3
#define POSITION_DIRTY_FETCH 4
#define POSITION_READ_INFLIGHT 5
#define POSITION_WRITE_INFLIGHT 6

struct PagePatch {
  uint16_t offset;
  uint16_t len;
};

struct PageNode {
  PagePatch patch;
  PageNode *next;
};

#define PAGE_PATCH_UNIT_SHIFT (9)
#define PAGE_PATCH_UNIT (1 << PAGE_PATCH_UNIT_SHIFT)
#define PAGE_PATCH_UNIT_MASK (~(PAGE_PATCH_UNIT - 1))
#define TO_PAGE_UNIT(raw) ((raw) >> PAGE_PATCH_UNIT_SHIFT)
#define PAGE_UNIT_TO_REAL(unit) ((unit) << PAGE_PATCH_UNIT_SHIFT)

#define DIRTY_MODE_NONE 0
#define DIRTY_MODE_COMPLETE 1
#define DIRTY_MODE_INCOMPLETE 2

struct Page {
  RBNode rb;
  uint64_t offset;  // protected by "tree_lock"
  uint16_t ictx_id; // protected by "tree_lock"
  // When becoming *_GHOST Page, NULL. is assigned. When translate from *_GHOST Page,
  // data address is assigned(only replace)
  uint8_t reference;// protected by "car_state"
  uint8_t arc_idx;// protected by "car_state"
  uint8_t dirty_mode;  // is protected by "dirty_page_lock"
  uint8_t inline_num_patches; // is protected by "dirty_page_lock"
  uint8_t position;
  char *addr;       // is protected by "data_lock"
  uint64_t version; // is protected by "data_lock"
  Page *page_prev, *page_next;
#define INLINE_MAX_PATCHES 4
  union {
    PagePatch inline_patches[INLINE_MAX_PATCHES];
    struct {
      PageNode *patch_head;
      uint64_t num_linked_patches;
    } linked_patches;
  } patches;

  static bool merge_inline_patch(
      uint16_t offset, uint16_t len, PagePatch inline_patches[], uint8_t &inline_num_patches) {
    // [blank][large][middle][small]
    //        |<- start from here
    for (uint8_t i = INLINE_MAX_PATCHES - inline_num_patches; i < INLINE_MAX_PATCHES; ++i) {
      uint16_t patch_end = inline_patches[i].offset + inline_patches[i].len;
      uint16_t add_end = offset + len;
      if (offset > patch_end) {
        // |------------- patch -----------|
        //                                    |-- len --|
        // OR this is the last time, we need to migrate to get a new slot
        if (inline_num_patches < INLINE_MAX_PATCHES)
          memmove(&inline_patches[INLINE_MAX_PATCHES-inline_num_patches],
                  &inline_patches[INLINE_MAX_PATCHES-inline_num_patches-1],
                  (i-(INLINE_MAX_PATCHES-inline_num_patches))*sizeof(PagePatch));
        else
          return false;
        inline_patches[i-1].offset = offset;
        inline_patches[i-1].len = len;
        ++inline_num_patches;
      } else if (offset >= inline_patches[i].offset) {
        if (add_end >= patch_end) {
          // |------------- patch -----------|
          //                         |-- len --|
          inline_patches[i].len = patch_end + (add_end - patch_end);
        } else if (offset + len < patch_end) {
          // |------------- patch -----------|
          //                   |-- len --|
          // no need to change
        }
      } else if (add_end >= inline_patches[i].offset) {
        //      |------------- patch -----------|
        // |-- len --|
        inline_patches[i].len += inline_patches[i].offset - offset;
        inline_patches[i].offset = offset;
        if (i + 1 < INLINE_MAX_PATCHES &&
            inline_patches[i+1].offset + inline_patches[i+1].len > offset) {
          // |--- patch[i+1] ---|     |------- patch -------|
          //                  |-- len --|
          inline_patches[i].offset = inline_patches[i+1].offset;
          inline_patches[i].len = patch_end - inline_patches[i+1].offset;
          memmove(&inline_patches[INLINE_MAX_PATCHES-inline_num_patches],
                  &inline_patches[INLINE_MAX_PATCHES-inline_num_patches+1],
                  (i-(INLINE_MAX_PATCHES-inline_num_patches)+1)*sizeof(PagePatch));
        }
      } else {
        assert(add_end < patch_end);
        continue;
      }
      return true;
    }
    if (inline_num_patches < INLINE_MAX_PATCHES) {
      if (inline_num_patches)
        memmove(&inline_patches[INLINE_MAX_PATCHES-inline_num_patches],
                &inline_patches[INLINE_MAX_PATCHES-inline_num_patches-1],
                inline_num_patches*sizeof(PagePatch));
      inline_patches[INLINE_MAX_PATCHES-1].offset = offset;
      inline_patches[INLINE_MAX_PATCHES-1].len = len;
      ++inline_num_patches;
      return true;
    }
    return false;
  }
  static void merge_linked_patch(uint16_t offset, uint16_t len, PageNode **head, uint64_t &nodes) {
    // [blank][large][middle][small]
    //        |<- start from here
    uint16_t patch_end, add_end;
    PageNode *next, *node = *head;
    while (node) {
      patch_end = node->patch.offset + node->patch.len;
      add_end = node->patch.offset + node->patch.len;
      next = node->next;
      if (offset > patch_end) {
        // |------------- patch -----------|
        //                                    |-- len --|
        break;
      } else if (offset >= node->patch.offset) {
        if (add_end >= patch_end) {
          // |------------- patch -----------|
          //                         |-- len --|
          node->patch.len = patch_end + (add_end - patch_end);
        } else if (offset + len < patch_end) {
          // |------------- patch -----------|
          //                   |-- len --|
          // no need to change
        }
      } else if (add_end >= node->patch.offset) {
        //      |------------- patch -----------|
        // |-- len --|
        node->patch.len += node->patch.offset - offset;
        node->patch.offset = offset;
        if (next && next->patch.offset + next->patch.len > offset) {
          // |--- next ---|     |------- patch ------|
          //             |-- len --|
          node->patch.offset = next->patch.offset;
          node->patch.len = patch_end - next->patch.offset;
          node->next = next->next;
          delete next;
        }
      } else {
        assert(add_end < patch_end);
        node = next;
        continue;
      }
      return ;
    }
    PageNode *new_node = new PageNode;
    new_node->patch.offset = offset;
    new_node->patch.len = len;
    new_node->next = *head;
    *head = new_node;
    ++nodes;
  }
  bool add_patch(uint64_t real_offset, uint64_t real_len, uint64_t real_page_length) {
    uint16_t offset, len, page_length;
    if (!real_offset && !real_len)
      return false;
    if (real_len == real_page_length) {
      assert(!real_offset);
      goto cleanup;
    }
    offset = TO_PAGE_UNIT(real_offset);
    len = TO_PAGE_UNIT(real_len);
    page_length = TO_PAGE_UNIT(real_page_length);
    // A hint for seq read/write
    if (inline_num_patches <= INLINE_MAX_PATCHES &&
        merge_inline_patch(offset, len, patches.inline_patches, inline_num_patches)) {
      if (inline_num_patches == 1 && patches.inline_patches[INLINE_MAX_PATCHES-1].offset == 0 && patches.inline_patches[INLINE_MAX_PATCHES-1].len == page_length)
        goto cleanup;
    } else if (inline_num_patches == INLINE_MAX_PATCHES) {
      PageNode *prev = NULL;
      for (int i = 0; i < INLINE_MAX_PATCHES; ++i) {
        PageNode *node = new PageNode;
        node->patch = patches.inline_patches[i];
        node->next = prev;
        prev = node;
      }
      patches.linked_patches.patch_head = prev;
      patches.linked_patches.num_linked_patches = inline_num_patches;
      ++inline_num_patches;
    } else {
      merge_linked_patch(offset, len, &patches.linked_patches.patch_head, patches.linked_patches.num_linked_patches);
      PageNode *node = patches.linked_patches.patch_head;
      if (patches.linked_patches.num_linked_patches == 1 &&
          node->patch.offset == 0 && node->patch.len == page_length)
        goto cleanup;
    }

    return false;

   cleanup:
    if (inline_num_patches > INLINE_MAX_PATCHES) {
      PageNode *prev, *node = patches.linked_patches.patch_head;
      while (node) {
        prev = node;
        node = node->next;
        delete prev;
      }
    }
    memset(&patches, 0, sizeof(patches));
    inline_num_patches = 0;
    return true;
  }
  void fill_rest(const char *data, uint64_t real_offset, uint64_t real_len) {
    uint16_t data_off = 0;
    uint16_t start = TO_PAGE_UNIT(real_offset);
    uint16_t end = start + TO_PAGE_UNIT(real_len);
    if (inline_num_patches <= INLINE_MAX_PATCHES) {
      for (uint8_t i = INLINE_MAX_PATCHES - 1;
           i >= INLINE_MAX_PATCHES - inline_num_patches; ++i) {
        if (start < patches.inline_patches[i].offset) {
          if (end < patches.inline_patches[i].offset) {
            if (data)
              memcpy(addr+PAGE_UNIT_TO_REAL(start),
                     data+PAGE_UNIT_TO_REAL(data_off),
                     PAGE_UNIT_TO_REAL(end - start));
            else
              memset(addr+PAGE_UNIT_TO_REAL(start), 0,
                     PAGE_UNIT_TO_REAL(end - start));
            start = end;
            data_off += end - start;
            break;
          }
          if (data)
            memcpy(addr+PAGE_UNIT_TO_REAL(start),
                   data+PAGE_UNIT_TO_REAL(data_off),
                   PAGE_UNIT_TO_REAL(patches.inline_patches[i].offset - start));
          else
            memset(addr+PAGE_UNIT_TO_REAL(start), 0,
                   PAGE_UNIT_TO_REAL(patches.inline_patches[i].offset - start));
          start = patches.inline_patches[i].offset + patches.inline_patches[i].len;
          data_off += patches.inline_patches[i].offset - start;
        }
        data_off += patches.inline_patches[i].offset + patches.inline_patches[i].len;
      }
    } else {
      PageNode *node = patches.linked_patches.patch_head;
      while (node) {
        if (start < node->patch.offset) {
          if (end < node->patch.offset) {
            if (data)
              memcpy(addr+PAGE_UNIT_TO_REAL(start),
                     data+PAGE_UNIT_TO_REAL(data_off),
                     PAGE_UNIT_TO_REAL(end - start));
            else
              memset(addr+PAGE_UNIT_TO_REAL(start), 0,
                     PAGE_UNIT_TO_REAL(end - start));
            start = end;
            data_off += end - start;
            break;
          }
          if (data)
            memcpy(addr+PAGE_UNIT_TO_REAL(start),
                   data+PAGE_UNIT_TO_REAL(data_off),
                   PAGE_UNIT_TO_REAL(node->patch.offset - start));
          else
            memset(addr+PAGE_UNIT_TO_REAL(start), 0,
                   PAGE_UNIT_TO_REAL(node->patch.offset - start));
          start = node->patch.offset + node->patch.len;
          data_off += node->patch.offset - start;
        }
        data_off += node->patch.offset + node->patch.len;
        node = node->next;
      }
    }
    if (start < end) {
      if (data)
        memcpy(addr+PAGE_UNIT_TO_REAL(start),
               data+PAGE_UNIT_TO_REAL(data_off),
               PAGE_UNIT_TO_REAL(end - start));
      else
        memset(addr+PAGE_UNIT_TO_REAL(start), 0,
               PAGE_UNIT_TO_REAL(end - start));
    }
  }
};

inline std::ostream& operator<<(std::ostream& out, const Page& page) {
  out << " Page(state=" << page.arc_idx << ", offset=" << page.offset << ", reference=" << page.reference;
  return out;
}


namespace librbd {
  class ImageCtx;
  class PageRBTree {
    RBTree root;

   public:
    RBTree::Iterator lower_bound(uint64_t offset) {
      RBNode *node = root.rb_node, *parent = NULL;
      Page *page = NULL;

      while (node) {
        page = node->get_container<Page>(offsetof(Page, rb));
        if (offset <= page->offset) {
          parent = node;
          node = node->rb_left;
        } else if (offset > page->offset) {
          node = node->rb_right;
        }
      }
      return RBTree::Iterator(parent);
    }
    RBTree::Iterator end() {
      return root.end();
    }
    RBTree::Iterator last() {
      RBTree::Iterator it = root.last();
      return it;
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
    void clear() {
      root.rb_node = NULL;
    }
  };

  class CARState {
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
    uint32_t data_pages;
    Mutex lock;

    Page* _pop_head_page(uint8_t arc_idx) {
      Page *p = arc_list_head[arc_idx];
      if (!p)
        return NULL;
      if (p->page_next)
        p->page_next->page_prev = NULL;
      arc_list_head[arc_idx] = p->page_next;
      p->page_next = p->page_prev = NULL;
      // no element
      if (!arc_list_head[arc_idx])
        arc_list_foot[arc_idx] = NULL;
      --arc_list_size[arc_idx];
      p->arc_idx = ARC_COUNT;
      return p;
    }
    void _append_page(Page *page, uint8_t dst) {
      assert(page->dirty_mode == DIRTY_MODE_NONE && !page->page_next && !page->page_prev);
      page->arc_idx = dst;
      if (arc_list_foot[dst])
        arc_list_foot[dst]->page_next = page;
      page->page_prev = arc_list_foot[dst];
      arc_list_foot[dst] = page;
      if (!arc_list_head[dst])
        arc_list_head[dst] = page;
      page->reference = 0;
      ++arc_list_size[dst];
    }
    void _remove_page(Page *p) {
      --arc_list_size[p->arc_idx];
      if (p->page_prev)
        p->page_prev->page_next = p->page_next;
      else
        arc_list_head[p->arc_idx] = p->page_next;
      if (p->page_next)
        p->page_next->page_prev = p->page_prev;
      else
        arc_list_foot[p->arc_idx] = p->page_prev;
      p->page_prev = p->page_next = NULL;
    }

   public:
    CARState(CephContext *c): cct(c), arc_lru_limit(0), data_pages(0),
                              lock("BlockCacher::CARState::lock") {
      for (int i = ARC_LRU; i < ARC_COUNT; ++i) {
        arc_list_head[i] = NULL;
        arc_list_foot[i] = NULL;
        arc_list_size[i] = 0;
      }
    }
    ~CARState() {
    }
    void hit_page(Page *p) {
      p->reference = 1;
    }
    Page* get_ghost_page(Page *ghost_page) {
      Mutex::Locker l(lock);
      Page *p = NULL;
      if (ghost_page) {
        _remove_page(ghost_page);
        p = ghost_page;
      } else if (arc_list_size[ARC_LRU] + arc_list_size[ARC_LRU_GHOST] == data_pages) {
        p = _pop_head_page(ARC_LRU_GHOST);
      } else if (arc_list_size[ARC_LRU] + arc_list_size[ARC_LFU] +
                 arc_list_size[ARC_LRU_GHOST] + arc_list_size[ARC_LFU_GHOST] == data_pages * 2) {
        p = _pop_head_page(ARC_LFU_GHOST);
      }
      return p;
    }
    Page* evict_data();
    void set_lru_limit(uint32_t s) { arc_lru_limit = s; }
    void set_data_pages(uint32_t s) { data_pages = s; }

    void insert_page(Page *page) {
      Mutex::Locker l(lock);
      // we already increase size in adjust/make_dirty call, we need to decrease one now
      assert(page->arc_idx < ARC_COUNT);
      --arc_list_size[page->arc_idx];
      page->position = POSITION_ARC;
      _append_page(page, page->arc_idx);
    }

    void adjust_and_hold(Page *cur_page, int hit_ghost_history);
    void make_dirty(Page *page) {
      Mutex::Locker l(lock);
      _remove_page(page);
      ++arc_list_size[page->arc_idx];
    }
    // Test Purpose
    bool is_page_in_or_inflight(Page *page) {
      assert(page->arc_idx != ARC_COUNT);
      Mutex::Locker l(lock);
      Page *p = arc_list_head[page->arc_idx];
      while (p) {
        if (p == page)
          return true;
        p = p->page_next;
      }
      return false;
    }
    bool is_full() {
      Mutex::Locker l(lock);
      return arc_list_size[ARC_LRU] + arc_list_size[ARC_LFU] == data_pages;
    }
    bool validate() {
      Mutex::Locker l(lock);
      if (arc_list_size[ARC_LRU] + arc_list_size[ARC_LFU] > data_pages)
        return false;
      if (arc_list_size[ARC_LRU] + arc_list_size[ARC_LRU_GHOST] > data_pages)
        return false;
      if (arc_list_size[ARC_LFU] + arc_list_size[ARC_LFU_GHOST] > data_pages * 2)
        return false;
      if (arc_list_size[ARC_LRU] + arc_list_size[ARC_LFU] +
          arc_list_size[ARC_LRU_GHOST] + arc_list_size[ARC_LFU_GHOST] > data_pages * 2)
        return false;
      if (arc_list_size[ARC_LRU] + arc_list_size[ARC_LFU] < data_pages) {
        if (arc_list_size[ARC_LRU_GHOST] + arc_list_size[ARC_LFU_GHOST] != 0)
          return false;
      }
      if (arc_list_size[ARC_LRU] + arc_list_size[ARC_LFU] +
          arc_list_size[ARC_LRU_GHOST] + arc_list_size[ARC_LFU_GHOST] > data_pages) {
        if (arc_list_size[ARC_LRU] + arc_list_size[ARC_LFU] != data_pages)
          return false;
      }
      return true;
    }
  };

  class BlockCacherCompletion {
    public:
      BlockCacherCompletion(Context *c) : count(1), rval(0), ctxt(c) { }
      virtual ~BlockCacherCompletion() {}
      void add_request() { count.inc(); }
      void complete_request(int r);
      void finish_adding_requests() { complete_request(0); }
    private:
      Spinlock lock;
      atomic_t count;
      ssize_t rval;
      Context *ctxt;
  };

  class C_AioRead2 : public Context {
    public:
      C_AioRead2(CephContext *cct, AioCompletion *completion)
        : m_cct(cct), m_completion(completion) { }
      virtual ~C_AioRead2() {}
      virtual void finish(int r);
    private:
      CephContext *m_cct;
      AioCompletion *m_completion;
  };

  class MockThread : public Thread {
   public:
    virtual void queue_read(AioRead *r, string &oid) = 0;
    virtual void queue_write(AioWrite *w, string &oid) = 0;
    virtual void stop() = 0;
  };

  class BlockCacher {
    struct Region {
      void *addr;
      uint64_t length;
    };

    MockThread *mock_thread;
    CephContext *cct;
    Mutex tree_lock;
    Cond tree_cond;
    RWLock ictx_management_lock;
    int ictx_next;
    map<ImageCtx*, int> ictx_ids;
    vector<ImageCtx*> registered_ictx;
    vector<PageRBTree*> registered_tree;
    vector<PageRBTree*> ghost_trees;

    // Immutable
    Page *all_pages;
    uint32_t total_half_pages;
    uint32_t region_maxpages;
    uint32_t page_length;
    uint32_t max_writing_pages;

    // protect by tree_lock
    uint32_t remain_data_pages;
    vector<Region> regions;
    Page *free_pages_head;
    Page *free_data_pages_head;
    size_t num_free_data_pages;
    bool get_page_wait;
    uint32_t inflight_reading_pages; // If page is inflight, it won't in car_state

    Mutex data_lock;
    uint64_t global_version;

    CARState car_state;

    atomic_t inflight_writing_pages;
    Mutex dirty_page_lock;
    struct DirtyPageState {
      BlockCacher *block_cacher;
      bool wt;
      Page *complete_pages_head, *complete_pages_foot;
      Mutex incomplete_pages_lock;
      Page *incomplete_pages_head, *incomplete_pages_foot;
      atomic_t num_dirty_pages; // contains full dirty page and incomplete dirty pages
      // indicate the number of total incomplete_pages, contains
      // "incomplete_pages_head" and inflight read pages
      atomic_t num_incomplete_pages;
      uint32_t target_pages;
      uint32_t max_dirty_pages;   // if 0 means writethrough
      uint32_t page_length;
      utime_t max_dirty_age;
      bool stopping;
      DirtyPageState(BlockCacher *bc):
        block_cacher(NULL), wt(true), complete_pages_head(NULL), complete_pages_foot(NULL),
        incomplete_pages_lock("BlockCacher::DirtyPageState::incomplete_pages_lock"),
        num_dirty_pages(0), num_incomplete_pages(0),
        target_pages(0), max_dirty_pages(0), page_length(0), stopping(false) {}
      bool writethrough() const { return wt || !max_dirty_pages; }
      bool need_writeback() const { return num_dirty_pages.read() > target_pages; }
      uint32_t need_writeback_pages() const {
        return num_dirty_pages.read() > target_pages ? num_dirty_pages.read() - target_pages : 0;
      }
      void mark_dirty(Page *p, uint64_t offset, uint64_t len) {
        if (p->dirty_mode == DIRTY_MODE_NONE) {
          if (len == page_length) {
            p->dirty_mode = DIRTY_MODE_COMPLETE;
          } else {
            p->dirty_mode = DIRTY_MODE_INCOMPLETE;
            num_incomplete_pages.inc();
          }
          num_dirty_pages.inc();
        } else if (p->dirty_mode == DIRTY_MODE_COMPLETE) {
          if (p->page_prev)
            p->page_prev->page_next = p->page_next;
          else
            complete_pages_head = p->page_next;
          if (p->page_next)
            p->page_next->page_prev = p->page_prev;
          else
            complete_pages_foot = p->page_prev;
          p->page_prev = p->page_next = NULL;
        } else { // assert(p->dirty_mode == DIRTY_MODE_NONE);
          // Doesn't support unaligned io size
          if (offset & ~PAGE_PATCH_UNIT_MASK || len & ~PAGE_PATCH_UNIT_MASK)
            assert(0 == "unaligned io size");
          if (p->add_patch(offset, len, page_length)) {
            p->dirty_mode = DIRTY_MODE_COMPLETE;
            num_incomplete_pages.dec();
            if (stopping) {
              Mutex::Locker l(block_cacher->flush_lock);
              block_cacher->flush_cond.Signal();
            }
          } else {
            if (p->page_prev)
              p->page_prev->page_next = p->page_next;
            else
              incomplete_pages_head = p->page_next;
            if (p->page_next)
              p->page_next->page_prev = p->page_prev;
            else
              incomplete_pages_foot = p->page_prev;
            p->page_prev = p->page_next = NULL;
          }
        }
        p->position = POSITION_DIRTY;
        assert(!p->page_prev && !p->page_next);
        if (p->dirty_mode == DIRTY_MODE_COMPLETE) {
          p->page_prev = complete_pages_foot;
          if (complete_pages_foot)
            complete_pages_foot->page_next = p;
          if (!complete_pages_head)
            complete_pages_head = p;
          complete_pages_foot = p;
        } else {
          Mutex::Locker l(incomplete_pages_lock);
          p->page_prev = incomplete_pages_foot;
          if (incomplete_pages_foot)
            incomplete_pages_foot->page_next = p;
          if (!incomplete_pages_head)
            incomplete_pages_head = p;
          incomplete_pages_foot = p;
        }
      }
      uint32_t writeback_pages(map<uint16_t, map<uint64_t, Page*> > &sorted_flush, uint32_t num) {
        uint32_t i = 0;
        Page *p = complete_pages_head, *prev;
        while (p) {
          sorted_flush[p->ictx_id][p->offset] = p;
          prev = p;
          p = p->page_next;
          prev->dirty_mode = DIRTY_MODE_NONE;
          prev->page_next = prev->page_prev = NULL;
          i++;
          if (num && i >= num)
            break;
        }
        complete_pages_head = p;
        if (complete_pages_head)
          complete_pages_head->page_prev = NULL;
        else
          complete_pages_foot = NULL;
        return i;
      }
      uint32_t get_incomplete_pages(map<uint16_t, map<uint64_t, Page*> > &pages, uint32_t size) {
        Mutex::Locker l(incomplete_pages_lock);
        Page *p = incomplete_pages_head;
        Page *prev = p;
        uint32_t i = 0;
        while (i < size && p) {
          pages[p->ictx_id][p->offset] = p;
          p->page_next = p->page_prev = NULL;
        }
        incomplete_pages_head = p;
        if (incomplete_pages_head)
          incomplete_pages_head->page_prev = NULL;
        else
          incomplete_pages_foot = NULL;
        return i;
      }
      void set_writeback() {
        wt = false;
      }
    } dirty_page_state;

    class C_BlockCacheRead : public Context {
    public:
      BlockCacher *block_cacher;
      BlockCacherCompletion *comp;
      ObjectPage extent;
      uint64_t start, end;
      char *start_buf;
      AioRead *req;
      uint64_t version;

      C_BlockCacheRead(BlockCacher *bc, BlockCacherCompletion *c, ObjectPage &e,
                       uint64_t o, size_t l, char *b, uint64_t v):
          block_cacher(bc), comp(c), extent(e), start(o), end(o+l), start_buf(b), version(v) {
        comp->add_request();
      }
      virtual ~C_BlockCacheRead() {}
      virtual void finish(int r) {
        if (start_buf)
          block_cacher->complete_read(this, r);
        else
          block_cacher->complete_async_fetch(this, r);
      }
    };
    friend class C_BlockCacherRead;

    class C_BlockCacheWrite : public Context {
     public:
      BlockCacher *block_cacher;
      BlockCacherCompletion *comp;
      ImageCtx *ictx;
      ObjectPage extent;
      bufferlist data;
      uint64_t flush_id;
      uint64_t version;

      C_BlockCacheWrite(BlockCacher *bc, BlockCacherCompletion *c, ImageCtx *ctx, ObjectPage &e,
                        uint64_t fid, uint64_t v):
          block_cacher(bc), comp(c), ictx(ctx), extent(e), flush_id(fid), version(v) {
        comp->add_request();
      }
      virtual ~C_BlockCacheWrite() {}
      virtual void finish(int r) {
        block_cacher->complete_write(this, r);
      }
      void send_by_bc_write_comp(SnapContext &snapc)
      {
        AioWrite *req = new AioWrite(ictx, extent.oid.name, extent.objectno, extent.offset,
                                     data, snapc, this);
        if (block_cacher->mock_thread)
          block_cacher->mock_thread->queue_write(req, extent.oid.name);
        else
          req->send();
      }
    };
    friend class C_BlockCacherWrite;

    class C_AsyncFetch : public Context {
      BlockCacher *block_cacher;
      Context *c;

     public:
      C_AsyncFetch(BlockCacher *bc, Context *c): block_cacher(bc), c(c) {}
      virtual ~C_AsyncFetch() {}
      virtual void finish(int r) {
        if (c)
          c->complete(r);
      }
    };
    friend class C_AsyncFetch;

    class C_FlushWrite : public Context {
      BlockCacher *block_cacher;
      Context *c;

     public:
      C_FlushWrite(BlockCacher *bc, Context *c): block_cacher(bc), c(c) {}
      virtual ~C_FlushWrite() {}
      virtual void finish(int r) {
        if (c)
          c->complete(r);
        Mutex::Locker l(block_cacher->flush_lock);
        block_cacher->flush_cond.Signal();
      }
    };
    friend class C_FlushWrite;

    Cond flush_cond;
    Mutex flush_lock;
    bool flusher_stop;
    uint64_t flush_id;
    map<uint64_t, pair<uint64_t, Context*> > flush_commits;
    std::list<Context*> wait_writeback;
    bool max_writing_wait;

    class FlusherThread : public Thread {
      BlockCacher *block_cacher;
     public:
      FlusherThread(BlockCacher *bc): block_cacher(bc) {}
      void *entry() {
        block_cacher->flusher_entry();
        return 0;
      }
    } flusher_thread;
    void flush_continuous_pages(ImageCtx *ictx, Page **pages, size_t page_size, Context *c,
        ::SnapContext &snapc);
    void flush_pages(uint32_t num, Context *c);
    void flusher_entry();
    void flush_object_extent(ImageCtx *ictx, map<object_t, vector<ObjectPage> > &object_extents,
                             BlockCacherCompletion *c, ::SnapContext &snapc, uint64_t v);
    void async_read_partial_pages(map<uint16_t, map<uint64_t, Page*> > &pages, BlockCacherCompletion *comp);

    int reg_region(uint64_t num_pages);
    void complete_read(C_BlockCacheRead *bc_read_comp, int r);
    void complete_async_fetch(C_BlockCacheRead *bc_read_comp, int r);
    void complete_write(C_BlockCacheWrite *bc_write_comp, int r);
    void prepare_continuous_pages(ImageCtx *ictx, map<uint64_t, Page*> &flush_pages,
                                  map<object_t, vector<ObjectPage> > &object_extents);
    int get_pages(uint16_t ictx_id, PageRBTree *tree, PageRBTree *ghost_tree, Page **pages,
                  bool hit[], size_t page_size, size_t align_offset, bool write, bool only_hit=false);
    void read_object_extents(ImageCtx *ictx, uint64_t offset, size_t len,
                             map<object_t, vector<ObjectPage> > &object_extents,
                             char *buf, BlockCacherCompletion *c, uint64_t snap_id, uint64_t v);

   public:
    BlockCacher(CephContext *c):
      mock_thread(NULL), cct(c),
      tree_lock("BlockCacher::tree_lock"),
      ictx_management_lock("BlockCacher::ictx_management_lock"), ictx_next(1), all_pages(NULL),
      total_half_pages(0), region_maxpages(0), page_length(0), max_writing_pages(0), remain_data_pages(0),
      free_pages_head(NULL), free_data_pages_head(NULL), num_free_data_pages(0), get_page_wait(false),
      inflight_reading_pages(0), data_lock("BlockCacher::data_lock"),
      global_version(1), car_state(c), inflight_writing_pages(0),
      dirty_page_lock("BlockCacher::dirty_page_lock"), dirty_page_state(this),
      flush_lock("BlockCacher::BlockCacher"), flusher_stop(true),
      flush_id(0), max_writing_wait(false), flusher_thread(this) {}

    ~BlockCacher() {
      if (flusher_thread.is_started()) {
        flush_lock.Lock();
        flusher_stop = true;
        flush_cond.Signal();
        flush_lock.Unlock();
        flusher_thread.join();
      }

      assert(flush_commits.empty());
      assert(wait_writeback.empty());
      if (mock_thread) {
        mock_thread->stop();
        mock_thread->join();
        delete mock_thread;
      }

      for (vector<ImageCtx*>::iterator it = registered_ictx.begin();
           it != registered_ictx.end(); ++it)
        assert(!*it);
      for (vector<PageRBTree*>::iterator it = registered_tree.begin();
          it != registered_tree.end(); ++it)
        assert(!*it);
      for (vector<PageRBTree*>::iterator it = ghost_trees.begin();
          it != ghost_trees.end(); ++it)
        assert(!*it);
      for (vector<Region>::iterator it = regions.begin(); it != regions.end(); ++it)
        free(it->addr);
      assert(!registered_ictx.empty());
      regions.clear();
      free(all_pages);
    }

    void init(uint64_t cache_size, uint64_t unit, uint64_t region_units,
              uint32_t target_dirty, uint32_t max_dirty, double dirty_age, MockThread *m=NULL) {
      // Don't init again if already init
      if (page_length)
        return ;
      page_length = unit;
      region_maxpages = region_units;
      total_half_pages = remain_data_pages = cache_size / unit;
      dirty_page_state.target_pages = target_dirty / unit;
      dirty_page_state.max_dirty_pages = max_dirty / unit;
      dirty_page_state.max_dirty_age.set_from_double(dirty_age);
      dirty_page_state.page_length = page_length;
      max_writing_pages = target_dirty / unit;
      car_state.set_lru_limit(remain_data_pages / 2);
      car_state.set_data_pages(remain_data_pages);

      int r = ::posix_memalign((void**)&all_pages, CEPH_PAGE_SIZE, total_half_pages*2*page_length);
      assert(!r);
      memset(all_pages, 0, total_half_pages*2*page_length);
      Page *p = all_pages;
      for (uint64_t i = 0; i < total_half_pages * 2; ++i) {
        p->page_next = free_pages_head;
        free_pages_head = p;
        p->arc_idx = ARC_COUNT;
        ++p;
      }
      flusher_stop = false;
      flusher_thread.create();
      if (m) {
        mock_thread = m;
        mock_thread->create();
      }
    }

    void read_buffer(uint64_t ictx_id, uint64_t offset, size_t len,
                     char *buf, Context *c, uint64_t snap_id, int op_flags);
    int write_buffer(uint64_t ictx_id, uint64_t off, size_t len, const char *buf,
                     Context *c, int op_flags, ::SnapContext &snapc);
    void user_flush(Context *c);
    void discard(uint64_t ictx_id, uint64_t offset, size_t len);
    int register_image(ImageCtx *ictx) {
      RWLock::WLocker l(ictx_management_lock);
      if (ictx_ids.find(ictx) != ictx_ids.end())
        return ictx_ids[ictx];
      PageRBTree *pt = new PageRBTree;
      PageRBTree *gpt = new PageRBTree;

      registered_ictx.resize(ictx_next+1);
      registered_ictx[ictx_next] = ictx;
      registered_tree.resize(ictx_next+1);
      registered_tree[ictx_next] = pt;
      ghost_trees.resize(ictx_next+1);
      ghost_trees[ictx_next] = gpt;
      ictx_ids[ictx] = ictx_next;
      return ictx_next++;
    }
    void unregister_image(ImageCtx *ictx) {
      RWLock::WLocker l(ictx_management_lock);
      map<ImageCtx*, int>::iterator it = ictx_ids.find(ictx);
      if (it == ictx_ids.end())
        return ;
      registered_ictx[it->second] = NULL;
      delete registered_tree[it->second];
      registered_tree[it->second]= NULL;
      delete ghost_trees[it->second];
      ghost_trees[it->second]= NULL;
      ictx_ids.erase(it);
    }
    // purge.  non-blocking.  violently removes dirty buffers from cache.
    void purge(uint64_t ictx_id);
    vector<int> print_usage() {
      vector<int> u;
      Page *p = all_pages;
      for (uint64_t i = 0; i < total_half_pages * 2; ++i) {
        u[p->position]++;
        ++p;
      }
      return u;
    }

    // uniq name for CephContext to distinguish differnt object
    static const string name;
  };

}

#endif
