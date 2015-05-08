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

#include <stdlib.h>
#include <iostream>
#include <list>
#include "include/Spinlock.h"
#include "common/RBTree.h"
#include "common/Mutex.h"
#include "librbd/AioCompletion.h"
#include "librbd/AioRequest.h"

#define PAGE_DIRTY 1 << 0

struct Page {
  RBNode rb;
  uint64_t offset;
  uint16_t ictx_id;
  uint16_t flags;
  // The following fields are managed by CARState. When becoming
  // *_GHOST Page, NULL. is assigned. When translate from *_GHOST Page, data
  // address is assigned(only replace)
  uint8_t reference;
  uint8_t arc_idx;
  uint8_t inflight; // is on reading
  char *addr;
  Page *arc_next;
  Page *dirty_or_ghost_prev, *dirty_or_ghost_next;
};

inline std::ostream& operator<<(std::ostream& out, const Page& page) {
  out << " Page(state=" << page.arc_idx << ", offset=" << page.offset << ", reference=" << page.reference << std::endl;
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
    Cond wait_cond;

    Page* _pop_head_page(uint8_t arc_idx) {
      Page *p = arc_list_head[arc_idx];
      if (arc_idx >= ARC_LRU_GHOST) {
        p->dirty_or_ghost_next->dirty_or_ghost_prev = NULL;
        arc_list_head[arc_idx] = p->dirty_or_ghost_next;
        p->dirty_or_ghost_next = p->dirty_or_ghost_prev = NULL;
      } else {
        arc_list_head[arc_idx] = p->arc_next;
        p->arc_next = NULL;
      }
      --arc_list_size[arc_idx];
      p->arc_idx = ARC_COUNT;
      return p;
    }
    void _insert_page(Page *page, uint8_t dst) {
      assert(page->arc_idx == ARC_COUNT && dst != ARC_COUNT);
      page->arc_idx = dst;
      assert(!page->dirty_or_ghost_next && !page->dirty_or_ghost_prev && !page->arc_next);
      if (dst >= ARC_LRU_GHOST) {
        page->addr = NULL;
        arc_list_foot[dst]->dirty_or_ghost_next = page;
        page->dirty_or_ghost_prev = arc_list_foot[dst];
      } else {
        arc_list_foot[dst]->arc_next = page;
      }
      arc_list_foot[dst] = page;
      page->reference = 0;
      ++arc_list_size[dst];
    }
    void _remove_ghost_page(Page *p) {
      if (p->dirty_or_ghost_prev)
        p->dirty_or_ghost_prev->dirty_or_ghost_next = p->dirty_or_ghost_next;
      else
        arc_list_head[p->arc_idx] = p->dirty_or_ghost_next;
      if (p->dirty_or_ghost_next)
        p->dirty_or_ghost_next->dirty_or_ghost_prev = p->dirty_or_ghost_prev;
      else
        arc_list_foot[p->arc_idx] = p->dirty_or_ghost_prev;
      p->arc_idx = ARC_COUNT;
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
        _remove_ghost_page(ghost_page);
        p = ghost_page;
      } else if (arc_list_size[ARC_LFU] == arc_list_size[ARC_LRU_GHOST]) {
        p = _pop_head_page(ARC_LRU_GHOST);
      } else if (arc_list_size[ARC_LRU_GHOST] + arc_list_size[ARC_LFU_GHOST] == data_pages) {
        p = _pop_head_page(ARC_LFU_GHOST);
      }
      return p;
    }
    Page* evict_data(Page *data_page) {
      Page *p;
      Mutex::Locker l(lock);
      while (true) {
        if (arc_list_size[ARC_LRU] >= arc_lru_limit) {
          p = _pop_head_page(ARC_LRU);
          if (p->reference) {
            _insert_page(p, ARC_LFU);
          } else {
            _insert_page(p, ARC_LRU_GHOST);
            break;
          }
        } else {
          p = _pop_head_page(ARC_LFU);
          if (p->reference) {
            _insert_page(p, ARC_LFU);
          } else {
            _insert_page(p, ARC_LFU_GHOST);
            break;
          }
        }
      };
      data_page->offset = p->offset;
      data_page->addr = p->addr;
      return p;
    }

    void set_lru_limit(uint32_t s) { arc_lru_limit = s; }
    void set_data_pages(uint32_t s) { data_pages = s; }

    void insert_pages(Page **pages, size_t num_pages) {
      Mutex::Locker l(lock);
      for (size_t i = 0; i < num_pages; ++i) {
        _insert_page(pages[i], pages[i]->arc_idx);
      }
    }
    void adjust_lru_limit(Page *cur_page, int hit_ghost_history);
    // Test Purpose
    bool is_page_in(Page *page) {
      assert(page->arc_idx != ARC_COUNT);
      Mutex::Locker l(lock);
      Page *p = arc_list_head[page->arc_idx];
      while (p) {
        if (p == page)
          return true;
        p = p->arc_next;
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
      BlockCacherCompletion(Context *c) : count(0), ctxt(c) { }
      virtual ~BlockCacherCompletion() {}
      void add_request() { count.inc(); }
      void complete_request(int r);
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

  class MockLibrbdThread : public Thread {
    CephContext *cct;
    std::list<AioRead*> reads;
    std::list<AioWrite*> writes;
    bool done;
    Mutex lock;
    Cond cond;
    double delay;

   public:
    MockLibrbdThread(CephContext *c, double delay):
      cct(c), done(false), lock("BlockCacher::MockLibrbdThread::lock"), delay(delay) { }
    ~MockLibrbdThread() {}
    void stop() {
      Mutex::Locker l(lock);
      done = true;
      cond.Signal();
    }
    void* entry() {
      Mutex::Locker l(lock);
      while (!done && reads.empty() && writes.empty()) {
        utime_t t = ceph_clock_now(cct);
        t += delay * (rand() % 1000) / 1000.0;
        cond.WaitUntil(lock, t);
        if (!reads.empty()) {
          AioRead *r = reads.front();
          reads.pop_front();
          bufferlist &data = r->data();
          data.append_zero(r->get_object_len());
          r->complete(0);
        }

        if (!writes.empty()) {
          AioWrite *w = writes.front();
          writes.pop_front();
          w->complete(0);
        }
        if (reads.empty() && writes.empty())
          cond.Wait(lock);
      }
      return 0;
    }

    void queue_read(AioRead *r) {
      Mutex::Locker l(lock);
      reads.push_back(r);
    }
    void queue_write(AioWrite *w) {
      Mutex::Locker l(lock);
      writes.push_back(w);
    }
  };

  class BlockCacher {
    struct Region {
      void *addr;
      uint64_t length;
    };

    bool mock;
    MockLibrbdThread *mock_thread;
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
    uint32_t total_pages;
    uint32_t region_maxpages;
    uint32_t page_length;

    // protect by tree_lock
    uint32_t remain_data_pages;
    vector<Region> regions;
    Page *free_pages_head;
    Page *free_data_pages_head;

    bool inflight_page_wait;
    uint32_t inflight_pages; // If page is inflight, it won't in car_state

    CARState car_state;

    Mutex dirty_page_lock;
    struct DirtyPageState {
      bool wt;
      Page *dirty_pages_head, *dirty_pages_foot;
      atomic_t dirty_writing_page;
      uint32_t dirty_pages;
      uint32_t target_pages;
      uint32_t max_dirty_pages;   // if 0 means writethrough
      utime_t max_dirty_age;
      DirtyPageState(): wt(true), dirty_pages_head(NULL), dirty_pages_foot(NULL),
                        dirty_writing_page(0), dirty_pages(0), target_pages(0), max_dirty_pages(0) {}
      bool writethrough() const { return wt || !max_dirty_pages; }
      bool need_writeback() const { return dirty_pages > target_pages; }
      uint32_t writeback_pages() const {
        return dirty_pages > target_pages ? dirty_pages - target_pages : 0;
      }
      void mark_dirty(Page *p) {
        if (p->flags & PAGE_DIRTY) {
          if (p->dirty_or_ghost_prev)
            p->dirty_or_ghost_prev->dirty_or_ghost_next = p->dirty_or_ghost_next;
          else
            dirty_pages_head = p->dirty_or_ghost_next;
          if (p->dirty_or_ghost_next)
            p->dirty_or_ghost_next->dirty_or_ghost_prev = p->dirty_or_ghost_prev;
          else
            dirty_pages_foot = p->dirty_or_ghost_prev;
        } else {
          p->flags |= PAGE_DIRTY;
          ++dirty_pages;
        }
        p->dirty_or_ghost_prev = dirty_pages_foot;
        dirty_pages_foot->dirty_or_ghost_next = p;
        dirty_pages_foot = p;
        p->dirty_or_ghost_next = NULL;
      }
      void writeback_pages(map<uint16_t, map<uint64_t, Page*> > &sorted_flush, uint32_t num) {
        uint32_t i = 0;
        Page *p = dirty_pages_head;
        while (p) {
          p->flags &= ~PAGE_DIRTY;
          sorted_flush[p->ictx_id][p->offset] = p;
          p = p->dirty_or_ghost_next;
          if (num && i++ > num)
            break;
        }
        dirty_pages -= i;
        dirty_pages_head = p;
        dirty_writing_page.add(i);
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
      char *buf;
      AioRead *req;

      C_BlockCacheRead(BlockCacher *bc, BlockCacherCompletion *c, ObjectPage &e,
                       uint64_t o, size_t l, char *b):
          block_cacher(bc), comp(c), extent(e), start(o), end(o+l), buf(b) {
        comp->add_request();
      }
      virtual ~C_BlockCacheRead() {}
      virtual void finish(int r) {
        block_cacher->complete_read(this, r);
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

      C_BlockCacheWrite(BlockCacher *bc, BlockCacherCompletion *c, ImageCtx *ctx, ObjectPage &e, uint64_t fid):
          block_cacher(bc), comp(c), ictx(ctx), extent(e), flush_id(fid) {}
      virtual ~C_BlockCacheWrite() {}
      virtual void finish(int r) {
        block_cacher->complete_write(this, r);
      }
      void send_by_bc_write_comp(SnapContext &snapc)
      {
        AioWrite *req = new AioWrite(ictx, extent.oid.name, extent.objectno, extent.offset,
                                     data, snapc, this);
        if (block_cacher->mock)
          block_cacher->mock_thread->queue_write(req);
        else
          req->send();
      }
    };
    friend class C_BlockCacherWrite;

    class C_FlushWrite : public Context {
      BlockCacher *block_cacher;
      Context *c;
      uint64_t flush_id;

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
    std::list<C_BlockCacheWrite*> flush_retry_writes;
    map<uint64_t, pair<uint64_t, Context*> > flush_commits;
    std::list<Context*> wait_writeback;

    void unflight_pages(Page **pages, size_t page_num) {
      assert(tree_lock.is_locked());
      for (size_t i = 0; i < page_num; ++i) {
        assert(pages[i]->inflight);
        pages[i]->inflight = 0;
      }
      car_state.insert_pages(pages, page_num);
      inflight_pages -= page_num;
      if (inflight_page_wait) {
        inflight_page_wait = false;
        tree_cond.Signal();
      }
    }

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
                             BlockCacherCompletion *c, ::SnapContext &snapc);

    int reg_region(uint64_t size);
    void complete_read(C_BlockCacheRead *bc_read_comp, int r);
    void complete_write(C_BlockCacheWrite *bc_write_comp, int r, bool noretry=false);
    void prepare_continuous_pages(ImageCtx *ictx, map<uint64_t, Page*> &flush_pages,
                                  map<object_t, vector<ObjectPage> > &object_extents);
    int get_pages(PageRBTree *tree, PageRBTree *ghost_tree, Page **pages, bool hit[], size_t page_size,
                  size_t align_offset, bool only_hit=false);
    int read_object_extents(ImageCtx *ictx, uint64_t offset, size_t len,
                            map<object_t, vector<ObjectPage> > &object_extents,
                            char *buf, BlockCacherCompletion *c, uint64_t snap_id);

   public:
    BlockCacher(CephContext *c):
      mock(false), cct(c), tree_lock("BlockCacher::tree_lock"),
      ictx_management_lock("BlockCacher::ictx_management_lock"), ictx_next(1), all_pages(NULL),
      total_pages(0), region_maxpages(0), page_length(0), remain_data_pages(0),
      free_pages_head(NULL), free_data_pages_head(NULL), inflight_page_wait(false),
      inflight_pages(0), car_state(c), dirty_page_lock("BlockCacher::dirty_page_lock"),
      flush_lock("BlockCacher::BlockCacher"), flusher_stop(true), flush_id(0), flusher_thread(this) {}

    ~BlockCacher() {
      if (flusher_thread.is_started()) {
        flusher_stop = true;
        flush_lock.Lock();
        flush_cond.Signal();
        flush_lock.Unlock();
        flusher_thread.join();
      }

      assert(flush_retry_writes.empty());
      assert(flush_commits.empty());
      assert(wait_writeback.empty());
      if (mock && mock_thread) {
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
              uint32_t target_dirty, uint32_t max_dirty, double dirty_age) {
      // Don't init again if already init
      if (page_length)
        return ;
      page_length = unit;
      region_maxpages = region_units;
      remain_data_pages = cache_size / unit;
      total_pages = remain_data_pages * 2;
      dirty_page_state.target_pages = target_dirty / unit;
      dirty_page_state.max_dirty_pages = max_dirty / unit;
      dirty_page_state.max_dirty_age.set_from_double(dirty_age);
      car_state.set_lru_limit(remain_data_pages / 2);
      car_state.set_data_pages(remain_data_pages);

      int r = ::posix_memalign((void**)&all_pages, CEPH_PAGE_SIZE, total_pages*page_length);
      assert(r);
      memset(all_pages, 0, total_pages*page_length);
      Page *p = all_pages;
      for (uint64_t i = 0; i < total_pages; ++i) {
        p->arc_next = free_pages_head;
        free_pages_head = p;
        p->arc_idx = ARC_COUNT;
        ++p;
      }
      flusher_thread.create();
      if (mock) {
        mock_thread = new MockLibrbdThread(cct, 0.005);
        mock_thread->create();
      }
    }

    int read_buffer(uint64_t ictx_id, uint64_t offset, size_t len,
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
      registered_ictx.push_back(ictx);
      registered_tree.push_back(pt);
      ghost_trees.push_back(gpt);
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

    // uniq name for CephContext to distinguish differnt object
    static const string name;
  };

}

#endif
