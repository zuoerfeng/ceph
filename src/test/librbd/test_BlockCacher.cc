// -*- mode:C; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2015 Haomai Wang <haomaiwang@gmail.com>
 *
 * LGPL2.1 (see COPYING-LGPL2.1) or later
 */
#include <fcntl.h>
#include <stdlib.h>
#include <time.h>
#include <unistd.h>
#include <map>
#include <vector>
#include <iostream>

#include <boost/random/mersenne_twister.hpp>
#include <boost/random/uniform_int.hpp>
#include <boost/random/binomial_distribution.hpp>

#include "include/rados/librados.h"
#include "include/interval_set.h"
#include "common/ceph_context.h"
#include "common/ceph_argparse.h"
#include "global/global_init.h"
#include "librbd/BlockCacher.h"

#include "gtest/gtest.h"
#include "test/librbd/test_fixture.h"

typedef boost::mt11213b gen_type;
using namespace librbd;

class TestCARState: public ::testing::Test {
 public:
  CARState car_state;
  Page *all_pages;
  static const int PAGE_COUNT = 100;
  map<uint64_t, Page*> data_tree, ghost_tree;
  set<Page*> caches;
  vector<Page*> free_data_pages;
  vector<Page*> free_ghost_pages;

  TestCARState(): car_state(g_ceph_context) {
    all_pages = new Page[PAGE_COUNT];
    car_state.set_lru_limit(PAGE_COUNT/4);
    car_state.set_data_pages(PAGE_COUNT/2);
    memset(all_pages, 0, sizeof(Page)*PAGE_COUNT);
    for (int i = 0; i < PAGE_COUNT; ++i) {
      all_pages[i].arc_idx = ARC_COUNT;
      if (i % 2)
        free_data_pages.push_back(&all_pages[i]);
      else
        free_ghost_pages.push_back(&all_pages[i]);
    }
    srand(time(NULL));
  }
};

TEST_F(TestCARState, Simulate)
{
  map<uint64_t, Page*>::iterator data_it, ghost_it;
  uint64_t offset;
  Page *p, *ghost_page, *cur_page;
  int hit_ghost_history;
  for (int i = 0; i < 10000; ++i) {
    offset = rand() % PAGE_COUNT;
    if (offset % 10)
      ASSERT_TRUE(car_state.validate());
    data_it = data_tree.find(offset);
    hit_ghost_history = 0;
    p = NULL;
    if (data_it != data_tree.end() && data_it->second->offset == offset) {
      ASSERT_TRUE(car_state.is_page_in_or_inflight(data_it->second));
      car_state.hit_page(data_it->second);
      continue;
    } else if (!free_data_pages.empty()) {
      ASSERT_FALSE(car_state.is_full());
      p = free_data_pages.back();
      free_data_pages.pop_back();
      p->page_next = NULL;
    } else {
      ASSERT_TRUE(car_state.is_full());
      if (data_it != data_tree.end())
        ASSERT_FALSE(car_state.is_page_in_or_inflight(data_it->second));
      ghost_it = ghost_tree.find(offset);
      cur_page = car_state.evict_data();
      data_tree.erase(cur_page->offset);
      ghost_tree[cur_page->offset] = cur_page;

      ghost_page = NULL;
      if (ghost_it != ghost_tree.end() && ghost_it->second->offset == offset) {
        ghost_page = ghost_it->second;
        hit_ghost_history = ghost_page->arc_idx;
      }

      p = car_state.get_ghost_page(hit_ghost_history ? ghost_page : NULL);
      if (p) {
        ghost_tree.erase(p->offset);
      } else {
        assert(!free_ghost_pages.empty());
        p = free_ghost_pages.back();
        free_ghost_pages.pop_back();
        p->page_next = NULL;
      }
      p->addr = cur_page->addr;
    }
    p->offset = offset;
    car_state.adjust_and_hold(p, hit_ghost_history);
    data_tree[offset] = p;
    car_state.insert_page(p);
  }
}

class MockLibrbdThread : public MockThread {
  CephContext *cct;
  std::list<pair<AioRead*, string> > reads;
  std::list<pair<AioWrite*, string> > writes;
  bool done;
  Mutex lock;
  Cond cond;
  double delay;
  char path[64];

 public:
  MockLibrbdThread(CephContext *c, double delay):
    cct(c), done(false), lock("BlockCacher::MockLibrbdThread::lock"), delay(delay) {
    sprintf(path, "MockLibrbdThread-%d-%d", getpid(), rand());
    assert(::mkdir(path, 0755) == 0);
  }
  ~MockLibrbdThread() {}
  void stop() {
    Mutex::Locker l(lock);
    done = true;
    cond.Signal();
  }
  void* entry() {
    char oid_path[256];
    int fd;
    ssize_t r;
    Mutex::Locker l(lock);
    while (!done || !reads.empty() || !writes.empty()) {
      if (rand() % 100 == 0) {
        utime_t t = ceph_clock_now(cct);
        t += delay * (rand() % 1000) / 1000.0;
        cond.WaitUntil(lock, t);
      }
      if (!reads.empty()) {
        pair<AioRead*, string> p = reads.front();
        reads.pop_front();
        sprintf(oid_path, "%s/%s", path, p.second.c_str());
        fd = ::open(oid_path, O_RDWR | O_CREAT, 0755);
        bufferptr ptr(p.first->get_object_len());
        memset(ptr.c_str(), 0, ptr.length());
        r = ::pread(fd, ptr.c_str(), p.first->get_object_len(), p.first->get_object_off());
        assert(r != -1);
        p.first->data().append(ptr);
        lock.Unlock();
        p.first->complete(0);
        lock.Lock();
        ::close(fd);
      }

      if (!writes.empty()) {
        pair<AioWrite*, string> p = writes.front();
        writes.pop_front();
        sprintf(oid_path, "%s/%s", path, p.second.c_str());
        fd = ::open(oid_path, O_RDWR | O_CREAT, 0755);
        assert(fd != -1);
        assert(p.first->get_object_len() == p.first->data().length());
        r = ::pwrite(fd, p.first->data().c_str(), p.first->get_object_len(), p.first->get_object_off());
        assert(r != -1);
        ::close(fd);
        lock.Unlock();
        p.first->complete(0);
        lock.Lock();
      }
      if (reads.empty() && writes.empty())
        cond.Wait(lock);
    }
    return 0;
  }

  void queue_read(AioRead *r, string &oid) {
    Mutex::Locker l(lock);
    reads.push_back(make_pair(r, oid));
    cond.Signal();
  }
  void queue_write(AioWrite *w, string &oid) {
    Mutex::Locker l(lock);
    writes.push_back(make_pair(w, oid));
    cond.Signal();
  }
};


class TestBlockCacher : public ::testing::Test {
  librados::IoCtx m_ioctx;
  vector<ImageCtx*> ictxs;
 public:
  BlockCacher *block_cacher;
  TestBlockCacher() {
    block_cacher = new BlockCacher(g_ceph_context);
  }

  void init(uint64_t cache_size=32*1024*1024, uint64_t unit=4*1024, uint64_t region_units=8*1024,
            uint32_t target_dirty=8*1024*1024, uint32_t max_dirty=16*1024*1024, double dirty_age=1) {
    block_cacher->init(cache_size, unit, region_units, target_dirty, max_dirty, dirty_age,
                       new MockLibrbdThread(g_ceph_context, 0.1));
  }
  virtual void TearDown() {
    for (vector<ImageCtx*>::iterator it = ictxs.begin(); it != ictxs.end(); ++it) {
      block_cacher->unregister_image(*it);
      delete *it;
    }
    delete block_cacher;
  }
  ImageCtx *generate_ictx(string image_name, uint64_t stripe_unit, uint64_t stripe_count, uint64_t order) {
    ImageCtx *ictx = new ImageCtx(image_name.c_str(), "", NULL, m_ioctx, false, g_ceph_context);
    ictx->layout.fl_stripe_unit = stripe_unit;
    ictx->layout.fl_stripe_count = stripe_count;
    ictx->layout.fl_object_size = 1ull << order;
    ictx->layout.fl_pg_pool = 1;
    ictx->format_string = new char[10];
    sprintf(ictx->format_string, "prefix.%%016llx");
    ictx->block_cacher = block_cacher;
    ictx->block_cacher_id = block_cacher->register_image(ictx);
    ictxs.push_back(ictx);
    return ictx;
  }
};

TEST_F(TestBlockCacher, BasicOps)
{
  init();
  char buf[100], read_buf[100];
  memset(buf, 1, sizeof(buf));
  ImageCtx *ictx = generate_ictx("test_image", 1<<22, 1, 22);
  uint64_t snap_id;
  {
    RWLock::RLocker snap_locker(ictx->snap_lock);
    snap_id = ictx->snap_id;
  }
  C_SaferCond *ctx = new C_SaferCond();
  block_cacher->write_buffer(ictx->block_cacher_id, 0, 100, buf, ctx, 0, ictx->snapc);
  ctx->wait();
  delete ctx;
  ctx = new C_SaferCond();
  block_cacher->user_flush(ctx);
  ctx->wait();
  delete ctx;
  ctx = new C_SaferCond();
  block_cacher->read_buffer(ictx->block_cacher_id, 0, 100, read_buf, ctx, snap_id, 0);
  ctx->wait();
  delete ctx;
  ctx = new C_SaferCond();
  ASSERT_EQ(memcmp(buf, read_buf, sizeof(buf)), 0);

  size_t unit = 1024*1024;
  char large_buf[unit], large_read_buf[unit];
  memset(large_buf, 2, sizeof(large_buf));
  block_cacher->write_buffer(ictx->block_cacher_id, 1<<23, unit, large_buf, ctx, 0, ictx->snapc);
  ctx->wait();
  delete ctx;

  for (int i = 0; i < unit*64; i+=unit) {
    ctx = new C_SaferCond();
    block_cacher->read_buffer(ictx->block_cacher_id, i, unit, large_read_buf, ctx, snap_id, 0);
    ctx->wait();
    delete ctx;
  }

  ctx = new C_SaferCond();
  block_cacher->read_buffer(ictx->block_cacher_id, 1<<23, 1024*1024, large_read_buf, ctx, snap_id, 0);
  ctx->wait();
  delete ctx;
  ctx = new C_SaferCond();
  ASSERT_EQ(memcmp(large_buf, large_read_buf, sizeof(large_buf)), 0);

  block_cacher->discard(ictx->block_cacher_id, 0, 100);
  memset(buf, 0, sizeof(buf));
  block_cacher->read_buffer(ictx->block_cacher_id, 0, 100, read_buf, ctx, snap_id, 0);
  ctx->wait();
  delete ctx;
  ASSERT_EQ(memcmp(buf, read_buf, sizeof(buf)), 0);
  block_cacher->purge(ictx->block_cacher_id);
}

class SyntheticWorkload {
  BlockCacher *block_cacher;
  vector<ImageCtx*> ictxs;
  vector<uint32_t> digests;
  vector<string> rand_data;
  Mutex lock;
  interval_set<uint64_t> inflight_offsets;
  set<Context*> inflight_ctxts;
  gen_type rng;
  int bs;
  size_t max_size;

  #define READ_OP 0
  #define WRITE_OP 1
  #define FLUSH_OP 2
  class SyntheticContext: public Context {
    SyntheticWorkload *w;
    char *buf;
    uint64_t offset;
    size_t len;
    int op;

   public:
    SyntheticContext(SyntheticWorkload *w, char *b, uint64_t off, size_t len, int op):
        w(w), buf(b), offset(off), len(len), op(op) {}
    virtual void finish(int r) {
      Mutex::Locker l(w->lock);
      uint32_t crc;
      uint64_t start = offset;
      if (op == READ_OP) {
        for (const char *b = buf; start < offset + len;
             start += w->bs, b += w->bs) {
          crc = ceph_crc32c(0, (unsigned char*)b, w->bs);
          ASSERT_EQ(crc, w->digests[start/w->bs]);
        }
        w->inflight_offsets.erase(offset, len);
        delete buf;
      } else if (op == WRITE_OP) {
        for (const char *b = buf; start < offset + len;
             start += w->bs, b += w->bs) {
          crc = ceph_crc32c(0, (unsigned char*)b, w->bs);
          w->digests[start/w->bs] = crc;
        }
        w->inflight_offsets.erase(offset, len);
        delete buf;
      } else if (op == FLUSH_OP) {
      }
      w->inflight_ctxts.erase(this);
    }
  };
  friend class SyntheticContext;

 public:
  static const int RANDOM_DATA_NUM = 100;
  SyntheticWorkload(BlockCacher *bc, int bs, int s):
      block_cacher(bc), lock("SyntheticWorkload::lock"), rng(time(NULL)), bs(bs), max_size(s) {
    boost::uniform_int<> u(0, max_size/100 - 4096);
    char *data = new char[max_size/100];
    size_t len;
    for (int i = 0; i < RANDOM_DATA_NUM; i++) {
      len = u(rng) + 4096;
      len = len - len % bs;
      memset(data, 0, len);
      for (uint64_t j = 0; j < len-sizeof(i); j += 512)
        memcpy(data+j, &i, sizeof(i));
      rand_data.push_back(string(data, len));
    }
    digests.resize(s/bs);
    char zero_data[bs];
    memset(zero_data, 0, bs);
    uint32_t crc = ceph_crc32c(0, (unsigned char*)zero_data, bs);
    for (int i = 0; i < s/bs; ++i)
      digests[i] = crc;
  }

  int add_image(ImageCtx *ictx) {
    ictxs.push_back(ictx);
    return ictxs.size() - 1;
  }

  void read(int id) {
    boost::uniform_int<> u1(0, max_size);
    boost::uniform_int<> u2(0, max_size/100 - 4096);
    uint64_t offset, len;
    lock.Lock();
    do {
      offset = u1(rng);
      offset = offset - offset % bs;
      len = u2(rng);
      if (len + offset >= max_size)
        len = max_size - offset;
      len = len - len % bs;
    } while (!len || inflight_offsets.intersects(offset, len));
    char *buf = new char[len];
    SyntheticContext *ctx = new SyntheticContext(this, buf, offset, len, READ_OP);
    inflight_offsets.insert(offset, len);
    inflight_ctxts.insert(ctx);
    lock.Unlock();
    uint64_t snap_id;
    {
      RWLock::RLocker snap_locker(ictxs[id]->snap_lock);
      snap_id = ictxs[id]->snap_id;
    }
    block_cacher->read_buffer(ictxs[id]->block_cacher_id, offset, len, buf, ctx, snap_id, 0);
  }

  void write(int id) {
    boost::uniform_int<> u(0, max_size);
    uint64_t offset, len;
    lock.Lock();
    string &d = rand_data[u(rng) % (rand_data.size()-1)];
    do {
      offset = u(rng);
      if (offset + d.size() > max_size)
        offset = max_size - d.size();
      offset = offset - offset % bs;
      len = d.size();
    } while (inflight_offsets.intersects(offset, len));
    char *buf = new char[len];
    SyntheticContext *ctx = new SyntheticContext(this, buf, offset, len, WRITE_OP);
    inflight_offsets.insert(offset, len);
    inflight_ctxts.insert(ctx);
    lock.Unlock();
    d.copy(buf, len, 0);
    block_cacher->write_buffer(ictxs[id]->block_cacher_id, offset, len, buf, ctx, 0, ictxs[id]->snapc);
  }

  void flush(int id) {
    lock.Lock();
    SyntheticContext *ctx = new SyntheticContext(this, NULL, 0, 0, FLUSH_OP);
    inflight_ctxts.insert(ctx);
    lock.Unlock();
    block_cacher->user_flush(ctx);
  }

  void print_internal_state() {
    Mutex::Locker l(lock);
    cerr << " available_ops: " << inflight_offsets.num_intervals() << std::endl;
  }

  void wait_for_done() {
    uint64_t i = 0;
    lock.Lock();
    while (!inflight_offsets.empty()) {
      lock.Unlock();
      usleep(1000*100);
      if (i++ % 50 == 0)
        print_internal_state();
      lock.Lock();
    }
    lock.Unlock();
    assert(inflight_ctxts.empty());
  }
};

TEST_F(TestBlockCacher, WriteThrough)
{
  init();
  ImageCtx *ictx = generate_ictx("test_image", 1<<22, 1, 22);
  SyntheticWorkload work_load(block_cacher, 4096, 1024*1024*1024);
  int id = work_load.add_image(ictx);

  gen_type rng(time(NULL));
  for (int i = 0; i < 10000; ++i) {
    if (!(i % 10)) {
      cerr << "Op " << i << ": ";
      work_load.print_internal_state();
    }
    boost::uniform_int<> true_false(0, 99);
    int val = true_false(rng);
    if (val > 10) {
      work_load.write(id);
    } else {
      usleep(rand() % 1000 + 500);
    }
  }
  work_load.wait_for_done();
}

TEST_F(TestBlockCacher, WriteBack)
{
  init();
  ImageCtx *ictx = generate_ictx("test_image", 1<<22, 1, 22);
  SyntheticWorkload work_load(block_cacher, 4096, 1024*1024*1024);
  int id = work_load.add_image(ictx);

  gen_type rng(time(NULL));
  work_load.flush(id);
  for (int i = 0; i < 10000; ++i) {
    if (!(i % 10)) {
      cerr << "Op " << i << ": ";
      work_load.print_internal_state();
    }
    boost::uniform_int<> true_false(0, 99);
    int val = true_false(rng);
    if (val > 10) {
      work_load.write(id);
    } else {
      usleep(rand() % 1000 + 500);
    }
  }
  work_load.wait_for_done();
}

TEST_F(TestBlockCacher, Read)
{
  init();
  ImageCtx *ictx = generate_ictx("test_image", 1<<22, 1, 22);
  SyntheticWorkload work_load(block_cacher, 4096, 1024*1024*1024);
  int id = work_load.add_image(ictx);

  for (int i = 0; i < 100; ++i) {
    if (!(i % 10)) cerr << "seeding write " << i << std::endl;
    work_load.write(id);
  }

  gen_type rng(time(NULL));
  for (int i = 0; i < 10000; ++i) {
    if (!(i % 10)) {
      cerr << "Op " << i << ": ";
      work_load.print_internal_state();
    }
    boost::uniform_int<> true_false(0, 99);
    int val = true_false(rng);
    if (val > 10) {
      work_load.read(id);
    } else {
      usleep(rand() % 1000 + 500);
    }
  }
  work_load.wait_for_done();
}

TEST_F(TestBlockCacher, ReadWrite)
{
  init();
  ImageCtx *ictx = generate_ictx("test_image", 1<<22, 1, 22);
  SyntheticWorkload work_load(block_cacher, 4096, 1024*1024*1024);
  int id = work_load.add_image(ictx);

  for (int i = 0; i < 1000; ++i) {
    if (!(i % 10)) cerr << "seeding write " << i << std::endl;
    work_load.write(id);
  }
  gen_type rng(time(NULL));
  for (int i = 0; i < 10000; ++i) {
    if (!(i % 10)) {
      cerr << "Op " << i << ": ";
      work_load.print_internal_state();
    }
    boost::uniform_int<> true_false(0, 99);
    int val = true_false(rng);
    if (val > 55) {
      work_load.write(id);
    } else if (val > 10) {
      work_load.read(id);
    } else if (val > 9) {
      work_load.flush(id);
    } else {
      usleep(rand() % 1000 + 500);
    }
  }
  work_load.wait_for_done();
}


int main(int argc, char **argv) {
  vector<const char*> args;
  argv_to_vec(argc, (const char **)argv, args);

  global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT, CODE_ENVIRONMENT_UTILITY, 0);
  common_init_finish(g_ceph_context);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

/*
 * Local Variables:
 * compile-command: "cd ../.. ; make ceph_test_block_cacher && 
 *    ./ceph_test_block_cacher \
 *        --gtest_filter=* --log-to-stderr=true --debug-filestore=20
 *  "
 * End:
 */
