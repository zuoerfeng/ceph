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
#include <stdlib.h>
#include <time.h>
#include <map>
#include "include/rados/librados.h"
#include "common/ceph_context.h"
#include "common/ceph_argparse.h"
#include "global/global_init.h"
#include "librbd/BlockCacher.h"

#include "gtest/gtest.h"
#include "test/librbd/test_fixture.h"

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
  Page *p, *ghost_page;
  int hit_ghost_history;
  for (int i = 0; i < 10000; ++i) {
    offset = rand() % PAGE_COUNT;
    if (offset % 10)
      ASSERT_TRUE(car_state.validate());
    data_it = data_tree.find(offset);
    hit_ghost_history = 2;
    if (data_it != data_tree.end()) {
      ASSERT_TRUE(car_state.is_page_in(data_it->second));
      car_state.hit_page(data_it->second);
      continue;
    } else if (!free_data_pages.empty()) {
      ASSERT_FALSE(car_state.is_full());
      p = free_data_pages.back();
      free_data_pages.pop_back();
    } else {
      ASSERT_TRUE(car_state.is_full());
      ASSERT_FALSE(car_state.is_page_in(data_it->second));
      ghost_it = ghost_tree.find(offset);
      ghost_page = NULL;
      if (ghost_it != ghost_tree.end() && ghost_it->second->offset == offset) {
        ghost_page = ghost_it->second;
        hit_ghost_history = ghost_page->arc_idx;
      }
      p = car_state.get_ghost_page(ghost_page);
      ASSERT_TRUE(p->arc_idx == ARC_COUNT);
      if (p) {
        ghost_tree.erase(p->offset);
      } else {
        p = free_ghost_pages.back();
        free_ghost_pages.pop_back();
      }
      ghost_page = car_state.evict_data(p);
      ghost_tree[ghost_page->offset] = ghost_page;
      data_tree.erase(p->offset);
    }
    p->offset = offset;
    car_state.adjust_lru_limit(p, hit_ghost_history);
    data_tree[offset] = p;
    car_state.insert_pages(&p, 1);
  }
}

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
    block_cacher->init(cache_size, unit, region_units, target_dirty, max_dirty, dirty_age);
  }
  virtual void TearDown() {
    for (vector<ImageCtx*>::iterator it = ictxs.begin(); it != ictxs.end(); ++it) {
      block_cacher->unregister_image(*it);
      delete *it;
    }
    delete block_cacher;
  }
  ImageCtx *generate_ictx(string image_name, uint64_t stripe_unit, uint64_t stripe_count, uint64_t order) {
    ImageCtx *ictx = new ImageCtx(image_name.c_str(), "", NULL, m_ioctx, false);
    ictx->layout.fl_stripe_unit = stripe_unit;
    ictx->layout.fl_stripe_count = stripe_count;
    ictx->layout.fl_object_size = 1ull << order;
    ictx->layout.fl_pg_pool = 1;
    ictx->block_cacher = block_cacher;
    block_cacher->register_image(ictx);
    ictxs.push_back(ictx);
    return ictx;
  }
};

TEST_F(TestBlockCacher, BasicOps)
{
  init();
  char buf[100];
  C_SaferCond ctx;
  ImageCtx *ictx = generate_ictx("test_image", 0, 0, 22);
  block_cacher->write_buffer(ictx->block_cacher_id, 0, 100, buf, &ctx, 0, ictx->snapc);
  ctx.wait();
  block_cacher->read_buffer(ictx->block_cacher_id, 0, 100, buf, &ctx, 0, 0);
  ctx.wait();
  block_cacher->user_flush(&ctx);
  ctx.wait();
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
