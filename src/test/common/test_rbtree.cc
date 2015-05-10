// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2015 Haomai Wang <haomaiwang@gmail.com>
 *
 * LGPL2.1 (see COPYING-LGPL2.1) or later
 */

#include <gtest/gtest.h>
#include <time.h>
#include <stdint.h>
#include <stdio.h>
#include <map>
#include "common/RBTree.h"
#include "common/Cycles.h"

#define NODES       100
#define PERF_LOOPS  100000
#define CHECK_LOOPS 100

using namespace std;

class TestRBTree : public ::testing::Test {
  map<uint32_t, uint32_t> verify;

 public:
  struct TestNode {
    RBNode rb;
    uint32_t key;

    /* following fields used for testing augmented rbtree functionality */
    uint32_t val;
    uint32_t augmented;
  };

  RBTree root;
  TestNode nodes[NODES];
  virtual void SetUp() {
    set<uint32_t> existings;
    uint32_t k;
    for (int i = 0; i < NODES; i++) {
      do {
        k = rand() % 1000;
      } while (existings.count(k));
      existings.insert(k);
      nodes[i].key = k;
      nodes[i].val = rand();
    }
  }

  void lower_bound(uint64_t key) {
    RBNode *n = root.rb_node, *parent = NULL;
    TestNode *node = NULL;

    while (n) {
      parent = n;
      node = n->get_container<TestNode>(offsetof(TestNode, rb));

      if (key < node->key) {
        n = n->rb_left;
      } else if (key > node->key) {
        n = n->rb_right;
      } else {
        parent = n;
        break;
      }
    }

    RBTree::Iterator it(parent);
    while (node && key > node->key) {
      ++it;
      node = it->get_container<TestNode>(offsetof(TestNode, rb));
    }

    map<uint32_t, uint32_t>::iterator verify_it = verify.lower_bound(key);
    if (verify_it != verify.end()) {
      ASSERT_EQ(verify_it->second, node->val);
      ASSERT_EQ(verify_it->first, node->key);
    } else {
      ASSERT_TRUE(it == root.end());
    }
  }

  void insert(TestNode *node)
  {
    RBNode **n = &root.rb_node, *parent = NULL;
    uint32_t key = node->key;

    while (*n) {
      parent = *n;
      if (key < parent->get_container<TestNode>(offsetof(TestNode, rb))->key)
        n = &parent->rb_left;
      else
        n = &parent->rb_right;
    }

    root.rb_link_node(&node->rb, parent, n);
    root.insert_color(&node->rb);

    verify[key] = node->val;
  }

  inline void erase(TestNode *node)
  {
    root.erase(&node->rb);
    verify.erase(node->key);
  }

  bool is_red(const RBNode &rb)
  {
    return !(rb.__rb_parent_color & 1);
  }

  int black_path_count(RBNode &rb)
  {
    RBNode *p = &rb;
    int count;
    for (count = 0; p; p = p->parent())
      count += !is_red(*p);
    return count;
  }

  void check(int nr_nodes)
  {
    int count = 0, blacks = 0;
    uint32_t prev_key = 0;
    map<uint32_t, uint32_t>::iterator verify_it = verify.begin();

    for (RBTree::Iterator it = root.begin(); it != root.end(); ++it, ++verify_it) {
      TestNode *node = it->get_container<TestNode>(offsetof(TestNode, rb));
      ASSERT_TRUE(node->key >= prev_key);
      ASSERT_FALSE(is_red(*it) && (!it->parent() || is_red(*it->parent())));
      if (!count)
        blacks = black_path_count(*it);
      else
        ASSERT_FALSE((!it->rb_left || !it->rb_right) && blacks != black_path_count(*it));
      prev_key = node->key;
      count++;
      ASSERT_EQ(verify_it->second, node->val);
      ASSERT_EQ(verify_it->first, prev_key);
    }

    ASSERT_TRUE(count == nr_nodes);
    ASSERT_FALSE(count < (1 << black_path_count(*root.last())) - 1);
  }
};

TEST_F(TestRBTree, Basic)
{
  int i, j;
  uint64_t time1, time2, time;

  std::cout << "rbtree testing";

  srand(3588);

  time1 = Cycles::rdtsc();

  for (i = 0; i < PERF_LOOPS; i++) {
    for (j = 0; j < NODES; j++)
      insert(nodes + j);
    for (j = 0; j < NODES; j++)
      erase(nodes + j);
  }

  time2 = Cycles::rdtsc();
  time = time2 - time1;

  time = time / PERF_LOOPS;
  std::cout << " -> " << time << " cycles" << std::endl;

  for (i = 0; i < CHECK_LOOPS; i++) {
    for (j = 0; j < NODES; j++) {
      check(j);
      insert(nodes + j);
      lower_bound(rand() % 1000);
    }
    for (j = 0; j < NODES; j++) {
      check(NODES - j);
      erase(nodes + j);
      lower_bound(rand() % 1000);
    }
    check(0);
  }
}
