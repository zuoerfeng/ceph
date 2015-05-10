// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2015 Haomai Wang <haomaiwang@gmail.com>
 *
 * Author: Haomai Wang <haomaiwang@gmail.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef CEPH_COMMON_RBTREE_H
#define CEPH_COMMON_RBTREE_H

#include <stddef.h>

struct RBNode {
  unsigned long  __rb_parent_color;
  RBNode *rb_left;
  RBNode *rb_right;
  inline RBNode *parent() const { return (RBNode *)(__rb_parent_color & ~3); }
  template<typename T>
  inline T *get_container(int off) {
    return reinterpret_cast<T*>((char*)(this) - off);
  }
} __attribute__((aligned(sizeof(long))));

struct RBTree {
  RBNode *rb_node;

  RBTree(): rb_node(NULL) {}
  void insert_color(RBNode *node);
  void erase(RBNode *node);
  void replace(RBNode *old, RBNode *n);

  class Iterator {
    friend class RBTree;
   protected:
    RBNode *cur;

   public:
    Iterator(): cur(NULL) {}
    Iterator(RBNode *n): cur(n) {}
    bool operator==(const Iterator& itr) const {
      return cur == itr.cur;
    }
    bool operator!=(const Iterator& itr) const {
      return cur != itr.cur;
    }
    RBNode* operator->() { return &(operator*()); }
    RBNode& operator*() const { return *cur; }
    Iterator& operator++() {
      /*
       * If we have a right-hand child, go down and then left as far
       * as we can.
       */
      if (cur->rb_right) {
        cur = cur->rb_right;
        while (cur->rb_left)
          cur = cur->rb_left;
      } else {
        /*
         * No right-hand children. Everything down and left is smaller than us,
         * so any 'next' node must be in the general direction of our parent.
         * Go up the tree; any time the ancestor is a right-hand child of its
         * parent, keep going up. First time it's a left-hand child of its
         * parent, said parent is our 'next' node.
         */
        RBNode *child = cur;
        while ((cur = cur->parent()) && child == cur->rb_right)
          child = cur;
      }
      return *this;
    }
    Iterator& operator--() {
      /*
       * If we have a left-hand child, go down and then right as far
       * as we can.
       */
      if (cur->rb_left) {
        cur = cur->rb_left;
        while (cur->rb_right)
          cur = cur->rb_right;
      } else {
        /*
         * No left-hand children. Go up till we find an ancestor which
         * is a right-hand child of its parent.
         */
        RBNode *child = cur;
        while ((cur = cur->parent()) && child == cur->rb_right)
          child = cur;
      }
      return *this;
    }
  };
  Iterator begin() {
    RBNode *n = rb_node;
    if (n) {
      while (n->rb_left)
        n = n->rb_left;
    }
    return Iterator(n);
  }
  Iterator last() {
    RBNode *n = rb_node;
    if (n) {
      while (n->rb_right)
        n = n->rb_right;
    }
    return Iterator(n);
  }
  Iterator end() {
    return Iterator(NULL);
  }

  static inline void rb_link_node(RBNode *node, RBNode *parent, RBNode **rb_link)
  {
    node->__rb_parent_color = (unsigned long)parent;
    node->rb_left = node->rb_right = NULL;
    *rb_link = node;
  }
};

#endif
