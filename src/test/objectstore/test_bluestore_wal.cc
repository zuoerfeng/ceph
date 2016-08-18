// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2016 XSKY <haomai@xsky.com>
 *
 * Author: Haomai Wang <haomaiwang@gmail.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */


#include <stdio.h>
#include <string.h>
#include <iostream>
#include <time.h>
#include <fcntl.h>
#include <unistd.h>
#include <thread>
#include "global/global_init.h"
#include "common/ceph_argparse.h"
#include "include/stringify.h"
#include "include/page.h"
#include "common/Cycles.h"
#include "common/errno.h"
#include <gtest/gtest.h>

#include "os/filestore/FileJournal.h"
#include "os/bluestore/BlueFS.h"
#include "os/bluestore/BlueRocksEnv.h"
#include "kv/RocksDBStore.h"

class WALBench {
 public:
  virtual void queue_wal(bufferlist &bl, Context *c) = 0;
  virtual ~WALBench() {}
};

class WALFileJournal : public WALBench {
  FileJournal *journal;
  Finisher finisher;
  Cond sync_cond;
  char path[200];
  uint64_t seq = 1;
 public:
  WALFileJournal(const string &p, uint64_t s): finisher(g_ceph_context) {
    uuid_d fsid;
    fsid.generate_random();
    finisher.start();
    journal = new FileJournal(fsid, &finisher, &sync_cond, p.c_str(), true, true, true);
    journal->create();
    journal->make_writeable();
  }
  virtual ~WALFileJournal() {
    journal->close();
    delete journal;
    finisher.wait_for_empty();
    finisher.stop();
  }
  void queue_wal(bufferlist &bl, Context *c) override {
    bl.append_zero(header_size*2);
    uint64_t pad = ROUND_UP_TO(bl.length(), CEPH_PAGE_SIZE) - bl.length();
    bl.append_zero(pad);
    bl.rebuild_aligned(CEPH_PAGE_SIZE);
    journal->reserve_throttle_and_backoff(bl.length());
    journal->submit_entry(seq++, bl, bl.length(), c);
  }
};

class WALRocksDB : public WALBench {
  BlueFS fs;
  rocksdb::Env *env;
  RocksDBStore *db;
  uint64_t seq = 0;
  char buf[64];
  std::thread db_t;
  std::mutex m;
  std::condition_variable c;
  std::deque<std::pair<bufferlist, Context*> > queue;
  bool stop = false;
  void sync_thread() {
    memset(buf, 0, sizeof(buf));
    std::vector<std::string> wal_clean;
    std::unique_lock<std::mutex> l(m);
    while (!stop) {
      if (queue.empty()) {
        c.wait(l);
        continue;
      }
      assert(!queue.empty());
      std::pair<bufferlist, Context*> p = std::move(queue.front());
      queue.pop_front();
      l.unlock();
      KeyValueDB::Transaction t = db->get_transaction();
      memcpy(buf, &seq, sizeof(seq));
      ++seq;
      t->set("pre", buf, p.first);
      for (auto &s : wal_clean)
        t->rm_single_key("pre", s);
      wal_clean.clear();
      ASSERT_EQ(0, db->submit_transaction_sync(t));
      wal_clean.push_back(std::string(buf, sizeof(buf)));
      p.second->complete(0);
      l.lock();
    }
  }

 public:
  WALRocksDB(const string &p, uint64_t s) {
    uuid_d fsid;
    fsid.generate_random();
    fs.add_block_device(BlueFS::BDEV_DB, p);
    fs.add_block_extent(BlueFS::BDEV_DB, 1048576, s-1048576);
    fs.mkfs(fsid);
    fs.mount();
    env = new BlueRocksEnv(&fs);
    db = new RocksDBStore(g_ceph_context, p, env);
    std::string options = g_ceph_context->_conf->bluestore_rocksdb_options;
    db->init(options);
    stringstream err;
    db->create_and_open(err);
    db_t = std::thread([this] { this->sync_thread(); });
  }
  ~WALRocksDB() {
    fs.umount();
    stop = true;
    m.lock();
    c.notify_all();
    m.unlock();
    db_t.join();
    delete db;
  }
  void queue_wal(bufferlist &bl, Context *s) override {
    std::lock_guard<std::mutex> l(m);
    queue.emplace_back(bl, s);
    c.notify_all();
  }
};

struct PerfCollector {
  uint64_t started;
  uint64_t last_lat = 0;
  uint64_t last_count = 0;
  std::atomic_ulong lat;
  std::atomic_ulong count;
  PerfCollector(): lat(0), count(0) {
    started = Cycles::rdtsc();
  }
  ~PerfCollector() {
    std::cerr << " total last " << Cycles::to_microseconds(Cycles::rdtsc() - started) << "us" << std::endl;
  }
  void print() {
    if (count) {
      std::cerr << " count " << count << " avg lat " << Cycles::to_microseconds((lat-last_lat)/(count-last_count)) << "us" << std::endl;
      last_count = count;
      last_lat = lat;
    }
  }
};

class C_on_commit : public Context {
  PerfCollector *c;
  uint64_t started;
  std::atomic_int &inflight;
  std::mutex &lock;
  std::condition_variable &cond;
 public:
  C_on_commit(PerfCollector *d, std::atomic_int &i, std::mutex &l, std::condition_variable &con): c(d), inflight(i), lock(l), cond(con) {
    started = Cycles::rdtsc();
    ++inflight;
  }
  void finish(int r) {
    uint64_t lat = Cycles::rdtsc() - started;
    c->lat += lat;
    c->count++;
    --inflight;
    std::unique_lock<std::mutex> l(lock);
    cond.notify_all();
  }
};


string get_temp_bdev(uint64_t size)
{
  static int n = 0;
  string fn = "ceph_test_bluefs.tmp.block." + stringify(getpid())
    + "." + stringify(++n);
  int fd = ::open(fn.c_str(), O_CREAT|O_RDWR|O_TRUNC, 0644);
  assert(fd >= 0);
  int r = ::ftruncate(fd, size);
  assert(r >= 0);
  ::close(fd);
  return fn;
}

void gen_buffer(char *buffer, uint64_t size)
{
  boost::random::random_device rand;
  rand.generate(buffer, buffer + size);
}

void rm_temp_bdev(string f)
{
  ::unlink(f.c_str());
}

static void bench_iodepth(WALBench &bench, int iodepth, uint64_t iosize)
{
  std::mutex lock;
  std::condition_variable cond;
  std::atomic_int inflight(0);
  char buf[iosize];
  gen_buffer(buf, iosize);
  PerfCollector collector;
  for (int i = 0; i < 5000; i++) {
    if (inflight > iodepth) {
      std::unique_lock<std::mutex> l(lock);
      while (inflight > iodepth)
        cond.wait(l);
    }
    bufferlist bl;
    C_on_commit *c = new C_on_commit(&collector, inflight, lock, cond);
    bufferptr ptr = buffer::create_aligned(iosize, CEPH_PAGE_SIZE);
    ptr.copy_in(0, iosize, buf);
    bl.push_back(ptr);
    bench.queue_wal(bl, c);
    if (i % 1000 == 0)
      collector.print();
  }
  {
    std::unique_lock<std::mutex> l(lock);
    while (inflight)
      cond.wait(l);
  }
}

TEST(WALBench, bench_iodepth_1_4096) {
  uint64_t s = 1024 * 1024 * 1024;
  string fn = get_temp_bdev(s);
  g_ceph_context->_conf->set_val(
    "bluefs_alloc_size",
    "65536");
  g_ceph_context->_conf->apply_changes(NULL);

  {
    WALFileJournal journal(fn, s);
    bench_iodepth(journal, 1, 4096);
  }
  {
    WALRocksDB db(fn, s);
    bench_iodepth(db, 1, 4096);
  }
  rm_temp_bdev(fn);
}

// unaligned
TEST(WALBench, bench_iodepth_1_5124) {
  uint64_t s = 1024 * 1024 * 1024;
  string fn = get_temp_bdev(s);
  g_ceph_context->_conf->set_val(
    "bluefs_alloc_size",
    "65536");
  g_ceph_context->_conf->apply_changes(NULL);

  {
    WALFileJournal journal(fn, s);
    bench_iodepth(journal, 1, 5124);
  }
  {
    WALRocksDB db(fn, s);
    bench_iodepth(db, 1, 5124);
  }
  rm_temp_bdev(fn);
}

// unaligned
TEST(WALBench, bench_iodepth_1_2048) {
  uint64_t s = 1024 * 1024 * 1024;
  string fn = get_temp_bdev(s);
  g_ceph_context->_conf->set_val(
    "bluefs_alloc_size",
    "65536");
  g_ceph_context->_conf->apply_changes(NULL);

  {
    WALFileJournal journal(fn, s);
    bench_iodepth(journal, 1, 2048);
  }
  {
    WALRocksDB db(fn, s);
    bench_iodepth(db, 1, 2048);
  }
  rm_temp_bdev(fn);
}

TEST(WALBench, bench_iodepth_16_4096) {
  uint64_t s = 1024 * 1024 * 1024;
  string fn = get_temp_bdev(s);
  g_ceph_context->_conf->set_val(
    "bluefs_alloc_size",
    "65536");
  g_ceph_context->_conf->apply_changes(NULL);

  {
    WALFileJournal journal(fn, s);
    bench_iodepth(journal, 16, 4096);
  }
  {
    WALRocksDB db(fn, s);
    bench_iodepth(db, 16, 4096);
  }
  rm_temp_bdev(fn);
}

// unaligned
TEST(WALBench, bench_iodepth_16_5124) {
  uint64_t s = 1024 * 1024 * 1024;
  string fn = get_temp_bdev(s);
  g_ceph_context->_conf->set_val(
    "bluefs_alloc_size",
    "65536");
  g_ceph_context->_conf->apply_changes(NULL);

  {
    WALFileJournal journal(fn, s);
    bench_iodepth(journal, 16, 5124);
  }
  {
    WALRocksDB db(fn, s);
    bench_iodepth(db, 16, 5124);
  }
  rm_temp_bdev(fn);
}

int main(int argc, char **argv) {
  vector<const char*> args;
  argv_to_vec(argc, (const char **)argv, args);
  env_to_vec(args);

  vector<const char *> def_args;
  def_args.push_back("--debug-bluefs=0/0");
  def_args.push_back("--debug-bdev=0/0");
  def_args.push_back("--debug-rocksdb=0/0");

  global_init(&def_args, args, CEPH_ENTITY_TYPE_CLIENT, CODE_ENVIRONMENT_UTILITY,
              0);
  common_init_finish(g_ceph_context);
  g_ceph_context->_conf->set_val(
    "enable_experimental_unrecoverable_data_corrupting_features",
    "*");
  g_ceph_context->_conf->apply_changes(NULL);
  Cycles::init();

  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
