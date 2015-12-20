// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2014 UnitedStack <haomai@unitedstack.com>
 *
 * Author: Haomai Wang <haomaiwang@gmail.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef CEPH_MSG_EVENT_H
#define CEPH_MSG_EVENT_H

#ifdef __APPLE__
#include <AvailabilityMacros.h>
#endif

// We use epoll, kqueue, evport, select in descending order by performance.
#if defined(__linux__)
#define HAVE_EPOLL 1
#endif

#if (defined(__APPLE__) && defined(MAC_OS_X_VERSION_10_6)) || defined(__FreeBSD__) || defined(__OpenBSD__) || defined (__NetBSD__)
#define HAVE_KQUEUE 1
#endif

#ifdef __sun
#include <sys/feature_tests.h>
#ifdef _DTRACE_VERSION
#define HAVE_EVPORT 1
#endif
#endif

#include <pthread.h>

#include "include/atomic.h"
#include "include/Context.h"
#include "include/unordered_map.h"
#include "common/WorkQueue.h"
#include "net_handler.h"

#define EVENT_NONE 0
#define EVENT_READABLE 1
#define EVENT_WRITABLE 2

class EventCenter;

struct FiredFileEvent {
  int fd;
  int mask;
};

/*
 * EventDriver is a wrap of event mechanisms depends on different OS.
 * For example, Linux will use epoll(2), BSD will use kqueue(2) and select will
 * be used for worst condition.
 */
class EventDriver {
 public:
  virtual ~EventDriver() {}       // we want a virtual destructor!!!
  virtual int init(int nevent) = 0;
  virtual int add_event(int fd, int cur_mask, int mask) = 0;
  virtual int del_event(int fd, int cur_mask, int del_mask) = 0;
  virtual int event_wait(vector<FiredFileEvent> &fired_events, struct timeval *tp) = 0;
  virtual int resize_events(int newsize) = 0;
};


using callback_t = std::function<void(uint64_t)>;
/*
 * EventCenter maintain a set of file descriptor and handle registered events.
 */
class EventCenter {
  struct FileEvent {
    int mask;
    callback_t read_cb;
    callback_t write_cb;
    FileEvent(): mask(0) {}
  };

  struct TimeEvent {
    uint64_t id;
    callback_t time_cb;

    TimeEvent(uint64_t i, callback_t cb): id(id), time_cb(std::move(cb)) {}
  };

  CephContext *cct;
  int nevent;
  // Used only to external event
  Mutex external_lock, file_lock, time_lock;
  deque<callback_t> external_events;
  FileEvent *file_events;
  EventDriver *driver;
  map<utime_t, list<TimeEvent> > time_events;
  uint64_t time_event_next_id;
  time_t last_time; // last time process time event
  utime_t next_time; // next wake up time
  int notify_receive_fd;
  int notify_send_fd;
  NetHandler net;
  pthread_t owner;

  int process_time_events();
  FileEvent *_get_file_event(int fd) {
    assert(fd < nevent);
    FileEvent *p = &file_events[fd];
    if (!p->mask)
      new(p) FileEvent();
    return p;
  }

 public:
  atomic_t already_wakeup;

  EventCenter(CephContext *c):
    cct(c), nevent(0),
    external_lock("AsyncMessenger::external_lock"),
    file_lock("AsyncMessenger::file_lock"),
    time_lock("AsyncMessenger::time_lock"),
    file_events(NULL),
    driver(NULL), time_event_next_id(0),
    notify_receive_fd(-1), notify_send_fd(-1), net(c), owner(0), already_wakeup(0) {
    last_time = time(NULL);
  }
  ~EventCenter();
  ostream& _event_prefix(std::ostream *_dout);

  int init(int nevent);
  void set_owner(pthread_t p) { owner = p; }
  pthread_t get_owner() { return owner; }

  // Used by internal thread
  template <typename Func>
  int create_file_event(int fd, int mask, Func &&cb);
  template <typename Func>
  uint64_t create_time_event(uint64_t milliseconds, Func &&callback);
  void delete_file_event(int fd, int mask);
  void delete_time_event(uint64_t id);
  int process_events(int timeout_microseconds);
  void wakeup();

  // Used by external thread
  template <typename Func>
  void dispatch_event_external(Func &&e) {
    external_lock.Lock();
    external_events.emplace_back(std::move(e));
    external_lock.Unlock();
    wakeup();
  }
};

template <typename Func>
int EventCenter::create_file_event(int fd, int mask, Func &&cb)
{
  assert(!(mask & EVENT_READABLE && mask & EVENT_WRITABLE));
  int r = 0;
  Mutex::Locker l(file_lock);
  if (fd >= nevent) {
    int new_size = nevent << 2;
    while (fd > new_size)
      new_size <<= 2;
    r = driver->resize_events(new_size);
    if (r < 0) {
      return -ERANGE;
    }
    FileEvent *new_events = static_cast<FileEvent *>(realloc(file_events, sizeof(FileEvent)*new_size));
    if (!new_events) {
      return -errno;
    }
    file_events = new_events;
    memset(file_events+nevent, 0, sizeof(FileEvent)*(new_size-nevent));
    nevent = new_size;
  }

  EventCenter::FileEvent *event = _get_file_event(fd);
  if (event->mask == mask)
    return 0;

  r = driver->add_event(fd, event->mask, mask);
  if (r < 0) {
    // Actually we don't allow any failed error code, caller doesn't prepare to
    // handle error status. So now we need to assert failure here. In practice,
    // add_event shouldn't report error, otherwise it must be a innermost bug!
    assert(0 == "BUG!");
    return r;
  }

  event->mask |= mask;
  if (mask & EVENT_READABLE) {
    event->read_cb = std::move(cb);
  } else if (mask & EVENT_WRITABLE) {
    event->write_cb = std::move(cb);
  }
  return 0;
}


template <typename Func>
uint64_t EventCenter::create_time_event(uint64_t microseconds, Func &&cb)
{
  Mutex::Locker l(time_lock);
  uint64_t id = time_event_next_id++;

  utime_t expire;
  struct timeval tv;

  if (microseconds < 5) {
    tv.tv_sec = 0;
    tv.tv_usec = microseconds;
  } else {
    expire = ceph_clock_now(cct);
    expire.copy_to_timeval(&tv);
    tv.tv_sec += microseconds / 1000000;
    tv.tv_usec += microseconds % 1000000;
  }
  expire.set_from_timeval(&tv);

  time_events[expire].emplace_back(id, std::move(cb));
  if (expire < next_time)
    wakeup();

  return id;
}


#endif
