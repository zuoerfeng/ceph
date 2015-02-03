// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2014 Haomai Wang <haomaiwang@gmail.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 *
 */

#ifndef CEPH_INFRCCONNECTION_H
#define CEPH_INFRCCONNECTION_H

#include <map>
#include <list>
using namespace std;

#include "msg/Connection.h"
#include "Infiniband.h"

class InfRcWorker;
class InfRcWorkerPool;
class InfRcMessenger;

class InfRcConnection : public Connection {
  // With 64 KB seglets 1 MB is fractured into 16 or 17 pieces, plus we
  // need an entry for the headers.
  enum { MAX_TX_SGE_COUNT = 24 };
  static const uint32_t MIN_ZERO_COPY_SGE = 500;
  static const uint32_t MAX_MESSAGE_LEN = ((1 << 23) + 200);

  Infiniband::QueuePair *qp;

  // Communicate Management
  InfRcWorker *worker;
  InfRcWorkerPool *pool;
  EventCenter *center;
  Mutex cm_lock;
  uint32_t global_seq;
  uint32_t connect_seq;
  utime_t backoff;         // backoff time
  utime_t last_connect;
  uint64_t out_seq, in_seq;
  int state;
  Messenger::Policy policy;
  int client_setup_socket; // UDP socket for outgoing setup requests
  int exchange_count;
  set<uint64_t> register_time_events; // need to delete it if stop
  list<Message*> pending_send;
  list<Message*> local_messages;      // local deliver
  list<pair<uint64_t, Message*> > sent;
  Infiniband::Infiniband *infiniband;
  enum {
    STATE_NEW,
    STATE_BEFORE_CONNECTING,
    STATE_CONNECTING_SENDING,
    STATE_CONNECTING,
    STATE_CONNECTING_READY,
    STATE_OPEN,
    STATE_STANDBY,
    STATE_CLOSED,
    STATE_WAIT
  };

  static const char *get_state_name(int state) {
    const char* const statenames[] = {"STATE_NEW",
                                      "STATE_BEFORE_CONNECTING",
                                      "STATE_CONNECTING_SENDING",
                                      "STATE_CONNECTING",
                                      "STATE_CONNECTING_READY",
                                      "STATE_OPEN",
                                      "STATE_STANDBY",
                                      "STATE_CLOSED",
                                      "STATE_WAIT"};
    return statenames[state];
  }

  EventCallbackRef read_handler;
  EventCallbackRef write_handler;
  EventCallbackRef reset_handler;
  EventCallbackRef remote_reset_handler;
  EventCallbackRef local_deliver_handler;

  static int client_try_send_qp(int client_setup_socket, Infiniband::QueuePairTuple *outgoing_qpt,
                                Infiniband::QueuePairTuple *incoming_qpt);
  void _stop();
  void _fault();
  void retry_send(Message *m);
  void handle_other_tag(Infiniband::QueuePairTuple &incoming_qpt);
  void was_session_reset();
  int randomize_out_seq();
  void discard_pending_queue_to(uint64_t seq);
  int send_zero_copy(Message *m, Infiniband::BufferDescriptor *bd);
  int _ready(Infiniband::QueuePairTuple &incoming_qpt,
             Infiniband::QueuePairTuple &outgoing_qpt);
  Infiniband::QueuePairTuple build_qp_tuple();
  void requeue_sent() {
    for (list<pair<uint64_t, Message*> >::reverse_iterator it = sent.rbegin();
         it != sent.rend(); ++it)
      pending_send.insert(pending_send.begin(), it->second);
    sent.clear();
  }

 public:
  InfRcMessenger *infrc_msgr;

  InfRcConnection(CephContext *c, InfRcMessenger *m, InfRcWorker *w,
                  InfRcWorkerPool *p, Infiniband::QueuePair *qp,
                  Infiniband *ib, const entity_addr_t& addr, int type);
  virtual ~InfRcConnection();

  ostream& _conn_prefix(std::ostream *_dout);

  int send_message(Message *m);
  void mark_down() {
    Mutex::Locker l(cm_lock);
    _stop();
  }

  bool is_connected() {
    Mutex::Locker l(cm_lock);
    return state == STATE_OPEN || state == STATE_CONNECTING_READY;
  }

  void connect();
  int ready(Infiniband::QueuePairTuple &incoming_qpt,
            Infiniband::QueuePairTuple &outgoing_qpt) {
    Mutex::Locker l(cm_lock);
    assert(state == STATE_NEW);
    return _ready(incoming_qpt, outgoing_qpt);
  }
  void send_keepalive() {}
  void mark_disposable() {
    Mutex::Locker l(cm_lock);
    policy.lossy = true;
  }

 public:
  void process();
  void wakeup_from(uint64_t id);
  void local_deliver();
  void cleanup_handler() {
    read_handler.reset();
    write_handler.reset();
    reset_handler.reset();
    remote_reset_handler.reset();
    local_deliver_handler.reset();
  }
  void process_request(Message *m);
  Infiniband::QueuePair* get_qp() {
    Mutex::Locker l(cm_lock);
    return qp;
  }
  void ack_message(ibv_wc &wc, Infiniband::BufferDescriptor *bd);
  bool replace(Infiniband::QueuePairTuple &incoming_qpt, Infiniband::QueuePairTuple &outgoing_qpt);
  void wakeup_writer() { center->dispatch_event_external(write_handler); }
  bool send_pending_messages();
  void fault() {
    Mutex::Locker l(cm_lock);
    _fault();
  }
};

typedef boost::intrusive_ptr<InfRcConnection> InfRcConnectionRef;
#endif // CEPH_INFRCCONNECTION_H
