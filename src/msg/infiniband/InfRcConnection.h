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
class InfRcMessenger;

class InfRcConnection : public Connection {
  Infiniband::QueuePair *qp;

  // Communicate Management
  InfRcWorker *worker;
  EventCenter *center;
  Mutex cm_lock;
  utime_t backoff;         // backoff time
  utime_t last_connect;
  uint64_t nonce;
  uint64_t in_seq;
  int state;
  Messenger::Policy policy;
  int client_setup_socket; // UDP socket for outgoing setup requests
  int exchange_count;
  set<uint64_t> register_time_events; // need to delete it if stop
  list<Message*> pending_send;
  list<Message*> local_messages;    // local deliver
  enum {
    STATE_NEW,
    STATE_BEFORE_CONNECTING,
    STATE_CONNECTING,
    STATE_CONNECTING_READY,
    STATE_ACCEPTING,
    STATE_OPEN,
    STATE_STANDBY,
    STATE_CLOSED
  };

  static const char *get_state_name(int state) {
    const char* const statenames[] = {"STATE_NEW",
                                      "STATE_BEFORE_CONNECTING",
                                      "STATE_CONNECTING",
                                      "STATE_CONNECTING_READY",
                                      "STATE_ACCEPTING",
                                      "STATE_OPEN",
                                      "STATE_STANDBY",
                                      "STATE_CLOSED"};
    return statenames[state];
  }

  EventCallbackRef read_handler;
  EventCallbackRef reset_handler;
  EventCallbackRef remote_reset_handler;
  EventCallbackRef local_deliver_handler;

  static int client_try_send_qp(int client_setup_socket, Infiniband::QueuePairTuple *outgoing_qpt,
                                Infiniband::QueuePairTuple *incoming_qpt);
  void _stop();
  void _fault(list<Message*> *messages=NULL);
  void retry_send(Message *m);

 public:
  InfRcMessenger *infrc_msgr;
  atomic_t out_seq;

  InfRcConnection(CephContext *c, InfRcMessenger *m, InfRcWorker *w, Infiniband::QueuePair *qp,
                  const entity_addr_t& addr, int type);
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
  int ready(Infiniband::QueuePairTuple &incoming_qpt);
  void send_keepalive() {}
  void mark_disposable() {
    Mutex::Locker l(cm_lock);
    policy.lossy = true;
  }

 public:
  Infiniband::QueuePairTuple build_qp_tuple(uint64_t nonce);
  void process();
  void wakeup_from(uint64_t id);
  void local_deliver();
  void cleanup_handler() {
    read_handler.reset();
    reset_handler.reset();
    remote_reset_handler.reset();
    local_deliver_handler.reset();
  }
  void process_request(Message *m);
  void fault(list<Message*> *messages) {
    Mutex::Locker l(cm_lock);
    _fault(messages);
  }
  Infiniband::QueuePair* get_qp() {
    Mutex::Locker l(cm_lock);
    return qp;
  }
};

typedef boost::intrusive_ptr<InfRcConnection> InfRcConnectionRef;
#endif // CEPH_INFRCCONNECTION_H
