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

#define INFRC_MAGIC_CODE     "deadbeaf"

#define INFRC_UDP_BOOT       1 << 1
#define INFRC_UDP_BOOT_ACK   1 << 2
#define INFRC_UDP_PING       1 << 3
#define INFRC_UDP_PONG       1 << 4
#define INFRC_UDP_BROKEN     1 << 5

struct InfRcMsg {
  __le32 tag;
  ceph_entity_addr addr;
  union {
    struct {
      Infiniband::ceph_queue_pair_tuple qpt;
    } __attribute__((packed)) boot;
  } payload;
  __u8 magic_code[8];
} __attribute__((packed));

#define INFRC_MSG_FIRST 1 << 1
#define INFRC_MSG_CONTINUE 1 << 2

class InfRcConnection : public Connection {
  // With 64 KB seglets 1 MB is fractured into 16 or 17 pieces, plus we
  // need an entry for the headers.
  enum { MAX_TX_SGE_COUNT = 24 };
  static const uint32_t MIN_ZERO_COPY_SGE = 500;
  static const uint32_t KEEPALIVE_MIN_PREIOD_MS = 500;
  static const uint32_t KEEPALIVE_TIMEOUT_MS = 5000;
  static const uint32_t KEEPALIVE_RETRY_COUNT = 8;

  Infiniband::Infiniband *infiniband;
  Infiniband::QueuePair *qp;

  // Communicate Management
  InfRcWorker *worker;
  InfRcWorkerPool *pool;
  EventCenter *center;
  Mutex cm_lock;
  uint32_t global_seq;
  uint32_t connect_seq;
  utime_t backoff;         // backoff time
  utime_t last_wakeup;
  utime_t last_ping;
  utime_t last_pong;
  uint64_t out_seq, in_seq;
  int state;
  Messenger::Policy policy;
  int client_setup_socket; // UDP socket for outgoing setup requests
  uint32_t exchange_count;
  uint32_t keepalive_retry;
  set<uint64_t> register_time_events; // need to delete it if stop
  list<Message*> pending_send;
  list<Message*> local_messages;      // local deliver

  bufferlist pending_bl;
  Message *pending_msg;
  list<pair<uint64_t, Message*> > sent_queue;

  uint64_t data_left;
  ceph_msg_header rcv_header;
  ceph_msg_footer rcv_footer;
  bufferlist front, middle, data_bl;

  enum {
    STATE_NEW,
    STATE_BEFORE_CONNECTING,
    STATE_CONNECTING_SENDING,
    STATE_CONNECTING,
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
  EventCallbackRef wakeup_handler;

  void _stop();
  void _fault(bool onlive=false);
  void retry_send(Message *m);
  void handle_other_tag(Infiniband::QueuePairTuple &incoming_qpt);
  void was_session_reset();
  int randomize_out_seq();
  int recv_udp_msg(const char tag, char *buf, size_t len);
  void discard_pending_queue_to(uint64_t seq);
  int send_zero_copy_msg(Message *m, Infiniband::BufferDescriptor *bd);
  int send_zero_copy(bufferlist &bl, Infiniband::BufferDescriptor *bd, Message *m, const char tag);
  int _ready(Infiniband::QueuePairTuple &incoming_qpt,
             Infiniband::QueuePairTuple &outgoing_qpt);
  Infiniband::QueuePairTuple build_qp_tuple();
  void requeue_sent() {
    Message *last_msg = pending_msg;
    if (pending_msg) {
      pending_send.insert(pending_send.begin(), last_msg);
      pending_msg = NULL;
      pending_bl.clear();
    }
    for (list<pair<uint64_t, Message*> >::reverse_iterator it = sent_queue.rbegin();
         it != sent_queue.rend(); ++it) {
      if (last_msg != it->second) {
        last_msg = it->second;
        pending_send.insert(pending_send.begin(), last_msg);
      } else {
        it->second->put();
      }
    }
    sent_queue.clear();
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
    return state == STATE_OPEN;
  }

  void connect();
  int ready(Infiniband::QueuePairTuple &incoming_qpt,
            Infiniband::QueuePairTuple &outgoing_qpt) {
    Mutex::Locker l(cm_lock);
    assert(state == STATE_NEW);
    return _ready(incoming_qpt, outgoing_qpt);
  }
  void send_keepalive();
  void mark_disposable() {
    Mutex::Locker l(cm_lock);
    policy.lossy = true;
  }

 public:
  bool in_queue() const { return !pending_send.empty() || pending_bl.length(); }
  void process();
  void wakeup_from(uint64_t id);
  void local_deliver();
  void cleanup_handler() {
    read_handler.reset();
    write_handler.reset();
    reset_handler.reset();
    remote_reset_handler.reset();
    local_deliver_handler.reset();
    wakeup_handler.reset();
  }
  void process_request(bufferptr &bp);
  Infiniband::QueuePair* get_qp() {
    Mutex::Locker l(cm_lock);
    return qp;
  }
  void ack_message(ibv_wc &wc, Infiniband::BufferDescriptor *bd);
  bool replace(Infiniband::QueuePairTuple &incoming_qpt, Infiniband::QueuePairTuple &outgoing_qpt);
  void wakeup_writer() { center->dispatch_event_external(write_handler); }
  bool send_pending_messages();
  void fault(bool onlive=false) {
    Mutex::Locker l(cm_lock);
    _fault(onlive);
  }
};

typedef boost::intrusive_ptr<InfRcConnection> InfRcConnectionRef;
#endif // CEPH_INFRCCONNECTION_H
