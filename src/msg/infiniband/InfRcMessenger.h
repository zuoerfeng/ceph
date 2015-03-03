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

#ifndef CEPH_INFRCMESSENGER_H
#define CEPH_INFRCMESSENGER_H

#include "include/atomic.h"
#include "common/Thread.h"
#include "common/Mutex.h"
#include "msg/SimplePolicyMessenger.h"
#include "msg/async/Event.h"
#include "Infiniband.h"
#include "InfRcConnection.h"


class InfRcWorker;

class InfRcWorkerPool: public CephContext::AssociatedSingletonObject {
  typedef Infiniband::BufferDescriptor BufferDescriptor;
  typedef Infiniband::RegisteredBuffers RegisteredBuffers;

  InfRcWorkerPool(const InfRcWorkerPool &);
  InfRcWorkerPool& operator=(const InfRcWorkerPool &);

  CephContext *cct;
  uint64_t seq;
  bool started;
  vector<InfRcWorker*> workers;
  vector<int> coreids;
  Mutex barrier_lock;
  Cond barrier_cond;
  atomic_t barrier_count;

  class C_barrier : public EventCallback {
    InfRcWorkerPool *pool;
   public:
    C_barrier(InfRcWorkerPool *p): pool(p) {}
    void do_request(int id) {
      Mutex::Locker l(pool->barrier_lock);
      pool->barrier_count.dec();
      pool->barrier_cond.Signal();
    }
  };
  friend class C_barrier;

  string ib_device_name;            // physical device name on the HCA
  int ib_physical_port;             // physical port number on the HCA
  int lid;                          // local id for this HCA and physical port

  static const uint32_t MAX_MESSAGE_LEN = ((1 << 23) + 200);
  // Since we always use at most 1 SGE per receive request, there is no need
  // to set this parameter any higher. In fact, larger values for this
  // parameter result in increased descriptor size, which means that the
  // Infiniband controller needs to fetch more data from host memory,
  // which results in a higher number of on-controller cache misses.
  static const uint32_t MAX_SHARED_RX_SGE_COUNT = 1;
  static const uint32_t MAX_TX_QUEUE_DEPTH = 16;
  // With 64 KB seglets 1 MB is fractured into 16 or 17 pieces, plus we
  // need an entry for the headers.
  enum { MAX_TX_SGE_COUNT = 24 };
  static const uint32_t MAX_SHARED_RX_QUEUE_DEPTH = 32;

  /**
   * The number of client receive buffers that are in use, either from
   * oustanding_buffers or from RPC responses that have borrowed these
   * buffers and will return them with the PayloadChunk mechanism.
   * Invariant: num_used_srq_buffers <= MAX_SHARED_RX_QUEUE_DEPTH.
   */
  RegisteredBuffers *rx_buffers;    // Infiniband receive buffers, written directly by the HCA.
  RegisteredBuffers *tx_buffers;    // Infiniband transmit buffers.
  ibv_srq*         srq;             // shared receive work queue

  Mutex lock;                       // protect `num_used_srq_buffers`, `pending_sent_conns`
                                    // `free_tx_buffers`
  uint32_t num_used_srq_buffers;
  vector<BufferDescriptor*> free_tx_buffers;
  list<InfRcConnectionRef> pending_sent_conns;

 public:
  /// Starting address of the region registered with the HCA for zero-copy
  /// transmission, if any. If no region is registered then 0.
  /// See registerMemory().
  uintptr_t log_memory_base;

  /// Length of the region starting at #log_memory_base which is registered
  /// with the HCA for zero-copy transmission. If no region is registered
  /// then 0. See registerMemory().
  size_t log_memory_bytes;

  /// Infiniband memory region of the region registered with the HCA for
  /// zero-copy transmission. If no region is registered then NULL.
  /// See registerMemory().
  ibv_mr* log_memory_region;

  /**
   * Used by this class to make all Infiniband verb calls.  In normal
   * production use it points to #realInfiniband; for testing it points to a
   * mock object.
   */
  Infiniband *infiniband;

  InfRcWorkerPool(CephContext *c);
  virtual ~InfRcWorkerPool();
  int start();
  static string get_name(string name, int port) {
    string s("InfRcWorkerPool-");
    char buf[64];
    int n = snprintf(buf, sizeof(buf), "%d", port);
    s.append(name);
    s.append(port, n);
    return s;
  }
  InfRcWorker *get_worker() {
    return workers[(seq++)%workers.size()];
  }
  int get_cpuid(int id) {
    if (coreids.empty())
      return -1;
    return coreids[id % coreids.size()];
  }
  void barrier();
  void shutdown();
  BufferDescriptor* reserve_message_buffer(InfRcConnectionRef c);
  int post_srq_receive(const char *buf) {
    BufferDescriptor *bd = rx_buffers->get_descriptor(buf);
    assert(bd);
    return post_srq_receive(bd);
  }
  int post_srq_receive(BufferDescriptor *bd) {
    if (infiniband->post_srq_receive(srq, bd))
      return -1;
    Mutex::Locker l(lock);
    --num_used_srq_buffers;
    return 0;
  }
  int post_tx_buffer(BufferDescriptor *bd, bool wakeup);
  uint32_t incr_used_srq_buffers() {
    Mutex::Locker l(lock);
    ++num_used_srq_buffers;
    return num_used_srq_buffers - 1;
  }
  int get_ib_physical_port() { return ib_physical_port; }
  int get_lid() { return lid; }
  ibv_srq* get_srq() { return srq; }
};


class InfRcWorker : public Thread {
  typedef Infiniband::BufferDescriptor BufferDescriptor;
  typedef Infiniband::QueuePair QueuePair;
  typedef Infiniband::CompletionQueue CompletionQueue;
  typedef Infiniband::CompletionChannel CompletionChannel;
  typedef Infiniband::QueuePairTuple QueuePairTuple;

  CephContext *cct;
  InfRcWorkerPool *pool;
  bool done;
  int id;

  // Since we always use at most 1 SGE per receive request, there is no need
  // to set this parameter any higher. In fact, larger values for this
  // parameter result in increased descriptor size, which means that the
  // Infiniband controller needs to fetch more data from host memory,
  // which results in a higher number of on-controller cache misses.
  static const uint32_t MAX_TX_QUEUE_DEPTH = 16;
  static const uint32_t MIN_PREFETCH_LEN = 1024;
  static const uint32_t MAX_SHARED_RX_QUEUE_DEPTH = 32;

  static const uint32_t MIN_ACK_LEN = 128;
  /**
   * Used by this class to make all Infiniband verb calls.  In normal
   * production use it points to #realInfiniband; for testing it points to a
   * mock object.
   */
  Infiniband *infiniband;
  CompletionQueue* rx_cq;           // completion queue for incoming requests
  CompletionQueue* tx_cq;           // common completion queue for all transmits
  uint32_t rxcq_events_need_ack;
  uint32_t txcq_events_need_ack;
  CompletionChannel* rx_cc;         // completion channel for incoming requests
  CompletionChannel* tx_cc;         // completion channel for all transmits
                                    // -1 means we're not a server
    /// true, specifying we haven't learned our addr; set false when we find it.
  // maybe this should be protected by the lock?

  Mutex lock;                       // protect `oustanding_buffers`, `qp_conns`,
                                    // `dead_queue_pairs` whether to reap queue pair

  // RPCs which are awaiting their responses from the network.
  ceph::unordered_map<uint64_t, InfRcConnectionRef> outstanding_buffers;

  // qp_num -> InfRcConnection
  // The main usage of `qp_conns` is looking up connection by qp_num,
  // so the lifecycle of element in `qp_conns` is the lifecycle of qp.
  //// make qp queue into dead state
  /**
   * 1. Connection call mark_down
   * 2. Move the Queue Pair into the Error state(QueuePair::to_dead)
   * 3. Wait for the affiliated event IBV_EVENT_QP_LAST_WQE_REACHED(handle_async_event)
   * 4. Wait for CQ to be empty(handle_tx_event)
   * 5. Destroy the QP by calling ibv_destroy_qp()(handle_tx_event)
   *
   * @param qp The qp needed to dead
   */
  ceph::unordered_map<uint64_t, pair<QueuePair*, InfRcConnectionRef> > qp_conns;
  /// if a queue pair is closed when transmit buffers are active
  /// on it, the transmit buffers never get returned via tx_cq.  To
  /// work around this problem, don't delete queue pairs immediately. Instead,
  /// save them in this vector and delete them at a safe time, when there are
  /// no outstanding transmit buffers to be lost.
  vector<QueuePair*> dead_queue_pairs;

  int send_zero_copy(Message *m, InfRcConnectionRef conn, QueuePair* qp, BufferDescriptor *bd);

 public:
  EventCenter center;
  InfRcWorker(CephContext *c, InfRcWorkerPool *p, int i);
  virtual ~InfRcWorker() { shutdown(); }

  int init();
  void *entry();
  int start();
  void stop();
  void shutdown();

  // queue pair connection setup helpers
  /**
   * Create a connection associated with the given entity (of the given type).
   * Initiate the connection. (This function returning does not guarantee
   * connection success.)
   *
   * @param addr The address of the entity to connect to.
   * @param type The peer type of the entity at the address.
   * @param msg an initial message to queue on the new connection
   *
   * @return a pointer to the newly-created connection. Caller does not own a
   * reference; take one if you need it.
   */
  InfRcConnectionRef create_connection(const entity_addr_t &dest, int type, InfRcMessenger *m);
  int submit_message(Message *m, InfRcConnectionRef conn);
  void send_pending_messages(bool locked);
  void handle_rx_event();
  void handle_tx_event();
  void handle_async_event();
  int get_lid() { return pool->get_lid(); }
  BufferDescriptor* get_message_buffer(InfRcConnectionRef conn);
  void put_message_buffer(BufferDescriptor *bd);
};


class InfRcMessenger : public SimplePolicyMessenger {
 public:
  /**
   * Initialize the InfRcMessenger!
   *
   * @param cct The CephContext to use
   * @param name The name to assign ourselves
   * _nonce A unique ID to use for this InfRcMessenger. It should not
   * be a value that will be repeated if the daemon restarts.
   */
  InfRcMessenger(CephContext *cct, entity_name_t name,
                 string mname, uint64_t _nonce);

  /**
   * Destroy the InfRcMessenger. Pretty simple since all the work is done
   * elsewhere.
   */
  virtual ~InfRcMessenger();

  /** @defgroup Accessors
   * @{
   */
  void set_addr_unknowns(entity_addr_t& addr);

  int get_dispatch_queue_len() {
    return 0;
  }

  double get_dispatch_queue_max_age(utime_t now) {
    return 0;
  }
  /** @} Accessors */

  /**
   * @defgroup Configuration functions
   * @{
   */
  void set_cluster_protocol(int p) {
    assert(!started && server_setup_socket == -1);
    cluster_protocol = p;
  }

  int bind(const entity_addr_t& bind_addr);
  int rebind(const set<int>& avoid_ports);

  /** @} Configuration functions */

  /**
   * @defgroup Startup/Shutdown
   * @{
   */
  virtual int start();
  virtual void wait();
  virtual int shutdown();

  /** @} // Startup/Shutdown */

  /**
   * @defgroup Messaging
   * @{
   */
  virtual int send_message(Message *m, const entity_inst_t& dest) {
    Mutex::Locker l(lock);
    return 0;
    //return _send_message(m, dest);
  }

  /** @} // Messaging */

  /**
   * @defgroup Connection Management
   * @{
   */
  virtual ConnectionRef get_connection(const entity_inst_t& dest);
  virtual ConnectionRef get_loopback_connection() { return local_connection; }
  int send_keepalive(Connection *con);
  virtual void mark_down(const entity_addr_t& addr);
  virtual void mark_down_all();
  /** @} // Connection Management */

  /**
   * @defgroup Inner classes
   * @{
   */

 protected:
  /**
   * @defgroup Messenger Interfaces
   * @{
   */
  /**
   * Start up the DispatchQueue thread once we have somebody to dispatch to.
   */
  virtual void ready();
  /** @} // Messenger Interfaces */

 private:

  /**
   * @defgroup Utility functions
   * @{
   */

  int _bind(const entity_addr_t& bind_addr, const set<int>& avoid_ports);

  InfRcConnectionRef _lookup_conn(const entity_addr_t& k) {
    ceph::unordered_map<entity_addr_t, InfRcConnectionRef>::iterator p = connections.find(k);
    if (p == connections.end())
      return NULL;

    // lazy delete, see "deleted_conns"
    Mutex::Locker l(deleted_lock);
    if (deleted_conns.count(p->second)) {
      deleted_conns.erase(p->second);
      connections.erase(p);
      return NULL;
    }

    return p->second;
  }

  InfRcWorkerPool *pool;
  bool started;
  bool stopped;
  int server_setup_socket; // UDP socket for incoming setup requests;
                           // -1 means we're not a server
  // InfRcMessenger stuff
  /// approximately unique ID set by the Constructor for use in entity_addr_t
  atomic_t nonce;
  bool need_addr;

  /// internal cluster protocol version, if any, for talking to entities of the same type.
  int cluster_protocol;

  Cond stop_cond;

  /// counter for the global seq our connection protocol uses
  __u32 global_seq;
  /// lock to protect the global_seq
  ceph_spinlock_t global_seq_lock;

  Mutex lock;
  ceph::unordered_map<entity_addr_t, InfRcConnectionRef> connections;
  set<InfRcConnectionRef> pending_conns;

  Mutex deleted_lock;
  set<InfRcConnectionRef> deleted_conns;

  /// Pool allocator for our ServerRpc objects.
  // ServerRpcPool<ServerRpc> serverRpcPool;

  /// Allocator for ClientRpc objects.
  // ObjectPool<ClientRpc> clientRpcPool;

 void _init_local_connection() {
    assert(lock.is_locked());
    local_connection->peer_addr = my_inst.addr;
    local_connection->peer_type = my_inst.name.type();
    ms_deliver_handle_fast_connect(local_connection.get());
  }

 public:

  InfRcWorker *server_setup_worker;
  /// con used for sending messages to ourselves
  ConnectionRef local_connection;

  /**
   * @defgroup InfRcMessenger internals
   * @{
   */
  InfRcConnectionRef lookup_conn(const entity_addr_t& k) {
    Mutex::Locker l(lock);
    return _lookup_conn(k);
  }

  void learned_addr(const entity_addr_t &peer_addr_for_me);
  InfRcConnectionRef add_accept(int sd);

  /**
   * This wraps ms_deliver_get_authorizer. We use it for InfRcConnection.
   */
  AuthAuthorizer *get_authorizer(int peer_type, bool force_new) {
    return ms_deliver_get_authorizer(peer_type, force_new);
  }

  /**
   * This wraps ms_deliver_verify_authorizer; we use it for InfRcConnection.
   */
  bool verify_authorizer(Connection *con, int peer_type, int protocol, bufferlist& auth, bufferlist& auth_reply,
                         bool& isvalid, CryptoKey& session_key) {
    return ms_deliver_verify_authorizer(con, peer_type, protocol, auth,
                                        auth_reply, isvalid, session_key);
  }
  /**
   * Increment the global sequence for this InfRcMessenger and return it.
   * This is for the connect protocol, although it doesn't hurt if somebody
   * else calls it.
   *
   * @return a global sequence ID that nobody else has seen.
   */
  __u32 get_global_seq(__u32 old=0) {
    ceph_spin_lock(&global_seq_lock);
    if (old > global_seq)
      global_seq = old;
    __u32 ret = ++global_seq;
    ceph_spin_unlock(&global_seq_lock);
    return ret;
  }
  /**
   * Get the protocol version we support for the given peer type: either
   * a peer protocol (if it matches our own), the protocol version for the
   * peer (if we're connecting), or our protocol version (if we're accepting).
   */
  int get_proto_version(int peer_type, bool connect);

  int accept_conn(InfRcConnectionRef conn) {
    Mutex::Locker l(lock);
    if (connections.count(conn->get_peer_addr())) {
      InfRcConnectionRef existing = connections[conn->get_peer_addr()];

      // lazy delete, see "deleted_conns"
      // If conn already in, we will return 0
      Mutex::Locker l(deleted_lock);
      if (deleted_conns.count(existing)) {
        deleted_conns.erase(existing);
      } else if (conn != existing) {
        return -1;
      }
    }
    connections[conn->get_peer_addr()] = conn;
    pending_conns.erase(conn);
    return 0;

  }

  /**
   * Unregister connection from `conns`
   * `external` is used to indicate whether need to lock InfRcMessenger::lock,
   * it may call. If external is false, it means that InfRcConnection take the
   * initiative to unregister
   */
  void unregister_conn(InfRcConnectionRef conn) {
    Mutex::Locker l(deleted_lock);
    deleted_conns.insert(conn);
  }

  void accept(Infiniband::QueuePairTuple &incoming_qpt, entity_addr_t &socket_addr);
  void handle_ping(entity_addr_t addr);
  void recv_message();
  int recv_udp_msg(int sd, InfRcMsg &msg, uint8_t extag, entity_addr_t *addr=NULL);
  int send_udp_msg(int sd, const char tag, char *buf, size_t len, entity_addr_t &peer_ddr, const entity_addr_t &myaddr);
  /**
   * @} // InfRcMessenger Internals
   */

};

#endif // CEPH_INFRCMESSENGER_H
