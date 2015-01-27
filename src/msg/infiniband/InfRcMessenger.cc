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

#include <sys/socket.h>
#ifdef HAVE_SCHED
#include <sched.h>
#endif

#include "auth/Crypto.h"
#include "common/config.h"
#include "InfRcMessenger.h"

#define dout_subsys ceph_subsys_ms
#undef dout_prefix
#define dout_prefix _prefix(_dout, this)
static ostream& _prefix(std::ostream *_dout, InfRcMessenger *m) {
  return *_dout << "-- " << m->get_myaddr() << " ";
}

static ostream& _prefix(std::ostream *_dout, InfRcWorker *w) {
  return *_dout << " InfRcWorker(" << w << ") ";
}

static ostream& _prefix(std::ostream *_dout, InfRcWorkerPool *p) {
  return *_dout << " InfRcWorkerPool--";
}


/**
 * \file
 * Implementation of an Infiniband reliable transport layer using reliable
 * connected queue pairs. Handshaking is done over IP/UDP and addressing
 * is based on that, i.e. addresses look like normal IP/UDP addresses
 * because the infiniband queue pair set up is bootstrapped over UDP.
 *
 * The transport uses common pools of receive and transmit buffers that
 * are pre-registered with the HCA for direct access. All receive buffers
 * are placed on two shared receive queues (one for issuing RPCs and one for
 * servicing RPCs), which avoids having to allocate buffers to individual
 * receive queues for each client queue pair (this would be costly for many
 * queue pairs, and wasteful if they're idle). The shared receive queues can be
 * associated with many queue pairs, and each shared receive queue has its own
 * completion queue.
 *
 * In short, the receive path looks like the following:
 *  - As a server, we have just one completion queue for all incoming client
 *    queue pairs.
 *  - As a client, we have just one completion queue for all outgoing client
 *    queue pairs.
 *
 * For the transmit path, we have one completion queue for all cases, since
 * we currently do synchronous sends.
 *
 * Each receive and transmit buffer is sized large enough for the maximum
 * possible RPC size for simplicity. Note that if a node sends to another node
 * that does not have a sufficiently large receive buffer at the head of its
 * receive queue, _both_ ends will get an error (IBV_WC_REM_INV_REQ_ERR on the
 * sender, and IBV_WC_LOC_LEN_ERR on the receiver)! The HCA will _not_ search
 * the receive queue to find a larger posted buffer, nor will it scatter the
 * incoming data over multiple posted buffers. You have been warned.
 *
 * To reference the buffer associated with each work queue element on the shared
 * receive queue, we stash pointers in the 64-bit `wr_id' field of the work
 * request.
 *
 * Connected queue pairs require some bootstrapping, which we do as follows:
 *  - The server maintains a UDP listen port.
 *  - Clients establish QPs by sending their tuples to the server as a request.
 *    Tuples are basically (address, queue pair number, sequence number),
 *    similar to TCP. Think of this as TCP's SYN packet.
 *  - Servers receive client tuples, create an associated queue pair, and
 *    reply via UDP with their QP's tuple. Think of this as TCP's SYN/ACK.
 *  - Clients receive the server's tuple reply and complete their queue pair
 *    setup. Communication over infiniband is ready to go.
 *
 * Of course, using UDP means these things can get lost. We should have a
 * mechanism for cleaning up halfway-completed QPs that occur when clients
 * die before completing or never get the server's UDP response. Similarly,
 * clients right now block forever if the request is lost. They should time
 * out and retry, although at what level retries should occur isn't clear.
 */

/*
 * Random Notes:
 *  1) ibv_reg_mr() takes about 30usec to register one 4k page on the E5620.
 *     8MB takes about 1.25msec.  This implies that we can't afford to register
 *     on the fly.
 */


void infrc_buffer_post_buffer(const char *buf, void *ctxt)
{
  InfRcWorkerPool *pool = static_cast<InfRcWorkerPool*>(ctxt);
  pool->post_srq_receive(buf);
}

class C_infrc_handle_accept : public EventCallback {
  InfRcMessenger *msgr;

 public:
  C_infrc_handle_accept(InfRcMessenger *m): msgr(m) {}
  void do_request(int id) {
    msgr->recv_connect();
  }
};

class C_handle_cq_rx : public EventCallback {
  InfRcWorker *worker;

 public:
  C_handle_cq_rx(InfRcWorker *w): worker(w) {}
  void do_request(int fd) {
    worker->handle_rx_event();
  }
};

class C_handle_cq_tx : public EventCallback {
  InfRcWorker *worker;

 public:
  C_handle_cq_tx(InfRcWorker *w): worker(w) {}
  void do_request(int fd) {
    worker->handle_tx_event();
  }
};

class C_infrc_handle_dispatch : public EventCallback {
  InfRcMessenger *msgr;
  Message *m;

 public:
  C_infrc_handle_dispatch(InfRcMessenger *msgr, Message *m): msgr(msgr), m(m) {}
  void do_request(int id) {
    msgr->ms_deliver_dispatch(m);
  }
};

class C_infrc_async_ev_handler : public EventCallback {
  InfRcWorker *worker;

 public:
  C_infrc_async_ev_handler(InfRcWorker *w): worker(w) {}
  void do_request(int id) {
    worker->handle_async_event();
  }
};

class C_infrc_send_message : public EventCallback {
  InfRcWorker *worker;

 public:
  C_infrc_send_message(InfRcWorker *w): worker(w) {}
  void do_request(int id) {
    worker->send_pending_messages(false);
  }
};

//------------------------------
// InfRcWorker class
//------------------------------
InfRcWorker::InfRcWorker(CephContext *c, InfRcWorkerPool *p, int i)
  : cct(c), pool(p), done(false), id(i), infiniband(p->infiniband),
    rx_cq(NULL), tx_cq(NULL), rxcq_events_need_ack(0), txcq_events_need_ack(0),
    rx_cc(NULL), tx_cc(NULL), nonce(0), lock("InfRcWorker::lock"),
    center(c)
{
  center.init(5000);
}

// Don't need lock
int InfRcWorker::start()
{
  assert(!done);
  ldout(cct, 20) << __func__ << dendl;
  rx_cc = infiniband->create_comp_channel();
  if (!rx_cc)
    goto err;

  rx_cq = infiniband->create_comp_queue(MAX_SHARED_RX_QUEUE_DEPTH, rx_cc);
  if (!rx_cq)
    goto err;

  center.create_file_event(rx_cc->get_fd(), EVENT_READABLE,
                           EventCallbackRef(new C_handle_cq_rx(this)));

  tx_cc = infiniband->create_comp_channel();
  if (!tx_cc)
    goto err;

  tx_cq = infiniband->create_comp_queue(MAX_TX_QUEUE_DEPTH, tx_cc);
  if (!tx_cq)
    goto err;
  center.create_file_event(tx_cc->get_fd(), EVENT_READABLE,
                           EventCallbackRef(new C_handle_cq_tx(this)));

  create();
  return 0;

err:
  shutdown();
  return -1;
}

void InfRcWorker::stop()
{
  ldout(cct, 10) << __func__ << dendl;
  done = true;
  center.wakeup();
}

void *InfRcWorker::entry()
{
  ldout(cct, 10) << __func__ << " starting" << dendl;
  if (cct->_conf->ms_async_set_affinity) {
#ifdef HAVE_SCHED
    int cpuid;
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);

    cpuid = pool->get_cpuid(id);
    if (cpuid < 0) {
      cpuid = sched_getcpu();
    }

    if (cpuid < CPU_SETSIZE) {
      CPU_SET(cpuid, &cpuset);

      if (sched_setaffinity(0, sizeof(cpuset), &cpuset) < 0) {
        ldout(cct, 0) << __func__ << " sched_setaffinity failed: "
            << cpp_strerror(errno) << dendl;
      }
      /* guaranteed to take effect immediately */
      sched_yield();
    }
#endif
  }

  center.set_owner(pthread_self());
  while (!done) {
    ldout(cct, 20) << __func__ << " calling event process" << dendl;

    int r = center.process_events(30000000);
    if (r < 0) {
      ldout(cct, 20) << __func__ << " process events failed: "
          << cpp_strerror(errno) << dendl;
      // TODO do something?
    }
  }

  return 0;
}

void InfRcWorker::shutdown()
{
  ldout(cct, 20) << __func__ << dendl;
  if (rx_cq) {
    delete rx_cq;
    rx_cq = NULL;
  }
  if (tx_cq) {
    delete tx_cq;
    tx_cq = NULL;
  }
  if (rx_cc) {
    delete rx_cc;
    rx_cc = NULL;
  }
  if (tx_cc) {
    delete tx_cc;
    tx_cc = NULL;
  }

  Mutex::Locker l(lock);
  while (!client_send_queue.empty()) {
    pair<Message*, QueuePair*> p = client_send_queue.front();
    client_send_queue.pop_front();
    p.first->put();
  }
  client_send_queue.clear();
  for (ceph::unordered_map<uint64_t, Message*>::iterator it = outstanding_messages.begin();
       it != outstanding_messages.end(); ++it)
    it->second->put();
  outstanding_messages.clear();
  for (ceph::unordered_map<uint64_t, pair<QueuePair*, InfRcConnectionRef> >::iterator it = qp_conns.begin();
       it != qp_conns.end(); ++it)
    it->second.second->mark_down();
  qp_conns.clear();
  while (!dead_queue_pairs.empty()) {
    delete dead_queue_pairs.back();
    dead_queue_pairs.pop_back();
  }
}

/**
 * Attempt to set up a QueuePair with the given server. The client
 * allocates a QueuePair and sends the necessary tuple to the
 * server to begin the handshake. The server then replies with its
 * QueuePair tuple information. This is all done over IP/UDP.
 */
InfRcConnectionRef InfRcWorker::create_connection(const entity_addr_t& dest, int type, InfRcMessenger *m)
{
  ldout(cct, 20) << __func__ << " dest=" << dest << " type=" << type << " messenger=" << m << dendl;
  QueuePair *qp = infiniband->create_queue_pair(IBV_QPT_RC, pool->get_ib_physical_port(),
                                                pool->get_srq(), tx_cq->get_cq(), rx_cq->get_cq(),
                                                MAX_TX_QUEUE_DEPTH,
                                                MAX_SHARED_RX_QUEUE_DEPTH);
  // create connection
  InfRcConnectionRef conn = new InfRcConnection(cct, m, this, qp, dest, type);
  Mutex::Locker l(lock);
  assert(!qp_conns.count(qp->get_local_qp_number()));
  qp_conns[qp->get_local_qp_number()] = make_pair(qp, conn);
  return conn;
}

void InfRcWorker::process_request(bufferptr &bp, uint32_t qpnum)
{
  ldout(cct, 20) << __func__ << " bp(" << bp.length() << ")=" << bp << " qpnum=" << qpnum << dendl;
  bufferlist front, middle, data;
  ceph_msg_header &header(*reinterpret_cast<ceph_msg_header*>(bp.c_str()));
  ceph_msg_footer &footer(*reinterpret_cast<ceph_msg_footer*>(bp.c_str()+bp.length()-sizeof(ceph_msg_footer)));
  uint32_t total_len = header.front_len + header.middle_len + header.data_len + sizeof(ceph_msg_header) + sizeof(ceph_msg_footer);
  if (total_len != bp.length()) {
    // FIXME do what?
    assert(0);
  }

  uint32_t offset = sizeof(ceph_msg_header);
  front.append(bp, offset, header.front_len);
  offset += header.front_len;
  middle.append(bp, offset, header.middle_len);
  offset += header.middle_len;
  data.append(bp, offset, header.data_len);
  Message *message = decode_message(cct, 0, header, footer, front, middle, data);
  if (!message) {
    ldout(cct, 1) << __func__ << " decode message failed, dropped" << dendl;
    return ;
  }
  utime_t now;
  message->set_recv_stamp(now);
  message->set_throttle_stamp(now);
  message->set_recv_complete_stamp(now);
  // check received seq#.  if it is old, drop the message.  
  // note that incoming messages may skip ahead.  this is convenient for the client
  // side queueing because messages can't be renumbered, but the (kernel) client will
  // occasionally pull a message out of the sent queue to send elsewhere.  in that case
  // it doesn't matter if we "got" it or not.
  //if (message->get_seq() <= in_seq) {
  //  ldout(cct,0) << __func__ << " got old message "
  //          << message->get_seq() << " <= " << in_seq << " " << message << " " << *message
  //          << ", discarding" << dendl;
  //  message->put();
  //  if (has_feature(CEPH_FEATURE_RECONNECT_SEQ) && cct->_conf->ms_die_on_old_message)
  //    assert(0 == "old msgs despite reconnect_seq feature");
  //}

  InfRcConnectionRef conn = NULL;
  {
    Mutex::Locker l(lock);
    ceph::unordered_map<uint64_t, pair<QueuePair*, InfRcConnectionRef> >::iterator it = qp_conns.find(qpnum);
    if (it == qp_conns.end()) {
      // discard message
      ldout(cct, 0) << __func__ << " missing qp_num " << qpnum << ", discard message "
                    << *message << dendl;
      message->put();
      return ;
    }
    conn = it->second.second;
    message->set_connection(conn);
  }

  //TODO last received message.
  //in_seq = message->get_seq();
  ldout(cct, 10) << __func__ << " got message=" << message << " seq=" << message->get_seq()
                             << " qpn=" << qpnum << " " << *message << dendl;

  conn->infrc_msgr->ms_fast_preprocess(message);
  if (conn->infrc_msgr->ms_can_fast_dispatch(message)) {
    conn->infrc_msgr->ms_fast_dispatch(message);
  } else {
    center.dispatch_event_external(EventCallbackRef(new C_infrc_handle_dispatch(conn->infrc_msgr, message)));
  }
}

void InfRcWorker::send_pending_messages(bool locked)
{
  ldout(cct, 20) << __func__ << " locked=" << locked << dendl;
  if (!locked)
    lock.Lock();
  while (!client_send_queue.empty()) {
    BufferDescriptor *bd = pool->reserve_message_buffer(this);
    if (bd) {
      pair<Message*, QueuePair*> p = client_send_queue.front();
      client_send_queue.pop_front();
      if (p.second->is_dead()) {
        ldout(cct, 1) << __func__ << " qp=" << p.second->get_local_qp_number()
                      << " disconnected. Discard message=" << p.first << dendl;
        p.first->put();
        pool->post_tx_buffer(this, bd);
      } else if (send_zero_copy(p.first, p.second, bd) < 0) {
        ldout(cct, 1) << __func__ << " failed to send message " << p.first << dendl;
        pool->post_tx_buffer(this, bd);
        p.first->put();
        assert(0);
      }
    } else {
      break;
    }
  }

  if (!locked)
    lock.Unlock();
}

/**
 * Called from main event loop when a RX CQ notification is available.
 */
void InfRcWorker::handle_rx_event()
{
  ldout(cct, 20) << __func__ << dendl;
  if (!rx_cc->get_cq_event())
    return ;

  static const int MAX_COMPLETIONS = 16;
  ibv_wc wc[MAX_COMPLETIONS];
  bool rearmed = false;
 again:
  int n = rx_cq->poll_completion_queue(MAX_COMPLETIONS, wc);
  ldout(cct, 20) << __func__ << " pool completion queue got " << n
                 << " responses."<< dendl;
  for (int i = 0; i < n; i++) {
    ibv_wc* response = &wc[i];
    BufferDescriptor *bd = reinterpret_cast<BufferDescriptor *>(response->wr_id);
    ldout(cct, 20) << __func__ << " got bd=" << response->wr_id << dendl;

    if (response->status != IBV_WC_SUCCESS) {
      lderr(cct) << __func__ << " work request returned error for buffer(" << response->wr_id
                 << ") status(" << response->status << ":"
                 << infiniband->wc_status_to_string(response->status) << dendl;
      pool->post_srq_receive(bd);
      continue;
    }

    if (response->byte_len < MIN_PREFETCH_LEN)
      ;//prefetch(bd->buffer, response->byte_len);

    bufferptr bp;
    ceph_msg_header header(*reinterpret_cast<ceph_msg_header*>(bd->buffer));
    if (pool->incr_used_srq_buffers() >= MAX_SHARED_RX_QUEUE_DEPTH / 2) {
      // srq is low on buffers, better return this one
      bp = bufferptr(bd->buffer, response->byte_len);
      ldout(cct, 20) << __func__ << " low srq buffer level, use copy instead" << dendl;
      pool->post_srq_receive(bd);
    } else {
      // message will hold one of srq's buffers until message is destroyed
      bp = bufferptr(
          buffer::create_free_hook(response->byte_len, bd->buffer,
                                   infrc_buffer_post_buffer, pool));
    }
    process_request(bp, response->qp_num);
    ldout(cct, 20) << __func__ << " Received message from " << response->src_qp
                    << " with " << response->byte_len << " bytes" << dendl;
  }
  if (n)
    goto again;

  if (!rearmed) {
    rx_cq->rearm_notify();
    rearmed = true;
    // Clean up cq events after rearm notify ensure no new incoming event
    // arrived between polling and rearm
    goto again;
  }

  /* accumulate number of cq events that need to * be acked, and
   * periodically ack them
   */
  if (++rxcq_events_need_ack == MIN_ACK_LEN) {
    ibv_ack_cq_events(rx_cq->get_cq(), MIN_ACK_LEN);
    rxcq_events_need_ack = 0;
  }
}

/**
 * Called from main event loop when a TX CQ notification is available.
 */
void InfRcWorker::handle_tx_event()
{
  ldout(cct, 20) << __func__ << dendl;
  if (!tx_cc->get_cq_event())
    return ;

  ibv_wc ret_array[MAX_TX_QUEUE_DEPTH];
  bool rearmed = false;
 again:
  int n = tx_cq->poll_completion_queue(MAX_TX_QUEUE_DEPTH, ret_array);
  ldout(cct, 20) << __func__ << " pool completion queue got " << n
                 << " responses."<< dendl;

  Mutex::Locker l(lock);
  for (int i = 0; i < n; i++) {
    uint64_t id = ret_array[i].wr_id;
    BufferDescriptor* bd = reinterpret_cast<BufferDescriptor*>(id);

    ceph::unordered_map<uint64_t, Message*>::iterator it = outstanding_messages.find(id);
    if (it == outstanding_messages.end()) {
      lderr(cct) << __func__ << " unknown message, bd=" << id << " should be a bug?" << dendl;
      assert(0);
    }
    ldout(cct, 20) << __func__ << " ack message =" << it->second << " seq="
                   << it->second->get_seq() << " qpn=" << ret_array[i].qp_num
                   << " bd=" << id << dendl;

    if (ret_array[i].status != IBV_WC_SUCCESS) {
      if (ret_array[i].status == IBV_WC_RETRY_EXC_ERR) {
        lderr(cct) << __func__ << " connection between server and client not working. Disconnect this now" << dendl;
      } else if (ret_array[i].status == IBV_WC_WR_FLUSH_ERR){
        ceph::unordered_map<uint64_t, pair<QueuePair*, InfRcConnectionRef> >::iterator qp_it = qp_conns.find(ret_array[i].qp_num);
        if (qp_it != qp_conns.end()) {
          lderr(cct) << __func__ << " this connection(" << qp_it->second.second
                     << ") still inline in this side, mark down it" << dendl;
        } else {
          // Must be STOPPED
          assert(!it->second->get_connection()->is_connected());
        }

        lderr(cct) << __func__ << " Work Request Flushed Error: this connection's qp="
                   << ret_array[i].qp_num << " should be down while this WR=" << id
                   << " still in flight." << dendl;

      } else {
        lderr(cct) << __func__ << " send work request returned error for buffer(" << id
                   << ") status(" << ret_array[i].status << "): "
                   << infiniband->wc_status_to_string(ret_array[i].status) << dendl;
      }
      ldout(cct, 1) << __func__ << " discard message=" << it->second << " mark down associated connection="
                    << it->second->get_connection() << dendl;
      it->second->get_connection()->mark_down();
    }

    it->second->put();
    outstanding_messages.erase(it);

    pool->post_tx_buffer(this, bd);
  }
  if (n)
    goto again;

  if (!rearmed) {
    tx_cq->rearm_notify();
    rearmed = true;
    // Clean up cq events after rearm notify ensure no new incoming event
    // arrived between polling and rearm
    goto again;
  }

  send_pending_messages(true);

  // NOTE: Has TX just transitioned to idle? We should do it when idle!
  // It's now safe to delete queue pairs (see comment by declaration
  // for dead_queue_pairs).
  // Additionally, don't delete qp while client_send_queue isn't empty,
  // because we need to check qp's state before sending
  if (client_send_queue.empty() && outstanding_messages.empty()) {
    while (!dead_queue_pairs.empty()) {
      ldout(cct, 10) << __func__ << " finally delete qp=" << dead_queue_pairs.back() << dendl;
      assert(dead_queue_pairs.back()->is_dead());
      delete dead_queue_pairs.back();
      dead_queue_pairs.pop_back();
    }
   }

  /* accumulate number of cq events that need to * be acked, and
   * periodically ack them
   */
  if (++txcq_events_need_ack == MIN_ACK_LEN) {
    ibv_ack_cq_events(tx_cq->get_cq(), MIN_ACK_LEN);
    txcq_events_need_ack = 0;
  }
}

void InfRcWorker::handle_async_event()
{
  ldout(cct, 20) << __func__ << dendl;
  while (1) {
    ibv_async_event async_event;
    ldout(cct, 20) << __func__ << dendl;
    if (ibv_get_async_event(infiniband->device.ctxt, &async_event)) {
      if (errno == EAGAIN)
        return ;

      lderr(cct) << __func__ << " ibv_get_async_event failed. (errno=" << errno
                 << " " << cpp_strerror(errno) << ")" << dendl;
      return;
    }
    if (async_event.event_type == IBV_EVENT_QP_LAST_WQE_REACHED) {
      ldout(cct, 10) << __func__ << " event associated qp=" << async_event.element.qp
                     << " evt: " << ibv_event_type_str(async_event.event_type) << dendl;
      lock.Lock();
      ceph::unordered_map<uint64_t, pair<QueuePair*, InfRcConnectionRef> >::iterator it;
      it = qp_conns.find(async_event.element.qp->qp_num);
      if (it == qp_conns.end()) {
        ldout(cct, 1) << __func__ << " missing qp_num=" << async_event.element.qp->qp_num
                                  << " discard event" << dendl;
        ibv_ack_async_event(&async_event);
        lock.Unlock();
        return ;
      }
      it->second.second->mark_down();
      dead_queue_pairs.push_back(it->second.first);
      ldout(cct, 10) << __func__ << " associated conn=" << it->second.second
                     << " stop this" << dendl;
      qp_conns.erase(it);
      lock.Unlock();
    } else {
      ldout(cct, 0) << __func__ << " ibv_get_async_event: dev=" << infiniband->device.ctxt
                    << " evt: " << ibv_event_type_str(async_event.event_type)
                    << dendl;
    }
    ibv_ack_async_event(&async_event);
  }
}

int InfRcWorker::submit_message(Message *m, QueuePair *qp)
{
  Mutex::Locker l(lock);
  if (!client_send_queue.empty()) {
    client_send_queue.push_back(make_pair(m, qp));
    ldout(cct, 20) << __func__ << " pending message=" << *m << " to qp=" << qp << dendl;
    return 0;
  }

  ldout(cct, 20) << __func__ << " message=" << *m << " to qp=" << qp << dendl;
  BufferDescriptor *bd = pool->reserve_message_buffer(this);
  if (bd) {
    if (send_zero_copy(m, qp, bd) < 0) {
      pool->post_tx_buffer(this, bd);
      m->put();
      assert(0);
      return -1;
    }
  } else {
    client_send_queue.push_back(make_pair(m, qp));
  }
  return 0;
}

/**
 * Post a message for transmit by the HCA, attempting to zero-copy any
 * data from registered buffers (currently only seglets that are part of
 * the log). Transparently handles buffers with more chunks than the
 * scatter-gather entry limit and buffers that mix registered and
 * non-registered chunks.
 *
 * \param m
 *      Message that should be transmitted to the endpoint listening on #connection.
 * \param qp
 *      Queue pair on which to transmit the message.
 * \param bd
 *      BufferDescriptor used to store transmit buffer
 */
int InfRcWorker::send_zero_copy(Message *m, QueuePair *qp, BufferDescriptor *bd)
{
  ldout(cct, 20) << __func__ << " m=" << m << " qp=" << qp << " bd=" << bd << dendl;
  assert(lock.is_locked());
  bufferlist bl;

  m->set_seq(nonce.inc());
  // prepare everything
  ceph_msg_header& header = m->get_header();
  ceph_msg_footer& footer = m->get_footer();

  bl.append((char*)&header, sizeof(header));
  bl.append(m->get_payload());
  bl.append(m->get_middle());
  bl.append(m->get_data());
  bl.append((char*)&footer, sizeof(footer));

  if (bl.length() > MAX_MESSAGE_LEN) {
    lderr(cct) << __func__ << " message exceeds maximum message size "
               << "(attempted " << bl.length() << " bytes, maximum "
               << MAX_MESSAGE_LEN << " bytes)" << dendl;
    assert(0);
  }

  const bool allow_zero_copy = true;
  ibv_sge isge[MAX_TX_SGE_COUNT];

  uint32_t last_chunk_index = bl.buffers().size() - 1;

  uint32_t current_chunk = 0;
  uint32_t current_sge = 0;

  // The variables below allow us to collect several chunks from the
  // Buffer into a single sge in some situations. They describe a
  // range of bytes in bd that have not yet been put in an sge, but
  // must go into the next sge.
  char* unadded_start = bd->buffer;
  char* unadded_end = bd->buffer;

  list<bufferptr>::const_iterator it = bl.buffers().begin();
  while (it != bl.buffers().end()) {
    const uintptr_t addr = reinterpret_cast<const uintptr_t>(it->c_str());
    // See if we can transmit this chunk from its current location
    // (zero copy) vs. copying it into a transmit buffer:
    // * The chunk must lie in the range of registered memory that
    //   the NIC knows about.
    // * If we run out of sges, then everything has to be copied
    //   (but save the last sge for the last chunk, since it's the
    //   one most likely to benefit from zero copying.
    // * For small chunks, it's cheaper to copy than to send a
    //   separate descriptor to the NIC.
    if (allow_zero_copy &&
        // The "4" below means this: can't do zero-copy for this chunk
        // unless there are at least 4 sges left (1 for unadded data, one
        // for this zero-copy chunk, 1 for more unadded data up to the
        // last chunk, and one for a final zero-copy chunk), or this is
        // the last chunk (in which there better be at least 2 sge's left).
        (current_sge <= MAX_TX_SGE_COUNT - 4 ||
         current_chunk == last_chunk_index) &&
        addr >= pool->log_memory_base &&
        (addr + it->length()) <= (pool->log_memory_base + pool->log_memory_bytes) &&
        it->length() > MIN_ZERO_COPY_SGE) {
      if (unadded_start != unadded_end) {
        isge[current_sge].addr = reinterpret_cast<uint64_t>(unadded_start);
        isge[current_sge].length = static_cast<uint32_t>(unadded_end - unadded_start);
        isge[current_sge].lkey = bd->mr->lkey;
        ++current_sge;
        unadded_start = unadded_end;
      }

      isge[current_sge].addr = addr;
      isge[current_sge].length = it->length();
      isge[current_sge].lkey = pool->log_memory_region->lkey;
      ++current_sge;
    } else {
      memcpy(unadded_end, it->c_str(), it->length());
      unadded_end += it->length();
    }
    it++;
    ++current_chunk;
  }
  if (unadded_start != unadded_end) {
    isge[current_sge].addr = reinterpret_cast<uint64_t>(unadded_start);
    isge[current_sge].length = static_cast<uint32_t>(unadded_end - unadded_start);
    isge[current_sge].lkey = bd->mr->lkey;
    ++current_sge;
    unadded_start = unadded_end;
  }

  ibv_send_wr tx_work_request;

  memset(&tx_work_request, 0, sizeof(tx_work_request));
  tx_work_request.wr_id = reinterpret_cast<uint64_t>(bd);// stash descriptor ptr
  tx_work_request.next = NULL;
  tx_work_request.sg_list = isge;
  tx_work_request.num_sge = current_sge;
  tx_work_request.opcode = IBV_WR_SEND;
  tx_work_request.send_flags = IBV_SEND_SIGNALED;

  // We can get a substantial latency improvement (nearly 2usec less per RTT)
  // by inlining data with the WQE for small messages. The Verbs library
  // automatically takes care of copying from the SGEs to the WQE.
  if ((bl.length()) <= Infiniband::MAX_INLINE_DATA)
    tx_work_request.send_flags |= IBV_SEND_INLINE;

  ibv_send_wr* bad_tx_work_request;
  if (ibv_post_send(qp->get_qp(), &tx_work_request, &bad_tx_work_request)) {
    lderr(cct) << __func__ << " ibv_post_send failed: " << cpp_strerror(errno) << dendl;
    return -1;
  }

  assert(!outstanding_messages.count(reinterpret_cast<uint64_t>(bd)));
  outstanding_messages[reinterpret_cast<uint64_t>(bd)] = m;
  ldout(cct, 20) << __func__ << " successfully post seq=" << m->get_seq() << " qpn="
                 << qp->get_local_qp_number() << " bd=" << reinterpret_cast<uint64_t>(bd)
                 << " message(" << *m << ")" << dendl;
  return 0;
}

//------------------------------
// InfRcWorkerPool class
//------------------------------
InfRcWorkerPool::InfRcWorkerPool(CephContext *c)
  : cct(c), seq(0), started(false),
    barrier_lock("InfRcWorkerPool::barrier_lock"), barrier_count(0),
    ib_device_name(cct->_conf->ms_infiniband_device_name),
    ib_physical_port(cct->_conf->ms_infiniband_port), lid(-1),
    rx_buffers(NULL), tx_buffers(NULL), srq(NULL), lock("InfRcWorkerPool::Lock"),
    num_used_srq_buffers(0), log_memory_base(NULL), log_memory_bytes(0), log_memory_region(NULL),
    infiniband(new Infiniband(cct, ib_device_name.length() ? ib_device_name.c_str(): NULL))
{
  assert(infiniband);
  for (int i = 0; i < cct->_conf->ms_async_op_threads; ++i) {
    InfRcWorker *w = new InfRcWorker(cct, this, i);
    workers.push_back(w);
  }
}

InfRcWorkerPool::~InfRcWorkerPool()
{
  shutdown();
}

int InfRcWorkerPool::start()
{
  ldout(cct, 20) << __func__ << dendl;
  if (!started) {
    InfRcWorker *w = get_worker();
    w->center.create_file_event(
        infiniband->get_async_fd(), EVENT_READABLE,
        EventCallbackRef(new C_infrc_async_ev_handler(w)));

    //  Set up the initial verbs necessities: open the device, allocate
    //  protection domain, create shared receive queue, register buffers.
    lid = infiniband->get_lid(ib_physical_port);
    assert(lid != -1);

    // create two shared receive queues. all client queue pairs use one and all
    // server queue pairs use the other. we post receive buffer work requests
    // to these queues only. the motiviation is to avoid having to post at
    // least one buffer to every single queue pair (we may have thousands of
    // them with megabyte buffers).
    srq = infiniband->create_shared_receive_queue(MAX_SHARED_RX_QUEUE_DEPTH,
                                                  MAX_SHARED_RX_SGE_COUNT);
    if (!srq) {
      lderr(cct) << __func__ << " failed to create shared receive queue" << dendl;
      goto err;
    }

    // Note: RPC performance is highly sensitive to the buffer size. For
    // example, as of 11/2012, using buffers of (1<<23 + 200) bytes is
    // 1.3 microseconds slower than using buffers of (1<<23 + 4096)
    // bytes.  For now, make buffers large enough for the largest RPC,
    // and round up to the next multiple of 4096.  This approach isn't
    // perfect (for example buffers of 1<<23 bytes also seem to be slow)
    // but it will work for now.
    uint32_t buffer_size = (MAX_MESSAGE_LEN + 4095) & ~0xfff;

    rx_buffers = new RegisteredBuffers(infiniband->pd, buffer_size,
                                       uint32_t(MAX_SHARED_RX_QUEUE_DEPTH));
    if (!rx_buffers) {
      lderr(cct) << __func__ << " failed to malloc rx_buffers: " << cpp_strerror(errno) << dendl;
      goto err;
    }
    num_used_srq_buffers = MAX_SHARED_RX_QUEUE_DEPTH;
    for (BufferDescriptor *bd = rx_buffers->begin();
         bd != rx_buffers->end(); ++bd) {
      if (post_srq_receive(bd)) {
        lderr(cct) << __func__ << " failed to post srq buffer: " << cpp_strerror(errno) << dendl;
        goto err;
      }
    }
    assert(num_used_srq_buffers == 0);

    tx_buffers = new RegisteredBuffers(infiniband->pd, buffer_size,
                                       uint32_t(MAX_TX_QUEUE_DEPTH));
    if (!tx_buffers) {
      lderr(cct) << __func__ << " failed to malloc tx_buffers: " << cpp_strerror(errno) << dendl;
      goto err;
    }
    for (BufferDescriptor *bd = tx_buffers->begin(); bd != tx_buffers->end(); ++bd)
      free_tx_buffers.push_back(bd);

    for (vector<InfRcWorker*>::iterator it = workers.begin(); it != workers.end(); ++it) {
      if ((*it)->start() < 0) {
        lderr(cct) << __func__ << " worker " << *it << " failed to start." << dendl;
        goto err;
      }
    }
    started = true;
  }

  return 0;

 err:
  shutdown();
  return -1;
}

void InfRcWorkerPool::shutdown()
{
  ldout(cct, 20) << __func__ << dendl;
  for (uint64_t i = 0; i < workers.size(); ++i) {
    if (workers[i]->is_started()) {
      workers[i]->stop();
      workers[i]->join();
    }
    delete workers[i];
  }
  if (infiniband) {
    delete infiniband;
    infiniband = NULL;
  }
  if (srq) {
    infiniband->destroy_shared_receive_queue(srq);
    srq = NULL;
  }
  if (rx_buffers) {
    delete rx_buffers;
    rx_buffers = NULL;
  }
  if (tx_buffers) {
    delete tx_buffers;
    tx_buffers = NULL;
  }
  free_tx_buffers.clear();
  workers.clear();
  started = false;
}

/**
 * Add the given BufferDescriptor to the given shared receive queue.
 *
 * \param[in] bd
 *      The BufferDescriptor to enqueue.
 * \return
 *      0 if success or -1 for failure
 */
int InfRcWorkerPool::post_srq_receive(BufferDescriptor *bd)
{
  ldout(cct, 20) << __func__ << " bd=" << bd << dendl;
  if (infiniband->post_srq_receive(srq, bd)) {
    return -1;
  }

  Mutex::Locker l(lock);
  --num_used_srq_buffers;
  return 0;
}

int InfRcWorkerPool::post_tx_buffer(InfRcWorker *worker, BufferDescriptor *bd)
{
  ldout(cct, 20) << __func__ << " bd=" << bd << dendl;
  Mutex::Locker l(lock);
  free_tx_buffers.push_back(bd);
  if (!pending_sent_workers.empty()) {
    InfRcWorker *w = pending_sent_workers.front();
    if (worker != w) {
      w->center.dispatch_event_external(
          EventCallbackRef(new C_infrc_send_message(w)));
      w->center.wakeup();
      ldout(cct, 10) << __func__ << " wakeup pending sent worker=" << w << dendl;
    }
    pending_sent_workers.pop_front();
  }
  return 0;
}

Infiniband::BufferDescriptor* InfRcWorkerPool::reserve_message_buffer(InfRcWorker *w)
{
  ldout(cct, 20) << __func__ << dendl;
  BufferDescriptor *bd = NULL;
  Mutex::Locker l(lock);
  if (!free_tx_buffers.empty()) {
    bd = free_tx_buffers.back();
    free_tx_buffers.pop_back();
  } else {
    pending_sent_workers.push_back(w);
  }
  ldout(cct, 20) << __func__ << " return bd: " << bd << dendl;
  return bd;
}

void InfRcWorkerPool::barrier()
{
  ldout(cct, 10) << __func__ << " started." << dendl;
  pthread_t cur = pthread_self();
  for (vector<InfRcWorker*>::iterator it = workers.begin(); it != workers.end(); ++it) {
    assert(cur != (*it)->center.get_owner());
    (*it)->center.dispatch_event_external(EventCallbackRef(new C_barrier(this)));
    barrier_count.inc();
  }
  ldout(cct, 10) << __func__ << " wait for " << barrier_count.read() << " barrier" << dendl;
  Mutex::Locker l(barrier_lock);
  while (barrier_count.read())
    barrier_cond.Wait(barrier_lock);

  ldout(cct, 10) << __func__ << " end." << dendl;
}

//------------------------------
// InfRcMessenger class
//------------------------------
/**
 * Construct a InfRcMessenger.
 *
 * \param context
 *      Overall information about the RAMCloud server or client.
 * \param sl
 *      The ServiceLocator describing which HCA to use and the IP/UDP
 *      address and port numbers to use for handshaking. If NULL,
 *      the transport will be configured for client use only.
 */
InfRcMessenger::InfRcMessenger(CephContext *cct, entity_name_t name,
                               string mname, uint64_t _nonce)
  : SimplePolicyMessenger(cct, name, mname, _nonce), started(false), stopped(true),
    server_setup_socket(-1), nonce(_nonce), need_addr(true), cluster_protocol(0),
    global_seq(0), lock("InfRcMessenger::lock"), deleted_lock("InfRcMessenger::deleted_lock"),
    server_setup_worker(NULL)
{
  cct->lookup_or_create_singleton_object<InfRcWorkerPool>(
      pool, InfRcWorkerPool::get_name(cct->_conf->ms_infiniband_device_name,
                                      cct->_conf->ms_infiniband_port));
  local_connection = new InfRcConnection(cct, this, pool->get_worker(), NULL,
                                         my_inst.addr, my_inst.name.type());
}

/**
 * Destructor for InfRcMessenger.
 */
InfRcMessenger::~InfRcMessenger()
{
  // Note: this destructor isn't yet complete; it contains just enough cleanup
  // for the unit tests to run.
  if (server_setup_socket != -1)
    ::close(server_setup_socket);
}

int InfRcMessenger::bind(const entity_addr_t& bind_addr)
{
  Mutex::Locker l(lock);
  if (server_setup_socket > 0) {
    ldout(cct,10) << __func__ << " already started" << dendl;
    return -1;
  }
  ldout(cct, 10) << __func__ << " bind " << bind_addr << dendl;

  // bind to a socket
  set<int> avoid_ports;
  return _bind(bind_addr, avoid_ports);
}

int InfRcMessenger::rebind(const set<int>& avoid_ports)
{
  ldout(cct,1) << __func__ << " rebind avoid " << avoid_ports << dendl;
  assert(server_setup_socket > 0);

  mark_down_all();

  entity_addr_t addr = get_myaddr();
  set<int> new_avoid = avoid_ports;
  new_avoid.insert(addr.get_port());
  addr.set_port(0);

  // adjust the nonce; we want our entity_addr_t to be truly unique.
  nonce.add(1000000);
  my_inst.addr.nonce = nonce.read();
  ldout(cct, 10) << __func__ << " new nonce " << nonce.read() << " and inst "
                 << my_inst << dendl;

  if (server_setup_socket != -1) {
    server_setup_worker->center.delete_file_event(server_setup_socket, EVENT_READABLE);
    ::close(server_setup_socket);
    server_setup_socket = -1;
    server_setup_worker = NULL;
  }

  ldout(cct, 10) << __func__ << " will try " << addr << " and avoid ports "
                 << new_avoid << dendl;
  Mutex::Locker l(lock);
  int r = _bind(addr, new_avoid);
  if (r == 0) {
    ready();
  }

  return r;
}

int InfRcMessenger::start()
{
  lock.Lock();
  ldout(cct,1) << __func__ << " start" << dendl;

  // register at least one entity, first!
  assert(my_inst.name.type() >= 0);

  assert(!started);

  started = true;
  stopped = false;
  if (server_setup_socket == -1) {
    my_inst.addr.nonce = nonce.read();
    _init_local_connection();
  }

  lock.Unlock();
  return pool->start();
}

int InfRcMessenger::shutdown()
{
  // Clean up all resources "start()" declared
  // Set all resource to init state in case of calling start again
  ldout(cct, 10) << __func__ << " " << get_myaddr() << dendl;
  lock.Lock();
  if (server_setup_socket != -1) {
    server_setup_worker->center.delete_file_event(server_setup_socket, EVENT_READABLE);
    ::close(server_setup_socket);
    server_setup_socket = -1;
    server_setup_worker = NULL;
  }
  lock.Unlock();

  mark_down_all();
  local_connection->set_priv(NULL);

  pool->barrier();
  lock.Lock();
  stop_cond.Signal();
  lock.Unlock();
  stopped = true;
  return 0;
}

void InfRcMessenger::wait()
{
  lock.Lock();
  if (!started) {
    lock.Unlock();
    return;
  }
  if (!stopped)
    stop_cond.Wait(lock);

  lock.Unlock();

  // close all connections
  mark_down_all();

  ldout(cct, 10) << __func__ << ": done." << dendl;
  ldout(cct, 1) << __func__ << " complete." << dendl;
  started = false;
}

void InfRcMessenger::mark_down(const entity_addr_t& addr)
{
  ldout(cct, 1) << __func__ << dendl;
  Mutex::Locker l(lock);
  InfRcConnectionRef p = _lookup_conn(addr);
  if (p) {
    ldout(cct, 1) << __func__ << " " << addr << " -- " << p << dendl;
    p->mark_down();
    ms_deliver_handle_reset(p.get());
  } else {
    ldout(cct, 1) << __func__ << " " << addr << " -- connection dne" << dendl;
  }
}

void InfRcMessenger::mark_down_all()
{
  ldout(cct, 1) << __func__ << dendl;

  lock.Lock();
  while (!pending_conns.empty()) {
    set<InfRcConnectionRef>::iterator it = pending_conns.begin();
    InfRcConnectionRef p = *it;
    ldout(cct, 5) << __func__ << " delete " << p << dendl;
    p->mark_down();
    pending_conns.erase(it);
  }

  while (!connections.empty()) {
    ceph::unordered_map<entity_addr_t, InfRcConnectionRef>::iterator it = connections.begin();
    InfRcConnectionRef p = it->second;
    ldout(cct, 5) << __func__ << " mark down " << it->first << " " << p << dendl;
    connections.erase(it);
    p->mark_down();
    ms_deliver_handle_reset(p.get());
  }

  while (!deleted_conns.empty()) {
    set<InfRcConnectionRef>::iterator it = deleted_conns.begin();
    InfRcConnectionRef p = *it;
    ldout(cct, 5) << __func__ << " delete " << p << dendl;
    deleted_conns.erase(it);
  }
  lock.Unlock();
}

void InfRcMessenger::ready()
{
  ldout(cct, 1) << __func__ << dendl;
  if (server_setup_socket > 0) {
    server_setup_worker = pool->get_worker();
    server_setup_worker->center.create_file_event(
        server_setup_socket, EVENT_READABLE,
        EventCallbackRef(new C_infrc_handle_accept(this)));
  }
}

/**
 * If my_inst.addr doesn't have an IP set, this function
 * will fill it in from the passed addr. Otherwise it does nothing and returns.
 */
void InfRcMessenger::set_addr_unknowns(entity_addr_t &addr)
{
  ldout(cct, 1) << __func__ << dendl;
  Mutex::Locker l(lock);
  if (my_inst.addr.is_blank_ip()) {
    int port = my_inst.addr.get_port();
    my_inst.addr.addr = addr.addr;
    my_inst.addr.set_port(port);
    _init_local_connection();
  }
}

ConnectionRef InfRcMessenger::get_connection(const entity_inst_t& dest)
{
  if (my_inst.addr == dest.addr) {
    // local
    return local_connection;
  }

  assert(!dest.addr.is_blank_ip());
  Mutex::Locker l(lock);
  InfRcConnectionRef conn = _lookup_conn(dest.addr);
  if (conn) {
    ldout(cct, 10) << __func__ << " " << dest << " existing " << conn << dendl;
  } else {
    ldout(cct, 10) << __func__ << " " << dest << " new " << conn << dendl;
    InfRcWorker *w = pool->get_worker();
    conn = w->create_connection(dest.addr, dest.name.type(), this);
    pending_conns.insert(conn);
    conn->connect();
  }

  return conn;
}

int InfRcMessenger::_bind(const entity_addr_t& bind_addr, const set<int>& avoid_ports)
{
  ldout(cct, 1) << __func__ << dendl;

  int family;
  switch (bind_addr.get_family()) {
    case AF_INET:
    case AF_INET6:
      family = bind_addr.get_family();
      break;

    default:
      // bind_addr is empty
      family = cct->_conf->ms_bind_ipv6 ? AF_INET6 : AF_INET;
  }

  server_setup_socket = ::socket(family, SOCK_DGRAM, 0);
  if (server_setup_socket == -1) {
    lderr(cct) << __func__ << " failed to create server socket: "
      << cpp_strerror(errno) << dendl;
    return -1;
  }

  // use whatever user specified (if anything)
  entity_addr_t listen_addr = bind_addr, addr;
  listen_addr.set_family(family);

  /* bind to port */
  int rc = -1;
  if (listen_addr.get_port()) {
    // specific port

    // reuse addr+port when possible
    int on = 1;
    rc = ::setsockopt(server_setup_socket, SOL_SOCKET, SO_REUSEADDR,
        &on, sizeof(on));
    if (rc < 0) {
      lderr(cct) << __func__ << " unable to setsockopt: "
        << cpp_strerror(errno) << dendl;
      goto err;
    }

    rc = ::bind(server_setup_socket, (struct sockaddr *)&listen_addr.ss_addr(),
                listen_addr.addr_size());
    if (rc < 0) {
      lderr(cct) << __func__ << " unable to bind to " << listen_addr.ss_addr()
        << ": " << cpp_strerror(errno) << dendl;
      goto err;
    }
  } else {
    // try a range of ports
    for (int port = cct->_conf->ms_bind_port_min;
        port <= cct->_conf->ms_bind_port_max; port++) {
      if (avoid_ports.count(port))
        continue;
      listen_addr.set_port(port);
      rc = ::bind(server_setup_socket, (struct sockaddr *)&listen_addr.ss_addr(),
          listen_addr.addr_size());
      if (rc == 0)
        break;
    }
    if (rc < 0) {
      lderr(cct) << __func__ << " unable to bind to " << listen_addr.ss_addr()
        << " on any port in range " << cct->_conf->ms_bind_port_min
        << "-" << cct->_conf->ms_bind_port_max
        << ": " << cpp_strerror(errno) << dendl;
      goto err;
    }
    ldout(cct, 10) << __func__ << " bound on random port "
      << listen_addr << dendl;
  }

  int flags;

  /* Set the socket nonblocking.
   * Note that fcntl(2) for F_GETFL and F_SETFL can't be
   * interrupted by a signal. */
  if ((flags = fcntl(server_setup_socket, F_GETFL)) < 0 ) {
    lderr(cct) << __func__ << " fcntl(F_GETFL) failed: %s" << cpp_strerror(errno) << dendl;
    return -errno;
  }
  if (fcntl(server_setup_socket, F_SETFL, flags | O_NONBLOCK) < 0) {
    lderr(cct) << __func__ << " fcntl(F_SETFL,O_NONBLOCK): %s" << cpp_strerror(errno) << dendl;
    return -errno;
  }

  set_myaddr(bind_addr);

  if (get_myaddr().get_port() == 0) {
    set_myaddr(listen_addr);
  }
  addr = get_myaddr();
  addr.nonce = nonce.read();
  set_myaddr(addr);
  _init_local_connection();

  return 0;

 err:
  ::close(server_setup_socket);
  server_setup_socket = -1;
  return -1;
}

/**
 * This method is invoked by the dispatcher when #server_setup_socket becomes
 * readable. It attempts to set up QueuePair with a connecting remote
 * client.
 *
 * \param events
 *      Indicates whether the socket was readable, writable, or both
 */
void InfRcMessenger::recv_connect()
{
  ldout(cct, 20) << __func__ << dendl;
  while (1) {
    entity_addr_t socket_addr;
    socklen_t slen = sizeof(socket_addr.ss_addr());
    Infiniband::QueuePairTuple incoming_qpt, outgoing_qpt;
    ssize_t len = ::recvfrom(server_setup_socket, &incoming_qpt,
                             sizeof(incoming_qpt), 0,
                             reinterpret_cast<sockaddr *>(&socket_addr.ss_addr()), &slen);
    if (len <= -1) {
      if (errno != EAGAIN)
        lderr(cct) << __func__ << " recvfrom failed: " << cpp_strerror(errno) << dendl;
      break;
    } else if (len != sizeof(incoming_qpt)) {
      lderr(cct) << __func__ << " recvfrom got a strange incoming size: "
                 << cpp_strerror(errno) << dendl;
      continue;
    }
    ldout(cct, 20) << __func__ << " receiving new connection qpt="
                   << incoming_qpt << dendl;
    if (incoming_qpt.get_sender_addr().is_blank_ip()) {
      // peer apparently doesn't know what ip they have; figure it out for them.
      entity_addr_t peer_addr = incoming_qpt.get_sender_addr();
      int port = peer_addr.get_port();
      peer_addr.addr = socket_addr.addr;
      peer_addr.set_port(port);
      incoming_qpt.set_sender_addr(peer_addr);
      ldout(cct, 10) << __func__ << " accept peer addr is really " << peer_addr << dendl;
    }
    incoming_qpt.set_features(ceph_sanitize_features(incoming_qpt.get_features()));
    uint64_t required = get_policy(incoming_qpt.get_type()).features_required;
    uint64_t feat_missing = required & ~(uint64_t)incoming_qpt.get_features();
    if (feat_missing) {
      outgoing_qpt.set_tag(CEPH_MSGR_TAG_FEATURES);
      outgoing_qpt.set_features(required);
      ldout(cct, 1) << __func__ << " peer missing required features "
                                << std::hex << feat_missing << std::dec << dendl;
      len = ::sendto(server_setup_socket, &outgoing_qpt, sizeof(outgoing_qpt), 0,
                     reinterpret_cast<sockaddr *>(&socket_addr.ss_addr()), slen);
      if (len != sizeof(outgoing_qpt)) {
        lderr(cct) << __func__ << " sendto failed, len = " << len << ": "
                   << cpp_strerror(errno) << dendl;
      }
      continue;
    }

    Mutex::Locker l(lock);
    InfRcConnectionRef conn = _lookup_conn(incoming_qpt.get_sender_addr());
    // TODO: now just accept new and reject old
    if (conn) {
      ldout(cct, 5) << __func__ << " accept new conection from "
                    << incoming_qpt.get_sender_addr() << dendl;
      conn->mark_down();
    }
    assert(!_lookup_conn(incoming_qpt.get_sender_addr()));
    // create a new queue pair, set it up according to our client's parameters,
    // and feed back our lid, qpn, and psn information so they can complete
    // the out-of-band handshake.

    // Note: It is possible that we already created a queue pair, but the
    // response to the client was lost and so we allocated another.
    // We should probably look up the QueuePair first using incoming_qpt,
    // just to be sure, esp. if we use an unreliable means of handshaking, in
    // which case the response to the client request could have been lost.

    InfRcWorker *w = pool->get_worker();
    conn = w->create_connection(incoming_qpt.get_sender_addr(), incoming_qpt.get_type(), this);
    connections[incoming_qpt.get_sender_addr()] = conn;
    if (conn->ready(incoming_qpt) < 0) {
      lderr(cct) << __func__ << " ready failed: " << cpp_strerror(errno) << dendl;
      conn->mark_down();
      continue;
    }

    // now send the client back our queue pair information so they can
    // complete the initialisation.
    outgoing_qpt = conn->build_qp_tuple(incoming_qpt.get_nonce());
    ldout(cct, 20) << __func__ << " sending qpt=" << outgoing_qpt << dendl;
    len = ::sendto(server_setup_socket, &outgoing_qpt,
                   sizeof(outgoing_qpt), 0,
                   reinterpret_cast<sockaddr *>(&socket_addr.ss_addr()), slen);
    if (len != sizeof(outgoing_qpt)) {
      lderr(cct) << __func__ << " sendto failed, len = " << len << ": "
                 << cpp_strerror(errno) << dendl;
      conn->mark_down();
      continue;
    }
  }
}


void InfRcMessenger::learned_addr(const entity_addr_t &peer_addr_for_me)
{
  // be careful here: multiple threads may block here, and readers of
  // my_inst.addr do NOT hold any lock.

  // this always goes from true -> false under the protection of the
  // mutex.  if it is already false, we need not retake the mutex at
  // all.
  if (!need_addr)
    return ;
  lock.Lock();
  if (need_addr) {
    need_addr = false;
    entity_addr_t t = peer_addr_for_me;
    t.set_port(my_inst.addr.get_port());
    my_inst.addr.addr = t.addr;
    ldout(cct, 1) << __func__ << " learned my addr " << my_inst.addr << dendl;
    _init_local_connection();
  }
  lock.Unlock();
}
