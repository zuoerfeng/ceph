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

static ostream& _prefix(std::ostream *_dout, InfRcWorkerPool::ManagementThread *p) {
  return *_dout << " ManagementThread--";
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
    msgr->recv_message();
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

class C_infrc_async_ev_handler : public EventCallback {
  InfRcWorker *worker;

 public:
  C_infrc_async_ev_handler(InfRcWorker *w): worker(w) {}
  void do_request(int id) {
    worker->handle_rx_event();
    worker->handle_tx_event();
    worker->handle_async_event();
  }
};

static void get_infrc_msg(InfRcMsg &msg, const ceph_entity_addr &addr, const char tag,
                          char *buf, size_t len)
{
  memset(&msg, 0, sizeof(msg));
  msg.addr = addr;
  memcpy(msg.magic_code, INFRC_MAGIC_CODE, sizeof(INFRC_MAGIC_CODE)-1);
  msg.tag = tag;
  if (buf)
    memcpy(&msg.payload, buf, len);
}


//------------------------------
// InfRcWorker class
//------------------------------
InfRcWorker::InfRcWorker(CephContext *c, InfRcWorkerPool *p, int i)
  : cct(c), pool(p), done(false), id(i), infiniband(p->infiniband),
    rx_cq(NULL), tx_cq(NULL), rx_cc(NULL), tx_cc(NULL), lock("InfRcWorker::lock"),
    low_level_rx_buffers(cct->_conf->ms_infiniband_low_level_receive_buffers), center(c)
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

  rx_cq = infiniband->create_comp_queue(pool->get_max_rx_buffers(), rx_cc);
  if (!rx_cq)
    goto err;

  center.create_file_event(rx_cc->get_fd(), EVENT_READABLE,
                           EventCallbackRef(new C_handle_cq_rx(this)));

  tx_cc = infiniband->create_comp_channel();
  if (!tx_cc)
    goto err;

  tx_cq = infiniband->create_comp_queue(pool->get_max_tx_buffers(), tx_cc);
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

    int r = center.process_events(1000*1000*3);
    if (r < 0) {
      ldout(cct, 20) << __func__ << " process events failed: " << cpp_strerror(errno) << dendl;
      // TODO do something?
    } else if (r == 0) {
      // idle
      Mutex::Locker l(lock);
      for (ceph::unordered_map<uint64_t, pair<QueuePair*, InfRcConnectionRef> >::iterator it = qp_conns.begin();
           it != qp_conns.end(); ++it)
        it->second.second->send_keepalive();

    }
  }

  return 0;
}

void InfRcWorker::shutdown()
{
  ldout(cct, 20) << __func__ << dendl;

  Mutex::Locker l(lock);
  outstanding_buffers.clear();
  for (ceph::unordered_map<uint64_t, pair<QueuePair*, InfRcConnectionRef> >::iterator it = qp_conns.begin();
       it != qp_conns.end(); ++it)
    it->second.second->mark_down();
  qp_conns.clear();

  while (!dead_queue_pairs.empty()) {
    delete dead_queue_pairs.back();
    dead_queue_pairs.pop_back();
  }

  if (rx_cq) {
    rx_cc->ack_events();
    delete rx_cq;
    rx_cq = NULL;
  }
  if (tx_cq) {
    tx_cc->ack_events();
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
  QueuePair *qp = get_new_qp();
  // create connection
  InfRcConnectionRef conn = new InfRcConnection(cct, m, this, pool, qp,
                                                pool->infiniband, dest, type);
  register_qp(qp, conn);
  return conn;
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
    if (pool->incr_used_srq_buffers() >= low_level_rx_buffers) {
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

    InfRcConnectionRef conn = get_conn_by_qp(response->qp_num);
    if (!conn) {
      // discard buffer
      ldout(cct, 0) << __func__ << " missing qp_num " << response->qp_num << ", discard bd "
                    << bd << dendl;
    } else {
      conn->process_request(response->qp_num, bp);
      ldout(cct, 20) << __func__ << " Received message from " << response->src_qp
                     << " with " << response->byte_len << " bytes" << dendl;
    }
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
}

/**
 * Called from main event loop when a TX CQ notification is available.
 */
void InfRcWorker::handle_tx_event()
{
  ldout(cct, 20) << __func__ << dendl;
  if (!tx_cc->get_cq_event())
    return ;

  uint64_t tx_queue_depth = pool->get_max_tx_buffers();
  ibv_wc ret_array[tx_queue_depth];
  bool rearmed = false;
 again:
  int n = tx_cq->poll_completion_queue(tx_queue_depth, ret_array);
  ldout(cct, 20) << __func__ << " pool completion queue got " << n
                 << " responses."<< dendl;

  Mutex::Locker l(lock);
  for (int i = 0; i < n; i++) {
    uint64_t id = ret_array[i].wr_id;
    BufferDescriptor* bd = reinterpret_cast<BufferDescriptor*>(id);

    ldout(cct, 20) << __func__ << " got bd=" << id << dendl;
    ceph::unordered_map<uint64_t, InfRcConnectionRef>::iterator it = outstanding_buffers.find(id);
    if (it == outstanding_buffers.end()) {
      lderr(cct) << __func__ << " unknown message, bd=" << id << " should be a bug?" << dendl;
      assert(0);
    }

    InfRcConnectionRef conn = it->second;
    outstanding_buffers.erase(it);
    lock.Unlock();
    conn->ack_message(ret_array[i], bd);
    pool->post_tx_buffer(bd, i + 1 == n ? true : false);
    lock.Lock();
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

  // NOTE: Has TX just transitioned to idle? We should do it when idle!
  // It's now safe to delete queue pairs (see comment by declaration
  // for dead_queue_pairs).
  // Additionally, don't delete qp while outstanding_buffers isn't empty,
  // because we need to check qp's state before sending
  if (outstanding_buffers.empty()) {
    while (!dead_queue_pairs.empty()) {
      ldout(cct, 10) << __func__ << " finally delete qp=" << dead_queue_pairs.back() << dendl;
      delete dead_queue_pairs.back();
      dead_queue_pairs.pop_back();
    }
  }
}

void InfRcWorker::handle_async_event()
{
  ldout(cct, 20) << __func__ << dendl;
  while (1) {
    ibv_async_event async_event;
    ldout(cct, 20) << __func__ << dendl;
    if (ibv_get_async_event(infiniband->device->ctxt, &async_event)) {
      if (errno == EAGAIN)
        return ;

      lderr(cct) << __func__ << " ibv_get_async_event failed. (errno=" << errno
                 << " " << cpp_strerror(errno) << ")" << dendl;
      return;
    }
    // FIXME: Currently we must ensure no other factor make QP in ERROR state,
    // otherwise this qp can't be deleted in current cleanup flow.
    if (async_event.event_type == IBV_EVENT_QP_LAST_WQE_REACHED) {
      uint64_t qpn = async_event.element.qp->qp_num;
      ldout(cct, 10) << __func__ << " event associated qp=" << async_event.element.qp
                     << " evt: " << ibv_event_type_str(async_event.event_type) << dendl;
      InfRcWorker *worker = pool->get_worker_by_qp(qpn);
      InfRcConnectionRef conn = worker->get_conn_by_qp(qpn);
      if (!conn) {
        ldout(cct, 1) << __func__ << " missing qp_num=" << qpn << " discard event" << dendl;
      } else {
        QueuePair *qp = conn->get_qp();
        if (qp && qp->get_local_qp_number() == qpn) {
          ldout(cct, 0) << __func__ << " it's not forwardly stopped by us, reenable=" << conn << dendl;
          conn->fault();
        } else {
          ldout(cct, 10) << __func__ << " this qp is discarded for conn=" << conn << ", just delete it"<< dendl;
        }
        worker->erase_qpn(qpn);
      }
    } else {
      ldout(cct, 0) << __func__ << " ibv_get_async_event: dev=" << infiniband->device->ctxt
                    << " evt: " << ibv_event_type_str(async_event.event_type)
                    << dendl;
    }
    ibv_ack_async_event(&async_event);
  }
}

Infiniband::BufferDescriptor* InfRcWorker::get_message_buffer(InfRcConnectionRef conn)
{
  BufferDescriptor *bd = pool->reserve_message_buffer(conn);
  if (bd) {
    Mutex::Locker l(lock);
    outstanding_buffers[reinterpret_cast<uint64_t>(bd)] = conn;
  }
  ldout(cct, 20) << __func__ << " conn=" << conn << " bd=" << reinterpret_cast<uint64_t>(bd) << dendl;
  return bd;
}

void InfRcWorker::put_message_buffer(BufferDescriptor *bd)
{
  {
    Mutex::Locker l(lock);
    outstanding_buffers.erase(reinterpret_cast<uint64_t>(bd));
  }
  pool->post_tx_buffer(bd, true);
  ldout(cct, 20) << __func__ << " bd=" << reinterpret_cast<uint64_t>(bd) << dendl;
}


//------------------------------
// InfRcWorkerPool class
//------------------------------
void *InfRcWorkerPool::ManagementThread::entry()
{
  ldout(cct, 10) << __func__ << " starting" << dendl;
  while (!done) {
    ldout(cct, 20) << __func__ << " calling regular process" << dendl;
    // idle
    Mutex::Locker l(stop_lock);
    stop_cond.WaitInterval(cct, stop_lock, utime_t(MANAGEMENT_PERIOD, 0));
  }

  return 0;
}

InfRcWorkerPool::InfRcWorkerPool(CephContext *c)
  : cct(c), seq(0), started(false),
    barrier_lock("InfRcWorkerPool::barrier_lock"), barrier_count(0),
    ib_device_name(cct->_conf->ms_infiniband_device_name),
    ib_physical_port(cct->_conf->ms_infiniband_port), lid(-1),
    rx_buffers(NULL), tx_buffers(NULL), srq(NULL), lock("InfRcWorkerPool::Lock"),
    num_used_srq_buffers(0), message_len(cct->_conf->ms_infiniband_buffer_len),
    max_rx_buffers(cct->_conf->ms_infiniband_receive_buffers),
    max_tx_buffers(cct->_conf->ms_infiniband_send_buffers),
    qp_to_worker_lock("InfRcWorkerPool::qp_to_worker_lock"),
    management_thread(this, c),
    log_memory_base(NULL), log_memory_bytes(0), log_memory_region(NULL),
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
    srq = infiniband->create_shared_receive_queue(max_rx_buffers,
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
    uint32_t buffer_size = (message_len + 4095) & ~0xfff;

    rx_buffers = new RegisteredBuffers(cct, infiniband->pd, buffer_size,
                                       max_rx_buffers);
    if (!rx_buffers) {
      lderr(cct) << __func__ << " failed to malloc rx_buffers: " << cpp_strerror(errno) << dendl;
      goto err;
    }
    num_used_srq_buffers = max_rx_buffers;
    for (BufferDescriptor *bd = rx_buffers->begin();
         bd != rx_buffers->end(); ++bd) {
      if (post_srq_receive(bd)) {
        lderr(cct) << __func__ << " failed to post srq buffer: " << cpp_strerror(errno) << dendl;
        goto err;
      }
    }
    assert(num_used_srq_buffers == 0);

    tx_buffers = new RegisteredBuffers(cct, infiniband->pd, buffer_size,
                                       cct->_conf->ms_infiniband_send_buffers);
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
    management_thread.create();
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
  management_thread.stop();
  management_thread.join();
  for (uint64_t i = 0; i < workers.size(); ++i) {
    if (workers[i]->is_started()) {
      workers[i]->stop();
      workers[i]->join();
    }
    delete workers[i];
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
  if (infiniband) {
    delete infiniband;
    infiniband = NULL;
  }
  free_tx_buffers.clear();
  workers.clear();
  started = false;
}

/**
 * Add the given BufferDescriptor to the given free queue.
 *
 * \param[in] bd
 *      The BufferDescriptor to enqueue.
 * \param[in] wakeup
 *      Whether wakeup pending connections
 * \return
 *      0 if success or -1 for failure
 */
int InfRcWorkerPool::post_tx_buffer(BufferDescriptor *bd, bool wakeup)
{
  ldout(cct, 20) << __func__ << " bd=" << reinterpret_cast<uint64_t>(bd) << dendl;
  Mutex::Locker l(lock);
  free_tx_buffers.push_back(bd);
  while (wakeup && !pending_sent_conns.empty()) {
    InfRcConnectionRef c = pending_sent_conns.front();
    c->wakeup_writer();
    ldout(cct, 20) << __func__ << " wakeup pending sent conn=" << c << dendl;
    pending_sent_conns.pop_front();
  }
  return 0;
}

Infiniband::BufferDescriptor* InfRcWorkerPool::reserve_message_buffer(InfRcConnectionRef c)
{
  BufferDescriptor *bd = NULL;
  Mutex::Locker l(lock);
  if (!free_tx_buffers.empty()) {
    bd = free_tx_buffers.back();
    free_tx_buffers.pop_back();
  } else if (pending_sent_conns.back() != c) {
    pending_sent_conns.push_back(c);
  }
  ldout(cct, 20) << __func__ << " bd=" << reinterpret_cast<uint64_t>(bd) << dendl;
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
  ceph_spin_init(&global_seq_lock);
  cct->lookup_or_create_singleton_object<InfRcWorkerPool>(
      pool, InfRcWorkerPool::get_name(cct->_conf->ms_infiniband_device_name,
                                      cct->_conf->ms_infiniband_port));
  local_connection = new InfRcConnection(cct, this, pool->get_worker(), pool,
                                         NULL, pool->infiniband, my_inst.addr,
                                         my_inst.name.type());
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
    connections[dest.addr] = conn;
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

int InfRcMessenger::get_proto_version(int peer_type, bool connect)
{
  int my_type = my_inst.name.type();

  // set reply protocol version
  if (peer_type == my_type) {
    // internal
    return cluster_protocol;
  } else {
    // public
    if (connect) {
      switch (peer_type) {
        case CEPH_ENTITY_TYPE_OSD: return CEPH_OSDC_PROTOCOL;
        case CEPH_ENTITY_TYPE_MDS: return CEPH_MDSC_PROTOCOL;
        case CEPH_ENTITY_TYPE_MON: return CEPH_MONC_PROTOCOL;
      }
    } else {
      switch (my_type) {
        case CEPH_ENTITY_TYPE_OSD: return CEPH_OSDC_PROTOCOL;
        case CEPH_ENTITY_TYPE_MDS: return CEPH_MDSC_PROTOCOL;
        case CEPH_ENTITY_TYPE_MON: return CEPH_MONC_PROTOCOL;
      }
    }
  }
  return 0;
}

/**
 * This method is invoked by the dispatcher when #server_setup_socket becomes
 * readable. It attempts to set up QueuePair with a connecting remote
 * client.
 *
 * \param events
 *      Indicates whether the socket was readable, writable, or both
 */
void InfRcMessenger::recv_message()
{
  ldout(cct, 20) << __func__ << dendl;
  while (1) {
    InfRcMsg msg;
    entity_addr_t addr;
    int r = recv_udp_msg(server_setup_socket, msg, INFRC_UDP_BOOT|INFRC_UDP_PING, &addr);
    if (r < 0) {
      ldout(cct, 0) << __func__ << " recv msg failed." << dendl;
      break;
    } else if (r > 0) {
      break;
    }
    if (msg.tag == INFRC_UDP_BOOT) {
      Infiniband::QueuePairTuple qpt;
      memcpy(&qpt, &msg.payload.boot.qpt, sizeof(msg.payload.boot.qpt));
      accept(qpt, addr);
    } else if (msg.tag == INFRC_UDP_PING) {
      entity_addr_t send_addr = msg.addr, peer_addr = entity_addr_t(msg.addr);
      send_addr.set_port(addr.get_port());
      handle_ping(peer_addr, send_addr);
    }
  }
}

/**
 * This method is invoked by the dispatcher when #server_setup_socket becomes
 * readable. It attempts to set up QueuePair with a connecting remote
 * client.
 *
 * \param events
 *      Indicates whether the socket was readable, writable, or both
 */
void InfRcMessenger::accept(Infiniband::QueuePairTuple &incoming_qpt, entity_addr_t &socket_addr)
{
  ldout(cct, 20) << __func__ << dendl;
  Infiniband::QueuePairTuple outgoing_qpt;

  ldout(cct, 20) << __func__ << " receiving new connection request qpt=" << incoming_qpt << dendl;
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
  } else {
    Mutex::Locker l(lock);
    InfRcConnectionRef conn = _lookup_conn(incoming_qpt.get_sender_addr());
    if (conn) {
      if (!conn->replace(incoming_qpt, outgoing_qpt))
        return;
    } else if (incoming_qpt.get_connect_seq() > 0) {
      // we reset, and they are opening a new session
      ldout(cct, 0) << __func__ << " accept we reset (peer sent cseq "
                    << incoming_qpt.get_connect_seq() << "), sending RESETSESSION"
                    << dendl;
      outgoing_qpt.set_tag(CEPH_MSGR_TAG_RESETSESSION);
    } else {
      // new session
      ldout(cct, 10) << __func__ << " accept new session" << dendl;
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
      if (conn->ready(incoming_qpt, outgoing_qpt) < 0) {
        lderr(cct) << __func__ << " ready failed: " << cpp_strerror(errno) << dendl;
        return;
      }
    }
  }
  if (send_udp_msg(server_setup_socket, INFRC_UDP_BOOT_ACK,
                   (char*)&outgoing_qpt, sizeof(outgoing_qpt), socket_addr,
                   get_myaddr()) < 0)
    lderr(cct) << __func__ << " sendto peer boot message failed." << dendl; // FIXME To do something?
  else
    ldout(cct, 20) << __func__ << " sending qpt=" << outgoing_qpt << dendl;
}

void InfRcMessenger::handle_ping(entity_addr_t &addr, entity_addr_t &sendaddr)
{
  Mutex::Locker l(lock);
  InfRcConnectionRef conn = _lookup_conn(addr);
  if (conn) {
    ldout(cct, 10) << __func__ << " got con ping message" << conn << dendl;
    if (conn->is_connected()) {
      utime_t now = ceph_clock_now(cct);
      struct ceph_timespec ts;
      now.encode_timeval(&ts);
      if (!send_udp_msg(server_setup_socket, INFRC_UDP_PONG, (char*)&ts, sizeof(ts), sendaddr, get_myaddr())) {
        ldout(cct, 20) << __func__ << " send pong message successfully" << dendl;
        return ;
      }
      ldout(cct, 0) << __func__ << " send pong message failed" << dendl;
    }
  }

  if (send_udp_msg(server_setup_socket, INFRC_UDP_BROKEN, NULL, 0, sendaddr, get_myaddr()) < 0)
    ldout(cct, 0) << __func__ << " send broken message failed" << dendl;
  else
    ldout(cct, 20) << __func__ << " send broken message successfully" << dendl;
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

// 1 means no valid buffer read, 0 means got enough buffer
// else return < 0 means error
int InfRcMessenger::recv_udp_msg(int sd, InfRcMsg &msg, uint8_t extag, entity_addr_t *addr)
{
  assert(sd >= 0);
  ssize_t r;
  entity_addr_t socket_addr;
  socklen_t slen = sizeof(socket_addr.ss_addr());
  r = ::recvfrom(sd, &msg, sizeof(msg), 0,
                 reinterpret_cast<sockaddr *>(&socket_addr.ss_addr()), &slen);
  // Drop incoming qpt
  if (cct->_conf->ms_inject_socket_failures && sd >= 0) {
    if (rand() % cct->_conf->ms_inject_socket_failures == 0) {
      ldout(cct, 0) << __func__ << " injecting socket failure" << dendl;
      r = -1;
    }
  }
  if (r == -1) {
    if (errno == EINTR || errno == EAGAIN) {
      return 1;
    } else {
      lderr(cct) << __func__ << " recv got error " << errno << ": "
                             << cpp_strerror(errno) << dendl;
      return -1;
    }
  } else if ((size_t)r != sizeof(msg)) { // valid message length
    lderr(cct) << __func__ << " recv got bad length (" << r << ")." << dendl;
    return 1;
  } else if (memcmp(msg.magic_code, INFRC_MAGIC_CODE, sizeof(msg.magic_code))) { // valid magic code
    ldout(cct, 0) << __func__ << " wrong magic code: " << msg.magic_code << dendl;
    return 1;
  } else if (extag && !(msg.tag & extag)) { // valid tag
    ldout(cct, 0) << __func__ << " wrong tag: " << msg.tag << dendl;
    return 1;
  } else { // valid message
    if (addr) {
      *addr = socket_addr;
    }
    ldout(cct, 5) << __func__ << " get valid udp message(" << msg.tag << ")" << dendl;
    return 0;
  }
}

int InfRcMessenger::send_udp_msg(int sd, const char tag, char *buf, size_t len, entity_addr_t &peeraddr, const entity_addr_t &myaddr)
{
  assert(sd >= 0);
  int retry = 0;
  ssize_t r;
  InfRcMsg msg;
  get_infrc_msg(msg, myaddr, tag, buf, len);

 retry:
  r = ::sendto(sd, (char*)&msg, sizeof(msg), 0, reinterpret_cast<sockaddr *>(&peeraddr.ss_addr()),
               sizeof(peeraddr.ss_addr()));
  // Drop incoming qpt
  if (cct->_conf->ms_inject_socket_failures && sd >= 0) {
    if (rand() % cct->_conf->ms_inject_socket_failures == 0) {
      ldout(cct, 0) << __func__ << " injecting socket failure" << dendl;
      r = -1;
    }
  }

  if ((size_t)r != sizeof(msg)) {
    if (r < 0 && (errno == EINTR || errno == EAGAIN) && retry < 3) {
      retry++;
      goto retry;
    }
    if (r < 0)
      lderr(cct) << __func__ << " send returned error " << errno << ": "
                             << cpp_strerror(errno) << dendl;
    else
      lderr(cct) << __func__ << " send got bad length (" << r << ") " << dendl;
    return -1;
  }
  return 0;
}
