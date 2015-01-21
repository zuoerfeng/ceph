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

#include <fcntl.h>

#include "auth/Crypto.h"
#include "common/errno.h"
#include "Infiniband.h"

#define dout_subsys ceph_subsys_ms

#undef dout_prefix
#define dout_prefix *_dout << "Infiniband "

/**
 * Create a new QueuePair. This factory should be used in preference to
 * the QueuePair constructor directly, since this lets derivatives of
 * Infiniband, e.g. MockInfiniband (if it existed),
 * return mocked out QueuePair derivatives.
 *
 * \return
 *      QueuePair on success or NULL if init fails
 * See QueuePair::QueuePair for parameter documentation.
 */
Infiniband::QueuePair*
Infiniband::create_queue_pair(ibv_qp_type type, int ib_physical_port,
                              ibv_srq *srq, ibv_cq *txcq, ibv_cq *rxcq,
                              uint32_t max_send_wr, uint32_t max_recv_wr,
                              uint32_t q_key)
{
  ldout(cct, 20) << __func__ << " type=" << type << " ib_port=" << ib_physical_port
                 << " srq=" << srq << " txcq=" << txcq << " rxcq=" << rxcq
                 << " max_send_wr=" << max_send_wr << " max_recv_wr=" << max_recv_wr
                 << " q_key=" << q_key << dendl;
  Infiniband::QueuePair *qp = new QueuePair(*this, type, ib_physical_port, srq,
                                            txcq, rxcq, max_send_wr, max_recv_wr,
                                            q_key);
  if (qp->init()) {
    delete qp;
    return NULL;
  }
  return qp;
}


/**
 * Create a new CompletionChannel. This factory should be used in preference to
 * the CompletionChannel constructor directly, since this lets derivatives of
 * Infiniband.
 *
 * \return
 *      CompletionChannel on success or NULL if init fails
 * See CompletionChannel::CompletionChannel for parameter documentation.
 */
Infiniband::CompletionChannel* Infiniband::create_comp_channel()
{
  ldout(cct, 20) << __func__ << " started." << dendl;
  Infiniband::CompletionChannel *cc = new Infiniband::CompletionChannel(*this);
  if (cc->init()) {
    delete cc;
    return NULL;
  }
  return cc;
}

/**
 * Create a new CompletionQueue. This factory should be used in preference to
 * the CompletionQueue constructor directly, since this lets derivatives of
 * Infiniband.
 *
 * \return
 *      CompletionQueue on success or NULL if init fails
 * See CompletionQueue::CompletionQueue for parameter documentation.
 */
Infiniband::CompletionQueue* Infiniband::create_comp_queue(const uint32_t qd,
                                                           CompletionChannel *cc)
{
  ldout(cct, 20) << __func__ << " qd=" << qd << " completion channel=" << cc << dendl;
  Infiniband::CompletionQueue *cq = new Infiniband::CompletionQueue(*this, qd, cc);
  if (cq->init()) {
    delete cq;
    return NULL;
  }
  return cq;
}

/**
 * Given a string representation of the `status' field from Verbs
 * struct `ibv_wc'.
 *
 * \param[in] status
 *      The integer status obtained in ibv_wc.status.
 * \return
 *      A string corresponding to the given status.
 */
const char*
Infiniband::wc_status_to_string(int status)
{
    static const char *lookup[] = {
        "SUCCESS",
        "LOC_LEN_ERR",
        "LOC_QP_OP_ERR",
        "LOC_EEC_OP_ERR",
        "LOC_PROT_ERR",
        "WR_FLUSH_ERR",
        "MW_BIND_ERR",
        "BAD_RESP_ERR",
        "LOC_ACCESS_ERR",
        "REM_INV_REQ_ERR",
        "REM_ACCESS_ERR",
        "REM_OP_ERR",
        "RETRY_EXC_ERR",
        "RNR_RETRY_EXC_ERR",
        "LOC_RDD_VIOL_ERR",
        "REM_INV_RD_REQ_ERR",
        "REM_ABORT_ERR",
        "INV_EECN_ERR",
        "INV_EEC_STATE_ERR",
        "FATAL_ERR",
        "RESP_TIMEOUT_ERR",
        "GENERAL_ERR"
    };

    if (status < IBV_WC_SUCCESS || status > IBV_WC_GENERAL_ERR)
        return "<status out of range!>";
    return lookup[status];
}

/**
 * Obtain the infiniband "local ID" of the device corresponding to
 * the provided context and port number.
 *
 * \param[in] port
 *      Port on the device whose local ID we're looking up. This value
 *      is typically 1, except on adapters with multiple physical ports.
 * \return
 *      The local ID corresponding to the given parameters. Otherwise, return -1
 *      if error.
 */
int Infiniband::get_lid(int port)
{
    ibv_port_attr ipa;
    int ret = ibv_query_port(device.ctxt, (uint8_t)(port), &ipa);
    if (ret) {
        lderr(cct) << __func__ << " ibv_query_port failed on port "
                   << port << dendl;
        return -1;
    }
    ldout(cct, 20) << __func__ << " port=" << port << " get lid=" << ipa.lid << dendl;
    return ipa.lid;
}

/**
 * Add the given BufferDescriptor to the given shared receive queue.
 *
 * \param[in] srq
 *      The shared receive queue on which to enqueue this BufferDescriptor.
 * \param[in] bd
 *      The BufferDescriptor to enqueue.
 * \return
 *      0 on success, -1 on error.
 */
int Infiniband::post_srq_receive(ibv_srq* srq, BufferDescriptor *bd)
{
  ldout(cct, 20) << __func__ << " srq=" << srq << " bd=" << bd << dendl;
  ibv_sge isge;
  isge.addr = reinterpret_cast<uint64_t>(bd->buffer);
  isge.length = bd->bytes;
  isge.lkey = bd->mr->lkey;
  ibv_recv_wr rx_work_request;

  memset(&rx_work_request, 0, sizeof(rx_work_request));
  rx_work_request.wr_id = reinterpret_cast<uint64_t>(bd);// stash descriptor ptr
  rx_work_request.next = NULL;
  rx_work_request.sg_list = &isge;
  rx_work_request.num_sge = 1;

  ibv_recv_wr *badWorkRequest;
  int ret = ibv_post_srq_recv(srq, &rx_work_request, &badWorkRequest);
  if (ret) {
    lderr(cct) << __func__ << " ib_post_srq_recv failed on post "
               << cpp_strerror(errno) << dendl;
    return -1;
  }
  return 0;
}

/**
 * Create a shared receive queue. This basically wraps the verbs call. 
 *
 * \param[in] max_wr
 *      The max number of outstanding work requests in the SRQ.
 * \param[in] max_sge
 *      The max number of scatter elements per WR.
 * \return
 *      A valid ibv_srq pointer, or NULL on error.
 */
ibv_srq* Infiniband::create_shared_receive_queue(uint32_t max_wr, uint32_t max_sge)
{
  ldout(cct, 20) << __func__ << " max_wr=" << max_wr << " max_sge=" << max_sge << dendl;
  ibv_srq_init_attr sia;
  memset(&sia, 0, sizeof(sia));
  sia.srq_context = device.ctxt;
  sia.attr.max_wr = max_wr;
  sia.attr.max_sge = max_sge;
  return ibv_create_srq(pd.pd, &sia);
}

/**
 * Destroy a shared receive queue. This basically wraps the verbs call. 
 *
 * \param[in] srq
 *      The shared receive queue to destroy.
 * \return
 *      0 on success, -1 on error.
 */
int Infiniband::destroy_shared_receive_queue(ibv_srq *srq)
{
  ldout(cct, 20) << __func__ << " srq=" << srq << dendl;
  if (ibv_destroy_srq(srq)) {
    lderr(cct) << __func__ << " destroy_srq failed: " << cpp_strerror(errno) << dendl;
    return -1;
  }
  return 0;
}

int Infiniband::set_nonblocking(int fd)
{
  ldout(cct, 20) << __func__ << " fd=" << fd << dendl;
  int flags = ::fcntl(fd, F_GETFL);
  if (flags == -1) {
    lderr(cct) << __func__ << " fcntl F_GETFL failed: "
               << strerror(errno) << dendl;
    return -1;
  }
  if (::fcntl(fd, F_SETFL, flags | O_NONBLOCK)) {
    lderr(cct) << __func__ << " fcntl F_SETFL failed: "
               << strerror(errno) << dendl;
    return -1;
  }
  return 0;
}

//-------------------------------------
// Infiniband::QueuePair class
//-------------------------------------

/**
 * Construct a QueuePair. This object hides some of the ugly
 * initialisation of Infiniband "queue pairs", which are single-side
 * transmit and receive queues. This object can represent both reliable
 * connected (RC) and unreliable datagram (UD) queue pairs. Not all
 * methods are valid to all queue pair types.
 *
 * Somewhat confusingly, each communicating end has a QueuePair, which are
 * bound (one might say "paired", but that's even more confusing). This
 * object is somewhat analogous to a TCB in TCP. 
 *
 * After this method completes, the QueuePair will be in the INIT state.
 * A later call to #plumb() will transition it into the RTS state for
 * regular use with RC queue pairs.
 *
 * \param infiniband
 *      The #Infiniband object to associate this QueuePair with.
 * \param type
 *      The type of QueuePair to create. Currently valid values are
 *      IBV_QPT_RC for reliable QueuePairs and IBV_QPT_UD for
 *      unreliable ones.
 * \param port
 *      The physical port on the HCA we will use this QueuePair on.
 *      The default is 1, though some devices have multiple ports.
 * \param srq
 *      The Verbs shared receive queue to associate this QueuePair
 *      with. All writes received will use WQEs placed on the
 *      shared queue. If NULL, do not use a shared receive queue.
 * \param txcq
 *      The Verbs completion queue to be used for transmissions on
 *      this QueuePair.
 * \param rxcq
 *      The Verbs completion queue to be used for receives on this
 *      QueuePair.
 * \param max_send_wr
 *      Maximum number of outstanding send work requests allowed on
 *      this QueuePair.
 * \param max_recv_wr
 *      Maximum number of outstanding receive work requests allowed on
 *      this QueuePair.
 * \param q_key
 *      UD Queue Pairs only. The q_key for this pair.
 */
Infiniband::QueuePair::QueuePair(Infiniband& infiniband, ibv_qp_type type,
    int port, ibv_srq *srq, ibv_cq *txcq, ibv_cq *rxcq,
    uint32_t max_send_wr, uint32_t max_recv_wr, uint32_t q_key)
    : infiniband(infiniband),
      type(type),
      ctxt(infiniband.device.ctxt),
      ib_physical_port(port),
      pd(infiniband.pd.pd),
      srq(srq),
      qp(NULL),
      txcq(txcq),
      rxcq(rxcq),
      initial_psn(0),
      max_send_wr(max_send_wr),
      max_recv_wr(max_recv_wr),
      q_key(q_key),
      dead(false)
{
  get_random_bytes((char*)&initial_psn, sizeof(initial_psn));
  if (type != IBV_QPT_RC && type != IBV_QPT_UD && type != IBV_QPT_RAW_PACKET) {
    lderr(infiniband.cct) << __func__ << "invalid queue pair type" << dendl;
    assert(0);
  }
}

int Infiniband::QueuePair::init()
{
  ldout(infiniband.cct, 20) << __func__ << " started." << dendl;
  ibv_qp_init_attr qpia;
  memset(&qpia, 0, sizeof(qpia));
  qpia.send_cq = txcq;
  qpia.recv_cq = rxcq;
  qpia.srq = srq;                      // use the same shared receive queue
  qpia.cap.max_send_wr  = max_send_wr; // max outstanding send requests
  qpia.cap.max_recv_wr  = max_recv_wr; // max outstanding recv requests
  qpia.cap.max_send_sge = 1;           // max send scatter-gather elements
  qpia.cap.max_recv_sge = 1;           // max recv scatter-gather elements
  qpia.cap.max_inline_data =           // max bytes of immediate data on send q
      MAX_INLINE_DATA;
  qpia.qp_type = type;                 // RC, UC, UD, or XRC
  qpia.sq_sig_all = 0;                 // only generate CQEs on requested WQEs

  qp = ibv_create_qp(pd, &qpia);
  if (qp == NULL) {
    lderr(infiniband.cct) << __func__ << " failed to create queue pair" << dendl;
    return -1;
  }

  ldout(infiniband.cct, 20) << __func__ << " successfully create queue pair: "
                            << "qp=" << qp << dendl;

  // move from RESET to INIT state
  ibv_qp_attr qpa;
  memset(&qpa, 0, sizeof(qpa));
  qpa.qp_state   = IBV_QPS_INIT;
  qpa.pkey_index = 0;
  qpa.port_num   = (uint8_t)(ib_physical_port);
  qpa.qp_access_flags = IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_LOCAL_WRITE;
  qpa.qkey       = q_key;

  int mask = IBV_QP_STATE | IBV_QP_PORT;
  switch (type) {
  case IBV_QPT_RC:
      mask |= IBV_QP_ACCESS_FLAGS;
      mask |= IBV_QP_PKEY_INDEX;
      break;
  case IBV_QPT_UD:
      mask |= IBV_QP_QKEY;
      mask |= IBV_QP_PKEY_INDEX;
      break;
  case IBV_QPT_RAW_PACKET:
      break;
  default:
      assert(0);
  }

  int ret = ibv_modify_qp(qp, &qpa, mask);
  if (ret) {
    ibv_destroy_qp(qp);
    lderr(infiniband.cct) << __func__ << " failed to transition to INIT state: "
               << cpp_strerror(errno) << dendl;
    return -1;
  }
  ldout(infiniband.cct, 20) << __func__ << " successfully change queue pair to INIT:"
                            << " qp=" << qp << dendl;
  return 0;
}

/**
 * Destroy the QueuePair by freeing the Verbs resources allocated.
 */
Infiniband::QueuePair::~QueuePair()
{
  if (qp)
    assert(!ibv_destroy_qp(qp));
}

/**
 * Bring an newly created RC QueuePair into the RTS state, enabling
 * regular bidirectional communication. This is necessary before
 * the QueuePair may be used. Note that this only applies to
 * RC QueuePairs.
 *
 * \param qpt
 *      QueuePairTuple representing the remote QueuePair. The Verbs
 *      interface requires us to exchange handshaking information
 *      manually. This includes initial sequence numbers, queue pair
 *      numbers, and the HCA infiniband addresses.
 *
 * \return
 *      -1 if this method is called on a QueuePair that is not of
 *      type IBV_QPT_RC, or if the QueuePair is not in the INIT state.
 *      0 for success.
 */
int Infiniband::QueuePair::plumb(QueuePairTuple *qpt)
{
  ldout(infiniband.cct, 20) << __func__ << " qpt=" << *qpt << dendl;
  ibv_qp_attr qpa;
  int r;

  if (type != IBV_QPT_RC) {
    lderr(infiniband.cct) << __func__ << "plumb() called on wrong qp type" << dendl;
    assert(0);
  }

  if (get_state() != IBV_QPS_INIT) {
    lderr(infiniband.cct) << __func__ << "plumb() on qp in state " << get_state() << dendl;
    return -1;
  }

  // now connect up the qps and switch to RTR
  memset(&qpa, 0, sizeof(qpa));
  qpa.qp_state = IBV_QPS_RTR;
  qpa.path_mtu = IBV_MTU_1024;
  qpa.dest_qp_num = qpt->get_qpn();
  qpa.rq_psn = qpt->get_psn();
  qpa.max_dest_rd_atomic = 1;
  qpa.min_rnr_timer = 12;
  qpa.ah_attr.is_global = 0;
  qpa.ah_attr.dlid = qpt->get_lid();
  qpa.ah_attr.sl = 0;
  qpa.ah_attr.src_path_bits = 0;
  qpa.ah_attr.port_num = (uint8_t)(ib_physical_port);

  r = ibv_modify_qp(qp, &qpa, IBV_QP_STATE |
                              IBV_QP_AV |
                              IBV_QP_PATH_MTU |
                              IBV_QP_DEST_QPN |
                              IBV_QP_RQ_PSN |
                              IBV_QP_MIN_RNR_TIMER |
                              IBV_QP_MAX_DEST_RD_ATOMIC);
  if (r) {
    lderr(infiniband.cct) << __func__ << " failed to transition to RTR state: "
               << cpp_strerror(errno) << dendl;
    return -1;
  }

  ldout(infiniband.cct, 20) << __func__ << " transition to RTR state successfully." << dendl;

  // now move to RTS
  qpa.qp_state = IBV_QPS_RTS;

  // How long to wait before retrying if packet lost or server dead.
  // Supposedly the timeout is 4.096us*2^timeout.  However, the actual
  // timeout appears to be 4.096us*2^(timeout+1), so the setting
  // below creates a 135ms timeout.
  qpa.timeout = 14;

  // How many times to retry after timeouts before giving up.
  qpa.retry_cnt = 7;

  // How many times to retry after RNR (receiver not ready) condition
  // before giving up. Occurs when the remote side has not yet posted
  // a receive request.
  qpa.rnr_retry = 7; // 7 is infinite retry.
  qpa.sq_psn = initial_psn;
  qpa.max_rd_atomic = 1;

  r = ibv_modify_qp(qp, &qpa, IBV_QP_STATE |
                              IBV_QP_TIMEOUT |
                              IBV_QP_RETRY_CNT |
                              IBV_QP_RNR_RETRY |
                              IBV_QP_SQ_PSN |
                              IBV_QP_MAX_QP_RD_ATOMIC);
  if (r) {
    lderr(infiniband.cct) << __func__ << " failed to transition to RTS state: "
               << cpp_strerror(errno) << dendl;
    return -1;
  }

  // the queue pair should be ready to use once the client has finished
  // setting up their end.
  ldout(infiniband.cct, 20) << __func__ << " transition to RTS state successfully." << dendl;
  return 0;
}

/**
 * Change RC QueuePair into the ERROR state. This is necessary modify
 * the Queue Pair into the Error state and poll all of the relevant
 * Work Completions prior to destroying a Queue Pair.
 * Since destroying a Queue Pair does not guarantee that its Work
 * Completions are removed from the CQ upon destruction. Even if the
 * Work Completions are already in the CQ, it might not be possible to
 * retrieve them. If the Queue Pair is associated with an SRQ, it is
 * recommended wait for the affiliated event IBV_EVENT_QP_LAST_WQE_REACHED
 *
 * \return
 *      -1 if the QueuePair can't switch to ERROR
 *      0 for success.
 */
int Infiniband::QueuePair::to_dead()
{
  assert(!dead);
  dead = true;

  ibv_qp_attr qpa;
  memset(&qpa, 0, sizeof(qpa));
  qpa.qp_state = IBV_QPS_ERR;

  int mask = IBV_QP_STATE;
  int ret = ibv_modify_qp(qp, &qpa, mask);
  if (ret) {
    lderr(infiniband.cct) << __func__ << " failed to transition to ERROR state: "
                           << cpp_strerror(errno) << dendl;
    return -1;
  }
  return ret;
}

//-------------------------------------
// Infiniband::CompletionChannel class
//-------------------------------------
Infiniband::CompletionChannel::~CompletionChannel()
{
  if (channel)
    assert(ibv_destroy_comp_channel(channel) == 0);
}

int Infiniband::CompletionChannel::init()
{
  ldout(infiniband.cct, 20) << __func__ << " started." << dendl;
  channel = ibv_create_comp_channel(infiniband.device.ctxt);
  if (!channel) {
    lderr(infiniband.cct) << __func__ << " failed to create receive completion channel: "
                          << cpp_strerror(errno) << dendl;
    return -1;
  }
  int rc = infiniband.set_nonblocking(channel->fd);
  if (rc < 0) {
    ibv_destroy_comp_channel(channel);
    return -1;
  }
  return 0;
}

/**
 * Waits for a notification to be sent on the indicated completion channel.
 *
 * \param cq
 *      CompletionQueue which will be notified
 *
 * \return
 *      true if exists event notify, false if no
 */
bool Infiniband::CompletionChannel::get_cq_event()
{
  ldout(infiniband.cct, 20) << __func__ << " started." << dendl;
  ibv_cq *cq = NULL;
  void *ev_ctx;
  if (ibv_get_cq_event(channel, &cq, &ev_ctx)) {
    if (errno != EAGAIN && errno != EINTR)
      lderr(infiniband.cct) << __func__ << "failed to retrieve CQ event: "
                                  << cpp_strerror(errno) << dendl;
    return false;
  }

  /* accumulate number of cq events that need to
   * be acked, and periodically ack them
   */
  if (++cq_events_that_need_ack == MAX_ACK_EVENT) {
    ldout(infiniband.cct, 20) << __func__ << " ack aq events." << dendl;
    ibv_ack_cq_events(cq, MAX_ACK_EVENT);
    cq_events_that_need_ack = 0;
  }

  return true;
}
//-------------------------------------
// Infiniband::CompletionQueue class
//-------------------------------------
Infiniband::CompletionQueue::~CompletionQueue()
{
  int r = ibv_destroy_cq(cq);
  assert(r == 0);
}

int Infiniband::CompletionQueue::init()
{
  cq = ibv_create_cq(infiniband.device.ctxt, queue_depth, this, channel->get_channel(), 0);
  if (!cq) {
    lderr(infiniband.cct) << __func__ << " failed to create receive completion queue: "
                          << cpp_strerror(errno) << dendl;
    return -1;
  }

  if (ibv_req_notify_cq(cq, 0)) {
    lderr(infiniband.cct) << " ibv_req_notify_cq failed: " << cpp_strerror(errno) << dendl;
    ibv_destroy_cq(cq);
    return -1;
  }

  ldout(infiniband.cct, 20) << __func__ << " successfully create cq=" << cq << dendl;
  return 0;
}

int Infiniband::CompletionQueue::rearm_notify(bool solicite_only)
{
  ldout(infiniband.cct, 20) << __func__ << " started." << dendl;
  int r = ibv_req_notify_cq(cq, 0);
  if (r) {
    lderr(infiniband.cct) << __func__ << " failed to notify cq: " << cpp_strerror(errno) << dendl;
  }
  return r;
}

/**
 * Poll a completion queue. This simply wraps the verbs call.
 *
 * \param[in] num_entries
 *      The maximum number of work completion entries to obtain.
 * \param[out] retWcArray
 *      Pointer to an array of ``numEntries'' ibv_wc structs.
 *      Completions are returned here.
 * \return
 *      The number of entries obtained. 0 if none, < 0 on error. Strictly less
 *      than ``numEntries'' means that ``cq'' has been emptied.
 */
int Infiniband::CompletionQueue::poll_completion_queue(int num_entries,
                                                       ibv_wc *ret_wc_array)
{
  int r = ibv_poll_cq(cq, num_entries, ret_wc_array);
  if (r < 0) {
    lderr(infiniband.cct) << __func__ << " poll_completion_queue occur met error: "
                          << cpp_strerror(errno) << dendl;
    return -1;
  }
  return r;
}
