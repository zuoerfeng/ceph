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
#include <arpa/inet.h>
#include <sys/socket.h>

#include "auth/none/AuthNoneProtocol.h"
#include "InfRcMessenger.h"
#include "InfRcConnection.h"

#define SEQ_MASK  0x7fffffff

#define dout_subsys ceph_subsys_ms
#undef dout_prefix
#define dout_prefix _conn_prefix(_dout)
ostream& InfRcConnection::_conn_prefix(std::ostream *_dout) {
  return *_dout << "-- " << infrc_msgr->get_myinst().addr << " >> " << peer_addr << " conn(" << this
        << " qp=" << (qp ? qp->get_local_qp_number() : -1)
        << " s=" << get_state_name(state)
        << " psn=" << (qp ? qp->get_initial_psn() : -1)
        << " gseq=" << global_seq
        << " cseq=" << connect_seq
        << " l=" << policy.lossy
        << ").";
}

class C_infrc_wakeup : public EventCallback {
  InfRcConnectionRef conn;

 public:
  C_infrc_wakeup(InfRcConnectionRef c): conn(c) {}
  void do_request(int fd_or_id) {
    conn->wakeup_from(fd_or_id);
  }
};

class C_infrc_read : public EventCallback {
  InfRcConnectionRef conn;

 public:
  C_infrc_read(InfRcConnectionRef c): conn(c) {}
  void do_request(int fd_or_id) {
    conn->process();
  }
};

class C_infrc_handle_reset : public EventCallback {
  InfRcMessenger *msgr;
  InfRcConnectionRef conn;

 public:
  C_infrc_handle_reset(InfRcMessenger *m, InfRcConnectionRef c): msgr(m), conn(c) {}
  void do_request(int id) {
    msgr->ms_deliver_handle_reset(conn.get());
  }
};

class C_infrc_handle_remote_reset : public EventCallback {
  InfRcMessenger *msgr;
  InfRcConnectionRef conn;

 public:
  C_infrc_handle_remote_reset(InfRcMessenger *m, InfRcConnectionRef c): msgr(m), conn(c) {}
  void do_request(int id) {
    msgr->ms_deliver_handle_remote_reset(conn.get());
  }
};

class C_infrc_deliver_connect : public EventCallback {
  InfRcMessenger *msgr;
  InfRcConnectionRef conn;

 public:
  C_infrc_deliver_connect(InfRcMessenger *msgr, InfRcConnectionRef c): msgr(msgr), conn(c) {}
  void do_request(int id) {
    msgr->ms_deliver_handle_connect(conn.get());
  }
};

class C_infrc_deliver_accept : public EventCallback {
  InfRcMessenger *msgr;
  InfRcConnectionRef conn;

 public:
  C_infrc_deliver_accept(InfRcMessenger *msgr, InfRcConnectionRef c): msgr(msgr), conn(c) {}
  void do_request(int id) {
    msgr->ms_deliver_handle_accept(conn.get());
  }
};


class C_infrc_local_deliver : public EventCallback {
  InfRcConnectionRef conn;
 public:
  C_infrc_local_deliver(InfRcConnectionRef c): conn(c) {}
  void do_request(int id) {
    conn->local_deliver();
  }
};

class C_infrc_clean_handler : public EventCallback {
  InfRcConnectionRef conn;
 public:
  C_infrc_clean_handler(InfRcConnectionRef c): conn(c) {}
  void do_request(int id) {
    conn->cleanup_handler();
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

class C_infrc_write : public EventCallback {
  InfRcConnectionRef conn;

 public:
  C_infrc_write(InfRcConnection *c): conn(c) {}
  void do_request(int id) {
    conn->send_pending_messages();
  }
};


InfRcConnection::InfRcConnection(CephContext *c, InfRcMessenger *m, InfRcWorker *w,
                                 InfRcWorkerPool *p, Infiniband::QueuePair *qp,
                                 Infiniband *ib, const entity_addr_t& addr, int type)
  : Connection(c, m), infiniband(ib), qp(qp), worker(w), pool(p), center(&w->center),
    cm_lock("InfRcConnection::cm_lock"), global_seq(0), connect_seq(0),
    out_seq(0), in_seq(0), state(STATE_NEW), got_bad_auth(false), authorizer(NULL),
    client_setup_socket(-1), exchange_count(0),
    keepalive_retry(0), pending_msg(NULL), receive_stage(END), infrc_msgr(m)
{
  set_peer_type(type);
  set_peer_addr(addr);
  policy = m->get_policy(type);

  read_handler.reset(new C_infrc_read(this));
  write_handler.reset(new C_infrc_write(this));
  reset_handler.reset(new C_infrc_handle_reset(infrc_msgr, this));
  remote_reset_handler.reset(new C_infrc_handle_remote_reset(infrc_msgr, this));
  local_deliver_handler.reset(new C_infrc_local_deliver(this));
  wakeup_handler.reset(new C_infrc_wakeup(this));
}

InfRcConnection::~InfRcConnection()
{
  delete authorizer;
  assert(client_setup_socket == -1);
  assert(!qp);
  assert(pending_send.empty());
  assert(register_time_events.empty());
}

void InfRcConnection::connect()
{
  ldout(infrc_msgr->cct, 20) << __func__ << dendl;

  state = STATE_BEFORE_CONNECTING;
  // rescheduler connection in order to avoid lock dep
  // may called by external thread(send_message)
  center->dispatch_event_external(read_handler);
}

void InfRcConnection::send_keepalive()
{
  if (client_setup_socket < 0 || (state != STATE_OPEN && state != STATE_WAIT))
    return ;

  ldout(infrc_msgr->cct, 20) << __func__ << dendl;
  utime_t now = ceph_clock_now(infrc_msgr->cct);
  if (!sent_queue.empty() || in_queue() || (now - last_ping).to_msec() < KEEPALIVE_MIN_PREIOD_MS) {
    ldout(infrc_msgr->cct, 10) << __func__ << " connection has job or just sent before, no need to detect alive" << dendl;
    return ;
  }

  uint64_t diff_ms = (now - last_keepalive_ack).to_msec();
  if (keepalive_retry > KEEPALIVE_RETRY_COUNT && diff_ms > KEEPALIVE_TIMEOUT_MS) {
    ldout(infrc_msgr->cct, 0) << __func__ << " failed to send keepalive, reach max limit("
                              << KEEPALIVE_RETRY_COUNT << ")=" << keepalive_retry << " or timeout("
                              << KEEPALIVE_TIMEOUT_MS << ")=" << diff_ms << "ms."<< dendl;
    fault(true);
    return ;
  }

  struct ceph_timespec ts;
  now.encode_timeval(&ts);
  int r = infrc_msgr->send_udp_msg(client_setup_socket, INFRC_UDP_PING, (char*)&ts, sizeof(ts), peer_addr,
                                   infrc_msgr->get_myaddr());
  keepalive_retry++;
  last_ping = now;
  if (r < 0) {
    ldout(infrc_msgr->cct, 5) << __func__ << " failed to send keepalive, retry later." << dendl;
  } else {
    ldout(infrc_msgr->cct, 10) << __func__ << " successfully to send keepalive." << dendl;
  }
}

void InfRcConnection::process()
{
  int r = 0;
  int prev_state;
  Mutex::Locker l(cm_lock);
  do {
    prev_state = state;
    ldout(infrc_msgr->cct, 20) << __func__ << " prev state is " << get_state_name(prev_state) << dendl;
    switch (state) {
      case STATE_BEFORE_CONNECTING:
      {
        if (!got_bad_auth) {
          delete authorizer;
          authorizer = infrc_msgr->get_authorizer(peer_type, false);
        }

        if (client_setup_socket != -1) {
          center->delete_file_event(client_setup_socket, EVENT_READABLE);
          ::close(client_setup_socket);
        }
        // Set up the udp sockets we use for out-of-band infiniband handshaking.
        // For clients, the kernel will automatically assign a dynamic port on
        // first use.
        client_setup_socket = ::socket(PF_INET, SOCK_DGRAM, 0);
        if (client_setup_socket == -1) {
          lderr(infrc_msgr->cct) << __func__ << " failed to create client socket: "
                     << strerror(errno) << dendl;
          goto fail;
        }

        r = ::connect(client_setup_socket, (sockaddr*)(&peer_addr.addr),
                      peer_addr.addr_size());
        if (r < 0) {
          lderr(infrc_msgr->cct) << __func__ << " failed to connect " << peer_addr << ": "
                     << strerror(errno) << dendl;
          goto fail;
        }

        int flags;
        /* Set the socket nonblocking.
         * Note that fcntl(2) for F_GETFL and F_SETFL can't be
         * interrupted by a signal. */
        if ((flags = fcntl(client_setup_socket, F_GETFL)) < 0 ) {
          lderr(infrc_msgr->cct) << __func__ << " fcntl(F_GETFL) failed: " << cpp_strerror(errno) << dendl;
          goto fail;
        }
        if (fcntl(client_setup_socket, F_SETFL, flags | O_NONBLOCK) < 0) {
          lderr(infrc_msgr->cct) << __func__ << " fcntl(F_SETFL,O_NONBLOCK): " << cpp_strerror(errno) << dendl;
          goto fail;
        }
        if (!qp) {
          qp = worker->get_new_qp();
          worker->register_qp(qp, this);
        }
        global_seq = infrc_msgr->get_global_seq();
        state = STATE_CONNECTING_SENDING;
        exchange_count = 0;
        break;
      }

      case STATE_CONNECTING_SENDING:
      {
        if (exchange_count++ <= infrc_msgr->cct->_conf->ms_infiniband_exchange_max_timeouts) {
          Infiniband::QueuePairTuple outgoing_qpt(
              static_cast<uint16_t>(worker->get_lid()),
              qp->get_local_qp_number(), qp->get_initial_psn(),
              policy.features_supported, 0, infrc_msgr->get_myinst().name.type(),
              global_seq, connect_seq, authorizer ? authorizer->protocol: 0, 0,
              in_seq, infrc_msgr->get_myaddr(), peer_addr);

          if (authorizer)
            ldout(infrc_msgr->cct, 10) << __func__ <<  "connect_msg.authorizer_len="
                                       << outgoing_qpt.get_authorizer_len() << " protocol="
                                       << outgoing_qpt.get_authorizer_protocol() << dendl;
          // Send wrong qpt
          if (infrc_msgr->cct->_conf->ms_inject_socket_failures && client_setup_socket >= 0) {
            if (rand() % infrc_msgr->cct->_conf->ms_inject_socket_failures == 0) {
              ldout(infrc_msgr->cct, 0) << __func__ << " injecting socket failure" << dendl;
              memset(&outgoing_qpt, sizeof(outgoing_qpt), 1);
            }
          }

          ldout(infrc_msgr->cct, 20) << __func__ << " sending qpt=" << outgoing_qpt << dendl;
          r = infrc_msgr->send_udp_msg(client_setup_socket, INFRC_UDP_BOOT, (char*)&outgoing_qpt,
                                       sizeof(outgoing_qpt), peer_addr, infrc_msgr->get_myaddr());
          if (r < 0)
            goto fail;
          state = STATE_CONNECTING;
          last_wakeup = ceph_clock_now(infrc_msgr->cct);
          center->create_file_event(client_setup_socket, EVENT_READABLE, read_handler);
        } else {
          lderr(infrc_msgr->cct) << __func__ << " failed to exchange with server ("
                                 << peer_addr.addr << ") within sent request "
                                 << infrc_msgr->cct->_conf->ms_infiniband_exchange_max_timeouts
                                 << " times" << dendl;
          goto fail;
        }
        break;
      }

      case STATE_CONNECTING:
      {
        Infiniband::QueuePairTuple incoming_qpt;
        InfRcMsg msg;
        r = infrc_msgr->recv_udp_msg(client_setup_socket, msg, INFRC_UDP_BOOT_ACK);
        if (r > 0) {
          utime_t diff = ceph_clock_now(infrc_msgr->cct) - last_wakeup;
          if (diff.to_msec() >= infrc_msgr->cct->_conf->ms_infiniband_exchange_timeout_ms) {
            ldout(infrc_msgr->cct, 1) << __func__ << " timeout since last connect=" << last_wakeup
                                      << " retry connect" << dendl;
          } else {
            if (register_time_events.empty()) {
              // avoid time precious problem and leak retry event
              register_time_events.insert(center->create_time_event(
                      diff.to_nsec()/1000 + 10000, wakeup_handler));
              ldout(infrc_msgr->cct, 20) << __func__ << " still has " << diff.to_msec() << "ms"
                                         << " recreate time event" << dendl;
            }
            break;
          }
        } else if (r < 0) {
          goto fail;
        } else if (msg.tag == INFRC_UDP_BOOT_ACK) {
          memcpy(&incoming_qpt, &msg.payload.boot.qpt, sizeof(msg.payload.boot.qpt));
          if (CEPH_MSGR_TAG_READY == incoming_qpt.get_tag()) {
            ldout(infrc_msgr->cct, 20) << __func__ << " state=" << get_state_name(state)
                                       << " receiving qpt=" << incoming_qpt << dendl;
            if (global_seq == incoming_qpt.get_global_seq()) {
              uint64_t feat_missing = policy.features_required & ~(uint64_t)incoming_qpt.get_features();
              if (feat_missing) {
                ldout(infrc_msgr->cct, 1) << __func__ << " missing required features " << std::hex
                                          << feat_missing << std::dec << dendl;
                goto fail;
              }

              // plumb up our queue pair with the server's parameters.
              r = qp->plumb(&incoming_qpt);
              if (r == 0) {
                cm_lock.Unlock();
                ldout(infrc_msgr->cct, 20) << __func__ << " connect peer addr for me is " << incoming_qpt.get_receiver_addr() << dendl;
                infrc_msgr->learned_addr(incoming_qpt.get_receiver_addr());
                if (infrc_msgr->cct->_conf->ms_inject_internal_delays) {
                  ldout(infrc_msgr->cct, 10) << __func__ << " sleep for " << infrc_msgr->cct->_conf->ms_inject_internal_delays << dendl;
                  utime_t t;
                  t.set_from_double(infrc_msgr->cct->_conf->ms_inject_internal_delays);
                  t.sleep();
                }
                cm_lock.Lock();
                if (state != STATE_CONNECTING) {
                  ldout(infrc_msgr->cct, 1) << __func__ << " conn may already down or accept peer connect msg" << dendl;
                  assert(state == STATE_CLOSED || state == STATE_OPEN);
                  return ;
                }
                connect_seq++;
                assert(connect_seq == incoming_qpt.get_connect_seq());
                discard_pending_queue_to(incoming_qpt.get_msg_seq());
                // FIXME: If we have an authorizer, get a new AuthSessionHandler to deal with ongoing
                // security of the connection.  PLR
                center->dispatch_event_external(
                    EventCallbackRef(new C_infrc_deliver_connect(infrc_msgr, this)));
                infrc_msgr->ms_deliver_handle_fast_connect(this);
                set_features(policy.features_supported & incoming_qpt.get_features());
                state = STATE_OPEN;
                if (in_queue())
                  center->dispatch_event_external(write_handler);
                ldout(infrc_msgr->cct, 10) << __func__ << " ready to send/redeive to "
                                           << peer_addr << dendl;
                break;
              }
            } else {
              lderr(infrc_msgr->cct) << __func__ << " received global_seq doesn't match "
                                     << global_seq << " != " << incoming_qpt.get_global_seq() << dendl;
            }
          } else {
            handle_other_tag(incoming_qpt);
            break;
          }
        } else {
          ldout(infrc_msgr->cct, 0) << __func__ << " unexpected message tag=" << msg.tag << dendl;
        }

        state = STATE_CONNECTING_SENDING;
        break;
      }

      case STATE_OPEN:
      {
        ldout(infrc_msgr->cct, 20) << __func__ << " enter open state" << dendl;
        InfRcMsg msg;
        while (client_setup_socket >= 0) {
          int r = infrc_msgr->recv_udp_msg(client_setup_socket, msg, INFRC_UDP_PONG|INFRC_UDP_BROKEN);
          if (r != 0)
            break;
          if (msg.tag == INFRC_UDP_PONG) {
            ldout(infrc_msgr->cct, 10) << " got pong message." << dendl;
            last_keepalive_ack = utime_t(msg.payload.pong.ts);
            keepalive_retry = 0;
          } else if (msg.tag == INFRC_UDP_BROKEN) {
            ldout(infrc_msgr->cct, 10) << " got peer broken message." << dendl;
            _fault(true);
          }

        }

        break;
      }

      case STATE_STANDBY:
      {
        ldout(infrc_msgr->cct, 20) << __func__ << " enter standby state" << dendl;
        break;
      }

      case STATE_CLOSED:
      {
        ldout(infrc_msgr->cct, 10) << __func__ << " already closed "
                                   << peer_addr << dendl;
        break;
      }

      case STATE_WAIT:
      {
        ldout(infrc_msgr->cct, 20) << __func__ << " enter wait state" << dendl;
        break;
      }

      default:
      {
        lderr(infrc_msgr->cct) << __func__ << " unknown state=" << state << dendl;
        assert(0);
      }
    }
    continue;

  } while (prev_state != state);

  return ;
fail:
    _fault();
}

void InfRcConnection::handle_other_tag(Infiniband::QueuePairTuple &incoming_qpt)
{
  switch (incoming_qpt.get_tag()) {
    case CEPH_MSGR_TAG_BADAUTHORIZER:
      ldout(infrc_msgr->cct, 0) << __func__ << " connect got BADAUTHORIZER" << dendl;
      if (got_bad_auth) {
        _fault();
      } else {
        got_bad_auth = true;
        delete authorizer;
        authorizer = infrc_msgr->get_authorizer(peer_type, true);  // try harder
        state = STATE_BEFORE_CONNECTING;
      }
      break;

    case CEPH_MSGR_TAG_FEATURES:
      ldout(infrc_msgr->cct, 0) << __func__ << " connect protocol feature mismatch, my "
                                << std::hex << policy.features_supported << " < peer "
                                << incoming_qpt.get_features() << " missing "
                                << (incoming_qpt.get_features() & ~policy.features_supported)
                                << std::dec << dendl;
      center->dispatch_event_external(reset_handler);
      _stop();
      break;

    case CEPH_MSGR_TAG_RETRY_GLOBAL:
      global_seq = infrc_msgr->get_global_seq(incoming_qpt.get_global_seq());
      ldout(infrc_msgr->cct, 10) << __func__ << " connect got RETRY_GLOBAL "
                                 << incoming_qpt.get_global_seq() << " chose new "
                                 << global_seq << dendl;
      state = STATE_BEFORE_CONNECTING;
      break;

   case CEPH_MSGR_TAG_RESETSESSION:
      ldout(infrc_msgr->cct, 0) << __func__ << " connect got RESETSESSION" << dendl;
      was_session_reset();
      state = STATE_BEFORE_CONNECTING;
      break;

    case CEPH_MSGR_TAG_RETRY_SESSION:
      assert(incoming_qpt.get_connect_seq() > connect_seq);
      connect_seq = incoming_qpt.get_connect_seq();
      ldout(infrc_msgr->cct, 10) << __func__ << " connect got RETRY_SESSION "
                                 << connect_seq << " -> "
                                 << incoming_qpt.get_connect_seq() << dendl;
      state = STATE_BEFORE_CONNECTING;
      break;

    case CEPH_MSGR_TAG_WAIT:
      ldout(infrc_msgr->cct, 3) << __func__ << " connect got WAIT (connection race)" << dendl;
      state = STATE_WAIT;
      break;

    default:
      ldout(infrc_msgr->cct, 0) << __func__ << " unknown tag=" << incoming_qpt.get_tag()
                                << ". This is bug!" << dendl;
      assert(0);
  }
}

int InfRcConnection::send_message(Message *m)
{
  ldout(infrc_msgr->cct, 10) << __func__ << " m=" << m << dendl;
  m->get_header().src = infrc_msgr->get_myname();
  if (!m->get_priority())
    m->set_priority(msgr->get_default_send_priority());

  Mutex::Locker l(cm_lock);
  m->set_seq(++out_seq);
  if (state == STATE_OPEN) {
    if (!in_queue()) {
      Infiniband::BufferDescriptor *bd = worker->get_message_buffer(this);
      if (bd) {
        if (send_zero_copy_msg(m, bd) < 0) {
          pending_send.push_back(m);
          worker->put_message_buffer(bd);
          _fault();
        }
        return 0;
      }
    }
    ldout(infrc_msgr->cct, 20) << __func__ << " pending " << *m << " wait for send." << dendl;
    pending_send.push_back(m);
  } else if (state == STATE_CLOSED) {
    ldout(infrc_msgr->cct, 1) << __func__ << " connection already stopped: " << *m << dendl;
    m->put();
    return -1;
  } else if (infrc_msgr->get_myaddr() == peer_addr) {
    ldout(infrc_msgr->cct, 20) << __func__ << " local dispatch " << *m << dendl;
    local_messages.push_back(m);
    center->dispatch_event_external(local_deliver_handler);
  } else {
    ldout(infrc_msgr->cct, 20) << __func__ << " state=" << get_state_name(state)
                               << " pending " << *m << " wait for send." << dendl;
    pending_send.push_back(m);
    if (state == STATE_STANDBY && !policy.server) {
      ldout(infrc_msgr->cct, 10) << __func__ << " new message comming, try to connect"
                                 << " policy.server is false" << dendl;
      connect();
    } else {
      center->dispatch_event_external(write_handler);
    }
  }

  return 0;
}

int InfRcConnection::send_zero_copy_msg(Message *m, Infiniband::BufferDescriptor *bd)
{
  ldout(infrc_msgr->cct, 20) << __func__ << " m=" << m << " bd=" << reinterpret_cast<uint64_t>(bd) << dendl;

  m->set_connection(this);

  uint64_t features = get_features();
  if (m->empty_payload())
    ldout(infrc_msgr->cct, 20) << __func__ << " encoding " << " features " << features
                               << " " << m << " " << *m << dendl;
  else
    ldout(infrc_msgr->cct, 20) << __func__ << " half-reencoding " << " features "
                               << features << " " << m << " " << *m << dendl;
  // encode and copy out of *m
  m->encode(features, 0);

  assert(!pending_bl.length());
  bufferlist bl;
  // prepare everything
  ceph_msg_header& header = m->get_header();
  ceph_msg_footer& footer = m->get_footer();

  ldout(infrc_msgr->cct, 20) << __func__ << " sending " << m->get_payload().length() << " + "
                             << m->get_middle().length() << " + " << m->get_data().length()
                             << " byte message" << dendl;
  bl.append((char*)&header, sizeof(header));
  bl.append((char*)&footer, sizeof(footer));
  bl.append(m->get_payload());
  bl.append(m->get_middle());
  bl.append(m->get_data());

  int r = send_zero_copy(bl, bd, m, INFRC_MSG_FIRST);
  if (r > 0) {
    assert(!pending_bl.length());
    bl.swap(pending_bl);
    center->dispatch_event_external(write_handler);
  }

  return r;
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
 * \return
 *      0 if success or -1 for failure, 1 means exist remaining buffer
 */
int InfRcConnection::send_zero_copy(bufferlist &bl, Infiniband::BufferDescriptor *bd,
                                    Message *m, const char tag)
{
  assert(cm_lock.is_locked());
  assert(qp);
  const bool allow_zero_copy = true;
  ibv_sge isge[MAX_TX_SGE_COUNT];

  uint32_t last_chunk_index = bl.buffers().size() - 1;

  uint32_t current_chunk = 0;
  uint32_t current_sge = 0;

  // The variables below allow us to collect several chunks from the
  // Buffer into a single sge in some situations. They describe a
  // range of bytes in bd that have not yet been put in an sge, but
  // must go into the next sge.
  memcpy(bd->buffer, &tag, sizeof(tag));
  char* unadded_start = bd->buffer;
  char* unadded_end = bd->buffer + sizeof(tag);
  uint64_t bd_left = bd->bytes - sizeof(tag);
  uint64_t sent = 0;

  ldout(infrc_msgr->cct, 20) << __func__ << " associated message=" << m << " seq=" << m->get_seq()
                             << " bd_left=" << bd_left << " bl length=" << bl.length() << dendl;
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
      sent += it->length();
    } else {
      uint64_t copied = MIN(it->length(), bd_left);
      memcpy(unadded_end, it->c_str(), copied);
      unadded_end += copied;
      bd_left -= copied;
      sent += copied;
      if (bd_left == 0)
        break;
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

  ibv_send_wr *bad_tx_work_request;

  //if (infrc_msgr->cct->_conf->ms_inject_socket_failures) {
  //  if (rand() % infrc_msgr->cct->_conf->ms_inject_socket_failures == 0) {
  //    ldout(infrc_msgr->cct, 0) << __func__ << " injecting socket failure" << dendl;
  //    qp->to_reset();
  //  }
  //}

  if (ibv_post_send(qp->get_qp(), &tx_work_request, &bad_tx_work_request)) {
    lderr(infrc_msgr->cct) << __func__ << " failed to send message=" << m
                           << " ibv_post_send failed(most probably should be peer not ready): "
                           << cpp_strerror(errno) << dendl;
    return -1;
  }

  if (!policy.lossy)
    sent_queue.push_back(make_pair(reinterpret_cast<uint64_t>(bd), m));

  if (sent != bl.length()) {
    // unfinished bl
    ldout(infrc_msgr->cct, 10) << __func__ << " remaining " << bl.length()-sent << " bytes wait for sent" << dendl;
    bufferlist s;
    bl.splice(sent, bl.length()-sent, &s);
    s.swap(bl);
    // in case of ack_message release pending message
    if (!policy.lossy) {
      pending_msg = m;
      pending_msg->get();
    }
    ldout(infrc_msgr->cct, 20) << __func__ << " partial post seq=" << m->get_seq() << " qpn="
                               << qp->get_local_qp_number() << " bd=" << reinterpret_cast<uint64_t>(bd)
                               << " message(" << *m << ")" << dendl;
    return 1;
  }

  ldout(infrc_msgr->cct, 20) << __func__ << " successfully post seq=" << m->get_seq() << " qpn="
                             << qp->get_local_qp_number() << " bd=" << reinterpret_cast<uint64_t>(bd)
                             << " message(" << *m << ")" << dendl;
  // release message if completed and we don't queue it
  if (policy.lossy)
    m->put();

  return 0;
}

/*
 * return true hit caller can continue send
 */
void InfRcConnection::send_pending_messages()
{
  ldout(infrc_msgr->cct, 20) << __func__ << dendl;
  Mutex::Locker l(cm_lock);

  if (state != STATE_OPEN)
    return ;

  int r;
  while (in_queue()) {
    Infiniband::BufferDescriptor *bd = worker->get_message_buffer(this);
    if (bd) {
      if (pending_bl.length()) {
        assert(pending_msg);
        r = send_zero_copy(pending_bl, bd, pending_msg, INFRC_MSG_CONTINUE);
        if (r == 0) {
          pending_bl.clear();
          pending_msg = NULL;
        }
      } else {
        Message *m = pending_send.front();
        r = send_zero_copy_msg(m, bd);
        if (r >= 0)
          pending_send.pop_front();
      }
      if (r < 0) {
        worker->put_message_buffer(bd);
        _fault();
        break;
      }
    } else {
      break;
    }
  }
}

void InfRcConnection::_stop()
{
  ldout(infrc_msgr->cct, 10) << __func__ << " qp=" << qp << dendl;
  assert(cm_lock.is_locked());
  if (state == STATE_CLOSED) {
    assert(!qp);
    return ;
  }

  infrc_msgr->unregister_conn(this);
  state = STATE_CLOSED;

  if (client_setup_socket >= 0) {
    center->delete_file_event(client_setup_socket, EVENT_READABLE);
    ::close(client_setup_socket);
    client_setup_socket = -1;
  }

  if (qp) {
    qp->to_dead();
    qp = NULL;
  }
  requeue_sent();
  while (!pending_send.empty()) {
    Message *m = pending_send.front();
    pending_send.pop_front();
    m->put();
  }
  for (set<uint64_t>::iterator it = register_time_events.begin();
       it != register_time_events.end(); ++it)
    center->delete_time_event(*it);
  register_time_events.clear();
  center->dispatch_event_external(
      EventCallbackRef(new C_infrc_clean_handler(this)));
}

void InfRcConnection::_fault(bool onlive)
{
  assert(cm_lock.is_locked());

  if (state == STATE_CLOSED) {
    ldout(infrc_msgr->cct, 10) << __func__ << " state is already STATE_CLOSED" << dendl;
    assert(pending_send.empty());
    return ;
  }

  if (policy.lossy &&
      state != STATE_BEFORE_CONNECTING && state != STATE_CONNECTING_SENDING && state != STATE_CONNECTING) {
    ldout(infrc_msgr->cct, 10) << __func__ << " on lossy channel, failing" << dendl;
    center->dispatch_event_external(reset_handler);
    _stop();
    return ;
  }

  if (qp) {
    qp->to_dead();
    qp = NULL;
  }
  requeue_sent();

  if (policy.standby && !in_queue() && !onlive) {
    ldout(infrc_msgr->cct, 0) << __func__ << " with nothing to send, going to standby" << dendl;
    state = STATE_STANDBY;
    return;
  }

  if (state != STATE_CONNECTING_SENDING && state != STATE_CONNECTING && state != STATE_BEFORE_CONNECTING) {
    // policy maybe empty when state is in accepting
    if (policy.server) {
      ldout(infrc_msgr->cct, 0) << __func__ << " server, going to standby" << dendl;
      state = STATE_STANDBY;
    } else {
      ldout(infrc_msgr->cct, 0) << __func__ << " initiating reconnect" << dendl;
      connect_seq++;
      state = STATE_BEFORE_CONNECTING;
    }
    backoff = utime_t();
  } else {
    if (backoff == utime_t()) {
      backoff.set_from_double(infrc_msgr->cct->_conf->ms_initial_backoff);
    } else {
      backoff += backoff;
      if (backoff > infrc_msgr->cct->_conf->ms_max_backoff)
        backoff.set_from_double(infrc_msgr->cct->_conf->ms_max_backoff);
    }
    state = STATE_BEFORE_CONNECTING;
    ldout(infrc_msgr->cct, 10) << __func__ << " waiting " << backoff << dendl;
  }

  // woke up again;
  register_time_events.insert(center->create_time_event(backoff.to_nsec()/1000, wakeup_handler));
}

int InfRcConnection::randomize_out_seq()
{
  if (get_features() & CEPH_FEATURE_MSG_AUTH) {
    // Set out_seq to a random value, so CRC won't be predictable.   Don't bother checking seq_error
    // here.  We'll check it on the call.  PLR
    int seq_error = get_random_bytes((char *)&out_seq, sizeof(out_seq));
    out_seq &= SEQ_MASK;
    ldout(infrc_msgr->cct, 10) << __func__ << " " << out_seq << dendl;
    return seq_error;
  } else {
    // previously, seq #'s always started at 0.
    out_seq = 0;
    return 0;
  }
}

void InfRcConnection::discard_pending_queue_to(uint64_t seq)
{
  ldout(infrc_msgr->cct, 20) << __func__ << " current out_seq=" << out_seq
                             << " pending_send's size=" << pending_send.size()
                             << " discard message to " << seq << dendl;
  while (!pending_send.empty()) {
    Message *m = pending_send.front();
    if (m->get_seq() > seq)
      break;
    ldout(infrc_msgr->cct, 20) << __func__ << " discard message's seq=" << m->get_seq() << dendl;
    m->put();
    pending_send.pop_front();
  }
}

void InfRcConnection::was_session_reset()
{
  ldout(infrc_msgr->cct, 10) << __func__ << " started" << dendl;

  requeue_sent();
  while (!pending_send.empty()) {
    Message *m = pending_send.front();
    pending_send.pop_front();
    m->put();
  }
  center->dispatch_event_external(remote_reset_handler);

  if (randomize_out_seq()) {
    ldout(infrc_msgr->cct, 15) << __func__ << " could not get random bytes to set seq number for session reset; set seq number to " << out_seq << dendl;
  }

  in_seq = 0;
  connect_seq = 0;
}

void InfRcConnection::wakeup_from(uint64_t id)
{
  cm_lock.Lock();
  register_time_events.erase(id);
  cm_lock.Unlock();
  process();
}

int InfRcConnection::_ready(Infiniband::QueuePairTuple &incoming_qpt,
                            Infiniband::QueuePairTuple &outgoing_qpt)
{
  CryptoKey session_key;
  ldout(infrc_msgr->cct, 10) << __func__ << " qpt=" << incoming_qpt << dendl;

  // fake an auth buffer
  AuthNoneAuthorizer auth;
  auth.build_authorizer(infrc_msgr->cct->_conf->name, 1);

  bool authorizer_valid;
  bufferlist auth_reply;
  assert(incoming_qpt.get_authorizer_len() == 0);
  if (!infrc_msgr->verify_authorizer(this, peer_type, incoming_qpt.get_authorizer_protocol(), auth.bl,
                                     auth_reply, authorizer_valid, session_key) || !authorizer_valid) {
    ldout(infrc_msgr->cct, 0) << __func__ << " got bad authorizer" << dendl;
    outgoing_qpt.set_tag(CEPH_MSGR_TAG_BADAUTHORIZER);
    return -1;
  }

  int r = qp->plumb(&incoming_qpt);
  if (r == 0) {
    center->dispatch_event_external(
        EventCallbackRef(new C_infrc_deliver_accept(infrc_msgr, this)));
    infrc_msgr->ms_deliver_handle_fast_accept(this);
    connect_seq = incoming_qpt.get_connect_seq() + 1;
    global_seq = incoming_qpt.get_global_seq();
    set_features(policy.features_supported & incoming_qpt.get_features());
    state = STATE_OPEN;
    outgoing_qpt = build_qp_tuple();
    if (in_queue())
      center->dispatch_event_external(write_handler);
  } else {
    _fault();
  }
  return r;
}

void InfRcConnection::local_deliver()
{
  ldout(infrc_msgr->cct, 10) << __func__ << dendl;
  Mutex::Locker l(cm_lock);
  while (!local_messages.empty()) {
    Message *m = local_messages.back();
    local_messages.pop_back();
    m->set_connection(this);
    m->set_recv_stamp(ceph_clock_now(infrc_msgr->cct));
    ldout(infrc_msgr->cct, 10) << __func__ << " " << *m << " local deliver " << dendl;
    infrc_msgr->ms_fast_preprocess(m);
    cm_lock.Unlock();
    if (infrc_msgr->ms_can_fast_dispatch(m)) {
      infrc_msgr->ms_fast_dispatch(m);
    } else {
      infrc_msgr->ms_deliver_dispatch(m);
    }
    cm_lock.Lock();
  }
}

Infiniband::QueuePairTuple InfRcConnection::build_qp_tuple()
{
  Infiniband::QueuePairTuple t(worker->get_lid(), qp->get_local_qp_number(), qp->get_initial_psn(),
                               policy.features_supported, CEPH_MSGR_TAG_READY,
                               infrc_msgr->get_myinst().name.type(),
                               global_seq, connect_seq, in_seq, 0, 0,
                               infrc_msgr->get_myaddr(), peer_addr);
  return t;
}

void InfRcConnection::process_request(uint64_t qpn, bufferptr &bp)
{
  ldout(infrc_msgr->cct, 20) << __func__ << " processing qp=" << qpn << dendl;
  Mutex::Locker l(cm_lock);
  if (state == STATE_CLOSED) {
    ldout(infrc_msgr->cct, 1) << __func__ << " this connection is closed" << ", discard bp="
                              << bp << dendl;
    return ;
  }

  char tag = *reinterpret_cast<char*>(bp.c_str());
  uint32_t copied, offset = sizeof(tag);
  ldout(infrc_msgr->cct, 20) << __func__ << " tag=" << tag << ": " << bp << dendl;
  if (tag == INFRC_MSG_FIRST) {
    // new message
    stage_bl[0].clear();
    stage_bl[1].clear();
    stage_bl[2].clear();
    rcv_header = *reinterpret_cast<ceph_msg_header*>(bp.c_str()+offset);
    ldout(infrc_msgr->cct, 20) << __func__ << " got header type=" << rcv_header.type
                               << " src " << entity_name_t(rcv_header.src)
                               << " front=" << rcv_header.front_len
                               << " middle=" << rcv_header.middle_len
                               << " data=" << rcv_header.data_len
                               << " off " << rcv_header.data_off << dendl;
    offset += sizeof(ceph_msg_header);
    rcv_footer = *reinterpret_cast<ceph_msg_footer*>(bp.c_str()+offset);
    offset += sizeof(ceph_msg_footer);
    stage_left[0] = rcv_header.front_len;
    stage_left[1] = rcv_header.middle_len;
    stage_left[2] = rcv_header.data_len;
    receive_stage = FRONT;
  } else {
    assert(tag == INFRC_MSG_CONTINUE);
  }

  while (receive_stage != END) {
    if (stage_left[receive_stage]) {
      if (offset == bp.length())
        break;
      copied = MIN(stage_left[receive_stage], bp.length()-offset);
      stage_left[receive_stage] -= copied;
      stage_bl[receive_stage].append(bp, offset, copied);
      offset += copied;
    }
    if (!stage_left[receive_stage])
      receive_stage++;
  }
  if (receive_stage != END) {
    ldout(infrc_msgr->cct, 20) << __func__ << " got partial buffer(" << bp.length() << "), cur stage="
                               << receive_stage << " cur left " << stage_left[receive_stage] << dendl;
    return ;
  }

  ldout(infrc_msgr->cct, 20) << __func__ << " got " << stage_bl[0].length() << " + " << stage_bl[1].length()
                              << " + " << stage_bl[2].length() << " byte message" << dendl;
  Message *message = decode_message(infrc_msgr->cct, 0, rcv_header, rcv_footer, stage_bl[0], stage_bl[1],
                                    stage_bl[2]);
  if (!message) {
    ldout(infrc_msgr->cct, 1) << __func__ << " decode message failed, dropped" << dendl;
    return ;
  }

  utime_t now = ceph_clock_now(infrc_msgr->cct);
  message->set_recv_stamp(now);
  message->set_throttle_stamp(now);
  message->set_recv_complete_stamp(now);
  message->set_connection(this);

  if (message->get_seq() <= in_seq) {
    ldout(infrc_msgr->cct, 0) << __func__ << " got old message " << message->get_seq()
                  << " <= " << in_seq << " " << message << " " << *message
                  << ", discarding" << dendl;
    message->put();
    if (has_feature(CEPH_FEATURE_RECONNECT_SEQ) && infrc_msgr->cct->_conf->ms_die_on_old_message)
      assert(0 == "old msgs despite reconnect_seq feature");
    return ;
  }
  if (message->get_seq() > in_seq + 1) {
    ldout(infrc_msgr->cct, 0) << __func__ << " missed message?  skipped from seq "
                              << in_seq << " to " << message->get_seq() << dendl;
    if (infrc_msgr->cct->_conf->ms_die_on_skipped_message)
      assert(0 == "skipped incoming seq");
  }

  in_seq = message->get_seq();

  ldout(infrc_msgr->cct, 10) << __func__ << " got message=" << message << " seq=" << in_seq
                             << " qpn=" << qpn << " " << *message << dendl;

  infrc_msgr->ms_fast_preprocess(message);
  if (infrc_msgr->ms_can_fast_dispatch(message)) {
    cm_lock.Unlock();
    infrc_msgr->ms_fast_dispatch(message);
    cm_lock.Lock();
  } else {
    center->dispatch_event_external(
        EventCallbackRef(new C_infrc_handle_dispatch(infrc_msgr, message)));
  }
}

void InfRcConnection::ack_message(ibv_wc &wc, Infiniband::BufferDescriptor *bd)
{
  Mutex::Locker l(cm_lock);
  if (state != STATE_OPEN) {
    ldout(infrc_msgr->cct, 0) << __func__ << " current state is " << get_state_name(state) << ", discard ack bd="
                              << bd << dendl;
    return ;
  }

  if (wc.status != IBV_WC_SUCCESS) {
    if (wc.status == IBV_WC_RETRY_EXC_ERR) {
      lderr(infrc_msgr->cct) << __func__ << " connection between server and client not working. Disconnect this now" << dendl;
    } else if (wc.status == IBV_WC_WR_FLUSH_ERR) {
      lderr(infrc_msgr->cct) << __func__ << " Work Request Flushed Error: this connection's qp="
                             << wc.qp_num << " should be down while this WR=" << wc.wr_id
                             << " still in flight." << dendl;
    } else {
      lderr(infrc_msgr->cct) << __func__ << " send work request returned error for buffer("
                             << wc.wr_id << ") status(" << wc.status << "): "
                             << infiniband->wc_status_to_string(wc.status) << dendl;
    }
    _fault();
  } else if (!sent_queue.empty()) {
    pair<uint64_t, Message*> p = sent_queue.front();
    Message *m = p.second;
    if (p.first != reinterpret_cast<uint64_t>(bd)) {
      ldout(infrc_msgr->cct, 1) << __func__ << " it should be a reset for qp, dropping message " << m << dendl;
      return ;
    }
    ldout(infrc_msgr->cct, 20) << __func__ << " m=" << m << " seq=" << m->get_seq()
                               << " bd=" << wc.wr_id << dendl;
    sent_queue.pop_front();
    m->put();
  }
}

bool InfRcConnection::replace(Infiniband::QueuePairTuple &incoming_qpt,
                              Infiniband::QueuePairTuple &outgoing_qpt)
{
  if (infrc_msgr->cct->_conf->ms_inject_socket_failures && client_setup_socket >= 0) {
    if (rand() % infrc_msgr->cct->_conf->ms_inject_socket_failures == 0 &&
        infrc_msgr->cct->_conf->ms_inject_internal_delays) {
      ldout(infrc_msgr->cct, 10) << __func__ << " sleep for "
                           << infrc_msgr->cct->_conf->ms_inject_internal_delays << dendl;
      utime_t t;
      t.set_from_double(infrc_msgr->cct->_conf->ms_inject_internal_delays);
      t.sleep();
    }
  }

  Mutex::Locker l(cm_lock);
  if (global_seq >= incoming_qpt.get_global_seq()) {
    ldout(infrc_msgr->cct, 1) << __func__ << " receive older global gseq=" << incoming_qpt.get_global_seq()
                              << " existing gseq=" << global_seq << ", let peer retry." << dendl;
    outgoing_qpt.set_tag(CEPH_MSGR_TAG_RETRY_GLOBAL);
    outgoing_qpt.set_global_seq(global_seq);
    return true;
  }
  ldout(infrc_msgr->cct, 10) << __func__ << " accept existing " << this << ".gseq " << global_seq << dendl;

  if (policy.lossy) {
    ldout(infrc_msgr->cct, 0) << __func__ << " accept replacing existing (lossy) channel (new one lossy="
                              << policy.lossy << ")" << dendl;
    was_session_reset();
    goto replace;
  }

  ldout(infrc_msgr->cct, 0) << __func__ << " accept connect_seq " << incoming_qpt.get_connect_seq()
                            << " vs existing csq=" << connect_seq << " state="
                            << get_state_name(state) << dendl;
  if (incoming_qpt.get_connect_seq() == 0 && connect_seq > 0) {
    ldout(infrc_msgr->cct,0) << __func__ << " accept peer reset, then tried to connect to us, replacing" << dendl;
    // this is a hard reset from peer
    //is_reset_from_peer = true;
    if (policy.resetcheck)
      was_session_reset(); // this resets out_queue, msg_ and connect_seq #'s
    goto replace;
  }

  if (incoming_qpt.get_connect_seq() < connect_seq) {
    // old attempt, or we sent READY but they didn't get it.
    ldout(infrc_msgr->cct, 10) << __func__ << " accept existing " << this << ".cseq "
                               << connect_seq << " > " << incoming_qpt.get_connect_seq()
                               << ", RETRY_SESSION" << dendl;
    outgoing_qpt.set_connect_seq(connect_seq + 1);
    outgoing_qpt.set_tag(CEPH_MSGR_TAG_RETRY_SESSION);
    return true;
  }

  if (incoming_qpt.get_connect_seq() == connect_seq) {
    // if the existing connection successfully opened, and/or
    // subsequently went to standby, then the peer should bump
    // their connect_seq and retry: this is not a connection race
    // we need to resolve here.
    if (state == STATE_OPEN || state == STATE_STANDBY) {
      ldout(infrc_msgr->cct, 10) << __func__ << " accept connection race, existing " << this
                                 << ".cseq " << connect_seq << " == "
                                 << incoming_qpt.get_connect_seq()
                                 << ", OPEN|STANDBY, RETRY_SESSION" << dendl;
      outgoing_qpt.set_connect_seq(connect_seq + 1);
      outgoing_qpt.set_tag(CEPH_MSGR_TAG_RETRY_SESSION);
      return true;
    }

    // connection race?
    if (peer_addr < infrc_msgr->get_myaddr() || policy.server) {
      // incoming wins
      ldout(infrc_msgr->cct, 10) << __func__ << " accept connection race, existing " << this
                                 << ".cseq " << connect_seq << " == " << incoming_qpt.get_connect_seq()
                                 << ", or we are server, replacing my attempt" << dendl;
      goto replace;
    } else {
      // our existing outgoing wins
      ldout(infrc_msgr->cct, 10) << __func__ << " accept connection race, existing "
                                 << this << ".cseq " << connect_seq
                                 << " == " << connect_seq << ", sending WAIT" << dendl;
      assert(peer_addr > infrc_msgr->get_myaddr());
      outgoing_qpt.set_tag(CEPH_MSGR_TAG_WAIT);
      return true;
    }
  }

  assert(incoming_qpt.get_connect_seq() > connect_seq);
  assert(incoming_qpt.get_global_seq() >= global_seq);
  if (policy.resetcheck &&   // RESETSESSION only used by servers; peers do not reset each other
      connect_seq == 0) {
    ldout(infrc_msgr->cct, 0) << __func__ << " accept we reset (peer sent cseq "
                              << incoming_qpt.get_connect_seq() << ", " << this << ".cseq = "
                              << connect_seq << "), sending RESETSESSION" << dendl;
    outgoing_qpt.set_tag(CEPH_MSGR_TAG_RESETSESSION);
    return true;
  }

  // reconnect
  ldout(infrc_msgr->cct, 10) << __func__ << " accept peer sent cseq " << incoming_qpt.get_connect_seq()
                             << " > " << connect_seq << dendl;

 replace:
  if (policy.lossy)
    // disconnect from the Connection
    center->dispatch_event_external(reset_handler);
  requeue_sent();
  discard_pending_queue_to(incoming_qpt.get_msg_seq());
  if (qp)
    qp->to_dead();
  qp = worker->get_new_qp();
  worker->register_qp(qp, this);

  if (_ready(incoming_qpt, outgoing_qpt)) {
    ldout(infrc_msgr->cct, 5) << __func__ << " failed to plumb qp=" << qp << dendl;
    return false;
  }
  return true;
}
