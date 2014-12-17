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

#include "InfRcMessenger.h"
#include "InfRcConnection.h"

#define dout_subsys ceph_subsys_ms
#undef dout_prefix
#define dout_prefix _conn_prefix(_dout)
ostream& InfRcConnection::_conn_prefix(std::ostream *_dout) {
  return *_dout << "-- " << infrc_msgr->get_myinst().addr << " >> " << peer_addr << " conn(" << this
        << " qp=" << (qp ? qp->get_local_qp_number() : -1)
        << " s=" << get_state_name(state)
        << " psn=" << (qp ? qp->get_initial_psn() : -1)
        << " nonce=" << nonce
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


InfRcConnection::InfRcConnection(CephContext *c, InfRcMessenger *m, InfRcWorker *w,
                                 Infiniband::QueuePair *qp, const entity_addr_t& addr, int type)
  : Connection(c, m), qp(qp), worker(w), center(&w->center),
    cm_lock("InfRcConnection::cm_lock"), nonce(0), state(STATE_NEW), client_setup_socket(-1),
    exchange_count(0), infrc_msgr(m)
{
  set_peer_type(type);
  set_peer_addr(addr);
  policy = m->get_policy(type);

  read_handler.reset(new C_infrc_read(this));
  reset_handler.reset(new C_infrc_handle_reset(infrc_msgr, this));
  remote_reset_handler.reset(new C_infrc_handle_remote_reset(infrc_msgr, this));
  local_deliver_handler.reset(new C_infrc_local_deliver(this));
}

InfRcConnection::~InfRcConnection()
{
  assert(client_setup_socket == -1);
  assert(!qp);
  assert(pending_send.empty());
  assert(register_time_events.empty());
}

void InfRcConnection::connect()
{
  ldout(infrc_msgr->cct, 10) << __func__ << dendl;

  state = STATE_BEFORE_CONNECTING;
  // rescheduler connection in order to avoid lock dep
  // may called by external thread(send_message)
  center->dispatch_event_external(read_handler);
}

void InfRcConnection::process()
{
  int r = 0;
  int prev_state;
  Mutex::Locker l(cm_lock);
  do {
    prev_state = state;
    ldout(infrc_msgr->cct, 20) << __func__ << " state is " << get_state_name(state)
                               << ", prev state is " << get_state_name(prev_state) << dendl;
    switch (state) {
      case STATE_BEFORE_CONNECTING:
      {
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

        r = ::connect(client_setup_socket, (sockaddr*)(&get_peer_addr().addr),
                      get_peer_addr().addr_size());
        if (r < 0) {
          lderr(infrc_msgr->cct) << __func__ << " failed to connect " << get_peer_addr() << ": "
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

        if (exchange_count++ < QP_EXCHANGE_MAX_TIMEOUTS) {
          r = get_random_bytes((char*)&nonce, sizeof(nonce));
          if (r)
            nonce += 1000000;
          Infiniband::QueuePairTuple outgoing_qpt(static_cast<uint16_t>(worker->get_lid()),
                                                  qp->get_local_qp_number(),
                                                  qp->get_initial_psn(), nonce, infrc_msgr->get_myinst().name.type(),
                                                  infrc_msgr->get_myaddr(),
                                                  get_peer_addr());
          ldout(infrc_msgr->cct, 20) << __func__ << " sending qpt=" << outgoing_qpt << dendl;
          r = ::send(client_setup_socket, &outgoing_qpt, sizeof(outgoing_qpt), 0);
          if (r != sizeof(outgoing_qpt)) {
            if (r < 0) {
              if (errno != EINTR && errno != EAGAIN) {
                lderr(infrc_msgr->cct) << __func__ << " sendto returned error " << errno << ": "
                                       << cpp_strerror(errno) << dendl;
              }
            } else {
              lderr(infrc_msgr->cct) << __func__ << " sendto returned bad length (" << r
                                     << ") " << dendl;
            }
          } else {
            // Successfully sent
            // FIXME: maybe timeout
            state = STATE_CONNECTING;
            center->create_file_event(client_setup_socket, EVENT_READABLE, read_handler);
          }
        } else {
          lderr(infrc_msgr->cct) << __func__ << " failed to exchange with server ("
                                 << get_peer_addr().addr << ") within sent request "
                                 << QP_EXCHANGE_MAX_TIMEOUTS << " times" << dendl;
          goto fail;
        }
        break;
      }

      case STATE_CONNECTING:
      {
        sockaddr_in sin;
        Infiniband::QueuePairTuple incoming_qpt;
        ssize_t len = ::recv(client_setup_socket, &incoming_qpt,
                             sizeof(incoming_qpt), 0);
        if (len == -1) {
          if (errno == EINTR || errno == EAGAIN) {
            break;
          }
          lderr(infrc_msgr->cct) << __func__ << " recv returned error " << errno << ": "
                                 << cpp_strerror(errno) << dendl;
        } else if (len != sizeof(incoming_qpt)) {
          lderr(infrc_msgr->cct) << __func__ << " recvfrom returned bad length (" << len
                                 << ") while sending to ip: [" << inet_ntoa(sin.sin_addr)
                                 << "] port: [" << htons(sin.sin_port) << "]" << dendl;
        } else {
          ldout(infrc_msgr->cct, 20) << __func__ << " state=" << get_state_name(state)
                                     << " receiving qpt=" << incoming_qpt << dendl;
          if (nonce == incoming_qpt.get_nonce()) {
            // plumb up our queue pair with the server's parameters.
            r = qp->plumb(&incoming_qpt);
            if (r == 0) {
              infrc_msgr->learned_addr(incoming_qpt.get_receiver_addr());
              if (infrc_msgr->accept_conn(this)) {
                ldout(infrc_msgr->cct, 1) << __func__ << " accept conn("
                                          << incoming_qpt.get_receiver_addr()
                                          << ") racing, mark me down" << dendl;
                _stop();
                break;
              }
              center->delete_file_event(client_setup_socket, EVENT_READABLE);
              ::close(client_setup_socket);
              client_setup_socket = -1;
              center->dispatch_event_external(
                  EventCallbackRef(new C_infrc_deliver_connect(infrc_msgr, this)));
              infrc_msgr->ms_deliver_handle_fast_connect(this);
              state = STATE_OPEN;
              exchange_count = 0;
              break;
            }
          } else {
            lderr(infrc_msgr->cct) << __func__ << " received nonce doesn't match "
                                   << nonce << " != " << incoming_qpt.get_nonce() << dendl;
          }
        }

        state = STATE_BEFORE_CONNECTING;
        break;
      }

      case STATE_OPEN:
      {
        ldout(infrc_msgr->cct, 10) << __func__ << " ready to send/redeive to "
                                   << get_peer_addr() << dendl;
        while (!pending_send.empty()) {
          Message *m = pending_send.front();
          pending_send.pop_front();
          ldout(infrc_msgr->cct, 10) << __func__ << " submit pending message " << *m << dendl;
          worker->submit_message(m, qp);
        }
        break;
      }

      case STATE_CLOSED:
      {
        ldout(infrc_msgr->cct, 10) << __func__ << " already closed "
                                   << get_peer_addr() << dendl;
        break;
      }
    }
    continue;

fail:
    fault();
  } while (prev_state != state);
}

int InfRcConnection::send_message(Message *m)
{
  ldout(infrc_msgr->cct, 10) << __func__ << " m=" << m << dendl;
  m->get_header().src = infrc_msgr->get_myname();
  if (!m->get_priority())
    m->set_priority(msgr->get_default_send_priority());
  m->set_connection(this);

  uint64_t features = get_features();
  if (m->empty_payload())
    ldout(infrc_msgr->cct, 20) << __func__ << " encoding " << " features " << features
                               << " " << m << " " << *m << dendl;
  else
    ldout(infrc_msgr->cct, 20) << __func__ << " half-reencoding " << " features "
                               << features << " " << m << " " << *m << dendl;

  // encode and copy out of *m
  m->encode(features, !infrc_msgr->cct->_conf->ms_nocrc);
  cm_lock.Lock();
  if (state == STATE_OPEN && pending_send.empty()) {
    worker->submit_message(m, qp);
  } else if (state == STATE_CLOSED) {
    ldout(infrc_msgr->cct, 1) << __func__ << " connection already stopped: " << *m << dendl;
    m->put();
    cm_lock.Unlock();
    return -1;
  } else if (infrc_msgr->get_myaddr() == get_peer_addr()) {
    ldout(infrc_msgr->cct, 20) << __func__ << " local dispatch " << *m << dendl;
    local_messages.push_back(m);
    center->dispatch_event_external(local_deliver_handler);
  } else {
    ldout(infrc_msgr->cct, 20) << __func__ << " pending " << *m << " wait for send." << dendl;
    pending_send.push_back(m);
    center->dispatch_event_external(read_handler);
  }
  cm_lock.Unlock();

  return 0;
}

void InfRcConnection::_stop()
{
  ldout(infrc_msgr->cct, 10) << __func__ << " qp=" << qp << dendl;
  assert(cm_lock.is_locked());
  if (state == STATE_CLOSED)
    return ;

  infrc_msgr->unregister_conn(this);
  state = STATE_CLOSED;
  if (qp) {
    worker->ready_dead(qp);
    qp = NULL;
  }
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

void InfRcConnection::fault()
{
  assert(cm_lock.is_locked());
  if (state == STATE_CLOSED) {
    ldout(infrc_msgr->cct, 10) << __func__ << " state is already STATE_CLOSED" << dendl;
    center->dispatch_event_external(reset_handler);
    return ;
  }

  if (policy.lossy && state != STATE_BEFORE_CONNECTING) {
    ldout(infrc_msgr->cct, 10) << __func__ << " on lossy channel, failing" << dendl;
    center->dispatch_event_external(reset_handler);
    _stop();
    return ;
  }

  if (client_setup_socket >= 0) {
    center->delete_file_event(client_setup_socket, EVENT_READABLE);
    ::close(client_setup_socket);
    client_setup_socket = -1;
  }

  // FIXME: requeue sent items
  // requeue_sent();
  if (policy.standby && !pending_send.empty()) {
    ldout(infrc_msgr->cct, 0) << __func__ << " with nothing to send, going to standby" << dendl;
    state = STATE_STANDBY;
    return;
  }

  if (state != STATE_CONNECTING && state != STATE_BEFORE_CONNECTING) {
    // policy maybe empty when state is in accept
    if (policy.server || state == STATE_ACCEPTING) {
      ldout(infrc_msgr->cct, 0) << __func__ << " server, going to standby" << dendl;
      state = STATE_STANDBY;
    } else {
      ldout(infrc_msgr->cct, 0) << __func__ << " initiating reconnect" << dendl;
      //connect_seq++;
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
  register_time_events.insert(center->create_time_event(
          backoff.to_nsec()/1000, EventCallbackRef(new C_infrc_wakeup(this))));
}

void InfRcConnection::wakeup_from(uint64_t id)
{
  cm_lock.Lock();
  register_time_events.erase(id);
  cm_lock.Unlock();
  process();
}

int InfRcConnection::ready(Infiniband::QueuePairTuple &incoming_qpt)
{
  ldout(infrc_msgr->cct, 10) << __func__ << " qpt=" << incoming_qpt << dendl;
  Mutex::Locker l(cm_lock);
  assert(state == STATE_NEW);
  int r = qp->plumb(&incoming_qpt);
  if (r == 0) {
    center->dispatch_event_external(
        EventCallbackRef(new C_infrc_deliver_accept(infrc_msgr, this)));
    infrc_msgr->ms_deliver_handle_fast_accept(this);
    state = STATE_OPEN;
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

Infiniband::QueuePairTuple InfRcConnection::build_qp_tuple(uint64_t nonce) 
{
  Infiniband::QueuePairTuple t(worker->get_lid(), qp->get_local_qp_number(), qp->get_initial_psn(),
                               nonce, infrc_msgr->get_myinst().name.type(), infrc_msgr->get_myaddr(), get_peer_addr());
  return t;
}
