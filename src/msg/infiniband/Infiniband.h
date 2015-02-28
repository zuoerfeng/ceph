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

#ifndef CEPH_INFINIBAND_H
#define CEPH_INFINIBAND_H

#include <arpa/inet.h>
#include <netinet/in.h>
#include <infiniband/verbs.h>

#include "include/int_types.h"
#include "common/debug.h"
#include "common/errno.h"
#include "msg/msg_types.h"

/**
 * A collection of Infiniband helper functions and classes, which can be shared
 * across different Infiniband transports and drivers.
 */
class Infiniband {
 public:
  static const char* wc_status_to_string(int status);

  // this class exists simply for passing queue pair handshake information
  // back and forth.
  class QueuePairTuple {
   public:
    QueuePairTuple() : qpn(0), psn(0), features(0), lid(0), tag(0), host_type(0),
                       global_seq(0), connect_seq(0), msg_seq(0) {
      assert(sizeof(QueuePairTuple) == 36+2*sizeof(ceph_entity_addr));
    }
    QueuePairTuple(uint16_t lid, uint32_t qpn, uint32_t psn, uint64_t f,
                   uint8_t tag, uint8_t t, uint32_t gseq, uint32_t cseq, uint64_t mseq,
                   const entity_addr_t &s, const entity_addr_t &r)
      : qpn(qpn), psn(psn), features(f), lid(lid), tag(tag), host_type(t),
        global_seq(gseq), connect_seq(cseq), msg_seq(mseq) {
      set_sender_addr(s);
      set_recevier_addr(r);
    }
    __le16 get_lid() const { return lid; }
    __le32 get_qpn() const { return qpn; }
    __le32 get_psn() const { return psn; }
    __le32 get_global_seq() const { return global_seq; }
    __le32 get_connect_seq() const { return connect_seq; }
    __le64 get_msg_seq() const { return msg_seq; }
    __u8     get_type() const { return host_type; }
    __u8     get_tag() const { return tag; }
    void     set_tag(uint8_t t) { tag = t; }
    __le64   get_features() const { return features; }
    void     set_features(uint64_t f) { features = f; }
    void set_global_seq(uint32_t s) { global_seq = s; }
    void set_connect_seq(uint32_t s) { connect_seq = s; }
    void set_msg_seq(uint64_t s) { msg_seq = s; }
    const entity_addr_t get_sender_addr() const { return entity_addr_t(addr); }
    const entity_addr_t get_receiver_addr() const { return entity_addr_t(peer_addr); }
    void set_sender_addr(const entity_addr_t &r) {
      addr = ceph_entity_addr(r);
    }
    void set_recevier_addr(const entity_addr_t &r) {
      peer_addr = ceph_entity_addr(r);
    }

   private:
    __le32   qpn;            // queue pair number
    __le32   psn;            // initial packet sequence number
    __le64   features;
    __le16   lid;            // infiniband address: "local id"
    __u8     tag;
    __u8     host_type;
    __le32   global_seq;     /* count connections initiated by this host */
    __le32   connect_seq;
    __le64   msg_seq;
    // for received requests
    ceph_entity_addr addr;      // address for the sender
    ceph_entity_addr peer_addr;      // address for the receiver
  } __attribute__((packed));

  /**
   * Construct an Infiniband object.
   * \param[in] deviceName
   *      The string name of the installed interface to look for.
   *      If NULL, open the first one returned by the Verbs library.
   */
  explicit Infiniband(CephContext *cct, const char* device_name)
  : cct(cct) {
    device = new Device(device_name);
    pd = new ProtectionDomain(device);
    assert(set_nonblocking(device->ctxt->async_fd) == 0);
  }

  /**
   * Destroy an Infiniband object.
   */
  ~Infiniband() {
    delete pd;
    delete device;
  }

  class DeviceList {
   public:
    DeviceList(): devices(ibv_get_device_list(&num)) {
      if (devices == NULL || num == 0) {
       derr << __func__ << "Could not get infiniband device list: "
            << cpp_strerror(errno)<< dendl;
       assert(0);
      }
    }
    ~DeviceList() {
      ibv_free_device_list(devices);
    }
    ibv_device* lookup(const char* name) {
      if (name == NULL)
        return devices[0];
      for (int i = 0; devices[i] != NULL; i++) {
        if (strcmp(devices[i]->name, name) == 0)
          return devices[i];
      }
      return NULL;
    }
   private:
    ibv_device** const devices;
    int num;
  };

  class Device {
   public:
    explicit Device(const char* name): ctxt(NULL) {
      // The lifetime of the device list needs to extend
      // through the call to ibv_open_device.
      DeviceList device_list;

      ibv_device* dev = device_list.lookup(name);
      if (dev == NULL) {
        derr << __func__ << " failed to find infiniband device: "
             << name << " " << cpp_strerror(errno) << dendl;
        assert(0);
      }

      ctxt = ibv_open_device(dev);
      if (ctxt == NULL) {
        derr << __func__ << " failed to open infiniband device: "
             << name << cpp_strerror(errno) << dendl;
        assert(0);
      }
    }

    ~Device() {
      int rc = ibv_close_device(ctxt);
      if (rc != 0)
        derr << __func__ << " ibv_close_device failed: "
             << cpp_strerror(errno) << dendl;
    }

    ibv_context* ctxt; // const after construction
  };

  class ProtectionDomain {
   public:
    explicit ProtectionDomain(Device *device)
      : pd(ibv_alloc_pd(device->ctxt))
    {
      if (pd == NULL) {
        derr << __func__ << " failed to allocate infiniband protection domain: "
             << cpp_strerror(errno) << dendl;
        assert(0);
      }
    }
    ~ProtectionDomain() {
      int rc = ibv_dealloc_pd(pd);
      if (rc != 0) {
        derr << __func__ << " ibv_dealloc_pd failed: "
             << cpp_strerror(errno) << dendl;
      }
    }
    ibv_pd* const pd;
  };

  // this class encapsulates the creation, use, and destruction of an RC
  // queue pair.
  //
  // you need call init and it will create a qp and bring it to the INIT state.
  // after obtaining the lid, qpn, and psn of a remote queue pair, one
  // must call plumb() to bring the queue pair to the RTS state.
  class QueuePair {
   public:
    QueuePair(Infiniband& infiniband, ibv_qp_type type, int ib_physical_port,
              ibv_srq *srq, ibv_cq *txcq, ibv_cq *rxcq, uint32_t max_send_wr,
              uint32_t max_recv_wr, uint32_t q_key = 0);
    // exists solely as superclass constructor for MockQueuePair derivative
    explicit QueuePair(Infiniband& infiniband):
      infiniband(infiniband), type(IBV_QPT_RC), ctxt(NULL), ib_physical_port(-1),
      pd(NULL), srq(NULL), qp(NULL), txcq(NULL), rxcq(NULL),
      initial_psn(-1), dead(false) {}
    ~QueuePair();

    int init();

    /**
    * Get the initial packet sequence number for this QueuePair.
    * This is randomly generated on creation. It should not be confused
    * with the remote side's PSN, which is set in #plumb(). 
    */
    uint32_t get_initial_psn() const { return initial_psn; };
    /**
    * Get the local queue pair number for this QueuePair.
    * QPNs are analogous to UDP/TCP port numbers.
    */
    uint32_t get_local_qp_number() const { return qp->qp_num; };
    /**
    * Get the remote queue pair number for this QueuePair, as set in #plumb().
    * QPNs are analogous to UDP/TCP port numbers.
    */
    int get_remote_qp_number(uint32_t *rqp) const {
      ibv_qp_attr qpa;
      ibv_qp_init_attr qpia;

      int r = ibv_query_qp(qp, &qpa, IBV_QP_DEST_QPN, &qpia);
      if (r) {
          derr << __func__ << " failed to query qp: "
               << cpp_strerror(errno) << dendl;
          return -1;
      }

      if (rqp)
        *rqp = qpa.dest_qp_num;
      return 0;
    }
    /**
    * Get the remote infiniband address for this QueuePair, as set in #plumb().
    * LIDs are "local IDs" in infiniband terminology. They are short, locally
    * routable addresses.
    */
    int get_remote_lid(uint16_t *lid) const {
      ibv_qp_attr qpa;
      ibv_qp_init_attr qpia;

      int r = ibv_query_qp(qp, &qpa, IBV_QP_AV, &qpia);
      if (r) {
          derr << __func__ << " failed to query qp: "
               << cpp_strerror(errno) << dendl;
          return -1;
      }

      if (lid)
        *lid = qpa.ah_attr.dlid;
      return 0;
    }
    /**
    * Get the state of a QueuePair.
    */
    int get_state() const {
      ibv_qp_attr qpa;
      ibv_qp_init_attr qpia;

      int r = ibv_query_qp(qp, &qpa, IBV_QP_STATE, &qpia);
      if (r) {
          derr << __func__ << " failed to get state: "
               << cpp_strerror(errno) << dendl;
          return -1;
      }
      return qpa.qp_state;
    }
    /**
    * Return true if the queue pair is in an error state, false otherwise.
    */
    bool is_error() const {
      ibv_qp_attr qpa;
      ibv_qp_init_attr qpia;

      int r = ibv_query_qp(qp, &qpa, -1, &qpia);
      if (r) {
          derr << __func__ << " failed to get state: "
               << cpp_strerror(errno) << dendl;
          return true;
      }
      return qpa.cur_qp_state == IBV_QPS_ERR;
    }
    ibv_qp* get_qp() const { return qp; }
    int plumb(QueuePairTuple *qpt);
    int to_reset();
    int to_dead();
    bool is_dead() { return dead; }

   private:
    Infiniband&  infiniband;     // Infiniband to which this QP belongs
    ibv_qp_type  type;           // QP type (IBV_QPT_RC, etc.)
    ibv_context* ctxt;           // device context of the HCA to use
    int          ib_physical_port; // physical port number of the HCA
    ibv_pd*      pd;             // protection domain
    ibv_srq*     srq;            // shared receive queue
    ibv_qp*      qp;             // infiniband verbs QP handle
    ibv_cq*      txcq;           // transmit completion queue
    ibv_cq*      rxcq;           // receive completion queue
    uint32_t     initial_psn;    // initial packet sequence number
    uint32_t     max_send_wr;
    uint32_t     max_recv_wr;
    uint32_t     q_key;
    bool         dead;
  };

  class CompletionChannel {
   public:
    CompletionChannel(Infiniband &ib): infiniband(ib), channel(NULL), cq_events_that_need_ack(0) {}
    ~CompletionChannel();
    int init();
    bool get_cq_event();
    int get_fd() { return channel->fd; }
    ibv_comp_channel* get_channel() { return channel; }

   private:
    static const uint32_t MAX_ACK_EVENT = 128;
    Infiniband& infiniband;
    ibv_comp_channel *channel;
    uint32_t      cq_events_that_need_ack;
  };
  // this class encapsulates the creation, use, and destruction of an RC
  // completion queue.
  //
  // You need to call init and it will create a cq and associate to comp channel
  class CompletionQueue {
   public:
    CompletionQueue(Infiniband &ib, const uint32_t qd, CompletionChannel *cc):
      infiniband(ib), channel(cc), cq(NULL), queue_depth(qd) {}
    ~CompletionQueue();
    int init();
    int poll_completion_queue(int num_entries, ibv_wc *ret_wc_array);

    ibv_cq* get_cq() const { return cq; }
    int rearm_notify(bool solicited_only=true);

   private:
    Infiniband&  infiniband;     // Infiniband to which this QP belongs
    CompletionChannel *channel;
    ibv_cq           *cq;
    uint32_t      queue_depth;
  };

  // wrap an RX or TX buffer registered with the HCA
  // Possible memory management issues with mr member?
  struct BufferDescriptor {
    char *          buffer;         // buf of ``bytes'' length
    uint32_t        bytes;          // length of buffer in bytes
    uint32_t        messageBytes;   // byte length of message in the buffer
    ibv_mr *        mr;             // memory region of the buffer

    BufferDescriptor(char *buffer, uint32_t bytes, ibv_mr *mr)
      : buffer(buffer), bytes(bytes), messageBytes(0), mr(mr) {}
    BufferDescriptor()
      : buffer(NULL), bytes(0), messageBytes(0), mr(NULL) {}
  };

  /**
   * A region of memory registered with the HCA for DMA, split
   * into fixed-size buffers with easy-to-access BufferDescriptors
   * for each of them.
   *
   * RegisteredBuffers's purpose is twofold:
   *  1) It performs a single allocation and registration of a large set
   *     of buffers -- operations that have high per-call overheads.
   *  2) It provides a mapping from pointers into the buffers back to
   *     the BufferDescriptor which contains the details of the buffer.
   *     This is important, for example, in InfUdDriver::release where a
   *     pointer into the packet buffer is given to the driver which must then
   *     reclaim the buffer (see get_descriptor()).
   */
  class RegisteredBuffers {
   public:
    /**
     * Allocate BufferDescriptors and register the backing memory with the
     * HCA. Note that the memory will be wired (i.e. cannot be swapped out)!
     *
     * \param pd
     *      The ProtectionDomain this registered memory should be a part of.
     * \param buffer_size
     *      Size in bytes of each of the registered buffers this
     *      allocator manages.
     * \param buffer_count
     *      The number of registered buffers of #buffer_size to allocate.
     * \throw
     *      TransportException if allocation or registration failed.
     */
    RegisteredBuffers(ProtectionDomain *pd, const uint32_t bsize,
                      const uint32_t bcount)
      : buffer_size(bsize), buffer_count(bcount), base_pointer(NULL),
        descriptors(NULL) {
      const size_t bytes = buffer_size * buffer_count;
      int r = posix_memalign(&base_pointer, 4096, bytes);
      if (r) {
        derr << __func__ << " failed to allocate " << bytes
             << " bytes of memory: " << cpp_strerror(r) << dendl;
        assert(0);
      }

      ibv_mr *mr = ibv_reg_mr(pd->pd, base_pointer, bytes,
                              IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_LOCAL_WRITE);
      if (mr == NULL) {
        derr << __func__ << " failed to register buffer" << dendl;
        assert(0);
      }

      descriptors = new BufferDescriptor[buffer_count];
      char* buffer = static_cast<char*>(base_pointer);
      for (uint32_t i = 0; i < buffer_count; ++i) {
        descriptors[i].~BufferDescriptor();
        new(&descriptors[i]) BufferDescriptor(buffer, buffer_size, mr);
        buffer += buffer_size;
      }
    }

    ~RegisteredBuffers() { free(base_pointer); delete descriptors; }

    /**
     * Get the BufferDescriptor associated with the buffer that #buffer
     * points into.
     *
     * \param buffer
     *       A pointer into a buffer allocated from this RegisteredBuffers.
     *       If #buffer does not point into a buffer from this
     *       RegisteredBuffers the result is undefined.
     * \return
     *      The BufferDescriptor for the buffer which #buffer points into.
     */
    BufferDescriptor* get_descriptor(const void* buffer) const {
      size_t descriptor_index = (static_cast<const char*>(buffer) -
          static_cast<char*>(base_pointer)) / buffer_size;
      return &descriptors[descriptor_index];
    }

    /**
     * Returns an iterator to the start of the BufferDescriptors in
     * this RegisteredBuffers.
     */
    BufferDescriptor* begin() { return descriptors; }

    /**
     * Returns an iterator to the end of the BufferDescriptors in
     * this RegisteredBuffers.
     */
    BufferDescriptor* end() { return descriptors + buffer_count; }

   private:
    /// The size in bytes of each of the buffers.
    const uint32_t buffer_size;

    /// The count of buffers.
    const uint32_t buffer_count;

    /**
     * Points to the start of the first registered buffer.
     * Buffers are contiguous in memory, which means given a pointer
     * to a registered buffer its index in the consective sequence of
     * buffers is (ptr - base_pointer) / buffer_size.
     */
    void* base_pointer;

    /// BufferDescriptors for each of the buffers.
    BufferDescriptor* descriptors;
  };

  QueuePair* create_queue_pair(ibv_qp_type type, int ib_physical_port,
                               ibv_srq *srq, ibv_cq *txcq, ibv_cq *rxcq,
                               uint32_t max_send_wr, uint32_t max_recv_wr,
                               uint32_t q_key = 0);
  CompletionChannel *create_comp_channel();
  CompletionQueue *create_comp_queue(const uint32_t qd,
                                     CompletionChannel *cc=NULL);

  int get_lid(int port);
  int get_async_fd() { return device->ctxt->async_fd; }

  int post_srq_receive(ibv_srq* srq, BufferDescriptor* bd);

  ibv_srq* create_shared_receive_queue(uint32_t max_wr, uint32_t max_sge);
  int destroy_shared_receive_queue(ibv_srq *srq);

  int set_nonblocking(int fd);

  //  Keep public for 0-copy hack. nothing to see here, move along.
  CephContext *cct;
  Device *device;
  ProtectionDomain *pd;
  static const uint32_t MAX_INLINE_DATA = 400;
};

inline ostream& operator<<(ostream& out, const Infiniband::QueuePairTuple &qpt)
{
  return out << " lid=" << qpt.get_lid() << " qpn=" << qpt.get_qpn()
             << " psn=" << qpt.get_psn() << " features=" << qpt.get_features()
             << " type=" << qpt.get_type() << " gseq=" << qpt.get_global_seq()
             << " cseq=" << qpt.get_connect_seq() << " mseq=" << qpt.get_msg_seq();
}


#endif // CEPH_INFINIBAND_H
