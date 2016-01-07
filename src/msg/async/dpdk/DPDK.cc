// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
/*
 * This file is open source software, licensed to you under the terms
 * of the Apache License, Version 2.0 (the "License").  See the NOTICE file
 * distributed with this work for additional information regarding copyright
 * ownership.  You may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
/*
 * Copyright (C) 2014 Cloudius Systems, Ltd.
 */
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2015 XSky <haomai@xsky.com>
 *
 * Author: Haomai Wang <haomaiwang@gmail.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include <cstddef>

#include <atomic>
#include <vector>
#include <queue>

#include <rte_config.h>
#include <rte_common.h>
#include <rte_eal.h>
#include <rte_pci.h>
#include <rte_ethdev.h>
#include <rte_cycles.h>
#include <rte_memzone.h>

#include "IP.h"
#include "const.h"
#include "dpdk_rte.h"
#include "DPDK.h"
#include "toeplitz.h"

#define dout_subsys ceph_subsys_dpdk

#if RTE_VERSION <= RTE_VERSION_NUM(2,0,0,16)

static inline char* rte_mbuf_to_baddr(rte_mbuf* mbuf) {
  return reinterpret_cast<char*>(RTE_MBUF_TO_BADDR(mbuf));
}

void* as_cookie(struct rte_pktmbuf_pool_private& p) {
  return reinterpret_cast<void*>(uint64_t(p.mbuf_data_room_size));
};

#else

void* as_cookie(struct rte_pktmbuf_pool_private& p) {
  return &p;
};

#endif

#ifndef MARKER
typedef void    *MARKER[0];   /**< generic marker for a point in a structure */
#endif

/******************* Net device related constatns *****************************/
static constexpr uint16_t default_ring_size      = 512;

// 
// We need 2 times the ring size of buffers because of the way PMDs 
// refill the ring.
//
static constexpr uint16_t mbufs_per_queue_rx     = 2 * default_ring_size;
static constexpr uint16_t rx_gc_thresh           = 64;

//
// No need to keep more descriptors in the air than can be sent in a single
// rte_eth_tx_burst() call.
//
static constexpr uint16_t mbufs_per_queue_tx     = 2 * default_ring_size;

static constexpr uint16_t mbuf_cache_size        = 512;
static constexpr uint16_t mbuf_overhead          =
sizeof(struct rte_mbuf) + RTE_PKTMBUF_HEADROOM;
//
// We'll allocate 2K data buffers for an inline case because this would require
// a single page per mbuf. If we used 4K data buffers here it would require 2
// pages for a single buffer (due to "mbuf_overhead") and this is a much more
// demanding memory constraint.
//
static constexpr size_t inline_mbuf_data_size = 2048;

//
// Size of the data buffer in the non-inline case.
//
// We may want to change (increase) this value in future, while the
// inline_mbuf_data_size value will unlikely change due to reasons described
// above.
//
static constexpr size_t mbuf_data_size = 2048;

// (INLINE_MBUF_DATA_SIZE(2K)*32 = 64K = Max TSO/LRO size) + 1 mbuf for headers
static constexpr uint8_t max_frags = 32 + 1;

//
// Intel's 40G NIC HW limit for a number of fragments in an xmit segment.
//
// See Chapter 8.4.1 "Transmit Packet in System Memory" of the xl710 devices
// spec. for more details.
//
static constexpr uint8_t i40e_max_xmit_segment_frags = 8;

static constexpr uint16_t inline_mbuf_size = inline_mbuf_data_size + mbuf_overhead;

uint32_t qp_mempool_obj_size(bool hugetlbfs_membackend)
{
  uint32_t mp_size = 0;
  struct rte_mempool_objsz mp_obj_sz = {};

  //
  // We will align each size to huge page size because DPDK allocates
  // physically contiguous memory region for each pool object.
  //

  // Rx
  if (hugetlbfs_membackend) {
    mp_size +=
        align_up(rte_mempool_calc_obj_size(mbuf_overhead, 0, &mp_obj_sz)+
                 sizeof(struct rte_pktmbuf_pool_private),
                 memory::huge_page_size);
  } else {
    mp_size +=
        align_up(rte_mempool_calc_obj_size(inline_mbuf_size, 0, &mp_obj_sz)+
                 sizeof(struct rte_pktmbuf_pool_private),
                 memory::huge_page_size);
  }
  //Tx
  std::memset(&mp_obj_sz, 0, sizeof(mp_obj_sz));
  mp_size += align_up(rte_mempool_calc_obj_size(inline_mbuf_size, 0,
                                                &mp_obj_sz)+
                      sizeof(struct rte_pktmbuf_pool_private),
                      memory::huge_page_size);
  return mp_size;
}

static constexpr const char* pktmbuf_pool_name   = "dpdk_net_pktmbuf_pool";

/*
 * When doing reads from the NIC queues, use this batch size
 */
static constexpr uint8_t packet_read_size        = 32;
/******************************************************************************/

int DPDKDevice::init_port_start()
{
  assert(_port_idx < rte_eth_dev_count());

  rte_eth_dev_info_get(_port_idx, &_dev_info);

  //
  // This is a workaround for a missing handling of a HW limitation in the
  // DPDK i40e driver. This and all related to _is_i40e_device code should be
  // removed once this handling is added.
  //
  if (std::string("rte_i40evf_pmd") == _dev_info.driver_name ||
      std::string("rte_i40e_pmd") == _dev_info.driver_name) {
    printf("Device is an Intel's 40G NIC. Enabling 8 fragments hack!\n");
    _is_i40e_device = true;
  }

  //
  // Another workaround: this time for a lack of number of RSS bits.
  // ixgbe PF NICs support up to 16 RSS queues.
  // ixgbe VF NICs support up to 4 RSS queues.
  // i40e PF NICs support up to 64 RSS queues.
  // i40e VF NICs support up to 16 RSS queues.
  //
  if (std::string("rte_ixgbe_pmd") == _dev_info.driver_name) {
    _dev_info.max_rx_queues = std::min(_dev_info.max_rx_queues, (uint16_t)16);
  } else if (std::string("rte_ixgbevf_pmd") == _dev_info.driver_name) {
    _dev_info.max_rx_queues = std::min(_dev_info.max_rx_queues, (uint16_t)4);
  } else if (std::string("rte_i40e_pmd") == _dev_info.driver_name) {
    _dev_info.max_rx_queues = std::min(_dev_info.max_rx_queues, (uint16_t)64);
  } else if (std::string("rte_i40evf_pmd") == _dev_info.driver_name) {
    _dev_info.max_rx_queues = std::min(_dev_info.max_rx_queues, (uint16_t)16);
  }

  // Clear txq_flags - we want to support all available offload features
  // except for multi-mempool and refcnt'ing which we don't need
  _dev_info.default_txconf.txq_flags =
      ETH_TXQ_FLAGS_NOMULTMEMP | ETH_TXQ_FLAGS_NOREFCOUNT;

  //
  // Disable features that are not supported by port's HW
  //
  if (!(_dev_info.tx_offload_capa & DEV_TX_OFFLOAD_UDP_CKSUM)) {
    _dev_info.default_txconf.txq_flags |= ETH_TXQ_FLAGS_NOXSUMUDP;
  }

  if (!(_dev_info.tx_offload_capa & DEV_TX_OFFLOAD_TCP_CKSUM)) {
    _dev_info.default_txconf.txq_flags |= ETH_TXQ_FLAGS_NOXSUMTCP;
  }

  if (!(_dev_info.tx_offload_capa & DEV_TX_OFFLOAD_SCTP_CKSUM)) {
    _dev_info.default_txconf.txq_flags |= ETH_TXQ_FLAGS_NOXSUMSCTP;
  }

  if (!(_dev_info.tx_offload_capa & DEV_TX_OFFLOAD_VLAN_INSERT)) {
    _dev_info.default_txconf.txq_flags |= ETH_TXQ_FLAGS_NOVLANOFFL;
  }

  if (!(_dev_info.tx_offload_capa & DEV_TX_OFFLOAD_VLAN_INSERT)) {
    _dev_info.default_txconf.txq_flags |= ETH_TXQ_FLAGS_NOVLANOFFL;
  }

  if (!(_dev_info.tx_offload_capa & DEV_TX_OFFLOAD_TCP_TSO) &&
      !(_dev_info.tx_offload_capa & DEV_TX_OFFLOAD_UDP_TSO)) {
    _dev_info.default_txconf.txq_flags |= ETH_TXQ_FLAGS_NOMULTSEGS;
  }

  /* for port configuration all features are off by default */
  rte_eth_conf port_conf = { 0 };

  printf("Port %d: max_rx_queues %d max_tx_queues %d\n",
         _port_idx, _dev_info.max_rx_queues, _dev_info.max_tx_queues);

  _num_queues = std::min({_num_queues, _dev_info.max_rx_queues, _dev_info.max_tx_queues});

  printf("Port %d: using %d %s\n", _port_idx, _num_queues,
         (_num_queues > 1) ? "queues" : "queue");

  // Set RSS mode: enable RSS if seastar is configured with more than 1 CPU.
  // Even if port has a single queue we still want the RSS feature to be
  // available in order to make HW calculate RSS hash for us.
  if (cores > 1) {
    if (_dev_info.hash_key_size == 40) {
      _rss_key = default_rsskey_40bytes;
    } else if (_dev_info.hash_key_size == 52) {
      _rss_key = default_rsskey_52bytes;
    } else if (_dev_info.hash_key_size != 0) {
      // WTF?!!
      rte_exit(EXIT_FAILURE,
               "Port %d: We support only 40 or 52 bytes RSS hash keys, %d bytes key requested",
               _port_idx, _dev_info.hash_key_size);
    }

    port_conf.rxmode.mq_mode = ETH_MQ_RX_RSS;
    port_conf.rx_adv_conf.rss_conf.rss_hf = ETH_RSS_PROTO_MASK;
    if (_dev_info.hash_key_size) {
      port_conf.rx_adv_conf.rss_conf.rss_key = const_cast<uint8_t *>(_rss_key.data());
      port_conf.rx_adv_conf.rss_conf.rss_key_len = _dev_info.hash_key_size;
    }
  } else {
    port_conf.rxmode.mq_mode = ETH_MQ_RX_NONE;
  }

  if (_num_queues > 1) {
    if (_dev_info.reta_size) {
      // RETA size should be a power of 2
      assert((_dev_info.reta_size & (_dev_info.reta_size - 1)) == 0);

      // Set the RSS table to the correct size
      _redir_table.resize(_dev_info.reta_size);
      _rss_table_bits = std::lround(std::log2(_dev_info.reta_size));
      printf("Port %d: RSS table size is %d\n",
             _port_idx, _dev_info.reta_size);
    } else {
      _rss_table_bits = std::lround(std::log2(_dev_info.max_rx_queues));
    }
  } else {
    _redir_table.push_back(0);
  }

  // Set Rx VLAN stripping
  if (_dev_info.rx_offload_capa & DEV_RX_OFFLOAD_VLAN_STRIP) {
    port_conf.rxmode.hw_vlan_strip = 1;
  }

  // Enable HW CRC stripping
  port_conf.rxmode.hw_strip_crc = 1;

#ifdef RTE_ETHDEV_HAS_LRO_SUPPORT
  // Enable LRO
  if (_use_lro && (_dev_info.rx_offload_capa & DEV_RX_OFFLOAD_TCP_LRO)) {
    printf("LRO is on\n");
    port_conf.rxmode.enable_lro = 1;
    _hw_features.rx_lro = true;
  } else
#endif
    printf("LRO is off\n");

  // Check that all CSUM features are either all set all together or not set
  // all together. If this assumption breaks we need to rework the below logic
  // by splitting the csum offload feature bit into separate bits for IPv4,
  // TCP and UDP.
  assert(((_dev_info.rx_offload_capa & DEV_RX_OFFLOAD_IPV4_CKSUM) &&
          (_dev_info.rx_offload_capa & DEV_RX_OFFLOAD_UDP_CKSUM) &&
          (_dev_info.rx_offload_capa & DEV_RX_OFFLOAD_TCP_CKSUM)) ||
         (!(_dev_info.rx_offload_capa & DEV_RX_OFFLOAD_IPV4_CKSUM) &&
          !(_dev_info.rx_offload_capa & DEV_RX_OFFLOAD_UDP_CKSUM) &&
          !(_dev_info.rx_offload_capa & DEV_RX_OFFLOAD_TCP_CKSUM)));

  // Set Rx checksum checking
  if (  (_dev_info.rx_offload_capa & DEV_RX_OFFLOAD_IPV4_CKSUM) &&
      (_dev_info.rx_offload_capa & DEV_RX_OFFLOAD_UDP_CKSUM) &&
      (_dev_info.rx_offload_capa & DEV_RX_OFFLOAD_TCP_CKSUM)) {
    printf("RX checksum offload supported\n");
    port_conf.rxmode.hw_ip_checksum = 1;
    _hw_features.rx_csum_offload = 1;
  }

  if ((_dev_info.tx_offload_capa & DEV_TX_OFFLOAD_IPV4_CKSUM)) {
    printf("TX ip checksum offload supported\n");
    _hw_features.tx_csum_ip_offload = 1;
  }

  // TSO is supported starting from DPDK v1.8
  if (_dev_info.tx_offload_capa & DEV_TX_OFFLOAD_TCP_TSO) {
    printf("TSO is supported\n");
    _hw_features.tx_tso = 1;
  }

  // There is no UFO support in the PMDs yet.
#if 0
  if (_dev_info.tx_offload_capa & DEV_TX_OFFLOAD_UDP_TSO) {
    printf("UFO is supported\n");
    _hw_features.tx_ufo = 1;
  }
#endif

  // Check that Tx TCP and UDP CSUM features are either all set all together
  // or not set all together. If this assumption breaks we need to rework the
  // below logic by splitting the csum offload feature bit into separate bits
  // for TCP and UDP.
  assert(((_dev_info.tx_offload_capa & DEV_TX_OFFLOAD_UDP_CKSUM) &&
          (_dev_info.tx_offload_capa & DEV_TX_OFFLOAD_TCP_CKSUM)) ||
         (!(_dev_info.tx_offload_capa & DEV_TX_OFFLOAD_UDP_CKSUM) &&
          !(_dev_info.tx_offload_capa & DEV_TX_OFFLOAD_TCP_CKSUM)));

  if (  (_dev_info.tx_offload_capa & DEV_TX_OFFLOAD_UDP_CKSUM) &&
      (_dev_info.tx_offload_capa & DEV_TX_OFFLOAD_TCP_CKSUM)) {
    printf("TX TCP&UDP checksum offload supported\n");
    _hw_features.tx_csum_l4_offload = 1;
  }

  int retval;

  printf("Port %u init ... ", _port_idx);
  fflush(stdout);

  /*
   * Standard DPDK port initialisation - config port, then set up
   * rx and tx rings.
   */
  if ((retval = rte_eth_dev_configure(_port_idx, _num_queues, _num_queues,
                                      &port_conf)) != 0) {
    return retval;
  }

  //rte_eth_promiscuous_enable(port_num);
  printf("done: \n");

  return 0;
}

void DPDKDevice::set_hw_flow_control()
{
  // Read the port's current/default flow control settings
  struct rte_eth_fc_conf fc_conf;
  auto ret = rte_eth_dev_flow_ctrl_get(_port_idx, &fc_conf);

  if (ret == -ENOTSUP) {
    goto not_supported;
  }

  if (ret < 0) {
    rte_exit(EXIT_FAILURE, "Port %u: failed to get hardware flow control settings: (error %d)\n", _port_idx, ret);
  }

  if (_enable_fc) {
    fc_conf.mode = RTE_FC_FULL;
  } else {
    fc_conf.mode = RTE_FC_NONE;
  }

  ret = rte_eth_dev_flow_ctrl_set(_port_idx, &fc_conf);
  if (ret == -ENOTSUP) {
    goto not_supported;
  }

  if (ret < 0) {
    rte_exit(EXIT_FAILURE, "Port %u: failed to set hardware flow control (error %d)\n", _port_idx, ret);
  }

  printf("Port %u: %s HW FC\n", _port_idx,
         (_enable_fc ? "Enabling" : "Disabling"));
  return;

not_supported:
  printf("Port %u: Changing HW FC settings is not supported\n", _port_idx);
}

void DPDKDevice::init_port_fini()
{
  // Changing FC requires HW reset, so set it before the port is initialized.
  set_hw_flow_control();

  if (rte_eth_dev_start(_port_idx) < 0) {
    rte_exit(EXIT_FAILURE, "Cannot start port %d\n", _port_idx);
  }

  if (_num_queues > 1) {
    if (!rte_eth_dev_filter_supported(_port_idx, RTE_ETH_FILTER_HASH)) {
      printf("Port %d: HASH FILTER configuration is supported\n", _port_idx);

      // Setup HW touse the TOEPLITZ hash function as an RSS hash function
      struct rte_eth_hash_filter_info info = {};

      info.info_type = RTE_ETH_HASH_FILTER_GLOBAL_CONFIG;
      info.info.global_conf.hash_func = RTE_ETH_HASH_FUNCTION_TOEPLITZ;

      if (rte_eth_dev_filter_ctrl(_port_idx, RTE_ETH_FILTER_HASH,
                                  RTE_ETH_FILTER_SET, &info) < 0) {
        rte_exit(EXIT_FAILURE, "Cannot set hash function on a port %d\n", _port_idx);
      }
    }

    set_rss_table();
  }

  // Wait for a link
  check_port_link_status();

  ldout(cct, 5) << __func__ << "Created DPDK device" << dendl;
}

void DPDKQueuePair::configure_proxies(const std::map<unsigned, float>& cpu_weights) {
  assert(!cpu_weights.empty());
  if (cpu_weights.size() == 1 && cpu_weights.begin()->first == _qid) {
    // special case queue sending to self only, to avoid requiring a hash value
    return;
  }
  register_packet_provider([this] {
    Tub<Packet> p;
    if (!_proxy_packetq.empty()) {
      p.construct(_proxy_packetq.front());
      _proxy_packetq.pop_front();
    }
    return p;
  });
  build_sw_reta(cpu_weights);
}

void DPDKQueuePair::build_sw_reta(const std::map<unsigned, float>& cpu_weights) {
  float total_weight = 0;
  for (auto&& x : cpu_weights) {
    total_weight += x.second;
  }
  float accum = 0;
  unsigned idx = 0;
  std::array<uint8_t, 128> reta;
  for (auto&& entry : cpu_weights) {
    auto cpu = entry.first;
    auto weight = entry.second;
    accum += weight;
    while (idx < (accum / total_weight * reta.size() - 0.5)) {
      reta[idx++] = cpu;
    }
  }
  _sw_reta = reta;
}


void* DPDKQueuePair::alloc_mempool_xmem(
    uint16_t num_bufs, uint16_t buf_sz, std::vector<phys_addr_t>& mappings)
{
  using namespace memory;
  char* xmem;
  struct rte_mempool_objsz mp_obj_sz = {};

  rte_mempool_calc_obj_size(buf_sz, 0, &mp_obj_sz);

  size_t xmem_size =
      rte_mempool_xmem_size(num_bufs,
                            mp_obj_sz.elt_size + mp_obj_sz.header_size +
                            mp_obj_sz.trailer_size,
                            page_bits);

  // Aligning to 2M causes the further failure in small allocations.
  // TODO: Check why - and fix.
  if (posix_memalign((void**)&xmem, page_size, xmem_size)) {
    printf("Can't allocate %ld bytes aligned to %ld\n",
           xmem_size, page_size);
    return nullptr;
  }

  for (size_t i = 0; i < xmem_size / page_size; ++i) {
    translation tr = translate(xmem + i * page_size, page_size);
    assert(tr.size);
    mappings.push_back(tr.addr);
  }

  return xmem;
}

bool DPDKQueuePair::init_rx_mbuf_pool()
{
  std::string name = std::string(pktmbuf_pool_name) + std::to_string(_qid) + "_rx";

  printf("Creating Rx mbuf pool '%s' [%u mbufs] ...\n",
         name.c_str(), mbufs_per_queue_rx);

  //
  // If we have a hugetlbfs memory backend we may perform a virt2phys
  // translation and memory is "pinned". Therefore we may provide an external
  // memory for DPDK pools and this way significantly reduce the memory needed
  // for the DPDK in this case.
  //
  std::vector<phys_addr_t> mappings;

  _rx_xmem.reset(alloc_mempool_xmem(mbufs_per_queue_rx, mbuf_overhead,
                                    mappings));
  if (!_rx_xmem.get()) {
    printf("Can't allocate a memory for Rx buffers\n");
    return false;
  }

  //
  // Don't pass single-producer/single-consumer flags to mbuf create as it
  // seems faster to use a cache instead.
  //
  struct rte_pktmbuf_pool_private roomsz = {};
  roomsz.mbuf_data_room_size = mbuf_data_size + RTE_PKTMBUF_HEADROOM;
  _pktmbuf_pool_rx =
      rte_mempool_xmem_create(name.c_str(),
                              mbufs_per_queue_rx, mbuf_overhead,
                              mbuf_cache_size,
                              sizeof(struct rte_pktmbuf_pool_private),
                              rte_pktmbuf_pool_init, as_cookie(roomsz),
                              rte_pktmbuf_init, nullptr,
                              rte_socket_id(), 0,
                              _rx_xmem.get(), mappings.data(),
                              mappings.size(),
                              page_bits);

  // reserve the memory for Rx buffers containers
  _rx_free_pkts.reserve(mbufs_per_queue_rx);
  _rx_free_bufs.reserve(mbufs_per_queue_rx);

  //
  // 1) Pull all entries from the pool.
  // 2) Bind data buffers to each of them.
  // 3) Return them back to the pool.
  //
  for (int i = 0; i < mbufs_per_queue_rx; i++) {
    rte_mbuf* m = rte_pktmbuf_alloc(_pktmbuf_pool_rx);
    assert(m);
    _rx_free_bufs.push_back(m);
  }

  for (auto&& m : _rx_free_bufs) {
    if (!init_noninline_rx_mbuf(m, mbuf_data_size)) {
      printf("Failed to allocate data buffers for Rx ring. "
             "Consider increasing the amount of memory.\n");
      exit(1);
    }
  }

  rte_mempool_put_bulk(_pktmbuf_pool_rx, (void**)_rx_free_bufs.data(),
                       _rx_free_bufs.size());

  _rx_free_bufs.clear();

  return _pktmbuf_pool_rx != nullptr;
}

void DPDKDevice::check_port_link_status()
{
  int count = 0;
  constexpr auto check_interval = 100ms;

  ldout(cct, 20) << __func__ << "Checking link status " << dendl;
  const int max_check_time = 90;  /* 9s (90 * 100ms) in total */
  struct rte_eth_link link;
  memset(&link, 0, sizeof(link));
  rte_eth_link_get_nowait(_port_idx, &link);

  while (true) {
    if (link.link_status) {
      ldout(cct, 5) << __func__ << " done Port "
                    << static_cast<unsigned>(_port_idx)
                    << " Link Up - speed " << link.link_speed
                    << " Mbps - "
                    << ((link.link_duplex == ETH_LINK_FULL_DUPLEX) ? ("full-duplex") : ("half-duplex\n"))
                    << dendl;
    } else if (count++ < max_check_time) {
      ldout(cct, 20) << __func__ << " not ready, continue to wait." << dendl;
    } else {
      lderr(cct) << __func__ << "done Port " << _port_idx << " Link Down" << dendl;
      assert(0);
    }
  }
}

DPDKQueuePair::DPDKQueuePair(EventCenter *c, DPDKDevice* dev, uint8_t qid)
  : _tx_poller(this), _dev(dev), center(c), _qid(qid),
    _rx_gc_poller(this), _tx_buf_factory(qid),
    _tx_gc_poller(this)
{
  if (!init_rx_mbuf_pool()) {
    rte_exit(EXIT_FAILURE, "Cannot initialize mbuf pools\n");
  }

  static_assert(offsetof(tx_buf, private_end) -
                offsetof(tx_buf, private_start) <= RTE_PKTMBUF_HEADROOM,
                "RTE_PKTMBUF_HEADROOM is less than DPDKQueuePair::tx_buf size! "
                "Increase the headroom size in the DPDK configuration");
  static_assert(offsetof(tx_buf, _mbuf) == 0,
                "There is a pad at the beginning of the tx_buf before _mbuf "
                "field!");
  static_assert((inline_mbuf_data_size & (inline_mbuf_data_size - 1)) == 0,
                "inline_mbuf_data_size has to be a power of two!");

  if (rte_eth_rx_queue_setup(_dev->port_idx(), _qid, default_ring_size,
                             rte_eth_dev_socket_id(_dev->port_idx()),
                             _dev->def_rx_conf(), _pktmbuf_pool_rx) < 0) {
    rte_exit(EXIT_FAILURE, "Cannot initialize rx queue\n");
  }

  if (rte_eth_tx_queue_setup(_dev->port_idx(), _qid, default_ring_size,
                             rte_eth_dev_socket_id(_dev->port_idx()), _dev->def_tx_conf()) < 0) {
    rte_exit(EXIT_FAILURE, "Cannot initialize tx queue\n");
  }

  std::string name(std::string("queue") + std::to_string(qid));
  PerfCountersBuilder plb(cct, name, l_dpdk_qp_first, l_dpdk_qp_last);

  plb.add_u64_counter(l_dpdk_qp_rx_packets, "dpdk_receive_packets", "DPDK received packets");
  plb.add_u64_counter(l_dpdk_qp_tx_packets, "dpdk_send_packets", "DPDK sendd packets");
  plb.add_u64_counter(l_dpdk_qp_rx_total_errors, "dpdk_receive_total_errors", "DPDK received total error packets");
  plb.add_u64_counter(l_dpdk_qp_rx_bad_checksum_errors, "dpdk_receive_bad_checksum_errors", "DPDK received bad checksum packets");
  plb.add_u64_counter(l_dpdk_qp_rx_no_memory_errors, "dpdk_receive_no_memory_errors", "DPDK received no memory packets");
  plb.add_u64_counter(l_dpdk_qp_rx_bytes, "dpdk_receive_bytes", "DPDK received bytes");
  plb.add_u64_counter(l_dpdk_qp_tx_bytes, "dpdk_send_bytes", "DPDK sendd bytes");
  plb.add_u64_counter(l_dpdk_qp_rx_last_bunch, "dpdk_receive_last_bunch", "DPDK last received bunch");
  plb.add_u64_counter(l_dpdk_qp_tx_last_bunch, "dpdk_send_last_bunch", "DPDK last send bunch");
  plb.add_u64_counter(l_dpdk_qp_rx_fragments, "dpdk_receive_fragments", "DPDK received total fragments");
  plb.add_u64_counter(l_dpdk_qp_tx_fragments, "dpdk_send_fragments", "DPDK sendd total fragments");
  plb.add_u64_counter(l_dpdk_qp_rx_copy_ops, "dpdk_receive_copy_ops", "DPDK received copy operations");
  plb.add_u64_counter(l_dpdk_qp_tx_copy_ops, "dpdk_send_copy_ops", "DPDK sendd copy operations");
  plb.add_u64_counter(l_dpdk_qp_rx_copy_bytes, "dpdk_receive_copy_bytes", "DPDK received copy bytes");
  plb.add_u64_counter(l_dpdk_qp_tx_copy_bytes, "dpdk_send_copy_bytes", "DPDK send copy bytes");
  plb.add_u64_counter(l_dpdk_qp_rx_linearize_ops, "dpdk_receive_linearize_ops", "DPDK received linearize operations");
  plb.add_u64_counter(l_dpdk_qp_tx_linearize_ops, "dpdk_send_linearize_ops", "DPDK send linearize operations");
  plb.add_u64_counter(l_dpdk_qp_tx_queue_length, "dpdk_send_queue_length", "DPDK send queue length");

  perf_logger = plb.create_perf_counters();
  cct->get_perfcounters_collection()->add(perf_logger);
}

inline bool DPDKQueuePair::poll_tx() {
  if (_tx_packetq.size() < 16) {
    // refill send queue from upper layers
    uint32_t work;
    do {
      work = 0;
      for (auto&& pr : _pkt_providers) {
        auto p = pr();
        if (p) {
          work++;
          _tx_packetq.push_back(std::move(p.value()));
          if (_tx_packetq.size() == 128) {
            break;
          }
        }
      }
    } while (work && _tx_packetq.size() < 128);
  }
  if (!_tx_packetq.empty()) {
    _stats.tx.good.update_pkts_bunch(send(_tx_packetq));
    return true;
  }

  return false;
}


void DPDKQueuePair::rx_start() {
  _rx_poller.construct(this);
}

template<>
inline Tub<Packet> DPDKQueuePair<false>::from_mbuf_lro(rte_mbuf* m)
{
  //
  // Try to allocate a buffer for the whole packet's data.
  // If we fail - construct the packet from mbufs.
  // If we succeed - copy the data into this buffer, create a packet based on
  // this buffer and return the mbuf to its pool.
  //
  Tub<Packet> p;
  auto pkt_len = rte_pktmbuf_pkt_len(m);
  char* buf = (char*)malloc(pkt_len);
  if (buf) {
    // Copy the contents of the packet into the buffer we've just allocated
    size_t offset = 0;
    for (rte_mbuf* m1 = m; m1 != nullptr; m1 = m1->next) {
      char* data = rte_pktmbuf_mtod(m1, char*);
      auto len = rte_pktmbuf_data_len(m1);

      rte_memcpy(buf + offset, data, len);
      offset += len;
    }

    rte_pktmbuf_free(m);

    p.construct((fragment{buf, pkt_len}, make_free_deleter(buf)));
  }

  // Drop if allocation failed
  rte_pktmbuf_free(m);

  return p;
}

template<>
inline Tub<Packet> DPDKQueuePair<false>::from_mbuf(rte_mbuf* m)
{
  if (!_dev->hw_features_ref().rx_lro || rte_pktmbuf_is_contiguous(m)) {
    //
    // Try to allocate a buffer for packet's data. If we fail - give the
    // application an mbuf itself. If we succeed - copy the data into this
    // buffer, create a packet based on this buffer and return the mbuf to
    // its pool.
    //
    auto len = rte_pktmbuf_data_len(m);
    char* buf = (char*)malloc(len);
    Tub<Packet> p;

    if (!buf) {
      // Drop if allocation failed
      rte_pktmbuf_free(m);

      return p;
    } else {
      rte_memcpy(buf, rte_pktmbuf_mtod(m, char*), len);
      rte_pktmbuf_free(m);

      p.construct((fragment{buf, len}, make_free_deleter(buf)));
      return p;
    }
  } else {
    return from_mbuf_lro(m);
  }
}

template<>
inline Tub<Packet> DPDKQueuePair<true>::from_mbuf_lro(rte_mbuf* m)
{
  _frags.clear();
  _bufs.clear();

  for (; m != nullptr; m = m->next) {
    char* data = rte_pktmbuf_mtod(m, char*);

    _frags.emplace_back(fragment{data, rte_pktmbuf_data_len(m)});
    _bufs.push_back(data);
  }

  Tub<Packet> p;
  p.construct(
      _frags.begin(), _frags.end(),
      make_deleter(deleter(), [_bufs] { for (auto&& b : bufs_vec) { free(b); } }));
  return p;
}

template<>
inline Tub<Packet> DPDKQueuePair<true>::from_mbuf(rte_mbuf* m)
{
  _rx_free_pkts.push_back(m);
  _num_rx_free_segs += m->nb_segs;

  if (!_dev->hw_features_ref().rx_lro || rte_pktmbuf_is_contiguous(m)) {
    char* data = rte_pktmbuf_mtod(m, char*);

    Tub<Packet> p;
    p.construct(fragment{data, rte_pktmbuf_data_len(m)}, make_free_deleter(data));
    return p;
  } else {
    return from_mbuf_lro(m);
  }
}

inline bool DPDKQueuePair::refill_one_cluster(rte_mbuf* head)
{
  for (; head != nullptr; head = head->next) {
    if (!refill_rx_mbuf(head, mbuf_data_size)) {
      //
      // If we failed to allocate a new buffer - push the rest of the
      // cluster back to the free_packets list for a later retry.
      //
      _rx_free_pkts.push_back(head);
      return false;
    }
    _rx_free_bufs.push_back(head);
  }

  return true;
}

bool DPDKQueuePair::rx_gc()
{
  if (_num_rx_free_segs >= rx_gc_thresh) {
    while (!_rx_free_pkts.empty()) {
      //
      // Use back() + pop_back() semantics to avoid an extra
      // _rx_free_pkts.clear() at the end of the function - clear() has a
      // linear complexity.
      //
      auto m = _rx_free_pkts.back();
      _rx_free_pkts.pop_back();

      if (!refill_one_cluster(m)) {
        break;
      }
    }

    if (_rx_free_bufs.size()) {
      rte_mempool_put_bulk(_pktmbuf_pool_rx,
                           (void **)_rx_free_bufs.data(),
                           _rx_free_bufs.size());

      // TODO: assert() in a fast path! Remove me ASAP!
      assert(_num_rx_free_segs >= _rx_free_bufs.size());

      _num_rx_free_segs -= _rx_free_bufs.size();
      _rx_free_bufs.clear();

      // TODO: assert() in a fast path! Remove me ASAP!
      assert((_rx_free_pkts.empty() && !_num_rx_free_segs) ||
             (!_rx_free_pkts.empty() && _num_rx_free_segs));
    }
  }

  return _num_rx_free_segs >= rx_gc_thresh;
}


void DPDKQueuePair::process_packets(
    struct rte_mbuf **bufs, uint16_t count)
{
  uint64_t nr_frags = 0, bytes = 0;

  for (uint16_t i = 0; i < count; i++) {
    struct rte_mbuf *m = bufs[i];
    offload_info oi;

    Tub<Packet> p = from_mbuf(m);

    // Drop the packet if translation above has failed
    if (!p) {
      _stats.rx.bad.inc_no_mem();
      continue;
    }

    nr_frags += m->nb_segs;
    bytes    += m->pkt_len;

    // Set stipped VLAN value if available
    if ((_dev->_dev_info.rx_offload_capa & DEV_RX_OFFLOAD_VLAN_STRIP) &&
        (m->ol_flags & PKT_RX_VLAN_PKT)) {
      oi.vlan_tci = m->vlan_tci;
    }

    if (_dev->hw_features().rx_csum_offload) {
      if (m->ol_flags & (PKT_RX_IP_CKSUM_BAD | PKT_RX_L4_CKSUM_BAD)) {
        // Packet with bad checksum, just drop it.
        _stats.rx.bad.inc_csum_err();
        continue;
      }
      // Note that when _hw_features.rx_csum_offload is on, the receive
      // code for ip, tcp and udp will assume they don't need to check
      // the checksum again, because we did this here.
    }

    (*p).set_offload_info(oi);
    if (m->ol_flags & PKT_RX_RSS_HASH) {
      (*p).set_rss_hash(m->hash.rss);
    }

    _dev->l2receive(_qid, std::move(*p));
  }

  _stats.rx.good.update_pkts_bunch(count);
  _stats.rx.good.update_frags_stats(nr_frags, bytes);
}

bool DPDKQueuePair::poll_rx_once()
{
  struct rte_mbuf *buf[packet_read_size];

  /* read a port */
  uint16_t rx_count = rte_eth_rx_burst(_dev->port_idx(), _qid,
                                       buf, packet_read_size);

  /* Now process the NIC packets read */
  if (likely(rx_count > 0)) {
    process_packets(buf, rx_count);
  }

  return rx_count;
}

DPDKQueuePair::tx_buf_factory::tx_buf_factory(uint8_t qid)
{
  std::string name = std::string(pktmbuf_pool_name) + std::to_string(qid) + "_tx";
  printf("Creating Tx mbuf pool '%s' [%u mbufs] ...\n",
         name.c_str(), mbufs_per_queue_tx);

  std::vector<phys_addr_t> mappings;

  _xmem.reset(DPDKQueuePair::alloc_mempool_xmem(mbufs_per_queue_tx,
                                                inline_mbuf_size,
                                                mappings));
  if (!_xmem.get()) {
    printf("Can't allocate a memory for Tx buffers\n");
    exit(1);
  }

  //
  // We are going to push the buffers from the mempool into
  // the circular_buffer and then poll them from there anyway, so
  // we prefer to make a mempool non-atomic in this case.
  //
  _pool =
      rte_mempool_xmem_create(name.c_str(),
                              mbufs_per_queue_tx, inline_mbuf_size,
                              mbuf_cache_size,
                              sizeof(struct rte_pktmbuf_pool_private),
                              rte_pktmbuf_pool_init, nullptr,
                              rte_pktmbuf_init, nullptr,
                              rte_socket_id(), 0,
                              _xmem.get(), mappings.data(),
                              mappings.size(), page_bits);

  if (!_pool) {
    printf("Failed to create mempool for Tx\n");
    exit(1);
  }

  //
  // Fill the factory with the buffers from the mempool allocated
  // above.
  //
  init_factory();
}

static bool DPDKQueuePair::tx_buf::i40e_should_linearize(rte_mbuf *head)
{
  bool is_tso = head->ol_flags & PKT_TX_TCP_SEG;

  // For a non-TSO case: number of fragments should not exceed 8
  if (!is_tso){
    return head->nb_segs > i40e_max_xmit_segment_frags;
  }

  //
  // For a TSO case each MSS window should not include more than 8
  // fragments including headers.
  //

  // Calculate the number of frags containing headers.
  //
  // Note: we support neither VLAN nor tunneling thus headers size
  // accounting is super simple.
  //
  size_t headers_size = head->l2_len + head->l3_len + head->l4_len;
  unsigned hdr_frags = 0;
  size_t cur_payload_len = 0;
  rte_mbuf *cur_seg = head;

  while (cur_seg && cur_payload_len < headers_size) {
    cur_payload_len += cur_seg->data_len;
    cur_seg = cur_seg->next;
    hdr_frags++;
  }

  //
  // Header fragments will be used for each TSO segment, thus the
  // maximum number of data segments will be 8 minus the number of
  // header fragments.
  //
  // It's unclear from the spec how the first TSO segment is treated
  // if the last fragment with headers contains some data bytes:
  // whether this fragment will be accounted as a single fragment or
  // as two separate fragments. We prefer to play it safe and assume
  // that this fragment will be accounted as two separate fragments.
  //
  size_t max_win_size = i40e_max_xmit_segment_frags - hdr_frags;

  if (head->nb_segs <= max_win_size) {
    return false;
  }

  // Get the data (without headers) part of the first data fragment
  size_t prev_frag_data = cur_payload_len - headers_size;
  auto mss = head->tso_segsz;

  while (cur_seg) {
    unsigned frags_in_seg = 0;
    size_t cur_seg_size = 0;

    if (prev_frag_data) {
      cur_seg_size = prev_frag_data;
      frags_in_seg++;
      prev_frag_data = 0;
    }

    while (cur_seg_size < mss && cur_seg) {
      cur_seg_size += cur_seg->data_len;
      cur_seg = cur_seg->next;
      frags_in_seg++;

      if (frags_in_seg > max_win_size) {
        return true;
      }
    }

    if (cur_seg_size > mss) {
      prev_frag_data = cur_seg_size - mss;
    }
  }

  return false;
}

static tx_buf* DPDKQueuePair::tx_buf::from_packet_zc(Packet&& p, DPDKQueuePair& qp)
{
  // Too fragmented - linearize
  if (p.nr_frags() > max_frags) {
    p.linearize();
    ++qp._stats.tx.linearized;
  }

  build_mbuf_cluster:
  rte_mbuf *head = nullptr, *last_seg = nullptr;
  unsigned nsegs = 0;

  //
  // Create a HEAD of the fragmented packet: check if frag0 has to be
  // copied and if yes - send it in a copy way
  //
  if (!check_frag0(p)) {
    if (!copy_one_frag(qp, p.frag(0), head, last_seg, nsegs)) {
      return nullptr;
    }
  } else if (!translate_one_frag(qp, p.frag(0), head, last_seg, nsegs)) {
    return nullptr;
  }

  unsigned total_nsegs = nsegs;

  for (unsigned i = 1; i < p.nr_frags(); i++) {
    rte_mbuf *h = nullptr, *new_last_seg = nullptr;
    if (!translate_one_frag(qp, p.frag(i), h, new_last_seg, nsegs)) {
      me(head)->recycle();
      return nullptr;
    }

    total_nsegs += nsegs;

    // Attach a new buffers' chain to the packet chain
    last_seg->next = h;
    last_seg = new_last_seg;
  }

  // Update the HEAD buffer with the packet info
  head->pkt_len = p.len();
  head->nb_segs = total_nsegs;

  set_cluster_offload_info(p, qp, head);

  //
  // If a packet hasn't been linearized already and the resulting
  // cluster requires the linearisation due to HW limitation:
  //
  //    - Recycle the cluster.
  //    - Linearize the packet.
  //    - Build the cluster once again
  //
  if (head->nb_segs > max_frags ||
      (p.nr_frags() > 1 && qp.port().is_i40e_device() && i40e_should_linearize(head))) {
    me(head)->recycle();
    p.linearize();
    ++qp._stats.tx.linearized;

    goto build_mbuf_cluster;
  }

  me(last_seg)->set_packet(std::move(p));

  return me(head);
}

static void DPDKQueuePair::tx_buf::copy_packet_to_cluster(const Packet& p, rte_mbuf* head)
{
  rte_mbuf* cur_seg = head;
  size_t cur_seg_offset = 0;
  unsigned cur_frag_idx = 0;
  size_t cur_frag_offset = 0;

  while (true) {
    size_t to_copy = std::min(p.frag(cur_frag_idx).size - cur_frag_offset,
                              inline_mbuf_data_size - cur_seg_offset);

    memcpy(rte_pktmbuf_mtod_offset(cur_seg, void*, cur_seg_offset),
           p.frag(cur_frag_idx).base + cur_frag_offset, to_copy);

    cur_frag_offset += to_copy;
    cur_seg_offset += to_copy;

    if (cur_frag_offset >= p.frag(cur_frag_idx).size) {
      ++cur_frag_idx;
      if (cur_frag_idx >= p.nr_frags()) {
        //
        // We are done - set the data size of the last segment
        // of the cluster.
        //
        cur_seg->data_len = cur_seg_offset;
        break;
      }

      cur_frag_offset = 0;
    }

    if (cur_seg_offset >= inline_mbuf_data_size) {
      cur_seg->data_len = inline_mbuf_data_size;
      cur_seg = cur_seg->next;
      cur_seg_offset = 0;

      // FIXME: assert in a fast-path - remove!!!
      assert(cur_seg);
    }
  }
}

static tx_buf* DPDKQueuePair::tx_buf::from_packet_copy(Packet&& p, DPDKQueuePair& qp)
{
  // sanity
  if (!p.len()) {
    return nullptr;
  }

  /*
   * Here we are going to use the fact that the inline data size is a
   * power of two.
   *
   * We will first try to allocate the cluster and only if we are
   * successful - we will go and copy the data.
   */
  auto aligned_len = align_up((size_t)p.len(), inline_mbuf_data_size);
  unsigned nsegs = aligned_len / inline_mbuf_data_size;
  rte_mbuf *head = nullptr, *last_seg = nullptr;

  tx_buf* buf = qp.get_tx_buf();
  if (!buf) {
    return nullptr;
  }

  head = buf->rte_mbuf_p();
  last_seg = head;
  for (unsigned i = 1; i < nsegs; i++) {
    buf = qp.get_tx_buf();
    if (!buf) {
      me(head)->recycle();
      return nullptr;
    }

    last_seg->next = buf->rte_mbuf_p();
    last_seg = last_seg->next;
  }

  //
  // If we've got here means that we have succeeded already!
  // We only need to copy the data and set the head buffer with the
  // relevant info.
  //
  head->pkt_len = p.len();
  head->nb_segs = nsegs;

  copy_packet_to_cluster(p, head);
  set_cluster_offload_info(p, qp, head);

  return me(head);
}

static size_t DPDKQueuePair::tx_buf::copy_one_data_buf(
    DPDKQueuePair& qp, rte_mbuf*& m, char* data, size_t buf_len)
{
  tx_buf* buf = qp.get_tx_buf();
  if (!buf) {
    return 0;
  }

  size_t len = std::min(buf_len, inline_mbuf_data_size);

  m = buf->rte_mbuf_p();

  // mbuf_put()
  m->data_len = len;
  m->pkt_len  = len;

  qp._stats.tx.good.update_copy_stats(1, len);

  memcpy(rte_pktmbuf_mtod(m, void*), data, len);

  return len;
}

void DPDKDevice::set_rss_table()
{
  if (_dev_info.reta_size == 0)
    return;

  int reta_conf_size = std::max(1, _dev_info.reta_size / RTE_RETA_GROUP_SIZE);
  rte_eth_rss_reta_entry64 reta_conf[reta_conf_size];

  // Configure the HW indirection table
  unsigned i = 0;
  for (auto& x : reta_conf) {
    x.mask = ~0ULL;
    for (auto& r: x.reta) {
      r = i++ % _num_queues;
    }
  }

  if (rte_eth_dev_rss_reta_update(_port_idx, reta_conf, _dev_info.reta_size)) {
    rte_exit(EXIT_FAILURE, "Port %d: Failed to update an RSS indirection table", _port_idx);
  }

  // Fill our local indirection table. Make it in a separate loop to keep things simple.
  i = 0;
  for (auto& r : _redir_table) {
    r = i++ % _num_queues;
  }
}

/******************************** Interface functions *************************/

std::unique_ptr<DPDKDevice> create_dpdk_net_device(
    CephContext *cct,
    uint8_t port_idx,
    uint8_t num_queues,
    bool use_lro,
    bool enable_fc)
{
  static bool called = false;

  assert(!called);
  assert(dpdk::eal::initialized);

  called = true;

  // Check that we have at least one DPDK-able port
  if (rte_eth_dev_count() == 0) {
    rte_exit(EXIT_FAILURE, "No Ethernet ports - bye\n");
  } else {
    ldout(cct, 10) << __func__ << "ports number: " << rte_eth_dev_count() << dendl;
  }

  return std::make_unique<DPDKDevice>(cct, port_idx, num_queues, use_lro,
                                      enable_fc);
}
