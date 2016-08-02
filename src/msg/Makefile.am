libmsg_la_SOURCES = \
	msg/Message.cc \
	msg/Messenger.cc \
	msg/DispatchQueue.cc \
	msg/msg_types.cc

noinst_HEADERS += \
	msg/Connection.h \
	msg/Dispatcher.h \
	msg/DispatchQueue.h \
	msg/Message.h \
	msg/Messenger.h \
	msg/SimplePolicyMessenger.h \
	msg/msg_types.h

libmsg_la_SOURCES += \
	msg/simple/Accepter.cc \
	msg/simple/Pipe.cc \
	msg/simple/PipeConnection.cc \
	msg/simple/SimpleMessenger.cc \
	msg/async/AsyncConnection.cc \
	msg/async/AsyncMessenger.cc \
	msg/async/Event.cc \
	msg/async/net_handler.cc \
	msg/async/Stack.cc \
	msg/async/PosixStack.cc \
	msg/async/EventSelect.cc

if LINUX
libmsg_la_SOURCES += msg/async/EventEpoll.cc
endif

if DARWIN
libmsg_la_SOURCES += msg/async/EventKqueue.cc
endif

if FREEBSD
libmsg_la_SOURCES += msg/async/EventKqueue.cc
endif


noinst_HEADERS += \
	msg/simple/Accepter.h \
	msg/simple/Pipe.h \
	msg/simple/PipeConnection.h \
	msg/simple/SimpleMessenger.h \
	msg/async/AsyncConnection.h \
	msg/async/AsyncMessenger.h \
	msg/async/Event.h \
	msg/async/EventEpoll.h \
	msg/async/EventSelect.h \
	msg/async/Stack.h \
	msg/async/PosixStack.h \
	msg/async/net_handler.h

if LINUX
libmsg_la_SOURCES += msg/async/EventEpoll.h
endif

if DARWIN
libmsg_la_SOURCES += msg/async/EventKqueue.h
endif

if FREEBSD
libmsg_la_SOURCES += msg/async/EventKqueue.h
endif

if ENABLE_XIO
libmsg_la_SOURCES += \
	msg/xio/QueueStrategy.cc \
	msg/xio/XioConnection.cc \
	msg/xio/XioMessenger.cc \
	msg/xio/XioMsg.cc \
	msg/xio/XioPortal.cc \
	msg/xio/XioPool.cc

noinst_HEADERS += \
	msg/xio/DispatchStrategy.h \
	msg/xio/FastStrategy.h \
	msg/xio/QueueStrategy.h \
	msg/xio/XioConnection.h \
	msg/xio/XioInSeq.h \
	msg/xio/XioMessenger.h \
	msg/xio/XioMsg.h \
	msg/xio/XioPool.h \
	msg/xio/XioPortal.h \
	msg/xio/XioSubmit.h
endif

if WITH_DPDK

DPDK_LIBS=" -Wl,--whole-archive -lrte_hash -lrte_kvargs -lrte_mbuf -lethdev -lrte_mempool -lrte_ring "
DPDK_LIBS+="-lrte_eal -lrte_cmdline -lrte_pmd_bond -lrte_pmd_vmxnet3_uio "
DPDK_LIBS+="-lrte_pmd_ixgbe -lrte_pmd_i40e -lrte_pmd_ring -lrte_pmd_af_packet -Wl,--no-whole-archive "
libmsg_la_CXXFLAGS = -I ${with_dpdk}/include -fPIC
libmsg_la_LIBADD = ${DPDK_LIBS}
libmsg_la_SOURCES += \
	msg/async/dpdk/ARP.cc \
	msg/async/dpdk/DPDK.cc \
	msg/async/dpdk/dpdk_rte.cc \
	msg/async/dpdk/DPDKStack.cc \
	msg/async/dpdk/EventDPDK.cc \
	msg/async/dpdk/IP.cc \
	msg/async/dpdk/IPChecksum.cc \
	msg/async/dpdk/Packet.cc \
	msg/async/dpdk/TCP.cc \
	msg/async/dpdk/UserspaceEvent.cc \
	msg/async/dpdk/ethernet.cc \
	msg/async/dpdk/ethernet.cc

noinst_HEADERS += \
	msg/async/dpdk/ARP.h \
	msg/async/dpdk/align.h \
	msg/async/dpdk/array_map.h \
	msg/async/dpdk/byteorder.h \
	msg/async/dpdk/capture.h \
	msg/async/dpdk/circular_buffer.h \
	msg/async/dpdk/const.h \
	msg/async/dpdk/dpdk_rte.h \
	msg/async/dpdk/DPDK.h \
	msg/async/dpdk/DPDKStack.h \
	msg/async/dpdk/ethernet.h \
	msg/async/dpdk/EventDPDK.h \
	msg/async/dpdk/IP.h \
	msg/async/dpdk/IPChecksum.h \
	msg/async/dpdk/net.h \
	msg/async/dpdk/Packet.h \
	msg/async/dpdk/PacketUtil.h \
	msg/async/dpdk/queue.h \
	msg/async/dpdk/shared_ptr.h \
	msg/async/dpdk/stream.h \
	msg/async/dpdk/TCP.h \
	msg/async/dpdk/TCP-Stack.h \
	msg/async/dpdk/toeplitz.h \
	msg/async/dpdk/transfer.h \
	msg/async/dpdk/UserspaceEvent.h
endif

noinst_LTLIBRARIES += libmsg.la
