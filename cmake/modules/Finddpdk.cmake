# - Find dpdk
# Find dpdk transport library
#
# DPDK_INCLUDE_DIR -  libxio include dir
# DPDK_LIBRARIES - List of libraries
# DPDK_FOUND - True if libxio found.

set(_dpdk_include_path ${HT_DEPENDENCY_INCLUDE_DIR})
set(_dpdk_lib_path ${HT_DEPENDENCY_LIB_DIR})
if (EXISTS ${DPDK_TARGET})
  list(APPEND _dpdk_include_path "${DPDK_TARGET}/include")
  list(APPEND _dpdk_lib_path "${DPDK_TARGET}/lib")
else()
  list(APPEND _dpdk_include_path /usr/include /usr/local/include /opt/accelio/include)
  list(APPEND _dpdk_lib_path /lib /usr/lib /usr/local/lib /opt/accelio/lib)
endif()

find_path(DPDK_INCLUDE_DIR rte_eal.h NO_DEFAULT_PATH PATHS ${_dpdk_include_path})

find_library(DPDK_LIBRARY NO_DEFAULT_PATH NAMES rte_eal PATHS ${_dpdk_lib_path})

if (DPDK_INCLUDE_DIR AND DPDK_LIBRARY)
  set(DPDK_FOUND TRUE)
  set(DPDK_LIBRARIES ${DPDK_LIBRARY})
else ()
  set(DPDK_FOUND FALSE)
  set(DPDK_LIBRARIES )
endif ()

if (DPDK_FOUND)
  message(STATUS "Found DPDK: ${DPDK_INCLUDE_DIR} ${DPDK_LIBRARY}")
else ()
  message(STATUS "Not Found DPDK: ${DPDK_INCLUDE_DIR} ${DPDK_LIBRARY}")
  if (DPDK_FIND_REQUIRED)
    message(STATUS "Looked for DPDK libraries named ${DPDK_NAMES}.")
    message(FATAL_ERROR "Could NOT find DPDK library")
  endif ()
endif ()

mark_as_advanced(
  DPDK_LIBRARY
  DPDK_INCLUDE_DIR
  )
