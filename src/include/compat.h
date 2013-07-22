/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2011 Stanislav Sedov <stas@FreeBSD.org>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 */

#ifndef CEPH_COMPAT_H
#define CEPH_COMPAT_H

#include "acconfig.h"

#if defined(__FreeBSD__)
#define	lseek64(fd, offset, whence)	lseek(fd, offset, whence)
#define	ENODATA	61
#define	TEMP_FAILURE_RETRY
#define	MSG_MORE 0
#endif /* !__FreeBSD__ */

#ifdef DARWIN

#define TEMP_FAILURE_RETRY(expression) \
  ({ long int __result; \
     do __result = (long int) (expression); \
     while (__result == -1L && errno == EINTR); \
     __result; })

#define	lseek64(fd, offset, whence)	lseek(fd, offset, whence)

#endif

#endif /* !CEPH_COMPAT_H */
