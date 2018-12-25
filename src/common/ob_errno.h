/**
 * (C) 2010-2013 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_errno.h
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#ifndef _OB_ERRNO_H
#define _OB_ERRNO_H 1

namespace oceanbase
{
  namespace common
  {
    const char* ob_strerror(int err);
  } // end namespace common
} // end namespace oceanbase

#endif /* _OB_ERRNO_H */
