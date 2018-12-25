/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 *
 * Version: 0.1: load_define.h,v 0.1 2012/03/15 14:00:00 zhidong Exp $
 *
 * Authors:
 *   xielun <xielun.szd@taobao.com>
 *     - some work details if you want
 *
 */

#ifndef LOAD_DEFINE_H_
#define LOAD_DEFINE_H_

#include <stdint.h>
#include <time.h>
#include <string.h>
#include <stdlib.h>

#include "tblog.h"
#include "tbsys.h"

#include "common/ob_define.h"

namespace oceanbase
{
  namespace tools
  {
    // err code
    static const int QUEUE_EMPTY = 10001;
    static const int QUEUE_FULL = 10002;
    static const int FILE_END = 10003;
    // enum const
    const static int64_t MAX_FILE_LEN = 1024;
  }
}

#endif // LOAD_DEFINE_H_
