/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Time-stamp: <2013-04-23 17:24:05 fufeng.syd>
 * Version: $Id$
 * Filename: ob_reload_config.h
 *
 * Authors:
 *   Yudi Shi <fufeng.syd@alipay.com>
 *
 */

#ifndef _OB_RELOAD_CONFIG_H_
#define _OB_RELOAD_CONFIG_H_

#include "tbsys.h"
#include "common/ob_define.h"

namespace oceanbase {
  namespace common {
    class ObReloadConfig
    {
      public:
        virtual ~ObReloadConfig(){}
        virtual int operator()()
        {
          return OB_SUCCESS;
        }
    };
  } // end of namespace common
} // end of namespace oceanbase

#endif /* _OB_RELOAD_CONFIG_H_ */
