/*
 * Copyright (C) 2007-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * Description here
 *
 * Version: $Id$
 *
 * Authors:
 *   Zhifeng YANG <zhifeng.yang@aliyun-inc.com>
 *     - some work details here
 */
/** 
 * @file ob_ocm_main.cpp
 * @author Zhifeng YANG <zhifeng.yang@aliyun-inc.com>
 * @date 2011-07-06
 * @brief 
 */

#ifndef OB_OCM_MAIN_H
#define OB_OCM_MAIN_H

#include "common/base_main.h"
#include "common/ob_define.h"
#include "ob_ocm_server.h"
#include <cstdio>
#include <malloc.h>

using namespace oceanbase::common;

namespace oceanbase
{
  namespace clustermanager
  {
    class OcmMain: public ::oceanbase::common::BaseMain
    {
      public:
        OcmMain();
        virtual ~OcmMain();
        static OcmMain* get_instance();
      public:
        OcmServer* get_ocm_server()
        {
          return ocm_server_;
        }
        const OcmServer* get_ocm_server() const
        {
          return ocm_server_;
        }
      protected:
        int do_work();
        void do_signal(const int sig);
      private:
        OcmServer *ocm_server_;
      private:
        static const int DEFAULT_MMAP_MAX_VAL = 65536;
    };
  }
}
#endif

