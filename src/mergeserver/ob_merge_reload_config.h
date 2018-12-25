/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Time-stamp: <2013-04-23 15:44:31 fufeng.syd>
 * Version: $Id$
 * Filename: ob_merge_reload_config.h
 *
 * Authors:
 *   Yudi Shi <fufeng.syd@alipay.com>
 *
 */

#ifndef _OB_MERGE_RELOAD_CONFIG_H_
#define _OB_MERGE_RELOAD_CONFIG_H_

#include "common/ob_reload_config.h"

namespace oceanbase {
  namespace mergeserver {
    /* forward declearation */
    class ObMergeServer;

    class ObMergeReloadConfig
      : public common::ObReloadConfig
    {
      public:
        ObMergeReloadConfig();
        virtual ~ObMergeReloadConfig();

        int operator() ();

        void set_merge_server(ObMergeServer& merge_server);
      private:
        ObMergeServer *merge_server_;
    };
  } // end of namespace mergeserver
} // end of namespace oceanbase

#endif /* _OB_MERGE_RELOAD_CONFIG_H_ */
