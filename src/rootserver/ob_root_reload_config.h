/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Time-stamp: <2013-04-23 14:46:37 fufeng.syd>
 * Version: $Id$
 * Filename: ob_root_reload_config.h
 *
 * Authors:
 *   Yudi Shi <fufeng.syd@alipay.com>
 *
 */

#ifndef _OB_ROOT_RELOAD_CONFIG_H_
#define _OB_ROOT_RELOAD_CONFIG_H_

#include "common/ob_reload_config.h"
#include "ob_root_server_config.h"

namespace oceanbase {
  namespace rootserver {
    /* forward declearation */
    class ObRootServer2;

    class ObRootReloadConfig
      : public common::ObReloadConfig
    {
      public:
        ObRootReloadConfig(const ObRootServerConfig &config);
        virtual ~ObRootReloadConfig();

        int operator() ();

        void set_root_server(ObRootServer2 &root_server);
      private:
        const ObRootServerConfig &config_;
        ObRootServer2 *root_server_;
    };
  } // end of namespace rootserver
} // end of namespace oceanbase

#endif /* _OB_ROOT_RELOAD_CONFIG_H_ */
