/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Time-stamp: <2013-04-23 17:02:14 fufeng.syd>
 * Version: $Id$
 * Filename: ob_chunk_reload_config.h
 *
 * Authors:
 *   Yudi Shi <fufeng.syd@alipay.com>
 *
 */

#ifndef _OB_CHUNK_RELOAD_CONFIG_H_
#define _OB_CHUNK_RELOAD_CONFIG_H_

#include "common/ob_reload_config.h"

namespace oceanbase {
  namespace chunkserver {
    /* forward declearation */
    class ObChunkServer;

    class ObChunkReloadConfig
      : public common::ObReloadConfig
    {
      public:
        ObChunkReloadConfig();
        virtual ~ObChunkReloadConfig();

        int operator() ();

        void set_chunk_server(ObChunkServer &chunk_server);

      private:
        ObChunkServer *chunk_server_;
    };
  } // end of namespace chunkserver
} // end of namespace oceanbase

#endif /* _OB_CHUNK_RELOAD_CONFIG_H_ */
