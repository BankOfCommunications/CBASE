/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 *
 * Authors:
 *   jianming <jianming.cjq@taobao.com>
 *     - some work details if you want
 *
 */

#ifndef OCEANBASE_COMMON_OB_CHUNK_SERVER_ITEM_H_
#define OCEANBASE_COMMON_OB_CHUNK_SERVER_ITEM_H_

#include "ob_server.h"
#include "utility.h"

namespace oceanbase
{
  namespace common
  {
    struct ObChunkServerItem
    {
      enum RequestStatus {
        UNREQUESTED,
        REQUESTED
      };

      /// address of the server
      common::ObServer addr_;
      /// indicate if the current request has requested this server, 0 for requested, others for not
      enum RequestStatus status_;
      int64_t tablet_version_; //add zhaoqiong [fixbug:tablet version info lost when query from ms] 20170228

      int64_t to_string(char* buffer, const int64_t length) const
      {
        int64_t pos = 0;
        pos += addr_.to_string(buffer + pos, length - pos);
        if (NULL != buffer && length - pos > 0)
        {
          databuff_printf(buffer, length, pos, ",%d", status_);
        }
        return pos;
      }

    };
  }
}

#endif /* OCEANBASE_COMMON_OB_CHUNK_SERVER_ITEM_H_ */


