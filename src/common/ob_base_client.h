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
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *     - some work details here
 */
#ifndef _OB_BASE_CLIENT_H
#define _OB_BASE_CLIENT_H

#include "common/ob_client_manager.h"
#include "common/ob_packet_factory.h"
#include "common/ob_server.h"
#include "easy_io.h"
#include "ob_tbnet_callback.h"

namespace oceanbase
{
namespace common
{
class ObBaseClient
{
public:
  ObBaseClient();
  virtual ~ObBaseClient();
  virtual int initialize(const ObServer& server);
  void destroy();
  int send_recv(const int32_t pcode, const int32_t version, const int64_t timeout, 
                ObDataBuffer& message_buff);
  ObClientManager &get_client_mgr();
private:
  bool init_;
  ObServer server_;
  easy_io_t *eio_;
  easy_io_handler_pt client_handler_;
  ObClientManager client_mgr_;
};

inline ObClientManager &ObBaseClient::get_client_mgr()
{
  return client_mgr_;
}
  
} // end namespace common
} // end namespace oceanbase

#endif /* _OB_BASE_CLIENT_H */

