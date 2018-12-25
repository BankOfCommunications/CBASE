/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_sql_rpc_stub.cpp
 *
 * Authors:
 *   Junquan Chen <jianming.cjq@taobao.com>
 *
 */

#include "ob_sql_rpc_stub.h"
#include "common/ob_client_manager.h"
#include "common/ob_server.h"
#include "common/ob_result.h"
#include "common/ob_operate_result.h"
#include "common/thread_buffer.h"
#include "common/ob_schema.h"
#include "common/ob_tablet_info.h"
#include "common/ob_read_common_data.h"
#include "common/ob_scanner.h"
#include "common/ob_trace_log.h"
#include "common/utility.h"


using namespace oceanbase;
using namespace chunkserver;
using namespace common;


ObSqlRpcStub::ObSqlRpcStub()
{
}

ObSqlRpcStub::~ObSqlRpcStub()
{
}

int ObSqlRpcStub::get(const int64_t timeout, const ObServer & server, const ObGetParam & get_param, ObNewScanner & new_scanner) const
{
  return send_1_return_1(server, timeout, OB_NEW_GET_REQUEST, DEFAULT_VERSION, get_param, new_scanner);
}

int ObSqlRpcStub::scan(const int64_t timeout, const ObServer & server, const ObScanParam & scan_param, ObNewScanner & new_scanner) const
{
  return send_1_return_1(server, timeout, OB_NEW_SCAN_REQUEST, DEFAULT_VERSION, scan_param, new_scanner);
}

