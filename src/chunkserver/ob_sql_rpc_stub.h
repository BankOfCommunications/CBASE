/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_sql_rpc_stub.h
 *
 * Authors:
 *   Junquan Chen <jianming.cjq@taobao.com>
 *
 */

#ifndef _OB_SQL_RPC_STUB_H
#define _OB_SQL_RPC_STUB_H 1

#include "common/ob_rpc_stub.h"
#include "common/ob_new_scanner.h"

namespace oceanbase
{
  using namespace common;

  namespace chunkserver
  {
    class ObSqlRpcStub : public common::ObRpcStub
    {
      public:
        ObSqlRpcStub();
        virtual ~ObSqlRpcStub();

        int get(const int64_t timeout, const ObServer & server, const ObGetParam & get_param, ObNewScanner & new_scanner) const;
        int scan(const int64_t timeout, const ObServer & server, const ObScanParam & scan_param, ObNewScanner & new_scanner) const;
 
    };
  }
}

#endif /* _OB_SQL_RPC_STUB_H */

