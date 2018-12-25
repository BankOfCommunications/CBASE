/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_fake_sql_ups_rpc_proxy2.h
 *
 * Authors:
 *   Junquan Chen <jianming.cjq@taobao.com>
 *
 */

#ifndef _OB_FAKE_SQL_UPS_RPC_PROXY2_H
#define _OB_FAKE_SQL_UPS_RPC_PROXY2_H 1

#include "common/ob_sql_ups_rpc_proxy.h"
#include "common/ob_string_buf.h"
#include "test_utility.h"

#define TABLE_ID 1000
#define COLUMN_NUMS 8

namespace test
{
  class ObFakeSqlUpsRpcProxyTest2_get_int_test_Test;
  class ObFakeSqlUpsRpcProxyTest2_gen_new_scanner_test1_Test;
  class ObFakeSqlUpsRpcProxyTest2_gen_new_scanner_test2_Test;
}

namespace oceanbase
{
  namespace common
  {
    class ObFakeSqlUpsRpcProxy2 : public ObSqlUpsRpcProxy 
    {
      public:
        ObFakeSqlUpsRpcProxy2();
        virtual ~ObFakeSqlUpsRpcProxy2();

        int sql_ups_get(const ObGetParam & get_param, ObNewScanner & new_scanner, const int64_t timeout);
        int sql_ups_scan(const ObScanParam & scan_param, ObNewScanner & new_scanner, const int64_t timeout);

        void set_mem_size_limit(const int64_t limit);

        int check_incremental_data_range( int64_t table_id, ObVersionRange &version, ObVersionRange &new_range);

        inline void set_column_count(int64_t column_count)
        {
          column_count_ = column_count;
        }

        friend class test::ObFakeSqlUpsRpcProxyTest2_get_int_test_Test;
        friend class test::ObFakeSqlUpsRpcProxyTest2_gen_new_scanner_test1_Test;
        friend class test::ObFakeSqlUpsRpcProxyTest2_gen_new_scanner_test2_Test;

      private:
        // disallow copy
        ObFakeSqlUpsRpcProxy2(const ObFakeSqlUpsRpcProxy2 &other);
        ObFakeSqlUpsRpcProxy2& operator=(const ObFakeSqlUpsRpcProxy2 &other);

        int gen_new_scanner(const ObScanParam & scan_param, ObNewScanner & new_scanner);
        int gen_new_scanner(uint64_t table_id, int64_t start_rowkey, int64_t end_rowkey, ObBorderFlag border_flag, ObNewScanner &new_scanner, bool is_fullfilled);
        int64_t get_int(ObRowkey rowkey);

      private:
        // data members
        ObStringBuf str_buf_;
        CharArena range_buf_;
        int64_t column_count_;
        static const int64_t MAX_ROW_COUNT = 100;
        int64_t mem_size_limit_;
    };
  }
}

#endif /* _OB_FAKE_SQL_UPS_RPC_PROXY2_H */

