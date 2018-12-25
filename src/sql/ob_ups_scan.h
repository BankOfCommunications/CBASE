/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_ups_scan.h
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#ifndef _OB_UPS_SCAN_H
#define _OB_UPS_SCAN_H 1

#include "ob_rowkey_phy_operator.h"
#include "common/ob_string.h"
#include "common/ob_sql_ups_rpc_proxy.h"
#include "common/ob_scan_param.h"
#include "common/ob_range.h"


namespace oceanbase
{
  using namespace common;

  namespace sql
  {
    namespace test
    {
      class ObTabletScanTest_create_plan_not_join_Test;
      class ObTabletScanTest_create_plan_join_Test;
      class ObFakeUpsMultiGet;
    }

    // 用于CS从UPS扫描一批动态数据
    class ObUpsScan: public ObRowkeyPhyOperator
    {
      friend class test::ObTabletScanTest_create_plan_not_join_Test;
      friend class test::ObTabletScanTest_create_plan_join_Test;
      friend class test::ObFakeUpsMultiGet;

      public:
        ObUpsScan();
        virtual ~ObUpsScan();
        virtual void reset();
        virtual void reuse();
        virtual int set_child(int32_t child_idx, ObPhyOperator &child_operator);
        virtual int64_t to_string(char* buf, const int64_t buf_len) const;

        int set_ups_rpc_proxy(ObSqlUpsRpcProxy *rpc_proxy);
        virtual int add_column(const uint64_t &column_id);
        virtual int open();
        virtual int close();
        virtual ObPhyOperatorType get_type() const { return PHY_UPS_SCAN; }
        virtual int get_next_row(const common::ObRowkey *&rowkey, const common::ObRow *&row);
        virtual int get_row_desc(const common::ObRowDesc *&row_desc) const;
        /**
         * 设置要扫描的range
         */
        virtual int set_range(const ObNewRange &range);
        void set_version_range(const ObVersionRange &version_range);
        //add duyr [Delete_Update_Function_isolation] [JHOBv0.1] 20160531:b
        void set_data_mark_param(const ObDataMarkParam &param);
        //add duyr 20160531:e

        bool is_result_empty() const;

        inline void set_is_read_consistency(bool is_read_consistency)
        {
          is_read_consistency_ = is_read_consistency;
        }

        inline int set_ts_timeout_us(int64_t ts_timeout_us)
        {
          int ret = OB_SUCCESS;
          if (ts_timeout_us > 0)
          {
            ts_timeout_us_ = ts_timeout_us;
          }
          else
          {
            ret = OB_INVALID_ARGUMENT;
          }
          return ret;
        }

        inline bool is_timeout(int64_t *remain_us /*= NULL*/) const
        {
          int64_t now = tbsys::CTimeUtil::getTime();
          if (NULL != remain_us)
          {
            if (OB_LIKELY(ts_timeout_us_ > 0))
            {
              *remain_us = ts_timeout_us_ - now;
            }
            else
            {
              *remain_us = INT64_MAX; // no timeout
            }
          }
          return (ts_timeout_us_ > 0 && now > ts_timeout_us_);
        }
        //add zhujun [transaction read uncommit]2016/3/25
        inline int set_trans_id(ObTransID trans_id)
        {
            cur_scan_param_.set_trans_id(trans_id);
            return OB_SUCCESS;
        }
        inline ObTransID get_trans_id() const
        {
            return cur_scan_param_.get_trans_id();
        }
        //add:e

      private:
        // disallow copy
        ObUpsScan(const ObUpsScan &other);
        ObUpsScan& operator=(const ObUpsScan &other);

        int get_next_scan_param(const ObRowkey &last_rowkey, ObScanParam &scan_param);
        int fetch_next(bool first_scan);
        bool check_inner_stat();

      protected:
        // data members
        ObNewRange range_;
        ObNewScanner cur_new_scanner_;
        ObScanParam cur_scan_param_;
        ObUpsRow cur_ups_row_;
        ObRowDesc row_desc_;
        ObStringBuf range_str_buf_;
        ObSqlUpsRpcProxy *rpc_proxy_;
        int64_t ts_timeout_us_;
        int64_t row_counter_;
        bool is_read_consistency_;
    };
  } // end namespace sql
} // end namespace oceanbase

#endif /* _OB_UPS_SCAN_H */
