/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_ups_multi_get.h
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#ifndef _OB_UPS_MULTI_GET_H
#define _OB_UPS_MULTI_GET_H 1

#include "common/ob_define.h"
#include "common/ob_get_param.h"
#include "common/ob_sql_ups_rpc_proxy.h"
#include "ob_rowkey_phy_operator.h"
#include "common/ob_row.h"

namespace oceanbase
{
  using namespace common;
  namespace sql
  {
    // 用于CS从UPS获取多行数据
    class ObUpsMultiGet: public ObRowkeyPhyOperator
    {
      public:
        ObUpsMultiGet();
        virtual ~ObUpsMultiGet();
        virtual void reset();
        virtual void reuse();
        virtual int set_child(int32_t child_idx, ObPhyOperator &child_operator);
        virtual int open();
        virtual int close();
        virtual ObPhyOperatorType get_type() const { return PHY_UPS_MULTI_GET; }
        virtual int get_next_row(const ObRowkey *&rowkey, const ObRow *&row);
        virtual void set_row_desc(const ObRowDesc &row_desc);
        virtual int64_t to_string(char* buf, const int64_t buf_len) const;
        virtual int get_row_desc(const common::ObRowDesc *&row_desc) const;

        inline int set_rpc_proxy(ObSqlUpsRpcProxy *rpc_proxy);

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
        /**
         * 设置MultiGet的参数
         */
        inline void set_get_param(const ObGetParam &get_param);

        //add zhujun[transaction read uncommit]2016/3/28
        inline int set_trans_id(ObTransID trans_id)
        {
            cur_get_param_.set_trans_id(trans_id);
            return OB_SUCCESS;
        }
        inline ObTransID get_trans_id() const
        {
            return cur_get_param_.get_trans_id();
        }
        //add:e

      private:
        // disallow copy
        ObUpsMultiGet(const ObUpsMultiGet &other);
        ObUpsMultiGet& operator=(const ObUpsMultiGet &other);

      protected:
        int next_get_param();
        bool check_inner_stat();

      protected:
        // data members
        const ObGetParam *get_param_;
        ObGetParam cur_get_param_;
        ObNewScanner cur_new_scanner_;
        ObSqlUpsRpcProxy *rpc_proxy_;
        ObUpsRow cur_ups_row_;
        int64_t got_row_count_;
        const ObRowDesc *row_desc_;
        int64_t ts_timeout_us_;
    };

    int ObUpsMultiGet::set_rpc_proxy(ObSqlUpsRpcProxy *rpc_proxy)
    {
      int ret = OB_SUCCESS;
      if(NULL == rpc_proxy)
      {
        ret = OB_INVALID_ARGUMENT;
        TBSYS_LOG(WARN, "rpc_proxy is null");
      }
      else
      {
        rpc_proxy_ = rpc_proxy;
      }
      return ret;
    }

    void ObUpsMultiGet::set_get_param(const ObGetParam &get_param)
    {
      get_param_ = &get_param;
    }
  } // end namespace sql
} // end namespace oceanbase

#endif /* _OB_UPS_MULTI_GET_H */
