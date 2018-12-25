/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_tablet_join.h
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#ifndef _OB_TABLET_JOIN_H
#define _OB_TABLET_JOIN_H 1

#include "common/ob_define.h"
#include "common/ob_array.h"
#include "common/ob_cache.h"
#include "ob_phy_operator.h"
#include "common/ob_ups_row.h"
#include "common/ob_kv_storecache.h"
#include "common/ob_get_param.h"
#include "common/ob_row_store.h"
#include "ob_ups_multi_get.h"
#include "common/thread_buffer.h"
#include "ob_tablet_fuse.h"

namespace oceanbase
{
  using namespace common;

  namespace sql
  {
    namespace test
    {
      class ObTabletJoinTest_fetch_ups_row_Test;
      class ObTabletJoinTest_compose_get_param_Test;
      class ObTabletJoinTest_gen_ups_row_desc_Test;
      class ObTabletJoinTest_get_right_table_rowkey_Test;
      class ObTabletJoinTest_fetch_fused_row_Test;

      class ObTabletScanTest_create_plan_join_Test;
      class ObTabletScanTest_create_plan_not_join_Test;
    }

    class ObTabletJoin: public ObPhyOperator
    {
      public:
        struct JoinInfo
        {
          uint64_t left_column_id_;
          uint64_t right_column_id_;

          JoinInfo();
        };

        struct TableJoinInfo
        {
          uint64_t left_table_id_; //左表table_id
          uint64_t right_table_id_;

          /* join条件信息，左表的column对应于右表的rowkey */
          ObArray<uint64_t> join_condition_;

          /* 指示左表哪些列是从右表取过来的 */
          ObArray<JoinInfo> join_column_;

          int64_t to_string(char* buf, const int64_t buf_len) const
          {
            int64_t pos = 0;
            databuff_printf(buf, buf_len, pos, "left tid[%lu]", left_table_id_);
            databuff_printf(buf, buf_len, pos, "right tid[%lu]", right_table_id_);
            databuff_printf(buf, buf_len, pos, " join condtion:");
            for (int64_t i = 0; i < join_condition_.count(); i ++)
            {
              databuff_printf(buf, buf_len, pos, "%lu, ", join_condition_.at(i));
            }
            databuff_printf(buf, buf_len, pos, " join column:");
            for (int64_t i = 0; i < join_column_.count(); i ++)
            {
              databuff_printf(buf, buf_len, pos, "<%lu, %lu>, ", 
                join_column_.at(i).left_column_id_, join_column_.at(i).right_column_id_);
            }
            return pos;
          }

          public:
            TableJoinInfo();
        };


      public:
        ObTabletJoin();
        virtual ~ObTabletJoin();
        virtual void reset();
        virtual void reuse();
        int set_child(int32_t child_idx, ObPhyOperator &child_operator);
        virtual int open();
        virtual int close();
        int get_next_row(const ObRow *&row);
        int64_t to_string(char* buf, const int64_t buf_len) const;
        int get_row_desc(const common::ObRowDesc *&row_desc) const;
        void set_ts_timeout_us(int64_t ts_timeout_us);
        inline void set_batch_count(const int64_t batch_count);
        inline void set_table_join_info(const TableJoinInfo &table_join_info);
        inline void set_is_read_consistency(bool is_read_consistency)
        {
          is_read_consistency_ = is_read_consistency;
        }

        inline int set_rpc_proxy(ObSqlUpsRpcProxy *rpc_proxy)
        {
          return ups_multi_get_.set_rpc_proxy(rpc_proxy);
        }

        inline void set_version_range(const ObVersionRange &version_range)
        {
          version_range_ = version_range;
        }

        friend class test::ObTabletScanTest_create_plan_join_Test;
        friend class test::ObTabletScanTest_create_plan_not_join_Test;
        friend class test::ObTabletJoinTest_compose_get_param_Test;
        friend class test::ObTabletJoinTest_gen_ups_row_desc_Test;
        friend class test::ObTabletJoinTest_get_right_table_rowkey_Test;
        friend class test::ObTabletJoinTest_fetch_fused_row_Test;

      protected:
        // disallow copy
        ObTabletJoin(const ObTabletJoin &other);
        ObTabletJoin& operator=(const ObTabletJoin &other);

        int get_right_table_rowkey(const ObRow &row, ObRowkey &rowkey, ObObj *rowkey_obj) const;
        int compose_get_param(uint64_t table_id, const ObRowkey &rowkey, ObGetParam &get_param);
        bool check_inner_stat();

        int fetch_next_batch_row(ObGetParam *get_param);
        int fetch_fused_row(ObGetParam *get_param);

        int gen_ups_row_desc();

        virtual int fetch_fused_row_prepare();
        virtual int get_ups_row(const ObRowkey &rowkey, ObUpsRow &ups_row, const ObGetParam &get_param) = 0;
        virtual int gen_get_param(ObGetParam &get_param, const ObRow &fused_row) = 0;

      protected:
        // data members
        TableJoinInfo table_join_info_;
        int64_t batch_count_;
        bool    fused_row_iter_end_;
        ObTabletFuse *fused_scan_;
        ObUpsMultiGet ups_multi_get_;
        ObRowStore fused_row_store_;
        ThreadSpecificBuffer thread_buffer_;
        ObRowDesc ups_row_desc_;
        ObRowDesc ups_row_desc_for_join_;
        ObRow curr_row_;
        const ObRow *fused_row_;
        ObArray<const ObRowStore::StoredRow *> fused_row_array_;
        int64_t fused_row_idx_;
        bool is_read_consistency_;
        int64_t valid_fused_row_count_;
        ObVersionRange version_range_;
    };

    void ObTabletJoin::set_table_join_info(const TableJoinInfo &table_join_info)
    {
      table_join_info_ = table_join_info;
    }

    void ObTabletJoin::set_batch_count(const int64_t batch_count)
    {
      batch_count_ = batch_count;
    }

  } // end namespace sql
} // end namespace oceanbase

#endif /* _OB_TABLET_JOIN_H */
