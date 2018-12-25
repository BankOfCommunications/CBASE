/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_tablet_scan.h
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#ifndef _OB_TABLET_SCAN_H
#define _OB_TABLET_SCAN_H 1

#include "sql/ob_sstable_scan.h"
#include "sql/ob_ups_scan.h"
#include "sql/ob_tablet_fuse.h"
#include "sql/ob_tablet_direct_join.h"
#include "sql/ob_tablet_cache_join.h"
#include "sql/ob_sql_expression.h"
#include "sql/ob_project.h"
#include "sql/ob_filter.h"
#include "sql/ob_scalar_aggregate.h"
#include "sql/ob_sort.h"
#include "sql/ob_merge_groupby.h"
#include "sql/ob_limit.h"
#include "sql/ob_table_rename.h"
#include "sql/ob_tablet_scan_fuse.h"
#include "common/ob_schema.h"
#include "common/hash/ob_hashset.h"
#include "sql/ob_sql_scan_param.h"
#include "sql/ob_tablet_read.h"

namespace oceanbase
{
  namespace sql
  {
    namespace test
    {
      class ObTabletScanTest_create_plan_not_join_Test;
      class ObTabletScanTest_create_plan_join_Test;
      class ObTabletScanTest_serialize_Test;
    }

    // 用于CS从磁盘扫描一个tablet，合并、join动态数据，并执行计算过滤等
    // @code
    // int cs_handle_scan(...)
    // {
    //   ObTabletScan tablet_scan_op;
    //   ScanResult results;
    //   设置tablet_scan的参数;
    //   tablet_scan_op.open();
    //   const ObRow *row = NULL;
    //   for(tablet_scan_op.get_next_row(row))
    //   {
    //     results.output(row);
    //   }
    //   tablet_scan_op.close();
    //   send_response(results);
    // }
    class ObTabletScan : public ObTabletRead
    {
      friend class test::ObTabletScanTest_create_plan_not_join_Test;
      friend class test::ObTabletScanTest_create_plan_join_Test;
      friend class test::ObTabletScanTest_serialize_Test;

      public:
        ObTabletScan();
        virtual ~ObTabletScan();
        virtual void reset();
        virtual void reuse();

        virtual ObPhyOperatorType get_type() const { return PHY_TABLET_SCAN; }

        inline int get_tablet_range(ObNewRange& range);
        inline int get_scan_range(ObNewRange& range); //add zhaoqiong [Truncate Table]:20160318

        virtual int create_plan(const ObSchemaManagerV2 &schema_mgr);

        bool has_incremental_data() const;
        int64_t to_string(char* buf, const int64_t buf_len) const;
        inline void set_sql_scan_param(const ObSqlScanParam &sql_scan_param);
        void set_scan_context(const ScanContext &scan_context)
        {
          scan_context_ = scan_context;
        }
        //add wenghaixing [secondary index ]20141110
        int build_sstable_scan_param_pub(ObArray<uint64_t> &basic_columns,
            const ObSqlScanParam &sql_scan_param, sstable::ObSSTableScanParam &sstable_scan_param) const;
        //add e
      private:
        // disallow copy
        ObTabletScan(const ObTabletScan &other);
        ObTabletScan& operator=(const ObTabletScan &other);

        bool check_inner_stat() const;

        int need_incremental_data(
            ObArray<uint64_t> &basic_columns,
            ObTabletJoin::TableJoinInfo &table_join_info,
            int64_t start_data_version,
            int64_t end_data_version,
            bool is_truncated = false /*add zhaoqiong [Truncate Table]:20170519 */);

        //del zhaoqiong [Truncate Table]:20170519:b
//        int need_incremental_data(
//            ObArray<uint64_t> &basic_columns,
//            ObTabletJoin::TableJoinInfo &table_join_info,
//            int64_t data_version, ObVersionRange &range); //add zhaoqiong [Truncate Table]:20160318
        //del:e

        int build_sstable_scan_param(ObArray<uint64_t> &basic_columns,
            const ObSqlScanParam &sql_scan_param, sstable::ObSSTableScanParam &sstable_scan_param) const;
        int set_ups_scan_range(const ObNewRange &scan_range);

        int check_incremental_data_range(int64_t table_id,
                                         int64_t start_version,
                                         int64_t end_version,
                                         bool &is_truncated /*mod zhaoqiong [Truncate Table]:20170519 */);

      private:
        // data members
        const ObSqlScanParam *sql_scan_param_;
        ScanContext scan_context_;
        ObSSTableScan op_sstable_scan_;

        /* operator maybe used */
        ObUpsScan op_ups_scan_;
        ObTabletScanFuse op_tablet_scan_fuse_;
        ObTabletDirectJoin op_tablet_join_;
        ObTableRename op_rename_;
        ObFilter op_filter_;
        ObProject op_project_;
        ObScalarAggregate op_scalar_agg_;
        ObMergeGroupBy op_group_;
        ObSort op_group_columns_sort_;
        ObLimit op_limit_;
        ObSort sort_for_listagg_;//add gaojt [ListAgg][JHOBv0.1]20150109

    };

    int ObTabletScan::get_tablet_range(ObNewRange& range)
    {
      int ret = OB_SUCCESS;
      ret = op_sstable_scan_.get_tablet_range(range);
      return ret;
    }
    //add zhaoqiong [Truncate Table]:20160318:b
    int ObTabletScan::get_scan_range(ObNewRange& range)
    {
      int ret = OB_SUCCESS;
      range = *sql_scan_param_->get_range();
      return ret;
    }
    //add:e

    void ObTabletScan::set_sql_scan_param(const ObSqlScanParam &sql_scan_param)
    {
      sql_scan_param_ = &sql_scan_param;
    }
  } // end namespace sql
} // end namespace oceanbase

#endif /* _OB_TABLET_SCAN_H */
