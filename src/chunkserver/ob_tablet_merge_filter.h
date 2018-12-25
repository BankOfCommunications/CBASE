/**
 * (C) 2010-2012 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *  
 * ob_tablet_merge_filter.h for expire row of tablet when do 
 * daily merge. 
 *
 * Authors:
 *   huating <huating.zmq@taobao.com>
 *
 */
#ifndef OCEANBASE_CHUNKSERVER_OB_TABLET_MERGE_FILTER_H_
#define OCEANBASE_CHUNKSERVER_OB_TABLET_MERGE_FILTER_H_

#include "common/ob_bitmap.h"
#include "common/ob_simple_filter.h"
#include "common/ob_schema.h"
#include "common/ob_scan_param.h"
#include "common/ob_string.h"
#include "common/page_arena.h"
#include "common/hash/ob_hashmap.h"
#include "sstable/ob_sstable_row.h"
#include "sql/ob_sql_expression.h"
#include "sql/ob_sql_scan_param.h"
#include "ob_tablet.h"

namespace oceanbase
{
  namespace chunkserver
  {
    class ObTabletMergerFilter
    {
      public:
        ObTabletMergerFilter();
        ~ObTabletMergerFilter();

        /**
         * initialize tablet merge filter, before daily merge each 
         * tablet, call this function first. 
         * 
         * @param schema the current schema when do daily merge
         * @param tablet the tablet to do daily merge
         * @param frozen_version the current frozen version of daily 
         *                       merge
         * @param frozen_time the current frozen time of daily merge
         * 
         * @return int if success, return OB_SUCCESS, else return 
         *         OB_ERROR
         */
        int init(const common::ObSchemaManagerV2& schema, 
            const int64_t column_group_num, ObTablet* tablet, 
            const int64_t frozen_version, const int64_t frozen_time);

        /**
         * for each column group, after add the all the columns into 
         * scan param to scan, call this function to add some extra 
         * columns which are reuqired by expire condition, but these 
         * columns doesn't belongs to the column group to do daily 
         * merge. this function only to adjust the scan param of the 
         * first column group in the tablet. 
         * 
         * @param column_group_idx the column group index to do daily 
         *                         merge
         * @param column_group_id the column group id to do daily merge
         * @param scan_param the scan param stored the columns in the 
         *                   column group to do daily merge
         * 
         * @return int if success, return OB_SUCCESS, else return 
         *         OB_ERROR
         */
        int adjust_scan_param(const int64_t column_group_idx, 
          const uint64_t column_group_id, common::ObScanParam& scan_param);

        /**
         * for ObTabletMergerV2
         */
        int adjust_scan_param(sql::ObSqlScanParam& scan_param);

        /**
         * for each row of every column group, call this fucntion to 
         * check if it's expired 
         * 
         * @param column_group_idx the column group index to do daily 
         *                         merge
         * @param row_num the row number of current row
         * @param sstable_row the sstable row data
         * @param is_expired [out] if the current row is expired
         * 
         * @return int if the current row is expired, return true, else 
         *         return false
         */
        int check_and_trim_row(const int64_t column_group_idx, const int64_t row_num, 
          sstable::ObSSTableRow& sstable_row, bool& is_expired);

        // reset tablet merge filter, for each tablet, call it once
        void reset();

        // clear expire bitmap, for each splited tablet, call it once
        // if one tablet splits to 5 tablets, call it 5 times 
        void clear_expire_bitmap();

        // whether it need do expire filter, this function must be called
        // after init()
        inline bool need_filter() const 
        {
          return need_filter_;
        }

      private:
        bool need_filter(const int64_t frozen_version, 
          const int64_t last_do_expire_version);
        int fill_cname_to_idx_map(const uint64_t column_group_id);
        bool check_expire_condition_need_filter(const ObTableSchema& table_schema) const;
        //mod  peiouya [EXPIRE_INFO_NOT_ARITHMETIC_FIX] 20160608:b
        //int check_expire_dependent_columns(const ObString& expr,
        //  const ObTableSchema& table_schema, ObExpressionParser& parser) const;
        int check_expire_dependent_columns(const                      ObString& expr,
                                           const ObTableSchema&       table_schema, 
                                           ObExpressionParser&        parser,
                                           bool                       is_expire_info = false) const;
        int replace_system_variable(char* expire_condition, const int64_t buf_size) const;
        //int infix_str_to_sql_expression(const common::ObString& table_name, 
        //    const common::ObString& cond_expr, sql::ObSqlExpression& sql_expr);
        int infix_str_to_sql_expression(const common::ObString&       table_name, 
                                        const common::ObString&       cond_expr, 
                                        sql::ObSqlExpression&         sql_expr,
                                        const bool                    is_expire_info = false);
        int trans_op_func_obj(const int64_t type, common::ObObj &obj_value1, common::ObObj &obj_value2);
        //int infix_to_postfix(const common::ObString& expr, common::ObScanParam& scan_param, 
        //  common::ObObj* objs, const int64_t obj_count);
        int infix_to_postfix(const common::ObString&       expr, 
                             common::ObScanParam&          scan_param, 
                             common::ObObj*                objs, 
                             const int64_t                 obj_count,
                             const bool                    is_expire_info = false);
        ////mod  peiouya [EXPIRE_INFO_NOT_ARITHMETIC_FIX] 20160608:e
        int add_extra_basic_column(common::ObScanParam& scan_param, 
          const common::ObString& column_name, int64_t& column_index);
        int add_extra_composite_column(common::ObScanParam& scan_param,
          const common::ObObj* post_expr);
        int build_expire_condition_filter(common::ObScanParam& scan_param);
        int build_expire_condition_filter(sql::ObSqlExpression& sql_expr, bool& has_filter);
        int finish_expire_filter(sql::ObSqlExpression& sql_expr, 
            const bool has_column_filter, const bool has_condition_filter);

      private:
        DISALLOW_COPY_AND_ASSIGN(ObTabletMergerFilter);

        static const int64_t HASH_MAP_BUCKET_NUM = 512;
        static const int64_t DEFAULT_SSTABLE_ROW_COUNT = 1000000;
        static const int64_t MAX_COMPOSITE_CNAME_LEN = 64;
        static const char* EXPIRE_CONDITION_CNAME_PREFIX;

        typedef common::hash::ObHashMap<common::ObString, int64_t, 
          common::hash::NoPthreadDefendMode> HashMap;
        typedef common::ObBitmap<char, common::ModuleArena> ExpireRowFilter;

        bool inited_;
        bool hash_map_inited_;

        // transfer infix expression to post expression
        HashMap cname_to_idx_map_;
        common::ObExpressionParser post_expression_parser_;
        common::ObSimpleFilter expire_filter_;  //check whether row is expired

        // bitmap of column group row, if tablet includes serval column
        // group, just calculate the bitmap once
        common::ModuleArena expire_filter_allocator_;
        ExpireRowFilter expire_row_filter_;
        
        // current daily merge info and tablet info
        const common::ObSchemaManagerV2* schema_;
        uint64_t table_id_;
        int64_t frozen_version_;
        int64_t frozen_time_;
        int64_t column_group_num_;  // how many column groups in tablet
        int64_t extra_column_cnt_;  // how many extra columns are added into scan param for filter
        bool need_filter_;
    };
  } // namespace oceanbase::chunkserver
} // namespace Oceanbase

#endif // OCEANBASE_CHUNKSERVER_OB_TABLET_MERGE_FILTER_H_
