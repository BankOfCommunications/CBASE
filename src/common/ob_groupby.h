/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * ob_groupby.h is for what ...
 *
 * Version: $id: ob_groupby.h,v 0.1 3/24/2011 11:51a wushi Exp $
 *
 * Authors:
 *   wushi <wushi.ly@taobao.com>
 *     - some work details if you want
 *
 */
#ifndef OCEANBASE_COMMON_OB_GROUPBY_H_
#define OCEANBASE_COMMON_OB_GROUPBY_H_

#include "ob_string.h"
#include "ob_object.h"
#include "ob_string_buf.h"
#include "ob_cell_array.h"
#include "ob_array_helper.h"
#include "ob_composite_column.h"
#include "ob_simple_condition.h"
#include "ob_simple_filter.h"
#include <vector>

namespace oceanbase
{
  namespace common
  {
    extern const char * GROUPBY_CLAUSE_HAVING_COND_AS_CNAME_PREFIX;
    /// we do not implement avg, because avg can be calculated by client using SUM and COUNT
    typedef enum
    {
      AGG_FUNC_MIN,
      SUM,
      COUNT,
      MAX,
      MIN,
      LISTAGG,//add gaojt [ListAgg][JHOBv0.1]20150104
      AGG_FUNC_END
    }ObAggregateFuncType;
    /// aggregate column
    class ObAggregateColumn
    {
    public:
      ObAggregateColumn();
      ObAggregateColumn(ObString & org_column_name, ObString & as_column_name,
        const int64_t as_column_idx, const ObAggregateFuncType & func_type);
      ObAggregateColumn(const int64_t org_column_idx, const int64_t as_column_idx, const ObAggregateFuncType & func_type);
      ~ObAggregateColumn();

      /// accessor members
      inline int64_t get_org_column_idx()const
      {
        return org_column_idx_;
      }
      inline int64_t get_as_column_idx()const
      {
        return as_column_idx_;
      }
      inline ObString get_org_column_name()const
      {
        return org_column_name_;
      }
      inline ObString get_as_column_name()const
      {
        return as_column_name_;
      }
      inline ObAggregateFuncType get_func_type()const
      {
        return func_type_;
      }
      int to_str(char *buf, int64_t buf_size, int64_t &pos)const;

      /// calculage aggregate value
      int calc_aggregate_val(oceanbase::common::ObObj & aggregated_val, const oceanbase::common::ObObj & new_val)const;

      bool operator==(const ObAggregateColumn &other)const;

      int get_first_aggregate_obj(const oceanbase::common::ObObj & first_obj,
        oceanbase::common::ObObj & agg_obj)const;
      int init_aggregate_obj(oceanbase::common::ObObj & agg_obj)const;
    private:
      oceanbase::common::ObString org_column_name_;
      oceanbase::common::ObString as_column_name_;
      int64_t                     org_column_idx_;
      int64_t                     as_column_idx_;
      ObAggregateFuncType         func_type_;
    };

    class ObGroupByParam;
    /// key of group decided by groupby columns
    class ObGroupKey
    {
    public:
      enum
      {
        INVALID_KEY_TYPE,
        ORG_KEY,
        AGG_KEY
      };
      ObGroupKey();
      ~ObGroupKey();
      /// init key
      int init(const ObCellArray & cell_array, const ObGroupByParam & param,
        const int64_t row_beg, const int64_t row_end, const int32_t type);
      /// get hash value of the key
      inline uint64_t hash() const
      {
        return static_cast<uint64_t>(hash_val_);
      }
      /// check if two key is equal
      bool operator ==(const ObGroupKey &other)const;
      bool equal_to(const ObGroupKey &other) const;

      int64_t to_string(char* buffer, const int64_t length) const;

      inline int get_key_type()const
      {
        return key_type_;
      }
      inline int64_t get_row_beg() const
      {
        return row_beg_;
      }
      inline int64_t get_row_end() const
      {
        return row_end_;
      }
      inline const ObGroupByParam * get_groupby_param() const
      {
        return group_by_param_;
      }
      inline const oceanbase::common::ObCellArray * get_cell_array() const
      {
        return cell_array_;
      }

      inline const uint32_t get_hash_val()const
      {
        return hash_val_;
      }

    private:
      void initialize();
      uint32_t hash_val_;
      int32_t  key_type_;
      const ObGroupByParam *group_by_param_;
      const oceanbase::common::ObCellArray *cell_array_;
      int64_t row_beg_;
      int64_t row_end_;
    };

    class ObCompositeColumn;
    /// groupby parameter
    class ObGroupByParam
    {
    public:
      ObGroupByParam(bool deep_copy_args = true);
      ~ObGroupByParam();
      void reset(bool deep_copy_args = true);

      /// groupby columns were used to divide groups, if column_name is NULL, means all result is a single group, e.x. count(*)
      int add_groupby_column(const oceanbase::common::ObString & column_name, bool is_return = true);
      /// column_idx == -1 means all result is a single group, e.x. count(*)
      int add_groupby_column(const int64_t column_idx, bool is_return = true);

      /// the return columns' values of a group were only decided by first row fetched belong to this group
      int add_return_column(const oceanbase::common::ObString & column_name, bool is_return = true);
      int add_return_column(const int64_t column_idx, bool is_return = true);

      /// add an aggregate column
      static oceanbase::common::ObString COUNT_ROWS_COLUMN_NAME;
      int add_aggregate_column(const oceanbase::common::ObString & org_column_name,
        const oceanbase::common::ObString & as_column_name, const ObAggregateFuncType  aggregate_func,
        bool is_return = true);
      int add_aggregate_column(const int64_t org_column_idx, const ObAggregateFuncType  aggregate_func, bool is_return = true);

      /// add composite columns
      int add_column(const ObString & expr, const ObString & as_name, bool is_return = true);
      int add_column(const ObObj *expr, bool is_return = true);
      int add_having_cond(const ObString & expr);

      int add_having_cond(const ObString & column_name, const ObLogicOperator & cond_op, const ObObj & cond_value);

      inline void set_all_column_return()
      {
        for (int64_t i = 0; i < return_infos_.get_array_index();i ++)
        {
          *(return_infos_.at(i))  = true;
        }
      }

      /// calculate the group key of a row, each group decided by groupby columns has a uniq group key,
      /// group key was composited by all column values of grouby columns,
      /// if groupby columns indicate that this param is something like count(*), this function will give same result for all rows
      static int calc_group_key_hash_val(const ObGroupKey & key, uint32_t &val);
      static bool is_equal(const ObGroupKey & left, const ObGroupKey &right);

      /// calculate aggregate result
      /// org_cells [in] result after merge & join  [org_row_beg, org_row_end]
      /// aggregate_cells [in/out] aggregate result row of current group [aggregate_row_beg,aggregate_row_end]
      int aggregate(const oceanbase::common::ObCellArray & org_cells,  const int64_t org_row_beg,
        const int64_t org_row_end, oceanbase::common::ObCellArray & aggregate_cells,
        const int64_t aggregate_row_beg, const int64_t aggregate_row_end)const;

      int serialize(char* buf, const int64_t buf_len, int64_t& pos) const;
      int deserialize(const char* buf, const int64_t data_len, int64_t& pos);
      int64_t get_serialize_size(void) const;

      bool operator == (const ObGroupByParam & other)const;

      inline int64_t get_aggregate_row_width()const
      {
        return column_num_;
      }

      int64_t get_returned_column_num();

      struct ColumnInfo
      {
        oceanbase::common::ObString column_name_;
        int64_t                     org_column_idx_;
        int64_t                     as_column_idx_;
        ColumnInfo()
        {
          org_column_idx_ = -1;
          as_column_idx_ = -1;
        }
        bool operator == (const ColumnInfo & other) const
        {
          return(column_name_ == other.column_name_
            && org_column_idx_ == other.org_column_idx_
            && as_column_idx_ == other.as_column_idx_);
        }
        int to_str(char *buf, int64_t buf_size, int64_t &pos)const
        {
          int err = OB_SUCCESS;
          if ((NULL == buf) || (0 >= buf_size) || (pos >= buf_size))
          {
            TBSYS_LOG(WARN,"argument error [buf:%p,buf_size:%ld, pos:%ld]", buf, buf_size, pos);
            err = OB_INVALID_ARGUMENT;
          }
          if (OB_SUCCESS == err)
          {
            int64_t used_len = 0;
            if ((org_column_idx_ < 0) && (pos < buf_size))
            {
              used_len = snprintf(buf + pos, (buf_size-pos>0)?(buf_size-pos):0, "%.*s,", column_name_.length(), column_name_.ptr());
            }
            else if (pos < buf_size)
            {
              used_len = snprintf(buf + pos, (buf_size-pos>0)?(buf_size-pos):0, "idx:%ld,", org_column_idx_);
            }
            if (used_len > 0)
            {
              pos += used_len;
            }
          }
          return err;
        }
      };

      inline const oceanbase::common::ObArrayHelper<ColumnInfo> & get_groupby_columns(void)const
      {
        return group_by_columns_;
      }

      inline const oceanbase::common::ObArrayHelper<ColumnInfo> & get_return_columns(void)const
      {
        return return_columns_;
      }

      inline const oceanbase::common::ObArrayHelper<ObAggregateColumn> & get_aggregate_columns(void)const
      {
        return aggregate_columns_;
      }

      inline const oceanbase::common::ObArrayHelper<ObCompositeColumn> & get_composite_columns(void)const
      {
        return groupby_comp_columns_;
      }

      inline const oceanbase::common::ObArrayHelpers<bool> & get_return_infos(void)const
      {
        return return_infos_;
      }

      inline const ObSimpleFilter & get_having_condition(void)const
      {
        return condition_filter_;
      }

      inline  ObSimpleFilter & get_having_condition(void)
      {
        return condition_filter_;
      }


      int64_t find_column(const oceanbase::common::ObString & column_name)const;

      int  get_aggregate_column_name(const int64_t column_idx, ObString & column_name)const;

      int safe_copy(const ObGroupByParam & other);
    private:
      int64_t serialize_helper(char* buf, const int64_t buf_len, int64_t & pos) const;
      int64_t groupby_columns_serialize_helper(char* buf, const int64_t buf_len, int64_t & pos) const;
      int64_t return_columns_serialize_helper(char* buf, const int64_t buf_len, int64_t & pos) const;
      int64_t aggregate_columns_serialize_helper(char* buf, const int64_t buf_len, int64_t & pos) const;
      int comp_columns_serialize_helper(char* buf, const int64_t buf_len, int64_t & pos) const;
      int64_t comp_columns_get_serialize_size(void)const;
      int return_info_serialize_helper(char* buf, const int64_t buf_len, int64_t & pos) const;
      int64_t return_info_get_serialize_size(void)const;
      int having_condition_serialize_helper(char* buf, const int64_t buf_len, int64_t & pos)const;
      int64_t having_condition_get_serialize_size(void)const;
      int deserialize_groupby_columns(const char *buf, const int64_t buf_len, int64_t &pos);
      int deserialize_return_columns(const char *buf, const int64_t buf_len, int64_t &pos);
      int deserialize_aggregate_columns(const char *buf, const int64_t buf_len, int64_t &pos);
      int deserialize_comp_columns(const char *buf, const int64_t buf_len, int64_t &pos);
      int deserialize_return_info(const char *buf, const int64_t buf_len, int64_t &pos);
      int deserialize_having_condition(const char *buf, const int64_t buf_len, int64_t &pos);

      int calc_org_group_key_hash_val(const ObCellArray & cells, const int64_t row_beg, const int64_t row_end, uint32_t &val)const;
      int calc_agg_group_key_hash_val(const ObCellArray & cells, const int64_t row_beg, const int64_t row_end, uint32_t &val)const;

      int64_t get_target_cell_idx(const ObGroupKey & key, const int64_t groupby_idx)const;

      int malloc_composite_columns();


      mutable bool using_id_;
      mutable bool using_name_;
      int64_t                         column_num_;
      ColumnInfo                      group_by_columns_buf_[OB_MAX_COLUMN_NUMBER];
      ObArrayHelper<ColumnInfo>       group_by_columns_;
      bool gc_return_infos_buf_[OB_MAX_COLUMN_NUMBER];
      ObArrayHelper<bool> gc_return_infos_;
      ColumnInfo                      return_columns_buf_[OB_MAX_COLUMN_NUMBER];
      ObArrayHelper<ColumnInfo>       return_columns_;
      bool rc_return_infos_buf_[OB_MAX_COLUMN_NUMBER];
      ObArrayHelper<bool> rc_return_infos_;
      ObAggregateColumn               aggregate_columns_buf_[OB_MAX_COLUMN_NUMBER];
      ObArrayHelper<ObAggregateColumn> aggregate_columns_;
      bool ac_return_infos_buf_[OB_MAX_COLUMN_NUMBER];
      ObArrayHelper<bool> ac_return_infos_;

      ObCompositeColumn                 *groupby_comp_columns_buf_;
      ObArrayHelper<ObCompositeColumn>  groupby_comp_columns_;
      bool cc_return_infos_buf_[OB_MAX_COLUMN_NUMBER];
      ObArrayHelper<bool> cc_return_infos_;

      ObArrayHelpers<bool> return_infos_;

      /// having condition
      ObSimpleFilter condition_filter_;
      ObStringBuf buffer_pool_;
      bool deep_copy_args_;
    };
  }
}
#endif /* OCEANBASE_COMMON_OB_GROUPBY_H_ */
