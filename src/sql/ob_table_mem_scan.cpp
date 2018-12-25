/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_table_scan.cpp
 *
 * Authors:
 *   Yu Huang <xiaochu.yh@taobao.com>
 *
 */
#include "ob_table_mem_scan.h"
#include "common/utility.h"


namespace oceanbase{
  namespace sql{
    REGISTER_PHY_OPERATOR(ObTableMemScan, PHY_TABLE_MEM_SCAN);
  }
}
namespace oceanbase
{
  namespace sql
  {
    ObTableMemScan::ObTableMemScan() :
      rename_(), project_(), filter_(), limit_(),
      has_rename_(false), has_project_(false), has_filter_(false), has_limit_(false),
      plan_generated_(false)
    {
    }

    ObTableMemScan::~ObTableMemScan()
    {
    }

    void ObTableMemScan::reset()
    {
      rename_.reset();
      project_.reset();
      filter_.reset();
      limit_.reset();
      has_rename_ = false;
      has_project_ = false;
      has_filter_ = false;
      has_limit_ = false;
      plan_generated_ = false;
      ObTableScan::reset();
    }

    void ObTableMemScan::reuse()
    {
      rename_.reuse();
      project_.reuse();
      filter_.reuse();
      limit_.reuse();
      has_rename_ = false;
      has_project_ = false;
      has_filter_ = false;
      has_limit_ = false;
      plan_generated_ = false;
      ObTableScan::reuse();
    }

    int ObTableMemScan::open()
    {
      int ret = OB_SUCCESS;

      if (OB_UNLIKELY(NULL == child_op_))
      {
        ret = OB_NOT_INIT;
      }
      else if (!plan_generated_)
      {
        if (OB_SUCCESS == ret && has_rename_)
        {
          if (OB_SUCCESS != (ret = rename_.set_child(0, *child_op_)))
          {
            TBSYS_LOG(WARN, "fail to set rename child. ret=%d", ret);
          }
          else
          {
            child_op_ = &rename_;
          }
        }
        else
        {
          ret = OB_NOT_INIT;
          TBSYS_LOG(WARN, "must call set_table() before call open(). ret=%d", ret);
        }
        if (OB_SUCCESS == ret && has_project_)
        {
          if (OB_SUCCESS != (ret = project_.set_child(0, *child_op_)))
          {
            TBSYS_LOG(WARN, "fail to set project child. ret=%d", ret);
          }
          else
          {
            child_op_ = &project_;
          }
        }
        if (OB_SUCCESS == ret && has_filter_)
        {
          if (OB_SUCCESS != (ret = filter_.set_child(0, *child_op_)))
          {
            TBSYS_LOG(WARN, "fail to set filter child. ret=%d", ret);
          }
          else
          {
            child_op_ = &filter_;
          }
        }
        if (OB_SUCCESS == ret && has_limit_)
        {
          if (OB_SUCCESS != (ret = limit_.set_child(0, *child_op_)))
          {
            TBSYS_LOG(WARN, "fail to set limit child. ret=%d", ret);
          }
          else
          {
            child_op_ = &limit_;
          }
        }
        plan_generated_ = true;
      }
      if (OB_SUCCESS == ret)
      {
        ret = child_op_->open();
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "fail to open table scan in mem operator. ret=%d", ret);
        }
      }
      return ret;
    }

    int ObTableMemScan::close()
    {
      int ret = OB_SUCCESS;
      if (NULL == child_op_)
      {
        ret = OB_NOT_INIT;
      }
      else
      {
        ret = child_op_->close();

      }
      return ret;
    }

    int ObTableMemScan::get_next_row(const common::ObRow *&row)
    {
      int ret = OB_SUCCESS;
      if (OB_UNLIKELY(NULL == child_op_))
      {
        TBSYS_LOG(ERROR, "child op is NULL");
        ret = OB_NOT_INIT;
      }
      else
      {
        ret = child_op_->get_next_row(row);
      }
      return ret;
    }

    int ObTableMemScan::get_row_desc(const common::ObRowDesc *&row_desc) const
    {
      int ret = OB_SUCCESS;
      if (OB_UNLIKELY(NULL == child_op_))
      {
        TBSYS_LOG(ERROR, "child op is NULL");
        ret = OB_NOT_INIT;
      }
      else
      {
        ret = child_op_->get_row_desc(row_desc);
      }
      return ret;
    }

    int ObTableMemScan::add_output_column(const ObSqlExpression& expr, bool change_tid_for_storing)//mod liumz, [optimize group_order by index]20170419
    {
      UNUSED(change_tid_for_storing);//add liumz, [optimize group_order by index]20170419
      int ret = OB_SUCCESS;
      ret = project_.add_output_column(expr);
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "fail to add project. ret=%d", ret);
      }
      else
      {
        has_project_ = true;
      }
      return ret;
    }

    //add fanqiushi_index
    int ObTableMemScan::add_main_output_column(const ObSqlExpression& expr)
    {
        //todo
        UNUSED(expr);
        return OB_SUCCESS;
    }
    int ObTableMemScan::add_main_filter(ObSqlExpression* expr)
    {
        //todo
        UNUSED(expr);
        return OB_SUCCESS;
    }
    int ObTableMemScan::add_index_filter(ObSqlExpression* expr)
    {
        //todo
        UNUSED(expr);
        return OB_SUCCESS;
    }
    int ObTableMemScan::cons_second_row_desc(ObRowDesc &row_desc)
    {
        //todo
        UNUSED(row_desc);
        return OB_SUCCESS;
    }
    int ObTableMemScan::set_second_row_desc(ObRowDesc *row_desc)
    {
        //todo
        UNUSED(row_desc);
        return OB_SUCCESS;
    }
    //add:e
    //add wanglei [semi join] 20160108:b
    int ObTableMemScan::add_index_filter_ll(ObSqlExpression* expr)
    {
        UNUSED(expr);
        return OB_SUCCESS;
    }
    int ObTableMemScan::add_index_output_column_ll(ObSqlExpression& expr)
    {
        UNUSED(expr);
        return OB_SUCCESS;
    }
    //add:e
    int ObTableMemScan::set_table(const uint64_t table_id, const uint64_t base_table_id)
    {
      int ret = OB_SUCCESS;
      ret = rename_.set_table(table_id, base_table_id);
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "fail to rename table id. ret=%d", ret);
      }
      else
      {
        has_rename_ = true;
      }
      return ret;
    }

    //add zhaoqiong [TableMemScan_Subquery_BUGFIX] 20151118:b
    bool ObTableMemScan::is_base_table_id_valid()
    {
      return rename_.is_base_table_id_valid();
    }
    //add:e
    int ObTableMemScan::add_filter(ObSqlExpression *expr)
    {
      int ret = OB_SUCCESS;
      ret = filter_.add_filter(expr);
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "fail to add filter. ret=%d", ret);
      }
      else
      {
        has_filter_ = true;
      }
      return ret;
    }

    int ObTableMemScan::set_limit(const ObSqlExpression& limit, const ObSqlExpression& offset)
    {
      int ret = OB_SUCCESS;
      if (OB_SUCCESS != (ret = limit_.set_limit(limit, offset)))
      {
        TBSYS_LOG(WARN, "fail to set limit. ret=%d", ret);
      }
      else
      {
        has_limit_ = true;
      }
      return ret;
    }

    //add zhaoqiong [TableMemScan_Subquery_BUGFIX] 20151118:b
    int ObTableMemScan::get_sub_query_index(int32_t idx,int32_t & index)
    {
      return filter_.get_sub_query_index(idx,index);
    }

    int ObTableMemScan::remove_or_sub_query_expr()
    {
      int ret = OB_SUCCESS;
      if(OB_SUCCESS != (ret = filter_.remove_or_sub_query_expr()))
      {
        TBSYS_LOG(WARN, "fail to remove sub query expr filter. ret=%d", ret);
      }
      return ret;
    }

    void ObTableMemScan::update_sub_query_num()
    {
      filter_.update_sub_query_num();
    }


    int ObTableMemScan::copy_filter(ObFilter &object)
    {
      return filter_.copy_filter(object);
    }
    //add:e


    int64_t ObTableMemScan::to_string(char* buf, const int64_t buf_len) const
    {
      int64_t pos = 0;
      databuff_printf(buf, buf_len, pos, "TableMemScan(");
      if (has_project_)
      {
        databuff_printf(buf, buf_len, pos, "project=<");
        pos += project_.to_string(buf+pos, buf_len-pos);
        databuff_printf(buf, buf_len, pos, ">, ");
      }
      if (has_limit_)
      {
        databuff_printf(buf, buf_len, pos, "limit=<");
        pos += limit_.to_string(buf+pos, buf_len-pos);
        databuff_printf(buf, buf_len, pos, ">, ");
      }
      if (has_filter_)
      {
        databuff_printf(buf, buf_len, pos, "filter=<");
        pos += filter_.to_string(buf+pos, buf_len-pos);
        databuff_printf(buf, buf_len, pos, ">, ");
      }
      if (has_rename_)
      {
        databuff_printf(buf, buf_len, pos, "rename=<");
        pos += rename_.to_string(buf+pos, buf_len-pos);
        databuff_printf(buf, buf_len, pos, ">)\n");
      }
      if (NULL != child_op_)
      {
        pos += child_op_->to_string(buf+pos, buf_len-pos);
      }
      return pos;
    }

    PHY_OPERATOR_ASSIGN(ObTableMemScan)
    {
      int ret = OB_SUCCESS;
      CAST_TO_INHERITANCE(ObTableMemScan);
      reset();
      if ((ret = rename_.assign(&o_ptr->rename_)) != OB_SUCCESS)
      {
        TBSYS_LOG(WARN, "Assign rename_ failed, ret=%d", ret);
      }
      else if ((ret = project_.assign(&o_ptr->project_)) != OB_SUCCESS)
      {
        TBSYS_LOG(WARN, "Assign project_ failed, ret=%d", ret);
      }
      else if ((ret = filter_.assign(&o_ptr->filter_)) != OB_SUCCESS)
      {
        TBSYS_LOG(WARN, "Assign filter_ failed, ret=%d", ret);
      }
      else if ((ret = limit_.assign(&o_ptr->limit_)) != OB_SUCCESS)
      {
        TBSYS_LOG(WARN, "Assign limit_ failed, ret=%d", ret);
      }
      else
      {
        has_rename_ = o_ptr->has_rename_;
        has_project_ = o_ptr->has_project_;
        has_filter_ = o_ptr->has_filter_;
        has_limit_ = o_ptr->has_limit_;
        plan_generated_ = o_ptr->plan_generated_;
      }
      return ret;
    }

    DEFINE_SERIALIZE(ObTableMemScan)
    {
      int ret = OB_SUCCESS;
#define ENCODE_OP(has_op, op) \
      if (OB_SUCCESS == ret) \
      { \
        if (OB_SUCCESS != (ret = common::serialization::encode_bool(buf, buf_len, pos, has_op))) \
        { \
          TBSYS_LOG(WARN, "fail to encode " #has_op ":ret[%d]", ret); \
        } \
        else if (has_op) \
        { \
          if (OB_SUCCESS != (ret = op.serialize(buf, buf_len, pos))) \
          { \
            TBSYS_LOG(WARN, "fail to serialize " #op ":ret[%d]", ret); \
          } \
        } \
      }

      ENCODE_OP(has_rename_, rename_);
      ENCODE_OP(has_project_, project_);
      ENCODE_OP(has_filter_, filter_);
      ENCODE_OP(has_limit_, limit_);
#undef ENCODE_OP

      return ret;
    }

    DEFINE_DESERIALIZE(ObTableMemScan)
    {
      int ret = OB_SUCCESS;

#define DECODE_OP(has_op, op) \
      if (OB_SUCCESS == ret) \
      { \
        if (OB_SUCCESS != (ret = common::serialization::decode_bool(buf, data_len, pos, &has_op))) \
        { \
          TBSYS_LOG(WARN, "fail to decode " #has_op ":ret[%d]", ret); \
        } \
        else if (has_op) \
        { \
          if (OB_SUCCESS != (ret = op.deserialize(buf, data_len, pos))) \
          { \
            TBSYS_LOG(WARN, "fail to deserialize " #op ":ret[%d]", ret); \
          } \
        } \
      }
      rename_.reset();
      DECODE_OP(has_rename_, rename_);
      project_.reset();
      DECODE_OP(has_project_, project_);
      filter_.reset();
      DECODE_OP(has_filter_, filter_);
      limit_.reset();
      DECODE_OP(has_limit_, limit_);
#undef DECODE_OP
      return ret;
    }

    DEFINE_GET_SERIALIZE_SIZE(ObTableMemScan)
    {
      int64_t size = 0;
#define GET_OP_SIZE(has_op, op) \
      size += common::serialization::encoded_length_bool(has_op); \
      if (has_op)\
      {\
        size += op.get_serialize_size();\
      }
      GET_OP_SIZE(has_rename_, rename_);
      GET_OP_SIZE(has_project_, project_);
      GET_OP_SIZE(has_filter_, filter_);
      GET_OP_SIZE(has_limit_, limit_);
#undef GET_OP_SIZE
      return size;
    }

    ObPhyOperatorType ObTableMemScan::get_type() const
    {
      return PHY_TABLE_MEM_SCAN;
    }
  } // end namespace sql
} // end namespace oceanbase
