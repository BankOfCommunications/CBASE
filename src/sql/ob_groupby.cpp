/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_groupby.cpp
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#include "ob_groupby.h"
#include "common/utility.h"
using namespace oceanbase::sql;
using namespace oceanbase::common;
using namespace oceanbase::common::serialization;

ObGroupBy::ObGroupBy()
  :mem_size_limit_(0)
{
}

ObGroupBy::~ObGroupBy()
{
}

void ObGroupBy::reset()
{
  group_columns_.clear();
  aggr_columns_.clear();
  partition_columns_.clear();//add liumz, [ROW_NUMBER]20150826
  anal_columns_.clear();//add liumz, [ROW_NUMBER]20150824
  mem_size_limit_ = 0;
  ObSingleChildPhyOperator::reset();
}

void ObGroupBy::reuse()
{
  group_columns_.clear();
  aggr_columns_.clear();
  partition_columns_.clear();//add liumz, [ROW_NUMBER]20150826
  anal_columns_.clear();//add liumz, [ROW_NUMBER]20150824
  mem_size_limit_ = 0;
  ObSingleChildPhyOperator::reuse();
}

void ObGroupBy::set_mem_size_limit(const int64_t limit)
{
  TBSYS_LOG(INFO, "groupby mem limit=%ld", limit);
  mem_size_limit_ = limit;
}

int ObGroupBy::add_group_column(const uint64_t tid, const uint64_t cid)
{
  int ret = OB_SUCCESS;
  ObGroupColumn group_column;
  group_column.table_id_ = tid;
  group_column.column_id_ = cid;
  if (OB_SUCCESS != (ret = group_columns_.push_back(group_column)))
  {
    TBSYS_LOG(WARN, "failed to push back, err=%d", ret);
  }
  return ret;
}

int ObGroupBy::add_aggr_column(const ObSqlExpression& expr)
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS != (ret = aggr_columns_.push_back(expr)))
  {
    TBSYS_LOG(WARN, "failed to push back, err=%d", ret);
  }
  else
  {
    aggr_columns_.at(aggr_columns_.count() - 1).set_owner_op(this);
  }
  return ret;
}

//add liumz, [ROW_NUMBER]20150824:b
int ObGroupBy::add_partition_column(const uint64_t tid, const uint64_t cid)
{
  int ret = OB_SUCCESS;
  ObGroupColumn group_column;
  group_column.table_id_ = tid;
  group_column.column_id_ = cid;
  if (OB_SUCCESS != (ret = partition_columns_.push_back(group_column)))
  {
    TBSYS_LOG(WARN, "failed to push back, err=%d", ret);
  }
  return ret;
}
//add:e

//add liumz, [ROW_NUMBER]20150824:b
int ObGroupBy::add_anal_column(const ObSqlExpression& expr)
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS != (ret = anal_columns_.push_back(expr)))
  {
    TBSYS_LOG(WARN, "failed to push back, err=%d", ret);
  }
  else
  {
    anal_columns_.at(anal_columns_.count() - 1).set_owner_op(this);
  }
  return ret;
}
//add:e

int64_t ObGroupBy::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  databuff_printf(buf, buf_len, pos, "GroupBy(group_cols=[");
  for (int64_t i = 0; i < group_columns_.count(); ++i)
  {
    const ObGroupColumn &g = group_columns_.at(static_cast<int32_t>(i));
    databuff_printf(buf, buf_len, pos, "<%lu,%lu>", g.table_id_, g.column_id_);
    if (i != group_columns_.count() -1)
    {
      databuff_printf(buf, buf_len, pos, ",");
    }
  } // end for
  databuff_printf(buf, buf_len, pos, "], aggr_cols=[");
  for (int64_t i = 0; i < aggr_columns_.count(); ++i)
  {
    const ObSqlExpression &expr = aggr_columns_.at(static_cast<int32_t>(i));
    pos += expr.to_string(buf+pos, buf_len-pos);
    if (i != aggr_columns_.count() -1)
    {
      databuff_printf(buf, buf_len, pos, ",");
    }
  } // end for
  //add liumz, [ROW_NUMBER]20150826
  databuff_printf(buf, buf_len, pos, "], partition_cols=[");
  for (int64_t i = 0; i < partition_columns_.count(); ++i)
  {
    const ObGroupColumn &g = partition_columns_.at(static_cast<int32_t>(i));
    databuff_printf(buf, buf_len, pos, "<%lu,%lu>", g.table_id_, g.column_id_);
    if (i != partition_columns_.count() -1)
    {
      databuff_printf(buf, buf_len, pos, ",");
    }
  } // end for
  //add:e
  //add liumz, [ROW_NUMBER]20150824
  databuff_printf(buf, buf_len, pos, "], anal_cols=[");
  for (int64_t i = 0; i < anal_columns_.count(); ++i)
  {
    const ObSqlExpression &expr = anal_columns_.at(static_cast<int32_t>(i));
    pos += expr.to_string(buf+pos, buf_len-pos);
    if (i != anal_columns_.count() -1)
    {
      databuff_printf(buf, buf_len, pos, ",");
    }
  } // end for
  //add:e
  databuff_printf(buf, buf_len, pos, "])\n");
  if (NULL != child_op_)
  {
    int64_t pos2 = child_op_->to_string(buf+pos, buf_len-pos);
    pos += pos2;
  }
  return pos;
}

PHY_OPERATOR_ASSIGN(ObGroupBy)
{
  int ret = OB_SUCCESS;
  CAST_TO_INHERITANCE(ObGroupBy);
  reset();
  group_columns_ = o_ptr->group_columns_;
  for (int64_t i = 0; i < o_ptr->aggr_columns_.count(); i++)
  {
    if ((ret = aggr_columns_.push_back(o_ptr->aggr_columns_.at(i))) == OB_SUCCESS)
    {
      aggr_columns_.at(i).set_owner_op(this);
    }
    else
    {
      break;
    }
  }
  return ret;
}

DEFINE_SERIALIZE(ObGroupBy)
{
  int ret = OB_SUCCESS;
  ObObj obj;

  obj.set_int(group_columns_.count());
  if (OB_SUCCESS != (ret = obj.serialize(buf, buf_len, pos)))
  {
    TBSYS_LOG(WARN, "fail to group column count. ret=%d", ret);
  }
  else
  {
    for (int64_t i = 0; ret == OB_SUCCESS && i < group_columns_.count(); ++i)
    {
      const ObGroupColumn &group_column = group_columns_.at(i);
      if (OB_SUCCESS != (ret = encode_vi64(buf, buf_len, pos, static_cast<int64_t>(group_column.table_id_)))
        || OB_SUCCESS != (ret = encode_vi64(buf, buf_len, pos, static_cast<int64_t>(group_column.column_id_))))
      {
        TBSYS_LOG(WARN, "serialize group column fail. ret=%d", ret);
      }
    } // end for
  }
  if (ret == OB_SUCCESS)
  {
    obj.set_int(aggr_columns_.count());
    if (OB_SUCCESS != (ret = obj.serialize(buf, buf_len, pos)))
    {
      TBSYS_LOG(WARN, "fail to serialize expr count. ret=%d", ret);
    }
    else
    {
      for (int64_t i = 0; ret == OB_SUCCESS && i < aggr_columns_.count(); ++i)
      {
        const ObSqlExpression &expr = aggr_columns_.at(i);
        if ((OB_SUCCESS != (ret = expr.serialize(buf, buf_len, pos))))
        {
          TBSYS_LOG(WARN, "serialize aggregate column fail. ret=%d", ret);
        }
      } // end for
    }
  }
  if (ret == OB_SUCCESS && (ret = encode_vi64(buf, buf_len, pos, mem_size_limit_)) != OB_SUCCESS)
  {
    TBSYS_LOG(WARN, "serialize memory size limit fail. ret=%d", ret);
  }
  return ret;
}

DEFINE_DESERIALIZE(ObGroupBy)
{
  int ret = OB_SUCCESS;
  int64_t count = 0;
  ObObj obj;
  if (OB_SUCCESS != (ret = obj.deserialize(buf, data_len, pos)))
  {
    TBSYS_LOG(WARN, "fail to deserialize expr count. ret=%d", ret);
  }
  else if (OB_SUCCESS != (ret = obj.get_int(count)))
  {
    TBSYS_LOG(WARN, "fail to get group column count. ret=%d", ret);
  }
  else
  {
    int64_t table_id = 0;
    int64_t column_id = 0;
    for (int64_t i = 0; ret == OB_SUCCESS && i < count; i++)
    {
      if (OB_SUCCESS != (ret = decode_vi64(buf, data_len, pos, &table_id))
        || OB_SUCCESS != (ret = decode_vi64(buf, data_len, pos, &column_id)))
      {
        TBSYS_LOG(WARN, "fail to deserialize group column. ret=%d", ret);
      }
      else if ((ret = add_group_column(static_cast<uint64_t>(table_id),
         static_cast<uint64_t>(column_id))) != OB_SUCCESS)
      {
        TBSYS_LOG(DEBUG, "fail to add group column, table_id=%lu, column_id=%lu. ret=%d",
          static_cast<uint64_t>(table_id), static_cast<uint64_t>(column_id), ret);
      }
    }
  }
  if (OB_UNLIKELY(ret != OB_SUCCESS))
  {
  }
  if (OB_SUCCESS != (ret = obj.deserialize(buf, data_len, pos)))
  {
    TBSYS_LOG(WARN, "fail to deserialize aggregate function count. ret=%d", ret);
  }
  else if (OB_SUCCESS != (ret = obj.get_int(count)))
  {
    TBSYS_LOG(WARN, "fail to get aggregate function count. ret=%d", ret);
  }
  else
  {
    for (int64_t i = 0; ret == OB_SUCCESS && i < count; i++)
    {
      ObSqlExpression expr;
      if (OB_SUCCESS != (ret = expr.deserialize(buf, data_len, pos)))
      {
        TBSYS_LOG(WARN, "fail to deserialize expression. ret=%d", ret);
      }
      else if (OB_SUCCESS != (ret = add_aggr_column(expr)))
      {
        TBSYS_LOG(DEBUG, "fail to add aggregate function,ret=%d. buf=%p, data_len=%ld, pos=%ld",
          ret, buf, data_len, pos);
      }
    }
  }
  if (ret == OB_SUCCESS && (ret = decode_vi64(buf, data_len, pos, &mem_size_limit_)) != OB_SUCCESS)
  {
    TBSYS_LOG(WARN, "fail to deserialize memory size limit. ret=%d", ret);
  }
  return ret;
}

DEFINE_GET_SERIALIZE_SIZE(ObGroupBy)
{
  int64_t size = 0;
  ObObj obj;
  obj.set_int(group_columns_.count());
  size += obj.get_serialize_size();
  for (int64_t i = 0; i < group_columns_.count(); ++i)
  {
    const ObGroupColumn &group_column = group_columns_.at(i);
    size += encoded_length_vi64(group_column.table_id_);
    size += encoded_length_vi64(group_column.column_id_);
  }
  obj.set_int(aggr_columns_.count());
  size += obj.get_serialize_size();
  for (int64_t i = 0; i < aggr_columns_.count(); ++i)
  {
    const ObSqlExpression &expr = aggr_columns_.at(i);
    size += expr.get_serialize_size();
  }
  size += encoded_length_vi64(mem_size_limit_);
  return size;
}
