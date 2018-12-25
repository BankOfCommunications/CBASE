/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * ob_groupby.cpp is for what ...
 *
 * Version: $id: ob_groupby.cpp,v 0.1 3/24/2011 11:51a wushi Exp $
 *
 * Authors:
 *   wushi <wushi.ly@taobao.com>
 *     - some work details if you want
 *
 */
#include "ob_groupby.h"
#include "ob_define.h"
#include "murmur_hash.h"
#include "ob_action_flag.h"
using namespace oceanbase;
using namespace oceanbase::common;
namespace oceanbase
{
  namespace common
  {
    const char * GROUPBY_CLAUSE_HAVING_COND_AS_CNAME_PREFIX = "__HAVING_";
  }
}

oceanbase::common::ObAggregateColumn::ObAggregateColumn()
{
  org_column_idx_ = -1;
  as_column_idx_ = -1;
  func_type_ = AGG_FUNC_MIN;
}


oceanbase::common::ObAggregateColumn::~ObAggregateColumn()
{
}

oceanbase::common::ObAggregateColumn::ObAggregateColumn(ObString & org_column_name, ObString & as_column_name,
  const int64_t as_column_idx, const ObAggregateFuncType & func_type)
{
  org_column_name_ = org_column_name;
  as_column_name_ = as_column_name;
  org_column_idx_ = -1;
  as_column_idx_ = as_column_idx;
  func_type_ = func_type;
}

oceanbase::common::ObAggregateColumn::ObAggregateColumn(const int64_t org_column_idx,  const int64_t as_column_idx,
  const ObAggregateFuncType & func_type)
{
  org_column_idx_ = org_column_idx;
  as_column_idx_ = as_column_idx;
  func_type_ = func_type;
}

int oceanbase::common::ObAggregateColumn::calc_aggregate_val(ObObj & aggregated_val, const ObObj & new_val)const
{
  int err = OB_SUCCESS;
  switch (func_type_)
  {
  case SUM:
    if (ObNullType == aggregated_val.get_type())
    {
      aggregated_val = new_val;
    }
    else if (ObNullType != new_val.get_type())
    {
      ObObj mutation(new_val);
      err = mutation.set_add(true);
      if (OB_SUCCESS == err)
      {
        err = aggregated_val.apply(mutation);
        if (OB_SUCCESS != err)
        {
          TBSYS_LOG(WARN,"calculate aggregate value error [aggregated_val.type:%d,mutation.type:%d]",
            aggregated_val.get_type(), mutation.get_type());
        }
      }
      else
      {
        TBSYS_LOG(WARN,"fail to set add property of mutation [mutation.type:%d]", mutation.get_type());
      }
    }
    break;
  case COUNT:
    {
      ObObj mutation;
      mutation.set_int(1,true);
      err = aggregated_val.apply(mutation);
      break;
    }
  case MAX:
    if ((ObNullType != new_val.get_type())
      && ((new_val > aggregated_val) || aggregated_val.get_type() == ObNullType)
      )
    {
      aggregated_val = new_val;
    }
    break;
  case MIN:
    if ((ObNullType != new_val.get_type())
      && ((new_val < aggregated_val) || aggregated_val.get_type() == ObNullType)
      )
    {
      aggregated_val = new_val;
    }
    break;
  //add gaojt [ListAgg][JHOBv0.1]20150104:b
  case LISTAGG:
      if ((ObNullType != new_val.get_type())
        && ((new_val < aggregated_val) || aggregated_val.get_type() == ObNullType)
        )
      {
        aggregated_val = new_val;
      }
      break;
  //add f20150104:e
  default:
    TBSYS_LOG(WARN,"unsupported aggregate function type [type:%d]", func_type_);
    err = OB_NOT_SUPPORTED;
    break;
  }
  return err;
}


bool oceanbase::common::ObAggregateColumn::operator==(const ObAggregateColumn &other)const
{
  bool result = false;
  result = (org_column_name_ == other.org_column_name_
    && org_column_idx_ == other.org_column_idx_
    && as_column_name_ == other.as_column_name_
    && as_column_idx_ == other.as_column_idx_
    && func_type_ == other.func_type_);
  return result;
}

int oceanbase::common::ObAggregateColumn::get_first_aggregate_obj(const ObObj & first_obj,
  ObObj & agg_obj)const
{
  int err = OB_SUCCESS;
  switch (func_type_)
  {
  case MAX:
  case MIN:
  case SUM:
  case LISTAGG://add gaojt [ListAgg][JHOBv0.1]20150104
    agg_obj = first_obj;
    break;
  case COUNT:
    agg_obj.set_int(1);
    break;
  default:
    TBSYS_LOG(WARN,"unsupported aggregate function type [type:%d]", func_type_);
    err = OB_NOT_SUPPORTED;
    break;
  }
  return err;
}

int oceanbase::common::ObAggregateColumn::init_aggregate_obj(oceanbase::common::ObObj & agg_obj)const
{
  int err = OB_SUCCESS;
  switch (func_type_)
  {
  case MAX:
  case MIN:
  case SUM:
    agg_obj.set_null();
    break;
  case COUNT:
    agg_obj.set_int(0);
    break;
  default:
    TBSYS_LOG(WARN,"unsupported aggregate function type [type:%d]", func_type_);
    err = OB_NOT_SUPPORTED;
    break;
  }
  return err;
}

int oceanbase::common::ObAggregateColumn::to_str(char *buf, int64_t buf_size, int64_t &pos)const
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
    const char *func_name = NULL;
    switch (func_type_)
    {
    case MAX:
      func_name = "MAX";
      break;
    case MIN:
      func_name = "MIN";
      break;
    case SUM:
      func_name = "SUM";
      break;
    case COUNT:
      func_name = "COUNT";
      break;
    //add gaojt [ListAgg][JHOBv0.1]20150104:b
    case LISTAGG:
      func_name = "LISTAGG";
      break;
    //add 20150104:e
    default:
      func_name = "UNKOWN";
      break;
    }
    if ((pos < buf_size) && ((used_len = snprintf(buf + pos, (buf_size-pos>0)?(buf_size-pos):0, "%s(", func_name)) > 0))
    {
      pos += used_len;
    }
    if ((pos < buf_size)
      && (org_column_idx_ >= 0)
      && ((used_len = snprintf(buf + pos, (buf_size-pos>0)?(buf_size-pos):0, "idx:%ld),", org_column_idx_)) > 0))
    {
      pos += used_len;
    }
    else if ((pos < buf_size)
      && ((used_len = snprintf(buf + pos, (buf_size-pos>0)?(buf_size-pos):0, "%.*s) AS %.*s,",
      org_column_name_.length(), org_column_name_.ptr(), as_column_name_.length(), as_column_name_.ptr())) > 0))
    {
      pos += used_len;
    }
  }
  return err;
}

void oceanbase::common::ObGroupKey::initialize()
{
  hash_val_ = 0;
  key_type_ = INVALID_KEY_TYPE;
  group_by_param_ = NULL;
  cell_array_ = NULL;
  row_beg_ = -1;
  row_end_ = -1;
}

oceanbase::common::ObGroupKey::ObGroupKey()
{
  initialize();
}

oceanbase::common::ObGroupKey::~ObGroupKey()
{
  initialize();
}

int oceanbase::common::ObGroupKey::init(const ObCellArray & cell_array, const ObGroupByParam & param,
  const int64_t row_beg, const int64_t row_end, const int32_t type)
{
  int err = OB_SUCCESS;
  if (ORG_KEY != type && AGG_KEY != type)
  {
    TBSYS_LOG(WARN,"param error, unknow key type [type:%d]", type);
    err = OB_INVALID_ARGUMENT;
  }
  if (OB_SUCCESS == err)
  {
    if (row_beg < 0
      || row_end < 0
      || row_beg > row_end
      || row_end >= cell_array.get_cell_size())
    {
      TBSYS_LOG(WARN,"param error [row_beg:%ld,row_end:%ld,cell_array.get_cell_size():%ld]",
        row_beg, row_end, cell_array.get_cell_size());
      err = OB_INVALID_ARGUMENT;
    }
  }
  if (OB_SUCCESS == err)
  {
    cell_array_ = &cell_array;
    group_by_param_ = &param;
    row_beg_ = row_beg;
    row_end_ = row_end;
    key_type_ = type;
    err = param.calc_group_key_hash_val(*this,hash_val_);
    if (OB_SUCCESS != err)
    {
      TBSYS_LOG(WARN,"fail to calc hash value of current key [err:%d]", err);
    }
  }
  if (OB_SUCCESS != err)
  {
    initialize();
  }
  return err;
}

bool oceanbase::common::ObGroupKey::equal_to(const ObGroupKey &other) const
{
  bool bret = false;

  if (group_by_param_ != other.group_by_param_)
  {
    TBSYS_LOG(WARN,"compared group key have different groupby param, "
      "this.groupby_param=%p, other.groupby_param=%p",
      group_by_param_, other.group_by_param_);
  }
  else if (hash_val_ == other.hash_val_)
  {
    int64_t i = 0;
    int64_t this_idx = -1;
    int64_t that_idx = -1;
    ObObj this_obj;
    const ObArrayHelper<ObGroupByParam::ColumnInfo> &groupby_col =
      group_by_param_->get_groupby_columns();
    for (i = 0; i < groupby_col.get_array_index(); i++)
    {
      if (key_type_ == ObGroupKey::AGG_KEY)
      {
        this_idx = groupby_col.at(i)->as_column_idx_ + row_beg_;
      }
      else if (key_type_ == ObGroupKey::ORG_KEY)
      {
        this_idx = groupby_col.at(i)->org_column_idx_ + row_beg_;
      }
      else
      {
        TBSYS_LOG(WARN,"unrecogonized key type [type:%d]", key_type_);
        break;
      }

      if (other.key_type_ == ObGroupKey::AGG_KEY)
      {
        that_idx = groupby_col.at(i)->as_column_idx_ + other.row_beg_;
      }
      else if (other.key_type_ == ObGroupKey::ORG_KEY)
      {
        that_idx = groupby_col.at(i)->org_column_idx_ + other.row_beg_;
      }
      else
      {
        TBSYS_LOG(WARN,"unrecogonized key type [type:%d]", other.key_type_);
        break;
      }

      if (this_idx < 0 || that_idx < 0)
      {
        TBSYS_LOG(WARN,"unexpected error this_idx=%ld, that_idx=%ld",
          this_idx, that_idx);
        break;
      }
      else
      {
        /**
         * WARNING: maybe cell_array_ and other.cell_array_ are the same
         * cell array, the member method operator[] return a const
         * internal ObCellInfo instance, if we want to do *+-/ < > == >=
         * <= !=, we need use a copy of the returned instance.
         */
        this_obj = (*cell_array_)[this_idx].value_;
        if (this_obj != (*other.cell_array_)[that_idx].value_)
        {
          break;
        }
      }
    }
    if (i == groupby_col.get_array_index())
    {
      bret = true;
    }
  }

  return bret;
}

bool oceanbase::common::ObGroupKey::operator ==(const ObGroupKey &other)const
{
  if (1)
  {
    return equal_to(other);
  }
  else
  {
    return ObGroupByParam::is_equal(*this,other);
  }

}

int64_t oceanbase::common::ObGroupKey::to_string(char* buffer, const int64_t length) const
{
  int64_t pos = 0;
  int64_t max_idx = row_end_ - row_beg_;
  const ObArrayHelper<ObGroupByParam::ColumnInfo>& group_by_columns
    = group_by_param_->get_groupby_columns();
  const ObGroupByParam::ColumnInfo* groupby_col = NULL;

  pos += snprintf(buffer + pos, length - pos, "hash_val_=%u, key_type_=%d, group_by_param_=%p ",
    hash_val_, key_type_, group_by_param_);
  for (int64_t i = 0; i < group_by_columns.get_array_index(); i++)
  {
    groupby_col = group_by_columns.at(i);
    if (groupby_col->as_column_idx_ > max_idx || groupby_col->as_column_idx_ < 0)
    {
      TBSYS_LOG(WARN,"param error [max_idx:%ld,groupby_column_idx:%ld,as_column_idx:%ld]", max_idx, i,
        groupby_col->as_column_idx_);
    }
    else
    {
      if (key_type_ == ObGroupKey::AGG_KEY)
      {
        pos += (*cell_array_)[row_beg_ + groupby_col->as_column_idx_].value_.to_string(buffer + pos, length - pos);
      }
      else if (key_type_ == ObGroupKey::ORG_KEY)
      {
        pos += (*cell_array_)[row_beg_ + groupby_col->org_column_idx_].value_.to_string(buffer + pos, length - pos);
      }
    }
  }

  return pos;
}

oceanbase::common::ObGroupByParam::ObGroupByParam(bool deep_copy_args):
group_by_columns_(sizeof(group_by_columns_buf_)/sizeof(ColumnInfo),group_by_columns_buf_),
  gc_return_infos_(sizeof(gc_return_infos_buf_)/sizeof(gc_return_infos_buf_[0]), gc_return_infos_buf_),
  return_columns_(sizeof(return_columns_buf_)/sizeof(ColumnInfo),return_columns_buf_),
  rc_return_infos_(sizeof(rc_return_infos_buf_)/sizeof(rc_return_infos_buf_[0]), rc_return_infos_buf_),
  aggregate_columns_(sizeof(aggregate_columns_buf_)/sizeof(ObAggregateColumn), aggregate_columns_buf_),
  ac_return_infos_(sizeof(ac_return_infos_buf_)/sizeof(ac_return_infos_buf_[0]), ac_return_infos_buf_),
  cc_return_infos_(sizeof(cc_return_infos_buf_)/sizeof(cc_return_infos_buf_[0]), cc_return_infos_buf_)
{
  column_num_ = 0;
  using_id_ = false;
  using_name_ = false;
  groupby_comp_columns_buf_ = NULL;
  deep_copy_args_ = deep_copy_args;
  return_infos_.add_array_helper(gc_return_infos_);
  return_infos_.add_array_helper(rc_return_infos_);
  return_infos_.add_array_helper(ac_return_infos_);
  return_infos_.add_array_helper(cc_return_infos_);
}


oceanbase::common::ObGroupByParam::~ObGroupByParam()
{
  if (NULL != groupby_comp_columns_buf_)
  {
    ob_free(groupby_comp_columns_buf_);
    groupby_comp_columns_buf_  = NULL;
  }
}


void oceanbase::common::ObGroupByParam::reset(bool deep_copy_args)
{
  column_num_ = 0;
  group_by_columns_.clear();
  return_columns_.clear();
  aggregate_columns_.clear();
  groupby_comp_columns_.clear();
  gc_return_infos_.clear();
  rc_return_infos_.clear();
  ac_return_infos_.clear();
  cc_return_infos_.clear();
  using_id_ = false;
  using_name_ = false;
  ///new(group_by_columns_buf_)ColumnInfo[OB_MAX_COLUMN_NUMBER];
  ///new(return_columns_buf_)ColumnInfo[OB_MAX_COLUMN_NUMBER];
  ///new(aggregate_columns_buf_)ObAggregateColumn[OB_MAX_COLUMN_NUMBER];
  /// if (NULL != groupby_comp_columns_buf_)
  /// {
  ///   new(groupby_comp_columns_buf_)ObCompositeColumn[OB_MAX_COLUMN_NUMBER];
  /// }
  buffer_pool_.reset();
  condition_filter_.reset();
  deep_copy_args_ = deep_copy_args;
}

int oceanbase::common::ObGroupByParam::add_groupby_column(const ObString &column_name, bool is_return)
{
  int err = OB_SUCCESS;
  ObString stored_column_name = column_name;
  ColumnInfo group_by_column;

  if ((OB_SUCCESS == err) && deep_copy_args_)
  {
    err = buffer_pool_.write_string(column_name, &stored_column_name);
    if (OB_SUCCESS != err)
    {
      TBSYS_LOG(WARN,"fail to store column name into local buffer [err:%d]", err);
    }
  }
  if (OB_SUCCESS == err)
  {
    group_by_column.as_column_idx_ = column_num_;
    group_by_column.column_name_ = stored_column_name;
    group_by_column.org_column_idx_ = -1;
    if (!group_by_columns_.push_back(group_by_column))
    {
      err = OB_ARRAY_OUT_OF_RANGE;
      TBSYS_LOG(WARN,"fail to push_back group by column [err:%d]", err);
    }
    else
    {
      column_num_ ++;
    }
  }
  if ((OB_SUCCESS == err ) && (!gc_return_infos_.push_back(is_return)))
  {
    TBSYS_LOG(WARN,"fail to add return info");
    err = OB_ARRAY_OUT_OF_RANGE;
  }
  return err;
}

int oceanbase::common::ObGroupByParam::add_groupby_column(const int64_t column_idx, bool is_return)
{
  int err = OB_SUCCESS;
  ColumnInfo group_by_column;

  if (OB_SUCCESS == err)
  {
    group_by_column.as_column_idx_ = column_num_;
    group_by_column.column_name_.assign(NULL,0);
    group_by_column.org_column_idx_ = column_idx;
    if (!group_by_columns_.push_back(group_by_column))
    {
      err =  OB_ARRAY_OUT_OF_RANGE;
      TBSYS_LOG(WARN,"fail to push_back group by column [err:%d]", err);
    }
    else
    {
      column_num_ ++;
    }
  }
  if ((OB_SUCCESS == err ) && (!gc_return_infos_.push_back(is_return)))
  {
    TBSYS_LOG(WARN,"fail to add return info");
    err = OB_ARRAY_OUT_OF_RANGE;
  }
  return err;
}

int oceanbase::common::ObGroupByParam::add_return_column(const ObString & column_name, bool is_return)
{
  int err = OB_SUCCESS;
  ObString stored_column_name = column_name;
  ColumnInfo return_column;

  if (OB_SUCCESS == err && deep_copy_args_)
  {
    err = buffer_pool_.write_string(column_name, &stored_column_name);
    if (OB_SUCCESS != err)
    {
      TBSYS_LOG(WARN,"fail to store column name into local buffer [err:%d]", err);
    }
  }
  if (OB_SUCCESS == err)
  {
    return_column.as_column_idx_ = column_num_;
    return_column.column_name_ = stored_column_name;
    return_column.org_column_idx_ = -1;
    if (!return_columns_.push_back(return_column))
    {
      err =  OB_ARRAY_OUT_OF_RANGE;
      TBSYS_LOG(WARN,"fail to push_back return column [err:%d]", err);
    }
    else
    {
      column_num_ ++;
    }
  }
  if ((OB_SUCCESS == err ) && (!rc_return_infos_.push_back(is_return)))
  {
    TBSYS_LOG(WARN,"fail to add return info");
    err = OB_ARRAY_OUT_OF_RANGE;
  }
  return err;
}

int oceanbase::common::ObGroupByParam::add_return_column(const int64_t column_idx, bool is_return)
{
  int err = OB_SUCCESS;
  ColumnInfo return_column;

  if (OB_SUCCESS == err)
  {
    return_column.as_column_idx_ = column_num_;
    return_column.column_name_.assign(NULL,0);
    return_column.org_column_idx_ = column_idx;
    if (!return_columns_.push_back(return_column))
    {
      err =  OB_ARRAY_OUT_OF_RANGE;
      TBSYS_LOG(WARN,"fail to push_back return column [err:%d]", err);
    }
    else
    {
      column_num_ ++;
    }
  }
  if ((OB_SUCCESS == err ) && (!rc_return_infos_.push_back(is_return)))
  {
    TBSYS_LOG(WARN,"fail to add return info");
    err = OB_ARRAY_OUT_OF_RANGE;
  }
  return err;
}

ObString oceanbase::common::ObGroupByParam::COUNT_ROWS_COLUMN_NAME = ObString(static_cast<int32_t>(strlen("*")),
    static_cast<int32_t>(strlen("*")), const_cast<char*>("*"));

int oceanbase::common::ObGroupByParam::add_aggregate_column(const ObString & org_column_name, const ObString &as_column_name,
  const ObAggregateFuncType aggregate_func, bool is_return)
{
  int err = OB_SUCCESS;
  ObString stored_org_column_name = org_column_name;
  ObString stored_as_column_name = as_column_name;

  if (OB_SUCCESS == err)
  {
    if (aggregate_func >= AGG_FUNC_END || aggregate_func <= AGG_FUNC_MIN)
    {
      TBSYS_LOG(WARN,"param error, unrecogonized aggregate function type [type:%d]", aggregate_func);
      err = OB_INVALID_ARGUMENT;
    }
  }
  if (OB_SUCCESS == err && deep_copy_args_)
  {
    err = buffer_pool_.write_string(org_column_name, &stored_org_column_name);
    if (OB_SUCCESS != err)
    {
      TBSYS_LOG(WARN,"fail to store org_column_name to local buffer [err:%d]", err);
    }
  }
  if (OB_SUCCESS == err && deep_copy_args_)
  {
    err = buffer_pool_.write_string(as_column_name, &stored_as_column_name);
    if (OB_SUCCESS != err)
    {
      TBSYS_LOG(WARN,"fail to store as_column_name to local buffer [err:%d]", err);
    }
  }
  if (OB_SUCCESS == err)
  {
    ObAggregateColumn aggregate_column(stored_org_column_name, stored_as_column_name, column_num_, aggregate_func);
    if (!aggregate_columns_.push_back(aggregate_column))
    {
      err =  OB_ARRAY_OUT_OF_RANGE;
      TBSYS_LOG(WARN,"fail to push_back aggregate_column [err:%d,column_num_:%ld]", err, column_num_);
    }
    else
    {
      column_num_ ++;
    }
  }
  if ((OB_SUCCESS == err ) && (!ac_return_infos_.push_back(is_return)))
  {
    TBSYS_LOG(WARN,"fail to add return info");
    err = OB_ARRAY_OUT_OF_RANGE;
  }
  return err;
}

int oceanbase::common::ObGroupByParam::add_aggregate_column(const int64_t org_column_idx,
  const ObAggregateFuncType aggregate_func, bool is_return)
{
  int err = OB_SUCCESS;

  if (OB_SUCCESS == err)
  {
    if (aggregate_func >= AGG_FUNC_END || aggregate_func <= AGG_FUNC_MIN)
    {
      TBSYS_LOG(WARN,"param error, unrecogonized aggregate function type [type:%d]", aggregate_func);
      err = OB_INVALID_ARGUMENT;
    }
  }
  if (OB_SUCCESS == err)
  {
    ObAggregateColumn aggregate_column(org_column_idx, column_num_, aggregate_func);
    if (!aggregate_columns_.push_back(aggregate_column))
    {
      err =  OB_ARRAY_OUT_OF_RANGE;
      TBSYS_LOG(WARN,"fail to push_back aggregate_column [err:%d,column_num_:%ld]", err, column_num_);
    }
    else
    {
      column_num_ ++;
    }
  }
  if ((OB_SUCCESS == err ) && (!ac_return_infos_.push_back(is_return)))
  {
    TBSYS_LOG(WARN,"fail to add return info");
    err = OB_ARRAY_OUT_OF_RANGE;
  }
  return err;
}


int oceanbase::common::ObGroupByParam::malloc_composite_columns()
{
  int err = OB_SUCCESS;
  groupby_comp_columns_buf_ = reinterpret_cast<ObCompositeColumn*>(ob_malloc(sizeof(ObCompositeColumn)*OB_MAX_COLUMN_NUMBER, ObModIds::OB_OLD_GROUPBY));
  if (NULL == groupby_comp_columns_buf_)
  {
    TBSYS_LOG(WARN,"fail to allocate memory");
    err = OB_ALLOCATE_MEMORY_FAILED;
  }
  else
  {
    new(groupby_comp_columns_buf_) ObCompositeColumn[OB_MAX_COLUMN_GROUP_NUMBER];
    groupby_comp_columns_.init(OB_MAX_COLUMN_NUMBER, groupby_comp_columns_buf_);
  }
  return err;

}

int oceanbase::common::ObGroupByParam::add_column(const ObString & expr, const ObString & as_name, bool is_return)
{
  int err = OB_SUCCESS;
  if (NULL == groupby_comp_columns_buf_)
  {
    err  = malloc_composite_columns();
  }
  ObString stored_expr = expr;
  ObString stored_as_name = as_name;
  if ((OB_SUCCESS == err)
    && deep_copy_args_
    && (OB_SUCCESS != (err = buffer_pool_.write_string(expr, &stored_expr))))
  {
    TBSYS_LOG(WARN,"fail to copy expr to local buffer [err:%d]", err);
  }
  if ((OB_SUCCESS == err)
    && deep_copy_args_
    && (OB_SUCCESS != (err = buffer_pool_.write_string(as_name, &stored_as_name))))
  {
    TBSYS_LOG(WARN,"fail to copy as_name to local buffer [err:%d]", err);
  }
  ObCompositeColumn comp_column;

  if ((OB_SUCCESS == err) && (OB_SUCCESS != (err = comp_column.set_expression(stored_expr, stored_as_name))))
  {
    TBSYS_LOG(WARN,"fail to set expression [err:%d,expr:%.*s, as_name:%.*s]", err,
      stored_expr.length(), stored_expr.ptr(), stored_as_name.length(), stored_as_name.ptr());
  }
  if ((OB_SUCCESS == err) && (!groupby_comp_columns_.push_back(comp_column)))
  {
    TBSYS_LOG(WARN,"groupby composite column list is full");
    err = OB_ARRAY_OUT_OF_RANGE;
  }

  if ((OB_SUCCESS == err) && (!cc_return_infos_.push_back(is_return)))
  {
    TBSYS_LOG(WARN,"groupby return info list is full");
    err = OB_ARRAY_OUT_OF_RANGE;
  }
  if (OB_SUCCESS == err)
  {
    column_num_ ++;
  }
  return err;
}

int oceanbase::common::ObGroupByParam::add_column(const ObObj *expr, bool is_return)
{
  int err = OB_SUCCESS;
  if (NULL == groupby_comp_columns_buf_)
  {
    err = malloc_composite_columns();
  }
  ObCompositeColumn comp_column;
  if ((OB_SUCCESS == err) && (OB_SUCCESS != (err = comp_column.set_expression(expr, buffer_pool_))))
  {
    TBSYS_LOG(WARN,"fail to set expression [err:%d]", err);
  }
  if ((OB_SUCCESS == err) && (!groupby_comp_columns_.push_back(comp_column)))
  {
    TBSYS_LOG(WARN,"groupby composite column list is full");
    err = OB_ARRAY_OUT_OF_RANGE;
  }

  if ((OB_SUCCESS == err) && (!cc_return_infos_.push_back(is_return)))
  {
    TBSYS_LOG(WARN,"groupby return info list is full");
    err = OB_ARRAY_OUT_OF_RANGE;
  }
  if (OB_SUCCESS == err)
  {
    column_num_ ++;
  }
  return err;
}
int oceanbase::common::ObGroupByParam::add_having_cond(const ObString & expr)
{
  int err = OB_SUCCESS;
  static const int32_t max_filter_column_name = 128;
  char filter_column_name[max_filter_column_name]="";
  int32_t filter_column_name_len = 0;
  ObObj false_obj;
  ObString filter_column_name_str;
  ObString as_name;
  ObString stored_expr = expr;
  false_obj.set_bool(false);
  filter_column_name_len = snprintf(filter_column_name,sizeof(filter_column_name),
    "%s%ld",GROUPBY_CLAUSE_HAVING_COND_AS_CNAME_PREFIX, return_infos_.get_array_index());
  filter_column_name_str.assign(filter_column_name,filter_column_name_len);
  if ((OB_SUCCESS == err) && (OB_SUCCESS != (err = buffer_pool_.write_string(filter_column_name_str, &as_name))))
  {
    TBSYS_LOG(WARN,"fail to copy as column name [err:%d]", err);
  }

  if ((OB_SUCCESS == err) && deep_copy_args_ && (OB_SUCCESS != (err = buffer_pool_.write_string(expr, &stored_expr))))
  {
    TBSYS_LOG(WARN,"fail to copy expr [err:%d]", err);
  }

  if ((OB_SUCCESS == err ) && (OB_SUCCESS != (err = add_column(stored_expr,as_name,false))))
  {
    TBSYS_LOG(WARN,"fail to add composite column [err:%d,expr:%.*s]", err, stored_expr.length(), stored_expr.ptr());
  }
  if ((OB_SUCCESS == err) && (OB_SUCCESS != (err = add_having_cond(as_name,NE, false_obj))))
  {
    TBSYS_LOG(WARN,"fail to add condition [err:%d]", err);
  }
  return err;
}

int oceanbase::common::ObGroupByParam::add_having_cond(const ObString & column_name, const ObLogicOperator & cond_op,
  const ObObj & cond_value)
{
  int err = OB_SUCCESS;
  ObString stored_column_name = column_name;
  ObObj stored_cond_value = cond_value;
  if ((OB_SUCCESS == err) && deep_copy_args_ && (OB_SUCCESS != (err = buffer_pool_.write_string(column_name,&stored_column_name))))
  {
    TBSYS_LOG(WARN,"fail to copy colum_name to local buffer [err:%d]", err);
  }
  if ((OB_SUCCESS == err) && deep_copy_args_ && (OB_SUCCESS != (err = buffer_pool_.write_obj(cond_value,&stored_cond_value))))
  {
    TBSYS_LOG(WARN,"fail to copy cond_value to local buffer [err:%d]", err);
  }
  if ((OB_SUCCESS == err) && (OB_SUCCESS != (err = condition_filter_.add_cond(stored_column_name, cond_op, stored_cond_value))))
  {
    TBSYS_LOG(WARN,"fail to add condition [err:%d]", err);
  }
  return err;
}



int oceanbase::common::ObGroupByParam::calc_group_key_hash_val(const ObGroupKey & key, uint32_t &val)
{
  uint32_t hash_val = 0;
  int err = OB_SUCCESS;
  if (NULL == key.get_cell_array() || NULL == key.get_groupby_param())
  {
    TBSYS_LOG(WARN,"param error [key.get_cell_array():%p,key.get_groupby_param():%p]",
      key.get_cell_array(), key.get_groupby_param());
    err = OB_INVALID_ARGUMENT;
  }
  if (OB_SUCCESS == err)
  {
    switch (key.get_key_type())
    {
    case ObGroupKey::AGG_KEY:
      err = key.get_groupby_param()->calc_agg_group_key_hash_val(*key.get_cell_array(),key.get_row_beg(),
        key.get_row_end(),hash_val);
      break;
    case ObGroupKey::ORG_KEY:
      err = key.get_groupby_param()->calc_org_group_key_hash_val(*key.get_cell_array(),key.get_row_beg(),
        key.get_row_end(),hash_val);
      break;
    default:
      TBSYS_LOG(WARN,"param error, unregonized key type [key.get_key_type():%d]", key.get_key_type());
      err = OB_INVALID_ARGUMENT;
    }
  }
  val = hash_val;
  return err;
}

bool oceanbase::common::ObGroupByParam::is_equal(const ObGroupKey & left, const ObGroupKey &right)
{
  bool result = false;
  int err = OB_SUCCESS;
  if (NULL == left.get_cell_array()
    || NULL == left.get_groupby_param()
    || 0 > left.get_row_beg()
    || 0 > left.get_row_end()
    || left.get_row_beg() > left.get_row_end()
    || NULL == right.get_cell_array()
    || NULL == right.get_groupby_param()
    || 0 > right.get_row_beg()
    || 0 > right.get_row_end()
    || right.get_row_beg() > right.get_row_end()
    || left.get_groupby_param() != right.get_groupby_param()
    )
  {
    err = OB_INVALID_ARGUMENT;
    TBSYS_LOG(WARN,"invalid agument ["
      "left.get_cell_array():%p,left.get_groupby_param():%p,left.get_row_beg():%ld,left.get_row_end():%ld,"
      "right.get_cell_array():%p,right.get_groupby_param():%p,right.get_row_beg():%ld,right.get_row_end():%ld,"
      "]"
      ,left.get_cell_array(), left.get_groupby_param(), left.get_row_beg(), left.get_row_end()
      ,right.get_cell_array(), right.get_groupby_param(), right.get_row_beg(), right.get_row_end()
      );
  }
  if (OB_SUCCESS == err)
  {
    if (left.get_row_end() >= left.get_cell_array()->get_cell_size()
      || right.get_row_end() >= right.get_cell_array()->get_cell_size())
    {
      TBSYS_LOG(WARN,"invalid agument [left.get_cell_array()->get_cell_size():%ld,right.get_cell_array()->get_cell_size():%ld]",
        left.get_cell_array()->get_cell_size(),
        right.get_cell_array()->get_cell_size());
      err = OB_INVALID_ARGUMENT;
    }
  }
  if (OB_SUCCESS == err)
  {
    result = true;
    const ObGroupByParam *param = left.get_groupby_param();
    if (left.get_hash_val() != right.get_hash_val())
    {
      result = false;
    }
    for (int64_t i = 0; OB_SUCCESS == err && result && i < param->group_by_columns_.get_array_index(); i++)
    {
      int64_t left_idx = param->get_target_cell_idx(left,i);
      int64_t right_idx = param->get_target_cell_idx(right,i);
      if (left_idx < 0 || right_idx < 0)
      {
        TBSYS_LOG(WARN,"unexpected error [left_idx:%ld,right_idx:%ld]", left_idx, right_idx);
        err = static_cast<int32_t>((right_idx < 0)? right_idx:left_idx);
        result = false;
      }
      else
      {
        if (left.get_cell_array()->operator [](left_idx).value_
          != right.get_cell_array()->operator [](right_idx).value_)
        {
          result = false;
        }
      }
    }
  }
  return result;
}


int64_t oceanbase::common::ObGroupByParam::get_target_cell_idx(const ObGroupKey & key, const int64_t groupby_idx)const
{
  int64_t max_idx = key.get_row_end() - key.get_row_beg();
  int64_t idx = 0;
  int64_t err = OB_SUCCESS;
  if (groupby_idx < 0 || groupby_idx >= group_by_columns_.get_array_index())
  {
    TBSYS_LOG(WARN,"param error [groupby_idx:%ld,group_by_columns_.size():%ld]", groupby_idx, group_by_columns_.get_array_index());
    err = OB_INVALID_ARGUMENT;
  }
  if (OB_SUCCESS == err)
  {
    switch (key.get_key_type())
    {
    case ObGroupKey::AGG_KEY:
      idx = group_by_columns_.at(groupby_idx)->as_column_idx_;
      break;
    case ObGroupKey::ORG_KEY:
      idx = group_by_columns_.at(groupby_idx)->org_column_idx_;
      break;
    default:
      TBSYS_LOG(WARN,"unrecogonized key type [type:%d]", key.get_key_type());
      err = OB_INVALID_ARGUMENT;
    }
    if (OB_SUCCESS == err)
    {
      if (idx > max_idx)
      {
        TBSYS_LOG(WARN,"unexpected error [idx:%ld,row_beg:%ld,row_end:%ld]", idx, key.get_row_beg(), key.get_row_end());
      }
      else
      {
        idx += key.get_row_beg();
      }
    }
  }
  if (OB_SUCCESS == err)
  {
    if (idx >= key.get_cell_array()->get_cell_size())
    {
      err = OB_ERR_UNEXPECTED;
      TBSYS_LOG(WARN,"unexpected error, get idx bigger than size of cell array");
    }
    else
    {
      err = idx;
    }
  }
  return err;
}

int oceanbase::common::ObGroupByParam::calc_org_group_key_hash_val(const ObCellArray & cells, const int64_t row_beg,
  const int64_t row_end, uint32_t &val)const
{
  uint32_t hash_val = 0;
  int err = OB_SUCCESS;
  if (row_beg < 0
    || row_end < 0
    || row_beg >= cells.get_cell_size()
    || row_end >= cells.get_cell_size()
    )
  {
    TBSYS_LOG(WARN,"param error [row_beg:%ld,row_end:%ld, cells.get_cell_size():%ld]", row_beg, row_end,
      cells.get_cell_size());
    err = OB_INVALID_ARGUMENT;
  }
  if (OB_SUCCESS == err)
  {
    int64_t max_idx = row_end - row_beg;
    const ColumnInfo* groupby_col = NULL;
    for (int64_t i = 0; OB_SUCCESS == err && i < group_by_columns_.get_array_index(); i++)
    {
      groupby_col = &group_by_columns_buf_[i];
      if (groupby_col->org_column_idx_ > max_idx || groupby_col->org_column_idx_ < 0)
      {
        TBSYS_LOG(WARN,"param error [max_idx:%ld,group_by_column_idx:%ld,org_column_idx:%ld]", max_idx, i,
          groupby_col->org_column_idx_);
        err = OB_INVALID_ARGUMENT;
      }
      if (OB_SUCCESS == err)
      {
        hash_val = cells[row_beg + groupby_col->org_column_idx_].value_.murmurhash2(hash_val);
      }
    }
  }
  val = hash_val;
  return err;
}

int oceanbase::common::ObGroupByParam::calc_agg_group_key_hash_val(const ObCellArray & cells, const int64_t row_beg,
  const int64_t row_end, uint32_t &val)const
{
  uint32_t hash_val = 0;
  int err = OB_SUCCESS;
  if (row_beg < 0
    || row_end < 0
    || row_beg >= cells.get_cell_size()
    || row_end >= cells.get_cell_size()
    )
  {
    TBSYS_LOG(WARN,"param error [row_beg:%ld,row_end:%ld, cells.get_cell_size():%ld]", row_beg, row_end,
      cells.get_cell_size());
    err = OB_INVALID_ARGUMENT;
  }
  if (OB_SUCCESS == err)
  {
    int64_t max_idx = row_end - row_beg;
    const ColumnInfo* groupby_col = NULL;
    for (int64_t i = 0; OB_SUCCESS == err && i < group_by_columns_.get_array_index(); i++)
    {
      groupby_col = &group_by_columns_buf_[i];
      if (groupby_col->as_column_idx_ > max_idx || groupby_col->as_column_idx_ < 0)
      {
        TBSYS_LOG(WARN,"param error [max_idx:%ld,groupby_column_idx:%ld,as_column_idx:%ld]", max_idx, i,
          groupby_col->as_column_idx_);
        err = OB_INVALID_ARGUMENT;
      }
      if (OB_SUCCESS == err)
      {
        hash_val = cells[row_beg + groupby_col->as_column_idx_].value_.murmurhash2(hash_val);
      }
    }
  }
  val = hash_val;
  return err;
}



int oceanbase::common::ObGroupByParam::aggregate(const ObCellArray & org_cells,  const int64_t org_row_beg,
  const int64_t org_row_end, ObCellArray & aggregate_cells,
  const int64_t aggregate_row_beg, const int64_t aggregate_row_end)const
{
  int err = OB_SUCCESS;
  bool first_row_of_group = false;
  int64_t max_org_idx = org_row_end - org_row_beg;
  ObInnerCellInfo *cell_out = NULL;
  if (aggregate_row_beg == aggregate_cells.get_cell_size())
  {
    first_row_of_group = true;
  }
  if (aggregate_row_end - aggregate_row_beg + 1 != column_num_)
  {
    TBSYS_LOG(WARN,"param error [expected_aggregate_row_width:%ld,real_aggregate_row_width:%ld]",
      column_num_, aggregate_row_end - aggregate_row_beg + 1);
    err = OB_INVALID_ARGUMENT;
  }
  if (OB_SUCCESS == err && first_row_of_group) /// append groupby return and aggregate columns
  {
    ObCellInfo first_agg_cell;
    ObRowkey fake_agg_rowkey;
    for (int64_t i = 0;  OB_SUCCESS == err && i < group_by_columns_.get_array_index(); i++)
    {
      if (group_by_columns_.at(i)->org_column_idx_ > max_org_idx)
      {
        TBSYS_LOG(WARN,"unexpected error [groupby_column_idx:%ld,org_column_idx:%ld,max_org_idx:%ld]",
          i, group_by_columns_.at(i)->org_column_idx_,max_org_idx);
        err = OB_ERR_UNEXPECTED;
      }
      else
      {
        first_agg_cell = org_cells[org_row_beg + group_by_columns_.at(i)->org_column_idx_];
        first_agg_cell.row_key_ = fake_agg_rowkey;
        err = aggregate_cells.append(first_agg_cell, cell_out);
        if (OB_SUCCESS != err)
        {
          TBSYS_LOG(WARN,"fail to append group by columns into aggregate cell array [err:%d]", err);
        }
      }
    }

    for (int64_t i = 0;  OB_SUCCESS == err && i < return_columns_.get_array_index(); i++)
    {
      if (return_columns_.at(i)->org_column_idx_ > max_org_idx)
      {
        TBSYS_LOG(WARN,"unexpected error [return_column_idx:%ld,org_column_idx:%ld,max_org_idx:%ld]",
          i, return_columns_.at(i)->org_column_idx_,max_org_idx);
        err = OB_ERR_UNEXPECTED;
      }
      else
      {
        first_agg_cell = org_cells[org_row_beg + return_columns_.at(i)->org_column_idx_];
        first_agg_cell.row_key_ = fake_agg_rowkey;
        err = aggregate_cells.append(first_agg_cell, cell_out);
        if (OB_SUCCESS != err)
        {
          TBSYS_LOG(WARN,"fail to append return columns into aggregate cell array [err:%d]", err);
        }
      }
    }

    first_agg_cell = org_cells[org_row_beg];
    for (int64_t i = 0; OB_SUCCESS == err && i < aggregate_columns_.get_array_index(); i++)
    {
      if (aggregate_columns_.at(i)->get_org_column_idx() > max_org_idx)
      {
        TBSYS_LOG(WARN,"unexpected error [aggregate_column_idx:%ld,org_column_idx:%ld,max_org_idx:%ld]",
          i, aggregate_columns_.at(i)->get_org_column_idx(),max_org_idx);
        err = OB_ERR_UNEXPECTED;
      }
      else
      {
        err = aggregate_columns_.at(i)->get_first_aggregate_obj(org_cells[org_row_beg + aggregate_columns_.at(i)->get_org_column_idx()].value_,
          first_agg_cell.value_);
        if (OB_SUCCESS != err)
        {
          TBSYS_LOG(WARN,"fail to calc first aggregate value of aggregate cell [aggregeate_idx:%ld]", i);
        }
        else
        {
          first_agg_cell.row_key_ = fake_agg_rowkey;
          first_agg_cell.table_id_ = org_cells[org_row_beg].table_id_;
          first_agg_cell.column_id_ = OB_INVALID_ID;
          err = aggregate_cells.append(first_agg_cell, cell_out);
          if (OB_SUCCESS != err)
          {
            TBSYS_LOG(WARN,"fail to append aggregate columns into aggregate cell array [err:%d]", err);
          }
        }
      }
    }
  }
  /// process aggregate columns
  if (OB_SUCCESS == err && !first_row_of_group)
  {
    const ObAggregateColumn* agg_col = NULL;
    for (int64_t i = 0;  OB_SUCCESS == err && i < aggregate_columns_.get_array_index(); i++)
    {
      agg_col = &aggregate_columns_buf_[i];
      if (agg_col->get_org_column_idx() > max_org_idx)
      {
        TBSYS_LOG(WARN,"unexpected error [aggregate_column_idx:%ld,org_column_idx:%ld,max_org_idx:%ld]",
          i, agg_col->get_org_column_idx(),max_org_idx);
        err = OB_ERR_UNEXPECTED;
      }
      else
      {
        ObObj &target_obj = aggregate_cells[aggregate_row_beg + agg_col->get_as_column_idx()].value_;
        err =  agg_col->calc_aggregate_val(target_obj,
          org_cells[org_row_beg + agg_col->get_org_column_idx()].value_);
        if (OB_SUCCESS != err)
        {
          TBSYS_LOG(WARN,"fail to aggregate value [err:%d,org_idx:%ld,as_idx:%ld,func_type:%d]",
            err, agg_col->get_org_column_idx(), agg_col->get_as_column_idx(),
            agg_col->get_func_type());
        }
      }
    }
  }
  return err;
}

int oceanbase::common::ObGroupByParam::serialize(char* buf, const int64_t buf_len, int64_t& pos) const
{
  int err = OB_SUCCESS;
  if ((return_infos_.get_array_index() > 0 ) &&
    (return_infos_.get_array_index() != column_num_))
  {
    TBSYS_LOG(WARN,"return info count not coincident with total columns [return_info_count:%ld, column_num_:%ld]",
      return_infos_.get_array_index(), column_num_);
    err = OB_INVALID_ARGUMENT;
  }
  else
  {
    err = serialize_helper(buf,buf_len,pos);
    if (0 <= err)
    {
      err = OB_SUCCESS;
    }
  }
  return err;
}

int oceanbase::common::ObGroupByParam::deserialize_groupby_columns(const char *buf, const int64_t buf_len, int64_t & pos)
{
  int err = OB_SUCCESS;
  int64_t prev_pos = pos;
  ObObj cur_obj;
  int64_t int_val;
  ObString str_val;
  while (OB_SUCCESS == err && pos < buf_len)
  {
    prev_pos = pos;
    err = cur_obj.deserialize(buf,buf_len,pos);
    if (OB_SUCCESS == err)
    {
      if (cur_obj.get_ext() != 0
        || (cur_obj.get_type() != ObIntType && cur_obj.get_type() != ObVarcharType))
      {
        pos = prev_pos;
        break;
      }
      if (!using_id_ && !using_name_)
      {
        if (cur_obj.get_type() == ObIntType)
        {
          using_id_ = true;
        }
        else
        {
          using_name_ = true;
        }
      }
      if (using_id_)
      {
        if (OB_SUCCESS != (err = cur_obj.get_int(int_val)))
        {
          TBSYS_LOG(WARN,"fail to get int from obj [err:%d]", err);
        }
        if ((OB_SUCCESS == err) && (OB_SUCCESS != (err = add_groupby_column(int_val))))
        {
          TBSYS_LOG(WARN,"fail to add groupby column [err:%d]", err);
        }
        else if (OB_SUCCESS == err)
        {
          gc_return_infos_.pop();
        }
      }
      else
      {
        if (OB_SUCCESS != (err = cur_obj.get_varchar(str_val)))
        {
          TBSYS_LOG(WARN,"fail to get int from obj [err:%d]", err);
        }
        if ((OB_SUCCESS == err) && (OB_SUCCESS != (err = add_groupby_column(str_val))))
        {
          TBSYS_LOG(WARN,"fail to add groupby column [err:%d]", err);
        }
        else if (OB_SUCCESS == err)
        {
          gc_return_infos_.pop();
        }
      }
    }
    else
    {
      TBSYS_LOG(WARN,"fail to deserialize groupby column [column_idx:%ld,err:%d]", group_by_columns_.get_array_index(),err);
    }
  }
  return err;
}

int oceanbase::common::ObGroupByParam::deserialize_return_columns(const char *buf, const int64_t buf_len, int64_t & pos)
{
  int err = OB_SUCCESS;
  int64_t prev_pos = pos;
  ObObj cur_obj;
  int64_t int_val;
  ObString str_val;
  while (OB_SUCCESS == err && pos < buf_len)
  {
    prev_pos = pos;
    err = cur_obj.deserialize(buf,buf_len,pos);
    if (OB_SUCCESS == err)
    {
      if (cur_obj.get_ext() != 0
        || (cur_obj.get_type() != ObIntType && cur_obj.get_type() != ObVarcharType))
      {
        pos = prev_pos;
        break;
      }
      if (!using_id_ && !using_name_)
      {
        if (cur_obj.get_type() == ObIntType)
        {
          using_id_ = true;
        }
        else
        {
          using_name_ = true;
        }
      }
      if (using_id_)
      {
        if (OB_SUCCESS != (err = cur_obj.get_int(int_val)))
        {
          TBSYS_LOG(WARN,"fail to get int from obj [err:%d]", err);
        }
        if ((OB_SUCCESS == err) && (OB_SUCCESS != (err = add_return_column(int_val))))
        {
          TBSYS_LOG(WARN,"fail to add return column [err:%d]", err);
        }
        else if (OB_SUCCESS == err)
        {
          rc_return_infos_.pop();
        }
      }
      else
      {
        if (OB_SUCCESS != (err = cur_obj.get_varchar(str_val)))
        {
          TBSYS_LOG(WARN,"fail to get int from obj [err:%d]", err);
        }
        if ((OB_SUCCESS == err) && (OB_SUCCESS != (err = add_return_column(str_val))))
        {
          TBSYS_LOG(WARN,"fail to add return column [err:%d]", err);
        }
        else if (OB_SUCCESS == err)
        {
          rc_return_infos_.pop();
        }
      }
    }
    else
    {
      TBSYS_LOG(WARN,"fail to deserialize return column [column_idx:%ld,err:%d]", return_columns_.get_array_index(),err);
    }
  }
  return err;
}

int oceanbase::common::ObGroupByParam::deserialize_aggregate_columns(const char *buf, const int64_t buf_len, int64_t & pos)
{
  int err = OB_SUCCESS;
  int64_t prev_pos = pos;
  ObObj cur_obj;
  int64_t func_type=0;
  int64_t org_column_idx = -1;
  ObString org_column_name;
  ObString as_column_name;
  while (OB_SUCCESS == err && pos < buf_len)
  {
    prev_pos = pos;
    err = cur_obj.deserialize(buf,buf_len,pos);
    if (OB_SUCCESS == err)
    {
      if (cur_obj.get_ext() != 0
        || (cur_obj.get_type() != ObIntType))
      {
        pos = prev_pos;
        break;
      }
      /// func type
      err = cur_obj.get_int(func_type);
      if (OB_SUCCESS != err)
      {
        TBSYS_LOG(WARN,"fail to get int value from obj [err:%d]", err);
      }
      /// as column name
      if (OB_SUCCESS == err)
      {
        err = cur_obj.deserialize(buf,buf_len,pos);
      }
      if (OB_SUCCESS == err)
      {
        err = cur_obj.get_varchar(as_column_name);
        if (OB_SUCCESS != err)
        {
          TBSYS_LOG(WARN,"fail to get varchar from obj [err:%d]", err);
        }
      }
      /// org info
      if (OB_SUCCESS == err)
      {
        err = cur_obj.deserialize(buf,buf_len,pos);
      }
      if (OB_SUCCESS == err)
      {
        if (!using_id_ && !using_name_)
        {
          if (cur_obj.get_type() == ObIntType)
          {
            using_id_ = true;
          }
          else
          {
            using_name_ = true;
          }
        }
        if (using_id_)
        {
          err = cur_obj.get_int(org_column_idx);
          if (OB_SUCCESS != err)
          {
            TBSYS_LOG(WARN,"fail to get int from obj [err:%d]", err);
          }
        }
        else
        {
          err = cur_obj.get_varchar(org_column_name);
          if (OB_SUCCESS != err)
          {
            TBSYS_LOG(WARN,"fail to get varchar from obj [err:%d]", err);
          }
        }
      }
      /// add column
      if (OB_SUCCESS == err)
      {
        if (using_id_)
        {
          err = add_aggregate_column(org_column_idx,static_cast<ObAggregateFuncType>(func_type));
        }
        else
        {
          err = add_aggregate_column(org_column_name,as_column_name, static_cast<ObAggregateFuncType>(func_type));
        }
        if (OB_SUCCESS != err)
        {
          TBSYS_LOG(WARN,"fail to add aggregate column [idx:%ld,err:%d]", column_num_, err);
        }
        else
        {
          ac_return_infos_.pop();
        }
      }
    }
    else
    {
      TBSYS_LOG(WARN,"fail to deserialize aggregate column [column_idx:%ld,err:%d]", return_columns_.get_array_index(),err);
    }
  }
  return err;
}

int oceanbase::common::ObGroupByParam::deserialize_comp_columns(const char *buf, const int64_t buf_len, int64_t &pos)
{
  int err = OB_SUCCESS;

  ObCompositeColumn comp_column;
  while ((OB_SUCCESS == err) && (pos < buf_len))
  {
    if ((OB_SUCCESS != (err = comp_column.deserialize(buf, buf_len, pos)))
      && (OB_UNKNOWN_OBJ != err))
    {
      TBSYS_LOG(WARN,"fail to decode composite column [err:%d]", err);
    }
    if ((NULL == groupby_comp_columns_buf_) && (OB_SUCCESS == err))
    {
      err = malloc_composite_columns();
    }
    if ((OB_SUCCESS == err) && (!groupby_comp_columns_.push_back(comp_column)))
    {
      TBSYS_LOG(WARN,"composite column list is full");
      err = OB_ARRAY_OUT_OF_RANGE;
    }
    else if (OB_SUCCESS == err)
    {
      column_num_ ++;
    }
  }
  if (OB_UNKNOWN_OBJ == err)
  {
    err = OB_SUCCESS;
  }
  return err;
}

int oceanbase::common::ObGroupByParam::deserialize_return_info(const char *buf, const int64_t buf_len, int64_t &pos)
{
  int err = OB_SUCCESS;
  ObObj obj;
  int64_t old_pos = pos;
  gc_return_infos_.clear();
  rc_return_infos_.clear();
  ac_return_infos_.clear();
  cc_return_infos_.clear();
  int64_t gc_count = group_by_columns_.get_array_index();
  for (int64_t i = 0;
    (i < gc_count) && (OB_SUCCESS == err) && (pos < buf_len);
    i++)
  {
    int64_t val = 0;
    old_pos = pos;
    if ((OB_SUCCESS == err) && (OB_SUCCESS != (err = obj.deserialize(buf,buf_len, pos))))
    {
      TBSYS_LOG(WARN,"fail to deserialize obj [err:%d]", err);
    }
    if (OB_SUCCESS != err || (obj.get_type() != ObIntType))
    {
      pos = old_pos;
      break;
    }
    if ((OB_SUCCESS == err) && (OB_SUCCESS != (err = obj.get_int(val))))
    {
      TBSYS_LOG(WARN,"fail to get bool val from obj [err:%d,type:%d]", err, obj.get_type());
    }
    if ((OB_SUCCESS == err) && (!gc_return_infos_.push_back(val)))
    {
      TBSYS_LOG(WARN,"groupby return info list is full");
      err = OB_ARRAY_OUT_OF_RANGE;
    }
  }

  int64_t rc_count = return_columns_.get_array_index();
  for (int64_t i = 0;
    (i < rc_count) && (OB_SUCCESS == err) && (pos < buf_len);
    i++)
  {
    int64_t val = 0;
    old_pos = pos;
    if ((OB_SUCCESS == err) && (OB_SUCCESS != (err = obj.deserialize(buf,buf_len, pos))))
    {
      TBSYS_LOG(WARN,"fail to deserialize obj [err:%d]", err);
    }
    if (OB_SUCCESS != err || (obj.get_type() != ObIntType))
    {
      pos = old_pos;
      break;
    }
    if ((OB_SUCCESS == err) && (OB_SUCCESS != (err = obj.get_int(val))))
    {
      TBSYS_LOG(WARN,"fail to get bool val from obj [err:%d,type:%d]", err, obj.get_type());
    }
    if ((OB_SUCCESS == err) && (!rc_return_infos_.push_back(val)))
    {
      TBSYS_LOG(WARN,"groupby return info list is full");
      err = OB_ARRAY_OUT_OF_RANGE;
    }
  }

  int64_t ac_count = aggregate_columns_.get_array_index();
  for (int64_t i = 0;
    (i < ac_count) && (OB_SUCCESS == err) && (pos < buf_len);
    i++)
  {
    int64_t val = 0;
    old_pos = pos;
    if ((OB_SUCCESS == err) && (OB_SUCCESS != (err = obj.deserialize(buf,buf_len, pos))))
    {
      TBSYS_LOG(WARN,"fail to deserialize obj [err:%d]", err);
    }
    if (OB_SUCCESS != err || (obj.get_type() != ObIntType))
    {
      pos = old_pos;
      break;
    }
    if ((OB_SUCCESS == err) && (OB_SUCCESS != (err = obj.get_int(val))))
    {
      TBSYS_LOG(WARN,"fail to get bool val from obj [err:%d,type:%d]", err, obj.get_type());
    }
    if ((OB_SUCCESS == err) && (!ac_return_infos_.push_back(val)))
    {
      TBSYS_LOG(WARN,"groupby return info list is full");
      err = OB_ARRAY_OUT_OF_RANGE;
    }
  }

  int64_t cc_count = groupby_comp_columns_.get_array_index();
  for (int64_t i = 0;
    (i < cc_count) && (OB_SUCCESS == err) && (pos < buf_len);
    i++)
  {
    int64_t val = 0;
    old_pos = pos;
    if ((OB_SUCCESS == err) && (OB_SUCCESS != (err = obj.deserialize(buf,buf_len, pos))))
    {
      TBSYS_LOG(WARN,"fail to deserialize obj [err:%d]", err);
    }
    if (OB_SUCCESS != err || (obj.get_type() != ObIntType))
    {
      pos = old_pos;
      break;
    }
    if ((OB_SUCCESS == err) && (OB_SUCCESS != (err = obj.get_int(val))))
    {
      TBSYS_LOG(WARN,"fail to get bool val from obj [err:%d,type:%d]", err, obj.get_type());
    }
    if ((OB_SUCCESS == err) && (!cc_return_infos_.push_back(val)))
    {
      TBSYS_LOG(WARN,"groupby return info list is full");
      err = OB_ARRAY_OUT_OF_RANGE;
    }
  }

  return err;
}


int oceanbase::common::ObGroupByParam::deserialize_having_condition(const char *buf, const int64_t buf_len, int64_t &pos)
{
  int err = OB_SUCCESS;
  if (OB_SUCCESS != (err = condition_filter_.deserialize(buf,buf_len,pos)))
  {
    TBSYS_LOG(WARN,"fail to deserialize having condition [err:%d]", err);
  }
  return err;
}


int oceanbase::common::ObGroupByParam::deserialize(const char* buf, const int64_t data_len, int64_t& pos)
{
  int err = OB_SUCCESS;
  int64_t prev_pos = pos;
  ObObj cur_obj;
  if (NULL == buf || 0 >= data_len)
  {
    TBSYS_LOG(WARN,"param error [buf:%p,data_len:%ld]", buf,data_len);
    err = OB_INVALID_ARGUMENT;
  }
  if (OB_SUCCESS == err)
  {
    reset();
    while (OB_SUCCESS == err && pos < data_len)
    {
      prev_pos = pos;
      err = cur_obj.deserialize(buf,data_len,pos);
      if (OB_SUCCESS == err)
      {
        switch (cur_obj.get_ext())
        {
        case ObActionFlag::GROUPBY_GRO_COLUMN_FIELD:
          err = deserialize_groupby_columns(buf,data_len,pos);
          break;
        case ObActionFlag::GROUPBY_AGG_COLUMN_FIELD:
          err = deserialize_aggregate_columns(buf,data_len,pos);
          break;
        case ObActionFlag::GROUPBY_RET_COLUMN_FIELD:
          err = deserialize_return_columns(buf,data_len,pos);
          break;
        case ObActionFlag::GROUPBY_CLAUSE_COMP_COLUMN_FIELD:
          err = deserialize_comp_columns(buf,data_len, pos);
          break;
        case ObActionFlag::GROUPBY_CLAUSE_RETURN_INFO_FIELD:
          err = deserialize_return_info(buf,data_len, pos);
          break;
        case ObActionFlag::GROUPBY_CLAUSE_HAVING_FIELD:
          err = deserialize_having_condition(buf,data_len,pos);
          break;
        default:
          pos = prev_pos;
          break;
        }
        if (pos == prev_pos)
        {
          break;
        }
      }
    }
  }
  /// if (OB_SUCCESS == err)
  /// {
  ///   if (column_num_ > 0 && aggregate_columns_.get_array_index() <= 0)
  ///   {
  ///     err = OB_INVALID_ARGUMENT;
  ///     TBSYS_LOG(WARN,"there is no aggregate column, but has groupby or return column");
  ///   }
  /// }
  /// Compatible with old client
  if ((OB_SUCCESS == err )
    && (return_infos_.get_array_index() == 0)
    && (column_num_ > 0))
  {
    for (int64_t idx = 0; idx < group_by_columns_.get_array_index() && OB_SUCCESS == err; idx ++)
    {
      if (!gc_return_infos_.push_back(true))
      {
        TBSYS_LOG(WARN,"fail to add default return info");
        err  = OB_ARRAY_OUT_OF_RANGE ;
      }
    }
    for (int64_t idx = 0; idx < return_columns_.get_array_index() && OB_SUCCESS == err; idx ++)
    {
      if (!rc_return_infos_.push_back(true))
      {
        TBSYS_LOG(WARN,"fail to add default return info");
        err  = OB_ARRAY_OUT_OF_RANGE ;
      }
    }
    for (int64_t idx = 0; idx < aggregate_columns_.get_array_index() && OB_SUCCESS == err; idx ++)
    {
      if (!ac_return_infos_.push_back(true))
      {
        TBSYS_LOG(WARN,"fail to add default return info");
        err  = OB_ARRAY_OUT_OF_RANGE ;
      }
    }
    for (int64_t idx = 0; idx < groupby_comp_columns_.get_array_index() && OB_SUCCESS == err; idx ++)
    {
      if (!cc_return_infos_.push_back(true))
      {
        TBSYS_LOG(WARN,"fail to add default return info");
        err  = OB_ARRAY_OUT_OF_RANGE ;
      }
    }
  }
  if (return_infos_.get_array_index() != column_num_)
  {
    TBSYS_LOG(WARN,"return info count not coincident with total columns [return_info_count:%ld, column_num_:%ld]",
      return_infos_.get_array_index(), column_num_);
    err = OB_INVALID_ARGUMENT;
  }
  return err;
}

int64_t oceanbase::common::ObGroupByParam::get_serialize_size(void) const
{
  int64_t pos;
  return serialize_helper(NULL, 0, pos);
}

int64_t oceanbase::common::ObGroupByParam::groupby_columns_serialize_helper(char* buf, const int64_t buf_len, int64_t & pos) const
{
  int err = OB_SUCCESS;
  ObString str_val;
  ObObj    obj;
  int64_t need_size = 0;
  /// group by columns
  if (OB_SUCCESS == err && group_by_columns_.get_array_index() > 0)
  {
    obj.set_ext(ObActionFlag::GROUPBY_GRO_COLUMN_FIELD);
    need_size += obj.get_serialize_size();
    if (NULL != buf)
    {
      err = obj.serialize(buf, buf_len,pos);
    }
  }
  for (int64_t i = 0; OB_SUCCESS == err && i < group_by_columns_.get_array_index(); i++)
  {
    if (!using_id_ && !using_name_)
    {
      if (0 != group_by_columns_.at(i)->column_name_.length()
        && NULL != group_by_columns_.at(i)->column_name_.ptr())
      {
        using_name_ = true;
      }
      else
      {
        using_id_ = true;
      }
    }
    if (using_name_)
    {
      if (0 == group_by_columns_.at(i)->column_name_.length()
        || NULL == group_by_columns_.at(i)->column_name_.ptr())
      {
        err = OB_INVALID_ARGUMENT;
        TBSYS_LOG(WARN,"argument error, using only name [group_by_column_idx:%ld]", i);
      }
      else
      {
        obj.set_varchar(group_by_columns_.at(i)->column_name_);
        need_size += obj.get_serialize_size();
        if (NULL != buf)
        {
          err = obj.serialize(buf, buf_len,pos);
        }
      }
    }
    else
    {
      if (0 > group_by_columns_.at(i)->org_column_idx_)
      {
        err = OB_INVALID_ARGUMENT;
        TBSYS_LOG(WARN,"argument error, using only id [group_by_column_idx:%ld]", i);
      }
      else
      {
        obj.set_int(group_by_columns_.at(i)->org_column_idx_);
        need_size += obj.get_serialize_size();
        if (NULL != buf)
        {
          err = obj.serialize(buf, buf_len,pos);
        }
      }
    }
  }
  if (OB_SUCCESS == err)
  {
    err = static_cast<int32_t>(need_size);
  }
  return err;
}

int64_t oceanbase::common::ObGroupByParam::return_columns_serialize_helper(char* buf, const int64_t buf_len, int64_t & pos) const
{
  int err = OB_SUCCESS;
  ObString str_val;
  ObObj    obj;
  int64_t need_size = 0;
  /// return columns
  if (OB_SUCCESS == err && return_columns_.get_array_index() > 0)
  {
    obj.set_ext(ObActionFlag::GROUPBY_RET_COLUMN_FIELD);
    need_size += obj.get_serialize_size();
    if (NULL != buf)
    {
      err = obj.serialize(buf, buf_len,pos);
    }
  }
  for (int64_t i = 0; OB_SUCCESS == err && i < return_columns_.get_array_index(); i++)
  {
    if (!using_id_ && !using_name_)
    {
      if (0 != return_columns_.at(i)->column_name_.length()
        && NULL != return_columns_.at(i)->column_name_.ptr())
      {
        using_name_ = true;
      }
      else
      {
        using_id_ = true;
      }
    }
    if (using_name_)
    {
      if (0 == return_columns_.at(i)->column_name_.length()
        || NULL == return_columns_.at(i)->column_name_.ptr())
      {
        err = OB_INVALID_ARGUMENT;
        TBSYS_LOG(WARN,"argument error, using only name [return_column_idx:%ld]", i);
      }
      else
      {
        obj.set_varchar(return_columns_.at(i)->column_name_);
        need_size += obj.get_serialize_size();
        if (NULL != buf)
        {
          err = obj.serialize(buf, buf_len,pos);
        }
      }
    }
    else
    {
      if (0 > return_columns_.at(i)->org_column_idx_)
      {
        err = OB_INVALID_ARGUMENT;
        TBSYS_LOG(WARN,"argument error, using only id [return_column_idx:%ld]", i);
      }
      else
      {
        obj.set_int(return_columns_.at(i)->org_column_idx_);
        need_size += obj.get_serialize_size();
        if (NULL != buf)
        {
          err = obj.serialize(buf, buf_len,pos);
        }
      }
    }
  }
  if (OB_SUCCESS == err)
  {
    err = static_cast<int32_t>(need_size);
  }
  return err;
}

int64_t oceanbase::common::ObGroupByParam::aggregate_columns_serialize_helper(char* buf, const int64_t buf_len, int64_t & pos) const
{
  int err = OB_SUCCESS;
  ObString str_val;
  ObObj    obj;
  int64_t need_size = 0;
  ObString empty_str;
  /// aggregate columns
  if (OB_SUCCESS == err && aggregate_columns_.get_array_index() > 0)
  {
    obj.set_ext(ObActionFlag::GROUPBY_AGG_COLUMN_FIELD);
    need_size += obj.get_serialize_size();
    if (NULL != buf)
    {
      err = obj.serialize(buf, buf_len,pos);
    }
  }
  for (int64_t i = 0; OB_SUCCESS == err && i < aggregate_columns_.get_array_index(); i++)
  {
    /// func type
    obj.set_int(aggregate_columns_.at(i)->get_func_type());
    need_size += obj.get_serialize_size();
    if (NULL != buf)
    {
      err = obj.serialize(buf, buf_len,pos);
    }
    if (OB_SUCCESS == err)
    {
      if (!using_id_ && !using_name_)
      {
        if (0 != aggregate_columns_.at(i)->get_org_column_name().length()
          && NULL != aggregate_columns_.at(i)->get_org_column_name().ptr())
        {
          using_name_ = true;
        }
        else
        {
          using_id_ = true;
        }
      }
      if (using_name_)
      {
        if (0 == aggregate_columns_.at(i)->get_org_column_name().length()
          || NULL == aggregate_columns_.at(i)->get_org_column_name().ptr()
          || 0 == aggregate_columns_.at(i)->get_as_column_name().length()
          || NULL == aggregate_columns_.at(i)->get_as_column_name().ptr())
        {
          err = OB_INVALID_ARGUMENT;
          TBSYS_LOG(WARN,"argument error, using only name [aggregate_column_idx:%ld]", i);
        }
        else
        {
          obj.set_varchar(aggregate_columns_.at(i)->get_as_column_name());
          need_size += obj.get_serialize_size();
          if (NULL != buf)
          {
            err = obj.serialize(buf, buf_len,pos);
          }
          if (OB_SUCCESS == err)
          {
            obj.set_varchar(aggregate_columns_.at(i)->get_org_column_name());
            need_size += obj.get_serialize_size();
            if (NULL != buf)
            {
              err = obj.serialize(buf, buf_len,pos);
            }
          }
        }
      }
      else
      {
        if (0 > aggregate_columns_.at(i)->get_org_column_idx())
        {
          err = OB_INVALID_ARGUMENT;
          TBSYS_LOG(WARN,"argument error, using only id [aggregate_column_idx:%ld]", i);
        }
        else
        {
          obj.set_varchar(empty_str); /// when using index, just serialize an empty ObString
          need_size += obj.get_serialize_size();
          if (NULL != buf)
          {
            err = obj.serialize(buf, buf_len,pos);
          }
          if (OB_SUCCESS == err)
          {
            obj.set_int(aggregate_columns_.at(i)->get_org_column_idx());
            need_size += obj.get_serialize_size();
            if (NULL != buf)
            {
              err = obj.serialize(buf, buf_len,pos);
            }
          }
        }
      }
    }
  }
  if (OB_SUCCESS == err)
  {
    err = static_cast<int32_t>(need_size);
  }
  return err;
}

int oceanbase::common::ObGroupByParam::comp_columns_serialize_helper(char* buf, const int64_t buf_len, int64_t & pos) const
{
  int err = OB_SUCCESS;
  if (groupby_comp_columns_.get_array_index() > 0)
  {
    ObObj obj;
    obj.set_ext(ObActionFlag::GROUPBY_CLAUSE_COMP_COLUMN_FIELD);
    if ((OB_SUCCESS == err) && (OB_SUCCESS != (err = obj.serialize(buf,buf_len,pos))))
    {
      TBSYS_LOG(WARN,"fail to serialize GROUPBY_CLAUSE_COMP_COLUMN_FIELD ext obj [err:%d]", err);
    }
  }
  for (int64_t idx = 0; OB_SUCCESS == err && idx < groupby_comp_columns_.get_array_index(); idx ++)
  {
    if ((OB_SUCCESS == err) && (OB_SUCCESS != (err = groupby_comp_columns_buf_[idx].serialize(buf,buf_len,pos))))
    {
      TBSYS_LOG(WARN,"fail to serialize composite column [idx:%ld, err:%d]", idx, err);
    }
  }
  return err;
}


int64_t oceanbase::common::ObGroupByParam::comp_columns_get_serialize_size(void)const
{
  int64_t total_len = 0;
  if (groupby_comp_columns_.get_array_index() > 0)
  {
    ObObj obj;
    obj.set_ext(ObActionFlag::GROUPBY_CLAUSE_COMP_COLUMN_FIELD);
    total_len += obj.get_serialize_size();
  }
  for (int64_t idx = 0; idx < groupby_comp_columns_.get_array_index(); idx ++)
  {
    total_len += groupby_comp_columns_buf_[idx].get_serialize_size();
  }
  return total_len;
}

int oceanbase::common::ObGroupByParam::return_info_serialize_helper(char* buf, const int64_t buf_len, int64_t & pos) const
{
  int err = OB_SUCCESS;
  if (return_infos_.get_array_index() > 0)
  {
    ObObj obj;
    obj.set_ext(ObActionFlag::GROUPBY_CLAUSE_RETURN_INFO_FIELD);
    if (OB_SUCCESS != (err = obj.serialize(buf,buf_len, pos)))
    {
      TBSYS_LOG(WARN,"fail to serialize GROUPBY_CLAUSE_RETURN_INFO_FIELD [err:%d]", err);
    }
  }
  ObObj bool_obj;
  for (int64_t idx = 0; OB_SUCCESS == err && idx < return_infos_.get_array_index(); idx ++)
  {
    bool_obj.set_int(*return_infos_.at(idx));
    if (OB_SUCCESS != (err = bool_obj.serialize(buf, buf_len, pos)))
    {
      TBSYS_LOG(WARN,"fail to serialize groupby return info [idx:%ld,err:%d]", idx, err);
    }
  }
  return err;
}


int64_t oceanbase::common::ObGroupByParam::return_info_get_serialize_size(void)const
{
  int64_t total_len = 0;
  if (return_infos_.get_array_index() > 0)
  {
    ObObj obj;
    obj.set_ext(ObActionFlag::GROUPBY_CLAUSE_RETURN_INFO_FIELD);
    total_len += obj.get_serialize_size();
  }
  ObObj bool_obj;
  for (int64_t idx = 0; idx < return_infos_.get_array_index(); idx ++)
  {
    bool_obj.set_int(*return_infos_.at(idx));
    total_len += bool_obj.get_serialize_size();
  }
  return total_len;
}

int oceanbase::common::ObGroupByParam::having_condition_serialize_helper(char* buf, const int64_t buf_len, int64_t & pos)const
{
  int err = OB_SUCCESS;
  if (condition_filter_.get_count() > 0)
  {
    ObObj obj;
    obj.set_ext(ObActionFlag::GROUPBY_CLAUSE_HAVING_FIELD);
    if ((OB_SUCCESS == err) && (OB_SUCCESS != (err = obj.serialize(buf, buf_len, pos))))
    {
      TBSYS_LOG(WARN,"fail to serialize GROUPBY_CLAUSE_HAVING_FIELD ext field [err:%d]", err);
    }

    if ((OB_SUCCESS == err) && (OB_SUCCESS != (err = condition_filter_.serialize(buf, buf_len, pos))))
    {
      TBSYS_LOG(WARN,"fail to serialize having condition [err:%d]", err);
    }
  }
  return err;
}
int64_t oceanbase::common::ObGroupByParam::having_condition_get_serialize_size(void)const
{
  int64_t total_size = 0;
  if (condition_filter_.get_count() > 0)
  {
    ObObj obj;
    obj.set_ext(ObActionFlag::SELECT_CLAUSE_WHERE_FIELD);
    total_size = obj.get_serialize_size();
    total_size += condition_filter_.get_serialize_size();
  }
  return total_size;
}

int64_t oceanbase::common::ObGroupByParam::serialize_helper(char* buf, const int64_t buf_len, int64_t& pos) const
{
  int err = OB_SUCCESS;
  ObString str_val;
  ObObj    obj;
  int64_t need_size = 0;
  /// if (column_num_ > 0 && aggregate_columns_.get_array_index() == 0)
  /// {
  ///   err = OB_INVALID_ARGUMENT;
  ///   TBSYS_LOG(WARN,"there is no aggregate column, but has groupby or return column");
  /// }
  if ((OB_SUCCESS == err) && (column_num_ > 0) && (column_num_ == groupby_comp_columns_.get_array_index()))
  {
    err = OB_INVALID_ARGUMENT;
    TBSYS_LOG(WARN,"there is only composite column, cannot calculate");
  }
  if (OB_SUCCESS == err)
  {
    err = groupby_columns_serialize_helper(buf,buf_len,pos);
    if (0 <= err)
    {
      need_size += err;
      err = OB_SUCCESS;
    }
    if (OB_SUCCESS == err)
    {
      err = return_columns_serialize_helper(buf,buf_len,pos);
      if (0 <= err)
      {
        need_size += err;
        err = OB_SUCCESS;
      }
    }
    if (OB_SUCCESS == err)
    {
      err = aggregate_columns_serialize_helper(buf,buf_len,pos);
      if (0 <= err)
      {
        need_size += err;
        err = OB_SUCCESS;
      }
    }
  }
  if (OB_SUCCESS == err)
  {
    need_size += comp_columns_get_serialize_size();
    need_size += return_info_get_serialize_size();
    need_size += having_condition_get_serialize_size();
  }
  if ((OB_SUCCESS == err) && (NULL != buf)
    && (OB_SUCCESS != (err = comp_columns_serialize_helper(buf,buf_len,pos))))
  {
    TBSYS_LOG(WARN,"fail to serialize composite columns [err:%d]", err);
  }

  if ((OB_SUCCESS == err) && (NULL != buf)
    && (OB_SUCCESS != (err = return_info_serialize_helper(buf,buf_len,pos))))
  {
    TBSYS_LOG(WARN,"fail to serialize composite columns [err:%d]", err);
  }

  if ((OB_SUCCESS == err) && (NULL != buf)
    && (OB_SUCCESS != (err = having_condition_serialize_helper(buf,buf_len,pos))))
  {
    TBSYS_LOG(WARN,"fail to serialize having condition [err:%d]", err);
  }
  if (OB_SUCCESS == err)
  {
    err = static_cast<int32_t>(need_size);
  }
  return err;
}


bool oceanbase::common::ObGroupByParam::operator == (const ObGroupByParam & other)const
{
  bool result = true;
  if (column_num_ != other.column_num_)
  {
    result = false;
  }
  if (result && group_by_columns_.get_array_index() != other.group_by_columns_.get_array_index())
  {
    result = false;
  }
  if (result && return_columns_.get_array_index() != other.return_columns_.get_array_index())
  {
    result = false;
  }
  if (result && aggregate_columns_.get_array_index() != other.aggregate_columns_.get_array_index())
  {
    result = false;
  }
  for (uint32_t i = 0; result && i < group_by_columns_.get_array_index(); i++)
  {
    if (!((*group_by_columns_.at(i)) == *(other.group_by_columns_.at(i))))
    {
      result = false;
    }
  }
  for (uint32_t i = 0; result && i < return_columns_.get_array_index(); i++)
  {
    if (!((*return_columns_.at(i)) == (*other.return_columns_.at(i))))
    {
      result = false;
    }
  }
  for (uint32_t i = 0; result && i < aggregate_columns_.get_array_index(); i++)
  {
    if (!((*aggregate_columns_.at(i)) == (*other.aggregate_columns_.at(i))))
    {
      result = false;
    }
  }
  return result;
}


int64_t oceanbase::common::ObGroupByParam::find_column(const oceanbase::common::ObString & column_name)const
{
  bool find_column = false;
  int64_t idx = 0;
  for (uint32_t i = 0; !find_column && i < group_by_columns_.get_array_index();i ++, idx ++)
  {
    if (group_by_columns_.at(i)->column_name_ == column_name)
    {
      find_column = true;
      break;
    }
  }
  for (uint32_t i = 0; !find_column && i < return_columns_.get_array_index();i ++, idx ++)
  {
    if (return_columns_.at(i)->column_name_ == column_name)
    {
      find_column = true;
      break;
    }
  }

  for (uint32_t i = 0; !find_column && i < aggregate_columns_.get_array_index();i ++, idx ++)
  {
    if (aggregate_columns_.at(i)->get_as_column_name() == column_name)
    {
      find_column = true;
      break;
    }
  }

  for (uint32_t i = 0; !find_column && i < groupby_comp_columns_.get_array_index();i ++, idx ++)
  {
    if (groupby_comp_columns_.at(i)->get_as_column_name() == column_name)
    {
      find_column = true;
      break;
    }
  }
  if (!find_column)
  {
    idx = -1;
  }
  return idx;
}


int  oceanbase::common::ObGroupByParam::get_aggregate_column_name(const int64_t column_idx, ObString & column_name)const
{
  int err = OB_SUCCESS;
  if (column_idx < 0 || column_idx >= column_num_)
  {
    TBSYS_LOG(WARN,"agument error [column_idx:%ld,column_num_:%ld]", column_idx, column_num_);
    err = OB_ARRAY_OUT_OF_RANGE;
  }
  int64_t column_count_returned = 0;
  int64_t c_idx = 0;
  if (OB_SUCCESS == err)
  {
    for (c_idx = 0; c_idx < return_infos_.get_array_index(); c_idx ++)
    {
      if (*return_infos_.at(c_idx))
      {
        column_count_returned ++;
        if (column_count_returned >= column_idx + 1)
        {
          break;
        }
      }
    }
    if (column_count_returned < column_idx + 1)
    {
      TBSYS_LOG(WARN,"[column_num_need_return:%ld,column_idx:%ld]", column_count_returned, column_idx);
      err = OB_ARRAY_OUT_OF_RANGE;
    }
  }
  if (OB_SUCCESS == err)
  {
    if (c_idx < group_by_columns_.get_array_index())
    {
      column_name = group_by_columns_.at(c_idx)->column_name_;
    }
    else if (c_idx < group_by_columns_.get_array_index() + return_columns_.get_array_index())
    {
      column_name = return_columns_.at(c_idx - group_by_columns_.get_array_index())->column_name_;
    }
    else if (c_idx < group_by_columns_.get_array_index() + return_columns_.get_array_index() +
      aggregate_columns_.get_array_index())
    {
      column_name = aggregate_columns_.at(c_idx
        - group_by_columns_.get_array_index()
        - return_columns_.get_array_index())->get_as_column_name();
    }
    else
    {
      column_name = groupby_comp_columns_.at(c_idx
        - group_by_columns_.get_array_index()
        - return_columns_.get_array_index()
        - aggregate_columns_.get_array_index())->get_as_column_name();
    }
  }
  return err;
}


int oceanbase::common::ObGroupByParam::safe_copy(const ObGroupByParam & other)
{
  int err = OB_SUCCESS;
  ObCompositeColumn * this_comp_columns = groupby_comp_columns_buf_;
  using_id_ = other.using_id_;
  using_name_ = other.using_name_;
  column_num_ = other.column_num_;
  deep_copy_args_ = other.deep_copy_args_;
  memcpy(group_by_columns_buf_, other.group_by_columns_buf_,sizeof(group_by_columns_buf_));
  group_by_columns_.init(OB_MAX_COLUMN_NUMBER, group_by_columns_buf_, other.group_by_columns_.get_array_index());

  memcpy(return_columns_buf_, other.return_columns_buf_, sizeof(return_columns_buf_));
  return_columns_.init(OB_MAX_COLUMN_NUMBER, return_columns_buf_, other.return_columns_.get_array_index());

  memcpy(aggregate_columns_buf_, other.aggregate_columns_buf_, sizeof(aggregate_columns_buf_));
  aggregate_columns_.init(OB_MAX_COLUMN_NUMBER, aggregate_columns_buf_, other.aggregate_columns_.get_array_index());

  memcpy(gc_return_infos_buf_, other.gc_return_infos_buf_, sizeof(gc_return_infos_buf_));
  gc_return_infos_.init(OB_MAX_COLUMN_NUMBER, gc_return_infos_buf_, other.gc_return_infos_.get_array_index());
  memcpy(rc_return_infos_buf_, other.rc_return_infos_buf_, sizeof(rc_return_infos_buf_));
  rc_return_infos_.init(OB_MAX_COLUMN_NUMBER, rc_return_infos_buf_, other.rc_return_infos_.get_array_index());
  memcpy(ac_return_infos_buf_, other.ac_return_infos_buf_, sizeof(ac_return_infos_buf_));
  ac_return_infos_.init(OB_MAX_COLUMN_NUMBER, ac_return_infos_buf_, other.ac_return_infos_.get_array_index());
  memcpy(ac_return_infos_buf_, other.ac_return_infos_buf_, sizeof(ac_return_infos_buf_));
  ac_return_infos_.init(OB_MAX_COLUMN_NUMBER, ac_return_infos_buf_, other.ac_return_infos_.get_array_index());
  memcpy(cc_return_infos_buf_, other.cc_return_infos_buf_, sizeof(cc_return_infos_buf_));
  cc_return_infos_.init(OB_MAX_COLUMN_NUMBER, cc_return_infos_buf_, other.cc_return_infos_.get_array_index());

  if ((OB_SUCCESS == err) && (OB_SUCCESS != (err = condition_filter_.safe_copy(other.condition_filter_))))
  {
    TBSYS_LOG(WARN,"fail to deep copy condition filter");
  }

  if ((OB_SUCCESS == err) && (NULL == this_comp_columns) && (NULL != other.groupby_comp_columns_buf_))
  {
    if (OB_SUCCESS != (err = malloc_composite_columns()))
    {
      TBSYS_LOG(WARN,"fail to allocate composite columns [err:%d]", err);
    }
    else
    {
      this_comp_columns = groupby_comp_columns_buf_;
    }
  }
  if (OB_SUCCESS == err)
  {
    groupby_comp_columns_buf_ = this_comp_columns;
    groupby_comp_columns_.init(OB_MAX_COLUMN_NUMBER, groupby_comp_columns_buf_,other.groupby_comp_columns_.get_array_index());
  }
  if ((OB_SUCCESS == err) && (NULL != other.groupby_comp_columns_buf_))
  {
    memcpy(groupby_comp_columns_buf_, other.groupby_comp_columns_buf_, sizeof(ObCompositeColumn)*OB_MAX_COLUMN_GROUP_NUMBER);
  }
  return err;
}

int64_t ObGroupByParam::get_returned_column_num()
{
  int64_t counter = 0;
  for (int i = 0; i < return_infos_.get_array_index(); ++i)
  {
    if (*return_infos_.at(i))
    {
      counter++;
    }
  }
  return counter;
}
