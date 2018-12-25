/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_update.cpp
 *
 * Authors:
 *   Huang Yu <xiaochu.yh@taobao.com>
 *
 */
#include "ob_update.h"
#include "common/utility.h"
#include "mergeserver/ob_ms_rpc_proxy.h"
#include "common/ob_obj_cast.h"
#include "common/ob_rowkey_helper.h"
using namespace oceanbase::sql;
using namespace oceanbase::common;

ObUpdate::ObUpdate()
  :rpc_(NULL), table_id_(OB_INVALID_ID)
{
}

ObUpdate::~ObUpdate()
{
}

void ObUpdate::reset()
{
  rpc_ = NULL;
  table_id_ = OB_INVALID_ID;
  mutator_.reset();
  update_column_ids_.clear();
  update_column_exprs_.clear();
  ObSingleChildPhyOperator::reset();
}

void ObUpdate::reuse()
{
  rpc_ = NULL;
  table_id_ = OB_INVALID_ID;
  mutator_.clear();
  update_column_ids_.clear();
  update_column_exprs_.clear();
  ObSingleChildPhyOperator::reset();
}

int ObUpdate::open()
{
  int ret = OB_SUCCESS;
  if (OB_INVALID_ID == table_id_
      || 0 >= cols_desc_.get_column_num()
      || NULL == rpc_)
  {
    TBSYS_LOG(WARN, "not init, tid=%lu, rpc=%p, row column num[%ld]",
              table_id_, rpc_, cols_desc_.get_column_num());
  }
  else if (OB_SUCCESS != (ret = ObSingleChildPhyOperator::open()))
  {
    TBSYS_LOG(WARN, "failed to open child op, err=%d", ret);
  }
  else
  {
    mutator_.use_ob_sem();
    if (OB_SUCCESS != (ret = mutator_.reset()))
    {
      TBSYS_LOG(WARN, "fail to reset mutator. ret=%d", ret);
    }
    else if (OB_SUCCESS != (ret = update_by_mutator()))
    {
      TBSYS_LOG(WARN, "fail to update by mutator. ret=%d", ret);
    }
  }
  return ret;
}

int ObUpdate::close()
{
  mutator_.reset();
  return ObSingleChildPhyOperator::close();
}

int ObUpdate::add_update_expr(const uint64_t column_id, const ObSqlExpression &expr)
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS != (ret = update_column_ids_.push_back(column_id)))
  {
    TBSYS_LOG(WARN, "fail to add column id to ObUpate operator. ret=%d", ret);
  }
  else if (OB_SUCCESS != (ret = update_column_exprs_.push_back(expr)))
  {
    TBSYS_LOG(WARN, "fail to add column expression to ObUpate operator. ret=%d", ret);
  }
  else
  {
    update_column_exprs_.at(update_column_exprs_.count() - 1).set_owner_op(this);
  }
  return ret;
}

int ObUpdate::update_by_mutator()
{
  int ret = OB_SUCCESS;
  OB_ASSERT(child_op_);
  const ObRow *row = NULL;
  int64_t idx = OB_INVALID_INDEX;
  uint64_t column_id = OB_INVALID_ID;
  const ObObj *cell = NULL;
  ObObj rowkey_objs[OB_MAX_ROWKEY_COLUMN_NUMBER];
  ObRowkey rowkey;
  ObObj data_type;
  ObObj casted_cells[OB_MAX_ROWKEY_COLUMN_NUMBER];
  char *varchar_buff = NULL;
  const ObObj *res_cell = NULL;
  int64_t i = 0;
  if (NULL == (varchar_buff = (char*)ob_malloc(OB_MAX_VARCHAR_LENGTH*(OB_MAX_COLUMN_NUMBER), ObModIds::OB_SQL_UPDATE)))
  {
    TBSYS_LOG(ERROR, "no memory");
    ret = OB_ALLOCATE_MEMORY_FAILED;
  }

  // add updates to mutator
  while(OB_SUCCESS == ret
        && OB_SUCCESS == (ret = child_op_->get_next_row(row)))
  {
    // construct rowkey
    if (NULL == row)
    {
      ret = OB_ERR_UNEXPECTED;
      TBSYS_LOG(WARN, "fail to get next row. unexpected.");
      break;
    }
    else if (OB_SUCCESS != (ret = get_row_key(table_id_, *row, rowkey_info_,
            rowkey_objs, OB_MAX_ROWKEY_COLUMN_NUMBER, rowkey)))
    {
      TBSYS_LOG(WARN, "fail to get rowkey for table %lu", table_id_);
      break;
    }
    // set updates to mutator
    for (i = 0; i < update_column_ids_.count(); ++i)
    {
      // for the case convert_sth_2_varchar, we have to allocate buffer for the generated varchar
      ObString varchar;
      varchar.assign_ptr(varchar_buff + OB_MAX_VARCHAR_LENGTH * i, OB_MAX_VARCHAR_LENGTH);
      casted_cells[i].set_varchar(varchar);

      column_id = update_column_ids_.at(i);
      if (OB_SUCCESS != (ret = cols_desc_.get_by_id(table_id_, column_id, idx, data_type)))
      {
        TBSYS_LOG(WARN, "failed to get data type from row desc, err=%d column_id=%lu", ret, column_id);
        break;
      }
      else
      {
        if (OB_SUCCESS != (ret = update_column_exprs_.at(i).calc(*row, cell)))
        {
          TBSYS_LOG(WARN, "fail to calc %ld column value. ret=%d", i, ret);
        }
        else if (NULL == cell)
        {
          TBSYS_LOG(WARN, "null cell");
          ret = OB_ERR_UNEXPECTED;
          break;
        }
        else if (OB_SUCCESS != (ret = obj_cast(*cell, data_type, casted_cells[i], res_cell)))
        {
          TBSYS_LOG(WARN, "failed to cast obj, err=%d", ret);
          break;
        }
        else if (NULL == res_cell)
        {
          TBSYS_LOG(WARN, "null cell");
          ret = OB_ERR_UNEXPECTED;
          break;
        }
        else if (OB_SUCCESS != (ret = mutator_.update(table_id_, rowkey, column_id, *res_cell)))
        {
          TBSYS_LOG(WARN, "fail to add table %lu item %s to update mutator. ret=%d", table_id_, to_cstring(rowkey), ret);
          break;
        }
        if (0)
        {
          TBSYS_LOG(DEBUG, "mutator.update() rowkey=%s, column_id=%lu, cell=%s, i=%ld",
              to_cstring(rowkey), column_id, to_cstring(*res_cell), i);
        }
      }
    } // end for
  }   // end while
  if (OB_ITER_END != ret)
  {
    TBSYS_LOG(WARN, "failed to cons mutator, err=%d", ret);
  }
  else
  {
    ret = OB_SUCCESS;
  }
  // send mutator
  if (OB_SUCCESS == ret)
  {
    ObScanner scanner;          // useless
    if (OB_SUCCESS != (ret = rpc_->ups_mutate(mutator_, false, scanner)))
    {
      TBSYS_LOG(WARN, "failed to send mutator to ups, err=%d", ret);
    }
  }
  if (NULL != varchar_buff)
  {
    ob_free(varchar_buff);
    varchar_buff = NULL;
  }
  return ret;
}

int ObUpdate::get_row_key(const uint64_t table_id,
    const ObRow &row,
    const ObRowkeyInfo &rowkey_info,
    ObObj *rowkey_objs,
    const int64_t obj_size,
    ObRowkey &rowkey)
{
  int ret = OB_SUCCESS;
  uint64_t cid = OB_INVALID_ID;
  const ObObj *cell = NULL;

  if (obj_size < rowkey_info.get_size())
  {
    TBSYS_LOG(WARN, "rowkey_obj array is too small. size=%ld, expect %ld", obj_size, rowkey_info.get_size());
    ret = OB_INVALID_ARGUMENT;
  }
  else if (NULL == rowkey_objs)
  {
    TBSYS_LOG(WARN, "invalid argument. rowkey_objs=null");
    ret = OB_INVALID_ARGUMENT;
  }
  else
  {
    for (int64_t i = 0; i < rowkey_info.get_size(); ++i)
    {
      if (OB_SUCCESS != (ret = rowkey_info.get_column_id(i, cid)))
      {
        TBSYS_LOG(WARN, "failed to get primary key column, err=%d cid=%lu", ret, cid);
        break;
      }
      else if (OB_SUCCESS != (ret = row.get_cell(table_id, cid, cell)))
      {
        TBSYS_LOG(WARN, "failed to get rowkey cell, err=%d tid=%lu cid=%lu", ret, table_id, cid);
        break;
      }
      else if (cell->is_null())
      {
        TBSYS_LOG(WARN, "rowkey column cannot be null, tid=%lu cid=%lu", table_id, cid);
        ret = OB_ERR_UNEXPECTED;
        break;
      }
      else
      {
        rowkey_objs[i] = *cell;
      }
    } // end for
    if (OB_LIKELY(OB_SUCCESS == ret))
    {
      rowkey.assign(rowkey_objs, rowkey_info.get_size());
    }
  }
  return ret;
}

namespace oceanbase{
  namespace sql{
    REGISTER_PHY_OPERATOR(ObUpdate, PHY_UPDATE);
  }
}

int64_t ObUpdate::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  databuff_printf(buf, buf_len, pos, "Update(tid=%lu cols_num=%ld)\n",
                  table_id_, cols_desc_.get_column_num());
  if (NULL != child_op_)
  {
    pos += child_op_->to_string(buf+pos, buf_len-pos);
  }
  return pos;
}
