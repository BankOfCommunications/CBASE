/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_delete.cpp
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *   Huang Yu <xiaochu.yh@taobao.com>
 *
 */
#include "ob_delete.h"
#include "common/utility.h"
#include "mergeserver/ob_ms_rpc_proxy.h"
#include "common/ob_obj_cast.h"
using namespace oceanbase::sql;
using namespace oceanbase::common;

ObDelete::ObDelete()
  :rpc_(NULL), table_id_(OB_INVALID_ID)
{
}

ObDelete::~ObDelete()
{
}

void ObDelete::reset()
{
  rpc_ = NULL;
  table_id_ = OB_INVALID_ID;
  mutator_.reset();
  ObSingleChildPhyOperator::reset();
}

void ObDelete::reuse()
{
  rpc_ = NULL;
  table_id_ = OB_INVALID_ID;
  mutator_.clear();
  ObSingleChildPhyOperator::reuse();
}

int ObDelete::open()
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
    mutator_.use_db_sem();
    if (OB_SUCCESS != (ret = mutator_.reset()))
    {
      TBSYS_LOG(WARN, "fail to reset mutator. ret=%d", ret);
    }
    else if (OB_SUCCESS != (ret = delete_by_mutator()))
    {
      TBSYS_LOG(WARN, "fail to delete by mutator. ret=%d", ret);
    }
  }
  return ret;
}

int ObDelete::close()
{
  mutator_.reset();
  return ObSingleChildPhyOperator::close();
}

int ObDelete::delete_by_mutator()
{
  int ret = OB_SUCCESS;
  OB_ASSERT(child_op_);
  const ObRow *row = NULL;
  uint64_t cid = OB_INVALID_ID;
  const ObObj *cell = NULL;
  ObObj rowkey_objs[OB_MAX_ROWKEY_COLUMN_NUMBER];
  ObRowkey rowkey;
  ObObj data_type;
  ObObj casted_cells[OB_MAX_ROWKEY_COLUMN_NUMBER+1];
  char *varchar_buff = NULL;
  if (NULL == (varchar_buff = (char*)ob_malloc(OB_MAX_VARCHAR_LENGTH*(OB_MAX_ROWKEY_COLUMN_NUMBER), ObModIds::OB_SQL_EXECUTER)))
  {
    TBSYS_LOG(ERROR, "no memory");
    ret = OB_ALLOCATE_MEMORY_FAILED;
  }
  while(OB_SUCCESS == ret
        && OB_SUCCESS == (ret = child_op_->get_next_row(row)))
  {
    // construct rowkey
    int64_t idx = OB_INVALID_INDEX;
    for (int64_t i = 0; i < rowkey_info_.get_size(); ++i)
    {
      ObString varchar;
      varchar.assign_ptr(varchar_buff + OB_MAX_VARCHAR_LENGTH * i, OB_MAX_VARCHAR_LENGTH);
      casted_cells[i].set_varchar(varchar);
      if (OB_SUCCESS != (ret = rowkey_info_.get_column_id(i, cid)))
      {
        TBSYS_LOG(WARN, "failed to get primary key column, err=%d cid=%lu", ret, cid);
        break;
      }
      else if (OB_SUCCESS != (ret = row->get_cell(table_id_, cid, cell)))
      {
        TBSYS_LOG(USER_ERROR, "primary key can not be empty");
        ret = OB_ERR_DELETE_NULL_ROWKEY;
        break;
      }
      else if (cell->is_null())
      {
        TBSYS_LOG(USER_ERROR, "primary key can not be null");
        ret = OB_ERR_DELETE_NULL_ROWKEY;
        break;
      }
      else if (OB_SUCCESS != (ret = cols_desc_.get_by_id(table_id_, cid, idx, data_type)))
      {
        TBSYS_LOG(WARN, "failed to get data type from row desc, err=%d cid=%lu", ret, cid);
        break;
      }
      else
      {
        const ObObj *res_cell = NULL;
        if (OB_SUCCESS != (ret = obj_cast(*cell, data_type, casted_cells[i], res_cell)))
        {
          TBSYS_LOG(WARN, "failed to cast obj, err=%d", ret);
          break;
        }
        else
        {
          rowkey_objs[i] = *res_cell;
        }
      }
    } // end for
    if (OB_UNLIKELY(OB_SUCCESS != ret))
    {
      break;
    }
    else
    {
      rowkey.assign(rowkey_objs, rowkey_info_.get_size());
    }
    // We want to get no columns other than primary key.
    OB_ASSERT(rowkey_info_.get_size() <= row->get_column_num());
    if (OB_SUCCESS != (ret = mutator_.del_row(table_id_, rowkey)))
    {
      TBSYS_LOG(WARN, "failed to delete cell, err=%d", ret);
      break;
    }
    else
    {
      TBSYS_LOG(DEBUG, "delete cell, rowkey=%s", to_cstring(rowkey));
    }
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
    ob_free(varchar_buff, ObModIds::OB_SQL_EXECUTER);
    varchar_buff = NULL;
  }
  return ret;
}

namespace oceanbase{
  namespace sql{
    REGISTER_PHY_OPERATOR(ObDelete, PHY_DELETE);
  }
}

int64_t ObDelete::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  databuff_printf(buf, buf_len, pos, "Delete(tid=%lu cols_num=%ld)\n",
                  table_id_, cols_desc_.get_column_num());
  if (NULL != child_op_)
  {
    pos += child_op_->to_string(buf+pos, buf_len-pos);
  }
  return pos;
}
