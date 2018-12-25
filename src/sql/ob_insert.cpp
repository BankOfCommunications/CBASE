/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_insert.cpp
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#include "ob_insert.h"
#include "common/utility.h"
#include "mergeserver/ob_ms_rpc_proxy.h"
#include "common/ob_obj_cast.h"
#include "common/ob_trace_log.h"
using namespace oceanbase::sql;
using namespace oceanbase::common;

ObInsert::ObInsert()
  :rpc_(NULL), table_id_(OB_INVALID_ID), is_replace_(true)
{
}

ObInsert::~ObInsert()
{
}

void ObInsert::reset()
{
  rpc_ = NULL;
  table_id_ = OB_INVALID_ID;
  is_replace_ = true;
  mutator_.clear();
  ObSingleChildPhyOperator::reset();
}

void ObInsert::reuse()
{
  rpc_ = NULL;
  table_id_ = OB_INVALID_ID;
  is_replace_ = true;
  mutator_.clear();
  ObSingleChildPhyOperator::reuse();
}

int ObInsert::open()
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
    if (OB_SUCCESS != (ret = mutator_.reuse()))
    {
      TBSYS_LOG(WARN, "fail to reset mutator. ret=%d", ret);
    }
    else if (OB_SUCCESS != (ret = insert_by_mutator()))
    {
      TBSYS_LOG(WARN, "fail to insert by mutator. ret=%d", ret);
    }
  }
  return ret;
}

int ObInsert::close()
{
  mutator_.reuse();
  return ObSingleChildPhyOperator::close();
}

inline bool ObInsert::is_rowkey_column(uint64_t column_id)
{
  bool ret = false;
  uint64_t cid = OB_INVALID_ID;
  for (int64_t i = 0; i < rowkey_info_.get_size(); ++i)
  {
    if (OB_SUCCESS != (ret = rowkey_info_.get_column_id(i, cid)))
    {
      TBSYS_LOG(WARN, "failed to get column id, err=%d", ret);
      break;
    }
    else if (column_id == cid)
    {
      ret = true;
      break;
    }
  }
  return ret;
}

int ObInsert::insert_by_mutator()
{
  int ret = OB_SUCCESS;
  OB_ASSERT(child_op_);
  const ObRow *row = NULL;
  uint64_t tid = OB_INVALID_ID;
  uint64_t cid = OB_INVALID_ID;
  const ObObj *cell = NULL;
  ObObj rowkey_objs[OB_MAX_ROWKEY_COLUMN_NUMBER];
  ObRowkey rowkey;
  ObObj cell_nop;
  cell_nop.set_ext(ObActionFlag::OP_NOP);
  ObObj data_type;
  ObObj casted_cells[OB_MAX_ROWKEY_COLUMN_NUMBER+1];
  char *varchar_buff = NULL;

  if (NULL == (varchar_buff = (char*)ob_malloc(OB_MAX_VARCHAR_LENGTH*(OB_MAX_ROWKEY_COLUMN_NUMBER+1), ObModIds::OB_SQL_INSERT)))
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
        TBSYS_LOG(USER_ERROR, "primary key can not be empty");
        ret = OB_ERR_INSERT_NULL_ROWKEY;
        break;
      }
      else if (OB_SUCCESS != (ret = cols_desc_.get_by_id(table_id_, cid, idx, data_type)))
      {
        TBSYS_LOG(USER_ERROR, "primary key can not be empty");
        ret = OB_ERR_INSERT_NULL_ROWKEY;
        break;
      }
      else if (OB_SUCCESS != (ret = row->raw_get_cell(idx, cell, tid, cid)))// ignore tid and cid
      {
        TBSYS_LOG(USER_ERROR, "primary key can not be empty");
        ret = OB_ERR_INSERT_NULL_ROWKEY;
        break;
      }
      else
      {
        const ObObj *res_cell = NULL;
        if (OB_SUCCESS != (ret = obj_cast(*cell, data_type, casted_cells[i], res_cell)))
        {
          TBSYS_LOG(USER_ERROR, "primary key can not be empty");
          ret = OB_ERR_INSERT_NULL_ROWKEY;
          break;
        }
        else
        {
          rowkey_objs[i] = *res_cell;
        }
      }
    }
    if (OB_UNLIKELY(OB_SUCCESS != ret))
    {
      break;
    }
    else
    {
      rowkey.assign(rowkey_objs, rowkey_info_.get_size());
    }
    if (rowkey_info_.get_size() >= row->get_column_num())
    {
      // We have got no columns other than primary key.
      // This is a special protocol with the updateserver.
      if (OB_SUCCESS != (ret = mutator_.insert(table_id_, rowkey, OB_INVALID_ID, cell_nop)))
      {
        TBSYS_LOG(WARN, "failed to insert cell, err=%d", ret);
        break;
      }
      else
      {
        TBSYS_LOG(DEBUG, "insert cell, rowkey=%s cell=%s", to_cstring(rowkey), to_cstring(cell_nop));
      }
      continue;
    }
    // other columns
    for (int64_t i = 0; i < row->get_column_num(); ++i)
    {
      if (OB_SUCCESS != (ret = row->raw_get_cell(i, cell, tid, cid))) // ignore tid and cid here
      {
        TBSYS_LOG(WARN, "failed to get cell, err=%d i=%ld", ret, i);
        break;
      }
      else if (OB_SUCCESS != (ret = cols_desc_.get_by_idx(i, tid, cid, data_type))) // this is the real tid & cid
      {
        TBSYS_LOG(WARN, "failed to get cid from row desc, err=%d", ret);
        break;
      }
      if (is_rowkey_column(cid))
      {
        continue;
      }
      else
      {
        // insert the cell with rowkey
        ObString varchar;
        varchar.assign_ptr(varchar_buff + OB_MAX_ROWKEY_COLUMN_NUMBER * OB_MAX_VARCHAR_LENGTH, OB_MAX_VARCHAR_LENGTH);
        ObObj &casted_cell = casted_cells[OB_MAX_ROWKEY_COLUMN_NUMBER];
        casted_cell.set_varchar(varchar);
        const ObObj *res_cell = NULL;
        if (OB_SUCCESS != (ret = obj_cast(*cell, data_type, casted_cell, res_cell)))
        {
          TBSYS_LOG(WARN, "failed to cast obj, err=%d", ret);
          break;
        }
        else if (OB_SUCCESS != (ret = mutator_.insert(table_id_, rowkey, cid, *res_cell)))
        {
          TBSYS_LOG(WARN, "failed to insert cell, err=%d", ret);
          break;
        }
        else
        {
          TBSYS_LOG(DEBUG, "insert cell, rowkey=%s cell=%s", to_cstring(rowkey), to_cstring(*res_cell));
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
  FILL_TRACE_LOG("before_send_ups");
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

namespace oceanbase{
  namespace sql{
    REGISTER_PHY_OPERATOR(ObInsert, PHY_INSERT);
  }
}

int64_t ObInsert::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  databuff_printf(buf, buf_len, pos, "Insert(tid=%lu cols_num=%ld)\n",
                  table_id_, cols_desc_.get_column_num());
  if (NULL != child_op_)
  {
    pos += child_op_->to_string(buf+pos, buf_len-pos);
  }
  return pos;
}
