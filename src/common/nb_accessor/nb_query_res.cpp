/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * nb_query_res.cpp
 *
 * Authors:
 *   Junquan Chen <jianming.cjq@taobao.com>
 *
 */

#include "nb_query_res.h"
#include "common/utility.h"

#define COLUMN_NAME_MAP_BUCKET_NUM 100

using namespace oceanbase;
using namespace common;
using namespace nb_accessor;

int QueryRes::get_row(TableRow** table_row)
{
  *table_row = &cur_row_;
  return OB_SUCCESS;
}

int QueryRes::next_row()
{
  int ret = OB_SUCCESS;
  int64_t column_index = 0;
  ObCellInfo* cell = NULL;
  bool is_row_changed = false;

  // in last next_row(), we iterate to last cell of row.
  // so advance scanner_iter_ before we check next row.
  if (first_row_)
  {
    first_row_ = false;
  }
  else
  {
    ++scanner_iter_;
  }

  // get first cell and check if begin cell of row.
  ret = scanner_iter_.get_cell(&cell, &is_row_changed);
  if (OB_ITER_END == ret)
  {
    // return OB_ITER_END;
  }
  else if (OB_SUCCESS != ret)
  {
    TBSYS_LOG(WARN, "scanner iter get cell fail:ret[%d]", ret);
  }
  else if(!is_row_changed)
  {
    ret = OB_ERROR;
    TBSYS_LOG(WARN, "not the begin cell, is_row_changed=%d, cell=%s", 
        is_row_changed, print_cellinfo(cell));
  }
  else if (cur_row_.count() < 1)
  {
    ret = OB_ERR_UNEXPECTED;
    TBSYS_LOG(WARN, "need row cell count :%ld cannot got any cells.", cur_row_.count());
  }
  else
  {
    // iterator cur_row_.count() cell
    while (OB_SUCCESS == ret)
    {
      if (OB_SUCCESS != (ret = cur_row_.set_cell(cell, column_index)))
      {
        TBSYS_LOG(WARN, "cannot set to cur_row_ (%ld), cell=%p", column_index, cell);
      }
      else if (++column_index == cur_row_.count())
      {
        // got enough cell
        break;
      }
      else
      {
        // need more cells;
        ++scanner_iter_;
        ret = scanner_iter_.get_cell(&cell, &is_row_changed);
        if(OB_ITER_END == ret)
        { 
          ret = OB_ERR_UNEXPECTED;
          TBSYS_LOG(WARN, "current cell stream end before got enough cell, index:(%ld), count:(%ld)",
              column_index, cur_row_.count());
        }
        else if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "get cell from scanner error, index:(%ld), count:(%ld)",
              column_index, cur_row_.count());
        }
        else if (is_row_changed)
        {
          ret = OB_ERR_UNEXPECTED;
          TBSYS_LOG(WARN, "current row changed end before got enough cell, index:(%ld), count:(%ld)",
              column_index, cur_row_.count());
        }
      }
    }
    if(OB_SUCCESS == ret && cur_row_.count() != column_index)
    {
      ret = OB_ERR_UNEXPECTED;
      TBSYS_LOG(WARN, "column from scanner for one row is not enough, "
          "column_index[%ld], cur_row_.count[%ld]", column_index, cur_row_.count());
    }

  }

  return ret;
}

int QueryRes::init(const SC& sc)
{
  int ret = OB_SUCCESS;

  ret = sc.get_exec_status();
  if(OB_SUCCESS != ret)
  {
    TBSYS_LOG(WARN, "SC exec fail:ret[%d]", ret);
  }

  //构建列名到列序号的映射表
  if(OB_SUCCESS == ret)
  {
    ret = cell_map_.create(COLUMN_NAME_MAP_BUCKET_NUM);
    if(OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "create hash map fail:ret[%d]", ret);
    }
  }
  if(OB_SUCCESS == ret)
  {
    for(int32_t i=0;i<sc.count() && OB_SUCCESS == ret;i++)
    {
      int err = 0;
      const char* col_name = NULL;
      sc.at(i, col_name);
      if(NULL == col_name)
      {
        ret = OB_ERROR;
        TBSYS_LOG(WARN, "col_name is null");
      }

      if(OB_SUCCESS == ret)
      {
        err = cell_map_.set(col_name, i);
        if(hash::HASH_INSERT_SUCC != err)
        {
          ret = OB_ERROR;
          TBSYS_LOG(WARN, "hash map insert error:err[%d]", err);
        }
        else
        {
          TBSYS_LOG(DEBUG, "add [%s] to cell map", col_name);
        }
      }
    }
  }

  if(OB_SUCCESS == ret)
  {
    scanner_iter_ = scanner_.begin();
    first_row_ = true;
  }

  ObCellInfo* buf = NULL;
  if(OB_SUCCESS == ret)
  {
    buf = reinterpret_cast<ObCellInfo *>(ob_malloc(sizeof(ObCellInfo) * sc.count(), ObModIds::OB_NB_ACCESSOR));
    if(NULL == buf)
    {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      TBSYS_LOG(WARN, "allocate memory fail");
    }
  }

  if(OB_SUCCESS == ret)
  {
    ret = cur_row_.init(&cell_map_, buf, sc.count());
    if(OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "init table row fail:ret[%d]", ret);
    }
  }

  return ret;

}

QueryRes::QueryRes() : first_row_(false)
{
}

QueryRes::~QueryRes()
{
}


TableRow* QueryRes::get_only_one_row()
{
  TableRow* ret = NULL;
  int err = OB_SUCCESS;

  err = next_row();
  if(OB_SUCCESS == err)
  {
    err = get_row(&ret);
  }
  return ret;
}
