/**
 * (C) 2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * libobjoin.cpp : Implementation of join functions
 *
 * Authors:
 *   Yanran <yanran.hfs@taobao.com>
 *
 */

#include "oceanbase.h"
#include "libobjoin.h"
#include "libobapi.h"

OB_ERR_CODE ob_scan_res_join_append(OB_SCAN* scan_st,
                                    const char* res_columns,
                                    const char* join_table,
                                    const char* join_column,
                                    const char* as_name)
{
  OB_ERR_CODE err_code = OB_ERR_SUCCESS;
  ENSURE_COND(NULL != scan_st && NULL != res_columns
              && NULL != join_table && NULL != join_column);
  if (OB_ERR_SUCCESS == err_code)
  {
    scan_st->join_st.join_list.push_back(
        st_ob_join::st_res_join_desc(res_columns,
                                     join_table,
                                     join_column,
                                     as_name)
        );
  }
  return err_code;
}

st_ob_join::st_res_join_desc::st_res_join_desc() :
    res_columns(NULL), join_table(NULL), join_column(NULL)
{
}

st_ob_join::st_res_join_desc::st_res_join_desc(const char* new_res_columns,
                                               const char* new_join_table,
                                               const char* new_join_column,
                                               const char* new_as_name) :
    res_columns(NULL), join_table(NULL), join_column(NULL), as_name(NULL)
{
  set_res_columns(new_res_columns);
  set_join_table(new_join_table);
  set_join_column(new_join_column);
  set_as_name(new_as_name);
}

st_ob_join::st_res_join_desc::~st_res_join_desc()
{
  clear();
}

st_ob_join::st_res_join_desc::st_res_join_desc(const st_res_join_desc& copy) :
    res_columns(NULL), join_table(NULL), join_column(NULL), as_name(NULL)
{
  set_res_columns(copy.res_columns);
  set_join_table(copy.join_table);
  set_join_column(copy.join_column);
  set_as_name(copy.as_name);
}

void st_ob_join::st_res_join_desc::set_res_columns(const char* new_res_columns)
{
  if (NULL == new_res_columns)
  {
    TBSYS_LOG(ERROR, "argument is invalid, new_res_columns=NULL");
  }
  else
  {
    unsigned int new_res_columns_len = static_cast<uint32_t>(strlen(new_res_columns));
    if (NULL == res_columns || strlen(res_columns) < new_res_columns_len)
    {
      if (NULL != res_columns)
      {
        ob_free(res_columns);
      }
      res_columns = static_cast<char*>(ob_malloc(new_res_columns_len + 1));
    }
    if (NULL == res_columns)
    {
      TBSYS_LOG(ERROR, "ob_malloc error");
    }
    else
    {
      strcpy(res_columns, new_res_columns);
      unsigned word_begin = 0;
      for (unsigned cursor = 0; cursor < new_res_columns_len; cursor++)
      {
        while (cursor < new_res_columns_len && res_columns[cursor] != ',')
        {
          cursor++;
        }
        if (cursor > word_begin)
        {
          res_column_list.push_back(
              std::string(res_columns + word_begin, cursor - word_begin));
        }
        if (cursor < new_res_columns_len)
        {
          cursor++;
          word_begin = cursor;
        }
      }
      for (std::vector<std::string>::iterator iter = res_column_list.begin();
           iter != res_column_list.end(); iter++)
      {
        TBSYS_LOG(DEBUG, "foreign key column: %s", iter->c_str());
      }
    }
  }
}

void st_ob_join::st_res_join_desc::set_join_table(const char* new_join_table)
{
  if (NULL == new_join_table)
  {
    TBSYS_LOG(ERROR, "argument is invalid, new_res_table=NULL");
  }
  else
  {
    unsigned new_join_table_len = static_cast<int32_t>(strlen(new_join_table));
    if (NULL == join_table || strlen(join_table) < new_join_table_len)
    {
      if (NULL != join_table)
      {
        ob_free(join_table);
      }
      join_table = static_cast<char*>(ob_malloc(new_join_table_len + 1));
    }
    if (NULL == join_table)
    {
      TBSYS_LOG(ERROR, "ob_malloc error");
    }
    else
    {
      strcpy(join_table, new_join_table);
    }
  }
}

void st_ob_join::st_res_join_desc::set_join_column(const char* new_join_column)
{
  if (NULL == new_join_column)
  {
    TBSYS_LOG(ERROR, "argument is invalid, new_join_column=NULL");
  }
  else
  {
    unsigned new_join_column_len = static_cast<int32_t>(strlen(new_join_column));
    if (NULL == join_column || strlen(join_column) < new_join_column_len)
    {
      if (NULL != join_column)
      {
        ob_free(join_column);
      }
      join_column = static_cast<char*>(ob_malloc(new_join_column_len + 1));
    }
    if (NULL == join_column)
    {
      TBSYS_LOG(ERROR, "ob_malloc error");
    }
    else
    {
      strcpy(join_column, new_join_column);
    }
  }
}

void st_ob_join::st_res_join_desc::set_as_name(const char* new_as_name)
{
  unsigned new_as_name_len = 0;
  bool default_name = false;
  if (NULL == new_as_name || strlen(new_as_name) == 0)
  {
    default_name = true;
    // default name is "join_table.join_column"
    new_as_name_len = static_cast<int32_t>(strlen(join_table) + strlen(join_column) + 1);
  }
  else
  {
    new_as_name_len = static_cast<int32_t>(strlen(new_as_name));
  }
  if (NULL == as_name || strlen(as_name) < new_as_name_len)
  {
    if (NULL != as_name)
    {
      ob_free(as_name);
    }
    as_name = static_cast<char*>(ob_malloc(new_as_name_len + 1));
  }
  if (NULL == as_name)
  {
    TBSYS_LOG(ERROR, "ob_malloc error");
  }
  else
  {
    if (default_name)
    {
      strcpy(as_name, join_table);
      strcat(as_name, ".");
      strcat(as_name, join_column);
    }
    else
    {
      strcpy(as_name, new_as_name);
    }
  }
}

void st_ob_join::st_res_join_desc::clear()
{
  if (NULL != res_columns)
  {
    ob_free(res_columns);
    res_columns = NULL;
  }
  if (NULL != join_table)
  {
    ob_free(join_table);
    join_table = NULL;
  }
  if (NULL != join_column)
  {
    ob_free(join_column);
    join_column = NULL;
  }
}

OB_ERR_CODE ob_exec_res_join(OB* ob,
                             OB_RES* res_st,
                             OB_JOIN* join_st,
                             OB_RES* &final_res)
{
  OB_ERR_CODE err_code = OB_ERR_SUCCESS;
  OB_GET* get_st = NULL;
  OB_RES* join_res = NULL;
  ENSURE_COND(NULL != ob && NULL != res_st && NULL != join_st);
  if (OB_ERR_SUCCESS == err_code)
  {
    final_res = ob_acquire_res_st(ob);
    if (NULL == final_res)
    {
      TBSYS_LOG(ERROR, "ob_acquire_res_st error");
      err_code = ob_errno();
    }
    else
    {
      final_res->scanner.resize(1);
      final_res->cur_scanner = final_res->scanner.begin();
    }
  }
  if (OB_ERR_SUCCESS == err_code)
  {
    get_st = ob_acquire_get_st(ob);
    if (NULL == get_st)
    {
      TBSYS_LOG(ERROR, "ob_acquire_get_st error");
      err_code = ob_errno();
    }
  }
  if (OB_ERR_SUCCESS == err_code)
  {
    err_code = prepare_join_get_st(join_st, res_st, get_st);
    if (OB_ERR_SUCCESS != err_code)
    {
      TBSYS_LOG(ERROR, "prepare_join_get_st error, err_code=%d", err_code);
    }
    else
    {
      join_res = ob_exec_get(ob, get_st);
      if (NULL == join_res)
      {
        TBSYS_LOG(ERROR, "ob_exec_get error, err_code=%d", ob_errno());
        err_code = ob_errno();
      }
      else
      {
        err_code = append_join_res(join_st, res_st, join_res, final_res);
        if (OB_ERR_SUCCESS != err_code)
        {
          TBSYS_LOG(ERROR, "join_res error, err_code=%d", err_code);
        }
        else
        {
          final_res->cur_scanner = final_res->scanner.begin();
        }
      }
    }
  }
  if (NULL != get_st)
  {
    ob_release_get_st(ob, get_st);
    get_st = NULL;
  }
  if (NULL != join_res)
  {
    ob_release_res_st(ob, join_res);
    join_res = NULL;
  }
  return err_code;
}

int64_t datetime_to_int(OB_DATETIME v)
{
  return static_cast<int64_t>(v.tv_sec) * 1000000 + v.tv_usec;
}

OB_ERR_CODE extract_foreign_key(OB_ROW* row,
                                std::vector<std::string> res_column_list,
                                sbuffer<> &rowkey)
{
  OB_ERR_CODE err_code = OB_ERR_SUCCESS;
  ENSURE_COND(NULL != row && NULL != rowkey.ptr());
  if (OB_ERR_SUCCESS == err_code)
  {
    rowkey.length() = 0;
    std::vector<std::string>::iterator col = res_column_list.begin();
    for (; col != res_column_list.end(); col++)
    {
      int col_found = -1;
      for (int64_t i = 0; i < row->cell_num; i++)
      {
        if (row->cell[i].column_len == static_cast<int64_t>(col->length()))
        {
          col_found = memcmp(row->cell[i].column, col->c_str(),
              row->cell[i].column_len);
        }
        if (0 == col_found)
        {
          int err = OB_SUCCESS;
          OB_VARCHAR v_varchar;
          switch (row->cell[i].v.type)
          {
            case OB_INT_TYPE:
              err = serialization::encode_i64(rowkey.ptr(), rowkey.capacity(),
                  rowkey.length(), row->cell[i].v.v.v_int);
              if (OB_SUCCESS != err)
              {
                TBSYS_LOG(ERROR, "encode_i64 error, err=%d", err);
              }
              else
              {
                TBSYS_LOG(DEBUG, "Extrace foreign key(\"%s\" = %ld), "
                    "extend row key to %s",
                    col->c_str(), row->cell[i].v.v.v_int,
                    get_display_row_key(rowkey.ptr(), rowkey.length()));
              }
              break;
            //add lijianqiang [INT_32] 20150930:b
            case OB_INT32_TYPE:
              err = serialization::encode_i32(rowkey.ptr(), rowkey.capacity(),
                  rowkey.length(), row->cell[i].v.v.v_int32);
              if (OB_SUCCESS != err)
              {
                TBSYS_LOG(ERROR, "encode_i32 error, err=%d", err);
              }
              else
              {
                TBSYS_LOG(DEBUG, "Extrace foreign key(\"%s\" = %d), "
                    "extend row key to %s",
                    col->c_str(), row->cell[i].v.v.v_int32,
                    get_display_row_key(rowkey.ptr(), rowkey.length()));
              }
              break;
            //add 20150930:e
            case OB_VARCHAR_TYPE:
              v_varchar = row->cell[i].v.v.v_varchar;
              err = rowkey.append(v_varchar.p, v_varchar.len);
              if (OB_SUCCESS != err)
              {
                TBSYS_LOG(ERROR, "rowkey.assign error, err=%d", err);
              }
              else
              {
                TBSYS_LOG(DEBUG, "Extrace foreign key(\"%s\" = \"%s\"), "
                    "extend row key to %s",
                    col->c_str(), get_display_row_key(v_varchar.p, v_varchar.len),
                    get_display_row_key(rowkey.ptr(), rowkey.length()));
              }
            //add peiouya [DATE_TIME] 20150906:b
              break;
            case OB_DATE_TYPE:
            case OB_TIME_TYPE:
            //add 20150906:e
            case OB_DATETIME_TYPE:
              err = serialization::encode_i64(rowkey.ptr(), rowkey.capacity(),
                  rowkey.length(), datetime_to_int(row->cell[i].v.v.v_datetime));
              if (OB_SUCCESS != err)
              {
                TBSYS_LOG(ERROR, "encode_i64 error, err=%d", err);
              }
              else
              {
                TBSYS_LOG(DEBUG, "Extrace foreign key(\"%s\" = %ld), "
                    "extend row key to %s",
                    col->c_str(), datetime_to_int(row->cell[i].v.v.v_datetime),
                    get_display_row_key(rowkey.ptr(), rowkey.length()));
              }
              break;
            default:
              err = OB_ERROR;
              TBSYS_LOG(ERROR, "Unsupported join column type: %d",
                  row->cell[i].v.type);
          }
          if (OB_SUCCESS != err)
          {
            err_code = err_code_map(err);
            ob_set_err_with_default(err_code);
          }
          break;
        }
      }
      if (-1 == col_found)
      {
        TBSYS_LOG(ERROR, "Error column name: %s", col->c_str());
        err_code = OB_ERR_INVALID_PARAM;
        char buf[BUFSIZ];
        snprintf(buf, BUFSIZ, "Error column name: %s", col->c_str());
        ob_set_err(err_code, buf);
        break;
      }
    }
    if (OB_ERR_SUCCESS == err_code)
    {
      if (rowkey.length() == 0)
      {
        TBSYS_LOG(ERROR, "do not match any foreign keys");
        err_code = OB_ERR_INVALID_PARAM;
      }
    }
  }
  return err_code;
}

OB_ERR_CODE push_to_join_get_st(OB_GET* get_st,
                                const sbuffer<> &rowkey,
                                const char* join_table,
                                const char* join_column)
{
  OB_ERR_CODE err_code = OB_ERR_SUCCESS;
  ENSURE_COND(NULL != get_st && NULL != join_table && NULL != join_column);
  if (OB_ERR_SUCCESS == err_code)
  {
    err_code = ob_get_cell(get_st, join_table, rowkey.ptr(), rowkey.length(),
        join_column);
    if (OB_ERR_SUCCESS != err_code)
    {
      TBSYS_LOG(ERROR, "ob_get_cell error, err_code=%d", err_code);
    }
    else
    {
      TBSYS_LOG(DEBUG, "JOIN GET Table=%s rowkey=%s Column=%s",
          join_table, get_display_row_key(rowkey.ptr(), rowkey.length()),
          join_column);
    }
  }
  return err_code;
}

OB_ERR_CODE prepare_join_get_st(OB_JOIN* join_st,
                                OB_RES* res,
                                OB_GET* get_st)
{
  OB_ERR_CODE err_code = OB_ERR_SUCCESS;
  ENSURE_COND(NULL != join_st && NULL != res && NULL != get_st);
  if (OB_ERR_SUCCESS == err_code)
  {
    OB_ROW* cur_row = ob_fetch_row(res);
    while (NULL != cur_row)
    {
      OB_JOIN::JoinDescList::iterator join_iter = join_st->join_list.begin();
      for (; join_iter != join_st->join_list.end(); join_iter++)
      {
        sbuffer<> rowkey;
        err_code = extract_foreign_key(cur_row, join_iter->res_column_list, rowkey);
        if (OB_ERR_SUCCESS != err_code)
        {
          TBSYS_LOG(ERROR, "extract_foreign_key error, err=%d", err_code);
        }
        else
        {
          err_code = push_to_join_get_st(get_st, rowkey, join_iter->join_table,
              join_iter->join_column);
          if (OB_ERR_SUCCESS != err_code)
          {
            TBSYS_LOG(ERROR, "push_to_join_get_st error, err=%d", err_code);
          }
        }
      }
      cur_row = ob_fetch_row(res);
    }
  }
  return err_code;
}

OB_ERR_CODE append_to_res(const char* table, int64_t table_len,
                          const char* row_key, int64_t row_key_len,
                          const char* column, int64_t column_len,
                          const OB_VALUE* v, OB_RES* res)
{
  OB_ERR_CODE err_code = OB_ERR_SUCCESS;
  ENSURE_COND(NULL != table && table_len > 0
              && NULL != row_key && row_key_len > 0
              && NULL != column && column_len > 0
              && NULL != res);
  if (OB_ERR_SUCCESS == err_code)
  {
    ObCellInfo cell_info;
    cell_info.table_name_.assign_ptr(const_cast<char*>(table), static_cast<int32_t>(table_len));
    cell_info.table_id_ = OB_INVALID_ID;
    // TODO set new rowkey
    //cell_info.row_key_.assign_ptr(const_cast<char*>(row_key), static_cast<int32_t>(row_key_len));
    cell_info.column_name_.assign_ptr(const_cast<char*>(column), static_cast<int32_t>(column_len));
    cell_info.column_id_ = OB_INVALID_ID;
    if (NULL == v)
    {
      cell_info.value_.set_null();
    }
    else
    {
      switch (v->type)
      {
        case OB_INT_TYPE:
          cell_info.value_.set_int(v->v.v_int);
          break;
        //add lijianqiang [INT_32] 20150930:b
        case OB_INT32_TYPE:
          cell_info.value_.set_int32(v->v.v_int32);
          break;
        //add 20150930:e
        case OB_VARCHAR_TYPE:
          cell_info.value_.set_varchar(
              ObString(static_cast<int32_t>(v->v.v_varchar.len), static_cast<int32_t>(v->v.v_varchar.len),
                  v->v.v_varchar.p));
          break;
        case OB_DATETIME_TYPE:
          cell_info.value_.set_precise_datetime(
              datetime_to_int(v->v.v_datetime));
          break;
        //add peiouya [DATE_TIME] 20150906:b
        case OB_DATE_TYPE:
          cell_info.value_.set_date(
              datetime_to_int(v->v.v_datetime));
          break;
        case OB_TIME_TYPE:
          cell_info.value_.set_time(
              datetime_to_int(v->v.v_datetime));
          break;
        //add 20150906:e
        case OB_DOUBLE_TYPE:
          cell_info.value_.set_double(v->v.v_double);
          break;
        default:
          TBSYS_LOG(ERROR, "Unknown type, type=%d", v->type);
          err_code = OB_ERR_ERROR;
          ob_set_err_with_default(err_code);
      }
    }
    if (OB_ERR_SUCCESS == err_code)
    {
      int err = res->scanner.back().add_cell(cell_info);
      if (OB_SIZE_OVERFLOW == err)
      {
        res->scanner.resize(res->scanner.size() + 1);
        err = res->scanner.back().add_cell(cell_info);
      }
      if (OB_SUCCESS != err)
      {
        TBSYS_LOG(ERROR, "add_cell error, err=%d", err);
        err_code = err_code_map(err);
        ob_set_err_with_default(err_code);
      }
      else
      {
        TBSYS_LOG(DEBUG, "add_cell Table=%.*s Row_key=%s Column=%.*s",
            (int)table_len, table, get_display_row_key(row_key, row_key_len),
            (int)column_len, column);
      }
    }
  }
  return err_code;
}

OB_ERR_CODE append_cell_to_res(OB_CELL* cell, OB_RES* res)
{
  OB_ERR_CODE err_code = OB_ERR_SUCCESS;
  ENSURE_COND(NULL != cell && NULL != res);
  if (OB_ERR_SUCCESS == err_code)
  {
    err_code = append_to_res(cell->table, cell->table_len,
                             cell->row_key, cell->row_key_len,
                             cell->column, cell->column_len,
                             &(cell->v), res);
  }
  return err_code;
}

OB_ERR_CODE append_row_to_res(OB_ROW* row, OB_RES* res)
{
  OB_ERR_CODE err_code = OB_ERR_SUCCESS;
  ENSURE_COND(NULL != row && NULL != res);
  if (OB_ERR_SUCCESS == err_code)
  {
    for (int i = 0; OB_ERR_SUCCESS == err_code && i < row->cell_num; i++)
    {
      err_code = append_cell_to_res(&(row->cell[i]), res);
    }
  }
  return err_code;
}

OB_ERR_CODE append_join_cell_to_res(OB_RES* join_res,
                                    OB_CELL* &cell,
                                    const char* res_table,
                                    int64_t res_table_len,
                                    const char* res_row_key,
                                    int64_t res_row_key_len,
                                    const sbuffer<> &rowkey,
                                    OB_JOIN::JoinDescList::iterator &join_it,
                                    OB_RES* res)
{
  OB_ERR_CODE err_code = OB_ERR_SUCCESS;
  ENSURE_COND(NULL != cell && NULL != res);
  if (OB_ERR_SUCCESS == err_code)
  {
    if (NULL == cell)
    {
      TBSYS_LOG(ERROR, "join_res error");
      err_code = OB_ERR_ERROR;
      ob_set_err_with_default(err_code);
    }
    else if (memcmp(cell->table, join_it->join_table, cell->table_len) != 0
        || memcmp(cell->row_key, rowkey.ptr(), cell->row_key_len) != 0
        || cell->row_key_len != rowkey.length()
        || (!cell->is_row_not_exist
            && memcmp(cell->column, join_it->join_column, cell->column_len)))
    {
      cell = ob_fetch_cell(join_res);
    }
    if (OB_ERR_SUCCESS == err_code)
    {
      if (NULL == cell)
      {
        TBSYS_LOG(ERROR, "join_res error");
        err_code = OB_ERR_ERROR;
        ob_set_err_with_default(err_code);
      }
      else if (memcmp(cell->table, join_it->join_table, cell->table_len) != 0
          || memcmp(cell->row_key, rowkey.ptr(), cell->row_key_len) != 0
          || cell->row_key_len != rowkey.length()
          || (!cell->is_row_not_exist
              && memcmp(cell->column, join_it->join_column, cell->column_len)))
      {
        TBSYS_LOG(ERROR, "join cell not match get_st");
        TBSYS_LOG(ERROR, "join cell Table=%.*s Row_key=%s Column=%.*s Row_not_exist=%d",
            (int)cell->table_len, cell->table,
            get_display_row_key(cell->row_key, cell->row_key_len),
            (int)cell->column_len, cell->column,
            cell->is_row_not_exist);
        TBSYS_LOG(ERROR, "join st Table=%s Row_key=%s Column=%s",
            join_it->join_table,
            get_display_row_key(rowkey.ptr(), rowkey.length()),
            join_it->join_column);
        err_code = OB_ERR_ERROR;
        ob_set_err_with_default(err_code);
      }
      else
      {
        OB_VALUE* v = NULL;
        if (!cell->is_row_not_exist && !cell->is_null)
        {
          v = &(cell->v);
        }
        if (NULL != v)
        {
          TBSYS_LOG(DEBUG, "v.type = %d", v->type);
        }
        err_code = append_to_res(res_table, res_table_len,
                                 res_row_key, res_row_key_len,
                                 join_it->as_name, strlen(join_it->as_name),
                                 v, res);
        if (OB_ERR_SUCCESS != err_code)
        {
          TBSYS_LOG(ERROR, "append_to_res error");
        }
      }
    }
  }
  return err_code;
}

OB_ERR_CODE append_join_res(OB_JOIN* join_st,
                            OB_RES* res,
                            OB_RES* join_res,
                            OB_RES* final_res)
{
  OB_ERR_CODE err_code = OB_ERR_SUCCESS;
  ENSURE_COND(NULL != join_st && NULL != res
      && NULL != join_res && NULL != final_res);
  if (OB_ERR_SUCCESS == err_code)
  {
    err_code = ob_res_seek_to_begin_row(res);
    if (OB_ERR_SUCCESS != err_code)
    {
      TBSYS_LOG(ERROR, "ob_res_seek_to_begin_row error, err_code=%d", err_code);
    }
  }
  if (OB_ERR_SUCCESS == err_code)
  {
    OB_ROW* res_row = ob_fetch_row(res);
    OB_CELL* join_res_cell = NULL;
    join_res_cell = ob_fetch_cell(join_res);
    while (NULL != res_row && OB_ERR_SUCCESS == err_code)
    {
      err_code = append_row_to_res(res_row, final_res);
      if (OB_ERR_SUCCESS != err_code)
      {
        TBSYS_LOG(ERROR, "append_row_to_res error, err_code=%d", err_code);
      }
      else
      {
        OB_JOIN::JoinDescList::iterator join_iter = join_st->join_list.begin();
        for (; join_iter != join_st->join_list.end(); join_iter++)
        {
          sbuffer<> rowkey;
          err_code = extract_foreign_key(res_row, join_iter->res_column_list, rowkey);
          if (OB_ERR_SUCCESS != err_code)
          {
            TBSYS_LOG(ERROR, "extract_foreign_key error, err=%d", err_code);
          }
          else
          {
            err_code = append_join_cell_to_res(
                join_res,
                join_res_cell,
                res_row->cell[0].table,
                res_row->cell[0].table_len,
                res_row->cell[0].row_key,
                res_row->cell[0].row_key_len,
                rowkey, join_iter, final_res);
            if (OB_ERR_SUCCESS != err_code)
            {
              TBSYS_LOG(ERROR, "append_join_cell_to_res error, err_code=%d",
                  err_code);
            }
          }
        }
        res_row = ob_fetch_row(res);
      }
    }
    if (OB_ERR_SUCCESS == err_code)
    {
      bool is_fullfilled = false;
      int64_t fullfilled_item_num = 0;
      int64_t version = 0;
      int64_t whole_result_row_num = 0;
      res->scanner.front().get_is_req_fullfilled(
          is_fullfilled, fullfilled_item_num);
      version = res->scanner.front().get_data_version();
      whole_result_row_num = res->scanner.front().get_whole_result_row_num();
      final_res->scanner.front().set_is_req_fullfilled(
          is_fullfilled, fullfilled_item_num);
      final_res->scanner.front().set_data_version(version);
      final_res->scanner.front().set_whole_result_row_num(whole_result_row_num);
    }
  }
  return err_code;
}

OB_JOIN* ob_acquire_join_st(OB* ob)
{
  (void)(ob);
  OB_JOIN* res = new (std::nothrow) st_ob_join;
  return res;
}

void ob_release_join_st(OB* ob, OB_JOIN* join_st)
{
  (void)(ob);
  delete join_st;
  join_st = NULL;
}

