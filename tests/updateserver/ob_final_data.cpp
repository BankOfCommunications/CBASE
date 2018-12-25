/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * ob_final_data.cpp is for what ...
 *
 * Version: $id: ob_final_data.cpp,v 0.1 11/1/2010 3:47p wushi Exp $
 *
 * Authors:
 *   wushi <wushi.ly@taobao.com>
 *     - some work details if you want
 *
 */
#include <utility>
#include <sstream>
#include <algorithm>
#include <string>
#include "ob_final_data.h"
#include "common/ob_schema.h"
#include "common/ob_define.h"
#include "common/ob_action_flag.h"
#include "tbsys.h"
using namespace std;
using namespace oceanbase;
using namespace oceanbase::common;
bool ObRowInfo::operator <(const ObRowInfo &other) const
{
  bool result = false;
  if (table_name_ < other.table_name_)
  {
    result = true;
  }
  else if (table_name_ > other.table_name_)
  {
    result = false;
  }
  else
  {
    result = (row_key_ < other.row_key_);
  }
  return  result;
}

bool ObRowInfo::operator ==(const ObRowInfo &other) const
{
  return((table_name_ == other.table_name_) && (row_key_ == other.row_key_));
}

ObFinalResult::ObFinalResult()
{
  row_infos_ = new std::map<ObRowInfo, std::vector<oceanbase::common::ObCellInfo> >();
  cell_array_ = new ObCellArray();
}

ObFinalResult::~ObFinalResult()
{
  row_infos_->clear();
  cell_array_->clear();
  delete row_infos_;
  delete cell_array_;
}

int ObFinalResult::apply_join(ObSchemaManager &schema_mgr,
                              oceanbase::common::ObCellInfo& left_cell,
                              const oceanbase::common::ObJoinInfo &join_info)
{
  int err = OB_SUCCESS;
  ObCellInfo get_cell;
  const ObSchema *right_schema = NULL;
  uint64_t right_table_id = OB_INVALID_ID;
  uint64_t right_column_id = OB_INVALID_ID;
  int32_t right_key_start_pos = 0;
  int32_t right_key_end_pos = 0;
  ObScanner right_scanner;
  ObGetParam get_param;
  get_param.reset();
  join_info.get_rowkey_join_range(right_key_start_pos, right_key_end_pos);
  if (-1 == right_key_start_pos && -1 == right_key_end_pos)
  {
    right_key_start_pos = 0;
    right_key_end_pos = left_cell.row_key_.length();
  }
  else
  {
    right_key_end_pos += 1;
  }
  get_cell.row_key_.assign(left_cell.row_key_.ptr() + right_key_start_pos,
                           right_key_end_pos - right_key_start_pos);
  right_table_id  =  join_info.get_table_id_joined();
  right_column_id =  join_info.find_right_column_id(left_cell.column_id_);
  right_schema = schema_mgr.get_table_schema(right_table_id);
  if (NULL == right_schema)
  {
    TBSYS_LOG(WARN,"unexpected error, fail to get right table schema");
    err = OB_ERR_UNEXPECTED;
  }
  if (OB_SUCCESS == err)
  {
    const ObColumnSchema * column_info = NULL;
    get_cell.table_name_.assign(const_cast<char*>(right_schema->get_table_name()),
                                strlen(right_schema->get_table_name()));
    column_info = right_schema->find_column_info(right_column_id);
    if (NULL == column_info)
    {
      TBSYS_LOG(WARN,"unexpected error, fail to get right column schema");
      err = OB_ERR_UNEXPECTED;
    }
    if (OB_SUCCESS == err)
    {
      get_cell.column_name_.assign(const_cast<char*>(column_info->get_name()),
                                   strlen(column_info->get_name()));
      err = get_param.add_cell(get_cell);
    }
  }
  if (OB_SUCCESS == err)
  {
    err = get(get_param,right_scanner,false);
  }
  while (OB_SUCCESS == err)
  {
    ObCellInfo *cur_cell = NULL;
    err = right_scanner.next_cell();
    if (OB_SUCCESS == err)
    {
      err = right_scanner.get_cell(&cur_cell);
    }
    if (OB_SUCCESS == err)
    {
      TBSYS_LOG(DEBUG, "apply one cell");
      err = left_cell.value_.apply(cur_cell->value_);
    }
  }
  if (OB_ITER_END == err)
  {
    err = OB_SUCCESS;
  }
  return err;
}

int ObFinalResult::parse_from_file(const char *  filename, const char *schema_fname)
{
  int err = OB_SUCCESS;
  ObSchemaManager *schema_mgr = new ObSchemaManager;
  tbsys::CConfig config;
  if (!schema_mgr->parse_from_file(schema_fname,config))
  {
    TBSYS_LOG(WARN,"fail to load schema from file [fname:%s]", schema_fname);
    err = OB_SCHEMA_ERROR;
  }
  ifstream input(filename);
  int64_t line_number = 0;
  string line;
  ObCellInfo cur_cell;
  string table_name;
  uint64_t table_id;
  string rowkey;
  string column_name;
  uint64_t column_id;
  string value_type;
  int64_t value;
  string op_type;
  while (getline(input,line) && OB_SUCCESS == err)
  {
    if (input.eof())
    {
      break;
    }
    if (line[0]  == '#')
    {
      line_number ++;
      continue ;
    }
    /// replace all ,
    replace(line.begin(), line.end(),',', ' ');
    istringstream line_stream(line);
    line_stream>>table_name
    >>table_id
    >>rowkey
    >>column_name
    >>column_id
    >>value_type
    >>value
    >>op_type;
    if (!line_stream || count(line.begin(),line.end(), ' ') != 7)
    {
      TBSYS_LOG(WARN,"line format error [line_number:%lld,line:%s]", line_number, line.c_str());
      err = OB_INVALID_ARGUMENT;
    }
    if (!line_stream.eof())
    {
      TBSYS_LOG(WARN,"line format error [line_number:%lld,line:%s]", line_number, line.c_str());
      err = OB_INVALID_ARGUMENT;
    }
    if (OB_SUCCESS == err && value_type != "int")
    {
      TBSYS_LOG(WARN,"value type error [line_number:%lld,value_type:%s]",
                line_number, value_type.c_str());
      err = OB_INVALID_ARGUMENT;
    }
    if (OB_SUCCESS == err
        && op_type != "null"
        && op_type != "row_not_exist"
        && op_type != "noop"
        && op_type != "update"
        && op_type != "insert"
        && op_type != "add"
        && op_type != "del_row"
       )
    {
      TBSYS_LOG(WARN,"optype error [line_number:%lld,optype:%s]",
                line_number, op_type.c_str());
      err = OB_INVALID_ARGUMENT;
    }
    if (OB_SUCCESS == err)
    {
      const ObJoinInfo *join_info = NULL;
      const ObSchema *table_schema = NULL;
      cur_cell.table_name_.assign(const_cast<char*>(table_name.c_str()), table_name.size());
      cur_cell.row_key_.assign(const_cast<char*>(rowkey.c_str()), rowkey.size());
      cur_cell.column_name_.assign(const_cast<char*>(column_name.c_str()), column_name.size());
      cur_cell.table_id_ = table_id;
      cur_cell.column_id_ = column_id;
      if (op_type == "null")
      {
        cur_cell.value_.set_null();
      }
      else if (op_type == "row_not_exist")
      {
        if(row_exist(cur_cell))
        {
          op_type = "null";
          cur_cell.value_.set_null();
        }
        else
        {
          cur_cell.value_.set_ext(ObActionFlag::OP_ROW_DOES_NOT_EXIST);
        }
      }
      else if (op_type == "del_row")
      {
        cur_cell.value_.set_ext(ObActionFlag::OP_DEL_ROW);
      }
      else
      {
        bool is_add = false;
        if (op_type == "add")
        {
          is_add = true;
        }
        cur_cell.value_.set_int(value,is_add);
      }
      if ( op_type != "del_row"
           && op_type != "row_not_exist"
           && NULL != (table_schema =  schema_mgr->get_table_schema(table_id))
           && (NULL != (join_info = table_schema->find_join_info(column_id))))
      {
        err = apply_join(*schema_mgr,cur_cell,*join_info);
      }
      if (OB_SUCCESS == err)
      {
        cur_cell.table_id_ = OB_INVALID_ID;
        cur_cell.column_id_ = OB_INVALID_ID;

        if (OB_SUCCESS == err)
        {
          err = add_cell(cur_cell);
        }
      }
    }
    line_number ++;
  }
  return err;
}


bool ObFinalResult::row_exist(const ObCellInfo & add_cell)
{
  ObRowInfo row_info;
  row_info.table_name_ = add_cell.table_name_;
  row_info.row_key_ = add_cell.row_key_;
  map<ObRowInfo, vector<ObCellInfo> >::iterator target = row_infos_->find(row_info);
  return (target != row_infos_->end() && target->second.size() > 0);
}

int ObFinalResult::add_cell(const ObCellInfo & add_cell)
{
  int err = OB_SUCCESS;
  ObRowInfo row_info;
  row_info.table_name_ = add_cell.table_name_;
  row_info.row_key_ = add_cell.row_key_;
  map<ObRowInfo, vector<ObCellInfo> >::iterator target = row_infos_->find(row_info);
  ObCellInfo *cell_out = NULL;
  err = cell_array_->append(add_cell, cell_out);
  if (OB_SUCCESS == err)
  {
    if (ObActionFlag::OP_DEL_ROW == cell_out->value_.get_ext()
        && row_infos_->end() != target)
    {
      target->second.clear();
    }
    else if (ObActionFlag::OP_ROW_DOES_NOT_EXIST == cell_out->value_.get_ext())
    {
      /// did nothing
    }
    else
    {
      if (OB_SUCCESS == err)
      {
        if (row_infos_->end() == target)
        {
          row_info.table_name_ = cell_out->table_name_;
          row_info.row_key_ = cell_out->row_key_;
          pair<map<ObRowInfo, vector<ObCellInfo> >::iterator, bool> insert_result;
          insert_result = row_infos_->insert(make_pair(row_info, vector<ObCellInfo>()));
          target = insert_result.first;
        }
        if (row_infos_->end() == target)
        {
          TBSYS_LOG(WARN,"unexpected error");
          err = OB_ERR_UNEXPECTED;
        }
        if (OB_SUCCESS == err
            && ObActionFlag::OP_ROW_DOES_NOT_EXIST != cell_out->value_.get_ext()
            && ObActionFlag::OP_DEL_ROW != cell_out->value_.get_ext())
        {
          target->second.push_back(*cell_out);
        }
      }
    }
  }
  return err;
}

int ObFinalResult::get(const ObGetParam &get_param, ObScanner &scanner,bool ret_null)
{
  int err = OB_SUCCESS;
  ObRowInfo row_info;
  ObCellInfo cur_cell;
  map<ObRowInfo, vector<ObCellInfo> >::iterator target;
  bool prev_row_not_exist = false;
  for (int64_t get_idx = 0 ; OB_SUCCESS == err && get_idx < get_param.get_cell_size(); get_idx ++)
  {
    row_info.table_name_ = get_param[get_idx]->table_name_;
    row_info.row_key_ = get_param[get_idx]->row_key_;
    cur_cell = *(get_param[get_idx]);
    target = row_infos_->lower_bound(row_info);
    if (get_idx > 0
        && prev_row_not_exist
        && get_param[get_idx]->table_name_ == get_param[get_idx-1]->table_name_
        && get_param[get_idx]->row_key_ == get_param[get_idx-1]->row_key_
       )
    {
      continue;
    }
    if (row_infos_->end() ==  target
        || row_info < target->first
        || target->second.size() == 0)
    {
      prev_row_not_exist = true;
      cur_cell.value_.set_ext(ObActionFlag::OP_ROW_DOES_NOT_EXIST);
    }
    else
    {
      prev_row_not_exist = false;
      vector<ObCellInfo>::iterator beg = target->second.begin();
      for (;beg != target->second.end(); beg ++)
      {
        if (beg->column_name_ == cur_cell.column_name_)
        {
          cur_cell = *beg;
          break;
        }
      }
      if (target->second.end() == beg)
      {
        if (ret_null)
        {
          cur_cell.value_.set_null();
        }
        else
        {
          cur_cell.value_.set_ext(ObActionFlag::OP_NOP);
        }
      }
    }
    cur_cell.table_id_ = OB_INVALID_ID;
    cur_cell.column_id_ = OB_INVALID_ID;
    err = scanner.add_cell(cur_cell);
  }
  return err;
}

int ObFinalResult::scan(const ObScanParam &scan_param, ObScanner &scanner)
{
  UNUSED(scanner);
  int err = OB_SUCCESS;
  char max_key_buf[512];
  memset(max_key_buf,0xff, sizeof(max_key_buf));
  ObString max_key;
  max_key.assign(max_key_buf,sizeof(max_key_buf));
  ObRowInfo start;
  ObRowInfo end;
  map<ObRowInfo, vector<ObCellInfo> >::iterator start_it;
  map<ObRowInfo, vector<ObCellInfo> >::iterator end_it;
  ObCellInfo cur_cell;
  start.table_name_ = scan_param.get_table_name();
  end.table_name_ = scan_param.get_table_name();
  if (!scan_param.get_range()->start_key_.is_min_row())
  {
    start.row_key_ = scan_param.get_range()->start_key_;
  }
  if (!scan_param.get_range()->end_key_.is_max_row())
  {
    end.row_key_ = scan_param.get_range()->end_key_;
  }
  else
  {
    end.row_key_ = max_key;
  }
  if (scan_param.get_range()->border_flag_.inclusive_start())
  {
    start_it = row_infos_->lower_bound(start);
  }
  else
  {
    start_it = row_infos_->upper_bound(start);
  }
  if (scan_param.get_range()->border_flag_.inclusive_end())
  {
    end_it = row_infos_->upper_bound(end);
  }
  else
  {
    end_it = row_infos_->lower_bound(end);
  }
  for (;start_it != end_it && OB_SUCCESS == err; start_it ++)
  {
    for (int64_t scan_idx = 0;
        scan_idx < scan_param.get_column_name_size() && OB_SUCCESS == err;
        scan_idx ++)
    {
      vector<ObCellInfo>::iterator cell_it = start_it->second.begin();
      cur_cell.table_name_ = scan_param.get_table_name();
      cur_cell.row_key_ = start_it->first.row_key_;
      cur_cell.column_name_ = scan_param.get_column_name()[scan_idx];
      cur_cell.value_.set_null();
      for (;cell_it != start_it->second.end(); cell_it ++)
      {
        if (cell_it->column_name_ == scan_param.get_column_name()[scan_idx])
        {
          break;
        }
      }
      if (cell_it != start_it->second.end())
      {
        cur_cell.value_ = cell_it->value_;
      }
      err = scanner.add_cell(cur_cell);
    }
  }
  return err;
}


bool check_result(const ObGetParam &get_param,  ObScanner &ob_scanner,
                  ObFinalResult & local_result)
{
  int err = OB_SUCCESS;
  bool res = true;
  ObScanner local_scanner;
  ObCellInfo *ob_cell = NULL;
  ObCellInfo *local_cell = NULL;
  err = local_result.get(get_param,local_scanner);
  if (OB_SUCCESS != err)
  {
    TBSYS_LOG(WARN,"fail to get local result");
  }
  for (int64_t get_idx = 0;
      get_idx < get_param.get_cell_size() && OB_SUCCESS == err;
      get_idx ++)
  {
    int ob_err = ob_scanner.next_cell();
    int local_err = local_scanner.next_cell();
    if (OB_ITER_END == ob_err && OB_ITER_END == local_err)
    {
      err = OB_SUCCESS;
      break;
    }
    if (OB_SUCCESS != ob_err)
    {
      err = ob_err;
      TBSYS_LOG(WARN,"ob next cell err");
      break;
    }
    if (OB_SUCCESS != local_err)
    {
      err = local_err;
      TBSYS_LOG(WARN,"local next cell err");
      break;
    }

    if (OB_SUCCESS == err)
    {
      err = ob_scanner.get_cell(&ob_cell);
    }
    if (OB_SUCCESS != err)
    {
      TBSYS_LOG(WARN,"fail to get cell from ob result [idx:%lld]", get_idx);
    }
    if (OB_SUCCESS == err)
    {
      if (OB_SUCCESS == err)
      {
        err = local_scanner.get_cell(&local_cell);
      }
      if (OB_SUCCESS != err)
      {
        TBSYS_LOG(WARN,"fail to get cell from local result [idx:%lld]", get_idx);
      }
    }
    if (OB_SUCCESS == err)
    {
      /*
      /// check ob result
      if (ob_cell->table_name_ != get_param[get_idx]->table_name_)
      {
        TBSYS_LOG(WARN,"ob result table name error [idx:%lld,ob.table_name_:%.*s,"
                  "param.table_name_:%.*s]", get_idx,ob_cell->table_name_.length(),
                  ob_cell->table_name_.ptr(), get_param[get_idx]->table_name_.length(),
                  get_param[get_idx]->table_name_.ptr());
        err = OB_INVALID_ARGUMENT;
      }
      if (ob_cell->row_key_ != get_param[get_idx]->row_key_)
      {
        TBSYS_LOG(WARN,"ob result rowkey error [idx:%lld,ob.row_key_:%.*s,"
                  "param.row_key_:%.*s]", get_idx,ob_cell->row_key_.length(),
                  ob_cell->row_key_.ptr(), get_param[get_idx]->row_key_.length(),
                  get_param[get_idx]->row_key_.ptr());
        err = OB_INVALID_ARGUMENT;
      }
      if (ob_cell->column_name_ != get_param[get_idx]->column_name_)
      {
        TBSYS_LOG(WARN,"ob result column name error [idx:%lld,ob.column_name_:%.*s,"
                  "param.column_name_:%.*s]", get_idx,ob_cell->column_name_.length(),
                  ob_cell->column_name_.ptr(), get_param[get_idx]->column_name_.length(),
                  get_param[get_idx]->column_name_.ptr());
        err = OB_INVALID_ARGUMENT;
      }

      /// check local result
      if (local_cell->table_name_ != get_param[get_idx]->table_name_)
      {
        TBSYS_LOG(WARN,"local result table name error [idx:%lld,ob.table_name_:%.*s,"
                  "param.table_name_:%.*s]", get_idx,local_cell->table_name_.length(),
                  local_cell->table_name_.ptr(), get_param[get_idx]->table_name_.length(),
                  get_param[get_idx]->table_name_.ptr());
        err = OB_INVALID_ARGUMENT;
      }
      if (local_cell->row_key_ != get_param[get_idx]->row_key_)
      {
        TBSYS_LOG(WARN,"local result rowkey error [idx:%lld,local.row_key_:%.*s,"
                  "param.row_key_:%.*s]", get_idx,local_cell->row_key_.length(),
                  local_cell->row_key_.ptr(), get_param[get_idx]->row_key_.length(),
                  get_param[get_idx]->row_key_.ptr());
        err = OB_INVALID_ARGUMENT;
      }
      if (local_cell->column_name_ != get_param[get_idx]->column_name_)
      {
        TBSYS_LOG(WARN,"local result column_name error [idx:%lld,local.column_name_:%.*s,"
                  "param.column_name_:%.*s]", get_idx,local_cell->column_name_.length(),
                  local_cell->column_name_.ptr(), get_param[get_idx]->column_name_.length(),
                  get_param[get_idx]->column_name_.ptr());
        err = OB_INVALID_ARGUMENT;
      }
      */
      TBSYS_LOG(DEBUG, "ob.table_name:%.*s,ob.rowkey:%.*s,ob.column_name:%.*s",
                ob_cell->table_name_.length(),ob_cell->table_name_.ptr(),
                ob_cell->row_key_.length(),ob_cell->row_key_.ptr(),
                ob_cell->column_name_.length(),ob_cell->column_name_.ptr()
               );
      TBSYS_LOG(DEBUG, "local.table_name:%.*s,local.rowkey:%.*s,local.column_name:%.*s",
                local_cell->table_name_.length(),local_cell->table_name_.ptr(),
                local_cell->row_key_.length(),local_cell->row_key_.ptr(),
                local_cell->column_name_.length(),local_cell->column_name_.ptr()
               );
      if (local_cell->table_name_ != ob_cell->table_name_)
      {
        TBSYS_LOG(WARN,"table name not coincident [idx:%lld,ob.table_name_:%.*s,"
                  "local.table_name_:%.*s]", get_idx,ob_cell->table_name_.length(),
                  ob_cell->table_name_.ptr(), local_cell->table_name_.length(),
                  local_cell->table_name_.ptr());
        err = OB_INVALID_ARGUMENT;
      }
      if (local_cell->row_key_ != ob_cell->row_key_)
      {
        TBSYS_LOG(WARN,"rowkey not coincident [idx:%lld,ob.row_key_:%.*s,"
                  "local.row_key_:%.*s]", get_idx,ob_cell->row_key_.length(),
                  ob_cell->row_key_.ptr(), local_cell->row_key_.length(),
                  local_cell->row_key_.ptr());
        err = OB_INVALID_ARGUMENT;
      }
      if (local_cell->column_name_ != ob_cell->column_name_)
      {
        TBSYS_LOG(WARN,"column name not coincident [idx:%lld,ob.column_name_:%.*s,"
                  "local.column_name_:%.*s]", get_idx,ob_cell->column_name_.length(),
                  ob_cell->column_name_.ptr(), local_cell->column_name_.length(),
                  local_cell->column_name_.ptr());
        err = OB_INVALID_ARGUMENT;
      }

      if (OB_SUCCESS == err)
      {
        if (ob_cell->value_.get_ext() != local_cell->value_.get_ext())
        {
          TBSYS_LOG(WARN,"ext info not coincident [idx:%lld,ob.ext:%lld,local.ext:%lld]",
                    get_idx, ob_cell->value_.get_ext(), local_cell->value_.get_ext());
          err = OB_INVALID_ARGUMENT;
        }
        if (OB_SUCCESS == err && ObExtendType != ob_cell->value_.get_type())
        {
          int64_t ob_value;
          int64_t local_value;
          if (ob_cell->value_.get_ext() != local_cell->value_.get_ext())
          {
            TBSYS_LOG(WARN,"ext not coincident [idx:%lld,ob.ext:%lld,"
                      "local.ext:%lld]",get_idx, ob_cell->value_.get_ext(),
                      local_cell->value_.get_ext());
            err = OB_INVALID_ARGUMENT;
          }
          if (ob_cell->value_.get_ext() == 0  && OB_SUCCESS == err)
          {
            if (ob_cell->value_.get_type() == ObNullType
                && ob_cell->value_.get_type() == local_cell->value_.get_type())
            {
              /// check pass
            }
            else
            {
              if (OB_SUCCESS == err)
              {
                err = local_cell->value_.get_int(local_value);
                if (OB_SUCCESS != err)
                {
                  TBSYS_LOG(WARN,"fail to get int from local, ext:%lld,type:%d",
                            local_cell->value_.get_ext(),local_cell->value_.get_type());
                }
                else
                {
                  TBSYS_LOG(DEBUG, "local value:%lld", local_value);
                }
              }
              if (OB_SUCCESS == err)
              {
                err = ob_cell->value_.get_int(ob_value);
                if (OB_SUCCESS != err)
                {
                  TBSYS_LOG(WARN,"fail to get int from ob, ext:%lld,type:%d",
                            ob_cell->value_.get_ext(),ob_cell->value_.get_type());
                }
                else
                {
                  TBSYS_LOG(DEBUG, "ob value:%lld", ob_value);
                }
              }

              if (OB_SUCCESS != err)
              {
                TBSYS_LOG(WARN,"fail to get int value [idx:%lld]", get_idx);
                err = OB_INVALID_ARGUMENT;
              }
              else
              {
                if (ob_value != local_value)
                {
                  TBSYS_LOG(WARN,"value not coincident [idx:%lld,ob.value:%lld,"
                            "local.value:%lld]",get_idx, ob_value, local_value);
                  err = OB_INVALID_ARGUMENT;
                }
              }
            }
          }
        }
      }
    }
  }

  if (OB_SUCCESS == err && ob_scanner.next_cell() != OB_ITER_END)
  {
    TBSYS_LOG(WARN,"ob return more result than expected");
  }

  if (OB_SUCCESS == err && local_scanner.next_cell() != OB_ITER_END)
  {
    TBSYS_LOG(WARN,"local return more result than expected");
  }

  if (OB_SUCCESS != err)
  {
    res = false;
    TBSYS_LOG(DEBUG, "check fail");
  }
  else
  {
    res = true;
    TBSYS_LOG(DEBUG, "check pass");
  }
  return res;
}

bool check_result(const ObScanParam &scan_param,  ObScanner &ob_scanner,
                  ObFinalResult & local_result)
{
  int err = OB_SUCCESS;
  bool res = true;
  ObScanner local_scanner;
  ObCellInfo *ob_cell = NULL;
  ObCellInfo *local_cell = NULL;
  err = local_result.scan(scan_param,local_scanner);
  if (OB_SUCCESS != err)
  {
    TBSYS_LOG(WARN,"fail to scan local result");
  }
  int64_t row_number = 0;
  bool ob_end = false;
  ObString rowkey;

  while (OB_SUCCESS == err)
  {
    for (int64_t scan_idx = 0; scan_idx < scan_param.get_column_name_size(); scan_idx ++)
    {
      if (OB_SUCCESS == err)
      {
        err = ob_scanner.next_cell();
      }
      if (OB_ITER_END == err && scan_idx == 0)
      {
        ob_end = true;
        err = OB_SUCCESS;
      }
      else if (OB_SUCCESS == err)
      {
        err = ob_scanner.get_cell(&ob_cell);
      }
      if (OB_SUCCESS != err)
      {
        TBSYS_LOG(WARN,"fail to get cell from ob result [row_idx:%lld,idx:%lld]",
                  row_number, scan_idx);
      }
      if (OB_SUCCESS == err)
      {
        err = local_scanner.next_cell();
        if (ob_end && OB_ITER_END != err)
        {
          TBSYS_LOG(WARN,"local result return more cells than ob result");
          err = OB_INVALID_ARGUMENT;
        }
        if ( !ob_end && OB_ITER_END == err)
        {
          TBSYS_LOG(WARN,"ob result return more cells than local result");
          err = OB_INVALID_ARGUMENT;
        }
        if (ob_end && OB_ITER_END == err)
        {
          err = OB_ITER_END;
          break;
        }
        if (OB_SUCCESS == err)
        {
          err = local_scanner.get_cell(&local_cell);
        }
        if (OB_SUCCESS != err)
        {
          TBSYS_LOG(WARN,"fail to get cell from local result [row_idx:%lld,idx:%lld]",
                    row_number, scan_idx);
        }
      }
      if (OB_SUCCESS == err)
      {
        if (0 == scan_idx)
        {
          rowkey = ob_cell->row_key_;
        }
        /// check ob result
        if (ob_cell->table_name_ != scan_param.get_table_name())
        {
          TBSYS_LOG(WARN,"ob result table name error [row_idx:%lld,idx:%lld,ob.table_name_:%.*s,"
                    "param.table_name_:%.*s]", row_number, scan_idx,ob_cell->table_name_.length(),
                    ob_cell->table_name_.ptr(), scan_param.get_table_name().length(),
                    scan_param.get_table_name().ptr());
          err = OB_INVALID_ARGUMENT;
        }

        if (ob_cell->column_name_ != scan_param.get_column_name()[scan_idx])
        {
          TBSYS_LOG(WARN,"ob result rowkey error [row_idx:%lld,idx:%lld,ob.column_name_:%.*s,"
                    "param.column_name_:%.*s]", row_number, scan_idx,ob_cell->column_name_.length(),
                    ob_cell->column_name_.ptr(), scan_param.get_column_name()[scan_idx].length(),
                    scan_param.get_column_name()[scan_idx].ptr());
          err = OB_INVALID_ARGUMENT;
        }
        /// check local result
        if (local_cell->table_name_ != scan_param.get_table_name())
        {
          TBSYS_LOG(WARN,"ob result table name error [row_idx:%lld,idx:%lld,ob.table_name_:%.*s,"
                    "param.table_name_:%.*s]", row_number, scan_idx,local_cell->table_name_.length(),
                    local_cell->table_name_.ptr(), scan_param.get_table_name().length(),
                    scan_param.get_table_name().ptr());
          err = OB_INVALID_ARGUMENT;
        }

        if (local_cell->column_name_ != scan_param.get_column_name()[scan_idx])
        {
          TBSYS_LOG(WARN,"ob result rowkey error [row_idx:%lld,idx:%lld,ob.column_name_:%.*s,"
                    "param.column_name_:%.*s]", row_number,scan_idx,local_cell->column_name_.length(),
                    local_cell->column_name_.ptr(), scan_param.get_column_name()[scan_idx].length(),
                    scan_param.get_column_name()[scan_idx].ptr());
          err = OB_INVALID_ARGUMENT;
        }


        if (local_cell->row_key_ != ob_cell->row_key_)
        {
          TBSYS_LOG(WARN,"rowkey error [row_idx:%lld,idx:%lld,ob.row_key_:%.*s,"
                    "local.row_key_:%.*s]", row_number, scan_idx,ob_cell->row_key_.length(),
                    ob_cell->row_key_.ptr(), local_cell->row_key_.length(),
                    local_cell->row_key_.ptr());
          err = OB_INVALID_ARGUMENT;
        }
        if (local_cell->row_key_ != rowkey)
        {
          TBSYS_LOG(WARN,"rowkey error [row_idx:%lld,idx:%lld,ob.row_key_:%.*s,"
                    "row_first_cell.row_key_:%.*s]", row_number,scan_idx,local_cell->row_key_.length(),
                    local_cell->row_key_.ptr(), rowkey.length(),  rowkey.ptr());
          err = OB_INVALID_ARGUMENT;
        }

        if (OB_SUCCESS == err)
        {
          if (ob_cell->value_.get_ext() != local_cell->value_.get_ext())
          {
            TBSYS_LOG(WARN,"ext info not coincident [row_idx:%lld,idx:%lld,"
                      "ob.ext:%lld,local.ext:%lld]",
                      row_number, scan_idx, ob_cell->value_.get_ext(), local_cell->value_.get_ext());
            err = OB_INVALID_ARGUMENT;
          }
          if (ob_cell->value_.get_type() == ObNullType
              && ob_cell->value_.get_type() == local_cell->value_.get_type())
          {
            /// check pass
          }
          else
          {
            int64_t ob_value = 0;
            int64_t local_value = 0;
            if (OB_SUCCESS == err)
            {
              err = ob_cell->value_.get_int(ob_value);
            }
            if (OB_SUCCESS == err)
            {
              err = local_cell->value_.get_int(local_value);
            }
            if (OB_SUCCESS != err)
            {
              TBSYS_LOG(WARN,"fail to get int value [row_idx:%lld,idx:%lld]",row_number, scan_idx);
              err = OB_INVALID_ARGUMENT;
            }
            else
            {
              if (ob_value != local_value)
              {
                TBSYS_LOG(WARN,"value not coincident [row_idx:%lld,idx:%lld,ob.value:%lld,"
                          "local.value:%lld]",row_number, scan_idx, ob_value, local_value);
                err = OB_INVALID_ARGUMENT;
              }
            }
          }
        }
      }
    }
    if (OB_ITER_END == err)
    {
      err  = OB_SUCCESS;
      break;
    }

    row_number ++;
  }

  if (OB_SUCCESS != err)
  {
    res = false;
    TBSYS_LOG(DEBUG, "check fail");
  }
  else
  {
    res = true;
    TBSYS_LOG(DEBUG, "check pass");
  }
  return res;
}
