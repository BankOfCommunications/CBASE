/**
 * (C) 2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * oceanbase.h : API of Oceanbase
 *
 * Authors:
 *   yanran <yanran.hfs@taobao.com>
 *
 */

#include "oceanbase.h"
#include "libobapi.h"
#include "ob_client.h"

using namespace oceanbase::client;

OB_ERR_PMSG OB_ERR_DEFAULT_MSG[] = {
  "Success",
  "Internal error",
  "Unmatched schema",
  "Not enough memory",
  "Invalid parameter",
  "Timeout response",
  "Not supported operation",
  "Not existed user",
  "Wrong password",
  "Permission denied",
  "Operation timeout",
};

void ob_set_err_with_default(OB_ERR_CODE code)
{
  if (code < sizeof(OB_ERR_DEFAULT_MSG))
  {
    ob_set_err(code, OB_ERR_DEFAULT_MSG[code]);
  }
  else
  {
    ob_set_err(code, "");
  }
}

void set_obj_with_add(ObObj &obj, const OB_VALUE* v, int is_add)
{
  if (NULL != v)
  {
    switch (v->type)
    {
      case OB_INT_TYPE:
        set_obj_int(obj, v->v.v_int, is_add);
        break;
      case OB_VARCHAR_TYPE:
        set_obj_varchar(obj, v->v.v_varchar);
        break;
      case OB_DATETIME_TYPE:
        set_obj_datetime(obj, v->v.v_datetime);
        break;
      //add peiouya [DATE_TIME] 20150906:b
      case OB_DATE_TYPE:
        set_obj_date(obj, v->v.v_datetime);
        break;
      case OB_TIME_TYPE:
        set_obj_time(obj, v->v.v_datetime);
        break;
      //add 20150906:e
      //add lijianqiang [INT_32] 20150930:b
      case OB_INT32_TYPE:
        set_obj_int32(obj, v->v.v_int32, is_add);
        break;
      //add 20150930:e
      default:
        obj.set_null();
    };
  }
}

void set_obj(ObObj &obj, const OB_VALUE* v)
{
  int is_add_false = 0;
  set_obj_with_add(obj, v, is_add_false);
}

void set_obj_int(ObObj &obj, OB_INT v, int is_add)
{
  obj.set_int(v, is_add != 0);
}

void set_obj_varchar(ObObj &obj, OB_VARCHAR v)
{
  obj.set_varchar(ObString(static_cast<int32_t>(v.len), static_cast<int32_t>(v.len), v.p));
}

void set_obj_datetime(ObObj &obj, OB_DATETIME v)
{
  ObPreciseDateTime t = v.tv_sec * 1000000 + v.tv_usec;
  obj.set_precise_datetime(t);
}

//add peiouya [DATE_TIME] 20150906:b
void set_obj_date(ObObj &obj, OB_DATETIME v)
{
  ObDate t = v.tv_sec * 1000000 + v.tv_usec;
  obj.set_date(t);
}

void set_obj_time(ObObj &obj, OB_DATETIME v)
{
  ObTime t = v.tv_sec * 1000000 + v.tv_usec;
  obj.time(t);
}
//add 20150906:e

//add lijianqiang[INT_32] 20150930:b
void set_obj_int32(ObObj &obj,OB_INT32 v, int is_add)
{
  obj.set_int32(v, is_add != 0) ;
}
//add 20150930:e

ObLogicOperator get_op_logic_operator(OB_LOGIC_OPERATOR o)
{
  switch (o)
  {
    case OB_LT: return LT;
    case OB_LE: return LE;
    case OB_EQ: return EQ;
    case OB_GT: return GT;
    case OB_GE: return GE;
    case OB_NE: return NE;
    case OB_LIKE: return LIKE;
    default: return NIL;
  }
}

ObScanParam::Order get_ob_order(OB_ORDER o)
{
  switch (o)
  {
    case OB_ASC: return ObScanParam::ASC;
    case OB_DESC: return ObScanParam::DESC;
    default: return ObScanParam::ASC;
  }
}

ObAggregateFuncType get_ob_aggregation_type(OB_AGGREGATION_TYPE o)
{
  switch (o)
  {
    case OB_SUM: return SUM;
    case OB_COUNT: return COUNT;
    case OB_MAX: return MAX;
    case OB_MIN: return MIN;
    case OB_LISTAGG: return LISTAGG;//add gaojt [ListAgg][JHOBv0.1]20150104
    default: return AGG_FUNC_MIN;
  }
}

const char* ob_type_to_str(OB_TYPE type)
{
  const char* str = OB_UNKNOWN_TYPE_STR;
  switch (type)
  {
    case OB_INT_TYPE:
      str = "INT";
      break;
    //add lijianqiang [INT_32] 20150930:b
    case OB_INT32_TYPE:
      str = "INT32";
      break;
    //add 20150930:e
    case OB_VARCHAR_TYPE:
      str = "VARCHAR";
      break;
    case OB_DATETIME_TYPE:
      str = "DATETIME";
      break;
    case OB_DOUBLE_TYPE:
      str = "DOUBLE";
      break;
    //add peiouya [DATE_TIME] 20150906:b
    case OB_DATE_TYPE:
      str = "DATE";
      break;
    case OB_TIME_TYPE:
      str = "TIME";
      break;
    //add 20150906:e
    default:
      break;
  }
  return str;
}

const char* ob_operation_code_to_str(OB_OPERATION_CODE oper_code)
{
  const char* str = OB_UNKNOWN_OPER_STR;
  switch (oper_code)
  {
    case OB_OP_GET:
      str = "GET";
      break;
    case OB_OP_SCAN:
      str = "SCAN";
      break;
    case OB_OP_SET:
      str = "SET";
      break;
  }
  return str;
}

const char* ob_order_to_str(OB_ORDER order)
{
  const char* str = OB_UNKNOWN_TYPE_STR;
  switch (order)
  {
    case OB_ASC:
      str = "OB_ASC";
      break;
    case OB_DESC:
      str = "OB_DESC";
      break;
  }
  return str;
}

const char* ob_aggregation_type_to_str(OB_AGGREGATION_TYPE aggr_type)
{
  const char* str = OB_UNKNOWN_OPER_STR;
  switch (aggr_type)
  {
    case OB_SUM:
      str = "OB_SUM";
      break;
    case OB_COUNT:
      str = "OB_COUNT";
      break;
    case OB_MAX:
      str = "OB_MAX";
      break;
    case OB_MIN:
      str = "OB_MIN";
      break;
  }
  return str;
}

const char* ob_logic_operation_to_str(OB_LOGIC_OPERATOR logic_oper)
{
  const char* str = OB_UNKNOWN_OPER_STR;
  switch (logic_oper)
  {
    case OB_LT:
      str = "OB_LT";
      break;
    case OB_LE:
      str = "OB_LE";
      break;
    case OB_EQ:
      str = "OB_EQ";
      break;
    case OB_GT:
      str = "OB_GT";
      break;
    case OB_GE:
      str = "OB_GE";
      break;
    case OB_NE:
      str = "OB_NE";
      break;
    case OB_LIKE:
      str = "OB_LIKE";
      break;
  }
  return str;
}

char* get_display_row_key(const char* row_key, int64_t row_key_len)
{
  const int aux_len = 2;
  const int ending_len = 1;
  static char dis_row_key[MAX_DISPLAY_ROW_KEY_LEN + aux_len + ending_len];
  int64_t dis_row_key_len = row_key_len <= MAX_DISPLAY_ROW_KEY_LEN / 2 ?
                            row_key_len : MAX_DISPLAY_ROW_KEY_LEN / 2;
  int64_t dis_len = dis_row_key_len * 2;
  dis_row_key[0] = '0';
  dis_row_key[1] = 'x';
  for (int64_t i = 0; i < dis_row_key_len; i++)
  {
    snprintf(dis_row_key + i * 2 + aux_len, 3, "%02x", (unsigned char)row_key[i]);
  }
  dis_row_key[dis_len + aux_len] = '\0';
  return dis_row_key;
}


////////// ERROR CODE & MSG //////////

OB_PERR ob_get_err()
{
  static __thread OB_ERR s_ob_err;
  return &s_ob_err;
}

OB_ERR_CODE ob_errno()
{
  return ob_get_err()->c;
}

OB_ERR_PMSG ob_error()
{
  return ob_get_err()->m;
}

void ob_set_errno(OB_ERR_CODE code)
{
  ob_get_err()->c = code;
}

void ob_set_error(OB_ERR_PMSG msg)
{
  int msg_len = static_cast<int32_t>(strlen(msg));
  if (msg_len >= OB_ERR_MSG_MAX_LEN) msg_len = OB_ERR_MSG_MAX_LEN - 1;
  strncpy(ob_get_err()->m, msg, msg_len);
  ob_get_err()->m[msg_len + 1] = '\0';
}

void ob_set_err(OB_ERR_CODE code, OB_ERR_PMSG msg)
{
  ob_set_errno(code);
  ob_set_error(msg);
}

////////// ERROR CODE & MSG END //////////

////////// OB_VALUE Process //////////
void ob_set_value_int(OB_VALUE* value, OB_INT v)
{
  if (NULL != value)
  {
    value->v.v_int = v;
    value->type = OB_INT_TYPE;
  }
}

void ob_set_value_varchar(OB_VALUE* value, OB_VARCHAR v)
{
  if (NULL != value)
  {
    value->v.v_varchar = v;
    value->type = OB_VARCHAR_TYPE;
  }
}

void ob_set_value_datetime(OB_VALUE* value, OB_DATETIME v)
{
  if (NULL != value)
  {
    value->v.v_datetime = v;
    value->type = OB_DATETIME_TYPE;
  }
}

//add peiouya [DATE_TIME] 20150906:b
void ob_set_value_date(OB_VALUE* value, OB_DATETIME v)
{
  if (NULL != value)
  {
    value->v.v_datetime = v;
    value->type = OB_DATE_TYPE;
  }
}

void ob_set_value_time(OB_VALUE* value, OB_DATETIME v)
{
  if (NULL != value)
  {
    value->v.v_datetime = v;
    value->type = OB_TIME_TYPE;
  }
}
//add 20150906:e

//add lijianqiang [INT_32] 20150930:b
void ob_set_value_int32(OB_VALUE* value, OB_INT32 v)
{
  if (NULL != value)
  {
    value->v.v_int32 = v;
    value->type = OB_INT32_TYPE;
  }
}
//add 20150930:e

void ob_set_value_double(OB_VALUE* value, OB_DOUBLE v)
{
  if (NULL != value)
  {
    value->v.v_double = v;
    value->type = OB_DOUBLE_TYPE;
  }
}

////////// OB_VALUE Process END //////////

////////// OB_RES //////////
OB_CELL* ob_fetch_cell(OB_RES* ob_res)
{
  OB_CELL* ret = NULL;
  int err = OB_SUCCESS;
  OB_ERR_CODE err_code = OB_ERR_SUCCESS;

  ObCellInfo *cell_info = NULL;
  bool is_row_changed = false;
  OB_INT v_int = 0;
  ObString stmp;
  int64_t itmp = 0;
  double dtmp = 0.0;
  OB_VARCHAR v_varchar;
  OB_DATETIME v_datetime;
  //add lijianqiang [INT_32] 20150930:b
  OB_INT32 v_int32 = 0;
  //add 20150930:e

  ENSURE_COND(NULL != ob_res);
  if (OB_ERR_SUCCESS == err_code)
  {
    while (ob_res->cur_scanner != ob_res->scanner.end()
        && OB_ITER_END == (err = ob_res->cur_scanner->next_cell()))
    {
      ob_res->cur_scanner++;
    }
    if (ob_res->cur_scanner != ob_res->scanner.end())
    {
      if (OB_SUCCESS == err)
      {
        if (OB_SUCCESS ==
            (err = ob_res->cur_scanner->get_cell(&cell_info, &is_row_changed)))
        {
          ob_set_value_int(&(ob_res->cur_cell.v), 0);
          ob_res->cur_cell.table = cell_info->table_name_.ptr();
          ob_res->cur_cell.row_key = cell_info->row_key_.ptr();
          ob_res->cur_cell.column = cell_info->column_name_.ptr();
          ob_res->cur_cell.table_len = cell_info->table_name_.length();
          ob_res->cur_cell.row_key_len = cell_info->row_key_.length();
          ob_res->cur_cell.column_len = cell_info->column_name_.length();
          ob_res->cur_cell.is_row_changed = is_row_changed;
          ob_res->cur_cell.is_null = false;
          ob_res->cur_cell.is_row_not_exist = false;

          if (ObExtendType == cell_info->value_.get_type())
          {
            switch (cell_info->value_.get_ext())
            {
              case ObActionFlag::OP_ROW_DOES_NOT_EXIST:
                ob_res->cur_cell.is_row_not_exist = true;
                break;
              default:
                err_code = OB_ERR_ERROR;
                ob_set_err_with_default(err_code);
                TBSYS_LOG(ERROR, "ObScanner get an unknow extend type cell, "
                        "extend value=%ld", cell_info->value_.get_ext());
                break;
            }
          }
          else
          {
            switch (cell_info->value_.get_type())
            {
              case ObNullType:
                ob_res->cur_cell.is_null = true;
                break;
              case ObIntType:
                cell_info->value_.get_int(v_int);
                ob_set_value_int(&(ob_res->cur_cell.v), v_int);
                break;
              //add lijianqiang [INT_32] 20150930:b
              case ObInt32Type:
                cell_info->value_.get_int32(v_int32);
                ob_set_value_int32(&(ob_res->cur_cell.v), v_int32);
                break;
              //add 20150930:e
              case ObVarcharType:
                cell_info->value_.get_varchar(stmp);
                v_varchar.p = stmp.ptr();
                v_varchar.len = stmp.length();
                ob_set_value_varchar(&ob_res->cur_cell.v, v_varchar);
                break;
              case ObDateTimeType:
                cell_info->value_.get_datetime(itmp);
                v_datetime.tv_sec = itmp / 1000;
                v_datetime.tv_usec = itmp % 1000;
                ob_set_value_datetime(&ob_res->cur_cell.v, v_datetime);
                break;
              //add peiouya [DATE_TIME] 20150831:e
              case ObDateType:
                cell_info->value_.get_date(itmp);
                v_datetime.tv_sec = itmp / 1000000;
                v_datetime.tv_usec = itmp % 1000000;
                ob_set_value_date(&ob_res->cur_cell.v, v_datetime);
                break;
              case ObTimeType:
                cell_info->value_.get_time(itmp);
                v_datetime.tv_sec = itmp / 1000000;
                v_datetime.tv_usec = itmp % 1000000;
                ob_set_value_time(&ob_res->cur_cell.v, v_datetime);
                break;
              //add 20150831:e
              case ObPreciseDateTimeType:
                cell_info->value_.get_precise_datetime(itmp);
                v_datetime.tv_sec = itmp / 1000000;
                v_datetime.tv_usec = itmp % 1000000;
                ob_set_value_datetime(&ob_res->cur_cell.v, v_datetime);
                break;
              case ObCreateTimeType:
                cell_info->value_.get_createtime(itmp);
                v_datetime.tv_sec = itmp / 1000000;
                v_datetime.tv_usec = itmp % 1000000;
                ob_set_value_datetime(&ob_res->cur_cell.v, v_datetime);
                break;
              case ObModifyTimeType:
                cell_info->value_.get_modifytime(itmp);
                v_datetime.tv_sec = itmp / 1000000;
                v_datetime.tv_usec = itmp % 1000000;
                ob_set_value_datetime(&ob_res->cur_cell.v, v_datetime);
                break;
              case ObDoubleType:
                cell_info->value_.get_double(dtmp);
                ob_set_value_double(&ob_res->cur_cell.v, dtmp);
                break;
              default:
                err_code = OB_ERR_ERROR;
                ob_set_err_with_default(err_code);
                TBSYS_LOG(ERROR, "ObScanner get an unknow type cell, "
                        "value_type=%d", cell_info->value_.get_type());
                break;
            }
          }

          if (OB_ERR_SUCCESS == err_code)
          {
            ret = &ob_res->cur_cell;
          }
        }
        else
        {
          ret = NULL;
          ob_set_err_with_default(OB_ERR_ERROR);
          TBSYS_LOG(ERROR, "ObScanner get_cell returned error, err=%d", err);
        }
      }
      else if (OB_ITER_END == err)
      {
        ret = NULL;
      }
      else
      {
        ret = NULL;
        ob_set_err_with_default(OB_ERR_ERROR);
        TBSYS_LOG(ERROR, "ObScanner next_cell returned error, err=%d", err);
      }
    }
    else
    {
      ret = NULL;
    }
  }
  else
  {
    ret = NULL;
  }

  return ret;
}

OB_ROW* ob_fetch_row(OB_RES* ob_res)
{
  OB_ROW* ret = NULL;
  int err = OB_SUCCESS;
  OB_ERR_CODE err_code = OB_ERR_SUCCESS;

  ObCellInfo *cell_info;
  int64_t num = 0;
  OB_INT v_int = 0;
  ObString stmp;
  int64_t itmp = 0;
  double dtmp = 0.0;
  OB_VARCHAR v_varchar;
  OB_DATETIME v_datetime;
  //add lijianqiang [INT_32] 20150930:b
  OB_INT32 v_int32 = 0;
  //add 20150930:e

  ENSURE_COND(NULL != ob_res);
  if (OB_ERR_SUCCESS == err_code)
  {
    while (ob_res->cur_scanner != ob_res->scanner.end()
        && OB_ITER_END == (err = ob_res->cur_scanner->next_row()))
    {
      TBSYS_LOG(DEBUG, "reach end of a scanner");
      ob_res->cur_scanner++;
    }
    if (ob_res->cur_scanner != ob_res->scanner.end())
    {
      if (OB_SUCCESS == err)
      {
        if (OB_SUCCESS == (err = ob_res->cur_scanner->get_row(&cell_info, &num)))
        {
          for (int64_t i = 0; i < num && OB_ERR_SUCCESS == err_code; i++)
          {
            ob_set_value_int(&(ob_res->cell_array[i].v), 0);
            ob_res->cell_array[i].table = cell_info[i].table_name_.ptr();
            ob_res->cell_array[i].row_key = cell_info[i].row_key_.ptr();
            ob_res->cell_array[i].column = cell_info[i].column_name_.ptr();
            ob_res->cell_array[i].table_len = cell_info[i].table_name_.length();
            ob_res->cell_array[i].row_key_len = cell_info[i].row_key_.length();
            ob_res->cell_array[i].column_len = cell_info[i].column_name_.length();
            ob_res->cell_array[i].is_row_changed = false;
            ob_res->cell_array[i].is_null = false;
            ob_res->cell_array[i].is_row_not_exist = false;

            if (ObExtendType == cell_info[i].value_.get_type())
            {
              switch (cell_info[i].value_.get_ext())
              {
                case ObActionFlag::OP_ROW_DOES_NOT_EXIST:
                  ob_res->cell_array[i].is_row_not_exist = true;
                  break;
                default:
                  err_code = OB_ERR_ERROR;
                  ob_set_err_with_default(err_code);
                  TBSYS_LOG(ERROR, "ObScanner get an unknow extend type cell, "
                          "extend value=%ld", cell_info->value_.get_ext());
              }
            }
            else
            {
              switch (cell_info[i].value_.get_type())
              {
                case ObNullType:
                  ob_res->cell_array[i].is_null = true;
                  break;
                case ObIntType:
                  cell_info[i].value_.get_int(v_int);
                  ob_set_value_int(&(ob_res->cell_array[i].v), v_int);
                  break;
                //add lijianqiang[INT_32] 20150930:b
                case ObInt32Type:
                  cell_info[i].value_.get_int32(v_int32);
                  ob_set_value_int(&(ob_res->cell_array[i].v), v_int32);
                  break;
                //add 20150930:e
                case ObVarcharType:
                  cell_info[i].value_.get_varchar(stmp);
                  v_varchar.p = stmp.ptr();
                  v_varchar.len = stmp.length();
                  ob_set_value_varchar(&ob_res->cell_array[i].v, v_varchar);
                  break;
                case ObDateTimeType:
                  cell_info[i].value_.get_datetime(itmp);
                  v_datetime.tv_sec = itmp / 1000;
                  v_datetime.tv_usec = itmp % 1000;
                  ob_set_value_datetime(&ob_res->cell_array[i].v, v_datetime);
                  break;
                //add peiouya [DATE_TIME] 20150831:b
                case ObDateType:
                  cell_info[i].value_.get_date(itmp);
                  v_datetime.tv_sec = itmp / 1000000;
                  v_datetime.tv_usec = itmp % 1000000;
                  ob_set_value_date(&ob_res->cell_array[i].v, v_datetime);
                  break;
                case ObTimeType:
                  cell_info[i].value_.get_time(itmp);
                  v_datetime.tv_sec = itmp / 1000000;
                  v_datetime.tv_usec = itmp % 1000000;
                  ob_set_value_time(&ob_res->cell_array[i].v, v_datetime);
                  break;
                //add 20150831:e
                case ObPreciseDateTimeType:
                  cell_info[i].value_.get_precise_datetime(itmp);
                  v_datetime.tv_sec = itmp / 1000000;
                  v_datetime.tv_usec = itmp % 1000000;
                  ob_set_value_datetime(&ob_res->cell_array[i].v, v_datetime);
                  break;
                case ObCreateTimeType:
                  cell_info[i].value_.get_createtime(itmp);
                  v_datetime.tv_sec = itmp / 1000000;
                  v_datetime.tv_usec = itmp % 1000000;
                  ob_set_value_datetime(&ob_res->cell_array[i].v, v_datetime);
                  break;
                case ObModifyTimeType:
                  cell_info[i].value_.get_modifytime(itmp);
                  v_datetime.tv_sec = itmp / 1000000;
                  v_datetime.tv_usec = itmp % 1000000;
                  ob_set_value_datetime(&ob_res->cell_array[i].v, v_datetime);
                  break;
                case ObDoubleType:
                  cell_info[i].value_.get_double(dtmp);
                  ob_set_value_double(&ob_res->cell_array[i].v, dtmp);
                  break;
                default:
                  err_code = OB_ERR_ERROR;
                  ob_set_err_with_default(err_code);
                  TBSYS_LOG(ERROR, "ObScanner get an unknow type cell, "
                          "value_type=%d", cell_info[i].value_.get_type());
                  break;
              }
            }
          }
          if (OB_ERR_SUCCESS == err_code)
          {
            ob_res->cur_row.cell_num = num;
            ob_res->cur_row.cell = ob_res->cell_array;
            ret = &ob_res->cur_row;
          }
        }
        else
        {
          ret = NULL;
          ob_set_err_with_default(OB_ERR_ERROR);
          TBSYS_LOG(ERROR, "ObScanner get_row returned error, err=%d", err);
        }
      }
      else if (OB_ITER_END == err)
      {
        ret = NULL;
      }
      else
      {
        ret = NULL;
        ob_set_err_with_default(OB_ERR_ERROR);
        TBSYS_LOG(ERROR, "ObScanner next_row returned error, err=%d", err);
      }
    }
  }
  else
  {
    ret = NULL;
  }

  return ret;
}

OB_ERR_CODE ob_res_seek_to_begin_cell(OB_RES* ob_res)
{
  OB_ERR_CODE err_code = OB_ERR_SUCCESS;

  ENSURE_COND(NULL != ob_res);
  if (OB_ERR_SUCCESS == err_code)
  {
    for (OB_RES::ScannerListIter i = ob_res->scanner.begin();
        OB_ERR_SUCCESS == err_code && i != ob_res->scanner.end(); i++)
    {
      int err = i->reset_iter();
      if (OB_SUCCESS != err)
      {
        err_code = OB_ERR_ERROR;
        ob_set_err_with_default(err_code);
        TBSYS_LOG(ERROR, "ObScanner reset_iter returned error, err=%d", err);
      }
    }
    ob_res->cur_scanner = ob_res->scanner.begin();
  }

  return err_code;
}

OB_ERR_CODE ob_res_seek_to_begin_row(OB_RES* ob_res)
{
  OB_ERR_CODE err_code = OB_ERR_SUCCESS;

  ENSURE_COND(NULL != ob_res);
  if (OB_ERR_SUCCESS == err_code)
  {
    for (OB_RES::ScannerListIter i = ob_res->scanner.begin();
        OB_ERR_SUCCESS == err_code && i != ob_res->scanner.end(); i++)
    {
      int err = i->reset_row_iter();
      if (OB_SUCCESS != err)
      {
        err_code = OB_ERR_ERROR;
        ob_set_err_with_default(err_code);
        TBSYS_LOG(ERROR, "ObScanner reset_iter returned error, err=%d", err);
      }
    }
    ob_res->cur_scanner = ob_res->scanner.begin();
  }

  return err_code;
}

int64_t ob_fetch_row_num(OB_RES* ob_res)
{
  OB_ERR_CODE err_code = OB_ERR_SUCCESS;
  int64_t num = 0;
  ENSURE_COND(NULL != ob_res);
  if (OB_ERR_SUCCESS == err_code)
  {
    if (ob_res->scanner.size() > 0)
    {
      for (OB_RES::ScannerListIter i = ob_res->scanner.begin();
          i != ob_res->scanner.end(); i++)
      {
        num += i->get_row_num();
      }
    }
  }
  return num;
}

int64_t ob_fetch_whole_res_row_num(OB_RES* ob_res)
{
  OB_ERR_CODE err_code = OB_ERR_SUCCESS;
  int64_t num = 0;
  ENSURE_COND(NULL != ob_res);
  if (OB_ERR_SUCCESS == err_code)
  {
    num = ob_res->scanner.front().get_whole_result_row_num();
  }
  return num;
}

int ob_is_fetch_all_res(OB_RES* ob_res)
{
  OB_ERR_CODE err_code = OB_ERR_SUCCESS;
  bool all_res = false;
  int64_t num = 0;
  ENSURE_COND(NULL != ob_res);
  if (OB_ERR_SUCCESS == err_code)
  {
    if (ob_res->scanner.size() > 0)
    {
      ob_res->scanner.back().get_is_req_fullfilled(all_res, num);
    }
  }
  return all_res ? 1 : 0;
}

////////// OB_RES END //////////

////////// OB_GET //////////
OB_ERR_CODE ob_get_cell(
    OB_GET* ob_get,
    const char* table,
    const char* row_key, int64_t row_key_len,
    const char* column)
{
  OB_ERR_CODE err_code = OB_ERR_SUCCESS;
  ENSURE_COND(NULL != ob_get && NULL != table
              && NULL != column && NULL != row_key && row_key_len > 0);
  if (OB_ERR_SUCCESS == err_code)
  {
    ObVersionRange vrange;
    vrange.border_flag_.set_inclusive_start();
    vrange.border_flag_.set_max_value();
    vrange.start_version_ = 1;
    ob_get->get_param.set_version_range(vrange);

    int table_len = static_cast<int32_t>(strlen(table));
    int column_len = static_cast<int32_t>(strlen(column));
    ObCellInfo cell;
    cell.table_name_ = ObString(table_len, table_len, (char*)table);
    cell.table_id_ = OB_INVALID_ID;
    cell.row_key_ = ObString(static_cast<int32_t>(row_key_len), static_cast<int32_t>(row_key_len), (char*)row_key);
    cell.column_name_ = ObString(column_len, column_len, (char*)column);
    cell.column_id_ = OB_INVALID_ID;
    int err = ob_get->get_param.add_cell(cell);
    if (OB_SUCCESS != err)
    {
      TBSYS_LOG(ERROR, "ObGetParam add_cell returned error, "
          "err=%d table=%s row_key=%s column=%s",
          err, table, get_display_row_key(row_key, row_key_len), column);
      err_code = err_code_map(err);
      ob_set_err_with_default(err_code);
    }
  }
  return err_code;
}

////////// OB_GET //////////

////////// OB_SET //////////
OB_ERR_CODE ob_real_update(
    OB_SET* ob_set,
    const char* table,
    const char* row_key, int64_t row_key_len,
    const char* column,
    const OB_VALUE* v,
    int is_add)
{
  OB_ERR_CODE err_code = OB_ERR_SUCCESS;
  ENSURE_COND(NULL != ob_set && NULL != table && NULL != column
      && NULL != row_key && row_key_len > 0 && NULL != v);
  if (OB_ERR_SUCCESS == err_code)
  {
    int table_len = static_cast<int32_t>(strlen(table));
    int column_len = static_cast<int32_t>(strlen(column));
    ObObj obj;
    set_obj_with_add(obj, v, is_add);
    int err = ob_set->mutator.update(
        ObString(table_len, table_len, (char*)table),
        ObString(static_cast<int32_t>(row_key_len), static_cast<int32_t>(row_key_len), (char*)row_key),
        ObString(column_len, column_len, (char*)column),
        obj);
    if (OB_SUCCESS != err)
    {
      TBSYS_LOG(ERROR, "ObMutator update returned error, "
          "err=%d table=%s row_key=%s column=%s",
          err, table, get_display_row_key(row_key, row_key_len), column);
      err_code = err_code_map(err);
      ob_set_err_with_default(err_code);
    }
  }
  return err_code;
}

OB_ERR_CODE ob_update(
    OB_SET* ob_set,
    const char* table,
    const char* row_key, int64_t row_key_len,
    const char* column,
    const OB_VALUE* v)
{
  int is_add_false = 0;
  return ob_real_update(ob_set, table, row_key, row_key_len,
      column, v, is_add_false);
}

OB_ERR_CODE ob_update_int(
    OB_SET* ob_set,
    const char* table,
    const char* row_key, int64_t row_key_len,
    const char* column,
    OB_INT v)
{
  OB_VALUE value;
  ob_set_value_int(&value, v);
  return ob_update(ob_set, table, row_key, row_key_len, column, &value);
}

OB_ERR_CODE ob_update_varchar(
    OB_SET* ob_set,
    const char* table,
    const char* row_key, int64_t row_key_len,
    const char* column,
    OB_VARCHAR v)
{
  OB_VALUE value;
  ob_set_value_varchar(&value, v);
  return ob_update(ob_set, table, row_key, row_key_len, column, &value);
}

OB_ERR_CODE ob_update_datetime(
    OB_SET* ob_set,
    const char* table,
    const char* row_key, int64_t row_key_len,
    const char* column,
    OB_DATETIME v)
{
  OB_VALUE value;
  ob_set_value_datetime(&value, v);
  return ob_update(ob_set, table, row_key, row_key_len, column, &value);
}

OB_ERR_CODE ob_insert(
    OB_SET* ob_set,
    const char* table,
    const char* row_key, int64_t row_key_len,
    const char* column,
    const OB_VALUE* v)
{
  OB_ERR_CODE err_code = OB_ERR_SUCCESS;
  ENSURE_COND(NULL != ob_set && NULL != table && NULL != column
      && NULL != row_key && row_key_len > 0 && NULL != v);
  if (OB_ERR_SUCCESS == err_code)
  {
    int table_len = static_cast<int32_t>(strlen(table));
    int column_len = static_cast<int32_t>(strlen(column));
    ObObj obj;
    set_obj(obj, v);
    int err = ob_set->mutator.insert(
        ObString(table_len, table_len, (char*)table),
        ObString(static_cast<int32_t>(row_key_len), static_cast<int32_t>(row_key_len), (char*)row_key),
        ObString(column_len, column_len, (char*)column),
        obj);
    if (OB_SUCCESS != err)
    {
      TBSYS_LOG(ERROR, "ObMutator insert returned error, "
          "err=%d table=%s row_key=%s column=%s",
          err, table, get_display_row_key(row_key, row_key_len), column);
      err_code = err_code_map(err);
      ob_set_err_with_default(err_code);
    }
  }
  return err_code;
}

OB_ERR_CODE ob_insert_int(
    OB_SET* ob_set,
    const char* table,
    const char* row_key, int64_t row_key_len,
    const char* column,
    OB_INT v)
{
  OB_VALUE value;
  ob_set_value_int(&value, v);
  return ob_update(ob_set, table, row_key, row_key_len, column, &value);
}

OB_ERR_CODE ob_insert_varchar(
    OB_SET* ob_set,
    const char* table,
    const char* row_key, int64_t row_key_len,
    const char* column,
    OB_VARCHAR v)
{
  OB_VALUE value;
  ob_set_value_varchar(&value, v);
  return ob_insert(ob_set, table, row_key, row_key_len, column, &value);
}

OB_ERR_CODE ob_insert_datetime(
    OB_SET* ob_set,
    const char* table,
    const char* row_key, int64_t row_key_len,
    const char* column,
    OB_DATETIME v)
{
  OB_VALUE value;
  ob_set_value_datetime(&value, v);
  return ob_insert(ob_set, table, row_key, row_key_len, column, &value);
}

OB_ERR_CODE ob_accumulate_int(
    OB_SET* ob_set,
    const char* table,
    const char* row_key, int64_t row_key_len,
    const char* column,
    OB_INT v)
{
  OB_VALUE value;
  ob_set_value_int(&value, v);
  int is_add_true = 1;
  return ob_real_update(ob_set, table, row_key, row_key_len,
      column, &value, is_add_true);
}

OB_ERR_CODE ob_del_row(
    OB_SET* ob_set,
    const char* table,
    const char* row_key, int64_t row_key_len)
{
  OB_ERR_CODE err_code = OB_ERR_SUCCESS;
  ENSURE_COND(NULL != ob_set && NULL != table
      && NULL != row_key && row_key_len > 0);
  if (OB_ERR_SUCCESS == err_code)
  {
    int table_len = static_cast<int32_t>(strlen(table));
    ObObj obj;
    int err = ob_set->mutator.del_row(
        ObString(table_len, table_len, (char*)table),
        ObString(static_cast<int32_t>(row_key_len), static_cast<int32_t>(row_key_len), (char*)row_key));
    if (OB_SUCCESS != err)
    {
      TBSYS_LOG(ERROR, "ObMutator del_row returned error, "
          "err=%d table=%s row_key=%s",
          err, table, get_display_row_key(row_key, row_key_len));
      err_code = err_code_map(err);
      ob_set_err_with_default(err_code);
    }
  }
  return err_code;
}

OB_SET_COND* ob_get_ob_set_cond(
    OB_SET* ob_set)
{
  OB_SET_COND* ob_set_cond = NULL;
  OB_ERR_CODE err_code = OB_ERR_SUCCESS;
  ENSURE_COND(NULL != ob_set)
  if (OB_ERR_SUCCESS == err_code)
  {
    ob_set_cond = ob_set->ob_set_cond;
  }
  return ob_set_cond;
}

OB_ERR_CODE ob_cond_add(
    OB_SET_COND* cond,
    const char* table,
    const char* row_key, int64_t row_key_len,
    const char* column,
    OB_LOGIC_OPERATOR op_type,
    const OB_VALUE* v)
{
  OB_ERR_CODE err_code = OB_ERR_SUCCESS;
  ENSURE_COND(NULL != cond && NULL != table && NULL != column
      && NULL != row_key && row_key_len > 0 && NULL != v);
  if (OB_ERR_SUCCESS == err_code)
  {
    int table_len = static_cast<int32_t>(strlen(table));
    int column_len = static_cast<int32_t>(strlen(column));
    ObObj obj;
    set_obj(obj, v);
    int err = cond->update_condition.add_cond(
        ObString(table_len, table_len, (char*)table),
        ObString(static_cast<int32_t>(row_key_len), static_cast<int32_t>(row_key_len), (char*)row_key),
        ObString(column_len, column_len, (char*)column),
        get_op_logic_operator(op_type), obj);
    if (OB_SUCCESS != err)
    {
      TBSYS_LOG(ERROR, "ObUpdateCondition add_cond returned error, "
          "err=%d table=%s row_key=%s column=%s op_type=%s",
          err, table, get_display_row_key(row_key, row_key_len), column,
          ob_logic_operation_to_str(op_type));
      err_code = err_code_map(err);
      ob_set_err_with_default(err_code);
    }
  }
  return err_code;
}

OB_ERR_CODE ob_cond_add_int(
    OB_SET_COND* cond,
    const char* table,
    const char* row_key, int64_t row_key_len,
    const char* column,
    const OB_LOGIC_OPERATOR op_type,
    OB_INT v)
{
  OB_VALUE value;
  ob_set_value_int(&value, v);
  return ob_cond_add(cond, table, row_key, row_key_len,
                     column, op_type, &value);
}

OB_ERR_CODE ob_cond_add_varchar(
    OB_SET_COND* cond,
    const char* table,
    const char* row_key, int64_t row_key_len,
    const char* column,
    const OB_LOGIC_OPERATOR op_type,
    OB_VARCHAR v)
{
  OB_VALUE value;
  ob_set_value_varchar(&value, v);
  return ob_cond_add(cond, table, row_key, row_key_len,
                     column, op_type, &value);
}

OB_ERR_CODE ob_cond_add_datetime(
    OB_SET_COND* cond,
    const char* table,
    const char* row_key, int64_t row_key_len,
    const char* column,
    const OB_LOGIC_OPERATOR op_type,
    OB_DATETIME v)
{
  OB_VALUE value;
  ob_set_value_datetime(&value, v);
  return ob_cond_add(cond, table, row_key, row_key_len,
                     column, op_type, &value);
}

OB_ERR_CODE ob_cond_add_exist(
    OB_SET_COND* cond,
    const char* table,
    const char* row_key, int64_t row_key_len,
    int is_exist)
{
  OB_ERR_CODE err_code = OB_ERR_SUCCESS;
  ENSURE_COND(NULL != cond && NULL != table
      && NULL != row_key && row_key_len > 0);
  if (OB_ERR_SUCCESS == err_code)
  {
    int table_len = static_cast<int32_t>(strlen(table));
    int err = cond->update_condition.add_cond(
        ObString(table_len, table_len, (char*)table),
        ObString(static_cast<int32_t>(row_key_len), static_cast<int32_t>(row_key_len), (char*)row_key),
        0 != is_exist);
    if (OB_SUCCESS != err)
    {
      TBSYS_LOG(ERROR, "ObUpdateCondition update returned error, "
          "err=%d table=%s row_key=%s is_exist=%d",
          err, table, get_display_row_key(row_key, row_key_len), is_exist);
      err_code = err_code_map(err);
      ob_set_err_with_default(err_code);
    }
  }
  return err_code;
}

////////// OB_SET END //////////

////////// OB_SCAN //////////
OB_ERR_CODE ob_scan(
    OB_SCAN* ob_scan,
    const char* table,
    const char* row_start, int64_t row_start_len,
    int start_included,
    const char* row_end, int64_t row_end_len,
    int end_included)
{
  OB_ERR_CODE err_code = OB_ERR_SUCCESS;
  ENSURE_COND(NULL != ob_scan && NULL != table
              && ((NULL != row_start && row_start_len > 0)
                  || (NULL == row_start && 0 == row_start_len
                      && 0 == start_included))
              && ((NULL != row_end && row_end_len > 0)
                  || (NULL == row_end && 0 == row_end_len
                      && 0 == end_included)))
  if (OB_ERR_SUCCESS == err_code)
  {
    int table_len = static_cast<int32_t>(strlen(table));

    ObVersionRange vrange;
    vrange.border_flag_.set_inclusive_start();
    vrange.border_flag_.set_max_value();
    vrange.start_version_ = 1;
    ob_scan->scan_param.set_version_range(vrange);

    ObRange range;
    if (NULL == row_start)
    {
      range.border_flag_.set_min_value();
    }
    else
    {
      if (0 != start_included)
      {
        range.border_flag_.set_inclusive_start();
      }
      range.start_key_ = ObString(static_cast<int32_t>(row_start_len), static_cast<int32_t>(row_start_len),
                                  (char*)row_start);
    }
    if (NULL == row_end)
    {
      range.border_flag_.set_max_value();
    }
    else
    {
      if (0 != end_included)
      {
        range.border_flag_.set_inclusive_end();
      }
      range.end_key_ = ObString(static_cast<int32_t>(row_end_len), static_cast<int32_t>(row_end_len), (char*)row_end);
    }

    const bool DEEP_COPY = true;
    ob_scan->scan_param.set(
        OB_INVALID_ID, ObString(table_len, table_len, (char*)table),
        range, DEEP_COPY);
  }
  return err_code;
}

OB_ERR_CODE ob_scan_column(
    OB_SCAN* ob_scan,
    const char* column,
    int is_return)
{
  OB_ERR_CODE err_code = OB_ERR_SUCCESS;
  ENSURE_COND(NULL != ob_scan && NULL != column)
  if (OB_ERR_SUCCESS == err_code)
  {
    int column_len = static_cast<int32_t>(strlen(column));
    int err = ob_scan->scan_param.add_column(
        ObString(column_len, column_len, (char*)column),
        is_return != 0);
    if (OB_SUCCESS != err)
    {
      TBSYS_LOG(ERROR, "ObScanParam add_column returned error, "
          "err=%d column=%s column_len=%d is_return=%d",
          err, column, column_len, is_return);
      err_code = err_code_map(err);
      ob_set_err_with_default(err_code);
    }
  }
  return err_code;
}

OB_ERR_CODE ob_scan_complex_column(
    OB_SCAN* ob_scan,
    const char* expression,
    const char* as_name,
    int is_return)
{
  OB_ERR_CODE err_code = OB_ERR_SUCCESS;
  ENSURE_COND(NULL != ob_scan
              && NULL != expression && NULL != as_name)
  if (OB_ERR_SUCCESS == err_code)
  {
    int expr_len = static_cast<int32_t>(strlen(expression));
    int name_len = static_cast<int32_t>(strlen(as_name));
    int err = ob_scan->scan_param.add_column(
        ObString(expr_len, expr_len, (char*)expression),
        ObString(name_len, name_len, (char*)as_name),
        is_return != 0);
    if (OB_SUCCESS != err)
    {
      TBSYS_LOG(ERROR, "ObScanParam add_column returned error, "
          "err=%d expression=%s expr_len=%d as_name=%s name_len=%d is_return=%d",
          err, expression, expr_len, as_name, name_len, is_return);
      err_code = err_code_map(err);
      ob_set_err_with_default(err_code);
    }
  }
  return err_code;
}

OB_ERR_CODE ob_scan_set_where(
    OB_SCAN* ob_scan,
    const char* expression)
{
  OB_ERR_CODE err_code = OB_ERR_SUCCESS;
  ENSURE_COND(NULL != ob_scan && NULL != expression)
  if (OB_ERR_SUCCESS == err_code)
  {
    int expr_len = static_cast<int32_t>(strlen(expression));
    int err = ob_scan->scan_param.add_where_cond(
        ObString(expr_len, expr_len, (char*)expression));
    if (OB_SUCCESS != err)
    {
      TBSYS_LOG(ERROR, "ObScanParam add_where_cond returned error, "
          "err=%d expression=%s expr_len=%d",
          err, expression, expr_len);
      err_code = err_code_map(err);
      ob_set_err_with_default(err_code);
    }
  }
  return err_code;
}

OB_ERR_CODE ob_scan_add_simple_where(
    OB_SCAN* ob_scan,
    const char* column,
    OB_LOGIC_OPERATOR op_type,
    const OB_VALUE* v)
{
  OB_ERR_CODE err_code = OB_ERR_SUCCESS;
  ENSURE_COND(NULL != ob_scan && NULL != column && NULL != v)
  if (OB_ERR_SUCCESS == err_code)
  {
    int column_len = static_cast<int32_t>(strlen(column));
    ObObj obj;
    set_obj(obj, v);
    int err = ob_scan->scan_param.get_filter_info().add_cond(
        ObString(column_len, column_len, (char*)column),
        get_op_logic_operator(op_type),
        obj);
    if (OB_SUCCESS != err)
    {
      TBSYS_LOG(ERROR, "ObSimpleFilter add_cond returned error, "
          "err=%d column=%s column_len=%d op_type=%s",
          err, column, column_len, ob_logic_operation_to_str(op_type));
      err_code = err_code_map(err);
      ob_set_err_with_default(err_code);
    }
  }
  return err_code;
}

OB_ERR_CODE ob_scan_add_simple_where_int(
    OB_SCAN* ob_scan,
    const char* column,
    OB_LOGIC_OPERATOR op_type,
    OB_INT v)
{
  OB_VALUE value;
  ob_set_value_int(&value, v);
  return ob_scan_add_simple_where(ob_scan, column, op_type, &value);
}

OB_ERR_CODE ob_scan_add_simple_where_varchar(
    OB_SCAN* ob_scan,
    const char* column,
    OB_LOGIC_OPERATOR op_type,
    OB_VARCHAR v)
{
  OB_VALUE value;
  ob_set_value_varchar(&value, v);
  return ob_scan_add_simple_where(ob_scan, column, op_type, &value);
}

OB_ERR_CODE ob_scan_add_simple_where_datetime(
    OB_SCAN* ob_scan,
    const char* column,
    OB_LOGIC_OPERATOR op_type,
    OB_DATETIME v)
{
  OB_VALUE value;
  ob_set_value_datetime(&value, v);
  return ob_scan_add_simple_where(ob_scan, column, op_type, &value);
}

OB_ERR_CODE ob_scan_orderby_column(
    OB_SCAN* ob_scan,
    const char* column,
    OB_ORDER order)
{
  OB_ERR_CODE err_code = OB_ERR_SUCCESS;
  ENSURE_COND(NULL != ob_scan && NULL != column)
  if (OB_ERR_SUCCESS == err_code)
  {
    int column_len = static_cast<int32_t>(strlen(column));
    int err = ob_scan->scan_param.add_orderby_column(
        ObString(column_len, column_len, (char*)column),
        get_ob_order(order));
    if (OB_SUCCESS != err)
    {
      TBSYS_LOG(ERROR, "ObScanParam add_orderby_column returned error, "
          "err=%d column=%s column_len=%d order=%s",
          err, column, column_len, ob_order_to_str(order));
      err_code = err_code_map(err);
      ob_set_err_with_default(err_code);
    }
  }
  return err_code;
}

OB_ERR_CODE ob_scan_set_limit(
    OB_SCAN* ob_scan,
    int64_t offset,
    int64_t count)
{
  OB_ERR_CODE err_code = OB_ERR_SUCCESS;
  ENSURE_COND(NULL != ob_scan
              && offset >= 0 && count >= 0)
  if (OB_ERR_SUCCESS == err_code)
  {
    int err = ob_scan->scan_param.set_limit_info(offset, count);
    if (OB_SUCCESS != err)
    {
      TBSYS_LOG(ERROR, "ObScanParam set_limit_info returned error, "
          "err=%d offset=%ld count=%ld",
          err, offset, count);
      err_code = err_code_map(err);
      ob_set_err_with_default(err_code);
    }
  }
  return err_code;
}

OB_ERR_CODE ob_scan_set_precision(
    OB_SCAN* ob_scan,
    double precision,
    int64_t reserved_count)
{
  OB_ERR_CODE err_code = OB_ERR_SUCCESS;
  ENSURE_COND(NULL != ob_scan
              && 0 <= precision && precision <= 1)
  if (OB_ERR_SUCCESS == err_code)
  {
    int err = ob_scan->scan_param.set_topk_precision(reserved_count, precision);
    if (OB_SUCCESS != err)
    {
      TBSYS_LOG(ERROR, "ObScanParam set_topk_precision returned error, "
          "err=%d precision=%f reserved_count=%ld",
          err, precision, reserved_count);
      err_code = err_code_map(err);
      ob_set_err_with_default(err_code);
    }
  }
  return err_code;
}

OB_GROUPBY_PARAM* ob_get_ob_groupby_param(
    OB_SCAN* ob_scan)
{
  OB_GROUPBY_PARAM* ob_groupby_param = NULL;
  OB_ERR_CODE err_code = OB_ERR_SUCCESS;
  ENSURE_COND(NULL != ob_scan)
  if (OB_ERR_SUCCESS == err_code)
  {
    ob_groupby_param = ob_scan->ob_groupby_param;
  }
  return ob_groupby_param;
}


OB_ERR_CODE ob_groupby_column(
    OB_GROUPBY_PARAM* ob_groupby,
    const char* column,
    int is_return)
{
  OB_ERR_CODE err_code = OB_ERR_SUCCESS;
  ENSURE_COND(NULL != ob_groupby && NULL != column)
  if (OB_ERR_SUCCESS == err_code)
  {
    int column_len = static_cast<int32_t>(strlen(column));
    int err = ob_groupby->groupby_param.add_groupby_column(
        ObString(column_len, column_len, (char*)column),
        is_return != 0);
    if (OB_SUCCESS != err)
    {
      TBSYS_LOG(ERROR, "ObGroupByParam add_groupby_column returned error, "
          "err=%d column=%s column_len=%d is_return=%d",
          err, column, column_len, is_return);
      err_code = err_code_map(err);
      ob_set_err_with_default(err_code);
    }
  }
  return err_code;
}

OB_ERR_CODE ob_aggregate_column(
    OB_GROUPBY_PARAM* ob_groupby,
    const OB_AGGREGATION_TYPE aggregation,
    const char* column,
    const char* as_name,
    int is_return)
{
  OB_ERR_CODE err_code = OB_ERR_SUCCESS;
  ENSURE_COND(NULL != ob_groupby && NULL != column)
  if (OB_ERR_SUCCESS == err_code)
  {
    int column_len = static_cast<int32_t>(strlen(column));
    int as_name_len = static_cast<int32_t>(strlen(as_name));
    int err = ob_groupby->groupby_param.add_aggregate_column(
        ObString(column_len, column_len, (char*)column),
        ObString(as_name_len, as_name_len, (char*)as_name),
        get_ob_aggregation_type(aggregation),
        is_return != 0);
    if (OB_SUCCESS != err)
    {
      TBSYS_LOG(ERROR, "ObGroupByParam add_aggregate_column returned error, "
          "err=%d column=%s column_len=%d as_name=%s as_name_len=%d "
          "is_return=%d",
          err, column, column_len, as_name, as_name_len, is_return);
      err_code = err_code_map(err);
      ob_set_err_with_default(err_code);
    }
  }
  return err_code;
}

OB_ERR_CODE ob_groupby_add_complex_column(
    OB_GROUPBY_PARAM* ob_groupby,
    const char* expression,
    const char* as_name,
    int is_return)
{
  OB_ERR_CODE err_code = OB_ERR_SUCCESS;
  ENSURE_COND(NULL != ob_groupby
              && NULL != expression && NULL != as_name)
  if (OB_ERR_SUCCESS == err_code)
  {
    int expr_len = static_cast<int32_t>(strlen(expression));
    int name_len = static_cast<int32_t>(strlen(as_name));
    int err = ob_groupby->groupby_param.add_column(
        ObString(expr_len, expr_len, (char*)expression),
        ObString(name_len, name_len, (char*)as_name),
        is_return != 0);
    if (OB_SUCCESS != err)
    {
      TBSYS_LOG(ERROR, "ObGroupByParam add_complex_column returned error, "
          "err=%d expression=%s expr_len=%d as_name=%s name_len=%d "
          "is_return=%d",
          err, expression, expr_len, as_name, name_len, is_return);
      err_code = err_code_map(err);
      ob_set_err_with_default(err_code);
    }
  }
  return err_code;
}

OB_ERR_CODE ob_groupby_add_return_column(
    OB_GROUPBY_PARAM* ob_groupby,
    const char* column_name,
    int is_return)
{
  OB_ERR_CODE err_code = OB_ERR_SUCCESS;
  ENSURE_COND(NULL != ob_groupby && NULL != column_name)
  if (OB_ERR_SUCCESS == err_code)
  {
    int name_len = static_cast<int32_t>(strlen(column_name));
    int err = ob_groupby->groupby_param.add_return_column(
        ObString(name_len, name_len, (char*)column_name),
        is_return != 0);
    if (OB_SUCCESS != err)
    {
      TBSYS_LOG(ERROR, "ObGroupByParam add_return_column returned error, "
          "err=%d column_name=%s name_len=%d is_return=%d",
          err, column_name, name_len, is_return);
      err_code = err_code_map(err);
      ob_set_err_with_default(err_code);
    }
  }
  return err_code;
}

OB_ERR_CODE ob_groupby_set_having(
    OB_GROUPBY_PARAM* ob_groupby,
    const char* expression)
{
  OB_ERR_CODE err_code = OB_ERR_SUCCESS;
  ENSURE_COND(NULL != ob_groupby
              && NULL != expression)
  if (OB_ERR_SUCCESS == err_code)
  {
    int expr_len = static_cast<int32_t>(strlen(expression));
    int err = ob_groupby->groupby_param.add_having_cond(
        ObString(expr_len, expr_len, (char*)expression));
    if (OB_SUCCESS != err)
    {
      TBSYS_LOG(ERROR, "ObScanParam add_where_cond returned error, "
          "err=%d expression=%s expr_len=%d",
          err, expression, expr_len);
      err_code = err_code_map(err);
      ob_set_err_with_default(err_code);
    }
  }
  return err_code;
}

////////// OB_SCAN END //////////

////////// OB FUNCTION //////////
OB* ob_api_init()
{
  OB* ob = NULL;

  ob_set_err(OB_ERR_SUCCESS, OB_ERR_DEFAULT_MSG[OB_ERR_SUCCESS]);

  ob_init_memory_pool();
  TBSYS_LOGGER._level = -1;

  ob = new (std::nothrow) OB;
  if (NULL == ob)
  {
    OB_ERR_CODE err_code = OB_ERR_MEM_NOT_ENOUGH;
    ob_set_err(err_code, OB_ERR_DEFAULT_MSG[err_code]);
  }

  return ob;
}

void ob_api_destroy(OB* ob)
{
  if (NULL != ob)
  {
    SAFE_DELETE(ob);
  }
}

OB_ERR_CODE ob_connect(
    OB* ob,
    const char* rs_addr, unsigned int rs_port,
    const char* user, const char* passwd)
{
  OB_ERR_CODE err_code = OB_ERR_SUCCESS;
  ENSURE_COND(NULL != ob);
  if (OB_ERR_SUCCESS == err_code)
  {
    int err = ob->client.connect(rs_addr, rs_port, user, passwd);
    err_code = err_code_map(err);
    if (OB_ERR_SUCCESS != err_code)
    {
      switch (err_code)
      {
        case OB_ERR_MEM_NOT_ENOUGH:
          SET_ERR_MEM_NOT_ENOUGH();
          break;
        case OB_ERR_RESPONSE_TIMEOUT:
          SET_ERR_RESPONSE_TIMEOUT();
          break;
      }
    }
  }
  return err_code;
}

OB_ERR_CODE ob_close(OB* ob)
{
  OB_ERR_CODE err_code = OB_ERR_SUCCESS;
  ENSURE_COND(NULL != ob);
  if (OB_ERR_SUCCESS == err_code)
  {
    ob->client.close();
  }
  return err_code;
}

OB_RES* ob_exec_scan(OB* ob, OB_SCAN* ob_scan)
{
  OB_RES* res = NULL;
  OB_RES* scan_res = NULL;
  OB_ERR_CODE err_code = OB_ERR_SUCCESS;
  ENSURE_COND(NULL != ob && NULL != ob_scan);
  if (OB_ERR_SUCCESS == err_code)
  {
    scan_res = ob->client.scan(ob_scan->scan_param);
    if (NULL == scan_res)
    {
      switch (ob_errno())
      {
        case OB_ERR_MEM_NOT_ENOUGH:
          SET_ERR_MEM_NOT_ENOUGH();
          break;
        case OB_ERR_RESPONSE_TIMEOUT:
          SET_ERR_RESPONSE_TIMEOUT();
          break;
      }
    }
    else
    {
      if (ob_fetch_row_num(scan_res) > 0 && ob_scan->join_st.join_list.size() > 0)
      {
        err_code = ob_exec_res_join(ob, scan_res, &(ob_scan->join_st), res);
        if (OB_ERR_SUCCESS != err_code)
        {
          TBSYS_LOG(ERROR, "ob_exec_res_join error");
          if (NULL != res)
          {
            ob_release_res_st(ob, res);
            res = NULL;
          }
        }
        ob_release_res_st(ob, scan_res);
      }
      else
      {
        res = scan_res;
      }
    }
  }
  return res;
}

OB_RES* ob_exec_get(OB* ob, OB_GET* ob_get)
{
  OB_RES* res = NULL;
  OB_ERR_CODE err_code = OB_ERR_SUCCESS;
  ENSURE_COND(NULL != ob && NULL != ob_get);
  if (OB_ERR_SUCCESS == err_code)
  {
    res = ob->client.get(ob_get->get_param);
    if (NULL == res)
    {
      switch (ob_errno())
      {
        case OB_ERR_MEM_NOT_ENOUGH:
          SET_ERR_MEM_NOT_ENOUGH();
          break;
        case OB_ERR_RESPONSE_TIMEOUT:
          SET_ERR_RESPONSE_TIMEOUT();
          break;
      }
    }
  }
  return res;
}

OB_ERR_CODE ob_exec_set(OB* ob, OB_SET* ob_set)
{
  OB_ERR_CODE err_code = OB_ERR_SUCCESS;
  ENSURE_COND(NULL != ob && NULL != ob_set);
  if (OB_ERR_SUCCESS == err_code)
  {
    err_code = ob->client.set(ob_set->mutator);
    if (OB_ERR_SUCCESS != err_code)
    {
      switch (ob_errno())
      {
        case OB_ERR_MEM_NOT_ENOUGH:
          SET_ERR_MEM_NOT_ENOUGH();
          break;
        case OB_ERR_RESPONSE_TIMEOUT:
          SET_ERR_RESPONSE_TIMEOUT();
          break;
      }
    }
  }
  TBSYS_LOG(DEBUG, "ob_exec_set err_code=%d", err_code);
  return err_code;
}

OB_ERR_CODE ob_submit(OB* ob, OB_REQ* req)
{
  OB_ERR_CODE err_code = OB_ERR_SUCCESS;
  ENSURE_COND(NULL != ob && NULL != req)
  if (OB_ERR_SUCCESS == err_code)
  {
    err_code = ob->client.submit(1, &req);
  }
  return err_code;
}

OB_ERR_CODE ob_get_results(
    OB* ob,
    int min_num,
    int max_num,
    int64_t timeout,
    int64_t* num,
    OB_REQ* reqs[])
{
  OB_ERR_CODE err_code = OB_ERR_SUCCESS;
  ENSURE_COND(NULL != ob && NULL != num && NULL != reqs
              && min_num <= max_num);
  if (OB_ERR_SUCCESS == err_code)
  {
    err_code = ob->client.get_results(min_num,
                                      max_num,
                                      timeout,
                                      *num,
                                      reqs);
    if (err_code != OB_ERR_SUCCESS)
    {
      if (OB_ERR_TIMEOUT == err_code)
      {
        ob_set_err(err_code, "Can't get enough results in function "
                "`ob_get_results'");
      }
      else
      {
        ob_set_err_with_default(err_code);
      }
    }
  }
  return err_code;
}


OB_REQ* ob_acquire_get_req(OB* ob)
{
  OB_REQ* req = NULL;
  if (NULL != ob)
  {
    req = ob->client.acquire_get_req();
  }
  else
  {
    ob_set_err(OB_ERR_INVALID_PARAM,
        OB_ERR_DEFAULT_MSG[OB_ERR_INVALID_PARAM]);
  }
  return req;
}

OB_REQ* ob_acquire_scan_req(OB* ob)
{
  OB_REQ* req = NULL;
  if (NULL != ob)
  {
    req = ob->client.acquire_scan_req();
  }
  else
  {
    ob_set_err(OB_ERR_INVALID_PARAM,
        OB_ERR_DEFAULT_MSG[OB_ERR_INVALID_PARAM]);
  }
  return req;
}

OB_REQ* ob_acquire_set_req(OB* ob)
{
  OB_REQ* req = NULL;
  if (NULL != ob)
  {
    req = ob->client.acquire_set_req();
  }
  else
  {
    ob_set_err(OB_ERR_INVALID_PARAM,
        OB_ERR_DEFAULT_MSG[OB_ERR_INVALID_PARAM]);
  }
  return req;
}

OB_GET* ob_acquire_get_st(OB* ob)
{
  OB_GET* get_st = NULL;
  OB_ERR_CODE err_code = OB_ERR_SUCCESS;
  ENSURE_COND(NULL != ob);
  if (OB_ERR_SUCCESS == err_code)
  {
    get_st = ob->client.acquire_get_st();
    if (OB_ERR_MEM_NOT_ENOUGH == ob_errno())
    {
      SET_ERR_MEM_NOT_ENOUGH();
    }
  }
  return get_st;
}

OB_SCAN* ob_acquire_scan_st(OB* ob)
{
  OB_SCAN* scan_st = NULL;
  OB_ERR_CODE err_code = OB_ERR_SUCCESS;
  ENSURE_COND(NULL != ob);
  if (OB_ERR_SUCCESS == err_code)
  {
    scan_st = ob->client.acquire_scan_st();
    if (OB_ERR_MEM_NOT_ENOUGH == ob_errno())
    {
      SET_ERR_MEM_NOT_ENOUGH();
    }
  }
  return scan_st;
}

OB_SET* ob_acquire_set_st(OB* ob)
{
  OB_SET* set_st = NULL;
  OB_ERR_CODE err_code = OB_ERR_SUCCESS;
  ENSURE_COND(NULL != ob);
  if (OB_ERR_SUCCESS == err_code)
  {
    set_st = ob->client.acquire_set_st();
    if (OB_ERR_MEM_NOT_ENOUGH == ob_errno())
    {
      SET_ERR_MEM_NOT_ENOUGH();
    }
  }
  return set_st;
}

void ob_release_get_st(OB* ob, OB_GET* ob_get)
{
  OB_ERR_CODE err_code = OB_ERR_SUCCESS;
  ENSURE_COND(NULL != ob);
  if (OB_ERR_SUCCESS == err_code)
  {
    ob->client.release_get_st(ob_get);
  }
}

void ob_release_scan_st(OB* ob, OB_SCAN* ob_scan)
{
  OB_ERR_CODE err_code = OB_ERR_SUCCESS;
  ENSURE_COND(NULL != ob);
  if (OB_ERR_SUCCESS == err_code)
  {
    ob->client.release_scan_st(ob_scan);
  }
}

void ob_release_set_st(OB* ob, OB_SET* ob_set)
{
  OB_ERR_CODE err_code = OB_ERR_SUCCESS;
  ENSURE_COND(NULL != ob);
  if (OB_ERR_SUCCESS == err_code)
  {
    ob->client.release_set_st(ob_set);
  }
}

void ob_release_req(OB* ob, OB_REQ* req, int is_free_mem)
{
  OB_ERR_CODE err_code = OB_ERR_SUCCESS;
  ENSURE_COND(NULL != ob && NULL != req);
  if (OB_ERR_SUCCESS == err_code)
  {
    ob->client.release_req(req, is_free_mem);
  }
}

void ob_reset_get_st(OB* ob, OB_GET* ob_get)
{
  OB_ERR_CODE err_code = OB_ERR_SUCCESS;
  ENSURE_COND(NULL != ob && NULL != ob_get);
  if (OB_ERR_SUCCESS == err_code)
  {
    ob->client.reset_get_st(ob_get);
  }
}

void ob_reset_scan_st(OB* ob, OB_SCAN* ob_scan)
{
  OB_ERR_CODE err_code = OB_ERR_SUCCESS;
  ENSURE_COND(NULL != ob && NULL != ob_scan);
  if (OB_ERR_SUCCESS == err_code)
  {
    ob->client.reset_scan_st(ob_scan);
  }
}

void ob_reset_set_st(OB* ob, OB_SET* ob_set)
{
  OB_ERR_CODE err_code = OB_ERR_SUCCESS;
  ENSURE_COND(NULL != ob && NULL != ob_set);
  if (OB_ERR_SUCCESS == err_code)
  {
    ob->client.reset_set_st(ob_set);
  }
}

OB_RES* ob_acquire_res_st(OB* ob)
{
  OB_RES* res_st = NULL;
  OB_ERR_CODE err_code = OB_ERR_SUCCESS;
  ENSURE_COND(NULL != ob);
  if (OB_ERR_SUCCESS == err_code)
  {
    res_st = ob->client.acquire_res_st();
    if (OB_ERR_MEM_NOT_ENOUGH == ob_errno())
    {
      SET_ERR_MEM_NOT_ENOUGH();
    }
  }
  return res_st;
}

void ob_release_res_st(OB* ob, OB_RES* res)
{
  OB_ERR_CODE err_code = OB_ERR_SUCCESS;
  ENSURE_COND(NULL != ob);
  if (OB_ERR_SUCCESS == err_code)
  {
    ob->client.release_res_st(res);
  }
}

void ob_req_set_resfd(OB_REQ* req, int32_t fd)
{
  if (NULL != req)
  {
    req->flags = FD_INVOCATION;
    req->resfd = fd;
  }
}

OB_ERR_CODE ob_api_cntl(OB* ob, int32_t cmd, ...)
{
  OB_ERR_CODE err_code = OB_ERR_SUCCESS;
  ENSURE_COND(NULL != ob);
  if (OB_ERR_SUCCESS == err_code)
  {
    va_list ap;
    va_start(ap, cmd);
    err_code = ob->client.api_cntl(cmd, ap);
    if (OB_ERR_SUCCESS != err_code)
    {
      TBSYS_LOG(ERROR, "api_cntl error");
    }
    va_end(ap);
  }
  return err_code;
}


////////// OB FUNCTION END //////////


////////// OB API DEBUG //////////

OB_RES* ob_get_ms_list(OB* ob)
{
  OB_RES* res = NULL;
  OB_ERR_CODE err_code = OB_ERR_SUCCESS;
  ENSURE_COND(NULL != ob);
  if (OB_ERR_SUCCESS == err_code)
  {
    res = ob_acquire_res_st(ob);
    if (NULL != res)
    {
      res->scanner.resize(1);
      res->cur_scanner = res->scanner.begin();
      int ret_code = ob->client.get_ms_list(res->scanner.front());
      if (OB_SUCCESS != ret_code)
      {
        ret_code = err_code_map(ret_code);
        switch (ret_code)
        {
          case OB_ERR_MEM_NOT_ENOUGH:
            SET_ERR_MEM_NOT_ENOUGH();
            break;
          case OB_ERR_RESPONSE_TIMEOUT:
            SET_ERR_RESPONSE_TIMEOUT();
            break;
          default:
            ob_set_err_with_default(ret_code);
        }
      }
    }
  }
  return res;
}

void ob_api_debug_log(OB* ob, const char* log_level, const char* log_file)
{
  OB_ERR_CODE err_code = OB_ERR_SUCCESS;
  ENSURE_COND(NULL != ob);
  if (OB_ERR_SUCCESS == err_code)
  {
    if (NULL != log_level)
    {
      TBSYS_LOGGER.setLogLevel(log_level);
    }
    else
    {
      TBSYS_LOGGER._level = -1;
    }
    if (NULL != log_file)
    {
      TBSYS_LOGGER.setFileName(log_file, true);
    }
  }
}

OB_ERR_CODE ob_debug(OB* ob, int32_t cmd, ...)
{
  OB_ERR_CODE err_code = OB_ERR_SUCCESS;
  ENSURE_COND(NULL != ob);
  if (OB_ERR_SUCCESS == err_code)
  {
    va_list ap;
    va_start(ap, cmd);
    err_code = ob->client.ob_debug(cmd, ap);
    if (OB_ERR_SUCCESS != err_code)
    {
      TBSYS_LOG(ERROR, "ob_debug error");
    }
    va_end(ap);
  }
  return err_code;
}

////////// OB API DEBUG END //////////

////////// OB EXTEND API //////////

OB_ERR_CODE ob_get_set_version(
    OB_GET* ob_get,
    int64_t version)
{
  OB_ERR_CODE err_code = OB_ERR_SUCCESS;
  ENSURE_COND(NULL != ob_get);
  if (OB_ERR_SUCCESS == err_code)
  {
    ObVersionRange version_range;
  
    version_range.start_version_ = version;
    version_range.end_version_ = version;
    version_range.border_flag_.set_inclusive_start();
    version_range.border_flag_.set_inclusive_end();
    ob_get->get_param.set_version_range(version_range);
  }
  return err_code;
}

////////// OB EXTEND API END //////////
