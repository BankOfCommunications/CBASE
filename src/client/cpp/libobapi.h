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

#ifndef _LIBOBAPI_H__
#define _LIBOBAPI_H__

#include "libobjoin.h"
#include <common/ob_action_flag.h>
#include <common/ob_scanner.h>
#include <common/ob_mutator.h>
#include <common/ob_scan_param.h>
#include <common/ob_get_param.h>
#include "ob_client.h"
#include <list>

using namespace oceanbase::common;
using namespace oceanbase::client;

#define OB_INVALID_THREAD_KEY     UINT32_MAX
#define MAX_REQ_MAXIMUM           1024
#define MAX_REQ_MINIMUM           1
#define OB_UNKNOWN_TYPE_STR       "UNKNOWN_TYPE"
#define OB_UNKNOWN_OPER_STR       "UNKNOWN_OPERATION"
#define MAX_DISPLAY_ROW_KEY_LEN   48

#define ENSURE_COND(cond) \
  if (!(cond)) \
  { \
    err_code = OB_ERR_INVALID_PARAM; \
    ob_set_errno(err_code); \
    snprintf(ob_get_err()->m, OB_ERR_MSG_MAX_LEN, \
        "Invalid parameter at calling function `%s', " \
        "pointer points to NULL address or length is invalid value", \
        __FUNCTION__); \
    TBSYS_LOG(ERROR, "%s", ob_error()); \
  }

#define SET_ERR_MEM_NOT_ENOUGH() \
  { \
    ob_set_errno(OB_ERR_MEM_NOT_ENOUGH); \
    snprintf(ob_get_err()->m, OB_ERR_MSG_MAX_LEN, \
        "Malloc memory failed in function `%s'", \
        __FUNCTION__); \
    TBSYS_LOG(ERROR, "%s", ob_error()); \
  }

#define SET_ERR_RESPONSE_TIMEOUT() \
  { \
    ob_set_errno(OB_ERR_RESPONSE_TIMEOUT); \
    snprintf(ob_get_err()->m, OB_ERR_MSG_MAX_LEN, \
        "Response from server is timeout in function `%s'", \
        __FUNCTION__); \
    TBSYS_LOG(ERROR, "%s", ob_error()); \
  }

#define SAFE_DELETE(ptr) {delete ptr; ptr = NULL;}

struct st_ob
{
  ObClient client;
};

struct st_ob_res
{
  typedef std::list<ObScanner> ScannerList;
  typedef std::list<ObScanner>::iterator ScannerListIter;
  ScannerList scanner;
  ScannerListIter cur_scanner;
  OB_CELL cur_cell;
  OB_CELL cell_array[OB_MAX_COLUMN_NUMBER];
  OB_ROW cur_row;
};

struct st_ob_groupby_param
{
  st_ob_groupby_param(ObGroupByParam& gp) : groupby_param(gp) {}
  ObGroupByParam& groupby_param;
};

struct st_ob_scan
{
  ObScanParam scan_param;
  st_ob_groupby_param* ob_groupby_param;
  OB_JOIN join_st;
};

struct st_ob_get
{
  static const bool DEEP_COPY = true;
  st_ob_get() : get_param(DEEP_COPY) {}
  ObGetParam get_param;
};

struct st_ob_set_cond
{
  st_ob_set_cond(ObUpdateCondition& uc) : update_condition(uc) {}
  ObUpdateCondition& update_condition;
};

struct st_ob_set
{
  ObMutator mutator;
  st_ob_set_cond* ob_set_cond;
};

void ob_set_err_with_default(OB_ERR_CODE code);

void set_obj_with_add(ObObj &obj, const OB_VALUE* v, int is_add);

void set_obj(ObObj &obj, const OB_VALUE* v);

void set_obj_int(ObObj &obj, OB_INT v, int is_add);

void set_obj_varchar(ObObj &obj, OB_VARCHAR v);

void set_obj_datetime(ObObj &obj, OB_DATETIME v);

//add peiouya [DATE_TIME] 20150906:b
void set_obj_date(ObObj &obj, OB_DATETIME v);

void set_obj_time(ObObj &obj, OB_DATETIME v);
//add 20150906:e

//add lijianqiang [INT_32] 20150930:b
void set_obj_int32(ObObj &obj, OB_INT32 v, int is_add);
//add 20150930:e

ObLogicOperator get_op_logic_operator(OB_LOGIC_OPERATOR o);

ObScanParam::Order get_ob_order(OB_ORDER o);

ObAggregateFuncType get_ob_aggregation_type(OB_AGGREGATION_TYPE o);

const char* ob_type_to_str(OB_TYPE type);

const char* ob_operation_code_to_str(OB_OPERATION_CODE oper_code);

const char* ob_order_to_str(OB_ORDER order);

const char* ob_aggregation_type_to_str(OB_AGGREGATION_TYPE aggr_type);

const char* ob_logic_operation_to_str(OB_LOGIC_OPERATOR logic_oper);

char* get_display_row_key(const char* row_key, int64_t row_key_len);

OB_ERR_CODE ob_real_update(
    OB_SET* ob_set,
    const char* table,
    const char* row_key, int64_t row_key_len,
    const char* column,
    const OB_VALUE* v,
    int is_add);

OB_RES* ob_acquire_res_st(OB* ob);

#endif // _LIBOBAPI_H__
