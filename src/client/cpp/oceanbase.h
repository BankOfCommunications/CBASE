/**
 * (C) 2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * oceanbase.h : API of OceanBase
 *
 * Authors:
 *   yanran <yanran.hfs@taobao.com>
 *
 */


#ifndef _OCEANBASE_H__
#define _OCEANBASE_H__

#ifdef  __cplusplus
extern "C" {
#endif

#include <stdint.h>
#include <sys/time.h>
#include <pthread.h>

#define OB_ERR_MSG_MAX_LEN        1024
typedef unsigned int   OB_ERR_CODE;
typedef char           OB_ERR_MSG[OB_ERR_MSG_MAX_LEN];
typedef const char*    OB_ERR_PMSG;
typedef struct ob_err
{
  OB_ERR_CODE c;
  OB_ERR_MSG  m;
} OB_ERR, *OB_PERR;

/* error code */
#define OB_ERR_SUCCESS           0
#define OB_ERR_ERROR             1
#define OB_ERR_SCHEMA            2
#define OB_ERR_MEM_NOT_ENOUGH    3
#define OB_ERR_INVALID_PARAM     4
#define OB_ERR_RESPONSE_TIMEOUT  5
#define OB_ERR_NOT_SUPPORTED     6
#define OB_ERR_USER_NOT_EXIST    7
#define OB_ERR_WRONG_PASSWORD    8
#define OB_ERR_PERMISSION_DENIED 9
#define OB_ERR_TIMEOUT           10
#define OB_ERR_NOT_MASTER        11

typedef enum en_ob_type
{
  OB_INT_TYPE,
  OB_VARCHAR_TYPE,
  OB_DATETIME_TYPE,
  OB_DOUBLE_TYPE,
  //add peiouya [DATE_TIME] 20150906:b
  OB_DATE_TYPE,
  OB_TIME_TYPE,
  //add 20150906:e
  //add lijianqiang [INT_32] 20150930:b
  OB_INT32_TYPE
  //add 20150930:e
} OB_TYPE;

typedef enum en_ob_operation_code
{
  OB_OP_GET,
  OB_OP_SCAN,
  OB_OP_SET,
} OB_OPERATION_CODE;

typedef enum en_ob_req_flag
{
  FD_INVOCATION = 1,
} OB_REQ_FLAG;

typedef enum en_ob_order
{
  /** 升序 */
  OB_ASC,
  /** 降序 */
  OB_DESC,
} OB_ORDER;

typedef enum en_ob_aggregation_type
{
  OB_SUM,
  OB_COUNT,
  OB_MAX,
  OB_MIN,
  OB_LISTAGG,//add gaojt [ListAgg][JHOBv0.1]20150104
} OB_AGGREGATION_TYPE;

typedef enum en_ob_logic_operator
{
  /** less than */
  OB_LT,
  /** less or equal */
  OB_LE,
  /** equal with */
  OB_EQ,
  /** greater than */
  OB_GT,
  /** greater or equal */
  OB_GE,
  /** not equal */
  OB_NE,
  /** substring */
  OB_LIKE,
} OB_LOGIC_OPERATOR;

typedef struct st_ob_varchar
{
  char* p;
  int64_t len;
}                       OB_VARCHAR;
typedef int64_t         OB_INT;
typedef struct timeval  OB_DATETIME;
typedef double          OB_DOUBLE;
//add lijianqiang [INT_32] 20150930:b
typedef int32_t         OB_INT32;
//add 20150930:e
/*
struct timeval details:

typedef long int __time_t
typedef long int __suseconds_t
struct timeval
{
  __time_t tv_sec;        * Seconds *
  __suseconds_t tv_usec;  * Microseconds *
};  

struct timeval represents the number of seconds and microseconds
since the Epoch (00:00:00 UTC, January 1, 1970)

And the total microsends since the Epoch is tv_sec * 1000000 + tv_usec
*/


typedef struct st_ob_value
{
  OB_TYPE type;
  union un_v
  {
    OB_INT      v_int;
    //add lijianqiang [INT_32] 20150930:b
    OB_INT32    v_int32;
    //add 20150930:e
    OB_VARCHAR  v_varchar;
    OB_DATETIME v_datetime;
    OB_DOUBLE  v_double;
  } v;
} OB_VALUE;

typedef struct st_ob_cell
{
  /** 具体的数据 */
  OB_VALUE      v;
  /** 这一项是否为NULL, 有这一项的原因是, 从数据v中看不出这个信息 */
  int           is_null;
  /** 表示一行是否存在 */
  int           is_row_not_exist;
  /** 这一项是否是这一行的第一个元素 */
  int           is_row_changed;
  /** 表名 */
  const char*   table;
  /** Row Key */
  const char*   row_key;
  /** 列名 */
  const char*   column;
  /** 表名字段的长度 */
  int64_t       table_len;
  /** Row Key的长度 */
  int64_t       row_key_len;
  /** 列名字段的长度 */
  int64_t       column_len;
} OB_CELL;

typedef struct st_ob_row
{
  int64_t        cell_num;
  OB_CELL*       cell;
} OB_ROW;

typedef enum OB_API_CNTL_CMD
{
  OB_S_MAX_REQ             = 101,
  OB_G_MAX_REQ             = 102,
  OB_S_TIMEOUT             = 103,
  OB_S_TIMEOUT_GET         = 105,
  OB_G_TIMEOUT_GET         = 106,
  OB_S_TIMEOUT_SCAN        = 107,
  OB_G_TIMEOUT_SCAN        = 108,
  OB_S_TIMEOUT_SET         = 109,
  OB_G_TIMEOUT_SET         = 110,
  OB_S_READ_MODE           = 111,
  OB_G_READ_MODE           = 112,
  OB_S_REFRESH_INTERVAL    = 113,
  OB_G_REFRESH_INTERVAL    = 114,
} OB_API_CNTL_CMD;

typedef enum OB_DEBUG_CMD
{
  OB_DEBUG_CLEAR_ACTIVE_MEMTABLE = 1000,
} OB_DEBUG_CMD;

typedef struct st_ob OB;

typedef struct st_ob_res OB_RES;

typedef struct st_ob_scan OB_SCAN;

typedef struct st_ob_get OB_GET;

typedef struct st_ob_set OB_SET;

typedef struct st_ob_set_cond OB_SET_COND;

typedef struct st_ob_groupby_param OB_GROUPBY_PARAM;

/** OB异步请求描述结构 */
typedef struct st_ob_req
{
  /** operation code */
  OB_OPERATION_CODE opcode;
  /** 是否使用下面的事件通知fd */
  OB_REQ_FLAG       flags;
  /** 进行事件通知的fd */
  int               resfd;
  /** 请求超时时间(微秒) */
  int32_t           timeout;
  /** 请求错误码 */
  OB_ERR_CODE       err_code;
  /** 用户自定数据域 */
  int32_t           req_id;
  /** 用户自定数据域 */
  void*             data;
  /** 用户自定数据域 */
  void*             arg;
  /** Get请求 */
  OB_GET*           ob_get;
  /** Scan请求 */
  OB_SCAN*          ob_scan;
  /** Set请求 */
  OB_SET*           ob_set;
  /** 请求结果 */
  OB_RES*           ob_res;
} OB_REQ;

/********* ERROR CODE & MSG *********/

/**
 * 得到错误码
 * @retval errno 错误码
 */
OB_ERR_CODE ob_errno();

/**
 * 得到错误描述
 * @retval msg 错误描述
 */
OB_ERR_PMSG ob_error();

OB_PERR ob_get_err();

void ob_set_errno(OB_ERR_CODE code);

void ob_set_error(OB_ERR_PMSG msg);

void ob_set_err(OB_ERR_CODE code, OB_ERR_PMSG msg);

/********* ERROR CODE & MSG END *********/


/********* OB_VALUE Process *********/
void ob_set_value_int(OB_VALUE* cell, OB_INT v);

void ob_set_value_varchar(OB_VALUE* cell, OB_VARCHAR v);

void ob_set_value_datetime(OB_VALUE* cell, OB_DATETIME v);

//add peiouya [DATE_TIME] 20150906:b
void ob_set_value_date(OB_VALUE* cell, OB_DATETIME v);

void ob_set_value_time(OB_VALUE* cell, OB_DATETIME v);
//add 20150906:e

//add lijianqian [INT_32] 20150930:b
void ob_set_value_int32(OB_VALUE* cell, OB_INT32 v);
//add 20150930:e

void ob_set_value_double(OB_VALUE* cell, OB_DOUBLE v);

/********* OB_VALUE Process END *********/


/********* OB_RES *********/

/**
 * @brief 从OB_RES结构体中获取请求结果
 *
 *   <b>ob_fetch_cell</b>从结果中不断的取出每一个cell, 即表格中的一个表项.
 *   取出的顺序是先在一行内按照Scan或者Get参数中指定的列的顺序依次取出,
 *   当一行取完后, 从下一行继续.
 *   每次取出的是一个指向OB_CELL结构体的指针, 直到所有数据取完,
 *   取完后, 会返回空指针.
 *
 *   \ref OB_CELL结构体代表表格中的一个项, 从这一项中可以取出很多信息:
 *   <ul>
 *     <li><b>v</b> 表示具体的数据;</li>
 *     <li><b>is_null</b> 表示这一项是否为NULL, 从数据v中看不出这个信息;</li>
 *     <li><b>is_row_not_exist</b> 这一项只有在Get请求的返回值中才会出现,
 *                      表示要取的某个数据, 所指定的行是不存在的.
 *                      在响应Scan请求时, 不存在的行不会返回.</li>
 *     <li><b>is_row_changed</b> 表示这一项是否是这一行的第一个元素</li>
 *     <li><b>table</b> 是表名</li>
 *     <li><b>row_key</b> 是Row Key</li>
 *     <li><b>column</b> 是列名</li>
 *     <li><b>table_len</b> 是表名字段的长度</li>
 *     <li><b>row_key_len</b> 是Row Key的长度</li>
 *     <li><b>column_len</b> 是列名字段的长度</li>
 *   </ul>
 *
 *   OB_RES结构体有两种方式可以取得, 第一种是使用\ref OB_REQ 结构体发起请求时,
 *   结果在OB_REQ结构体的<b>ob_res</b>域; 第二种是使用调用同步请求
 *   \ref ob_exec_scan 和 \ref ob_exec_get 两个函数时,
 *   若成功返回, 其返回值就是指向OB_RES结构提的指针.
 *
 * @param ob_res 有效的指向OB_RES结构体的指针
 * @retval OB_CELL* 有效的指向OB_CELL结构体的指针
 * @retval NULL 失败
 */
OB_CELL* ob_fetch_cell(OB_RES* ob_res);

/**
 * @brief 从OB_RES结构体中获取一行请求结果
 *
 *   <b>ob_fetch_row</b>取出的是结果中的一行数据.
 *
 *   \ref OB_ROW结构体中<b>cell_num</b>表示的是一行数据的个数.
 *   <b>cell</b>数组表示的是一个个元素, 类型是\ref OB_CELL*.
 *
 * @param ob_res 有效的指向OB_RES结构体的指针
 * @retval OB_ROW* 有效的指向OB_ROW结构体的指针
 * @retval NULL 失败
 *
 */
OB_ROW* ob_fetch_row(OB_RES* ob_res);

/**
 * @brief 将OB_RES结构体的游标设置到开头
 *
 *   <b>ob_res_seek_to_begin_cell</b>一般和<b>ob_fetch_cell</b>结合起来使用,
 *   不断的调用ob_fetch_cell函数, 可以将OB_RES中的所有结果依次遍历出来,
 *   如果需要回到开头, 从第一个Cell重新读取, 则调用ob_res_seek_begin_cell
 *   函数.
 *
 * @param ob_res 有效的指向OB_RES结构体的指针
 * @retval OB_ERR_SUCCESS 成功
 * @retval otherwise 失败
 */
OB_ERR_CODE ob_res_seek_to_begin_cell(OB_RES* ob_res);

/**
 * @brief 将OB_RES结构体的游标设置到开头
 *
 *   <b>ob_res_seek_to_begin_row</b>一般和<b>ob_fetch_row</b>结合起来使用,
 *   不断的调用ob_fetch_row函数, 可以将OB_RES中的所有结果依次遍历出来,
 *   如果需要回到开头, 从第一行重新读取, 则调用ob_res_seek_begin_row
 *   函数.
 *
 * @param ob_res 有效的指向OB_RES结构体的指针
 * @retval OB_ERR_SUCCESS 成功
 * @retval otherwise 失败
 */
OB_ERR_CODE ob_res_seek_to_begin_row(OB_RES* ob_res);

/**
 * @brief 从OB_RES结构体中获取结果行数
 *
 *   <b>ob_fetch_row_num</b>仅在返回Scan请求结果时, 才是有效的.
 *   在使用Get请求时, 这里返回的值没有意义.
 *
 *   执行Scan请求时, <b>ob_fetch_row_num</b>函数返回结果的行数.
 *
 * @param ob_res 有效的指向OB_RES结构体的指针
 * @retval row_num 结果的行数
 */
int64_t ob_fetch_row_num(OB_RES* ob_res);

/**
 * @brief 从OB_RES结构体中获取全部结果行数
 *
 *   <b>ob_fetch_whole_res_row_num</b>仅在返回Scan请求结果时, 
 *   并且Scan请求包含ORDER BY或者GROUP BY才是有效的,
 *   否则, 这里返回的值没有意义.
 *
 *   执行Scan请求时, <b>ob_fetch_whole_res_row_num</b>函数返回这次查询
 *   所处理结果的全部行数, 即不包含LIMIT限定时结果行数。
 *
 * @param ob_res 有效的指向OB_RES结构体的指针
 * @retval whole_res_row_num 全部结果的行数
 */
int64_t ob_fetch_whole_res_row_num(OB_RES* ob_res);

/**
 * @brief 表示结果是否包含了所请求的全部数据
 *
 *   \deprecated
 *   <b>ob_is_fetch_all_res</b>这个函数已经过时，
 *   在之前的版本中，
 *   客户端最多能拿到的数据是2MB限制，这是OB的一个天然限制。
 *   在这种情况下，需要ob_is_fetch_all_res函数
 *   来判断2MB是否装下了全部结果
 *   新版本中（自客户端1.1.0开始）已经没有这个限制了，
 *   所有结果可以一次取完。
 *
 *   在OceanBase中, 请求结果OB_RES的结构体大小有原生的限制,
 *   OB_RES结构体大小不允许超过2MB.
 *   所有, 请求的返回结果不一定包含了所请求的全部数据,
 *   一般来说, 如果需要全部数据, 需要根据这次返回的结果的最后一个row key
 *   发起下一次查询, 如此重复直到取到所有结果.
 *
 * @param ob_res 有效的指向OB_RES结构体的指针
 * @retval 0 表示没有返回所请求的所有数据
 * @retval not_0 表示没有返回所请求的所有数据
 */
int ob_is_fetch_all_res(OB_RES* ob_res);

/********* OB_RES END *********/


/********* OB_GET *********/

/**
 * @brief 向OB_GET结构体中添加一个Cell
 *
 *   <b>ob_get_cell</b>用于取一个或者多个特定的Cell.
 *   指向\ref OB_GET结构体的有效指针有两种方法可以获得,
 *   一是调用\ref ob_acquire_get_req 函数, 获得的\ref OB_REQ结构体中
 *   ob_get域即是; 另一种是调用\ref ob_acquire_get_st 函数, 直接获得
 *   指向\ref OB_GET结构体的指针.
 *
 *   <b>ob_get_cell</b>函数可以多次调用分别指定get多个Cell, 每一次调用
 *   均需要指定完整的表名, row_key, 和列名.
 *
 * @param ob_get 有效的指向OB_GET结构体的指针
 * @param table 表名
 * @param row_key Row_key
 * @param row_key_len Row_key长度
 * @param column 列名
 * @retval OB_ERR_SUCCESS 成功
 * @retval otherwise 失败
 */
OB_ERR_CODE ob_get_cell(
    OB_GET* ob_get,
    const char* table,
    const char* row_key, int64_t row_key_len,
    const char* column);

/********* OB_GET *********/


/********* OB_SET *********/

/**
 * 向OB_SET中添加一条更新操作
 * @param ob_set Rootserver地址
 * @param table 表名, '\\0'结尾字符串
 * @param row_key row_key
 * @param row_key_len row_key长度
 * @param column 列名, '\\0'结尾字符串
 * @param v 添加的内容
 * @retval OB_ERR_SUCCESS 成功
 * @retval otherwise 失败
 */
OB_ERR_CODE ob_update(
    OB_SET* ob_set,
    const char* table,
    const char* row_key, int64_t row_key_len,
    const char* column,
    const OB_VALUE* v);

/**
 * 向OB_SET中添加一条更新int型的操作
 * @param ob_set Rootserver地址
 * @param table 表名, '\\0'结尾字符串
 * @param row_key row_key
 * @param row_key_len row_key长度
 * @param column 列名, '\\0'结尾字符串
 * @param v 添加的内容
 * @retval OB_ERR_SUCCESS 成功
 * @retval otherwise 失败
 */
OB_ERR_CODE ob_update_int(
    OB_SET* ob_set,
    const char* table,
    const char* row_key, int64_t row_key_len,
    const char* column,
    OB_INT v);

/**
 * 向OB_SET中添加一条更新varchar型的操作
 * @param ob_set Rootserver地址
 * @param table 表名, '\\0'结尾字符串
 * @param row_key row_key
 * @param row_key_len row_key长度
 * @param column 列名, '\\0'结尾字符串
 * @param v 添加的内容
 * @retval OB_ERR_SUCCESS 成功
 * @retval otherwise 失败
 */
OB_ERR_CODE ob_update_varchar(
    OB_SET* ob_set,
    const char* table,
    const char* row_key, int64_t row_key_len,
    const char* column,
    OB_VARCHAR v);

/**
 * 向OB_SET中添加一条更新datetime型的操作
 * @param ob_set 有效的指向OB_SET结构体的指针
 * @param table 表名, '\\0'结尾字符串
 * @param row_key row_key
 * @param row_key_len row_key长度
 * @param column 列名, '\\0'结尾字符串
 * @param v 添加的内容
 * @retval OB_ERR_SUCCESS 成功
 * @retval otherwise 失败
 */
OB_ERR_CODE ob_update_datetime(
    OB_SET* ob_set,
    const char* table,
    const char* row_key, int64_t row_key_len,
    const char* column,
    OB_DATETIME v);

/**
 * 向OB_SET中添加一条插入操作
 * @param ob_set 有效的指向OB_SET结构体的指针
 * @param table 表名, '\\0'结尾字符串
 * @param row_key row_key
 * @param row_key_len row_key长度
 * @param column 列名, '\\0'结尾字符串
 * @param v 添加的内容
 * @retval OB_ERR_SUCCESS 成功
 * @retval otherwise 失败
 */
OB_ERR_CODE ob_insert(
    OB_SET* ob_set,
    const char* table,
    const char* row_key, int64_t row_key_len,
    const char* column,
    const OB_VALUE* v);

/**
 * 向OB_SET中添加一条插入int型的操作
 * @param ob_set 有效的指向OB_SET结构体的指针
 * @param table 表名, '\\0'结尾字符串
 * @param row_key row_key
 * @param row_key_len row_key长度
 * @param column 列名, '\\0'结尾字符串
 * @param v 添加的内容
 * @retval OB_ERR_SUCCESS 成功
 * @retval otherwise 失败
 */
OB_ERR_CODE ob_insert_int(
    OB_SET* ob_set,
    const char* table,
    const char* row_key, int64_t row_key_len,
    const char* column,
    OB_INT v);

/**
 * 向OB_SET中添加一条插入varchar型的操作
 * @param ob_set 有效的指向OB_SET结构体的指针
 * @param table 表名, '\\0'结尾字符串
 * @param row_key row_key
 * @param row_key_len row_key长度
 * @param column 列名, '\\0'结尾字符串
 * @param v 添加的内容
 * @retval OB_ERR_SUCCESS 成功
 * @retval otherwise 失败
 */
OB_ERR_CODE ob_insert_varchar(
    OB_SET* ob_set,
    const char* table,
    const char* row_key, int64_t row_key_len,
    const char* column,
    OB_VARCHAR v);

/**
 * 向OB_SET中添加一条插入datetime型的操作
 * @param ob_set 有效的指向OB_SET结构体的指针
 * @param table 表名, '\\0'结尾字符串
 * @param row_key row_key
 * @param row_key_len row_key长度
 * @param column 列名, '\\0'结尾字符串
 * @param v 添加的内容
 * @retval OB_ERR_SUCCESS 成功
 * @retval otherwise 失败
 */
OB_ERR_CODE ob_insert_datetime(
    OB_SET* ob_set,
    const char* table,
    const char* row_key, int64_t row_key_len,
    const char* column,
    OB_DATETIME v);

/**
 * 向OB_SET中添加一条增加int型的操作
 *
 *   accumulate操作是在原数值基础上累加新的数值
 *
 * @param ob_set Rootserver地址
 * @param table 表名, '\\0'结尾字符串
 * @param row_key row_key
 * @param row_key_len row_key长度
 * @param column 列名, '\\0'结尾字符串
 * @param v 添加的内容
 * @retval OB_ERR_SUCCESS 成功
 * @retval otherwise 失败
 */
OB_ERR_CODE ob_accumulate_int(
    OB_SET* ob_set,
    const char* table,
    const char* row_key, int64_t row_key_len,
    const char* column,
    OB_INT v);

/**
 * 向OB_SET中添加一条删除一行的操作
 * @param ob_set 有效的指向OB_SET结构体的指针
 * @param table 表名, '\\0'结尾字符串
 * @param row_key row_key
 * @param row_key_len row_key长度
 * @retval OB_ERR_SUCCESS 成功
 * @retval otherwise 失败
 */
OB_ERR_CODE ob_del_row(
    OB_SET* ob_set,
    const char* table,
    const char* row_key, int64_t row_key_len);

OB_SET_COND* ob_get_ob_set_cond(
    OB_SET* ob_set);

OB_ERR_CODE ob_cond_add(
    OB_SET_COND* cond,
    const char* table,
    const char* row_key, int64_t row_key_len,
    const char* column,
    OB_LOGIC_OPERATOR op_type,
    const OB_VALUE* v);

OB_ERR_CODE ob_cond_add_int(
    OB_SET_COND* cond,
    const char* table,
    const char* row_key, int64_t row_key_len,
    const char* column,
    OB_LOGIC_OPERATOR op_type,
    OB_INT v);

OB_ERR_CODE ob_cond_add_varchar(
    OB_SET_COND* cond,
    const char* table,
    const char* row_key, int64_t row_key_len,
    const char* column,
    OB_LOGIC_OPERATOR op_type,
    OB_VARCHAR v);

OB_ERR_CODE ob_cond_add_datetime(
    OB_SET_COND* cond,
    const char* table,
    const char* row_key, int64_t row_key_len,
    const char* column,
    OB_LOGIC_OPERATOR op_type,
    OB_DATETIME v);

OB_ERR_CODE ob_cond_add_exist(
    OB_SET_COND* cond,
    const char* table,
    const char* row_key, int64_t row_key_len,
    int is_exist);

/********* OB_SET END *********/


/********* OB_SCAN *********/

/**
 * @brief 指定这次Scan操作的表名称, 和起止行.
 *
 *   ob_scan是\ref OB_REQ 的ob_scan域. 调用\ref ob_acquire_scan_req 函数
 *   获取的OB_REQ结构体包含, ob_scan域会指向一个有效OB_SCAN结构体.
 *   或者使用\ref ob_acquire_scan_st可以直接获得指向\ref OB_REQ结构体的指针.
 *
 *   table是这次扫描的目标表格.
 *
 *   row_start和row_end指明了扫描范围,
 *   start_included和end_included用于指定范围的开闭关系.
 *   所以范围可以有[row_start, row_end], [row_start, row_end),
 *   (row_start, row_end], (row_start, row_end)四种.
 *
 *   另外, OB支持row_start和row_end分别指定为所有最小和最大的row_key.
 *   例如, 扫描一张表的所有数据, 那么就是需要指定从最小的key到最大的key.
 *   或者扫描从某一个key开始到最后的所有数据, 就需要指定end_key为最大的key.
 *
 *   在现有的参数中, 保证row_start为NULL, row_start_len为0, start_included为0.
 *   表示start_key是最小的key.
 *   保证row_end为NULL, row_end_len为0, end_included为0,
 *   表示end_key是最大的key.
 *
 * @param ob_scan 有效的指向OB_SCAN结构体的指针
 * @param table 表名, '\\0'结尾字符串
 * @param row_start 起始行
 * @param row_start_len row_start字段长度
 * @param start_included Scan范围是否包含起始行
 * @param row_end 终止行
 * @param row_end_len row_end字段长度
 * @param end_included Scan范围是否包含终止行
 * @retval OB_ERR_SUCCESS 成功
 * @retval otherwise 失败
 *
 * Example:
 * @include scan_sync_demo.c
 */
OB_ERR_CODE ob_scan(
    OB_SCAN* ob_scan,
    const char* table,
    const char* row_start, int64_t row_start_len,
    int start_included,
    const char* row_end, int64_t row_end_len,
    int end_included);

/**
 * @brief 添加一个Scan操作的列.
 *
 *   ob_scan是\ref OB_REQ 的ob_scan域. 调用\ref ob_acquire_scan_req 函数
 *   获取的OB_REQ结构体包含, ob_scan域会指向一个有效OB_SCAN结构体.
 *
 *   column是添加的列.
 *
 *   is_return表示这个列是否出现在最终结果里, 在一些查询请求里,
 *   有些列只参与了更复杂的列的计算.
 *   即, 如果需要返回一列A + B * C, 被操作的列是A, B, C三列,
 *   但是A, B, C并不需要成为结果中的列, 这些列需要调用ob_scan_column
 *   函数添加进来, 同时is_return设置为false.
 *
 * @param ob_scan 有效的指向OB_SCAN结构体的指针
 * @param column 列名, '\\0'结尾字符串
 * @param is_return 这一列是否作为最后结果返回
 * @retval OB_ERR_SUCCESS 成功
 * @retval otherwise 失败
 *
 * Example:
 * @include scan_sync_demo.c
 */
OB_ERR_CODE ob_scan_column(
    OB_SCAN* ob_scan,
    const char* column,
    int is_return);

/**
 * @brief 添加一个Scan操作的复杂列.
 *
 *   ob_scan是\ref OB_REQ 的ob_scan域. 调用\ref ob_acquire_scan_req 函数
 *   获取的OB_REQ结构体包含, ob_scan域会指向一个有效OB_SCAN结构体.
 *
 *   expression是需要添加的复杂列. 复杂列指包含有加、减、乘、除等操作
 *   的列, 譬如A + B * C这种列. 对复杂列的要求是, 复杂列的所有运算元素,
 *   例如这里的A, B, C都需要已经通过\ref ob_scan_column添加进来.
 *
 *   as_name为添加的这个列定义一个新名字, 这个名字会作为最终结果的列名
 *   (如果需要返回为最终结果的话).
 *
 *   is_return表示这个列是否出现在最终结果里, 在一些查询请求里,
 *   有些只参与了更复杂的列的计算.
 *
 * @param ob_scan 有效的指向OB_SCAN结构体的指针
 * @param expression 复杂列, '\\0'结尾字符串
 * @param as_name 复杂列计算结果的新列名
 * @param is_return 这一列是否作为最后结果返回
 * @retval OB_ERR_SUCCESS 成功
 * @retval otherwise 失败
 *
 * Example:
 * @include scan_sync_demo.c
 */
OB_ERR_CODE ob_scan_complex_column(
    OB_SCAN* ob_scan,
    const char* expression,
    const char* as_name,
    int is_return);

/**
 * @brief 添加一个Scan操作的过滤条件.
 *
 *   ob_scan是\ref OB_REQ 的ob_scan域. 调用\ref ob_acquire_scan_req 函数
 *   获取的OB_REQ结构体包含, ob_scan域会指向一个有效OB_SCAN结构体.
 *
 *   expression是需要添加的过滤条件. 过滤条件相当于SQL语句中的where条件.
 *
 * @param ob_scan 有效的指向OB_SCAN结构体的指针
 * @param expression 过滤条件, '\\0'结尾字符串
 * @retval OB_ERR_SUCCESS 成功
 * @retval otherwise 失败
 *
 * Example:
 * @include scan_sync_demo.c
 */
OB_ERR_CODE ob_scan_set_where(
    OB_SCAN* ob_scan,
    const char* expression);

/**
 * @brief 添加一个Scan操作的简单过滤条件.
 *
 *   <b>ob_scan_add_simple_where</b>相比较与\ref ob_scan_set_where，
 *   是一个弱化的条件过滤方法. ob_scan_set_where函数可以书写任意的表达式,
 *   包括某个运算结果是否满足条件.
 *   ob_scan_add_simple_where只能过滤原始的列.
 *
 *   ob_scan_add_simple_where可以调用多次添加多个过滤条件,
 *   但是多个条件必须同时满足才认为是符合条件的行.
 *
 *   如果OB Server运行的是0.2版本, 则只能支持ob_scan_add_simple_where不支持
 *   ob_scan_set_where.
 *
 * @param ob_scan  有效的指向OB_SCAN结构体的指针
 * @param column   需要过滤的列, '\\0'结尾字符串
 * @param op_type  过滤操作的方法, \ref OB_LOGIC_OPERATOR类型
 * @param v        过滤值, \ref OB_VALUE*类型
 * @retval OB_ERR_SUCCESS 成功
 * @retval otherwise 失败
 */
OB_ERR_CODE ob_scan_add_simple_where(
    OB_SCAN* ob_scan,
    const char* column,
    OB_LOGIC_OPERATOR op_type,
    const OB_VALUE* v);

/**
 * @brief 添加一个Scan操作的简单过滤条件.
 *
 *   <b>ob_scan_add_simple_where_int</b>相比较与\ref ob_scan_set_where，
 *   是一个弱化的条件过滤方法. ob_scan_set_where函数可以书写任意的表达式,
 *   包括某个运算结果是否满足条件.
 *   ob_scan_add_simple_where_int只能过滤原始的列,
 *   并且过滤值只能是OB_INT型.
 *
 *   ob_scan_add_simple_where_int可以调用多次添加多个过滤条件,
 *   但是多个条件必须同时满足才认为是符合条件的行.
 *
 *   如果OB Server运行的是0.2版本, 则只能支持ob_scan_add_simple_where_int不支持
 *   ob_scan_set_where.
 *
 * @param ob_scan  有效的指向OB_SCAN结构体的指针
 * @param column   需要过滤的列, '\\0'结尾字符串
 * @param op_type  过滤操作的方法, \ref OB_LOGIC_OPERATOR类型
 * @param v        过滤值, \ref OB_INT类型
 * @retval OB_ERR_SUCCESS 成功
 * @retval otherwise 失败
 */
OB_ERR_CODE ob_scan_add_simple_where_int(
    OB_SCAN* ob_scan,
    const char* column,
    OB_LOGIC_OPERATOR op_type,
    OB_INT v);

/**
 * @brief 添加一个Scan操作的简单过滤条件.
 *
 *   <b>ob_scan_add_simple_where_varchar</b>相比较与\ref ob_scan_set_where，
 *   是一个弱化的条件过滤方法. ob_scan_set_where函数可以书写任意的表达式,
 *   包括某个运算结果是否满足条件.
 *   ob_scan_add_simple_where_varchar只能过滤原始的列,
 *   并且过滤值只能是OB_VARCHAR型.
 *
 *   ob_scan_add_simple_where_varchar可以调用多次添加多个过滤条件,
 *   但是多个条件必须同时满足才认为是符合条件的行.
 *
 *   如果OB Server运行的是0.2版本,
 *   则只能支持ob_scan_add_simple_where_varchar
 *   不支持ob_scan_set_where.
 *
 * @param ob_scan  有效的指向OB_SCAN结构体的指针
 * @param column   需要过滤的列, '\\0'结尾字符串
 * @param op_type  过滤操作的方法, \ref OB_LOGIC_OPERATOR类型
 * @param v        过滤值, \ref OB_VARCHAR类型
 * @retval OB_ERR_SUCCESS 成功
 * @retval otherwise 失败
 */
OB_ERR_CODE ob_scan_add_simple_where_varchar(
    OB_SCAN* ob_scan,
    const char* column,
    OB_LOGIC_OPERATOR op_type,
    OB_VARCHAR v);

/**
 * @brief 添加一个Scan操作的简单过滤条件.
 *
 *   <b>ob_scan_add_simple_where_datetime</b>相比较与\ref ob_scan_set_where，
 *   是一个弱化的条件过滤方法. ob_scan_set_where函数可以书写任意的表达式,
 *   包括某个运算结果是否满足条件.
 *   ob_scan_add_simple_where_datetime只能过滤原始的列,
 *   并且过滤值只能是OB_DATETIME型.
 *
 *   ob_scan_add_simple_where_datetime可以调用多次添加多个过滤条件,
 *   但是多个条件必须同时满足才认为是符合条件的行.
 *
 *   如果OB Server运行的是0.2版本,
 *   则只能支持ob_scan_add_simple_where_datetime
 *   不支持ob_scan_set_where.
 *
 * @param ob_scan  有效的指向OB_SCAN结构体的指针
 * @param column   需要过滤的列, '\\0'结尾字符串
 * @param op_type  过滤操作的方法, \ref OB_LOGIC_OPERATOR类型
 * @param v        过滤值, \ref OB_DATETIME类型
 * @retval OB_ERR_SUCCESS 成功
 * @retval otherwise 失败
 */
OB_ERR_CODE ob_scan_add_simple_where_datetime(
    OB_SCAN* ob_scan,
    const char* column,
    OB_LOGIC_OPERATOR op_type,
    OB_DATETIME v);

/**
 * @brief 指定Scan操作结果的排序方法.
 *
 *   ob_scan是\ref OB_REQ 的ob_scan域. 调用\ref ob_acquire_scan_req 函数
 *   获取的OB_REQ结构体包含, ob_scan域会指向一个有效OB_SCAN结构体.
 *
 *   column指定排序的列, order指定排序的方向.
 *
 *   当需要按照多个列排序时, 可以调用这个函数多次.
 *
 * @param ob_scan 有效的指向OB_SCAN结构体的指针
 * @param column 需要排序的列, '\\0'结尾字符串
 * @param order 排序方向, OB_ASC(升序)或者OB_DESC(降序)
 * @retval OB_ERR_SUCCESS 成功
 * @retval otherwise 失败
 *
 * Example:
 * @include scan_sync_demo.c
 */
OB_ERR_CODE ob_scan_orderby_column(
    OB_SCAN* ob_scan,
    const char* column,
    OB_ORDER order);

/**
 * @brief 设置Scan操作的分页.
 *
 *   ob_scan是\ref OB_REQ 的ob_scan域. 调用\ref ob_acquire_scan_req 函数
 *   获取的OB_REQ结构体包含, ob_scan域会指向一个有效OB_SCAN结构体.
 *
 *   offset, count两个参数的行为和SQL语句LIMIT的两个参数相同.
 *   offset指明返回结果的起始点, count指明返回的结果条数.
 *
 * @param ob_scan 有效的指向OB_SCAN结构体的指针
 * @param offset 返回结果的起始点
 * @param count 返回的结果条数
 * @retval OB_ERR_SUCCESS 成功
 * @retval otherwise 失败
 *
 * Example:
 * @include scan_sync_demo.c
 */
OB_ERR_CODE ob_scan_set_limit(
    OB_SCAN* ob_scan,
    int64_t offset,
    int64_t count);

/**
 * @brief 设置Scan操作的计算精度.
 *
 *   普通的Scan参做是没有精度概念的,
 *   但是如果操作是类似于SUM(A)/SUM(B)的Top N, Scan操作就需要指明精度.
 *   OceanBase是一个分布式系统, 数据分散在多台机器上,
 *   如果操作的精度要求不高, 就可以大大减少机器之间的数据传输量,
 *   进而大大提交查询的速度.
 *
 *   ob_scan是\ref OB_REQ 的ob_scan域. 调用\ref ob_acquire_scan_req 函数
 *   获取的OB_REQ结构体包含, ob_scan域会指向一个有效OB_SCAN结构体.
 *
 *   precision是一个在0到1之间的小数, 表示每个分片保留数据的百分比.
 *   reserved_count是每隔数据分片最少保留的条目数.
 *
 * @param ob_scan 有效的指向OB_SCAN结构体的指针
 * @param precision 每隔数据分片保留数据的百分比
 * @param reserved_count 每个数据分片最少保留的数据条目数
 * @retval OB_ERR_SUCCESS 成功
 * @retval otherwise 失败
 *
 * Example:
 * @include scan_sync_demo.c
 */
OB_ERR_CODE ob_scan_set_precision(
    OB_SCAN* ob_scan,
    double precision,
    int64_t reserved_count);

/**
 * @brief 得到OB_GROUPBY_PARAM结构体的指针
 *
 *   ob_scan是\ref OB_REQ 的ob_scan域. 调用\ref ob_acquire_scan_req 函数
 *   获取的OB_REQ结构体包含, ob_scan域会指向一个有效OB_SCAN结构体.
 *
 *   每一个OB_SCAN结构体有且只有一个对应的OB_GROUPBY_PARAM结构体,
 *   OB_GROUPBY_PARAM结构体用来构造Scan操作的聚合行为.
 *
 * @param ob_scan 有效的指向OB_SCAN结构体的指针
 * @retval OB_GROUPBY_PARAM* 指向OB_GROUPBY_PARAM结构体的有效指针
 * @retval NULL 出错
 *
 * Example:
 * @include sum_async_demo.c
 */
OB_GROUPBY_PARAM* ob_get_ob_groupby_param(
    OB_SCAN* ob_scan);

/**
 * @brief 给Group By操作添加一个被Group By的列
 *
 *   ob_groupby是通过\ref ob_get_ob_groupby_param 函数获得的指针,
 *   这个指针须有效的指向OB_GROUPBY_PARAM结构体.
 *
 *   column是需要添加的列名.
 *
 *   这个函数指定的列, 等同于在SQL的SELECT语句中GROUP BY后面的列.
 *
 * @param ob_groupby 有效的指向OB_GROUPBY_PARAM结构体的指针
 * @param column 列名, '\\0'结尾字符串
 * @param is_return 这一列是否作为最后结果返回
 * @retval OB_ERR_SUCCESS 成功
 * @retval otherwise 失败
 *
 * Example:
 * @include sum_async_demo.c
 */
OB_ERR_CODE ob_groupby_column(
    OB_GROUPBY_PARAM* ob_groupby,
    const char* column,
    int is_return);

/**
 * @brief 给Group By操作添加一个需要聚合的列
 *
 *   ob_groupby是通过\ref ob_get_ob_groupby_param 函数获得的指针,
 *   这个指针须有效的指向OB_GROUPBY_PARAM结构体.
 *
 *   aggregation指定进行哪一种聚合操作.
 *   目前允许的聚合操作可参考\ref OB_AGGREGATION_TYPE 内定义的常量.
 *
 *   column是需要进行聚合操作的列名.
 *   as_name是为这个进行聚合操作的新列指定列名.
 *
 *   这个函数指定的列, 等同于在SQL的SELECT语句中GROUP BY后面的列.
 *
 * @param ob_groupby 有效的指向OB_GROUPBY_PARAM结构体的指针
 * @param aggregation 聚合操作的类型
 * @param column 列名, '\\0'结尾字符串
 * @param as_name 新列名, '\\0'结尾字符串
 * @param is_return 这一列是否作为最后结果返回
 * @retval OB_ERR_SUCCESS 成功
 * @retval otherwise 失败
 *
 * Example:
 * @include sum_async_demo.c
 */
OB_ERR_CODE ob_aggregate_column(
    OB_GROUPBY_PARAM* ob_groupby,
    const OB_AGGREGATION_TYPE aggregation,
    const char* column,
    const char* as_name,
    int is_return);

/**
 * @brief 给Group By操作添加一个复杂列
 *
 *   ob_groupby是通过\ref ob_get_ob_groupby_param 函数获得的指针,
 *   这个指针须有效的指向OB_GROUPBY_PARAM结构体.
 *
 *   expression是需要添加的复杂列.
 *   复杂列指包含有加、减、乘、除等操作的列, 譬如A + B * C这种列.
 *   对复杂列的要求是, 复杂列的所有运算元素,
 *   例如这里的A, B, C都需要已经通过\ref ob_aggregate_column添加进来.
 *
 *   以SUM(A)/SUM(B)这个操作需求为例:
 *   先通过ob_aggregate_column函数添加SUM(A)操作和SUM(B)操作,
 *   假设分别命名(as_name参数指定的名字) SA和SB.
 *   那么再调用\ref ob_groupby_add_complex_column函数将"SA/SB"加入,
 *   即完成复杂列的添加.
 *
 * @param ob_groupby 有效的指向OB_GROUPBY_PARAM结构体的指针
 * @param expression 复杂列, '\\0'结尾字符串
 * @param as_name 新列名, '\\0'结尾字符串
 * @param is_return 这一列是否作为最后结果返回
 * @retval OB_ERR_SUCCESS 成功
 * @retval otherwise 失败
 *
 * Example:
 * @include sum_async_demo.c
 */
OB_ERR_CODE ob_groupby_add_complex_column(
    OB_GROUPBY_PARAM* ob_groupby,
    const char* expression,
    const char* as_name,
    int is_return);

/**
 * @brief 添加一个既不在GROUP BY中的列也不在聚合中的列
 *
 *   ob_groupby是通过\ref ob_get_ob_groupby_param 函数获得的指针,
 *   这个指针须有效的指向OB_GROUPBY_PARAM结构体.
 *
 *   做GROUP BY操作时，如果一个列既不在GROUP BY中，也不在聚合列中，
 *   进行GROUP BY操作后，这些列的值是不确定的。
 *   OB允许在GROUP BY操作后，返回这些列，列值选择的是对应GROUP中
 *   该列的第一个值。
 *
 * @param ob_groupby 有效的指向OB_GROUPBY_PARAM结构体的指针
 * @param column_name 列名, '\\0'结尾字符串
 * @param is_return 这一列是否作为最后结果返回
 * @retval OB_ERR_SUCCESS 成功
 * @retval otherwise 失败
 *
 * Example:
 * @include sum_async_demo.c
 */
OB_ERR_CODE ob_groupby_add_return_column(
    OB_GROUPBY_PARAM* ob_groupby,
    const char* column_name,
    int is_return);

/**
 * @brief 给Group By操作添加一个过滤条件
 *
 *   ob_groupby是通过\ref ob_get_ob_groupby_param 函数获得的指针,
 *   这个指针须有效的指向OB_GROUPBY_PARAM结构体.
 *
 *   expression过滤条件.
 *
 *   在SQL中，本函数的调用相当于HAVING.
 *
 * @param ob_groupby 有效的指向OB_GROUPBY_PARAM结构体的指针
 * @param expression 过滤条件, '\\0'结尾
 * @retval OB_ERR_SUCCESS 成功
 * @retval otherwise 失败
 *
 * Example:
 * @include sum_async_demo.c
 */
OB_ERR_CODE ob_groupby_set_having(
    OB_GROUPBY_PARAM* ob_groupby,
    const char* expression);

/**
 * @brief 添加一个JOIN操作
 *
 * JOIN列在结果中的列名是“join_table" + "." + "join_column"的形式。
 *
 * @param scan_st 有效的指向OB_SCAN结构体的指针
 * @param res_column 结果集中的列名, '\\0'结尾字符串
 * @param join_table 右侧JOIN的表名, '\\0'结尾字符串
 * @param join_column 右侧JOIN的列名, '\\0'结尾字符串
 * @param as_name JOIN之后在结果中JOIN列的列名, 
 *                如果参数是空字符串则用默认值"join_table.join_column"
 * @retval OB_ERR_SUCCESS 成功
 * @retval otherwise 失败
 *
 * Example:
 * @include join_demo.c
 */
OB_ERR_CODE ob_scan_res_join_append(OB_SCAN* scan_st,
                                    const char* res_column,
                                    const char* join_table,
                                    const char* join_column,
                                    const char* as_name);

/********* OB_SCAN END *********/


/********* OB FUNCTION *********/

/**
 * 初始化OB API
 * @retval valid_pointer 有效的指向OB结构体得的指针
 * @retval NULL 失败
 */
OB* ob_api_init();

/**
 * 释放OB结构体
 * @param ob 有效的指向OB结构体的指针
 * @retval none
 */
void ob_api_destroy(OB* ob);

/**
 * 连接Oceanbase
 * @param ob 有效的指向OB结构体的指针
 * @param rs_addr Rootserver地址
 * @param rs_port Rootserver端口号
 * @param user 用户名
 * @param passwd 密码
 * @retval OB_ERR_SUCCESS 成功
 * @retval otherwise 失败
 */
OB_ERR_CODE ob_connect(
    OB* ob,
    const char* rs_addr, unsigned int rs_port,
    const char* user, const char* passwd);

/**
 * 断开Oceanbase链接, 释放资源
 * @param ob 有效的指向OB结构体的指针
 * @retval OB_ERR_SUCCESS 成功
 * @retval otherwise 失败
 */
OB_ERR_CODE ob_close(OB* ob);

/**
 * Scan请求, 同步调用
 *
 * @param ob 有效的指向OB结构体的指针
 * @param ob_scan Scan请求结构体
 * @retval OB_RES* 有效的指向OB_RES结构体的指针
 * @retval NULL 失败
 */
OB_RES* ob_exec_scan(OB* ob, OB_SCAN* ob_scan);

/**
 * Get请求, 同步调用
 *
 * @param ob 有效的指向OB结构体的指针
 * @param ob_get Get请求结构体
 * @retval OB_RES* 有效的指向OB_RES结构体的指针
 * @retval NULL 失败
 */
OB_RES* ob_exec_get(OB* ob, OB_GET* ob_get);

/**
 * Set请求, 同步调用
 *
 * @param ob 有效的指向OB结构体的指针
 * @param ob_set Set请求结构体
 * @retval OB_ERR_SUCCESS 成功
 * @retval otherwise 失败
 */
OB_ERR_CODE ob_exec_set(OB* ob, OB_SET* ob_set);

/**
 * 提交异步请求
 * @param ob 有效的指向OB结构体的指针
 * @param req 异步请求
 * @retval OB_ERR_SUCCESS 成功
 * @retval otherwise 失败
 */
OB_ERR_CODE ob_submit(OB* ob, OB_REQ* req);

/**
 * 取消异步请求
 * @todo 暂未实现
 * @param ob 有效的指向OB结构体的指针
 * @param req 异步请求
 * @retval OB_ERR_SUCCESS 成功
 * @retval otherwise 失败
 */
OB_ERR_CODE ob_cancel(OB* ob, OB_REQ* req);

/**
 * 获取异步结果
 *
 *   当min_num=0或者timeout=0时, 该函数不会阻塞
 *   当timeout=-1时, 表示等待时间是无限长, 直到满足条件
 *
 * @param ob 有效的指向OB结构体的指针
 * @param min_num 本次获取的最少异步完成事件个数
 * @param max_num 本次获取的最多异步完成事件个数
 * @param timeout 超时(us)
 * @param num 输出参数, 异步返回结果个数
 * @param reqs 输出参数, 异步返回结果数组
 * @retval OB_ERR_SUCCESS 成功
 * @retval otherwise 失败
 */
OB_ERR_CODE ob_get_results(
    OB* ob,
    int min_num,
    int max_num,
    int64_t timeout,
    int64_t* num,
    OB_REQ* reqs[]);

/**
 * 获取get请求的OB_REQ
 * @param ob 有效的指向OB结构体的指针
 * @retval OB_ERR_SUCCESS 成功
 * @retval otherwise 失败
 */
OB_REQ* ob_acquire_get_req(OB* ob);

/**
 * 获取scan请求的OB_REQ
 * @param ob 有效的指向OB结构体的指针
 * @retval OB_ERR_SUCCESS 成功
 * @retval otherwise 失败
 */
OB_REQ* ob_acquire_scan_req(OB* ob);

/**
 * 获取set请求的OB_REQ
 * @param ob 有效的指向OB结构体的指针
 * @retval OB_ERR_SUCCESS 成功
 * @retval otherwise 失败
 */
OB_REQ* ob_acquire_set_req(OB* ob);

/**
 * 释放OB_REQ
 * @param ob 有效的指向OB结构体的指针
 * @param req 有效的指向OB_REQ结构体的指针
 * @param is_free_mem 是否释放OB_REQ结构体里的内存, 如果不释放,
 *                    内存会一直持有, 并在下次使用这个OB_REQ结构体时复用
 */
void ob_release_req(OB* ob, OB_REQ* req, int is_free_mem);

OB_GET* ob_acquire_get_st(OB* ob);
OB_SCAN* ob_acquire_scan_st(OB* ob);
OB_SET* ob_acquire_set_st(OB* ob);
void ob_release_get_st(OB* ob, OB_GET* ob_get);
void ob_release_scan_st(OB* ob, OB_SCAN* ob_scan);
void ob_release_set_st(OB* ob, OB_SET* ob_set);
void ob_reset_get_st(OB* ob, OB_GET* ob_get);
void ob_reset_scan_st(OB* ob, OB_SCAN* ob_scan);
void ob_reset_set_st(OB* ob, OB_SET* ob_set);

/**
 * 释放OB_RES结构体
 * @param ob 有效的指向OB结构体的指针
 * @param res 有效的指向OB_RES结构体的指针
 */
void ob_release_res_st(OB*ob, OB_RES* res);

/**
 * 设置事件通知fd
 * @param req 有效的指向OB_REQ结构体的指针
 * @param fd 有效的fd
 */
void ob_req_set_resfd(OB_REQ* req, int32_t fd);

/**
 * @brief 设置OB API参数
 * 
 *   <b>ob_api_cntl</b>函数用来设置OB API的各种细节参数:
 *     <ul>
 *       <li><b>OB_S_MAX_REQ</b>: 设置最大异步并发量.
 *       OB_ERR_CODE ob_api_cntl(OB* ob, int32_t cmd, int32_t max_req); 
 *       </li>
 *       <li><b>OB_G_MAX_REQ</b>: 获取最大异步并发量.
 *       OB_ERR_CODE ob_api_cntl(OB* ob, int32_t cmd, int32_t* max_req); 
 *       </li>
 *       <li><b>OB_S_TIMEOUT</b>: 设置所有的操作的超时时间.
 *       OB_ERR_CODE ob_api_cntl(OB* ob, int32_t cmd, int32_t timeout); 
 *       </li>
 *       <li><b>OB_S_TIMEOUT_GET</b>: 设置Get操作的超时时间.
 *       OB_ERR_CODE ob_api_cntl(OB* ob, int32_t cmd, int32_t timeout_get); 
 *       </li>
 *       <li><b>OB_G_TIMEOUT_GET</b>: 获取Get操作的超时时间.
 *       OB_ERR_CODE ob_api_cntl(OB* ob, int32_t cmd, int32_t* timeout_get); 
 *       </li>
 *       <li><b>OB_S_TIMEOUT_SCAN</b>: 设置Scan操作的超时时间.
 *       OB_ERR_CODE ob_api_cntl(OB* ob, int32_t cmd, int32_t timeout_scan); 
 *       </li>
 *       <li><b>OB_G_TIMEOUT_SCAN</b>: 获取Scan操作的超时时间.
 *       OB_ERR_CODE ob_api_cntl(OB* ob, int32_t cmd, int32_t* timeout_scan); 
 *       </li>
 *       <li><b>OB_S_TIMEOUT_SET</b>: 设置Set操作的超时时间.
 *       OB_ERR_CODE ob_api_cntl(OB* ob, int32_t cmd, int32_t timeout_set); 
 *       </li>
 *       <li><b>OB_G_TIMEOUT_SET</b>: 获取Set操作的超时时间.
 *       OB_ERR_CODE ob_api_cntl(OB* ob, int32_t cmd, int32_t* timeout_set); 
 *       </li>
 *       <li><b>OB_S_READ_MODE</b>: 设置读取模式：同步（0）or异步（1）
 *       OB_ERR_CODE ob_api_cntl(OB* ob, int32_t cmd, int32_t read_mode); 
 *       </li>
 *       <li><b>OB_G_READ_MODE</b>: 获取读取模式：同步（0）or异步（1）
 *       OB_ERR_CODE ob_api_cntl(OB* ob, int32_t cmd, int32_t* read_mode); 
 *       </li>
 *     </ul>
 */
OB_ERR_CODE ob_api_cntl(OB* ob, int32_t cmd, ...);

/********* OB FUNCTION END *********/

/********* OB API DEBUG *********/

/**
 * @brief 获取OB集群MergeServer地址列表
 *
 * OceanBase集群的MergeServer地址列表
 * 返回结果的格式是OB_RES结构体
 * 结果格式描述如下：
\verbatim
  Row Key         | Port(INT) |
  -----------------------------
  10.232.36.30    | 2600      |
  10.232.36.31    | 2600      |
  10.232.36.32    | 2600      |
row key是Mergeserver地址的字符串表示
Port是Mergeserver的端口号
\endverbatim
 * 使用结束后需要调用\ref ob_release_res_st函数释放
 *
 * @param ob 有效的指向OB结构体的指针
 * @retval OB_RES* 指向OB_RES结构体的指针
 * @retval NULL 出错
 */
OB_RES* ob_get_ms_list(OB* ob);

/**
 * @brief 设置ob_api日志输出信息及日志文件
 *
 *   OceanBase C API在默认情况下是不会打印日志的, 无论是否出错.
 *   <b>ob_api_debug_log</b>可以打开OB API的日志记录.
 *
 *   <ul>
 *     <li><b>log_level</b>指定日志级别, 如果为NULL表示什么都不输出.</li>
 *     <li><b>log_file</b>指定日志输出文件, 如果为NULL表示输出到屏幕,
 *         但是如果曾经调用ob_api_debug_log函数设置过日志输出到某个文件,
 *         再次调用该函数时, log_file参数是NULL代表日志输出文件不变.</li>
 *   </ul>
 *
 * @param ob 有效的指向OB结构体的指针
 * @param log_level 日志级别
 * @param log_file 日志文件
 */
void ob_api_debug_log(OB* ob, const char* log_level, const char* log_file);

/**
 * @brief 用于OceanBase debug的控制命令, 部分命令会危及数据安全性, 需要慎重使用
 *
 * 命令说明：
 *   <ul>
 *     <li><b>OB_DEBUG_CLEAR_ACTIVE_MEMTABLE</b>:
 *     清理OceanBase当前的Active Memtable. <em><b>(非常危险)</b></em>
 *     OB_ERR_CODE ob_debug(OB* ob, int32_t cmd); 
 *     </li>
 *   </ul>
 */
OB_ERR_CODE ob_debug(OB* ob, int32_t cmd, ...);

/********* OB API DEBUG END *********/

/********* OB EXTEND API *********/

/**
 * 设置get请求需读取的数据版本
 * @param ob_get 有效的指向OB_GET结构体的指针
 * @param version 有效的数据版本，为0表示只读取静态数据
 */
OB_ERR_CODE ob_get_set_version(
    OB_GET* ob_get,
    int64_t version);

/********* OB EXTEND API END *********/

#ifdef  __cplusplus
}
#endif

#endif // _OCEANBASE_H__
