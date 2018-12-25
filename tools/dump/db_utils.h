/*
 * =====================================================================================
 *
 *       Filename:  db_utils.h
 *
 *    Description:  
 *
 *        Version:  1.0
 *        Created:  06/16/2011 12:19:55 AM
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  yushun.swh (), yushun.swh@taobao.com
 *        Company:  taobao
 *
 * =====================================================================================
 */

#ifndef  DB_UTILS_INC
#define  DB_UTILS_INC
#include <time.h>
#include "common/ob_object.h"
#include "common/utility.h"
#include "common/data_buffer.h"
#include "common/ob_common_param.h"
#include "common/ob_action_flag.h"
#include "common/ob_obj_type.h" //add by zhuxh:20150916

#define MAX_TIME_LEN 32

inline int get_current_date(char *p ,size_t size)
{
  time_t now = time(NULL);
  struct tm tm_now;
  localtime_r(&now, &tm_now);
  return static_cast<int32_t>(strftime(p, size, "%Y-%m-%d", &tm_now));
}

inline int64_t gen_usec()
{
  timeval tv;
  gettimeofday(&tv, NULL);
  return (tv.tv_sec * 1000 * 1000 + tv.tv_usec);
}


int is_decimal(const char *str, const int &len);//add by liyongfeng 20140818

//add by liyongfeng
int transform_str_to_int(const char *str, const int &len, int64_t &value);
int transform_str_to_int(const char *str, const int &len, int64_t &value, bool is_decimal);
//add:e

int transform_date_to_time(const char *str, int len, oceanbase::common::ObPreciseDateTime &t);
int how_many_days(int y, int m);//add by zhuxh:20150826
int transform_date_to_time(oceanbase::common::ObObjType tp, const char *str, int len, char timestamp[]); //add by zhuxh 20150715

int ObDateTime2MySQLDate(int64_t ob_time, int time_type, char *outp, int size);

int serialize_cell(oceanbase::common::ObCellInfo *cell, oceanbase::common::ObDataBuffer &buff);

int append_header_delima(oceanbase::common::ObDataBuffer &buff);

int append_delima(oceanbase::common::ObDataBuffer &buff);

int append_end_rec(oceanbase::common::ObDataBuffer &buff);

const char *get_op_string(int action);

int db_utils_init();

void encode_int32(char* buf, uint32_t value);

int32_t decode_int32(const char *buf);

int append_obj(const oceanbase::common::ObObj &obj, oceanbase::common::ObDataBuffer &buff);
#endif   /* ----- #ifndef db_utils_INC  ----- */

