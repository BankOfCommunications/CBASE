/*
 * =====================================================================================
 *
 *       Filename:  db_record_filter.cpp
 *
 *    Description:  
 *
 *        Version:  1.0
 *        Created:  06/15/2011 04:17:45 AM
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  yushun.swh (), yushun.swh@taobao.com
 *        Company:  taobao
 *
 * =====================================================================================
 */
#include "db_record_filter.h"
#include "common/utility.h"
#include "db_utils.h"

using namespace oceanbase::common;

std::string &trim(std::string &str)
{
  str.erase(0, str.find_first_not_of(' '));
  str.erase(str.find_last_not_of(' ') + 1, std::string::npos);
  return str;
}

DbRowFilter *create_filter_from_str(std::string str)
{
  DbRowFilter *filter = NULL;

  std::string column;
  int filter_type;
  std::string type_str;

  trim(str);
  std::string::size_type start_pos = 0;
  std::string::size_type pos = str.find(',');

  if (pos == std::string::npos) {
    TBSYS_LOG(WARN, "filter config error");
    return filter;
  }

  column = str.substr(start_pos, pos);
  trim(column);

  //skip ','
  start_pos = pos + 1;
  pos = str.find(',', start_pos);
  if (pos == std::string::npos) {
    TBSYS_LOG(WARN, "filter config error");
    return filter;
  }
  type_str = str.substr(start_pos, pos - start_pos);
  trim(type_str);
  filter_type = atoi(type_str.c_str());

  switch(filter_type) {
   case DATE_FILTER_TYPE:
   case MODIFY_TIME_FILTER_TYPE:
     {
       std::string min;
       std::string max;

       start_pos = pos + 1;
       pos = str.find(',', start_pos);
       if (pos == std::string::npos) {
         TBSYS_LOG(WARN, "filter config error, skiped");
       }
       min = str.substr(start_pos, pos - start_pos);

       start_pos = pos + 1;
       max = str.substr(start_pos, std::string::npos);

       trim(min);
       trim(max);

       TBSYS_LOG(DEBUG, "TIME [%s]:[%s]", min.c_str(), max.c_str());
       filter = new(std::nothrow) DbDateTimeFilter(column, min, max, filter_type);
     }
     break;
  }

  return filter;
}


bool DbDateTimeFilter::operator()(DbRecord *record) const
{
  ObCellInfo *cell;
  bool skip = false;

  int ret = record->get(column_, &cell);
  if (ret != OB_SUCCESS)
    skip = false;
  else {
    char time_str[MAX_TIME_LEN];
    ObObjType type;
    int64_t value = 0;

    switch(filter_type_) {
     case MODIFY_TIME_FILTER_TYPE:
       ret = cell->value_.get_modifytime(value);
       type = ObModifyTimeType;
       break;
     case DATE_FILTER_TYPE:
       ret = cell->value_.get_datetime(value);
       type = ObDateTimeType;
       break;
     default:
       ret = OB_ERROR;
    }

    if (ret == OB_SUCCESS) {
      ObDateTime2MySQLDate(value, type, time_str, MAX_TIME_LEN);
      TBSYS_LOG(DEBUG, "TIME:[%s]", time_str);
      skip = !((start_time_.compare(time_str) <= 0) && (end_time_.compare(time_str) >= 0));
    }
  }

  return skip;
}
