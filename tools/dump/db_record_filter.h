/*
 * =====================================================================================
 *
 *       Filename:  db_record_filter.h
 *
 *    Description:  
 *
 *        Version:  1.0
 *        Created:  06/12/2011 06:04:58 AM
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  yushun.swh (), yushun.swh@taobao.com
 *        Company:  taobao
 *
 * =====================================================================================
 */


#ifndef  DB_RECORD_FILTER_INC
#define  DB_RECORD_FILTER_INC
#include <string>
#include "db_record.h"

using namespace oceanbase::api;

class DbRowFilter {
  public:
  virtual ~DbRowFilter() { };
  //filter row
  virtual bool operator() (DbRecord *row) const = 0;
};

enum FilterType {
  DATE_FILTER_TYPE,
  MODIFY_TIME_FILTER_TYPE,
  MAX_FILTER_TYPE
};

DbRowFilter *create_filter_from_str(std::string str);

class DbDateTimeFilter : public DbRowFilter {
  public: 
    DbDateTimeFilter(std::string column, std::string start_time, 
                     std::string end_time, int tp)
      : column_(column), start_time_(start_time), 
      end_time_(end_time), filter_type_(tp) {  }

    virtual bool operator() (DbRecord *row) const;

    std::string &column() { return column_; }
    std::string &start_time() { return start_time_; }
    std::string &end_time() { return end_time_; }
  private:
    std::string column_;
    std::string start_time_;
    std::string end_time_;
    int filter_type_;
};

#endif   /* ----- #ifndef db_record_filter_INC  ----- */
