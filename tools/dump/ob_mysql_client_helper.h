/**
  *
  * author by zhangcd 20150814
  *
  * */

#ifndef OB_MYSQL_CLIENT_HELPER_H
#define OB_MYSQL_CLIENT_HELPER_H

#include <mysql/mysql.h>
#include <tbsys.h>
#include <common/ob_string.h>

using namespace oceanbase::common;

class ObMysqlClientHelper
{
public:
  ObMysqlClientHelper();
  ~ObMysqlClientHelper();
  int connect(const char *host_name, const char *user_name, const char *user_password,
           const char *db_name, unsigned short host_port, unsigned int timeout);
  int execute_dml_sql(const ObString &stmt, int &affected_row);

  bool is_connected();
//private:
  static tbsys::CThreadMutex init_lock_; //add by zhuxh 20160922 [connection mutex] //A mutex is undoubtedly static.
};

#endif // OB_MYSQL_CLIENT_HELPER_H

