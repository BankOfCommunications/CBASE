/**
  *
  * author by zhangcd 20150814
  *
  * */
#include <common/ob_tsi_factory.h>
#include <common/ob_define.h>
#include "ob_mysql_client_helper.h"
#include "ob_import_v2.h"

using namespace oceanbase::common;

tbsys::CThreadMutex ObMysqlClientHelper::init_lock_; //add by zhuxh 20160922 [connection mutex] //Every static member is defined even with default constructor only.

ObMysqlClientHelper::ObMysqlClientHelper()
{

}

ObMysqlClientHelper::~ObMysqlClientHelper()
{
  MYSQL **link = GET_TSI_MULT(MYSQL*, 0);
  if(link != NULL && *link != NULL)
  {
    mysql_close(*link);
    *link = NULL;
  }
}

int ObMysqlClientHelper::connect(const char *host_name, const char *user_name, const char *user_password,
                              const char *db_name, unsigned short host_port, unsigned int timeout)
{
  int ret = OB_SUCCESS;
  MYSQL **link = GET_TSI_MULT(MYSQL*, 0);
  if(link == NULL)
  {
    ret = OB_ERROR;
    TBSYS_LOG(ERROR, "failed to get tsi object from tsi_factory!");
  }

  tbsys::CThreadGuard guard(&ObMysqlClientHelper::init_lock_); //add by zhuxh 20160922 [connection mutex] //A guard cannot be in a inner area otherwise it ends in advance unexpectedly.
  if(OB_SUCCESS == ret)
  {
    if(*link != NULL)
    {
      mysql_close(*link);
      *link = NULL;
    }
    //tbsys::CThreadGuard guard(&ObMysqlClientHelper::init_lock_); //del by zhuxh 20160922 [connection mutex]
    *link = mysql_init(NULL);
  }

  if(OB_SUCCESS == ret)
  {
    mysql_options(*link, MYSQL_OPT_CONNECT_TIMEOUT, &timeout);
    mysql_options(*link, MYSQL_OPT_READ_TIMEOUT, &timeout);
    mysql_options(*link, MYSQL_OPT_WRITE_TIMEOUT, &timeout);
  }

  if(OB_SUCCESS == ret)
  {
    if(!mysql_real_connect(*link, host_name, user_name, user_password, db_name, host_port, NULL, 0))
    {
      ret = OB_ERROR;
      TBSYS_LOG(ERROR, "mysql_real_connect failed! mysql error_code[%d] error_msg[%s]", mysql_errno(*link), mysql_error(*link));
      mysql_close(*link);
      *link = NULL;
    }
  }
  return ret;
}

int ObMysqlClientHelper::execute_dml_sql(const ObString& stmt, int &affected_row)
{
  int ret = OB_SUCCESS;
  MYSQL **link = GET_TSI_MULT(MYSQL*, 0);
  if(link == NULL || *link == NULL)
  {
    ret = OB_ERROR;
    TBSYS_LOG(ERROR, "failed to get MYSQL* from tsi buffer!");
  }

  if(ret != OB_SUCCESS)
  {
  }
  else if(0 != mysql_real_query(*link, stmt.ptr(), stmt.length()))
  {
    TBSYS_LOG(ERROR, "failed to get result while execute mysql_query! err_code[%d] err_msg[%s]", mysql_errno(*link), mysql_error(*link));
    ret = OB_ERROR;
    mysql_close(*link);
    *link = NULL;
  }
  if(ret == OB_SUCCESS)
  {
    affected_row = (int)mysql_affected_rows(*link);
  }
  return ret;
}

bool ObMysqlClientHelper::is_connected()
{
  MYSQL **link = GET_TSI_MULT(MYSQL*, 0);
  if(link == NULL || *link == NULL)
  {
    return false;
  }
  return true;
}
