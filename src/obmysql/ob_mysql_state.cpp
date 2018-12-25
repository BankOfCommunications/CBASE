#include "ob_mysql_state.h"
#include "common/ob_define.h"
#include <tbsys.h>
using namespace oceanbase::common;
using namespace oceanbase::obmysql;

ObMySQLState::StObErrorStringMap ObMySQLState::maps_[OB_MAX_ERROR_CODE] = {};
const ObMySQLState ObMySQLState::instance_;

ObMySQLState::ObMySQLState()
{
#include "ob_mysql_state.map"
}

void ObMySQLState::OB_ADD_SQLSTATE(int oberr, const char *jdbc_state, const char *odbc_state)
{
  if (oberr <= 0)
  {
    maps_[-oberr].jdbc_state = jdbc_state;
    maps_[-oberr].odbc_state = odbc_state;
  }
}

const char *ObMySQLState::get_odbc_state(int oberr) const
{
  const char *state = "S1000";
  const char *state_succ = "00000";

  if (oberr <= 0)
  {
    if (oberr == OB_SUCCESS)
    {
      state = state_succ;
    }
    else if ((-oberr) >= OB_MAX_ERROR_CODE)
    {
      TBSYS_LOG(WARN, "oceanbase error code out of range, err=[%d]", oberr);
    }
    else if (NULL != maps_[-oberr].odbc_state && maps_[-oberr].odbc_state[0] != '\0')
    {
      state = maps_[-oberr].odbc_state;
    }
  }
  return state;
}

const char *ObMySQLState::get_jdbc_state(int oberr) const
{
  const char *state = "HY000";
  const char *state_succ = "00000";

  if (oberr <= 0)
  {
    if (oberr == OB_SUCCESS)
    {
      state = state_succ;
    }
    else if ((-oberr) >= OB_MAX_ERROR_CODE)
    {
      TBSYS_LOG(WARN, "oceanbase error code out of range, err=[%d]", oberr);
    }
    else if (NULL != maps_[-oberr].jdbc_state && maps_[-oberr].jdbc_state[0] != '\0')
    {
      state = maps_[-oberr].jdbc_state;
    }
  }
  return state;
}
