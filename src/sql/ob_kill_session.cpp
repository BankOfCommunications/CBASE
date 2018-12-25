#include "ob_kill_session.h"

using namespace oceanbase::sql;

ObKillSession::ObKillSession()
  :rpc_(NULL), session_mgr_(NULL), session_id_(OB_INVALID_ID),is_kill_query_(false), is_global_(false)
{

}

ObKillSession::~ObKillSession()
{

}

void ObKillSession::reset()
{
  rpc_ = NULL;
  session_mgr_ = NULL;
  session_id_ = OB_INVALID_ID;
  is_kill_query_ = false;
}

void ObKillSession::reuse()
{
  rpc_ = NULL;
  session_mgr_ = NULL;
  session_id_ = OB_INVALID_ID;
  is_kill_query_ = false;
}

int ObKillSession::open()
{
  int ret = OB_SUCCESS;
  if (NULL == rpc_)
  {
    ret = OB_NOT_INIT;
    TBSYS_LOG(ERROR, "not init, rpc_=%p", rpc_);
  }
  else
  {
    //ip is 0 means kill local session
    int32_t ip = 0;
    int32_t index = 0;
    if (is_global_)
    {
      ip = static_cast<int32_t>(session_id_&0xFFFFFFFF);
      index = static_cast<int32_t>((session_id_>>32)&0xFFFFFFFF);
    }
    else
    {
      index = static_cast<int32_t>(session_id_);
    }

    if (0 == ip)
    {
      //kill local session
      ret = session_mgr_->kill_session(index, is_kill_query_);
    }
    else
    {
      ret = rpc_->kill_session(ip, index, is_kill_query_);
    }
    if (OB_ENTRY_NOT_EXIST == ret || OB_RPC_SEND_ERROR == ret
        || OB_RESPONSE_TIME_OUT == ret)
    {
      ret = OB_ERR_UNKNOWN_SESSION_ID;
    }
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "session kill failed");
    }
  }
  return ret;
}

int64_t ObKillSession::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  databuff_printf(buf, buf_len, pos, "KillSession(is_kill_query=%c, session id=%ld", is_kill_query_?'Y':'N', session_id_);
  return pos;
}
