#include "ob_session_mgr.h"
#include "ob_define.h"
#include "ob_atomic.h"
using namespace oceanbase;
using namespace oceanbase::common;
__thread ObSessionManager::SessionInfo * oceanbase::common::ObSessionManager::thread_session_info_ = NULL;
volatile uint32_t ObSessionManager::session_id_generator_ = ObSessionManager::INVALID_SESSION_ID;

oceanbase::common::ObSessionManager::ObSessionManager()
{
  session_worker_count_ = 0;
}

oceanbase::common::ObSessionManager::~ObSessionManager()
{
  session_worker_count_ = 0;
}


ObSessionManager::SessionInfo *oceanbase::common::ObSessionManager::get_session_info(const uint32_t session_id)
{
  UNUSED(session_id);
  ObSessionManager::SessionInfo *res = NULL;
  if (NULL == thread_session_info_)
  {
    uint64_t idx = 0;
    if ((idx = atomic_inc(&session_worker_count_)) <= MAX_SESSION_WORKER_COUNT)
    {
      idx --;
      res = &session_infos_per_thread_[idx];
      thread_session_info_ = res;
    }
    else
    {
      TBSYS_LOG(WARN,"too many session workers [MAX_SESSION_WORKER_COUNT:%u,cur_count:%lu]",
        MAX_SESSION_WORKER_COUNT, idx);
    }
  }
  else
  {
    res = thread_session_info_;
  }
  return res;
}

int oceanbase::common::ObSessionManager::session_begin(const ObScanParam & scan_param,
  const uint64_t peer_port,  uint64_t &session_id, const pthread_t tid, const pid_t pid)
{
  int err = OB_SUCCESS;
  SessionInfo * session_info = NULL;
  session_id = gen_session_id();
  session_info = get_session_info(static_cast<uint32_t>(session_id));
  if (NULL == session_info)
  {
    TBSYS_LOG(WARN,"fail to get session info");
    err = OB_ERROR;
  }
  int64_t pos = 0;
  if (OB_SUCCESS == err)
  {
    session_info->begin(session_id, tid, pid);
  }
  int64_t buf_size = sizeof(session_info->infos_);
  if ((OB_SUCCESS == err) && (OB_SUCCESS != (err = scan_param.to_str(session_info->infos_,
    buf_size, pos))))
  {
    TBSYS_LOG(WARN,"fail to get ObScanParam str expression [err:%d]", err);
  }
  if (OB_SUCCESS == err)
  {
    time_t start_time_sec = session_info->start_time_/1000000;
    struct tm t;
    localtime_r(&start_time_sec,&t);
    pos += snprintf(session_info->infos_ + pos,(buf_size - pos > 0)?(buf_size - pos):0,
      "; peer:%s; start_time:%04d/%02d/%02d-%02d:%02d:%02d.%06ld; tid:%lu; pid:%lu; session_id:%lu", inet_ntoa_r(peer_port),
      t.tm_year + 1900,
      t.tm_mon + 1,
      t.tm_mday,
      t.tm_hour,
      t.tm_min,
      t.tm_sec,
      session_info->start_time_%1000000,
      static_cast<uint64_t>(session_info->worker_thread_),
      static_cast<uint64_t>(session_info->worker_tid_),
      session_info->session_id());
  }
  return err;
}

int oceanbase::common::ObSessionManager::session_begin(const ObGetParam & get_param,
  const uint64_t peer_port, uint64_t &session_id, const pthread_t tid, const pid_t pid)
{
  int err = OB_SUCCESS;
  SessionInfo * session_info = NULL;
  session_id = gen_session_id();
  session_info = get_session_info(static_cast<uint32_t>(session_id));
  if (NULL == session_info)
  {
    TBSYS_LOG(WARN,"fail to get session info");
    err = OB_ERROR;
  }
  int64_t pos = 0;
  if (OB_SUCCESS == err)
  {
    session_info->begin(session_id, tid, pid);
  }
  int64_t buf_size = sizeof(session_info->infos_);
  if ((OB_SUCCESS == err) && (pos = get_param.to_string(session_info->infos_, buf_size) < 0))
  {
    TBSYS_LOG(WARN,"fail to get ObScanParam str expression [err:%d]", err);
  }
  if (OB_SUCCESS == err)
  {
    time_t start_time_sec = session_info->start_time_/1000000;
    struct tm t;
    localtime_r(&start_time_sec,&t);
    pos += snprintf(session_info->infos_ + pos,(buf_size - pos > 0)?(buf_size - pos):0,
      "; peer:%s; start_time:%04d/%02d/%02d-%02d:%02d:%02d.%06ld; tid:%lu; pid:%lu; session_id:%lu", inet_ntoa_r(peer_port),
      t.tm_year + 1900,
      t.tm_mon + 1,
      t.tm_mday,
      t.tm_hour,
      t.tm_min,
      t.tm_sec,
      session_info->start_time_%1000000,
      static_cast<uint64_t>(session_info->worker_thread_),
      static_cast<uint64_t>(session_info->worker_tid_),
      session_info->session_id());
  }
  return err;
}




int oceanbase::common::ObSessionManager::session_end(const uint64_t session_id)
{
  int err = OB_SUCCESS;
  SessionInfo * session_info = get_session_info(static_cast<uint32_t>(session_id));
  if (NULL == session_info)
  {
    TBSYS_LOG(WARN,"fail to get session info");
    err = OB_ERROR;
  }
  if (OB_SUCCESS == err)
  {
    if (!session_info->end(session_id))
    {
      TBSYS_LOG(WARN,"fail to end session [session_id:%lu,session_info->session_id:%lu,"
        "session_info->alive:%hhu]", session_id, session_info->session_id(),
        session_info->alive(session_id));
      err = OB_ERROR;
    }
  }
  return err;
}

void oceanbase::common::ObSessionManager::kill_session(const uint64_t session_id)
{
  SessionInfo *session_info = NULL;
  for (uint32_t i = 0; i < MAX_SESSION_WORKER_COUNT; i++)
  {
    session_info = session_infos_per_thread_ + i;
    if (session_id == session_info->session_id())
    {
      if (session_info->end(session_id))
      {
        TBSYS_LOG(INFO, "kill session sucess [session_id:%lu]", session_id);
      }
      break;
    }
  }
}

bool oceanbase::common::ObSessionManager::session_alive(const uint64_t session_id)
{
  int err = OB_SUCCESS;
  bool res = false;
  SessionInfo * session_info = get_session_info(static_cast<uint32_t>(session_id));
  if (NULL == session_info)
  {
    TBSYS_LOG(WARN,"fail to get session info");
    err = OB_ERROR;
  }
  if (OB_SUCCESS == err)
  {
    res = session_info->alive(session_id);
  }
  if (OB_SUCCESS != err)
  {
    res = false;
  }
  return res;
}

void oceanbase::common::ObSessionManager::get_sessions(char *buf, int64_t buf_size, int64_t & pos)
{
  if ((NULL != buf) && (0 < buf_size))
  {
    SessionInfo *session_info = NULL;
    uint64_t session_seq_lock = 0;
    int32_t msg_len = 0;
    int64_t now = tbsys::CTimeUtil::getTime();
    for (uint32_t i = 0; i < MAX_SESSION_WORKER_COUNT; i++)
    {
      session_info = session_infos_per_thread_ + i;
      do
      {
        msg_len = 0;
        session_seq_lock = session_info->seq_lock();
        if (session_info->alive())
        {
          msg_len = snprintf(buf + pos, (buf_size-pos>0)?(buf_size-pos):0, "%s; used_time:%ld\n",
            session_info->infos_, now - session_info->start_time_);
        }
      }while (session_seq_lock != session_info->seq_lock());
      if (msg_len > 0)
      {
        pos += msg_len;
      }
    }
  }
}

const char * oceanbase::common::ObSessionManager::get_session(const uint64_t session_id)
{
  int err = OB_SUCCESS;
  const char * res = NULL;
  SessionInfo * session_info = get_session_info(static_cast<uint32_t>(session_id));
  if (NULL == session_info)
  {
    TBSYS_LOG(WARN,"fail to get session info");
    err = OB_ERROR;
  }
  if (OB_SUCCESS == err)
  {
    if ((session_info->session_id() == session_id) && (session_info->alive()))
    {
      res = session_info->infos_;
    }
  }
  return res;
}
