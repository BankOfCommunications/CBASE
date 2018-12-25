/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * ob_session_mgr.h for query session manager
 *
 * Authors:
 *   wushi <wushi.ly@taobao.com>
 *
 */
#ifndef COMMON_OB_SESSION_MGR_H_
#define COMMON_OB_SESSION_MGR_H_
#include <sys/types.h>
#include <pthread.h>
#include <sys/syscall.h>
#include <unistd.h>
#include "common/ob_scan_param.h"
#include "common/ob_get_param.h"
#include "common/ob_atomic.h"
#include "tbsys.h"
namespace oceanbase
{
  namespace common
  {

    /// transform peerid to ascii str
    const char *inet_ntoa_r(const uint64_t ipport);
    /// session manager, current we suppose that each session will be only processed by one worker thread
    class ObSessionManager
    {
    public:
      ObSessionManager();
      ~ObSessionManager();

      /// called when a session begin, this will record the session info in the mgr, and will record
      /// session start time in a thread specific variable whose type is SessionInfo, so the info can
      /// be checked by ob administrator
      int session_begin(const oceanbase::common::ObScanParam & scan_param, const uint64_t peer_port,
        uint64_t &session_id, const pthread_t tid, const pid_t pid);
      int session_begin(const oceanbase::common::ObGetParam & get_param, const uint64_t peer_port,
        uint64_t &session_id, const pthread_t tid, const pid_t pid);

      /// record the end time of the given session
      int session_end(const uint64_t  session_id);

      /// kill the given session, this function will be triggered by ob administrator when he/she
      /// finds that some ssessions are using up a lot of resource or have taken a long time.
      /// Caller should pay attention that, this function will not really kill the given session. It
      /// only sets a flag which indicates that the given session should be terminated.
      /// It's the caller's responsibility to check this flag according session_alive, and to act
      /// when decting session should be terminated
      void kill_session(const uint64_t  session_id);

      /// check if session is alive, checking by session worker. When decting the session is not
      /// alive, the worker should take appropriate action, like stopping to process this given session
      bool session_alive(const uint64_t  session_id);

      /// get infos of current sessions, this function will put readable infos of all the sessions
      /// current alive into buf
      /// using this function, ob administrator can see all current alive sessions
      /// <sql statement of current session> <client ip port> <start_time> <time used>
      void get_sessions(char *buf, const int64_t buf_size, int64_t & pos);

      /// get info of given session
      const char * get_session(const uint64_t  session_id);
    private:
      static const uint32_t MAX_SESSION_WORKER_COUNT = 512;
      static const int64_t SESSION_INFO_SIZE = 8192;
      /// static const int64_t SESSION_INFO_SIZE = 1024;
      static const uint64_t INVALID_SESSION_ID = 0;

      typedef union
      {
        struct
        {
          uint64_t session_id_:48;
          uint64_t status_:2;
          /// reserved is for node id
          uint64_t reserved_:14;
        } id_status_;
        uint64_t uval_;
      }SessionIdStatus;
      struct SessionInfo
      {
        SessionInfo()
        {
          session_id_status_.uval_ = INVALID_SESSION_ID;
        }
        void begin(const uint64_t session_id, const pthread_t tid, const pid_t pid)
        {
          SessionIdStatus tmp;
          tmp.uval_ = 0;
          tmp.uval_ |= session_id;
          tmp.id_status_.status_ = TRUE_VAL;
          session_id_status_.uval_ = tmp.uval_;
          start_time_ =  tbsys::CTimeUtil::getTime();
          end_time_ = -1;
          /// worker_tid_ = syscall(SYS_gettid);
          worker_tid_ = static_cast<pid_t>(tid);
          worker_thread_ = pid;
        }

        bool end(const uint64_t session_id)
        {
          bool res = false;
          SessionIdStatus org_id_status;
          org_id_status.uval_ = 0;
          org_id_status.uval_ |= session_id;
          org_id_status.id_status_.status_ = TRUE_VAL;
          SessionIdStatus new_id_status;
          new_id_status.uval_ = 0;
          new_id_status.uval_ |= session_id;
          new_id_status.id_status_.status_ = FALSE_VAL;
          if (atomic_compare_exchange(&(session_id_status_.uval_), new_id_status.uval_, org_id_status.uval_) == org_id_status.uval_)
          {
            end_time_ = tbsys::CTimeUtil::getTime();
            res = true;
          }
          return res;
        }

        bool alive(const uint64_t session_id)const
        {
          return((session_id_status_.id_status_.session_id_ == session_id)
            && (session_id_status_.id_status_.status_));
        }

        bool alive()const
        {
          return(session_id_status_.id_status_.status_);
        }

        uint64_t session_id()const
        {
          return(session_id_status_.id_status_.session_id_);
        }

        uint64_t seq_lock()const
        {
          return session_id_status_.uval_;
        }

        char infos_[SESSION_INFO_SIZE];
        int64_t start_time_;
        int64_t end_time_;
        volatile SessionIdStatus session_id_status_;
        volatile pid_t     worker_tid_;
        volatile pthread_t worker_thread_;
#undef TRUE
#undef FALSE
        static const uint32_t TRUE_VAL = 1;
        static const uint32_t FALSE_VAL = 0;
#define TRUE 1
#define FALSE 0
      };

      static __thread SessionInfo * thread_session_info_;
      SessionInfo session_infos_per_thread_[MAX_SESSION_WORKER_COUNT];
      mutable volatile uint32_t session_worker_count_;
      volatile static uint32_t session_id_generator_;

    private:
      SessionInfo *get_session_info(const uint32_t session_id);
      uint64_t gen_session_id()
      {
        SessionIdStatus id_status;
        id_status.uval_ = atomic_inc(&session_id_generator_);
        return id_status.id_status_.session_id_;
      }
    };
  }
}
#endif /* COMMON_OB_SESSION_MGR_H_ */
