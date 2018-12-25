/**
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * Authors:
 *   yuanqi <yuanqi.xhf@taobao.com>
 *     - some work details if you want
 */
#ifndef __OB_UPDATESERVER_OB_SESSION_GUARD_H__
#define __OB_UPDATESERVER_OB_SESSION_GUARD_H__

#include "ob_sessionctx_factory.h"
#include "ob_update_server_main.h"

namespace oceanbase
{
  namespace updateserver
  {
#define UPS ObUpdateServerMain::get_instance()->get_update_server()
    class SessionGuard
    {
      public:
        SessionGuard(SessionMgr& session_mgr, LockMgr& lock_mgr, int& ret):
          ret_(ret),
          session_descriptor_(INVALID_SESSION_DESCRIPTOR), session_ctx_(NULL),
          session_mgr_(session_mgr), lock_mgr_(lock_mgr) {}
        ~SessionGuard()
        {
          revert();
        }
        void revert()
        {
          if (NULL != session_ctx_)
          {
            if (OB_SUCCESS == ret_)
            {
              session_ctx_->set_last_active_time(tbsys::CTimeUtil::getTime());
            }
            session_ctx_->unlock_session();
            session_mgr_.revert_ctx(session_descriptor_);
            if (OB_SUCCESS != ret_)
            {
              bool rollback = true;
              session_mgr_.end_session(session_descriptor_, rollback);
            }
            session_ctx_ = NULL;
            session_descriptor_ = INVALID_SESSION_DESCRIPTOR;
          }
        }

        static SessionType get_session_type(TransType type)
        {
          SessionType session_type = ST_READ_ONLY;
          switch(type)
          {
            case READ_ONLY_TRANS:
              session_type = ST_READ_ONLY;
              break;
            case READ_WRITE_TRANS:
              session_type = ST_READ_WRITE;
              break;
            case INTERNAL_WRITE_TRANS:
            case REPLAY_TRANS:
              session_type = ST_REPLAY;
              break;
            default:
              TBSYS_LOG(ERROR, "unknown trans_type=%d", type);
              break;
          }
          return session_type;
        }

        template<typename SessionT>
        int start_session(const ObTransReq& req, ObTransID& sid, SessionT*& ret_ctx)
        {
          int ret = OB_SUCCESS;
          static TransSessionCallback trans_cb;
          if (sid.is_valid() || NULL != session_ctx_)
          {
            ret = OB_INIT_TWICE;
            TBSYS_LOG(ERROR, "sid[%s] is valid or session_ctx[%p] != NULL", to_cstring(sid), session_ctx_);
          }
          else if (OB_SUCCESS != (ret = session_mgr_.begin_session(get_session_type((TransType)req.type_),
                                                                   req.start_time_,
                                                                   req.timeout_,
                                                                   req.idle_time_,
                                                                   session_descriptor_)))
          {
            if (OB_BEGIN_TRANS_LOCKED != ret
                || REACH_TIME_INTERVAL(100 * 1000))
            {
              TBSYS_LOG(WARN, "begin session fail ret=%d", ret);
            }
          }
          else if (NULL == (session_ctx_ = session_mgr_.fetch_ctx(session_descriptor_)))
          {
            TBSYS_LOG(WARN, "fetch ctx fail session_descriptor=%u", session_descriptor_);
            ret = OB_TRANS_ROLLBACKED;
          }
          else if (session_ctx_->is_session_expired())
          {
            ret = OB_TRANS_ROLLBACKED;
          }
          else if (OB_SUCCESS != (ret = session_ctx_->init_lock_info(lock_mgr_, (IsolationLevel)req.isolation_)))
          {
            TBSYS_LOG(ERROR, "init_lock_info fail ret=%d %s", ret, to_cstring(sid));
          }
          else if (READ_ONLY_TRANS != req.type_ && INTERNAL_WRITE_TRANS != req.type_ && OB_SUCCESS != (ret = session_ctx_->add_free_callback(UPS.get_trigger_handler().get_trigger_callback(), NULL)))
          {
            TBSYS_LOG(ERROR, "add_publish_callback fail ret=%d %s", ret, to_cstring(*session_ctx_));
          }
          else if (READ_ONLY_TRANS != req.type_ && OB_SUCCESS != (ret = session_ctx_->add_publish_callback(&trans_cb, &((RWSessionCtx*)session_ctx_)->get_uc_info())))
          {
            TBSYS_LOG(ERROR, "add_publish_callback(%s)=>%d", to_cstring(*session_ctx_), ret);
          }
          else if (NULL == (ret_ctx = dynamic_cast<SessionT*>(session_ctx_)))
          {
            ret = OB_TRANS_NOT_MATCH;
            TBSYS_LOG(USER_ERROR, "session=%p type=%d is not read-write session", session_ctx_, session_ctx_->get_type());
          }
          else
          {
            sid.descriptor_ = session_descriptor_;
            sid.start_time_us_ = session_ctx_->get_session_start_time();
            sid.ups_ = UPS.get_self();
            session_ctx_->lock_session();
          }
          return ret;
        }

        template<typename SessionT>
        int fetch_session(const ObTransID& sid, SessionT*& session_ctx, const bool check_session_expired = false)
        {
          int ret = OB_SUCCESS;
          if (!sid.is_valid())
          {
            ret = OB_INVALID_ARGUMENT;
            TBSYS_LOG(WARN, "sid[%s] is invalid", to_cstring(sid));
          }
          else if (NULL != session_ctx_)
          {
            ret = OB_INIT_TWICE;
            TBSYS_LOG(ERROR, "session_ctx_[%p] != NULL", session_ctx_);
          }
          else if (NULL == (session_ctx = session_mgr_.fetch_ctx<SessionT>(sid.descriptor_)))
          {
            TBSYS_LOG(WARN, "fetch ctx fail session_descriptor=%u", sid.descriptor_);
            ret = OB_TRANS_ROLLBACKED;
          }
          else if (check_session_expired
                  && session_ctx->is_session_expired())
          {
            session_mgr_.revert_ctx(sid.descriptor_);
            session_ctx = NULL;
            ret = OB_TRANS_ROLLBACKED;
          }
          else if (session_ctx->get_session_descriptor() != sid.descriptor_
                   || session_ctx->get_session_start_time() != sid.start_time_us_
                   || !(UPS.get_self() == sid.ups_))
          {
            TBSYS_LOG(WARN, "session not match: id=%u, start_time=%ld, ups=%s",
                      sid.descriptor_, session_ctx->get_session_start_time(), to_cstring(sid.ups_));
            session_mgr_.revert_ctx(sid.descriptor_);
            session_ctx = NULL;
            ret = OB_TRANS_NOT_MATCH;
          }
          else
          {
            session_descriptor_ = sid.descriptor_;
            session_ctx_ = session_ctx;
            session_ctx_->lock_session();
          }
          return ret;
        }
      private:
        int& ret_;
        uint32_t session_descriptor_;
        BaseSessionCtx* session_ctx_;
        SessionMgr& session_mgr_;
        LockMgr& lock_mgr_;
    };
#undef UPS
  }; // end namespace updateserver
}; // end namespace oceanbase
#endif /* __OB_UPDATESERVER_OB_SESSION_GUARD_H__ */
