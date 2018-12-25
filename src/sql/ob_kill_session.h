/*
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * ob_kill_session.h is for what ...
 *
 * Version: ***: ob_kill_session.h  Thu May  9 17:40:06 2013 fangji.hcm Exp $
 *
 * Authors:
 *   Author fangji
 *   Email: fangji.hcm@alipay.com
 *     -some work detail if you want
 *
 */
#ifndef _OB_KILL_SESSION_H_
#define _OB_KILL_SESSION_H_
#include "common/ob_string.h"
#include "ob_no_children_phy_operator.h"
#include "mergeserver/ob_ms_rpc_proxy.h"
#include "ob_sql_session_mgr.h"
namespace oceanbase
{
  namespace sql
  {
    class ObKillSession: public ObNoChildrenPhyOperator
    {
      public:
        ObKillSession();
        virtual ~ObKillSession();
        virtual void reset();
        virtual void reuse();

        void set_rpc_stub();
        //
        virtual int open();
        virtual int close();
        virtual int64_t to_string(char* buf, const int64_t buf_len) const;
        virtual ObPhyOperatorType get_type() const { return PHY_KILL_SESSION; }

        virtual int get_next_row(const common::ObRow *&row);
        virtual int get_row_desc(const common::ObRowDesc *&row_desc) const;

        void set_session_id(const int64_t session_id)
        {
          session_id_ = session_id;
        }

        int64_t get_session_id() const
        {
          return session_id_;
        }

        void set_is_query(bool is_query)
        {
          is_kill_query_ = is_query;
        }

        bool get_is_query() const
        {
          return is_kill_query_;
        }

        void set_is_global(bool is_global)
        {
          is_global_ = is_global;
        }

        bool get_is_global() const
        {
          return is_global_;
        }

        void set_rpc_stub(mergeserver::ObMergerRpcProxy* rpc)
        {
          rpc_ = rpc;
        }

        void set_session_mgr(ObSQLSessionMgr *mgr)
        {
          session_mgr_ = mgr;
        }
      private:
        const char* get_session_info() const;
        DISALLOW_COPY_AND_ASSIGN(ObKillSession);

      private:
        mergeserver::ObMergerRpcProxy *rpc_; /* kill remote session */
        ObSQLSessionMgr *session_mgr_;       /* kill local session */
        int64_t session_id_;    /* real session id(32bit) + server ip(32bit) using defualt merge server port */
        bool is_kill_query_;
        bool is_global_;        /* is_global_ equal true session_id_ = real session id(32bits) + server ip(32bits)
                                   is_global_ equal false session_id_ = real session id just kill local session/query */
    };

    inline int ObKillSession::get_next_row(const common::ObRow *&row)
    {
      row = NULL;
      return common::OB_ITER_END;
    }

    inline int ObKillSession::get_row_desc(const common::ObRowDesc *&row_desc) const
    {
      row_desc = NULL;
      return common::OB_NOT_SUPPORTED;
    }

    inline int ObKillSession::close()
    {
      return OB_SUCCESS;
    }
  }
}
#endif
