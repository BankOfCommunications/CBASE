/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_end_trans.h
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#ifndef _OB_END_TRANS_H
#define _OB_END_TRANS_H 1
#include "sql/ob_no_children_phy_operator.h"
#include "mergeserver/ob_ms_rpc_proxy.h"
#include "common/ob_transaction.h"
#include "sql/ob_result_set.h"
#include "sql/ob_sql_session_info.h"
namespace oceanbase
{
  namespace sql
  {
    class ObEndTrans: public ObNoChildrenPhyOperator
    {
      public:
        ObEndTrans();
        virtual ~ObEndTrans(){};
        virtual void reset();
        virtual void reuse();
        void set_rpc_stub(mergeserver::ObMergerRpcProxy* rpc){rpc_ = rpc;}
        void set_trans_param(const common::ObTransID &trans_id, bool is_rollback);

        /// execute the insert statement
        virtual int open();
        virtual int close() {return common::OB_SUCCESS;}
        virtual ObPhyOperatorType get_type() const { return PHY_END_TRANS; }
        virtual int64_t to_string(char* buf, const int64_t buf_len) const;

        virtual int get_next_row(const common::ObRow *&row) {UNUSED(row); return common::OB_NOT_SUPPORTED;}
        virtual int get_row_desc(const common::ObRowDesc *&row_desc) const {UNUSED(row_desc); return common::OB_NOT_SUPPORTED;}
        DECLARE_PHY_OPERATOR_ASSIGN;
      private:
        // types and constants
      private:
        // disallow copy
        ObEndTrans(const ObEndTrans &other);
        ObEndTrans& operator=(const ObEndTrans &other);
        // function members
      private:
        // data members
        mergeserver::ObMergerRpcProxy* rpc_;
        ObEndTransReq req_;
    };

    inline void ObEndTrans::reset()
    {
      rpc_ = NULL;
    }

    inline void ObEndTrans::reuse()
    {
      rpc_ = NULL;
    }

    inline ObEndTrans::ObEndTrans()
      :rpc_(NULL)
    {
    }

    inline void ObEndTrans::set_trans_param(const ObTransID &trans_id, bool is_rollback)
    {
      req_.trans_id_ = trans_id;
      req_.rollback_ = is_rollback;
    }
  } // end namespace sql
} // end namespace oceanbase

#endif /* _OB_END_TRANS_H */
