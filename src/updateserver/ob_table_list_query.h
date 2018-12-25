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
#ifndef __OB_UPDATESERVER_OB_TABLE_LIST_QUERY_H__
#define __OB_UPDATESERVER_OB_TABLE_LIST_QUERY_H__
#include "ob_table_mgr.h"
#include "sql/ob_phy_operator.h"
#include "sql/ob_multiple_scan_merge.h"
#include "ob_ups_utils.h"

namespace oceanbase
{
  namespace updateserver
  {
    class IObSingleTableQuery
    {
      public:
        IObSingleTableQuery() {}
        virtual ~IObSingleTableQuery() {}
      public:
        virtual int query(BaseSessionCtx* session_ctx, ITableEntity* table_entity, ITableIterator* iter) = 0;
    };

    class ObSingleTableScanQuery: public IObSingleTableQuery
    {
      public:
        ObSingleTableScanQuery(): scan_param_(NULL) {}
        virtual ~ObSingleTableScanQuery() {}
      public:
        void set(const ObScanParam* scan_param) { scan_param_ = scan_param; }
        int query(BaseSessionCtx* session_ctx, ITableEntity* table_entity, ITableIterator* iter);
      private:
        const ObScanParam* scan_param_;
    };

    class ObSingleTableGetQuery: public IObSingleTableQuery
    {
      public:
        ObSingleTableGetQuery(): get_param_(NULL), start_idx_(0), end_idx_(0), lock_flag_(sql::LF_NONE) {}
        virtual ~ObSingleTableGetQuery() {}
      public:
        void set(const ObGetParam* get_param, const int64_t start_idx, const int64_t end_idx,
                 const sql::ObLockFlag lock_flag)
        {
          get_param_ = get_param;
          start_idx_ = start_idx;
          end_idx_ = end_idx;
          lock_flag_ = lock_flag;
        }
        int query(BaseSessionCtx* session_ctx, ITableEntity* table_entity, ITableIterator* iter);
      private:
        const ObGetParam* get_param_;
        int64_t start_idx_;
        int64_t end_idx_;
        sql::ObLockFlag lock_flag_;
    };

    class ObTableListQuery
    {
      public:
        ObTableListQuery(): max_valid_version_(0), is_final_minor_(false),
                         session_ctx_(NULL), table_mgr_(NULL), guard_(NULL)
        {
          //memset(guard_buf_, 0, sizeof(guard_buf_));
          if (NULL == (merger_ = rp_alloc(sql::ObMultipleScanMerge)))
          {
            TBSYS_LOG(ERROR, "alloc sql::ObMultipleScanMerge from resource_pool fail");
          }
        }
        ~ObTableListQuery()
        {
          close();
          if (NULL != merger_)
          {
            rp_free(merger_);
            merger_ = NULL;
          }
        }
        void reset()
        {
          close();
          max_valid_version_ = 0;
          is_final_minor_ = false;
          session_ctx_ = NULL;
          table_mgr_ = NULL;
          guard_ = NULL;
          table_list_.reset();
          merger_->reset();
        }
      public:
        int open(BaseSessionCtx* session_ctx, ObUpsTableMgr* table_mgr, const ObVersionRange& version_range, uint64_t table_id);
        int close();
        int query(IObSingleTableQuery* table_query, common::ObRowDesc* row_desc, sql::ObPhyOperator*& result);
      protected:
        bool is_inited() const;
      protected:
        uint64_t max_valid_version_;
        bool is_final_minor_;
        BaseSessionCtx* session_ctx_;
        ObUpsTableMgr* table_mgr_;
        TableList table_list_;
        ITableEntity::Guard* guard_;
        char guard_buf_[sizeof(ITableEntity::Guard)];
        sql::ObMultipleScanMerge *merger_;
    };
  }; // end namespace updateserver
}; // end namespace oceanbase

#endif /* __OB_UPDATESERVER_OB_TABLE_LIST_QUERY_H__ */
