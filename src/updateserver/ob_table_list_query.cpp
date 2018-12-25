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
#include "common/ob_iterator_adaptor.h"
#include "ob_table_list_query.h"
#include "ob_ups_table_mgr.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;
namespace oceanbase
{
  namespace updateserver
  {
    int ObSingleTableScanQuery::query(BaseSessionCtx* session_ctx, ITableEntity* table_entity, ITableIterator* iter)
    {
      int err = OB_SUCCESS;
      if (NULL == scan_param_)
      {
        err = OB_NOT_INIT;
      }
      else if (NULL == session_ctx || NULL == table_entity || NULL == iter)
      {
        err = OB_INVALID_ARGUMENT;
        TBSYS_LOG(ERROR, "query(session=%p, table_entity=%p, iter=%p):INVALID_ARGUMENT", session_ctx, table_entity, iter);
      }
      else if (OB_SUCCESS != (err = table_entity->scan(*session_ctx, *scan_param_, iter)))
      {
        TBSYS_LOG(WARN, "table entity scan fail ret=%d scan_range=%s", err, scan_range2str(*scan_param_->get_range()));
      }
      return err;
    }

    int ObSingleTableGetQuery::query(BaseSessionCtx* session_ctx, ITableEntity* table_entity, ITableIterator* iter)
    {
      int err = OB_SUCCESS;
      const ObCellInfo* cell = NULL;
      ColumnFilter *column_filter = NULL;
      uint64_t table_id = 0;
      ObRowkey row_key;
      if (NULL == get_param_)
      {
        err = OB_NOT_INIT;
      }
      else if (NULL == session_ctx || NULL == table_entity || NULL == iter)
      {
        err = OB_INVALID_ARGUMENT;
        TBSYS_LOG(ERROR, "query(session=%p, table_entity=%p, iter=%p):INVALID_ARGUMENT", session_ctx, table_entity, iter);
      }
      else if (start_idx_ < 0 || end_idx_ < 0 || start_idx_ > end_idx_)
      {
        err = OB_INVALID_ARGUMENT;
        TBSYS_LOG(ERROR, "query([%ld, %ld]):INVALID_ARGUMENT", start_idx_, end_idx_);
      }
      else if (NULL == (cell = (*get_param_)[start_idx_]))
      {
        TBSYS_LOG(WARN, "cellinfo null pointer idx=%ld", start_idx_);
        err = OB_ERROR;
      }
      else if (NULL == (column_filter = ITableEntity::get_tsi_columnfilter()))
      {
        TBSYS_LOG(WARN, "get tsi columnfilter fail");
        err = OB_ERROR;
      }
      else
      {
        table_id = cell->table_id_;
        row_key = cell->row_key_;
        column_filter->clear();
        const ObGetParam& get_param = *get_param_;
        for (int64_t i = start_idx_; i < end_idx_; ++i)
        {
          if (NULL != get_param[i]) // double check
          {
            column_filter->add_column(get_param[i]->column_id_);
          }
        }
        //add duyr [Delete_Update_Function_isolation] [JHOBv0.1] 20160531:b
        column_filter->set_data_mark_param(get_param.get_data_mark_param());
        //add duyr 20160531:e
        if (OB_SUCCESS != (err = table_entity->get(*session_ctx, table_id, row_key, column_filter, lock_flag_, iter)))
        {
          if (!IS_SQL_ERR(err))
          {
            TBSYS_LOG(WARN, "table entity get fail ret=%d table_id=%lu row_key=[%s] columns=[%s]",
                      err, table_id, to_cstring(row_key), column_filter->log_str());
          }
        }
      }
      return err;
    }

    int acquire_table_func(TableMgr& table_mgr, TableList& table_list, const ObVersionRange& version_range,
                           uint64_t& max_valid_version, bool& is_final_minor, uint64_t table_id)
    {
      int err = OB_SUCCESS;

      if (OB_SUCCESS != (err = table_mgr.acquire_table(version_range, max_valid_version, table_list, is_final_minor, table_id))
          || 0 == table_list.size())
      {
        TBSYS_LOG(WARN, "acquire table fail version_range=%s, err=%d", range2str(version_range), err);
        err = (OB_SUCCESS == err) ? OB_INVALID_START_VERSION : err;
      }
      else
      {
        TableList::iterator iter;
        int64_t index = 0;
        for (iter = table_list.begin(); iter != table_list.end(); iter++, index++)
        {
          ITableEntity *table_entity = *iter;
          if (NULL == table_entity)
          {
            TBSYS_LOG(WARN, "invalid table_entity version_range=%s", range2str(version_range));
            err = OB_ERROR;
            break;
          }
        }
      }
      return err;
    }

    int revert_table_func(TableMgr& table_mgr, TableList& table_list)
    {
      int err = OB_SUCCESS;
      table_mgr.revert_table(table_list);
      table_list.clear();
      return err;
    }

    int ObTableListQuery::open(BaseSessionCtx* session_ctx, ObUpsTableMgr* table_mgr, const ObVersionRange& version_range, uint64_t table_id)
    {
      int err = OB_SUCCESS;
      if (NULL != session_ctx_ || NULL != table_mgr_)
      {
        err = OB_INIT_TWICE;
        TBSYS_LOG(ERROR, "open():open twice.");
      }
      else if (NULL == session_ctx || NULL == table_mgr)
      {
        err = OB_INVALID_ARGUMENT;
      }
      else
      {
        session_ctx_ = session_ctx;
        table_mgr_ = table_mgr;
        guard_ = new(guard_buf_)ITableEntity::Guard(table_mgr_->get_table_mgr()->get_resource_pool());
        if (OB_SUCCESS != (err = acquire_table_func(*table_mgr_->get_table_mgr(), table_list_, version_range,
                                                    max_valid_version_, is_final_minor_, table_id)))
        {
          TBSYS_LOG(ERROR, "acquire table fail, err=%d", err);
        }
        else
        {
          FILL_TRACE_BUF(session_ctx_->get_tlog_buffer(), "version=%s table_num=%ld", range2str(version_range), table_list_.size());
        }
      }
      return err;
    }

    bool ObTableListQuery::is_inited() const
    {
      return NULL != table_mgr_ && NULL != session_ctx_;
    }

    int ObTableListQuery::close()
    {
      int err = OB_SUCCESS;
      if (!is_inited())
      {
        //TBSYS_LOG(WARN, "maybe you don't init");
      }
      else if (OB_SUCCESS != (err = revert_table_func(*table_mgr_->get_table_mgr(), table_list_)))
      {
        TBSYS_LOG(ERROR, "revert_table_func()=>%d", err);
      }
      else
      {
        session_ctx_ = NULL;
        table_mgr_ = NULL;
        if (NULL != guard_)
        {
          guard_->~Guard();
          guard_ = NULL;
        }
      }
      return err;
    }

    int ObTableListQuery::query(IObSingleTableQuery* table_query, ObRowDesc* row_desc, ObPhyOperator*& result)
    {
      int ret = OB_SUCCESS;
      ObPhyOperator *ret_iter = merger_;

      TableList::iterator iter;
      int32_t merge_child_idx = 0;
      int64_t index = 0;
      SSTableID sst_id;
      ITableIterator *prev_table_iter = NULL;
      ObRowIterAdaptor *prev_table_row_iter = NULL;
      TableList& table_list = table_list_;
      if (NULL == table_query || NULL == row_desc)
      {
        ret = OB_INVALID_ARGUMENT;
        TBSYS_LOG(ERROR, "table_query == %p || NULL == %p", table_query, row_desc);
      }
      else if (NULL == ret_iter)
      {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        TBSYS_LOG(WARN, "merger null point, maybe allocate from resource_pool fail");
      }
      guard_->reset();
      merge_child_idx = 0;
      for (iter = table_list.begin(); OB_SUCCESS == ret && iter != table_list.end(); iter++, index++)
      {
        SSTableID sst_id;
        ITableEntity *table_entity = *iter;
        ITableIterator *table_iter = NULL;
        ObRowIterAdaptor *table_row_iter = NULL;
        if (NULL == table_entity)
        {
          TBSYS_LOG(WARN, "invalid table_entity");
          ret = OB_ERROR;
          break;
        }
        sst_id = (NULL == table_entity) ? 0 : table_entity->get_table_item().get_sstable_id();
        FILL_TRACE_BUF(session_ctx_->get_tlog_buffer(), "get table %s", sst_id.log_str());
        if (ITableEntity::SSTABLE == table_entity->get_table_type())
        {
          table_iter = prev_table_iter;
          table_row_iter = prev_table_row_iter;
        }
        if (NULL == table_iter
            && NULL == (table_iter = table_entity->alloc_iterator(table_mgr_->get_table_mgr()->get_resource_pool(), *guard_)))
        {
          TBSYS_LOG(WARN, "alloc table iterator fai index=%ld", index);
          ret = OB_MEM_OVERFLOW;
          break;
        }
        if (NULL == table_row_iter
            && NULL == (table_row_iter = table_entity->alloc_row_iter_adaptor(table_mgr_->get_table_mgr()->get_resource_pool(), *guard_)))
        {
          TBSYS_LOG(WARN, "alloc table row iterator fai index=%ld", index);
          ret = OB_MEM_OVERFLOW;
          break;
        }
        prev_table_iter = NULL;
        prev_table_row_iter = NULL;
        if (OB_SUCCESS != (ret = table_query->query(session_ctx_, table_entity, table_iter)))
        {
          if (!IS_SQL_ERR(ret))
          {
            TBSYS_LOG(WARN, "table entity query fail ret=%d", ret);
          }
          break;
        }
        sst_id = (NULL == table_entity) ? 0 : table_entity->get_table_item().get_sstable_id();
        FILL_TRACE_BUF(session_ctx_->get_tlog_buffer(), "scan table type=%d %s ret=%d iter=%p", table_entity->get_table_type(), sst_id.log_str(), ret, table_iter);
        table_row_iter->set_cell_iter(table_iter, *row_desc, false);
        if (ITableEntity::SSTABLE == table_entity->get_table_type())
        {
          if (OB_ITER_END == table_iter->next_cell())
          {
            table_iter->reset();
            prev_table_iter = table_iter;
            prev_table_row_iter = table_row_iter;
            continue;
          }
        }
        if (1 == table_list.size())
        {
          ret_iter = table_row_iter;
          break;
        }
        if (OB_SUCCESS != (ret = merger_->set_child(merge_child_idx++, *table_row_iter)))
        {
          TBSYS_LOG(WARN, "add iterator to merger fail ret=%d", ret);
          break;
        }
      }
      if (OB_SUCCESS == ret)
      {
        result = ret_iter;
      }
      return ret;
    }
  }; // end namespace updateserver
}; // end namespace oceanbase
