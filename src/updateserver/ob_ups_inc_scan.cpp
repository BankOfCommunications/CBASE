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
#include "common/ob_new_scanner_helper.h"
#include "ob_ups_inc_scan.h"
#include "ob_update_server_main.h"

using namespace oceanbase::sql;

namespace oceanbase{
  namespace updateserver{
    REGISTER_CREATOR(oceanbase::sql::ObPhyOperatorGFactory, oceanbase::sql::ObPhyOperator, ObIncGetIter, oceanbase::sql::PHY_INC_GET_ITER);
  }
}

namespace oceanbase
{
  namespace updateserver
  {
    ObUpsTableMgr* get_global_table_mgr()
    {
      ObUpdateServerMain *ups_main = ObUpdateServerMain::get_instance();
      return ups_main? &ups_main->get_update_server().get_table_mgr(): NULL;
    }

    int ObRowDescPrepare::set_rowkey_size(ObUpsTableMgr* table_mgr, ObRowDesc* row_desc)
    {
      int err = OB_SUCCESS;
      uint64_t table_id = OB_INVALID_ID;
      uint64_t column_id = OB_INVALID_ID;
      const ObRowkeyInfo *rki = NULL;
      if (NULL == table_mgr || NULL == row_desc)
      {
        err = OB_INVALID_ARGUMENT;
      }
      else if (OB_SUCCESS != (err  = row_desc->get_tid_cid(0, table_id, column_id)))

      {
        TBSYS_LOG(WARN, "row_desc_.get_tid_cid(0)=>%d", err);
      }
      else if (NULL == (rki = get_rowkey_info(*table_mgr, table_id)))
      {
        TBSYS_LOG(WARN, "get rowkey info fail, table_id=%lu", table_id);
        err = OB_SCHEMA_ERROR;
      }
      else
      {
        row_desc->set_rowkey_cell_count(rki->get_size());
      }
      return err;
    }

    int ObIncGetIter::open(BaseSessionCtx* session_ctx, ObUpsTableMgr* table_mgr,
                           const ObGetParam* get_param, const sql::ObLockFlag lock_flag,
                           ObPhyOperator*& result)
    {
      int err = OB_SUCCESS;
      row_desc_.reset();
      if (NULL == session_ctx || NULL == table_mgr || NULL == get_param)
      {
        err = OB_INVALID_ARGUMENT;
        TBSYS_LOG(WARN, "open(session_ctx=%p, table_mgr=%p, get_param=%p):INVALID_ARGUMENT",
                  session_ctx, table_mgr, get_param);
      }
      else if (OB_SUCCESS != (err = ObTableListQuery::open(session_ctx, table_mgr,
                                                           get_param->get_version_range(), get_table_id(*get_param))))
      {
        TBSYS_LOG(WARN, "open(%s)=>%d", to_cstring(*get_param), err);
      }
      else
      {
        lock_flag_ = lock_flag;
        get_param_ = get_param;
        last_cell_idx_ = 0;
        result = this;
      }
      if (OB_SUCCESS != err)
      {}
      else if (OB_SUCCESS != (err = ObNewScannerHelper::get_row_desc(*get_param, true, row_desc_)))
      {
        if (OB_DUPLICATE_COLUMN == err)
        {
          TBSYS_LOG(USER_ERROR, "Duplicate entry for key \'PRIMARY\'");
          err = OB_ERR_PRIMARY_KEY_DUPLICATE;
        }
        else
        {
          TBSYS_LOG(WARN, "get_row_desc()=>%d", err);
        }
      }
      else if (OB_SUCCESS != (err = set_rowkey_size(table_mgr, &row_desc_)))
      {
        TBSYS_LOG(WARN, "set_rowkey_size()=>%d", err);
      }
      return err;
    }

    int ObIncGetIter::close()
    {
      int err = OB_SUCCESS;
      if (NULL != get_param_)
      {
        if (NULL != result_ && OB_SUCCESS != (err = result_->close()))
        {
          TBSYS_LOG(WARN, "result->close()=>%d", err);
        }
        if (OB_SUCCESS != (err = ObTableListQuery::close()))
        {
          TBSYS_LOG(WARN, "close()=>%d", err);
        }
        lock_flag_ = sql::LF_NONE;
        get_param_ = NULL;
        result_ = NULL;
        last_cell_idx_ = 0;
      }
      return err;
    }

    int get_last_cell_idx_of_row(const ObGetParam& get_param, const int64_t start_idx, int64_t& end_idx)
    {
      int err = OB_SUCCESS;
      int64_t cur_idx = start_idx;
      const ObCellInfo* first_cell = NULL;
      const ObCellInfo* cell = NULL;
      if (cur_idx >= get_param.get_cell_size())
      {
        err = OB_ITER_END;
      }
      else if (NULL == (first_cell = get_param[cur_idx]))
      {
        err = OB_ERR_UNEXPECTED;
        TBSYS_LOG(WARN, "get_param[%ld]=>%d", cur_idx, err);
      }
      for(; OB_SUCCESS == err && cur_idx < get_param.get_cell_size(); cur_idx++)
      {
        if (NULL == (cell = get_param[cur_idx]))
        {
          err = OB_ERR_UNEXPECTED;
          TBSYS_LOG(WARN, "cellinfo null pointer idx=%ld", cur_idx);
        }
        else if (cell->row_key_ != first_cell->row_key_ ||
                 cell->table_id_ != first_cell->table_id_)
        {
          break;
        }
      }
      if (OB_SUCCESS == err)
      {
        end_idx = cur_idx;
      }
      return err;
    }

    int ObIncGetIter::get_next_row(const common::ObRow *&row)
    {
      int err = OB_SUCCESS;
      int64_t end_cell_idx = 0;
      if (!is_inited())
      {
        err = OB_NOT_INIT;
        TBSYS_LOG(WARN, "table_mgr == NULL || sessio_ctx == NULL, maybe you don't init");
      }
      else if (OB_SUCCESS != (err = get_last_cell_idx_of_row(*get_param_, last_cell_idx_, end_cell_idx)))
      {
        if (OB_ITER_END != err)
        {
          TBSYS_LOG(WARN, "get_row(last_cell_idx=%ld)=>%d", last_cell_idx_, err);
        }
      }
      else
      {
        get_query_.set(get_param_, last_cell_idx_, end_cell_idx, lock_flag_);
      }
      if (OB_SUCCESS != err)
      {}
      else if (NULL != result_ && OB_SUCCESS != (err = result_->close()))
      {
        TBSYS_LOG(WARN, "result->close()=>%d", err);
      }
      else if (OB_SUCCESS != (err = query(&get_query_, &row_desc_, result_)))
      {
        if (!IS_SQL_ERR(err))
        {
          TBSYS_LOG(WARN, "query()=>%d", err);
        }
      }
      else if (OB_SUCCESS != (err = result_->open()))
      {
        TBSYS_LOG(WARN, "result->open()=>%d", err);
      }
      else if (OB_SUCCESS != (err = result_->get_next_row(row)))
      {
        TBSYS_LOG(WARN, "result->get_next_row()=>%d", err);
      }
      else
      {
        //add lbzhong [Update rowkey] 20160420:b
        const ObCellInfo* cell = NULL;
        for (int64_t i = last_cell_idx_, index = 0; i < end_cell_idx; ++i, index++)
        {
          cell = (*get_param_)[i];
          if (NULL != cell)
          {
            if(cell->is_new_row_)
            {
              (const_cast<ObRow *&>(row))->set_is_new_row(true);
              break;
            }
          }
        }
        //add:e
        last_cell_idx_ = end_cell_idx;
      }

      //add duyr [Delete_Update_Function_isolation] [JHOBv0.1] 20160531:b
      if (NULL != row && NULL != get_param_ && get_param_->get_data_mark_param().is_valid())
      {
          TBSYS_LOG(DEBUG,"mul_del::debug,ups inc row=[%s],param=[%s],ret=%d",
                    to_cstring(*row),to_cstring(get_param_->get_data_mark_param()),err);
      }
      //add duyr 20160531:e

      return err;
    }

    int ObIncGetIter::get_row_desc(const common::ObRowDesc *&row_desc) const
    {
      int err = OB_SUCCESS;
      if (NULL == get_param_)
      {
        err = OB_NOT_INIT;
        TBSYS_LOG(WARN, "get_param == NULL");
      }
      else
      {
        row_desc = &row_desc_;
      }
      return err;
    }

    int ObIncScanIter::open(BaseSessionCtx* session_ctx, ObUpsTableMgr* table_mgr,
                            const ObScanParam* scan_param, ObPhyOperator*& result)
    {
      int err = OB_SUCCESS;
      row_desc_.reset();
      if (NULL == session_ctx || NULL == table_mgr || NULL == scan_param)
      {
        err = OB_INVALID_ARGUMENT;
        TBSYS_LOG(WARN, "open(session_ctx=%p, table_mgr=%p, scan_param=%p): INVALID_ARGUMENT",
                  session_ctx, table_mgr, scan_param);
      }
      else if (OB_SUCCESS != (err = ObTableListQuery::open(session_ctx, table_mgr, scan_param->get_version_range(), get_table_id(*scan_param))))
      {
        TBSYS_LOG(WARN, "open(%s)=>%d", to_cstring(*scan_param), err);
      }
      else
      {
        scan_query_.set(scan_param);
      }
      if (OB_SUCCESS != err)
      {}
      else if (OB_SUCCESS != (err = ObNewScannerHelper::get_row_desc(*scan_param, row_desc_)))
      {
        TBSYS_LOG(WARN, "get_row_desc()=>%d", err);
      }
      else if (OB_SUCCESS != (err = set_rowkey_size(table_mgr, &row_desc_)))
      {
        TBSYS_LOG(WARN, "set_rowkey_size()=>%d", err);
      }
      else if (OB_SUCCESS != (err = query(&scan_query_, &row_desc_, result)))
      {
        TBSYS_LOG(WARN, "query()=>%d", err);
      }
      return err;
    }

    ObUpsTableMgr* ObUpsIncScan::get_table_mgr()
    {
      return get_global_table_mgr();
    }

    int ObUpsIncScan::open()
    {
      int err = OB_SUCCESS;
      ObUpsTableMgr* table_mgr = NULL;

      if (NULL == (table_mgr = get_table_mgr()))
      {
        err = OB_ERR_UNEXPECTED;
        TBSYS_LOG(WARN, "get_table_mgr()=>NULL");
      }
      else if (ObIncScan::ST_SCAN == scan_type_)
      {
        if (OB_SUCCESS != (err = scan_iter_.open(session_ctx_, table_mgr, scan_param_, result_)))
        {
          TBSYS_LOG(WARN, "scan_iter_.open()=>%d", err);
        }
      }
      else if (ObIncScan::ST_MGET == scan_type_)
      {
        if (OB_SUCCESS != (err = get_iter_.open(session_ctx_, table_mgr, get_param_, lock_flag_, result_)))
        {
          TBSYS_LOG(WARN, "get_iter_.open()=>%d", err);
        }
      }
      else
      {
        err = OB_NOT_INIT;
        TBSYS_LOG(WARN, "open(): result == NULL");
      }
      if (OB_SUCCESS == err)
      {
        if (OB_SUCCESS != (err = result_->open()))
        {
          TBSYS_LOG(WARN, "result->open()=>%d", err);
        }
      }
      return err;
    }

    int ObUpsIncScan::close()
    {
      int err = OB_SUCCESS;
      if (NULL == result_)
      {
        //TBSYS_LOG(WARN, "result == NULL, no need to close");
      }
      else if (OB_SUCCESS != (err = result_->close()))
      {
        TBSYS_LOG(WARN, "result->close()=>%d", err);
      }
      else
      {
        if (ObIncScan::ST_SCAN == scan_type_
            && OB_SUCCESS != (err = scan_iter_.close()))
        {
          TBSYS_LOG(WARN, "scan_iter.close()=>%d", err);
        }
        if (ObIncScan::ST_MGET == scan_type_
            && OB_SUCCESS != (err = get_iter_.close()))
        {
          TBSYS_LOG(WARN, "get_iter.close()=>%d", err);
        }
        result_ = NULL;
      }
      return err;
    }

    int64_t ObUpsIncScan::to_string(char* buf, const int64_t buf_len) const
    {
      int64_t pos = 0;
      pos = sql::ObIncScan::to_string(buf, buf_len);
      databuff_printf(buf, buf_len, pos, "UpsIncScan(session=%p[%d:%ld])\n",
                      session_ctx_, session_ctx_->get_session_descriptor(), session_ctx_->get_session_start_time());
      return pos;
    }

    int ObUpsIncScan::get_next_row(const ObRow *&row)
    {
      int err = OB_SUCCESS;
      if (NULL == result_)
      {
        err = OB_NOT_INIT;
        TBSYS_LOG(WARN, "result == NULL");
      }
      else if (OB_SUCCESS != (err = result_->get_next_row(row))
               && OB_ITER_END != err)
      {
        if (!IS_SQL_ERR(err))
        {
          TBSYS_LOG(WARN, "result->get_next_row()=>%d", err);
        }
      }
      return err;
    }

    int ObUpsIncScan::get_row_desc(const common::ObRowDesc *&row_desc) const
    {
      int err = OB_SUCCESS;
      if (NULL == result_)
      {
        err = OB_NOT_INIT;
        TBSYS_LOG(WARN, "result == NULL");
      }
      else if (OB_SUCCESS != (err = result_->get_row_desc(row_desc)))
      {
        TBSYS_LOG(WARN, "result->get_row_desc()=>%d", err);
      }
      return err;
    }
  }; // end namespace updateserver
}; // end namespace oceanbase
