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
#include "ob_ups_lock_filter.h"
#include "ob_schema_mgrv2.h"
#include "ob_ups_utils.h"

namespace oceanbase
{
  namespace updateserver
  {
    int ObUpsLockFilter::open()
    {
      int err = OB_SUCCESS;
      if (NULL == child_op_)
      {
        err = OB_NOT_INIT;
        TBSYS_LOG(ERROR, "child_op == NULL");
      }
      else if (OB_SUCCESS != (err = child_op_->open()))
      {
        TBSYS_LOG(ERROR, "child_op->open()=>%d", err);
      }
      return err;
    }

    int ObUpsLockFilter::close()
    {
      int err = OB_SUCCESS;
      if (NULL == child_op_)
      {
        err = OB_NOT_INIT;
        TBSYS_LOG(ERROR, "child_op == NULL");
      }
      else if (OB_SUCCESS != (err = child_op_->close()))
      {
        TBSYS_LOG(ERROR, "child_op->open()=>%d", err);
      }
      return err;
    }

    int ObUpsLockFilter::get_row_desc(const common::ObRowDesc *&row_desc) const
    {
      int err = OB_SUCCESS;
      if (NULL == child_op_)
      {
        err = OB_NOT_INIT;
        TBSYS_LOG(ERROR, "child_op == NULL");
      }
      else if (OB_SUCCESS != (err = child_op_->get_row_desc(row_desc)))
      {
        TBSYS_LOG(ERROR, "child_op->get_row_desc()=>%d", err);
      }
      return err;
    }

    int ObUpsLockFilter::lock_row(const uint64_t table_id, const common::ObRowkey& rowkey)
    {
      int err = OB_SUCCESS;
      TEKey key(table_id, rowkey);
      TEValue* value = NULL;
      MemTable* memtable = session_.get_uc_info().host;
      ILockInfo* lock_info = session_.get_lock_info();
      if (NULL == memtable || NULL == lock_info)
      {
        err = OB_NOT_INIT;
        TBSYS_LOG(ERROR, "lock_row(session_descriptor=%u, memtable=%p, lock_info=%p): not init",
                  session_.get_session_descriptor(), memtable, lock_info);
      }
      else if (OB_SUCCESS != (err = memtable->rdlock_row(lock_info, key, value, lock_flag_)))
      {
        TBSYS_LOG(ERROR, "memtable->rdlock_row()=>%d", err);
      }
      else if (NULL != value && value->get_cur_version() > session_.get_trans_id())
      {
        err = OB_ROW_MODIFIED;
        TBSYS_LOG(DEBUG, "row_version[%ld] > session.version[%ld]", value->get_cur_version(), session_.get_trans_id());
      }
      return err;
    }

    int ObUpsLockFilter::get_next_row(const common::ObRow *&row)
    {
      int err = OB_SUCCESS;
      const ObRowkey* rowkey = NULL;
      const ObObj* obj0 = NULL;
      uint64_t table_id = 0;
      uint64_t column_id = 0;
      if (NULL == child_op_)
      {
        err = OB_NOT_INIT;
        TBSYS_LOG(ERROR, "child_op == NULL");
      }
      else if (OB_SUCCESS != (err = child_op_->get_next_row(row))
               && OB_ITER_END != err)
      {
        TBSYS_LOG(ERROR, "get_next_row()=>%d", err);
      }
      else if (OB_ITER_END == err)
      {}
      else if (NULL == row)
      {
        err = OB_ERR_UNEXPECTED;
        TBSYS_LOG(ERROR, "child return NULL row");
      }
      else if (OB_SUCCESS != (err = row->raw_get_cell(0L, obj0, table_id, column_id)))
      {
        TBSYS_LOG(ERROR, "row.raw_get_cell(0)=>%d", err);
      }
      else if (OB_SUCCESS != (err = row->get_rowkey(rowkey)))
      {
        TBSYS_LOG(ERROR, "row.get_rowkey()=>%d", err);
      }
      else if (OB_SUCCESS != (err = lock_row(table_id, *rowkey)))
      {
        TBSYS_LOG(WARN, "row_locker->lock_row()=>%d", err);
      }
      return err;
    }

    int64_t ObUpsLockFilter::to_string(char* buf, const int64_t buf_len) const
    {
      int64_t pos = 0;
      databuff_printf(buf, buf_len, pos, "UpsLockFilter(session=%p[%d:%ld])\n",
                                &session_,
                                session_.get_session_descriptor(),
                                session_.get_session_start_time());
      if (NULL != child_op_)
      {
        pos += child_op_->to_string(buf+pos, buf_len-pos);
      }
      return pos;
    }
  }; // end namespace updateserver
}; // end namespace oceanbase
