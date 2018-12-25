/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_ups_modify.h
 *
 * Authors:
 *   Li Kai <yubai.lk@alipay.com>
 *
 */
#include "ob_memtable_modify.h"

namespace oceanbase
{
  namespace updateserver
  {
    /*
    MemTableModify::MemTableModify(RWSessionCtx &session, ObIUpsTableMgr &host): session_(session),
                                                                                 host_(host)
    {
    }

    MemTableModify::~MemTableModify()
    {
    }

    int MemTableModify::open()
    {
      int ret = OB_SUCCESS;
      const ObRowDesc *row_desc = NULL;
      uint64_t table_id = OB_INVALID_ID;
      uint64_t column_id = OB_INVALID_ID;
      const ObRowkeyInfo *rki = NULL;
      if (NULL == child_op_)
      {
        ret = OB_NOT_INIT;
      }
      else if (OB_SUCCESS != (ret = child_op_->open()))
      {
        if (OB_ERR_PRIMARY_KEY_DUPLICATE != ret)
        {
          TBSYS_LOG(WARN, "child operator open fail ret=%d", ret);
        }
      }
      else if (OB_SUCCESS != (ret = child_op_->get_row_desc(row_desc))
              || NULL == row_desc)
      {
        if (OB_ITER_END != ret)
        {
          TBSYS_LOG(WARN, "get_row_desc from child_op=%p type=%d fail ret=%d", child_op_, child_op_->get_type(), ret);
          ret = (OB_SUCCESS != ret) ? OB_ERROR : ret;
        }
        else
        {
          ret = OB_SUCCESS;
        }
      }
      else if (OB_SUCCESS != (ret = row_desc->get_tid_cid(0, table_id, column_id))
              || OB_INVALID_ID == table_id)
      {
        TBSYS_LOG(WARN, "get_tid_cid from row_desc fail, child_op=%p type=%d %s ret=%d",
                  child_op_, child_op_->get_type(), to_cstring(*row_desc), ret);
        ret = (OB_SUCCESS != ret) ? OB_ERROR : ret;
      }
      else if (NULL == (rki = get_rowkey_info(host_, table_id)))
      {
        TBSYS_LOG(WARN, "get_rowkey_info fail table_id=%lu", table_id);
        ret = OB_SCHEMA_ERROR;
      }
      else
      {
        UpsSchemaMgrGuard sm_guard;
        const CommonSchemaManager *sm = NULL;
        if (NULL == (sm = host_.get_schema_mgr().get_schema_mgr(sm_guard)))
        {
          TBSYS_LOG(WARN, "get_schema_mgr fail");
          ret = OB_SCHEMA_ERROR;
        }
        else
        {
          ObCellIterAdaptor cia;
          cia.set_row_iter(child_op_, rki->get_size(), sm);
          ret = host_.apply(session_, cia, dml_type_);
        }
      }
      if (OB_SUCCESS != ret)
      {
        if (NULL != child_op_)
        {
          child_op_->close();
        }
      }
      return ret;
    }

    int MemTableModify::close()
    {
      int ret = OB_SUCCESS;
      if (NULL == child_op_)
      {
        ret = OB_NOT_INIT;
      }
      else
      {
        int tmp_ret = OB_SUCCESS;
        if (OB_SUCCESS != (tmp_ret = child_op_->close()))
        {
          TBSYS_LOG(WARN, "child operator close fail ret=%d", tmp_ret);
        }
      }
      return ret;
    }

    int MemTableModify::get_affected_rows(int64_t& row_count)
    {
      int ret = OB_SUCCESS;
      row_count = session_.get_ups_result().get_affected_rows();
      return ret;
    }

    int MemTableModify::get_next_row(const common::ObRow *&row)
    {
      UNUSED(row);
      return OB_ITER_END;
    }

    int MemTableModify::get_row_desc(const common::ObRowDesc *&row_desc) const
    {
      UNUSED(row_desc);
      return OB_ITER_END;
    }

    int64_t MemTableModify::to_string(char* buf, const int64_t buf_len) const
    {
      int64_t pos = 0;
      databuff_printf(buf, buf_len, pos, "MemTableModify(dml_type=%s session=%p[%d:%ld])\n",
                      str_dml_type(dml_type_),
                      &session_,
                      session_.get_session_descriptor(),
                      session_.get_session_start_time());
      if (NULL != child_op_)
      {
        pos += child_op_->to_string(buf+pos, buf_len-pos);
      }
      return pos;
    }
    */
  } // end namespace updateserver
} // end namespace oceanbase
