////===================================================================
 //
 // ob_table_engine.h / hash / common / Oceanbase
 //
 // Copyright (C) 2010, 2011, 2012 Taobao.com, Inc.
 //
 // Created on 2012-06-20 by Yubai (yubai.lk@taobao.com)
 //
 // -------------------------------------------------------------------
 //
 // Description
 //
 //
 // -------------------------------------------------------------------
 //
 // Change Log
 //
////====================================================================

#include "ob_update_server_main.h"
#include "ob_table_engine.h"
#include "ob_memtable.h"
#include "ob_sessionctx_factory.h"

namespace oceanbase
{
  using namespace common;
  namespace updateserver
  {
    std::pair<int64_t, int64_t> ObCellInfoNode::get_size_and_cnt() const
    {
      std::pair<int64_t, int64_t> ret(0, 0);
      ObCellInfoNodeIterable cci;
      uint64_t column_id = OB_INVALID_ID;
      ObObj value;
      cci.set_cell_info_node(this);
      while (OB_SUCCESS == cci.next_cell())
      {
        if (OB_SUCCESS == cci.get_cell(column_id, value))
        {
          ret.first += 1;
          ret.second += MemTable::get_varchar_length_kb_(value);
        }
      }
      return ret;
    }

    ObCellInfoNodeIterable::ObCellInfoNodeIterable() : cell_info_node_(NULL),
                                                       is_iter_end_(false),
                                                       cci_(),
                                                       ctrl_list_(NULL)
    {
      head_node_.column_id = OB_INVALID_ID;
      head_node_.next = NULL;

      rne_node_.column_id = OB_INVALID_ID;
      rne_node_.value.set_ext(ObActionFlag::OP_ROW_DOES_NOT_EXIST);
      rne_node_.next = NULL;

      mtime_node_.column_id = OB_INVALID_ID;
      mtime_node_.next = NULL;

      //add duyr [Delete_Update_Function_isolation] [JHOBv0.1] 20160531:b
      reset_data_mark_nodes();
      //add duyr 20160531:e
    }

    ObCellInfoNodeIterable::~ObCellInfoNodeIterable()
    {
    }

    int ObCellInfoNodeIterable::next_cell()
    {
      int ret = OB_SUCCESS;
      if (is_iter_end_
          || (NULL == cell_info_node_ && NULL == ctrl_list_))
      {
        ret = OB_ITER_END;
      }
      else
      {
        if (NULL == ctrl_list_
            || NULL == (ctrl_list_ = ctrl_list_->next))
        {
          if (NULL == cell_info_node_)
          {
            ret = OB_ITER_END;
          }
          else
          {
            ret = cci_.next_cell();
            bool is_row_finished = false;
            if (OB_SUCCESS == ret)
            {
              uint64_t column_id = OB_INVALID_ID;
              ObObj value;
              ret = cci_.get_cell(column_id, value, &is_row_finished);
            }
            if (OB_SUCCESS == ret
                && is_row_finished)
            {
              ret = OB_ITER_END;
            }
          }
        }
      }
      is_iter_end_ = (OB_ITER_END == ret);
      return ret;
    }

    int ObCellInfoNodeIterable::get_cell(uint64_t &column_id, ObObj &value)
    {
      int ret = OB_SUCCESS;
      if (is_iter_end_
          || (NULL == cell_info_node_ && NULL == ctrl_list_)
          || &head_node_ == ctrl_list_)
      {
        ret = OB_ITER_END;
      }
      else if (NULL != ctrl_list_)
      {
        column_id = ctrl_list_->column_id;
        value = ctrl_list_->value;
      }
      else
      {
        bool is_row_finished = false;
        ret = cci_.get_cell(column_id, value, &is_row_finished);
      }
      return ret;
    }

    void ObCellInfoNodeIterable::set_mtime(const uint64_t column_id, const ObModifyTime value)
    {
      mtime_node_.column_id = column_id;
      mtime_node_.value.set_modifytime(value);
      mtime_node_.next = ctrl_list_;
      ctrl_list_ = &mtime_node_;
    }

    void ObCellInfoNodeIterable::set_rne()
    {
      rne_node_.next = ctrl_list_;
      ctrl_list_ = &rne_node_;
    }

    void ObCellInfoNodeIterable::set_head()
    {
      head_node_.next = ctrl_list_;
      ctrl_list_ = &head_node_;
    }

    //add duyr [Delete_Update_Function_isolation] [JHOBv0.1] 20160531:b
    void ObCellInfoNodeIterable::set_data_mark_mtime(const uint64_t column_id, const int64_t value)
    {
        data_mark_mtime_node_.column_id = column_id;
        data_mark_mtime_node_.value.set_int(value);
        data_mark_mtime_node_.next = ctrl_list_;
        ctrl_list_ = &data_mark_mtime_node_;
    }

    void ObCellInfoNodeIterable::set_data_mark_major_ver(const uint64_t column_id, const int64_t value)
    {
        data_mark_major_version_node_.column_id = column_id;
        data_mark_major_version_node_.value.set_int(value);
        data_mark_major_version_node_.next = ctrl_list_;
        ctrl_list_ = &data_mark_major_version_node_;
    }

    void ObCellInfoNodeIterable::set_data_mark_minor_ver_start(const uint64_t column_id, const int64_t value)
    {
        data_mark_minor_ver_start_node_.column_id = column_id;
        data_mark_minor_ver_start_node_.value.set_int(value);
        data_mark_minor_ver_start_node_.next = ctrl_list_;
        ctrl_list_ = &data_mark_minor_ver_start_node_;
    }
    void ObCellInfoNodeIterable::set_data_mark_minor_ver_end(const uint64_t column_id, const int64_t value)
    {
        data_mark_minor_ver_end_node_.column_id = column_id;
        data_mark_minor_ver_end_node_.value.set_int(value);
        data_mark_minor_ver_end_node_.next = ctrl_list_;
        ctrl_list_ = &data_mark_minor_ver_end_node_;
    }
    void ObCellInfoNodeIterable::set_data_mark_data_store_type(const uint64_t column_id, const int64_t value)
    {
        data_mark_data_store_type_node_.column_id = column_id;
        data_mark_data_store_type_node_.value.set_int(value);
        data_mark_data_store_type_node_.next = ctrl_list_;
        ctrl_list_ = &data_mark_data_store_type_node_;
    }

    void ObCellInfoNodeIterable::reset_data_mark_nodes()
    {
        data_mark_mtime_node_.column_id = OB_INVALID_ID;
        data_mark_mtime_node_.next = NULL;

        data_mark_major_version_node_.column_id = OB_INVALID_ID;
        data_mark_major_version_node_.next = NULL;

        data_mark_minor_ver_start_node_.column_id = OB_INVALID_ID;
        data_mark_minor_ver_start_node_.next = NULL;

        data_mark_minor_ver_end_node_.column_id = OB_INVALID_ID;
        data_mark_minor_ver_end_node_.next = NULL;

        data_mark_data_store_type_node_.column_id = OB_INVALID_ID;
        data_mark_data_store_type_node_.next = NULL;
    }

    //add duyr 20160531:e

    void ObCellInfoNodeIterable::reset()
    {
      cell_info_node_ = NULL;
      is_iter_end_ = false;
      //cci_.init(NULL);
      ctrl_list_ = NULL;
    }

    void ObCellInfoNodeIterable::set_cell_info_node(const ObCellInfoNode *cell_info_node)
    {
      cell_info_node_ = cell_info_node;
      is_iter_end_ = false;
      //cci_.init(NULL == cell_info_node_ ? NULL : cell_info_node_->buf);
      if (NULL != cell_info_node_)
      {
        cci_.init(cell_info_node_->buf);
      }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////

    ObCellInfoNodeIterableWithCTime::ObCellInfoNodeIterableWithCTime() : inner_err(OB_SUCCESS),
                                                                         is_iter_end_(false),
                                                                         append_trun_(false), /*mod zhaoqiong [Truncate Table]:20160519*/
                                                                         ext_list_(NULL),
                                                                         trun_node_(NULL), /*mod zhaoqiong [Truncate Table]:20160519*/
                                                                         list_iter_(NULL),
                                                                         column_id_(OB_INVALID_ID),
                                                                         value_()
    {
      ctime_node_.column_id = OB_INVALID_ID;
      ctime_node_.next = NULL;

      for (int64_t i = 0; i < OB_MAX_ROWKEY_COLUMN_NUMBER; i++)
      {
        rowkey_node_[i].column_id = OB_INVALID_ID;
        rowkey_node_[i].next = NULL;
      }

      nop_node_.column_id = OB_INVALID_ID;
      nop_node_.value.set_ext(ObActionFlag::OP_NOP);
      nop_node_.next = NULL;

      //zhaoqiong [Truncate table] 20170519:b
      rdel_node_.column_id = OB_INVALID_ID;
      rdel_node_.value.set_ext(ObActionFlag::OP_DEL_ROW);
      rdel_node_.next = NULL;
      //add:e
    }

    ObCellInfoNodeIterableWithCTime::~ObCellInfoNodeIterableWithCTime()
    {
    }

    int ObCellInfoNodeIterableWithCTime::next_cell()
    {
      int ret = OB_SUCCESS;
      if (OB_SUCCESS != inner_err)
      {
        ret = inner_err;
      }
      else if (is_iter_end_)
      {
        ret = OB_ITER_END;
      }
      else if (NULL != list_iter_
              && NULL != (list_iter_ = list_iter_->next))
      {
        column_id_ = list_iter_->column_id;
        value_ = list_iter_->value;
      }
      else if (ObModifyTimeType == value_.get_type())
      {
        int64_t vtime = 0;
        value_.get_modifytime(vtime);
        ctime_node_.value.set_createtime(vtime);
        list_iter_ = ext_list_;

        column_id_ = list_iter_->column_id;
        value_ = list_iter_->value;
      }
      else
      {
        ret = ObCellInfoNodeIterable::next_cell();
        if (OB_SUCCESS != ret
            || OB_SUCCESS != (ret = ObCellInfoNodeIterable::get_cell(column_id_, value_)))
        {
          is_iter_end_ = true;
        }
      }
      return ret;
    }

    int ObCellInfoNodeIterableWithCTime::get_cell(uint64_t &column_id, common::ObObj &value)
    {
      int ret = OB_SUCCESS;
      if (OB_SUCCESS != inner_err)
      {
        ret = inner_err;
      }
      //mod zhaoqiong [Truncate Table]:20160519
//      else if (is_iter_end_)
      else if (is_iter_end_ && trun_node_ == NULL)
      //mod:e
      {
        ret = OB_ITER_END;
      }
      else
      {
        column_id = column_id_;
        value = value_;
        //add zhaoqiong [Truncate Table]:20160519
        if (append_trun_)
        {
            TBSYS_LOG(DEBUG, "get the truncate cell");
            trun_node_ = NULL;
        }
        //add:e
      }
      return ret;
    }

    void ObCellInfoNodeIterableWithCTime::reset()
    {
      ObCellInfoNodeIterable::reset();
      inner_err = OB_SUCCESS;
      is_iter_end_ = false;
      ext_list_ = NULL;
      list_iter_ = NULL;
      column_id_ = OB_INVALID_ID;
      value_.set_null();
    }

    //add zhaoqiong [Truncate Table]:20160519
    void ObCellInfoNodeIterableWithCTime::reset_all()
    {
        trun_node_ = NULL;
        append_trun_ = false;
    }
    //add:e


    //add zhaoqiong [Truncate Table]:20160519
    bool ObCellInfoNodeIterableWithCTime::get_rdel_cell()
    {
        bool bret = false;
        if (trun_node_ != NULL)
        {
            column_id_ = trun_node_->column_id;
            value_ = trun_node_->value;
            append_trun_ = true;
            bret = true;
        }
        return bret;
    }
    //add:e

    void ObCellInfoNodeIterableWithCTime::set_ctime_column_id(const uint64_t column_id)
    {
      ctime_node_.column_id = column_id;
      ctime_node_.next = ext_list_;
      ext_list_ = &ctime_node_;
    }

    //add zhaoqiong [Truncate Table]:20160519
    void ObCellInfoNodeIterableWithCTime::set_rdel()
    {
      trun_node_ = &rdel_node_;
    }
    //add:e

    void ObCellInfoNodeIterableWithCTime::set_rowkey_column(const uint64_t table_id, const ObRowkey &row_key)
    {
      const ObRowkeyInfo *rki = get_rowkey_info(table_id);
      if (NULL == rki
          || NULL == row_key.ptr())
      {
        //TBSYS_LOG(WARN, "get rowkey info fail table_id=%lu rowkey_info=%p rowkey=%p %s", table_id, rki, row_key.ptr(), to_cstring(row_key));
        inner_err = OB_SCHEMA_ERROR;
      }
      else
      {
        for (int64_t i = row_key.length() - 1; i >= 0; i--)
        {
          if (OB_SUCCESS == rki->get_column_id(i, rowkey_node_[i].column_id))
          {
            rowkey_node_[i].value = row_key.ptr()[i];
            rowkey_node_[i].next = ext_list_;
            ext_list_ = &(rowkey_node_[i]);
          }
          else
          {
            TBSYS_LOG(WARN, "get rowkey column id fail index=%ld table_id=%lu", i, table_id);
            inner_err = OB_SCHEMA_ERROR;
          }
        }
      }
    }

    void ObCellInfoNodeIterableWithCTime::set_nop()
    {
      nop_node_.next = ext_list_;
      ext_list_ = &nop_node_;
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////


    int TEValueStmtCallback::cb_func(const bool rollback, void *data, BaseSessionCtx &session)
    {
      int ret = OB_SUCCESS;
      TEValueUCInfo *uci = (TEValueUCInfo*)data;
      if (NULL == uci
          || NULL == uci->value)
      {
        ret = OB_INVALID_ARGUMENT;
      }
      else if (session.get_type() != ST_READ_WRITE)
      {}
      else if (!rollback)
      {
        uci->uc_list_tail_before_stmt = TEValueUCInfo::INVALID_CELL_INFO_NODE;
      }
      else
      {
        TBSYS_LOG(DEBUG, "rollback: uc_list_tail=%p", uci->uc_list_tail_before_stmt);
        if (TEValueUCInfo::INVALID_CELL_INFO_NODE == uci->uc_list_tail_before_stmt)
        {}
        else
        {
          if (NULL == uci->uc_list_tail_before_stmt)
          {
            uci->uc_list_head = NULL;
          }
          uci->uc_list_tail = uci->uc_list_tail_before_stmt;
          uci->uc_list_tail_before_stmt = TEValueUCInfo::INVALID_CELL_INFO_NODE;
          if (NULL != uci->uc_list_tail)
          {
            uci->uc_list_tail->next = NULL;
          }
        }
      }
      return ret;
    }

    int TEValueSessionCallback::cb_func(const bool rollback, void *data, BaseSessionCtx &session)
    {
      int ret = OB_SUCCESS;
      TEValueUCInfo *uci = (TEValueUCInfo*)data;
      if (NULL == uci
          || NULL == uci->value)
      {
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        bool need_unlock = false;
        if (!uci->value->row_lock.is_exclusive_locked_by(session.get_session_descriptor()))
        {
          uci->value->row_lock.exclusive_lock(session.get_session_descriptor());
          need_unlock = true;
        }
        if (!rollback)
        {
          ObCellInfoNode *iter = uci->uc_list_head;
          while (NULL != iter)
          {
            if (INT64_MAX == iter->modify_time)
            {
              iter->modify_time = session.get_trans_id();
            }
            else
            {
              //TBSYS_LOG(INFO, "user supply ctime %ld", iter->modify_time);
            }
            TBSYS_LOG(DEBUG, "set value=%p node=%p modify_time=%ld", uci->value, iter, iter->modify_time);
            if (uci->uc_list_tail == iter)
            {
              break;
            }
            else
            {
              iter = iter->next;
            }
          }

          if (NULL == uci->value->list_tail)
          {
            uci->value->list_head = uci->uc_list_head;
          }
          else
          {
            uci->value->list_tail->next = uci->uc_list_head;
          }
          if (NULL != uci->uc_list_tail)
          {
            uci->value->list_tail = uci->uc_list_tail;
          }
          TBSYS_LOG(DEBUG, "commit value=%p %s "
                    "cur_uc_info=%p uc_list_head=%p uc_list_tail=%p "
                    "trans_id=%ld session=%p sd=%u",
                    uci->value, uci->value->log_list(),
                    uci->value->cur_uc_info, uci->uc_list_head, uci->uc_list_tail,
                    session.get_trans_id(), &session, session.get_session_descriptor());
          uci->value->cell_info_cnt = (int16_t)(uci->value->cell_info_cnt + uci->uc_cell_info_cnt);
          uci->value->cell_info_size = (int16_t)(uci->value->cell_info_size + uci->uc_cell_info_size);
          if (ATOMIC_CAS(&(uci->value->cur_uc_info), uci, NULL) != uci)
          {
            // only exist on replaying
            TBSYS_TRACE_LOG("reset cur_uc_info to NULL, te_value=%p uci=%p", uci->value, uci);
          }
        }
        else
        {
          TBSYS_LOG(DEBUG, "rollback value=%p %s "
                    "cur_uc_info=%p uc_list_head=%p uc_list_tail=%p "
                    "session=%p sd=%u",
                    uci->value, uci->value->log_list(),
                    uci->value->cur_uc_info, uci->uc_list_head, uci->uc_list_tail,
                    &session, session.get_session_descriptor());
          if (NULL != uci->value->list_tail)
          {
            uci->value->list_tail->next = NULL;
          }
          if (ATOMIC_CAS(&(uci->value->cur_uc_info), uci, NULL) != uci)
          {
            // only exist on replaying
            TBSYS_TRACE_LOG("reset cur_uc_info to NULL, te_value=%p uci=%p", uci->value, uci);
          }
          // maybe free uncommited memory
        }
        if (need_unlock)
        {
          uci->value->row_lock.exclusive_unlock(session.get_session_descriptor());
        }
      }
      return ret;
    }

    int TransSessionCallback::cb_func(const bool rollback, void *data, BaseSessionCtx &session)
    {
      UNUSED(session);
      int ret = OB_SUCCESS;
      TransUCInfo *uci = (TransUCInfo*)data;
      if (NULL == uci)
      {
        ret = OB_INVALID_ARGUMENT;
      }
      else if (!rollback)
      {
        if (NULL != uci->host)
        {
          uci->host->add_row_counter(uci->uc_row_counter);
          uci->host->update_checksum(uci->uc_checksum);
          uci->host->update_last_trans_id(session.get_trans_id());
        }
      }
      else
      {
        // do nothing
      }
      return ret;
    }
  }
}
