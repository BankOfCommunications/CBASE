/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 *
 * Version: 0.1: ob_mutator.cpp,v 0.1 2010/09/15 14:59:14 chuanhui Exp $
 *
 * Authors:
 *   chuanhui <rizhao.ych@taobao.com>
 *     - some work details if you want
 *
 */
#include "ob_mutator.h"
#include "ob_rowkey_helper.h"
#include "utility.h"

namespace oceanbase
{
  namespace common
  {
    ObMutator :: ObMutator() : mod_(ObModIds::OB_MUTATOR),
                               local_page_arena_(ALLOCATOR_PAGE_SIZE, mod_),
                               str_buf_(local_page_arena_),
                               page_arena_(local_page_arena_),
                               type_(NORMAL_UPDATE),
                               mutator_buf_(NULL)
    {
      list_head_ = NULL;
      list_tail_ = NULL;
      last_row_key_.assign(NULL, 0);
      last_table_name_.assign(NULL, 0);
      cell_count_ = 0;
      first_table_name_.assign(NULL, 0);
      last_table_id_ = 0;
      id_name_type_ = UNSURE;
      cell_store_size_ = 0;
      cur_iter_node_ = NULL;
      has_begin_ = false;
      schema_manager_ = NULL;
    }

    ObMutator :: ObMutator(ModuleArena &arena) : str_buf_(arena),
                                                 page_arena_(arena),
                                                 type_(NORMAL_UPDATE),
                                                 mutator_buf_(NULL)
    {
      list_head_ = NULL;
      list_tail_ = NULL;
      last_row_key_.assign(NULL, 0);
      last_table_name_.assign(NULL, 0);
      cell_count_ = 0;
      first_table_name_.assign(NULL, 0);
      last_table_id_ = 0;
      id_name_type_ = UNSURE;
      cell_store_size_ = 0;
      cur_iter_node_ = NULL;
      has_begin_ = false;
      schema_manager_ = NULL;
    }

    ObMutator :: ~ObMutator()
    {
    }

    int ObMutator :: clear()
    {
      int err = OB_SUCCESS;
      list_head_ = NULL;
      list_tail_ = NULL;
      last_row_key_.assign(NULL, 0);
      last_table_name_.assign(NULL, 0);
      cell_count_ = 0;
      first_table_name_.assign(NULL, 0);
      last_table_id_ = 0;
      id_name_type_ = UNSURE;
      cell_store_size_ = 0;
      cur_iter_node_ = NULL;
      type_ = NORMAL_UPDATE;
      has_begin_ = false;
      mutator_buf_ = NULL;
      err = str_buf_.reset();
      if (OB_SUCCESS == err)
      {
        local_page_arena_.reuse();
      }
      if (OB_SUCCESS != err)
      {
        TBSYS_LOG(WARN, "failed to clear str_buf or page_arena, err=%d", err);
      }
      return err;
    }

    int ObMutator :: reset()
    {
      int err = OB_SUCCESS;
      list_head_ = NULL;
      list_tail_ = NULL;
      last_row_key_.assign(NULL, 0);
      last_table_name_.assign(NULL, 0);
      cell_count_ = 0;
      first_table_name_.assign(NULL, 0);
      last_table_id_ = 0;
      id_name_type_ = UNSURE;
      cell_store_size_ = 0;
      cur_iter_node_ = NULL;
      type_ = NORMAL_UPDATE;
      has_begin_ = false;
      mutator_buf_ = NULL;
      err = str_buf_.reset();
      if (OB_SUCCESS == err)
      {
        local_page_arena_.free();
      }
      if (OB_SUCCESS != err)
      {
        TBSYS_LOG(WARN, "failed to reset str_buf or page_arena, err=%d", err);
      }
      return err;
    }

    int64_t ObMutator::size(void) const
    {
      return cell_count_;
    }

    const ObString & ObMutator::get_first_table_name(void) const
    {
      return first_table_name_;
    }

    int ObMutator :: use_ob_sem()
    {
      int err = OB_SUCCESS;

      ObMutatorCellInfo mutation;
      mutation.cell_info.value_.set_ext(ObActionFlag::OP_USE_OB_SEM);

      err = add_cell(mutation);
      if (OB_SUCCESS != err)
      {
        TBSYS_LOG(WARN, "failed to add ob sem op, err=%d", err);
      }

      return err;
    }

    int ObMutator :: use_db_sem()
    {
      int err = OB_SUCCESS;

      ObMutatorCellInfo mutation;
      mutation.cell_info.value_.set_ext(ObActionFlag::OP_USE_DB_SEM);

      err = add_cell(mutation);
      if (OB_SUCCESS != err)
      {
        TBSYS_LOG(WARN, "failed to add db sem op, err=%d", err);
      }

      return err;
    }

    void ObMutator :: set_mutator_type(const MUTATOR_TYPE type)
    {
      type_ = type;
    }

    ObMutator::MUTATOR_TYPE ObMutator :: get_mutator_type(void) const
    {
      return type_;
    }

    int ObMutator :: update(const ObString& table_name, const ObRowkey& row_key,
        const ObString& column_name, const ObObj& value, const int return_flag)
    {
      int err = OB_SUCCESS;

      ObMutatorCellInfo mutation;

      mutation.cell_info.table_name_ = table_name;
      mutation.cell_info.row_key_ = row_key;
      mutation.cell_info.column_name_ = column_name;

      int64_t ext_value = ObActionFlag::OP_UPDATE;
      if (RETURN_UPDATE_RESULT == return_flag)
      {
        ext_value |= ObActionFlag::OP_RETURN_UPDATE_RESULT;
      }
      mutation.op_type.set_ext(ext_value);
      mutation.cell_info.value_ = value;

      err = add_cell(mutation);
      if (OB_SUCCESS != err)
      {
        TBSYS_LOG(WARN, "failed to add mutation, err=%d", err);
      }

      return err;
    }

    int ObMutator :: update(const uint64_t table_id, const ObRowkey& row_key,
        const uint64_t column_id, const ObObj& value, const int return_flag)
    {
      int err = OB_SUCCESS;

      ObMutatorCellInfo mutation;

      mutation.cell_info.table_id_ = table_id;
      mutation.cell_info.row_key_ = row_key;
      mutation.cell_info.column_id_ = column_id;

      int64_t ext_value = ObActionFlag::OP_UPDATE;
      if (RETURN_UPDATE_RESULT == return_flag)
      {
        ext_value |= ObActionFlag::OP_RETURN_UPDATE_RESULT;
      }
      mutation.op_type.set_ext(ext_value);
      mutation.cell_info.value_ = value;

      err = add_cell(mutation);
      if (OB_SUCCESS != err)
      {
        TBSYS_LOG(WARN, "failed to add mutation, err=%d", err);
      }

      return err;
    }

    int ObMutator :: insert(const ObString& table_name, const ObRowkey& row_key,
        const ObString& column_name, const ObObj& value, const int return_flag)
    {
      int err = OB_SUCCESS;

      ObMutatorCellInfo mutation;

      mutation.cell_info.table_name_ = table_name;
      mutation.cell_info.row_key_ = row_key;
      mutation.cell_info.column_name_ = column_name;

      int64_t ext_value = ObActionFlag::OP_INSERT;
      if (RETURN_UPDATE_RESULT == return_flag)
      {
        ext_value |= ObActionFlag::OP_RETURN_UPDATE_RESULT;
      }
      mutation.op_type.set_ext(ext_value);
      mutation.cell_info.value_ = value;

      err = add_cell(mutation);
      if (OB_SUCCESS != err)
      {
        TBSYS_LOG(WARN, "failed to add mutation, err=%d", err);
      }

      return err;
    }

    int ObMutator :: insert(const uint64_t table_id, const ObRowkey& row_key,
        const uint64_t column_id, const ObObj& value, const int return_flag)
    {
      int err = OB_SUCCESS;

      ObMutatorCellInfo mutation;

      mutation.cell_info.table_id_ = table_id;
      mutation.cell_info.row_key_ = row_key;
      mutation.cell_info.column_id_ = column_id;

      int64_t ext_value = ObActionFlag::OP_INSERT;
      if (RETURN_UPDATE_RESULT == return_flag)
      {
        ext_value |= ObActionFlag::OP_RETURN_UPDATE_RESULT;
      }
      mutation.op_type.set_ext(ext_value);
      mutation.cell_info.value_ = value;

      err = add_cell(mutation);
      if (OB_SUCCESS != err)
      {
        TBSYS_LOG(WARN, "failed to add mutation, err=%d", err);
      }

      return err;
    }

    int ObMutator :: add(const ObString& table_name, const ObRowkey& row_key,
        const ObString& column_name, const int64_t value, const int return_flag)
    {
      int err = OB_SUCCESS;

      ObMutatorCellInfo mutation;

      mutation.cell_info.table_name_ = table_name;
      mutation.cell_info.row_key_ = row_key;
      mutation.cell_info.column_name_ = column_name;

      int64_t ext_value = ObActionFlag::OP_UPDATE;
      if (RETURN_UPDATE_RESULT == return_flag)
      {
        ext_value |= ObActionFlag::OP_RETURN_UPDATE_RESULT;
      }
      mutation.op_type.set_ext(ext_value);
      mutation.cell_info.value_.set_int(value, true);

      err = add_cell(mutation);
      if (OB_SUCCESS != err)
      {
        TBSYS_LOG(WARN, "failed to add mutation, err=%d", err);
      }

      return err;
    }

    int ObMutator :: add(const ObString& table_name, const ObRowkey& row_key,
        const ObString& column_name, const float value, const int return_flag)
    {
      int err = OB_SUCCESS;
      ObMutatorCellInfo mutation;

      mutation.cell_info.table_name_ = table_name;
      mutation.cell_info.row_key_ = row_key;
      mutation.cell_info.column_name_ = column_name;

      int64_t ext_value = ObActionFlag::OP_UPDATE;
      if (RETURN_UPDATE_RESULT == return_flag)
      {
        ext_value |= ObActionFlag::OP_RETURN_UPDATE_RESULT;
      }
      mutation.op_type.set_ext(ext_value);
      mutation.cell_info.value_.set_float(value, true);

      err = add_cell(mutation);
      if (OB_SUCCESS != err)
      {
        TBSYS_LOG(WARN, "failed to add mutation, err=%d", err);
      }

      return err;
    }

    int ObMutator :: add(const ObString& table_name, const ObRowkey& row_key,
        const ObString& column_name, const double value, const int return_flag)
    {
      int err = OB_SUCCESS;
      ObMutatorCellInfo mutation;

      mutation.cell_info.table_name_ = table_name;
      mutation.cell_info.row_key_ = row_key;
      mutation.cell_info.column_name_ = column_name;

      int64_t ext_value = ObActionFlag::OP_UPDATE;
      if (RETURN_UPDATE_RESULT == return_flag)
      {
        ext_value |= ObActionFlag::OP_RETURN_UPDATE_RESULT;
      }
      mutation.op_type.set_ext(ext_value);
      mutation.cell_info.value_.set_double(value, true);

      err = add_cell(mutation);
      if (OB_SUCCESS != err)
      {
        TBSYS_LOG(WARN, "failed to add mutation, err=%d", err);
      }

      return err;
    }

    int ObMutator :: add_datetime(const ObString& table_name, const ObRowkey& row_key,
        const ObString& column_name, const ObDateTime& value, const int return_flag)
    {
      int err = OB_SUCCESS;
      ObMutatorCellInfo mutation;

      mutation.cell_info.table_name_ = table_name;
      mutation.cell_info.row_key_ = row_key;
      mutation.cell_info.column_name_ = column_name;

      int64_t ext_value = ObActionFlag::OP_UPDATE;
      if (RETURN_UPDATE_RESULT == return_flag)
      {
        ext_value |= ObActionFlag::OP_RETURN_UPDATE_RESULT;
      }
      mutation.op_type.set_ext(ext_value);
      mutation.cell_info.value_.set_datetime(value, true);

      err = add_cell(mutation);
      if (OB_SUCCESS != err)
      {
        TBSYS_LOG(WARN, "failed to add mutation, err=%d", err);
      }

      return err;
    }

    int ObMutator :: add_precise_datetime(const ObString& table_name, const ObRowkey& row_key,
        const ObString& column_name, const ObPreciseDateTime& value, const int return_flag)
    {
      int err = OB_SUCCESS;
      ObMutatorCellInfo mutation;

      mutation.cell_info.table_name_ = table_name;
      mutation.cell_info.row_key_ = row_key;
      mutation.cell_info.column_name_ = column_name;

      int64_t ext_value = ObActionFlag::OP_UPDATE;
      if (RETURN_UPDATE_RESULT == return_flag)
      {
        ext_value |= ObActionFlag::OP_RETURN_UPDATE_RESULT;
      }
      mutation.op_type.set_ext(ext_value);
      mutation.cell_info.value_.set_precise_datetime(value, true);

      err = add_cell(mutation);
      if (OB_SUCCESS != err)
      {
        TBSYS_LOG(WARN, "failed to add mutation, err=%d", err);
      }

      return err;
    }

    int ObMutator :: del_row(const ObString& table_name, const ObRowkey& row_key)
    {
      int err = OB_SUCCESS;
      ObMutatorCellInfo mutation;

      mutation.cell_info.table_name_ = table_name;
      mutation.cell_info.row_key_ = row_key;
      mutation.cell_info.value_.set_ext(ObActionFlag::OP_DEL_ROW);

      err = add_cell(mutation);
      if (OB_SUCCESS != err)
      {
        TBSYS_LOG(WARN, "failed to add del op, err=%d", err);
      }

      return err;
    }

    int ObMutator::del_row(const uint64_t table_id, const ObRowkey& row_key)
    {
      int err = OB_SUCCESS;
      ObMutatorCellInfo mutation;

      mutation.cell_info.table_id_ = table_id;
      mutation.cell_info.row_key_ = row_key;
      mutation.cell_info.value_.set_ext(ObActionFlag::OP_DEL_ROW);

      err = add_cell(mutation);
      if (OB_SUCCESS != err)
      {
        TBSYS_LOG(WARN, "failed to add del op, err=%d", err);
      }

      return err;
    }

    //add zhaoqiong [Truncate Table]:20160318:b
    int ObMutator :: trun_tab(const uint64_t table_id, const common::ObString & table_name, const common::ObString & user_name, const common::ObString & comment)
    {
      int err = OB_SUCCESS;
      ObMutatorCellInfo mutation;
      mutation.cell_info.table_id_ = table_id;
      mutation.cell_info.value_.set_ext(ObActionFlag::OP_TRUN_TAB);
      err = add_cell(mutation);
      if (OB_SUCCESS != err)
      {
        TBSYS_LOG(WARN, "failed to add trun op, err=%d", err);
      }
      ObRowkey rowkey;
      ObObj value[2];
      value[0].set_precise_datetime(tbsys::CTimeUtil::getTime());
      value[1].set_int(table_id);
      rowkey.assign(value, 2);

      ObObj varchar_obj1;
      ObObj varchar_obj2;
      ObObj varchar_obj3;
      varchar_obj1.set_varchar(table_name);
      varchar_obj2.set_varchar(user_name);
      varchar_obj3.set_varchar(comment);

      if (OB_SUCCESS != (err = update(OB_ALL_TRUNCATE_OP_TID, rowkey, OB_APP_MIN_COLUMN_ID+2 , varchar_obj1)))
      {
         TBSYS_LOG(WARN, "failed to add trun op, err=%d", err);
      }
      else if (OB_SUCCESS != (err = update(OB_ALL_TRUNCATE_OP_TID, rowkey, OB_APP_MIN_COLUMN_ID+3, varchar_obj2)))
      {
         TBSYS_LOG(WARN, "failed to add trun op, err=%d", err);
      }
      else if (OB_SUCCESS != (err = update(OB_ALL_TRUNCATE_OP_TID, rowkey, OB_APP_MIN_COLUMN_ID+4, varchar_obj3)))
      {
         TBSYS_LOG(WARN, "failed to add trun op, err=%d", err);
      }
      return err;
    }
    //add:e

    //add zhaoqiong [Truncate Table]:20160318:b
    int ObMutator :: ups_trun_tab(const uint64_t table_id, const bool force_finish)
    {
      int err = OB_SUCCESS;
      ObMutatorCellInfo mutation;

      mutation.cell_info.table_id_ = table_id;
      mutation.cell_info.value_.set_ext(ObActionFlag::OP_TRUN_TAB);

      err = add_cell(mutation,CHANGED,ROWKEY_BARRIER,OB_DML_UNKNOW);
      if (OB_SUCCESS != err)
      {
        TBSYS_LOG(WARN, "failed to add del op, err=%d", err);
      }
      else if (force_finish)
      {
        list_tail_->row_finished_stat = FINISHED;
      }
      return err;
    }
    //add:e

    int ObMutator :: add_row_barrier()
    {
      ObMutatorCellInfo cell;
      return add_cell(cell, CHANGED_UNKNOW, ROWKEY_BARRIER, OB_DML_UNKNOW);
    }

    int ObMutator :: set_dml_type(const ObDmlType dml_type)
    {
      ObMutatorCellInfo cell;
      return add_cell(cell, CHANGED_UNKNOW, ROW_DML_TYPE, dml_type);
    }

    int ObMutator :: add_cell(const ObMutatorCellInfo& cell)
    {
      return add_cell(cell, CHANGED_UNKNOW, NO_BARRIER, OB_DML_UNKNOW);
    }

    int ObMutator :: add_cell(const ObMutatorCellInfo& cell,
                              const RowChangedStat row_changed_stat,
                              const BarrierFlag barrier_flag,
                              const ObDmlType dml_type)
    {
      int err = OB_SUCCESS;
      int64_t store_size = 0;

      CellInfoNode* cur_node = NULL;
      cur_node = (CellInfoNode*)page_arena_.alloc(sizeof(*cur_node));
      if (NULL == cur_node)
      {
        TBSYS_LOG(WARN, "failed to alloc, cur_node=%p", cur_node);
        err = OB_MEM_OVERFLOW;
      }
      else
      {
        err = copy_cell_(cell, cur_node->cell, row_changed_stat, store_size);
        if (OB_SUCCESS != err)
        {
          TBSYS_LOG(WARN, "failed to copy cell, err=%d", err);
        }
        else
        {
          cur_node->next = NULL;
          cur_node->row_changed_stat = row_changed_stat;
          cur_node->row_finished_stat = FINISHED_UNKNOW;
          cur_node->barrier_flag = barrier_flag;
          cur_node->dml_type = dml_type;

          err = add_node_(cur_node);
          if (OB_SUCCESS == err)
          {
            if (0 == cell_count_++)
            {
              first_table_name_ = cur_node->cell.cell_info.table_name_;
            }
          }
        }
      }

      if (OB_SUCCESS == err)
      {
        cell_store_size_ += store_size;
      }

      return err;
    }

    void ObMutator :: reset_iter()
    {
      has_begin_ = false;
    }

    int ObMutator :: next_cell()
    {
      int err = OB_SUCCESS;
      if (OB_SUCCESS != (err = next_cell_())
          && OB_ITER_END != err)
      {
        TBSYS_LOG(ERROR, "next_cell_()=>%d", err);
      }
      else if (OB_ITER_END == err)
      {}
      else if (NULL == cur_iter_node_)
      {
        err = OB_ERR_UNEXPECTED;
        TBSYS_LOG(ERROR, "cur_iter_node = NULL");
      }
      else if (NO_BARRIER == cur_iter_node_->barrier_flag)
      {} // do nothing
      else
      {
        //TBSYS_LOG(INFO, "skip dml_type_cell: type=%d", cur_iter_node_->dml_type);
        err = next_cell_();
      }
      return err;
    }

    int ObMutator :: next_cell_()
    {
      int err = OB_SUCCESS;

      if (!has_begin_)
      {
        has_begin_ = true;
        cur_iter_node_ = list_head_;
      }
      else
      {
        if (NULL != cur_iter_node_)
        {
          cur_iter_node_ = cur_iter_node_->next;
        }
      }

      if (NULL == cur_iter_node_)
      {
        err = OB_ITER_END;
      }
      else if (NULL == cur_iter_node_->next)
      {
        cur_iter_node_->row_finished_stat = FINISHED;
      }

      return err;
    }

    int ObMutator :: get_cell(ObMutatorCellInfo** cell)
    {
      return get_cell(cell, NULL, NULL);
    }

    int ObMutator :: get_cell(ObMutatorCellInfo** cell, bool* is_row_changed, bool* is_row_finished, ObDmlType *dml_type)
    {
      int err = OB_SUCCESS;

      if (NULL == cell)
      {
        TBSYS_LOG(WARN, "invalid param, cell=%p", cell);
        err = OB_INVALID_ARGUMENT;
      }
      else if (NULL == cur_iter_node_)
      {
        TBSYS_LOG(WARN, "invalid status, cur_iter_node=%p", cur_iter_node_);
        err = OB_ERROR;
      }
      else
      {
        *cell = &(cur_iter_node_->cell);
        if (NULL != is_row_changed)
        {
          if (CHANGED_UNKNOW == cur_iter_node_->row_changed_stat)
          {
            err = OB_NOT_SUPPORTED;
          }
          else
          {
            *is_row_changed = (NOCHANGED != cur_iter_node_->row_changed_stat);
          }
        }
        if (NULL != is_row_finished)
        {
          if (FINISHED_UNKNOW == cur_iter_node_->row_finished_stat)
          {
            err = OB_NOT_SUPPORTED;
          }
          else
          {
            *is_row_finished = (NOFINISHED != cur_iter_node_->row_finished_stat);
          }
        }
        if (NULL != dml_type)
        {
          if (OB_DML_UNKNOW == cur_iter_node_->dml_type)
          {
            err = OB_NOT_SUPPORTED;
          }
          else
          {
            *dml_type = cur_iter_node_->dml_type;
          }
        }
      }

      return err;
    }

    int ObMutator :: pre_serialize()
    {
      int err = OB_SUCCESS;
      char* buf = NULL;
      int64_t size = get_serialize_size();
      int64_t pos = 0;
      if (NULL != mutator_buf_)
      {
        err = OB_INIT_TWICE;
        TBSYS_LOG(ERROR, "pre_serialize(mutator_buf=%p)", mutator_buf_);
      }
      else if (NULL == (buf = page_arena_.alloc(size)))
      {
        err = OB_MEM_OVERFLOW;
        TBSYS_LOG(ERROR, "page_arena.alloc(%ld)=>%d", size, err);
      }
      else if (OB_SUCCESS != (err = serialize(buf, size, pos)))
      {
        page_arena_.free(buf);
        TBSYS_LOG(ERROR, "serialize(%p[%ld])=>%d", buf, size, err);
      }
      else
      {
        mutator_buf_ = buf;
      }
      return err;
    }

    int ObMutator :: serialize(char* buf, const int64_t buf_len, int64_t& pos) const
    {
      int err = OB_SUCCESS;
      int64_t ori_pos = pos;

      if (NULL == buf || buf_len <= 0 || pos >= buf_len)
      {
        TBSYS_LOG(WARN, "invalid param, buf=%p, buf_len=%ld, pos=%ld", buf, buf_len, pos);
        err = OB_INVALID_ARGUMENT;
      }
      else if (NULL != mutator_buf_)
      {
        int64_t size = get_serialize_size();
        if (pos + size > buf_len)
        {
          err = OB_BUF_NOT_ENOUGH;
          TBSYS_LOG(WARN, "pos[%ld] + size[%ld] > buf_len[%ld]", pos, size, buf_len);
        }
        else
        {
          memcpy(buf + pos, mutator_buf_, size);
          pos += size;
        }
      }
      else
      {
        CellInfoNode* node = list_head_;
        ObString last_table_name;
        ObRowkey last_row_key;
        uint64_t last_table_id = 0;
        last_table_name.assign(NULL, 0);
        last_row_key.assign(NULL, 0);
        int64_t tmp_ext_val = 0;
        ObObj tmp_obj;

        // MUTATOR START PARAM FIELD
        err = serialize_flag_(buf, buf_len, pos, ObActionFlag::MUTATOR_PARAM_FIELD);
        if (OB_SUCCESS == err)
        {
          // MUTATOR TYPE PARAM FIELD
          err = serialize_flag_(buf, buf_len, pos, ObActionFlag::MUTATOR_TYPE_FIELD);
          if (OB_SUCCESS == err)
          {
            tmp_obj.set_int(type_);
            err = tmp_obj.serialize(buf, buf_len, pos);
          }
        }

        while (OB_SUCCESS == err && NULL != node)
        {
          if (NO_BARRIER != node->barrier_flag)
          {
            if (ROWKEY_BARRIER & node->barrier_flag)
            {
              last_row_key.assign(NULL, 0);
            }
            if (ROW_DML_TYPE & node->barrier_flag)
            {
              err = serialize_flag_(buf, buf_len, pos, ObActionFlag::DML_TYPE_FIELD);
              {
                tmp_obj.set_int(node->dml_type);
                err = tmp_obj.serialize(buf, buf_len, pos);
              }
            }
            node = node->next;
            continue;
          }

          int64_t type = node->cell.cell_info.value_.get_type();
          if (ObExtendType == type)
          {
            node->cell.cell_info.value_.get_ext(tmp_ext_val);
          }

          if (ObExtendType == type && (ObActionFlag::OP_USE_OB_SEM == tmp_ext_val
              || ObActionFlag::OP_USE_DB_SEM == tmp_ext_val))
          {
            // use ob/db sem
            err = serialize_flag_(buf, buf_len, pos, ObActionFlag::OBDB_SEMANTIC_FIELD);
            if (OB_SUCCESS == err)
            {
              err = node->cell.cell_info.value_.serialize(buf, buf_len, pos);
            }
          }
          else
          {
            bool table_changed = false;
            // serialize table name or table id
            if (0 != node->cell.cell_info.table_name_.length())
            {
              // serialize table name
              if (0 != node->cell.cell_info.table_name_.compare(last_table_name))
              {
                table_changed = true;
                err = serialize_flag_(buf, buf_len, pos, ObActionFlag::TABLE_NAME_FIELD);
                if (OB_SUCCESS == err)
                {
                  tmp_obj.reset();
                  tmp_obj.set_varchar(node->cell.cell_info.table_name_);
                  err = tmp_obj.serialize(buf, buf_len, pos);
                }
              }
            }
            else
            {
              // serialize table id
              if (last_table_id != node->cell.cell_info.table_id_)
              {
                table_changed = true;
                err = serialize_flag_(buf, buf_len, pos, ObActionFlag::TABLE_NAME_FIELD);
                if (OB_SUCCESS == err)
                {
                  tmp_obj.reset();
                  tmp_obj.set_int(node->cell.cell_info.table_id_);
                  err = tmp_obj.serialize(buf, buf_len, pos);
                }
              }
            }

            if (OB_SUCCESS == err)
            {
              // serialize row key
              if (table_changed
                  || 0 != node->cell.cell_info.row_key_.compare(last_row_key))
              {
                err = serialize_flag_(buf, buf_len, pos, ObActionFlag::FORMED_ROW_KEY_FIELD);
                if (OB_SUCCESS == err)
                {
                  err = node->cell.cell_info.row_key_.serialize(buf, buf_len, pos);
                }
              }
            }

            if (OB_SUCCESS != err)
            {
              // does nothing
            }
            else if (ObExtendType == type
                && ObActionFlag::OP_DEL_ROW == tmp_ext_val)
            {
              // serialize del row
              err = serialize_flag_(buf, buf_len, pos, ObActionFlag::OP_DEL_ROW);
            }
            //add zhaoqiong [Truncate Table]:20160318:b
            else if (ObExtendType == type
                && ObActionFlag::OP_TRUN_TAB == tmp_ext_val)
            {
              // serialize truncate_flag
              err = serialize_flag_(buf, buf_len, pos, ObActionFlag::OP_TRUN_TAB);
            }
            //add:e
            else
            {
              // serialize column param
              if (OB_SUCCESS == err)
              {
                // serialize column name or column id
                if (OB_SUCCESS == err)
                {
                  if (0 != node->cell.cell_info.column_name_.length())
                  {
                    tmp_obj.reset();
                    tmp_obj.set_varchar(node->cell.cell_info.column_name_);
                    err = tmp_obj.serialize(buf, buf_len, pos);
                  }
                  else
                  {
                    tmp_obj.reset();
                    tmp_obj.set_int(node->cell.cell_info.column_id_);
                    err = tmp_obj.serialize(buf, buf_len, pos);
                  }
                }

                // serialize op type
                if (OB_SUCCESS == err)
                {
                  err = node->cell.op_type.serialize(buf, buf_len, pos);
                }

                // serialize column value
                if (OB_SUCCESS == err)
                {
                  err = node->cell.cell_info.value_.serialize(buf, buf_len, pos);
                }
              }
            }

            if (OB_SUCCESS == err)
            {
              last_table_name = node->cell.cell_info.table_name_;
              last_table_id = node->cell.cell_info.table_id_;
              last_row_key = node->cell.cell_info.row_key_;
            }
          }

          if (OB_SUCCESS == err)
          {
            node = node->next;
          }
        }

        // serialize end flag
        if (OB_SUCCESS == err)
        {
          err = serialize_flag_(buf, buf_len, pos, ObActionFlag::END_PARAM_FIELD);
        }

        if (OB_SUCCESS != err)
        {
          TBSYS_LOG(WARN, "failed to serialize, buf=%p, buf_len=%ld, ori_pos=%ld, pos=%ld, err=%d",
              buf, buf_len, ori_pos, pos, err);
        }
      }

      return err;
    }

    int ObMutator :: deserialize(const char* buf, const int64_t buf_len, int64_t& pos)
    {
      int err = OB_SUCCESS;

      if (NULL == buf || buf_len <= 0 || pos >= buf_len)
      {
        TBSYS_LOG(WARN, "invalid param, buf=%p, buf_len=%ld, pos=%ld", buf, buf_len, pos);
        err = OB_INVALID_ARGUMENT;
      }
      else
      {
        // reset ob_mutator
        err = reuse();
        if (OB_SUCCESS != err)
        {
          TBSYS_LOG(WARN, "failed to reset, err=%d", err);
        }
      }

      if (OB_SUCCESS == err)
      {
        ObObj op;
        ObObj tmp_obj;
        ObDmlType dml_type = OB_DML_REPLACE;
        ObString tmp_str;
        ObMutatorCellInfo cur_cell;
        bool end_flag = false;
        ObObj rowkey_col_buf[OB_MAX_ROWKEY_COLUMN_NUMBER];
        bool is_row_changed = false;

        while (OB_SUCCESS == err && pos < buf_len && !end_flag)
        {
          op.reset();
          err = op.deserialize(buf, buf_len, pos);
          if (OB_SUCCESS != err)
          {
            TBSYS_LOG(WARN, "failed to deserialize, err=%d", err);
          }
          else
          {
            int64_t ext_val = 0;
            int64_t type = op.get_type();
            if (ObExtendType == type)
            {
              op.get_ext(ext_val);
            }

            if (ObExtendType != type)
            {
              if (ObIntType == type)
              {
                int64_t column_id = 0;
                op.get_int(column_id);
                cur_cell.cell_info.column_id_ = column_id;
                cur_cell.cell_info.column_name_.assign(NULL, 0);
              }
              else if (ObVarcharType == type)
              {
                op.get_varchar(tmp_str);
                cur_cell.cell_info.column_name_ = tmp_str;
                cur_cell.cell_info.column_id_ = 0;
              }
              else
              {
                TBSYS_LOG(WARN, "invalid obj type, obj_type=%ld", type);
                err = OB_ERROR;
              }

              if (OB_SUCCESS == err)
              {
                // deserialize op type
                cur_cell.op_type.reset();
                err = cur_cell.op_type.deserialize(buf, buf_len, pos);
              }

              if (OB_SUCCESS == err)
              {
                // deserialize column value
                tmp_obj.reset();
                err = tmp_obj.deserialize(buf, buf_len, pos);
                cur_cell.cell_info.value_.reset();
                cur_cell.cell_info.value_ = tmp_obj;
                if (OB_SUCCESS != err)
                {
                  TBSYS_LOG(WARN, "failed to add cell, err=%d", err);
                }
                else
                {
                  err = add_cell(cur_cell, is_row_changed ? CHANGED : NOCHANGED, NO_BARRIER, dml_type);
                  is_row_changed = false;
                  if (OB_SUCCESS != err)
                  {
                    TBSYS_LOG(WARN, "failed to add cell, err=%d", err);
                  }
                }
              }
            }
            else
            {
              switch (ext_val)
              {
                case ObActionFlag::MUTATOR_PARAM_FIELD:
                  // does nothing
                  break;
                case ObActionFlag::MUTATOR_TYPE_FIELD:
                  err = tmp_obj.deserialize(buf, buf_len, pos);
                  if (OB_SUCCESS == err)
                  {
                    int64_t type = 0;
                    err = tmp_obj.get_int(type);
                    if (OB_SUCCESS != err)
                    {
                      TBSYS_LOG(WARN, "failed to get int for mutator type, err=%d", err);
                    }
                    else
                    {
                      type_ = (MUTATOR_TYPE)(type);
                    }
                  }
                  break;
                case ObActionFlag::OBDB_SEMANTIC_FIELD:
                  // deserialize semantic field
                  err = tmp_obj.deserialize(buf, buf_len, pos);
                  if (OB_SUCCESS == err)
                  {
                    int64_t sem_op = 0;
                    tmp_obj.get_ext(sem_op);
                    ObMutatorCellInfo sem_cell;
                    sem_cell.cell_info.value_.set_ext(sem_op);
                    err = add_cell(sem_cell, NOCHANGED, NO_BARRIER, dml_type);
                    if (OB_SUCCESS != err)
                    {
                      TBSYS_LOG(WARN, "failed to add_cell, err=%d", err);
                    }
                  }
                  break;

                case ObActionFlag::TABLE_NAME_FIELD:
                  // deserialize table name
                  err = tmp_obj.deserialize(buf, buf_len, pos);
                  if (OB_SUCCESS == err)
                  {
                    if (ObIntType == tmp_obj.get_type())
                    {
                      int64_t table_id = 0;
                      tmp_obj.get_int(table_id);
                      cur_cell.cell_info.table_id_ = table_id;
                      cur_cell.cell_info.table_name_.assign(NULL, 0);
                    }
                    else if (ObVarcharType == tmp_obj.get_type())
                    {
                      tmp_obj.get_varchar(tmp_str);
                      cur_cell.cell_info.table_name_ = tmp_str;
                      cur_cell.cell_info.table_id_ = 0;
                    }
                    else
                    {
                      TBSYS_LOG(WARN, "invalid obj type, type=%d", tmp_obj.get_type());
                      err = OB_ERROR;
                    }

                    if (OB_SUCCESS == err && NULL != schema_manager_)
                    {
                      get_rowkey_info_from_sm(schema_manager_, cur_cell.cell_info.table_id_,
                          cur_cell.cell_info.table_name_, rowkey_info_);
                    }

                    /*
                    TBSYS_LOG(INFO, "get_rowkey_info, table_id=%lu, table_name=%s, rowkey_info_size=%ld, err=%d, schema_manager_=%p",
                        cur_cell.cell_info.table_id_, to_cstring(cur_cell.cell_info.table_name_),
                        rowkey_info_.get_size(), err, schema_manager_);
                        */
                  }
                  is_row_changed = true;
                  break;

                case ObActionFlag::ROW_KEY_FIELD:
                  // deserialize row key
                  if (rowkey_info_.get_size() <= 0)
                  {
                    TBSYS_LOG(WARN, "rowkey_info should be set, rowkey_info_size=%ld", rowkey_info_.get_size());
                    err = OB_NOT_SUPPORTED;
                  }
                  else
                  {
                    // deserialize row key
                    err = tmp_obj.deserialize(buf, buf_len, pos);
                    if (OB_SUCCESS == err)
                    {
                      tmp_obj.get_varchar(tmp_str);
                      err = ObRowkeyHelper::binary_rowkey_to_obj_array(rowkey_info_, tmp_str, rowkey_col_buf, rowkey_info_.get_size());
                    }
                    if (OB_SUCCESS != err)
                    {
                      TBSYS_LOG(WARN, "binary_rowkey_to_obj_array_fail err=%d binary=[%s]", err, to_cstring(tmp_str));
                    }
                    else
                    {
                      cur_cell.cell_info.row_key_.assign(rowkey_col_buf, rowkey_info_.get_size());
                    }
                  }
                  is_row_changed = true;
                  break;

                case ObActionFlag::FORMED_ROW_KEY_FIELD:
                  cur_cell.cell_info.row_key_.assign(rowkey_col_buf, OB_MAX_ROWKEY_COLUMN_NUMBER);
                  err = cur_cell.cell_info.row_key_.deserialize(buf, buf_len, pos);
                  is_row_changed = true;
                  break;

                case ObActionFlag::OP_DEL_ROW:
                case ObActionFlag::OP_TRUN_TAB: //add zhaoqiong [Truncate Table]:20160318
                  cur_cell.cell_info.column_name_.assign(NULL, 0);
                  cur_cell.cell_info.column_id_ = common::OB_INVALID_ID;
                  cur_cell.cell_info.value_.reset();
                  //mod zhaoqiong [Truncate Table]:20160318:b
                  //cur_cell.cell_info.value_.set_ext(ObActionFlag::OP_DEL_ROW);
                  cur_cell.cell_info.value_.set_ext(ext_val);
                  //mod:e

                  //add zhaoqiong [bugfix:: illegal op type of DEL_ROW MUTATOR]:20160615:b
                  cur_cell.op_type.set_ext(common::ObActionFlag::OP_UPDATE);
                  //add:e
                  err = add_cell(cur_cell, is_row_changed ? CHANGED : NOCHANGED, NO_BARRIER, dml_type);
                  is_row_changed = false;
                  if (OB_SUCCESS != err)
                  {
                    TBSYS_LOG(WARN, "failed to add cell, err=%d", err);
                  }
                  break;

                case ObActionFlag::UPDATE_COND_PARAM_FIELD:
                  err = OB_NOT_SUPPORTED;
                  TBSYS_LOG(ERROR, "not support UPDATE_COND_PARAM_FIELD[%ld]", ext_val);
                  break;
                case ObActionFlag::PREFETCH_PARAM_FIELD:
                  err = OB_NOT_SUPPORTED;
                  TBSYS_LOG(ERROR, "not support PREFETCH_PARAM_FIELD[%ld]", ext_val);
                  break;
                case ObActionFlag::END_PARAM_FIELD:
                  end_flag = true;
                  break;
                case ObActionFlag::DML_TYPE_FIELD:
                  err = tmp_obj.deserialize(buf, buf_len, pos);
                  if (OB_SUCCESS == err)
                  {
                    err = tmp_obj.get_int((int64_t&)dml_type);
                    if (OB_SUCCESS != err)
                    {
                      TBSYS_LOG(WARN, "failed to get int for mutator type, err=%d", err);
                    }
                  }
                  break;
                default:
                  TBSYS_LOG(WARN, "unknown format, omit it:type[%ld]", ext_val);
                  do
                  {
                    int64_t old_pos = pos;
                    do
                    {
                      old_pos = pos;
                      err = tmp_obj.deserialize(buf, buf_len, pos);
                    } while (OB_SUCCESS == err && ObExtendType != tmp_obj.get_type());
                    pos = old_pos;
                  } while (0);
                  break;
              }
            }
          }
        }
      }

      return err;
    }

    int64_t ObMutator :: get_serialize_size(void) const
    {
      int64_t store_size = cell_store_size_ + get_obj_serialize_size_(ObActionFlag::MUTATOR_PARAM_FIELD, true)
        + get_obj_serialize_size_(ObActionFlag::END_PARAM_FIELD, true);
      store_size += get_obj_serialize_size_(ObActionFlag::MUTATOR_TYPE_FIELD, true) + get_obj_serialize_size_(type_, false);
      return store_size;
    }

    int ObMutator :: copy_cell_(const ObMutatorCellInfo& src_cell, ObMutatorCellInfo& dst_cell,
                                RowChangedStat row_changed_stat, int64_t& store_size)
    {
      int err = OB_SUCCESS;
      store_size = 0;

      int64_t ext_val = 0;
      int64_t type = src_cell.cell_info.value_.get_type();
      if (ObExtendType == type)
      {
        src_cell.cell_info.value_.get_ext(ext_val);
      }

      if (ObExtendType == type
          && (ObActionFlag::OP_USE_OB_SEM == ext_val
            || ObActionFlag::OP_USE_DB_SEM == ext_val))
      {
        dst_cell.cell_info.reset();
        dst_cell.cell_info.value_.reset();
        dst_cell.cell_info.value_.set_ext(ext_val);
        store_size += (get_obj_serialize_size_(ObActionFlag::OBDB_SEMANTIC_FIELD, true)
            + dst_cell.cell_info.value_.get_serialize_size());
      }
      else
      {
        bool table_changed = false;
        // store table name or table id
        if (0 != src_cell.cell_info.table_name_.length())
        {
          if (USE_ID == id_name_type_)
          {
            TBSYS_LOG(WARN, "invalid status, should use name");
            err = OB_ERROR;
          }
          else
          {
            id_name_type_ = USE_NAME;
            dst_cell.cell_info.table_id_ = common::OB_INVALID_ID;
            // store table name
            if (0 == src_cell.cell_info.table_name_.compare(last_table_name_))
            {
              dst_cell.cell_info.table_name_ = last_table_name_;
            }
            else
            {
              table_changed = true;
              err = str_buf_.write_string(src_cell.cell_info.table_name_,
                  &(dst_cell.cell_info.table_name_));
              if (OB_SUCCESS != err)
              {
                TBSYS_LOG(WARN, "failed to store table name, err=%d", err);
              }
              else
              {
                store_size += (get_obj_serialize_size_(ObActionFlag::TABLE_NAME_FIELD, true)
                    + get_obj_serialize_size_(dst_cell.cell_info.table_name_));
              }
            }
          }
        }
        else
        {
          if (USE_NAME == id_name_type_)
          {
            TBSYS_LOG(WARN, "invalid status, should use id");
            err = OB_ERROR;
          }
          else
          {
            id_name_type_ = USE_ID;
            dst_cell.cell_info.table_name_.assign(NULL, 0);
            // store table id
            if (last_table_id_ == src_cell.cell_info.table_id_)
            {
              dst_cell.cell_info.table_id_ = last_table_id_;
            }
            else
            {
              table_changed = true;
              dst_cell.cell_info.table_id_ = src_cell.cell_info.table_id_;
              store_size += (get_obj_serialize_size_(ObActionFlag::TABLE_NAME_FIELD, true)
                  + get_obj_serialize_size_(dst_cell.cell_info.table_id_, false));
            }
          }
        }

        // store row key
        if (OB_SUCCESS == err)
        {
          if (NOCHANGED == row_changed_stat)
          {
            dst_cell.cell_info.row_key_ = last_row_key_;
          }
          else if (!table_changed && CHANGED_UNKNOW == row_changed_stat
                  && 0 == src_cell.cell_info.row_key_.compare(last_row_key_))
          {
            dst_cell.cell_info.row_key_ = last_row_key_;
          }
          else
          {
            err = str_buf_.write_string(src_cell.cell_info.row_key_,
                &(dst_cell.cell_info.row_key_));
            if (OB_SUCCESS != err)
            {
              TBSYS_LOG(WARN, "failed to store row key, err=%d", err);
            }
            else
            {
              store_size += (get_obj_serialize_size_(ObActionFlag::ROW_KEY_FIELD, true)
                  + get_obj_serialize_size_(dst_cell.cell_info.row_key_));
            }
          }
        }

        if (OB_SUCCESS != err)
        {
          // does nothing
        }
        else if (ObExtendType == type && ObActionFlag::OP_DEL_ROW == ext_val)
        {
          // delete row
          dst_cell.cell_info.column_id_ = common::OB_INVALID_ID;
          dst_cell.cell_info.column_name_.assign(NULL, 0);
          dst_cell.cell_info.value_.reset();
          dst_cell.cell_info.value_.set_ext(ext_val);
          //add zhaoqiong [bugfix:: illegal op type of DEL_ROW MUTATOR]:20160615:b
          dst_cell.op_type = src_cell.op_type;
          //add:e
          store_size += get_obj_serialize_size_(ObActionFlag::OP_DEL_ROW, true);
        }
        else
        {
          // store column name
          if (0 != src_cell.cell_info.column_name_.length())
          {
            if (USE_ID == id_name_type_)
            {
              TBSYS_LOG(WARN, "invalid status, should use name");
              err = OB_ERROR;
            }
            else
            {
              id_name_type_ = USE_NAME;
              dst_cell.cell_info.column_id_ = common::OB_INVALID_ID;

              err = str_buf_.write_string(src_cell.cell_info.column_name_,
                  &(dst_cell.cell_info.column_name_));
              if (OB_SUCCESS != err)
              {
                TBSYS_LOG(WARN, "failed to store column name, err=%d", err);
              }
              else
              {
                store_size += get_obj_serialize_size_(dst_cell.cell_info.column_name_);
              }
            }
          }
          else
          {
            if (USE_NAME == id_name_type_)
            {
              TBSYS_LOG(WARN, "invalid status, should use id");
              err = OB_ERROR;
            }
            else
            {
              id_name_type_ = USE_ID;
              dst_cell.cell_info.column_name_.assign(NULL, 0);
              //store column id
              dst_cell.cell_info.column_id_ = src_cell.cell_info.column_id_;
              store_size += get_obj_serialize_size_(dst_cell.cell_info.column_id_, false);
            }
          }

          // store op type
          if (OB_SUCCESS == err)
          {
            dst_cell.op_type = src_cell.op_type;
            store_size += dst_cell.op_type.get_serialize_size();
          }

          if (OB_SUCCESS == err)
          {
            // store column value
            err = str_buf_.write_obj(src_cell.cell_info.value_, &(dst_cell.cell_info.value_));
            if (OB_SUCCESS != err)
            {
              TBSYS_LOG(WARN, "failed to store column value, err=%d", err);
            }
            else
            {
              store_size += dst_cell.cell_info.value_.get_serialize_size();
            }
          }
        }

        if (OB_SUCCESS == err)
        {
          last_table_name_ = dst_cell.cell_info.table_name_;
          last_row_key_ = dst_cell.cell_info.row_key_;
          last_table_id_ = dst_cell.cell_info.table_id_;
        }
      }

      return err;
    }

    int ObMutator :: add_node_(CellInfoNode* cur_node)
    {
      int err = OB_SUCCESS;

      if (NULL == cur_node)
      {
        TBSYS_LOG(WARN, "invalid param, cur_node=%p", cur_node);
        err = OB_INVALID_ARGUMENT;
      }
      else
      {
        if (NULL == list_head_)
        {
          list_head_ = cur_node;
          list_tail_ = cur_node;
        }
        else
        {
          if (NOCHANGED == cur_node->row_changed_stat)
          {
            list_tail_->row_finished_stat = NOFINISHED;
          }
          else if (CHANGED == cur_node->row_changed_stat)
          {
            list_tail_->row_finished_stat = FINISHED;
          }
          list_tail_->next = cur_node;
          list_tail_ = cur_node;
        }
      }

      return err;
    }

    int ObMutator::serialize_flag_(char* buf, const int64_t buf_len, int64_t& pos,
        const int64_t flag) const
    {
      int err = OB_SUCCESS;

      ObObj obj;
      obj.set_ext(flag);

      err = obj.serialize(buf, buf_len, pos);

      return err;
    }

    int64_t ObMutator::get_obj_serialize_size_(const int64_t value, bool is_ext) const
    {
      ObObj obj;
      if (is_ext)
      {
        obj.set_ext(value);
      }
      else
      {
        obj.set_int(value);
      }

      return obj.get_serialize_size();
    }

    int64_t ObMutator::get_obj_serialize_size_(const ObString& str) const
    {
      ObObj obj;
      obj.set_varchar(str);

      return obj.get_serialize_size();
    }

    int64_t ObMutator::get_obj_serialize_size_(const ObRowkey& rowkey) const
    {
      return rowkey.get_serialize_size();
    }
  }
}

