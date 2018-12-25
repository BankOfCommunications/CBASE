/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * ob_write_worker.cpp for define write worker thread.
 *
 * Authors:
 *   huating <huating.zmq@taobao.com>
 *
 */
#include "common/ob_malloc.h"
#include "common/ob_mutator.h"
#include "ob_write_worker.h"

namespace oceanbase
{
  namespace syschecker
  {
    using namespace tbsys;
    using namespace common;
    using namespace client;

    ObWriteWorker::ObWriteWorker(ObClient& client, ObSyscheckerRule& rule,
                                 ObSyscheckerStat& stat)
    : client_(client), write_rule_(rule), stat_(stat)
    {

    }

    ObWriteWorker::~ObWriteWorker()
    {

    }

    int ObWriteWorker::init(ObOpParam** write_param)
    {
      int ret = OB_SUCCESS;

      if (NULL != write_param && NULL == *write_param)
      {
        *write_param = reinterpret_cast<ObOpParam*>(ob_malloc(sizeof(ObOpParam), ObModIds::TEST));
        if (NULL == *write_param)
        {
          TBSYS_LOG(ERROR, "failed to allocate memory for write param");
          ret = OB_ERROR;
        }
        else
        {
          memset(*write_param, 0, sizeof(ObOpParam));
        }
      }

      return ret;
    }

    void ObWriteWorker::set_obj(const ObOpCellParam& cell_param, ObObj& obj)
    {
      ObString varchar;

      switch (cell_param.cell_type_)
      {
      case ObNullType:
        obj.set_null();
        break;
      case ObIntType:
        obj.set_int(cell_param.value_.int_val_);
        break;
      case ObFloatType:
        obj.set_float(cell_param.value_.float_val_);
        break;
      case ObDoubleType:
        obj.set_double(cell_param.value_.double_val_);
        break;
      case ObDateTimeType:
        obj.set_datetime(cell_param.value_.time_val_);
        break;
      case ObPreciseDateTimeType:
        obj.set_precise_datetime(cell_param.value_.precisetime_val_);
        break;
      case ObVarcharType:
        varchar.assign(const_cast<char*>(cell_param.value_.varchar_val_),
                       cell_param.varchar_len_);
        obj.set_varchar(varchar);
        break;
      case ObSeqType:
      case ObCreateTimeType:
      case ObModifyTimeType:
      case ObExtendType:
      case ObMaxType:
      case ObMinType:
        break;
      default:
        TBSYS_LOG(WARN, "unknown object type %s",
                  OBJ_TYPE_STR[cell_param.cell_type_]);
        break;
      }
    }

    int ObWriteWorker::set_mutator_add(const ObOpParam& write_param,
                                       ObMutator& mutator,
                                       const ObOpCellParam& cell_param,
                                       const ObRowkey& row_key)
    {
      int ret = OB_SUCCESS;

      switch (cell_param.cell_type_)
      {
      case ObIntType:
        ret = mutator.add(write_param.table_name_, row_key,
                          cell_param.column_name_,
                          cell_param.value_.int_val_);
        break;
      case ObFloatType:
        ret = mutator.add(write_param.table_name_, row_key,
                          cell_param.column_name_,
                          cell_param.value_.float_val_);
        break;
      case ObDoubleType:
        ret = mutator.add(write_param.table_name_, row_key,
                          cell_param.column_name_,
                          cell_param.value_.double_val_);
        break;
      case ObDateTimeType:
        ret = mutator.add_datetime(write_param.table_name_, row_key,
                                   cell_param.column_name_,
                                   cell_param.value_.int_val_);
        break;
      case ObPreciseDateTimeType:
        ret = mutator.add_precise_datetime(write_param.table_name_, row_key,
                                           cell_param.column_name_,
                                           cell_param.value_.int_val_);
        break;
      case ObVarcharType:
        /**
         * try to add varchar, must fail, and it can't affect the data
         * consistence, we just add some illegal value
         */
      case ObCreateTimeType:
        /**
         * try to add ObCreateTimeType, must fail, and it can't affect
         * the data consistence, we just add some illegal value
         */
      case ObModifyTimeType:
        /**
         * try to add ObModifyTimeType, must fail, and it can't affect
         * the data consistence, we just add some illegal value
         */
        ret = mutator.add(write_param.table_name_, row_key,
                          cell_param.column_name_,
                          cell_param.value_.int_val_);
        break;
      case ObSeqType:
      case ObExtendType:
      case ObMaxType:
      case ObMinType:
      case ObNullType:
        TBSYS_LOG(WARN, "wrong addable object type %s",
                  OBJ_TYPE_STR[cell_param.cell_type_]);
        ret = OB_ERROR;
        break;
      default:
        TBSYS_LOG(WARN, "unknown object type %s",
                  OBJ_TYPE_STR[cell_param.cell_type_]);
        ret = OB_ERROR;
        break;
      }

      return ret;
    }

    int ObWriteWorker::run_mix_op(const ObOpParam& write_param,
                                  ObMutator& mutator)
    {
      int ret                   = OB_SUCCESS;
      int64_t delete_row_count  = 0;
      int64_t add_cell_count    = 0;
      int64_t update_cell_count = 0;
      int64_t insert_cell_count = 0;
      const ObOpRowParam* row_param   = NULL;
      const ObOpCellParam* cell_param = NULL;
      int64_t start_time = 0;
      int64_t consume_time = 0;
      ObRowkey row_key;
      ObObj obj;

      mutator.reset();

      //build mutator
      for (int64_t i = 0; i < write_param.row_count_ && OB_SUCCESS == ret; ++i)
      {
        row_param = &write_param.row_[i];
        row_key.assign(const_cast<ObObj*>(row_param->rowkey_), row_param->rowkey_len_);
        if (OP_DELETE == row_param->op_type_)
        {
          delete_row_count++;
          ret = mutator.del_row(write_param.table_name_, row_key);
          continue;
        }

        for (int64_t j = 0; j < row_param->cell_count_ && OB_SUCCESS == ret; ++j)
        {
          obj.reset();
          cell_param = &row_param->cell_[j];
          if (OP_ADD == cell_param->op_type_)
          {
            add_cell_count++;
            ret = set_mutator_add(write_param, mutator, *cell_param, row_key);
          }
          else if (OP_UPDATE == cell_param->op_type_)
          {
            update_cell_count++;
            set_obj(*cell_param, obj);
            ret = mutator.update(write_param.table_name_, row_key,
                                 cell_param->column_name_, obj);
          }
          else if (OP_INSERT == cell_param->op_type_)
          {
            insert_cell_count++;
            set_obj(*cell_param, obj);
            ret = mutator.insert(write_param.table_name_, row_key,
                                 cell_param->column_name_, obj);
          }
        }
      }

      if (OB_SUCCESS == ret)
      {
        start_time = tbsys::CTimeUtil::getTime();
        ret = client_.ups_apply(mutator);
        consume_time = tbsys::CTimeUtil::getTime() - start_time;
        stat_.record_resp_event(CMD_UPDATE, consume_time, (OB_SUCCESS != ret));
      }

      //update stat
      switch (write_param.op_type_)
      {
      case OP_MIX:
        stat_.add_mix_opt(1);
        break;
      case OP_ADD:
        stat_.add_add_opt(1);
        break;
      case OP_UPDATE:
        stat_.add_update_opt(1);
        break;
      case OP_INSERT:
        stat_.add_insert_opt(1);
        break;
      case OP_DELETE:
        stat_.add_delete_row_opt(1);
        break;
      default:
        break;
      }
      stat_.add_write_cell(delete_row_count + add_cell_count
                           + update_cell_count + insert_cell_count);
      stat_.add_delete_row(delete_row_count);
      stat_.add_add_cell(add_cell_count);
      stat_.add_update_cell(update_cell_count);
      stat_.add_insert_cell(insert_cell_count);
      if (OB_SUCCESS != ret)
      {
        stat_.add_write_cell_fail(delete_row_count + add_cell_count
                                  + update_cell_count + insert_cell_count);
        stat_.add_delete_row_fail(delete_row_count);
        stat_.add_add_cell_fail(add_cell_count);
        stat_.add_update_cell_fail(update_cell_count);
        stat_.add_insert_cell_fail(insert_cell_count);
      }

      //check result
      if (OB_RESPONSE_TIME_OUT == ret)
      {
        //modify timeout,skip it
        ret = OB_SUCCESS;
      }
      else if (OB_SUCCESS == ret
          && GEN_INVALID_WRITE == write_param.param_gen_.gen_cell_
          && OP_DELETE != write_param.op_type_)
      {
        TBSYS_LOG(WARN, "run invalid operation, expect fail, but success, "
                        "table_name=%.*s, is_wide_table=%s, op_type=%s, "
                        "row_count=%ld, ret=%d",
                  write_param.table_name_.length(), write_param.table_name_.ptr(),
                  write_param.is_wide_table_ ? "true" : "false",
                  OP_TYPE_STR[write_param.op_type_], write_param.row_count_, ret);
        ret = OB_ERROR;
      }
      else if (OB_SUCCESS != ret
               && GEN_VALID_WRITE == write_param.param_gen_.gen_cell_)
      {
        TBSYS_LOG(WARN, "run valid operation, expect success, but fail, "
                        "table_name=%.*s, is_wide_table=%s, op_type=%s, "
                        "row_count=%ld, ret=%d",
                  write_param.table_name_.length(), write_param.table_name_.ptr(),
                  write_param.is_wide_table_ ? "true" : "false",
                  OP_TYPE_STR[write_param.op_type_], write_param.row_count_, ret);
        ret = OB_ERROR;
      }
      else
      {
        ret = OB_SUCCESS;
      }

      return ret;
    }

    void ObWriteWorker::run(CThread* thread, void* arg)
    {
      int err                 = OB_SUCCESS;
      ObOpParam* write_param  = NULL;
      ObMutator mutator;
      UNUSED(thread);
      UNUSED(arg);

      if (OB_SUCCESS == init(&write_param) && NULL != write_param)
      {
        while (!_stop)
        {
          err = write_rule_.get_next_write_param(*write_param);
          if (OB_SUCCESS == err)
          {
            if (write_param->row_count_ <= 0)
            {
              continue;
            }

            err = run_mix_op(*write_param, mutator);
            if (OB_SUCCESS != err)
            {
              TBSYS_LOG(WARN, "failed to run mixed operation");
              write_param->display();
              break;
            }
          }
          else
          {
            TBSYS_LOG(WARN, "get next write param failed");
          }
        }
      }

      if (NULL != write_param)
      {
        ob_free(write_param);
        write_param = NULL;
      }
    }
  } // end namespace syschecker
} // end namespace oceanbase
