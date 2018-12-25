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
#include "ob_async_log_applier.h"
#include "ob_ups_log_mgr.h"
#include "ob_lock_mgr.h"
#include "ob_trans_executor.h"
#include "ob_session_guard.h"
#include "ob_ups_utils.h"

namespace oceanbase
{
  namespace updateserver
  {
    int64_t ReplayTaskProfile::to_string(char* buf, int64_t len) const
    {
      return snprintf(buf, len, "log: %ld[%s], submit: %ld, aqueue: %ld, apply: %ld, cqueue: %ld, commit: %ld, flush: %ld",
                      host_.log_id_, ::to_cstring(host_.trans_id_),
                      submit_time_us_, start_apply_time_us_ - submit_time_us_, end_apply_time_us_ - start_apply_time_us_,
                      start_commit_time_us_ - end_apply_time_us_, end_commit_time_us_ - start_commit_time_us_,
                      end_flush_time_us_ - start_flush_time_us_);
    }

    ObLogTask::ObLogTask(): log_id_(0), barrier_log_id_(0),
                            log_entry_(), log_data_(NULL), batch_buf_(NULL), batch_buf_len_(0),
                            trans_id_(), mutation_ts_(0),
                            checksum_before_mutate_(0), checksum_after_mutate_(0),
                            replay_type_(RT_LOCAL), profile_(*this)
    {
    }

    ObLogTask::~ObLogTask()
    {}

    void ObLogTask::reset()
    {
      log_id_ = 0;
      barrier_log_id_ = 0;
      new(&log_entry_)ObLogEntry;
      log_data_ = NULL;
      batch_buf_ = NULL;
      batch_buf_len_ = 0;
      trans_id_.reset();
      mutation_ts_ = 0;
      checksum_before_mutate_ = 0;
      checksum_after_mutate_ = 0;
      replay_type_ = RT_LOCAL;
      new(&profile_)ReplayTaskProfile(*this);
      //add shili [LONG_TRANSACTION_LOG]  20160926:b
      is_big_log_completed_ = false;
      //add e
    }

    ObAsyncLogApplier::ObAsyncLogApplier(): inited_(false),
                                            retry_wait_time_us_(REPLAY_RETRY_WAIT_TIME_US),
                                            trans_executor_(NULL), session_mgr_(NULL),
                                            lock_mgr_(NULL), table_mgr_(NULL), log_mgr_(NULL)
    {
    }

    ObAsyncLogApplier::~ObAsyncLogApplier()
    {}

    int ObAsyncLogApplier::init(TransExecutor* trans_executor, SessionMgr* session_mgr,
                                LockMgr* lock_mgr, ObUpsTableMgr* table_mgr, ObUpsLogMgr* log_mgr)
    {
      int err = OB_SUCCESS;
      if (NULL == trans_executor || NULL == session_mgr || NULL == lock_mgr || NULL == table_mgr || NULL == log_mgr)
      {
        err = OB_INVALID_ARGUMENT;
      }
      else if (NULL != trans_executor_ || NULL != session_mgr_ || NULL != lock_mgr_ || NULL != table_mgr_ || NULL != log_mgr_)
      {
        err = OB_INIT_TWICE;
      }
      else
      {
        trans_executor_ = trans_executor;
        session_mgr_ = session_mgr;
        lock_mgr_ = lock_mgr;
        table_mgr_ = table_mgr;
        log_mgr_ = log_mgr;
        inited_ = true;
      }
      return err;
    }

    bool ObAsyncLogApplier::is_inited() const
    {
      return inited_;
    }

    int ObAsyncLogApplier::on_submit(ObLogTask& task)
    {
      int err = OB_SUCCESS;
      task.profile_.on_submit();
      return err;
    }

    int ObAsyncLogApplier::start_transaction(ObLogTask& task)
    {
      int err = OB_SUCCESS;
      UNUSED(task);
      if (!is_inited())
      {
        err = OB_NOT_INIT;
      }
      else
      {}
      return err;
    }


    //add shili [LONG_TRANSACTION_LOG]  20160926:b
    /* @berif  将buf 反序列化成mutator,并将mutator 应用到 memtable 中
     * @param    buf   [in]  mutator 反序列化之前的字节流
     * @param    data_size  [in] mutator 反序列化之前的字节流长度
     * @param    task  [out] 设置用于提交时 参数
     */
    int ObAsyncLogApplier::handle_normal_mutator(const char *buf, const int32_t data_size, ObLogTask &task)
    {
      int err = OB_SUCCESS;
      int64_t pos = 0;
      RPSessionCtx *session_ctx = NULL;
      SessionGuard session_guard(*session_mgr_, *lock_mgr_, err);
      ObTransReq req;
      req.type_ = REPLAY_TRANS;
      req.isolation_ = NO_LOCK;
      req.start_time_ = tbsys::CTimeUtil::getTime();
      req.timeout_ = INT64_MAX;
      req.idle_time_ = INT64_MAX;
      task.trans_id_.reset();

      if (OB_SUCCESS != (err = session_guard.start_session(req, task.trans_id_, session_ctx)))
      {
        if (OB_BEGIN_TRANS_LOCKED == err)
        {
          err = OB_EAGAIN;
          TBSYS_LOG(TRACE, "begin session fail: TRANS_LOCKED, log_id=%ld", task.log_id_);
        }
        else
        {
          TBSYS_LOG(WARN, "begin session fail ret=%d, log_id=%ld", err, task.log_id_);
        }
      }
      else if (OB_SUCCESS != (err = session_ctx->get_ups_mutator().deserialize(buf,(int64_t)data_size, pos)))
      {
        TBSYS_LOG(ERROR, "ups_mutator.deserialize(buf[%p:%d])", buf, data_size);
      }
      else
      {
        const bool using_id = true;
        ObUpsMutator& mutator = session_ctx->get_ups_mutator();
        task.mutation_ts_ = mutator.get_mutate_timestamp();
        task.checksum_before_mutate_ = mutator.get_memtable_checksum_before_mutate();
        task.checksum_after_mutate_ = mutator.get_memtable_checksum_after_mutate();
        session_ctx->set_replay_local_log(RT_LOCAL == task.replay_type_);
        session_ctx->set_trans_id(mutator.get_mutate_timestamp());
        tc_is_replaying_log() = true;
        if (OB_SUCCESS != (err = table_mgr_->apply(using_id, *session_ctx,
                                                   *session_ctx->get_lock_info(),
                                                   mutator.get_mutator())))
        {
          TBSYS_LOG(DEBUG, "apply err=%d trans_id=%s, checksum=%lu",
                    err, to_cstring(task.trans_id_), session_ctx->get_uc_info().uc_checksum);
        }
        else
        {
          int64_t mutate_ts = mutator.get_mutate_timestamp();
          session_ctx->get_uc_info().uc_checksum = ob_crc64(session_ctx->get_uc_info().uc_checksum,
                                                            &mutate_ts, sizeof(mutate_ts));
        }
        tc_is_replaying_log() = false;
      }
      return err;
    }
    //add e

    int ObAsyncLogApplier::handle_normal_mutator(ObLogTask& task)
    {
      int err = OB_SUCCESS;
      const char* log_data = task.log_data_;
      const int64_t data_len = task.log_entry_.get_log_data_len();
      int64_t pos = 0;
      RPSessionCtx *session_ctx = NULL;
      SessionGuard session_guard(*session_mgr_, *lock_mgr_, err);
      ObTransReq req;
      req.type_ = REPLAY_TRANS;
      req.isolation_ = NO_LOCK;
      req.start_time_ = tbsys::CTimeUtil::getTime();
      req.timeout_ = INT64_MAX;
      req.idle_time_ = INT64_MAX;
      task.trans_id_.reset();

      if (OB_SUCCESS != (err = session_guard.start_session(req, task.trans_id_, session_ctx)))
      {
        if (OB_BEGIN_TRANS_LOCKED == err)
        {
          err = OB_EAGAIN;
          TBSYS_LOG(TRACE, "begin session fail: TRANS_LOCKED, log_id=%ld", task.log_id_);
        }
        else
        {
          TBSYS_LOG(WARN, "begin session fail ret=%d, log_id=%ld", err, task.log_id_);
        }
      }
      else if (OB_SUCCESS != (err = session_ctx->get_ups_mutator().deserialize(log_data, data_len, pos)))
      {
        TBSYS_LOG(ERROR, "ups_mutator.deserialize(buf[%p:%ld])", log_data, data_len);
      }
      else
      {
        const bool using_id = true;
        ObUpsMutator& mutator = session_ctx->get_ups_mutator();
        task.mutation_ts_ = mutator.get_mutate_timestamp();
        task.checksum_before_mutate_ = mutator.get_memtable_checksum_before_mutate();
        task.checksum_after_mutate_ = mutator.get_memtable_checksum_after_mutate();
        session_ctx->set_replay_local_log(RT_LOCAL == task.replay_type_);
        session_ctx->set_trans_id(mutator.get_mutate_timestamp());
        tc_is_replaying_log() = true;
        if (OB_SUCCESS != (err = table_mgr_->apply(using_id, *session_ctx,
                                                   *session_ctx->get_lock_info(),
                                                   mutator.get_mutator())))
        {
          TBSYS_LOG(DEBUG, "apply err=%d trans_id=%s, checksum=%lu",
                    err, to_cstring(task.trans_id_), session_ctx->get_uc_info().uc_checksum);
        }
        else
        {
          int64_t mutate_ts = mutator.get_mutate_timestamp();
          session_ctx->get_uc_info().uc_checksum = ob_crc64(session_ctx->get_uc_info().uc_checksum,
                                                            &mutate_ts, sizeof(mutate_ts));
        }
        tc_is_replaying_log() = false;
      }
      return err;
    }

    bool ObAsyncLogApplier::is_memory_warning()
    {
      bool bret = false;
      TableMemInfo mi;
      table_mgr_->get_memtable_memory_info(mi);
      int64_t memory_available = mi.memtable_limit - mi.memtable_total + (MemTank::REPLAY_MEMTABLE_RESERVE_SIZE_GB << 30);
      if (get_table_available_warn_size() > memory_available)
      {
        ups_available_memory_warn_callback(memory_available);
        if (memory_available <= 0)
        {
          bret = true;
        }
      }
      return bret;
    }

    int ObAsyncLogApplier::apply(ObLogTask& task)
    {
      int err = OB_SUCCESS;
      ObUpsMutator *mutator = GET_TSI_MULT(ObUpsMutator, 1);
      CommonSchemaManagerWrapper *schema = GET_TSI_MULT(CommonSchemaManagerWrapper, 1);
      const char* log_data = task.log_data_;
      const int64_t data_len = task.log_entry_.get_log_data_len();
      LogCommand cmd = (LogCommand)task.log_entry_.cmd_;
      int64_t pos = 0;
      int64_t file_id = 0;
      task.profile_.start_apply();
      task.mutation_ts_ = tbsys::CTimeUtil::getTime();
      ObBigLogWriter * big_log_writer = log_mgr_->get_big_log_writer();//add shili [LONG_TRANSACTION_LOG]  20160926
      if (!is_inited())
      {
        err = OB_NOT_INIT;
      }
      else if (NULL == mutator || NULL == schema)
      {
        err = OB_ALLOCATE_MEMORY_FAILED;
      }
      else if (is_memory_warning())
      {
        if (TC_REACH_TIME_INTERVAL(MEM_OVERFLOW_REPORT_INTERVAL))
        {
          TBSYS_LOG(WARN, "no memory, sleep %ldus wait for dumping sstable", retry_wait_time_us_);
        }
        err = OB_EAGAIN;
        usleep(static_cast<useconds_t>(retry_wait_time_us_));
      }
      else
      {
        //add zhaoqiong [Schema Manager] 20150327:b
        //schema from TSI, the attributes are not the default values
        //should reset first
        schema->reset();
        ObSchemaMutator schema_mutator;
        //add:e
        switch(cmd)
        {
          case OB_LOG_UPS_MUTATOR:
            if (OB_SUCCESS != (err = mutator->deserialize_header(log_data, data_len, pos)))
            {
              TBSYS_LOG(ERROR, "UpsMutator deserialize error[ret=%d log_data=%p data_len=%ld]", err, log_data, data_len);
            }
            //mod zhaoqiong [Truncate Table]:20170519
//            else if (mutator->is_normal_mutator())
            else if (mutator->is_normal_mutator()||mutator->is_truncate_mutator())
            //mod:e
            {
              if (OB_SUCCESS != (err = handle_normal_mutator(task)))
              {
                TBSYS_LOG(WARN, "fail to handle normal mutator. err=%d", err);
              }
            }
            else
            {
              pos = 0;
              if (OB_SUCCESS != (err = mutator->deserialize(log_data, data_len, pos)))
              {
                TBSYS_LOG(ERROR, "mutator->deserialize(buf=%p[%ld])", log_data, data_len);
              }
              else if (OB_SUCCESS != (err = table_mgr_->replay_mgt_mutator(*mutator, task.replay_type_)))
              {
                TBSYS_LOG(ERROR, "replay_mgt_mutator(replay_type=%d)=>%d", task.replay_type_, err);
              }
            }
            if (OB_SUCCESS != err)
            {
              TBSYS_LOG(WARN, "table_mgr->replay(log_cmd=%d, data=%p[%ld])=>%d", cmd, log_data, data_len, err);
            }
            break;
          case OB_UPS_SWITCH_SCHEMA:
            if (OB_SUCCESS != (err = schema->deserialize(log_data, data_len, pos)))
            {
              TBSYS_LOG(ERROR, "ObSchemaManagerWrapper deserialize error[ret=%d log_data=%p data_len=%ld]",
                        err, log_data, data_len);
            }
            else if (OB_SUCCESS != (err = table_mgr_->set_schemas(*schema)))
            {
              TBSYS_LOG(ERROR, "UpsTableMgr set_schemas error, ret=%d schema_version=%ld", err, schema->get_version());
            }
            else
            {
              TBSYS_LOG(INFO, "switch schema succ");
            }
            break;
          //add zhaoqiong [Schema Manager] 20150327:b
          case OB_UPS_WRITE_SCHEMA_NEXT:
            if (OB_SUCCESS != (err = table_mgr_->replay_mgt_next(log_data, data_len, pos, task.replay_type_)))
            {
              TBSYS_LOG(ERROR, "UpsTableMgr deserialize_schema_next error");
            }
            else
            {
             TBSYS_LOG(INFO, "deserialize_schema_next succ");
            }
            break;
          case OB_UPS_SWITCH_SCHEMA_NEXT:
            if (OB_SUCCESS != (err = table_mgr_->set_schema_next(log_data, data_len, pos)))
            {
              TBSYS_LOG(ERROR, "UpsTableMgr deserialize_schema_next error");
            }
            else
            {
              TBSYS_LOG(INFO, "deserialize_schema_next succ");
            }
            break;
          case OB_UPS_SWITCH_SCHEMA_MUTATOR:
            if (OB_SUCCESS != (err = schema_mutator.deserialize(log_data, data_len, pos)))
            {
             TBSYS_LOG(ERROR, "ObSchemaMutator deserialize error[ret=%d log_data=%p data_len=%ld]",
                      err, log_data, data_len);
            }
            else if (OB_SUCCESS != (err = table_mgr_->get_schema_mgr().apply_schema_mutator(schema_mutator)))
            {
              TBSYS_LOG(ERROR, "UpsTableMgr apply_schema_mutator error, ret=%d schema_mutator version=%ld", err, schema_mutator.get_end_version());
            }
            else
            {
              TBSYS_LOG(INFO, "apply schema mutator succ");
            }
            break;
          //add:e
          // add shili [LONG_TRANSACTION_LOG]  20160926:b
          case OB_UPS_BIG_LOG_DATA:
            TBSYS_LOG(DEBUG, "OB_UPS_BIG_LOG_DATA");
            if(big_log_writer == NULL)
            {
              err = OB_ERR_NULL_POINTER;
              TBSYS_LOG(ERROR, "big log writer is NULL,err=%d", err);
            }
            else
            {
              big_log_writer->set_log_applier(this);
              if(OB_SUCCESS != (err = big_log_writer->handle_big_log(task)))
              {
                TBSYS_LOG(WARN, "big log writer package big log fail,,err=%d", err);
              }
            }
            break;
          //add e
          case OB_LOG_SWITCH_LOG:
            if (OB_SUCCESS != (err = serialization::decode_i64(log_data, data_len, pos, (int64_t*)&file_id)))
            {
              TBSYS_LOG(ERROR, "decode_i64 log_id error, err=%d", err);
            }
            else
            {
              pos = data_len;
              TBSYS_LOG(INFO, "replay log: SWITCH_LOG, file_id=%ld", file_id);
            }
            break;
          case OB_LOG_NOP:
            pos = data_len;
            break;
          default:
            err = OB_ERROR;
            break;
        }
        if (OB_MEM_OVERFLOW == err
            || OB_ALLOCATE_MEMORY_FAILED == err)
        {
          err = OB_EAGAIN;
          //TBSYS_LOG(WARN, "memory overflow, need retry");
        }
        if (OB_SUCCESS != err && OB_EAGAIN != err)
        {
          TBSYS_LOG(ERROR, "replay_log(cmd=%d, log_data=%p[%ld], %s)=>%d",
                    cmd, log_data, data_len, to_cstring(task), err);
        }
      }
      task.profile_.end_apply();
      return err;
    }

    int ObAsyncLogApplier::add_memtable_uncommited_checksum_(const uint32_t session_descriptor, uint64_t *ret_checksum)
    {
      int ret = OB_SUCCESS;
      RPSessionCtx *session = session_mgr_->fetch_ctx<RPSessionCtx>(session_descriptor);
      if (NULL == session)
      {
        TBSYS_LOG(WARN, "fetch session ctx fail sd=%u",  session_descriptor);
        ret = OB_ERROR;
      }
      else
      {
        ret = table_mgr_->add_memtable_uncommited_checksum(&(session->get_uc_info().uc_checksum));
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "add_memtable_uncommited_checksum fail ret=%d", ret);
        }
        else
        {
          if (NULL != ret_checksum)
          {
            *ret_checksum = session->get_uc_info().uc_checksum;
            (*ret_checksum) ^= session->get_checksum();
          }
        }
        session_mgr_->revert_ctx(session_descriptor);
      }
      return ret;
    }

    int ObAsyncLogApplier::end_transaction(ObLogTask& task)
    {
      int err = OB_SUCCESS;
      uint64_t checksum2check = 0;
      bool rollback = false;
      task.profile_.start_commit();
      if (!is_inited())
      {
        err = OB_NOT_INIT;
      }
      else if (OB_SUCCESS != (err = log_mgr_->add_log_replay_event(task.log_id_, task.mutation_ts_, task.replay_type_)))
      {
        TBSYS_LOG(ERROR, "delay_stat_.add_log_replay_event(seq=%ld, ts=%ld)=>%d",
                  task.log_id_, task.mutation_ts_, err);
      }
      else if (!task.trans_id_.is_valid())
      {
        TBSYS_LOG(TRACE, "task.trans_id is invalid, log_id=%ld,cmd=%d", task.log_id_, task.log_entry_.cmd_);
      }
      else if (OB_SUCCESS != (err = add_memtable_uncommited_checksum_(task.trans_id_.descriptor_, &checksum2check)))
      {
        TBSYS_LOG(ERROR, "calc_transaction_checksum fail err=%d sd=%u", err, task.trans_id_.descriptor_);
      }
      else if (OB_SUCCESS != (err = table_mgr_->check_checksum(checksum2check,
                                                               task.checksum_before_mutate_,
                                                               task.checksum_after_mutate_)))
      {
        TBSYS_LOG(ERROR, "table_mgr->check_checksum(%s)=>%d", to_cstring(task), err);
      }
      else if (OB_SUCCESS != (err = session_mgr_->end_session(task.trans_id_.descriptor_, rollback)))
      {
        TBSYS_LOG(ERROR, "end_session(%s)=>%d", to_cstring(task.trans_id_), err);
      }
      else
      {
        session_mgr_->get_trans_seq().update(task.mutation_ts_);
      }
      if (OB_SUCCESS != err)
      {
        TBSYS_LOG(WARN, "err=%d %s", err, to_cstring(task));
      }
      //TBSYS_LOG(INFO, "end_trans[trans_id=%ld, checksum=%ld]", task.trans_id_, task.checksum_after_mutate_);
      task.profile_.end_commit();
      return err;
    }

    int ObAsyncLogApplier::flush(ObLogTask& task)
    {
      int err = OB_SUCCESS;
      task.profile_.start_flush();
      if (!is_inited())
      {
        err = OB_NOT_INIT;
      }
      else
      {
        err = log_mgr_->write_log_as_slave(task.batch_buf_, task.batch_buf_len_);
      }
      task.profile_.end_flush();
      return err;
    }

    int ObAsyncLogApplier::on_destroy(ObLogTask& task)
    {
      int err = OB_SUCCESS;
      if (task.profile_.enable_)
      {
        char buf[ReplayTaskProfile::MAX_PROFILE_INFO_BUF_LEN];
        task.profile_.to_string(buf, sizeof(buf));
        TBSYS_LOG(INFO, "%s", buf);
      }
      return err;
    }
  }; // end namespace updateserver
}; // end namespace oceanbase
