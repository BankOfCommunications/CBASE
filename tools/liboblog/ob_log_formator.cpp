////===================================================================
 //
 // ob_log_formator.cpp liboblog / Oceanbase
 //
 // Copyright (C) 2013 Alipay.com, Inc.
 //
 // Created on 2013-06-07 by Yubai (yubai.lk@alipay.com)
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

#include "ob_log_dml_info.h"      // ObLogDmlMeta ObLogDmlStmt
#include "tbsys.h"
#include "common/ob_obj_cast.h"
#include "ob_log_formator.h"
#include "ob_log_common.h"
#include "ob_log_partitioner.h"
#include "ob_log_utils.h"       // print_log_position_info
#include <string.h>             // memset
#include <MD.h>

namespace oceanbase
{
  using namespace common;
  namespace liboblog
  {
#define BINLOG_RECORD_PUT_OLD_CSTR(binlog_record, cstr) \
    do {\
      if (OB_SUCCESS == ret) \
      {\
        const char *cstr_value = (cstr); \
        std::string *str_value = NULL; \
        if (NULL != cstr_value) \
        { \
          if (NULL == (str_value = (std::string*)string_pool_.alloc())) \
          { \
            ret = OB_ALLOCATE_MEMORY_FAILED;\
            TBSYS_LOG(WARN, "construct std::string fail, ret=%d", ret);\
          }\
          else \
          { \
            *str_value = cstr_value;\
          } \
        }\
\
        if (OB_SUCCESS == ret) \
        {\
          binlog_record->putOld(str_value);\
        }\
      }\
    } while (0)

#define BINLOG_RECORD_PUT_NEW_CSTR(binlog_record, cstr) \
    do {\
      if (OB_SUCCESS == ret) \
      {\
        const char *cstr_value = (cstr); \
        std::string *str_value = NULL; \
        if (NULL != cstr) \
        { \
          if (NULL == (str_value = (std::string*)string_pool_.alloc())) \
          { \
            ret = OB_ALLOCATE_MEMORY_FAILED;\
            TBSYS_LOG(WARN, "construct std::string fail, ret=%d", ret);\
          }\
          else \
          { \
            *str_value = cstr_value;\
          } \
        }\
\
        if (OB_SUCCESS == ret) \
        {\
          binlog_record->putNew(str_value);\
        }\
      }\
    } while (0)

    ObLogDenseFormator::ObLogDenseFormator() : inited_(false),
                                               need_query_back_(false),
                                               formator_thread_num_(0),
                                               log_filter_(NULL),
                                               config_(NULL),
                                               schema_getter_(NULL),
                                               db_name_builder_(NULL),
                                               tb_name_builder_(NULL),
                                               query_engine_(NULL),
                                               heartbeat_info_lock_(),
                                               heartbeat_info_map_(),
                                               p_cond_(COND_SPIN_WAIT_NUM),
                                               v_cond_(COND_SPIN_WAIT_NUM),
                                               br_list_iter_(NULL),
                                               p_list_(),
                                               v_list_(),
                                               mod_(ObModIds::OB_LOG_BINLOG_RECORD),
                                               allocator_(ALLOCATOR_PAGE_SIZE, mod_)
    {
    }

    ObLogDenseFormator::~ObLogDenseFormator()
    {
      destroy();
    }

    int ObLogDenseFormator::init(const ObLogConfig &config,
        IObLogSchemaGetter *schema_getter,
        IObLogDBNameBuilder *db_name_builder,
        IObLogTBNameBuilder *tb_name_builder,
        IObLogMysqlQueryEngine *query_engine,
        IObLogFilter *log_filter)
    {
      int ret = OB_SUCCESS;

      if (inited_)
      {
        ret = OB_INIT_TWICE;
      }
      else if (NULL == (schema_getter_ = schema_getter)
              || NULL == (log_filter_ = log_filter)
              || NULL == (config_ = &config)
              || NULL == (db_name_builder_ = db_name_builder)
              || NULL == (tb_name_builder_ = tb_name_builder))
      {
        ret = OB_INVALID_ARGUMENT;
      }
      else if ((need_query_back_ = (0 != config.get_query_back())) && NULL == (query_engine_ = query_engine))
      {
        TBSYS_LOG(ERROR, "invalid argument, query_engine is null");
        ret = OB_INVALID_ARGUMENT;
      }
      else if (OB_SUCCESS != (ret = init_binlogrecord_list_(MAX_BINLOG_RECORD_NUM)))
      {
        TBSYS_LOG(WARN, "prepare binglog record list fail, ret=%d", ret);
      }
      else if (0 != heartbeat_info_map_.create(MAX_CACHED_HEARTBEAT_NUM))
      {
        TBSYS_LOG(ERROR, "create heartbeat_info_map_ fail, size=%d", MAX_CACHED_HEARTBEAT_NUM);
        ret = OB_ERR_UNEXPECTED;
      }
      else
      {
        formator_thread_num_ = config.get_router_thread_num();
        inited_ = true;
      }

      return ret;
    }

    void ObLogDenseFormator::destroy()
    {
      inited_ = false;

      destroy_binlogrecord_list_();

      br_list_iter_ = NULL;

      heartbeat_info_map_.destroy();

      need_query_back_ = false;
      log_filter_ = NULL;
      config_ = NULL;
      schema_getter_ = NULL;
      db_name_builder_ = NULL;
      tb_name_builder_ = NULL;
      query_engine_ = NULL;

      allocator_.reuse();
    }

    int ObLogDenseFormator::next_record(IBinlogRecord **record, const int64_t timeout)
    {
      int ret = OB_SUCCESS;
      const int64_t TIME_WAIT_INTERVAL = 1000000L;
      static int64_t __thread last_log_id = 0;
      static int64_t __thread last_log_timestamp = 0;
      static int64_t __thread last_heartbeat_timestamp = 0;

      if (NULL == record)
      {
        ret = OB_INVALID_ARGUMENT;
      }
      else if (NULL != br_list_iter_)
      {
        *record = br_list_iter_;
        br_list_iter_ = br_list_iter_->get_next();
      }
      else
      {
        ObLogBinlogRecord *binlog_record = NULL;
        int64_t start_time = tbsys::CTimeUtil::getTime();
        while (ob_log_alive())
        {
          ret = p_list_.pop(binlog_record);
          if (OB_SUCCESS == ret)
          {
            break;
          }

          int64_t wait_time = timeout - (tbsys::CTimeUtil::getTime() - start_time);
          wait_time = TIME_WAIT_INTERVAL < wait_time ? TIME_WAIT_INTERVAL : wait_time;

          if (0 >= wait_time)
          {
            ret = OB_PROCESS_TIMEOUT;
            break;
          }
          p_cond_.timedwait(wait_time);
        }

        if (! ob_log_alive())
        {
          ret = OB_IN_STOP_STATE;
        }

        if (OB_SUCCESS != ret && NULL != binlog_record)
        {
          // 如果成功取出来一个事务的BinlogRecord链表，但是出错了，不去销毁，
          // 而是保留该有效的链表，这样下次取可以取到
          br_list_iter_ = binlog_record;
          binlog_record = NULL;
        }

        if (OB_SUCCESS == ret)
        {
          if (NULL == binlog_record)
          {
            ret = OB_ERR_UNEXPECTED;
          }
          else
          {
            *record = binlog_record;
            br_list_iter_ = binlog_record->get_next();
          }
        }
      }

      if (OB_SUCCESS == ret && NULL != record && NULL != *record)
      {
        int64_t tm = (reinterpret_cast<ObLogBinlogRecord *>(*record))->get_precise_timestamp();
        int64_t cp = (*record)->getCheckpoint1();
        int record_type = (*record)->recordType();

        if (HEARTBEAT == record_type)
        {
          last_heartbeat_timestamp = tm;
        }
        else
        {
          last_log_timestamp = tm;
          last_log_id = cp;
        }
      }

      if (REACH_TIME_INTERVAL(PRINT_LOG_POSITIOIN_INTERVAL) || (OB_SUCCESS != ret && OB_PROCESS_TIMEOUT != ret))
      {
        print_log_position_info(last_log_id, last_log_timestamp, "USER", last_heartbeat_timestamp);
        print_user_queue_state_();
      }

      return ret;
    }

    void ObLogDenseFormator::release_record(IBinlogRecord *record)
    {
      if (NULL != record)
      {
        release_oblog_record_(dynamic_cast<ObLogBinlogRecord*>(record));
      }
    }

    int ObLogDenseFormator::add_mutator(ObLogMutator &mutator, volatile bool &loop_stop)
    {
      int ret = OB_SUCCESS;
      if (!inited_)
      {
        ret = OB_NOT_INIT;
      }
      else if (! ob_log_alive(loop_stop))
      {
        ret = OB_IN_STOP_STATE;
      }
      else
      {
        if (mutator.is_heartbeat())
        {
          ret = format_heartbeat_mutator_(mutator, loop_stop);
        }
        else
        {
          ret = format_dml_mutator_(mutator, loop_stop);
        }

        if (OB_SUCCESS != ret && OB_IN_STOP_STATE != ret)
        {
          TBSYS_LOG(ERROR, "failed to format %s mutator, ret=%d",
              mutator.is_heartbeat() ? "HEARTBEAT" : "DML", ret);
        }
      }

      return ret;
    }

    int ObLogDenseFormator::init_binlogrecord_list_(const int64_t max_binlogrecord_num)
    {
      int ret = OB_SUCCESS;
      ret = p_list_.init(max_binlogrecord_num);
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "init binlogrecord p_list fail, max_binlogrecord_num=%ld", max_binlogrecord_num);
      }
      if (OB_SUCCESS == ret)
      {
        ret = v_list_.init(max_binlogrecord_num);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "init binlogrecord v_list fail, max_binlogrecord_num=%ld", max_binlogrecord_num);
        }
      }
      for (int64_t i = 0; OB_SUCCESS == ret && i < max_binlogrecord_num; i++)
      {
        void *buffer = allocator_.alloc(sizeof(ObLogBinlogRecord));
        if (NULL == buffer)
        {
          TBSYS_LOG(WARN, "allocator memory to build binglog_record fail, i=%ld", i);
          ret = OB_ALLOCATE_MEMORY_FAILED;
          break;
        }

        ObLogBinlogRecord *binlog_record = new(buffer) ObLogBinlogRecord();
        if (NULL == binlog_record)
        {
          TBSYS_LOG(WARN, "build binlog_record fail, i=%ld", i);
          ret = OB_ERR_UNEXPECTED;
          break;
        }

        ret = v_list_.push(binlog_record);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "push binlog_record to list fail, i=%ld", i);
          break;
        }
      }

      return ret;
    }

    void ObLogDenseFormator::destroy_binlogrecord_list_()
    {
      int64_t counter = v_list_.get_total() + v_list_.get_free();
      ObLogBinlogRecord *binlog_record = NULL;
      while (OB_SUCCESS == p_list_.pop(binlog_record))
      {
        if (NULL != binlog_record)
        {
          release_record(binlog_record);
          binlog_record = NULL;
          counter--;
        }
      }
      p_list_.destroy();

      while (OB_SUCCESS == v_list_.pop(binlog_record))
      {
        if (NULL != binlog_record)
        {
          binlog_record->~ObLogBinlogRecord();
          binlog_record = NULL;
          counter--;
        }
      }
      v_list_.destroy();

      if (0 != counter)
      {
        TBSYS_LOG(WARN, "still have binlog_record not release, counter=%ld", counter);
      }
    }

    void ObLogDenseFormator::release_oblog_record_(ObLogBinlogRecord *oblog_br)
    {
      if (NULL != oblog_br)
      {
        // Release DML BinlogRecord content
        if (EUNKNOWN != oblog_br->recordType() && HEARTBEAT != oblog_br->recordType())
        {
          // Free newCols
          std::vector<std::string *>::const_iterator iter = oblog_br->newCols().begin();
          for (; iter != oblog_br->newCols().end(); iter++)
          {
            if (NULL != *iter)
            {
              string_pool_.free((ObLogString*)(*iter));
            }
          }

          // Free oldCols
          iter = oblog_br->oldCols().begin();
          for (; iter != oblog_br->oldCols().end(); iter++)
          {
            if (NULL != *iter)
            {
              string_pool_.free((ObLogString*)(*iter));
            }
          }

          // Revert ITableMeta
          ITableMeta *table_meta = oblog_br->getTableMeta();
          if (NULL != table_meta)
          {
            IObLogMetaManager *meta_manager = reinterpret_cast<IObLogMetaManager *>(table_meta->getUserData());
            if (NULL != meta_manager)
            {
              meta_manager->revert_table_meta(table_meta);
            }
            else
            {
              TBSYS_LOG(WARN, "the UserData of ITableMeta is NULL, cannot revert table meta");
            }
          }
        }

        // Clear content
        oblog_br->clear();

        int ret = v_list_.push(oblog_br);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "push ObLogBinlogRecord to v_list fail, ret=%d", ret);
        }
        else
        {
          v_cond_.signal();
        }
      }
    }

    int ObLogDenseFormator::format_heartbeat_mutator_(ObLogMutator &mutator, volatile bool &loop_stop)
    {
      OB_ASSERT(inited_ && mutator.is_heartbeat());

      int ret = OB_SUCCESS;
      int64_t timestamp = mutator.get_mutate_timestamp();
      int64_t signature = mutator.get_signature();
      HeartbeatInfo hb_info(timestamp, signature);
      bool can_generate_heartbeat = false;

      if (OB_LOG_INVALID_TIMESTAMP == timestamp)
      {
        TBSYS_LOG(ERROR, "invalid heartbeat mutator, timestamp=%ld, signature=%ld", timestamp, signature);
        ret = OB_INVALID_ARGUMENT;
      }
      else if (OB_SUCCESS != (ret = update_and_check_heartbeat_info_(hb_info, can_generate_heartbeat)))
      {
        TBSYS_LOG(ERROR, "update_and_check_heartbeat_info_ fail, hb_info={%s}, ret=%d", to_cstring(hb_info), ret);
      }
      // Generate HEARTBEAT
      else if (can_generate_heartbeat)
      {
        TBSYS_LOG(DEBUG, "HEARTBEAT (TM=%ld, SGNT=%ld) is ready to show", timestamp, signature);

        // Get available BinlogRecord
        ObLogBinlogRecord *binlog_record = fetch_binlog_record_(loop_stop);
        if (NULL != binlog_record)
        {
          // Initialize binlog_record
          binlog_record->init_heartbeat(mutator.get_mutate_timestamp(), need_query_back_);

          // Push HEARTBEAT BinlogRecord into p_list_
          if (OB_SUCCESS != (ret = p_list_.push(binlog_record)))
          {
            TBSYS_LOG(WARN, "push to p_list fail, ret=%d", ret);

            // Recycle binlog_record
            release_record(binlog_record);
          }
          else
          {
            p_cond_.signal();
          }

          binlog_record = NULL;
        }
        else if (ob_log_alive(loop_stop))
        {
          ret = OB_ERR_UNEXPECTED;
          TBSYS_LOG(WARN, "unexpected error: failed to fetch binlog record");
        }
        else
        {
          ret = OB_IN_STOP_STATE;
        }
      }

      if (REACH_TIME_INTERVAL(PRINT_HEARTBEAT_INFO_INTERVAL))
      {
        print_heartbeat_info_();
      }
      return ret;
    }

    int ObLogDenseFormator::update_and_check_heartbeat_info_(const HeartbeatInfo &hb_info,
        bool &can_generate_heartbeat)
    {
      int ret = OB_SUCCESS;
      int32_t count = 0;
      can_generate_heartbeat = false;

      ObSpinLockGuard guard(heartbeat_info_lock_);

      int hash_ret = heartbeat_info_map_.get(hb_info, count);
      if (-1 == hash_ret)
      {
        TBSYS_LOG(ERROR, "get from heartbeat_info_map_ fail, key={%s}, hash_ret=%d",
            to_cstring(hb_info), hash_ret);
        ret = OB_ERR_UNEXPECTED;
      }
      else if (hash::HASH_NOT_EXIST == hash_ret)
      {
        count = 0;
      }

      if (OB_SUCCESS == ret)
      {
        if (count >= (formator_thread_num_ - 1))
        {
          if (0 < count && -1 == (hash_ret = heartbeat_info_map_.erase(hb_info)))
          {
            TBSYS_LOG(ERROR, "erase from heartbeat_info_map_ fail, key={%s}, hash_ret=%d",
                to_cstring(hb_info), hash_ret);
            ret = OB_ERR_UNEXPECTED;
          }
          else
          {
            can_generate_heartbeat = true;
          }
        }
        else
        {
          // Overwrite pattern
          int flag = 1;
          if (-1 == (hash_ret = heartbeat_info_map_.set(hb_info, count + 1, flag)))
          {
            TBSYS_LOG(ERROR, "set heartbeat_info_map_ fail, key={%s}, value=%d, hash_ret=%d",
                to_cstring(hb_info), count+1, hash_ret);

            ret = OB_ERR_UNEXPECTED;
          }
        }
      }

      TBSYS_LOG(DEBUG, "FORMATOR: HEARTBEAT TM=%ld, SGNT=%ld, N=%d, READY=%s",
          hb_info.timestamp_, hb_info.signature_, count + 1, can_generate_heartbeat ? "true" : "false");
      return ret;
    }

    ObLogBinlogRecord *ObLogDenseFormator::fetch_binlog_record_(volatile bool &loop_stop)
    {
      ObLogBinlogRecord *binlog_record = NULL;
      while (NULL == binlog_record
            && ob_log_alive(loop_stop))
      {
        int tmp_ret = v_list_.pop(binlog_record);
        if (OB_SUCCESS != tmp_ret)
        {
          v_cond_.timedwait(WAIT_VLIST_TIMEOUT);
        }
        else
        {
          break;
        }
      }
      if (! ob_log_alive(loop_stop)
          && NULL != binlog_record)
      {
        v_list_.push(binlog_record);
        binlog_record = NULL;
      }

      return binlog_record;
    }

    void ObLogDenseFormator::print_heartbeat_info_()
    {
      if (inited_)
      {
        int64_t index = 1;
        TBSYS_LOG(INFO, "==================== HEARTBEAT INFO ===================");

        HeartbeatInfoMap::iterator iter = heartbeat_info_map_.begin();
        for (; iter != heartbeat_info_map_.end(); iter++)
        {
          HeartbeatInfo &hb_info = iter->first;
          int32_t count = iter->second;

          TBSYS_LOG(INFO, "HEARTBEAT %ld: TM=%ld, SGNT=%ld, N=%d",
              index++, hb_info.timestamp_, hb_info.signature_, count);
        }

        TBSYS_LOG(INFO, "=======================================================");
      }
    }

    int ObLogDenseFormator::format_dml_mutator_(ObLogMutator &mutator, volatile bool &loop_stop)
    {
      OB_ASSERT(inited_ && mutator.is_dml_mutator());

      int ret = OB_SUCCESS;
      uint64_t stmt_count = 0;
      ObLogBinlogRecord *br_list_head = NULL;
      ObLogBinlogRecord *br_list_tail = NULL;
      const ObLogSchema *total_schema = NULL;

      if (NULL == (total_schema = schema_getter_->get_schema()))
      {
        TBSYS_LOG(ERROR, "get_schema fail, return NULL");
        ret = OB_ERR_UNEXPECTED;
      }
      // Transaction BEGIN
      else if (OB_SUCCESS != (ret = fill_trans_barrier_(EBEGIN, mutator, br_list_tail, 0, loop_stop)))
      {
        if (OB_IN_STOP_STATE != ret)
        {
          TBSYS_LOG(ERROR, "fill_trans_barrier_ fail, record_type=EBEGIN");
        }
      }
      else
      {
        br_list_head = br_list_tail;

        int64_t retry_times = 0;
        int64_t start_time = tbsys::CTimeUtil::getTime();

        // NOTE: 在OB_NEED_RETRY和OB_SCHEMA_ERROR错误情况下持续重试，不退出
        while (ob_log_alive(loop_stop))
        {
          stmt_count = 0;

          // Format all DML statements in mutator
          ret = format_all_dml_statements_(mutator, loop_stop, total_schema, br_list_tail, stmt_count);
          if (OB_SCHEMA_ERROR != ret && OB_NEED_RETRY != ret)
          {
            if (OB_SUCCESS != ret && OB_IN_STOP_STATE != ret)
            {
              TBSYS_LOG(ERROR, "format_all_dml_statements_ fail, ret=%d, mutator=(checkpoint=%ld, timestamp=%ld), "
                  "retry_times=%ld, run_time=%ld, schema=%p, schema version=%ld, timestamp=%ld",
                  ret, mutator.get_log_id(), mutator.get_mutate_timestamp(), retry_times,
                  tbsys::CTimeUtil::getTime() - start_time, total_schema, total_schema->get_version(),
                  total_schema->get_timestamp());
            }
            break;
          }
          else if (OB_NEED_RETRY == ret)
          {
            TBSYS_LOG(WARN, "format mutator(checkpoint=%lu, timestamp=%ld) fail: NEED RETRY, wait %ld us and retry. "
                "retry_times=%ld, run_time=%ld",
                mutator.get_log_id(), mutator.get_mutate_timestamp(), FORMATOR_RETRY_TIMEWAIT, ++retry_times,
                tbsys::CTimeUtil::getTime() - start_time);

            usleep(FORMATOR_RETRY_TIMEWAIT);
          }
          else    // OB_SCHEMA_ERROR == ret
          {
            TBSYS_LOG(WARN, "format mutator(checkpoint=%lu, timestamp=%ld) fail: SCHEMA ERROR, refresh schema "
                "and retry. schema=(version=%ld, timestamp=%ld) retry_times=%ld, run_time=%ld",
                mutator.get_log_id(), mutator.get_mutate_timestamp(), total_schema->get_version(),
                total_schema->get_timestamp(), ++retry_times, tbsys::CTimeUtil::getTime() - start_time);

            // 强制更新Schema
            const ObLogSchema *new_schema = NULL;
            if (OB_SUCCESS != (ret = update_schema_(total_schema, new_schema, loop_stop)))
            {
              if (OB_IN_STOP_STATE != ret)
              {
                TBSYS_LOG(ERROR, "update_schema_ fail, ret=%d", ret);
              }
              break;
            }

            TBSYS_LOG(INFO, "formator force update schema from (%ld,%ld) to (%ld,%ld)",
                total_schema->get_version(), total_schema->get_timestamp(),
                new_schema->get_version(), new_schema->get_timestamp());

            // Revert old schema
            schema_getter_->revert_schema(total_schema);
            total_schema = new_schema;
          }
        }

        if (! ob_log_alive(loop_stop))
        {
          ret = OB_IN_STOP_STATE;
        }
      }

      if (OB_SUCCESS == ret)
      {
        // Transaction Commit
        if (OB_SUCCESS == (ret = fill_trans_barrier_(ECOMMIT, mutator, br_list_tail, stmt_count + 1, loop_stop)))
        {
          if (OB_SUCCESS != (ret = p_list_.push(br_list_head)))
          {
            TBSYS_LOG(WARN, "push to p_list fail, ret=%d", ret);
          }
          else
          {
            p_cond_.signal();
          }
        }
      }

      if (OB_SUCCESS != ret)
      {
        release_oblog_record_list_(br_list_head);
        br_list_head = NULL;
        br_list_tail = NULL;
      }

      if (NULL != total_schema)
      {
        schema_getter_->revert_schema(total_schema);
        total_schema = NULL;
      }

      return ret;
    }

    int ObLogDenseFormator::fill_trans_barrier_(const RecordType type,
        const ObLogMutator &mutator,
        ObLogBinlogRecord *&br_list_tail,
        uint64_t br_index_in_trans,
        volatile bool &loop_stop)
    {
      int ret = OB_SUCCESS;
      ObLogBinlogRecord *binlog_record = fetch_binlog_record_(loop_stop);
      if (NULL != binlog_record)
      {
        binlog_record->init_trans_barrier(type, mutator, need_query_back_, br_index_in_trans);

        if (NULL != br_list_tail)
        {
          br_list_tail->set_next(binlog_record);
        }
        br_list_tail = binlog_record;

        binlog_record = NULL;
      }
      else if (ob_log_alive(loop_stop))
      {
        ret = OB_ERR_UNEXPECTED;
        TBSYS_LOG(WARN, "unexpected error: failed to fetch binlog record");
      }
      else
      {
        ret = OB_IN_STOP_STATE;
      }
      return ret;
    }

    int ObLogDenseFormator::format_all_dml_statements_(ObLogMutator &mutator,
        volatile bool &loop_stop,
        const ObLogSchema *total_schema,
        ObLogBinlogRecord *&br_list_tail,
        uint64_t &stmt_count)
    {
      OB_ASSERT(inited_ && mutator.is_dml_mutator() && NULL != total_schema && NULL != br_list_tail);

      int ret = OB_SUCCESS;
      ObLogDmlMeta last_dml_meta;
      ObLogMysqlQueryResult *query_result = NULL;
      ObLogBinlogRecord *binlog_record = NULL, *dml_br_list_head = NULL, *dml_br_list_tail = NULL;

      stmt_count = 0;

      // Query back first if needed
      if (need_query_back_ && OB_SUCCESS != (ret = query_back_(mutator, total_schema, loop_stop, query_result)))
      {
        if (OB_SCHEMA_ERROR != ret && OB_IN_STOP_STATE != ret)
        {
          TBSYS_LOG(ERROR, "query_back_ fail, ret=%d", ret);
        }
      }
      else
      {
        int64_t index = -1;
        // Transaction Statement
        // NOTE: This "while" should exit with OB_ITER_END, other exit code should be regarded as ERROR
        while (OB_SUCCESS == ret
            && OB_SUCCESS == (ret = mutator.get_mutator().next_cell()))
        {
          // Filter unused cells which contain table data which is not selected
          if (OB_SUCCESS != (ret = filter_cells_(mutator)))
          {
            if (OB_ITER_END != ret)
            {
              TBSYS_LOG(ERROR, "filter_cells_ fail, ret=%d", ret);
            }

            break;
          }

          index++;
          // Format DML statement to BinlogRecord
          ret = format_dml_statement_(index, mutator, loop_stop, last_dml_meta, total_schema, query_result, binlog_record);
          if (OB_SUCCESS != ret)
          {
            if (OB_IN_STOP_STATE == ret)
            {
              TBSYS_LOG(INFO, "Loop stoped. stop to format dml statement");
            }
            else if (OB_SCHEMA_ERROR != ret)
            {
              TBSYS_LOG(WARN, "failed to format dml statement, ret=%d", ret);
            }

            // ROLLBACK in error state
            if (NULL != dml_br_list_head)
            {
              release_oblog_record_list_(dml_br_list_head);
              dml_br_list_head = NULL;
              dml_br_list_tail = NULL;
            }

            break;
          }

          // Increase statement count
          stmt_count++;

          binlog_record->set_next(NULL);
          if (NULL != dml_br_list_tail)
          {
            dml_br_list_tail->set_next(binlog_record);
          }
          dml_br_list_tail = binlog_record;

          if (NULL == dml_br_list_head)
          {
            dml_br_list_head = dml_br_list_tail;
          }
          binlog_record = NULL;
        } // end while

        // Reset Mutator
        mutator.reset_iter();

        // NOTE: ret should be OB_ITER_END
        if (OB_SUCCESS == ret)
        {
          TBSYS_LOG(ERROR, "unexcepted error: earlier exiting before iterating to the end of mutator");
          ret = OB_ERR_UNEXPECTED;
        }
        else if (OB_ITER_END == ret)
        {
          if (NULL == dml_br_list_head)
          {
            // NOTE: As the router has filtered already, mutators in formator should contain
            // valid table data user selected.
            TBSYS_LOG(ERROR, "invalid mutator: it does not contain table data user selected");
            ret = OB_ERR_UNEXPECTED;
          }
          else
          {
            br_list_tail->set_next(dml_br_list_head);
            br_list_tail = dml_br_list_tail;

            // Mark with SUCCESS
            ret = OB_SUCCESS;
          }
        }
        else
        {
          // Do nothing on error
        }
      }

      if (OB_SUCCESS != ret)
      {
        // Clear statement count on error
        stmt_count = 0;
      }

      return ret;
    }

    int ObLogDenseFormator::query_back_(ObLogMutator &mutator,
        const ObLogSchema *total_schema,
        volatile bool &loop_stop,
        ObLogMysqlQueryResult *&query_result)
    {
      OB_ASSERT(inited_ && need_query_back_ && NULL != query_engine_);

      int ret = OB_SUCCESS;

      query_result = NULL;
      // Get thread specific query result
      if (OB_SUCCESS != (ret = ObLogMysqlQueryResult::get_tsi_mysql_query_result(query_result,
              DEFAULT_TRANSACTION_STATEMENT_COUNT,
              &string_pool_)))
      {
        TBSYS_LOG(ERROR, "get_tsi_mysql_query_result fail, default_row_count=%ld, string_pool=%p, ret=%d",
            DEFAULT_TRANSACTION_STATEMENT_COUNT, &string_pool_, ret);
      }
      // Query back mutator
      else if (OB_SUCCESS != (ret = query_engine_->query_back(mutator, total_schema, loop_stop, *query_result)))
      {
        if (OB_SCHEMA_ERROR != ret && OB_IN_STOP_STATE != ret)
        {
          TBSYS_LOG(ERROR, "query back fail, ret=%d, mutator=(checkpoint=%ld, timestamp=%ld), "
              "schema version=%ld, query_result=%s",
              ret, mutator.get_log_id(), mutator.get_mutate_timestamp(),
              total_schema->get_version(), to_cstring(*query_result));
        }
      }

      return ret;
    }

    int ObLogDenseFormator::filter_cells_(ObLogMutator &mutator)
    {
      int ret = OB_SUCCESS;

      bool is_row_changed = false;
      bool is_row_finished = false;
      ObMutatorCellInfo *cell = NULL;
      ObDmlType dml_type = OB_DML_UNKNOW;

      do
      {
        ret = mutator.get_mutator_cell(&cell, &is_row_changed, &is_row_finished, &dml_type);
        if (OB_SUCCESS != ret || NULL == cell)
        {
          TBSYS_LOG(WARN, "get_mutator_cell fail, ret=%d, cell=%p", ret, cell);
          ret = OB_SUCCESS == ret ? OB_ERR_UNEXPECTED : ret;
        }
        else if (!is_row_changed)
        {
          TBSYS_LOG(WARN, "get first changed cell fail, mutator: TM=%ld, CP=%lu, cell=%s",
              mutator.get_mutate_timestamp(), mutator.get_log_id(), print_cellinfo(&cell->cell_info));
          ret = OB_INVALID_DATA;
        }
        else
        {
          if (log_filter_->contain(cell->cell_info.table_id_))
          {
            break;
          }

          ret = mutator_iterate_to_row_finish_cell_(mutator);
          if (OB_SUCCESS != ret)
          {
            TBSYS_LOG(WARN, "failed to iterate to the finish cell, ret=%d", ret);
          }
        }
      } while (OB_SUCCESS == ret
          && OB_SUCCESS == (ret = mutator.get_mutator().next_cell()));

      return ret;
    }

    int ObLogDenseFormator::mutator_iterate_to_row_finish_cell_(ObLogMutator &mutator)
    {
      int ret = OB_SUCCESS;
      bool is_row_changed = false;
      bool is_row_finished = false;
      ObMutatorCellInfo *cell = NULL;
      ObDmlType dml_type = OB_DML_UNKNOW;

      do
      {
        ret = mutator.get_mutator_cell(&cell, &is_row_changed, &is_row_finished, &dml_type);
        if (OB_SUCCESS != ret || NULL == cell)
        {
          TBSYS_LOG(WARN, "get_mutator_cell fail, ret=%d", ret);
          ret = OB_SUCCESS == ret ? OB_ERR_UNEXPECTED : ret;
          break;
        }

        if (is_row_finished)
        {
          break;
        }
      } while (OB_SUCCESS == ret && !is_row_finished
          && (OB_SUCCESS == (ret = mutator.get_mutator().next_cell())));

      if (OB_ITER_END == ret)
      {
        TBSYS_LOG(ERROR, "get row finished cell fail, mutator(TM=%ld,CP=%lu) last cell is not row finished cell",
            mutator.get_mutate_timestamp(), mutator.get_log_id());
        ret = OB_INVALID_DATA;
      }

      return ret;
    }

    int ObLogDenseFormator::format_dml_statement_(int64_t index,
        ObLogMutator &mutator,
        volatile bool &loop_stop,
        ObLogDmlMeta &last_dml_meta,
        const ObLogSchema *total_schema,
        ObLogMysqlQueryResult *query_result,
        ObLogBinlogRecord *&binlog_record)
    {
      // binlog_record should be NULL
      // NOTE: query result should be valid if need query back
      OB_ASSERT(NULL == binlog_record && NULL != total_schema
          && (! need_query_back_ || NULL != query_result));

      int ret = OB_SUCCESS;
      RowValue *row_value = NULL;
      ObLogDmlStmt *dml_stmt = NULL;
      ObLogStrColValue *query_value = NULL;

      // Construct DML statement based on mutator
      if (OB_SUCCESS != (ret = construct_dml_stmt_(dml_stmt, mutator, total_schema, &last_dml_meta, index + 1))
          || NULL == dml_stmt)
      {
        TBSYS_LOG(ERROR, "construct_dml_stmt_ fails, ret=%d", ret);
        ret = (OB_SUCCESS == ret ? OB_ERR_UNEXPECTED : ret);
      }
      // Get query value of current statement if needed
      else if (need_query_back_ && OB_SUCCESS != (ret = get_query_value_(index, dml_stmt, query_result, query_value)))
      {
        if (OB_SCHEMA_ERROR != ret)
        {
          TBSYS_LOG(ERROR, "get_query_value_ fail, ret=%d", ret);
        }
      }
      // Construct RowValue based on ObLogDmlStmt and query value
      else if (OB_SUCCESS != (ret = construct_row_value_(row_value, dml_stmt, query_value)))
      {
        TBSYS_LOG(ERROR, "fails to build row value, ret=%d", ret);
      }
      // Construct BinlogRecord based on ObLogDmlStmt and RowValue
      else if (OB_SUCCESS != (ret = construct_binlog_record_(binlog_record,
              dml_stmt,
              row_value,
              loop_stop)))
      {
        if (OB_IN_STOP_STATE != ret)
        {
          TBSYS_LOG(ERROR, "construct_binlog_record_ fail, ret=%d", ret);
        }
      }
      else
      {
        // Modify last DML meta information
        last_dml_meta = dml_stmt->cur_dml_meta_;
      }

      // Destruct RowValue
      if (NULL != row_value)
      {
        // NOTE: If program is in abnormal state, we should free its items which is
        // allocated by string_pool_.
        bool free_row_value_items = (OB_SUCCESS != ret);
        destruct_row_value_(row_value, free_row_value_items);
        row_value = NULL;
      }

      // Destruct DML statement
      if (NULL != dml_stmt)
      {
        destruct_dml_stmt_(dml_stmt);
        dml_stmt = NULL;
      }

      return ret;
    }

    int ObLogDenseFormator::construct_dml_stmt_(ObLogDmlStmt *&dml_stmt,
        ObLogMutator &mutator,
        const ObLogSchema *total_schema,
        ObLogDmlMeta *last_dml_meta_ptr,
        uint64_t stmt_index_in_trans)
    {
      OB_ASSERT(NULL == dml_stmt && mutator.is_dml_mutator() && NULL != total_schema && NULL != last_dml_meta_ptr);

      int ret = OB_SUCCESS;
      ObLogDmlStmt *tmp_dml_stmt = NULL;
      IObLogPartitioner *log_partitioner = NULL;
      IObLogMetaManager *meta_manager = NULL;

      // Get LogPartitioner
      if (OB_SUCCESS != (ret = ObLogPartitioner::get_log_paritioner(log_partitioner, *config_, schema_getter_)))
      {
        TBSYS_LOG(ERROR, "get_log_paritioner fail, ret=%d", ret);
      }
      else if (OB_SUCCESS != (ret = ObLogMetaManager::get_meta_manager(meta_manager,
              schema_getter_,
              db_name_builder_,
              tb_name_builder_)))
      {
        TBSYS_LOG(ERROR, "get_meta_manager fail, ret=%d", ret);
      }
      // Construct DML Statement
      else if (OB_SUCCESS != (ret = ObLogDmlStmt::construct_dml_stmt(tmp_dml_stmt,
              total_schema,
              log_partitioner,
              meta_manager,
              mutator,
              last_dml_meta_ptr,
              stmt_index_in_trans)))
      {
        TBSYS_LOG(ERROR, "ObLogDmlStmt::construct_dml_stmt fails, ret=%d", ret);
      }
      else
      {
        dml_stmt = tmp_dml_stmt;
      }

      return ret;
    }

    int ObLogDenseFormator::get_query_value_(int64_t index,
        ObLogDmlStmt *dml_stmt,
        ObLogMysqlQueryResult *query_result,
        ObLogStrColValue *&query_value)
    {
      OB_ASSERT(need_query_back_ && 0 <= index && NULL != dml_stmt && dml_stmt->is_inited() && NULL != query_result);

      int ret = OB_SUCCESS;
      ObDmlType dml_type = dml_stmt->dml_type_;

      // Query back only for REPLACE and UPDATE DML type
      bool really_need_query_back = (OB_DML_REPLACE == dml_type || OB_DML_UPDATE == dml_type);

      if (really_need_query_back)
      {
        // Get query value
        ret = query_result->get_query_value(index, query_value);
        // Query value is empty
        if (OB_ENTRY_NOT_EXIST == ret)
        {
          // NOTE: Change DML type to DELETE when query back empty value
          dml_stmt->change_dml_type_to_delete();
          query_value = NULL;

          // NOTE: Mark OB_ENTRY_NOT_EXIST with success
          ret = OB_SUCCESS;
        }
        // NOTE: Schema should be updated
        else if (OB_SCHEMA_ERROR == ret)
        {
          TBSYS_LOG(WARN, "query back table (ID=%lu) data fail with schema (%ld,%ld). "
              "schema should be updated",
              dml_stmt->table_id_,
              dml_stmt->total_schema_->get_version(),
              dml_stmt->total_schema_->get_timestamp());
        }
        else if (OB_SUCCESS != ret || NULL == query_value)
        {
          TBSYS_LOG(ERROR, "get_query_value fail, row[%ld]='%s', table_id=%lu, ret=%d, query_value=%p",
              index, to_cstring(*(dml_stmt->row_key_)), dml_stmt->table_id_, ret, query_value);

          ret = OB_SUCCESS == ret ? OB_ERR_UNEXPECTED : ret;
        }
      }

      return ret;
    }

    int ObLogDenseFormator::construct_row_value_(RowValue *&row_value,
        const ObLogDmlStmt *dml_stmt,
        ObLogStrColValue *query_value)
    {
      OB_ASSERT(NULL == row_value && NULL != dml_stmt && dml_stmt->is_inited());

      int ret = OB_SUCCESS;

      // Get thread specific RowValue
      RowValue *tmp_row_value = GET_TSI_MULT(RowValue, TSI_LIBOBLOG_ROW_VALUE);
      if (NULL == tmp_row_value)
      {
        TBSYS_LOG(ERROR, "alloc from row_value_pool fail, ret=%d", ret);
        ret = OB_ALLOCATE_MEMORY_FAILED;
      }
      else
      {
        tmp_row_value->reset();

        // Fill RowValue
        ret = fill_row_value_(tmp_row_value, dml_stmt, query_value);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "failed to fill RowValue, ret=%d", ret);
        }
      }

      if (OB_SUCCESS == ret)
      {
        row_value = tmp_row_value;
      }

      return ret;
    }

    int ObLogDenseFormator::fill_row_value_(RowValue *row_value,
        const ObLogDmlStmt *dml_stmt,
        ObLogStrColValue *query_value)
    {
      OB_ASSERT(NULL != row_value && NULL != dml_stmt && dml_stmt->is_inited());

      int ret = OB_SUCCESS;
      bool fill_row_value = true;

      // Fill query value first if it is ready
      if (NULL != query_value)
      {
        ret = fill_row_value_query_value_(row_value, query_value);

        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR, "fill_row_value_query_value_ fail, ret=%d", ret);
        }
        else
        {
          // NOTE: We have filled queried back value, so we need not fill value subsequently.
          fill_row_value = false;
        }
      }

      // Fill row key
      if (OB_SUCCESS == ret
          && OB_SUCCESS != (ret = fill_row_value_rowkey_(row_value, fill_row_value, dml_stmt)))
      {
        TBSYS_LOG(ERROR, "failed to fill rowkey into RowValue, ret=%d", ret);
      }

      // Fill normal column that is not row key
      if (OB_SUCCESS == ret && OB_DML_DELETE != dml_stmt->dml_type_)
      {
        ret = fill_row_value_normal_column_(row_value, fill_row_value, dml_stmt);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR, "failed to fill normal columns into RowValue, ret=%d", ret);
        }
      }

      if (OB_SUCCESS == ret)
      {
        // Set column number
        row_value->column_num_ = dml_stmt->column_num_;
      }

      return ret;
    }

    int ObLogDenseFormator::fill_row_value_query_value_(RowValue *row_value,
        ObLogStrColValue *query_value)
    {
      OB_ASSERT(NULL != row_value && NULL != query_value);

      int ret = OB_SUCCESS;
      ObLogStrColValue *cv_iter = query_value;

      int i = 0;
      for (i = 0; i < OB_MAX_COLUMN_NUMBER && NULL != cv_iter; i++)
      {
        std::string *cv_str = cv_iter->str_val_;
        row_value->columns_[i] = cv_str;
        cv_iter = cv_iter->next_;
      }

      if (OB_MAX_COLUMN_NUMBER <= i && NULL != cv_iter)
      {
        TBSYS_LOG(ERROR, "query back column value number is bigger than OB_MAX_COLUMN_NUMBER=%ld",
            OB_MAX_COLUMN_NUMBER);

        ret = OB_ERR_UNEXPECTED;
      }

      return ret;
    }

    int ObLogDenseFormator::fill_row_value_rowkey_(RowValue *row_value,
        bool fill_value,
        const ObLogDmlStmt *dml_stmt)
    {
      OB_ASSERT(NULL != row_value && NULL != dml_stmt && dml_stmt->is_inited());

      int ret = OB_SUCCESS;

      const ObLogSchema *total_schema = dml_stmt->total_schema_;
      uint64_t table_id = dml_stmt->table_id_;
      const ObRowkey &rowkey = *(dml_stmt->row_key_);

      const ObTableSchema *table_schema = NULL;
      if (NULL == (table_schema = total_schema->get_table_schema(table_id)))
      {
        TBSYS_LOG(WARN, "get table schema fail, table_id=%lu", table_id);
        ret = OB_ERR_UNEXPECTED;
      }

      for (int64_t i = 0; OB_SUCCESS == ret && i < rowkey.get_obj_cnt(); i++)
      {
        int32_t idx = 0;
        uint64_t column_id = table_schema->get_rowkey_info().get_column(i)->column_id_;

        if (NULL == total_schema->get_column_schema(table_id, column_id, &idx)
            || 0 > idx || OB_MAX_COLUMN_NUMBER <= idx)
        {
          TBSYS_LOG(WARN, "get_column_schema fail, table_id=%lu column_id=%lu idx=%d",
              table_id, column_id, idx);
          ret = OB_ERR_UNEXPECTED;
          break;
        }

        // Fill RowValue meta information
        row_value->is_rowkey_[idx] = true;
        row_value->is_changed_[idx] = true;

        if (fill_value)
        {
          const ObObj &obj = rowkey.get_obj_ptr()[i];

          if (OB_SUCCESS != (ret = fill_row_value_obj_(row_value, obj, idx)))
          {
            TBSYS_LOG(WARN, "failed to fill %ldth row key, table_id=%lu, column_id=%lu idx=%d",
                i, table_id, column_id, idx);
            break;
          }
        }
      }
      return ret;
    }

    // Fill value into RowValue normal columns which does not include row keys
    int ObLogDenseFormator::fill_row_value_normal_column_(RowValue *row_value,
        bool fill_value,
        const ObLogDmlStmt *dml_stmt)
    {
      OB_ASSERT(NULL != row_value && NULL != dml_stmt && dml_stmt->is_inited());

      int ret = OB_SUCCESS;
      const ObCellInfo *cell_info = NULL;
      const ObLogSchema *total_schema = dml_stmt->total_schema_;
      int64_t normal_count = dml_stmt->normal_columns_.count();

      for (int64_t i = 0; OB_SUCCESS == ret && i < normal_count; i++)
      {
        if (OB_SUCCESS != (ret = dml_stmt->normal_columns_.at(i, cell_info))
            || NULL == cell_info)
        {
          TBSYS_LOG(ERROR, "fails to get cell_info from normal_columns_, ret=%d", ret);
          ret = (OB_SUCCESS == ret) ? OB_ERR_UNEXPECTED : ret;
          break;
        }

        int32_t idx = -1;
        uint64_t column_id = cell_info->column_id_;
        uint64_t table_id = cell_info->table_id_;

        if (NULL == total_schema->get_column_schema(table_id, column_id, &idx)
            || 0 > idx || OB_MAX_COLUMN_NUMBER <= idx)
        {
          TBSYS_LOG(WARN, "get_column_schema fail, table_id=%lu column_id=%lu idx=%d",
              table_id, column_id, idx);
          ret = OB_ERR_UNEXPECTED;
          break;
        }

        // Fill RowValue meta information
        row_value->is_changed_[idx] = true;
        row_value->is_rowkey_[idx] = false;

        if (fill_value)
        {
          ret = fill_row_value_obj_(row_value, cell_info->value_, idx);
          if (OB_SUCCESS != ret)
          {
            TBSYS_LOG(WARN, "fill_row_value_obj_() fails to fill Object into RowValue, column_index=%d, %s",
                idx, print_obj(cell_info->value_));
          }
        }
      }

      return ret;
    }

    int ObLogDenseFormator::fill_row_value_obj_(RowValue *row_value,
        const ObObj &obj,
        const int32_t column_index)
    {
      OB_ASSERT(NULL != row_value && column_index >= 0 && column_index < OB_MAX_COLUMN_NUMBER);

      int ret = OB_SUCCESS;
      const char *vstr = NULL;
      int64_t vstr_len = 0;
      std::string *vstr_str = NULL;

      if (obj.is_null())
      {
        // FIXME: Do anything here when column value is NULL if you should do
        vstr_str = NULL;
      }
      else
      {
        if (NULL == (vstr = value2str_(obj, vstr_len)))
        {
          TBSYS_LOG(WARN, "trans value to string fail, column_index=%d %s",
              column_index, print_obj(obj));
          ret = OB_ERR_UNEXPECTED;
        }
        else if (NULL == (vstr_str = (std::string*)string_pool_.alloc()))
        {
          TBSYS_LOG(WARN, "construct std::string fail");
          ret = OB_ALLOCATE_MEMORY_FAILED;
        }
        else
        {
          (*vstr_str).assign(vstr, vstr_len);
        }
      }

      if (OB_SUCCESS == ret)
      {
        row_value->columns_[column_index] = vstr_str;
      }

      return ret;
    }

    const char *ObLogDenseFormator::value2str_(const ObObj &v, int64_t &v_len)
    {
      static const int64_t DEFAULT_VALUE_STR_BUFFER_SIZE = 65536;
      static __thread char buffer[DEFAULT_VALUE_STR_BUFFER_SIZE];
      const char *ret = NULL;
      int64_t ret_size = 0;

      if (ObVarcharType == v.get_type())
      {
        ObString str;
        int tmp_ret = v.get_varchar(str);
        if (OB_SUCCESS == tmp_ret
            && DEFAULT_VALUE_STR_BUFFER_SIZE > snprintf(buffer, DEFAULT_VALUE_STR_BUFFER_SIZE, "%.*s", str.length(), str.ptr()))
        {
          ret = buffer;
          v_len = str.length();
        }
        else
        {
          TBSYS_LOG(WARN, "maybe varchar too long, length=%d", str.length());
        }
      }
      else
      {
        ObObj casted_obj = v;

        if (OB_SUCCESS == obj_cast(casted_obj, ObVarcharType, buffer, DEFAULT_VALUE_STR_BUFFER_SIZE - 1, ret_size))
        {
          buffer[ret_size] = '\0';
          ret = buffer;
          v_len = ret_size;
        }
      }
      return ret;
    }

    int ObLogDenseFormator::construct_binlog_record_(ObLogBinlogRecord *&binlog_record,
        const ObLogDmlStmt *dml_stmt,
        const RowValue *row_value,
        volatile bool &loop_stop)
    {
      OB_ASSERT(NULL == binlog_record && NULL != dml_stmt && dml_stmt->is_inited() && NULL != row_value);

      int ret = OB_SUCCESS;
      ObDmlType dml_type = dml_stmt->dml_type_;

      // Initialize Binlog Record with dml_stmt
      if (OB_SUCCESS != (ret = init_binlog_record_(binlog_record, dml_stmt, loop_stop)))
      {
        if (OB_IN_STOP_STATE != ret)
        {
          TBSYS_LOG(ERROR, "failed to initialize binlog record, dml_type=%s ret=%d",
              str_dml_type(dml_type), ret);
        }
      }
      // Fill Binlog Record with row_value
      else if (OB_SUCCESS != (ret = fill_binlog_record_(binlog_record, row_value, dml_type)))
      {
        TBSYS_LOG(WARN, "failed to fill binlog record, dml_type=%s ret=%d",
            str_dml_type(dml_type), ret);
      }

      // FIXME: Recycle binlog_record or its list on abnormal state, push it into v_list
      // to avoid Memory Leak
      // See fetch_binlog_record_()
      if (OB_SUCCESS != ret && NULL != binlog_record)
      {
        release_record(binlog_record);
        binlog_record = NULL;
      }

      return ret;
    }

    int ObLogDenseFormator::init_binlog_record_(ObLogBinlogRecord * &binlog_record,
        const ObLogDmlStmt *dml_stmt,
        volatile bool &loop_stop)
    {
      OB_ASSERT(NULL == binlog_record && NULL != dml_stmt && dml_stmt->is_inited());

      int ret = OB_SUCCESS;

      // Fetch ObLogBinlogRecord
      if (NULL == (binlog_record = fetch_binlog_record_(loop_stop)))
      {
        if (ob_log_alive(loop_stop))
        {
          ret = OB_ERR_UNEXPECTED;
        }
        else
        {
          ret = OB_IN_STOP_STATE;
        }
      }
      else if (OB_SUCCESS != (ret = binlog_record->init_dml(dml_stmt, need_query_back_)))
      {
        TBSYS_LOG(ERROR, "fails to initialize binlog record, ret=%d", ret);
      }

      return ret;
    }

    int ObLogDenseFormator::fill_binlog_record_(ObLogBinlogRecord *binlog_record,
        const RowValue *row_value,
        ObDmlType dml_type)
    {
      OB_ASSERT(NULL != binlog_record && NULL != row_value);

      int ret = OB_SUCCESS;

      switch (dml_type)
      {
        case OB_DML_DELETE:
          ret = format_dml_delete_(binlog_record, row_value);
          break;

        case OB_DML_INSERT:
          ret = format_dml_insert_(binlog_record, row_value);
          break;

        case OB_DML_REPLACE:
          ret = format_dml_replace_update_(binlog_record, row_value, true);
          break;

        case OB_DML_UPDATE:
          ret = format_dml_replace_update_(binlog_record, row_value, false);
          break;

        default:
          ret = OB_ERR_UNEXPECTED;
          TBSYS_LOG(WARN, "unexpected error: unknown DML type %d", dml_type);
          break;
      }

      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "failed to format dml %s, ret=%d", str_dml_type(dml_type), ret);
      }

      return ret;
    } // end of fill_binlog_record_

    int ObLogDenseFormator::format_dml_delete_(ObLogBinlogRecord *binlog_record, const RowValue *row_value)
    {
      OB_ASSERT(NULL != binlog_record && NULL != row_value);

      int ret = OB_SUCCESS;

      // Fill binlog record OldCols
      for (int64_t i = 0; OB_SUCCESS == ret && i < row_value->column_num_; i++)
      {
        if (row_value->is_rowkey_[i])
        {
          binlog_record->putOld(row_value->columns_[i]);
        }
        // Set normal column with COLUMN_VALUE_UNCHANGED_MARK
        else
        {
          BINLOG_RECORD_PUT_OLD_CSTR(binlog_record, COLUMN_VALUE_UNCHANGED_MARK);
        }
      } // end of for

      return ret;
    }

    int ObLogDenseFormator::format_dml_insert_(ObLogBinlogRecord *binlog_record, const RowValue *row_value)
    {
      OB_ASSERT(NULL != binlog_record && NULL != row_value);

      int ret = OB_SUCCESS;
      for (int64_t i = 0; OB_SUCCESS == ret && i < row_value->column_num_; i++)
      {
        // Fill into default value when column is not changed
        // FIXME: Here take all column default value as null type.
        //        Please get the right default value
        if (!row_value->is_changed_[i])
        {
          binlog_record->putNew(NULL);
        }
        else
        {
          // Take the changed value
          binlog_record->putNew(row_value->columns_[i]);
        }

        const char *mark = NULL;
        if (row_value->is_rowkey_[i])
        {
          // NOTE: mark rowkey as changed columns
          mark = COLUMN_VALUE_CHANGED_MARK;
        }
        else
        {
          mark = row_value->is_changed_[i] ? COLUMN_VALUE_CHANGED_MARK : COLUMN_VALUE_UNCHANGED_MARK;
        }

        BINLOG_RECORD_PUT_OLD_CSTR(binlog_record, mark);
      } // end of for

      return ret;
    }

    int ObLogDenseFormator::format_dml_replace_update_(ObLogBinlogRecord *binlog_record,
        const RowValue *row_value,
        const bool is_dml_replace)
    {
      // NOTE: Now it is unused, but maybe it will be used someday to distinguish REPLACE/UPDATE.
      UNUSED(is_dml_replace);

      OB_ASSERT(NULL != binlog_record && NULL != row_value);

      int ret = OB_SUCCESS;
      for (int i = 0; OB_SUCCESS == ret && i < row_value->column_num_; i++)
      {
        if (!need_query_back_ && !row_value->is_changed_[i])
        {
          BINLOG_RECORD_PUT_NEW_CSTR(binlog_record, COLUMN_VALUE_UNCHANGED_MARK);
        }
        else
        {
          binlog_record->putNew(row_value->columns_[i]);
        }

        if (OB_SUCCESS == ret)
        {
          const char *mark = NULL;
          if (row_value->is_rowkey_[i])
          {
            // NOTE: mark rowkey as changed columns
            mark = COLUMN_VALUE_CHANGED_MARK;
          }
          else
          {
            mark = row_value->is_changed_[i] ? COLUMN_VALUE_CHANGED_MARK : COLUMN_VALUE_UNCHANGED_MARK;
          }

          BINLOG_RECORD_PUT_OLD_CSTR(binlog_record, mark);
        }
      } // end of for

      return ret;
    }

    void ObLogDenseFormator::destruct_row_value_(RowValue *row_value, bool free_items)
    {
      if (NULL != row_value)
      {
        if (free_items && 0 < row_value->column_num_)
        {
          // Free all allocated string value
          for (int i = 0; i < row_value->column_num_; i++)
          {
            if (NULL != row_value->columns_[i])
            {
              string_pool_.free(static_cast<ObLogString*>(row_value->columns_[i]));
              row_value->columns_[i] = NULL;
            }
          }
        }

        // Reset RowValue
        row_value->reset();
      }
    }

    void ObLogDenseFormator::destruct_dml_stmt_(ObLogDmlStmt *dml_stmt)
    {
      if (NULL != dml_stmt)
      {
        ObLogDmlStmt::destruct_dml_stmt(dml_stmt);
      }
    }

    void ObLogDenseFormator::release_oblog_record_list_(ObLogBinlogRecord *br_list)
    {
      OB_ASSERT(NULL != br_list);

      ObLogBinlogRecord *br = br_list;
      while (NULL != br)
      {
        ObLogBinlogRecord *next = br->get_next();

        release_oblog_record_(br);

        br = next;
      }
    }

    void ObLogDenseFormator::print_user_queue_state_()
    {
      int64_t free = v_list_.get_total();
      int64_t filled = p_list_.get_total();
      int64_t out = v_list_.get_free() - filled;
      int64_t total = v_list_.get_total() + v_list_.get_free();

      TBSYS_LOG(INFO, "STAT: [USER_QUEUE] FREE=%ld FILLED=%ld OUT=%ld TOTAL=%ld", free, filled, out, total);
    }

    int ObLogDenseFormator::update_schema_(const ObLogSchema *total_schema, const ObLogSchema *&new_schema, volatile bool &loop_stop)
    {
      OB_ASSERT(NULL != total_schema);

      int ret = OB_SUCCESS;
      int64_t start_time = tbsys::CTimeUtil::getTime();
      const int64_t WARN_TIME = 10L * 1000000L;

      // 强制更新schema，不退出
      while (ob_log_alive(loop_stop))
      {
        ret = schema_getter_->get_schema_newer_than(total_schema, new_schema);

        if (OB_NEED_RETRY != ret)
        {
          break;
        }

        usleep(SCHEMA_UPDATE_FAIL_TIMEWAIT);

        if (REACH_TIME_INTERVAL(WARN_TIME))
        {
          TBSYS_LOG(ERROR, "get_schema_newer_than fail, run_time=%ld, old_schema version=%ld, timestamp=%ld",
            tbsys::CTimeUtil::getTime() - start_time, total_schema->get_version(), total_schema->get_timestamp());
        }
      }

      if (! ob_log_alive(loop_stop))
      {
        ret = OB_IN_STOP_STATE;
      }

      if (OB_SUCCESS != ret && OB_IN_STOP_STATE != ret)
      {
        TBSYS_LOG(ERROR, "get_schema_newer_than fail, old_schema version=%ld, timestamp=%ld",
            total_schema->get_version(), total_schema->get_timestamp());
      }

      return ret;
    }
  }
}

