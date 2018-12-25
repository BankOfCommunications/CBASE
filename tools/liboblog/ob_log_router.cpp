////===================================================================
 //
 // ob_log_router.cpp liboblog / Oceanbase
 //
 // Copyright (C) 2013 Alipay.com, Inc.
 //
 // Created on 2013-05-23 by Yubai (yubai.lk@alipay.com)
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

#include "tbsys.h"
#include "ob_log_router.h"
#include "ob_log_common.h"    // MAX_*
#include <string.h>           // memset

namespace oceanbase
{
  using namespace common;
  namespace liboblog
  {
    ObLogRouter::ObLogRouter() : inited_(false),
                                 launched_(false),
                                 skip_dirty_data_(false),
                                 source_(NULL),
                                 schema_getter_(NULL),
                                 observer_map_lock_(),
                                 observer_map_(),
                                 observer_set_(),
                                 log_perf_(),
                                 table_record_map_(),
                                 heartbeat_mutator_allocator_(),
                                 formater_thread_num_(0)
    {
      heartbeat_mutator_allocator_.set_mod_id(ObModIds::OB_LOG_HEARTBEAT_MUTATOR);
    }

    ObLogRouter::~ObLogRouter()
    {
      destroy();
    }

    int ObLogRouter::init(ObLogConfig &config, IObLogFilter *source, IObLogSchemaGetter *schema_getter)
    {
      int ret = OB_SUCCESS;

      if (inited_)
      {
        ret = OB_INIT_TWICE;
      }
      else if (OB_SUCCESS != (ret = heartbeat_mutator_allocator_.init(HEARTBEAT_MUTATOR_SIZE_TOTAL_LIMIT,
              HEARTBEAT_MUTATOR_SIZE_HOLD_LIMIT, HEARTBEAT_MUTATOR_PAGE_SIZE)))
      {
        TBSYS_LOG(ERROR, "initialize heartbeat mutator allocator fail, ret=%d", ret);
      }
      else if (NULL == (source_ = source)
              || NULL == (schema_getter_ = schema_getter)
              || NULL == (config_ = &config))
      {
        TBSYS_LOG(WARN, "invalid param source=%p schema_getter=%p, config=%p", source, schema_getter, &config);
        ret = OB_INVALID_ARGUMENT;
      }
      else if (0 >= (formater_thread_num_ = config.get_router_thread_num())
          || formater_thread_num_ > MAX_LOG_FORMATER_THREAD_NUM)
      {
        TBSYS_LOG(ERROR, "invalid router_thread_num %d, which should be in [1, %d]",
            formater_thread_num_, MAX_LOG_FORMATER_THREAD_NUM);

        ret = OB_INVALID_CONFIG;
      }
      else if (OB_SUCCESS != (ret = log_perf_.init(config, schema_getter)))
      {
        TBSYS_LOG(ERROR, "failed to initialize ObLogPerf, ret=%d", ret);
      }
      else if (0 != table_record_map_.create(DEFAULT_MAX_TABLE_NUM))
      {
        TBSYS_LOG(WARN, "init observer_set fail");
        ret = OB_ERR_UNEXPECTED;
      }
      else if (0 != observer_set_.create(MAX_OBSERVER_NUM))
      {
        TBSYS_LOG(WARN, "init observer_set fail");
        ret = OB_ERR_UNEXPECTED;
      }
      else if (0 != observer_map_.create(MAX_DB_PARTITION_NUM))
      {
        TBSYS_LOG(WARN, "init observer_map fail");
        ret = OB_ERR_UNEXPECTED;
      }
      else
      {
        skip_dirty_data_ = config.get_skip_dirty_data();
        launched_ = false;
        inited_ = true;
      }

      if (OB_SUCCESS != ret)
      {
        destroy();
      }
      return ret;
    }

    void ObLogRouter::destroy()
    {
      inited_ = false;

      stop();
      formater_thread_num_ = 0;
      skip_dirty_data_ = false;
      observer_map_.destroy();
      observer_set_.destroy();
      log_perf_.destroy();
      table_record_map_.destroy();
      schema_getter_ = NULL;
      source_ = NULL;
      heartbeat_mutator_allocator_.destroy();
    }

    int ObLogRouter::register_observer(const uint64_t partition, IObLogObserver *observer)
    {
      int ret = OB_SUCCESS;
      int hash_ret = 0;
      if (!inited_)
      {
        ret = OB_NOT_INIT;
      }
      else if (launched_)
      {
        TBSYS_LOG(WARN, "thread has been launched, can not register observer partition=%lu observer=%p",
            partition, observer);
        ret = OB_NOT_SUPPORTED;
      }
      else if (OB_SUCCESS != (ret = log_perf_.register_db_partition(partition)))
      {
        TBSYS_LOG(ERROR, "ObLogPerf::register_db_partition() fails, partition=%lu, ret=%d",
            partition, ret);
      }
      else if (-1 == (hash_ret = observer_set_.set(reinterpret_cast<uint64_t>(observer))))
      {
        TBSYS_LOG(WARN, "add observer into observer_set_ fail, hash_ret=%d observer=%p",
            hash_ret, observer);
        ret = OB_ERR_UNEXPECTED;
      }
      else if (hash::HASH_INSERT_SUCC != (hash_ret = observer_map_.set(partition, observer)))
      {
        TBSYS_LOG(WARN, "add observer fail, hash_ret=%d partition=%lu observer=%p",
            hash_ret, partition, observer);
        ret = (hash::HASH_EXIST == hash_ret) ? OB_ENTRY_EXIST : OB_ERR_UNEXPECTED;
      }
      else
      {
        // do nothing
      }
      return ret;
    }

    void ObLogRouter::withdraw_observer(const uint64_t partition)
    {
      if (inited_)
      {
        log_perf_.withdraw_db_partition(partition);

        SpinWLockGuard guard(observer_map_lock_);
        observer_map_.erase(partition);
      }
    }

    int ObLogRouter::launch()
    {
      int ret = OB_SUCCESS;
      bool queue_rebalance = false;
      bool dynamic_rebalance = false;

      if (!inited_)
      {
        ret = OB_NOT_INIT;
      }
      else if (0 >= observer_map_.size())
      {
        TBSYS_LOG(WARN, "no observer cannot launch");
        ret = OB_ENTRY_NOT_EXIST;
      }
      else if (launched_)
      {
        TBSYS_LOG(WARN, "thread has already been launched");
        ret = OB_INIT_TWICE;
      }
      // Start Perf
      else if (OB_SUCCESS != (ret = log_perf_.start()))
      {
        TBSYS_LOG(ERROR, "ObLogPerf::start() fails, ret=%d", ret);
      }
      // Initialize formater threads
      else if (OB_SUCCESS != (ret = S2MQueueThread::init(formater_thread_num_, THREAD_QUEUE_SIZE, queue_rebalance, dynamic_rebalance)))
      {
        TBSYS_LOG(WARN, "init queue_thread fail, ret=%d thread_num=%d", ret, formater_thread_num_);
      }
      // Start router thread
      else if (1 != start())
      {
        TBSYS_LOG(WARN, "start thread to route log fail");
        ret = OB_ERR_UNEXPECTED;
      }
      else
      {
        launched_ = true;
      }
      return ret;
    }

    void ObLogRouter::stop()
    {
      if (launched_)
      {
        TBSYS_LOG(INFO, "stop log router begin");

        launched_ = false;

        // Stop router thread
        CDefaultRunnable::stop();
        CDefaultRunnable::wait();

        // Destroy formater threads
        S2MQueueThread::destroy();

        // Stop Perf
        log_perf_.stop();
        TBSYS_LOG(INFO, "stop log router end");
      }
    }

    void ObLogRouter::run(tbsys::CThread* thread, void* arg)
    {
      UNUSED(thread);
      UNUSED(arg);

      int tmp_ret = 0;
      IObLogPartitioner *log_partitioner = NULL;

      tmp_ret = ObLogPartitioner::get_log_paritioner(log_partitioner, *config_, schema_getter_);
      if (OB_SUCCESS != tmp_ret || NULL == log_partitioner)
      {
        TBSYS_LOG(ERROR, "get_log_paritioner fail, log_partitioner=%p, ret=%d", log_partitioner, tmp_ret);
        abort();
      }

      while (!_stop)
      {
        if (REACH_TIME_INTERVAL(SCHEMA_GETTER_REFRESH_INTERVAL))
        {
          schema_getter_->force_refresh();
        }

        if (REACH_TIME_INTERVAL(PERF_PRINT_INTERVAL))
        {
          int perf_ret = OB_SUCCESS;
          if (OB_SUCCESS != (perf_ret = log_perf_.set_total_trans_count(source_->get_trans_count())))
          {
            TBSYS_LOG(WARN, "ObLogPerf::set_total_trans_count fails, ret=%d", perf_ret);
          }

          log_perf_.print_perf_info();
        }

        tmp_ret = OB_SUCCESS;
        if (REACH_TIME_INTERVAL(HEARTBEAT_INTERVAL))
        {
          // Route HEARTBEAT mutator
          tmp_ret = route_heartbeat_mutator_();
        }
        else
        {
          ObLogMutator *mutator = NULL;

          tmp_ret = source_->next_mutator(&mutator, WAIT_SOURCE_TIMEOUT);
          if (NULL != mutator)
          {
            // Mutator contains DDL operations
            if (mutator->get_filter_info().contain_ddl_operations_)
            {
              TBSYS_LOG(INFO, "DDL operations appear, force refresh schema, mutator timestamp=%ld",
                  mutator->get_mutate_timestamp());

              // Refresh to get a new schema whose version is greater than DDL mutator timestamp
              tmp_ret = schema_getter_->refresh_schema_greater_than(mutator->get_mutate_timestamp());
              if (OB_SUCCESS != tmp_ret)
              {
                TBSYS_LOG(ERROR, "refresh_schema_greater_than() fail, DDL mutator timestamp=%ld, ret=%d",
                    mutator->get_mutate_timestamp(), tmp_ret);
              }
            }

            if (OB_SUCCESS == tmp_ret)
            {
              if (! mutator->get_filter_info().contain_table_selected_)
              {
                source_->release_mutator(mutator);
                mutator = NULL;
              }
              // Mutator contains table selected
              else
              {
                // Route DML mutator
                tmp_ret = route_dml_mutator_(mutator, log_partitioner);
              }
            }
          }
        }

        if (skip_dirty_data_ && OB_INVALID_DATA == tmp_ret)
        {
          TBSYS_LOG(WARN, "router skip dirty data, last mutator timestamp=%ld",
              source_->get_last_mutator_timestamp());

          tmp_ret = OB_SUCCESS;
        }
        else if (OB_SUCCESS != tmp_ret && OB_PROCESS_TIMEOUT != tmp_ret)
        {
          TBSYS_LOG(ERROR, "router aborted, last mutator timestamp=%ld, ret=%d",
              source_->get_last_mutator_timestamp(), tmp_ret);

          abort();
        }
      } // while

      return;
    }

    int ObLogRouter::construct_heartbeat_mutator_(ObLogMutator *&mutator, int64_t ht_tm, int64_t ht_sgnt)
    {
      OB_ASSERT(inited_ && NULL == mutator);

      int ret = OB_SUCCESS;

      ObLogMutator *tmp_mutator = NULL;
      void *buffer = heartbeat_mutator_allocator_.alloc(sizeof(ObLogMutator));
      if (NULL == buffer)
      {
        TBSYS_LOG(ERROR, "allocate heartbeat mutator fail, allocator hold=%ld, allocated=%ld",
            heartbeat_mutator_allocator_.hold(), heartbeat_mutator_allocator_.allocated());
        ret = OB_ALLOCATE_MEMORY_FAILED;
      }
      else if (NULL == (tmp_mutator = new(buffer) ObLogMutator))
      {
        TBSYS_LOG(ERROR, "new ObLogMutator fail, buffer=%p", buffer);
        ret = OB_ALLOCATE_MEMORY_FAILED;
      }
      else
      {
        tmp_mutator->reset();
        tmp_mutator->set_mutate_timestamp(ht_tm);
        tmp_mutator->set_heartbeat(true);

        // NOTE: Use unique signature to identiry HEARTBEAT
        tmp_mutator->set_signature(ht_sgnt);

        mutator = tmp_mutator;
      }

      return ret;
    }

    void ObLogRouter::destruct_heartbeat_mutator_(ObLogMutator *mutator)
    {
      OB_ASSERT(inited_ && NULL != mutator);

      mutator->~ObLogMutator();

      heartbeat_mutator_allocator_.free(mutator);

      mutator = NULL;
    }

    int64_t ObLogRouter::get_heartbeat_timestamp_() const
    {
      OB_ASSERT(inited_);

      int64_t heartbeat_timestamp = source_->get_last_mutator_timestamp();

      // Correct HEARTBEAT timestamp
      heartbeat_timestamp = ((heartbeat_timestamp >= 1000000L) ? (heartbeat_timestamp - 1000000L) : 0);

      return heartbeat_timestamp;
    }

    int ObLogRouter::route_heartbeat_mutator_()
    {
      OB_ASSERT(inited_);

      int ret = OB_SUCCESS;

      int64_t heartbeat_timestamp = get_heartbeat_timestamp_();
      int64_t heartbeat_signature = tbsys::CTimeUtil::getTime();
      int64_t formater_thread_num = S2MQueueThread::get_thread_num();

      // NOTE: Route the same HEARTBEAT to ALL LogFormator threads queue
      for (int index = 0; OB_SUCCESS == ret && index < formater_thread_num; index++)
      {
        ObLogMutator *mutator = NULL;

        // Construct HEARTBEAT mutator
        ret = construct_heartbeat_mutator_(mutator, heartbeat_timestamp, heartbeat_signature);
        if (OB_SUCCESS == ret)
        {
          // NOTE: if 0 == sign, mutator will not be pushed into spec_task_queue
          ret = S2MQueueThread::push(mutator, index + formater_thread_num, PriorityPacketQueueThread::NORMAL_PRIV);
          if (OB_SUCCESS != ret)
          {
            // unexpected error
            TBSYS_LOG(WARN, "push queue fail, queue index=%d, ret=%d", index, ret);
            ret = OB_ERR_UNEXPECTED;
          }
        }
        else
        {
          TBSYS_LOG(ERROR, "construct_heartbeat_mutator_ fail, index=%d, ret=%d", index, ret);
          break;
        }
      }

      if (OB_SUCCESS == ret)
      {
        TBSYS_LOG(DEBUG, "route HEARTBEAT (TM=%ld, SGNT=%ld) to %ld formators",
            heartbeat_timestamp, heartbeat_signature, formater_thread_num);
      }
      return ret;
    }

    int ObLogRouter::partition_mutator_(IObLogPartitioner *log_partitioner,
        ObLogMutator *mutator, uint64_t *db_partition)
    {
      OB_ASSERT(NULL != mutator && NULL != log_partitioner && NULL != db_partition);

      int ret = OB_SUCCESS;
      uint64_t local_db_partition = 0;
      ObMutatorCellInfo *local_cell = NULL;
      bool is_local_db_partition_valid = false;

      bool irc = false;
      int valid_record_count = 0;
      table_record_map_.clear();

      while (OB_SUCCESS == ret
            && OB_SUCCESS == (ret = mutator->get_mutator().next_cell()))
      {
        ObMutatorCellInfo *cell = NULL;
        if (OB_SUCCESS != (ret = mutator->get_mutator_cell(&cell, &irc, NULL, NULL)) || NULL == cell)
        {
          TBSYS_LOG(WARN, "get_mutator_cell fail, ret=%d", ret);
          ret = OB_SUCCESS == ret ? OB_ERR_UNEXPECTED : ret;
          break;
        }

        // Table is not selected
        if (! source_->contain(cell->cell_info.table_id_))
        {
          continue;
        }

        // We only care about the first cell of the row
        // FIXME: Change me if the non-first cells are used at following 'while' section
        if (! irc)
        {
          continue;
        }

        uint64_t tmp_db_partition = 0;
        uint64_t tmp_tb_partition = 0;
        ret = log_partitioner->partition(cell, &tmp_db_partition, &tmp_tb_partition);
        if (OB_ENTRY_NOT_EXIST == ret)
        {
          TBSYS_LOG(ERROR, "unexpected error, table (ID=%ld) contains in Filter but no in Partitioner, ret=%d",
              cell->cell_info.table_id_, ret);
          break;
        }
        else if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR, "ObLogPartitioner::partition() fails, ret=%d, cellinfo='%s'",
              ret, print_cellinfo(&cell->cell_info));
          break;
        }

        if (!is_local_db_partition_valid)
        {
          local_db_partition = tmp_db_partition;
          is_local_db_partition_valid = true;
          local_cell = cell;
        }
        else if (local_db_partition != tmp_db_partition)
        {
          LOG_AND_ERR(ERROR, "invalid data (mutator TM=%ld, CP=%lu): "
              "one transaction contains different DB partitions (%lu, %lu)\n"
              "DB partition %lu cell = '%s'\n"
              "DB partition %lu cell = '%s'\n",
              mutator->get_mutate_timestamp(), mutator->get_log_id(),
              local_db_partition, tmp_db_partition,
              local_db_partition, print_cellinfo(&local_cell->cell_info),
              tmp_db_partition, print_cellinfo(&cell->cell_info));

          ret = OB_INVALID_DATA;
          break;
        }

        valid_record_count++;
        uint64_t record_count = 0;

        int hash_ret = table_record_map_.get(cell->cell_info.table_id_, record_count);
        if (hash::HASH_EXIST == hash_ret)
        {
          record_count += 1;
        }
        else if (-1 == hash_ret)
        {
          TBSYS_LOG(ERROR, "table_record_map_ get fails, hash_ret=%d", hash_ret);
          ret = OB_ERR_UNEXPECTED;
          break;
        }
        else
        {
          record_count = 1;
        }

        // Overwrite pattern
        int flag = 1;
        hash_ret = table_record_map_.set(cell->cell_info.table_id_, record_count, flag);
        if (-1 == hash_ret)
        {
          TBSYS_LOG(ERROR, "table_record_map_ set fails, hash_ret=%d", hash_ret);
          ret = OB_ERR_UNEXPECTED;
          break;
        }
      } // end of while

      mutator->reset_iter();

      if (OB_ITER_END == ret)
      {
        if (is_local_db_partition_valid)
        {
          int tmp_ret = OB_SUCCESS;

          *db_partition = local_db_partition;

          PerfUnit pu;
          pu.reset();
          pu.record_count_ = valid_record_count;
          pu.trans_count_ = 1;

          if (OB_SUCCESS != (tmp_ret = log_perf_.add_db_perf(local_db_partition, pu)))
          {
            TBSYS_LOG(WARN, "ObLogPerf::add_db_perf fails, db_partition=%lu, ret=%d",
                local_db_partition, tmp_ret);
          }

          pu.reset();
          for (TableRecordMap::iterator iter = table_record_map_.begin(); iter != table_record_map_.end(); iter++)
          {
            pu.record_count_ = iter->second;
            pu.trans_count_ = 1;

            if (OB_SUCCESS != (tmp_ret = log_perf_.add_tb_perf(iter->first, pu)))
            {
              TBSYS_LOG(WARN, "ObLogPerf::add_tb_perf fails, table_id=%lu, ret=%d", iter->first, tmp_ret);
            }
          }

          ret = OB_SUCCESS;
        }
        else
        {
          // Mutator does not contain selected table
          TBSYS_LOG(ERROR, "Mutator does not contain selected tables");
          ret = OB_ERR_UNEXPECTED;
        }
      }

      table_record_map_.clear();

      return ret;
    } // end of partition_mutator_

    int ObLogRouter::route_dml_mutator_(ObLogMutator *mutator, IObLogPartitioner *log_partitioner)
    {
      OB_ASSERT(NULL != mutator && NULL != log_partitioner && ! mutator->is_heartbeat());

      int ret = OB_SUCCESS;

      uint64_t db_partition = 0;
      ret = partition_mutator_(log_partitioner, mutator, &db_partition);
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "partition mutator fail, ret=%d", ret);
      }
      else
      {
        SpinRLockGuard guard(observer_map_lock_);
        IObLogObserver *observer = NULL;
        int hash_ret = observer_map_.get(db_partition, observer);
        if (hash::HASH_EXIST == hash_ret)
        {
          if (NULL == observer)
          {
            // unexpected error
            TBSYS_LOG(WARN, "get observer fail, hash_ret=%d", hash_ret);
            ret = OB_ERR_UNEXPECTED;
          }
          else
          {
            mutator->set_num(observer->inc_num());
            mutator->set_db_partition(db_partition);
          }
        }
        else
        {
          // This mutator is filtered, there is no observer to handle it.
          // We should release it.
          source_->release_mutator(mutator);
          mutator = NULL;
        }
      }

      if (OB_SUCCESS == ret && NULL != mutator)
      {
        TBSYS_LOG(DEBUG, "push DML mutator=%p into formator (index=%ld), timestamp=%ld",
            mutator, (db_partition + S2MQueueThread::get_thread_num()) % S2MQueueThread::get_thread_num(),
            mutator->get_mutate_timestamp());

        // Notice: if 0 == sign, will not spec thread
        // NOTE: Route mutator based on DB partition number.
        // It means transactions with the same DB partition number will be handled by
        // the same LogFormator thread.
        ret = S2MQueueThread::push(mutator,
            db_partition + S2MQueueThread::get_thread_num(),
            PriorityPacketQueueThread::NORMAL_PRIV);
        if (OB_SUCCESS != ret)
        {
          // unexpected error
          TBSYS_LOG(WARN, "push queue fail, ret=%d", ret);
          ret = OB_ERR_UNEXPECTED;
        }
        else
        {
          TBSYS_LOG(DEBUG, "push task db_partition=%ld timestamp=%ld",
              db_partition,
              mutator->get_mutate_timestamp());
        }
      }
      else if (OB_SUCCESS != ret && NULL != mutator)
      {
        // Release unused mutator to avoid memory leak
        source_->release_mutator(mutator);
        mutator = NULL;
      }

      return ret;
    }

    void ObLogRouter::handle_with_stopflag(void *task, void *pdata, volatile bool &stop_flag)
    {
      UNUSED(pdata);

      int tmp_ret = OB_SUCCESS;
      ObLogMutator *mutator = (ObLogMutator*)task;
      if (NULL != mutator)
      {
        bool is_heartbeat = mutator->is_heartbeat();
        uint64_t checkpoint = mutator->get_log_id();
        int64_t timestamp = mutator->get_mutate_timestamp();

        TBSYS_LOG(DEBUG, "formator %ld handle mutator=%p, type=%s, CP=%lu, TM=%ld",
            S2MQueueThread::get_thread_index(), mutator, is_heartbeat ? "HEARTBEAT" : "DML",
            checkpoint, timestamp);

        SpinRLockGuard guard(observer_map_lock_);

        if (mutator->is_heartbeat())
        {
          tmp_ret = add_heartbeat_mutator_(mutator, stop_flag);
        }
        else
        {
          tmp_ret = add_dml_mutator_(mutator, stop_flag);
        }

        // NOTE: Mutator has been released and can not be accessed anymore.
        if (skip_dirty_data_ && OB_INVALID_DATA == tmp_ret)
        {
          TBSYS_LOG(WARN, "Formator thread %ld: skip dirty data, mutator: TM=%ld, CP=%lu, is_heartbeat=%s",
              S2MQueueThread::get_thread_index(), timestamp, checkpoint, is_heartbeat ? "true" : "false");

          tmp_ret = OB_SUCCESS;
        }
        else if (OB_SUCCESS != tmp_ret && OB_IN_STOP_STATE != tmp_ret)
        {
          TBSYS_LOG(ERROR, "Formator thread %ld aborted, add mutator fail, "
              "mutator: TM=%ld, CP=%lu, is_heartbeat=%s, ret=%d",
              S2MQueueThread::get_thread_index(), timestamp, checkpoint, is_heartbeat ? "true" : "false", tmp_ret);

          abort();
        }
      }

      return;
    }

    int ObLogRouter::add_heartbeat_mutator_(ObLogMutator *mutator, volatile bool &stop_flag)
    {
      OB_ASSERT(NULL != mutator);

      int ret = OB_SUCCESS;
      ObserverSet::iterator iter;

      // NOTE: Put HEARTBEAT into ALL observer
      for (iter = observer_set_.begin(); OB_SUCCESS == ret && iter != observer_set_.end(); iter++)
      {
        IObLogObserver *observer = reinterpret_cast<IObLogObserver *>(iter->first);

        ret = observer->add_mutator(*mutator, stop_flag);

        TBSYS_LOG(DEBUG, "Thread %ld: push HEARTBEAT (TM=%ld, SGNT=%ld) to ObLogServer %p, ret=%d",
            S2MQueueThread::get_thread_index(),
            mutator->get_mutate_timestamp(), mutator->get_signature(),
            observer, ret);

        if (OB_SUCCESS != ret)
        {
          break;
        }
      }

      destruct_heartbeat_mutator_(mutator);
      mutator = NULL;

      return ret;
    } // end of add_heartbeat_mutator_

    int ObLogRouter::add_dml_mutator_(ObLogMutator *mutator, volatile bool &stop_flag)
    {
      OB_ASSERT(NULL != mutator);

      int ret = OB_SUCCESS;

      IObLogObserver *observer = NULL;
      int hash_ret = observer_map_.get(mutator->get_db_partition(), observer);
      if (hash::HASH_EXIST != hash_ret || NULL == observer)
      {
        TBSYS_LOG(ERROR, "observer_map_ get observer fails, hash_ret=%d", hash_ret);
        ret = OB_ERR_UNEXPECTED;
      }
      else
      {
        ret = observer->add_mutator(*mutator, stop_flag);

        TBSYS_LOG(DEBUG, "Add DML mutator timestamp=%ld, DB=%ld, ret=%d",
            mutator->get_mutate_timestamp(), mutator->get_db_partition(), ret);
      }

      source_->release_mutator(mutator);
      mutator = NULL;

      return ret;
    } // end of add_dml_mutator_
  }
}
