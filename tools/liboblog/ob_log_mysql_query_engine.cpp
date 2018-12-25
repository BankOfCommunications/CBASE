////===================================================================
 //
 // ob_log_mysql_query_engine.cpp liboblog / Oceanbase
 //
 // Copyright (C) 2014 Alipay.com, Inc.
 //
 // Created on 2014-08-19 by XiuMing (wanhong.wwh@alipay.com)
 //
 // -------------------------------------------------------------------
 //
 // Description
 //   MySQL query engine used to query back concurrently
 //
 // -------------------------------------------------------------------
 //
 // Change Log
 //
////====================================================================

#include "ob_log_mysql_query_engine.h"
#include "ob_log_config.h"              // ObLogConfig
#include "ob_log_filter.h"              // ObLogMutator
#include "ob_log_schema_getter.h"       // ObLogSchema
#include "ob_log_mysql_adaptor.h"       // ObLogMysqlAdaptor, IObLogColValue
#include "ob_log_utils.h"               // print_dml_type
#include <pthread.h>                    // pthread_*

namespace oceanbase
{
  using namespace common;
  namespace liboblog
  {
    ObLogMysqlQueryEngine::ObLogMysqlQueryEngine() : inited_(false),
                                                     stop_flag_(true),
                                                     config_(NULL),
                                                     log_filter_(NULL),
                                                     thread_num_(0),
                                                     allocator_()
    {
      allocator_.set_mod_id(ObModIds::OB_LOG_MYSQL_QUERY_ENGINE);
    }

    ObLogMysqlQueryEngine::~ObLogMysqlQueryEngine()
    {
      destroy();
    }

    int ObLogMysqlQueryEngine::init(const ObLogConfig &config,
        IObLogFilter *log_filter)
    {
      int ret = OB_SUCCESS;

      if (inited_)
      {
        ret = OB_INIT_TWICE;
      }
      else if (NULL == (log_filter_ = log_filter))
      {
        ret = OB_INVALID_ARGUMENT;
      }
      else if (OB_SUCCESS != (ret = allocator_.init(ALLOCATOR_SIZE_TOTAL_LIMIT,
              ALLOCATOR_SIZE_HOLD_LIMIT, ALLOCATOR_PAGE_SIZE)))
      {
        TBSYS_LOG(ERROR, "initialize FIFOAllocator fail, ret=%d, total_limit=%ld, hold_limit=%ld, page_size=%ld",
            ret, ALLOCATOR_SIZE_TOTAL_LIMIT, ALLOCATOR_SIZE_HOLD_LIMIT, ALLOCATOR_PAGE_SIZE);
      }
      else if (0 >= config.get_query_back_thread_num()
          || MAX_QUERY_BACK_THREAD_NUM < config.get_query_back_thread_num())
      {
        TBSYS_LOG(ERROR, "invalid query back thread count %d which should be in (0, %d]",
            config.get_query_back_thread_num(), MAX_QUERY_BACK_THREAD_NUM);
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        config_ = &config;

        thread_num_ = 0;
        stop_flag_ = true;
        inited_ = true;
      }

      return ret;
    }

    void ObLogMysqlQueryEngine::destroy()
    {
      stop();
      inited_ = false;

      clear_job_queue_();

      stop_flag_ = true;
      config_ = NULL;
      log_filter_ = NULL;
      thread_num_ = 0;
      allocator_.destroy();
    }

    int ObLogMysqlQueryEngine::start()
    {
      int ret = OB_SUCCESS;

      if (!inited_)
      {
        ret = OB_NOT_INIT;
      }
      else if (stop_flag_)
      {
        stop_flag_ = false;
        thread_num_ = 0;

        for (int64_t index = 0; index < config_->get_query_back_thread_num(); index++)
        {
          thread_infos_[index].query_engine_ = this;
          thread_infos_[index].thread_index_ = index;

          if (OB_SUCCESS != (ret = thread_infos_[index].job_queue_.init(JOB_QUEUE_SIZE)))
          {
            TBSYS_LOG(ERROR, "job_queue_.init fail, size=%ld, thread_index=%ld, ret=%d",
                JOB_QUEUE_SIZE, index, ret);
            break;
          }

          int tmp_ret = pthread_create(&thread_infos_[index].thread_id_, NULL, thread_func_, thread_infos_ + index);
          if (0 != tmp_ret)
          {
            ret = OB_ERR_UNEXPECTED;

            thread_infos_[index].thread_id_ = 0;
            thread_infos_[index].thread_index_ = -1;
            thread_infos_[index].job_queue_.destroy();
            thread_infos_[index].query_engine_ = NULL;

            TBSYS_LOG(WARN, "create %ldth mysql query thread fail, ret=%d", index, tmp_ret);
            break;
          }

          thread_num_++;
        }

        if (OB_SUCCESS == ret)
        {
          TBSYS_LOG(INFO, "start %d mysql query threads success", thread_num_);
        }
        else
        {
          stop();
        }
      }

      return ret;
    }

    void ObLogMysqlQueryEngine::stop()
    {
      TBSYS_LOG(INFO, "stop mysql query engine begin, thread_num=%d, stop_flag=%s",
          thread_num_, stop_flag_ ? "Y" : "N");

      stop_flag_ = true;

      for (int64_t index = 0; index < thread_num_; index++)
      {
        // Signal target thread
        thread_infos_[index].queue_cond_.signal();

        // Join thread
        pthread_join(thread_infos_[index].thread_id_, NULL);

        // Destroy job queue
        thread_infos_[index].job_queue_.destroy();

        // Clear member variables
        thread_infos_[index].thread_id_ = 0;
        thread_infos_[index].thread_index_ = -1;
        thread_infos_[index].query_engine_ = NULL;

        TBSYS_LOG(INFO, "mysql query thread %ld stoped", index);
      }
      TBSYS_LOG(INFO, "stop mysql query engine end");
    }

    void *ObLogMysqlQueryEngine::thread_func_(void *data)
    {
      OB_ASSERT(NULL != data);

      ThreadInfo *args = (ThreadInfo *)data;
      ObLogMysqlQueryEngine *query_engine = args->query_engine_;

      if (NULL != query_engine)
      {
        query_engine->run_query(args);
      }

      return (void *)NULL;
    }

    void ObLogMysqlQueryEngine::run_query(ThreadInfo *ti)
    {
      int err = OB_SUCCESS;

      if (NULL == ti)
      {
        TBSYS_LOG(ERROR, "invalid argument, thread_info=%p", ti);
        err = OB_INVALID_ARGUMENT;
      }
      else if (!inited_)
      {
        TBSYS_LOG(ERROR, "query thread %ld start fail: mysql query engine has not been initialized",
            ti->thread_index_);
        err = OB_NOT_INIT;
      }
      else if (! ob_log_alive(stop_flag_))
      {
        err = OB_IN_STOP_STATE;
      }
      else
      {
        void *query_job = NULL;
        while (ob_log_alive(stop_flag_))
        {
          // Pop a query job from queue
          if (OB_SUCCESS != (err = ti->job_queue_.pop(query_job)))
          {
            if (OB_ENTRY_NOT_EXIST != err)
            {
              TBSYS_LOG(ERROR, "job_queue_.pop fail, ret=%d", err);
            }
          }

          if (NULL != query_job)
          {
            // Handle query job
            handle_query_job_(static_cast<MysqlQueryJob *>(query_job), ti);

            // Free unused query job
            allocator_.free(query_job);
            query_job = NULL;
          }
          else
          {
            ti->queue_cond_.timedwait(JOB_QUEUE_TIMEWAIT);
          }
        }
      }

      TBSYS_LOG(INFO, "mysql query thread %ld exit, err=%d, stop=%s, ob_log_alive=%s",
          ti->thread_index_, err, stop_flag_ ? "Y" : "F", ob_log_alive() ? "Y" : "F");
    }

    void ObLogMysqlQueryEngine::handle_query_job_(const MysqlQueryJob *query_job, ThreadInfo *ti)
    {
      OB_ASSERT(NULL != query_job && NULL != ti);

      int err = OB_SUCCESS;
      const IObLogColValue *col_value = NULL;
      int64_t table_id = query_job->table_id_;
      const ObRowkey *rowkey = query_job->rowkey_;
      const ObLogSchema *schema = query_job->schema_;
      ObLogMysqlQueryResult *query_result = query_job->query_result_;
      ObLogMysqlAdaptor *mysql_adaptor = NULL;

      // FIXME: For TIME DEBUG
      int64_t start_time = tbsys::CTimeUtil::getTime();

      // First get thread specific MySQL Adaptor
      if (OB_SUCCESS != (err = ObLogMysqlAdaptor::get_tsi_mysql_adaptor(mysql_adaptor, *config_, schema)))
      {
        // NOTE: Schema should be updated
        if (err == OB_SCHEMA_ERROR)
        {
          TBSYS_LOG(WARN, "get_tsi_mysql_adaptor fail with schema(%ld,%ld). It should be updated. ret=%d",
              schema->get_version(), schema->get_timestamp(), err);
        }
        else
        {
          TBSYS_LOG(ERROR, "get_tsi_mysql_adaptor fail, ret=%d, schema=%p", err, schema);
        }
      }
      // Query whole row with specific schema
      else if (OB_SUCCESS != (err = mysql_adaptor->query_whole_row(table_id, *rowkey, schema, col_value))
          || NULL == col_value)
      {
        // NOTE: Schema should be updated
        if (OB_SCHEMA_ERROR == err)
        {
          TBSYS_LOG(WARN, "query table (ID=%lu) data fail with schema (%ld,%ld). rowkey=%s, schema should be updated",
              table_id, schema->get_version(), schema->get_timestamp(), to_cstring(*rowkey));
        }
        else if (OB_ENTRY_NOT_EXIST != err)
        {
          TBSYS_LOG(ERROR, "query_whole_row fail, col_value=%p, query_job='%s', err=%d",
              col_value, to_cstring(*query_job), err);

          err = OB_SUCCESS == err ? OB_ERR_UNEXPECTED : err;
        }
        else  // OB_ENTRY_NOT_EXIST
        {
          // Do nothing when query value is empty
        }
      }

      // Fill query result including errors
      int tmp_ret = OB_SUCCESS;
			int64_t query_interval = tbsys::CTimeUtil::getTime() - start_time;
      if (OB_SUCCESS != (tmp_ret = query_result->fill_query_value(query_job->row_index_, col_value, err, query_interval)))
      {
        TBSYS_LOG(ERROR, "fill_query_value fail, ret=%d, col_value=%p, err=%d, query_result='%s', query_job='%s'",
            tmp_ret, col_value, err, to_cstring(*query_result), to_cstring(*query_job));
      }

      TBSYS_LOG(DEBUG, "QUERY_BACK: THREAD %ld: ROW[%ld], table=%lu, ITVL=%ld, queue.size=%ld, err=%d",
          ti->thread_index_, query_job->row_index_, query_job->table_id_,
          query_interval, ti->job_queue_.get_total(), err);
    }

    int ObLogMysqlQueryEngine::query_back(ObLogMutator &mutator,
        const ObLogSchema *schema,
        volatile bool &stop_flag,
        ObLogMysqlQueryResult &query_result)
    {
      int ret = OB_SUCCESS;
      static int64_t __thread trans_count = 0;                          // 事务数
      static int64_t __thread trans_query_time = 0;                     // 事务回查时间
      static int64_t __thread stmt_count_per_trans = 0;                 // 每条事务的语句数
      static int64_t __thread stmt_query_time_sum_per_trans = 0;        // 事务内需要回查语句的回查时间和
      static int64_t __thread need_query_stmt_count_per_trans  = 0;     // 事务内需要回查的语句数

      if (! inited_)
      {
        ret = OB_NOT_INIT;
      }
      else if (! ob_log_alive(stop_flag_))
      {
        ret = OB_IN_STOP_STATE;
      }
      else if (NULL == schema)
      {
        TBSYS_LOG(ERROR, "invalid argument, schema=%p", schema);
        ret = OB_INVALID_ARGUMENT;
      }
      else if (0 >= thread_num_)
      {
        TBSYS_LOG(ERROR, "no valid query back thread started, thread_num=%d, stop_flag=%s, ob_log_alive=%s",
            thread_num_, stop_flag_ ? "Y" : "N", ob_log_alive() ? "Y" : "N");
        ret = OB_NOT_INIT;
      }
      else
      {
        int query_err = OB_SUCCESS;
        int wait_err = OB_SUCCESS;

        // FIXME: For TIME DEBUG
        int64_t start_time = tbsys::CTimeUtil::getTime();

        // Clear query result
        // TODO: 使用者来clear
        query_result.clear();

        if (OB_SUCCESS != (query_err = query_all_rows_(mutator, schema, query_result)))
        {
          TBSYS_LOG(ERROR, "query_all_rows_ fail, err=%d, mutator=(CP=%lu,TM=%ld), schema=%p, query_result='%s'",
              query_err, mutator.get_log_id(), mutator.get_mutate_timestamp(),
              schema, to_cstring(query_result));
        }

        // NOTE: Mark all rows added after all rows are added into query result
        // Do it anyway regardless of error states.
        query_result.mark_all_rows_added();

        // Wait for query result ready anyway
        if (OB_SUCCESS != (wait_err = wait_for_query_result_ready_(query_result, stop_flag)))
        {
          if (OB_IN_STOP_STATE != wait_err)
          {
            TBSYS_LOG(ERROR, "wait_for_query_result_ready_ fail, err=%d, query_result='%s'",
                wait_err, to_cstring(query_result));
          }
        }

        // Decide correct return code
        int result_err = query_result.get_error_code();

        if (OB_SUCCESS == ret)
        {
          ret = (OB_SUCCESS != result_err) ? result_err : (OB_SUCCESS != query_err ? query_err : wait_err);
        }

        if (OB_SUCCESS != ret && OB_SCHEMA_ERROR != ret && OB_IN_STOP_STATE != ret)
        {
          TBSYS_LOG(WARN, "error occur during quering back mutator(CP=%lu,TM=%ld), "
              "result_err=%d, query_err=%d, wait_err=%d, ret=%d",
              mutator.get_log_id(), mutator.get_mutate_timestamp(), result_err, query_err, wait_err, ret);
        }

        int64_t query_interval = tbsys::CTimeUtil::getTime() - start_time;

        TBSYS_LOG(DEBUG, "QUERY_BACK: MUTATOR TM=%ld, ITVL=%ld, QUERY_ROWS=%ld/%ld, "
            "result_err=%d, query_err=%d, wait_err=%d, query_result=%s",
            mutator.get_mutate_timestamp(), query_interval,
            query_result.get_need_query_row_count(), query_result.get_valid_row_count(),
            result_err, query_err, wait_err, to_cstring(query_result));

        stmt_count_per_trans += query_result.get_valid_row_count();
        need_query_stmt_count_per_trans += query_result.get_need_query_row_count();
        stmt_query_time_sum_per_trans += query_result.get_row_query_time_sum();
        trans_query_time += query_interval;
        trans_count++;
      }

      if (REACH_TIME_INTERVAL(PRINT_QUERY_BACK_STAT_INFO_INTERVAL))
      {
        int64_t trans_query_time_avg = 0;
        int64_t stmt_count_per_trans_avg = 0;
        int64_t need_query_stmt_count_per_trans_avg = 0;
        int64_t stmt_query_time_avg = 0;

        if (0 < trans_count)
        {
          trans_query_time_avg = trans_query_time / trans_count;
          stmt_count_per_trans_avg = stmt_count_per_trans / trans_count;
          need_query_stmt_count_per_trans_avg = need_query_stmt_count_per_trans / trans_count;

          if (0 < need_query_stmt_count_per_trans)
          {
            // 每条语句的平均回查时间等于语句回查时间总和除以需要回查的语句数
            stmt_query_time_avg = stmt_query_time_sum_per_trans / need_query_stmt_count_per_trans;
          }
        }

        TBSYS_LOG(INFO, "STAT: [QUERY_BACK] TRANS_QUERY_TIME=%ld STMT_QUERY_TIME=%ld STMTS_PER_TRANS=%ld  NEED_QUERY_STMTS=%ld",
            trans_query_time_avg, stmt_query_time_avg, stmt_count_per_trans_avg, need_query_stmt_count_per_trans_avg);

        // 清空所有的统计项
        trans_count = 0;
        trans_query_time = 0;
        stmt_count_per_trans = 0;
        stmt_query_time_sum_per_trans = 0;
        need_query_stmt_count_per_trans = 0;
      }

      return ret;
    }

    int ObLogMysqlQueryEngine::query_all_rows_(ObLogMutator &mutator,
        const ObLogSchema *schema,
        ObLogMysqlQueryResult &query_result)
    {
      OB_ASSERT(0 < thread_num_);

      int ret = OB_SUCCESS;
      bool irc = false, irf = false;
      ObMutatorCellInfo *cell = NULL;
      ObDmlType dml_type = OB_DML_UNKNOW;
      int64_t row_index = -1;

      while (OB_SUCCESS == ret
          && OB_SUCCESS == (ret = mutator.get_mutator().next_cell()))
      {
        if (OB_SUCCESS != (ret = mutator.get_mutator_cell(&cell, &irc, &irf, &dml_type))
            || NULL == cell)
        {
          TBSYS_LOG(ERROR, "get_mutator_cell fail, ret=%d, cell=%p", ret, cell);
          ret = OB_SUCCESS == ret ? OB_ERR_UNEXPECTED : ret;
          break;
        }

        // Filter statements of un-selected tables and get the first changed cells
        if (! log_filter_->contain(cell->cell_info.table_id_) || !irc)
        {
          continue;
        }

        // Update current row index
        row_index++;

        if (OB_SUCCESS != (ret = query_row_(row_index, cell, dml_type, schema, query_result)))
        {
          TBSYS_LOG(ERROR, "query_row_ fail, ret=%d, row_index=%ld, cell='%s', dml_type=%d, schema=%p",
              ret, row_index, print_cellinfo(&cell->cell_info), dml_type, schema);
          break;
        }
      }

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
        ret = OB_SUCCESS;
      }

      return ret;
    }

    int ObLogMysqlQueryEngine::query_row_(int64_t row_index,
        ObMutatorCellInfo *cell,
        ObDmlType dml_type,
        const ObLogSchema *schema,
        ObLogMysqlQueryResult &query_result)
    {
      OB_ASSERT(0 < thread_num_ && 0 <= row_index && NULL != cell && NULL != schema);

      int ret = OB_SUCCESS;
      bool need_query = false;
      MysqlQueryJob *query_job = NULL;

      // Add current row into query result
      if (OB_SUCCESS != (ret = query_result.add_row(cell->cell_info.table_id_,
              cell->cell_info.row_key_,
              dml_type,
              need_query)))
      {
        TBSYS_LOG(ERROR, "add_row into query result fail, rowkey='%s', dml_type=%d, ret=%d",
            to_cstring(cell->cell_info.row_key_), dml_type, ret);
      }
      // Generate query job and push job into job queue
      else if (need_query)
      {
        query_job = static_cast<MysqlQueryJob *>(allocator_.alloc(sizeof(MysqlQueryJob)));
        if (NULL == query_job)
        {
          TBSYS_LOG(ERROR, "allocate memory for MysqlQueryJob fail, size=%ld", sizeof(MysqlQueryJob));
          ret = OB_ALLOCATE_MEMORY_FAILED;
        }
        else
        {
          query_job->row_index_ = row_index;
          query_job->table_id_ = cell->cell_info.table_id_;
          query_job->schema_ = schema;
          query_job->rowkey_ = &(cell->cell_info.row_key_);
          query_job->query_result_ = &query_result;

          int64_t hash_index = cell->cell_info.row_key_.hash();
          int64_t thread_index = hash_index % thread_num_;
          ThreadInfo *ti = thread_infos_ + thread_index;

          TBSYS_LOG(DEBUG, "QUERY_BACK: push row{%s, %ld} into thread %ld queue(size=%ld)",
              to_cstring(cell->cell_info.row_key_), hash_index, thread_index,
              ti->job_queue_.get_total());

          // Push job into job queue
          if (0 != (ret = ti->job_queue_.push(query_job)))
          {
            TBSYS_LOG(ERROR, "push query job into job_queue fail, ret=%d, size=%ld, query_job='%s'",
                ret, ti->job_queue_.get_total(), to_cstring(*query_job));

            allocator_.free(query_job);
            query_job = NULL;
          }
          else
          {
            ti->queue_cond_.signal();
          }
        }

        if (OB_SUCCESS != ret)
        {
          // NOTE: Here we must fill query value as current row has been added successfully
          (void)query_result.fill_query_value(row_index, NULL, ret, 0);
        }
      }

      TBSYS_LOG(DEBUG, "QUERY_BACK: ROW[%ld]='%s', table=%lu, need_query=%s, ret=%d",
          row_index, print_dml_type(dml_type), cell->cell_info.table_id_, need_query ? "Y" : "N", ret);
      return ret;
    }

    int ObLogMysqlQueryEngine::wait_for_query_result_ready_(ObLogMysqlQueryResult &query_result,
        volatile bool &stop_flag)
    {
      // Wait for query result ready
      int ret = OB_SUCCESS;
      while (ob_log_alive(stop_flag))
      {
        ret = query_result.wait_for_ready(QUERY_RESULT_READY_TIMEWAIT);
        if (OB_PROCESS_TIMEOUT != ret)
        {
          if (OB_SUCCESS != ret)
          {
            TBSYS_LOG(ERROR, "wait for query result ready fail, ret=%d, result='%s'",
                ret, to_cstring(query_result));
          }
          break;
        }
        else
        {
          TBSYS_LOG(WARN, "wait for query result ready timeout(%ld), query_result='%s'",
              QUERY_RESULT_READY_TIMEWAIT, to_cstring(query_result));
        }
      }

      if (! ob_log_alive(stop_flag))
      {
        ret = OB_IN_STOP_STATE;
      }

      return ret;
    }

    void ObLogMysqlQueryEngine::clear_job_queue_()
    {
#if 0
      if (0 < job_queue_.size())
      {
        TBSYS_LOG(INFO, "clear mysql query engine job queue: still %d query jobs not handled", job_queue_.size());

        while (0 < job_queue_.size())
        {
          void *job = NULL;
          if (0 != job_queue_.pop(job, &timeout))
          {
            break;
          }

          if (NULL != job)
          {
            // Free job directly
            allocator_.free(job);
            job = NULL;
          }
        }
      }
#endif
    }
  }
}
