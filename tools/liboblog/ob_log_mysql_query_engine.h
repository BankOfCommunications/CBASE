////===================================================================
 //
 // ob_log_mysql_query_engine.h liboblog / Oceanbase
 //
 // Copyright (C) 2014 Alipay.com, Inc.
 //
 // Created on 2014-08-18 by XiuMing (wanhong.wwh@alipay.com)
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

#ifndef OCEANBASE_LIBOBLOG_MYSQL_QUERY_ENGINE_H_
#define OCEANBASE_LIBOBLOG_MYSQL_QUERY_ENGINE_H_

#include "common/ob_define.h"
#include "common/ob_queue_thread.h"     // ObCond
#include "common/ob_rowkey.h"           // ObRowkey
#include "common/ob_fifo_allocator.h"   // FIFOAllocator
#include "common/ob_mutator.h"          // ObMutatorCellInfo
#include "common/ob_fixed_queue.h"      // ObFixedQueue
#include "ob_log_common.h"              // MAX_QUERY_BACK_THREAD_NUM
#include "ob_log_mysql_query_result.h"  // ObLogMysqlQueryResult

namespace oceanbase
{
  namespace liboblog
  {
    class ObLogConfig;
    class ObLogMutator;
    class ObLogSchema;
    class ObLogMysqlAdaptor;
    class IObLogFilter;

    class IObLogMysqlQueryEngine
    {
      public:
        virtual ~IObLogMysqlQueryEngine() {}

      public:
        virtual int init(const ObLogConfig &config, IObLogFilter *log_filter) = 0;
        virtual void destroy() = 0;
        virtual int start() = 0;
        virtual void stop() = 0;
        virtual int query_back(ObLogMutator &mutator,
            const ObLogSchema *schema,
            volatile bool &stop_flag,
            ObLogMysqlQueryResult &query_result) = 0;
    };

    class ObLogMysqlQueryEngine : public IObLogMysqlQueryEngine
    {
      struct MysqlQueryJob
      {
        int64_t row_index_;                     ///< row index in transaction
        uint64_t table_id_;                     ///< target table ID
        const ObLogSchema *schema_;             ///< specific schema
        const common::ObRowkey *rowkey_;        ///< target row key
        ObLogMysqlQueryResult *query_result_;   ///< target result container

        int64_t to_string(char* buf, const int64_t len) const
        {
          int64_t pos = 0;
          common::databuff_printf(buf, len, pos,
              "MysqlQueryJob(row_index=%ld, table_id=%ld, schema=%p, rowkey=%p, query_result=%p)",
              row_index_, table_id_, schema_, rowkey_, query_result_);

          return pos;
        }
      };

      struct ThreadInfo
      {
        pthread_t thread_id_;

        int64_t thread_index_;
        ObLogMysqlQueryEngine *query_engine_;

        common::ObCond queue_cond_;
        common::ObFixedQueue<void> job_queue_;
      } __attribute__ ((aligned (CPU_CACHE_LINE)));

      static const int64_t JOB_QUEUE_SIZE = 16384;                      // must be 2^N
      static const int64_t JOB_QUEUE_TIMEWAIT = 1000L * 1000L;          // time to wait for available jobs in queue
      static const int64_t QUERY_RESULT_READY_TIMEWAIT = 1000L * 1000L; // time to wait for query result ready

      static const int64_t ALLOCATOR_PAGE_SIZE = 16L * 1024L * 1024L;
      static const int64_t ALLOCATOR_SIZE_TOTAL_LIMIT = 4L * 1024L * 1024L * 1024L;
      static const int64_t ALLOCATOR_SIZE_HOLD_LIMIT = ALLOCATOR_PAGE_SIZE * 2;

      public:
        ObLogMysqlQueryEngine();
        virtual ~ObLogMysqlQueryEngine();

      public:
        int init(const ObLogConfig &config, IObLogFilter *log_filter);
        void destroy();

        /// @brief Start MySQL query engine
        /// @retval OB_SUCCESS on success
        /// @retval ! OB_SUCCESS on fail
        int start();

        /// @brief Stop MySQL query engine
        void stop();

        /// @brief Query all statements in specific mutator if needed and wait for query result ready
        /// @param[in] mutator specific mutator
        /// @param[in] schema specific schema
        /// @param[in] stop_flag bool flag used to control when to stop query
        /// @param[out] query_result returned query result
        /// @retval OB_SUCCESS on success
        /// @retval OB_SCHEMA_ERROR when specific schema is invalid and should be updated
        /// @retval OB_IN_STOP_STATE when stop_flag changes to be true
        /// @retval other error code when fail to query all rows
        int query_back(ObLogMutator &mutator,
            const ObLogSchema *schema,
            volatile bool &stop_flag,
            ObLogMysqlQueryResult &query_result);

      public:
        /// @brief Real query threads run function
        /// @param ti thread information
        void run_query(ThreadInfo *ti);

      private:
        /// @brief Common query threads run function
        static void *thread_func_(void *data);

        /// @brief Handle specific query job
        /// @param query_job specific query job
        /// @param ti thread information
        void handle_query_job_(const MysqlQueryJob *query_job, ThreadInfo *ti);

        /// @brief Query all rows in mutator with specific schema and add rows into query result
        /// @param mutator target mutator
        /// @param schema specific schema
        /// @param query_result target query result
        /// @retval OB_SUCCESS on success
        /// @retval ! OB_SUCCESS on fail
        int query_all_rows_(ObLogMutator &mutator, const ObLogSchema *schema, ObLogMysqlQueryResult &query_result);

        /// @brief Query specific row with specific schema and fill query value into query result
        /// @param row_index specific row index
        /// @param dml_type row DML type
        /// @param schema specific schema
        /// @param query_result target query result
        /// @retval OB_SUCCESS on success
        /// @retval ! OB_SUCCESS on fail
        int query_row_(int64_t row_index,
            common::ObMutatorCellInfo *cell,
            common::ObDmlType dml_type,
            const ObLogSchema *schema,
            ObLogMysqlQueryResult &query_result);

        /// @brief Wait for query result ready
        /// @param query_result target query result
        /// @param stop_flag bool flag used to control when to stop waiting
        /// @retval OB_IN_STOP_STATE on stop
        /// @retval OB_SUCCESS on success
        /// @retval other return code on fail
        int wait_for_query_result_ready_(ObLogMysqlQueryResult &query_result,
            volatile bool &stop_flag);

        /// @brief Clear job queue and free all jobs in job queue
        void clear_job_queue_();

      private:
        bool inited_;
        volatile bool stop_flag_ CACHE_ALIGNED;
        const ObLogConfig *config_;
        IObLogFilter *log_filter_;                                ///< filter used to filter un-selected tables

        int32_t thread_num_;                                      ///< query threads number
        ThreadInfo thread_infos_[MAX_QUERY_BACK_THREAD_NUM];      ///< query threads information

        common::FIFOAllocator allocator_;                         ///< FIFO allocator
    };
  }
}

#endif
