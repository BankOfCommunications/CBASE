////===================================================================
 //
 // ob_log_router.h liboblog / Oceanbase
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

#ifndef  OCEANBASE_LIBOBLOG_ROUTER_H_
#define  OCEANBASE_LIBOBLOG_ROUTER_H_

#include "common/ob_define.h"
#include "common/ob_queue_thread.h"
#include "common/hash/ob_hashmap.h"
#include "ob_log_filter.h"
#include "ob_log_partitioner.h"
#include "ob_log_meta_manager.h"
#include "ob_log_perf.h"          // ObLogPerf
#include "ob_log_common.h"        // MAX_*
#include "ob_log_formator.h"      // IObLogObserver
#include "common/ob_fifo_allocator.h" // FIFOAllocator

namespace oceanbase
{
  namespace liboblog
  {
    class IObLogRouter
    {
      public:
        virtual ~IObLogRouter() {};
      public:
        virtual int init(ObLogConfig &config,
            IObLogFilter *source,
            IObLogSchemaGetter *schema_getter) = 0;

        virtual void destroy() = 0;

        virtual int register_observer(const uint64_t partition, IObLogObserver *observer) = 0;

        virtual void withdraw_observer(const uint64_t partition) = 0;

        virtual void set_perf_print_to_stderr(bool print_to_stderr) = 0;

        virtual int launch() = 0;

        virtual void stop() = 0;
    };

    class ObLogRouter : public IObLogRouter, public tbsys::CDefaultRunnable, public common::S2MQueueThread
    {
      typedef common::hash::ObHashMap<uint64_t, IObLogObserver*> ObserverMap;
      typedef common::hash::ObHashSet<uint64_t> ObserverSet;
      typedef common::hash::ObHashMap<uint64_t, uint64_t> TableRecordMap;
      static const int64_t WAIT_SOURCE_TIMEOUT = 1000000L;
      static const int64_t SCHEMA_GETTER_REFRESH_INTERVAL = 10L * 1000000L;
      static const int64_t THREAD_QUEUE_SIZE = 100000L;
      static const int64_t FETCH_FREE_MUTATOR_WAIT_TIMEOUT = 1000000L;
      static const int32_t MAX_FETCH_FREE_MUTATOR_RETRY_TIMES = 600;
      static const int64_t HEARTBEAT_MUTATOR_SIZE_TOTAL_LIMIT = 1L * 1024L * 1024L * 1024L;
      static const int64_t HEARTBEAT_MUTATOR_SIZE_HOLD_LIMIT = HEARTBEAT_MUTATOR_SIZE_TOTAL_LIMIT / 2;
      static const int64_t HEARTBEAT_MUTATOR_PAGE_SIZE = 16L * 1024L * 1024L;
      public:
        ObLogRouter();
        ~ObLogRouter();
      public:
        int init(ObLogConfig &config,
            IObLogFilter *source,
            IObLogSchemaGetter *schema_getter);
        void destroy();
        int register_observer(const uint64_t partition, IObLogObserver *observer);
        void withdraw_observer(const uint64_t partition);
        int launch();
        void stop();

        void set_perf_print_to_stderr(bool print_to_stderr)
        {
          if (inited_)
          {
            log_perf_.set_print_to_stderr(print_to_stderr);
          }
        }
      public:
        virtual void run(tbsys::CThread* thread, void* arg);
        virtual void handle(void *task, void *pdata) {UNUSED(task); UNUSED(pdata);};
        virtual void handle_with_stopflag(void *task, void *pdata, volatile bool &stop_flag);
        virtual void on_end(void *ptr) {UNUSED(ptr);};

      // Private member functions
      private:
        /// @brief Add heartbeat mutator to ObLogServer
        /// @param mutator target mutator
        /// @param stop_flag flag to stop
        /// @retval OB_SUCCESS on success
        /// @retval ! OB_SUCCESS on fail
        int add_heartbeat_mutator_(ObLogMutator *mutator, volatile bool &stop_flag);

        /// @brief Add DML mutator to ObLogServer
        /// @param mutator target mutator
        /// @param stop_flag flag to stop
        /// @retval OB_SUCCESS on success
        /// @retval ! OB_SUCCESS on fail
        int add_dml_mutator_(ObLogMutator *mutator, volatile bool &stop_flag);

        /// @brief Route DML mutator to specific formator thread
        /// @param mutator target mutator
        /// @param log_partitioner partitioner
        /// @return handle state
        /// @retval OB_SUCCESS on success
        /// @retval OB_INVALID_DATA when mutator data is invalid
        /// @retval other codes on fail
        /// @note This function will filter mutators
        int route_dml_mutator_(ObLogMutator *mutator, IObLogPartitioner *log_partitioner);

        /// @brief Route HEARTBEAT mutator to formator threads
        /// @return handle state
        /// @retval OB_SUCCESS on success
        /// @retval ! OB_SUCCESS on fail
        int route_heartbeat_mutator_();

        /// @brief Construct HEARTBEAT mutator
        /// @param mutator returned heartbeat mutator
        /// @param timestamp target heartbeat timestamp
        /// @param signature target heartbeat signature
        /// @retval OB_SUCCESS on success
        /// @retval ! OB_SUCCESS on fail
        int construct_heartbeat_mutator_(ObLogMutator *&mutator, int64_t timestamp, int64_t signature);

        /// @brief Destruct HEARTBEAT mutator
        /// @param mutator target mutator
        void destruct_heartbeat_mutator_(ObLogMutator *mutator);

        /// @brief Get HEARTBEAT Timstamp
        /// @return timestamp of current HEARTBEAT
        int64_t get_heartbeat_timestamp_() const;

        /// @brief Partition mutator and get the unique DB partition number
        /// @param log_partitioner partitioner
        /// @param mutator target mutator
        /// @param db_partition returned DB partition
        /// @return handle state
        /// @retval OB_SUCCESS on success
        /// @retval OB_INVALID_DATA when mutator data is invalid
        /// @retval other error codes on fail
        int partition_mutator_(IObLogPartitioner *log_partitioner, ObLogMutator *mutator, uint64_t *db_partition);

      private:
        bool inited_;
        bool launched_;
        bool skip_dirty_data_;
        IObLogFilter *source_;
        IObLogSchemaGetter *schema_getter_;
        common::SpinRWLock observer_map_lock_;
        ObserverMap observer_map_;
        ObserverSet observer_set_;
        ObLogPerf log_perf_;
        ObLogConfig *config_;
        TableRecordMap table_record_map_;

        common::FIFOAllocator heartbeat_mutator_allocator_;  ///< Allocator for heartbeat mutator

        int formater_thread_num_;
    };
  }
}

#endif //OCEANBASE_LIBOBLOG_ROUTER_H_

