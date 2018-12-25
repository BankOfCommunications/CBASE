////===================================================================
 //
 // ob_log_fetcher.h liboblog / Oceanbase
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

#ifndef  OCEANBASE_LIBOBLOG_FETCHER_H_
#define  OCEANBASE_LIBOBLOG_FETCHER_H_

#include "tbsys.h"
#include "common/ob_define.h"
#include "common/ob_mutator.h"
#include "common/ob_fixed_queue.h"
#include "common/ob_queue_thread.h"
#include "ob_log_filter.h"
#include "ob_log_server_selector.h"
#include "ob_log_rpc_stub.h"

namespace oceanbase
{
  namespace liboblog
  {
    class IObLogFetcher : public IObLogFilter
    {
      public:
        virtual ~IObLogFetcher() {};

      public:
        virtual int init(const ObLogConfig &config,
            IObLogServerSelector *server_selector,
            IObLogRpcStub *rpc_stub,
            const int64_t start_id) = 0;

        virtual void destroy() = 0;

        virtual int next_mutator(ObLogMutator **mutator, const int64_t timeout) = 0;

        virtual void release_mutator(ObLogMutator *mutator) = 0;

        virtual int64_t get_cur_id() const = 0;

        virtual bool contain(const uint64_t table_id) {UNUSED(table_id); return true;};

        virtual uint64_t get_trans_count() const = 0;

        virtual int64_t get_last_mutator_timestamp() { return 0; };

        virtual void stop() = 0;
    };

    class ObLogFetcher : public IObLogFetcher, public tbsys::CDefaultRunnable
    {
      static const int64_t MAX_MUTATOR_NUM = 2048;
      static const int64_t ALLOCATOR_PAGE_SIZE = 16L * 1024L * 1024L;
      static const int64_t FETCH_BUFFER_SIZE = common::OB_MAX_LOG_BUFFER_SIZE;
      static const int64_t FETCH_LOG_TIMEOUT = 10L * 1000000L;
      static const int64_t WAIT_VLIST_TIMEOUT = 1L * 1000000L;
      static const int64_t UPS_ADDR_REFRESH_INTERVAL = 10L * 1000000L;
      static const int64_t COND_SPIN_WAIT_NUM = 40000;
      static const int64_t PRINT_MEMORY_USAGE_INTERVAL = 1L * 60L * 1000000L;
      static const int64_t PRINT_CHECKPOINT_INTERVAL = 10L * 1000000L;
      static const int64_t GET_MAX_LOG_SEQ_TIMEOUT = 1L * 1000000L;
      static const int64_t DATA_NOT_SERVE_RETRY_TIMEWAIT = 100L * 1000L;
      static const int64_t NO_UPS_TIMEWAIT = 500L * 1000L;
      static const int64_t FETCH_EMPTY_LOG_TIMEWAIT = 100L * 1000L;
      static const int64_t MAX_FETCH_LOG_FAIL_RETRY_TIMES = 10;
      static const int64_t FETCH_LOG_FAIL_FROM_ALL_UPS_TIMEWAIT = 500L * 1000L;
      public:
        ObLogFetcher();
        ~ObLogFetcher();
      public:
        int init(const ObLogConfig &config,
            IObLogServerSelector *server_selector,
            IObLogRpcStub *rpc_stub,
            const int64_t start_id);
        void destroy();
        int next_mutator(ObLogMutator **mutator, const int64_t timeout);
        void release_mutator(ObLogMutator *mutator);
        int64_t get_cur_id() const;
        uint64_t get_trans_count() const { return trans_count_; }

        // Stop fetcher
        void stop();
      public:
        virtual void run(tbsys::CThread* thread, void* arg);
      private:
        int init_mutator_list_(const int64_t max_mutator_num);
        void destroy_mutator_list_();

        /// @brief Refresh UPS list
        /// @param force_refresh whether to force to refresh
        void refresh_ups_list_(bool force_refresh);

        /// @brief Get an available UPS
        /// @param[out] ups_addr returned UPS address
        /// @param[in] change_ups_addr whether to change UPS address
        /// @retval OB_SUCCESS on success
        /// @retval OB_NEED_RETRY when no UPS available and need retry
        /// @retval other error code on error
        int get_ups_(common::ObServer &ups_addr, bool change_ups_addr);

        /// @brief Fetch log from specific UPS
        /// @param[in] ups_addr target UPS address
        /// @param[in] req fetch log request
        /// @param[out] res fetched log result
        /// @retval OB_SUCCESS on fetching log successfully
        /// @retval OB_NEED_RETRY when need to retry on other UPS
        /// @retval OB_IN_STOP_STATE on stop
        /// @retval other error code on error
        int fetch_log_(common::ObServer &ups_addr,
            updateserver::ObFetchLogReq &req,
            updateserver::ObFetchedLog &res);

        /// @brief Fetch log through RPC
        /// @param[in] ups_addr target UPS address
        /// @param[in] req fetch log request
        /// @param[out] res fetched log result
        /// @param[out] rc result code
        /// @retval OB_SUCCESS on success
        /// @retval OB_NEED_RETRY when need to retry on other UPS
        /// @retval OB_IN_STOP_STATE on stop
        /// @retval other error code on error
        /// @note req.start_id_ may be decreased if asked log id is not aligned
        int rpc_fetch_log_(common::ObServer &ups_addr,
            updateserver::ObFetchLogReq &req,
            updateserver::ObFetchedLog &res,
            common::ObResultCode &rc);

        /// @brief Construct mutator
        /// @param log_entry log entry
        /// @param log fetched log
        /// @param pos target data position
        /// @return handle state
        /// @retval OB_SUCCESS on success
        /// @retval ! OB_SUCCESS on fail
        int construct_mutator_(common::ObLogEntry &log_entry, updateserver::ObFetchedLog &log, const int64_t pos);

        /// @brief Handle fetched log
        /// @param[in] log fetched log
        /// @param[out] max_fetched_log_id max fetched log id
        /// @retval OB_SUCCESS on success
        /// @retval OB_IN_STOP_STATE on stop
        /// @retval other error code on fail
        int handle_fetched_log_(updateserver::ObFetchedLog &log, int64_t &max_fetched_log_id);

        /// @brief Set min_process_timestamp of ObLogMutator
        /// @param mutator target mutator
        void set_mutator_min_process_timestamp_(ObLogMutator &mutator);

        /// @brief Get max checkpoint from ALL UPS
        /// @param[out] max_checkpoint returned max checkpoint
        /// @retval OB_SUCCESS on success
        /// @retval ! OB_SUCCESS on fail
        int get_max_checkpoint_(int64_t &max_checkpoint);
      private:
        common::ModulePageAllocator mod_;
        common::ModuleArena allocator_;
        bool inited_;
        IObLogServerSelector *server_selector_;
        IObLogRpcStub *rpc_stub_;
        volatile int64_t start_id_;
        common::ObFixedQueue<ObLogMutator> p_list_;
        common::ObFixedQueue<ObLogMutator> v_list_;
        common::ObCond p_cond_;
        common::ObCond v_cond_;
        uint64_t trans_count_;
        bool query_back_;
        int64_t query_back_timewait_us_;
        int64_t fetch_log_timeout_us_;
        int64_t asked_min_checkpoint_;
    };
  }
}

#endif //OCEANBASE_LIBOBLOG_FETCHER_H_

