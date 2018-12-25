////===================================================================
 //
 // ob_log_fetcher.cpp liboblog / Oceanbase
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
#include "ob_log_fetcher.h"
#include "ob_log_utils.h"
#include "ob_log_common.h"

namespace oceanbase
{
  using namespace common;
  namespace liboblog
  {
    ObLogFetcher::ObLogFetcher() : mod_(ObModIds::OB_MUTATOR),
                                   allocator_(ALLOCATOR_PAGE_SIZE, mod_),
                                   inited_(false),
                                   server_selector_(NULL),
                                   rpc_stub_(NULL),
                                   start_id_(1),
                                   p_list_(),
                                   v_list_(),
                                   p_cond_(COND_SPIN_WAIT_NUM),
                                   v_cond_(COND_SPIN_WAIT_NUM),
                                   trans_count_(0),
                                   query_back_(false),
                                   query_back_timewait_us_(-1),
                                   fetch_log_timeout_us_(-1),
                                   asked_min_checkpoint_(-1)
    {
    }

    ObLogFetcher::~ObLogFetcher()
    {
      destroy();
    }

    int ObLogFetcher::init(const ObLogConfig &config,
        IObLogServerSelector *server_selector,
        IObLogRpcStub *rpc_stub,
        const int64_t start_id)
    {
      int ret = OB_SUCCESS;
      int64_t max_checkpoint = 0;
      ObServer ups_addr;

      if (inited_)
      {
        ret = OB_INIT_TWICE;
      }
      else if (NULL == (server_selector_ = server_selector)
              || NULL == (rpc_stub_ = rpc_stub))
      {
        TBSYS_LOG(WARN, "invalid param, server_selector=%p rpc_stub=%p start_id=%ld",
                  server_selector, rpc_stub, start_id);
        ret = OB_INVALID_ARGUMENT;
      }
      // NOTE: Get max checkpoint from all UPS
      else if (OB_SUCCESS != (ret = get_max_checkpoint_(max_checkpoint)))
      {
        TBSYS_LOG(WARN, "get_max_checkpoint_ fail, ret=%d", ret);
      }
      else if (max_checkpoint < start_id || 0 > start_id)
      {
        LOG_AND_ERR(ERROR, "invalid checkpoint: %ld which should be a nonnetative number "
            "and should be less than or equal to current max checkpoint: %ld. "
            "Please check your configuration again.",
            start_id, max_checkpoint);

        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        start_id_ = start_id == 0 ? max_checkpoint : start_id;
        asked_min_checkpoint_ = start_id_;

        query_back_ = config.get_query_back();
        query_back_timewait_us_ = config.get_query_back_timewait_us();

        fetch_log_timeout_us_ = config.get_fetch_log_timeout_sec();
        if (fetch_log_timeout_us_ > 0)
        {
          fetch_log_timeout_us_ *= 1000L * 1000L;
        }

        TBSYS_LOG(INFO, "start fetch log from checkpoint %ld", start_id_);

        if (OB_SUCCESS != (ret = init_mutator_list_(MAX_MUTATOR_NUM)))
        {
          TBSYS_LOG(WARN, "prepare mutator list fail, ret=%d", ret);
        }
        else if (1 != start())
        {
          TBSYS_LOG(WARN, "start thread to fetch log fail");
          ret = OB_ERR_UNEXPECTED;
        }
        else
        {
          inited_ = true;
        }
      }

      if (OB_SUCCESS != ret)
      {
        destroy();
      }
      return ret;
    }

    void ObLogFetcher::destroy()
    {
      inited_ = false;
      stop();
      wait();
      destroy_mutator_list_();
      start_id_ = 1;
      rpc_stub_ = NULL;
      server_selector_ = NULL;
      allocator_.reuse();
      trans_count_ = 0;
      query_back_ = false;
      query_back_timewait_us_ = -1;
      fetch_log_timeout_us_ = -1;
      asked_min_checkpoint_ = -1;
    }

    void ObLogFetcher::stop()
    {
      TBSYS_LOG(INFO, "stop log fetcher begin");
      CDefaultRunnable::stop();
      TBSYS_LOG(INFO, "stop log fetcher end");
    }

    int ObLogFetcher::next_mutator(ObLogMutator **mutator, const int64_t timeout)
    {
      int ret = OB_SUCCESS;
      if (NULL == mutator)
      {
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        ObLogMutator *log_mutator = NULL;
        int64_t start_time = tbsys::CTimeUtil::getTime();
        while (ob_log_alive())
        {
          ret = p_list_.pop(log_mutator);
          if (OB_SUCCESS == ret)
          {
            break;
          }

          int64_t wait_time = timeout - (tbsys::CTimeUtil::getTime() - start_time);
          if (0 >= wait_time)
          {
            break;
          }
          p_cond_.timedwait(timeout);
        }

        if (OB_SUCCESS != ret)
        {
          ret = OB_PROCESS_TIMEOUT;
        }
        else if (! ob_log_alive())
        {
          ret = OB_IN_STOP_STATE;
          if (NULL != log_mutator)
          {
            release_mutator(log_mutator);
            log_mutator = NULL;
          }
        }
        else if (NULL == log_mutator)
        {
          ret = OB_ERR_UNEXPECTED;
        }
        else
        {
          *mutator = log_mutator;
        }
      }
      return ret;
    }

    void ObLogFetcher::release_mutator(ObLogMutator *mutator)
    {
      if (NULL != mutator)
      {
        mutator->reset();
        int ret = v_list_.push(mutator);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "push mutator to v_list fail, ret=%d", ret);
        }
        else
        {
          v_cond_.signal();
        }
      }
    }

    int64_t ObLogFetcher::get_cur_id() const
    {
      return start_id_;
    }

    void ObLogFetcher::refresh_ups_list_(bool force_refresh)
    {
      static int64_t last_refresh_time = 0;

      if (force_refresh || UPS_ADDR_REFRESH_INTERVAL <= (tbsys::CTimeUtil::getTime() - last_refresh_time))
      {
        server_selector_->refresh();
        last_refresh_time = tbsys::CTimeUtil::getTime();
      }
    }

    int ObLogFetcher::get_ups_(ObServer &ups_addr, bool change_ups_addr)
    {
      OB_ASSERT(NULL != server_selector_);

      int ret = OB_SUCCESS;

      if (OB_SUCCESS != (ret = server_selector_->get_ups(ups_addr, change_ups_addr)))
      {
        if (OB_ENTRY_NOT_EXIST == ret)
        {
          TBSYS_LOG(WARN, "get_ups fail: no updateserver available, wait %ld usec and refresh updateserver list",
              NO_UPS_TIMEWAIT);

          usleep(NO_UPS_TIMEWAIT);

          // NOTE: Force to refresh UPS list
          refresh_ups_list_(true);

          ret = OB_NEED_RETRY;
        }
        else
        {
          TBSYS_LOG(ERROR, "get_ups fail, ret=%d, change_ups_addr=%s", ret, change_ups_addr ? "Y" : "N");
        }
      }

      return ret;
    }

    void ObLogFetcher::run(tbsys::CThread* thread, void* arg)
    {
      UNUSED(thread);
      UNUSED(arg);
      char *buffer = new char[FETCH_BUFFER_SIZE];
      if (NULL == buffer)
      {
        TBSYS_LOG(WARN, "allocate fetch buffer fail");
      }
      else
      {
        memset(buffer, 0, FETCH_BUFFER_SIZE);

        updateserver::ObFetchLogReq req;
        req.start_id_ = start_id_;
        req.max_data_len_ = FETCH_BUFFER_SIZE;

        int err = OB_SUCCESS;
        bool change_ups_addr = false;
        int64_t change_ups_num_on_fail = 0;
        int64_t last_log_fetched_time = tbsys::CTimeUtil::getTime();

        while (ob_log_alive(_stop))
        {
          // Refresh server list periodically
          refresh_ups_list_(false);

          ObServer ups_addr;
          int64_t max_fetched_log_id = -1;
          updateserver::ObFetchedLog res;
          res.max_data_len_ = FETCH_BUFFER_SIZE;
          res.log_data_ = buffer;

          // Get an available UPS
          if (OB_SUCCESS != (err = get_ups_(ups_addr, change_ups_addr)))
          {
            if (OB_NEED_RETRY != err)
            {
              break;
            }
          }
          // Fetch log from the specific UPS
          else if (OB_SUCCESS != (err = fetch_log_(ups_addr, req, res)))
          {
            if (OB_NEED_RETRY == err)
            {
              // Change UPS anyway if fail to fetch log from current UPS
              change_ups_addr = true;
              change_ups_num_on_fail++;
            }
            else
            {
              break;
            }
          }
          // Handle fetched log
          else if (OB_SUCCESS != (err = TIMED_FUNC(handle_fetched_log_, 10L * 1000000L, res, max_fetched_log_id)))
          {
            // NOTE: Exit anyway if handle fetched log fails
            break;
          }
          else
          {
            change_ups_addr = false;
            change_ups_num_on_fail = 0;
            start_id_ = max_fetched_log_id;
            req.start_id_ = max_fetched_log_id + 1;
            last_log_fetched_time = tbsys::CTimeUtil::getTime();
          }

          // Handle RETRY
          if (OB_NEED_RETRY == err)
          {
            int64_t log_not_served_interval = tbsys::CTimeUtil::getTime() - last_log_fetched_time;

            /// If fetch log timeout is little than 0, it means retry forever
            if (0 <= fetch_log_timeout_us_ && log_not_served_interval > fetch_log_timeout_us_
                && change_ups_num_on_fail >= server_selector_->get_ups_num())
            {
              TBSYS_LOG(ERROR, "fetch log (checkpoint=%ld) timeout, timeout=%ld/%ld, change_ups_num=%ld, ups_num=%ld",
                  req.start_id_, log_not_served_interval, fetch_log_timeout_us_,
                  change_ups_num_on_fail, server_selector_->get_ups_num());

              break;
            }

            // NOTE: Force to wait when fetch log fail from ALL UPS
            int64_t ups_num = server_selector_->get_ups_num();
            if (0 < change_ups_num_on_fail && 0 < ups_num && 0 == (change_ups_num_on_fail % ups_num))
            {
              TBSYS_LOG(INFO, "fetch log fail from ALL UPS, force to wait %ld usec, ups_num=%ld, change_ups_num=%ld",
                  FETCH_LOG_FAIL_FROM_ALL_UPS_TIMEWAIT, ups_num, change_ups_num_on_fail);
              usleep(FETCH_LOG_FAIL_FROM_ALL_UPS_TIMEWAIT);
            }
          }

          // Print checkpoint periodically
          if (REACH_TIME_INTERVAL(PRINT_CHECKPOINT_INTERVAL))
          {
            TBSYS_LOG(INFO, "current checkpoint is %ld", req.start_id_);
          }

          // Print memory usage periodically
          if (REACH_TIME_INTERVAL(PRINT_MEMORY_USAGE_INTERVAL))
          {
            ob_print_mod_memory_usage();
          }
        }

        TBSYS_LOG(INFO, "log fetcher exit: stop=%s, ob_log_alive=%s, checkpoint=%ld, asked_min_checkpoint=%ld, err=%d",
            _stop ? "Y" : "N", ob_log_alive() ? "Y" : "N", req.start_id_, asked_min_checkpoint_, err);

        delete[] buffer;

        if (OB_SUCCESS != err && OB_IN_STOP_STATE != err)
        {
          TBSYS_LOG(INFO, "abort in fetcher");
          abort();
        }
      }
    }

    int ObLogFetcher::fetch_log_(ObServer &ups_addr,
        updateserver::ObFetchLogReq &req,
        updateserver::ObFetchedLog &res)
    {
      static ObServer last_ups_addr;

      int ret = OB_SUCCESS;
      ObResultCode rc;
      int64_t retry_times = 1;

      // NOTE: req.session_ must be reset if UPS has changed
      if (ups_addr != last_ups_addr)
      {
        req.session_.reset();
        last_ups_addr = ups_addr;
      }

      do
      {
        if (OB_SUCCESS != (ret = rpc_fetch_log_(ups_addr, req, res, rc)))
        {
          break;
        }

        // FIXME: Currently OB_NEED_RETRY means OB_DATA_NOT_SERVE
        if (OB_NEED_RETRY != rc.result_code_)
        {
          break;
        }

        if (++retry_times > MAX_FETCH_LOG_FAIL_RETRY_TIMES)
        {
          TBSYS_LOG(WARN, "fetch log from UPS(%s) fail: DATA_NOT_SERVE, checkpoint=%ld, retry_times=%ld",
              to_cstring(ups_addr), req.start_id_, MAX_FETCH_LOG_FAIL_RETRY_TIMES);

          // NOTE: Retry on other UPS
          ret = OB_NEED_RETRY;
          break;
        }

        // NOTE: Wait for UPS ready
        usleep(DATA_NOT_SERVE_RETRY_TIMEWAIT);
        TBSYS_LOG(DEBUG, "LOG (checkpoint=%ld) not served on UPS(%s)", req.start_id_, to_cstring(ups_addr));
      } while (MAX_FETCH_LOG_FAIL_RETRY_TIMES >= retry_times);

      if (OB_SUCCESS == ret)
      {
        if (OB_SUCCESS != rc.result_code_)
        {
          TBSYS_LOG(WARN, "fetch log from UPS(%s) fail: checkpoint=%ld, result code = %d",
              to_cstring(ups_addr), req.start_id_, rc.result_code_);

          // NOTE: Retry on other UPS
          ret = OB_NEED_RETRY;
        }
        else
        {
          req.session_ = res.next_req_.session_;
          req.max_data_len_ = FETCH_BUFFER_SIZE;

          TBSYS_LOG(DEBUG, "fetch done %s from %s, req='%s'", to_cstring(res), to_cstring(ups_addr), to_cstring(req));
        }
      }

      return ret;
    }

    int ObLogFetcher::rpc_fetch_log_(ObServer &ups_addr,
        updateserver::ObFetchLogReq &req,
        updateserver::ObFetchedLog &res,
        ObResultCode &rc)
    {
      OB_ASSERT(req.start_id_ > 0);

      int ret = OB_SUCCESS;
      int64_t req_start_id = req.start_id_;
      bool handle_log_not_aligned = false;

      while (ob_log_alive(_stop) && 0 < req.start_id_)
      {
        ret = rpc_stub_->fetch_log(ups_addr, req, res, FETCH_LOG_TIMEOUT, rc);

        if (OB_LOG_NOT_ALIGN != ret)
        {
          break;
        }

        handle_log_not_aligned = true;

        // FIXME: Handle "LOG NOT ALIGN" ERROR
        // Go back to find an aligned log
        if (0 >= --req.start_id_)
        {
          TBSYS_LOG(ERROR, "fetch log from UPS(%s) fail: no aligned log between ID [%ld, %ld]",
              to_cstring(ups_addr), req.start_id_ + 1, req_start_id);
          break;
        }
      }

      if (! ob_log_alive(_stop))
      {
        ret = OB_IN_STOP_STATE;
      }
      else if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "fetch log from UPS(%s) fail, checkpoint=%ld, req='%s', ret=%d",
            to_cstring(ups_addr), req_start_id, to_cstring(req), ret);

        // Retry to fetch log from other UPS on error
        // NOTE: If log still not aligned, do not retry on other UPS
        if (OB_LOG_NOT_ALIGN != ret)
        {
          ret = OB_NEED_RETRY;
        }
      }
      else if (handle_log_not_aligned)
      {
        TBSYS_LOG(WARN, "log (checkpoint=%ld) not aligned. forward range [%ld, %ld], UPS=%s",
            req_start_id, req.start_id_, req_start_id, to_cstring(ups_addr));
      }

      return ret;
    }

    //////////////////////////////////////////////////

    int ObLogFetcher::init_mutator_list_(const int64_t max_mutator_num)
    {
      int ret = OB_SUCCESS;
      ret = p_list_.init(max_mutator_num);
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "init mutator p_list fail, max_mutator_num=%ld", max_mutator_num);
      }
      if (OB_SUCCESS == ret)
      {
        ret = v_list_.init(max_mutator_num);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "init mutator v_list fail, max_mutator_num=%ld", max_mutator_num);
        }
      }
      for (int64_t i = 0; OB_SUCCESS == ret && i < max_mutator_num; i++)
      {
        void *buffer = allocator_.alloc(sizeof(ObLogMutator));
        if (NULL == buffer)
        {
          TBSYS_LOG(WARN, "allocator memory to build log_mutator fail, i=%ld", i);
          ret = OB_ALLOCATE_MEMORY_FAILED;
          break;
        }

        ObLogMutator *log_mutator = new(buffer) ObLogMutator();
        if (NULL == log_mutator)
        {
          TBSYS_LOG(WARN, "build log_mutator fail, i=%ld", i);
          ret = OB_ERR_UNEXPECTED;
          break;
        }

        ret = v_list_.push(log_mutator);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "push mutator to list fail, i=%ld", i);
          break;
        }
      }
      return ret;
    }

    void ObLogFetcher::destroy_mutator_list_()
    {
      int64_t counter = v_list_.get_total() + v_list_.get_free();
      ObLogMutator *log_mutator = NULL;
      while (OB_SUCCESS == p_list_.pop(log_mutator))
      {
        if (NULL != log_mutator)
        {
          log_mutator->~ObLogMutator();
          log_mutator = NULL;
          counter--;
        }
      }
      p_list_.destroy();

      while (OB_SUCCESS == v_list_.pop(log_mutator))
      {
        if (NULL != log_mutator)
        {
          log_mutator->~ObLogMutator();
          log_mutator = NULL;
          counter--;
        }
      }
      v_list_.destroy();

      if (0 != counter)
      {
        TBSYS_LOG(WARN, "still have mutator not release, counter=%ld", counter);
      }
    }

    int ObLogFetcher::handle_fetched_log_(updateserver::ObFetchedLog &log, int64_t &max_fetched_log_id)
    {
      int ret = OB_SUCCESS;

      if (0 == log.data_len_)
      {
        TBSYS_LOG(WARN, "fetch empty log: fetched_log='%s', wait %ld usec and retry",
            to_cstring(log), FETCH_EMPTY_LOG_TIMEWAIT);
        usleep(FETCH_EMPTY_LOG_TIMEWAIT);
      }
      else
      {
        int64_t pos = 0;
        bool filter_log = false;
        int64_t min_filterd_log_id = INT64_MAX;
        int64_t max_filterd_log_id = -1;

        while (OB_SUCCESS == ret && ob_log_alive(_stop) && pos < log.data_len_)
        {
          ObLogEntry log_entry;
          if (OB_SUCCESS != (ret = log_entry.deserialize(log.log_data_, log.data_len_, pos)))
          {
            TBSYS_LOG(ERROR, "deserialize log_entry fail, ret=%d", ret);
          }
          else if (OB_SUCCESS != (ret = log_entry.check_header_integrity(false)))
          {
            TBSYS_LOG(ERROR, "check_header_integrity fail, ret=%d", ret);
          }
          else if (OB_SUCCESS != (ret = log_entry.check_data_integrity(log.log_data_ + pos, false)))
          {
            TBSYS_LOG(ERROR, "check_data_integrity fail, ret=%d", ret);
          }
          else if (asked_min_checkpoint_ > (int64_t)log_entry.seq_)
          {
            filter_log = true;

            min_filterd_log_id = std::min((int64_t)log_entry.seq_, min_filterd_log_id);
            max_filterd_log_id = log_entry.seq_;
          }
          // NOTE: we only construct mutator for UPS mutator
          else if (OB_LOG_UPS_MUTATOR == log_entry.cmd_)
          {
            if (OB_SUCCESS != (ret = construct_mutator_(log_entry, log, pos)))
            {
              if (OB_IN_STOP_STATE != ret)
              {
                TBSYS_LOG(ERROR, "construct_mutator_ fails. ret=%d", ret);
              }
            }
          }

          // Update pos and seq on success
          if (OB_SUCCESS == ret)
          {
            pos += log_entry.get_log_data_len();
            max_fetched_log_id = log_entry.seq_;
          }
        } // end while

        if (! ob_log_alive(_stop))
        {
          ret = OB_IN_STOP_STATE;
        }

        if (filter_log && 0 < min_filterd_log_id)
        {
          TBSYS_LOG(WARN, "filter not aligned log [%ld, %ld], asked_min_checkpoint=%ld, "
              "max_fetched_log_id=%ld, fetched_log='%s', stop=%s, ob_log_alive=%s",
              min_filterd_log_id, max_filterd_log_id, asked_min_checkpoint_, max_fetched_log_id, to_cstring(log),
              _stop ? "Y" : "N", ob_log_alive() ? "Y" : "N");
        }

        if (OB_SUCCESS != ret && OB_IN_STOP_STATE != ret)
        {
          TBSYS_LOG(ERROR, "handle_fetched_log_ fail, ret=%d, fetched_log='%s', "
              "asked_min_checkpoint=%ld, max_fetched_log_id=%ld",
              ret, to_cstring(log), asked_min_checkpoint_, max_fetched_log_id);
        }
      }

      return ret;
    }

    int ObLogFetcher::construct_mutator_(ObLogEntry &log_entry, updateserver::ObFetchedLog &log, const int64_t pos)
    {
      OB_ASSERT(OB_LOG_UPS_MUTATOR == log_entry.cmd_);

      int ret = OB_SUCCESS;
      ObLogMutator *log_mutator = NULL;

      // Get mutator from free list
      while (ob_log_alive(_stop))
      {
        ret = v_list_.pop(log_mutator);
        if (OB_SUCCESS != ret)
        {
          v_cond_.timedwait(WAIT_VLIST_TIMEOUT);
        }
        else
        {
          break;
        }
      }

      if (! ob_log_alive(_stop) && NULL != log_mutator)
      {
        release_mutator(log_mutator);
        log_mutator = NULL;
      }

      if (NULL == log_mutator)
      {
        if (ob_log_alive(_stop))
        {
          ret = OB_ERR_UNEXPECTED;
        }
        else
        {
          ret = OB_IN_STOP_STATE;
        }
      }
      else
      {
        // Reset content before use
        log_mutator->reset();

        // Deserialize MUTATOR
        int64_t tmp_pos = pos;
        if (OB_SUCCESS != (ret = log_mutator->deserialize(log.log_data_, log.data_len_, tmp_pos)))
        {
          TBSYS_LOG(WARN, "deserialize log_mutator fail, ret=%d", ret);
        }
        else
        {
          // Set cmd type and log ID
          log_mutator->set_cmd_type(static_cast<LogCommand>(log_entry.cmd_));
          log_mutator->set_log_id(log_entry.seq_);

          set_mutator_min_process_timestamp_(*log_mutator);

          // Push valid mutator into p_list_
          if (OB_SUCCESS != (ret = p_list_.push(log_mutator)))
          {
            TBSYS_LOG(WARN, "push to p_list fail, ret=%d", ret);
          }
          else
          {
            p_cond_.signal();

            // Update statistics information
            if (log_mutator->is_dml_mutator())
            {
              trans_count_++;
            }

            log_mutator = NULL;
          }
        }
      }

      // Recycle mutator
      if (OB_SUCCESS != ret && NULL != log_mutator)
      {
        release_mutator(log_mutator);
        log_mutator = NULL;
      }

      return ret;
    } // end construct_mutator_

    void ObLogFetcher::set_mutator_min_process_timestamp_(ObLogMutator &mutator)
    {
      int64_t min_process_timestamp = -1;
      int64_t cur_timestamp = tbsys::CTimeUtil::getTime();
      int64_t mutator_timestamp = mutator.get_mutate_timestamp();

      if (query_back_)
      {
        // NOTE: As to query back mode, the mutator should be processed after waiting for "query_back_timewait_us_".
        min_process_timestamp = mutator_timestamp + query_back_timewait_us_;
      }
      else
      {
        // NOTE: As to non-"query back" mode, mutator should be processed immediately
        min_process_timestamp = mutator_timestamp;
      }

      TBSYS_LOG(DEBUG, "MUTATOR INFO: TM=%ld, MIN_PROCESS_TM=%ld, SYS_TM=%ld, DELTA=%ld",
          mutator_timestamp, min_process_timestamp, cur_timestamp, min_process_timestamp - cur_timestamp);

      mutator.set_min_process_timestamp(min_process_timestamp);
    }

    int ObLogFetcher::get_max_checkpoint_(int64_t &max_checkpoint)
    {
      OB_ASSERT(NULL != server_selector_);

      int ret = OB_SUCCESS;
      ObServer ups_addr;
      int64_t index = 0L;
      int64_t fail_ups_num = 0;

      max_checkpoint = 0;

      // Fetch max replayed log ID from all UPS and get the max one.
      while (++index <= server_selector_->get_ups_num())
      {
        int64_t cp = -1L;
        if (OB_SUCCESS != (ret = server_selector_->get_ups(ups_addr, true)))
        {
          TBSYS_LOG(WARN, "get ups addr failed, index=%ld, ret=%d", index, ret);
        }
        else if (OB_SUCCESS != (ret = rpc_stub_->get_max_log_seq(ups_addr, GET_MAX_LOG_SEQ_TIMEOUT, cp)))
        {
          TBSYS_LOG(WARN, "failed to get max checkpoint from UPS '%s', index=%ld, ret=%d",
              to_cstring(ups_addr), index, ret);
        }
        else
        {
          TBSYS_LOG(INFO, "UPS(%s) --> MAX CHECKPOINT(%ld)", to_cstring(ups_addr), cp);
          max_checkpoint = cp > max_checkpoint ? cp : max_checkpoint;
        }

        if (OB_SUCCESS != ret)
        {
          fail_ups_num++;
        }
      }

      if (0 >= max_checkpoint)
      {
        ret = OB_ERR_UNEXPECTED;
        LOG_AND_ERR(ERROR, "Get max checkpoint from %ld UPS fail, Please check the cluster status",
            server_selector_->get_ups_num());
      }
      else
      {
        // NOTE: Return success anyway
        ret = OB_SUCCESS;
      }

      TBSYS_LOG(INFO, "max_checkpoint=%ld, fail_ups_num=%ld, available ups num=%ld, ret=%d",
          max_checkpoint, fail_ups_num, server_selector_->get_ups_num(), ret);

      return ret;
    }
  }
}
