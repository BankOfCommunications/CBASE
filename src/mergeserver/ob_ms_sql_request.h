#ifndef OCEANBASE_MS_SQL_RPC_REQUEST_H_
#define OCEANBASE_MS_SQL_RPC_REQUEST_H_

#include <pthread.h>
#include "threadmutex.h"
#include "common/ob_list.h"
#include "common/ob_vector.h"
#include "common/ob_row_iterator.h"
#include "common/ob_spop_spush_queue.h"
#include "common/ob_session_mgr.h"
#include "common/ob_lighty_queue.h"
#include "sql/ob_sql_scan_param.h"
#include "ob_ms_sql_rpc_event.h"
#include "common/location/ob_tablet_location_range_iterator.h"

namespace oceanbase
{
  namespace mergeserver
  {
    class ObMsSqlRpcEvent;
    class ObMergerAsyncRpcStub;
    /// no need add mutex for thread sync
    /// warning: can only add not delete the rpc event added into the list
    class ObMsSqlRequest:public common::ObRowIterator
    {
    public:
      static const int32_t MAX_SUBREQUEST_NUM = 1024*8;
      static const int32_t MAX_SUB_GET_REQUEST_NUM = 256;  // max Chunkserver count
      static const int64_t DEFAULT_MAX_PARELLEL_COUNT = 16;
      static const int64_t DEFAULT_MAX_TABLET_COUNT_PERQ = 64;
      static const int64_t DEFAULT_MAX_CS_RESULT_MEM_SIZE = 1024*1024*512;
      static const int64_t DEFAULT_TIMEOUT = 1000 * 1000;
      static const int32_t DEFAULT_TIMEOUT_PERCENT = 33;
      ObMsSqlRequest();
      virtual ~ObMsSqlRequest();
      void set_tablet_location_cache_proxy(common::ObTabletLocationCacheProxy * proxy);
      void set_merger_async_rpc_stub(const ObMergerAsyncRpcStub * async_rpc);

    public:
      /// init finish queue max count
      int init(const uint64_t count, const uint32_t mod_id,  common::ObSessionManager * session_mgr = NULL);
      bool inited()const
      {
        return finish_queue_inited_;
      }
      void alloc_request_id();
      int close(void);

      void set_session_id(const uint32_t session_id)
      {
        session_id_ = session_id;
      }

      /// get/set timeout for rpc
      int64_t get_timeout(void) const;
      void set_timeout(const int64_t timeout);

      /// get request id
      uint64_t get_request_id(void) const;

      const ObMergerAsyncRpcStub * get_rpc(void) const;

      // get/set read param
      virtual const common::ObReadParam * get_request_param(int64_t & timeout) const;
      void set_request_param(const common::ObReadParam * param, const int64_t timeout);

    public:
      /// add a rpc event and return the new event
      int create(ObMsSqlRpcEvent ** event);

      /// destroy the rpc event
      int destroy(ObMsSqlRpcEvent * event);

      /// reset all clear the processed queues
      int reset(void);

      /// signal for wait thread
      int signal(ObMsSqlRpcEvent & event);

      /// wait for all event return
      virtual int wait(const int64_t timeout);

      /// wait for all event return
      virtual int wait_single_event(int64_t &timeout);

      /// after signaled process the result according to child class
      virtual int process_result(const int64_t timeout, ObMsSqlRpcEvent * result,
        bool & finish) = 0;

      /// print info for debug
      void print_info(FILE * file) const;

      int update_location_cache(const common::ObServer &svr, const int32_t err,
        const sql::ObSqlScanParam & scan_param);
      int update_location_cache(const common::ObServer &svr, const int32_t err,
        const common::ObCellInfo & cell);
      int update_location_cache(const common::ObServer &svr, const int32_t err,
         const uint64_t table_id, const common::ObRowkey& rowkey);

      /*
       * terminate session with chunkserver when request finished.
       */
      int terminate_remote_session(const common::ObServer& server, const int64_t session_id);
      int invalidate_request_id();

      int64_t get_waiting_queue_size();
      int64_t get_finish_queue_size();
      bool is_finish();

      /// set and get timeout percent
      int32_t get_timeout_percent(void) const;
      void set_timeout_percent(const int32_t percent);
    private:
      // record request latency for user
      void request_used_time(const ObMsSqlRpcEvent * rpc);

      // check and remove the event from the wait queue
      int remove_wait_queue(ObMsSqlRpcEvent * event, bool & is_valid);

      // process the new event
      int process_rpc_event(const int64_t timeout, ObMsSqlRpcEvent * event, bool & is_finish);

      // create a new rpc event
      int create_rpc_event(ObMsSqlRpcEvent ** event);

      /// delete the rpc event
      void destroy_rpc_event(ObMsSqlRpcEvent * event);

      // release all success processed rpc event
      int release_succ_event(uint64_t & count);

      int remove_invalid_event_in_finish_queue(const int64_t timeout);

    private:
      uint32_t session_id_;
      common::ObSessionManager *session_mgr_;
      // async rpc param
      int64_t timeout_;
      int32_t rpc_timeout_percent_;
      // not deep copy the buffer of read_param
      const common::ObReadParam * read_param_;
      /// async rpc stub
      const ObMergerAsyncRpcStub * async_rpc_stub_;
      // for allocate request id
      static uint64_t id_allocator_;
      // every reqeust id
      volatile uint64_t request_id_;
      // finished rpc event queue
      common::LightyQueue finish_queue_;
      bool finish_queue_inited_;
      // finish queue lock
      tbsys::CThreadMutex flock_; //multi io thread push result to finish queue
      // wait rpc event queue for debug info
      tbsys::CThreadMutex lock_;
      common::ObList<ObMsSqlRpcEvent * > waiting_queue_;
      // processed rpc event queue
      common::ObVector<ObMsSqlRpcEvent *> result_queue_;
      // for param tablet location position
      common::ObTabletLocationCacheProxy * cache_proxy_;
      ObStringBuf buffer_pool_;
      bool finish_;
    protected:
      inline void set_finish(bool is_finish)
      {
        finish_ = is_finish;
      }

      inline common::ObTabletLocationCacheProxy *get_cache_proxy()
      {
        return cache_proxy_;
      }

      inline const common::ObTabletLocationCacheProxy *get_cache_proxy() const
      {
        return cache_proxy_;
      }

      inline ObStringBuf &get_buffer_pool()
      {
        return buffer_pool_;
      }
    };
  }
}

#endif // OCEANBASE_MS_SQL_RPC_REQUEST_H_
