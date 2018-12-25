/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 *
 * Version: 0.1: ob_ms_request_event.h,v 0.1 2011/09/26 14:01:30 zhidong Exp $
 *
 * Authors:
 *   zhidong <xielun.szd@taobao.com>
 *     - some work details if you want
 *
 */

#ifndef OCEANBASE_MERGER_REQUEST_H_
#define OCEANBASE_MERGER_REQUEST_H_

#include <pthread.h>
#include "threadmutex.h"
#include "common/ob_list.h"
#include "common/ob_vector.h"
#include "common/ob_iterator.h"
#include "common/ob_spop_spush_queue.h"
#include "common/ob_session_mgr.h"
#include "ob_ms_rpc_event.h"
#include "common/location/ob_tablet_location_range_iterator.h"

namespace oceanbase
{
  namespace mergeserver
  {
    class ObMergerRpcEvent;
    class ObMergerAsyncRpcStub;
    /// no need add mutex for thread sync
    /// warning: can only add not delete the rpc event added into the list
    class ObMergerRequest:public common::ObInnerIterator
    {
    public:
      static const int32_t MAX_SUBREQUEST_NUM = 1024*8;
      static const int64_t DEFAULT_MAX_PARELLEL_COUNT = 16;
      ObMergerRequest(common::ObTabletLocationCacheProxy * proxy, const ObMergerAsyncRpcStub * async_rpc);
      virtual ~ObMergerRequest();

    public:
      /// init finish queue max count
      int init(const uint64_t count, const uint32_t mod_id, common::ObSessionManager * session_mgr = NULL);
      bool inited()const
      {
        return finish_queue_inited_;
      }

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
      int create(ObMergerRpcEvent ** event);

      /// destroy the rpc event
      int destroy(ObMergerRpcEvent * event);

      /// reset all clear the processed queues
      int reset(void);

      /// signal for wait thread
      int signal(ObMergerRpcEvent & event);

      /// wait for all event return
      virtual int wait(const int64_t timeout);

      /// after signaled process the result according to child class
      virtual int process_result(const int64_t timeout, ObMergerRpcEvent * result,
        bool & finish) = 0;

      /// print info for debug
      void print_info(FILE * file) const;

      int update_location_cache(const common::ObServer &svr, const int32_t err,
        const common::ObScanParam & scan_param);
      int update_location_cache(const common::ObServer &svr, const int32_t err,
        const common::ObCellInfo & cell);

      /*
       * terminate session with chunkserver when request finished.
       */
      int terminate_remote_session(const common::ObServer& server, const int64_t session_id);

      /// set and get timeout percent
      double get_timeout_percent(void) const {return rpc_timeout_percent_;}
      void set_timeout_percent(const double percent) {rpc_timeout_percent_ = percent;}
    private:
      // check and remove the event from the wait queue
      int remove_wait_queue(ObMergerRpcEvent * event, bool & is_valid);

      // process the new event
      int process_rpc_event(const int64_t timeout, ObMergerRpcEvent * event, bool & is_finish);

      // create a new rpc event
      int create_rpc_event(ObMergerRpcEvent ** event);

      /// delete the rpc event
      void destroy_rpc_event(ObMergerRpcEvent * event);

      // release all success processed rpc event
      int release_succ_event(uint64_t & count);

      int remove_invalid_event_in_finish_queue(const int64_t timeout);
      // statistic used time for every server
      void request_used_time(const ObMergerRpcEvent * rpc);
    private:
      uint32_t session_id_;
      common::ObSessionManager *session_mgr_;
      // async rpc param
      int64_t timeout_;
      // not deep copy the buffer of read_param
      const common::ObReadParam * read_param_;
      /// async rpc stub
      const ObMergerAsyncRpcStub * async_rpc_stub_;
      // for allocate request id
      static uint64_t id_allocator_;
      // every reqeust id
      uint64_t request_id_;
      // finished rpc event queue
      common::ObSPopSPushQueue finish_queue_;
      bool finish_queue_inited_;
      // wait rpc event queue for debug info
      tbsys::CThreadMutex lock_;
      common::ObList<ObMergerRpcEvent * > waiting_queue_;
      // processed rpc event queue
      common::ObVector<ObMergerRpcEvent *> result_queue_;
      // for param tablet location position
      common::ObTabletLocationCacheProxy * cache_proxy_;
      ObStringBuf buffer_pool_;
      // timeout percent for rpc
      double rpc_timeout_percent_;
    protected:
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

#endif // OCEANBASE_MERGER_REQUEST_H_
