/*
 * Copyright (C) 2007-2012 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * Description here
 *
 * Version: $Id$
 *
 * Authors:
 *   zhidong <xielun.szd@taobao.com>
 *     - some work details here
 */

#ifndef OB_ROOT_ASYNC_TASK_QUEUE_H_
#define OB_ROOT_ASYNC_TASK_QUEUE_H_

#include "common/ob_define.h"
#include "common/ob_server.h"
#include "common/ob_single_pop_queue.h"

namespace oceanbase
{
  namespace rootserver
  {
    // meta data change
    enum ObTaskType
    {
      SERVER_OFFLINE = 0,
      SERVER_ONLINE = 1,
      CONFIG_CHANGE = 2,
      SCHEMA_CHANGE = 3,
      PRIV_CHANGE = 4,
      LMS_ONLINE = 6,
      OBI_ROLE_CHANGE = 7,
      ROLE_CHANGE = 5,
      //add lbzhong [Paxos Cluster.Balance] 201607014:b
      LMS_OFFLINE = 11,
      //add:e
      //add wangdonghui [ups state exception in __all_server] 20170522 :b
      RS_ROLE_CHANGE = 9,
      //add :e
    };
    // only safe for one thread head first then pop one task
    // multi-thread can push tasks safely
    class ObRootAsyncTaskQueue
    {
    public:
      ObRootAsyncTaskQueue();
      virtual ~ObRootAsyncTaskQueue();
    public:
      struct ObSeqTask
      {
        ObTaskType type_;
        int32_t inner_port_;
        int32_t server_status_;
        int64_t remain_times_;
        int64_t max_timeout_;
        int64_t cluster_id_;
        common::ObRole role_;
        int32_t flow_percent_;
        common::ObServer server_;  //server ip will using rootserver ip when lms registed
        //add chujiajia[paxos]20151012:b
        common::ObServer *other_nodes_;
        int64_t *other_nodes_cluster_id_;
        int32_t server_count_;
        //add:e
        //add chujiajia [Paxos rs_election] 20151102:b
        int32_t paxos_num_;
        //add:e
        int32_t cluster_role_;
	      char server_version_[common::OB_SERVER_VERSION_LENGTH];
      public:
        ObSeqTask()
        {
          inner_port_ = 0;
          flow_percent_ = 0;
          server_status_ = 0;
          remain_times_ = -1;
          max_timeout_ = -1;
          task_id_ = 0;
          server_version_[0] = '\0';
          timestamp_ = -1;
	  cluster_role_ = 0;
        }
      protected:
        void print_info(void) const;
        uint64_t get_task_id(void) const;
        int64_t get_task_timestamp(void) const;
        bool operator == (const ObSeqTask & other) const;
        bool operator != (const ObSeqTask & other) const;
      private:
        friend class ObRootAsyncTaskQueue;
        friend class ObRootInnerTableTask;
        uint64_t task_id_;
        int64_t timestamp_;
      };
    public:
      int init(const int64_t max_count);
      int push(const ObSeqTask & task);
      int head(ObSeqTask & task) const;
      int pop(ObSeqTask & task);
      int64_t size(void) const;
      void print_info(void) const;
    private:
      uint64_t id_allocator_;
      DISALLOW_COPY_AND_ASSIGN(ObRootAsyncTaskQueue);
      common::ObSinglePopQueue<ObSeqTask> queue_;
    };
  }
}

#endif //OB_ROOT_ASYNC_TASK_QUEUE_H_

