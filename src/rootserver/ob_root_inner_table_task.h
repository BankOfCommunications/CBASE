/**
  * (C) 2007-2010 Taobao Inc.
  *
  * This program is free software; you can redistribute it and/or modify
  * it under the terms of the GNU General Public License version 2 as
  * published by the Free Software Foundation.
  *
  * Version: $Id$
  *
  * Authors:
  *   zhidong <xielun.szd@taobao.com>
  *     - some work details if you want
  */

#ifndef OB_ROOT_INNER_TABLE_TASK_H_
#define OB_ROOT_INNER_TABLE_TASK_H_

#include "common/ob_timer.h"
#include "common/ob_define.h"
#include "ob_root_async_task_queue.h"
//add lbzhong [Paxos Cluster.Balance] 201607014:b
#include "ob_root_server2.h"
//add:e

namespace oceanbase
{
  namespace rootserver
  {
    class ObRootSQLProxy;
    class ObRootInnerTableTask: public common::ObTimerTask
    {
    public:
      ObRootInnerTableTask();
      virtual ~ObRootInnerTableTask();
    public:
      int init(const int cluster_id, ObRootSQLProxy & proxy, common::ObTimer & timer, ObRootAsyncTaskQueue & queue
               //add lbzhong [Paxos Cluster.Balance] 201607014:b
               , ObRootServer2 *root_server
               //add:e
               );
      void runTimerTask(void);
    private:
      // check inner stat
      bool check_inner_stat(void) const;
      // process head task
      int process_head_task(void);
      // update all server table
      int modify_all_server_table(const ObRootAsyncTaskQueue::ObSeqTask & task);
      // update all cluster table
      int modify_all_cluster_table(const ObRootAsyncTaskQueue::ObSeqTask & task);
      // make sure all other clusters are slave
      int clean_other_masters(void);
      //add lbzhong [Paxos Cluster.Balance] 201607014:b
      int check_cluster_alive(const ObRootAsyncTaskQueue::ObSeqTask &task);
      //add:e
      //add bingo [Paxos __all_cluster] 20170713:b
      int process_offline_cluster(int32_t cluster_id);
      int process_cluster_while_all_mscs_offline(int32_t cluster_id);
      //add:e
    private:
      // every run process task timeout
      const static int64_t MAX_TIMEOUT = 2000000; // 2s
      const static int64_t TIMEOUT = 1000000; // 1s
      const static int64_t RETRY_TIMES = 1;
      int cluster_id_;
      common::ObTimer * timer_;
      ObRootAsyncTaskQueue * queue_;
      ObRootSQLProxy * proxy_;
      //add lbzhong [Paxos Cluster.Balance] 201607014:b
      ObRootServer2 *root_server_;
      //add:e
    };
    inline bool ObRootInnerTableTask::check_inner_stat(void) const
    {
      return ((timer_ != NULL) && (queue_ != NULL) && (NULL != proxy_));
    }
  }
}

#endif //OB_ROOT_INNER_TABLE_TASK_H_
