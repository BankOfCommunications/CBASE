/**
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * Version: $Id$
 * First Create_time: 2011-8-5
 *
 * Authors:
 *   rongxuan <rongxuan.lc@taobao.com>
 *     - some work details if you want
 */

#include "ob_mms_heartbeat_task.h"
#include "ob_monitor.h"
#include "common/ob_trace_log.h"

using namespace oceanbase;
using namespace oceanbase::mms;
using namespace oceanbase::common;

ObMMSHeartbeatTask::ObMMSHeartbeatTask(ObMonitor *monitor)
{
  if(monitor != NULL)
  {
  monitor_ = monitor;
  timeout_ = monitor_->get_timeout();
  retry_times_ = monitor_->get_retry_times();
  }
}

ObMMSHeartbeatTask::~ObMMSHeartbeatTask()
{
}

void ObMMSHeartbeatTask::runTimerTask()
{
  node_map_t &node_map = monitor_->get_node_map();
  node_map_t::iterator it = node_map.begin();
  int err = OB_SUCCESS;
  int j = 0;
  CLEAR_TRACE_LOG();
  FILL_TRACE_LOG("start heartbeat");
  while(it != node_map.end())
  {
    j ++;
    FILL_TRACE_LOG("in the %d app+instance map", j);
    mms_value *pvalue = it->second;
    if(pvalue != NULL)
    {
      node_list_type *pnodelist = &(pvalue->node_list);
      if(pvalue->has_master == false)
      {
        ObServer new_master;
        FILL_TRACE_LOG("no master exist in the list.");
        err = monitor_->select_master(pnodelist, new_master);
        if(err != OB_SUCCESS)
        {
          TBSYS_LOG(ERROR, "select master fail.");
        }
        else
        {
          pvalue->has_master = true;
          pvalue->master.set_ipv4_addr(new_master.get_ipv4(), new_master.get_port());
        }
        FILL_TRACE_LOG("select master. err=%d", err);
        err = OB_SUCCESS;
      }

      if(err == OB_SUCCESS)
      {
        if(pnodelist != NULL)
        {
          node_list_type::iterator it_list = pnodelist->begin();
          int i = 0;
          FILL_TRACE_LOG("heartbeat to node in the %d list", j);
          while(it_list != pnodelist->end())
          {
            i++;
            //如果正常，则发心跳和租约
            if((*it_list)->is_valid == true)
            {
                FILL_TRACE_LOG("heartbeat to the %d node in the %d list.", i, j);
                MMS_Status status;
                err = monitor_->heartbeat(timeout_, pvalue->master, (*it_list)->node_server, status);
                if(OB_SUCCESS != err)
                {
                  (*it_list)->is_valid = false;
                }
                else
                {
                  (*it_list)->status = status;
                }
                it_list ++;
                FILL_TRACE_LOG("node normal. heartbeat err=%d", err);
            }

            //如果已经超时，则根据他的role分别调用不同的处理函数
            else
            {
              FILL_TRACE_LOG("node un_normal. check the timeout");
              if((*it_list)->lease.lease_time + (*it_list)->lease.lease_interval < tbsys::CTimeUtil::getTime())
              {
                if((*it_list)->status == MASTER_ACTIVE)
                {
                  FILL_TRACE_LOG("master node failure.");
                  pvalue->has_master = false;
                  ObServer new_master;
                  err = monitor_->select_master(pnodelist, new_master);
                  if(err != OB_SUCCESS)
                  {
                    TBSYS_LOG(ERROR, "fail to select master");
                  }
                  else
                  {
                    pvalue->has_master = true;
                    pvalue->master.set_ipv4_addr(new_master.get_ipv4(), new_master.get_port());
                  }
                  FILL_TRACE_LOG("select new master .err=%d", err);
                  //通知新主的产生
                  if(OB_SUCCESS == err)
                  {
                    err = monitor_->change_master(timeout_, retry_times_, pvalue->master);
                    if(err != OB_SUCCESS)
                    {
                      TBSYS_LOG(ERROR, "fail to change master.err=%d", err);
                    }
                    FILL_TRACE_LOG("notify the new master. err=%d", err);
                  }
                }
                else
                {
                  FILL_TRACE_LOG("slave node failure.");
                  err = monitor_->report_slave_failure(timeout_, retry_times_, (*it_list)->node_server, pvalue->master);
                  if(err != OB_SUCCESS)
                  {
                    TBSYS_LOG(WARN, "fail to report slave failure.err=%d", err);
                  }
                  FILL_TRACE_LOG("tell the master, slave down, err=%d", err);
                }
                //删除该节点
                pnodelist->erase(it_list);
                it_list++;
                FILL_TRACE_LOG("delete the failure node");
              }
              else
              {
                it_list ++;
              }
            }
          }
        }
      }
    }
    it++;
  }
  PRINT_TRACE_LOG();
}
