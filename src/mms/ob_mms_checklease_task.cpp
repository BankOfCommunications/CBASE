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

#include "ob_node.h" 
#include "ob_mms_checklease_task.h" 
 
using namespace oceanbase;
using namespace mms;

ObMMSCheckleaseTask::ObMMSCheckleaseTask(ObNode *node)
{
  if(node != NULL)
  {
    node_ = node;
  }
  else
  {
    TBSYS_LOG(WARN, "invalid argument.");
  }
} 

ObMMSCheckleaseTask::~ObMMSCheckleaseTask()
{
} 

//定时检查lease时间
void ObMMSCheckleaseTask::runTimerTask(void)
{
  if(node_ == NULL)
  {
    TBSYS_LOG(ERROR, "node_ should not be NULL");
  }
  else
  {
    MMS_Status &status = node_->get_status();
    if(status == UNKNOWN)
    {
      node_->handle_lease_timeout();
    }
    else
    {
      if(node_->get_lease().lease_time + node_->get_lease().renew_interval > tbsys::CTimeUtil::getTime())
      {
        //do nothing
      }
      else
      {
        if(node_->get_lease().lease_time + node_->get_lease().lease_interval < tbsys::CTimeUtil::getTime())
        {
          TBSYS_LOG(DEBUG, "lease time out.");
          status = UNKNOWN;
          node_->handle_lease_timeout();
        }
        else
        {
          if(node_->get_lease().lease_time + node_->get_lease().renew_interval < tbsys::CTimeUtil::getTime())
          {
            TBSYS_LOG(DEBUG, "need to re_regiter");
            node_->renew_lease(ObNode::DEFAULT_RESPONSE_TIME_OUT, node_->get_monitor_server());	
          }
        }
      }
    }
  }
}
