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
 *   rongxuan <rongxuan.lc@taobao.com>
 *     - some work details if you want
 */

#include "ob_ups_keep_alive.h"

namespace oceanbase
{
  namespace updateserver
  {
    ObUpsKeepAlive::ObUpsKeepAlive()
    {
      slave_mgr_ = NULL;
      role_mgr_ = NULL;
      is_registered_to_ups_ = false;
    }

    ObUpsKeepAlive::~ObUpsKeepAlive()
    {
    }

    int ObUpsKeepAlive::init(ObUpsSlaveMgr *slave_mgr, ObRoleMgr *role_mgr, ObiRole *obi_role, const int64_t keep_alive_timeout)
    {
      int err = OB_SUCCESS;
      if (NULL == slave_mgr || NULL == role_mgr || NULL == obi_role)
      {
        err = OB_INVALID_ARGUMENT;
      }
      else
      {
        slave_mgr_ = slave_mgr;
        role_mgr_ = role_mgr;
        obi_role_ = obi_role;
        keep_alive_timeout_ = keep_alive_timeout;
      }
      return err;
    }

    void ObUpsKeepAlive::renew_keep_alive_time()
    {
      int64_t cur_time_us = tbsys::CTimeUtil::getTime(); 
      TBSYS_LOG(DEBUG, "slave renew keep alive msg. cur_time = %ld, last_keep_alive_time=%ld", cur_time_us, last_keep_alive_time_);
      last_keep_alive_time_ = cur_time_us;
    }

    void ObUpsKeepAlive::set_is_registered_to_ups(const bool is_registered_to_ups)
    {
      if (is_registered_to_ups)
      {
        TBSYS_LOG(DEBUG, "start to check inner keep alive msg");
        renew_keep_alive_time();
      }
      else
      {
        TBSYS_LOG(DEBUG, "stop to check inner keep alive msg");
      }
      is_registered_to_ups_ = is_registered_to_ups;
    }

    void ObUpsKeepAlive::clear()
    {
      if (NULL != _thread)
      {
        delete[] _thread;
        _thread = NULL;
      }
      _stop = false;
    }

    void ObUpsKeepAlive::run(tbsys::CThread* thread, void* arg)
    {
      UNUSED(thread);
      UNUSED(arg);

      int err = OB_SUCCESS;
      int64_t count = 0;
      while(!_stop)
      {
        if (common::ObRoleMgr::MASTER == role_mgr_->get_role() && 0 == count % DEFAULT_KEEP_ALIVE_SEND_PERIOD)
        {
          err = slave_mgr_->grant_keep_alive();
          if (OB_SUCCESS != err)
          {
            TBSYS_LOG(WARN, "fail to grant keep alive to slave, err = %d", err);
          }
        }
        else if ((common::ObiRole::SLAVE == obi_role_->get_role() 
              || common::ObRoleMgr::SLAVE == role_mgr_->get_role()) 
              && is_registered_to_ups_)
        {
          int64_t cur_time_us = tbsys::CTimeUtil::getTime(); 
          if (last_keep_alive_time_ + keep_alive_timeout_ < cur_time_us)
          {
            TBSYS_LOG(WARN, "keep_alive msg timeout, last_time = %ld, cur_time = %ld, duration_time = %ld", last_keep_alive_time_,
                cur_time_us, keep_alive_timeout_);
            role_mgr_->set_state(ObRoleMgr::INIT);
          }
          else
          {
            TBSYS_LOG(DEBUG, "keep alive, last_time = %ld, cur_time = %ld, duration_time = %ld", last_keep_alive_time_, 
                cur_time_us, keep_alive_timeout_);
          }
        }
        else
        {
          TBSYS_LOG(DEBUG, "not ready to deal with keep_alive msg");
        }
        count ++;
        usleep(DEFAULT_CHECK_PERIOD);
      }
    }
  }
}
