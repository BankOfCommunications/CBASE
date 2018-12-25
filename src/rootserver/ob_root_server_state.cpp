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
 *   rongxuan<rongxuan.lc@alipay.com>
 *     - some work details here
 */

#include "ob_root_server_state.h"
#include "ob_root_server2.h"
using namespace oceanbase::common;
using namespace oceanbase::rootserver;

int ObRootServerState::init(ObRootServer2 *root_server, tbsys::CRWLock *server_manager_lock, const ObChunkServerManager* server_manager)
{
  int ret = OB_SUCCESS;
  if (NULL == server_manager_lock || NULL == server_manager
      || NULL == root_server)
  {
    ret = OB_INVALID_ARGUMENT;
    TBSYS_LOG(WARN, "invalid argument. lock=%p, server_manager=%p, root_server=%p",
        server_manager_lock, server_manager, root_server);
  }
  if (OB_SUCCESS == ret)
  {
    root_server_ = root_server;
    server_manager_rwlock_ = server_manager_lock;
    server_manager_ = const_cast<ObChunkServerManager*>(server_manager);
  }
  return ret;
}

int ObRootServerState::set_daily_merge_error(const char* err_msg, const int64_t length)
{
  int ret = OB_ERROR;
  if (err_msg == NULL || length > OB_MAX_ERROR_MSG_LEN)
  {
    TBSYS_LOG(WARN, "invalid argument. err_msg=%p, len=%ld", err_msg, length);
    ret = OB_INVALID_ARGUMENT;
  }
  if (OB_SUCCESS == ret)
  {
    tbsys::CThreadGuard guard(&mutex_);
    if (is_daily_merge_error_)
    {
      TBSYS_LOG(WARN, "already have daily_merge error.");
    }
    else
    {
      is_daily_merge_error_ = true;
      memcpy(err_msg_, err_msg, length);
      err_msg_[length] = '\0';
    }
  }
  return ret;
}
bool ObRootServerState::is_daily_merge_tablet_error()const
{
  tbsys::CThreadGuard guard(&mutex_);
  return is_daily_merge_error_;
}
char* ObRootServerState::get_error_msg()
{
  tbsys::CThreadGuard guard(&mutex_);
  return err_msg_;
}
void ObRootServerState::clean_daily_merge_error()
{
  tbsys::CThreadGuard guard(&mutex_);
  is_daily_merge_error_ = false;
}
