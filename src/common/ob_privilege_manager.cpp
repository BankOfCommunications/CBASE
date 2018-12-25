/* (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software: you can redistribute it and/or
 *  modify it under the terms of the GNU General Public License
 *  version 2 as published by the Free Software Foundation.
 *
 * Version: 0.1
 *
 * Authors:
 *    Wu Di <lide.wd@taobao.com>
 */
#include "ob_privilege_manager.h"

namespace oceanbase
{
  namespace common
  {
    ObPrivilegeManager::ObPrivilegeManager()
      : newest_version_index_(-1), newest_privilege_version_(0)
    {
    }
    ObPrivilegeManager::~ObPrivilegeManager()
    {
    }
    int ObPrivilegeManager::release_privilege(const ObPrivilege ** pp_privilege)
    {
      int ret = OB_SUCCESS;
      if (NULL == pp_privilege)
      {
        ret = OB_INVALID_ARGUMENT;
        TBSYS_LOG(WARN, "release privilege failed, ret=%d", ret);
      }
      else
      {
        tbsys::CThreadGuard lock(&lock_);
        PrivilegeWrapper *wrapper = const_cast<PrivilegeWrapper*>(reinterpret_cast<const PrivilegeWrapper*>(pp_privilege));
        wrapper->ref_count_--;
      }
      return ret;
    }
    int64_t ObPrivilegeManager::get_newest_privilege_version()
    {
      tbsys::CThreadGuard lock(&lock_);
      return newest_privilege_version_;
    }
    int ObPrivilegeManager::renew_privilege(const ObPrivilege & privilege)
    {
      int ret = OB_SUCCESS;
      tbsys::CThreadGuard lock(&lock_);
      int64_t new_version = privilege.get_version();
      if (new_version < newest_privilege_version_)
      {
        ret = OB_ERR_OLDER_PRIVILEGE_VERSION;
        TBSYS_LOG(WARN, "receive a older privilege version,local version=[%ld], incoming version=[%ld],ret=%d", newest_privilege_version_, new_version, ret);
      }
      else
      {
        int pos = -1;
        for (int i = 0;i < MAX_VERSION_COUNT; ++i)
        {
          if (privilege_wrappers_[i].ref_count_ == 0)
          {
            pos = i;
            break;
          }
        }
        if (pos != -1)
        {
          if (privilege_wrappers_[pos].p_privilege_ != NULL)
          {
            privilege_wrappers_[pos].p_privilege_->~ObPrivilege();
            ob_free(const_cast<ObPrivilege*>(privilege_wrappers_[pos].p_privilege_));
            privilege_wrappers_[pos].p_privilege_ = NULL;
          }
          privilege_wrappers_[pos].p_privilege_ = &privilege;
          privilege_wrappers_[pos].ref_count_ = 0;
          newest_version_index_ = pos;
          newest_privilege_version_ = new_version;
          TBSYS_LOG(INFO, "renew privilege success!!!");
          privilege.print_info();
        }
        else
        {
          ret = OB_NO_EMPTY_ENTRY;
        }
      }
      return ret;
    }
    int ObPrivilegeManager::get_newest_privilege(const ObPrivilege ** &pp_privilege)
    {
      int ret = OB_SUCCESS;
      tbsys::CThreadGuard lock(&lock_);
      if (newest_version_index_ != -1)
      {
        privilege_wrappers_[newest_version_index_].ref_count_++;
        pp_privilege = &(privilege_wrappers_[newest_version_index_].p_privilege_);
      }
      else
      {
        ret = OB_ERR_NO_AVAILABLE_PRIVILEGE_ENTRY;
      }
      return ret;

    }

  }//namespace common

}// namespace oceanbase
