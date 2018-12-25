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
#ifndef OB_PRIVILEGE_MANAGER_H_
#define OB_PRIVILEGE_MANAGER_H_

#include "ob_privilege.h"
#include "tbsys.h"

class ObPrivilegeManagerTest;
namespace oceanbase
{
  namespace common
  {
    class ObPrivilegeManager
    {
      public:
        ObPrivilegeManager();
        ~ObPrivilegeManager();
        int renew_privilege(const ObPrivilege & privilege);
        int get_newest_privilege(const ObPrivilege ** & pp_privilege);
        int release_privilege(const ObPrivilege ** pp_privilege);
        int64_t get_newest_privilege_version();
      public:
        static const int64_t MAX_VERSION_COUNT = 5;
      public:
        // for testing
        friend class ::ObPrivilegeManagerTest;
      public:
        /**
         * @synopsis 更新线程在外面ob_malloc, renew的时候释放
         */
        struct PrivilegeWrapper
        {
          const ObPrivilege *p_privilege_;
          uint64_t ref_count_;
          PrivilegeWrapper():p_privilege_(NULL),ref_count_(0)
          {
          }
        };
      private:
        PrivilegeWrapper privilege_wrappers_[MAX_VERSION_COUNT];
        tbsys::CThreadMutex lock_;
        int64_t newest_version_index_;
        int64_t newest_privilege_version_;

    };
  }// namespace common
}//namespace oceanbase

#endif
