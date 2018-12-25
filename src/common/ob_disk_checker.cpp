/**
 * (C) 2010-2011 Taobao Inc.
 *
 *  This program is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU General Public License 
 *  version 2 as published by the Free Software Foundation. 
 *  
 *  ob_disk_checker.cpp
 *
 *  Authors:
 *    zian <yunliang.shi@alipay.com>
 *
 */

#include "ob_disk_checker.h"
#include <math.h>
#include "tblog.h"

namespace oceanbase
{
  namespace common
  {
    ObDiskChecker ObDiskCheckerSingleton::disk_checker_;

    ObDiskChecker::ObDiskChecker()
    {
      reset();
    }

    int ObDiskChecker::init(const int32_t* disk_no_array, const int32_t disk_num)
    {
      int ret = OB_SUCCESS;
      tbsys::CWLockGuard guard(lock_);
      for (int32_t i = 0; i < disk_num && i < common::OB_MAX_DISK_NUMBER; i++)
      {
        if (disk_no_array[i] < common::OB_MAX_DISK_NUMBER)
        {
          disk_no_array_[disk_no_array[i]] = disk_no_array[i];
        }
        else
        {
          TBSYS_LOG(ERROR, "disk_no:%d exceed max_disk_no:%ld",
              disk_no_array[i], common::OB_MAX_DISK_NUMBER);
          ret = OB_ERROR;
        }
      }

      return ret;
    }

    void ObDiskChecker::reset()
    {
      memset(disk_no_array_, 0, sizeof(disk_no_array_));
      memset(check_status_, 0, sizeof(check_status_));
    }

    bool ObDiskChecker::has_bad_disk(int32_t& bad_disk)
    {
      bool ret = false;
      tbsys::CRLockGuard guard(lock_);
      for (int32_t disk_no = 1; disk_no < common::OB_MAX_DISK_NUMBER; disk_no++) 
      {
        ObCheckStatus status = CHECK_NORMAL;
        if (OB_SUCCESS == get_check_stat(disk_no, status) &&
            CHECK_ERROR == status)
        {
          bad_disk = disk_no;
          ret = true;
          break;
        }
      }

      return ret;
    }

    void ObDiskChecker::set_check_stat(const int32_t disk_no, const ObCheckStatus status)
    {
      int32_t bad_disk = 0;
      if (CHECK_ERROR == status && has_bad_disk(bad_disk) &&
          bad_disk != disk_no)
      {
        TBSYS_LOG(WARN, "already has bad_disk:%d, cannot unload disk_no:%d",
            bad_disk, disk_no);
      }
      else
      {
        tbsys::CWLockGuard guard(lock_);
        int32_t index = disk_no_array_[disk_no];
        if (index == disk_no)
        {
          check_status_[index] = status;
          TBSYS_LOG(WARN, "set disk:%d check_stat:%d", disk_no, status);
        }
        else
        {
          TBSYS_LOG(WARN, "set_check_stat disk_no:%d status:%d failed, not found",
              disk_no, status);
        }
      }
    }

    int ObDiskChecker::get_check_stat(const int32_t disk_no, ObCheckStatus &status)
    {
      int ret = OB_SUCCESS;
      int32_t index = disk_no_array_[disk_no];
      if (index == disk_no)
      {
        status = check_status_[index];
      }
      else
      {
        ret = OB_ERROR;
      }

      return ret;
    }
  }
}
