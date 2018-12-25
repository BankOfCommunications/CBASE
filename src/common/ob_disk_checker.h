/**
 * (C) 2010-2011 Taobao Inc.
 *
 *  This program is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU General Public License 
 *  version 2 as published by the Free Software Foundation. 
 *  
 *  ob_disk_checker.h
 *
 *  Authors:
 *    zian <yunliang.shi@alipay.com>
 *
 */

#ifndef __OB_DISK_ACCUMULATOR_H
#define __OB_DISK_ACCUMULATOR_H

#include "ob_define.h"
#include "tbtimeutil.h"
#include "ob_atomic.h"
#include <tbsys.h>

namespace oceanbase
{
  namespace common
  {
    enum ObCheckStatus
    {
      CHECK_NORMAL = 0,
      CHECK_ERROR,
    };

    class ObDiskChecker
    {
      public:
        ObDiskChecker();
        ~ObDiskChecker(){}

      public:
        int init(const int32_t* disk_no_array, const int32_t disk_num);
        void set_check_stat(const int32_t disk_no, const ObCheckStatus status);
        bool has_bad_disk(int32_t& bad_disk);

      private:
        void reset();
        int get_check_stat(const int32_t disk_no, ObCheckStatus &status);

      private:
        int32_t disk_no_array_[common::OB_MAX_DISK_NUMBER];
        ObCheckStatus check_status_[common::OB_MAX_DISK_NUMBER];
        mutable tbsys::CRWLock lock_;
    };



    class ObDiskCheckerSingleton
    {
      public:
        static ObDiskChecker&  get_instance()
        {
          return disk_checker_;
        }

      private:
        ObDiskCheckerSingleton(){}
        ObDiskCheckerSingleton(const ObDiskCheckerSingleton &) {}

      private:
        static ObDiskChecker disk_checker_;
    };
  }
}

#endif
