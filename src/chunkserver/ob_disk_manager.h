/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * ob_disk_manager.h is for what ...
 *
 * Version: $id$
 *
 * Authors:
 *   maoqi <maoqi@taobao.com>
 *
 */

#ifndef OCEANBASE_CHUNKSERVER_OB_DISK_MANAGER_H_
#define OCEANBASE_CHUNKSERVER_OB_DISK_MANAGER_H_
#include "common/ob_define.h"
#include <tbsys.h>

namespace oceanbase
{
  namespace chunkserver
  {
    enum ObDiskStatus
    {
      DISK_NORMAL = 0,
      DISK_ERROR,
    };

    struct ObDisk
    {
      ObDisk():capacity_(0),avail_(0),disk_no_(0),status_(DISK_NORMAL),last_check_io_error_(0) {};
      int64_t capacity_;
      int64_t avail_;
      int32_t disk_no_;
      ObDiskStatus status_;
      uint32_t last_check_io_error_;
    };


    class ObDiskManager
    {
      public:
        static const uint64_t ALARM_INTERVAL = 300 * 1000L * 1000L;
        static const uint32_t ALARM_TIMES = 100;

      public:
        ObDiskManager();
        int scan(const char *data_dir, const int64_t max_sstable_size);
        int32_t get_dest_disk();
        int32_t get_disk_for_migrate();
        void shrink_space(const int32_t disk_no,const int64_t used);
        void add_used_space(const int32_t disk_no,const int64_t used,
          const bool decr_pending_cnt = true);
        void release_space(const int32_t disk_no,const int64_t size);
        int64_t get_total_capacity(void) const;
        int64_t get_total_used(void) const;
        void add_sstable_num(const int32_t disk_no,const int32_t num);
        int set_disk_status(const int32_t disk_no,const ObDiskStatus stat);
        const int32_t *get_disk_no_array(int32_t& disk_num) const;
        bool is_disk_avail(const int32_t disk_no) const;
        int check_disk(int32_t& bad_disk);
        int add_mount_disk(const char* data_dir, const char* mount_path, int32_t& disk_no);
        void dump();
      private:
        void reset();
        int32_t find_disk(const int32_t disk_no) const;
        static const char *PROC_MOUNTS_FILE;
        static const int32_t PROC_MOUNTS_FILE_SIZE;
        bool is_mount_point(const char *path) const;
        mutable tbsys::CRWLock lock_;
        int32_t disk_num_;
        int32_t pending_files_[common::OB_MAX_DISK_NUMBER];
        int32_t disk_no_array_[common::OB_MAX_DISK_NUMBER];
        int64_t max_sstable_size_;

        // in daily merging,we use this filed to record 
        // how many sstables have been saved in this disk_
        int32_t sstable_files_[common::OB_MAX_DISK_NUMBER];
        ObDisk disk_[common::OB_MAX_DISK_NUMBER];
        uint64_t last_alarm_us_;
        uint32_t alarm_times_;
    };
  } /* chunkserver */
} /* oceanbase */
#endif
