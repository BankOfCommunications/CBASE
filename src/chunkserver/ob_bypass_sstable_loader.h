/**
 * (C) 2010-2012 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *  
 * ob_bypass_sstable_loader.h for bypass sstable loader. 
 *
 * Authors:
 *   huating <huating.zmq@taobao.com>
 *
 */
#ifndef OCEANBASE_CHUNKSERVER_BYPASS_SSTABLE_LOADER_H_
#define OCEANBASE_CHUNKSERVER_BYPASS_SSTABLE_LOADER_H_

#include <dirent.h>
#include <tbsys.h>
#include "common/ob_tablet_info.h"
#include "sstable/ob_disk_path.h"

namespace oceanbase
{
  namespace chunkserver
  {
    class ObTabletManager;

    class ObBypassSSTableLoader : public tbsys::CDefaultRunnable
    {
      public: 
        ObBypassSSTableLoader();
        ~ObBypassSSTableLoader();

        int init(ObTabletManager* manager);
        void destroy();

        int start_load(const common::ObTableImportInfoList& table_list);
        
        void run(tbsys::CThread* thread, void* arg);

        inline bool is_loader_stoped() const
        {
          return is_finish_load_;
        }

        inline bool is_pending_upgrade() const
        {
          return is_pending_upgrade_;
        }

      private:
        typedef int (*Filter)(const struct dirent* d);
        typedef int (ObBypassSSTableLoader::*Operate)(int32_t disk_no, const char* filename);

        static int sstable_file_name_filter(const struct dirent* d);
        int scan_sstable_files(const int32_t disk_no, Filter filter, Operate op);
        int do_load_sstable(const int32_t disk_no, const char* file_name);
        bool need_import(const char* file_name) const;
        int load_one_bypass_sstable(const int32_t disk_no, const char* file_name);
        int create_hard_link_sstable(const char* bypass_sstable_path, 
          char* link_sstable_path, const int64_t path_size, 
          const int32_t disk_no, sstable::ObSSTableId& sstable_id);
        int add_new_tablet(const sstable::ObSSTableId& sstable_id, 
          const int32_t disk_no, ObTablet*& tablet);
        int add_bypass_tablets_into_image();
        int load_bypass_sstables(const int64_t thread_index);

        int recycle_bypass_dir();
        int do_recycle_bypass_sstable(int32_t disk_no, const char* file_name);
        int rollback();
        int recycle_unload_tablets(bool only_recycle_removed_tablet = false);
        int recycle_tablet(ObTablet* tablet, bool only_recycle_removed_tablet = false);
        void reset();

        int finish_load();

      private:
        const static int64_t MAX_LOADER_THREAD = 16;
        static const int64_t DEFAULT_BYPASS_TABLET_NUM = 128 * 1024L;

      private:
        DISALLOW_COPY_AND_ASSIGN(ObBypassSSTableLoader);

        bool inited_;
        bool is_finish_load_;
        volatile uint32_t finish_load_disk_cnt_;
        bool is_load_succ_;
        bool is_continue_load_;
        bool is_pending_upgrade_;

        int32_t disk_count_;
        const int32_t* disk_no_array_;

        const common::ObTableImportInfoList* table_list_;
        ObTabletManager* tablet_manager_;
        common::ObVector<ObTablet*> tablet_array_;
        mutable tbsys::CThreadMutex tablet_array_mutex_;

        tbsys::CThreadCond cond_;
    };
  } /* chunkserver */
} /* oceanbase */

#endif // OCEANBASE_CHUNKSERVER_BYPASS_SSTABLE_LOADER_H_
