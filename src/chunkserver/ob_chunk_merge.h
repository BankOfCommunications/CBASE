/*
 * (C) 2007-2010 TaoBao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * ob_chunk_merge.h is for what ...
 *
 * Version: $id$
 *
 * Authors:
 *   MaoQi maoqi@taobao.com
 *
 */
#ifndef OB_CHUNKSERVER_OB_CHUNK_MERGE_H_
#define OB_CHUNKSERVER_OB_CHUNK_MERGE_H_
#include <tbsys.h>
#include <Mutex.h>
#include <Monitor.h>
#include "common/ob_define.h"
#include "common/ob_schema.h"
#include "common/ob_vector.h"
#include "common/thread_buffer.h"


namespace oceanbase
{
  namespace chunkserver
  {
    class ObTabletManager;
    class ObTabletMerger;
    class ObChunkMerge : public tbsys::CDefaultRunnable
    {
      public:
        ObChunkMerge();
        ~ObChunkMerge() {}

        int init(ObTabletManager *manager);
        void destroy();

        /**
         * the new frozen_version come,
         * we will wake up all merge thread to start merge
         *
         */
        int schedule(const int64_t frozen_version);

        inline const bool is_pending_in_upgrade() const
        {
          return pending_in_upgrade_;
        }
        //add liumz, [secondary index version management]20160413:b
        inline bool is_merge_done() const
        {
          return (0 == active_thread_num_ && round_end_);
        }
        //add:e

        inline bool is_merge_stoped() const
        {
          return 0 == active_thread_num_;
        }

        inline bool is_merge_reported() const
        {
          return (active_thread_num_ <= 1 && round_end_);
        }

        inline void set_newest_frozen_version(const int64_t frozen_version)
        {
          if (frozen_version > newest_frozen_version_)
            newest_frozen_version_ = frozen_version;
        }

        inline SpinRWLock & get_migrate_lock()
        {
          return migrate_lock_;
        }

        void set_config_param();

        bool can_launch_next_round(const int64_t frozen_version);

        int create_merge_threads(const int64_t max_merge_thread);


      private:
        virtual void run(tbsys::CThread* thread, void *arg);
        void merge_tablets(const int64_t thread_no);
        int get_tablets(ObTablet* &tablet);

        bool have_new_version_in_othercs(const ObTablet* tablet);

        int start_round(const int64_t frozen_version);
        int finish_round(const int64_t frozen_version);
        int fetch_frozen_time_busy_wait(const int64_t frozen_version, int64_t &frozen_time);
        int fetch_frozen_schema_busy_wait(
          const int64_t frozen_version, common::ObSchemaManagerV2& schema);
        int create_all_tablet_mergers();
        int destroy_all_tablets_mergers();
        template <typename Merger>
          int create_tablet_mergers(ObTabletMerger** mergers, const int64_t size);
        int destroy_tablet_mergers(ObTabletMerger** mergers, const int64_t size);
        int get_tablet_merger(const int64_t thread_no, ObTabletMerger* &merger);

      public:
        bool check_load();
      private:
        friend class ObTabletMergerV1;
        friend class ObTabletMergerV2;
        const static int64_t MAX_MERGE_THREAD = 32;
        const static int32_t TABLET_COUNT_PER_MERGE = 10240;
        const static uint32_t MAX_MERGE_PER_DISK = 2;
      private:
        volatile bool inited_;
        pthread_cond_t cond_;
        pthread_mutex_t mutex_;

        ObTablet *tablet_array_[TABLET_COUNT_PER_MERGE];
        int64_t tablets_num_;
        int64_t tablet_index_;
        int64_t thread_num_;

        volatile int64_t tablets_have_got_;
        volatile int64_t active_thread_num_;

        volatile int64_t frozen_version_; //frozen_version_ in merge
        volatile int64_t newest_frozen_version_; //the last frozen version in updateserver
        int64_t frozen_timestamp_; //current frozen_timestamp_ in merge;
        int64_t write_sstable_version_; // current round write sstable version;

        volatile int64_t merge_start_time_;    //this version start time
        volatile int64_t merge_last_end_time_;    //this version merged complete time

        volatile bool round_start_;
        volatile bool round_end_;
        volatile bool pending_in_upgrade_;


        volatile uint32_t pending_merge_[common::OB_MAX_DISK_NUMBER]; //use atomic op

        int64_t merge_load_high_;
        int64_t request_count_high_;
        int64_t merge_adjust_ratio_;
        int64_t merge_load_adjust_;
        int64_t merge_pause_row_count_;
        int64_t merge_pause_sleep_time_;
        int64_t merge_highload_sleep_time_;
        int64_t min_merge_thread_num_;
        SpinRWLock migrate_lock_;

        common::ObSchemaManagerV2 last_schema_;
        common::ObSchemaManagerV2 current_schema_;

        ObTabletManager *tablet_manager_;
        ObTabletMerger  *mergers_[MAX_MERGE_THREAD * 2];
    };

    template <typename Merger>
      int ObChunkMerge::create_tablet_mergers(ObTabletMerger** mergers, const int64_t size)
      {
        int ret = OB_SUCCESS;
        char* ptr = NULL;

        if (NULL == mergers || 0 >= size)
        {
          ret = OB_INVALID_ARGUMENT;
        }
        else if (NULL == (ptr = reinterpret_cast<char*>(ob_malloc(sizeof(Merger) * size, ObModIds::OB_CS_MERGER))))
        {
          TBSYS_LOG(WARN, "allocate memrory for merger object error.");
          ret = OB_ALLOCATE_MEMORY_FAILED;
        }
        else
        {
          for (int64_t i = 0 ; i < size; ++i)
          {
            Merger* merger = new (ptr + i * sizeof(Merger)) Merger(*this, *tablet_manager_);
            if (NULL == merger || OB_SUCCESS != (ret = merger->init()))
            {
              TBSYS_LOG(WARN, "init merger object error, merger=%p, ret=%d", merger, ret);
              ret = OB_ALLOCATE_MEMORY_FAILED;
              break;
            }
            else
            {
              mergers[i] = merger;
            }
          }
        }
        return ret;
      }


  } /* chunkserver */
} /* oceanbase */
#endif
