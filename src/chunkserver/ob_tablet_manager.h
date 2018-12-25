/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 *
 * Version: 0.1: ob_tablet_manager.h,v 0.1 2010/08/19 10:40:34 chuanhui Exp $
 *
 * Authors:
 *   chuanhui <rizhao.ych@taobao.com>
 *     - some work details if you want
 *
 */
#ifndef __OCEANBASE_CHUNKSERVER_OB_TABLET_MANAGER_H__
#define __OCEANBASE_CHUNKSERVER_OB_TABLET_MANAGER_H__

#include "common/thread_buffer.h"
#include "common/ob_file_client.h"
#include "sstable/ob_blockcache.h"
#include "sstable/ob_block_index_cache.h"
#include "sstable/ob_sstable_row_cache.h"
#include "sstable/ob_sstable_scanner.h"
#include "sstable/ob_sstable_getter.h"
#include "sstable/ob_sstable_reader.h"
#include "compactsstablev2/ob_sstable_block_cache.h"
#include "compactsstablev2/ob_sstable_block_index_cache.h"
#include "sql/ob_sstable_scan.h"
#include "ob_join_cache.h"
#include "ob_fileinfo_cache.h"
#include "ob_disk_manager.h"
#include "ob_tablet_image.h"
#include "ob_chunk_server_config.h"
#include "ob_chunk_server_stat.h"
#include "ob_chunk_merge.h"
#include "ob_switch_cache_utility.h"
#include "ob_compactsstable_cache.h"
#include "ob_multi_tablet_merger.h"
#include "ob_bypass_sstable_loader.h"
#include "ob_file_recycle.h"
//add wenghaixing [secondary index static_index_build]20150302
#include "ob_chunk_index_worker.h"
//add e
//add zhuyanchao[secondary index static_index_build.report]20150323
#include "common/ob_tablet_histogram_report_info.h"
#include "common/ob_tablet_histogram.h"
//add e
//add liuxiao [secondary index col checksum] 20150331
#include "common/ob_mutator.h"
//add e
namespace oceanbase
{
  namespace common
  {
    class ObSchemaManager;
    class ObSchemaManagerV2;
    class ObGetParam;
    class ObScanParam;
    class ObScanner;
    class ObNewRange;
    class ObTabletReportInfo;
    class ObTabletReportInfoList;
    class ObScanner;
    class ObServer;
  }
  namespace chunkserver
  {
    class ObChunkServerParam;

    class ObTabletManager
    {
      public:
        static const int32_t MAX_COMMAND_LENGTH = 1024*2;
        //add liuxiao [secondary index col checksum] 20150408
        //cs¡ä¨®rs??¦Ì??¨¦¨¢DD¡ê?¨¦o¨ª¦Ì?3?¨º¡À???¨®¨º¡À??
        static const int32_t MAX_GET_OLD_COLUMN_CHECKSUM_TIMEOUT = 5000000;
        //cs¡¤¡é?¨ªD?¦Ì?¨¢DD¡ê?¨¦o¨ª¦Ì?3?¨º¡À¦Ì¨¨¡äy¨º¡À??
        static const int32_t MAX_SEND_COLUMN_CHECKSUM_TIMEOUT = 3000000;
        //add e
      private:
        DISALLOW_COPY_AND_ASSIGN(ObTabletManager);

      public:
        ObTabletManager();
        ~ObTabletManager();
        //mod zhaoqiong [fixed for Backup]:20150811:b
        //int init(const ObChunkServerConfig* config);
        int init(const ObChunkServerConfig* config, const ObClientManager * client_mgr);
        //mod:e
        int init(const int64_t block_cache_size,
            const int64_t block_index_cache_size,
            const int64_t sstable_row_cache_size,
            const int64_t file_info_cache_num,
            const char* data_dir,
            const int64_t max_sstable_size);
        int start_merge_thread();
        int start_cache_thread();
        int start_bypass_loader_thread();
        int load_tablets(const int32_t* disk_no_array, const int32_t size);
        void destroy();

      public:
        /**
         * must call end_get() to release the resources.
         *
         */
        int get(const common::ObGetParam& get_param, common::ObScanner& scanner);
        int end_get();

        /**
         * this function just start scan, it initialize the internal
         * value for scan. when complete scan, must call end_scan()
         * function to release the resources.
         */
        int scan(const common::ObScanParam& scan_param, common::ObScanner& scanner);
        int end_scan(bool release_tablet = true);

      public:
        // get timestamp of current serving tablets
        int64_t get_serving_data_version(void) const;

        int prepare_merge_tablets(const int64_t memtable_frozen_version);
        int prepare_tablet_image(const int64_t memtable_frozen_version);

        int merge_tablets(const int64_t memtable_frozen_version);
        ObChunkMerge &get_chunk_merge() ;
        //add wenghaixing [secondary index static_index_build] 20150302
        ObIndexWorker &get_index_worker();
        int start_index_work();
        int build_index();
        //add e
        ObCompactSSTableMemThread& get_cache_thread();
        ObBypassSSTableLoader& get_bypass_sstable_loader();
        int uninstall_disk(int32_t disk_no);
        int install_disk(const char* mount_path, int32_t &disk_no);

        int report_tablets();
        //add zhuyanchao[secondary index static index build.report]20150413
        int report_index_tablets(uint64_t index_tid);
        //add e
        int delete_tablet_on_rootserver(const common::ObVector<ObTablet*>& delete_tablets);

        int report_capacity_info();

        int create_tablet(const common::ObNewRange& range, const int64_t data_version);

        int migrate_tablet(const common::ObNewRange& range,
            const common::ObServer& dest_server,
            char (*src_path)[common::OB_MAX_FILE_NAME_LENGTH],
            char (*dest_path)[common::OB_MAX_FILE_NAME_LENGTH],
            int64_t & num_file,
            int64_t & tablet_version,
            int64_t& tablet_seq_num,
            int32_t & dest_disk_no,
            uint64_t & crc_sum);

        int dest_load_tablet(const common::ObNewRange& range,
            char (*dest_path)[common::OB_MAX_FILE_NAME_LENGTH],
            const int64_t num_file,
            const int64_t tablet_version,
            const int64_t tablet_seq_num,
            const int32_t dest_disk_no,
            const uint64_t crc_sum);

        int check_fetch_tablet_version(const common::ObNewRange& range,
            const int64_t tablet_version, int64_t &old_tablet_version);

        void start_gc(const int64_t recycle_version);

        int merge_multi_tablets(common::ObTabletReportInfoList& tablet_list);

        int sync_all_tablet_images();

        int load_bypass_sstables(const common::ObTableImportInfoList& table_list);
        int load_bypass_sstables_over(const common::ObTableImportInfoList& table_list,
          const bool is_load_succ);

        int delete_table(const uint64_t table_id);

      public:
        inline const ObClientManager * get_client_manager(); //add zhaoqiong [fixed for Backup]:20150811
        inline FileInfoCache& get_fileinfo_cache();
        inline sstable::ObBlockCache& get_serving_block_cache();
        inline sstable::ObBlockCache& get_unserving_block_cache();
        inline sstable::ObBlockIndexCache& get_serving_block_index_cache();
        inline sstable::ObBlockIndexCache& get_unserving_block_index_cache();
        inline compactsstablev2::ObSSTableBlockIndexCache & get_compact_block_index_cache();
        inline compactsstablev2::ObSSTableBlockCache & get_compact_block_cache();
        inline ObMultiVersionTabletImage& get_serving_tablet_image();
        inline ObDiskManager& get_disk_manager();
        inline ObRegularRecycler& get_regular_recycler();
        inline ObScanRecycler& get_scan_recycler();
        inline ObJoinCache& get_join_cache();
        inline void build_scan_context(sql::ScanContext& scan_context);
        //add zhuyanchao[secondary index statix_data_build]
        inline ObTabletImage* get_index_image();
        //add e
        //add liuxiao [seconadry index static index] 20150626
        inline bool if_is_building_index();
        //add e

        const ObMultiVersionTabletImage& get_serving_tablet_image() const;
        inline sstable::ObSSTableRowCache* get_row_cache() const { return sstable_row_cache_; }

        /**
         * only after the new tablet image is loaded, and the tablet
         * manager is also using the old tablet image, this function can
         * be called. this function will duplicate serving cache to
         * unserving cache.
         *
         * @return int if success,return OB_SUCCESS, else return
         *         OB_ERROR
         */
        int build_unserving_cache();
        int build_unserving_cache(const int64_t block_cache_size,
                                  const int64_t block_index_cache_size);

        /**
         * after switch to the new tablet image, call this function to
         * drop the unserving cache.
         *
         * @return int if success,return OB_SUCCESS, else return
         *         OB_ERROR
         */
        int drop_unserving_cache();

      public:
        int dump();

      public:
        inline bool is_stoped() { return !is_init_; }
        inline bool is_disk_maintain() {return is_disk_maintain_;}
        //add wenghaixing [secondary index.cluster]20150702
        void set_beat_tid(const uint64_t tid);
        //add e

      public:
        struct ObGetThreadContext
        {
          ObTablet* tablets_[common::OB_MAX_GET_ROW_NUMBER];
          int64_t tablets_count_;
          int64_t min_compactsstable_version_;
          sstable::ObSSTableReader* readers_[common::OB_MAX_GET_ROW_NUMBER];
          int64_t readers_count_;
        };
    
        int gen_sstable_getter(const ObGetParam& get_param, sstable::ObSSTableGetter *sstable_getter, int64_t &tablet_version);

      private:
        int internal_scan(const common::ObScanParam& scan_param, common::ObScanner& scanner);
        int init_sstable_scanner(const common::ObScanParam& scan_param,
            const ObTablet* tablet, sstable::ObSSTableScanner& sstable_scanner);

        int internal_get(const common::ObGetParam& get_param, common::ObScanner& scanner);
        int acquire_tablet(const common::ObGetParam& get_param, ObMultiVersionTabletImage& image,
                           ObTablet* tablets[], int64_t& size, int64_t& tablet_version,
                           int64_t* compactsstable_version = NULL);
        int release_tablet(ObMultiVersionTabletImage& image, ObTablet* tablets[], int64_t size);
        int init_sstable_getter(const common::ObGetParam& get_param, ObTablet* tablets[],
                                const int64_t size, sstable::ObSSTableGetter& sstable_getter);
        int fill_get_data(common::ObIterator& iterator, common::ObScanner& scanner);

      public:
        ObTablet*& get_cur_thread_scan_tablet();
        ObGetThreadContext*& get_cur_thread_get_contex();

      public:

        int fill_tablet_info(const ObTablet& tablet, common::ObTabletReportInfo& tablet_info);
        int send_tablet_report(const common::ObTabletReportInfoList& tablets, bool has_more);
        //add zhuyanchao[secondary_index_static_data_build]20150320
        int fill_tablet_histogram_info(const ObTablet& tablet,
                                              common::ObTabletHistogramReportInfo& report_tablet_histogram_info);
        int send_tablet_histogram_report(const common::ObTabletHistogramReportInfoList& tablets, bool has_more);
        //add e
		//add wenghaixing [secondary index static_index_build.exceptional_handle] 20150409
        int whipping_wok(const BlackList &list, const ObServer chunk_server);
        //add e
        //add liuxiao [secondary index col checksum] 20150331
        int get_old_tablet_column_checksum(const ObNewRange new_range,col_checksum& column_checksum);
        int send_tablet_column_checksum(const col_checksum column_checksum,const ObNewRange new_range,const int64_t version);
        //add e

      public:
        // allocate new sstable file sequence, call after load_tablets();
        int64_t allocate_sstable_file_seq();
        // switch to new tablets, call by merge_tablets() after all new tablets loaded.
        int switch_cache();

      private:
        static const uint64_t TABLET_ARRAY_NUM = 2; // one for serving, the other for merging
        static const int64_t DEF_MAX_TABLETS_NUM  = 4000; // max tablets num
        static const int64_t MAX_RANGE_DUMP_SIZE = 256; // for log

      private:
        enum TabletMgrStatus
        {
          NORMAL = 0, // normal status
          MERGING,    // during daily merging process
          MERGED,     // merging complete, waiting to be switched
        };



      private:
        bool is_init_;
        volatile bool is_disk_maintain_;
        volatile uint64_t cur_serving_idx_;
        volatile uint64_t mgr_status_;
        volatile uint64_t max_sstable_file_seq_;

        FileInfoCache fileinfo_cache_;
        sstable::ObBlockCache block_cache_[TABLET_ARRAY_NUM];
        sstable::ObBlockIndexCache block_index_cache_[TABLET_ARRAY_NUM];
        compactsstablev2::ObSSTableBlockIndexCache compact_block_index_cache_;
        compactsstablev2::ObSSTableBlockCache compact_block_cache_;
        ObJoinCache join_cache_; //used for join phase of daily merge
        sstable::ObSSTableRowCache* sstable_row_cache_;

        ObDiskManager disk_manager_;
        ObRegularRecycler regular_recycler_;
        ObScanRecycler scan_recycler_;
        ObMultiVersionTabletImage tablet_image_;
        ObSwitchCacheUtility switch_cache_utility_;

        ObChunkMerge chunk_merge_;
        //add wenghaixing [secondary index static_index_build]20150302
        ObIndexWorker index_worker_;
        uint64_t index_beat_tid_;
        //add e
        ObCompactSSTableMemThread cache_thread_;
        const ObChunkServerConfig* config_;
        const ObClientManager * client_mgr_; //add zhaoqiong [fixed for Backup]:20150811
        ObBypassSSTableLoader bypass_sstable_loader_;
    };

    //add zhaoqiong [fixed for Backup]:20150811:b
    inline const ObClientManager*  ObTabletManager::get_client_manager()
    {
       return client_mgr_;
    }
    //add:e

    inline FileInfoCache&  ObTabletManager::get_fileinfo_cache()
    {
       return fileinfo_cache_;
    }

    //add liuxiao [secondary index static] 20150626
    //判断是否有在建索引
    inline bool ObTabletManager::if_is_building_index()
    {
      //TBSYS_LOG(ERROR,"test::whx if_is_building_index[%ld]",index_beat_tid_);
      return OB_INVALID_ID != index_beat_tid_;
    }
    //add e

    inline sstable::ObBlockCache& ObTabletManager::get_serving_block_cache()
    {
       return block_cache_[cur_serving_idx_];
    }

    inline sstable::ObBlockCache& ObTabletManager::get_unserving_block_cache()
    {
      return block_cache_[(cur_serving_idx_ + 1) % TABLET_ARRAY_NUM];
    }

    inline sstable::ObBlockIndexCache& ObTabletManager::get_serving_block_index_cache()
    {
      return block_index_cache_[cur_serving_idx_];
    }

    inline sstable::ObBlockIndexCache& ObTabletManager::get_unserving_block_index_cache()
    {
      return block_index_cache_[(cur_serving_idx_ + 1) % TABLET_ARRAY_NUM];
    }

    inline ObMultiVersionTabletImage& ObTabletManager::get_serving_tablet_image()
    {
      return tablet_image_;
    }

    inline const ObMultiVersionTabletImage& ObTabletManager::get_serving_tablet_image() const
    {
      return tablet_image_;
    }

    inline ObDiskManager& ObTabletManager::get_disk_manager()
    {
      return disk_manager_;
    }

    inline ObRegularRecycler& ObTabletManager::get_regular_recycler()
    {
      return regular_recycler_;
    }

    inline ObScanRecycler& ObTabletManager::get_scan_recycler()
    {
      return scan_recycler_;
    }

    inline ObJoinCache& ObTabletManager::get_join_cache()
    {
      return join_cache_;
    }

    inline compactsstablev2::ObSSTableBlockIndexCache & ObTabletManager::get_compact_block_index_cache()
    {
      return compact_block_index_cache_;
    }

    inline compactsstablev2::ObSSTableBlockCache & ObTabletManager::get_compact_block_cache()
    {
      return compact_block_cache_;
    }

    inline void ObTabletManager::build_scan_context(sql::ScanContext& scan_context)
    {
      scan_context.tablet_image_ = &tablet_image_;
      scan_context.block_index_cache_ = &block_index_cache_[cur_serving_idx_];
      scan_context.block_cache_ = &block_cache_[cur_serving_idx_];
      scan_context.compact_context_.block_index_cache_ = &compact_block_index_cache_;
      scan_context.compact_context_.block_cache_ = &compact_block_cache_;
    }

  }
}


#endif //__OB_TABLET_MANAGER_H__
