/*
 *  (C) 2007-2010 Taobao Inc.
 *  
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License version 2 as
 *  published by the Free Software Foundation.
 *
 *         ????.cpp is for what ...
 *
 *  Version: $Id: ipvsadm.c,v 1.27 2005/12/10 16:00:07 wensong Exp $
 *
 *  Authors:
 *     qushan <qushan@taobao.com>
 *        - some work details if you want
 */
#ifndef OCEANBASE_CHUNKSERVER_TABLET_IMAGE_H_
#define OCEANBASE_CHUNKSERVER_TABLET_IMAGE_H_

#include "atomic.h"
#include "common/page_arena.h"
#include "common/ob_range.h"
#include "common/ob_vector.h"
#include "common/ob_spin_rwlock.h"
#include "common/ob_file.h"
#include "common/ob_atomic.h"
#include "sstable/ob_disk_path.h"
#include "sstable/ob_sstable_reader.h"
#include "compactsstablev2/ob_compact_sstable_reader.h"
#include "ob_tablet.h"

namespace oceanbase 
{ 
  namespace chunkserver 
  {
    struct ObTabletMetaHeader
    {
      static const int16_t TABLET_META_MAGIC = 0x6D69; // "mi"
      int64_t tablet_count_;     
      int64_t data_version_;    
      int32_t row_key_stream_offset_;
      int32_t row_key_stream_size_;
      int32_t tablet_extend_info_offset_;
      int32_t tablet_extend_info_size_;

      NEED_SERIALIZE_AND_DESERIALIZE;
    };


    class ObTabletBuilder
    {
      public:
        ObTabletBuilder();
        virtual ~ObTabletBuilder();
        inline int64_t get_data_size() const { return data_size_; }
        inline int64_t get_buf_size() const { return buf_size_; }
        inline const char* get_buf() const { return buf_; }
        inline void reset() { data_size_ = 0; }
      protected:
        static const int64_t DEFAULT_BUILDER_BUF_SIZE = 2 * 1024 * 1024; //2M
        int ensure_space(const int64_t size);
        char* buf_;
        int64_t buf_size_;
        int64_t data_size_;
    };

    class ObTabletRowkeyBuilder : public ObTabletBuilder
    {
      public:
        ObTabletRowkeyBuilder();
        ~ObTabletRowkeyBuilder();
        int append_range(const common::ObNewRange& range);
    };

    class ObTabletExtendBuilder : public ObTabletBuilder
    {
      public:
        ObTabletExtendBuilder();
        ~ObTabletExtendBuilder();
        int append_tablet(const ObTabletExtendInfo& info);
    };

    struct ObThreadMetaWriter
    {
      void reset()
      {
        builder_.reset();
        extend_.reset();
        meta_data_size_ = 0;
      }

      common::ObFileAppender appender_;
      ObTabletRowkeyBuilder builder_;
      ObTabletExtendBuilder extend_;
      common::ObMemBuf meta_buf_;
      int64_t meta_data_size_;
    };

    bool compare_tablet(const ObTablet* lhs, const ObTablet* rhs);
    bool unique_tablet(const ObTablet* lhs, const ObTablet* rhs);
    class ObDiskManager;

    class ObMultiVersionTabletImage;
    class ObTabletImage
    {
      friend class ObMultiVersionTabletImage;
      static const int64_t INVALID_ITER_INDEX = -1;
      public:
        ObTabletImage();
        ~ObTabletImage();

        friend class ObTablet;

      public:
        int destroy();
        int dump(const bool dump_sstable = false) const;

        int acquire_tablet(const common::ObNewRange& range, const int32_t scan_direction, ObTablet* &tablet) const;
        int release_tablet(ObTablet* tablet, bool* is_remove_sstable = NULL) const;
        int acquire_tablet(const sstable::ObSSTableId& sstable_id, ObTablet* &tablet) const;
        int include_sstable(const sstable::ObSSTableId& sstable_id) const;

        int remove_sstable(ObTablet* tablet) const;

        int remove_tablet(const common::ObNewRange& range, int32_t &disk_no);
        int delete_table(const uint64_t table_id, tbsys::CRWLock& lock);

        //add wenghaixing [secondary index static_index_build] 20150303
        const int acquire_tablets_pub(const uint64_t table_id, common::ObVector<ObTablet*>& table_tablets) const ;
        //add e
        //add liuxiao [secondary index static_index_build] 20150330
        //用于删除磁盘上的局部索引文件
        const int delete_local_index_sstable() const;
        //add e
        /*
         * scan traverses over all tablets,
         * begin_scan_tablets, get_next_tablet will add
         * reference count of tablet, so need call release_tablet
         * when %tablet not used.
         */
        int begin_scan_tablets();
        int get_next_tablet(ObTablet* &tablet);
        int end_scan_tablets();

        inline int64_t get_ref_count() const { return ref_count_; }
        inline int64_t get_max_sstable_file_seq() const { return max_sstable_file_seq_; }
        inline int64_t get_data_version() const { return data_version_; }
        inline void set_data_version(int64_t version) { data_version_ = version; }

        int add_tablet(ObTablet* tablet);
        int alloc_tablet_object(const common::ObNewRange& range, ObTablet* &tablet);
        ObTablet* alloc_tablet_object();
        void set_fileinfo_cache(common::IFileInfoMgr& fileinfo_cache);
        void set_disk_manger(ObDiskManager* disk_manager);

        inline int64_t incr_merged_tablet_count() { return __sync_add_and_fetch(&merged_tablet_count_, 1); }
        inline int64_t decr_merged_tablet_count() { return __sync_sub_and_fetch(&merged_tablet_count_, 1); }
        inline int64_t get_merged_tablet_count() const { return merged_tablet_count_; }

      public:
        /**
         * all read methods are not support mutithread
         * read method used only for load tablets when system initialize.
         */
        int read(const int32_t* disk_no_array, const int32_t size, const bool load_sstable = false);
        int write(const int32_t* disk_no_array, const int32_t size);
        int read(const char* idx_path, const int32_t disk_no, const bool load_sstable = false);
        int write(const char* idx_path, const int32_t disk_no);
        /**
         * read/write from/to index file at specific disk.
         */
        int write(const int32_t disk_no);
        int read(const int32_t disk_no, const bool load_sstable = false);

        /**
         * seperate the write() function to two functions, 
         * prepare_write_meta() prepare the meta in memory, and it can 
         * run in critical section, write_meta() write the meta to 
         * disk by direct io
         */
        int prepare_write_meta(const int32_t disk_no);
        int write_meta(const char* idx_path, const int32_t disk_no);

      public:
        int serialize(const int32_t disk_no, char* buf, const int64_t buf_len, int64_t &pos);
        int deserialize(const int32_t disk_no, const char* buf, const int64_t buf_len, int64_t &pos);
        int64_t get_max_serialize(const int32_t disk_no) const;
        int32_t get_tablets_num() const;
      private:
        int serialize(const int32_t disk_no, 
            char* buf, const int64_t buf_len, int64_t& pos, 
            ObTabletRowkeyBuilder& builder, ObTabletExtendBuilder& extend);
        int deserialize(const bool load_sstable, const int32_t disk_no, 
            const char* buf, const int64_t data_len, int64_t& pos);

      private:
        int find_tablet(const sstable::ObSSTableId& sstable_id, ObTablet* &tablet) const;
        int find_tablet(const common::ObNewRange& range, const int32_t scan_direction, ObTablet* &tablet) const;
        int find_tablet( const uint64_t table_id, const common::ObRowkey& key, const ObRowkeyInfo* ri, 
            const ObBorderFlag& border_flag, const int32_t scan_direction, ObTablet* &tablet) const;

        int acquire_tablets_on_disk(const int32_t disk_no, ObVector<ObTablet*>& table_tablets);
        int acquire_tablets(const uint64_t table_id, common::ObVector<ObTablet*>& table_tablets);
        int release_tablets(const common::ObVector<ObTablet*>& table_tablets);
        int remove_table_tablets(const uint64_t table_id);
      private:
        sstable::ObSSTableReader* alloc_sstable_object();
        compactsstablev2::ObCompactSSTableReader* alloc_compact_sstable_object();
        int reset();
        inline int64_t acquire() const { return __sync_add_and_fetch((volatile int64_t*)&ref_count_, 1); }
        inline int64_t release() const { return __sync_sub_and_fetch((volatile int64_t*)&ref_count_, 1); }

      private:
        static const int64_t DEFAULT_TABLET_NUM = 128 * 1024L;
        static const int64_t DEFAULT_TABLE_TABLET_NUM = 8 * 1024L;
        typedef common::hash::ObHashMap<sstable::ObSSTableId, ObTablet*, 
          common::hash::NoPthreadDefendMode> HashMap;

      private:
        common::ObSortedVector<ObTablet*> tablet_list_;
        common::ObVector<sstable::ObSSTableReader*> sstable_list_;
        common::ObVector<ObTablet*> delete_table_tablet_list_;
        common::ObVector<ObTablet*> report_tablet_list_;
        HashMap sstable_to_tablet_map_;
        bool hash_map_inited_;
        // all tablets has same data_version_ for now.
        // will be discard on next version.
        int64_t data_version_; 
        int64_t max_sstable_file_seq_;
        volatile int64_t ref_count_ CACHE_ALIGNED;
        volatile int64_t cur_iter_idx_ CACHE_ALIGNED;
        volatile int64_t merged_tablet_count_ CACHE_ALIGNED;

        common::ModulePageAllocator mod_;
        common::ModuleArena allocator_;


        common::IFileInfoMgr* fileinfo_cache_;
        ObDiskManager* disk_manager_;
        mutable tbsys::CThreadMutex alloc_mutex_;
    };

    class ObMultiVersionTabletImage 
    {
      public:
        static const int32_t MAX_RESERVE_VERSION_COUNT = 2;
        enum ScanDirection
        {
          SCAN_FORWARD = 0,
          SCAN_BACKWARD = 1,
        };

        enum ScanPosition
        {
          FROM_SERVICE_INDEX = 0,
          FROM_NEWEST_INDEX = 1,
        };

      public:
        explicit ObMultiVersionTabletImage(common::IFileInfoMgr& fileinfo_cache);
        ObMultiVersionTabletImage(common::IFileInfoMgr& fileinfo_cache,
          ObDiskManager& disk_manager);
        ~ObMultiVersionTabletImage();
      public:
        /**
         *search tablet includes the %range in tablet image
         *and add the tablet's reference count.
         *@param range search range
         *@param scan_direction normally SCAN_FORWARD
         *@param version if > 0, then search the tablet which 
         *version less or equal this version, otherwise search
         *the tablet which version is newest.
         *@param [out] tablet search result.
         */
        int acquire_tablet(const common::ObNewRange &range, 
            const ScanDirection scan_direction, 
            const int64_t version, 
            ObTablet* &tablet) const;
        int acquire_tablet_all_version(
            const common::ObNewRange &range, 
            const ScanDirection scan_direction, 
            const ScanPosition from_index,
            const int64_t version, 
            ObTablet* &tablet) const;

        /**
         * release tablet, subtract ref count.
         */
        int release_tablet(ObTablet* tablet) const;

        /**
         * release tablets, if one is not OB_SUCCESS, will continue release other tablet.
         */
        int release_tablets(const ObVector<ObTablet*>& tablets) const;

        /**
         * get sstable reader object from tablet image.
         * this function could run slower than acquire_tablet
         * cause need traverse every tablet to find which one
         * include sstable.
         */
        int acquire_tablet(
            const sstable::ObSSTableId& sstable_id, 
            const int64_t version, 
            ObTablet* &tablet) const;
        int acquire_tablet_all_version(
            const sstable::ObSSTableId& sstable_id, 
            const ScanPosition from_index,
            const int64_t version, 
            ObTablet* &tablet) const;


        /**
         * acquire old tablet by split new tablet
         */
        int get_split_old_tablet(const ObTablet* const new_split_tablet, ObTablet* &old_split_tablet);

        /**
         * acquire all tablets locate on disk_no
         */
        int acquire_tablets_on_disk(
            const int32_t disk_no,
            ObVector<ObTablet*>& tablet_list) const;

        /**
         * check sstable if alive in current images.
         * which could be recycled when not exist.
         */
        int include_sstable(const sstable::ObSSTableId& sstable_id) const;

        /**
         * add new tablet to image
         * for create tablet.
         */
        int add_tablet(ObTablet *tablet, 
            const bool load_sstable = false, 
            const bool for_create = false);

        /**
         *version of tablet upgrade, when tablet compacted with update data in ups.
         *replace oldest tablet entry in image with new_tablet.
         *@param new_tablet new version tablet entry.
         */
        int upgrade_tablet(ObTablet *old_tablet, ObTablet *new_tablet, 
            const bool load_sstable = false);

        /**
         *same as above , but tablet splited in compaction. old tablet splits to 
         *several of new tablets.
         *@param old_tablet 
         *@param new_tablets  new split tablet array.
         *@param split_size size of new_tablets.
         *@param load_sstable load the sstable file of tablets.
         */
        int upgrade_tablet(ObTablet *old_tablet, 
            ObTablet *new_tablets[], const int32_t split_size, 
            const bool load_sstable = false);
        //add zhuyanchao[secondary index static_data_build.report]20150401
        int upgrade_index_tablet(ObTablet* obtablet,const bool load_sstable = false);
        //add e

        int upgrade_service();

        /**
         * remove specific version of tablet 
         * @param range of tablet
         * @param version of tablet
         * @param [out] disk no of meta index file.
         */
        int remove_tablet(const common::ObNewRange& range, 
            const int64_t version, int32_t &disk_no, bool sync = true);


        /**
         * remove all tablets, if one not OB_SUCCESS, will break 
         */
        int remove_tablets(ObVector<ObTablet*>& tablets, bool sync = true);

        /**
         * check if can launch a new merge process with version
         */
        int prepare_for_merge(const int64_t version);

        /**
         * fetch some tablets who has version less than %version for merge.
         * @param version 
         * @param [in,out] fetch size of tablets
         * @param [out] tablets for merge
         */
        int get_tablets_for_merge(const int64_t current_frozen_version, 
            int64_t &size, ObTablet *tablets[]) const;

        /**
         * check if remains some tablets which has data version = %version
         * need merge(upgrade to newer version)
         */
        bool has_tablets_for_merge(const int64_t version) const;

        /**
         * give up merge some tablets which has data version = %version
         * set them merged.
         */
        int discard_tablets_not_merged(const int64_t version);

        /**
         * allocate new tablet object, with range and version.
         * @param range of tablet, rowkey 's memory will be reallocated.
         * @param version of tablet.
         * @param [out] tablet object pointer.
         */
        int alloc_tablet_object(const common::ObNewRange& range, 
            const int64_t version, ObTablet* &tablet);
        int alloc_tablet_object(const int64_t version, ObTablet* &tablet);

        /**
         * write tablet image to disk index file
         * one file per version, disk
         */
        int write(const int64_t version, const int32_t disk_no);
        int write(const char* idx_path, const int64_t version, const int32_t disk_no);
        int sync_all_images(const int32_t* disk_no_array, const int32_t size);

        /**
         * read index file to tablet image object.
         * one file per version, disk, only for test.
         */
        int read(const int64_t version, const int32_t disk_no, 
            const bool load_sstable = false);
        int read(const char* idx_path, const int64_t version, 
            const int32_t disk_no, const bool load_sstable = false);
        int load_tablets(const int32_t* disk_no_array, const int32_t size, 
            const bool load_sstable = false);

        int delete_table(const uint64_t table_id);

        /*
         * scan traverses over all tablets,
         * begin_scan_tablets, get_next_tablet will add
         * reference count of tablet, so need call release_tablet
         * when %tablet not used.
         */
        int begin_scan_tablets();
        int get_next_tablet(ObTablet* &tablet);
        int end_scan_tablets();

        int64_t get_max_sstable_file_seq() const;

        int64_t get_eldest_version() const;
        int64_t get_newest_version() const;
        int64_t get_serving_version() const;

        void dump(const bool dump_sstable = true) const;
        inline void reset()
        {
          for (int64_t i = 0; i < MAX_RESERVE_VERSION_COUNT; ++i)
          {
            if (NULL != image_tracker_[i]) image_tracker_[i]->reset();
          }
        }
        inline int prepare_for_service()
        {
          return initialize_service_index();
        }

        int serialize(const int32_t index, const int32_t disk_no, char* buf, const int64_t buf_len, int64_t &pos);
        int deserialize(const int32_t disk_no, const char* buf, const int64_t buf_len, int64_t &pos);

        common::IFileInfoMgr& get_fileinfo_cache() const { return fileinfo_cache_; }
        const char* print_tablet_image_stat() const;

        inline const ObTabletImage &get_serving_image() const
        {
          return get_image(get_serving_version());
        }

      public:
        struct ObTabletImageStat
        {
          int64_t old_ver_tablets_num_;
          int64_t old_ver_merged_tablets_num_;
          int64_t new_ver_tablets_num_;
        };
        void get_image_stat(ObTabletImageStat& stat);

      private:
        int64_t get_eldest_index() const;
        int alloc_tablet_image(const int64_t version);
        
        inline int64_t get_index(const int64_t version) const
        {
          return version % MAX_RESERVE_VERSION_COUNT;
        }
        inline ObTabletImage &get_image(const int64_t version) 
        {
          return *image_tracker_[version % MAX_RESERVE_VERSION_COUNT];
        }
        inline const ObTabletImage &get_image(const int64_t version) const
        {
          return *image_tracker_[version % MAX_RESERVE_VERSION_COUNT];
        }
        inline bool has_tablet(const int64_t index) const
        {
          return NULL != image_tracker_[index] 
            && image_tracker_[index]->data_version_ > 0
            && image_tracker_[index]->tablet_list_.size() > 0;
        }
        inline bool has_version_tablet(const int64_t version) const
        {
          return has_tablet(version % MAX_RESERVE_VERSION_COUNT);
        }
        inline bool has_report_tablet(const int64_t index) const
        {
          return NULL != image_tracker_[index] 
            && image_tracker_[index]->data_version_ > 0
            && image_tracker_[index]->report_tablet_list_.size() > 0;
        }

        inline bool has_match_version_tablet(const int64_t version) const
        {
          return has_version_tablet(version) 
            && get_image(version).data_version_ == version;
        }

        //check if the version is existent, not care tablets count
        inline bool has_match_version(const int64_t version) const
        {
          int64_t index = version % MAX_RESERVE_VERSION_COUNT;
          return index >= 0 && NULL != image_tracker_[index] 
            && image_tracker_[index]->data_version_ == version;
        }

        int64_t initialize_service_index();
        int adjust_inconsistent_tablets();
        bool is_two_version_tablet_consistent(const common::ObNewRange& old_range);

        // check tablet image object for prepare to store tablets.
        int prepare_tablet_image(const int64_t version, const bool destroy_exist);
        // check tablet image object if can destroy.
        int tablets_all_merged(const int64_t index) const;
        // destroy tablet image object in image_tracker_[index].
        int destroy(const int64_t index);
        // destroy all tablet image objects.
        int destroy();


      private:
        struct Iterator
        {
          static const int64_t INVALID_INDEX = -1;
          int64_t cur_vi_;
          int64_t cur_ti_;
          int64_t start_vi_;
          int64_t end_vi_;
          const ObMultiVersionTabletImage& image_;
          Iterator(const ObMultiVersionTabletImage& image) 
            : image_(image)
          {
            reset();
          }
          inline void reset() 
          { 
            cur_vi_ = INVALID_INDEX; 
            cur_ti_ = INVALID_INDEX; 
            start_vi_ = INVALID_INDEX;
            end_vi_ = INVALID_INDEX;
          }
          inline bool is_stop() const
          {
            return start_vi_ == INVALID_INDEX;
          }
          inline bool is_end() const
          {
            return cur_vi_ == end_vi_ && (!image_.has_report_tablet(cur_vi_) 
                || cur_ti_ >= image_.image_tracker_[cur_vi_]->report_tablet_list_.size());
          }
          inline bool is_last_tablet() const
          {
            return image_.has_report_tablet(cur_vi_) 
              && cur_ti_ == image_.image_tracker_[cur_vi_]->report_tablet_list_.size() - 1;
          }
          inline ObTablet* get_tablet() const
          {
            ObTablet* tablet = NULL;
            if (image_.has_report_tablet(cur_vi_))
            {
              tablet = image_.image_tracker_[cur_vi_]->report_tablet_list_.at(static_cast<int32_t>(cur_ti_));
              image_.image_tracker_[cur_vi_]->acquire();
              tablet->inc_ref();
            }
            return tablet;
          }
          inline int end()
          {
            int ret = common::OB_SUCCESS;

            if (cur_vi_ >= 0 && cur_vi_ < ObMultiVersionTabletImage::MAX_RESERVE_VERSION_COUNT
                && image_.has_report_tablet(cur_vi_))
            {
              ret = image_.image_tracker_[cur_vi_]->release_tablets(
                  image_.image_tracker_[cur_vi_]->report_tablet_list_);
              image_.image_tracker_[cur_vi_]->report_tablet_list_.reset();
            }
            reset();
            return ret;
          }
          int start();
          int next();
        };
      private:
        int64_t newest_index_;
        int64_t service_index_;
        ObTabletImage *image_tracker_[MAX_RESERVE_VERSION_COUNT];
        Iterator iterator_;
        common::IFileInfoMgr& fileinfo_cache_;
        ObDiskManager* disk_manager_;
        mutable tbsys::CRWLock lock_;
    };

  } // end namespace chunkserver
} // end namespace oceanbase


#endif //OCEANBASE_CHUNKSERVER_TABLET_IMAGE_H_

