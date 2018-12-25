/**
 * (C) 2010-2012 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * ob_bypass_sstable_loader.cpp for bypass sstable loader.
 *
 * Authors:
 *   huating <huating.zmq@taobao.com>
 *
 */
#include "common/file_directory_utils.h"
#include "ob_chunk_server_main.h"
#include "ob_tablet_manager.h"
#include "ob_bypass_sstable_loader.h"

namespace oceanbase
{
  namespace chunkserver
  {
    using namespace tbsys;
    using namespace oceanbase::common;
    using namespace oceanbase::sstable;

    ObBypassSSTableLoader::ObBypassSSTableLoader()
    : inited_(false),
      is_finish_load_(true),
      finish_load_disk_cnt_(0),
      is_load_succ_(true),
      is_continue_load_(true),
      is_pending_upgrade_(false),
      disk_count_(0),
      disk_no_array_(NULL),
      table_list_(NULL),
      tablet_manager_(NULL),
      tablet_array_(DEFAULT_BYPASS_TABLET_NUM)
    {

    }

    ObBypassSSTableLoader::~ObBypassSSTableLoader()
    {
      destroy();
    }

    int ObBypassSSTableLoader::init(ObTabletManager* manager)
    {
      int ret = OB_SUCCESS;

      if (NULL == manager)
      {
        TBSYS_LOG(WARN, "invalid param, tablet manager is NULL");
        ret = OB_INVALID_ARGUMENT;
      }
      else if (!inited_)
      {
        tablet_manager_ = manager;
        int64_t thread_num = THE_CHUNK_SERVER.get_config().bypass_sstable_loader_thread_num;
        if (thread_num > MAX_LOADER_THREAD)
        {
          thread_num = MAX_LOADER_THREAD;
        }
        setThreadCount(static_cast<int32_t>(thread_num));
        start();
        inited_  = true;
      }

      return ret;
    }

    void ObBypassSSTableLoader::destroy()
    {
      if (inited_ && _threadCount > 0)
      {
        inited_ = false;
        //stop the thread
        stop();
        //signal
        cond_.broadcast();
        //join
        wait();

        reset();
      }
    }

    void ObBypassSSTableLoader::reset()
    {
      is_finish_load_ = true;
      finish_load_disk_cnt_ = 0;
      is_load_succ_ = true;
      is_continue_load_ = true;
      is_pending_upgrade_ = false;
      disk_count_ = 0;
      disk_no_array_ = NULL;
      table_list_ = NULL;
      tablet_array_.clear();
    }

    int ObBypassSSTableLoader::start_load(
      const ObTableImportInfoList& table_list)
    {
      int ret = OB_SUCCESS;

      if (!inited_)
      {
        TBSYS_LOG(WARN, "bypass sstable loader isn't initialized");
        ret = OB_ERROR;
      }
      else if (table_list.tablet_version_ < 1)
      {
        TBSYS_LOG(WARN, "invalid param, load_version=%ld",
          table_list.tablet_version_);
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        reset();
        is_finish_load_ = false;
        table_list_ = &table_list;
        disk_no_array_ = tablet_manager_->get_disk_manager().get_disk_no_array(disk_count_);
        if (NULL == disk_no_array_ || disk_count_ <= 0)
        {
          TBSYS_LOG(WARN, "get disk no array failed, disk_no_array_=%p, "
                          "disk_count_=%d",
            disk_no_array_, disk_count_);
          ret = OB_ERROR;
        }
        else
        {
          cond_.broadcast();
        }
      }

      return ret;
    }

    int ObBypassSSTableLoader::finish_load()
    {
      int ret = OB_SUCCESS;

      TBSYS_LOG(INFO, "finish scanning bypass sstable directory, start load "
                      "bypass tablet, import_tables_info=%s, load_succ=%d, "
                      "tablet_count=%d",
        to_cstring(*table_list_), is_load_succ_, tablet_array_.size());
      if (tablet_array_.size() > 0)
      {
        is_pending_upgrade_ = true;
        if (is_load_succ_)
        {
          if (OB_SUCCESS != (ret = add_bypass_tablets_into_image()))
          {
            TBSYS_LOG(ERROR, "failed to add bypass tablets into serving tablet image");
            is_load_succ_ = false;
          }
          else if (OB_SUCCESS != (ret = tablet_manager_->sync_all_tablet_images()))
          {
            TBSYS_LOG(WARN, "failed to sync all tablet images after load bypass sstables, "
                            "import_tables_info=%s", to_cstring(*table_list_));
          }
        }

        if (is_load_succ_)
        {
          /**
           * maybe the same sstable in different disks is loaded more than
           * once, only the first tablet will be added into tablet image, 
           * and the next tablets will be set removed flag, we will 
           * recycle teh unload tablet here 
           */
          recycle_unload_tablets(true);
          //delete all the bypass sstable in bypass directory
          recycle_bypass_dir();
        }
        else
        {
          //delete all the tablets inserted into tablet iamge, and delete
          //all the hard links in sstable directory
          rollback();
        }

        // re scan all local disk to recycle sstable
        tablet_manager_->get_scan_recycler().recycle();
        tablet_manager_->get_disk_manager().scan(
            THE_CHUNK_SERVER.get_config().datadir,
            OB_DEFAULT_MAX_TABLET_SIZE);

        if (table_list_->response_rootserver_
            && OB_SUCCESS != (ret = tablet_manager_->load_bypass_sstables_over(
            *table_list_, is_load_succ_)))
        {
          TBSYS_LOG(WARN, "failed to report load result to rootserver after loading "
                          "bypass sstables, import_tables_info=%s",
              to_cstring(*table_list_));
        }

        is_pending_upgrade_ = false;
      }
      else
      {
        TBSYS_LOG(WARN, "no bypass sstable was imported, tablet_count=%d",
          tablet_array_.size());
      }
      is_finish_load_ = true;
      TBSYS_LOG(INFO, "finish loading bypass sstables, import_tables_info=%s",
        to_cstring(*table_list_));

      return ret;
    }

    int ObBypassSSTableLoader::rollback()
    {
      TBSYS_LOG(INFO, "load failed, start rollback, import_tables_info=%s",
        to_cstring(*table_list_));

      return recycle_unload_tablets();
    }

    int ObBypassSSTableLoader::recycle_unload_tablets(bool only_recycle_removed_tablet)
    {
      int ret = OB_SUCCESS;

      tablet_array_mutex_.lock();
      ObVector<ObTablet*>::iterator it = tablet_array_.begin();
      for (; it != tablet_array_.end(); ++it)
      {
        if (NULL != *it)
        {
          ret = recycle_tablet(*it, only_recycle_removed_tablet);
        }
      }
      tablet_array_mutex_.unlock();

      return ret;
    }

    int ObBypassSSTableLoader::recycle_tablet(ObTablet* tablet, bool only_recycle_removed_tablet)
    {
      int ret = OB_SUCCESS;
      int32_t disk_no = 0;
      ObMultiVersionTabletImage& tablet_image = tablet_manager_->get_serving_tablet_image();
      ObSSTableId sstable_id;

      if (NULL == tablet)
      {
        TBSYS_LOG(WARN, "invalid param, tablet is NULL");
        ret = OB_INVALID_ARGUMENT;
      }
      else if (!only_recycle_removed_tablet 
               || (only_recycle_removed_tablet && tablet->is_removed()))
      {
        tablet->inc_ref(); //increase ref first, avoid another thread destroy this tablet
        tablet->set_merged();
        tablet->set_removed();  //avoid another thread read this tablet again
        //remove tablet if it exists in serving tablet image
        if (tablet->get_sstable_id_list().count() > 0)
        {
          sstable_id = (tablet->get_sstable_id_list().at(0));
          if (OB_SUCCESS == tablet_image.include_sstable(sstable_id))
          {
            //this function doesn't sync the index file
            ret = tablet_image.remove_tablet(
              tablet->get_range(), tablet->get_data_version(), disk_no, false);
            if (OB_SUCCESS != ret)
            {
              TBSYS_LOG(WARN, "failed to remove bypass tablet, sstable_id=%lu, range=%s",
                sstable_id.sstable_file_id_, to_cstring(tablet->get_range()));
            }
          }
        }

        /**
         * if no another thread hold this tablet, remove the sstable of
         * the tablet and destroy the tablet, else the last thread which
         * releases the tablet will destroy the tablet.
         */
        if (0 == tablet->dec_ref())
        {
          if (OB_SUCCESS == ret
            && OB_SUCCESS != (ret = tablet_image.get_serving_image().remove_sstable(tablet)))
          {
            TBSYS_LOG(WARN, "failed to remove sstable of tablet, range=%s",
            to_cstring(tablet->get_range()));
          }

          tablet->~ObTablet();
        }
      }

      return ret;
    }

    int ObBypassSSTableLoader::recycle_bypass_dir()
    {
      int ret = OB_SUCCESS;

      TBSYS_LOG(INFO, "recycle all sstables in bypass directory");
      for (int32_t i = 0; i < disk_count_; ++i)
      {
        //ignore the returned value
        scan_sstable_files(disk_no_array_[i], &sstable_file_name_filter,
          &ObBypassSSTableLoader::do_recycle_bypass_sstable);
      }

      return ret;
    }

    int ObBypassSSTableLoader::do_recycle_bypass_sstable(
      int32_t disk_no, const char* file_name)
    {
      int ret = OB_SUCCESS;
      char byapss_sstable_path[OB_MAX_FILE_NAME_LENGTH];

      if (disk_no <= 0 || NULL == file_name)
      {
        TBSYS_LOG(WARN, "invalid parameter, disk_no=%d, file_name=%p",
          disk_no, file_name);
        ret = OB_INVALID_ARGUMENT;
      }
      else if (need_import(file_name))
      {
        if (OB_SUCCESS != (ret = get_bypass_sstable_path(disk_no, file_name,
          byapss_sstable_path, OB_MAX_FILE_NAME_LENGTH)))
        {
          TBSYS_LOG(ERROR, "can't get bypass sstable path, disk_no=%d, sstable_name=%s",
            disk_no, file_name);
        }
        else if (0 != ::unlink(byapss_sstable_path))
        {
          TBSYS_LOG(ERROR, "failed to unlink sstable file, sstable_file=%s, "
                           "errno=%d, err=%s",
            byapss_sstable_path, errno, strerror(errno));
          ret = OB_IO_ERROR;
        }
      }

      return ret;
    }

    void ObBypassSSTableLoader::run(CThread* thread, void* arg)
    {
      int64_t thread_index = reinterpret_cast<int64_t>(arg);
      static __thread bool thread_finish_load = true;
      UNUSED(thread);

      TBSYS_LOG(INFO, "load bypass sstables thread start run, thread_index=%ld",
        thread_index);
      while(!_stop)
      {
        cond_.lock();
        while (!_stop && thread_finish_load)
        {
          cond_.wait();
          thread_finish_load = false;
        }

        if (_stop)
        {
          cond_.broadcast();
          cond_.unlock();
          break;
        }
        cond_.unlock();

        load_bypass_sstables(thread_index);
        thread_finish_load = true;
      }
    }

    int ObBypassSSTableLoader::load_bypass_sstables(const int64_t thread_index)
    {
      int ret = OB_SUCCESS;
      int64_t start_index = 0;
      int64_t end_index = 0;
      int64_t disks_per_thread = 0;
      int64_t mod = 0;

      if (NULL == disk_no_array_ || disk_count_ <= 0 || thread_index < 0)
      {
        TBSYS_LOG(ERROR, "invalid disk no array or disk count, disk_no_array_=%p, "
                         "disk_count_=%d, thread_index=%ld",
          disk_no_array_, disk_count_, thread_index);
        ret = OB_ERROR;
      }
      else
      {
        if (_threadCount > 0)
        {
          //thread count is greater than or equal to disk count
          if (_threadCount >= disk_count_)
          {
            start_index = thread_index >= disk_count_ ? disk_count_ : thread_index;
            end_index = thread_index >= disk_count_ ? disk_count_ : thread_index + 1;
          }
          else
          {
            // thread count is less than disk count
            disks_per_thread = disk_count_ / _threadCount;
            mod = disk_count_ % _threadCount;
            if (thread_index < mod)
            {
              start_index = thread_index * (disks_per_thread + 1);
              end_index = (thread_index + 1) * (disks_per_thread + 1);
            }
            else
            {
              start_index = mod * (disks_per_thread + 1) + (thread_index - mod) * disks_per_thread;
              end_index = mod * (disks_per_thread + 1) + (thread_index + 1 - mod) * disks_per_thread;
            }
          }
        }

        for (int64_t i = start_index; i < end_index && i < disk_count_; ++i)
        {
          ret = scan_sstable_files(disk_no_array_[i], &sstable_file_name_filter,
            &ObBypassSSTableLoader::do_load_sstable);
          if (OB_SUCCESS != ret)
          {
            TBSYS_LOG(WARN, "failed to scan bypass sstable file in disk no=%d",
              disk_no_array_[i]);

            /**
             * is_load_succ_ will be accessed by multi-thread, but all the
             * threads only read it except that multi-thread will set it to
             * false, but not set it to true in multi-thread case. so here
             * we not use lock to protect it.
             */
            is_load_succ_ = false;
          }
          else if (static_cast<uint32_t>(disk_count_) == atomic_inc(&finish_load_disk_cnt_))
          {
            ret = finish_load();
            if (OB_SUCCESS != ret)
            {
              TBSYS_LOG(WARN, "failed to finish load, is_load_succ=%d", is_load_succ_);
            }
            if (i != end_index - 1)
            {
              TBSYS_LOG(ERROR, "expect that all sstable in all disks_per_disk are loaded, "
                               "finish_load_disk_cnt_=%u, i=%ld, end_index=%ld",
                finish_load_disk_cnt_, i, end_index);
            }
            break;
          }
        }
      }

      return ret;
    }

    int ObBypassSSTableLoader::sstable_file_name_filter(const struct dirent* d)
    {
      int ret = 0;
      uint64_t table_id = OB_INVALID_ID;
      int64_t seq_no = -1;
      int num = 0;
      uint64_t sstable_id = 0;

      if (NULL != index(d->d_name, '-'))
      {
        /**
         * bypass sstable name format:
         *    ex: 1001-000001
         *    1001    table id
         *    -       delimeter '-'
         *    000001  range sequence number, 6 chars
         */
        num = sscanf(d->d_name, "%lu-%06ld", &table_id, &seq_no);
        ret = (2 == num && OB_INVALID_ID != table_id && seq_no >= 0) ? 1 : 0;
      }
      else
      {
        /**
         * the sstable id format
         */
        sstable_id = strtoull(d->d_name, NULL, 10);
        ret = sstable_id > 0 ? 1 : 0;
      }

      return ret;
    }

    int ObBypassSSTableLoader::scan_sstable_files(
      const int32_t disk_no, Filter filter, Operate op)
    {
      int ret                     = OB_SUCCESS;
      int tmp_err                 = OB_SUCCESS;
      int64_t file_num            = 0;
      struct dirent** file_dirent = NULL;
      char directory[OB_MAX_FILE_NAME_LENGTH];

      //each disk has one byapss dirctory
      ret = get_bypass_sstable_directory(disk_no, directory, OB_MAX_FILE_NAME_LENGTH);
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(ERROR, "get byapss sstable directory error, disk_no=%d.", disk_no);
      }
      else if (!FileDirectoryUtils::is_directory(directory))
      {
        TBSYS_LOG(ERROR, "byapss sstable dir doesn't exist, dir=%s", directory);
        ret = OB_DIR_NOT_EXIST;
      }
      else if ((file_num = ::scandir(directory,
                &file_dirent, filter, ::versionsort)) <= 0
               || NULL == file_dirent)
      {
        TBSYS_LOG(INFO, "byapss directory=%s doesn't have sstable files.", directory);
      }
      else
      {
        /**
         * we don't break the loop if some errors happen, just stores
         * the error status and continue the loop to free the memory ot
         * file_dirent struct.
         */
        for (int64_t n = 0; n < file_num; ++n)
        {
          if (NULL == file_dirent[n])
          {
            TBSYS_LOG(WARN, "scandir return null dirent[%ld]. directory=%s",
              n, directory);
            tmp_err = OB_IO_ERROR;
          }
          else
          {
            ret = (this->*op)(disk_no, file_dirent[n]->d_name);
            if (OB_SUCCESS != ret)
            {
              tmp_err = ret;
            }

            ::free(file_dirent[n]);
          }
        }
        ret = tmp_err;
      }

      if (NULL != file_dirent)
      {
        ::free(file_dirent);
        file_dirent = NULL;
      }

      return ret;
    }

    int ObBypassSSTableLoader::do_load_sstable(const int32_t disk_no, const char* file_name)
    {
      int ret = OB_SUCCESS;

      /**
       * is_continue_load_ is accessed by multi-thread, we don't use
       * the lock to protect it. if one loading thread happens error,
       * all the loading thread must stop loading bypass sstable. it
       * can work.
       */
      if (is_continue_load_&& need_import(file_name))
      {
        ret = load_one_bypass_sstable(disk_no, file_name);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "load_one_bypass_sstable failed, disk_no=%d, file_name=%s",
            disk_no, file_name);
          is_continue_load_ = false;
        }

        if (_stop)
        {
          cond_.broadcast();
          is_continue_load_ = false;
        }
      }

      return ret;
    }

    bool ObBypassSSTableLoader::need_import(const char* file_name) const
    {
      bool ret = false;
      uint64_t table_id = OB_INVALID_ID;
      int64_t seq_no = -1;
      int num = 0;
      uint64_t sstable_id = 0;

      if (NULL != index(file_name, '-'))
      {
        /**
         * bypass sstable name format:
         *    ex: 1001-000001
         *    1001    table id
         *    -       delimeter '-'
         *    000001  range sequence number, 6 chars
         */
        num = sscanf(file_name, "%lu-%06ld", &table_id, &seq_no);
        ret = (2 == num && OB_INVALID_ID != table_id && seq_no >= 0) ? true : false;
        if (ret)
        {
          ret = (NULL != table_list_ && table_list_->is_table_exist(table_id)) ? true : false;
        }
      }
      else
      {
        /**
         * the sstable id format
         */
        sstable_id = strtoull(file_name, NULL, 10);
        ret = sstable_id > 0 ? true : false;
      }

      return ret;
    }

    int ObBypassSSTableLoader::load_one_bypass_sstable(
      const int32_t disk_no, const char* file_name)
    {
      int ret = OB_SUCCESS;
      ObTablet* tablet = NULL;
      char bypass_sstable_path[OB_MAX_FILE_NAME_LENGTH];
      char link_sstable_path[OB_MAX_FILE_NAME_LENGTH];
      ObSSTableId sstable_id;

      if (disk_no <= 0 || NULL == file_name)
      {
        TBSYS_LOG(WARN, "invalid parameter, disk_no=%d, file_name=%p",
          disk_no, file_name);
        ret = OB_INVALID_ARGUMENT;
      }
      else if (OB_SUCCESS != (ret = get_bypass_sstable_path(disk_no, file_name,
          bypass_sstable_path, OB_MAX_FILE_NAME_LENGTH)))
      {
        TBSYS_LOG(ERROR, "can't get bypass sstable path, disk_no=%d, sstable_name=%s",
          disk_no, file_name);
      }
      else if (OB_SUCCESS != (ret = create_hard_link_sstable(bypass_sstable_path,
        link_sstable_path, OB_MAX_FILE_NAME_LENGTH, disk_no, sstable_id)))
      {
        TBSYS_LOG(ERROR, "can't create hard link for bypass sstable, "
                         "disk_no=%d, sstable_name=%s",
          disk_no, file_name);
      }
      else if (OB_SUCCESS != (ret = add_new_tablet(sstable_id, disk_no, tablet)))
      {
        TBSYS_LOG(ERROR, "can't add new tablet for bypass sstable into tablet image, "
                         "disk_no=%d, sstable_name=%s",
          disk_no, file_name);
      }
      else if (NULL != tablet)
      {
        TBSYS_LOG(INFO, "create hard link of bypass sstble=%s to dst sstable=%s, range=%s",
          bypass_sstable_path, link_sstable_path, to_cstring(tablet->get_range()));
      }

      return ret;
    }

    int ObBypassSSTableLoader::create_hard_link_sstable(
      const char* bypass_sstable_path, char* link_sstable_path,
      const int64_t path_size, const int32_t disk_no, ObSSTableId& sstable_id)
    {
      int ret = OB_SUCCESS;
      int64_t sstable_size = 0;
      ObSSTableId old_sstable_id;

      if (NULL == bypass_sstable_path || NULL == link_sstable_path
          || path_size <= 0 || disk_no <= 0)
      {
        TBSYS_LOG(WARN, "invalid parameter, bypass_sstable_path=%p, "
                        "link_sstable_path=%p, path_size=%ld, disk_no=%d",
          bypass_sstable_path, link_sstable_path, path_size, disk_no);
        ret = OB_INVALID_ARGUMENT;
      }
      else if ((sstable_size = get_file_size(bypass_sstable_path)) <= 0)
      {
        if (sstable_size < 0)
        {
          TBSYS_LOG(ERROR, "get file size error, sstable_size=%ld, bypass_sstable_path=%s, err=%s",
              sstable_size, bypass_sstable_path, strerror(errno));
          ret = OB_IO_ERROR;
        }
        else if (0 == sstable_size)
        {
          TBSYS_LOG(ERROR, "can't load empty bypass sstable, bypass sstable size=%ld",
              sstable_size);
          ret = OB_ERROR;
        }
      }
      else
      {
        do
        {
          sstable_id.sstable_file_id_ = tablet_manager_->allocate_sstable_file_seq();
          sstable_id.sstable_file_id_ = (sstable_id.sstable_file_id_ << 8) | (disk_no & 0xff);

          if (OB_SUCCESS != (ret = get_sstable_path(sstable_id, link_sstable_path, path_size)) )
          {
            TBSYS_LOG(ERROR, "create_hard_link_sstable: can't get the path of hard link sstable");
            ret = OB_ERROR;
          }
        } while (OB_SUCCESS == ret && FileDirectoryUtils::exists(link_sstable_path));

        if (OB_SUCCESS == ret)
        {
          if (0 != ::link(bypass_sstable_path, link_sstable_path))
          {
            TBSYS_LOG(ERROR, "failed create hard link for bypass sstable, "
                             "bypass_sstable_path=%s, new_sstable=%s",
              bypass_sstable_path, link_sstable_path);
            ret = OB_IO_ERROR;
          }
          else
          {
            tablet_manager_->get_disk_manager().add_used_space(disk_no, sstable_size);
          }
        }
      }

      return ret;
    }

    int ObBypassSSTableLoader::add_new_tablet(
      const ObSSTableId& sstable_id, const int32_t disk_no, ObTablet*& tablet)
    {
      int ret = OB_SUCCESS;
      ObTablet* new_tablet = NULL;
      ObMultiVersionTabletImage& tablet_image = tablet_manager_->get_serving_tablet_image();
      tablet = NULL;

      if (disk_no <= 0 || OB_INVALID_ID == sstable_id.sstable_file_id_)
      {
        TBSYS_LOG(WARN, "invalid parameter, sstable_id=%lu, disk_no=%d",
          sstable_id.sstable_file_id_, disk_no);
        ret = OB_INVALID_ARGUMENT;
      }
      else if (OB_SUCCESS != (ret = tablet_image.alloc_tablet_object(
        table_list_->tablet_version_, new_tablet)))
      {
        TBSYS_LOG(ERROR, "alloc_tablet_object failed, sstable_id=%lu, disk_no=%d, "
                         "load_version=%ld",
          sstable_id.sstable_file_id_, disk_no, table_list_->tablet_version_);
      }
      else
      {
        new_tablet->set_disk_no(disk_no);
        if (OB_SUCCESS != (ret = new_tablet->add_sstable_by_id(sstable_id)))
        {
          TBSYS_LOG(ERROR, "add sstable to tablet failed, sstable_id=%lu, disk_no=%d, "
                           "load_version=%ld",
            sstable_id.sstable_file_id_, disk_no, table_list_->tablet_version_);
        }

        if (OB_SUCCESS == ret)
        {
          tablet_array_mutex_.lock();
          if (OB_SUCCESS != (ret = tablet_array_.push_back(new_tablet)))
          {
            TBSYS_LOG(ERROR, "add tablet to tmp tablet array failed, "
                             "sstable_id=%lu, disk_no=%d, load_version=%ld",
              sstable_id.sstable_file_id_, disk_no, table_list_->tablet_version_);
          }
          tablet_array_mutex_.unlock();
        }

        if (OB_SUCCESS == ret)
        {
          if (OB_SUCCESS != (ret = new_tablet->load_sstable(table_list_->tablet_version_)))
          {
            TBSYS_LOG(ERROR, "failed to load sstable, sstable_id=%lu, disk_no=%d, "
                             "load_version=%ld",
              sstable_id.sstable_file_id_, disk_no, table_list_->tablet_version_);
          }
          else
          {
            tablet = new_tablet;
          }
        }
      }

      return ret;
    }

    int ObBypassSSTableLoader::add_bypass_tablets_into_image()
    {
      int ret = OB_SUCCESS;
      ObMultiVersionTabletImage& tablet_image = tablet_manager_->get_serving_tablet_image();

      tablet_array_mutex_.lock();
      ObVector<ObTablet*>::iterator it = tablet_array_.begin();

      for (; it != tablet_array_.end(); ++it)
      {
        if (NULL != *it)
        {
          if (OB_SUCCESS != (ret = tablet_image.add_tablet(
            *it, true, tablet_image.get_serving_version() == 0)))
          {
            TBSYS_LOG(ERROR, "add tablet to tablet image failed, range=%s",
              to_cstring((*it)->get_range()));
            break;
          }
        }
      }
      tablet_array_mutex_.unlock();

      return ret;
    }
  } // end namespace chunkserver
} // end namespace oceanbase
