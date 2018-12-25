/**
 * (C) 2010-2012 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *  
 * ob_multi_tablet_merger.h for merge multi-tablet. 
 *
 * Authors:
 *   huating <huating.zmq@taobao.com>
 *
 */
#include "common/file_directory_utils.h"
#include "sstable/ob_sstable_reader.h"
#include "sstable/ob_disk_path.h"
#include "ob_tablet_image.h"
#include "ob_tablet_manager.h"
#include "ob_multi_tablet_merger.h"
#include "ob_chunk_server_main.h"

namespace oceanbase 
{
  namespace chunkserver 
  {
    using namespace common;
    using namespace sstable;

    ObMultiTabletMerger::ObMultiTabletMerger()
    : manager_(NULL), sstable_size_(0), max_tablet_seq_(0)
    {
      path_[0] = '\0';
      memset(merge_tablets_, 0, sizeof(merge_tablets_));
      tablet_array_.init(MAX_MERGE_TABLET_NUM, merge_tablets_, 0);

      memset(sstable_readers_, 0, sizeof(sstable_readers_));
      sstable_array_.init(MAX_MERGE_TABLET_NUM, sstable_readers_, 0);
    }

    ObMultiTabletMerger::~ObMultiTabletMerger()
    {

    }

    void ObMultiTabletMerger::reset()
    {
      manager_ = NULL;
      path_[0] = '\0';
      sstable_id_.sstable_file_id_ = 0;
      sstable_id_.sstable_file_offset_ = 0;
      sstable_size_ = 0;
      sstable_path_.assign_ptr(NULL, 0);
      tablet_array_.clear();
      sstable_array_.clear();
      max_tablet_seq_ = 0;
    }

    void ObMultiTabletMerger::cleanup()
    {
      sstable_merger_.cleanup();
      reset();
    }

    int ObMultiTabletMerger::merge_tablets(
      ObTabletManager& manager,
      common::ObTabletReportInfoList& tablet_list, 
      const int64_t serving_version)
    {
      int ret = OB_SUCCESS;
      ObTablet* new_tablet = NULL;
      ObNewRange new_range;

      reset();
      manager_ = &manager;
      if (OB_SUCCESS != (ret = check_range_list_param(tablet_list, serving_version)))
      {
        TBSYS_LOG(WARN, "failed to check range list param, serving_version=%ld, ret=%d", 
          serving_version, ret);
      }
      else if (OB_SUCCESS != (ret = acquire_tablets_and_readers(tablet_list)))
      {
        TBSYS_LOG(WARN, "failed to acquire tablets and readers, serving_version=%ld, ret=%d", 
          serving_version, ret);
      }
      else if (OB_SUCCESS != (ret = get_new_tablet_range(new_range)))
      {
        TBSYS_LOG(WARN, "failed to get new tablet range");
      }
      else if (OB_SUCCESS != (ret = do_merge_sstable(new_range)))
      {
        TBSYS_LOG(WARN, "failed to merge sstable, ret=%d", ret);
      }
      else if (OB_SUCCESS != (ret = create_new_tablet(new_range, new_tablet)) 
               || NULL == new_tablet)
      {
        TBSYS_LOG(WARN, "failed to create new tablet, new_tablet=%p, ret=%d", 
          new_tablet, ret);
      }
      else if (NULL != new_tablet 
               && OB_SUCCESS != (ret = update_tablet_image(new_tablet)))
      {
        char range_buf[OB_RANGE_STR_BUFSIZ];
        new_tablet->get_range().to_string(range_buf, sizeof(range_buf));
        TBSYS_LOG(WARN, "failed to update tablet image, new_tablet=%p, ret=%d, range=%s", 
          new_tablet, ret, range_buf);
      }
      else if (NULL != new_tablet 
               && OB_SUCCESS != (ret = fill_return_tablet_list(tablet_list, *new_tablet)))
      {
        char range_buf[OB_RANGE_STR_BUFSIZ];
        new_tablet->get_range().to_string(range_buf, sizeof(range_buf));
        TBSYS_LOG(WARN, "failed to fill return tablet list, new_tablet=%p, "
                        "ret=%d, range=%s", 
          new_tablet, ret, range_buf);
      }

      if (OB_SUCCESS != release_tablets())
      {
        TBSYS_LOG(WARN, "failed to release tablets");
      }

      return ret;
    }

    int ObMultiTabletMerger::check_range_list_param(
      const ObTabletReportInfoList& tablet_list, 
      const int64_t serving_version)
    {
      int ret = OB_SUCCESS;
      int64_t tablet_count = tablet_list.get_tablet_size();
      const ObTabletReportInfo* const tablet_infos = tablet_list.get_tablet();

      if (NULL == tablet_infos || tablet_count <= 1)
      {
        TBSYS_LOG(WARN, "invalid param, merge tablet count must be greater "
                        "than 1, tablet_infos=%p, tablet_count=%ld", 
          tablet_infos, tablet_count);
        ret = OB_INVALID_ARGUMENT;
      }
      else 
      {
        for (int64_t i = 0; i < tablet_count; ++i)
        {
          if (serving_version != tablet_infos[i].tablet_location_.tablet_version_)
          {
            TBSYS_LOG(WARN, "merge tabelt version isn't equal to current "
                            "chunkserver serving version, serving_version=%ld,"
                            "tablet_version=%ld",
              serving_version, tablet_infos[i].tablet_location_.tablet_version_);
            ret = OB_INVALID_ARGUMENT;
            break;
          }

          if (tablet_infos[i].tablet_info_.range_.empty())
          {
            TBSYS_LOG(WARN, "the %ldth tablet range to merge is empty", i);
            ret = OB_INVALID_ARGUMENT;
            break;
          }

          if (i > 0 && 0 != memcmp(
            tablet_infos[i - 1].tablet_info_.range_.end_key_.ptr(), 
            tablet_infos[i].tablet_info_.range_.start_key_.ptr(), 
            tablet_infos[i].tablet_info_.range_.start_key_.length()))
          {
            char prev_range_buf[OB_RANGE_STR_BUFSIZ];
            char cur_range_buf[OB_RANGE_STR_BUFSIZ];
            tablet_infos[i - 1].tablet_info_.range_.to_string(
              prev_range_buf, sizeof(prev_range_buf));
            tablet_infos[i].tablet_info_.range_.to_string(
              cur_range_buf, sizeof(cur_range_buf));
            TBSYS_LOG(WARN, "range isn't adjacent, prev_range=%s, cur_range=%s", 
              prev_range_buf, cur_range_buf);
            ret = OB_INVALID_ARGUMENT;
            break;
          }
        }
      }

      if (OB_SUCCESS == ret)
      {
        // dump merge tablets information.
        for (int64_t i = 0; i < tablet_list.get_tablet_size(); ++i)
        {
          const ObTabletReportInfo& info = tablet_list.get_tablet()[i];
          TBSYS_LOG(INFO, "merge tablets[%ld]<%s>, "
              "tablet occupy size:%ld, row count:%ld, checksum:%lu, version:%ld, sequence_num=%ld", 
              i, to_cstring(info.tablet_info_.range_), info.tablet_info_.occupy_size_, 
              info.tablet_info_.row_count_, info.tablet_info_.crc_sum_,
              info.tablet_location_.tablet_version_,
              info.tablet_location_.tablet_seq_);
        }
      }

      return ret;
    }

    int ObMultiTabletMerger::acquire_tablets_and_readers(
      const ObTabletReportInfoList& tablet_list)
    {
      int ret = OB_SUCCESS;
      ObTablet* tablet = NULL;
      ObSSTableReader* reader = NULL;
      ObMultiVersionTabletImage& tablet_image = manager_->get_serving_tablet_image();
      int64_t tablet_count = tablet_list.get_tablet_size();
      const ObTabletReportInfo* const tablet_infos = tablet_list.get_tablet();
      char range_buf[OB_RANGE_STR_BUFSIZ];

      for (int64_t i = 0; i < tablet_count && OB_SUCCESS == ret; ++i)
      {
        reader = NULL;
        if (OB_SUCCESS != (ret = tablet_image.acquire_tablet(
            tablet_infos[i].tablet_info_.range_, 
            ObMultiVersionTabletImage::SCAN_FORWARD, 0, tablet)))
        {
          tablet_infos[i].tablet_info_.range_.to_string(range_buf, sizeof(range_buf));
          TBSYS_LOG(WARN, "failed to qcquire tablet from tablet image, "
                          "index=%ld, range=%s",
             i, range_buf);
        }
        else if (OB_SUCCESS == ret && NULL == tablet)
        {
          tablet_infos[i].tablet_info_.range_.to_string(range_buf, sizeof(range_buf));
          TBSYS_LOG(WARN, "tablet image acquire tablet successfully, "
                          "but tablet is NULL, index=%ld, range=%s",
            i, range_buf);
          ret = OB_ERROR;
        }
        else if (tablet->get_sequence_num() != tablet_infos[i].tablet_location_.tablet_seq_)
        {
          tablet_infos[i].tablet_info_.range_.to_string(range_buf, sizeof(range_buf));
          TBSYS_LOG(WARN, "tablet sequence num from rootserver is different "
                          "from chunkserver, rs_tablet_seq=%ld, cs_tablet_seq=%ld, "
                          "index=%ld, range=%s",
            tablet_infos[i].tablet_location_.tablet_seq_, 
            tablet->get_sequence_num(), i, range_buf);
          ret = OB_ERROR;
        }
        else if (!tablet_array_.push_back(tablet))
        {
          TBSYS_LOG(WARN, "add tablet push to tablet array error, size=%ld",
              tablet_array_.get_array_index());
          ret = OB_SIZE_OVERFLOW;
        }
        else if (OB_SUCCESS != (ret = tablet->load_sstable()))
        {
          tablet_infos[i].tablet_info_.range_.to_string(range_buf, sizeof(range_buf));
          TBSYS_LOG(WARN, "failed to load sstable, index=%ld, range=%s", 
            i, range_buf);
        }
        else if (tablet->get_sstable_reader_list().count() > 0
                 && NULL == (reader = dynamic_cast<ObSSTableReader*>(tablet->get_sstable_reader_list().at(0))))
        {
          tablet_infos[i].tablet_info_.range_.to_string(range_buf, sizeof(range_buf));
          TBSYS_LOG(WARN, "tablet has NULL sstable reader, index=%ld, range=%s", 
            i, range_buf);
          ret = OB_ERROR;
        }
        else if (NULL != reader && !sstable_array_.push_back(reader))
        {
          TBSYS_LOG(WARN, "add sstable reader push to sstable array error, size=%ld",
              sstable_array_.get_array_index());
          ret = OB_SIZE_OVERFLOW;
        }
        else if (NULL != tablet && tablet->get_sequence_num() > max_tablet_seq_)
        {
          max_tablet_seq_ = tablet->get_sequence_num(); 
        }
      }

      return ret;
    }

    int ObMultiTabletMerger::do_merge_sstable(const ObNewRange& new_range)
    {
      int ret = OB_SUCCESS;

      //init sstable_id_ and stable_path_ first, the disk_no is gotten from sstable_id_
      if (OB_SUCCESS != (ret = get_new_sstable_path(sstable_path_)))
      {
        TBSYS_LOG(WARN, "failed to get sstable path for merged sstable");
      }
      else 
      {
        if (sstable_array_.get_array_index() > 0)
        {
          if (OB_SUCCESS != (ret = sstable_merger_.merge_sstables(sstable_path_, 
              manager_->get_serving_block_cache(), 
              manager_->get_serving_block_index_cache(),
              sstable_readers_, sstable_array_.get_array_index(), new_range)))
          {
            TBSYS_LOG(WARN, "failed to merge sstable, ret=%d", ret);
          }
        }
        else 
        {
          //do nothing, no sstable to merge
          TBSYS_LOG(INFO, "all tablets to merge is empty, tablet_count=%ld",
            tablet_array_.get_array_index());
        }
      }

      return ret;
    }

    int ObMultiTabletMerger::get_new_sstable_path(ObString& sstable_path)
    {
      int ret               = OB_SUCCESS;
      bool is_sstable_exist = false;
      int32_t disk_no       = manager_->get_disk_manager().get_dest_disk();
      sstable_id_.sstable_file_id_     = manager_->allocate_sstable_file_seq();
      sstable_id_.sstable_file_offset_ = 0;
      sstable_path.assign_ptr(NULL, 0);

      if (disk_no < 0)
      {
        TBSYS_LOG(ERROR, "does't have enough disk space");
        ret = OB_CS_OUTOF_DISK_SPACE;
      }
      else
      {
        /**
         * mustn't use the same sstable id in the the same disk, because 
         * if tablet isn't changed, we just add a hard link pointer to 
         * the old sstable file, so maybe different sstable file pointer 
         * the same content in disk. if we cancle daily merge, maybe 
         * some tablet meta isn't synced into index file, then we 
         * restart chunkserver will do daily merge again, it may reuse 
         * the same sstable id, if the sstable id is existent and it 
         * pointer to a share disk content, the sstable will be 
         * truncated if we create sstable with the sstable id. 
         */
        do
        {
          sstable_id_.sstable_file_id_ = (sstable_id_.sstable_file_id_ << 8) | (disk_no & 0xff);

          if (OB_SUCCESS == ret
              && OB_SUCCESS != (ret = get_sstable_path(sstable_id_, path_, sizeof(path_))) )
          {
            TBSYS_LOG(WARN, "can't get the path of new sstable");
            ret = OB_ERROR;
          }

          if (OB_SUCCESS == ret)
          {
            is_sstable_exist = FileDirectoryUtils::exists(path_);
            if (is_sstable_exist)
            {
              sstable_id_.sstable_file_id_ = manager_->allocate_sstable_file_seq();
            }
          }
        } while (OB_SUCCESS == ret && is_sstable_exist);

        if (OB_SUCCESS == ret)
        {
          sstable_path.assign_ptr(path_, static_cast<int32_t>(strlen(path_) + 1));
        }
      }

      return ret;
    }

    int ObMultiTabletMerger::create_new_tablet(
      const ObNewRange& new_range, ObTablet*& new_tablet)
    {
      int ret = OB_SUCCESS;
      ObMultiVersionTabletImage& tablet_image = manager_->get_serving_tablet_image();
      const ObSSTableTrailer* trailer = NULL;
      ObTablet* tablet = NULL;
      ObTablet* first_tablet = *tablet_array_.at(0);
      int64_t sstable_size = 0;
      ObTabletExtendInfo extend_info;
      new_tablet = NULL;

      if (NULL == first_tablet)
      {
        TBSYS_LOG(WARN, "the first tablet to merge is NULL");
        ret = OB_ERROR;
      }
      else if (OB_SUCCESS != (ret = tablet_image.alloc_tablet_object(
        new_range, first_tablet->get_data_version(), tablet)))
      {
        TBSYS_LOG(WARN, "alloc_tablet_object failed.");
      }
      else
      {
        if (sstable_array_.get_array_index() > 0)
        {
          trailer = &sstable_merger_.get_sstable_trailer();
          sstable_size = sstable_merger_.get_sstable_size();
        }
        else
        {
          //default value
          //trailer = NULL;
          //sstable_size = 0;
        }

        //set new data version
        tablet->set_data_version(first_tablet->get_data_version());
        tablet->set_disk_no((sstable_id_.sstable_file_id_) & DISK_NO_MASK);
        extend_info.last_do_expire_version_ = first_tablet->get_last_do_expire_version();

        if (sstable_size > 0)
        {
          if (NULL == trailer)
          {
            TBSYS_LOG(ERROR, "after merge sstable, trailer=NULL, "
                             "sstable_size=%ld", sstable_size);
            ret = OB_ERROR;
          }
          else if (OB_SUCCESS != (ret = tablet->add_sstable_by_id(sstable_id_)))
          {
            TBSYS_LOG(ERROR, "add sstable to tablet failed.");
          }
          else 
          {
            int64_t pos = 0;
            int64_t checksum_len = sizeof(uint64_t);
            char checksum_buf[checksum_len];

            if (trailer->get_row_count() > 0)
            {
              extend_info.occupy_size_ = sstable_size;
              extend_info.row_count_ = trailer->get_row_count();
              ret = serialization::encode_i64(checksum_buf, 
                  checksum_len, pos, trailer->get_sstable_checksum());
              if (OB_SUCCESS == ret)
              {
                extend_info.check_sum_ = ob_crc64(checksum_buf, checksum_len);
              }
            }
          }
        }
        else if (0 == sstable_size)
        {
          /**
           * the tablet doesn't include sstable, the occupy_size_, 
           * row_count_ and check_sum_ in extend_info is 0, needn't set 
           * them again 
           */
        }

        /**
         * we set the extent info of tablet here. when we write the 
         * index file, we needn't load the sstable of this tablet to get 
         * the extend info. 
         */
        if (OB_SUCCESS == ret)
        {
          extend_info.sequence_num_ = max_tablet_seq_ + 1;  // seq + 1 as new seq
          tablet->set_extend_info(extend_info);
        }
      }

      if (OB_SUCCESS == ret)
      {
        new_tablet = tablet;
        manager_->get_disk_manager().add_used_space(
          (sstable_id_.sstable_file_id_ & DISK_NO_MASK), 
          sstable_size, (sstable_array_.get_array_index() > 1));
      }

      return ret;
    }

    int ObMultiTabletMerger::get_new_tablet_range(ObNewRange& new_range)
    {
      int ret = OB_SUCCESS;
      ObTablet* first_tablet = *tablet_array_.at(0);
      ObTablet* last_tablet = *tablet_array_.at(tablet_array_.get_array_index() - 1);

      if (NULL == first_tablet || NULL == last_tablet || first_tablet == last_tablet)
      {
        TBSYS_LOG(WARN, "invalid tablet in tablet array to merge, first_tablet=%p, "
                        "last_tablet=%p",
          first_tablet, last_tablet);
        ret = OB_ERROR;
      }
      else
      {
        new_range = first_tablet->get_range();
        new_range.end_key_ = last_tablet->get_range().end_key_;
        if (last_tablet->get_range().end_key_.is_max_row())
        {
          TBSYS_LOG(INFO, "this last mrege tablet has max flag");
          new_range.end_key_.set_max_row();
          new_range.border_flag_.unset_inclusive_end();
        }
      }

      return ret;
    }

    int ObMultiTabletMerger::update_tablet_image(ObTablet* tablet)
    {
      int ret = OB_SUCCESS;
      int32_t disk_no = 0;
      ObMultiVersionTabletImage& tablet_image = manager_->get_serving_tablet_image();

      /**
       * FIXME: if remove some tablets from tablet image failed, it 
       * can't rollback, and if it doesn't write the index file, we 
       * can restart chunk server to fix it. if write some disk index 
       * files, the origin tablet will be lost. if the same tablet in 
       * different chunk servers loss at the same time, the tablet 
       * will be lost. 
       */
      for (int64_t i = 0; i < tablet_array_.get_array_index() && OB_SUCCESS == ret; ++i)
      {
        if (NULL != merge_tablets_[i])
        {
          //remove tablet from tablet image, then release tablet
          ret = tablet_image.remove_tablet(merge_tablets_[i]->get_range(),
            merge_tablets_[i]->get_data_version(), disk_no);
          if (OB_SUCCESS != ret)
          {
            TBSYS_LOG(ERROR, "failed to remove tablet, tablet=%p, ret=%d", 
                      merge_tablets_[i], ret);
            break;
          }

          if (OB_SUCCESS == ret)
          {
            /**
             * set the removed flag, when release tablet, if the ref_count 
             * is 0, the sstable resource of tablet will be released. 
             */
            merge_tablets_[i]->set_removed();
            merge_tablets_[i]->set_merged();
            ret = tablet_image.release_tablet(merge_tablets_[i]);
            if (OB_SUCCESS != ret)
            {
              TBSYS_LOG(ERROR, "failed to release tablet, tablet=%p, err=%d", 
                        merge_tablets_[i], ret);
            }
            else
            {
              merge_tablets_[i] = NULL;
            }
          }
        }
      }

      if (OB_SUCCESS == ret
          && OB_SUCCESS != (ret = tablet_image.add_tablet(tablet)))
      {
        TBSYS_LOG(ERROR, "add new merged tablet into tablet iamge failed, "
                         "new_range=%s", to_cstring(tablet->get_range()));
      }
      else if (OB_SUCCESS != (ret = tablet_image.write(
               tablet->get_data_version(), tablet->get_disk_no())))
      {
        TBSYS_LOG(WARN, "write new meta failed version=%ld, disk_no=%d",  
            tablet->get_data_version(), tablet->get_disk_no());
      }

      return ret;
    }

    int ObMultiTabletMerger::fill_return_tablet_list(
      ObTabletReportInfoList& tablet_list, const ObTablet& tablet)
    {
      int ret = OB_SUCCESS;
      ObTabletReportInfo report_tablet_info;

      tablet_list.reset();

      report_tablet_info.tablet_info_.range_ = tablet.get_range();
      report_tablet_info.tablet_info_.occupy_size_ = tablet.get_occupy_size();
      report_tablet_info.tablet_info_.row_count_ = tablet.get_row_count();
      report_tablet_info.tablet_info_.crc_sum_ = tablet.get_checksum();
      report_tablet_info.tablet_location_.tablet_version_ = tablet.get_data_version();
      report_tablet_info.tablet_location_.tablet_seq_ = tablet.get_sequence_num();
      report_tablet_info.tablet_location_.chunkserver_ = THE_CHUNK_SERVER.get_self();

      ret = tablet_list.add_tablet(report_tablet_info);

      return ret;
    }

    int ObMultiTabletMerger::release_tablets()
    {
      int err = OB_SUCCESS;
      int ret = OB_SUCCESS;
      ObMultiVersionTabletImage& tablet_image = manager_->get_serving_tablet_image();

      for (int64_t i = 0; i < tablet_array_.get_array_index(); ++i)
      {
        if (NULL != merge_tablets_[i])
        {
          err = tablet_image.release_tablet(merge_tablets_[i]);
          if (OB_SUCCESS != err)
          {
            TBSYS_LOG(WARN, "failed to release tablet, tablet=%p, err=%d", 
                      merge_tablets_[i], err);
            ret = err;
          }
        }
      }

      return ret;
    }
  } // end namespace chunkserver
} // end namespace oceanbase
