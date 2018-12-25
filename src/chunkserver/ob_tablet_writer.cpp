/**
 * (C) 2010-2011 Taobao Inc.
 *
 *  This program is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU General Public License
 *  version 2 as published by the Free Software Foundation.
 *
 *  ob_tablet_writer.cpp
 *
 *  Authors:
 *    zian <yunliang.shi@alipay.com>
 *
 */
#include "ob_tablet_writer.h"

namespace oceanbase
{
  namespace chunkserver
  {
    int ObSSTableWriterUtil::gen_sstable_file_location(const int32_t disk_no,
        sstable::ObSSTableId& sstable_id, char* path, const int64_t path_size)
    {
      int ret = OB_SUCCESS;
      bool is_sstable_exist = false;
      sstable_id.sstable_file_id_ = manager_.allocate_sstable_file_seq();
      sstable_id.sstable_file_offset_ = 0;
      do
      {
        sstable_id.sstable_file_id_ = (sstable_id.sstable_file_id_ << 8) | (disk_no & DISK_NO_MASK);

        if ( OB_SUCCESS != (ret = get_sstable_path(sstable_id, path, path_size)) )
        {
          TBSYS_LOG(WARN, "can't get the path of new sstable, ");
        }
        else if (true == (is_sstable_exist = FileDirectoryUtils::exists(path)))
        {
          // reallocate new file seq until get file name not exist.
          sstable_id.sstable_file_id_ = manager_.allocate_sstable_file_seq();
        }
      } while (OB_SUCCESS == ret && is_sstable_exist);

      return ret;
    }

    int ObSSTableWriterUtil::cleanup_uncomplete_sstable_files(const ObSSTableId& sstable_id)
    {
      int64_t file_id = 0;
      char path[OB_MAX_FILE_NAME_LENGTH];

      if ((sstable_id.sstable_file_id_ > 0))
      {
        file_id = sstable_id.sstable_file_id_;
        if (OB_SUCCESS == get_sstable_path(file_id,path,sizeof(path)))
        {
          unlink(path);
          TBSYS_LOG(WARN,"cleanup sstable %s",path);
        }
      }

      manager_.get_disk_manager().add_used_space(
          static_cast<int32_t>(get_sstable_disk_no(sstable_id.sstable_file_id_)) , 0);
      return OB_SUCCESS;
    }

    int pipe_row(ObTabletManager& manager, ObTabletReader& reader, ObTabletWriter & writer)
    {
      // assume reader & writer already opened.
      int ret = OB_SUCCESS;
      const common::ObRow* row = NULL;
      while (OB_SUCCESS == ret)
      {
        if ( manager.is_stoped() )
        {
          TBSYS_LOG(WARN, "stop in merging");
          ret = OB_CS_MERGE_CANCELED;
        }
        else 
        {
          ret = reader.read_next_row(row);
        }

        if (OB_ITER_END == ret)
        {
          TBSYS_LOG(INFO, "reach end of reader.");
          ret = OB_SUCCESS;
          break;
        }
        else if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "reader error =%d", ret);
        }
        else if (NULL == row)
        {
          TBSYS_LOG(WARN, "reader error of NULL row.");
          ret = OB_ERR_UNEXPECTED;
        }
        else if (OB_SUCCESS != (ret = writer.write_row(*row)))
        {
          TBSYS_LOG(WARN, "write error =%d row=%s.", ret, to_cstring(*row));
        }
      }
      return ret;
    }

    int pipe_buffer(ObTabletManager& manager, ObTabletReader& reader, ObTabletWriter & writer)
    {
      // assume reader & writer already opened.
      int ret = OB_SUCCESS;
      common::ObDataBuffer buffer;
      int64_t total_cost_time_us = 0;
      int64_t sleep_time_us = 0;
      int64_t total_sleep_time_us = 0;

      int64_t total_read_size = 0;
      const int64_t band_limit = THE_CHUNK_SERVER.get_config().migrate_band_limit_per_second;

      int64_t start_time_us = tbsys::CTimeUtil::getTime();
      while (OB_SUCCESS == ret)
      {
        if ( manager.is_stoped() )
        {
          TBSYS_LOG(WARN, "stop in merging");
          ret = OB_CS_MERGE_CANCELED;
        }
        else 
        {
          ret = reader.read_next(buffer);
        }
        if (OB_ITER_END == ret)
        {
          TBSYS_LOG(INFO, "reach end of reader.");
          ret = OB_SUCCESS;
          break;
        }
        else if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "reader error =%d", ret);
        }
        else if (OB_SUCCESS != (ret = writer.write(buffer)))
        {
          TBSYS_LOG(WARN, "write error =%d buffer=%p,%ld", ret, buffer.get_data(), buffer.get_position());
        }
        else
        {
          if(buffer.get_capacity() > 0 && NULL != buffer.get_data() && band_limit > 0) 
          {
            total_read_size += buffer.get_capacity();
            sleep_time_us = 1000000L*total_read_size/band_limit - (tbsys::CTimeUtil::getTime() - start_time_us);

            if (sleep_time_us > 0)
            {
              total_sleep_time_us += sleep_time_us;
              usleep(static_cast<useconds_t>(sleep_time_us));
            }
          }
        }
      }

      total_cost_time_us = tbsys::CTimeUtil::getTime() - start_time_us;

      double read_speed = 0;
      if(total_cost_time_us + total_sleep_time_us > 0)
      {
        read_speed = static_cast<double>(total_read_size * 1000) /static_cast<double>(total_cost_time_us);
      }

      /**
       * the first packet rt is ignored, data is contained by meta negotiation
       * response(called by ObTabletRemoteReader::prepare), so the band limit
       * and read speed is not accurate.
       */
      TBSYS_LOG(INFO, "duration time of pipe buffer:"
          " band_limit[%.2f]kb/s total_cost_time_us[%ld], total_sleep_time_us[%ld] total_read_size:[%.2f]kb read_speed:[%.2f]kb/s",
          static_cast<double>(band_limit)/1024.0, total_cost_time_us, total_sleep_time_us, static_cast<double>(total_read_size)/1024.0, read_speed);
      return ret;
    }


    ObTabletFileWriter::ObTabletFileWriter() : is_empty_(true) , add_used_space_(false), disk_no_(0)
    {
    }

    int ObTabletFileWriter::prepare(const ObNewRange& range, const int64_t tablet_version, const int64_t tablet_seq_num, const uint64_t row_checksum, ObTabletManager* manager)
    {
      range_ = range;
      tablet_version_ = tablet_version;
      manager_ = manager;
      extend_info_.sequence_num_ = tablet_seq_num;
      extend_info_.last_do_expire_version_ = tablet_version;
      extend_info_.row_checksum_ = row_checksum;
      extend_info_.sstable_version_ = static_cast<int16_t>(THE_CHUNK_SERVER.get_config().merge_write_sstable_version);
      return OB_SUCCESS;
    }

    int ObTabletFileWriter::open()
    {
      int ret = OB_SUCCESS;
      char path[OB_MAX_FILE_NAME_LENGTH];
      ObSSTableWriterUtil util(*(manager_));

      disk_no_ = manager_->get_disk_manager().get_dest_disk();
      if (disk_no_ < 0)
      {
        TBSYS_LOG(ERROR,"does't have enough disk space");
        sstable_id_.sstable_file_id_ = 0;
        ret = OB_CS_OUTOF_DISK_SPACE;
      }
      else if (OB_SUCCESS != (ret = util.gen_sstable_file_location(disk_no_, sstable_id_, path, sizeof(path))))
      {
        TBSYS_LOG(WARN, "gen_sstable_file_location disk_no=%d failed.", disk_no_);
      }
      else
      {
        ObString path_string;
        path_string.assign_ptr(path,static_cast<int32_t>(strlen(path) + 1));
        if (OB_SUCCESS != (ret = appender_.open(path_string, true, true, true)))
        {
          TBSYS_LOG(ERROR, "open file path:%s failed, erro:%s"
              , path, strerror(errno));
        }
        else
        {
          TBSYS_LOG(INFO, "prepare the open file, locate path:%s", path);
        }
      }

      return ret;
    }

    int ObTabletFileWriter::cleanup()
    {
      int ret = OB_SUCCESS;
      appender_.close();

      if(!add_used_space_)
      {
        ObSSTableWriterUtil util(*(manager_));
        if(sstable_id_.sstable_file_id_ > 0)
        {
          // writer failed, cleanup create sstable files;
          util.cleanup_uncomplete_sstable_files(sstable_id_);
          add_used_space_ = true;
        }
        else if(disk_no_ > 0)
        {
          manager_->get_disk_manager().add_used_space(disk_no_ , 0); 
        }
      }
      return ret;
    }

    int ObTabletFileWriter::close()
    {
      int ret = OB_SUCCESS;
      appender_.close();

      if(OB_SUCCESS != (ret = finish_sstable()))
      {
        TBSYS_LOG(ERROR, "finish sstable failed ret:%d", ret);
      }

      return ret;
    }

    int ObTabletFileWriter::build_new_tablet(ObTablet* &tablet)
    {
      int ret = OB_SUCCESS;

      if (!is_empty_ && OB_SUCCESS != (ret = tablet->add_sstable_by_id(sstable_id_)) )
      {
        TBSYS_LOG(ERROR, "Merge : add sstable to tablet failed. ret=%d", ret);
      }
      else
      {
        tablet->set_disk_no(static_cast<int32_t>(get_sstable_disk_no(sstable_id_.sstable_file_id_)));
        tablet->set_data_version(tablet_version_);
        tablet->set_extend_info(extend_info_);
      }

      return ret;
    }

    int ObTabletFileWriter::finish_sstable()
    {
      int ret = OB_SUCCESS;
      ObTablet* new_tablet = NULL;
      ObMultiVersionTabletImage& tablet_image = manager_->get_serving_tablet_image();
      bool for_create = tablet_image.get_serving_version() == 0 ? true : false;
      bool sync_meta = THE_CHUNK_SERVER.get_config().each_tablet_sync_meta;

      if (OB_SUCCESS != (ret =
            tablet_image.alloc_tablet_object(range_, tablet_version_, new_tablet)))
      {
        TBSYS_LOG(ERROR,"alloc_tablet_object failed, range=%s, version=%ld, ret=%d",
            to_cstring(range_), tablet_version_, ret);
      }
      else if (OB_SUCCESS != (ret = build_new_tablet(new_tablet)))
      {
        TBSYS_LOG(ERROR,"build_new_tablet failed, range=%s, ret=%d",
            to_cstring(range_), ret);
      }
      else if(OB_SUCCESS != (ret = tablet_image.add_tablet(new_tablet, true, for_create)))
      {
        TBSYS_LOG(INFO, "add tablet failed ret:%d", ret);
      }
      else
      {
        if(sync_meta)
        {
          if (OB_SUCCESS != (ret = tablet_image.write(
                  new_tablet->get_data_version(),
                  new_tablet->get_disk_no())))
          {
            TBSYS_LOG(WARN,"write new meta failed version=%ld, disk_no=%d", 
                new_tablet->get_data_version(), new_tablet->get_disk_no());
          }
        }
      }

      if(is_empty_)
      {
        char path[OB_MAX_FILE_NAME_LENGTH];
        if(OB_SUCCESS != get_sstable_path(sstable_id_, path, sizeof(path)))
        {
          TBSYS_LOG(WARN, "can't get the path of new sstable, ");
        }
        else
        {
          TBSYS_LOG(WARN, "sstable file:%s is empty, will cleanup", path);
        }
      }


      if(OB_SUCCESS == ret && !is_empty_)
      {
        manager_->get_disk_manager().add_used_space(
            static_cast<int32_t>(get_sstable_disk_no(sstable_id_.sstable_file_id_)),
            new_tablet->get_occupy_size());
        add_used_space_ = true;
      }

      return ret;
    }

    int ObTabletFileWriter::write(const common::ObDataBuffer& out_buffer)
    {
      int ret = OB_SUCCESS;
      if(is_empty_ && out_buffer.get_capacity())
      {
        is_empty_ = false;
      }
      if(OB_SUCCESS != (ret = appender_.append(out_buffer.get_data(), out_buffer.get_capacity(), false)))
      {
        TBSYS_LOG(ERROR, "append file erro:%s", strerror(errno));
      }
      else
      {
        OB_STAT_INC(SSTABLE, INDEX_DISK_IO_WRITE_NUM);
        OB_STAT_INC(SSTABLE, INDEX_DISK_IO_WRITE_BYTES, out_buffer.get_capacity());
      }

      return ret;
    }

  }
}
