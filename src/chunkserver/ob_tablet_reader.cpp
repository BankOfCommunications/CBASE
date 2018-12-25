/**
 * (C) 2010-2011 Taobao Inc.
 *
 *  This program is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU General Public License
 *  version 2 as published by the Free Software Foundation.
 *
 *  ob_tablet_reader.cpp
 *
 *  Authors:
 *    zian <yunliang.shi@alipay.com>
 *
 */
#include "ob_tablet_reader.h"
#include "ob_chunk_server_main.h"

namespace oceanbase
{
  namespace chunkserver
  {
    ObTabletRemoteReader::ObTabletRemoteReader(common::ObDataBuffer &out_buffer, ObTabletManager &manager) :
      out_buffer_(out_buffer), manager_(manager), pos_(0),
      buffer_reader_(out_buffer, pos_), has_new_data_(true), session_id_(0)
    {
    }

    int ObTabletRemoteReader::prepare(
        ObDataSourceDesc &desc,
        int16_t &remote_sstable_version,
        int64_t &sequence_num,
        int64_t &row_checksum,
        int64_t &sstable_type)
    {
      int ret = OB_SUCCESS;
      const int32_t CS_FETCH_DATA_VERSION = 1;
      common::ObResultCode  rc;
      int8_t is_fullfilled = 0;
      src_server_ = desc.src_server_;

      if(OB_SUCCESS != (ret = desc.serialize(out_buffer_.get_data(), out_buffer_.get_capacity(), out_buffer_.get_position())))
      {
        TBSYS_LOG(ERROR, "serialize desc failed, %d", ret);
      }
      //mod zhaoqiong [fixed for Backup]:20150811:b
//      else if(OB_SUCCESS != (ret = THE_CHUNK_SERVER.get_client_manager().send_request(src_server_, OB_CS_FETCH_DATA,
//                                                                                      CS_FETCH_DATA_VERSION, THE_CHUNK_SERVER.get_config().datasource_timeout, out_buffer_, session_id_)))
      else if(OB_SUCCESS != (ret = manager_.get_client_manager()->send_request(src_server_, OB_CS_FETCH_DATA,
              CS_FETCH_DATA_VERSION, THE_CHUNK_SERVER.get_config().datasource_timeout, out_buffer_, session_id_)))
      //mod:e
      {
        TBSYS_LOG(WARN, "send request to %s failed, %d ", to_cstring(src_server_), ret);
        ret = OB_DATA_SOURCE_TIMEOUT;
      }
      else if(OB_SUCCESS != (ret = deserialize_result_5(out_buffer_, pos_, rc, remote_sstable_version, sequence_num, row_checksum, sstable_type, is_fullfilled)))
      {
        TBSYS_LOG(WARN, "deserialize response failed, %d", ret);
      }

      if(1 == is_fullfilled)
      {
        has_new_data_ = false;
      }

      return ret;
    }


    int ObTabletRemoteReader::open()
    {
      return buffer_reader_.open();
    }

    int ObTabletRemoteReader::close()
    {
      return buffer_reader_.close();
    }

    int ObTabletRemoteReader::read_next(common::ObDataBuffer& buffer)
    {
      int ret = OB_SUCCESS;
      int8_t is_fullfilled = 0;

      if(!has_new_data_ && !buffer_reader_.has_new_data())
      {
        ret = OB_ITER_END;
      }
      else if(OB_SUCCESS == (ret = buffer_reader_.read_next(buffer)))
      {
      }
      else if(has_new_data_ && OB_ITER_END == ret)
      {
        common::ObResultCode rc;
        out_buffer_.get_position() = 0;
        pos_ = 0;
        buffer_reader_.reuse();
        //mod zhaoqiong [fixed for Backup]:20150811:b
        //if(OB_SUCCESS != (ret = THE_CHUNK_SERVER.get_client_manager().get_next(src_server_, session_id_, THE_CHUNK_SERVER.get_config().datasource_timeout, out_buffer_, out_buffer_)))
        if(OB_SUCCESS != (ret = manager_.get_client_manager()->get_next(src_server_, session_id_, THE_CHUNK_SERVER.get_config().datasource_timeout, out_buffer_, out_buffer_)))
        //mod:e
        {
          TBSYS_LOG(WARN, "send request failed, %d", ret);
          ret = OB_DATA_SOURCE_TIMEOUT;
        }
        else if(OB_SUCCESS != (ret = rc.deserialize(out_buffer_.get_data(), out_buffer_.get_position(), pos_)))
        {
          TBSYS_LOG(ERROR, "finish flag deserialize failed, %d", ret);
        }
        else if(OB_SUCCESS != rc.result_code_)
        {
          ret = rc.result_code_;
          TBSYS_LOG(ERROR, "remote reader failed, ret:%d", ret);
        }
        else if(OB_SUCCESS != (ret = serialization::decode_i8(out_buffer_.get_data(), out_buffer_.get_position(), pos_, &is_fullfilled)))
        {
          TBSYS_LOG(ERROR, "finish flag deserialize failed, %d", ret);
        }
        else
        {
          if(is_fullfilled == 1)
          {
            has_new_data_ = false;
          }

          ret = buffer_reader_.read_next(buffer);
        }
      }
      else if(OB_SUCCESS != ret && OB_ITER_END != ret)
      {
        TBSYS_LOG(WARN, "remote read failed, ret:%d", ret);
      }

      return ret;
    }


    ObTabletBufferReader::ObTabletBufferReader(common::ObDataBuffer &buffer, int64_t &pos) : buffer_(buffer), pos_(pos)
    {
      reset();
    }

    void ObTabletBufferReader::reset()
    {
      has_new_data_ = true;
      has_new_buffer_ = true;
    }

    int ObTabletBufferReader::read_next(common::ObDataBuffer& buffer)
    {
      int ret = OB_SUCCESS;
      if(has_new_buffer_)
      {
        buffer.set_data(buffer_.get_data() + pos_, buffer_.get_position() - pos_);
        pos_ = buffer_.get_position();
        has_new_data_ = false;
        has_new_buffer_ = false;
      }
      else
      {
        ret = OB_ITER_END;
      }

      return ret;
    }


    int ObTabletFileReader::prepare(const ObNewRange& range, const int64_t req_tablet_version, const int16_t remote_sstable_version)
    {
      int ret = OB_SUCCESS;
      ObMultiVersionTabletImage& tablet_image = manager_->get_serving_tablet_image();

      if(OB_SUCCESS != (ret = tablet_image.acquire_tablet(
              range, ObMultiVersionTabletImage::SCAN_FORWARD, req_tablet_version, tablet_)) ||
          (NULL == tablet_ || req_tablet_version != tablet_->get_data_version()))
      {
        int64_t tablet_version = 0;
        if (OB_SUCCESS == ret && NULL != tablet_)
        {
          tablet_version = tablet_->get_data_version();
          ret = OB_CS_TABLET_NOT_EXIST;
        }
        TBSYS_LOG(WARN, "not found the tablet range:%s "
            "req_tablet_version:%ld return_version:%ld, ret:%d",
            to_cstring(range), req_tablet_version, tablet_version, ret);
      }
      else if ( range.compare_with_startkey2(tablet_->get_range()) != 0
          || range.compare_with_endkey2(tablet_->get_range()) != 0)
      {
        TBSYS_LOG(WARN, "input tablet range:<%s> not equal to local range:<%s>", 
            to_cstring(range), to_cstring(tablet_->get_range()));
        ret = OB_CS_TABLET_NOT_EXIST;
      }
      else
      {
        remote_sstable_version_ = remote_sstable_version;
        req_tablet_version_ = req_tablet_version;
      }

      return ret;
    }


    int ObTabletFileReader::open()
    {
      int ret = OB_SUCCESS;
      int16_t local_sstable_version = 0;
      if(NULL == tablet_)
      {
        TBSYS_LOG(WARN, "not found tablet, tablet is NULL");
      }
      else
      {
        if(OB_SUCCESS != (ret != get_sstable_version(local_sstable_version)))
        {
          TBSYS_LOG(ERROR, "get sstable version failed, ret:%d", ret);
        }
        else if(remote_sstable_version_ != local_sstable_version)
        {
          ret = OB_SSTABLE_VERSION_UNEQUAL;
          TBSYS_LOG(ERROR, "sstable version not equal, remote_sstable_version:%d"
              "local_sstable_version:%d ret:%d", remote_sstable_version_, local_sstable_version, ret);
        }
        else if(((tablet_->get_sstable_id_list()).count() <= 0))
        {
          TBSYS_LOG(WARN, "not found sstable file");
        }
        else
        {
          char path[OB_MAX_FILE_NAME_LENGTH];
          sstable::ObSSTableId sstable_id;
          sstable_id = (tablet_->get_sstable_id_list()).at(0).sstable_file_id_;

          if(sstable_id.sstable_file_id_ <= 0)
          {
            TBSYS_LOG(WARN, "sstable_file_id <= 0 not found the file");
          }
          else if(OB_SUCCESS != (ret = get_sstable_path(sstable_id, path, sizeof(path))) )
          {
            TBSYS_LOG(WARN, "can't get the path of new sstable, ");
          }
          else
          {
            ObString fname(0, static_cast<int32_t>(strlen(path)), const_cast<char*>(path));
            if(OB_SUCCESS != (ret = reader_.open(fname)))
            {
              TBSYS_LOG(ERROR, "open %s for read error, %s.", path, strerror(errno));
            }
            else
            {
              TBSYS_LOG(INFO, "sstable version equal, local_sstable_version:%d remote_sstable_version:%d direct file copy, path:%s",
                  local_sstable_version, remote_sstable_version_, path);
            }
          }
        }
      }
      return ret;
    }

    int ObTabletFileReader::get_sstable_type(int64_t &sstable_type)
    {
        sstable_type = 0;
        return OB_SUCCESS;
    }
    
    int ObTabletFileReader::get_row_checksum(int64_t &row_checksum)
    {
      int ret = OB_SUCCESS;
      if(NULL == tablet_)
      {
        TBSYS_LOG(WARN, "not found the tablet");
        ret = OB_ERROR;
      }
      else
      {
        row_checksum = tablet_->get_row_checksum();
      }
      return ret;
    }

    int ObTabletFileReader::get_sstable_version(int16_t& sstable_version)
    {
      int ret = OB_SUCCESS;
      if(NULL == tablet_)
      {
        TBSYS_LOG(WARN, "not found the tablet");
        ret = OB_ERROR;
      }
      else
      {
        sstable_version = tablet_->get_sstable_version();
        if(0 == sstable_version)
        {
          sstable_version = static_cast<int16_t>(THE_CHUNK_SERVER.get_config().merge_write_sstable_version);
        }
      }
      return ret;
    }

    int ObTabletFileReader::get_sequence_num(int64_t& sequence_num)
    {
      int ret = OB_SUCCESS;
      if(NULL == tablet_)
      {
        ret = OB_ERROR;
      }
      else
      {
        sequence_num = tablet_->get_sequence_num();
      }
      return ret;
    }

    void ObTabletFileReader::reset()
    {
      file_offset_ = 0;
      remote_sstable_version_ = 0;
      req_tablet_version_ = 0;
      manager_ = NULL;
      has_new_data_ = true;
      tablet_ = NULL;
    }

    int ObTabletFileReader::close()
    {
      int ret = OB_SUCCESS;
      ObMultiVersionTabletImage& tablet_image = manager_->get_serving_tablet_image();
      if(NULL != tablet_)
      {
        ret = tablet_image.release_tablet(tablet_);
        tablet_ = NULL;
      }
      reader_.close();
      return ret;
    }


    int ObTabletFileReader::read_next(common::ObDataBuffer& buffer)
    {
      return read_file_data(buffer);
    }

    int ObTabletFileReader::read_file_data(common::ObDataBuffer& buffer)
    {
      int ret = OB_SUCCESS;
      int serialize_ret = OB_SUCCESS;
      int64_t read_size = 0;

      int8_t read_result = -1;

      if(reader_.is_opened())
      {
        if(OB_SUCCESS != (ret = reader_.pread(buffer.get_data() + 1 + buffer.get_position(), buffer.get_remain() - 1, file_offset_, read_size)))
        {
          TBSYS_LOG(ERROR, "file read error, %s", strerror(errno));
        }
      }

      if( OB_SUCCESS == ret)
      {
        if(read_size < buffer.get_remain() - 1)
        {
          has_new_data_ = false;
        }
        file_offset_ += read_size;

        //set read result flag
        read_result = has_new_data_ ? 0 : 1;
      }

      if(OB_SUCCESS != (serialize_ret = serialization::encode_i8(buffer.get_data(), 1 + buffer.get_position(), buffer.get_position(), read_result)))
      {
        TBSYS_LOG(ERROR, "read result flag serialize, %d", serialize_ret);
      }

      if(OB_SUCCESS == ret && OB_SUCCESS == serialize_ret)
      {
        buffer.get_position() += read_size;
      }

      if(OB_SUCCESS == ret && read_size > 0)
      {
        OB_STAT_INC(SSTABLE, INDEX_DISK_IO_READ_NUM);
        OB_STAT_INC(SSTABLE, INDEX_DISK_IO_READ_BYTES, read_size);
      }

      TBSYS_LOG(DEBUG, "copy file size:%ld finish:%d ret:%d", read_size, read_result, ret);
      return ret;
    }

  }
}
