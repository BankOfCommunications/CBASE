/**
 * (C) 2010-2011 Taobao Inc.
 *
 *  This program is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU General Public License
 *  version 2 as published by the Free Software Foundation.
 *
 *  ob_proxy_reader.cpp is for
 *
 *  Authors:
 *    zian <yunliang.shi@alipay.com>
 *
 */

#include "ob_proxy_reader.h"
#include "ob_proxy_server_main.h"

namespace oceanbase
{
  namespace proxyserver
  {
    int ProxyReader::prepare_buffer()
    {
      int ret = OB_SUCCESS;
      if(NULL == internal_buf_.get_data())
      {
        int32_t buf_size = static_cast<int32_t>(THE_PROXY_SERVER.get_config().get_yt_buf_size());
        char*  buf = static_cast<char*>(ob_malloc(buf_size, ObModIds::OB_POOL));
        if(NULL == buf)
        {

          TBSYS_LOG(ERROR, "memory exhaust");
          ret = OB_ALLOCATE_MEMORY_FAILED;
        }
        else
        {
          memset(buf, 0, buf_size);
          internal_buf_.set_data(buf, buf_size);
          pos_ = 0;
        }
      }
      reset();
      return ret;
    }

    ProxyReader::~ProxyReader()
    {
      if(NULL != internal_buf_.get_data())
      {
        ob_free(internal_buf_.get_data(), ObModIds::OB_POOL);
      }
    }

    void ProxyReader::reset()
    {
      pos_ = 0;
      internal_buf_.get_position() = 0;
      open_file_time_us_ = 0;
      total_read_size_ = 0;
      total_read_time_us_ = 0;
    }

    int ProxyReader::fill_data(common::ObDataBuffer& buffer)
    {
      int8_t finish_flag = 0;
      int ret = OB_SUCCESS;
      int32_t send_len = static_cast<int32_t>((internal_buf_.get_position() - pos_) < (buffer.get_remain() - 1) ? (internal_buf_.get_position() - pos_) : (buffer.get_remain() - 1)); 
      if(send_len > 0)
      {
        if(pos_ + send_len == internal_buf_.get_position() && internal_buf_.get_remain() > 0)
        {
          finish_flag = 1;
        }

        if(OB_SUCCESS != (ret = serialization::encode_i8(buffer.get_data(),  buffer.get_capacity(), buffer.get_position(), finish_flag)))
        {
          TBSYS_LOG(ERROR, "serialize finish flag failed, ret:%d", ret);
        }

        memcpy(buffer.get_data() + buffer.get_position(), internal_buf_.get_data() + pos_, send_len);

        if(OB_SUCCESS == ret)
        {
          pos_ += send_len;
          buffer.get_position() += send_len;
        }
      }
      else
      {
        TBSYS_LOG(ERROR, "has no remain data or argument invalid");
        ret = OB_DATA_SOURCE_READ_ERROR;
      }
      return ret;
    }

    int YuntiFileReader::get_sstable_version(const char* sstable_dir, int16_t &sstable_version)
    {
      return proxy_->get_sstable_version(sstable_dir, sstable_version);
    }
    int YuntiFileReader::get_tablet_version(int64_t &tablet_version)
    {
      tablet_version = 0;
      return OB_SUCCESS;
    }

    int YuntiFileReader::get_sstable_type(int64_t &sstable_type)
    {
      sstable_type = 0;
      return OB_SUCCESS;
    }

    int YuntiFileReader::prepare(YuntiProxy* proxy, common::ObNewRange &range,
        int64_t req_tablet_version, int16_t remote_sstable_version, const char* sstable_dir)
    {
      int ret = OB_SUCCESS;
      fp_ = NULL;
      int16_t local_sstable_version = 0;
      proxy_ = proxy;
      int64_t start_time_us = tbsys::CTimeUtil::getTime();
      if(NULL == proxy_ || NULL == sstable_dir)
      {
        TBSYS_LOG(ERROR, "argument invalid");
        ret = OB_INVALID_ARGUMENT;
      }
      else if(OB_SUCCESS != (ret = proxy->open_rfile(fp_, range, sstable_dir)))
      {
        TBSYS_LOG(WARN, "yunti has some problem, or path:%s not exist", sstable_dir);
      }
      else if(OB_SUCCESS != (ret = prepare_buffer()))
      {
        TBSYS_LOG(ERROR, "no memory");
      }
      else if(OB_SUCCESS != (ret = get_sstable_version(sstable_dir, local_sstable_version)))
      {
        TBSYS_LOG(ERROR, "get sstable version failed, ret:%d", ret);
      }
      else if(remote_sstable_version != local_sstable_version)
      {
        TBSYS_LOG(ERROR, "version not equal, req_tablet_version:%ld local_sstable_version:%d remote_sstable_version:%d", req_tablet_version, local_sstable_version, remote_sstable_version);
        ret = OB_SSTABLE_VERSION_UNEQUAL;
      }

      open_file_time_us_ = tbsys::CTimeUtil::getTime() - start_time_us;

      return ret;
    }

    int YuntiFileReader::read_next(common::ObDataBuffer& buffer)
    {
      int ret = OB_SUCCESS;
      int64_t read_size = 0;
      if(!has_remain_data())
      {
        internal_buf_.get_position() = 0;
        pos_ = 0;
        int64_t start_time_us = tbsys::CTimeUtil::getTime();
        if(OB_SUCCESS != (ret = proxy_->fetch_data(fp_, internal_buf_.get_data() + internal_buf_.get_position(),
                internal_buf_.get_remain(), read_size)))
        {
          TBSYS_LOG(WARN, "yunti has some problem, read_size:%ld ret:%d", read_size, ret);
          ret = OB_DATA_SOURCE_READ_ERROR;
        }
        else
        {
          if(read_size < 0)
          {
            TBSYS_LOG(WARN, "yunti has some problem, read_size:%ld", read_size);
            ret = OB_DATA_SOURCE_READ_ERROR;
          }
          else
          {
            internal_buf_.get_position() += read_size;
            total_read_size_ += read_size;
            total_read_time_us_ += tbsys::CTimeUtil::getTime() - start_time_us;
          }
        }
      }

      if(OB_SUCCESS == ret)
      {
        ret = fill_data(buffer);
      }
      return ret;
    }

    int YuntiFileReader::close()
    {
      int ret = OB_SUCCESS;
      if(fp_)
      {
        ret = proxy_->close_rfile(fp_);
      }

      double read_speed = 0;
      if(total_read_size_ > 0 && total_read_time_us_ + open_file_time_us_ > 0)
      {
        read_speed = static_cast<double>(total_read_size_ * 1000)/static_cast<double>(total_read_time_us_ + open_file_time_us_);
      }

      TBSYS_LOG(INFO, "duration of read file: open_file_time_us[%ld] total_read_size[%ld] total_read_time_us[%ld] read_speed[%.2f]kb/s", open_file_time_us_, total_read_size_, total_read_time_us_, read_speed);

      return ret;
    }

    void YuntiRangeReader::reset()
    {
      range_list_.clear();
      offset_ = 0;
    }


    int YuntiRangeReader::prepare(YuntiProxy* proxy, const char* table_name, const char* sstable_dir)
    {
      return proxy->fetch_range_list(table_name, sstable_dir, range_list_);
    }

    bool YuntiRangeReader::has_new_data()
    {
      bool ret = true;
      if(offset_ == range_list_.count())
      {
        ret = false;
      }
      return ret;
    }

    int YuntiRangeReader::read_next(common::ObDataBuffer& buffer)
    {
      int64_t count = 0;
      int ret = OB_SUCCESS;
      bool has_more = true;
      common::ObNewRange* range = NULL;
      int64_t pos = 0;
#ifndef RANGE_META
#define RANGE_META 9
#endif
      if(buffer.get_remain() < RANGE_META)
      {
        TBSYS_LOG(ERROR, "invalid argument");
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        pos = buffer.get_position() + RANGE_META;
        //9 bytes for has_more and count
        for(; offset_ < range_list_.count(); offset_++)
        {
          range = range_list_.at(offset_);
          if(range->get_serialize_size() > buffer.get_capacity() - pos - RANGE_META)
          {
            break;
          }
          else if(OB_SUCCESS != (ret = range->serialize(buffer.get_data(), buffer.get_capacity(),  pos)))
          {
            TBSYS_LOG(ERROR, "serialize failed, ret:%d", ret);
            break;
          }
          else
          {
            TBSYS_LOG(INFO, "range:%s", to_cstring(*range));
            count++;
          }
        }
      }

      if(OB_SUCCESS == ret)
      {
        if(offset_ == range_list_.count())
        {
          has_more = false;
        }

        if(OB_SUCCESS != (ret = serialization::encode_bool(buffer.get_data(), buffer.get_capacity(), buffer.get_position(), has_more)))
        {
          TBSYS_LOG(ERROR, "serialize failed, ret:%d", ret);
        }
        else if(OB_SUCCESS != (ret = serialization::encode_i64(buffer.get_data(), buffer.get_capacity(), buffer.get_position(), count)))
        {
          TBSYS_LOG(ERROR, "serialize failed, ret:%d", ret);
        }
        else
        {
          buffer.get_position() += pos - RANGE_META;
        }
      }
      TBSYS_LOG(INFO, "range, count:%ld", count);

      return ret;
    }
  }
}
