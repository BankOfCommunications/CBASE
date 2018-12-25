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
 *     Author Name <email address>
 *        - some work details if you want
 */

#include "ob_tablet.h"
#include "common/ob_crc64.h"
#include "common/utility.h"
#include "sstable/ob_sstable_reader.h"
#include "ob_tablet_image.h"

using namespace oceanbase::common;
using namespace oceanbase::sstable;
using namespace oceanbase::compactsstable;

namespace oceanbase 
{ 
  namespace chunkserver 
  {
    //----------------------------------------
    // struct ObTabletRangeInfo
    //----------------------------------------
    DEFINE_SERIALIZE(ObTabletRangeInfo)
    {
      int ret = OB_SUCCESS;
      int64_t serialize_size = get_serialize_size();

      if((NULL == buf) || (serialize_size + pos > buf_len)) 
      {
        ret = OB_ERROR;
      }
      
      if ( OB_SUCCESS == ret
          && OB_SUCCESS == serialization::encode_i16(buf, buf_len, pos, start_key_size_)
          && OB_SUCCESS == serialization::encode_i16(buf, buf_len, pos, end_key_size_)
          && OB_SUCCESS == serialization::encode_i8 (buf, buf_len, pos, is_removed_)
          && OB_SUCCESS == serialization::encode_i8 (buf, buf_len, pos, is_merged_)
          && OB_SUCCESS == serialization::encode_i8 (buf, buf_len, pos, is_with_next_brother_)
          && OB_SUCCESS == serialization::encode_i8(buf, buf_len, pos, border_flag_)
          && OB_SUCCESS == serialization::encode_i64(buf, buf_len, pos, table_id_))
      {
        ret = OB_SUCCESS;
      }
      else
      {
        ret = OB_ERROR;
      }

      return ret;
    }

    DEFINE_DESERIALIZE(ObTabletRangeInfo)
    {
      int ret = OB_SUCCESS;
      int64_t serialize_size = get_serialize_size();

      if (NULL == buf || serialize_size > data_len) 
      {
        ret = OB_ERROR;
      }

      if (OB_SUCCESS == ret 
          && OB_SUCCESS == serialization::decode_i16(buf, data_len, pos, &start_key_size_)
          && OB_SUCCESS == serialization::decode_i16(buf, data_len, pos, &end_key_size_)
          && OB_SUCCESS == serialization::decode_i8 (buf, data_len, pos, &is_removed_)
          && OB_SUCCESS == serialization::decode_i8 (buf, data_len, pos, &is_merged_)
          && OB_SUCCESS == serialization::decode_i8 (buf, data_len, pos, &is_with_next_brother_)
          && OB_SUCCESS == serialization::decode_i8 (buf, data_len, pos, &border_flag_)
          && OB_SUCCESS == serialization::decode_i64(buf, data_len, pos, &table_id_))
      {
        ret = OB_SUCCESS;
      }
      else
      {
        ret = OB_ERROR;
      }

      return ret;
    }

    DEFINE_GET_SERIALIZE_SIZE(ObTabletRangeInfo)
    {
      int64_t total_size = 0;
      
      total_size += serialization::encoded_length_i16(start_key_size_);
      total_size += serialization::encoded_length_i16(end_key_size_);
      total_size += serialization::encoded_length_i8(is_removed_);
      total_size += serialization::encoded_length_i8(is_merged_);
      total_size += serialization::encoded_length_i8(is_with_next_brother_);
      total_size += serialization::encoded_length_i8(border_flag_);
      total_size += serialization::encoded_length_i64(table_id_);

      return total_size;
    }

    //----------------------------------------
    // struct ObTabletExtendInfo 
    //----------------------------------------
    DEFINE_SERIALIZE(ObTabletExtendInfo)
    {
      int ret = OB_SUCCESS;
      int64_t serialize_size = get_serialize_size();

      if((NULL == buf) || (serialize_size + pos > buf_len)) 
      {
        ret = OB_ERROR;
      }
      
      if ( OB_SUCCESS == ret
          && OB_SUCCESS == serialization::encode_i64(buf, buf_len, pos, row_count_)
          && OB_SUCCESS == serialization::encode_i64(buf, buf_len, pos, occupy_size_)
          && OB_SUCCESS == serialization::encode_i64(buf, buf_len, pos, check_sum_)
          && OB_SUCCESS == serialization::encode_i64(buf, buf_len, pos, last_do_expire_version_)
          && OB_SUCCESS == serialization::encode_i64(buf, buf_len, pos, sequence_num_) 
          && OB_SUCCESS == serialization::encode_i16(buf, buf_len, pos, sstable_version_) 
          && OB_SUCCESS == serialization::encode_i16(buf, buf_len, pos, reserved16_) 
          && OB_SUCCESS == serialization::encode_i32(buf, buf_len, pos, reserved32_) 
          && OB_SUCCESS == serialization::encode_i64(buf, buf_len, pos, row_checksum_)
          && OB_SUCCESS == serialization::encode_i64(buf, buf_len, pos, reserved64_) )
      {
      }
      else
      {
        ret = OB_ERROR;
      }

      return ret;
    }

    DEFINE_DESERIALIZE(ObTabletExtendInfo)
    {
      int ret = OB_SUCCESS;
      int64_t serialize_size = get_serialize_size();

      if (NULL == buf || serialize_size > data_len) 
      {
        ret = OB_ERROR;
      }

      if (OB_SUCCESS == ret 
          && OB_SUCCESS == serialization::decode_i64(buf, data_len, pos, &row_count_)
          && OB_SUCCESS == serialization::decode_i64(buf, data_len, pos, &occupy_size_)
          && OB_SUCCESS == serialization::decode_i64(buf, data_len, pos, reinterpret_cast<int64_t*>(&check_sum_))
          && OB_SUCCESS == serialization::decode_i64(buf, data_len, pos, &last_do_expire_version_)
          && OB_SUCCESS == serialization::decode_i64(buf, data_len, pos, &sequence_num_)
          && OB_SUCCESS == serialization::decode_i16(buf, data_len, pos, &sstable_version_)
          && OB_SUCCESS == serialization::decode_i16(buf, data_len, pos, &reserved16_)
          && OB_SUCCESS == serialization::decode_i32(buf, data_len, pos, &reserved32_)
          && OB_SUCCESS == serialization::decode_i64(buf, data_len, pos, reinterpret_cast<int64_t*>(&row_checksum_))
          && OB_SUCCESS == serialization::decode_i64(buf, data_len, pos, &reserved64_)
          )
      {
      }
      else
      {
        ret = OB_ERROR;
      }

      return ret;
    }

    DEFINE_GET_SERIALIZE_SIZE(ObTabletExtendInfo)
    {
      int64_t total_size = 0;
      
      total_size += serialization::encoded_length_i64(row_count_);
      total_size += serialization::encoded_length_i64(occupy_size_);
      total_size += serialization::encoded_length_i64(check_sum_);
      total_size += serialization::encoded_length_i64(last_do_expire_version_);
      total_size += serialization::encoded_length_i64(sequence_num_);
      total_size += serialization::encoded_length_i16(sstable_version_);
      total_size += serialization::encoded_length_i16(reserved16_);
      total_size += serialization::encoded_length_i32(reserved32_);
      total_size += serialization::encoded_length_i64(row_checksum_);
      total_size += serialization::encoded_length_i64(reserved64_);

      return total_size;
    }



    //----------------------------------------
    // class ObTablet
    //----------------------------------------
    ObTablet::ObTablet(ObTabletImage* image) : 
      sstable_id_list_(TABLET_ARRAY_BLOCK_SIZE, image->mod_),
      sstable_reader_list_(TABLET_ARRAY_BLOCK_SIZE, image->mod_)
    {
      reset();
      image_ = image;
    }

    ObTablet::~ObTablet()
    {
      destroy();
    }

    void ObTablet::destroy()
    {
      // ObTabletImage will free reader's memory on destory.
      if (0 != ref_count_)
      {
        TBSYS_LOG(ERROR, "try to destroy tablet, but ref count is not 0, "
                         "ref_count_=%ld",
          ref_count_);
      }
      int64_t reader_count = sstable_reader_list_.count();
      for (int64_t i = 0 ; i < reader_count; ++i)
      {
        SSTableReader* reader = sstable_reader_list_.at(i);
        reader->~SSTableReader();
      }
      ObCompactSSTableMemNode* tmp = compact_header_;
      while(compact_header_ != NULL)
      {
        tmp = compact_header_->next_;
        delete compact_header_;
        compact_header_ = tmp;
      }
      compact_header_ = NULL;
      compact_tail_   = NULL;
      reset();
    }

    void ObTablet::reset()
    {
      sstable_loaded_ = OB_NOT_INIT;
      merged_ = 0;
      removed_ = 0;
      merge_count_ = 0;
      disk_no_ = 0;
      data_version_ = 0;
      compactsstable_num_ = 0;
      compactsstable_loading_ = 0;
      compact_header_ = NULL;
      compact_tail_ = NULL;
      ref_count_ = 0;
      image_ = NULL;
      memset(&extend_info_, 0, sizeof(extend_info_));
      sstable_id_list_.clear();
      sstable_reader_list_.clear();
    }

    int ObTablet::add_sstable_by_id(const ObSSTableId& sstable_id)
    {
      int ret = OB_SUCCESS;
      if (OB_SUCCESS == sstable_loaded_)
      {
        TBSYS_LOG(ERROR, "sstable already loaded, cannot add.");
        ret = OB_INIT_TWICE;
      }
      // check disk no?
      else if (0 != disk_no_ && disk_no_ != static_cast<int32_t>(get_sstable_disk_no(sstable_id.sstable_file_id_)))
      {
        TBSYS_LOG(ERROR, "add file :%ld not in same disk:%d",
            sstable_id.sstable_file_id_, disk_no_);
        ret = OB_ERROR;
      }
      else if (OB_SUCCESS != (ret = sstable_id_list_.push_back(sstable_id)))
      {
        TBSYS_LOG(ERROR, "add sstable id :%ld in list error ret=%d",
            sstable_id.sstable_file_id_, ret);
      }

      return ret;
    }

    //add wenghaixing [secondary index static_index_build]20150307
    int ObTablet::add_local_index_sstable_by_id(const ObSSTableId &sstable_id)
    {
      int ret = OB_SUCCESS;
      if (0 != disk_no_ && disk_no_ != static_cast<int32_t>(get_sstable_disk_no(sstable_id.sstable_file_id_)))
      {
        TBSYS_LOG(ERROR, "add file :%ld not in same disk:%d",
            sstable_id.sstable_file_id_, disk_no_);
        ret = OB_ERROR;
      }
      else if(MAX_SSTABLE_PER_TABLET <= sstable_id_list_.count())
      {
        TBSYS_LOG(ERROR,"there is already [%ld] sstable in tablet!",sstable_id_list_.count());
      }
      else if (OB_SUCCESS != (ret = sstable_id_list_.push_back(sstable_id)))
      {
        TBSYS_LOG(ERROR, "add sstable id :%ld in list error ret=%d",
            sstable_id.sstable_file_id_, ret);
      }
      return ret;
    }
    //add e

    //add liuxiao [secondary index static_index_build]20150307
    /*
     * 用于删除局部索引sstable，操作简单，主要是将对应位置sstableid和信息删除
     * 不直接删除sstable1文件，删除操作有下一次合并时自动回收到recycle
     */
    int ObTablet::delete_local_index_sstableid()
    {
      int ret = OB_SUCCESS;
      if(this->sstable_id_list_.count() < 2)
      {
        TBSYS_LOG(DEBUG, "no local index sstable file need to delete");
      }
      else if(OB_SUCCESS != (ret = sstable_id_list_.remove(1)))
      {
        TBSYS_LOG(WARN, "delete local index sstable failed");
      }

      if(this->sstable_reader_list_.count() < 2)
      {
        TBSYS_LOG(DEBUG, "no local index reader need to delete");
      }
      else
      {
        sstable_reader_list_.at(1)->reset();
        ret = (sstable_reader_list_.remove(1));
      }

      return ret;
    }

    //add e

    int ObTablet::set_range_by_info(const ObTabletRangeInfo& info,
        char* row_key_stream_ptr, const int64_t row_key_stream_size)
    {
      int ret = OB_SUCCESS;
      if (info.start_key_size_ + info.end_key_size_ > row_key_stream_size)
      {
        ret = OB_SIZE_OVERFLOW;
      }
      else
      {
        range_.start_key_.deserialize_from_stream(row_key_stream_ptr, info.start_key_size_);
        range_.end_key_.deserialize_from_stream(row_key_stream_ptr + info.start_key_size_, 
            info.end_key_size_);
        range_.table_id_ = info.table_id_;
        range_.border_flag_.set_data(static_cast<int8_t>(info.border_flag_));
        merged_ = info.is_merged_;
        removed_ = info.is_removed_;
        with_next_brother_ = info.is_with_next_brother_;
        if (range_.border_flag_.is_min_value())
        {
          range_.start_key_.set_min_row();
        }
        if (range_.border_flag_.is_max_value())
        {
          range_.end_key_.set_max_row();
        }
      }
      return ret;
    }

    void ObTablet::get_range_info(ObTabletRangeInfo& info) const
    {
      info.start_key_size_ = static_cast<int16_t>(range_.start_key_.get_serialize_objs_size());
      info.end_key_size_ = static_cast<int16_t>(range_.end_key_.get_serialize_objs_size());
      info.table_id_ = range_.table_id_;
      info.border_flag_ = range_.border_flag_.get_data();
      info.is_merged_ = static_cast<int8_t>(merged_);
      info.is_removed_ = static_cast<int8_t>(removed_);
      info.is_with_next_brother_ = static_cast<int8_t>(with_next_brother_);
    }

    DEFINE_SERIALIZE(ObTablet)
    {
      int ret = OB_ERROR;
      ret = serialization::encode_vi64(buf, buf_len, pos, data_version_);

      int64_t size = sstable_id_list_.count();
      if (OB_SUCCESS == ret)
      {
        ret = serialization::encode_vi64(buf, buf_len, pos, size);
      }

      if (OB_SUCCESS == ret)
      {
        for (int64_t i = 0; i < size; ++i)
        {
          const ObSSTableId & sstable_id = sstable_id_list_.at(i);
          ret = sstable_id.serialize(buf, buf_len, pos);
          if (OB_SUCCESS != ret)
            break;
        }
      }

      return ret;
    }

    DEFINE_DESERIALIZE(ObTablet)
    {
      int ret = OB_ERROR;

      ret = serialization::decode_vi64(buf, data_len, pos, &data_version_);

      int64_t size = 0;
      if (OB_SUCCESS == ret)
      {
        ret = serialization::decode_vi64(buf, data_len, pos, &size);
      }

      if (OB_SUCCESS == ret && size > 0)
      {
        for (int64_t i = 0; i < size; ++i)
        {
          ObSSTableId sstable_id;
          ret = sstable_id.deserialize(buf, data_len, pos);
          if (ret != OB_SUCCESS)
            break;

          sstable_id_list_.push_back(sstable_id);
        }
      }

      return ret;
    }

    DEFINE_GET_SERIALIZE_SIZE(ObTablet)
    {
      int64_t total_size = 0;
      total_size += serialization::encoded_length_vi64(data_version_);

      int64_t size = sstable_id_list_.count();
      total_size += serialization::encoded_length_vi64(size);

      if (size > 0)
      {
        for (int64_t i = 0; i < size; ++i)
          total_size += sstable_id_list_.at(i).get_serialize_size();
      }

      return total_size;
    }

    int ObTablet::find_sstable(const ObSSTableId &sstable_id,
        ObSSTableReader* &reader) const
    {
      int ret = OB_SUCCESS;
      if (OB_SUCCESS != sstable_loaded_)
        ret = (sstable_loaded_ = const_cast<ObTablet*>(this)->load_sstable()); 

      int64_t i = 0;
      int64_t size = sstable_reader_list_.count();
      if (OB_SUCCESS == ret)
      {
        for (; i < size; ++i)
        {
          reader = dynamic_cast<ObSSTableReader*>(sstable_reader_list_.at(i));
          if (reader->get_sstable_id() == sstable_id)
            break;
        }

        if (i >= size) 
        {
          ret = OB_ENTRY_NOT_EXIST;
          reader = NULL;
        }
      }

      if (OB_SUCCESS == ret && NULL != reader && reader->empty())
      {
        reader = NULL;
      }

      return ret;
    }

    /*
     * @return OB_SUCCESS if sstable_id exists in tablet
     * or OB_CS
     */
    int ObTablet::include_sstable(const ObSSTableId& sstable_id) const
    {
      int ret = OB_SUCCESS;

      int64_t i = 0;
      int64_t size = sstable_id_list_.count();
      if (OB_SUCCESS == ret)
      {
        for (; i < size; ++i)
        {
          if (sstable_id_list_.at(i) == sstable_id)
            break;
        }

        if (i >= size) 
        {
          ret = OB_ENTRY_NOT_EXIST;
        }
      }

      return ret;
    }
    //add wenghaixing [secondary index staic_index_build.cs_scan]20150330
    int ObTablet::load_local_sstable(const int64_t tablet_version)
    {
      int ret = OB_SUCCESS;
      sstable::SSTableReader* reader = NULL;

      load_sstable_mutex_.lock();

      if (NULL == image_)
      {
        ret = OB_ERROR;
      }
      else
      {
        int64_t size = sstable_id_list_.count();
        int64_t reader_size = sstable_reader_list_.count();
        for (int64_t i = reader_size; i < size ; ++i)
        {
          if (get_sstable_version() < SSTableReader::COMPACT_SSTABLE_VERSION)
          {
            reader = image_->alloc_sstable_object();
          }
          else
          {
            reader = image_->alloc_compact_sstable_object();
          }

          if (NULL == reader)
          {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            break;
          }
          else if ( OB_SUCCESS != (ret = reader->open(sstable_id_list_.at(i).sstable_file_id_, tablet_version)))
          {
            TBSYS_LOG(ERROR, "read sstable failed, sstable id=%ld, ret =%d",
                sstable_id_list_.at(i).sstable_file_id_, ret);
            break;
          }
          else if ( OB_SUCCESS != (ret = sstable_reader_list_.push_back(reader)))
          {
            TBSYS_LOG(ERROR, "add sstable reader in list error ret=%d", ret);
            break;
          }
          else if (0 == i && OB_INVALID_ID == range_.table_id_)
          {
            //if the range of tablet is not set, use the first sstable's range
            range_ = reader->get_range();

          }
        }
      }
      sstable_loaded_ = ret;

      load_sstable_mutex_.unlock();

      return ret;
    }
    //add e
    int ObTablet::load_sstable(const int64_t tablet_version)
    {
      int ret = OB_SUCCESS;
      sstable::SSTableReader* reader = NULL;

      load_sstable_mutex_.lock();

      if (NULL == image_)
      {
        ret = OB_ERROR;
      }
      else if (OB_SUCCESS == sstable_loaded_)
      {
        ret = OB_SUCCESS;
      }
      else
      {
        int64_t size = sstable_id_list_.count();
        int64_t reader_size = sstable_reader_list_.count();
        for (int64_t i = 0; i < size && reader_size < size; ++i)
        {
          /*
          //we can't put sstable reader with same sstable id into sstable_reader_list_ twice
          if (i < reader_size 
              && sstable_id_list_.at(i) == (sstable_reader_list_.at(i))->get_sstable_id()) 
          {
            //this sstable is opened, just skip it
            continue;
          }
          else if (reader_size > 0 && i < reader_size 
              && !(sstable_id_list_.at(i) == (sstable_reader_list_.at(i))->get_sstable_id())) 
          {
            //we must ensure the order in sstable_id_list_ is the same as sstable_reader_list_
            TBSYS_LOG(WARN, "the order in sstable_id_list_ isn't the same as "
                "sstable_reader_list_, sstable_id=%ld, reader_sstable_id=%ld", 
                sstable_id_list_.at(i).sstable_file_id_, 
                (sstable_reader_list_.at(i))->get_sstable_id().sstable_file_id_);
            ret = OB_ERROR;
            break;
          }
          */

          if (get_sstable_version() < SSTableReader::COMPACT_SSTABLE_VERSION)
          {
            reader = image_->alloc_sstable_object();
          }
          else
          {
            reader = image_->alloc_compact_sstable_object();
          }

          if (NULL == reader)
          {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            break;
          }
          else if ( OB_SUCCESS != (ret = reader->open(sstable_id_list_.at(i).sstable_file_id_, tablet_version)))
          {
            TBSYS_LOG(ERROR, "read sstable failed, sstable id=%ld, ret =%d", 
                sstable_id_list_.at(i).sstable_file_id_, ret);
            break;
          }
          else if ( OB_SUCCESS != (ret = sstable_reader_list_.push_back(reader)))
          {
            TBSYS_LOG(ERROR, "add sstable reader in list error ret=%d", ret);
            break;
          }
          else if (0 == i && OB_INVALID_ID == range_.table_id_)
          {
            //if the range of tablet is not set, use the first sstable's range
            range_ = reader->get_range();
          }
        }
      }
      sstable_loaded_ = ret;

      load_sstable_mutex_.unlock();

      return ret;
    }

    int64_t ObTablet::get_max_sstable_file_seq() const
    {
      int64_t size = sstable_id_list_.count();
      int64_t max_sstable_file_seq = 0;
      for (int64_t i = 0; i < size; ++i)
      {
        int64_t cur_file_seq = sstable_id_list_.at(i).sstable_file_id_ >> 8;
        if (cur_file_seq > max_sstable_file_seq)
          max_sstable_file_seq = cur_file_seq;
      }
      return max_sstable_file_seq;
    }

    /*
     * get row count of all sstables in this tablet
     */
    int64_t ObTablet::get_row_count() const
    {
      int64_t row_count = 0;
      const_cast<tbsys::CThreadMutex&>(extend_info_mutex_).lock();
      if (extend_info_.row_count_ == 0 && sstable_id_list_.count() > 0)
      {
        const_cast<ObTablet*>(this)->calc_extend_info();
      }
      row_count = extend_info_.row_count_;
      const_cast<tbsys::CThreadMutex&>(extend_info_mutex_).unlock();
      return row_count;
    }

    /*
     * get approximate occupy size of all sstables in this tablet
     */
    int64_t ObTablet::get_occupy_size() const
    {
      int64_t occupy_size = 0;
      const_cast<tbsys::CThreadMutex&>(extend_info_mutex_).lock();
      if (extend_info_.occupy_size_ == 0 && sstable_id_list_.count() > 0)
      {
        const_cast<ObTablet*>(this)->calc_extend_info();
      }
      occupy_size = extend_info_.occupy_size_;
      const_cast<tbsys::CThreadMutex&>(extend_info_mutex_).unlock();
      return occupy_size;
    }

    int64_t ObTablet::get_checksum() const
    {
      int64_t check_sum = 0;
      const_cast<tbsys::CThreadMutex&>(extend_info_mutex_).lock();
      if (extend_info_.check_sum_ == 0 && sstable_id_list_.count() > 0)
      {
        const_cast<ObTablet*>(this)->calc_extend_info();
      }
      check_sum = extend_info_.check_sum_;
      const_cast<tbsys::CThreadMutex&>(extend_info_mutex_).unlock();
      return check_sum;
    }

    const ObTabletExtendInfo& ObTablet::get_extend_info() const
    {
      const_cast<tbsys::CThreadMutex&>(extend_info_mutex_).lock();
      if (extend_info_.occupy_size_ == 0 && sstable_id_list_.count() > 0)
      {
        const_cast<ObTablet*>(this)->calc_extend_info();
      }
      const_cast<tbsys::CThreadMutex&>(extend_info_mutex_).unlock();
      return extend_info_;
    }

    int ObTablet::calc_extend_info()
    {
      // calc extend info by sstable reader.
      int ret = OB_SUCCESS;
      int64_t pos = 0;
      int64_t checksum_len = sizeof(uint64_t);
      int64_t sstable_checksum = 0;
      char checksum_buf[checksum_len];

      if (OB_SUCCESS != sstable_loaded_)
        ret = (sstable_loaded_ = load_sstable()); 

      if (OB_SUCCESS == ret)
      {
        extend_info_.row_count_ = 0;
        extend_info_.occupy_size_ = 0;
        extend_info_.check_sum_ = 0;
        int64_t size = sstable_reader_list_.count();
        for (int64_t i = 0; i < size && OB_SUCCESS == ret; ++i)
        {
          SSTableReader* reader = sstable_reader_list_.at(i);
          if (NULL != reader) 
          {
            extend_info_.row_count_ += reader->get_row_count();
            extend_info_.occupy_size_ += reader->get_sstable_size();

            sstable_checksum = reader->get_sstable_checksum();
            pos = 0;
            ret = serialization::encode_i64(checksum_buf, 
                checksum_len, pos, sstable_checksum);
            if (OB_SUCCESS == ret)
            {
              extend_info_.check_sum_ = ob_crc64(
                  extend_info_.check_sum_, checksum_buf, checksum_len);
            }
          }
          else
          {
            TBSYS_LOG(ERROR, "calc extend_info error, sstable %ld,%ld reader = NULL",
                i, sstable_id_list_.at(i).sstable_file_id_);
            ret = OB_ERROR;
          }
        }
      }

      return ret;
    }

    int64_t ObTablet::get_cache_data_version() const
    {
      int64_t data_version = get_data_version();
      if (compactsstable_num_ > 0 && compact_tail_ != NULL)
      {
        data_version = compact_tail_->mem_.get_data_version();
      }
      return data_version;
    }

    const ObFrozenVersionRange* ObTablet::get_cache_version_range() const
    {
      const ObFrozenVersionRange* ret = NULL;
      if (compactsstable_num_ > 0 && compact_tail_ != NULL)
      {
        ret =  &(compact_tail_->mem_.get_version_range());
      }
      return ret;
    }

    int ObTablet::add_compactsstable(ObCompactSSTableMemNode* cache)
    {
      int ret = OB_SUCCESS;
      if (NULL == cache)
      {
        TBSYS_LOG(WARN,"invalid argument");
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        if (0 == compactsstable_num_)
        {
          compact_tail_ = compact_header_ = cache;
        }
        else
        {
          compact_tail_->next_ = cache;
          compact_tail_        = cache;
        }
        ++compactsstable_num_;
      }
      return ret;
    }

    ObCompactSSTableMemNode* ObTablet::get_compactsstable_list()
    {
      return compact_header_;
    }

    /** 
     * atomic compare and set compactsstable loading flag
     * 
     * @return true on if there is no other threads hold this tablet and set flag,
     *         false on there is another thread that have already hold this tablet
     */
    bool ObTablet::compare_and_set_compactsstable_loading()
    {
      bool ret = false;
      if (0 == atomic_compare_exchange(&compactsstable_loading_,1,0))
      {
        ret = true;
      }
      return ret;
    }

    void ObTablet::clear_compactsstable_flag()
    {
      atomic_dec(&compactsstable_loading_);
    }

    int ObTablet::dump(const bool dump_sstable) const
    {
      char range_buf[OB_RANGE_STR_BUFSIZ];
      range_.to_string(range_buf, OB_RANGE_STR_BUFSIZ);
      TBSYS_LOG(INFO, "range=%s, data version=%ld, disk_no=%d, merged=%d", 
          range_buf, data_version_, disk_no_, merged_);
      if (dump_sstable && OB_SUCCESS == sstable_loaded_)
      {
        //do nothing
        int64_t size = sstable_reader_list_.count();
        for (int64_t i = 0; i < size; ++i)
        {
          const SSTableReader* reader = sstable_reader_list_.at(i);
          if (NULL != reader) 
          {
            TBSYS_LOG(INFO, "sstable [%ld]: id=%ld, "
                "size = %ld, row count =%ld, checksum=%ld", 
                i, sstable_id_list_.at(i).sstable_file_id_, 
                reader->get_sstable_size(),
                reader->get_row_count(),
                reader->get_sstable_checksum()) ;
          }
        }
      }
      else
      {
        int64_t size = sstable_id_list_.count();
        for (int64_t i = 0; i < size; ++i)
        {
          TBSYS_LOG(INFO, "sstable [%ld]: id=%ld", 
              i, sstable_id_list_.at(i).sstable_file_id_) ;
        }
      }
      return OB_SUCCESS;
    }

    void ObTablet::set_merged(int status) 
    { 
      merged_ = status; 
    }

  } // end namespace chunkserver
} // end namespace oceanbase



