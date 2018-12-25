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

#include "ob_tablet_image.h"
#include <dirent.h>
#include "common/ob_record_header.h"
#include "common/file_directory_utils.h"
#include "common/ob_file.h"
#include "common/ob_mod_define.h"
#include "sstable/ob_sstable_block_index_v2.h"
#include "ob_tablet.h"
#include "ob_disk_manager.h"
#include "ob_fileinfo_cache.h"

using namespace oceanbase::common;
using namespace oceanbase::common::serialization;
using namespace oceanbase::sstable;


namespace oceanbase
{
  namespace chunkserver
  {

    //----------------------------------------
    // struct ObTabletMetaHeader
    //----------------------------------------
    DEFINE_SERIALIZE(ObTabletMetaHeader)
    {
      int ret = OB_SUCCESS;
      int64_t serialize_size = get_serialize_size();

      if((NULL == buf) || (serialize_size + pos > buf_len))
      {
        ret = OB_INVALID_ARGUMENT;
      }

      if (OB_SUCCESS == ret
          && (OB_SUCCESS == encode_i64(buf, buf_len, pos, tablet_count_))
          && (OB_SUCCESS == encode_i64(buf, buf_len, pos, data_version_))
          && (OB_SUCCESS == encode_i32(buf, buf_len, pos, row_key_stream_offset_))
          && (OB_SUCCESS == encode_i32(buf, buf_len, pos, row_key_stream_size_))
          && (OB_SUCCESS == encode_i32(buf, buf_len, pos, tablet_extend_info_offset_))
          && (OB_SUCCESS == encode_i32(buf, buf_len, pos, tablet_extend_info_size_)))
      {
        ret = OB_SUCCESS;
      }
      else
      {
        ret = OB_SERIALIZE_ERROR;
      }

      return ret;
    }

    DEFINE_DESERIALIZE(ObTabletMetaHeader)
    {
      int ret = OB_SUCCESS;
      int64_t serialize_size = get_serialize_size();

      if (NULL == buf || serialize_size > data_len)
      {
        ret = OB_INVALID_ARGUMENT;
      }

      if (OB_SUCCESS == ret
          && (OB_SUCCESS == decode_i64(buf, data_len, pos, &tablet_count_))
          && (OB_SUCCESS == decode_i64(buf, data_len, pos, &data_version_))
          && (OB_SUCCESS == decode_i32(buf, data_len, pos, &row_key_stream_offset_))
          && (OB_SUCCESS == decode_i32(buf, data_len, pos, &row_key_stream_size_))
          && (OB_SUCCESS == decode_i32(buf, data_len, pos, &tablet_extend_info_offset_))
          && (OB_SUCCESS == decode_i32(buf, data_len, pos, &tablet_extend_info_size_)))
      {
        ret = OB_SUCCESS;
      }
      else
      {
        ret = OB_DESERIALIZE_ERROR;
      }

      return ret;
    }

    DEFINE_GET_SERIALIZE_SIZE(ObTabletMetaHeader)
    {
      return (encoded_length_i64(tablet_count_)
          + encoded_length_i64(data_version_)
          + encoded_length_i32(row_key_stream_offset_)
          + encoded_length_i32(row_key_stream_size_)
          + encoded_length_i32(tablet_extend_info_offset_)
          + encoded_length_i32(tablet_extend_info_size_));
    }

    //----------------------------------------
    // struct ObTabletRowkeyBuilder
    //----------------------------------------
    ObTabletBuilder::ObTabletBuilder()
      :  buf_(NULL), buf_size_(DEFAULT_BUILDER_BUF_SIZE), data_size_(0)
    {
    }

    ObTabletBuilder::~ObTabletBuilder()
    {
      if (NULL != buf_)
      {
        ob_free(buf_);
      }

      buf_ = NULL;
      buf_size_ = 0;
      data_size_ = 0;
    }

    int ObTabletBuilder::ensure_space(const int64_t size)
    {
      int ret = OB_SUCCESS;
      int64_t new_buf_size = 0;
      char* new_buf = NULL;

      if (NULL == buf_ || (NULL != buf_ && size > buf_size_ - data_size_))
      {
        if (NULL == buf_)
        {
          new_buf_size = size > buf_size_ ? size : buf_size_;
        }
        else
        {
          if (buf_size_ * 2 - data_size_ >= size)
          {
            new_buf_size = buf_size_ * 2;
          }
          else
          {
            new_buf_size = buf_size_ + size;
          }
        }
        new_buf = static_cast<char*>(ob_malloc(new_buf_size, ObModIds::OB_CS_TABLET_IMAGE));
        if (NULL == new_buf)
        {
          TBSYS_LOG(ERROR, "failed to alloc memory, new_buffer=%p", new_buf);
          ret = OB_ALLOCATE_MEMORY_FAILED;
        }
        else
        {
          if (NULL != buf_ && size > buf_size_ - data_size_)
          {
            memcpy(new_buf, buf_, data_size_);
            ob_free(buf_);
            buf_ = NULL;
          }
          buf_ = new_buf;
          buf_size_ = new_buf_size;
        }
      }

      return ret;
    }

    ObTabletRowkeyBuilder::ObTabletRowkeyBuilder()
    {
    }

    ObTabletRowkeyBuilder::~ObTabletRowkeyBuilder()
    {
    }

    int ObTabletRowkeyBuilder::append_range(const common::ObNewRange& range)
    {
      int ret = OB_SUCCESS;
      int64_t row_key_size =
        range.start_key_.get_serialize_objs_size() + range.end_key_.get_serialize_objs_size();

      if (OB_SUCCESS != (ret = ensure_space(row_key_size)))
      {
        TBSYS_LOG(ERROR, "rowkey size =%ld large than buf size(%ld,%ld)",
            row_key_size, data_size_, buf_size_);
      }
      else if ( OB_SUCCESS != (ret = range.start_key_.serialize_objs(
              buf_, buf_size_, data_size_)))
      {
      }
      else if ( OB_SUCCESS != (ret = range.end_key_.serialize_objs(
              buf_, buf_size_, data_size_)))
      {
      }
      return ret;
    }

    ObTabletExtendBuilder::ObTabletExtendBuilder()
    {
    }

    ObTabletExtendBuilder::~ObTabletExtendBuilder()
    {
    }

    int ObTabletExtendBuilder::append_tablet(const ObTabletExtendInfo& info)
    {
      int ret = OB_SUCCESS;

      if (OB_SUCCESS != (ret = ensure_space(info.get_serialize_size())))
      {
        TBSYS_LOG(ERROR, "tablet extend size =%ld large than buf size(%ld,%ld)",
            info.get_serialize_size(), data_size_, buf_size_);
      }
      else if (OB_SUCCESS != (ret = info.serialize(buf_, buf_size_, data_size_)))
      {
        TBSYS_LOG(ERROR, "serialize extend info error, buf size(%ld,%ld)",
            data_size_, buf_size_);
      }
      return ret;
    }

    //----------------------------------------
    // class ObTabletImage
    //----------------------------------------
    ObTabletImage::ObTabletImage()
      : tablet_list_(DEFAULT_TABLET_NUM), sstable_list_(DEFAULT_TABLET_NUM),
      delete_table_tablet_list_(DEFAULT_TABLE_TABLET_NUM),
      report_tablet_list_(DEFAULT_TABLET_NUM),
      hash_map_inited_(false), data_version_(0),
      max_sstable_file_seq_(0),
      ref_count_(0), cur_iter_idx_(INVALID_ITER_INDEX),
      merged_tablet_count_(0),
      mod_(ObModIds::OB_CS_TABLET_IMAGE),
      allocator_(ModuleArena::DEFAULT_PAGE_SIZE, mod_),
      fileinfo_cache_(NULL),
      disk_manager_(NULL)
    {
      // TODO reserve vector size.
    }

    ObTabletImage::~ObTabletImage()
    {
      destroy();
    }

    int ObTabletImage::destroy()
    {
      int ret = OB_SUCCESS;
      if (ref_count_ != 0)
      {
        TBSYS_LOG(ERROR, "ObTabletImage still been used ref=%ld, "
                         "cannot destory..", ref_count_);
        /**
         * FIXME: sometime the ref count is not zero when doing destroy,
         * it's a bug, but we review the code again and again, we don't
         * find the reason. if the tablet image can't be destroyed,
         * daily merge can't start. so now we just ignore the error and
         * destroy the tablet image. there is a risk that someone is
         * using the tablet image and the tablet image destroyed. after
         * switch to new tablet image, we only allow destroy the old
         * tablet image after several minutes. so the risk is very
         * small.
         */
        ret = OB_CS_EAGAIN;
      }
      if (OB_SUCCESS == ret)
      {
        int64_t tablet_count = tablet_list_.size();
        for (int32_t i = 0 ; i < tablet_count; ++i)
        {
          ObTablet* tablet = tablet_list_.at(i);
          if (NULL != tablet) tablet->~ObTablet();
        }
      }
      if (OB_SUCCESS == ret)
      {
        ret = reset();
      }
      return ret;
    }

    int ObTabletImage::reset()
    {
      allocator_.free();
      tablet_list_.clear();
      sstable_list_.clear();
      delete_table_tablet_list_.clear();
      report_tablet_list_.clear();
      if (hash_map_inited_)
      {
        sstable_to_tablet_map_.clear();
      }

      data_version_ = 0;
      max_sstable_file_seq_ = 0;
      ref_count_ = 0;
      cur_iter_idx_ = INVALID_ITER_INDEX;
      merged_tablet_count_ = 0;

      return OB_SUCCESS;
    }

    int ObTabletImage::find_tablet(const ObSSTableId& sstable_id,
      ObTablet* &tablet) const
    {
      int ret = OB_SUCCESS;
      int hash_ret = 0;
      tablet = NULL;

      if (hash_map_inited_)
      {
        hash_ret = sstable_to_tablet_map_.get(sstable_id, tablet);
        if (hash::HASH_EXIST == hash_ret && NULL != tablet)
        {
          //do nothing
        }
        else if (-1 == hash_ret)
        {
          TBSYS_LOG(WARN, "failed to find sstable_id=%lu in hash map, hash_ret=%d",
            sstable_id.sstable_file_id_, hash_ret);
          ret = OB_ERROR;
        }
        else
        {
          ret = OB_ENTRY_NOT_EXIST;
        }
      }
      else
      {
        ObSortedVector<ObTablet*>::iterator it = tablet_list_.begin();
        for (; it != tablet_list_.end(); ++it)
        {
          if (OB_SUCCESS == (*it)->include_sstable(sstable_id))
          {
            break;
          }
        }

        if (it != tablet_list_.end())
        {
          tablet = *it;
        }
        else
        {
          ret = OB_ENTRY_NOT_EXIST;
        }
      }
      return ret;
    }

    int ObTabletImage::include_sstable(const ObSSTableId& sstable_id) const
    {
      int ret = OB_ERROR;
      int hash_ret = 0;

      if (hash_map_inited_)
      {
        ObTablet* tablet = NULL;
        hash_ret = sstable_to_tablet_map_.get(sstable_id, tablet);
        if (hash::HASH_EXIST == hash_ret && NULL != tablet)
        {
          ret = OB_SUCCESS;
        }
        else if (-1 == hash_ret)
        {
          TBSYS_LOG(WARN, "failed to find sstable_id=%lu in hash map, hash_ret=%d",
            sstable_id.sstable_file_id_, hash_ret);
          ret = OB_ERROR;
        }
        else
        {
          ret = OB_ENTRY_NOT_EXIST;
        }
      }
      else
      {
        ObSortedVector<ObTablet*>::iterator it = tablet_list_.begin();
        for (; it != tablet_list_.end(); ++it)
        {
          if (OB_SUCCESS == (*it)->include_sstable(sstable_id))
          {
            ret = OB_SUCCESS;
            break;
          }
        }

        if (it == tablet_list_.end())
        {
          ret = OB_ENTRY_NOT_EXIST;
        }
      }
      return ret;
    }

    int ObTabletImage::acquire_tablets_on_disk(
      const int32_t disk_no, ObVector<ObTablet*>& tablet_list)
    {
      int ret = OB_SUCCESS;

      if (0 > disk_no)
      {
        TBSYS_LOG(WARN, "tablet image acquire_tablets, invalid disk_no=%d",
          disk_no);
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        ObSortedVector<ObTablet*>::iterator it = tablet_list_.begin();
        for (; it != tablet_list_.end(); ++it)
        {
          if ((*it)->get_disk_no() == disk_no)
          {
            acquire();
            (*it)->inc_ref();
            if (OB_SUCCESS != (ret = tablet_list.push_back(*it)))
            {
              TBSYS_LOG(WARN, "failed to push back tablet into "
                              "tablet vector, range=%s",
                to_cstring((*it)->get_range()));
              break;
            }
          }
        }
      }

      return ret;
    }

    int ObTabletImage::acquire_tablets(
      const uint64_t table_id, ObVector<ObTablet*>& table_tablets)
    {
      int ret = OB_SUCCESS;

      if (OB_INVALID_ID == table_id)
      {
        TBSYS_LOG(WARN, "tablet image acquire_tablets, invalid table_id=%lu",
          table_id);
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        ObSortedVector<ObTablet*>::iterator it = tablet_list_.begin();
        for (; it != tablet_list_.end(); ++it)
        {
          if (0 == table_id || (*it)->get_range().table_id_ == table_id)
          {
            acquire();
            (*it)->inc_ref();
            if (OB_SUCCESS != (ret = table_tablets.push_back(*it)))
            {
              TBSYS_LOG(WARN, "failed to push back tablet into "
                              "tablet vector, range=%s",
                to_cstring((*it)->get_range()));
              break;
            }
          }
        }
      }

      return ret;
    }

    int ObTabletImage::release_tablets(const ObVector<ObTablet*>& tablet_list)
    {
      int ret = OB_SUCCESS;
      bool is_remove_sstable = false;
      ObVector<ObTablet*>::iterator it = tablet_list.begin();
      ObTablet* tablet = NULL;

      for (; it != tablet_list.end(); ++it)
      {
        tablet = *it;
        if (OB_SUCCESS != (ret = release_tablet(tablet, &is_remove_sstable)))
        {
          TBSYS_LOG(WARN, "failed to release tablet, range=%s",
            to_cstring(tablet->get_range()));
        }

        if (OB_SUCCESS == ret && is_remove_sstable)
        {
          remove_sstable(tablet);
          tablet->~ObTablet();
        }
      }

      return ret;
    }

    int ObTabletImage::remove_table_tablets(const uint64_t table_id)
    {
      int ret = OB_SUCCESS;
      int hash_ret = 0;
      int64_t sstable_count = 0;
      uint64_t prev_table_id = OB_INVALID_ID;
      uint64_t cur_table_id = OB_INVALID_ID;

      if (OB_INVALID_ID == table_id)
      {
        TBSYS_LOG(WARN, "tablet image delete table, invalid table_id=%lu", table_id);
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        ObSortedVector<ObTablet*>::iterator start = tablet_list_.end();
        ObSortedVector<ObTablet*>::iterator end = tablet_list_.end();
        ObSortedVector<ObTablet*>::iterator it = tablet_list_.begin();
        for (; it != tablet_list_.end(); ++it)
        {
          cur_table_id = (*it)->get_range().table_id_;
          if (cur_table_id == table_id)
          {
            (*it)->set_merged();
            (*it)->set_removed();
            if (hash_map_inited_)
            {
              sstable_count = (*it)->get_sstable_id_list().count();
              for (int64_t i = 0; i < sstable_count; ++i)
              {
                hash_ret = sstable_to_tablet_map_.erase((*it)->get_sstable_id_list().at(i));
                if (hash::HASH_EXIST != hash_ret)
                {
                  TBSYS_LOG(WARN, "failed to erase (sstabl_id, tablet) pair frome hash map, "
                                  "sstable_id=%lu, tablet=%p, hash_ret=%d",
                    (*it)->get_sstable_id_list().at(i).sstable_file_id_, (*it), hash_ret);
                }
              }
            }
          }

          if (prev_table_id != cur_table_id && cur_table_id == table_id)
          {
            start = it;
          }

          if (prev_table_id == table_id && cur_table_id != table_id)
          {
            end = it;
            break;
          }
          prev_table_id = cur_table_id;
        }

        if (OB_SUCCESS == ret && end - start > 0)
        {
          ret = tablet_list_.remove(start, end);
        }
      }

      return ret;
    }

    int ObTabletImage::delete_table(const uint64_t table_id, tbsys::CRWLock& lock)
    {
      int ret = OB_SUCCESS;
      delete_table_tablet_list_.reset();

      //acquire table tablets
      {
        tbsys::CRLockGuard guard(lock);
        if (OB_INVALID_ID == table_id)
        {
          TBSYS_LOG(WARN, "tablet image delete table, invalid table_id=%lu", table_id);
          ret = OB_INVALID_ARGUMENT;
        }
        else if (NULL == disk_manager_)
        {
          TBSYS_LOG(WARN, "disk_manager member of tablet image isn't set, it's NULL");
          ret = OB_INVALID_ARGUMENT;
        }
        else if (OB_SUCCESS != (ret = acquire_tablets(table_id, delete_table_tablet_list_)))
        {
          TBSYS_LOG(WARN, "failed to acquier table tablets from tablet image, table_id=%lu",
            table_id);
        }
      }

      //remove table tablets from tablet image
      if (OB_SUCCESS == ret && delete_table_tablet_list_.size() > 0)
      {
        tbsys::CWLockGuard guard(lock);
        if (OB_SUCCESS != (ret = remove_table_tablets(table_id)))
        {
          TBSYS_LOG(WARN, "failed to remove table tablets from table image, table_id=%lu", table_id);
        }
      }

      //sync tablet image
      if (OB_SUCCESS == ret && delete_table_tablet_list_.size() > 0)
      {
        char idx_path[OB_MAX_FILE_NAME_LENGTH];
        int32_t disk_num = 0;
        const int32_t* disk_no_array = disk_manager_->get_disk_no_array(disk_num);

        if (disk_num > 0 && NULL != disk_no_array)
        {
          /**
           * ignore the write fail status, we just ensure flush meta file
           * of each disk once
           */
          for (int32_t i = 0; i < disk_num; ++i)
          {
            ret = get_meta_path(data_version_, disk_no_array[i], true,
              idx_path, OB_MAX_FILE_NAME_LENGTH);
            if (OB_SUCCESS == ret)
            {
              {
                tbsys::CRLockGuard guard(lock);
                ret = prepare_write_meta(disk_no_array[i]);
              }
              if (OB_SUCCESS == ret)
              {
                ret = write_meta(idx_path, disk_no_array[i]);
              }
            }
            else
            {
              TBSYS_LOG(WARN, "failed to sync tablet image, disk_no=%d", disk_no_array[i]);
            }
          }
        }
        else
        {
          TBSYS_LOG(WARN, "sync tablet images invalid argument, "
              "disk_no_array=%p is NULL or size=%d < 0", disk_no_array, disk_num);
        ret = OB_INVALID_ARGUMENT;
      }
      }

      //release table tablets and delete them
      release_tablets(delete_table_tablet_list_);

      return ret;
    }

    //add wenghaixing [secondary index static_index_build]20150303
    const int ObTabletImage::acquire_tablets_pub(const uint64_t table_id, common::ObVector<ObTablet *> &table_tablets) const
    {
      int ret = OB_SUCCESS;
      if (OB_INVALID_ID == table_id)
      {
        TBSYS_LOG(WARN, "tablet image acquire_tablets, invalid table_id=%lu",
          table_id);
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        ObSortedVector<ObTablet*>::iterator it = tablet_list_.begin();
        for (; it != tablet_list_.end(); ++it)
        {
          if (0 == table_id || (*it)->get_range().table_id_ == table_id)
          {
            acquire();
            (*it)->inc_ref();
            if (OB_SUCCESS != (ret = table_tablets.push_back(*it)))
            {
              TBSYS_LOG(WARN, "failed to push back tablet into "
                            "tablet vector, range=%s",
              to_cstring((*it)->get_range()));
              break;
            }
          }
        }
      }
      return ret;
    }
    //add e

    //add liuxiao [secondary index static_index_build]
    /*
     *删除局部索引的sstable，遍历所有的tablet，逐个检查并删除
     */
    const int ObTabletImage::delete_local_index_sstable() const
    {
      int ret = OB_SUCCESS;
      ObSortedVector<ObTablet*>::iterator it = tablet_list_.begin();
      for (; it != tablet_list_.end(); ++it)
      {
        if(OB_SUCCESS != (ret = (*it)->delete_local_index_sstableid()))
        {
          TBSYS_LOG(WARN, "failed to delete local index sstable file_id:%ld", (*it)->get_sstable_id_list().at(1).sstable_file_id_);
        }
      }
      return ret;
    }
    //add e

    void ObTabletImage::set_fileinfo_cache(common::IFileInfoMgr& fileinfo_cache)
    {
      fileinfo_cache_ = &fileinfo_cache;
    }

    void ObTabletImage::set_disk_manger(ObDiskManager* disk_manager)
    {
      disk_manager_ = disk_manager;
    }

    ObSSTableReader* ObTabletImage::alloc_sstable_object()
    {
      alloc_mutex_.lock();
      ObSSTableReader* reader = NULL;
      char* ptr = allocator_.alloc_aligned(sizeof(ObSSTableReader));
      if (ptr && NULL != fileinfo_cache_)
      {
        reader = new (ptr) ObSSTableReader(allocator_, *fileinfo_cache_, &alloc_mutex_);
      }
      alloc_mutex_.unlock();
      return reader;
    }

    compactsstablev2::ObCompactSSTableReader* ObTabletImage::alloc_compact_sstable_object()
    {
      alloc_mutex_.lock();
      compactsstablev2::ObCompactSSTableReader* reader = NULL;
      char* ptr = allocator_.alloc_aligned(sizeof(compactsstablev2::ObCompactSSTableReader));
      if (ptr && NULL != fileinfo_cache_)
      {
        reader = new (ptr) compactsstablev2::ObCompactSSTableReader(*fileinfo_cache_);
      }
      alloc_mutex_.unlock();
      return reader;
    }

    int ObTabletImage::alloc_tablet_object(const ObNewRange& range, ObTablet* &tablet)
    {
      tablet = NULL;
      ObNewRange copy_range;
      alloc_mutex_.lock();

      int ret = OB_SUCCESS;
      if ( OB_SUCCESS != (ret = deep_copy_range(allocator_, range, copy_range)) )
      {
        TBSYS_LOG(ERROR, "copy range failed.");
      }
      alloc_mutex_.unlock();

      if (OB_SUCCESS == ret)
      {
        if ( NULL == (tablet = alloc_tablet_object()) )
        {
          TBSYS_LOG(ERROR, "allocate tablet object failed.");
          ret = OB_ALLOCATE_MEMORY_FAILED;
        }
        else
        {
          tablet->set_range(copy_range);
        }
      }

      return ret;
    }

    ObTablet* ObTabletImage::alloc_tablet_object()
    {
      ObTablet* tablet = NULL;
      alloc_mutex_.lock();
      char* ptr = allocator_.alloc_aligned(sizeof(ObTablet));
      int64_t max_rowkey_obj_arr_len = sizeof(ObObj) * OB_MAX_ROWKEY_COLUMN_NUMBER ;
      if (NULL != ptr)
      {
        tablet = new (ptr) ObTablet(this);
        ptr = allocator_.alloc_aligned(max_rowkey_obj_arr_len * 2);
        if (NULL != ptr)
        {
          tablet->range_.start_key_.assign(reinterpret_cast<ObObj*>(ptr), OB_MAX_ROWKEY_COLUMN_NUMBER);
          tablet->range_.end_key_.assign(reinterpret_cast<ObObj*>(ptr + max_rowkey_obj_arr_len), OB_MAX_ROWKEY_COLUMN_NUMBER);
        }
      }
      alloc_mutex_.unlock();
      return tablet;
    }

    bool compare_tablet(const ObTablet* lhs, const ObTablet* rhs)
    {
      return lhs->get_range().compare_with_endkey(rhs->get_range()) < 0;
    }

    bool compare_tablet_range(const ObTablet* lhs, const ObNewRange& rhs)
    {
      return lhs->get_range().compare_with_endkey(rhs) < 0;
    }

    bool equal_tablet_range(const ObTablet* lhs, const ObNewRange& rhs)
    {
      return lhs->get_range().equal(rhs);
    }

    bool unique_tablet(const ObTablet* lhs, const ObTablet* rhs)
    {
      return lhs->get_range().intersect(rhs->get_range());
    }

    int ObTabletImage::add_tablet(ObTablet* tablet)
    {
      int ret = OB_SUCCESS;
      int hash_ret = 0;
      int64_t sstable_count = 0;
      ObSortedVector<ObTablet*>::iterator it = tablet_list_.end();

      if (!hash_map_inited_)
      {
        ret = sstable_to_tablet_map_.create(DEFAULT_TABLET_NUM);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "failed to create hash map for sstable id "
                          "to tablet map ret=%d", ret);
        }
        else
        {
          hash_map_inited_ = true;
        }
      }
      if (OB_SUCCESS == ret)
      {
        ret = tablet_list_.insert_unique(tablet, it, compare_tablet, unique_tablet);
      }
      if (OB_SUCCESS != ret)
      {
        char intersect_buf[OB_RANGE_STR_BUFSIZ];
        char input_buf[OB_RANGE_STR_BUFSIZ];
        tablet->get_range().to_string(intersect_buf, OB_RANGE_STR_BUFSIZ);
        if (it != tablet_list_.end())
          (*it)->get_range().to_string(input_buf, OB_RANGE_STR_BUFSIZ);
        TBSYS_LOG(WARN, "cannot insert this tablet:%s, maybe intersect with exist tablet:%s",
            intersect_buf, input_buf);
        // TODO fix bug when image corrupt
        if (tablet->get_range().equal((*it)->get_range()))
        {
          tablet->set_merged();
          tablet->set_removed();
          ret = OB_SUCCESS; // added by rizhao.ych
        }
      }
      else
      {
        // only after insert tablet into tablet list success, insert hash map
        sstable_count = tablet->get_sstable_id_list().count();
        for (int64_t i = 0; i < sstable_count; ++i)
        {
          hash_ret = sstable_to_tablet_map_.set(tablet->get_sstable_id_list().at(i), tablet);
          if (hash::HASH_INSERT_SUCC != hash_ret)
          {
            TBSYS_LOG(WARN, "failed to insert (sstabl_id, tablet) pair into hash map, "
                            "sstable_id=%lu, tablet=%p, hash_ret=%d",
              tablet->get_sstable_id_list().at(i).sstable_file_id_, tablet, hash_ret);
            ret = OB_ERROR;
          }
        }
      }
      return ret;
    }

    bool check_border_inclusive(const ObRowkey& key, const ObBorderFlag& border_flag,
        const int32_t scan_direction, const ObTablet& tablet)
    {
      // %tablet.range.end_key_ must >= %key
      // just care about end_key_ == %key, like below:
      //            start_key forward search
      //                     |------------
      //            end_key backward search
      //         ------------|
      // |--| |--| |---------| |---------|
      bool ret = true;
      int  cmp = key.compare(tablet.get_range().end_key_);
      if (cmp < 0)
      {
        ret = true;
      }
      else if (cmp == 0)
      {
        if (ObMultiVersionTabletImage::SCAN_FORWARD == scan_direction)
        {
          ret = (tablet.get_range().border_flag_.inclusive_end()
              && border_flag.inclusive_start());
        }
        else
        {
          ret = tablet.get_range().border_flag_.inclusive_end()
              || (!border_flag.inclusive_end());
        }
      }
      return ret;
    }

    int ObTabletImage::find_tablet(const common::ObNewRange& range,
        const int32_t scan_direction, ObTablet* &tablet) const
    {
      int ret = OB_SUCCESS;
      tablet = NULL;
      ObRowkey lookup_key = (ObMultiVersionTabletImage::SCAN_FORWARD == scan_direction)
        ? range.start_key_ : range.end_key_;

      if (tablet_list_.size() <= 0)
      {
        TBSYS_LOG(WARN, "chunkserver has no tablets, cannot find tablet.");
        ret = OB_CS_TABLET_NOT_EXIST;
      }
      else if (range.empty() || NULL == lookup_key.ptr() || 0 >= lookup_key.length())
      {
        TBSYS_LOG(WARN, "find invalid range:%s", to_cstring(range));
        ret = OB_INVALID_ARGUMENT;
      }
      else if (OB_SUCCESS != (ret = find_tablet(range.table_id_,
              lookup_key, range.get_rowkey_info(), range.border_flag_, scan_direction, tablet)))
      {
        TBSYS_LOG(WARN, "cannot find range :%s", to_cstring(range));
      }
      else if (NULL != tablet && !range.intersect(tablet->get_range()))
      {
        // in the hole?
        ret = OB_CS_TABLET_NOT_EXIST;
        tablet = NULL;
      }

      return ret;
    }

    int ObTabletImage::find_tablet( const uint64_t table_id,
        const common::ObRowkey& key, const ObRowkeyInfo* ri,
        const ObBorderFlag& border_flag,
        const int32_t scan_direction, ObTablet* &tablet) const
    {
      int ret = OB_SUCCESS;

      tablet = NULL;
      ObSortedVector<ObTablet*>::iterator it = tablet_list_.end();

      ObNewRange range;
      range.table_id_ = table_id;
      range.border_flag_ = border_flag;
      range.end_key_ = key;
      range.set_rowkey_info(ri);

      it = tablet_list_.lower_bound(range, compare_tablet_range);
      if (it == tablet_list_.end())
      {
        //                       start_key
        //                        |--------------
        // |--| |--| |---------|

        //                       end_key
        //              ----------|
        // |--| |--| |---------|

        if (ObMultiVersionTabletImage::SCAN_BACKWARD == scan_direction)
        {
          // check table id  in intersect range.
          tablet = *tablet_list_.last();
        }
      }
      else if (NULL != *it)
      {
        //            start_key forward search
        //                |------------
        //            end_key backward search
        //    ------------|
        // |--| |--| |---------| |---------|
        tablet = *it;
        if(tablet->get_range().table_id_ != table_id)
        {
          ret = OB_CS_TABLET_NOT_EXIST;
          tablet = NULL;
        }
        else
        {
          ObSortedVector<ObTablet*>::const_iterator next_it = ++it;
          if (!check_border_inclusive(key, border_flag, scan_direction, *tablet))
          {
            tablet =(next_it != tablet_list_.end()) ? (*next_it) : NULL;
          }
        }
      }

      // can not get any tablet satisfied.
      if (NULL == tablet)
      {
        ret = OB_CS_TABLET_NOT_EXIST;
      }

      return ret;
    }

    int ObTabletImage::acquire_tablet(const ObSSTableId& sstable_id,
        ObTablet* &tablet) const
    {
      int ret = find_tablet(sstable_id, tablet);

      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(INFO, "find sstable sstable_id=%ld failed, ret=%d",
            sstable_id.sstable_file_id_, ret);
      }
      else if (NULL == tablet)
      {
        TBSYS_LOG(INFO, "reader is NULL");
        ret = OB_ERROR;
      }
      else
      {
        acquire();
        tablet->inc_ref();
      }

      return ret;
    }

    int ObTabletImage::remove_sstable(ObTablet* tablet) const
    {
      int ret = OB_SUCCESS;
      int64_t sstable_size = 0;
      FileInfoCache* fileinfo_cache = dynamic_cast<FileInfoCache*>(fileinfo_cache_);
      ObSSTableId sstable_id;
      char sstable_path[OB_MAX_FILE_NAME_LENGTH];

      if (NULL == tablet || NULL == fileinfo_cache)
      {
        TBSYS_LOG(WARN, "invalid param, tablet is NULL, tablet=%p, fileinfo_cache=%p",
          tablet, fileinfo_cache);
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        for (int64_t i = 0; i < tablet->get_sstable_id_list().count() && OB_SUCCESS == ret; ++i)
        {
          sstable_id = (tablet->get_sstable_id_list().at(i));
          // destroy file info cache if exist.
          const FileInfo * fileinfo =
            fileinfo_cache->get_cache_fileinfo(sstable_id.sstable_file_id_);
          if (NULL != fileinfo)
          {
            const_cast<FileInfo*>(fileinfo)->destroy();
            fileinfo_cache->revert_fileinfo(fileinfo);
          }

          if (OB_SUCCESS != (ret = get_sstable_path(sstable_id,
            sstable_path, OB_MAX_FILE_NAME_LENGTH)))
          {
            TBSYS_LOG(WARN, "failed to get sstable path, sstable_id=%lu",
              sstable_id.sstable_file_id_);
          }
          else if (!FileDirectoryUtils::exists(sstable_path))
          {
            TBSYS_LOG(INFO, "sstable file=%s not exist.", sstable_path);
            ret = OB_ERROR;
          }
          else if ((sstable_size = get_file_size(sstable_path)) <= 0)
          {
            TBSYS_LOG(WARN, "sstable file=%s not exist or invalid, sstable_size=%ld",
                  sstable_path, sstable_size);
          }
          else if (0 != ::unlink(sstable_path))
          {
            TBSYS_LOG(WARN, "failed to unlink sstable, sstable_file=%s, "
                            "errno=%d, err=%s",
              sstable_path, errno, strerror(errno));
            ret = OB_IO_ERROR;
          }
          else if (NULL != disk_manager_)
          {
            disk_manager_->release_space(
              static_cast<int32_t>(get_sstable_disk_no(sstable_id.sstable_file_id_)), sstable_size);
            TBSYS_LOG(INFO, "recycle tablet version = %ld, recycle sstable file = %s",
              tablet->get_data_version(), sstable_path);
          }
          sstable_path[0] = '\0';
        }

      }

      return ret;
    }

    int ObTabletImage::remove_tablet(const common::ObNewRange& range, int32_t &disk_no)
    {
      int ret = OB_SUCCESS;
      int hash_ret = 0;
      disk_no = -1;
      if (range.empty())
      {
        ret = OB_INVALID_ARGUMENT;
      }

      if (OB_SUCCESS == ret)
      {
        ObTablet* tablet = NULL;
        ret = tablet_list_.remove_if(range, compare_tablet_range, equal_tablet_range, tablet);
        if (OB_SUCCESS == ret && NULL != tablet)
        {
          if (hash_map_inited_)
          {
            int64_t sstable_count = tablet->get_sstable_id_list().count();
            for (int64_t i = 0; i < sstable_count; ++i)
            {
              hash_ret = sstable_to_tablet_map_.erase(tablet->get_sstable_id_list().at(i));
              if (hash::HASH_EXIST != hash_ret)
              {
                TBSYS_LOG(WARN, "failed to erase (sstabl_id, tablet) pair frome hash map, "
                                "sstable_id=%lu, tablet=%p, hash_ret=%d",
                  tablet->get_sstable_id_list().at(i).sstable_file_id_, tablet, hash_ret);
              }
            }
          }
          disk_no = tablet->get_disk_no();
        }
        else
        {
          char range_buf[OB_RANGE_STR_BUFSIZ];
          range.to_string(range_buf, OB_RANGE_STR_BUFSIZ);
          TBSYS_LOG(WARN, "remove_tablet : the specific range:%s not exist, ret=%d", range_buf, ret);
        }
      }
      return ret;
    }

    int ObTabletImage::acquire_tablet(const common::ObNewRange& range,
        const int32_t scan_direction, ObTablet* &tablet) const
    {
      int ret =  find_tablet(range, scan_direction, tablet);

      if (OB_SUCCESS != ret)
      {
        // do nothing.
      }
      else if (NULL == tablet)
      {
        char range_buf[OB_RANGE_STR_BUFSIZ];
        range.to_string(range_buf, OB_RANGE_STR_BUFSIZ);
        TBSYS_LOG(ERROR, "found tablet:%s null, ret=%d", range_buf, ret);
        ret = OB_ERROR;
      }
      else
      {
        acquire();
        tablet->inc_ref();
      }

      return ret;
    }

    int ObTabletImage::release_tablet(ObTablet* tablet, bool* is_remove_sstable) const
    {
      int ret = OB_SUCCESS;

      if (NULL == tablet)
      {
        TBSYS_LOG(WARN, "invalid param, tablet=%p", tablet);
        ret = OB_INVALID_ARGUMENT;
      }
      else if (ref_count_ <= 0)
      {
        TBSYS_LOG(WARN, "invalid status, ref_count_=%ld", ref_count_);
        ret = OB_ERROR;
      }
      else
      {
        release();
        if (0 == tablet->dec_ref() && tablet->is_removed() && NULL != is_remove_sstable)
        {
          *is_remove_sstable = true;
        }
        else if (NULL != is_remove_sstable)
        {
          *is_remove_sstable = false;
        }
      }

      return ret;
    }

    int ObTabletImage::serialize(const int32_t disk_no,
        char* buf, const int64_t buf_len, int64_t& pos,
        ObTabletRowkeyBuilder& builder, ObTabletExtendBuilder& extend)
    {
      int ret = OB_SUCCESS;
      int64_t origin_pos = pos;

      // serialize record record_header;
      ObRecordHeader record_header;
      ObTabletMetaHeader meta_header;

      record_header.set_magic_num(ObTabletMetaHeader::TABLET_META_MAGIC);
      record_header.header_length_ = OB_RECORD_HEADER_LENGTH;
      record_header.version_ = 0;
      record_header.reserved_ = 0;

      int64_t payload_pos = pos + OB_RECORD_HEADER_LENGTH;
      int64_t tablet_pos = payload_pos + meta_header.get_serialize_size();

      int64_t tablet_count = 0;
      ObSortedVector<ObTablet*>::iterator it = tablet_list_.begin();
      for (; it != tablet_list_.end(); ++it)
      {
        if (((*it)->get_disk_no() == disk_no || 0 == disk_no)
            && !(*it)->is_removed())
        {
          ObTabletRangeInfo info;
          (*it)->get_range_info(info);

          ret = info.serialize(buf, buf_len, tablet_pos);
          if (OB_SUCCESS != ret) break;

          ret = (*it)->serialize(buf, buf_len, tablet_pos);
          if (OB_SUCCESS != ret) break;

          ret = builder.append_range((*it)->get_range());
          if (OB_SUCCESS != ret) break;

          ret = extend.append_tablet((*it)->get_extend_info());
          if (OB_SUCCESS != ret) break;

          ++tablet_count;
        }
      }

      int64_t origin_payload_pos = payload_pos;
      int64_t tablet_record_size = tablet_pos - origin_payload_pos;
      if (OB_SUCCESS == ret)
      {
        meta_header.tablet_count_ = tablet_count;
        meta_header.data_version_ = get_data_version();
        meta_header.row_key_stream_offset_ = static_cast<int32_t>(tablet_record_size);
        meta_header.row_key_stream_size_ = static_cast<int32_t>(builder.get_data_size());
        meta_header.tablet_extend_info_offset_ = (meta_header.row_key_stream_offset_ + meta_header.row_key_stream_size_);
        meta_header.tablet_extend_info_size_ = static_cast<int32_t>(extend.get_data_size());
        ret = meta_header.serialize(buf, buf_len, payload_pos);
      }

      if (OB_SUCCESS == ret)
      {
        record_header.data_length_ = static_cast<int32_t>(
            tablet_record_size + builder.get_data_size() + extend.get_data_size());
        record_header.data_zlength_ = record_header.data_length_;
        int64_t crc =  common::ob_crc64(
            0, buf + origin_payload_pos, tablet_record_size);
        if (NULL != builder.get_buf() && 0 < builder.get_data_size())
        {
          crc = common::ob_crc64(
              crc, builder.get_buf(), builder.get_data_size());
        }
        if (NULL != extend.get_buf() && 0 < extend.get_data_size())
        {
          crc = common::ob_crc64(
              crc, extend.get_buf(), extend.get_data_size());
        }

        record_header.data_checksum_ = crc;
        record_header.set_header_checksum();
        ret = record_header.serialize(buf, buf_len, origin_pos);
      }

      if (OB_SUCCESS == ret) pos = tablet_pos;

      return ret;
    }

    int ObTabletImage::deserialize(const bool load_sstable,
        const int32_t disk_no, const char* buf, const int64_t data_len, int64_t& pos)
    {
      UNUSED(disk_no);

      int ret = OB_SUCCESS;
      int64_t origin_payload_pos = 0;
      ret = ObRecordHeader::nonstd_check_record(buf + pos, data_len,
          ObTabletMetaHeader::TABLET_META_MAGIC);
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(ERROR, "check common record header failed, disk_no=%d", disk_no);
      }

      ObTabletMetaHeader meta_header;
      if (OB_SUCCESS == ret)
      {
        pos += OB_RECORD_HEADER_LENGTH;
        origin_payload_pos = pos;
        ret = meta_header.deserialize(buf, data_len, pos);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR, "deserialize meta header failed, disk_no=%d", disk_no);
        }
        else if (data_version_ != meta_header.data_version_)
        {
          TBSYS_LOG(ERROR, "data_version_=%ld != header.version=%ld",
              data_version_, meta_header.data_version_);
          ret = OB_ERROR;
        }
      }


      // check the rowkey char stream
      char* row_key_buf = NULL;
      if (OB_SUCCESS == ret)
      {
        if (origin_payload_pos
            + meta_header.row_key_stream_offset_
            + meta_header.row_key_stream_size_
            > data_len)
        {
          ret = OB_SIZE_OVERFLOW;
        }
        else
        {
          const char* row_key_stream_ptr = buf + origin_payload_pos
            + meta_header.row_key_stream_offset_;
          alloc_mutex_.lock();
          row_key_buf = allocator_.alloc_aligned(
              meta_header.row_key_stream_size_);
          alloc_mutex_.unlock();
          if (NULL == row_key_buf)
          {
            ret = OB_ALLOCATE_MEMORY_FAILED;
          }
          else
          {
            memcpy(row_key_buf, row_key_stream_ptr,
                meta_header.row_key_stream_size_);
          }
        }
      }

      const char* tablet_extend_buf = NULL;
      if (OB_SUCCESS == ret
          && meta_header.tablet_extend_info_offset_ > 0
          && meta_header.tablet_extend_info_size_ > 0)
      {
        if (origin_payload_pos
            + meta_header.tablet_extend_info_offset_
            + meta_header.tablet_extend_info_size_
            > data_len)
        {
          ret = OB_SIZE_OVERFLOW;
        }
        else
        {
          tablet_extend_buf =  buf + origin_payload_pos
            + meta_header.tablet_extend_info_offset_;
        }
      }

      if (OB_SUCCESS == ret)
      {
        int64_t row_key_cur_offset = 0;
        int64_t tablet_extend_cur_pos = 0;
        ObTabletExtendInfo extend_info;
        for (int64_t i = 0; i < meta_header.tablet_count_; ++i)
        {
          ObTabletRangeInfo info;
          ret = info.deserialize(buf, data_len, pos);
          if (OB_SUCCESS != ret) break;

          ObTablet* tablet = alloc_tablet_object();
          if (NULL != tablet)
          {
            ret = tablet->deserialize(buf, data_len, pos);
            if (OB_SUCCESS == ret)
            {
              ret = tablet->set_range_by_info(info,
                  row_key_buf + row_key_cur_offset,
                  meta_header.row_key_stream_size_ - row_key_cur_offset);
            }

            if (OB_SUCCESS == ret)
            {
              row_key_cur_offset += info.start_key_size_ + info.end_key_size_;
            }

            // set extend info if extend info exist
            if (OB_SUCCESS == ret && NULL != tablet_extend_buf
                && OB_SUCCESS == (ret = extend_info.deserialize(tablet_extend_buf,
                  meta_header.tablet_extend_info_size_, tablet_extend_cur_pos)))
            {
              tablet->set_extend_info(extend_info);
            }

            if (OB_SUCCESS == ret && load_sstable)
            {
              ret = tablet->load_sstable();
            }
          }
          else
          {
            ret = OB_ALLOCATE_MEMORY_FAILED;
          }

          if (OB_SUCCESS == ret)
          {
            int64_t max_seq = tablet->get_max_sstable_file_seq();
            if (max_seq > max_sstable_file_seq_) max_sstable_file_seq_ = max_seq;
            tablet->set_disk_no(disk_no);
            ret = add_tablet(tablet);
          }

          if (OB_SUCCESS != ret) break;
        }
      }

      return ret;

    }

    int ObTabletImage::serialize(const int32_t disk_no, char* buf,
        const int64_t buf_len, int64_t &pos)
    {
      int ret = OB_SUCCESS;
      ObThreadMetaWriter* writer =
        GET_TSI_MULT(ObThreadMetaWriter, TSI_CS_THEEAD_META_WRITER_1);

      if (NULL == buf || buf_len <= 0 || pos >= buf_len)
      {
        ret = OB_INVALID_ARGUMENT;
      }
      if (NULL == writer)
      {
        TBSYS_LOG(ERROR, "failed to allocate memory for thread local meta writer, "
                         "meta_writer=%p", writer);
        ret = OB_ALLOCATE_MEMORY_FAILED;
      }
      else
      {
        writer->reset();
      }

      if (OB_SUCCESS == ret)
      {
        // dump, migrate, create_tablet maybe serialize same disk image concurrency
        ret = serialize(disk_no, buf, buf_len, pos, writer->builder_, writer->extend_);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR, "ObTabletImage::serialize error, disk no=%d", disk_no);
        }
      }

      if (OB_SUCCESS == ret)
      {
        if (pos + writer->builder_.get_data_size() + writer->extend_.get_data_size() > buf_len)
        {
          TBSYS_LOG(ERROR, "size overflow, "
              "pos=%ld,builder.datasize=%ld, extend.datasize=%ld, buf_len=%ld",
              pos, writer->builder_.get_data_size(), writer->extend_.get_data_size(), buf_len);
          ret = OB_SIZE_OVERFLOW;
        }
        else
        {
          memcpy(buf + pos,  writer->builder_.get_buf(), writer->builder_.get_data_size());
          pos += writer->builder_.get_data_size();
          memcpy(buf + pos,  writer->extend_.get_buf(), writer->extend_.get_data_size());
          pos += writer->extend_.get_data_size();
        }
      }

      return ret;
    }

    int ObTabletImage::deserialize(const int32_t disk_no,
        const char* buf, const int64_t buf_len, int64_t &pos)
    {
      return deserialize(false, disk_no, buf, buf_len, pos);
    }

    int64_t ObTabletImage::get_max_serialize(const int32_t disk_no) const
    {
      UNUSED(disk_no);
      int64_t max_serialize_size =
        OB_RECORD_HEADER_LENGTH + sizeof(ObTabletMetaHeader);
      max_serialize_size +=
        ( sizeof(ObTabletRangeInfo) + sizeof(int64_t) * 2
          + sizeof(ObSSTableId) * ObTablet::MAX_SSTABLE_PER_TABLET )
        * tablet_list_.size();
      return max_serialize_size;
    }

    int32_t ObTabletImage::get_tablets_num() const
    {
      return tablet_list_.size();
    }

    int ObTabletImage::read(const int32_t disk_no, const bool load_sstable)
    {
      int ret = OB_SUCCESS;
      char path[OB_MAX_FILE_NAME_LENGTH];
      ret = get_meta_path(disk_no, true, path, OB_MAX_FILE_NAME_LENGTH);
      if (OB_SUCCESS == ret && FileDirectoryUtils::exists(path))
      {
        ret = read(path, disk_no, load_sstable);
      }
      return ret;
    }

    int ObTabletImage::read(const char* idx_path, const int32_t disk_no, const bool load_sstable)
    {
      int ret = OB_SUCCESS;
      if (NULL == idx_path || strlen(idx_path) == 0)
      {
        ret = OB_INVALID_ARGUMENT;
      }
      else if (!FileDirectoryUtils::exists(idx_path))
      {
        TBSYS_LOG(INFO, "meta index file path=%s, disk_no=%d not exist", idx_path, disk_no);
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        TBSYS_LOG(INFO, "read meta index file path=%s, disk_no=%d", idx_path, disk_no);
      }

      if (OB_SUCCESS == ret)
      {
        ObString fname(0, static_cast<int32_t>(strlen(idx_path)), const_cast<char*>(idx_path));

        char* file_buf = NULL;
        int64_t file_size = get_file_size(idx_path);
        int64_t read_size = 0;

        if (file_size < static_cast<int64_t>(sizeof(ObTabletMetaHeader)))
        {
          TBSYS_LOG(INFO, "invalid idx file =%s file_size=%ld", idx_path, file_size);
          ret = OB_ERROR;
        }

        // not use direct io
        FileComponent::BufferFileReader reader;
        if (OB_SUCCESS == ret)
        {
          if (OB_SUCCESS != (ret = reader.open(fname)))
          {
            TBSYS_LOG(ERROR, "open %s for read error, %s.", idx_path, strerror(errno));
          }
        }

        if (OB_SUCCESS == ret)
        {
          file_buf = static_cast<char*>(ob_malloc(file_size, ObModIds::OB_CS_TABLET_IMAGE));
          if (NULL == file_buf)
          {
            ret = OB_ALLOCATE_MEMORY_FAILED;
          }
        }

        if (OB_SUCCESS == ret)
        {
          ret = reader.pread(file_buf, file_size, 0, read_size);
          if (ret != OB_SUCCESS || read_size < file_size)
          {
            TBSYS_LOG(ERROR, "read idx file = %s , ret = %d, read_size = %ld, file_size = %ld, %s.",
                idx_path, ret, read_size, file_size, strerror(errno));
            ret = OB_IO_ERROR;
          }
          else
          {
            ret = OB_SUCCESS;
          }
        }

        if (OB_SUCCESS == ret)
        {
          int64_t pos = 0;
          ret = deserialize(load_sstable, disk_no, file_buf, file_size, pos);
        }

        if (NULL != file_buf)
        {
          ob_free(file_buf);
          file_buf = NULL;
        }

      }

      return ret;
    }

    int ObTabletImage::write(const int32_t disk_no)
    {
      int ret = OB_SUCCESS;
      char path[OB_MAX_FILE_NAME_LENGTH];
      ret = get_meta_path(disk_no, true, path, OB_MAX_FILE_NAME_LENGTH);
      if (OB_SUCCESS == ret)
      {
        ret = write(path, disk_no);
      }
      return ret;
    }

    int ObTabletImage::write(const char* idx_path, const int32_t disk_no)
    {
      int ret = OB_SUCCESS;
      int64_t pos = 0;
      char tmp_idx_file[OB_MAX_FILE_NAME_LENGTH];

      if (NULL == idx_path || strlen(idx_path) == 0)
      {
        ret = OB_INVALID_ARGUMENT;
      }
      else if (OB_SUCCESS != (ret = get_tmp_meta_path(disk_no,
              tmp_idx_file, OB_MAX_FILE_NAME_LENGTH)))
      {
        // use old backup meta path as tmp idx file;
        // tmp_idx_[disk_no]_seconds
        TBSYS_LOG(WARN, "cannot get tmp meta file path disk_no=%d.", disk_no);
      }
      else
      {
        TBSYS_LOG(INFO, "write meta index file path=%s, disk_no=%d, tmp_file=%s",
            idx_path, disk_no, tmp_idx_file);
      }

      if (OB_SUCCESS == ret)
      {
        int64_t max_serialize_size = get_max_serialize(disk_no);

        ObString fname(0, static_cast<int32_t>(strlen(tmp_idx_file)), tmp_idx_file);
        ObThreadMetaWriter* writer =
          GET_TSI_MULT(ObThreadMetaWriter, TSI_CS_THEEAD_META_WRITER_1);

        if (NULL == writer)
        {
          TBSYS_LOG(ERROR, "failed to allocate memory for thread local meta writer, "
                           "meta_writer=%p", writer);
          ret = OB_ALLOCATE_MEMORY_FAILED;
        }
        else
        {
          writer->reset();
          if (OB_SUCCESS != (ret = writer->meta_buf_.ensure_space(max_serialize_size, ObModIds::OB_CS_TABLET_IMAGE)))
          {
            TBSYS_LOG(ERROR, "allocate memory for serialize image error, size =%ld", max_serialize_size);
            ret = OB_ALLOCATE_MEMORY_FAILED;
          }
        }

        if (OB_SUCCESS == ret)
        {
          if (OB_SUCCESS != (ret = serialize(disk_no, writer->meta_buf_.get_buffer(),
            max_serialize_size, pos, writer->builder_, writer->extend_)))
          {
            TBSYS_LOG(ERROR, "serialize error, disk_no=%d", disk_no);
          }
          // use direct io, create new file, trucate if exist.
          else if (OB_SUCCESS != (ret = writer->appender_.open(fname, true, true, true)))
          {
            TBSYS_LOG(ERROR, "open idx file = %s for write error, %s.", idx_path, strerror(errno));
          }
          else if (OB_SUCCESS != (ret = writer->appender_.append(writer->meta_buf_.get_buffer(), pos, false) ))
          {
            TBSYS_LOG(ERROR, "write meta buffer failed,%s.", strerror(errno));
          }
          else if (writer->builder_.get_data_size() > 0 && OB_SUCCESS != (ret = writer->appender_.append(
                  writer->builder_.get_buf(), writer->builder_.get_data_size(), false)))
          {
            TBSYS_LOG(ERROR, "write row key buffer failed,%s.", strerror(errno));
          }
          else if (writer->extend_.get_data_size() > 0 && OB_SUCCESS != (ret = writer->appender_.append(
                  writer->extend_.get_buf(), writer->extend_.get_data_size(), false)))
          {
            TBSYS_LOG(ERROR, "write extend info buffer failed,%s.", strerror(errno));
          }
        }

        // must call close even through the ret isn't OB_SUCCESS, close() will
        // reset the internal variables, we can reuse the appender next time
        if (NULL != writer)
        {
          writer->appender_.close();
        }

        if (OB_SUCCESS == ret)
        {
          if (!FileDirectoryUtils::rename(tmp_idx_file, idx_path))
          {
            TBSYS_LOG(ERROR, "rename src meta = %s to dst meta =%s error.", tmp_idx_file, idx_path);
            ret = OB_IO_ERROR;
          }
        }
      }

      return ret;
    }

    int ObTabletImage::prepare_write_meta(const int32_t disk_no)
    {
      int ret = OB_SUCCESS;
      int64_t max_serialize_size = get_max_serialize(disk_no);
      ObThreadMetaWriter* writer =
        GET_TSI_MULT(ObThreadMetaWriter, TSI_CS_THEEAD_META_WRITER_1);

      if (NULL == writer)
      {
        TBSYS_LOG(ERROR, "failed to allocate memory for thread local meta writer, "
                         "meta_writer=%p", writer);
        ret = OB_ALLOCATE_MEMORY_FAILED;
      }
      else
      {
        writer->reset();
        if (OB_SUCCESS != (ret = writer->meta_buf_.ensure_space(max_serialize_size, ObModIds::OB_CS_TABLET_IMAGE)))
        {
          TBSYS_LOG(ERROR, "allocate memory for serialize image error, size =%ld", max_serialize_size);
          ret = OB_ALLOCATE_MEMORY_FAILED;
        }
      }

      if (OB_SUCCESS == ret)
      {
        int64_t pos = 0;

        if (OB_SUCCESS != (ret = serialize(disk_no, writer->meta_buf_.get_buffer(),
          max_serialize_size, pos, writer->builder_, writer->extend_)))
        {
          TBSYS_LOG(ERROR, "serialize error, disk_no=%d", disk_no);
        }
        else
        {
          writer->meta_data_size_ = pos;
        }
      }

      return ret;
    }

    int ObTabletImage::write_meta(const char* idx_path, const int32_t disk_no)
    {
      int ret = OB_SUCCESS;
      char tmp_idx_file[OB_MAX_FILE_NAME_LENGTH];

      if (NULL == idx_path || strlen(idx_path) == 0)
      {
        ret = OB_INVALID_ARGUMENT;
      }
      else if (OB_SUCCESS != (ret = get_tmp_meta_path(disk_no,
              tmp_idx_file, OB_MAX_FILE_NAME_LENGTH)))
      {
        // use old backup meta path as tmp idx file;
        // tmp_idx_[disk_no]_seconds
        TBSYS_LOG(WARN, "cannot get tmp meta file path disk_no=%d.", disk_no);
      }
      else
      {
        TBSYS_LOG(INFO, "write meta file, index file path=%s, disk_no=%d, tmp_file=%s",
            idx_path, disk_no, tmp_idx_file);
      }

      if (OB_SUCCESS == ret)
      {
        ObString fname(0, static_cast<int32_t>(strlen(tmp_idx_file)), tmp_idx_file);
        ObThreadMetaWriter* writer =
          GET_TSI_MULT(ObThreadMetaWriter, TSI_CS_THEEAD_META_WRITER_1);

        if (NULL == writer)
        {
          TBSYS_LOG(ERROR, "failed to allocate memory for thread local meta writer, "
                           "meta_writer=%p", writer);
          ret = OB_ALLOCATE_MEMORY_FAILED;
        }
        else if (writer->meta_data_size_ <= 0)
        {
          TBSYS_LOG(WARN, "invalid meta data size, must call prepare_write_meta first, "
                          "meta_data_size_=%ld", writer->meta_data_size_);
          ret = OB_ERROR;
        }

        if (OB_SUCCESS == ret)
        {
          // use direct io, create new file, trucate if exist.
          if (OB_SUCCESS != (ret = writer->appender_.open(fname, true, true, true)))
          {
            TBSYS_LOG(ERROR, "open idx file = %s for write error, %s.", idx_path, strerror(errno));
          }
          else if (OB_SUCCESS != (ret = writer->appender_.append(
            writer->meta_buf_.get_buffer(), writer->meta_data_size_, false) ))
          {
            TBSYS_LOG(ERROR, "write meta buffer failed,%s.", strerror(errno));
          }
          else if (writer->builder_.get_data_size() > 0 && OB_SUCCESS != (ret = writer->appender_.append(
                  writer->builder_.get_buf(), writer->builder_.get_data_size(), false)))
          {
            TBSYS_LOG(ERROR, "write row key buffer failed,%s.", strerror(errno));
          }
          else if (writer->extend_.get_data_size() > 0 && OB_SUCCESS != (ret = writer->appender_.append(
                  writer->extend_.get_buf(), writer->extend_.get_data_size(), false)))
          {
            TBSYS_LOG(ERROR, "write extend info buffer failed,%s.", strerror(errno));
          }
        }

        // must call close even through the ret isn't OB_SUCCESS, close() will
        // reset the internal variables, we can reuse the appender next time
        if (NULL != writer)
        {
          writer->appender_.close();
        }

        if (OB_SUCCESS == ret)
        {
          if (!FileDirectoryUtils::rename(tmp_idx_file, idx_path))
          {
            TBSYS_LOG(ERROR, "rename src meta = %s to dst meta =%s error.", tmp_idx_file, idx_path);
            ret = OB_IO_ERROR;
          }
          else
          {
            TBSYS_LOG(INFO, "write tablet image file %s succ.", idx_path);
          }
        }
      }

      return ret;
    }

    int ObTabletImage::read(const int32_t* disk_no_array, const int32_t size, const bool load_sstable)
    {
      int ret = OB_SUCCESS;
      if (NULL == disk_no_array || size <= 0)
      {
        ret = OB_INVALID_ARGUMENT;
      }

      if (OB_SUCCESS == ret)
      {
        for (int32_t i = 0; i < size; ++i)
        {
          ret = read(disk_no_array[i], load_sstable);
          // TODO read failed on one disk.
          if (OB_SUCCESS != ret)
          {
            TBSYS_LOG(WARN, "read meta info from disk %d failed", disk_no_array[i]);
            continue;
          }
        }
      }
      return ret;
    }

    int ObTabletImage::write(const int32_t* disk_no_array, const int32_t size)
    {
      int ret = OB_SUCCESS;
      if (NULL == disk_no_array || size <= 0)
      {
        ret = OB_INVALID_ARGUMENT;
      }

      if (OB_SUCCESS == ret)
      {
        for (int32_t i = 0; i < size; ++i)
        {
          ret = write(disk_no_array[i]);
          // TODO write failed on one disk.
          if (OB_SUCCESS != ret)
          {
            TBSYS_LOG(WARN, "write meta info to disk %d failed", disk_no_array[i]);
            continue;
          }
        }
      }
      return ret;
    }

    int ObTabletImage::begin_scan_tablets()
    {
      int ret = OB_SUCCESS;
      if (cur_iter_idx_ != INVALID_ITER_INDEX)
      {
        TBSYS_LOG(ERROR, "scan in progress, cannot luanch another, cur_iter_idx_=%ld", cur_iter_idx_);
        ret = OB_CS_EAGAIN;
      }
      else
      {
        TBSYS_LOG(INFO, "begin_scan_tablets cur_iter_idx_ = %ld", cur_iter_idx_);
      }

      if (OB_SUCCESS == ret)
      {
        if (tablet_list_.size() == 0)
        {
          ret = OB_ITER_END;
        }
      }

      if (OB_SUCCESS == ret)
      {
        cur_iter_idx_ = 0;
      }

      return ret;
    }

    int ObTabletImage::get_next_tablet(ObTablet* &tablet)
    {
      int ret = OB_SUCCESS;
      tablet = NULL;
      if (cur_iter_idx_ == INVALID_ITER_INDEX)
      {
        TBSYS_LOG(ERROR, "not a initialized scan process, call begin_scan_tablets first");
        ret = OB_ERROR;
      }

      if (cur_iter_idx_ >= tablet_list_.size())
      {
        ret = OB_ITER_END;
      }
      else
      {
        tablet = tablet_list_.at(static_cast<int32_t>(cur_iter_idx_));
        __sync_add_and_fetch(&cur_iter_idx_, 1);
        acquire();
        tablet->inc_ref();
      }
      return ret;
    }

    int ObTabletImage::end_scan_tablets()
    {
      int ret = OB_SUCCESS;
      if (cur_iter_idx_ == INVALID_ITER_INDEX)
      {
        TBSYS_LOG(INFO, "scans not begin or has no tablets, cur_iter_idx_=%ld", cur_iter_idx_);
        ret = OB_ERROR;
      }
      else
      {
        TBSYS_LOG(INFO, "end_scan_tablets cur_iter_idx_=%ld", cur_iter_idx_);
      }
      cur_iter_idx_ = INVALID_ITER_INDEX;
      return ret;
    }

    int ObTabletImage::dump(const bool dump_sstable) const
    {
      TBSYS_LOG(INFO, "ref_count_=%ld, cur_iter_idx_=%ld, memory usage=%ld",
          ref_count_, cur_iter_idx_, allocator_.total());

      TBSYS_LOG(INFO, "----->begin dump tablets in image<--------");
      for (int32_t i = 0; i < tablet_list_.size(); ++i)
      {
        ObTablet* tablet = tablet_list_.at(i);
        TBSYS_LOG(INFO, "----->tablet(%d)<--------", i);
        tablet->dump(dump_sstable);
      }
      TBSYS_LOG(INFO, "----->end dump tablets in image<--------");

      return OB_SUCCESS;
    }

    //----------------------------------------
    // class ObMultiVersionTabletImage
    //----------------------------------------
    ObMultiVersionTabletImage::ObMultiVersionTabletImage(IFileInfoMgr& fileinfo_cache)
      : newest_index_(0), service_index_(-1), iterator_(*this),
      fileinfo_cache_(fileinfo_cache), disk_manager_(NULL)
    {
      memset(image_tracker_, 0, sizeof(image_tracker_));
    }

    ObMultiVersionTabletImage::ObMultiVersionTabletImage(IFileInfoMgr& fileinfo_cache, ObDiskManager& disk_manager)
      : newest_index_(0), service_index_(-1), iterator_(*this), fileinfo_cache_(fileinfo_cache), disk_manager_(&disk_manager)
    {
      memset(image_tracker_, 0, sizeof(image_tracker_));
    }

    ObMultiVersionTabletImage::~ObMultiVersionTabletImage()
    {
      destroy();
    }

    int64_t ObMultiVersionTabletImage::get_serving_version() const
    {
      int64_t data_version = 0;
      int64_t index = 0;

      tbsys::CRLockGuard guard(lock_);

      index = service_index_;
      if (index >= 0 && index < MAX_RESERVE_VERSION_COUNT && has_tablet(index))
      {
        data_version = image_tracker_[index]->data_version_;

      }
      return data_version;
    }

    int64_t ObMultiVersionTabletImage::get_eldest_version() const
    {
      int64_t data_version = 0;
      int64_t index = 0;

      tbsys::CRLockGuard guard(lock_);

      index = get_eldest_index();
      if (index >= 0 && index < MAX_RESERVE_VERSION_COUNT && has_tablet(index))
      {
        data_version = image_tracker_[index]->data_version_;

      }
      return data_version;
    }

    int64_t ObMultiVersionTabletImage::get_newest_version() const
    {
      int64_t data_version = 0;

      tbsys::CRLockGuard guard(lock_);

      int64_t index = newest_index_;
      do
      {
        if (index < 0 || index >= MAX_RESERVE_VERSION_COUNT)
        {
          break;
        }
        if (has_tablet(index))
        {
          data_version = image_tracker_[index]->data_version_;
          break;
        }
        // if not found, search older version tablet until iterated every version of tablet.
        if (--index < 0) index = MAX_RESERVE_VERSION_COUNT - 1;
      } while (index != newest_index_);

      return data_version;
    }

    int64_t ObMultiVersionTabletImage::get_eldest_index() const
    {
      int64_t index = (newest_index_ + 1) % MAX_RESERVE_VERSION_COUNT;
      do
      {
        if (has_tablet(index))
        {
          break;
        }
        else
        {
          index = (index + 1) % MAX_RESERVE_VERSION_COUNT;
        }
      } while (index != newest_index_);

      return index;
    }

    int ObMultiVersionTabletImage::acquire_tablet(
        const common::ObNewRange &range,
        const ScanDirection scan_direction,
        const int64_t version,
        ObTablet* &tablet) const
    {
      return acquire_tablet_all_version(
          range, scan_direction, FROM_SERVICE_INDEX, version, tablet);
    }

    int ObMultiVersionTabletImage::acquire_tablet_all_version(
        const common::ObNewRange &range,
        const ScanDirection scan_direction,
        const ScanPosition from_index,
        const int64_t version,
        ObTablet* &tablet) const
    {
      int ret = OB_SUCCESS;
      if (range.empty() || 0 > version
          || (from_index != FROM_SERVICE_INDEX && from_index != FROM_NEWEST_INDEX))
      {
        TBSYS_LOG(WARN, "acquire_tablet invalid argument, "
            "range(%s) is emtpy(%d) or version=%ld < 0, or from_index =%d illegal",
            to_cstring(range), range.empty(), version, from_index);
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        ret = OB_CS_TABLET_NOT_EXIST;

        tbsys::CRLockGuard guard(lock_);

        int64_t start_index =
          (from_index == FROM_SERVICE_INDEX) ? service_index_ : newest_index_;
        int64_t index = start_index;
        int64_t serving_version = 0;
        if (service_index_ >= 0 && service_index_ < MAX_RESERVE_VERSION_COUNT
            && has_tablet(service_index_))
        {
          serving_version = image_tracker_[service_index_]->data_version_;
        }

        do
        {
          if (index < 0 || index >= MAX_RESERVE_VERSION_COUNT)
          {
            TBSYS_LOG(WARN, "image (index=%ld) not initialized, has no tablets", index);
            break;
          }
          // version == 0 search from serving tablet
          if (version == 0 && has_tablet(index))
          {
            ret = image_tracker_[index]->acquire_tablet(range, scan_direction, tablet);
          }
          // version != 0 search from newest tablet which has version less or equal than %version
          if (version != 0 && has_tablet(index))
          {
            if (version >= serving_version && image_tracker_[index]->data_version_ <= version)
            {
              // search from serving tablet
              ret = image_tracker_[index]->acquire_tablet(range, scan_direction, tablet);
            }
            else if (version < serving_version && image_tracker_[index]->data_version_ == version)
            {
              // search from oldest tablet
              ret = image_tracker_[index]->acquire_tablet(range, scan_direction, tablet);
            }
            else
            {
              // query tablet version not exist
            }
          }

          if (OB_SUCCESS == ret) break;
          // if not found, search older version tablet until iterated every version of tablet.
          if (--index < 0) index = MAX_RESERVE_VERSION_COUNT - 1;
        } while (index != start_index);
      }

      return ret;
    }

    int ObMultiVersionTabletImage::release_tablets(const ObVector<ObTablet*>& tablets) const
    {
      int ret = OB_SUCCESS;
      int tmp_ret = OB_SUCCESS;
      for(ObVector<ObTablet*>::iterator it = tablets.begin(); it != tablets.end(); ++it)
      {
        if(OB_SUCCESS != (tmp_ret = release_tablet((*it))))
        {
          TBSYS_LOG(WARN, "failed to release tablet, tablet=%s ret=:%d",
              to_cstring((*it)->get_range()), tmp_ret);
          ret = tmp_ret;
        }
      }

      return ret;
    }

    int ObMultiVersionTabletImage::release_tablet(ObTablet* tablet) const
    {
      int ret = OB_SUCCESS;
      bool is_remove_sstable = false;

      {
        tbsys::CRLockGuard guard(lock_);

        if (NULL == tablet)
        {
          TBSYS_LOG(WARN, "release_tablet invalid argument tablet null");
          ret = OB_INVALID_ARGUMENT;
        }
        else if (!has_match_version(tablet->get_data_version()))
        {
          TBSYS_LOG(WARN, "release_tablet version=%ld dont match",
              tablet->get_data_version());
          ret = OB_INVALID_ARGUMENT;
        }
        else
        {
          ret = get_image(tablet->get_data_version()).release_tablet(tablet, &is_remove_sstable);
        }
      }

      if (OB_SUCCESS == ret && is_remove_sstable)
      {
        get_image(tablet->get_data_version()).remove_sstable(tablet);
        tablet->~ObTablet();
      }
      return ret;
    }

    int ObMultiVersionTabletImage::acquire_tablet(
        const sstable::ObSSTableId& sstable_id,
        const int64_t version,
        ObTablet* &tablet) const
    {
      return acquire_tablet_all_version(sstable_id,
          FROM_SERVICE_INDEX, version, tablet);
    }

    int ObMultiVersionTabletImage::acquire_tablet_all_version(
        const sstable::ObSSTableId& sstable_id,
        const ScanPosition from_index,
        const int64_t version,
        ObTablet* &tablet) const
    {
      int ret = OB_SUCCESS;

      if (0 > version
          || (from_index != FROM_SERVICE_INDEX && from_index != FROM_NEWEST_INDEX))
      {
        TBSYS_LOG(ERROR, "acquire_sstable invalid argument, "
            "version=%ld < 0", version);
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        ret = OB_ENTRY_NOT_EXIST;
        tablet = NULL;

        tbsys::CRLockGuard guard(lock_);
        int64_t start_index =
          (from_index == FROM_SERVICE_INDEX) ? service_index_ : newest_index_;
        int64_t index = start_index;

        do
        {
          if (index < 0 || index >= MAX_RESERVE_VERSION_COUNT)
          {
            TBSYS_LOG(WARN, "image (index=%ld) not initialized, has no tablets", index);
            break;
          }

          // version == 0 search from newest tablet
          if (version == 0 && has_tablet(index))
          {
            ret = image_tracker_[index]->acquire_tablet(sstable_id, tablet);
          }

          // version != 0 search from newest tablet which has version less or equal than %version
          if (version != 0 && has_tablet(index) && image_tracker_[index]->data_version_ <= version)
          {
            ret = image_tracker_[index]->acquire_tablet(sstable_id, tablet);
          }

          if (OB_SUCCESS == ret) break;
          // if not found, search older version tablet until iterated every version of tablet.
          if (--index < 0) index = MAX_RESERVE_VERSION_COUNT - 1;
        } while (index != start_index);
      }

      return ret;
    }

    int ObMultiVersionTabletImage::acquire_tablets_on_disk(
        const int32_t disk_no,
        ObVector<ObTablet*>& tablet_list) const
    {
      int ret = OB_SUCCESS;

      if (0 > disk_no)
      {
        TBSYS_LOG(ERROR, "acquire_sstable invalid argument, "
            "disk_no=%d < 0", disk_no);
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        tbsys::CRLockGuard guard(lock_);
        int64_t start_index = service_index_;
        int64_t index = start_index;

        do
        {
          if (index < 0 || index >= MAX_RESERVE_VERSION_COUNT)
          {
            TBSYS_LOG(WARN, "image (index=%ld) not initialized, has no tablets", index);
            break;
          }

          if (has_tablet(index))
          {
            ret = image_tracker_[index]->acquire_tablets_on_disk(disk_no, tablet_list);
          }

          if (OB_SUCCESS != ret) break;

          if (--index < 0) index = MAX_RESERVE_VERSION_COUNT - 1;
        } while (index != start_index);
      }

      return ret;
    }


    int ObMultiVersionTabletImage::include_sstable(const ObSSTableId& sstable_id) const
    {
      int ret = OB_SUCCESS;

      if (0 >= sstable_id.sstable_file_id_)
      {
        TBSYS_LOG(ERROR, "include_sstable invalid argument, "
            "sstable file id = %ld ", sstable_id.sstable_file_id_);
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        ret = OB_ENTRY_NOT_EXIST;

        tbsys::CRLockGuard guard(lock_);
        int64_t start_index = newest_index_;
        int64_t index = start_index;

        do
        {
          if (index < 0 || index >= MAX_RESERVE_VERSION_COUNT)
          {
            TBSYS_LOG(WARN, "image (index=%ld) not initialized, has no tablets", index);
            break;
          }

          // version == 0 search from newest tablet
          if (has_tablet(index))
          {
            ret = image_tracker_[index]->include_sstable(sstable_id);
          }

          if (OB_SUCCESS == ret) break;
          // if not found, search older version tablet until iterated every version of tablet.
          if (--index < 0) index = MAX_RESERVE_VERSION_COUNT - 1;
        } while (index != start_index);
      }

      return ret;
    }


    int ObMultiVersionTabletImage::remove_tablets(ObVector<ObTablet*>& tablets, bool sync)
    {
      int32_t tmp_disk_no = 0;
      int ret = OB_SUCCESS;
      for(ObVector<ObTablet*>::iterator it = tablets.begin();
          it != tablets.end() && OB_SUCCESS == ret; ++it)
      {
        //remove delete_tablet_list_ from the tablet image
        ret = remove_tablet((*it)->get_range(), (*it)->get_data_version(), tmp_disk_no, sync);

        if(OB_SUCCESS != ret && OB_ENTRY_NOT_EXIST != ret)
        {
          TBSYS_LOG(WARN, "remove tablet<%s> error, ret:%d", to_cstring((*it)->get_range()), ret);
          break;
        }
        else
        {
          TBSYS_LOG(INFO, "remove tablet<%s> on disk_no:%d data_version:%ld",
              to_cstring((*it)->get_range()), (*it)->get_disk_no(), (*it)->get_data_version());
          ret = OB_SUCCESS;
        }
      }

      return ret;
    }

    int ObMultiVersionTabletImage::remove_tablet(const common::ObNewRange& range,
        const int64_t version, int32_t &disk_no, bool sync)
    {
      int ret = OB_SUCCESS;

      {
        tbsys::CWLockGuard guard(lock_);

        if (range.empty() || 0 >= version)
        {
          TBSYS_LOG(WARN, "remove_tablet invalid argument, "
              "range is emtpy or version=%ld < 0", version);
          ret = OB_INVALID_ARGUMENT;
        }
        else if (!has_match_version_tablet(version))
        {
          TBSYS_LOG(WARN, "remove_tablet version=%ld dont match", version);
          ret = OB_INVALID_ARGUMENT;
        }
        else
        {
          ret = get_image(version).remove_tablet(range, disk_no);
        }
      }

      //sync index
      if (OB_SUCCESS == ret && sync)
      {
        ret = write(version, disk_no);
      }
      return ret;
    }

    int ObMultiVersionTabletImage::add_tablet(ObTablet *tablet,
        const bool load_sstable /* = false */,
        const bool for_create /* = false */)
    {
      int ret = OB_SUCCESS;

      if (NULL == tablet || tablet->get_data_version() <= 0)
      {
        TBSYS_LOG(WARN, "add_tablet invalid argument, "
            "tablet=%p or version<0", tablet);
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        // load_sstable first
        if (load_sstable)
        {
          ret = tablet->load_sstable();
        }

        if(OB_SUCCESS == ret)
        {
          int64_t new_version = tablet->get_data_version();

          tbsys::CWLockGuard guard(lock_);

          if ( OB_SUCCESS != (ret = prepare_tablet_image(new_version, false)) )
          {
            TBSYS_LOG(ERROR, "prepare new version = %ld image error.", new_version);
          }
          else if ( OB_SUCCESS != (ret =
                get_image(new_version).add_tablet(tablet)) )
          {
            TBSYS_LOG(ERROR, "add tablet error with new version = %ld.", new_version);
          }
          else if (for_create && service_index_ != newest_index_)
          {
            service_index_ = newest_index_;
          }
        }
      }


      return ret;
    }

    int ObMultiVersionTabletImage::upgrade_tablet(
        ObTablet *old_tablet, ObTablet *new_tablet, const bool load_sstable)
    {
      int ret = OB_SUCCESS;
      if (NULL == old_tablet || NULL == new_tablet
          || old_tablet->get_data_version() <= 0
          || new_tablet->get_data_version() <= 0
          || new_tablet->get_data_version() <= old_tablet->get_data_version()
          || old_tablet->is_merged())
      {
        TBSYS_LOG(WARN, "upgrade_tablet invalid argument, "
            "old_tablet=%p or new_tablet=%p, old merged=%d",
            old_tablet, new_tablet, old_tablet->is_merged());
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        int64_t new_version = new_tablet->get_data_version();
        char range_buf[OB_RANGE_STR_BUFSIZ];
        old_tablet->get_range().to_string(range_buf, OB_RANGE_STR_BUFSIZ);

        TBSYS_LOG(INFO, "upgrade_tablet range:(%s) old version = %ld, new version = %ld",
            range_buf, old_tablet->get_data_version(), new_tablet->get_data_version());

        tbsys::CWLockGuard guard(lock_);

        if ( OB_SUCCESS != (ret = prepare_tablet_image(new_version, true)) )
        {
          TBSYS_LOG(ERROR, "prepare new version = %ld image error.", new_version);
        }
        else if ( OB_SUCCESS != (ret =
              get_image(new_version).add_tablet(new_tablet)) )
        {
          TBSYS_LOG(ERROR, "add tablet error with new version = %ld.", new_version);
        }
        else
        {
          old_tablet->set_merged();
          get_image(old_tablet->get_data_version()).incr_merged_tablet_count();
        }
      }

      if (OB_SUCCESS == ret && NULL != new_tablet && load_sstable)
      {
        ret = new_tablet->load_sstable();
      }

      return ret;
    }

    int ObMultiVersionTabletImage::upgrade_tablet(ObTablet *old_tablet,
        ObTablet *new_tablets[], const int32_t split_size,
        const bool load_sstable)
    {
      int ret = OB_SUCCESS;
      if (NULL == old_tablet || old_tablet->is_merged()
          || NULL == new_tablets || 0 >= split_size)
      {
        TBSYS_LOG(WARN, "upgrade_tablet invalid argument, "
            "old_tablet=%p or old merged=%d new_tablet=%p, split_size=%d",
            old_tablet, old_tablet->is_merged(), new_tablets, split_size);
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        int64_t new_version = new_tablets[0]->get_data_version();

        char range_buf[OB_RANGE_STR_BUFSIZ];
        old_tablet->get_range().to_string(range_buf, OB_RANGE_STR_BUFSIZ);
        TBSYS_LOG(DEBUG, "upgrade old range:(%s), old version=%ld",
            range_buf, old_tablet->get_data_version());

        tbsys::CWLockGuard guard(lock_);

        if (new_version <= old_tablet->get_data_version())
        {
          TBSYS_LOG(ERROR, "new version =%ld <= old version=%ld",
              new_version, old_tablet->get_data_version());
          ret = OB_INVALID_ARGUMENT;
        }
        else if ( OB_SUCCESS != (ret = prepare_tablet_image(new_version, true)) )
        {
          TBSYS_LOG(ERROR, "prepare new version = %ld image error.", new_version);
        }

        for (int32_t i = 0; i < split_size && OB_SUCCESS == ret; ++i)
        {
          // each tablet will store the info that whether the next tablet is its brother,
          // last tablet splited from the same parents tablet will set the falg of
          // with_next_brother to 0, the other tablet will set the falg to 1, so when
          // doing report tablets to rootserver, we can know which tablets are with
          // same parents tablet, and we can report the tablets in one packet
          if (i == split_size - 1)
          {
            new_tablets[i]->set_with_next_brother(0);
          }
          else
          {
            new_tablets[i]->set_with_next_brother(1);
          }

          if (new_tablets[i]->get_data_version() != new_version)
          {
            TBSYS_LOG(ERROR, "split tablet i=%d, version =%ld <> fisrt tablet version =%ld",
                i, new_tablets[i]->get_data_version(), new_version);
            ret = OB_INVALID_ARGUMENT;
            break;
          }
          else if ( OB_SUCCESS != (ret =
                get_image(new_version).add_tablet(new_tablets[i])) )
          {
            TBSYS_LOG(ERROR, "add tablet error with new version = %ld.", new_version);
          }
          else
          {
            new_tablets[i]->get_range().to_string(range_buf, OB_RANGE_STR_BUFSIZ);
            TBSYS_LOG(DEBUG, "upgrade with new range:(%s), new version=%ld",
                range_buf, new_version);
          }
        }

        if (OB_SUCCESS == ret)
        {
          old_tablet->set_merged();
          get_image(old_tablet->get_data_version()).incr_merged_tablet_count();
        }
      }

      if (OB_SUCCESS == ret && load_sstable)
      {
        for (int32_t i = 0; i < split_size && OB_SUCCESS == ret; ++i)
        {
          if (NULL != new_tablets[i] && !new_tablets[i]->is_removed())
          {
            ret = new_tablets[i]->load_sstable();
          }
        }
      }

      return ret;
    }

    //add zhuyanchao[secondary index static_data_build.report]20150401
    int ObMultiVersionTabletImage::upgrade_index_tablet(ObTablet* tablet,
                                                        const bool load_sstable)
    {
      int ret =OB_SUCCESS;
      int64_t new_version = tablet->get_data_version();
      char range_buf[OB_RANGE_STR_BUFSIZ];
      tbsys::CWLockGuard guard(lock_);
      if ( OB_SUCCESS != (ret =
                      get_image(new_version).add_tablet(tablet)) )
      {
        TBSYS_LOG(ERROR, "add tablet error with new version = %ld.", new_version);
      }
      else
      {
        tablet->get_range().to_string(range_buf, OB_RANGE_STR_BUFSIZ);
        TBSYS_LOG(DEBUG, "upgrade with new index range:(%s), new version=%ld",
                   range_buf, new_version);
      }
      if (OB_SUCCESS == ret && load_sstable)
      {
        //TBSYS_LOG(WARN, "test::zhuyanchao load sstable");
        ret = tablet->load_sstable();
      }
      return ret;
    }
    //add e


    int ObMultiVersionTabletImage::upgrade_service()
    {
      int ret = OB_SUCCESS;

      tbsys::CWLockGuard guard(lock_);
      int64_t new_version = image_tracker_[newest_index_]->data_version_;
      int64_t service_version = 0;
      if (service_index_ >= 0
          && service_index_ < MAX_RESERVE_VERSION_COUNT
          && has_tablet(service_index_))
      {
        service_version = image_tracker_[service_index_]->data_version_;
      }

      if (!has_tablet(newest_index_))
      {
        TBSYS_LOG(WARN, "there is no tablets on version = %ld, still upgrade.", new_version);
        service_index_ = newest_index_;
      }
      else if (new_version <= service_version)
      {
        TBSYS_LOG(WARN, "service version =%ld >= newest version =%ld, cannot upgrade.",
            service_version, new_version);
        ret = OB_ERROR;
      }
      else
      {
        // TODO check service_version merged complete?
        TBSYS_LOG(INFO, "upgrade service version =%ld to new version =%ld",
            service_version, new_version);
        service_index_ = newest_index_;
      }

      return ret;
    }

    int ObMultiVersionTabletImage::alloc_tablet_object(
        const common::ObNewRange& range, const int64_t version, ObTablet* &tablet)
    {
      int ret = OB_SUCCESS;

      if (range.empty() || 0 >= version)
      {
        TBSYS_LOG(WARN, "alloc_tablet_object invalid argument, "
            "range is emtpy or version=%ld < 0", version);
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        tablet = NULL;
        tbsys::CWLockGuard guard(lock_);

        if ( OB_SUCCESS != (ret = prepare_tablet_image(version, true)) )
        {
          TBSYS_LOG(ERROR, "prepare new version = %ld image error.", version);
        }
        else if ( OB_SUCCESS != (ret =
              get_image(version).alloc_tablet_object(range, tablet)) )
        {
          TBSYS_LOG(ERROR, "cannot alloc tablet object new version = %ld", version);
        }
        else if (NULL != tablet)
        {
          tablet->set_data_version(version);
        }
      }
      return ret;
    }

    int ObMultiVersionTabletImage::alloc_tablet_object(
        const int64_t version, ObTablet* &tablet)
    {
      int ret = OB_SUCCESS;

      if (0 >= version)
      {
        TBSYS_LOG(WARN, "alloc_tablet_object invalid argument, version=%ld < 0",
          version);
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        tablet = NULL;
        tbsys::CWLockGuard guard(lock_);

        if ( OB_SUCCESS != (ret = prepare_tablet_image(version, true)) )
        {
          TBSYS_LOG(ERROR, "prepare new version = %ld image error.", version);
        }
        else if (NULL == (tablet = get_image(version).alloc_tablet_object()) )
        {
          TBSYS_LOG(ERROR, "cannot alloc tablet object new version = %ld", version);
          ret = OB_ALLOCATE_MEMORY_FAILED;
        }
        else if (NULL != tablet)
        {
          tablet->set_data_version(version);
        }
      }
      return ret;
    }

    int ObMultiVersionTabletImage::prepare_for_merge(const int64_t version)
    {
      tbsys::CWLockGuard guard(lock_);
      return prepare_tablet_image(version, true);
    }

    bool ObMultiVersionTabletImage::has_tablets_for_merge(const int64_t version) const
    {
      bool ret = false;
      if (0 > version)
      {
        ret = false;
      }
      else if (!has_match_version_tablet(version))
      {
        ret = false;
      }
      else
      {
        int64_t index = get_index(version);
        tbsys::CRLockGuard guard(lock_);
        ret = (OB_SUCCESS != tablets_all_merged(index));
      }
      return ret;
    }

    int ObMultiVersionTabletImage::get_tablets_for_merge(
        const int64_t version, int64_t &size, ObTablet *tablets[]) const
    {
      int ret = OB_SUCCESS;

      tbsys::CRLockGuard guard(lock_);

      int64_t eldest_index = get_eldest_index();
      int64_t index = eldest_index;
      int64_t merge_count = 0;
      do
      {
        if (index < 0 || index >= MAX_RESERVE_VERSION_COUNT)
        {
          ret = OB_ERROR;
          break;
        }

        if (has_tablet(index) && image_tracker_[index]->data_version_ < version)
        {
          ObSortedVector<ObTablet*> & tablet_list = image_tracker_[index]->tablet_list_;
          ObSortedVector<ObTablet*>::iterator it = tablet_list.begin();
          for (; it != tablet_list.end() && merge_count < size; ++it)
          {
            if (!(*it)->is_merged())
            {
              tablets[merge_count++] = *it;
              // add reference count
              image_tracker_[index]->acquire();
              (*it)->inc_ref();
            }
          }
        }
        if (merge_count >= size) break;
        index = (index + 1) % MAX_RESERVE_VERSION_COUNT;
      } while (index != eldest_index);

      TBSYS_LOG(DEBUG, "for merge, version=%ld,size=%ld,newest=%ld", version, size, newest_index_);

      size = merge_count;
      return OB_SUCCESS;
    }

    int ObMultiVersionTabletImage::discard_tablets_not_merged(const int64_t version)
    {
      int ret = OB_SUCCESS;
      if (0 > version)
      {
        ret = OB_INVALID_ARGUMENT;
        TBSYS_LOG(ERROR, "invalid version = %ld", version);
      }
      else
      {
        tbsys::CWLockGuard guard(lock_);

        if (has_match_version_tablet(version))
        {
          ObSortedVector<ObTablet*> & tablet_list = get_image(version).tablet_list_;
          ObSortedVector<ObTablet*>::iterator it = tablet_list.begin();
          for (; it != tablet_list.end() ; ++it)
          {
            if (!(*it)->is_merged())
            {
              (*it)->set_merged();
            }
          }
        }
      }
      return ret;
    }

    int ObMultiVersionTabletImage::prepare_tablet_image(const int64_t version, const bool destroy_exist)
    {
      int ret = OB_SUCCESS;
      int64_t index = get_index(version);
      if (index < 0 || index >= MAX_RESERVE_VERSION_COUNT)
      {
        TBSYS_LOG(ERROR, "index=%ld out of range.", index);
        ret = OB_ERROR;
      }
      else if (NULL == image_tracker_[index])
      {
        image_tracker_[index] = new (std::nothrow)ObTabletImage();
        if (NULL == image_tracker_[index])
        {
          TBSYS_LOG(ERROR, "cannot new ObTabletImage object, index=%ld", index);
          ret = OB_ALLOCATE_MEMORY_FAILED;
        }
        else
        {
          image_tracker_[index]->set_fileinfo_cache(fileinfo_cache_);
          image_tracker_[index]->set_disk_manger(disk_manager_);
        }
      }
      else if (version != image_tracker_[index]->data_version_)
      {
        if (destroy_exist && OB_SUCCESS == (ret = tablets_all_merged(index)))
        {
          ret = destroy(index);
        }
        else
        {
          TBSYS_LOG(ERROR, "new version=%ld not matched with old verion=%ld,"
              "index=%ld cannot destroy" , version, image_tracker_[index]->data_version_, index);
        }
      }

      if (OB_SUCCESS == ret)
      {
        image_tracker_[index]->data_version_ = version;
        if (!has_tablet(newest_index_)
            || version > image_tracker_[newest_index_]->data_version_)
        {
          newest_index_ = index;
        }
      }

      return ret;
    }

    int ObMultiVersionTabletImage::tablets_all_merged(const int64_t index) const
    {
      int ret = OB_SUCCESS;
      if (index < 0 || index >= MAX_RESERVE_VERSION_COUNT)
      {
        ret = OB_ERROR;
      }
      else if (NULL != image_tracker_[index])
      {
        ObSortedVector<ObTablet*> & tablet_list = image_tracker_[index]->tablet_list_;
        ObSortedVector<ObTablet*>::iterator it = tablet_list.begin();
        for (; it != tablet_list.end() ; ++it)
        {
          if (!(*it)->is_merged())
          {
            TBSYS_LOG(INFO, "tablet range=<%s> version=%ld has not merged", to_cstring((*it)->get_range()), (*it)->get_data_version());
            ret = OB_CS_EAGAIN;
            break;
          }
        }
      }
      return ret;
    }

    int ObMultiVersionTabletImage::destroy(const int64_t index)
    {
      int ret = OB_SUCCESS;
      if (index < 0 || index >= MAX_RESERVE_VERSION_COUNT)
      {
        ret = OB_ERROR;
      }
      else if (has_tablet(index))
      {
        ret = image_tracker_[index]->destroy();
      }
      return ret;
    }

    int ObMultiVersionTabletImage::destroy()
    {
      int ret = OB_SUCCESS;
      for (int i = 0; i < MAX_RESERVE_VERSION_COUNT && OB_SUCCESS == ret; ++i)
      {
        if (NULL != image_tracker_[i])
        {
          ret = image_tracker_[i]->destroy();
          if (OB_SUCCESS == ret)
          {
            delete image_tracker_[i];
            image_tracker_[i] = NULL;
          }
        }
      }
      return ret;
    }

    int ObMultiVersionTabletImage::write(const int64_t version,
        const int32_t disk_no)
    {
      int ret = OB_SUCCESS;
      if (0 >= version || 0 >= disk_no)
      {
        TBSYS_LOG(WARN, "write image invalid argument, "
            "disk_no =%d <0 or version=%ld < 0", disk_no, version);
        ret = OB_INVALID_ARGUMENT;
      }
      else if (!has_match_version(version))
      {
        TBSYS_LOG(WARN, "write image version=%ld don't match", version);
        ret = OB_ERROR;
      }
      else
      {
        char idx_path[OB_MAX_FILE_NAME_LENGTH];
        ret = get_meta_path(version, disk_no, true, idx_path, OB_MAX_FILE_NAME_LENGTH);
        if (OB_SUCCESS == ret)
        {
          {
            tbsys::CRLockGuard guard(lock_);
            ret = get_image(version).prepare_write_meta(disk_no);
          }
          if (OB_SUCCESS == ret)
          {
            ret = get_image(version).write_meta(idx_path, disk_no);
          }
        }
      }
      return ret;
    }

    int ObMultiVersionTabletImage::write(const char* idx_path,
        const int64_t version, const int32_t disk_no)
    {
      int ret = OB_SUCCESS;
      if (0 >= version || 0 >= disk_no)
      {
        TBSYS_LOG(WARN, "write image invalid argument, "
            "disk_no =%d <0 or version=%ld < 0", disk_no, version);
        ret = OB_INVALID_ARGUMENT;
      }
      else if (!has_match_version(version))
      {
        TBSYS_LOG(WARN, "write image version=%ld don't match", version);
        ret = OB_ERROR;
      }
      else
      {
        {
          tbsys::CRLockGuard guard(lock_);
          ret = get_image(version).prepare_write_meta(disk_no);
        }
        if (OB_SUCCESS == ret)
        {
          ret = get_image(version).write_meta(idx_path, disk_no);
        }
      }
      return ret;
    }

    int ObMultiVersionTabletImage::sync_all_images(const int32_t* disk_no_array,
      const int32_t size)
    {
      int ret = OB_SUCCESS;

      if (NULL == disk_no_array || size <= 0)
      {
        TBSYS_LOG(WARN, "sync all images invalid argument, "
            "disk_no_array=%p is NULL or size=%d < 0", disk_no_array, size);
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        int64_t newest_version = get_newest_version();
        int64_t eldest_version = get_eldest_version();

        //sync the eldest image first
        if (0 < eldest_version)
        {
          for (int32_t i = 0; i < size; ++i)
          {
            ret = write(eldest_version, disk_no_array[i]);
            // TODO write failed on one disk.
            if (OB_SUCCESS != ret)
            {
              TBSYS_LOG(WARN, "write eldest meta info to disk=%d failed, version=%ld",
                  disk_no_array[i], eldest_version);
              continue;
            }
          }
        }
        else
        {
          TBSYS_LOG(WARN, "eldest_version:%ld <= 0", eldest_version);
        }

        //sync the newest image if necessary
        if (newest_version != eldest_version)
        {
          if (0 < newest_version)
          {
            for (int32_t i = 0; i < size; ++i)
            {
              ret = write(newest_version, disk_no_array[i]);
              // TODO write failed on one disk.
              if (OB_SUCCESS != ret)
              {
                TBSYS_LOG(WARN, "write newest meta info to disk=%d failed, version=%ld",
                    disk_no_array[i], newest_version);
                continue;
              }
            }
          }
          else
          {
            TBSYS_LOG(WARN, "newest_version:%ld <= 0", newest_version);
          }
        }
      }

      return ret;
    }

    int ObMultiVersionTabletImage::read(const char* idx_path,
        const int64_t version, const int32_t disk_no, const bool load_sstable)
    {
      int ret = OB_SUCCESS;
      if (0 >= version || 0 >= disk_no)
      {
        TBSYS_LOG(ERROR, "read image invalid argument, "
            "disk_no =%d <0 or version=%ld < 0", disk_no, version);
        ret = OB_INVALID_ARGUMENT;
      }
      else if (OB_SUCCESS != (ret = prepare_tablet_image(version, true)))
      {
        TBSYS_LOG(ERROR, "cannot prepare image, version=%ld", version);
      }
      else if (OB_SUCCESS != (ret = get_image(version).read(
              idx_path, disk_no, load_sstable)) )
      {
        TBSYS_LOG(ERROR, "read idx file = %s , disk_no = %d, version = %ld error",
            idx_path, disk_no, version);
      }
      return ret;
    }

    int ObMultiVersionTabletImage::read(const int64_t version,
        const int32_t disk_no, const bool load_sstable)
    {
      int ret = OB_SUCCESS;
      char idx_path[OB_MAX_FILE_NAME_LENGTH];

      if ( OB_SUCCESS != (ret = get_meta_path(version,
              disk_no, true, idx_path, OB_MAX_FILE_NAME_LENGTH)) )
      {
        TBSYS_LOG(ERROR, "get meta file path version = %ld, disk_no = %d error",
            version, disk_no);
      }
      else if ( OB_SUCCESS != (ret = read(idx_path,
              version, disk_no, load_sstable)) )
      {
        TBSYS_LOG(ERROR, "read idx file =%s version = %ld, disk_no = %d error",
            idx_path, version, disk_no);
      }
      return ret;
    }

    int ObMultiVersionTabletImage::load_tablets(const int32_t* disk_no_array,
        const int32_t size, const bool load_sstable)
    {
      int ret = OB_SUCCESS;
      if (NULL == disk_no_array || size <= 0)
      {
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        char idx_dir_path[OB_MAX_FILE_NAME_LENGTH];
        struct dirent **idx_dirent = NULL;
        const char* idx_file_name = NULL;
        int64_t idx_file_num = 0;
        int64_t version = 0;
        int32_t disk_no = 0;

        for (int32_t i = 0; i < size && (OB_SUCCESS == ret || OB_CS_EAGAIN == ret); ++i)
        {
          disk_no = disk_no_array[i];
          ret = get_sstable_directory(disk_no, idx_dir_path, OB_MAX_FILE_NAME_LENGTH);
          if (OB_SUCCESS != ret)
          {
            TBSYS_LOG(WARN, "get sstable directory disk %d failed", disk_no);
            ret = OB_ERROR;
            break;
          }

          idx_file_num = ::scandir(idx_dir_path, &idx_dirent, idx_file_name_filter, ::versionsort);
          if (idx_file_num <= 0)
          {
            TBSYS_LOG(INFO, "idx directory %s has no idx files.", idx_dir_path);
            continue;
          }

          for (int n = 0; n < idx_file_num; ++n)
          {
            idx_file_name = idx_dirent[n]->d_name;
            // idx_file_name likes "idx_[version]_[disk]
            ret = sscanf(idx_file_name, "idx_%ld_%d", &version, &disk_no);
            if (ret < 2)
            {
              ret = OB_SUCCESS;
            }
            else if (disk_no != disk_no_array[i])
            {
              TBSYS_LOG(ERROR, "disk no = %d in idx file name cannot match with disk=%d ",
                  disk_no, disk_no_array[i]);
              ret = OB_ERROR;
            }
            else
            {
              ret = read(version, disk_no, load_sstable);
            }

            ::free(idx_dirent[n]);
          }

          ::free(idx_dirent);
          idx_dirent = NULL;

        }  // end for

      } // end else

      if (OB_CS_EAGAIN == ret)
      {
        // ignore OB_CS_EAGAIN error.
        // there's old meta index file remains sstable dir
        // but read first, it will be replaced by newer tablets,
        ret = OB_SUCCESS;
      }


      if (OB_SUCCESS == ret)
      {
        if (initialize_service_index() < 0)
        {
          TBSYS_LOG(ERROR, "search service tablet image failed,"
              "service_index_=%ld", service_index_);
          ret = OB_ERROR;
        }
        else if (OB_SUCCESS != (ret = adjust_inconsistent_tablets()))
        {
          TBSYS_LOG(ERROR, "failed to adjust inconsistent tablets");
        }
        else if (has_tablet(service_index_))
        {
          TBSYS_LOG(INFO, "current service tablet version=%ld",
              image_tracker_[service_index_]->data_version_);
        }
      }



      return ret;
    }

    int64_t ObMultiVersionTabletImage::initialize_service_index()
    {
      // get eldest index;
      int64_t eldest_index = (newest_index_ + 1) % MAX_RESERVE_VERSION_COUNT;
      int64_t index = eldest_index;
      service_index_ = index;

      do
      {
        if (index < 0 || index >= MAX_RESERVE_VERSION_COUNT)
        {
          service_index_ = -1;
          break;
        }

        if (OB_SUCCESS != tablets_all_merged(index))
        {
          service_index_ = index;
          break;
        }

        index = (index + 1) % MAX_RESERVE_VERSION_COUNT;

      } while (index != eldest_index);


      return service_index_;
    }

    int ObMultiVersionTabletImage::get_split_old_tablet(const ObTablet* const new_split_tablet, ObTablet* &old_split_tablet)
    {
      tbsys::CRLockGuard guard(lock_);

      ObTablet* old_tablet = NULL;
      int ret = OB_CS_TABLET_NOT_EXIST;
      int64_t index = get_index(new_split_tablet->get_data_version() - 1);
      if (index < 0 || index >= MAX_RESERVE_VERSION_COUNT)
      {
        TBSYS_LOG(ERROR, "index=%ld out of range.", index);
        ret = OB_ERROR;
      }
      else if (has_tablet(index) &&
          image_tracker_[index]->data_version_ == new_split_tablet->get_data_version() - 1)
      {
        int err = OB_SUCCESS;
        err = image_tracker_[index]->acquire_tablet(
            new_split_tablet->get_range(), SCAN_FORWARD, old_tablet);

        if(OB_SUCCESS == err &&
            !old_tablet->get_range().equal(new_split_tablet->get_range()) &&
            old_tablet->get_range().intersect(new_split_tablet->get_range()))
        {
          old_split_tablet = old_tablet;
          ret = OB_SUCCESS;
        }
        else if(OB_SUCCESS == err && NULL != old_tablet)
        {
          image_tracker_[index]->release_tablet(old_tablet);
        }
      }

      return ret;
    }

    int ObMultiVersionTabletImage::adjust_inconsistent_tablets()
    {
      int ret = OB_SUCCESS;

      /**
       * if chunkserver is killed in daily merge stage, maybe the two
       * version index file isn't consistent. for example, the
       * is_merged flag is true in eldest version index file, but
       * there is no new corresponding tablet in newest version index
       * file, this function will handle this case.
       */
      if (service_index_ >= 0 && service_index_ < MAX_RESERVE_VERSION_COUNT
          && service_index_ != newest_index_
          && has_tablet(service_index_) && has_tablet(newest_index_))
      {
        ObSortedVector<ObTablet*> & tablet_list = image_tracker_[service_index_]->tablet_list_;
        ObSortedVector<ObTablet*>::iterator it = tablet_list.begin();
        for (; it != tablet_list.end(); ++it)
        {
          if ((*it)->is_merged() && !(*it)->is_removed())
          {
            if (!is_two_version_tablet_consistent((*it)->get_range()))
            {
              (*it)->set_merged(0);
              TBSYS_LOG(WARN, "maybe chunkserver is killed in daily merge stage, "
                              "tablet in eldest image is merged, but no corresponding "
                              "tablet in newest image, just change the is_merged flag "
                              "to flase, range=%s",
                to_cstring((*it)->get_range()));
              ret = OB_SUCCESS;
            }
          }
        }
      }

      return ret;
    }


    bool ObMultiVersionTabletImage::is_two_version_tablet_consistent(
       const ObNewRange& old_range)
    {
      bool bret = true;
      int err = OB_SUCCESS;
      ObNewRange next_split_range = old_range;
      ObTablet* tablet = NULL;

      /**
       * in tablet image, all the tablet range is left open and right close, so 
       * we needn't check border flag. there is no tablet merging in daily 
       * merge process, just only tablet spliting, so we check 3 case: 
       * 1. range is the same in two version 
       * 2. the new range is not the first splited range of the old range 
       * 3. check if the rest splited tablets are in new version tablet image
       */
      while (OB_SUCCESS == err)
      {
        err = image_tracker_[newest_index_]->acquire_tablet(
          next_split_range, SCAN_FORWARD, tablet);
        if (OB_SUCCESS == err && NULL != tablet)
        {
          if (next_split_range == tablet->get_range())
          {
            //tablet isn't splited, range is the same in two version, it's consistent
            break;
          }
          else if (next_split_range.compare_with_startkey2(tablet->get_range()) < 0)
          {
            //the first splited part lost
            ObNewRange lost_range = next_split_range;
            lost_range.end_key_ = tablet->get_range().start_key_;
            TBSYS_LOG(WARN, "splited tablet range lost, lost_range=%s, old_range=%s, "
                            "acquired_new_range=%s",
                      to_cstring(lost_range), to_cstring(old_range), 
                      to_cstring(tablet->get_range()));
            bret = false;
            break;
          }
          else if (next_split_range.compare_with_endkey2(tablet->get_range()) < 0)
          {
            //no tablet merging in daily merge, it can't happen
            TBSYS_LOG(WARN, "splited tablet range of new version cross previous version "
                            "tablet range bound, old_range=%s, acquired_new_range=%s",
                      to_cstring(old_range), to_cstring(tablet->get_range()));
            bret = false;
            break;
          }
          else
          {
            //tablet splited, new range is the first splited part, 
            //check next splited part is in the new version tablet image
            next_split_range.start_key_ = tablet->get_range().end_key_;
            if (next_split_range.empty())
            {
              //checked all splited tablet range, it's consistent, return true
              break;
            }
          }
          image_tracker_[newest_index_]->release_tablet(tablet);
          tablet = NULL;
        }
        else
        {
          //no corresponding tablet in new version tablet image
          bret = false;
          break;
        }
      }

      if (NULL != tablet)
      {
        image_tracker_[newest_index_]->release_tablet(tablet);
        tablet = NULL;
      }

      return bret;
    }

    int ObMultiVersionTabletImage::begin_scan_tablets()
    {
      int ret = OB_SUCCESS;
      if (!iterator_.is_stop())
      {
        TBSYS_LOG(WARN, "scan in progress, cannot luanch another, cur_vi_=%ld", iterator_.cur_vi_);
        ret = OB_CS_EAGAIN;
      }
      else if (OB_SUCCESS != (ret = iterator_.start()))
      {
        TBSYS_LOG(WARN, "start failed ret = %d", ret);
      }
      else
      {
        TBSYS_LOG(INFO, "start succeed vi=%ld, ti=%ld", iterator_.cur_vi_, iterator_.cur_ti_);
      }
      return ret;
    }

    int ObMultiVersionTabletImage::get_next_tablet(ObTablet* &tablet)
    {
      int ret = OB_SUCCESS;
      tablet = NULL;
      if (iterator_.is_stop())
      {
        TBSYS_LOG(ERROR, "not a initialized scan process, call begin_scan_tablets first");
        ret = OB_ERROR;
      }
      else if (iterator_.is_end())
      {
        ret = OB_ITER_END;
      }
      else
      {
        if (OB_SUCCESS == ret)
        {
          tablet = iterator_.get_tablet();
          if (NULL == tablet)
          {
            ret = OB_ERROR;
          }
          else
          {
            iterator_.next();
          }
        }
      }
      return ret;
    }

    int ObMultiVersionTabletImage::end_scan_tablets()
    {
      int ret = OB_SUCCESS;
      if (iterator_.is_stop())
      {
        TBSYS_LOG(WARN, "scans not begin.");
        ret = OB_ERROR;
      }
      else
      {
        ret = iterator_.end();
      }
      return ret;
    }

    int64_t ObMultiVersionTabletImage::get_max_sstable_file_seq() const
    {
      int64_t max_seq = 0;
      for (int32_t index = 0; index < MAX_RESERVE_VERSION_COUNT; ++index)
      {
        if (has_tablet(index) )
        {
          int64_t seq = image_tracker_[index]->get_max_sstable_file_seq();
          if (seq > max_seq) max_seq = seq;
        }
      }
      return max_seq;
    }

    void ObMultiVersionTabletImage::dump(const bool dump_sstable) const
    {
      int64_t eldest_index = get_eldest_index();
      int64_t index = eldest_index;
      do
      {
        if (index < 0 || index >= MAX_RESERVE_VERSION_COUNT)
        {
          break;
        }

        if (has_tablet(index))
        {
          image_tracker_[index]->dump(dump_sstable);
        }
        index = (index + 1) % MAX_RESERVE_VERSION_COUNT;
      } while (index != eldest_index);
    }

    int ObMultiVersionTabletImage::serialize(
        const int32_t index, const int32_t disk_no, char* buf, const int64_t buf_len, int64_t &pos)
    {
      int ret = OB_SUCCESS;
      if (index < 0 || index >= MAX_RESERVE_VERSION_COUNT || disk_no <= 0)
      {
        TBSYS_LOG(WARN, "invalid argument index= %d, disk_no=%d", index, disk_no);
        ret = OB_INVALID_ARGUMENT;
      }
      else if (!has_tablet(index))
      {
        TBSYS_LOG(WARN, "has no tablet index= %d, disk_no=%d", index, disk_no);
        ret = OB_ERROR;
      }
      else
      {
        ret = image_tracker_[index]->serialize(disk_no, buf, buf_len, pos);
      }

      return ret;
    }

    int ObMultiVersionTabletImage::deserialize(
        const int32_t disk_no, const char* buf, const int64_t buf_len, int64_t &pos)
    {
      int ret = OB_SUCCESS;
      int64_t origin_payload_pos = 0;
      ObTabletMetaHeader meta_header;

      if ( OB_SUCCESS != (ret = ObRecordHeader::check_record(
              buf + pos, buf_len, ObTabletMetaHeader::TABLET_META_MAGIC)) )
      {
        TBSYS_LOG(ERROR, "check common record header failed, disk_no=%d", disk_no);
      }
      else
      {
        pos += OB_RECORD_HEADER_LENGTH;
        origin_payload_pos = pos;
        ret = meta_header.deserialize(buf, buf_len, pos);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR, "deserialize meta header failed, disk_no=%d", disk_no);
        }
        else if (OB_SUCCESS != (ret = prepare_tablet_image(meta_header.data_version_, false)))
        {
          TBSYS_LOG(ERROR, "prepare_tablet_image header.version=%ld error",
              meta_header.data_version_);
          ret = OB_ERROR;
        }
        else
        {
          pos = 0;
          get_image(meta_header.data_version_).deserialize(disk_no, buf, buf_len, pos);
        }
      }
      return ret;
    }

    //----------------------------------------
    // class ObMultiVersionTabletImage::Iterator
    //----------------------------------------
    int ObMultiVersionTabletImage::Iterator::start()
    {
      int ret = OB_SUCCESS;

      tbsys::CRLockGuard guard(image_.lock_);
      cur_vi_ = image_.service_index_;
      cur_ti_ = 0;
      start_vi_ = cur_vi_;
      end_vi_ = image_.service_index_;
      if (cur_vi_ >= 0 && cur_vi_ < ObMultiVersionTabletImage::MAX_RESERVE_VERSION_COUNT
          && image_.has_tablet(cur_vi_))
      {
        ret = image_.image_tracker_[cur_vi_]->acquire_tablets(
          0, image_.image_tracker_[cur_vi_]->report_tablet_list_);
        if (OB_SUCCESS == ret && !image_.has_report_tablet(cur_vi_))
        {
          ret = OB_ITER_END;
        }
      }
      else
      {
        ret = OB_ITER_END;
      }
      return ret;
    }

    int ObMultiVersionTabletImage::Iterator::next()
    {
      int ret = OB_SUCCESS;
      if (is_end())
      {
        ret = OB_ITER_END;
      }
      else
      {
        if (is_last_tablet())
        {
          // reach last tablet of service version  tablet image
          // set cur_ti_ to end position, for next() call lately.
          ++cur_ti_;
          ret = OB_ITER_END;
        }
        else
        {
          // reach middle of tablet of service version tablet image.
          ++cur_ti_;
        }
      }
      return ret;
    }

    const char* ObMultiVersionTabletImage::print_tablet_image_stat() const
    {
      static const int64_t STAT_BUF_SIZE = 1024;
      static __thread char buf[STAT_BUF_SIZE];
      int64_t pos = 0;
      int64_t eldest_index = get_eldest_index();
      int64_t tablet_num = 0;
      int64_t merged_num = 0;

      pos += snprintf(buf + pos, STAT_BUF_SIZE - pos,
                      "\n\t==========> print tablet image status, merge process start <==========\n"
                      "\t%10s %20s %20s %15s\n",
                      "version", "total_tablet_count", "merged_tablet_count", "merge_process");
      if (has_tablet(eldest_index))
      {
        tablet_num = image_tracker_[eldest_index]->get_tablets_num();
        merged_num = image_tracker_[eldest_index]->get_merged_tablet_count();
        pos += snprintf(buf + pos, STAT_BUF_SIZE - pos,
                        "\t%10ld %20ld %20ld %15.2f%%\n",
                        image_tracker_[eldest_index]->get_data_version(),
                        tablet_num, merged_num,
                        (double)merged_num / (double)tablet_num * 100);
      }
      if (eldest_index != newest_index_ && has_tablet(newest_index_))
      {
        pos += snprintf(buf + pos, STAT_BUF_SIZE - pos,
                        "\t%10ld %20d %20ld\n",
                        image_tracker_[newest_index_]->get_data_version(),
                        image_tracker_[newest_index_]->get_tablets_num(),
                        image_tracker_[newest_index_]->get_merged_tablet_count());
      }
      pos += snprintf(buf + pos, STAT_BUF_SIZE - pos,
                      "\t==========> print tablet image status, "
                      "merge process end <============");

      return buf;
    }

    void ObMultiVersionTabletImage::get_image_stat(ObTabletImageStat& stat)
    {
      int64_t eldest_index = get_eldest_index();

      memset(&stat, 0, sizeof(ObTabletImageStat));
      if (has_tablet(eldest_index))
      {
        stat.old_ver_tablets_num_ = image_tracker_[eldest_index]->get_tablets_num();
        stat.old_ver_merged_tablets_num_ = image_tracker_[eldest_index]->get_merged_tablet_count();
      }
      if (eldest_index != newest_index_ && has_tablet(newest_index_))
      {
        stat.new_ver_tablets_num_ = image_tracker_[newest_index_]->get_tablets_num();
      }
    }


    int ObMultiVersionTabletImage::delete_table(const uint64_t table_id)
    {
      int ret = OB_SUCCESS;
      int64_t index = -1;

      if (OB_INVALID_ID == table_id)
      {
        TBSYS_LOG(WARN, "delete_table invalid argument, table_id = %lu",
          table_id);
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        {
          tbsys::CRLockGuard guard(lock_);
          index = service_index_;
          if (index < 0 || index >= MAX_RESERVE_VERSION_COUNT || !has_tablet(index))
          {
            TBSYS_LOG(WARN, "image (index=%ld) not initialized, has no tablets", index);
            index = -1;
          }
        }

        if (index >= 0)
        {
          ret = image_tracker_[index]->delete_table(table_id, lock_);
        }
      }

      return ret;
    }

  } // end namespace chunkserver
} // end namespace oceanbase
