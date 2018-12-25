/**
 * (C) 2010-2011 Taobao Inc.
 *  
 *  This program is free software; you can redistribute it
 *  and/or modify it under the terms of the GNU General Public
 *  License version 2 as published by the Free Software
 *  Foundation.
 *
 *         ob_disk_path.h is for what ...
 *
 *  Authors:
 *     qushan < qushan@taobao.com >
 *        
 */

#ifndef OCEANBASE_SSTABLE_DISK_PATH_H__
#define OCEANBASE_SSTABLE_DISK_PATH_H__

#include <stdint.h>
#include <dirent.h>
#include "common/serialization.h"
#include "common/murmur_hash.h"

namespace
{
  const uint64_t DISK_NO_MASK = ((1UL << 8) - 1);
}

namespace oceanbase
{
  namespace sstable 
  {
    struct ObSSTableId
    {
      ObSSTableId():sstable_file_id_(0),sstable_file_offset_(0) {}
      ObSSTableId(uint64_t id):sstable_file_id_(id),sstable_file_offset_(0) {}
      uint64_t sstable_file_id_;
      int64_t sstable_file_offset_;

      bool operator < (const ObSSTableId& rhs) const
      {
        return ((sstable_file_id_ < rhs.sstable_file_id_) || \
          ((sstable_file_id_ == rhs.sstable_file_id_) && \
           (sstable_file_offset_ < rhs.sstable_file_offset_)));
      }

      bool operator == (const ObSSTableId& rhs) const
      {
        return (sstable_file_id_ == rhs.sstable_file_id_) &&
          (sstable_file_offset_ == rhs.sstable_file_offset_);
      }

      int64_t hash() const
      {
        return common::murmurhash2(this, sizeof(ObSSTableId), 0);
      };

      int serialize(char *buf,int64_t buf_len,int64_t& pos) const
      {
        int64_t p = pos;
        int ret = common::OB_ERROR;
        if ((common::OB_SUCCESS == common::serialization::encode_vi64(
             buf,buf_len,p,static_cast<int64_t>(sstable_file_id_))) 
            && (common::OB_SUCCESS == common::serialization::encode_vi64(
              buf,buf_len,p,sstable_file_offset_)))
        {
          pos = p;
          ret = common::OB_SUCCESS;
        }
        return ret;
      }

      int deserialize(const char *buf,int64_t data_len,int64_t& pos)
      {
        int64_t p = pos;
        int ret = common::OB_ERROR;
        int64_t id = 0;
        if ((common::OB_SUCCESS == common::serialization::decode_vi64(buf,data_len,p,&id)) 
            && (common::OB_SUCCESS == common::serialization::decode_vi64(
              buf,data_len,p,&sstable_file_offset_)))
        {
          sstable_file_id_ = static_cast<uint64_t>(id);
          pos = p;
          ret = common::OB_SUCCESS;
        }
        return ret;
      }

      int64_t get_serialize_size() const
      {
        return common::serialization::encoded_length_vi64(static_cast<int64_t>(sstable_file_id_))
          + common::serialization::encoded_length_vi64(sstable_file_offset_);
      }
    };
	
    uint64_t get_ups_sstable_disk_no(const uint64_t sstable_file_id);//add zhaoqiong [fixed for Backup]:20150811

    uint64_t get_sstable_disk_no(const uint64_t sstable_file_id);
    /**
     * get sstable absolute path base on sstable file id.
     * @param sstable_id file_id
     * @param [out] path sstable file path
     * @param path_len length of %path
     */
    int get_sstable_path(const ObSSTableId& sstable_id, char *path, const int64_t path_len);

    /**
     * get sstable file directory base on disk no of sstable
     * @param disk_no disk no where sstable file in.
     * @param [out] path sstable file path
     * @param path_len length of %path
     */
    int get_sstable_directory(const int32_t disk_no, char *path, const int64_t path_len);
    int get_sstable_directory(const char* disk_path, char *path, const int64_t path_len);

    int get_recycle_directory(const int32_t disk_no, char *path, const int64_t path_len);
    int get_recycle_path(const ObSSTableId& sstable_id, char *path, const int64_t path_len);
    int get_recycle_directory(const char* disk_path, char *path, const int64_t path_len);

    /**
     * get tablet index file path
     */
    int get_tmp_meta_path(const int32_t disk_no, char *path, const int32_t path_len);
    int get_meta_path(const int32_t disk_no, const bool current, char *path, const int32_t path_len);
    int get_meta_path(const int64_t version, const int32_t disk_no, const bool current, char *path, const int32_t path_len);

    /**
     * get sstable file bypass directory base on disk no of sstable
     * @param disk_no disk no where sstable file in.
     * @param [out] path sstable file path
     * @param path_len length of %path
     */
    int get_bypass_sstable_directory(const int32_t disk_no, char *path, const int64_t path_len);
    int get_bypass_sstable_directory(const char* disk_path, char *path, const int64_t path_len);
    int get_bypass_sstable_path(const int32_t disk_no, 
      const char* sstable_name, char *path, const int64_t path_len);

    int idx_file_name_filter(const struct dirent *d);
    int bak_idx_file_name_filter(const struct dirent *d);
  }
}

#endif //OCEANBASE_SSTABLE_DISK_PATH_H__
