/*
 *  (C) 2007-2010 Taobao Inc.
 *  
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License version 2 as
 *  published by the Free Software Foundation.
 *
 *         ob_disk_path.cpp is for what ...
 *
 *  Version: $Id: ob_disk_path.cpp 2011年03月22日 09时54分22秒 qushan Exp $
 *
 *  Authors:
 *     qushan < qushan@taobao.com >
 *        - some work details if you want
 */

#include "common/ob_define.h"
#include "common/ob_common_stat.h"
#include "sstable/ob_disk_path.h"

using namespace oceanbase::common;

namespace oceanbase 
{
  namespace sstable
  {
    uint64_t get_sstable_disk_no(const uint64_t sstable_file_id)
    {
      return (sstable_file_id & DISK_NO_MASK);
    }

    int get_sstable_directory(const int32_t disk_no, char *path, const int64_t path_len)
    {
      int ret = OB_SUCCESS;
      if (disk_no < 0 || NULL == path || path_len < 0)
      {
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        // read from configure file
        snprintf(path, path_len, "%s/%d", "./tmp", disk_no);
      }
      return ret;
    }

    int get_sstable_path(const ObSSTableId& sstable_id, char *path, const int64_t path_len)
    {
      int ret = OB_SUCCESS;
      if (sstable_id.sstable_file_offset_ < 0 || NULL == path || path_len < 0)
      {
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        // read from configure file
        snprintf(path, path_len, "%s/%lu/%ld", "./tmp",
                 get_sstable_disk_no(sstable_id.sstable_file_id_), sstable_id.sstable_file_id_);
      }
      return ret;
    }

    int get_meta_path(const int32_t disk_no, const bool current, char *path, const int32_t path_len)
    {
      int ret = OB_SUCCESS;
      UNUSED(current);
      if (disk_no <= 0 || NULL == path ||  path_len <= 0)
      {
        ret = OB_INVALID_ARGUMENT;
      }
      if (OB_SUCCESS == ret)
      {
        snprintf(path, path_len, "idx_%d", disk_no);
      }
      return ret;
    }

    int get_meta_path(const int64_t version, const int32_t disk_no, 
        const bool current, char *path, const int32_t path_len)
    {
      int ret = OB_SUCCESS;
      UNUSED(current);
      if (disk_no <= 0 || NULL == path ||  path_len <= 0)
      {
        ret = OB_INVALID_ARGUMENT;
      }
      if (OB_SUCCESS == ret)
      {
        snprintf(path, path_len, "./tmp/%d/idx_%ld_%d", disk_no, version, disk_no);
      }
      return ret;
    }

#ifndef NO_STAT
    void set_stat(const uint64_t table_id, const int32_t index, const int64_t value)
    {
      UNUSED(table_id);
      UNUSED(index);
      UNUSED(value);

      //TODO: add your handle code
      switch (index)
      {
      case INDEX_BLOCK_INDEX_CACHE_HIT:
        break;
      case INDEX_BLOCK_INDEX_CACHE_MISS:
        break;
      case INDEX_BLOCK_CACHE_HIT:
        break;
      case INDEX_BLOCK_CACHE_MISS:
        break;
      case INDEX_DISK_IO_READ_NUM:
        break;
      case INDEX_DISK_IO_WRITE_NUM:
        break;
      case INDEX_DISK_IO_READ_BYTES:
        break;
      case INDEX_DISK_IO_WRITE_BYTES:
        break;
      default:
        break;
      }
    }

    void inc_stat(const uint64_t table_id, const int32_t index, const int64_t inc_value)
    {
      UNUSED(table_id);
      UNUSED(index);
      UNUSED(inc_value);

      //TODO: add your handle code
      switch (index)
      {
      case INDEX_BLOCK_INDEX_CACHE_HIT:
        break;
      case INDEX_BLOCK_INDEX_CACHE_MISS:
        break;
      case INDEX_BLOCK_CACHE_HIT:
        break;
      case INDEX_BLOCK_CACHE_MISS:
        break;
      case INDEX_DISK_IO_READ_NUM:
        break;
      case INDEX_DISK_IO_WRITE_NUM:
        break;
      case INDEX_DISK_IO_READ_BYTES:
        break;
      case INDEX_DISK_IO_WRITE_BYTES:
        break;
      default:
        break;
      }
    }
#endif
  }
}
