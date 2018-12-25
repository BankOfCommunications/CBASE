/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *  
 * ob_ups_stat.cpp for statistic sstable for ups. 
 *
 * Authors:
 *   huating <huating.zmq@taobao.com>
 *
 */
#include "common/ob_define.h"
#include "common/ob_common_stat.h"

using namespace oceanbase::common;

#ifndef NO_STAT
namespace oceanbase
{
  namespace sstable
  {
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
  }
}
#endif
