/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *
 *         ob_sstable_reader_i.h is for what ...
 *
 *  Version: $Id: ob_sstable_reader_i.h 01/04/2013 04:56:29 PM qushan Exp $
 *
 *  Authors:
 *     qushan < qushan@taobao.com >
 *        - some work details if you want
 */



#ifndef OCEANBASE_SSTABLE_OB_SSTABLE_READER_I_H_
#define OCEANBASE_SSTABLE_OB_SSTABLE_READER_I_H_

#include "common/ob_define.h"

class ObCompressor;
namespace oceanbase
{
  namespace common
  {
    class ObNewRange;
    class BloomFilter;
  }
  namespace sstable
  {
    class SSTableReader
    {
      public:
        static const int64_t NORMAL_SSTABLE_VERSION = 1;
        static const int64_t ROWKEY_SSTABLE_VERSION = 2;
        static const int64_t COMPACT_SSTABLE_VERSION = 3;
        virtual ~SSTableReader() {}
        virtual int open(const int64_t sstable_id, const int64_t version) = 0;
        virtual void reset() = 0;
        virtual const common::ObNewRange& get_range() const = 0;
        virtual int64_t get_row_count() const = 0;
        virtual int64_t get_sstable_size() const = 0;
        virtual int64_t get_sstable_checksum() const = 0;
        virtual ObCompressor* get_decompressor() = 0;
        virtual const common::BloomFilter* get_bloom_filter() const = 0;
    };

  }
}


#endif
