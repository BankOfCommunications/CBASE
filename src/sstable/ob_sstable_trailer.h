/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *  
 * ob_sstable_trailer.h for define sstable trailer. 
 *
 * Authors:
 *   huating <huating.zmq@taobao.com>
 *
 */
#ifndef OCEANBASE_SSTABLE_SSTABLE_TRAILER_H_
#define OCEANBASE_SSTABLE_SSTABLE_TRAILER_H_

#include "string.h"
#include "common/ob_define.h"
#include "common/ob_string.h"
#include "common/ob_malloc.h"
#include "common/serialization.h"
#include "common/ob_range2.h"
#include "ob_sstable_block_builder.h"

namespace oceanbase
{
  namespace common
  {
    class ObRowkeyInfo;
  }
  namespace sstable
  { 
    static const int OB_SSTABLE_STORE_DENSE = 1;
    static const int OB_SSTABLE_STORE_SPARSE = 2;
    static const int OB_SSTABLE_STORE_MIXED = 3;
     
    //add 64 bit to desc the trailer
    struct ObTrailerOffset
    { 
      int64_t trailer_record_offset_;
      NEED_SERIALIZE_AND_DESERIALIZE;
    };

    struct ObTrailerParam
    {
      common::ObString compressor_name_;
      int64_t table_version_;
      int store_type_;
      int64_t block_size_;
      int64_t frozen_time_;

      ObTrailerParam() 
      : table_version_(0), store_type_(OB_SSTABLE_STORE_DENSE),
        block_size_(ObSSTableBlockBuilder::SSTABLE_BLOCK_SIZE), frozen_time_(0)
      {
      }
    };
    
    //this class is for sstable trailer,you should read it first, then do other things
    class ObSSTableTrailer
    {
    public:
      static const int32_t RESERVED_LEN = 60;
      static const int64_t DEFAULT_KEY_BUF_SIZE = 8192; //8k
      static const int32_t SSTABLEV3  = 0x300;
      static const int32_t SSTABLEV2  = 0x200;
      static const int32_t SSTABLEV1  = 0x0;

      ObSSTableTrailer();
      ~ObSSTableTrailer();
      
      //------set and get--------------------------
      //you should first use serialize or deserialize.
      // After that, size_ will be set.
      const int32_t get_size() const;
      
      const int32_t get_trailer_version() const;
      int set_trailer_version(const int32_t version);

      const int64_t get_table_version() const;
      int set_table_version(const int64_t version);
      
      const int64_t get_block_count() const;
      int set_block_count(const int64_t count);
      
      const int64_t get_block_index_record_offset() const;
      int set_block_index_record_offset(const int64_t offset);
      
      const int64_t get_block_index_record_size() const;
      int set_block_index_record_size(const int64_t size);
      
      const int64_t get_bloom_filter_hash_count() const;
      int set_bloom_filter_hash_count(const int64_t count);
      const int64_t get_bloom_filter_record_offset() const; 
      int set_bloom_filter_record_offset(const int64_t offset);
      const int64_t get_bloom_filter_record_size() const;   
      int set_bloom_filter_record_size(const int64_t size);
      
      const int64_t get_schema_record_offset() const;
      int set_schema_record_offset(const int64_t offset);
      const int64_t get_schema_record_size() const;
      int set_schema_record_size(const int64_t size);
      
      const int64_t get_block_size() const;
      int set_block_size(const int64_t size);
      
      const int64_t get_row_count() const;
      int set_row_count(const int64_t count);

      const uint64_t get_sstable_checksum() const;
      void set_sstable_checksum(uint64_t sstable_checksum);
      
      const char *get_compressor_name() const;
      int set_compressor_name(const char* name);
      
      const int16_t get_row_value_store_style() const;
      int set_row_value_store_style(const int16_t style);
      
      const int64_t get_first_block_data_offset() const;
      int set_first_block_data_offset(const int64_t offset);

      const uint64_t get_first_table_id() const;
      int set_first_table_id(const uint64_t table_id);

      const int64_t get_frozen_time() const;
      int set_frozen_time(const int64_t frozen_time);
      
      void reset();

      bool is_valid();
      void dump() const;

      const int64_t get_range_record_offset() const;
      int set_range_record_offset(const int64_t offset);
      const int64_t get_range_record_size() const;
      int set_range_record_size(const int64_t size);
      
      const common::ObNewRange& get_range() const;
      int set_range(const common::ObNewRange& range);
      int copy_range(const common::ObNewRange& range);

      NEED_SERIALIZE_AND_DESERIALIZE;
      
    private:
      DISALLOW_COPY_AND_ASSIGN(ObSSTableTrailer);

    private:
      mutable int32_t size_;              //ObSstableTrailer size
      int32_t trailer_version_;           //trailer version
      int64_t table_version_;             //table version
      int64_t first_block_data_offset_;   //first block offset in sstable
      int64_t block_count_;               //block count in sstable
      int64_t block_index_record_offset_; //block index record offset
      int64_t block_index_record_size_;   //block index size, includes record header
      int64_t bloom_filter_hash_count_;   //hash count of bloom filter
      int64_t bloom_filter_record_offset_;//bloom filter record offset
      int64_t bloom_filter_record_size_;  //bloom filter size, includes record header
      int64_t schema_record_offset_;      //schema record offset in sstable
      int64_t schema_record_size_;        //scheam record size, includes record header        
      int64_t block_size_;                //block size      
      int64_t row_count_;                 //row count in sstable
      uint64_t sstable_checksum_;         //checksum of sstable
      uint64_t first_table_id_;           //first table in sstable schema
      int64_t frozen_time_;               //frozen time 
      int64_t range_record_offset_;       //range stream record offset in sstable
      int64_t range_record_size_;         //range record size, includes record header
      int64_t reserved64_[RESERVED_LEN];  //reserved, must be 0
      char compressor_name_[common::OB_MAX_COMPRESSOR_NAME_LENGTH]; //compressor name
      int16_t row_value_store_style_;     //dense,sparse or mixed
      int16_t reserved_;                  //reversed, must be 0

      //varialbes only in memory, no serialization
      common::ObMemBuf start_key_buf_;
      common::ObMemBuf end_key_buf_;
      common::ObNewRange range_;          //tablet range of this sstable
    };
  }
}

#endif  //OCEANBASE_SSTABLE_SSTABLE_TRAILER_H_
