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
#ifndef OCEANBASE_SSTABLE_SSTABLE_TRAILERV1_H_
#define OCEANBASE_SSTABLE_SSTABLE_TRAILERV1_H_

#include "string.h"
#include "common/ob_define.h"
#include "common/ob_string.h"
#include "common/ob_malloc.h"
#include "common/serialization.h"
#include "sstable/ob_sstable_trailer.h"

namespace oceanbase
{
  namespace sstable
  { 
     static const int OB_SSTABLE_STORE_DENSEV1  = 1;
     static const int OB_SSTABLE_STORE_SPARSEV1 = 2;
     static const int OB_SSTABLE_STORE_MIXEDV1  = 3;
     
    //add 64 bit to desc the trailer
    struct ObTrailerOffsetV1
    { 
      int64_t trailer_record_offset_;
      NEED_SERIALIZE_AND_DESERIALIZE;
    };
    
    struct ObTableTrailerInfoV1 {
      uint64_t table_id_;             //table id
      int16_t column_count_;          //column count in schema
      int16_t start_row_key_length_;  //start key length
      int16_t end_row_key_length_;    //end key length
      int16_t reversed_;              //reserved, must be 0
      int32_t start_row_key_offset_;  //start key offset in key buffer(no serialization)
      int32_t end_row_key_offset_;    //end key offset in key buffer(no serialization)

      NEED_SERIALIZE_AND_DESERIALIZE;
    };
    
    //this class is for sstable trailer,you should read it first, then do other things
    class ObSSTableTrailerV1
    {
    public:
      static const int32_t RESERVED_LEN = 64;
      static const int64_t DEFAULT_KEY_BUF_SIZE = 8192; //8k
      
      ObSSTableTrailerV1();
      ~ObSSTableTrailerV1();
      
      //------set and get--------------------------
      //you should first use serialize or deserialize.
      // After that, size_ will be set.
      int32_t get_size() const;
      
      int32_t get_trailer_version() const;
      int set_trailer_version(const int32_t version);

      int64_t get_table_version() const;
      int set_table_version(const int64_t version);
      
      int64_t get_block_count() const;
      int set_block_count(const int64_t count);
      
      int64_t get_block_index_record_offset() const;
      int set_block_index_record_offset(const int64_t offset);
      
      int64_t get_block_index_record_size() const;
      int set_block_index_record_size(const int64_t size);
      
      int64_t get_bloom_filter_hash_count() const;
      int set_bloom_filter_hash_count(const int64_t count);
      int64_t get_bloom_filter_record_offset() const; 
      int set_bloom_filter_record_offset(const int64_t offset);
      int64_t get_bloom_filter_record_size() const;   
      int set_bloom_filter_record_size(const int64_t size);
      
      int64_t get_schema_record_offset() const;
      int set_schema_record_offset(const int64_t offset);
      int64_t get_schema_record_size() const;
      int set_schema_record_size(const int64_t size);

      int64_t get_key_stream_record_offset() const;
      int set_key_stream_record_offset(const int64_t offset);
      int64_t get_key_stream_record_size() const;
      int set_key_stream_record_size(const int64_t size);
      
      int64_t get_block_size() const;
      int set_block_size(const int64_t size);
      
      uint64_t get_table_id(const int64_t index) const;
      /**
       * WARNING: must set table id in ascending order
       * 
       * @param index table index 
       * @param table_id table id 
       * @return int if success, return OB_SUCCESS, else return 
       *         OB_ERROR
       */
      int set_table_id(const int64_t index, const uint64_t table_id);

      uint64_t get_column_count(const int64_t index) const;
      int set_column_count(const int64_t index, const int64_t column_count);

      int32_t get_table_count() const;
      int set_table_count(const int32_t count);
      
      int64_t get_row_count() const;
      int set_row_count(const int64_t count);

      uint64_t get_sstable_checksum() const;
      void set_sstable_checksum(uint64_t sstable_checksum);
      
      const char *get_compressor_name() const;
      int set_compressor_name(const char* name);
      
      int16_t get_row_value_store_style() const;
      int set_row_value_store_style(const int16_t style);
      
      int64_t get_first_block_data_offset() const;
      int set_first_block_data_offset(const int64_t offset);
      
      void reset();

      //WARNING:this function only be called by sstable writer 
      int init_table_info(const int64_t table_count);

      //WARNING: must set start key and end according to the order of table
      //only be called sstable writer
      int set_start_key(const int64_t table_index, common::ObString& start_key);

      //WARNING: must set end key and end according to the order of table,
      //and end key must be adjacent with start key with the same table
      //only be called sstable writer
      int set_end_key(const int64_t table_index, common::ObString& end_key);

      const common::ObString get_start_key(const uint64_t table_id) const;
      const common::ObString get_end_key(const uint64_t table_id) const;

      //WARNING:only be called sstable reader to set the key buffer
      int set_key_buf(common::ObString key_buf);
      common::ObString get_key_buf() const;

      int64_t find_table_id(const uint64_t table_id) const;

      NEED_SERIALIZE_AND_DESERIALIZE;
      
    private:
      DISALLOW_COPY_AND_ASSIGN(ObSSTableTrailerV1);
      int ensure_key_buf_space(const int64_t size);
      
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
      int64_t key_stream_record_offset_;  //key stream record offset in sstable
      int64_t key_stream_record_size_;    //key stream record size, includes record header
      int64_t block_size_;                //block size      
      int64_t row_count_;                 //row count in sstable
      uint64_t sstable_checksum_;         //checksum of sstable
      int64_t reserved64_[RESERVED_LEN];  //reserved, must be 0
      char compressor_name_[common::OB_MAX_COMPRESSOR_NAME_LENGTH]; //compressor name
      int16_t row_value_store_style_;     //dense,sparse or mixed
      int16_t reserved_;                  //reversed, must be 0
      int32_t table_count_;               //table count
      ObTableTrailerInfoV1* table_info_;    //table trailer info
      char* key_buf_;                     //key buffer to store start key and end key
      int64_t key_buf_size_;              //key buffer size
      int64_t key_data_size_;             //actual key data length
      bool own_key_buf_;                  //whether trailer allocate key buffer
    };
  }
}

#endif  //OCEANBASE_SSTABLE_SSTABLE_TRAILERV1_H_
