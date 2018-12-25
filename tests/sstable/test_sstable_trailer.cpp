/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * test_sstable_trailer.cpp for test trailer of sstable
 *
 * Authors:
 *   fanggang< fanggang@taobao.com>
 *   huating <huating.zmq@taobao.com>
 *
 */
#include <gtest/gtest.h>
#include <stdint.h>
#include <string.h>
#include <iostream>
#include "ob_string.h"
#include "sstable/ob_sstable_trailer.h"
#include "ob_sstable_trailerV1.h"
#include "common/file_utils.h"
#include "common/file_directory_utils.h"

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::sstable;
using namespace std;

namespace oceanbase
{
  namespace tests
  {
    namespace sstable
    {
      static const char * trailer_file_name = "trailer_for_test";
      static const ObString start_key(static_cast<int32_t>(strlen("start_key")),
            static_cast<int32_t>(strlen("start_key")), const_cast<char*>("start_key"));
      static const ObString end_key(static_cast<int32_t>(strlen("end_key")),
            static_cast<int32_t>(strlen("end_key")), const_cast<char*>("end_key"));
      static ObString key_stream;
      static ObObj start_key_obj;
      static ObObj end_key_obj;
      static ObNewRange range;
      static ObNewRange default_range;
      static char key_stream_buf[512];
      static ModuleArena arena;

      class TestObSSTableTrailer: public ::testing::Test
      {
      public:
        virtual void SetUp()
        {
          int64_t pos = 0;

          range.table_id_ = 1001;
          start_key_obj.set_varchar(start_key);
          range.start_key_.assign(&start_key_obj, 1);
          end_key_obj.set_varchar(end_key);
          range.end_key_.assign(&end_key_obj, 1);
          range.border_flag_.unset_inclusive_start();
          range.border_flag_.set_inclusive_end();

          default_range.table_id_ = OB_INVALID_ID;
          default_range.set_whole_range();
          default_range.border_flag_.unset_inclusive_start();
          default_range.border_flag_.unset_inclusive_end();

          range.start_key_.serialize(key_stream_buf, 512, pos);
          range.end_key_.serialize(key_stream_buf, 512, pos);
          key_stream.assign_ptr(key_stream_buf, static_cast<int32_t>(pos));
        }

        virtual void TearDown()
        {

        }
      };

      TEST_F(TestObSSTableTrailer, SetAndGetV2)
      {
        ObSSTableTrailer trailer;
        EXPECT_TRUE(default_range == trailer.get_range());
        trailer.set_trailer_version(0x300);
        trailer.set_table_version(1);
        trailer.set_first_block_data_offset(0);
        trailer.set_row_value_store_style(1);
        trailer.set_block_count(1023);
        trailer.set_block_index_record_offset(2047);
        trailer.set_block_index_record_size(1024);
        trailer.set_bloom_filter_hash_count(8);
        trailer.set_bloom_filter_record_offset(2047 + 1024);
        trailer.set_bloom_filter_record_size(511);
        trailer.set_schema_record_offset(2047+ 1024 + 511);
        trailer.set_schema_record_size(1023);
        trailer.set_block_size(64 * 1024);
        trailer.set_row_count(777);
        trailer.set_sstable_checksum(123456789);
        trailer.set_first_table_id(1001);
        trailer.set_frozen_time(123456789);
        trailer.set_range_record_offset(2047+ 1024 + 511 +1023);
        trailer.set_range_record_size(48);
        const char *compressor_name = "lzo1x_1_11_compress";
        trailer.set_compressor_name(compressor_name);
        ObTrailerOffset trailer_offset;
        trailer_offset.trailer_record_offset_= 256 * 1024 - 1023;
        trailer.set_range(range);
        EXPECT_TRUE(range == trailer.get_range());

        int64_t offset_len = trailer_offset.get_serialize_size();
        int64_t trailer_len = trailer.get_serialize_size();
        int64_t len = offset_len + trailer_len;
        char *buf = new char[len];
        int64_t pos = 0;
        trailer.serialize(buf, len, pos);
        trailer_offset.serialize(buf, len, pos);
        EXPECT_EQ(pos, len);
        pos = trailer_len;
        trailer_offset.trailer_record_offset_ = 0;
        trailer_offset.deserialize(buf, len, pos);
        EXPECT_EQ(trailer_offset.trailer_record_offset_, 256 * 1024 - 1023);

        trailer.reset();
        EXPECT_TRUE(default_range == trailer.get_range());
        pos = 0;
        trailer.deserialize(buf, len, pos);
        EXPECT_EQ(0x300, trailer.get_trailer_version());
        EXPECT_EQ(1, trailer.get_table_version());
        EXPECT_EQ(0, trailer.get_first_block_data_offset());
        EXPECT_EQ(1, trailer.get_row_value_store_style());
        EXPECT_EQ(1023, trailer.get_block_count());
        EXPECT_EQ(2047, trailer.get_block_index_record_offset());
        EXPECT_EQ(1024, trailer.get_block_index_record_size());
        EXPECT_EQ(8, trailer.get_bloom_filter_hash_count());
        EXPECT_EQ(2047 + 1024, trailer.get_bloom_filter_record_offset());
        EXPECT_EQ(511, trailer.get_bloom_filter_record_size());
        EXPECT_EQ(2047+ 1024 + 511, trailer.get_schema_record_offset());
        EXPECT_EQ(1023, trailer.get_schema_record_size());
        EXPECT_EQ(2047+ 1024 + 511 + 1023, trailer.get_range_record_offset());
        EXPECT_EQ(48, trailer.get_range_record_size());
        EXPECT_EQ(64 * 1024, trailer.get_block_size());
        EXPECT_EQ(777, trailer.get_row_count());
        EXPECT_EQ(123456789, (int64_t)trailer.get_sstable_checksum());
        EXPECT_EQ(1001, (int64_t)trailer.get_first_table_id());
        EXPECT_EQ(123456789, trailer.get_frozen_time());
        int ret = memcmp(compressor_name, trailer.get_compressor_name(), strlen(compressor_name));
        EXPECT_EQ(0, ret);
        trailer.copy_range(range);
        EXPECT_TRUE(range == trailer.get_range());

        delete [] buf;
      }

      TEST_F(TestObSSTableTrailer, write_tariler_to_disk)
      {
        ObSSTableTrailer trailer;
        EXPECT_TRUE(default_range == trailer.get_range());
        FileUtils filesys;
        trailer.set_trailer_version(0x300);
        trailer.set_table_version(1);
        trailer.set_first_block_data_offset(0);
        trailer.set_row_value_store_style(1);
        trailer.set_block_count(1023);
        trailer.set_block_index_record_offset(2047);
        trailer.set_block_index_record_size(1024);
        trailer.set_bloom_filter_hash_count(8);
        trailer.set_bloom_filter_record_offset(2047 + 1024);
        trailer.set_bloom_filter_record_size(511);
        trailer.set_schema_record_offset(2047+ 1024 + 511);
        trailer.set_schema_record_size(1023);
        trailer.set_block_size(64 * 1024);
        trailer.set_row_count(777);
        trailer.set_sstable_checksum(123456789);
        trailer.set_first_table_id(1001);
        trailer.set_frozen_time(123456789);
        trailer.set_range_record_offset(2047+ 1024 + 511 +1023);
        trailer.set_range_record_size(0);
        const char *compressor_name = "lzo1x_1_11_compress";
        trailer.set_compressor_name(compressor_name);

        ObTrailerOffset trailer_offset;
        trailer_offset.trailer_record_offset_= 256 * 1024 - 1023;

        int64_t offset_len = trailer_offset.get_serialize_size();
        int64_t trailer_len = trailer.get_serialize_size();

        int64_t buf_size = offset_len + trailer_len;
        int64_t pos = 0;
        char *serialize_buf = reinterpret_cast<char*>(malloc(buf_size));
        EXPECT_TRUE(NULL != serialize_buf);
        trailer.serialize(serialize_buf, buf_size, pos);
        trailer_offset.serialize(serialize_buf, buf_size, pos);
        filesys.open(trailer_file_name, O_WRONLY | O_CREAT | O_TRUNC, 0644);
        int64_t write_size;
        write_size = filesys.write(serialize_buf, buf_size);
        EXPECT_EQ(write_size, buf_size);
        free(serialize_buf);
        serialize_buf = NULL;
        filesys.close();
      }

      //test compatible between version2 && version2 compiled without -DCOMPATIBLE
      TEST_F(TestObSSTableTrailer, Compatible)
      {
        ObTrailerOffset trailer_offset;
        ObSSTableTrailer trailer;
        EXPECT_TRUE(default_range == trailer.get_range());
        FileUtils filesys;
        const char *compressor_name = "lzo1x_1_11_compress";
        int64_t file_len = FileDirectoryUtils::get_size(trailer_file_name);
        char *file_buf = reinterpret_cast<char*>(malloc(file_len));
        EXPECT_TRUE(NULL != file_buf);
        int64_t read_size = 0;
        filesys.open(trailer_file_name, O_RDONLY);
        read_size = filesys.read(file_buf, file_len);
        EXPECT_EQ(read_size, file_len);

        int64_t pos = 0;
        pos = trailer.get_serialize_size();
        trailer_offset.trailer_record_offset_ = 0;
        trailer_offset.deserialize(file_buf, file_len, pos);
        EXPECT_EQ(trailer_offset.trailer_record_offset_, 256 * 1024 - 1023);

        pos = 0;
        trailer.deserialize(file_buf, file_len, pos);
        EXPECT_EQ(0x300, trailer.get_trailer_version());
        EXPECT_EQ(1, trailer.get_table_version());
        EXPECT_EQ(0, trailer.get_first_block_data_offset());
        EXPECT_EQ(1, trailer.get_row_value_store_style());
        EXPECT_EQ(1023, trailer.get_block_count());
        EXPECT_EQ(2047, trailer.get_block_index_record_offset());
        EXPECT_EQ(1024, trailer.get_block_index_record_size());
        EXPECT_EQ(8, trailer.get_bloom_filter_hash_count());
        EXPECT_EQ(2047 + 1024, trailer.get_bloom_filter_record_offset());
        EXPECT_EQ(511, trailer.get_bloom_filter_record_size());
        EXPECT_EQ(2047+ 1024 + 511, trailer.get_schema_record_offset());
        EXPECT_EQ(1023, trailer.get_schema_record_size());
        EXPECT_EQ(64 * 1024, trailer.get_block_size());
        EXPECT_EQ(777, trailer.get_row_count());
        EXPECT_EQ(123456789, (int64_t)trailer.get_sstable_checksum());
        EXPECT_EQ(1001, (int64_t)trailer.get_first_table_id());
        EXPECT_EQ(123456789, trailer.get_frozen_time());
        int ret = memcmp(compressor_name, trailer.get_compressor_name(), strlen(compressor_name));
        EXPECT_EQ(0, ret);
        EXPECT_TRUE(default_range == trailer.get_range());
        free(file_buf);
        file_buf = NULL;
        filesys.close();
      }
    }//end namespace sstable
  }//end namespace tests
}//end namespace oceanbase

int main(int argc, char** argv)
{
  ob_init_memory_pool();
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
