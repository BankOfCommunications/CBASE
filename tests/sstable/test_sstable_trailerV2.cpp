/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *
 * test_sstable_trailerV2.cpp is for what ...
 *
 * Authors:
 *   fangji.hcm <fangji.hcm@taobao.com>
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
      class TestObSSTableTrailerV2: public ::testing::Test
      {
      public:
        virtual void SetUp()
        {
      
        }
      
        virtual void TearDown()
        {
      
        }
      };
      
      TEST_F(TestObSSTableTrailerV2, deserialize_and_check)
      {
        ObSSTableTrailer trailer2;
        ObTrailerOffset trailer_offset;
        common::FileUtils filesys;
        common::FileDirectoryUtils dirsys;
        const char *compressor_name = "lzo1x_1_11_compress";
        int32_t fd = -1;
        fd = filesys.open("tmptrailer",O_RDONLY, 0644);
        int64_t file_size = dirsys.get_size("tmptrailer");
        int64_t read_size = 0;
        char *buf = new char[file_size];
        read_size = filesys.read(buf, file_size);
        EXPECT_EQ(read_size, file_size);

        int64_t pos = 0;
        
        pos = trailer2.get_serialize_size();
        trailer_offset.trailer_record_offset_ = 0;
        trailer_offset.deserialize(buf, file_size, pos);
        EXPECT_EQ(trailer_offset.trailer_record_offset_, 256 * 1024 - 1023);
        
        pos = 0;
        trailer2.deserialize(buf, file_size, pos);
        EXPECT_EQ(0x200, trailer2.get_trailer_version());
        EXPECT_EQ(1, trailer2.get_table_version());
        EXPECT_EQ(0, trailer2.get_first_block_data_offset());
        EXPECT_EQ(1, trailer2.get_row_value_store_style());
        EXPECT_EQ(1023, trailer2.get_block_count());
        EXPECT_EQ(2047, trailer2.get_block_index_record_offset());
        EXPECT_EQ(1024, trailer2.get_block_index_record_size());
        EXPECT_EQ(8, trailer2.get_bloom_filter_hash_count());
        EXPECT_EQ(2047 + 1024, trailer2.get_bloom_filter_record_offset());
        EXPECT_EQ(511, trailer2.get_bloom_filter_record_size());
        EXPECT_EQ(2047+ 1024 + 511, trailer2.get_schema_record_offset());
        EXPECT_EQ(1023, trailer2.get_schema_record_size());
        EXPECT_EQ(64 * 1024, trailer2.get_block_size());
        EXPECT_EQ(777, trailer2.get_row_count());
        EXPECT_EQ(123456789, (int64_t)trailer2.get_sstable_checksum());
        int ret = memcmp(compressor_name, trailer2.get_compressor_name(), strlen(compressor_name));
        EXPECT_EQ(0, ret);

        delete [] buf;
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
