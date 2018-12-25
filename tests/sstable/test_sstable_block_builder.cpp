/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *  
 * test_sstable_block_builder.cpp for test sstable builder
 *
 * Authors: 
 *   huating <huating.zmq@taobao.com>
 *
 */

#include <tblog.h>
#include <gtest/gtest.h>
#include "common/ob_object.h"
#include "common/utility.h"
#include "sstable/ob_sstable_schema.h"
#include "sstable/ob_sstable_row.h"
#include "key.h"
#include "sstable/ob_sstable_block_builder.h"

using namespace oceanbase::common;
using namespace oceanbase::sstable;

namespace oceanbase
{
  namespace tests
  {
    namespace sstable 
    {
      class TestObSSTableBlockBuilder: public ::testing::Test
      {
      public:
        virtual void SetUp()
        {
      
        }
      
        virtual void TearDown()
        {
      
        }
      };
      
      TEST_F(TestObSSTableBlockBuilder, test_init)
      {
        ObSSTableBlockBuilder block_builder;
        const char *block_buf = NULL;
        const char *index_buf = NULL;
        int ret;

        ret = block_builder.init();
        EXPECT_TRUE(OB_SUCCESS == ret);
        EXPECT_EQ(0, block_builder.get_row_count());
        EXPECT_EQ((int64_t)sizeof(ObSSTableBlockHeader), block_builder.get_block_size());
        EXPECT_EQ(0, block_builder.get_row_index_array_offset());

        block_buf = block_builder.block_buf();
        index_buf = block_builder.row_index_buf();

        EXPECT_TRUE(block_buf != NULL);
        EXPECT_TRUE(index_buf != NULL);
        EXPECT_EQ((int64_t)sizeof(ObSSTableBlockHeader), 
                  block_builder.get_block_data_size());
        EXPECT_EQ(0, block_builder.get_row_index_size());
      }

      TEST_F(TestObSSTableBlockBuilder, test_reinit)
      {
        ObSSTableBlockBuilder block_builder;
        const char *block_buf = NULL;
        const char *index_buf = NULL;
        const char *block_buf_ptr = NULL;
        const char *index_buf_ptr = NULL;
        int ret;

        ret = block_builder.init();
        EXPECT_TRUE(OB_SUCCESS == ret);
        EXPECT_EQ(0, block_builder.get_row_count());
        EXPECT_EQ((int64_t)sizeof(ObSSTableBlockHeader), 
                  block_builder.get_block_size());
        EXPECT_EQ(0, block_builder.get_row_index_array_offset());

        block_buf_ptr = block_builder.block_buf();
        index_buf_ptr = block_builder.row_index_buf();

        //reinit block builder, ensure not allocate memory again
        ret = block_builder.init();
        EXPECT_TRUE(OB_SUCCESS == ret);
        block_buf = block_builder.block_buf();
        index_buf = block_builder.row_index_buf();
        EXPECT_EQ(block_buf_ptr, block_buf);
        EXPECT_EQ(index_buf_ptr, index_buf);

        EXPECT_TRUE(block_buf != NULL);
        EXPECT_TRUE(index_buf != NULL);
        EXPECT_EQ((int64_t)sizeof(ObSSTableBlockHeader), 
                  block_builder.get_block_data_size());
        EXPECT_EQ(0, block_builder.get_row_index_size());
      }
      
void build_rowkey_info(ObRowkeyInfo& rowkey_info)
      {
        ObRowkeyColumn split;
        split.length_ = 8;
        split.type_   = ObIntType;
        rowkey_info.add_column(split);

        split.length_ = 1;
        split.type_   = ObVarcharType;
        rowkey_info.add_column(split);

        split.length_ = 8;
        split.type_   = ObIntType;
        rowkey_info.add_column(split);
        
        ASSERT_EQ(3, rowkey_info.get_size());
        
        rowkey_info.get_column(0, split);
        ASSERT_EQ(8, split.length_);
        ASSERT_EQ(ObIntType, split.type_);
        
        rowkey_info.get_column(1, split);
        ASSERT_EQ(1, split.length_);
        ASSERT_EQ(ObVarcharType, split.type_);
        
        rowkey_info.get_column(2, split);
        ASSERT_EQ(8, split.length_);
        ASSERT_EQ(ObIntType, split.type_);
      }

      TEST_F(TestObSSTableBlockBuilder, test_add_one_row)
      {
        ObSSTableBlockBuilder block_builder;
        block_builder.set_table_id(1000);
        block_builder.set_column_group_id(2);
        const char *block_buf = NULL;
        const char *index_buf = NULL;
        ObRowkeyInfo rowkey_info;
        build_rowkey_info(rowkey_info);
        ObSSTableRow row(NULL);
        ObObj tmp_obj;
        ObString row_key;
        char value_data[1024 + 1];
        char *ptr;
        int ret;

        //build data
        ptr = value_data;
        for (int i = 0; i < 128; ++i) {
          memcpy(ptr, "testing ", 8);
          ptr += 8;
        }
        ObString value_str(1025, 1025, value_data);

        ObRowkey key;
        Key tmp_key(10, 0, 0);
        tmp_key.trans_to_rowkey(key);
        row.set_rowkey(key);
        row.set_table_id(1000);
        row.set_column_group_id(2);

        tmp_obj.set_int(10);
        row.add_obj(tmp_obj);
        tmp_obj.set_int(100);
        row.add_obj(tmp_obj);
        tmp_obj.set_varchar(value_str);
        row.add_obj(tmp_obj);
        EXPECT_EQ(3+key.get_obj_cnt(), row.get_obj_count());

        ret = block_builder.init();
        EXPECT_TRUE(OB_SUCCESS == ret);
        uint64_t row_checksum = 0;
        ret = block_builder.add_row(row, row_checksum);
        EXPECT_TRUE(OB_SUCCESS == ret);

        EXPECT_EQ(1, block_builder.get_row_count());
        EXPECT_EQ(0, block_builder.get_row_index_array_offset());

        block_buf = block_builder.block_buf();
        index_buf = block_builder.row_index_buf();

        EXPECT_TRUE(block_builder.get_block_data_size() > 0);
        EXPECT_TRUE(block_builder.get_row_index_size() > 0);

        row.clear();
        row.set_rowkey(key);
        row.set_table_id(1000);
        row.set_column_group_id(3);

        tmp_obj.set_int(10);
        row.add_obj(tmp_obj);
        tmp_obj.set_int(100);
        row.add_obj(tmp_obj);
        tmp_obj.set_varchar(value_str);
        row.add_obj(tmp_obj);
        EXPECT_EQ(3+key.get_obj_cnt(), row.get_obj_count());
       
        ret = block_builder.add_row(row, row_checksum);
        EXPECT_TRUE(OB_ERROR == ret);
      }

      void build_row(int64_t index, ObSSTableRow &row)
      {
        ObObj tmp_obj;

        ObRowkey key;
        Key tmp_key(index, 10, 1000);
        tmp_key.trans_to_rowkey(key);
        row.set_rowkey(key);
        row.set_table_id(1000);
        row.set_column_group_id(2);

        for (int i = 0; i < OB_MAX_COLUMN_NUMBER; ++i)
        {
          tmp_obj.set_int(i);
          row.add_obj(tmp_obj);
        }      
      }

      TEST_F(TestObSSTableBlockBuilder, test_add_many_rows_to_one_block)
      {
        ObSSTableBlockBuilder block_builder;
        block_builder.set_table_id(1000);
        block_builder.set_column_group_id(2);
        const char *block_buf = NULL;
        const char *index_buf = NULL;
        ObRowkeyInfo rowkey_info;
        build_rowkey_info(rowkey_info);
        ObSSTableRow row(NULL);
        int i = 0;
        int ret;

        ret = block_builder.init();
        EXPECT_TRUE(OB_SUCCESS == ret);

        while (true)
        {
          if (i > 10000)
          {
            break;
          }
          build_row(i, row);
          uint64_t row_checksum = 0;
          ret = block_builder.add_row(row, row_checksum);
          if (ret == OB_SUCCESS)
          {
            i++;
            EXPECT_EQ(i, block_builder.get_row_count());
          }
          else 
          {
            block_buf = block_builder.block_buf();
            index_buf = block_builder.row_index_buf();
    
            EXPECT_TRUE(block_builder.get_block_data_size() > 0);
            EXPECT_TRUE(block_builder.get_row_index_size() > 0);

            ret = block_builder.ensure_space(row.get_serialize_size());
            EXPECT_TRUE(ret == OB_SUCCESS);
            break;
          }
        }
      }

      TEST_F(TestObSSTableBlockBuilder, test_add_many_rows_to_blocks)
      {
        ObSSTableBlockBuilder block_builder;
        block_builder.set_table_id(1000);
        block_builder.set_column_group_id(2);
        ObRowkeyInfo rowkey_info;
        build_rowkey_info(rowkey_info);
        ObSSTableRow row(NULL);
        int block_count = 0;
        int i = 0;
        int ret;

        ret = block_builder.init();
        EXPECT_TRUE(OB_SUCCESS == ret);

        while (true)
        {
          build_row(i, row);
          uint64_t row_checksum = 0;
          ret = block_builder.add_row(row, row_checksum);
          ASSERT_EQ(ret , OB_SUCCESS);

          i++;
          EXPECT_EQ(i, block_builder.get_row_count());

          if (block_builder.get_block_size() 
              >= ObSSTableBlockBuilder::SSTABLE_BLOCK_SIZE / 4)
          {
            block_count++;
            EXPECT_TRUE(block_builder.ensure_space(4) == OB_SUCCESS);
            block_builder.reset();
            block_builder.set_table_id(1000);
            block_builder.set_column_group_id(2);
            EXPECT_EQ(0, block_builder.get_row_count());
            i = 0;
            if (block_count > 10)
            {
              break;
            }
          }   
        }
      }

      TEST_F(TestObSSTableBlockBuilder, test_add_null_row)
      {
        ObSSTableBlockBuilder block_builder;
        ObRowkeyInfo rowkey_info;
        build_rowkey_info(rowkey_info);
        ObSSTableRow row(NULL);
        int ret;

        ret = block_builder.init();
        EXPECT_TRUE(OB_SUCCESS == ret);
        
        uint64_t row_checksum = 0;
        ret = block_builder.add_row(row, row_checksum);
        EXPECT_TRUE(OB_ERROR == ret);
      }

      void build_large_row(ObSSTableRow &row)
      {
        ObObj tmp_obj;
        int64_t value_size = 1024 * 64 + 1;
        char value_data[value_size];
        char *ptr;

        //build data
        ptr = value_data;
        for (int i = 0; i < 128 * 64; ++i) {
          memcpy(ptr, "testing ", 8);
          ptr += 8;
        }
        ObString value_str(static_cast<int32_t>(value_size), static_cast<int32_t>(value_size), value_data);

        ObRowkey key;
        Key tmp_key(10, 10, 1000);
        tmp_key.trans_to_rowkey(key);
        row.set_rowkey(key);
        row.set_table_id(1000);
        row.set_column_group_id(2);

        for (int i = 0; i < OB_MAX_COLUMN_NUMBER; ++i)
        {
          tmp_obj.set_varchar(value_str);
          row.add_obj(tmp_obj);
        }      
      }

      TEST_F(TestObSSTableBlockBuilder, test_add_large_row)
      {
        ObSSTableBlockBuilder block_builder;
        block_builder.set_table_id(1000);
        block_builder.set_column_group_id(2);
        ObRowkeyInfo rowkey_info;
        build_rowkey_info(rowkey_info);
        ObSSTableRow row(NULL);
        int ret;

        ret = block_builder.init();
        EXPECT_TRUE(OB_SUCCESS == ret);
        build_large_row(row);
        uint64_t row_checksum = 0;
        ret = block_builder.add_row(row, row_checksum);
        //fprintf(stderr, "block buffer size is %ld\n", block_builder.get_block_data_size()
        //      + block_builder.get_row_index_size());
        EXPECT_TRUE(OB_SUCCESS == ret);
      }

      TEST_F(TestObSSTableBlockBuilder, test_ensure_space)
      {
        ObSSTableBlockBuilder block_builder;
        int ret;

        ret = block_builder.init();
        EXPECT_TRUE(OB_SUCCESS == ret);

        ret = block_builder.ensure_space(-1);
        EXPECT_TRUE(OB_ERROR == ret);

        ret = block_builder.ensure_space(0);
        EXPECT_TRUE(OB_ERROR == ret);

        ret = block_builder.ensure_space(1234569);
        EXPECT_TRUE(OB_SUCCESS == ret);
        
        ret = block_builder.ensure_space(3 * 1024 * 1024);
        EXPECT_TRUE(OB_SUCCESS == ret);

        ret = block_builder.ensure_space(16);
        EXPECT_TRUE(OB_SUCCESS == ret);

        ret = block_builder.ensure_space(ObSSTableBlockBuilder::SSTABLE_BLOCK_SIZE / 8);
        EXPECT_TRUE(OB_SUCCESS == ret);
      }

      TEST_F(TestObSSTableBlockBuilder, test_build_blocks)
      {
        ObSSTableBlockBuilder block_builder;
        block_builder.set_table_id(1000);
        block_builder.set_column_group_id(2);
        ObSSTableBlockHeader block_header;
        const char *block_buf = NULL;
        const char *index_buf = NULL;
        char *row_data = NULL;
        ObRowkeyInfo rowkey_info;
        build_rowkey_info(rowkey_info);
        ObSSTableRow row(NULL);
        ObSSTableRow new_row(NULL);
        ObRowkey row_key(NULL, 3);
        ObObj tmp_obj;
        const ObObj *obj = NULL;
        int64_t row_data_len = 0;
        int block_count = 0;
        int64_t row_count = 0;
        int64_t row_index_offset = 0;
        int64_t sstable_size = ObSSTableBlockBuilder::SSTABLE_BLOCK_SIZE / 8;
        int i = 0;
        int64_t pos = 0;
        int ret;
        ObRowkey key;

        ret = block_builder.init();
        EXPECT_TRUE(OB_SUCCESS == ret);

        while (true)
        {
          Key tmp_key(i, 10, 1000);
          tmp_key.trans_to_rowkey(key);
          row.clear();
          row.set_rowkey(key);
          row.set_table_id(1000);
          row.set_column_group_id(2);

          for (int64_t k = row_key.get_obj_cnt(); k < OB_MAX_COLUMN_NUMBER; ++k)
          {
            tmp_obj.set_int(k);
            row.add_obj(tmp_obj);
          }

          uint64_t row_checksum = 0;
          ret = block_builder.add_row(row, row_checksum);
          EXPECT_TRUE(ret == OB_SUCCESS);

          i++;
          ASSERT_EQ(i, block_builder.get_row_count());

          if (block_builder.get_block_size() >= sstable_size)
          {
            block_count++;
            EXPECT_TRUE(block_builder.ensure_space(4) == OB_SUCCESS);

            ret = block_builder.build_block();
            
            EXPECT_TRUE(OB_SUCCESS == ret);
            EXPECT_TRUE(block_builder.get_row_index_array_offset() > 0);
            EXPECT_TRUE(block_builder.get_block_size() >= sstable_size);

            //check row index
            row_count = block_builder.get_row_count();
            row_index_offset = block_builder.get_row_index_array_offset();
            block_buf = block_builder.block_buf();
            index_buf = block_builder.row_index_buf();
            pos = 0;
            ret = block_header.deserialize(const_cast<char *>(block_buf),
                                           block_header.get_serialize_size(), pos);
            EXPECT_TRUE(OB_SUCCESS == ret);
            EXPECT_EQ(row_count, block_header.row_count_);
            EXPECT_EQ(row_index_offset, block_header.row_index_array_offset_);
            EXPECT_TRUE(memcmp(block_buf + row_index_offset,
                        index_buf, block_builder.get_row_index_size()) == 0);

            //check each row
            row_data = const_cast<char *>(block_buf) + block_header.get_serialize_size();
            row_data_len = row_index_offset - block_header.get_serialize_size();
            pos = 0;
            for (int j = 0; j < row_count; ++j)
            {
              new_row.clear();
              new_row.set_obj_count(OB_MAX_COLUMN_NUMBER);
              ret = new_row.deserialize(row_data, row_data_len, pos);
              EXPECT_TRUE(OB_SUCCESS == ret);
              new_row.get_rowkey(row_key);

              Key tmp_key(j, 10, 1000);
              tmp_key.trans_to_rowkey(key);
              EXPECT_EQ(tmp_key.key_len(), row_key.length());
              ASSERT_EQ(key , row_key);

              for (int64_t k = row_key.get_obj_cnt(); k < OB_MAX_COLUMN_NUMBER; ++k)
              {
                EXPECT_EQ(OB_INVALID_ID, new_row.get_table_id());
                EXPECT_EQ(OB_INVALID_ID, new_row.get_column_group_id());
                obj = new_row.get_obj((int32_t)k);
                EXPECT_TRUE(NULL != obj);
                int64_t val;
                obj->get_int(val);
                ASSERT_EQ(k, val);
              }
            }

            block_builder.reset();
            block_builder.set_table_id(1000);
            block_builder.set_column_group_id(2);
            EXPECT_EQ(0, block_builder.get_row_count());
            i = 0;
            if (block_count > 10)
            {
              break;
            }
          }
        }
      }
    }//end namespace sstable
  }//end namespace tests
}//end namespace oceanbase

int main(int argc, char** argv)
{
  ob_init_memory_pool();
  TBSYS_LOGGER.setLogLevel("ERROR");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

