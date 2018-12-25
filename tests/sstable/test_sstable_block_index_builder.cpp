/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * test_sstable_block_index_builder.cpp for test sstable block
 * index builder
 *
 * Authors:
 *   huating <huating.zmq@taobao.com>
 *
 */

#include <tblog.h>
#include <gtest/gtest.h>
#include "common/ob_object.h"
#include "sstable/ob_sstable_schema.h"
#include "sstable/ob_sstable_row.h"
#include "sstable/ob_sstable_block_index_buffer.h"
#include "sstable/ob_sstable_block_index_builder.h"

using namespace oceanbase::common;
using namespace oceanbase::sstable;

namespace oceanbase
{
  namespace tests
  {
    namespace sstable
    {
      static const int64_t table_id = 100;
      static const int64_t column_group_id = 10;
      class TestObSSTableBlockIndexBuilder: public ::testing::Test
      {
      public:
        virtual void SetUp()
        {

        }

        virtual void TearDown()
        {

        }
      };

      TEST_F(TestObSSTableBlockIndexBuilder, test_init)
      {
        ObSSTableBlockIndexBuilder index_builder;

        EXPECT_EQ(0, index_builder.get_index_items_buffer()->get_data_size());
        EXPECT_EQ(0, index_builder.get_end_keys_buffer()->get_data_size());
        EXPECT_EQ(0, index_builder.get_index_items_buffer()->get_total_size());
        EXPECT_EQ(0, index_builder.get_end_keys_buffer()->get_total_size());

        EXPECT_EQ(0, index_builder.get_block_count());
        EXPECT_EQ((int64_t)sizeof(ObSSTableBlockIndexHeader),
                  index_builder.get_index_block_size());
      }

      TEST_F(TestObSSTableBlockIndexBuilder, test_add_one_entry)
      {
        ObSSTableBlockIndexBuilder index_builder;
        char *key = (char*)"test";
        ObString end_key;
        int ret = OB_SUCCESS;

        //null key,right record size
        ret = index_builder.add_entry(table_id, column_group_id, end_key, 100);
        EXPECT_TRUE(OB_ERROR == ret);
        EXPECT_EQ(0, index_builder.get_block_count());
        EXPECT_EQ(0, index_builder.get_index_items_size());
        EXPECT_EQ(0, index_builder.get_end_keys_size());
        EXPECT_EQ(0, index_builder.get_index_items_buffer()->get_data_size());
        EXPECT_EQ(0, index_builder.get_end_keys_buffer()->get_data_size());
        EXPECT_EQ(0, index_builder.get_index_items_buffer()->get_total_size());
        EXPECT_EQ(0, index_builder.get_end_keys_buffer()->get_total_size());


        //null key,wrong offset
        ret = index_builder.add_entry(table_id, column_group_id, end_key, -1);
        EXPECT_TRUE(OB_ERROR == ret);
        EXPECT_EQ(0, index_builder.get_block_count());
        EXPECT_EQ(0, index_builder.get_index_items_size());
        EXPECT_EQ(0, index_builder.get_end_keys_size());
        EXPECT_EQ((int64_t)sizeof(ObSSTableBlockIndexHeader),
                  index_builder.get_index_block_size());
        EXPECT_EQ(0, index_builder.get_index_items_buffer()->get_data_size());
        EXPECT_EQ(0, index_builder.get_end_keys_buffer()->get_data_size());
        EXPECT_EQ(0, index_builder.get_index_items_buffer()->get_total_size());
        EXPECT_EQ(0, index_builder.get_end_keys_buffer()->get_total_size());

        //right key, wrong record size
        end_key.assign(key, static_cast<int32_t>(strlen(key)));
        ret = index_builder.add_entry(table_id, column_group_id, end_key, -1);
        EXPECT_TRUE(OB_ERROR == ret);
        EXPECT_EQ(0, index_builder.get_block_count());
        EXPECT_EQ(0, index_builder.get_index_items_size());
        EXPECT_EQ(0, index_builder.get_end_keys_size());
        EXPECT_EQ((int64_t)sizeof(ObSSTableBlockIndexHeader),
                  index_builder.get_index_block_size());
        EXPECT_EQ(0, index_builder.get_index_items_buffer()->get_data_size());
        EXPECT_EQ(0, index_builder.get_end_keys_buffer()->get_data_size());
        EXPECT_EQ(0, index_builder.get_index_items_buffer()->get_total_size());
        EXPECT_EQ(0, index_builder.get_end_keys_buffer()->get_total_size());

        //right key, right offset
        end_key.assign(key, static_cast<int32_t>(strlen(key)));
        ret = index_builder.add_entry(table_id, column_group_id, end_key, 100);
        EXPECT_TRUE(OB_SUCCESS == ret);
        EXPECT_EQ(1, index_builder.get_block_count());
        EXPECT_EQ((int64_t)sizeof(ObSSTableBlockIndexItem),
                  index_builder.get_index_items_size());
        EXPECT_EQ((int64_t)strlen(key), index_builder.get_end_keys_size());
        EXPECT_EQ((int64_t)(sizeof(ObSSTableBlockIndexHeader)
                  + sizeof(ObSSTableBlockIndexItem) + strlen(key)),
                  index_builder.get_index_block_size());

        EXPECT_EQ((int64_t)(sizeof(ObSSTableBlockIndexItem)), index_builder.get_index_items_buffer()->get_data_size());
        EXPECT_EQ((int64_t)(strlen(key)), index_builder.get_end_keys_buffer()->get_data_size());
        EXPECT_EQ(ObSSTableBlockIndexBuffer::DEFAULT_MEM_BLOCK_SIZE, index_builder.get_index_items_buffer()->get_total_size());
        EXPECT_EQ(ObSSTableBlockIndexBuffer::DEFAULT_MEM_BLOCK_SIZE, index_builder.get_end_keys_buffer()->get_total_size());

        //test reset
        index_builder.reset();
        EXPECT_EQ(0, index_builder.get_block_count());
        EXPECT_EQ((int64_t)sizeof(ObSSTableBlockIndexHeader),
                  index_builder.get_index_block_size());
        EXPECT_EQ(0, index_builder.get_end_key_char_stream_offset());
        EXPECT_EQ(0, index_builder.get_index_items_size());
        EXPECT_EQ(0, index_builder.get_end_keys_size());

        EXPECT_EQ(0, index_builder.get_index_items_buffer()->get_data_size());
        EXPECT_EQ(0, index_builder.get_end_keys_buffer()->get_data_size());
        EXPECT_EQ(ObSSTableBlockIndexBuffer::DEFAULT_MEM_BLOCK_SIZE, index_builder.get_index_items_buffer()->get_total_size());
        EXPECT_EQ(ObSSTableBlockIndexBuffer::DEFAULT_MEM_BLOCK_SIZE, index_builder.get_end_keys_buffer()->get_total_size());
      }

      TEST_F(TestObSSTableBlockIndexBuilder, test_add_many_entries)
      {
        ObSSTableBlockIndexBuilder index_builder;
        char *key = (char*)"test test test ";
        ObString end_key;
        int ret = OB_SUCCESS;
        ObSSTableBlockIndexItem ii;
        int item_size = static_cast<int32_t>(ii.get_serialize_size());

        int npindex = static_cast<int32_t>((ObSSTableBlockIndexBuffer::DEFAULT_MEM_BLOCK_SIZE -
                       sizeof(ObSSTableBlockIndexBuffer::MemBlock))/item_size);

        int npkey   = static_cast<int32_t>((ObSSTableBlockIndexBuffer::DEFAULT_MEM_BLOCK_SIZE -
                       sizeof(ObSSTableBlockIndexBuffer::MemBlock))/strlen(key));

        if (0 == static_cast<int32_t>((ObSSTableBlockIndexBuffer::DEFAULT_MEM_BLOCK_SIZE -
                       sizeof(ObSSTableBlockIndexBuffer::MemBlock))%strlen(key)))
        {
          npkey--;
        }

        if (0 == (ObSSTableBlockIndexBuffer::DEFAULT_MEM_BLOCK_SIZE -
                       sizeof(ObSSTableBlockIndexBuffer::MemBlock))%item_size)
        {
          npindex--;
        }

        end_key.assign(key, static_cast<int32_t>(strlen(key)));
        for (int i = 0; i < 204800; ++i)
        {
          ret = index_builder.add_entry(table_id, i/1000, end_key, i * 16 + 1);
          EXPECT_EQ(i + 1, index_builder.get_block_count());
          EXPECT_EQ((int64_t)sizeof(ObSSTableBlockIndexItem) * (i + 1),
                    index_builder.get_index_items_size());
          EXPECT_EQ((int64_t)strlen(key) * (i + 1), index_builder.get_end_keys_size());
          EXPECT_EQ((int64_t)(sizeof(ObSSTableBlockIndexHeader)
                    + (sizeof(ObSSTableBlockIndexItem) + strlen(key)) * (i + 1)),
                    index_builder.get_index_block_size());


          EXPECT_EQ((int64_t)(sizeof(ObSSTableBlockIndexItem)*(i+1)), index_builder.get_index_items_buffer()->get_data_size());
          EXPECT_EQ((int64_t)(strlen(key)*(i+1)), index_builder.get_end_keys_buffer()->get_data_size());
          if ( npindex <= i )
          {
            EXPECT_EQ(ObSSTableBlockIndexBuffer::DEFAULT_MEM_BLOCK_SIZE*2,
                      index_builder.get_index_items_buffer()->get_total_size());
          }
          else
          {
            EXPECT_EQ(ObSSTableBlockIndexBuffer::DEFAULT_MEM_BLOCK_SIZE,
                      index_builder.get_index_items_buffer()->get_total_size());
          }

          if ( npkey <= i )
          {
            EXPECT_EQ(ObSSTableBlockIndexBuffer::DEFAULT_MEM_BLOCK_SIZE*2,
                      index_builder.get_end_keys_buffer()->get_total_size());
          }
          else
          {
            EXPECT_EQ(ObSSTableBlockIndexBuffer::DEFAULT_MEM_BLOCK_SIZE,
                      index_builder.get_end_keys_buffer()->get_total_size());
          }
        }

        //test reset
        index_builder.reset();
        EXPECT_EQ(0, index_builder.get_block_count());
        EXPECT_EQ((int64_t)sizeof(ObSSTableBlockIndexHeader),
                  index_builder.get_index_block_size());
        EXPECT_EQ(0, index_builder.get_end_key_char_stream_offset());
        EXPECT_EQ(0, index_builder.get_index_items_size());
        EXPECT_EQ(0, index_builder.get_end_keys_size());


        EXPECT_EQ(0, index_builder.get_index_items_buffer()->get_data_size());
        EXPECT_EQ(0, index_builder.get_end_keys_buffer()->get_data_size());
        EXPECT_EQ(ObSSTableBlockIndexBuffer::DEFAULT_MEM_BLOCK_SIZE, index_builder.get_index_items_buffer()->get_total_size());
        EXPECT_EQ(ObSSTableBlockIndexBuffer::DEFAULT_MEM_BLOCK_SIZE, index_builder.get_end_keys_buffer()->get_total_size());

      }

      TEST_F(TestObSSTableBlockIndexBuilder, test_build_index_block)
      {
        ObSSTableBlockIndexBuilder index_builder;
        ObSSTableBlockIndexHeader deserialize_header;
        ObSSTableBlockIndexItem index_item;

        char *key = (char*)"test test test test test test test ";
        int64_t key_len = strlen(key);

        char *index_block = NULL;
        int64_t index_size = 0;
        int64_t pos = 0;
        ObString end_key;
        int i = 0;
        int ret;

        end_key.assign(key, static_cast<int32_t>(strlen(key)));
        for (i = 0; i < 204800; ++i)
        {
          ret = index_builder.add_entry(table_id, i/100, end_key, 320);
          EXPECT_TRUE(OB_SUCCESS == ret);
          EXPECT_EQ(i + 1, index_builder.get_block_count());
          EXPECT_EQ((int64_t)sizeof(ObSSTableBlockIndexItem) * (i + 1),
                    index_builder.get_index_items_size());
          EXPECT_EQ((int64_t)key_len * (i + 1), index_builder.get_end_keys_size());
          EXPECT_EQ((int64_t)(sizeof(ObSSTableBlockIndexHeader)
                    + (sizeof(ObSSTableBlockIndexItem) + key_len) * (i + 1)),
                    index_builder.get_index_block_size());
        }

        index_block = (char*)ob_malloc(index_builder.get_index_block_size(), ObModIds::TEST);
        EXPECT_TRUE(NULL != index_block);
        ret = index_builder.build_block_index(true, index_block,
                                              index_builder.get_index_block_size(),
                                              index_size);
        EXPECT_TRUE(OB_SUCCESS == ret);

        EXPECT_EQ(i, index_builder.get_block_count());
        EXPECT_EQ((int64_t)sizeof(ObSSTableBlockIndexItem) * i,
                  index_builder.get_index_items_size());
        EXPECT_EQ((int64_t)key_len * i, index_builder.get_end_keys_size());
        EXPECT_EQ((int64_t)(sizeof(ObSSTableBlockIndexHeader)
                  + (sizeof(ObSSTableBlockIndexItem) + key_len) * i),
                  index_builder.get_index_block_size());
        EXPECT_EQ((int64_t)(sizeof(ObSSTableBlockIndexHeader)
                  + sizeof(ObSSTableBlockIndexItem) * i),
                  index_builder.get_end_key_char_stream_offset());

        ret = deserialize_header.deserialize(index_block,
                                             sizeof(ObSSTableBlockIndexHeader), pos);
        EXPECT_TRUE(OB_SUCCESS == ret);
        EXPECT_EQ(i, deserialize_header.sstable_block_count_);
        EXPECT_EQ((int64_t)(sizeof(ObSSTableBlockIndexHeader)
                  + sizeof(ObSSTableBlockIndexItem) * i),
                  deserialize_header.end_key_char_stream_offset_);
        EXPECT_EQ(0, deserialize_header.rowkey_flag_);
        EXPECT_EQ(0, deserialize_header.reserved16_);
        EXPECT_EQ(0, deserialize_header.reserved64_[0]);
        EXPECT_EQ(0, deserialize_header.reserved64_[1]);

        const char *end_keys = index_block
          + deserialize_header.end_key_char_stream_offset_;
        for (i = 0; i < 204800; ++i)
        {
          pos = 0;
          ret = index_item.deserialize(index_block + sizeof(ObSSTableBlockIndexHeader)
                                       + sizeof(ObSSTableBlockIndexItem) * i,
                                       sizeof(ObSSTableBlockIndexItem), pos);
          EXPECT_TRUE(OB_SUCCESS == ret);

          EXPECT_EQ(320, index_item.block_record_size_);
          EXPECT_EQ(key_len, index_item.block_end_key_size_);
          EXPECT_EQ(0, index_item.reserved_);

          EXPECT_TRUE(memcmp(key, end_keys + i * key_len, key_len) == 0);
        }

        if (NULL != index_block)
        {
          ob_free(index_block);
        }
      }
    }//end namespace chunkserver
  }//end namespace tests
}//end namespace oceanbase

int main(int argc, char** argv)
{
  ob_init_memory_pool();
  TBSYS_LOGGER.setLogLevel("ERROR");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
