/**
 * (C) 2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * test_block_cache_reader_loader.cpp for test block cache
 * reader and loader.
 *
 * Authors:
 *   huating <huating.zmq@taobao.com>
 *
 */

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include "gtest/gtest.h"
#include "common/ob_common_param.h"
#include "common/ob_common_stat.h"
#include "sstable/ob_blockcache.h"
#include "sstable/ob_sstable_writer.h"
#include "sstable/ob_disk_path.h"
#include "chunkserver/ob_fileinfo_cache.h"
#include "chunkserver/ob_block_cache_reader.h"
#include "chunkserver/ob_block_cache_loader.h"
#include "chunkserver/ob_switch_cache_utility.h"
#include "chunkserver/ob_chunk_server_config.h"
#include "../common/test_rowkey_helper.h"

using namespace oceanbase::common;
using namespace oceanbase::sstable;
using namespace oceanbase::chunkserver;


namespace oceanbase
{
  namespace sstable
  {
  }
}

namespace oceanbase
{
  namespace tests
  {
    namespace chunkserver
    {
      static const int64_t table_id = 100;
      static const int64_t sstable_file_id = 1001;
      static const int64_t sstable_file_offset = 0;
      static const int64_t DISK_NUM = 12;
      static const int64_t SSTABLE_NUM = DISK_NUM * 2;
      static const int64_t SSTABLE_ROW_NUM = 100;
      static const int64_t ROW_NUM = SSTABLE_NUM * SSTABLE_ROW_NUM;
      static const int64_t COL_NUM = 5;
      static const int64_t NON_EXISTENT_ROW_NUM = 100;
      static const ObString table_name(strlen("sstable") + 1, strlen("sstable") + 1, (char*)"sstable");
      static const int64_t OB_MAX_GET_COLUMN_NUMBER = 128;

      static char sstable_file_path[OB_MAX_FILE_NAME_LENGTH];
      static ObCellInfo** cell_infos;
      static char* row_key_strs[ROW_NUM + NON_EXISTENT_ROW_NUM][COL_NUM];
      static FileInfoCache fic;

      static ModulePageAllocator mod(0);
      static ModuleArena allocator(ModuleArena::DEFAULT_PAGE_SIZE, mod);
      static ObSSTableReader reader_(allocator, fic);
      static CharArena allocator_;


      class TestObBlockCacheReaderLoader: public ::testing::Test
      {
      public:
        TestObBlockCacheReaderLoader()
        : old_index_cache_(fic), old_block_cache_(fic),
          new_index_cache_(fic), new_block_cache_(fic)
        {
        }

        ~TestObBlockCacheReaderLoader()
        {
        }

        int init()
        {
          int ret = OB_SUCCESS;

          EXPECT_EQ(OB_SUCCESS, ret);

          ret = old_block_cache_.init(conf_.block_cache_size);
          EXPECT_EQ(OB_SUCCESS, ret);
          ret = new_block_cache_.init(conf_.block_cache_size);
          EXPECT_EQ(OB_SUCCESS, ret);

          ret = old_index_cache_.init(conf_.block_index_cache_size);
          EXPECT_EQ(OB_SUCCESS, ret);
          ret = new_index_cache_.init(conf_.block_index_cache_size);
          EXPECT_EQ(OB_SUCCESS, ret);

          return ret;
        }

        static int init_sstable(ObSSTableReader& sstable, const ObCellInfo** cell_infos,
            const int64_t row_num, const int64_t col_num, const int64_t sst_id = 0L)
        {
          int err = OB_SUCCESS;
          UNUSED(sstable);

          ObSSTableSchema sstable_schema;
          ObSSTableSchemaColumnDef column_def;

          EXPECT_TRUE(NULL != cell_infos);
          EXPECT_TRUE(row_num > 0);
          EXPECT_TRUE(col_num > 0);

          uint64_t table_id = cell_infos[0][0].table_id_;
          ObString path;
          int64_t sstable_file_id = 0;
          ObString compress_name;
          char* path_str = sstable_file_path;
          int64_t path_len = OB_MAX_FILE_NAME_LENGTH;

          column_def.reserved_ = 0;
          column_def.rowkey_seq_ = 1;
          column_def.column_group_id_= 0;
          column_def.column_name_id_ = 1; //rowkey column id;
          column_def.column_value_type_ = ObVarcharType;
          column_def.table_id_ = static_cast<uint32_t>(table_id);
          sstable_schema.add_column_def(column_def);

          for (int64_t i = 0; i < col_num; ++i)
          {
            column_def.reserved_ = 0;
            column_def.rowkey_seq_ = 0;
            if (i >=2)
            {
              column_def.column_group_id_= 2;
            }
            else
            {
              column_def.column_group_id_= 0;
            }
            column_def.column_name_id_ = static_cast<uint16_t>(cell_infos[0][i].column_id_);
            column_def.column_value_type_ = cell_infos[0][i].value_.get_type();
            column_def.table_id_ = static_cast<uint32_t>(table_id);
            sstable_schema.add_column_def(column_def);
          }

          if (0 == sst_id)
          {
            sstable_file_id = 100;
          }
          else
          {
            sstable_file_id = sst_id;
          }

          ObSSTableId sstable_id(sst_id);
          get_sstable_path(sstable_id, path_str, path_len);
          char cmd[256];
          sprintf(cmd, "mkdir -p %s", path_str);
          system(cmd);
          path.assign((char*)path_str, static_cast<int32_t>(strlen(path_str)));
          compress_name.assign((char*)"lzo_1.0", static_cast<int32_t>(strlen("lzo_1.0")));
          remove(path.ptr());

          ObSSTableWriter writer;
          err = writer.create_sstable(sstable_schema, path, compress_name, 0, 1, 4 * 1024);
          EXPECT_EQ(OB_SUCCESS, err);

          for (int64_t i = 0; i < row_num; ++i)
          {
            ObSSTableRow row;
            row.set_table_id(table_id);
            row.set_column_group_id(0);
            err = row.set_rowkey(cell_infos[i][0].row_key_);
            EXPECT_EQ(OB_SUCCESS, err);
            for (int64_t j = 0; j < 2; ++j)
            {
              err = row.add_obj(cell_infos[i][j].value_);
              EXPECT_EQ(OB_SUCCESS, err);
            }

            int64_t space_usage = 0;
            err = writer.append_row(row, space_usage);
            EXPECT_EQ(OB_SUCCESS, err);
          }

          for (int64_t i = 0; i < row_num; ++i)
          {
            ObSSTableRow row;
            row.set_table_id(table_id);
            row.set_column_group_id(2);
            err = row.set_rowkey(cell_infos[i][0].row_key_);
            EXPECT_EQ(OB_SUCCESS, err);
            for (int64_t j = 2; j < col_num; ++j)
            {
              err = row.add_obj(cell_infos[i][j].value_);
              EXPECT_EQ(OB_SUCCESS, err);
            }

            int64_t space_usage = 0;
            err = writer.append_row(row, space_usage);
            EXPECT_EQ(OB_SUCCESS, err);
          }

          int64_t offset = 0;
          err = writer.close_sstable(offset);
          EXPECT_EQ(OB_SUCCESS, err);

          err = fic.init(1024);
          EXPECT_EQ(OB_SUCCESS, err);

          err = reader_.open(sstable_id, 0);
          EXPECT_EQ(OB_SUCCESS, err);
          EXPECT_TRUE(reader_.is_opened());

          return err;
        }

      public:
        static void SetUpTestCase()
        {
          int err = OB_SUCCESS;

          //malloc
          cell_infos = new ObCellInfo*[ROW_NUM + NON_EXISTENT_ROW_NUM];
          for (int64_t i = 0; i < ROW_NUM + NON_EXISTENT_ROW_NUM; ++i)
          {
            cell_infos[i] = new ObCellInfo[COL_NUM];
          }

          for (int64_t i = 0; i < ROW_NUM + NON_EXISTENT_ROW_NUM; ++i)
          {
            for (int64_t j = 0; j < COL_NUM; ++j)
            {
              row_key_strs[i][j] = new char[50];
            }
          }

          // init cell infos
          for (int64_t i = 0; i < ROW_NUM + NON_EXISTENT_ROW_NUM; ++i)
          {
            for (int64_t j = 0; j < COL_NUM; ++j)
            {
              cell_infos[i][j].table_id_ = table_id;
              sprintf(row_key_strs[i][j], "row_key_%08ld", i);
              cell_infos[i][j].row_key_ = make_rowkey(row_key_strs[i][j], &allocator_);
              cell_infos[i][j].column_id_ = j + 2;
              cell_infos[i][j].value_.set_int(1000 + i * COL_NUM + j);
            }
          }

          //init sstable
          err = init_sstable(reader_, (const ObCellInfo**)cell_infos,
                             ROW_NUM, COL_NUM, sstable_file_id);
          EXPECT_EQ(OB_SUCCESS, err);
        }

        static void TearDownTestCase()
        {
          for (int64_t i = 0; i < ROW_NUM + NON_EXISTENT_ROW_NUM; ++i)
          {
            for (int64_t j = 0; j < COL_NUM; ++j)
            {
              if (NULL != row_key_strs[i][j])
              {
                delete[] row_key_strs[i][j];
                row_key_strs[i][j] = NULL;
              }
            }
          }

          for (int64_t i = 0; i < ROW_NUM + NON_EXISTENT_ROW_NUM; ++i)
          {
            if (NULL != cell_infos[i])
            {
              delete[] cell_infos[i];
              cell_infos[i] = NULL;
            }
          }
          if (NULL != cell_infos)
          {
            delete[] cell_infos;
          }

          reader_.reset();
        }

      public:
        virtual void SetUp()
        {
          int ret;

          ret = init();
          ASSERT_EQ(OB_SUCCESS, ret);
        }

        virtual void TearDown()
        {
          int ret;

          ret = cache_utility_.destroy_cache(old_block_cache_, old_index_cache_);
          ASSERT_EQ(OB_SUCCESS, ret);
          ret = cache_utility_.destroy_cache(new_block_cache_,new_index_cache_);
          ASSERT_EQ(OB_SUCCESS, ret);
        }

      public:
        ObChunkServerConfig conf_;
        ObBlockIndexCache old_index_cache_;
        ObBlockCache old_block_cache_;
        ObBlockIndexCache new_index_cache_;
        ObBlockCache new_block_cache_;
        ObBlockCacheReader cache_reader_;
        ObBlockCacheLoader cache_loader_;
        ObSwitchCacheUtility cache_utility_;
      };

      TEST_F(TestObBlockCacheReaderLoader, test_block_cache_loader)
      {
        int ret;
        uint64_t table_id = 100;
        uint64_t column_group_id = 0;

        ObRowkey start_key = cell_infos[0][0].row_key_;
        ObRowkey end_key = cell_infos[ROW_NUM - 1][0].row_key_;

        ret = cache_loader_.load_block_into_cache(old_index_cache_, old_block_cache_,
                                                  table_id, column_group_id,
                                                  start_key, &reader_);
        ASSERT_EQ(OB_SUCCESS, ret);
        ret = cache_loader_.load_block_into_cache(old_index_cache_, old_block_cache_,
                                                  table_id, column_group_id,
                                                  end_key, &reader_);
        ASSERT_EQ(OB_SUCCESS, ret);

        ret = cache_reader_.get_start_key_of_next_block(old_block_cache_, table_id,
                                                        column_group_id, start_key, &reader_);
        ASSERT_EQ(OB_SUCCESS, ret);
        ret = cache_loader_.load_block_into_cache(new_index_cache_, new_block_cache_,
                                                  table_id, column_group_id,
                                                  start_key, &reader_);
        ASSERT_EQ(OB_SUCCESS, ret);
        ret = cache_reader_.get_start_key_of_next_block(old_block_cache_, table_id,
                                                        column_group_id, end_key, &reader_);
        ASSERT_EQ(OB_SUCCESS, ret);
        ret = cache_loader_.load_block_into_cache(new_index_cache_, new_block_cache_,
                                                  table_id, column_group_id,
                                                  end_key, &reader_);
        ASSERT_EQ(OB_SUCCESS, ret);
        ret = cache_reader_.get_start_key_of_next_block(old_block_cache_, table_id,
                                                        column_group_id, end_key, &reader_);
        ASSERT_EQ(OB_ITER_END, ret);
      }

      TEST_F(TestObBlockCacheReaderLoader, test_block_cache_loader_twice)
      {
        int ret;
        uint64_t table_id = 100;
        uint64_t column_group_id = 0;

        ObRowkey start_key = cell_infos[0][0].row_key_;

        ret = cache_loader_.load_block_into_cache(old_index_cache_, old_block_cache_,
                                                  table_id, column_group_id,
                                                  start_key, &reader_);
        ASSERT_EQ(OB_SUCCESS, ret);
        ret = cache_loader_.load_block_into_cache(old_index_cache_, old_block_cache_,
                                                  table_id, column_group_id,
                                                  start_key, &reader_);
        ASSERT_EQ(OB_SUCCESS, ret);

        ret = cache_reader_.get_start_key_of_next_block(old_block_cache_, table_id,
                                                        column_group_id, start_key, &reader_);
        ASSERT_EQ(OB_SUCCESS, ret);
        ret = cache_loader_.load_block_into_cache(new_index_cache_, new_block_cache_,
                                                  table_id, column_group_id,
                                                  start_key, &reader_);
        ASSERT_EQ(OB_SUCCESS, ret);
        ret = cache_reader_.get_start_key_of_next_block(old_block_cache_, table_id,
                                                        column_group_id, start_key, &reader_);
        ASSERT_EQ(OB_ITER_END, ret);
      }

      TEST_F(TestObBlockCacheReaderLoader, test_block_cache_switch)
      {
        int ret;
        uint64_t table_id = 100;
        uint64_t column_group_id = 0;

        ObRowkey start_key = cell_infos[0][0].row_key_;
        ObRowkey end_key = cell_infos[ROW_NUM - 1][0].row_key_;

        ret = cache_loader_.load_block_into_cache(old_index_cache_, old_block_cache_,
                                                  table_id, column_group_id,
                                                  start_key, &reader_);
        ASSERT_EQ(OB_SUCCESS, ret);
        ret = cache_loader_.load_block_into_cache(old_index_cache_, old_block_cache_,
                                                  table_id, column_group_id,
                                                  end_key, &reader_);
        ASSERT_EQ(OB_SUCCESS, ret);

        ret = cache_reader_.get_start_key_of_next_block(old_block_cache_, table_id,
                                                        column_group_id, start_key, &reader_);
        ASSERT_EQ(OB_SUCCESS, ret);
        ret = cache_loader_.load_block_into_cache(new_index_cache_, new_block_cache_,
                                                  table_id, column_group_id,
                                                  start_key, &reader_);
        ASSERT_EQ(OB_SUCCESS, ret);
        //ret = cache_reader_.get_start_key_of_next_block(old_block_cache_, table_id,
        //                                                column_group_id, end_key, &reader_);
        //ASSERT_EQ(OB_SUCCESS, ret);
        //ret = cache_loader_.load_block_into_cache(new_index_cache_, new_block_cache_,
        //                                          table_id, column_group_id,
        //                                          end_key, &reader_);
        //ASSERT_EQ(OB_SUCCESS, ret);
        ret = cache_reader_.get_start_key_of_next_block(old_block_cache_, table_id,
                                                        column_group_id, end_key, &reader_);
        ASSERT_EQ(OB_ITER_END, ret);

        ret = cache_utility_.destroy_cache(old_block_cache_, old_index_cache_);
        ASSERT_EQ(OB_SUCCESS, ret);
        old_block_cache_.set_fileinfo_cache(fic);
        ret = old_block_cache_.init(conf_.block_cache_size);
        EXPECT_EQ(OB_SUCCESS, ret);
        old_index_cache_.set_fileinfo_cache(fic);
        ret = old_index_cache_.init(conf_.block_index_cache_size);
        EXPECT_EQ(OB_SUCCESS, ret);

        ret = cache_reader_.get_start_key_of_next_block(new_block_cache_, table_id,
                                                        column_group_id, start_key, &reader_);
        ASSERT_EQ(OB_SUCCESS, ret);
        ret = cache_loader_.load_block_into_cache(old_index_cache_, old_block_cache_,
                                                  table_id, column_group_id,
                                                  start_key, &reader_);
        ASSERT_EQ(OB_SUCCESS, ret);
        //ret = cache_reader_.get_start_key_of_next_block(new_block_cache_, table_id,
        //                                                column_group_id, end_key, &reader_);
        //ASSERT_EQ(OB_SUCCESS, ret);
        //ret = cache_loader_.load_block_into_cache(old_index_cache_, old_block_cache_,
        //                                          table_id, column_group_id,
        //                                          end_key, &reader_);
        //ASSERT_EQ(OB_SUCCESS, ret);
        ret = cache_reader_.get_start_key_of_next_block(old_block_cache_, table_id,
                                                        column_group_id, end_key, &reader_);
        ASSERT_EQ(OB_ITER_END, ret);
      }

      TEST_F(TestObBlockCacheReaderLoader, test_block_cache_load_one_file)
      {
        int ret;
        uint64_t table_id = 100;
        uint64_t column_group_id = 0;
        uint64_t column_group_id2 = 2;
        ObRowkey row_key;

        for (int i = 0; i < ROW_NUM; i++)
        {
          row_key = cell_infos[i][0].row_key_;
          ret = cache_loader_.load_block_into_cache(old_index_cache_, old_block_cache_,
                                                    table_id, column_group_id,
                                                    row_key, &reader_);
          ASSERT_EQ(OB_SUCCESS, ret);
          ret = cache_loader_.load_block_into_cache(old_index_cache_, old_block_cache_,
                                                    table_id, column_group_id2,
                                                    row_key, &reader_);
          ASSERT_EQ(OB_SUCCESS, ret);
        }

        for (int i = 0; i < ROW_NUM; i++)
        {
          ret = cache_reader_.get_start_key_of_next_block(old_block_cache_, table_id,
                                                          column_group_id, row_key, &reader_);
          if (OB_ITER_END == ret)
          {
            break;
          }
          else
          {
            ASSERT_EQ(OB_SUCCESS, ret);
          }
          ret = cache_loader_.load_block_into_cache(new_index_cache_, new_block_cache_,
                                                    table_id, column_group_id,
                                                    row_key, &reader_);
          ASSERT_EQ(OB_SUCCESS, ret);
        }
      }

    }//end namespace common
  }//end namespace tests
}//end namespace oceanbase

int main(int argc, char** argv)
{
  ob_init_memory_pool();
  TBSYS_LOGGER.setLogLevel("WARN");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
