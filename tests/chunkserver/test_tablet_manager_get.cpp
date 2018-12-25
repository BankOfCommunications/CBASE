/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * test_tablet_manager_get.cpp for test get interface of tablet
 * manager, it also test the switch cache feature.
 *
 * Authors:
 *   huating <huating.zmq@taobao.com>
 *
 */

#include <tblog.h>
#include <gtest/gtest.h>
#include "ob_action_flag.h"
#include "thread_buffer.h"
#include "ob_scanner.h"
#include "sstable/ob_sstable_reader.h"
#include "sstable/ob_sstable_writer.h"
#include "sstable/ob_sstable_getter.h"
#include "ob_tablet_manager.h"
#include "sstable/ob_disk_path.h"
#include "chunkserver/ob_chunk_server_config.h"
#include "../common/test_rowkey_helper.h"

using namespace std;
using namespace oceanbase::common;
using namespace oceanbase::chunkserver;
using namespace oceanbase::sstable;

static CharArena allocator_;

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
      static ObTabletManager tablet_mgr;
      static ObChunkServerConfig config;
      //add dragon [repair_test] 2016-12-30
      static ObClientManager client_mgr;
      //add e

      class TestObTabletManagerGet : public ::testing::Test
      {
      public:
        void check_string(const ObString& expected, const ObString& real)
        {
          EXPECT_EQ(expected.length(), real.length());
          if (NULL != expected.ptr() && NULL != real.ptr())
          {
            EXPECT_EQ(0, memcmp(expected.ptr(), real.ptr(), expected.length()));
          }
          else
          {
            EXPECT_EQ((const char*) NULL, expected.ptr());
            EXPECT_EQ((const char*) NULL, real.ptr());
          }
        }

        void check_obj(const ObObj& real, int type, int64_t exp_val)
        {
          int64_t real_val;

          if (ObMinType != type)
          {
            EXPECT_EQ(type, real.get_type());
          }
          if (ObIntType == type)
          {
            real.get_int(real_val);
            EXPECT_EQ(exp_val, real_val);
          }
        }

        void check_cell(const ObCellInfo& expected, const ObCellInfo& real,
                        int type=ObIntType, uint64_t column_id = UINT64_MAX)
        {
          int64_t exp_val = 0;

          if (UINT64_MAX == column_id)
          {
            ASSERT_EQ(expected.column_id_, real.column_id_);
          }
          else
          {
            ASSERT_EQ(column_id, real.column_id_);
          }
          ASSERT_EQ(expected.table_id_, real.table_id_);
          EXPECT_EQ(expected.row_key_, real.row_key_);

          if (ObIntType == type)
          {
            expected.value_.get_int(exp_val);
          }
          check_obj(real.value_, type, exp_val);
        }

        void test_adjacent_row_query(const int32_t row_index = 0,
                                     const int32_t row_count = 1,
                                     const int32_t column_count = COL_NUM)
        {
          int ret = OB_SUCCESS;
          ObGetParam get_param;
          ObScanner scanner;
          ObCellInfo ci;
          ObCellInfo expected;

          for (int i = row_index; i < row_index + row_count; i++)
          {
            for (int j = 0; j < column_count; j++)
            {
              ret = get_param.add_cell(cell_infos[i][j]);
              EXPECT_EQ(OB_SUCCESS, ret);
            }
          }

          ret = tablet_mgr.get(get_param, scanner);
          ASSERT_EQ(OB_SUCCESS, ret);

          // check result
          int64_t count = 0;
          ObScannerIterator iter;
          for (iter = scanner.begin(); iter != scanner.end(); iter++)
          {
            expected = cell_infos[row_index + count / column_count][count % column_count];
            expected.table_name_ = table_name;
            EXPECT_EQ(OB_SUCCESS, iter.get_cell(ci));
            check_cell(expected, ci);
            ++count;
          }
          EXPECT_EQ(row_count * column_count, count);
        }

        void test_full_row_query(const int32_t row_index = 0,
                                 const int32_t row_count = 1)
        {
          int ret = OB_SUCCESS;
          ObGetParam get_param;
          ObCellInfo param_cell;
          ObScanner scanner;
          ObCellInfo ci;
          ObCellInfo expected;
          bool is_fullfilled = false;
          int64_t fullfilled_num = 0;

          for (int i = row_index; i < row_index + row_count; i++)
          {
            param_cell = cell_infos[i][0];
            param_cell.column_id_ = 0;
            ret = get_param.add_cell(param_cell);
            EXPECT_EQ(OB_SUCCESS, ret);
          }

          ret = tablet_mgr.get(get_param, scanner);
          ASSERT_EQ(OB_SUCCESS, ret);

          // check result
          int64_t count = 0;
          int64_t col_num = COL_NUM + 1;
          ObScannerIterator iter;
          for (iter = scanner.begin(); iter != scanner.end(); iter++)
          {
            EXPECT_EQ(OB_SUCCESS, iter.get_cell(ci));
            if (count % col_num == 0)
            {
              // rowkey column
              EXPECT_EQ((uint64_t)1, ci.column_id_);
            }
            else
            {
              expected = cell_infos[row_index + count / col_num][count % col_num - 1];
              expected.table_name_ = table_name;
              check_cell(expected, ci);
            }
            ++count;
          }
          EXPECT_EQ(row_count * col_num, count);

          scanner.get_is_req_fullfilled(is_fullfilled, fullfilled_num);
          EXPECT_TRUE(is_fullfilled);
          EXPECT_EQ(row_count, fullfilled_num);
        }

        void test_nonexistent_row_query(const int32_t row_index = 0,
                                        const int32_t row_count = 1,
                                        const int32_t column_count = COL_NUM,
                                        const int32_t nonexistent_row_index = 0,
                                        const int32_t nonexistent_row_count = 1,
                                        const int32_t nonexistent_column_count = COL_NUM)
        {
          int ret = OB_SUCCESS;
          ObGetParam get_param;
          ObScanner scanner;
          ObCellInfo ci;
          ObCellInfo expected;
          int64_t flag;
          int64_t index_i = 0;
          bool is_fullfilled = false;
          int64_t fullfilled_num = 0;

          for (int i = row_index; i < row_index + row_count; i++)
          {
            for (int j = 0; j < column_count; j++)
            {
              ret = get_param.add_cell(cell_infos[i][j]);
              EXPECT_EQ(OB_SUCCESS, ret);
            }
          }

          for (int i = nonexistent_row_index;
               i < nonexistent_row_index + nonexistent_row_count; i++)
          {
            for (int j = 0; j < nonexistent_column_count; j++)
            {
              ret = get_param.add_cell(cell_infos[i][j]);
              EXPECT_EQ(OB_SUCCESS, ret);
            }
          }

          ret = tablet_mgr.get(get_param, scanner);
          if (scanner.is_empty())
          {
            ASSERT_EQ(OB_CS_TABLET_NOT_EXIST, ret);
          }
          else
          {
            ASSERT_EQ(OB_SUCCESS, ret);
          }

          // check result
          int64_t count = 0;
          ObScannerIterator iter;
          for (iter = scanner.begin(); iter != scanner.end(); iter++)
          {
            if (0 == column_count || count >= row_count * column_count)
            {
              index_i = nonexistent_row_index + (count - row_count * column_count);
              expected = cell_infos[index_i][0];
              EXPECT_EQ(OB_SUCCESS, iter.get_cell(ci));
              ASSERT_EQ(expected.table_id_, ci.table_id_);
              EXPECT_EQ(expected.row_key_, ci.row_key_);
              ret = ci.value_.get_ext(flag);
              EXPECT_EQ(OB_SUCCESS, ret);
              EXPECT_EQ((int64_t)ObActionFlag::OP_ROW_DOES_NOT_EXIST, flag);
            }
            else
            {
              expected = cell_infos[row_index + count / column_count][count % column_count];
              expected.table_name_ = table_name;
              EXPECT_EQ(OB_SUCCESS, iter.get_cell(ci));
              check_cell(expected, ci);
            }
            ++count;
          }
          EXPECT_EQ(row_count * column_count, count);

          scanner.get_is_req_fullfilled(is_fullfilled, fullfilled_num);
          EXPECT_TRUE(is_fullfilled);
          EXPECT_EQ(row_count * column_count, fullfilled_num);
        }

        void test_nonexistent_full_row_query(const int32_t row_index = 0,
                                             const int32_t row_count = 1,
                                             const int32_t column_count = COL_NUM,
                                             const int32_t nonexistent_row_index = 0,
                                             const int32_t nonexistent_row_count = 1)
        {
          int ret = OB_SUCCESS;
          ObGetParam get_param;
          ObScanner scanner;
          ObCellInfo ci;
          ObCellInfo expected;
          ObCellInfo param_cell;
          int64_t flag;
          int64_t index_i = 0;
          bool is_fullfilled = false;
          int64_t fullfilled_num = 0;

          for (int i = row_index; i < row_index + row_count; i++)
          {
            for (int j = 0; j < column_count; j++)
            {
              ret = get_param.add_cell(cell_infos[i][j]);
              EXPECT_EQ(OB_SUCCESS, ret);
            }
          }

          for (int i = nonexistent_row_index;
               i < nonexistent_row_index + nonexistent_row_count; i++)
          {
            param_cell = cell_infos[i][0];
            param_cell.column_id_ = 0;
            ret = get_param.add_cell(param_cell);
            EXPECT_EQ(OB_SUCCESS, ret);
          }

          ret = tablet_mgr.get(get_param, scanner);
          if (scanner.is_empty())
          {
            ASSERT_EQ(OB_CS_TABLET_NOT_EXIST, ret);
          }
          else
          {
            ASSERT_EQ(OB_SUCCESS, ret);
          }

          // check result
          int64_t count = 0;
          ObScannerIterator iter;
          for (iter = scanner.begin(); iter != scanner.end(); iter++)
          {
            if (0 == column_count || count >= row_count * column_count)
            {
              index_i = nonexistent_row_index + (count - row_count * column_count);
              expected = cell_infos[index_i][0];
              EXPECT_EQ(OB_SUCCESS, iter.get_cell(ci));
              ASSERT_EQ(expected.table_id_, ci.table_id_);
              EXPECT_EQ(expected.row_key_, ci.row_key_);
              ret = ci.value_.get_ext(flag);
              EXPECT_EQ(OB_SUCCESS, ret);
              EXPECT_EQ((int64_t)ObActionFlag::OP_ROW_DOES_NOT_EXIST, flag);
            }
            else
            {
              expected = cell_infos[row_index + count / column_count][count % column_count];
              expected.table_name_ = table_name;
              EXPECT_EQ(OB_SUCCESS, iter.get_cell(ci));
              check_cell(expected, ci);
            }
            ++count;
          }
          EXPECT_EQ(row_count * column_count, count);

          scanner.get_is_req_fullfilled(is_fullfilled, fullfilled_num);
          EXPECT_TRUE(is_fullfilled);
          EXPECT_EQ(row_count * column_count, fullfilled_num);
        }

        void test_nonexistent_column_query(const int32_t row_index = 0,
                                           const int32_t row_count = 1,
                                           const int32_t column_count = 0,
                                           const int32_t nonexistent_column_count = COL_NUM)
        {
          int ret = OB_SUCCESS;
          ObGetParam get_param;
          ObScanner scanner;
          ObCellInfo ci;
          ObCellInfo expected;
          ObCellInfo param_cell;
          int64_t index_i = 0;
          int64_t index_j = 0;

          for (int i = row_index; i < row_index + row_count; i++)
          {
            for (int j = 0; j < column_count; j++)
            {
              ret = get_param.add_cell(cell_infos[i][j]);
              EXPECT_EQ(OB_SUCCESS, ret);
            }
            for (int j = column_count; j < column_count + nonexistent_column_count; j++)
            {
              param_cell = cell_infos[i][j];
              param_cell.column_id_ = COL_NUM + 10;
              ret = get_param.add_cell(param_cell);
              EXPECT_EQ(OB_SUCCESS, ret);
            }
          }

          ret = tablet_mgr.get(get_param, scanner);
          ASSERT_EQ(OB_SUCCESS, ret);

          // check result
          int64_t count = 0;
          ObScannerIterator iter;
          for (iter = scanner.begin(); iter != scanner.end(); iter++)
          {
            index_i = row_index + count / (column_count + nonexistent_column_count);
            index_j = count % (column_count + nonexistent_column_count);
            expected = cell_infos[index_i][index_j];
            expected.table_name_ = table_name;
            EXPECT_EQ(OB_SUCCESS, iter.get_cell(ci));

            //when reading the first column group, handle non-existent column
            if (column_count <= 2 && index_j < column_count)
            {
              check_cell(expected, ci);
            }
            else if (column_count > 2 && index_j >= 2 && index_j < 2 + nonexistent_column_count)
            {
              check_cell(expected, ci, ObNullType, COL_NUM + 10);
            }
            else if (column_count > 2 && index_j >= 2 + nonexistent_column_count)
            {
              expected = cell_infos[index_i][index_j - nonexistent_column_count];
              check_cell(expected, ci);
            }
            ++count;
          }
          EXPECT_EQ(row_count * (column_count + nonexistent_column_count), count);
        }

      public:
        static void init_cell_infos()
        {
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
        }

        static void destroy_cell_infos()
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
        }

        static int init_sstable(const ObCellInfo** cell_infos, const int64_t row_num,
                                const int64_t col_num, const int64_t sst_id = 0L)
        {
          int err = OB_SUCCESS;

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

          // add rowkey column
          column_def.reserved_ = 0;
          column_def.rowkey_seq_ = 1;
          column_def.column_group_id_= 0;
          column_def.column_name_id_ = 1;
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
          err = writer.create_sstable(sstable_schema, path, compress_name, 0);
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

          return err;
        }

        static int create_tablet(const ObNewRange& range, const ObSSTableId& sst_id,
                                 bool serving = true, const int64_t version = 1)
        {
          int err = OB_SUCCESS;
          UNUSED(serving);

          if (range.empty())
          {
            TBSYS_LOG(ERROR, "create_tablet error, input range is empty.");
            err = OB_INVALID_ARGUMENT;
          }

          if (OB_SUCCESS == err)
          {
            ObTablet* tablet = NULL;
            // find tablet if exist?
            ObMultiVersionTabletImage &image = tablet_mgr.get_serving_tablet_image();
            err = image.acquire_tablet(range, ObMultiVersionTabletImage::SCAN_FORWARD, version, tablet);
            if (OB_SUCCESS == err)
            {
              TBSYS_LOG(ERROR, "tablet already exists! dump input and exist:");
              range.hex_dump(TBSYS_LOG_LEVEL_ERROR);
              tablet->get_range().hex_dump(TBSYS_LOG_LEVEL_ERROR);
              err = OB_ERROR;
              tablet = NULL;
              image.release_tablet(tablet);
            }
            else
            {
              err = image.alloc_tablet_object(range, version, tablet);
              if (OB_SUCCESS == err)
              {
                err = tablet->add_sstable_by_id(sst_id);
                EXPECT_EQ(OB_SUCCESS, err);
              }
              if (OB_SUCCESS == err)
              {
                err = image.add_tablet(tablet, true, true);
                EXPECT_EQ(OB_SUCCESS, err);
              }
            }
          }

          return err;
        }

        static int create_and_load_tablets()
        {
          int ret = OB_SUCCESS;
          ObNewRange ranges[SSTABLE_NUM];
          ObSSTableId sst_ids[SSTABLE_NUM];

          for (int64_t i = 0; i < SSTABLE_NUM; ++i)
          {
            sst_ids[i].sstable_file_id_ = i % DISK_NUM + 256 * (i / DISK_NUM + 1) + 1;
            sst_ids[i].sstable_file_offset_ = 0;

            ret = init_sstable((const ObCellInfo**)(&cell_infos[i * SSTABLE_ROW_NUM]),
                               SSTABLE_ROW_NUM, COL_NUM, sst_ids[i].sstable_file_id_);
            EXPECT_EQ(OB_SUCCESS, ret);
          }

          // init range
          for (int64_t i = 0; i < SSTABLE_NUM; ++i)
          {
            ranges[i].table_id_ = table_id;
            ranges[i].end_key_ = cell_infos[(i + 1) * SSTABLE_ROW_NUM - 1][0].row_key_;
            ranges[i].border_flag_.set_inclusive_end();
            ranges[i].border_flag_.unset_inclusive_start();

            if (0 == i)
            {
              ranges[i].start_key_.assign(NULL, 0);
              ranges[i].start_key_.set_min_row();
            }
            else
            {
              ranges[i].start_key_ = cell_infos[i * SSTABLE_ROW_NUM - 1][0].row_key_;
            }

            ret = create_tablet(ranges[i], sst_ids[i], true);
            EXPECT_EQ(OB_SUCCESS, ret);
            if (OB_SUCCESS != ret)
            {
              ret = OB_ERROR;
              break;
            }
          }

          if (OB_SUCCESS == ret)
          {
            int32_t disk_no_array[DISK_NUM];
            char path[OB_MAX_FILE_NAME_LENGTH];

            ObMultiVersionTabletImage &image = tablet_mgr.get_serving_tablet_image();
            for (int i = 0; i < DISK_NUM; i++)
            {
              disk_no_array[i] = i + 1;
              ret = get_meta_path(1, disk_no_array[i], false, path, OB_MAX_FILE_NAME_LENGTH);
              EXPECT_EQ(OB_SUCCESS, ret);
              remove(path);
              ret = image.write(1, i + 1);
              EXPECT_EQ(OB_SUCCESS, ret);
            }
          }

          return ret;
        }

        static int init_mgr()
        {
          int err = OB_SUCCESS;

          //mod dragon [repair_test] 2016-12-30
          err = tablet_mgr.init(&config, &client_mgr);
          //err = tablet_mgr.init(&config);
          //mod e

          EXPECT_EQ(OB_SUCCESS, err);

          err = create_and_load_tablets();
          EXPECT_EQ(OB_SUCCESS, err);

          return err;
        }

      public:
        static void SetUpTestCase()
        {
          int err = OB_SUCCESS;

          TBSYS_LOGGER.setLogLevel("ERROR");
          err = ob_init_memory_pool();
          ASSERT_EQ(OB_SUCCESS, err);

          init_cell_infos();

          err = init_mgr();
          ASSERT_EQ(OB_SUCCESS, err);
        }

        static void TearDownTestCase()
        {
          destroy_cell_infos();
        }

        virtual void SetUp()
        {

        }

        virtual void TearDown()
        {

        }
      };

      TEST_F(TestObTabletManagerGet, test_get_first_row)
      {
        test_adjacent_row_query(0, 1, 1);
        test_adjacent_row_query(0, 1, 2);
        test_adjacent_row_query(0, 1, 3);
        test_adjacent_row_query(0, 1, 4);
        test_adjacent_row_query(0, 1, 5);
      }

      TEST_F(TestObTabletManagerGet, test_get_middle_row)
      {
        test_adjacent_row_query((ROW_NUM - 1) / 2, 1, 1);
        test_adjacent_row_query((ROW_NUM - 1) / 2, 1, 2);
        test_adjacent_row_query((ROW_NUM - 1) / 2, 1, 3);
        test_adjacent_row_query((ROW_NUM - 1) / 2, 1, 4);
        test_adjacent_row_query((ROW_NUM - 1) / 2, 1, 5);
      }

      TEST_F(TestObTabletManagerGet, test_get_last_row)
      {
        test_adjacent_row_query(ROW_NUM - 1, 1, 1);
        test_adjacent_row_query(ROW_NUM - 1, 1, 2);
        test_adjacent_row_query(ROW_NUM - 1, 1, 3);
        test_adjacent_row_query(ROW_NUM - 1, 1, 4);
        test_adjacent_row_query(ROW_NUM - 1, 1, 5);
      }

      TEST_F(TestObTabletManagerGet, test_get_x_columns_two_rows)
      {
        test_adjacent_row_query(0, 2, 1);
        test_adjacent_row_query(0, 2, 2);
        test_adjacent_row_query(0, 2, 3);
        test_adjacent_row_query(0, 2, 4);
        test_adjacent_row_query(0, 2, 5);
      }

      TEST_F(TestObTabletManagerGet, test_get_x_columns_ten_rows)
      {
        test_adjacent_row_query(0, 10, 1);
        test_adjacent_row_query(0, 10, 2);
        //test_adjacent_row_query(0, 10, 3);
        //test_adjacent_row_query(0, 10, 4);
        //test_adjacent_row_query(0, 10, 5);
      }

      TEST_F(TestObTabletManagerGet, test_get_x_columns_hundred_rows)
      {
        test_adjacent_row_query(0, 100, 1);
      }

      TEST_F(TestObTabletManagerGet, test_get_max_columns)
      {
        test_adjacent_row_query(0, OB_MAX_GET_COLUMN_NUMBER, 1);
      }

      TEST_F(TestObTabletManagerGet, test_get_one_row)
      {
        test_full_row_query(0, 1);
        test_full_row_query(ROW_NUM / 2, 1);
        test_full_row_query(ROW_NUM - 1, 1);
      }

      TEST_F(TestObTabletManagerGet, test_get_two_rows)
      {
        test_full_row_query(0, 2);
        test_full_row_query(ROW_NUM / 2, 2);
        test_full_row_query(ROW_NUM - 2, 2);
      }

      TEST_F(TestObTabletManagerGet, test_get_ten_rows)
      {
        test_full_row_query(0, 10);
        test_full_row_query(ROW_NUM / 2, 10);
        test_full_row_query(ROW_NUM - 10, 10);
      }

      TEST_F(TestObTabletManagerGet, test_get_one_nonexistent_row)
      {
        test_nonexistent_row_query(0, 0, 0, ROW_NUM, 1, 1);
        test_nonexistent_row_query(0, 1, COL_NUM, ROW_NUM, 1, 1);
        test_nonexistent_row_query(ROW_NUM / 2, 1, COL_NUM, ROW_NUM, 1, 1);
        test_nonexistent_row_query(ROW_NUM - 1, 1, COL_NUM, ROW_NUM, 1, 1);
        test_nonexistent_row_query(0, 2, COL_NUM, ROW_NUM, 1, 1);
        test_nonexistent_row_query(0, 10, COL_NUM, ROW_NUM, 1, 1);

        test_nonexistent_row_query(0, 0, 0, ROW_NUM, 1, COL_NUM);
        test_nonexistent_row_query(0, 1, COL_NUM, ROW_NUM, 1, COL_NUM);
        test_nonexistent_row_query(ROW_NUM / 2, 1, COL_NUM, ROW_NUM, 1, COL_NUM);
        test_nonexistent_row_query(ROW_NUM - 1, 1, COL_NUM, ROW_NUM, 1, COL_NUM);
        test_nonexistent_row_query(0, 2, COL_NUM, ROW_NUM, 1, COL_NUM);
        test_nonexistent_row_query(0, 10, COL_NUM, ROW_NUM, 1, COL_NUM);
      }

      TEST_F(TestObTabletManagerGet, test_get_two_nonexistent_rows)
      {
        test_nonexistent_row_query(0, 0, 0, ROW_NUM, 2, 1);
        test_nonexistent_row_query(0, 1, COL_NUM, ROW_NUM, 2, 1);
        test_nonexistent_row_query(ROW_NUM / 2, 1, COL_NUM, ROW_NUM, 2, 1);
        test_nonexistent_row_query(ROW_NUM - 1, 1, COL_NUM, ROW_NUM, 2, 1);
        test_nonexistent_row_query(0, 2, COL_NUM, ROW_NUM, 2, 1);
        test_nonexistent_row_query(0, 10, COL_NUM, ROW_NUM, 2, 1);

        test_nonexistent_row_query(0, 0, 0, ROW_NUM, 2, COL_NUM);
        test_nonexistent_row_query(0, 1, COL_NUM, ROW_NUM, 2, COL_NUM);
        test_nonexistent_row_query(ROW_NUM / 2, 1, COL_NUM, ROW_NUM, 2, COL_NUM);
        test_nonexistent_row_query(ROW_NUM - 1, 1, COL_NUM, ROW_NUM, 2, COL_NUM);
        test_nonexistent_row_query(0, 2, COL_NUM, ROW_NUM, 2, COL_NUM);
        test_nonexistent_row_query(0, 10, COL_NUM, ROW_NUM, 2, COL_NUM);
      }

      TEST_F(TestObTabletManagerGet, test_get_ten_nonexistent_rows)
      {
        test_nonexistent_row_query(0, 0, 0, ROW_NUM, 10, 1);
        test_nonexistent_row_query(0, 1, COL_NUM, ROW_NUM, 10, 1);
        test_nonexistent_row_query(ROW_NUM / 2, 1, COL_NUM, ROW_NUM, 10, 1);
        test_nonexistent_row_query(ROW_NUM - 1, 1, COL_NUM, ROW_NUM, 10, 1);
        test_nonexistent_row_query(0, 2, COL_NUM, ROW_NUM, 10, 1);
        test_nonexistent_row_query(0, 10, COL_NUM, ROW_NUM, 10, 1);

        test_nonexistent_row_query(0, 0, 0, ROW_NUM, 10, COL_NUM);
        test_nonexistent_row_query(0, 1, COL_NUM, ROW_NUM, 10, COL_NUM);
        test_nonexistent_row_query(ROW_NUM / 2, 1, COL_NUM, ROW_NUM, 10, COL_NUM);
        test_nonexistent_row_query(ROW_NUM - 1, 1, COL_NUM, ROW_NUM, 10, COL_NUM);
        test_nonexistent_row_query(0, 2, COL_NUM, ROW_NUM, 10, COL_NUM);
        test_nonexistent_row_query(0, 10, COL_NUM, ROW_NUM, 10, COL_NUM);
      }

      TEST_F(TestObTabletManagerGet, test_get_one_nonexistent_full_row)
      {
        test_nonexistent_full_row_query(0, 0, 0, ROW_NUM, 1);
        test_nonexistent_full_row_query(0, 1, COL_NUM, ROW_NUM);
        test_nonexistent_full_row_query(ROW_NUM / 2, 1, COL_NUM, ROW_NUM, 1);
        test_nonexistent_full_row_query(ROW_NUM - 1, 1, COL_NUM, ROW_NUM, 1);
        test_nonexistent_full_row_query(0, 2, COL_NUM, ROW_NUM, 1);
        test_nonexistent_full_row_query(0, 10, COL_NUM, ROW_NUM, 1);
      }

      TEST_F(TestObTabletManagerGet, test_get_two_nonexistent_full_rows)
      {
        test_nonexistent_full_row_query(0, 0, 0, ROW_NUM, 2);
        test_nonexistent_full_row_query(0, 1, COL_NUM, ROW_NUM, 2);
        test_nonexistent_full_row_query(ROW_NUM / 2, 1, COL_NUM, ROW_NUM, 2);
        test_nonexistent_full_row_query(ROW_NUM - 1, 1, COL_NUM, ROW_NUM, 2);
        test_nonexistent_full_row_query(0, 2, COL_NUM, ROW_NUM, 2);
        test_nonexistent_full_row_query(0, 10, COL_NUM, ROW_NUM, 2);
      }

      TEST_F(TestObTabletManagerGet, test_get_ten_nonexistent_full_row)
      {
        test_nonexistent_full_row_query(0, 0, 0, ROW_NUM, 10);
        test_nonexistent_full_row_query(0, 1, COL_NUM, ROW_NUM, 10);
        test_nonexistent_full_row_query(ROW_NUM / 2, 1, COL_NUM, ROW_NUM, 10);
        test_nonexistent_full_row_query(ROW_NUM - 1, 1, COL_NUM, ROW_NUM, 10);
        test_nonexistent_full_row_query(0, 2, COL_NUM, ROW_NUM, 10);
        test_nonexistent_full_row_query(0, 10, COL_NUM, ROW_NUM, 10);
      }

      TEST_F(TestObTabletManagerGet, test_get_one_nonexistent_column)
      {
        test_nonexistent_column_query(0, 1, 0, 1);
        test_nonexistent_column_query(0, 1, 1, 1);
        test_nonexistent_column_query(0, 1, 4, 1);
        test_nonexistent_column_query(0, 2, 0, 1);
        test_nonexistent_column_query(0, 2, 1, 1);
        test_nonexistent_column_query(0, 2, 4, 1);
        test_nonexistent_column_query(0, 10, 0, 1);
        test_nonexistent_column_query(0, 10, 1, 1);
        test_nonexistent_column_query(0, 10, 4, 1);
      }

      TEST_F(TestObTabletManagerGet, test_get_two_nonexistent_columns)
      {
        test_nonexistent_column_query(0, 1, 0, 2);
        test_nonexistent_column_query(0, 1, 1, 2);
        test_nonexistent_column_query(0, 1, 3, 2);
        test_nonexistent_column_query(0, 2, 0, 2);
        test_nonexistent_column_query(0, 2, 1, 2);
        test_nonexistent_column_query(0, 2, 3, 2);
        test_nonexistent_column_query(0, 10, 0, 2);
        test_nonexistent_column_query(0, 10, 1, 2);
        test_nonexistent_column_query(0, 10, 3, 2);
      }

      TEST_F(TestObTabletManagerGet, test_get_max_nonexistent_columns)
      {
        test_nonexistent_column_query(0, 1, 0, COL_NUM);
        test_nonexistent_column_query(0, 2, 0, COL_NUM);
        test_nonexistent_column_query(0, 10, 0, COL_NUM);
      }

      TEST_F(TestObTabletManagerGet, test_get_all_nonexistent_columns)
      {
        int32_t row_count = 20;

        for (int i = 0; i < ROW_NUM / row_count; i++)
        {
          test_nonexistent_column_query(row_count * i, row_count, 0, COL_NUM);
        }
      }

      TEST_F(TestObTabletManagerGet, test_get_all_columns)
      {
        int32_t row_count = 20;

        for (int i = 0; i < ROW_NUM / row_count; i++)
        {
          test_adjacent_row_query(row_count * i, row_count, COL_NUM);
        }
      }

      TEST_F(TestObTabletManagerGet, test_get_all_columns_with_full_row_mark)
      {
        int32_t row_count = 20;

        for (int i = 0; i < ROW_NUM / row_count; i++)
        {
          test_full_row_query(row_count * i, row_count);
        }
      }

      TEST_F(TestObTabletManagerGet, test_get_all_columns_with_nonexistent_rows)
      {
        int32_t row_count = 10;

        for (int i = 0; i < ROW_NUM / row_count; i++)
        {
          test_nonexistent_row_query(row_count * i, row_count, COL_NUM, ROW_NUM, 10, COL_NUM);
        }
      }

      TEST_F(TestObTabletManagerGet, test_get_all_columns_with_nonexistent_full_rows)
      {
        int32_t row_count = 10;

        for (int i = 0; i < ROW_NUM / row_count; i++)
        {
          test_nonexistent_full_row_query(row_count * i, row_count, COL_NUM, ROW_NUM, 10);
        }
      }

      TEST_F(TestObTabletManagerGet, DISABLED_test_switch_cache)
      {
        int ret = OB_SUCCESS;
        int32_t row_count = 10;

        ret = tablet_mgr.build_unserving_cache();
        EXPECT_EQ(OB_SUCCESS, ret);
        ret = tablet_mgr.switch_cache();
        EXPECT_EQ(OB_SUCCESS, ret);
        ret = tablet_mgr.drop_unserving_cache();
        EXPECT_EQ(OB_SUCCESS, ret);

        for (int i = 0; i < ROW_NUM / row_count; i++)
        {
          test_adjacent_row_query(row_count * i, row_count, COL_NUM);
        }

        for (int i = 0; i < ROW_NUM / row_count; i++)
        {
          test_full_row_query(row_count * i, row_count);
        }

        for (int i = 0; i < ROW_NUM / row_count; i++)
        {
          test_nonexistent_row_query(row_count * i, row_count, COL_NUM, ROW_NUM, 10, COL_NUM);
        }

        for (int i = 0; i < ROW_NUM / row_count; i++)
        {
          test_nonexistent_full_row_query(row_count * i, row_count, COL_NUM, ROW_NUM, 10);
        }
      }
    }//end namespace common
  }//end namespace tests
}//end namespace oceanbase

int main(int argc, char** argv)
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
