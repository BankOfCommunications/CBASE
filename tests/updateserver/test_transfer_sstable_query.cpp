/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * test_transfer_sstable_query.cpp for test get and scan
 * interface of transfer sstable query
 *
 * Authors:
 *   huating <huating.zmq@taobao.com>
 *
 */
#include <tblog.h>
#include <gtest/gtest.h>
#include "common/ob_action_flag.h"
#include "common/ob_scan_param.h"
#include "common/ob_common_stat.h"
#include "sstable/ob_sstable_reader.h"
#include "sstable/ob_sstable_writer.h"
#include "sstable/ob_sstable_getter.h"
#include "sstable/ob_sstable_scanner.h"
#include "updateserver/ob_transfer_sstable_query.h"
#include "sstable/ob_disk_path.h"
#include "chunkserver/ob_fileinfo_cache.h"
#include "../common/test_rowkey_helper.h"

using namespace std;
using namespace oceanbase::common;
using namespace oceanbase::sstable;
using namespace oceanbase::chunkserver;
using namespace oceanbase::updateserver;

static CharArena allocator_;
namespace oceanbase
{
  namespace tests
  {
    namespace updateserver
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
      static const int64_t DEL_ROW_INTERVAL = 10;

      static char sstable_file_path[OB_MAX_FILE_NAME_LENGTH];
      static ObCellInfo** cell_infos;
      static char* row_key_strs[ROW_NUM + NON_EXISTENT_ROW_NUM][COL_NUM];
      static ModulePageAllocator mod(0);
      static ModuleArena arena(ModuleArena::DEFAULT_PAGE_SIZE, mod);
      static ObSSTableReader* readers[SSTABLE_NUM];
      static ObGetParam get_param;
      static ObScanParam scan_param;
      FileInfoCache fic;
      static ObTransferSSTableQuery trans_sst_query(fic);

      class TestObTransferSSTableQuery : public ::testing::Test
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
          if (ObExtendType == type)
          {
            real.get_ext(real_val);
            EXPECT_EQ(exp_val, real_val);
          }
        }

        void check_cell(const ObCellInfo& expected, const ObCellInfo& real,
                        int type=ObIntType, uint64_t column_id = UINT64_MAX,
                        bool nop = false)
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
          if (ObExtendType == type)
          {
            if (!nop)
            {
              expected.value_.get_ext(exp_val);
            }
            else
            {
              exp_val = ObActionFlag::OP_NOP;
            }
          }
          check_obj(real.value_, type, exp_val);
        }

        void get_one_row(const int32_t row_index = 0,
                         const int32_t column_count = COL_NUM,
                         const int32_t nonexistent_column_count = 0,
                         const bool full_row = false)
        {
          int ret = OB_SUCCESS;
          ObSSTableReader* reader = NULL;
          ObSSTableReader* reader2 = NULL;
          ObSSTableGetter getter;
          ObSSTableGetter getter2;
          ObIterator* iterator = &getter;
          ObIterator* iterator2 = &getter2;
          ObCellInfo* ci;
          ObCellInfo expected;
          ObCellInfo param_cell;
          int64_t index_i = 0;
          int64_t index_j = 0;
          int64_t exp_cell_cnt = 0;

          get_param.reset();
          if (full_row)
          {
            param_cell = cell_infos[row_index][0];
            param_cell.column_id_ = 0;
            ret = get_param.add_cell(param_cell);
            EXPECT_EQ(OB_SUCCESS, ret);
          }
          else
          {
            for (int j = 0; j < column_count; j++)
            {
              param_cell = cell_infos[row_index][j];
              param_cell.column_id_ = j + 2;
              ret = get_param.add_cell(param_cell);
              EXPECT_EQ(OB_SUCCESS, ret);
            }
            for (int j = column_count; j < column_count + nonexistent_column_count; j++)
            {
              param_cell = cell_infos[row_index][j];
              param_cell.column_id_ = COL_NUM + 10;
              ret = get_param.add_cell(param_cell);
              EXPECT_EQ(OB_SUCCESS, ret);
            }
          }

          exp_cell_cnt = column_count + nonexistent_column_count;
          if (row_index % DEL_ROW_INTERVAL == 0)
          {
            exp_cell_cnt += 1;
          }
          exp_cell_cnt = full_row ? exp_cell_cnt + 1 : exp_cell_cnt;

          reader = readers[row_index / SSTABLE_ROW_NUM];
          reader2 = readers[0];
          ASSERT_TRUE(NULL != reader);
          ret = trans_sst_query.get(get_param, *reader, iterator);
          ASSERT_EQ(OB_SUCCESS, ret);
          ret = trans_sst_query.get(get_param, *reader2, iterator2);
          ASSERT_EQ(OB_SUCCESS, ret);

          // check result
          int64_t count = 0;
          int64_t del_row = 0;
          bool is_del_row = false;
          int64_t col_num = full_row ? column_count + nonexistent_column_count + 1 
            : column_count + nonexistent_column_count;
          while (OB_SUCCESS == (ret = iterator->next_cell())
                 && OB_SUCCESS == (ret = iterator2->next_cell()))
          {
            index_i = row_index + (count - del_row) / col_num;
            index_j = (count - del_row) % col_num;
            expected.table_name_ = table_name;
            EXPECT_EQ(OB_SUCCESS, iterator->get_cell(&ci));
            ASSERT_TRUE(NULL != ci);
            is_del_row = (index_i % DEL_ROW_INTERVAL == 0);
            if (is_del_row && index_j == 0 && del_row == 0)
            {
              expected = cell_infos[index_i][index_j];
              check_cell(expected, *ci, ObExtendType);
            }
            else if (full_row && ((!is_del_row && index_j == 0) 
                || (is_del_row && ((del_row == 0 && index_j == 1) 
                    || (del_row > 0 && index_j == 0)))))
            {
              // rowkey column 
              EXPECT_EQ((uint64_t)1, ci->column_id_);
            }
            else
            {
              index_j = full_row ? index_j - 1 : index_j;
              expected = cell_infos[index_i][index_j];
              if (is_del_row)
              {
                if (index_j < COL_NUM - 1)
                {
                  expected = cell_infos[index_i][index_j + 1];
                }
                else
                {
                  expected.column_id_ = index_j + 2;
                }
                expected.table_name_ = table_name;
              }
              if (column_count == COL_NUM && index_j == COL_NUM - 1)
              {
                check_cell(expected, *ci, ObExtendType, UINT64_MAX, true);
              }
              else if (index_j >= column_count)
              {
                check_cell(expected, *ci, ObExtendType, COL_NUM + 10, true);
              }
              else
              {
                check_cell(expected, *ci);
              }
              index_j = full_row ? index_j + 1 : index_j;
            }
            EXPECT_EQ(OB_SUCCESS, iterator2->get_cell(&ci));
            ASSERT_TRUE(NULL != ci);
            if (full_row && ((!is_del_row && index_j == 0) 
                || (is_del_row && ((del_row == 0 && index_j == 1) 
                    || (del_row > 0 && index_j == 0)))))
            {
              // rowkey column 
              EXPECT_EQ((uint64_t)1, ci->column_id_);
            }
            else if (is_del_row && index_j == 0 && del_row == 0)
            {
              del_row++;
              check_cell(expected, *ci, ObExtendType);
            }
            else
            {
              index_j = full_row ? index_j - 1 : index_j;
              if (is_del_row)
              {
                if (index_j < COL_NUM - 1)
                {
                  expected = cell_infos[index_i][index_j + 1];
                }
                else
                {
                  expected.column_id_ = index_j + 2;
                }
                expected.table_name_ = table_name;
              }
              if (column_count == COL_NUM && index_j == COL_NUM - 1)
              {
                check_cell(expected, *ci, ObExtendType, UINT64_MAX, true);
              }
              else if (index_j >= column_count)
              {
                check_cell(expected, *ci, ObExtendType, COL_NUM + 10, true);
              }
              else
              {
                check_cell(expected, *ci);
              }
            }
            ++count;
            if (count == exp_cell_cnt)
            {
              bool is_row_finished = false;
              EXPECT_EQ(OB_SUCCESS, iterator->is_row_finished(&is_row_finished));
              EXPECT_TRUE(is_row_finished);
              EXPECT_EQ(OB_SUCCESS, iterator2->is_row_finished(&is_row_finished));
              EXPECT_TRUE(is_row_finished);
            }
            else
            {
              bool is_row_finished = false;
              EXPECT_EQ(OB_SUCCESS, iterator->is_row_finished(&is_row_finished));
              EXPECT_FALSE(is_row_finished);
              EXPECT_EQ(OB_SUCCESS, iterator2->is_row_finished(&is_row_finished));
              EXPECT_FALSE(is_row_finished);
            }
          }
          EXPECT_EQ(OB_ITER_END, ret);
          EXPECT_EQ(exp_cell_cnt, count);
          EXPECT_EQ(OB_ITER_END, iterator->next_cell());
          EXPECT_EQ(OB_ITER_END, iterator2->next_cell());
          bool is_row_finished = false;
          EXPECT_EQ(OB_SUCCESS, iterator->is_row_finished(&is_row_finished));
          EXPECT_TRUE(is_row_finished);
          EXPECT_EQ(OB_SUCCESS, iterator2->is_row_finished(&is_row_finished));
          EXPECT_TRUE(is_row_finished);
        }

        void test_adjacent_row_query(const int32_t row_index = 0,
                                     const int32_t row_count = 1,
                                     const int32_t column_count = COL_NUM)
        {
          for (int i = row_index; i < row_index + row_count; i++)
          {
            get_one_row(i, column_count);
          }
        }

        void test_full_row_query(const int32_t row_index = 0,
                                 const int32_t row_count = 1)
        {
          for (int i = row_index; i < row_index + row_count; i++)
          {
            get_one_row(i, COL_NUM, 0, true);
          }
        }

        void test_nonexistent_column_query(const int32_t row_index = 0,
                                           const int32_t row_count = 1,
                                           const int32_t column_count = 0,
                                           const int32_t nonexistent_column_count = COL_NUM)
        {
          for (int i = row_index; i < row_index + row_count; i++)
          {
            get_one_row(i, column_count, nonexistent_column_count);
          }
        }

        void scan_one_sstable(const int32_t row_index = 0,
                              const int32_t row_count = 1,
                              const int32_t column_count = COL_NUM,
                              const int32_t nonexistent_column_count = 0)
        {
          int ret = OB_SUCCESS;
          ObSSTableReader* reader = NULL;
          ObSSTableScanner scanner;
          ObIterator* iterator = &scanner;
          ObCellInfo* ci;
          ObCellInfo expected;
          ObVersionRange ver_range;
          ObNewRange range;
          int64_t index_i = 0;
          int64_t index_j = 0;
          int64_t exp_cell_cnt = 0;
          int64_t row_column_cnt = 0;

          scan_param.reset();

          ver_range.start_version_ = 0;
          ver_range.end_version_ = INT64_MAX;
          ver_range.border_flag_.set_inclusive_start();
          ver_range.border_flag_.set_inclusive_end();
          scan_param.set_version_range(ver_range);

          scan_param.set_scan_size(OB_MAX_PACKET_LENGTH);
          scan_param.set_is_result_cached(true);

          range.table_id_ = table_id;
          range.start_key_ = cell_infos[row_index][0].row_key_;
          range.border_flag_.set_inclusive_start();
          range.end_key_ = cell_infos[row_index + row_count - 1][0].row_key_;
          range.border_flag_.set_inclusive_end();
          scan_param.set(table_id, table_name, range);

          for (int j = 0; j < column_count; j++)
          {
            ret = scan_param.add_column(j + 2);
            EXPECT_EQ(OB_SUCCESS, ret);
          }
          for (int j = column_count; j < column_count + nonexistent_column_count; j++)
          {
            ret = scan_param.add_column(COL_NUM + 10);
            EXPECT_EQ(OB_SUCCESS, ret);
          }
          row_column_cnt = column_count + nonexistent_column_count;
          exp_cell_cnt = row_column_cnt * row_count
            + (row_count / DEL_ROW_INTERVAL +
               ((row_count < DEL_ROW_INTERVAL && row_index / DEL_ROW_INTERVAL == 0) ? 1 : 0));

          reader = readers[row_index / SSTABLE_ROW_NUM];
          ASSERT_TRUE(NULL != reader);
          ret = trans_sst_query.scan(scan_param, *reader, iterator);
          ASSERT_EQ(OB_SUCCESS, ret);
          ASSERT_TRUE(NULL != iterator);

          // check result
          int64_t count = 0;
          int64_t del_row = 0;
          bool is_del_row = false;
          bool handled_del_row = false;
          int64_t next_index_i = 0;
          while (OB_SUCCESS == (ret = iterator->next_cell()))
          {
            index_i = row_index + (count - del_row) / row_column_cnt;
            index_j = (count - del_row) % row_column_cnt;
            expected = cell_infos[index_i][index_j];
            expected.table_name_ = table_name;
            EXPECT_EQ(OB_SUCCESS, iterator->get_cell(&ci));
            ASSERT_TRUE(NULL != ci);
            is_del_row = (index_i % DEL_ROW_INTERVAL == 0);
            if (is_del_row && index_j == 0 && !handled_del_row)
            {
              del_row++;
              handled_del_row = true;
              check_cell(expected, *ci, ObExtendType);
            }
            else
            {
              handled_del_row = false;
              if (is_del_row)
              {
                if (index_j < COL_NUM - 1)
                {
                  expected = cell_infos[index_i][index_j + 1];
                }
                else
                {
                  expected.column_id_ = index_j + 2;
                }
                expected.table_name_ = table_name;
              }
              if (column_count == COL_NUM && index_j == COL_NUM - 1)
              {
                check_cell(expected, *ci, ObExtendType, UINT64_MAX, true);
              }
              else if (index_j >= column_count)
              {
                check_cell(expected, *ci, ObExtendType, COL_NUM + 10, true);
              }
              else
              {
                check_cell(expected, *ci);
              }
            }
            ++count;
            next_index_i = row_index + (count - del_row) / row_column_cnt;
            if (count == exp_cell_cnt || next_index_i != index_i)
            {
              bool is_row_finished = false;
              EXPECT_EQ(OB_SUCCESS, iterator->is_row_finished(&is_row_finished));
              EXPECT_TRUE(is_row_finished);
            }
            else
            {
              bool is_row_finished = false;
              EXPECT_EQ(OB_SUCCESS, iterator->is_row_finished(&is_row_finished));
              EXPECT_FALSE(is_row_finished);
            }
          }

          EXPECT_EQ(OB_ITER_END, ret);
          EXPECT_EQ(exp_cell_cnt, count);
          EXPECT_EQ(OB_ITER_END, iterator->next_cell());
          bool is_row_finished = false;
          EXPECT_EQ(OB_SUCCESS, iterator->is_row_finished(&is_row_finished));
          EXPECT_TRUE(is_row_finished);
        }

        void test_adjacent_row_scan(const int32_t row_index = 0,
                                    const int32_t row_count = 1,
                                    const int32_t column_count = COL_NUM)
        {
          int32_t cur_row_count = 0;
          int32_t sstable_idx   = 0;

          for (int32_t i = row_index; i < row_index + row_count; i += cur_row_count)
          {
            sstable_idx = i / SSTABLE_ROW_NUM;
            if ((sstable_idx + 1) * SSTABLE_ROW_NUM < row_index + row_count)
            {
              cur_row_count = static_cast<int32_t>((sstable_idx + 1) * SSTABLE_ROW_NUM - i);
            }
            else
            {
              cur_row_count = row_index + row_count - i;
            }
            scan_one_sstable(i, cur_row_count, column_count);
          }
        }

        void test_nonexistent_column_scan(const int32_t row_index = 0,
                                          const int32_t row_count = 1,
                                          const int32_t column_count = 0,
                                          const int32_t nonexistent_column_count = COL_NUM)
        {
          int32_t cur_row_count = 0;
          int32_t sstable_idx   = 0;

          for (int32_t i = row_index; i < row_index + row_count; i += cur_row_count)
          {
            sstable_idx = i / SSTABLE_ROW_NUM;
            if ((sstable_idx + 1) * SSTABLE_ROW_NUM < row_index + row_count)
            {
              cur_row_count = static_cast<int32_t>((sstable_idx + 1) * SSTABLE_ROW_NUM - i);
            }
            else
            {
              cur_row_count = row_index + row_count - i;
            }
            scan_one_sstable(i, cur_row_count, column_count, nonexistent_column_count);
          }
        }

        void test_get_sstable_end_key()
        {
          int ret = OB_SUCCESS;
          ObRowkey rowkey;

          for (int64_t i = 0; i < SSTABLE_NUM; ++i)
          {
            ret = trans_sst_query.get_sstable_end_key(*readers[i],
              table_id, rowkey);
            ASSERT_EQ(OB_SUCCESS, ret);
            EXPECT_EQ(cell_infos[ROW_NUM - 1][0].row_key_, rowkey);
          }
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
              if (i % DEL_ROW_INTERVAL == 0)
              {
                if (j == 0)
                {
                  //1/10 of the rows own delete row op
                  cell_infos[i][j].column_id_ = 0;
                  cell_infos[i][j].value_.set_ext(ObActionFlag::OP_NOP);
                }
                else
                {
                  cell_infos[i][j].column_id_ = j - 1 + 2;
                  cell_infos[i][j].value_.set_int(1000 + i * COL_NUM + j - 1);
                }
              }
              else
              {
                cell_infos[i][j].column_id_ = j + 2;
                cell_infos[i][j].value_.set_int(1000 + i * COL_NUM + j);
              }
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
            column_def.column_group_id_= 0;
            column_def.column_name_id_ = static_cast<uint16_t>(i + 2);
            column_def.column_value_type_ = ObIntType;
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
          err = writer.create_sstable(sstable_schema, path, compress_name, 0,
                                      OB_SSTABLE_STORE_SPARSE);
          EXPECT_EQ(OB_SUCCESS, err);

          for (int64_t i = 0; i < row_num; ++i)
          {
            ObSSTableRow row;
            row.set_table_id(table_id);
            row.set_column_group_id(0);
            err = row.set_rowkey(cell_infos[i][0].row_key_);
            EXPECT_EQ(OB_SUCCESS, err);

            /**
             * we make the last column is non-existent for sparse format
             */
            int64_t colmun_cnt = (i % DEL_ROW_INTERVAL == 0) ? col_num : col_num - 1;
            for (int64_t j = 0; j < colmun_cnt; ++j)
            {
              err = row.shallow_add_obj(cell_infos[i][j].value_, cell_infos[i][j].column_id_);
              EXPECT_EQ(OB_SUCCESS, err);
            }

            int64_t space_usage = 0;
            err = writer.append_row(row, space_usage);
            EXPECT_EQ(OB_SUCCESS, err);
          }

          int64_t offset = 0;
          int64_t sstable_size = 0;
          err = writer.close_sstable(offset, sstable_size);
          EXPECT_EQ(OB_SUCCESS, err);

          return err;
        }

        static int create_and_load_sstables()
        {
          int ret = OB_SUCCESS;
          ObSSTableId sst_ids[SSTABLE_NUM];

          for (int64_t i = 0; i < SSTABLE_NUM; ++i)
          {
            sst_ids[i].sstable_file_id_ = i % DISK_NUM + 256 * (i / DISK_NUM + 1) + 1;
            sst_ids[i].sstable_file_offset_ = 0;

            ret = init_sstable((const ObCellInfo**)(&cell_infos[0]),
                               ROW_NUM, COL_NUM, sst_ids[i].sstable_file_id_);
            EXPECT_EQ(OB_SUCCESS, ret);
          }

          // init range
          for (int64_t i = 0; i < SSTABLE_NUM; ++i)
          {
            //init sstable readers
            ObSSTableReader* reader = new ObSSTableReader(arena, fic);
            ret = reader->open(sst_ids[i], 1);
            if (OB_SUCCESS == ret)
            {
              readers[i] = reader;
            }
            else
            {
              TBSYS_LOG(ERROR, "sstable reader open sstable failed, sstable_id=%lu, "
                                "index = %ld", sst_ids[i].sstable_file_id_, i);
              break;
            }
          }

          return ret;
        }

        static int init_tans_sst_query()
        {
          int err = OB_SUCCESS;

          err = fic.init(1024);
          EXPECT_EQ(OB_SUCCESS, err);

          err = trans_sst_query.init(1024<<20, 128<<20, 128 * 1024 * 1024);
          EXPECT_EQ(OB_SUCCESS, err);

          err = create_and_load_sstables();
          EXPECT_EQ(OB_SUCCESS, err);

          return err;
        }

      public:
        static void SetUpTestCase()
        {
          int err = OB_SUCCESS;

          TBSYS_LOGGER.setLogLevel("WARN");
          err = ob_init_memory_pool();
          ASSERT_EQ(OB_SUCCESS, err);

          init_cell_infos();

          err = init_tans_sst_query();
          ASSERT_EQ(OB_SUCCESS, err);
        }

        static void TearDownTestCase()
        {
          destroy_cell_infos();
          for (int64_t i = 0; i < SSTABLE_NUM; ++i)
          {
            if (NULL != readers[i])
            {
              delete readers[i];
            }
          }
        }

        virtual void SetUp()
        {

        }

        virtual void TearDown()
        {

        }
      };

      TEST_F(TestObTransferSSTableQuery, test_get_first_row)
      {
        test_adjacent_row_query(0, 1, 1);
        test_adjacent_row_query(0, 1, 2);
        test_adjacent_row_query(0, 1, 3);
        test_adjacent_row_query(0, 1, 4);
        test_adjacent_row_query(0, 1, 5);
      }

      TEST_F(TestObTransferSSTableQuery, test_get_middle_row)
      {
        test_adjacent_row_query((ROW_NUM - 1) / 2, 1, 1);
        test_adjacent_row_query((ROW_NUM - 1) / 2, 1, 2);
        test_adjacent_row_query((ROW_NUM - 1) / 2, 1, 3);
        test_adjacent_row_query((ROW_NUM - 1) / 2, 1, 4);
        test_adjacent_row_query((ROW_NUM - 1) / 2, 1, 5);
      }

      TEST_F(TestObTransferSSTableQuery, test_get_last_row)
      {
        test_adjacent_row_query(ROW_NUM - 1, 1, 1);
        test_adjacent_row_query(ROW_NUM - 1, 1, 2);
        test_adjacent_row_query(ROW_NUM - 1, 1, 3);
        test_adjacent_row_query(ROW_NUM - 1, 1, 4);
        test_adjacent_row_query(ROW_NUM - 1, 1, 5);
      }

      TEST_F(TestObTransferSSTableQuery, test_get_x_columns_two_rows)
      {
        test_adjacent_row_query(0, 2, 1);
        test_adjacent_row_query(0, 2, 2);
        test_adjacent_row_query(0, 2, 3);
        test_adjacent_row_query(0, 2, 4);
        test_adjacent_row_query(0, 2, 5);
      }

      TEST_F(TestObTransferSSTableQuery, test_get_x_columns_ten_rows)
      {
        test_adjacent_row_query(0, 10, 1);
        test_adjacent_row_query(0, 10, 2);
        test_adjacent_row_query(0, 10, 3);
        test_adjacent_row_query(0, 10, 4);
        test_adjacent_row_query(0, 10, 5);
      }

      TEST_F(TestObTransferSSTableQuery, test_get_x_columns_hundred_rows)
      {
        test_adjacent_row_query(0, 100, 1);
      }

      TEST_F(TestObTransferSSTableQuery, test_get_max_columns)
      {
        test_adjacent_row_query(0, OB_MAX_GET_COLUMN_NUMBER, 1);
      }

      TEST_F(TestObTransferSSTableQuery, test_get_one_row)
      {
        test_full_row_query(0, 1);
        test_full_row_query(ROW_NUM / 2, 1);
        test_full_row_query(ROW_NUM - 1, 1);
      }

      TEST_F(TestObTransferSSTableQuery, test_get_two_rows)
      {
        test_full_row_query(0, 2);
        test_full_row_query(ROW_NUM / 2, 2);
        test_full_row_query(ROW_NUM - 2, 2);
      }

      TEST_F(TestObTransferSSTableQuery, test_get_ten_rows)
      {
        test_full_row_query(0, 10);
        test_full_row_query(ROW_NUM / 2, 10);
        test_full_row_query(ROW_NUM - 10, 10);
      }

      TEST_F(TestObTransferSSTableQuery, test_get_one_nonexistent_column)
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

      TEST_F(TestObTransferSSTableQuery, test_get_two_nonexistent_columns)
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

      TEST_F(TestObTransferSSTableQuery, test_get_max_nonexistent_columns)
      {
        test_nonexistent_column_query(0, 1, 0, COL_NUM);
        test_nonexistent_column_query(0, 2, 0, COL_NUM);
        test_nonexistent_column_query(0, 10, 0, COL_NUM);
      }

      TEST_F(TestObTransferSSTableQuery, test_get_all_nonexistent_columns)
      {
        int32_t row_count = 20;

        for (int32_t i = 0; i < ROW_NUM / row_count; i++)
        {
          test_nonexistent_column_query(row_count * i, row_count, 0, COL_NUM);
        }
      }

      TEST_F(TestObTransferSSTableQuery, test_get_all_columns)
      {
        int32_t row_count = 20;

        for (int32_t i = 0; i < ROW_NUM / row_count; i++)
        {
          test_adjacent_row_query(row_count * i, row_count, COL_NUM);
        }
      }

      TEST_F(TestObTransferSSTableQuery, test_get_all_columns_with_full_row_mark)
      {
        int32_t row_count = 20;

        for (int32_t i = 0; i < ROW_NUM / row_count; i++)
        {
          test_full_row_query(row_count * i, row_count);
        }
      }

      //test scan infterface
      TEST_F(TestObTransferSSTableQuery, test_scan_first_row)
      {
        test_adjacent_row_scan(0, 1, 1);
        test_adjacent_row_scan(0, 1, 2);
        test_adjacent_row_scan(0, 1, 3);
        test_adjacent_row_scan(0, 1, 4);
        test_adjacent_row_scan(0, 1, 5);
      }

      TEST_F(TestObTransferSSTableQuery, test_scan_middle_row)
      {
        test_adjacent_row_scan((ROW_NUM - 1) / 2, 1, 1);
        test_adjacent_row_scan((ROW_NUM - 1) / 2, 1, 2);
        test_adjacent_row_scan((ROW_NUM - 1) / 2, 1, 3);
        test_adjacent_row_scan((ROW_NUM - 1) / 2, 1, 4);
        test_adjacent_row_scan((ROW_NUM - 1) / 2, 1, 5);
      }

      TEST_F(TestObTransferSSTableQuery, test_scan_last_row)
      {
        test_adjacent_row_scan(ROW_NUM - 1, 1, 1);
        test_adjacent_row_scan(ROW_NUM - 1, 1, 2);
        test_adjacent_row_scan(ROW_NUM - 1, 1, 3);
        test_adjacent_row_scan(ROW_NUM - 1, 1, 4);
        test_adjacent_row_scan(ROW_NUM - 1, 1, 5);
      }

      TEST_F(TestObTransferSSTableQuery, test_scan_x_columns_two_rows)
      {
        test_adjacent_row_scan(0, 2, 1);
        test_adjacent_row_scan(0, 2, 2);
        test_adjacent_row_scan(0, 2, 3);
        test_adjacent_row_scan(0, 2, 4);
        test_adjacent_row_scan(0, 2, 5);
      }

      TEST_F(TestObTransferSSTableQuery, test_scan_x_columns_ten_rows)
      {
        test_adjacent_row_scan(0, 10, 1);
        test_adjacent_row_scan(0, 10, 2);
        test_adjacent_row_scan(0, 10, 3);
        test_adjacent_row_scan(0, 10, 4);
        test_adjacent_row_scan(0, 10, 5);
      }

      TEST_F(TestObTransferSSTableQuery, test_scan_x_columns_hundred_rows)
      {
        test_adjacent_row_scan(0, 100, 1);
      }

      TEST_F(TestObTransferSSTableQuery, test_scan_one_nonexistent_column)
      {
        test_nonexistent_column_scan(0, 1, 0, 1);
        test_nonexistent_column_scan(0, 1, 1, 1);
        test_nonexistent_column_scan(0, 1, 4, 1);
        test_nonexistent_column_scan(0, 2, 0, 1);
        test_nonexistent_column_scan(0, 2, 1, 1);
        test_nonexistent_column_scan(0, 2, 4, 1);
        test_nonexistent_column_scan(0, 10, 0, 1);
        test_nonexistent_column_scan(0, 10, 1, 1);
        test_nonexistent_column_scan(0, 10, 4, 1);
      }

      TEST_F(TestObTransferSSTableQuery, test_scan_two_nonexistent_columns)
      {
        test_nonexistent_column_scan(0, 1, 0, 2);
        test_nonexistent_column_scan(0, 1, 1, 2);
        test_nonexistent_column_scan(0, 1, 3, 2);
        test_nonexistent_column_scan(0, 2, 0, 2);
        test_nonexistent_column_scan(0, 2, 1, 2);
        test_nonexistent_column_scan(0, 2, 3, 2);
        test_nonexistent_column_scan(0, 10, 0, 2);
        test_nonexistent_column_scan(0, 10, 1, 2);
        test_nonexistent_column_scan(0, 10, 3, 2);
      }

      TEST_F(TestObTransferSSTableQuery, test_scan_max_nonexistent_columns)
      {
        test_nonexistent_column_scan(0, 1, 0, COL_NUM);
        test_nonexistent_column_scan(0, 2, 0, COL_NUM);
        test_nonexistent_column_scan(0, 10, 0, COL_NUM);
      }

      TEST_F(TestObTransferSSTableQuery, test_scan_all_nonexistent_columns)
      {
        test_nonexistent_column_scan(0, ROW_NUM, 0, COL_NUM);
      }

      TEST_F(TestObTransferSSTableQuery, test_scan_all_columns)
      {
        test_adjacent_row_query(0, ROW_NUM, COL_NUM);
      }

      TEST_F(TestObTransferSSTableQuery, test_get_end_key)
      {
        test_get_sstable_end_key();
      }

    }//end namespace common
  }//end namespace tests
}//end namespace oceanbase

int main(int argc, char** argv)
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
