/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *  
 * test_sstable_scanner.cpp for what ... 
 *
 * Authors:
 *   chuanhui <rizhao.ych@taobao.com>
 *   qushan<qushan@taobao.com>
 *
 */
#include <iostream>
#include <sstream>
#include <algorithm>
#include <tblog.h>
#include <gtest/gtest.h>
#include "common/utility.h"
#include "common/page_arena.h"
#include "common/ob_common_param.h"
#include "chunkserver/ob_fileinfo_cache.h"
#include "test_helper.h"
#include "sstable/ob_sstable_reader.h"
#include "sstable/ob_sstable_scanner.h"

using namespace std;
using namespace oceanbase::common;
using namespace oceanbase::chunkserver;
using namespace oceanbase::sstable;

namespace oceanbase
{
  namespace tests
  {
    namespace sstable
    {

      static const ObString table_name;
      static const int64_t sstable_file_id = 1234;
      static const int64_t sstable_file_offset = 0;
      static const ObSSTableId sstable_id(sstable_file_id);
      static ModulePageAllocator mod(0);
      static ModuleArena allocator(ModuleArena::DEFAULT_PAGE_SIZE, mod);
      static ObSSTableReader sstable_reader_(allocator, GFactory::get_instance().get_fi_cache());
      static ObRowkeyInfo rowkey_info_;

      class TestObSSTableScanner : public ::testing::Test
      {
        public:
          static const int32_t ROW_NUM = 10000;
          static const int32_t COL_NUM = 12;
          static const int32_t COLUMN_GROUP_NUM = 3;
          static const int32_t ROWKEY_COL_NUM = 3;
          static const int32_t GROUP1_COL_NUM = 4;
          static const int32_t GROUP2_COL_NUM = COL_NUM - ROWKEY_COL_NUM - GROUP1_COL_NUM;
          static const int32_t NONKEY_COL_NUM = COL_NUM - ROWKEY_COL_NUM;
          static const int64_t block_internal_bufsiz = BUFSIZ;
        public:
          TestObSSTableScanner()
            : scanner_ ()
        {
        }
        protected:
          void create_query_range(ObNewRange& range, 
              const int64_t start_row, const int64_t end_row,
              const ObBorderFlag border_flag)
          {
            ::create_new_range(range, COL_NUM, ROWKEY_COL_NUM, start_row, end_row, border_flag);
          }

          void create_scan_param(
              ObScanParam& scan_param,
              const int64_t start_row, const int64_t end_row, 
              const ObBorderFlag border_flag, const ScanFlag::Direction dir, 
              const ScanFlag::SyncMode read_mode,
              const int32_t query_columns[], const int32_t query_column_count)
          {
            ObNewRange range;
            create_query_range(range, start_row, end_row, border_flag);

            scan_param.set(CellInfoGen::table_id, table_name, range);
            scan_param.set_scan_direction(dir);
            scan_param.set_read_mode(read_mode);

            for (int32_t i = 0; i < query_column_count; ++i)
            {
              scan_param.add_column(query_columns[i]);
            }

          }

          int check_binary_rowkey(const ObString& binary_rowkey, const int64_t row, const int64_t col_num)
          {
            ObObj obj_array[ROWKEY_COL_NUM];
            int ret = ObRowkeyHelper::prepare_obj_array(rowkey_info_, obj_array, ROWKEY_COL_NUM);
            if (ret) return ret;
            ret = ObRowkeyHelper::get_obj_array(binary_rowkey, obj_array, ROWKEY_COL_NUM);
            if (ret) return ret;
            ObRowkey rowkey(obj_array, ROWKEY_COL_NUM);
            ret = check_rowkey(rowkey, row, col_num);
            return ret;
          }

          void generate_query_columns(
              const int32_t query_start_column,
              int32_t query_columns[],
              int32_t query_column_count)
          {
            for (int32_t i = 0; i < query_column_count; ++i)
            {
              query_columns[i] = i + query_start_column;
            }
          }


          void query_helper(
              const int32_t start_row, 
              const int32_t end_row, 
              const ObBorderFlag &border_flag,
              const int32_t query_start_column, 
              const int32_t query_column_count,
              const ScanFlag::Direction dir,
              const ScanFlag::SyncMode mode
              )
          {
            int32_t query_columns[query_column_count] ;
            generate_query_columns(query_start_column, query_columns, query_column_count);
            query_helper(start_row, end_row, border_flag, query_columns, query_column_count, dir, mode);
          }

          void query_helper(
              const int32_t start_row, 
              const int32_t end_row, 
              const ObBorderFlag &border_flag,
              const int32_t *query_columns, 
              const int32_t query_column_count,
              const ScanFlag::Direction dir,
              const ScanFlag::SyncMode mode
              )
          {

            ObScanParam sstable_scan_param;
            create_scan_param(sstable_scan_param, 
                start_row, end_row, border_flag, dir, mode, query_columns, query_column_count);

            ObCellInfo *cell_info = 0;
            int32_t row_count = 0; 
            if (border_flag.is_min_value() && border_flag.is_max_value())
            {
              row_count = ROW_NUM;
            }
            else if (border_flag.is_min_value())
            {
              row_count = end_row ;
              if (border_flag.inclusive_end()) row_count++;
            }
            else if (border_flag.is_max_value())
            {
              row_count = ROW_NUM - start_row - 1;
              if (border_flag.inclusive_start()) row_count++;
            }
            else
            {
              row_count = end_row - start_row - 1;
              if (border_flag.inclusive_start()) row_count++;
              if (border_flag.inclusive_end()) row_count++;
            }

            if (row_count > ROW_NUM) row_count = ROW_NUM;

            int32_t total_count = 0;
            int32_t row_index = 0;
            int32_t col_index = 0;


            int32_t expect_start_row = start_row;
            int32_t expect_end_row = end_row;
            int32_t expect_row_index = 0;
            if (border_flag.is_min_value())
            {
              expect_start_row = 0;
            }
            if (border_flag.is_max_value())
            {
              expect_end_row = ROW_NUM - 1;
            }

            bool is_reverse_scan = (dir == ScanFlag::BACKWARD);
            expect_row_index = is_reverse_scan ? expect_end_row : expect_start_row;
            if (!is_reverse_scan && !border_flag.is_min_value() && !border_flag.inclusive_start()) expect_row_index++;
            if (is_reverse_scan && !border_flag.is_max_value() && !border_flag.inclusive_end()) expect_row_index--;

            int ret = scanner_.set_scan_param(sstable_scan_param, &sstable_reader_, 
                GFactory::get_instance().get_block_cache(), GFactory::get_instance().get_block_index_cache(), false);

            if (row_count <= 0 || expect_row_index < 0 || expect_row_index >= ROW_NUM)
            {
              EXPECT_EQ(OB_ITER_END, ret)
                << "start row:" << start_row << ",end row:" << end_row;
            }
            else if (row_count > 0)
            {
              EXPECT_EQ(0, ret)
                << "start row:" << start_row << ",end row:" << end_row;
            }


            while (scanner_.next_cell() == 0)
            {
              bool is_row_changed = false;
              scanner_.get_cell(&cell_info, &is_row_changed);
              if (is_row_changed) 
              {
                EXPECT_EQ(OB_SUCCESS, check_rowkey(cell_info->row_key_, expect_row_index, COL_NUM)) 
                  << "start row:" << start_row << ",end row:" << end_row
                  << ",row_index:" << row_index << ",col_index:" << col_index;
                if (col_index != 0) EXPECT_EQ(col_index, query_column_count);
                col_index = 0;
                ++row_index;
                if (is_reverse_scan) --expect_row_index;
                else ++expect_row_index;
              }

              if ((int64_t)cell_info->column_id_ <= CellInfoGen::START_ID
                  || (int64_t)cell_info->column_id_ >= COL_NUM + CellInfoGen::START_ID)
              {
                EXPECT_EQ(ObNullType, cell_info->value_.get_type());
              }
              else
              {
                EXPECT_EQ(ObIntType, cell_info->value_.get_type());
                int64_t val = 0;
                ASSERT_EQ(0, cell_info->value_.get_int(val));
                int32_t r = is_reverse_scan ? (expect_row_index + 1) : (expect_row_index - 1);
                ASSERT_EQ(val, r * COL_NUM +  ((int64_t)cell_info->column_id_ - CellInfoGen::START_ID));
              }
              ++total_count;
              ++col_index;
            }

            EXPECT_EQ(row_index, row_count);
          }

          void random_query_helper_mix_mode(
              const ObBorderFlag &border_flag,
              const int32_t query_start_column, 
              const int32_t query_column_count)
          {
            int32_t query_columns[query_column_count];
            generate_query_columns(query_start_column, query_columns, query_column_count);
            random_query_helper_mix_mode(border_flag, query_columns, query_column_count);
          }

          void random_query_helper_mix_mode(
              const ObBorderFlag &border_flag,
              const int32_t *query_columns, 
              const int32_t query_column_count)
          {
            int32_t start_row = 0;
            int32_t end_row = 0;
            int count = 0;
            const int64_t N = 1;
            for (; count < N; ++count)
            {
              start_row = static_cast<int32_t>(random_number(0, ROW_NUM-1));
              end_row = static_cast<int32_t>(random_number(start_row, ROW_NUM-1));
              if (count % 50 == 0) fprintf(stderr, "running %d query cases...\n", count);
              fprintf(stderr, "query forward, syncread mode , row range:(%d,%d)\n", start_row, end_row);
              query_helper(start_row, end_row, border_flag, query_columns, query_column_count, 
                  ScanFlag::FORWARD, ScanFlag::SYNCREAD);
              TearDown();
              SetUp();
              fprintf(stderr, "query backward, syncread mode , row range:(%d,%d)\n", start_row, end_row);
              query_helper(start_row, end_row, border_flag, query_columns, query_column_count, 
                  ScanFlag::BACKWARD, ScanFlag::SYNCREAD);
              TearDown();
              SetUp();
              fprintf(stderr, "query forward, preread mode , row range:(%d,%d)\n", start_row, end_row);
              query_helper(start_row, end_row, border_flag, query_columns, query_column_count, 
                  ScanFlag::FORWARD, ScanFlag::ASYNCREAD);
              TearDown();
              SetUp();
              fprintf(stderr, "query backward, preread mode , row range:(%d,%d)\n", start_row, end_row);
              query_helper(start_row, end_row, border_flag, query_columns, query_column_count, 
                  ScanFlag::BACKWARD, ScanFlag::ASYNCREAD);
            }
          }

        public:
          static void SetUpTestCase()
          {
            TBSYS_LOGGER.setLogLevel("ERROR");
            CellInfoGen::Desc desc[COLUMN_GROUP_NUM] = {
              {0, 0, ROWKEY_COL_NUM - 1}, 
              {1, ROWKEY_COL_NUM, ROWKEY_COL_NUM + GROUP1_COL_NUM - 1},
              {2, ROWKEY_COL_NUM + GROUP1_COL_NUM, COL_NUM - 1} 
            };
            int ret = write_sstable(sstable_id, ROW_NUM, COL_NUM, desc, COLUMN_GROUP_NUM);
            EXPECT_EQ(OB_SUCCESS, ret);

            ObRowkeyColumn split;
            for (int i = 0; i < ROWKEY_COL_NUM; ++i)
            {
              split.length_ = 8;
              split.column_id_ = i + CellInfoGen::START_ID;
              split.type_ = ObIntType;
              rowkey_info_.add_column(split);
            }
          }

          static void TearDownTestCase()
          {
          }

          virtual void SetUp()
          {
            oceanbase::common::ModuleArena * arena = GET_TSI_MULT(oceanbase::common::ModuleArena, TSI_SSTABLE_MODULE_ARENA_1);
            arena->set_page_size(1024*1024*2);
            arena->reuse();
            sstable_reader_.reset();
            int err = sstable_reader_.open(sstable_id, 0);
            EXPECT_EQ(0, err);
            EXPECT_TRUE(sstable_reader_.is_opened());
          }

          virtual void TearDown()
          {
            sstable_reader_.reset();
          }

          oceanbase::sstable::ObSSTableScanner scanner_;
      };


      TEST_F(TestObSSTableScanner, test_query_case_random_column)
      {
        ObBorderFlag border_flag;
        border_flag.set_inclusive_start(); //inclusive end
        border_flag.set_inclusive_end(); //inclusive end

        int32_t query_start_column =static_cast<int32_t>( random_number(ROWKEY_COL_NUM + CellInfoGen::START_ID, 
            COL_NUM + CellInfoGen::START_ID - 1));
        int32_t query_column_count = 1;

        //query_helper( 1, 2, border_flag, 5, 1, ScanFlag::FORWARD, ScanFlag::SYNCREAD);
        random_query_helper_mix_mode(border_flag, query_start_column, query_column_count);
      }

      TEST_F(TestObSSTableScanner, test_query_case_first_column)
      {
        ObBorderFlag border_flag;
        border_flag.set_inclusive_start(); //inclusive end
        border_flag.set_inclusive_end(); //inclusive end

        int32_t query_start_column = ROWKEY_COL_NUM + CellInfoGen::START_ID;
        int32_t query_column_count = 1;

        random_query_helper_mix_mode(border_flag, query_start_column, query_column_count);
      }

      TEST_F(TestObSSTableScanner, test_query_case_last_column)
      {
        ObBorderFlag border_flag;
        border_flag.set_inclusive_start(); //inclusive end
        border_flag.set_inclusive_end(); //inclusive end

        int32_t query_start_column = COL_NUM + CellInfoGen::START_ID - 1;
        int32_t query_column_count = 1;

        random_query_helper_mix_mode(border_flag, query_start_column, query_column_count);
      }

      TEST_F(TestObSSTableScanner, test_query_case_invalid_column)
      {
        ObBorderFlag border_flag;
        border_flag.set_inclusive_start(); //inclusive end
        border_flag.set_inclusive_end(); //inclusive end

        int32_t query_start_column = COL_NUM + CellInfoGen::START_ID + 10; // invalid columns;
        int32_t query_column_count = 1;

        random_query_helper_mix_mode(border_flag, query_start_column, query_column_count);
      }

      TEST_F(TestObSSTableScanner, test_query_case_left_open_random_columns)
      {
        ObBorderFlag border_flag;
        border_flag.unset_inclusive_start(); //inclusive end
        border_flag.set_inclusive_end(); //inclusive end

        int32_t query_start_column = static_cast<int32_t>(random_number(ROWKEY_COL_NUM + CellInfoGen::START_ID, 
            COL_NUM + CellInfoGen::START_ID - 1));
        int32_t query_column_count = static_cast<int32_t>(random_number(1, NONKEY_COL_NUM));

        random_query_helper_mix_mode(border_flag, query_start_column, query_column_count);
      }

      TEST_F(TestObSSTableScanner, test_query_case_right_open_random_columns)
      {
        ObBorderFlag border_flag;
        border_flag.set_inclusive_start(); //inclusive end
        border_flag.unset_inclusive_end(); //inclusive end

        int32_t query_start_column = static_cast<int32_t>(random_number(ROWKEY_COL_NUM + CellInfoGen::START_ID, 
            COL_NUM + CellInfoGen::START_ID - 1));
        int32_t query_column_count = static_cast<int32_t>(random_number(1, NONKEY_COL_NUM));

        random_query_helper_mix_mode(border_flag, query_start_column, query_column_count);
      }

      TEST_F(TestObSSTableScanner, test_query_case_lr_open_random_columns)
      {
        ObBorderFlag border_flag;
        border_flag.unset_inclusive_start(); //inclusive end
        border_flag.unset_inclusive_end(); //inclusive end

        int32_t query_start_column = static_cast<int32_t>(random_number(ROWKEY_COL_NUM + CellInfoGen::START_ID, 
            COL_NUM + CellInfoGen::START_ID - 1));
        int32_t query_column_count = static_cast<int32_t>(random_number(1, NONKEY_COL_NUM));

        random_query_helper_mix_mode(border_flag, query_start_column, query_column_count);
      }

      TEST_F(TestObSSTableScanner, test_query_case_min_value_random_columns)
      {
        ObBorderFlag border_flag;
        border_flag.set_min_value();
        border_flag.unset_inclusive_start(); //inclusive end
        border_flag.unset_inclusive_end(); //inclusive end

        int32_t query_start_column = static_cast<int32_t>(random_number(ROWKEY_COL_NUM + CellInfoGen::START_ID, 
            COL_NUM + CellInfoGen::START_ID - 1));
        int32_t query_column_count = static_cast<int32_t>(random_number(1, NONKEY_COL_NUM));

        random_query_helper_mix_mode(border_flag, query_start_column, query_column_count);
      }

      TEST_F(TestObSSTableScanner, test_query_case_max_value_random_columns)
      {
        ObBorderFlag border_flag;
        border_flag.unset_inclusive_start(); //inclusive end
        border_flag.unset_inclusive_end(); //inclusive end
        border_flag.set_max_value();

        int32_t query_start_column = static_cast<int32_t>(random_number(ROWKEY_COL_NUM + CellInfoGen::START_ID, 
            COL_NUM + CellInfoGen::START_ID - 1));
        int32_t query_column_count = static_cast<int32_t>(random_number(1, NONKEY_COL_NUM));

        random_query_helper_mix_mode(border_flag, query_start_column, query_column_count);
      }

      TEST_F(TestObSSTableScanner, test_query_case_min_max_value_random_columns)
      {
        ObBorderFlag border_flag;
        border_flag.unset_inclusive_start(); //inclusive end
        border_flag.unset_inclusive_end(); //inclusive end
        border_flag.set_min_value();
        border_flag.set_max_value();

        int32_t query_start_column = static_cast<int32_t>(random_number(ROWKEY_COL_NUM + CellInfoGen::START_ID, 
            COL_NUM + CellInfoGen::START_ID - 1));
        int32_t query_column_count = static_cast<int32_t>(random_number(1, NONKEY_COL_NUM));

        random_query_helper_mix_mode(border_flag, query_start_column, query_column_count);
      }


    } // end namespace sstable
  } // end namespace tests
} // end namespace oceanbase


int main(int argc, char** argv)
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
