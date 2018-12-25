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
#include "chunkserver/ob_fileinfo_cache.h"
#include "test_helper.h"
#include "sstable/ob_sstable_reader.h"
#include "sstable/ob_sstable_scanner.h"
#include "sstable/ob_blockcache.h"
#include "sstable/ob_block_index_cache.h"
#include "sql/ob_sstable_scan.h"

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
      static const int64_t sstable_file_id = 268801;
      static const int64_t sstable_file_offset = 0;
      static const ObSSTableId sstable_id(sstable_file_id);
      static ModulePageAllocator mod(0);
      static ModuleArena allocator(ModuleArena::DEFAULT_PAGE_SIZE, mod);
      static chunkserver::ObMultiVersionTabletImage image_(GFactory::get_instance().get_fi_cache());
      static ObRowkeyInfo rowkey_info_;
      const char* idx_file = "./tmp/idx_1_1";

      class TestObSSTableScan : public ::testing::Test
      {
        public:
          static const int32_t ROW_NUM = 10000;
          static const int32_t COL_NUM = 12;
          static const int32_t COLUMN_GROUP_NUM = 2;
          static const int32_t ROWKEY_COL_NUM = 3;
          static const int32_t GROUP1_COL_NUM = 4;
          static const int32_t GROUP2_COL_NUM = COL_NUM - ROWKEY_COL_NUM - GROUP1_COL_NUM;
          static const int32_t NONKEY_COL_NUM = COL_NUM - ROWKEY_COL_NUM;
          static const int64_t block_internal_bufsiz = BUFSIZ;
        public:
          TestObSSTableScan()
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
              ObSSTableScanParam& sstable_scan_param,
              const int64_t start_row, const int64_t end_row,
              const ObBorderFlag border_flag, const ScanFlag::Direction dir,
              const ScanFlag::SyncMode read_mode,
              const int32_t query_columns[], const int32_t query_column_count)
          {
            ObNewRange range;
            create_query_range(range, start_row, end_row, border_flag);

            ObScanParam scan_param;
            scan_param.set(CellInfoGen::table_id, table_name, range);
            scan_param.set_scan_direction(dir);
            scan_param.set_read_mode(read_mode);

            for (int32_t i = 0; i < query_column_count; ++i)
            {
              scan_param.add_column(query_columns[i]);
            }

            sstable_scan_param.assign(scan_param);

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

          int check_row(const ObRow* row,
              const int32_t* query_columns, const int32_t query_column_count,
              const bool is_reverse_scan, const int64_t expect_row_index)
          {
            int ret = OB_SUCCESS;
            int64_t col_index = 0;
            int64_t value = 0;
            int64_t r = 0;
            const ObObj *cell_obj = NULL;
            uint64_t table_id = 0;
            uint64_t column_id = 0;
            for (col_index = 0; col_index < query_column_count && OB_SUCCESS == ret; ++col_index)
            {
              ret = row->raw_get_cell(col_index, cell_obj, table_id, column_id);
              EXPECT_EQ(OB_SUCCESS, ret);
              if (query_columns[col_index] > COL_NUM + CellInfoGen::START_ID - 1)
              {
                EXPECT_EQ(ObNullType, cell_obj->get_type());
                EXPECT_EQ(column_id, (uint64_t)query_columns[col_index]);
              }
              else
              {
                EXPECT_EQ(column_id, (uint64_t)query_columns[col_index]);
                EXPECT_EQ(0, cell_obj->get_int(value));
                r = is_reverse_scan ? (expect_row_index + 1) : (expect_row_index - 1);
                EXPECT_EQ(value, r * COL_NUM +  (query_columns[col_index] - CellInfoGen::START_ID));
              }
            }

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

            ObSSTableScanParam sstable_scan_param;
            create_scan_param(sstable_scan_param,
                start_row, end_row, border_flag, dir, mode, query_columns, query_column_count);

            //ObCellInfo *cell_info = 0;
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

            //int ret = scanner_.set_scan_param(sstable_scan_param);
            int ret = scanner_.open_scan_context(sstable_scan_param, context_);
            ret = scanner_.open();

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


            ObScanColumnIndexes::Column column;
            const ObRowkey* r_rowkey = NULL;
            const ObRow* r_row = NULL;
            while (scanner_.get_next_row(r_rowkey, r_row) == 0)
            {
              EXPECT_EQ(OB_SUCCESS, check_rowkey(*r_rowkey, expect_row_index, COL_NUM))
                << "start row:" << start_row << ",end row:" << end_row
                << ",row_index:" << row_index ;

              ++row_index;
              if (is_reverse_scan) --expect_row_index;
              else ++expect_row_index;

              check_row(r_row, query_columns, query_column_count, is_reverse_scan, expect_row_index);

              ++total_count;
            }

            EXPECT_EQ(row_index, row_count);
            scanner_.close();
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
              fprintf(stderr, "query mix mode , row range:(%d,%d)\n", start_row, end_row);
              if (count % 50 == 0) fprintf(stderr, "running %d query cases...\n", count);
              query_helper(start_row, end_row, border_flag, query_columns, query_column_count,
                  ScanFlag::FORWARD, ScanFlag::SYNCREAD);
              TearDown();
              SetUp();
              query_helper(start_row, end_row, border_flag, query_columns, query_column_count,
                  ScanFlag::BACKWARD, ScanFlag::SYNCREAD);
              TearDown();
              SetUp();
              query_helper(start_row, end_row, border_flag, query_columns, query_column_count,
                  ScanFlag::FORWARD, ScanFlag::ASYNCREAD);
              TearDown();
              SetUp();
              query_helper(start_row, end_row, border_flag, query_columns, query_column_count,
                  ScanFlag::BACKWARD, ScanFlag::ASYNCREAD);
            }
          }

        public:
          static void SetUpTestCase()
          {
            //TBSYS_LOGGER.setLogLevel("ERROR");
            CellInfoGen::Desc desc[COLUMN_GROUP_NUM] = {
              {0, 0, ROWKEY_COL_NUM - 1},
              {0, ROWKEY_COL_NUM, ROWKEY_COL_NUM+NONKEY_COL_NUM-1}
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

            ObNewRange range;
            range.table_id_ = CellInfoGen::table_id;
            range.set_whole_range();
            ObTablet* tablet = NULL;
            ObMultiVersionTabletImage image(GFactory::get_instance().get_fi_cache());
            image.alloc_tablet_object(range, 1, tablet);
            if (NULL != tablet)
            {
              tablet->set_data_version(1);
              tablet->set_disk_no(1);
              tablet->add_sstable_by_id(sstable_id);
              image.add_tablet(tablet, false, true);
              image.dump();
              image.write(idx_file, 1, 1);
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
            image_.read(idx_file, 1, 1, true);
            image_.prepare_for_service();
            image_.dump();
            //sql::ScanContext context;
            context_.tablet_image_ = &image_;
            context_.block_cache_ = &GFactory::get_instance().get_block_cache();
            context_.block_index_cache_ = &GFactory::get_instance().get_block_index_cache();
            //scanner_.set_scan_context(context);
          }

          virtual void TearDown()
          {
            image_.reset();
          }

          sql::ObSSTableScan scanner_;
          sql::ScanContext context_;
      };

      TEST_F(TestObSSTableScan, query_one_row_with_one_valid_column)
      {

        int32_t query_start_column = static_cast<int32_t>(random_number(ROWKEY_COL_NUM + CellInfoGen::START_ID, COL_NUM + CellInfoGen::START_ID));
        ObBorderFlag border_flag;
        border_flag.set_data(3);


        query_helper( 0, 1, border_flag, query_start_column, 1, ScanFlag::FORWARD, ScanFlag::SYNCREAD);

      }

      TEST_F(TestObSSTableScan, test_query_case_random_column)
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

      TEST_F(TestObSSTableScan, test_query_case_first_column)
      {
        ObBorderFlag border_flag;
        border_flag.set_inclusive_start(); //inclusive end
        border_flag.set_inclusive_end(); //inclusive end

        int32_t query_start_column = ROWKEY_COL_NUM + CellInfoGen::START_ID;
        int32_t query_column_count = 1;

        random_query_helper_mix_mode(border_flag, query_start_column, query_column_count);
      }

      TEST_F(TestObSSTableScan, test_query_case_last_column)
      {
        ObBorderFlag border_flag;
        border_flag.set_inclusive_start(); //inclusive end
        border_flag.set_inclusive_end(); //inclusive end

        int32_t query_start_column = COL_NUM + CellInfoGen::START_ID - 1;
        int32_t query_column_count = 1;

        random_query_helper_mix_mode(border_flag, query_start_column, query_column_count);
      }

      TEST_F(TestObSSTableScan, test_query_case_invalid_column)
      {
        ObBorderFlag border_flag;
        border_flag.set_inclusive_start(); //inclusive end
        border_flag.set_inclusive_end(); //inclusive end

        int32_t query_start_column = COL_NUM + CellInfoGen::START_ID + 10; // invalid columns;
        int32_t query_column_count = 1;

        random_query_helper_mix_mode(border_flag, query_start_column, query_column_count);
      }

      TEST_F(TestObSSTableScan, test_query_case_left_open_random_columns)
      {
        ObBorderFlag border_flag;
        border_flag.unset_inclusive_start(); //inclusive end
        border_flag.set_inclusive_end(); //inclusive end

        int32_t query_start_column = static_cast<int32_t>(random_number(ROWKEY_COL_NUM + CellInfoGen::START_ID,
            COL_NUM + CellInfoGen::START_ID - 1));
        int32_t query_column_count = static_cast<int32_t>(random_number(1, NONKEY_COL_NUM));

        random_query_helper_mix_mode(border_flag, query_start_column, query_column_count);
      }

      TEST_F(TestObSSTableScan, test_query_case_right_open_random_columns)
      {
        ObBorderFlag border_flag;
        border_flag.set_inclusive_start(); //inclusive end
        border_flag.unset_inclusive_end(); //inclusive end

        int32_t query_start_column = static_cast<int32_t>(random_number(ROWKEY_COL_NUM + CellInfoGen::START_ID,
            COL_NUM + CellInfoGen::START_ID - 1));
        int32_t query_column_count = static_cast<int32_t>(random_number(1, NONKEY_COL_NUM));

        random_query_helper_mix_mode(border_flag, query_start_column, query_column_count);
      }

      TEST_F(TestObSSTableScan, test_query_case_lr_open_random_columns)
      {
        ObBorderFlag border_flag;
        border_flag.unset_inclusive_start(); //inclusive end
        border_flag.unset_inclusive_end(); //inclusive end

        int32_t query_start_column = static_cast<int32_t>(random_number(ROWKEY_COL_NUM + CellInfoGen::START_ID,
            COL_NUM + CellInfoGen::START_ID - 1));
        int32_t query_column_count = static_cast<int32_t>(random_number(1, NONKEY_COL_NUM));

        random_query_helper_mix_mode(border_flag, query_start_column, query_column_count);
      }

      TEST_F(TestObSSTableScan, test_query_case_min_value_random_columns)
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

      TEST_F(TestObSSTableScan, test_query_case_max_value_random_columns)
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

      TEST_F(TestObSSTableScan, test_query_case_min_max_value_random_columns)
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

      TEST_F(TestObSSTableScan, test_build_row_desc)
      {
        ObBorderFlag border_flag;
        border_flag.unset_inclusive_start(); //inclusive end
        border_flag.unset_inclusive_end(); //inclusive end
        border_flag.set_min_value();
        border_flag.set_max_value();

        int32_t query_start_column =  CellInfoGen::START_ID + 5;
        int32_t query_column_count = 2;
        int32_t query_columns[query_column_count];
        generate_query_columns(query_start_column, query_columns, query_column_count);

        ObSSTableScanParam sstable_scan_param;
        create_scan_param(sstable_scan_param,
            1, 100, border_flag, ScanFlag::FORWARD, ScanFlag::SYNCREAD, query_columns, query_column_count);

        ObNewRange range;
        range.table_id_ = CellInfoGen::table_id;
        range.set_whole_range();
        ObTablet* tablet = NULL;
        ObMultiVersionTabletImage image(GFactory::get_instance().get_fi_cache());
        image.alloc_tablet_object(range, 2, tablet);
        if (NULL != tablet)
        {
          tablet->set_data_version(1);
          tablet->set_disk_no(2);
          image.add_tablet(tablet, false, true);
          image.dump();
          //image.write(idx_file, 1, 1);
        }

        sql::ObSSTableScan scanner;
        sql::ScanContext context;
        context.tablet_image_ = &image;
        context.block_cache_ = &GFactory::get_instance().get_block_cache();
        context.block_index_cache_ = &GFactory::get_instance().get_block_index_cache();
        //scanner.set_scan_context(context);
        //int ret = scanner.set_scan_param(sstable_scan_param, context_);
        int ret = scanner.open_scan_context(sstable_scan_param, context);
        ASSERT_EQ(0, ret);
        ret = scanner.open();
        ASSERT_EQ(0, ret);
        const oceanbase::common::ObRowDesc * row_desc = NULL;
        ret = scanner.get_row_desc(row_desc);
        ASSERT_EQ(0, ret);
        ASSERT_TRUE(NULL != row_desc);

        uint64_t table_id = 0;
        uint64_t column_id = 0;
        for (int i = 0; i < row_desc->get_column_num(); ++i)
        {
          ret = row_desc->get_tid_cid(i, table_id, column_id);
          ASSERT_EQ(0, ret);
          ASSERT_EQ(range.table_id_, table_id);
          ASSERT_EQ(CellInfoGen::START_ID + 5 + i, (int64_t)column_id);
        }

        scanner.close();
      }


    } // end namespace sstable
  } // end namespace tests
} // end namespace oceanbase


int main(int argc, char** argv)
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
