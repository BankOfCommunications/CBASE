/**
 *  (C) 2010-2011 Taobao Inc.
 *  
 *  This program is free software; you can redistribute it
 *  and/or modify it under the terms of the GNU General Public
 *  License version 2 as published by the Free Software
 *  Foundation.
 *
 *  test_sstable_block_scanner.cpp is for what ...
 *
 *  Authors:
 *     qushan<qushan@taobao.com>
 *        
 */

#include <gtest/gtest.h>
#include "common/ob_malloc.h"
#include "common/ob_object.h"
#include "common/ob_define.h"
#include "common/ob_range2.h"
#include "common/page_arena.h"
#include "sstable/ob_sstable_block_reader.h"
#include "sstable/ob_sstable_block_scanner.h"
#include "sstable/ob_scan_column_indexes.h"
#include "sstable/ob_sstable_block_index_v2.h"
#include "sstable/ob_sstable_row.h"
#include "sstable/ob_sstable_block_builder.h"
#include "sstable/ob_sstable_trailer.h"
#include "test_helper.h"

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::sstable;

class TestObSSTableBlockScanner : public ::testing::Test
{
  public:
    static const int64_t ROW_NUM = 1000;
    static const int64_t COL_NUM = 10;
    static const int64_t COLUMN_GROUP_NUM = 2;
    static const int64_t ROWKEY_COL_NUM = 3;
    static const int64_t NONKEY_COL_NUM = COL_NUM - ROWKEY_COL_NUM;
    static const int64_t block_internal_bufsiz = BUFSIZ;

  public:
    TestObSSTableBlockScanner() : scanner_(indexes_) { }

  protected:
    void create_query_start_columns( const int32_t* column_ids, const int32_t  column_count)
    {
      ObScanColumnIndexes::ColumnType type = ObScanColumnIndexes::Normal;
      for (int32_t i = 0; i < column_count; ++i)
      {
        int64_t offset = cgen_.schema.find_offset_column_group_schema(
            CellInfoGen::table_id, 0, column_ids[i]);
        if (offset < 0) 
        {
          type = ObScanColumnIndexes::NotExist;
          offset = 0;
        }
        indexes_.add_column_id(type, static_cast<int32_t>(offset), column_ids[i]);
      }
    }

    void create_query_range(ObNewRange& range, 
        const int64_t start_row, const int64_t end_row,
        const ObBorderFlag border_flag)
    {
      ::create_new_range(range, COL_NUM, ROWKEY_COL_NUM, start_row, end_row, border_flag);
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
        const bool is_reverse_scan)
    {
      int32_t query_columns[query_column_count] ;
      generate_query_columns(query_start_column, query_columns, query_column_count);
      query_helper(start_row, end_row, border_flag, query_columns, query_column_count, is_reverse_scan);
    }

    void query_helper(
        const int32_t start_row, 
        const int32_t end_row, 
        const ObBorderFlag &border_flag,
        const int32_t *query_columns, 
        const int32_t query_column_count,
        const bool is_reverse_scan
        )
    {

      ObNewRange query_range;
      create_query_range(query_range, start_row, end_row, border_flag);
      create_query_start_columns(query_columns, query_column_count);

      ObCellInfo *cell_info = 0;
      int64_t row_count = 0; 
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

      expect_row_index = is_reverse_scan ? expect_end_row : expect_start_row;
      if (!is_reverse_scan && !border_flag.is_min_value() && !border_flag.inclusive_start()) expect_row_index++;
      if (is_reverse_scan && !border_flag.is_max_value() && !border_flag.inclusive_end()) expect_row_index--;

      bool need_looking_forward = false;
      int ret = scanner_.set_scan_param(query_range,  is_reverse_scan, block_desc_, block_data_, need_looking_forward);

      if (row_count <= 0 || expect_row_index < 0 || expect_row_index >= ROW_NUM)
      {
        EXPECT_EQ(OB_BEYOND_THE_RANGE, ret)
          << "start row:" << start_row << ",end row:" << end_row;
      }
      else if (row_count > 0)
      {
        EXPECT_EQ(0, ret)
          << "start row:" << start_row << ",end row:" << end_row;
      }


      ObScanColumnIndexes::Column column;
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

        indexes_.get_column(col_index, column);
        if (column.type_ == ObScanColumnIndexes::NotExist)
        {
          EXPECT_EQ(ObNullType, cell_info->value_.get_type());
        }
        else
        {
          EXPECT_EQ(ObIntType, cell_info->value_.get_type());
          int64_t val = 0;
          EXPECT_EQ(0, cell_info->value_.get_int(val));
          int32_t r = is_reverse_scan ? (expect_row_index + 1) : (expect_row_index - 1);
          EXPECT_EQ(val, r * COL_NUM +  (query_columns[col_index] - CellInfoGen::START_ID));
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
      for (; count < 10; ++count)
      {
        start_row = static_cast<int32_t>(random_number(0, ROW_NUM-1));
        end_row = static_cast<int32_t>(random_number(start_row, ROW_NUM-1));
        fprintf(stderr, "query mix mode , row range:(%d,%d)\n", start_row, end_row);
        if (count % 50 == 0) fprintf(stderr, "running %d query cases...\n", count);
        query_helper(start_row, end_row, border_flag, query_columns, query_column_count, false);
        TearDown();
        query_helper(start_row, end_row, border_flag, query_columns, query_column_count, true);
        TearDown();
      }
    }

  public:
    static void SetUpTestCase()
    {
      TBSYS_LOGGER.setLogLevel("ERROR");
      CellInfoGen::Desc desc[COLUMN_GROUP_NUM] = {
        {0, 0, ROWKEY_COL_NUM - 1}, 
        {0, ROWKEY_COL_NUM, ROWKEY_COL_NUM+NONKEY_COL_NUM-1} 
      };
      int ret = cgen_.gen(desc, COLUMN_GROUP_NUM);
      EXPECT_EQ(OB_SUCCESS, ret);
      ret = write_block(builder_, cgen_, desc, COLUMN_GROUP_NUM);
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
      block_desc_.rowkey_info_ = &rowkey_info_;
      block_desc_.rowkey_column_count_ = ROWKEY_COL_NUM;
      block_desc_.store_style_ = OB_SSTABLE_STORE_DENSE;

      block_data_.internal_buffer_ = block_internal_buffer;
      block_data_.internal_bufsiz_ = block_internal_bufsiz;
      block_data_.data_buffer_ = builder_.block_buf();
      block_data_.data_bufsiz_ = builder_.get_block_data_size();
    }

    virtual void TearDown()
    {
      indexes_.reset();
      memset(block_internal_buffer, 0, block_internal_bufsiz);
    }


    ObNewRange query_range_;
    ObScanColumnIndexes indexes_;

    sstable::ObSSTableBlockScanner scanner_;

    char block_internal_buffer[block_internal_bufsiz];
    ObSSTableBlockReader::BlockDataDesc block_desc_;
    ObSSTableBlockReader::BlockData block_data_;

    static CellInfoGen cgen_;
    static ObSSTableBlockBuilder builder_;
    static ObRowkeyInfo rowkey_info_;

};

CellInfoGen TestObSSTableBlockScanner::cgen_(TestObSSTableBlockScanner::ROW_NUM, TestObSSTableBlockScanner::COL_NUM);
ObSSTableBlockBuilder TestObSSTableBlockScanner::builder_;
ObRowkeyInfo TestObSSTableBlockScanner::rowkey_info_;



TEST_F(TestObSSTableBlockScanner, query_one_row_with_one_valid_column)
{

  int32_t query_start_column = static_cast<int32_t>(random_number(ROWKEY_COL_NUM + CellInfoGen::START_ID, COL_NUM + CellInfoGen::START_ID));
  ObBorderFlag border_flag;
  border_flag.set_data(3);


  query_helper(0, 1, border_flag, query_start_column, 1, false);

}


TEST_F(TestObSSTableBlockScanner, query_one_valid_column)
{

  int32_t query_start_column = static_cast<int32_t>(random_number(ROWKEY_COL_NUM + CellInfoGen::START_ID, COL_NUM + CellInfoGen::START_ID));
  ObBorderFlag border_flag;
  border_flag.set_data(3);


  random_query_helper_mix_mode(border_flag, query_start_column, 1);

}

TEST_F(TestObSSTableBlockScanner, query_all_valid_column)
{
  const int32_t query_start_column = ROWKEY_COL_NUM + CellInfoGen::START_ID;
  const int32_t query_column_count = NONKEY_COL_NUM;
  ObBorderFlag border_flag;
  border_flag.set_data(3);

  random_query_helper_mix_mode(border_flag, query_start_column, query_column_count);
}

TEST_F(TestObSSTableBlockScanner, query_random_valid_column)
{

  const int32_t query_column_count = 4;
  const int32_t query_columns[query_column_count] = {5, 7, 8, 9};
  ObBorderFlag border_flag;
  border_flag.set_data(3);

  random_query_helper_mix_mode(border_flag, query_columns, query_column_count);
}

TEST_F(TestObSSTableBlockScanner, query_one_invalid_column)
{
  const int32_t query_start_column = ROWKEY_COL_NUM + CellInfoGen::START_ID + NONKEY_COL_NUM + 5;
  const int32_t query_column_count = 1;
  ObBorderFlag border_flag;
  border_flag.set_data(3);

  random_query_helper_mix_mode(border_flag, query_start_column, query_column_count);
}

TEST_F(TestObSSTableBlockScanner, query_some_invalid_column)
{
  const int32_t query_start_column = ROWKEY_COL_NUM + CellInfoGen::START_ID + NONKEY_COL_NUM + 5;
  const int32_t query_column_count = 4;
  ObBorderFlag border_flag;
  border_flag.set_data(3);

  random_query_helper_mix_mode(border_flag, query_start_column, query_column_count);
}

TEST_F(TestObSSTableBlockScanner, query_mix_columns)
{
  const int32_t query_column_count = 4;
  const int32_t query_columns[query_column_count] = {5, 27, 8, 39};
  ObBorderFlag border_flag;
  border_flag.set_data(3);

  random_query_helper_mix_mode(border_flag, query_columns, query_column_count);
}

TEST_F(TestObSSTableBlockScanner, query_one_single_row_block)
{
  const int32_t query_column_count = 4;
  const int32_t query_columns[query_column_count] = {5, 27, 8, 39};

  int32_t start_row = static_cast<int32_t>(random_number(0, ROW_NUM - 1));
  ObBorderFlag border_flag;
  border_flag.set_data(3);

  query_helper(start_row, start_row+1, border_flag, query_columns, query_column_count, false);
  TearDown();
  query_helper(start_row, start_row+1, border_flag, query_columns, query_column_count, true);

}

TEST_F(TestObSSTableBlockScanner, query_not_in_blocks)
{
  //ObNewRange query_range;
  const int32_t query_column_count = 4;
  const int32_t query_columns[query_column_count] = {5, 27, 8, 39};
  ObBorderFlag border_flag;
  border_flag.set_data(3);

  random_query_helper_mix_mode(border_flag, query_columns, query_column_count);
}

TEST_F(TestObSSTableBlockScanner, query_not_inclsive_start)
{
  ObNewRange query_range;
  const int32_t query_column_count = 4;
  const int32_t query_columns[query_column_count] = {5, 27, 8, 39};
  ObBorderFlag border_flag;
  border_flag.unset_inclusive_start();
  border_flag.set_inclusive_end();
  random_query_helper_mix_mode(border_flag, query_columns, query_column_count);

}

TEST_F(TestObSSTableBlockScanner, query_not_inclsive_end)
{
  ObNewRange query_range;
  const int32_t query_column_count = 4;
  const int32_t query_columns[query_column_count] = {5, 27, 8, 39};
  ObBorderFlag border_flag;
  border_flag.set_inclusive_start();
  border_flag.unset_inclusive_end();
  random_query_helper_mix_mode(border_flag, query_columns, query_column_count);
}

TEST_F(TestObSSTableBlockScanner, query_not_inclsive_start_end)
{
  ObNewRange query_range;
  const int32_t query_column_count = 4;
  const int32_t query_columns[query_column_count] = {5, 27, 8, 39};
  ObBorderFlag border_flag;
  border_flag.unset_inclusive_start();
  border_flag.unset_inclusive_end();
  random_query_helper_mix_mode(border_flag, query_columns, query_column_count);
}

TEST_F(TestObSSTableBlockScanner, query_with_min_value)
{
  ObNewRange query_range;
  const int32_t query_column_count = 4;
  const int32_t query_columns[query_column_count] = {5, 27, 8, 39};
  ObBorderFlag border_flag;
  border_flag.set_min_value();
  border_flag.unset_inclusive_end();
  random_query_helper_mix_mode(border_flag, query_columns, query_column_count);

  TearDown();

  border_flag.set_min_value();
  border_flag.set_inclusive_end();
  random_query_helper_mix_mode(border_flag, query_columns, query_column_count);

}

TEST_F(TestObSSTableBlockScanner, query_with_max_value)
{
  ObNewRange query_range;
  const int32_t query_column_count = 4;
  const int32_t query_columns[query_column_count] = {5, 27, 8, 39};
  ObBorderFlag border_flag;
  border_flag.unset_inclusive_start();
  border_flag.set_max_value();
  random_query_helper_mix_mode(border_flag, query_columns, query_column_count);

  border_flag.set_inclusive_start();
  border_flag.set_max_value();
  random_query_helper_mix_mode(border_flag, query_columns, query_column_count);
}

TEST_F(TestObSSTableBlockScanner, query_with_min_max_value)
{
  ObNewRange query_range;
  const int32_t query_column_count = 4;
  const int32_t query_columns[query_column_count] = {5, 27, 8, 39};
  ObBorderFlag border_flag;
  border_flag.set_min_value();
  border_flag.set_max_value();
  query_helper(-1, 0, border_flag, query_columns, query_column_count, false);
  TearDown();
  query_helper(-1, 0, border_flag, query_columns, query_column_count, true);
}

int main(int argc, char **argv)
{
  ob_init_memory_pool();
  testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}

