/**
 *  (C) 2010-2011 Taobao Inc.
 *  
 *  This program is free software; you can redistribute it
 *  and/or modify it under the terms of the GNU General Public
 *  License version 2 as published by the Free Software
 *  Foundation.
 *
 *  test_sstable_block_reader.cpp is for what ...
 *
 *  Authors:
 *     qushan<qushan@taobao.com>
 *        
 */

#include <gtest/gtest.h>
#include "common/ob_malloc.h"
#include "common/ob_object.h"
#include "common/ob_define.h"
#include "common/page_arena.h"
#include "test_helper.h"
#include "sstable/ob_sstable_block_reader.h"
#include "sstable/ob_sstable_block_index_v2.h"
#include "sstable/ob_sstable_row.h"
#include "sstable/ob_sstable_trailer.h"
#include "sstable/ob_sstable_block_builder.h"
using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::sstable;



TEST(ObTestObSSTableBlockReader, add)
{

  ObSSTableBlockBuilder builder;

  const int64_t ROW_NUM = 100;
  const int64_t COL_NUM = 10;
  const int64_t COLUMN_GROUP_NUM = 2;
  const int64_t ROWKEY_COL_NUM = 2;
  const int64_t NONKEY_COL_NUM = COL_NUM - ROWKEY_COL_NUM;

  CellInfoGen cgen(ROW_NUM, COL_NUM);
  CellInfoGen::Desc desc[COLUMN_GROUP_NUM] = {
    {0, 0, ROWKEY_COL_NUM - 1}, 
    {0, ROWKEY_COL_NUM, ROWKEY_COL_NUM+NONKEY_COL_NUM-1} 
  };


  int ret = cgen.gen(desc, ROWKEY_COL_NUM);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = write_block(builder, cgen, desc, COLUMN_GROUP_NUM);


  char internal_buffer[BUFSIZ];
  ObSSTableBlockReader::BlockDataDesc block_desc((ObRowkeyInfo*)1, ROWKEY_COL_NUM, OB_SSTABLE_STORE_DENSE);
  ObSSTableBlockReader::BlockData block_data(internal_buffer, BUFSIZ, builder.block_buf(), builder.get_block_data_size());


  ObSSTableBlockReader image;
  ret = image.deserialize(block_desc, block_data);
  ASSERT_EQ(0, ret);
  EXPECT_EQ(ROW_NUM, image.get_row_count());


  typedef ObSSTableBlockReader::const_iterator const_iterator;

  ObRowkey rowkey;
  ObRowkey query_rowkey;
  ObObj ids[OB_MAX_COLUMN_NUMBER];
  ObObj query_columns[OB_MAX_COLUMN_NUMBER];
  int64_t column_size = OB_MAX_COLUMN_NUMBER;

  for (int64_t row = 0; row < ROW_NUM; ++row)
  {
    create_rowkey(row, COL_NUM, ROWKEY_COL_NUM, rowkey);

    const_iterator index = image.lower_bound(rowkey);
    EXPECT_LT(index, image.end());


    ret = image.get_row(OB_SSTABLE_STORE_DENSE, index, query_rowkey, ids, query_columns, column_size);
    EXPECT_EQ(0, ret);
    EXPECT_EQ(OB_SUCCESS, check_rowkey(query_rowkey, row, COL_NUM));
    EXPECT_EQ(NONKEY_COL_NUM, column_size);
    EXPECT_EQ(OB_SUCCESS, check_rows(query_columns, column_size, row, COL_NUM, ROWKEY_COL_NUM));
  }

}

int main(int argc, char **argv)
{
  ob_init_memory_pool();
  testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}

