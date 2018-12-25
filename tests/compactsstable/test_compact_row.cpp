/*
* (C) 2007-2011 TaoBao Inc.
*
* This program is free software; you can redistribute it and/or modify
* it under the terms of the GNU General Public License version 2 as
* published by the Free Software Foundation.
*
* test_compact_row.cpp is for what ...
*
* Version: $id$
*
* Authors:
*   MaoQi maoqi@taobao.com
*
*/
#include <gtest/gtest.h>
#include "common/ob_define.h"
#include "common/ob_malloc.h"
#include "compactsstable/ob_compact_row.h"

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::compactsstable;


TEST(ObCompactRow,test_basic_row)
{
  ObCompactRow row;

  char rowkey_buf[8];
  ObString rowkey;
  rowkey.assign_ptr(rowkey_buf,sizeof(rowkey_buf));
  ObObj obj;
  ObObj rowkey_obj;
  rowkey_obj.set_varchar(rowkey);
  obj.set_int(97);
  for(int64_t i=0;i<10;++i)
  {
    row.reset();
    snprintf(rowkey_buf,sizeof(rowkey_buf),"%08ld",i);
    ASSERT_EQ(0,row.add_col(1,rowkey_obj));
    ASSERT_EQ(0,row.add_col(16,obj));
    ASSERT_EQ(0,row.set_end());
    ASSERT_EQ(13,row.get_key_columns_len(1));
    ASSERT_EQ(5,row.get_value_columns_len(1));
  }
}


TEST(ObCompactRow,test_set_row)
{
  ObCompactRow row;
  char rowkey_buf[8];
  ObString rowkey;
  rowkey.assign_ptr(rowkey_buf,sizeof(rowkey_buf));
  ObObj obj;
  ObObj rowkey_obj;
  rowkey_obj.set_varchar(rowkey);

  snprintf(rowkey_buf,sizeof(rowkey_buf),"%08ld",0L);
  row.add_col(1,rowkey_obj);
  for(int64_t i=2;i<10;++i)
  {
    obj.set_int(i);
    ASSERT_EQ(0,row.add_col(i,obj));
  }
  ASSERT_EQ(0,row.set_end());

  ASSERT_EQ(9,row.get_col_num());
  ASSERT_EQ(13,row.get_key_columns_len(1));
  ASSERT_EQ(33,row.get_value_columns_len(1));

  ObCellInfo* cell = NULL;

  ASSERT_EQ(0,row.next_cell());
  ASSERT_EQ(0,row.get_cell(&cell));
  ASSERT_TRUE(rowkey_obj == cell->value_);
  int64_t value = 2;  
  while( OB_SUCCESS == row.next_cell() )
  {
    obj.set_int(value);
    ASSERT_EQ(0,row.get_cell(&cell));
    ASSERT_TRUE(obj == cell->value_);
    ++value;
  }
  ASSERT_EQ(10,value);

  ObCompactRow other_row;
  ASSERT_EQ(0,other_row.set_row(row.get_key_buffer(),row.get_row_size()));
  ASSERT_EQ(9,other_row.get_col_num());
  ASSERT_EQ(13,other_row.get_key_columns_len(1));
  ASSERT_EQ(33,other_row.get_value_columns_len(1));

  ASSERT_EQ(0,other_row.next_cell());
  ASSERT_EQ(0,other_row.get_cell(&cell));
  ASSERT_TRUE(rowkey_obj == cell->value_);
  value = 2;  
  while( OB_SUCCESS == other_row.next_cell() )
  {
    obj.set_int(value);
    ASSERT_EQ(0,other_row.get_cell(&cell));
    ASSERT_TRUE(obj == cell->value_);
    ++value;
  }
  ASSERT_EQ(10,value);
}

TEST(ObCompactRow,test_int)
{
  ObCompactRow row;
  char rowkey_buf[8];
  ObString rowkey;
  rowkey.assign_ptr(rowkey_buf,sizeof(rowkey_buf));
  ObObj obj;
  ObObj rowkey_obj;
  rowkey_obj.set_varchar(rowkey);

  snprintf(rowkey_buf,sizeof(rowkey_buf),"%08ld",0L);
  //varchar
  ASSERT_EQ(0,row.add_col(1,rowkey_obj));

  int64_t array[] = {127L,32767L,2147483647L,1L << 47};
  //int
  for(uint64_t i=0; i < 4; ++i)
  {
    obj.set_int(array[i]);  
    ASSERT_EQ(0,row.add_col(i + 2,obj));
  }

  ASSERT_EQ(0,row.set_end());

  ASSERT_EQ(5,row.get_col_num());
  ASSERT_EQ(13,row.get_key_columns_len(1));
  ASSERT_EQ(28,row.get_value_columns_len(1));

  ObCellInfo* cell = NULL;

  ASSERT_EQ(0,row.next_cell());
  ASSERT_EQ(0,row.get_cell(&cell));
  ASSERT_TRUE(rowkey_obj == cell->value_);
  int64_t value=0;
  while( value < 4 && OB_SUCCESS == row.next_cell() )
  {
    obj.set_int(array[value]);
    ASSERT_EQ(0,row.get_cell(&cell));
    ASSERT_TRUE(obj == cell->value_);
    ++value;
  }
  ASSERT_EQ(4,value);
}



int main(int argc, char **argv)
{
  common::ob_init_memory_pool();
  testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
