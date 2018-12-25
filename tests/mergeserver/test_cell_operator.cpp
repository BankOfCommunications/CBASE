#include "common/ob_read_common_data.h"
#include "gtest/gtest.h"
#include "common/ob_malloc.h"
#include "common/ob_string.h"
#include "common/ob_action_flag.h"
#include "common/ob_common_param.h"
#include "mergeserver/ob_cell_operator.h"
#include "../common/test_rowkey_helper.h"

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::mergeserver;

static CharArena allocator_;
TEST(TestObCellInfo, intTestApply)
{ 
  ObCellInfo del_cell;
  del_cell.value_.set_null();
  ObCellInfo null_cell;
  ObCellInfo dst_cell;
  ObCellInfo src_cell;
  int64_t expect_val = 500;
  int64_t real_val = 0;
  char table_name[128] = "tablename";
  ObString table_name_str;
  table_name_str.assign(table_name,static_cast<int32_t>(strlen(table_name) + 1));
  src_cell.table_name_ = table_name_str;

  char rowkey[128] = "rowkey";
  ObString rowkey_str;
  rowkey_str.assign(rowkey,static_cast<int32_t>(strlen(rowkey)+1));
  src_cell.row_key_ = make_rowkey(rowkey, &allocator_);;

  char column_name[128] = "col";
  ObString column_name_str;
  column_name_str.assign(column_name,static_cast<int32_t>(strlen(column_name)+1));
  src_cell.column_name_ = column_name_str;

  /// copy cell
  src_cell.value_.set_int(expect_val);
  EXPECT_EQ(ob_cell_info_apply(dst_cell,src_cell),0);
  EXPECT_TRUE(dst_cell.table_name_== src_cell.table_name_);
  EXPECT_TRUE(dst_cell.row_key_== src_cell.row_key_);
  EXPECT_TRUE(dst_cell.column_name_== src_cell.column_name_);
  EXPECT_EQ(dst_cell.value_.get_int(real_val),0);
  EXPECT_EQ(real_val,expect_val);

  /// update 
  expect_val *=2;
  src_cell.value_.set_int(expect_val);
  EXPECT_EQ(ob_cell_info_apply(dst_cell,src_cell),0);
  EXPECT_EQ(dst_cell.value_.get_int(real_val),0);
  EXPECT_EQ(real_val,expect_val);

  /// insert 
  expect_val /= 2;
  src_cell.value_.set_int(expect_val);
  EXPECT_EQ(ob_cell_info_apply(dst_cell,src_cell),0);
  EXPECT_EQ(dst_cell.value_.get_int(real_val),0);
  EXPECT_EQ(real_val,expect_val);

  /// add
  expect_val -= 10;
  src_cell.value_.set_int(-10, true);
  EXPECT_EQ(ob_cell_info_apply(dst_cell,src_cell),0);
  EXPECT_EQ(dst_cell.value_.get_int(real_val),0);
  EXPECT_EQ(real_val,expect_val);

  /// apply NULL
  ///EXPECT_EQ(ob_obj_apply(src_cell.value_,null_cell.value_),0);
  EXPECT_EQ(src_cell.value_.apply(del_cell.value_),0);
  EXPECT_EQ(src_cell.value_.get_type(), ObNullType);
  EXPECT_NE(src_cell.value_.get_int(real_val),0);

  /// delete
  src_cell.value_.set_null();
  EXPECT_EQ(ob_cell_info_apply(dst_cell,src_cell),0);
  EXPECT_TRUE(dst_cell.table_name_== src_cell.table_name_);
  EXPECT_TRUE(dst_cell.row_key_== src_cell.row_key_);
  EXPECT_TRUE(dst_cell.column_name_== src_cell.column_name_);
  EXPECT_EQ(dst_cell.value_.get_type(), ObNullType);
  EXPECT_NE(dst_cell.value_.get_int(real_val),0);
}

TEST(TestObCellInfo, intTestCompare)
{
  ObObj obj_null;
  ObObj obj_this;
  ObObj obj_that;
  ASSERT_TRUE(obj_this == obj_that);
  ASSERT_FALSE(obj_this < obj_that);
  ASSERT_FALSE(obj_this > obj_that);

  /// eq
  obj_this.set_int(100);
  obj_that.set_int(100);
  ASSERT_TRUE(obj_this == obj_that);
  ASSERT_FALSE(obj_this < obj_that);
  ASSERT_FALSE(obj_this > obj_that);

  /// gt
  obj_this.set_int(200);
  obj_that.set_int(100);
  ASSERT_TRUE(obj_this > obj_that);
  ASSERT_FALSE(obj_this < obj_that);
  ASSERT_FALSE(obj_this == obj_that);

  /// lt
  ASSERT_TRUE(obj_that < obj_this);
  ASSERT_FALSE(obj_that > obj_this);
  ASSERT_FALSE(obj_that == obj_this);

  /// compare with null
  ASSERT_TRUE(obj_null < obj_this);
  ASSERT_FALSE(obj_null > obj_this);
  ASSERT_FALSE(obj_null == obj_this);
}

TEST(TestObCellInfo, stringTestCompare)
{
  ObObj obj_null;
  ObObj obj_this;
  ObObj obj_that;
  ASSERT_TRUE(obj_this == obj_that);
  ASSERT_FALSE(obj_this < obj_that);
  ASSERT_FALSE(obj_this > obj_that);
  char c_this = 'a';
  char c_that = 'a';
  ObString str_this;
  ObString str_that;

  /// eq
  str_this.assign(&c_this,1);
  str_that.assign(&c_that,1);
  obj_this.set_varchar(str_this);
  obj_that.set_varchar(str_that);
  ASSERT_TRUE(obj_this == obj_that);
  ASSERT_FALSE(obj_this < obj_that);
  ASSERT_FALSE(obj_this > obj_that);

  /// gt
  c_this ++;
  ASSERT_TRUE(obj_this > obj_that);
  ASSERT_FALSE(obj_this < obj_that);
  ASSERT_FALSE(obj_this == obj_that);

  /// lt
  ASSERT_TRUE(obj_that < obj_this);
  ASSERT_FALSE(obj_that > obj_this);
  ASSERT_FALSE(obj_that == obj_this);

  /// compare with null
  ASSERT_TRUE(obj_null < obj_this);
  ASSERT_FALSE(obj_null > obj_this);
  ASSERT_FALSE(obj_null == obj_this);
}

TEST(TestObCellInfo, stringTestApply)
{ 
  ObCellInfo del_cell;
  del_cell.value_.set_null();
  ObCellInfo null_cell;
  ObCellInfo dst_cell;
  ObCellInfo src_cell;
  char value_content[1024] = "content";
  ObString expect_val;
  ObString real_val;
  long long int expect_val_int = 500;
  char table_name[128] = "tablename";
  ObString table_name_str;
  table_name_str.assign(table_name,static_cast<int32_t>(strlen(table_name) + 1));
  src_cell.table_name_ = table_name_str;

  char rowkey[128] = "rowkey";
  src_cell.row_key_ = make_rowkey(rowkey, &allocator_);;

  char column_name[128] = "col";
  ObString column_name_str;
  column_name_str.assign(column_name,static_cast<int32_t>(strlen(column_name)+1));
  src_cell.column_name_ = column_name_str;

  /// copy cell
  expect_val.assign(value_content,static_cast<int32_t>(strlen(value_content)));
  src_cell.value_.set_varchar(expect_val);
  EXPECT_EQ(ob_cell_info_apply(dst_cell,src_cell),0);
  EXPECT_TRUE(dst_cell.table_name_== src_cell.table_name_);
  EXPECT_TRUE(dst_cell.row_key_== src_cell.row_key_);
  EXPECT_TRUE(dst_cell.column_name_== src_cell.column_name_);
  EXPECT_EQ(dst_cell.value_.get_varchar(real_val),0);
  EXPECT_TRUE(real_val == expect_val);

  /// update 
  expect_val_int *=2;
  snprintf(value_content,sizeof(value_content),"%lld",expect_val_int);
  expect_val.assign(value_content,static_cast<int32_t>(strlen(value_content)));
  src_cell.value_.set_varchar(expect_val);
  EXPECT_EQ(ob_cell_info_apply(dst_cell,src_cell),0);
  EXPECT_EQ(dst_cell.value_.get_varchar(real_val),0);
  EXPECT_TRUE(real_val == expect_val);

  /// insert 
  expect_val_int /=2;
  snprintf(value_content,sizeof(value_content),"%lld",expect_val_int);
  expect_val.assign(value_content,static_cast<int32_t>(strlen(value_content)));
  src_cell.value_.set_varchar(expect_val);
  EXPECT_EQ(ob_cell_info_apply(dst_cell,src_cell),0);
  EXPECT_EQ(dst_cell.value_.get_varchar(real_val),0);
  EXPECT_TRUE(real_val == expect_val);

  /// add 
  expect_val_int -= 10;
  snprintf(value_content,sizeof(value_content),"%lld",expect_val_int);
  expect_val.assign(value_content,static_cast<int32_t>(strlen(value_content)));
  src_cell.value_.set_varchar(expect_val);
  EXPECT_EQ(ob_cell_info_apply(dst_cell,src_cell),0);

  /// apply NULL
  /// EXPECT_EQ(ob_obj_apply(src_cell.value_,null_cell.value_),0);
  EXPECT_EQ(src_cell.value_.apply(del_cell.value_),0);
  EXPECT_EQ(src_cell.value_.get_type(), ObNullType);
  EXPECT_NE(src_cell.value_.get_varchar(real_val),0);

  /// delete
  src_cell.value_.set_null();
  EXPECT_EQ(ob_cell_info_apply(dst_cell,src_cell),0);
  EXPECT_TRUE(dst_cell.table_name_== src_cell.table_name_);
  EXPECT_TRUE(dst_cell.row_key_== src_cell.row_key_);
  EXPECT_TRUE(dst_cell.column_name_== src_cell.column_name_);
  EXPECT_EQ(dst_cell.value_.get_type(), ObNullType);
  EXPECT_NE(dst_cell.value_.get_varchar(real_val),0);
}

int main(int argc, char **argv)
{
  oceanbase::common::ob_init_memory_pool();

  testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
