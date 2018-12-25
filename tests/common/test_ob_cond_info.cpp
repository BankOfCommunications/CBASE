#include <gtest/gtest.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include "ob_string_buf.h"
#include "ob_define.h"
#include "ob_cond_info.h"
#include "test_rowkey_helper.h"

using namespace oceanbase::common;

static CharArena allocator_;

#define BLOCK_FUNC()  if (true) \

class TestCondInfo: public ::testing::Test
{
  public:
    virtual void SetUp()
    {
    }

    virtual void TearDown()
    {
    }
};


int main(int argc, char **argv)
{
  ob_init_memory_pool();
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}


TEST_F(TestCondInfo, test_set)
{
  ObCondInfo cond;
  EXPECT_TRUE(NIL == cond.get_operator());
  ObString table_name;
  char * table = (char*)"table";
  table_name.assign(table, static_cast<int32_t>(strlen(table)));
  ObString column_name;
  char * column = (char*)"column";
  column_name.assign(column, static_cast<int32_t>(strlen(column)));

  ObString str_row_key;
  str_row_key.assign((char*)"row_key", static_cast<int32_t>(strlen("row_key")));
  ObRowkey row_key = TestRowkeyHelper(str_row_key, &allocator_);

  ObObj value;
  value.set_int(1234);
  cond.set(EQ, table_name, row_key, column_name, value);
  EXPECT_TRUE(cond.is_exist_type() == false);
  cond.set(table_name, row_key, true);
  EXPECT_TRUE(cond.is_exist_type() == true);
  cond.set(table_name, row_key, false);
  EXPECT_TRUE(cond.is_exist_type() == true);
  cond.set(LT, 1234, row_key, 23, value);
  EXPECT_TRUE(cond.is_exist_type() == false);
  cond.reset();
  EXPECT_TRUE(NIL == cond.get_operator());
}

TEST_F(TestCondInfo, test_copy)
{
  ObCondInfo cond;
  EXPECT_TRUE(NIL == cond.get_operator());
  ObString table_name;
  char * table = (char*)"table";
  table_name.assign(table, static_cast<int32_t>(strlen(table)));
  ObString column_name;
  char * column = (char*)"column";
  column_name.assign(column, static_cast<int32_t>(strlen(column)));

  ObString str_row_key;
  str_row_key.assign((char*)"row_key", static_cast<int32_t>(strlen("row_key")));
  ObRowkey row_key = TestRowkeyHelper(str_row_key, &allocator_);

  ObObj value;
  value.set_int(1234);
  cond.set(EQ, table_name, row_key, column_name, value);
  EXPECT_TRUE(cond.is_exist_type() == false);
  
  ObCondInfo dest;
  ObStringBuf buffer;
  EXPECT_TRUE(OB_SUCCESS == cond.deep_copy(dest, buffer));
  EXPECT_TRUE(dest == cond);
  EXPECT_TRUE(cond == dest);
  EXPECT_TRUE(dest.is_exist_type() == false);
  cond.set(table_name, row_key, true);
  EXPECT_FALSE(cond == dest);
  EXPECT_FALSE(dest == cond);

  EXPECT_TRUE(cond.is_exist_type() == true);
  EXPECT_TRUE(dest.is_exist_type() == false);
  // not reset
  cond.set(LT, 1234, row_key, 23, value);
  EXPECT_TRUE(OB_SUCCESS == cond.deep_copy(dest, buffer));
  EXPECT_FALSE(cond == dest);
  cond.reset();
  dest.reset();
  // after reset
  cond.set(LT, 1234, row_key, 23, value);
  EXPECT_TRUE(OB_SUCCESS == cond.deep_copy(dest, buffer));
  EXPECT_TRUE(cond == dest);
}


TEST_F(TestCondInfo, test_serialize)
{
  ObCondInfo cond;
  EXPECT_TRUE(NIL == cond.get_operator());
  ObString table_name;
  char * table = (char*)"table";
  table_name.assign(table, static_cast<int32_t>(strlen(table)));
  ObString column_name;
  char * column = (char*)"column";
  column_name.assign(column, static_cast<int32_t>(strlen(column)));
  ObString str_row_key;
  str_row_key.assign((char*)"row_key", static_cast<int32_t>(strlen("row_key")));
  ObRowkey row_key = TestRowkeyHelper(str_row_key, &allocator_);
  ObObj value;
  value.set_int(1234);
  cond.set(EQ, table_name, row_key, column_name, value);
  EXPECT_TRUE(cond.is_exist_type() == false);
  
  char buffer[1024] = "";
  int64_t pos = 0;

  // name type
  int64_t len = cond.get_serialize_size();
  EXPECT_TRUE(cond.serialize(buffer, len - 1, pos) != OB_SUCCESS);
  pos = 0;
  EXPECT_TRUE(cond.serialize(buffer, len, pos) == OB_SUCCESS);
  ObCondInfo cond2;
  pos = 0;
  EXPECT_TRUE(cond2.deserialize(buffer, len, pos) == OB_SUCCESS);

  ObCondInfo dest;
  ObStringBuf Buffer;
  EXPECT_TRUE(OB_SUCCESS == cond.deep_copy(dest, Buffer));
  EXPECT_TRUE(dest == cond);
  EXPECT_TRUE(dest == cond2);
  EXPECT_TRUE(cond2 == cond);
  // id type
  cond.reset();
  cond.set(GT, 1234, row_key, 12, value);
  // reset
  len = cond.get_serialize_size();
  pos = 0;
  EXPECT_TRUE(cond.serialize(buffer, len, pos) == OB_SUCCESS);
  pos = 0;
  // cond2 not reset
  EXPECT_TRUE(cond2.deserialize(buffer, len, pos) == OB_SUCCESS);
  EXPECT_FALSE(cond2 == cond);
  cond2.reset();
  pos = 0;
  EXPECT_TRUE(cond2.deserialize(buffer, len, pos) == OB_SUCCESS);
  EXPECT_TRUE(cond2 == cond);
  
  // exist type
  cond.reset();
  cond2.reset();
  cond.set(table_name, row_key, true);
  len = cond.get_serialize_size();
  pos = 0;
  EXPECT_TRUE(cond.serialize(buffer, len, pos) == OB_SUCCESS);
  pos = 0;
  EXPECT_TRUE(cond2.deserialize(buffer, len, pos) == OB_SUCCESS);
  EXPECT_TRUE(cond2 == cond);
}

