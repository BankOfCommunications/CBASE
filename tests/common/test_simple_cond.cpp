#include <iostream>
#include <sstream>
#include <algorithm>
#include <tblog.h>
#include <gtest/gtest.h>
#include <iostream>
#include "ob_simple_condition.h"


#define BLOCK_FUNC() if(true) \

using namespace std;
using namespace oceanbase::common;

int main(int argc, char **argv)
{
  ob_init_memory_pool();
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}

class TestSimpleCond:public ::testing::Test
{
  public:
    virtual void SetUp()
    {
    }

    virtual void TearDown()
    {
    }
};


TEST_F(TestSimpleCond, test_index)
{
  ObSimpleCond cond;
  EXPECT_TRUE(cond.get_column_index() == ObSimpleCond::INVALID_INDEX);

  ObLogicOperator operate = LIKE;
  char * ptr = "test";
  ObString sub_string;
  sub_string.assign(ptr, strlen(ptr));
  
  ObObj operand;
  operand.set_varchar(sub_string);
  BLOCK_FUNC()
  {
    ObSimpleCond temp;
    EXPECT_TRUE(temp.set(123, operate, operand) == OB_SUCCESS);
    EXPECT_TRUE(temp.get_column_index() == 123);
  }

  BLOCK_FUNC()
  {
    char * column_name = "column";
    ObString name;
    name.assign(column_name, strlen(column_name));
    ObSimpleCond temp(name, operate, operand);
    EXPECT_TRUE(temp.get_column_index() == ObSimpleCond::INVALID_INDEX);
    EXPECT_TRUE(temp.get_column_name() == name);
  }
}

TEST_F(TestSimpleCond, test_check)
{
  ObLogicOperator operate = LIKE;
  ObString sub_string;
  ObObj operand;
  operand.set_varchar(sub_string);
  ObSimpleCond temp;
  EXPECT_TRUE(temp.set(123, operate, operand) != OB_SUCCESS);
  EXPECT_TRUE(temp.check(ObVarcharType) == false);
  
  char * ptr = "test";
  sub_string.assign(ptr, strlen(ptr));
  operand.set_varchar(sub_string);
  EXPECT_TRUE(temp.set(123, operate, operand) == OB_SUCCESS);
  EXPECT_TRUE(temp.check(ObVarcharType) == true);
  EXPECT_TRUE(temp.check(ObNullType) == false);
  EXPECT_TRUE(temp.check(ObIntType) == false);

  operate = NE;
  operand.set_null();
  EXPECT_TRUE(temp.set(123, operate, operand) == OB_SUCCESS);
  EXPECT_TRUE(temp.check(ObVarcharType) == true);
  EXPECT_TRUE(temp.check(ObIntType) == true);
  EXPECT_TRUE(temp.check(ObDateTimeType) == true);
  EXPECT_TRUE(temp.check(ObNullType) == true);
  
  operate = EQ;
  operand.set_null();
  EXPECT_TRUE(temp.set(123, operate, operand) == OB_SUCCESS);
  EXPECT_TRUE(temp.check(ObVarcharType) == true);
  EXPECT_TRUE(temp.check(ObIntType) == true);
  EXPECT_TRUE(temp.check(ObDateTimeType) == true);
  EXPECT_TRUE(temp.check(ObNullType) == true);

  operate = GT;
  operand.set_null();
  EXPECT_TRUE(temp.set(123, operate, operand) == OB_SUCCESS);
  EXPECT_TRUE(temp.check(ObVarcharType) == false);
  EXPECT_TRUE(temp.check(ObIntType) == false);
  EXPECT_TRUE(temp.check(ObDateTimeType) == false);
  EXPECT_TRUE(temp.check(ObNullType) == false);
  
  operate = GE;
  operand.set_datetime(1234567890);
  EXPECT_TRUE(temp.set(123, operate, operand) == OB_SUCCESS);
  EXPECT_TRUE(temp.check(ObVarcharType) == false);
  EXPECT_TRUE(temp.check(ObIntType) == false);
  EXPECT_TRUE(temp.check(ObDateTimeType) == true);
  EXPECT_TRUE(temp.check(ObModifyTimeType) == true);
  EXPECT_TRUE(temp.check(ObCreateTimeType) == true);
  EXPECT_TRUE(temp.check(ObPreciseDateTimeType) == true);
  EXPECT_TRUE(temp.check(ObNullType) == false);
  
  operand.set_precise_datetime(1234567890);
  EXPECT_TRUE(temp.set(123, operate, operand) == OB_SUCCESS);
  EXPECT_TRUE(temp.check(ObVarcharType) == false);
  EXPECT_TRUE(temp.check(ObIntType) == false);
  EXPECT_TRUE(temp.check(ObDateTimeType) == true);
  EXPECT_TRUE(temp.check(ObModifyTimeType) == true);
  EXPECT_TRUE(temp.check(ObCreateTimeType) == true);
  EXPECT_TRUE(temp.check(ObPreciseDateTimeType) == true);
  EXPECT_TRUE(temp.check(ObNullType) == false);
  
  operand.set_modifytime(1234567890);
  EXPECT_TRUE(temp.set(123, operate, operand) == OB_SUCCESS);
  EXPECT_TRUE(temp.check(ObVarcharType) == false);
  EXPECT_TRUE(temp.check(ObIntType) == false);
  EXPECT_TRUE(temp.check(ObDateTimeType) == true);
  EXPECT_TRUE(temp.check(ObModifyTimeType) == true);
  EXPECT_TRUE(temp.check(ObCreateTimeType) == true);
  EXPECT_TRUE(temp.check(ObPreciseDateTimeType) == true);
  EXPECT_TRUE(temp.check(ObNullType) == false);
  
  operand.set_createtime(1234567890);
  EXPECT_TRUE(temp.set(123, operate, operand) == OB_SUCCESS);
  EXPECT_TRUE(temp.check(ObVarcharType) == false);
  EXPECT_TRUE(temp.check(ObIntType) == false);
  EXPECT_TRUE(temp.check(ObDateTimeType) == true);
  EXPECT_TRUE(temp.check(ObModifyTimeType) == true);
  EXPECT_TRUE(temp.check(ObCreateTimeType) == true);
  EXPECT_TRUE(temp.check(ObPreciseDateTimeType) == true);
  EXPECT_TRUE(temp.check(ObNullType) == false);
}


TEST_F(TestSimpleCond, test_calc)
{
  ObLogicOperator operate = LIKE;
  char * ptr = "test";
  ObString sub_string;
  sub_string.assign(ptr, strlen(ptr));
  ObObj operand;
  operand.set_varchar(sub_string);
  ObSimpleCond temp;
  EXPECT_TRUE(temp.set(123, operate, operand) == OB_SUCCESS);

  // LIKE
  BLOCK_FUNC()
  {
    char * ptr1 = "aaatestaaa";
    ObString string1;
    string1.assign(ptr1, strlen(ptr1));
    operand.set_varchar(string1);
    EXPECT_TRUE(temp.calc(operand) == true);
    
    char * ptr2 = "tes1234tes1234";
    string1.assign(ptr2, strlen(ptr2));
    operand.set_varchar(string1);
    EXPECT_TRUE(temp.calc(operand) == false);
  }

  BLOCK_FUNC()
  {
    operate = LE;
    operand.set_int(1234);
    ObSimpleCond temp;
    EXPECT_TRUE(temp.set(123, operate, operand) == OB_SUCCESS);
    operand.set_int(1233);
    EXPECT_TRUE(temp.calc(operand) == true);
    operand.set_int(1234);
    EXPECT_TRUE(temp.calc(operand) == true);
    operand.set_int(1236);
    EXPECT_TRUE(temp.calc(operand) == false);
  }
  
  BLOCK_FUNC()
  {
    operate = LT;
    operand.set_int(1234);
    ObSimpleCond temp;
    EXPECT_TRUE(temp.set(123, operate, operand) == OB_SUCCESS);
    operand.set_int(1234);
    EXPECT_TRUE(temp.calc(operand) == false);
    operand.set_int(1235);
    EXPECT_TRUE(temp.calc(operand) == false);
    operand.set_int(1233);
    EXPECT_TRUE(temp.calc(operand) == true);
  }

  BLOCK_FUNC()
  {
    operate = GT;
    operand.set_int(1234);
    ObSimpleCond temp;
    EXPECT_TRUE(temp.set(123, operate, operand) == OB_SUCCESS);
    operand.set_int(1234);
    EXPECT_TRUE(temp.calc(operand) == false);
    operand.set_int(1235);
    EXPECT_TRUE(temp.calc(operand) == true);
    operand.set_int(1233);
    EXPECT_TRUE(temp.calc(operand) == false);
  }

  BLOCK_FUNC()
  {
    operate = GE;
    operand.set_int(1234);
    ObSimpleCond temp;
    EXPECT_TRUE(temp.set(123, operate, operand) == OB_SUCCESS);
    operand.set_int(1234);
    EXPECT_TRUE(temp.calc(operand) == true);
    operand.set_int(1235);
    EXPECT_TRUE(temp.calc(operand) == true);
    operand.set_int(1233);
    EXPECT_TRUE(temp.calc(operand) == false);
  }
  
  BLOCK_FUNC()
  {
    operate = EQ;
    operand.set_int(1234);
    ObSimpleCond temp;
    EXPECT_TRUE(temp.set(123, operate, operand) == OB_SUCCESS);
    operand.set_int(1234);
    EXPECT_TRUE(temp.calc(operand) == true);
    operand.set_int(1235);
    EXPECT_TRUE(temp.calc(operand) == false);
    operand.set_int(1233);
    EXPECT_TRUE(temp.calc(operand) == false);
  }

  BLOCK_FUNC()
  {
    operate = NE;
    operand.set_int(1234);
    ObSimpleCond temp;
    EXPECT_TRUE(temp.set(123, operate, operand) == OB_SUCCESS);
    operand.set_int(1234);
    EXPECT_TRUE(temp.calc(operand) == false);
    operand.set_int(1235);
    EXPECT_TRUE(temp.calc(operand) == true);
    operand.set_int(1233);
    EXPECT_TRUE(temp.calc(operand) == true);
  }

  // null type
  BLOCK_FUNC()
  {
    operate = LE;
    operand.set_null();
    ObSimpleCond temp;
    EXPECT_TRUE(temp.set(123, operate, operand) == OB_SUCCESS);
    operand.set_null();
    EXPECT_TRUE(temp.calc(operand) == false);
    operand.set_int(1233);
    EXPECT_TRUE(temp.calc(operand) == false);
    char * ptr1 = "aaatestaaa";
    ObString string1;
    string1.assign(ptr1, strlen(ptr1));
    operand.set_varchar(string1);
    EXPECT_TRUE(temp.calc(operand) == false);
    
    operate = LT;
    operand.set_null();
    EXPECT_TRUE(temp.set(123, operate, operand) == OB_SUCCESS);
    operand.set_null();
    EXPECT_TRUE(temp.calc(operand) == false);
    
    operand.set_int(1233);
    EXPECT_TRUE(temp.set(123, operate, operand) == OB_SUCCESS);
    operand.set_null();
    EXPECT_TRUE(temp.calc(operand) == false);
    
    operand.set_varchar(string1);
    EXPECT_TRUE(temp.set(123, operate, operand) == OB_SUCCESS);
    operand.set_null();
    EXPECT_TRUE(temp.calc(operand) == false);
    
    operate = GT;
    operand.set_null();
    EXPECT_TRUE(temp.set(123, operate, operand) == OB_SUCCESS);
    operand.set_null();
    EXPECT_TRUE(temp.calc(operand) == false);
    
    operand.set_int(1233);
    EXPECT_TRUE(temp.set(123, operate, operand) == OB_SUCCESS);
    operand.set_null();
    EXPECT_TRUE(temp.calc(operand) == false);
    
    operand.set_varchar(string1);
    EXPECT_TRUE(temp.set(123, operate, operand) == OB_SUCCESS);
    operand.set_null();
    EXPECT_TRUE(temp.calc(operand) == false);
    
    operate = GE;
    operand.set_null();
    EXPECT_TRUE(temp.set(123, operate, operand) == OB_SUCCESS);
    operand.set_null();
    EXPECT_TRUE(temp.calc(operand) == false);
    
    operand.set_int(1233);
    EXPECT_TRUE(temp.set(123, operate, operand) == OB_SUCCESS);
    operand.set_null();
    EXPECT_TRUE(temp.calc(operand) == false);
    
    operand.set_varchar(string1);
    EXPECT_TRUE(temp.set(123, operate, operand) == OB_SUCCESS);
    operand.set_null();
    EXPECT_TRUE(temp.calc(operand) == false);
    
    operate = LIKE;
    operand.set_null();
    EXPECT_TRUE(temp.set(123, operate, operand) != OB_SUCCESS);
    operand.set_int(1233);
    EXPECT_TRUE(temp.set(123, operate, operand) != OB_SUCCESS);
    
    operand.set_varchar(string1);
    EXPECT_TRUE(temp.set(123, operate, operand) == OB_SUCCESS);
    operand.set_null();
    EXPECT_TRUE(temp.calc(operand) == false);
    
    // ok can compare
    operate = NE;
    operand.set_null();
    EXPECT_TRUE(temp.set(123, operate, operand) == OB_SUCCESS);
    operand.set_null();
    EXPECT_TRUE(temp.calc(operand) == false);
    
    operand.set_int(1233);
    EXPECT_TRUE(temp.set(123, operate, operand) == OB_SUCCESS);
    operand.set_null();
    EXPECT_TRUE(temp.calc(operand) == true);
    
    operand.set_varchar(string1);
    EXPECT_TRUE(temp.set(123, operate, operand) == OB_SUCCESS);
    operand.set_null();
    EXPECT_TRUE(temp.calc(operand) == true);
    
    operate = EQ;
    operand.set_null();
    EXPECT_TRUE(temp.set(123, operate, operand) == OB_SUCCESS);
    operand.set_null();
    EXPECT_TRUE(temp.calc(operand) == true);
    
    operand.set_int(1233);
    EXPECT_TRUE(temp.set(123, operate, operand) == OB_SUCCESS);
    operand.set_null();
    EXPECT_TRUE(temp.calc(operand) == false);
    
    operand.set_varchar(string1);
    EXPECT_TRUE(temp.set(123, operate, operand) == OB_SUCCESS);
    operand.set_null();
    EXPECT_TRUE(temp.calc(operand) == false);
  }

  BLOCK_FUNC()
  {
    ObSimpleCond temp;
    operate = LE;
    operand.set_int(1233);
    EXPECT_TRUE(temp.set(123, operate, operand) == OB_SUCCESS);
    operand.set_null();
    EXPECT_TRUE(temp.calc(operand) == false);

    operate = LT;
    operand.set_int(1233);
    EXPECT_TRUE(temp.set(123, operate, operand) == OB_SUCCESS);
    operand.set_null();
    EXPECT_TRUE(temp.calc(operand) == false);

    operate = GE;
    operand.set_int(1233);
    EXPECT_TRUE(temp.set(123, operate, operand) == OB_SUCCESS);
    operand.set_null();
    EXPECT_TRUE(temp.calc(operand) == false);
    
    operate = GT;
    operand.set_int(1233);
    EXPECT_TRUE(temp.set(123, operate, operand) == OB_SUCCESS);
    operand.set_null();
    EXPECT_TRUE(temp.calc(operand) == false);
    
    operate = EQ;
    operand.set_int(1233);
    EXPECT_TRUE(temp.set(123, operate, operand) == OB_SUCCESS);
    operand.set_null();
    EXPECT_TRUE(temp.calc(operand) == false);
    
    operate = NE;
    operand.set_int(1233);
    EXPECT_TRUE(temp.set(123, operate, operand) == OB_SUCCESS);
    operand.set_null();
    EXPECT_TRUE(temp.calc(operand) == true);
  }
}


TEST_F(TestSimpleCond, test_find)
{
  char * ptr = "test";
  ObString sub_string;
  sub_string.assign(ptr, strlen(ptr));
  ObObj obj1;
  obj1.set_varchar(sub_string);
  
  ptr = "1234test1234";
  sub_string.assign(ptr, strlen(ptr));
  ObObj obj2;
  obj2.set_varchar(sub_string);
  uint64_t sign = 0;
  EXPECT_TRUE(ObSimpleCond::calc_sign(obj1, sign) == OB_SUCCESS);
  EXPECT_TRUE(ObSimpleCond::find(obj1, sign, obj2) == 4);

  ptr = "t1e1s123";
  sub_string.assign(ptr, strlen(ptr));
  obj2.set_varchar(sub_string);
  EXPECT_TRUE(ObSimpleCond::find(obj1, sign, obj2) != 0);

  ObObj obj3;
  obj3.set_int(123);
  EXPECT_TRUE(ObSimpleCond::find(obj1, sign, obj2) != 0);
  EXPECT_TRUE(ObSimpleCond::find(obj2, sign, obj1) != 0);
}


TEST_F(TestSimpleCond, test_serialize)
{
  ObLogicOperator operate = LIKE;
  char * ptr = "test";
  ObString sub_string;
  sub_string.assign(ptr, strlen(ptr));
  ObObj operand;
  operand.set_varchar(sub_string);
  char buffer[1024];
  int64_t pos = 0;
  
  BLOCK_FUNC()
  {
    ObSimpleCond cond;
    EXPECT_TRUE(cond.set(123, operate, operand) == OB_SUCCESS);
    EXPECT_TRUE(cond.serialize(buffer, cond.get_serialize_size() - 1, pos) != OB_SUCCESS);
    pos = 0;
    EXPECT_TRUE(cond.serialize(buffer, cond.get_serialize_size(), pos) == OB_SUCCESS);
    EXPECT_TRUE(cond.get_serialize_size() == pos);

    ObSimpleCond cond1;
    int64_t size = 0;
    EXPECT_TRUE(cond1.deserialize(buffer, sizeof(buffer), size) == OB_SUCCESS);
    EXPECT_TRUE(cond1.get_serialize_size() == size);
    EXPECT_TRUE(pos == size);
    EXPECT_TRUE(cond1 == cond);
  }

  BLOCK_FUNC()
  {
    ObString name;
    char * column_name = "column";
    name.assign(column_name, strlen(column_name));
    ObSimpleCond cond(name, operate, operand);
    EXPECT_TRUE(cond.serialize(buffer, cond.get_serialize_size() - 1, pos) != OB_SUCCESS);
    pos = 0;
    EXPECT_TRUE(cond.serialize(buffer, cond.get_serialize_size(), pos) == OB_SUCCESS);
    EXPECT_TRUE(cond.get_serialize_size() == pos);

    ObSimpleCond cond1;
    int64_t size = 0;
    EXPECT_TRUE(cond1.deserialize(buffer, sizeof(buffer), size) == OB_SUCCESS);
    EXPECT_TRUE(cond1.get_serialize_size() == size);
    EXPECT_TRUE(pos == size);
    EXPECT_TRUE(cond1 == cond);
  }
}

