#include <iostream>
#include <sstream>
#include <algorithm>
#include <tblog.h>
#include <gtest/gtest.h>
#include <iostream>

#include "ob_common_param.h"
#include "ob_cell_array.h"
#include "ob_simple_filter.h"

#define BLOCK_FUNC() if(true) \

using namespace std;
using namespace oceanbase::common;

int main(int argc, char **argv)
{
  ob_init_memory_pool();
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}

class TestSimpleFilter:public ::testing::Test
{
  public:
    virtual void SetUp()
    {
    }

    virtual void TearDown()
    {
    }
};

TEST_F(TestSimpleFilter, test_add_cond)
{
  ObStringBuf buf;
  ObLogicOperator operate = LIKE;
  char * ptr = "test_test";
  ObString str;
  ObString sub_string;
  str.assign(ptr, strlen(ptr));
  buf.write_string(str,&sub_string);
  ObObj operand;
  operand.set_varchar(sub_string);
  
  int64_t count = 0;
  ObSimpleFilter filter;
  EXPECT_TRUE(filter.add_cond(2, operate, operand) == OB_SUCCESS);
  ++count;
  const ObSimpleCond * cond = filter[1];
  EXPECT_TRUE(cond == NULL);
  cond = filter[0];
  EXPECT_TRUE(cond != NULL);
  EXPECT_TRUE(cond->get_column_index() == 2);
  EXPECT_TRUE(cond->get_logic_operator() == operate);
  EXPECT_TRUE(cond->get_right_operand() == operand);

  operate = GE;
  operand.set_int(23);
  EXPECT_TRUE(filter.add_cond(1, operate, operand) == OB_SUCCESS);
  ++count; 
  // add some much string
  for (int64_t i = 3; i < 128; ++i)
  {
    operate = NE;
    operand.set_varchar(sub_string);
    EXPECT_TRUE(filter.add_cond(i, operate, operand) == OB_SUCCESS);
    ++count;
    cond = filter[count - 1];
    EXPECT_TRUE(cond != NULL);
    EXPECT_TRUE(cond->get_column_index() == i);
    EXPECT_TRUE(cond->get_logic_operator() == operate);
    EXPECT_TRUE(cond->get_right_operand() == operand);
  }
  
  char name[128] = "";
  ObString column_name;
  int64_t old_count = count;
  for (int64_t i = count; i < 128; ++i)
  {
    snprintf(name, sizeof(name), "column_%ld", i);
    str.assign(name, strlen(name));
    buf.write_string(str,&column_name);

    operand.set_varchar(sub_string);
    EXPECT_TRUE(filter.add_cond(column_name, operate, operand) == OB_SUCCESS);
    ++count;
  }
  
  for (int64_t i = old_count; i < count; ++i)
  {
    cond = filter[i];
    snprintf(name, sizeof(name), "column_%ld", i);
    str.assign(name, strlen(name));
    buf.write_string(str,&column_name);
    EXPECT_TRUE(cond != NULL);
    EXPECT_TRUE(cond->get_column_name() == column_name);
    EXPECT_TRUE(cond->get_column_index() == ObSimpleCond::INVALID_INDEX);
    EXPECT_TRUE(cond->get_logic_operator() == operate);
    EXPECT_TRUE(cond->get_right_operand() == operand);
  }

  filter.reset();
  for (int64_t i = 0; i < 128; ++i)
  {
    snprintf(name, sizeof(name), "column_%ld", i);
    str.assign(name, strlen(name));
    buf.write_string(str,&column_name);
    operand.set_varchar(sub_string);
    EXPECT_TRUE(filter.add_cond(column_name, operate, operand) == OB_SUCCESS);
  }
  
  for (int64_t i = 0; i < 128; ++i)
  {
    cond = filter[i];
    snprintf(name, sizeof(name), "column_%ld", i);
    str.assign(name, strlen(name));
    buf.write_string(str,&column_name);
    EXPECT_TRUE(cond != NULL);
    EXPECT_TRUE(cond->get_column_name() == column_name);
    EXPECT_TRUE(cond->get_column_index() == ObSimpleCond::INVALID_INDEX);
    EXPECT_TRUE(cond->get_logic_operator() == operate);
  }
}

TEST_F(TestSimpleFilter, test_check_cond)
{
  ObSimpleFilter filter;
  
  // 2, LIKE test_test
  ObLogicOperator operate = LIKE;
  char * ptr = "test_test";
  ObString sub_string;
  sub_string.assign(ptr, strlen(ptr));
  ObObj operand;
  operand.set_varchar(sub_string);
  EXPECT_TRUE(filter.add_cond(2, operate, operand) == OB_SUCCESS);
  
  // 1, NE 23
  operate = NE;
  operand.set_int(23);
  EXPECT_TRUE(filter.add_cond(1, operate, operand) == OB_SUCCESS);
  
  // 3, LT test_test
  operate = LT;
  operand.set_varchar(sub_string);
  EXPECT_TRUE(filter.add_cond(3, operate, operand) == OB_SUCCESS);
 
  ObCellInfo cell;
  cell.table_id_ = 1;
  cell.column_id_ = 2;
  ObCellInfo * out_cell;
  ObObj obj;
  obj.set_int(1234);
  cell.value_ = obj;
  bool result = false;
   
  // true
  BLOCK_FUNC()
  {
    // 0
    ObCellArray cells;
    EXPECT_TRUE(cells.append(cell, out_cell) == OB_SUCCESS);
    // 1
    obj.set_int(22);
    cell.value_ = obj;
    EXPECT_TRUE(cells.append(cell, out_cell) == OB_SUCCESS);
    // 2
    ObString temp_string;
    char * temp = "1234test_test_1234";
    temp_string.assign(temp, strlen(temp));
    obj.set_varchar(temp_string);
    cell.value_ = obj;
    EXPECT_TRUE(cells.append(cell, out_cell) == OB_SUCCESS);
    // 3
    EXPECT_TRUE(cells.append(cell, out_cell) == OB_SUCCESS);

    // only one cell
    EXPECT_TRUE(filter.check(cells, 0, 10, result) == OB_SUCCESS);
    EXPECT_TRUE(result == true);
  }

  // first false
  BLOCK_FUNC()
  {
    ObCellArray cells;
    EXPECT_TRUE(cells.append(cell, out_cell) == OB_SUCCESS);
    // 1
    obj.set_int(23);
    cell.value_ = obj;
    EXPECT_TRUE(cells.append(cell, out_cell) == OB_SUCCESS);
    
    // 2
    ObString temp_string;
    char * temp = "1234test_test_1234";
    temp_string.assign(temp, strlen(temp));
    obj.set_varchar(temp_string);
    cell.value_ = obj;
    EXPECT_TRUE(cells.append(cell, out_cell) == OB_SUCCESS);
    // 3
    EXPECT_TRUE(cells.append(cell, out_cell) == OB_SUCCESS);
    
    EXPECT_TRUE(filter.check(cells, 0, 10, result) == OB_SUCCESS);
    EXPECT_TRUE(result == false);
  }

  // second false
  BLOCK_FUNC()
  {
    ObCellArray cells;
    EXPECT_TRUE(cells.append(cell, out_cell) == OB_SUCCESS);
    // 1
    obj.set_int(22);
    cell.value_ = obj;
    EXPECT_TRUE(cells.append(cell, out_cell) == OB_SUCCESS);
    
    // 2
    ObString temp_string;
    char * temp = "1234tes_est_1234";
    temp_string.assign(temp, strlen(temp));
    obj.set_varchar(temp_string);
    cell.value_ = obj;
    EXPECT_TRUE(cells.append(cell, out_cell) == OB_SUCCESS);
    // 3
    EXPECT_TRUE(cells.append(cell, out_cell) == OB_SUCCESS);

    EXPECT_TRUE(filter.check(cells, 0, 10, result) == OB_SUCCESS);
    EXPECT_TRUE(result == false);
  }

  // last false
  BLOCK_FUNC()
  {
    ObCellArray cells;
    EXPECT_TRUE(cells.append(cell, out_cell) == OB_SUCCESS);
    // 1
    obj.set_int(22);
    cell.value_ = obj;
    EXPECT_TRUE(cells.append(cell, out_cell) == OB_SUCCESS);
    // 2
    ObString temp_string;
    char * temp = "1234test_test_1234";
    temp_string.assign(temp, strlen(temp));
    obj.set_varchar(temp_string);
    cell.value_ = obj;
    EXPECT_TRUE(cells.append(cell, out_cell) == OB_SUCCESS);
    // 3
    temp = "ztest";
    temp_string.assign(temp, strlen(temp));
    obj.set_varchar(temp_string);
    cell.value_ = obj;
    EXPECT_TRUE(cells.append(cell, out_cell) == OB_SUCCESS);

    EXPECT_TRUE(filter.check(cells, 0, 10, result) == OB_SUCCESS);
    EXPECT_TRUE(result == false);
  }

  // last error
  BLOCK_FUNC()
  {
    ObCellArray cells;
    EXPECT_TRUE(cells.append(cell, out_cell) == OB_SUCCESS);
    // 1
    obj.set_int(22);
    cell.value_ = obj;
    EXPECT_TRUE(cells.append(cell, out_cell) == OB_SUCCESS);
    
    // 2
    ObString temp_string;
    char * temp = "1234test_test_1234";
    temp_string.assign(temp, strlen(temp));
    obj.set_varchar(temp_string);
    cell.value_ = obj;
    EXPECT_TRUE(cells.append(cell, out_cell) == OB_SUCCESS);
    
    // 3
    obj.set_int(32);
    cell.value_ = obj;
    EXPECT_TRUE(cells.append(cell, out_cell) == OB_SUCCESS);
    result = true;
    EXPECT_TRUE(filter.check(cells, 0, 1, result) != OB_SUCCESS);
    EXPECT_TRUE(filter.check(cells, 0, 10, result) == OB_SUCCESS);
    EXPECT_TRUE(result == false);
  }
}

TEST_F(TestSimpleFilter, test_index)
{
  ObLogicOperator operate = LIKE;
  char * ptr = "test_test";
  ObString sub_string;
  sub_string.assign(ptr, strlen(ptr));
  ObObj operand;
  operand.set_varchar(sub_string);
  
  ObSimpleFilter filter;
  ObSimpleCond cond1;
  EXPECT_TRUE(cond1.set(2, operate, operand) == OB_SUCCESS);
  EXPECT_TRUE(filter.add_cond(2, operate, operand) == OB_SUCCESS);
  
  operate = GE;
  operand.set_int(23);
  ObSimpleCond cond2;
  EXPECT_TRUE(cond2.set(1, operate, operand) == OB_SUCCESS);
  EXPECT_TRUE(filter.add_cond(1, operate, operand) == OB_SUCCESS);
  
  operate = NE;
  operand.set_varchar(sub_string);
  ObSimpleCond cond3;
  EXPECT_TRUE(cond3.set(3, operate, operand) == OB_SUCCESS);
  EXPECT_TRUE(filter.add_cond(3, operate, operand) == OB_SUCCESS);
  
  const ObSimpleCond * cond = NULL;
  cond = filter[-1];
  EXPECT_TRUE(cond == NULL);
  
  cond = filter[3];
  EXPECT_TRUE(cond == NULL);

  cond = filter[0];
  EXPECT_TRUE(*cond == cond1);
  
  cond = filter[1];
  EXPECT_TRUE(cond != NULL);
  EXPECT_TRUE(*cond == cond2);
  
  cond = filter[2];
  EXPECT_TRUE(cond != NULL);
  EXPECT_TRUE(*cond == cond3);
}

TEST_F(TestSimpleFilter, test_serialize)
{
  ObLogicOperator operate = LIKE;
  char * ptr = "test_test";
  ObString sub_string;
  sub_string.assign(ptr, strlen(ptr));
  ObObj operand;
  operand.set_varchar(sub_string);
  
  ObSimpleFilter filter;
  EXPECT_TRUE(filter.add_cond(2, operate, operand) == OB_SUCCESS);
  operate = GE;
  operand.set_int(23);
  EXPECT_TRUE(filter.add_cond(1, operate, operand) == OB_SUCCESS);
  operate = NE;
  operand.set_varchar(sub_string);
  EXPECT_TRUE(filter.add_cond(3, operate, operand) == OB_SUCCESS);
 
  ObString column_name;
  char name[128] = "";
  for (int64_t i = 0; i < 1; ++i)
  {
    snprintf(name, sizeof(name), "column_name_%ld", i);
    column_name.assign(name, strlen(name));
    EXPECT_TRUE(filter.add_cond(column_name, operate, operand) == OB_SUCCESS);
  }
  char buffer[1024];
  int64_t pos = 0;
  EXPECT_TRUE(filter.serialize(buffer, filter.get_serialize_size() - 1, pos) != OB_SUCCESS);
  pos = 0;
  EXPECT_TRUE(filter.serialize(buffer, filter.get_serialize_size(), pos) == OB_SUCCESS);

  int64_t size = 0;
  ObSimpleFilter filter1;
  EXPECT_TRUE(filter1.deserialize(buffer, pos, size) == OB_SUCCESS);
  EXPECT_TRUE(pos == size);
  EXPECT_TRUE(filter1 == filter);
  
  // add some much string
  for (int64_t i = 10; i < 128; ++i)
  //for (int64_t i = 0; i < 1; ++i)
  {
    operate = NE;
    snprintf(name, sizeof(name), "obj_value_%ld", i);
    sub_string.assign(name, strlen(name));
    operand.set_varchar(sub_string);
    EXPECT_TRUE(filter.add_cond(i, operate, operand) == OB_SUCCESS);
  }
  
  char * temp_buffer = new char[filter.get_serialize_size()];
  EXPECT_TRUE(NULL != temp_buffer);
  
  pos = 0; 
  EXPECT_TRUE(filter.serialize(temp_buffer, filter.get_serialize_size(), pos) == OB_SUCCESS);
  EXPECT_TRUE(pos == filter.get_serialize_size());
  
  size = 0;
  EXPECT_TRUE(filter1.deserialize(temp_buffer, filter.get_serialize_size(), size) == OB_SUCCESS);
  EXPECT_TRUE(pos == size);
  EXPECT_TRUE(filter1 == filter);

  delete []temp_buffer;
  temp_buffer = NULL;
}

