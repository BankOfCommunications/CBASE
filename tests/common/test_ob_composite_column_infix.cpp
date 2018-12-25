/*
 *  (C) 2007-2010 Taobao Inc.
 *  
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License version 2 as
 *  published by the Free Software Foundation.
 *
 *         test_ob_composite_column_infix.cpp is for what ...
 *
 *  Version: $Id: test_ob_composite_column by xiaochu Exp $
 *
 *  Authors:
 *     xiaochu < xiaochu.yh@taobao.com >
 *        - some work details if you want
 */


#include "ob_array_helper.h"
#include "gtest/gtest.h"
#include "common/ob_composite_column.h"



#define MAX_SYMBOL_COUNT 100

using namespace oceanbase;
using namespace common;

TEST(Test_ob_infix, infix_test)
{
  int i = 0;
  //const char *ptr = "column_a > column_b + 10 AND `my_time` > #1232-123-12# OR my_string = 'hi all' AND `hi` = b'32fda'";
  //const char *ptr = "`hi` = b'4141'";
  char *ptr = (char*)"column_a > column_b + 10 AND `my_time` < #1232-123-12# OR my_string = 'hi all'   ";
  //const char *ptr = "('a' OR 'b') AND 'c' AND 'd' OR ('e' OR 'f') ";
  //const char *ptr = "('a' OR 'b') AND ) ";
  //const char *ptr = "('a' OR 'b+'+')";
  //const char *ptr = "('a' OR 'b+')";
  ObObj sym_list[MAX_SYMBOL_COUNT];  
  ObArrayHelper<ObObj> symbols;
  symbols.init(MAX_SYMBOL_COUNT, sym_list);
  ObString str;
  str.assign(ptr, static_cast<int32_t>(strlen(ptr)));
  ObExpressionParser p;
  p.parse(str, symbols);
  
  // 调试：输出转换结果
  for(i = 0; i < symbols.get_array_index(); i++)
  {
    ObObj * obj = symbols.at(i);
    TBSYS_LOG(INFO, "==========================================");
    obj->dump();
  }

  TBSYS_LOG(INFO, "output size=%ld", symbols.get_array_index());
}



TEST(Test_ob_infix, infix_test2)
{
  int i = 0;
  //const char *ptr = "column_a > column_b + 10 AND `my_time` > #1232-123-12# OR my_string = 'hi all' AND `hi` = b'32fda'";
  //const char *ptr = "`hi` = b'4141'";
  char *ptr = (char*)"column_a > column_b + 10 AND `my_time` < #1232-12-12# OR my_string = 'hi all'   ";
  //const char *ptr = "('a' OR 'b') AND 'c' AND 'd' OR ('e' OR 'f') ";
  //const char *ptr = "('a' OR 'b') AND ) ";
  //const char *ptr = "('a' OR 'b+'+')";
  //const char *ptr = "('a' OR 'b+')";
  ObObj sym_list[MAX_SYMBOL_COUNT];  
  ObArrayHelper<ObObj> symbols;
  symbols.init(MAX_SYMBOL_COUNT, sym_list);

  ObString str;
  str.assign(ptr, static_cast<int32_t>(strlen(ptr)));
  ObExpressionParser p;
  p.parse(str, symbols);
  
  // 调试：输出转换结果
  for(i = 0; i < symbols.get_array_index(); i++)
  {
    ObObj * obj = symbols.at(i);
    TBSYS_LOG(INFO, "==========================================");
    obj->dump();
  }

  TBSYS_LOG(INFO, "output size=%ld", symbols.get_array_index());
}

int main(int argc, char **argv)
{
  srandom(static_cast<uint32_t>(time(NULL)));
  ob_init_memory_pool(64*1024);

  //TBSYS_LOGGER.setLogLevel("INFO");
  //ob_init_memory_pool();
  testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
