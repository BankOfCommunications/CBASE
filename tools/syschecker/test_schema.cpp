/**
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * Version: $Id$
 * First Create_time: 2011-8-5
 *
 * Authors:
 *   rongxuan <rongxuan.lc@taobao.com>
 *     - some work details if you want
 */
 
#include "gtest/gtest.h"
#include "ob_syschecker.h"
#include "common/ob_schema.h"
#include "common/ob_array.h"
#include "common/ob_schema_service.h"
#include "common/ob_schema_helper.h"
using namespace oceanbase::common;
using namespace oceanbase::syschecker;
class TestSysChecker : public ObSyschecker
{
  public:
    TestSysChecker(){}
    ~TestSysChecker(){}
    ObSyscheckerSchema* get_syschecker_schema()
    {
      return ObSyschecker::get_syschecker_schema(); 
    }
    inline int translate_user_schema(const ObSchemaManagerV2& ori_schema_mgr,
        ObSchemaManagerV2& user_schema_mgr)
    {
      return ObSyschecker::translate_user_schema(ori_schema_mgr, user_schema_mgr);
    }
};
class TestSchema : public ::testing::Test
{
  public:
    virtual void SetUp()
    {
    }
    virtual void TearDown()
    {
    }
};
TEST(TestSchema, check_schema)                                                                      
{ 
  tbsys::CConfig c1;
  ObSchemaManagerV2 *schema_manager = new ObSchemaManagerV2();
  ASSERT_EQ(true, schema_manager->parse_from_file("collect_rowkey.ini", c1));                         
  //TestSysChecker *sys_check = new TestSysChecker();
  //ObSchemaManagerV2& syschecker_schema_manager = sys_check->get_syschecker_schema()->get_schema_manager();  
  //sys_check->translate_user_schema(*schema_manager, syschecker_schema_manager);
  //ASSERT_EQ(OB_SUCCESS, sys_check->get_syschecker_schema()->init());

  ObArray<TableSchema> table_schema_array;
  ObSchemaHelper::transfer_manager_to_table_schema(*schema_manager, table_schema_array);
  ObSchemaManagerV2 *schema = new ObSchemaManagerV2();
  ASSERT_EQ(OB_SUCCESS, schema->add_new_table_schema(table_schema_array));
  TestSysChecker *sys = new TestSysChecker();
  ObSchemaManagerV2& syschecker_schema = sys->get_syschecker_schema()->get_schema_manager();
  sys->translate_user_schema(*schema, syschecker_schema);
  ASSERT_EQ(OB_SUCCESS, sys->get_syschecker_schema()->init());
}

int main(int argc, char** argv)
{
  ob_init_memory_pool();
  testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
