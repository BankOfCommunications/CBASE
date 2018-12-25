/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_result_set_test.cpp
 *
 * Authors:
 *   Guibin Du <tianguan.dgb@taobao.com>
 *
 */
#include "sql/ob_result_set.h"
#include "sql/ob_schema_checker.h"
#include "sql/parse_node.h"
#include "sql/build_plan.h"
#include "sql/ob_multi_logic_plan.h"
#include "sql/ob_result_set.h"
#include "sql/ob_logical_plan.h"
#include "common/ob_string_buf.h"
#include <gtest/gtest.h>

using namespace oceanbase::sql;
using namespace oceanbase::common;

TEST(ObResultSetTest, logical_plan_return)
{
  int ret = OB_SUCCESS;
  ObStringBuf malloc_pool;
  const char *sql_str = ""
    "select qty as quantity, user_name, qty / 100, id + 1000 as new_id, new_id as n_id "
    "from (select * from order_list where order_time > date '2012-08-01') o, user_t "
    "where o.user_id = user_t.user_id ";

  ParseResult result;
  result.malloc_pool_ = &malloc_pool;
  ret = parse_init(&result);
  ASSERT_EQ(ret, 0);

  parse_sql(&result, sql_str, strlen(sql_str));
  ASSERT_NE(result.result_tree_, (ParseNode*)NULL);

  /*
  ObSchemaChecker schema_checker;
  ResultPlan resultPlan;
  resultPlan.name_pool_ = &malloc_pool;
  resultPlan.schema_checker_ = &schema_checker;
  resultPlan.plan_tree_ = NULL;
  TBSYS_LOG(INFO, "AAA");
  ret = resolve(&resultPlan, result.result_tree_);
  TBSYS_LOG(INFO, "AAA");
  ASSERT_EQ(ret, OB_ERR_TABLE_UNKNOWN);
  ASSERT_EQ(ret, OB_SUCCESS);

  ObMultiLogicPlan* multi_plan = static_cast<ObMultiLogicPlan*>(resultPlan.plan_tree_);
  ObLogicalPlan *logical_plan = multi_plan->at(0);
  ObResultSet result_set;
  StackAllocator allocator;
  ret = logical_plan->fill_result_set(result_set, NULL, allocator);
  ASSERT_EQ(ret, OB_SUCCESS);
  const ObArray<ObResultSet::Field>& fields = result_set.get_field_columns();

  char name[OB_MAX_COLUMN_NAME_LENGTH];

  memcpy(name, fields.at(0).cname_.ptr(), fields.at(0).cname_.length());
  name[fields.at(0).cname_.length()] = '\0';
  EXPECT_STREQ(name, "quantity");
  memcpy(name, fields.at(0).org_cname_.ptr(), fields.at(0).org_cname_.length());
  name[fields.at(0).org_cname_.length()] = '\0';
  EXPECT_STREQ(name, "qty");
  memcpy(name, fields.at(0).tname_.ptr(), fields.at(0).tname_.length());
  name[fields.at(0).tname_.length()] = '\0';
  EXPECT_STREQ(name, "o");
  memcpy(name, fields.at(0).org_tname_.ptr(), fields.at(0).org_tname_.length());
  name[fields.at(0).org_tname_.length()] = '\0';
  EXPECT_STREQ(name, "o");
  EXPECT_EQ(fields.at(0).type_.get_type(), ObIntType);

  memcpy(name, fields.at(1).cname_.ptr(), fields.at(1).cname_.length());
  name[fields.at(1).cname_.length()] = '\0';
  EXPECT_STREQ(name, "user_name");
  memcpy(name, fields.at(1).org_cname_.ptr(), fields.at(1).org_cname_.length());
  name[fields.at(1).org_cname_.length()] = '\0';
  EXPECT_STREQ(name, "user_name");
  memcpy(name, fields.at(1).tname_.ptr(), fields.at(1).tname_.length());
  name[fields.at(1).tname_.length()] = '\0';
  EXPECT_STREQ(name, "user");
  memcpy(name, fields.at(1).org_tname_.ptr(), fields.at(1).org_tname_.length());
  name[fields.at(1).org_tname_.length()] = '\0';
  EXPECT_STREQ(name, "user");
  EXPECT_EQ(fields.at(1).type_.get_type(), ObVarcharType);

  memcpy(name, fields.at(2).cname_.ptr(), fields.at(2).cname_.length());
  name[fields.at(2).cname_.length()] = '\0';
  EXPECT_STREQ(name, "qty / 100");
  memcpy(name, fields.at(2).org_cname_.ptr(), fields.at(2).org_cname_.length());
  name[fields.at(2).org_cname_.length()] = '\0';
  EXPECT_STREQ(name, "");
  memcpy(name, fields.at(2).tname_.ptr(), fields.at(2).tname_.length());
  name[fields.at(2).tname_.length()] = '\0';
  EXPECT_STREQ(name, "");
  memcpy(name, fields.at(2).org_tname_.ptr(), fields.at(2).org_tname_.length());
  name[fields.at(2).org_tname_.length()] = '\0';
  EXPECT_STREQ(name, "");
  EXPECT_EQ(fields.at(2).type_.get_type(), ObDecimalType);

  memcpy(name, fields.at(3).cname_.ptr(), fields.at(3).cname_.length());
  name[fields.at(3).cname_.length()] = '\0';
  EXPECT_STREQ(name, "new_id");
  memcpy(name, fields.at(3).org_cname_.ptr(), fields.at(3).org_cname_.length());
  name[fields.at(3).org_cname_.length()] = '\0';
  EXPECT_STREQ(name, "");
  memcpy(name, fields.at(3).tname_.ptr(), fields.at(3).tname_.length());
  name[fields.at(3).tname_.length()] = '\0';
  EXPECT_STREQ(name, "");
  memcpy(name, fields.at(3).org_tname_.ptr(), fields.at(3).org_tname_.length());
  name[fields.at(3).org_tname_.length()] = '\0';
  EXPECT_STREQ(name, "");
  EXPECT_EQ(fields.at(3).type_.get_type(), ObIntType);

  memcpy(name, fields.at(4).cname_.ptr(), fields.at(4).cname_.length());
  name[fields.at(4).cname_.length()] = '\0';
  EXPECT_STREQ(name, "n_id");
  memcpy(name, fields.at(4).org_cname_.ptr(), fields.at(4).org_cname_.length());
  name[fields.at(4).org_cname_.length()] = '\0';
  EXPECT_STREQ(name, "");
  memcpy(name, fields.at(4).tname_.ptr(), fields.at(4).tname_.length());
  name[fields.at(4).tname_.length()] = '\0';
  EXPECT_STREQ(name, "");
  memcpy(name, fields.at(4).org_tname_.ptr(), fields.at(4).org_tname_.length());
  name[fields.at(4).org_tname_.length()] = '\0';
  EXPECT_STREQ(name, "");
  EXPECT_EQ(fields.at(4).type_.get_type(), ObIntType);

  destroy_plan(&resultPlan);
  if (result.result_tree_)
  {
    destroy_tree(result.result_tree_);
    result.result_tree_ = NULL;
  }
  */
}

int main(int argc, char **argv)
{
  TBSYS_LOGGER.setLogLevel("DEBUG");
  ob_init_memory_pool();
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
