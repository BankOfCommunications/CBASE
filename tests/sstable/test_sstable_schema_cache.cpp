/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * test_sstable_schema.cpp for test sstable schema
 *
 * Authors:
 *   huating <huating.zmq@taobao.com>
 *
 */

#include <gtest/gtest.h>
#include <tblog.h>
#include "sstable/ob_sstable_schema_cache.h"

using namespace oceanbase::common;
using namespace oceanbase::sstable;

namespace oceanbase
{
  namespace tests
  {
    namespace sstable
    {
      static const int64_t MAX_SCHEMA_VER_COUNT = 1024;

      class TestObSSTableSchemaCache: public ::testing::Test
      {
      public:
        virtual void SetUp()
        {

        }

        virtual void TearDown()
        {

        }

        ObSSTableSchema* create_sstable_schema()
        {
          char* schema_buf = static_cast<char*>(ob_malloc(sizeof(ObSSTableSchema), ObModIds::TEST));
          EXPECT_TRUE(NULL != schema_buf);

          ObSSTableSchema* schema = new (schema_buf) ObSSTableSchema();
          EXPECT_TRUE(NULL != schema);

          return schema;
        }
      };

      TEST_F(TestObSSTableSchemaCache, test_null_schema_cache)
      {
        ObSSTableSchemaCache schema_cache;
        ObSSTableSchema* schema = NULL;
        int ret = OB_SUCCESS;

        schema = schema_cache.get_schema(0, -1);
        EXPECT_TRUE(NULL == schema);
        schema = schema_cache.get_schema(OB_INVALID_ID, -1);
        EXPECT_TRUE(NULL == schema);
        schema = schema_cache.get_schema(1, -1);
        EXPECT_TRUE(NULL == schema);
        schema = schema_cache.get_schema(0, 0);
        EXPECT_TRUE(NULL == schema);
        schema = schema_cache.get_schema(OB_INVALID_ID, 0);
        EXPECT_TRUE(NULL == schema);
        schema = schema_cache.get_schema(1, 0);
        EXPECT_TRUE(NULL == schema);
        schema = schema_cache.get_schema(0, 1);
        EXPECT_TRUE(NULL == schema);
        schema = schema_cache.get_schema(OB_INVALID_ID, 1);
        EXPECT_TRUE(NULL == schema);
        schema = schema_cache.get_schema(1, 1);
        EXPECT_TRUE(NULL == schema);

        ret = schema_cache.revert_schema(0, -1);
        EXPECT_EQ(OB_ERROR, ret);
        ret = schema_cache.revert_schema(OB_INVALID_ID, -1);
        EXPECT_EQ(OB_ERROR, ret);
        ret = schema_cache.revert_schema(1, -1);
        EXPECT_EQ(OB_ERROR, ret);
        ret = schema_cache.revert_schema(0, 0);
        EXPECT_EQ(OB_ERROR, ret);
        ret = schema_cache.revert_schema(OB_INVALID_ID, 0);
        EXPECT_EQ(OB_ERROR, ret);
        ret = schema_cache.revert_schema(1, 0);
        EXPECT_EQ(OB_ERROR, ret);
        ret = schema_cache.revert_schema(0, 1);
        EXPECT_EQ(OB_ERROR, ret);
        ret = schema_cache.revert_schema(OB_INVALID_ID, 1);
        EXPECT_EQ(OB_ERROR, ret);
        ret = schema_cache.revert_schema(1, 1);
        EXPECT_EQ(OB_ERROR, ret);

        ret = schema_cache.add_schema(NULL, 0, -1);
        EXPECT_EQ(OB_ERROR, ret);
        ret = schema_cache.add_schema(NULL, OB_INVALID_ID, -1);
        EXPECT_EQ(OB_ERROR, ret);
        ret = schema_cache.add_schema(NULL, 1, -1);
        EXPECT_EQ(OB_ERROR, ret);
        ret = schema_cache.add_schema(NULL, 0, 0);
        EXPECT_EQ(OB_ERROR, ret);
        ret = schema_cache.add_schema(NULL, OB_INVALID_ID, 0);
        EXPECT_EQ(OB_ERROR, ret);
        ret = schema_cache.add_schema(NULL, 1, 0);
        EXPECT_EQ(OB_ERROR, ret);
        ret = schema_cache.add_schema(NULL, 0, 1);
        EXPECT_EQ(OB_ERROR, ret);
        ret = schema_cache.add_schema(NULL, OB_INVALID_ID, 1);
        EXPECT_EQ(OB_ERROR, ret);
        ret = schema_cache.add_schema(NULL, 1, 1);
        EXPECT_EQ(OB_ERROR, ret);

        ret = schema_cache.clear();
        EXPECT_EQ(OB_SUCCESS, ret);
        ret = schema_cache.destroy();
        EXPECT_EQ(OB_SUCCESS, ret);
        ret = schema_cache.destroy();
        EXPECT_EQ(OB_SUCCESS, ret);
      }

      TEST_F(TestObSSTableSchemaCache, test_add_one_schema)
      {
        ObSSTableSchemaCache schema_cache;
        ObSSTableSchema* schema = create_sstable_schema();
        uint64_t table_id = 1;
        int64_t version = 0;
        int ret = OB_SUCCESS;

        ret = schema_cache.add_schema(schema, table_id, version);
        EXPECT_EQ(OB_SUCCESS, ret);

        EXPECT_TRUE(schema == schema_cache.get_schema(table_id, version));

        ret = schema_cache.revert_schema(table_id, version);
        EXPECT_EQ(OB_SUCCESS, ret);

        ret = schema_cache.revert_schema(table_id, version);
        EXPECT_EQ(OB_SUCCESS, ret);
      }

      TEST_F(TestObSSTableSchemaCache, test_add_two_schemas)
      {
        ObSSTableSchemaCache schema_cache;
        ObSSTableSchema* schema1 = create_sstable_schema();
        ObSSTableSchema* schema2 = create_sstable_schema();
        uint64_t table_id1 = 1;
        uint64_t table_id2 = 2;
        int64_t version1 = 0;
        int64_t version2 = 1;
        int ret = OB_SUCCESS;

        ret = schema_cache.add_schema(schema1, table_id1, version1);
        EXPECT_EQ(OB_SUCCESS, ret);
        EXPECT_TRUE(schema1 == schema_cache.get_schema(table_id1, version1));

        ret = schema_cache.add_schema(schema2, table_id2, version2);
        EXPECT_EQ(OB_SUCCESS, ret);
        EXPECT_TRUE(schema2 == schema_cache.get_schema(table_id2, version2));

        ret = schema_cache.revert_schema(table_id1, version1);
        EXPECT_EQ(OB_SUCCESS, ret);
        ret = schema_cache.revert_schema(table_id1, version1);
        EXPECT_EQ(OB_SUCCESS, ret);

        ret = schema_cache.revert_schema(table_id2, version2);
        EXPECT_EQ(OB_SUCCESS, ret);
        ret = schema_cache.revert_schema(table_id2, version2);
        EXPECT_EQ(OB_SUCCESS, ret);
      }

      TEST_F(TestObSSTableSchemaCache, test_add_max_schemas)
      {
        ObSSTableSchemaCache schema_cache;
        int64_t schema_cnt = MAX_SCHEMA_VER_COUNT + 1;
        ObSSTableSchema* schema[schema_cnt];
        uint64_t table_id[schema_cnt];
        int64_t version[schema_cnt];
        int ret = OB_SUCCESS;

        for (int64_t i = 0; i < schema_cnt; ++i)
        {
          table_id[i] = i + 10;
          version[i] = schema_cnt - i;
          schema[i] = create_sstable_schema();
          ret = schema_cache.add_schema(schema[i], table_id[i], version[i]);
          EXPECT_EQ(OB_SUCCESS, ret);
        }

        for (int64_t i = 0; i < schema_cnt; ++i)
        {
          EXPECT_TRUE(schema[i] == schema_cache.get_schema(table_id[i], version[i]));
        }

        for (int64_t i = 0; i < schema_cnt; ++i)
        {
          ret = schema_cache.revert_schema(table_id[i], version[i]);
          EXPECT_EQ(OB_SUCCESS, ret);
          ret = schema_cache.revert_schema(table_id[i], version[i]);
          EXPECT_EQ(OB_SUCCESS, ret);
        }

        ret = schema_cache.destroy();
        EXPECT_EQ(OB_SUCCESS, ret);
      }

      TEST_F(TestObSSTableSchemaCache, test_add_destroy)
      {
        ObSSTableSchemaCache schema_cache;
        ObSSTableSchema* schema1 = create_sstable_schema();
        ObSSTableSchema* schema2 = create_sstable_schema();
        uint64_t table_id1 = 1;
        uint64_t table_id2 = 2;
        int64_t version1 = 0;
        int64_t version2 = 1;
        int ret = OB_SUCCESS;

        ret = schema_cache.add_schema(schema1, table_id1, version1);
        EXPECT_EQ(OB_SUCCESS, ret);
        EXPECT_TRUE(schema1 == schema_cache.get_schema(table_id1, version1));

        ret = schema_cache.add_schema(schema2, table_id2, version2);
        EXPECT_EQ(OB_SUCCESS, ret);
        EXPECT_TRUE(schema2 == schema_cache.get_schema(table_id2, version2));

        ret = schema_cache.destroy();
        EXPECT_EQ(OB_ERROR, ret);
        EXPECT_TRUE(NULL == schema_cache.get_schema(table_id1, version1));
        EXPECT_TRUE(NULL == schema_cache.get_schema(table_id2, version2));

        ret = schema_cache.revert_schema(table_id1, version1);
        EXPECT_EQ(OB_ERROR, ret);
        ret = schema_cache.revert_schema(table_id2, version2);
        EXPECT_EQ(OB_ERROR, ret);
      }
    }//end namespace sstable
  }//end namespace tests
}//end namespace oceanbase

int main(int argc, char** argv)
{
  ob_init_memory_pool();
  TBSYS_LOGGER.setLogLevel("ERROR");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
