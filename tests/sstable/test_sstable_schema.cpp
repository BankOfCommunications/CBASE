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
#include "sstable/ob_sstable_schema.h"
#include "ob_sstable_schemaV1.h"
#include "common/ob_object.h"
#include "file_directory_utils.h"
#include "file_utils.h"

using namespace oceanbase::common;
using namespace oceanbase::sstable;

namespace oceanbase
{
  namespace tests
  {
    namespace sstable
    {
      static const int64_t DEFAULT_COLUMN_DEF_SIZE = 1280; 
      static const char * schema_file_name = "schema_for_test";
      static const int64_t ROWKEY_COLUMN_GROUP = 65535;
      class TestObSSTableSchema: public ::testing::Test
      {
      public:
        virtual void SetUp()
        {

        }

        virtual void TearDown()
        {

        }
      };

      TEST_F(TestObSSTableSchema, test_init)
      {
        ObSSTableSchema schema;
        const ObSSTableSchemaColumnDef *column_def = NULL;

        EXPECT_EQ(0, schema.get_column_count());

        for (int i = 0; i < oceanbase::common::OB_MAX_COLUMN_NUMBER; ++i)
        {
          column_def = schema.get_column_def(i);
          EXPECT_TRUE(NULL == column_def);
        }
      }

      TEST_F(TestObSSTableSchema, test_add_columns)
      {
        ObSSTableSchema schema;
        ObSSTableSchemaColumnDef column_def;
        const ObSSTableSchemaColumnDef *column = NULL;
        int ret = OB_SUCCESS;

        column_def.reserved_ = 0;
        column_def.rowkey_seq_ = 0;
        column_def.column_group_id_ = 2;
        column_def.column_name_id_ = 2;
        column_def.column_value_type_ = ObDoubleType;
        column_def.table_id_ = 1000;
        ret = schema.add_column_def(column_def);
        EXPECT_TRUE(OB_SUCCESS == ret);

        column_def.column_group_id_ = 2;
        column_def.column_name_id_ = 3;
        column_def.column_value_type_ = ObIntType;
        column_def.table_id_ = 1000;
        ret = schema.add_column_def(column_def);
        EXPECT_TRUE(OB_SUCCESS == ret);

        column_def.column_group_id_ = 3;
        column_def.column_name_id_ = 4;
        column_def.column_value_type_ = ObVarcharType;
        column_def.table_id_ = 1000;
        ret = schema.add_column_def(column_def);
        EXPECT_TRUE(OB_SUCCESS == ret);

        EXPECT_EQ(3, schema.get_column_count());

        column = schema.get_column_def(0);
        EXPECT_TRUE(NULL != column);
        EXPECT_EQ(2, (int32_t)column->column_group_id_);
        EXPECT_EQ(2, (int32_t)column->column_name_id_);
        EXPECT_EQ(ObDoubleType, column->column_value_type_);
        EXPECT_EQ(1000, (int32_t)column->table_id_);

        column = schema.get_column_def(1);
        EXPECT_EQ(2, (int32_t)column->column_group_id_);
        EXPECT_EQ(3, (int32_t)column->column_name_id_);
        EXPECT_EQ(ObIntType, column->column_value_type_);
        EXPECT_EQ(1000, (int32_t)column->table_id_);

        column = schema.get_column_def(2);
        EXPECT_EQ(3, (int32_t)column->column_group_id_);
        EXPECT_EQ(4, (int32_t)column->column_name_id_);
        EXPECT_EQ(ObVarcharType, column->column_value_type_);
        EXPECT_EQ(1000, (int32_t)column->table_id_);
      }

      TEST_F(TestObSSTableSchema, test_add_columns_one_table_with_rowkey)
      {
        ObSSTableSchema schema;
        ObSSTableSchemaColumnDef column_def;
        const ObSSTableSchemaColumnDef *column = NULL;
        int ret = OB_SUCCESS;

        const uint64_t TABLE_ID = 1000;

        // add column group 2; rowkey(3,2), column 4.
        column_def.reserved_ = 0;
        column_def.column_group_id_ = 2;
        column_def.column_name_id_ = 2;
        column_def.rowkey_seq_ = 1;
        column_def.column_value_type_ = ObDoubleType;
        column_def.table_id_ = TABLE_ID;
        ret = schema.add_column_def(column_def);
        EXPECT_TRUE(OB_SUCCESS == ret);

        column_def.column_group_id_ = 2;
        column_def.column_name_id_ = 3;
        column_def.column_value_type_ = ObIntType;
        column_def.rowkey_seq_ = 2;
        column_def.table_id_ = 1000;
        ret = schema.add_column_def(column_def);
        EXPECT_TRUE(OB_SUCCESS == ret);

        column_def.column_group_id_ = 2;
        column_def.column_name_id_ = 4;
        column_def.column_value_type_ = ObVarcharType;
        column_def.table_id_ = TABLE_ID;
        column_def.rowkey_seq_ = 0;
        ret = schema.add_column_def(column_def);
        EXPECT_TRUE(OB_SUCCESS == ret);

        // add column_group 3; rowkey(3,2), column 5;
        column_def.column_group_id_ = 3;
        column_def.column_name_id_ = 2;
        column_def.rowkey_seq_ = 1;
        column_def.column_value_type_ = ObDoubleType;
        column_def.table_id_ = TABLE_ID;
        ret = schema.add_column_def(column_def);
        EXPECT_TRUE(OB_ERROR == ret);

        column_def.column_group_id_ = 3;
        column_def.column_name_id_ = 3;
        column_def.column_value_type_ = ObIntType;
        column_def.rowkey_seq_ = 0;
        column_def.table_id_ = TABLE_ID;
        ret = schema.add_column_def(column_def);
        EXPECT_TRUE(OB_ERROR== ret);

        column_def.column_group_id_ = 3;
        column_def.column_name_id_ = 5;
        column_def.column_value_type_ = ObDateTimeType;
        column_def.rowkey_seq_ = 0;
        column_def.table_id_ = TABLE_ID;
        ret = schema.add_column_def(column_def);
        EXPECT_TRUE(OB_SUCCESS == ret);

        EXPECT_EQ(4, schema.get_column_count());

        column = schema.get_column_def(0);
        EXPECT_TRUE(NULL != column);
        EXPECT_EQ(65535, (int32_t)column->column_group_id_);
        EXPECT_EQ(2, (int32_t)column->column_name_id_);
        EXPECT_EQ(ObDoubleType, column->column_value_type_);
        EXPECT_EQ(TABLE_ID, (uint64_t)column->table_id_);

        column = schema.get_column_def(1);
        EXPECT_EQ(65535, (int32_t)column->column_group_id_);
        EXPECT_EQ(3, (int32_t)column->column_name_id_);
        EXPECT_EQ(ObIntType, column->column_value_type_);
        EXPECT_EQ(TABLE_ID, (uint64_t)column->table_id_);

        column = schema.get_column_def(2);
        EXPECT_EQ(2, (int32_t)column->column_group_id_);
        EXPECT_EQ(4, (int32_t)column->column_name_id_);
        EXPECT_EQ(ObVarcharType, column->column_value_type_);
        EXPECT_EQ(TABLE_ID, (uint64_t)column->table_id_);

        column = schema.get_rowkey_column(TABLE_ID, 1);
        EXPECT_TRUE(NULL != column);
        EXPECT_EQ(2, (int32_t)column->column_name_id_);
        EXPECT_EQ(65535, (int32_t)column->column_group_id_);

        column = schema.get_rowkey_column(TABLE_ID, 2);
        EXPECT_TRUE(NULL != column);
        EXPECT_EQ(3, (int32_t)column->column_name_id_);
        EXPECT_EQ(65535, (int32_t)column->column_group_id_);

        int64_t offset = schema.find_offset_column_group_schema(TABLE_ID, 2, 4);
        EXPECT_EQ(offset, 0);
        offset = schema.find_offset_column_group_schema(TABLE_ID, 3, 5);
        EXPECT_EQ(offset, 0);
        offset = schema.find_offset_column_group_schema(TABLE_ID, 3, 6);
        EXPECT_EQ(offset, -1);
      }

      TEST_F(TestObSSTableSchema, test_add_columns_continuous_rowkey)
      {
        ObSSTableSchema schema;
        ObSSTableSchemaColumnDef column_def;
        const ObSSTableSchemaColumnDef *column = NULL;
        int ret = OB_SUCCESS;

        const uint64_t TABLE_ID = 1000;
        const int CG_NUM = 4;
        const int COL_NUM = 10;
        const int RK_COL_NUM = 3;
        for (int ci = 1; ci < 1 + RK_COL_NUM; ++ci)
        {
          column_def.reserved_ = 0;
          column_def.column_group_id_ = 0;
          column_def.column_name_id_ = static_cast<uint16_t>(ci);
          column_def.rowkey_seq_ = static_cast<uint16_t>(ci);
          column_def.column_value_type_ = ObDoubleType;
          column_def.table_id_ = TABLE_ID;
          ret = schema.add_column_def(column_def);
          EXPECT_TRUE(OB_SUCCESS == ret);
        }

        for (int cg = 10; cg < 10 + CG_NUM; ++cg)
        {
          for (int ci = RK_COL_NUM + 1; ci < 1 + COL_NUM; ++ci)
          {
            column_def.reserved_ = 0;
            column_def.column_group_id_ = static_cast<uint16_t>(cg);
            column_def.column_name_id_ = static_cast<uint16_t>(ci);
            if (10 == cg && ci <= 3)
            {
              column_def.rowkey_seq_ = static_cast<uint16_t>(ci);
            }
            else 
            {
              column_def.rowkey_seq_ = 0;
            }
            column_def.column_value_type_ = ObDoubleType;
            column_def.table_id_ = TABLE_ID;
            ret = schema.add_column_def(column_def);
            EXPECT_TRUE(OB_SUCCESS == ret);
          }
        }

        EXPECT_EQ(CG_NUM * (COL_NUM-RK_COL_NUM) + RK_COL_NUM, schema.get_column_count());

        for (int seq = 1; seq <= 3; ++seq)
        {
          column = schema.get_rowkey_column(TABLE_ID, seq);
          EXPECT_TRUE(NULL != column);
          EXPECT_EQ(seq, (int32_t)column->column_name_id_);
          EXPECT_EQ(ROWKEY_COLUMN_GROUP, (int32_t)column->column_group_id_);
        }

      }

      TEST_F(TestObSSTableSchema, test_add_columns_multi_table_rowkey)
      {
        ObSSTableSchema schema;
        ObSSTableSchemaColumnDef column_def;
        const ObSSTableSchemaColumnDef *column = NULL;
        int ret = OB_SUCCESS;

        const uint64_t TABLE_ID = 1000;
        const int CG_NUM = 4;
        const int COL_NUM = 10;
        const int RK_COL_NUM = 3;

        for (int ci = 1; ci < 1 + RK_COL_NUM; ++ci)
        {
          column_def.reserved_ = 0;
          column_def.column_group_id_ = 0;
          column_def.column_name_id_ = static_cast<uint16_t>(ci);
          column_def.rowkey_seq_ = static_cast<uint16_t>(ci);
          column_def.column_value_type_ = ObDoubleType;
          column_def.table_id_ = TABLE_ID;
          ret = schema.add_column_def(column_def);
          EXPECT_TRUE(OB_SUCCESS == ret);
        }

        for (int cg = 10; cg < 10 + CG_NUM; ++cg)
        {
          for (int ci = 1+RK_COL_NUM; ci < 1 + COL_NUM; ++ci)
          {
            column_def.reserved_ = 0;
            column_def.column_group_id_ = static_cast<uint16_t>(cg);
            column_def.column_name_id_ = static_cast<uint16_t>(ci);
            if (10 == cg &&  ci <= 3)
            {
              column_def.rowkey_seq_ = static_cast<uint16_t>(ci);
            }
            else
            {
              column_def.rowkey_seq_ = 0;
            }
            column_def.column_value_type_ = ObDoubleType;
            column_def.table_id_ = TABLE_ID;
            ret = schema.add_column_def(column_def);
            EXPECT_TRUE(OB_SUCCESS == ret);
          }
        }

        for (int ci = 1; ci < 1 + RK_COL_NUM; ++ci)
        {
          column_def.reserved_ = 0;
          column_def.column_group_id_ = 0;
          column_def.column_name_id_ = static_cast<uint16_t>(ci);
          column_def.rowkey_seq_ = static_cast<uint16_t>(ci);
          column_def.column_value_type_ = ObDoubleType;
          column_def.table_id_ = TABLE_ID + 1;
          ret = schema.add_column_def(column_def);
          EXPECT_TRUE(OB_SUCCESS == ret);
        }

        for (int cg = 10; cg < 10 + CG_NUM; ++cg)
        {
          for (int ci = 1 + RK_COL_NUM; ci < 1 + COL_NUM; ++ci)
          {
            column_def.reserved_ = 0;
            column_def.column_group_id_ = static_cast<uint16_t>(cg);
            column_def.column_name_id_ = static_cast<uint16_t>(ci);
            if (10 == cg &&  ci <= 3)
            {
              column_def.rowkey_seq_ = static_cast<uint16_t>(ci);
            }
            else
            {
              column_def.rowkey_seq_ = 0;
            }
            column_def.column_value_type_ = ObDoubleType;
            column_def.table_id_ = TABLE_ID+1;
            ret = schema.add_column_def(column_def);
            EXPECT_TRUE(OB_SUCCESS == ret);
          }
        }

        EXPECT_EQ(2 * (CG_NUM * (COL_NUM-RK_COL_NUM) + RK_COL_NUM), schema.get_column_count());

        for (int seq = 1; seq <= 3; ++seq)
        {
          column = schema.get_rowkey_column(TABLE_ID, seq);
          EXPECT_TRUE(NULL != column);
          EXPECT_EQ(seq, (int32_t)column->column_name_id_);
          EXPECT_EQ(ROWKEY_COLUMN_GROUP, (int32_t)column->column_group_id_);
          column = schema.get_rowkey_column(TABLE_ID+1, seq);
          EXPECT_TRUE(NULL != column);
          EXPECT_EQ(seq, (int32_t)column->column_name_id_);
          EXPECT_EQ(ROWKEY_COLUMN_GROUP, (int32_t)column->column_group_id_);
        }

      }
      
      TEST_F(TestObSSTableSchema, test_add_max_columns_one_table)
      {
        ObSSTableSchema schema;
        ObSSTableSchemaColumnDef column_def;
        const ObSSTableSchemaColumnDef *column = NULL;
        int ret = OB_SUCCESS;
        for (int i = 0; i < oceanbase::common::OB_MAX_COLUMN_NUMBER; ++i)
        {
          column_def.column_group_id_ = 2;
          column_def.column_name_id_ = static_cast<uint16_t>(i + 2);
          column_def.table_id_ = 1000;
          column_def.rowkey_seq_ = 0;
          column_def.column_value_type_ = ObNullType;
          ret = schema.add_column_def(column_def);
          EXPECT_EQ(OB_SUCCESS, ret);
        }

        EXPECT_EQ(oceanbase::common::OB_MAX_COLUMN_NUMBER, schema.get_column_count());

        for (int i = 0; i < oceanbase::common::OB_MAX_COLUMN_NUMBER; ++i)
        {
          column = schema.get_column_def(i);
          EXPECT_EQ(i + 2, (int32_t)column->column_name_id_);
          EXPECT_EQ(2, (int32_t)column->column_group_id_);
          EXPECT_EQ(1000, (int32_t)column->table_id_);
          EXPECT_EQ(ObNullType, column->column_value_type_);
        }
      }

      TEST_F(TestObSSTableSchema, test_add_max_columns)
      {
        ObSSTableSchema schema;
        ObSSTableSchemaColumnDef column_def;
        const ObSSTableSchemaColumnDef *column = NULL;
        int ret = OB_SUCCESS;
        for (int i = 0; i < DEFAULT_COLUMN_DEF_SIZE; ++i)
        {
          column_def.column_group_id_ = 2;
          column_def.column_name_id_ = static_cast<uint16_t>(i + 2);
          column_def.rowkey_seq_ = 0;
          if (oceanbase::common::OB_MAX_COLUMN_NUMBER > i)
          {
            column_def.table_id_ = 1000;
          }
          else
          {
            column_def.table_id_ = 1001;
          }
          column_def.column_value_type_ = ObNullType;
          ret = schema.add_column_def(column_def);
          EXPECT_EQ(OB_SUCCESS, ret);
        }

        EXPECT_EQ(DEFAULT_COLUMN_DEF_SIZE, schema.get_column_count());

        for (int i = 0; i < DEFAULT_COLUMN_DEF_SIZE; ++i)
        {
          column = schema.get_column_def(i);
          EXPECT_EQ(i + 2, (int32_t)column->column_name_id_);
          EXPECT_EQ(2, (int32_t)column->column_group_id_);
          if (oceanbase::common::OB_MAX_COLUMN_NUMBER > i)
          {
            EXPECT_EQ(1000, (int32_t)column->table_id_);
          }
          else
          {
            EXPECT_EQ(1001, (int32_t)column->table_id_);
          }
          EXPECT_EQ(ObNullType, column->column_value_type_);
        }
      }

      TEST_F(TestObSSTableSchema, test_reset_schema)
      {
        ObSSTableSchema schema;
        ObSSTableSchemaColumnDef column_def;
        const ObSSTableSchemaColumnDef *column = NULL;
        int ret = OB_SUCCESS;
        for (int i = 0; i < DEFAULT_COLUMN_DEF_SIZE; ++i)
        {
          column_def.column_group_id_ = 2;
          column_def.column_name_id_ = static_cast<uint16_t>(i + 2);
          column_def.rowkey_seq_ = 0;
          if (oceanbase::common::OB_MAX_COLUMN_NUMBER > i)
          {
            column_def.table_id_ = 1000;
          }
          else
          {
            column_def.table_id_ = 1001;
          }
          column_def.column_value_type_ = ObNullType;
          ret = schema.add_column_def(column_def);
          EXPECT_EQ(OB_SUCCESS, ret);
        }

        EXPECT_EQ(DEFAULT_COLUMN_DEF_SIZE, schema.get_column_count());

        for (int i = 0; i < DEFAULT_COLUMN_DEF_SIZE; ++i)
        {
          column = schema.get_column_def(i);
          EXPECT_EQ(i + 2, (int32_t)column->column_name_id_);
          EXPECT_EQ(2, (int32_t)column->column_group_id_);
          if (oceanbase::common::OB_MAX_COLUMN_NUMBER > i)
          {
            EXPECT_EQ(1000, (int32_t)column->table_id_);
          }
          else
          {
            EXPECT_EQ(1001, (int32_t)column->table_id_);
          }
          EXPECT_EQ(ObNullType, column->column_value_type_);
        }

        schema.reset();

        for (int i = 0; i < DEFAULT_COLUMN_DEF_SIZE; ++i)
        {
          column_def.column_group_id_ = 2;
          column_def.column_name_id_ = static_cast<uint16_t>(i + 2);
          column_def.rowkey_seq_ = 0;
          if (oceanbase::common::OB_MAX_COLUMN_NUMBER > i)
          {
            column_def.table_id_ = 1000;
          }
          else
          {
            column_def.table_id_ = 1001;
          }
          column_def.column_value_type_ = ObNullType;
          ret = schema.add_column_def(column_def);
          EXPECT_EQ(OB_SUCCESS, ret);
        }

        EXPECT_EQ(DEFAULT_COLUMN_DEF_SIZE, schema.get_column_count());

        for (int i = 0; i < DEFAULT_COLUMN_DEF_SIZE; ++i)
        {
          column = schema.get_column_def(i);
          EXPECT_EQ(i + 2, (int32_t)column->column_name_id_);
          EXPECT_EQ(2, (int32_t)column->column_group_id_);
          if (oceanbase::common::OB_MAX_COLUMN_NUMBER > i)
          {
            EXPECT_EQ(1000, (int32_t)column->table_id_);
          }
          else
          {
            EXPECT_EQ(1001, (int32_t)column->table_id_);
          }
          EXPECT_EQ(ObNullType, column->column_value_type_);
        }
      }


      TEST_F(TestObSSTableSchema, test_add_column_def_not_in_order)
      {
        ObSSTableSchema schema;
        ObSSTableSchemaColumnDef column_def;
        const ObSSTableSchemaColumnDef *column = NULL;
        int ret = OB_SUCCESS;

        column_def.reserved_ = 0;
        column_def.rowkey_seq_ = 0;
        column_def.column_group_id_ = 2;
        column_def.column_name_id_ = 2;
        column_def.column_value_type_ = ObDoubleType;
        column_def.table_id_ = 1000;
        ret = schema.add_column_def(column_def);
        EXPECT_TRUE(OB_SUCCESS == ret);

        //add an column def with sama
        //table_id column_group_id column_name_id with previous one
        column_def.column_group_id_ = 2;
        column_def.column_name_id_ = 2;
        column_def.column_value_type_ = ObIntType;
        column_def.table_id_ = 1000;
        ret = schema.add_column_def(column_def);
        EXPECT_TRUE(OB_ERROR == ret);

        column_def.column_group_id_ = 2;
        column_def.column_name_id_ = 3;
        column_def.column_value_type_ = ObIntType;
        column_def.table_id_ = 1000;
        ret = schema.add_column_def(column_def);
        EXPECT_TRUE(OB_SUCCESS == ret);

        //curr_table_id < pre_table_id
        column_def.column_group_id_ = 2;
        column_def.column_name_id_ = 4;
        column_def.column_value_type_ = ObIntType;
        column_def.table_id_ = 900;
        ret = schema.add_column_def(column_def);
        EXPECT_TRUE(OB_ERROR == ret);

        column_def.column_group_id_ = 3;
        column_def.column_name_id_ = 4;
        column_def.column_value_type_ = ObVarcharType;
        column_def.table_id_ = 1000;
        ret = schema.add_column_def(column_def);
        EXPECT_TRUE(OB_SUCCESS == ret);

        //curr_column_group_id < pre_column_group_id
        column_def.column_group_id_ = 2;
        column_def.column_name_id_ = 2;
        column_def.column_value_type_ = ObIntType;
        column_def.table_id_ = 1000;
        ret = schema.add_column_def(column_def);
        EXPECT_TRUE(OB_ERROR == ret);

        //curr_column_name_id < pre_column_group_id
        column_def.column_group_id_ = 3;
        column_def.column_name_id_ = 2;
        column_def.column_value_type_ = ObIntType;
        column_def.table_id_ = 1000;
        ret = schema.add_column_def(column_def);
        EXPECT_TRUE(OB_ERROR == ret);

        EXPECT_EQ(3, schema.get_column_count());

        column = schema.get_column_def(0);
        EXPECT_TRUE(NULL != column);
        EXPECT_EQ(2, (int32_t)column->column_group_id_);
        EXPECT_EQ(2, (int32_t)column->column_name_id_);
        EXPECT_EQ(ObDoubleType, column->column_value_type_);
        EXPECT_EQ(1000, (int32_t)column->table_id_);

        column = schema.get_column_def(1);
        EXPECT_EQ(2, (int32_t)column->column_group_id_);
        EXPECT_EQ(3, (int32_t)column->column_name_id_);
        EXPECT_EQ(ObIntType, column->column_value_type_);
        EXPECT_EQ(1000, (int32_t)column->table_id_);

        column = schema.get_column_def(2);
        EXPECT_EQ(3, (int32_t)column->column_group_id_);
        EXPECT_EQ(4, (int32_t)column->column_name_id_);
        EXPECT_EQ(ObVarcharType, column->column_value_type_);
        EXPECT_EQ(1000, (int32_t)column->table_id_);
      }


      int64_t sum(int64_t num)
      {
        int64_t ret = 0;
        for ( int64_t i = 0; i <= num; ++i )
        {
          ret += i;
        }
        return ret;
      }

      TEST_F(TestObSSTableSchema, test_get_group_schema)
      {
        ObSSTableSchema schema;
        ObSSTableSchemaColumnDef column_def;
        const ObSSTableSchemaColumnDef* def_ptr;
        int ret = OB_SUCCESS;
        column_def.reserved_ = 0;

        for ( int table_id = 1000; table_id < 1010; ++table_id )
        {
          column_def.table_id_ = table_id;
          for ( int group_id = 1 ; group_id <= (table_id - 1000)%3 + 1; ++group_id )
          {
            column_def.column_group_id_ = static_cast<uint16_t>(group_id);
            for ( int column_id = 0; column_id < group_id; ++column_id )
            {
              column_def.column_name_id_ = static_cast<uint16_t>(column_id);
              column_def.column_value_type_ = ObIntType;
              ret = schema.add_column_def(column_def);
              EXPECT_TRUE(OB_SUCCESS == ret);
            }
          }
        }

        EXPECT_EQ(31, schema.get_column_count());
        int64_t size = 0;
        for ( int i = 1000; i < 1010; ++i)
        {
          for ( int gi = 1; gi <= (i - 1000)%3+1; ++gi)
          {
            def_ptr = schema.get_group_schema(i, gi, size);
            EXPECT_EQ(gi, size);
            for ( int j = 0; j < size; ++j )
            {
              EXPECT_EQ(i, (int32_t)def_ptr->table_id_);
              EXPECT_EQ(gi, def_ptr->column_group_id_);
              def_ptr++;
            }
          }
        }
      }

      TEST_F(TestObSSTableSchema, test_get_table_column_groups_id)
      {
        ObSSTableSchema schema;
        ObSSTableSchemaColumnDef column_def;
        int ret = OB_SUCCESS;
        column_def.reserved_ = 0;
        column_def.rowkey_seq_ = 0;

        for ( int table_id = 1000; table_id < 1010; ++table_id )
        {
          column_def.table_id_ = table_id;
          for ( int group_id = 1 ; group_id <= (table_id - 1000)%3 + 1; ++group_id )
          {
            column_def.column_group_id_ = static_cast<uint16_t>(group_id);
            for ( int column_id = 0; column_id < group_id; ++column_id )
            {
              column_def.column_name_id_ = static_cast<uint16_t>(column_id);
              column_def.column_value_type_ = ObIntType;
              ret = schema.add_column_def(column_def);
              EXPECT_TRUE(OB_SUCCESS == ret);
            }
          }
        }

        EXPECT_EQ(31, schema.get_column_count());

        //find an column that doesn't exist
        uint64_t find[oceanbase::common::OB_MAX_COLUMN_GROUP_NUMBER];
        int64_t size = OB_MAX_COLUMN_NUMBER;
        uint64_t table_id = 10000;
        //uint64_t group_id = 4;
        ret = schema.get_table_column_groups_id(table_id, find, size);
        EXPECT_EQ(OB_ERROR, ret);
        EXPECT_EQ(0, size);
        for ( int i = 1000; i < 1010; ++i)
        {
          for ( int ci = 0; ci < (i - 1000)%3 + 1; ++ci)
          {
            size = oceanbase::common::OB_MAX_COLUMN_GROUP_NUMBER;
            ret = schema.get_table_column_groups_id(i, find, size);
            EXPECT_EQ(OB_SUCCESS, ret);
            EXPECT_EQ((i-1000)%3+1, size);
            for (int index = 0; index < size; ++index)
            {
              EXPECT_EQ(index + 1, (int)find[index]);
            }
          }
        }  
      }

      TEST_F(TestObSSTableSchema, test_get_column_groups_id)
      {
        ObSSTableSchema schema;
        ObSSTableSchemaColumnDef column_def;
        int ret = OB_SUCCESS;
        column_def.reserved_ = 0;
        column_def.rowkey_seq_ = 0;

        for ( int table_id = 1000; table_id < 1010; ++table_id )
        {
          column_def.table_id_ = table_id;
          for ( int group_id = 1 ; group_id <= (table_id - 1000)%3 + 1; ++group_id )
          {
            column_def.column_group_id_ = static_cast<uint16_t>(group_id);
            for ( int column_id = 0; column_id < group_id; ++column_id )
            {
              column_def.column_name_id_ = static_cast<uint16_t>(column_id);
              column_def.column_value_type_ = ObIntType;
              ret = schema.add_column_def(column_def);
              EXPECT_TRUE(OB_SUCCESS == ret);
            }
          }
        }

        EXPECT_EQ(31, schema.get_column_count());

        //find an column that doesn't exist
        uint64_t find[oceanbase::common::OB_MAX_COLUMN_GROUP_NUMBER];
        int64_t size = OB_MAX_COLUMN_GROUP_NUMBER;
        uint64_t table_id = 10000;
        uint64_t group_id = 4;
        ret = schema.get_column_groups_id(table_id, group_id, find, size);
        EXPECT_EQ(OB_ERROR, ret);
        EXPECT_EQ(OB_MAX_COLUMN_GROUP_NUMBER, size);
        for ( int i = 1000; i < 1010; ++i)
        {
          for ( int ci = 0; ci < (i - 1000)%3 + 1; ++ci)
          {
            size = oceanbase::common::OB_MAX_COLUMN_GROUP_NUMBER;
            ret = schema.get_column_groups_id(i, ci, find, size);
            EXPECT_EQ(OB_SUCCESS, ret);
            EXPECT_EQ((i-1000)%3+1 - ci, size);
            for (int index = 0; index < size; ++index)
            {
              EXPECT_EQ(ci+index+1, (int)find[index]);
            }
          }
        }
      }

      TEST_F(TestObSSTableSchema, test_find_first_offset_column_group_schema)
      {
        ObSSTableSchema schema;
        ObSSTableSchemaColumnDef column_def;
        int ret = OB_SUCCESS;
        column_def.reserved_ = 0;
        column_def.rowkey_seq_ = 0;

        for ( int table_id = 1000; table_id < 1010; ++table_id )
        {
          column_def.table_id_ = table_id;
          for ( int group_id = 1 ; group_id <= (table_id - 1000)%3 + 1; ++group_id )
          {
            column_def.column_group_id_ = static_cast<uint16_t>(group_id);
            for ( int column_id = 0; column_id < group_id; ++column_id )
            {
              column_def.column_name_id_ = static_cast<uint16_t>(column_id);
              column_def.column_value_type_ = ObIntType;
              ret = schema.add_column_def(column_def);
              EXPECT_TRUE(OB_SUCCESS == ret);
            }
          }
        }

        EXPECT_EQ(31, schema.get_column_count());

        //find an column that doesn't exist
        uint64_t table_id  = 10000;
        uint64_t column_id = 4;
        uint64_t group_id = 0;
        int64_t offset_in_table = schema.find_offset_first_column_group_schema(table_id, column_id, group_id);
        EXPECT_EQ(-1, offset_in_table);
        EXPECT_EQ(0, (int)group_id);

        for ( int i = 1000; i < 1010; ++i)
        {
           for ( int gi = 1; gi <= (i - 1000)%3+1; ++gi)
           {
             for ( int ci = 0; ci < gi; ++ci)
             {
               offset_in_table = schema.find_offset_first_column_group_schema(i, ci, group_id);
               EXPECT_EQ((int)group_id, ci + 1) << ",i=" << i << ",gi=" << gi << ",ci=" << ci;
               EXPECT_EQ(offset_in_table, ci);
             }
           }
        }
      }

      TEST_F(TestObSSTableSchema, test_find_offset_column_group_schema)
      {
        ObSSTableSchema schema;
        ObSSTableSchemaColumnDef column_def;
        int ret = OB_SUCCESS;
        int64_t offset_in_group = 0;
        column_def.reserved_ = 0;
        column_def.rowkey_seq_ = 0;

        for ( int table_id = 1000; table_id < 1010; ++table_id )
        {
          column_def.table_id_ = table_id;
          for ( int group_id = 1 ; group_id <= (table_id - 1000)%3 + 1; ++group_id )
          {
            column_def.column_group_id_ = static_cast<uint16_t>(group_id);
            for (int column_id = 0; column_id < group_id; ++column_id)
            {
              column_def.column_name_id_ = static_cast<uint16_t>(column_id);
              column_def.column_value_type_ = ObIntType;
              ret = schema.add_column_def(column_def);
              EXPECT_TRUE(OB_SUCCESS == ret);
            }
          }
        }

        EXPECT_EQ(31, schema.get_column_count());

        //find an column that doesn't exist
        offset_in_group = schema.find_offset_column_group_schema(10000, 3, 4);
        EXPECT_EQ(-1, offset_in_group);

        for ( int i = 1000; i < 1010; ++i)
        {
          for ( int gi = 1; gi < (i - 1000)%3 +1; ++gi)
          {
            for ( int ci = 0; ci < gi; ++ci)
            {
              offset_in_group = schema.find_offset_column_group_schema(i, gi, ci);
              EXPECT_EQ(ci, offset_in_group);
            }
          }
        }
      }

      TEST_F(TestObSSTableSchema, write_schema_to_disk)
      {
        ObSSTableSchema schema;
        ObSSTableSchemaColumnDef column_def;
        int ret = OB_SUCCESS;
        column_def.reserved_ = 0;
        column_def.rowkey_seq_ = 0;

        for ( int table_id = 1000; table_id < 1010; ++table_id )
        {
          column_def.table_id_ = table_id;
          for ( int group_id = 1 ; group_id <= 10; ++group_id )
          {
            column_def.column_group_id_ = static_cast<uint16_t>(group_id);
            for ( int column_id = 0; column_id < group_id; ++column_id )
            {
              column_def.column_name_id_ = static_cast<uint16_t>(column_id);
              column_def.column_value_type_ = ObIntType;
              ret = schema.add_column_def(column_def);
              EXPECT_TRUE(OB_SUCCESS == ret);
            }
          }
        }

        EXPECT_EQ(550, schema.get_column_count());
        FileUtils filesys;
        filesys.open(schema_file_name, O_WRONLY | O_CREAT | O_TRUNC, 0644);
        int64_t buf_size = schema.get_serialize_size();
        char * serialize_buf = reinterpret_cast<char *>(malloc(buf_size));
        EXPECT_TRUE(NULL != serialize_buf);
        int64_t pos = 0;
        schema.serialize(serialize_buf, buf_size, pos);
        
        int64_t write_size = filesys.write(serialize_buf, buf_size);
        EXPECT_EQ(write_size, buf_size);
        free(serialize_buf);
        serialize_buf = NULL;
      }

      TEST_F(TestObSSTableSchema, test_serialize_and_deserialize)
      {
        ObSSTableSchema schema;
        ObSSTableSchema deserialized_schema;
        ObSSTableSchemaColumnDef column_def;
        memset(&column_def, 0, sizeof(column_def));
        const ObSSTableSchemaColumnDef *column = NULL;
        const ObSSTableSchemaColumnDef *deserialized_column = NULL;
        int64_t buf_size = (DEFAULT_COLUMN_DEF_SIZE + 1)  * sizeof(ObSSTableSchemaColumnDef)
                            + sizeof(ObSSTableSchemaHeader);
        char* serialize_buf = new char[buf_size];
        int64_t pos = 0;
        int ret = OB_SUCCESS;
        int64_t group_columns_count = OB_MAX_COLUMN_NUMBER / OB_MAX_COLUMN_GROUP_NUMBER;

        for (int i = 0; i < DEFAULT_COLUMN_DEF_SIZE; ++i)
        {
          column_def.column_group_id_ = static_cast<uint16_t>((i % OB_MAX_COLUMN_NUMBER) / group_columns_count);
          column_def.column_name_id_ = (uint16_t)((i % group_columns_count) + 2);
          column_def.column_value_type_ = ObNullType;
          column_def.table_id_ = (uint32_t)(i / OB_MAX_COLUMN_NUMBER + 1000);
          column_def.rowkey_seq_ = 0;
          ret = schema.add_column_def(column_def);
          EXPECT_EQ(OB_SUCCESS, ret);
        }

        EXPECT_EQ(DEFAULT_COLUMN_DEF_SIZE, schema.get_column_count());

        for (int i = 0; i < DEFAULT_COLUMN_DEF_SIZE; ++i)
        {
          column = schema.get_column_def(i);
          EXPECT_EQ((i % OB_MAX_COLUMN_NUMBER) / group_columns_count, (int32_t)column->column_group_id_);
          EXPECT_EQ((i % group_columns_count) + 2, (int32_t)column->column_name_id_);
          EXPECT_EQ(ObNullType, column->column_value_type_);
          EXPECT_EQ(i / OB_MAX_COLUMN_NUMBER + 1000, (int32_t)column->table_id_);
        }

        uint64_t find_id[OB_MAX_COLUMN_GROUP_NUMBER];
        uint64_t group_id_out  = OB_INVALID_ID;

        int64_t size = OB_MAX_COLUMN_GROUP_NUMBER;
        
        uint64_t max_table_id = 1000 + DEFAULT_COLUMN_DEF_SIZE / OB_MAX_COLUMN_NUMBER ;
        
        //TEST get table column group ids
        for(uint64_t table_id = 1000; table_id < max_table_id; ++table_id)
        {
          size = OB_MAX_COLUMN_GROUP_NUMBER;
          ret  = schema.get_table_column_groups_id(table_id, find_id, size);
          EXPECT_EQ(16, size);
          for(int64_t index = 0; index < size; ++index)
          {
            EXPECT_EQ(index, (int64_t)find_id[index]);
          }
        }
        
        //TEST get offset first column group schema
        int64_t offset = -1;
        for(uint64_t table_id = 1000; table_id < max_table_id; ++table_id)
        {
          for(uint64_t column_id = 2; column_id < 10; ++column_id)
          {
            size = OB_MAX_COLUMN_GROUP_NUMBER;
            offset  = schema.find_offset_first_column_group_schema(table_id, column_id, group_id_out);
            EXPECT_EQ(offset, (int64_t)(column_id - 2));
            EXPECT_EQ(group_id_out, (uint64_t)0);
          }
        }
        
        //TEST get column group schema
        int64_t group_size = 0;
        const ObSSTableSchemaColumnDef* def = NULL;
        for(uint64_t table_id = 1000; table_id < max_table_id; ++table_id)
        {
          for(uint64_t group_id = 0; group_id < 16; ++group_id)
          {
            def = schema.get_group_schema(table_id, group_id, group_size);
            EXPECT_EQ(group_size, group_columns_count);
            EXPECT_EQ(def, schema.get_column_def(static_cast<int32_t>(group_id * group_columns_count + (table_id - 1000) * OB_MAX_COLUMN_NUMBER)));
          }
        }
        
        //TEST get column group ids 
        for(uint64_t table_id = 1000; table_id < max_table_id; ++table_id)
        {
          for(uint64_t column_id = 2; column_id < 10; ++column_id)
          {
            size = OB_MAX_COLUMN_GROUP_NUMBER;
            ret  = schema.get_column_groups_id(table_id, column_id, find_id, size);
            EXPECT_EQ((int)size, 16);
            for(int i = 0; i < size; ++i)
            {
              EXPECT_EQ((int)find_id[i], i);
            }
          }
        }

        schema.serialize(serialize_buf, buf_size, pos);
        pos = 0;

        deserialized_schema.deserialize(serialize_buf, buf_size, pos);

        EXPECT_EQ(schema.get_column_count(), deserialized_schema.get_column_count());
        for (int i = 0; i < DEFAULT_COLUMN_DEF_SIZE; ++i)
        {
          column = schema.get_column_def(i);
          deserialized_column = deserialized_schema.get_column_def(i);
          EXPECT_EQ((i % OB_MAX_COLUMN_NUMBER) / group_columns_count, (int32_t)column->column_group_id_);
          EXPECT_EQ((i % group_columns_count) + 2, (int32_t)column->column_name_id_);
          EXPECT_EQ(column->column_name_id_, deserialized_column->column_name_id_);
          EXPECT_EQ(ObNullType, column->column_value_type_);
          EXPECT_EQ(i / OB_MAX_COLUMN_NUMBER + 1000, (int32_t)column->table_id_);
          EXPECT_EQ(column->column_value_type_, deserialized_column->column_value_type_);
          EXPECT_EQ(column->table_id_, deserialized_column->table_id_);
        }
        for(uint64_t table_id = 1000; table_id < max_table_id; ++table_id)
        {
          size = OB_MAX_COLUMN_GROUP_NUMBER;
          ret  = deserialized_schema.get_table_column_groups_id(table_id, find_id, size);
          EXPECT_EQ(16, size);
          for(int64_t index = 0; index < size; ++index)
          {
            EXPECT_EQ(index, (int64_t)find_id[index]);
          }
        }

        offset = -1;
        group_id_out = OB_INVALID_ID;
        for(uint64_t table_id = 1000; table_id < max_table_id; ++table_id)
        {
          for(uint64_t column_id = 2; column_id < 10; ++column_id)
          {
            size = OB_MAX_COLUMN_GROUP_NUMBER;
            offset = deserialized_schema.find_offset_first_column_group_schema(table_id, column_id, group_id_out);
            EXPECT_EQ(offset, (int64_t)(column_id - 2));
            EXPECT_EQ(group_id_out, (uint64_t)0);
          }
        }
                 
        //TEST get column group schema
        group_size = 0;
        for(uint64_t table_id = 1000; table_id < max_table_id; ++table_id)
        {
          for(uint64_t group_id = 0; group_id < 16; ++group_id)
          {
            def = deserialized_schema.get_group_schema(table_id, group_id, group_size);
            EXPECT_EQ(group_size, group_columns_count);
            EXPECT_EQ(def, deserialized_schema.get_column_def(static_cast<int32_t>(group_id * group_columns_count + (table_id - 1000) * OB_MAX_COLUMN_NUMBER)));
          }
        }
        
        //TEST get column group ids 
        for(uint64_t table_id = 1000; table_id < max_table_id; ++table_id)
        {
          for(uint64_t column_id = 2; column_id < 10; ++column_id)
          {
            size = OB_MAX_COLUMN_GROUP_NUMBER;
            ret  = deserialized_schema.get_column_groups_id(table_id, column_id, find_id, size);
            EXPECT_EQ((int)size, 16);
            for(int i = 0; i < size; ++i)
            {
              EXPECT_EQ((int)find_id[i], i);
            }
          }
        }

        delete [] serialize_buf;
      }
    }//end namespace common
  }//end namespace tests
}//end namespace oceanbase

int main(int argc, char** argv)
{
  ob_init_memory_pool();
  TBSYS_LOGGER.setLogLevel("ERROR");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

