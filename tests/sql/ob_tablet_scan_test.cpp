/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_tablet_scan_test.cpp
 *
 * Authors:
 *   Junquan Chen <jianming.cjq@taobao.com>
 *
 */


#include "common/utility.h"
#include "sql/ob_tablet_scan.h"
#include "common/ob_malloc.h"
#include "common/ob_schema.h"
#include <gtest/gtest.h>
#include "ob_file_table.h"
#include "test_helper.h"
#include "sstable/ob_sstable_writer.h"
#include "common/file_directory_utils.h"
#include "ob_fake_sql_ups_rpc_proxy.h"
#include "chunkserver/ob_chunk_server_main.h"

using namespace oceanbase;
using namespace common;
using namespace sql;
using namespace sstable;
using namespace test;
using namespace chunkserver;

#define TABLE_ID 1001
#define OK(value) ASSERT_EQ(OB_SUCCESS, (value))
#define OB_STR(str) (ObString(0, (int32_t)strlen(str), str))

int get_value(const ObObj &cell, char *buf)
{
  int ret = OB_SUCCESS;
  ObString str;
  int64_t int_value = 0;
  if(cell.get_type() == ObVarcharType)
  {
    cell.get_varchar(str);
    sprintf(buf, "%.*s", str.length(), str.ptr());
  }
  else if(cell.get_type() == ObIntType)
  {
    cell.get_int(int_value);
    sprintf(buf, "%ld", int_value);
  }
  else
  {
    ret = OB_ERROR;
  }
  return ret;
}

int int64_cmp(const void *a1, const void *a2)
{
  int64_t &b1 = (*(int64_t *)a1);
  int64_t &b2 = (*(int64_t *)a2);
  if(b1 > b2)
  {
    return 1;
  }
  else if(b1 < b2)
  {
    return -1;
  }
  else
  {
    return 0;
  }
}

namespace oceanbase
{
  namespace sql
  {
    namespace test
    {
      static const int64_t sstable_file_id = 268801;
      static const int64_t sstable_file_offset = 0;
      static const ObSSTableId sstable_id(sstable_file_id);
      const char* idx_file = "./tmp/idx_1_1";
      static chunkserver::ObMultiVersionTabletImage image_(GFactory::get_instance().get_fi_cache());

      class ObTabletScanTest: public ::testing::Test
      {
        public:
          ObTabletScanTest();
          virtual ~ObTabletScanTest();
          virtual void SetUp();
          virtual void TearDown();

          static int gen_sstable(const char *file_name, const char *sst_file);
          static int gen_sstable_idx(const char *file_name);

        private:
          // disallow copy
          ObTabletScanTest(const ObTabletScanTest &other);
          ObTabletScanTest& operator=(const ObTabletScanTest &other);
        protected:
          // data members
      };

      ObTabletScanTest::ObTabletScanTest()
      {
      }

      ObTabletScanTest::~ObTabletScanTest()
      {
      }

      void ObTabletScanTest::SetUp()
      {
      }

      void ObTabletScanTest::TearDown()
      {
      }

      int ObTabletScanTest::gen_sstable(const char *file_name, const char *sst_file)
      {
        int ret = OB_SUCCESS;

        const ObRow *row = NULL;
        const ObObj *cell = NULL;
        uint64_t table_id = OB_INVALID_ID;
        uint64_t column_id = OB_INVALID_ID;

        ObSSTableWriter writer;
        ObSSTableSchema schema;
        ObSSTableSchemaColumnDef column_def;
        ObSSTableRow sst_row;

        ObFileTable file_table(file_name);
        file_table.open();

        for(int32_t i=0;i<file_table.column_count_;i++)
        {
          column_def.table_id_ = TABLE_ID;
          column_def.column_group_id_ = 0;
          if(0 == i || 1 == i)
          {
            column_def.rowkey_seq_ = (uint16_t)(i + 1);
          }
          else
          {
            column_def.rowkey_seq_ = 0;
          }
          column_def.column_name_id_ = (uint16_t)(file_table.column_ids_[i]);
          column_def.column_value_type_ = file_table.column_type_[i];
          schema.add_column_def(column_def);
        }

        if(OB_SUCCESS != (ret = writer.create_sstable(schema, OB_STR(sst_file), OB_STR("lzo_1.0"), 1)))
        {
          TBSYS_LOG(WARN, "create sstable fail:ret[%d]", ret);
        }

        int64_t space_usage = 0;

        while(OB_SUCCESS == ret)
        {
          sst_row.clear();
          sst_row.set_table_id(TABLE_ID);
          sst_row.set_column_group_id(0);

          ret = file_table.get_next_row(row);
          if(OB_SUCCESS == ret)
          {
            for(int64_t col = 0; col < row->get_column_num(); col ++)
            {
              row->raw_get_cell(col, cell, table_id, column_id);
              sst_row.add_obj(*cell);
            }
            writer.append_row(sst_row, space_usage);
          }
          else if(OB_ITER_END == ret)
          {
            ret = OB_SUCCESS;
            break;
          }
        }

        int64_t offset = 0;
        writer.close_sstable(offset);

        return ret;
      }

      int ObTabletScanTest::gen_sstable_idx(const char *file_name)
      {
        ObChunkServerMain::get_instance()->get_chunk_server().get_param();

        int err = OB_SUCCESS;
        char sstable_file_path[OB_MAX_FILE_NAME_LENGTH];
        char sstable_file_dir[OB_MAX_FILE_NAME_LENGTH];
        err = get_sstable_directory((sstable_id.sstable_file_id_ & 0xFF), sstable_file_dir, OB_MAX_FILE_NAME_LENGTH);
        if (err) return err;
        else
        {
          bool ok = FileDirectoryUtils::exists(sstable_file_dir);
          if (!ok)
          {
            ok = FileDirectoryUtils::create_full_path(sstable_file_dir);
            if (!ok)
              TBSYS_LOG(ERROR, "create sstable path:%s failed", sstable_file_dir);
          }
        }
        err = get_sstable_path(sstable_id, sstable_file_path, OB_MAX_FILE_NAME_LENGTH);
        if (err) return err;

        //sprintf(sstable_file_path, "./tmp/%ld", sstable_file_id);

        gen_sstable(file_name, sstable_file_path);

        ObNewRange range;
        range.table_id_ = TABLE_ID;
        range.set_whole_range();
        ObTablet* tablet = NULL;
        ObMultiVersionTabletImage image(GFactory::get_instance().get_fi_cache());
        image.alloc_tablet_object(range, 1, tablet);
        if (NULL != tablet)
        {
          tablet->set_data_version(1);
          tablet->set_disk_no(1);
          tablet->add_sstable_by_id(sstable_id);
          image.add_tablet(tablet, false, true);
          image.dump();
          image.write(idx_file, 1, 1);
        }

        return err;
      }

#define INIT_TABLET_SCAN \
        ObTabletScan tablet_scan; \
        ObSSTableScan sstable_scan; \
        ObUpsScan ups_scan; \
        ObUpsMultiGet ups_multi_get; \
        ObFakeSqlUpsRpcProxy rpc_proxy; \
        sql::ScanContext context; \
        const int64_t network_timeout = 1000 * 1000; \
 \
        rpc_proxy.set_ups_scan("./tablet_scan_test_data/ups_table1.ini"); \
        rpc_proxy.set_ups_multi_get("./tablet_scan_test_data/ups_table2.ini"); \
 \
        ups_scan.set_ups_rpc_proxy(&rpc_proxy); \
        ups_scan.set_network_timeout(network_timeout); \
        tablet_scan.set_ups_scan(&ups_scan); \
 \
        ups_multi_get.set_rpc_proxy(&rpc_proxy); \
        ups_multi_get.set_network_timeout(network_timeout); \
        tablet_scan.set_ups_multi_get(&ups_multi_get); \
         \
        image_.read(idx_file, 1, 1, true); \
        image_.prepare_for_service(); \
        image_.dump(); \
 \
        context.tablet_image_ = &image_; \
        context.block_cache_ = &GFactory::get_instance().get_block_cache(); \
        context.block_index_cache_ = &GFactory::get_instance().get_block_index_cache(); \
 \
        sstable_scan.set_scan_context(context); \
        tablet_scan.set_sstable_scan(&sstable_scan); \
 \
        ObSchemaManagerV2 schema_mgr; \
        tbsys::CConfig cfg; \
        ASSERT_TRUE(schema_mgr.parse_from_file("./tablet_scan_test_data/schema.ini", cfg));


      TEST_F(ObTabletScanTest, create_plan_not_join)
      {
        INIT_TABLET_SCAN;
        tablet_scan.set_join_batch_count(3);

        ObSqlExpression expr;
        expr.set_tid_cid(TABLE_ID, 40);
        ExprItem item_a;
        item_a.type_ = T_REF_COLUMN;
        item_a.value_.cell_.tid = TABLE_ID;
        item_a.value_.cell_.cid = 40;
        expr.add_expr_item(item_a);
        expr.add_expr_item_end();

        OK(tablet_scan.add_output_column(expr));

        int64_t start = 100;
        int64_t end = 1000;

        ObNewRange range;
        range.table_id_ = TABLE_ID;
        CharArena arena;
        gen_new_range(start, end, arena, range);

        tablet_scan.set_range(range);

        OK(tablet_scan.create_plan(schema_mgr));

        //ASSERT_TRUE(!(tablet_scan.need_join_));

        ObScanParam *scan_param = &(tablet_scan.op_ups_scan_->cur_scan_param_);

        int id_size = 1;
        ASSERT_EQ(id_size, scan_param->get_column_id_size());

        uint64_t id_array[id_size];
        for(int i=0;i<id_size;i++)
        {
          id_array[i] = scan_param->get_column_id()[i];
        }

        qsort(id_array, id_size, sizeof(uint64_t), int64_cmp);

        uint64_t result[] = {40};
        for(int i=0;i<id_size;i++)
        {
          ASSERT_EQ(id_array[i], result[i]);
        }
      }

      TEST_F(ObTabletScanTest, create_plan_join)
      {
        INIT_TABLET_SCAN;
        tablet_scan.set_join_batch_count(30);

        ObSqlExpression expr1, expr2;
        expr1.set_tid_cid(TABLE_ID, 40);

        ExprItem item_a;
        item_a.type_ = T_REF_COLUMN;
        item_a.value_.cell_.tid = TABLE_ID;
        item_a.value_.cell_.cid = 40;
        expr1.add_expr_item(item_a);
        expr1.add_expr_item_end();

        OK(tablet_scan.add_output_column(expr1));
        expr2.set_tid_cid(TABLE_ID, 50);

        ExprItem item_b;
        item_b.type_ = T_REF_COLUMN;
        item_b.value_.cell_.tid = TABLE_ID;
        item_b.value_.cell_.cid = 50;
        expr2.add_expr_item(item_b);
        expr2.add_expr_item_end();

        OK(tablet_scan.add_output_column(expr2));

        int64_t start = 100;
        int64_t end = 1000;

        CharArena arena;
        ObNewRange range;
        range.table_id_ = TABLE_ID;
        gen_new_range(start, end, arena, range);

        tablet_scan.set_range(range);

        OK(tablet_scan.create_plan(schema_mgr));

        //ASSERT_TRUE(tablet_scan.need_join_);
        ObTabletJoin::TableJoinInfo &table_join_info = tablet_scan.op_tablet_join_.table_join_info_;

        ASSERT_EQ(1, table_join_info.join_column_.count());
        ASSERT_EQ(1002U, table_join_info.right_table_id_);
        ASSERT_EQ(50U, table_join_info.join_column_.at(0).left_column_id_);
        ASSERT_EQ(30U, table_join_info.join_column_.at(0).right_column_id_);

        ASSERT_EQ(1, table_join_info.join_condition_.count());
        ASSERT_EQ(1002U, table_join_info.right_table_id_);
        ASSERT_EQ(30U, table_join_info.join_condition_.at(0));
/*
        ObScanParam *scan_param = &(tablet_scan.op_ups_scan_->cur_scan_param_);

        int id_size = 3;
        ASSERT_EQ(id_size, scan_param->get_column_id_size());

        uint64_t id_array[id_size];
        for(int i=0;i<id_size;i++)
        {
          id_array[i] = scan_param->get_column_id()[i];
        }

        qsort(id_array, id_size, sizeof(uint64_t), int64_cmp);

        uint64_t result[] = {30, 40, 50};
        for(int i=0;i<id_size;i++)
        {
          ASSERT_EQ(id_array[i], result[i]);
        }
        */
      }

      TEST_F(ObTabletScanTest, get_next_row)
      {
        INIT_TABLET_SCAN;
        tablet_scan.set_join_batch_count(3);

        ObSqlExpression expr1, expr2, expr3;

        expr1.set_tid_cid(TABLE_ID, 40);
        ExprItem item_a;
        item_a.type_ = T_REF_COLUMN;
        item_a.value_.cell_.tid = TABLE_ID;
        item_a.value_.cell_.cid = 40;
        expr1.add_expr_item(item_a);
        expr1.add_expr_item_end();
        OK(tablet_scan.add_output_column(expr1));

        expr2.set_tid_cid(TABLE_ID, 50);
        ExprItem item_b;
        item_b.type_ = T_REF_COLUMN;
        item_b.value_.cell_.tid = TABLE_ID;
        item_b.value_.cell_.cid = 50;
        expr2.add_expr_item(item_b);
        expr2.add_expr_item_end();
        OK(tablet_scan.add_output_column(expr2));

        int64_t start = 0;
        int64_t end = 1000;

        ObNewRange range;
        range.table_id_ = TABLE_ID;
        CharArena arena;
        gen_new_range(start, end, arena, range);

        tablet_scan.set_range(range);

        ObFileTable result("./tablet_scan_test_data/result.ini");

        int err = OB_SUCCESS;
        int64_t count = 0;
        const ObRow *row = NULL;
        const ObRow *result_row = NULL;
        const ObObj *value = NULL;
        const ObObj *result_value = NULL;
        uint64_t table_id = 0;
        uint64_t column_id = 0;

        OK(tablet_scan.create_plan(schema_mgr));
        OK(tablet_scan.open());
        OK(result.open());

        while(OB_SUCCESS == err)
        {
          err = tablet_scan.get_next_row(row);
          if(OB_SUCCESS != err)
          {
            ASSERT_TRUE(OB_ITER_END == err);
            break;
          }

          OK(result.get_next_row(result_row));
          count ++;

          ASSERT_TRUE(row->get_column_num() > 0);

          for(int64_t i=0;i<row->get_column_num();i++)
          {
            OK(row->raw_get_cell(i, value, table_id, column_id));
            OK(result_row->get_cell(table_id, column_id, result_value));

            if( *value != *result_value )
            {
              printf("row:[%ld], column[%ld]===========\n", count, i);
              printf("row: %s\n", print_obj(*value));
              printf("result: %s\n", print_obj(*result_value));
            }

            ASSERT_TRUE((*value) == (*result_value));
          }
        }

        ASSERT_TRUE(count > 0);

        OK(result.close());
        OK(tablet_scan.close());

      }
    }
  }
}

int main(int argc, char **argv)
{
  system("python gen_tablet_scan_test_data.py");
  system("rm -rf ./tmp");
  sql::test::ObTabletScanTest::gen_sstable_idx("./tablet_scan_test_data/table1.ini");

  ob_init_memory_pool();
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
