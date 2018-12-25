/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_tablet_join_test.cpp
 *
 * Authors:
 *   Junquan Chen <jianming.cjq@taobao.com>
 *
 */

#include "common/ob_malloc.h"
#include <gtest/gtest.h>
#include "sql/ob_tablet_join.h"
#include "sql/ob_tablet_cache_join.h"
#include "sql/ob_tablet_direct_join.h"
#include "ob_file_table.h"
#include "common/utility.h"
#include "ob_fake_ups_multi_get.h"
#include "common/ob_ups_row_util.h"
#include "ob_fake_sstable_scan.h"
#include "ob_fake_ups_scan.h"
#include "sql/ob_tablet_scan_fuse.h"

using namespace oceanbase;
using namespace common;
using namespace sql;
using namespace test;

#define OK(value) ASSERT_EQ(OB_SUCCESS, (value))

namespace oceanbase
{
  namespace sql
  {
    namespace test
    {
      class ObTabletJoinTest: public ::testing::Test
      {
        public:
          ObTabletJoinTest();
          virtual ~ObTabletJoinTest();
          virtual void SetUp();
          virtual void TearDown();
          int read_join_info(ObTabletJoin::TableJoinInfo &tablet_join_info, const char *file_name);
          int get_rowkey(const ObRow &row, ObString &rowkey);
          void test_big_data(ObTabletJoin &tablet_join);
          void test_get_next_row(ObTabletJoin &tablet_join);

        private:
          // disallow copy
          ObTabletJoinTest(const ObTabletJoinTest &other);
          ObTabletJoinTest& operator=(const ObTabletJoinTest &other);
        protected:
          // data members
          ObTabletJoin::TableJoinInfo tablet_join_info_;
          static const uint64_t LEFT_TABLE_ID = 1000;
          static const uint64_t RIGHT_TABLE_ID = 1001;
      };

      ObTabletJoinTest::ObTabletJoinTest()
      {
      }

      ObTabletJoinTest::~ObTabletJoinTest()
      {
      }

      void ObTabletJoinTest::test_get_next_row(ObTabletJoin &tablet_join)
      {
        tablet_join.set_table_join_info(tablet_join_info_);

        ObFakeUpsMultiGet fake_ups_multi_get("tablet_join_test_data/fetch_ups_row_ups_row.ini");
        ObFakeSSTableScan sstable("tablet_join_test_data/fetch_ups_row_fused_row.ini");
        ObFakeUpsScan ups_scan("tablet_join_test_data/none_ups.ini");

        ObTabletScanFuse tablet_fuse;
        OK(tablet_fuse.set_sstable_scan(&sstable));
        OK(tablet_fuse.set_incremental_scan(&ups_scan));

        tablet_join.set_child(0, tablet_fuse);
        tablet_join.set_batch_count(30000);
        tablet_join.set_ups_multi_get(&fake_ups_multi_get);

        tablet_join.add_column_id(1);
        tablet_join.add_column_id(4);
        tablet_join.add_column_id(5);
        tablet_join.add_column_id(6);

        const ObRow *row = NULL;
        const ObRow *result_row = NULL;
        const ObObj *cell = NULL;
        const ObObj *result_cell = NULL;
        uint64_t table_id = OB_INVALID_ID;
        uint64_t column_id = OB_INVALID_ID;

        ObFileTable result("tablet_join_test_data/fetch_ups_row_result.ini");
        OK(result.open());
        OK(tablet_join.open());

        for(int i=0;i<7;i++)
        {
          OK(tablet_join.get_next_row(row)) << "row:" << i << std::endl;
          OK(result.get_next_row(result_row));

          for(int j=1;j<4;j++)
          {
            OK(row->raw_get_cell(j, cell, table_id, column_id));
            OK(result_row->raw_get_cell(j, result_cell, table_id, column_id));
            bool eq = (*cell) == (*result_cell);
            if(!eq)
            {
              TBSYS_LOG(WARN, "================row idx[%d] column idx[%d]", i, j);
              TBSYS_LOG(WARN, "cell:%s", print_obj(*cell));
              TBSYS_LOG(WARN, "result_cell:%s", print_obj(*result_cell));
            }
            ASSERT_TRUE(eq);
          }
        }
        OK(result.close());
        OK(tablet_join.close());

      }

      void ObTabletJoinTest::test_big_data(ObTabletJoin &tablet_join)
      {
        system("python gen_test_data.py join");

        ObTabletJoin::TableJoinInfo tablet_join_info;
        tablet_join_info.left_table_id_ = LEFT_TABLE_ID;
        tablet_join_info.right_table_id_ = RIGHT_TABLE_ID;

        ObTabletJoin::JoinInfo join_info;

        OK(tablet_join_info.join_condition_.push_back(1));

        OK(read_join_info(tablet_join_info, "tablet_join_test_data/join_info.ini"));

        tablet_join.set_table_join_info(tablet_join_info);

        ObFakeUpsMultiGet fake_ups_multi_get("tablet_join_test_data/big_ups_scan2.ini");
        ObFileTable result("tablet_join_test_data/big_result2.ini");

        ObFakeSSTableScan sstable("tablet_join_test_data/big_sstable2.ini");
        ObFakeUpsScan ups_scan("tablet_join_test_data/none_ups.ini");

        ObTabletScanFuse tablet_fuse;
        OK(tablet_fuse.set_sstable_scan(&sstable));
        OK(tablet_fuse.set_incremental_scan(&ups_scan));

        tablet_join.set_child(0, tablet_fuse);
        tablet_join.set_batch_count(30000);
        tablet_join.set_ups_multi_get(&fake_ups_multi_get);
        
        OK(sstable.open());
        OK(sstable.close());

        uint64_t table_id = OB_INVALID_ID;
        uint64_t column_id = OB_INVALID_ID;

        const ObRowDesc *row_desc = NULL;
        sstable.get_row_desc(row_desc);
        for(int64_t i=0;i<row_desc->get_column_num();i++)
        {
          row_desc->get_tid_cid(i, table_id, column_id);
          tablet_join.add_column_id(column_id);
        }

        ObGetParam get_param(true);

        const ObRow *row = NULL;
        const ObRow *result_row = NULL;
        const ObObj *value = NULL;
        const ObObj *result_value = NULL;
        
        OK(result.open());
        OK(tablet_join.open());

        int err = OB_SUCCESS;
        int64_t count = 0;
        
        while(OB_SUCCESS == (err = tablet_join.get_next_row(row)))
        {
          OK(result.get_next_row(result_row));
          count ++;

          for(int64_t i=0;i<row->get_column_num();i++)
          {
            OK(row->raw_get_cell(i, value, table_id, column_id));
            OK(result_row->get_cell(table_id, column_id, result_value));

            if( *value != *result_value )
            {
              printf("row:[%ld], column[%ld]===========\n", count, i);

              ObString rowkey;
              get_rowkey(*row, rowkey);
              printf("row rowkey: %.*s\n", rowkey.length(), rowkey.ptr());
              printf("row: %s\n", print_obj(*value));
              get_rowkey(*result_row, rowkey);
              printf("result rowkey: %.*s\n", rowkey.length(), rowkey.ptr());
              printf("result: %s\n", print_obj(*result_value));
            }

            ASSERT_TRUE((*value) == (*result_value));
          }
        }
        ASSERT_TRUE(OB_SUCCESS == err || OB_ITER_END == err);

        OK(result.close());
        OK(tablet_join.close());

      }

      int ObTabletJoinTest::get_rowkey(const ObRow &row, ObString &rowkey)
      {
        int ret = OB_SUCCESS;
        const ObObj *cell = NULL;
        uint64_t table_id = OB_INVALID_ID;
        uint64_t column_id = OB_INVALID_ID;

        if(OB_SUCCESS != (ret = row.raw_get_cell(0, cell, table_id, column_id)))
        {
          TBSYS_LOG(WARN, "raw get cell fail:ret[%d]", ret);
        }
        else if(OB_SUCCESS != (ret = cell->get_varchar(rowkey)))
        {
          TBSYS_LOG(WARN, "get varchar:ret[%d]", ret);
        }

        return ret;
      }

      void ObTabletJoinTest::SetUp()
      {
        tablet_join_info_.left_table_id_ = LEFT_TABLE_ID;
        tablet_join_info_.right_table_id_ = RIGHT_TABLE_ID;

        ObTabletJoin::JoinInfo join_info;

        OK(tablet_join_info_.join_condition_.push_back(1));

        join_info.left_column_id_ = 4;
        join_info.right_column_id_ = 2;
        OK(tablet_join_info_.join_column_.push_back(join_info));

        join_info.left_column_id_ = 5;
        join_info.right_column_id_ = 3;
        OK(tablet_join_info_.join_column_.push_back(join_info));

        join_info.left_column_id_ = 6;
        join_info.right_column_id_ = 4;
        OK(tablet_join_info_.join_column_.push_back(join_info));
      }

      void ObTabletJoinTest::TearDown()
      {
      }

      int ObTabletJoinTest::read_join_info(ObTabletJoin::TableJoinInfo &tablet_join_info, const char *file_name)
      {
        int ret = OB_SUCCESS;
        FILE *fp = NULL; 
        char buf[1024];
        char *tokens[100];
        int32_t count = 0;
        ObTabletJoin::JoinInfo join_info;

        if(NULL == file_name)
        {
          ret = OB_ERROR;
          TBSYS_LOG(WARN, "file_name is null");
        }
        else if(NULL == (fp = fopen(file_name, "r")))
        {
          ret = OB_ERROR;
          TBSYS_LOG(WARN, "open file[%s] fail", file_name);
        }
        else if(NULL == fgets(buf, 1024, fp))
        {
          ret = OB_ERROR;
          TBSYS_LOG(WARN, "fgets fail");
        }
        else
        {
          int num = atoi(buf);
          for(int i=0;i<num;i++)
          {
            if(NULL == fgets(buf, 1024, fp))
            {
              ret = OB_ERROR;
              TBSYS_LOG(WARN, "fgets fail");
            }
            else
            {
              split(buf, " ", tokens, count);
              if(2 != count)
              {
                ret = OB_ERROR;
                TBSYS_LOG(WARN, "count[%d]", count);
              }
              else
              {
                join_info.left_column_id_ = atoi(tokens[0]);
                join_info.right_column_id_ = atoi(tokens[1]);
                if(OB_SUCCESS != (ret = tablet_join_info.join_column_.push_back(join_info)))
                {
                  TBSYS_LOG(WARN, "add join column fail:ret[%d]", ret);
                }
              }
            }
          }
        }

        if(NULL != fp)
        {
          fclose(fp);
        }
        return ret;
      }

      TEST_F(ObTabletJoinTest, direct_big_data_test)
      {
        ObTabletDirectJoin tablet_join;
        test_big_data(tablet_join);
      }

      TEST_F(ObTabletJoinTest, cache_big_data_test)
      {
        ObTabletCacheJoin tablet_join;
        test_big_data(tablet_join);
      }

      TEST_F(ObTabletJoinTest, direct_get_next_row)
      {
        ObTabletDirectJoin tablet_join;
        test_get_next_row(tablet_join);
      }

      TEST_F(ObTabletJoinTest, cache_get_next_row)
      {
        ObTabletCacheJoin tablet_join;
        test_get_next_row(tablet_join);
      }

      TEST_F(ObTabletJoinTest, fetch_ups_row)
      {
        ObTabletCacheJoin tablet_join;
        tablet_join.set_table_join_info(tablet_join_info_);

        ObFakeUpsMultiGet fake_ups_multi_get("tablet_join_test_data/fetch_ups_row_ups_row.ini");

        ObFakeSSTableScan sstable("tablet_join_test_data/fetch_ups_row_fused_row.ini");
        ObFakeUpsScan ups_scan("tablet_join_test_data/none_ups.ini");

        ObTabletScanFuse tablet_fuse;
        OK(tablet_fuse.set_sstable_scan(&sstable));
        OK(tablet_fuse.set_incremental_scan(&ups_scan));

        tablet_join.set_child(0, tablet_fuse);
        tablet_join.set_batch_count(3);
        tablet_join.set_ups_multi_get(&fake_ups_multi_get);

        ObGetParam get_param(true);

        OK(tablet_join.open());

        OK(tablet_join.fetch_fused_row(&get_param));
        OK(tablet_join.fetch_ups_row(get_param));

        uint64_t right_table_id = get_param[0]->table_id_;

        char rowkey_buf[100];
        ObString rowkey_str;
        ObObj rowkey_obj;
        ObRowkey rowkey;
        ObString value;
        ObUpsRow ups_row;
        const ObObj *cell = NULL;
        uint64_t table_id = OB_INVALID_ID;
        uint64_t column_id = OB_INVALID_ID;
        int64_t int_value = 0;

        ups_row.set_row_desc(tablet_join.ups_row_desc_);

        for(int i=0;i<3;i++)
        {
          sprintf(rowkey_buf, "chen%d", i+1);
          printf("rowkey:%s\n", rowkey_buf);

          rowkey_str.assign_ptr(rowkey_buf, (int32_t)strlen(rowkey_buf));
          rowkey_obj.set_varchar(rowkey_str);
          rowkey.assign(&rowkey_obj, 1);

          OK(tablet_join.ups_row_cache_.get(rowkey, value));
          OK(ObUpsRowUtil::convert(right_table_id, value, ups_row));
          ASSERT_EQ(3, ups_row.get_column_num());

          for(int j=0;j<3;j++)
          {
            OK(ups_row.raw_get_cell(j, cell, table_id, column_id));
            cell->get_int(int_value);
            ASSERT_EQ(2, int_value);
          }
        }

        OK(tablet_join.close());
      }
      
      TEST_F(ObTabletJoinTest, get_right_table_rowkey)
      {
        ObRowDesc row_desc;
        row_desc.add_column_desc(LEFT_TABLE_ID, 1);

        const char *rowkey_str = "oceanbase";
        ObString row_key_str;
        row_key_str.assign_ptr(const_cast<char *>(rowkey_str), (int32_t)strlen(rowkey_str));

        ObObj row_key_obj;
        row_key_obj.set_varchar(row_key_str);

        ObRowkey row_key;
        row_key.assign(&row_key_obj, 1);

        ObObj value;
        value.set_varchar(row_key_str);

        ObRow row;
        row.set_row_desc(row_desc);
        row.raw_set_cell(0, value);

        ObObj rowkey_obj[OB_MAX_ROWKEY_COLUMN_NUMBER];
        ObRowkey rowkey2;

        ObTabletCacheJoin tablet_join;
        tablet_join.set_table_join_info(tablet_join_info_);

        OK(tablet_join.get_right_table_rowkey(row, rowkey2, rowkey_obj));
        ASSERT_TRUE(row_key == rowkey2);
      }

      TEST_F(ObTabletJoinTest, gen_ups_row_desc)
      {
        ObTabletCacheJoin tablet_join;
        tablet_join.set_table_join_info(tablet_join_info_);

        tablet_join.gen_ups_row_desc();

        ASSERT_EQ(0, tablet_join.ups_row_desc_.get_idx(RIGHT_TABLE_ID, 2));
        ASSERT_EQ(1, tablet_join.ups_row_desc_.get_idx(RIGHT_TABLE_ID, 3));
        ASSERT_EQ(2, tablet_join.ups_row_desc_.get_idx(RIGHT_TABLE_ID, 4));

      }

      TEST_F(ObTabletJoinTest, compose_get_param)
      {
        ObTabletCacheJoin tablet_join;
        tablet_join.set_table_join_info(tablet_join_info_);

        ObGetParam get_param;
        ObObj row_key_obj;
        ObString row_key_str;        
        ObRowkey row_key;

        get_param.reset(true);

        row_key_str = ObString::make_string("oceanbase");
        row_key_obj.set_varchar(row_key_str);
        row_key.assign(&row_key_obj, 1);

        tablet_join.compose_get_param(RIGHT_TABLE_ID, row_key, get_param);
        ASSERT_EQ(3, get_param.get_cell_size());

        tablet_join.compose_get_param(RIGHT_TABLE_ID, row_key, get_param);
        ASSERT_EQ(3, get_param.get_cell_size());

        row_key_str = ObString::make_string("ob");
        row_key_obj.set_varchar(row_key_str);
        row_key.assign(&row_key_obj, 1);

        tablet_join.compose_get_param(RIGHT_TABLE_ID, row_key, get_param);
        ASSERT_EQ(6, get_param.get_cell_size());
      }
    }
  }
}


int main(int argc, char **argv)
{
  ob_init_memory_pool();
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}


