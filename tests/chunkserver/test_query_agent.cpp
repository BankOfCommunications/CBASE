/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or 
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *  
 * test_query_agent.cpp for test of query agent.
 *
 * Authors:
 *   huating <huating.zmq@taobao.com>
 *
 */
#include <gtest/gtest.h>
#include "common/ob_cell_array.h"
#include "common/ob_define.h"
#include "common/ob_string.h"
#include "common/ob_action_flag.h"
#include "common/ob_groupby.h"
#include "ob_query_agent.h"
#include "../common/test_rowkey_helper.h"

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::chunkserver;
using namespace oceanbase::common::ObExpression;
using namespace testing;
using namespace std;

namespace oceanbase
{
  namespace tests
  {
    namespace chunkserver 
    {
      static const int64_t table_id = 100;
      static const int64_t DISK_NUM = 12;
      static const int64_t SSTABLE_NUM = DISK_NUM * 2;
      static const int64_t SSTABLE_ROW_NUM = 100;
      static const int64_t ROW_NUM = SSTABLE_NUM * SSTABLE_ROW_NUM;
      static const int64_t COL_NUM = 6;
      static const int64_t MAX_MEMORY_SIZE = 1024 * 1024 * 1024;

      static ObCellInfo** cell_infos;
      static char* row_key_strs[ROW_NUM][COL_NUM];
      static ObMergerRpcProxy rpc_proxy;
      static ObCellArray cell_array;
      static ObQueryAgent query_agent(rpc_proxy);
      static ObScanParam scan_param;
      static ObComposeOperator compose_operator;
      static CharArena allocator_;

      class TestQueryAgent : public testing::Test
      {
      public:
        void check_string(const ObString& expected, const ObString& real)
        {
          if (NULL != expected.ptr() && NULL != real.ptr())
          {
            EXPECT_EQ(expected.length(), real.length());
            EXPECT_EQ(0, memcmp(expected.ptr(), real.ptr(), expected.length()));
          }
          else if (expected.length() == 0 && expected.ptr() == NULL)
          {
            // do nothing
          }
          else
          {
            EXPECT_EQ(expected.length(), real.length());
            EXPECT_EQ((const char*) NULL, expected.ptr());
            EXPECT_EQ((const char*) NULL, real.ptr());
          }
        }
        
        void check_obj(const ObObj& real, int type, int64_t exp_val)
        {
          int64_t real_val;

          if (ObMinType != type)
          {
            EXPECT_EQ(type, real.get_type());
          }
          if (ObIntType == type)
          {
            real.get_int(real_val);
            EXPECT_EQ(exp_val, real_val);
          }
        }
        
        void check_cell(const ObCellInfo& expected, const ObInnerCellInfo& real, 
                        int type=ObIntType, uint64_t column_id = UINT64_MAX)
        {
          int64_t exp_val = 0;

          if (UINT64_MAX == column_id)
          {
            ASSERT_EQ(expected.column_id_, real.column_id_);
          }
          else
          {
            ASSERT_EQ(column_id, real.column_id_);
          }
          ASSERT_EQ(expected.table_id_, real.table_id_);
          EXPECT_EQ(expected.row_key_, real.row_key_);

          if (ObVarcharType == type)
          {
            ObString exp_val;
            ObString real_val;
            expected.value_.get_varchar(exp_val);
            real.value_.get_varchar(real_val);
            check_string(exp_val, real_val);
          }
          else
          {
            if (ObIntType == type)
            {
              expected.value_.get_int(exp_val);
            }
            check_obj(real.value_, type, exp_val);
          }
        }
      
        void check_cell(const ObCellInfo& expected, const ObCellInfo& real, 
                        int type=ObIntType, uint64_t column_id = UINT64_MAX)
        {
          int64_t exp_val = 0;

          if (UINT64_MAX == column_id)
          {
            ASSERT_EQ(expected.column_id_, real.column_id_);
          }
          else
          {
            ASSERT_EQ(column_id, real.column_id_);
          }
          ASSERT_EQ(expected.table_id_, real.table_id_);
          EXPECT_EQ(expected.row_key_, real.row_key_);

          if (ObVarcharType == type)
          {
            ObString exp_val;
            ObString real_val;
            expected.value_.get_varchar(exp_val);
            real.value_.get_varchar(real_val);
            check_string(exp_val, real_val);
          }
          else
          {
            if (ObIntType == type)
            {
              expected.value_.get_int(exp_val);
            }
            check_obj(real.value_, type, exp_val);
          }
        }

      public:
        void init_cell_array(const int64_t row_index = 0, 
                             const int64_t row_count = 1,
                             const int64_t column_count = COL_NUM,
                             const int64_t compose_count = 0)
        {
          int ret = OB_SUCCESS;
          ObInnerCellInfo* cell_out = NULL;
          ObCellInfo fake_cell;

          cell_array.clear();
          for (int64_t i = row_index; i < row_index + row_count; ++i)
          {
            for (int64_t j = 0; j < column_count + compose_count; j++)
            {
              if (j >= column_count)
              {
                fake_cell = cell_infos[i][0];
                fake_cell.column_id_ = OB_INVALID_ID;
                fake_cell.value_.set_null();
                ret = cell_array.append(fake_cell, cell_out);
              }
              else
              {
                ret = cell_array.append(cell_infos[i][j], cell_out);
              }
              EXPECT_EQ(OB_SUCCESS, ret);
            }
          }
        }

        void init_scan_param(const int64_t row_index = 0, 
                             const int64_t row_count = 1,
                             const int64_t column_count = COL_NUM,
                             const int64_t return_count = COL_NUM,
                             const int64_t limit_offset = 0,
                             const int64_t limit_count = 0)
        {
          int ret = OB_SUCCESS;
          ObNewRange scan_range;
          ObString table_name;

          scan_param.reset();
          scan_range.table_id_ = table_id;
          scan_range.start_key_ = cell_infos[row_index][0].row_key_;
          scan_range.end_key_ = cell_infos[row_index + row_count - 1][0].row_key_;
          scan_range.border_flag_.set_inclusive_start();
          scan_range.border_flag_.set_inclusive_end();
          
          ret = scan_param.set(table_id, table_name, scan_range);
          EXPECT_EQ(OB_SUCCESS, ret);
          if (limit_offset > 0 || limit_count > 0)
          {
            ret = scan_param.set_limit_info(limit_offset, limit_count);
            EXPECT_EQ(OB_SUCCESS, ret);
          }
          for (int64_t j = 0; j < column_count; ++j)
          {
            bool is_return = j < return_count ? true : false;
            ret = scan_param.add_column(cell_infos[0][j].column_id_, is_return);
            EXPECT_EQ(OB_SUCCESS, ret);
          }
        }

        void init_comp_scan_param(const int64_t compose_count)
        {
          int ret = OB_SUCCESS;
          ObObj exp[20];

          if (compose_count > 0)
          {
            //column 0 + column 1
            exp[0].set_int(COLUMN_IDX);
            exp[1].set_int(0);
            exp[2].set_int(COLUMN_IDX);
            exp[3].set_int(1);
            exp[4].set_int(OP);
            exp[5].set_int(ADD_FUNC);
            exp[6].set_int(END);
            ret = scan_param.add_column(exp);
            EXPECT_EQ(OB_SUCCESS, ret);
          }

          if (compose_count > 1)
          {
            //column 2 * column 3
            exp[0].set_int(COLUMN_IDX);
            exp[1].set_int(2);
            exp[2].set_int(COLUMN_IDX);
            exp[3].set_int(3);
            exp[4].set_int(OP);
            exp[5].set_int(MUL_FUNC);
            exp[6].set_int(END);
            ret = scan_param.add_column(exp);
            EXPECT_EQ(OB_SUCCESS, ret);
          }
        }

        void init_filter_scan_param(const int64_t column_idx, 
                                    const ObLogicOperator cond_op,
                                    const int64_t val)
        {
          int ret = OB_SUCCESS;
          ObObj cond_val;

          cond_val.set_int(val);
          ret = scan_param.get_filter_info().add_cond(column_idx, cond_op, cond_val);
          EXPECT_EQ(OB_SUCCESS, ret);
        }

        void init_groupby_param()
        {
          int ret = OB_SUCCESS;

          //group by sstable number
          ret = scan_param.get_group_by_param().add_groupby_column(1);
          EXPECT_EQ(OB_SUCCESS, ret);

          ret = scan_param.get_group_by_param().add_return_column(0);
          EXPECT_EQ(OB_SUCCESS, ret);

          ret = scan_param.get_group_by_param().add_aggregate_column(2, COUNT);
          EXPECT_EQ(OB_SUCCESS, ret);
          ret = scan_param.get_group_by_param().add_aggregate_column(0, SUM);
          EXPECT_EQ(OB_SUCCESS, ret);
        }

        void init_comp_groupby_param(const int64_t compose_count)
        {
          int ret = OB_SUCCESS;
          ObObj exp[20];

          if (compose_count > 0)
          {
            //column 0 + column 1
            exp[0].set_int(COLUMN_IDX);
            exp[1].set_int(0);
            exp[2].set_int(COLUMN_IDX);
            exp[3].set_int(1);
            exp[4].set_int(OP);
            exp[5].set_int(ADD_FUNC);
            exp[6].set_int(END);
            ret = scan_param.get_group_by_param().add_column(exp);
            EXPECT_EQ(OB_SUCCESS, ret);
          }

          if (compose_count > 1)
          {
            //column 2 * column 3
            exp[0].set_int(COLUMN_IDX);
            exp[1].set_int(2);
            exp[2].set_int(COLUMN_IDX);
            exp[3].set_int(3);
            exp[4].set_int(OP);
            exp[5].set_int(MUL_FUNC);
            exp[6].set_int(END);
            ret = scan_param.get_group_by_param().add_column(exp);
            EXPECT_EQ(OB_SUCCESS, ret);
          }
        }

        void init_orderby_scan_param(ObScanParam::Order order)
        {
          int ret = OB_SUCCESS;
          ret = scan_param.add_orderby_column(0, order);
          EXPECT_EQ(OB_SUCCESS, ret);
        }

        void test_return_org_input(const int64_t row_index = 0, 
                                   const int64_t row_count = 1,
                                   const int64_t column_count = COL_NUM,
                                   const int64_t return_count = COL_NUM,
                                   const int64_t limit_offset = 0,
                                   const int64_t limit_count = 0,
                                   const int64_t compose_count = 0)
        {
          int ret = OB_SUCCESS;
          ObInnerCellInfo* cell_info;
          ObCellInfo expected;
          int64_t count = 0;
          int64_t exp_ret_count = 0;
          int64_t row_idx = 0;
          int64_t col_idx = 0;
          int64_t val_1 = 0;
          int64_t val_2 = 0;
          int64_t exp_val = 0;

          init_cell_array(row_index, row_count, column_count, compose_count);
          init_scan_param(row_index, row_count, column_count, return_count, 
                          limit_offset, limit_count);
          if (compose_count > 0)
          {
            init_comp_scan_param(compose_count);
            ret = compose_operator.start_compose(
              cell_array, 
              scan_param.get_composite_columns(), 
              column_count + compose_count);
            EXPECT_EQ(OB_SUCCESS, ret);
          }
          ret = query_agent.start_agent(cell_array, scan_param, MAX_MEMORY_SIZE);
          EXPECT_EQ(OB_SUCCESS, ret);

          while (true)
          {
            ret = query_agent.next_cell();
            if (OB_SUCCESS == ret)
            {
              ret = query_agent.get_cell(&cell_info);
              EXPECT_EQ(OB_SUCCESS, ret);
              row_idx = row_index + limit_offset + count / (return_count + compose_count);
              col_idx = count % (return_count + compose_count);
              if (row_idx < ROW_NUM && col_idx < COL_NUM)
              {
                expected = cell_infos[row_idx][col_idx];
              }
              if (compose_count == 0)
              {
                if (col_idx == 4)
                {
                  check_cell(expected, *cell_info, ObVarcharType);
                }
                else if (col_idx > 4)
                {
                  check_cell(expected, *cell_info, ObNullType);
                }
                else
                {
                  check_cell(expected, *cell_info);
                }
              }
              else
              {
                if (col_idx >= return_count)
                {
                  //composite column
                  if ((col_idx - return_count) % compose_count == 0)
                  {
                    ret = cell_infos[row_idx][0].value_.get_int(val_1);
                    EXPECT_EQ(OB_SUCCESS, ret);
                    ret = cell_infos[row_idx][1].value_.get_int(val_2);
                    EXPECT_EQ(OB_SUCCESS, ret);
                    exp_val = val_1 + val_2;
                    expected.value_.set_int(exp_val);
                    expected.column_id_ = OB_INVALID_ID;
                    expected.table_id_ = table_id;
                    check_cell(expected, *cell_info);
                  }
                  else
                  {
                    ret = cell_infos[row_idx][2].value_.get_int(val_1);
                    EXPECT_EQ(OB_SUCCESS, ret);
                    ret = cell_infos[row_idx][3].value_.get_int(val_2);
                    EXPECT_EQ(OB_SUCCESS, ret);
                    exp_val = val_1 * val_2;
                    expected.value_.set_int(exp_val);
                    expected.column_id_ = OB_INVALID_ID;
                    expected.table_id_ = table_id;
                    check_cell(expected, *cell_info);
                  }
                }
                else
                {
                  if (return_count >= 5 && col_idx == 4)
                  {
                    check_cell(expected, *cell_info, ObVarcharType);
                  }
                  else if (return_count == COL_NUM && col_idx == 5)
                  {
                    check_cell(expected, *cell_info, ObNullType);
                  }
                  else
                  {
                    check_cell(expected, *cell_info);
                  }
                }
              }
              count++;
            }
            else if (OB_ITER_END == ret)
            {
              ret = query_agent.next_cell();
              EXPECT_EQ(OB_ITER_END, ret);
              break;
            }
            else
            {
              break;
            }
          }

          EXPECT_EQ(OB_ITER_END, ret);
          exp_ret_count = row_count > limit_offset ? 
            (row_count - limit_offset) * (return_count + compose_count) : 0;
          exp_ret_count = (limit_count > 0 && exp_ret_count > (return_count + compose_count) * limit_count) 
            ? (return_count + compose_count) * limit_count : exp_ret_count;
          EXPECT_EQ(exp_ret_count, count);
        }

        void test_groupby(const int64_t row_index = 0, 
                          const int64_t row_count = 1,
                          const int64_t column_count = COL_NUM,
                          const int64_t return_count = COL_NUM,
                          const int64_t filter_count = 0,
                          const bool is_aggreate = false,
                          const int64_t compose_count = 0,
                          ObScanParam::Order order = ObScanParam::ASC)
        {
          int ret = OB_SUCCESS;
          ObInnerCellInfo* cell_info;
          ObCellInfo expected;
          int64_t count = 0;
          int64_t exp_ret_count = 0;
          int64_t row_idx = 0;
          int64_t col_idx = 0;
          int64_t val_1 = 0;
          int64_t val_2 = 0;
          int64_t exp_val = 0;
          int64_t row_start = 0;
          int64_t groupby_ret_count = 0;

          init_cell_array(row_index, row_count, column_count);
          init_scan_param(row_index, row_count, column_count);
          if (filter_count > 0)
          {
            init_filter_scan_param(0, GT, 5);
          }
          if (filter_count > 1)
          {
            init_filter_scan_param(1, EQ, 0);
            EXPECT_EQ(2, scan_param.get_filter_info().get_count());
          }
          if (is_aggreate)
          {
            init_groupby_param();
          }

          if (compose_count > 0)
          {
            init_comp_groupby_param(compose_count);
          }
          init_orderby_scan_param(order);

          ret = query_agent.start_agent(cell_array, scan_param, MAX_MEMORY_SIZE);
          EXPECT_EQ(OB_SUCCESS, ret);

          if (row_index - 5 > 0)
          {
            row_start = row_index;
          }
          else if (row_index - 5 <= 0 && row_index + row_count > 6)
          {
            row_start = 6;
          }
          else if (row_index + row_count <= 6)
          {
            row_start = 0;
          }

          groupby_ret_count = scan_param.get_group_by_param().get_aggregate_row_width();

          //caculate the expected column count
          if (groupby_ret_count > 0)
          {
            if (row_index + row_count > 5)
            {
              exp_ret_count = ((row_index + row_count - 6) / SSTABLE_ROW_NUM + 1) * groupby_ret_count;
            }
            else
            {
              exp_ret_count = 0;
            }
            if (filter_count > 1)
            {
              if (row_index + row_count > 5)
              {
                exp_ret_count = groupby_ret_count;
              }
              else
              {
                exp_ret_count = 0;
              }
            }
          }
          else
          {
            if (row_index - 5 > 0)
            {
              exp_ret_count = row_count * (return_count + compose_count);
            }
            else if (row_index - 5 <= 0 && row_index + row_count > 6)
            {
              exp_ret_count = (row_index + row_count - 6) * (return_count + compose_count);
            }
            else if (row_index + row_count <= 6)
            {
              exp_ret_count = 0;
            }
            if (filter_count > 1)
            {
              exp_ret_count = row_start + row_count >= SSTABLE_ROW_NUM
                ? (SSTABLE_ROW_NUM - 6) * (return_count + compose_count) : exp_ret_count;
            }
          }

          while (true)
          {
            ret = query_agent.next_cell();
            if (OB_SUCCESS == ret)
            {
              ret = query_agent.get_cell(&cell_info);
              EXPECT_EQ(OB_SUCCESS, ret);
              expected.reset();
              if (groupby_ret_count == 0)
              {
                row_idx = row_start + count / (return_count + compose_count);
                col_idx = count % (return_count + compose_count);
                if (row_idx < ROW_NUM && col_idx < COL_NUM)
                {
                  expected = cell_infos[row_idx][col_idx];
                }
              }
              else
              {
                if (ObScanParam::DESC != order)
                {
                  row_idx = count / groupby_ret_count * SSTABLE_ROW_NUM;
                }
                else
                {
                  row_idx = (exp_ret_count - count - 1) / groupby_ret_count * SSTABLE_ROW_NUM;
                }
                col_idx = count % groupby_ret_count;
                if (row_index < 6 && row_index + row_count > 6 && row_idx == 0)
                {
                  row_idx = 6;
                }
              }

              if (groupby_ret_count > 0)
              {
                int64_t sst_end_idx = (row_idx / SSTABLE_ROW_NUM + 1) * SSTABLE_ROW_NUM;
                int64_t act_row_cnt = sst_end_idx - row_idx;
                if (col_idx == 0)
                {
                  expected = cell_infos[row_idx][1];
                }
                else if (col_idx == 1)
                {
                  expected = cell_infos[row_idx][0];
                }
                else if (col_idx == 2)
                {
                  //count
                  if (sst_end_idx <= row_index + row_count)
                  {
                    exp_val = act_row_cnt;
                  }
                  else if (sst_end_idx > row_index + row_count
                           && row_idx < row_index + row_count)
                  {
                    exp_val = row_index + row_count - row_idx;
                  }
                  expected.value_.set_int(exp_val);
                  expected.column_id_ = OB_INVALID_ID;
									expected.row_key_ = cell_infos[row_idx][0].row_key_;
                  expected.table_id_ = cell_infos[row_idx][0].table_id_;
                }
                else if (col_idx == 3)
                {
                  //sum
                  if (sst_end_idx <= row_index + row_count)
                  {
                    exp_val = act_row_cnt * (row_idx + sst_end_idx - 1) / 2;
                  }
                  else if (sst_end_idx > row_index + row_count
                           && row_idx < row_index + row_count)
                  {
                    act_row_cnt = row_index + row_count - row_idx;
                    exp_val = act_row_cnt * (row_idx + row_index + row_count - 1) / 2;
                  }
                  expected.value_.set_int(exp_val);
                  expected.column_id_ = OB_INVALID_ID;
									expected.row_key_ = cell_infos[row_idx][0].row_key_;
									expected.table_id_ = cell_infos[row_idx][0].table_id_;
                }
                else if (col_idx == 4)
                {
                  //composite column
                  ret = cell_infos[row_idx][0].value_.get_int(val_1);
                  EXPECT_EQ(OB_SUCCESS, ret);
                  ret = cell_infos[row_idx][1].value_.get_int(val_2);
                  EXPECT_EQ(OB_SUCCESS, ret);
                  exp_val = val_1 + val_2;
                  expected.value_.set_int(exp_val);
                  expected.column_id_ = OB_INVALID_ID;
									expected.row_key_ = cell_infos[row_idx][0].row_key_;
						      expected.table_id_ = cell_infos[row_idx][0].table_id_;
                }
                else if (col_idx == 5)
                {
                  //composite column
                  if (sst_end_idx <= row_index + row_count)
                  {
                    val_1 = act_row_cnt;
                  }
                  else if (sst_end_idx > row_index + row_count
                           && row_idx < row_index + row_count)
                  {
                    val_1 = row_index + row_count - row_idx;
                  }
                  if (sst_end_idx <= row_index + row_count)
                  {
                    val_2 = act_row_cnt * (row_idx + sst_end_idx - 1) / 2;
                  }
                  else if (sst_end_idx > row_index + row_count
                           && row_idx < row_index + row_count)
                  {
                    act_row_cnt = row_index + row_count - row_idx;
                    val_2 = act_row_cnt * (row_idx + row_index + row_count - 1) / 2;
                  }
                  exp_val = val_1 * val_2;
                  expected.value_.set_int(exp_val);
                  expected.column_id_ = OB_INVALID_ID;
									expected.row_key_ = cell_infos[row_idx][0].row_key_;
									expected.table_id_ = cell_infos[row_idx][0].table_id_;
                }
                if (is_aggreate)
                {
                  expected.row_key_.assign(NULL, 0);
                  cell_info->row_key_.assign(NULL, 0);
                }
                check_cell(expected, *cell_info);
              }
              else
              {
                if (col_idx == 4)
                {
                  check_cell(expected, *cell_info, ObVarcharType);
                }
                else if (col_idx > 4)
                {
                  check_cell(expected, *cell_info, ObNullType);
                }
                else
                {
                  check_cell(expected, *cell_info);
                }
              }
              count++;
            }
            else if (OB_ITER_END == ret)
            {
              ret = query_agent.next_cell();
              EXPECT_EQ(OB_ITER_END, ret);
              break;
            }
            else
            {
              break;
            }
          }

          EXPECT_EQ(OB_ITER_END, ret);
          EXPECT_EQ(exp_ret_count, count);
        }

      public:
        static void init_cell_infos()
        {
          //malloc
          cell_infos = new ObCellInfo*[ROW_NUM];
          for (int64_t i = 0; i < ROW_NUM; ++i)
          {
            cell_infos[i] = new ObCellInfo[COL_NUM];
          }
  
          for (int64_t i = 0; i < ROW_NUM; ++i)
          {
            for (int64_t j = 0; j < COL_NUM; ++j)
            {
              row_key_strs[i][j] = new char[50];
            }
          }
  
          // init cell infos
          for (int64_t i = 0; i < ROW_NUM; ++i)
          {
            for (int64_t j = 0; j < COL_NUM; ++j)
            {
              cell_infos[i][j].table_id_ = table_id;
              sprintf(row_key_strs[i][j], "row_key_%08ld", i);
              cell_infos[i][j].row_key_ = make_rowkey(row_key_strs[i][j], &allocator_);
              cell_infos[i][j].column_id_ = j + 2;
              if (j == 0)
              {
                //column 0, row number
                cell_infos[i][j].value_.set_int(i);
              }
              else if (j == 1)
              {
                //column 1, sstable number
                cell_infos[i][j].value_.set_int(i / SSTABLE_ROW_NUM);
              }
              else if (j == 2)
              {
                //column 2, disk number
                cell_infos[i][j].value_.set_int(i / (SSTABLE_ROW_NUM* 2));
              }
              else if (j == 3)
              {
                //column 3, index number
                cell_infos[i][j].value_.set_int(i * COL_NUM + j);
              }
              else if (j == 4)
              {
                //column 4, row key
                cell_infos[i][j].value_.set_varchar(TestRowkeyHelper(cell_infos[i][j].row_key_, &allocator_));
              }
              else
              {
                //other column, null
                cell_infos[i][j].value_.set_null();
              }
            }
          }
        }
  
        static void destroy_cell_infos()
        {
          for (int64_t i = 0; i < ROW_NUM; ++i)
          {
            for (int64_t j = 0; j < COL_NUM; ++j)
            {
              if (NULL != row_key_strs[i][j])
              {
                delete[] row_key_strs[i][j];
                row_key_strs[i][j] = NULL;
              }
            }
          }
  
          for (int64_t i = 0; i < ROW_NUM; ++i)
          {
            if (NULL != cell_infos[i])
            {
              delete[] cell_infos[i];
              cell_infos[i] = NULL;
            }
          }
          if (NULL != cell_infos)
          {
            delete[] cell_infos;
          }
        }

      public:
        static void SetUpTestCase()
        {
          int err = OB_SUCCESS;

          TBSYS_LOGGER.setLogLevel("WARN");
          err = ob_init_memory_pool();
          ASSERT_EQ(OB_SUCCESS, err);

          init_cell_infos();
        }

        static void TearDownTestCase()
        {
          destroy_cell_infos();
        }

        virtual void SetUp()
        {

        }

        virtual void TearDown()
        {

        }
      };

      TEST_F(TestQueryAgent, test_return_all)
      {
        test_return_org_input(0, 1);
        test_return_org_input(0, 10);
        test_return_org_input(0, 100);
        test_return_org_input(0, ROW_NUM);

        test_return_org_input(0, 1, 3, 3);
        test_return_org_input(0, 10, 3, 3);
        test_return_org_input(0, 100, 3, 3);
        test_return_org_input(0, ROW_NUM, 3, 3);
      }

      TEST_F(TestQueryAgent, test_return_part)
      {
        test_return_org_input(0, 1, COL_NUM, COL_NUM - 1);
        test_return_org_input(0, 10, COL_NUM, COL_NUM - 1);
        test_return_org_input(0, 100, COL_NUM, COL_NUM - 1);
        test_return_org_input(0, ROW_NUM, COL_NUM, COL_NUM - 1);

        test_return_org_input(0, 1, 5, COL_NUM - 2);
        test_return_org_input(0, 10, 5, COL_NUM - 2);
        test_return_org_input(0, 100, 5, COL_NUM - 2);
        test_return_org_input(0, ROW_NUM, 5, COL_NUM - 2);
      }

      TEST_F(TestQueryAgent, test_limit_offset)
      {
        test_return_org_input(0, 1, COL_NUM, COL_NUM - 1, 1);
        test_return_org_input(0, 10, COL_NUM, COL_NUM - 1, 2);
        test_return_org_input(0, 100, COL_NUM, COL_NUM - 1, 3);
        test_return_org_input(0, ROW_NUM, COL_NUM, COL_NUM - 1, 40);

        test_return_org_input(0, 1, 5, COL_NUM - 2, 1);
        test_return_org_input(0, 10, 5, COL_NUM - 2, 2);
        test_return_org_input(0, 100, 5, COL_NUM - 2, 3);
        test_return_org_input(0, ROW_NUM, 5, COL_NUM - 2, 100);

        test_return_org_input(10, 1, COL_NUM, COL_NUM - 1, 2);
        test_return_org_input(100, 10, COL_NUM, COL_NUM - 1, 50);
        test_return_org_input(500, 100, COL_NUM, COL_NUM - 1, 500);
        test_return_org_input(1000, ROW_NUM - 1000, COL_NUM, COL_NUM - 1, 2000);
      }

      TEST_F(TestQueryAgent, test_limit_count)
      {
        test_return_org_input(0, 1, COL_NUM, COL_NUM - 1, 0, 1);
        test_return_org_input(0, 10, COL_NUM, COL_NUM - 1, 0, 8);
        test_return_org_input(0, 100, COL_NUM, COL_NUM - 1, 0, 100);
        test_return_org_input(0, 200, COL_NUM, COL_NUM - 1, 0, 500);
        test_return_org_input(0, ROW_NUM, COL_NUM, COL_NUM - 1, 0, ROW_NUM - 100);

        test_return_org_input(0, 1, 5, COL_NUM - 2, 0, 100);
        test_return_org_input(0, 10, 5, COL_NUM - 2, 0, 10);
        test_return_org_input(0, 100, 5, COL_NUM - 2, 0, 90);
        test_return_org_input(0, ROW_NUM, 5, COL_NUM - 2, 0, ROW_NUM);
      }

      TEST_F(TestQueryAgent, test_return_limit)
      {
        test_return_org_input(0, 1, COL_NUM, COL_NUM - 1, 1, 1);
        test_return_org_input(0, 10, COL_NUM, COL_NUM - 1, 2, 8);
        test_return_org_input(0, 100, COL_NUM, COL_NUM - 1, 3, 100);
        test_return_org_input(0, 200, COL_NUM, COL_NUM - 1, 40, 500);
        test_return_org_input(0, ROW_NUM, COL_NUM, COL_NUM - 1, 100, ROW_NUM - 100);

        test_return_org_input(0, 1, 5, COL_NUM - 2, 1, 100);
        test_return_org_input(0, 10, 5, COL_NUM - 2, 2, 10);
        test_return_org_input(0, 100, 5, COL_NUM - 2, 3, 90);
        test_return_org_input(0, ROW_NUM, 5, COL_NUM - 2, 100, ROW_NUM);
      }

      TEST_F(TestQueryAgent, test_compose)
      {
        test_return_org_input(0, 1, COL_NUM, COL_NUM, 1, 1, 1);
        test_return_org_input(0, 10, COL_NUM, COL_NUM, 2, 8, 1);
        test_return_org_input(0, 100, COL_NUM, COL_NUM, 3, 100, 1);
        test_return_org_input(0, 200, COL_NUM, COL_NUM, 40, 500, 1);
        test_return_org_input(0, ROW_NUM, COL_NUM, COL_NUM, 100, ROW_NUM - 100, 1);

        test_return_org_input(0, 1, COL_NUM, COL_NUM - 1, 1, 1, 2);
        test_return_org_input(0, 10, COL_NUM, COL_NUM - 1, 2, 8, 2);
        test_return_org_input(0, 100, COL_NUM, COL_NUM - 1, 3, 100, 2);
        test_return_org_input(0, 200, COL_NUM, COL_NUM - 1, 40, 500, 2);
        test_return_org_input(0, ROW_NUM, COL_NUM, COL_NUM - 1, 100, ROW_NUM - 100, 2);

        test_return_org_input(0, 1, 5, COL_NUM - 2, 1, 100, 2);
        test_return_org_input(0, 10, 5, COL_NUM - 2, 2, 10, 2);
        test_return_org_input(0, 100, 5, COL_NUM - 2, 3, 90, 2);
        test_return_org_input(0, ROW_NUM, 5, COL_NUM - 2, 100, ROW_NUM, 2);
      }

      TEST_F(TestQueryAgent, test_filter)
      {
        test_groupby(0, 1, COL_NUM, COL_NUM, 1);
        test_groupby(0, 10, COL_NUM, COL_NUM, 1);
        test_groupby(0, 100, COL_NUM, COL_NUM, 1);
        test_groupby(0, 200, COL_NUM, COL_NUM, 1);
        test_groupby(0, ROW_NUM, COL_NUM, COL_NUM, 1);

        test_groupby(0, 1, COL_NUM, COL_NUM, 2);
        test_groupby(0, 10, COL_NUM, COL_NUM, 2);
        test_groupby(0, 100, COL_NUM, COL_NUM, 2);
        test_groupby(0, 200, COL_NUM, COL_NUM, 2);
        test_groupby(0, ROW_NUM, COL_NUM, COL_NUM, 2);
      }

      TEST_F(TestQueryAgent, test_filter_aggreate)
      {
        test_groupby(0, 1, COL_NUM, COL_NUM, 1, true);
        test_groupby(0, 10, COL_NUM, COL_NUM, 1, true);
        test_groupby(0, 100, COL_NUM, COL_NUM, 1, true);
        test_groupby(0, 200, COL_NUM, COL_NUM, 1, true);
        test_groupby(0, ROW_NUM, COL_NUM, COL_NUM, 1, true);

        test_groupby(0, 1, COL_NUM, COL_NUM, 2, true);
        test_groupby(0, 10, COL_NUM, COL_NUM, 2, true);
        test_groupby(0, 100, COL_NUM, COL_NUM, 2, true);
        test_groupby(0, 200, COL_NUM, COL_NUM, 2, true);
        test_groupby(0, ROW_NUM, COL_NUM, COL_NUM, 2, true);
      }

      TEST_F(TestQueryAgent, test_filter_aggreate_compose)
      {
        test_groupby(0, 1, COL_NUM, COL_NUM, 1, true, 1);
        test_groupby(0, 10, COL_NUM, COL_NUM, 1, true, 1);
        test_groupby(0, 100, COL_NUM, COL_NUM, 1, true, 1);
        test_groupby(0, 200, COL_NUM, COL_NUM, 1, true, 1);
        test_groupby(0, ROW_NUM, COL_NUM, COL_NUM, 1, true, 1);

        test_groupby(0, 1, COL_NUM, COL_NUM, 2, true, 2);
        test_groupby(0, 10, COL_NUM, COL_NUM, 2, true, 2);
        test_groupby(0, 100, COL_NUM, COL_NUM, 2, true, 2);
        test_groupby(0, 200, COL_NUM, COL_NUM, 2, true, 2);
        test_groupby(0, ROW_NUM, COL_NUM, COL_NUM, 2, true, 2);
      }

      TEST_F(TestQueryAgent, test_filter_aggreate_compose_orderby)
      {
        test_groupby(0, 1, COL_NUM, COL_NUM, 1, true, 1, ObScanParam::DESC);
        test_groupby(0, 10, COL_NUM, COL_NUM, 1, true, 1, ObScanParam::DESC);
        test_groupby(0, 100, COL_NUM, COL_NUM, 1, true, 1, ObScanParam::DESC);
        test_groupby(0, 200, COL_NUM, COL_NUM, 1, true, 1, ObScanParam::DESC);
        test_groupby(0, ROW_NUM, COL_NUM, COL_NUM, 1, true, 1, ObScanParam::DESC);

        test_groupby(0, 1, COL_NUM, COL_NUM, 2, true, 2, ObScanParam::DESC);
        test_groupby(0, 10, COL_NUM, COL_NUM, 2, true, 2, ObScanParam::DESC);
        test_groupby(0, 100, COL_NUM, COL_NUM, 2, true, 2, ObScanParam::DESC);
        test_groupby(0, 200, COL_NUM, COL_NUM, 2, true, 2, ObScanParam::DESC);
        test_groupby(0, ROW_NUM, COL_NUM, COL_NUM, 2, true, 2, ObScanParam::DESC);
      }
    }//end namespace common
  }//end namespace tests
}//end namespace oceanbase

int main(int argc, char** argv)
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
