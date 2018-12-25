/*
 * (C) 2007-2012 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version:  ob_table_scan_test.cpp,  06/04/2012 05:47:34 PM xiaochu Exp $
 * 
 * Author:  
 *   xiaochu.yh <xiaochu.yh@taobao.com>
 * 
 */
#include "common/ob_malloc.h"
#include <gtest/gtest.h>
#include "common/ob_string_buf.h"
#include "sql/ob_sql_expression.h"
#include "common/ob_row.h"
#include "sql/ob_table_mem_scan.h"
#include "ob_fake_table.h"


using namespace oceanbase::sql;
using namespace oceanbase::sql::test;
using namespace oceanbase::common;

#define NEW_TABLE_ID 10

namespace oceanbase
{
  namespace sql
  {
    class ObTableMemScanTest: public ::testing::Test
    {
      public:
        ObTableMemScanTest();
        virtual ~ObTableMemScanTest();
        virtual void SetUp();
        virtual void TearDown();
        int add_output_column(ObTableMemScan &op, ObStringBuf &buf, uint64_t cid);
        int add_filter_expr(ObTableMemScan &op, ObStringBuf &buf, uint64_t cid1, ExprItem::ExprType type, uint64_t cid2);
      private:
        // disallow copy
        ObTableMemScanTest(const ObTableMemScanTest &other);
        ObTableMemScanTest& operator=(const ObTableMemScanTest &other);
      private:
        // data members
    };

    ObTableMemScanTest::ObTableMemScanTest()
    {
    }

    ObTableMemScanTest::~ObTableMemScanTest()
    {
    }

    int ObTableMemScanTest::add_output_column(ObTableMemScan &op, ObStringBuf &buf, uint64_t cid)
    {
      int ret = OB_SUCCESS;
      sql::ObSqlExpression p;

      ObExpression expr;
      ExprItem item_a;
      ObObj obj_a;

      item_a.type_ = ExprItem::T_REF_COLUMN;
      item_a.value_.cell_.tid = test::ObFakeTable::TABLE_ID + NEW_TABLE_ID;
      item_a.value_.cell_.cid = cid;
      expr.add_expr_item(item_a);

      p.set_table_id(test::ObFakeTable::TABLE_ID + NEW_TABLE_ID);
      p.set_column_id(cid);
      p.set_expression(expr);//, buf);

      ret = op.add_output_column(p);
      return ret; 
    }
    
    int ObTableMemScanTest::add_filter_expr(ObTableMemScan &op, ObStringBuf &buf, uint64_t cid1, ExprItem::ExprType type, uint64_t cid2)
    {
      int ret = OB_SUCCESS;
      sql::ObSqlExpression p;

      ObExpression expr;
      ExprItem item_a, item_b, item_op;
      ObObj obj_a, obj_b;

      item_a.type_ = ExprItem::T_REF_COLUMN;
      item_b.type_ = ExprItem::T_REF_COLUMN;
      item_op.type_ = type;
      item_a.value_.cell_.tid = test::ObFakeTable::TABLE_ID + NEW_TABLE_ID;
      item_a.value_.cell_.cid = cid1;
      item_b.value_.cell_.tid = test::ObFakeTable::TABLE_ID + NEW_TABLE_ID;
      item_b.value_.cell_.cid = cid2;
      item_op.value_.int_ = type;

      expr.add_expr_item(item_a);
      expr.add_expr_item(item_b);
      expr.add_expr_item(item_op);

      //p.set_table_id(test::ObFakeTable::TABLE_ID);
      //p.set_column_id(1);
      p.set_expression(expr);//, buf);

      ret = op.add_filter(p);

      return ret;
    }

    void ObTableMemScanTest::SetUp()
    {
      TBSYS_LOG(DEBUG, "PARAM: test::ObFakeTable::TABLE_ID=%ld, OB_APP_MIN_COLUMN_ID=%ld", test::ObFakeTable::TABLE_ID, OB_APP_MIN_COLUMN_ID );
    }

    void ObTableMemScanTest::TearDown()
    {
    }


    TEST_F(ObTableMemScanTest, fake_table_only_test)
    {
      ObFakeTable input_table;
      input_table.set_row_count(10);

      const ObRow *row= NULL;
      ObTableMemScan ts;
      ts.set_child(0, input_table);
      ts.set_table(test::ObFakeTable::TABLE_ID + NEW_TABLE_ID, test::ObFakeTable::TABLE_ID);
      ASSERT_EQ(OB_SUCCESS, ts.open());
      while(OB_SUCCESS == ts.get_next_row(row))
      {
        TBSYS_LOG(DEBUG, "got row");
      }
      ASSERT_EQ(OB_SUCCESS, ts.close());
    }

    TEST_F(ObTableMemScanTest, project_fake_table_test)
    {
      ObStringBuf buf;
      ObFakeTable input_table;
      input_table.set_row_count(10);

      const ObRow *row= NULL;
      ObTableMemScan ts;
      add_output_column(ts, buf, OB_APP_MIN_COLUMN_ID + 1);
      ts.set_child(0, input_table);
      ts.set_table(test::ObFakeTable::TABLE_ID + NEW_TABLE_ID, test::ObFakeTable::TABLE_ID);
      ASSERT_EQ(OB_SUCCESS, ts.open());
      while(OB_SUCCESS == ts.get_next_row(row))
      {
        const ObObj *obj = NULL;
        TBSYS_LOG(DEBUG, "got row");
        ASSERT_TRUE(NULL != row);
        row->get_cell(test::ObFakeTable::TABLE_ID + NEW_TABLE_ID, OB_APP_MIN_COLUMN_ID + 1, obj);
        ASSERT_TRUE(NULL != obj);
        obj->dump();    
      }
      ASSERT_EQ(OB_SUCCESS, ts.close());
    }
    
    TEST_F(ObTableMemScanTest, filter_project_fake_table_test)
    {
      ObStringBuf buf;
      ObFakeTable input_table;
      const int cycle = 3;
      input_table.set_row_count(6 * cycle);

      const ObRow *row= NULL;
      ObTableMemScan ts;
      /* c1: row_idx
       * c2: row_idx%2 
       * c3: row_idx%3 
       * c4: row_idx/2 
       */
      add_output_column(ts, buf, OB_APP_MIN_COLUMN_ID + 1);
      ASSERT_EQ(OB_SUCCESS, add_output_column(ts, buf, OB_APP_MIN_COLUMN_ID + 2));
      ASSERT_EQ(OB_SUCCESS, add_output_column(ts, buf, OB_APP_MIN_COLUMN_ID + 3));
      ASSERT_EQ(OB_SUCCESS, add_output_column(ts, buf, OB_APP_MIN_COLUMN_ID + 4));
      ASSERT_EQ(OB_SUCCESS, add_filter_expr(ts, buf, OB_APP_MIN_COLUMN_ID + 2, ExprItem::T_OP_EQ, OB_APP_MIN_COLUMN_ID + 3));
      ts.set_table(test::ObFakeTable::TABLE_ID + NEW_TABLE_ID, test::ObFakeTable::TABLE_ID);
      ts.set_child(0, input_table);
      ASSERT_EQ(OB_SUCCESS, ts.open());
      int counter = 0;
      while(OB_SUCCESS == ts.get_next_row(row))
      {
        counter++;
        const ObObj *obj = NULL;
        TBSYS_LOG(DEBUG, "got row");
        ASSERT_TRUE(NULL != row);
         
        row->get_cell(test::ObFakeTable::TABLE_ID + NEW_TABLE_ID, OB_APP_MIN_COLUMN_ID + 1, obj);
        ASSERT_TRUE(NULL != obj);
        obj->dump();    

        row->get_cell(test::ObFakeTable::TABLE_ID + NEW_TABLE_ID, OB_APP_MIN_COLUMN_ID + 2, obj);
        ASSERT_TRUE(NULL != obj);
        obj->dump();    
        row->get_cell(test::ObFakeTable::TABLE_ID + NEW_TABLE_ID, OB_APP_MIN_COLUMN_ID + 4, obj);
        ASSERT_TRUE(NULL != obj);
        obj->dump();    
      }
      ASSERT_EQ(counter, cycle * 2);
      ASSERT_EQ(OB_SUCCESS, ts.close());
    }

    TEST_F(ObTableMemScanTest, limit_filter_project_fake_table_test)
    {
      ObStringBuf buf;
      ObFakeTable input_table;
      const int cycle = 3;
      input_table.set_row_count(6 * cycle);

      const ObRow *row= NULL;
      ObTableMemScan ts;
      /* c1: row_idx
       * c2: row_idx%2 
       * c3: row_idx%3 
       * c4: row_idx/2 
       */
      ts.set_child(0, input_table);
      add_output_column(ts, buf, OB_APP_MIN_COLUMN_ID + 1);
      add_output_column(ts, buf, OB_APP_MIN_COLUMN_ID + 2);
      add_output_column(ts, buf, OB_APP_MIN_COLUMN_ID + 3);
      add_output_column(ts, buf, OB_APP_MIN_COLUMN_ID + 4);
      add_filter_expr(ts, buf, OB_APP_MIN_COLUMN_ID + 2, ExprItem::T_OP_EQ, OB_APP_MIN_COLUMN_ID + 3); 
      ts.set_table(test::ObFakeTable::TABLE_ID + NEW_TABLE_ID, test::ObFakeTable::TABLE_ID);
      ts.set_limit(2, 1);
      ASSERT_EQ(OB_SUCCESS, ts.open());
      int counter = 0;
      while(OB_SUCCESS == ts.get_next_row(row))
      {
        counter++;
        const ObObj *obj = NULL;
        TBSYS_LOG(DEBUG, "got row");
        ASSERT_TRUE(NULL != row);
         
        row->get_cell(test::ObFakeTable::TABLE_ID + NEW_TABLE_ID, OB_APP_MIN_COLUMN_ID + 1, obj);
        ASSERT_TRUE(NULL != obj);
        obj->dump();    

        row->get_cell(test::ObFakeTable::TABLE_ID + NEW_TABLE_ID, OB_APP_MIN_COLUMN_ID + 2, obj);
        ASSERT_TRUE(NULL != obj);
        obj->dump();    
        row->get_cell(test::ObFakeTable::TABLE_ID + NEW_TABLE_ID, OB_APP_MIN_COLUMN_ID + 4, obj);
        ASSERT_TRUE(NULL != obj);
        obj->dump();    
      }
      ASSERT_EQ(counter, 2);
      ASSERT_EQ(OB_SUCCESS, ts.close());
    }

    TEST_F(ObTableMemScanTest, limit_none_filter_project_fake_table_test)
    {
      ObStringBuf buf;
      ObFakeTable input_table;
      const int cycle = 3;
      input_table.set_row_count(6 * cycle);

      const ObRow *row= NULL;
      ObTableMemScan ts;
      /* c1: row_idx
       * c2: row_idx%2 
       * c3: row_idx%3 
       * c4: row_idx/2 
       */
      ts.set_child(0, input_table);
      add_output_column(ts, buf, OB_APP_MIN_COLUMN_ID + 1);
      add_output_column(ts, buf, OB_APP_MIN_COLUMN_ID + 2);
      add_output_column(ts, buf, OB_APP_MIN_COLUMN_ID + 3);
      add_output_column(ts, buf, OB_APP_MIN_COLUMN_ID + 4);
      add_filter_expr(ts, buf, OB_APP_MIN_COLUMN_ID + 2, ExprItem::T_OP_EQ, OB_APP_MIN_COLUMN_ID + 3); 
      ts.set_table(test::ObFakeTable::TABLE_ID + NEW_TABLE_ID, test::ObFakeTable::TABLE_ID);
      ts.set_limit(10000, 0);
      ASSERT_EQ(OB_SUCCESS, ts.open());
      int counter = 0;
      while(OB_SUCCESS == ts.get_next_row(row))
      {
        counter++;
        const ObObj *obj = NULL;
        TBSYS_LOG(DEBUG, "got row");
        ASSERT_TRUE(NULL != row);
         
        row->get_cell(test::ObFakeTable::TABLE_ID + NEW_TABLE_ID, OB_APP_MIN_COLUMN_ID + 1, obj);
        ASSERT_TRUE(NULL != obj);
        obj->dump();    

        row->get_cell(test::ObFakeTable::TABLE_ID + NEW_TABLE_ID, OB_APP_MIN_COLUMN_ID + 2, obj);
        ASSERT_TRUE(NULL != obj);
        obj->dump();    
        row->get_cell(test::ObFakeTable::TABLE_ID + NEW_TABLE_ID, OB_APP_MIN_COLUMN_ID + 4, obj);
        ASSERT_TRUE(NULL != obj);
        obj->dump();    
      }
      ASSERT_EQ(counter, cycle * 2);
      ASSERT_EQ(OB_SUCCESS, ts.close());
    }

    TEST_F(ObTableMemScanTest, limit_project_fake_table_test)
    {
      ObStringBuf buf;
      ObFakeTable input_table;
      const int cycle = 3;
      input_table.set_row_count(6 * cycle);

      const ObRow *row= NULL;
      ObTableMemScan ts;
      /* c1: row_idx
       * c2: row_idx%2 
       * c3: row_idx%3 
       * c4: row_idx/2 
       */
      ts.set_child(0, input_table);
      add_output_column(ts, buf, OB_APP_MIN_COLUMN_ID + 1);
      add_output_column(ts, buf, OB_APP_MIN_COLUMN_ID + 2);
      add_output_column(ts, buf, OB_APP_MIN_COLUMN_ID + 3);
      add_output_column(ts, buf, OB_APP_MIN_COLUMN_ID + 4);
      ts.set_table(test::ObFakeTable::TABLE_ID + NEW_TABLE_ID, test::ObFakeTable::TABLE_ID);
      ts.set_limit(10000, 0);
      ASSERT_EQ(OB_SUCCESS, ts.open());
      int counter = 0;
      while(OB_SUCCESS == ts.get_next_row(row))
      {
        counter++;
        const ObObj *obj = NULL;
        TBSYS_LOG(DEBUG, "got row");
        ASSERT_TRUE(NULL != row);
         
        row->get_cell(test::ObFakeTable::TABLE_ID + NEW_TABLE_ID, OB_APP_MIN_COLUMN_ID + 1, obj);
        ASSERT_TRUE(NULL != obj);
        obj->dump();    

        row->get_cell(test::ObFakeTable::TABLE_ID + NEW_TABLE_ID, OB_APP_MIN_COLUMN_ID + 2, obj);
        ASSERT_TRUE(NULL != obj);
        obj->dump();    
        row->get_cell(test::ObFakeTable::TABLE_ID + NEW_TABLE_ID, OB_APP_MIN_COLUMN_ID + 4, obj);
        ASSERT_TRUE(NULL != obj);
        obj->dump();    
      }
      ASSERT_EQ(counter, 6 * cycle);
      ASSERT_EQ(OB_SUCCESS, ts.close());
    }

    TEST_F(ObTableMemScanTest, limit2_project_fake_table_test)
    {
      ObStringBuf buf;
      ObFakeTable input_table;
      const int cycle = 3;
      input_table.set_row_count(6 * cycle);

      const ObRow *row= NULL;
      ObTableMemScan ts;
      /* c1: row_idx
       * c2: row_idx%2 
       * c3: row_idx%3 
       * c4: row_idx/2 
       */
      ts.set_child(0, input_table);
      add_output_column(ts, buf, OB_APP_MIN_COLUMN_ID + 1);
      add_output_column(ts, buf, OB_APP_MIN_COLUMN_ID + 2);
      add_output_column(ts, buf, OB_APP_MIN_COLUMN_ID + 3);
      add_output_column(ts, buf, OB_APP_MIN_COLUMN_ID + 4);
      ts.set_table(test::ObFakeTable::TABLE_ID + NEW_TABLE_ID, test::ObFakeTable::TABLE_ID);
      ASSERT_EQ(OB_SUCCESS, ts.set_limit(10, 10));
      ASSERT_EQ(OB_SUCCESS, ts.open());
      int counter = 0;
      while(OB_SUCCESS == ts.get_next_row(row))
      {
        counter++;
        const ObObj *obj = NULL;
        TBSYS_LOG(DEBUG, "got row");
        ASSERT_TRUE(NULL != row);
         
        row->get_cell(test::ObFakeTable::TABLE_ID + NEW_TABLE_ID, OB_APP_MIN_COLUMN_ID + 1, obj);
        ASSERT_TRUE(NULL != obj);
        obj->dump();    

        row->get_cell(test::ObFakeTable::TABLE_ID + NEW_TABLE_ID, OB_APP_MIN_COLUMN_ID + 2, obj);
        ASSERT_TRUE(NULL != obj);
        obj->dump();    
        row->get_cell(test::ObFakeTable::TABLE_ID + NEW_TABLE_ID, OB_APP_MIN_COLUMN_ID + 4, obj);
        ASSERT_TRUE(NULL != obj);
        obj->dump();    
      }
      ASSERT_EQ(counter, std::min(10, 6 * cycle - 10));
      ASSERT_EQ(OB_SUCCESS, ts.close());
    }


    TEST_F(ObTableMemScanTest, must_set_table_test)
    {
      ObStringBuf buf;
      ObFakeTable input_table;
      const int cycle = 3;
      input_table.set_row_count(6 * cycle);

      ObTableMemScan ts;
      /* c1: row_idx
       * c2: row_idx%2 
       * c3: row_idx%3 
       * c4: row_idx/2 
       */
      ts.set_child(0, input_table);
      add_output_column(ts, buf, OB_APP_MIN_COLUMN_ID + 1);
      add_output_column(ts, buf, OB_APP_MIN_COLUMN_ID + 2);
      add_output_column(ts, buf, OB_APP_MIN_COLUMN_ID + 3);
      add_output_column(ts, buf, OB_APP_MIN_COLUMN_ID + 4);
      //ts.set_table(test::ObFakeTable::TABLE_ID + NEW_TABLE_ID, test::ObFakeTable::TABLE_ID);
      ASSERT_EQ(OB_SUCCESS, ts.set_limit(10, 10));
      ASSERT_EQ(OB_NOT_INIT, ts.open());
    }

  };
};

int main(int argc, char **argv)
{
  ob_init_memory_pool();
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}

