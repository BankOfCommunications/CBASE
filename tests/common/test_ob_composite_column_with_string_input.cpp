/*
 *  (C) 2007-2010 Taobao Inc.
 *  
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License version 2 as
 *  published by the Free Software Foundation.
 *
 *         test_ob_composite_column.cpp is for what ...
 *
 *  Version: $Id: test_ob_composite_column by xiaochu Exp $
 *
 *  Authors:
 *     xiaochu < xiaochu.yh@taobao.com >
 *        - some work details if you want
 */


#include "gtest/gtest.h"
#include "common/ob_composite_column.h"


using namespace oceanbase;
using namespace common;


int build_single_cell(ObCellArray &org_cell_array)
{
  ObCellInfo cell;
  ObCellInfo *cell_out = NULL;
  int64_t i;

  for(i = 0; i < 1; i++)
  {
    cell.value_.set_int(i);
    EXPECT_EQ(org_cell_array.append(cell, cell_out),OB_SUCCESS);
  }
  return 0;
}


TEST(Test_ob_composite_column, calc_test_1)
{
  ObScanParam scan_param;
  ObCellArray cells;
  ObObj result;
  ObCompositeColumn column;
  build_single_cell(cells);
  int64_t i = 0;
  double d = 0.0;
  bool b = true;

  ObString as_name;
  as_name.assign("fake", strlen("fake"));

  ObString str;
  char *expr = NULL;

  expr = "2";
  str.assign(expr, strlen(expr));
  EXPECT_EQ(OB_SUCCESS, column.set_expression(str, as_name));
  EXPECT_EQ(OB_SUCCESS, column.decode(scan_param));
  EXPECT_EQ(OB_SUCCESS, column.calc_composite_val(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_int(i));
  ASSERT_EQ(i, 2);


  expr = "2+7";
  str.assign(expr, strlen(expr));
  EXPECT_EQ(OB_SUCCESS, column.set_expression(str, as_name));
  EXPECT_EQ(OB_SUCCESS, column.decode(scan_param));
  EXPECT_EQ(OB_SUCCESS, column.calc_composite_val(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_int(i));
  ASSERT_EQ(i, 9);

  expr = "2-7";
  str.assign(expr, strlen(expr));
  EXPECT_EQ(OB_SUCCESS, column.set_expression(str, as_name));
  EXPECT_EQ(OB_SUCCESS, column.decode(scan_param));
  EXPECT_EQ(OB_SUCCESS, column.calc_composite_val(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_int(i));
  ASSERT_EQ(i, -5);
 
  expr = "2*7";
  str.assign(expr, strlen(expr));
  EXPECT_EQ(OB_SUCCESS, column.set_expression(str, as_name));
  EXPECT_EQ(OB_SUCCESS, column.decode(scan_param));
  EXPECT_EQ(OB_SUCCESS, column.calc_composite_val(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_int(i));
  ASSERT_EQ(i, 14);

  expr = "8/2";
  str.assign(expr, strlen(expr));
  EXPECT_EQ(OB_SUCCESS, column.set_expression(str, as_name));
  EXPECT_EQ(OB_SUCCESS, column.decode(scan_param));
  EXPECT_EQ(OB_SUCCESS, column.calc_composite_val(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_int(i));
  ASSERT_EQ(i, 4);

  expr = "2.0+7";
  str.assign(expr, strlen(expr));
  EXPECT_EQ(OB_SUCCESS, column.set_expression(str, as_name));
  EXPECT_EQ(OB_SUCCESS, column.decode(scan_param));
  EXPECT_EQ(OB_SUCCESS, column.calc_composite_val(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_double(d));
  ASSERT_EQ(d, 9);

  expr = "2.0-7";
  str.assign(expr, strlen(expr));
  EXPECT_EQ(OB_SUCCESS, column.set_expression(str, as_name));
  EXPECT_EQ(OB_SUCCESS, column.decode(scan_param));
  EXPECT_EQ(OB_SUCCESS, column.calc_composite_val(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_double(d));
  ASSERT_EQ(d, -5);
 
  expr = "2.0*7";
  str.assign(expr, strlen(expr));
  EXPECT_EQ(OB_SUCCESS, column.set_expression(str, as_name));
  EXPECT_EQ(OB_SUCCESS, column.decode(scan_param));
  EXPECT_EQ(OB_SUCCESS, column.calc_composite_val(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_double(d));
  ASSERT_EQ(d, 14);

  expr = "8.0/2";
  str.assign(expr, strlen(expr));
  EXPECT_EQ(OB_SUCCESS, column.set_expression(str, as_name));
  EXPECT_EQ(OB_SUCCESS, column.decode(scan_param));
  EXPECT_EQ(OB_SUCCESS, column.calc_composite_val(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_double(d));
  ASSERT_EQ(d, 4.0);

  expr = "2+7.0";
  str.assign(expr, strlen(expr));
  EXPECT_EQ(OB_SUCCESS, column.set_expression(str, as_name));
  EXPECT_EQ(OB_SUCCESS, column.decode(scan_param));
  EXPECT_EQ(OB_SUCCESS, column.calc_composite_val(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_double(d));
  ASSERT_EQ(d, 9);

  expr = "2-7.0";
  str.assign(expr, strlen(expr));
  EXPECT_EQ(OB_SUCCESS, column.set_expression(str, as_name));
  EXPECT_EQ(OB_SUCCESS, column.decode(scan_param));
  EXPECT_EQ(OB_SUCCESS, column.calc_composite_val(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_double(d));
  ASSERT_EQ(d, -5);
 
  expr = "2*7.0";
  str.assign(expr, strlen(expr));
  EXPECT_EQ(OB_SUCCESS, column.set_expression(str, as_name));
  EXPECT_EQ(OB_SUCCESS, column.decode(scan_param));
  EXPECT_EQ(OB_SUCCESS, column.calc_composite_val(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_double(d));
  ASSERT_EQ(d, 14);

  expr = "8/2.0";
  str.assign(expr, strlen(expr));
  EXPECT_EQ(OB_SUCCESS, column.set_expression(str, as_name));
  EXPECT_EQ(OB_SUCCESS, column.decode(scan_param));
  EXPECT_EQ(OB_SUCCESS, column.calc_composite_val(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_double(d));
  ASSERT_EQ(d, 4.0);

  expr = "2.0+7.0";
  str.assign(expr, strlen(expr));
  EXPECT_EQ(OB_SUCCESS, column.set_expression(str, as_name));
  EXPECT_EQ(OB_SUCCESS, column.decode(scan_param));
  EXPECT_EQ(OB_SUCCESS, column.calc_composite_val(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_double(d));
  ASSERT_EQ(d, 9);

  expr = "2.0-7.0";
  str.assign(expr, strlen(expr));
  EXPECT_EQ(OB_SUCCESS, column.set_expression(str, as_name));
  EXPECT_EQ(OB_SUCCESS, column.decode(scan_param));
  EXPECT_EQ(OB_SUCCESS, column.calc_composite_val(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_double(d));
  ASSERT_EQ(d, -5);
 
  expr = "2.0*7.0";
  str.assign(expr, strlen(expr));
  EXPECT_EQ(OB_SUCCESS, column.set_expression(str, as_name));
  EXPECT_EQ(OB_SUCCESS, column.decode(scan_param));
  EXPECT_EQ(OB_SUCCESS, column.calc_composite_val(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_double(d));
  ASSERT_EQ(d, 14);

  expr = "8.0/2.0";
  str.assign(expr, strlen(expr));
  EXPECT_EQ(OB_SUCCESS, column.set_expression(str, as_name));
  EXPECT_EQ(OB_SUCCESS, column.decode(scan_param));
  EXPECT_EQ(OB_SUCCESS, column.calc_composite_val(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_double(d));
  ASSERT_EQ(d, 4.0);

  /*
  expr = "2";
  str.assign(expr, strlen(expr));
  EXPECT_EQ(OB_SUCCESS, column.set_expression(str, as_name));
  EXPECT_EQ(OB_SUCCESS, column.decode(scan_param));
  EXPECT_EQ(OB_SUCCESS, column.calc_composite_val(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_bool(b));
  ASSERT_EQ(b, true);
  */

  /// FIXME: NOT op is not supported yet
  /*
  printf("=============\n\n\n");

  expr = "!2";
  str.assign(expr, strlen(expr));
  EXPECT_EQ(OB_SUCCESS, column.set_expression(str, as_name));
  EXPECT_EQ(OB_SUCCESS, column.decode(scan_param));
  EXPECT_EQ(OB_SUCCESS, column.calc_composite_val(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_bool(b));
  ASSERT_EQ(b, false);

  expr = "!0.0";
  str.assign(expr, strlen(expr));
  EXPECT_EQ(OB_SUCCESS, column.set_expression(str, as_name));
  EXPECT_EQ(OB_SUCCESS, column.decode(scan_param));
  EXPECT_EQ(OB_SUCCESS, column.calc_composite_val(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_bool(b));
  ASSERT_EQ(b, true);

  expr = "!2.3";
  str.assign(expr, strlen(expr));
  EXPECT_EQ(OB_SUCCESS, column.set_expression(str, as_name));
  EXPECT_EQ(OB_SUCCESS, column.decode(scan_param));
  EXPECT_EQ(OB_SUCCESS, column.calc_composite_val(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_bool(b));
  ASSERT_EQ(b, false);
  */

  
}


TEST(Test_ob_composite_column, calc_test_2)
{
  ObScanParam scan_param;
  ObCellArray cells;
  ObObj result;
  ObCompositeColumn column;
  build_single_cell(cells);
  int64_t i = 0;
  double d = 0.0;
  bool b = true;

  ObString as_name;
  as_name.assign("fake", strlen("fake"));

  ObString str;
  char *expr = NULL;

  expr = "6 > 1";
  str.assign(expr, strlen(expr));
  EXPECT_EQ(OB_SUCCESS, column.set_expression(str, as_name));
  EXPECT_EQ(OB_SUCCESS, column.decode(scan_param));
  EXPECT_EQ(OB_SUCCESS, column.calc_composite_val(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_bool(b));
  ASSERT_EQ(b, true);

  expr = "6 > 1.3";
  str.assign(expr, strlen(expr));
  EXPECT_EQ(OB_SUCCESS, column.set_expression(str, as_name));
  EXPECT_EQ(OB_SUCCESS, column.decode(scan_param));
  EXPECT_EQ(OB_SUCCESS, column.calc_composite_val(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_bool(b));
  ASSERT_EQ(b, true);

  expr = "6 < 1";
  str.assign(expr, strlen(expr));
  EXPECT_EQ(OB_SUCCESS, column.set_expression(str, as_name));
  EXPECT_EQ(OB_SUCCESS, column.decode(scan_param));
  EXPECT_EQ(OB_SUCCESS, column.calc_composite_val(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_bool(b));
  ASSERT_EQ(b, false);

  expr = "6 < 1.23";
  str.assign(expr, strlen(expr));
  EXPECT_EQ(OB_SUCCESS, column.set_expression(str, as_name));
  EXPECT_EQ(OB_SUCCESS, column.decode(scan_param));
  EXPECT_EQ(OB_SUCCESS, column.calc_composite_val(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_bool(b));
  ASSERT_EQ(b, false);

  expr = "6 >= 1";
  str.assign(expr, strlen(expr));
  EXPECT_EQ(OB_SUCCESS, column.set_expression(str, as_name));
  EXPECT_EQ(OB_SUCCESS, column.decode(scan_param));
  EXPECT_EQ(OB_SUCCESS, column.calc_composite_val(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_bool(b));
  ASSERT_EQ(b, true);

  expr = "6 >= 1.3";
  str.assign(expr, strlen(expr));
  EXPECT_EQ(OB_SUCCESS, column.set_expression(str, as_name));
  EXPECT_EQ(OB_SUCCESS, column.decode(scan_param));
  EXPECT_EQ(OB_SUCCESS, column.calc_composite_val(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_bool(b));
  ASSERT_EQ(b, true);

  expr = "6 <= 1";
  str.assign(expr, strlen(expr));
  EXPECT_EQ(OB_SUCCESS, column.set_expression(str, as_name));
  EXPECT_EQ(OB_SUCCESS, column.decode(scan_param));
  EXPECT_EQ(OB_SUCCESS, column.calc_composite_val(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_bool(b));
  ASSERT_EQ(b, false);

  expr = "6 <= 1.23";
  str.assign(expr, strlen(expr));
  EXPECT_EQ(OB_SUCCESS, column.set_expression(str, as_name));
  EXPECT_EQ(OB_SUCCESS, column.decode(scan_param));
  EXPECT_EQ(OB_SUCCESS, column.calc_composite_val(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_bool(b));
  ASSERT_EQ(b, false);

  expr = "2.0 > 7.0";
  str.assign(expr, strlen(expr));
  EXPECT_EQ(OB_SUCCESS, column.set_expression(str, as_name));
  EXPECT_EQ(OB_SUCCESS, column.decode(scan_param));
  EXPECT_EQ(OB_SUCCESS, column.calc_composite_val(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_bool(b));
  ASSERT_EQ(b, false);

  expr = "2.0 < 7";
  str.assign(expr, strlen(expr));
  EXPECT_EQ(OB_SUCCESS, column.set_expression(str, as_name));
  EXPECT_EQ(OB_SUCCESS, column.decode(scan_param));
  EXPECT_EQ(OB_SUCCESS, column.calc_composite_val(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_bool(b));
  ASSERT_EQ(b, true);
 
  expr = "2.0 >= 7";
  str.assign(expr, strlen(expr));
  EXPECT_EQ(OB_SUCCESS, column.set_expression(str, as_name));
  EXPECT_EQ(OB_SUCCESS, column.decode(scan_param));
  EXPECT_EQ(OB_SUCCESS, column.calc_composite_val(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_bool(b));
  ASSERT_EQ(b, false);

  expr = "2.0 <= 8.0";
  str.assign(expr, strlen(expr));
  EXPECT_EQ(OB_SUCCESS, column.set_expression(str, as_name));
  EXPECT_EQ(OB_SUCCESS, column.decode(scan_param));
  EXPECT_EQ(OB_SUCCESS, column.calc_composite_val(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_bool(b));
  ASSERT_EQ(b, true);

  expr = "2.0 = 8.0";
  str.assign(expr, strlen(expr));
  EXPECT_EQ(OB_SUCCESS, column.set_expression(str, as_name));
  EXPECT_EQ(OB_SUCCESS, column.decode(scan_param));
  EXPECT_EQ(OB_SUCCESS, column.calc_composite_val(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_bool(b));
  ASSERT_EQ(b, false);

  expr = "2.0 != 8";
  str.assign(expr, strlen(expr));
  EXPECT_EQ(OB_SUCCESS, column.set_expression(str, as_name));
  EXPECT_EQ(OB_SUCCESS, column.decode(scan_param));
  EXPECT_EQ(OB_SUCCESS, column.calc_composite_val(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_bool(b));
  ASSERT_EQ(b, true);

  expr = "2.0 != 2";
  str.assign(expr, strlen(expr));
  EXPECT_EQ(OB_SUCCESS, column.set_expression(str, as_name));
  EXPECT_EQ(OB_SUCCESS, column.decode(scan_param));
  EXPECT_EQ(OB_SUCCESS, column.calc_composite_val(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_bool(b));
  ASSERT_EQ(b, false);

  expr = "2 = 2.0";
  str.assign(expr, strlen(expr));
  EXPECT_EQ(OB_SUCCESS, column.set_expression(str, as_name));
  EXPECT_EQ(OB_SUCCESS, column.decode(scan_param));
  EXPECT_EQ(OB_SUCCESS, column.calc_composite_val(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_bool(b));
  ASSERT_EQ(b, true);

  expr = "(2.0 + 3) >= (3.0 + 2)";
  str.assign(expr, strlen(expr));
  EXPECT_EQ(OB_SUCCESS, column.set_expression(str, as_name));
  EXPECT_EQ(OB_SUCCESS, column.decode(scan_param));
  EXPECT_EQ(OB_SUCCESS, column.calc_composite_val(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_bool(b));
  ASSERT_EQ(b, true);

  expr = "(2.0 + 3) <= (3.0 + 2)";
  str.assign(expr, strlen(expr));
  EXPECT_EQ(OB_SUCCESS, column.set_expression(str, as_name));
  EXPECT_EQ(OB_SUCCESS, column.decode(scan_param));
  EXPECT_EQ(OB_SUCCESS, column.calc_composite_val(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_bool(b));
  ASSERT_EQ(b, true);

  expr = "(1 % 3) <= (3 % 2)";
  str.assign(expr, strlen(expr));
  EXPECT_EQ(OB_SUCCESS, column.set_expression(str, as_name));
  EXPECT_EQ(OB_SUCCESS, column.decode(scan_param));
  EXPECT_EQ(OB_SUCCESS, column.calc_composite_val(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_bool(b));
  ASSERT_EQ(b, true);
}

TEST(Test_ob_composite_column, calc_test_3)
{
  ObScanParam scan_param;
  ObCellArray cells;
  ObObj result;
  ObCompositeColumn column;
  build_single_cell(cells);
  int64_t i = 0;
  double d = 0.0;
  bool b = true;

  ObString as_name;
  as_name.assign("fake", strlen("fake"));

  ObString str;
  char *expr = NULL;

  expr = "6+3+1";
  str.assign(expr, strlen(expr));
  EXPECT_EQ(OB_SUCCESS, column.set_expression(str, as_name));
  EXPECT_EQ(OB_SUCCESS, column.decode(scan_param));
  EXPECT_EQ(OB_SUCCESS, column.calc_composite_val(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_int(i));
  ASSERT_EQ(i, 10);

  expr = "6/3+1";
  str.assign(expr, strlen(expr));
  EXPECT_EQ(OB_SUCCESS, column.set_expression(str, as_name));
  EXPECT_EQ(OB_SUCCESS, column.decode(scan_param));
  EXPECT_EQ(OB_SUCCESS, column.calc_composite_val(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_int(i));
  ASSERT_EQ(i, 3);
 
  expr = "1+6/3";
  str.assign(expr, strlen(expr));
  EXPECT_EQ(OB_SUCCESS, column.set_expression(str, as_name));
  EXPECT_EQ(OB_SUCCESS, column.decode(scan_param));
  EXPECT_EQ(OB_SUCCESS, column.calc_composite_val(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_int(i));
  ASSERT_EQ(i, 3);

  expr = "6/(3+1)";
  str.assign(expr, strlen(expr));
  EXPECT_EQ(OB_SUCCESS, column.set_expression(str, as_name));
  EXPECT_EQ(OB_SUCCESS, column.decode(scan_param));
  EXPECT_EQ(OB_SUCCESS, column.calc_composite_val(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_int(i));
  ASSERT_EQ(i, 1);

  expr = "6/(3+1.0)";
  str.assign(expr, strlen(expr));
  EXPECT_EQ(OB_SUCCESS, column.set_expression(str, as_name));
  EXPECT_EQ(OB_SUCCESS, column.decode(scan_param));
  EXPECT_EQ(OB_SUCCESS, column.calc_composite_val(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_double(d));
  ASSERT_EQ(d, 1.5);

  expr = "6/(3+1.0)-1.5+10*10/10-10";
  str.assign(expr, strlen(expr));
  EXPECT_EQ(OB_SUCCESS, column.set_expression(str, as_name));
  EXPECT_EQ(OB_SUCCESS, column.decode(scan_param));
  EXPECT_EQ(OB_SUCCESS, column.calc_composite_val(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_double(d));
  ASSERT_EQ(d, 0);

  expr = "6/(3+1.0)-1.5+10*10/10-10%4";
  str.assign(expr, strlen(expr));
  EXPECT_EQ(OB_SUCCESS, column.set_expression(str, as_name));
  EXPECT_EQ(OB_SUCCESS, column.decode(scan_param));
  EXPECT_EQ(OB_SUCCESS, column.calc_composite_val(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_double(d));
  ASSERT_EQ(d, 8);

  expr = "6 AND 3";
  str.assign(expr, strlen(expr));
  EXPECT_EQ(OB_SUCCESS, column.set_expression(str, as_name));
  EXPECT_EQ(OB_SUCCESS, column.decode(scan_param));
  EXPECT_EQ(OB_SUCCESS, column.calc_composite_val(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_bool(b));
  ASSERT_EQ(b, true);

  expr = "6 AND 0.0";
  str.assign(expr, strlen(expr));
  EXPECT_EQ(OB_SUCCESS, column.set_expression(str, as_name));
  EXPECT_EQ(OB_SUCCESS, column.decode(scan_param));
  EXPECT_EQ(OB_SUCCESS, column.calc_composite_val(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_bool(b));
  ASSERT_EQ(b, false);

  expr = "6 or 3";
  str.assign(expr, strlen(expr));
  EXPECT_EQ(OB_SUCCESS, column.set_expression(str, as_name));
  EXPECT_EQ(OB_SUCCESS, column.decode(scan_param));
  EXPECT_EQ(OB_SUCCESS, column.calc_composite_val(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_bool(b));
  ASSERT_EQ(b, true);

  expr = "6 or 0.0";
  str.assign(expr, strlen(expr));
  EXPECT_EQ(OB_SUCCESS, column.set_expression(str, as_name));
  EXPECT_EQ(OB_SUCCESS, column.decode(scan_param));
  EXPECT_EQ(OB_SUCCESS, column.calc_composite_val(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_bool(b));
  ASSERT_EQ(b, true);

  expr = "6 AND (3+1)";
  str.assign(expr, strlen(expr));
  EXPECT_EQ(OB_SUCCESS, column.set_expression(str, as_name));
  EXPECT_EQ(OB_SUCCESS, column.decode(scan_param));
  EXPECT_EQ(OB_SUCCESS, column.calc_composite_val(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_bool(b));
  ASSERT_EQ(b, true);

  expr = "6.0 AND (3.2+1)";
  str.assign(expr, strlen(expr));
  EXPECT_EQ(OB_SUCCESS, column.set_expression(str, as_name));
  EXPECT_EQ(OB_SUCCESS, column.decode(scan_param));
  EXPECT_EQ(OB_SUCCESS, column.calc_composite_val(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_bool(b));
  ASSERT_EQ(b, true);

  expr = "6.0 AND (3 - 3.0)";
  str.assign(expr, strlen(expr));
  EXPECT_EQ(OB_SUCCESS, column.set_expression(str, as_name));
  EXPECT_EQ(OB_SUCCESS, column.decode(scan_param));
  EXPECT_EQ(OB_SUCCESS, column.calc_composite_val(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_bool(b));
  ASSERT_EQ(b, false);

  expr = "6 or (3+1)";
  str.assign(expr, strlen(expr));
  EXPECT_EQ(OB_SUCCESS, column.set_expression(str, as_name));
  EXPECT_EQ(OB_SUCCESS, column.decode(scan_param));
  EXPECT_EQ(OB_SUCCESS, column.calc_composite_val(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_bool(b));
  ASSERT_EQ(b, true);

  expr = "6.0 Or (3.2+1)";
  str.assign(expr, strlen(expr));
  EXPECT_EQ(OB_SUCCESS, column.set_expression(str, as_name));
  EXPECT_EQ(OB_SUCCESS, column.decode(scan_param));
  EXPECT_EQ(OB_SUCCESS, column.calc_composite_val(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_bool(b));
  ASSERT_EQ(b, true);

  expr = "6.0 oR (3 - 3.0)";
  str.assign(expr, strlen(expr));
  EXPECT_EQ(OB_SUCCESS, column.set_expression(str, as_name));
  EXPECT_EQ(OB_SUCCESS, column.decode(scan_param));
  EXPECT_EQ(OB_SUCCESS, column.calc_composite_val(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_bool(b));
  ASSERT_EQ(b, true);


  expr = "(12 + 1) AND (3 - 3.0) or (3 / 2)";
  str.assign(expr, strlen(expr));
  EXPECT_EQ(OB_SUCCESS, column.set_expression(str, as_name));
  EXPECT_EQ(OB_SUCCESS, column.decode(scan_param));
  EXPECT_EQ(OB_SUCCESS, column.calc_composite_val(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_bool(b));
  ASSERT_EQ(b, true);

  expr = "(12 + 1) AND (3 - 3.0) or (0 / 2)";
  str.assign(expr, strlen(expr));
  EXPECT_EQ(OB_SUCCESS, column.set_expression(str, as_name));
  EXPECT_EQ(OB_SUCCESS, column.decode(scan_param));
  EXPECT_EQ(OB_SUCCESS, column.calc_composite_val(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_bool(b));
  ASSERT_EQ(b, false);

  expr = "(12 + 1) AND (3 - 3.0) or (0 / 2 AND 1 + 3 OR 4 - 1)";
  str.assign(expr, strlen(expr));
  EXPECT_EQ(OB_SUCCESS, column.set_expression(str, as_name));
  EXPECT_EQ(OB_SUCCESS, column.decode(scan_param));
  EXPECT_EQ(OB_SUCCESS, column.calc_composite_val(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_bool(b));
  ASSERT_EQ(b, true);

  expr = "(12 + 1) AND (3 - 3.0) or (0 / 2 AND 1 + 3)";
  str.assign(expr, strlen(expr));
  EXPECT_EQ(OB_SUCCESS, column.set_expression(str, as_name));
  EXPECT_EQ(OB_SUCCESS, column.decode(scan_param));
  EXPECT_EQ(OB_SUCCESS, column.calc_composite_val(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_bool(b));
  ASSERT_EQ(b, false);

  expr = "(12 + 1) AND (3 - 3.0) or (0 / 2 AND 1 + 3 or (2 / 5 * 0.2 AND 8 / 8.12))";
  str.assign(expr, strlen(expr));
  EXPECT_EQ(OB_SUCCESS, column.set_expression(str, as_name));
  EXPECT_EQ(OB_SUCCESS, column.decode(scan_param));
  EXPECT_EQ(OB_SUCCESS, column.calc_composite_val(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_bool(b));
  ASSERT_EQ(b, false);

  expr = "(12 + 1) AND (3 - 3.0) or (0 / 2 AND 1 + 3 or (2.0 / 5 * 0.2 AND 8 / 8.12))";
  str.assign(expr, strlen(expr));
  EXPECT_EQ(OB_SUCCESS, column.set_expression(str, as_name));
  EXPECT_EQ(OB_SUCCESS, column.decode(scan_param));
  EXPECT_EQ(OB_SUCCESS, column.calc_composite_val(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_bool(b));
  ASSERT_EQ(b, true);


  expr = "(12 + 1)>143 AND (3 - 3.0 = 0) or ((2 / 5 * 0.2 AND 8 / 8.12))";
  str.assign(expr, strlen(expr));
  EXPECT_EQ(OB_SUCCESS, column.set_expression(str, as_name));
  EXPECT_EQ(OB_SUCCESS, column.decode(scan_param));
  EXPECT_EQ(OB_SUCCESS, column.calc_composite_val(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_bool(b));
  ASSERT_EQ(b, false);

  expr = "(12 + 1)>143 AND (3 - 3.0 = 0) or ((2.0 / 5 * 0.2 AND 8 / 8.12))";
  str.assign(expr, strlen(expr));
  EXPECT_EQ(OB_SUCCESS, column.set_expression(str, as_name));
  EXPECT_EQ(OB_SUCCESS, column.decode(scan_param));
  EXPECT_EQ(OB_SUCCESS, column.calc_composite_val(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_bool(b));
  ASSERT_EQ(b, true);

}


TEST(Test_ob_composite_column, calc_test_4)
{
  ObScanParam scan_param;
  ObCellArray cells;
  ObObj result;
  ObCompositeColumn column;
  build_single_cell(cells);
  int64_t i = 0;
  double d = 0.0;
  bool b = true;

  ObString as_name;
  as_name.assign("fake", strlen("fake"));

  ObString str;
  char *expr = NULL;

  expr = "'home' like 'h'";
  str.assign(expr, strlen(expr));
  EXPECT_EQ(OB_SUCCESS, column.set_expression(str, as_name));
  EXPECT_EQ(OB_SUCCESS, column.decode(scan_param));
  EXPECT_EQ(OB_SUCCESS, column.calc_composite_val(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_bool(b));
  ASSERT_EQ(b, true);

  expr = "'home' like 'hard'";
  str.assign(expr, strlen(expr));
  EXPECT_EQ(OB_SUCCESS, column.set_expression(str, as_name));
  EXPECT_EQ(OB_SUCCESS, column.decode(scan_param));
  EXPECT_EQ(OB_SUCCESS, column.calc_composite_val(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_bool(b));
  ASSERT_EQ(b, false);

  expr = "'home' like 'home town'";
  str.assign(expr, strlen(expr));
  EXPECT_EQ(OB_SUCCESS, column.set_expression(str, as_name));
  EXPECT_EQ(OB_SUCCESS, column.decode(scan_param));
  EXPECT_EQ(OB_SUCCESS, column.calc_composite_val(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_bool(b));
  ASSERT_EQ(b, false);

  expr = "'home town' like 'home'";
  str.assign(expr, strlen(expr));
  EXPECT_EQ(OB_SUCCESS, column.set_expression(str, as_name));
  EXPECT_EQ(OB_SUCCESS, column.decode(scan_param));
  EXPECT_EQ(OB_SUCCESS, column.calc_composite_val(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_bool(b));
  ASSERT_EQ(b, true);

  expr = "'home' is 'home'";
  str.assign(expr, strlen(expr));
  EXPECT_EQ(OB_SUCCESS, column.set_expression(str, as_name));
  EXPECT_EQ(OB_SUCCESS, column.decode(scan_param));
  EXPECT_EQ(OB_SUCCESS, column.calc_composite_val(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_bool(b));
  ASSERT_EQ(b, true);

  expr = "'home' is 3";
  str.assign(expr, strlen(expr));
  EXPECT_EQ(OB_SUCCESS, column.set_expression(str, as_name));
  EXPECT_EQ(OB_SUCCESS, column.decode(scan_param));
  EXPECT_EQ(OB_SUCCESS, column.calc_composite_val(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_bool(b));
  ASSERT_EQ(b, false);


  expr = "3.0 is 'home'";
  str.assign(expr, strlen(expr));
  EXPECT_EQ(OB_SUCCESS, column.set_expression(str, as_name));
  EXPECT_EQ(OB_SUCCESS, column.decode(scan_param));
  EXPECT_EQ(OB_SUCCESS, column.calc_composite_val(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_bool(b));
  ASSERT_EQ(b, false);

  expr = "3.0 is 3";
  str.assign(expr, strlen(expr));
  EXPECT_EQ(OB_SUCCESS, column.set_expression(str, as_name));
  EXPECT_EQ(OB_SUCCESS, column.decode(scan_param));
  EXPECT_EQ(OB_SUCCESS, column.calc_composite_val(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_bool(b));
  ASSERT_EQ(b, false);

  expr = "3.0 is 3.0";
  str.assign(expr, strlen(expr));
  EXPECT_EQ(OB_SUCCESS, column.set_expression(str, as_name));
  EXPECT_EQ(OB_SUCCESS, column.decode(scan_param));
  EXPECT_EQ(OB_SUCCESS, column.calc_composite_val(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_bool(b));
  ASSERT_EQ(b, true);

  expr = "3-1.0 is 2.0";
  str.assign(expr, strlen(expr));
  EXPECT_EQ(OB_SUCCESS, column.set_expression(str, as_name));
  EXPECT_EQ(OB_SUCCESS, column.decode(scan_param));
  EXPECT_EQ(OB_SUCCESS, column.calc_composite_val(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_bool(b));
  ASSERT_EQ(b, true);
}

TEST(Test_ob_composite_column, calc_test_5)
{
  ObScanParam scan_param;
  ObCellArray cells;
  ObObj result;
  ObCompositeColumn column;
  build_single_cell(cells);
  int64_t i = 0;
  double d = 0.0;
  bool b = true;

  ObString as_name;
  as_name.assign("fake", strlen("fake"));

  ObString str;
  char *expr = NULL;

  expr = "NOT 10";
  str.assign(expr, strlen(expr));
  EXPECT_EQ(OB_SUCCESS, column.set_expression(str, as_name));
  EXPECT_EQ(OB_SUCCESS, column.decode(scan_param));
  EXPECT_EQ(OB_SUCCESS, column.calc_composite_val(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_bool(b));
  ASSERT_EQ(b, false);


  expr = "NOT 0";
  str.assign(expr, strlen(expr));
  EXPECT_EQ(OB_SUCCESS, column.set_expression(str, as_name));
  EXPECT_EQ(OB_SUCCESS, column.decode(scan_param));
  EXPECT_EQ(OB_SUCCESS, column.calc_composite_val(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_bool(b));
  ASSERT_EQ(b, true);

} 

int main(int argc, char **argv)
{
  srandom(time(NULL));
  ob_init_memory_pool(64*1024);
  TBSYS_LOGGER.setLogLevel("DEBUG");
  //TBSYS_LOGGER.setLogLevel("WARN");

  //ob_init_memory_pool();
  testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
