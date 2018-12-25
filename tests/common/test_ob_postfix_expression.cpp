/*
 *  (C) 2007-2010 Taobao Inc.
 *
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License version 2 as
 *  published by the Free Software Foundation.
 *
 *         test_ob_postfix_expression.cpp is for what ...
 *
 *  Version: $Id: test_ob_composite_column by xiaochu Exp $
 *
 *  Authors:
 *     xiaochu < xiaochu.yh@taobao.com >
 *        - some work details if you want
 */


#include "gtest/gtest.h"
#include "common/ob_composite_column.h"
#include "hash/ob_hashmap.h"
#include "ob_cell_array.h"


using namespace oceanbase;
using namespace common;

class ObCellReaderUtil
{
private:
  char filename[1024];
  FILE *fp;
public:
  ObCellReaderUtil(const char *fn);
  ~ObCellReaderUtil();
  int next_cell();
  int get_cell(ObCellInfo &cell, char* buf);
private:
  int fill_cell(char *data, int len, ObCellInfo &cell);
};

ObCellReaderUtil::ObCellReaderUtil(const char *fn)
{
  strncpy(filename, fn, 1024);
  filename[1023] = '\0';  // force null-terminator

  fp = fopen(filename, "r");
  if (NULL == fp)
  {
    TBSYS_LOG(WARN, "fail to open file. filename=%s", filename);
  }
}

ObCellReaderUtil::~ObCellReaderUtil()
{
  if(NULL != fp)
  {
    fclose(fp);
    fp = NULL;
  }
}

int ObCellReaderUtil::next_cell()
{
  int err = OB_SUCCESS;
  int c;
  if (NULL != fp)
  {
    while(EOF != (c = fgetc(fp)))
    {
      if (isblank(c) || iscntrl(c))
      {
        continue;
      }
      else
      {
        ungetc(c, fp);
        break;
      }
    }
    if (EOF == c)
    {
      //TBSYS_LOG(WARN, "EOF. filename=%s", filename);
      err = OB_ERROR;
    }
  }
  else
  {
     TBSYS_LOG(WARN, "null file. filename=%s", filename);
     err = OB_ERROR;
  }
  return err;
}

int ObCellReaderUtil::get_cell(ObCellInfo &cell, char *buf)
{
  int err = OB_SUCCESS;
  int c;
  int pos = 0;

  if (NULL != fp && !feof(fp))
  {
    while(EOF != (c = fgetc(fp)) && pos < 1023)
    {
      if (!isblank(c) && !iscntrl(c))
      {
        buf[pos++] = static_cast<char>(c);
        continue;
      }
      else
      {
        break;
      }
    }
    buf[pos] = '\0';
    //TBSYS_LOG(DEBUG, ">>>>>>>>>>>>>buf=[%s]", buf);
    fill_cell(buf, pos, cell);
    //cell.value_.dump();
  }
  else
  {
     TBSYS_LOG(WARN, "EOF. filename=%s", filename);
     err = OB_ERROR;
  }
  return err;
}


int ObCellReaderUtil::fill_cell(char *data, int len, ObCellInfo &cell)
{
  ObString cell_str;

  int i = 0;

  if (i < len)
  {
    if (isdigit(data[i]))
    {
      int type = 1; /// assume intType;
      for(int j = i; j < len; j++)
      {
        if (data[j] == '.')
        {
          type = 0; ///floatType;
          break;
        }
      }
      if (1 == type)
      {
        cell.value_.set_int(atoi(data));
        //TBSYS_LOG(DEBUG, "int. data=%s", data);
      }
      else if(0 == type)
      {
        cell.value_.set_double(atof(data));
        //TBSYS_LOG(DEBUG, "double. data=%s", data);
      }
    }
    else
    {
      cell_str.assign(data, len);
      cell.value_.set_varchar(cell_str);
      //TBSYS_LOG(DEBUG, "string. data=%s", data);
      //cell.value_.dump();
    }
  }
  return OB_SUCCESS;
}

int cell_array_reader(const char *filename, ObCellArray &org_cell_array)
{
  ObString str;
  ObCellInfo cell;
  ObInnerCellInfo *cell_out = NULL;
  ObCellReaderUtil cu(filename);
  char buf[1024];

  //org_cell_array.clear();
  //TBSYS_LOG(DEBUG, "BEGIN DUMP============================================");
  while(OB_SUCCESS == cu.next_cell())
  {
    cell.reset();
    if(OB_SUCCESS == cu.get_cell(cell, buf))
    {
#if 0
      if (cell.value_.get_type() == ObVarcharType)
      {
       //cell.value_.get_varchar(str);
       //TBSYS_LOG(DEBUG, "varchar =[%.*s]", str.length(), str.ptr());
        cell.value_.dump();
      }
      else
      {
        cell.value_.dump();
      }
#endif
      //cell.value_.dump();
      EXPECT_EQ(org_cell_array.append(cell, cell_out),OB_SUCCESS);
    }
    else
    {
      TBSYS_LOG(WARN, "fail to get cell");
    }
  }
  //TBSYS_LOG(DEBUG, "END  DUMP================================================");
  return OB_SUCCESS;
}


void INIT_NAME_TO_IDX_HASH_1(hash::ObHashMap<ObString,int64_t, hash::NoPthreadDefendMode> &name_to_idx_map)
{
  ObString key_name;
  int err = OB_SUCCESS;

  ASSERT_EQ(OB_SUCCESS, name_to_idx_map.create(hash::cal_next_prime(512)));
  key_name.assign((char*)"0", 1);
  err = name_to_idx_map.set(key_name, 0);
  ASSERT_EQ(true, -1 != err && hash::HASH_EXIST != err);

  key_name.assign((char*)"1", 1);
  err = name_to_idx_map.set(key_name, 1);
  ASSERT_EQ(true, -1 != err && hash::HASH_EXIST != err);

  key_name.assign((char*)"2", 1);
  err = name_to_idx_map.set(key_name, 2);
  ASSERT_EQ(true, -1 != err && hash::HASH_EXIST != err);

  key_name.assign((char*)"3", 1);
  err = name_to_idx_map.set(key_name, 3);
  ASSERT_EQ(true, -1 != err && hash::HASH_EXIST != err);

  key_name.assign((char*)"4", 1);
  err = name_to_idx_map.set(key_name, 4);
  ASSERT_EQ(true, -1 != err && hash::HASH_EXIST != err);

  key_name.assign((char*)"5", 1);
  err = name_to_idx_map.set(key_name, 5);
  ASSERT_EQ(true, -1 != err && hash::HASH_EXIST != err);

  key_name.assign((char*)"6", 1);
  err = name_to_idx_map.set(key_name, 6);
  ASSERT_EQ(true, -1 != err && hash::HASH_EXIST != err);

  key_name.assign((char*)"7", 1);
  err = name_to_idx_map.set(key_name, 7);
  ASSERT_EQ(true, -1 != err && hash::HASH_EXIST != err);

  key_name.assign((char*)"8", 1);
  err = name_to_idx_map.set(key_name, 8);
  ASSERT_EQ(true, -1 != err && hash::HASH_EXIST != err);

  key_name.assign((char*)"9", 1);
  err = name_to_idx_map.set(key_name, 9);
  ASSERT_EQ(true, -1 != err && hash::HASH_EXIST != err);

  key_name.assign((char*)"10", 2);
  err = name_to_idx_map.set(key_name, 10);
  ASSERT_EQ(true, -1 != err && hash::HASH_EXIST != err);

}

void INIT_NAME_TO_IDX_HASH_2(hash::ObHashMap<ObString,int64_t, hash::NoPthreadDefendMode> &name_to_idx_map)
{
  ObString key_name;
  int err = OB_SUCCESS;

  ASSERT_EQ(OB_SUCCESS, name_to_idx_map.create(hash::cal_next_prime(512)));
  key_name.assign((char*)"username", static_cast<int32_t>(strlen("username")));
  err = name_to_idx_map.set(key_name, 0);
  ASSERT_EQ(true, -1 != err && hash::HASH_EXIST != err);

  key_name.assign((char*)"password", static_cast<int32_t>(strlen("password")));
  err = name_to_idx_map.set(key_name, 1);
  ASSERT_EQ(true, -1 != err && hash::HASH_EXIST != err);

  key_name.assign((char*)"score", static_cast<int32_t>(strlen("score")));
  err = name_to_idx_map.set(key_name, 2);
  ASSERT_EQ(true, -1 != err && hash::HASH_EXIST != err);
}




int build_single_cell(ObCellArray &org_cell_array)
{
  ObCellInfo cell;
  ObInnerCellInfo *cell_out = NULL;
  int64_t i;

  for(i = 0; i < 1; i++)
  {
    cell.value_.set_int(i);
    EXPECT_EQ(org_cell_array.append(cell, cell_out),OB_SUCCESS);
  }
  return 0;
}


TEST(Test_ob_postfix_expression, calc_test_1)
{
  ObCellArray cells;
  ObObj result;
  ObPostfixExpression postfix_expr;
  build_single_cell(cells);
  int64_t i = 0;
  double d = 0.0;

  ObExpressionParser parser;
  hash::ObHashMap<ObString,int64_t, hash::NoPthreadDefendMode> name_to_idx_map;
  INIT_NAME_TO_IDX_HASH_1(name_to_idx_map);

  ObString str;
  char *expr = NULL;

  expr = (char*)"2";
  str.assign(expr, static_cast<int32_t>(strlen(expr)));
  EXPECT_EQ(OB_SUCCESS, postfix_expr.set_expression(str, name_to_idx_map, parser));

  EXPECT_EQ(OB_SUCCESS, postfix_expr.calc(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_int(i));
  ASSERT_EQ(i, 2);


  expr = (char*)"2+7";
  str.assign(expr, static_cast<int32_t>(strlen(expr)));
  EXPECT_EQ(OB_SUCCESS, postfix_expr.set_expression(str, name_to_idx_map, parser));

  EXPECT_EQ(OB_SUCCESS, postfix_expr.calc(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_int(i));
  ASSERT_EQ(i, 9);

  expr = (char*)"2-7";
  str.assign(expr, static_cast<int32_t>(strlen(expr)));
  EXPECT_EQ(OB_SUCCESS, postfix_expr.set_expression(str, name_to_idx_map, parser));

  EXPECT_EQ(OB_SUCCESS, postfix_expr.calc(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_int(i));
  ASSERT_EQ(i, -5);

  expr = (char*)"2*7";
  str.assign(expr, static_cast<int32_t>(strlen(expr)));
  EXPECT_EQ(OB_SUCCESS, postfix_expr.set_expression(str, name_to_idx_map, parser));

  EXPECT_EQ(OB_SUCCESS, postfix_expr.calc(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_int(i));
  ASSERT_EQ(i, 14);

  expr = (char*)"8/2";
  str.assign(expr, static_cast<int32_t>(strlen(expr)));
  EXPECT_EQ(OB_SUCCESS, postfix_expr.set_expression(str, name_to_idx_map, parser));

  EXPECT_EQ(OB_SUCCESS, postfix_expr.calc(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_double(d));
  ASSERT_EQ(d, 4);

  expr = (char*)"2.0+7";
  str.assign(expr, static_cast<int32_t>(strlen(expr)));
  EXPECT_EQ(OB_SUCCESS, postfix_expr.set_expression(str, name_to_idx_map, parser));

  EXPECT_EQ(OB_SUCCESS, postfix_expr.calc(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_double(d));
  ASSERT_EQ(d, 9);

  expr = (char*)"2.0-7";
  str.assign(expr, static_cast<int32_t>(strlen(expr)));
  EXPECT_EQ(OB_SUCCESS, postfix_expr.set_expression(str, name_to_idx_map, parser));

  EXPECT_EQ(OB_SUCCESS, postfix_expr.calc(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_double(d));
  ASSERT_EQ(d, -5);

  expr = (char*)"2.0*7";
  str.assign(expr, static_cast<int32_t>(strlen(expr)));
  EXPECT_EQ(OB_SUCCESS, postfix_expr.set_expression(str, name_to_idx_map, parser));

  EXPECT_EQ(OB_SUCCESS, postfix_expr.calc(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_double(d));
  ASSERT_EQ(d, 14);

  expr = (char*)"8.0/2";
  str.assign(expr, static_cast<int32_t>(strlen(expr)));
  EXPECT_EQ(OB_SUCCESS, postfix_expr.set_expression(str, name_to_idx_map, parser));

  EXPECT_EQ(OB_SUCCESS, postfix_expr.calc(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_double(d));
  ASSERT_EQ(d, 4.0);

  expr = (char*)"2+7.0";
  str.assign(expr, static_cast<int32_t>(strlen(expr)));
  EXPECT_EQ(OB_SUCCESS, postfix_expr.set_expression(str, name_to_idx_map, parser));

  EXPECT_EQ(OB_SUCCESS, postfix_expr.calc(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_double(d));
  ASSERT_EQ(d, 9);

  expr = (char*)"2-7.0";
  str.assign(expr, static_cast<int32_t>(strlen(expr)));
  EXPECT_EQ(OB_SUCCESS, postfix_expr.set_expression(str, name_to_idx_map, parser));

  EXPECT_EQ(OB_SUCCESS, postfix_expr.calc(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_double(d));
  ASSERT_EQ(d, -5);

  expr = (char*)"2*7.0";
  str.assign(expr, static_cast<int32_t>(strlen(expr)));
  EXPECT_EQ(OB_SUCCESS, postfix_expr.set_expression(str, name_to_idx_map, parser));

  EXPECT_EQ(OB_SUCCESS, postfix_expr.calc(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_double(d));
  ASSERT_EQ(d, 14);

  expr = (char*)"8/2.0";
  str.assign(expr, static_cast<int32_t>(strlen(expr)));
  EXPECT_EQ(OB_SUCCESS, postfix_expr.set_expression(str, name_to_idx_map, parser));

  EXPECT_EQ(OB_SUCCESS, postfix_expr.calc(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_double(d));
  ASSERT_EQ(d, 4.0);

  expr = (char*)"2.0+7.0";
  str.assign(expr, static_cast<int32_t>(strlen(expr)));
  EXPECT_EQ(OB_SUCCESS, postfix_expr.set_expression(str, name_to_idx_map, parser));

  EXPECT_EQ(OB_SUCCESS, postfix_expr.calc(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_double(d));
  ASSERT_EQ(d, 9);

  expr = (char*)"2.0-7.0";
  str.assign(expr, static_cast<int32_t>(strlen(expr)));
  EXPECT_EQ(OB_SUCCESS, postfix_expr.set_expression(str, name_to_idx_map, parser));

  EXPECT_EQ(OB_SUCCESS, postfix_expr.calc(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_double(d));
  ASSERT_EQ(d, -5);

  expr = (char*)"2.0*7.0";
  str.assign(expr, static_cast<int32_t>(strlen(expr)));
  EXPECT_EQ(OB_SUCCESS, postfix_expr.set_expression(str, name_to_idx_map, parser));

  EXPECT_EQ(OB_SUCCESS, postfix_expr.calc(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_double(d));
  ASSERT_EQ(d, 14);

  expr = (char*)"8.0/2.0";
  str.assign(expr, static_cast<int32_t>(strlen(expr)));
  EXPECT_EQ(OB_SUCCESS, postfix_expr.set_expression(str, name_to_idx_map, parser));

  EXPECT_EQ(OB_SUCCESS, postfix_expr.calc(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_double(d));
  ASSERT_EQ(d, 4.0);

  /*
  expr = (char*)"2";
  str.assign(expr, static_cast<int32_t>(strlen(expr)));
  EXPECT_EQ(OB_SUCCESS, postfix_expr.set_expression(str, name_to_idx_map, parser));

  EXPECT_EQ(OB_SUCCESS, postfix_expr.calc(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_bool(b));
  ASSERT_EQ(b, true);
  */

  /// FIXME: NOT op is not supported yet
  /*
  printf("=============\n\n\n");

  expr = (char*)"!2";
  str.assign(expr, static_cast<int32_t>(strlen(expr)));
  EXPECT_EQ(OB_SUCCESS, postfix_expr.set_expression(str, name_to_idx_map, parser));

  EXPECT_EQ(OB_SUCCESS, postfix_expr.calc(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_bool(b));
  ASSERT_EQ(b, false);

  expr = (char*)"!0.0";
  str.assign(expr, static_cast<int32_t>(strlen(expr)));
  EXPECT_EQ(OB_SUCCESS, postfix_expr.set_expression(str, name_to_idx_map, parser));

  EXPECT_EQ(OB_SUCCESS, postfix_expr.calc(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_bool(b));
  ASSERT_EQ(b, true);

  expr = (char*)"!2.3";
  str.assign(expr, static_cast<int32_t>(strlen(expr)));
  EXPECT_EQ(OB_SUCCESS, postfix_expr.set_expression(str, name_to_idx_map, parser));

  EXPECT_EQ(OB_SUCCESS, postfix_expr.calc(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_bool(b));
  ASSERT_EQ(b, false);
  */


}


TEST(Test_ob_postfix_expression, calc_test_2)
{
  ObCellArray cells;
  ObObj result;
  ObPostfixExpression postfix_expr;
  build_single_cell(cells);
  bool b = true;

  ObExpressionParser parser;
  hash::ObHashMap<ObString,int64_t, hash::NoPthreadDefendMode> name_to_idx_map;
  INIT_NAME_TO_IDX_HASH_1(name_to_idx_map);

  ObString str;
  char *expr = NULL;

  expr = (char*)"6 > 1";
  str.assign(expr, static_cast<int32_t>(strlen(expr)));
  EXPECT_EQ(OB_SUCCESS, postfix_expr.set_expression(str, name_to_idx_map, parser));

  EXPECT_EQ(OB_SUCCESS, postfix_expr.calc(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_bool(b));
  ASSERT_EQ(b, true);

  expr = (char*)"6 > 1.3";
  str.assign(expr, static_cast<int32_t>(strlen(expr)));
  EXPECT_EQ(OB_SUCCESS, postfix_expr.set_expression(str, name_to_idx_map, parser));

  EXPECT_EQ(OB_SUCCESS, postfix_expr.calc(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_bool(b));
  ASSERT_EQ(b, true);

  expr = (char*)"6 < 1";
  str.assign(expr, static_cast<int32_t>(strlen(expr)));
  EXPECT_EQ(OB_SUCCESS, postfix_expr.set_expression(str, name_to_idx_map, parser));

  EXPECT_EQ(OB_SUCCESS, postfix_expr.calc(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_bool(b));
  ASSERT_EQ(b, false);

  expr = (char*)"6 < 1.23";
  str.assign(expr, static_cast<int32_t>(strlen(expr)));
  EXPECT_EQ(OB_SUCCESS, postfix_expr.set_expression(str, name_to_idx_map, parser));

  EXPECT_EQ(OB_SUCCESS, postfix_expr.calc(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_bool(b));
  ASSERT_EQ(b, false);

  expr = (char*)"6 >= 1";
  str.assign(expr, static_cast<int32_t>(strlen(expr)));
  EXPECT_EQ(OB_SUCCESS, postfix_expr.set_expression(str, name_to_idx_map, parser));

  EXPECT_EQ(OB_SUCCESS, postfix_expr.calc(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_bool(b));
  ASSERT_EQ(b, true);

  expr = (char*)"6 >= 1.3";
  str.assign(expr, static_cast<int32_t>(strlen(expr)));
  EXPECT_EQ(OB_SUCCESS, postfix_expr.set_expression(str, name_to_idx_map, parser));

  EXPECT_EQ(OB_SUCCESS, postfix_expr.calc(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_bool(b));
  ASSERT_EQ(b, true);

  expr = (char*)"6 <= 1";
  str.assign(expr, static_cast<int32_t>(strlen(expr)));
  EXPECT_EQ(OB_SUCCESS, postfix_expr.set_expression(str, name_to_idx_map, parser));

  EXPECT_EQ(OB_SUCCESS, postfix_expr.calc(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_bool(b));
  ASSERT_EQ(b, false);

  expr = (char*)"6 <= 1.23";
  str.assign(expr, static_cast<int32_t>(strlen(expr)));
  EXPECT_EQ(OB_SUCCESS, postfix_expr.set_expression(str, name_to_idx_map, parser));

  EXPECT_EQ(OB_SUCCESS, postfix_expr.calc(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_bool(b));
  ASSERT_EQ(b, false);

  expr = (char*)"2.0 > 7.0";
  str.assign(expr, static_cast<int32_t>(strlen(expr)));
  EXPECT_EQ(OB_SUCCESS, postfix_expr.set_expression(str, name_to_idx_map, parser));

  EXPECT_EQ(OB_SUCCESS, postfix_expr.calc(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_bool(b));
  ASSERT_EQ(b, false);

  expr = (char*)"2.0 < 7";
  str.assign(expr, static_cast<int32_t>(strlen(expr)));
  EXPECT_EQ(OB_SUCCESS, postfix_expr.set_expression(str, name_to_idx_map, parser));

  EXPECT_EQ(OB_SUCCESS, postfix_expr.calc(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_bool(b));
  ASSERT_EQ(b, true);

  expr = (char*)"2.0 >= 7";
  str.assign(expr, static_cast<int32_t>(strlen(expr)));
  EXPECT_EQ(OB_SUCCESS, postfix_expr.set_expression(str, name_to_idx_map, parser));

  EXPECT_EQ(OB_SUCCESS, postfix_expr.calc(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_bool(b));
  ASSERT_EQ(b, false);

  expr = (char*)"2.0 <= 8.0";
  str.assign(expr, static_cast<int32_t>(strlen(expr)));
  EXPECT_EQ(OB_SUCCESS, postfix_expr.set_expression(str, name_to_idx_map, parser));

  EXPECT_EQ(OB_SUCCESS, postfix_expr.calc(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_bool(b));
  ASSERT_EQ(b, true);

  expr = (char*)"2.0 = 8.0";
  str.assign(expr, static_cast<int32_t>(strlen(expr)));
  EXPECT_EQ(OB_SUCCESS, postfix_expr.set_expression(str, name_to_idx_map, parser));

  EXPECT_EQ(OB_SUCCESS, postfix_expr.calc(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_bool(b));
  ASSERT_EQ(b, false);

  expr = (char*)"2.0 != 8";
  str.assign(expr, static_cast<int32_t>(strlen(expr)));
  EXPECT_EQ(OB_SUCCESS, postfix_expr.set_expression(str, name_to_idx_map, parser));

  EXPECT_EQ(OB_SUCCESS, postfix_expr.calc(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_bool(b));
  ASSERT_EQ(b, true);

  expr = (char*)"2.0 != 2";
  str.assign(expr, static_cast<int32_t>(strlen(expr)));
  EXPECT_EQ(OB_SUCCESS, postfix_expr.set_expression(str, name_to_idx_map, parser));

  EXPECT_EQ(OB_SUCCESS, postfix_expr.calc(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_bool(b));
  ASSERT_EQ(b, false);

  expr = (char*)"2 = 2.0";
  str.assign(expr, static_cast<int32_t>(strlen(expr)));
  EXPECT_EQ(OB_SUCCESS, postfix_expr.set_expression(str, name_to_idx_map, parser));

  EXPECT_EQ(OB_SUCCESS, postfix_expr.calc(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_bool(b));
  ASSERT_EQ(b, true);

  expr = (char*)"(2.0 + 3) >= (3.0 + 2)";
  str.assign(expr, static_cast<int32_t>(strlen(expr)));
  EXPECT_EQ(OB_SUCCESS, postfix_expr.set_expression(str, name_to_idx_map, parser));

  EXPECT_EQ(OB_SUCCESS, postfix_expr.calc(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_bool(b));
  ASSERT_EQ(b, true);

  expr = (char*)"(2.0 + 3) <= (3.0 + 2)";
  str.assign(expr, static_cast<int32_t>(strlen(expr)));
  EXPECT_EQ(OB_SUCCESS, postfix_expr.set_expression(str, name_to_idx_map, parser));

  EXPECT_EQ(OB_SUCCESS, postfix_expr.calc(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_bool(b));
  ASSERT_EQ(b, true);

  expr = (char*)"(1 % 3) <= (3 % 2)";
  str.assign(expr, static_cast<int32_t>(strlen(expr)));
  EXPECT_EQ(OB_SUCCESS, postfix_expr.set_expression(str, name_to_idx_map, parser));

  EXPECT_EQ(OB_SUCCESS, postfix_expr.calc(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_bool(b));
  ASSERT_EQ(b, true);
}

TEST(Test_ob_postfix_expression, calc_test_3)
{
  ObCellArray cells;
  ObObj result;
  ObPostfixExpression postfix_expr;
  build_single_cell(cells);
  int64_t i = 0;
  double d = 0.0;
  bool b = true;

  ObExpressionParser parser;
  hash::ObHashMap<ObString,int64_t, hash::NoPthreadDefendMode> name_to_idx_map;
  INIT_NAME_TO_IDX_HASH_1(name_to_idx_map);

  ObString str;
  char *expr = NULL;

  expr = (char*)"6+3+1";
  str.assign(expr, static_cast<int32_t>(strlen(expr)));
  EXPECT_EQ(OB_SUCCESS, postfix_expr.set_expression(str, name_to_idx_map, parser));

  EXPECT_EQ(OB_SUCCESS, postfix_expr.calc(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_int(i));
  ASSERT_EQ(i, 10);

  expr = (char*)"6/3+1";
  str.assign(expr, static_cast<int32_t>(strlen(expr)));
  EXPECT_EQ(OB_SUCCESS, postfix_expr.set_expression(str, name_to_idx_map, parser));

  EXPECT_EQ(OB_SUCCESS, postfix_expr.calc(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_double(d));
  ASSERT_EQ(d, 3);

  expr = (char*)"1+6/3";
  str.assign(expr, static_cast<int32_t>(strlen(expr)));
  EXPECT_EQ(OB_SUCCESS, postfix_expr.set_expression(str, name_to_idx_map, parser));

  EXPECT_EQ(OB_SUCCESS, postfix_expr.calc(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_double(d));
  ASSERT_EQ(d, 3);

  expr = (char*)"6/(3+1)";
  str.assign(expr, static_cast<int32_t>(strlen(expr)));
  EXPECT_EQ(OB_SUCCESS, postfix_expr.set_expression(str, name_to_idx_map, parser));

  EXPECT_EQ(OB_SUCCESS, postfix_expr.calc(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_double(d));
  ASSERT_EQ(d, (double)6/(double)(3+1));

  expr = (char*)"6/(3+1.0)";
  str.assign(expr, static_cast<int32_t>(strlen(expr)));
  EXPECT_EQ(OB_SUCCESS, postfix_expr.set_expression(str, name_to_idx_map, parser));

  EXPECT_EQ(OB_SUCCESS, postfix_expr.calc(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_double(d));
  ASSERT_EQ(d, 1.5);

  expr = (char*)"1.0 * 54 / 1";
  str.assign(expr, static_cast<int32_t>(strlen(expr)));
  EXPECT_EQ(OB_SUCCESS, postfix_expr.set_expression(str, name_to_idx_map, parser));

  EXPECT_EQ(OB_SUCCESS, postfix_expr.calc(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_double(d));
  ASSERT_EQ(d, 54);


  expr = (char*)"6/(3+1.0)-1.5+10*10/10-10";
  str.assign(expr, static_cast<int32_t>(strlen(expr)));
  EXPECT_EQ(OB_SUCCESS, postfix_expr.set_expression(str, name_to_idx_map, parser));

  EXPECT_EQ(OB_SUCCESS, postfix_expr.calc(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_double(d));
  ASSERT_EQ(d, 0);

  expr = (char*)"6/(3+1.0)-1.5+10*10/10-10%4";
  str.assign(expr, static_cast<int32_t>(strlen(expr)));
  EXPECT_EQ(OB_SUCCESS, postfix_expr.set_expression(str, name_to_idx_map, parser));

  EXPECT_EQ(OB_SUCCESS, postfix_expr.calc(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_double(d));
  ASSERT_EQ(d, 8);

  expr = (char*)"6 AND 3";
  str.assign(expr, static_cast<int32_t>(strlen(expr)));
  EXPECT_EQ(OB_SUCCESS, postfix_expr.set_expression(str, name_to_idx_map, parser));

  EXPECT_EQ(OB_SUCCESS, postfix_expr.calc(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_bool(b));
  ASSERT_EQ(b, true);

  expr = (char*)"6 AND 0.0";
  str.assign(expr, static_cast<int32_t>(strlen(expr)));
  EXPECT_EQ(OB_SUCCESS, postfix_expr.set_expression(str, name_to_idx_map, parser));

  EXPECT_EQ(OB_SUCCESS, postfix_expr.calc(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_bool(b));
  ASSERT_EQ(b, false);

  expr = (char*)"6 or 3";
  str.assign(expr, static_cast<int32_t>(strlen(expr)));
  EXPECT_EQ(OB_SUCCESS, postfix_expr.set_expression(str, name_to_idx_map, parser));

  EXPECT_EQ(OB_SUCCESS, postfix_expr.calc(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_bool(b));
  ASSERT_EQ(b, true);

  expr = (char*)"6 or 0.0";
  str.assign(expr, static_cast<int32_t>(strlen(expr)));
  EXPECT_EQ(OB_SUCCESS, postfix_expr.set_expression(str, name_to_idx_map, parser));

  EXPECT_EQ(OB_SUCCESS, postfix_expr.calc(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_bool(b));
  ASSERT_EQ(b, true);

  expr = (char*)"1 or 0";
  str.assign(expr, static_cast<int32_t>(strlen(expr)));
  EXPECT_EQ(OB_SUCCESS, postfix_expr.set_expression(str, name_to_idx_map, parser));

  EXPECT_EQ(OB_SUCCESS, postfix_expr.calc(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_bool(b));
  ASSERT_EQ(b, true);

  expr = (char*)"1=2 or 0=1";
  str.assign(expr, static_cast<int32_t>(strlen(expr)));
  EXPECT_EQ(OB_SUCCESS, postfix_expr.set_expression(str, name_to_idx_map, parser));

  EXPECT_EQ(OB_SUCCESS, postfix_expr.calc(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_bool(b));
  ASSERT_EQ(b, false);


  expr = (char*)"6 AND (3+1)";
  str.assign(expr, static_cast<int32_t>(strlen(expr)));
  EXPECT_EQ(OB_SUCCESS, postfix_expr.set_expression(str, name_to_idx_map, parser));

  EXPECT_EQ(OB_SUCCESS, postfix_expr.calc(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_bool(b));
  ASSERT_EQ(b, true);

  expr = (char*)"6.0 AND (3.2+1)";
  str.assign(expr, static_cast<int32_t>(strlen(expr)));
  EXPECT_EQ(OB_SUCCESS, postfix_expr.set_expression(str, name_to_idx_map, parser));

  EXPECT_EQ(OB_SUCCESS, postfix_expr.calc(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_bool(b));
  ASSERT_EQ(b, true);

  expr = (char*)"6.0 AND (3 - 3.0)";
  str.assign(expr, static_cast<int32_t>(strlen(expr)));
  EXPECT_EQ(OB_SUCCESS, postfix_expr.set_expression(str, name_to_idx_map, parser));

  EXPECT_EQ(OB_SUCCESS, postfix_expr.calc(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_bool(b));
  ASSERT_EQ(b, false);

  expr = (char*)"6 or (3+1)";
  str.assign(expr, static_cast<int32_t>(strlen(expr)));
  EXPECT_EQ(OB_SUCCESS, postfix_expr.set_expression(str, name_to_idx_map, parser));

  EXPECT_EQ(OB_SUCCESS, postfix_expr.calc(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_bool(b));
  ASSERT_EQ(b, true);

  expr = (char*)"6.0 Or (3.2+1)";
  str.assign(expr, static_cast<int32_t>(strlen(expr)));
  EXPECT_EQ(OB_SUCCESS, postfix_expr.set_expression(str, name_to_idx_map, parser));

  EXPECT_EQ(OB_SUCCESS, postfix_expr.calc(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_bool(b));
  ASSERT_EQ(b, true);

  expr = (char*)"6.0 oR (3 - 3.0)";
  str.assign(expr, static_cast<int32_t>(strlen(expr)));
  EXPECT_EQ(OB_SUCCESS, postfix_expr.set_expression(str, name_to_idx_map, parser));

  EXPECT_EQ(OB_SUCCESS, postfix_expr.calc(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_bool(b));
  ASSERT_EQ(b, true);


  expr = (char*)"(12 + 1) AND (3 - 3.0) or (3 / 2)";
  str.assign(expr, static_cast<int32_t>(strlen(expr)));
  EXPECT_EQ(OB_SUCCESS, postfix_expr.set_expression(str, name_to_idx_map, parser));

  EXPECT_EQ(OB_SUCCESS, postfix_expr.calc(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_bool(b));
  ASSERT_EQ(b, true);

  expr = (char*)"(12 + 1) AND (3 - 3.0) or (0 / 2)";
  str.assign(expr, static_cast<int32_t>(strlen(expr)));
  EXPECT_EQ(OB_SUCCESS, postfix_expr.set_expression(str, name_to_idx_map, parser));

  EXPECT_EQ(OB_SUCCESS, postfix_expr.calc(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_bool(b));
  ASSERT_EQ(b, false);

  expr = (char*)"(12 + 1) AND (3 - 3.0) or (0 / 2 AND 1 + 3 OR 4 - 1)";
  str.assign(expr, static_cast<int32_t>(strlen(expr)));
  EXPECT_EQ(OB_SUCCESS, postfix_expr.set_expression(str, name_to_idx_map, parser));

  EXPECT_EQ(OB_SUCCESS, postfix_expr.calc(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_bool(b));
  ASSERT_EQ(b, true);

  expr = (char*)"(12 + 1) AND (3 - 3.0) or (0 / 2 AND 1 + 3)";
  str.assign(expr, static_cast<int32_t>(strlen(expr)));
  EXPECT_EQ(OB_SUCCESS, postfix_expr.set_expression(str, name_to_idx_map, parser));

  EXPECT_EQ(OB_SUCCESS, postfix_expr.calc(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_bool(b));
  ASSERT_EQ(b, false);

  expr = (char*)"(12 + 1) AND (3 - 3.0) or (0 / 2 AND 1 + 3 or (2 / 5 * 0.2 AND 8 / 8.12))";
  str.assign(expr, static_cast<int32_t>(strlen(expr)));
  EXPECT_EQ(OB_SUCCESS, postfix_expr.set_expression(str, name_to_idx_map, parser));

  EXPECT_EQ(OB_SUCCESS, postfix_expr.calc(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_bool(b));
  ASSERT_EQ(b, true);

  expr = (char*)"(12 + 1) AND (3 - 3.0) or (0 / 2 AND 1 + 3 or (2.0 / 5 * 0.2 AND 8 / 8.12))";
  str.assign(expr, static_cast<int32_t>(strlen(expr)));
  EXPECT_EQ(OB_SUCCESS, postfix_expr.set_expression(str, name_to_idx_map, parser));

  EXPECT_EQ(OB_SUCCESS, postfix_expr.calc(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_bool(b));
  ASSERT_EQ(b, true);


  expr = (char*)"(12 + 1)>143 AND (3 - 3.0 = 0) or ((2 / 5 * 0.2 AND 8 / 8.12))";
  str.assign(expr, static_cast<int32_t>(strlen(expr)));
  EXPECT_EQ(OB_SUCCESS, postfix_expr.set_expression(str, name_to_idx_map, parser));

  EXPECT_EQ(OB_SUCCESS, postfix_expr.calc(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_bool(b));
  ASSERT_EQ(b, true);

  expr = (char*)"(12 + 1)>143 AND (3 - 3.0 = 0) or ((2.0 / 5 * 0.2 AND 8 / 8.12))";
  str.assign(expr, static_cast<int32_t>(strlen(expr)));
  EXPECT_EQ(OB_SUCCESS, postfix_expr.set_expression(str, name_to_idx_map, parser));

  EXPECT_EQ(OB_SUCCESS, postfix_expr.calc(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_bool(b));
  ASSERT_EQ(b, true);

}


TEST(Test_ob_postfix_expression, calc_test_4)
{
  ObCellArray cells;
  ObObj result;
  ObPostfixExpression postfix_expr;
  build_single_cell(cells);
  bool b = true;

  ObExpressionParser parser;
  hash::ObHashMap<ObString,int64_t, hash::NoPthreadDefendMode> name_to_idx_map;
  INIT_NAME_TO_IDX_HASH_1(name_to_idx_map);


  ObString str;
  char *expr = NULL;

  expr = (char*)"'home' like 'h'";
  str.assign(expr, static_cast<int32_t>(strlen(expr)));
  EXPECT_EQ(OB_SUCCESS, postfix_expr.set_expression(str, name_to_idx_map, parser));

  EXPECT_EQ(OB_SUCCESS, postfix_expr.calc(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_bool(b));
  ASSERT_EQ(b, true);

  expr = (char*)"'home' like 'hard'";
  str.assign(expr, static_cast<int32_t>(strlen(expr)));
  EXPECT_EQ(OB_SUCCESS, postfix_expr.set_expression(str, name_to_idx_map, parser));

  EXPECT_EQ(OB_SUCCESS, postfix_expr.calc(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_bool(b));
  ASSERT_EQ(b, false);

  expr = (char*)"'home' like 'home town'";
  str.assign(expr, static_cast<int32_t>(strlen(expr)));
  EXPECT_EQ(OB_SUCCESS, postfix_expr.set_expression(str, name_to_idx_map, parser));

  EXPECT_EQ(OB_SUCCESS, postfix_expr.calc(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_bool(b));
  ASSERT_EQ(b, false);

  expr = (char*)"'home town' like 'home'";
  str.assign(expr, static_cast<int32_t>(strlen(expr)));
  EXPECT_EQ(OB_SUCCESS, postfix_expr.set_expression(str, name_to_idx_map, parser));

  EXPECT_EQ(OB_SUCCESS, postfix_expr.calc(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_bool(b));
  ASSERT_EQ(b, true);

  expr = (char*)"'home' is 'home'";
  str.assign(expr, static_cast<int32_t>(strlen(expr)));
  EXPECT_EQ(OB_SUCCESS, postfix_expr.set_expression(str, name_to_idx_map, parser));

  EXPECT_EQ(OB_SUCCESS, postfix_expr.calc(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_bool(b));
  ASSERT_EQ(b, false);

  expr = (char*)"'home' is 3";
  str.assign(expr, static_cast<int32_t>(strlen(expr)));
  EXPECT_EQ(OB_SUCCESS, postfix_expr.set_expression(str, name_to_idx_map, parser));

  EXPECT_EQ(OB_SUCCESS, postfix_expr.calc(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_bool(b));
  ASSERT_EQ(b, false);


  expr = (char*)"3.0 is 'home'";
  str.assign(expr, static_cast<int32_t>(strlen(expr)));
  EXPECT_EQ(OB_SUCCESS, postfix_expr.set_expression(str, name_to_idx_map, parser));

  EXPECT_EQ(OB_SUCCESS, postfix_expr.calc(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_bool(b));
  ASSERT_EQ(b, false);

  expr = (char*)"3.0 is 3";
  str.assign(expr, static_cast<int32_t>(strlen(expr)));
  EXPECT_EQ(OB_SUCCESS, postfix_expr.set_expression(str, name_to_idx_map, parser));

  EXPECT_EQ(OB_SUCCESS, postfix_expr.calc(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_bool(b));
  ASSERT_EQ(b, false);

  expr = (char*)"3.0 is 3.0";
  str.assign(expr, static_cast<int32_t>(strlen(expr)));
  EXPECT_EQ(OB_SUCCESS, postfix_expr.set_expression(str, name_to_idx_map, parser));
  EXPECT_EQ(OB_SUCCESS, postfix_expr.calc(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_bool(b));
  ASSERT_EQ(b, false);

  expr = (char*)"3-1.0 is 2.0";
  str.assign(expr, static_cast<int32_t>(strlen(expr)));
  EXPECT_EQ(OB_SUCCESS, postfix_expr.set_expression(str, name_to_idx_map, parser));
  EXPECT_EQ(OB_SUCCESS, postfix_expr.calc(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_bool(b));
  ASSERT_EQ(b, false);

  expr = (char*)"null is null";
  str.assign(expr, static_cast<int32_t>(strlen(expr)));
  EXPECT_EQ(OB_SUCCESS, postfix_expr.set_expression(str, name_to_idx_map, parser));
  EXPECT_EQ(OB_SUCCESS, postfix_expr.calc(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_bool(b));
  ASSERT_EQ(b, true);
}

TEST(Test_ob_postfix_expression, calc_test_5)
{
  ObCellArray cells;
  ObObj result;
  ObPostfixExpression postfix_expr;
  build_single_cell(cells);
  bool b = true;

  ObExpressionParser parser;
  hash::ObHashMap<ObString,int64_t, hash::NoPthreadDefendMode> name_to_idx_map;
  INIT_NAME_TO_IDX_HASH_1(name_to_idx_map);


  ObString str;
  char *expr = NULL;

  expr = (char*)"NOT 10";
  str.assign(expr, static_cast<int32_t>(strlen(expr)));
  EXPECT_EQ(OB_SUCCESS, postfix_expr.set_expression(str, name_to_idx_map, parser));
  EXPECT_EQ(OB_SUCCESS, postfix_expr.calc(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_bool(b));
  ASSERT_EQ(b, false);


  expr = (char*)"NOT 0";
  str.assign(expr, static_cast<int32_t>(strlen(expr)));
  EXPECT_EQ(OB_SUCCESS, postfix_expr.set_expression(str, name_to_idx_map, parser));
  EXPECT_EQ(OB_SUCCESS, postfix_expr.calc(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_bool(b));
  ASSERT_EQ(b, true);

}

TEST(Test_ob_postfix_expression, calc_test_decoder_1)
{
  ObCellArray cells;
  ObObj result;
  ObPostfixExpression postfix_expr;
  int64_t i = 0;

  ObExpressionParser parser;
  hash::ObHashMap<ObString,int64_t, hash::NoPthreadDefendMode> name_to_idx_map;
  INIT_NAME_TO_IDX_HASH_1(name_to_idx_map);


  ObString str;
  char *expr = NULL;

  cell_array_reader("composite_column_input.txt", cells);
  expr = (char*)"`0`+`1`+`2`+`3`+`4`+`5`+`6`+`7`+`8`+`9`";
  str.assign(expr, static_cast<int32_t>(strlen(expr)));
  str.assign(expr, static_cast<int32_t>(strlen(expr)));
  EXPECT_EQ(OB_SUCCESS, postfix_expr.set_expression(str, name_to_idx_map, parser));
  EXPECT_EQ(OB_SUCCESS, postfix_expr.calc(result, cells, 0, 9));
  ASSERT_EQ(OB_SUCCESS, result.get_int(i));
  ASSERT_EQ(i, 55);

  EXPECT_EQ(OB_SUCCESS, postfix_expr.calc(result, cells, 10, 19));
  ASSERT_EQ(OB_SUCCESS, result.get_int(i));
  ASSERT_EQ(i, 155);

}

TEST(Test_ob_postfix_expression, calc_test_decoder_2)
{
  ObCellArray cells;
  ObObj result;
  ObPostfixExpression postfix_expr;
  bool b = true;

  ObExpressionParser parser;
  hash::ObHashMap<ObString,int64_t, hash::NoPthreadDefendMode> name_to_idx_map;
  INIT_NAME_TO_IDX_HASH_2(name_to_idx_map);


  ObString str;
  char *expr = NULL;

  cell_array_reader("composite_column_input_3.txt", cells);
#if 0
  TBSYS_LOG(WARN, "&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&");
  for (i = 0; i < 9; i++)
  {
    cells[i].value_.dump();
  }
  TBSYS_LOG(WARN, "&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&");
#endif

  #if 1
  expr = (char*)"(`username`='raywill') OR (`password`= 'p123456')";
  expr = (char*)"(`username`='raywill') OR 0";
  str.assign(expr, static_cast<int32_t>(strlen(expr)));
  EXPECT_EQ(OB_SUCCESS, postfix_expr.set_expression(str, name_to_idx_map, parser));

  EXPECT_EQ(OB_SUCCESS, postfix_expr.calc(result, cells, 0, 2));

  ASSERT_EQ(OB_SUCCESS, result.get_bool(b));

  ASSERT_EQ(b, true);
  EXPECT_EQ(OB_SUCCESS, postfix_expr.calc(result, cells, 3, 5));

  ASSERT_EQ(OB_SUCCESS, result.get_bool(b));
  ASSERT_EQ(b, true);
  EXPECT_EQ(OB_SUCCESS, postfix_expr.calc(result, cells, 6, 8));
  ASSERT_EQ(OB_SUCCESS, result.get_bool(b));
  ASSERT_EQ(b, false);
#endif


  expr = (char*)"(`username`='raywill') AND (`password`= 'p123456')";
  str.assign(expr, static_cast<int32_t>(strlen(expr)));
  EXPECT_EQ(OB_SUCCESS, postfix_expr.set_expression(str, name_to_idx_map, parser));
  EXPECT_EQ(OB_SUCCESS, postfix_expr.calc(result, cells, 0, 2));
  ASSERT_EQ(OB_SUCCESS, result.get_bool(b));
  ASSERT_EQ(b, false);
  EXPECT_EQ(OB_SUCCESS, postfix_expr.calc(result, cells, 3, 5));
  ASSERT_EQ(OB_SUCCESS, result.get_bool(b));
  ASSERT_EQ(b, true);
  EXPECT_EQ(OB_SUCCESS, postfix_expr.calc(result, cells, 6, 8));
  ASSERT_EQ(OB_SUCCESS, result.get_bool(b));
  ASSERT_EQ(b, false);
}

TEST(Test_ob_postfix_expression, calc_test_sign_1)
{
  ObCellArray cells;
  ObObj result;
  ObPostfixExpression postfix_expr;
  int64_t i = 0;
  double d = 0.0;
  bool b = true;

  ObExpressionParser parser;
  hash::ObHashMap<ObString,int64_t, hash::NoPthreadDefendMode> name_to_idx_map;
  INIT_NAME_TO_IDX_HASH_1(name_to_idx_map);


  ObString str;
  char *expr = NULL;

  cell_array_reader("composite_column_input.txt", cells);
  expr = (char*)"1+(-33)";
  str.assign(expr, static_cast<int32_t>(strlen(expr)));
  EXPECT_EQ(OB_SUCCESS, postfix_expr.set_expression(str, name_to_idx_map, parser));
  EXPECT_EQ(OB_SUCCESS, postfix_expr.calc(result, cells, 0, 9));
  ASSERT_EQ(OB_SUCCESS, result.get_int(i));
  ASSERT_EQ(i, -32);

  expr = (char*)"1+ -  33  +  + 12 /6";
  str.assign(expr, static_cast<int32_t>(strlen(expr)));
  EXPECT_EQ(OB_SUCCESS, postfix_expr.set_expression(str, name_to_idx_map, parser));
  EXPECT_EQ(OB_SUCCESS, postfix_expr.calc(result, cells, 0, 9));
  ASSERT_EQ(OB_SUCCESS, result.get_double(d));
  ASSERT_EQ(d, -30);


  expr = (char*)"1+( -  33 ) + ( + 12 )/6";
  str.assign(expr, static_cast<int32_t>(strlen(expr)));
  EXPECT_EQ(OB_SUCCESS, postfix_expr.set_expression(str, name_to_idx_map, parser));
  EXPECT_EQ(OB_SUCCESS, postfix_expr.calc(result, cells, 0, 9));
  ASSERT_EQ(OB_SUCCESS, result.get_double(d));
  ASSERT_EQ(d, -30);

  expr = (char*)"-3>1";
  str.assign(expr, static_cast<int32_t>(strlen(expr)));
  EXPECT_EQ(OB_SUCCESS, postfix_expr.set_expression(str, name_to_idx_map, parser));
  EXPECT_EQ(OB_SUCCESS, postfix_expr.calc(result, cells, 0, 9));
  ASSERT_EQ(OB_SUCCESS, result.get_bool(b));
  ASSERT_EQ(b, false);


  expr = (char*)"-3>-1";
  str.assign(expr, static_cast<int32_t>(strlen(expr)));
  EXPECT_EQ(OB_SUCCESS, postfix_expr.set_expression(str, name_to_idx_map, parser));
  EXPECT_EQ(OB_SUCCESS, postfix_expr.calc(result, cells, 0, 9));
  ASSERT_EQ(OB_SUCCESS, result.get_bool(b));
  ASSERT_EQ(b, false);


  expr = (char*)"3>-1";
  str.assign(expr, static_cast<int32_t>(strlen(expr)));
  EXPECT_EQ(OB_SUCCESS, postfix_expr.set_expression(str, name_to_idx_map, parser));
  EXPECT_EQ(OB_SUCCESS, postfix_expr.calc(result, cells, 0, 9));
  ASSERT_EQ(OB_SUCCESS, result.get_bool(b));
  ASSERT_EQ(b, true);

  expr = (char*)"1+-2";
  str.assign(expr, static_cast<int32_t>(strlen(expr)));
  EXPECT_EQ(OB_SUCCESS, postfix_expr.set_expression(str, name_to_idx_map, parser));
  EXPECT_EQ(OB_SUCCESS, postfix_expr.calc(result, cells, 0, 9));
  ASSERT_EQ(OB_SUCCESS, result.get_int(i));
  ASSERT_EQ(i, -1);


  expr = (char*)"1--2";
  str.assign(expr, static_cast<int32_t>(strlen(expr)));
  EXPECT_EQ(OB_SUCCESS, postfix_expr.set_expression(str, name_to_idx_map, parser));
  EXPECT_EQ(OB_SUCCESS, postfix_expr.calc(result, cells, 0, 9));
  ASSERT_EQ(OB_SUCCESS, result.get_int(i));
  ASSERT_EQ(i, 3);


  expr = (char*)"1-+2";
  str.assign(expr, static_cast<int32_t>(strlen(expr)));
  EXPECT_EQ(OB_SUCCESS, postfix_expr.set_expression(str, name_to_idx_map, parser));
  EXPECT_EQ(OB_SUCCESS, postfix_expr.calc(result, cells, 0, 9));
  ASSERT_EQ(OB_SUCCESS, result.get_int(i));
  ASSERT_EQ(i, -1);



  expr = (char*)"1++2";
  str.assign(expr, static_cast<int32_t>(strlen(expr)));
  EXPECT_EQ(OB_SUCCESS, postfix_expr.set_expression(str, name_to_idx_map, parser));
  EXPECT_EQ(OB_SUCCESS, postfix_expr.calc(result, cells, 0, 9));
  ASSERT_EQ(OB_SUCCESS, result.get_int(i));
  ASSERT_EQ(i, 3);


  expr = (char*)"1+-+2";
  str.assign(expr, static_cast<int32_t>(strlen(expr)));
  EXPECT_EQ(OB_SUCCESS, postfix_expr.set_expression(str, name_to_idx_map, parser));
  EXPECT_EQ(OB_SUCCESS, postfix_expr.calc(result, cells, 0, 9));
  ASSERT_EQ(OB_SUCCESS, result.get_int(i));
  ASSERT_EQ(i, -1);


  expr = (char*)"1-++2";
  str.assign(expr, static_cast<int32_t>(strlen(expr)));
  EXPECT_EQ(OB_SUCCESS, postfix_expr.set_expression(str, name_to_idx_map, parser));
  EXPECT_EQ(OB_SUCCESS, postfix_expr.calc(result, cells, 0, 9));
  ASSERT_EQ(OB_SUCCESS, result.get_int(i));
  ASSERT_EQ(i, -1);


  expr = (char*)"1+-+-+9";
  str.assign(expr, static_cast<int32_t>(strlen(expr)));
  EXPECT_EQ(OB_SUCCESS, postfix_expr.set_expression(str, name_to_idx_map, parser));
  EXPECT_EQ(OB_SUCCESS, postfix_expr.calc(result, cells, 0, 9));
  ASSERT_EQ(OB_SUCCESS, result.get_int(i));
  ASSERT_EQ(i, 10);

  expr = (char*)"1++-+9";
  str.assign(expr, static_cast<int32_t>(strlen(expr)));
  EXPECT_EQ(OB_SUCCESS, postfix_expr.set_expression(str, name_to_idx_map, parser));
  EXPECT_EQ(OB_SUCCESS, postfix_expr.calc(result, cells, 0, 9));
  ASSERT_EQ(OB_SUCCESS, result.get_int(i));
  ASSERT_EQ(i, -8);

  expr = (char*)"-------9";
  str.assign(expr, static_cast<int32_t>(strlen(expr)));
  EXPECT_EQ(OB_SUCCESS, postfix_expr.set_expression(str, name_to_idx_map, parser));
  EXPECT_EQ(OB_SUCCESS, postfix_expr.calc(result, cells, 0, 9));
  ASSERT_EQ(OB_SUCCESS, result.get_int(i));
  ASSERT_EQ(i, -9);


  expr = (char*)"++++++++9";
  str.assign(expr, static_cast<int32_t>(strlen(expr)));
  EXPECT_EQ(OB_SUCCESS, postfix_expr.set_expression(str, name_to_idx_map, parser));
  EXPECT_EQ(OB_SUCCESS, postfix_expr.calc(result, cells, 0, 9));
  ASSERT_EQ(OB_SUCCESS, result.get_int(i));
  ASSERT_EQ(i, 9);


  expr = (char*)"+++-+9";
  str.assign(expr, static_cast<int32_t>(strlen(expr)));
  EXPECT_EQ(OB_SUCCESS, postfix_expr.set_expression(str, name_to_idx_map, parser));
  EXPECT_EQ(OB_SUCCESS, postfix_expr.calc(result, cells, 0, 9));
  ASSERT_EQ(OB_SUCCESS, result.get_int(i));
  ASSERT_EQ(i, -9);

}

int64_t get_date_time_val(const char *str)
{
  char *format = NULL;
  char tmp_buf[1024];
  struct tm tm;
  int64_t decoded_val = 0;

  strcpy(tmp_buf, str);
  if (NULL != strchr(tmp_buf, ' '))
  {
    format = (char*)"%Y-%m-%d %H:%M:%S";
  }
  else if(NULL != strchr(tmp_buf, '-'))
  {
    format = (char*)"%Y-%m-%d";
  }
  memset(&tm, 0, sizeof(tm));
  strptime(tmp_buf, format, &tm);
  decoded_val = (int64_t)mktime(&tm);
  decoded_val *= 1000L * 1000; // convert sec to micro-sec
  return decoded_val;
}

// test #2011-12-23#, #2012-2-23 23:23# format
TEST(Test_ob_postfix_expression, calc_test_datetime_format_v1)
{
  ObCellArray cells;
  ObObj result;
  ObPostfixExpression postfix_expr;
  build_single_cell(cells);
  int64_t time_val = 0;

  ObExpressionParser parser;
  hash::ObHashMap<ObString,int64_t, hash::NoPthreadDefendMode> name_to_idx_map;
  INIT_NAME_TO_IDX_HASH_1(name_to_idx_map);

  ObString str;
  char *expr = NULL;

  expr = (char*)"#1970-1-1 8:00:00#";  // East 08 TIMEZONE
  str.assign(expr, static_cast<int32_t>(strlen(expr)));
  EXPECT_EQ(OB_SUCCESS, postfix_expr.set_expression(str, name_to_idx_map, parser));
  EXPECT_EQ(OB_SUCCESS, postfix_expr.calc(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_precise_datetime(time_val));
  ASSERT_EQ(time_val, get_date_time_val("1970-1-1 8:00:00"));
  
  expr = (char*)"#1970-1-1#";  // East 08 TIMEZONE
  str.assign(expr, static_cast<int32_t>(strlen(expr)));
  EXPECT_EQ(OB_SUCCESS, postfix_expr.set_expression(str, name_to_idx_map, parser));
  EXPECT_EQ(OB_SUCCESS, postfix_expr.calc(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_precise_datetime(time_val));
  ASSERT_EQ(time_val, get_date_time_val("1970-1-1"));
  
  expr = (char*)"#2012-8-7 12:23:42#";  // East 08 TIMEZONE
  str.assign(expr, static_cast<int32_t>(strlen(expr)));
  EXPECT_EQ(OB_SUCCESS, postfix_expr.set_expression(str, name_to_idx_map, parser));
  EXPECT_EQ(OB_SUCCESS, postfix_expr.calc(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_precise_datetime(time_val));
  ASSERT_EQ(time_val, get_date_time_val("2012-8-7 12:23:42"));
}

// test date'2011-12-23', Datetime'2012-2-23 23:42', D'2012-2-23 23:23' format
TEST(Test_ob_postfix_expression, calc_test_datetime_format_v2)
{
  ObCellArray cells;
  ObObj result;
  ObPostfixExpression postfix_expr;
  build_single_cell(cells);
  int64_t time_val = 0;

  ObExpressionParser parser;
  hash::ObHashMap<ObString,int64_t, hash::NoPthreadDefendMode> name_to_idx_map;
  INIT_NAME_TO_IDX_HASH_1(name_to_idx_map);

  ObString str;
  char *expr = NULL;

  expr = (char*)"datetime'1970-1-1 8:00:00'";  // East 08 TIMEZONE
  str.assign(expr, static_cast<int32_t>(strlen(expr)));
  EXPECT_EQ(OB_SUCCESS, postfix_expr.set_expression(str, name_to_idx_map, parser));
  EXPECT_EQ(OB_SUCCESS, postfix_expr.calc(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_precise_datetime(time_val));
  ASSERT_EQ(time_val, get_date_time_val("1970-1-1 8:00:00"));
  
  // FIXME: actuall datetime'1970-1-1' is not valid
  expr = (char*)"datetime'1970-1-1'";  // East 08 TIMEZONE
  str.assign(expr, static_cast<int32_t>(strlen(expr)));
  EXPECT_EQ(OB_SUCCESS, postfix_expr.set_expression(str, name_to_idx_map, parser));
  EXPECT_EQ(OB_SUCCESS, postfix_expr.calc(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_precise_datetime(time_val));
  ASSERT_EQ(time_val, get_date_time_val("1970-1-1"));
  

  expr = (char*)"date'1970-1-1 8:00:01'";  // East 08 TIMEZONE
  str.assign(expr, static_cast<int32_t>(strlen(expr)));
  EXPECT_EQ(OB_SUCCESS, postfix_expr.set_expression(str, name_to_idx_map, parser));
  EXPECT_EQ(OB_SUCCESS, postfix_expr.calc(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_precise_datetime(time_val));
  ASSERT_EQ(time_val, get_date_time_val("1970-1-1 8:00:01"));

  expr = (char*)"date'2012-8-7'";  // East 08 TIMEZONE
  str.assign(expr, static_cast<int32_t>(strlen(expr)));
  EXPECT_EQ(OB_SUCCESS, postfix_expr.set_expression(str, name_to_idx_map, parser));
  EXPECT_EQ(OB_SUCCESS, postfix_expr.calc(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_precise_datetime(time_val));
  ASSERT_EQ(time_val, get_date_time_val("2012-8-7"));

  //FIXME: known problem: data'2012-9-9 12:23:23' is not valid actually
  expr = (char*)"Date'2012-8-7 12:23:42'";  // East 08 TIMEZONE
  str.assign(expr, static_cast<int32_t>(strlen(expr)));
  EXPECT_EQ(OB_SUCCESS, postfix_expr.set_expression(str, name_to_idx_map, parser));
  EXPECT_EQ(OB_SUCCESS, postfix_expr.calc(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_precise_datetime(time_val));
  ASSERT_EQ(time_val, get_date_time_val("2012-8-7 12:23:42"));


  expr = (char*)"d'2012-8-7'";  // East 08 TIMEZONE
  str.assign(expr, static_cast<int32_t>(strlen(expr)));
  EXPECT_EQ(OB_SUCCESS, postfix_expr.set_expression(str, name_to_idx_map, parser));
  EXPECT_EQ(OB_SUCCESS, postfix_expr.calc(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_precise_datetime(time_val));
  ASSERT_EQ(time_val, get_date_time_val("2012-8-7"));

  expr = (char*)"D'2012-8-7 12:23:42'";  // East 08 TIMEZONE
  str.assign(expr, static_cast<int32_t>(strlen(expr)));
  EXPECT_EQ(OB_SUCCESS, postfix_expr.set_expression(str, name_to_idx_map, parser));
  EXPECT_EQ(OB_SUCCESS, postfix_expr.calc(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_precise_datetime(time_val));
  ASSERT_EQ(time_val, get_date_time_val("2012-8-7 12:23:42"));
}


int main(int argc, char **argv)
{
  srandom(static_cast<uint32_t>(time(NULL)));
  ob_init_memory_pool(64*1024);
  TBSYS_LOGGER.setLogLevel("DEBUG");
  TBSYS_LOGGER.setLogLevel("WARN");

  //ob_init_memory_pool();
  testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
