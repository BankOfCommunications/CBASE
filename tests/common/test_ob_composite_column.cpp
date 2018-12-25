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
#include "ob_cell_array.h"
#include "common/ob_composite_column.h"
#include <string.h>
#include <stdlib.h>
#include <ctype.h>


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


void INIT_NAME_TO_IDX_HASH_3(hash::ObHashMap<ObString,int64_t, hash::NoPthreadDefendMode> &name_to_idx_map)
{     
  ObString key_name;
  int err = OB_SUCCESS;

  ASSERT_EQ(OB_SUCCESS, name_to_idx_map.create(hash::cal_next_prime(512)));
  key_name.assign((char*)"c0", 2);
  err = name_to_idx_map.set(key_name, 0);
  ASSERT_EQ(true, -1 != err && hash::HASH_EXIST != err);

  key_name.assign((char*)"c1", 2);
  err = name_to_idx_map.set(key_name, 1);
  ASSERT_EQ(true, -1 != err && hash::HASH_EXIST != err);

  key_name.assign((char*)"c2", 2);
  err = name_to_idx_map.set(key_name, 2);
  ASSERT_EQ(true, -1 != err && hash::HASH_EXIST != err);

  key_name.assign((char*)"c3", 2);
  err = name_to_idx_map.set(key_name, 3);
  ASSERT_EQ(true, -1 != err && hash::HASH_EXIST != err);

  key_name.assign((char*)"c4", 2);
  err = name_to_idx_map.set(key_name, 4);
  ASSERT_EQ(true, -1 != err && hash::HASH_EXIST != err);

  key_name.assign((char*)"c5", 2);
  err = name_to_idx_map.set(key_name, 5);
  ASSERT_EQ(true, -1 != err && hash::HASH_EXIST != err);

  key_name.assign((char*)"c6", 2);
  err = name_to_idx_map.set(key_name, 6);
  ASSERT_EQ(true, -1 != err && hash::HASH_EXIST != err);

  key_name.assign((char*)"c7", 2);
  err = name_to_idx_map.set(key_name, 7);
  ASSERT_EQ(true, -1 != err && hash::HASH_EXIST != err);

  key_name.assign((char*)"c8", 2);
  err = name_to_idx_map.set(key_name, 8);
  ASSERT_EQ(true, -1 != err && hash::HASH_EXIST != err);

  key_name.assign((char*)"c9", 2);
  err = name_to_idx_map.set(key_name, 9);
  ASSERT_EQ(true, -1 != err && hash::HASH_EXIST != err);

  key_name.assign((char*)"c10", 3);
  err = name_to_idx_map.set(key_name, 10);
  ASSERT_EQ(true, -1 != err && hash::HASH_EXIST != err);
}


void INIT_NAME_TO_IDX_HASH_4(hash::ObHashMap<ObString,int64_t, hash::NoPthreadDefendMode> &name_to_idx_map)
{     
  ObString key_name;
  int err = OB_SUCCESS;

  ASSERT_EQ(OB_SUCCESS, name_to_idx_map.create(hash::cal_next_prime(512)));
  key_name.assign((char*)"a", 1);
  err = name_to_idx_map.set(key_name, 0);
  ASSERT_EQ(true, -1 != err && hash::HASH_EXIST != err);

  key_name.assign((char*)"a1", 2);
  err = name_to_idx_map.set(key_name, 1);
  ASSERT_EQ(true, -1 != err && hash::HASH_EXIST != err);

  key_name.assign((char*)"a2", 2);
  err = name_to_idx_map.set(key_name, 2);
  ASSERT_EQ(true, -1 != err && hash::HASH_EXIST != err);

  key_name.assign((char*)"a3", 2);
  err = name_to_idx_map.set(key_name, 3);
  ASSERT_EQ(true, -1 != err && hash::HASH_EXIST != err);
}



#define ADD_FUNC_EXPR(arr, idx, type, value) \
  /*printf("type=%ld, value=%ld\n", type, value);*/ \
arr[idx * 2].set_int(type); \
arr[idx * 2 + 1].set_int(value);


#define ADD_INT_EXPR(arr, idx, value) \
arr[idx * 2].set_int(ObExpression::CONST_OBJ); \
arr[idx * 2 + 1].set_int(value);


#define ADD_DOUBLE_EXPR(arr, idx, value) \
arr[idx * 2].set_int(ObExpression::CONST_OBJ); \
arr[idx * 2 + 1].set_double(value);


#define ADD_VARCHAR_EXPR(arr, idx, str_value) \
arr[idx * 2].set_int(ObExpression::CONST_OBJ); \
arr[idx * 2 + 1].set_varchar(str_value);


#define ADD_DATETIME_EXPR(arr, idx, value) \
arr[idx * 2].set_int(ObExpression::CONST_OBJ); \
arr[idx * 2 + 1].set_datetime(value);


#define ADD_NULL_EXPR(arr, idx) \
arr[idx * 2].set_int(ObExpression::CONST_OBJ); \
arr[idx * 2 + 1].set_null();


#define END_EXPR(arr, idx)  \
  arr[idx * 2].set_int(ObExpression::END); 



/* a + b */
int build_polish_expr_simple_add_ii(ObObj *arr)
{     
  ADD_FUNC_EXPR(arr, 0, ObExpression::COLUMN_IDX, 0);
  ADD_FUNC_EXPR(arr, 1, ObExpression::COLUMN_IDX, 1);
  ADD_FUNC_EXPR(arr, 2, ObExpression::OP, ObExpression::ADD_FUNC);
  END_EXPR(arr, 3);

  return 0;
}

/* a - b */
int build_polish_expr_simple_sub_ii(ObObj *arr)
{     
  ADD_FUNC_EXPR(arr, 0, ObExpression::COLUMN_IDX, 0);
  ADD_FUNC_EXPR(arr, 1, ObExpression::COLUMN_IDX, 1);
  ADD_FUNC_EXPR(arr, 2, ObExpression::OP, ObExpression::SUB_FUNC);
  END_EXPR(arr, 3);

  return 0;
}

/* a * b */
int build_polish_expr_simple_mul_ii(ObObj *arr)
{     
  ADD_FUNC_EXPR(arr, 0, ObExpression::COLUMN_IDX, 0);
  ADD_FUNC_EXPR(arr, 1, ObExpression::COLUMN_IDX, 1);
  ADD_FUNC_EXPR(arr, 2, ObExpression::OP, ObExpression::MUL_FUNC);
  END_EXPR(arr, 3);

  return 0;
}

/* a / b */
int build_polish_expr_simple_div_ii(ObObj *arr)
{     
  ADD_FUNC_EXPR(arr, 0, ObExpression::COLUMN_IDX, 0);
  ADD_FUNC_EXPR(arr, 1, ObExpression::COLUMN_IDX, 1);
  ADD_FUNC_EXPR(arr, 2, ObExpression::OP, ObExpression::DIV_FUNC);
  END_EXPR(arr, 3);

  return 0;
}


/* a > b */
int build_polish_expr_simple_gt_ii(ObObj *arr)
{     
  ADD_FUNC_EXPR(arr, 0, ObExpression::COLUMN_IDX, 0);
  ADD_FUNC_EXPR(arr, 1, ObExpression::COLUMN_IDX, 1);
  ADD_FUNC_EXPR(arr, 2, ObExpression::OP, ObExpression::GT_FUNC);
  END_EXPR(arr, 3);

  return 0;
}



/* a >= b */
int build_polish_expr_simple_ge_ii(ObObj *arr)
{     
  ADD_FUNC_EXPR(arr, 0, ObExpression::COLUMN_IDX, 0);
  ADD_FUNC_EXPR(arr, 1, ObExpression::COLUMN_IDX, 1);
  ADD_FUNC_EXPR(arr, 2, ObExpression::OP, ObExpression::GE_FUNC);
  END_EXPR(arr, 3);

  return 0;
}



/* a < b */
int build_polish_expr_simple_lt_ii(ObObj *arr)
{     
  ADD_FUNC_EXPR(arr, 0, ObExpression::COLUMN_IDX, 0);
  ADD_FUNC_EXPR(arr, 1, ObExpression::COLUMN_IDX, 1);
  ADD_FUNC_EXPR(arr, 2, ObExpression::OP, ObExpression::LT_FUNC);
  END_EXPR(arr, 3);

  return 0;
}



/* a <= b */
int build_polish_expr_simple_le_ii(ObObj *arr)
{     
  ADD_FUNC_EXPR(arr, 0, ObExpression::COLUMN_IDX, 0);
  ADD_FUNC_EXPR(arr, 1, ObExpression::COLUMN_IDX, 1);
  ADD_FUNC_EXPR(arr, 2, ObExpression::OP, ObExpression::LE_FUNC);
  END_EXPR(arr, 3);

  return 0;
}



/* a == b */
int build_polish_expr_simple_eq_ii(ObObj *arr)
{     
  ADD_FUNC_EXPR(arr, 0, ObExpression::COLUMN_IDX, 0);
  ADD_FUNC_EXPR(arr, 1, ObExpression::COLUMN_IDX, 1);
  ADD_FUNC_EXPR(arr, 2, ObExpression::OP, ObExpression::EQ_FUNC);
  END_EXPR(arr, 3);

  return 0;
}


/* a != b */
int build_polish_expr_simple_ne_ii(ObObj *arr)
{     
  ADD_FUNC_EXPR(arr, 0, ObExpression::COLUMN_IDX, 0);
  ADD_FUNC_EXPR(arr, 1, ObExpression::COLUMN_IDX, 1);
  ADD_FUNC_EXPR(arr, 2, ObExpression::OP, ObExpression::NE_FUNC);
  END_EXPR(arr, 3);

  return 0;
}


/* a is not b */
int build_polish_expr_simple_is_not_ii(ObObj *arr)
{     
  ADD_FUNC_EXPR(arr, 0, ObExpression::COLUMN_IDX, 0);
  ADD_FUNC_EXPR(arr, 1, ObExpression::COLUMN_IDX, 1);
  ADD_FUNC_EXPR(arr, 2, ObExpression::OP, ObExpression::IS_NOT_FUNC);
  END_EXPR(arr, 3);

  return 0;
}



/* a is b */
int build_polish_expr_simple_is_ii(ObObj *arr)
{     
  ADD_FUNC_EXPR(arr, 0, ObExpression::COLUMN_IDX, 0);
  ADD_FUNC_EXPR(arr, 1, ObExpression::COLUMN_IDX, 1);
  ADD_FUNC_EXPR(arr, 2, ObExpression::OP, ObExpression::IS_FUNC);
  END_EXPR(arr, 3);

  return 0;
}




/* !a */
int build_polish_expr_simple_not_ii(ObObj *arr)
{     
  ADD_FUNC_EXPR(arr, 0, ObExpression::COLUMN_IDX, 0);
  ADD_FUNC_EXPR(arr, 1, ObExpression::OP, ObExpression::NOT_FUNC);
  END_EXPR(arr, 2);

  return 0;
}


/* a && b */
int build_polish_expr_simple_and_ii(ObObj *arr)
{     
  ADD_FUNC_EXPR(arr, 0, ObExpression::COLUMN_IDX, 0);
  ADD_FUNC_EXPR(arr, 1, ObExpression::COLUMN_IDX, 1);
  ADD_FUNC_EXPR(arr, 2, ObExpression::OP, ObExpression::AND_FUNC);
  END_EXPR(arr, 3);

  return 0;
}


/* a || b */
int build_polish_expr_simple_or_ii(ObObj *arr)
{     
  ADD_FUNC_EXPR(arr, 0, ObExpression::COLUMN_IDX, 0);
  ADD_FUNC_EXPR(arr, 1, ObExpression::COLUMN_IDX, 1);
  ADD_FUNC_EXPR(arr, 2, ObExpression::OP, ObExpression::OR_FUNC);
  END_EXPR(arr, 3);

  return 0;
}
/******************const  op int *********************/


/* a is not b */
int build_polish_expr_simple_is_not_ic(ObObj *arr)
{     
  ADD_FUNC_EXPR(arr, 0, ObExpression::COLUMN_IDX, 0);
  ADD_NULL_EXPR(arr, 1);
  ADD_FUNC_EXPR(arr, 2, ObExpression::OP, ObExpression::IS_NOT_FUNC);
  END_EXPR(arr, 3);

  return 0;
}



/* a is b */
int build_polish_expr_simple_is_ic(ObObj *arr)
{     
  ADD_FUNC_EXPR(arr, 0, ObExpression::COLUMN_IDX, 0);
  ADD_NULL_EXPR(arr, 1);
  ADD_FUNC_EXPR(arr, 2, ObExpression::OP, ObExpression::IS_FUNC);
  END_EXPR(arr, 3);

  return 0;
}

int build_polish_expr_simple_eq_ic(ObObj *arr)
{     
  ADD_FUNC_EXPR(arr, 0, ObExpression::COLUMN_IDX, 0);
  ADD_NULL_EXPR(arr, 1);
  ADD_FUNC_EXPR(arr, 2, ObExpression::OP, ObExpression::EQ_FUNC);
  END_EXPR(arr, 3);

  return 0;
}

int build_polish_expr_simple_ne_ic(ObObj *arr)
{     
  ADD_FUNC_EXPR(arr, 0, ObExpression::COLUMN_IDX, 0);
  ADD_NULL_EXPR(arr, 1);
  ADD_FUNC_EXPR(arr, 2, ObExpression::OP, ObExpression::NE_FUNC);
  END_EXPR(arr, 3);

  return 0;
}


int build_polish_expr_simple_gt_ic(ObObj *arr)
{     
  ADD_FUNC_EXPR(arr, 0, ObExpression::COLUMN_IDX, 0);
  ADD_NULL_EXPR(arr, 1);
  ADD_FUNC_EXPR(arr, 2, ObExpression::OP, ObExpression::GT_FUNC);
  END_EXPR(arr, 3);

  return 0;
}

int build_polish_expr_simple_lt_ic(ObObj *arr)
{     
  ADD_FUNC_EXPR(arr, 0, ObExpression::COLUMN_IDX, 0);
  ADD_NULL_EXPR(arr, 1);
  ADD_FUNC_EXPR(arr, 2, ObExpression::OP, ObExpression::LT_FUNC);
  END_EXPR(arr, 3);

  return 0;
}

int build_polish_expr_simple_ge_ic(ObObj *arr)
{     
  ADD_FUNC_EXPR(arr, 0, ObExpression::COLUMN_IDX, 0);
  ADD_NULL_EXPR(arr, 1);
  ADD_FUNC_EXPR(arr, 2, ObExpression::OP, ObExpression::GE_FUNC);
  END_EXPR(arr, 3);

  return 0;
}

int build_polish_expr_simple_le_ic(ObObj *arr)
{     
  ADD_FUNC_EXPR(arr, 0, ObExpression::COLUMN_IDX, 0);
  ADD_NULL_EXPR(arr, 1);
  ADD_FUNC_EXPR(arr, 2, ObExpression::OP, ObExpression::LE_FUNC);
  END_EXPR(arr, 3);

  return 0;
}


/* null op const int */
int build_polish_expr_simple_eq_icons(ObObj *arr)
{     
  ADD_FUNC_EXPR(arr, 0, ObExpression::COLUMN_IDX, 0);
  ADD_INT_EXPR(arr, 1, 0);
  ADD_FUNC_EXPR(arr, 2, ObExpression::OP, ObExpression::EQ_FUNC);
  END_EXPR(arr, 3);

  return 0;
}

int build_polish_expr_simple_ne_icons(ObObj *arr)
{     
  ADD_FUNC_EXPR(arr, 0, ObExpression::COLUMN_IDX, 0);
  ADD_INT_EXPR(arr, 1, 0);
  ADD_FUNC_EXPR(arr, 2, ObExpression::OP, ObExpression::NE_FUNC);
  END_EXPR(arr, 3);

  return 0;
}


int build_polish_expr_simple_gt_icons(ObObj *arr)
{     
  ADD_FUNC_EXPR(arr, 0, ObExpression::COLUMN_IDX, 0);
  ADD_INT_EXPR(arr, 1, 0);
  ADD_FUNC_EXPR(arr, 2, ObExpression::OP, ObExpression::GT_FUNC);
  END_EXPR(arr, 3);

  return 0;
}

int build_polish_expr_simple_lt_icons(ObObj *arr)
{     
  ADD_FUNC_EXPR(arr, 0, ObExpression::COLUMN_IDX, 0);
  ADD_INT_EXPR(arr, 1, 0);
  ADD_FUNC_EXPR(arr, 2, ObExpression::OP, ObExpression::LT_FUNC);
  END_EXPR(arr, 3);

  return 0;
}

int build_polish_expr_simple_ge_icons(ObObj *arr)
{     
  ADD_FUNC_EXPR(arr, 0, ObExpression::COLUMN_IDX, 0);
  ADD_INT_EXPR(arr, 1, 0);
  ADD_FUNC_EXPR(arr, 2, ObExpression::OP, ObExpression::GE_FUNC);
  END_EXPR(arr, 3);

  return 0;
}

int build_polish_expr_simple_le_icons(ObObj *arr)
{     
  ADD_FUNC_EXPR(arr, 0, ObExpression::COLUMN_IDX, 0);
  ADD_INT_EXPR(arr, 1, 0);
  ADD_FUNC_EXPR(arr, 2, ObExpression::OP, ObExpression::LE_FUNC);
  END_EXPR(arr, 3);

  return 0;
}






/****************** int op double ********************/


/* a + b */
int build_polish_expr_simple_add_id(ObObj *arr)
{     
  ADD_FUNC_EXPR(arr, 0, ObExpression::COLUMN_IDX, 0);
  ADD_FUNC_EXPR(arr, 1, ObExpression::COLUMN_IDX, 1);
  ADD_FUNC_EXPR(arr, 2, ObExpression::OP, ObExpression::ADD_FUNC);
  END_EXPR(arr, 3);

  return 0;
}

/* a - b */
int build_polish_expr_simple_sub_id(ObObj *arr)
{     
  ADD_FUNC_EXPR(arr, 0, ObExpression::COLUMN_IDX, 0);
  ADD_FUNC_EXPR(arr, 1, ObExpression::COLUMN_IDX, 1);
  ADD_FUNC_EXPR(arr, 2, ObExpression::OP, ObExpression::SUB_FUNC);
  END_EXPR(arr, 3);

  return 0;
}

/* a * b */
int build_polish_expr_simple_mul_id(ObObj *arr)
{     
  ADD_FUNC_EXPR(arr, 0, ObExpression::COLUMN_IDX, 0);
  ADD_FUNC_EXPR(arr, 1, ObExpression::COLUMN_IDX, 1);
  ADD_FUNC_EXPR(arr, 2, ObExpression::OP, ObExpression::MUL_FUNC);
  END_EXPR(arr, 3);

  return 0;
}

/* a / b */
int build_polish_expr_simple_div_id(ObObj *arr)
{     
  ADD_FUNC_EXPR(arr, 0, ObExpression::COLUMN_IDX, 0);
  ADD_FUNC_EXPR(arr, 1, ObExpression::COLUMN_IDX, 1);
  ADD_FUNC_EXPR(arr, 2, ObExpression::OP, ObExpression::DIV_FUNC);
  END_EXPR(arr, 3);

  return 0;
}


/* a > b */
int build_polish_expr_simple_gt_id(ObObj *arr)
{     
  ADD_FUNC_EXPR(arr, 0, ObExpression::COLUMN_IDX, 0);
  ADD_FUNC_EXPR(arr, 1, ObExpression::COLUMN_IDX, 1);
  ADD_FUNC_EXPR(arr, 2, ObExpression::OP, ObExpression::GT_FUNC);
  END_EXPR(arr, 3);

  return 0;
}



/* a >= b */
int build_polish_expr_simple_ge_id(ObObj *arr)
{     
  ADD_FUNC_EXPR(arr, 0, ObExpression::COLUMN_IDX, 0);
  ADD_FUNC_EXPR(arr, 1, ObExpression::COLUMN_IDX, 1);
  ADD_FUNC_EXPR(arr, 2, ObExpression::OP, ObExpression::GE_FUNC);
  END_EXPR(arr, 3);

  return 0;
}



/* a < b */
int build_polish_expr_simple_lt_id(ObObj *arr)
{     
  ADD_FUNC_EXPR(arr, 0, ObExpression::COLUMN_IDX, 0);
  ADD_FUNC_EXPR(arr, 1, ObExpression::COLUMN_IDX, 1);
  ADD_FUNC_EXPR(arr, 2, ObExpression::OP, ObExpression::LT_FUNC);
  END_EXPR(arr, 3);

  return 0;
}



/* a <= b */
int build_polish_expr_simple_le_id(ObObj *arr)
{     
  ADD_FUNC_EXPR(arr, 0, ObExpression::COLUMN_IDX, 0);
  ADD_FUNC_EXPR(arr, 1, ObExpression::COLUMN_IDX, 1);
  ADD_FUNC_EXPR(arr, 2, ObExpression::OP, ObExpression::LE_FUNC);
  END_EXPR(arr, 3);

  return 0;
}



/* a == b */
int build_polish_expr_simple_eq_id(ObObj *arr)
{     
  ADD_FUNC_EXPR(arr, 0, ObExpression::COLUMN_IDX, 0);
  ADD_FUNC_EXPR(arr, 1, ObExpression::COLUMN_IDX, 1);
  ADD_FUNC_EXPR(arr, 2, ObExpression::OP, ObExpression::EQ_FUNC);
  END_EXPR(arr, 3);

  return 0;
}



/* a != b */
int build_polish_expr_simple_ne_id(ObObj *arr)
{     
  ADD_FUNC_EXPR(arr, 0, ObExpression::COLUMN_IDX, 0);
  ADD_FUNC_EXPR(arr, 1, ObExpression::COLUMN_IDX, 1);
  ADD_FUNC_EXPR(arr, 2, ObExpression::OP, ObExpression::NE_FUNC);
  END_EXPR(arr, 3);

  return 0;
}


/* !a */
int build_polish_expr_simple_not_id(ObObj *arr)
{     
  ADD_FUNC_EXPR(arr, 0, ObExpression::COLUMN_IDX, 0);
  ADD_FUNC_EXPR(arr, 1, ObExpression::OP, ObExpression::NOT_FUNC);
  END_EXPR(arr, 2);

  return 0;
}


/* a && b */
int build_polish_expr_simple_and_id(ObObj *arr)
{     
  ADD_FUNC_EXPR(arr, 0, ObExpression::COLUMN_IDX, 0);
  ADD_FUNC_EXPR(arr, 1, ObExpression::COLUMN_IDX, 1);
  ADD_FUNC_EXPR(arr, 2, ObExpression::OP, ObExpression::AND_FUNC);
  END_EXPR(arr, 3);

  return 0;
}


/* a || b */
int build_polish_expr_simple_or_id(ObObj *arr)
{     
  ADD_FUNC_EXPR(arr, 0, ObExpression::COLUMN_IDX, 0);
  ADD_FUNC_EXPR(arr, 1, ObExpression::COLUMN_IDX, 1);
  ADD_FUNC_EXPR(arr, 2, ObExpression::OP, ObExpression::OR_FUNC);
  END_EXPR(arr, 3);

  return 0;
}


/**************** const op float *******************/


/* a + b */
int build_polish_expr_simple_add_cf(ObObj *arr)
{     
  ADD_FUNC_EXPR(arr, 0, ObExpression::CONST_OBJ, 10);
  ADD_FUNC_EXPR(arr, 1, ObExpression::COLUMN_IDX, 1);
  ADD_FUNC_EXPR(arr, 2, ObExpression::OP, ObExpression::ADD_FUNC);
  END_EXPR(arr, 3);

  return 0;
}

/* a - b */
int build_polish_expr_simple_sub_cf(ObObj *arr)
{     
  ADD_FUNC_EXPR(arr, 0, ObExpression::CONST_OBJ, 10);
  ADD_FUNC_EXPR(arr, 1, ObExpression::COLUMN_IDX, 1);
  ADD_FUNC_EXPR(arr, 2, ObExpression::OP, ObExpression::SUB_FUNC);
  END_EXPR(arr, 3);

  return 0;
}

/* a * b */
int build_polish_expr_simple_mul_cf(ObObj *arr)
{     
  ADD_FUNC_EXPR(arr, 0, ObExpression::CONST_OBJ, 10);
  ADD_FUNC_EXPR(arr, 1, ObExpression::COLUMN_IDX, 1);
  ADD_FUNC_EXPR(arr, 2, ObExpression::OP, ObExpression::MUL_FUNC);
  END_EXPR(arr, 3);

  return 0;
}

/* a / b */
int build_polish_expr_simple_div_cf(ObObj *arr)
{     
  ADD_FUNC_EXPR(arr, 0, ObExpression::CONST_OBJ, 10);
  ADD_FUNC_EXPR(arr, 1, ObExpression::COLUMN_IDX, 1);
  ADD_FUNC_EXPR(arr, 2, ObExpression::OP, ObExpression::DIV_FUNC);
  END_EXPR(arr, 3);

  return 0;
}


/* a > b */
int build_polish_expr_simple_gt_cf(ObObj *arr)
{     
  ADD_FUNC_EXPR(arr, 0, ObExpression::CONST_OBJ, 10);
  ADD_FUNC_EXPR(arr, 1, ObExpression::COLUMN_IDX, 1);
  ADD_FUNC_EXPR(arr, 2, ObExpression::OP, ObExpression::GT_FUNC);
  END_EXPR(arr, 3);

  return 0;
}



/* a >= b */
int build_polish_expr_simple_ge_cf(ObObj *arr)
{     
  ADD_FUNC_EXPR(arr, 0, ObExpression::CONST_OBJ, 10);
  ADD_FUNC_EXPR(arr, 1, ObExpression::COLUMN_IDX, 1);
  ADD_FUNC_EXPR(arr, 2, ObExpression::OP, ObExpression::GE_FUNC);
  END_EXPR(arr, 3);

  return 0;
}



/* a < b */
int build_polish_expr_simple_lt_cf(ObObj *arr)
{     
  ADD_FUNC_EXPR(arr, 0, ObExpression::CONST_OBJ, 10);
  ADD_FUNC_EXPR(arr, 1, ObExpression::COLUMN_IDX, 1);
  ADD_FUNC_EXPR(arr, 2, ObExpression::OP, ObExpression::LT_FUNC);
  END_EXPR(arr, 3);

  return 0;
}



/* a <= b */
int build_polish_expr_simple_le_cf(ObObj *arr)
{     
  ADD_FUNC_EXPR(arr, 0, ObExpression::CONST_OBJ, 10);
  ADD_FUNC_EXPR(arr, 1, ObExpression::COLUMN_IDX, 1);
  ADD_FUNC_EXPR(arr, 2, ObExpression::OP, ObExpression::LE_FUNC);
  END_EXPR(arr, 3);

  return 0;
}


/* a_str LIKE b_const_str */
int build_polish_expr_simple_like_str(ObObj *arr)
{
  char *p = (char*)"col";  /* match test_column */
  ObString pattern_str;
  pattern_str.assign(p, static_cast<int32_t>(strlen(p)));

  ADD_FUNC_EXPR(arr, 0, ObExpression::COLUMN_IDX, 0);
  ADD_VARCHAR_EXPR(arr, 1, pattern_str);
  ADD_FUNC_EXPR(arr, 2, ObExpression::OP, ObExpression::LIKE_FUNC);

  END_EXPR(arr, 3);

  return 0;
}   

/* null LIKE b_const_str */
int build_polish_expr_null_like_str(ObObj *arr)
{
  char *p = (char*)"col";  /* match test_column */
  ObString pattern_str;
  pattern_str.assign(p, static_cast<int32_t>(strlen(p)));

  ADD_NULL_EXPR(arr, 0);
  ADD_VARCHAR_EXPR(arr, 1, pattern_str);
  ADD_FUNC_EXPR(arr, 2, ObExpression::OP, ObExpression::LIKE_FUNC);

  END_EXPR(arr, 3);

  return 0;
}   



/* a_str LIKE empty string */
int build_polish_expr_simple_like_str_2(ObObj *arr)
{
  char *p = (char*)"";  /* match test_column */
  ObString pattern_str;
  pattern_str.assign(p, static_cast<int32_t>(strlen(p)));

  ADD_FUNC_EXPR(arr, 0, ObExpression::COLUMN_IDX, 0);
  ADD_VARCHAR_EXPR(arr, 1, pattern_str);
  ADD_FUNC_EXPR(arr, 2, ObExpression::OP, ObExpression::LIKE_FUNC);

  END_EXPR(arr, 3);

  return 0;
}   


/* a == b */
int build_polish_expr_simple_eq_cf(ObObj *arr)
{     
  ADD_FUNC_EXPR(arr, 0, ObExpression::CONST_OBJ, 10);
  ADD_FUNC_EXPR(arr, 1, ObExpression::COLUMN_IDX, 1);
  ADD_FUNC_EXPR(arr, 2, ObExpression::OP, ObExpression::EQ_FUNC);
  END_EXPR(arr, 3);

  return 0;
}



/* a != b */
int build_polish_expr_simple_ne_cf(ObObj *arr)
{     
  ADD_FUNC_EXPR(arr, 0, ObExpression::CONST_OBJ, 10);
  ADD_FUNC_EXPR(arr, 1, ObExpression::COLUMN_IDX, 1);
  ADD_FUNC_EXPR(arr, 2, ObExpression::OP, ObExpression::NE_FUNC);
  END_EXPR(arr, 3);

  return 0;
}



/* !a */
int build_polish_expr_simple_not_cf(ObObj *arr)
{     
  ADD_FUNC_EXPR(arr, 0, ObExpression::CONST_OBJ, 10);
  ADD_FUNC_EXPR(arr, 1, ObExpression::OP, ObExpression::NOT_FUNC);
  END_EXPR(arr, 2);

  return 0;
}



/* a && b */
int build_polish_expr_simple_and_cf(ObObj *arr)
{     
  ADD_FUNC_EXPR(arr, 0, ObExpression::CONST_OBJ, 10);
  ADD_FUNC_EXPR(arr, 1, ObExpression::COLUMN_IDX, 1);
  ADD_FUNC_EXPR(arr, 2, ObExpression::OP, ObExpression::AND_FUNC);
  END_EXPR(arr, 3);

  return 0;
}


/* a || b */
int build_polish_expr_simple_or_cf(ObObj *arr)
{     
  ADD_FUNC_EXPR(arr, 0, ObExpression::CONST_OBJ, 10);
  ADD_FUNC_EXPR(arr, 1, ObExpression::COLUMN_IDX, 1);
  ADD_FUNC_EXPR(arr, 2, ObExpression::OP, ObExpression::OR_FUNC);
  END_EXPR(arr, 3);

  return 0;
}


/* int int pair */
int build_cellinfo_ii(ObCellArray &org_cell_array)
{
  ObCellInfo cell;
  ObInnerCellInfo *cell_out = NULL;
  int64_t i;

  for(i = 0; i < 1000; i+=2)
  {
    cell.value_.set_int(i);
    EXPECT_EQ(org_cell_array.append(cell, cell_out),OB_SUCCESS);
    cell.value_.set_int(i+1);
    EXPECT_EQ(org_cell_array.append(cell, cell_out),OB_SUCCESS);
  }
  return 0;
}


/* double double pair */
int build_cellinfo_dd(ObCellArray &org_cell_array)
{
  ObCellInfo cell;
  ObInnerCellInfo *cell_out = NULL;
  int64_t i;

  for(i = 0; i < 1000; i+=2)
  {
    cell.value_.set_double((double)i);
    EXPECT_EQ(org_cell_array.append(cell, cell_out),OB_SUCCESS);
    cell.value_.set_double((double)(i+1));
    EXPECT_EQ(org_cell_array.append(cell, cell_out),OB_SUCCESS);
  }
  return 0;
}



/* int double pair */
int build_cellinfo_id(ObCellArray &org_cell_array)
{
  ObCellInfo cell;
  ObInnerCellInfo *cell_out = NULL;
  int64_t i;

  for(i = 0; i < 1000; i+=2)
  {
    cell.value_.set_int(i);
    EXPECT_EQ(org_cell_array.append(cell, cell_out),OB_SUCCESS);
    cell.value_.set_double((double)(i+1));
    EXPECT_EQ(org_cell_array.append(cell, cell_out),OB_SUCCESS);
  }
  return 0;
}




/* const float pair */
int build_cellinfo_cf(ObCellArray &org_cell_array)
{
  ObCellInfo cell;
  ObInnerCellInfo *cell_out = NULL;
  int64_t i;

  for(i = 0; i < 1000; i+=1)
  {
    cell.value_.set_float((float)i);
    EXPECT_EQ(org_cell_array.append(cell, cell_out),OB_SUCCESS);
  }
  return 0;
}




/* str str pair */
int build_cellinfo_str(ObCellArray &org_cell_array)
{
  char *ptr1 = (char*)"test_column_str";
  char *ptr2 = (char*)"test_row_str";
  ObCellInfo cell;
  ObInnerCellInfo *cell_out = NULL;
  int64_t i;
  ObString str1;
  ObString str2;
  str1.assign(ptr1,static_cast<int32_t>(strlen(ptr1)));
  str2.assign(ptr2,static_cast<int32_t>(strlen(ptr2)));

  for(i = 0; i < 1000; i+=2)
  {
    cell.value_.set_varchar(str1);
    EXPECT_EQ(org_cell_array.append(cell, cell_out),OB_SUCCESS);
    cell.value_.set_varchar(str2);
    EXPECT_EQ(org_cell_array.append(cell, cell_out),OB_SUCCESS);
  }
  return 0;
}


/* null */
int build_cellinfo_null(ObCellArray &org_cell_array)
{
  ObCellInfo cell;
  ObInnerCellInfo *cell_out = NULL;
  int64_t i;

  for(i = 0; i < 1000; i+=1)
  {
    cell.value_.set_null();
    EXPECT_EQ(org_cell_array.append(cell, cell_out),OB_SUCCESS);
  }
  return 0;
}


/* int null pair */
int build_cellinfo_in(ObCellArray &org_cell_array)
{
  ObCellInfo cell;
  ObInnerCellInfo *cell_out = NULL;
  int64_t i;

  for(i = 0; i < 1000; i+=1)
  {
    cell.value_.set_int(i);
    EXPECT_EQ(org_cell_array.append(cell, cell_out),OB_SUCCESS);
    cell.value_.set_null();
    EXPECT_EQ(org_cell_array.append(cell, cell_out),OB_SUCCESS);
  }
  return 0;
}



/* test above ii setting */
TEST(Test_ob_composite_column, ii_test)
{
  ObObj arr[20];
  ObStringBuf data_buf;
	ObObj result;
  ObCellArray ca;
  int64_t beg = 2, end = 3;
  bool b = false;
  int64_t i = 0;
  float f = 0;
  double d = 0.0;

  UNUSED(beg);
  UNUSED(end);
  UNUSED(f);

  ObCompositeColumn column;

  build_cellinfo_ii(ca);
  
  build_polish_expr_simple_add_ii(arr);
  column.set_expression(arr,data_buf);

  column.calc_composite_val(result, ca, 2, 3);  /* 2 + 3 */
  ASSERT_EQ(OB_SUCCESS, result.get_int(i));
  ASSERT_EQ(i, 5);


  build_polish_expr_simple_sub_ii(arr);
  column.set_expression(arr,data_buf);
  column.calc_composite_val(result, ca, 2, 3);  /* 2 - 3 */
  ASSERT_EQ(OB_SUCCESS, result.get_int(i));
  ASSERT_EQ(i, -1);


  
  build_polish_expr_simple_mul_ii(arr);
  column.set_expression(arr,data_buf);
  column.calc_composite_val(result, ca, 2, 3);  /* 2 * 3 */
  ASSERT_EQ(OB_SUCCESS, result.get_int(i));
  ASSERT_EQ(i, 6);


  
  build_polish_expr_simple_div_ii(arr);
  column.set_expression(arr,data_buf);
  column.calc_composite_val(result, ca, 2, 3);  /* 2 / 3 */
  ASSERT_EQ(OB_SUCCESS, result.get_double(d));
  ASSERT_EQ(d, 1.0 * (double)2 / 3);

  
  build_polish_expr_simple_lt_ii(arr);
  column.set_expression(arr,data_buf);
  column.calc_composite_val(result, ca, 2, 3);  /* 2 < 3 */
  result.get_bool(b);
  ASSERT_EQ(b, true);

  build_polish_expr_simple_gt_ii(arr);
  column.set_expression(arr,data_buf);
  column.calc_composite_val(result, ca, 2, 3);  /* 2 > 3 */
  result.get_bool(b);
  ASSERT_EQ(b, false);

  build_polish_expr_simple_ge_ii(arr);
  column.set_expression(arr,data_buf);  
  column.calc_composite_val(result, ca, 2, 3);  /* 2 >= 3 */
  result.get_bool(b);
  ASSERT_EQ(b, false);

  build_polish_expr_simple_le_ii(arr);
  column.set_expression(arr,data_buf);
  column.calc_composite_val(result, ca, 2, 3);  /* 2 <= 3 */
  result.get_bool(b);
  ASSERT_EQ(b, true);


  build_polish_expr_simple_eq_ii(arr);
  column.set_expression(arr,data_buf);
  column.calc_composite_val(result, ca, 2, 3);  /* 2 == 3 */
  result.get_bool(b);
  ASSERT_EQ(b, false);


  build_polish_expr_simple_ne_ii(arr);
  column.set_expression(arr,data_buf);
  column.calc_composite_val(result, ca, 2, 3);  /* 2 != 3 */
  result.get_bool(b);
  ASSERT_EQ(b, true);

  build_polish_expr_simple_is_ic(arr);
  column.set_expression(arr,data_buf);  
  column.calc_composite_val(result, ca, 0, 1);  /* 0 is NULL */
  result.get_bool(b);
  ASSERT_EQ(b, false);


  build_polish_expr_simple_is_not_ic(arr);
  column.set_expression(arr,data_buf);  
  column.calc_composite_val(result, ca, 0, 1);  /* 0 is not NULL */
  result.get_bool(b);
  ASSERT_EQ(b, true);



  build_polish_expr_simple_not_ii(arr);
  column.set_expression(arr,data_buf);
  column.calc_composite_val(result, ca, 2, 3);  /* !2 */
  result.get_bool(b);
  ASSERT_EQ(b, false);
  column.calc_composite_val(result, ca, 0, 1);  /* !0 (not bool) */
  result.get_bool(b);
  ASSERT_EQ(b, true);
  


  build_polish_expr_simple_and_ii(arr);
  column.set_expression(arr,data_buf);
  column.calc_composite_val(result, ca, 2, 3);  /* 2 && 3 (not bool)*/
  result.get_bool(b);
  ASSERT_EQ(b, true);



  build_polish_expr_simple_or_ii(arr);
  column.set_expression(arr,data_buf);  
  column.calc_composite_val(result, ca, 2, 3);  /* 2 || 3 */
  result.get_bool(b);
  ASSERT_EQ(b, true);
} 

/* test int null setting */
TEST(Test_ob_composite_column, in_test)
{
  ObObj arr[20];
  ObStringBuf data_buf;
	ObObj result;
  ObCellArray ca;
  int64_t beg = 2, end = 3;
  bool b = false;
  double d = 0;
  float f = 0;

  UNUSED(beg);
  UNUSED(end);
  UNUSED(d);
  UNUSED(f);

  ObCompositeColumn column;

  build_cellinfo_in(ca);  // int null
  
  build_polish_expr_simple_is_ii(arr);
  column.set_expression(arr,data_buf);  
  column.calc_composite_val(result, ca, 0, 1);  /* 0 is 1 */
  result.get_bool(b);
  ASSERT_EQ(b, false);


  build_polish_expr_simple_is_not_ii(arr);
  column.set_expression(arr,data_buf);  
  column.calc_composite_val(result, ca, 0, 1);  /* 0 is not 1 */
  result.get_bool(b);
  ASSERT_EQ(b, true);


  build_polish_expr_simple_is_ic(arr);
  column.set_expression(arr,data_buf);
  column.calc_composite_val(result, ca, 0, 1);  /* 0 is NULL */
  result.get_bool(b);
  ASSERT_EQ(b, false);


  build_polish_expr_simple_is_not_ic(arr);
  column.set_expression(arr,data_buf);  
  column.calc_composite_val(result, ca, 0, 1);  /* 0 is not NULL */
  result.get_bool(b);
  ASSERT_EQ(b, true);

}


 
/* test null setting */
TEST(Test_ob_composite_column, null_test)
{
  ObObj arr[20];
  ObStringBuf data_buf;
	ObObj result;
  ObCellArray ca;
  int64_t beg = 2, end = 3;
  bool b = false;
  double d = 0;
  float f = 0;


  UNUSED(beg);
  UNUSED(end);
  UNUSED(d);
  UNUSED(f);


  ObCompositeColumn column;

  build_cellinfo_null(ca);  // int null
  
  build_polish_expr_simple_is_ii(arr);
  column.set_expression(arr,data_buf);  
  column.calc_composite_val(result, ca, 0, 1);  /* 0 is 1 */
  result.get_bool(b);
  ASSERT_EQ(b, true);


  build_polish_expr_simple_is_not_ii(arr);
  column.set_expression(arr,data_buf);
  column.calc_composite_val(result, ca, 0, 1);  /* 0 is not 1 */
  result.get_bool(b);
  ASSERT_EQ(b, false);


  build_polish_expr_simple_is_ic(arr);
  column.set_expression(arr,data_buf);
  column.calc_composite_val(result, ca, 0, 1);  /* 0 is NULL */
  result.get_bool(b);
  ASSERT_EQ(b, true);


  build_polish_expr_simple_is_not_ic(arr);
  column.set_expression(arr,data_buf);
  column.calc_composite_val(result, ca, 0, 1);  /* 0 is not NULL */

  build_polish_expr_simple_eq_ii(arr);
  column.set_expression(arr,data_buf);
  column.calc_composite_val(result, ca, 0, 2);  /* NULL == NULL  */
  ASSERT_TRUE(result.is_null());

  build_polish_expr_simple_le_ii(arr);
  column.set_expression(arr,data_buf);
  column.calc_composite_val(result, ca, 0, 2);  /* NULL <= NULL  */
  ASSERT_TRUE(result.is_null());

  build_polish_expr_simple_ge_ii(arr);
  column.set_expression(arr,data_buf);
  column.calc_composite_val(result, ca, 0, 2);  /* NULL >= NULL  */
  ASSERT_TRUE(result.is_null());

  build_polish_expr_simple_ne_ii(arr);
  column.set_expression(arr,data_buf);
  column.calc_composite_val(result, ca, 0, 2);  /* NULL != NULL  */
  ASSERT_TRUE(result.is_null());

  build_polish_expr_simple_lt_ii(arr);
  column.set_expression(arr,data_buf);
  column.calc_composite_val(result, ca, 0, 2);  /* NULL < NULL  */
  ASSERT_TRUE(result.is_null());

  build_polish_expr_simple_gt_ii(arr);
  column.set_expression(arr,data_buf);
  column.calc_composite_val(result, ca, 0, 2);  /* NULL > NULL  */
  ASSERT_TRUE(result.is_null());



}

/* test null setting i_c */
TEST(Test_ob_composite_column, null_ic_test)
{
  ObObj arr[20];
  ObStringBuf data_buf;
	ObObj result;
  ObCellArray ca;
  int64_t beg = 2, end = 3;
  bool b = false;
  double d = 0;
  float f = 0;


  UNUSED(beg);
  UNUSED(end);
  UNUSED(d);
  UNUSED(f);


  ObCompositeColumn column;

  build_cellinfo_null(ca);  // null
  
  build_polish_expr_simple_is_ic(arr);
  column.set_expression(arr,data_buf);
  column.calc_composite_val(result, ca, 0, 1);  /* null is NULL */
  result.get_bool(b);
  ASSERT_EQ(b, true);


  build_polish_expr_simple_is_not_ic(arr);
  column.set_expression(arr,data_buf);
  column.calc_composite_val(result, ca, 0, 1);  /* null is not NULL */
  result.get_bool(b);
  ASSERT_EQ(b, false);


  build_polish_expr_simple_eq_ic(arr);
  column.set_expression(arr,data_buf);
  column.calc_composite_val(result, ca, 0, 1);  /* NULL == NULL  */
  ASSERT_TRUE(result.is_null());

  build_polish_expr_simple_le_ic(arr);
  column.set_expression(arr,data_buf);
  column.calc_composite_val(result, ca, 0, 1);  /* NULL <= NULL  */
  ASSERT_TRUE(result.is_null());

  build_polish_expr_simple_ge_ic(arr);
  column.set_expression(arr,data_buf);
  column.calc_composite_val(result, ca, 0, 1);  /* NULL >= NULL  */
  ASSERT_TRUE(result.is_null());

  build_polish_expr_simple_ne_ic(arr);
  column.set_expression(arr,data_buf);
  column.calc_composite_val(result, ca, 0, 1);  /* NULL != NULL  */
  ASSERT_TRUE(result.is_null());

  build_polish_expr_simple_lt_ic(arr);
  column.set_expression(arr,data_buf);
  column.calc_composite_val(result, ca, 0, 1);  /* NULL < NULL  */
  ASSERT_TRUE(result.is_null());

  build_polish_expr_simple_gt_ic(arr);
  column.set_expression(arr,data_buf);
  column.calc_composite_val(result, ca, 0, 1);  /* NULL > NULL  */
  ASSERT_TRUE(result.is_null());

  
  

  
  /* const value 10  op null */

 
  build_polish_expr_simple_eq_icons(arr);
  column.set_expression(arr,data_buf);
  column.calc_composite_val(result, ca, 0, 2);  /* 0 == NULL  */
  ASSERT_TRUE(result.is_null());

  build_polish_expr_simple_le_icons(arr);
  column.set_expression(arr,data_buf);
  column.calc_composite_val(result, ca, 0, 2);  /* 0 >= NULL  */
  ASSERT_TRUE(result.is_null());

  build_polish_expr_simple_ge_icons(arr);
  column.set_expression(arr,data_buf);
  column.calc_composite_val(result, ca, 0, 2);  /* 0 <= NULL  */
  ASSERT_TRUE(result.is_null());

  build_polish_expr_simple_ne_icons(arr);
  column.set_expression(arr,data_buf);
  column.calc_composite_val(result, ca, 0, 2);  /* 0 != NULL  */
  ASSERT_TRUE(result.is_null());

  build_polish_expr_simple_lt_icons(arr);
  column.set_expression(arr,data_buf);
  column.calc_composite_val(result, ca, 0, 2);  /* 0 > NULL  */
  ASSERT_TRUE(result.is_null());

  build_polish_expr_simple_gt_icons(arr);
  column.set_expression(arr,data_buf);
  column.calc_composite_val(result, ca, 0, 2);  /* 0 < NULL  */
  ASSERT_TRUE(result.is_null());
}




/* test above int op double setting */
TEST(Test_ob_composite_column, id_test)
{
  ObObj arr[20];
  ObStringBuf data_buf;
	ObObj result;
  ObCellArray ca;
  int64_t beg = 2, end = 4;
  bool b = false;
  double d = 0;
  float f = 0;


  UNUSED(beg);
  UNUSED(end);
  UNUSED(f);


  ObCompositeColumn column;

  build_cellinfo_id(ca);


  build_polish_expr_simple_add_id(arr);
  column.set_expression(arr,data_buf);  
  column.calc_composite_val(result, ca, 2, 4);  /* 2 + 3.0 */
  ASSERT_EQ(OB_SUCCESS, result.get_double(d));
  ASSERT_EQ(d, 5);



  build_polish_expr_simple_sub_id(arr);
  column.set_expression(arr,data_buf);  
  column.calc_composite_val(result, ca, 2, 4);  /* 2 - 3 */
  ASSERT_EQ(OB_SUCCESS, result.get_double(d));
  ASSERT_EQ(d, -1);



  build_polish_expr_simple_mul_id(arr);
  column.set_expression(arr,data_buf);
  column.calc_composite_val(result, ca, 2, 4);  /* 2 * 3 */
  ASSERT_EQ(OB_SUCCESS, result.get_double(d));
  ASSERT_EQ(d, 6);



  build_polish_expr_simple_div_id(arr);
  column.set_expression(arr,data_buf);
  column.calc_composite_val(result, ca, 2, 4);  /* 2 / 3 */
  ASSERT_EQ(OB_SUCCESS, result.get_double(d));
  ASSERT_EQ(d, (double)2.0/3);


  build_polish_expr_simple_lt_id(arr);
  column.set_expression(arr,data_buf);
  column.calc_composite_val(result, ca, 2, 4);  /* 2 < 3 */
  result.get_bool(b);
  ASSERT_EQ(b, true);


  build_polish_expr_simple_gt_id(arr);
  column.set_expression(arr,data_buf);
  column.calc_composite_val(result, ca, 2, 4);  /* 2 > 3 */
  result.get_bool(b);
  ASSERT_EQ(b, false);

  build_polish_expr_simple_ge_id(arr);
  column.set_expression(arr,data_buf);
  column.calc_composite_val(result, ca, 2, 4);  /* 2 >= 3 */
  result.get_bool(b);
  ASSERT_EQ(b, false);

  build_polish_expr_simple_le_id(arr);
  column.set_expression(arr,data_buf);
  column.calc_composite_val(result, ca, 2, 4);  /* 2 <= 3 */
  result.get_bool(b);
  ASSERT_EQ(b, true);

  build_polish_expr_simple_eq_id(arr);
  column.set_expression(arr,data_buf);
  column.calc_composite_val(result, ca, 2, 4);  /* 2 == 3 */
  result.get_bool(b);
  ASSERT_EQ(b, false);

  build_polish_expr_simple_ne_id(arr);
  column.set_expression(arr,data_buf);
  column.calc_composite_val(result, ca, 2, 4);  /* 2 != 3 */
  result.get_bool(b);
  ASSERT_EQ(b, true);

  build_polish_expr_simple_not_id(arr);
  column.set_expression(arr,data_buf);
  column.calc_composite_val(result, ca, 2, 4);  /* !2 */
  result.get_bool(b);
  ASSERT_EQ(b, false);

  column.calc_composite_val(result, ca, 0, 1);  /* !0 (not bool) */
  result.get_bool(b);
  ASSERT_EQ(b, true);

  column.calc_composite_val(result, ca, 1, 2);  /* !1.0 */
  result.get_bool(b);
  ASSERT_EQ(b, false);


  build_polish_expr_simple_and_id(arr);
  column.set_expression(arr,data_buf);
  column.calc_composite_val(result, ca, 2, 4);  /* 2 && 3.0 */
  result.get_bool(b);
  ASSERT_EQ(b, true);



  build_polish_expr_simple_or_id(arr);
  column.set_expression(arr,data_buf);
  column.calc_composite_val(result, ca, 2, 4);  /* 2 || 3.0 */
  result.get_bool(b);
  ASSERT_EQ(b, true);
} 

/* test above const op float setting */
TEST(Test_ob_composite_column, cf_test)
{
  ObObj arr[20];
  ObStringBuf data_buf;
	ObObj result;
  ObCellArray ca;
  int64_t beg = 2, end = 3;
  bool b = false;  
  double d = 0;
  float f = 0;

  UNUSED(beg);
  UNUSED(end);
  UNUSED(f);

  ObCompositeColumn column;

  build_cellinfo_id(ca);


  build_polish_expr_simple_add_cf(arr);
  column.set_expression(arr,data_buf);
  column.calc_composite_val(result, ca, 2, 3);  /* 10c + 3.0 */
  ASSERT_EQ(OB_SUCCESS, result.get_double(d));
  ASSERT_EQ(d, 13);


  build_polish_expr_simple_sub_cf(arr);
  column.set_expression(arr,data_buf);
  column.calc_composite_val(result, ca, 2, 3);  /* 10 - 3.0 */
  ASSERT_EQ(OB_SUCCESS, result.get_double(d));
  ASSERT_EQ(d, 7);



  build_polish_expr_simple_mul_cf(arr);
  column.set_expression(arr,data_buf);
  column.calc_composite_val(result, ca, 2, 3);  /* 10 * 3.0 */
  ASSERT_EQ(OB_SUCCESS, result.get_double(d));
  ASSERT_EQ(d, 30);



  build_polish_expr_simple_div_cf(arr);
  column.set_expression(arr,data_buf);
  column.calc_composite_val(result, ca, 2, 3);  /* 10 / 3.0 */
  ASSERT_EQ(OB_SUCCESS, result.get_double(d));
  ASSERT_EQ(d, 10/3.0);


  build_polish_expr_simple_lt_cf(arr);
  column.set_expression(arr,data_buf);
  column.calc_composite_val(result, ca, 2, 3);  /* 10 < 3 */
  result.get_bool(b);
  ASSERT_EQ(b, false);

  build_polish_expr_simple_gt_cf(arr);
  column.set_expression(arr,data_buf);
  column.calc_composite_val(result, ca, 2, 3);  /* 10 > 3 */
  result.get_bool(b);
  ASSERT_EQ(b, true);

  build_polish_expr_simple_ge_cf(arr);
  column.set_expression(arr,data_buf);
  column.calc_composite_val(result, ca, 2, 3);  /* 10 >= 3 */
  result.get_bool(b);
  ASSERT_EQ(b, true);

  build_polish_expr_simple_le_cf(arr);
  column.set_expression(arr,data_buf);
  column.calc_composite_val(result, ca, 2, 3);  /* 10 <= 3 */
  result.get_bool(b);
  ASSERT_EQ(b, false);


  build_polish_expr_simple_eq_cf(arr);
  column.set_expression(arr,data_buf);
  column.calc_composite_val(result, ca, 2, 3);  /* 10 == 3 */

  result.get_bool(b);
  ASSERT_EQ(b, false);



  build_polish_expr_simple_ne_cf(arr);
  column.set_expression(arr,data_buf);
  column.calc_composite_val(result, ca, 2, 3);  /* 10 != 3.0 */
  result.get_bool(b);
  ASSERT_EQ(b, true);

  build_polish_expr_simple_not_cf(arr);
  column.set_expression(arr,data_buf);
  column.calc_composite_val(result, ca, 2, 3);  /* !10 */
  result.get_bool(b);
  ASSERT_EQ(b, false);



  build_polish_expr_simple_and_cf(arr);
  column.set_expression(arr,data_buf);
  column.calc_composite_val(result, ca, 2, 3);  /* 10 && 3.0 */
  result.get_bool(b);
  ASSERT_EQ(b, true);


  build_polish_expr_simple_or_cf(arr);
  column.set_expression(arr,data_buf);
  column.calc_composite_val(result, ca, 2, 3);  /* 10 || 3.0 */
  result.get_bool(b);
  ASSERT_EQ(b, true);


}


/* a+b+c */
int build_polish_expr_comp_add_mix_iii(ObObj *arr)
{     
  ADD_FUNC_EXPR(arr, 0, ObExpression::COLUMN_IDX, 0);
  ADD_FUNC_EXPR(arr, 1, ObExpression::COLUMN_IDX, 1);
  ADD_FUNC_EXPR(arr, 2, ObExpression::OP, ObExpression::ADD_FUNC);
  ADD_FUNC_EXPR(arr, 3, ObExpression::COLUMN_IDX, 2);
  ADD_FUNC_EXPR(arr, 4, ObExpression::OP, ObExpression::ADD_FUNC);
  END_EXPR(arr, 5);

  return 0;
}


/* a*b/c */
int build_polish_expr_comp_arith_mix_iii(ObObj *arr)
{     
  ADD_FUNC_EXPR(arr, 0, ObExpression::COLUMN_IDX, 0);
  ADD_FUNC_EXPR(arr, 1, ObExpression::COLUMN_IDX, 1);
  ADD_FUNC_EXPR(arr, 2, ObExpression::OP, ObExpression::MUL_FUNC);
  ADD_FUNC_EXPR(arr, 3, ObExpression::COLUMN_IDX, 2);
  ADD_FUNC_EXPR(arr, 4, ObExpression::OP, ObExpression::DIV_FUNC);
  END_EXPR(arr, 5);

  return 0;
}


/* a&&b||c */
int build_polish_expr_comp_logic_mix_iii(ObObj *arr)
{     
  ADD_FUNC_EXPR(arr, 0, ObExpression::COLUMN_IDX, 0);
  ADD_FUNC_EXPR(arr, 1, ObExpression::COLUMN_IDX, 1);
  ADD_FUNC_EXPR(arr, 2, ObExpression::OP, ObExpression::AND_FUNC);
  ADD_FUNC_EXPR(arr, 3, ObExpression::COLUMN_IDX, 2);
  ADD_FUNC_EXPR(arr, 4, ObExpression::OP, ObExpression::OR_FUNC);
  END_EXPR(arr, 5);

  return 0;
}



/* (a+b) <= (c+d) */
int build_polish_expr_comp_compare_mix_iii(ObObj *arr)
{     
  ADD_FUNC_EXPR(arr, 0, ObExpression::COLUMN_IDX, 0);
  ADD_FUNC_EXPR(arr, 1, ObExpression::COLUMN_IDX, 1);
  ADD_FUNC_EXPR(arr, 2, ObExpression::OP, ObExpression::ADD_FUNC);
  ADD_FUNC_EXPR(arr, 3, ObExpression::COLUMN_IDX, 2);
  ADD_FUNC_EXPR(arr, 4, ObExpression::COLUMN_IDX, 3);
  ADD_FUNC_EXPR(arr, 5, ObExpression::OP, ObExpression::ADD_FUNC);
  ADD_FUNC_EXPR(arr, 6, ObExpression::OP, ObExpression::LE_FUNC);
  END_EXPR(arr, 7);

  return 0;
}



/* test above const op float setting */
TEST(Test_ob_composite_column, mix_iii_test)
{
  ObObj arr[20];
  ObStringBuf data_buf;
	ObObj result;
  ObCellArray ca;
  int64_t beg = 2, end = 4;
  bool b = false;
  int64_t i = 0;
  double d = 0;
  float f = 0;

  UNUSED(beg);
  UNUSED(end);
  UNUSED(d);
  UNUSED(f);

  ObCompositeColumn column;

  build_cellinfo_ii(ca);


  build_polish_expr_comp_add_mix_iii(arr);
  column.set_expression(arr,data_buf);
  column.calc_composite_val(result, ca, 0, 3);  /* a+b+c , 0+1+2*/
  ASSERT_EQ(OB_SUCCESS, result.get_int(i));
  ASSERT_EQ(i, 3);

  build_polish_expr_comp_arith_mix_iii(arr);
  column.set_expression(arr,data_buf);
  column.calc_composite_val(result, ca, 0, 3);  /* a*b/c , 0*1/2*/
  ASSERT_EQ(OB_SUCCESS, result.get_double(d));
  ASSERT_EQ(d, (double)0.0);
  column.calc_composite_val(result, ca, 3, 6);  /* a*b/c , 3*4/5*/
  ASSERT_EQ(OB_SUCCESS, result.get_double(d));
  ASSERT_EQ(d, (double)3.0 * 4 / 5);

  build_polish_expr_comp_logic_mix_iii(arr);
  column.set_expression(arr,data_buf);
  column.calc_composite_val(result, ca, 0, 3);
  result.get_bool(b);
  ASSERT_EQ(b, true);


  build_polish_expr_comp_compare_mix_iii(arr);
  column.set_expression(arr,data_buf);
  column.calc_composite_val(result, ca, 0, 4);  /* (a+b) <= (c+d)  */
  result.get_bool(b);
  ASSERT_EQ(b, true);

}




/* test above const op float setting */
TEST(Test_ob_composite_column, mix_ddd_test)
{
  ObObj arr[20];
  ObStringBuf data_buf;
	ObObj result;
  ObCellArray ca;
  int64_t beg = 2, end = 4;
  bool b = false;
  double d = 0;
  float f = 0;

  const int64_t data_len = 1024*64;
  char buf[data_len];
  int64_t pos = 0;

  UNUSED(beg);
  UNUSED(end);
  UNUSED(f);

  ObCompositeColumn column;

  build_cellinfo_dd(ca);
  
  
  build_polish_expr_comp_add_mix_iii(arr);
  column.set_expression(arr,data_buf);
  column.calc_composite_val(result, ca, 0, 3);  /* a+b+c , 0+1+2*/
  ASSERT_EQ(OB_SUCCESS, result.get_double(d));
  ASSERT_EQ(d, 3);

  build_polish_expr_comp_arith_mix_iii(arr);
  column.set_expression(arr,data_buf);
  column.calc_composite_val(result, ca, 0, 3);  /* a*b/c , 0*1/2*/
  ASSERT_EQ(OB_SUCCESS, result.get_double(d));
  ASSERT_EQ(d, 0);
 // ====>>>
  pos = 0;
  column.serialize(buf, data_len, pos);
  printf("after serialize, pos = %ld\n", pos);
  pos = 0;
  ObCompositeColumn my_column;
  my_column.deserialize(buf, data_len,pos);
  printf("after deserialize, pos = %ld\n", pos);
  my_column.calc_composite_val(result, ca, 3, 6);  /* a*b/c , 3*4/5*/
  ASSERT_EQ(OB_SUCCESS, result.get_double(d));
  ASSERT_EQ(d, (double)3.0*4/5);
// <<<====
  column.calc_composite_val(result, ca, 3, 6);  /* a*b/c , 3*4/5*/
  ASSERT_EQ(OB_SUCCESS, result.get_double(d));
  ASSERT_EQ(d, (double)3.0*4/5);

  build_polish_expr_comp_logic_mix_iii(arr);
  column.set_expression(arr,data_buf);
  column.calc_composite_val(result, ca, 0, 3);  /* a&&b||c */
  result.get_bool(b);
  ASSERT_EQ(b, true);

 
  build_polish_expr_comp_compare_mix_iii(arr);
  column.set_expression(arr,data_buf);  
// ====>>>
  pos = 0;
  column.serialize(buf, data_len, pos);
  printf("after serialize, pos = %ld\n", pos);
  pos = 0;
  ObCompositeColumn my_column2;
  my_column2.deserialize(buf, data_len,pos);
  printf("after deserialize, pos = %ld\n", pos);
  my_column2.calc_composite_val(result, ca, 0, 4);  /* (a+b) <= (c+d)  */
  result.get_bool(b);
  ASSERT_EQ(b, true);

// <<<====
  column.calc_composite_val(result, ca, 0, 4);  /* (a+b) <= (c+d)  */
  result.get_bool(b);
  ASSERT_EQ(b, true);

}








int build_cellinfo_exception(ObCellArray &org_cell_array)
{
  ObCellInfo cell;
  ObInnerCellInfo *cell_out = NULL;
  int64_t i;


  cell.value_.set_int(10);
  EXPECT_EQ(org_cell_array.append(cell, cell_out),OB_SUCCESS);
  cell.value_.set_int(0);
  EXPECT_EQ(org_cell_array.append(cell, cell_out),OB_SUCCESS);

  cell.value_.set_int(10);
  EXPECT_EQ(org_cell_array.append(cell, cell_out),OB_SUCCESS);
  cell.value_.set_double(0.0);
  EXPECT_EQ(org_cell_array.append(cell, cell_out),OB_SUCCESS);

  cell.value_.set_float(10);
  EXPECT_EQ(org_cell_array.append(cell, cell_out),OB_SUCCESS);
  cell.value_.set_float(0.0);
  EXPECT_EQ(org_cell_array.append(cell, cell_out),OB_SUCCESS);




  for(i = 0; i < 1000; i+=2)
  {
    cell.value_.set_int(i);
    EXPECT_EQ(org_cell_array.append(cell, cell_out),OB_SUCCESS);
    cell.value_.set_int(i+1);
    EXPECT_EQ(org_cell_array.append(cell, cell_out),OB_SUCCESS);
  }
  return 0;
}




/* test above exception setting */
TEST(Test_ob_composite_column, exception_test)
{
  ObObj arr[20];
  ObStringBuf data_buf;
	ObObj result;
  ObCellArray ca;
  int64_t beg = 2, end = 4;
  float f = 0;

  UNUSED(beg);
  UNUSED(end);
  UNUSED(f);

  ObCompositeColumn column;

  build_cellinfo_exception(ca);

  build_polish_expr_simple_div_ii(arr);
  column.set_expression(arr,data_buf);  
  ASSERT_TRUE(OB_SUCCESS == column.calc_composite_val(result, ca, 0, 1));  /* a/b = i/i */
  ASSERT_TRUE(result.get_type() == ObNullType);
  ASSERT_TRUE(OB_SUCCESS == column.calc_composite_val(result, ca, 2, 3));  /* a/b = f/i */
  ASSERT_TRUE(result.get_type() == ObNullType);
  ASSERT_TRUE(OB_SUCCESS == column.calc_composite_val(result, ca, 4, 5));  /* a/b = f/i */
  ASSERT_TRUE(result.get_type() == ObNullType);

}



#if 0
/* (a*b) + (c/d) */
int build_polish_expr_mix_efficiency(ObObj *arr)
{     
  ADD_FUNC_EXPR(arr, 0, ObExpression::COLUMN_IDX, 0);
  ADD_FUNC_EXPR(arr, 1, ObExpression::COLUMN_IDX, 1);
  ADD_FUNC_EXPR(arr, 2, ObExpression::OP, ObExpression::MUL_FUNC);
  ADD_FUNC_EXPR(arr, 3, ObExpression::COLUMN_IDX, 2);
  ADD_FUNC_EXPR(arr, 4, ObExpression::COLUMN_IDX, 3);
  ADD_FUNC_EXPR(arr, 5, ObExpression::OP, ObExpression::DIV_FUNC);
  ADD_FUNC_EXPR(arr, 6, ObExpression::OP, ObExpression::ADD_FUNC);
  END_EXPR(arr, 7);

  return 0;
}


/* test above efficiency setting */
TEST(Test_ob_composite_column, efficiency_test_int)
{
  ObObj arr[20];
  ObStringBuf data_buf;
	ObObj result;
  ObCellArray ca;
  int64_t beg = 2, end = 4;
  int64_t i = 0;
  double d = 0;
  float f = 0;
  int loop = 0;

  UNUSED(beg);
  UNUSED(end);
  UNUSED(f);
  UNUSED(d);

  ObCompositeColumn column;
  
  build_cellinfo_ii(ca);

  build_polish_expr_mix_efficiency(arr);
  column.set_expression(arr,data_buf);
  column.calc_composite_val(result, ca, 0, 3);  /* a*b + c/d */
  ASSERT_EQ(OB_SUCCESS, result.get_int(i));
  ASSERT_EQ(i, 0);
  column.calc_composite_val(result, ca, 4, 7);  /* a*b + c/d */
  ASSERT_EQ(OB_SUCCESS, result.get_int(i));
  ASSERT_EQ(i, 20);

  for(loop = 0; loop < 10000; loop++)
  {
    for (beg = 0, end = beg + 3; end < 1000; beg+= 4, end+=4)
    {
      column.calc_composite_val(result, ca, beg, end);  /* a*b + c/d */
    }
  }

  TBSYS_LOG(DEBUG, "total rows = %d", 10000 * 1000 / 4);
}




/* test above efficiency setting */
TEST(Test_ob_composite_column, efficiency_test_double)
{
  ObObj arr[20];
  ObStringBuf data_buf;
	ObObj result;
  ObCellArray ca;
  int64_t beg = 2, end = 4;
  int64_t i = 0;
  double d = 0;
  float f = 0;
  int loop = 0;

  UNUSED(beg);
  UNUSED(beg);
  UNUSED(i);
  UNUSED(f);

  ObCompositeColumn column;

  build_cellinfo_dd(ca);

  build_polish_expr_mix_efficiency(arr);
  column.set_expression(arr,data_buf);
  column.calc_composite_val(result, ca, 0, 3);  /* a*b + c/d */
  ASSERT_EQ(OB_SUCCESS, result.get_double(d));
  ASSERT_EQ(d, (double)2/3);
  column.calc_composite_val(result, ca, 4, 7);  /* a*b + c/d */
  ASSERT_EQ(OB_SUCCESS, result.get_double(d));
  ASSERT_EQ(d, 4*5 + (double)6/7);

  for(loop = 0; loop < 10000; loop++)
  {
    for (beg = 0, end = beg + 3; end < 1000; beg+= 4, end+=4)
    {
      column.calc_composite_val(result, ca, beg, end);  /* a*b + c/d */
      // @note: passed following test. 
      //        for efficiency test purpose, comment following code out
      //ASSERT_EQ(OB_SUCCESS, result.get_double(d));
      //EXPECT_EQ(d, (double)beg * (beg + 1) + ((double)beg + 2) / (beg + 3));

    }
  }
  TBSYS_LOG(DEBUG, "total rows = %d", 10000 * 1000 / 4);
}

#endif

TEST(Test_ob_composite_column, composite_test_simple_like)
{
  ObObj arr[20];
  ObStringBuf data_buf;
	ObObj result;
  ObCellArray org_cell_array;
  int64_t beg = 0, end = 0;
  bool b = false;
  ObCompositeColumn column;

  build_polish_expr_simple_like_str(arr);
  build_cellinfo_str(org_cell_array);
  
  column.set_expression(arr,data_buf);
  column.calc_composite_val(result, org_cell_array, beg = 0, end = 0);
  /* 'test_column_str' like 'col' */
  result.get_bool(b);
  ASSERT_EQ(b, true);

  column.calc_composite_val(result, org_cell_array, beg = 1, end = 1);
  /* 'test_row_str' like 'col' */
  result.get_bool(b);
  ASSERT_EQ(b, false);
}




TEST(Test_ob_composite_column, composite_test_simple_like_2)
{
  ObObj arr[20];
  ObStringBuf data_buf;
	ObObj result;
  ObCellArray org_cell_array;
  int64_t beg = 0, end = 0;
  bool b = false;
  ObCompositeColumn column;

  build_polish_expr_simple_like_str_2(arr);
  build_cellinfo_str(org_cell_array);
  
  column.set_expression(arr,data_buf);
  column.calc_composite_val(result, org_cell_array, beg = 0, end = 0);
  /* 'test_column_str' like 'col' */
  result.get_bool(b);
  ASSERT_EQ(b, true);

  column.calc_composite_val(result, org_cell_array, beg = 1, end = 1);
  /* 'test_row_str' like 'col' */
  result.get_bool(b);
  ASSERT_EQ(b, true);
}




TEST(Test_ob_composite_column, composite_test_serialization_simple)
{
  int ret =0;
  ObObj arr[20];
  ObStringBuf data_buf;
	ObObj result;
  ObCellArray org_cell_array;
  int64_t beg = 0, end = 0;
  bool b = false;

  char *exp = (char*)"a + b";
  ObString expr_str;
  expr_str.assign(exp, static_cast<int32_t>(strlen(exp)));
 
  ObCompositeColumn column;

  build_polish_expr_simple_like_str(arr);
  build_cellinfo_str(org_cell_array);
  column.set_expression(arr,data_buf); 

  const int64_t data_len = 1024*64;
  char buf[data_len];
  int64_t pos = 0;

  column.serialize(buf, data_len, pos);
  printf("after serialize, pos = %ld\n", pos);
  pos = 0;
  ret = column.deserialize(buf, data_len,pos);
  ASSERT_EQ(OB_SUCCESS, ret);
  printf("after deserialize, pos = %ld\n", pos);

  ret = column.calc_composite_val(result, org_cell_array, beg = 0, end = 0);
  ASSERT_EQ(OB_SUCCESS, ret);
  /* 'test_column_str' like 'col' */
  ASSERT_EQ(OB_SUCCESS, result.get_bool(b));
  ASSERT_EQ(b, true);  
 
  ret = column.calc_composite_val(result, org_cell_array, beg = 1, end = 1);
  ASSERT_EQ(OB_SUCCESS, ret);
  /* 'test_row_str' like 'col' */
  ASSERT_EQ(OB_SUCCESS, result.get_bool(b));
  ASSERT_EQ(b, false);

}


TEST(Test_ob_composite_column, composite_test_null_like_pattern)
{
  int ret =0;
  ObObj arr[20];
  ObStringBuf data_buf;
	ObObj result;
  ObCellArray org_cell_array;
  int64_t beg = 0, end = 0;
 
  ObCompositeColumn column;

  build_polish_expr_null_like_str(arr);
  build_cellinfo_str(org_cell_array);
  column.set_expression(arr,data_buf); 


  ret = column.calc_composite_val(result, org_cell_array, beg = 0, end = 0);
  ASSERT_EQ(OB_SUCCESS, ret);
  /* null like 'col' */
  ASSERT_TRUE(result.is_null());
}


TEST(Test_ob_composite_column, composite_test_serialization_complex)
{
  ObObj arr[20];
  ObStringBuf data_buf;
	ObObj result;
  ObCellArray ca;
  int64_t beg = 2, end = 4;
  bool b = false;
  double d = 0;
  float f = 0;

  const int64_t data_len = 1024*64;
  char buf[data_len];
  int64_t pos = 0;
  UNUSED(beg);
  UNUSED(end);
  UNUSED(f);

  build_cellinfo_dd(ca);

  ObCompositeColumn column;

  ObCompositeColumn my_column;
  
  
  
  build_polish_expr_comp_add_mix_iii(arr);
  column.set_expression(arr,data_buf);
  column.calc_composite_val(result, ca, 0, 2);  /* a+b+c , 0+1+2*/
  ASSERT_EQ(OB_SUCCESS, result.get_double(d));
  ASSERT_EQ(d, 3);
// ====>>>
  pos = 0;
  column.serialize(buf, data_len, pos);
  printf("after serialize, pos = %ld\n", pos);
  pos = 0;
  my_column.deserialize(buf, data_len,pos);
  printf("after deserialize, pos = %ld\n", pos);
  my_column.calc_composite_val(result, ca, 0, 2);  /*  a+b+c */
  ASSERT_EQ(OB_SUCCESS, result.get_double(d));
  ASSERT_EQ(d, 3);
  my_column.calc_composite_val(result, ca, 3, 5);  /*  a+b+c */
  ASSERT_EQ(OB_SUCCESS, result.get_double(d));
  ASSERT_EQ(d, 12);
// <<<====
 
  build_polish_expr_comp_arith_mix_iii(arr);
  column.set_expression(arr,data_buf);
  column.calc_composite_val(result, ca, 0, 3);  /* a*b/c , 0*1/2*/
  ASSERT_EQ(OB_SUCCESS, result.get_double(d));
  ASSERT_EQ(d, 0);
 // ====>>>
  pos = 0;
  column.serialize(buf, data_len, pos);
  printf("after serialize, pos = %ld\n", pos);
  pos = 0;
  my_column.deserialize(buf, data_len,pos);
  printf("after deserialize, pos = %ld\n", pos);
  my_column.calc_composite_val(result, ca, 3, 6);  /* a*b/c , 3*4/5*/
  ASSERT_EQ(OB_SUCCESS, result.get_double(d));
  ASSERT_EQ(d, (double)3.0*4/5);
// <<<====
  column.calc_composite_val(result, ca, 3, 6);  /* a*b/c , 3*4/5*/
  ASSERT_EQ(OB_SUCCESS, result.get_double(d));
  ASSERT_EQ(d, (double)3.0*4/5);

  build_polish_expr_comp_logic_mix_iii(arr);
  column.set_expression(arr,data_buf);
  column.calc_composite_val(result, ca, 0, 3);  /* a&&b||c */
  ASSERT_EQ(OB_SUCCESS, result.get_bool(b));
  ASSERT_EQ(b, true);


  build_polish_expr_comp_compare_mix_iii(arr);
  column.set_expression(arr,data_buf);
// ====>>>
  pos = 0;
  column.serialize(buf, data_len, pos);
  printf("after serialize, pos = %ld\n", pos);
  pos = 0;
  my_column.deserialize(buf, data_len,pos);
  printf("after deserialize, pos = %ld\n", pos);
  my_column.calc_composite_val(result, ca, 0, 4);  /* (a+b) <= (c+d)  */
  ASSERT_EQ(OB_SUCCESS, result.get_bool(b));
  ASSERT_EQ(b, true);

  // <<<====
  column.calc_composite_val(result, ca, 0, 4);  /* (a+b) <= (c+d)  */
  ASSERT_EQ(OB_SUCCESS, result.get_bool(b));
  ASSERT_EQ(b, true);

}









/* a * b + b * 10 */
int build_polish_expr_2(ObObj *arr)
{         
  ADD_FUNC_EXPR(arr, 0, ObExpression::COLUMN_IDX, 0);
  ADD_FUNC_EXPR(arr, 1, ObExpression::COLUMN_IDX, 1);
  ADD_FUNC_EXPR(arr, 2, ObExpression::OP, ObExpression::MUL_FUNC);

  ADD_FUNC_EXPR(arr, 3, ObExpression::COLUMN_IDX, 1);
  ADD_FUNC_EXPR(arr, 4, ObExpression::CONST_OBJ, 10);
  ADD_FUNC_EXPR(arr, 5, ObExpression::OP, ObExpression::MUL_FUNC);

  ADD_FUNC_EXPR(arr, 6, ObExpression::OP, ObExpression::ADD_FUNC);

  END_EXPR(arr, 7);

  return 0;
}   


/* (a * b) < (b * 10) */
int build_polish_expr_3(ObObj *arr)
{         
  ADD_FUNC_EXPR(arr, 0, ObExpression::COLUMN_IDX, 0);
  ADD_FUNC_EXPR(arr, 1, ObExpression::COLUMN_IDX, 1);
  ADD_FUNC_EXPR(arr, 2, ObExpression::OP, ObExpression::MUL_FUNC);

  ADD_FUNC_EXPR(arr, 3, ObExpression::COLUMN_IDX, 1);
  ADD_FUNC_EXPR(arr, 4, ObExpression::CONST_OBJ, 10);
  ADD_FUNC_EXPR(arr, 5, ObExpression::OP, ObExpression::MUL_FUNC);

  ADD_FUNC_EXPR(arr, 6, ObExpression::OP, ObExpression::LT_FUNC);

  END_EXPR(arr, 7);

  return 0;
}   



int build_cell_info_float(ObCellArray &org_cell_array)
{
  ObCellInfo cell;
  ObInnerCellInfo *cell_out = NULL;
  int64_t i;

  for(i = 0; i < 1000; i+=2)
  {
    cell.value_.set_int(i);
    EXPECT_EQ(org_cell_array.append(cell, cell_out),OB_SUCCESS);
    cell.value_.set_float((float)(i+1));
    EXPECT_EQ(org_cell_array.append(cell, cell_out),OB_SUCCESS);
  }
  return 0;
}



int build_cell_info_int(ObCellArray &org_cell_array)
{
  ObCellInfo cell;
  ObInnerCellInfo *cell_out = NULL;
  int64_t i;

  for(i = 0; i < 1000; i+=2)
  {
    cell.value_.set_int(i);
    EXPECT_EQ(org_cell_array.append(cell, cell_out),OB_SUCCESS);
    cell.value_.set_int((i+1));
    EXPECT_EQ(org_cell_array.append(cell, cell_out),OB_SUCCESS);
  }
  return 0;
}


int build_cell_info_str(ObCellArray &org_cell_array)
{
  char *ptr1 = (char*)"test_column_str";
  char *ptr2 = (char*)"test_row_str";
  ObCellInfo cell;
  ObInnerCellInfo *cell_out = NULL;
  int64_t i;
  ObString str1;
  ObString str2;
  str1.assign(ptr1,static_cast<int32_t>(strlen(ptr1)));
  str2.assign(ptr2,static_cast<int32_t>(strlen(ptr2)));

  for(i = 0; i < 1000; i+=2)
  {
    cell.value_.set_varchar(str1);
    EXPECT_EQ(org_cell_array.append(cell, cell_out),OB_SUCCESS);
    cell.value_.set_varchar(str2);
    EXPECT_EQ(org_cell_array.append(cell, cell_out),OB_SUCCESS);
  }
  return 0;
}




/* a * b + b * 10 */
TEST(Test_ob_composite_column, composite_test)
{
  ObObj arr[20];
  ObStringBuf data_buf;
	ObObj result;
  ObCellArray org_cell_array;
  int64_t beg = 0, end = 0;

  ObCompositeColumn column;

  build_polish_expr_2(arr);
  build_cell_info_float(org_cell_array);
 
  column.set_expression(arr,data_buf);

  float result_list[5] = {10.0, 36.0, 70.0, 112.0, 162.0};
  float f = 0.0;

  for(beg = 0, end = beg + 2; beg < 10; beg+=2, end+=2)
  {
    column.calc_composite_val(result, org_cell_array, beg, end);
    EXPECT_EQ(OB_SUCCESS, result.get_float(f));
    ASSERT_EQ(f, result_list[beg/2]);
    //printf("type: %d\n", result.get_type());
  }
} 


/* a * b < b * 10 */
TEST(Test_ob_composite_column, composite_test_lt)
{
  ObObj arr[20];
  ObStringBuf data_buf;
	ObObj result;
  ObCellArray org_cell_array;
  int64_t beg = 0, end = 0;

  ObCompositeColumn column;

  build_polish_expr_3(arr);
  build_cell_info_float(org_cell_array);
  
  column.set_expression(arr,data_buf);
  //int64_t result_list[5] = {0, 1, 1, 1, 1};
  bool result_list[5] = {true, true, true, true, true};
  bool b;

  for(beg = 0, end = beg + 2; beg < 10; beg+=2, end+=2)
  {
    column.calc_composite_val(result, org_cell_array, beg, end);
    ASSERT_EQ(OB_SUCCESS, result.get_bool(b));
    //printf("beg=%ld, b =%ld\n", beg, b);
    ASSERT_EQ(b, result_list[beg/2]);
    //printf("type: %d\n", result.get_type());
  }
}


/* (b + a) < (a * 2) */
int build_polish_expr_4(ObObj *arr)
{         
  ADD_FUNC_EXPR(arr, 0, ObExpression::COLUMN_IDX, 0);
  ADD_FUNC_EXPR(arr, 1, ObExpression::COLUMN_IDX, 1);
  ADD_FUNC_EXPR(arr, 2, ObExpression::OP, ObExpression::ADD_FUNC);

  ADD_FUNC_EXPR(arr, 3, ObExpression::COLUMN_IDX, 0);
  ADD_FUNC_EXPR(arr, 4, ObExpression::CONST_OBJ, 2);
  ADD_FUNC_EXPR(arr, 5, ObExpression::OP, ObExpression::MUL_FUNC);

  ADD_FUNC_EXPR(arr, 6, ObExpression::OP, ObExpression::LT_FUNC);

  END_EXPR(arr, 7);

  return 0;
}   



/* (b + a) < (a * 2) */
TEST(Test_ob_composite_column, composite_test_lt2)
{
  ObObj arr[20];
  ObStringBuf data_buf;
	ObObj result;
  ObCellArray org_cell_array;
  int64_t beg = 0, end = 0;

  ObCompositeColumn column;

  build_polish_expr_4(arr);
  build_cell_info_int(org_cell_array);
  
  column.set_expression(arr,data_buf);
  bool result_list[5] = {false, false, false, false, false};
  bool b = false;

  for(beg = 0, end = beg + 2; beg < 10; beg+=2, end+=2)
  {
    column.calc_composite_val(result, org_cell_array, beg, end);
    ASSERT_EQ(OB_SUCCESS, result.get_bool(b));
    //printf("beg=%ld, b =%ld\n", beg, b);
    ASSERT_EQ(b, result_list[beg/2]);
    //printf("type: %d\n", result.get_type());
  }
}


/* ((a + b) < (a * 2)) || (a < 3) */
int build_polish_expr_5(ObObj *arr)
{
  ADD_FUNC_EXPR(arr, 0, ObExpression::COLUMN_IDX, 0);
  ADD_FUNC_EXPR(arr, 1, ObExpression::COLUMN_IDX, 1);
  ADD_FUNC_EXPR(arr, 2, ObExpression::OP, ObExpression::ADD_FUNC);

  ADD_FUNC_EXPR(arr, 3, ObExpression::COLUMN_IDX, 0);
  ADD_FUNC_EXPR(arr, 4, ObExpression::CONST_OBJ, 2);
  ADD_FUNC_EXPR(arr, 5, ObExpression::OP, ObExpression::MUL_FUNC);

  ADD_FUNC_EXPR(arr, 6, ObExpression::OP, ObExpression::LT_FUNC);

  ADD_FUNC_EXPR(arr, 7, ObExpression::COLUMN_IDX, 0);
  ADD_FUNC_EXPR(arr, 8, ObExpression::CONST_OBJ, 3);
  ADD_FUNC_EXPR(arr, 9, ObExpression::OP, ObExpression::LT_FUNC);

  ADD_FUNC_EXPR(arr, 10, ObExpression::OP, ObExpression::OR_FUNC);

  END_EXPR(arr, 11);

  return 0;
}   



/* ((a + b) < (a * 2)) || (a < 3) */
TEST(Test_ob_composite_column, composite_test_lt_or1)
{
  ObObj arr[60];
  ObStringBuf data_buf;
	ObObj result;
  ObCellArray org_cell_array;
  int64_t beg = 0, end = 0;

  ObCompositeColumn column;

  build_polish_expr_5(arr);
  build_cell_info_int(org_cell_array);
  
  column.set_expression(arr,data_buf);

  bool result_list[5] = {true, true, false, false, false};
  bool b;

  for(beg = 0, end = beg + 2; beg < 10; beg+=2, end+=2)
  {
    column.calc_composite_val(result, org_cell_array, beg, end);
    ASSERT_EQ(OB_SUCCESS, result.get_bool(b));    
    //printf("beg=%ld, b =%ld\n", beg, b);
    ASSERT_EQ(b, result_list[beg/2]);
    //printf("type: %d\n", result.get_type());
  }
}


//#if 0
/* ((a + b) < (a * 3)) && (a > 3) */
int build_polish_expr_6(ObObj *arr)
{
  ADD_FUNC_EXPR(arr, 0, ObExpression::COLUMN_IDX, 0);
  ADD_FUNC_EXPR(arr, 1, ObExpression::COLUMN_IDX, 1);
  ADD_FUNC_EXPR(arr, 2, ObExpression::OP, ObExpression::ADD_FUNC);

  ADD_FUNC_EXPR(arr, 3, ObExpression::COLUMN_IDX, 0);
  ADD_FUNC_EXPR(arr, 4, ObExpression::CONST_OBJ, 2);
  ADD_FUNC_EXPR(arr, 5, ObExpression::OP, ObExpression::MUL_FUNC);

  ADD_FUNC_EXPR(arr, 6, ObExpression::OP, ObExpression::LT_FUNC);

  ADD_FUNC_EXPR(arr, 7, ObExpression::COLUMN_IDX, 0);
  ADD_FUNC_EXPR(arr, 8, ObExpression::CONST_OBJ, 3);
  ADD_FUNC_EXPR(arr, 9, ObExpression::OP, ObExpression::GT_FUNC);

  ADD_FUNC_EXPR(arr, 10, ObExpression::OP, ObExpression::AND_FUNC);

  END_EXPR(arr, 11);

  return 0;
}   


TEST(Test_ob_composite_column, composite_test_lt_gt_and1)
{
  ObObj arr[40];
  ObStringBuf data_buf;
	ObObj result;
  ObCellArray org_cell_array;
  int64_t beg = 0, end = 0;

  ObCompositeColumn column;

  build_polish_expr_6(arr);
  build_cell_info_int(org_cell_array);

  column.set_expression(arr,data_buf);
  //int64_t result_list[5] = {0, 0, 1, 1, 1};
  bool result_list[5] = {false, false, false, false, false};
  bool b = false;

  for(beg = 0, end = beg + 2; beg < 10; beg+=2, end+=2)
  {
    column.calc_composite_val(result, org_cell_array, beg, end);
    ASSERT_EQ(OB_SUCCESS, result.get_bool(b));
    //printf("beg=%ld, b =%ld\n", beg, b);
    ASSERT_EQ(b, result_list[beg/2]);
    //printf("type: %d\n", result.get_type());
  }
}

//#endif


/* (a != b) */
int build_polish_expr_7(ObObj *arr)
{
  ADD_FUNC_EXPR(arr, 0, ObExpression::COLUMN_IDX, 0);
  ADD_FUNC_EXPR(arr, 1, ObExpression::COLUMN_IDX, 1);
  ADD_FUNC_EXPR(arr, 2, ObExpression::OP, ObExpression::NE_FUNC);

  END_EXPR(arr, 3);

  return 0;
}   


TEST(Test_ob_composite_column, composite_test_simple_neq)
{
  ObObj arr[20];
  ObStringBuf data_buf;
	ObObj result;
  ObCellArray org_cell_array;
  int64_t beg = 0, end = 0;

  ObString as_name;
  as_name.assign(const_cast<char*>("test_column"),static_cast<int32_t>(strlen("test_column")));
  ObCompositeColumn column;

  build_polish_expr_7(arr);
  build_cell_info_int(org_cell_array);
 
  column.set_expression(arr,data_buf);
  bool result_list[5] = {true, true, true, true, true};
  bool b = false;
  for(beg = 0, end = beg + 2; beg < 10; beg+=2, end+=2)
  {
    column.calc_composite_val(result, org_cell_array, beg, end);
    ASSERT_EQ(OB_SUCCESS, result.get_bool(b));
    //printf("beg=%ld, b =%ld\n", beg, b);
    ASSERT_EQ(b, result_list[beg/2]);
    //printf("type: %d\n", result.get_type());
  }
}




/* a == b */
int build_polish_expr_8(ObObj *arr)
{
  ADD_FUNC_EXPR(arr, 0, ObExpression::COLUMN_IDX, 0);
  ADD_FUNC_EXPR(arr, 1, ObExpression::COLUMN_IDX, 1);
  ADD_FUNC_EXPR(arr, 2, ObExpression::OP, ObExpression::EQ_FUNC);

  END_EXPR(arr, 3);

  return 0;
}   


TEST(Test_ob_composite_column, composite_test_simple_eq)
{
  ObObj arr[20];
  ObStringBuf data_buf;
	ObObj result;
  ObCellArray org_cell_array;
  int64_t beg = 0, end = 0;

  ObCompositeColumn column;

  build_polish_expr_8(arr);
  build_cell_info_int(org_cell_array);
  
  column.set_expression(arr,data_buf);
  //int64_t result_list[5] = {1, 1, 1,1,1};
  bool result_list[5] = {false, false, false, false, false};
  bool b = false;

  for(beg = 0, end = beg + 2; beg < 10; beg+=2, end+=2)
  {
    column.calc_composite_val(result, org_cell_array, beg, end);
    ASSERT_EQ(OB_SUCCESS, result.get_bool(b));
    //printf("beg=%ld, b =%ld\n", beg, b);
    ASSERT_EQ(b, result_list[beg/2]);
    //printf("type: %d\n", result.get_type());
  }
}


/* a_str LIKE b_const_str */
int build_polish_expr_9(ObObj *arr)
{
  char *p = (char*)"col";  /* match test_column */
  ObString pattern_str;
  pattern_str.assign(p, static_cast<int32_t>(strlen(p)));

  ADD_FUNC_EXPR(arr, 0, ObExpression::COLUMN_IDX, 0);
  ADD_VARCHAR_EXPR(arr, 1, pattern_str);
  ADD_FUNC_EXPR(arr, 2, ObExpression::OP, ObExpression::LIKE_FUNC);

  END_EXPR(arr, 3);

  return 0;
}   

//////////////////////////////////////////////////////////////////////////////////////
//  测试复合列功能 
//////////////////////////////////////////////////////////////////////////////////////

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


TEST(Test_ob_composite_column, calc_test_1)
{
  ObPostfixExpression postexpr;
  ObExpressionParser parser;
  hash::ObHashMap<ObString,int64_t, hash::NoPthreadDefendMode> name_to_idx_map;  
  ObCellArray cells;
  ObStringBuf data_buf;
	ObObj result;
  ObCompositeColumn column;
  
  build_single_cell(cells);
  int64_t i = 0;
  double d = 0.0;

  ObString as_name;
  as_name.assign((char*)"fake", static_cast<int32_t>(strlen("fake")));

  ObString str;
  char *expr = NULL;
  const ObObj *expr_array = NULL;
 
  expr = (char*)"2";
  str.assign(expr, static_cast<int32_t>(strlen(expr)));
  EXPECT_EQ(OB_SUCCESS, postexpr.set_expression(str, name_to_idx_map, parser));
  expr_array = postexpr.get_expression();
  EXPECT_EQ(OB_SUCCESS, column.set_expression(expr_array, data_buf));
  EXPECT_EQ(OB_SUCCESS, column.calc_composite_val(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_int(i));
  ASSERT_EQ(i, 2);


  expr = (char*)"2+7";
  str.assign(expr, static_cast<int32_t>(strlen(expr)));
  EXPECT_EQ(OB_SUCCESS, postexpr.set_expression(str, name_to_idx_map, parser));
  expr_array = postexpr.get_expression();
  EXPECT_EQ(OB_SUCCESS, column.set_expression(expr_array, data_buf));
  EXPECT_EQ(OB_SUCCESS, column.calc_composite_val(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_int(i));
  ASSERT_EQ(i, 9);

  expr = (char*)"2-7";
  str.assign(expr, static_cast<int32_t>(strlen(expr)));
  EXPECT_EQ(OB_SUCCESS, postexpr.set_expression(str, name_to_idx_map, parser));
  expr_array = postexpr.get_expression();
  EXPECT_EQ(OB_SUCCESS, column.set_expression(expr_array, data_buf));
  EXPECT_EQ(OB_SUCCESS, column.calc_composite_val(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_int(i));
  ASSERT_EQ(i, -5);
 
  expr = (char*)"2*7";
  str.assign(expr, static_cast<int32_t>(strlen(expr)));
  EXPECT_EQ(OB_SUCCESS, postexpr.set_expression(str, name_to_idx_map, parser));
  expr_array = postexpr.get_expression();
  EXPECT_EQ(OB_SUCCESS, column.set_expression(expr_array, data_buf));
  EXPECT_EQ(OB_SUCCESS, column.calc_composite_val(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_int(i));
  ASSERT_EQ(i, 14);

  expr = (char*)"8/2";
  str.assign(expr, static_cast<int32_t>(strlen(expr)));
  EXPECT_EQ(OB_SUCCESS, postexpr.set_expression(str, name_to_idx_map, parser));
  expr_array = postexpr.get_expression();
  EXPECT_EQ(OB_SUCCESS, column.set_expression(expr_array, data_buf));
  EXPECT_EQ(OB_SUCCESS, column.calc_composite_val(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_double(d));
  ASSERT_EQ(d, 4.0);

  expr = (char*)"2.0+7";
  str.assign(expr, static_cast<int32_t>(strlen(expr)));
  EXPECT_EQ(OB_SUCCESS, postexpr.set_expression(str, name_to_idx_map, parser));
  expr_array = postexpr.get_expression();
  EXPECT_EQ(OB_SUCCESS, column.set_expression(expr_array, data_buf));
  EXPECT_EQ(OB_SUCCESS, column.calc_composite_val(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_double(d));
  ASSERT_EQ(d, 9);

  expr = (char*)"2.0-7";
  str.assign(expr, static_cast<int32_t>(strlen(expr)));
  EXPECT_EQ(OB_SUCCESS, postexpr.set_expression(str, name_to_idx_map, parser));
  expr_array = postexpr.get_expression();
  EXPECT_EQ(OB_SUCCESS, column.set_expression(expr_array, data_buf));
  EXPECT_EQ(OB_SUCCESS, column.calc_composite_val(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_double(d));
  ASSERT_EQ(d, -5);
 
  expr = (char*)"2.0*7";
  str.assign(expr, static_cast<int32_t>(strlen(expr)));
  EXPECT_EQ(OB_SUCCESS, postexpr.set_expression(str, name_to_idx_map, parser));
  expr_array = postexpr.get_expression();
  EXPECT_EQ(OB_SUCCESS, column.set_expression(expr_array, data_buf));
  EXPECT_EQ(OB_SUCCESS, column.calc_composite_val(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_double(d));
  ASSERT_EQ(d, 14);

  expr = (char*)"8.0/2";
  str.assign(expr, static_cast<int32_t>(strlen(expr)));
  EXPECT_EQ(OB_SUCCESS, postexpr.set_expression(str, name_to_idx_map, parser));
  expr_array = postexpr.get_expression();
  EXPECT_EQ(OB_SUCCESS, column.set_expression(expr_array, data_buf));
  EXPECT_EQ(OB_SUCCESS, column.calc_composite_val(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_double(d));
  ASSERT_EQ(d, 4.0);

  expr = (char*)"2+7.0";
  str.assign(expr, static_cast<int32_t>(strlen(expr)));
  EXPECT_EQ(OB_SUCCESS, postexpr.set_expression(str, name_to_idx_map, parser));
  expr_array = postexpr.get_expression();
  EXPECT_EQ(OB_SUCCESS, column.set_expression(expr_array, data_buf));
  EXPECT_EQ(OB_SUCCESS, column.calc_composite_val(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_double(d));
  ASSERT_EQ(d, 9);

  expr = (char*)"2-7.0";
  str.assign(expr, static_cast<int32_t>(strlen(expr)));
  EXPECT_EQ(OB_SUCCESS, postexpr.set_expression(str, name_to_idx_map, parser));
  expr_array = postexpr.get_expression();
  EXPECT_EQ(OB_SUCCESS, column.set_expression(expr_array, data_buf));
  EXPECT_EQ(OB_SUCCESS, column.calc_composite_val(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_double(d));
  ASSERT_EQ(d, -5);
 
  expr = (char*)"2*7.0";
  str.assign(expr, static_cast<int32_t>(strlen(expr)));
  EXPECT_EQ(OB_SUCCESS, postexpr.set_expression(str, name_to_idx_map, parser));
  expr_array = postexpr.get_expression();
  EXPECT_EQ(OB_SUCCESS, column.set_expression(expr_array, data_buf));
  EXPECT_EQ(OB_SUCCESS, column.calc_composite_val(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_double(d));
  ASSERT_EQ(d, 14);

  expr = (char*)"8/2.0";
  str.assign(expr, static_cast<int32_t>(strlen(expr)));
  EXPECT_EQ(OB_SUCCESS, postexpr.set_expression(str, name_to_idx_map, parser));
  expr_array = postexpr.get_expression();
  EXPECT_EQ(OB_SUCCESS, column.set_expression(expr_array, data_buf));
  EXPECT_EQ(OB_SUCCESS, column.calc_composite_val(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_double(d));
  ASSERT_EQ(d, 4.0);

  expr = (char*)"2.0+7.0";
  str.assign(expr, static_cast<int32_t>(strlen(expr)));
  EXPECT_EQ(OB_SUCCESS, postexpr.set_expression(str, name_to_idx_map, parser));
  expr_array = postexpr.get_expression();
  EXPECT_EQ(OB_SUCCESS, column.set_expression(expr_array, data_buf));
  EXPECT_EQ(OB_SUCCESS, column.calc_composite_val(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_double(d));
  ASSERT_EQ(d, 9);

  expr = (char*)"2.0-7.0";
  str.assign(expr, static_cast<int32_t>(strlen(expr)));
  EXPECT_EQ(OB_SUCCESS, postexpr.set_expression(str, name_to_idx_map, parser));
  expr_array = postexpr.get_expression();
  EXPECT_EQ(OB_SUCCESS, column.set_expression(expr_array, data_buf));
  EXPECT_EQ(OB_SUCCESS, column.calc_composite_val(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_double(d));
  ASSERT_EQ(d, -5);
 
  expr = (char*)"2.0*7.0";
  str.assign(expr, static_cast<int32_t>(strlen(expr)));
  EXPECT_EQ(OB_SUCCESS, postexpr.set_expression(str, name_to_idx_map, parser));
  expr_array = postexpr.get_expression();
  EXPECT_EQ(OB_SUCCESS, column.set_expression(expr_array, data_buf));
  EXPECT_EQ(OB_SUCCESS, column.calc_composite_val(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_double(d));
  ASSERT_EQ(d, 14);

  expr = (char*)"8.0/2.0";
  str.assign(expr, static_cast<int32_t>(strlen(expr)));
  EXPECT_EQ(OB_SUCCESS, postexpr.set_expression(str, name_to_idx_map, parser));
  expr_array = postexpr.get_expression();
  EXPECT_EQ(OB_SUCCESS, column.set_expression(expr_array, data_buf));
  EXPECT_EQ(OB_SUCCESS, column.calc_composite_val(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_double(d));
  ASSERT_EQ(d, 4.0);

  expr = (char*)"3 / 0.0";
  str.assign(expr, static_cast<int32_t>(strlen(expr)));
  EXPECT_EQ(OB_SUCCESS, postexpr.set_expression(str, name_to_idx_map, parser));
  expr_array = postexpr.get_expression();
  EXPECT_EQ(OB_SUCCESS, column.set_expression(expr_array, data_buf));
  EXPECT_TRUE(OB_SUCCESS == column.calc_composite_val(result, cells, 0, 0));
  ASSERT_TRUE(result.get_type() == ObNullType);

  /*
  expr = (char*)"2";
  str.assign(expr, static_cast<int32_t>(strlen(expr)));
  EXPECT_EQ(OB_SUCCESS, column.set_expression(str, as_name));
  EXPECT_EQ(OB_SUCCESS, column.decode(scan_param));
  EXPECT_EQ(OB_SUCCESS, column.calc_composite_val(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_bool(b));
  ASSERT_EQ(b, true);
  */

  /// FIXME: NOT op is not supported yet
  /*
  printf("=============\n\n\n");

  expr = (char*)"!2";
  str.assign(expr, static_cast<int32_t>(strlen(expr)));
  EXPECT_EQ(OB_SUCCESS, column.set_expression(str, as_name));
  EXPECT_EQ(OB_SUCCESS, column.decode(scan_param));
  EXPECT_EQ(OB_SUCCESS, column.calc_composite_val(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_bool(b));
  ASSERT_EQ(b, false);

  expr = (char*)"!0.0";
  str.assign(expr, static_cast<int32_t>(strlen(expr)));
  EXPECT_EQ(OB_SUCCESS, column.set_expression(str, as_name));
  EXPECT_EQ(OB_SUCCESS, column.decode(scan_param));
  EXPECT_EQ(OB_SUCCESS, column.calc_composite_val(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_bool(b));
  ASSERT_EQ(b, true);

  expr = (char*)"!2.3";
  str.assign(expr, static_cast<int32_t>(strlen(expr)));
  EXPECT_EQ(OB_SUCCESS, column.set_expression(str, as_name));
  EXPECT_EQ(OB_SUCCESS, column.decode(scan_param));
  EXPECT_EQ(OB_SUCCESS, column.calc_composite_val(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_bool(b));
  ASSERT_EQ(b, false);
  */

  
}


TEST(Test_ob_composite_column, calc_test_2)
{
  ObPostfixExpression postexpr;
  ObExpressionParser parser;
  hash::ObHashMap<ObString,int64_t, hash::NoPthreadDefendMode> name_to_idx_map;  
  ObCellArray cells;
  ObStringBuf data_buf;
	ObObj result;
  ObCompositeColumn column;
  build_single_cell(cells);
  bool b = true;

  ObString as_name;
  as_name.assign((char*)"fake", static_cast<int32_t>(strlen("fake")));

  ObString str;
  char *expr = NULL;
  const ObObj *expr_array = NULL;

  expr = (char*)"6 > 1";
  str.assign(expr, static_cast<int32_t>(strlen(expr)));
  EXPECT_EQ(OB_SUCCESS, postexpr.set_expression(str, name_to_idx_map, parser));
  expr_array = postexpr.get_expression();
  EXPECT_EQ(OB_SUCCESS, column.set_expression(expr_array, data_buf));
  EXPECT_EQ(OB_SUCCESS, column.calc_composite_val(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_bool(b));
  ASSERT_EQ(b, true);

  expr = (char*)"6 > 1.3";
  str.assign(expr, static_cast<int32_t>(strlen(expr)));
  EXPECT_EQ(OB_SUCCESS, postexpr.set_expression(str, name_to_idx_map, parser));
  expr_array = postexpr.get_expression();
  EXPECT_EQ(OB_SUCCESS, column.set_expression(expr_array, data_buf));
  EXPECT_EQ(OB_SUCCESS, column.calc_composite_val(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_bool(b));
  ASSERT_EQ(b, true);

  expr = (char*)"6 < 1";
  str.assign(expr, static_cast<int32_t>(strlen(expr)));
  EXPECT_EQ(OB_SUCCESS, postexpr.set_expression(str, name_to_idx_map, parser));
  expr_array = postexpr.get_expression();
  EXPECT_EQ(OB_SUCCESS, column.set_expression(expr_array, data_buf));
  EXPECT_EQ(OB_SUCCESS, column.calc_composite_val(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_bool(b));
  ASSERT_EQ(b, false);

  expr = (char*)"6 < 1.23";
  str.assign(expr, static_cast<int32_t>(strlen(expr)));
  EXPECT_EQ(OB_SUCCESS, postexpr.set_expression(str, name_to_idx_map, parser));
  expr_array = postexpr.get_expression();
  EXPECT_EQ(OB_SUCCESS, column.set_expression(expr_array, data_buf));
  EXPECT_EQ(OB_SUCCESS, column.calc_composite_val(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_bool(b));
  ASSERT_EQ(b, false);

  expr = (char*)"6 >= 1";
  str.assign(expr, static_cast<int32_t>(strlen(expr)));
  EXPECT_EQ(OB_SUCCESS, postexpr.set_expression(str, name_to_idx_map, parser));
  expr_array = postexpr.get_expression();
  EXPECT_EQ(OB_SUCCESS, column.set_expression(expr_array, data_buf));
  EXPECT_EQ(OB_SUCCESS, column.calc_composite_val(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_bool(b));
  ASSERT_EQ(b, true);

  expr = (char*)"6 >= 1.3";
  str.assign(expr, static_cast<int32_t>(strlen(expr)));
  EXPECT_EQ(OB_SUCCESS, postexpr.set_expression(str, name_to_idx_map, parser));
  expr_array = postexpr.get_expression();
  EXPECT_EQ(OB_SUCCESS, column.set_expression(expr_array, data_buf));
  EXPECT_EQ(OB_SUCCESS, column.calc_composite_val(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_bool(b));
  ASSERT_EQ(b, true);

  expr = (char*)"6 <= 1";
  str.assign(expr, static_cast<int32_t>(strlen(expr)));
  EXPECT_EQ(OB_SUCCESS, postexpr.set_expression(str, name_to_idx_map, parser));
  expr_array = postexpr.get_expression();
  EXPECT_EQ(OB_SUCCESS, column.set_expression(expr_array, data_buf));
  EXPECT_EQ(OB_SUCCESS, column.calc_composite_val(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_bool(b));
  ASSERT_EQ(b, false);

  expr = (char*)"6 <= 1.23";
  str.assign(expr, static_cast<int32_t>(strlen(expr)));
  EXPECT_EQ(OB_SUCCESS, postexpr.set_expression(str, name_to_idx_map, parser));
  expr_array = postexpr.get_expression();
  EXPECT_EQ(OB_SUCCESS, column.set_expression(expr_array, data_buf));
  EXPECT_EQ(OB_SUCCESS, column.calc_composite_val(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_bool(b));
  ASSERT_EQ(b, false);

  expr = (char*)"2.0 > 7.0";
  str.assign(expr, static_cast<int32_t>(strlen(expr)));
  EXPECT_EQ(OB_SUCCESS, postexpr.set_expression(str, name_to_idx_map, parser));
  expr_array = postexpr.get_expression();
  EXPECT_EQ(OB_SUCCESS, column.set_expression(expr_array, data_buf));
  EXPECT_EQ(OB_SUCCESS, column.calc_composite_val(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_bool(b));
  ASSERT_EQ(b, false);

  expr = (char*)"2.0 < 7";
  str.assign(expr, static_cast<int32_t>(strlen(expr)));
  EXPECT_EQ(OB_SUCCESS, postexpr.set_expression(str, name_to_idx_map, parser));
  expr_array = postexpr.get_expression();
  EXPECT_EQ(OB_SUCCESS, column.set_expression(expr_array, data_buf));
  EXPECT_EQ(OB_SUCCESS, column.calc_composite_val(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_bool(b));
  ASSERT_EQ(b, true);
 
  expr = (char*)"2.0 >= 7";
  str.assign(expr, static_cast<int32_t>(strlen(expr)));
  EXPECT_EQ(OB_SUCCESS, postexpr.set_expression(str, name_to_idx_map, parser));
  expr_array = postexpr.get_expression();
  EXPECT_EQ(OB_SUCCESS, column.set_expression(expr_array, data_buf));  
  EXPECT_EQ(OB_SUCCESS, column.calc_composite_val(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_bool(b));
  ASSERT_EQ(b, false);

  expr = (char*)"2.0 <= 8.0";
  str.assign(expr, static_cast<int32_t>(strlen(expr)));
  EXPECT_EQ(OB_SUCCESS, postexpr.set_expression(str, name_to_idx_map, parser));
  expr_array = postexpr.get_expression();
  EXPECT_EQ(OB_SUCCESS, column.set_expression(expr_array, data_buf));
  EXPECT_EQ(OB_SUCCESS, column.calc_composite_val(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_bool(b));
  ASSERT_EQ(b, true);

  expr = (char*)"2.0 = 8.0";
  str.assign(expr, static_cast<int32_t>(strlen(expr)));
  EXPECT_EQ(OB_SUCCESS, postexpr.set_expression(str, name_to_idx_map, parser));
  expr_array = postexpr.get_expression();
  EXPECT_EQ(OB_SUCCESS, column.set_expression(expr_array, data_buf));
  EXPECT_EQ(OB_SUCCESS, column.calc_composite_val(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_bool(b));
  ASSERT_EQ(b, false);

  expr = (char*)"2.0 != 8";
  str.assign(expr, static_cast<int32_t>(strlen(expr)));
  EXPECT_EQ(OB_SUCCESS, postexpr.set_expression(str, name_to_idx_map, parser));
  expr_array = postexpr.get_expression();
  EXPECT_EQ(OB_SUCCESS, column.set_expression(expr_array, data_buf));
  EXPECT_EQ(OB_SUCCESS, column.calc_composite_val(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_bool(b));
  ASSERT_EQ(b, true);

  expr = (char*)"2.0 != 2";
  str.assign(expr, static_cast<int32_t>(strlen(expr)));
  EXPECT_EQ(OB_SUCCESS, postexpr.set_expression(str, name_to_idx_map, parser));
  expr_array = postexpr.get_expression();
  EXPECT_EQ(OB_SUCCESS, column.set_expression(expr_array, data_buf));
  EXPECT_EQ(OB_SUCCESS, column.calc_composite_val(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_bool(b));
  ASSERT_EQ(b, false);

  expr = (char*)"2 = 2.0";
  str.assign(expr, static_cast<int32_t>(strlen(expr)));
  EXPECT_EQ(OB_SUCCESS, postexpr.set_expression(str, name_to_idx_map, parser));
  expr_array = postexpr.get_expression();
  EXPECT_EQ(OB_SUCCESS, column.set_expression(expr_array, data_buf));
  EXPECT_EQ(OB_SUCCESS, column.calc_composite_val(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_bool(b));
  ASSERT_EQ(b, true);

  expr = (char*)"(2.0 + 3) >= (3.0 + 2)";
  str.assign(expr, static_cast<int32_t>(strlen(expr)));
  EXPECT_EQ(OB_SUCCESS, postexpr.set_expression(str, name_to_idx_map, parser));
  expr_array = postexpr.get_expression();
  EXPECT_EQ(OB_SUCCESS, column.set_expression(expr_array, data_buf));
  EXPECT_EQ(OB_SUCCESS, column.calc_composite_val(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_bool(b));
  ASSERT_EQ(b, true);

  expr = (char*)"(2.0 + 3) <= (3.0 + 2)";
  str.assign(expr, static_cast<int32_t>(strlen(expr)));
  EXPECT_EQ(OB_SUCCESS, postexpr.set_expression(str, name_to_idx_map, parser));
  expr_array = postexpr.get_expression();
  EXPECT_EQ(OB_SUCCESS, column.set_expression(expr_array, data_buf));
  EXPECT_EQ(OB_SUCCESS, column.calc_composite_val(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_bool(b));
  ASSERT_EQ(b, true);

  expr = (char*)"(1 % 3) <= (3 % 2)";
  str.assign(expr, static_cast<int32_t>(strlen(expr)));
  EXPECT_EQ(OB_SUCCESS, postexpr.set_expression(str, name_to_idx_map, parser));
  expr_array = postexpr.get_expression();
  EXPECT_EQ(OB_SUCCESS, column.set_expression(expr_array, data_buf));
  EXPECT_EQ(OB_SUCCESS, column.calc_composite_val(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_bool(b));
  ASSERT_EQ(b, true);
}

TEST(Test_ob_composite_column, calc_test_3)
{
  ObPostfixExpression postexpr;
  ObExpressionParser parser;
  hash::ObHashMap<ObString,int64_t, hash::NoPthreadDefendMode> name_to_idx_map;  
  ObCellArray cells;
  ObStringBuf data_buf;
	ObObj result;
  ObCompositeColumn column;
  build_single_cell(cells);
  int64_t i = 0;
  double d = 0.0;
  bool b = true;

  ObString as_name;
  as_name.assign((char*)"fake", static_cast<int32_t>(strlen("fake")));

  ObString str;
  char *expr = NULL;
  const ObObj *expr_array = NULL;

  expr = (char*)"6+3+1";
  str.assign(expr, static_cast<int32_t>(strlen(expr)));
  EXPECT_EQ(OB_SUCCESS, postexpr.set_expression(str, name_to_idx_map, parser));
  expr_array = postexpr.get_expression();
  EXPECT_EQ(OB_SUCCESS, column.set_expression(expr_array, data_buf));
  EXPECT_EQ(OB_SUCCESS, column.calc_composite_val(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_int(i));
  ASSERT_EQ(i, 10);

  expr = (char*)"6/3+1";
  str.assign(expr, static_cast<int32_t>(strlen(expr)));
  EXPECT_EQ(OB_SUCCESS, postexpr.set_expression(str, name_to_idx_map, parser));
  expr_array = postexpr.get_expression();
  EXPECT_EQ(OB_SUCCESS, column.set_expression(expr_array, data_buf));
  EXPECT_EQ(OB_SUCCESS, column.calc_composite_val(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_double(d));
  ASSERT_EQ(d, 3.0);
 
  expr = (char*)"1+6/3";
  str.assign(expr, static_cast<int32_t>(strlen(expr)));
  EXPECT_EQ(OB_SUCCESS, postexpr.set_expression(str, name_to_idx_map, parser));
  expr_array = postexpr.get_expression();
  EXPECT_EQ(OB_SUCCESS, column.set_expression(expr_array, data_buf));
  EXPECT_EQ(OB_SUCCESS, column.calc_composite_val(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_double(d));
  ASSERT_EQ(d, 3.0);

  expr = (char*)"6/(3+1)";
  str.assign(expr, static_cast<int32_t>(strlen(expr)));
  EXPECT_EQ(OB_SUCCESS, postexpr.set_expression(str, name_to_idx_map, parser));
  expr_array = postexpr.get_expression();
  EXPECT_EQ(OB_SUCCESS, column.set_expression(expr_array, data_buf));
  EXPECT_EQ(OB_SUCCESS, column.calc_composite_val(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_double(d));
  ASSERT_EQ(d, 6.0/(3+1));

  expr = (char*)"6/(3+1.0)";
  str.assign(expr, static_cast<int32_t>(strlen(expr)));
  EXPECT_EQ(OB_SUCCESS, postexpr.set_expression(str, name_to_idx_map, parser));
  expr_array = postexpr.get_expression();
  EXPECT_EQ(OB_SUCCESS, column.set_expression(expr_array, data_buf));
  EXPECT_EQ(OB_SUCCESS, column.calc_composite_val(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_double(d));
  ASSERT_EQ(d, 1.5);

  expr = (char*)"6/(3+1.0)-1.5+10*10/10-10";
  str.assign(expr, static_cast<int32_t>(strlen(expr)));
  EXPECT_EQ(OB_SUCCESS, postexpr.set_expression(str, name_to_idx_map, parser));
  expr_array = postexpr.get_expression();
  EXPECT_EQ(OB_SUCCESS, column.set_expression(expr_array, data_buf));
  EXPECT_EQ(OB_SUCCESS, column.calc_composite_val(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_double(d));
  ASSERT_EQ(d, 0);

  expr = (char*)"6/(3+1.0)-1.5+10*10/10-10%4";
  str.assign(expr, static_cast<int32_t>(strlen(expr)));
  EXPECT_EQ(OB_SUCCESS, postexpr.set_expression(str, name_to_idx_map, parser));
  expr_array = postexpr.get_expression();
  EXPECT_EQ(OB_SUCCESS, column.set_expression(expr_array, data_buf));
  EXPECT_EQ(OB_SUCCESS, column.calc_composite_val(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_double(d));
  ASSERT_EQ(d, 8);

  expr = (char*)"6 AND 3";
  str.assign(expr, static_cast<int32_t>(strlen(expr)));
  EXPECT_EQ(OB_SUCCESS, postexpr.set_expression(str, name_to_idx_map, parser));
  expr_array = postexpr.get_expression();
  EXPECT_EQ(OB_SUCCESS, column.set_expression(expr_array, data_buf));
  EXPECT_EQ(OB_SUCCESS, column.calc_composite_val(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_bool(b));
  ASSERT_EQ(b, true);

  expr = (char*)"6 AND 0.0";
  str.assign(expr, static_cast<int32_t>(strlen(expr)));
  EXPECT_EQ(OB_SUCCESS, postexpr.set_expression(str, name_to_idx_map, parser));
  expr_array = postexpr.get_expression();
  EXPECT_EQ(OB_SUCCESS, column.set_expression(expr_array, data_buf));  
  EXPECT_EQ(OB_SUCCESS, column.calc_composite_val(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_bool(b));
  ASSERT_EQ(b, false);

  expr = (char*)"6 or 3";
  str.assign(expr, static_cast<int32_t>(strlen(expr)));
  EXPECT_EQ(OB_SUCCESS, postexpr.set_expression(str, name_to_idx_map, parser));
  expr_array = postexpr.get_expression();
  EXPECT_EQ(OB_SUCCESS, column.set_expression(expr_array, data_buf));
  EXPECT_EQ(OB_SUCCESS, column.calc_composite_val(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_bool(b));
  ASSERT_EQ(b, true);

  expr = (char*)"6 or 0.0";
  str.assign(expr, static_cast<int32_t>(strlen(expr)));
  EXPECT_EQ(OB_SUCCESS, postexpr.set_expression(str, name_to_idx_map, parser));
  expr_array = postexpr.get_expression();
  EXPECT_EQ(OB_SUCCESS, column.set_expression(expr_array, data_buf));
  EXPECT_EQ(OB_SUCCESS, column.calc_composite_val(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_bool(b));
  ASSERT_EQ(b, true);

  expr = (char*)"6 AND (3+1)";
  str.assign(expr, static_cast<int32_t>(strlen(expr)));
  EXPECT_EQ(OB_SUCCESS, postexpr.set_expression(str, name_to_idx_map, parser));
  expr_array = postexpr.get_expression();
  EXPECT_EQ(OB_SUCCESS, column.set_expression(expr_array, data_buf));
  EXPECT_EQ(OB_SUCCESS, column.calc_composite_val(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_bool(b));
  ASSERT_EQ(b, true);

  expr = (char*)"6.0 AND (3.2+1)";
  str.assign(expr, static_cast<int32_t>(strlen(expr)));
  EXPECT_EQ(OB_SUCCESS, postexpr.set_expression(str, name_to_idx_map, parser));
  expr_array = postexpr.get_expression();
  EXPECT_EQ(OB_SUCCESS, column.set_expression(expr_array, data_buf));
  EXPECT_EQ(OB_SUCCESS, column.calc_composite_val(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_bool(b));
  ASSERT_EQ(b, true);

  expr = (char*)"6.0 AND (3 - 3.0)";
  str.assign(expr, static_cast<int32_t>(strlen(expr)));
  EXPECT_EQ(OB_SUCCESS, postexpr.set_expression(str, name_to_idx_map, parser));
  expr_array = postexpr.get_expression();
  EXPECT_EQ(OB_SUCCESS, column.set_expression(expr_array, data_buf));
  EXPECT_EQ(OB_SUCCESS, column.calc_composite_val(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_bool(b));
  ASSERT_EQ(b, false);

  expr = (char*)"6 or (3+1)";
  str.assign(expr, static_cast<int32_t>(strlen(expr)));
  EXPECT_EQ(OB_SUCCESS, postexpr.set_expression(str, name_to_idx_map, parser));
  expr_array = postexpr.get_expression();
  EXPECT_EQ(OB_SUCCESS, column.set_expression(expr_array, data_buf));
  EXPECT_EQ(OB_SUCCESS, column.calc_composite_val(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_bool(b));
  ASSERT_EQ(b, true);

  expr = (char*)"6.0 Or (3.2+1)";
  str.assign(expr, static_cast<int32_t>(strlen(expr)));
  EXPECT_EQ(OB_SUCCESS, postexpr.set_expression(str, name_to_idx_map, parser));
  expr_array = postexpr.get_expression();
  EXPECT_EQ(OB_SUCCESS, column.set_expression(expr_array, data_buf));
  EXPECT_EQ(OB_SUCCESS, column.calc_composite_val(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_bool(b));
  ASSERT_EQ(b, true);

  expr = (char*)"6.0 oR (3 - 3.0)";
  str.assign(expr, static_cast<int32_t>(strlen(expr)));
  EXPECT_EQ(OB_SUCCESS, postexpr.set_expression(str, name_to_idx_map, parser));
  expr_array = postexpr.get_expression();
  EXPECT_EQ(OB_SUCCESS, column.set_expression(expr_array, data_buf));
  EXPECT_EQ(OB_SUCCESS, column.calc_composite_val(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_bool(b));
  ASSERT_EQ(b, true);


  expr = (char*)"(12 + 1) AND (3 - 3.0) or (3 / 2)";
  str.assign(expr, static_cast<int32_t>(strlen(expr)));
  EXPECT_EQ(OB_SUCCESS, postexpr.set_expression(str, name_to_idx_map, parser));
  expr_array = postexpr.get_expression();
  EXPECT_EQ(OB_SUCCESS, column.set_expression(expr_array, data_buf));
  EXPECT_EQ(OB_SUCCESS, column.calc_composite_val(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_bool(b));
  ASSERT_EQ(b, true);

  expr = (char*)"(12 + 1) AND (3 - 3.0) or (0 / 2)";
  str.assign(expr, static_cast<int32_t>(strlen(expr)));
  EXPECT_EQ(OB_SUCCESS, postexpr.set_expression(str, name_to_idx_map, parser));
  expr_array = postexpr.get_expression();
  EXPECT_EQ(OB_SUCCESS, column.set_expression(expr_array, data_buf));
  EXPECT_EQ(OB_SUCCESS, column.calc_composite_val(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_bool(b));
  ASSERT_EQ(b, false);

  expr = (char*)"(12 + 1) AND (3 - 3.0) or (0 / 2 AND 1 + 3 OR 4 - 1)";
  str.assign(expr, static_cast<int32_t>(strlen(expr)));
  EXPECT_EQ(OB_SUCCESS, postexpr.set_expression(str, name_to_idx_map, parser));
  expr_array = postexpr.get_expression();
  EXPECT_EQ(OB_SUCCESS, column.set_expression(expr_array, data_buf));
  EXPECT_EQ(OB_SUCCESS, column.calc_composite_val(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_bool(b));
  ASSERT_EQ(b, true);

  expr = (char*)"(12 + 1) AND (3 - 3.0) or (0 / 2 AND 1 + 3)";
  str.assign(expr, static_cast<int32_t>(strlen(expr)));
  EXPECT_EQ(OB_SUCCESS, postexpr.set_expression(str, name_to_idx_map, parser));
  expr_array = postexpr.get_expression();
  EXPECT_EQ(OB_SUCCESS, column.set_expression(expr_array, data_buf));
  EXPECT_EQ(OB_SUCCESS, column.calc_composite_val(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_bool(b));
  ASSERT_EQ(b, false);

  expr = (char*)"(12 + 1) AND (3 - 3.0) or (0 / 2 AND 1 + 3 or (2 / 5 * 0.2 AND 8 / 8.12))";
  str.assign(expr, static_cast<int32_t>(strlen(expr)));
  EXPECT_EQ(OB_SUCCESS, postexpr.set_expression(str, name_to_idx_map, parser));
  expr_array = postexpr.get_expression();
  EXPECT_EQ(OB_SUCCESS, column.set_expression(expr_array, data_buf));  
  EXPECT_EQ(OB_SUCCESS, column.calc_composite_val(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_bool(b));
  ASSERT_EQ(b, true);

  expr = (char*)"(12 + 1) AND (3 - 3.0) or (0 / 2 AND 1 + 3 or (2.0 / 5 * 0.2 AND 8 / 8.12))";
  str.assign(expr, static_cast<int32_t>(strlen(expr)));
  EXPECT_EQ(OB_SUCCESS, postexpr.set_expression(str, name_to_idx_map, parser));
  expr_array = postexpr.get_expression();
  EXPECT_EQ(OB_SUCCESS, column.set_expression(expr_array, data_buf));
  EXPECT_EQ(OB_SUCCESS, column.calc_composite_val(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_bool(b));
  ASSERT_EQ(b, true);


  expr = (char*)"(12 + 1)>143 AND (3 - 3.0 = 0) or ((2 / 5 * 0.2 AND 8 / 8.12))";
  str.assign(expr, static_cast<int32_t>(strlen(expr)));
  EXPECT_EQ(OB_SUCCESS, postexpr.set_expression(str, name_to_idx_map, parser));
  expr_array = postexpr.get_expression();
  EXPECT_EQ(OB_SUCCESS, column.set_expression(expr_array, data_buf));
  EXPECT_EQ(OB_SUCCESS, column.calc_composite_val(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_bool(b));
  ASSERT_EQ(b, true);

  expr = (char*)"(12 + 1)>143 AND (3 - 3.0 = 0) or ((2.0 / 5 * 0.2 AND 8 / 8.12))";
  str.assign(expr, static_cast<int32_t>(strlen(expr)));
  EXPECT_EQ(OB_SUCCESS, postexpr.set_expression(str, name_to_idx_map, parser));
  expr_array = postexpr.get_expression();
  EXPECT_EQ(OB_SUCCESS, column.set_expression(expr_array, data_buf));
  EXPECT_EQ(OB_SUCCESS, column.calc_composite_val(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_bool(b));
  ASSERT_EQ(b, true);

}


TEST(Test_ob_composite_column, calc_test_4)
{
  ObPostfixExpression postexpr;
  ObExpressionParser parser;
  hash::ObHashMap<ObString,int64_t, hash::NoPthreadDefendMode> name_to_idx_map;  
  ObCellArray cells;
  ObStringBuf data_buf;
	ObObj result;
  ObCompositeColumn column;
  build_single_cell(cells);
  bool b = true;

  ObString as_name;
  as_name.assign((char*)"fake", static_cast<int32_t>(strlen("fake")));

  ObString str;
  char *expr = NULL;
  const ObObj *expr_array = NULL;

  expr = (char*)"'home' like 'h'";
  str.assign(expr, static_cast<int32_t>(strlen(expr)));
  EXPECT_EQ(OB_SUCCESS, postexpr.set_expression(str, name_to_idx_map, parser));
  expr_array = postexpr.get_expression();
  EXPECT_EQ(OB_SUCCESS, column.set_expression(expr_array, data_buf));
  EXPECT_EQ(OB_SUCCESS, column.calc_composite_val(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_bool(b));
  ASSERT_EQ(b, true);

  expr = (char*)"'home' like 'hard'";
  str.assign(expr, static_cast<int32_t>(strlen(expr)));
  EXPECT_EQ(OB_SUCCESS, postexpr.set_expression(str, name_to_idx_map, parser));
  expr_array = postexpr.get_expression();
  EXPECT_EQ(OB_SUCCESS, column.set_expression(expr_array, data_buf));
  EXPECT_EQ(OB_SUCCESS, column.calc_composite_val(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_bool(b));
  ASSERT_EQ(b, false);

  expr = (char*)"'home' like 'home town'";
  str.assign(expr, static_cast<int32_t>(strlen(expr)));
  EXPECT_EQ(OB_SUCCESS, postexpr.set_expression(str, name_to_idx_map, parser));
  expr_array = postexpr.get_expression();
  EXPECT_EQ(OB_SUCCESS, column.set_expression(expr_array, data_buf));
  EXPECT_EQ(OB_SUCCESS, column.calc_composite_val(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_bool(b));
  ASSERT_EQ(b, false);

  expr = (char*)"'home town' like 'home'";
  str.assign(expr, static_cast<int32_t>(strlen(expr)));
  EXPECT_EQ(OB_SUCCESS, postexpr.set_expression(str, name_to_idx_map, parser));
  expr_array = postexpr.get_expression();
  EXPECT_EQ(OB_SUCCESS, column.set_expression(expr_array, data_buf));
  EXPECT_EQ(OB_SUCCESS, column.calc_composite_val(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_bool(b));
  ASSERT_EQ(b, true);

  expr = (char*)"'' like 'hard'";
  str.assign(expr, static_cast<int32_t>(strlen(expr)));
  EXPECT_EQ(OB_SUCCESS, postexpr.set_expression(str, name_to_idx_map, parser));
  expr_array = postexpr.get_expression();
  EXPECT_EQ(OB_SUCCESS, column.set_expression(expr_array, data_buf));
  EXPECT_EQ(OB_SUCCESS, column.calc_composite_val(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_bool(b));
  ASSERT_EQ(b, false);
}

TEST(Test_ob_composite_column, calc_test_5)
{
  ObPostfixExpression postexpr;
  ObExpressionParser parser;
  hash::ObHashMap<ObString,int64_t, hash::NoPthreadDefendMode> name_to_idx_map;  
  ObCellArray cells;
  ObStringBuf data_buf;
	ObObj result;
  ObCompositeColumn column;
  build_single_cell(cells);
  bool b = true;

  ObString as_name;
  as_name.assign((char*)"fake", static_cast<int32_t>(strlen("fake")));

  ObString str;
  char *expr = NULL;
  const ObObj *expr_array = NULL;

  expr = (char*)"NOT 10";
  str.assign(expr, static_cast<int32_t>(strlen(expr)));
  EXPECT_EQ(OB_SUCCESS, postexpr.set_expression(str, name_to_idx_map, parser));
  expr_array = postexpr.get_expression();
  EXPECT_EQ(OB_SUCCESS, column.set_expression(expr_array, data_buf));
  EXPECT_EQ(OB_SUCCESS, column.calc_composite_val(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_bool(b));
  ASSERT_EQ(b, false);

  expr = (char*)"NOT 0";
  str.assign(expr, static_cast<int32_t>(strlen(expr)));
  EXPECT_EQ(OB_SUCCESS, postexpr.set_expression(str, name_to_idx_map, parser));
  expr_array = postexpr.get_expression();
  EXPECT_EQ(OB_SUCCESS, column.set_expression(expr_array, data_buf));
  EXPECT_EQ(OB_SUCCESS, column.calc_composite_val(result, cells, 0, 0));
  ASSERT_EQ(OB_SUCCESS, result.get_bool(b));
  ASSERT_EQ(b, true);
} 


TEST(Test_ob_composite_column, single_element_test)
{
  TBSYS_LOGGER.setLogLevel("DEBUG");

  ObCellArray cells;
  ObStringBuf data_buf;
	ObObj result;
  ObCompositeColumn column;
  ObPostfixExpression postexpr;
  ObExpressionParser parser;
  hash::ObHashMap<ObString,int64_t, hash::NoPthreadDefendMode> name_to_idx_map;
  INIT_NAME_TO_IDX_HASH_3(name_to_idx_map);

  cell_array_reader("composite_column_input2.txt", cells);

  ObString str;
  char *expr = NULL;
  const ObObj *expr_array = NULL;

  expr = (char*)"`c3`";
  str.assign(expr, static_cast<int32_t>(strlen(expr)));
  EXPECT_EQ(OB_SUCCESS, postexpr.set_expression(str, name_to_idx_map, parser));
  expr_array = postexpr.get_expression();
  EXPECT_EQ(OB_SUCCESS, column.set_expression(expr_array, data_buf));
  EXPECT_EQ(OB_SUCCESS, column.calc_composite_val(result, cells, 0, 9));  
  ASSERT_EQ(OB_SUCCESS, result.get_varchar(str));
  TBSYS_LOG(DEBUG, "c3 content:[%.*s]", str.length(), str.ptr());
}


//////////////////////////////////////////////////////////////////////////////
//  测试带列名的复合列功能
//////////////////////////////////////////////////////////////////////////////

TEST(Test_ob_composite_column, schema_calc_test_1)
{
  //TBSYS_LOGGER.setLogLevel("DEBUG");

  ObCellArray cells;
  ObStringBuf data_buf;
	ObObj result;
  ObCompositeColumn column;
  ObPostfixExpression postexpr;
  ObExpressionParser parser;
  hash::ObHashMap<ObString,int64_t, hash::NoPthreadDefendMode> name_to_idx_map;
  INIT_NAME_TO_IDX_HASH_3(name_to_idx_map);

  cell_array_reader("composite_column_input.txt", cells);
  int64_t i = 0;

  ObString str;
  char *expr = NULL;

  expr = (char*)"`c0`+`c1`+`c2`+`c3`+`c4`+`c5`+`c6`+`c7`+`c8`+`c9`";
  str.assign(expr, static_cast<int32_t>(strlen(expr)));
  EXPECT_EQ(OB_SUCCESS, postexpr.set_expression(str, name_to_idx_map, parser));
  const ObObj *expr_array = postexpr.get_expression();
  EXPECT_EQ(OB_SUCCESS, column.set_expression(expr_array, data_buf));
  EXPECT_EQ(OB_SUCCESS, column.calc_composite_val(result, cells, 0, 9));
  ASSERT_EQ(OB_SUCCESS, result.get_int(i));
  ASSERT_EQ(i, 55);

  EXPECT_EQ(OB_SUCCESS, column.calc_composite_val(result, cells, 10, 19));
  ASSERT_EQ(OB_SUCCESS, result.get_int(i));
  ASSERT_EQ(i, 155);
}


TEST(Test_ob_composite_column, schema_calc_test_bug_column_name_a)
{
  //TBSYS_LOGGER.setLogLevel("DEBUG");

  ObCellArray cells;
  ObStringBuf data_buf;
	ObObj result;
  ObCompositeColumn column;
  ObPostfixExpression postexpr;
  ObExpressionParser parser;
  hash::ObHashMap<ObString,int64_t, hash::NoPthreadDefendMode> name_to_idx_map;
  INIT_NAME_TO_IDX_HASH_4(name_to_idx_map);

  cell_array_reader("composite_column_input.txt", cells);
  int64_t i = 0;

  ObString str;
  char *expr = NULL;

  expr = (char*)"a+a1+a2+a3";
  //expr =  "b+b1+b2+b3"; 
  str.assign(expr, static_cast<int32_t>(strlen(expr)));
  EXPECT_EQ(OB_SUCCESS, postexpr.set_expression(str, name_to_idx_map, parser));
  const ObObj *expr_array = postexpr.get_expression();
  EXPECT_EQ(OB_SUCCESS, column.set_expression(expr_array, data_buf));
  EXPECT_EQ(OB_SUCCESS, column.calc_composite_val(result, cells, 0, 9));
  ASSERT_EQ(OB_SUCCESS, result.get_int(i));
  ASSERT_EQ(i, 10);

  EXPECT_EQ(OB_SUCCESS, column.calc_composite_val(result, cells, 10, 19));
  ASSERT_EQ(OB_SUCCESS, result.get_int(i));
  ASSERT_EQ(i, 50);
}



TEST(Test_ob_composite_column, schema_calc_test_2)
{
  //TBSYS_LOGGER.setLogLevel("DEBUG");

  ObCellArray cells;
  ObStringBuf data_buf;
	ObObj result;
  ObCompositeColumn column;
  ObPostfixExpression postexpr;
  ObExpressionParser parser;
  hash::ObHashMap<ObString,int64_t, hash::NoPthreadDefendMode> name_to_idx_map;
  INIT_NAME_TO_IDX_HASH_3(name_to_idx_map);

  cell_array_reader("composite_column_input2.txt", cells);
  bool b = false;

  ObString str;
  char *expr = NULL;
  const ObObj *expr_array = NULL;

  expr = (char*)"`c3`";
  str.assign(expr, static_cast<int32_t>(strlen(expr)));
  EXPECT_EQ(OB_SUCCESS, postexpr.set_expression(str, name_to_idx_map, parser));
  expr_array = postexpr.get_expression();
  EXPECT_EQ(OB_SUCCESS, column.set_expression(expr_array, data_buf));
  EXPECT_EQ(OB_SUCCESS, column.calc_composite_val(result, cells, 0, 9));  
  ASSERT_EQ(OB_SUCCESS, result.get_varchar(str));
  TBSYS_LOG(DEBUG, "c3 content:[%.*s]", str.length(), str.ptr());

  expr = (char*)"`c3` like 'dream'";
  str.assign(expr, static_cast<int32_t>(strlen(expr)));
  EXPECT_EQ(OB_SUCCESS, postexpr.set_expression(str, name_to_idx_map, parser));
  expr_array = postexpr.get_expression();
  EXPECT_EQ(OB_SUCCESS, column.set_expression(expr_array, data_buf));
  EXPECT_EQ(OB_SUCCESS, column.calc_composite_val(result, cells, 0, 9));  
  ASSERT_EQ(OB_SUCCESS, result.get_bool(b));
  ASSERT_EQ(b, true);


  expr = (char*)"`c0`+`c1` <= `c2` AND `c3` like 'dream' OR `c4` % 10 = 0";
  str.assign(expr, static_cast<int32_t>(strlen(expr)));
  EXPECT_EQ(OB_SUCCESS, postexpr.set_expression(str, name_to_idx_map, parser));
  expr_array = postexpr.get_expression();
  EXPECT_EQ(OB_SUCCESS, column.set_expression(expr_array, data_buf));
  EXPECT_EQ(OB_SUCCESS, column.calc_composite_val(result, cells, 0, 9));  
  ASSERT_EQ(OB_SUCCESS, result.get_bool(b));
  ASSERT_EQ(b, true);
  EXPECT_EQ(OB_SUCCESS, column.calc_composite_val(result, cells, 10, 19));
  ASSERT_EQ(OB_SUCCESS, result.get_bool(b));
  ASSERT_EQ(b, false);
}

TEST(Test_ob_composite_column, cell_reader_util_test_1)
{
  ObString str;
  ObCellInfo cell;
  char buf[1024];
  ObCellReaderUtil cu("composite_column_input.txt");
  while(OB_SUCCESS == cu.next_cell())
  {
    if(OB_SUCCESS == cu.get_cell(cell, buf))
    {
      if (cell.value_.get_type() == ObVarcharType)
      {
        cell.value_.get_varchar(str);
        TBSYS_LOG(DEBUG, "varchar =%.*s", str.length(), str.ptr());
      }
      else
      {
        cell.value_.dump();
      }
    }
    else
    {
      TBSYS_LOG(WARN, "fail to get cell");
    }
  }
}

int main(int argc, char **argv)
{
  srandom(static_cast<uint32_t>(time(NULL)));
  ob_init_memory_pool(64*1024);
  TBSYS_LOGGER.setLogLevel("WARN");

  //ob_init_memory_pool();
  testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
