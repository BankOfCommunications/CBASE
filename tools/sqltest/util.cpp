/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 *
 * Version: 0.1: util.cpp,v 0.1 2012/02/22 12:30:53 chuanhui Exp $
 *
 * Authors:
 *   chuanhui <rizhao.ych@taobao.com>
 *     - some work details if you want
 *
 */
#include "util.h"

#define EPS 1e-4

Array::Array()
{
  row_num_ = 0;
  column_num_ = 0;

  array_ = NULL;
  type_array_ = NULL;
}

Array::~Array()
{
  clear();
}

int Array::clear()
{
  int err = OB_SUCCESS;

  if (NULL != array_)
  {
    for (int i = 0; i < row_num_; ++i)
    {
      for (int j = 0; j < column_num_; ++j)
      {
        if (array_[i][j] != NULL)
        {
          free(array_[i][j]);
          array_[i][j] = NULL;
        }
      }
      delete[] array_[i];
      array_[i] = NULL;
      delete[] type_array_[i];
      type_array_[i] = NULL;
    }
    delete[] array_;
    array_ = NULL;
    delete[] type_array_;
    type_array_ = NULL;
  }

  row_num_ = 0;
  column_num_ = 0;

  return err;
}

int Array::init(int row_num, int column_num)
{
  int err = OB_SUCCESS;

  if (row_num < 0 || column_num <= 0)
  {
    TBSYS_LOG(WARN, "invalid param, row_num=%d column_num=%d", row_num, column_num);
    err = OB_INVALID_ARGUMENT;
  }
  else
  {
    array_ = new char**[row_num];
    type_array_ = new int*[row_num];
    for (int i = 0; i < row_num; ++i)
    {
      array_[i] = new char*[column_num];
      for (int j = 0; j < column_num; ++j)
      {
        array_[i][j] = NULL;
      }

      type_array_[i] = new int[column_num];
      for (int j = 0; j < column_num; ++j)
      {
        type_array_[i][j] = 0;
      }
    }

    row_num_ = row_num;
    column_num_ = column_num;
  }

  return err;
}

int Array::get_row_num() const
{
  return row_num_;
}

int Array::get_column_num() const
{
  return column_num_;
}

bool Array::is_empty() const
{
  return 0 == row_num_;
}

int Array::set_value(int row_idx, int col_idx, const char* value, int type)
{
  int err = OB_SUCCESS;
  if (row_idx < 0 || row_idx >= row_num_)
  {
    TBSYS_LOG(WARN, "invalid row idx, row_idx=%d, row_num_=%d", row_idx, row_num_);
    err = OB_INVALID_ARGUMENT;
  }
  else if (col_idx < 0 || col_idx >= column_num_)
  {
    TBSYS_LOG(WARN, "invalid column idx, col_idx=%d, column_num_=%d", col_idx, column_num_);
    err = OB_INVALID_ARGUMENT;
  }
  else if (NULL == array_ || NULL == type_array_)
  {
    TBSYS_LOG(WARN, "not init");
    err = OB_ERROR;
  }
  else if (NULL != array_[row_idx][col_idx])
  {
    TBSYS_LOG(WARN, "the cell is set before, row_idx=%d, col_idx=%d", row_idx, col_idx);
    err = OB_ERROR;
  }
  else
  {
    int len = strlen(value) + 1;
    array_[row_idx][col_idx] = (char*) malloc(len);
    sprintf(array_[row_idx][col_idx], "%s", value);
    type_array_[row_idx][col_idx] = type;
  }

  return err;
}

int Array::get_value(int row_idx, int col_idx, char*& value, int& type)
{
  int err =  OB_SUCCESS;

  if (row_idx < 0 || row_idx >= row_num_)
  {
    TBSYS_LOG(WARN, "invalid row idx, row_idx=%d, row_num_=%d", row_idx, row_num_);
    err = OB_INVALID_ARGUMENT;
  }
  else if (col_idx < 0 || col_idx >= column_num_)
  {
    TBSYS_LOG(WARN, "invalid column idx, col_idx=%d, column_num_=%d", col_idx, column_num_);
    err = OB_INVALID_ARGUMENT;
  }
  else if (NULL == array_)
  {
    TBSYS_LOG(WARN, "not init");
    err = OB_ERROR;
  }
  else
  {
    value = array_[row_idx][col_idx];
    type = type_array_[row_idx][col_idx];
  }

  return err;
}

void Array::print() const
{
  if (NULL != array_)
  {
    for (int i = 0; i < row_num_; ++i)
    {
      for (int j = 0; j < column_num_; ++j)
      {
        fprintf(stderr, "%10s", array_[i][j]);
      }
      fprintf(stderr, "\n");
    }
    fprintf(stderr, "\n");
  }
  else
  {
    fprintf(stderr, "Array is empty\n");
  }
}

int Array::compare(Array& other) const
{
  int cmp_val = 0;

  if (row_num_ != other.get_row_num() ||
      column_num_ != other.get_column_num())
  {
    TBSYS_LOG(ERROR, "row num or column num is not equal, this[%d,%d], other[%d,%d]",
        row_num_, column_num_, other.get_row_num(), other.get_column_num());
    cmp_val = -1;
  }
  else
  {
    for (int i = 0; i < row_num_ && 0 == cmp_val; ++i)
    {
      for (int j = 0; j < column_num_ && 0 == cmp_val; ++j)
      {
        char* this_val = array_[i][j];
        int this_type = type_array_[i][j];
        char* other_val = NULL;
        int other_type = 0;
        other.get_value(i, j, other_val, other_type);

        if (this_type == FLOAT_TYPE || other_type == FLOAT_TYPE)
        {
          this_type = FLOAT_TYPE;
          other_type = FLOAT_TYPE;
        }

        if (this_type != other_type)
        {
          TBSYS_LOG(ERROR, "type is not equal, row_idx=%d, col_idx=%d, this_type=%d, other_type=%d", i, j, this_type, other_type);
        }
        else
        {
          switch (this_type)
          {
            case INT_TYPE:
            case VARCHAR_TYPE:
              if (strcmp(this_val, other_val))
              {
                TBSYS_LOG(ERROR, "value is not equal, row_idx=%d, col_idx=%d, this=%s, other=%s, len1=%d, len2=%d",
                    i, j, this_val, other_val, strlen(this_val), strlen(other_val));
                cmp_val = -1;
              }
              break;
            case FLOAT_TYPE:
              {
                double this_double_val = atof(this_val);
                double other_double_val = atof(other_val);
                if ((this_double_val - other_double_val) > EPS
                    || (this_double_val - other_double_val) < -EPS)
                {
                  TBSYS_LOG(ERROR, "value is not equal, row_idx=%d, col_idx=%d, this=%s(%f), other=%s(%f), len1=%d, len2=%d",
                      i, j, this_val, this_double_val, other_val, other_double_val, strlen(this_val), strlen(other_val));
                  cmp_val = -1;
                }
                else
                {
                  TBSYS_LOG(INFO, "double value is equal, row_idx=%d, col_idx=%d, this=%s, other=%s, len1=%d, len2=%d",
                      i, j, this_val, other_val, strlen(this_val), strlen(other_val));
                }
              }
              break;
            case DATETIME_TYPE:
              TBSYS_LOG(ERROR, "datetime type is not supported");
              cmp_val = -1;
              break;
            case INVALID_TYPE:
              TBSYS_LOG(ERROR, "invalid type");
              cmp_val = -1;
              break;
            default:
              TBSYS_LOG(ERROR, "invalid type");
              cmp_val = -1;
              break;
          }
        }
      }
    }
  }

  return cmp_val;
}


