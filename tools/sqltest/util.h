/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 *
 * Version: 0.1: util.h,v 0.1 2012/02/22 12:30:30 chuanhui Exp $
 *
 * Authors:
 *   chuanhui <rizhao.ych@taobao.com>
 *     - some work details if you want
 *
 */
#ifndef __OCEANBASE_UTIL_H__
#define __OCEANBASE_UTIL_H__
#include "common/ob_define.h"
#include "tbnet.h"
#include "tbsys.h"

using namespace oceanbase::common;
using namespace tbsys;
using namespace tbnet;

enum SqlTestError
{
  SQLTEST_UPDATING = 1,
  SQLTEST_READING = 2,
  MYSQL_UPDATE_TIMEOUT,
  OB_UPDATE_TIMEOUT,
  SQLTEST_OB_NOT_EXIST,
  MYSQL_NOT_EXIST,
  READ_WRITE_CONFLICT, // read write conflict error
};

class Array
{
  public:
    enum ElemType
    {
      INVALID_TYPE = 0,
      INT_TYPE,
      VARCHAR_TYPE,
      FLOAT_TYPE,
      DATETIME_TYPE,
    };
  public:
    Array();
    ~Array();
    int init(int row_num, int column_num);
    int clear();

  public:
    int get_row_num() const;
    int get_column_num() const;
    bool is_empty() const;

    int set_value(int row_idx, int col_idx, const char* value, int type);
    int get_value(int row_idx, int col_idx, char*& value, int& type);

    void print() const;

    int compare(Array& other) const;

  private:
    static const int MAX_COLUMN_NUM = 32;
    int row_num_;
    int column_num_;
    char*** array_; // 2d array, each element is a char*
    int** type_array_; // 2d array, each element represents an element type
};


#endif //__UTIL_H__

