/*
* (C) 2007-2011 Alibaba Group Holding Limited.
*
* This program is free software; you can redistribute it and/or modify
* it under the terms of the GNU General Public License version 2 as
* published by the Free Software Foundation.
*
*
* Version: $Id
*
* Authors:
*   yiming.czw <yiming.czw@taobao.com>
*      - initial release
*
*/
#ifndef SQL_OB_INIT_H_
#define SQL_OB_INIT_H_


#include <mysql/mysql.h>

#include <gtest/gtest.h>

#include <stdio.h>
#include <stdlib.h>

#define HOST "10.232.23.18"
#define PORT  25049

class ObWithoutInit : public testing::Test
{
  protected:
  static void SetUpTestCase()
  {
  }
  static void TearDownTestCase()
  {
  }

  public:
  virtual void SetUp()
  {
  }
  virtual void TearDown()
  {
  }

};
#endif
