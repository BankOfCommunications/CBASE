/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 *
 * Version: 0.1: sqltest_pattern.h,v 0.1 2012/02/27 11:25:52 chuanhui Exp $
 *
 * Authors:
 *   chuanhui <rizhao.ych@taobao.com>
 *     - some work details if you want
 *
 */
#ifndef __OCEANBASE_SQLTEST_PATTERN_H__
#define __OCEANBASE_SQLTEST_PATTERN_H__

#include "util.h"
#include "common/ob_define.h"

class SqlTestPattern
{
  public:
    SqlTestPattern();
    ~SqlTestPattern();

    int init(char* pattern_file);

  public:
    int get_next_pattern(char* pattern);
    int dump();

  private:
    int read_patterns();
    bool is_empty(const char* line);

    static const int MAX_PATTERN_NUM = 10240;
    static const int MAX_LINE_NUM = 2048;

  private:
    char file_[256];
    FILE* fd_;
    int num_patterns_;
    char ** patterns_;
};

#endif //__SQLTEST_PATTERN_H__

