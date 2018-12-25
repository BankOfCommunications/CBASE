#ifndef OCEANBASE_OBSQL_COMMON_FUNC_H
#define OCEANBASE_OBSQL_COMMON_FUNC_H
/*
 *  (C) 2007-2010 Taobao Inc.
 *  
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License version 2 as
 *  published by the Free Software Foundation.
 *
 *         common_func.h is for what ...
 *
 *  Version: $Id: common_func.h 
 *
 *  Authors:
 *     
 *        - some work details if you want
 */

#include "common/ob_array_helper.h"
//using namespace oceanbase::common;

typedef enum //rowKeySubType
{
  ROWKEY_MIN = -1,
  ROWKEY_INT8_T,
  ROWKEY_INT16_T,
  ROWKEY_INT32_T,
  ROWKEY_INT64_T,
  ROWKEY_FLOAT,
  ROWKEY_DOUBLE,
  ROWKEY_VARCHAR,
  ROWKEY_DATETIME,
  ROWKEY_PRECISEDATETIME,
  ROWKEY_CEATETIME,
  ROWKEY_MODIFYTIME,
  ROWKEY_MAX,
} RowKeySubType;

//typedef enum rowKeySubType RowKeySubType;

char* trim(char* str);
char* get_token(char *&cmdstr);
char* putback_token(char *token, char *cmdstr);
char* string_split_by_str(char *&str1, 
                           char *str2, 
                           bool is_case_sensitive,
                           bool whole_words_only);
char* string_split_by_ch(char *&str, const int ch);
int parse_number_range(const char *number_string, 
                       oceanbase::common::ObArrayHelper<int32_t> &disk_array);
char* duplicate_str(const char *str);
int64_t atoi64(const char *str);
char* strlwr(char *str);
char* strupr(char *str);

#endif

