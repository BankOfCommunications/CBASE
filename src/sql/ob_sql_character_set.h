/*
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * ob_sql_character_set.h is for what ...
 *
 * Version: ***: ob_sql_character_set.h  Wed Apr 10 19:23:36 2013 fangji.hcm Exp $
 *
 * Authors:
 *   Author fangji
 *   Email: fangji.hcm@alipay.com
 *     -some work detail if you want
 *
 */
#ifndef OB_SQL_CHARACTER_SET_H_
#define OB_SQL_CHARACTER_SET_H_
#include <stdint.h>
#include "common/ob_define.h"
#include "common/ob_string.h"
namespace oceanbase
{
  namespace sql
  {
    extern int32_t get_char_number_from_name(const char* name);
    extern int32_t get_char_number_from_name(const common::ObString& name);
    extern const char* get_char_name_from_number(const int32_t number);
  }
}
#endif
