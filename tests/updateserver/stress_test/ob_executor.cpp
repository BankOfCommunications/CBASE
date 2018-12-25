/**
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * Authors:
 *   yanran <yanran.hfs@taobao.com>
 *     - some work details if you want
 */

#include "ob_executor.h"

#include "common/ob_define.h"
#include "common/ob_scanner.h"
#include "ob_test_bomb.h"

using namespace oceanbase::common;
using namespace oceanbase::test;

ObExecutor::ObExecutor()
{
}

ObExecutor::~ObExecutor()
{
  scan_client_.destroy();
  apply_client_.destroy();
}

int ObExecutor::init(common::ObServer& ms, common::ObServer& ups)
{
  int ret = OB_SUCCESS;

  ret = scan_client_.init(ms);
  ret = apply_client_.init(ups);

  return ret;
}

int ObExecutor::exec(const ObTestBomb &bomb)
{
  int ret = OB_SUCCESS;

  switch (bomb.get_type())
  {
    case UPDATE_BOMB:
      ret = exec_update(bomb);
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "exec_update error, ret=%d", ret);
      }
      break;

    case SCAN_BOMB:
      ret = exec_scan(bomb);
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "exec_scan error, ret=%d", ret);
      }
      break;

    default:
      break;
  }

  return ret;
}

int ObExecutor::exec_update(const ObTestBomb &bomb)
{
  int ret = OB_SUCCESS;

  ret = apply_client_.ups_apply(*bomb.get_mutator(), 1000);

  return ret;
}

int ObExecutor::exec_scan(const ObTestBomb &bomb)
{
  int ret = OB_SUCCESS;

  ObScanner scanner;

  ret = scan_client_.ups_scan(*bomb.get_scan_param(), scanner, 1000);
  if (OB_SUCCESS != ret)
  {
    TBSYS_LOG(WARN, "ups_scan error, ret=%d", ret);
  }
  else
  {
    ObCellInfo *cell = NULL;
    bool row_changed = false;
    bool first_time = true;
    int64_t sum = 0;
    ObString last_row;
    int64_t value;
    int cell_no = 0;

    const int key_len = 16;
    char start_key[key_len];
    char end_key[key_len];
    while (OB_SUCCESS == ret && OB_SUCCESS == scanner.next_cell())
    {
      ret = scanner.get_cell(&cell, &row_changed);
      if (ObIntType == cell->value_.get_type())
      {
        int64_t value = 0;
        cell->value_.get_int(value);
//        TBSYS_LOG(DEBUG, "cell column_name=%.*s type=%d value=%ld",
//            cell->column_name_.length(), cell->column_name_.ptr(),
//            cell->value_.get_type(), value);
      }
      else
      {
//        TBSYS_LOG(DEBUG, "cell column_name=%.*s type=%d",
//            cell->column_name_.length(), cell->column_name_.ptr(),
//            cell->value_.get_type());
      }
      if (ObExtendType == cell->value_.get_type())
      {
        int ext = cell->value_.get_ext();
        TBSYS_LOG(DEBUG, "cell ext=%d", ext);
      }
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "ObScanner get_cell error, ret=%d", ret);
      }
      else
      {
        if (row_changed)
        {
          if (first_time)
          {
            memcpy(start_key, cell->row_key_.ptr(), key_len);
            first_time = false;
          }
          else
          {
            int64_t rk_p1;
            int64_t rk_p2;
            int64_t pos = 0;
            serialization::decode_i64(last_row.ptr(), last_row.length(), pos, &rk_p1);
            serialization::decode_i64(last_row.ptr(), last_row.length(), pos, &rk_p2);

            if (rk_p2 != sum)
            {
              const int str_len = 1024;
              char str[str_len];
              int str_pos = 0;
              for (int i = 0; i < key_len; i++)
              {
                str_pos += snprintf(str + str_pos, str_len - str_pos, "%02hhx", last_row.ptr()[i]);
              }
              TBSYS_LOG(ERROR, "INCORRECT ROW[%s], rk_p1=%ld rk_p2=%ld sum=%ld", str, rk_p1, rk_p2, sum);
              ret = OB_ERROR;
            }

            sum = 0;
          }
        }

        last_row = cell->row_key_;
        ret = cell->value_.get_int(value);
        if (OB_SUCCESS != ret)
        {
          if (ObNullType != cell->value_.get_type())
          {
            TBSYS_LOG(WARN, "cell->value_ get_int error, ret=%d", ret);
          }
          else
          {
            ret = OB_SUCCESS;
          }
        }
        else
        {
          cell_no++;
          sum += value;
        }
      }
    }

    if (first_time)
    {
      TBSYS_LOG(ERROR, "NO DATA"); ret = OB_ERROR;
    }
    else
    {
      int64_t rk_p1;
      int64_t rk_p2;
      int64_t pos = 0;
      serialization::decode_i64(last_row.ptr(), last_row.length(), pos, &rk_p1);
      serialization::decode_i64(last_row.ptr(), last_row.length(), pos, &rk_p2);

      if (rk_p2 != sum)
      {
        const int str_len = 1024;
        char str[str_len];
        int str_pos = 0;
        for (int i = 0; i < key_len; i++)
        {
          str_pos += snprintf(str + str_pos, str_len - str_pos, "%02hhx", last_row.ptr()[i]);
        }
        TBSYS_LOG(ERROR, "INCORRECT ROW[%s], rk_p1=%ld rk_p2=%ld sum=%ld", str, rk_p1, rk_p2, sum);
        ret = OB_ERROR;
      }
      else
      {
        memcpy(end_key, last_row.ptr(), key_len);

        const int str_len = 1024;
        char str[str_len];
        int str_pos = 0;
        for (int i = 0; i < key_len; i++)
        {
          str_pos += snprintf(str + str_pos, str_len - str_pos, "%02hhx", start_key[i]);
        }
        str_pos += snprintf(str + str_pos, str_len - str_pos, ", ");
        for (int i = 0; i < key_len; i++)
        {
          str_pos += snprintf(str + str_pos, str_len - str_pos, "%02hhx", end_key[i]);
        }

        TBSYS_LOG(DEBUG, "SCAN RES [%s] #%d#", str, cell_no);
      }
    }

  }

  return ret;
}

