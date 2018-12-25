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

#include "ob_scan_gen.h"

#include "ob_schema_proxy.h"
#include "ob_row_dis.h"

using namespace oceanbase::common;
using namespace oceanbase::test;

ObScanGen::ObScanGen(int64_t max_line_no)
{
  ran_.initt();
  max_line_no_ = max_line_no;
}

ObScanGen::~ObScanGen()
{
}

int ObScanGen::gen(ObTestBomb &bomb)
{
  int ret = OB_SUCCESS;

  sp_.reset();

  ObSchemaProxy &schema = ObSchemaProxy::get_instance();
  ObRowDis &row_dis = ObRowDis::get_instance();

  ObRange range;

  int64_t rk_p1 = row_dis.get_row_p1(ran_.randt(row_dis.get_row_p1_num()));

  int64_t rk_p2_start_index = ran_.randt(row_dis.get_row_p2_num());
  int64_t rk_p2_start = row_dis.get_row_p2(rk_p2_start_index);
  int64_t rk_p2_interval = ran_.randt(0, std::min(max_line_no_, row_dis.get_row_p2_num() - rk_p2_start_index));
  int64_t rk_p2_end = row_dis.get_row_p2(rk_p2_start_index + rk_p2_interval);

  const int rs_len = 2 * sizeof(int64_t);
  char rs_start[rs_len];
  char rs_end[rs_len];
  int64_t rs_pos = 0;
  ret = serialization::encode_i64(rs_start, rs_len, rs_pos, rk_p1);
  if (OB_SUCCESS != ret)
  {
    TBSYS_LOG(WARN, "encode_i64 rk_p1 error, ret=%d rk_p1=%ld", ret, rk_p1);
  }
  else
  {
    ret = serialization::encode_i64(rs_start, rs_len, rs_pos, rk_p2_start);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "encode_i64 rk_p2_start error, ret=%d rk_p2_start=%ld", ret, rk_p2_start);
    }
  }

  if (OB_SUCCESS == ret)
  {
    rs_pos = 0;
    ret = serialization::encode_i64(rs_end, rs_len, rs_pos, rk_p1);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "encode_i64 rk_p1 error, ret=%d rk_p1=%ld", ret, rk_p1);
    }
    else
    {
      ret = serialization::encode_i64(rs_end, rs_len, rs_pos, rk_p2_end);
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "encode_i64 rk_p2_end error, ret=%d rk_p2_end=%ld", ret, rk_p2_end);
      }
    }
  }

  if (OB_SUCCESS == ret)
  {
    range.table_id_ = OB_INVALID_ID;
    string_buf_.write_string(ObString(rs_len, rs_len, rs_start), &(range.start_key_));
    string_buf_.write_string(ObString(rs_len, rs_len, rs_end), &(range.end_key_));
    range.border_flag_.set_inclusive_start();
    range.border_flag_.set_inclusive_end();

    sp_.set(OB_INVALID_ID, schema.get_table_name(), range);

    ObVersionRange vr;
    vr.start_version_ = 1;
    vr.end_version_ = 0;
    vr.border_flag_.set_inclusive_start();
    vr.border_flag_.set_max_value();
    sp_.set_version_range(vr);

    for (int i = 0; OB_SUCCESS == ret && i < schema.get_column_num(); i++)
    {
      ObString cn;
      ret = string_buf_.write_string(schema.get_column_name(i + 2), &cn);
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "ObStringBuf write_string error, ret=%d", ret);
      }
      else
      {
        ret = sp_.add_column(cn);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "ObScanParam add_column error, ret=%d", ret);
        }
      }
    }

    const int str_len = 1024;
    char str[str_len];
    int str_pos = 0;
    const int key_len = sizeof(int64_t) * 2;
    for (int i = 0; i < key_len; i++)
    {
      str_pos += snprintf(str + str_pos, str_len - str_pos, "%02hhx", rs_start[i]);
    }
    str_pos += snprintf(str + str_pos, str_len - str_pos, ", ");
    for (int i = 0; i < key_len; i++)
    {
      str_pos += snprintf(str + str_pos, str_len - str_pos, "%02hhx", rs_end[i]);
    }

    TBSYS_LOG(DEBUG, "SCAN GEN [%s]", str);

  }

  if (OB_SUCCESS == ret)
  {
    bomb.set_scan_param(&sp_);
  }

  return ret;
}

