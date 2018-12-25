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

#include "ob_update_gen.h"

#include <assert.h>

#include "common/ob_define.h"
#include "ob_schema_proxy.h"
#include "ob_row_dis.h"

using namespace oceanbase::common;
using namespace oceanbase::test;

ObUpdateGen::ObUpdateGen(int64_t max_cell_no)
{
  ran_.initt();
  max_cell_no_ = max_cell_no;
}

ObUpdateGen::~ObUpdateGen()
{
}

int ObUpdateGen::gen(ObTestBomb &bomb)
{
  int ret = OB_SUCCESS;

  ObSchemaProxy &schema = ObSchemaProxy::get_instance();
  ObRowDis &row_dis = ObRowDis::get_instance();

  int column_num = schema.get_column_num();
  int max_N = (int)max_cell_no_;

  int N = ran_.randt(2, max_N);  // number of cells

  int line_num_min = N / column_num + 1;
  int line_num_max = N / 2;
  int line_num = ObRandom::rand(line_num_min, line_num_max);

  ret = mut_.reset();
  if (OB_SUCCESS != ret)
  {
    TBSYS_LOG(WARN, "ObMutator reset error, ret=%d", ret);
  }
  else
  {
    int n_sum = 0;
    for (int i = 0; OB_SUCCESS == ret && i < line_num; i++)
    {
      int64_t row_key_part1 = row_dis.get_row_p1(ran_.randt(row_dis.get_row_p1_num()));
      int64_t row_key_part2 = row_dis.get_row_p2(ran_.randt(row_dis.get_row_p2_num()));
      const int row_key_len = 2 * sizeof(int64_t);
      char row_key_buf[row_key_len];
      int64_t pos = 0;

      ret = serialization::encode_i64(row_key_buf, row_key_len, pos, row_key_part1);
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "encode_i64 row_key_part1 error, ret=%d row_key_part1=%ld", ret, row_key_part1);
      }
      else
      {
        ret = serialization::encode_i64(row_key_buf, row_key_len, pos, row_key_part2);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "encode_i64 row_key_part2 error, ret=%d row_key_part2=%ld", ret, row_key_part1);
        }
      }

      if (OB_SUCCESS == ret)
      {
        int n = ran_.randt(2, std::min(column_num, N - n_sum - 2 * (line_num - i - 1)));
        ObString row_key;
        row_key.assign(row_key_buf, row_key_len);

        ret = gen_line(n, row_key, 0, ran_, mut_);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "gen_line error, ret=%d", ret);
        }

        n_sum += n;
      }
    }

    bomb.set_mutator(&mut_);
  }

  return ret;
}

int ObUpdateGen::gen_line(int n, ObString& row_key, int64_t sum, ObRandom& ran, common::ObMutator& mut)
{
  int ret = OB_SUCCESS;

  ObSchemaProxy &schema = ObSchemaProxy::get_instance();
  const int log_buf_len = 2048;
  char log_buf[log_buf_len];
  log_buf[0] = '(';
  int64_t log_buf_pos = 1;

  int64_t line_sum = 0;
  for (int j = 0; j < n; j++)
  {
    int column_id = ran.randt(schema.get_column_num()) + 1;

    int rem = n - j - 1;
    int64_t range_begin = std::max(BOMB_INT_MIN, sum + rem * BOMB_INT_MIN - line_sum);
    int64_t range_end = std::min(BOMB_INT_MAX, sum + rem * BOMB_INT_MAX - line_sum);
    int64_t value = ran.rand64t(range_begin, range_end);

    ObObj v;
    v.set_int(value, true);

    ret = mut.update(schema.get_table_name(), row_key, schema.get_column_name(column_id), v);
    log_buf_pos += snprintf(log_buf + log_buf_pos, log_buf_len - log_buf_pos, "C%d=>%ld ", column_id, value);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "ObMutator update error, ret=%d", ret);
    }

    line_sum += value;
  }
  if (line_sum != sum)
  {
    ret = OB_ERROR;
    TBSYS_LOG(WARN, "line_sum is not equal sum: line_sum=%ld sum=%ld", line_sum, sum);
  }
  else
  {
    if (log_buf_pos > 1)
    {
      log_buf[log_buf_pos - 1] = ')';
    }
    else
    {
      log_buf[1] = ')';
      log_buf_pos = 2;
    }

    const int rs_len = 1024;
    char rs[rs_len];
    rs[0] = '0'; rs[1] = 'x';
    int rs_pos = 2;
    for (int i = 0; i < row_key.length(); i++)
    {
      rs_pos += snprintf(rs + rs_pos, rs_len - rs_pos, "%02hhx", row_key.ptr()[i]);
    }
    TBSYS_LOG(DEBUG, "UPDATE %s #%d#%s", rs, n, log_buf);
  }

  return ret;
}
