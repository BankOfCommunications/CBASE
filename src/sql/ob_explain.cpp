/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_explain.cpp
 *
 * Authors:
 *   Guibin Du <tianguan.dgb@taobao.com>
 *
 */
#include "ob_explain.h"
#include "common/utility.h"

using namespace oceanbase::sql;
using namespace oceanbase::common;

ObExplain::ObExplain()
  : verbose_(false), been_read_(false)
{
}

ObExplain::ObExplain(bool verbose)
  : verbose_(verbose)
{
}

ObExplain::~ObExplain()
{
}

void ObExplain::reset()
{
  verbose_ = false;
  been_read_ = false;
  row_desc_.reset();
  str_buf_.reset();
}

void ObExplain::reuse()
{
  verbose_ = false;
  been_read_ = false;
  row_desc_.reset();
  str_buf_.reuse();
}

int ObExplain::open()
{
  int ret = OB_SUCCESS;
  // explain operator does not need to open its child
  if (child_op_ == NULL)
  {
    TBSYS_LOG(WARN, "explain operator must have a child operator");
    ret = OB_ERR_GEN_PLAN;
  }
  // No one will real use this row_desc, just for setting row value
  else if (OB_SUCCESS != (ret = row_desc_.add_column_desc(OB_INVALID_ID, OB_APP_MIN_COLUMN_ID)))
  {
    TBSYS_LOG(WARN, "failed to construct row desc, err=%d", ret);
  }
  else
  {
    row_.set_row_desc(row_desc_);
  }
  return ret;
}

int ObExplain::close()
{
  row_desc_.reset();
  been_read_ = false;
  return OB_SUCCESS;
}

int ObExplain::get_next_row(const common::ObRow *&row)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(been_read_))
  {
    ret = OB_ITER_END;
  }
  else
  {
    char buf[OB_MAX_LOG_BUFFER_SIZE];
    int64_t pos = 0;
    if (verbose_)
    {
      // add logical plan to output
      // now logical plan can not add to buf, we will do it later
      // FIX ME
      databuff_printf(buf, OB_MAX_LOG_BUFFER_SIZE, pos, "\n");
      pos += strlen("\n");
    }
    pos += to_string(buf + pos, OB_MAX_LOG_BUFFER_SIZE - pos);
    ObString str(static_cast<int32_t>(pos), static_cast<int32_t>(pos), buf);
    ObString stored_str;
    if ((ret = str_buf_.write_string(str, &stored_str)) != OB_SUCCESS)
    {
      TBSYS_LOG(WARN, "failed to make space for explain string, err=%d", ret);
    }
    else
    {
      ObObj explain_string;
      explain_string.set_varchar(stored_str);
      if ((ret = row_.set_cell(OB_INVALID_ID, OB_APP_MIN_COLUMN_ID, explain_string)) != OB_SUCCESS)
      {
        TBSYS_LOG(WARN, "set cell fail:ret[%d]", ret);
      }
      else
      {
        been_read_ = true;
        row = &row_;
      }
    }
  }

  return ret;
}

int64_t ObExplain::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  databuff_printf(buf, buf_len, pos, "Explain(%s)\n", verbose_ ? "Verbose" : "");
  if (NULL != child_op_)
  {
    pos += child_op_->to_string(buf + pos, buf_len - pos);
  }
  return pos;
}


namespace oceanbase{
  namespace sql{
    REGISTER_PHY_OPERATOR(ObExplain, PHY_EXPLAIN);
  }
}
