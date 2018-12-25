/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_ms_request_result.cpp
 *
 * Authors:
 *   Junquan Chen <jianming.cjq@alipay.com>
 *
 */

#include "ob_ms_request_result.h"

using namespace oceanbase;
using namespace mergeserver;
using namespace common;

ObMsRequestResult::ObMsRequestResult() : current_(0)
{
}

ObMsRequestResult::~ObMsRequestResult()
{
}

void ObMsRequestResult::clear()
{
  for (int32_t i = 0; i < scanner_list_.size(); i ++)
  {
    scanner_list_.at(i)->clear();
  }
  current_ = 0;
  scanner_list_.clear();
}

ObNewScanner *ObMsRequestResult::get_last_scanner()
{
  ObNewScanner *ret = NULL;
  if (scanner_list_.size() > 0)
  {
    ret = scanner_list_.at(scanner_list_.size() - 1);
  }
  return ret;
}

bool ObMsRequestResult::is_finish() const
{
  bool is_fullfilled = false;
  int64_t fullfilled_row_num = 0;
  int err = OB_SUCCESS;
  if (scanner_list_.size() > 0)
  {
    if (OB_SUCCESS != (err = scanner_list_.at(scanner_list_.size() - 1)->get_is_req_fullfilled(is_fullfilled, fullfilled_row_num)))
    {
      TBSYS_LOG(WARN, "fail to get is req fullfilled:err[%d]", err);
    }
  }
  return is_fullfilled;
}

int64_t ObMsRequestResult::get_row_num() const
{
  int64_t ret = 0;
  for (int32_t i = 0; i < scanner_list_.size(); i ++)
  {
    ret += scanner_list_.at(i)->get_row_num();
  }
  return ret;
}

int64_t ObMsRequestResult::get_used_mem_size() const
{
  int64_t ret = 0;
  for (int32_t i = 0; i < scanner_list_.size(); i ++)
  {
    ret += scanner_list_.at(i)->get_used_mem_size();
  }
  return ret;
}

int ObMsRequestResult::add_scanner(ObNewScanner *scanner)
{
  return scanner_list_.push_back(scanner);
}

int ObMsRequestResult::get_next_row(ObRow &row)
{
  int ret = OB_SUCCESS;
  if (current_ >= scanner_list_.size())
  {
    ret = OB_ITER_END;
  }

  while (OB_SUCCESS == ret)
  {
    ret = scanner_list_.at(current_)->get_next_row(row);
    if (OB_ITER_END == ret)
    {
      current_ ++;
      if (current_ >= scanner_list_.size())
      {
        break;
      }
      else
      {
        ret = OB_SUCCESS;
      }
    }
    else if (OB_SUCCESS == ret)
    {
      break;
    }
    else
    {
      TBSYS_LOG(WARN, "fail to get next row");
    }
  }
  return ret;
}

