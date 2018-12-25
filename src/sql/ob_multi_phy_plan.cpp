/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_multi_phy_plan.cpp
 *
 * Authors:
 *   Guibin Du <tianguan.dgb@taobao.com>

 *
 */
#include "ob_multi_phy_plan.h"
#include "parse_malloc.h"
#include "common/utility.h"

using namespace oceanbase::sql;
using namespace oceanbase::common;

ObMultiPhyPlan::ObMultiPhyPlan()
{
}

ObMultiPhyPlan::~ObMultiPhyPlan()
{
  for(int32_t i = 0; i < size(); ++i)
  {
    //delete at(i);
    at(i)->~ObPhysicalPlan();
    parse_free(at(i));
  }
}

int64_t ObMultiPhyPlan::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  databuff_printf(buf, buf_len, pos, "MultiPhyPlan(plans=[\n");
  for (int32_t i = 0; i < size(); ++i)
  {
    int64_t pos2 = at(i)->to_string(buf + pos, buf_len - pos);
    pos += pos2;
    if (i != size() -1)
    {
      databuff_printf(buf, buf_len, pos, ",\n");
    }
  }
  return pos;
}
    

