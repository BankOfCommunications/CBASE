/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 * 
 * Version: $Id$
 *
 * ob_schema_service_ups_provider.cpp
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#include "ob_schema_service_ups_provider.h"
using namespace oceanbase::rootserver;
using namespace oceanbase::common;

ObSchemaServiceUpsProvider::ObSchemaServiceUpsProvider(const ObUpsManager &ups_manager)
  :ups_manager_(ups_manager)
{
}

ObSchemaServiceUpsProvider::~ObSchemaServiceUpsProvider()
{
}


int ObSchemaServiceUpsProvider::get_ups(common::ObServer &ups)
{
  int ret = OB_SUCCESS;
  ObUps ups_master;
  if (OB_SUCCESS != (ret = ups_manager_.get_ups_master(ups_master)))
  {
    TBSYS_LOG(WARN, "failed to get ups for schema service, err=%d", ret);
  }
  else
  {
    ups = ups_master.addr_;
  }
  return ret;
}

