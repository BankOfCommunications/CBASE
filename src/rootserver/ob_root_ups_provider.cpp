/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 * 
 * Version: $Id$
 *
 * ob_root_ups_provider.cpp
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#include "ob_root_ups_provider.h"
using namespace oceanbase::rootserver;
using namespace oceanbase::common;

ObRootUpsProvider::ObRootUpsProvider(const common::ObServer &ups)
  :ups_(ups)
{
}

ObRootUpsProvider::~ObRootUpsProvider()
{
}

int ObRootUpsProvider::get_ups(ObServer &ups)
{
  int ret = OB_SUCCESS;
  ups = ups_;
  return ret;
}


