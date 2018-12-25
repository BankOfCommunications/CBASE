/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_sys_params_mgr.cpp
 *
 * Authors:
 *   Guibin Du <tianguan.dgb@taobao.com>
 *
 */
#include "ob_sys_params_mgr.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

ObSysParamsMgr::ObSysParamsMgr()
{
  // default values are set here
  sort_mem_size_limit_ = 500000;  //500M
  group_mem_size_limit_ = 500000;  //500M
}

ObSysParamsMgr::~ObSysParamsMgr()
{
}

int ObSysParamsMgr::parse_from_file(const char* file_name)
{
  // Fix me, parse system parameters from .ini file
  UNUSED(file_name);
  int ret = OB_SUCCESS;

  return ret;
}

