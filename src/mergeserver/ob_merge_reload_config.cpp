/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Time-stamp: <2013-04-23 17:33:12 fufeng.syd>
 * Version: $Id$
 * Filename: ob_merge_reload_config.cpp
 *
 * Authors:
 *   Yudi Shi <fufeng.syd@alipay.com>
 *
 */

#include "ob_merge_server.h"
#include "ob_merge_reload_config.h"

using namespace oceanbase;
using namespace oceanbase::mergeserver;
using namespace oceanbase::common;

ObMergeReloadConfig::ObMergeReloadConfig()
  : merge_server_(NULL)
{

}

ObMergeReloadConfig::~ObMergeReloadConfig()
{

}

void ObMergeReloadConfig::set_merge_server(ObMergeServer& merge_server)
{
    merge_server_ = &merge_server;
}

int ObMergeReloadConfig::operator ()()
{
  int ret = OB_SUCCESS;
  if (NULL == merge_server_)
  {
    TBSYS_LOG(WARN, "NULL merge server.");
    ret = OB_NOT_INIT;
  }
  else
  {
    ret = merge_server_->reload_config();
  }
  return ret;
}
