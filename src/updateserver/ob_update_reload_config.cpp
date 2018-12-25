/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Time-stamp: <2013-04-23 21:50:00 fufeng.syd>
 * Version: $Id$
 * Filename: ob_update_reload_config.cpp
 *
 * Authors:
 *   Yudi Shi <fufeng.syd@alipay.com>
 *
 */

#include "ob_update_server.h"
#include "ob_update_reload_config.h"

using namespace oceanbase;
using namespace oceanbase::updateserver;
using namespace oceanbase::common;

ObUpdateReloadConfig::ObUpdateReloadConfig()
  : update_server_(NULL)
{

}

ObUpdateReloadConfig::~ObUpdateReloadConfig()
{

}

void ObUpdateReloadConfig::set_update_server(ObUpdateServer &update_server)
{
  update_server_ = &update_server;
}

int ObUpdateReloadConfig::operator ()()
{
  int ret = OB_SUCCESS;
  if (NULL == update_server_)
  {
    TBSYS_LOG(WARN, "NULL update server, not apply configuration.");
    ret = OB_NOT_INIT;
  }
  else
  {
    update_server_->apply_conf();
  }
  return ret;
}
