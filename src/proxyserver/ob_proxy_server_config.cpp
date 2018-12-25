/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Time-stamp: <2013-04-23 12:59:03 fufeng.syd>
 * Version: $Id$
 * Filename: ob_proxy_server_config.cpp
 *
 * Authors:
 *   Yudi Shi <fufeng.syd@taobao.com>
 *
 */

#include "ob_proxy_server_config.h"
#include "common/ob_system_config_key.h"

using namespace oceanbase::proxyserver;

const char* ObProxyServerConfig::YUNTI_PROXY = "hadoop";
const char* ObProxyServerConfig::PUBLIC = "public";
const char* ObProxyServerConfig::DEFAULT_DEVNAME = "bond0";

ObProxyServerConfig::ObProxyServerConfig() : devname_(DEFAULT_DEVNAME)
{
}

ObProxyServerConfig::~ObProxyServerConfig()
{
}

