/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_version.cpp
 *
 * Authors:
 *   Liangjie Li <liangjie.li@alipay.com>
 *
 */
#include <stdio.h>
#include "common/ob_version.h"
#include "tblog.h"

void get_package_and_svn(char* server_version, int64_t buf_len)
{
  const char* server_version_template = "%s_%s(%s %s)";
  snprintf(server_version, buf_len, server_version_template, PACKAGE_VERSION,
      svn_version(), build_date(), build_time());
}

