/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_version.h
 *
 * Authors:
 *   Liangjie Li <liangjie.li@alipay.com>
 *
 */
#ifndef OCEANBASE_COMMON_VERSION_H_
#define OCEANBASE_COMMON_VERSION_H_

#include <stdint.h>

const char* svn_version();
const char* build_date();
const char* build_time();
const char* build_flags();

void get_package_and_svn(char* server_version, int64_t buf_len);

#endif
