/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 * 
 * Version: $Id$
 *
 * ob_old_root_table_schema.cpp
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#include "ob_old_root_table_schema.h"
using namespace oceanbase::common;

static const char* RT_OCCUPY_SIZE ="occupy_size";
static const char* RT_RECORD_COUNT ="record_count";

static const char* RT_1_PORT =   "1_port";
static const char* RT_1_MS_PORT =   "1_ms_port";
static const char* RT_1_IPV6_1 = "1_ipv6_1";
static const char* RT_1_IPV6_2 = "1_ipv6_2";
static const char* RT_1_IPV6_3 = "1_ipv6_3";
static const char* RT_1_IPV6_4 = "1_ipv6_4";
static const char* RT_1_IPV4   = "1_ipv4";
static const char* RT_1_TABLET_VERSION= "1_tablet_version";

static const char* RT_2_PORT =   "2_port";
static const char* RT_2_MS_PORT =   "2_ms_port";
static const char* RT_2_IPV6_1 = "2_ipv6_1";
static const char* RT_2_IPV6_2 = "2_ipv6_2";
static const char* RT_2_IPV6_3 = "2_ipv6_3";
static const char* RT_2_IPV6_4 = "2_ipv6_4";
static const char* RT_2_IPV4   = "2_ipv4";
static const char* RT_2_TABLET_VERSION= "2_tablet_version";

static const char* RT_3_PORT =   "3_port";
static const char* RT_3_MS_PORT =   "3_ms_port";
static const char* RT_3_IPV6_1 = "3_ipv6_1";
static const char* RT_3_IPV6_2 = "3_ipv6_2";
static const char* RT_3_IPV6_3 = "3_ipv6_3";
static const char* RT_3_IPV6_4 = "3_ipv6_4";
static const char* RT_3_IPV4   = "3_ipv4";
static const char* RT_3_TABLET_VERSION= "3_tablet_version";


ObString old_root_table_columns::COL_OCCUPY_SIZE(0, static_cast<int32_t>(strlen(RT_OCCUPY_SIZE)), const_cast<char*>(RT_OCCUPY_SIZE));
ObString old_root_table_columns::COL_RECORD_COUNT(0, static_cast<int32_t>(strlen(RT_RECORD_COUNT)), const_cast<char*>(RT_RECORD_COUNT));
ObString old_root_table_columns::COL_PORT[MAX_REPLICA_COUNT] = {
  ObString(0, static_cast<int32_t>(strlen(RT_1_PORT)), const_cast<char*>(RT_1_PORT)),
  ObString(0, static_cast<int32_t>(strlen(RT_2_PORT)), const_cast<char*>(RT_2_PORT)),
  ObString(0, static_cast<int32_t>(strlen(RT_3_PORT)), const_cast<char*>(RT_3_PORT))};
ObString old_root_table_columns::COL_MS_PORT[MAX_REPLICA_COUNT] = {
  ObString(0, static_cast<int32_t>(strlen(RT_1_MS_PORT)), const_cast<char*>(RT_1_MS_PORT)),
  ObString(0, static_cast<int32_t>(strlen(RT_2_MS_PORT)), const_cast<char*>(RT_2_MS_PORT)),
  ObString(0, static_cast<int32_t>(strlen(RT_3_MS_PORT)), const_cast<char*>(RT_3_MS_PORT))};
ObString old_root_table_columns::COL_IPV6[MAX_REPLICA_COUNT][IPV6_PART_COUNT] = {
  {
    ObString(0, static_cast<int32_t>(strlen(RT_1_IPV6_1)), const_cast<char*>(RT_1_IPV6_1)),
    ObString(0, static_cast<int32_t>(strlen(RT_1_IPV6_2)), const_cast<char*>(RT_1_IPV6_2)),
    ObString(0, static_cast<int32_t>(strlen(RT_1_IPV6_3)), const_cast<char*>(RT_1_IPV6_3)),
    ObString(0, static_cast<int32_t>(strlen(RT_1_IPV6_4)), const_cast<char*>(RT_1_IPV6_4))
  },
  {
    ObString(0, static_cast<int32_t>(strlen(RT_2_IPV6_1)), const_cast<char*>(RT_2_IPV6_1)),
    ObString(0, static_cast<int32_t>(strlen(RT_2_IPV6_2)), const_cast<char*>(RT_2_IPV6_2)),
    ObString(0, static_cast<int32_t>(strlen(RT_2_IPV6_3)), const_cast<char*>(RT_2_IPV6_3)),
    ObString(0, static_cast<int32_t>(strlen(RT_2_IPV6_4)), const_cast<char*>(RT_2_IPV6_4))
  },
  {
    ObString(0, static_cast<int32_t>(strlen(RT_3_IPV6_1)), const_cast<char*>(RT_3_IPV6_1)),
    ObString(0, static_cast<int32_t>(strlen(RT_3_IPV6_2)), const_cast<char*>(RT_3_IPV6_2)),
    ObString(0, static_cast<int32_t>(strlen(RT_3_IPV6_3)), const_cast<char*>(RT_3_IPV6_3)),
    ObString(0, static_cast<int32_t>(strlen(RT_3_IPV6_4)), const_cast<char*>(RT_3_IPV6_4))
  }
};
  
ObString old_root_table_columns::COL_IPV4[MAX_REPLICA_COUNT] = { 
  ObString(0, static_cast<int32_t>(strlen(RT_1_IPV4)), const_cast<char*>(RT_1_IPV4)),
  ObString(0, static_cast<int32_t>(strlen(RT_2_IPV4)), const_cast<char*>(RT_2_IPV4)),
  ObString(0, static_cast<int32_t>(strlen(RT_3_IPV4)), const_cast<char*>(RT_3_IPV4))};
ObString old_root_table_columns::COL_TABLET_VERSION[MAX_REPLICA_COUNT] = {
  ObString(0, static_cast<int32_t>(strlen(RT_1_TABLET_VERSION)), const_cast<char*>(RT_1_TABLET_VERSION)),
  ObString(0, static_cast<int32_t>(strlen(RT_2_TABLET_VERSION)), const_cast<char*>(RT_2_TABLET_VERSION)),
  ObString(0, static_cast<int32_t>(strlen(RT_3_TABLET_VERSION)), const_cast<char*>(RT_3_TABLET_VERSION))};
