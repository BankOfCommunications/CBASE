/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 * 
 * Version: $Id$
 *
 * ob_old_root_table_schema.h
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#ifndef _OB_OLD_ROOT_TABLE_SCHEMA_H
#define _OB_OLD_ROOT_TABLE_SCHEMA_H 1
#include "common/ob_string.h"
namespace oceanbase
{
  namespace common
  {
    namespace old_root_table_columns
    {
      static const int64_t MAX_REPLICA_COUNT = 3;
      static const int64_t IPV6_PART_COUNT = 4;
      extern common::ObString COL_OCCUPY_SIZE;
      extern common::ObString COL_RECORD_COUNT;
      extern common::ObString COL_PORT[MAX_REPLICA_COUNT];
      extern common::ObString COL_MS_PORT[MAX_REPLICA_COUNT];
      extern common::ObString COL_IPV6[MAX_REPLICA_COUNT][IPV6_PART_COUNT];
      extern common::ObString COL_IPV4[MAX_REPLICA_COUNT];
      extern common::ObString COL_TABLET_VERSION[MAX_REPLICA_COUNT];
    } // end namespace old_root_table_columns
  } // end namespace common
} // end namespace oceanbase

#endif /* _OB_OLD_ROOT_TABLE_SCHEMA_H */

