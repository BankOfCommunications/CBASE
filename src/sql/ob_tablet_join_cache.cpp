/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_tablet_join_cache.cpp 
 *
 * Authors:
 *   Junquan Chen <jianming.cjq@alipay.com>
 *
 */

#include "ob_tablet_join_cache.h"

using namespace oceanbase;
using namespace sql;
using namespace common;

ObTabletJoinCacheKey::ObTabletJoinCacheKey()
  : table_id_(OB_INVALID_ID)
{
}

uint32_t ObTabletJoinCacheKey::murmurhash2(const uint32_t hash) const
{
  uint32_t ret = 0;
  ret = rowkey_.murmurhash2(hash);
  ret = ::murmurhash2(&table_id_, sizeof(table_id_), ret);
  return ret;
}

bool ObTabletJoinCacheKey::operator ==(const ObTabletJoinCacheKey &other) const
{
  return (table_id_ == other.table_id_) && (rowkey_ == other.rowkey_);
}

