/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_tablet_fuse.h
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#ifndef _OB_TABLET_FUSE_H
#define _OB_TABLET_FUSE_H 1

#include "ob_rowkey_phy_operator.h"
#include "common/ob_ups_row.h"
#include "common/ob_range.h"
#include "ob_sstable_scan.h"
#include "ob_ups_scan.h"

namespace oceanbase
{
  using namespace common;

  namespace sql
  {
    class ObTabletFuse: public ObPhyOperator, public ObLastRowkeyInterface
    {
    };
  } // end namespace sql
} // end namespace oceanbase

#endif /* _OB_TABLET_FUSE_H */
