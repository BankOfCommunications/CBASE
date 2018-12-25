/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_operator_factory_impl.cpp
 *
 * Authors:
 *   Junquan Chen <jianming.cjq@taobao.com>
 *
 */

#include "ob_operator_factory_impl.h"

using namespace oceanbase;
using namespace sql;
using namespace test;


ObSSTableScanInterface *ObOperatorFactoryImpl::new_sstable_scan()
{
  return new ObFakeSSTableScan("./tablet_scan_test_data/table1.ini");
}

ObUpsScan *ObOperatorFactoryImpl::new_ups_scan()
{
  return new ObFakeUpsScan("./tablet_scan_test_data/ups_table1.ini");
}

ObUpsMultiGet *ObOperatorFactoryImpl::new_ups_multi_get()
{
  return new ObFakeUpsMultiGet("./tablet_scan_test_data/ups_table2.ini");
}

