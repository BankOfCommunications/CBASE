/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_operator_factory_impl.h
 *
 * Authors:
 *   Junquan Chen <jianming.cjq@taobao.com>
 *
 */

#ifndef _OB_OPERATOR_FACTORY_IMPL_H
#define _OB_OPERATOR_FACTORY_IMPL_H 1

#include "sql/ob_operator_factory.h"
#include "ob_fake_sstable_scan.h"
#include "ob_fake_ups_scan.h"
#include "ob_fake_ups_multi_get.h"

namespace oceanbase
{
  namespace sql
  {
    class ObOperatorFactoryImpl : public ObOperatorFactory
    {
      public:
        virtual ~ObOperatorFactoryImpl() {}

      public:
        ObSSTableScanInterface *new_sstable_scan();
        ObUpsScan *new_ups_scan();
        ObUpsMultiGet *new_ups_multi_get();
    };
  }
}

#endif /* _OB_OPERATOR_FACTORY_IMPL_H */

