/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_phy_operator_factory.h
 *
 * Authors:
 *   Junquan Chen <jianming.cjq@alipay.com>
 *
 */

#ifndef _OB_PHY_OPERATOR_FACTORY_H
#define _OB_PHY_OPERATOR_FACTORY_H 1

#include "ob_phy_operator_type.h"
#include "common/page_arena.h"
#include "common/ob_resource_pool.h"
#include "ob_project.h"
#include "ob_filter.h"
#include "ob_when_filter.h"
#include "ob_insert_dbsem_filter.h"
#include "ob_mem_sstable_scan.h"
#include "ob_empty_row_filter.h"
#include "ob_row_count.h"
#include "ob_multiple_get_merge.h"
#include "ob_expr_values.h"
#include "ob_multiple_scan_merge.h"

namespace oceanbase
{
  namespace sql
  {
    class ObPhyOperator;
    class ObPhyOperatorFactory
    {
      static const int64_t TC_NUM = 1;
      static const int64_t GL_NUM = 4096;
      public:
        ObPhyOperatorFactory(){}
        virtual ~ObPhyOperatorFactory(){}
      public:
        virtual ObPhyOperator *get_one(ObPhyOperatorType phy_operator_type, common::ModuleArena &allocator);
        virtual void release_one(ObPhyOperator *opt);
      protected:
        //ObResourcePool<ObProject, TC_NUM, GL_NUM> pool_project_;
        //ObResourcePool<ObFilter, TC_NUM, GL_NUM> pool_filter_;
        //ObResourcePool<ObWhenFilter, TC_NUM, GL_NUM> pool_when_filter_;
        //ObResourcePool<ObInsertDBSemFilter, TC_NUM, GL_NUM> pool_insert_db_sem_filter_;
        //ObResourcePool<ObMemSSTableScan, TC_NUM, GL_NUM> pool_mem_sstable_scan_;
        //ObResourcePool<ObEmptyRowFilter, TC_NUM, GL_NUM> pool_empty_row_filter_;
        //ObResourcePool<ObRowCount, TC_NUM, GL_NUM> pool_row_count_;
        //ObResourcePool<ObMultipleGetMerge, TC_NUM, GL_NUM> pool_multiple_get_merge_;
        //ObResourcePool<ObExprValues, TC_NUM, GL_NUM> pool_expr_values_;
        //ObResourcePool<ObMultipleScanMerge, TC_NUM, GL_NUM> pool_multiple_scan_merge_;
    };
  }
}

#endif /* _OB_PHY_OPERATOR_FACTORY_H */
