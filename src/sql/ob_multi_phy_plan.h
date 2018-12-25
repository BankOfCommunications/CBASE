/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_multi_phy_plan.h
 *
 * Authors:
 *   Guibin Du <tianguan.dgb@taobao.com>
 *
 */

#ifndef OCEANBASE_OB_SQL_MULTI_PHY_PLAN_H_
#define OCEANBASE_OB_SQL_MULTI_PHY_PLAN_H_
#include "ob_physical_plan.h"
#include "common/ob_vector.h"

namespace oceanbase
{
  namespace sql
  {
    class ObMultiPhyPlan : public oceanbase::common::ObVector<ObPhysicalPlan*>
    {
    public:
      ObMultiPhyPlan();
      ~ObMultiPhyPlan();

      int64_t to_string(char* buf, const int64_t buf_len) const;
    
    private:
      DISALLOW_COPY_AND_ASSIGN(ObMultiPhyPlan);
    };
  }
}

#endif //OCEANBASE_OB_SQL_MULTI_PHY_PLAN_H_

