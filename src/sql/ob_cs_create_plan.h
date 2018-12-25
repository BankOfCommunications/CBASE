/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_cs_create_plan.h
 *
 * Authors:
 *   Junquan Chen <jianming.cjq@alipay.com>
 *
 */

#ifndef _OB_CS_CREATE_PLAN_H
#define _OB_CS_CREATE_PLAN_H 1

#include "common/ob_schema.h"
#include "ob_project.h"
#include "ob_tablet_join.h"
#include "common/ob_array.h"
#include "common/ob_bit_set.h"

namespace oceanbase
{
  using namespace common;
  namespace sql
  {
    class ObCSCreatePlan
    {
      public:
        ObCSCreatePlan() {}
        virtual ~ObCSCreatePlan() {}

        virtual int create_plan(const ObSchemaManagerV2 &schema_mgr) = 0;
      
        const static int64_t COLUMN_IDS_HASH_SET_BUCKET_SIZE = 1000;

      protected:
        int get_basic_column_and_join_info(
            const ObProject &project, 
            const ObSchemaManagerV2 &schema_mgr, 
            const uint64_t table_id, 
            const uint64_t renamed_table_id, 
            ObArray<uint64_t> &basic_columns, 
            ObTabletJoin::TableJoinInfo &table_join_info
                // mod by maosy [Delete_Update_Function_isolation_RC] 20161218 b:
                //add duyr [Delete_Update_Function_isolation] [JHOBv0.1] 20160531:b
                //            ,const ObDataMarkParam *data_mark_param = NULL
                //add duyr 20160531:e
                //mod e
            );

      private:
        //max column id is 65536
        common::ObBitSet<65536> column_ids_;
    };
  }
}

#endif /* _OB_CS_CREATE_PLAN_H */

