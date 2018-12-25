/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_multiple_merge.h
 *
 * Authors:
 *   Junquan Chen <jianming.cjq@alipay.com>
 *
 */

#ifndef _OB_MULTIPLE_MERGE_H
#define _OB_MULTIPLE_MERGE_H 1

#include "ob_phy_operator.h"
#include "common/ob_ups_row.h"
//add duyr [Delete_Update_Function_isolation] [JHOBv0.1] 20160531:b
#include "common/ob_common_param.h"
//add duyr 20160531:e

namespace oceanbase
{
  namespace sql
  {
    using namespace common;

    class ObMultipleMerge : public ObPhyOperator
    {
      public:
        ObMultipleMerge();
        virtual ~ObMultipleMerge();
        virtual void reset();
        virtual void reuse();
        static const int64_t MAX_CHILD_OPERATOR_NUM = 128;
        virtual int set_child(int32_t child_idx, ObPhyOperator &child_operator );
        virtual ObPhyOperator *get_child(int32_t child_idx) const;

        virtual int32_t get_child_num() const;

        virtual int get_row_desc(const ObRowDesc *&row_desc) const;
        void set_is_ups_row(bool is_ups_row);
        //add duyr [Delete_Update_Function_isolation] [JHOBv0.1] 20160531:b
        int set_data_mark_param(const ObDataMarkParam &param);
        const ObDataMarkParam& get_data_mark_param()const;
        // add by maosy [Delete_Update_Function_isolation_RC] 20161218
        void set_start_time (int64_t start_time )
        {
            if(start_time ==0)
            {
                stmt_start_time_ = INT64_MAX;
            }
            else
            {
                stmt_start_time_ = start_time;
            }
        }
        int64_t get_start_time ()
        {
            return stmt_start_time_ ;
        }
    //add by maosy
        //add duyr 20160531:e

        DECLARE_PHY_OPERATOR_ASSIGN;
        VIRTUAL_NEED_SERIALIZE_AND_DESERIALIZE;
      protected:
        int copy_rowkey(const ObRow &row, ObRow &result_row, bool deep_copy);

      protected:
        CharArena allocator_;
        ObPhyOperator *child_array_[MAX_CHILD_OPERATOR_NUM];
        int32_t child_num_;
        bool is_ups_row_;
        //add duyr [Delete_Update_Function_isolation] [JHOBv0.1] 20160531:b
        ObDataMarkParam data_mark_param_;
		// del by maosy [Delete_Update_Function_isolation_RC] 20161228
//        ObDataMark cur_data_mark_;
//        ObRow data_mark_final_row_;
//        ObRowDesc data_mark_final_row_desc_;
// del by maosy e
        // add by maosy [Delete_Update_Function_isolation_RC] 20161218
        int64_t stmt_start_time_;
    //add by maosy
        //add duyr 20160531:e
    };
  }
}

#endif /* _OB_MULTIPLE_MERGE_H */
