/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_multiple_get_merge.h 
 *
 * Authors:
 *   Junquan Chen <jianming.cjq@alipay.com>
 *
 */

#ifndef _OB_MULTIPLE_GET_MERGE_H
#define _OB_MULTIPLE_GET_MERGE_H 1

#include "ob_multiple_merge.h"
//add wenghaixing [secondary index upd.2] 20141127
#include "ob_mem_sstable_scan.h"
//#include "ob_inc_scan.h"
//add e

namespace oceanbase
{
  namespace sql
  {
    class ObMultipleGetMerge : public ObMultipleMerge
    {
      public:
        int open();
        int close();
        virtual void reset();
        virtual void reuse();
        int get_next_row(const ObRow *&row);
        enum ObPhyOperatorType get_type() const{return PHY_MULTIPLE_GET_MERGE;};
        int64_t to_string(char *buf, int64_t buf_len) const;
         //add wenghaixing [secondary index upd.2] 20141127
        void reset_iterator();
        //add e
        DECLARE_PHY_OPERATOR_ASSIGN;
      private:
        ObRow cur_row_;
    };
  }
}

#endif /* _OB_MULTIPLE_GET_MERGE_H */
  

