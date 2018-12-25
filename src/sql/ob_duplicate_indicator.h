/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 *
 * Version: 0.1: ob_string_buf.h,v 0.1 2010/08/19 16:19:02 chuanhui Exp $
 *
 * Authors:
 *   Guibin Du <tianguan.dgb@taobao.com>
 *     - some work details if you want
 */
#ifndef OCEANBASE_SQL_OB_DUPLICATE_INDICATOR_H_
#define OCEANBASE_SQL_OB_DUPLICATE_INDICATOR_H_

#include "tblog.h"
#include "common/hash/ob_hashset.h"
#include "common/ob_rowkey.h"

namespace oceanbase
{
  namespace sql
  {
    class ObDuplicateIndicator
    {
      public:
        ObDuplicateIndicator() {}
        ~ObDuplicateIndicator() {}
        
        int init(int64_t bucket_num);
        int reset();
        int have_seen(const common::ObRowkey& val, bool& exist);
      private:
        DISALLOW_COPY_AND_ASSIGN(ObDuplicateIndicator);
      private:
        common::hash::ObHashSet<common::ObRowkey> distinct_set_;
        common::ModuleArena allocator_;
    };

    inline int ObDuplicateIndicator :: init(int64_t bucket_num)
    {
      return distinct_set_.create(common::hash::cal_next_prime(bucket_num));
    }

    inline int ObDuplicateIndicator :: have_seen(const common::ObRowkey& val, bool& exist)
    {
      int ret = common::OB_SUCCESS;
      common::ObRowkey tmp_val;
      if ((ret = common::ob_write_rowkey(allocator_, val, tmp_val)) == common::OB_SUCCESS)
      {
        exist = (distinct_set_.set(tmp_val) != common::hash::HASH_INSERT_SUCC);
      }
      return ret;
    }

    inline int ObDuplicateIndicator :: reset()
    {
      return distinct_set_.clear();
    }
  }
}

#endif //OCEANBASE_SQL_OB_DUPLICATE_INDICATOR_H_

