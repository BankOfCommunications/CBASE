/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 *
 * Version: 0.1: load_statics.h,v 0.1 2012/03/15 14:00:00 zhidong Exp $
 *
 * Authors:
 *   xielun <xielun.szd@taobao.com>
 *     - some work details if you want
 *
 */

#ifndef LOAD_STATICS_H_
#define LOAD_STATICS_H_

#include "load_define.h"

namespace oceanbase
{
  namespace tools
  {
    struct NewStatics
    {
      int64_t execute_count;
      int64_t prepare_count;
      int64_t query_count;
      int64_t filter_count;
      int64_t err_count;
      int64_t consumer_count;
    };
    struct ProduceInfo
    {
      bool is_finish_;
      int64_t produce_count_;
      int64_t filter_count_;
      bool operator == (const ProduceInfo & other) const
      {
        return ((produce_count_ == other.produce_count_)
            && (filter_count_ == other.filter_count_));
      }
    };

    struct ConsumeInfo
    {
      int64_t consume_count_;
      int64_t succ_count_;
      int64_t succ_time_used_;
      int64_t return_res_;
      int64_t return_rows_;
      int64_t return_cells_;
      int64_t err_count_;
      int64_t err_time_used_;
      bool operator == (const ConsumeInfo & other) const
      {
        return ((consume_count_ == other.consume_count_) && (succ_count_ == other.succ_count_)
            && (succ_time_used_ == other.succ_time_used_) && (return_res_ == other.return_res_)
            && (return_rows_ == other.return_rows_) && (return_cells_ == other.return_cells_)
            && (err_count_ == other.err_count_) && (err_time_used_ == other.err_time_used_));
      }
    };

    struct Statics
    {
      ProduceInfo share_info_;
      ConsumeInfo new_server_;
      ConsumeInfo old_server_;
      int64_t compare_err_;
    public:
      Statics()
      {
        memset(this, 0, sizeof(Statics));
      }
      void print(const bool compare, const char * name, FILE * file) const;
      Statics operator - (const Statics & other) const;
    private:
      void print(const ProduceInfo & product, const char * section,
          const ConsumeInfo & info, FILE * file) const;
    };
  }
}

#endif // LOAD_STATICS_H_

