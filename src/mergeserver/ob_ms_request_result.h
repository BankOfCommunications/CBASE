/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_ms_request_result.h
 *
 * Authors:
 *   Junquan Chen <jianming.cjq@alipay.com>
 *
 */

#ifndef _OB_MS_REQUEST_RESULT_H
#define _OB_MS_REQUEST_RESULT_H 1

#include "common/ob_vector.h"
#include "common/ob_new_scanner.h"

namespace oceanbase
{
  namespace mergeserver
  {
    class ObMsRequestResult
    {
      public:
        ObMsRequestResult();
        virtual ~ObMsRequestResult();

        int get_next_row(common::ObRow &row);
        int add_scanner(common::ObNewScanner *scanner);

        int64_t get_used_mem_size() const;
        int64_t get_row_num() const;
        bool is_finish() const;
        void clear();
        common::ObNewScanner *get_last_scanner();

      private:
        common::ObVector<common::ObNewScanner *> scanner_list_;
        int32_t current_;
    };
  }
}

#endif /* _OB_MS_REQUEST_RESULT_H */
