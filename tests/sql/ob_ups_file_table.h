/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_ups_file_table.h
 *
 * Authors:
 *   Junquan Chen <jianming.cjq@taobao.com>
 *
 */

#ifndef _OB_UPS_FILE_TABLE_H
#define _OB_UPS_FILE_TABLE_H 1

#include "common/ob_ups_row.h"
#include "ob_file_table.h"

namespace oceanbase
{
  namespace sql
  {
    namespace test
    {
      class ObUpsFileTable : public ObFileTable
      {
        public:
          ObUpsFileTable(const char *file_name);
          virtual ~ObUpsFileTable() { }
          void reset() {}
          void reuse() {}
          virtual ObPhyOperatorType get_type() const { return PHY_INVALID; }

        protected:
          virtual int parse_line(const ObRow *&row);
          virtual ObRow *get_curr_row();

        private:
          ObUpsRow curr_ups_row_;
      };
    }
  }
}

#endif /* _OB_UPS_FILE_TABLE_H */
