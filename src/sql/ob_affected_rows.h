/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_affected_rows.h
 *
 * Authors:
 *   Guibin Du <tianguan.dgb@taobao.com>
 *
 */
#ifndef OCEANBASE_SQL_OB_AFFECTED_ROWS_H_
#define OCEANBASE_SQL_OB_AFFECTED_ROWS_H_ 
#include "common/ob_define.h"

namespace oceanbase
{
  namespace sql
  {
    class ObAffectedRows
    {
      public:
        ObAffectedRows() {}
        virtual ~ObAffectedRows() {}
        
        virtual int get_affected_rows(int64_t& row_count) = 0;
    };
  } // end namespace sql
} // end namespace oceanbase

#endif /* OCEANBASE_SQL_OB_AFFECTED_ROWS_H_ */



