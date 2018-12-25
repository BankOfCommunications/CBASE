/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_table_rename.h
 *
 * Authors:
 *   Junquan Chen <jianming.cjq@taobao.com>
 *
 */

#ifndef _OB_TABLE_RENAME_H
#define _OB_TABLE_RENAME_H 1

#include "ob_rename.h"

namespace oceanbase
{
  namespace sql
  {
    class ObTableRename : public ObRename
    {
      protected:
        virtual int cons_row_desc();
        virtual ObPhyOperatorType get_type() const;
    };
  }
}

#endif /* _OB_TABLE_RENAME_H */

