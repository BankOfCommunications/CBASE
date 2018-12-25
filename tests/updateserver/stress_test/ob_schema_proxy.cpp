/**
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * Authors:
 *   yanran <yanran.hfs@taobao.com>
 *     - some work details if you want
 */

#include "ob_schema_proxy.h"

using namespace oceanbase::common;
using namespace oceanbase::test;

char* id_2_name[] = {
  "c0", "c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8", "c9", "c10", "c11", "c12"
};

int ObSchemaProxy::fetch_schema()
{
  return 0;
}

int64_t ObSchemaProxy::get_column_num()
{
  return 10;
}

const ObString& ObSchemaProxy::get_column_name(uint64_t id)
{
  return column_name_[id];
}

uint64_t ObSchemaProxy::get_table_id()
{
  return 1001;
}

const ObString& ObSchemaProxy::get_table_name()
{
  table_name_.assign("add_table", 9);
  return table_name_;
}

ObSchemaProxy::ObSchemaProxy()
{
  for (int i = 0; i < 12; i++)
  {
    column_name_[i].assign(id_2_name[i], strlen(id_2_name[i]));
  }
}
