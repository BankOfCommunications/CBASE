/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_prepare_stmt.cpp
 *
 * Authors:
 *   Guibin Du <tianguan.dgb@taobao.com>
 *
 */
#include "ob_prepare_stmt.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

void ObPrepareStmt::print(FILE* fp, int32_t level, int32_t index)
{
  UNUSED(index);
  print_indentation(fp, level);
  fprintf(fp, "<ObPrepareStmt %d Begin>\n", index);
  print_indentation(fp, level + 1);
  fprintf(fp, "Statement name ::= %.*s\n", stmt_name_.length(), stmt_name_.ptr());
  print_indentation(fp, level + 1);
  fprintf(fp, "Prepared Query Id ::= <%ld>\n", prepare_query_id_);
  print_indentation(fp, level);
  fprintf(fp, "<ObPrepareStmt %d End>\n", index);
}

