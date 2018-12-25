/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_explain_stmt.cpp
 *
 * Authors:
 *   Guibin Du <tianguan.dgb@taobao.com>
 *
 */
#include "ob_explain_stmt.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

void ObExplainStmt::print(FILE* fp, int32_t level, int32_t index)
{
  UNUSED(index);
  print_indentation(fp, level);
  fprintf(fp, "ObExplainStmt %d Begin\n", index);
  if (verbose_)
  {
    print_indentation(fp, level + 1);
    fprintf(fp, "VERBOSE\n");
  }
  print_indentation(fp, level + 1);
  fprintf(fp, "Explain Query Id ::= <%ld>\n", explain_query_id_);
  print_indentation(fp, level);
  fprintf(fp, "ObExplainStmt %d End\n", index);
}



