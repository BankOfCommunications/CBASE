/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_execute_stmt.cpp
 *
 * Authors:
 *   Guibin Du <tianguan.dgb@taobao.com>
 *
 */
#include "ob_execute_stmt.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

void ObExecuteStmt::print(FILE* fp, int32_t level, int32_t index)
{
  UNUSED(index);
  print_indentation(fp, level);
  fprintf(fp, "<ObExecuteStmt %d Begin>\n", index);
  print_indentation(fp, level + 1);
  fprintf(fp, "<Using Variables Begin>\n");
  print_indentation(fp, level + 2);
  for (int64_t i = 0; i < variable_names_.count(); i++)
  {
    if (i == 0)
      fprintf(fp, "Variable names ::= <%.*s>", variable_names_.at(i).length(), variable_names_.at(i).ptr());
    else
      fprintf(fp, ", <%.*s>", variable_names_.at(i).length(), variable_names_.at(i).ptr());
  }
  fprintf(fp, "\n");
  print_indentation(fp, level + 1);
  fprintf(fp, "<Using Variables End>\n");
  print_indentation(fp, level);
  fprintf(fp, "<ObExecuteStmt %d End>\n", index);

}

