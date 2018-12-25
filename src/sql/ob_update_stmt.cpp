#include "ob_update_stmt.h"
#include <stdio.h>
#include <stdlib.h>

namespace oceanbase
{
  namespace sql
  {
    using namespace oceanbase::common;

    ObUpdateStmt::ObUpdateStmt(oceanbase::common::ObStringBuf* name_pool)
    : ObStmt(name_pool, T_UPDATE)
    {
      //add lijianqiang [sequence] 20150910:b
      set_clause_boundary_ = OB_INVALID_ID;
      //add 20150910:e
      //add gaojt [Delete_Update_Function] [JHOBv0.1] 20160319:b
      is_update_multi_batch_ = false;
      questionmark_num_ = 0;
      //add gaojt 20160319:e
      is_update_all_rowkey_ = false;//add gaojt [Delete_Update_Function] [JHOBv0.1] 20160418
    }

    ObUpdateStmt::~ObUpdateStmt()
    {
    }

    void ObUpdateStmt::print(FILE* fp, int32_t level, int32_t index)
    {
      print_indentation(fp, level);
      fprintf(fp, "ObUpdateStmt %d Begin\n", index);
      ObStmt::print(fp, level + 1);
      print_indentation(fp, level + 1);
      fprintf(fp, "UPDATE ::= <%ld>\n", table_id_);
      print_indentation(fp, level + 1);
      fprintf(fp, "SET ::= ");
      for (int64_t i = 0; i < update_columns_.count(); i++)
      {
        if (i > 0)
          fprintf(fp, ", <%ld, %ld>", update_columns_.at(i), update_exprs_.at(i));
        else
          fprintf(fp, "<%ld, %ld>", update_columns_.at(i), update_exprs_.at(i));
      }
      fprintf(fp, "\n");
      print_indentation(fp, level);
      fprintf(fp, "ObUpdateStmt %d End\n", index);
    }


  }
}
