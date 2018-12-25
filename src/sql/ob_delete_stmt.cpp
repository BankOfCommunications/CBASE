#include "ob_delete_stmt.h"
#include <stdio.h>

namespace oceanbase
{
  namespace sql
  {
    using namespace oceanbase::common;

    ObDeleteStmt::ObDeleteStmt(ObStringBuf* name_pool)
    : ObStmt(name_pool, ObStmt::T_DELETE)
    {
        is_delete_multi_batch_ = false;//add gaojt [Delete_Update_Function] [JHOBv0.1] 20160308
        is_delete_all_rowkey_ = false;//add gaojt [Delete_Update_Function] [JHOBv0.1] 20160418
    }

    ObDeleteStmt::~ObDeleteStmt()
    {
    }

    void ObDeleteStmt::print(FILE* fp, int32_t level, int32_t index)
    {
      print_indentation(fp, level);
      fprintf(fp, "ObDeleteStmt %d Begin\n", index);
      ObStmt::print(fp, level + 1);
      print_indentation(fp, level + 1);
      fprintf(fp, "FROM ::= <%ld>\n", table_id_);
      print_indentation(fp, level);
      fprintf(fp, "ObDeleteStmt %d End\n", index);
    }

  }
}
