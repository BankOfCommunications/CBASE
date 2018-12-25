#include "ob_kill_stmt.h"

using namespace oceanbase::sql;
using namespace oceanbase::common;
ObKillStmt::ObKillStmt(ObStringBuf* name_pool): ObBasicStmt(ObBasicStmt::T_KILL)
{
  name_pool_ = name_pool;
  thread_id_ = -1;
  is_query_ = false;
  is_global_ = false;
}

ObKillStmt::~ObKillStmt()
{

}

void ObKillStmt::print(FILE* fp, int32_t level, int32_t index)
{
  UNUSED(index);
  print_indentation(fp, level);
  fprintf(fp, "ObKillStmt %d Begin\n", index);
  print_indentation(fp, level + 1);
  fprintf(fp, "thread id is %ld\n", thread_id_);
  print_indentation(fp, level + 1);
  fprintf(fp, "is query is %d\n", is_query_? 1 : 0);
  print_indentation(fp, level);
  fprintf(fp, "ObKillStmt %d End\n", index);
}
