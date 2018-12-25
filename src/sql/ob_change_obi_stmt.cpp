#include "ob_change_obi_stmt.h"

using namespace oceanbase;
using namespace oceanbase::sql;
using namespace oceanbase::common;

ObChangeObiStmt::ObChangeObiStmt(ObStringBuf *name_pool) : ObBasicStmt(ObBasicStmt::T_CHANGE_OBI)
{
  force_ = false;
  change_flow_ = false;
  name_pool_ = name_pool;
  target_server_addr_.assign_ptr(NULL, 0);
  role_.set_role(ObiRole::INIT);
}
ObChangeObiStmt::~ObChangeObiStmt()
{
}


void ObChangeObiStmt::print(FILE* fp, int32_t level, int32_t index)
{
  UNUSED(index);
  print_indentation(fp, level);
  fprintf(fp, "ObChangeObiStmt %d Begin\n", index);
  print_indentation(fp, level + 1);
  fprintf(fp, "target_server_addr = %.*s\n", target_server_addr_.length(), target_server_addr_.ptr());
  print_indentation(fp, level);
  fprintf(fp, "ObChangeObiStmt %d End\n", index);
}
int ObChangeObiStmt::set_target_server_addr(const char *target_server_addr)
{
  int ret = OB_SUCCESS;
  ObString src;
  src.assign_ptr(const_cast<char*>(target_server_addr), static_cast<int32_t>(strlen(target_server_addr)));
  if (OB_SUCCESS != (ret = ob_write_string(*name_pool_, src, target_server_addr_)))
  {
    TBSYS_LOG(WARN, "set target server addr failed, ret=%d", ret);
  }
  return ret;
}
void ObChangeObiStmt::get_target_server_addr(ObString &target_server_addr)
{
  target_server_addr = target_server_addr_;
}

void ObChangeObiStmt::set_force(bool force)
{
  force_ = force;
}
bool ObChangeObiStmt::get_force()
{
  return force_;
}
void ObChangeObiStmt::set_change_flow(bool change_flow)
{
  change_flow_ = change_flow;
}
bool ObChangeObiStmt::get_change_flow()
{
  return change_flow_;
}
void ObChangeObiStmt::set_target_role(ObiRole::Role role)
{
  role_.set_role(role);
}
ObiRole ObChangeObiStmt::get_target_role()
{
  return role_;
}
