#include "common/ob_define.h"
#include "common/ob_string.h"
#include "common/ob_strings.h"
#include "ob_use_db_stmt.h"

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::sql;

const common::ObStrings* ObUseDbStmt::get_database() const
{
  return &database_;
}

ObUseDbStmt::ObUseDbStmt(const ObUseDbStmt &other)
   :ObBasicStmt(other.get_stmt_type(), other.get_query_id())
{
  *this = other;
}

ObUseDbStmt& ObUseDbStmt::operator=(const ObUseDbStmt &other)
{
  if(this == &other)
  {

  }
  else
  {
    int ret = OB_SUCCESS;
    const common::ObStrings* db = other.get_database();
    int i = 0;
    for(i = 0; i < db->count(); i++)
    {
      ObString db_name;
      if(OB_SUCCESS != (ret = db->get_string(i, db_name)))
      {
        TBSYS_LOG(WARN, "failed to get db_name from use db statement");
        break;
      }
      else if(OB_SUCCESS != (ret = this->add_database(db_name)))
      {
        TBSYS_LOG(WARN, "failed to set db name to use db statement");
        break;
      }
    }
  }
  return *this;
}

ObUseDbStmt::ObUseDbStmt()
  :ObBasicStmt(ObBasicStmt::T_USE_DATABASE)
{
}

ObUseDbStmt::~ObUseDbStmt()
{
}

int ObUseDbStmt::add_database(const ObString &database)
{
  int ret = common::OB_SUCCESS;
  if(common::OB_SUCCESS != (ret = database_.add_string(database)))
  {
    TBSYS_LOG(WARN, "failed to add database, err = %d", ret);
  }
  return ret;
}

void ObUseDbStmt::print(FILE *fp, int32_t level, int32_t index)
{
  print_indentation(fp, level);
  fprintf(fp, "<ObUseDbStmt id=%d>\n", index);
  common::ObString database;
  for(int64_t i = 0; i < database_.count(); i++)
  {
    if(common::OB_SUCCESS != (database_.get_string(i,database)))
    {
      TBSYS_LOG(WARN, "get string from database failed, index[%ld],", i);
    }
    if(0 == i)
    {
      print_indentation(fp, level + 1);
      fprintf(fp, "Database := [%.*s]", database.length(), database.ptr());
    }
    else
    {
      fprintf(fp, "%.*s", database.length(), database.ptr());
    }
  }
  fprintf(fp, "\n");
  print_indentation(fp, level);
  fprintf(fp, "</ObUseDbStmt>\n");
}
