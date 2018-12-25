/*
 * add by wenghaixing[secondary index] 2014/10/24
 * This is create index stmt cpp file
 *
 **/
#include "ob_create_index_stmt.h"
#include "ob_schema_checker.h"
using namespace oceanbase::sql;
using namespace oceanbase::common;

ObCreateIndexStmt::ObCreateIndexStmt(ObStringBuf* name_pool)
  : ObBasicStmt(ObBasicStmt::T_CREATE_INDEX)
{
  name_pool_ = name_pool;
  //add wenghaixing[secondary index create fix]20141225
  has_storing_col_=true;
  //add e
}
ObCreateIndexStmt::~ObCreateIndexStmt()
{
}
int ObCreateIndexStmt::add_index_colums(ResultPlan& result_plan,const common::ObString tname,const common::ObString index_column)
{
/*
 *      first,you should check if index colums of source table are exist;
 *      then,you should check if there are duplicate colums;
 **/
  int ret = OB_SUCCESS;
  ObString str;
  ObSchemaChecker* schema_checker = NULL;
  uint64_t col_id = OB_INVALID_ID;
  schema_checker = static_cast<ObSchemaChecker*>(result_plan.schema_checker_);
  if (schema_checker == NULL)
  {
    ret = common::OB_ERR_SCHEMA_UNSET;
    snprintf(result_plan.err_stat_.err_msg_, MAX_ERROR_MSG,
          "Schema(s) are not set");
  }
  if(OB_SUCCESS == ret)
  {
    col_id = schema_checker->get_column_id(tname,index_column);
    if(OB_INVALID_ID == col_id)
    {
      ret = OB_ERR_COLUMN_NOT_FOUND;
      snprintf(result_plan.err_stat_.err_msg_, MAX_ERROR_MSG,
                "Index Columns %.*s are not found",index_column.length(), index_column.ptr());
    }
  }
  if(OB_SUCCESS == ret)
  {
    if(index_colums_.count()>0)
    {
      for (int32_t i = 0; i < index_colums_.count(); i++)
      {
        if (index_colums_.at(i) == index_column)
        {
          //modify liuxiao [secondary index static_index_build.bug_fix.merge_error]20150604
          //ret = OB_ERR_TABLE_DUPLICATE;
          ret = OB_ERR_COLUMN_DUPLICATE;
          //modify e
          snprintf(result_plan.err_stat_.err_msg_, MAX_ERROR_MSG,
                             "Not unique index_colums_: '%.*s'", index_column.length(), index_column.ptr());
          break;
        }
      }
    }
    //modify liuxiao [secondary index static_index_build.bug_fix.merge_error]20150604
    //if (OB_SUCCESS != (ret = ob_write_string(*name_pool_, index_column, str)))
    if(OB_SUCCESS != ret)
    {
      TBSYS_LOG(ERROR,"create index stmt error ret=[%d]", ret);
    }
    else if (OB_SUCCESS != (ret = ob_write_string(*name_pool_, index_column, str)))
    //modify e
    {
      TBSYS_LOG(ERROR,
                "Make space for %.*s failed", index_column.length(), index_column.ptr());
    }
    else
    {
      index_colums_.push_back(str);
    }
  }
  return ret;
}
void ObCreateIndexStmt::print(FILE* fp, int32_t level, int32_t index)
{
    //todo
    UNUSED(fp);
    UNUSED(level);
    UNUSED(index);
}
