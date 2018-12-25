#include "ob_insert_stmt.h"
//add dragon [varchar limit] 2016-8-10 09:34:17
#include "ob_schema_checker.h"
//add e
#include <stdio.h>
#include <stdlib.h>

using namespace oceanbase::common;
using namespace oceanbase::sql;

ObInsertStmt::ObInsertStmt(oceanbase::common::ObStringBuf* name_pool)
: ObStmt(name_pool, T_INSERT)
{
  sub_query_id_ = OB_INVALID_ID;
  is_replace_ = false;
  //add lijianqiang [sequence] 20150401 :b
  column_num_sum_=0;
  //add 20150401:e
    is_insert_multi_batch_ = false;//add gaojt [Insert_Subquery_Function] [JHOBv0.1] 20150507

}

ObInsertStmt::~ObInsertStmt()
{
  for (int64_t i = 0; i < value_vectors_.count(); i++)
  {
    ObArray<uint64_t>& value_row = value_vectors_.at(i);
    value_row.clear();
  }
  //add lijianqiang [sequence] 20150401
  col_sequence_map_.clear();
  col_sequence_types_.clear();
  for (int64_t i = 0; i < next_prev_ids_.count(); i++)
  {
    ObArray<uint64_t>& value_row = next_prev_ids_.at(i);
    value_row.clear();
  }
  //add 20150401
}

void ObInsertStmt::print(FILE* fp, int32_t level, int32_t index)
{
  print_indentation(fp, level);
  fprintf(fp, "<ObInsertStmt %d Begin>\n", index);
  ObStmt::print(fp, level + 1);
  print_indentation(fp, level + 1);
  fprintf(fp, "INTO ::= <%ld>\n", table_id_);
  if (sub_query_id_ == OB_INVALID_ID)
  {
    print_indentation(fp, level + 1);
    fprintf(fp, "VALUES ::= ");
    for (int64_t i = 0; i < value_vectors_.count(); i++)
    {
      if (i == 0)
        fprintf(fp, "<");
      else
        fprintf(fp, ", <");
      ObArray<uint64_t>& value_row = value_vectors_.at(i);
      for (int j = 0; j < value_row.count(); j++)
      {
        if (j == 0)
          fprintf(fp, "%ld", value_row.at(j));
        else
          fprintf(fp, ", %ld", value_row.at(j));
      }
      fprintf(fp, ">");
    }
    fprintf(fp, "\n");
  }
  else
  {
    print_indentation(fp, level + 1);
    fprintf(fp, "SUBQUERY ::= <%ld>\n", sub_query_id_);
  }
  print_indentation(fp, level);
  fprintf(fp, "<ObInsertStmt %d End>\n", index);
}

//add　dragon [varchar limit] 2016-8-9 15:00:38
int ObInsertStmt::do_var_len_check (ResultPlan *result_plan, ObColumnInfo &cinfo, int64_t length)
{
  int ret = OB_SUCCESS;
  if(NULL == result_plan)
    ret = OB_ERROR;
  else
  {
    if(NULL == result_plan->schema_checker_)
    {
      TBSYS_LOG(WARN, "result_plan is NULL");
      ret = OB_ERROR;
    }
    else
    {
      ObSchemaChecker *sc = static_cast<ObSchemaChecker*>(result_plan->schema_checker_);
      if(OB_SUCCESS != (ret = sc->varchar_len_check(cinfo, length)))
      {
        TBSYS_LOG(WARN, "varchar length check failed!ret[%d]", ret);
      }
    }
  }
  return ret;
}
//add 2016-8-9 e
