#include <stdio.h>
#include <stdlib.h>
#include <readline/readline.h>
#include <readline/history.h>
#include "sql/parse_node.h"
#include "sql/build_plan.h"
#include "sql/ob_multi_logic_plan.h"
#include "sql/ob_transformer.h"
#include "sql/ob_schema_checker.h"
#include "common/ob_string_buf.h"
#include "common/ob_malloc.h"

using namespace oceanbase::sql;
using namespace oceanbase::common;

#define BUF_LEN 102400
int main(void)
{
  clock_t sqlStartTime;
  clock_t sqlEndTime;
  char* pSqlString = NULL;
  bool  bExitFlag = false;
  int   ret;
  int64_t pos;
  char  buf[BUF_LEN];

  // It is for test only, no more questions about this usage.
  oceanbase::common::ob_init_memory_pool();
  oceanbase::common::ObStringBuf malloc_pool;
  ObSchemaChecker schema_checker;

  ParseResult result;
  result.malloc_pool_ = &malloc_pool;
  if(parse_init(&result))
  {
    printf("parse_init error!!!");
    return -1;
  }

  while (!bExitFlag)
  {
    // 1. read sql from terminator
    pSqlString = readline("SQL>");
    if(pSqlString && *pSqlString)
      add_history(pSqlString);
    else
      continue;

    if (strncasecmp(pSqlString, "exit", strlen("exit")) == 0
      || strncasecmp(pSqlString, "quit", strlen("quit")) == 0
      || strncasecmp(pSqlString, "q", strlen("q")) == 0)
    {
      bExitFlag = true;
      free(pSqlString);
      continue;
    }


    sqlStartTime = clock();

    // 2. parse the sql
    parse_sql(&result, pSqlString, strlen(pSqlString));
    if(result.result_tree_ == 0)
    {
      printf("parse_sql error!\n");
      printf("%s at %d:%d\n", result.error_msg_, result.line_, result.start_col_);
      continue;
    }
    print_tree(result.result_tree_, 0);

    // 3. resolve node trees

    ResultPlan resultPlan;
    resultPlan.name_pool_ = &malloc_pool;
    resultPlan.schema_checker_ = &schema_checker;
    resultPlan.plan_tree_ = NULL;
    ret = resolve(&resultPlan, result.result_tree_);

    sqlEndTime = clock();
    printf("SQL parse time: %lf\n", ((double) (sqlEndTime - sqlStartTime)) / CLOCKS_PER_SEC);

    if (ret == OB_SUCCESS)
    {
      // 4. print result
      ObMultiLogicPlan* pMultiPlan = static_cast<ObMultiLogicPlan*>(resultPlan.plan_tree_);
      pMultiPlan->print();
      oceanbase::sql::ObSqlContext context;
      oceanbase::sql::ObResultSet result_set; //add dragon [repair_test] 2016-12-30
      //ObTransformer ob_transformer(*pMultiPlan->at(0)->get_name_pool(), context);
      //mod dragon [repair_test] 2016-12-30 b
      ObTransformer ob_transformer(context, result_set);
      //ObTransformer ob_transformer(context);
      //mod e
      ObMultiPhyPlan multi_phy_plan;
      ErrStat err_stat;
       if (OB_SUCCESS != (ret = ob_transformer.generate_physical_plans(*pMultiPlan, multi_phy_plan, err_stat)))
      {
        TBSYS_LOG(WARN, "failed to transform to physical plan");
        ret = OB_ERR_GEN_PLAN;
      }
      else
      {
        ObPhysicalPlan *phy_plan = NULL;
        ObPhyOperator *exec_plan = NULL;
        for (int32_t i = 0; i < multi_phy_plan.size(); i++)
        {
          if ((NULL != (phy_plan = multi_phy_plan.at(0))) && (NULL != (exec_plan = phy_plan->get_phy_query(0))))
          {
            pos = phy_plan->to_string(buf, BUF_LEN);
            printf("%.*s\n", (int32_t)pos, buf);
          }
          else
          {
            printf("Transformer logical plan to physical plan error!\n");
            break;
          }
        }
      }
    }
    else
    {
      printf("Resolve error!\n");
      printf("ERROR MESSAGE: %s\n", resultPlan.err_stat_.err_msg_);
    }

    destroy_plan(&resultPlan);

    // 5. release node tree
    if (result.result_tree_)
    {
      destroy_tree(result.result_tree_);
      result.result_tree_ = NULL;
    }

    // 6. release sql
    free(pSqlString);
  }

  return 0;
}
