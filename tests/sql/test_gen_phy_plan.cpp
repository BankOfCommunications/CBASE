#include <gtest/gtest.h>
#include <unistd.h>
#include "common/ob_define.h"
#include "common/ob_string_buf.h"
#include "common/ob_malloc.h"
#include "common/utility.h"
#include "sql/parse_node.h"
#include "sql/build_plan.h"
#include "sql/ob_multi_logic_plan.h"
#include "sql/ob_transformer.h"
#include "sql/ob_schema_checker.h"

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::sql;

const char* STR_SQL_TEST = "SQL_TEST_PHY_PLAN";
const char* STR_SQL_FILE = "sql_file_2.sql";
const int64_t BUF_LEN = 102400;

// NOTE about sql_file_2.sql
// 1. while line && line begin with "--" will be ignored
// 2. @@[XXX] wile be filename XXX.input/XXX.output
// 3. sql must be in one line
int32_t run_sql_test_file(const char* file_name)
{
  char    sql_string[OB_MAX_SQL_LENGTH];
  int32_t ret = OB_SUCCESS;  // ok
  FILE*   sql_fd = NULL;
  FILE*   cur_fd = NULL;
  char    cwd[OB_MAX_FILE_NAME_LENGTH];
  char    cmd[OB_MAX_FETCH_CMD_LENGTH];
  memset(cwd, 0, OB_MAX_FILE_NAME_LENGTH);
  memset(cmd, 0, OB_MAX_FILE_NAME_LENGTH);
  int64_t pos;
  char  buf[BUF_LEN];
  oceanbase::common::ObStringBuf str_buf;
  ObSchemaChecker schema_checker;

  // It is for test only, no more questions about this usage.
  if (ret == OB_SUCCESS && tbsys::CConfig::getCConfig().load("MyTestSchema.ini") == EXIT_FAILURE)
  {
    TBSYS_LOG(ERROR, "load schema error");
    ret = OB_ERROR;
  }
  if (ret == OB_SUCCESS && (sql_fd = fopen(file_name, "r")) == NULL)
  {
    TBSYS_LOG(ERROR, "can not open sql file");
    ret = OB_ERROR;
  }
  if (ret == OB_SUCCESS && NULL == getcwd(cwd, OB_MAX_FILE_NAME_LENGTH))
  {
    TBSYS_LOG(ERROR, "getcwd error[%s]", strerror(errno));
    ret = OB_ERROR;
  }
  if (ret == OB_SUCCESS)
  {
    snprintf(cmd, OB_MAX_FETCH_CMD_LENGTH, "mkdir -p %s/%s", cwd, STR_SQL_TEST);
    system(cmd);
    memset(cmd, 0, OB_MAX_FILE_NAME_LENGTH);
    snprintf(cmd, OB_MAX_FETCH_CMD_LENGTH, "%s/%s", cwd, STR_SQL_TEST);
    if (chdir(cmd))
    {
      TBSYS_LOG(ERROR, "chdir error[%s]", strerror(errno));
      ret = OB_ERROR;
    }
  }

  ParseResult result;
  result.malloc_pool_ = &str_buf;
  if(ret == OB_SUCCESS && parse_init(&result))
  {
    TBSYS_LOG(ERROR, "parse_init error!!!");
    ret = OB_ERROR;
  }

  while (ret == OB_SUCCESS && fgets(sql_string, OB_MAX_SQL_LENGTH, sql_fd) != NULL)
  {
    char* ptr = sql_string;
    ptr = ltrim(ptr);
    ptr = rtrim(ptr);
    if (strlen(ptr) == 0 || strncmp(ptr, "--", 2) == 0)
    {
      fprintf(stderr, "%s\n", sql_string);
      continue;
    }
    if (strncmp(ptr, "@@", 2) == 0)
    {
      if (cur_fd)
      {
        fclose(cur_fd);
        cur_fd = NULL;
      }
      memset(cmd, 0, OB_MAX_FILE_NAME_LENGTH);
      snprintf(cmd, OB_MAX_FETCH_CMD_LENGTH, "%s/%s/%s.output", cwd, STR_SQL_TEST, ltrim(ptr + 2));
      if ((cur_fd = freopen(cmd, "w", stderr)) == NULL)
      {
        TBSYS_LOG(ERROR, "open %s error", cmd);
        ret = OB_ERROR;
        break;
      }
      continue;
    }

    // parse sql
    fprintf(stderr, "<<Part 1 : SQL STRING>>\n%s\n", sql_string);
    parse_sql(&result, ptr, strlen(ptr));
    fflush(stderr);
    if(result.result_tree_ == 0)
    {
      fprintf(stderr, "parse_sql error!\n");
      fprintf(stderr, "%s at %d:%d\n", result.error_msg_, result.line_, result.start_col_);
      continue;
    }
    else
    {
      fprintf(stderr, "\n<<Part 2 : PARSE TREE>>\n");
      //print_tree(result.result_tree_, 0);
    }

    ResultPlan result_plan;
    result_plan.name_pool_ = &str_buf;
    result_plan.schema_checker_ = &schema_checker;
    result_plan.plan_tree_ = NULL;
    if (resolve(&result_plan, result.result_tree_) == OB_SUCCESS)
    {
      ObMultiLogicPlan* multi_plan = static_cast<ObMultiLogicPlan*>(result_plan.plan_tree_);
      fflush(stderr);
      fprintf(stderr, "\n<<Part 3 : LOGICAL PLAN>>\n");
      multi_plan->print();
      oceanbase::sql::ObSqlContext context;
      oceanbase::sql::ObResultSet result; //add dragon [repair_test] 2016-12-30

      fprintf(stderr, "\n<<Part 4 : PHYSICAL PLAN>>\n");
      //ObTransformer ob_transformer(str_buf, context);
      //mod dragon [repair_test] 2016-12-30 b
      ObTransformer ob_transformer(context, result);
      //ObTransformer ob_transformer(context);
      //mod e
      ObMultiPhyPlan multi_phy_plan;
      ErrStat err_stat;
      if (OB_SUCCESS != (ret = ob_transformer.generate_physical_plans(*multi_plan, multi_phy_plan, err_stat)))
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
            fprintf(stderr, "%.*s\n", (int32_t)pos, buf);
          }
          else
          {
            fprintf(stderr, "Physical plan error of statmement %d!\n", i + 1);
          }
        }
      }
    }
    else
    {
      fflush(stderr);
      fprintf(stderr, "Resolve error!\n");
      continue;
    }

    destroy_plan(&result_plan);
    if (result.result_tree_)
    {
      destroy_tree(result.result_tree_);
      result.result_tree_ = NULL;
    }
  }

  if (sql_fd)
    fclose(sql_fd);
  if (cur_fd)
    fclose(cur_fd);
  if (strlen(cwd) != 0)
    chdir(cwd);
  freopen("sqltest.result", "w", stderr);
  //freopen("/dev/console", "r", stdin);
  return ret;
}

// Error message like this:
// [2012-03-14 15:07:54] ERROR add_column_item (ObStmt.cpp:228) [47286656761056] Unkown column name qty
// to compare the real error message, that need to skip the time and thread message
// we pick the last ']' as flag.
int32_t sql_result_cmp(FILE* input, FILE* output)
{
  int32_t ret = 0;
  char  input_str[OB_MAX_SQL_LENGTH];
  char  output_str[OB_MAX_SQL_LENGTH];

  assert(input && output);
  while (true)
  {
    char* in_str = fgets(input_str, OB_MAX_SQL_LENGTH, input);
    char* out_str = fgets(output_str, OB_MAX_SQL_LENGTH, output);
    // skip line begin with patten like "[2012-07-17 15:19:16]"
    while (in_str && strlen(in_str) > 21
        && *in_str == '['
        && isdigit(*(in_str + 1))
        && isdigit(*(in_str + 2))
        && isdigit(*(in_str + 3))
        && isdigit(*(in_str + 4))
        && isdigit(*(in_str + 4))
        && *(in_str + 5) == '-'
        && isdigit(*(in_str + 6))
        && isdigit(*(in_str + 7))
        && *(in_str + 8) == '-'
        && isdigit(*(in_str + 9))
        && isdigit(*(in_str + 10))
        && *(in_str + 11) == ' '
        && isdigit(*(in_str + 12))
        && isdigit(*(in_str +13))
        && *(in_str + 14) == ':'
        && isdigit(*(in_str + 15))
        && isdigit(*(in_str + 16))
        && *(in_str + 17) == ':'
        && isdigit(*(in_str + 18))
        && isdigit(*(in_str + 19))
        && *(in_str + 20) == ']')
      in_str = fgets(input_str, OB_MAX_SQL_LENGTH, input);
    while (out_str && strlen(in_str) > 21
        && *in_str == '['
        && isdigit(*(in_str + 1))
        && isdigit(*(in_str + 2))
        && isdigit(*(in_str + 3))
        && isdigit(*(in_str + 4))
        && isdigit(*(in_str + 4))
        && *(in_str + 5) == '-'
        && isdigit(*(in_str + 6))
        && isdigit(*(in_str + 7))
        && *(in_str + 8) == '-'
        && isdigit(*(in_str + 9))
        && isdigit(*(in_str + 10))
        && *(in_str + 11) == ' '
        && isdigit(*(in_str + 12))
        && isdigit(*(in_str +13))
        && *(in_str + 14) == ':'
        && isdigit(*(in_str + 15))
        && isdigit(*(in_str + 16))
        && *(in_str + 17) == ':'
        && isdigit(*(in_str + 18))
        && isdigit(*(in_str + 19))
        && *(in_str + 20) == ']')
      out_str = fgets(output_str, OB_MAX_SQL_LENGTH, output);
    if (in_str && out_str)
    {
      if ((ret = strncmp(input_str, output_str, OB_MAX_SQL_LENGTH)) != 0)
      {
        return ret;
      }
    }
    else
      return ((in_str ? 1 : 0) - (out_str ? 1 : 0));
  }
  return 0;
}

int32_t cmp_sql_results(const char* file_name)
{
  char    sql_string[OB_MAX_SQL_LENGTH];
  int32_t ret = 0;  // ok
  FILE*   sql_fd = NULL;
  FILE*   input = NULL;
  FILE*   output = NULL;
  char    cwd[OB_MAX_FILE_NAME_LENGTH];
  char    cmd[OB_MAX_FETCH_CMD_LENGTH];
  memset(cwd, 0, OB_MAX_FILE_NAME_LENGTH);
  memset(cmd, 0, OB_MAX_FILE_NAME_LENGTH);

  if ((sql_fd = fopen(file_name, "r")) == NULL)
  {
    TBSYS_LOG(ERROR, "can not open sql file");
    ret = -1;
    goto errExit;
  }
  if (NULL == getcwd(cwd, OB_MAX_FILE_NAME_LENGTH))
  {
    TBSYS_LOG(ERROR, "getcwd error[%s]", strerror(errno));
    ret = -1;
    goto errExit;
  }
  memset(cmd, 0, OB_MAX_FILE_NAME_LENGTH);
  snprintf(cmd, OB_MAX_FETCH_CMD_LENGTH, "%s/%s", cwd, STR_SQL_TEST);
  if (chdir(cmd))
  {
    TBSYS_LOG(ERROR, "chdir error[%s]", strerror(errno));
    ret = -1;
    goto errExit;
  }

  while (fgets(sql_string, OB_MAX_SQL_LENGTH, sql_fd) != NULL)
  {
    char* ptr = sql_string;
    ptr = ltrim(ptr);
    ptr = rtrim(ptr);
    if (strncmp(ptr, "@@", 2) == 0)
    {
      memset(cmd, 0, OB_MAX_FILE_NAME_LENGTH);
      snprintf(cmd, OB_MAX_FETCH_CMD_LENGTH, "%s/%s/%s.input", cwd, STR_SQL_TEST, ltrim(ptr + 2));
      if ((input = fopen(cmd, "r")) == NULL)
      {
        TBSYS_LOG(ERROR, "open %s error", cmd);
        ret = -1;
        goto errExit;
      }
      memset(cmd, 0, OB_MAX_FILE_NAME_LENGTH);
      snprintf(cmd, OB_MAX_FETCH_CMD_LENGTH, "%s/%s/%s.output", cwd, STR_SQL_TEST, ltrim(ptr + 2));
      if ((output = fopen(cmd, "r")) == NULL)
      {
        TBSYS_LOG(ERROR, "open %s error", cmd);
        ret = -1;
        goto errExit;
      }

      EXPECT_TRUE(input != NULL);
      EXPECT_TRUE(output != NULL);
      if (input && output)
        EXPECT_EQ(0, sql_result_cmp(input, output));

      if (input)
      {
        fclose(input);
        input = NULL;
      }
      if (output)
      {
        fclose(output);
        output = NULL;
      }
    }
  }

errExit:
  if (sql_fd)
    fclose(sql_fd);
  if (input)
    fclose(input);
  if (output)
    fclose(output);
  if (strlen(cwd) != 0)
    chdir(cwd);
  return ret;
}



TEST(ObSqlParserTest, physical_check)
{
  run_sql_test_file(STR_SQL_FILE);
  ASSERT_EQ(0, cmp_sql_results(STR_SQL_FILE));
  fprintf(stderr, "Compare sql results successfully!\n");
}

int main(int argc, char **argv)
{
  ob_init_memory_pool();
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
