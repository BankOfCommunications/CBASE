#include <gtest/gtest.h>
#include <unistd.h>
#include "common/ob_define.h"
#include "common/ob_string_buf.h"
#include "common/ob_malloc.h"
#include "common/utility.h"
#include "sql/parse_node.h"
#include "sql/build_plan.h"
#include "sql/ob_multi_logic_plan.h"
#include "sql/ob_schema_checker.h"

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::sql;

const char* STR_SQL_TEST = "SQL_TEST";
const char* STR_SQL_FILE = "sql_file.sql";


// NOTE about sql_file.sql
// 1. while line && line begin with "--" will be ignored
// 2. @@[XXX] wile be filename XXX.input/XXX.output
// 3. sql must in one line
int32_t runSqlTestFile(const char* pFileName)
{
  char    sqlString[OB_MAX_SQL_LENGTH];
  int32_t ret = 0;  // ok
  FILE*   sqlFd = NULL;
  FILE*   pCurFd = NULL;
  char    cwd[OB_MAX_FILE_NAME_LENGTH];
  char    cmd[OB_MAX_FETCH_CMD_LENGTH];
  memset(cwd, 0, OB_MAX_FILE_NAME_LENGTH);
  memset(cmd, 0, OB_MAX_FILE_NAME_LENGTH);
  oceanbase::common::ObStringBuf obStringBuf;
  ObSchemaChecker schema_checker;

  // It is for test only, no more questions about this usage.
  if (tbsys::CConfig::getCConfig().load("MyTestSchema.ini") == EXIT_FAILURE)
  {
    TBSYS_LOG(ERROR, "load schema error");
    ret = -1;
    goto errExit;
  }
  if ((sqlFd = fopen(pFileName, "r")) == NULL)
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
  snprintf(cmd, OB_MAX_FETCH_CMD_LENGTH, "mkdir -p %s/%s", cwd, STR_SQL_TEST);
  system(cmd);
  memset(cmd, 0, OB_MAX_FILE_NAME_LENGTH);
  snprintf(cmd, OB_MAX_FETCH_CMD_LENGTH, "%s/%s", cwd, STR_SQL_TEST);
  if (chdir(cmd))
  {
    TBSYS_LOG(ERROR, "chdir error[%s]", strerror(errno));
    ret = -1;
    goto errExit;
  }

  ParseResult result;
  result.malloc_pool_ = &obStringBuf;
  if(parse_init(&result))
  {
    TBSYS_LOG(ERROR, "parse_init error!!!");
    ret = -1;
    goto errExit;
  }

  while (fgets(sqlString, OB_MAX_SQL_LENGTH, sqlFd) != NULL)
  {
    char* ptr = sqlString;
    ptr = ltrim(ptr);
    ptr = rtrim(ptr);
    if (strlen(ptr) == 0 || strncmp(ptr, "--", 2) == 0)
    {
      fprintf(stderr, "%s\n", sqlString);
      continue;
    }
    if (strncmp(ptr, "@@", 2) == 0)
    {
      if (pCurFd)
      {
        fclose(pCurFd);
        pCurFd = NULL;
      }
      memset(cmd, 0, OB_MAX_FILE_NAME_LENGTH);
      snprintf(cmd, OB_MAX_FETCH_CMD_LENGTH, "%s/%s/%s.output", cwd, STR_SQL_TEST, ltrim(ptr + 2));
      if ((pCurFd = freopen(cmd, "w", stderr)) == NULL)
      {
        TBSYS_LOG(ERROR, "open %s error", cmd);
        ret = -1;
        goto errExit;
      }
      continue;
    }

    // parse sql
    fprintf(stderr, "%s\n", sqlString);
    parse_sql(&result, ptr, strlen(ptr));
    fflush(stderr);
    if(result.result_tree_ == 0)
    {
      fprintf(stderr, "parse_sql error!\n");
      fprintf(stderr, "%s at %d:%d\n", result.error_msg_, result.line_, result.start_col_);
      continue;
    }

    ResultPlan resultPlan;
    resultPlan.name_pool_ = &obStringBuf;
    resultPlan.schema_checker_ = &schema_checker;
    resultPlan.plan_tree_ = NULL;
    if (resolve(&resultPlan, result.result_tree_) == OB_SUCCESS)
    {
      ObMultiLogicPlan* pMultiPlan = static_cast<ObMultiLogicPlan*>(resultPlan.plan_tree_);
      pMultiPlan->print();
    }
    else
    {
      fprintf(stderr, "%s\n", resultPlan.err_stat_.err_msg_);
      fprintf(stderr, "Resolve error!\n");
    }
    destroy_plan(&resultPlan);

    if (result.result_tree_)
    {
      destroy_tree(result.result_tree_);
      result.result_tree_ = NULL;
    }
  }

errExit:
  if (sqlFd)
    fclose(sqlFd);
  if (pCurFd)
    fclose(pCurFd);
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
int32_t sqlResultCmp(FILE* pInput, FILE* pOutput)
{
  int32_t ret = 0;
  char  inputStr[OB_MAX_SQL_LENGTH];
  char  outputStr[OB_MAX_SQL_LENGTH];

  assert(pInput && pOutput);
  while (true)
  {
    char* pInStr = fgets(inputStr, OB_MAX_SQL_LENGTH, pInput);
    char* pOutStr = fgets(outputStr, OB_MAX_SQL_LENGTH, pOutput);
    while (pInStr && strstr(inputStr, "ob_tsi_factory.h"))
      pInStr = fgets(inputStr, OB_MAX_SQL_LENGTH, pInput);
    while (pOutStr && strstr(outputStr, "ob_tsi_factory.h"))
      pOutStr = fgets(outputStr, OB_MAX_SQL_LENGTH, pOutput);
    if (pInStr && pOutStr)
    {
      if ((ret = strncmp(inputStr, outputStr, OB_MAX_SQL_LENGTH)) != 0)
      {
        char* pInMsg = strrchr(inputStr, ']');
        char* pOutMsg = strrchr(outputStr, ']');
        if (pInMsg && pOutMsg && (ret = strcmp(pInMsg + 1, pOutMsg + 1)) == 0)
          continue;
        else
          return ret;
      }
    }
    else
      return ((pInStr ? 1 : 0) - (pOutStr ? 1 : 0));
  }
  return 0;
}

int32_t cmpSqlResults(const char* pFileName)
{
  char    sqlString[OB_MAX_SQL_LENGTH];
  int32_t ret = 0;  // ok
  FILE*   sqlFd = NULL;
  FILE*   pInput = NULL;
  FILE*   pOutput = NULL;
  char    cwd[OB_MAX_FILE_NAME_LENGTH];
  char    cmd[OB_MAX_FETCH_CMD_LENGTH];
  memset(cwd, 0, OB_MAX_FILE_NAME_LENGTH);
  memset(cmd, 0, OB_MAX_FILE_NAME_LENGTH);

  if ((sqlFd = fopen(pFileName, "r")) == NULL)
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

  while (fgets(sqlString, OB_MAX_SQL_LENGTH, sqlFd) != NULL)
  {
    char* ptr = sqlString;
    ptr = ltrim(ptr);
    ptr = rtrim(ptr);
    if (strncmp(ptr, "@@", 2) == 0)
    {
      memset(cmd, 0, OB_MAX_FILE_NAME_LENGTH);
      snprintf(cmd, OB_MAX_FETCH_CMD_LENGTH, "%s/%s/%s.input", cwd, STR_SQL_TEST, ltrim(ptr + 2));
      if ((pInput = fopen(cmd, "r")) == NULL)
      {
        TBSYS_LOG(ERROR, "open %s error", cmd);
        ret = -1;
        goto errExit;
      }
      memset(cmd, 0, OB_MAX_FILE_NAME_LENGTH);
      snprintf(cmd, OB_MAX_FETCH_CMD_LENGTH, "%s/%s/%s.output", cwd, STR_SQL_TEST, ltrim(ptr + 2));
      if ((pOutput = fopen(cmd, "r")) == NULL)
      {
        TBSYS_LOG(ERROR, "open %s error", cmd);
        ret = -1;
        goto errExit;
      }

      EXPECT_TRUE(pInput != NULL);
      EXPECT_TRUE(pOutput != NULL);
      if (pInput && pOutput)
        EXPECT_EQ(0, sqlResultCmp(pInput, pOutput));

      if (pInput)
      {
        fclose(pInput);
        pInput = NULL;
      }
      if (pOutput)
      {
        fclose(pOutput);
        pOutput = NULL;
      }
    }
  }

errExit:
  if (sqlFd)
    fclose(sqlFd);
  if (pInput)
    fclose(pInput);
  if (pOutput)
    fclose(pOutput);
  if (strlen(cwd) != 0)
    chdir(cwd);
  return ret;
}



TEST(ObSqlParserTest, semantic_check)
{
  runSqlTestFile(STR_SQL_FILE);
  ASSERT_EQ(0, cmpSqlResults(STR_SQL_FILE));
  fprintf(stderr, "Compare sql results successfully!\n");
}

int main(int argc, char **argv)
{
  ob_init_memory_pool();
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
