#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include "sql/parse_node.h"
#include "sql/parse_malloc.h"
#include "common/ob_string_buf.h"
#include "common/ob_malloc.h"

using namespace oceanbase::common;

const int64_t MUTI_THREAD_NUM = 50;
//#define THREAD_CIRCLE_TIMES 10000000
const int64_t THREAD_CIRCLE_TIMES = 10000;

struct sqlsAndResultsNode
{
  int num;
  const char** pszSqls;
  ParseNode**  pResults;
};

typedef struct sqlsAndResultsNode SqlsAndResultsNode;

int nodeTreeCompare(ParseNode* pNode1, ParseNode* pNode2)
{
  int ret = 0;
  
  if (pNode1->type_ != pNode2->type_)
    return pNode1->type_ - pNode2->type_;

  if (pNode1->value_ != pNode2->value_)
    return (int)(pNode1->value_ - pNode2->value_);

  switch (pNode1->type_)
  {
    case T_STRING:
    case T_BINARY:
      if ((ret = strncmp(pNode1->str_value_, pNode2->str_value_, pNode1->value_)) != 0)
        return ret;
      break;
    default:
      if (pNode1->str_value_ == NULL && pNode2->str_value_ == NULL)
        break;
      else if (pNode1->str_value_ == NULL)
        return -1;
      else if (pNode2->str_value_ == NULL)
        return 1;
      
      if ((ret = strcmp(pNode1->str_value_, pNode2->str_value_)) != 0)
        return ret;
      break;
  }
  
  if (pNode1->num_child_ != pNode2->num_child_)
    return pNode1->num_child_ - pNode2->num_child_;
  
  for (int i = 0; i < pNode1->num_child_; i++)
  {
    if (pNode1->children_[i] == NULL && pNode2->children_[i] == NULL)
      continue;
    else if(pNode1->children_[i] == NULL)
      return -1;
    else if(pNode2->children_[i] == NULL)
      return 1;
  
    if ((ret = nodeTreeCompare(pNode1->children_[i], pNode1->children_[i])) != 0)
      return ret;
  }

  return ret;
}

void *singThreadWork(void* arg)
{
  SqlsAndResultsNode* pSqlsAndResults = static_cast<SqlsAndResultsNode*>(arg);
  int index;

  srand((uint32_t)time(NULL));

  oceanbase::common::ObStringBuf str_buf; 
  ParseResult result;
  result.malloc_pool_ = &str_buf;
  if(parse_init(&result))
  {
    printf("Thread %ld, parse_init error!\n", (unsigned long int)pthread_self());
    fflush(stdout);
    pthread_exit(NULL);
  }

  for(int i = 0; i < THREAD_CIRCLE_TIMES; ++i)
  {
    index = rand() % (pSqlsAndResults->num);
    const char* sqlStr = pSqlsAndResults->pszSqls[index];
    ParseNode* node = pSqlsAndResults->pResults[index];

    parse_sql(&result, sqlStr, strlen(sqlStr));
    if(result.result_tree_ == 0)
    {
      printf("Thread %ld, syntax error!\n", (unsigned long int)pthread_self());
      printf("Thread %ld, SQL: %s\n", (unsigned long int)pthread_self(), sqlStr);
      fflush(stdout);
      continue;
    }
    
    if (nodeTreeCompare(result.result_tree_, node) != 0)
    {
      printf("Thread %ld, ERROR: compare parse tree error!!!\n", 
        (unsigned long int)pthread_self());
      printf("Thread %ld, \nTHREAD TREE:\n", (unsigned long int)pthread_self());
      print_tree(result.result_tree_, 0);
      printf("Thread %ld, \nSTANDARD TREE:\n", (unsigned long int)pthread_self());
      print_tree(node, 0);

      exit(-1);
    }

    if (result.result_tree_)
    {
      destroy_tree(result.result_tree_);
      result.result_tree_ = NULL;
    }
  }

  parse_terminate(&result);
  return (void*) NULL;
}

int checkMutiThreadSafe(SqlsAndResultsNode& sqlsAndResults)
{
  int ret = 0;

  assert(sqlsAndResults.num >= 1);

  pthread_t tids[MUTI_THREAD_NUM];

  for (int i = 0; i < MUTI_THREAD_NUM; i++)
  {
    if (pthread_create(&(tids[i]), NULL, singThreadWork, &sqlsAndResults) != 0)
    {
      printf("Create %dth thread error", i + 1);
      return -1;
    }
  }

  for (int j = 0; j < MUTI_THREAD_NUM; j++)
  {
    pthread_join(tids[j], NULL);
  }

  return ret;
}

int main(void)
{
  oceanbase::common::ob_init_memory_pool();
  const char* pszSql[] = {
    "select id as a from tab1 group by name;",
    "select sum(k) as a from tab2 group by i,j;",
    "select c1, c2, c3 from tab3 where c1 > 500;",
    "select sum(k) as a from tab4 group by i,j having a < 67;",
    "select * from tab5 order by userid limit 10 offset 500;",
    "select col1, col2 from tab6 where col1 < 100 and col2 > 50;",
    "select column1, column2 from tab7 order by column3;",
    "select sum(k) from tab8 where j like '777777';",
  };
  int num = sizeof(pszSql)/sizeof(pszSql[0]);
  SqlsAndResultsNode sqlsAndResults;
  oceanbase::common::ObStringBuf stringBuf;

  /* Step 1: Allocte space to record SQLs and results */
  sqlsAndResults.num = 0;
  sqlsAndResults.pszSqls = pszSql;
  sqlsAndResults.pResults = (ParseNode**)malloc(sizeof(ParseNode*) * num);

  /* Step 2: Using single thread to generate serveral trees, and store the by sqlsAndResults */
  ParseResult result;
  result.malloc_pool_ = &stringBuf;
  if(parse_init(&result))
  {
    return 1;
  }

  for(int i = 0; i < num; ++i)
  {
    parse_sql(&result, pszSql[i], strlen(pszSql[i]));
    if(result.result_tree_ == 0)
    {
      printf("%s\n", result.error_msg_);
      printf("%s\n", pszSql[i]);

      continue;
    }
    printf("\nSQL%d: %s\n", i + 1, pszSql[i]);
    print_tree(result.result_tree_, 0);
    sqlsAndResults.pResults[sqlsAndResults.num++] = result.result_tree_;
    result.result_tree_ = NULL;
  }

  parse_terminate(&result);

  /* Step 3: Run multiple thread, and compare the results with former */
  checkMutiThreadSafe(sqlsAndResults);

  /* Step 4: Free spaces */
  for (int i = 0; i < num; i++)
  {
    if (sqlsAndResults.pResults[i])
      destroy_tree(sqlsAndResults.pResults[i]);
  }
  parse_free(sqlsAndResults.pResults);

  printf("Multiple thread test run successfully!!!\n");
  return 0;
}

