#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include "oceanbase.h"

int main()
{
  char* table = "collect_item";
  char* column1 = "item_collector_count";
  char* column2 = "item_collect_count";
  char* rowkey1 = "AAAAAAAAA";
  char* rowkey2 = "AAAAAAAAB";

  OB* ob = ob_api_init();
  if (NULL == ob)
  {
    fprintf(stderr, "ob_init error: %s\n", ob_error());
    return 1;
  }

  ob_api_debug_log(ob, "INFO", NULL);

  // 连接OceanBase
  OB_ERR_CODE err = ob_connect(ob, "10.232.36.33", 11010, NULL, NULL);
  if (OB_ERR_SUCCESS != err)
  {
    fprintf(stderr, "ob_connect error: %s\n", ob_error());
  }
  else
  {
    // 获得OB_REQ结构体
    OB_SET* set_st = ob_acquire_set_st(ob);
    if (NULL == set_st)
    {
      fprintf(stderr, "ob_acquire_set_st error: %s\n", ob_error());
    }
    else
    {
      ob_insert_int(set_st, table, rowkey1, strlen(rowkey1), column1, 1);
      ob_insert_int(set_st, table, rowkey1, strlen(rowkey1), column2, 3);
      ob_insert_int(set_st, table, rowkey2, strlen(rowkey2), column1, 1);
      ob_insert_int(set_st, table, rowkey2, strlen(rowkey2), column2, 4);

      if (OB_ERR_SUCCESS != ob_errno())
      {
        fprintf(stderr, "insert error: %s\n", ob_error());
      }
      else
      {
        int32_t t = 0;
        OB_ERR_CODE err = ob_exec_set(ob, set_st);
        if (OB_ERR_SUCCESS != err)
        {
          fprintf(stderr, "set failed: %s\n", ob_error());
        }
        else
        {
          printf("set succ\n");
        }
      }

      ob_release_set_st(ob, set_st);
    }
  }

  ob_api_destroy(ob);

  return 0;
}
