#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include "oceanbase.h"

int main()
{
  OB_REQ* req = NULL;
  int64_t num = 0;

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

  // 连接OceanBase
  OB_ERR_CODE err = ob_connect(ob, "10.232.36.33", 11010, NULL, NULL);
  if (OB_ERR_SUCCESS != err)
  {
    fprintf(stderr, "ob_connect error: %s\n", ob_error());
  }
  else
  {
    // 获得OB_REQ结构体
    req = ob_acquire_set_req(ob);
    // 用户自定义请求id
    req->req_id = 1;

    ob_insert_int(req->ob_set, table, rowkey1, strlen(rowkey1), column1, 1);
    ob_insert_int(req->ob_set, table, rowkey1, strlen(rowkey1), column2, 3);
    ob_insert_int(req->ob_set, table, rowkey2, strlen(rowkey2), column1, 1);
    ob_insert_int(req->ob_set, table, rowkey2, strlen(rowkey2), column2, 4);

    if (OB_ERR_SUCCESS != ob_errno())
    {
      fprintf(stderr, "insert error: %s\n", ob_error());
    }
    else
    {
      err = ob_submit(ob, req);
      if (OB_ERR_SUCCESS != err)
      {
        fprintf(stderr, "ob_submit error: %s\n", ob_error());
      }
      else
      {
        err = ob_get_results(ob, 1, 1, 1000000, &num, &req);

        if (OB_ERR_SUCCESS == err && req->err_code == OB_ERR_SUCCESS)
        {
          printf("set succ\n");
        }
        else
        {
          fprintf(stderr, "set failed: %s\n", ob_error());
        }
      }
    }

    ob_release_req(ob, req, 1);
  }

  ob_api_destroy(ob);

  return 0;
}
