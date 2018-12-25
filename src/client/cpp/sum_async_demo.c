#include <stdio.h>
#include <string.h>
#include "oceanbase.h"

void pcell(OB_CELL* cell)
{
  printf("%.*s\t%.*s\t%.*s\t%ld(%s%s%s)\n",
         (int)cell->table_len, cell->table,
         (int)cell->row_key_len, cell->row_key,
         (int)cell->column_len, cell->column,
         cell->v.v.v_int,
         cell->is_null ? "NULL " : "",
         cell->is_row_not_exist ? "ROW NOT EXIST " : "",
         cell->is_row_changed ? "NEW ROW" : "");
}

int main()
{
  OB_REQ* req = NULL;
  int64_t num = 0;

  char* table = "collect_item";
  char* column1 = "item_collector_count";
  char* column2 = "item_collect_count";
  char* rowkey1 = "AAAAAAAAA";
  char* rowkey2 = "AAAAAAAAF";

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
    req = ob_acquire_scan_req(ob);
    // 用户自定义请求id
    req->req_id = 1;

    ob_scan(req->ob_scan, table, rowkey1, strlen(rowkey1), 1,
        rowkey2, strlen(rowkey2), 1);

    ob_scan_column(req->ob_scan, column1, 1);
    ob_scan_column(req->ob_scan, column2, 1);

    OB_GROUPBY_PARAM* groupby = ob_get_ob_groupby_param(req->ob_scan);
    if (NULL != groupby)
    {
      ob_groupby_column(groupby, column1, 1);
      ob_aggregate_column(groupby, OB_SUM, column2, "SUM", 1);
      ob_scan_orderby_column(req->ob_scan, column1, OB_ASC);

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
            printf("scan succ: row_num=%ld\n", ob_fetch_row_num(req->ob_res));

            OB_CELL* cell = NULL;
            while ((cell = ob_fetch_cell(req->ob_res)) != NULL)
            {
              //printf("%ld\n", cell->v.v.v_int);
              pcell(cell);
            }
          }
          else
          {
            fprintf(stderr, "scan failed: %s\n", ob_error());
          }
        }
      }
    }

    ob_release_req(ob, req, 1);
  }

  ob_api_destroy(ob);

  return 0;
}
