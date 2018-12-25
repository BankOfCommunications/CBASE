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
  char* table = "collect_item";
  char* column1 = "item_collector_count";
  char* column2 = "item_collect_count";
  char* rowkey1 = "AAAAAAAAA";
  char* rowkey2 = "AAAAAAAAF";

  //memset(rowkey1, 0x00, 9);
  OB* ob = ob_api_init();
  if (NULL == ob)
  {
    fprintf(stderr, "ob_init error: %s\n", ob_error());
    return 1;
  }

  ob_api_debug_log(ob, "DEBUG", NULL);

  // 连接OceanBase
  OB_ERR_CODE err = ob_connect(ob, "10.232.36.33", 11010, NULL, NULL);
  if (OB_ERR_SUCCESS != err)
  {
    fprintf(stderr, "ob_connect error: %s\n", ob_error());
  }
  else
  {
    // 获得OB_SCAN结构体
    OB_SCAN* scan_st = ob_acquire_scan_st(ob);
    if (NULL == scan_st)
    {
      fprintf(stderr, "ob_acquire_scan_st error: %s\n", ob_error());
    }
    else
    {
      ob_scan(scan_st, table, rowkey1, 9, 1, rowkey2, strlen(rowkey2), 1);
      ob_scan_column(scan_st, column1, 1);
      ob_scan_column(scan_st, column2, 1);

      if (OB_ERR_SUCCESS != ob_errno())
      {
        fprintf(stderr, "ob_scan error: %s\n", ob_error());
      }
      else
      {
        OB_RES* res_st = ob_exec_scan(ob, scan_st);
        if (NULL == res_st)
        {
          fprintf(stderr, "ob_exec_scan error(%d): %s\n", ob_errno(), ob_error());
        }
        else
        {
          printf("scan succ: row_num=%ld\n", ob_fetch_row_num(res_st));

          OB_CELL* cell = NULL;
          while ((cell = ob_fetch_cell(res_st)) != NULL)
          {
            pcell(cell);
          }
        }
        ob_release_res_st(ob, res_st);
      }
    ob_release_scan_st(ob, scan_st);
    }
  }

  ob_api_destroy(ob);

  return 0;
}
