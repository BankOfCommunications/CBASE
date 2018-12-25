#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "oceanbase.h"

const int MAX_DISPLAY_ROW_KEY_LEN = 48;

char* get_display_row_key(const char* row_key, int64_t row_key_len)
{
  const int aux_len = 2;
  const int ending_len = 1;
  static char dis_row_key[MAX_DISPLAY_ROW_KEY_LEN + aux_len + ending_len];
  int64_t dis_row_key_len = row_key_len <= MAX_DISPLAY_ROW_KEY_LEN / 2 ?
                            row_key_len : MAX_DISPLAY_ROW_KEY_LEN / 2;
  int64_t dis_len = dis_row_key_len * 2;
  dis_row_key[0] = '0';
  dis_row_key[1] = 'x';
  int i = 0;
  for (i = 0; i < dis_row_key_len; i++)
  {
    snprintf(dis_row_key + i * 2 + aux_len, 3, "%02x", (unsigned char)row_key[i]);
  }
  dis_row_key[dis_len + aux_len] = '\0';
  return dis_row_key;
}

char* rowkey2hex(const char* row_key, int64_t row_key_len)
{
  static char* hex = NULL;
  static int64_t hex_len = 0;
  if (NULL == hex || hex_len <= row_key_len)
  {
    if (NULL != hex && hex_len <= row_key_len)
    {
      free(hex);
      hex = NULL;
    }
    hex = (char*)malloc(row_key_len + 1);
    if (NULL != hex)
    {
      hex_len = row_key_len + 1;
    }
  }
  if (NULL != hex)
  {
    snprintf(hex, hex_len, "%.*x", row_key_len, row_key);
  }

  return hex;
}

char* trim_newline(char* s)
{
  int s_len = strlen(s);
  while (s_len > 0 && s[s_len - 1] == '\n')
  {
    s[s_len - 1] = '\0';
    s_len--;
  }
  return s;
}

void pcell(OB_CELL* cell)
{
  printf("%ld", cell->table_len);
  printf("%.*s\t%s(%ld)\t%.*s\t",
         (int)cell->table_len, cell->table,
         rowkey2hex(cell->row_key, cell->row_key_len), cell->row_key_len,
         (int)cell->column_len, cell->column);
  switch (cell->v.type)
  {
    case OB_INT_TYPE:
      printf("%ld", cell->v.v.v_int);
      break;
    //add lijianqiang [INT_32] 20150930:b
    case OB_INT32_TYPE:
      printf("%d", cell->v.v.v_int32);
      break;
    //add 20150930:e
    case OB_VARCHAR_TYPE:
      printf("%.*s", cell->v.v.v_varchar.len, cell->v.v.v_varchar.p);
      break;
    //add peiouya [DATE_TIME] 20150906:b
    case OB_DATE_TYPE:
    case OB_TIME_TYPE:
    //add 20150906:e
    case OB_DATETIME_TYPE:
      printf("\t%s(%d.%d)", trim_newline(ctime(&(cell->v.v.v_datetime.tv_sec))),
          cell->v.v.v_datetime.tv_sec, cell->v.v.v_datetime.tv_usec);
      break;
  }
  if (cell->is_null || cell->is_row_not_exist || cell->is_row_changed)
  {
    printf("(%s%s%s)\n",
           cell->is_null ? "NULL " : "",
           cell->is_row_not_exist ? "ROW NOT EXIST " : "",
         cell->is_row_changed ? "NEW ROW" : "");
  }
  else
  {
    printf("\n");
  }
}

void pcolumnname(OB_RES* res)
{
  OB_CELL* cell = NULL;
  cell = ob_fetch_cell(res);
  if (NULL != cell)
  {
    printf("\t\t%.*s", cell->column_len, cell->column);
  }
  while ((cell = ob_fetch_cell(res)) != NULL && !cell->is_row_changed)
  {
    printf("\t%.*s", cell->column_len, cell->column);
  }
}

void pres(OB_RES* res)
{
  pcolumnname(res);
  printf("\n");
  ob_res_seek_to_begin_cell(res);

  OB_ROW* row = NULL;
  while ((row = ob_fetch_row(res)) != NULL)
  {
    int64_t i = 0;
    printf("%s", get_display_row_key(row->cell[0].row_key, row->cell[0].row_key_len));
    for (; i < row->cell_num; i++)
    {
      switch (row->cell[i].v.type)
      {
        case OB_INT_TYPE:
          printf("\t%ld", row->cell[i].v.v.v_int);
          break;
        //add lijianqiang [INT_32] 20150930:b
        case OB_INT32_TYPE:
          printf("\t%d", row->cell[i].v.v.v_int32);
          break;
        //add 20150930:e
        case OB_VARCHAR_TYPE:
          printf("\t%.*s", row->cell[i].v.v.v_varchar.len, row->cell[i].v.v.v_varchar.p);
          break;
        //add peiouya [DATE_TIME] 20150906:b
        case OB_DATE_TYPE:
        case OB_TIME_TYPE:
        //add 20150906:e
        case OB_DATETIME_TYPE:
          printf("\t%s(%d.%d)", trim_newline(ctime(&(row->cell[i].v.v.v_datetime.tv_sec))),
              row->cell[i].v.v.v_datetime.tv_sec, row->cell[i].v.v.v_datetime.tv_usec);
          break;
      }
    }
    printf("\n");
  }
//  OB_CELL* cell = NULL;
//  while ((cell = ob_fetch_cell(res)) != NULL)
//  {
//    if (cell->is_row_changed)
//    {
//      printf("\n");
//      printf("%s", get_display_row_key(cell->row_key, cell->row_key_len));
//    }
//    switch (cell->v.type)
//    {
//      case OB_INT_TYPE:
//        printf("\t%ld", cell->v.v.v_int);
//        break;
//      case OB_VARCHAR_TYPE:
//        printf("\t%.*s", cell->v.v.v_varchar.len, cell->v.v.v_varchar.p);
//        break;
//      case OB_DATETIME_TYPE:
//        printf("\t%d.%d", cell->v.v.v_datetime.tv_sec, cell->v.v.v_datetime.tv_usec);
//        break;
//    }
//  }
}

#if 0
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
#endif

int main(int argc, char **argv)
{
  /// select nick,auc_cnt_max_cnt,prov from test_seller_wm_4 
  /// where /*+ rowkey, issue_id:6,cat_id:8,selr_user_id:8 */ issue_id = '201145' and cat_id=16 
  /// and nick='lovinlovin3' and prov='shanghai3' limit 2 offset 0;
/*
  char* table = "collect_item";
  char* column1 = "item_collector_count";
  char* column2 = "item_collect_count";
  char* rowkey1 = "AAAAAAAAA";
  char* rowkey2 = "AAAAAAAAF";
*/

  char* table = "test_seller_wm_4";
  char* column1 = "nick";
  char* column2 = "prov";
  char* column3 = "auc_cnt_max_cnt";
  char rowkey1[44];
  char rowkey2[44];
  OB_ERR_CODE err = OB_ERR_SUCCESS;

  memset(rowkey1, 0x00, 44);
  memset(rowkey2, 0x00, 44);
  
  memcpy(&rowkey1[0], "201145", 6);
  rowkey1[8+6-1] = 16;
  
  memcpy(&rowkey2[0], "201145", 6);
  rowkey2[8+6-1] = 16;
  memset(&rowkey2[14], 0xff, 8);
  
  
  OB* ob = ob_api_init();
  if (NULL == ob)
  {
    fprintf(stderr, "ob_init error: %s\n", ob_error());
    return 1;
  }

  ob_api_debug_log(ob, "DEBUG", NULL);

  if (argc > 2)
  {
    fprintf(stderr, "connection param used: %s:%s\n", argv[1], argv[2]);
    char *rs_ip = argv[1];
    int rs_port = atoi(argv[2]);
    // 连接OceanBase
    err = ob_connect(ob, rs_ip, rs_port, NULL, NULL);
  }
  else
  {
    // 连接OceanBase
    err = ob_connect(ob, "172.24.131.234", 5433, NULL, NULL);
  }
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
      memset(rowkey1, 0x00, 22);
      memset(rowkey2, 0xff, 22);
      ob_scan(scan_st, table, rowkey1, 22, 1, rowkey2, 22, 1);
      ob_scan_column(scan_st, "nick", 1);
      ob_scan_column(scan_st, "prov", 1);
      //ob_scan_complex_column(scan_st, "1=1", "_expr0", 0);
      //ob_scan_complex_column(scan_st, "2 < 1", "_expr1", 0);
      ob_scan_complex_column(scan_st, "nick='lovinlovin'", "_expr0", 0);
      ob_scan_complex_column(scan_st, "`prov`='shanghai'", "_expr1", 0);
      //ob_scan_complex_column(scan_st, "3+4", "_expr2", 0);
      
      //ob_scan_complex_column(scan_st, "nick='songyi0209'", "_expr1", 0);
      
      //ob_scan_set_where(scan_st, "nick='lovinlovin' and prov='shanghai'");
      //ob_scan_set_where(scan_st, "_expr0 or nick='songyi0209'");
      if (argc > 3)
      {
      }
      else
      {
        //ob_scan_set_where(scan_st, "`_expr0` = `_expr0` and `prov`='beijing'");
        fprintf(stderr, "++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n");
        //ob_scan_set_where(scan_st, "_expr2 < 10 and (_expr1 or _expr0)");
        ob_scan_set_where(scan_st, "_expr1 and _expr0");
      }
      //ob_scan_set_where(scan_st, "_expr0 = _expr1");
      //ob_scan_set_where(scan_st, "nick='lovinlovin'");
      //ob_scan_column(scan_st, column2, 1);
      //ob_scan_column(scan_st, column3, 1);

      ob_scan_set_limit(scan_st,0, 20);
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

          pres(res_st);
#if 0
          OB_CELL* cell = NULL;
          while ((cell = ob_fetch_cell(res_st)) != NULL)
          {
            pcell(cell);
          }
#endif          
        }
        ob_release_res_st(ob, res_st);
      }
    ob_release_scan_st(ob, scan_st);
    }
  }

  ob_api_destroy(ob);

  return 0;
}
