#include <stdio.h>
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

void pres(OB_RES* res)
{
  OB_CELL* cell = NULL;

  pcolumnname(res);
  ob_res_seek_to_begin_cell(res);
  while ((cell = ob_fetch_cell(res)) != NULL)
  {
    if (cell->is_row_changed)
    {
      printf("\n");
      printf("%s", get_display_row_key(cell->row_key, cell->row_key_len));
    }
    switch (cell->v.type)
    {
      case OB_INT_TYPE:
        printf("\t%ld", cell->v.v.v_int);
        break;
      //add lijianqiang [INT_32] 20150930:b
      case OB_INT32_TYPE:
        printf("%d", cell->v.v.v_int32);
        break;
      //add 20150930:e
      case OB_VARCHAR_TYPE:
        printf("\t%.*s", cell->v.v.v_varchar.len, cell->v.v.v_varchar.p);
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
  }
  printf("\n\n");
}


void pcell(OB_CELL* cell)
{
  printf("%ld", cell->table_len);
  printf("%.*s\t%s\t%.*s\t",
         (int)cell->table_len, cell->table,
         rowkey2hex(cell->row_key, cell->row_key_len),
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
      printf("%d.%d", cell->v.v.v_datetime.tv_sec, cell->v.v.v_datetime.tv_usec);
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

int main()
{
  int64_t num = 0;

  //char* table = "collect_item";
  char* table = "dpunit_sr_auction_info_d";
  //char* table = "lz_rpt_p4p_campaign_name";
  char rowkey1[24];
  char rowkey2[24];
  //memset(rowkey1, 0x00, sizeof(rowkey1));
  //memset(rowkey2, 0x00, sizeof(rowkey2));
  memset(rowkey1, 0x00, sizeof(rowkey1));
  memset(rowkey2, 0xFF, sizeof(rowkey2));
  memset(rowkey2, 0x00, 8);
  //char rowkey1[9] = "AAAAAAAAA";
  //char rowkey2[9] = "AAAAAAAAF";
  //000000000000282C000000004D5AA30000000000002AD6C70000000001F5948D0000000000000054
  //rowkey2[6] = 0xC0;
  rowkey1[7] = 0x03;
  rowkey2[7] = 0x03;
  //rowkey2[7] = 0x10;

  //memset(rowkey1, 0x00, 9);
  OB* ob = ob_api_init();
  if (NULL == ob)
  {
    fprintf(stderr, "ob_init error: %s\n", ob_error());
    return 1;
  }

  ob_api_debug_log(ob, "DEBUG", NULL);
  ob_api_cntl(ob, OB_S_TIMEOUT, 100000000);

  // 连接OceanBase
  //OB_ERR_CODE err = ob_connect(ob, "10.232.36.33", 12010, NULL, NULL);
  OB_ERR_CODE err = ob_connect(ob, "10.232.36.189", 12010, NULL, NULL);
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
      //ob_scan(scan_st, table, rowkey1, sizeof(rowkey1), 1, rowkey2, sizeof(rowkey2), 1);
      ob_scan(scan_st, table, rowkey1, sizeof(rowkey1), 1, rowkey2, sizeof(rowkey2), 1);

      //ob_scan_column(scan_st, "item_collector_count", 0);
      //ob_scan_column(scan_st, "item_collect_count", 0);
      //ob_scan_complex_column(scan_st, "item_collector_count+item_collect_count", "i2", 1);
      //ob_scan_column(scan_st, "item_title", 1);
      //ob_scan_column(scan_st, "item_ends", 1);
      //ob_scan_set_where(scan_st, "item_title is NULL");
      //ob_scan_complex_column(scan_st, "item_ends = #2011-11-10 10:12:13#", "i2", 0);
      //ob_scan_set_where(scan_st, "i2");
      //ob_scan_set_where(scan_st, "item_ends < #1970-1-1#");
      //ob_scan_complex_column(scan_st, "item_collector_count*2", "i2", 1);
      //ob_scan_set_where(scan_st, "item_title != 'test'");
      //ob_scan_orderby_column(scan_st, "i2", OB_DESC);
      //ob_scan_column(scan_st, "aucid", 1);
      ob_scan_complex_column(scan_st, "aucid = 3803471207", "_expr_0", 0);
      //ob_scan_column(scan_st, "unit_p4p_id", 1);
      //ob_scan_column(scan_st, "finclick", 1);
      //ob_scan_column(scan_st, "finprice", 0);
      //ob_scan_column(scan_st, "shop_id", 1);
      //ob_scan_column(scan_st, "unit_id", 1);
      //ob_scan_complex_column(scan_st, "`finprice` * 2", "2unit_id", 1);
      //ob_scan_complex_column(scan_st, "`unit_id` * 2", "2unit_id", 1);
      //ob_scan_set_where(scan_st, "(`unit_id` >= 30000) AND (`unit_id` <= 40000) AND (`2unit_id` <= 70000)");
      //ob_scan_set_where(scan_st, "unit_p4p_id = 138");
      //ob_scan_set_where(scan_st, "aucid = 3803471207");
      ob_scan_set_where(scan_st, "_expr_0");
      //ob_scan_orderby_column(scan_st, "SUM(finclick)", OB_DESC);
      //ob_scan_set_limit(scan_st, 0, 0);

      OB_GROUPBY_PARAM* groupby = ob_get_ob_groupby_param(scan_st);
      if (NULL != groupby)
      {
        //ob_groupby_column(groupby, "unit_p4p_id", 1);
        //ob_aggregate_column(groupby, OB_SUM, "finclick", "SUM(finclick)", 1);
        //ob_groupby_set_having(groupby, "`SUM(finclick)` >= 1000");
        //ob_groupby_add_complex_column(groupby, "unit_p4p_id * 2", "unit_p4p_idX2", 1);
        //ob_aggregate_column(groupby, OB_SUM, "finprice", "SUM(finprice)", 1);
        //ob_groupby_add_complex_column(groupby, "`SUM(finprice)` / `SUM(finclick)`", "AVG PRICE", 1);
        //ob_aggregate_column(groupby, OB_COUNT, "finclick", "COUNT(finclick)", 1);
        //ob_aggregate_column(groupby, OB_COUNT, "shop_id", "count_shop_id", 1);
        //ob_groupby_add_complex_column(groupby, "(3 - `count_thedate`) * (2 + `count_shop_id`)", "equal1", 1);
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
          }
          ob_release_res_st(ob, res_st);
        }
      }
      ob_release_scan_st(ob, scan_st);
    }
  }

  ob_api_destroy(ob);

  return 0;
}
