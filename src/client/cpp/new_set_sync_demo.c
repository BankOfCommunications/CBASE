#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include "oceanbase.h"

int main(int argc, char **argv)
{

  char* table = "test_seller_wm_4";
  char* column1 = "nick";
  char* column2 = "prov";
  char* column3 = "auc_cnt_max_cnt";
  char rowkey1[44];
  char rowkey2[44];

  OB* ob = ob_api_init();
  if (NULL == ob)
  {
    fprintf(stderr, "ob_init error: %s\n", ob_error());
    return 1;
  }

  ob_api_debug_log(ob, "DEBUG", NULL);

  // 连接OceanBase
  OB_ERR_CODE err = OB_ERR_SUCCESS;
  if (argc > 2)
  {

    fprintf(stderr, "hello");
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
    // 获得OB_REQ结构体
    OB_SET* set_st = ob_acquire_set_st(ob);
    if (NULL == set_st)
    {
      fprintf(stderr, "ob_acquire_set_st error: %s\n", ob_error());
    }
    else
    {
      fprintf(stderr, "init rowkey... %s\n", ob_error());
      
      memset(rowkey1, 0x00, 44);
      memset(rowkey2, 0x00, 44);

      memcpy(&rowkey1[0], "201145", 6);
      rowkey1[8+6-1] = 16;

      memcpy(&rowkey2[0], "201145", 6);
      rowkey2[8+6-1] = 16;
      memset(&rowkey2[14], 0xee, 8);

      OB_VARCHAR r1c1 = {"lovinlovin", strlen("lovinlovin")};
      OB_VARCHAR r1c2 = {"shanghai", strlen("shanghai")};
      OB_VARCHAR r2c1 = {"songyi0209", strlen("songyi0209")};
      OB_VARCHAR r2c2 = {"beijing", strlen("beijing")};

      fprintf(stderr, "insert value... %s\n", ob_error());

      //memset(rowkey1, 0x00, 44);
      //memset(rowkey2, 0xFF, 44);
      ob_insert_varchar(set_st, table, rowkey1, 22, column1, r1c1);
      ob_insert_varchar(set_st, table, rowkey1, 22, column2, r1c2);

      ob_insert_varchar(set_st, table, rowkey2, 22, column1, r2c1);
      ob_insert_varchar(set_st, table, rowkey2, 22, column2, r1c2);
      /*
      ob_insert_int(set_st, table, rowkey1, strlen(rowkey1), column1, 1);
      ob_insert_int(set_st, table, rowkey1, strlen(rowkey1), column2, 3);
      ob_insert_int(set_st, table, rowkey2, strlen(rowkey2), column1, 1);
      ob_insert_int(set_st, table, rowkey2, strlen(rowkey2), column2, 4);
      */
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
