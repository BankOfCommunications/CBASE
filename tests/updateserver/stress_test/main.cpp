/**
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * Authors:
 *   yanran <yanran.hfs@taobao.com>
 *     - some work details if you want
 */

#include <getopt.h>

#include "tblog.h"

#include "common/ob_define.h"
#include "common/ob_server.h"
#include "ob_test_bomb.h"
#include "ob_executor.h"
#include "ob_update_gen.h"
#include "ob_scan_gen.h"
#include "ob_random.h"
#include "ob_row_dis.h"
#include "ob_schema_proxy.h"
#include "ob_add_runnable.h"

namespace
{
  char* upsvip = NULL;
  int upsport = -1;
  char* msip = NULL;
  int msport = -1;
  bool init = false;
  int thread_no = 1;
  int write_weight = 1;
  int read_weight = 1;
  char* log_name = "test_add.log";
  char* log_level = "debug";
  int64_t scan_max_line_no = 10;
  int64_t update_max_cell_no = 30;
}

using namespace oceanbase::common;
using namespace oceanbase::test;

class DIntRowkey
{
  public:
    void init(int64_t rk_p1, int64_t rk_p2)
    {
      rk_p1_ = rk_p1;
      rk_p2_ = rk_p2;

      int64_t pos = 0;
      serialization::encode_i64(rk_buf_, rk_buf_len_, pos, rk_p1);
      pos = sizeof(int64_t);
      serialization::encode_i64(rk_buf_, rk_buf_len_, pos, rk_p2);
      rk_.assign(rk_buf_, rk_buf_len_);
    }

    void init(char *rk_buf)
    {
      memcpy(rk_buf_, rk_buf, rk_buf_len_);
      int64_t pos = 0;
      serialization::decode_i64(rk_buf_, rk_buf_len_, pos, &rk_p1_);
      pos = sizeof(int64_t);
      serialization::decode_i64(rk_buf_, rk_buf_len_, pos, &rk_p2_);
      rk_.assign(rk_buf_, rk_buf_len_);
    }

    void init(ObString rk)
    {
      memcpy(rk_buf_, rk.ptr(), rk_buf_len_);
      int64_t pos = 0;
      serialization::decode_i64(rk_buf_, rk_buf_len_, pos, &rk_p1_);
      pos = sizeof(int64_t);
      serialization::decode_i64(rk_buf_, rk_buf_len_, pos, &rk_p2_);
      rk_.assign(rk_buf_, rk_buf_len_);
    }

    int64_t get_rk_p1() const {return rk_p1_;}
    int64_t get_rk_p2() const {return rk_p2_;}

    ObString& get_rk() {return rk_;}
    const ObString& get_rk() const {return rk_;}

  private:
    int64_t rk_p1_;
    int64_t rk_p2_;
    static const int rk_buf_len_ = sizeof(int64_t) * 2;
    char rk_buf_[rk_buf_len_];
    ObString rk_;
};

int init_ups()
{
  int ret = 0;

  ObRowDis &row_dis = ObRowDis::get_instance();
  ObSchemaProxy &schema = ObSchemaProxy::get_instance();
  ObRandom rand;
  DIntRowkey row_key;
  ObMutator mut;
  ObTestBomb test_bomb;
  ObExecutor exer;

  ObServer ups(ObServer::IPV4, upsvip, upsport);
  ret = exer.init(ups, ups);

  rand.initt();

  for (int i = 0; OB_SUCCESS == ret && i <= row_dis.get_row_p1_num(); i++)
  {
    for (int j = 0; OB_SUCCESS == ret && j <= row_dis.get_row_p2_num(); j++)
    {
      row_key.init(row_dis.get_row_p1(i), row_dis.get_row_p2(j));

      int n = rand.randt(2, schema.get_column_num());

      mut.reset();
      ret = ObUpdateGen::gen_line(n, row_key.get_rk(), row_key.get_rk_p2(), rand, mut);
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "gen_line error, ret=%d", ret);
      }
      else
      {
        test_bomb.set_mutator(&mut);
        ret = exer.exec(test_bomb);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "ObExecutor exec error, ret=%d", ret);
        }
      }
    }
  }

  return ret;
}

int work()
{
  int ret = OB_SUCCESS;

  ObServer ms(ObServer::IPV4, msip, msport);
  ObServer ups(ObServer::IPV4, upsvip, upsport);

  ObAddRunnable *add_runnable = new ObAddRunnable[thread_no];
  for (int i = 0; i < thread_no; i++)
  {
    add_runnable[i].init(ms, ups, write_weight, read_weight, update_max_cell_no, scan_max_line_no);
    add_runnable[i].start();
  }

  for (int i = 0; i < thread_no; i++)
  {
    add_runnable[i].wait();
  }

  delete[] add_runnable;

  return ret;
}

void print_usage(const char *prog_name)
{
  fprintf(stderr, "\nUsage: %s -u UPS_VIP -p UPS_PORT -m MS_IP -q MS_PORT\n"
      "    -u, --upsvip       UPS VIP\n"
      "    -p, --upsport      UPS Port\n"
      "    -m, --msip         MS IP\n"
      "    -q, --msport       MS Port\n"
      "    -i, --init         Initialize UPS (default NOT)\n"
      "    -t, --thread       Thread Number (default %d)\n"
      "    -w, --write        Write Weight (deafult %d)\n"
      "    -r, --read         Read Weight (default %d)\n"
      "    -c, --update       Maximum Number of Cells of Update per op (deafult %ld)\n"
      "    -s, --scan         Maximum Number of Lines of Scan per op (default %ld)\n"
      "    -l, --log_name     Log Name (default %s)\n"
      "    -e, --log_level    Log Level (ERROR|WARN|INFO|DEBUG, default %s)\n"
      "    -h, --help         this help\n\n",
      prog_name, thread_no, write_weight, read_weight, update_max_cell_no, scan_max_line_no, log_name, log_level);
}

int parse_cmd_line(const int argc,  char *const argv[])
{
  int opt = 0;
  const char* opt_string = "-u:m:p:q:it:w:r:l:e:c:s:h";
  struct option longopts[] = 
  {
    {"upsvip", 1, NULL, 'u'},
    {"upsport", 1, NULL, 'p'},
    {"msip", 1, NULL, 'm'},
    {"msport", 1, NULL, 'q'},
    {"init", 0, NULL, 'i'},
    {"thread", 1, NULL, 't'},
    {"write", 1, NULL, 'w'},
    {"read", 1, NULL, 'r'},
    {"log_name", 1, NULL, 'l'},
    {"log_level", 1, NULL, 'e'},
    {"update", 1, NULL, 'c'},
    {"scan", 1, NULL, 's'},
    {"help", 0, NULL, 'h'},
    {0, 0, 0, 0}
  };

  while((opt = getopt_long(argc, argv, opt_string, longopts, NULL)) != -1) {
    switch (opt) {
      case 'u':
        upsvip = optarg;
        TBSYS_LOG(INFO, "UPS VIP = %s", upsvip);
        break;
      case 'p':
        upsport = atoi(optarg);
        TBSYS_LOG(INFO, "UPS PORT = %d", upsport);
        break;
      case 'm':
        msip = optarg;
        TBSYS_LOG(INFO, "MS VIP = %s", msip);
        break;
      case 'q':
        msport = atoi(optarg);
        TBSYS_LOG(INFO, "MS PORT = %d", msport);
        break;
      case 'i':
        init = true;
        TBSYS_LOG(INFO, "Enable init UPS");
        break;
      case 't':
        thread_no = atoi(optarg);
        TBSYS_LOG(INFO, "Thread Number = %d", thread_no);
        break;
      case 'w':
        write_weight = atoi(optarg);
        TBSYS_LOG(INFO, "Write Weight = %d", write_weight);
        break;
      case 'r':
        read_weight = atoi(optarg);
        TBSYS_LOG(INFO, "Read Weight = %d", read_weight);
        break;
      case 'l':
        log_name = optarg;
        TBSYS_LOG(INFO, "Log Name = %s", log_name);
        break;
      case 'e':
        log_level = optarg;
        TBSYS_LOG(INFO, "Log Level = %s", log_level);
        break;
      case 'c':
        update_max_cell_no = atoi(optarg);
        TBSYS_LOG(INFO, "Maximum Number of Cells of Update = %ld", update_max_cell_no);
        break;
      case 's':
        scan_max_line_no = atoi(optarg);
        TBSYS_LOG(INFO, "Maximum Number of Lines of Scan = %ld", scan_max_line_no);
        break;
      case 'h':
        print_usage(argv[0]);
        exit(0);
        break;
      default:
        break;
    }
  }

  if ((NULL == upsvip)
      || (-1 == upsport)
      || (NULL == msip)
      || (-1 == msport))
  {
    print_usage(argv[0]);
    exit(0);
  }

  return 0;
}

int main(int argc, char* argv[])
{
  int ret = OB_SUCCESS;

  ob_init_memory_pool();

  parse_cmd_line(argc, argv);

  if (NULL != log_name)
  {
    TBSYS_LOGGER.setFileName(log_name);
  }
  else
  {
    TBSYS_LOGGER.setFileName("test_add.log");
  }

  if (NULL != log_level)
  {
    TBSYS_LOGGER.setLogLevel(log_level);
  }
  else
  {
    TBSYS_LOGGER.setLogLevel("debug");
  }

  if (init)
  {
    init_ups();
  }

  work();

  return ret;
}
