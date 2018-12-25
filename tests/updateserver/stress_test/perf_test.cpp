/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 *
 * Version: 0.1: perf_test.cpp,v 0.1 2011/03/22 14:32:03 chuanhui Exp $
 *
 * Authors:
 *   chuanhui <rizhao.ych@taobao.com>
 *     - some work details if you want
 *
 */

#include <pthread.h>
#include <unistd.h>
#include <getopt.h>
#include "tbnet.h"
#include "common/ob_define.h"
#include "common/ob_string.h"
#include "common/ob_schema.h"
#include "common/ob_server.h"
#include "common/ob_mutator.h"
#include "common/ob_get_param.h"
#include "common/ob_scanner.h"
#include "common/ob_memory_pool.h"
#include "updateserver/ob_schema_mgr.h"
#include "mock_client.h"

const static int64_t WRITE_THREAD_NUM = 1;
const static int64_t READ_THREAD_NUM = 10;
const static int64_t READ_ROW_NUM = 50; // scan 50 rows
const static int64_t START_ROW = 0;
const static int64_t END_ROW = 100000000; // 100M
const static int64_t READ_TIMES = 1000000; // 1M

static const char* UPDATE_SERVER_ADDR = "10.232.36.40:3434";
static ObServer g_update_server;
static MockClient g_update_client;

static int64_t MEMTABLE_START_VERSION = 2;
static const int MY_VARCHAR_LEN = 20;

using namespace oceanbase::common;
using namespace oceanbase::updateserver;
static int set_server_addr(const char* addr_str, ObServer& server);

struct ConfigParam
{
  int64_t write_thread_num_;
  int64_t write_thread_idx_;
  int64_t write_start_row_;
  int64_t write_end_row_;
  int64_t read_thread_num_;
  int64_t read_start_row_;
  int64_t read_end_row_;
  int64_t read_row_num_;
  int64_t read_times_;

  // write schema
  int64_t num_int_columns_;
  int64_t num_other_columns_;
  ObString table_name_;
  ObString int_columns_name_[64];
  ObString other_columns_name_[64];
  ColumnType other_columns_type_[64];
  uint64_t other_columns_size_[64];
  char schema_file_[64];

  int64_t read_num_int_columns_;
  int64_t read_num_other_columns_;
  uint64_t read_table_id_;
  uint64_t read_int_columns_id_[64];
  uint64_t read_other_columns_id_[64];
  char read_schema_file_[64];
};

class ObGenerator
{
  private:
    int64_t min_;
    int64_t max_;
    bool rand_;
    int64_t thread_num_;
    int64_t thread_idx_;

    int64_t num_;
  private:
    unsigned short xsubi[3];

  public:
    ObGenerator(int64_t min, int64_t max, bool rand = true, int64_t thread_num = -1, int64_t thread_idx = -1)
    {
      if (!rand)
      {
        assert((min % thread_num)== 0);
      }
      min_ = min;
      max_ = max;
      rand_ = rand;
      thread_num_ = thread_num;
      thread_idx_ = thread_idx;

      num_ = min_ + thread_idx;

      int64_t time = tbsys::CTimeUtil::getTime();
      xsubi[0] = time & 0xFFFF;
      xsubi[1] = (time >> 2) & 0xFFFF;
      xsubi[2] = (time >> 4) & 0xFFFF;
    }

    ~ObGenerator()
    {
    }

  public:
    int64_t next_val()
    {
      int64_t next_val = 0;

      if (!rand_)
      {
        if (num_ > max_)
        {
          next_val = -1;
        }
        else
        {
          next_val = num_;
          num_ = num_ + thread_num_;
        }
      }
      else
      {
        next_val = nrand48(xsubi);
        next_val = next_val % (max_ - min_ + 1) + min_;
      }

      return next_val;
    }
};

class PerformanceTest
{
  private:
    ConfigParam param_;
    static bool is_stop_;

  public:
    PerformanceTest(const ConfigParam& param)
    {
      param_ = param;
    }

    ~PerformanceTest()
    {
    }

    int init(ObSchemaManagerWrapper& schema_wrapper, ObSchemaManagerWrapper& read_schema_wrapper)
    {
      int err = OB_SUCCESS;
      const ObSchema* iter = schema_wrapper.begin();
      const char* table_name = iter->get_table_name();
      param_.table_name_.assign((char*) table_name, strlen(table_name));

      const ObColumnSchema* col_iter = NULL;
      param_.num_int_columns_ = 0;
      param_.num_other_columns_ = 0;
      for (col_iter = iter->column_begin(); col_iter < iter->column_end(); ++col_iter)
      {
        const char* col_name = col_iter->get_name();
        ColumnType col_type = col_iter->get_type();
        uint64_t col_size = col_iter->get_size();
        if (ObIntType == col_type)
        {
          param_.int_columns_name_[param_.num_int_columns_].assign((char*) col_name, strlen(col_name));
          param_.num_int_columns_++;
        }
        else
        {
          param_.other_columns_name_[param_.num_other_columns_].assign((char*) col_name, strlen(col_name));
          param_.other_columns_type_[param_.num_other_columns_] = col_type;
          param_.other_columns_size_[param_.num_other_columns_] = col_size;
          param_.num_other_columns_++;
        }
      }

      const ObSchema* read_iter = read_schema_wrapper.begin();
      uint64_t table_id = read_iter->get_table_id();
      param_.read_table_id_ = table_id;

      param_.read_num_int_columns_ = 0;
      param_.read_num_other_columns_ = 0;
      for (col_iter = read_iter->column_begin(); col_iter < read_iter->column_end(); ++col_iter)
      {
        ColumnType col_type = col_iter->get_type();
        uint64_t col_id = col_iter->get_id();
        if (ObIntType == col_type)
        {
          param_.read_int_columns_id_[param_.read_num_int_columns_] = col_id;
          param_.read_num_int_columns_++;
        }
        else
        {
          param_.read_other_columns_id_[param_.read_num_other_columns_] = col_id;
          param_.read_num_other_columns_++;
        }
      }

      TBSYS_LOG(INFO, "table_name=%s, table_id=%lu, num_int_columns=%ld, num_other_columns=%ld, "
          "read_num_int_columns=%ld, read_num_other_columns=%ld, err=%d",
          table_name, table_id, param_.num_int_columns_, param_.num_other_columns_,
          param_.read_num_int_columns_, param_.read_num_other_columns_, err);

      return err;
    }


    int run_test()
    {
      fprintf(stderr, "write_thread_num=%ld, read_thread_num=%ld\n", param_.write_thread_num_,
          param_.read_thread_num_);
      pthread_t* write_threads = new pthread_t[param_.write_thread_num_];
      pthread_t* read_threads = new pthread_t[param_.read_thread_num_];
      
      ConfigParam* params = new ConfigParam[param_.write_thread_num_];

      // start write thread
      for (int64_t i = 0; i < param_.write_thread_num_; ++i)
      {
        params[i] = param_;
        params[i].write_thread_idx_ = i;
        pthread_create(write_threads + i, NULL, 
            write_thread_routine, &params[i]);
      }

      // start read thread
      for (int64_t i = 0; i < param_.read_thread_num_; ++i)
      {
        pthread_create(read_threads + i, NULL, read_thread_routine,
            &param_);
      }

      for (int64_t i = 0; i < param_.write_thread_num_; ++i)
      {
        pthread_join(write_threads[i], NULL);
      }

      for (int64_t i = 0; i < param_.read_thread_num_; ++i)
      {
        pthread_join(read_threads[i], NULL);
      }

      delete[] write_threads;
      delete[] read_threads;
      delete[] params;
      return 0;
    }

  public:
    static void stop()
    {
      is_stop_ = true;
    }

  public:
    /*
    static int build_get_param(const ObString& table_name, const ObString& row_key,
        const int64_t num_int_columns, const ObString* int_columns_name,
        const ObVersionRange& version_range, ObGetParam& get_param)
    {
      int err = OB_SUCCESS;

      if (num_int_columns <= 0 || NULL == int_columns_name)
      {
        err = OB_ERROR;
      }
      else
      {
        ObCellInfo cell_info;
        for (int64_t i = 0; OB_SUCCESS == err && i < num_int_columns; ++i)
        {
          cell_info.column_name_ = int_columns_name[i];
          cell_info.row_key_ = row_key;
          cell_info.table_name_ = table_name;
          err = get_param.add_cell(cell_info);
        }

        if (OB_SUCCESS == err)
        {
          get_param.set_version_range(version_range);
          get_param.set_is_result_cached(true);
        }
      }

      return err;
    }
    */

    static int build_scan_param(const int64_t num_int_columns, const uint64_t* int_columns_id,
        const int64_t num_other_columns, const uint64_t* other_columns_id,
        const ObVersionRange& version_range, ObScanParam& scan_param)
    {
      int err = OB_SUCCESS;

      if ((num_int_columns <= 0 || NULL == int_columns_id)
          && (num_other_columns <= 0 || NULL == other_columns_id))
      {
        err = OB_ERROR;
      }
      else
      {
        for (int64_t i = 0; OB_SUCCESS == err && i < num_int_columns; ++i)
        {
          scan_param.add_column(int_columns_id[i]);
        }

        for (int64_t i = 0; OB_SUCCESS == err && i < num_other_columns; ++i)
        {
          scan_param.add_column(other_columns_id[i]);
        }

        scan_param.set_version_range(version_range);
      }

      return err;
    }

    static int build_int_write_param(const ObString& table_name, const ObString& row_key,
        const int64_t num_int_columns, const ObString* int_columns_name, ObMutator& mutator)
    {
      int err = OB_SUCCESS;
      int64_t sum = 0;

      for (int64_t i = 0; OB_SUCCESS == err && i < num_int_columns; ++i)
      {
        ObObj value;
        if (i < num_int_columns - 1)
        {
          int tmp_val = rand() % 2048 - 1024;
          sum += tmp_val;
          value.set_int(tmp_val);
        }
        else
        {
          value.set_int(-sum);
        }
        err = mutator.update(table_name, row_key, int_columns_name[i], value);
      }

      if (OB_SUCCESS != err)
      {
        TBSYS_LOG(WARN, "failed to build int write params, err=%d", err);
      }

      return err;
    }

    static int build_other_write_param(const ObString& table_name, const ObString& row_key,
        const int64_t num_other_columns, const ObString* other_columns_name,
        const ColumnType* other_columns_type, const int64_t* other_columns_size, ObMutator& mutator)
    {
      int err = OB_SUCCESS;
      char str[2048];

      for (int64_t i = 0; OB_SUCCESS == err && i < num_other_columns; ++i)
      {
        ObObj value;
        int type = other_columns_type[i];
        bool found = true;
        switch (type)
        {
          case ObVarcharType:
            {
              //int len = ::rand() % 16 + 1;
              int len = (other_columns_size[i] < 2048) ? other_columns_size[i] : 2048;
              for (int j = 0; j < len; ++j)
              {
                str[j] = 'a' + ::rand() % 26;
              }
              ObString tmp_str;
              tmp_str.assign(str, len);
              value.set_varchar(tmp_str);
            }
            break;
          default:
            //TBSYS_LOG(WARN, "unknown type, omit it, type=%d", type);
            found = false;
            break;
        }

        if (found)
        {
          err = mutator.update(table_name, row_key, other_columns_name[i], value);
        }
      }

      if (OB_SUCCESS != err)
      {
        TBSYS_LOG(WARN, "failed to build other write params, err=%d", err);
      }
      return err;
    }

    static int build_write_param(const ObString& table_name, const ObString& row_key,
        const int64_t num_int_columns, const ObString* int_columns_name,
        const int64_t num_other_columns, const ObString* other_columns_name,
        const ColumnType* other_columns_type, const int64_t* other_columns_size, ObMutator& mutator)
    {
      int err = OB_SUCCESS;

      err = build_int_write_param(table_name, row_key, num_int_columns, int_columns_name,
          mutator);
      if (OB_SUCCESS == err)
      {
        err = build_other_write_param(table_name, row_key, num_other_columns,
            other_columns_name, other_columns_type, other_columns_size, mutator);
      }

      if (OB_SUCCESS != err)
      {
        TBSYS_LOG(WARN, "failed to build write param, err=%d", err);
      }

      return err;
    }

    /*
    static int get_data(ObGetParam& get_param, ObScanner& scanner)
    {
      int err = OB_SUCCESS;
      int64_t timeout = 2 * 1000L * 1000L;

      err = g_update_client.ups_get(get_param, scanner, timeout);
      if (OB_SUCCESS != err)
      {
        TBSYS_LOG(WARN, "failed to get data ,err=%d", err);
      }

      return err;
    }
    */

    static int scan_data(ObScanParam& scan_param, ObScanner& scanner)
    {
      int err = OB_SUCCESS;

      int64_t timeout = 2 * 1000L * 1000L;

      err = g_update_client.ups_scan(scan_param, scanner, timeout);
      if (OB_SUCCESS != err)
      {
        TBSYS_LOG(WARN, "failed to scan data, err=%d", err);
      }

      return err;
    }

    static int write_data(ObMutator& mutator)
    {
      int err = OB_SUCCESS;
      int64_t timeout = 2 * 1000L * 1000L;

      err = g_update_client.ups_apply(mutator, timeout);
      if (OB_SUCCESS != err)
      {
        TBSYS_LOG(WARN, "failed to call ups_apply, err=%d", err);
      }

      return err;
    }

    static int translate_row_key(const int64_t row_id, ObString& row_key)
    {
      int err = OB_SUCCESS;
      char tmp[16];
      sprintf(tmp, "%08x", (unsigned int) row_id);
      int64_t num = row_key.write(tmp, 8);
      if (num != 8)
      {
        TBSYS_LOG(WARN, "error occurs, num=%ld", num);
        err = OB_ERROR;
      }
      return err;
    }

    static int64_t get_cur_time_us()
    {
      struct timeval tv;
      gettimeofday(&tv, NULL);
      int64_t cur_time_us = tv.tv_sec * 1000000L + tv.tv_usec;
      return cur_time_us;
    }

    static void* write_thread_routine(void* arg)
    {
      int err = OB_SUCCESS;
      ConfigParam* param = (ConfigParam*) arg;
      ObString table_name = param->table_name_;
      int64_t write_thread_num = param->write_thread_num_;
      int64_t write_thread_idx = param->write_thread_idx_;
      int64_t write_start_row = param->write_start_row_;
      int64_t write_end_row = param->write_end_row_;
      int64_t num_int_columns = param->num_int_columns_;
      ObString* int_columns_name = param->int_columns_name_;
      int64_t num_other_columns = param->num_other_columns_;
      ObString* other_columns_name = param->other_columns_name_;
      ColumnType* other_columns_type = param->other_columns_type_;
      int64_t* other_columns_size = (int64_t*) (param->other_columns_size_);

      TBSYS_LOG(INFO, "START write thread, write_thread_num=%ld, write_thread_idx=%ld "
          "write_start_row=%ld, write_end_row=%ld",
          write_thread_num, write_thread_idx, write_start_row, write_end_row);

      ObGenerator generator(write_start_row, write_end_row, false, write_thread_num, write_thread_idx);
      int64_t succeed_row_num = 0;
      int64_t failed_row_num = 0;

      while (!is_stop_)
      {
        ObMutator mutator;
        int64_t row_idx = generator.next_val();
        if (row_idx < 0)
        {
          break;
        }
        ObString row_key;
        char row_key_buf[16];
        row_key.assign_buffer(row_key_buf, sizeof(row_key_buf));
        translate_row_key(row_idx, row_key);

        err = build_write_param(table_name, row_key,
            num_int_columns, int_columns_name,
            num_other_columns, other_columns_name, other_columns_type, other_columns_size, mutator);
        if (OB_SUCCESS != err)
        {
          TBSYS_LOG(WARN, "failed to build write param, err=%d", err);
        }
        else
        {
          err = write_data(mutator);
          if (OB_SUCCESS != err)
          {
            TBSYS_LOG(WARN, "failed to write data, err=%d", err);
          }
        }

        if (OB_SUCCESS == err)
        {
          ++succeed_row_num;
          //TBSYS_LOG(INFO, "write int success, row_key=%ld", row_idx);
          if (succeed_row_num % 10000 == 0)
          {
            TBSYS_LOG(INFO, "Successfully write 10000 rows");
          }
        }
        else
        {
          ++failed_row_num;
          TBSYS_LOG(WARN, "write int failed, row_key=%ld, err=%d",
              row_idx, err);
        }
      }

      TBSYS_LOG(INFO, "write thread STOP, succeed_row_num=%ld, failed_row_num=%ld",
          succeed_row_num, failed_row_num);
      return NULL;
    }

    static void* read_thread_routine(void* arg)
    {
      int err = OB_SUCCESS;
      ConfigParam* param = (ConfigParam*) arg;
      uint64_t table_id = param->read_table_id_;
      int64_t num_int_columns = param->read_num_int_columns_;
      uint64_t* int_columns_id = param->read_int_columns_id_;
      int64_t num_other_columns = param->read_num_other_columns_;
      uint64_t* other_columns_id = param->read_other_columns_id_;

      int64_t read_start_row = param->read_start_row_;
      int64_t read_end_row = param->read_end_row_;
      int64_t read_row_num = param->read_row_num_;
      int64_t read_times = param->read_times_;

      ObVersionRange version_range;
      //version_range.border_flag_.set_min_value();
      version_range.start_version_ = MEMTABLE_START_VERSION;
      version_range.border_flag_.set_inclusive_start();
      version_range.border_flag_.set_max_value();

      ObGenerator generator(read_start_row, read_end_row);
      ObString table_name;
      table_name.assign(NULL, 0);
      int64_t succeed_row_num = 0;
      int64_t failed_row_num = 0;
      int64_t start_time_us = get_cur_time_us();
      int64_t read_size = 0;

      TBSYS_LOG(INFO, "START read thread, read_start_row=%ld, read_end_row=%ld "
          "read_row_num=%ld, read_times=%ld", read_start_row, read_end_row, read_row_num, read_times);

      while (!is_stop_ && --read_times >= 0)
      {
        ObScanParam scan_param;
        ObScanner scanner;
        ObRange range;
        int64_t start_val = generator.next_val();
        int64_t end_val = start_val + read_row_num - 1;

        ObString start_row_key;
        ObString end_row_key;
        char start_row_key_buf[16];
        char end_row_key_buf[16];

        start_row_key.assign_buffer(start_row_key_buf, sizeof(start_row_key_buf));
        translate_row_key(start_val, start_row_key);
        end_row_key.assign_buffer(end_row_key_buf, sizeof(end_row_key_buf));
        translate_row_key(end_val, end_row_key);

        range.start_key_ = start_row_key;
        range.end_key_ = end_row_key;
        range.border_flag_.set_inclusive_start();
        range.border_flag_.set_inclusive_end();

        scan_param.set(table_id, table_name, range);
        //TBSYS_LOG(INFO, "Scan data, start_val=%ld, end_val=%ld", start_val, end_val);

        err = build_scan_param(num_int_columns, int_columns_id, num_other_columns,
            other_columns_id, version_range, scan_param);
        if (OB_SUCCESS != err)
        {
          TBSYS_LOG(WARN, "failed to build scan param, err=%d", err);
        }
        else
        {
          err = scan_data(scan_param, scanner);
          if (OB_SUCCESS != err)
          {
            TBSYS_LOG(WARN, "failed to scan data, table_name=%.*s, err=%d",
                table_name.length(), table_name.ptr(), err);
            ++failed_row_num;
          }
          else
          {
            ++succeed_row_num;
            read_size += scanner.get_size();
            if (succeed_row_num % 10000000 == 0)
            {
              TBSYS_LOG(INFO, "Successfully scan 10000000 rows");
            }

            /*
            ObCellInfo* cell_info = NULL;
            while (OB_SUCCESS == (err = scanner.next_cell()))
            {
              err = scanner.get_cell(&cell_info);
              if (OB_SUCCESS != err || NULL == cell_info)
              {
                TBSYS_LOG(WARN, "invalid scanner");
                err = OB_ERROR;
              }
              else
              {
                ObString str_val;
                cell_info->value_.get_varchar(str_val);
                TBSYS_LOG(INFO, "read_times=%ld column_id=%lu str len=%ld", read_times, cell_info->column_id_, str_val.length());
              }
            }
            */
          }
        }
      }

      int64_t end_time_us = get_cur_time_us();

      TBSYS_LOG(INFO, "read thread STOP, succeed_row_num=%ld, read_size=%ld, failed_row_num=%ld "
          "time_used=%ld us", succeed_row_num, read_size, failed_row_num, end_time_us - start_time_us);
      return NULL;
    }
};

bool PerformanceTest::is_stop_ = false;


//////////////////////////////////////////////////////////////////////////////////
//
// Command Line Related Functions
//
/////////////////////////////////////////////////////////////////////////////////
static int set_server_addr(const char* addr_str, ObServer& server)
{
  int err = OB_SUCCESS;
  assert(NULL != addr_str);
  char* p_seg = strstr(addr_str, ":");
  if (NULL == p_seg)
  {
    TBSYS_LOG(WARN, "failed to find :");
    err = OB_ERROR;
  }
  else
  {
    char tmp[32];
    memcpy(tmp, addr_str, p_seg - addr_str);
    tmp[p_seg - addr_str] = '\0';
    int port = atoi(p_seg + 1);
    server.set_ipv4_addr(tmp, port);
  }

  return err;
}

static void init_config_param(ConfigParam& param)
{
  memset(&param, 0x00, sizeof(param));
  param.write_thread_num_ = WRITE_THREAD_NUM;
  param.write_start_row_ = START_ROW;
  param.write_end_row_ = END_ROW;
  param.read_thread_num_ = READ_THREAD_NUM;
  param.read_row_num_ = READ_ROW_NUM;
  param.read_start_row_ = START_ROW;
  param.read_end_row_ = END_ROW;
  param.read_times_ = READ_TIMES;

  set_server_addr(UPDATE_SERVER_ADDR, g_update_server);
  strcpy(param.schema_file_, "schema.ini");
  strcpy(param.read_schema_file_, "schema.ini");
}

void print_usage()
{
  fprintf(stderr, "\n");
  fprintf(stderr, "perf_test [OPTION]\n");
  fprintf(stderr, "   -u|--update_server update server addr(host:port)\n");
  fprintf(stderr, "   -w|--write_thread_num the number of write thread\n");
  fprintf(stderr, "   -s|--write_start_row the start write row\n");
  fprintf(stderr, "   -e|--write_end_row the end write row\n");
  fprintf(stderr, "   -r|--read_thread_num the number of read thread\n");
  fprintf(stderr, "   -t|--read_start_row the start read row\n");
  fprintf(stderr, "   -d|--read_end_row the end read row\n");
  fprintf(stderr, "   -n|--read_row_num the number of scan row\n");
  fprintf(stderr, "   -m|--read_times the read times\n");

  fprintf(stderr, "   -f|--schema_file the schema file\n");
  fprintf(stderr, "   -g|--read_schema_file the read schema file\n");
  fprintf(stderr, "   -l|--log_file the log file name\n");
  fprintf(stderr, "   -h|--help print help message\n");
  fprintf(stderr, "Example: ./perf_test -u 10.232.36.40:3434 -w 1 -s 0 -e 100000000 "
      "-r 10 -t 0 -d 100000000 -n 50\n");
  fprintf(stderr, "\n");
}

void parse_cmd_line(int argc, char **argv, ConfigParam &clp)
{
  int opt = 0;
  const char* opt_string = "u:w:s:e:r:t:d:n:m:f:g:l:h";
  struct option longopts[] =
  {
    {"update_server", 1, NULL, 'u'},
    {"write_thread_num", 1, NULL, 'w'},
    {"write_start_row", 1, NULL, 's'},
    {"write_end_row", 1, NULL, 'e'},
    {"read_thread_num", 1, NULL, 'r'},
    {"read_start_row", 1, NULL, 't'},
    {"read_end_row", 1, NULL, 'd'},
    {"read_row_num", 1, NULL, 'n'},
    {"read_times", 1, NULL, 'm'},
    {"schema_file", 1, NULL, 'f'},
    {"read_schema_file", 1, NULL, 'g'},
    {"log_file", 1, NULL, 'l'},
    {"help", 0, NULL, 'h'},
    {0, 0, 0, 0}
  };
  init_config_param(clp);
  char log_file_name[1024];
  log_file_name[0] = '\0';

  while ((opt = getopt_long(argc, argv, opt_string, longopts, NULL)) != -1)
  {
    switch (opt)
    {
    case 'u':
      set_server_addr(optarg, g_update_server);
      break;
    case 'w':
      clp.write_thread_num_ = atoi(optarg);
      break;
    case 's':
      clp.write_start_row_ = atoi(optarg);
      break;
    case 'e':
      clp.write_end_row_ = atoi(optarg);
      break;
    case 'r':
      clp.read_thread_num_ = atoi(optarg);
      break;
    case 't':
      clp.read_start_row_ = atoi(optarg);
      break;
    case 'd':
      clp.read_end_row_ = atoi(optarg);
      break;
    case 'l':
      strcpy(log_file_name, optarg);
      TBSYS_LOGGER.setFileName(log_file_name);
      break;
    case 'n':
      clp.read_row_num_ = atoi(optarg);
      break;
    case 'm':
      clp.read_times_ = atoi(optarg);
      break;
    case 'f':
      strcpy(clp.schema_file_, optarg);
      break;
    case 'g':
      strcpy(clp.read_schema_file_, optarg);
      break;
    case '?':
    case 'h':
    default:
      print_usage();
      exit(1);
    }
  }

  if (NULL == clp.schema_file_ || '\0' == clp.schema_file_[0] ||
      NULL == clp.read_schema_file_ || '\0' == clp.read_schema_file_[0])
  {
    print_usage();
    exit(1);
  }

  fprintf(stderr, "write_thread_num=%ld, write_start_row=%ld, write_end_row=%ld, "
      "read_thread_num=%ld, read_start_row=%ld, read_end_row=%ld, read_row_num=%ld, "
      "read_times=%ld, schema_file=%s, read_schema_file=%s\n",
      clp.write_thread_num_, clp.write_start_row_, clp.write_end_row_,
      clp.read_thread_num_, clp.read_start_row_, clp.read_end_row_,
      clp.read_row_num_, clp.read_times_, clp.schema_file_, clp.read_schema_file_);
}

static void sign_handler(const int sig)
{
  switch (sig) {
    case SIGTERM:
    case SIGINT:
      PerformanceTest::stop();
      break;
    default:
      TBSYS_LOG(WARN, "unknown signal, sig=%d", sig);
      break;
  }
}

int main(int argc, char** argv)
{
  int err = OB_SUCCESS;
  ob_init_memory_pool();
  ConfigParam param;
  parse_cmd_line(argc, argv, param);

  //TBSYS_LOGGER.setFileName("./perf_test.log");
  TBSYS_LOGGER.setMaxFileSize(100 * 1024L * 1024L); // 100M per log file
  TBSYS_LOGGER.setLogLevel("INFO");
  PerformanceTest perf_test(param);
  ObSchemaManagerWrapper schema_wrapper;
  tbsys::CConfig config;
  bool ret_val = schema_wrapper.parse_from_file(param.schema_file_, config);
  if (false == ret_val)
  {
    TBSYS_LOG(WARN, "failed to parse schema file, file_name=%s", param.schema_file_);
    exit(1);
  }

  ObSchemaManagerWrapper read_schema_wrapper;
  tbsys::CConfig read_config;
  ret_val = read_schema_wrapper.parse_from_file(param.read_schema_file_, read_config);
  if (false == ret_val)
  {
    TBSYS_LOG(WARN, "failed to parse read schema file, file_name=%s", param.read_schema_file_);
    exit(1);
  }

  err = perf_test.init(schema_wrapper, read_schema_wrapper);
  if (OB_SUCCESS != err)
  {
    TBSYS_LOG(WARN, "failed to init, err=%d", err);
  }

  if (OB_SUCCESS == err)
  {
    err = g_update_client.init(g_update_server);
    if (OB_SUCCESS != err)
    {
      TBSYS_LOG(WARN, "failed to init update client, err=%d", err);
    }
  }

  if (OB_SUCCESS == err)
  {
    signal(SIGPIPE, SIG_IGN);
    signal(SIGHUP, SIG_IGN);
    signal(SIGINT, sign_handler);
    signal(SIGTERM, sign_handler);

    perf_test.run_test();
  }

  g_update_client.destroy();
  return 0;
}



