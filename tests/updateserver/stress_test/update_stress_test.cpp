/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 *
 * Version: 0.1: update_stress_test.cpp,v 0.1 2010/11/16 14:32:03 chuanhui Exp $
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

const static int64_t MOD_VAL = 1; // the mod value
const static int64_t NUM_SECTIONS = 1000; // the number of sections
//const static int64_t NUM_TOTAL_ROWS = MOD_VAL * NUM_SECTIONS; // total row num

const static int64_t NUM_INT_UPDATE_THREADS = 1;
const static int64_t NUM_READ_THREADS = 10;

static const char* MERGE_SERVER_ADDR = "10.232.36.40:5656";
static const char* UPDATE_SERVER_ADDR = "10.232.36.40:3434";
static ObServer g_merge_server;
static ObServer g_update_server;
static MockClient g_merge_client;
static MockClient g_update_client;

using namespace oceanbase::common;
using namespace oceanbase::updateserver;
static int set_server_addr(const char* addr_str, ObServer& server);

struct ConfigParam
{
  int64_t mod_val_;
  int64_t num_sections_;
  //int64_t num_total_rows_;
  int64_t num_int_update_threads_;
  int64_t num_other_update_threads_;
  int64_t num_read_threads_;
  int64_t num_other_update_start_;
  int64_t num_other_update_end_;
  int64_t other_update_idx_;
  int64_t start_row_idx_;

  int64_t num_int_columns_;
  int64_t num_other_columns_;
  int64_t scan_num_;

  ObString table_name_;
  ObString int_columns_name_[64];
  ObString other_columns_name_[64];
  ColumnType other_columns_type_[64];
  char schema_file_[64];
};

class ObGenerator
{
  private:
    int64_t min_;
    int64_t max_;
    int64_t mod_val_;
    int64_t idx_;

    int64_t num_;
    static const int64_t SEG_NUM = 10;
  private:
    unsigned short xsubi[3];

  public:
    ObGenerator(int64_t min, int64_t max, int64_t mod_val = -1, int64_t idx = -1)
    {
      min_ = min;
      max_ = max;
      mod_val_ = mod_val;
      idx_ = idx;
      num_ = 0;

      int64_t time = tbsys::CTimeUtil::getTime();
      xsubi[0] = time & 0xFFFF;
      xsubi[1] = (time >> 2) & 0xFFFF;
      xsubi[2] = (time >> 4) & 0xFFFF;

      if (mod_val_ != -1 && idx_ != -1)
      {
        if (min_ % mod_val_ != idx_)
        {
          min_ = min_ + mod_val_ - min_ % mod_val_;
          min_ = min_ + idx_;
        }

        if (max_ % mod_val_ != idx_)
        {
          max_ = max_ - max_ % mod_val_;
          max_ = max_ - mod_val_ + idx_;
        }
      }
    }

    ~ObGenerator()
    {
    }

  public:
    int64_t next_val()
    {
      int64_t rand_val = nrand48(xsubi); // 0 ~ 2 ^32-1
      rand_val = rand_val % SEG_NUM;

      int64_t seg = num_ / (2 * SEG_NUM);
      int64_t next_val = 0;
      if (mod_val_ == -1 && idx_ == -1) // int update threads
      {
        next_val = (seg * SEG_NUM + rand_val) % (max_ - min_ + 1);
        next_val += min_;
      }
      else  // other update threads
      {
        next_val = (seg * SEG_NUM + rand_val) % ((max_ - min_) / mod_val_ + 1);
        next_val = next_val * mod_val_ + min_;
      }

      ++num_;
      return next_val;
    }
};

class UpdateStressTest
{
  private:
    ConfigParam param_;
    static bool is_stop_;

  public:
    UpdateStressTest(const ConfigParam& param)
    {
      param_ = param;
    }

    ~UpdateStressTest()
    {
    }

    int init(ObSchemaManagerWrapper& schema_wrapper)
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
        if (ObIntType == col_type)
        {
          param_.int_columns_name_[param_.num_int_columns_].assign((char*) col_name, strlen(col_name));
          param_.num_int_columns_++;
        }
        else
        {
          param_.other_columns_name_[param_.num_other_columns_].assign((char*) col_name, strlen(col_name));
          param_.other_columns_type_[param_.num_other_columns_] = col_type;
          param_.num_other_columns_++;
        }
      }

      if (param_.num_int_columns_ <= 1)
      {
        TBSYS_LOG(WARN, "num_int_columns should be >= 2, num_int_columns_=%ld", param_.num_int_columns_);
        err = OB_ERROR;
      }

      TBSYS_LOG(INFO, "table_name=%s, num_int_columns=%ld, num_other_columns=%ld, err=%d",
          table_name, param_.num_int_columns_, param_.num_other_columns_, err);
      return err;
    }


    int run_test()
    {
      fprintf(stderr, "int_update_thread_num=%ld, other_update_thread_num=%ld, read_thread_num=%ld\n",
          param_.num_int_update_threads_, param_.num_other_update_threads_, param_.num_read_threads_);
      pthread_t* int_update_threads = new pthread_t[param_.num_int_update_threads_];
      pthread_t* read_threads = new pthread_t[param_.num_read_threads_];
      pthread_t* scan_threads = new pthread_t[param_.scan_num_];

      pthread_t* other_update_threads = new pthread_t[param_.mod_val_];
      ConfigParam* params = new ConfigParam[param_.mod_val_];

      // start int write thread
      for (int64_t i = 0; i < param_.num_int_update_threads_; ++i)
      {
        pthread_create(int_update_threads + i, NULL, int_update_thread_routine,
            &param_);
      }

      // start other write thread
      for (int64_t i = 0; i < param_.num_other_update_threads_; ++i)
      {
        params[i] = param_;
        params[i].num_other_update_start_ = 0;
        params[i].num_other_update_end_ = param_.mod_val_ * param_.num_sections_ - 1;
        params[i].other_update_idx_ = i;
        pthread_create(other_update_threads + i, NULL, other_update_thread_routine,
            params + i);
      }

      // start read thread
      for (int64_t i = 0; i < param_.num_read_threads_; ++i)
      {
        pthread_create(read_threads + i, NULL, read_thread_routine,
            &param_);
      }

      // start scan thread
      for (int64_t i = 0; i < param_.scan_num_; ++i)
      {
        pthread_create(scan_threads + i, NULL, scan_routine, &param_);
      }


      for (int64_t i = 0; i < param_.num_int_update_threads_; ++i)
      {
        pthread_join(int_update_threads[i], NULL);
      }

      for (int64_t i = 0; i < param_.num_other_update_threads_; ++i)
      {
        pthread_join(other_update_threads[i], NULL);
      }

      for (int64_t i = 0; i < param_.num_read_threads_; ++i)
      {
        pthread_join(read_threads[i], NULL);
      }

      for (int64_t i = 0; i < param_.scan_num_; ++i)
      {
        pthread_join(scan_threads[i], NULL);
      }

      delete[] int_update_threads;
      delete[] other_update_threads;
      delete[] read_threads;
      delete[] scan_threads;
      delete[] params;
      return 0;
    }

  public:
    static void stop()
    {
      is_stop_ = true;
    }

  public:
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

    static int build_scan_param(const int64_t num_int_columns, const ObString* int_columns_name,
        const ObVersionRange& version_range, ObScanParam& scan_param)
    {
      int err = OB_SUCCESS;

      if (num_int_columns <= 0 || NULL == int_columns_name)
      {
        err = OB_ERROR;
      }
      else
      {
        for (int64_t i = 0; OB_SUCCESS == err && i < num_int_columns; ++i)
        {
          scan_param.add_column(int_columns_name[i]);
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
        const ColumnType* other_columns_type, ObMutator& mutator)
    {
      int err = OB_SUCCESS;

      for (int64_t i = 0; OB_SUCCESS == err && i < num_other_columns; ++i)
      {
        ObObj value;
        int type = other_columns_type[i];
        bool found = true;
        switch (type)
        {
          case ObVarcharType:
            {
              char str[16];
              int len = ::rand() % 16 + 1;
              for (int i = 0; i < len; ++i)
              {
                str[i] = 'a' + ::rand() % 26;
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

    static int build_get_param_from_mutator(ObMutator& mutator,
        const ObVersionRange& version_range, ObGetParam& get_param)
    {
      int err = OB_SUCCESS;

      ObMutatorCellInfo* cell = NULL;

      while (OB_SUCCESS == err && OB_SUCCESS == (err = mutator.next_cell()))
      {
        err = mutator.get_cell(&cell);
        if (OB_SUCCESS != err || NULL == cell)
        {
          TBSYS_LOG(WARN, "failed to get_cell, err=%d", err);
          err = OB_ERROR;
        }
        else
        {
          ObCellInfo tmp;
          tmp.table_name_ = cell->cell_info.table_name_;
          tmp.row_key_ = cell->cell_info.row_key_;
          tmp.column_name_ = cell->cell_info.column_name_;
          tmp.value_ = cell->cell_info.value_;

          err = get_param.add_cell(tmp);
          if (OB_SUCCESS != err)
          {
            TBSYS_LOG(WARN, "failed to add cell to get_param, err=%d", err);
          }
        }
      }

      if (OB_ITER_END == err)
      {
        err = OB_SUCCESS;
      }
      else if (OB_SUCCESS != err)
      {
        TBSYS_LOG(WARN, "error occurs when translate mutator to get_param, err=%d", err);
      }

      if (OB_SUCCESS == err)
      {
        get_param.set_version_range(version_range);
        get_param.set_is_result_cached(true);
      }

      return err;
    }

    static int verify_int_res(ObScanner& scanner)
    {
      int err = OB_SUCCESS;

      ObCellInfo* scanner_cell = NULL;
      int64_t sum = 0;

      while (OB_SUCCESS == err && OB_SUCCESS == (err = scanner.next_cell()))
      {
        err = scanner.get_cell(&scanner_cell);
        if (OB_SUCCESS != err || NULL == scanner_cell)
        {
          TBSYS_LOG(WARN, "failed to get cell, err=%d", err);
          err = OB_ERROR;
        }
        else
        {
          int type = scanner_cell->value_.get_type();
          if (ObIntType != type)
          {
            TBSYS_LOG(WARN, "invalid type, type=%d", type);
          }
          else
          {
            int64_t tmp_val = 0;
            scanner_cell->value_.get_int(tmp_val);
            sum += tmp_val;
          }
        }
      }

      if (OB_ITER_END == err)
      {
        err = OB_SUCCESS;
      }

      if (OB_SUCCESS == err)
      {
        if (0 != sum)
        {
          TBSYS_LOG(ERROR, "invalid sum, sum=%ld", sum);
          err = OB_ERROR;
        }
      }
      
      return err;
    }

    static int verify_res(ObMutator& mutator, ObScanner& scanner)
    {
      int err = OB_SUCCESS;

      ObMutatorCellInfo* mutator_cell = NULL;
      ObCellInfo* scanner_cell = NULL;

      while (OB_SUCCESS == err)
      {
        int err1 = mutator.next_cell();
        int err2 = scanner.next_cell();
        if (OB_SUCCESS == err1 && OB_SUCCESS == err2)
        {
          // normal
          err = OB_SUCCESS;
        }
        else if (OB_ITER_END == err1 && OB_ITER_END == err2)
        {
          err = OB_ITER_END;
        }
        else
        {
          TBSYS_LOG(WARN, "failed to call next, err1=%d, err2=%d", err1, err2);
          err = OB_ERROR;
        }

        if (OB_SUCCESS == err)
        {
          err1 = mutator.get_cell(&mutator_cell);
          err2 = scanner.get_cell(&scanner_cell);
          if (OB_SUCCESS != err1 || OB_SUCCESS != err2
              || NULL == mutator_cell || NULL == scanner_cell)
          {
            TBSYS_LOG(WARN, "failed to call next cell, err1=%d, err2=%d, "
                "mutator_cell=%p, scanner_cell=%p", err1, err2, mutator_cell, scanner_cell);
            err = OB_ERROR;
          }
          else
          {
            if (mutator_cell->cell_info.table_name_ != scanner_cell->table_name_
                || mutator_cell->cell_info.row_key_ != scanner_cell->row_key_)
            {
              TBSYS_LOG(WARN, "table name or row key not equal");
              err = OB_ERROR;
            }
            else if (mutator_cell->cell_info.column_name_ != scanner_cell->column_name_)
            {
              TBSYS_LOG(WARN, "column name not equal");
              err = OB_ERROR;
            }
            else
            {
              int type1 = mutator_cell->cell_info.value_.get_type();
              int type2 = scanner_cell->value_.get_type();
              if (type1 != type2)
              {
                TBSYS_LOG(WARN, "cell type not equal");
                err = OB_ERROR;
              }
              else
              {
                switch (type1)
                {
                  case ObVarcharType:
                    {
                      ObString str1;
                      ObString str2;
                      mutator_cell->cell_info.value_.get_varchar(str1);
                      scanner_cell->value_.get_varchar(str2);
                      if (str1 != str2)
                      {
                        TBSYS_LOG(WARN, "value is not equal, mutator_str=%.*s, scanner_str=%.*s",
                            (int) str1.length(), str1.ptr(), (int) str2.length(), str2.ptr());
                        err = OB_ERROR;
                      }
                    }
                    break;
                  default:
                    TBSYS_LOG(WARN, "unknown type, omit it, type=%d", type1);
                    err = OB_ERROR;
                    break;
                }
              }
            }
          }
        }
      }

      if (OB_ITER_END == err)
      {
        err = OB_SUCCESS;
      }

      return err;
    }

    static int get_data(ObGetParam& get_param, ObScanner& scanner)
    {
      int err = OB_SUCCESS;
      int64_t timeout = 2 * 1000L * 1000L;

      err = g_merge_client.ups_get(get_param, scanner, timeout);
      if (OB_SUCCESS != err)
      {
        TBSYS_LOG(WARN, "failed to get data ,err=%d", err);
      }

      return err;
    }

    static int scan_data(ObScanParam& scan_param, ObScanner& scanner)
    {
      int err = OB_SUCCESS;

      int64_t timeout = 2 * 1000L * 1000L;

      err = g_merge_client.ups_scan(scan_param, scanner, timeout);
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

    static void* read_thread_routine(void* arg)
    {
      int err = OB_SUCCESS;
      ConfigParam* param = (ConfigParam*) arg;
      int64_t num_total_rows = param->mod_val_ * param->num_sections_;
      ObString table_name = param->table_name_;
      int64_t num_int_columns = param->num_int_columns_;
      ObString* int_columns_name = param->int_columns_name_;
      ObVersionRange version_range;
      version_range.border_flag_.set_min_value();
      version_range.border_flag_.set_max_value();

      ObGenerator generator(0, num_total_rows - 1);

      while (!is_stop_)
      {
        ObGetParam get_param;
        ObScanner scanner;
        int64_t row_idx = generator.next_val() + param->start_row_idx_;
        ObString row_key;
        char row_key_buf[16];
        row_key.assign_buffer(row_key_buf, sizeof(row_key_buf));
        translate_row_key(row_idx, row_key);
        err = build_get_param(table_name, row_key, num_int_columns, int_columns_name,
            version_range, get_param);
        if (OB_SUCCESS != err)
        {
          TBSYS_LOG(WARN, "failed to build get param, err=%d", err);
        }
        else
        {
          err = get_data(get_param, scanner);
        }
        
        bool row_exist = true;
        if (OB_SUCCESS == err)
        {
          ObCellInfo* cell = NULL;
          int64_t sum = 0;
          while (OB_SUCCESS == err && OB_SUCCESS == (err=scanner.next_cell()))
          {
            err = scanner.get_cell(&cell);
            if (OB_SUCCESS != err || NULL == cell)
            {
              TBSYS_LOG(WARN, "failed to get_cell, cell=%p, err=%d", cell, err);
              err = OB_ERROR;
            }
            else
            {
              int64_t type = 0;
              int64_t tmp_val = 0;
              type = cell->value_.get_type();
              if (ObExtendType == type)
              {
                cell->value_.get_ext(tmp_val);
                if (ObActionFlag::OP_ROW_DOES_NOT_EXIST != tmp_val)
                {
                  TBSYS_LOG(WARN, "invalid ext_val, exe_val=%ld", tmp_val);
                  err = OB_ERROR;
                }
                else
                {
                  row_exist = false;
                }
              }
              else if (ObIntType == type)
              {
                cell->value_.get_int(tmp_val);
                sum += tmp_val;
              }
              else
              {
                TBSYS_LOG(WARN, "invalid type, obj_type=%d", type);
                err = OB_ERROR;
              }
            }
          }

          if (OB_ITER_END == err)
          {
            err = OB_SUCCESS;
          }

          if (0 == sum && OB_SUCCESS == err)
          {
            TBSYS_LOG(INFO, "verify read success, row_key=%ld, row_exist=%s", row_idx,
                row_exist ? "true" : "false");
          }
          else
          {
            TBSYS_LOG(WARN, "verify read failed, row_key=%ld, sum=%ld, err=%d",
                row_idx, sum, err);
            int64_t cell_size = get_param.get_cell_size();
            for (int64_t i = 0; i < cell_size; ++i)
            {
              TBSYS_LOG(WARN, "the %ld-th column is %.*s", i, (int) get_param[i]->column_name_.length(),
                  get_param[i]->column_name_.ptr());
            }
          }
        }
      }

      TBSYS_LOG(INFO, "read thread STOP, err=%d", err);
      return NULL;
    }

    static void* scan_routine(void* arg)
    {
      int err = OB_SUCCESS;
      ConfigParam* param = (ConfigParam*) arg;
      ObString table_name = param->table_name_;
      int64_t num_int_columns = param->num_int_columns_;
      ObString* int_columns_name = param->int_columns_name_;
      ObVersionRange version_range;
      version_range.border_flag_.set_min_value();
      version_range.border_flag_.set_max_value();

      ObScanParam scan_param;
      ObScanner scanner;
      ObRange range;
      range.border_flag_.set_min_value();
      range.border_flag_.set_max_value();
      int64_t table_id = OB_INVALID_ID;
      scan_param.set(table_id, table_name, range);

      err = build_scan_param(num_int_columns, int_columns_name, version_range, scan_param);
      if (OB_SUCCESS != err)
      {
        TBSYS_LOG(WARN, "failed to build scan param, err=%d", err);
      }
      else
      {
        bool is_end = false;
        while (OB_SUCCESS == err && false == is_end)
        {
          err = scan_data(scan_param, scanner);
          if (OB_SUCCESS != err)
          {
            TBSYS_LOG(WARN, "failed to scan data, table_name=%.*s, err=%d",
                table_name.length(), table_name.ptr(), err);
          }
          else
          {
            err = verify_int_res(scanner);
            if (OB_SUCCESS != err)
            {
              TBSYS_LOG(WARN, "failed to verify int res, err=%d", err);
            }
          }

          if (OB_SUCCESS == err)
          {
            bool is_full = false;
            int64_t got_item_num = 0;
            err = scanner.get_is_req_fullfilled(is_full, got_item_num);
            if (OB_SUCCESS != err)
            {
              TBSYS_LOG(WARN, "failed to get is_req_fullfilled, err=%d", err);
            }
            else
            {
              is_end = is_full;
            }
          }

          if (OB_SUCCESS == err && !is_end)
          {
            ObString last_row_key;
            err = scanner.get_last_row_key(last_row_key);
            if (OB_SUCCESS != err)
            {
              TBSYS_LOG(WARN, "failed to get last row key, err=%d", err);
            }
            else
            {
              range.border_flag_.unset_min_value();
              range.border_flag_.unset_inclusive_start();
              range.start_key_ = last_row_key;
              scan_param.set(table_id, table_name, range);
            }
          }
        }
      }

      return NULL;
    }

    static void* int_update_thread_routine(void* arg)
    {
      int err = OB_SUCCESS;
      ConfigParam* param = (ConfigParam*) arg;
      ObString table_name = param->table_name_;
      int64_t num_total_rows = param->mod_val_ * param->num_sections_;
      int64_t num_int_columns = param->num_int_columns_;
      ObString* int_columns_name = param->int_columns_name_;
      ObVersionRange version_range;
      version_range.border_flag_.set_min_value();
      version_range.border_flag_.set_max_value();

      ObGenerator generator(0, num_total_rows - 1);
      while (!is_stop_)
      {
        ObMutator mutator;
        ObGetParam get_param;
        ObScanner scanner;
        int64_t row_idx = generator.next_val() + param->start_row_idx_;
        ObString row_key;
        char row_key_buf[16];
        row_key.assign_buffer(row_key_buf, sizeof(row_key_buf));
        translate_row_key(row_idx, row_key);

        err = build_int_write_param(table_name, row_key,
            num_int_columns, int_columns_name, mutator);
        if (OB_SUCCESS != err)
        {
          TBSYS_LOG(WARN, "failed to build int write param, err=%d", err);
        }
        else
        {
          mutator.reset_iter();
          err = build_get_param_from_mutator(mutator, version_range, get_param);
          if (OB_SUCCESS != err)
          {
            TBSYS_LOG(WARN, "failed to build get param from mutator, err=%d", err);
          }
        }

        if (OB_SUCCESS == err)
        {
          mutator.reset_iter();
          err = write_data(mutator);
          if (OB_SUCCESS != err)
          {
            TBSYS_LOG(WARN, "failed to write data, err=%d", err);
          }
        }

        /*
        if (OB_SUCCESS == err)
        {
          err = get_data(get_param, scanner);
          if (OB_SUCCESS != err)
          {
            TBSYS_LOG(WARN, "failed to get data, err=%d", err);
          }
          else
          {
            err = verify_int_res(scanner);
            if (OB_SUCCESS != err)
            {
              TBSYS_LOG(WARN, "failed to verify res, err=%d", err);
            }
          }
        }
        */

        if (OB_SUCCESS == err)
        {
          TBSYS_LOG(INFO, "write int success, row_key=%ld", row_idx);
        }
        else
        {
          TBSYS_LOG(WARN, "write int failed, row_key=%ld, err=%d",
              row_idx, err);
        }
      }

      TBSYS_LOG(INFO, "int write thread STOP, err=%d", err);
      return NULL;
    }

    static void* other_update_thread_routine(void* arg)
    {
      int err = OB_SUCCESS;
      ConfigParam* param = (ConfigParam*) arg;
      ObString table_name = param->table_name_;
      //int64_t num_total_rows = param->mod_val_ * param->num_sections_;
      int64_t num_other_columns = param->num_other_columns_;
      ObString* other_columns_name = param->other_columns_name_;
      ColumnType* other_columns_type = param->other_columns_type_;

      int64_t num_other_update_start = param->num_other_update_start_;
      int64_t num_other_update_end = param->num_other_update_end_;
      int64_t mod_val = param->mod_val_;
      int64_t other_update_idx = param->other_update_idx_;
      ObVersionRange version_range;
      version_range.border_flag_.set_min_value();
      version_range.border_flag_.set_max_value();

      ObGenerator generator(num_other_update_start, num_other_update_end,
          mod_val, other_update_idx);
      while (!is_stop_)
      {
        int64_t row_idx = generator.next_val() + param->start_row_idx_;
        ObString row_key;
        char row_key_buf[16];
        row_key.assign_buffer(row_key_buf, sizeof(row_key_buf));
        translate_row_key(row_idx, row_key);

        ObMutator mutator;
        ObGetParam get_param;
        ObScanner scanner;

        err = build_other_write_param(table_name, row_key,
            num_other_columns, other_columns_name, other_columns_type, mutator);
        if (OB_SUCCESS != err)
        {
          TBSYS_LOG(WARN, "failed to build int write param, err=%d", err);
        }
        else
        {
          mutator.reset_iter();
          err = build_get_param_from_mutator(mutator, version_range, get_param);
          if (OB_SUCCESS != err)
          {
            TBSYS_LOG(WARN, "failed to build get param from mutator, err=%d", err);
          }
        }

        if (OB_SUCCESS == err)
        {
          mutator.reset_iter();
          err = write_data(mutator);
          if (OB_SUCCESS != err)
          {
            TBSYS_LOG(WARN, "failed to write data, err=%d", err);
          }
        }

        if (OB_SUCCESS == err)
        {
          err = get_data(get_param, scanner);
          if (OB_SUCCESS != err)
          {
            TBSYS_LOG(WARN, "failed to get data, err=%d", err);
          }
          else
          {
            err = verify_res(mutator, scanner);
            if (OB_SUCCESS != err)
            {
              TBSYS_LOG(WARN, "failed to verify res, err=%d", err);
            }
          }
        }

        if (OB_SUCCESS == err)
        {
          TBSYS_LOG(INFO, "write other type success, row_key=%ld", row_idx);
        }
        else
        {
          TBSYS_LOG(WARN, "write other type failed, row_key=%ld, err=%d",
              row_idx, err);
        }
      }

      TBSYS_LOG(INFO, "other type write thread STOP, err=%d", err);
      return NULL;
    }
};

bool UpdateStressTest::is_stop_ = false;


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
  param.mod_val_ = MOD_VAL;
  param.num_sections_ = NUM_SECTIONS;
  param.num_int_update_threads_ = NUM_INT_UPDATE_THREADS;
  param.num_other_update_threads_ = param.mod_val_;
  param.num_read_threads_ = NUM_READ_THREADS;
  param.start_row_idx_ = 0;
  set_server_addr(MERGE_SERVER_ADDR, g_merge_server);
  set_server_addr(UPDATE_SERVER_ADDR, g_update_server);
  strcpy(param.schema_file_, "schema.ini");
}

void print_usage()
{
  fprintf(stderr, "\n");
  fprintf(stderr, "update_stress_test [OPTION]\n");
  fprintf(stderr, "   -m|--merge_server merge server addr(host:port)\n");
  fprintf(stderr, "   -u|--update_server update server addr(host:port)\n");
  fprintf(stderr, "   -i|--int_update_thread the number of int update thread\n");
  fprintf(stderr, "   -r|--read_thread the number of read thread\n");
  fprintf(stderr, "   -o|--other_update_thread the number of other update thread\n");
  fprintf(stderr, "   -s|--section_num the section num\n");
  fprintf(stderr, "   -b|--begin_row the begin row idx\n");
  fprintf(stderr, "   -a|--scan_num scan thread num\n");
  fprintf(stderr, "   -f|--schema_ini schema file name\n");
  fprintf(stderr, "Example: ./update_stress_test -m 10.232.36.40:5656 -u 10.232.36.40:3434 -i 10 -r 10 -o 10 -s 100 -b 1000 -a 4\n");
  fprintf(stderr, "\n");
}

void parse_cmd_line(int argc, char **argv, ConfigParam &clp)
{
  int opt = 0;
  const char* opt_string = "m:u:o:s:i:r:b:f:a:h";
  struct option longopts[] =
  {
    {"merge_server", 1, NULL, 'm'},
    {"update_server", 1, NULL, 'u'},
    {"int_update_thread", 1, NULL, 'i'},
    {"other_update_thread", 1, NULL, 'o'},
    {"read_thread", 1, NULL, 'r'},
    {"begin_row", 1, NULL, 'b'},
    {"section_num", 1, NULL, 's'},
    {"schema_ini", 1, NULL, 'f'},
    {"scan_num", 0, NULL, 'a'},
    {"help", 0, NULL, 'h'},
    {0, 0, 0, 0}
  };
  init_config_param(clp);

  while ((opt = getopt_long(argc, argv, opt_string, longopts, NULL)) != -1)
  {
    switch (opt)
    {
    case 'm':
      set_server_addr(optarg, g_merge_server);
      break;
    case 'u':
      set_server_addr(optarg, g_update_server);
      break;
    case 'o':
      clp.num_other_update_threads_ = atoi(optarg);
      if (clp.num_other_update_threads_ > 0)
      {
        clp.mod_val_ = clp.num_other_update_threads_;
      }
      break;
    case 's':
      clp.num_sections_ = atoi(optarg);
      break;
    case 'i':
      clp.num_int_update_threads_ = atoi(optarg);
      break;
    case 'r':
      clp.num_read_threads_ = atoi(optarg);
      break;
    case 'b':
      clp.start_row_idx_ = atoi(optarg);
      break;
    case 'f':
      strcpy(clp.schema_file_, optarg);
      break;
    case 'a':
      clp.scan_num_ = atoi(optarg);
      break;
    case '?':
    case 'h':
    default:
      print_usage();
      exit(1);
    }
  }

  if (NULL == clp.schema_file_ || '\0' == clp.schema_file_[0])
  {
    print_usage();
    exit(1);
  }

  fprintf(stderr, "num_sections=%ld, mod_val=%ld, num_int_update_threads=%ld, "
      "num_other_update_threads=%ld, num_read_threads=%ld, schema_file=%s scan_num=%ld\n",
      clp.num_sections_, clp.mod_val_, clp.num_int_update_threads_,
      clp.num_other_update_threads_, clp.num_read_threads_, clp.schema_file_,
      clp.scan_num_);
}

static void sign_handler(const int sig)
{
  switch (sig) {
    case SIGTERM:
    case SIGINT:
      UpdateStressTest::stop();
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

  TBSYS_LOGGER.setFileName("./stress.log");
  TBSYS_LOGGER.setMaxFileSize(100 * 1024L * 1024L); // 100M per log file
  //TBSYS_LOGGER.setLogLevel("INFO");
  TBSYS_LOGGER.setLogLevel("WARN");
  UpdateStressTest stress_test(param);
  ObSchemaManagerWrapper schema_wrapper;
  tbsys::CConfig config;
  bool ret_val = schema_wrapper.parse_from_file(param.schema_file_, config);
  if (false == ret_val)
  {
    TBSYS_LOG(WARN, "failed to parse schema file, file_name=%s", param.schema_file_);
    exit(1);
  }

  err = stress_test.init(schema_wrapper);
  if (OB_SUCCESS != err)
  {
    TBSYS_LOG(WARN, "failed to init, err=%d", err);
  }

  if (OB_SUCCESS == err)
  {
    err = g_merge_client.init(g_merge_server);
    if (OB_SUCCESS != err)
    {
      TBSYS_LOG(WARN, "failed to init merge client, err=%d", err);
    }
    else
    {
      err = g_update_client.init(g_update_server);
      if (OB_SUCCESS != err)
      {
        TBSYS_LOG(WARN, "failed to init update client, err=%d", err);
      }
    }
  }

  if (OB_SUCCESS == err)
  {
    signal(SIGPIPE, SIG_IGN);
    signal(SIGHUP, SIG_IGN);
    signal(SIGINT, sign_handler);
    signal(SIGTERM, sign_handler);

    stress_test.run_test();
  }

  g_merge_client.destroy();
  g_update_client.destroy();
  return 0;
}



