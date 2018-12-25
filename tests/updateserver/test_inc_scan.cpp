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
 *   yuanqi <yuanqi.xhf@taobao.com>
 *     - some work details if you want
 */
#include "test_base.h"
#include "sql/ob_values.h"
#include "updateserver/ob_ups_inc_scan.h"
#include "updateserver/ob_memtable_modify.h"
#include "ob_mock_ups.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;
using namespace oceanbase::updateserver;
namespace oceanbase
{
  namespace test
  {
    struct Config: public BaseConfig
    {
      int64_t mem_limit;
      int64_t read_thread_num;
      int64_t write_thread_num;
      int64_t max_row_num;
      Config()
      {
        mem_limit = 1<<20;
        read_thread_num = 1;
        write_thread_num = 1;
        max_row_num = 100;
      }
    };

    class ObMockIncScan: public ObUpsIncScan
    {
      public:
        ObMockIncScan(ObUpsTableMgr* table_mgr, BaseSessionCtx& session_ctx):
          ObUpsIncScan(session_ctx), table_mgr_(table_mgr) {}
        ~ObMockIncScan(){}
        ObUpsTableMgr* get_table_mgr(){ return table_mgr_; }
      private:
        ObUpsTableMgr* table_mgr_;
    };
    class ObIncScanTest: public ::testing::Test, public Config, public ObMockUps, public RWT {
      public:
        ObIncScanTest(){ RWT::set(read_thread_num, write_thread_num); }
        ~ObIncScanTest(){}
      protected:
        int64_t n_read_error;
        int64_t n_write_error;
        int64_t n_read;
        int64_t n_write;
        int64_t n_read_rows;
        int64_t n_write_rows;
        ObRowDesc row_desc;
      protected:
        virtual void SetUp(){
          ASSERT_EQ(OB_SUCCESS, ObMockUps::init(*this));
          srandom(static_cast<unsigned int>(time(NULL)));
          n_read_error = 0;
          n_write_error = 0;
          n_read = 0;
          n_write = 0;
          n_read_rows = 0;
          n_write_rows = 0;
          //schema_mgr_.print(stdout);
        }
        virtual void TearDown(){
        }
        int64_t check_error() { return n_read_error + n_write_error; }
        int read_rows() {
          int err = OB_SUCCESS;
          RowBuilder row_builder("a2", 0, schema_mgr_);
          ObMockIncScan* scan_op = NULL;
          ObGetParam get_param;
          ObValues values;
          const ObRow* row = NULL;
          SessionGuard session_guard(*this);
          RWSessionCtx* session_ctx = (RWSessionCtx*)session_guard.session_ctx_;
          if (OB_SUCCESS != (err = row_builder.build_values(values, random()% max_row_num + 1)))
          {
            TBSYS_LOG(ERROR, "rand_values()=>%d", err);
          }
          else if (false && OB_SUCCESS != (err = test_values(values)))
          {
            TBSYS_LOG(ERROR, "test_values()=>%d", err);
          }
          else if (OB_SUCCESS != (err = values.open()))
          {}
          else if (NULL == session_ctx)
          {
            err = OB_ERR_UNEXPECTED;
            TBSYS_LOG(ERROR, "session_ctx == NULL");
          }
          else
          {
            session_ctx->get_uc_info().host = NULL;
            scan_op = new ObMockIncScan(&get_table_mgr(), *session_ctx);
            scan_op->set_scan_type(ObIncScan::ST_MGET);
            if (OB_SUCCESS != (err = row_builder.build_get_param(values, *scan_op->get_get_param())))
            {
              TBSYS_LOG(ERROR, "build_get_param()=>%d", err);
            }
          }
          if (OB_SUCCESS != err)
          {}
          else if (OB_SUCCESS != (err = scan_op->open()))
          {
            TBSYS_LOG(ERROR, "open()=>%d", err);
          }
          while(OB_SUCCESS == err)
          {
            if (OB_SUCCESS != (err = scan_op->get_next_row(row))
                && OB_ITER_END != err)
            {
              __sync_fetch_and_add(&n_read_error, 1);
            }
            else if (OB_ITER_END == err)
            {
              __sync_fetch_and_add(&n_read, 1);
            }
            else
            {
              __sync_fetch_and_add(&n_read_rows, 1);
            }
          }
          if (OB_ITER_END == err)
          {
            err = OB_SUCCESS;
          }
          if (OB_SUCCESS != err)
          {}
          else if (OB_SUCCESS != (err = scan_op->close()))
          {
            TBSYS_LOG(ERROR, "scan_op.close()=>%d", err);
          }
          else if (OB_SUCCESS != (err = values.close()))
          {
            TBSYS_LOG(ERROR, "values.close()=>%d", err);
          }
          if (NULL != scan_op)
          {
            delete scan_op;
          }
          return err;
        }
        int write_rows() {
          __sync_fetch_and_add(&n_write, 1);
          int err = OB_SUCCESS;
          MemTableModify* modify = NULL;
          RowBuilder row_builder("a1", 0, schema_mgr_);
          ObValues values;
          const ObRow* row = NULL;
          SessionGuard session_guard(*this);
          RWSessionCtx* session_ctx = (RWSessionCtx*)session_guard.session_ctx_;
          if (OB_SUCCESS != (err = row_builder.build_values(values, random()% max_row_num + 1)))
          {
            TBSYS_LOG(ERROR, "rand_values()=>%d", err);
          }
          else if (NULL == session_ctx)
          {
            err = OB_ERR_UNEXPECTED;
            TBSYS_LOG(ERROR, "session_ctx == NULL");
          }
          else
          {
            session_ctx->get_uc_info().host = NULL;
            modify = new MemTableModify(*session_ctx, table_mgr_);
            modify->set_child(0, values);
          }
          if (OB_SUCCESS != err)
          {}
          else if (OB_SUCCESS != (err = modify->open()))
          {
            TBSYS_LOG(ERROR, "open()=>%d", err);
          }
          while(OB_SUCCESS == err)
          {
            if (OB_SUCCESS != (err = modify->get_next_row(row))
                && OB_ITER_END != err)
            {
              __sync_fetch_and_add(&n_write_error, 1);
            }
            else if (OB_ITER_END == err)
            {
              __sync_fetch_and_add(&n_write, 1);
            }
            else
            {
              __sync_fetch_and_add(&n_write_rows, 1);
            }
          }
          if (OB_ITER_END == err)
          {
            err = OB_SUCCESS;
          }
          if (OB_SUCCESS != err)
          {}
          else if (OB_SUCCESS != (err = modify->close()))
          {
            TBSYS_LOG(ERROR, "modify.close()=>%d", err);
          }
          if (NULL != modify)
          {
            delete modify;
          }
          return err;
        }
        int report() {
          int err = OB_SUCCESS;
          TBSYS_LOG(INFO, "thread[%ld:%ld],read=[%ld:%ld:%ld],write=[%ld:%ld:%ld]",
                    read_thread_num, write_thread_num,
                    n_read, n_read_rows, n_read_error,
                    n_write, n_write_rows, n_write_error);
          return err;
        }
        int read(const int64_t idx) {
          int err = OB_SUCCESS;
          UNUSED(idx);
          while(!stop_ && OB_SUCCESS == err)
          {
            err = read_rows();
          }
          return err;
        }
        int write(const int64_t idx) {
          int err = OB_SUCCESS;
          UNUSED(idx);
          while(!stop_ && OB_SUCCESS == err)
          {
            err = write_rows();
          }
          return err;
        }
    };
    RWT_def(ObIncScanTest);
  }
}
