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
#include "updateserver/ob_ups_lock_filter.h"
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
      int64_t thread_num;
      int64_t max_row_num;
      Config()
      {
        thread_num = 1;
        max_row_num = 100;
      }
    };
    class ObMockLockFilter: public ObUpsLockFilter
    {
      public:
        ObMockLockFilter(RWSessionCtx& session_ctx): ObUpsLockFilter(session_ctx) {}
        virtual ~ObMockLockFilter(){}
      protected:
        int get_rowkey_obj_cnt(const uint64_t table_id, int64_t& rowkey_obj_cnt) {
          UNUSED(table_id);
          rowkey_obj_cnt = 3;
          return 0;
        }
#if 0        
        int lock_row(const uint64_t table_id, const common::ObRowkey& rowkey) {
          UNUSED(table_id);
          UNUSED(rowkey);
          return 0;
        }
#endif
    };

    int set_row_desc(ObRowDesc& row_desc, int64_t table_id, int64_t n_col)
    {
      for(int64_t col_id = 0; col_id < n_col; col_id++) {
        row_desc.add_column_desc(table_id, col_id + 16);
      }
      row_desc.set_rowkey_cell_count(1);
      return 0;
    }

    ObRow& build_row(ObRowDesc& row_desc, ObRow& row) {
      int err = OB_SUCCESS;
      row.set_row_desc(row_desc);
      for (int64_t j = 0; OB_SUCCESS == err && j < row_desc.get_column_num(); j++)
      {
        uint64_t table_id = OB_INVALID_ID;
        uint64_t column_id = OB_INVALID_ID;
        row_desc.get_tid_cid(j, table_id, column_id);
        ObObj obj;
        obj.set_int(j);
        if (OB_SUCCESS != (err = row.set_cell(table_id, column_id, obj)))
        {
          TBSYS_LOG(ERROR, "set_cell()=>%d", err);
        }
      }
      return row;
    }
    int build_values(ObRowDesc& row_desc, ObValues& values, int64_t row_count) {
      int err = OB_SUCCESS;
      for(int64_t i = 0; OB_SUCCESS == err && i < row_count; i++){
        ObRow row;
        if (OB_SUCCESS != (err = values.add_values(build_row(row_desc, row))))
        {
          TBSYS_LOG(ERROR, "values.add_values()=>%d", err);
        }
      }
      return err;
    }
    class ObLockFilterTest: public ::testing::Test, public Config, public ObMockUps, public RWT {
      public:
        ObLockFilterTest(){ RWT::set(0, thread_num); }
        ~ObLockFilterTest(){}
      protected:
        int64_t n_error;
        int64_t n_filter;
        int64_t n_row;
        ObRowDesc row_desc;
        MemTable memtable;
      protected:
        virtual void SetUp(){
          ASSERT_EQ(OB_SUCCESS, ObMockUps::init(*this));
          srandom(static_cast<unsigned int>(time(NULL)));
          n_error = 0;
          n_filter = 0;
          n_row = 0;
          set_row_desc(row_desc, 258, 14);
          ASSERT_EQ(OB_SUCCESS, memtable.init());
        }
        virtual void TearDown(){
        }
        int report() {
          int err = OB_SUCCESS;
          TBSYS_LOG(INFO, "n_thread=%ld,n_error=%ld,n_filte=%ld,n_row=%ld", thread_num, n_error, n_filter, n_row);
          return err;
        }
        int64_t check_error(){ return n_error; }
        int read(const int64_t idx) {
          int err = OB_SUCCESS;
          UNUSED(idx);
          return err;
        }
        int rand_values(ObValues& values) {
          values.set_row_desc(row_desc);
          return build_values(row_desc, values, random()% max_row_num);
        }
        int lock_rows(){
          int err = OB_SUCCESS;
          ObMockLockFilter* lock_filter = NULL;
          ObValues values;
          const ObRow* row = NULL;
          SessionGuard session_guard(*this);
          RWSessionCtx* session_ctx = (RWSessionCtx*)session_guard.session_ctx_;
          if (OB_SUCCESS != (err = rand_values(values)))
          {}
          else if (NULL == session_ctx)
          {
            err = OB_ERR_UNEXPECTED;
            TBSYS_LOG(ERROR, "session_ctx == NULL");
          }
          else
          {
            lock_filter = new ObMockLockFilter(*session_ctx);
            lock_filter->set_child(0, values);
          }
          if (OB_SUCCESS != err)
          {}
          else if (OB_SUCCESS != (err = lock_filter->open()))
          {
            TBSYS_LOG(ERROR, "open()=>%d", err);
          }
          while(OB_SUCCESS == err)
          {
            if (OB_SUCCESS != (err = lock_filter->get_next_row(row))
                && OB_ITER_END != err)
            {
              __sync_fetch_and_add(&n_error, 1);
            }
            else if (OB_ITER_END == err)
            {
              __sync_fetch_and_add(&n_filter, 1);
            }
            else
            {
              __sync_fetch_and_add(&n_row, 1);
            }
          }
          if (OB_ITER_END == err)
          {
            err = OB_SUCCESS;
          }
          if (OB_SUCCESS != err)
          {}
          else if (OB_SUCCESS != (err = lock_filter->close()))
          {
            TBSYS_LOG(ERROR, "lock_filter.close()=>%d", err);
          }
          if (NULL != lock_filter)
          {
            delete lock_filter;
          }
          return err;
        }
          
        int write(const int64_t idx) {
          int err = OB_SUCCESS;
          UNUSED(idx);
          while(!stop_ && OB_SUCCESS == err)
          {
            err = lock_rows();
          }
          return err;
        }
    };
    RWT_def(ObLockFilterTest);
  }
}
