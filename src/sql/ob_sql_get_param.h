/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * ob_get_param.h for define get param
 *
 * Authors:
 *   huating <huating.zmq@taobao.com>
 *   xiaochu <xiaochu.yh@alipay.com>
 *
 */
#ifndef OCEANBASE_SQL_GET_PARAM_H_
#define OCEANBASE_SQL_GET_PARAM_H_

#include "common/ob_common_param.h"
#include "common/ob_rowkey.h"
#include "common/page_arena.h"
#include "common/ob_array.h"
#include "ob_sql_read_param.h"

namespace oceanbase
{
  namespace sql
  {
    class ObSqlGetParam : public ObSqlReadParam
    {
      static const int64_t DEFAULT_ROW_BUF_SIZE = 64 * 1024; // 64KB
      static const int64_t MAX_ROW_CAPACITY  = 10240;
      static const int64_t DEFAULT_ROWKEY_LIST_NUM = 1024;
    public:
      explicit ObSqlGetParam();
      virtual ~ObSqlGetParam ();
      void reset();
      void reset_local();
      int destroy();
      inline ObStringBuf* get_buffer_pool()
      {
        return &buffer_pool_;
      }

      /// data interface
      /// if deep_copy is true, will copy all content of rowkey
      /// if false, only copy rowkey.ptr pointer and count val
      int add_rowkey(const common::ObRowkey& rowkey, bool deep_copy = false);
      /// read rowkey in rowkey_array
      inline const common::ObRowkey* operator[](int64_t index) const;
      inline int64_t get_row_size() const;

      int64_t to_string(char *buf, int64_t buf_size) const;
      void print_memory_usage(const char* msg) const;

      virtual int assign(const ObSqlReadParam* other);
      NEED_SERIALIZE_AND_DESERIALIZE;
      //add zhujun [transaction read uncommit]2016/3/25
      inline int set_trans_id(ObTransID trans_id) {
          trans_id_ = trans_id;
          return OB_SUCCESS;
      }
      inline ObTransID get_trans_id() const { return trans_id_;}
      //add:e
    private:
      typedef ObSEArray<common::ObRowkey, OB_PREALLOCATED_NUM> RowkeyListType;
    private:
      int init();
      int copy_rowkey(const common::ObRowkey &rowkey, common::ObRowkey &stored_rowkey);

      /// serialize & deserialize
      int serialize_flag(char* buf, const int64_t buf_len, int64_t& pos,
          const int64_t flag) const;

      int serialize_int(char* buf, const int64_t buf_len, int64_t& pos,
          const int64_t value) const;

      int deserialize_int(const char* buf, const int64_t data_len, int64_t& pos,
          int64_t& value) const;

      int64_t get_obj_serialize_size(const int64_t value, bool is_ext=false) const;

      int serialize_basic_field(char* buf,
          const int64_t buf_len,
          int64_t& pos) const;

      int deserialize_basic_field(const char* buf,
          const int64_t data_len, int64_t& pos);

      int64_t get_basic_field_serialize_size(void) const;

      int serialize_rowkeys(char* buf, const int64_t buf_len,
          int64_t& pos, const RowkeyListType & rowkey_list) const;

      int deserialize_rowkeys(const char* buf, const int64_t data_len,
          int64_t& pos);
      int64_t get_serialize_rowkeys_size(const RowkeyListType & rowkey_list) const;
    private:
      DISALLOW_COPY_AND_ASSIGN(ObSqlGetParam);
      int32_t max_row_capacity_;
      RowkeyListType rowkey_list_;
      ObStringBuf buffer_pool_;
      //add zhujun [transaction read uncommit]2016/3/25
      ObTransID trans_id_;

    };

    inline const common::ObRowkey* ObSqlGetParam::operator[](int64_t index) const
    {
      const common::ObRowkey* rowkey = NULL;
      if (index >= 0 && index < get_row_size())
      {
        rowkey = &rowkey_list_.at(index);
      }
      return rowkey;
    }

    inline int64_t ObSqlGetParam::get_row_size() const
    {
      return rowkey_list_.count();
    }


  } /* common */
} /* oceanbase */

#endif /* end of include guard: OCEANBASE_SQL_GET_PARAM_H_ */
