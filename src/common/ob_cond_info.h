/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 *
 * Version: 0.1: ob_cond_info.h,v 0.1 2011/11/29 10:06:35 xielun.szd Exp $
 *
 * Authors:
 *   xielun <xielun.szd@taobao.com>
 *
 */

#ifndef OCEANBASE_CONDITION_INFO_H_
#define OCEANBASE_CONDITION_INFO_H_

#include "ob_object.h"
#include "ob_string.h"
#include "ob_vector.h"
#include "ob_action_flag.h"
#include "ob_common_param.h"
#include "ob_simple_condition.h"
#include "ob_rowkey.h"
#include "ob_schema.h"
#include "ob_string_buf.h"
namespace oceanbase
{
  namespace common
  {
    class ObCondInfo
    {
    public:
      ObCondInfo();
      ~ObCondInfo();
      void reset();
    public:
      // get number interface
      inline ObLogicOperator get_operator(void) const;
      inline const ObCellInfo & get_cell(void) const;
      inline ObCellInfo & get_cell(void);

      // using ext obj for row exist op
      inline bool is_exist_type(void) const;

      // set cell info with name type and logic operator
      void set(const ObLogicOperator op_type, const ObString & table_name,
          const ObRowkey& row_key, const ObString & column_name, const ObObj & value);
      void set(const ObString & table_name, const ObRowkey& row_key, const bool is_exist);

      // set cell info using id type and logic operator
      void set(const ObLogicOperator op_type, const uint64_t table_id, const ObRowkey& row_key,
          const uint64_t column_id, const ObObj & value);
      void set(const uint64_t table_id, const ObRowkey& row_key, const bool is_exist);

      // set cell info
      void set(const ObLogicOperator op_type, const ObCellInfo & cell);

      // deep copy the cell content into string buffer
      int deep_copy(ObCondInfo & dest, ObStringBuf & buffer) const;

      // for unit test
      bool operator == (const ObCondInfo & other) const;

      inline void set_compatible_rowkey_info(const ObRowkeyInfo* rowkey_info) {rowkey_info_ = rowkey_info;};

      NEED_SERIALIZE_AND_DESERIALIZE;
    private:
      ObLogicOperator op_type_;
      ObCellInfo cell_;
      const ObRowkeyInfo *rowkey_info_;
      ObObj rowkey_col_buf_[common::OB_MAX_ROWKEY_COLUMN_NUMBER];
    };

    ObLogicOperator ObCondInfo::get_operator(void) const
    {
      return op_type_;
    }

    const ObCellInfo & ObCondInfo::get_cell(void) const
    {
      return cell_;
    }

    ObCellInfo & ObCondInfo::get_cell(void)
    {
      return cell_;
    }

    bool ObCondInfo::is_exist_type(void) const
    {
      bool ret = false;
      if (NIL == op_type_)
      {
        int64_t ext_type = cell_.value_.get_ext();
        ret = (ObActionFlag::OP_ROW_DOES_NOT_EXIST == ext_type) || (ObActionFlag::OP_ROW_EXIST == ext_type);
      }
      return ret;
    }

    template <>
    struct ob_vector_traits<ObCondInfo>
    {
      typedef ObCondInfo* pointee_type;
      typedef ObCondInfo value_type;
      typedef const ObCondInfo const_value_type;
      typedef value_type* iterator;
      typedef const value_type* const_iterator;
      typedef int32_t difference_type;
    };
  }
}

#endif // OCEANBASE_CONDITION_INFO_H_
