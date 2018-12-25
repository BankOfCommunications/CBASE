/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 *
 * Version: 0.1: ob_simple_filter.h,v 0.1 2011/03/17 15:39:30 zhidong Exp $
 *
 * Authors:
 *   chuanhui <xielun.szd@taobao.com>
 *     - some work details if you want
 *
 */

#ifndef OB_SIMPLE_FILTER_H_
#define OB_SIMPLE_FILTER_H_

#include "ob_array_helper.h"
#include "ob_string_buf.h"
#include "ob_simple_condition.h"

namespace oceanbase
{
  namespace common
  {
    class ObCellArray;
    // the container of all logic and conditions using for logic calculate
    // when add condition item, deeply copy the right operand object especially for string
    class ObSimpleFilter
    {
    public:
      ObSimpleFilter();
      // will be removed after scan_param refactor
      ObSimpleFilter(const ObSimpleFilter & other);
      virtual ~ObSimpleFilter();
    
    public:
      /// add a condition by column name used by client api
      int add_cond(const ObString & column_name, const ObLogicOperator & cond_op,
          const ObObj & cond_value);
    
    public:
      /// add a condition by column index used as inner interface
      int add_cond(const uint64_t column_index, const ObLogicOperator & cond_op,
          const ObObj & cond_value);
      
      /// random access condition item
      const ObSimpleCond * operator [] (const int64_t index) const;

      /// do check all conditions
      /// require condition.column_index + row_begin <= row_end
      int check(const ObCellArray & cells, const int64_t row_begin, const int64_t row_end,
          bool & result) const;

      /// use to do expire filter check
      int default_false_check(const ObObj* objs, const int64_t obj_count, bool& result);
      
      /// reset for reusing this instance
      void reset(void);
      
      /// get condition count
      int64_t get_count(void) const;

      /// operator = overload
      int operator = (const ObSimpleFilter & other);

      /// operator == overload
      bool operator == (const ObSimpleFilter & other) const;

      int safe_copy(const ObSimpleFilter &other);

      /// serailize or deserialization
      NEED_SERIALIZE_AND_DESERIALIZE;

    private:
      /// add a condition item
      int add_cond(const ObSimpleCond & cond);
      /// deep copy obj cond_value to store_value
      int copy_obj(const ObObj & cond_value, ObObj & store_value);

    private:
      ObSimpleCond  condition_buf_[OB_MAX_COLUMN_NUMBER];
      ObArrayHelper<ObSimpleCond> conditions_;
      ObStringBuf string_buffer_;
    };

    inline int64_t ObSimpleFilter::get_count(void) const
    {
      return conditions_.get_array_index();
    }
  }
}


#endif //OB_SIMPLE_FILTER_H_

