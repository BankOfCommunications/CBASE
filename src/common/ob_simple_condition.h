/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 *
 * Version: 0.1: ob_simple_condition.h,v 0.1 2011/03/17 15:39:30 zhidong Exp $
 *
 * Authors:
 *   chuanhui <xielun.szd@taobao.com>
 *     - some work details if you want
 *
 */

#ifndef OB_SIMPLE_COND_H_
#define OB_SIMPLE_COND_H_

#include "ob_vector.h"
#include "ob_object.h"
#include "ob_string.h"

namespace oceanbase
{
  namespace common
  {
    // system supported logic operator
    enum ObLogicOperator
    {
      NIL   = 0, // invalid op
      LT    = 1, // less than
      LE    = 2, // less or equal
      EQ    = 3, // equal with
      GT    = 4, // greater than
      GE    = 5, // greater or equal
      NE    = 6, // not equal
      LIKE  = 7, // substring
    };

    // simple condition for where clause
    class ObSimpleCond
    {
      //friend class ObSimpleFilter;
    public:
      ObSimpleCond();
      /// constructed column name as left operand
      ObSimpleCond(const ObString & name, const ObLogicOperator oper, const ObObj & value);
      virtual ~ObSimpleCond();

    public:
      // default value for invalid column_index
      static const int64_t INVALID_INDEX = -11;

      /// set content by index as left operand pos, logic operator and the right operand
      int set(const int64_t index, const ObLogicOperator oper, const ObObj & value);

      /// check type is time type:create\modify\precise\datetime
      bool is_timetype(const ObObjType & type) const;

      /// check condition is valide
      bool check(const ObObjType & type) const;

      /// calculate logic result
      bool calc(const common::ObObj & left_operand) const;

      /// get column name of left operand
      const ObString & get_column_name(void) const;
      /// get column index of left operand
      int64_t get_column_index(void) const;

      /// get logic operator
      ObLogicOperator get_logic_operator(void) const;

      /// get right operand
      const ObObj & get_right_operand(void) const;

      /// find substring in src
      static int64_t find(const ObObj & pattern, const uint64_t pattern_sign, const ObObj & src);

      /// calculate pattern string sign for find
      static int calc_sign(const ObObj & operand, uint64_t & sign);

      /// operator == overload
      bool operator == (const ObSimpleCond & other) const;

      /// serailize or deserialization
      NEED_SERIALIZE_AND_DESERIALIZE;
    private:
      // pattern sign for string operand obj
      // no need serailize this value
      uint64_t pattern_sign_;
      // column name or index as left operand
      ObString column_name_;
      int64_t column_index_;
      // logic operator
      ObLogicOperator logic_operator_;
      // right operand
      ObObj right_operand_;
    };

    inline const ObString & ObSimpleCond::get_column_name(void) const
    {
      return column_name_;
    }

    inline int64_t ObSimpleCond::get_column_index(void) const
    {
      return column_index_;
    }

    inline ObLogicOperator ObSimpleCond::get_logic_operator(void) const
    {
      return logic_operator_;
    }

    inline const ObObj & ObSimpleCond::get_right_operand(void) const
    {
      return right_operand_;
    }

    inline bool ObSimpleCond::operator == (const ObSimpleCond & other) const
    {
      return ((column_name_ == other.column_name_)
              && (column_index_ == other.column_index_)
              && (logic_operator_ == other.logic_operator_)
              && (right_operand_ == other.right_operand_));
    }

    inline bool ObSimpleCond::is_timetype(const ObObjType & type) const
    {
      return ((type == ObDateTimeType)
              || (type == ObPreciseDateTimeType)
              || (type == ObCreateTimeType)
              || (type == ObModifyTimeType)
              //add peiouya [DATE_TIME] 20150906:b
              || (type == ObDateType)
              || (type == ObTimeType)
              //add 20150906:e
              );
    }

    template <>
    struct ob_vector_traits<ObSimpleCond>
    {
      typedef ObSimpleCond* pointee_type;
      typedef ObSimpleCond value_type;
      typedef const ObSimpleCond const_value_type;
      typedef value_type* iterator;
      typedef const value_type* const_iterator;
      typedef int32_t difference_type;
    };
  }
}

#endif //OB_SIMPLE_COND_H_

