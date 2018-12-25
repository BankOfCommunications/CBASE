/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 *
 * Version: 0.1: ob_simple_condition.cpp,v 0.1 2011/03/17 15:39:30 zhidong Exp $
 *
 * Authors:
 *   chuanhui <xielun.szd@taobao.com>
 *     - some work details if you want
 *
 */

#include "ob_schema.h"
#include "ob_string_search.h"
#include "ob_simple_condition.h"

using namespace oceanbase::common;

ObSimpleCond::ObSimpleCond()
{
  pattern_sign_ = 0;
  column_index_ = INVALID_INDEX;
  logic_operator_ = NIL;
}

ObSimpleCond::ObSimpleCond(const ObString & name, const ObLogicOperator oper, const ObObj & value)
{
  pattern_sign_ = 0;
  column_index_ = INVALID_INDEX;
  column_name_ = name;
  logic_operator_ = oper;
  right_operand_ = value;
}

int ObSimpleCond::set(const int64_t index, const ObLogicOperator oper, const ObObj & value)
{
  int ret = OB_SUCCESS;
  ObString temp_string;
  column_name_ = temp_string;
  column_index_ = index;
  logic_operator_ = oper;
  right_operand_ = value;
  if (LIKE == logic_operator_)
  {
    ret = calc_sign(right_operand_, pattern_sign_);
    if (ret != OB_SUCCESS)
    {
      logic_operator_ = NIL;
      TBSYS_LOG(ERROR, "calculate sign failed:ret[%d]", ret);
    }
  }
  return ret;
}

ObSimpleCond::~ObSimpleCond()
{
}

bool ObSimpleCond::check(const ObObjType & type) const
{
  bool ret = true;
  if (NIL == logic_operator_)
  {
    TBSYS_LOG(WARN, "%s", "check logic operator is nil");
    ret = false;
  }
  else
  {
    ObObjType operand_type = right_operand_.get_type();
    if ((operand_type != ObNullType) && (operand_type != type))
    {
      // different datetime types can compare
      if (!is_timetype(operand_type) || !is_timetype(type))
      {
        ret = false;
        TBSYS_LOG(WARN, "check column data type not the same as oprand type:"
            "column[%u], operand[%u]", type, operand_type);
      }
    }
    else if ((ObNullType == operand_type) && (logic_operator_ != NE) && (logic_operator_ != EQ))
    {
      ret = false;
      TBSYS_LOG(WARN, "check operand type failed:operand_type[null], operator[%u]",
          logic_operator_);
    }
  }
  return ret;
}

int ObSimpleCond::calc_sign(const ObObj & operand, uint64_t & sign)
{
  ObString pattern;
  int ret = operand.get_varchar(pattern);
  if ((ret != OB_SUCCESS) || (pattern.length() == 0))
  {
    ret = OB_ERROR;
    TBSYS_LOG(ERROR, "get varchar pattern or check length failed:len[%d], ret[%d]",
        pattern.length(), ret);
  }
  else
  {
    sign = ObStringSearch::cal_print(pattern);
  }
  return ret;
}


bool ObSimpleCond::calc(const ObObj & left_operand) const
{
  bool ret = false;
  // skip nulltype for compare return false
  switch (logic_operator_)
  {
    case LT:
    {
      if (ObNullType != left_operand.get_type())
      {
        ret = left_operand < right_operand_;
      }
      break;
    }
    case LE:
    {
      if (ObNullType != left_operand.get_type())
      {
        ret = left_operand <= right_operand_;
      }
      break;
    }
    case EQ:
    {
      ret = left_operand == right_operand_;
      break;
    }
    case NE:
    {
      ret = left_operand != right_operand_;
      break;
    }
    case GT:
    {
      if (ObNullType != left_operand.get_type())
      {
        ret = left_operand > right_operand_;
      }
      break;
    }
    case GE:
    {
      if (ObNullType != left_operand.get_type())
      {
        ret = left_operand >= right_operand_;
      }
      break;
    }
    case LIKE:
    {
      if (ObNullType != left_operand.get_type())
      {
        ret = find(right_operand_, pattern_sign_, left_operand) >= 0;
      }
      break;
    }
    default:
    {
      TBSYS_LOG(ERROR, "check condition logic type failed:type[%d]", logic_operator_);
    }
  }
  return ret;
}

int64_t ObSimpleCond::find(const ObObj & pattern, const uint64_t pattern_sign, const ObObj & src)
{
  int64_t pos = -1;
  ObString str_pattern;
  ObString str_src;
  int ret = pattern.get_varchar(str_pattern);
  if (ret != OB_SUCCESS)
  {
    TBSYS_LOG(ERROR, "get varchar type failed from pattern:[%d]", ret);
  }
  else
  {
    ret = src.get_varchar(str_src);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(ERROR, "get varchar type failed from src:[%d]", ret);
    }
    else
    {
      pos = ObStringSearch::kr_search(str_pattern, pattern_sign, str_src);
    }
  }
  return pos;
}

DEFINE_SERIALIZE(ObSimpleCond)
{
  int ret = OB_SUCCESS;
  ObObj obj;
  // column index
  if (INVALID_INDEX == column_index_)
  {
    obj.set_varchar(column_name_);
  }
  else
  {
    obj.set_int(column_index_);
  }
  ret = obj.serialize(buf, buf_len, pos);
  if (OB_SUCCESS != ret)
  {
    TBSYS_LOG(ERROR, "serialize column_index or column name failed:index[%ld], name[%.*s], ret[%d]",
        column_index_, column_name_.length(), column_name_.ptr(), ret);
  }
  
  // logic operator
  if (OB_SUCCESS == ret)
  {
    obj.set_int(logic_operator_);
    ret = obj.serialize(buf, buf_len, pos);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(ERROR, "serialize operator failed:operator[%d], ret[%d]",
          logic_operator_, ret);
    }
  }
  
  // right operand
  if (OB_SUCCESS == ret)
  {
    ret = right_operand_.serialize(buf, buf_len, pos);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(ERROR, "serialize operand failed:ret[%d]", ret);
    }
  }
  // no need serialize the pattern sign for LIKE Operand
  return ret;
}


DEFINE_DESERIALIZE(ObSimpleCond)
{
  ObObj obj;
  // column_index
  int ret = obj.deserialize(buf, data_len, pos);
  if (OB_SUCCESS != ret)
  {
    TBSYS_LOG(ERROR, "deserialize column_index failed:ret[%d]", ret);
  }
  else if (ObIntType == obj.get_type())
  {
    ObString temp_string;
    column_name_ = temp_string;
    ret = obj.get_int(column_index_);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(ERROR, "get column index int value failed:ret[%d]", ret);
    }
  }
  else
  {
    column_index_ = INVALID_INDEX;
    ret = obj.get_varchar(column_name_);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(ERROR, "get column name value failed:ret[%d]", ret);
    }
  }
  
  // logic operator
  if (OB_SUCCESS == ret)
  {
    ret = obj.deserialize(buf, data_len, pos);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(ERROR, "deserialize operator failed:ret[%d]", ret);
    }
    else
    {
      int64_t value = 0;
      ret = obj.get_int(value);
      if (ret != OB_SUCCESS)
      {
        TBSYS_LOG(ERROR, "get operator int value failed:ret[%d]", ret);
      }
      else
      {
        logic_operator_ = (ObLogicOperator)value;
      }
    }
  }
  
  // right operand
  if (OB_SUCCESS == ret)
  {
    ret = right_operand_.deserialize(buf, data_len, pos);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(ERROR, "deserialize operand failed:ret[%d]", ret);
    }
    // compute pattern sign
    else if (LIKE == logic_operator_)
    {
      ret = calc_sign(right_operand_, pattern_sign_);
      if (ret != OB_SUCCESS)
      {
        TBSYS_LOG(ERROR, "calculate sign failed:ret[%d]", ret);
      }
    }
  }
  return ret;
}


DEFINE_GET_SERIALIZE_SIZE(ObSimpleCond)
{
  ObObj obj;
  if (INVALID_INDEX == column_index_)
  {
    obj.set_varchar(column_name_);
  }
  else
  {
    obj.set_int(column_index_);
  }
  // no need serialize the pattern sign for LIKE Operand
  int64_t total_size = obj.get_serialize_size();
  obj.set_int(logic_operator_);
  total_size += obj.get_serialize_size();
  total_size += right_operand_.get_serialize_size();
  return total_size;
}


