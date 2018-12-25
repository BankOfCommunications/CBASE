/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_limit.cpp
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#include "ob_limit.h"
#include "common/utility.h"
#include "common/ob_obj_cast.h"
using namespace oceanbase::sql;
using namespace oceanbase::common;

ObLimit::ObLimit()
  :is_instantiated_(false), limit_(-1), offset_(0), input_count_(0), output_count_(0)
{
}

ObLimit::~ObLimit()
{
}

void ObLimit::reset()
{
  is_instantiated_ = false;
  limit_ = -1;
  offset_ = input_count_ = output_count_ = 0;
  org_limit_.reset();
  org_offset_.reset();
  ObSingleChildPhyOperator::reset();
  return;
}

void ObLimit::reuse()
{
  is_instantiated_ = false;
  limit_ = -1;
  offset_ = input_count_ = output_count_ = 0;
  org_limit_.reset();
  org_offset_.reset();
  ObSingleChildPhyOperator::reuse();
}

int ObLimit::set_limit(const ObSqlExpression& limit, const ObSqlExpression& offset)
{
  int ret = OB_SUCCESS;
  org_limit_ = limit;
  org_offset_ = offset;
  org_limit_.set_owner_op(this);
  org_offset_.set_owner_op(this);
  return ret;
}

int ObLimit::get_int_value(const ObSqlExpression& in_val, int64_t& out_val) const
{
  int ret = OB_SUCCESS;
  ObRow input_row;
  const ObObj *result = NULL;
  ObObj casted_cell;
  ObSqlExpression tmp_val = in_val;
  if (in_val.is_empty())
  {
    /* do nothing */
  }
  else if ((ret = tmp_val.calc(input_row, result)) != OB_SUCCESS)
  {
    TBSYS_LOG(WARN, "Failed to calculate, err=%d", ret);
  }
  else if (result->get_type() != ObIntType)
  {
    ObObj type;
    type.set_type(ObIntType);
    if ((ret = obj_cast(*result, type, casted_cell, result)) != OB_SUCCESS)
    {
      TBSYS_LOG(ERROR, "Cast limit/offset value failed, err=%d", ret);
    }
  }
  if (ret == OB_SUCCESS)
  {
    if (result != NULL && (ret = result->get_int(out_val)) != OB_SUCCESS)
    {
      TBSYS_LOG(ERROR, "get_int error [err:%d]", ret);
    }
  }
  return ret;
}

int ObLimit::get_limit(int64_t &limit, int64_t &offset) const
{
  int ret = OB_SUCCESS;
  limit = -1;
  offset = 0;
  if (!is_instantiated_
    && ((ret = get_int_value(org_limit_, limit)) != OB_SUCCESS
    || (ret = get_int_value(org_offset_, offset)) != OB_SUCCESS))
  {
    TBSYS_LOG(WARN, "Get limit/offset value failed, err=%d", ret);
  }
  else if (limit < -1 || offset < 0)
  {
    ret = OB_INVALID_ARGUMENT;
    TBSYS_LOG(WARN, "Invalid arguments, limit=%ld offset=%ld", limit_, offset_);
  }
  return ret;
}

int ObLimit::open()
{
  int ret = OB_SUCCESS;
  input_count_ = 0;
  output_count_ = 0;
  if ((ret = ObSingleChildPhyOperator::open()) != OB_SUCCESS)
  {
    TBSYS_LOG(WARN, "Failed to open child_op, err=%d", ret);
  }
  else if ((ret = get_limit(limit_, offset_)) != OB_SUCCESS)
  {
    TBSYS_LOG(WARN, "Failed to instantiate limit/offset, err=%d", ret);
  }
  else
  {
    is_instantiated_ = true;
  }
  return ret;
}

int ObLimit::close()
{
  is_instantiated_ = false;
  return ObSingleChildPhyOperator::close();
}

int ObLimit::get_row_desc(const common::ObRowDesc *&row_desc) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(NULL == child_op_ || 0 > offset_))
  {
    ret = OB_NOT_INIT;
    TBSYS_LOG(ERROR, "child_op_ is NULL or invalid offset=%ld", offset_);
  }
  else
  {
    ret = child_op_->get_row_desc(row_desc);
  }
  return ret;
}

int ObLimit::get_next_row(const common::ObRow *&row)
{
  int ret = OB_SUCCESS;
  const common::ObRow *input_row = NULL;
  if (OB_UNLIKELY(NULL == child_op_ || 0 > offset_))
  {
    ret = OB_NOT_INIT;
    TBSYS_LOG(ERROR, "child_op_ is NULL or invalid offset=%ld", offset_);
  }
  else
  {
    while (input_count_ < offset_)
    {
      ret = child_op_->get_next_row(input_row);
      if (OB_ITER_END == ret)
      {
        break;
      }
      else if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "child_op failed to get next row, err=%d", ret);
        break;
      }
      else
      {
        ++input_count_;
      }
    } // end while
    if (OB_SUCCESS == ret)
    {
      if (output_count_ < limit_ || 0 > limit_)
      {
        if (OB_SUCCESS != (ret = child_op_->get_next_row(input_row)))
        {
          if (OB_ITER_END != ret)
          {
            TBSYS_LOG(WARN, "child_op failed to get next row, err=%d, limit_=%ld, offset_=%ld, input_count_=%ld, output_count=%ld",
              ret, limit_, offset_, input_count_, output_count_);
          }
        }
        else
        {
          ++output_count_;
          row = input_row;
        }
      }
      else
      {
        ret = OB_ITER_END;
      }
    }
  }
  return ret;
}

namespace oceanbase{
  namespace sql{
    REGISTER_PHY_OPERATOR(ObLimit, PHY_LIMIT);
  }
}

int64_t ObLimit::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  databuff_printf(buf, buf_len, pos, "Limit(limit=");
  pos += org_limit_.to_string(buf + pos, buf_len - pos);
  databuff_printf(buf, buf_len, pos, ", offset=");
  pos += org_offset_.to_string(buf + pos, buf_len - pos);
  databuff_printf(buf, buf_len, pos, ")\n");
  if (NULL != child_op_)
  {
    int64_t pos2 = child_op_->to_string(buf+pos, buf_len-pos);
    pos += pos2;
  }
  return pos;
}


DEFINE_SERIALIZE(ObLimit)
{
  int ret = OB_SUCCESS;
  bool has_limit_count = !org_limit_.is_empty();
  bool has_limit_offset = !org_offset_.is_empty();

#define ENCODE_EXPRESSION(has_part, expr) \
  if (OB_SUCCESS == ret) \
  { \
    if (OB_SUCCESS != (ret = serialization::encode_bool(buf, buf_len, pos, has_part))) \
    { \
      TBSYS_LOG(WARN, "fail to encode " #expr ":ret[%d]", ret); \
    } \
    else if (has_part && OB_SUCCESS != (ret = expr.serialize(buf, buf_len, pos))) \
    { \
      TBSYS_LOG(WARN, "fail to serialize " #expr ":ret[%d]", ret); \
    } \
  }
  ENCODE_EXPRESSION(has_limit_count, org_limit_);
  ENCODE_EXPRESSION(has_limit_offset, org_offset_);
#undef ENCODE_EXPRESSION

  return ret;
}

DEFINE_GET_SERIALIZE_SIZE(ObLimit)
{
  int64_t size = 0;
  bool has_limit_count = !org_limit_.is_empty();
  bool has_limit_offset = !org_offset_.is_empty();
  size += serialization::encoded_length_bool(has_limit_count);
  if (has_limit_count)
    size += org_limit_.get_serialize_size();
  if (has_limit_offset)
    size += org_offset_.get_serialize_size();
  return size;
}


DEFINE_DESERIALIZE(ObLimit)
{
  int ret = OB_SUCCESS;
#define DECODE_EXPRESSION(expr) \
  if (OB_SUCCESS == ret) \
  { \
    bool has_part = false; \
    if (OB_SUCCESS != (ret = serialization::decode_bool(buf, data_len, pos, &has_part))) \
    { \
      TBSYS_LOG(WARN, "fail to decode Limit/Offset expression :ret[%d]", ret); \
    } \
    else if (has_part && OB_SUCCESS != (ret = expr.deserialize(buf, data_len, pos))) \
    { \
      TBSYS_LOG(WARN, "fail to deserialize " #expr ":ret[%d]", ret); \
    } \
  }
  org_limit_.reset();
  DECODE_EXPRESSION(org_limit_);
  org_offset_.reset();
  DECODE_EXPRESSION(org_offset_);
  org_limit_.set_owner_op(this);
  org_offset_.set_owner_op(this);
#undef DECODE_EXPRESSION

  return ret;
}

PHY_OPERATOR_ASSIGN(ObLimit)
{
  int ret = OB_SUCCESS;
  CAST_TO_INHERITANCE(ObLimit);
  reset();
  org_limit_ = o_ptr->org_limit_;
  org_offset_ = o_ptr->org_offset_;
  org_limit_.set_owner_op(this);
  org_offset_.set_owner_op(this);
  return ret;
}

ObPhyOperatorType ObLimit::get_type() const
{
  return PHY_LIMIT;
}
