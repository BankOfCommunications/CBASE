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
 *   yubai <yubai.lk@alipay.com>
 *     - some work details if you want
 */
#include "ob_ups_modify.h"

namespace oceanbase
{
  namespace sql
  {
    int64_t ObUpsModifyWithDmlType::to_string(char* buf, const int64_t buf_len) const
    {
      int64_t pos = 0;
      databuff_printf(buf, buf_len, pos, "ObUpsModifyWithDmlType(dml_type=%s)\n", str_dml_type(dml_type_));
      if (NULL != child_op_)
      {
        pos += child_op_->to_string(buf+pos, buf_len-pos);
      }
      return pos;
    }

    int ObUpsModifyWithDmlType::serialize(char* buf, const int64_t buf_len, int64_t& pos) const
    {
      int err = OB_SUCCESS;
      int64_t new_pos = pos;
      if (OB_SUCCESS != (err = serialization::encode_i32(buf, buf_len, new_pos, dml_type_)))
      {
        TBSYS_LOG(ERROR, "serialize(buf=%p[%ld-%ld])=>%d", buf, new_pos, buf_len, err);
      }
      else
      {
        pos = new_pos;
      }
      return OB_SUCCESS;
    }

    int ObUpsModifyWithDmlType::deserialize(const char* buf, const int64_t data_len, int64_t& pos)
    {
      int err = OB_SUCCESS;
      int64_t new_pos = pos;
      if (OB_SUCCESS != (err = serialization::decode_i32(buf, data_len, new_pos, (int32_t*)&dml_type_)))
      {
        TBSYS_LOG(ERROR, "deserialize(buf=%p[%ld-%ld])=>%d", buf, new_pos, data_len, err);
      }
      else
      {
        pos = new_pos;
      }
      return OB_SUCCESS;
    }

    int64_t ObUpsModifyWithDmlType::get_serialize_size(void) const
    {
      return serialization::encoded_length_i32(dml_type_);
    }

    REGISTER_PHY_OPERATOR(ObUpsModify, PHY_UPS_MODIFY);
    REGISTER_PHY_OPERATOR(ObUpsModifyWithDmlType, PHY_UPS_MODIFY_WITH_DML_TYPE);
  }; // end namespace sql
}; // end namespace oceanbase

