/*===============================================================
*   (C) 2007-2010 Taobao Inc.
*   
*   
*   Version: 0.1 2010-09-26
*   
*   Authors:
*          daoan(daoan@taobao.com)
*   
*
================================================================*/
#ifndef OCEANBASE_COMMON_OB_RESULT_H_
#define OCEANBASE_COMMON_OB_RESULT_H_
#include "ob_define.h"
#include "ob_string.h"
namespace oceanbase 
{ 
  namespace common 
  {
    struct ObResultCode {
      ObResultCode():result_code_(0)
      {
      }
      INLINE_NEED_SERIALIZE_AND_DESERIALIZE;
      int32_t result_code_; 
      ObString message_; 
    };
    DEFINE_SERIALIZE(ObResultCode)
    {
      int ret = 0;
      int64_t tmp_pos = pos;
      ret = serialization::encode_vi32(buf, buf_len, tmp_pos, result_code_);
      if (OB_SUCCESS == ret)
      {
        ret = message_.serialize(buf, buf_len, tmp_pos);
      }
      if (OB_SUCCESS == ret)
      {
        pos = tmp_pos;
      }
      return ret;
    }
    DEFINE_DESERIALIZE(ObResultCode)
    {
      int ret = OB_SUCCESS;
      int64_t tmp_pos = pos;
      if (OB_SUCCESS == ret)
      {
        ret = serialization::decode_vi32(buf, data_len, tmp_pos, &result_code_);
      }
      if (OB_SUCCESS == ret)
      {
        ret = message_.deserialize(buf, data_len, tmp_pos);
      }
      if (OB_SUCCESS == ret)
      {
        pos = tmp_pos;
      }
      return ret;
    }
    DEFINE_GET_SERIALIZE_SIZE(ObResultCode)
    {
      int64_t len = serialization::encoded_length_vi32(result_code_);
      len += message_.get_serialize_size();
      return len;
    }

  }
}
#endif
