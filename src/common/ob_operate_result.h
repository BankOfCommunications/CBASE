#ifndef OCEANBASE_COMMON_OPERATE_RESULT_H_
#define OCEANBASE_COMMON_OPERATE_RESULT_H_

#include "ob_define.h"

namespace oceanbase
{
  namespace common
  {
    struct ObOperateResult
    {
      ObOperateResult() : err_step(0), err_code(0), affected(0) {}
      int32_t err_step;
      int32_t err_code;
      int32_t affected;
      ObString err_msg;

      INLINE_NEED_SERIALIZE_AND_DESERIALIZE;
    };

    DEFINE_SERIALIZE(ObOperateResult)
    {
      int ret = OB_ERROR;
      ret = serialization::encode_vi32(buf, buf_len, pos, err_step);

      if (ret == OB_SUCCESS)
        ret = serialization::encode_vi32(buf, buf_len, pos, err_code);

      if (ret == OB_SUCCESS)
        ret = serialization::encode_vi32(buf, buf_len, pos, affected);

      if (ret == OB_SUCCESS)
        ret = err_msg.serialize(buf, buf_len, pos);

      return ret;
    }

    DEFINE_DESERIALIZE(ObOperateResult)
    {
      int ret = OB_ERROR;
      ret = serialization::decode_vi32(buf, data_len, pos, &err_step);

      if (ret != OB_SUCCESS)
        ret = serialization::decode_vi32(buf, data_len, pos, &err_code);

      if (ret != OB_SUCCESS)
        ret = serialization::decode_vi32(buf, data_len, pos, &affected);

      if (ret != OB_SUCCESS)
        ret = err_msg.deserialize(buf, data_len, pos);

      return ret;
    }

    DEFINE_GET_SERIALIZE_SIZE(ObOperateResult)
    {
      int64_t total_size = 0;
      
      total_size += serialization::encoded_length_vi32(err_step);
      total_size += serialization::encoded_length_vi32(err_code);
      total_size += serialization::encoded_length_vi32(affected);
      total_size += err_msg.get_serialize_size();

      return total_size;
    }

  } /* common */
} /* oceanbase */

#endif /* end of include guard: OCEANBASE_COMMON_OPERATE_RESULT_H_ */
