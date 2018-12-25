#include "ob_flag.h"

using namespace oceanbase;
using namespace oceanbase::common;

int oceanbase::common::string_copy(char* dst, const char* src, int64_t max_length)
{
  int ret = OB_SUCCESS;
  int64_t src_len = 0;
  if(OB_SUCCESS == ret && NULL == src)
  {
    ret = OB_INVALID_ARGUMENT;
    TBSYS_LOG(WARN, "src string is null");
  }
  else
  {
    src_len = strlen(src);
  }
  if(OB_SUCCESS == ret && src_len >= max_length)
  {
    ret = OB_SIZE_OVERFLOW;
    TBSYS_LOG(WARN, "src string length[%ld] is greater than dst buffer[%ld]", src_len, max_length);
  }
  if(OB_SUCCESS == ret)
  {
    memcpy(dst, src, src_len);
    dst[src_len] = '\0';
  }
  return ret;
}

int oceanbase::common::decode_string(char* str_buf, const char* buf, const int64_t data_len, int64_t& pos,  int64_t max_length)
{ 
  int ret = OB_SUCCESS;
  const char* tmp = NULL;
  int64_t len = 0;

  if(OB_SUCCESS == ret) 
  { 
    tmp = serialization::decode_vstr(buf, data_len, pos, &len);
    if(len >= max_length)
    { 
      ret = OB_SIZE_OVERFLOW;
      TBSYS_LOG(WARN, "decoded string length[%ld] is greater than buf size[%ld]", len, max_length);
    }
  }

  if(OB_SUCCESS == ret)
  {
    if(NULL == tmp)
    {
      ret = OB_ERROR;
      TBSYS_LOG(WARN, "decode string fail");
    } 
  }

  if(OB_SUCCESS == ret)
  {
    memcpy(str_buf, tmp, len);
    str_buf[len] = '\0';
  }
  return ret;
}

DEFINE_SERIALIZE(oceanbase::common::ObFlag<const char*>)
{
  int ret = OB_SUCCESS;
  if(OB_SUCCESS == ret)
  {
    ret = serialization::encode_vstr(buf, buf_len, pos, value_, strlen(value_)); 
  }

  if(OB_SUCCESS == ret)
  {
    ret = serialization::encode_vstr(buf, buf_len, pos, default_value_, strlen(default_value_)); 
  }

  if(OB_SUCCESS == ret)
  {
    ret = serialization::encode_vstr(buf, buf_len, pos, section_str_, strlen(section_str_));
  }

  if(OB_SUCCESS == ret)
  {
    ret = serialization::encode_vstr(buf, buf_len, pos, flag_str_, strlen(flag_str_));
  }

  return ret;
}

DEFINE_DESERIALIZE(oceanbase::common::ObFlag<const char*>)
{
  int ret = OB_SUCCESS;

  if(OB_SUCCESS == ret)
  {
    ret = decode_string(value_, buf, data_len, pos, OB_MAX_FLAG_VALUE_LENGTH);
  }
  
  if(OB_SUCCESS == ret)
  {
    ret = decode_string(default_value_, buf, data_len, pos, OB_MAX_FLAG_VALUE_LENGTH);
  }

  if(OB_SUCCESS == ret)
  {
    ret = decode_string(section_str_, buf, data_len, pos, OB_MAX_SECTION_NAME_LENGTH);
  }
  
  if(OB_SUCCESS == ret)
  {
    ret = decode_string(flag_str_, buf, data_len, pos, OB_MAX_FLAG_NAME_LENGTH);
  }
  return ret;
}

DEFINE_GET_SERIALIZE_SIZE(oceanbase::common::ObFlag<const char*>)
{
  int64_t size = 0;
  size += serialization::encoded_length_vstr(value_);
  size += serialization::encoded_length_vstr(default_value_);
  size += serialization::encoded_length_vstr(section_str_);
  size += serialization::encoded_length_vstr(flag_str_);
  return size;
}

namespace oceanbase
{
namespace common
{

template<>
DEFINE_SERIALIZE(ObFlag<int>)
{
  int ret = OB_SUCCESS;
  if(OB_SUCCESS == ret)
  {
    ret = serialization::encode_vi32(buf, buf_len, pos, value_); 
  }

  if(OB_SUCCESS == ret)
  {
    ret = serialization::encode_vi32(buf, buf_len, pos, default_value_); 
  }

  if(OB_SUCCESS == ret)
  {
    ret = serialization::encode_vstr(buf, buf_len, pos, section_str_, strlen(section_str_));
  }

  if(OB_SUCCESS == ret)
  {
    ret = serialization::encode_vstr(buf, buf_len, pos, flag_str_, strlen(flag_str_));
  }

  return ret;
}

template<>
DEFINE_DESERIALIZE(ObFlag<int>)
{
  int ret = OB_SUCCESS;

  if(OB_SUCCESS == ret)
  {
    ret = serialization::decode_vi32(buf, data_len, pos, &value_);
  }
  
  if(OB_SUCCESS == ret)
  {
    ret = serialization::decode_vi32(buf, data_len, pos, const_cast<int32_t*>(&default_value_));
  }

  if(OB_SUCCESS == ret)
  {
    ret = decode_string(section_str_, buf, data_len, pos, OB_MAX_SECTION_NAME_LENGTH);
  }
  
  if(OB_SUCCESS == ret)
  {
    ret = decode_string(flag_str_, buf, data_len, pos, OB_MAX_FLAG_NAME_LENGTH);
  }

  return ret;
}

template<>
DEFINE_GET_SERIALIZE_SIZE(ObFlag<int>)
{
  int64_t size = 0;
  size += serialization::encoded_length_vi32(value_);
  size += serialization::encoded_length_vi32(default_value_);
  size += serialization::encoded_length_vstr(section_str_);
  size += serialization::encoded_length_vstr(flag_str_);
  return size;
}

template<>
DEFINE_SERIALIZE(ObFlag<int64_t>)
{
  int ret = OB_SUCCESS;
  if(OB_SUCCESS == ret)
  {
    ret = serialization::encode_vi64(buf, buf_len, pos, value_); 
  }

  if(OB_SUCCESS == ret)
  {
    ret = serialization::encode_vi64(buf, buf_len, pos, default_value_); 
  }

  if(OB_SUCCESS == ret)
  {
    ret = serialization::encode_vstr(buf, buf_len, pos, section_str_, strlen(section_str_));
  }

  if(OB_SUCCESS == ret)
  {
    ret = serialization::encode_vstr(buf, buf_len, pos, flag_str_, strlen(flag_str_));
  }

  return ret;
}

template<>
DEFINE_DESERIALIZE(ObFlag<int64_t>)
{
  int ret = OB_SUCCESS;

  if(OB_SUCCESS == ret)
  {
    ret = serialization::decode_vi64(buf, data_len, pos, &value_);
  }
  
  if(OB_SUCCESS == ret)
  {
    ret = serialization::decode_vi64(buf, data_len, pos, const_cast<int64_t*>(&default_value_));
  }

  if(OB_SUCCESS == ret)
  {
    ret = decode_string(this->section_str_, buf, data_len, pos, OB_MAX_SECTION_NAME_LENGTH);
  }
  if(OB_SUCCESS == ret)
  {
    ret = decode_string(this->flag_str_, buf, data_len, pos, OB_MAX_FLAG_NAME_LENGTH);
  }

  return ret;
}

template<>
DEFINE_GET_SERIALIZE_SIZE(ObFlag<int64_t>)
{
  int64_t size = 0;
  size += serialization::encoded_length_vi64(value_);
  size += serialization::encoded_length_vi64(default_value_);
  size += serialization::encoded_length_vstr(section_str_);
  size += serialization::encoded_length_vstr(flag_str_);
  return size;
}

}
}
