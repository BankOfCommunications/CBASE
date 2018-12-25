#ifndef _OCEANBASE_TOOLS_ADMIN_UTILS_H_
#define _OCEANBASE_TOOLS_ADMIN_UTILS_H_

#include "common/serialization.h"

namespace oceanbase {
  namespace tools {
    using namespace oceanbase::common;
    
    template <class T>
    inline int temp_serialize(const T &data, char *buf, const int64_t data_len, int64_t& pos)
    {
      return data.serialize(buf, data_len, pos);
    };

    template <class T>
    inline int temp_deserialize(T &data, char *buf, const int64_t data_len, int64_t& pos)
    {
      return data.deserialize(buf, data_len, pos);
    };

    template <>
    inline int temp_serialize<uint64_t>(const uint64_t &data, char *buf, const int64_t data_len, int64_t& pos)
    {
      return serialization::encode_vi64(buf, data_len, pos, (int64_t)data);
    };

    template <>
    inline int temp_deserialize<uint64_t>(uint64_t &data, char *buf, const int64_t data_len, int64_t& pos)
    {
      return serialization::decode_vi64(buf, data_len, pos, (int64_t*)&data);
    };

    template <>
    inline int temp_serialize<int64_t>(const int64_t &data, char *buf, const int64_t data_len, int64_t& pos)
    {
      return serialization::encode_vi64(buf, data_len, pos, data);
    };

    template <>
    inline int temp_deserialize<int64_t>(int64_t &data, char *buf, const int64_t data_len, int64_t& pos)
    {
      return serialization::decode_vi64(buf, data_len, pos, &data);
    };

    template <>
    inline int temp_serialize<uint32_t>(const uint32_t &data, char *buf, const int64_t data_len, int64_t& pos)
    {
      return serialization::encode_vi32(buf, data_len, pos, (int32_t)data);
    };

    template <>
    inline int temp_deserialize<uint32_t>(uint32_t &data, char *buf, const int64_t data_len, int64_t& pos)
    {
      return serialization::decode_vi32(buf, data_len, pos, (int32_t*)&data);
    };

    template <>
    inline int temp_serialize<int32_t>(const int32_t &data, char *buf, const int64_t data_len, int64_t& pos)
    {
      return serialization::encode_vi32(buf, data_len, pos, data);
    };

    template <>
    inline int temp_deserialize<int32_t>(int32_t &data, char *buf, const int64_t data_len, int64_t& pos)
    {
      return serialization::decode_vi32(buf, data_len, pos, &data);
    };
  } // namespace tools
} // namespace oceanbae

#endif /* _OCEANBASE_TOOLS_ADMIN_UTILS_H_ */
