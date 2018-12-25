#ifndef OCEANBASE_COMMON_OB_RECORD_HEADER_V2_H_
#define OCEANBASE_COMMON_OB_RECORD_HEADER_V2_H_

#include "ob_define.h"
#include "ob_crc64.h"
#include "ob_record_header.h"

namespace oceanbase
{
  namespace common
  {
    struct ObRecordHeaderV2
    {
      int16_t magic_;
      int16_t header_length_;
      int16_t version_;
      int16_t header_checksum_;
      int16_t reserved16_;
      int64_t data_length_;
      int64_t data_zlength_;
      int64_t data_checksum_;

      ObRecordHeaderV2()
      {
        memset(this, 0, sizeof(ObRecordHeaderV2));
        header_length_ = sizeof(ObRecordHeaderV2);
        version_ = 0x2;
      }

      inline void reset()
      {
        memset(this, 0, sizeof(ObRecordHeaderV2));
        header_length_ = sizeof(ObRecordHeaderV2);
        version_ = 0x2;
      }

      void set_header_checksum();

      int check_header_checksum() const;

      inline int check_magic_num(const int16_t magic) const
      {
        int ret = OB_SUCCESS;

        if (magic_ != magic)
        {
          ret = OB_ERROR;
        }

        return ret;
      }

      inline int check_data_length(const int64_t data_length) const
      {
        int ret  = OB_SUCCESS;

        if (data_length != data_length_)
        {
          ret = OB_ERROR;
        }

        return ret;
      }

      inline int check_data_zlength(const int64_t data_zlength) const
      {
        int ret = OB_SUCCESS;

        if (data_zlength != data_zlength_)
        {
          ret = OB_ERROR;
        }

        return ret;
      }

      int check_check_checksum(const char* buf, const int64_t len) const;

      inline bool is_compress() const
      {
        bool ret = true;

        if (data_length_ == data_zlength_)
        {
          ret = false;
        }

        return ret;
      }

      static int check_record(const char* ptr, const int64_t size,
          const int16_t magic, ObRecordHeaderV2& header,
          const char*& payload_ptr, int64_t& payload_size);

      static int check_record(const char* buf, const int64_t len,
          const int16_t magic);

      static int get_record_header(const char* buf, const int64_t size,
          ObRecordHeaderV2& header, const char*& payload_ptr,
          int64_t& payload_size);
    };
  }//end namespace commmon
}//end namespace oceanbase
#endif

