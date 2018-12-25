#include "ob_record_header_v2.h"

namespace oceanbase 
{ 
  namespace common 
  {
    void ObRecordHeaderV2::set_header_checksum()
    {  
      header_checksum_ = 0;
      int16_t checksum = 0;
      
      checksum = checksum ^ magic_;
      checksum = checksum ^ header_length_;
      checksum = checksum ^ version_;
      checksum = checksum ^ reserved16_;
      format_i64(data_length_, checksum);
      format_i64(data_zlength_, checksum);
      format_i64(data_checksum_, checksum);
      header_checksum_ = checksum;
    }

    int ObRecordHeaderV2::check_header_checksum() const
    { 
      int ret = OB_ERROR;
      int16_t checksum = 0;

      checksum = checksum ^ magic_;
      checksum = checksum ^ header_length_;
      checksum = checksum ^ version_;
      checksum = checksum ^ header_checksum_;
      checksum = checksum ^ reserved16_;
      format_i64(data_length_, checksum);
      format_i64(data_zlength_, checksum);
      format_i64(data_checksum_, checksum);

      if (checksum == 0)
      {
        ret = OB_SUCCESS;
      }

      return ret;
    }

    int ObRecordHeaderV2::check_check_checksum(const char* buf, const int64_t len) const
    { 
      int ret = OB_SUCCESS;

      if ((data_zlength_ != len) || (NULL == buf) || (0 > len))
      {
        TBSYS_LOG(WARN, "chechk chekcksum error:data_zlength_=%ld,"
            "len=%ld", data_zlength_, len);
        ret = OB_ERROR;
      }
      else
      {
        int64_t crc_check_sum = ob_crc64(buf, len);
        if (crc_check_sum !=  data_checksum_)
        { 
          TBSYS_LOG(WARN, "crc_check_sum != data_checksum_:"
              "crc_check_sum=%ld,data_checksum_=%ld",
              crc_check_sum, data_checksum_);
          ret = OB_ERROR;
        }
      }

      return ret;
    }

    int ObRecordHeaderV2::check_record(const char* ptr,
        const int64_t size, const int16_t magic, ObRecordHeaderV2& header,
        const char*& payload_ptr, int64_t& payload_size)
    {
      int ret = OB_SUCCESS;
      const int64_t record_header_len = sizeof(ObRecordHeaderV2);

      if (NULL == ptr || record_header_len > size)
      {
        TBSYS_LOG(WARN, "invalid argument:ptr=%p,size=%ld,record_header_len=%ld",
            ptr, size, record_header_len);
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        memcpy(&header, ptr, record_header_len);
      }

      if (OB_SUCCESS == ret)
      {
        payload_ptr = ptr + record_header_len;
        payload_size = size - record_header_len;

        bool check = ((payload_size > 0)
            && (magic == header.magic_)
            && (0x2 == header.version_)
            && (OB_SUCCESS == header.check_header_checksum())
            && (OB_SUCCESS == header.check_check_checksum(
                payload_ptr, payload_size)));
        if (!check)
        {
          TBSYS_LOG(WARN, "check error:payload_size=%ld,magic=%d,"
              "header.magic_=%d,header.check_header_checksum()=%d,"
              "header.check_check_checksum(payload_ptr, payload_size)=%d",
              payload_size, magic, header.magic_,
              header.check_header_checksum(),
              header.check_check_checksum(payload_ptr, payload_size));
          ret = OB_ERROR;
        }
      }

      return ret;
    }

    int ObRecordHeaderV2::check_record(const char* buf,
        const int64_t len, const int16_t magic)
    {
      int ret = OB_SUCCESS;
      const int64_t record_header_len = sizeof(ObRecordHeaderV2);
      ObRecordHeaderV2 record_header;
      const char* payload_ptr = buf + record_header_len;
      int64_t payload_size = len - record_header_len;

      if (NULL == buf || record_header_len > len)
      {
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        memcpy(&record_header, buf, record_header_len);
      }

      if (OB_SUCCESS == ret)
      {
        bool check = ((payload_size > 0)
            && (magic == record_header.magic_)
            && (0x2 == record_header.version_)
            && (OB_SUCCESS == record_header.check_header_checksum())
            && (OB_SUCCESS == record_header.check_check_checksum(payload_ptr,
                payload_size)));
        if (!check)
        {
          ret = OB_ERROR;
        }
      }

      return ret;
    }

    int ObRecordHeaderV2::get_record_header(
        const char* buf, const int64_t size,
        ObRecordHeaderV2& header, const char*& payload_ptr,
        int64_t& payload_size)
    {
      int ret = OB_SUCCESS;

      if (NULL == buf
          || size < static_cast<const int64_t>(sizeof(ObRecordHeaderV2)))
      {
        ret = OB_INVALID_ARGUMENT;
        TBSYS_LOG(WARN, "invalid argument");
      }

      if (OB_SUCCESS == ret)
      {
        memcpy(&header, buf, sizeof(ObRecordHeaderV2));
      }

      if (OB_SUCCESS == ret)
      {
        payload_ptr = buf + sizeof(ObRecordHeaderV2);
        payload_size = size - sizeof(ObRecordHeaderV2);
      }

      return ret;
    }
  }//end namespace common
}//end namespace oceanbase

