/**
 * (C) 2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *  
 * ob_record_header.h for record header. 
 *
 * Authors: 
 *    fanggang <fanggang@taobao.com>
 *    huating <huating.zmq@taobao.com>
 *
 */

#ifndef OCEANBASE_CHUNKSERVER_RECORD_HEADER_H_
#define OCEANBASE_CHUNKSERVER_RECORD_HEADER_H_

#include "ob_define.h"
#include "serialization.h"
#include "ob_crc64.h"
#include "utility.h"

namespace oceanbase 
{
  namespace common 
  { 
    static const int16_t MAGIC_NUMER = static_cast<int16_t>(0xB0CC);
    
    inline void format_i64(const int64_t value, int16_t& check_sum)
    {  
       int i = 0;
       while (i < 4)
       {
         check_sum =  static_cast<int16_t>(check_sum ^ ((value >> i * 16) & 0xFFFF));
         ++i;
       }
    }
    
    inline void format_i32(const int32_t value,int16_t& check_sum)
    {
       int i = 0;
       while (i < 2)
       {
         check_sum =  static_cast<int16_t>(check_sum ^ ((value >> i * 16) & 0xFFFF));
         ++i;
       }
    }
    
    struct ObRecordHeader
    { 
      int16_t magic_;          // magic number
      int16_t header_length_;  // header length
      int16_t version_;        // version
      int16_t header_checksum_;// header checksum
      int64_t reserved_;       // reserved,must be 0
      int32_t data_length_;    // length before compress
      int32_t data_zlength_;   // length after compress, if without compresssion 
                               // data_length_= data_zlength_
      int64_t data_checksum_;  // record checksum
      ObRecordHeader();

      int64_t to_string(char* buf, const int64_t buf_len) const
      {
        int64_t pos = 0;
        databuff_printf(buf, buf_len, pos, "[RecordHeader] magic=%hd header_length=%hd version=%hd "
                        "header_checksum=%hd reserved=%ld data_length=%d data_zlength=%d",
                        magic_, header_length_, version_,
                        header_checksum_, reserved_, data_length_, data_zlength_);
        return pos;
      }
      
      /**
       * sert magic number of record header
       * 
       * @param magic magic number to set
       */
      inline void set_magic_num(const int16_t magic = MAGIC_NUMER)
      {
        magic_ = magic;
      }
      int64_t get_reserved()
      {
        return reserved_;
      }

      /**
       * set checksum of record header
       */
      void set_header_checksum();
      
      /**
       * check whether the checksum of record header is correct after 
       * deserialize record header 
       * 
       * @return int if success, return OB_SUCCESS, else return 
       *         OB_ERROR
       */
      int check_header_checksum() const;
      
      /**
       * check whether the magic number is correct after deserialize 
       * the record header 
       * 
       * @param magic the magic number to check
       * 
       * @return int if success, return OB_SUCCESS, else return 
       *         OB_ERROR
       */
      int check_magic_num(const int16_t magic = MAGIC_NUMER) const; 
      
      /**
       * check whether the size of compressed payload data is equal to
       * the compressed payload data size stored in reacord header 
       * 
       * @param commpressed_len the length of compressed payload data
       * 
       * @return int if success, return OB_SUCCESS, else return 
       *         OB_ERROR
       */
      inline int check_data_zlength_(const int32_t commpressed_len) const
      { 
        int ret = OB_SUCCESS; 
        if(commpressed_len != data_zlength_)
        {
          ret = OB_ERROR;
        } 
        return ret;
      }
      
      /**
       * check whether the size of uncompressed payload data is 
       * equal to the uncompressed payload data size stored in reacord
       * header 
       * 
       * @param data_len the actual payload data size
       * 
       * @return int if success, return OB_SUCCESS, else return 
       *         OB_ERROR
       */
      inline int check_data_length(const int32_t data_len) const
      {
        int ret = OB_SUCCESS;
        if (data_len != data_length_)
        {
          ret = OB_ERROR;
        }
        return ret;
      }

      /**
       * after deserialize the record header, check whether the 
       * payload data is compressed 
       * 
       * @return bool if payload is compressed, return true, else 
       *         return false
       */
      inline bool is_compress() const
      {  
        bool ret = true;
        if (data_length_ == data_zlength_)
        {
          ret = false;
        }
        return ret;
      }
      
      /**
       * this method should use after deserialization,use it to check 
       * check_sum of record 
       * 
       * @param buf record buf
       * @param len record len
       * 
       * @return int if success, return OB_SUCCESS, else return 
       *         OB_ERROR
       */
      int check_check_sum(const char *buf, const int64_t len) const;
      
      /**
       * after read a record and record_header, should check it
       * 
       * @param buf record and record_header byte string which is read 
       *            from disk
       * @param len total length of record and record_header
       * @param magic magic number to check
       * 
       * @return int if success, return OB_SUCCESS, else return 
       *         OB_ERROR
       */
      static int check_record(const char *buf, const int64_t len, const int16_t magic);

      /**
       * non-standard check record, the buffer size maybe is larger 
       * than expected buffer size, but the buffer include the data we
       * need, we also pass this case check.  
       * 
       * @param buf record and record_header byte string which is read 
       *            from disk
       * @param len total length of record and record_header
       * @param magic magic number to check
       * 
       * @return int if success, return OB_SUCCESS, else return 
       *         OB_ERROR
       */
      static int nonstd_check_record(const char *buf, const int64_t len, 
        const int16_t magic);

      /**
       * if user deserializes record header, he can use this fucntion 
       * to check record 
       * 
       * @param record_header record header to check
       * @param payload_buf payload data following the record header
       * @param payload_len payload data length
       * @param magic magic number to check
       * 
       * @return int if success, return OB_SUCCESS, else return 
       *         OB_ERROR
       */
      static int check_record(const ObRecordHeader &record_header, 
                              const char *payload_buf, 
                              const int64_t payload_len, 
                              const int16_t magic);
      
      /**
       * give the buffer of record and record size, check whether the 
       * record is ok, return the record header, payload buffer, and 
       * payload buffer size 
       * 
       * @param ptr pointer of record buffer
       * @param size size of record buffer
       * @param magic magic number to check
       * @param header [out] record header to return
       * @param payload_ptr [out] payload pointer to return
       * @param payload_size [out] payload data size
       * 
       * @return int if success, return OB_SUCCESS, else return 
       *         OB_ERROR or OB_INVALID_ARGUMENT
       */
      static int check_record(const char* ptr, const int64_t size, 
                              const int16_t magic, ObRecordHeader& header, 
                              const char*& payload_ptr, int64_t& payload_size);

      /**
       * give the buffer of record and record size, doesn't check 
       * whether the record is ok, return the record header, payload 
       * buffer, and payload buffer size 
       * 
       * @param ptr pointer of record buffer
       * @param size size of record buffer
       * @param header [out] record header to return
       * @param payload_ptr [out] payload pointer to return
       * @param payload_size [out] payload data size
       * 
       * @return int if success, return OB_SUCCESS, else return 
       *         OB_ERROR or OB_INVALID_ARGUMENT
       */
      static int get_record_header(const char* ptr, const int64_t size, 
                                   ObRecordHeader& header, 
                                   const char*& payload_ptr, int64_t& payload_size);

      NEED_SERIALIZE_AND_DESERIALIZE;
    };
    
    static const int OB_RECORD_HEADER_LENGTH = sizeof(ObRecordHeader);
  } // namespace Oceanbase::common
}// namespace Oceanbase
#endif
