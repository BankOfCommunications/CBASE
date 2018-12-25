/*
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * ob_mysql_ok_packet.h is for what ...
 *
 * Version: ***: ob_mysql_ok_packet.h  Wed Aug  1 11:13:20 2012 fangji.hcm Exp $
 *
 * Authors:
 *   Author fangji
 *   Email: fangji.hcm@taobao.com
 *     -some work detail if you want
 *
 */
#ifndef OB_MYSQL_OK_PACKET_H_
#define OB_MYSQL_OK_PACKET_H_

#include "ob_mysql_packet_header.h"
#include "ob_mysql_packet.h"
#include "common/ob_string.h"
#include "common/ob_string_buf.h"

namespace oceanbase
{
  namespace obmysql
  {
    class ObMySQLOKPacket : public ObMySQLPacket
    {
      public:
        ObMySQLOKPacket();
        ~ObMySQLOKPacket();

        /**
         * serialize all data into thread buffer
         * not include packet header
         * called whenever send ok packet except login ok
         */
        int serialize(char* buffer, int64_t length, int64_t& pos);

        uint64_t get_serialize_size();


        /**
         * This function used to serialize OK packet
         * to thread buffer include packet header
         * called when send login_ok packet
         */
        virtual int encode(char* buffer, int64_t length, int64_t& pos);

        //int32_t get_encode_size() const;

        int set_message(common::ObString& message);

        inline void set_affected_rows(uint64_t row)
        {
          affected_rows_ = row;
        }

        inline void set_insert_id(uint64_t id)
        {
          insert_id_ = id;
        }

        inline void set_server_status(uint16_t status)
        {
          server_status_ = status;
        }

        inline void set_warning_count(uint16_t warning_count)
        {
          warning_count_ = warning_count;
        }

        //for test
        inline uint8_t get_field_count() const
        {
          return field_count_;
        }

        inline uint64_t get_affected_rows() const
        {
          return affected_rows_;
        }

        inline uint64_t get_insert_id() const
        {
          return insert_id_;
        }

        inline uint16_t get_server_status() const
        {
          return server_status_;
        }

        inline uint16_t get_warning_count() const
        {
          return warning_count_;
        }

        inline common::ObString& get_message()
        {
          return message_;
        }

      private:
        DISALLOW_COPY_AND_ASSIGN(ObMySQLOKPacket);

        uint8_t  field_count_;         /* always 0x00 */
        uint64_t affected_rows_;
        uint64_t insert_id_;
        uint16_t server_status_;
        uint16_t warning_count_;
        common::ObString message_;

        common::ObStringBuf str_buf_;
    };
  } // end namespace obmysql
} // end namespace oceanbase
#endif
