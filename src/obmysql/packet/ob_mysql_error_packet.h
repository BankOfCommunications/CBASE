/*
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * ob_mysql_error_packet.h is for what ...
 *
 * Version: ***: ob_mysql_error_packet.h  Wed Aug  1 11:33:32 2012 fangji.hcm Exp $
 *
 * Authors:
 *   Author fangji
 *   Email: fangji.hcm@taobao.com
 *     -some work detail if you want
 *
 */
#ifndef OB_MYSQL_ERROR_PACKET_H_
#define OB_MYSQL_ERROR_PACKET_H_

#include "ob_mysql_packet_header.h"
#include "ob_mysql_packet.h"
#include "common/ob_string.h"
#include "common/ob_string_buf.h"
#include "tblog.h"
#include "../ob_mysql_state.h"

namespace oceanbase
{
  namespace obmysql
  {
    class ObMySQLErrorPacket : public ObMySQLPacket
    {
      public:
        static const uint64_t SQLSTATE_SIZE = 5;
        static const uint8_t MARKER = '#';
      public:
        ObMySQLErrorPacket();
        ~ObMySQLErrorPacket();

        int serialize(char* buffer, int64_t len, int64_t& pos);

        uint64_t get_serialize_size();
        virtual int encode(char *buffer, int64_t length, int64_t & pos);

        int set_message(const common::ObString& message);

        /**
         * set error code and sql state string.
         *
         * @note OB_ERROR shouldn't passing to this function for
         * hostorical reason.
         *
         * @note It will make a negation and truncate it from 4 bytes
         * (int) to 2 bytes (uint16_t) for fitting msyql protocol.
         *
         * @param errcode oceanbase error code
         */
        int set_oberrcode(int errcode)
        {
          errcode_ = static_cast<uint16_t>(-errcode);
          return set_sqlstate(ObMySQLState::get_instance().get_jdbc_state(errcode));
        }

      private:
        inline void set_errcode(uint16_t code)
        {
          errcode_ = code;
        }

        int set_sqlstate(const char *sqlstate)
        {
          int ret = common::OB_SUCCESS;
          if (SQLSTATE_SIZE == strlen(sqlstate))
          {
            sqlstate_ = sqlstate;
          }
          else
          {
            ret = common::OB_INVALID_ARGUMENT;
          }
          return ret;
        }

        //for test
        inline uint8_t get_field_count() const
        {
          return field_count_;
        }

        inline uint16_t get_err_code() const
        {
          return errcode_;
        }

        inline const char* get_sql_state()
        {
          return sqlstate_;
        }

        inline common::ObString& get_message()
        {
          return message_;
        }

      private:
        uint8_t field_count_;   /* Always 0xff */
        uint16_t errcode_;
        const char *sqlstate_;
        common::ObString message_;
    };
  }
}

#endif
