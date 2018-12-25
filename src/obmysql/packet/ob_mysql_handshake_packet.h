/*
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * ob_mysql_handshake_packet.h is for what ...
 *
 * Version: ***: ob_mysql_handshake_packet.h  Tue Jul 31 09:43:16 2012 fangji.hcm Exp $
 *
 * Authors:
 *   Author fangji
 *   Email: fangji.hcm@taobao.com
 *     -some work detail if you want
 *
 */
#ifndef OB_MYSQL_HANDSHAKE_PACKET_H_
#define OB_MYSQL_HANDSHAKE_PACKET_H_

#include "ob_mysql_packet_header.h"
#include "common/ob_define.h"
#include "tblog.h"
#include "../ob_mysql_util.h"
#include "common/ob_string.h"
#include "common/ob_string_buf.h"

namespace oceanbase
{
  namespace obmysql
  {
    class ObMySQLHandshakePacket
    {
      public:
        static const int32_t SCRAMBLE_SIZE = 8;
        static const int32_t PLUGIN_SIZE   = 13;
        static const int32_t PLUGIN2_SIZE   = 12;

      public:
        ObMySQLHandshakePacket();

        //TODO use ob server info to init handshake packet
        //ObMySQLHandshakePacket(ObServerInfo& info);
        ~ObMySQLHandshakePacket();


        int set_server_version(common::ObString& version);

        inline void set_server_capability(uint16_t capability)
        {
          server_capabilities_ = capability;
        }

        inline void set_server_status(uint16_t status)
        {
          server_status_ = status;
        }

        inline void set_server_language(uint8_t language)
        {
          server_language_ = language;
        }

        /**
         * Serialize all data not include packet header to buffer
         * @param buffer  buffer
         * @param len     buffer length
         * @param pos     buffer pos
         */
        int serialize(char* buffer, int64_t len, int64_t& pos);

        void set_thread_id(const uint32_t id);

        /**
         * Get packet serialize size not include
         * packet header
         */
        int32_t get_serialize_size();

        private:
        int serialize_header(char* buffer, int64_t length, uint32_t pkt_length, int64_t& pos);

      private:
        ObMySQLPacketHeader header_;
        uint8_t protocol_version_;
        common::ObString server_version_;// human-readable server version
        uint32_t thread_id_;// connection_id
        char scramble_buff_[8];// auth_plugin_data_part_1 : first 8 bytes of the auth-plugin data
        char filler_;                  /* always 0x00 */
        uint16_t server_capabilities_;  /* set value to use 4.1protocol */
        uint8_t server_language_;
        uint16_t server_status_;
        char plugin_[13];        /* always 0x00 */
        char plugin2_[12];
        char terminated_;
        common::ObStringBuf str_buf_;   //store ObString content
    };

    inline void ObMySQLHandshakePacket::set_thread_id(const uint32_t id)
    {
      thread_id_ = id;
    }
  }
}
#endif
