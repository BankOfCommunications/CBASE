/*
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * ob_mysql_spr_packet.h is for what ...
 *
 * Version: ***: ob_mysql_spr_packet.h  Thu Oct 25 15:03:49 2012 fangji.hcm Exp $
 *
 * Authors:
 *   Author fangji
 *   Email: fangji.hcm@taobao.com
 *     -some work detail if you want
 *
 */
#ifndef OB_MYSQL_SPR_PACKET_H_
#define OB_MYSQL_SPR_PACKET_H_

#include "ob_mysql_packet.h"

namespace oceanbase
{
  namespace obmysql
  {
    /**
     * This packet is response to COM_STMT_PREPARE
     * following with param desc && column desc packets
     *
     *  status (1) -- [00] OK
     *  statement_id (4) -- statement-id
     *  num_columns (2) -- number of columns
     *  num_params (2) -- number of params
     *  reserved_1 (1) -- [00] filler
     *  warning_count (2) -- number of warnings
     */
    class ObMySQLSPRPacket : public ObMySQLPacket
    {
      public:
        ObMySQLSPRPacket();
        ~ObMySQLSPRPacket();


        int serialize(char* buffer, int64_t length, int64_t& pos);
        virtual uint64_t get_serialize_size();

        inline void set_statement_id(uint32_t id)
        {
          statement_id_ = id;
        }

        inline void set_column_num(uint16_t num)
        {
          column_num_ = num;
        }

        inline void set_param_num(uint16_t num)
        {
          param_num_ = num;
        }

        inline void set_warning_count(uint16_t count)
        {
          warning_count_ = count;
        }


      private:
        DISALLOW_COPY_AND_ASSIGN(ObMySQLSPRPacket);
        uint8_t  status_;
        uint32_t statement_id_;
        uint16_t column_num_;
        uint16_t param_num_;
        uint8_t  reserved_;
        uint16_t warning_count_;
    };
  }
}
#endif
