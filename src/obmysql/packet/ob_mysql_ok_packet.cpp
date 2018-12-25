#include "ob_mysql_ok_packet.h"
#include "../ob_mysql_util.h"

using namespace oceanbase::common;

namespace oceanbase
{
  namespace obmysql
  {

    ObMySQLOKPacket::ObMySQLOKPacket()
      :field_count_(0x00),
       affected_rows_(0),
       insert_id_(0),
       server_status_(0x22),
       warning_count_(0)
    {
      //affected_row_ = 0;
      //insert_is_ = 0;
      //memset(this, 0, sizeof(ObMySQLOKPacket));
      //server_status_ = 0x22;//dump MySQL packet get this value
    }

    ObMySQLOKPacket::~ObMySQLOKPacket()
    {
      str_buf_.clear();
    }

    int ObMySQLOKPacket::set_message(ObString& message)
    {
      int ret = OB_SUCCESS;
      if (NULL == message.ptr() || 0 > message.length())
      {
        TBSYS_LOG(ERROR, "invalid argument message.ptr=%p, message.length=%d",
                  message.ptr(), message.length());
        ret = OB_ERROR;
      }
      else
      {
        ret = str_buf_.write_string(message, &message_);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR, "write string to messaget failed");
        }
      }
      return ret;
    }

    int ObMySQLOKPacket::encode(char* buffer, int64_t length, int64_t& pos)
    {
      int ret = OB_SUCCESS;
      int64_t start_pos = pos;
      if (NULL == buffer || 0 >= length || pos < 0)
      {
        TBSYS_LOG(ERROR, "invalid argument buffer=%p, length=%ld, pos=%ld",
                  buffer, length, pos);
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        pos += OB_MYSQL_PACKET_HEADER_SIZE;
        ret = serialize(buffer, length, pos);
        if (OB_SUCCESS == ret)
        {
          uint32_t pkt_len = static_cast<uint32_t>(pos - start_pos - OB_MYSQL_PACKET_HEADER_SIZE);
          if (OB_SUCCESS != (ret = ObMySQLUtil::store_int3(buffer, length, pkt_len, start_pos)))
          {
            TBSYS_LOG(ERROR, "serialize packet haader size failed, buffer=%p, buffer length=%ld, packet length=%d, pos=%ld",
                      buffer, length, pkt_len, start_pos);
          }
          else if (OB_SUCCESS != (ret = ObMySQLUtil::store_int1(buffer, length, 2, start_pos)))
          {
            TBSYS_LOG(ERROR, "serialize packet haader seq failed, buffer=%p, buffer length=%ld, seq number=%d, pos=%ld",
                      buffer, length, 2, start_pos);
          }
        }
        else
        {
          TBSYS_LOG(ERROR, "encode ok packet data failed");
        }
      }
      return ret;
    }

    uint64_t ObMySQLOKPacket::get_serialize_size()
    {
      uint64_t len = 0;
      len += 5; /*1byte field_count + 2bytes server_status + 2bytes warning_count see MySQL protocol*/
      len += 9; /*max length for unit64_t*/
      len += 9; /*max length for store_length*/
      len += 9 + message_.length();
      return len;
    }

    int ObMySQLOKPacket::serialize(char* buffer, int64_t length, int64_t& pos)
    {
      int ret = OB_SUCCESS;

      if (NULL == buffer || 0 >= length || pos < 0)
      {
        TBSYS_LOG(ERROR, "invalid argument buffer=%p, length=%ld, pos=%ld",
                  buffer, length, pos);
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        if (OB_SUCCESS != (ret = ObMySQLUtil::store_int1(buffer, length, field_count_, pos)))
        {
          TBSYS_LOG(ERROR, "serialize field_count failed, buffer=%p, length=%ld, field_count=%u,"
                    "pos=%ld", buffer, length, field_count_, pos);
        }
        else if (OB_SUCCESS != (ret = ObMySQLUtil::store_length(buffer, length, affected_rows_, pos)))
        {
          TBSYS_LOG(ERROR, "serialize affected_row failed, buffer=%p, length=%ld, affected_rows=%lu,"
                    "pos=%ld", buffer, length, affected_rows_, pos);
        }
        else if (OB_SUCCESS != (ret = ObMySQLUtil::store_length(buffer, length, insert_id_, pos)))
        {
          TBSYS_LOG(ERROR, "serialize insert_id failed, buffer=%p, length=%ld, insert_id=%lu,"
                    "pos=%ld", buffer, length, insert_id_, pos);
        }
        else if (OB_SUCCESS != (ret = ObMySQLUtil::store_int2(buffer, length, server_status_, pos)))
        {
          TBSYS_LOG(ERROR, "serialize server_status failed, buffer=%p, length=%ld, server_status=%u,"
                    "pos=%ld", buffer, length, server_status_, pos);
        }
        else if (OB_SUCCESS != (ret = ObMySQLUtil::store_int2(buffer, length, warning_count_, pos)))
        {
          TBSYS_LOG(ERROR, "serialize warning_count failed, buffer=%p, length=%ld, warning_count=%u,"
                    "pos=%ld", buffer, length, warning_count_, pos);
        }
        else if (0 != message_.length())
        {
          if (OB_SUCCESS != (ret = ObMySQLUtil::store_obstr(buffer, length, message_, pos)))
          {
            TBSYS_LOG(ERROR, "serialize message failed, buffer=%p, length=%ld, insert_id=%lu,"
                "pos=%ld", buffer, length, insert_id_, pos);
          }
        }
      }
      return ret;
    }
  }
}
