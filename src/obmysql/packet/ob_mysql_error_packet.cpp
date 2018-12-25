#include "ob_mysql_error_packet.h"
#include "../ob_mysql_util.h"

namespace oceanbase
{
  namespace obmysql
  {
    ObMySQLErrorPacket::ObMySQLErrorPacket():field_count_(0xff)
    {
      errcode_ = 2000;
      sqlstate_ = "HY000";
    }

    ObMySQLErrorPacket::~ObMySQLErrorPacket()
    {
    }

    int ObMySQLErrorPacket::encode(char* buffer, int64_t length, int64_t& pos)
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
          TBSYS_LOG(ERROR, "encode error packet data failed");
        }
      }
      return ret;

    }
    int ObMySQLErrorPacket::serialize(char* buffer, int64_t len, int64_t& pos)
    {
      int ret = OB_SUCCESS;

      if (NULL == buffer || 0 >= len || pos < 0)
      {
        TBSYS_LOG(ERROR, "invalid argument buffer=%p, length=%ld, pos=%ld",
                  buffer, len, pos);
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        if (OB_SUCCESS != (ret = ObMySQLUtil::store_int1(buffer, len, field_count_, pos)))
        {
          TBSYS_LOG(ERROR, "serialize field_count failed, buffer=%p, len=%ld, field_count_=%u, pos=%ld",
                    buffer, len, field_count_, pos);
        }
        else if (OB_SUCCESS != (ret = ObMySQLUtil::store_int2(buffer, len, errcode_, pos)))
        {
          TBSYS_LOG(ERROR, "serialize errcode failed, buffer=%p, len=%ld, errcode=%u, pos=%ld",
                    buffer, len, errcode_, pos);
        }
        else if (OB_SUCCESS != (ret = ObMySQLUtil::store_int1(buffer, len, MARKER, pos)))
        {
          TBSYS_LOG(ERROR, "serialize marker failed, buffer=%p, len=%ld, marker=%c, pos=%ld",
                    buffer, len, '#', pos);
        }

        if (OB_SUCCESS == ret)
        {
          if ((buffer + pos + SQLSTATE_SIZE) < buffer + len)
          {
            memcpy(buffer + pos, sqlstate_, SQLSTATE_SIZE);
            pos += SQLSTATE_SIZE;
          }
          else
          {
            TBSYS_LOG(ERROR, "not enough buffer to serialize sqlstate, buffer=%p, len=%ld,"
                      "sqlstate length=%ld,pos=%ld", buffer, len, SQLSTATE_SIZE, pos);
            ret = OB_ERROR;
          }
        }

        if (OB_SUCCESS == ret)
        {
          ret = ObMySQLUtil::store_obstr_nzt(buffer, len, message_, pos);
          if (OB_SUCCESS != ret)
          {
            TBSYS_LOG(ERROR, "serialize message failed, buffer=%p, len=%ld, message length=%d,"
                      "pos=%ld", buffer, len, message_.length(), pos);
          }
        }
      }
      return ret;
    }

    uint64_t ObMySQLErrorPacket::get_serialize_size()
    {
      uint64_t len = 0;
      len += 9;/*1byte field_count + 2bytes errno + 1byte sqlmarker + 5bytes sqlstate*/
      len += MAX_STORE_LENGTH + message_.length();
      return len;
    }

    int ObMySQLErrorPacket::set_message(const ObString& message)
    {
      int ret = OB_SUCCESS;
      if (NULL == message.ptr() || 0 > message.length())
      {
        TBSYS_LOG(ERROR, "invalid argument message.ptr=%p, message.length()=%d",
                  message.ptr(), message.length());
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        message_.assign(const_cast<char*>(message.ptr()), message.length());
      }
      return ret;
    }
  }
}
