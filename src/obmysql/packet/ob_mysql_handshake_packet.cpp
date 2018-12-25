#include "ob_mysql_handshake_packet.h"


using namespace oceanbase::common;
namespace oceanbase
{
  namespace obmysql
  {
    ObMySQLHandshakePacket::ObMySQLHandshakePacket()
    {
      const char* str = "5.5.1";
      protocol_version_ = 10;//Protocol::HandshakeV10
      server_version_.assign(const_cast<char*>(str), static_cast<int32_t>(strlen(str)));
      thread_id_ = 100;
      memset(scramble_buff_, 'a', 8);
      filler_ = 0;
      //0xF7FF, 多个flag的组合，其中支持4.1 协议的flag置了1,
      //CLIENT_PLUGIN_AUTH 这个flag没有置上,它的值为0x00080000
      //CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA 没有置为1，0x00200000
      //CLIENT_CONNECT_WITH_DB:IS SET,0x00000008
      server_capabilities_ = 63487;
      server_language_ = 8;//latin1_swedish_ci
      server_status_ = 0;// no this value in mysql protocol document
      memset(plugin_, 0, sizeof(plugin_));
      memset(plugin2_, 'b', 12);
    }

    ObMySQLHandshakePacket::~ObMySQLHandshakePacket()
    {
      str_buf_.clear();
    }

    //seq of handshake is 0
    int ObMySQLHandshakePacket::serialize(char* buffer, int64_t len, int64_t &pos)
    {
      int ret = OB_SUCCESS;
      int64_t start_pos = pos;
      if (NULL == buffer || 0 >= len || pos < 0)
      {
        TBSYS_LOG(ERROR, "invalid argument buffer=%p, length=%ld, pos=%ld",
                  buffer, len, pos);
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        //skip packet header serialize head at end of this function
        pos += OB_MYSQL_PACKET_HEADER_SIZE;

        if (OB_SUCCESS != (ret = ObMySQLUtil::store_int1(buffer, len, protocol_version_, pos)))
        {
          TBSYS_LOG(ERROR, "serialize packet protocol_version failed, buffer=%p, length=%ld,"
                    "protocol_version=%d, pos=%ld", buffer, len, protocol_version_, pos);
        }
        else if (OB_SUCCESS != (ret = ObMySQLUtil::store_obstr_zt(buffer, len, server_version_, pos)))
        {
          TBSYS_LOG(ERROR, "serialize packet server_version failed, buffer=%p, length=%ld,"
                    "server_version length=%d, pos=%ld", buffer, len, server_version_.length(), pos);
        }
        else if (OB_SUCCESS != (ret = ObMySQLUtil::store_int4(buffer, len, thread_id_, pos)))
        {
          TBSYS_LOG(ERROR, "serialize packet thread_id failed, buffer=%p, length=%ld,"
                    "thread_id=%d, pos=%ld", buffer, len, thread_id_, pos);
        }

        //make sure buffer + pos + SCRAMBLE_SIZE < buffer + length
        //serialize SCRAMBLE && filler_
        if (OB_SUCCESS == ret)
        {
          if ((buffer + pos + SCRAMBLE_SIZE < buffer + len))
          {
            memcpy(buffer + pos, scramble_buff_, SCRAMBLE_SIZE + 1);
            pos += SCRAMBLE_SIZE + 1;
          }
          else
          {
            TBSYS_LOG(ERROR, "not enough buffer to serialize scramble_buff && filler, buffer=%p, len=%ld,"
                      "scramble_buff&&filler length=%d,pos=%ld", buffer, len, SCRAMBLE_SIZE + 1, pos);
            ret = OB_ERROR;
          }
        }

        if (OB_SUCCESS == ret)
        {
          /*if (OB_SUCCESS != (ret = ObMySQLUtil::store_int1(buffer, len, filler_, pos)))
          {
            TBSYS_LOG(ERROR, "serialize packet filler_ failed, buffer=%p, length=%ld,"
                      "server_capabilities=%c, pos=%ld", buffer, len, filler_, pos);
          }
          else*/ if (OB_SUCCESS != (ret = ObMySQLUtil::store_int2(buffer, len, server_capabilities_, pos)))
          {
            TBSYS_LOG(ERROR, "serialize packet server_capabilities failed, buffer=%p, length=%ld,"
                      "server_capabilities=%d, pos=%ld", buffer, len, server_capabilities_, pos);
          }
          else if (OB_SUCCESS != (ret = ObMySQLUtil::store_int1(buffer, len, server_language_, pos)))
          {
            TBSYS_LOG(ERROR, "serialize packet server_language failed, buffer=%p, length=%ld,"
                      "server_language=%d, pos=%ld", buffer, len, server_language_, pos);
          }
          else if (OB_SUCCESS != (ret = ObMySQLUtil::store_int2(buffer, len, server_status_, pos)))
          {
            TBSYS_LOG(ERROR, "serialize packet server_status failed, buffer=%p, length=%ld,"
                      "server_status=%d, pos=%ld", buffer, len, server_status_, pos);
          }
        }

        if (OB_SUCCESS == ret)
        {
          if ((buffer + pos + PLUGIN_SIZE < buffer + len))
          {
            memcpy(buffer + pos, plugin_, PLUGIN_SIZE);
            pos += PLUGIN_SIZE;
          }
          else
          {
            TBSYS_LOG(ERROR, "not enough buffer to serialize plugin, buffer=%p, len=%ld,"
                      "scramble_buff length=%d,pos=%ld", buffer, len, PLUGIN_SIZE, pos);
            ret = OB_ERROR;
          }
        }

        if (OB_SUCCESS == ret)
        {
          if ((buffer + pos + PLUGIN2_SIZE < buffer + len))
          {
            memcpy(buffer + pos, plugin2_, PLUGIN2_SIZE);
            pos += PLUGIN2_SIZE;
          }
          else
          {
            TBSYS_LOG(ERROR, "not enough buffer to serialize plugin, buffer=%p, len=%ld,"
                      "plugin2 length=%d,pos=%ld", buffer, len, PLUGIN2_SIZE, pos);
            ret = OB_ERROR;
          }
        }

        if (OB_SUCCESS == ret)
        {
          if ((buffer + pos + 1 < buffer + len))
          {
            memset(buffer + pos, 0, 1);
            pos += 1;
          }
          else
          {
            TBSYS_LOG(ERROR, "not enough buffer to serialize filler2, buffer=%p, len=%ld,"
                      "terminated_ length=%c,pos=%ld", buffer, len, terminated_, pos);
            ret = OB_ERROR;
          }
        }

        uint32_t pkt_len = static_cast<uint32_t>(pos - start_pos - OB_MYSQL_PACKET_HEADER_SIZE);
        if (OB_SUCCESS == ret)
        {
          if (OB_SUCCESS != (ret = ObMySQLUtil::store_int3(buffer, len, pkt_len, start_pos)))
          {
            TBSYS_LOG(ERROR, "serialize packet haader size failed, buffer=%p, length=%ld, value=%d, pos=%ld",
                      buffer, len, pkt_len, start_pos);
          }
          else if (OB_SUCCESS != (ret = ObMySQLUtil::store_int1(buffer, len, 0, start_pos)))
          {
            TBSYS_LOG(ERROR, "serialize packet haader seq failed, buffer=%p, length=%ld, value=%d, pos=%ld",
                      buffer, len, 1, start_pos);
          }
        }
      }
      return ret;
    }

    int ObMySQLHandshakePacket::set_server_version(ObString& version)
    {
      int ret = OB_SUCCESS;
      if (NULL == version.ptr() || 0 >= version.length())
      {
        TBSYS_LOG(ERROR, "invalid argument version.ptr=%p, version.length()=%d",
                  version.ptr(), version.length());
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        ret = str_buf_.write_string(version, &server_version_);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR, "write string to server_version_ failed");
        }
      }
      return ret;
    }
  }
}
