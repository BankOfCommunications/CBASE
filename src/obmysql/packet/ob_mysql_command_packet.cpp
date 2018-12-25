#include "ob_mysql_command_packet.h"

using namespace oceanbase::common;

namespace oceanbase
{
  namespace obmysql
  {
    ObMySQLCommandPacket::ObMySQLCommandPacket() : type_(0), req_(NULL), next_(NULL), receive_ts_(-1)
    {

    }

    ObMySQLCommandPacket::~ObMySQLCommandPacket()
    {

    }

    int32_t ObMySQLCommandPacket::get_command_length() const
    {
      return command_.length();
    }

    int ObMySQLCommandPacket::set_request(easy_request_t* req)
    {
      int ret = OB_SUCCESS;
      if (NULL == req)
      {
        TBSYS_LOG(ERROR, "invalid argument reqeust is %p", req);
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        req_ = req;
      }
      return ret;
    }

    void ObMySQLCommandPacket::set_command(char* data, const int32_t length)
    {
      command_.assign_ptr(data, length);
    }
    void ObMySQLCommandPacket::set_receive_ts(const int64_t &now)
    {
      receive_ts_ = now;
    }
    int64_t ObMySQLCommandPacket::get_receive_ts() const
    {
      return receive_ts_;
    }
  }
}
