#include "ob_mysql_row_packet.h"

namespace oceanbase
{
  namespace obmysql
  {
    ObMySQLRowPacket::ObMySQLRowPacket(const ObMySQLRow* row):row_(row)
    {

    }

    int ObMySQLRowPacket::serialize(char* buffer, int64_t length, int64_t& pos)
    {
      return row_->serialize(buffer, length, pos);
    }

    uint64_t ObMySQLRowPacket::get_serialize_size()
    {
      // @bug
      OB_ASSERT(0);
      return 0;
    }

  }
}
