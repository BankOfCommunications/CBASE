#include "ob_mysql_field_packet.h"

namespace oceanbase
{
  namespace obmysql
  {
    ObMySQLFieldPacket::ObMySQLFieldPacket(ObMySQLField* field)
      :field_(field)
    {

    }

    int ObMySQLFieldPacket::serialize(char* buffer, int64_t length, int64_t& pos)
    {
      return field_->serialize(buffer, length, pos);
    }

    uint64_t ObMySQLFieldPacket::get_serialize_size()
    {
      // @bug
      OB_ASSERT(0);
      return 0;
    }

  }
}
