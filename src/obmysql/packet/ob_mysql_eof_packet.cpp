#include "ob_mysql_eof_packet.h"
#include "../ob_mysql_util.h"

using namespace oceanbase::common;

namespace oceanbase
{
  namespace obmysql
  {
    ObMySQLEofPacket::ObMySQLEofPacket()
      :field_count_(0xfe),
       warning_count_(0),
       server_status_(0)
    {

    }

    ObMySQLEofPacket::~ObMySQLEofPacket()
    {

    }

    int ObMySQLEofPacket::serialize(char* buffer, int64_t len, int64_t& pos)
    {
      int ret = OB_SUCCESS;

      if (NULL == buffer || len <= 0 || pos < 0)
      {
        TBSYS_LOG(ERROR, "invalid argument buffer=%p, len=%ld, pos=%ld",
                  buffer, len, pos);
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        if (OB_SUCCESS != (ret = ObMySQLUtil::store_int1(buffer, len, field_count_, pos)))
        {
          if (OB_UNLIKELY(OB_SIZE_OVERFLOW != ret))
          {
            TBSYS_LOG(ERROR, "serialize field_count failed, buffer=%p, len=%ld, field_count=%u, pos=%ld, ret=%d",
                      buffer, len, field_count_, pos, ret);
          }
        }
        else if (OB_SUCCESS != (ret = ObMySQLUtil::store_int2(buffer, len, warning_count_, pos)))
        {
          if (OB_UNLIKELY(OB_SIZE_OVERFLOW != ret))
          {
            TBSYS_LOG(ERROR, "serialize warning_count failed, buffer=%p, len=%ld, warning_count=%u, pos=%ld, ret=%d",
                      buffer, len, warning_count_, pos, ret);
          }
        }
        else if (OB_SUCCESS != (ret = ObMySQLUtil::store_int2(buffer, len, server_status_, pos)))
        {
          if (OB_UNLIKELY(OB_SIZE_OVERFLOW != ret))
          {
            TBSYS_LOG(ERROR, "serialize marker failed, buffer=%p, len=%ld, marker=%c, pos=%ld, ret=%d",
                      buffer, len, server_status_, pos, ret);
          }
        }
      }
      return ret;
    }

    uint64_t ObMySQLEofPacket::get_serialize_size()
    {
      uint64_t len = 0;
      len += 1;                 // field_count_
      len += 2;                 // warning_count_
      len += 2;                 // server_status_
      return len;
    }
  }
}
