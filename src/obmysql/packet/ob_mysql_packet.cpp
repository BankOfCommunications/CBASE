/* (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software: you can redistribute it and/or
 *  modify it under the terms of the GNU General Public License
 *  version 2 as published by the Free Software Foundation.
 *
 * Version: 0.1
 *
 * Authors:
 *    Wu Di <lide.wd@taobao.com>
 */
#include "common/ob_define.h"
#include "obmysql/packet/ob_mysql_packet.h"
#include "obmysql/ob_mysql_util.h"

using namespace oceanbase;
using namespace common;
using namespace oceanbase::obmysql;

int ObMySQLPacket::encode(char* buffer, int64_t length, int64_t& pos)
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
      else if (OB_SUCCESS != (ret = ObMySQLUtil::store_int1(buffer, length, header_.seq_, start_pos)))
      {
        TBSYS_LOG(ERROR, "serialize packet haader seq failed, buffer=%p, buffer length=%ld, seq number=%u, pos=%ld",
                  buffer, length, header_.seq_, start_pos);
      }
    }
    else
    {
      if (OB_LIKELY(OB_BUF_NOT_ENOUGH == ret || OB_SIZE_OVERFLOW == ret))
      {
        //do nothing
      }
      else
      {
        TBSYS_LOG(ERROR, "encode packet data failed, ret is %d", ret);
      }
    }

    if (OB_SUCCESS != ret)
    {
      pos = start_pos;
    }
  }
  return ret;
}
