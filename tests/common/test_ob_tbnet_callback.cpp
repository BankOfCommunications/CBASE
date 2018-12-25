/*
 * (C) 2013-2013 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Author:  
 *   yanran.hfs <yanran.hfs@alipay.com>
 */


#include <gtest/gtest.h>
#include "common/ob_packet.h"
#include "common/ob_tbnet_callback.h"

using namespace oceanbase;
using namespace common;

const int64_t bl = 256;
char          bf[bl];
ObDataBuffer  inner_buffer(bf, bl);
char          ub[0];
ObDataBuffer  user_buffer(ub, 0);

uint32_t chid = 100;
int32_t  pcode = 101;
int32_t  packet_len = 100;
uint16_t ob_packet_header_size = 16;
uint16_t api_version = 1;
int64_t  session_id = 1000;
int32_t  timeout = 1000000;
uint64_t trace_id = 10001000;
uint64_t req_sign = 11;
int32_t  pri = -1;
int32_t  padding1 = 0;

bool ob_packet_encode(char* output, int64_t len)
{
  int rc = OB_SUCCESS;
  bool ret = true;
  int64_t pos = 0;
  if (    (OB_SUCCESS == (rc = serialization::encode_i32(output, len, pos, OB_TBNET_PACKET_FLAG)))
       && (OB_SUCCESS == (rc = serialization::encode_i32(output, len, pos, chid)))
       && (OB_SUCCESS == (rc = serialization::encode_i32(output, len, pos, pcode)))
       && (OB_SUCCESS == (rc = serialization::encode_i32(output, len, pos, packet_len)))
       && (OB_SUCCESS == (rc = serialization::encode_i16(output, len, pos, ob_packet_header_size)))
       && (OB_SUCCESS == (rc = serialization::encode_i16(output, len, pos, api_version)))
       && (OB_SUCCESS == (rc = serialization::encode_i64(output, len, pos, session_id)))
       && (OB_SUCCESS == (rc = serialization::encode_i32(output, len, pos, timeout)))
       && (OB_SUCCESS == (rc = serialization::encode_i64(output, len, pos, trace_id)))
       && (OB_SUCCESS == (rc = serialization::encode_i64(output, len, pos, req_sign))))

  {
    memcpy(output + pos, inner_buffer.get_data(), inner_buffer.get_position());
  }

  if (OB_SUCCESS != rc)
  {
    TBSYS_LOG(ERROR, "encode packet into request output buffer faild, output is %p, pos is %ld",
              output, pos);
    ret = false;
  }
  return ret;
}

bool ob_packet_encode_future(char* output, int64_t len)
{
  int rc = OB_SUCCESS;
  bool ret = true;
  int64_t pos = 0;
  if (    (OB_SUCCESS == (rc = serialization::encode_i32(output, len, pos, OB_TBNET_PACKET_FLAG)))
       && (OB_SUCCESS == (rc = serialization::encode_i32(output, len, pos, chid)))
       && (OB_SUCCESS == (rc = serialization::encode_i32(output, len, pos, pcode)))
       && (OB_SUCCESS == (rc = serialization::encode_i32(output, len, pos, packet_len)))
       && (OB_SUCCESS == (rc = serialization::encode_i16(output, len, pos, ob_packet_header_size)))
       && (OB_SUCCESS == (rc = serialization::encode_i16(output, len, pos, api_version)))
       && (OB_SUCCESS == (rc = serialization::encode_i64(output, len, pos, session_id)))
       && (OB_SUCCESS == (rc = serialization::encode_i32(output, len, pos, timeout)))
       && (OB_SUCCESS == (rc = serialization::encode_i64(output, len, pos, trace_id)))
       && (OB_SUCCESS == (rc = serialization::encode_i64(output, len, pos, req_sign)))
       && (OB_SUCCESS == (rc = serialization::encode_i32(output, len, pos, pri)))
       && (OB_SUCCESS == (rc = serialization::encode_i32(output, len, pos, padding1))))
  {
    memcpy(output + pos, inner_buffer.get_data(), inner_buffer.get_position());
  }

  if (OB_SUCCESS != rc)
  {
    TBSYS_LOG(ERROR, "encode packet into request output buffer faild, output is %p, pos is %ld",
              output, pos);
    ret = false;
  }
  return ret;
}

TEST(tbnet_decode, decode)
{
  char buf[BUFSIZ];
  easy_buf_t b; b.pos = b.last = buf; b.end = buf + BUFSIZ;
  easy_message_t m;
  m.pool = easy_pool_create(BUFSIZ);
  m.input = &b;
  ob_packet_header_size = sizeof(ob_packet_header_size) + sizeof(int16_t)/* api_version_ */ + sizeof(int64_t) /* session_id_ */ + sizeof(int32_t)/* timeout_ */ + sizeof(uint64_t)/* trace_id_ */ + sizeof(uint64_t) /* req_sign_ */;
  packet_len = static_cast<int32_t>(inner_buffer.get_position() + ob_packet_header_size);
  ob_packet_encode(buf, BUFSIZ);
  b.last = b.pos + OB_TBNET_HEADER_LENGTH + packet_len;
  ObPacket * packet = reinterpret_cast<ObPacket *>(ObTbnetCallback::decode(&m));
  ASSERT_NE(reinterpret_cast<ObPacket *>(NULL), packet);
  ASSERT_EQ(chid, packet->get_channel_id());
  ASSERT_EQ(pcode, packet->get_packet_code());
  ASSERT_EQ(packet_len, packet->get_packet_len());
  ASSERT_EQ(ob_packet_header_size, packet->get_ob_packet_header_size());
  ASSERT_EQ(api_version, packet->get_api_version());
  ASSERT_EQ(session_id, packet->get_session_id());
  ASSERT_EQ(timeout, packet->get_source_timeout());
  ASSERT_EQ(trace_id, packet->get_trace_id());
  ASSERT_EQ(req_sign, packet->get_req_sign());

  b.pos = b.last = buf; b.end = buf + BUFSIZ;
  ob_packet_header_size = sizeof(ob_packet_header_size) + sizeof(int16_t)/* api_version_ */ + sizeof(int64_t) /* session_id_ */ + sizeof(int32_t)/* timeout_ */ + sizeof(uint64_t)/* trace_id_ */ + sizeof(uint64_t) /* req_sign_ */ + sizeof(pri) + sizeof(padding1);
  packet_len = static_cast<int32_t>(inner_buffer.get_position() + ob_packet_header_size);
  ob_packet_encode_future(buf, BUFSIZ);
  b.last = b.pos + OB_TBNET_HEADER_LENGTH + packet_len;
  packet = reinterpret_cast<ObPacket *>(ObTbnetCallback::decode(&m));
  ASSERT_NE(reinterpret_cast<ObPacket *>(NULL), packet);
  ASSERT_EQ(chid, packet->get_channel_id());
  ASSERT_EQ(pcode, packet->get_packet_code());
  ASSERT_EQ(packet_len, packet->get_packet_len());
  ASSERT_EQ(ob_packet_header_size, packet->get_ob_packet_header_size());
  ASSERT_EQ(api_version, packet->get_api_version());
  ASSERT_EQ(session_id, packet->get_session_id());
  ASSERT_EQ(timeout, packet->get_source_timeout());
  ASSERT_EQ(trace_id, packet->get_trace_id());
  ASSERT_EQ(req_sign, packet->get_req_sign());
}

int main(int argc, char **argv)
{
  testing::InitGoogleTest(&argc,argv);
  ObPacket packet;
  packet.set_data(user_buffer);
  packet.serialize(&inner_buffer);
  TBSYS_LOGGER.setLogLevel("INFO");
  return RUN_ALL_TESTS();
}

