#include <string.h>
#include <stdio.h>
#include "ob_server_ext.h"
#include "gtest/gtest.h"
#include "common/ob_define.h"
#include "tblog.h"

using namespace oceanbase::common;

TEST(TestServerExt, get_host_name)
{
  ObServerExt serverExt;
  char ip[128] = "10.232.36.35";
  ObServer server(ObServer::IPV4, ip, 2700);
  serverExt.init((char*)"hostname", server);

  const char * phost = serverExt.get_hostname();
  EXPECT_EQ(0, strcmp("hostname", phost));

}

TEST(TestServerExt, serialize)
{
  ObServerExt serverExt;
  char ip[128] = "10.232.36.35";
  ObServer server(ObServer::IPV4, ip, 2700);
  serverExt.init((char*)"hostname", server);

  char temp[10];
  int64_t buf_len = 10;
  int64_t pos = 0;

  EXPECT_NE(OB_SUCCESS, serverExt.serialize(temp, buf_len, pos));

  char* buf = new char[10000];
  buf_len = 10000;
  EXPECT_EQ(OB_SUCCESS,serverExt.serialize(buf, buf_len, pos));

  delete [] buf;
}

TEST(TestServerExt, deserialize)
{
  ObServerExt serverExt;
  char ip[128] = "10.232.36.35";
  ObServer server(ObServer::IPV4, ip, 2700);
  serverExt.init((char*)"hostname", server);

  char* buf = new char[10000];
  int64_t buf_len = 10000;
  int64_t pos = 10;
  EXPECT_EQ(OB_SUCCESS,serverExt.serialize(buf, buf_len, pos));

  ObServerExt serverExt_to;
  pos = 10;
  serverExt_to.deserialize(buf, buf_len, pos);

  EXPECT_EQ(0, strcmp((char*)"hostname", serverExt_to.get_hostname()));
  uint32_t ip_int = server.convert_ipv4_addr(ip);
  EXPECT_EQ(ip_int, serverExt_to.get_server().get_ipv4());
  EXPECT_EQ(2700, serverExt_to.get_server().get_port());
  delete [] buf;
}

TEST(TestServerExt, get_serialize_size)
{
  ObServerExt serverExt;
  char ip[128] = "10.232.36.35";
  ObServer server(ObServer::IPV4, ip, 2700);
  serverExt.init((char*)"hostname", server);

  int64_t size = server.get_serialize_size();
  printf("server size = %ld", size);

  int64_t size2 = serverExt.get_serialize_size();
  printf("serverExt size = %ld", size2);
}

int main(int argc, char **argv)
{
  testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
