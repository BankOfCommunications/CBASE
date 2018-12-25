#include "common/ob_define.h"
#include "common/ob_string.h"
#include "common/utility.h"
#include "common/ob_server.h"

using namespace oceanbase::common;

int main(int argc, char ** argv)
{
  UNUSED(argc);
  UNUSED(argv);
  int ret = OB_SUCCESS;
  //char * test = "00596331192663719";
  char * test = "00912219024438372";
  ObString rowkey;
  rowkey.assign(test, strlen(test));
  char temp[1024];
  hex_to_str(rowkey.ptr(), rowkey.length(), temp, 1024);
  printf("test2str:test[%s], str[%s]\n", test, temp);

  int64_t ip = 539289610; //488957962; //522512394;
  ObServer server;
  server.set_ipv4_addr(ip, 1024);
  static const int32_t MAX_SERVER_ADDR_SIZE = 128;
  char server_addr[MAX_SERVER_ADDR_SIZE];
  server.to_string(server_addr, MAX_SERVER_ADDR_SIZE);
  printf("server:[%ld], server[%s]\n", ip, server_addr);
  return ret;
}

