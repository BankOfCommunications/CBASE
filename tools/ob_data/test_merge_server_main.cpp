#include "mock_merge_server.h"
#include "common/ob_malloc.h"

using namespace oceanbase::mergeserver;

int main()
{
  ob_init_memory_pool();
  MockMergeServer server;
  int ret = OB_SUCCESS;
  MockServerRunner merge_server(server);
  tbsys::CThread merge_server_thread;
  merge_server_thread.start(&merge_server, NULL);
  merge_server_thread.join();
  return 0;
}
