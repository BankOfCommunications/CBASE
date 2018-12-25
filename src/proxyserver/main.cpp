/*
 *   (C) 2007-2010 Taobao Inc.
 *
 *   Version: 0.1
 *
 *   Authors:
 *      qushan <qushan@taobao.com>
 *        - some work details if you want
 *
 */

#include <new>
#include <malloc.h>
#include "common/ob_define.h"
#include "ob_proxy_server_main.h"
#include "easy_pool.h"

using namespace oceanbase::common;
using namespace oceanbase::proxyserver;

namespace
{
  // tc malloc allocate memory with 65K and 2M+1K, and the kernel allocate memory
  // with 4K aligned, so we must ensure the 65K memory is allocated from heap, 
  // and memory which is bigger than 68K is allocted from mmap arena. 
  static const int DEFAULT_MMAP_THRESHOLD = 68 * 1024; //68K
}

int main(int argc, char* argv[])
{
  //easy_pool_set_allocator(ob_malloc);
  ::mallopt(M_MMAP_THRESHOLD, DEFAULT_MMAP_THRESHOLD);
  ob_init_memory_pool();
  ObProxyServerMain* pm = ObProxyServerMain::get_instance();
  int ret = OB_SUCCESS;
  if (NULL == pm)
  {
    fprintf(stderr, "cannot start proxy, new ObProxyServerMain failed\n");
    ret = OB_ERROR;
  }
  else
  {
    ret = pm->start(argc, argv);
    pm->destroy();

    return ObProxyServerMain::restart_server(argc, argv);
  }
  // never get here.
}
