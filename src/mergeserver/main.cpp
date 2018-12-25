#include "common/ob_define.h"
#include "common/base_main.h"
#include "common/ob_malloc.h"
#include "ob_merge_server_main.h"
#include <malloc.h>
#include <stdlib.h>
#include <time.h>
#include "easy_pool.h"
#include "tbsys.h"
#ifdef __OB_MDEBUG__
#include <dmalloc.h>
#endif

using namespace oceanbase::common;
using namespace oceanbase::mergeserver;
namespace
{
  static const int DEFAULT_MMAP_MAX_VAL = 1024*1024*1024;
};

int main(int argc, char *argv[])
{
#ifdef __OB_MDEBUG__
  /* set the dmalloc flags */
  dmalloc_debug_setup("debug=0x404c03,log=/tmp/dmalloc.log,inter=1");
#endif
  int rc  = OB_SUCCESS;
  mallopt(M_MMAP_MAX, DEFAULT_MMAP_MAX_VAL);
  //mallopt(M_MMAP_THRESHOLD, 32*1024); // 68KB
  srandom(static_cast<uint32_t>(time(NULL)));
  ob_init_memory_pool();
  tbsys::WarningBuffer::set_warn_log_on(true);

  int err = ObPhyOperatorGFactory::get_instance()->init();
  if (0 != err)
  {
    fprintf(stderr, "failed to init global factory\n");
    abort();
  }
  err = ObPhyPlanGFactory::get_instance()->init();
  if (0 != err)
  {
    fprintf(stderr, "failed to init global factory\n");
    abort();
  }
  err = ObPsStoreItemGFactory::get_instance()->init();
  if (0 != err)
  {
    fprintf(stderr, "failed to init global factory\n");
    abort();
  }

  //easy_pool_set_allocator(ob_malloc);
  BaseMain* mergeServer = ObMergeServerMain::get_instance();
  if (NULL == mergeServer)
  {
    fprintf(stderr, "mergeserver start failed, instance is NULL.");
    rc = OB_ERROR;
  }
  else
  {
    rc = mergeServer->start(argc, argv);
  }

  if (NULL != mergeServer)
  {
    mergeServer->destroy();
  }

  return rc;
}
