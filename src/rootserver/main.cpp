/*===============================================================
*   (C) 2007-2010 Taobao Inc.
*
*
*   Version: 0.1 2010-09-26
*
*   Authors:
*          daoan(daoan@taobao.com)
*
*
================================================================*/
#include <malloc.h>
#include "tbsys.h"
#include "common/ob_malloc.h"
#include "ob_root_main.h"

using namespace oceanbase::rootserver;
using namespace oceanbase::common;

int main(int argc, char *argv[])
{
  static const int DEFAULT_MMAP_MAX_VAL = 1024*1024*1024;
  mallopt(M_MMAP_MAX, DEFAULT_MMAP_MAX_VAL);
  ob_init_memory_pool();
  tbsys::WarningBuffer::set_warn_log_on(true);
  BaseMain* pmain = ObRootMain::get_instance();
  if (pmain == NULL)
  {
    perror("not enought mem, exit \n");
  }
  else
  {
    pmain->start(argc, argv);
    pmain->destroy();
  }
  return 0;
}
