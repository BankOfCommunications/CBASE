#include "updateserver/ob_store_mgr.h"

using namespace oceanbase;
using namespace common;
using namespace updateserver;

int main(int argc, char **argv)
{
  if (4 != argc)
  {
    fprintf(stderr, "./test_storemgr [store_root] [raid_regex] [store_regex]\n");
    exit(-1);
  }

  ob_init_memory_pool();
  StoreMgr sm;

  int ret = sm.init(argv[1], argv[2], argv[3]);
  fprintf(stderr, "init ret=%d\n", ret);

  sm.load();

  ObList<StoreMgr::Handle> list;
  ret = sm.assign_stores(list);
  fprintf(stderr, "assign stores ret=%d\n", ret);
  ObList<StoreMgr::Handle>::iterator list_iter;
  for (list_iter = list.begin(); list_iter != list.end(); list_iter++)
  {
    fprintf(stderr, "store %s %lu\n", sm.get_dir(*list_iter), *list_iter);
  }

  sm.report_broken(sm.get_dir(0));

  ret = sm.assign_stores(list);
  fprintf(stderr, "assign stores ret=%d\n", ret);
  for (list_iter = list.begin(); list_iter != list.end(); list_iter++)
  {
    fprintf(stderr, "store %s %lu\n", sm.get_dir(*list_iter), *list_iter);
  }

  sm.umount(0);

  symlink("/tmp", "data/raid1/store5");
  sm.load();
  ret = sm.assign_stores(list);
  fprintf(stderr, "assign stores ret=%d\n", ret);
  for (list_iter = list.begin(); list_iter != list.end(); list_iter++)
  {
    fprintf(stderr, "store %s %lu\n", sm.get_dir(*list_iter), *list_iter);
  }
}

