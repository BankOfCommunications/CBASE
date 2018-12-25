#include "ob_location_list_cache_loader.h"
#include "gtest/gtest.h"
#include <string>

using namespace oceanbase;
using namespace common;
using namespace mergeserver;
using namespace mergeserver::test;
using namespace std;

void dump_decoded_location_list_cache(ObTabletLocationCache &p)
{
  UNUSED(p);
  /*
  int i = 0;
  TBSYS_LOG(INFO, "table_id[%lu]", p.get_table_id());
  TBSYS_LOG(INFO, "range info:");
  TBSYS_LOG(INFO, "scan flag[%x]", p.get_scan_flag());
  TBSYS_LOG(INFO, "scan direction[%x]", p.get_scan_direction());
  TBSYS_LOG(INFO, "column name size[%d]", p.get_column_name_size());
  TBSYS_LOG(INFO, "column id size[%d]", p.get_column_id_size());
  for (i = 0; i < p.get_column_id_size(); i++)
  {
    TBSYS_LOG(INFO, "\tid[%d]=%d", i, p.get_column_id()[i]);
  }
  */
}

int main()
{
  char *filename = (char*)"schema_location_list_cache.txt";
  ob_init_memory_pool();

  ObLocationListCacheLoader *loader = new ObLocationListCacheLoader();
  if (OB_SUCCESS != loader->load(filename, "tablet_location_cache"))
  {
    TBSYS_LOG(ERROR, "fail to load schema file %s", filename);
  }
  else
  {
    loader->dump_config();
    /* read from SQL file */
    ObTabletLocationCache cache;
    loader->get_decoded_location_list_cache(cache);
    dump_decoded_location_list_cache(cache);
    TBSYS_LOG(INFO, "end");
  }
  return 0;
}
