#include "ob_scanner_loader.h"
#include "gtest/gtest.h"
#include <string>

using namespace oceanbase;
using namespace common;
using namespace mergeserver;
using namespace mergeserver::test;
using namespace std;

void dump_decoded_scanner(ObScanner &p)
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
  ob_init_memory_pool();

  ObScannerLoader *loader = new ObScannerLoader();
  loader->load("test_data/schema_scan_test_1.txt", "schema_scanner_3_4");
  loader->dump_config();
  ObScanner scanner;
  scanner.reset();
  /* read from SQL file */
  loader->get_decoded_scanner(scanner);
  dump_decoded_scanner(scanner);
  scanner.dump();
  return 0;
}
