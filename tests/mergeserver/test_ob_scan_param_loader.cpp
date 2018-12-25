#include "ob_scan_param_loader.h"
#include "gtest/gtest.h"
#include <string>

using namespace oceanbase;
using namespace common;
using namespace mergeserver;
using namespace mergeserver::test;
using namespace std;

int main()
{
  int ret = OB_SUCCESS;
  ob_init_memory_pool();

  ObScanParamLoader *loader = new ObScanParamLoader();
  loader->load("test_data/schema_scan_param.txt", "scan_param");
  loader->dump_config();
  ObScanParam scan_param;
  ObScanParam org_param;
  scan_param.reset();
  org_param.reset();
  /* read from SQL file */
  if (OB_SUCCESS != (ret = loader->get_org_scan_param(org_param)))
  {
    TBSYS_LOG(WARN, "fail to get org scan param");
  }
  loader->decoded_scan_param(org_param, scan_param);
  scan_param.dump();
  return 0;
}
