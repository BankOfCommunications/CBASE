#include "ob_import_param.h"


int main()
{
  ImportParam param;
  int ret = param.load("test_import.ini");
  assert(ret == 0);
  param.PrintDebug();
}
