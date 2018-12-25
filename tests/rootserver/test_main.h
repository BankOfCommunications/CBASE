#ifndef OCEANBASE_TESTS_ROOTSERVER_TESTMAIN_H_
#define OCEANBASE_TESTS_ROOTSERVER_TESTMAIN_H_
#include "base_main.h"
class BaseMainTest : public oceanbase::common::BaseMain
{
  public:
    static BaseMain* get_instance();
    int do_work();
  public:
    int test_flag;
  private:
    BaseMainTest();
};
#endif
