#include <tbsys.h>

#include "test_main.h" 

BaseMainTest::BaseMainTest()
{
  test_flag = 0;
}

oceanbase::common::BaseMain* BaseMainTest::get_instance()
{
  if (instance_ == NULL)
  {
    instance_ = new BaseMainTest();
  }
  return instance_;

}

int BaseMainTest::do_work() 
{
  TBSYS_LOG(DEBUG, "debug do_work");
  TBSYS_LOG(INFO, "info do_work");
  TBSYS_LOG(WARN, "warn do_work");
  return 0;
}

