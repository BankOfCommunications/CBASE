
#include <new>
#include "bigquerytest_main.h"
#include "common/ob_malloc.h"


BigqueryTestMain::BigqueryTestMain()
{

}

BigqueryTestMain* BigqueryTestMain::get_instance()
{
  if (NULL == instance_)
  {
    instance_ = new (std::nothrow) BigqueryTestMain();
  }

  return dynamic_cast<BigqueryTestMain*>(instance_);
}

int BigqueryTestMain::do_work()
{
  return bigquerytest_.start();
}

void BigqueryTestMain::do_signal(const int sig)
{
  switch (sig)
  {
    case SIGTERM:
    case SIGINT:
      bigquerytest_.stop();
      break;
    default:
      break;
  }
}
