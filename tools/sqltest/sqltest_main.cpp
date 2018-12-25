
#include <new>
#include "sqltest_main.h"
#include "common/ob_malloc.h"


SqlTestMain::SqlTestMain()
{

}

SqlTestMain* SqlTestMain::get_instance()
{
  if (NULL == instance_)
  {
    instance_ = new (std::nothrow) SqlTestMain();
  }

  return dynamic_cast<SqlTestMain*>(instance_);
}

int SqlTestMain::do_work()
{
  return sqltest_.start();
}

void SqlTestMain::do_signal(const int sig)
{
  switch (sig)
  {
    case SIGTERM:
    case SIGINT:
      sqltest_.stop();
      break;
    default:
      break;
  }
}
