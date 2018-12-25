
#ifndef OCEANBASE_SQLTEST_MAIN_H_
#define OCEANBASE_SQLTEST_MAIN_H_

#include "common/base_main.h"
#include "bigquerytest.h"

class BigqueryTestMain : public BaseMain
{
  protected:
    BigqueryTestMain();

    virtual int do_work();
    virtual void do_signal(const int sig);

  public:
    static BigqueryTestMain* get_instance();

    const BigqueryTest& get_bigquerytest() const 
    {
      return bigquerytest_; 
    }

    BigqueryTest& get_bigquerytest() 
    {
      return bigquerytest_; 
    }

  private:
    DISALLOW_COPY_AND_ASSIGN(BigqueryTestMain);

    BigqueryTest bigquerytest_;
};

#endif

