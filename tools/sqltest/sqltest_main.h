
#ifndef OCEANBASE_SQLTEST_MAIN_H_
#define OCEANBASE_SQLTEST_MAIN_H_

#include "common/base_main.h"
#include "sqltest.h"

class SqlTestMain : public BaseMain
{
  protected:
    SqlTestMain();

    virtual int do_work();
    virtual void do_signal(const int sig);

  public:
    static SqlTestMain* get_instance();

    const SqlTest& get_sqltest() const 
    {
      return sqltest_; 
    }

    SqlTest& get_sqltest() 
    {
      return sqltest_; 
    }

  private:
    DISALLOW_COPY_AND_ASSIGN(SqlTestMain);

    SqlTest sqltest_;
};

#endif

