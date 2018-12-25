
#ifndef OCEANBASE_SQLTEST_H_
#define OCEANBASE_SQLTEST_H_

#include "sqltest_param.h"
#include "write_worker.h"
#include "read_worker.h"

class SqlTest
{
  public:
    SqlTest();
    ~SqlTest();

    int init();
    int start();
    int stop();
    int wait();

  private:
    int translate_user_schema(const ObSchemaManagerV2& ori_schema_mgr,
        ObSchemaManagerV2& user_schema_mgr);

  private:
    DISALLOW_COPY_AND_ASSIGN(SqlTest);

    SqlBuilder sql_builder_;
    KeyGenerator key_gen_;
    SqlTestPattern sqltest_pattern_;
    SqlTestDao sqltest_dao_;
    SqlTestSchema sqltest_schema_;
    SqlTestParam sqltest_param_;
    WriteWorker write_worker_;
    ReadWorker read_worker_;
};

#endif //OCEANBASE_SQLTEST_H_
