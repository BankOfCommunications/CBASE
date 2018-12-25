
#ifndef OCEANBASE_SQLTEST_H_
#define OCEANBASE_SQLTEST_H_

#include "bigquerytest_param.h"
#include "write_worker.h"
#include "read_worker.h"
#include "prefix_info.h"

class BigqueryTest
{
  public:
    BigqueryTest();
    ~BigqueryTest();

    int init();
    int start();
    int stop();
    int wait();

  private:
    void translate_server_addr(const char* ob_addr, ServerAddr* addr, int64_t& addr_num);

  private:
    DISALLOW_COPY_AND_ASSIGN(BigqueryTest);

    KeyGenerator key_gen_;
    MysqlClient client_;
    PrefixInfo prefix_info_;
    BigqueryTestParam bigquerytest_param_;
    WriteWorker write_worker_;
    ReadWorker read_worker_;
};

#endif //OCEANBASE_SQLTEST_H_
