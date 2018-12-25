#include "gtest/gtest.h"
#include "liboblog/ob_log_mysql_adaptor.h"

using namespace oceanbase;
using namespace common;
using namespace updateserver;
using namespace liboblog;

TEST(TestLogMysqlAdaptor, init)
{
  ObLogConfig config;
  ObLogServerSelector server_selector;
  ObLogRpcStub rpc_stub;
  ObLogSchemaGetter schema_getter;
  ObLogMysqlAdaptor mysql_adaptor;

  config.init("./liboblog.conf");
  server_selector.init(config);
  rpc_stub.init();
  schema_getter.init(&server_selector, &rpc_stub);
  mysql_adaptor.init(config, &schema_getter);

  ObObj objs[4];
  objs[0].set_int(13490859);
  objs[1].set_int(0);
  objs[2].set_int(1);
  ObString str;
  str.assign_ptr((char*)("fenxiao-001"), 11);
  objs[3].set_varchar(str);
  ObRowkey rk;
  rk.assign(objs, 4);

  const IObLogColValue *list = NULL;
  int ret = mysql_adaptor.query_whole_row(3001, rk, list);
  TBSYS_LOG(INFO, "query_whole_row ret=%d", ret);

  const IObLogColValue *iter = list;
  while (NULL != iter)
  {
    std::string str;
    iter->to_str(str);
    TBSYS_LOG(INFO, "[VALUE] [%s]", str.c_str());
    iter = iter->get_next();
  }

  ////////////////////

  objs[0].set_int(2222504619);
  str.assign_ptr((char*)("IC"), 2);
  objs[3].set_varchar(str);

  list = NULL;
  ret = mysql_adaptor.query_whole_row(3001, rk, list);
  TBSYS_LOG(INFO, "query_whole_row ret=%d", ret);

  iter = list;
  while (NULL != iter)
  {
    std::string str;
    iter->to_str(str);
    TBSYS_LOG(INFO, "[VALUE] [%s]", str.c_str());
    iter = iter->get_next();
  }
}

int main(int argc, char **argv)
{
  ob_init_memory_pool();
  testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}

