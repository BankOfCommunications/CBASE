#include "ob_ocm_app_info.h"
#include "gtest/gtest.h"

using namespace oceanbase::common;
using namespace oceanbase::clustermanager;
TEST(TestOcmAppInfo, set_app_name)
{
  ObOcmAppInfo app;
  EXPECT_EQ(0, app.set_app_name("favorite"));
}

TEST(TestOcmAppInfo, get_app_name)
{
  ObOcmAppInfo app;
  app.init("app_name");
  EXPECT_EQ(0, strcmp("app_name", app.get_app_name()));
  EXPECT_NE(0, strcmp("app_namei", app.get_app_name()));
}

TEST(TestOcmAppInfo, get_obi)
{
  ObOcmAppInfo app;
  app.init("app_name");
  app.get_obi(1);
  ObOcmInstance ocm_instance;
  ObServerExt serverExt;
  char ip[128] = "10.232.36.35";
  ObServer server(ObServer::IPV4, ip, 2700);
  serverExt.init((char*)"hostname", server);
  char instance_name[16] = "instance1";
  EXPECT_EQ(0, ocm_instance.init(instance_name, serverExt));

  EXPECT_EQ(0,app.add_obi(ocm_instance));

  const ObOcmInstance *instance = app.get_obi(0);
  EXPECT_EQ(0, strcmp(instance_name, instance->get_instance_name()));

  app.get_obi(1);
}

// int get_instance_master_rs(const char *app_name, const char *instance_name, ObServerExt &serv_info) const;
TEST(TestOcmAppInfo, get_instance_master_rs)
{
  ObOcmAppInfo app;
  app.init("app_name");

  ObOcmInstance ocm_instance1, ocm_instance2, ocm_instance3;
  ObServerExt serverExt;
  char ip[128] = "10.232.36.35";
  ObServer server(ObServer::IPV4, ip, 2700);
  serverExt.init((char*)"hostname", server);
  char instance_name1[16] = "instance1";
  //ocm_instance1
  EXPECT_EQ(0, ocm_instance1.init(instance_name1, serverExt));
  EXPECT_EQ(0, app.add_obi(ocm_instance1));

  //ocm_instance2
  char instance_name2[16] = "instance2";
  EXPECT_EQ(0, ocm_instance2.init(instance_name2, serverExt));
  EXPECT_EQ(0, app.add_obi(ocm_instance2));

  //ocm_instance3
  char instance_name3[16] = "instance3";
  ObServerExt serverExt3;
  ObServer server3(ObServer::IPV4, ip, 2701);
  serverExt3.init((char*)"hostname3", server3);
  EXPECT_EQ(0, ocm_instance3.init(instance_name3, serverExt3));
  EXPECT_EQ(0, app.add_obi(ocm_instance3));
  app.set_instance_role(instance_name3, MASTER);

  ObServerExt serv_info;
  EXPECT_NE(0, app.get_instance_master_rs("app_name", "instance_name3", serv_info));
  EXPECT_NE(0, app.get_instance_master_rs("app_name", "instance4", serv_info));
  EXPECT_EQ(0, app.get_instance_master_rs("app_name", instance_name3, serv_info));
  EXPECT_EQ(0, strcmp(serv_info.get_hostname(), serverExt3.get_hostname()));
}
//int get_instance_master_rs_list(const char *app_name, ObServList &serv_list) const;
TEST(TestOcmAppInfo, get_instance_master_rs_list)
{
  ObOcmAppInfo app;
  app.init("app_name");

  ObOcmInstance ocm_instance1, ocm_instance2, ocm_instance3;
  ObServerExt serverExt;
  char ip[128] = "10.232.36.35";
  ObServer server(ObServer::IPV4, ip, 2700);
  serverExt.init((char*)"hostname", server);
  char instance_name1[16] = "instance1";
  //ocm_instance1
  EXPECT_EQ(0, ocm_instance1.init(instance_name1, serverExt));
  EXPECT_EQ(0, app.add_obi(ocm_instance1));

  //ocm_instance2
  char instance_name2[16] = "instance2";
  EXPECT_EQ(0, ocm_instance2.init(instance_name2, serverExt));
  EXPECT_EQ(0, app.add_obi(ocm_instance2));

  //ocm_instance3
  char instance_name3[16] = "instance3";
  ObServerExt serverExt3;
  ObServer server3(ObServer::IPV4, ip, 2701);
  serverExt3.init((char*)"hostname3", server3);
  EXPECT_EQ(0, ocm_instance3.init(instance_name3, serverExt3));
  EXPECT_EQ(0, app.add_obi(ocm_instance3));
  ObServList serv_list;
  EXPECT_NE(0, app.get_instance_master_rs_list("app", serv_list));
  EXPECT_EQ(0, serv_list.size());
  EXPECT_EQ(0, app.get_instance_master_rs_list("app_name", serv_list));
  EXPECT_EQ(3, serv_list.size());
  ObServList::iterator iter = serv_list.begin();
  for(; iter != serv_list.end(); iter++)
  {
    TBSYS_LOG(INFO, "hostname=%s", (*iter)->get_hostname());
  }
}

TEST(TestOcmAppInfo,get_app_master_rs)
{
  ObOcmAppInfo app;
  app.init("app_name");

  ObOcmInstance ocm_instance1, ocm_instance2, ocm_instance3;
  ObServerExt serverExt;
  char ip[128] = "10.232.36.35";
  ObServer server(ObServer::IPV4, ip, 2700);
  serverExt.init((char*)"hostname", server);
  char instance_name1[16] = "instance1";
  //ocm_instance1
  EXPECT_EQ(0, ocm_instance1.init(instance_name1, serverExt));
  EXPECT_EQ(0, app.add_obi(ocm_instance1));

  //ocm_instance2
  char instance_name2[16] = "instance2";
  EXPECT_EQ(0, ocm_instance2.init(instance_name2, serverExt));
  EXPECT_EQ(0, app.add_obi(ocm_instance2));

  ObServerExt serv_info;
  //在没有master instance的时候调用
  EXPECT_EQ(OB_ENTRY_NOT_EXIST, app.get_app_master_rs("app_name", serv_info));

  //ocm_instance3
  char instance_name3[16] = "instance3";
  ObServerExt serverExt3;
  ObServer server3(ObServer::IPV4, ip, 2701);
  serverExt3.init((char*)"hostname3", server3);
  EXPECT_EQ(0, ocm_instance3.init(instance_name3, serverExt3));
  EXPECT_EQ(0, app.add_obi(ocm_instance3));
  app.set_instance_role(instance_name3, MASTER);

  //正常调用返回0
  EXPECT_EQ(0, app.get_app_master_rs("app_name", serv_info));
  //比较结果是否正确
  EXPECT_EQ(0, strcmp("hostname3", serv_info.get_hostname()));
  EXPECT_EQ(2701, serv_info.get_server().get_port());
}

TEST(TestOcmAppInfo, add_obi)
{
  ObOcmAppInfo app;
  ObOcmInstance ocm_instance;
  ObServerExt serverExt;
  char ip[128] = "10.232.36.35";
  ObServer server(ObServer::IPV4, ip, 2700);
  serverExt.init((char*)"hostname", server);
  char instance_name[16] = "instance1";
  EXPECT_EQ(0, ocm_instance.init(instance_name, serverExt));

  EXPECT_EQ(0,app.add_obi(ocm_instance));
  //add the same obi
  EXPECT_NE(0,app.add_obi(ocm_instance));
  EXPECT_EQ(1, app.get_obi_count());

  //add another obi
  ObOcmInstance ocm_instance2;
  char instance_name2[16] = "instance2";
  EXPECT_EQ(0, ocm_instance2.init(instance_name2, serverExt));
  EXPECT_EQ(0,app.add_obi(ocm_instance2));

  //check after add
  EXPECT_EQ(2, app.get_obi_count());
  EXPECT_EQ(0, strcmp(instance_name2, app.get_obi(1)->get_instance_name()));
}

TEST(TestOcmAppInfo, get_obi_count)
{
  const ObOcmAppInfo app;
  EXPECT_EQ(0, app.get_obi_count());
}

TEST(TestOcmAppInfo, serialize)
{
  //one instance
  ObOcmAppInfo app;
  ObOcmInstance ocm_instance;
  ObServerExt serverExt;
  char ip[128] = "10.232.36.35";
  ObServer server(ObServer::IPV4, ip, 2700);
  serverExt.init((char*)"hostname", server);
  char instance_name[16] = "instance1";
  EXPECT_EQ(0, ocm_instance.init(instance_name, serverExt));

  EXPECT_EQ(0,app.add_obi(ocm_instance));
  char *buf = new char[516];
  int64_t buf_len = 12;
  int64_t pos = 0;
  //错误长度
  EXPECT_NE(0, app.serialize(buf, buf_len, pos));
  buf_len = 516;
  pos = 0;
  //正常解析
  EXPECT_EQ(0, app.serialize(buf, buf_len, pos));
  TBSYS_LOG(INFO, "get serialize size1 =%d", app.get_serialize_size());
  //多加一个obi
  ObOcmInstance ocm_instance2;
  char instance_name2[16] = "instance2";
  EXPECT_EQ(0, ocm_instance2.init(instance_name2, serverExt));
  EXPECT_EQ(0,app.add_obi(ocm_instance2));
  EXPECT_EQ(2, app.get_obi_count());

  //正常解析
  pos = 0;
  EXPECT_EQ(0, app.serialize(buf, buf_len, pos));
  TBSYS_LOG(INFO, "get serialize size2 =%d", app.get_serialize_size());

  delete [] buf;
}

TEST(TestOcmAppInfo, deserialize)
{
  //只有一个实例
  ObOcmAppInfo app;
  app.init("app_name");
  ObOcmInstance ocm_instance;
  ObServerExt serverExt;
  char ip[128] = "10.232.36.35";
  ObServer server(ObServer::IPV4, ip, 2700);
  serverExt.init((char*)"hostname", server);
  char instance_name[16] = "instance1";
  EXPECT_EQ(0, ocm_instance.init(instance_name, serverExt));

  ocm_instance.set_read_radio(50);
  EXPECT_EQ(0,app.add_obi(ocm_instance));
  char *buf = new char[516];
  int64_t buf_len = 12;
  int64_t pos = 0;
  //错误长度
  EXPECT_NE(0, app.serialize(buf, buf_len, pos));
  buf_len = 516;
  pos = 0;
  //正常解析
  EXPECT_EQ(0, app.serialize(buf, buf_len, pos));
  TBSYS_LOG(INFO, "get serialize size1 =%d", app.get_serialize_size());
  //反序列化
  pos = 0;
  ObOcmAppInfo app_to;
  EXPECT_EQ(0, app_to.deserialize(buf, buf_len, pos));
  EXPECT_EQ(1, app_to.get_obi_count());
  EXPECT_EQ(0, strcmp(app_to.get_app_name(),app.get_app_name()));
  const ObOcmInstance *instance_to = app.get_obi(0);
  EXPECT_EQ(0, strcmp(instance_to->get_instance_name(), instance_name));
  EXPECT_EQ(50, instance_to->get_read_radio());
  EXPECT_EQ(0, strcmp("hostname", instance_to->get_master_rs().get_hostname()));

  //多加一个实例
  ObOcmInstance ocm_instance2;
  char instance_name2[16] = "instance2";
  EXPECT_EQ(0, ocm_instance2.init(instance_name2, serverExt));
  
  ocm_instance2.set_read_radio(50);
  EXPECT_EQ(0,app.add_obi(ocm_instance2));
  EXPECT_EQ(2, app.get_obi_count());

  //正常解析
  pos = 0;
  EXPECT_EQ(0, app.serialize(buf, buf_len, pos));
  TBSYS_LOG(INFO, "get serialize size2 =%d", app.get_serialize_size());
  //反序列化
  pos = 0;
  ObOcmAppInfo app_to_2;
  EXPECT_EQ(0, app_to_2.deserialize(buf, buf_len, pos));
  EXPECT_EQ(2, app_to_2.get_obi_count());
  EXPECT_EQ(0, strcmp(app_to_2.get_app_name(),app.get_app_name()));
  const ObOcmInstance *instance_to_2 = app.get_obi(0);
  EXPECT_EQ(0, strcmp(instance_to_2->get_instance_name(), instance_name));
  EXPECT_EQ(50, instance_to_2->get_read_radio());
  EXPECT_EQ(0, strcmp("hostname", instance_to_2->get_master_rs().get_hostname()));
  instance_to_2 = app.get_obi(1);
  EXPECT_EQ(0, strcmp(instance_to_2->get_instance_name(), instance_name2));
  EXPECT_EQ(50, instance_to_2->get_read_radio());
  EXPECT_EQ(0, strcmp("hostname", instance_to_2->get_master_rs().get_hostname()));
  delete [] buf;
}


int main(int argc, char **argv)
{
  ob_init_memory_pool();
  testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
