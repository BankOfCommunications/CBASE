#include <stdio.h>
#include <string.h>
#include "ob_ocm_meta_manager.h"
#include "gtest/gtest.h"

using namespace oceanbase::common;
using namespace oceanbase::clustermanager;

TEST(TestOcmMetaManager, add_ocm_info)
{
  ObOcmMetaManager ocm_meta_manager;
  char ocm_ip[128] = "10.232.36.35";
  ObServer server(ObServer::IPV4, ocm_ip, 2701);
  ObServerExt ocm_server;
  ocm_server.init((char*)"ocm_host_name", server);
  ocm_meta_manager.init_ocm_list("ocm_location", ocm_server);


  ObOcmMeta ocm_meta1;
  ocm_meta1.set_location("ocm_meta1_locat");
  ObOcmAppInfo app1;
  app1.init("app_name1");

  ObOcmInstance ocm_instance1, ocm_instance2, ocm_instance3;
  ObServerExt serverExt;
  char ip[128] = "10.232.36.35";
  ObServer server1(ObServer::IPV4, ip, 2700);
  serverExt.init((char*)"hostname", server1);
  char instance_name1[16] = "instance1";
  //ocm_instance1
  EXPECT_EQ(0, ocm_instance1.init(instance_name1, serverExt));
  EXPECT_EQ(0, app1.add_obi(ocm_instance1));

  //ocm_instance2
  char instance_name2[16] = "instance2";
  EXPECT_EQ(0, ocm_instance2.init(instance_name2, serverExt));
  EXPECT_EQ(0, app1.add_obi(ocm_instance2));

  //ocm_instance3
  char instance_name3[16] = "instance3";
  ObServerExt serverExt3;
  ObServer server3(ObServer::IPV4, ip, 2701);
  serverExt3.init((char*)"hostname3", server3);
  EXPECT_EQ(0, ocm_instance3.init(instance_name3, serverExt3));
  EXPECT_EQ(0, app1.add_obi(ocm_instance3));
  //only one app
  EXPECT_EQ(0,ocm_meta1.add_app(app1));
  //only one ocm
  EXPECT_EQ(0, ocm_meta_manager.add_ocm_info(ocm_meta1));
  OcmList::const_iterator it = ocm_meta_manager.get_ocm_list()->begin();
  it++;
  TBSYS_LOG(INFO, "%s", it->get_location());
  EXPECT_EQ(0, strcmp("ocm_meta1_locat", it->get_location()));
  EXPECT_EQ(1, it->get_app_count());
  const ObOcmAppInfo * tmp_app = NULL;
  const ObOcmInstance *tmp_instance = NULL;
  for(int i = 0; i < 1; i++)
  {
    tmp_app = it->get_app(i);
    TBSYS_LOG(INFO, "app %d name=%s", i, tmp_app->get_app_name());

    for(int j = 0; j < 3; j++)
    {
      tmp_instance = tmp_app->get_obi(j);
      TBSYS_LOG(INFO, "instance %d name = %s", j, tmp_instance->get_instance_name());
    }
  }
}
TEST(TestOcmMetaManager, serialize)
{
  ObOcmMetaManager ocm_meta_manager;
  char ocm_ip[128] = "10.232.36.35";                                                                                          
  ObServer server(ObServer::IPV4, ocm_ip, 2701);                                                                              
  ObServerExt ocm_server;                                                                                                     
  ocm_server.init((char*)"ocm_host_name", server);                                                                                   
  ocm_meta_manager.init_ocm_list("ocm_location", ocm_server);  
  //one ocm_meta
  ObOcmMeta ocm_meta1;
  ObOcmAppInfo app1;
  app1.init("app_name1");
  //one ocm_meta with one app which has three obi
  ObOcmInstance ocm_instance1, ocm_instance2, ocm_instance3;
  ObServerExt serverExt;
  char ip[128] = "10.232.36.35";
  ObServer server1(ObServer::IPV4, ip, 2700);
  serverExt.init((char*)"hostname", server1);
  char instance_name1[16] = "instance1";
  //ocm_instance1
  EXPECT_EQ(0, ocm_instance1.init(instance_name1, serverExt));
  EXPECT_EQ(0, app1.add_obi(ocm_instance1));

  //ocm_instance2
  char instance_name2[16] = "instance2";
  EXPECT_EQ(0, ocm_instance2.init(instance_name2, serverExt));
  EXPECT_EQ(0, app1.add_obi(ocm_instance2));

  //ocm_instance3
  char instance_name3[16] = "instance3";
  ObServerExt serverExt3;
  ObServer server3(ObServer::IPV4, ip, 2701);
  serverExt3.init((char*)"hostname3", server3);
  EXPECT_EQ(0, ocm_instance3.init(instance_name3, serverExt3));
  EXPECT_EQ(0, app1.add_obi(ocm_instance3));

  //only one app
  EXPECT_EQ(0,ocm_meta1.add_app(app1));

  //only one ocm
  EXPECT_EQ(0, ocm_meta_manager.add_ocm_info(ocm_meta1));
  EXPECT_EQ(2, ocm_meta_manager.get_ocm_count());
  char *buf = new char[2048];
  int64_t buf_len = 12;
  int64_t pos = 0;
  TBSYS_LOG(INFO, "only two ocm ,get_serialize_size =%ld", ocm_meta_manager.get_serialize_size());
  EXPECT_NE(0, ocm_meta_manager.serialize(buf, buf_len, pos));

  buf_len = 2048;
  pos = 0;
  EXPECT_EQ(0, ocm_meta_manager.serialize(buf, buf_len, pos));

  //add other ocm_meta
  ObOcmMeta ocm_meta21;

  ocm_meta21.set_location("ocm_location2");
  char ocm_ip_2[128] = "10.232.36.35";
  ObServer ocm_server21(ObServer::IPV4, ocm_ip_2, 2222);
  ocm_meta21.set_ocm_server((char*)"ocm_host_name", ocm_server21);
  // one ocm_meta with one app
  ObOcmAppInfo app21;
  app21.init("app_name1");
  //one ocm_meta with one app which has three obi
  ObOcmInstance ocm_instance21, ocm_instance22, ocm_instance23;
  ObServerExt serverExt2;
  ObServer server22(ObServer::IPV4, ip, 2700);
  serverExt2.init((char*)"hostname", server22);
  char instance_name21[16] = "instance21";
  //ocm_instance1

  EXPECT_EQ(0, ocm_instance21.init(instance_name21, serverExt2));
  EXPECT_EQ(0, app21.add_obi(ocm_instance21));

  //ocm_instance2
  char instance_name22[16] = "instance22";
  EXPECT_EQ(0, ocm_instance22.init(instance_name22, serverExt2));
  EXPECT_EQ(0, app21.add_obi(ocm_instance22));

  //ocm_instance3
  char instance_name23[16] = "instance23";
  ObServerExt serverExt23;
  ObServer server23(ObServer::IPV4, ip,2333);
  serverExt23.init((char*)"hostname23", server23);
  EXPECT_EQ(0, ocm_instance23.init(instance_name23, serverExt23));
  EXPECT_EQ(0, app21.add_obi(ocm_instance3));

  //only one app
  EXPECT_EQ(0,ocm_meta21.add_app(app21));

  //only one ocm
  EXPECT_EQ(0, ocm_meta_manager.add_ocm_info(ocm_meta21));
  buf_len = 512;
  pos = 0;
  TBSYS_LOG(INFO, "only two ocm ,get_serialize_size =%ld", ocm_meta_manager.get_serialize_size());
  EXPECT_NE(0, ocm_meta_manager.serialize(buf, buf_len, pos));
  buf_len = 2048;
  pos = 0;
  EXPECT_EQ(0, ocm_meta_manager.serialize(buf, buf_len, pos));
  delete [] buf;
}
TEST(TestOcmMetaManager, deserialize)
{
  ObOcmMetaManager ocm_meta_manager;
  char ocm_ip[128] = "10.232.36.35";                                                                                          
  ObServer server(ObServer::IPV4, ocm_ip, 2701);                                                                              
  ObServerExt ocm_server;                                                                                                     
  ocm_server.init((char*)"ocm_host_name", server);                                                                                   
  ocm_meta_manager.init_ocm_list("ocm_location", ocm_server); 

  //one ocm_meta
  ObOcmMeta ocm_meta1;
  ocm_meta1.set_location("ocm_location_1");
  // one ocm_meta with one app
  ObOcmAppInfo app1;
  app1.init("app_name1_1");
  //one ocm_meta with one app which has three obi
  ObOcmInstance ocm_instance1, ocm_instance2, ocm_instance3;
  ObServerExt serverExt;
  char ip[128] = "10.232.36.35";
  ObServer server1(ObServer::IPV4, ip, 2700);
  serverExt.init((char*)"hostname_1", server1);
  char instance_name1[16] = "instance1_1";
  //ocm_instance1
  EXPECT_EQ(0, ocm_instance1.init(instance_name1, serverExt));
  EXPECT_EQ(0, app1.add_obi(ocm_instance1));

  //ocm_instance2
  char instance_name2[16] = "instance2_1";
  EXPECT_EQ(0, ocm_instance2.init(instance_name2, serverExt));
  EXPECT_EQ(0, app1.add_obi(ocm_instance2));

  //ocm_instance3
  char instance_name3[16] = "instance3_1";
  ObServerExt serverExt3;
  ObServer server3(ObServer::IPV4, ip, 2701);
  serverExt3.init((char*)"hostname3", server3);
  EXPECT_EQ(0, ocm_instance3.init(instance_name3, serverExt3));
  EXPECT_EQ(0, app1.add_obi(ocm_instance3));

  //only one app
  EXPECT_EQ(0,ocm_meta1.add_app(app1));

  //only one ocm
  EXPECT_EQ(0, ocm_meta_manager.add_ocm_info(ocm_meta1));
  char *buf = new char[2048];
  memset(buf, '\0', 2048);
  int64_t buf_len = 2048;
  int64_t pos = 0;
  EXPECT_EQ(0, ocm_meta_manager.serialize(buf, buf_len, pos));

  //check deserialize stat
  ObOcmMetaManager meta_manager_to;
  pos = 0;
  EXPECT_EQ(0, meta_manager_to.deserialize(buf, buf_len, pos));
  EXPECT_EQ(2, meta_manager_to.get_ocm_count());
  const OcmList *list = meta_manager_to.get_ocm_list();
  OcmList::const_iterator it = list->begin();
  it++;
  TBSYS_LOG(INFO, "with one ocm, ocm location=%s", it->get_location());
  TBSYS_LOG(INFO,"app count=%ld", it->get_app_count());
  TBSYS_LOG(INFO, "ocm_server name=%s", it->get_ocm_server()->get_hostname());
  const ObOcmAppInfo* app = it->get_app(0);
  for(int i = 0; i < app->get_obi_count(); i++)
  {
    TBSYS_LOG(INFO, "%dth obi name=%s", i, app->get_obi(i)->get_instance_name());
  }

  //***********************
  //add other ocm_meta
  //***********************/

  ObOcmMeta ocm_meta21;

  ocm_meta21.set_location("ocm_location2");
  char ocm_ip_2[128] = "10.232.36.35";
  ObServer ocm_server21(ObServer::IPV4, ocm_ip_2, 2222);
  ocm_meta21.set_ocm_server((char*)"ocm_host_name_2", ocm_server21);
  // one ocm_meta with one app
  ObOcmAppInfo app21;
  app21.init("app_name1");
  //one ocm_meta with one app which has three obi
  ObOcmInstance ocm_instance21, ocm_instance22, ocm_instance23;
  ObServerExt serverExt2;
  ObServer server22(ObServer::IPV4, ip, 2700);
  serverExt2.init((char*)"hostname", server22);
  char instance_name21[16] = "instance21_2";
  //ocm_instance1

  EXPECT_EQ(0, ocm_instance21.init(instance_name21, serverExt2));
  EXPECT_EQ(0, app21.add_obi(ocm_instance21));

  //ocm_instance2
  char instance_name22[16] = "instance22_2";
  EXPECT_EQ(0, ocm_instance22.init(instance_name22, serverExt2));
  EXPECT_EQ(0, app21.add_obi(ocm_instance22));

  //ocm_instance3
  char instance_name23[16] = "instance23_2";
  ObServerExt serverExt23;
  ObServer server23(ObServer::IPV4, ip,2333);
  serverExt23.init((char*)"hostname23", server23);
  EXPECT_EQ(0, ocm_instance23.init(instance_name23, serverExt23));
  EXPECT_EQ(0, app21.add_obi(ocm_instance23));

  //only one app
  EXPECT_EQ(0,ocm_meta21.add_app(app21));

  //have two ocm
  EXPECT_EQ(0, ocm_meta_manager.add_ocm_info(ocm_meta21));
  memset(buf, '\0', 2048);
  buf_len = 2048;
  pos = 0;
  EXPECT_EQ(0, ocm_meta_manager.serialize(buf, buf_len, pos));

  ObOcmMetaManager meta_manager_to2;
  pos = 0;
  meta_manager_to2.deserialize(buf, buf_len, pos);
  EXPECT_EQ(3, meta_manager_to2.get_ocm_count());
  list = meta_manager_to2.get_ocm_list();
  it = list->begin();
  it++;
  TBSYS_LOG(INFO, "the first ocm_meta");
  TBSYS_LOG(INFO, "ocm location=%s", it->get_location());
  TBSYS_LOG(INFO,"app count=%ld", it->get_app_count());
  TBSYS_LOG(INFO, "ocm_server name=%s", it->get_ocm_server()->get_hostname());
  app = it->get_app(0);
  for(int i = 0; i < app->get_obi_count(); i++)
  {
    TBSYS_LOG(INFO, "%dth obi name=%s",i, app->get_obi(i)->get_instance_name());
  }
  it ++;
  TBSYS_LOG(INFO, "the second ocm_meta");
  TBSYS_LOG(INFO, "ocm location=%s", it->get_location());
  TBSYS_LOG(INFO,"app count=%ld", it->get_app_count());
  TBSYS_LOG(INFO, "ocm_server name=%s", it->get_ocm_server()->get_hostname());
  app = it->get_app(0);
  for(int i = 0; i < app->get_obi_count(); i++)
  {
    TBSYS_LOG(INFO, "%dth obi name=%s", i, app->get_obi(i)->get_instance_name());
  }

  delete [] buf;
}
//int get_app_master_rs(const char *app_name, ObServerExt &serv_info) const;
TEST(TestOcmMetaManager, get_app_master_rs)
{
  ObOcmMetaManager ocm_meta_manager;
  char ocm_ip[128] = "10.232.36.35";                                                                                          
  ObServer server(ObServer::IPV4, ocm_ip, 2701);                                                                              
  ObServerExt ocm_server;                                                                                                     
  ocm_server.init((char*)"ocm_host_name", server);                                                                                   
  ocm_meta_manager.init_ocm_list("ocm_location", ocm_server); 

  //one ocm_meta
  ObOcmMeta ocm_meta1;
  ocm_meta1.set_location("ocm_location");
  // one ocm_meta with one app
  ObOcmAppInfo app1;
  app1.init("app_name1");
  //one ocm_meta with one app which has three obi
  ObOcmInstance ocm_instance1, ocm_instance2, ocm_instance3;
  ObServerExt serverExt;
  char ip[128] = "10.232.36.35";
  ObServer server1(ObServer::IPV4, ip, 2700);
  serverExt.init((char*)"hostname", server1);
  char instance_name1[16] = "instance1";
  //ocm_instance1
  EXPECT_EQ(0, ocm_instance1.init(instance_name1, serverExt));
  EXPECT_EQ(0, app1.add_obi(ocm_instance1));

  //ocm_instance2
  char instance_name2[16] = "instance2";
  EXPECT_EQ(0, ocm_instance2.init(instance_name2, serverExt));
  EXPECT_EQ(0, app1.add_obi(ocm_instance2));

  //ocm_instance3
  char instance_name3[16] = "instance3";
  ObServerExt serverExt3;
  ObServer server3(ObServer::IPV4, ip, 2701);
  serverExt3.init((char*)"hostname3", server3);
  EXPECT_EQ(0, ocm_instance3.init(instance_name3, serverExt3));
  EXPECT_EQ(0, app1.add_obi(ocm_instance3));

  //only one app
  EXPECT_EQ(0,ocm_meta1.add_app(app1));
  //***************
  //add app
  //*********************
  ObOcmAppInfo app11;
  app11.init("app_name11");
  //one ocm_meta with one app which has three obi
  ObOcmInstance ocm_instance11;
  ObServerExt serverExt11;
  char ip11[128] = "10.232.36.35";
  ObServer server11(ObServer::IPV4, ip11, 8888);
  serverExt11.init((char*)"hostname11", server11);
  char instance_name11[16] = "instance11";
  //ocm_instance1
  EXPECT_EQ(0, ocm_instance11.init(instance_name11, serverExt11));
  EXPECT_EQ(0, app11.add_obi(ocm_instance11));
  app11.set_instance_role(instance_name11, MASTER);
  EXPECT_EQ(0, ocm_meta1.add_app(app11));

  //only one ocm
  EXPECT_EQ(0, ocm_meta_manager.add_ocm_info(ocm_meta1));

  //add other ocm_meta
  ObOcmMeta ocm_meta21;

  ocm_meta21.set_location("ocm_location2");
  char ocm_ip_2[128] = "10.232.36.35";
  ObServer ocm_server21(ObServer::IPV4, ocm_ip_2, 2222);
  ocm_meta21.set_ocm_server((char*)"ocm_host_name", ocm_server21);
  // one ocm_meta with one app
  ObOcmAppInfo app21;
  app21.init("app_name1");
  //one ocm_meta with one app which has three obi
  ObOcmInstance ocm_instance21, ocm_instance22, ocm_instance23;
  ObServerExt serverExt2;
  ObServer server22(ObServer::IPV4, ip, 2700);
  serverExt2.init((char*)"hostname", server22);
  char instance_name21[16] = "instance21";
  //ocm_instance1

  EXPECT_EQ(0, ocm_instance21.init(instance_name21, serverExt2));
  EXPECT_EQ(0, app21.add_obi(ocm_instance21));

  //ocm_instance2
  char instance_name22[16] = "instance22";
  EXPECT_EQ(0, ocm_instance22.init(instance_name22, serverExt2));
  EXPECT_EQ(0, app21.add_obi(ocm_instance22));

  //ocm_instance3
  char instance_name23[16] = "instance23";
  ObServerExt serverExt23;
  ObServer server23(ObServer::IPV4, ip,2333);
  serverExt23.init((char*)"hostname23", server23);
  EXPECT_EQ(0, ocm_instance23.init(instance_name23, serverExt23));
  EXPECT_EQ(0, app21.add_obi(ocm_instance23));

  //only one app
  EXPECT_EQ(0,ocm_meta21.add_app(app21));

  //only one ocm
  EXPECT_EQ(0, ocm_meta_manager.add_ocm_info(ocm_meta21));

  ObServerExt serv_info;
  EXPECT_NE(0, ocm_meta_manager.get_app_master_rs("app_name34", serv_info));
  EXPECT_EQ(0, ocm_meta_manager.get_app_master_rs("app_name11", serv_info));
  EXPECT_EQ(0, strcmp(serv_info.get_hostname(), "hostname11"));
  EXPECT_EQ(8888, serv_info.get_server().get_port());
}
//int get_instance_master_rs(const char *app_name, const char *instance_name, ObServerExt &serv_info) const;
TEST(TestOcmMetaManager, get_instance_master_rs)
{
  ObOcmMetaManager ocm_meta_manager;
  char ocm_ip[128] = "10.232.36.35";                                                                                          
  ObServer server(ObServer::IPV4, ocm_ip, 2701);                                                                              
  ObServerExt ocm_server;                                                                                                     
  ocm_server.init((char*)"ocm_host_name", server);                                                                                   
  ocm_meta_manager.init_ocm_list("ocm_location", ocm_server); 
  //one ocm_meta
  ObOcmMeta ocm_meta1;
  ocm_meta1.set_location("ocm_location");
  // one ocm_meta with one app
  ObOcmAppInfo app1;
  app1.init("app_name1");
  //one ocm_meta with one app which has three obi
  ObOcmInstance ocm_instance1, ocm_instance2, ocm_instance3;
  ObServerExt serverExt;
  char ip[128] = "10.232.36.35";
  ObServer server1(ObServer::IPV4, ip, 2700);
  serverExt.init((char*)"hostname", server1);
  char instance_name1[16] = "instance1";
  //ocm_instance1
  EXPECT_EQ(0, ocm_instance1.init(instance_name1, serverExt));
  EXPECT_EQ(0, app1.add_obi(ocm_instance1));

  //ocm_instance2
  char instance_name2[16] = "instance2";
  EXPECT_EQ(0, ocm_instance2.init(instance_name2, serverExt));
  EXPECT_EQ(0, app1.add_obi(ocm_instance2));

  //ocm_instance3
  char instance_name3[16] = "instance3";
  ObServerExt serverExt3;
  ObServer server3(ObServer::IPV4, ip, 2701);
  serverExt3.init((char*)"hostname3", server3);
  EXPECT_EQ(0, ocm_instance3.init(instance_name3, serverExt3));
  EXPECT_EQ(0, app1.add_obi(ocm_instance3));

  //only one app
  EXPECT_EQ(0,ocm_meta1.add_app(app1));
  //***************
  //add app
  //*********************
  ObOcmAppInfo app11;
  app11.init("app_name11");
  //one ocm_meta with one app which has three obi
  ObOcmInstance ocm_instance11;
  ObServerExt serverExt11;
  char ip11[128] = "10.232.36.35";
  ObServer server11(ObServer::IPV4, ip11, 8888);
  serverExt11.init((char*)"hostname11", server11);
  char instance_name11[16] = "instance11";
  //ocm_instance1
  EXPECT_EQ(0, ocm_instance11.init(instance_name11, serverExt11));
  EXPECT_EQ(0, app11.add_obi(ocm_instance11));
  EXPECT_EQ(0, ocm_meta1.add_app(app11));

  //only one ocm
  EXPECT_EQ(0, ocm_meta_manager.add_ocm_info(ocm_meta1));

  //add other ocm_meta
  ObOcmMeta ocm_meta21;

  ocm_meta21.set_location("ocm_location2");
  char ocm_ip_2[128] = "10.232.36.35";
  ObServer ocm_server21(ObServer::IPV4, ocm_ip_2, 2222);
  ocm_meta21.set_ocm_server((char*)"ocm_host_name", ocm_server21);
  // one ocm_meta with one app
  ObOcmAppInfo app21;
  app21.init("app_name21");
  //one ocm_meta with one app which has three obi
  ObOcmInstance ocm_instance21, ocm_instance22, ocm_instance23;
  ObServerExt serverExt2;
  ObServer server22(ObServer::IPV4, ip, 2700);
  serverExt2.init((char*)"hostname", server22);
  char instance_name21[16] = "instance21";
  //ocm_instance1

  EXPECT_EQ(0, ocm_instance21.init(instance_name21, serverExt2));
  EXPECT_EQ(0, app21.add_obi(ocm_instance21));

  //ocm_instance2
  char instance_name22[16] = "instance22";
  EXPECT_EQ(0, ocm_instance22.init(instance_name22, serverExt2));
  EXPECT_EQ(0, app21.add_obi(ocm_instance22));

  //ocm_instance3
  char instance_name23[16] = "instance23";
  ObServerExt serverExt23;
  ObServer server23(ObServer::IPV4, ip,2333);
  serverExt23.init((char*)"hostname23", server23);
  EXPECT_EQ(0, ocm_instance23.init(instance_name23, serverExt23));
  EXPECT_EQ(0, app21.add_obi(ocm_instance23));

  //only one app
  EXPECT_EQ(0,ocm_meta21.add_app(app21));

  EXPECT_EQ(0, ocm_meta_manager.add_ocm_info(ocm_meta21));

  ObServerExt serv_info;
  EXPECT_NE(0, ocm_meta_manager.get_instance_master_rs("app_name34", "instance22", serv_info));
  EXPECT_NE(0, ocm_meta_manager.get_instance_master_rs("app_name11", "instance222", serv_info));
  EXPECT_EQ(0, ocm_meta_manager.get_instance_master_rs("app_name11", "instance11", serv_info));
  EXPECT_EQ(0, strcmp(serv_info.get_hostname(), "hostname11"));
  EXPECT_EQ(8888, serv_info.get_server().get_port());

  //test get_instance_master_rs_list
  ObServList serv_list;
  EXPECT_EQ(0, ocm_meta_manager.get_instance_master_rs_list("app_name34", serv_list));
  EXPECT_EQ(0, ocm_meta_manager.get_instance_master_rs_list("app_name1", serv_list));

  EXPECT_EQ(3, serv_list.size());
  ObServList::iterator iter;
  int64_t i = 0;
  for(iter = serv_list.begin(); iter != serv_list.end(); iter++)
  {
    TBSYS_LOG(INFO, "%ldth rootserver", i);
    TBSYS_LOG(INFO, "hostname=%s", (*iter)->get_hostname());
    TBSYS_LOG(INFO, "port = %d", (*iter)->get_server().get_port());
    i++;
  }
  EXPECT_EQ(0, ocm_meta_manager.get_instance_master_rs_list("app_name21", serv_list));
  i = 0;
  for(iter = serv_list.begin(); iter != serv_list.end(); iter++)
  {
    TBSYS_LOG(INFO, "%ldth rootserver", i);
    TBSYS_LOG(INFO, "hostname=%s", (*iter)->get_hostname());
    TBSYS_LOG(INFO, "port = %d", (*iter)->get_server().get_port());
    i++;
  }
}

// int get_instance_master_rs_list(const char *app_name, ObServList &serv_list) const;

int main(int argc, char **argv)
{
  ob_init_memory_pool();
  testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
