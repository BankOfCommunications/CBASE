#include "ob_ocm_meta.h"
#include "gtest/gtest.h"

using namespace oceanbase::common;
using namespace oceanbase::clustermanager;

namespace oceanbase
{
  namespace tests
  {
    namespace clustermanager
    {
      class TestObOcmMeta : public ObOcmMeta
      {
        public:
          void set_app_count(int64_t count)
          {
            return ObOcmMeta::set_app_count(count);
          }

          int set_location(const char * location)
          {
            return ObOcmMeta::set_location(location);
          }

          int set_ocm_server(char *host_name, oceanbase::common::ObServer server)
          {
            return ObOcmMeta::set_ocm_server(host_name, server);
          }
          int add_app(ObOcmAppInfo &app_info)
          {
            return ObOcmMeta::add_app(app_info);
          }
      };

      TEST(TestOcmMeta, get_app)
      {
        TestObOcmMeta ocm_meta;
        char ocm_ip[128] = "10.232.36.35";
        ObServer ocm_server(ObServer::IPV4, ocm_ip, 2700);
        ocm_meta.set_ocm_server((char*)"ocm_host_name", ocm_server);

        ObOcmAppInfo app1;
        app1.init("app_name1");

        ObOcmInstance ocm_instance1, ocm_instance2, ocm_instance3;
        ObServerExt serverExt;
        char ip[128] = "10.232.36.35";
        ObServer server(ObServer::IPV4, ip, 2700);
        serverExt.init((char*)"hostname", server);
        char instance_name1[16] = "instance1";
        //ocm_instance1
        EXPECT_EQ(0, ocm_instance1.init(instance_name1, serverExt));
        EXPECT_EQ(0, app1.add_obi(ocm_instance1));

        //only one app
        EXPECT_EQ(0,ocm_meta.add_app(app1));

        ObOcmAppInfo app2;
        app2.init("app_name2");

        ObOcmInstance ocm_instance21, ocm_instance22, ocm_instance23;
        ObServerExt serverExt21;
        serverExt21.init((char*)"hostname21", server);
        char instance_name21[16] = "instance21";
        //ocm_instance21
        EXPECT_EQ(0, ocm_instance21.init(instance_name21, serverExt21));
        EXPECT_EQ(0, app2.add_obi(ocm_instance21));

        EXPECT_EQ(0, ocm_meta.add_app(app2));

        ObOcmAppInfo *app = NULL;
        ocm_meta.get_app((char*)"app_name2", app);

        EXPECT_EQ(0, strcmp("app_name2", app->get_app_name()));
        EXPECT_EQ(1, app->get_obi_count());
        const ObOcmInstance *instance = app->get_obi(0);
        EXPECT_EQ(0, strcmp(instance_name21, instance->get_instance_name()));
      }

      TEST(TestOcmMeta, add_app)
      {
        TestObOcmMeta ocm_meta;
        char ocm_ip[128] = "10.232.36.35";
        ObServer ocm_server(ObServer::IPV4, ocm_ip, 2700);
        ocm_meta.set_ocm_server((char*)"ocm_host_name", ocm_server);

        ObOcmAppInfo app1;
        app1.init("app_name1");

        ObOcmInstance ocm_instance1, ocm_instance2, ocm_instance3;
        ObServerExt serverExt;
        char ip[128] = "10.232.36.35";
        ObServer server(ObServer::IPV4, ip, 2700);
        serverExt.init((char*)"hostname", server);
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
        EXPECT_EQ(0,ocm_meta.add_app(app1));

        ObOcmAppInfo app2;
        app2.init("app_name2");

        ObOcmInstance ocm_instance21, ocm_instance22, ocm_instance23;
        ObServerExt serverExt21;
        serverExt21.init((char*)"hostname21", server);
        char instance_name21[16] = "instance21";
        //ocm_instance21
        EXPECT_EQ(0, ocm_instance21.init(instance_name21, serverExt21));
        EXPECT_EQ(0, app2.add_obi(ocm_instance21));

        //ocm_instance22
        char instance_name22[16] = "instance22";
        EXPECT_EQ(0, ocm_instance22.init(instance_name22, serverExt));
        EXPECT_EQ(0, app2.add_obi(ocm_instance22));

        //ocm_instance23
        char instance_name23[16] = "instance23";
        ObServer server23(ObServer::IPV4, ip, 2701);
        EXPECT_EQ(0, ocm_instance23.init(instance_name23, serverExt3));
        EXPECT_EQ(0, app2.add_obi(ocm_instance23));

        //add other app
        EXPECT_EQ(0,ocm_meta.add_app(app2));

        //not allow to add the same app
        EXPECT_NE(0, ocm_meta.add_app(app1));

        //test
        EXPECT_EQ(2, ocm_meta.get_app_count());
        for(int i = 0; i < ocm_meta.get_app_count(); i++)
        {
          TBSYS_LOG(INFO, "app_name=%s", ocm_meta.get_app(i)->get_app_name());
        }
      }

      TEST(TestOcmMeta, serialize)
      {
        TestObOcmMeta ocm_meta;
        char ocm_ip[128] = "10.232.36.35";
        ObServer ocm_server(ObServer::IPV4, ocm_ip, 2701);
        ocm_meta.set_ocm_server((char*)"ocm_host_name", ocm_server);

        ObOcmAppInfo app1;
        app1.init("app_name1");

        ObOcmInstance ocm_instance1, ocm_instance2, ocm_instance3;
        ObServerExt serverExt;
        char ip[128] = "10.232.36.35";
        ObServer server(ObServer::IPV4, ip, 2700);
        serverExt.init((char*)"hostname", server);
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
        EXPECT_EQ(0,ocm_meta.add_app(app1));

        ObOcmAppInfo app2;
        app2.init("app_name2");

        ObOcmInstance ocm_instance21, ocm_instance22, ocm_instance23;
        ObServerExt serverExt21;
        serverExt21.init((char*)"hostname21", server);
        char instance_name21[16] = "instance21";
        //ocm_instance21
        EXPECT_EQ(0, ocm_instance21.init(instance_name21, serverExt21));
        EXPECT_EQ(0, app2.add_obi(ocm_instance21));

        //ocm_instance22
        char instance_name22[16] = "instance22";
        EXPECT_EQ(0, ocm_instance22.init(instance_name22, serverExt));
        EXPECT_EQ(0, app2.add_obi(ocm_instance22));

        //ocm_instance23
        char instance_name23[16] = "instance23";
        ObServer server23(ObServer::IPV4, ip, 2701);
        EXPECT_EQ(0, ocm_instance23.init(instance_name23, serverExt3));
        EXPECT_EQ(0, app2.add_obi(ocm_instance23));

        //add other app
        EXPECT_EQ(0,ocm_meta.add_app(app2));

        TBSYS_LOG(INFO,"ocm_mate get serialize size=%ld", ocm_meta.get_serialize_size());
        char *buf = new char[1024];
        int64_t buf_len = 12;
        int64_t pos = 0;
        EXPECT_NE(0, ocm_meta.serialize(buf, buf_len, pos));

        buf_len = 1024;
        pos = 0;
        EXPECT_EQ(0, ocm_meta.serialize(buf, buf_len, pos));

        delete [] buf;

      }

      TEST(TestOcmMeta, deserialize)
      {
        TestObOcmMeta ocm_meta;
        ocm_meta.set_location("ocm_location");
        char ocm_ip[128] = "10.232.36.35";
        ObServer ocm_server(ObServer::IPV4, ocm_ip, 2701);
        ocm_meta.set_ocm_server((char*)"ocm_host_name", ocm_server);

        ObOcmAppInfo app1;
        app1.init("app_name1");

        ObOcmInstance ocm_instance1, ocm_instance2, ocm_instance3;
        ObServerExt serverExt;
        char ip[128] = "10.232.36.35";
        ObServer server(ObServer::IPV4, ip, 2700);
        serverExt.init((char*)"hostname", server);
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
        EXPECT_EQ(0,ocm_meta.add_app(app1));

        ObOcmAppInfo app2;
        app2.init("app_name2");

        ObOcmInstance ocm_instance21, ocm_instance22, ocm_instance23;
        ObServerExt serverExt21;
        serverExt21.init((char*)"hostname21", server);
        char instance_name21[16] = "instance21";
        //ocm_instance21
        EXPECT_EQ(0, ocm_instance21.init(instance_name21, serverExt21));
        EXPECT_EQ(0, app2.add_obi(ocm_instance21));

        //ocm_instance22
        char instance_name22[16] = "instance22";
        EXPECT_EQ(0, ocm_instance22.init(instance_name22, serverExt));
        EXPECT_EQ(0, app2.add_obi(ocm_instance22));

        //ocm_instance23
        char instance_name23[16] = "instance23";
        ObServer server23(ObServer::IPV4, ip, 2701);
        EXPECT_EQ(0, ocm_instance23.init(instance_name23, serverExt3));
        EXPECT_EQ(0, app2.add_obi(ocm_instance23));

        //add other app
        EXPECT_EQ(0,ocm_meta.add_app(app2));

        char *buf = new char[1024];
        int64_t buf_len = 12;
        int64_t pos = 0;
        EXPECT_NE(0, ocm_meta.serialize(buf, buf_len, pos));

        buf_len = 1024;
        pos = 0;
        EXPECT_EQ(0, ocm_meta.serialize(buf, buf_len, pos));

        pos = 0;
        TestObOcmMeta ocm_meta_to;
        EXPECT_EQ(0, ocm_meta_to.deserialize(buf, buf_len, pos));
        EXPECT_EQ(2, ocm_meta_to.get_app_count());
        EXPECT_EQ(0, strcmp("ocm_location", ocm_meta_to.get_location()));
        ObServerExt *ocm_server_to = ocm_meta_to.get_ocm_server();
        EXPECT_EQ(0, strcmp("ocm_host_name", ocm_server_to->get_hostname()));
        const ObOcmAppInfo *app_info = NULL;
        const ObOcmInstance *instance = NULL;
        for(int i = 0; i < ocm_meta_to.get_app_count(); i++)
        {
          TBSYS_LOG(INFO, "in the %dth app", i);
          app_info = ocm_meta_to.get_app(i);
          TBSYS_LOG(INFO, "app_name=%s", app_info->get_app_name());
          TBSYS_LOG(INFO, "obi count=%ld", app_info->get_obi_count());
          for(int j = 0; j < app_info->get_obi_count(); j++)
          {
            TBSYS_LOG(INFO, "in the %d obi", j);
            instance = app_info->get_obi(j);
            TBSYS_LOG(INFO, "instance name=%s", instance->get_instance_name());
            TBSYS_LOG(INFO, "role=%d",instance->get_role());
          }
        }

        delete [] buf;
      }

      TEST(TestOcmMeta, get_serialize_size)
      {
        TestObOcmMeta ocm_meta;
        ocm_meta.set_location("ocm_location");
        char ocm_ip[128] = "10.232.36.35";
        ObServer ocm_server(ObServer::IPV4, ocm_ip, 2701);
        ocm_meta.set_ocm_server((char*)"ocm_host_name", ocm_server);
        TBSYS_LOG(INFO, "with no app, size=%ld", ocm_meta.get_serialize_size());
        ObOcmAppInfo app1;
        app1.init("app_name1");

        ObOcmInstance ocm_instance1, ocm_instance2, ocm_instance3;
        ObServerExt serverExt;
        char ip[128] = "10.232.36.35";
        ObServer server(ObServer::IPV4, ip, 2700);
        serverExt.init((char*)"hostname", server);
        char instance_name1[16] = "instance1";
        //ocm_instance1
        EXPECT_EQ(0, ocm_instance1.init(instance_name1, serverExt));
        EXPECT_EQ(0, app1.add_obi(ocm_instance1));
        TBSYS_LOG(INFO, "app1 has one obi, size=%d", app1.get_serialize_size());
        //ocm_instance2
        char instance_name2[16] = "instance2";
        EXPECT_EQ(0, ocm_instance2.init(instance_name2, serverExt));
        EXPECT_EQ(0, app1.add_obi(ocm_instance2));
        TBSYS_LOG(INFO,"app1 has two oib, size=%d", app1.get_serialize_size());
        //ocm_instance3
        char instance_name3[16] = "instance3";
        ObServerExt serverExt3;
        ObServer server3(ObServer::IPV4, ip, 2701);
        serverExt3.init((char*)"hostname3", server3);
        EXPECT_EQ(0, ocm_instance3.init(instance_name3, serverExt3));
        EXPECT_EQ(0, app1.add_obi(ocm_instance3));
        TBSYS_LOG(INFO,"app1 has three oib, size=%d", app1.get_serialize_size());
        //only one app
        EXPECT_EQ(0,ocm_meta.add_app(app1));
        TBSYS_LOG(INFO, "ocm_meta with one app, size=%ld", ocm_meta.get_serialize_size());

        ObOcmAppInfo app2;
        app2.init("app_name2");

        ObOcmInstance ocm_instance21, ocm_instance22, ocm_instance23;
        ObServerExt serverExt21;
        serverExt21.init((char*)"hostname21", server);
        char instance_name21[16] = "instance21";
        //ocm_instance21
        EXPECT_EQ(0, ocm_instance21.init(instance_name21, serverExt21));
        EXPECT_EQ(0, app2.add_obi(ocm_instance21));
        TBSYS_LOG(INFO,"app2 has one oib, size=%d", app2.get_serialize_size());
        //ocm_instance22
        char instance_name22[16] = "instance22";
        EXPECT_EQ(0, ocm_instance22.init(instance_name22, serverExt));
        EXPECT_EQ(0, app2.add_obi(ocm_instance22));
        TBSYS_LOG(INFO,"app2 has 2 oib, size=%d", app2.get_serialize_size());
        //ocm_instance23
        char instance_name23[16] = "instance23";
        ObServer server23(ObServer::IPV4, ip, 2701);
        EXPECT_EQ(0, ocm_instance23.init(instance_name23, serverExt3));
        EXPECT_EQ(0, app2.add_obi(ocm_instance23));
        TBSYS_LOG(INFO,"app2 has 3 oib, size=%d", app2.get_serialize_size());
        //add other app
        EXPECT_EQ(0,ocm_meta.add_app(app2));
        TBSYS_LOG(INFO, "ocm_meta with 2 app, size=%ld", ocm_meta.get_serialize_size());
      }

      //int get_app_master_rs(const char *app_name, ObServerExt &serv_info) const;
      TEST(TestOcmMeta, get_app_master_rs)
      {
        TestObOcmMeta ocm_meta;
        ocm_meta.set_location("ocm_location");
        char ocm_ip[128] = "10.232.36.35";
        ObServer ocm_server(ObServer::IPV4, ocm_ip, 2701);
        ocm_meta.set_ocm_server((char*)"ocm_host_name", ocm_server);

        ObOcmAppInfo app1;
        app1.init("app_name1");

        ObOcmInstance ocm_instance1, ocm_instance2, ocm_instance3;
        ObServerExt serverExt;
        char ip[128] = "10.232.36.35";
        ObServer server(ObServer::IPV4, ip, 2700);
        serverExt.init((char*)"hostname", server);
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
        EXPECT_EQ(0,ocm_meta.add_app(app1));

        ObOcmAppInfo app2;
        app2.init("app_name2");

        ObOcmInstance ocm_instance21, ocm_instance22, ocm_instance23;
        ObServerExt serverExt21;
        serverExt21.init((char*)"hostname21", server);
        char instance_name21[16] = "instance21";
        //ocm_instance21
        EXPECT_EQ(0, ocm_instance21.init(instance_name21, serverExt21));
        EXPECT_EQ(0, app2.add_obi(ocm_instance21));

        //ocm_instance22
        char instance_name22[16] = "instance22";
        EXPECT_EQ(0, ocm_instance22.init(instance_name22, serverExt));
        EXPECT_EQ(0, app2.add_obi(ocm_instance22));

        //ocm_instance23
        char instance_name23[16] = "instance23";
        ObServer server23(ObServer::IPV4, ip, 2651);
        ObServerExt serverExt23;
        serverExt23.init((char*)"hostname23", server23);
        EXPECT_EQ(0, ocm_instance23.init(instance_name23, serverExt23));
        EXPECT_EQ(0, app2.add_obi(ocm_instance23));
        app2.set_instance_role(instance_name23, MASTER);

        //add other app
        EXPECT_EQ(0, ocm_meta.add_app(app2));

        ObServerExt serv_info;
        EXPECT_NE(0, ocm_meta.get_app_master_rs("app_name34", serv_info));
        EXPECT_EQ(0, ocm_meta.get_app_master_rs("app_name2", serv_info));
        EXPECT_EQ(0, strcmp(serv_info.get_hostname(), "hostname23"));
        EXPECT_EQ(2651, serv_info.get_server().get_port());
      }

      //  int get_instance_master_rs(const char *app_name, const char *instance_name, ObServerExt &serv_info) const;
      TEST(TestOcmMeta, get_instance_master_rs)
      {
        TestObOcmMeta ocm_meta;
        ocm_meta.set_location("ocm_location");
        char ocm_ip[128] = "10.232.36.35";
        ObServer ocm_server(ObServer::IPV4, ocm_ip, 2701);
        ocm_meta.set_ocm_server((char*)"ocm_host_name", ocm_server);

        ObOcmAppInfo app1;
        app1.init("app_name1");

        ObOcmInstance ocm_instance1, ocm_instance2, ocm_instance3;
        ObServerExt serverExt;
        char ip[128] = "10.232.36.35";
        ObServer server(ObServer::IPV4, ip, 2700);
        serverExt.init((char*)"hostname", server);
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
        EXPECT_EQ(0,ocm_meta.add_app(app1));

        ObOcmAppInfo app2;
        app2.init("app_name2");

        ObOcmInstance ocm_instance21, ocm_instance22, ocm_instance23;
        ObServerExt serverExt21;
        serverExt21.init((char*)"hostname21", server);
        char instance_name21[16] = "instance21";
        //ocm_instance21
        EXPECT_EQ(0, ocm_instance21.init(instance_name21, serverExt21));
        EXPECT_EQ(0, app2.add_obi(ocm_instance21));

        //ocm_instance22
        char instance_name22[16] = "instance22";
        EXPECT_EQ(0, ocm_instance22.init(instance_name22, serverExt));
        EXPECT_EQ(0, app2.add_obi(ocm_instance22));

        //ocm_instance23
        char instance_name23[16] = "instance23";
        ObServer server23(ObServer::IPV4, ip, 2651);
        ObServerExt serverExt23;
        serverExt23.init((char*)"hostname23", server23);
        EXPECT_EQ(0, ocm_instance23.init(instance_name23, serverExt23));
        EXPECT_EQ(0, app2.add_obi(ocm_instance23));

        //add other app
        EXPECT_EQ(0, ocm_meta.add_app(app2));

        ObServerExt serv_info;
        EXPECT_NE(0, ocm_meta.get_instance_master_rs("app_name34", "instance22", serv_info));
        EXPECT_NE(0, ocm_meta.get_instance_master_rs("app_name2", "instance222", serv_info));
        EXPECT_EQ(0, ocm_meta.get_instance_master_rs("app_name2", "instance23", serv_info));
        EXPECT_EQ(0, strcmp(serv_info.get_hostname(), "hostname23"));
        EXPECT_EQ(2651, serv_info.get_server().get_port());
      }

      //int get_instance_master_rs_list(const char *app_name, ObServList &serv_list) const;
      TEST(TestOcmMeta, get_instance_master_rs_list)
      {
        TestObOcmMeta ocm_meta;
        ocm_meta.set_location("ocm_location");
        char ocm_ip[128] = "10.232.36.35";
        ObServer ocm_server(ObServer::IPV4, ocm_ip, 2701);
        ocm_meta.set_ocm_server((char*)"ocm_host_name", ocm_server);

        ObOcmAppInfo app1;
        app1.init("app_name1");

        ObOcmInstance ocm_instance1, ocm_instance2, ocm_instance3;
        ObServerExt serverExt;
        char ip[128] = "10.232.36.35";
        ObServer server(ObServer::IPV4, ip, 2700);
        serverExt.init((char*)"hostname", server);
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
        EXPECT_EQ(0,ocm_meta.add_app(app1));

        ObOcmAppInfo app2;
        app2.init("app_name2");

        ObOcmInstance ocm_instance21, ocm_instance22, ocm_instance23;
        ObServerExt serverExt21;
        serverExt21.init((char*)"hostname21", server);
        char instance_name21[16] = "instance21";
        //ocm_instance21
        EXPECT_EQ(0, ocm_instance21.init(instance_name21, serverExt21));
        EXPECT_EQ(0, app2.add_obi(ocm_instance21));

        //ocm_instance22
        char instance_name22[16] = "instance22";
        EXPECT_EQ(0, ocm_instance22.init(instance_name22, serverExt));
        EXPECT_EQ(0, app2.add_obi(ocm_instance22));

        //ocm_instance23
        char instance_name23[16] = "instance23";
        ObServer server23(ObServer::IPV4, ip, 2651);
        ObServerExt serverExt23;
        serverExt23.init((char*)"hostname23", server23);
        EXPECT_EQ(0, ocm_instance23.init(instance_name23, serverExt23));
        EXPECT_EQ(0, app2.add_obi(ocm_instance23));

        //add other app
        EXPECT_EQ(0, ocm_meta.add_app(app2));
        ObServList serv_list;
        EXPECT_EQ(0, ocm_meta.get_instance_master_rs_list("app_name3", serv_list));
        EXPECT_EQ(0, serv_list.size());
        EXPECT_EQ(0, ocm_meta.get_instance_master_rs_list("app_name2", serv_list));

        EXPECT_EQ(3, serv_list.size());
        ObServList::iterator iter;
        int64_t i = 0;
        for(iter = serv_list.begin(); iter != serv_list.end(); iter++)
        {
          TBSYS_LOG(INFO, "%ldth rootserver", i);
          TBSYS_LOG(INFO, "hostname=%s", (*iter)->get_hostname());
          TBSYS_LOG(INFO, "port = %d", (*iter)->get_server().get_port());
        }
      }
    }
  }
}
int main(int argc, char **argv)
{
  ob_init_memory_pool();
  testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}

