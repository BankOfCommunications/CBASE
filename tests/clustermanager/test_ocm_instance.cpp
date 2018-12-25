#include "clustermanager/ob_ocm_instance.h"
#include "gtest/gtest.h"

using namespace oceanbase::common;
using namespace oceanbase::clustermanager;
namespace oceanbase
{
  namespace tests
  {
    namespace clustermanager
    {
      class TestObOcmInstance : public ObOcmInstance
      {
        public:
          void set_reserve2(int64_t value)
          {
            return ObOcmInstance::set_reserve2(value);
          }
          void set_role(Status state)
          {
            return ObOcmInstance::set_role(state);
          }
          void set_reserve1(int64_t value)
          {
            return ObOcmInstance::set_reserve1(value);
          }
      };
      TEST(TestOcmInstance, init)
      {
        ObServerExt serverExt;
        char ip[128] = "10.232.36.35";
        ObServer server(ObServer::IPV4, ip, 2700);
        serverExt.init((char*)"hostname", server);
        char instance_name[16] = "instance1";
        TestObOcmInstance ocm_instance;
        EXPECT_EQ(0, ocm_instance.init(instance_name, serverExt)); 

        EXPECT_EQ(0, strcmp("instance1", ocm_instance.get_instance_name()));
      }

      TEST(TestOcmInstance, get_master_rs)
      {
        TestObOcmInstance ocm_instance;
        ObServerExt serverExt;
        char ip[128] = "10.232.36.35";
        ObServer server(ObServer::IPV4, ip, 2700);
        serverExt.init((char*)"hostname", server);
        char instance_name[16] = "instance1";
        EXPECT_EQ(0, ocm_instance.init(instance_name, serverExt));
        ObServerExt serverExt2 = ocm_instance.get_master_rs();
        EXPECT_EQ(0, strcmp("hostname", serverExt2.get_hostname()));
      }

      TEST(TestOcmInstance, serialize)
      {
        TestObOcmInstance ocm_instance;
        ObServerExt serverExt;
        char ip[128] = "10.232.36.35";
        ObServer server(ObServer::IPV4, ip, 2700);
        serverExt.init((char*)"hostname", server);
        char instance_name[16] = "instance1";
        EXPECT_EQ(0, ocm_instance.init(instance_name, serverExt));
        char *buf = new char[256];
        //buf 长度不够
        int64_t buf_len = 10;
        int64_t pos = 0;
        EXPECT_NE(0, ocm_instance.serialize(buf, buf_len, pos));

        //正常解析
        buf_len = 256;
        pos = 0;
        EXPECT_EQ(0, ocm_instance.serialize(buf, buf_len, pos));
        int64_t size = ocm_instance.get_serialize_size();
        EXPECT_EQ(size, pos);

        delete [] buf;
      }

      TEST(TestOcmInstance, deserialize)
      {
        TestObOcmInstance ocm_instance;
        ObServerExt serverExt;
        char ip[128] = "10.232.36.35";
        ObServer server(ObServer::IPV4, ip, 2700);
        serverExt.init((char*)"hostname", server);
        char instance_name[16] = "instance1";
        EXPECT_EQ(0, ocm_instance.init(instance_name, serverExt));
        char *buf = new char[256];
        int64_t buf_len = 256;
        ocm_instance.set_role(MASTER);
        ocm_instance.set_read_radio(50);
        int64_t pos = 0;
        EXPECT_EQ(0, ocm_instance.serialize(buf, buf_len, pos));

        //pos位置不对时进行解析
        TestObOcmInstance ocm_instance_to;
        //会引起core
        //EXPECT_EQ(0,ocm_instance_to.deserialize(buf, buf_len, pos));

        //正常解析
        pos = 0;
        ocm_instance_to.deserialize(buf, buf_len, pos);
        EXPECT_EQ(MASTER, ocm_instance_to.get_role());
        EXPECT_EQ(50, ocm_instance_to.get_read_radio());
        ObServerExt serverExt_to = ocm_instance_to.get_master_rs();
        EXPECT_EQ(0, strcmp("hostname", serverExt_to.get_hostname()));
        int32_t ip_int = server.convert_ipv4_addr(ip);
        EXPECT_EQ(ip_int, serverExt_to.get_server().get_ipv4());
        EXPECT_EQ(2700, serverExt_to.get_server().get_port());
        delete [] buf;
      }

      TEST(TestOcmInstance, get_serialzie_size)
      {
        TestObOcmInstance ocm_instance;
        ObServerExt serverExt;
        char ip[128] = "10.232.36.35";
        ObServer server(ObServer::IPV4, ip, 2700);
        serverExt.init((char*)"hostname", server);
        TBSYS_LOG(INFO, "serverExt.get_serialize_size()=%ld",serverExt.get_serialize_size());
        char instance_name[16] = "instance1";
        EXPECT_EQ(0, ocm_instance.init(instance_name, serverExt));
        char *buf = new char[256];
        int64_t buf_len = 256;
        int64_t pos = 0;
        EXPECT_EQ(0, ocm_instance.serialize(buf, buf_len, pos));

        TBSYS_LOG(INFO, "ocm_instance get serialize size=%ld", ocm_instance.get_serialize_size());
      }

      TEST(TestOcmInstance, get_app_master_rs)
      {
        TestObOcmInstance ocm_instance;
        ObServerExt serverExt;
        char ip[128] = "10.232.36.35";
        ObServer server(ObServer::IPV4, ip, 2700);
        serverExt.init((char*)"hostname", server);
        TBSYS_LOG(INFO, "serverExt.get_serialize_size()=%ld",serverExt.get_serialize_size());
        char instance_name[16] = "instance1";
        EXPECT_EQ(0, ocm_instance.init(instance_name, serverExt));

        //只有当obi的角色为master的时候，才返回0，
        ObServerExt serv_info;
        EXPECT_EQ(OB_ENTRY_NOT_EXIST, ocm_instance.get_app_master_rs(serv_info));

        ObServerExt serverExtMaster;
        serverExtMaster.init((char*)"hostname", server);
        TestObOcmInstance ocm_master_instance;
        ocm_master_instance.init(instance_name, serverExtMaster);
        ocm_master_instance.set_role(MASTER);
        ObServerExt master_to;
        EXPECT_EQ(0, ocm_master_instance.get_app_master_rs(master_to));
      }

      TEST(TestOcmInstance, get_instance_master_rs)
      {
        TestObOcmInstance ocm_instance;
        ObServerExt serverExt;
        char ip[128] = "10.232.36.35";
        ObServer server(ObServer::IPV4, ip, 2700);
        serverExt.init((char*)"hostname", server);
        TBSYS_LOG(INFO, "serverExt.get_serialize_size()=%ld",serverExt.get_serialize_size());
        char instance_name[16] = "instance1";
        EXPECT_EQ(0, ocm_instance.init(instance_name, serverExt));

        ObServerExt serv_info;
        //invalid argument
        EXPECT_EQ(OB_INVALID_ARGUMENT, ocm_instance.get_instance_master_rs("instance2", serv_info));

        //valid argument
        EXPECT_EQ(OB_SUCCESS, ocm_instance.get_instance_master_rs("instance1", serv_info));
        EXPECT_EQ(0, strcmp("hostname", serv_info.get_hostname()));
        int32_t ip_int = server.convert_ipv4_addr(ip);
        EXPECT_EQ(ip_int, serv_info.get_server().get_ipv4());
        EXPECT_EQ(2700, serv_info.get_server().get_port());
      }

      TEST(TestOcmInstance, get_rs_list)
      {
        TestObOcmInstance ocm_instance;
        ObServerExt serverExt;
        char ip[128] = "10.232.36.35";
        ObServer server(ObServer::IPV4, ip, 2700);
        serverExt.init((char*)"hostname", server);
        TBSYS_LOG(INFO, "serverExt.get_serialize_size()=%ld",serverExt.get_serialize_size());
        char instance_name[16] = "instance1";
        EXPECT_EQ(0, ocm_instance.init(instance_name, serverExt));

        ObServList rs_list;
        EXPECT_EQ(0, ocm_instance.get_rs_list(rs_list));
        ObServList::iterator iter = rs_list.begin();

        while(iter != rs_list.end())
        {
          TBSYS_LOG(INFO, "hostname=%s", (*iter)->get_hostname());
          iter++;
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
