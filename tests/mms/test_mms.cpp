#include <gtest/gtest.h>
#include "mms/ob_node.h"
#include "common/ob_trace_log.h"
#include "mms/ob_monitor.h"
#include "mock_node_server.h"
#include "mock_monitor_server.h"
#include "common/ob_define.h"
using namespace std;
using namespace oceanbase::common;
using namespace oceanbase::mms;
using namespace oceanbase::mms::tests;

const char * addr = "localhost";

int main(int argc, char **argv)
{
  ob_init_memory_pool();
  testing::InitGoogleTest(&argc,argv);
  TBSYS_LOGGER.setFileName("./stress.log");
  TBSYS_LOGGER.setMaxFileSize(100 * 1024L * 1024L); // 100M per log file
 // TBSYS_LOGGER.setLogLevel("DEBUG");
 // SET_TRACE_LOG_LEVEL("DEBUG");
  TBSYS_LOGGER.setLogLevel("INFO");
  return RUN_ALL_TESTS();
}

class TestMMS : public ::testing::Test
{
  public:
    void SetUp()
    {
    }
    void TearDown()
    {
    }
};

///*
//两个不同应用节点的注册心跳过程。
TEST_F(TestMMS, start)
{
  //monitor start  
  int port = 4567;
  int64_t retry_time = 5;
  int64_t timeout = 2000 * 1000L;
  int64_t lease_interval = 7000 * 1000L;
  int64_t renew_interval = 4000 * 1000L;
  MockMonitorServer server(port, retry_time, timeout, lease_interval, renew_interval);
  MockServerRunner test_monitor_server(server);
  tbsys::CThread monitor_server_thread;
  monitor_server_thread.start(&test_monitor_server, NULL);
  sleep(1);
  //node start
  int node_port = 5678;
  ObServer monitor;
  uint32_t ip = tbsys::CNetUtil::getLocalAddr("bond0");
  monitor.set_ipv4_addr(ip, port);
  char app_name[OB_MAX_APP_NAME_LENGTH] = "app_1";
  char instance_name[OB_MAX_INSTANCE_NAME_LENGTH] = "instance_1";
  char hostname[OB_MAX_APP_NAME_LENGTH] = "host_1";
  MockNodeServer node(node_port, retry_time, timeout, monitor, app_name, instance_name, hostname);
  MockServerRunner test_node_server(node);
  tbsys::CThread node_server_thread;
  node_server_thread.start(&test_node_server, NULL);
  sleep(1);

  int node_port_3 = 9876;
  char app_name_3[OB_MAX_APP_NAME_LENGTH] = "app_1";
  char instance_name_3[OB_MAX_INSTANCE_NAME_LENGTH] = "instance_1";
  char hostname_3[OB_MAX_APP_NAME_LENGTH] = "host_2";
  MockNodeServer node_3(node_port_3, retry_time, timeout, monitor, app_name_3, instance_name_3, hostname_3);
  MockServerRunner test_node_server_3(node_3);
  tbsys::CThread node_server_thread_3;
  node_server_thread_3.start(&test_node_server_3, NULL);
  sleep(1);

  //the second node start. another app_instance
  int node_port_2 = 6789;
  char app_name_2[OB_MAX_APP_NAME_LENGTH] = "app_2";
  char instance_name_2[OB_MAX_INSTANCE_NAME_LENGTH] = "instance_2";
  char hostname_2[OB_MAX_APP_NAME_LENGTH] = "host_2";
  MockNodeServer node_2(node_port_2, retry_time, timeout, monitor, app_name_2, instance_name_2, hostname_2);
  MockServerRunner test_node_server_2(node_2);
  tbsys::CThread node_server_thread_2;
  node_server_thread_2.start(&test_node_server_2, NULL);
  sleep(1);

  //the same instance with node_2;
  int node_port_4 = 2345;
  char app_name_4[OB_MAX_APP_NAME_LENGTH] = "app_2";
  char instance_name_4[OB_MAX_INSTANCE_NAME_LENGTH] = "instance_2";
  char hostname_4[OB_MAX_APP_NAME_LENGTH] = "host_4";
  MockNodeServer node_4(node_port_4, retry_time, timeout, monitor, app_name_4, instance_name_4, hostname_4);
  MockServerRunner test_node_server_4(node_4);
  tbsys::CThread node_server_thread_4;
  node_server_thread_4.start(&test_node_server_4, NULL);
  sleep(5);
  node.stop();
  node_3.stop();
  node_2.stop();
  node_4.stop();
  server.stop();
  sleep(2);
}
//*/
///*
//同一个实例的两个node的注册过程
TEST_F(TestMMS, start_2)
{
  //monitor start  
  int port = 2211;
  int64_t retry_time = 5;
  int64_t timeout = 2000 * 1000L;
  int64_t lease_interval = 7000 * 1000L;
  int64_t renew_interval = 4000 * 1000L;

  MockMonitorServer server(port, retry_time, timeout, lease_interval, renew_interval);
  MockServerRunner test_monitor_server(server);
  tbsys::CThread monitor_server_thread;
  monitor_server_thread.start(&test_monitor_server, NULL);
 
  //node start
  int node_port = 2233;
  ObServer monitor;
  uint32_t ip = tbsys::CNetUtil::getLocalAddr("bond0");
  monitor.set_ipv4_addr(ip, port);
  char app_name[OB_MAX_APP_NAME_LENGTH] = "app_1";
  char instance_name[OB_MAX_INSTANCE_NAME_LENGTH] = "instance_1";
  char hostname[OB_MAX_APP_NAME_LENGTH] = "host_1";
  
  MockNodeServer node(node_port, retry_time, timeout, monitor, app_name, instance_name, hostname);
  MockServerRunner test_node_server(node);
  tbsys::CThread node_server_thread;
  node_server_thread.start(&test_node_server, NULL);
  //the second node start. as slave
  int node_port_2 = 2244;
  char app_name_2[OB_MAX_APP_NAME_LENGTH] = "app_1";
  char instance_name_2[OB_MAX_INSTANCE_NAME_LENGTH] = "instance_1";
  char hostname_2[OB_MAX_APP_NAME_LENGTH] = "host_2";
  
  MockNodeServer node_2(node_port_2, retry_time, timeout, monitor, app_name_2, instance_name_2, hostname_2);
  MockServerRunner test_node_server_2(node_2);
  tbsys::CThread node_server_thread_2;
  node_server_thread_2.start(&test_node_server_2, NULL);
  
  fprintf(stdout, "start sleep\n");
  usleep(10 * 1000000);
  fprintf(stdout, "end sleep\n");
  node.stop();
  node_2.stop();
  server.stop();
  sleep(2);
}
//*/
///*
//when a master down
TEST_F(TestMMS, master_down)
{
  //monitor start  
  int port = 2211;
  int64_t retry_time = 5;
  int64_t timeout = 2000 * 1000L;
  int64_t lease_interval = 5000 * 1000L;
  int64_t renew_interval = 2000 * 1000L;

  MockMonitorServer server(port, retry_time, timeout, lease_interval, renew_interval);
  MockServerRunner test_monitor_server(server);
  tbsys::CThread monitor_server_thread;
  monitor_server_thread.start(&test_monitor_server, NULL);

  //node start
  int node_port = 2233;
  ObServer monitor;
  uint32_t ip = tbsys::CNetUtil::getLocalAddr("bond0");
  monitor.set_ipv4_addr(ip, port);
  char app_name[OB_MAX_APP_NAME_LENGTH] = "app_1";
  char instance_name[OB_MAX_INSTANCE_NAME_LENGTH] = "instance_1";
  char hostname[OB_MAX_APP_NAME_LENGTH] = "host_1";

  MockNodeServer node(node_port, retry_time, timeout, monitor, app_name, instance_name, hostname);
  MockServerRunner test_node_server(node);
  tbsys::CThread node_server_thread;
  node_server_thread.start(&test_node_server, NULL);
  sleep(2);
  //the second node start. as slave
  int node_port_2 = 2244;
  char app_name_2[OB_MAX_APP_NAME_LENGTH] = "app_1";
  char instance_name_2[OB_MAX_INSTANCE_NAME_LENGTH] = "instance_1";
  char hostname_2[OB_MAX_APP_NAME_LENGTH] = "host_2";

  MockNodeServer node_2(node_port_2, retry_time, timeout, monitor, app_name_2, instance_name_2, hostname_2);
  MockServerRunner test_node_server_2(node_2);
  tbsys::CThread node_server_thread_2;
  node_server_thread_2.start(&test_node_server_2, NULL);
  sleep(2);
  node.stop();
  sleep(17);
  node_2.get_node()->transfer_state(SLAVE_SYNC);
  sleep(10);
  node_2.stop();
  server.stop();
  sleep(2);
}
//*/
///*
//slave down
TEST_F(TestMMS, slave_down)
{
  //monitor start  
  int port = 2211;
  int64_t retry_time = 5;
  int64_t timeout = 2000 * 1000L;
  int64_t lease_interval = 5000 * 1000L;
  int64_t renew_interval = 2000 * 1000L;

  MockMonitorServer server(port, retry_time, timeout, lease_interval, renew_interval);
  MockServerRunner test_monitor_server(server);
  tbsys::CThread monitor_server_thread;
  monitor_server_thread.start(&test_monitor_server, NULL);

  //node start
  int node_port = 2233;
  ObServer monitor;
  uint32_t ip = tbsys::CNetUtil::getLocalAddr("bond0");
  monitor.set_ipv4_addr(ip, port);
  char app_name[OB_MAX_APP_NAME_LENGTH] = "app_1";
  char instance_name[OB_MAX_INSTANCE_NAME_LENGTH] = "instance_1";
  char hostname[OB_MAX_APP_NAME_LENGTH] = "host_1";

  MockNodeServer node(node_port, retry_time, timeout, monitor, app_name, instance_name, hostname);
  MockServerRunner test_node_server(node);
  tbsys::CThread node_server_thread;
  node_server_thread.start(&test_node_server, NULL);
  sleep(2);
  //the second node start. as slave
  int node_port_2 = 2244;
  char app_name_2[OB_MAX_APP_NAME_LENGTH] = "app_1";
  char instance_name_2[OB_MAX_INSTANCE_NAME_LENGTH] = "instance_1";
  char hostname_2[OB_MAX_APP_NAME_LENGTH] = "host_2";

  MockNodeServer node_2(node_port_2, retry_time, timeout, monitor, app_name_2, instance_name_2, hostname_2);
  MockServerRunner test_node_server_2(node_2);
  tbsys::CThread node_server_thread_2;
  node_server_thread_2.start(&test_node_server_2, NULL);
  sleep(3);
  node_2.get_node()->transfer_state(SLAVE_SYNC);
  node_2.stop();
  sleep(30);
  node.stop();
  server.stop();
  sleep(2);
}
//*/
///*
//monitor failure
TEST_F(TestMMS, monitor_down)
{
  //monitor start  
  int port = 2211;
  int64_t retry_time = 5;
  int64_t timeout = 2000 * 1000L;
  int64_t lease_interval = 5000 * 1000L;
  int64_t renew_interval = 2000 * 1000L;

  MockMonitorServer server(port, retry_time, timeout, lease_interval, renew_interval);
  MockServerRunner test_monitor_server(server);
  tbsys::CThread monitor_server_thread;
  monitor_server_thread.start(&test_monitor_server, NULL);

  //node start
  int node_port = 2233;
  ObServer monitor;
  uint32_t ip = tbsys::CNetUtil::getLocalAddr("bond0");
  monitor.set_ipv4_addr(ip, port);
  char app_name[OB_MAX_APP_NAME_LENGTH] = "app_1";
  char instance_name[OB_MAX_INSTANCE_NAME_LENGTH] = "instance_1";
  char hostname[OB_MAX_APP_NAME_LENGTH] = "host_1";

  MockNodeServer node(node_port, retry_time, timeout, monitor, app_name, instance_name, hostname);
  MockServerRunner test_node_server(node);
  tbsys::CThread node_server_thread;
  node_server_thread.start(&test_node_server, NULL);
  sleep(2);

  //the second node start. as slave
  int node_port_2 = 2244;
  char app_name_2[OB_MAX_APP_NAME_LENGTH] = "app_1";
  char instance_name_2[OB_MAX_INSTANCE_NAME_LENGTH] = "instance_1";
  char hostname_2[OB_MAX_APP_NAME_LENGTH] = "host_2";

  MockNodeServer node_2(node_port_2, retry_time, timeout, monitor, app_name_2, instance_name_2, hostname_2);
  MockServerRunner test_node_server_2(node_2);
  tbsys::CThread node_server_thread_2;
  node_server_thread_2.start(&test_node_server_2, NULL);
  sleep(3);
  node_2.get_node()->transfer_state(SLAVE_SYNC);
  sleep(3);
  server.stop();
  sleep(10);
  node.stop();
  node_2.stop();
}
//*/
///*
//master report slave dowm
TEST_F(TestMMS, master_report_slave_down)
{
  //monitor start  
  int port = 2211;
  int64_t retry_time = 5;
  int64_t timeout = 2000 * 1000L;
  int64_t lease_interval = 5000 * 1000L;
  int64_t renew_interval = 2000 * 1000L;

  MockMonitorServer server(port, retry_time, timeout, lease_interval, renew_interval);
  MockServerRunner test_monitor_server(server);
  tbsys::CThread monitor_server_thread;
  monitor_server_thread.start(&test_monitor_server, NULL);

  //node start
  int node_port = 2233;
  ObServer monitor;
  uint32_t ip = tbsys::CNetUtil::getLocalAddr("bond0");
  monitor.set_ipv4_addr(ip, port);
  char app_name[OB_MAX_APP_NAME_LENGTH] = "app_1";
  char instance_name[OB_MAX_INSTANCE_NAME_LENGTH] = "instance_1";
  char hostname[OB_MAX_APP_NAME_LENGTH] = "host_1";

  MockNodeServer node(node_port, retry_time, timeout, monitor, app_name, instance_name, hostname);
  MockServerRunner test_node_server(node);
  tbsys::CThread node_server_thread;
  node_server_thread.start(&test_node_server, NULL);
  sleep(2);

  //the second node start. as slave
  int node_port_2 = 2244;
  char app_name_2[OB_MAX_APP_NAME_LENGTH] = "app_1";
  char instance_name_2[OB_MAX_INSTANCE_NAME_LENGTH] = "instance_1";
  char hostname_2[OB_MAX_APP_NAME_LENGTH] = "host_2";

  MockNodeServer node_2(node_port_2, retry_time, timeout, monitor, app_name_2, instance_name_2, hostname_2);
  MockServerRunner test_node_server_2(node_2);
  tbsys::CThread node_server_thread_2;
  node_server_thread_2.start(&test_node_server_2, NULL);
  sleep(3);
  
  TBSYS_LOG(INFO, "send to monitor, addr=%d, port=%d", monitor.get_ipv4(), monitor.get_port());
  node.get_node()->report_slave_down(timeout, retry_time, monitor, (node_2.get_node()->get_self_server()));

  sleep(10);
  server.stop();
  node.stop();
  node_2.stop();
}
//*/


