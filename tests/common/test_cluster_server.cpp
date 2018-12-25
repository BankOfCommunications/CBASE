#include "ob_define.h"
#include "gtest/gtest.h"
#include "ob_cluster_server.h"

using namespace oceanbase::common;

int main(int argc, char **argv)
{
  testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}

class TestClusterServer: public ::testing::Test
{
public:
  virtual void SetUp()
  {
  }

  virtual void TearDown()
  {
  }
};


TEST_F(TestClusterServer, ObServerCompare)
{
  ObServer server1;
  server1.set_ipv4_addr("127.0.0.1", 1024);
  EXPECT_FALSE(server1 < server1);
  EXPECT_TRUE(server1 == server1);
  ObServer server2;
  // ip <
  server2.set_ipv4_addr("127.0.0.0", 1023);
  EXPECT_TRUE(server2 < server1);
  EXPECT_FALSE(server1 < server2);
  EXPECT_FALSE(server1 == server2);
  server2.set_ipv4_addr("127.0.0.0", 1024);
  EXPECT_TRUE(server2 < server1);
  EXPECT_FALSE(server1 < server2);
  EXPECT_FALSE(server1 == server2);
  server2.set_ipv4_addr("127.0.0.0", 1025);
  EXPECT_TRUE(server2 < server1);
  EXPECT_FALSE(server1 < server2);
  EXPECT_FALSE(server1 == server2);
  // ip =
  server2.set_ipv4_addr("127.0.0.1", 1023);
  EXPECT_TRUE(server2 < server1);
  EXPECT_FALSE(server1 < server2);
  EXPECT_FALSE(server1 == server2);
  server2.set_ipv4_addr("127.0.0.1", 1024);
  EXPECT_FALSE(server2 < server1);
  EXPECT_FALSE(server1 < server2);
  EXPECT_TRUE(server1 == server2);
  server2.set_ipv4_addr("127.0.0.1", 1025);
  EXPECT_FALSE(server2 < server1);
  EXPECT_TRUE(server1 < server2);
  EXPECT_FALSE(server1 == server2);
  // ip >
  server2.set_ipv4_addr("127.0.0.2", 1023);
  EXPECT_TRUE(server1 < server2);
  EXPECT_FALSE(server2 < server1);
  EXPECT_FALSE(server1 == server2);
  server2.set_ipv4_addr("127.0.0.2", 1024);
  EXPECT_TRUE(server1 < server2);
  EXPECT_FALSE(server2 < server1);
  EXPECT_FALSE(server1 == server2);
  server2.set_ipv4_addr("127.0.0.2", 1025);
  EXPECT_TRUE(server1 < server2);
  EXPECT_FALSE(server2 < server1);
  EXPECT_FALSE(server1 == server2);
}

TEST_F(TestClusterServer, ObClusterServerCompare)
{
  ObServer server1;
  server1.set_ipv4_addr("127.0.0.1", 1024);
  ObClusterServer cluster1;
  cluster1.addr = server1;
  cluster1.role = OB_ROOTSERVER;
  EXPECT_FALSE(cluster1 < cluster1);

  ObClusterServer cluster2;
  ObServer server2;

  // role >
  cluster2.role = OB_UPDATESERVER;
  EXPECT_TRUE(cluster1 < cluster2);
  EXPECT_FALSE(cluster2 < cluster1);

  server2.set_ipv4_addr("127.0.0.0", 1023);
  cluster2.addr = server2;
  EXPECT_TRUE(cluster1 < cluster2);
  EXPECT_FALSE(cluster2 < cluster1);
  server2.set_ipv4_addr("127.0.0.0", 1024);
  cluster2.addr = server2;
  EXPECT_TRUE(cluster1 < cluster2);
  EXPECT_FALSE(cluster2 < cluster1);
  server2.set_ipv4_addr("127.0.0.0", 1025);
  cluster2.addr = server2;
  EXPECT_TRUE(cluster1 < cluster2);
  EXPECT_FALSE(cluster2 < cluster1);

  server2.set_ipv4_addr("127.0.0.1", 1023);
  cluster2.addr = server2;
  EXPECT_TRUE(cluster1 < cluster2);
  EXPECT_FALSE(cluster2 < cluster1);
  server2.set_ipv4_addr("127.0.0.1", 1024);
  cluster2.addr = server2;
  EXPECT_TRUE(cluster1 < cluster2);
  EXPECT_FALSE(cluster2 < cluster1);
  server2.set_ipv4_addr("127.0.0.1", 1025);
  cluster2.addr = server2;
  EXPECT_TRUE(cluster1 < cluster2);
  EXPECT_FALSE(cluster2 < cluster1);

  server2.set_ipv4_addr("127.0.0.2", 1023);
  cluster2.addr = server2;
  EXPECT_TRUE(cluster1 < cluster2);
  EXPECT_FALSE(cluster2 < cluster1);
  server2.set_ipv4_addr("127.0.0.2", 1024);
  cluster2.addr = server2;
  EXPECT_TRUE(cluster1 < cluster2);
  EXPECT_FALSE(cluster2 < cluster1);
  server2.set_ipv4_addr("127.0.0.2", 1025);
  cluster2.addr = server2;
  EXPECT_TRUE(cluster1 < cluster2);
  EXPECT_FALSE(cluster2 < cluster1);

  // role <
  cluster2.role = OB_MERGESERVER;
  EXPECT_TRUE(cluster2 < cluster1);
  EXPECT_FALSE(cluster1 < cluster2);

  server2.set_ipv4_addr("127.0.0.0", 1023);
  cluster2.addr = server2;
  EXPECT_TRUE(cluster2 < cluster1);
  EXPECT_FALSE(cluster1 < cluster2);
  server2.set_ipv4_addr("127.0.0.0", 1024);
  cluster2.addr = server2;
  EXPECT_TRUE(cluster2 < cluster1);
  EXPECT_FALSE(cluster1 < cluster2);
  server2.set_ipv4_addr("127.0.0.0", 1025);
  cluster2.addr = server2;
  EXPECT_TRUE(cluster2 < cluster1);
  EXPECT_FALSE(cluster1 < cluster2);

  server2.set_ipv4_addr("127.0.0.1", 1023);
  cluster2.addr = server2;
  EXPECT_TRUE(cluster2 < cluster1);
  EXPECT_FALSE(cluster1 < cluster2);
  server2.set_ipv4_addr("127.0.0.1", 1024);
  cluster2.addr = server2;
  EXPECT_TRUE(cluster2 < cluster1);
  EXPECT_FALSE(cluster1 < cluster2);
  server2.set_ipv4_addr("127.0.0.1", 1025);
  cluster2.addr = server2;
  EXPECT_TRUE(cluster2 < cluster1);
  EXPECT_FALSE(cluster1 < cluster2);

  server2.set_ipv4_addr("127.0.0.2", 1023);
  cluster2.addr = server2;
  EXPECT_TRUE(cluster2 < cluster1);
  EXPECT_FALSE(cluster1 < cluster2);
  server2.set_ipv4_addr("127.0.0.2", 1024);
  cluster2.addr = server2;
  EXPECT_TRUE(cluster2 < cluster1);
  EXPECT_FALSE(cluster1 < cluster2);
  server2.set_ipv4_addr("127.0.0.2", 1025);
  cluster2.addr = server2;
  EXPECT_TRUE(cluster2 < cluster1);
  EXPECT_FALSE(cluster1 < cluster2);

  // role =
  cluster2.role = OB_ROOTSERVER;
  server2.set_ipv4_addr("127.0.0.0", 1023);
  cluster2.addr = server2;
  EXPECT_TRUE(cluster2 < cluster1);
  EXPECT_FALSE(cluster1 < cluster2);
  server2.set_ipv4_addr("127.0.0.0", 1024);
  cluster2.addr = server2;
  EXPECT_TRUE(cluster2 < cluster1);
  EXPECT_FALSE(cluster1 < cluster2);
  server2.set_ipv4_addr("127.0.0.0", 1025);
  cluster2.addr = server2;
  EXPECT_TRUE(cluster2 < cluster1);
  EXPECT_FALSE(cluster1 < cluster2);

  server2.set_ipv4_addr("127.0.0.1", 1023);
  cluster2.addr = server2;
  EXPECT_TRUE(cluster2 < cluster1);
  EXPECT_FALSE(cluster1 < cluster2);
  server2.set_ipv4_addr("127.0.0.1", 1024);
  cluster2.addr = server2;
  EXPECT_FALSE(cluster2 < cluster1);
  EXPECT_FALSE(cluster1 < cluster2);
  server2.set_ipv4_addr("127.0.0.1", 1025);
  cluster2.addr = server2;
  EXPECT_TRUE(cluster1 < cluster2);
  EXPECT_FALSE(cluster2 < cluster1);

  server2.set_ipv4_addr("127.0.0.2", 1023);
  cluster2.addr = server2;
  EXPECT_TRUE(cluster1 < cluster2);
  EXPECT_FALSE(cluster2 < cluster1);
  server2.set_ipv4_addr("127.0.0.2", 1024);
  cluster2.addr = server2;
  EXPECT_TRUE(cluster1 < cluster2);
  EXPECT_FALSE(cluster2 < cluster1);
  server2.set_ipv4_addr("127.0.0.2", 1025);
  cluster2.addr = server2;
  EXPECT_FALSE(cluster2 < cluster1);
  EXPECT_TRUE(cluster1 < cluster2);
}

