#include <gtest/gtest.h>
#include "rootserver/ob_chunk_server_manager.h"
#include <tbsys.h>
#include <unistd.h>

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::rootserver;
TEST(ObChunkServerManagerTest, find)
{
  ObChunkServerManager chunk_m;
  {
    ObServer server(ObServer::IPV4, "10.10.1.1", 100);
    chunk_m.receive_hb(server, 10);
  }
  {
    ObServer server(ObServer::IPV4, "10.10.1.2", 100);
    chunk_m.receive_hb(server, 11);
  }
  {
    ObServer server(ObServer::IPV4, "10.10.1.3", 100);
    chunk_m.receive_hb(server, 12);
  }
  ObServer server(ObServer::IPV4, "10.10.1.2", 100);
  int64_t pos = 0;
  char buff[1024*100];
  chunk_m.serialize(buff, 1024*100, pos);
  ObChunkServerManager::iterator it = chunk_m.find_by_ip(server);

  ASSERT_TRUE(it != chunk_m.end());
  ASSERT_TRUE(server == it->server_);
  ASSERT_EQ(11, it->last_hb_time_);

  it = chunk_m.begin();
  int k=0;
  for (; it != chunk_m.end(); ++it)
  {
    TBSYS_LOG(DEBUG, " serverid = %lu", it->server_.get_ipv4_server_id());
    k++;
  }
  ASSERT_EQ(3, k);

  const char* filename = "chunk_server_list.dat";
  int ret = chunk_m.write_to_file(filename);
  ASSERT_TRUE(ret == OB_SUCCESS);

  ObChunkServerManager* read_manager = new ObChunkServerManager();
  int32_t cs_num = 0;
  int32_t ms_num = 0;
  ret = read_manager->read_from_file(filename, cs_num, ms_num);
  TBSYS_LOG(DEBUG, "read from file result code: [%d]", ret);
  ASSERT_TRUE(ret == OB_SUCCESS);

  // compare
  ObChunkServerManager::iterator old_it;
  ObChunkServerManager::iterator new_it;

  for(old_it = chunk_m.begin(), new_it = read_manager->begin();
      old_it < chunk_m.end();
      ++old_it, ++new_it)
  {
    ASSERT_TRUE(old_it->server_ == new_it->server_);
    ASSERT_TRUE(old_it->last_hb_time_ == new_it->last_hb_time_);
    ASSERT_TRUE(old_it->status_ == new_it->status_);
    ASSERT_TRUE(old_it->port_cs_ == new_it->port_cs_);
    ASSERT_TRUE(old_it->port_ms_ == new_it->port_ms_);
    ASSERT_TRUE(old_it->disk_info_.get_capacity() == new_it->disk_info_.get_capacity());
    ASSERT_TRUE(old_it->disk_info_.get_used() == new_it->disk_info_.get_used());
  }

  unlink(filename);
  delete read_manager;

}
int main(int argc, char **argv)
{
  ob_init_memory_pool();
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
