#include <gtest/gtest.h>
#include <stdlib.h>
#include <map>
#include <vector>
#include <string.h>
#include "common/ob_define.h"
#include "common/ob_schema.h"
#include "common/ob_range.h"
#include "common/ob_malloc.h"
#include "common/ob_scanner.h"
#include "common/ob_tablet_info.h"
#include "updateserver/ob_client_wrapper.h"
#include "updateserver/ob_ups_cache.h"
#include "updateserver/ob_update_server_main.h"
#include "mergeserver/ob_ms_rpc_stub.h"
#include "mergeserver/ob_ms_schema_manager.h"
#include "mergeserver/ob_ms_tablet_location.h"
#include "mock_server.h"
#include "mock_root_server2.h"
#include "mock_update_server2.h"
#include "mock_chunk_server2.h"
#include "test_ups_table_mgr_helper.h"

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::updateserver::test;
using namespace oceanbase::updateserver;
using namespace std;

namespace oceanbase
{
  namespace updateserver
  {
    namespace test
    {
      class TestClientWrapper : public ::testing::Test
      {
        public:
          static void SetUpTestCase()
          {
          }
          static void TearDownTestCase()
          {
          }

          virtual void SetUp()
          {
          }

          virtual void TearDown()
          {
          }
      };

      TEST_F(TestClientWrapper,init)
      {
        const char * addr = "localhost";
        int64_t rpc_retry_times = 3;
        int64_t rpc_timeout = 20;
        ObServer root_server;
        root_server.set_ipv4_addr(addr, MockRootServer::ROOT_SERVER_PORT);

        ObServer update_server;
        update_server.set_ipv4_addr(addr, MockUpdateServer::UPDATE_SERVER_PORT);

        ObServer merge_server;
        ObUpsTableMgr ups_table_mgr;
        ups_table_mgr.init();
        ObUpsCache ups_cache;
        ups_cache.init();

        ObClientWrapper client_wrapper(rpc_retry_times, rpc_timeout, root_server,
            update_server, merge_server, ups_table_mgr, ups_cache);

        ObMergerRpcStub stub;
        ASSERT_TRUE(OB_SUCCESS != client_wrapper.init(NULL, NULL, NULL));
        ASSERT_TRUE(OB_SUCCESS != client_wrapper.init(&stub, NULL, NULL));

        ObMergerSchemaManager * schema = new ObMergerSchemaManager;
        ASSERT_TRUE(NULL != schema);
        ASSERT_TRUE(OB_SUCCESS != client_wrapper.init(&stub, schema, NULL));

        ObMergerTabletLocationCache * location = new ObMergerTabletLocationCache;
        ASSERT_TRUE(NULL != location);

        ASSERT_TRUE(OB_SUCCESS == client_wrapper.init(&stub, schema, location));
        ASSERT_TRUE(rpc_timeout == client_wrapper.get_proxy().get_rpc_timeout());
        ASSERT_TRUE(root_server == client_wrapper.get_proxy().get_root_server());
        ASSERT_TRUE(update_server == client_wrapper.get_proxy().get_update_server());
        ASSERT_TRUE(NULL != client_wrapper.get_proxy().get_rpc_stub());

        ASSERT_TRUE(OB_SUCCESS != client_wrapper.init(&stub, schema, location));

        delete schema;
        schema = NULL;
        delete location;
        location = NULL;
      }

      TEST_F(TestClientWrapper,get)
      {
        int ret = 0;
        //const char *addr = "localhost";
        int64_t addr = 16777343;
        int64_t timeout = 1000000;
        ThreadSpecificBuffer buffer;
        ObPacketFactory factory;
        tbnet::Transport transport;
        tbnet::DefaultPacketStreamer streamer; 
        streamer.setPacketFactory(&factory);
        transport.start();
        ObClientManager client_manager;
        ASSERT_TRUE(OB_SUCCESS == client_manager.initialize(&transport, &streamer));

        // start root server
        MockRootServer root;
        tbsys::CThread root_server_thread;
        MockServerRunner test_root_server(root);
        root_server_thread.start(&test_root_server, NULL);

        // start chunk server
        MockChunkServer chunk;
        tbsys::CThread chunk_server_thread;
        MockServerRunner test_chunk_server(chunk);
        chunk_server_thread.start(&test_chunk_server, NULL);
        sleep(2);
        
        //init the client_wrapper
        ObServer root_server;
        root_server.set_ipv4_addr(addr, MockRootServer::ROOT_SERVER_PORT);
        ObServer update_server;
        update_server.set_ipv4_addr(addr, 2302);
        ObServer merge_server;
        merge_server.set_ipv4_addr(addr,10256);
        //ObMergerRpcProxy proxy(3, timeout, root_server, update_server, merge_server);
        //set the manager_param
        ObMergerSchemaManager * manager_param = new ObMergerSchemaManager;
        tbsys::CConfig c1;
        ObSchemaManagerV2 schema_manager(200);
        ASSERT_EQ(true, schema_manager.parse_from_file("./test1.ini", c1));
        //(*manager_param).init(schema_manager);
        CommonSchemaManagerWrapper ups_schema_mgr;
        ups_schema_mgr.set_impl(schema_manager);

        ObUpsTableMgr& ups_table_mgr = ObUpdateServerMain::get_instance()->get_update_server().get_table_mgr();
        ups_table_mgr.init();
        ret = ups_table_mgr.set_schemas(ups_schema_mgr);
        ASSERT_EQ(0, ret);
        ObUpsCache ups_cache;
        ups_cache.init();
        
        ObClientWrapper * client_wrapper = new ObClientWrapper(1, timeout, root_server,
            update_server, merge_server, ups_table_mgr, ups_cache);

        //set the get_param
        ObGetParam get_param;
        ObCellInfo *cell = new ObCellInfo[8];
        for (int64_t i = 1; i < 9; i++)
        {
          char *temp = new char[256];
          ObString row_key;
          snprintf(temp, 256, "row_key_%ld",i);
          row_key.assign(temp, strlen(temp));
          cell[i-1].table_id_ = 1001;
          cell[i-1].row_key_ = row_key;
          cell[i-1].column_id_ = i;
          ASSERT_TRUE(OB_SUCCESS == get_param.add_cell(cell[i-1]));
          
        }
        delete[] cell;
        TBSYS_LOG(INFO,"get_param cell_size=%lu,row_size=%lu",get_param.get_cell_size(),get_param.get_row_size()); 
        //set the version range
        ObVersionRange range;
        range.start_version_ = 0;
        range.end_version_ = 1;
        range.border_flag_.set_inclusive_start();
        range.border_flag_.set_inclusive_end();
        get_param.set_version_range(range);

        //uninited
        ASSERT_TRUE(OB_SUCCESS != client_wrapper->get(get_param,schema_manager));
        //init
        ObMergerTabletLocationCache * location = new ObMergerTabletLocationCache;
        ASSERT_TRUE(NULL != location);
        ASSERT_TRUE(OB_SUCCESS == location->init(50000 * 5, 1000, 10000));
        ObMergerRpcStub stub;
        ASSERT_TRUE(OB_SUCCESS == stub.init(&buffer, &client_manager));
        
        //success
        ASSERT_TRUE(OB_SUCCESS == client_wrapper->init(&stub,manager_param,location));
        // get from chunkserver only
        ASSERT_TRUE(OB_SUCCESS == client_wrapper->get(get_param,schema_manager));
        
        //test get_cell
        ObCellInfo * cell_info = NULL;
        int64_t i = 0;
        int64_t  j = 0;
        uint64_t table_id;
        uint64_t column_id;
        for(; ; i++)
        {
          j = 0;
          ASSERT_TRUE(OB_SUCCESS == client_wrapper->get_cell(&cell_info));
          table_id = cell_info->table_id_;
          column_id = cell_info->column_id_;
          cell_info->value_.get_int(j);
          ASSERT_EQ(1001, (int64_t) table_id);
          ASSERT_EQ((int64_t) column_id, i + 1);
          ASSERT_EQ((int64_t) (i+2233), j);
          ret = client_wrapper->next_cell();
          if(ret != OB_SUCCESS)
          {
            break;
          }
        }
        TBSYS_LOG(INFO,"i=%d",i);
        TBSYS_LOG(INFO,"table_id=%u,column_id=%u",cell_info->table_id_,cell_info->column_id_);

        range.start_version_ = 0;
        range.end_version_ = 2;
        get_param.set_version_range(range);

        // get data from update server (invalid start version)
        // change the version range
        //
        //ret = client_wrapper->get(get_param,schema_manager);
        //ASSERT_EQ(OB_INVALID_START_VERSION, ret);
        
        ups_table_mgr.sstable_scan_finished(10);
        // updateserver has no data
        ret = client_wrapper->get(get_param, schema_manager);
        ASSERT_EQ(OB_SUCCESS, ret);

        // updateserver has data, need merge
        TestUpsTableMgrHelper test_helper(ups_table_mgr);

        TableMgr& table_mgr = test_helper.get_table_mgr();
        TableItem* active_memtable_item = table_mgr.get_active_memtable();
        MemTable& active_memtable = active_memtable_item->get_memtable();
        ObCellInfo *ups_cell = new ObCellInfo[8];
        ObUpsMutator ups_mutator;
        ObMutator& mutator = ups_mutator.get_mutator();
        for (i = 1; i < 9; i++)
        {
          char *temp = new char[256];
          ObString row_key;
          snprintf(temp, 256, "row_key_%ld",i);
          row_key.assign(temp, strlen(temp));
          ups_cell[i-1].table_id_ = 1001;
          ups_cell[i-1].row_key_ = row_key;
          ups_cell[i-1].column_id_ = i;
          ups_cell[i-1].value_.set_int(i-1, true);
          ObMutatorCellInfo mutator_cell;
          mutator_cell.cell_info = ups_cell[i-1];
          mutator_cell.op_type.set_ext(ObActionFlag::OP_UPDATE);
          ASSERT_TRUE(OB_SUCCESS == mutator.add_cell(mutator_cell));
          
        }
        delete[] ups_cell;
        // write row to active memtable
        MemTableTransHandle write_handle;
        ret = active_memtable.start_transaction(WRITE_TRANSACTION, write_handle);
        ASSERT_EQ(0, ret);
        ups_mutator.set_mutate_timestamp(0);
        ret = active_memtable.set(write_handle, ups_mutator);
        ASSERT_EQ(0, ret);
        ret = active_memtable.end_transaction(write_handle);
        ASSERT_EQ(0, ret);

        ret = client_wrapper->get(get_param, schema_manager); 
        ASSERT_EQ(0, ret);
        for(i = 0; ; i++)
        {
          j = 0;
          ASSERT_TRUE(OB_SUCCESS == client_wrapper->get_cell(&cell_info));
          table_id = cell_info->table_id_;
          column_id = cell_info->column_id_;
          cell_info->value_.get_int(j);
          ASSERT_EQ(1001, (int64_t) table_id);
          ASSERT_EQ((int64_t) column_id, i + 1);
          ASSERT_EQ((int64_t) (2*i+2233), j);
          ret = client_wrapper->next_cell();
          if(ret != OB_SUCCESS)
          {
            break;
          }
        }

        root.stop();
        chunk.stop();
        sleep(3);
        transport.stop();
        
        delete manager_param;
        manager_param = NULL;
        
        delete location;
        location = NULL;
        delete client_wrapper;
        client_wrapper = NULL;
      }
    }
  }
}

int main(int argc, char **argv)
{
  ob_init_memory_pool();
  testing::InitGoogleTest(&argc,argv);
 // TBSYS_LOGGER.setLogLevel("ERROR");
  return RUN_ALL_TESTS();
}
