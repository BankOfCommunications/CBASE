#include <iostream>
#include <sstream>
#include <algorithm>
#include <tblog.h>
#include <tbsys.h>
#include <gtest/gtest.h>
#include <gmock/gmock.h>

#include "common/ob_schema.h"
#include "common/ob_malloc.h"
#include "common/ob_scanner.h"
#include "common/ob_tablet_info.h"
#include  "mergeserver/ob_ms_schema_manager.h"
#include "mergeserver/ob_rs_rpc_proxy.h"
#include "mergeserver/ob_ms_rpc_proxy.h"
#include "mergeserver/ob_ms_tablet_location.h"
#include "mergeserver/ob_ms_tablet_location_proxy.h"
#include "mergeserver/ob_ms_rpc_stub.h"
#include "mergeserver/ob_ms_scan_event.h"
#include "mergeserver/ob_ms_async_rpc.h"

#include "mock_server.h"
#include "mock_root_server.h"
#include "mock_update_server.h"
#include "mock_chunk_server.h"

#include "ob_fake_table.h"

#include "sql/ob_rpc_scan.h"
#include "sql/ob_item_type.h"
#include "common/ob_row.h"
using namespace std;
using namespace oceanbase::common;
using namespace tbsys;
using namespace oceanbase::mergeserver;
using namespace oceanbase::sql;
using namespace oceanbase::sql::test;
using namespace testing;

const uint64_t timeout = 10000000;
const char * addr = "127.0.0.1";
/*
namespace oceanbase
{
  namespace sql
  {
    namespace test
    {
*/

class ExprGen{
  public:
    static ObSqlExpression & create_expr_by_id(uint64_t id)
    {
      ExprItem item_a;
      item_a.type_ = T_REF_COLUMN;
      item_a.value_.cell_.tid = test::ObFakeTable::TABLE_ID;
      item_a.value_.cell_.cid = id;
      sql_expr.add_expr_item(item_a);
      sql_expr.add_expr_item_end();
      sql_expr.set_tid_cid(test::ObFakeTable::TABLE_ID, id);
      return sql_expr;
    }

    static ObSqlExpression sql_expr;
    static ObStringBuf buf;
};
ObSqlExpression  ExprGen::sql_expr;
ObStringBuf ExprGen::buf;



class TestObRpcScan: public ::testing::Test
{
  public:
    virtual void SetUp()
    {
    }

    virtual void TearDown()
    {
    }
};

class MockObClientManager : public ObClientManager
{
  public:
    MOCK_CONST_METHOD7(post_request,
        int (const ObServer& server,
          const int32_t pcode,
          const int32_t version,
          const int64_t timeout,
          const ObDataBuffer& in_buffer,
          tbnet::IPacketHandler* handler,
          void* args));

    //    MOCK_METHOD2(handlePacket,
    //         tbnet::IPacketHandler::HPRetCode(tbnet::Packet* packet, void * args));
};

class MockObMergerAsyncRpcStub : public ObMergerAsyncRpcStub {
  public:
/*
    MOCK_CONST_METHOD1(check_inner_stat,
        bool(void));
    MOCK_CONST_METHOD4(get,
        int(const int64_t timeout, const common::ObServer & server, const common::ObGetParam & get_param, ObMergerRpcEvent & result));
    MOCK_CONST_METHOD4(scan,
        int(const int64_t timeout, const ObServer & server, const ObScanParam & scan_param, ObMergerRpcEvent & result));
    MOCK_CONST_METHOD5(get_session_next,
        int(const int64_t timeout, const common::ObServer & server, const int64_t session_id, const int32_t req_type, ObMergerRpcEvent & result));
  */
    MOCK_CONST_METHOD4(scan,
        int(const int64_t timeout, const ObServer & server, const ObSqlScanParam & scan_param, ObMsSqlRpcEvent & result));
    MOCK_CONST_METHOD5(get_session_next,
        int(const int64_t timeout, const ObServer & server, const int64_t session_id, const int32_t req_type, ObMsSqlRpcEvent & result));
};


int times = 0;
int handle_scan_table(ObDataBuffer &out_buffer)
{
  int ret = OB_SUCCESS;

  ObResultCode result_msg;
  result_msg.result_code_ = ret;
  ret = result_msg.serialize(out_buffer.get_data(), out_buffer.get_capacity(), out_buffer.get_position());

  // fake cell
  ObNewScanner scanner;
  ObRow row_;
  ObRowDesc row_desc_;
  ObString column_name;
  char temp[256] = "";
  ObObj obj_a, obj_b, obj_d;
  ObObj str_c;
  ObString var_str;
  row_desc_.add_column_desc(test::ObFakeTable::TABLE_ID, OB_APP_MIN_COLUMN_ID + 0);
  //row_desc_.add_column_desc(test::ObFakeTable::TABLE_ID, OB_APP_MIN_COLUMN_ID + 1);
  //row_desc_.add_column_desc(test::ObFakeTable::TABLE_ID, OB_APP_MIN_COLUMN_ID + 2);
  //row_desc_.add_column_desc(test::ObFakeTable::TABLE_ID, OB_APP_MIN_COLUMN_ID + 3);
  obj_a.set_int(19);
  obj_b.set_int(2);
  var_str.assign((char*)"hello", 5);
  str_c.set_varchar(var_str);
  obj_d.set_int(3);
  row_.set_row_desc(row_desc_);
  row_.set_cell(test::ObFakeTable::TABLE_ID, OB_APP_MIN_COLUMN_ID + 0, obj_a);
  //row_.set_cell(test::ObFakeTable::TABLE_ID, OB_APP_MIN_COLUMN_ID + 1, obj_b);
  //row_.set_cell(test::ObFakeTable::TABLE_ID, OB_APP_MIN_COLUMN_ID + 2, str_c);
  //row_.set_cell(test::ObFakeTable::TABLE_ID, OB_APP_MIN_COLUMN_ID + 3, obj_d);


  /* begin add by xiaochu */
  //Scanner Range must be set other wise the ms client will report error
  ObNewRange range;
  ObRowkey start_key;
  ObRowkey end_key;
  ObObj start_key_obj;
  ObObj end_key_obj;
  bool is_full;
  ObRowkey last_row_key;
  ObObj last_row_key_obj;

  /* share same range for all sub scan */
  start_key_obj.set_min_value();
  end_key_obj.set_max_value();
  start_key.assign(&start_key_obj, 1);
  end_key.assign(&end_key_obj, 1);
  range.set_whole_range();
  range.border_flag_.unset_inclusive_start();
  range.border_flag_.set_inclusive_end();
  range.start_key_ = start_key;
  range.end_key_ = end_key;
  range.table_id_ = test::ObFakeTable::TABLE_ID;
  scanner.set_range(range);


  if (times == 0)
  {
    for (uint64_t i = 0; i < 3; ++i)
    {
      if (OB_SUCCESS != scanner.add_row(row_))
      {
        printf("handle table scan: fail! \n");
      }
      printf("handle table scan: add new row to scanner\n");
    }

    ObRowkey last_row_key;
    ObObj last_row_key_obj;
    last_row_key_obj.set_int(100 *  (times + 1));
    is_full = false;
    last_row_key.assign(&last_row_key_obj, 1);
    scanner.set_last_row_key(last_row_key);
    scanner.set_is_req_fullfilled(is_full, 3);
    times++;
    TBSYS_LOG(INFO, "times = %d", times);
  }
  else if (times == 1)
  {
    for (uint64_t i = 0; i < 2; ++i)
    {
      if (OB_SUCCESS != scanner.add_row(row_))
      {
        printf("handle table scan: fail! \n");
      }
      printf("handle table scan: add new row to scanner\n");
    }
    ObRowkey last_row_key;
    ObObj last_row_key_obj;
    last_row_key_obj.set_int(100 *  (times + 1));
    is_full = false;
    last_row_key.assign(&last_row_key_obj, 1);
    scanner.set_last_row_key(last_row_key);
    scanner.set_is_req_fullfilled(is_full, 2);
    times++;
    TBSYS_LOG(INFO, "times = %d", times);
  }
  else if (times == 2)
  {
    for (uint64_t i = 0; i < 5; ++i)
    {
      if (OB_SUCCESS != scanner.add_row(row_))
      {
        printf("handle table scan: fail! \n");
      }
      printf("handle table scan: add new row to scanner\n");
    }
    is_full = false;
    last_row_key_obj.set_int(100 *  (times + 1));
    last_row_key.assign(&last_row_key_obj, 1);
    scanner.set_last_row_key(last_row_key);
    scanner.set_is_req_fullfilled(is_full, 5);
    times++;
    TBSYS_LOG(INFO, "times = %d", times);
  }
  else if (times == 3)
  {
    for (uint64_t i = 0; i < 4; ++i)
    {
      if (OB_SUCCESS != scanner.add_row(row_))
      {
        printf("handle table scan: fail! \n");
      }
      printf("handle table scan: add new row to scanner\n");
    }
    is_full = true;
    last_row_key_obj.set_max_value();
    last_row_key.assign(&last_row_key_obj, 1);
    scanner.set_last_row_key(last_row_key);
    scanner.set_is_req_fullfilled(is_full, 4);
    times++;
    TBSYS_LOG(INFO, "times = %d", times);
  }

  else
  {
    TBSYS_LOG(WARN, "invalid times!!! times=%d", times);
  }

  /* end add by xiaochu */
  if (OB_SUCCESS != (ret = scanner.serialize(out_buffer.get_data(), out_buffer.get_capacity(), out_buffer.get_position())))
  {
    TBSYS_LOG(WARN, "fail to serialize scanner");
  }

  TBSYS_LOG(INFO, "handle scan table result:ret[%d]", ret);
  return ret;
}
int handle_scan_table2(ObDataBuffer &out_buffer)
{
  int ret = OB_SUCCESS;

  ObResultCode result_msg;
  result_msg.result_code_ = ret;
  ret = result_msg.serialize(out_buffer.get_data(), out_buffer.get_capacity(), out_buffer.get_position());

  // fake cell
  ObNewScanner scanner;
  ObRow row_;
  ObRowDesc row_desc_;
  ObString column_name;
  char temp[256] = "";
  ObObj obj_a, obj_b, obj_d;
  ObObj str_c;
  ObString var_str;
  row_desc_.add_column_desc(test::ObFakeTable::TABLE_ID, OB_APP_MIN_COLUMN_ID + 0);
  //row_desc_.add_column_desc(test::ObFakeTable::TABLE_ID, OB_APP_MIN_COLUMN_ID + 1);
  //row_desc_.add_column_desc(test::ObFakeTable::TABLE_ID, OB_APP_MIN_COLUMN_ID + 2);
  //row_desc_.add_column_desc(test::ObFakeTable::TABLE_ID, OB_APP_MIN_COLUMN_ID + 3);
  obj_a.set_int(19);
  obj_b.set_int(2);
  var_str.assign((char*)"hello", 5);
  str_c.set_varchar(var_str);
  obj_d.set_int(3);
  row_.set_row_desc(row_desc_);
  row_.set_cell(test::ObFakeTable::TABLE_ID, OB_APP_MIN_COLUMN_ID + 0, obj_a);
  //row_.set_cell(test::ObFakeTable::TABLE_ID, OB_APP_MIN_COLUMN_ID + 1, obj_b);
  //row_.set_cell(test::ObFakeTable::TABLE_ID, OB_APP_MIN_COLUMN_ID + 2, str_c);
  //row_.set_cell(test::ObFakeTable::TABLE_ID, OB_APP_MIN_COLUMN_ID + 3, obj_d);


  /* begin add by xiaochu */
  //Scanner Range must be set other wise the ms client will report error
  ObNewRange range;
  ObRowkey start_key;
  ObRowkey end_key;
  ObObj start_key_obj;
  ObObj end_key_obj;
  bool is_full;
  ObRowkey last_row_key;
  ObObj last_row_key_obj;

  if (times == 0)
  {
    for (uint64_t i = 0; i < 3; ++i)
    {
      if (OB_SUCCESS != scanner.add_row(row_))
      {
        printf("handle table scan: fail! \n");
      }
      printf("handle table scan: add new row to scanner\n");
    }
#if 0
    start_key_obj.set_min_value();
    end_key_obj.set_max_value();
    start_key.assign(&start_key_obj, 1);
    end_key.assign(&end_key_obj, 1);
    range.border_flag_.set_min_value();
    range.border_flag_.set_max_value();
    range.border_flag_.unset_inclusive_start();
    range.border_flag_.set_inclusive_end();
    range.start_key_ = start_key;
    range.end_key_ = end_key;
    range.table_id_ = test::ObFakeTable::TABLE_ID;
    scanner.set_range(range);

    ObRowkey last_row_key;
    ObObj last_row_key_obj;
    last_row_key_obj.set_int(3);//100 *  (times + 1));

    is_full = true;//false;
    last_row_key.assign(&last_row_key_obj, 1);
#else

    start_key_obj.set_min_value();
    end_key_obj.set_int(100);
    start_key.assign(&start_key_obj, 1);
    end_key.assign(&end_key_obj, 1);
    range.start_key_.set_min_row();
    range.border_flag_.unset_inclusive_start();
    range.border_flag_.set_inclusive_end();
    range.start_key_ = start_key;
    range.end_key_ = end_key;
    range.table_id_ = test::ObFakeTable::TABLE_ID;

    scanner.set_range(range);

    ObRowkey last_row_key;
    ObObj last_row_key_obj;
    last_row_key_obj.set_int(100 *  (times + 1));

    is_full = false;
    last_row_key.assign(&last_row_key_obj, 1);
#endif
    scanner.set_last_row_key(last_row_key);
    scanner.set_is_req_fullfilled(is_full, 3);
    times++;
    TBSYS_LOG(INFO, "times = %d", times);
  }
  else if (times == 1)
  {
    for (uint64_t i = 0; i < 2; ++i)
    {
      if (OB_SUCCESS != scanner.add_row(row_))
      {
        printf("handle table scan: fail! \n");
      }
      printf("handle table scan: add new row to scanner\n");
    }
    start_key_obj.set_int(100 * (times));
    end_key_obj.set_int(100 *  (times) + 2);
    start_key.assign(&start_key_obj, 1);
    end_key.assign(&end_key_obj, 1);
    range.start_key_ = start_key;
    range.end_key_ = end_key;
    range.border_flag_.unset_inclusive_start();
    range.border_flag_.set_inclusive_end();
    range.table_id_ = test::ObFakeTable::TABLE_ID;
    scanner.set_range(range);
    ObRowkey last_row_key;
    ObObj last_row_key_obj;
    last_row_key_obj.set_int(100 *  (times + 1));
    is_full = false;
    last_row_key.assign(&last_row_key_obj, 1);
    scanner.set_last_row_key(last_row_key);
    scanner.set_is_req_fullfilled(is_full, 2);
    times++;
    TBSYS_LOG(INFO, "times = %d", times);
  }
  else if (times == 2)
  {
    for (uint64_t i = 0; i < 5; ++i)
    {
      if (OB_SUCCESS != scanner.add_row(row_))
      {
        printf("handle table scan: fail! \n");
      }
      printf("handle table scan: add new row to scanner\n");
    }
    start_key_obj.set_int(100 * (times));
    end_key_obj.set_int(100 * (times) + 5);
    start_key.assign(&start_key_obj, 1);
    end_key.assign(&end_key_obj, 1);
    range.start_key_ = start_key;
    range.end_key_ = end_key;
    range.border_flag_.unset_inclusive_start();
    range.border_flag_.set_inclusive_end();
    range.table_id_ = test::ObFakeTable::TABLE_ID;
    scanner.set_range(range);
    is_full = false;
    last_row_key_obj.set_int(100 *  (times + 1));
    last_row_key.assign(&last_row_key_obj, 1);
    scanner.set_last_row_key(last_row_key);
    scanner.set_is_req_fullfilled(is_full, 5);
    times++;
    TBSYS_LOG(INFO, "times = %d", times);
  }
  else if (times == 3)
  {
    for (uint64_t i = 0; i < 4; ++i)
    {
      if (OB_SUCCESS != scanner.add_row(row_))
      {
        printf("handle table scan: fail! \n");
      }
      printf("handle table scan: add new row to scanner\n");
    }
    start_key_obj.set_int(100 * (times));
    end_key_obj.set_max_value();
    start_key.assign(&start_key_obj, 1);
    end_key.assign(&end_key_obj, 1);
    range.start_key_ = start_key;
    range.border_flag_.unset_inclusive_start();
    range.border_flag_.set_inclusive_end();
    range.table_id_ = test::ObFakeTable::TABLE_ID;
    range.end_key_.set_max_row();
    scanner.set_range(range);
    end_key_obj.set_max_value();
    is_full = true;
    last_row_key_obj.set_max_value();
    last_row_key.assign(&last_row_key_obj, 1);
    scanner.set_last_row_key(last_row_key);
    scanner.set_is_req_fullfilled(is_full, 4);
    times++;
    TBSYS_LOG(INFO, "times = %d", times);
  }

  else
  {
    TBSYS_LOG(WARN, "invalid times!!! times=%d", times);
  }

  /* end add by xiaochu */
  if (OB_SUCCESS != (ret = scanner.serialize(out_buffer.get_data(), out_buffer.get_capacity(), out_buffer.get_position())))
  {
    TBSYS_LOG(WARN, "fail to serialize scanner");
  }

  TBSYS_LOG(INFO, "handle scan table result:ret[%d]", ret);
  return ret;
}


class AsyncPacketQueueHandler : public Runnable{
  public:
    void push(ObMsSqlRpcEvent *result, ObPacket* packet)
    {
      packet_queue.push(std::pair<ObMsSqlRpcEvent *, ObPacket*>(result, packet));
      TBSYS_LOG(INFO, "push a packet into result queue [queue.size=%d]", packet_queue.size());
    }

    void run(CThread *thread, void *arg)
    {
      while(1)
      {
        while(packet_queue.size() > 0)
        {
          TBSYS_LOG(INFO, "proccessing packet in the queue [queue.size=%d]", packet_queue.size());
          handle_packet();
          // tuning this timeout param can simulate different situation:
          // fast network, slow network, packet losing network
          usleep(1 * 1000);
        }
        usleep(1000);
      }
    }

    void handle_packet()
    {
      std::pair<ObMsSqlRpcEvent *, ObPacket*> pair;
      pair = packet_queue.front();
      packet_queue.pop();
      if (NULL != pair.first)
      {
        pair.first->handlePacket(pair.second, NULL);
        TBSYS_LOG(INFO, "event->handlePacket called [queue.size=%d]", packet_queue.size());
      }
      else
      {
        TBSYS_LOG(WARN, "invalid event pointer. is NULL!!!");
      }
    }
  private:
    std::queue< std::pair<ObMsSqlRpcEvent *, ObPacket*> > packet_queue;
};


AsyncPacketQueueHandler handler;

int handle_async_request(ObMsSqlRpcEvent & result)
{
  int rc = OB_SUCCESS;
  ObPacket* packet = new (std::nothrow) ObPacket();
  if (NULL == packet)
  {
    rc = OB_ALLOCATE_MEMORY_FAILED;
  }
  else
  {
    ObDataBuffer in_buffer;
    int64_t data_len = 2 * 1024 * 1024;
    char *data = (char *)ob_malloc(data_len);
    const int32_t pcode = OB_SCAN_RESPONSE;
    const int32_t version = 1;
    const int64_t session_id = 0;
    const int64_t timeout = 1000000;
    in_buffer.set_data(data, data_len);
    // fill scanner to buffer
    handle_scan_table(in_buffer);
    packet->set_packet_code(pcode);
    packet->setChannelId(0);
    packet->set_source_timeout(timeout);
    packet->set_session_id(session_id);
    packet->set_api_version(version);
    packet->set_data(in_buffer);
    rc = packet->serialize();
    result.set_req_type(ObMergerRpcEvent::SCAN_RPC);
    result.set_result_code(OB_SUCCESS);
    if (rc != OB_SUCCESS)
    {
      TBSYS_LOG(WARN, "packet serialize error, error: %d", rc);
    }
    handler.push(&result, packet);
    //result.handlePacket(packet, NULL);
  }
  return OB_SUCCESS;
}


int callback_scan_async(Unused, Unused,Unused,ObMsSqlRpcEvent & result)
{
  TBSYS_LOG(WARN, "callback invoiked~ <SCAN> ~~~~~~~~~~~");
  handle_async_request(result);
  return OB_SUCCESS;
}

int callback_session_async(Unused, Unused, Unused, Unused, ObMsSqlRpcEvent & result)
{
  TBSYS_LOG(WARN, "callback invoiked~ <SESSION> ~~~~~~~~~~~~~~~~~");
  handle_async_request(result);
  return OB_SUCCESS;
}



int callback_func(Unused, Unused,Unused,Unused,Unused,  tbnet::IPacketHandler* handler, void* args)
{
  TBSYS_LOG(WARN, "callback invoiked~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
  TBSYS_LOG(WARN, "%p, %p", handler, args);
  EXPECT_NE((tbnet::IPacketHandler*)NULL, handler);
  handler->handlePacket(NULL, args);
  return OB_SUCCESS;
}


TEST_F(TestObRpcScan, test_single_table_scan)
{
  /// 0. start packet handler thread
  CThread packet_hanlder_thread;
  packet_hanlder_thread.start(&handler, NULL);

  /// 1. setup transport
  /// 2. setup servers (root, ups, CSes)
  /// 3. setup scan request (location list, scan range)

  TBSYS_LOG(INFO, "( 1 )");
  tbnet::Transport * transport = new tbnet::Transport();
  tbnet::DefaultPacketStreamer * streamer = new tbnet::DefaultPacketStreamer();
  ObPacketFactory * factory = new ObPacketFactory();
  streamer->setPacketFactory(factory);
  ObClientManager client_manager;
  client_manager.initialize(transport, streamer);
  transport->start();
#if 0
  EXPECT_CALL(client_manager, post_request(_,_,_,_,_,_,_))
    .Times(AtLeast(1))
    .WillRepeatedly(Invoke(callback_func));
  EXPECT_CALL(client_manager, handlePacket(_,_))
    .Times(AtLeast(1))
    .WillOnce(Return(OB_SUCCESS))
    .WillOnce(Return((tbnet::IPacketHandler::HPRetCode)0));
#endif

  TBSYS_LOG(INFO, "( 2 )");
  ObServer update_server;
  ObServer root_server;
  ObServer merge_server;
  update_server.set_ipv4_addr(addr, MockUpdateServer::UPDATE_SERVER_PORT);
  root_server.set_ipv4_addr(addr, MockRootServer::ROOT_SERVER_PORT);
  ObMergerRpcProxy proxy(3, timeout, root_server, merge_server);

  ObMergerRpcStub stub;
  ThreadSpecificBuffer buffer;
  stub.init(&buffer, &client_manager);
  ObMergerRootRpcProxy rpc(0, timeout, root_server);
  EXPECT_TRUE(OB_SUCCESS == rpc.init(&stub));

  merge_server.set_ipv4_addr(addr, 10256);
  ObMergerTabletLocationCache * location = new ObMergerTabletLocationCache;
  location->init(50000 * 5, 1000, 100000);

  TBSYS_LOG(INFO, "( 3 )");
  // start root server
  MockRootServer root;
  tbsys::CThread root_server_thread;
  MockServerRunner test_root_server(root);
  root_server_thread.start(&test_root_server, NULL);

  /// (4) do it!
  MockObMergerAsyncRpcStub async;
  async.init(&buffer, &client_manager);
  ObMergerLocationCacheProxy location_proxy(root_server, &rpc, location);
  ObRpcScan scan;
  ObRowkeyInfo rowkey_info;
  ObRowkeyColumn column;
  column.length_ = 4;
  column.column_id_ = 102;
  column.type_ = ObIntType;
  rowkey_info.add_column(column);
#if 0
  ObSqlExpression p, p_end;
  ExprItem item_a, item_op, item_const;
  bool is_cond = false;
  /* 101 <= column <= 107 */
  item_a.type_ = T_REF_COLUMN;
  item_a.value_.cell_.tid = 123;
  item_a.value_.cell_.cid = 102;
  item_op.type_ = T_OP_LT;
  item_op.value_.int_ = 2;  /* 2 operands */
  item_const.type_ = T_INT;
  item_const.value_.int_ = 107;
  ASSERT_EQ(OB_SUCCESS, p.add_expr_item(item_a));
  ASSERT_EQ(OB_SUCCESS, p.add_expr_item(item_const));
  ASSERT_EQ(OB_SUCCESS, p.add_expr_item(item_op));
  ASSERT_EQ(OB_SUCCESS, p.add_expr_item_end());
  ASSERT_EQ(OB_SUCCESS, p.is_simple_condition(is_cond));
  ASSERT_EQ(true, is_cond);


  item_a.type_ = T_REF_COLUMN;
  item_a.value_.cell_.tid = 123;
  item_a.value_.cell_.cid = 102;
  item_op.type_ = T_OP_GE;
  item_op.value_.int_ = 2;  /* 2 operands */
  item_const.type_ = T_INT;
  item_const.value_.int_ = 101;
  ASSERT_EQ(OB_SUCCESS, p_end.add_expr_item(item_a));
  ASSERT_EQ(OB_SUCCESS, p_end.add_expr_item(item_const));
  ASSERT_EQ(OB_SUCCESS, p_end.add_expr_item(item_op));
  ASSERT_EQ(OB_SUCCESS, p_end.add_expr_item_end());
  ASSERT_EQ(OB_SUCCESS, p_end.is_simple_condition(is_cond));
  ASSERT_EQ(true, is_cond);

  scan.add_filter(p);
  scan.add_filter(p_end);
#endif
  scan.set_table(test::ObFakeTable::TABLE_ID);
  scan.set_rowkey_info(rowkey_info);
  scan.add_output_column(ExprGen::create_expr_by_id(1));

  EXPECT_CALL(async, scan(_,_,_,_))
    .Times(AtLeast(1))
    .WillRepeatedly(Invoke(callback_scan_async));

  EXPECT_CALL(async, get_session_next(_,_,_,_,_))
    .Times(AtLeast(1))
    .WillRepeatedly(Invoke(callback_session_async));




  ASSERT_TRUE(OB_SUCCESS == scan.init(&location_proxy, &async));
  ASSERT_TRUE(OB_SUCCESS == scan.open());

  const ObRow * cur_row = NULL;
  while(1)
  {
    int ret = OB_SUCCESS;

    TBSYS_LOG(INFO, "get_next_row() - begin");
    if (OB_ITER_END == (ret = scan.get_next_row(cur_row)))
    {
      TBSYS_LOG(INFO, "get_next_row() - iter_end");
      break;
    }
    else if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "get next row failed. ret=%d", ret);
      break;
    }
    /*TBSYS_LOG(DEBUG, "[id:%lu][key:%.*s][obj:dumped bellow]", cur_row->table_id_, cur_row->row_key_.length(),
      cur_row->row_key_.ptr());
      cur_row->value_.dump();
      */
    if (NULL != cur_row)
    {
      TBSYS_LOG(INFO, "row info:[SEE_DEBUG_LEVEL]");
      cur_row->dump();
    }
    else
    {
      TBSYS_LOG(WARN, "no current row");
    }
    TBSYS_LOG(INFO, "get_next_row() - end");
  }
  TBSYS_LOG(INFO, "scannnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnning - terminatet");
  //sleep(10);

  scan.close();

  usleep(1000 * 1000 * 2);
  TBSYS_LOG(INFO, "( request sent )");
  transport->stop();
  test_root_server.~MockServerRunner();
  root_server_thread.join();
}
/*
   };
   };
   };
   */


int main(int argc, char **argv)
{
  ob_init_memory_pool();
  TBSYS_LOGGER.setLogLevel("INFO");
  //TBSYS_LOGGER.setLogLevel("DEBUG");
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
