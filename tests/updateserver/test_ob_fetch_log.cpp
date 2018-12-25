/**
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * test_ob_log_cache.cpp
 *
 * Authors:
 *   yuanqi.xhf <yuanqi.xhf@taobao.com>
 *
 */

#include "gtest/gtest.h"
#include "common/ob_malloc.h"
#include "common/ob_server_getter.h"
#include "updateserver/ob_ups_rpc_stub.h"
#include "updateserver/ob_log_buffer.h"
#include "updateserver/ob_pos_log_reader.h"
#include "updateserver/ob_cached_pos_log_reader.h"
#include "updateserver/ob_remote_log_src.h"
#include "common/thread_buffer.h"
#include "common/ob_base_client.h"
#include "test_utils2.h"
#include "log_utils.h"
#include "my_ob_server.h"

using namespace oceanbase::common;
using namespace oceanbase::updateserver;

namespace oceanbase
{
  namespace test
  {
    struct Config
    {
      static int64_t MAX_N_DATA_ITEMS;
      bool dio;
      const char* log_dir;
      int64_t log_file_max_size;
      int64_t log_buf_size;
      int64_t n_blocks;
      int64_t block_size_shift;
      int64_t reserved_buf_len;
      int64_t max_num_items;
      int64_t fetch_timeout;
      char* nic;
      char* ip;
      int32_t port;
      Config()
      {
        dio = true;
        log_dir = "log";
        log_file_max_size = 1<<22;
        log_buf_size = 1<<16;
        block_size_shift = 22;
        n_blocks = 1<<2;
        reserved_buf_len = 1<<14;
        max_num_items = MAX_N_DATA_ITEMS;
        fetch_timeout = 100 * 1000;
        nic = (char*)"eth0";
        ip = (char*)"127.0.0.1";
        port = 12342;
      }
    };
    int64_t Config::MAX_N_DATA_ITEMS = 1<<16;


    class ObFetchLogTest: public ::testing::Test, public Config, public MyObServer
    {
      public:
        ObFetchLogTest(){}
        ~ObFetchLogTest(){}
      protected:
        int err;
        MgTool mgt;
        ObLogGenerator log_generator;
        ObLogWriterV2 log_writer;
        ObPosLogReader pos_log_reader;
        ObLogBuffer log_cache;
        ObCachedPosLogReader log_reader;
        ObBaseClient base_client; 
        ObUpsRpcStub rpc_stub;
        ObStoredServer server_getter;
        ObRemoteLogSrc remote_log_src;
        ThreadSpecificBuffer server_log_buffer;
        ThreadSpecificBuffer client_log_buffer;
        ObLogCursor start_cursor;
        ObLogCursor end_cursor;
        common::ThreadSpecificBuffer thread_buffer; //add dragon [repair_test] 2016-12-30
      protected:
        virtual void SetUp(){
          ObServer server;
          ObLogCursor start_cursor;
          server.set_ipv4_addr(ip, port);
          ASSERT_EQ(OB_SUCCESS, base_client.initialize(server));
          ASSERT_EQ(OB_SUCCESS, mgt.sh("rm -rf %s", log_dir));
          srandom(static_cast<int32_t>(time(NULL)));
          ASSERT_EQ(OB_SUCCESS, log_generator.init(log_buf_size, log_file_max_size));
          ASSERT_EQ(OB_SUCCESS, log_generator.start_log(set_cursor(start_cursor, 1, 1, 0)));
          ASSERT_EQ(OB_SUCCESS, log_writer.init(log_dir, LOG_ALIGN-1, OB_LOG_SYNC));
          ASSERT_EQ(OB_SUCCESS, log_writer.start_log(set_cursor(start_cursor, 1, 1, 0)));

          ASSERT_EQ(OB_SUCCESS, pos_log_reader.init(log_dir, dio));
          ASSERT_EQ(OB_SUCCESS, log_cache.init(n_blocks, block_size_shift));
          ASSERT_EQ(OB_SUCCESS, log_reader.init(&pos_log_reader, &log_cache));
          //ASSERT_EQ(OB_SUCCESS, set_dev_name(nic));
          ASSERT_EQ(OB_SUCCESS, set_listen_port(port));

          ASSERT_EQ(OB_SUCCESS, start(false));
          //mod dragon [repair_test] 2016-12-30 b
          ASSERT_EQ(OB_SUCCESS, rpc_stub.init(&base_client.get_client_mgr(), &thread_buffer));
          //ASSERT_EQ(OB_SUCCESS, rpc_stub.init(&base_client.get_client_mgr()));
          //mod e
          ASSERT_EQ(OB_SUCCESS, server_getter.set_server(server));
          ASSERT_EQ(OB_SUCCESS, remote_log_src.init(&server_getter, &rpc_stub, fetch_timeout));
          err = OB_SUCCESS;
        }
        virtual void TearDown(){
          inspect(true);
          fprintf(stderr, "Teardown\n");
          base_client.destroy();
          stop();
          wait();
        }
        void inspect(bool verbose=false){
          fprintf(stderr, "ObPosLogReaderTest{log_buf_size=%ld, max_num_items=%ld}\n",
                  log_buf_size, max_num_items);
          if(verbose)
          {
          }
        }

        int server_fetch_log(ObFetchLogReq& req, ObFetchedLog& result) {
          return log_reader.get_log(req, result);
        }

        int server_fetch_log(ObDataBuffer& inbuf,  ObDataBuffer& outbuf) {
          int err = OB_SUCCESS;
          ObFetchLogReq req;
          ObFetchedLog result;
          ObDataBuffer log_buf;
          get_thread_buffer(server_log_buffer, log_buf);
          result.set_buf(log_buf.get_data(), log_buf.get_capacity());
          if (OB_SUCCESS != (err = req.deserialize(inbuf.get_data(), inbuf.get_capacity(), inbuf.get_position())))
          {
            TBSYS_LOG(ERROR, "req.deserialize(buf=%p[%ld])=>%d", inbuf.get_data(), inbuf.get_capacity(), err);
          }
          else if (OB_SUCCESS != (err = server_fetch_log(req, result)))
          {
            TBSYS_LOG(ERROR, "fetch_log()=>%d", err);
          }
          else if (OB_SUCCESS != (err = result.serialize(outbuf.get_data(), outbuf.get_capacity(), outbuf.get_position())))
          {
            TBSYS_LOG(ERROR, "result.serialize()=>%d", err);
          }
          return err;
        }

        virtual int do_request(int pcode, ObDataBuffer& inbuf,  int& ret_pcode, ObDataBuffer& outbuf){
          int err = OB_SUCCESS;
          UNUSED(inbuf);
          UNUSED(outbuf);
          switch(pcode) {
            case OB_FETCH_LOG:
              ret_pcode = OB_FETCH_LOG_RESPONSE;
              //err = server_fetch_log(inbuf, outbuf);
              break;
            default:
              err = OB_NOT_SUPPORTED;
              break;
          }
          return err;
        }

        int slave_fetch_log(const int64_t start_id, int64_t& end_id) {
          int err = OB_SUCCESS;
          char* buf = NULL;
          int64_t len = 0;
          int64_t read_count = 0;
          ObLogCursor start_cursor;
          ObLogCursor end_cursor;
          start_cursor.log_id_ = start_id;
          if (OB_SUCCESS != (err = get_cbuf(client_log_buffer, buf, len)))
          {
            TBSYS_LOG(ERROR, "get_cbuf()=>%d", err);
          }
          else if (OB_SUCCESS != (err = remote_log_src.get_log(start_cursor, end_cursor, buf, len, read_count)))
          {
            TBSYS_LOG(ERROR, "get_log(start_id=%ld)=>%d", start_id, err);
          }
          else
          {
            end_id = end_cursor.log_id_;
          }
          end_id++;
          return err;
        }
    };

    TEST_F(ObFetchLogTest, Inspect){
      inspect(true);
      stop();
      
    }

    TEST_F(ObFetchLogTest, SessionSerialize){
      char cbuf[1<<10];
      int64_t pos = 0;
      ObSessionBuffer buf1;
      ObSessionBuffer buf2;
      strcpy(buf1.data_, "hello");
      buf1.data_len_ = strlen("hello") + 1;
      ASSERT_EQ(OB_SUCCESS, buf1.serialize(cbuf, sizeof(cbuf), pos));
      hex_dump(cbuf, static_cast<int32_t>(pos));
      pos = 0;
      ASSERT_EQ(OB_SUCCESS, buf2.deserialize(cbuf, sizeof(cbuf), pos));
      ASSERT_EQ(buf1.data_len_, buf2.data_len_);
      ASSERT_TRUE(0 == strcmp(buf1.data_, buf2.data_));
    }

    TEST_F(ObFetchLogTest, FetchLogReqSerialize){
      char cbuf[1<<10];
      int64_t pos = 0;
      ObFetchLogReq req1;
      ObFetchLogReq req2;
      req1.start_id_ = 1;
      req1.max_data_len_ = 2;
      ASSERT_EQ(OB_SUCCESS, req1.serialize(cbuf, sizeof(cbuf), pos));
      hex_dump(cbuf, static_cast<int32_t>(pos));
      pos = 0;
      ASSERT_EQ(OB_SUCCESS, req2.deserialize(cbuf, sizeof(cbuf), pos));
    }

    TEST_F(ObFetchLogTest, FetchedLogSerialize){
      char cbuf[1<<10];
      char log_buf1[1<<10];
      char log_buf2[1<<10];
      int64_t pos = 0;
      ObFetchedLog fetched_log1;
      ObFetchedLog fetched_log2;
      fetched_log1.next_req_.start_id_ = 1;
      fetched_log1.next_req_.max_data_len_ = 2;
      fetched_log1.start_id_ = 3;
      fetched_log1.end_id_ = 4;
      fetched_log1.set_buf(log_buf1, sizeof(log_buf1));
      fetched_log2.set_buf(log_buf2, sizeof(log_buf2));
      ASSERT_EQ(OB_SUCCESS, fetched_log1.serialize(cbuf, sizeof(cbuf), pos));
      hex_dump(cbuf, static_cast<int32_t>(pos));
      pos = 0;
      ASSERT_EQ(OB_SUCCESS, fetched_log2.deserialize(cbuf, sizeof(cbuf), pos));
    }

    TEST_F(ObFetchLogTest, SlaveFetchLog){
      int64_t end_id = 0;
      int64_t n_err = 0;
      ASSERT_EQ(OB_SUCCESS, write_log_to_end(log_writer, log_generator, max_num_items));
      for (int64_t i = 1; i < max_num_items; i = end_id)
      {
        if (OB_SUCCESS != (err = slave_fetch_log(i, end_id)))
        {
          TBSYS_LOG(DEBUG, "read_log(%ld)=>%d", i, err);
          if (OB_NEED_RETRY != err)
            n_err++;
          end_id = i;
        }
      }
      ASSERT_EQ(0, n_err);
      fprintf(stderr, "ENDOFFETCHLO");
      sleep(1);
    }
  } // end namespace updateserver
} // end namespace oceanbase

using namespace oceanbase::test;
int main(int argc, char** argv)
{
  int err = OB_SUCCESS;
  int n_data_items_shift = 0;
  TBSYS_LOGGER.setLogLevel("INFO");
  if (OB_SUCCESS != (err = ob_init_memory_pool()))
    return err;
  ::testing::InitGoogleTest(&argc, argv);
  if(argc >= 2)
  {
    TBSYS_LOGGER.setLogLevel(argv[1]);
  }
  if(argc >= 3)
  {
    n_data_items_shift = atoi(argv[2]);
    if(n_data_items_shift > 4)
      Config::MAX_N_DATA_ITEMS = 1 << n_data_items_shift;
  }
  return RUN_ALL_TESTS();
}
