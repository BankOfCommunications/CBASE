#include <getopt.h>
#include "gtest/gtest.h"
#include "lsync/ob_lsync_server.h"
#include "common/ob_client_manager.h"
#include "common/ob_base_client.h"
#include "common/ob_malloc.h"

using namespace oceanbase::common;
namespace oceanbase
{
  namespace lsync
  {
    class Config
    {
    public:
      int parse(int argc, char** argv)
        {
          const char* optstr = "p:d:s:t:D:";
          struct option longopts[] = 
            {
              {"port", 1, NULL, 'p'},
              {"log_dir", 1, NULL, 'd'},
              {"start", 1, NULL, 's'},
              {"timeout", 1, NULL, 't'},
              {"dev", 1, NULL, 'D'},
              {0, 0, 0, 0}
            };
          int opt;
          while((opt = getopt_long(argc, argv, optstr, longopts, NULL)) != -1)
          {
            switch(opt){
            case 'p':
              port = atoi(optarg);
              break;
            case 'd':
              log_dir = optarg;
              break;
            case 's':
              start = atol(optarg);
              break;
            case 't':
              timeout = atol(optarg);
              break;
            case 'D':
              dev = optarg;
              break;
            default:
              break;
            }
          }
          fprintf(stderr, "Config{ log_dir: '%s', start: %lu, timeout: %lu, dev: '%s', port: %d };\n",
                  log_dir, start, timeout, dev, port);
          return 0;
        }
      static int port;
      static const char* log_dir;
      static const char* dev;
      static int64_t start;
      static int64_t timeout;
    };
    int Config::port = 2500;
    const char* Config::log_dir = ".";
    const char* Config::dev = "bond0";
    int64_t Config::start = 1;
    int64_t Config::timeout = 1000;

#if 0    
    class ObSeekableLogReaderTest: public ::testing::Test, public Config
    {
    protected:
      virtual void SetUp(){
        reader.initialize(log_dir, timeout);
      }
    protected:
      virtual void TearDown(){
      }
      ObSeekableLogReader reader;
      LogCommand cmd;
      uint64_t seq;
      char* log_data;
      int64_t data_len;
      int err;
    };
    
    TEST_F(ObSeekableLogReaderTest, NormalDump) {
      ASSERT_EQ(reader.seek(1, 0), OB_SUCCESS);
      for (int i = 0; i < (1<<4); i++)
      {
        err = reader.get(cmd, seq, log_data, data_len);
        ASSERT_EQ(err, OB_SUCCESS);
      }
    }

    TEST_F(ObSeekableLogReaderTest, SeekForward) {
      ASSERT_EQ(reader.seek(1, 0), OB_SUCCESS);
      ASSERT_EQ(reader.seek(1, 1), OB_SUCCESS);
//        ASSERT_NE(reader.seek(1, 2), OB_SUCCESS);
      ASSERT_EQ(reader.seek(1, 100), OB_SUCCESS);
      for (int i = 0; i < (1<<4); i++)
      {
        err = reader.get(cmd, seq, log_data, data_len);
        ASSERT_EQ(err, OB_SUCCESS);
        fprintf(stderr, "seq: %lu\n", seq);
      }
      
      ASSERT_EQ(reader.seek(2, 100), OB_ERROR);
      ASSERT_EQ(reader.seek(2, 419400), OB_SUCCESS);
      for (int i = 0; i < (1<<4); i++)
      {
        err = reader.get(cmd, seq, log_data, data_len);
        ASSERT_EQ(err, OB_SUCCESS);
        fprintf(stderr, "seq: %lu\n", seq);
      }

    }

    TEST_F(ObSeekableLogReaderTest, CrossFile) {
      int64_t last_seq_id =  419328;
      ASSERT_EQ(reader.seek(1, last_seq_id-1), OB_SUCCESS);
      err = reader.get(cmd, seq, log_data, data_len);
      ASSERT_EQ(seq, last_seq_id);
      ASSERT_EQ(err, OB_SUCCESS);
      
      ASSERT_EQ(reader.seek(2, last_seq_id+1), OB_SUCCESS);
      for (int i = 0; i < (1<<4); i++)
      {
        err = reader.get(cmd, seq, log_data, data_len);
        ASSERT_EQ(err, OB_SUCCESS);
        fprintf(stderr, "seq: %lu\n", seq);
      }
    }
    
    TEST_F(ObSeekableLogReaderTest, Timeout) {
      ASSERT_NE(reader.seek(1, ~1), OB_SUCCESS);
    }
    
    class ClientManagerStub: public ObClientManager
    {
    public:
      ClientManagerStub()
        {
        }
      ClientManagerStub(const char* host, int port)
        {
          init(host, port);
        }
      ~ClientManagerStub()
        {
          wait();
        }
      int init(const char* host, int port)
        {
          int err;
          this->host = host;
          this->port = port;
          packet_streamer.setPacketFactory(&packet_factory);
          err = initialize(&transport, &packet_streamer);
          if (OB_SUCCESS != err){
            TBSYS_LOG(WARN, "failed to initialize client_manager, err=%d", err);
          } else {
            transport.start();
          }
          return err;
        }
      void wait()
        {
          transport.stop();
          transport.wait();
        }
      int call(const int32_t pcode, char* buf, int len)
        {
          int err;
          const int32_t default_version = 1;
          ObServer server(ObServer::IPV4, host, port);
          ObDataBuffer buffer(buf, len);
          buffer.get_position() = len;
          err = post_request(server, pcode, default_version, buffer);
          if (OB_SUCCESS != err)
          {
            TBSYS_LOG(WARN, "failed to post request, err=%d", err);
          }
          return err;
        }
    private:
      const char* host;
      int port;
      ObPacketFactory packet_factory;
      tbnet::DefaultPacketStreamer packet_streamer;
      tbnet::Transport transport;
    };

    static long _start_sync_server(ObLsyncServer* server)
    {
      return server->start();
    }

    class ObLsyncServerTest: public ::testing::Test, public Config
    {
    protected:
      virtual void SetUp(){
        int ret;
        buf.set_data(cbuf, ARRAYSIZEOF(cbuf));
        ret = sync_server.initialize(log_dir, start, dev, port, timeout);
        ASSERT_EQ(ret, OB_SUCCESS);
        pthread_create(&server_thread, NULL, (void*(*)(void*))_start_sync_server, (void*)&sync_server);
        usleep(500000);
        //ObServer server(ObServer::IPV4, "127.0.0.1", config->port);
        //client.initialize(server);
        client_mgr.init("127.0.0.1", port);
      }
      virtual void TearDown(){
        long ret;
        usleep(500000);
        client_mgr.wait();
        sync_server.stop();
        pthread_join(server_thread, (void**)&ret);
        ASSERT_EQ(OB_SUCCESS, ret);
      }
      const static int MY_VERSION = 1;
      ObLsyncServer sync_server;
      ClientManagerStub client_mgr;
      ObBaseClient client;
      pthread_t server_thread;
      char cbuf[1<<21];
      ObDataBuffer buf;
    };

    TEST_F(ObLsyncServerTest, Client)
    {
      char* msg = "hello";
      strcpy(buf.get_data(), msg);
      buf.get_position() = strlen(msg);
      for(int i = 0; i < 1<<2; i++){
        client.send_recv(OB_SLAVE_REG, MY_VERSION, 1, buf);
      }
    }

    TEST_F(ObLsyncServerTest, Stop)
    {
      // Test server can stop correctly.
    }

    TEST_F(ObLsyncServerTest, InvalidPacket)
    {
      char* msg = "hello,world!";
      for (int i = 0; i < 1<<2; i++)
      {
        client_mgr.call(OB_HEARTBEAT, msg, strlen(msg));
      }
    }

    TEST_F(ObLsyncServerTest, RegisterOk)
    {
      ObServer server(ObServer::IPV4, "127.0.0.1", port);
      ObSlaveRegisterRequest reg_packet = {1,20};
      for (int i = 0; i < 1<<2; i++)
      {
        client_mgr.call(OB_SLAVE_REG, (char*)&reg_packet, sizeof(reg_packet));
      }
    }

    TEST_F(ObLsyncServerTest, RegisterSendLog)
    {
      ObServer server(ObServer::IPV4, "127.0.0.1", port);
      ObSlaveRegisterRequest reg_packet = {1,20};
      for (int i = 0; i < 1<<2; i++)
      {
        //todo
      }
    }
#endif    
  }
}

using namespace oceanbase::lsync;
int main(int argc, char** argv)
{
  int err;
  TBSYS_LOGGER.setLogLevel("INFO");
  err = ob_init_memory_pool();
  if (OB_SUCCESS != err)
    return err;
  ::testing::InitGoogleTest(&argc, argv);
  Config config;
  config.parse(argc, argv);
  return RUN_ALL_TESTS();
}
