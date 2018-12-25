#include<gtest/gtest.h>
#include"common/ob_file_service.h"
#include"common/ob_file_client.h"
#include "common/ob_tbnet_callback.h"

using namespace oceanbase;
using namespace oceanbase::common;

static const int32_t server_port = 8515;
ObServer file_server(ObServer::IPV4, "127.0.0.1", server_port);
ObFileClient client;
easy_io_t *eio_ = NULL;
ThreadSpecificBuffer rpc_buffer;
ObClientManager client_mgr;
easy_io_handler_pt client_handler_;


// compare two file
int compare_file(const char* file1, const char* file2)
{
  int ret = OB_SUCCESS;
  const int cmd_length = 1024;
  char cmd[cmd_length];
  int n = snprintf(cmd, cmd_length, "diff %s %s", file1, file2);
  if (n >= cmd_length)
  {
    ret = OB_ERROR;
  }
  if (OB_SUCCESS == ret)
  {
    ret = system(cmd);
  }
  TBSYS_LOG(INFO, "%s %d", cmd, ret);
  return ret;
}

// generate file
void generate_file(const char* path, int32_t size)
{
  const int cmd_length = 1024;
  char cmd[cmd_length];

  int n = snprintf(cmd, cmd_length, "dd if=/dev/urandom of=%s bs=1M count=%d", path, size);
  ASSERT_LT(n, cmd_length);
  ASSERT_EQ(0, system(cmd));
} 

/*TEST(test_ob_file_service, test_case_self)
{
  time_t t = time(NULL);
  char file_path[OB_MAX_FILE_NAME_LENGTH];
  int n = snprintf(file_path, OB_MAX_FILE_NAME_LENGTH, "/tmp/test_ob_file_sesrvice.%ld", t);
  ASSERT_LT(n,OB_MAX_FILE_NAME_LENGTH);
  generate_file(file_path, 10);
  char file_path2[OB_MAX_FILE_NAME_LENGTH];
  t = time(NULL);
  n = snprintf(file_path2, OB_MAX_FILE_NAME_LENGTH, "/tmp/test_ob_file_sesrvice.%ld", t);
  ASSERT_LT(n,OB_MAX_FILE_NAME_LENGTH);
  generate_file(file_path2, 10);
  ASSERT_NE(0, compare_file(file_path, file_path2));
  ASSERT_EQ(0, unlink(file_path));
  ASSERT_EQ(0, unlink(file_path2));
}*/

TEST(test_ob_file_service, send_one_file)
{
  time_t t = time(NULL);
  char src_path[OB_MAX_FILE_NAME_LENGTH];
  char dest_file_name[OB_MAX_FILE_NAME_LENGTH];
  char dest_dir[] = "/tmp";
  int n = snprintf(src_path, OB_MAX_FILE_NAME_LENGTH, "/tmp/test_ob_file_sesrvice.src.%ld", t);
  ASSERT_LT(n,OB_MAX_FILE_NAME_LENGTH);
  generate_file(src_path, 16);
  n = snprintf(dest_file_name, OB_MAX_FILE_NAME_LENGTH, "test_ob_file_sesrvice.dest.%ld", t);
  ASSERT_LT(n,OB_MAX_FILE_NAME_LENGTH);
  int ret = client.send_file(1000*1000, 2048, file_server,
      ObString(0, static_cast<int32_t>(strlen(src_path)), src_path),
      ObString(0, static_cast<int32_t>(strlen(dest_dir)), dest_dir),
      ObString(0, static_cast<int32_t>(strlen(dest_file_name)), dest_file_name));
  ASSERT_EQ(0, ret);

  char dest_path[OB_MAX_FILE_NAME_LENGTH];
  n = snprintf(dest_path,OB_MAX_FILE_NAME_LENGTH, "%s/%s", dest_dir, dest_file_name);
  ASSERT_LT(n,OB_MAX_FILE_NAME_LENGTH);
  ASSERT_EQ(0, compare_file(src_path, dest_path));
  ASSERT_EQ(0, unlink(src_path));
  ASSERT_EQ(0, unlink(dest_path));
}

class ObFileServer : public common::ObSingleServer
{
  public:
    int initialize()
    {
      int ret = OB_SUCCESS;

      set_listen_port(server_port);
      set_dev_name("bond0");
      set_default_queue_size(1000);
      set_thread_count(50);

      memset(&server_handler_, 0, sizeof(easy_io_handler_pt));
      server_handler_.encode = ObTbnetCallback::encode;
      server_handler_.decode = ObTbnetCallback::decode;
      server_handler_.process = process;
      //server_handler_.batch_process = ObTbnetCallback::batch_process;
      server_handler_.get_packet_id = ObTbnetCallback::get_packet_id;
      server_handler_.on_disconnect = ObTbnetCallback::on_disconnect;
      server_handler_.user_data = this;

      ObSingleServer::initialize();
      uint32_t limit_count = 3;

      ret = file_service_.initialize(this, &ObSingleServer::get_default_task_queue_thread(), 100000*1000, limit_count);
      if(OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "init file service fail:ret[%d]", ret);
      }

      return ret;
    }
    static int process(easy_request_t *r)
    {
      int ret = EASY_OK;

      if (NULL == r)
      {
        TBSYS_LOG(WARN, "request is NULL, r = %p", r);
        ret = EASY_BREAK;
      }
      else if (NULL == r->ipacket)
      {
        TBSYS_LOG(WARN, "request is NULL, r->ipacket = %p", r->ipacket);
        ret = EASY_BREAK;
      }
      else
      {
        ObFileServer* server = (ObFileServer*)r->ms->c->handler->user_data;
        ObPacket* packet = (ObPacket*)r->ipacket;
        packet->set_request(r);
        r->ms->c->pool->ref++;
        easy_atomic_inc(&r->ms->pool->ref);
        easy_pool_set_lock(r->ms->pool);
        if (OB_REQUIRE_HEARTBEAT == packet->get_packet_code())
        {
          server->handle_request(packet);
        }
        else
        {
          server->handlePacket(packet);
        }

        ret = EASY_AGAIN;
      }
      return ret;
    }
    int do_request(ObPacket* base_packet)
    {
      int ret = OB_SUCCESS;
      ObPacket* ob_packet = base_packet;
      int32_t packet_code = ob_packet->get_packet_code();
      int32_t version = ob_packet->get_api_version();
      int32_t channel_id = ob_packet->get_channel_id();
      ret = ob_packet->deserialize();

      if (OB_SUCCESS == ret)
      {
        ObDataBuffer* in_buffer = ob_packet->get_buffer();
        if (NULL == in_buffer)
        {
          TBSYS_LOG(ERROR, "%s", "in_buffer is NUll should not reach this");
        }
        else
        {
          ThreadSpecificBuffer::Buffer* thread_buffer =
            response_buffer_.get_buffer();
          if (NULL != thread_buffer)
          {
            thread_buffer->reset();
            ObDataBuffer out_buffer(thread_buffer->current(), thread_buffer->remain());
            //TODO read thread stuff multi thread
            TBSYS_LOG(DEBUG, "handle packet, packe code is %d, packet:%p",
                packet_code, ob_packet);
            ret = file_service_.handle_send_file_request(version, channel_id, ob_packet->get_request() ,
                *in_buffer, out_buffer);
          }
          else
          {
            TBSYS_LOG(ERROR, "%s", "get thread buffer error, ignore this packet");
          }
        }
      }
      return ret;
    }
  private:
    ObFileService file_service_;
    ThreadSpecificBuffer response_buffer_;
    ObPacketFactory packet_factory_;
};

int start_file_service()
{
  ObFileServer server;
  int ret = server.start(true);
  if (OB_SUCCESS != ret)
  {
    TBSYS_LOG(ERROR, "failed to start ob file service thread");
  }
  return (ret);
}

void init_client()
{
  int ret = OB_SUCCESS; 
  int rc = OB_SUCCESS;
  eio_ = easy_eio_create(eio_, 100);
  if (NULL == eio_)
  {
    ret = OB_ERROR;
    TBSYS_LOG(ERROR, "easy_io_create error");
  }
  memset(&client_handler_, 0, sizeof(easy_io_handler_pt));
  client_handler_.encode = ObTbnetCallback::encode;
  client_handler_.decode = ObTbnetCallback::decode;
  client_handler_.get_packet_id = ObTbnetCallback::get_packet_id;
  client_handler_.on_disconnect = ObTbnetCallback::on_disconnect;
  if (OB_SUCCESS != (ret = client_mgr.initialize(eio_, &client_handler_)))
  {
    TBSYS_LOG(ERROR, "failed to init client_mgr, err=%d", ret);
  }
  else
  {
    //start io thread
    if (ret == OB_SUCCESS)
    {
      rc = easy_eio_start(eio_);
      if (EASY_OK == rc)
      {
        ret = OB_SUCCESS;
        TBSYS_LOG(INFO, "start io thread");
      }
      else
      {
        TBSYS_LOG(ERROR, "easy_eio_start failed");
        ret = OB_ERROR;
      }
    }
  }
  client.initialize(&rpc_buffer, &client_mgr, 100000*1024);
}

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc,argv);

  ob_init_memory_pool();
  TBSYS_LOGGER.setLogLevel("INFO");

  int ret = OB_SUCCESS;
  pid_t pid = fork();
  if(pid == 0)
  {
//    daemon(1,1);
    start_file_service();
  }
  else
  {
    init_client();
    sleep(1);
    ret = RUN_ALL_TESTS();
    easy_eio_stop(eio_);
    easy_eio_wait(eio_);
    easy_eio_destroy(eio_);
    kill(pid, SIGTERM);
  }
  sleep(1);

  return ret;
}

