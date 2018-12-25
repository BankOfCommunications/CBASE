#include <getopt.h>

#include "tbnet.h"
#include "common/ob_result.h"
#include "common/ob_define.h"
#include "common/ob_server.h"
#include "common/data_buffer.h"
#include "common/thread_buffer.h"
#include "common/ob_client_manager.h"
#include "common/ob_packet_factory.h"
#include "common/ob_statistics.h"

using namespace oceanbase::common;

class ObMonitorClient 
{
public:
  ObMonitorClient()
  {
    int ret = init();
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(ERROR, "client init failed:ret[%d]", ret);
    }
  }

  virtual ~ObMonitorClient()
  {
    destroy();
  }

private:
  virtual int init();
  virtual int destroy();

public:
  // monitor stat
  virtual int get_stat(const ObServer & server, ObStatManager & stat);
  // clear cache
  virtual int clear_cache(const ObServer & server);

private:
  ThreadSpecificBuffer rpc_buffer_;
  tbnet::DefaultPacketStreamer streamer_;
  tbnet::Transport transport_;
  ObPacketFactory factory_;
  ObClientManager client_;
};

int ObMonitorClient::init()
{
  streamer_.setPacketFactory(&factory_);
  client_.initialize(&transport_, &streamer_);
  return transport_.start();
}

int ObMonitorClient::destroy()
{
  transport_.stop();
  return transport_.wait();
}


int ObMonitorClient::clear_cache(const ObServer & server)
{
  static const int32_t MY_VERSION = 1;
  int64_t timeout = 10 * 1000 * 1000L;
  ObDataBuffer data_buff;
  ThreadSpecificBuffer::Buffer* buffer = rpc_buffer_.get_buffer();
  if(NULL != buffer)
  {
    buffer->reset();
    data_buff.set_data(buffer->current(), buffer->remain());
  } 
  int err = client_.send_request(server, OB_CLEAR_REQUEST, MY_VERSION, timeout, data_buff);
  if (OB_SUCCESS != err)
  {
    TBSYS_LOG(WARN, "failed to send request, err=%d", err);
  }
  else
  {
    int64_t pos = 0;
    if (OB_SUCCESS == err)
    {
      ObResultCode result_code;
      err = result_code.deserialize(data_buff.get_data(), data_buff.get_position(), pos);
      if (OB_SUCCESS != err)
      {
        TBSYS_LOG(ERROR, "deserialize result_code failed:pos[%ld], err[%d].", pos, err);
      }
      else
      {
        err = result_code.result_code_;
      }
    }
  }
  return err; 
}

int ObMonitorClient::get_stat(const ObServer & server, ObStatManager & stat)
{
  static const int32_t MY_VERSION = 1;
  int64_t timeout = 10 * 1000 * 1000L;
  ObDataBuffer data_buff;
  ThreadSpecificBuffer::Buffer* buffer = rpc_buffer_.get_buffer();
  buffer->reset();
  data_buff.set_data(buffer->current(), buffer->remain());
  int err = client_.send_request(server, OB_FETCH_STATS, MY_VERSION, timeout, data_buff);
  if (OB_SUCCESS != err)
  {
    TBSYS_LOG(WARN, "failed to send request, err=%d", err);
  }
  else
  {
    int64_t pos = 0;
    if (OB_SUCCESS == err)
    {
      ObResultCode result_code;
      err = result_code.deserialize(data_buff.get_data(), data_buff.get_position(), pos);
      if (OB_SUCCESS != err)
      {
        TBSYS_LOG(ERROR, "deserialize result_code failed:pos[%ld], err[%d].", pos, err);
      }
      else
      {
        err = result_code.result_code_;
      }
    }
    
    if (OB_SUCCESS == err)
    {
      err = stat.deserialize(data_buff.get_data(), data_buff.get_position(), pos);
      if (OB_SUCCESS != err)
      {
        TBSYS_LOG(ERROR, "deserialize scanner from buff failed, "
            "pos[%ld], err[%d]", pos, err);
      }
    }
  }
  return err;
}

struct ObParam
{
  bool get_stat_;
  char * server_addr_;
  int32_t server_port_; 
};

void print_usage()
{
  fprintf(stderr, "\n");
  fprintf(stderr, "monitor_tool[OPTION]\n");
  fprintf(stderr, "   -a|--serv_addr server address\n");
  fprintf(stderr, "   -p|--serv_port server port\n");
  fprintf(stderr, "   -s|--get_stat get monitor stat\n");
}

int parse_cmd_args(int argc, char **argv, ObParam & param)
{
  int err = OB_SUCCESS;
  const char* opt_string = "a:p:sh";
  
  struct option longopts[] =
  {
    {"serv_addr", 1, NULL, 'a'},
    {"serv_port", 1, NULL, 'p'},
    {0, 0, 0, 0}
  };

  int opt = 0; 
  param.get_stat_ = false;
  while ((opt = getopt_long(argc, argv, opt_string, longopts, NULL)) != -1)
  {
    switch (opt)
    {
    case 'a':
      param.server_addr_ = optarg;
      break;
    case 'p':
      param.server_port_ = atoi(optarg);
      break;
    case 's':
      param.get_stat_ = true;
      break;
    case 'h':
    default:
      print_usage();
      exit(1);
    }
  }

  if ((NULL == param.server_addr_) || (0 == param.server_port_))
  {
    print_usage();
    exit(1);
  }
  return err;
}

int main(int argc, char ** argv)
{
  ObParam param;
  int ret = parse_cmd_args(argc, argv, param);  
  if (OB_SUCCESS == ret)
  {
    ObMonitorClient client;
    ObServer server;
    server.set_ipv4_addr(param.server_addr_, param.server_port_);
    if (param.get_stat_)
    {
      ObStatManager Stat(ObStatManager::SERVER_TYPE_MERGE);
      //TBSYS_LOGGER.setFileName("monitor.log");
      ret = client.get_stat(server, Stat);
      if (ret != OB_SUCCESS)
      {
        TBSYS_LOG(ERROR, "get server stat faild:host[%s], port[%d]", 
            param.server_addr_, param.server_port_);
      }
      else
      {
        ObStatManager::const_iterator it = Stat.begin();
        while (it != Stat.end())
        {
          printf("====table_stat:id[%lu]====\n", it->get_table_id());
          for (int i = 0; i < ObStat::MAX_STATICS_PER_TABLE; ++i)
          {
            printf("===key[%d], value[%ld]===\n", i, it->get_value(i));
          }
          ++it;
        }
      }
    }
    else
    {
      ret = client.clear_cache(server);
      if (ret != OB_SUCCESS)
      {
        TBSYS_LOG(ERROR, "clear cache failed:host[%s], port[%d]",
            param.server_addr_, param.server_port_);
      }
    }
  }
  return ret;
}


