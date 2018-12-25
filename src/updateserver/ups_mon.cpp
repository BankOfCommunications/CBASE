/**
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * Authors:
 *   yanran <yanran.hfs@taobao.com>
 *     - some work details if you want
 */

#include "ups_mon.h"

#include <signal.h>
#include <getopt.h>
#include <new>

#include "common/ob_malloc.h"
#include "common/utility.h"
#include "easy_io.h"
#include "common/ob_tbnet_callback.h"

using namespace oceanbase::common;
using namespace oceanbase::updateserver;
//add peiouya [OB_PING_SET_PRI_RT] 20141208:b
static int sched_priority = 0;
struct sched_param global_sched_param;
void obping_setscheduler(void);
//add 20141208:e
//modify peiouya [Create_Obping_Logfile] 20141029:b
//const char* UpsMon::DEFAULT_LOG_LEVEL = "WARN";
const char* UpsMon::DEFAULT_LOG_LEVEL = "DEBUG";
//modify 20141029:e
UpsMon* UpsMon::instance_ = NULL;

UpsMon* UpsMon::get_instance() {
  if (NULL == instance_)
  {
    instance_ = new (std::nothrow) UpsMon;
  }
  return instance_;
} 

UpsMon::~UpsMon()
{
}

//add peiouya [OB_PING_SET_PRI_RT] 20141208:b
/**
 *expr:set ob_ping priority RT
 */
void obping_setscheduler(void)
{
    int res = -1;
    sched_priority = sched_get_priority_max (SCHED_RR);
    if (sched_priority != -1)
    {
      global_sched_param.sched_priority = sched_priority;
      res = sched_setscheduler (0, SCHED_RR, &global_sched_param);
    }
}
//add 20141208:e

int UpsMon::start(const int argc, char *argv[])
{
  signal(SIGPIPE, SIG_IGN);
  signal(SIGCHLD, SIG_IGN);
  signal(SIGHUP, SIG_IGN);

  int ret = OB_SUCCESS;

  ret = parse_cmd_line(argc, argv);
  if (OB_SUCCESS != ret)
  {
    print_usage(argv[0]);
  }
  else
  {
    ret = do_work();
  }

  return ret;
}

UpsMon::UpsMon()
{
  new_sync_limit_ = -1;
  set_sync_limit_ = false;
  new_vip_ = 0;
}

void UpsMon::do_signal(const int sig)
{
  UNUSED(sig);
}

void UpsMon::add_signal_catched(const int sig)
{
  signal(sig, UpsMon::sign_handler);
}

void UpsMon::print_usage(const char *prog_name)
{
  fprintf(stderr, "\nUsage: %s -i host -p port\n"
      "    -i, --host         updateserver host name\n"
      "    -p, --port         updateserver port\n"
      "    -l, --limit        update commit log synchronization limit rate\n"
      "    -t, --timeout      ping timeout(us, default %ld)\n"
      "    -e, --loglevel     log level(ERROR|WARN|INFO|DEBUG, default %s)\n"
      "    -h, --help         this help\n"
      "    -V, --version      version\n\n", prog_name, DEFAULT_PING_TIMEOUT_US, DEFAULT_LOG_LEVEL);
}

int UpsMon::parse_cmd_line(const int argc, char *const argv[])
{
  int ret = 0;

  const int NEWVIP_CMD = 200;
  int opt = 0;
  const char* opt_string = "i:p:l:t:e:hV";
  struct option longopts[] = 
  {
    {"host", required_argument, NULL, 'i'},
    {"port", required_argument, NULL, 'p'},
    {"limit", required_argument, NULL, 'l'},
    {"timeout", required_argument, NULL, 't'},
    {"loglevel", required_argument, NULL, 'e'},
    {"help", no_argument, NULL, 'h'},
    {"version", no_argument, NULL, 'V'},
    {"newvip", required_argument, NULL, NEWVIP_CMD},
    {0, 0, 0, 0}
  };

  const char* host = NULL;
  int32_t port = 0;
  const char* log_level = DEFAULT_LOG_LEVEL;
  ping_timeout_us_ = DEFAULT_PING_TIMEOUT_US;

  while((opt = getopt_long(argc, argv, opt_string, longopts, NULL)) != -1) {
    switch (opt) {
      case 'i':
        host = optarg;
        break;
      case 'p':
        port = atoi(optarg);
        break;
      case 'l':
        new_sync_limit_ = atoi(optarg);
        set_sync_limit_ = true;
        break;
      case 't':
        ping_timeout_us_ = atol(optarg);
        break;
      case 'e':
        log_level = optarg;
        break;
      case 'V':
        fprintf(stderr, "BUILD_TIME: %s %s\n\n", __DATE__, __TIME__);
        ret = OB_ERROR;
        break;
      case 'h':
        print_usage(argv[0]);
        ret = OB_ERROR;
        break;
      case NEWVIP_CMD:
        new_vip_ = ObServer::convert_ipv4_addr(optarg);
        break;
    }
  }

  if (NULL == host)
  {
    fprintf(stderr, "\nArgument -i(--host) is required\n\n");
    ret = OB_ERROR;
  }
  else if (0 == port)
  {
    fprintf(stderr, "\nArgument -p(--port) is required\n\n");
    ret = OB_ERROR;
  }
  else if (set_sync_limit_ && new_sync_limit_ <= 0)
  {
    fprintf(stderr, "\nArgument -l(--limit) \"%ld\" is invalid, limit should be positive\n\n", new_sync_limit_);
    ret = OB_ERROR;
  }
  else
  {
    TBSYS_LOGGER.setLogLevel(log_level);
    //add peiouya [Create_Obping_Logfile] 20141029:b
    const char * log_file = "/home/admin/oceanbase/log/ob_ping.log";
    TBSYS_LOGGER.setFileName(log_file,true);
	TBSYS_LOGGER.setMaxFileSize(256 * 1024L * 1024L);  //256M
    //add 20141029:e
    ups_server_.set_ipv4_addr(host, port);

    TBSYS_LOG(INFO, "arguments parse succ, ups=%s:%d, timeout=%ldms, log_level=%s", host, port, ping_timeout_us_, log_level);
  }

  return ret;
}

int UpsMon::do_work()
{
  int ret = OB_SUCCESS;

  if (set_sync_limit_)
  {
    ret = set_sync_limit();
  }
  else
  {
    ret = ping_ups();
  }

  if (0 != new_vip_)
  {
    ret = set_new_vip();
  }

  return ret;
}

int UpsMon::set_sync_limit()
{
  int ret = OB_SUCCESS;

  static const int32_t MY_VERSION = 1;
  const int buff_size = sizeof(ObPacket) + 32;
  char buff[buff_size];
  ObDataBuffer data_buff(buff, buff_size);
  ObBaseClient client;
  int64_t start_time = tbsys::CTimeUtil::getTime();

  ret = client.initialize(ups_server_);

  if (OB_SUCCESS != ret)
  {
    TBSYS_LOG(ERROR, "failed to initialize client, ret=%d", ret);
  }
  else
  {
    ret = serialization::encode_vi64(data_buff.get_data(), data_buff.get_capacity(), data_buff.get_position(), new_sync_limit_);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(ERROR, "encode_vi64 error, new_limit_rate_=%ld", new_sync_limit_);
    }
    else
    {
      ObClientManager& client_mgr = client.get_client_mgr();
      ret = client_mgr.send_request(ups_server_, OB_SET_SYNC_LIMIT_REQUEST, MY_VERSION, ping_timeout_us_, data_buff);
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(ERROR, "failed to send request, ret=%d", ret);
      }
      else
      {
        // deserialize the response code
        int64_t pos = 0;
        ObResultCode result_code;
        ret = result_code.deserialize(data_buff.get_data(), data_buff.get_position(), pos);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR, "deserialize result_code failed, pos=%ld, ret=%d", pos, ret);
        }
        else
        {
          ret = result_code.result_code_;
        }
      }
    }
  }

  int64_t timeu = tbsys::CTimeUtil::getTime() - start_time;
  if (OB_SUCCESS == ret)
  {
    TBSYS_LOG(INFO, "set_new_limit ups(%s) succ, timeu=%ldus", to_cstring(ups_server_), timeu);
  }
  else
  {
    TBSYS_LOG(ERROR, "set_new_limit ups(%s) failed, timeu=%ldus", to_cstring(ups_server_), timeu);
  }

  client.destroy();
  return ret;
}

int UpsMon::set_new_vip()
{
  int ret = OB_SUCCESS;

  static const int32_t MY_VERSION = 1;
  const int buff_size = sizeof(ObPacket) + 32;
  char buff[buff_size];
  ObDataBuffer data_buff(buff, buff_size);
  ObBaseClient client;
  int64_t start_time = tbsys::CTimeUtil::getTime();

  ret = client.initialize(ups_server_);

  if (OB_SUCCESS != ret)
  {
    TBSYS_LOG(ERROR, "failed to initialize client, ret=%d", ret);
  }
  else
  {
    ret = serialization::encode_i32(data_buff.get_data(), data_buff.get_capacity(), data_buff.get_position(), new_vip_);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(ERROR, "encode_i32 error, new_vip_=%d", new_vip_);
    }
    else
    {
      ObClientManager& client_mgr = client.get_client_mgr();
      ret = client_mgr.send_request(ups_server_, OB_UPS_CHANGE_VIP_REQUEST, MY_VERSION, ping_timeout_us_, data_buff);
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(ERROR, "failed to send request, ret=%d", ret);
      }
      else
      {
        // deserialize the response code
        int64_t pos = 0;
        ObResultCode result_code;
        ret = result_code.deserialize(data_buff.get_data(), data_buff.get_position(), pos);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR, "deserialize result_code failed, pos=%ld, ret=%d", pos, ret);
        }
        else
        {
          ret = result_code.result_code_;
        }
      }
    }
  }

  int64_t timeu = tbsys::CTimeUtil::getTime() - start_time;
  if (OB_SUCCESS == ret)
  {
    TBSYS_LOG(INFO, "set_new_vip ups(%s) succ, timeu=%ldus", to_cstring(ups_server_), timeu);
  }
  else
  {
    TBSYS_LOG(ERROR, "set_new_vip ups(%s) failed, timeu=%ldus", to_cstring(ups_server_), timeu);
  }

  client.destroy();
  return ret;
}

int UpsMon::ping_ups()
{
  int ret = OB_SUCCESS;

  static const int32_t MY_VERSION = 1;
  const int buff_size = sizeof(ObPacket);
  char buff[buff_size];
  ObDataBuffer data_buff(buff, buff_size);
  ObBaseClient client;
  int64_t start_time = tbsys::CTimeUtil::getTime();

  ret = client.initialize(ups_server_);

  if (OB_SUCCESS != ret)
  {
    TBSYS_LOG(ERROR, "failed to initialize client, ret=%d", ret);
  }
  else
  {
    ObClientManager& client_mgr = client.get_client_mgr();
    ret = client_mgr.send_request(ups_server_, OB_PING_REQUEST, MY_VERSION, ping_timeout_us_, data_buff);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(ERROR, "failed to send request, ret=%d", ret);
    }
    else
    {
      // deserialize the response code
      int64_t pos = 0;
      ObResultCode result_code;
      ret = result_code.deserialize(data_buff.get_data(), data_buff.get_position(), pos);
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(ERROR, "deserialize result_code failed, pos=%ld, ret=%d", pos, ret);
      }
      else
      {
        ret = result_code.result_code_;
      }
    }
  }

  int64_t timeu = tbsys::CTimeUtil::getTime() - start_time;
  if (OB_SUCCESS == ret)
  {
    TBSYS_LOG(INFO, "ping ups(%s) succ, timeu=%ldus", to_cstring(ups_server_), timeu);
  }
  else
  {
    TBSYS_LOG(ERROR, "ping ups(%s) failed, timeu=%ldus", to_cstring(ups_server_), timeu);
  }

  client.destroy();

  return ret;
}

void UpsMon::sign_handler(const int sig)
{
  UNUSED(sig);
}

int main(int argc, char** argv)
{
  int ret = OB_SUCCESS;
  //add peiouya [OB_PING_SET_PRI_RT] 20141208:b
  obping_setscheduler();
  //add 20141208:e
  ob_init_memory_pool();
  UpsMon *ups_mon = UpsMon::get_instance();
  if (NULL == ups_mon)
  {
    fprintf(stderr, "new UpsMon failed\n");
    ret = OB_ERROR;
  }
  else
  {
    ret = ups_mon->start(argc, argv);
  }

  return ret;
}

