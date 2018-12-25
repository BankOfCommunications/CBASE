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

#include "clog_monitor.h"
#include "stdio.h"
#include "unistd.h"
#include <getopt.h>
#include "common/ob_malloc.h"
#include "common/utility.h"
#include "easy_io.h"
#include <string.h>
#include "common/ob_tbnet_callback.h"
#include <sys/time.h>
#include <time.h>

using namespace oceanbase::common;
using namespace oceanbase::updateserver;

ClogMonitor* ClogMonitor::instance_ = NULL;

ClogMonitor* ClogMonitor::get_instance() {
  if (NULL == instance_)
  {
    instance_ = new (std::nothrow) ClogMonitor;
  }
  return instance_;
} 

ClogMonitor::~ClogMonitor()
{
  base_client_.destroy();
}

int ClogMonitor::start(const int argc, char *argv[])
{
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

ClogMonitor::ClogMonitor(): ups_num_(0)
{

}

void ClogMonitor::print_usage(const char *prog_name)
{
  fprintf(stdout,
          "\n[Oceanbase Commit Log Monitor]\n"
          "Version 0.1 Author@lbzhong\n"
          "Usage: %s -h [host] -p [port]\n"
          "------------------------------------------------\n"
          "  -h, --host   : master ups host IP\n"
          "  -p, --port   : master ups PORT\n"
          "------------------------------------------------\n", prog_name);
}

int ClogMonitor::parse_cmd_line(const int argc, char *const argv[])
{
  int ret = 0;
  int opt = 0;
  const char* opt_string = "h:p:";
  struct option longopts[] = 
  {
    {"host", required_argument, NULL, 'h'},
    {"port", required_argument, NULL, 'p'},
  };
  const char* host = NULL;
  int32_t port = 0;
  while((opt = getopt_long(argc, argv, opt_string, longopts, NULL)) != -1) {
    switch (opt) {
      case 'h':
        host = optarg;
        break;
      case 'p':
        if(!optarg)
        {
          ret = OB_ERROR;
        }
        else
        {
          port = atoi(optarg);
        }
        break;
    }
  }
  if (NULL == host)
  {
    fprintf(stdout, "Argument '-h(--host)' is required\n");
    ret = OB_ERROR;
  }
  else if (0 == port)
  {
    fprintf(stdout, "Argument '-p(--port)' is required\n");
    ret = OB_ERROR;
  }
  else
  {
    common::ObServer ups_server;
    ups_server.set_ipv4_addr(host, port);
    initialize(ups_server);
  }
  return ret;
}

int ClogMonitor::initialize(const ObServer& server)
{
  int ret = OB_SUCCESS;
  if(OB_SUCCESS != (ret = base_client_.initialize(server)))
  {
    TBSYS_LOG(ERROR, "failed to initialize client_, ret=%d", ret);
  }
  else
  {
    master_ups_ = server;
    client_ = base_client_.get_client_mgr();
    timeout_ = 1000 * 1000;
  }
  return ret;
}

int ClogMonitor::do_work()
{
  int ret = OB_SUCCESS;
  printf("========================================================================================================================");
  printf("\n                                          Oceanbase Commit Log Monitor V0.1\n\n");
  if(OB_SUCCESS != (ret = get_ups_list(master_ups_)))
  {
    printf("get_ups_list err!\n");
  }
  else if(OB_SUCCESS != (ret = refresh_moitor()))
  {
    printf("refresh_moitor err!\n");
  }
  return ret;
}

int ClogMonitor::refresh_moitor()
{
  int ret = OB_SUCCESS;
  const int64_t buff_size = 1024 * 1024;
  char buff[buff_size * 2];
  char table_1[buff_size];
  char table_2[buff_size];
  int64_t pos_1 = 0;
  int64_t pos_2 = 0;

  char host_self[22];
  char state[8];
  char host_rs[22];
  char host_ups[22];
  char lease[16];
  char lease_remain[8];
  char role[7];
  char send_receive[13];
  char flush[13];
  char commit_replay[14];
  char commit_point[13];
  char log_term[16];

  struct timeval tv;
  struct tm tm;
  while(true)
  {
    pos_1 = 0;
    pos_2 = 0;
    for(int i = 0; i < ups_num_; i++)
    {
      ClogStatus &status = ups_clog_status_[i];
      if(OB_SUCCESS != get_ups_clog_status(ups_[i], status))
      {
        status.get_timeout_state(state, sizeof(state));
      }
      else
      {
        status.get_state(state, sizeof(state), true);
      }
      status.get_host_self(host_self, sizeof(host_self), true);
      status.get_host_rs(host_rs, sizeof(host_rs), true);
      status.get_host_ups(host_ups, sizeof(host_ups), true);
      status.get_lease(lease, sizeof(lease), true);
      status.get_lease_remain(lease_remain, sizeof(lease_remain), true);
      status.get_role(role, sizeof(role), true);
      status.get_send_receive(send_receive, sizeof(send_receive), true);
      status.get_flush(flush, sizeof(flush), true);
      status.get_commit_replay(commit_replay, sizeof(commit_replay), true);
      status.get_commit_point(commit_point, sizeof(commit_point), true);
      status.get_log_term(log_term, sizeof(log_term), true);

      databuff_printf(table_1, sizeof(table_1), pos_1,
                      "| %s | %s | %s | %s | %s | %s | %s |\n",
                      role, host_self, state, host_rs, host_ups, lease, lease_remain);
      databuff_printf(table_2, sizeof(table_2), pos_2,
                      "| %s | %s | %s | %s | %s | %s |               |\n",
                      host_self, log_term, send_receive, commit_replay, flush, commit_point);
    }
    gettimeofday(&tv, NULL);
    ::localtime_r((const time_t*)&tv.tv_sec, &tm);
    memset(buff, 0, sizeof(buff));
    snprintf(buff, sizeof(buff),
             "                                                 %04d-%02d-%02d %02d:%02d:%02d\n"
             "+--------+-----------------------+---------+-----------------------+-----------------------+-----------------+---------+\n"
             "|  Role  |      Host:Port        |  State  |       Master RS       |       Master UPS      |      Lease      | Remain  |\n"
             "|--------+-----------------------+---------+-----------------------+-----------------------+-----------------|---------|\n%s"
             "+--------+-----------------------+---------+-----------------------+-----------------------+-----------------+---------+\n"
             "\n"
             "+-----------------------+-----------------+--------------+---------------+--------------+--------------+---------------+\n"
             "|      Host:Port        |    Log term     | Send/Receive | Commit/Replay |    Flush     |   Cpoint     |               |\n"
             "|-----------------------+-----------------+--------------+---------------+--------------+--------------+---------------+\n%s"
             "+-----------------------+-----------------+--------------+---------------+--------------+--------------+---------------+\n"
             "========================================================================================================================\n",
             tm.tm_year+1900, tm.tm_mon+1, tm.tm_mday, tm.tm_hour, tm.tm_min, tm.tm_sec, table_1, table_2);
    const char* blank_line =
             "                                                                                                                              \n";
    printf("%s", buff);
    usleep(1000 * 1000);
    int line_num = 0;
    for(uint64_t i = 0; i < strlen(buff); i++)
    {
      if(buff[i] == '\n')
      {
        line_num++;
      }
    }
    printf("\e[%dA\e[K", line_num);
    memset(buff, 0, sizeof(buff));
    int64_t tmp_pos = 0;
    for(int i = 0; i < line_num; i++)
    {
      databuff_printf(buff, sizeof(buff), tmp_pos, blank_line);
    }
    printf("%s", buff); // wipe out window
    printf("\e[%dA\e[K", line_num);
  }
  return ret;
}

int ClogMonitor::get_ups_list(const ObServer &server)
{
  int ret = OB_SUCCESS;
  static const int32_t MY_VERSION = 1;
  const int buff_size = sizeof(ObPacket) + 32;
  char buff[buff_size];
  ObDataBuffer data_buff(buff, buff_size);
  ObString ups_list_str;

  if (OB_SUCCESS != (ret = client_.send_request(server, OB_CLOG_MONITOR_GET_UPS_LIST, MY_VERSION, timeout_, data_buff)))
  {
    TBSYS_LOG(ERROR, "failed to send request, ret=%d", ret);
  }
  else
  {
    // deserialize the response code
    int64_t pos = 0;
    ObResultCode result_code;
    if (OB_SUCCESS != (ret = result_code.deserialize(data_buff.get_data(), data_buff.get_position(), pos)))
    {
      TBSYS_LOG(ERROR, "deserialize result_code failed, pos=%ld, ret=%d", pos, ret);
    }
    else if (OB_SUCCESS != (ret = result_code.result_code_) && OB_NEED_RETRY != ret)
    {
      TBSYS_LOG(ERROR, "result_code is error: result_code_=%d", result_code.result_code_);
    }
    else if (OB_NEED_RETRY == ret)
    {
      TBSYS_LOG(DEBUG, "no data, retry");
    }
    else if (OB_SUCCESS != (ret = ups_list_str.deserialize(data_buff.get_data(), data_buff.get_position(), pos)))
    {
      TBSYS_LOG(ERROR, "ups_list_str.deserialize()=>%d", ret);
    }
    else
    {
      //printf("ups_list_str=%s, len=%ld\n", ups_list_str.ptr(), strlen(ups_list_str.ptr()));
      bool is_master = false;
      char master_ip[MONITOR_MAX_IP_LEN];
      int32_t master_port = 0;

      if(OB_SUCCESS != (ret = parse_ups_list(ups_list_str.ptr(), is_master, master_ip, master_port)))
      {
        TBSYS_LOG(ERROR, "parse_ups_list() fail, msg=%s, ret=%d", ups_list_str.ptr(), ret);
      }
      else
      {
        if(!is_master)
        {
          ObServer master_ups;
          master_ups.set_ipv4_addr(master_ip, master_port);
          master_ups_ = master_ups;
          if(OB_SUCCESS != (ret = get_ups_list(master_ups)))
          {
            TBSYS_LOG(ERROR, "get_ups_list() fail, master_ups=%s, ret=%d", to_cstring(master_ups), ret);
          }
        }
      }
    }
  }
  return ret;
}

int ClogMonitor::get_ups_clog_status(const ObServer &server, ClogStatus &status)
{
  int ret = OB_SUCCESS;
  static const int32_t MY_VERSION = 1;
  const int buff_size = sizeof(ObPacket) + 32;
  char buff[buff_size];
  ObDataBuffer data_buff(buff, buff_size);

  if (OB_SUCCESS != (ret = client_.send_request(server, OB_CLOG_MONITOR_GET_CLOG_STAT, MY_VERSION, timeout_, data_buff)))
  {
    TBSYS_LOG(ERROR, "failed to send request, ret=%d", ret);
  }
  else
  {
    // deserialize the response code
    int64_t pos = 0;
    ObResultCode result_code;
    if (OB_SUCCESS != (ret = result_code.deserialize(data_buff.get_data(), data_buff.get_position(), pos)))
    {
      TBSYS_LOG(ERROR, "deserialize result_code failed, pos=%ld, ret=%d", pos, ret);
    }
    else if (OB_SUCCESS != (ret = result_code.result_code_) && OB_NEED_RETRY != ret)
    {
      TBSYS_LOG(ERROR, "result_code is error: result_code_=%d", result_code.result_code_);
    }
    else if (OB_NEED_RETRY == ret)
    {
      TBSYS_LOG(DEBUG, "no data, retry");
    }
    else if (OB_SUCCESS != (ret = status.deserialize(data_buff.get_data(), data_buff.get_position(), pos)))
    {
      TBSYS_LOG(ERROR, "stat.deserialize()=>%d", ret);
    }
    else
    {
      //printf("->%s\n", to_cstring(status));
    }
  }
  return ret;
}

int ClogMonitor::parse_ups_list(const char* str, bool &is_master, char* master_ip, int32_t &master_port)
{
  int ret = OB_SUCCESS;
  int64_t len = strlen(str);
  int64_t start_pos = 2;
  if(str[0] == 's')
  {
    is_master = true;
    ups_num_ = 0;
    ups_[ups_num_++] = master_ups_; // put master ups in ups_[0]
    char ups_ips[MONITOR_MAX_UPS_NUM][MONITOR_MAX_IP_LEN];
    int64_t end_pos = start_pos;
    int i = 0;
    while(start_pos < len)
    {
      const char* p = strchr(str + start_pos, ',');
      if(NULL == p)
      {
        ret = OB_ERROR;
        break;
      }
      end_pos = p - str;
      memset(ups_ips[i], 0, sizeof(ups_ips[i]));
      memcpy(ups_ips[i], str + start_pos, end_pos - start_pos);
      const char* tmp_p = strchr(ups_ips[i], ':');
      if(NULL == tmp_p)
      {
        ret = OB_ERROR;
        break;
      }
      char ip[MONITOR_MAX_IP_LEN];
      char port_str[10];
      int32_t port = 0;
      memset(port_str, 0, sizeof(port_str));
      memcpy(ip, ups_ips[i], tmp_p - ups_ips[i]);
      memcpy(port_str, tmp_p + 1, strlen(ups_ips[i]) - (tmp_p - ups_ips[i]) - 1);
      port = atoi(port_str);
      //printf("%d: ip=%s, port=%d\n", i, ip, port);
      if(port > 0 && ups_num_ < MONITOR_MAX_UPS_NUM)
      {
        ObServer server;
        server.set_ipv4_addr(ip, port);
        ups_[ups_num_++] = server;
      }
      else
      {
        ret = OB_ERROR;
        TBSYS_LOG(ERROR, "add ups list fail, server.ip=%s, server.port=%d, ups_num=%d", ip, port, ups_num_);
        break;
      }
      start_pos = end_pos + 1;
      i++;
    }
  }
  else if(str[0] == 'm')
  {
    is_master = false;
    const char* p = strchr(str + start_pos, ':');
    if(NULL == p)
    {
      ret = OB_ERROR;
    }
    else
    {
      char port_str[10];
      memset(master_ip, 0, sizeof(master_ip));
      memset(port_str, 0, sizeof(port_str));
      memcpy(master_ip, str + start_pos, p - str - start_pos);
      memcpy(port_str, p + 1, len - (p - str) - 1);
      master_port = atoi(port_str);
      //printf("ip=%s, port=%d\n", master_ip, master_port);
    }
  }
  else
  {
    ret = OB_ERROR;
  }
  return ret;
}

int ClogMonitor::remove_server(const ObServer &server)
{
  int ret = OB_SUCCESS;
  ObServer ups[MONITOR_MAX_UPS_NUM];
  int32_t ups_num = 0;
  for(int i = 0; i < ups_num_; i++)
  {
    if(ups_[i] != server)
    {
      ups[i] = ups_[i];
      ups_num++;
    }
  }
  ups_num_ = 0;
  for(int i = 0; i < ups_num; i++)
  {
    ups_[i] = ups[i];
    ups_num_++;
  }
  return ret;
}

int main(int argc, char** argv)
{
  int ret = OB_SUCCESS;
  fclose(stderr);
  TBSYS_LOGGER.setLogLevel("INFO");
  TBSYS_LOGGER.setFileName("clog_monitor.log", true);
  TBSYS_LOGGER.setMaxFileSize(256 * 1024L * 1024L);  //256M
  TBSYS_LOG(INFO,"-------------------------------------------");
  TBSYS_LOG(INFO,"        Oceanbase Commit Log Monitor");
  TBSYS_LOG(INFO,"-------------------------------------------");
  ob_init_memory_pool();
  ClogMonitor *clog_mon = ClogMonitor::get_instance();
  if (NULL == clog_mon)
  {
    fprintf(stdout, "start [Commit Log Monitor] failed\n");
    ret = OB_ERROR;
  }
  else
  {
    ret = clog_mon->start(argc, argv);
  }
  return ret;
}

