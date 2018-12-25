#include <stdlib.h>
#include <stdio.h>
#include <time.h>
#include <vector>
#include <iostream>
#include "common/ob_define.h"
#include "common/ob_schema.h"
#include "olap_conf.h"
#include "olap.h"
#include "case_list.h"
#include "test_utils.h"
#include "olap_client.h"
using namespace std;
using namespace oceanbase;
using namespace oceanbase::common;

static bool g_stopped = false;
static MockClient g_client;

void stop(int )
{
  g_stopped = true;
}

void usage(char **argv)
{
  fprintf(stderr, "Usage:%s configuration_file\n", argv[0]);
}

void * worker(void * arg)
{
  OlapConf *conf = reinterpret_cast<OlapConf*>(arg);
  int err = OB_SUCCESS;
  uint32_t start_key = 0;
  uint32_t end_key = 0;
  OlapBaseCase * cur_case = NULL;
  ObScanParam *param = NULL;
  ObGetParam * get_param = NULL;
  ObScanner result;
  vector<ObServer> mss;
  mss.resize(conf->get_ms_servers().size());
  void *case_arg = NULL;
  if (NULL == (param = new(std::nothrow)ObScanParam))
  {
    TBSYS_LOG(WARN,"fail to allocate memory for ObScanParam");
    err = OB_ALLOCATE_MEMORY_FAILED;
  }
  if ((OB_SUCCESS == err) && (NULL == (get_param = new(std::nothrow)ObGetParam)))
  {
    TBSYS_LOG(WARN,"fail to allocate memory for ObGetParam");
    err = OB_ALLOCATE_MEMORY_FAILED;
  }
  for (size_t i = 0; (i < conf->get_ms_servers().size()) && (OB_SUCCESS == err); i++)
  {
    mss[i].set_ipv4_addr(conf->get_ms_servers()[i].ip_str_,conf->get_ms_servers()[i].port_);
  }

  int64_t beg = tbsys::CTimeUtil::getTime();
  while ((OB_SUCCESS == err) && !g_stopped)
  {
    cur_case = &random_select_case();
    msolap::gen_key_range(*conf,start_key,end_key);
    uint32_t ms_idx = static_cast<uint32_t>(random()%mss.size());
    if (OlapBaseCase::SCAN == cur_case->get_req_type())
    {
      if ((OB_SUCCESS == err) && (OB_SUCCESS != (err = cur_case->form_scan_param(*param,start_key,end_key, case_arg))))
      {
        TBSYS_LOG(WARN,"fail to form ObScanParam [err:%d]", err);
      }
      if ((OB_SUCCESS == err) 
        && (OB_SUCCESS != ( err = g_client.ups_scan(*param,result, conf->get_network_timeout(), mss[ms_idx]))))
      {
        TBSYS_LOG(WARN,"fail to get result from ms [err:%d,start_key:%u,endkey:%u,case_name:%s,ms_ip:%s,ms_port:%u]", err,start_key, 
          end_key,cur_case->get_name(), conf->get_ms_servers()[ms_idx].ip_str_, conf->get_ms_servers()[ms_idx].port_);
      }
    }
    else
    {
      if ((OB_SUCCESS == err) 
        && (OB_SUCCESS != (err = cur_case->form_get_param(*get_param,conf->get_start_key(), conf->get_end_key(),case_arg))))
      {
        TBSYS_LOG(WARN,"fail to form ObGetParam [err:%d]", err);
      }
      if ((OB_SUCCESS == err)
        && (OB_SUCCESS != (err = g_client.ups_get(*get_param,result,conf->get_network_timeout(),mss[ms_idx]))))
      {
        TBSYS_LOG(WARN,"fail to get result from ms [err:%d,start_key:%u,endkey:%u,case_name:%s,ms_ip:%s,ms_port:%u]", err,start_key, 
          end_key,cur_case->get_name(), conf->get_ms_servers()[ms_idx].ip_str_, conf->get_ms_servers()[ms_idx].port_);
      }
    }

    if ((OB_SUCCESS == err) && !cur_case->check_result(start_key,end_key,result, case_arg))
    {
      TBSYS_LOG(WARN,"case fail [start_key:%u,endkey:%u,case_name:%s,ms_ip:%s,ms_port:%u]", start_key, 
        end_key,cur_case->get_name(), conf->get_ms_servers()[ms_idx].ip_str_, conf->get_ms_servers()[ms_idx].port_);
      err = OB_ERR_UNEXPECTED;
    }
    if (OB_SUCCESS == err)
    {
      int64_t end = tbsys::CTimeUtil::getTime();
      TBSYS_LOG(INFO, "case success [start_key:%u,endkey:%u,case_name:%s,ms_ip:%s,ms_port:%u,used_us:%ld,type:%s]",  start_key, 
        end_key,cur_case->get_name(), conf->get_ms_servers()[ms_idx].ip_str_, conf->get_ms_servers()[ms_idx].port_, 
        end - beg, (cur_case->get_req_type() == OlapBaseCase::SCAN) ? "scan":"get");
      beg = end;
    }
  }

  if (OB_SUCCESS != err)
  {
    stop(0);
  }
  delete param;
  return reinterpret_cast<void*>(err);
}

int main(int argc, char **argv)
{
  srandom(static_cast<uint32_t>(time(NULL)));
  ob_init_memory_pool();
  OlapConf conf;
  int err = OB_SUCCESS;
  if (2 != argc)
  {
    usage(argv);
    err = OB_INVALID_ARGUMENT;
  }
  if (OB_SUCCESS == err)
  {
    signal(SIGINT,stop);
    signal(SIGTERM,stop);
  }

  if ((OB_SUCCESS == err) && (OB_SUCCESS != (err = conf.init(argv[1]))))
  {
    TBSYS_LOG(WARN,"fail to load configuration [err:%d]", err);
  }

  if (OB_SUCCESS != (err =  g_client.init()))
  {
    TBSYS_LOG(WARN,"fail to init MockClient [err:%d]", err);
  }

  vector<pthread_t> read_threads;
  for (int32_t i = 0; (i < conf.get_read_thread_count()) && (OB_SUCCESS == err); i++)
  {
    pthread_t pid;
    pthread_create(&pid, NULL, worker, &conf);
    read_threads.push_back(pid);
  }
  for (int32_t i = 0; (i < conf.get_read_thread_count()) && (OB_SUCCESS == err); i++)
  {
    void *res = NULL;
    pthread_join(read_threads[i], &res);
    err = static_cast<int32_t>(reinterpret_cast<int64_t>(res));
  }
  g_client.destroy();
  return err;
}
