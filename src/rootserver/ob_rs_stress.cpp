/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 * 
 * Version: $Id$
 *
 * ob_rs_stress.cpp
 * rootserver stress testing tool
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#include "common/ob_define.h"
#include "common/ob_version.h"
#include "common/ob_malloc.h"
#include "common/ob_server.h"
#include "common/ob_base_client.h"
#include "common/ob_atomic.h"
#include "rootserver/ob_root_rpc_stub.h"
#include "tbsys.h"
#include <cstdio>
using namespace oceanbase::common;
using namespace oceanbase::rootserver;

struct MyArguments
{
  const char* rs_host;
  int rs_port;
  int batch_count;
  int thread_count;
  int64_t timeout_us;
  public:
    MyArguments();
    bool check();
  private:    
    static const char* DEFAULT_RS_HOST;
    static const int DEFAULT_RS_PORT = 2500;
    static const int DEFAULT_THREAD_COUNT = 20;
    static const int64_t DEFAULT_TIMEOUT_US = 100000; // 100ms
};

const char* MyArguments::DEFAULT_RS_HOST = "127.0.0.1";
inline MyArguments::MyArguments()
{
  rs_host = DEFAULT_RS_HOST;
  rs_port = DEFAULT_RS_PORT;
  batch_count = 0;
  thread_count = DEFAULT_THREAD_COUNT;
  timeout_us = DEFAULT_TIMEOUT_US;
}

bool MyArguments::check()
{
  bool ret = true;
  if (0 >= batch_count)
  {
    printf("`batch_count' should be greater than 0, batch_count=%d\n", batch_count);
    ret = false;
  }
  else if (0 >= thread_count)
  {
    printf("`thread_count' should be greater than 0, thread_count=%d\n", thread_count);
    ret = false;
  }
  else if (0 >= timeout_us)
  {
    printf("`timeout_us' should be greater than 0, thread_count=%ld\n", timeout_us);
    ret = false;
  }
  return ret;
}

class MyMain
{
  public:
    MyMain();
    virtual ~MyMain();
    void usage();
    void version();
    int parse_cmd_line(int argc, char* argv[], MyArguments &args);
  private:
    // disallow copy
    MyMain(const MyMain &other);
    MyMain& operator=(const MyMain &other);
  private:
    // data members
    static const char* PROG_NAME;
};

const char* MyMain::PROG_NAME = "rs_stress";
MyMain::MyMain()
{
}

MyMain::~MyMain()
{
}

void MyMain::usage()
{
  printf("\nUsage: %s -r <rootserver_ip> -p <rootserver_port> <options>\n", PROG_NAME);
  printf("\n\t-r <rootserver_ip>\tthe default is `127.0.0.1'\n");
  printf("\t-p <rootserver_port>\tthe default is 2500\n");
  printf("\t-b <batch_count>\n");
  printf("\t-t <thread_count>\n");
  printf("\t-T <timeout_us>\n");
  printf("\n\t-h\tprint this help message\n");
  printf("\t-V\tprint the version\n");
}

void MyMain::version()
{
  printf("%s (%s %s)\n", PROG_NAME, PACKAGE_STRING, RELEASEID);
  printf("SVN_VERSION: %s\n", svn_version());
  printf("BUILD_TIME: %s %s\n\n", build_date(), build_time());
  printf("Copyright (c) 2007-2011 Taobao Inc.\n");
}

int MyMain::parse_cmd_line(int argc, char* argv[], MyArguments &args)
{
  int ret = OB_SUCCESS;
  int ch = -1;
  while(-1 != (ch = getopt(argc, argv, "hVr:p:b:t:T:")))
  {
    switch(ch)
    {
      case '?':
        usage();
        exit(-1);
        break;
      case 'h':
        usage();
        exit(0);
        break;
      case 'V':
        version();
        exit(0);
        break;
      case 'r':
        args.rs_host = optarg;
        break;
      case 'p':
        args.rs_port = atoi(optarg);
        break;
      case 'b':
        args.batch_count = atoi(optarg);
        break;
      case 't':
        args.thread_count = atoi(optarg);
        break;
      case 'T':
        args.timeout_us = atoi(optarg);
        break;
      default:
        break;
    } // end switch
  } // end while

  if (!args.check())
  {
    usage();
    ret = OB_INVALID_ARGUMENT;
  }
  return ret;
}

class ObRsStressRunnable : public tbsys::CDefaultRunnable
{
  public:
    ObRsStressRunnable(ObRootRpcStub &rpc_stub, ObServer &rs, MyArguments &args);
    virtual ~ObRsStressRunnable();
    virtual void run(tbsys::CThread* thread, void* arg);
    static uint64_t get_succ_count();
    static uint64_t get_fail_count();
  private:
    static uint64_t succ_count;
    static uint64_t fail_count;
    ObRootRpcStub &rpc_stub_;
    ObServer &rs_;
    MyArguments &args_;
};

uint64_t ObRsStressRunnable::succ_count = 0;
uint64_t ObRsStressRunnable::fail_count = 0;

ObRsStressRunnable::ObRsStressRunnable(ObRootRpcStub &rpc_stub, ObServer &rs, MyArguments &args)
  :rpc_stub_(rpc_stub), rs_(rs), args_(args)
{
}

ObRsStressRunnable::~ObRsStressRunnable()
{
}

uint64_t ObRsStressRunnable::get_succ_count()
{
  return succ_count;
}

uint64_t ObRsStressRunnable::get_fail_count()
{
  return fail_count;
}

void ObRsStressRunnable::run(tbsys::CThread* thread, void* arg)
{
  UNUSED(thread);
  UNUSED(arg);
  int ret = OB_SUCCESS;
  ObiRole obi_role;
  for (int i = 0; i < args_.batch_count; ++i)
  {
    if (OB_SUCCESS != (ret = rpc_stub_.get_obi_role(rs_, args_.timeout_us, obi_role)))
    {
      atomic_inc(&fail_count);
    }
    else
    {
      atomic_inc(&succ_count);
    }
  }
}

int main(int argc, char *argv[])
{
  int ret = OB_SUCCESS;
  ob_init_memory_pool();
  MyMain my_main;
  MyArguments args;
  TBSYS_LOGGER.setFileName("rs_stress.log", true);

  if (OB_SUCCESS != (ret = my_main.parse_cmd_line(argc, argv, args)))
  {
    printf("parse cmd line error, err=%d\n", ret);
  }
  else
  {
    // init
    ObServer server(ObServer::IPV4, args.rs_host, args.rs_port);
    ObBaseClient client;
    ThreadSpecificBuffer tsbuffer;
    ObRootRpcStub rpc_stub;
    if (OB_SUCCESS != (ret = client.initialize(server)))
    {
      printf("failed to init client, err=%d\n", ret);
    }
    else if (OB_SUCCESS != (ret = rpc_stub.init(&client.get_client_mgr(), &tsbuffer)))
    {
      printf("failed to init rpc stub, err=%d\n", ret);
    }
    else
    {
      ObRsStressRunnable threads(rpc_stub, server, args);
      threads.setThreadCount(args.thread_count);
      int64_t begin = tbsys::CTimeUtil::getTime();
      threads.start();
      printf("threads started...\n");
      threads.wait();
      int64_t finish = tbsys::CTimeUtil::getTime();
      int64_t elapsed = (finish - begin)/1000000LL;
      printf("elapsed=%ld count=%lu QPS=%ld failed_count=%lu\n", 
             elapsed, threads.get_succ_count(), 
             0 == elapsed ? 0 : threads.get_succ_count()/elapsed,
             threads.get_fail_count());
    }
    // destroy
    client.destroy();
  }
  return ret;
}

