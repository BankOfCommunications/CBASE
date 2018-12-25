/*
 * (C) 2007-2012 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version:  main.cpp,  02/01/2013 12:30:54 PM xiaochu Exp $
 * 
 * Author:  
 *   xiaochu.yh <xiaochu.yh@taobao.com>
 * Description:  
 *   main 
 * 
 */
#include <signal.h>
#include "get_query_test.h"
#include "tbsys.h"

class BaseMain
{
  public:
    static int start_get_query()
    {
      query.start();
      query.loop();
      return 0;
    }
    static void setup_signal()
    {
      signal(SIGPIPE, SIG_IGN);
      signal(SIGHUP, SIG_IGN);
      signal(SIGINT, add_signal_catched);
      signal(SIGTERM, add_signal_catched);
    }
  private: 
    static void add_signal_catched(const int sig)
    {
      TBSYS_LOG(INFO, "receive sig %d", sig);
      switch(sig)
      {
        case SIGINT:
        case SIGTERM:
          query.stop();
          break;
        default:
          break;
      }
    }
  private:
    static ObGetQueryTest query;
};

ObGetQueryTest BaseMain::query;
char *g_mysql_ip;
int g_mysql_port;

int main(int argc, char **argv)
{
  int ret = 0;
  BaseMain::setup_signal();
  TBSYS_LOGGER.setLogLevel("INFO");
  if (argc > 2)
  {
    g_mysql_ip = argv[1];
    g_mysql_port = atoi(argv[2]);
    TBSYS_LOG(INFO, "connect to %s:%d", g_mysql_ip, g_mysql_port);
    ret = BaseMain::start_get_query();
  }
  else
  {
    ret = -1;
    fprintf(stderr, "usage: %s mysql_server_ip mysql_server_port\n", argv[0]);
    fprintf(stderr, "example: %s 10.232.36.29 4797\n", argv[0]);
  }
  return ret;
}
