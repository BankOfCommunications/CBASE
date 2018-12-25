/**
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_msync_client_main.cpp
 * 
 * Authors:
 *   yuanqi.xhf <yuanqi.xhf@taobao.com>
 *
 */

#include <getopt.h>
#include "ob_msync_client_main.h"

using namespace oceanbase::common;

const char* svn_version();
const char* build_date();
const char* build_time();

namespace oceanbase
{
  namespace msync
  {
    ObMsyncClientMain* ObMsyncClientMain::get_instance()
    {
      if (NULL == instance_)
      {
        instance_ = new (std::nothrow) ObMsyncClientMain();
      }

      return dynamic_cast<ObMsyncClientMain*>(instance_);
    }

    int ObMsyncClientMain::do_work()
    {
      int err = OB_SUCCESS;
      TBSYS_LOG(DEBUG, "client.initialize(config=%s)=>%d", config_.to_str(), err);
      if (OB_SUCCESS != (err = client_.initialize(config_.schema, config_.log_dir, config_.host, config_.port,
                                                 config_.log_file_start_id, config_.last_log_seq_id,
                                                  config_.timeout, config_.retry_wait_time, config_.max_retry_times, config_.seq_marker)))
      {
        TBSYS_LOG(ERROR, "client.initialize(config=%s)=>%d", config_.to_str(), err);
      }
      else if (OB_SUCCESS != (err = client_.start_sync_mutator()))
      {
        TBSYS_LOG(ERROR, "client.start_send_mutator()=>%d", err);
      }
      else
      {
        client_.wait();
      }
      return err;
    }

    void ObMsyncClientMain::do_signal(const int sig)
    {
      switch (sig)
      {
      case SIGTERM:
      case SIGINT:
        TBSYS_LOG(INFO, "KILLED by signal, sig=%d", sig);
        client_.wait();
        break;
      default:
        break;
      }
    }

    const char* ObMsyncClientMain::parse_cmd_line(const int argc,  char* const* argv)
    {
      int opt = 0;
      const char* opt_string = "hNVf:s:a:p:d:F:S:t:r:R:";
      struct option longopts[] = 
        {
          {"config_file", 1, NULL, 'f'},
          {"help", 0, NULL, 'h'},
          {"version", 0, NULL, 'V'},
          {"no_deamon", 0, NULL, 'N'},
          {"schema", 1, NULL, 's'},
          {"ip", 1, NULL, 'a'},
          {"port", 1, NULL, 'p'},
          {"log-dir", 1, NULL, 'd'},
          {"log-file-start-id", 1, NULL, 'F'},
          {"last-log-seq-id", 1, NULL, 'S'},
          {"timeout", 1, NULL, 't'},
          {"retry-wait-time", 1, NULL, 'r'},
          {"max-retry-times", 1, NULL, 'R'},
          {0, 0, 0, 0}
        };
      app_name_ = argv[0];
      const char* rest_argv[32];
      int rest_idx = 1;
      while((opt = getopt_long(argc, argv, opt_string, longopts, NULL)) != -1)
      {
        switch (opt)
        {
        case 'f':
          rest_argv[rest_idx++] = "-f";
          rest_argv[rest_idx++] = optarg;
          break;
        case 'V':
          rest_argv[rest_idx++] = "-V";
          break;
        case 'h':
          rest_argv[rest_idx++] = "-h";
          break;
        case 'N':
          rest_argv[rest_idx++] = "-N";
          break;
        case 's':
          config_.schema = optarg;
          break;
        case 'a':
          config_.host = optarg;
          break;
        case 'p':
          config_.port = atoi(optarg);
          break;
        case 'd':
          config_.log_dir = optarg;
          break;
        case 'F':
          config_.log_file_start_id = atol(optarg);
          break;
        case 'S':
          config_.last_log_seq_id = atol(optarg);
          break;
        case 't':
          config_.timeout = atol(optarg);
          break;
        case 'r':
          config_.retry_wait_time = atol(optarg);
          break;
        case 'R':
          config_.max_retry_times = atol(optarg);
          break;
        default:
          break;
        }
      }
      if (!config_.check())
      {
        print_usage(app_name_);
        exit(1);
      }

      optind = 1;
      return BaseMain::parse_cmd_line(rest_idx, const_cast<char* const*>(rest_argv));
    }
    
    void ObMsyncClientMain::print_usage(const char *prog_name)
    {
      const char* usages = "Usages:\n"
        "%s -s schema.ini -a ups-addr -p ups-port -d log-dir -F file-start -S seq-start -t wait-response-timeout -r retry-wait-time -R max-retry-times\n";
      fprintf(stderr, usages, prog_name);
      fprintf(stderr, "\nCommon Options:\n");
      BaseMain::print_usage(prog_name);
    }
    void ObMsyncClientMain::print_version()
    {
      fprintf(stderr, "MsyncClient.\nsvn version: %s\nBuild time: %s %s\n", svn_version(), build_date(), build_time());
    }
  }
}

