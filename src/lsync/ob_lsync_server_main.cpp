/**
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_lsync_server_main.cpp
 *
 * Authors:
 *   yuanqi.xhf <yuanqi.xhf@taobao.com>
 *
 */

#include <getopt.h>
#include "ob_lsync_server_main.h"

using namespace oceanbase::common;

const char* svn_version();
const char* build_date();
const char* build_time();

namespace oceanbase
{
  namespace lsync
  {
    // static void set_log_level(int level)
    // {
    //   if (level < TBSYS_LOG_LEVEL_ERROR)
    //     level = TBSYS_LOG_LEVEL_ERROR;
    //   if (level > TBSYS_LOG_LEVEL_DEBUG)
    //     level = TBSYS_LOG_LEVEL_DEBUG;
    //   TBSYS_LOGGER._level = level;
    // }

    ObLsyncServerMain* ObLsyncServerMain::get_instance()
    {
      if (NULL == instance_)
      {
        instance_ = new (std::nothrow) ObLsyncServerMain();
      }

      return dynamic_cast<ObLsyncServerMain*>(instance_);
    }

    int ObLsyncServerMain::do_work()
    {
      int err = OB_SUCCESS;

      if (OB_SUCCESS != (err = param_.load_from_file(config_)))
      {
        TBSYS_LOG(WARN, "load_from_file('%s')=>%d", config_, err);
      }
      else
      {
        param_.merge(cmd_line_param_);
        if (!param_.check())
        {
          param_.print("not valid param");
          print_usage(app_name_);
          err = OB_INVALID_ARGUMENT;
        }
      }

      if (OB_SUCCESS == err)
      {
        param_.print("final param");
        err = server_.initialize(param_.get_commit_log_dir(), param_.get_log_file_start_id(), param_.get_dev_name(), param_.get_port(), param_.get_timeout(), param_.get_convert_switch_log(), param_.get_lsync_retry_wait_time_us());
        if (OB_SUCCESS != err)
        {
          TBSYS_LOG(WARN, "ObLsyncServer.initialize()=>%d", err);
        }
      }
      if (OB_SUCCESS == err)
      {
        server_.start(true);
        server_.stop();
        server_.wait();
      }
      return err;
    }

    void ObLsyncServerMain::do_signal(const int sig)
    {
      switch (sig)
      {
        case SIGTERM:
        case SIGINT:
          server_.stop();
          TBSYS_LOG(INFO, "KILLED by signal, sig=%d", sig);
          break;
        default:
          break;
      }
    }
    void ObLsyncServerMain::parse_cmd_line(const int argc,  char* const* argv)
    {
      int opt = 0;
      const char* opt_string = "hNVf:d:s:t:D:p:C";
      struct option longopts[] =
      {
        {"config_file", 1, NULL, 'f'},
        {"help", 0, NULL, 'h'},
        {"version", 0, NULL, 'V'},
        {"no_deamon", 0, NULL, 'N'},
        {"log_dir", 1, NULL, 'd'},
        {"start", 1, NULL, 's'},
        {"timeout", 1, NULL, 't'},
        {"dev", 1, NULL, 'D'},
        {"port", 1, NULL, 'p'},
        {"convert-switch-log", 0, NULL, 'C'},
        {0, 0, 0, 0}
      };

      app_name_ = argv[0];
      const char* rest_argv[32];
      int rest_idx = 1;
      rest_argv[0] = argv[0];
      while((opt = getopt_long(argc, argv, opt_string, longopts, NULL)) != -1
            && rest_idx + 2 < (int)ARRAYSIZEOF(rest_argv))
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
        case 'd':
          cmd_line_param_.set_commit_log_dir(optarg);
          break;
        case 's':
          cmd_line_param_.set_log_file_start_id(atol(optarg));
          break;
        case 't':
          cmd_line_param_.set_timeout(atol(optarg));
          break;
        case 'D':
          cmd_line_param_.set_dev_name(optarg);
          break;
        case 'p':
          cmd_line_param_.set_port(atoi(optarg));
          break;
        case 'C':
          cmd_line_param_.set_convert_switch_log(1);
          break;
        default:
          break;
        }
      }

      optind = 1;
      BaseMain::parse_cmd_line(rest_idx, const_cast<char* const*>(rest_argv));
    }

    void ObLsyncServerMain::print_usage(const char *prog_name)
    {
      const char* usages = "Usages:\n"
        "    %1$s --log-dir=v0.1-ups-log-dir --start=v0.1-ups-log-start-seq-id --dev=bond0 --port 2600 --convert-switch-log\n";
      fprintf(stderr, "\nNote: Options Can be set both in config file and cmdline(cmdline override config file).\n");
      fprintf(stderr, usages, prog_name);
      fprintf(stderr, "\nCommon Options:\n");
      BaseMain::print_usage(prog_name);
    }
    void ObLsyncServerMain::print_version()
    {
      fprintf(stderr, "LsyncServer.\nsvn version: %s\nBuild time: %s %s\n", svn_version(), build_date(), build_time());
    }
  }
}

