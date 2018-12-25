/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 *
 * Version: 0.1: ob_update_server_main.cpp,v 0.1 2010/09/28 13:33:44 chuanhui Exp $
 *
 * Authors:
 *   chuanhui <rizhao.ych@taobao.com>
 *     - some work details if you want
 *
 */

#include "ob_update_server_main.h"
#include "stress.h"
using namespace oceanbase::common;

namespace oceanbase
{
  namespace updateserver
  {
    ObUpdateServerMain::ObUpdateServerMain()
      : BaseMain(), config_mgr_(ups_config_, ups_reload_config_), server_(config_mgr_, ups_config_, shadow_server_),
        shadow_server_(&server_)
    {
      shadow_server_.set_priority(LOW_PRI);
    }

    ObUpdateServerMain* ObUpdateServerMain::get_instance()
    {
      if (NULL == instance_)
      {
        instance_ = new (std::nothrow) ObUpdateServerMain();
      }

      return dynamic_cast<ObUpdateServerMain*>(instance_);
    }

    int ObUpdateServerMain::do_work()
    {
      int ret = OB_SUCCESS;
      char dump_config_path[OB_MAX_FILE_NAME_LENGTH];

      print_version();
      ups_config_.init();

      ups_reload_config_.set_update_server(server_);

      snprintf(dump_config_path,
               sizeof dump_config_path, "etc/%s.config.bin", server_name_);
      config_mgr_.set_dump_path(dump_config_path);
      add_signal_catched(SIG_RESET_MEMORY_LIMIT);
      add_signal_catched(SIG_INC_WORK_THREAD);
      add_signal_catched(SIG_DEC_WORK_THREAD);
      add_signal_catched(SIG_START_STRESS);
      if (OB_SUCCESS != (ret = config_mgr_.base_init()))
      {
        TBSYS_LOG(ERROR, "init config manager error, ret: [%d]", ret);
      }
      else if (OB_SUCCESS != (ret = config_mgr_.load_config(config_)))
      {
        TBSYS_LOG(ERROR, "load config error, path: [%s], ret: [%d]",
                  config_, ret);
      }


      //add wangjiahao [Paxos ups_replication] 20150601 :b
      if (cmd_ups_quorum_scale_ > 0){
        ups_config_.quorum_scale = cmd_ups_quorum_scale_;
      }
      //add:e      
      //add pangtianze [Paxos Cluster.Balance] 20161124:b
      if (cmd_cluster_id_ < 0 || cmd_cluster_id_ > OB_MAX_CLUSTER_ID)
      {
        ret = OB_CLUSTER_ID_ERROR;
        TBSYS_LOG(ERROR, "Unexpected cluster id=%d, should be (0 < cluster_id <= %d)",
                  cmd_cluster_id_, OB_MAX_CLUSTER_ID);
      }
      else if (cmd_cluster_id_ != 0)
      {
        ups_config_.cluster_id = static_cast<int64_t>(cmd_cluster_id_);
      }
      //add bingo [Paxos server start bugfix] 20170321:b
      else if(cmd_cluster_id_ == 0 && ups_config_.cluster_id == 0){
        ret = OB_CLUSTER_ID_ERROR;
        TBSYS_LOG(ERROR, "Unexpected cluster id=%d, should be (0 < cluster_id <= %d)",
                  cmd_cluster_id_, OB_MAX_CLUSTER_ID);
      }
      //add:e
      //add:e
      /* set ups ip and port if command line has past in */
      if (strlen(cmd_rs_ip_) > 0 && cmd_rs_port_ > 0)
      {
        ups_config_.root_server_ip.set_value(cmd_rs_ip_);
        ups_config_.root_server_port = cmd_rs_port_;
      }
      if (cmd_port_ > 0)
      {
        ups_config_.port = cmd_port_;
      }
      if (cmd_inner_port_ > 0)
      {
        ups_config_.inner_port = cmd_inner_port_;
      }
      if (strlen(cmd_devname_) > 0)
      {
        ups_config_.devname.set_value(cmd_devname_);
      }
      if (strlen(config_) > 0)
      {
        TBSYS_LOG(INFO, "config file path: [%s]", config_);
      }
      if (strlen(cmd_extra_config_) > 0
          && OB_SUCCESS != (ret = ups_config_.add_extra_config(cmd_extra_config_)))
      {
        TBSYS_LOG(ERROR, "Parse extra config error! string: [%s], ret: [%d]",
                  cmd_extra_config_, ret);
      }
      ups_config_.print();

      if (OB_SUCCESS == ret && OB_SUCCESS != (ret = ups_config_.check_all()))
      {
        TBSYS_LOG(WARN, "failed to load from conf, ret: [%d]", ret);
      }

      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(ERROR, "Start Update server failed, ret: [%d]", ret);
      }
      else
      {
        shadow_server_.set_io_thread_count((int32_t)ups_config_.io_thread_count);
        server_.set_io_thread_count((int32_t)ups_config_.io_thread_count);
        shadow_server_.set_listen_port(static_cast<int32_t>(
                                         ups_config_.inner_port));
        TBSYS_LOG(INFO, "shadow_server port is %s",
                  ups_config_.inner_port.str());
        //shadow_server_.start(false);
        server_.start(false);
        //server_.apply_conf();
        server_.stop();
        shadow_server_.stop();
        server_.wait();
        shadow_server_.wait();
      }

      return ret;
    }

    void ObUpdateServerMain::do_signal(const int sig)
    {
      switch (sig)
      {
      case SIGTERM:
      case SIGINT:
        signal(SIGINT, SIG_IGN);
        signal(SIGTERM, SIG_IGN);
        TBSYS_LOG(INFO, "KILLED by signal, sig=%d", sig);
        server_.req_stop();
        break;
      case SIG_RESET_MEMORY_LIMIT:
        common::ob_set_memory_size_limit(INT64_MAX);
        break;
      case SIG_INC_WORK_THREAD:
        server_.get_trans_executor().TransHandlePool::add_thread(1, 100000);
        break;
      case SIG_DEC_WORK_THREAD:
        server_.get_trans_executor().TransHandlePool::sub_thread(1);
        break;
      case SIG_START_STRESS:
        TBSYS_LOG(INFO, "KILLED by signal, sig=%d", sig);
        StressRunnable::start_stress(server_.get_trans_executor());
        break;
      default:
        break;
      }
    }

    void ObUpdateServerMain::print_version()
    {
      fprintf(stderr, "updateserver (%s %s)\n", PACKAGE_STRING, RELEASEID);
      fprintf(stderr, "SVN_VERSION: %s\n", svn_version());
      fprintf(stderr, "BUILD_TIME: %s %s\n", build_date(), build_time());
      fprintf(stderr, "BUILD_FLAGS: %s\n\n", build_flags());
#ifdef _BTREE_ENGINE_
      fprintf(stderr, "Using Btree Key-Value Engine.\n");
#else
      fprintf(stderr, "Using Hash Key-Value Engine.\n");
#endif
      fprintf(stderr, "Copyright (c) 2007-2011 Taobao Inc.\n");
    }
  }
}
