#include "ob_merge_server_main.h"
#include "common/ob_privilege_manager.h"

namespace oceanbase
{
  namespace mergeserver
  {
    ObMergeServerMain::ObMergeServerMain()
      :  config_mgr_(ms_config_, ms_reload_config_), server_(config_mgr_, ms_config_)
    {
    }

    ObMergeServerMain * ObMergeServerMain::get_instance()
    {
      if (instance_ == NULL)
      {
        instance_ = new(std::nothrow) ObMergeServerMain();
        if (NULL == instance_)
        {
          TBSYS_LOG(ERROR, "check alloc ObMergeServerMain failed");
        }
      }
      return dynamic_cast<ObMergeServerMain *> (instance_);
    }

    int ObMergeServerMain::init_sql_server()
    {
      int ret = OB_SUCCESS;

      if (NULL == server_.get_rpc_proxy()
          || NULL == server_.get_root_rpc()
          || NULL == server_.get_async_rpc()
          || NULL == server_.get_schema_mgr()
          || NULL == server_.get_cache_proxy()
          || NULL == server_.get_privilege_manager()
          || NULL == server_.get_stat_manager())
      {
        TBSYS_LOG(ERROR, "invalid env var.");
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        sql_server_.set_config(&ms_config_);
        sql_server_.set_io_thread_count((int32_t)ms_config_.obmysql_io_thread_count);
        obmysql::ObMySQLServer::MergeServerEnv env;
        env.rpc_proxy_ = server_.get_rpc_proxy();
        env.root_rpc_ = server_.get_root_rpc();
        env.async_rpc_ = server_.get_async_rpc();
        env.schema_mgr_ = server_.get_schema_mgr();
        env.cache_proxy_ = server_.get_cache_proxy();
        env.privilege_mgr_ = server_.get_privilege_manager();
        env.stat_mgr_ = server_.get_stat_manager();
        env.merge_service_ = &(server_.get_service());
        sql_server_.set_env(env);
      }
      return ret;
    }

    ObMsType ObMergeServerMain::get_ms_type(const char* ms_type)
    {
      ObMsType type = INVALID;
      if (strlen(ms_type) == strlen("lms")
          && 0 == strcasecmp(ms_type, "lms"))
      {
        type = LMS;
      }
      else if (strlen(ms_type) == strlen("normal")
               && 0 == strcasecmp(ms_type, "normal"))
      {
        type = NORMAL;
      }
      return type;
    }

    int ObMergeServerMain::do_work()
    {
      int ret = OB_SUCCESS;
      char dump_config_path[OB_MAX_FILE_NAME_LENGTH];

      TBSYS_LOG(INFO, "oceanbase-mergeserver start svn_version=[%s] "
                "build_data=[%s] build_time=[%s]", svn_version(), build_date(),
                build_time());

      ms_reload_config_.set_merge_server(server_);

      snprintf(dump_config_path,
               sizeof (dump_config_path), "etc/%s.config.bin", server_name_);
      config_mgr_.set_dump_path(dump_config_path);
      if (OB_SUCCESS != (ret = config_mgr_.base_init()))
      {
        TBSYS_LOG(ERROR, "init config manager error, ret: [%d]", ret);
      }
      else if (OB_SUCCESS != (ret = config_mgr_.load_config(config_)))
      {
        TBSYS_LOG(ERROR, "load config error, path: [%s], ret: [%d]",
                  config_, ret);
      }

      /* set rs address if command line has past in */
      if (strlen(cmd_rs_ip_) > 0 && cmd_rs_port_ != 0)
      {
        ms_config_.root_server_ip.set_value(cmd_rs_ip_);
        ms_config_.root_server_port = cmd_rs_port_;
      }
      if (cmd_port_ > 0)
      {
        ms_config_.port = cmd_port_;
      }
      if (0 != cmd_obmysql_port_)
      {
        ms_config_.obmysql_port = cmd_obmysql_port_;
      }
      //add pangtianze [Paxos Cluster.Balance] 20161124:b
      if(cmd_cluster_id_ < 0 || cmd_cluster_id_ > OB_MAX_CLUSTER_ID)
      {
        ret = OB_CLUSTER_ID_ERROR;
        TBSYS_LOG(ERROR, "Unexpected cluster id=%d, should be (0 < cluster_id <= %d)",
                  cmd_cluster_id_, OB_MAX_CLUSTER_ID);
      }
      else if (cmd_cluster_id_ != 0)
      {
        ms_config_.cluster_id = static_cast<int64_t>(cmd_cluster_id_);
      }
      //add bingo [Paxos server start bugfix] 20170321:b
      else if(cmd_cluster_id_ == 0 && ms_config_.cluster_id == 0){
        ret = OB_CLUSTER_ID_ERROR;
        TBSYS_LOG(ERROR, "Unexpected cluster id=%d, should be (0 < cluster_id <= %d)",
                  cmd_cluster_id_, OB_MAX_CLUSTER_ID);
      }
      //add:e
      //add:e
      if (
          //add lbzhong [Paxos Cluster.Balance] 20160704:b
          OB_SUCCESS == ret &&
          //add:e
          strlen(cmd_ms_type_) > 0)
      {
        ObMsType type = get_ms_type(cmd_ms_type_);
        if (LMS == type)
        {
          ms_config_.lms = true;
        }
        else if (NORMAL == type)
        {
          ms_config_.lms = false;
        }
        else
        {
          TBSYS_LOG(ERROR, "cmd_ms_type is %s invalid", cmd_ms_type_);
          ret = OB_ERROR;
        }
      }
      if (OB_SUCCESS == ret)
      {
        if (strlen(cmd_devname_) > 0)
        {
          ms_config_.devname.set_value(cmd_devname_);
        }
        if (strlen(config_) > 0)
        {
          TBSYS_LOG(INFO, "config file path: [%s]", config_);
        }
        if (strlen(cmd_extra_config_) > 0
            && OB_SUCCESS != (ret = ms_config_.add_extra_config(cmd_extra_config_)))
        {
          TBSYS_LOG(ERROR, "Parse extra config error! string: [%s], ret: [%d]",
                    cmd_extra_config_, ret);
        }
        ms_config_.print();

        if (OB_SUCCESS == OB_SUCCESS && OB_SUCCESS != (ret = ms_config_.check_all()))
        {
          TBSYS_LOG(ERROR, "check config failed, ret: [%d]", ret);
        }
      }
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(ERROR, "Start merge server failed, ret: [%d]", ret);
      }
      else
      {
        server_.set_io_thread_count((int32_t)ms_config_.io_thread_count);
        server_.set_sql_id_mgr(sql_server_.get_ps_store()->get_id_mgr());
        if (OB_SUCCESS != (ret = server_.start(false)))
        {
          TBSYS_LOG(ERROR, "start mergeserver failed.");
        }
        else if (OB_SUCCESS != (ret = init_sql_server()))
        {
          TBSYS_LOG(ERROR, "init sql server error.");
        }
        else if (OB_SUCCESS != (ret = server_.set_sql_session_mgr(sql_server_.get_session_mgr())))
        {
          TBSYS_LOG(ERROR, "set sql session to mergeserver failed");
        }
        else if (OB_SUCCESS != (ret = sql_server_.start(false)))
        {
          TBSYS_LOG(ERROR, "obmysql server start failed,ret=%d", ret);
        }
        else
        {
          server_.wait();
        }
        TBSYS_LOG(WARN, "stop mysql server");
        sql_server_.stop();
        TBSYS_LOG(WARN, "stop mergeserver");
        server_.stop();
      }
      TBSYS_LOG(INFO, "exit do_work");
      return ret;
    }

    void ObMergeServerMain::print_version()
    {
      fprintf(stderr, "mergeserver (%s %s)\n", PACKAGE_STRING, RELEASEID);
      fprintf(stderr, "SVN_VERSION: %s\n", svn_version());
      fprintf(stderr, "BUILD_TIME: %s %s\n", build_date(), build_time());
      fprintf(stderr, "BUILD_FLAGS: %s\n\n", build_flags());
      fprintf(stderr, "Copyright (c) 2007-2012 Taobao Inc.\n");
    }

    void ObMergeServerMain::do_signal(const int sig)
    {
      switch (sig)
      {
        case SIGTERM:
        case SIGINT:
          signal(SIGINT, SIG_IGN);
          signal(SIGTERM, SIG_IGN);
          TBSYS_LOG(WARN, "KILLED by signal, sig=%d", sig);
          sql_server_.stop();
          server_.stop_eio();
          break;
        default:
          break;
      }
    }
  } /* mergeserver */
} /* oceanbase */
