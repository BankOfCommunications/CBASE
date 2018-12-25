/*
 *   (C) 2007-2010 Taobao Inc.
 *
 *   Version: 0.1
 *
 *   Authors:
 *      qushan <qushan@taobao.com>
 *        - some work details if you want
 *
 */

#include <new>
#include "ob_backup_server_main.h"
#include "common/ob_malloc.h"

namespace oceanbase
{
  namespace backupserver
  {

    // ----------------------------------------------------------
    // class ObBackupServerMain
    // ----------------------------------------------------------
    ObBackupServerMain::ObBackupServerMain()
      : backup_server_(cs_config_,backup_config_)
    {
      cs_config_.max_migrate_task_count = 3;
      cs_config_.max_merge_thread_num = 1;
    }


    ObBackupServerMain* ObBackupServerMain::get_instance()
    {
      if (NULL == instance_)
      {
        instance_ = new (std::nothrow) ObBackupServerMain();
      }
      return dynamic_cast<ObBackupServerMain*>(instance_);
    }

    int ObBackupServerMain::do_work()
    {
      int ret = OB_SUCCESS;
      print_version();

      /* set rs address if command line has past in */
      if (strlen(cmd_rs_ip_) > 0 && cmd_rs_port_ != 0)
      {
        cs_config_.root_server_ip.set_value(cmd_rs_ip_);
        cs_config_.root_server_port = cmd_rs_port_;
      }
      if (cmd_port_ != 0)
      {
        cs_config_.port = cmd_port_;
      }
      if (strlen(cmd_data_dir_) > 0)
      {
        cs_config_.datadir.set_value(cmd_data_dir_);
      }
      if (strlen(cmd_appname_) > 0)
      {
        cs_config_.appname.set_value(cmd_appname_);
      }
      if (strlen(cmd_devname_) > 0)
      {
        cs_config_.devname.set_value(cmd_devname_);
      }
      if (cmd_inner_port_ > 0)
      {
        backup_config_.inner_port = cmd_inner_port_;
      }
      if (strlen(config_))
      {
        TBSYS_LOG(INFO, "using config file path: [%s]", config_);
      }
      if (strlen(cmd_extra_config_) > 0
          && OB_SUCCESS != (ret = cs_config_.add_extra_config(cmd_extra_config_,true)))
      {
        TBSYS_LOG(ERROR, "Parse extra config error! string: [%s], ret: [%d]",
                  cmd_extra_config_, ret);
      }
      cs_config_.print();


      if(OB_SUCCESS != ret)
      {
        TBSYS_LOG(ERROR, "Start backup server failed, ret: [%d]", ret);
      }
      else
      {
        backup_server_.set_io_thread_count((int)cs_config_.io_thread_count);
        backup_server_.start();
      }

      return ret;
    }

    void ObBackupServerMain::do_signal(const int sig)
    {
      switch (sig)
      {
        case SIGTERM:
        case SIGINT:
          signal(SIGINT, SIG_IGN);
          signal(SIGTERM, SIG_IGN);
          TBSYS_LOG(INFO, "KILLED by signal, sig is %d", sig);
          backup_server_.stop_eio();
          break;
        default:
          break;
      }
    }

    void ObBackupServerMain::print_version()
    {
      fprintf(stderr, "backupserver (%s %s)\n", PACKAGE_STRING, RELEASEID);
      fprintf(stderr, "SVN_VERSION: %s\n", svn_version());
      fprintf(stderr, "BUILD_TIME: %s %s\n", build_date(), build_time());
      fprintf(stderr, "BUILD_FLAGS: %s\n\n", build_flags());
      fprintf(stderr, "Copyright (c) 2007-2011 Taobao Inc.\n");

      TBSYS_LOG(INFO, "oceanbase-chunk start svn_version=[%s] "
                "build_data=[%s] build_time=[%s]", svn_version(), build_date(),
                build_time());
    }

  } // end namespace backupserver
} // end namespace oceanbase

