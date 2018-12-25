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
#include "ob_proxy_server_main.h"
#include "common/ob_malloc.h"

namespace oceanbase
{
  namespace proxyserver
  {

    // ----------------------------------------------------------
    // class ObProxyServerMain
    // ----------------------------------------------------------
    ObProxyServerMain::ObProxyServerMain()
      : server_(proxy_config_)
    {
    }


    ObProxyServerMain* ObProxyServerMain::get_instance()
    {
      if (NULL == instance_)
      {
        instance_ = new (std::nothrow) ObProxyServerMain();
      }
      return dynamic_cast<ObProxyServerMain*>(instance_);
    }

    int ObProxyServerMain::do_work()
    {
      int ret = OB_SUCCESS;
      print_version();

      if(OB_SUCCESS != (ret = proxy_config_.load_proxy_config(proxy_config_file_)))
      {
        TBSYS_LOG(ERROR, "load config error, path: [%s], ret: [%d]",
            proxy_config_file_, ret);
      }

      if (cmd_port_ != 0)
      {
        proxy_config_.set_port(cmd_port_);
      }

      if (strlen(cmd_devname_) > 0)
      { 
        proxy_config_.set_devname(cmd_devname_);
      }

      //set env
      const char* java_home = NULL;
      if(NULL == (java_home = proxy_config_.get_java_home()))
      {
        TBSYS_LOG(ERROR, "not set the java home");
        ret = OB_ERROR;
      }
      else if(0 != setenv("JAVA_HOME", java_home, 1))
      {
        TBSYS_LOG(ERROR, "JAVA_HOME set failed errno:%d, error_str:%s", errno, strerror(errno));
        ret = OB_ERROR;
      }

      const char* hdfs_config = NULL;
      if(NULL == (hdfs_config = proxy_config_.get_hdfs_config()))
      {
        TBSYS_LOG(ERROR, "not set the java home");
        ret = OB_ERROR;
      }
      else if(0 != setenv("HADOOP_CONF_DIR", hdfs_config, 1))
      {
        TBSYS_LOG(ERROR, "HADOOP_CONF_DIR set failed errno:%d, error_str:%s", errno, strerror(errno));
      }

      //check hadoop config whether exist
      char config_file[OB_MAX_FILE_NAME_LENGTH];
      memset(config_file, 0, sizeof(config_file));
      int size = snprintf(config_file, OB_MAX_FILE_NAME_LENGTH, "%s/hadoop-site.xml", hdfs_config);
      if(size + 1 > OB_MAX_FILE_NAME_LENGTH || size < 0)
      {
        TBSYS_LOG(ERROR, "config, config_len=%ld <= bufsiz=%d", OB_MAX_FILE_NAME_LENGTH, size);
        ret = OB_SIZE_OVERFLOW;
      }
      else if(access(config_file, R_OK) != 0)
      {
        TBSYS_LOG(ERROR, "config_file:%s cannot access, errno:%d, error_str:%s", config_file, errno, strerror(errno));
        ret = OB_ERROR;
      }

      const char* hadoop_home = NULL;
      if(NULL == (hadoop_home = proxy_config_.get_hadoop_home()))
      {
        TBSYS_LOG(ERROR, "not set the hadoop home");
        ret = OB_ERROR;
      }
      else if(0 != setenv("HADOOP_HOME", hadoop_home, 1))
      {
        TBSYS_LOG(ERROR, "HADOOP_HOME set failed errno:%d, error_str:%s", errno, strerror(errno));
        ret = OB_ERROR;
      }
      else
      {
        //check hadoop config whether exist
        char hadoop_bin[OB_MAX_FILE_NAME_LENGTH];
        memset(hadoop_bin, 0, sizeof(hadoop_bin));
        int size = snprintf(hadoop_bin, OB_MAX_FILE_NAME_LENGTH, "%s/bin/hadoop", hadoop_home);
        if(size + 1 > OB_MAX_FILE_NAME_LENGTH || size < 0)
        {
          TBSYS_LOG(ERROR, "hadoop, hadoop_len=%ld <= bufsiz=%d", OB_MAX_FILE_NAME_LENGTH, size);
          ret = OB_SIZE_OVERFLOW;
        }
        else if(access(hadoop_bin, X_OK) != 0)
        {
          TBSYS_LOG(ERROR, "hadoop_bin:%s cannot access, errno:%d, error_str:%s", hadoop_bin, errno, strerror(errno));
          ret = OB_ERROR;
        }
      }

      if(OB_SUCCESS == ret)
      {
#ifndef MAX_ENV_LEN
#define MAX_ENV_LEN 1024
#endif
        char* path = NULL;
        char env[MAX_ENV_LEN];
        //set path
        memset(env, 0, sizeof(env));
        if(NULL == (path = getenv("PATH")))
        {
          TBSYS_LOG(ERROR, "get path failed errno:%d, error_str:%s", errno, strerror(errno));
          ret = OB_ERROR;
        }
        else
        {
          int buf_size = snprintf(env, MAX_ENV_LEN, "%s:%s/bin:%s/bin", path, hadoop_home, java_home);
          if(buf_size + 1 > MAX_ENV_LEN || 0 > buf_size)
          {
            TBSYS_LOG(ERROR, "env, env_len=%d <= bufsiz=%d", MAX_ENV_LEN, buf_size);
            ret = OB_SIZE_OVERFLOW;
          }
          else if(0 != setenv("PATH", env, 1))
          {
            TBSYS_LOG(ERROR, "PATH set failed errno:%d, error_str:%s", errno, strerror(errno));
            ret = OB_ERROR;
          }
        }
      }

      if(OB_SUCCESS != ret)
      {
        TBSYS_LOG(ERROR, "Start proxy server failed, ret: [%d]", ret);
      }
      else
      {
        server_.set_io_thread_count((int)proxy_config_.get_io_thread_count());
        server_.start();
      }

      return ret;
    }

    void ObProxyServerMain::do_signal(const int sig)
    {
      switch (sig)
      {
        case SIGTERM:
        case SIGINT:
          signal(SIGINT, SIG_IGN);
          signal(SIGTERM, SIG_IGN);
          TBSYS_LOG(INFO, "KILLED by signal, sig is %d", sig);
          server_.stop_eio();
          break;
        default:
          break;
      }
    }

    void ObProxyServerMain::print_version()
    {
      fprintf(stderr, "proxyserver (%s %s)\n", PACKAGE_STRING, RELEASEID);
      fprintf(stderr, "SVN_VERSION: %s\n", svn_version());
      fprintf(stderr, "BUILD_TIME: %s %s\n", build_date(), build_time());
      fprintf(stderr, "BUILD_FLAGS: %s\n\n", build_flags());
      fprintf(stderr, "Copyright (c) 2007-2011 Taobao Inc.\n");

      TBSYS_LOG(INFO, "oceanbase-chunk start svn_version=[%s] "
                "build_data=[%s] build_time=[%s]", svn_version(), build_date(),
                build_time());
    }

  } // end namespace proxyserver
} // end namespace oceanbase

