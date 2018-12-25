/**
 * (C) 2010-2011 Taobao Inc.
 *
 *  This program is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU General Public License
 *  version 2 as published by the Free Software Foundation.
 *
 *  ob_yunti_proxy.c 
 *
 *  Authors:
 *    zian <yunliang.shi@alipay.com>
 *
 */
#include "ob_yunti_proxy.h"
#include "ob_proxy_server_main.h"

namespace oceanbase
{
  namespace proxyserver
  {
    YuntiProxy::YuntiProxy()
    {
    }

    YuntiProxy::~YuntiProxy()
    {
    }

    int YuntiProxy::initialize()
    {
      int ret = OB_SUCCESS;

      if(OB_SUCCESS != (ret = yunti_meta_.initialize(this, THE_PROXY_SERVER.get_config().get_yt_mem_limit())))
      {
        TBSYS_LOG(ERROR, "hdfs meta init failed, ret:%d", ret);
      }

      return ret;
    }

    int YuntiProxy::open_rfile(FILE* &fp, const char* file_name)
    {
      int ret = OB_SUCCESS;
      char cmd[MAX_CMD_LEN];
      memset(cmd, 0, sizeof(cmd));

      int buf_size = snprintf(cmd, MAX_CMD_LEN, "hadoop fs -cat %s", file_name);
      if(buf_size + 1 > MAX_CMD_LEN)
      {
        TBSYS_LOG(WARN, "cmd, cmd_len=%d <= bufsiz=%d", MAX_CMD_LEN, buf_size);
        ret = OB_SIZE_OVERFLOW;
      }
      else if(NULL == (fp = popen(cmd, "r")))
      {
        TBSYS_LOG(WARN, "get file:%s locate path failed, error:%d error_str:%s", file_name, errno, strerror(errno));
        ret = OB_DATA_SOURCE_READ_ERROR;
      }

      return ret;
    }

    int YuntiProxy::open_rfile(FILE* &fp, common::ObNewRange& range, const char* sstable_dir)
    {
      int ret = OB_SUCCESS;

      char sstable_file_name[OB_MAX_FILE_NAME_LENGTH];
      memset(sstable_file_name, 0, sizeof(sstable_file_name));
      if(OB_SUCCESS != (ret = yunti_meta_.get_sstable_file_path(sstable_dir, range, sstable_file_name, OB_MAX_FILE_NAME_LENGTH)))
      {
        TBSYS_LOG(ERROR, "get range:%s locate path failed", to_cstring(range));
      }
      else if(!is_file_exist(sstable_file_name))
      {
        TBSYS_LOG(ERROR, "file %s not exist", sstable_file_name);
        ret = OB_DATA_SOURCE_DATA_NOT_EXIST;
      }
      else
      {
        ret = open_rfile(fp, sstable_file_name);
      }
      return ret;
    }

    int YuntiProxy::close_rfile(FILE* fp)
    {
      int ret = OB_SUCCESS;
      if(OB_SUCCESS != (ret = pclose(fp)))
      {
        TBSYS_LOG(WARN, "close file failed errno:%d error_str:%s ret:%d", errno, strerror(errno), ret);
      }

      return ret;
    }

    int YuntiProxy::fetch_data(FILE* fp, char* buf, int64_t size, int64_t &read_size)
    {
      int ret = OB_SUCCESS;
      if(!fp)
      {
        TBSYS_LOG(WARN, "fp is null, maybe file not open");
        read_size = 0;
      }
      else if(ferror(fp))
      {
        TBSYS_LOG(WARN, "fp read failed, errno:%d, error_str:%s", errno, strerror(errno));
        ret = OB_DATA_SOURCE_SYS_ERROR;
      }
      else if(feof(fp))
      {
        TBSYS_LOG(INFO, "file reach end");
      }
      else
      {
        read_size = fread(buf, 1, size, fp);
        if(read_size < 0)
        {
          TBSYS_LOG(WARN, "fp read failed, errno:%d, error_str:%s", errno, strerror(errno));
          ret = OB_DATA_SOURCE_SYS_ERROR;
        }
      }
      return ret;
    }

    bool YuntiProxy::is_file_exist(const char* file_name)
    {
      bool ret = true;
      char cmd[MAX_CMD_LEN];
      memset(cmd, 0, sizeof(cmd));
      FILE* fp = NULL;
      int buf_size = snprintf(cmd, MAX_CMD_LEN, "hadoop fs -du  %s 2>&1", file_name);

      if(buf_size + 1 > MAX_CMD_LEN || buf_size < 0)
      {
        TBSYS_LOG(WARN, "cmd, cmd_len=%d <= bufsiz=%d", MAX_CMD_LEN, buf_size);
        ret = OB_SIZE_OVERFLOW;
      }
      else if(NULL == (fp = popen(cmd, "r")))
      {
        TBSYS_LOG(WARN, "get file:%s locate path failed, error:%d, error_str:%s", file_name, errno, strerror(errno));
        ret = OB_DATA_SOURCE_READ_ERROR;
      }
      else
      {
#ifndef MAX_FILE_INFO_LEN 
#define MAX_FILE_INFO_LEN 1024
#endif
        char file_info[MAX_FILE_INFO_LEN];
        memset(file_info, 0, sizeof(file_info));
        if(fgets(file_info, sizeof(file_info), fp))
        {
          if(NULL != strstr(file_info, "No such file or directory"))
          {
            TBSYS_LOG(INFO, "file:%s is not exist", file_name);
            ret = false;
          }
        }

        pclose(fp);
      }

      return ret;
    }

    int YuntiProxy::get_file_size(const char* file_name, int64_t &file_size)
    {
      int ret = OB_SUCCESS;
      char cmd[MAX_CMD_LEN];
      memset(cmd, 0, sizeof(cmd));
      FILE* fp = NULL;
      int buf_size = snprintf(cmd, MAX_CMD_LEN, "hadoop fs -du  %s | sed -n 2p | awk -F \" \" '{print $1}'", file_name);

      if(buf_size + 1 > MAX_CMD_LEN)
      {
        TBSYS_LOG(WARN, "cmd, cmd_len=%d <= bufsiz=%d", MAX_CMD_LEN, buf_size);
        ret = OB_SIZE_OVERFLOW;
      }
      else if(NULL == (fp = popen(cmd, "r")))
      {
        TBSYS_LOG(WARN, "get file:%s locate path failed, error:%d, error_str:%s", file_name, errno, strerror(errno));
        ret = OB_DATA_SOURCE_READ_ERROR;
      }
      else
      {
#ifndef MAX_64INT_LEN 
#define MAX_64INT_LEN 20
#endif
        char file_size_info[MAX_64INT_LEN];
        memset(file_size_info, 0, sizeof(file_size_info));
        if(fgets(file_size_info, sizeof(file_size_info), fp))
        {
          file_size = atoll(file_size_info);
        }
        else
        {
          ret = OB_DATA_SOURCE_READ_ERROR;
        }
        pclose(fp);
      }

      return ret;
    }

  }
}
