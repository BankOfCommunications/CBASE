/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Time-stamp: <2013-11-04 13:55:50 fufeng.syd>
 * Version: $Id$
 * Filename: ob_config_manager.cpp
 *
 * Authors:
 *   Yudi Shi <fufeng.syd@alipay.com>
 *
 */

#include "common/roottable/ob_ms_provider.h"
#include "sql/ob_sql_result_set.h"
#include "file_utils.h"
#include "ob_config_manager.h"

using namespace oceanbase::common;

int ObConfigManager::base_init()
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS != (ret = system_config_.init()))
  {
    TBSYS_LOG(ERROR, "init system config, ret: [%d]", ret);
  }
  else if (OB_SUCCESS != (ret = server_config_.init(system_config_)))
  {
    TBSYS_LOG(ERROR, "init server config, ret: [%d]", ret);
  }
  update_task_.config_mgr_ = this;
  return ret;
}

int ObConfigManager::init(const ObServer &server,
                          const ObClientManager &client_mgr,
                          ObTimer &timer)
{
  ms_self_ = server;
  client_mgr_ = &client_mgr;
  timer_ = &timer;
  return OB_SUCCESS;
}

int ObConfigManager::init(MsList &mslist, const ObClientManager &client_mgr, ObTimer &timer)
{
  mslist_ = &mslist;
  client_mgr_ = &client_mgr;
  timer_ = &timer;
  return OB_SUCCESS;
}

int ObConfigManager::init(ObMsProvider &ms_provider, const ObClientManager &client_mgr, ObTimer &timer)
{
  ms_provider_ = &ms_provider;
  client_mgr_ = &client_mgr;
  timer_ = &timer;
  return OB_SUCCESS;
}

int ObConfigManager::reload_config()
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS != (ret = server_config_.check_all()))
  {
    TBSYS_LOG(WARN, "Check configuration failed, can't reload. ret: [%d]", ret);
  }
  else if (OB_SUCCESS != (ret = reload_config_func_()))
  {
    TBSYS_LOG(WARN, "Reload configuration failed. ret: [%d]", ret);
  }
  return ret;
}

int ObConfigManager::load_config(const char *path)
{
  int ret = OB_SUCCESS;
  FILE *fp = NULL;
  struct stat buf;

  if (path == NULL || 0 == strlen(path))
  {
    path = dump_path_;
  }
  if (0 != stat(path, &buf))
  {
    TBSYS_LOG(INFO, "No config file, read from command line! path: [%s]", path);
    ret = OB_FILE_NOT_EXIST;
  }
  else
  {
    TBSYS_LOG(INFO, "Using config file: [%s]", path);
  }

  if (OB_FILE_NOT_EXIST == ret)
  {
    TBSYS_LOG(INFO, "Config file doesn't exist, using command line options!");
    ret = OB_SUCCESS;
  }
  else if ((fp = fopen(path, "rb")) == NULL)
  {
    TBSYS_LOG(ERROR, "Can't open file: [%s]", path);
    ret = OB_IO_ERROR;
  }
  else
  {
    char buf[OB_MAX_PACKET_LENGTH] = {0};
    int64_t len = fread(buf, 1, OB_MAX_PACKET_LENGTH, fp);
    int64_t pos = 0;

    if (ferror(fp) != 0)        /* read with error */
    {
      TBSYS_LOG(ERROR, "Read config file error! file: [%s]", path);
      ret = OB_IO_ERROR;
    }
    else if (feof(fp) == 0)     /* not end of file */
    {
      TBSYS_LOG(ERROR, "Config file is too long! file: [%s]", path);
      ret = OB_BUF_NOT_ENOUGH;
    }
    else if(OB_SUCCESS != (ret = server_config_.deserialize(buf, len, pos)))
    {
      TBSYS_LOG(ERROR, "Deserialize server config failed! file: [%s], ret: [%d]", path, ret);
    }
    else if (pos != len)
    {
      TBSYS_LOG(ERROR, "Deserialize server config failed! file: [%s], ret: [%d]", path, ret);
      ret = OB_DESERIALIZE_ERROR;
    }
    if (0 != fclose(fp))
    {
      TBSYS_LOG(ERROR, "Close config file failed!");
      ret = OB_IO_ERROR;
    }
  }

  return ret;
}

int ObConfigManager::dump2file(const char* path) const
{
  int ret = OB_SUCCESS;
  int fd = 0;
  ssize_t size = 0;

  if (NULL == path)
  {
    path = dump_path_;
  }

  if (NULL == path || strlen(path) <= 0)
  {
    TBSYS_LOG(WARN, "NO dump path specified!");
    ret = OB_INVALID_ERROR;
  }
  else
  {
    fd = open(path, O_WRONLY | O_CREAT | O_TRUNC, S_IRUSR  | S_IWUSR | S_IRGRP);
    if (fd <= 0)
    {
      TBSYS_LOG(WARN, "fail to create config file [%s], msg: [%s]",
                path, strerror(errno));
      ret = OB_ERROR;
    }
    else
    {
      /* write server config */
      char buf[OB_MAX_PACKET_LENGTH] = {0};
      int64_t pos = 0;
      if (OB_SUCCESS != (ret = server_config_.serialize(buf, OB_MAX_PACKET_LENGTH, pos)))
      {
        TBSYS_LOG(WARN, "Serialize server config fail! ret: [%d]", ret);
      }
      else if (pos != (size = write(fd, buf, pos)))
      {
        TBSYS_LOG(WARN, "Write server config fail!");
        ret = OB_IO_ERROR;
      }
      else
      {
        TBSYS_LOG(INFO, "Write server config successfully!");
      }
    }
    if (0 != close(fd))
    {
      TBSYS_LOG(WARN, "fail to close file fd: [%d]", fd);
    }
  }
  return ret;
}

void ObConfigManager::get_ms(ObServer &ms_server)
{
  if (0 != ms_self_.get_port())
  {
    ms_server = ms_self_;
  }
  else if (NULL != ms_provider_)
  {
    ms_provider_->get_ms(ms_server);
    //add jinty [Paxos Cluster.Balance] 20160708:b
    //slave rs get one ms
    if(!ms_server.is_valid() && NULL != mslist_)
    {
       ms_server = mslist_->get_one();
    }
    //add e
  }
  else if (NULL != mslist_)
  {
    ms_server = mslist_->get_one();
  }
}
//add jinty  [Paxos Cluster.Balance] 20160708:b
int ObConfigManager::get_ms_list(std::vector<ObServer> &list)
{
    int ret = OB_SUCCESS;
    mslist_->get_list(list);
    return ret;
}
MsList* ObConfigManager::get_ms_list_ptr()
{
  return mslist_;
}
//add e
int ObConfigManager::update_local()
{
  int ret = OB_SUCCESS;
  static const int64_t TIMEOUT = 1000000;
  ObServer ms_server;

  get_ms(ms_server);

  if (0 == ms_server.get_port())
  {
    TBSYS_LOG(WARN, "No mergeserver right now.");
    ret = OB_NOT_INIT;
  }
  else
  {
    char buff[OB_MAX_PACKET_LENGTH];
    ObDataBuffer msgbuf(buff, OB_MAX_PACKET_LENGTH);
    sql::ObSQLResultSet rs;
    int64_t session_id = 0;
    ObString sqlstr = ObString::make_string("select /*+read_consistency(STRONG) */ * from __all_sys_config");

    if (OB_SUCCESS != (ret = sqlstr.serialize(msgbuf.get_data(),
                                              msgbuf.get_capacity(),
                                              msgbuf.get_position())))
    {
      TBSYS_LOG(ERROR, "failed to serialize, err = [%d]", ret);
    }
    else if (OB_SUCCESS !=
             (ret = client_mgr_->send_request(ms_server, OB_SQL_EXECUTE,
                                              DEFAULT_VERSION, TIMEOUT,
                                              msgbuf, session_id)))
    {
      TBSYS_LOG(ERROR, "failed to send request, err = [%d]", ret);
    }
    else
    {
      bool fullfilled = true;
      /* ObResultCode result_code; */
      bool empty = true;
      do
      {
        msgbuf.get_position() = 0;
        if (OB_SUCCESS !=
            (ret = rs.deserialize(msgbuf.get_data(), msgbuf.get_capacity(),
                                  msgbuf.get_position())))
        {
          TBSYS_LOG(ERROR,
                    "fail to deserialize result buffer, ret = [%d]\n", ret);
        }
        else if (OB_SUCCESS != rs.get_errno())
        {
          TBSYS_LOG(WARN, "fail to exeucte sql: [%s], errno: [%d]",
                    to_cstring(rs.get_sql_str()), rs.get_errno());
          ret = rs.get_errno();
          break;
        }
        else
        {
          empty = rs.get_new_scanner().is_empty();

          system_config_.update(rs.get_fields(), rs.get_new_scanner());

          rs.get_fullfilled(fullfilled);
          if (fullfilled || empty)
          {
            break;
          }
          else
          {
            msgbuf.get_position() = 0;
            if (OB_SUCCESS != (ret = client_mgr_->get_next(
                                 ms_server, session_id, TIMEOUT,
                                 msgbuf, msgbuf)))
            {
              TBSYS_LOG(ERROR, "failted to send get_next, ret = [%d]", ret);
              break;
            }
          }
        }
      } while (OB_SUCCESS == ret);
    }

    if (OB_SUCCESS == ret)
    {
      if (NULL == dump_path_)
      {
        TBSYS_LOG(ERROR, "Dump path doesn't set, stop read config.");
        ret = OB_NOT_INIT;
      }
      else if (OB_SUCCESS != (ret = server_config_.read_config()))
      {
        TBSYS_LOG(ERROR, "Read server config failed, ret: [%d]", ret);
      }
      else if (OB_SUCCESS != (ret = reload_config()))
      {
        TBSYS_LOG(WARN, "Reload configuration failed. ret: [%d]", ret);
      }
      else if (OB_SUCCESS != (ret = dump2file()))
      {
        TBSYS_LOG(ERROR, "Dump to file failed!"
                  " path: [%s], ret: [%d]", dump_path_, ret);
      }
      else
      {
        TBSYS_LOG(INFO, "Reload server config successfully!");
      }
      server_config_.print();
    }
    else
    {
      TBSYS_LOG(WARN, "Read system config from inner table error, ret: [%d]", ret);
    }
  }
  return ret;
}

int ObConfigManager::got_version(int64_t version)
{
  int ret = OB_SUCCESS;
  bool schedule_task = false;
  if (version < 0)
  {
    /* from rs_admin, do whatever */
    update_task_.update_local_ = false;
    schedule_task = true;
  }
  else if (0 == version)
  {
    /* do nothing */
    TBSYS_LOG(DEBUG, "root server restarting");
  }
  else if (current_version_ == version)
  {
    /* no new version  */
  }
  else if (version < current_version_)
  {
    TBSYS_LOG(WARN, "Local config is newer than rs, weird!"
              " local: [%ld], got: [%ld]", current_version_, version);
  }
  else if (version > current_version_)
  {
    if (!mutex_.tryLock())
    {
      TBSYS_LOG(DEBUG, "Processed by other thread!");
    }
    else
    {
      if (version > newest_version_)
      {
        TBSYS_LOG(INFO, "Got new config version! local: [%ld], newest: [%ld], got: [%ld]",
                  current_version_, newest_version_, version);
        newest_version_ = version;  /* for rootserver hb to others */
        update_task_.update_local_ = true;
        schedule_task = true;
      }
      else if (version < newest_version_) /* version < newest_version_ */
      {
        TBSYS_LOG(WARN, "Receive weird config version! local: [%ld], newest: [%ld], got: [%ld]",
                  current_version_, newest_version_, version);
        /* impossible */
      }
      mutex_.unlock();
    }
  }

  if (schedule_task)
  {
    update_task_.version_ = version;
    update_task_.scheduled_time_ = tbsys::CTimeUtil::getMonotonicTime();
    if (NULL == timer_)
    {
      TBSYS_LOG(ERROR, "Couldn't update config because timer is NULL!");
      ret = OB_NOT_INIT;
    }
    else if (OB_SUCCESS != (ret = timer_->schedule(update_task_, 0, false)))
    {
      TBSYS_LOG(ERROR, "Update local config failed! ret: [%d]", ret);
    }
    else
    {
      TBSYS_LOG(INFO, "Schedule update config task successfully!");
    }
  }
  return ret;
}

int ObConfigManager::UpdateTask::write2stat()
{
  int ret = OB_SUCCESS;
  static const int64_t TIMEOUT = 1000000;
  ObServer ms_server;

  if (NULL != config_mgr_)
  {
    config_mgr_->get_ms(ms_server);
  }
  else
  {
    ret = OB_NOT_INIT;
    TBSYS_LOG(ERROR, "config mgr is NULL, ret: [%d]", ret);
  }

  if (0 == ms_server.get_port())
  {
    ret = OB_NOT_INIT;
  }
  else
  {
    char sqlbuf[OB_MAX_PACKET_LENGTH] = {0};
    char buff[OB_MAX_PACKET_LENGTH] = {0};
    ObDataBuffer msgbuf(buff, OB_MAX_PACKET_LENGTH);
    char *saveptr = NULL;

    config_mgr_->server_config_.get_update_sql(sqlbuf, sizeof (sqlbuf));
    const char *one = strtok_r(sqlbuf, "\n", &saveptr);

    while (OB_SUCCESS == ret && NULL != one)
    {
      msgbuf.get_position() = 0;

      ObString sqlstr = ObString::make_string(one);
      if (OB_SUCCESS != (ret = sqlstr.serialize(msgbuf.get_data(),
                                                msgbuf.get_capacity(),
                                                msgbuf.get_position())))
      {
        TBSYS_LOG(ERROR, "failed to serialize, err = [%d]", ret);
      }
      else if (OB_SUCCESS !=
               (ret = config_mgr_->client_mgr_->send_request(ms_server, OB_SQL_EXECUTE,
                                                             DEFAULT_VERSION, TIMEOUT,
                                                             msgbuf)))
      {
        TBSYS_LOG(ERROR, "failed to send request, err = [%d]", ret);
      }
      else
      {
        sql::ObSQLResultSet rs;
        msgbuf.get_position() = 0;
        if (OB_SUCCESS !=
            (ret = rs.deserialize(msgbuf.get_data(), msgbuf.get_capacity(),
                                  msgbuf.get_position())))
        {
          TBSYS_LOG(WARN,
                    "fail to deserialize result buffer, ret = [%d]\n", ret);
        }
        else if (OB_SUCCESS != rs.get_errno())
        {
          TBSYS_LOG(WARN, "fail to exeucte sql: [%s], errno: [%d]",
                    one, rs.get_errno());
          ret = rs.get_errno();
        }
      }
      one = strtok_r(NULL, "\n", &saveptr);
    }

    if (OB_SUCCESS == ret)
    {
      TBSYS_LOG(INFO, "update config sys stat successfully!");
    }
  }

  return ret;
}

void ObConfigManager::UpdateTask::runTimerTask()
{
  int ret = OB_SUCCESS;

  if (update_local_)
  {
    config_mgr_->system_config_.clear();
    if (OB_SUCCESS != (ret = config_mgr_->update_local()))
    {
      TBSYS_LOG(WARN, "Update local config failed! ret: [%d]", ret);
    }
    else if (OB_SUCCESS != (ret = write2stat()))
    {
      TBSYS_LOG(WARN, "update __all_sys_config_stat failed, ret: [%d]", ret);
    }
    else
    {
      TBSYS_LOG(INFO, "update __all_sys_config_stat successfully");
      config_mgr_->current_version_ = version_;
    }

    if (OB_SUCCESS != ret
        && OB_SUCCESS != (ret = config_mgr_->timer_->schedule(*this, 1000 * 1000L, false)))
    {
      TBSYS_LOG(ERROR, "Reschedule update local config failed! ret: [%d]", ret);
    }
  }
  else if (OB_SUCCESS != (ret = config_mgr_->reload_config()))
  {
    TBSYS_LOG(WARN, "Reload configuration failed. ret: [%d]", ret);
  }
  else
  {
    config_mgr_->server_config_.print();
  }
}
