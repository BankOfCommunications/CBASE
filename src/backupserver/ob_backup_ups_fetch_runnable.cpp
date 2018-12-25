/**
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * Authors:
 *   yanran <yanran.hfs@taobao.com>
 *     - some work details if you want
 */

#include "ob_backup_ups_fetch_runnable.h"
#include "common/file_directory_utils.h"
#include "updateserver/ob_store_mgr.h"
#include "ob_backup_server.h"

using namespace oceanbase::common;
using namespace oceanbase::backupserver;


ObBackupUpsFetchRunnable::ObBackupUpsFetchRunnable()
{
  is_backup_done_= false;
}

ObBackupUpsFetchRunnable::~ObBackupUpsFetchRunnable()
{
}



void ObBackupUpsFetchRunnable::run(tbsys::CThread* thread, void* arg)
{
  int ret = OB_SUCCESS;
  is_backup_done_ = false;
  ups_param_.fetch_sstable_ = false;

  if (!is_initialized_)
  {
    TBSYS_LOG(ERROR, "ObUpsFetchRunnable has not been init");
    ret = OB_NOT_INIT;
  }
  else if (NULL != sstable_mgr_)
  {
    sstable_mgr_->log_sstable_info();
  }
  if (ret == OB_SUCCESS && !_stop)
  {
    if (ups_param_.fetch_sstable_)
    {
      ret = get_sstable_();
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(ERROR, "get_sstable error, ret=%d", ret);
      }
    }
    TBSYS_LOG(INFO, "log_dir = %s", log_dir_);
    ObFetchRunnable::run(thread, arg);
    is_backup_done_ = true;
  }
  backup_server_->report_sub_task_finish(OB_UPDATESERVER,ret); // report the backupserver
  TBSYS_LOG(INFO, "ObUpsFetchRunnable finished[stop=%d ret=%d]", _stop, ret);
}

int ObBackupUpsFetchRunnable::init(const ObServer& master, const char* log_dir, const updateserver::ObUpsFetchParam &param,
                                   updateserver::SSTableMgr *sstable_mgr, ObBackupServer *server)
{

  int ret = OB_SUCCESS;
  TBSYS_LOG(WARN, "ObFetchRunnable init log_dir=%s", log_dir);
  if (NULL == sstable_mgr)
  {
    TBSYS_LOG(ERROR, "Invalid arguments, sstable_mgr=%p", sstable_mgr);
    ret = OB_INVALID_ARGUMENT;
  }

  if (OB_SUCCESS == ret)
  {
    ret = init(master, log_dir, param);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "ObFetchRunnable init error, ret=%d log_dir=%s",
          ret, log_dir);
    }
    else
    {
      TBSYS_LOG(INFO, "ObFetchRunnable init succeed, ret=%d log_dir=%s",
      ret, log_dir_);
    }
  }

  if (OB_SUCCESS == ret)
  {
    ret = ups_param_.clone(param);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(ERROR, "ObUpsFetchParam clone error, ret=%d", ret);
      is_initialized_ = false;
    }
    else
    {
      sstable_mgr_ = sstable_mgr;
      is_initialized_ = true;
      is_backup_done_ = false;
      backup_server_ = server;
    }
  }
  TBSYS_LOG(INFO, "ObBackupUpsFetchRunnable init succeed, ret=%d log_dir=%s",ret, log_dir_);
  return ret;
}

int ObBackupUpsFetchRunnable::init(const ObServer& master, const char* log_dir, const ObFetchParam &param, ObRoleMgr *role_mgr, common::ObLogReplayRunnable *replay_thread)
{
  int ret = 0;

  int log_dir_len = 0;
  if (is_initialized_)
  {
    TBSYS_LOG(ERROR, "ObFetchRunnable has been initialized");
    ret = OB_INIT_TWICE;
  }
  else
  {
    if (NULL == role_mgr || NULL == replay_thread)
    {
       TBSYS_LOG(INFO, "ObFetchRunnable role_mgr, replay_thread = NULL");
    }
    if (NULL == log_dir)
    {
      TBSYS_LOG(ERROR, "Parameter are invalid[log_dir=%p]",log_dir);
      ret = OB_INVALID_ARGUMENT;
    }
    else
    {
      log_dir_len = static_cast<int32_t>(strlen(log_dir));
      if (log_dir_len >= OB_MAX_FILE_NAME_LENGTH)
      {
        TBSYS_LOG(ERROR, "Parameter is invalid[log_dir_len=%d log_dir=%s]", log_dir_len, log_dir);
        ret = OB_INVALID_ARGUMENT;
      }
    }
  }

  if (OB_SUCCESS == ret)
  {
    usr_opt_ = (char*)ob_malloc(OB_MAX_FETCH_CMD_LENGTH, ObModIds::OB_FETCH_RUNABLE);
    if (NULL == usr_opt_)
    {
      TBSYS_LOG(ERROR, "ob_malloc for usr_opt_ error, size=%ld", OB_MAX_FETCH_CMD_LENGTH);
      ret = OB_ALLOCATE_MEMORY_FAILED;
    }
    else
    {
      ret = set_usr_opt(DEFAULT_FETCH_OPTION);
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(ERROR, "set default user option error, DEFAULT_FETCH_OPTION=%s ret=%d", DEFAULT_FETCH_OPTION, ret);
      }
    }
  }

  if (OB_SUCCESS == ret)
  {
    strncpy(log_dir_, log_dir, log_dir_len);
    log_dir_[log_dir_len] = '\0';
    master_ = master;
    param_ = param;
    role_mgr_ = NULL;
    replay_thread_ = NULL;
    is_initialized_ = true;
  }

  return ret;
}

int ObBackupUpsFetchRunnable::set_fetch_param(const updateserver::ObUpsFetchParam& param)
{
  int ret = OB_SUCCESS;

  ret = ups_param_.clone(param);
  if (OB_SUCCESS != ret)
  {
    TBSYS_LOG(ERROR, "ObFetchParam clone error, ret=%d", ret);
  }
  else
  {
    ret = ObFetchRunnable::set_fetch_param(param);
  }

  return ret;
}

/*int ObBackupUpsFetchRunnable::got_log(uint64_t log_id)
{
  int ret = OB_SUCCESS;

  if (NULL == replay_thread_)
  {
    ret = OB_ERROR;
  }
  else
  {

  }

  return ret;
}
*/

int ObBackupUpsFetchRunnable::gen_fetch_sstable_cmd_(const char* name, const char* src_path, const char* dst_path, char* cmd, const int64_t size) const
{
  int ret = OB_SUCCESS;

  int err = 0;
  const int MAX_SERVER_ADDR_SIZE = 128;
  char master_addr[MAX_SERVER_ADDR_SIZE];
  const int MAX_FETCH_OPTION_SIZE = 128;
  char fetch_option[MAX_FETCH_OPTION_SIZE];

  if (NULL == name || NULL == src_path || NULL == dst_path || NULL == cmd || size <= 0)
  {
    TBSYS_LOG(ERROR, "Parameters are invalid[name=%s src_path=%s dst_path=%s cmd=%p size=%ld", name, src_path, dst_path, cmd, size);
    ret = OB_INVALID_ARGUMENT;
  }

  // get master address and generate fetch option
  if (OB_SUCCESS == ret)
  {
    if (!master_.ip_to_string(master_addr, MAX_SERVER_ADDR_SIZE))
    {
      TBSYS_LOG(ERROR, "ObServer to_string error[master_=%p]", &master_);
      ret = OB_ERROR;
    }
    else
    {
      const char* FETCH_OPTION_FORMAT = "%s --bwlimit=%ld";
      err = snprintf(fetch_option, MAX_FETCH_OPTION_SIZE, FETCH_OPTION_FORMAT, usr_opt_, limit_rate_);
      if (err < 0)
      {
        TBSYS_LOG(ERROR, "snprintf fetch_option[MAX_FETCH_OPTION_SIZE=%d] error[%s]", MAX_FETCH_OPTION_SIZE, strerror(errno));
        ret = OB_ERROR;
      }
      else if (err >= MAX_FETCH_OPTION_SIZE)
      {
        TBSYS_LOG(ERROR, "fetch_option buffer size is not enough[MAX_FETCH_OPTION_SIZE=%d real=%d]", MAX_FETCH_OPTION_SIZE, err);
        ret = OB_ERROR;
      }
    }
  }

  // generate fetch command
  if (OB_SUCCESS == ret)
  {
    const char* FETCH_CMD_FORMAT = "rsync %s %s:%s/%s %s/";
    err = snprintf(cmd, size, FETCH_CMD_FORMAT, fetch_option, master_addr, src_path, name, dst_path);

    if (err < 0)
    {
      TBSYS_LOG(ERROR, "snprintf cmd[size=%ld err=%d] error[%s]", size, err, strerror(errno));
      ret = OB_ERROR;
    }
    else if (err >= size)
    {
      TBSYS_LOG(ERROR, "cmd buffer is not enough[size=%ld err=%d]", size, err);
      ret = OB_ERROR;
    }
  }

  return ret;
}

int ObBackupUpsFetchRunnable::remote_cp_sst_(const char* name, const char* src_path, const char* dst_path) const
{
  int ret = OB_SUCCESS;
  char *cmd = NULL;

  cmd = static_cast<char*>(ob_malloc(OB_MAX_FETCH_CMD_LENGTH, ObModIds::OB_UPS_COMMON));
  if (NULL == cmd)
  {
    TBSYS_LOG(WARN, "ob_malloc error, OB_MAX_FETCH_CMD_LENGTH=%ld", OB_MAX_FETCH_CMD_LENGTH);
    ret = OB_ERROR;
  }

  if (OB_SUCCESS == ret)
  {
    ret = gen_fetch_sstable_cmd_(name, src_path, dst_path, cmd, OB_MAX_FETCH_CMD_LENGTH);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "gen_fetch_sstable_cmd_ error, ret=%d", ret);
    }
    else
    {
      ret = FSU::vsystem(cmd);
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "vsystem_(cmd=%s) error, ret=%d", cmd, ret);
      }
      else
      {
        TBSYS_LOG(DEBUG, "remote_cp_sst succ, cmd=%s", cmd);
      }
    }
  }

  if (NULL != cmd)
  {
    ob_free(cmd);
    cmd = NULL;
  }

  return ret;
}

int ObBackupUpsFetchRunnable::get_sstable_()
{
  int ret = OB_SUCCESS;

  updateserver::StoreMgr &store_mgr = sstable_mgr_->get_store_mgr();

  updateserver::SSTList::iterator ssti;
  ObList<updateserver::StoreMgr::Handle>::iterator stoi;
  const char* dst_path1 = NULL;

  for (ssti = ups_param_.sst_list_.begin(); !_stop && ssti != ups_param_.sst_list_.end(); ++ssti)
  {
    ObList<updateserver::StoreMgr::Handle> store_list;
    ret = store_mgr.assign_stores(store_list);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "ObStoreMgr assign_stores error, ret=%d", ret);
    }
    else if (store_list.empty())
    {
      TBSYS_LOG(WARN, "store list is empty, ret=%d", ret);
      ret = OB_ERROR;
    }

    if (OB_SUCCESS == ret)
    {
      stoi = store_list.begin();
      dst_path1 = store_mgr.get_dir(*stoi);
      if (NULL == dst_path1)
      {
        TBSYS_LOG(WARN, "1st dir in store list is NULL");
        ret = OB_ERROR;
      }
      else
      {
        ret = remote_cp_sst_(ssti->name.ptr(), ssti->path.ptr(), dst_path1);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "remote_cp_sst_ error, ret=%d", ret);
        }
        else
        {
          TBSYS_LOG(INFO, "get sstable succ, path=%s name=%s dst_path=%s", ssti->path.ptr(), ssti->name.ptr(), dst_path1);
          char fname_substr[OB_MAX_FILE_NAME_LENGTH];
          const char *fpath = NULL;
          int64_t sst_file_length = ssti->name.size()-5;
          memcpy(fname_substr,ssti->name.ptr(),sst_file_length);
          fname_substr[sst_file_length]='\0';
          fpath = updateserver::SSTableMgr::build_str("%s%s", fname_substr, SCHEMA_SUFFIX);

          ret = remote_cp_sst_(fpath, ssti->path.ptr(), dst_path1);
          if (OB_SUCCESS != ret)
          {
            TBSYS_LOG(WARN, "remote_cp_schema_ error, ret=%d", ret);
          }
          else
          {
            TBSYS_LOG(INFO, "get schema succ, path=%s name=%s dst_path=%s", ssti->path.ptr(), fpath, dst_path1);

          }
        }
      }
    }

    if (OB_SUCCESS == ret)
    {
      ++stoi;
      int counter = 2;
      for (; stoi != store_list.end(); ++stoi, ++counter)
      {
        const char* dst_path2 = store_mgr.get_dir(*stoi);
        if (NULL == dst_path2)
        {
          TBSYS_LOG(WARN, "%d dir in store list is NULL", counter);
        }
        else
        {
          ret = FSU::cp_safe(dst_path1, ssti->name.ptr(), dst_path2, ssti->name.ptr());
          if (OB_SUCCESS != ret)
          {
            TBSYS_LOG(WARN, "cp_safe error, ret=%d", ret);
          }
          else
          {
            TBSYS_LOG(INFO, "copy sstable succ, src_path=%s name=%s dst_path=%s", dst_path1, ssti->name.ptr(), dst_path2);
          }
        }
      }
    }

    if (OB_SUCCESS == ret)
    {
      sstable_mgr_->reload_all();
    }
  }

  return ret;
}

