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

#include "ob_ups_fetch_runnable.h"

#include "common/file_directory_utils.h"
#include "ob_store_mgr.h"

using namespace oceanbase::common;
using namespace oceanbase::updateserver;

int64_t ObSlaveInfo::get_serialize_size(void) const
{
  int64_t size = 0;

  size += self.get_serialize_size();
  size += serialization::encoded_length_i64(min_sstable_id);
  size += serialization::encoded_length_i64(max_sstable_id);

  return size;
}

int ObSlaveInfo::serialize(char* buf, const int64_t buf_len, int64_t& pos) const
{
  int err = OB_SUCCESS;

  if (OB_SUCCESS == err)
  {
    err = self.serialize(buf, buf_len, pos);
  }
  if (OB_SUCCESS == err)
  {
    err = serialization::encode_i64(buf, buf_len, pos, min_sstable_id);
  }
  if (OB_SUCCESS == err)
  {
    err = serialization::encode_i64(buf, buf_len, pos, max_sstable_id);
  }
  //add wangjiahao [Paxos ups_replication] 20150817 :b
  if (OB_SUCCESS == err)
  {
    err = serialization::encode_i64(buf, buf_len, pos, log_id);
  }
  //add :e
  if (err != OB_SUCCESS)
  {
    TBSYS_LOG(WARN, "ObSlaveInfo serialize error, err=%d buf=%p pos=%ld buf_len=%ld serialize_size=%ld",
        err, buf, pos, buf_len, get_serialize_size());
  }

  return err;
}

int ObSlaveInfo::deserialize(const char* buf, const int64_t buf_len, int64_t& pos)
{
  int err = OB_SUCCESS;

  if (OB_SUCCESS == err)
  {
    err = self.deserialize(buf, buf_len, pos);
  }
  if (OB_SUCCESS == err)
  {
    err = serialization::decode_i64(buf, buf_len, pos, reinterpret_cast<int64_t*>(&min_sstable_id));
  }
  if (OB_SUCCESS == err)
  {
    err = serialization::decode_i64(buf, buf_len, pos, reinterpret_cast<int64_t*>(&max_sstable_id));
  }
  //add wangjiahao [Paxos ups_replication] 20150817 :b
  if (OB_SUCCESS == err)
  {
    err = serialization::decode_i64(buf, buf_len, pos, reinterpret_cast<int64_t*>(&log_id));
  }
  //add :e
  if (OB_SUCCESS != err)
  {
    TBSYS_LOG(WARN, "ObSlaveInfo deserialize error, err=%d buf=%p buf_len=%ld pos=%ld",
        err, buf, buf_len, pos);
  }

  return err;
}


ObUpsFetchRunnable::ObUpsFetchRunnable()
{
}

ObUpsFetchRunnable::~ObUpsFetchRunnable()
{
}

void ObUpsFetchRunnable::run(tbsys::CThread* thread, void* arg)
{
  int ret = OB_SUCCESS;

  if (!is_initialized_)
  {
    TBSYS_LOG(ERROR, "ObUpsFetchRunnable has not been init");
    ret = OB_NOT_INIT;
  }
  else
  {
    if (!_stop && ups_param_.fetch_sstable_)
    {
      ret = get_sstable_();
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(ERROR, "get_sstable error, ret=%d", ret);
        role_mgr_->set_state(ObRoleMgr::ERROR);
      }
    }

    ObFetchRunnable::run(thread, arg);
  }

  if (NULL != sstable_mgr_)
  {
    sstable_mgr_->log_sstable_info();
  }
  TBSYS_LOG(INFO, "ObUpsFetchRunnable finished[stop=%d ret=%d]", _stop, ret);
}

int ObUpsFetchRunnable::init(const ObServer& master, const char* log_dir, const ObUpsFetchParam &param, ObRoleMgr *role_mgr, common::ObLogReplayRunnable *replay_thread, SSTableMgr *sstable_mgr)
{
  int ret = 0;

  if (NULL == sstable_mgr)
  {
    TBSYS_LOG(ERROR, "Invalid arguments, sstable_mgr=%p", sstable_mgr);
    ret = OB_INVALID_ARGUMENT;
  }

  if (OB_SUCCESS == ret)
  {
    ret = ObFetchRunnable::init(master, log_dir, param, role_mgr, replay_thread);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "ObFetchRunnable init error, ret=%d log_dir=%s role_mgr=%p replay_thread=%p",
          ret, log_dir, role_mgr, replay_thread);
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
    }
  }

  return ret;
}

int ObUpsFetchRunnable::set_fetch_param(const ObUpsFetchParam& param)
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

int ObUpsFetchRunnable::gen_fetch_sstable_cmd_(const char* name, const char* src_path, const char* dst_path, char* cmd, const int64_t size) const
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

int ObUpsFetchRunnable::remote_cp_sst_(const char* name, const char* src_path, const char* dst_path) const
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

int ObUpsFetchRunnable::get_sstable_()
{
  int ret = OB_SUCCESS;

  StoreMgr &store_mgr = sstable_mgr_->get_store_mgr();

  SSTList::iterator ssti;
  ObList<StoreMgr::Handle>::iterator stoi;
  const char* dst_path1 = NULL;

  for (ssti = ups_param_.sst_list_.begin(); !_stop && ssti != ups_param_.sst_list_.end(); ++ssti)
  {
    ObList<StoreMgr::Handle> store_list;
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
