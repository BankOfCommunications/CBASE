/**
 * (C) 2010-2013 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 * 
 * Version: $Id$
 *
 * ob_migrate_info.cpp
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *   yongle.xh@alipay.com
 *
 */
#include "ob_migrate_info.h"
#include "common/ob_define.h"
using namespace oceanbase::rootserver;
using namespace oceanbase::common;

ObMigrateInfo::ObMigrateInfo()
  : stat_(STAT_FREE),data_source_desc_(),src_server_(),dest_server_(), start_time_(0)
{
}

ObMigrateInfo::~ObMigrateInfo()
{
}

void ObMigrateInfo::reuse()
{
  stat_ = STAT_FREE;
  data_source_desc_.range_.start_key_.assign(NULL, 0);
  data_source_desc_.range_.end_key_.assign(NULL, 0);
  data_source_desc_.uri_.assign(NULL, 0);
  data_source_desc_.reset();
  src_server_.reset();
  dest_server_.reset();
  start_time_ = 0;
  allocator_.reuse();
}

const char* ObMigrateInfo::get_status() const
{
  const char* ret = "ERROR";
  switch(stat_)
  {
    case STAT_FREE:
      ret = "FREE";
      break;
    case STAT_USED:
      ret = "USED";
      break;
    default:
      break;
  }
  return ret;
}

////////////////////////////////////////////////////////////////
ObMigrateInfos::ObMigrateInfos()
  :size_(0)
{
}

ObMigrateInfos::~ObMigrateInfos()
{
}

void ObMigrateInfos::set_size(int64_t size)
{
  if (size > MAX_MIGRATE_INFO_NUM)
  {
    TBSYS_LOG(ERROR, "max migrate concurrency is %lu, can't set it as %lu, set it as %lu.",
        MAX_MIGRATE_INFO_NUM, size, MAX_MIGRATE_INFO_NUM);
    size_ = MAX_MIGRATE_INFO_NUM;
  }
  else
  {
    size_ = size;
  }

  TBSYS_LOG(INFO, "current max migrate concurrency is %lu", size_);
}


void ObMigrateInfos::reset()
{
  for (int i = 0; i < MAX_MIGRATE_INFO_NUM; ++i)
  {
    infos_[i].reuse();
  }
}

bool ObMigrateInfos::is_full() const
{
  bool ret = true;
  for(int64_t i=0; i<size_; ++i)
  {
    if (infos_[i].stat_ == ObMigrateInfo::STAT_FREE)
    {
      ret = false;
    }
  }
  return ret;
}

int64_t ObMigrateInfos::get_used_count() const
{
  int64_t count = 0;
  for(int64_t i=0; i<MAX_MIGRATE_INFO_NUM; ++i)
  {
    if (infos_[i].stat_ == ObMigrateInfo::STAT_USED)
    {
      ++count;
    }
  }
  return count;
}

int ObMigrateInfos::add_migrate_info(const common::ObServer& src_server,
    const common::ObServer& dest_server, const common::ObDataSourceDesc& data_source_desc)
{
  int ret = OB_SUCCESS;
  ObMigrateInfo* minfo = NULL;
  for(int64_t i=0; i<MAX_MIGRATE_INFO_NUM; ++i)
  {
    if (infos_[i].stat_ == ObMigrateInfo::STAT_USED &&
        infos_[i].data_source_desc_.range_ == data_source_desc.range_)
    {
      ret = OB_ROOT_MIGRATE_INFO_EXIST;
      break;
    }
  }

  if (OB_SUCCESS == ret)
  {
    for(int64_t i=0; i<size_; ++i)
    {
      if (infos_[i].stat_ == ObMigrateInfo::STAT_FREE)
      {
        minfo = &infos_[i];
        break;
      }
    }

    if (NULL == minfo)
    {
      ret = OB_ROOT_MIGRATE_CONCURRENCY_FULL;
      TBSYS_LOG(WARN, "no free migrate info found, failed to add migrate info, max_size=%ld", size_);
    }
    else
    {
      if (OB_SUCCESS != (ret = data_source_desc.range_.start_key_.deep_copy(
              minfo->data_source_desc_.range_.start_key_, minfo->allocator_)))
      {
        TBSYS_LOG(ERROR, "clone start key error, ret=%d", ret);
        minfo->reuse();
      }
      else if (OB_SUCCESS != (ret = data_source_desc.range_.end_key_.deep_copy(
              minfo->data_source_desc_.range_.end_key_, minfo->allocator_)))
      {
        TBSYS_LOG(ERROR, "clone end key error, ret=%d", ret);
        minfo->reuse();
      }
      else if (OB_SUCCESS != (ret = ob_write_string(minfo->allocator_, data_source_desc.uri_, minfo->data_source_desc_.uri_)))
      {
        TBSYS_LOG(ERROR, "clone uri error, ret=%d", ret);
        minfo->reuse();
      }
      else
      {
        minfo->data_source_desc_.type_ = data_source_desc.type_;
        minfo->data_source_desc_.range_.table_id_ = data_source_desc.range_.table_id_;
        minfo->data_source_desc_.range_.border_flag_ = data_source_desc.range_.border_flag_;
        minfo->data_source_desc_.sstable_version_ = data_source_desc.sstable_version_;
        minfo->data_source_desc_.tablet_version_ = data_source_desc.tablet_version_;
        minfo->data_source_desc_.keep_source_ = data_source_desc.keep_source_;
        minfo->src_server_ = src_server;
        minfo->dest_server_ = dest_server;
        minfo->start_time_ = tbsys::CTimeUtil::getTime();
        minfo->stat_ = ObMigrateInfo::STAT_USED;
        TBSYS_LOG(DEBUG, "new migrate info: %s %s", to_cstring(dest_server), to_cstring(data_source_desc));
      }
    }
  }

  return ret;
}

int ObMigrateInfos::get_migrate_info(const common::ObNewRange& range, const common::ObServer& src_server,
    const common::ObServer& dest_server, ObMigrateInfo*& info)
{
  int ret = OB_ROOT_MIGRATE_INFO_NOT_FOUND;
  info = NULL;
  for(int64_t i=0; i<MAX_MIGRATE_INFO_NUM; ++i)
  {
    if (infos_[i].stat_ == ObMigrateInfo::STAT_FREE)
    {
      continue;
    }
    else if (infos_[i].data_source_desc_.range_ == range &&
        infos_[i].src_server_ == src_server &&
        infos_[i].dest_server_ == dest_server)
    {
      info = &infos_[i];
      ret = OB_SUCCESS;
      break;
    }
  }
  return ret;
}

int ObMigrateInfos::free_migrate_info(const common::ObNewRange& range, const common::ObServer& src_server,
    const common::ObServer& dest_server)
{
  int ret = OB_SUCCESS;
  ObMigrateInfo* info = NULL;
  if (OB_SUCCESS != (ret = get_migrate_info(range, src_server, dest_server, info)))
  {
    TBSYS_LOG(WARN, "no migrate info found for range=%s src_server=%s dest_server=%s",
        to_cstring(range), to_cstring(src_server), to_cstring(dest_server));
    print_migrate_info();
  }
  else
  {
    info->reuse();
  }
  return ret;
}

bool ObMigrateInfos::check_migrate_info_timeout(int64_t timeout, common::ObServer& src_server,
            common::ObServer& dest_server, common::ObDataSourceDesc::ObDataSourceType& type)
{
  bool has_timeout = false;
  int64_t cur_time = tbsys::CTimeUtil::getTime();
  for (int64_t i=0; i< MAX_MIGRATE_INFO_NUM; ++i)
  {
    if (infos_[i].stat_ == ObMigrateInfo::STAT_USED && infos_[i].start_time_ + timeout < cur_time)
    {
      src_server = infos_[i].src_server_;
      dest_server = infos_[i].dest_server_;
      type = infos_[i].data_source_desc_.type_;
      has_timeout = true;
      TBSYS_LOG(WARN, "loading data timeout: type=%s range=%s src_server=%s"
          " dest_server=%s, start_time=%s cur_time=%s",
          infos_[i].data_source_desc_.get_type(),
          to_cstring(infos_[i].data_source_desc_.range_),
          to_cstring(infos_[i].src_server_),
          to_cstring(infos_[i].dest_server_),
          time2str(infos_[i].start_time_),
          time2str(cur_time));
      infos_[i].reuse();
      break;
    }
  }
  return has_timeout;
}

void ObMigrateInfos::print_migrate_info()
{
  int64_t count = 0;
  for (int64_t i=0; i< MAX_MIGRATE_INFO_NUM; ++i)
  {
    if (infos_[i].stat_ == ObMigrateInfo::STAT_USED)
    {
      ++count;
      ObMigrateInfo & info = infos_[i];
      TBSYS_LOG(INFO, "migrate info[%ld], start_time=%s dest_cs=%s src_cs=%s range=%s keep_src=%c tablet_version=%ld",
          i, time2str(info.start_time_), to_cstring(info.dest_server_),
          to_cstring(info.src_server_), to_cstring(info.data_source_desc_.range_),
          info.data_source_desc_.keep_source_?'Y':'N', info.data_source_desc_.tablet_version_);
    }
  }

  TBSYS_LOG(INFO, "migrate info: total concurrency migrate task is %ld", count);
}
