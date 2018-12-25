/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Time-stamp: <2013-04-23 15:59:13 fufeng.syd>
 * Version: $Id$
 * Filename: ob_update_server_config.cpp
 *
 * Authors:
 *   Yudi Shi <fufeng.syd@taobao.com>
 *
 */

#include <math.h>
#include "ob_update_server_config.h"
#include "common/compress/ob_compressor.h"
#include "ob_memtank.h"

using namespace oceanbase::updateserver;

ObUpdateServerConfig::ObUpdateServerConfig()
  : total_memory_limit_bk_(0)
{
}

ObUpdateServerConfig::~ObUpdateServerConfig()
{
}

int ObUpdateServerConfig::init()
{
  int err = OB_SUCCESS;
  static const int64_t phy_mem_size = get_phy_mem_size();

  trans_thread_num = get_cpu_num() - 2;
  replay_worker_num =  get_cpu_num() - 2;

  const int64_t total_memory_limit_gb = get_total_memory_limit(phy_mem_size / (1 << 30));
  auto_config_memory(total_memory_limit_gb);
  total_memory_limit_bk_ = total_memory_limit;
  return err;
}

int ObUpdateServerConfig::read_config()
{
  int ret = ObServerConfig::read_config();
  if (OB_SUCCESS != ret)
  {
    TBSYS_LOG(ERROR, "Reload server config fail! ret: [%d]", ret);
  }
  else if (total_memory_limit_bk_ != total_memory_limit)
  {
    auto_config_memory(total_memory_limit / (1 << 30));
    total_memory_limit_bk_ = total_memory_limit;
  }
  return ret;
}

int ObUpdateServerConfig::check_all()
{
  int ret = ObServerConfig::check_all();
  if (OB_SUCCESS != ret)
  {
    TBSYS_LOG(ERROR, "Check server config fail! ret: [%d]", ret);
  }
  else
  {
    int64_t mem_total = blockcache_size + blockindex_cache_size + table_memory_limit;
    if (mem_total >= total_memory_limit)
    {
      TBSYS_LOG(WARN, "{blockcache_size + blockindex_cache + table_memory_limit}"
                " [%ld] cannot larger than total_memory_limit=%ld",
                mem_total, static_cast<int64_t>(total_memory_limit));
      ret = OB_INVALID_ARGUMENT;
    }

    if (OB_SUCCESS == ret)
    {
      if (active_mem_limit >= table_memory_limit)
      {
        TBSYS_LOG(WARN, "active_mem_limit=%s cannot larger than table_memory_limit=%s",
                  active_mem_limit.str(), table_memory_limit.str());
        ret = OB_INVALID_ARGUMENT;
      }
      if (using_hash_index && active_mem_limit < memtable_hash_buckets_size * HASH_NODE_SIZE)
      {
        TBSYS_LOG(WARN, "active_mem_limit=%s cannot less than MIN_ACTIVE_MEMTABLE_WITH_HASH=%ld",
                  active_mem_limit.str(), memtable_hash_buckets_size * HASH_NODE_SIZE);
        ret = OB_INVALID_ARGUMENT;
      }
    }

    ObCompressor *compressor = create_compressor(sstable_compressor_name);
    if (NULL == compressor)
    {
      TBSYS_LOG(ERROR, "cannot load compressor library name=[%s]",
                sstable_compressor_name.str());
      ret = OB_ERROR;
    }
    else
    {
      destroy_compressor(compressor);
    }
  }
  return ret;
}

void ObUpdateServerConfig::auto_config_memory(const int64_t total_memory_limit_gb)
{
  memtable_hash_buckets_size = total_memory_limit_gb * (1 << 20) / 2;

  int64_t total_reserve = 0;
  if (40 <= total_memory_limit_gb)
  {
    total_reserve = 10;
  }
  else if (18 <= total_memory_limit_gb)
  {
    total_reserve = 6;
  }
  else if (13 < total_memory_limit_gb)
  {
    total_reserve = 4;
  }
  else if (5 < total_memory_limit_gb)
  {
    total_reserve = 3;
  }
  else if (3 < total_memory_limit_gb)
  {
    total_reserve = 2;
  }
  else
  {
    total_reserve = 1;
  }

  total_memory_limit = total_memory_limit_gb * (1 << 30);
  table_memory_limit  = (int64_t)((double)(total_memory_limit - (total_reserve << 30)) / (1 / 20.0 + 1 / 15.0 + 1));

  if (13 > total_memory_limit / (1L<<30))
  {
    table_available_warn_size.set_value("0GB");
    table_available_error_size.set_value("1GB");
  }
  else if (40 > total_memory_limit / (1L<<30))
  {
    table_available_warn_size.set_value("2GB");
    table_available_error_size.set_value("1GB");
  }
  else if (83 > total_memory_limit / (1L<<30))
  {
    table_available_warn_size.set_value("4GB");
    table_available_error_size.set_value("2GB");
  }
  else if (169 > total_memory_limit / (1L<<30))
  {
    table_available_warn_size.set_value("8GB");
    table_available_error_size.set_value("4GB");
  }
  else
  {
    table_available_warn_size.set_value("16GB");
    table_available_error_size.set_value("8GB");
  }

  blockcache_size            = (int64_t)(table_memory_limit / 15);
  blockindex_cache_size      = (int64_t)(table_memory_limit / 20);

  if (40 <= total_memory_limit / (1L<<30))
  {
    minor_num_limit.set_value("3");
  }
  else if (13 < total_memory_limit / (1L<<30))
  {
    minor_num_limit.set_value("2");
  }
  else
  {
    minor_num_limit.set_value("1");
  }
  active_mem_limit = (int64_t)((double)table_memory_limit * 0.7 / (double)minor_num_limit);

  TBSYS_LOG(INFO, "[auto_config_memory] "
            "total_memory_limit=%ld "
            "table_memory_limit=%ld "
            "active_mem_limit=%ld "
            "table_available_warn_size=%ld "
            "table_available_error_size=%ld "
            "blockcache_size=%ld "
            "blockindex_cache_size=%ld "
            "memtable_hash_buckets_size=%ld",
            total_memory_limit >> 30,
            table_memory_limit >> 30,
            active_mem_limit >> 30,
            table_available_warn_size >> 30,
            table_available_error_size >> 30,
            blockcache_size >> 20,
            blockindex_cache_size >> 20,
            +memtable_hash_buckets_size);
  fprintf(stdout, "[auto_config_memory]\n"
            "total_memory_limit=%ld\n"
            "table_memory_limit=%ld\n"
            "active_mem_limit=%ld\n"
            "table_available_warn_size=%ld\n"
            "table_available_error_size=%ld\n"
            "blockcache_size=%ld\n"
            "blockindex_cache_size=%ld\n"
            "memtable_hash_buckets_size=%ld\n",
            total_memory_limit >> 30,
            table_memory_limit >> 30,
            active_mem_limit >> 30,
            table_available_warn_size >> 30,
            table_available_error_size >> 30,
            blockcache_size >> 20,
            blockindex_cache_size >> 20,
            +memtable_hash_buckets_size);
}

int64_t ObUpdateServerConfig::get_total_memory_limit(const int64_t phy_mem_size_gb)
{
  static const double PAGE_MANAGE_STRUCT = 4000.0;
  static const double PAGE_SIZE = 4096.0;
  int64_t KERNEL_RESERVE_SIZE = (8 > phy_mem_size_gb) ? 0 : 2;
  int64_t ROOTSERVER_RESERVE_SIZE = (24 > phy_mem_size_gb) ? 0 : 2 * (phy_mem_size_gb / 24);
  int64_t ret = (int64_t)(round((double)phy_mem_size_gb * PAGE_MANAGE_STRUCT / PAGE_SIZE))
    - KERNEL_RESERVE_SIZE
    - ROOTSERVER_RESERVE_SIZE
    - MemTank::REPLAY_MEMTABLE_RESERVE_SIZE_GB;
  TBSYS_LOG(INFO, "phy_mem_size=%ld total_memory_limit=%ld", phy_mem_size_gb, ret);
  fprintf(stdout, "phy_mem_size=%ld total_memory_limit=%ld\n", phy_mem_size_gb, ret);
  return ret;
}
