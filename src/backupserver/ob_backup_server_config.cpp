/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Time-stamp: <2013-04-23 12:59:03 fufeng.syd>
 * Version: $Id$
 * Filename: ob_backup_server_config.cpp
 *
 * Authors:
 *   Yudi Shi <fufeng.syd@taobao.com>
 *
 */

#include "ob_backup_server_config.h"
#include "common/ob_system_config_key.h"

using namespace oceanbase::backupserver;

//const char* ObBackupServerConfig::DEFAULT_DEVNAME = "bond0";

ObBackupServerConfig::ObBackupServerConfig()
{
  //DEF_STR(datadir, "./backup", "sstable data path");
  //DEF_INT(max_migrate_task_count, "100", "[1,]", "max migrate task number");
  //DEF_INT(max_merge_thread_num, "1", "[1,32]", "max merge thread number");

}

ObBackupServerConfig::~ObBackupServerConfig()
{
}



