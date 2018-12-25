/*
 * Copyright (C) 2007-2012 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * Description here
 *
 * Version: $Id$
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *     - some work details here
 */
#ifndef _OB_ROOT_ADMIN_CMD_H
#define _OB_ROOT_ADMIN_CMD_H 1

namespace oceanbase
{
  namespace rootserver
  {
    static const int OB_RS_ADMIN_CHECKPOINT = 1;
    static const int OB_RS_ADMIN_RELOAD_CONFIG = 2;
    static const int OB_RS_ADMIN_SWITCH_SCHEMA = 3;
    static const int OB_RS_ADMIN_DUMP_ROOT_TABLE = 4;
    static const int OB_RS_ADMIN_DUMP_SERVER_INFO = 5;
    static const int OB_RS_ADMIN_INC_LOG_LEVEL = 6;
    static const int OB_RS_ADMIN_DEC_LOG_LEVEL = 7;
    static const int OB_RS_ADMIN_DUMP_UNUSUAL_TABLETS = 8;
    static const int OB_RS_ADMIN_DUMP_MIGRATE_INFO = 9;
    static const int OB_RS_ADMIN_ENABLE_BALANCE = 11;
    static const int OB_RS_ADMIN_DISABLE_BALANCE = 12;
    static const int OB_RS_ADMIN_ENABLE_REREPLICATION = 13;
    static const int OB_RS_ADMIN_DISABLE_REREPLICATION = 14;
    static const int OB_RS_ADMIN_CLEAN_ERROR_MSG = 15;
    static const int OB_RS_ADMIN_BOOT_STRAP = 16;
    static const int OB_RS_ADMIN_BOOT_RECOVER = 17;
    static const int OB_RS_ADMIN_REFRESH_SCHEMA = 18;
    static const int OB_RS_ADMIN_INIT_CLUSTER = 19;
    static const int OB_RS_ADMIN_CLEAN_ROOT_TABLE = 20;
    static const int OB_RS_ADMIN_CHECK_SCHEMA = 21;
    //add liuxiao
    //for create new sys table all_cchecksum_info
    static const int OB_RS_ADMIN_CREATE_ALL_CCHECKSUM_INFO = 22;
    //add e
    //add pangtianze [Paxos rs_election] 20170228:b
    static const int OB_RS_ADMIN_REFRESH_RS_LIST = 23;
    //add:e
    //add bingo [Paxos set_boot_ok] 20170315:b
    static const int OB_RS_ADMIN_SET_BOOT_OK = 24;
    //add:e
    //add bingo [Paxos sstable info to rs log] 20170614:b
    static const int OB_DUMP_SSTABLE_INFO = 25;
    //add:e
  } // end namespace rootserver
} // end namespace oceanbase

#endif /* _OB_ROOT_ADMIN_CMD_H */

