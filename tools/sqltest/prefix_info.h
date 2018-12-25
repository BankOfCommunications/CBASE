/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 *
 * Version: 0.1: error_record.h,v 0.1 2012/02/24 10:57:39 chuanhui Exp $
 *
 * Authors:
 *   chuanhui <rizhao.ych@taobao.com>
 *     - some work details if you want
 *
 */
#ifndef __OCEANBASE_ERROR_RECORD_H__
#define __OCEANBASE_ERROR_RECORD_H__

#include "obsql_client.h"

// prefix status is stored at OB
// create table prefix_info (
//    prefix int primary key,
//    last_key int,
//    status int
// );

class PrefixInfo
{
  public:
    PrefixInfo();
    ~PrefixInfo();
    int init(ObSqlClient& ob_client);

  public:
    int set_read_write_status(uint64_t prefix, int flag);
    int set_key(uint64_t prefix, uint64_t key);
    int get_key(uint64_t prefix, uint64_t& key);

    int get_max_prefix(uint64_t client_id, uint64_t& prefix);
    int set_max_prefix(uint64_t client_id, uint64_t prefix);

  private:
    int set_status(uint64_t prefix, int status);
    int get_status(uint64_t prefix, int& status);

  private:
    ObSqlClient* ob_client_;
    pthread_rwlock_t rw_lock_;
};

#endif //__ERROR_RECORD_H__

