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

#include "mysql_client.h"

// prefix status is stored at OB
// create table prefix_info (
//    prefix int64_t primary key,
//    row_num int64_t,
//    rule_id int64_t,
//    rule_data varchar(64)
// );

class PrefixInfo
{
  public:
    PrefixInfo();
    ~PrefixInfo();
    int init(MysqlClient& ob_client);

  public:
    int set_read_write_status(uint64_t prefix, int flag);
    int set_rule(uint64_t prefix, const char* rule_data);
    int get_rule(uint64_t prefix, char* rule_data, int64_t rule_size);
    int set_row_num(uint64_t prefix, uint64_t row_num);
    int get_row_num(uint64_t prefix, uint64_t& row_num);
    int set_rule_and_row_num(uint64_t prefix, char* rule_data, uint64_t row_num);

    int get_max_prefix(uint64_t client_id, uint64_t& prefix);
    int set_max_prefix(uint64_t client_id, uint64_t prefix);

  private:
    int set_status(uint64_t prefix, int status);
    int get_status(uint64_t prefix, int& status);

  private:
    MysqlClient* ob_client_;
    mutable pthread_rwlock_t w_lock_;
};

#endif //__ERROR_RECORD_H__

