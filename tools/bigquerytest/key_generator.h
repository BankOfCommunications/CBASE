/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 *
 * Version: 0.1: key_generator.h,v 0.1 2012/02/22 12:32:35 chuanhui Exp $
 *
 * Authors:
 *   chuanhui <rizhao.ych@taobao.com>
 *     - some work details if you want
 *
 */
#ifndef __OCEANBASE_KEY_GENERATOR_H__
#define __OCEANBASE_KEY_GENERATOR_H__

#include "util.h"
#include "mysql_client.h"
#include "prefix_info.h"
#include "bigquerytest_param.h"

enum GenType
{
  GEN_FIXED = 1,  // gen fixed key
  GEN_SEQ,        // gen key sequentially
  GEN_RANDOM,     // gen key randomly
  GEN_SEQ_SHUFFLE // shuffle key with seed in sequence
};

// Key is <prefix, suffix>
class KeyGenerator
{
  public:
    KeyGenerator();
    ~KeyGenerator();

    int init(PrefixInfo& prefix_info, BigqueryTestParam& param);

  public:
    int get_next_key(uint64_t& prefix, bool is_read);

  private:
    int get_next_key(uint64_t& prefix, GenType type);

  private:
    PrefixInfo* prefix_info_;
    uint64_t client_id_;
};

#endif //__KEY_GENERATOR_H__

