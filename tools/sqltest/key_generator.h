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
#include "sqltest_dao.h"
#include "prefix_info.h"

extern void set_client_id(int8_t client_id);
extern int8_t get_client_id();

enum GenType
{
  GEN_FIXED = 1,  // gen fixed key
  GEN_SEQ,        // gen key sequentially
  GEN_RANDOM,     // gen key randomly
  GEN_SEQ_SHUFFLE // shuffle key with seed in sequence
};

class Rule
{
  public:
    Rule()
    {
      rule_num_ = 0;
      sum_percent_ = 0;
    }
    ~Rule()
    {
    }
    
  public:
    void add_rule(GenType prefix_type, GenType suffix_type, int percent);
    void get_gen_type(GenType& prefix_type, GenType& suffix_type);

  private:
    const static int MAX_RULE_NUM = 16;
    int rule_num_;
    GenType prefix_types_[MAX_RULE_NUM];
    GenType suffix_types_[MAX_RULE_NUM];
    int percents_[MAX_RULE_NUM];
    int sum_percent_;
};

class KeyRule
{
  public:
    KeyRule();
    ~KeyRule();

    int init(Rule& read_rule, Rule& write_rule);

  public:
    void get_read_gen_type(GenType& prefix_type, GenType& suffix_type);
    void get_write_gen_type(GenType& prefix_type, GenType& suffix_type);
  private:
    bool is_init_;
    Rule read_rule_;
    Rule write_rule_;
};

// Key is <client_id, prefix, suffix>
class KeyGenerator
{
  public:
    KeyGenerator();
    ~KeyGenerator();

    int init(ObSqlClient& ob_client, KeyRule& rule);

  public:
    int get_next_key(uint64_t& prefix, uint64_t& suffix, bool is_read);

  private:
    int get_next_key(uint64_t prefix, uint64_t& suffix, GenType type);
    int get_next_key(uint64_t& prefix, GenType type);

  private:
    PrefixInfo prefix_info_;
    KeyRule key_rule_;
    uint64_t client_id_;
};

class ValueGenerator
{
  public:
    ValueGenerator()
    {
    };
    ~ValueGenerator()
    {
    }

  public:
    int next_int_value(int64_t& value)
    {
      value = rand() % 100;
      return 0;
    }
};

#endif //__KEY_GENERATOR_H__

