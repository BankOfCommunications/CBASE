/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 *
 * Version: 0.1: value_generator.h,v 0.1 2012/04/06 15:21:41 chuanhui Exp $
 *
 * Authors:
 *   chuanhui <rizhao.ych@taobao.com>
 *     - some work details if you want
 *
 */
#ifndef __OCEANBASE_VALUE_GENERATOR_H__
#define __OCEANBASE_VALUE_GENERATOR_H__

#include "util.h"
#include "mysql_client.h"
#include "prefix_info.h"

class ValueRule
{
  public:
    ValueRule();
    ~ValueRule();

    void reset();

  public:
    void add_equal_rule(int64_t value);
    void add_seq_rule(int64_t start_val, int64_t step);
    void add_cycle_rule(int64_t start_val, int64_t step, int64_t size_cycle, int64_t num_cycles);

    int64_t get_row_num();
    void set_row_num(int64_t row_num);
    int64_t get_rule_num();
    int64_t get_rule_id(int64_t rule_idx);
    void get_equal_rule(int64_t rule_idx, int64_t& value);
    void get_seq_rule(int64_t rule_idx, int64_t& start_val, int64_t& step);
    void get_cycle_rule(int64_t rule_idx, int64_t& start_val, int64_t& step, int64_t& size_cycle, int64_t& num_cycles);

    void get_col_values(uint64_t suffix, int64_t* col_ary, int64_t col_size, int64_t & ret_size);

    int serialize(char* buf, int64_t buf_size);
    int deserialize(const char* buf, int64_t buf_size);
    const char* to_string();

  private:
    void get_value(uint64_t suffix, int64_t col_idx, int64_t& value);

    void get_equal_value(uint64_t suffix, int64_t col_idx, int64_t& value);
    void get_seq_value(uint64_t suffix, int64_t col_idx, int64_t& value);
    void get_cycle_value(uint64_t suffix, int64_t col_idx, int64_t& value);

  public:
    static const int64_t MAX_RULE_NUM = 8;

    enum RULE_ID
    {
      MIN_RULE = 0,
      EQUAL_RULE = 1,
      SEQ_RULE,
      CYCLE_RULE,
      MAX_RULE,
    };

  private:
    int64_t rule_id_[MAX_RULE_NUM];
    char rule_data_[MAX_RULE_NUM][256];
    int64_t rule_num_;
    int64_t row_num_;
};

class ValueGenerator
{
  public:
    ValueGenerator();
    ~ValueGenerator();

  public:
    // Generates a value rule for the specified prefix
    int generate_value_rule(uint64_t prefix, ValueRule& rule);

  private:
    PrefixInfo prefix_info_;
};

#endif //__VALUE_GENERATOR_H__

