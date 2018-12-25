/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 *
 * Version: 0.1: value_generator.cpp,v 0.1 2012/04/06 15:22:22 chuanhui Exp $
 *
 * Authors:
 *   chuanhui <rizhao.ych@taobao.com>
 *     - some work details if you want
 *
 */
#include "value_generator.h"

ValueRule::ValueRule()
{
  rule_num_ = 0;
  row_num_ = 0;
}

ValueRule::~ValueRule()
{
}

void ValueRule::reset()
{
  rule_num_ = 0;
  row_num_ = 0;
}

void ValueRule::add_equal_rule(int64_t value)
{
  rule_id_[rule_num_] = EQUAL_RULE;
  sprintf(rule_data_[rule_num_], "%ld", value);
  ++rule_num_;
}

void ValueRule::add_seq_rule(int64_t start_val, int64_t step)
{
  rule_id_[rule_num_] = SEQ_RULE;
  sprintf(rule_data_[rule_num_], "%ld,%ld", start_val, step);
  ++rule_num_;
}

void ValueRule::add_cycle_rule(int64_t start_val, int64_t step, int64_t size_cycle, int64_t num_cycles)
{
  rule_id_[rule_num_] = CYCLE_RULE;
  sprintf(rule_data_[rule_num_], "%ld,%ld,%ld,%ld", start_val, step, size_cycle, num_cycles);
  ++rule_num_;
}

int64_t ValueRule::get_row_num()
{
  return row_num_;
}

void ValueRule::set_row_num(int64_t row_num)
{
  row_num_ = row_num;
}

int64_t ValueRule::get_rule_num()
{
  return rule_num_;
}

int64_t ValueRule::get_rule_id(int64_t rule_idx)
{
  assert(rule_idx >= 0 && rule_idx < rule_num_);
  return rule_id_[rule_idx];
}

void ValueRule::get_equal_rule(int64_t rule_idx, int64_t& value)
{
  assert(rule_idx >= 0 && rule_idx < rule_num_);
  assert(rule_id_[rule_idx] == EQUAL_RULE);

  sscanf(rule_data_[rule_idx], "%ld", &value);
}

void ValueRule::get_seq_rule(int64_t rule_idx, int64_t& start_val, int64_t& step)
{
  assert(rule_idx >= 0 && rule_idx < rule_num_);
  assert(rule_id_[rule_idx] == SEQ_RULE);

  sscanf(rule_data_[rule_idx], "%ld,%ld", &start_val, &step);
}

void ValueRule::get_cycle_rule(int64_t rule_idx, int64_t& start_val, int64_t& step,
    int64_t& size_cycle, int64_t& num_cycles)
{
  assert(rule_idx >= 0 && rule_idx < rule_num_);
  assert(rule_id_[rule_idx] == CYCLE_RULE);

  sscanf(rule_data_[rule_idx], "%ld,%ld,%ld,%ld", &start_val, &step, &size_cycle, &num_cycles);
}

void ValueRule::get_col_values(uint64_t suffix, int64_t* col_ary, int64_t col_size, int64_t& ret_size)
{
  assert(rule_num_ <= col_size);
  ret_size = rule_num_;
  for (int64_t i = 0; i < rule_num_; ++i)
  {
    get_value(suffix, i, col_ary[i]);
  }
}

const char* ValueRule::to_string()
{
  static char buf[1024];
  serialize(buf, sizeof(buf) - 1);
  buf[sizeof(buf) - 1] = '\0';
  return buf;
}

int ValueRule::serialize(char* buf, int64_t buf_size)
{
  int err = 0;
  assert(buf != NULL && buf_size > 0);

  int64_t buf_pos = 0;

  for (int64_t i = 0; i < rule_num_; ++i)
  {
    int64_t len = snprintf(buf + buf_pos, buf_size - buf_pos, "%ld,%s", rule_id_[i], rule_data_[i]);
    assert(buf_pos + len < buf_size);
    buf_pos += len;
    if (i != rule_num_ - 1)
    {
      len = snprintf(buf + buf_pos, buf_size - buf_pos, ":");
      assert(buf_pos + len < buf_size);
      buf_pos += len;
    }
  }

  buf[buf_pos] = '\0';

  return err;
}

int ValueRule::deserialize(const char* ori_buf, int64_t buf_size)
{
  int err = 0;
  assert(ori_buf != NULL && buf_size > 0);
  assert(rule_num_ == 0);
  char buf[256];
  memcpy(buf, ori_buf, buf_size);
  char* saveptr = NULL;

  int64_t buf_pos = 0;

  char* token = strtok_r(buf + buf_pos, ":", &saveptr);
  if (NULL == token)
  {
    TBSYS_LOG(WARN, "no any token, rule_data=%s", buf + buf_pos);
    err = OB_ERROR;
  }
  else
  {
    sscanf(token, "%ld,%s", &rule_id_[rule_num_], rule_data_[rule_num_]);
    ++rule_num_;
    while (NULL != (token = strtok_r(NULL, ":", &saveptr)))
    {
      sscanf(token, "%ld,%s", &rule_id_[rule_num_], rule_data_[rule_num_]);
      ++rule_num_;
    }
  }

  return err;
}

void ValueRule::get_value(uint64_t suffix, int64_t col_idx, int64_t& value)
{
  switch (rule_id_[col_idx])
  {
    case EQUAL_RULE:
      get_equal_value(suffix, col_idx, value);
      break;
    case SEQ_RULE:
      get_seq_value(suffix, col_idx, value);
      break;
    case CYCLE_RULE:
      get_cycle_value(suffix, col_idx, value);
      break;
    default:
      TBSYS_LOG(WARN, "invalid rule_id, rule_id=%ld", rule_id_[col_idx]);
      break;
  }
}

void ValueRule::get_equal_value(uint64_t suffix, int64_t col_idx, int64_t& value)
{
  sscanf(rule_data_[col_idx], "%ld", &value);
}

void ValueRule::get_seq_value(uint64_t suffix, int64_t col_idx, int64_t& value)
{
  int64_t start_val = 0;
  int64_t step = 0;
  sscanf(rule_data_[col_idx], "%ld,%ld", &start_val, &step);
  value = start_val + suffix * step;
}

void ValueRule::get_cycle_value(uint64_t suffix, int64_t col_idx, int64_t& value)
{
  int64_t start_val = 0;
  int64_t step = 0;
  int64_t size_cycle = 0;
  int64_t num_cycles = 0;
  sscanf(rule_data_[col_idx], "%ld,%ld,%ld,%ld", &start_val, &step, &size_cycle, &num_cycles);
  value = start_val + (suffix % size_cycle) * step;
}

ValueGenerator::ValueGenerator()
{
  // empty
}

ValueGenerator::~ValueGenerator()
{
  // empty
}

int ValueGenerator::generate_value_rule(uint64_t prefix, ValueRule& rule)
{
  int err = 0;

  UNUSED(prefix);
  int ref_row_num = 1000 * 1000;
  //int ref_row_num = 50;

  // TODO(rizhao) add more rule
  int rand_val = rand() % 3;
  switch (rand_val)
  {
    case 0:
      {
        // equal value
        rule.add_equal_rule(rand() % 100 + 1);
        rule.set_row_num(rand() % ref_row_num + 1024);
        break;
      }

    case 1:
      {
        // seq value + equal val
        int64_t start_val = rand() % 100 + 1;
        int64_t step = rand() % 5 + 1;
        rule.add_seq_rule(start_val, step);
        rule.add_equal_rule(rand() % 100 + 1);
        rule.set_row_num(rand() % ref_row_num + 1024);
        break;
      }

    case 2:
      {
        // cycle value + equal value
        int64_t start_val = rand() % 100 + 1;
        int64_t step = rand() % 5 + 1;
        int64_t size_cycle = rand() % 100 + 100;
        int64_t num_cycles = (rand() % ref_row_num + 1024) / size_cycle;
        rule.add_cycle_rule(start_val, step, size_cycle, num_cycles);
        rule.add_equal_rule(rand() % 100 + 1);
        rule.set_row_num(size_cycle * num_cycles);
        break;
      }

    default:
      TBSYS_LOG(WARN, "not supported, rand_val=%d", rand_val);
      break;
  }

  return err;
}

