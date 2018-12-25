/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 *
 * Version: 0.1: key_generator.cpp,v 0.1 2012/02/22 12:33:10 chuanhui Exp $
 *
 * Authors:
 *   chuanhui <rizhao.ych@taobao.com>
 *     - some work details if you want
 *
 */
#include "key_generator.h"
#include "tbnetutil.h"

static const int64_t PREFIX_BITS = 48;

void Rule::add_rule(GenType prefix_type, GenType suffix_type, int percent)
{
  assert(rule_num_ < MAX_RULE_NUM);
  assert(percent > 0);
  percents_[rule_num_] = percent;
  prefix_types_[rule_num_] = prefix_type;
  suffix_types_[rule_num_] = suffix_type;
  sum_percent_ += percent;
  ++rule_num_;
}

void Rule::get_gen_type(GenType& prefix_type, GenType& suffix_type)
{
  assert(rule_num_ > 0 && sum_percent_ > 0);
  int rand_val = rand() % sum_percent_;
  int sum = 0;
  int i = 0;
  for (i = 0; i < rule_num_; ++i)
  {
    sum += percents_[i];
    if (rand_val <= sum)
    {
      prefix_type = prefix_types_[i];
      suffix_type = suffix_types_[i];
      break;
    }
  }
  assert(i < rule_num_);
}

KeyRule::KeyRule()
{
  is_init_ = false;
}

KeyRule::~KeyRule()
{
}

int KeyRule::init(Rule& read_rule, Rule& write_rule)
{
  read_rule_ = read_rule;
  write_rule_ = write_rule;
  is_init_ = true;

  return 0;
}

void KeyRule::get_read_gen_type(GenType& prefix_type, GenType& suffix_type)
{
  assert(is_init_ == true);
  read_rule_.get_gen_type(prefix_type, suffix_type);
}

void KeyRule::get_write_gen_type(GenType& prefix_type, GenType& suffix_type)
{
  assert(is_init_ == true);
  write_rule_.get_gen_type(prefix_type, suffix_type);
}

KeyGenerator::KeyGenerator()
{
  client_id_ = 0;
}

KeyGenerator::~KeyGenerator()
{
}

int KeyGenerator::init(ObSqlClient& ob_client, KeyRule& key_rule)
{
  key_rule_ = key_rule;
  int err = prefix_info_.init(ob_client);
  if (0 != err)
  {
    TBSYS_LOG(WARN, "failed to init prefix info, err=%d", err);
  }

  if (0 == err)
  {
    uint32_t local_addr = ntohl(tbsys::CNetUtil::getLocalAddr(NULL));
    client_id_ = local_addr & 0xffff;

    TBSYS_LOG(INFO, "[CLIENT_ID] client_id=%lu", client_id_);
  }

  return err;
}

int KeyGenerator::get_next_key(uint64_t& prefix, uint64_t& suffix, bool is_read)
{
  int err = 0;
  GenType prefix_type = (GenType) 0;
  GenType suffix_type = (GenType) 0;

  if (is_read)
  {
    key_rule_.get_read_gen_type(prefix_type, suffix_type);
  }
  else
  {
    key_rule_.get_write_gen_type(prefix_type, suffix_type);
  }

  err = get_next_key(prefix, prefix_type);
  if (0 != err)
  {
    TBSYS_LOG(WARN, "failed to get prefix, type=%d, err=%d", prefix_type, err);
  }
  else
  {
    err = get_next_key(prefix, suffix, suffix_type);
    if (0 != err)
    {
      TBSYS_LOG(WARN, "failed to get suffix, type=%d, prefix=%lu, err=%d",
          suffix_type, prefix, err);
    }
  }
  return err;
}

int KeyGenerator::get_next_key(uint64_t prefix, uint64_t& suffix, GenType type)
{
  int err = 0;
  uint64_t max_suffix = 0;

  switch (type)
  {
    case GEN_SEQ:
      prefix_info_.get_key(prefix, suffix);
      prefix_info_.set_key(prefix, suffix + 1);
      break;

    case GEN_RANDOM:
      prefix_info_.get_key(prefix, max_suffix);
      suffix = rand() % (max_suffix + 1);
      break;

    case GEN_FIXED:
      TBSYS_LOG(WARN, "not supported, type=%d", type);
      err = OB_ERROR;
      break;

    case GEN_SEQ_SHUFFLE:
      TBSYS_LOG(WARN, "not supported, type=%d", type);
      err = OB_ERROR;
      break;

    default:
      TBSYS_LOG(WARN, "invalid gen type, type=%d", type);
      err = OB_ERROR;
      break;
  }

  return err;
}

int KeyGenerator::get_next_key(uint64_t& prefix, GenType type)
{
  int err = 0;
  uint64_t max_prefix = 0;

  int64_t MAX_PREFIX_NUM = 100 * 1024L;

  switch (type)
  {
    case GEN_SEQ:
      prefix_info_.get_max_prefix(client_id_, max_prefix);
      prefix_info_.set_max_prefix(client_id_, max_prefix + 1);
      prefix = max_prefix % MAX_PREFIX_NUM | (client_id_ << PREFIX_BITS);
      break;

    case GEN_RANDOM:
      prefix_info_.get_max_prefix(client_id_, max_prefix);
      prefix = rand() % (max_prefix + 1);
      prefix = prefix % MAX_PREFIX_NUM | (client_id_ << PREFIX_BITS);
      break;

    case GEN_FIXED:
      TBSYS_LOG(WARN, "not supported, type=%d", type);
      err = OB_ERROR;
      break;

    case GEN_SEQ_SHUFFLE:
      TBSYS_LOG(WARN, "not supported, type=%d", type);
      err = OB_ERROR;
      break;

    default:
      TBSYS_LOG(WARN, "invalid gen type, type=%d", type);
      err = OB_ERROR;
      break;
  }

  return err;
}

