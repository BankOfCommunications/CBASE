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

KeyGenerator::KeyGenerator()
{
  client_id_ = 0;
  prefix_info_ = NULL;
}

KeyGenerator::~KeyGenerator()
{
  prefix_info_ = NULL;
}

int KeyGenerator::init(PrefixInfo& prefix_info, BigqueryTestParam& param)
{
  int err = 0;

  prefix_info_ = &prefix_info;
  uint32_t local_addr = ntohl(tbsys::CNetUtil::getLocalAddr(NULL));
  client_id_ = local_addr & 0xffff;

  TBSYS_LOG(INFO, "[CLIENT_ID] client_id=%lu", client_id_);

  return err;
}

int KeyGenerator::get_next_key(uint64_t& prefix,  bool is_read)
{
  int err = 0;

  err = get_next_key(prefix, is_read ? GEN_RANDOM : GEN_SEQ);
  if (0 != err && OB_NEED_RETRY != err)
  {
    TBSYS_LOG(WARN, "failed to get prefix, err=%d", err);
  }

  return err;
}

int KeyGenerator::get_next_key(uint64_t& prefix, GenType type)
{
  int err = 0;
  uint64_t max_prefix = 0;

  int64_t MAX_PREFIX_NUM = 100 * 1024L * 1024L;

  switch (type)
  {
    case GEN_SEQ:
      prefix_info_->get_max_prefix(client_id_, max_prefix);
      prefix_info_->set_max_prefix(client_id_, max_prefix + 1);
      prefix = (max_prefix % MAX_PREFIX_NUM) | (client_id_ << PREFIX_BITS);
      break;

    case GEN_RANDOM:
      prefix_info_->get_max_prefix(client_id_, max_prefix);
      if (0 == max_prefix)
      {
        err = OB_NEED_RETRY;
      }
      else
      {
        prefix = rand() % max_prefix;
        prefix = (prefix % MAX_PREFIX_NUM) | (client_id_ << PREFIX_BITS);
      }
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



