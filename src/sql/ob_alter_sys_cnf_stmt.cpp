/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_alter_sys_cnf_stmt.cpp
 *
 * Authors:
 *   Guibin Du <tianguan.dgb@taobao.com>
 *
 */
#include "ob_alter_sys_cnf_stmt.h"
#include "common/utility.h"

using namespace oceanbase::sql;
using namespace oceanbase::common;

bool ObSysCnfItemKey::operator==(const ObSysCnfItemKey& other) const
{
  return (this->cluster_id_ == other.cluster_id_ &&
          this->param_name_ == other.param_name_ &&
          this->server_ip_ == other.server_ip_ &&
          this->server_port_ == other.server_port_ &&
          this->server_type_ == other.server_type_);
}

int64_t ObSysCnfItemKey::hash() const
{
  uint32_t hash_val = 0;
  if (cluster_id_ != common::OB_INVALID_ID)
  {
    hash_val = murmurhash2(&cluster_id_, sizeof(cluster_id_), hash_val);
  }
  if (param_name_.length() > 0)
  {
    hash_val = murmurhash2(param_name_.ptr(), param_name_.length(), hash_val);
  }
  if (server_type_ != common::OB_INVALID)
  {
    hash_val = murmurhash2(&server_type_, sizeof(server_type_), hash_val);
  }
  if (server_ip_.length() > 0)
  {
    hash_val = murmurhash2(server_ip_.ptr(), server_ip_.length(), hash_val);
  }
  if (server_port_ != common::OB_INVALID_ID)
  {
    hash_val = murmurhash2(&server_port_, sizeof(server_port_), hash_val);
  }
  return hash_val;
};

int64_t ObSysCnfItem::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  databuff_printf(buf, buf_len, pos, "Param Name ::= %.*s, ",
                  param_name_.length(), param_name_.ptr());
  pos += param_value_.to_string(buf + pos, buf_len - pos);
  databuff_printf(buf, buf_len, pos, "Comment ::= %.*s, ",
                  comment_.length(), comment_.ptr());
  switch (config_type_)
  {
    case MEMORY:
      databuff_printf(buf, buf_len, pos, "Config Type ::= MEMORY, ");
      break;
    case SPFILE:
      databuff_printf(buf, buf_len, pos, "Config Type ::= SPFILE, ");
      break;
    case BOTH:
      databuff_printf(buf, buf_len, pos, "Config Type ::= BOTH, ");
      break;
    default:
      break;
  }
  switch (server_type_)
  {
    case OB_ROOTSERVER:
      databuff_printf(buf, buf_len, pos, "SERVER TYPE ::= ROOTSERVER, ");
      break;
    case OB_UPDATESERVER:
      databuff_printf(buf, buf_len, pos, "SERVER TYPE ::= UPDATESERVER, ");
      break;
    case OB_CHUNKSERVER:
      databuff_printf(buf, buf_len, pos, "SERVER TYPE ::= CHUNKSERVER, ");
      break;
    case OB_MERGESERVER:
      databuff_printf(buf, buf_len, pos, "SERVER TYPE ::= MERGESERVER, ");
      break;
    default:
      databuff_printf(buf, buf_len, pos, "SERVER TYPE ::= NONESERVER, ");
      break;
  }
  if (cluster_id_ != OB_INVALID_ID)
  {
    databuff_printf(buf, buf_len, pos, "CLUSTER ::= %lu\n", cluster_id_);
  }
  else
  {
    databuff_printf(buf, buf_len, pos, "SERVER IP ::= %.*s, ",
                    server_ip_.length(), server_ip_.ptr());
    databuff_printf(buf, buf_len, pos, "SERVER PORT ::= %lu\n", server_port_);
  }
  return pos;
}

ObAlterSysCnfStmt::ObAlterSysCnfStmt()
  : ObBasicStmt(ObBasicStmt::T_ALTER_SYSTEM)
{
}

ObAlterSysCnfStmt::~ObAlterSysCnfStmt()
{
}

int ObAlterSysCnfStmt::init()
{
  int ret = OB_SUCCESS;
  ret = sys_cnf_map_.create(hash::cal_next_prime(OB_MAX_CONFIG_VALUE_LEN));
  return ret;
}

int ObAlterSysCnfStmt::add_sys_cnf_item(ResultPlan& result_plan, const ObSysCnfItem& sys_cnf_item)
{
  int& ret = result_plan.err_stat_.err_code_ = OB_SUCCESS;
  ObSysCnfItemKey sys_cnf_key;
  sys_cnf_key.cluster_id_ = sys_cnf_item.cluster_id_;
  sys_cnf_key.param_name_ = sys_cnf_item.param_name_;
  sys_cnf_key.server_type_ = sys_cnf_item.server_type_;
  sys_cnf_key.server_ip_ = sys_cnf_item.server_ip_;
  sys_cnf_key.server_port_ = sys_cnf_item.server_port_;
  if (sys_cnf_map_.get(sys_cnf_key) != NULL)
  {
    ret = OB_ERR_PARAM_DUPLICATE;
    snprintf(result_plan.err_stat_.err_msg_, MAX_ERROR_MSG,
        "Duplicate param name '%.*s'",
        sys_cnf_item.param_name_.length(), sys_cnf_item.param_name_.ptr());
  }
  else if (sys_cnf_map_.set(sys_cnf_key, sys_cnf_item, 0) != hash::HASH_INSERT_SUCC)
  {
    ret = OB_ERR_RESOLVE_SQL;
    snprintf(result_plan.err_stat_.err_msg_, MAX_ERROR_MSG,
        "Add new param '%.*s' failed",
        sys_cnf_item.param_name_.length(), sys_cnf_item.param_name_.ptr());
  }
  return ret;
}

void ObAlterSysCnfStmt::print(FILE* fp, int32_t level, int32_t index)
{
  UNUSED(index);
  print_indentation(fp, level);
  fprintf(fp, "ObAlterSysCnfStmt Begin\n");
  print_indentation(fp, level + 1);
  fprintf(fp, "Param(s) ::=\n");
  int32_t i = 1;
  int64_t pos = 0;
  char buf[OB_MAX_VARCHAR_LENGTH];
  hash::ObHashMap<ObSysCnfItemKey, ObSysCnfItem>::iterator iter;
  for (iter = sys_cnf_begin(); iter != sys_cnf_end(); iter++)
  {
    print_indentation(fp, level + 2);
    fprintf(fp, "Column(%d) ::=\n", i++);
    iter->second.to_string(buf, OB_MAX_VARCHAR_LENGTH);
    print_indentation(fp, level + 3);
    fprintf(fp, "%.*s", static_cast<int32_t>(pos), buf);
  }
  print_indentation(fp, level);
  fprintf(fp, "ObAlterSysCnfStmt End\n");
}


