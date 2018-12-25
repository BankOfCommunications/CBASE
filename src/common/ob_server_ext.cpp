/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 *
 * Version: 0.1: ob_ocm_info_manager.h,v 0.1 2010/09/28 13:39:26 rongxuan Exp $
 *
 * Authors:
 *   rongxuan <rongxuan.lc@taobao.com>
 *     - some work details if you want
 *
 */

#include "tblog.h"
#include "ob_server_ext.h"
#include "ob_define.h"
using namespace oceanbase::common;

ObServerExt::ObServerExt()
{
  //memset(hostname_, '\0', OB_MAX_HOST_NAME_LENGTH);
  hostname_[0] = '\0';
  magic_num_ = reinterpret_cast<int64_t>("erverExt");
}

ObServerExt::~ObServerExt()
{
}

int ObServerExt::init(char *hname, common::ObServer server)
{
  int err = OB_SUCCESS;
  if(NULL == hname || static_cast<int64_t>(strlen(hname)) >= OB_MAX_HOST_NAME_LENGTH)
  {
    TBSYS_LOG(WARN, "invalid param, hname=%s", hname);
    err = OB_INVALID_ARGUMENT;
  }
  else
  {
    memcpy(hostname_, hname, strlen(hname) + 1);
    server_.set_ipv4_addr(server.get_ipv4(), server.get_port());
  }
   TBSYS_LOG(INFO, "magic_num=%ld", magic_num_);
  return err;
}

const char* ObServerExt::get_hostname() const
{
  return hostname_;
}

void ObServerExt::deep_copy(const ObServerExt& server_ext)
{
  int64_t server_name_len = strlen(server_ext.get_hostname());
  memcpy(hostname_, server_ext.get_hostname(), server_name_len + 1);
  server_.set_ipv4_addr(server_ext.get_server().get_ipv4(), server_ext.get_server().get_port());
}

char* ObServerExt::get_hostname()
{
  return hostname_;
}
int ObServerExt::set_hostname(const char * hname)
{
  int err = OB_SUCCESS;
  if(NULL == hname || static_cast<int64_t>(strlen(hname)) >= OB_MAX_HOST_NAME_LENGTH)
  {
    TBSYS_LOG(WARN, "invalid param, hname=%s", hname);
    err = OB_INVALID_ARGUMENT;
  }
  else
  {
    memcpy(hostname_, hname, strlen(hname) + 1);
  }
  return err;
}

const ObServer& ObServerExt::get_server()const
{
  return server_;
}

ObServer& ObServerExt::get_server()
{
  return server_;
}

int ObServerExt::serialize(char* buf, const int64_t buf_len, int64_t& pos)const
{
  int err = OB_SUCCESS;

  if(NULL == buf || buf_len <= 0 || pos >= buf_len)
  {
    TBSYS_LOG(WARN, "invalid param, buf=%p, buf_len=%ld, pos=%ld", buf, buf_len, pos);
    err = OB_INVALID_ARGUMENT;
  }
  else
  {
    int64_t str_len = strlen(hostname_);
    if(pos  + str_len + (int64_t)sizeof(int64_t) * 2 >= buf_len)
    {
      TBSYS_LOG(WARN, "buf is not enough, pos=%ld, buf_len=%ld", pos, buf_len);
      err = OB_ERROR;
    }
    else
    {
      *(reinterpret_cast<int64_t*> (buf + pos)) = magic_num_;
      pos += sizeof(int64_t);
      *(reinterpret_cast<int64_t*> (buf + pos)) = str_len;
      pos += sizeof(int64_t);
      strncpy(buf + pos, hostname_, str_len);
      pos += str_len;
      err = server_.serialize(buf, buf_len, pos);
      if(OB_SUCCESS != err)
      {
        TBSYS_LOG(WARN, "ObServer rs_server serialize fail");
      }
    }
  }
  return err;
}

int ObServerExt::deserialize(const char* buf, const int64_t buf_len, int64_t& pos)
{
  int err = OB_SUCCESS;

  if(NULL == buf || buf_len <=0 || pos >=buf_len)
  {
    TBSYS_LOG(WARN, "invalid param, buf=%p, buf_len=%ld, pos=%ld", buf, buf_len, pos);
    err = OB_INVALID_ARGUMENT;
  }
  else
  {
    int64_t magic_num = 0;
    magic_num = *(reinterpret_cast<const int64_t*>(buf + pos));
    if(magic_num_ != magic_num)
    {
      err = OB_NOT_THE_OBJECT;
      TBSYS_LOG(WARN, "wrong magic num, can't deserilize the buffer to ObServerExt, err=%d", err);
    }
    else
    {
      pos += sizeof(int64_t);
      int64_t str_len = 0;
      str_len = *(reinterpret_cast<const int64_t*> (buf + pos));
      pos += sizeof(int64_t);
      strncpy(hostname_, buf + pos, str_len);
      hostname_[str_len] = '\0';
      pos += str_len;
      err = server_.deserialize(buf, buf_len, pos);
      if(OB_SUCCESS != err)
      {
        TBSYS_LOG(WARN, "ObServer rs_server deserialize fail.");
      }
    }
  }
  return err;
}

int64_t ObServerExt::get_serialize_size(void)const
{
  return server_.get_serialize_size() + strlen(hostname_) + 2 * (int64_t)sizeof(int64_t);
}
