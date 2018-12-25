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

#include "ob_ocm_instance.h"
#include "tblog.h"
#include <stdio.h>
#include <string.h>

using namespace oceanbase::common;
namespace oceanbase
{
  namespace clustermanager
  {
    ObOcmInstance::ObOcmInstance():role_(SLAVE),read_radio_(0),reserve1_(0),reserve2_(0)
    {
      memset(instance_name_, '\0', OB_MAX_INSTANCE_NAME_LENGTH);
      magic_num_ = reinterpret_cast<int64_t>("Instance");
    }

    ObOcmInstance::~ObOcmInstance()
    {
    }

    int ObOcmInstance::init(const char* instance_name, ObServerExt master_rs)
    {
      int err = OB_SUCCESS;
      if(NULL == instance_name || static_cast<int64_t>(strlen(instance_name)) >= OB_MAX_INSTANCE_NAME_LENGTH)
      {
        TBSYS_LOG(WARN, "invalid param, instance_name=%s", instance_name);
        err = OB_INVALID_ARGUMENT;
      }
      else
      {
        memcpy(instance_name_, instance_name, strlen(instance_name) + 1);
        master_rs_.init(master_rs.get_hostname(), master_rs.get_server());
      }
       TBSYS_LOG(INFO, "magic_num=%ld", magic_num_);
      return err;
    }

    Status ObOcmInstance::get_role() const
    {
      return role_;
    }

    int64_t ObOcmInstance:: get_read_radio() const
    {
      return read_radio_;
    }

    void ObOcmInstance::set_role(Status state)
    {
      role_ = state;
    }

    int ObOcmInstance::set_read_radio(const int64_t radio)
    {
      int err = OB_SUCCESS;
      if(radio < 0 || radio > 100)
      {
        TBSYS_LOG(WARN, "invalid param, radio=%ld", radio);
        err = OB_INVALID_ARGUMENT;
      }
      else
      {
        read_radio_ = radio;
      }
      return err;
    }
    char* ObOcmInstance::get_instance_name()
    {
      return instance_name_;
    }

    const char* ObOcmInstance::get_instance_name() const
    {
      return instance_name_;
    }

    int ObOcmInstance::set_instance_name(const char * instance_name)
    {
      int err = OB_SUCCESS;
      if(NULL == instance_name || static_cast<int64_t>(strlen(instance_name)) >= OB_MAX_INSTANCE_NAME_LENGTH)
      {
        TBSYS_LOG(WARN, "invalid param, instance_name=%s", instance_name);
        err = OB_INVALID_ARGUMENT;
      }
      else
      {
        memcpy(instance_name_, instance_name, strlen(instance_name) + 1);
      }
      return err;
    }
    
    //get the master rs, if the instance is master instance ,or return not exist
    int ObOcmInstance::get_app_master_rs(ObServerExt &serv_info) const
    {
      int err = OB_SUCCESS;
      if(role_ == MASTER)
      {
        serv_info.set_hostname(master_rs_.get_hostname());
        serv_info.get_server().set_ipv4_addr(master_rs_.get_server().get_ipv4(), master_rs_.get_server().get_port());
      }
      else
      {
        err = OB_ENTRY_NOT_EXIST;
      }
      return err;
    }

    int ObOcmInstance::get_instance_master_rs(const char *instance_name, ObServerExt &serv_info) const
    {
      int err = OB_SUCCESS;
      if(0 != strcmp(instance_name, instance_name_))
      {
        TBSYS_LOG(WARN, "invalid argument,instance_name=%p, instance_name_=%p", instance_name, instance_name_);
        err = OB_INVALID_ARGUMENT;
      }
      else
      {
        serv_info.set_hostname(master_rs_.get_hostname());
        serv_info.get_server().set_ipv4_addr(master_rs_.get_server().get_ipv4(), master_rs_.get_server().get_port());
      }
      return err;
    }
    int ObOcmInstance::get_rs_list(ObServList& serv_list)
    {
      int err = OB_SUCCESS;
      err = serv_list.push_back(&master_rs_);
      if(err != OB_SUCCESS)
      {
        TBSYS_LOG(WARN, "get_rs_list fail.");
      }
      return err;
    }

    const int ObOcmInstance::get_rs_list(ObServList& serv_list) const
    {
      int err = OB_SUCCESS;
      //ObServerExt *tmp_server = const_cast<ObServerExt *>(&master_rs_);
      err = serv_list.push_back(const_cast<ObServerExt*>(&master_rs_));
      if(err != OB_SUCCESS)
      {
        TBSYS_LOG(WARN, "get_rs_list fail.");
      }
      return err;
    }
    int ObOcmInstance::serialize(char* buf, const int64_t buf_len, int64_t& pos)const
    {
      int err = OB_SUCCESS;

      if(NULL == buf || buf_len <= 0 || pos >= buf_len)
      {
        TBSYS_LOG(WARN, "invalid param, buf=%p, buf_len=%ld, pos=%ld", buf, buf_len, pos);
        err = OB_INVALID_ARGUMENT;
      }
      else
      {
        int64_t len = strlen(instance_name_);
        if(pos + 6 * (int64_t)sizeof(int64_t) + len >= buf_len)
        {
          TBSYS_LOG(WARN, "buf is not enough, pos=%ld, buf_len=%ld", pos, buf_len);
          err = OB_ERROR;
        }
        else
        {
          *(reinterpret_cast<int64_t*> (buf + pos)) = magic_num_;
          pos += sizeof(int64_t);
          *(reinterpret_cast<int64_t*> (buf + pos)) = len;
          pos += sizeof(int64_t);
          strncpy(buf + pos, instance_name_, len);
          pos += len;
          *(reinterpret_cast<int64_t*> (buf + pos)) = role_;
          pos += sizeof(int64_t);
          *(reinterpret_cast<int64_t*> (buf + pos)) = read_radio_;
          pos += sizeof(int64_t);
          *(reinterpret_cast<int64_t*> (buf + pos)) = reserve1_;
          pos += sizeof(int64_t);
          *(reinterpret_cast<int64_t*> (buf + pos)) = reserve2_;
          pos += sizeof(int64_t);

          err = master_rs_.serialize(buf, buf_len, pos);
          if(OB_SUCCESS != err)
          {
            TBSYS_LOG(WARN, "ObServerExt master_rs serialize fail");
          }
        }
      }
      return err;
    }

    int ObOcmInstance::deserialize(const char* buf, const int64_t buf_len, int64_t& pos)
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
        magic_num = *(reinterpret_cast <const int64_t*>(buf + pos));
        if(magic_num != magic_num_)
        {
          err = OB_NOT_THE_OBJECT;
          TBSYS_LOG(WARN, "wrong magic num, can't deserilize the buffer to ObOcmInstance, err=%d", err);
        }
        else
        {
          pos += sizeof(int64_t);
          int64_t len = *(reinterpret_cast<const int64_t*> (buf + pos));
          pos += sizeof(int64_t);
          strncpy(instance_name_, buf + pos, len);
          pos += len;
          role_ = *(reinterpret_cast<const Status*> (buf + pos));
          pos += sizeof(int64_t);
          read_radio_ = *(reinterpret_cast<const int64_t*> (buf + pos));
          pos += sizeof(int64_t);
          reserve1_ = *(reinterpret_cast<const int64_t*> (buf + pos));
          pos += sizeof(int64_t);
          reserve2_ = *(reinterpret_cast<const int64_t*> (buf + pos));
          pos += sizeof(int64_t);

          err = master_rs_.deserialize(buf, buf_len, pos);
          if(OB_SUCCESS != err)
          {
            TBSYS_LOG(WARN, "ObServerExt master_rs deserialize fail");
          }
        }
      }
      return err;
    }

    int64_t ObOcmInstance::get_serialize_size(void)const
    {
      return master_rs_.get_serialize_size() + 6 * sizeof(int64_t) + strlen(instance_name_);
    }

    const ObServerExt& ObOcmInstance::get_master_rs() const
    {
      return master_rs_;
    }

    ObServerExt& ObOcmInstance::get_master_rs()
    {
      return master_rs_;
    }

    int64_t ObOcmInstance::get_reserve1()const
    {
      return reserve1_;
    }

    int64_t ObOcmInstance::get_reserve2() const
    {
      return reserve2_;
    }
    void ObOcmInstance::set_reserve2(int64_t reserve_value)
    {
      reserve2_ = reserve_value;
    }
    void ObOcmInstance::set_reserve1(int64_t reserve_value)
    {
      reserve1_ = reserve_value;
    }

    int ObOcmInstance::deep_copy(const ObOcmInstance &instance)
    {
      int err = OB_SUCCESS;
      int64_t instance_name_len = strlen(instance.get_instance_name());
      memcpy(instance_name_, instance.get_instance_name(), instance_name_len + 1);
      role_ = instance.get_role();
      read_radio_ = instance.get_read_radio();
      reserve1_ = instance.get_reserve1();
      reserve2_ = instance.get_reserve2();
      master_rs_.deep_copy(instance.get_master_rs());
      return err;
    }

     void ObOcmInstance::transfer_master(const ObServerExt &new_master)
     {
       master_rs_.deep_copy(new_master);
     }
  }
}
