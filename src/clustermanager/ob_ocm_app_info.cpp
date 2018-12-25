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

#include "ob_ocm_app_info.h"
#include "ob_define.h"
#include "tblog.h"

using namespace oceanbase::common;

namespace oceanbase
{
  namespace clustermanager
  {
    ObOcmAppInfo::ObOcmAppInfo()
    {
     // memset(app_name_, '\0', OB_MAX_APP_NAME_LENGTH);
      app_name_[0] = '\0';
      obi_count_ = 0;
      magic_num_ = reinterpret_cast<int64_t>("mAppInfo");
    }

    ObOcmAppInfo::~ObOcmAppInfo()
    {
    }

    int ObOcmAppInfo::init(const char* app_name)
    {
      int err = OB_SUCCESS;
      //int64_t app_name_len = strlen(app_name);

      if(NULL == app_name || static_cast<int64_t>(strlen(app_name)) >= OB_MAX_APP_NAME_LENGTH)
      {
        TBSYS_LOG(WARN, "invalid param, app_name=%s", app_name);
        err = OB_INVALID_ARGUMENT;
      }
      else
      {
        memcpy(app_name_, app_name, strlen(app_name) + 1);
      }
      TBSYS_LOG(INFO, "magic_num=%ld", magic_num_);
      return err;
    }

    int ObOcmAppInfo::add_obi(ObOcmInstance &instance)
    {
      int err = OB_SUCCESS;
      if(OB_MAX_OBI_COUNT <= obi_count_)
      {
        TBSYS_LOG(WARN, "cannot add any instance more, obi_count_=%ld", obi_count_);
        err = OB_INNER_STAT_ERROR;
      }
      else
      {
        int64_t i = 0;
        for(; i < obi_count_; i++)
        {
          if(0 == strcmp(instance.get_instance_name(), obi_[i].get_instance_name()))
          {
            TBSYS_LOG(WARN,"instance already exist.instance_name=%s", obi_[i].get_instance_name());
            err = OB_ENTRY_EXIST;
            break;
          }
        }
        if(obi_count_ == i)
        {
          err = obi_[obi_count_].deep_copy(instance);
          if(err == OB_SUCCESS)
          {
            obi_count_ ++;
          }
        }
      }
      return err;
    }

    int ObOcmAppInfo::add_register_node(const ObServerExt &node, const char* instance_name)
    {
      int err = OB_SUCCESS;
      //int64_t instance_name_len = strlen(instance_name);
      if(NULL == instance_name || static_cast<int64_t>(strlen(instance_name)) >= OB_MAX_INSTANCE_NAME_LENGTH)
      {
        TBSYS_LOG(WARN, "invalid param, instance_name=%s", instance_name);
        err = OB_INVALID_ARGUMENT;
      }
      else
      {
        //查找该instance是否存在，
        int64_t i = 0;
        for(; i < obi_count_; i++)
        {
          if(0 == strcmp(instance_name, obi_[i].get_instance_name()))
          {
            obi_[i].transfer_master(node);
            break;
          }
        }
        if(obi_count_ == i)
        {
          //增加instance
          ObOcmInstance *pinstance = new(std::nothrow) ObOcmInstance;
          err = pinstance->init(instance_name, node);
          if(OB_SUCCESS != err)
          {
            TBSYS_LOG(WARN, "fail to init the register node. err=%d", err);
          }
          err = obi_[i].deep_copy(*pinstance);
          if(OB_SUCCESS == err)
          {
            obi_count_++;
          }
          if(pinstance != NULL)
          {
            delete pinstance;
            pinstance = NULL;
          }
        }
      }
      return err;
    }

    int ObOcmAppInfo::transfer_master(const ObServerExt &new_master, const char* instance_name)
    {
      int err = OB_SUCCESS;
      //int64_t instance_name_len = strlen(instance_name);
      if(NULL == instance_name || static_cast<int64_t>(strlen(instance_name)) >= OB_MAX_INSTANCE_NAME_LENGTH)
      {
        TBSYS_LOG(WARN, "invalid param, instance_name=%s", instance_name);
        err = OB_INVALID_ARGUMENT;
      }
      else
      {
        int64_t i = 0; 
        for(; i < obi_count_; i++)
        {
          if(0 == strcmp(instance_name, obi_[i].get_instance_name()))
          {
            obi_[i].transfer_master(new_master);
            break;
          }
        }
        if(obi_count_ == i)
        {
          TBSYS_LOG(ERROR, "the instance not exitst. instance_name=%s", instance_name);
          err = OB_ENTRY_NOT_EXIST;
        }
      }
      return err;
    }

    int ObOcmAppInfo::deep_copy(const ObOcmAppInfo &app_info)
    {
      int err = OB_SUCCESS;
      int64_t app_name_len = strlen(app_info.get_app_name());
      if(app_name_len >= OB_MAX_APP_NAME_LENGTH)
      {
        TBSYS_LOG(WARN, "invalid param, app_name_len =%ld", app_name_len);
        err = OB_INVALID_ARGUMENT;
      }
      else
      {
        memcpy(app_name_, app_info.get_app_name(), app_name_len + 1);
        obi_count_ = app_info.get_obi_count();
        for(int i = 0; i < obi_count_; i++)
        {
          err = obi_[i].deep_copy(*app_info.get_obi(i));
          if(OB_SUCCESS != err)
          {
            TBSYS_LOG(WARN ,"deep copy fail.i=%d", i);
            break;
          }
        }
      }
      return err;
    }

    int ObOcmAppInfo::set_app_name(const char* app_name)
    {
      int err = OB_SUCCESS;
      //int64_t len = strlen(app_name);
      if(app_name == NULL || static_cast<int64_t>(strlen(app_name)) >= OB_MAX_APP_NAME_LENGTH)
      {
        TBSYS_LOG(WARN, "invalid param ,app_name=%s", app_name);
        err = OB_INVALID_ARGUMENT;
      }
      else
      {
        memcpy(app_name_, app_name, strlen(app_name) + 1);
      }
      return err;
    }

    const char* ObOcmAppInfo::get_app_name() const
    {
      return app_name_;
    }

    void ObOcmAppInfo::set_obi_count(int64_t count)
    {
      obi_count_ = count;
    }

    int64_t ObOcmAppInfo::get_obi_count()const
    {
      return obi_count_;
    }

    const ObOcmInstance* ObOcmAppInfo::get_obi(int64_t index) const
    {
      if(index >= OB_MAX_OBI_COUNT || index < 0)
      {
        TBSYS_LOG(WARN, "invalid param, index=%ld, OB_MAX_OBI_COUNT=%ld", index, OB_MAX_OBI_COUNT);
        return NULL;
      }
      else
      {
        if(index >= obi_count_)
        {
          TBSYS_LOG(WARN, "this obi have no entity");
        }
        return (obi_ + index);
      }
    }

  int ObOcmAppInfo::get_instance_role(const char* instance_name, Status& status)
  {
    int err = OB_SUCCESS; 
    //int64_t instance_name_len = strlen(instance_name);
    //if(instance_name_len >= OB_MAX_INSTANCE_NAME_LENGTH || NULL == instance_name)
    if(NULL == instance_name || static_cast<int64_t>(strlen(instance_name)) >= OB_MAX_INSTANCE_NAME_LENGTH)
    {
      TBSYS_LOG(WARN, "invalid param, instance_name=%s", instance_name);
      err = OB_INVALID_ARGUMENT;
    }
    else
    {
      int64_t i = 0;
      for(; i < obi_count_; i++)
      {
        if(0 == strcmp(obi_[i].get_instance_name(), instance_name))
        {
          status = obi_[i].get_role();
          break;
        }
      }
      if(i == obi_count_)
      {
        err = OB_ENTRY_NOT_EXIST;
      }
    }
    return err;
  }

  int ObOcmAppInfo::set_instance_role(const char* instance_name, const Status status)
  {
    int err = OB_SUCCESS; 
    if(NULL == instance_name || static_cast<int64_t>(strlen(instance_name)) >= OB_MAX_INSTANCE_NAME_LENGTH)
    {
      TBSYS_LOG(WARN, "invalid param, instance_name=%s", instance_name);
      err = OB_INVALID_ARGUMENT;
    }
    else
    {
      int64_t i = 0;
    for(; i < obi_count_; i++)
    {
      if(0 == strcmp(obi_[i].get_instance_name(), instance_name))
      {
        obi_[i].set_role(status);
      break;
      }
    }
    if(i == obi_count_)
    {
      err = OB_ENTRY_NOT_EXIST;
    }
    }
    return err;
  }
  
    int ObOcmAppInfo::get_app_master_rs(const char *app_name, ObServerExt &serv_info) const
    {
      int err = OB_SUCCESS;
      if(0 != strcmp(app_name, app_name_))
      {
        TBSYS_LOG(WARN, "invalid param, app_name=%s, app_name_=%s", app_name, app_name_);
        err = OB_INVALID_ARGUMENT;
      }
      else
      {
        int i = 0;
        for( ; i < obi_count_; i++)
        {
          err = obi_[i].get_app_master_rs(serv_info);
          if(OB_SUCCESS == err)
          {
            break;
          }
          else
          {
            //do nothing
          }
        }
        if(obi_count_ == i)
        {
          err = OB_ENTRY_NOT_EXIST;
        }
      }
      return err;
    }

    int ObOcmAppInfo::get_instance_master_rs(const char *app_name, const char *instance_name, ObServerExt &serv_info) const
    {
      int err = OB_SUCCESS;
      if(NULL == app_name || NULL == instance_name
          || static_cast<int64_t>(strlen(app_name)) >= OB_MAX_APP_NAME_LENGTH
          || static_cast<int64_t>(strlen(instance_name)) >= OB_MAX_INSTANCE_NAME_LENGTH)
      {
        TBSYS_LOG(WARN,"invalid param, app_name=%s, instance_name=%s",
            app_name, instance_name);
      err = OB_INVALID_ARGUMENT;
      }
      else
      {
        int64_t i = 0;
        for( ; i < obi_count_; i++)
        {
          char* tmp_instance_name =const_cast<char*>(instance_name);
          if(0 == strcmp(tmp_instance_name, obi_[i].get_instance_name()))
          {
            err = obi_[i].get_instance_master_rs(instance_name, serv_info);
            break;
          }
          else
          {
            //do nothing
          }
        }
        if(obi_count_ == i)
        {
          err = OB_ENTRY_NOT_EXIST;
        }
      }
      return err;
    }

    int ObOcmAppInfo::get_instance_master_rs_list(const char *app_name, ObServList &serv_list) const
    {
      int err = OB_SUCCESS;
      if(0 != strcmp(app_name, app_name_))
      {
        TBSYS_LOG(WARN, "invalid param, app_name=%p, app_name_=%p", app_name, app_name_);
        err = OB_INVALID_ARGUMENT;
      }
      else
      {
        for(int i = 0; i < obi_count_ ; i++)
        {
          err = obi_[i].get_rs_list(serv_list);
          if(err != OB_SUCCESS)
          {
            TBSYS_LOG(WARN, "get rs list fail, obi_name=%p", obi_[i].get_instance_name());
          }
        }
      }
      return err;
    }

    int ObOcmAppInfo::serialize(char* buf, const int64_t buf_len, int64_t& pos)
    {
      int err = OB_SUCCESS;

      if (NULL == buf || buf_len <= 0 || pos >= buf_len)
      {
        TBSYS_LOG(WARN, "invalid param, buf=%p, buf_len=%ld, pos=%ld", buf, buf_len, pos);
        err = OB_INVALID_ARGUMENT;
      }
      else
      {
        int64_t str_len = strlen(app_name_);
        if(pos + str_len + 3 * (int64_t)sizeof(int64_t) >= buf_len)
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
          strncpy(buf + pos, app_name_, str_len);
          pos += str_len;
          *(reinterpret_cast<int64_t*> (buf + pos)) = obi_count_;
          pos += sizeof(int64_t);
          for(int i = 0; i < obi_count_; i++)
          {
            err = obi_[i].serialize(buf, buf_len, pos);
            if(OB_SUCCESS != err)
            {
              TBSYS_LOG(WARN, "serialize the obi failed. i=%d, err=%d", i, err);
              break;
            }
          }
        }
      }
      return err;
    }

    int ObOcmAppInfo::deserialize(const char* buf, const int64_t buf_len, int64_t& pos)
    {
      int err = OB_SUCCESS;

      if (NULL == buf || buf_len <= 0 || pos >= buf_len)
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
          TBSYS_LOG(WARN, "wrong magic num, can't deserilize the buffer to ObOcmAppInfo, err=%d", err);       
        }
        else                                                                                                   
        { 
          pos += sizeof(int64_t);
          int64_t str_len = 0;
          str_len = *(reinterpret_cast<const int64_t*> (buf + pos));
          pos += sizeof(int64_t);
          strncpy(app_name_, buf + pos, str_len);
          app_name_[str_len] = '\0';
          pos += str_len;
          obi_count_ = *(reinterpret_cast<const int64_t*> (buf + pos));
          pos += sizeof(int64_t);
          for(int i = 0; i < obi_count_; i++)
          {
            err = obi_[i].deserialize(buf, buf_len, pos);
            if(OB_SUCCESS != err)
            {
              TBSYS_LOG(WARN, "obinstance deserialize failed. i=%d, err=%d", i, err);
              break;
            }
          }
        }
      }
      return err;
    }

    int ObOcmAppInfo::get_serialize_size()
    {
      int64_t ret = 0;
      for(int i = 0; i < obi_count_; i++)
      {
        ret += obi_[i].get_serialize_size();
      }
      ret = ret + strlen(app_name_) + 3 * sizeof(int64_t);
      return static_cast<int32_t>(ret);
    }
  }
}
