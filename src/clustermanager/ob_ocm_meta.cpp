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

#include <new>

#include "ob_ocm_meta.h"

namespace oceanbase
{
  namespace clustermanager
  {
    ObOcmMeta::ObOcmMeta()
    {
      app_count_ = 0;
      init_ = false;
      //memset(location_, '\0', OB_MAX_LOCATION_LENGTH);
      location_[0] = '\0';
      app_ = new(std::nothrow) ObOcmAppInfo[OB_MAX_APP_COUNT];
      if(NULL == app_)
      {
        TBSYS_LOG(WARN,"init ObOcmMeta fail");
      }
      else
      {
        init_ = true;
      }
      magic_num_ = reinterpret_cast<int64_t>("bOcmMeta");
      TBSYS_LOG(INFO, "magic num is %ld", magic_num_);
    }

    ObOcmMeta& ObOcmMeta::operator = (const ObOcmMeta & other)
    {
      int64_t location_len = strlen(other.get_location());
      memcpy(location_, other.get_location(), location_len + 1);
      app_count_ = other.get_app_count();

      ocm_server_.deep_copy(*other.get_ocm_server());
      for(int i = 0; i < app_count_; i++)
      {
        app_[i].deep_copy(*other.get_app(i));
      }
      return *this;
    }

    int ObOcmMeta::set_ocm_server(char *host_name, const common::ObServer server)
    {
      int err = OB_SUCCESS;
      //int64_t len = strlen(host_name);
      if(NULL == host_name || static_cast<int64_t>(strlen(host_name)) >= OB_MAX_HOST_NAME_LENGTH)
      {
        TBSYS_LOG(WARN, "invalid param, hname=%s", host_name);
        err = OB_INVALID_ARGUMENT;
      }
      else
      {
        err = ocm_server_.init(host_name, server);
      }
      return err;
    }

    const ObOcmAppInfo* ObOcmMeta::get_app(int64_t index) const
    {
      tbsys::CRLockGuard guard(ocm_rwlock_);
      if(index >= app_count_)
      {
        return NULL;
      }
      else
        return app_ + index;
    }

    int ObOcmMeta::add_app(ObOcmAppInfo &app_info)
    {
      int err = OB_SUCCESS;
      // tbsys::CWLockGuard guard(ocm_rwlock_);
      if(OB_MAX_APP_COUNT <= app_count_ || init_ == false)
      {
        TBSYS_LOG(INFO, "inner stat err, app_count_=%ld, init=%d", app_count_, init_);
        err = OB_INNER_STAT_ERROR;
      }
      else
      {
        int64_t i = 0;
        for(; i < app_count_; i++)
        {
          if(0 == strcmp(app_[i].get_app_name(), app_info.get_app_name()))
          {
            TBSYS_LOG(WARN,"app already exist.app_name=%s", app_[i].get_app_name());
            err = OB_ENTRY_EXIST;
            break;
          }
        }
        if(app_count_ == i)
        {
          err = app_[i].deep_copy(app_info);
          if(err == OB_SUCCESS)
          {
            app_count_ ++;
          }
        }
      }
      return err;
    }

    int ObOcmMeta::add_register_node(const ObServerExt &node, const char* instance_name, const char* app_name)
    {
      int err = OB_SUCCESS;
      //int64_t instance_name_len = strlen(instance_name);
      //int64_t app_name_len = strlen(app_name);

      if(NULL == instance_name || NULL == app_name
          || static_cast<int64_t>(strlen(instance_name)) >= OB_MAX_INSTANCE_NAME_LENGTH
          || static_cast<int64_t>(strlen(app_name)) >= OB_MAX_APP_NAME_LENGTH)
      {
        TBSYS_LOG(WARN, "invalid param, instance_name=%s, app_name=%s", instance_name, app_name);
        err = OB_INVALID_ARGUMENT;
      }
      else
      {
        int64_t i = 0; 
        tbsys::CWLockGuard guard(ocm_rwlock_);
        for(; i < app_count_ ; i++)
        {
          if(0 == strcmp(app_name, app_[i].get_app_name()))
          {
            err = app_[i].add_register_node(node, instance_name);
            if(OB_SUCCESS != err)
            {
              TBSYS_LOG(WARN, "add register node to app fail. app_name=%s", app_name);
            }
            break;            
          }       
        }

        if(i == app_count_)
        {
          //新增一个app然后add
          ObOcmAppInfo * app = new(std::nothrow) ObOcmAppInfo;
          if(NULL == app)
          {
            TBSYS_LOG(WARN, "fail to new ObOcmAppInfo, app=%p", app);
            err = OB_ALLOCATE_MEMORY_FAILED;
          }
          else
          {

            err = app->init(app_name);
            if(OB_SUCCESS == err)
              err = app->add_register_node(node, instance_name);
            if(OB_SUCCESS == err)
              err = add_app(*app);       
            if(OB_SUCCESS != err)
            {
              TBSYS_LOG(WARN, "fail to add app. err=%d", err);
            }

          }
          if(app != NULL)
          {
            delete app;
            app = NULL;
          }
        }
      }
      return err;
    }

    ObOcmMeta::~ObOcmMeta()
    {
      if(NULL != app_)
      {
        delete []app_;
        app_ = NULL;
      }
      init_ = false;
    }

    /*
       void ObOcmMeta::operator = (const ObOcmMeta & other)
       {
       UNUSED(other);
       }
     */
    const char* ObOcmMeta::get_location() const
    {
      return location_;
    }

    char* ObOcmMeta::get_location()
    {
      return location_;
    }

    int ObOcmMeta::set_location(const char * location)
    {
      int err = OB_SUCCESS;
      if(NULL == location || static_cast<int64_t>(strlen(location)) >= OB_MAX_LOCATION_LENGTH)
      {
        TBSYS_LOG(WARN,"invalid param, location=%s", location);
        err = OB_INVALID_ARGUMENT;
      }
      else
      {
        memcpy(location_, location, strlen(location) + 1);
      }
      return err;
    }

    ObServerExt* ObOcmMeta::get_ocm_server()
    {
      return &ocm_server_;
    }

    const ObServerExt* ObOcmMeta::get_ocm_server() const
    {
      return &ocm_server_;
    }

    int64_t ObOcmMeta::get_app_count() const
    {
      return app_count_;
    }

    void ObOcmMeta::set_app_count(int64_t count)
    {
      app_count_ = count;
    }

    int ObOcmMeta::transfer_master(const ObServerExt &new_master, const char* instance_name, const char* app_name)
    {
      int err = OB_SUCCESS;
      //int64_t instance_name_len = strlen(instance_name);
      //int64_t app_name_len = strlen(app_name);
      if(NULL == instance_name || NULL == app_name
          || static_cast<int64_t>(strlen(instance_name)) >= OB_MAX_INSTANCE_NAME_LENGTH
          || static_cast<int64_t>(strlen(app_name)) >= OB_MAX_APP_NAME_LENGTH)
      {
        TBSYS_LOG(WARN, "invalid param, instance_name=%s, app_name=%s", instance_name, app_name);
        err = OB_INVALID_ARGUMENT;
      }
      else
      {
        int64_t i = 0;
        tbsys::CWLockGuard guard(ocm_rwlock_);
        for(; i < app_count_; i++)
        {
          if(0 == strcmp(app_name, app_[i].get_app_name()))
          {
            err = app_[i].transfer_master(new_master, instance_name);
            if(OB_SUCCESS != err)
            {
              TBSYS_LOG(WARN, "fail to transfer master, err=%d", err);
            }
            break;
          }
        }
        if(app_count_ == i)
        {
          TBSYS_LOG(ERROR, "can't find the app, app_name=%s", app_name);
          err = OB_ENTRY_NOT_EXIST;
        }
      }
      return err;
    }

    int ObOcmMeta::get_app(char * app_name, ObOcmAppInfo* &app)
    {
      int err = OB_SUCCESS;
      if(NULL == app_ || app_name == NULL)
      {
        TBSYS_LOG(WARN,"invalid param,app_=%p,app_name=%s", app_, app_name);
        err = OB_INVALID_ARGUMENT;
      }
      else
      {
        tbsys::CRLockGuard guard(ocm_rwlock_);
        ObOcmAppInfo *tmp = app_;
        int64_t i = 0;
        for( ; i < app_count_; i++)
        {
          if(strcmp(tmp->get_app_name(), app_name) == 0)
          {
            app = tmp;
            break;
          }
          else
          {
            tmp ++;
          }
        }
        if( app_count_ == i)
        {
          TBSYS_LOG(INFO, "the app not exist.");
          err = OB_ITER_END;
        }
      }
      return err;
    }

    int ObOcmMeta::get_app_master_rs(const char *app_name, ObServerExt &serv_info) const
    {
      int err = OB_SUCCESS;
      //int len = strlen(app_name);
      if(NULL == app_name || static_cast<int64_t>(strlen(app_name)) >= OB_MAX_APP_NAME_LENGTH )
      {
        TBSYS_LOG(WARN,"invalid param,app_name=%s", app_name);
        err = OB_INVALID_ARGUMENT;
      }
      else
      {
        int64_t i = 0;
        tbsys::CRLockGuard guard(ocm_rwlock_);
        for( ; i < app_count_; i++)
        {
          if(0 == strcmp(app_[i].get_app_name(), app_name))
          {
            err = app_[i].get_app_master_rs(app_name, serv_info);
            if(OB_SUCCESS == err)
            {
              break;
            }
          }
          else
          {
            //do nothing;
          }
        }
        if(app_count_ == i)
        {
          err = OB_ENTRY_NOT_EXIST;
        }
      }
      return err;
    }

    int ObOcmMeta::get_instance_master_rs(const char *app_name, const char *instance_name, ObServerExt &serv_info) const
    {
      int err = OB_SUCCESS;
       
      if(NULL == app_name || NULL == instance_name
          || static_cast<int64_t>(strlen(app_name)) >= OB_MAX_APP_NAME_LENGTH
          || static_cast<int64_t>(strlen(instance_name)) >= OB_MAX_INSTANCE_NAME_LENGTH)
      {
        TBSYS_LOG(WARN,"invalid param, app_name=%s,instance_name=%s",
            app_name, instance_name);
        err = OB_INVALID_ARGUMENT;
      }
      else
      {
        tbsys::CRLockGuard guard(ocm_rwlock_);
        int64_t i = 0;
        for( ; i < app_count_; i++)
        {
          if(0 == strcmp(app_[i].get_app_name(), app_name))
          {
            err = app_[i].get_instance_master_rs(app_name, instance_name, serv_info);
            if(err == OB_SUCCESS)
            {
              break;
            }
          }
        }
        if(app_count_ == i)
        {
          err = OB_ENTRY_NOT_EXIST;
        }
      }
      return err;
    }

    int ObOcmMeta::get_instance_master_rs_list(const char *app_name, ObServList &serv_list) const
    {
      int err = OB_SUCCESS;
      //int64_t app_len = strlen(app_name);

      if(app_name == NULL || static_cast<int64_t>(strlen(app_name)) >= OB_MAX_APP_NAME_LENGTH)
      {
        TBSYS_LOG(WARN,"invalid param, app_name=%s", app_name);
        err = OB_INVALID_ARGUMENT;
      }
      else
      {
        tbsys::CRLockGuard guard(ocm_rwlock_);
        for(int i = 0; i < app_count_; i++)
        {
          if(0 == strcmp(app_[i].get_app_name(), app_name))
          {
            err = app_[i].get_instance_master_rs_list(app_name, serv_list);
            if(err != OB_SUCCESS)
            {
              TBSYS_LOG(WARN, "get rs list fail. i = %d", i);
              break;
            }
          }
        }
      }
      return err;
    }

    int ObOcmMeta::get_instance_role(const char* app_name, const char* instance_name, Status& status)
    {
      int err = OB_SUCCESS;
      //int64_t app_len = strlen(app_name);
      //int64_t instance_len = strlen(instance_name);
      if(NULL == app_name || NULL == instance_name
          || static_cast<int64_t>(strlen(app_name)) >= OB_MAX_APP_NAME_LENGTH
          || static_cast<int64_t>(strlen(instance_name)) >= OB_MAX_INSTANCE_NAME_LENGTH)
      {
        TBSYS_LOG(WARN,"invalid param, app_name=%s,instance_name=%s",
            app_name, instance_name);
        err = OB_INVALID_ARGUMENT;
      }
      else
      {
        tbsys::CRLockGuard guard(ocm_rwlock_);
        int64_t i = 0;
        for(; i < app_count_; i++)
        {
          if(0 == strcmp(app_[i].get_app_name(), app_name))
          {
            err = app_[i].get_instance_role(instance_name, status);
            if(OB_SUCCESS == err)
            {
              break;
            }
          }
        }
        if(i == app_count_)
        {
          err = OB_ENTRY_NOT_EXIST;
        }
      }
      return err;
    }

    int ObOcmMeta::set_instance_role(const char* app_name, const char* instance_name, const Status status)
    {
      int err = OB_SUCCESS;
      //int64_t app_len = strlen(app_name);
      //int64_t instance_len = strlen(instance_name);
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
        tbsys::CWLockGuard guard(ocm_rwlock_);
        int64_t i = 0;
        for(; i < app_count_; i++)
        {
          if(0 == strcmp(app_[i].get_app_name(), app_name))
          {
            err = app_[i].set_instance_role(instance_name, status);
            if(OB_SUCCESS == err)
            {
              break;
            }
          }
        }
        if(i == app_count_)
        {
          err = OB_ENTRY_NOT_EXIST;
        }
      }
      return err;
    }

    int ObOcmMeta::serialize(char* buf, const int64_t buf_len, int64_t& pos) const
    {
      int err = OB_SUCCESS;
      if (NULL == buf || buf_len <= 0 || pos >= buf_len)
      {
        TBSYS_LOG(WARN, "invalid param, buf=%p, buf_len=%ld, pos=%ld", buf, buf_len, pos);
        err = OB_INVALID_ARGUMENT;
      }
      else
      {
        int64_t str_len  = strlen(location_);
        tbsys::CRLockGuard guard(ocm_rwlock_);

        if(pos + str_len +  3 * (int64_t)sizeof(int64_t) >= buf_len)
        {
          TBSYS_LOG(WARN, "buf is not enough, pos=%ld, buf_len=%ld", pos, buf_len);
          err = OB_ERROR;
        }
        else
        {
          *(reinterpret_cast<int64_t*>(buf + pos)) = magic_num_;
          pos += sizeof(int64_t);
          *(reinterpret_cast<int64_t*> (buf + pos)) = str_len ;
          pos += sizeof(int64_t);
          //strncpy(buf + pos, location_, str_len );
          memcpy(buf + pos, location_, str_len);
          pos += str_len ;

          err = ocm_server_.serialize(buf, buf_len, pos);
          if(OB_SUCCESS != err)
          {
            TBSYS_LOG(WARN,"ocm_server serialize fail");
          }
          else
          {
            *(reinterpret_cast<int64_t*> (buf + pos)) = app_count_;
            pos += sizeof(int64_t);
            for(int i = 0; i < app_count_; i++)
            {
              err = app_[i].serialize(buf, buf_len, pos);
              if(OB_SUCCESS != err)
              {
                TBSYS_LOG(WARN, "serialize the obi failed. i=%d, err=%d", i, err);
                break;
              }
            }
          }
        }
      }
      return err;
    }

    int ObOcmMeta::deserialize(const char* buf, const int64_t buf_len, int64_t& pos)
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
          TBSYS_LOG(WARN, "wrong magic num, can't deserilize the buffer to ObOcmMeta, err=%d", err);       
        }
        else                                                                                                   
        { 
          pos += sizeof(int64_t);
          int str_len = 0;
          tbsys::CWLockGuard guard(ocm_rwlock_);
          str_len = static_cast<int32_t>(*(reinterpret_cast<const int64_t*> (buf + pos)));
          pos += sizeof(int64_t);
          //strncpy(location_, buf + pos, str_len);
          memcpy(location_, buf + pos, str_len);
          location_[str_len] = '\0';
          pos += str_len;

          err = ocm_server_.deserialize(buf, buf_len, pos);
          if(OB_SUCCESS != err)
          {
            TBSYS_LOG(WARN, "ocm_server deserialize fail");
          }
          else
          {
            app_count_ = *(reinterpret_cast<const int64_t*> (buf + pos));
            pos += sizeof(int64_t);
            for(int i = 0; i < app_count_; i++)
            {
              err = app_[i].deserialize(buf, buf_len, pos);
              if(OB_SUCCESS != err)
              {
                TBSYS_LOG(WARN, "obinstance deserialize failed. i=%d, err=%d", i, err);
                break;
              }
            }
          }
        }
      }
      return err;
    }

    int64_t ObOcmMeta::get_serialize_size() const
    {
      int64_t ret = 0;
      tbsys::CRLockGuard guard(ocm_rwlock_);
      for(int i = 0; i < app_count_; i++)
      {
        ret += app_[i].get_serialize_size();
      }
      ret = ret + strlen(location_) + 3 * sizeof(int64_t) + ocm_server_.get_serialize_size();
      return ret;
    }
  }
}
