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
#include "ob_ocm_meta_manager.h"

namespace oceanbase
{
  namespace clustermanager
  {
    ObOcmMetaManager::ObOcmMetaManager()
    {
      init_ = false;
      ocm_count_ = 0;
      magic_num_ = reinterpret_cast<int64_t>("aManager");
      TBSYS_LOG(INFO, "magic num is %ld", magic_num_);
    }

    ObOcmMetaManager::~ObOcmMetaManager()
    {
    }

    int ObOcmMetaManager::init_ocm_list(const char* ocm_location, const ObServerExt &ocm_server)
    {
      int err = OB_SUCCESS;
      if(init_)
      {
        TBSYS_LOG(WARN, "already inited.");
        err = OB_INIT_TWICE;
      }
      else
      {
        if(NULL == ocm_location || static_cast<int64_t>(strlen(ocm_location)) >= OB_MAX_LOCATION_LENGTH)
        {
          TBSYS_LOG(WARN, "invalid param ,ocm_location=%s", ocm_location);
          err = OB_INVALID_ARGUMENT;
        }
        else
        {
          ObOcmMeta *pmeta = new(std::nothrow) ObOcmMeta;
          if(pmeta == NULL)
          {
            TBSYS_LOG(WARN, "fail to init ocm list, pmeta=%p", pmeta);
            err = OB_ALLOCATE_MEMORY_FAILED;
          }
          else
          {
            pmeta->set_location(ocm_location);
            pmeta->get_ocm_server()->deep_copy(ocm_server);
            err = ocm_list_.push_front(*pmeta);
            if(OB_SUCCESS == err)
            {
              ocm_self_ = ocm_list_.begin();
              ocm_count_ ++;
              init_ = true;
            }
          }
          if(pmeta != NULL)
          {
            delete pmeta;
            pmeta = NULL;
          }
        }
      }
      return err;
    }

    int ObOcmMetaManager::serialize(char* buf, const int64_t buf_len, int64_t& pos) const
    {
      int err = OB_SUCCESS;
      tbsys::CRLockGuard guard(ocm_list_rwlock_);
      if (NULL == buf || buf_len <= 0 || pos >= buf_len)
      {
        TBSYS_LOG(WARN, "invalid param, buf=%p, buf_len=%ld, pos=%ld", buf, buf_len, pos);
        err = OB_INVALID_ARGUMENT;
      }
      else
      {
        if(pos + 2 * (int64_t)sizeof(int64_t) >= buf_len)
        {
          TBSYS_LOG(WARN, "buf is not enough, pos=%ld, buf_len=%ld", pos, buf_len);
          err = OB_ERROR;
        }
        else
        {
          *(reinterpret_cast<int64_t*> (buf + pos)) = magic_num_;
          pos += sizeof(int64_t);

          *(reinterpret_cast<int64_t*> (buf + pos)) = ocm_count_;
          pos += sizeof(int64_t);

          OcmList::const_iterator it;
          for(it = ocm_list_.begin(); it != ocm_list_.end(); it++)
          {
            err = (*it).serialize(buf, buf_len, pos);
            if(OB_SUCCESS != err)
            {
              TBSYS_LOG(WARN, "ocm list serialize fail, err= %d. ocm_location=%s", err, (*it).get_location());
              break;
            }
          }
        }
      }
      return err;
    }

    int ObOcmMetaManager::deserialize(const char* buf, const int64_t buf_len, int64_t& pos)
    {
      int err = OB_SUCCESS;
      if (NULL == buf || buf_len <= 0 || pos >= buf_len)
      {
        TBSYS_LOG(WARN, "invalid param, buf=%s, buf_len=%ld, pos=%ld", buf, buf_len, pos);
        err = OB_INVALID_ARGUMENT;
      }
      else
      {
        tbsys::CWLockGuard guard(ocm_list_rwlock_);
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
          ocm_count_ = *(reinterpret_cast<const int64_t*> (buf + pos)) ;
          pos += sizeof(int64_t);

          for(int i = 0; i < ocm_count_ ; i++)
          {
            ObOcmMeta *meta = new(std::nothrow) ObOcmMeta;
            err = meta->deserialize(buf, buf_len, pos);
            if(OB_SUCCESS != err)
            {
              TBSYS_LOG(WARN, "ocm list deserialize fail. i=%d, err=%d", i, err);
              break;
            }
            ocm_list_.push_back(*meta);
          }
        }
      }
      return err;
    }

    int64_t ObOcmMetaManager::get_serialize_size(void) const
    {
      int64_t ret = 0;
      OcmList::const_iterator it;
      tbsys::CRLockGuard guard(ocm_list_rwlock_);
      for(it = ocm_list_.begin(); it != ocm_list_.end(); it++)
      {
        ret += it->get_serialize_size();
      }
      ret += sizeof(int64_t) * 2;
      return ret;
    }

    int ObOcmMetaManager::get_app_master_rs(const char *app_name, ObServerExt &serv_info) const
    {
      int err = OB_SUCCESS;
     // int len = strlen(app_name);
      if(NULL == app_name || static_cast<int64_t>(strlen(app_name)) >= OB_MAX_APP_NAME_LENGTH )
      {
        TBSYS_LOG(WARN,"invalid param,app_name=%s", app_name);
        err = OB_INVALID_ARGUMENT;
      }
      else
      {
      tbsys::CRLockGuard guard(ocm_list_rwlock_);
        OcmList::const_iterator it = ocm_list_.begin();
        while(ocm_list_.end() != it)
        {
          err = it->get_app_master_rs(app_name, serv_info);
          if(OB_SUCCESS == err)
          {
            break;
          }
          else
          {
            it ++;
          }
        }
      }
      return err;
    }

    int ObOcmMetaManager::get_instance_master_rs(const char *app_name, const char *instance_name, ObServerExt &serv_info) const
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
      tbsys::CRLockGuard guard(ocm_list_rwlock_);
        OcmList::const_iterator it = ocm_list_.begin();
        while(ocm_list_.end() != it)
        {
          err = it->get_instance_master_rs(app_name, instance_name, serv_info);
          if(OB_SUCCESS == err)
          {
            break;
          }
          else
          {
            it ++;
          }
        }
      }
      return err;
    }

    int ObOcmMetaManager::get_instance_master_rs_list(const char *app_name, ObServList &serv_list) const
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
      tbsys::CRLockGuard guard(ocm_list_rwlock_);
        OcmList::const_iterator it = ocm_list_.begin();
        while(ocm_list_.end() != it)
        {
          err = it->get_instance_master_rs_list(app_name, serv_list);
          if(err != OB_SUCCESS)
          {
            TBSYS_LOG(WARN,"get rs list fail, ocm_location=%s", it->get_location());
            break;
          }
          it ++;
        }
      }
      return err;
    }

  int ObOcmMetaManager::get_instance_role(const char* app_name, const char* instance_name, Status& status)
  {
    int err = OB_SUCCESS;
    //int64_t app_len = strlen(app_name);
    // int64_t instance_len = strlen(instance_name);
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
     // tbsys::CRLockGuard guard(ocm_list_rwlock_);
     /* OcmList::iterator it;
    for(it = ocm_list_.begin(); it != ocm_list_.end(); it++)
    {
       err = it->get_instance_role(app_name, instance_name, status);
       if(OB_SUCCESS == err)
       {
         break;
       }
    } */
      err = ocm_self_->get_instance_role(app_name, instance_name, status);
    }
    return err;
  }

  int ObOcmMetaManager::set_instance_role(const char* app_name, const char* instance_name, const Status status)
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
      //tbsys::CWLockGuard guard(ocm_list_rwlock_);
      /*OcmList::iterator it;
      for(it = ocm_list_.begin(); it != ocm_list_.end(); it++)
      {
        err = it->set_instance_role(app_name, instance_name, status);
        if(OB_SUCCESS == err)
        {
          break;
        }
      }*/
      err = ocm_self_->set_instance_role(app_name, instance_name, status); 
    }
    return err;   
  }

  const OcmList::iterator& ObOcmMetaManager::get_self() const
  {
    return ocm_self_;
  }

  OcmList::iterator& ObOcmMetaManager::get_self()
    {
      return ocm_self_;
    }

    const OcmList* ObOcmMetaManager::get_ocm_list()const
    {
      return &ocm_list_;
    }

    int ObOcmMetaManager::add_ocm_info(const ObOcmMeta &ocm_meta)
    {
      int err = OB_SUCCESS;
      tbsys::CWLockGuard guard(ocm_list_rwlock_);
      if(!init_)
      {
        TBSYS_LOG(WARN, "have not init.");
        err = OB_INNER_STAT_ERROR;
      }
      else
      {
        OcmList::iterator it = ocm_list_.begin();
        it ++;
        while(it != ocm_list_.end())
        {
          if(0 == strcmp(it->get_location(), ocm_meta.get_location()))
          {
            err = ocm_list_.erase(it);
            if(OB_SUCCESS == err)
            {
              ocm_count_ --;
            }
            break;
          }
          it ++;
        }

        if(OB_SUCCESS == err)
        {
          err = ocm_list_.push_back(ocm_meta);
        }

        if(OB_SUCCESS == err)
        {
          ocm_count_++;
        }
        else
        {
          TBSYS_LOG(WARN, "add ocm fail. ocm_location=%s", ocm_meta.get_location());
        }
      }
      return err;
    }

    int64_t ObOcmMetaManager::get_ocm_count()const
    {
      return ocm_count_;
    }
  }
}
