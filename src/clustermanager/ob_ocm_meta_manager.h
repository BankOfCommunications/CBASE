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

#ifndef _OCEANBASE_CLUSTERMANAGER_OB_OCM_META_MANAGER_H_
#define _OCEANBASE_CLUSTERMANAGER_OB_OCM_META_MANAGER_H_

#include <tbsys.h>
#include "ob_ocm_meta.h"

namespace oceanbase
{
  namespace clustermanager
  {
    typedef common::ObList<ObOcmMeta> OcmList;
    class ObOcmMetaManager
    {
      public:
        ObOcmMetaManager();
        ~ObOcmMetaManager();
        int init_ocm_list(const char* ocm_location, const ObServerExt &ocm_server);
      public:
        int serialize(char* buf, const int64_t buf_len, int64_t& pos) const;
        int deserialize(const char* buf, const int64_t data_len, int64_t& pos);
        int64_t get_serialize_size(void) const;

      public:
        int get_app_master_rs(const char *app_name, ObServerExt &serv_info) const;
        int get_instance_master_rs(const char *app_name, const char *instance_name, ObServerExt &serv_info) const;
        int get_instance_master_rs_list(const char *app_name, ObServList &serv_list) const;

        int get_instance_role(const char* app_name, const char* instance_name, Status& status);
        int set_instance_role(const char* app_name, const char* instance_name, Status status);		
        
        const OcmList::iterator& get_self() const;
        OcmList::iterator& get_self();
        int add_ocm_info(const ObOcmMeta &ocm_meta);
        const OcmList *get_ocm_list()const;
        int64_t get_ocm_count()const;
      private:
        OcmList ocm_list_; //包含所有的ocm信息
        OcmList::iterator ocm_self_; //自己的ocm信息，定时将该信息扩散到其他ocm上。
        bool init_;
        int64_t ocm_count_;
        tbsys::CRWLock ocm_list_rwlock_;
        int64_t magic_num_;
    };
  }
}
#endif
