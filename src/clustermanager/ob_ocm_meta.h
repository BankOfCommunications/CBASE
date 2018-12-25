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

#ifndef _OCEANBASE_CLUSTERMANAGER_OB_OCM_META_H_
#define _OCEANBASE_CLUSTERMANAGER_OB_OCM_META_H_

#include "ob_define.h"
#include "ob_ocm_app_info.h"
#include "ob_ocm_param.h"

namespace oceanbase
{
  namespace tests
  {
    namespace clustermanager
    {
      class TestObOcmMeta;
    }
    class TestOcmMetaManager;
  }
  namespace clustermanager
  {
    const int64_t OB_MAX_APP_COUNT = 4;
    class ObOcmMetaManager;
    class ObOcmMeta
    {
      public:
        ObOcmMeta();
        ~ObOcmMeta();

        friend class common::ObList<ObOcmMeta>;
        friend class ObOcmMetaManager;
        friend class tests::TestOcmMetaManager;
        friend class tests::clustermanager::TestObOcmMeta; 
        const char* get_location() const;
        char* get_location();
      
        const ObServerExt* get_ocm_server()const;
        ObServerExt* get_ocm_server();
        int64_t get_app_count()const;
        int get_app(char * app_name, ObOcmAppInfo* &app);
        const ObOcmAppInfo* get_app(const int64_t index) const;

        int set_ocm_server(char *host_name, common::ObServer server);
        int set_location(const char * location);
        int add_app(ObOcmAppInfo &app_info);
        int get_instance_role(const char* app_name, const char* instance_name, Status& status);
        int get_app_master_rs(const char *app_name, ObServerExt &serv_info) const;
        int get_instance_master_rs(const char *app_name, const char *instance_name, ObServerExt &serv_info) const;
        int get_instance_master_rs_list(const char *app_name, ObServList &serv_list) const;
        
        int add_register_node(const ObServerExt &node, const char* instance_name, const char* app_name);
        int transfer_master(const ObServerExt &new_master, const char* instance_name, const char* app_name);
        int set_instance_role(const char* app_name, const char* instance_name, const Status status);
        NEED_SERIALIZE_AND_DESERIALIZE;
      
      private:
        void set_app_count(int64_t count);
        ObOcmMeta& operator = (const ObOcmMeta& other);
      private:
        tbsys::CRWLock ocm_rwlock_;
        char location_[OB_MAX_LOCATION_LENGTH];
        ObServerExt ocm_server_;
        int64_t app_count_;
        ObOcmAppInfo *app_;
        bool init_;
        int64_t magic_num_;
    };
  }
}
#endif
