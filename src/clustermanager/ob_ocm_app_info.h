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

#ifndef __OCEANBASE_CLUSTER_OB_OOM_APP_INFO__
#define __OCEANBASE_CLUSTER_OB_OOM_APP_INFO__

#include "ob_server_ext.h"
#include "ob_ocm_instance.h"
#include "common/ob_define.h"

using namespace oceanbase::common;

namespace oceanbase
{
  namespace tests
  {
    namespace clustermanager
    {
      class TestObOcmMeta;
    }
  }
  namespace clustermanager
  {
    const int64_t OB_MAX_OBI_COUNT = 5;
    class ObOcmAppInfo
    {
      public:
        friend class tests::clustermanager::TestObOcmMeta;
        ObOcmAppInfo();
        ~ObOcmAppInfo();
        int init(const char* app_name);
        int add_obi(ObOcmInstance &instance);
        int serialize(char* buf, const int64_t buf_len, int64_t& pos);
        int deserialize(const char* buf, const int64_t buf_len, int64_t& pos);
        int get_serialize_size();

        int set_app_name(const char * app_name);
        const char * get_app_name()const;
        int64_t get_obi_count()const;
        const ObOcmInstance* get_obi(int64_t index) const;

        int set_instance_role(const char* instance_name, const Status status);
        int get_instance_role(const char* instance_name, Status& status);
        int add_register_node(const ObServerExt &node, const char* instance_name);
        int transfer_master(const ObServerExt &new_master, const char* instance_name);
		
        int get_app_master_rs(const char *app_name, ObServerExt &serv_info) const;
        int get_instance_master_rs(const char *app_name, const char *instance_name, ObServerExt &serv_info) const;
        int get_instance_master_rs_list(const char *app_name, ObServList &serv_list) const;
		
        int deep_copy(const ObOcmAppInfo &app_info);
      private:
        void set_obi_count(int64_t count);
      private:
        char app_name_[OB_MAX_APP_NAME_LENGTH];
        int64_t obi_count_;
        ObOcmInstance obi_[OB_MAX_OBI_COUNT];
        int64_t magic_num_;
    };
  }
}
#endif
