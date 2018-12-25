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
#ifndef __OCEANBASE_CLUSTERMANAGER_OB_OCM_INSTANCE_H__
#define __OCEANBASE_CLUSTERMANAGER_OB_OCM_INSTANCE_H__

#include "common/ob_server_ext.h"
#include "common/ob_list.h"
#include "common/ob_define.h"
namespace oceanbase
{
 namespace tests
 {
   namespace clustermanager
   {
     class TestObOcmInstance;
   }
 }
  namespace clustermanager
  {
    enum Status
    {
      MASTER = 0,
      SLAVE = 1,
    };

    typedef common::ObList<common::ObServerExt*> ObServList;
    class ObOcmInstance
    {
      public:
        friend class ObOcmAppInfo;
        friend class tests::clustermanager::TestObOcmInstance;
        ObOcmInstance();
        ~ObOcmInstance();
        int init(const char* instance_name, common::ObServerExt master_rs);
        int serialize(char* buf, const int64_t buf_len, int64_t& pos) const;
        int deserialize(const char* buf, const int64_t buf_len, int64_t& pos);
        int64_t get_serialize_size(void)const;
        const common::ObServerExt& get_master_rs()const;
        common::ObServerExt& get_master_rs();
        Status get_role()const;
        int64_t get_read_radio()const;
        char* get_instance_name();
        const char* get_instance_name() const;
        int set_instance_name(const char * instance_name);
        int set_read_radio(const int64_t radio);
        int get_app_master_rs(common::ObServerExt &serv_info) const;
        int get_instance_master_rs(const char *instance_name, common::ObServerExt &serv_info) const;
        int get_rs_list(ObServList& serv_list);
        const int get_rs_list(ObServList& serv_list) const;
        int deep_copy(const ObOcmInstance &instance);
        void transfer_master(const common::ObServerExt &new_master);
        int64_t get_reserve2() const;
        int64_t get_reserve1() const;
      private:
        void set_reserve2(int64_t reserve_value);
        void set_role(Status state);
        void set_reserve1(int64_t reserve_value);
      private:
        common::ObServerExt master_rs_;
        char instance_name_[common::OB_MAX_INSTANCE_NAME_LENGTH];
        Status role_;  //表明该实例是否为主，有主备之分
        int64_t read_radio_;
        int64_t reserve1_;
        int64_t reserve2_;
        int64_t magic_num_;
    };
  }
}

#endif
