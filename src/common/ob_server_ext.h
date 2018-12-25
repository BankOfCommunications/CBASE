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

#ifndef __OCEANBASE_CLUSTERMANAGER_OB_SERVER_EXT_H__
#define __OCEANBASE_CLUSTERMANAGER_OB_SERVER_EXT_H__

#include "common/ob_server.h"

namespace oceanbase
{
  namespace common
  {
    class ObServerExt
    {
      public:
        friend class ObOcmInstance;
        ObServerExt();
        ~ObServerExt();

        int init(char *host_nema, ObServer server);
        int serialize(char* buf, const int64_t buf_len, int64_t& pos)const;
        int deserialize(const char* buf, const int64_t buf_len, int64_t& pos);
        int64_t get_serialize_size(void) const;
        const char* get_hostname() const;
        char* get_hostname();
        int set_hostname(const char * hname);
        const ObServer& get_server() const ;
        ObServer& get_server();
        void deep_copy(const ObServerExt& server_ext);
      private:
        char hostname_[OB_MAX_HOST_NAME_LENGTH];
        ObServer server_;
        int64_t magic_num_;
    };
  }
}
#endif
