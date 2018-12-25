/**
 * (C) 2010-2011 Taobao Inc.
 *
 *  This program is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU General Public License
 *  version 2 as published by the Free Software Foundation.
 *
 *  ob_yunti_proxy.h for yunti
 *
 *  Authors:
 *    zian <yunliang.shi@alipay.com>
 *
 */

#ifndef OCEANBASE_YUNTIPROXY_H_
#define OCEANBASE_YUNTIPROXY_H_
#include "ob_proxy_server_config.h"
#include "ob_yunti_meta.h"
#include "common/ob_file.h"

namespace oceanbase
{
  namespace proxyserver
  {
    class Proxy
    {
      public:
        virtual ~Proxy(){}
    };

    class YuntiMeta;

    class YuntiProxy : public Proxy
    {
      public:
        YuntiProxy();
        ~YuntiProxy();

        static const int MAX_CMD_LEN = 2 * OB_MAX_FILE_NAME_LENGTH;

        int initialize();
        //default RD_ONLY
        int open_rfile(FILE* &fp, const char* file_name);
        int open_rfile(FILE* &fp, common::ObNewRange& range, const char* sstable_dir);
        bool is_file_exist(const char* file_name);
        int close_rfile(FILE* pf);

        int fetch_data(FILE* fp, char* buf, int64_t size, int64_t &read_size);
        int get_file_size(const char* file_name, int64_t &file_size);
        int fetch_range_list(const char* table_name, const char* sstable_dir, common::ObArray<common::ObNewRange*> &range_list)
        {
          return yunti_meta_.fetch_range_list(table_name, sstable_dir, range_list);
        }
        int get_sstable_version(const char* sstable_dir, int16_t &sstable_version){return yunti_meta_.get_sstable_version(sstable_dir, sstable_version);}

      private:
        YuntiMeta yunti_meta_;
    };

  }
}

#endif
