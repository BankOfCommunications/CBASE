/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *  
 * ob_syschecker.h for define syschecker worker. 
 *
 * Authors:
 *   huating <huating.zmq@taobao.com>
 *
 */
#ifndef OCEANBASE_SYSCHECKER_H_
#define OCEANBASE_SYSCHECKER_H_

#include "client/ob_client.h"
#include "ob_syschecker_param.h"
#include "ob_write_worker.h"
#include "ob_read_worker.h"
#include "ob_syschecker_stat.h"
#include "ob_syschecker_schema.h"
#include "ob_syschecker_rule.h"
class TestSysChecker;
namespace oceanbase 
{ 
  namespace syschecker
  {
    class ObSyschecker
    {
      public:
        friend class TestSysChecker;
        ObSyschecker();
        ~ObSyschecker();

        int init();
        int start();
        int stop();
        int wait();
        inline ObSyscheckerSchema* get_syschecker_schema()
        {
          return &syschecker_schema_;
        }

      public:
        int translate_user_schema(const common::ObSchemaManagerV2& ori_schema_mgr,
            common::ObSchemaManagerV2& user_schema_mgr);
      private:
        int init_servers_manager();
        int init_rowkey_range();
        int read_rowkey_range(common::ObScanner& scanner);
        int get_cell_value(const common::ObCellInfo& cell_info, 
            int64_t& value);
        int write_rowkey_range();
        int set_cell_value(common::ObObj& obj, const common::ObObjType type, 
            char* value_buf, const int64_t value);

      private:
        DISALLOW_COPY_AND_ASSIGN(ObSyschecker);

        ObSyscheckerParam param_;
        client::ObServerManager servers_mgr_;
        ObSyscheckerSchema syschecker_schema_;
        ObSyscheckerRule rule_;
        client::ObClient client_;
        ObSyscheckerStat stat_;
        ObWriteWorker write_worker_;
        ObReadWorker read_worker_;
    };
  } // end namespace syschecker
} // end namespace oceanbase

#endif //OCEANBASE_SYSCHECKER_H_
