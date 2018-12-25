/*
 * (C) 2007-2010 TaoBao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * ob_client_helper.h is for what ...
 *
 * Version: $id$
 *
 * Authors:
 *   MaoQi maoqi@taobao.com
 *
 */

#ifndef OCEANBASE_COMMON_OB_CLIENT_HELPER_H_
#define OCEANBASE_COMMON_OB_CLIENT_HELPER_H_

#include <stdint.h>
#include "ob_mutator.h"
#include "ob_server.h"
#include "ob_scanner.h"

namespace oceanbase
{
  namespace common
  {
    class ObClientManager;
    class ThreadSpecificBuffer;
    class ObScanParam;
    class ObGetParam;

    class ObClientHelper 
    {
      public:
        ObClientHelper();

        void init(ObClientManager* client_manager,
                  ThreadSpecificBuffer *thread_buffer,
                  const ObServer root_server,
                  int64_t timeout);
        int scan(const ObScanParam& scan_param,ObScanner& scanner);
        int apply(const ObServer& update_server, const ObMutator& mutator);
        int get(const ObGetParam& get_param,ObScanner& scanner);
        //add pangtianze [Paxos rs_election] 20150716:b
        void set_root_server(const ObServer &server)
        {
          root_server_ = server;
        }
        //add:e
      private:
        int scan(const ObServer& server,const ObScanParam& scan_param,ObScanner& scanner);
        int get(const ObServer& server,const ObGetParam& get_param,ObScanner& scanner);

      private:
        int get_tablet_info(const ObGetParam& param);
        int get_tablet_info(const ObScanParam& scan_param);
        int parse_merge_server(ObScanner& scanner);
        int get_thread_buffer_(ObDataBuffer& data_buff);

      private:
        bool inited_;
        ObClientManager* client_manager_;
        ThreadSpecificBuffer* thread_buffer_;
        int64_t timeout_;
        ObServer root_server_;
        //mod zhaoqiong[roottable tablet management]20160104:b
        //ObServer merge_server_[OB_SAFE_COPY_COUNT];
        ObServer merge_server_[OB_MAX_COPY_COUNT];
        //mod:e
    };
    
  } /* common */
  
} /* oceanbase */
#endif
