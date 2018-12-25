/*
 * (C) 2007-2010 TaoBao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * ob_client_proxy.h is for what ...
 *
 * Version: $id$
 *
 * Authors:
 *   MaoQi maoqi@taobao.com
 *
 */

#ifndef OCEANBASE_COMMON_OB_CLIENT_PROXY_H_
#define OCEANBASE_COMMON_OB_CLIENT_PROXY_H_

#include <stdint.h>
#include "ob_mutator.h"
#include "ob_server.h"
#include "ob_scanner.h"
#include "ob_ms_list.h"

namespace oceanbase
{
  namespace common
  {
    class ObClientManager;
    class ThreadSpecificBuffer;
    class ObScanParam;
    class ObGetParam;

    class ObClientProxy 
    {
      public:
        ObClientProxy();
        virtual ~ObClientProxy();

        void init(ObClientManager* client_manager,
                  ThreadSpecificBuffer *thread_buffer,
                  MsList* ms_list,
                  int64_t timeout);
        virtual int scan(const ObScanParam& scan_param,ObScanner& scanner);
        virtual int apply(const ObMutator& mutator);
        virtual int get(const ObGetParam& get_param,ObScanner& scanner);
      private:
        int scan(const ObServer& server,const ObScanParam& scan_param,ObScanner& scanner);
        int get(const ObServer& server,const ObGetParam& get_param,ObScanner& scanner);

      private:
        int get_thread_buffer_(ObDataBuffer& data_buff);

      private:
        bool inited_;
        ObClientManager* client_manager_;
        ThreadSpecificBuffer* thread_buffer_;
        int64_t timeout_;
        MsList* ms_list_;
    };
    
  } /* common */
  
} /* oceanbase */
#endif
