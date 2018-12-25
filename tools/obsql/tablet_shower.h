/*
 *   (C) 2010-2012 Taobao Inc.
 *   
 *   Version: 0.1 $date
 *           
 *   Authors:
 *               
 */

#ifndef OCEANBASE_OBSQL_TABLET_SHOWER_H_
#define OCEANBASE_OBSQL_TABLET_SHOWER_H_

#include "common/ob_schema.h"
#include "common/ob_server.h"
#include "common/ob_define.h"

#include "common/data_buffer.h"
#include "client_rpc.h"

namespace oceanbase 
{
  namespace obsql 
  {
    using namespace oceanbase::common;
	
    class ObTabletShower
    {
      public:
        ObTabletShower(ObClientServerStub& rpc_stub, ObServer& server);
        virtual ~ObTabletShower() {}

      public:

        ObArrayHelper<int32_t>& get_disks_array() { return disk_nos_; }
        ObServer get_server() { return server_; }

        int output();

      private:
        int output_single_server(ObServer& server);

      private:
        /* Do we need change it to OB_MAX_TABLET_LIST_NUMBER */
        static const int32_t disk_no_size = 64; 
        int32_t disk_no_array[disk_no_size];
        ObArrayHelper<int32_t> disk_nos_;
        ObServer server_;
        ObClientServerStub& rpc_stub_;
    };
  }
}

#endif // OCEANBASE_OBSQL_TABLET_SHOWER_H_


