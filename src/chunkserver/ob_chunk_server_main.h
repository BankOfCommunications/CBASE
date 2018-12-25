/*
 *   (C) 2007-2010 Taobao Inc.
 *
 *
 *   Version: 0.1 2010-8-12
 *
 *   Authors:
 *      qushan <qushan@taobao.com>
 *
 */
#ifndef OCEANBASE_CHUNKSERVER_CHUNKSERVERMAIN_H_
#define OCEANBASE_CHUNKSERVER_CHUNKSERVERMAIN_H_

#include "common/base_main.h"
#include "common/ob_version.h"
#include "common/ob_config_manager.h"
#include "ob_chunk_server.h"
#include "ob_chunk_server_config.h"
#include "ob_chunk_reload_config.h"

namespace oceanbase
{
  namespace chunkserver
  {

    class ObChunkServerMain : public common::BaseMain
    {
      protected:
        ObChunkServerMain();
      protected:
        virtual int do_work();
        virtual void do_signal(const int sig);
      public:
        static ObChunkServerMain* get_instance();
      public:
        const ObChunkServer& get_chunk_server() const { return server_ ; }
        ObChunkServer& get_chunk_server() { return server_ ; }
      protected:
        virtual void print_version();
        //add zhaoqiong [fixed for Backup]:20150811:b
        ObChunkServer server_;
        ObConfigManager config_mgr_;
        ObChunkServerConfig cs_config_;
        //add:e
      private:
        ObChunkReloadConfig cs_reload_config_;
        //del zhaoqiong [fixed for Backup]:20150811:b
        //ObChunkServerConfig cs_config_;
        //ObChunkServer server_;
        //ObConfigManager config_mgr_;
        //del:e
    };


#define THE_CHUNK_SERVER ObChunkServerMain::get_instance()->get_chunk_server()

  } // end namespace chunkserver
} // end namespace oceanbase


#endif //OCEANBASE_CHUNKSERVER_CHUNKSERVERMAIN_H_

