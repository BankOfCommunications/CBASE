/*
 *   (C) 2007-2010 Taobao Inc.
 *
 *
 *   Version: 0.1 2010-8-12
 *
 *   Authors:
 *      qushan <qushan@taobao.com>
 *      zian <yunliang.shi@alipay.com>
 *
 */
#ifndef OCEANBASE_BACKUPSERVER_BACKUPSERVERMAIN_H_
#define OCEANBASE_BACKUPSERVER_BACKUPSERVERMAIN_H_

#include "common/base_main.h"
#include "common/ob_version.h"
#include "ob_backup_server.h"
#include "chunkserver/ob_chunk_server_main.h"

namespace oceanbase
{
  namespace backupserver
  {
    class ObBackupServerMain : public chunkserver::ObChunkServerMain
    {
      protected:
        ObBackupServerMain();
      protected:
        virtual int do_work();
        virtual void do_signal(const int sig);
      public:
        static ObBackupServerMain* get_instance();
      public:
        const ObBackupServer& get_backup_server() const { return backup_server_ ; }
        ObBackupServer& get_backup_server() { return backup_server_ ; }

      protected:
        virtual void print_version();
      private:
        ObBackupServer backup_server_;
        ObBackupServerConfig backup_config_;
    };

#define THE_BACKUP_SERVER ObBackupServerMain::get_instance()->get_backup_server()

  } // end namespace backupserver
} // end namespace oceanbase


#endif 

