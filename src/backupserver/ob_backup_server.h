/*
 *   (C) 2007-2010 Taobao Inc.
 *
 *   Version: 0.1 $date
 *
 *   Authors:
 *      qushan <qushan@taobao.com>
 *
 */
#ifndef OCEANBASE_BACKUPSERVER_BACKUPSERVER_H_
#define OCEANBASE_BACKUPSERVER_BACKUPSERVER_H_

#include <pthread.h>
#include "common/ob_single_server.h"
#include "common/ob_packet_factory.h"
#include "common/thread_buffer.h"
#include "common/ob_client_manager.h"
#include "common/ob_file_service.h"
#include "common/ob_file_client.h"
#include "ob_backup_server_config.h"
#include "ob_tablet_backup_manager.h"
#include "ob_tablet_backup_service.h"
#include "ob_tablet_backup_runnable.h"
#include "ob_backup_server_config.h"
#include "ob_backup_rs_fetch_runnable.h"
#include "ob_backup_ups_fetch_runnable.h"

#include "common/ob_client_manager.h"
#include "common/ob_file_service.h"
#include "common/ob_file_client.h"

#include "chunkserver/ob_tablet_manager.h"
#include "chunkserver/ob_chunk_server_config.h"
#include "rootserver/ob_root_rpc_stub.h"
#include "rootserver/ob_root_sql_proxy.h"

#include "updateserver/ob_sstable_mgr.h"
#include "updateserver/ob_ups_rpc_stub.h"

namespace oceanbase
{

  using oceanbase::chunkserver::ObTabletManager;
  using oceanbase::chunkserver::ObChunkServerConfig;
  using oceanbase::rootserver::ObRootRpcStub;
  using oceanbase::rootserver::ObRootSQLProxy;
  using oceanbase::updateserver::SSTableMgr;

  namespace backupserver
  {
    enum BackupMode
    {
      MIN_BACKUP = -1,
      FULL_BACKUP = 0,
      STATIC_BACKUP = 1,
      INCREMENTAL_BACKUP = 2,
      MAX_BACKUP = 3,
    };

    class ObTabletBackupManager;
    class ObTabletBackupRunnable;
    class ObBackupServer : public common::ObSingleServer
    {
      public:
        static const char * select_backup_info_str;
        static const char * update_backup_info_str;
        static const char * check_backup_privilege_str;
        static const char * ups_commit_log_dir;
        static const char * rs_data_dir;
        static const char * schema_ext;
        static const int32_t RESPONSE_PACKET_BUFFER_SIZE = 1024*1024*2; //2MB
        static const int32_t RPC_BUFFER_SIZE = 1024*1024*2; //2MB
        static const int64_t RETRY_INTERVAL_TIME = 1000 * 1000; // usleep 1 s
      public:
        ObBackupServer(ObChunkServerConfig &config, ObBackupServerConfig &backup_config);
        ~ObBackupServer();
      public:
        /** called before start server */
        virtual int initialize();
        /** called after start transport and listen port*/
        virtual int start_service();
        virtual void wait_for_queue();
        virtual void destroy();

        virtual int do_request(common::ObPacket* base_packet);
      public:
        common::ThreadSpecificBuffer::Buffer* get_rpc_buffer() const;
        common::ThreadSpecificBuffer::Buffer* get_response_buffer() const;

        const common::ThreadSpecificBuffer* get_thread_specific_rpc_buffer() const;

        const common::ObServer& get_self() const;

        const ObChunkServerConfig & get_config() const;
        ObChunkServerConfig & get_config();

        const ObBackupServerConfig & get_backup_config() const;
        ObBackupServerConfig & get_backup_config();

        common::ObGeneralRpcStub & get_rpc_stub();

        rootserver::ObRootRpcStub & get_root_rpc_stub();

        const ObTabletManager & get_tablet_manager() const ;
        ObTabletManager & get_tablet_manager() ;

        common::ObSchemaManagerV2* get_schema_manager();

        const common::ObServer get_root_server() const;

        const common::ObServer get_merge_server() const;

        int64_t get_ups_frozen_version() const;

        //int64_t get_ups_max_log_id() const;

        const ObTabletBackupManager * get_backup_manager() const;
        ObTabletBackupManager * get_backup_manager();

        int init_backup_rpc();

        int start_backup_task(int mode);

        void abort_ups_backup_task();

        int report_sub_task_finish(ObRole role, int32_t retValue);

        void start_threads();
        void stop_threads();

        int check_backup_privilege(const char * username, const char * pwd, bool & result);


      private:
        DISALLOW_COPY_AND_ASSIGN(ObBackupServer);
        int set_self(const char* dev_name, const int32_t port);
        int64_t get_process_timeout_time(const int64_t receive_time,
                                         const int64_t network_timeout);
        int init_ups_backup_info(int mode = 0);
        int init_backup_env(int mode = 0);
        int get_ups_fech_param(common::ObString& sql_str, int mode, uint64_t& max_log_id, uint64_t& max_sstable_id);
        int update_inner_backup_table(bool is_finish = FALSE);
        int update_ups_backup_info(int mode = 0);
      private:
        ObChunkServerConfig &config_;
        ObBackupServerConfig &backup_config_;
        ObTabletBackupService service_;

        ObTabletManager tablet_manager_;

        // ob file service
        common::ObFileService file_service_;
        // ob file client
        common::ObFileClient file_client_;
        common::ThreadSpecificBuffer file_client_rpc_buffer_;

        // network objects.
        common::ObClientManager  client_manager_;
        common::ObGeneralRpcStub rpc_stub_;
        rootserver::ObRootRpcStub rt_rpc_stub_;
        updateserver::ObUpsRpcStub ups_rpc_stub_;


        // thread specific buffers
        common::ThreadSpecificBuffer response_buffer_;
        common::ThreadSpecificBuffer rpc_buffer_;

        // server information
        common::ObServer self_;

        //schema information
        common::ObSchemaManagerV2 schema_mgr_;

        //rs info
        ObBackupRootFetchRunnable  *rs_fetch_thread_;

        //ups info
        ObServer master_ups_;
        SSTableMgr sstable_mgr_;
        uint64_t min_sstable_id_;
        uint64_t max_sstable_id_;
        uint64_t min_log_id_;
        uint64_t max_log_id_;
        ObBackupUpsFetchRunnable *ups_fetch_thread_;


        //tablet backup related
        int64_t cur_frozen_version_;
        ObTabletBackupManager * backup_mgr_;
        ObTabletBackupRunnable * backup_thread_;


        //backup info
        volatile int32_t backup_mode_;
        volatile int64_t backup_start_time_;
        volatile int64_t backup_end_time_;
        volatile bool rs_flag_;
        volatile bool ups_flag_;
        volatile bool cs_flag_;
        volatile int rs_ret_value_;
        volatile int ups_ret_value_;
        volatile int cs_ret_value_;

    };

  } // end namespace backupserver
} // end namespace oceanbase


#endif //OCEANBASE_BACKUPSERVER_BACKUPSERVER_H_

