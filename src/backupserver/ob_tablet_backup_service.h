/*
 *  (C) 2007-2010 Taobao Inc.
 *
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License version 2 as
 *  published by the Free Software Foundation.
 *
 *
 *  Version: 1.0 : 2010/08/22
 *
 *  Authors:
 *     qushan <qushan@taobao.com>
 */
#ifndef OCEANBASE_BACKUPSERVER_BACKUPSERVICE_H_
#define OCEANBASE_BACKUPSERVER_BACKUPSERVICE_H_

#include "common/ob_define.h"
#include "common/thread_buffer.h"
#include "common/data_buffer.h"
#include "easy_io.h"
#include "common/ob_data_source_desc.h"
#include "common/ob_schema.h"
#include "common/ob_general_rpc_proxy.h"

namespace oceanbase
{
  namespace backupserver
  {
    class ObBackupServer;
    class ObTabletBackupService
    {
      public:
        ObTabletBackupService();
        ~ObTabletBackupService();
      public:
        int initialize(ObBackupServer* backup_server);
        int start();
        int destroy();

      public:
        int do_request(
            const int64_t receive_time,
            const int32_t packet_code,
            const int32_t version,
            const int32_t channel_id,
            easy_request_t* req,
            common::ObDataBuffer& in_buffer,
            common::ObDataBuffer& out_buffer,
            const int64_t timeout_time = 0);

        int backup_fetch_data(
            const int32_t version,
            const int32_t channel_id,
            easy_request_t* req,
            common::ObDataBuffer& in_buffer,
            common::ObDataBuffer& out_buffer,
            const int64_t timeout_time);

        int backup_migrate_over(
            const int32_t version,
            const int32_t channel_id,
            easy_request_t* req,
            common::ObDataBuffer& in_buffer,
            common::ObDataBuffer& out_buffer,
            const int64_t timeout_time);

        int backup_dump_tablet_image(
            const int32_t version,
            const int32_t channel_id,
            easy_request_t* req,
            common::ObDataBuffer& in_buffer,
            common::ObDataBuffer& out_buffer);

        int backup_check_process(
            const int32_t version,
            const int32_t channel_id,
            easy_request_t* req,
            common::ObDataBuffer& in_buffer,
            common::ObDataBuffer& out_buffer);

        int backup_show_param(
            const int32_t version,
            const int32_t channel_id,
            easy_request_t* req,
            common::ObDataBuffer& in_buffer,
            common::ObDataBuffer& out_buffer);

        int backup_sync_all_images(
            const int32_t version,
            const int32_t channel_id,
            easy_request_t* req,
            common::ObDataBuffer& in_buffer,
            common::ObDataBuffer& out_buffer);

        int backup_install_disk(
            const int32_t version,
            const int32_t channel_id,
            easy_request_t* req,
            common::ObDataBuffer& in_buffer,
            common::ObDataBuffer& out_buffer);

        int backup_uninstall_disk(
            const int32_t version,
            const int32_t channel_id,
            easy_request_t* req,
            common::ObDataBuffer& in_buffer,
            common::ObDataBuffer& out_buffer);


        int backup_show_disk(
            const int32_t version,
            const int32_t channel_id,
            easy_request_t* req,
            common::ObDataBuffer& in_buffer,
            common::ObDataBuffer& out_buffer);

        int backup_stop_server(
            const int32_t version,
            const int32_t channel_id,
            easy_request_t* req,
            common::ObDataBuffer& in_buffer,
            common::ObDataBuffer& out_buffer);

        int backup_database(
            const int32_t version,
            const int32_t channel_id,
            easy_request_t* req,
            common::ObDataBuffer& in_buffer,
            common::ObDataBuffer& out_buffer);

        int backup_table(
            const int32_t version,
            const int32_t channel_id,
            easy_request_t* req,
            common::ObDataBuffer& in_buffer,
            common::ObDataBuffer& out_buffer);

        int backup_abort(
            const int32_t version,
            const int32_t channel_id,
            easy_request_t* req,
            common::ObDataBuffer& in_buffer,
            common::ObDataBuffer& out_buffer);

        int backup_check_privilege(
            const int32_t version,
            const int32_t channel_id,
            easy_request_t* req,
            common::ObDataBuffer& in_buffer,
            common::ObDataBuffer& out_buffer);


      private:
        int load_tablets();
        int check_compress_lib(const char* compress_name_buf);
        int start_backup_task(int mode, const char * db_name = NULL, const char * table_name = NULL);
        int abort_backup_task();

      private:
        DISALLOW_COPY_AND_ASSIGN(ObTabletBackupService);
        ObBackupServer* backup_server_;
        bool inited_;
        bool service_started_;
        volatile uint32_t migrate_task_count_;



        //hashmap backup_map_;
        //common::ThreadSpecificBuffer query_service_buffer_;
    };


  } // end namespace backupserver
} // end namespace oceanbase


#endif //OCEANBASE_BACKUPSERVER_BACKUPSERVICE_H_

