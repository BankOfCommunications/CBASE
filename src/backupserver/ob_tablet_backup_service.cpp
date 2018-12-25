/*
 *  (C) 2007-2010 Taobao Inc.
 *
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License version 2 as
 *  published by the Free Software Foundation.
 *
 *         ????.cpp is for what ...
 *
 *  Version: $Id: ipvsadm.c,v 1.27 2005/12/10 16:00:07 wensong Exp $
 *
 *  Authors:
 *     Author Name <email address>
 *        - some work details if you want
 */

#include "ob_tablet_backup_service.h"
#include "ob_backup_server.h"
#include "common/ob_atomic.h"
#include "common/utility.h"
#include "common/ob_define.h"
#include "common/data_buffer.h"
#include "common/ob_packet.h"
#include "common/ob_read_common_data.h"
#include "common/ob_scanner.h"
#include "common/ob_new_scanner.h"
#include "common/ob_range2.h"
#include "common/ob_result.h"
#include "common/file_directory_utils.h"
#include "common/ob_trace_log.h"
#include "common/ob_schema_manager.h"
#include "common/ob_version.h"
#include "common/ob_profile_log.h"
#include "ob_backup_server_main.h"
#include "common/ob_common_stat.h"

#include "chunkserver/ob_tablet_writer.h"


using namespace oceanbase::common;
using namespace oceanbase::chunkserver;

namespace oceanbase
{
  using backupserver::ObBackupServer;
  namespace backupserver
  {
    ObTabletBackupService::ObTabletBackupService()
    : backup_server_(NULL), inited_(false), service_started_(false), migrate_task_count_(0)
    {
    }

    ObTabletBackupService::~ObTabletBackupService()
    {
      destroy();
    }

    /**
     * use ObTabletBackupService after initialized.
     */
    int ObTabletBackupService::initialize(ObBackupServer* server)
    {
      int ret = OB_SUCCESS;

      if (inited_)
      {
        ret = OB_INIT_TWICE;
      }
      else if (NULL == server)
      {
        ret = OB_INVALID_ARGUMENT;
      }

      backup_server_ = server;
      if (OB_SUCCESS == ret)
      {
        inited_ = true;
      }
      return ret;
    }

    int ObTabletBackupService::destroy()
    {
      int rc = OB_SUCCESS;
      if (inited_)
      {
        inited_ = false;
        backup_server_ = NULL;
        backup_server_->stop_threads();
        service_started_ = false;
      }
      else
      {
        rc = OB_NOT_INIT;
      }

      return rc;
    }

    int ObTabletBackupService::start()
    {
      int rc = OB_SUCCESS;
      if (!inited_)
      {
        rc = OB_NOT_INIT;
      }
      else
      {
        rc = backup_server_->init_backup_rpc();
        if (rc != OB_SUCCESS)
        {
          TBSYS_LOG(ERROR, "init backup rpc failed.");
        }
      }

      if (OB_SUCCESS == rc)
      {
        rc = check_compress_lib(backup_server_->get_config().check_compress_lib);
        if (OB_SUCCESS != rc)
        {
          TBSYS_LOG(ERROR, "the compress lib in the list is not exist: rc=[%d]", rc);
        }
      }

      if (OB_SUCCESS == rc)
      {
        rc = load_tablets();
        if (OB_SUCCESS != rc)
        {
          TBSYS_LOG(ERROR, "load local tablets error, rc=%d", rc);
        }
      }

      backup_server_->start_threads();
      service_started_ = true;
      return rc;
    }

    int ObTabletBackupService::start_backup_task(int mode, const char * db_name, const char * table_name)
    {
      int ret = OB_SUCCESS;
      if (!service_started_)
      {
        ret = OB_NOT_INIT;
        TBSYS_LOG(WARN, "start rs and ups fetch task failed.");
      }
      else
      {
        ret = backup_server_->start_backup_task(mode);
        if (ret == OB_SUCCESS)
        {
          TBSYS_LOG(INFO, "start rs and ups fetch task succeed.");
        }
        else
        {
          TBSYS_LOG(ERROR, "start rs and ups fetch task failed.");
        }
      }
      if (OB_SUCCESS == ret && mode < INCREMENTAL_BACKUP)
      {
          ObTabletBackupManager * backup_manager = backup_server_->get_backup_manager();
          ret = backup_manager->do_start_backup_task(db_name, table_name);
      }
      else
      {
        backup_server_->report_sub_task_finish(OB_CHUNKSERVER,ret);
      }
      return ret;
    }

    int ObTabletBackupService::abort_backup_task()
    {
      int ret = OB_SUCCESS;
      if (OB_SUCCESS == ret && !service_started_)
      {

        backup_server_->abort_ups_backup_task();
        ObTabletBackupManager * backup_manager = backup_server_->get_backup_manager();
        ret = backup_manager->do_abort_backup_task();
      }
      return ret;
    }


    /*
     * after initialize() & start(), then can handle the request.
     */
    int ObTabletBackupService::do_request(
        const int64_t receive_time,
        const int32_t packet_code,
        const int32_t version,
        const int32_t channel_id,
        easy_request_t* req,
        common::ObDataBuffer& in_buffer,
        common::ObDataBuffer& out_buffer,
        const int64_t timeout_time)
    {
      UNUSED(receive_time);
      int rc = OB_SUCCESS;
      if (!inited_)
      {
        TBSYS_LOG(ERROR, "service not initialized, cannot accept any message.");
        rc = OB_NOT_INIT;
      }

      if (OB_SUCCESS != rc)
      {
        common::ObResultCode result;
        result.result_code_ = rc;
        // send response.
        int serialize_ret = result.serialize(out_buffer.get_data(),
            out_buffer.get_capacity(), out_buffer.get_position());
        if (OB_SUCCESS != serialize_ret)
        {
          TBSYS_LOG(ERROR, "serialize result code object failed.");
        }
        else
        {
          backup_server_->send_response(
              packet_code + 1, version,
              out_buffer, req, channel_id);
        }
      }
      else
      {
        switch(packet_code)
        {
          case OB_CS_MIGRATE:
            rc = backup_fetch_data(version, channel_id, req, in_buffer, out_buffer, timeout_time);
            break;
          case OB_MIGRATE_OVER:
            rc = backup_migrate_over(version, channel_id, req, in_buffer, out_buffer, timeout_time);
            break;
          case OB_CS_DUMP_TABLET_IMAGE:
            rc = backup_dump_tablet_image(version, channel_id, req, in_buffer, out_buffer);
            break;
          case OB_CHECK_BACKUP_PROCESS:
            rc = backup_check_process(version, channel_id, req, in_buffer, out_buffer);
            break;
          case OB_CS_SHOW_PARAM:
            rc = backup_show_param(version, channel_id, req, in_buffer, out_buffer);
            break;
          case OB_CS_SYNC_ALL_IMAGES:
            rc = backup_sync_all_images(version, channel_id, req, in_buffer, out_buffer);
            break;
          case OB_CS_INSTALL_DISK:
            rc = backup_install_disk(version, channel_id, req, in_buffer, out_buffer);
            break;
          case OB_CS_UNINSTALL_DISK:
            rc = backup_uninstall_disk(version, channel_id, req, in_buffer, out_buffer);
            break;
          case OB_CS_SHOW_DISK:
            rc = backup_show_disk(version, channel_id, req, in_buffer, out_buffer);
            break;
          case OB_STOP_SERVER:
            rc = backup_stop_server(version, channel_id, req, in_buffer, out_buffer);
            break;
          case OB_BACKUP_DATABASE:
            rc = backup_database(version ,channel_id, req, in_buffer, out_buffer);
            break;
          case OB_BACKUP_TABLE:
            rc = backup_table(version, channel_id, req, in_buffer, out_buffer);
            break;
          case OB_BACKUP_ABORT:
            rc = backup_abort(version, channel_id, req, in_buffer, out_buffer);
            break;
          case OB_CHECK_BACKUP_PRIVILEGE:
            rc = backup_check_privilege(version, channel_id, req, in_buffer, out_buffer);
            break;
          /*case OB_FETCH_RANGE_TABLE :
            rc = backup_task_over(version, channel_id, req, in_buffer, out_buffer, timeout_time);
            break;
            */
          default:
            rc = OB_ERROR;
            TBSYS_LOG(WARN, "not support packet code[%d]", packet_code);
            break;
        }
      }
      return rc;
    }

    int ObTabletBackupService::backup_fetch_data(
        const int32_t version,
        const int32_t channel_id,
        easy_request_t* req,
        common::ObDataBuffer& in_buffer,
        common::ObDataBuffer& out_buffer,
        const int64_t timeout_time)
    {
      TBSYS_LOG(INFO, "timeout_time =%ld", timeout_time);
      const int32_t CS_FETCH_DATA_VERSION = 3;
      common::ObResultCode rc;
      rc.result_code_ = OB_SUCCESS;
      int32_t is_inc_migrate_task_count = 0;
      //tbsys::CRLockGuard guard(del_tablet_rwlock_);

      if (version != CS_FETCH_DATA_VERSION)
      {
        rc.result_code_ = OB_ERROR_FUNC_VERSION;
      }

      ObDataSourceDesc desc;
      CharArena allocator;
      ObTabletFileWriter* tablet_writer = GET_TSI_MULT(ObTabletFileWriter, TSI_CS_FETCH_DATA_1);

      if (NULL == tablet_writer)
      {
        TBSYS_LOG(ERROR, "failed to get thread local tablet_writer, "
            "tablet_writer=%p", tablet_writer);
        rc.result_code_ = OB_ALLOCATE_MEMORY_FAILED;
      }
      else
      {
        tablet_writer->reset();
      }


      if (OB_SUCCESS == rc.result_code_)
      {
        rc.result_code_ = desc.deserialize(allocator, in_buffer.get_data(), in_buffer.get_capacity(),
            in_buffer.get_position());
        if (OB_SUCCESS != rc.result_code_)
        {
          TBSYS_LOG(ERROR, "parse data source param error.");
        }
        if (!desc.dst_server_.is_valid())
        {
          desc.dst_server_ = backup_server_->get_self();
        }
      }

      //response to root server first
      int serialize_ret = rc.serialize(out_buffer.get_data(),
          out_buffer.get_capacity(), out_buffer.get_position());
      if (serialize_ret != OB_SUCCESS)
      {
        TBSYS_LOG(ERROR, "fetch_data rc.serialize error");
      }

      if (OB_SUCCESS == serialize_ret)
      {
        backup_server_->send_response(
            OB_MIGRATE_OVER_RESPONSE,
            CS_FETCH_DATA_VERSION,
            out_buffer, req, channel_id);
      }

      if (OB_SUCCESS == rc.result_code_)
      {
        TBSYS_LOG(INFO, "begin migrate data %s", to_cstring(desc));
      }

      if (!backup_server_->get_config().merge_migrate_concurrency
          && (!backup_server_->get_tablet_manager().get_chunk_merge().is_merge_stoped()))
      {
        TBSYS_LOG(WARN, "merge running, cannot migrate.");
        rc.result_code_ = OB_CS_EAGAIN;
      }
      else if (!backup_server_->get_tablet_manager().get_bypass_sstable_loader().is_loader_stoped())
      {
        TBSYS_LOG(WARN, "load bypass sstable is running, cannot migrate");
        rc.result_code_ = OB_CS_EAGAIN;
      }

      int64_t max_migrate_task_count = backup_server_->get_config().max_migrate_task_count;
      if (OB_SUCCESS == rc.result_code_ )
      {
        uint32_t old_migrate_task_count = migrate_task_count_;
        while(old_migrate_task_count < max_migrate_task_count)
        {
          uint32_t tmp = atomic_compare_exchange(&migrate_task_count_, old_migrate_task_count+1, old_migrate_task_count);
          if (tmp == old_migrate_task_count)
          {
            is_inc_migrate_task_count = 1;
            break;
          }
          old_migrate_task_count = migrate_task_count_;
        }

        if (0 == is_inc_migrate_task_count)
        {
          TBSYS_LOG(WARN, "current migrate task count = %u, exceeded max = %ld",
              old_migrate_task_count, max_migrate_task_count);
          rc.result_code_ = OB_CS_EAGAIN;
        }
        else
        {
          TBSYS_LOG(INFO, "current migrate task count = %u", migrate_task_count_);
        }
      }

      ObTabletManager & tablet_manager = backup_server_->get_tablet_manager();
      ObMultiVersionTabletImage & tablet_image = tablet_manager.get_serving_tablet_image();
      int64_t tablet_version = 0;

      if (OB_SUCCESS == rc.result_code_ )
      {
        int16_t remote_sstable_version = 0;
        int16_t local_sstable_version = 0;
        int64_t old_tablet_version = 0;
        bool migrate_locked = true;

        out_buffer.get_position() = 0;
        ObTabletRemoteReader remote_reader(out_buffer, tablet_manager);
        local_sstable_version = static_cast<int16_t>(THE_BACKUP_SERVER.get_config().merge_write_sstable_version);
        int64_t sequence_num = 0;
        int64_t row_checksum = 0;
        int64_t sstable_type = 0;
        desc.sstable_version_ = local_sstable_version;
        tablet_version = desc.tablet_version_;

        //check is in pending in upgrade
        if (!tablet_manager.get_chunk_merge().get_migrate_lock().try_rdlock())
        {
          TBSYS_LOG(WARN, "daily merge is pending in upgrade");
          migrate_locked = false;
          rc.result_code_ = OB_CS_EAGAIN;
        }
        //remote reader prepare, send first request, and get the remote_sstable_version of the src server
        else if(OB_SUCCESS != (rc.result_code_ =
              tablet_manager.check_fetch_tablet_version(desc.range_, tablet_version, old_tablet_version)))
        {
          TBSYS_LOG(WARN, "tablet range:%s data_versin:%ld check failed, ret:%d",
              to_cstring(desc.range_), tablet_version, rc.result_code_);
        }
        else if(OB_SUCCESS != (rc.result_code_ = remote_reader.prepare(desc,
                remote_sstable_version, sequence_num, row_checksum, sstable_type)))
        {
          TBSYS_LOG(WARN, "prepare remote reader failed ret:%d", rc.result_code_);
        }
        else if(local_sstable_version != remote_sstable_version)
        {
          rc.result_code_ = OB_SSTABLE_VERSION_UNEQUAL;
          TBSYS_LOG(ERROR, "sstable version not equal local_sstable_version:%d remote_sstable_version:%d",
              local_sstable_version, remote_sstable_version);
        }
        else if(OB_SUCCESS != (rc.result_code_ = remote_reader.open()))
        {
          TBSYS_LOG(WARN, "remote reader open failed, ret:%d", rc.result_code_);
        }
        else if(OB_SUCCESS != (rc.result_code_ =
              tablet_writer->prepare(desc.range_, tablet_version, sequence_num, row_checksum, &tablet_manager)))
        {
          TBSYS_LOG(WARN, "remote writer prepare failed, ret:%d", rc.result_code_);
        }
        else if(OB_SUCCESS != (rc.result_code_ = tablet_writer->open()))
        {
          TBSYS_LOG(WARN, "remote writer open failed, ret:%d", rc.result_code_);
        }
        else
        {
          rc.result_code_ = pipe_buffer(tablet_manager, remote_reader, *tablet_writer);

          int ret = OB_SUCCESS;
          if(OB_SUCCESS != (ret = remote_reader.close()))
          {
            TBSYS_LOG(WARN, "remote reader close failed, ret:%d", ret);
          }

          if(OB_SUCCESS == rc.result_code_)
          {
            if(OB_SUCCESS != (rc.result_code_ = tablet_writer->close()))
            {
              TBSYS_LOG(WARN, "tablet writer close failed, ret:%d", rc.result_code_);
            }
          }

        }

        if(OB_SUCCESS == rc.result_code_ && old_tablet_version > 0)
        {
          //set old tablet merged, not need to merge
          ObTablet* old_tablet = NULL;
          if(OB_SUCCESS == tablet_image.acquire_tablet(desc.range_,
                ObMultiVersionTabletImage::SCAN_FORWARD, old_tablet_version, old_tablet) &&
              old_tablet != NULL &&
              old_tablet->get_data_version() == old_tablet_version &&
              old_tablet->get_range().equal2(desc.range_))
          {
            old_tablet->set_merged();
            tablet_image.write(old_tablet->get_data_version(), old_tablet->get_disk_no());
          }
          else
          {
            TBSYS_LOG(WARN, "not found old tablet:%s old_tablet_version:%ld, maybe deleted",
                to_cstring(desc.range_), old_tablet_version);
          }

          if(NULL != old_tablet)
          {
            tablet_image.release_tablet(old_tablet);
            old_tablet = NULL;
          }
        }

        int ret = OB_SUCCESS;
        if(OB_SUCCESS != (ret = tablet_writer->cleanup()))
        {
          TBSYS_LOG(WARN, "tablet writer cleanup failed, ret:%d", ret);
        }

        if(migrate_locked)
        {
          //unlock
          tablet_manager.get_chunk_merge().get_migrate_lock().unlock();
        }
      }

      /**
       * it's better to decrease the migrate task counter before
       * calling migrate_over(), because rootserver keeps the same
       * migrate task counter of each chunkserver, if migrate_over()
       * returned, rootserver has decreased the migrate task counter
       * and send a new migrate task to this chunkserver immediately,
       * if this chunkserver hasn't decreased the migrate task counter
       * at this time, the chunkserver will return -1010 error. if
       * rootserver doesn't send migrate message successfully, it
       * doesn't increase the migrate task counter, so it sends
       * migrate message continiously and it receives a lot of error
       * -1010.
       */
      if (0 != is_inc_migrate_task_count)
      {
        atomic_dec(&migrate_task_count_);
      }


      //report migrate info to root server
      desc.tablet_version_ = tablet_version;
      if(OB_CS_MIGRATE_IN_EXIST == rc.result_code_)
      {
        rc.result_code_ = OB_SUCCESS;
      }
      int64_t occupy_size = 0;
      int64_t check_sum = 0;
      uint64_t row_checksum = 0;
      int64_t row_count = 0;
      ObTablet* tablet = NULL;
      if(OB_SUCCESS == rc.result_code_)
      {
        if(OB_SUCCESS == tablet_image.acquire_tablet_all_version(desc.range_,
              ObMultiVersionTabletImage::SCAN_FORWARD,
              ObMultiVersionTabletImage::FROM_NEWEST_INDEX, 0, tablet) &&
            NULL != tablet &&
            tablet->get_data_version() == desc.tablet_version_)
        {
          occupy_size = tablet->get_occupy_size();
          check_sum = tablet->get_checksum();
          row_checksum = tablet->get_row_checksum();
          row_count = tablet->get_row_count();
        }
        else
        {
          TBSYS_LOG(WARN, "add tablet succ, but is deleted, range:%s tablet_version:%ld",
              to_cstring(desc.range_), desc.tablet_version_);
          rc.result_code_ = OB_ERROR;
        }

        if(NULL != tablet)
        {
          tablet_image.release_tablet(tablet);
          tablet = NULL;
        }
      }

      int ret = OB_SUCCESS;
      ret = backup_server_->get_rpc_stub().migrate_over(
            backup_server_->get_config().network_timeout,
            backup_server_->get_self(),rc,desc,occupy_size,check_sum,row_checksum,row_count);
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "report migrate over error, rc.code=%d", ret);
        if(OB_SUCCESS == rc.result_code_)
        {
          rc.result_code_ = ret;
        }
      }
      else
      {
        TBSYS_LOG(INFO,"report migrate tablet <%s> over to rootserver success, ret=%d",
            to_cstring(desc.range_), rc.result_code_);
      }

      if(OB_SUCCESS != rc.result_code_)
      {
        TBSYS_LOG(WARN, "fetch_data failed rc.code=%d",rc.result_code_);
      }
      else
      {
        TBSYS_LOG(INFO, "fetch_data finish rc.code=%d",rc.result_code_);
      }

      return rc.result_code_;
    }

    int ObTabletBackupService::backup_migrate_over(
        const int32_t version,
        const int32_t channel_id,
        easy_request_t* req,
        common::ObDataBuffer& in_buff,
        common::ObDataBuffer& out_buff,
        const int64_t timeout_time)
    {
      TBSYS_LOG(INFO, "timeout_time =%ld", timeout_time);
      static const int CS_MIGRATE_OVER_VERSION = 3;
      int ret = OB_SUCCESS;
      common::ObResultCode result_msg;
      if (version != CS_MIGRATE_OVER_VERSION)
      {
        ret = OB_ERROR_FUNC_VERSION;
      }

      ObDataSourceDesc desc;
      int64_t occupy_size = 0;
      uint64_t crc_sum = 0;
      int64_t row_count = 0;
      int64_t row_checksum = 0;

      ObTabletBackupManager *backup_mgr = backup_server_->get_backup_manager();

      if (OB_SUCCESS == ret)
      {
        ret = result_msg.deserialize(in_buff.get_data(), in_buff.get_capacity(), in_buff.get_position());
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(WARN, "failed to deserialize reuslt msg, ret=%d", ret);
        }
      }
      if (OB_SUCCESS == ret)
      {
        ret = desc.deserialize(in_buff.get_data(), in_buff.get_capacity(), in_buff.get_position());
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(WARN, "desc.deserialize error, ret=%d", ret);
        }
      }
      if (OB_SUCCESS == ret)
      {
        ret = serialization::decode_vi64(in_buff.get_data(), in_buff.get_capacity(),
            in_buff.get_position(), &occupy_size);
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(WARN, "failed to decode occupy size, ret=%d", ret);
        }
      }
      if (OB_SUCCESS == ret)
      {
        ret = serialization::decode_vi64(in_buff.get_data(), in_buff.get_capacity(),
            in_buff.get_position(), reinterpret_cast<int64_t*>(&crc_sum));
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(WARN, "failed to decode crc sum, ret=%d", ret);
        }
      }
      if (OB_SUCCESS == ret)
      {
        ret = serialization::decode_vi64(in_buff.get_data(), in_buff.get_capacity(),
            in_buff.get_position(), reinterpret_cast<int64_t*>(&row_checksum));
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(WARN, "failed to decode crc sum, ret=%d", ret);
        }
      }
      if (OB_SUCCESS == ret)
      {
        ret = serialization::decode_vi64(in_buff.get_data(), in_buff.get_capacity(),
            in_buff.get_position(), &row_count);
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(WARN, "failed to decode row count, ret=%d", ret);
        }
      }

      if (OB_SUCCESS == ret)
      {
        result_msg.result_code_ = backup_mgr->do_tablet_backup_over(result_msg.result_code_,desc);
      }
      else
      {
        result_msg.result_code_ = ret;
      }

      int tmp_ret = result_msg.serialize(out_buff.get_data(), out_buff.get_capacity(), out_buff.get_position());
      if (OB_SUCCESS != tmp_ret)
      {
        TBSYS_LOG(ERROR, "result_msg.serialize error, ret=%d", tmp_ret);
        if (OB_SUCCESS == ret)
        {
          tmp_ret = ret;
        }
      }
      else
      {
        backup_server_->send_response
            (OB_MIGRATE_OVER_RESPONSE, CS_MIGRATE_OVER_VERSION, out_buff, req, channel_id);
      }
      return ret;

    }

    int ObTabletBackupService::backup_dump_tablet_image(
        const int32_t version,
        const int32_t channel_id,
        easy_request_t* req,
        common::ObDataBuffer& in_buffer,
        common::ObDataBuffer& out_buffer)
    {
      const int32_t CS_DUMP_TABLET_IMAGE_VERSION = 1;
      common::ObResultCode rc;
      rc.result_code_ = OB_SUCCESS;

      int32_t index = 0;
      int32_t disk_no = 0;

      char *dump_buf = NULL;
      const int64_t dump_size = OB_MAX_PACKET_LENGTH - 1024;

      int64_t pos = 0;
      ObTabletImage * tablet_image = NULL;

      if (version != CS_DUMP_TABLET_IMAGE_VERSION)
      {
        rc.result_code_ = OB_ERROR_FUNC_VERSION;
      }
      else if (OB_SUCCESS != ( rc.result_code_ =
            serialization::decode_vi32(in_buffer.get_data(),
            in_buffer.get_capacity(), in_buffer.get_position(), &index)))
      {
        TBSYS_LOG(WARN, "parse cs_dump_tablet_image index param error.");
      }
      else if (OB_SUCCESS != ( rc.result_code_ =
            serialization::decode_vi32(in_buffer.get_data(),
            in_buffer.get_capacity(), in_buffer.get_position(), &disk_no)))
      {
        TBSYS_LOG(WARN, "parse cs_dump_tablet_image disk_no param error.");
      }
      else if (disk_no <= 0)
      {
        TBSYS_LOG(WARN, "cs_dump_tablet_image input param error, "
            "disk_no=%d", disk_no);
        rc.result_code_ = OB_INVALID_ARGUMENT;
      }
      else if (NULL == (dump_buf = static_cast<char*>(ob_malloc(dump_size, ObModIds::OB_CS_COMMON))))
      {
        rc.result_code_ = OB_ALLOCATE_MEMORY_FAILED;
        TBSYS_LOG(ERROR, "allocate memory for serialization failed.");
      }
      else if (OB_SUCCESS != (rc.result_code_ =
            backup_server_->get_tablet_manager().get_serving_tablet_image().
            serialize(index, disk_no, dump_buf, dump_size, pos)))
      {
        TBSYS_LOG(WARN, "serialize tablet image failed. disk_no=%d", disk_no);
      }

      //response to root server first
      int serialize_ret = rc.serialize(out_buffer.get_data(),
          out_buffer.get_capacity(), out_buffer.get_position());
      if (serialize_ret != OB_SUCCESS)
      {
        TBSYS_LOG(ERROR, "rc.serialize error");
      }

      // ifreturn success , we can return dump_buf
      if (OB_SUCCESS == rc.result_code_ && OB_SUCCESS == serialize_ret)
      {
        ObString return_dump_obj(static_cast<int32_t>(pos), static_cast<int32_t>(pos), dump_buf);
        serialize_ret = return_dump_obj.serialize(out_buffer.get_data(),
            out_buffer.get_capacity(), out_buffer.get_position());
        if (OB_SUCCESS != serialize_ret)
        {
          TBSYS_LOG(ERROR, "serialize return_dump_obj failed.");
        }
      }

      if (OB_SUCCESS == serialize_ret)
      {
        backup_server_->send_response(
            OB_CS_DUMP_TABLET_IMAGE_RESPONSE,
            CS_DUMP_TABLET_IMAGE_VERSION,
            out_buffer, req, channel_id);
      }

      if (NULL != dump_buf)
      {
        ob_free(dump_buf);
        dump_buf = NULL;
      }
      if (NULL != tablet_image)
      {
        delete(tablet_image);
        tablet_image = NULL;
      }

      return rc.result_code_;
    }



    int ObTabletBackupService::backup_check_process(
        const int32_t version,
        const int32_t channel_id,
        easy_request_t* req,
        common::ObDataBuffer& in_buffer,
        common::ObDataBuffer& out_buffer)
    {
      const int32_t CS_SHOW_PARAM_VERSION = 1;
      int ret = OB_SUCCESS;
      UNUSED(in_buffer);

      common::ObResultCode rc;
      rc.result_code_ = OB_SUCCESS;

      if (version != CS_SHOW_PARAM_VERSION)
      {
        rc.result_code_ = OB_ERROR_FUNC_VERSION;
      }

      rc.result_code_ = backup_server_->get_backup_manager()->print();
      ret = rc.serialize(out_buffer.get_data(),out_buffer.get_capacity(), out_buffer.get_position());

      if (ret != OB_SUCCESS)
      {
        TBSYS_LOG(ERROR, "rc.serialize error");
      }

      if (OB_SUCCESS == ret)
      {
        backup_server_->send_response(
            OB_CHECK_BACKUP_PROCESS_RESPONSE,
            CS_SHOW_PARAM_VERSION,
            out_buffer, req, channel_id);
      }

      return ret;
    }


    int ObTabletBackupService::backup_show_param(
        const int32_t version,
        const int32_t channel_id,
        easy_request_t* req,
        common::ObDataBuffer& in_buffer,
        common::ObDataBuffer& out_buffer)
    {
      const int32_t CS_SHOW_PARAM_VERSION = 1;
      int ret = OB_SUCCESS;
      UNUSED(in_buffer);

      common::ObResultCode rc;
      rc.result_code_ = OB_SUCCESS;

      if (version != CS_SHOW_PARAM_VERSION)
      {
        rc.result_code_ = OB_ERROR_FUNC_VERSION;
      }

      ret = rc.serialize(out_buffer.get_data(),out_buffer.get_capacity(), out_buffer.get_position());
      if (ret != OB_SUCCESS)
      {
        TBSYS_LOG(ERROR, "rc.serialize error");
      }

      if (OB_SUCCESS == ret)
      {
        backup_server_->send_response(
            OB_RESULT,
            CS_SHOW_PARAM_VERSION,
            out_buffer, req, channel_id);
      }
      backup_server_->get_config().print();
      ob_print_mod_memory_usage();
      return ret;
    }


    int ObTabletBackupService::backup_sync_all_images(
        const int32_t version,
        const int32_t channel_id,
        easy_request_t* req,
        common::ObDataBuffer& in_buffer,
        common::ObDataBuffer& out_buffer)
    {
      const int32_t CS_SYNC_ALL_IMAGES_VERSION = 1;
      int ret = OB_SUCCESS;
      UNUSED(in_buffer);

      ObTabletManager& tablet_manager = backup_server_->get_tablet_manager();
      common::ObResultCode rc;
      rc.result_code_ = OB_SUCCESS;

      TBSYS_LOG(INFO, "backupserver start sync all tablet images");
      if (version != CS_SYNC_ALL_IMAGES_VERSION)
      {
        rc.result_code_ = OB_ERROR_FUNC_VERSION;
      }

      ret = rc.serialize(out_buffer.get_data(),out_buffer.get_capacity(), out_buffer.get_position());
      if (ret != OB_SUCCESS)
      {
        TBSYS_LOG(ERROR, "rc.serialize error");
      }

      if (OB_SUCCESS == ret)
      {
        backup_server_->send_response(
            OB_RESULT,
            CS_SYNC_ALL_IMAGES_VERSION,
            out_buffer, req, channel_id);
      }

      if (inited_ && service_started_
          && !tablet_manager.get_chunk_merge().is_pending_in_upgrade())
      {
        rc.result_code_ = tablet_manager.sync_all_tablet_images();
      }
      else
      {
        TBSYS_LOG(WARN, "can't sync tablet images now, please try again later");
        rc.result_code_ = OB_CS_EAGAIN;
      }
      TBSYS_LOG(INFO, "finish sync all tablet images, ret=%d", rc.result_code_);

      return ret;
    }

    int ObTabletBackupService::backup_install_disk(
        const int32_t version,
        const int32_t channel_id,
        easy_request_t* req,
        common::ObDataBuffer& in_buffer,
        common::ObDataBuffer& out_buffer)
    {
      common::ObResultCode rc;
      const int32_t CS_INSTALL_DISK_VERSION = 1;
      rc.result_code_ = OB_SUCCESS;

      if(version != CS_INSTALL_DISK_VERSION)
      {
        rc.result_code_ = OB_ERROR_FUNC_VERSION;
      }
      else if(backup_server_->get_tablet_manager().is_disk_maintain())
      {
        TBSYS_LOG(WARN, "disk is maintaining");
        rc.result_code_ = OB_CS_EAGAIN;
      }
      else if (!backup_server_->get_tablet_manager().get_bypass_sstable_loader().is_loader_stoped())
      {
        TBSYS_LOG(WARN, "load bypass sstable is running, cannot do disk maintain");
        rc.result_code_ = OB_CS_EAGAIN;
      }

      int32_t disk_no = -1;
      char mount_path[OB_MAX_FILE_NAME_LENGTH];
      memset(mount_path, 0, sizeof(mount_path));

      ObString mount_path_str(static_cast<common::ObString::obstr_size_t>(OB_MAX_FILE_NAME_LENGTH),
          static_cast<common::ObString::obstr_size_t>(OB_MAX_FILE_NAME_LENGTH), mount_path);

      if(OB_SUCCESS != rc.result_code_)
      {
      }
      else if (OB_SUCCESS != (rc.result_code_ = mount_path_str.deserialize(in_buffer.get_data(),
              in_buffer.get_capacity(), in_buffer.get_position())))
      {
        TBSYS_LOG(WARN, "decode mount path failed");
      }
      else
      {
        rc.result_code_ = backup_server_->get_tablet_manager().install_disk(mount_path, disk_no);
      }

      int serialize_ret = rc.serialize(out_buffer.get_data(),
          out_buffer.get_capacity(), out_buffer.get_position());
      if (serialize_ret != OB_SUCCESS)
      {
        TBSYS_LOG(WARN, "cs_load_disk rc.serialize error, ret:%d", serialize_ret);
      }
      else if (OB_SUCCESS != (serialize_ret = serialization::encode_vi32(out_buffer.get_data(),
              out_buffer.get_capacity(), out_buffer.get_position(), disk_no)))
      {
        TBSYS_LOG(WARN, "cs_load_disk mount disk_no:%d serialize failed, ret:%d",
            disk_no, serialize_ret);
      }

      if (OB_SUCCESS == serialize_ret)
      {
        backup_server_->send_response(
            OB_CS_INSTALL_DISK_RESPONSE,
            CS_INSTALL_DISK_VERSION,
            out_buffer, req, channel_id);
      }

      return rc.result_code_;
    }

    int ObTabletBackupService::backup_uninstall_disk(
        const int32_t version,
        const int32_t channel_id,
        easy_request_t* req,
        common::ObDataBuffer& in_buffer,
        common::ObDataBuffer& out_buffer)
    {
      common::ObResultCode rc;
      const int32_t CS_UNINSTALL_DISK_VERSION = 1;
      rc.result_code_ = OB_SUCCESS;

      if(version != CS_UNINSTALL_DISK_VERSION)
      {
        rc.result_code_ = OB_ERROR_FUNC_VERSION;
      }
      else if(backup_server_->get_tablet_manager().is_disk_maintain())
      {
        TBSYS_LOG(WARN, "disk is maintaining");
        rc.result_code_ = OB_CS_EAGAIN;
      }
      else if (!backup_server_->get_tablet_manager().get_bypass_sstable_loader().is_loader_stoped())
      {
        TBSYS_LOG(WARN, "load bypass sstable is running, cannot do disk maintain");
        rc.result_code_ = OB_CS_EAGAIN;
      }

      int32_t disk_no = -1;

      if(OB_SUCCESS != rc.result_code_)
      {
      }
      else if (OB_SUCCESS != ( rc.result_code_ =
            serialization::decode_vi32(in_buffer.get_data(),
              in_buffer.get_capacity(), in_buffer.get_position(), &disk_no)))
      {
        TBSYS_LOG(WARN, "parse disk maintain param error.");
      }
      else
      {
        rc.result_code_ = backup_server_->get_tablet_manager().uninstall_disk(disk_no);
      }

      int serialize_ret = rc.serialize(out_buffer.get_data(),
          out_buffer.get_capacity(), out_buffer.get_position());
      if (serialize_ret != OB_SUCCESS)
      {
        TBSYS_LOG(WARN, "cs_load_disk rc.serialize error, ret:%d", serialize_ret);
      }

      if (OB_SUCCESS == serialize_ret)
      {
        backup_server_->send_response(
            OB_CS_UNINSTALL_DISK_RESPONSE,
            CS_UNINSTALL_DISK_VERSION,
            out_buffer, req, channel_id);
      }

      return rc.result_code_;
    }


    int ObTabletBackupService::backup_show_disk(
        const int32_t version,
        const int32_t channel_id,
        easy_request_t* req,
        common::ObDataBuffer& in_buffer,
        common::ObDataBuffer& out_buffer)
    {
      UNUSED(in_buffer);
      common::ObResultCode rc;
      const int32_t CS_SHOW_DISK_VERSION = 1;
      rc.result_code_ = OB_SUCCESS;

      if (version != CS_SHOW_DISK_VERSION)
      {
        rc.result_code_ = OB_ERROR_FUNC_VERSION;
      }
      else
      {
        ObTabletManager & tablet_manager = backup_server_->get_tablet_manager();
        ObDiskManager & disk_manager = tablet_manager.get_disk_manager();

        int32_t disk_num = 0;
        int32_t serialize_ret = 0;
        const int32_t* disk_no_array = disk_manager.get_disk_no_array(disk_num);

        if(OB_SUCCESS != (serialize_ret = rc.serialize(out_buffer.get_data(), out_buffer.get_capacity(), out_buffer.get_position())))
        {
          TBSYS_LOG(WARN, "serialize the rc failed, ret:%d", serialize_ret);
        }

        if (OB_SUCCESS == serialize_ret)
        {
          serialize_ret = serialization::encode_i32(out_buffer.get_data(), out_buffer.get_capacity(), out_buffer.get_position(), disk_num);
        }

        if (OB_SUCCESS == serialize_ret && NULL != disk_no_array)
        {
          for (int32_t i = 0; i < disk_num; i++)
          {
            if (OB_SUCCESS == serialize_ret)
            {
              serialize_ret = serialization::encode_i32(out_buffer.get_data(), out_buffer.get_capacity(), out_buffer.get_position(), disk_no_array[i]);
            }

            if (OB_SUCCESS == serialize_ret)
            {
              serialize_ret = serialization::encode_bool(out_buffer.get_data(), out_buffer.get_capacity(), out_buffer.get_position(),
                  disk_manager.is_disk_avail(disk_no_array[i]));
            }
          }
        }

        if (OB_SUCCESS == serialize_ret)
        {
          backup_server_->send_response(
              OB_CS_SHOW_DISK_RESPONSE,
              CS_SHOW_DISK_VERSION,
              out_buffer, req, channel_id);
        }
        else
        {
          TBSYS_LOG(WARN, "serialize failed, ret:%d", serialize_ret);
        }
      }

      return rc.result_code_;
    }


    int ObTabletBackupService::backup_stop_server(
      const int32_t version,
      const int32_t channel_id,
      easy_request_t* req,
      common::ObDataBuffer& in_buffer,
      common::ObDataBuffer& out_buffer)
    {
      UNUSED(in_buffer);
      common::ObResultCode rc;
      rc.result_code_ = OB_SUCCESS;

      //int64_t server_id = chunk_server_->get_root_server().get_ipv4_server_id();
      int64_t peer_id = convert_addr_to_server(req->ms->c->addr);

      /*
      if (server_id != peer_id)
      {
        TBSYS_LOG(WARN, "*stop server* WARNNING coz packet from unrecongnized address "
                  "which is [%lld], should be [%lld] as rootserver.", peer_id, server_id);
        // comment follow line not to strict packet from rs.
        // rc.result_code_ = OB_ERROR;
      }
      */

      int32_t restart = 0;
      rc.result_code_ = serialization::decode_i32(in_buffer.get_data(), in_buffer.get_capacity(),
                                                  in_buffer.get_position(), &restart);

      //int64_t pos = 0;
      //rc.result_code_ = serialization::decode_i32(in_buffer.get_data(), in_buffer.get_position(), pos, &restart);

      if (restart != 0)
      {
        BaseMain::set_restart_flag();
        TBSYS_LOG(INFO, "receive *restart server* packet from: [%ld]", peer_id);
      }
      else
      {
        TBSYS_LOG(INFO, "receive *stop server* packet from: [%ld]", peer_id);
      }

      int serialize_ret = rc.serialize(out_buffer.get_data(),
          out_buffer.get_capacity(), out_buffer.get_position());
      if (serialize_ret != OB_SUCCESS)
      {
        TBSYS_LOG(ERROR, "rc.serialize error");
      }

      if (OB_SUCCESS == serialize_ret)
      {
        backup_server_->send_response(
          OB_STOP_SERVER_RESPONSE,
          version,
          out_buffer, req, channel_id);
      }

      if (OB_SUCCESS == rc.result_code_)
      {

        backup_server_->stop();
      }
      return rc.result_code_;
    }

    int ObTabletBackupService::backup_database(
        const int32_t version,
        const int32_t channel_id,
        easy_request_t* req,
        common::ObDataBuffer& in_buffer,
        common::ObDataBuffer& out_buffer)
    {
      UNUSED(in_buffer);
      common::ObResultCode rc;
      const int32_t BACKUP_DATABASE_VERSION = 1;
      rc.result_code_ = OB_SUCCESS;
      int32_t mode = MIN_BACKUP;

      if (version != BACKUP_DATABASE_VERSION)
      {
        rc.result_code_ = OB_ERROR_FUNC_VERSION;
      }
      else if ( OB_SUCCESS != ( rc.result_code_ =
                                serialization::decode_vi32(in_buffer.get_data(),
              in_buffer.get_capacity(), in_buffer.get_position(),&mode)))
      {
        rc.result_code_ = OB_ERROR;
        TBSYS_LOG(WARN, "deserialize backup mode failed, ret:%d", rc.result_code_);
      }
      else if ( mode <=  MIN_BACKUP && mode >= MAX_BACKUP)
      {
        rc.result_code_ = OB_INVALID_ARGUMENT;
        TBSYS_LOG(WARN, "backup mode is invalid, ret:%d", rc.result_code_);
      }
      else if (OB_SUCCESS != ( rc.result_code_ = start_backup_task(mode)))
      {
        TBSYS_LOG(WARN, "start backup task failed, ret:%d", rc.result_code_);
      }
      //response to root server first
      int serialize_ret = rc.serialize(out_buffer.get_data(),
          out_buffer.get_capacity(), out_buffer.get_position());

      if (serialize_ret == OB_SUCCESS)
      {
        backup_server_->send_response(
              OB_BACKUP_DATABASE_RESPONSE,
              BACKUP_DATABASE_VERSION,
              out_buffer, req, channel_id);
      }
      else
      {
        TBSYS_LOG(WARN, "serialize failed, ret:%d", serialize_ret);
      }

      return rc.result_code_;
    }

    int ObTabletBackupService::backup_table(
        const int32_t version,
        const int32_t channel_id,
        easy_request_t* req,
        common::ObDataBuffer& in_buffer,
        common::ObDataBuffer& out_buffer)
    {
      UNUSED(in_buffer);
      common::ObResultCode rc;
      const int32_t BACKUP_TABLE_VERSION = 1;
      rc.result_code_ = OB_SUCCESS;
      int64_t len = 0;
      int32_t mode = MIN_BACKUP;
      char db_name[OB_MAX_DATBASE_NAME_LENGTH];
      char table_name[OB_MAX_TABLE_NAME_LENGTH];

      if (version != BACKUP_TABLE_VERSION)
      {
        rc.result_code_ = OB_ERROR_FUNC_VERSION;
      }
      else if ( OB_SUCCESS != ( rc.result_code_ =
                                serialization::decode_vi32(in_buffer.get_data(),
              in_buffer.get_capacity(), in_buffer.get_position(),&mode)))
      {
        rc.result_code_ = OB_ERROR;
        TBSYS_LOG(WARN, "deserialize backup mode failed, ret:%d", rc.result_code_);
      }
      else if ( NULL == serialization::decode_vstr(in_buffer.get_data(),
              in_buffer.get_capacity(), in_buffer.get_position(),db_name,OB_MAX_DATBASE_NAME_LENGTH,&len))
      {
        rc.result_code_ = OB_ERROR;
        TBSYS_LOG(WARN, "deserialize failed, ret:%d", rc.result_code_);
      }
      else if ( NULL == serialization::decode_vstr(in_buffer.get_data(),
              in_buffer.get_capacity(), in_buffer.get_position(),table_name,OB_MAX_TABLE_NAME_LENGTH,&len))
      {
        rc.result_code_ = OB_ERROR;
        TBSYS_LOG(WARN, "deserialize failed, ret:%d", rc.result_code_);
      }
      else if (OB_SUCCESS != ( rc.result_code_ = start_backup_task(mode,db_name,table_name)))
      {
        TBSYS_LOG(WARN, "db_name [%s], table_name [%s]", db_name, table_name);
        TBSYS_LOG(WARN, "start backup task failed, ret:%d", rc.result_code_);
      }
      //response to root server first
      int serialize_ret = rc.serialize(out_buffer.get_data(),
          out_buffer.get_capacity(), out_buffer.get_position());

      if (serialize_ret == OB_SUCCESS)
      {
        backup_server_->send_response(
              OB_BACKUP_TABLE_RESPONSE,
              BACKUP_TABLE_VERSION,
              out_buffer, req, channel_id);
      }
      else
      {
        TBSYS_LOG(WARN, "serialize failed, ret:%d", serialize_ret);
      }

      return rc.result_code_;
    }

    int ObTabletBackupService::backup_abort(
        const int32_t version,
        const int32_t channel_id,
        easy_request_t* req,
        common::ObDataBuffer& in_buffer,
        common::ObDataBuffer& out_buffer)
    {
      UNUSED(in_buffer);
      common::ObResultCode rc;
      const int32_t ABORT_BACKUP_VERSION = 1;
      rc.result_code_ = OB_SUCCESS;

      if (version != ABORT_BACKUP_VERSION)
      {
        rc.result_code_ = OB_ERROR_FUNC_VERSION;
      }
      else if (OB_SUCCESS != ( rc.result_code_ = abort_backup_task()))
      {
        TBSYS_LOG(WARN, "abort backup task failed, ret:%d", rc.result_code_);
      }

      //response to root server first
      int serialize_ret = rc.serialize(out_buffer.get_data(),
                                       out_buffer.get_capacity(), out_buffer.get_position());

      if (serialize_ret == OB_SUCCESS)
      {
        backup_server_->send_response(
              OB_BACKUP_ABORT_RESPONSE,
              ABORT_BACKUP_VERSION,
              out_buffer, req, channel_id);
      }
      else
      {
        TBSYS_LOG(WARN, "serialize failed, ret:%d", serialize_ret);
      }
      return rc.result_code_;

    }


    int ObTabletBackupService::backup_check_privilege(
        const int32_t version,
        const int32_t channel_id,
        easy_request_t* req,
        common::ObDataBuffer& in_buffer,
        common::ObDataBuffer& out_buffer)
    {
      common::ObResultCode rc;
      const int32_t CHECK_PRIVILEGE_VERSION = 1;
      rc.result_code_ = OB_SUCCESS;
      int64_t len = 0;
      bool result = false;

      char username[OB_MAX_USERNAME_LENGTH] = {'\0'};
      char passwd[OB_MAX_PASSWORD_LENGTH] = {'\0'};


      if (version != CHECK_PRIVILEGE_VERSION)
      {
        rc.result_code_ = OB_ERROR_FUNC_VERSION;
      }
      if ( NULL == serialization::decode_vstr(in_buffer.get_data(),
              in_buffer.get_capacity(), in_buffer.get_position(), username, OB_MAX_USERNAME_LENGTH, &len))
      {
        rc.result_code_ = OB_ERROR;
        TBSYS_LOG(WARN, "deserialize failed, ret:%d", rc.result_code_);
      }
      else if ( NULL == serialization::decode_vstr(in_buffer.get_data(),
              in_buffer.get_capacity(), in_buffer.get_position(),passwd, OB_MAX_PASSWORD_LENGTH, &len))
      {
        rc.result_code_ = OB_ERROR;
        TBSYS_LOG(WARN, "deserialize failed, ret:%d", rc.result_code_);
      }
      else if (OB_SUCCESS != ( rc.result_code_ = backup_server_->check_backup_privilege(username, passwd, result)))
      {
        TBSYS_LOG(WARN, "check backup privilege failed, ret:%d", rc.result_code_);
      }

      //response to root server first
      int serialize_ret = rc.serialize(out_buffer.get_data(),
                                       out_buffer.get_capacity(), out_buffer.get_position());


      serialize_ret = serialization::encode_bool(out_buffer.get_data(), out_buffer.get_capacity(), out_buffer.get_position(), result);

      if (serialize_ret == OB_SUCCESS)
      {
        backup_server_->send_response(
              OB_CHECK_BACKUP_PRIVILEGE_RESPONSE,
              CHECK_PRIVILEGE_VERSION,
              out_buffer, req, channel_id);
      }
      else
      {
        TBSYS_LOG(WARN, "serialize failed, ret:%d", serialize_ret);
      }
      return rc.result_code_;

    }

    int ObTabletBackupService::check_compress_lib(const char* compress_name_buf)
    {
      int rc = OB_SUCCESS;
      char temp_buf[OB_MAX_VARCHAR_LENGTH];
      ObCompressor* compressor = NULL;
      char* ptr = NULL;

      if (NULL == compress_name_buf)
      {
        TBSYS_LOG(ERROR, "compress name buf is NULL");
        rc = OB_INVALID_ARGUMENT;
      }
      else if (static_cast<int64_t>(strlen(compress_name_buf)) >= OB_MAX_VARCHAR_LENGTH)
      {
        TBSYS_LOG(ERROR, "compress name length >= OB_MAX_VARCHAR_LENGTH: compress name length=[%d]", static_cast<int>(strlen(compress_name_buf)));
        rc = OB_INVALID_ARGUMENT;
      }
      else
      {
        memcpy(temp_buf, compress_name_buf, strlen(compress_name_buf) + 1);
      }

      if (OB_SUCCESS == rc)
      {
        char* save_ptr = NULL;
        ptr = strtok_r(temp_buf, ":", &save_ptr);
        while (NULL != ptr)
        {
          if (NULL == (compressor = create_compressor(ptr)))
          {
            TBSYS_LOG(ERROR, "create compressor error: ptr=[%s]", ptr);
            rc = OB_ERROR;
            break;
          }
          else
          {
            destroy_compressor(compressor);
            TBSYS_LOG(INFO, "create compressor success: ptr=[%s]", ptr);
            ptr = strtok_r(NULL, ":", &save_ptr);
          }
        }
      }

      return rc;
    }
    int ObTabletBackupService::load_tablets()
    {
      int rc = OB_SUCCESS;
      if (!inited_)
      {
        rc = OB_NOT_INIT;
      }

      ObTabletManager & tablet_manager = backup_server_->get_tablet_manager();
      // load tablets;
      if (OB_SUCCESS == rc)
      {
        int32_t size = 0;
        const int32_t *disk_no_array = tablet_manager.get_disk_manager().get_disk_no_array(size);
        if (disk_no_array != NULL && size > 0)
        {
          rc = tablet_manager.load_tablets(disk_no_array, size);
        }
        else
        {
          rc = OB_ERROR;
          TBSYS_LOG(ERROR, "get disk no array failed.");
        }
      }

      return rc;
    }
  } // end namespace backupserver
} // end namespace oceanbase
