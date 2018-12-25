/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 *
 * Version: 0.1: ups_tablet_mgr.cpp,v 0.1 2010/09/14 10:11:10 chuanhui Exp $
 *
 * Authors:
 *   chuanhui <rizhao.ych@taobao.com>
 *     - some work details if you want
 *
 */
#include "common/ob_trace_log.h"
#include "common/ob_row_compaction.h"
#include "ob_update_server_main.h"
#include "ob_ups_table_mgr.h"
#include "ob_ups_utils.h"
//#include "ob_client_wrapper.h"
//#include "ob_client_wrapper_tsi.h"

namespace oceanbase
{
  namespace updateserver
  {
    using namespace oceanbase::common;
    int set_state_as_fatal()
    {
      int ret = OB_SUCCESS;
      ObUpdateServerMain *main = ObUpdateServerMain::get_instance();
      if (NULL == main)
      {
        TBSYS_LOG(ERROR, "get updateserver main null pointer");
        ret = OB_ERROR;
      }
      else
      {
        ObUpsRoleMgr& role_mgr = main->get_update_server().get_role_mgr();
        role_mgr.set_state(ObUpsRoleMgr::FATAL);
      }
      return ret;
    }

    ObUpsTableMgr :: ObUpsTableMgr(ObILogWriter &log_writer) : log_buffer_(NULL),
                                                               table_mgr_(log_writer),
                                                               check_checksum_(true),
                                                               has_started_(false),
                                                               last_bypass_checksum_(0)
                                                             ,tmp_schema_mutex_(0)//add zhaoqiong [Schema Manager] 20150327:b
    {
    }

    ObUpsTableMgr :: ~ObUpsTableMgr()
    {
      if (NULL != log_buffer_)
      {
        ob_free(log_buffer_);
        log_buffer_ = NULL;
      }
    }

    int ObUpsTableMgr :: init()
    {
      int err = OB_SUCCESS;

      err = table_mgr_.init();
      if (OB_SUCCESS != err)
      {
        TBSYS_LOG(WARN, "failed to init memtable list, err=%d", err);
      }
      else if (NULL == (log_buffer_ = (char*)ob_malloc(LOG_BUFFER_SIZE, ObModIds::OB_UPS_COMMON)))
      {
        TBSYS_LOG(WARN, "malloc log_buffer fail size=%ld", LOG_BUFFER_SIZE);
        err = OB_ERROR;
      }

      return err;
    }

    int ObUpsTableMgr::reg_table_mgr(SSTableMgr &sstable_mgr)
    {
      return sstable_mgr.reg_observer(&table_mgr_);
    }

    int ObUpsTableMgr :: get_active_memtable_version(uint64_t &version)
    {
      int ret = OB_SUCCESS;
      version = table_mgr_.get_active_version();
      if (OB_INVALID_ID == version
          || SSTableID::START_MAJOR_VERSION > version)
      {
        ret = OB_ERROR;
      }
      return ret;
    }


    //add zhaoqiong [Truncate Table]:20170704:b
    int ObUpsTableMgr :: get_last_truncate_timestamp_in_active(uint32_t uid, uint64_t table_id, int64_t &timestamp)
    {
      int ret = OB_SUCCESS;
      timestamp = INT64_MAX;
      TableItem *item = table_mgr_.get_active_memtable();
      if (item == NULL)
      {
        ret = OB_INNER_STAT_ERROR;
        TBSYS_LOG(ERROR, "active_memtable_ is null");
      }
      else
      {
          TEKey key;
          key.table_id = table_id;
          TEValue * table_value = NULL;
          if (OB_SUCCESS != (ret = item->get_memtable().query_cur_row_without_lock(key,table_value)))
          {
              TBSYS_LOG(WARN, "scan_one_table table_id=%ld,ret=%d",table_id,ret);
          }
          else if (table_value == NULL)
          {
              //do nothing
          }
          else if ((table_value != NULL) && OB_SUCCESS != (ret = table_value->row_lock.try_shared_lock(uid)))
          {
              TBSYS_LOG(ERROR, "get share lock failed, key.table_id=%ld", key.table_id);
              ret = OB_UD_PARALLAL_DATA_NOT_SAFE;
          }
          else
          {
              timestamp = table_value->list_tail->modify_time;
              ret = table_value->row_lock.shared_unlock(uid);
              TBSYS_LOG(DEBUG, "key.table_id=%ld, last_modify_time=%ld",table_id,timestamp);
          }
          table_mgr_.revert_active_memtable(item);
      }
      return ret;
    }
    //add:e

    int ObUpsTableMgr :: get_last_frozen_memtable_version(uint64_t &version)
    {
      int ret = OB_SUCCESS;
      uint64_t active_version = table_mgr_.get_active_version();
      if (OB_INVALID_ID == active_version
          || SSTableID::START_MAJOR_VERSION > active_version)
      {
        ret = OB_ERROR;
      }
      else
      {
        version = active_version - 1;
      }
      return ret;
    }

    int ObUpsTableMgr :: get_table_time_stamp(const uint64_t major_version, int64_t &time_stamp)
    {
      int ret = OB_SUCCESS;
      ret = table_mgr_.get_table_time_stamp(major_version, time_stamp);
      return ret;
    }

    int ObUpsTableMgr :: get_oldest_memtable_size(int64_t &size, uint64_t &major_version)
    {
      int ret = OB_SUCCESS;
      ret = table_mgr_.get_oldest_memtable_size(size, major_version);
      return ret;
    }

    int ObUpsTableMgr :: get_frozen_bloomfilter(const uint64_t version, TableBloomFilter &table_bf)
    {
      int ret = OB_SUCCESS;
      UNUSED(version);
      UNUSED(table_bf);
      TBSYS_LOG(WARN, "no get frozen bloomfilter impl now");
      return ret;
    }

    int ObUpsTableMgr :: start_transaction(const MemTableTransType type, UpsTableMgrTransHandle &handle)
    {
      int ret = OB_SUCCESS;
      TableItem *table_item = NULL;
      if (NULL == (table_item = table_mgr_.get_active_memtable()))
      {
        TBSYS_LOG(WARN, "failed to acquire active memtable");
        ret = OB_ERROR;
      }
      else
      {
        if (OB_SUCCESS == (ret = table_item->get_memtable().start_transaction(type, handle.trans_descriptor)))
        {
          handle.cur_memtable = table_item;
        }
        else
        {
          handle.cur_memtable = NULL;
          table_mgr_.revert_active_memtable(table_item);
        }
      }
      return ret;
    }

    int ObUpsTableMgr :: end_transaction(UpsTableMgrTransHandle &handle, bool rollback)
    {
      int ret = OB_SUCCESS;
      TableItem *table_item = NULL;
      if (NULL == (table_item = handle.cur_memtable))
      {
        TBSYS_LOG(WARN, "invalid param cur_memtable null pointer");
      }
      else
      {
        if (rollback)
        {
          ret = OB_ERROR;
        }
        if (OB_SUCCESS == ret)
        {
          ret = flush_commit_log_(TraceLog::get_logbuffer());
          FILL_TRACE_LOG("flush log ret=%d", ret);
        }
        if (OB_SUCCESS == ret)
        {
          ret = table_item->get_memtable().end_transaction(handle.trans_descriptor, rollback);
          FILL_TRACE_LOG("end transaction ret=%d", ret);
        }
        handle.cur_memtable = NULL;
        table_mgr_.revert_active_memtable(table_item);
      }

      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "flush log or end transaction fail ret=%d, enter FATAL state", ret);
        set_state_as_fatal();
        ret = OB_RESPONSE_TIME_OUT;
      }
      return ret;
    }

    void ObUpsTableMgr :: store_memtable(const bool all)
    {
      bool need2dump = false;
      do
      {
        need2dump = table_mgr_.try_dump_memtable();
      }
      while (all && need2dump);
    }

    bool ObUpsTableMgr :: need_auto_freeze() const
    {
      return table_mgr_.need_auto_freeze();
    }

    int ObUpsTableMgr :: freeze_memtable(const TableMgr::FreezeType freeze_type, uint64_t &frozen_version, bool &report_version_changed,
                                        const ObPacket *resp_packet)
    {
      int ret = OB_SUCCESS;
      uint64_t new_version = 0;
      uint64_t new_log_file_id = 0;
      int64_t freeze_time_stamp = 0;
      ThreadSpecificBuffer my_thread_buffer;
      CommonSchemaManagerWrapper schema_manager;
      ThreadSpecificBuffer::Buffer *my_buffer = my_thread_buffer_.get_buffer();
      if (NULL == my_buffer)
      {
        TBSYS_LOG(ERROR, "get thread specific buffer fail");
        ret = OB_ALLOCATE_MEMORY_FAILED;
      }
      else if (OB_SUCCESS != (ret = schema_mgr_.get_schema_mgr(schema_manager)))
      {
        TBSYS_LOG(WARN, "get schema mgr fail ret=%d", ret);
      }
      else if (schema_manager.get_version() <= CORE_SCHEMA_VERSION)
      {
        ret = OB_EAGAIN;
        TBSYS_LOG(ERROR, "try freeze but schema not ready, version=%ld", schema_manager.get_version());
      }
      else if (OB_SUCCESS == (ret = table_mgr_.try_freeze_memtable(freeze_type, new_version, frozen_version,
                                                              new_log_file_id, freeze_time_stamp, report_version_changed)))
      {
        ObUpdateServerMain *ups_main = ObUpdateServerMain::get_instance();
        ObUpsMutator ups_mutator;
        ObMutatorCellInfo mutator_cell_info;
        CurFreezeParam freeze_param;
        ObDataBuffer out_buff(my_buffer->current(), my_buffer->remain());

        freeze_param.param.active_version = new_version;
        freeze_param.param.frozen_version = frozen_version;
        freeze_param.param.new_log_file_id = new_log_file_id;
        freeze_param.param.time_stamp = freeze_time_stamp;
        freeze_param.param.op_flag = 0;
        if (TableMgr::MINOR_LOAD_BYPASS == freeze_type)
        {
          freeze_param.param.op_flag |= FLAG_MINOR_LOAD_BYPASS;
        }
        else if (TableMgr::MAJOR_LOAD_BYPASS == freeze_type)
        {
          freeze_param.param.op_flag |= FLAG_MAJOR_LOAD_BYPASS;
        }

        ups_mutator.set_freeze_memtable();
        if (NULL == ups_main)
        {
          TBSYS_LOG(ERROR, "get updateserver main null pointer");
          ret = OB_ERROR;
        }
        else if (OB_SUCCESS != (ret = freeze_param.serialize(out_buff.get_data(), out_buff.get_capacity(), out_buff.get_position())))
        {
          TBSYS_LOG(WARN, "serialize freeze_param fail ret=%d", ret);
        }
        else if (OB_SUCCESS != (ret = schema_mgr_.get_schema_mgr(schema_manager)))
        {
          TBSYS_LOG(WARN, "get schema mgr fail ret=%d", ret);
        }
        else if (OB_SUCCESS != (ret = schema_manager.serialize(out_buff.get_data(), out_buff.get_capacity(), out_buff.get_position())))
        {
          TBSYS_LOG(WARN, "serialize schema manager fail ret=%d", ret);
        }
        else
        {
          ObString str_freeze_param;
          str_freeze_param.assign_ptr(out_buff.get_data(), static_cast<int32_t>(out_buff.get_position()));
          mutator_cell_info.cell_info.value_.set_varchar(str_freeze_param);
          if (OB_SUCCESS != (ret = ups_mutator.get_mutator().add_cell(mutator_cell_info)))
          {
            TBSYS_LOG(WARN, "add cell to ups_mutator fail ret=%d", ret);
          }
          else
          {
            ret = flush_obj_to_log(OB_LOG_UPS_MUTATOR, ups_mutator);
          }
        }
        //add zhaoqiong [Schema Manager] 20150327:b
        /**
         * exp:if schema size bigger than log size threshold,
         * need write left schema to follow log
         * slave apply freeze log, use last log info,
         * need freeze_param
         */
        ObSchemaManagerV2* schema = const_cast<ObSchemaManagerV2*>(schema_manager.get_impl());
        while(OB_SUCCESS == ret && schema->need_next_serialize())
        {
          int64_t serialize_size = 0;
          if (OB_SUCCESS != (ret = schema->prepare_for_next_serialize()))
          {
            TBSYS_LOG(WARN, "prepare_for_next_serialize fail ret=%d", ret);
          }
          else if (OB_SUCCESS != (ret = freeze_param.serialize(log_buffer_, LOG_BUFFER_SIZE, serialize_size)))
          {
            TBSYS_LOG(WARN, "serialize freeze_param fail ret=%d", ret);
          }
          else if (OB_SUCCESS != (ret = schema->serialize(log_buffer_, LOG_BUFFER_SIZE, serialize_size)))
          {
            TBSYS_LOG(WARN, "ups_mutator serialilze fail log_buffer=%p log_buffer_size=%ld serialize_size=%ld ret=%d",
                      log_buffer_, LOG_BUFFER_SIZE, serialize_size, ret);
          }
          else
          {
            ObUpsLogMgr& log_mgr =  ObUpdateServerMain::get_instance()->get_update_server().get_log_mgr();
            TBSYS_LOG(INFO, "ups table mgr write schema next.");
            ret = log_mgr.write_and_flush_log(OB_UPS_WRITE_SCHEMA_NEXT, log_buffer_, serialize_size);
            if (OB_SUCCESS != ret)
            {
              TBSYS_LOG(WARN, "write log fail log_buffer_=%p serialize_size=%ld ret=%d", log_buffer_, serialize_size, ret);
            }
            else if (OB_SUCCESS != (ret = log_mgr.flush_log(TraceLog::get_logbuffer())))
            {
              TBSYS_LOG(WARN, "flush log fail ret=%d", ret);
            }
          }
        }
        //add:e
        TBSYS_LOG(INFO, "write freeze_op log ret=%d new_version=%lu frozen_version=%lu new_log_file_id=%lu op_flag=%d",
                  ret, new_version, frozen_version, new_log_file_id, 0);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "enter FATAL state.");
          set_state_as_fatal();
          ret = OB_RESPONSE_TIME_OUT;
        }
        else
        {
          if (TableMgr::MINOR_LOAD_BYPASS == freeze_type
              || TableMgr::MAJOR_LOAD_BYPASS == freeze_type)
          {
            submit_load_bypass(resp_packet);
          }
        }
      }
      return ret;
    }

    void ObUpsTableMgr :: drop_memtable(const bool force)
    {
      table_mgr_.try_drop_memtable(force);
    }

    void ObUpsTableMgr :: erase_sstable(const bool force)
    {
      table_mgr_.try_erase_sstable(force);
    }

    int ObUpsTableMgr :: handle_freeze_log_(ObUpsMutator &ups_mutator, const ReplayType replay_type)
    {
      int ret = OB_SUCCESS;

      ret = ups_mutator.get_mutator().next_cell();
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "next cell for freeze ups_mutator fail ret=%d", ret);
      }

      ObMutatorCellInfo *mutator_ci = NULL;
      ObCellInfo *ci = NULL;
      if (OB_SUCCESS == ret)
      {
        ret = ups_mutator.get_mutator().get_cell(&mutator_ci);
        if (OB_SUCCESS != ret
            || NULL == mutator_ci)
        {
          TBSYS_LOG(WARN, "get cell from freeze ups_mutator fail ret=%d", ret);
          ret = (OB_SUCCESS == ret) ? OB_ERROR : ret;
        }
        else
        {
          ci = &(mutator_ci->cell_info);
        }
      }

      ObString str_freeze_param;
      FreezeParamHeader *header = NULL;
      if (OB_SUCCESS == ret)
      {
        ret = ci->value_.get_varchar(str_freeze_param);
        header = (FreezeParamHeader*)str_freeze_param.ptr();
        if (OB_SUCCESS != ret
            || NULL == header
            || (int64_t)sizeof(FreezeParamHeader) >= str_freeze_param.length())
        {
          TBSYS_LOG(WARN, "get freeze_param from freeze ups_mutator fail ret=%d header=%p length=%d",
                    ret, header, str_freeze_param.length());
          ret = (OB_SUCCESS == ret) ? OB_ERROR : ret;
        }
      }

      if (OB_SUCCESS == ret)
      {
        static int64_t cur_version = 0;
        if (cur_version <= header->version)
        {
          cur_version = header->version;
        }
        else
        {
          TBSYS_LOG(ERROR, "there is a old clog version=%d follow a new clog version=%ld",
                    header->version, cur_version);
          ret = OB_ERROR;
        }
      }

      ObUpdateServerMain *ups_main = NULL;
      if (OB_SUCCESS == ret)
      {
        ups_main = ObUpdateServerMain::get_instance();
        if (NULL == ups_main)
        {
          TBSYS_LOG(WARN, "get ups main fail");
          ret = OB_ERROR;
        }
      }

      if (OB_SUCCESS == ret)
      {
        bool major_version_changed = false;
        bool is_schema_completion = false;//add zhaoqiong [Schema Manager] 20150327
        switch (header->version)
        {
          // 回放旧日志中的freeze操作
          case 1:
            {
              FreezeParamV1 *freeze_param_v1 = (FreezeParamV1*)(header->buf);
              uint64_t new_version = freeze_param_v1->active_version + 1;
              uint64_t new_log_file_id = freeze_param_v1->new_log_file_id;
              SSTableID new_sst_id;
              SSTableID frozen_sst_id;
              //new_sst_id.major_version = new_version;
              new_sst_id.id = 0;
              new_sst_id.id = (new_version << SSTableID::MINOR_VERSION_BIT);
              new_sst_id.minor_version_start = static_cast<uint16_t>(SSTableID::START_MINOR_VERSION);
              new_sst_id.minor_version_end = static_cast<uint16_t>(SSTableID::START_MINOR_VERSION);
              frozen_sst_id = new_sst_id;
              //frozen_sst_id.major_version -= 1;
              frozen_sst_id.id -= (1L << SSTableID::MINOR_VERSION_BIT);
              major_version_changed = true;
              ret = table_mgr_.replay_freeze_memtable(new_sst_id.id, frozen_sst_id.id, new_log_file_id);
              TBSYS_LOG(INFO, "replay freeze memtable using freeze_param_v1 active_version=%ld new_log_file_id=%ld op_flag=%lx ret=%d",
                        freeze_param_v1->active_version, freeze_param_v1->new_log_file_id, freeze_param_v1->op_flag, ret);
              break;
            }
          case 2:
            {
              FreezeParamV2 *freeze_param_v2 = (FreezeParamV2*)(header->buf);
              uint64_t new_version = SSTableID::trans_format_v1(freeze_param_v2->active_version);
              uint64_t frozen_version = SSTableID::trans_format_v1(freeze_param_v2->frozen_version);
              uint64_t new_log_file_id = freeze_param_v2->new_log_file_id;
              CommonSchemaManagerWrapper schema_manager;
              char *data = str_freeze_param.ptr();
              int64_t length = str_freeze_param.length();
              int64_t pos = sizeof(FreezeParamHeader) + sizeof(FreezeParamV2);
              if (OB_SUCCESS == (ret = schema_manager.deserialize(data, length, pos)))
              {
                if (OB_SCHEMA_VERSION_FOUR <= schema_manager.get_code_version())
                {
                  ret = schema_mgr_.set_schema_mgr(schema_manager);
                }
                else
                {
                  TBSYS_LOG(WARN, "schema version=%ld too old, will not use", schema_manager.get_version());
                }
              }
              if (OB_SUCCESS == ret)
              {
                ret = table_mgr_.replay_freeze_memtable(new_version, frozen_version, new_log_file_id);
                if (OB_SUCCESS == ret)
                {
                  SSTableID sst_id_new = new_version;
                  SSTableID sst_id_frozen = frozen_version;
                  if (sst_id_new.major_version != sst_id_frozen.major_version)
                  {
                    major_version_changed = true;
                  }
                }
              }
              TBSYS_LOG(INFO, "replay freeze memtable using freeze_param_v2 active_version=%ld after_trans=%ld "
                        "new_log_file_id=%ld op_flag=%lx ret=%d",
                        freeze_param_v2->active_version, SSTableID::trans_format_v1(freeze_param_v2->active_version),
                        freeze_param_v2->new_log_file_id, freeze_param_v2->op_flag, ret);
              break;
            }
          case 3:
            {
              FreezeParamV3 *freeze_param_v3 = (FreezeParamV3*)(header->buf);
              uint64_t new_version = SSTableID::trans_format_v1(freeze_param_v3->active_version);
              uint64_t frozen_version = SSTableID::trans_format_v1(freeze_param_v3->frozen_version);
              uint64_t new_log_file_id = freeze_param_v3->new_log_file_id;
              int64_t time_stamp = freeze_param_v3->time_stamp;
              CommonSchemaManagerWrapper schema_manager;
              char *data = str_freeze_param.ptr();
              int64_t length = str_freeze_param.length();
              int64_t pos = sizeof(FreezeParamHeader) + sizeof(FreezeParamV3);
              if (OB_SUCCESS == (ret = schema_manager.deserialize(data, length, pos)))
              {
                if (OB_SCHEMA_VERSION_FOUR <= schema_manager.get_code_version())
                {
                  ret = schema_mgr_.set_schema_mgr(schema_manager);
                }
                else
                {
                  TBSYS_LOG(WARN, "schema version=%ld too old, will not use", schema_manager.get_version());
                }
              }
              if (OB_SUCCESS == ret)
              {
                ret = table_mgr_.replay_freeze_memtable(new_version, frozen_version, new_log_file_id, time_stamp);
                if (OB_SUCCESS == ret)
                {
                  SSTableID sst_id_new = new_version;
                  SSTableID sst_id_frozen = frozen_version;
                  if (sst_id_new.major_version != sst_id_frozen.major_version)
                  {
                    major_version_changed = true;
                  }
                }
              }
              TBSYS_LOG(INFO, "replay freeze memtable using freeze_param_v3 active_version=%ld after_trans=%ld "
                        "new_log_file_id=%ld op_flag=%lx ret=%d",
                        freeze_param_v3->active_version, SSTableID::trans_format_v1(freeze_param_v3->active_version),
                        freeze_param_v3->new_log_file_id, freeze_param_v3->op_flag, ret);
              break;
            }
          case 4:
            {
              FreezeParamV4 *freeze_param_v4 = (FreezeParamV4*)(header->buf);
              uint64_t new_version = freeze_param_v4->active_version;
              uint64_t frozen_version = freeze_param_v4->frozen_version;
              uint64_t new_log_file_id = freeze_param_v4->new_log_file_id;
              int64_t time_stamp = freeze_param_v4->time_stamp;
              CommonSchemaManagerWrapper schema_manager;
              char *data = str_freeze_param.ptr();
              int64_t length = str_freeze_param.length();
              int64_t pos = sizeof(FreezeParamHeader) + sizeof(FreezeParamV4);
              if (OB_SUCCESS == (ret = schema_manager.deserialize(data, length, pos)))
              {
                if (OB_SCHEMA_VERSION_FOUR <= schema_manager.get_code_version())
                {
                  //mod zhaoqiong [Schema Manager] 20150327:b
                  //ret = schema_mgr_.set_schema_mgr(schema_manager);
                  //exp:if schema is not comlete, save it to tem schema
                  if (schema_manager.get_impl()->is_completion())
                  {
                    ret = schema_mgr_.set_schema_mgr(schema_manager);
                  }
                  else
                  {
                    ObSpinLockGuard guard(tmp_schema_lock_);
                    tmp_schema_mgr_ = *(schema_manager.get_impl());
                  }
                  //mod:e
                }
                else
                {
                  TBSYS_LOG(WARN, "schema version=%ld too old, will not use", schema_manager.get_code_version());
                }
              }
              //mod zhaoqiong [Schema Manager] 20150327:b
              /**
              * exp:if schema no completion, do nothing
              * when receive last schema log, do replay_freeze_memtable
              */
              //if (OB_SUCCESS == ret)
              if (OB_SUCCESS == ret && schema_manager.get_impl()->is_completion())
              //mod:e
              {
                is_schema_completion = true;//add zhaoqiong [Schema Manager] 20150327
                if (RT_LOCAL == replay_type
                    && (freeze_param_v4->op_flag & FLAG_MINOR_LOAD_BYPASS
                        || freeze_param_v4->op_flag & FLAG_MAJOR_LOAD_BYPASS))
                {
                  int minor_num_limit = (freeze_param_v4->op_flag & FLAG_MINOR_LOAD_BYPASS)? SSTableID::MAX_MINOR_VERSION: 0;
                  ret = table_mgr_.sstable_scan_finished(minor_num_limit);
                }
                else
                {
                  ret = table_mgr_.replay_freeze_memtable(new_version, frozen_version, new_log_file_id, time_stamp);
                }
                if (OB_SUCCESS == ret)
                {
                  SSTableID sst_id_new = new_version;
                  SSTableID sst_id_frozen = frozen_version;
                  if (sst_id_new.major_version != sst_id_frozen.major_version)
                  {
                    major_version_changed = true;
                  }

                  if (RT_APPLY == replay_type
                      && (freeze_param_v4->op_flag & FLAG_MINOR_LOAD_BYPASS
                          || freeze_param_v4->op_flag & FLAG_MAJOR_LOAD_BYPASS))
                  {
                    int64_t loaded_num = 0;
                    int tmp_ret = load_sstable_bypass(ups_main->get_update_server().get_sstable_mgr(), loaded_num);
                    TBSYS_LOG(INFO, "replay load bypass ret=%d loader_num=%ld", tmp_ret, loaded_num);
                  }
                }
              }
              TBSYS_LOG(INFO, "replay freeze memtable using freeze_param_v4 active_version=%ld new_log_file_id=%ld op_flag=%lx ret=%d",
                        freeze_param_v4->active_version, freeze_param_v4->new_log_file_id, freeze_param_v4->op_flag, ret);
              break;
            }
          default:
            ret = OB_ERROR;
            TBSYS_LOG(WARN, "freeze_param version error %d", header->version);
            break;
        }
        //mod zhaoqiong [Schema Manager] 20150327:b
        //exp:if schema no completion, do nothing
        //if (OB_SUCCESS == ret)
        if (OB_SUCCESS == ret && is_schema_completion)
        //mod:e
        {
          if (major_version_changed)
          {
            ups_main->get_update_server().submit_report_freeze();
          }
          ups_main->get_update_server().submit_handle_frozen();
        }
      }
      ups_mutator.get_mutator().reset_iter();
      log_table_info();
      return ret;
    }

    int ObUpsTableMgr :: replay(ObUpsMutator &ups_mutator, const ReplayType replay_type)
    {
      int ret = OB_SUCCESS;
      if (ups_mutator.is_freeze_memtable())
      {
        has_started_ = true;
        ret = handle_freeze_log_(ups_mutator, replay_type);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "failed to handle freeze log, ret=%d", ret);
        }
      }
      else if (ups_mutator.is_drop_memtable())
      {
        TBSYS_LOG(INFO, "ignore drop commit log");
      }
      else if (ups_mutator.is_first_start())
      {
        has_started_ = true;
        ObUpdateServerMain *main = ObUpdateServerMain::get_instance();
        if (NULL == main)
        {
          TBSYS_LOG(ERROR, "get updateserver main null pointer");
        }
        else
        {
          table_mgr_.sstable_scan_finished(main->get_update_server().get_param().minor_num_limit);
        }
        TBSYS_LOG(INFO, "handle first start flag log");
      }
      else if (ups_mutator.is_check_cur_version())
      {
        if (table_mgr_.get_cur_major_version() != ups_mutator.get_cur_major_version())
        {
          TBSYS_LOG(ERROR, "cur major version not match, table_major=%lu mutator_major=%lu table_minor=%lu mutator_minor=%lu",
                    table_mgr_.get_cur_major_version(), ups_mutator.get_cur_major_version(),
                    table_mgr_.get_cur_minor_version(), ups_mutator.get_cur_minor_version());
          ret = OB_ERROR;
        }
        else if (table_mgr_.get_cur_minor_version() != ups_mutator.get_cur_minor_version())
        {
          TBSYS_LOG(ERROR, "cur major version not match, table_major=%lu mutator_major=%lu table_minor=%lu mutator_minor=%lu",
                    table_mgr_.get_cur_major_version(), ups_mutator.get_cur_major_version(),
                    table_mgr_.get_cur_minor_version(), ups_mutator.get_cur_minor_version());
          ret = OB_ERROR;
        }
        else if (check_checksum_
                && 0 != last_bypass_checksum_
                && ups_mutator.get_last_bypass_checksum() != last_bypass_checksum_)
        {
          TBSYS_LOG(ERROR, "last bypass checksum not match, local_checksum=%lu mutator_checksum=%lu",
                    last_bypass_checksum_, ups_mutator.get_last_bypass_checksum());
          ret = OB_ERROR;
        }
        else
        {
          TBSYS_LOG(INFO, "check cur version succ, cur_major_version=%lu cur_minor_version=%lu last_bypass_checksum=%lu",
                    table_mgr_.get_cur_major_version(), table_mgr_.get_cur_minor_version(), last_bypass_checksum_);
        }
      }
      else
      {
        ret = set_mutator_(ups_mutator);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "failed to set mutator, ret=%d", ret);
        }
      }
      return ret;
    }

    int ObUpsTableMgr :: replay_mgt_mutator(ObUpsMutator &ups_mutator, const ReplayType replay_type)
    {
      int ret = OB_SUCCESS;
      if (ups_mutator.is_freeze_memtable())
      {
        has_started_ = true;
        ret = handle_freeze_log_(ups_mutator, replay_type);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "failed to handle freeze log, ret=%d", ret);
        }
      }
      else if (ups_mutator.is_drop_memtable())
      {
        TBSYS_LOG(INFO, "ignore drop commit log");
      }
      else if (ups_mutator.is_first_start())
      {
        has_started_ = true;
        ObUpdateServerMain *main = ObUpdateServerMain::get_instance();
        if (NULL == main)
        {
          TBSYS_LOG(ERROR, "get updateserver main null pointer");
        }
        else
        {
          table_mgr_.sstable_scan_finished(main->get_update_server().get_param().minor_num_limit);
        }
        TBSYS_LOG(INFO, "handle first start flag log");
      }
      else if (ups_mutator.is_check_cur_version())
      {
        if (table_mgr_.get_cur_major_version() != ups_mutator.get_cur_major_version())
        {
          TBSYS_LOG(ERROR, "cur major version not match, table_major=%lu mutator_major=%lu table_minor=%lu mutator_minor=%lu",
                    table_mgr_.get_cur_major_version(), ups_mutator.get_cur_major_version(),
                    table_mgr_.get_cur_minor_version(), ups_mutator.get_cur_minor_version());
          ret = OB_ERROR;
        }
        else if (table_mgr_.get_cur_minor_version() != ups_mutator.get_cur_minor_version())
        {
          TBSYS_LOG(ERROR, "cur major version not match, table_major=%lu mutator_major=%lu table_minor=%lu mutator_minor=%lu",
                    table_mgr_.get_cur_major_version(), ups_mutator.get_cur_major_version(),
                    table_mgr_.get_cur_minor_version(), ups_mutator.get_cur_minor_version());
          ret = OB_ERROR;
        }
        else if (check_checksum_
                && 0 != last_bypass_checksum_
                && ups_mutator.get_last_bypass_checksum() != last_bypass_checksum_)
        {
          TBSYS_LOG(ERROR, "last bypass checksum not match, local_checksum=%lu mutator_checksum=%lu",
                    last_bypass_checksum_, ups_mutator.get_last_bypass_checksum());
          ret = OB_ERROR;
        }
        else
        {
          TBSYS_LOG(INFO, "check cur version succ, cur_major_version=%lu cur_minor_version=%lu last_bypass_checksum=%lu",
                    table_mgr_.get_cur_major_version(), table_mgr_.get_cur_minor_version(), last_bypass_checksum_);
        }
      }
      else if (check_checksum_
              && ups_mutator.is_check_sstable_checksum())
      {
        SSTableID sst_id = SSTableID::get_id(ups_mutator.get_cur_major_version(),
                                            ups_mutator.get_cur_minor_version(),
                                            ups_mutator.get_cur_minor_version());
        ret = table_mgr_.check_sstable_checksum(sst_id.id, ups_mutator.get_sstable_checksum());
      }
      else
      {}
      return ret;
    }

    template <typename T>
    int ObUpsTableMgr :: flush_obj_to_log(const LogCommand log_command, T &obj)
    {
      int ret = OB_SUCCESS;
      ObUpdateServerMain *ups_main = ObUpdateServerMain::get_instance();
      if (NULL == ups_main)
      {
        TBSYS_LOG(ERROR, "get updateserver main null pointer");
        ret = OB_ERROR;
      }
      else
      {
        ObUpsLogMgr& log_mgr = ups_main->get_update_server().get_log_mgr();
        int64_t serialize_size = 0;
        if (NULL == log_buffer_)
        {
          TBSYS_LOG(WARN, "log buffer malloc fail");
          ret = OB_ERROR;
        }
        else if (OB_SUCCESS != (ret = ups_serialize(obj, log_buffer_, LOG_BUFFER_SIZE, serialize_size)))
        {
          TBSYS_LOG(WARN, "obj serialilze fail log_buffer=%p log_buffer_size=%ld serialize_size=%ld ret=%d",
                    log_buffer_, LOG_BUFFER_SIZE, serialize_size, ret);
        }
        else
        {
          if (OB_SUCCESS != (ret = log_mgr.write_log(log_command, log_buffer_, serialize_size)))
          {
            TBSYS_LOG(WARN, "write log fail log_command=%d log_buffer_=%p serialize_size=%ld ret=%d",
                      log_command, log_buffer_, serialize_size, ret);
          }
          else if (OB_SUCCESS != (ret = log_mgr.flush_log(TraceLog::get_logbuffer())))
          {
            TBSYS_LOG(WARN, "flush log fail ret=%d", ret);
          }
          else
          {
            // do nothing
          }
          if (OB_SUCCESS != ret)
          {
            TBSYS_LOG(WARN, "write log fail ret=%d, enter FATAL stat", ret);
            set_state_as_fatal();
            ret = OB_RESPONSE_TIME_OUT;
          }
        }
      }
      return ret;
    }

    int ObUpsTableMgr :: write_start_log()
    {
      int ret = OB_SUCCESS;
      if (has_started_)
      {
        TBSYS_LOG(INFO, "system has started need not write start flag");
      }
      else
      {
        ObUpsMutator ups_mutator;
        ups_mutator.set_first_start();
        ret = flush_obj_to_log(OB_LOG_UPS_MUTATOR, ups_mutator);
        TBSYS_LOG(INFO, "write first start flag ret=%d", ret);
      }
      return ret;
    }

    int ObUpsTableMgr :: set_mutator_(ObUpsMutator& mutator)
    {
      int ret = OB_SUCCESS;
      TableItem *table_item = NULL;
      uint64_t trans_descriptor = 0;
      if (NULL == (table_item = table_mgr_.get_active_memtable()))
      {
        TBSYS_LOG(WARN, "failed to acquire active memtable");
        ret = OB_ERROR;
      }
      else if (OB_SUCCESS != (ret = table_item->get_memtable().start_transaction(WRITE_TRANSACTION,
              trans_descriptor, mutator.get_mutate_timestamp())))
      {
        TBSYS_LOG(WARN, "start transaction fail ret=%d", ret);
      }
      else
      {
        //FILL_TRACE_LOG("start replay one mutator");
        if (OB_SUCCESS != (ret = table_item->get_memtable().start_mutation(trans_descriptor)))
        {
          TBSYS_LOG(WARN, "start mutation fail ret=%d", ret);
        }
        else
        {
          if (OB_SUCCESS != (ret = table_item->get_memtable().set(trans_descriptor, mutator, check_checksum_)))
          {
            TBSYS_LOG(WARN, "set to memtable fail ret=%d", ret);
          }
          else
          {
            TBSYS_LOG(DEBUG, "replay mutator succ");
          }
          table_item->get_memtable().end_mutation(trans_descriptor, OB_SUCCESS != ret);
        }
        table_item->get_memtable().end_transaction(trans_descriptor, OB_SUCCESS != ret);
        //FILL_TRACE_LOG("ret=%d", ret);
        //PRINT_TRACE_LOG();
      }
      if (NULL != table_item)
      {
        table_mgr_.revert_active_memtable(table_item);
      }
      return ret;
    }

    int ObUpsTableMgr :: apply(const bool using_id, UpsTableMgrTransHandle &handle, ObUpsMutator &ups_mutator, ObScanner *scanner)
    {
      int ret = OB_SUCCESS;
      TableItem *table_item = NULL;
      if (NULL == (table_item = table_mgr_.get_active_memtable()))
      {
        TBSYS_LOG(WARN, "failed to acquire active memtable");
        ret = OB_ERROR;
      }
      else
      {
        MemTable *p_active_memtable = &(table_item->get_memtable());
        if (!using_id
            && OB_SUCCESS != trans_name2id_(ups_mutator.get_mutator()))
        {
          TBSYS_LOG(WARN, "cellinfo do not pass valid check or trans name to id fail");
          ret = OB_SCHEMA_ERROR;
        }
        else if (OB_SUCCESS != p_active_memtable->start_mutation(handle.trans_descriptor))
        {
          TBSYS_LOG(WARN, "start mutation fail trans_descriptor=%lu", handle.trans_descriptor);
          ret = OB_ERROR;
        }
        else
        {
          bool check_checksum = false;
          scanner->reset();
          scanner->set_id_name_type(using_id ? ObScanner::ID : ObScanner::NAME);
          FILL_TRACE_LOG("prepare mutator");
          if (OB_SUCCESS != (ret = p_active_memtable->set(handle.trans_descriptor, ups_mutator, check_checksum, this, scanner)))
          {
            TBSYS_LOG(WARN, "set to memtable fail ret=%d", ret);
          }
          else
          {
            log_scanner(scanner);
            FILL_TRACE_LOG("scanner info %s", print_scanner_info(scanner));
            // 注意可能返回OB_EAGAIN
            ret = fill_commit_log_(ups_mutator, TraceLog::get_logbuffer());
          }
          if (OB_SUCCESS == ret)
          {
            bool rollback = false;
            ret = p_active_memtable->end_mutation(handle.trans_descriptor, rollback);
          }
          else
          {
            bool rollback = true;
            p_active_memtable->end_mutation(handle.trans_descriptor, rollback);
          }
        }
        table_mgr_.revert_active_memtable(table_item);
      }
      log_memtable_memory_info();
      return ret;
    }

    int ObUpsTableMgr :: apply(RWSessionCtx &session_ctx,
                              ObIterator &iter,
                              const ObDmlType dml_type)
    {
      int ret = OB_SUCCESS;
      TableItem *table_item = NULL;
      if (NULL == (table_item = table_mgr_.get_active_memtable()))
      {
        TBSYS_LOG(WARN, "failed to acquire active memtable");
        ret = OB_ERROR;
      }
      else
      {
        MemTable *p_active_memtable = &(table_item->get_memtable());
        session_ctx.get_uc_info().host = p_active_memtable;
        if (OB_SUCCESS != (ret = p_active_memtable->set(session_ctx, iter, dml_type))
            && !IS_SQL_ERR(ret))
        {
          TBSYS_LOG(WARN, "set to memtable fail ret=%d", ret);
        }
        FILL_TRACE_BUF(session_ctx.get_tlog_buffer(), "set to memtable ret=%d", ret);
        table_mgr_.revert_active_memtable(table_item);
      }
      log_memtable_memory_info();
      return ret;
    }

    int ObUpsTableMgr :: apply(const bool using_id,
                              RWSessionCtx &session_ctx,
                              ILockInfo &lock_info,
                              ObMutator &mutator)
    {
      int ret = OB_SUCCESS;
      TableItem *table_item = NULL;
      if (NULL == (table_item = table_mgr_.get_active_memtable()))
      {
        TBSYS_LOG(WARN, "failed to acquire active memtable");
        ret = OB_ERROR;
      }
      else
      {
        MemTable *p_active_memtable = &(table_item->get_memtable());
        if (!using_id
            && OB_SUCCESS != trans_name2id_(mutator))
        {
          TBSYS_LOG(WARN, "cellinfo do not pass valid check or trans name to id fail");
          ret = OB_SCHEMA_ERROR;
        }
        else
        {
          session_ctx.get_uc_info().host = p_active_memtable;
          if (OB_SUCCESS != (ret = p_active_memtable->set(session_ctx, lock_info, mutator)))
          {
            TBSYS_LOG(WARN, "set to memtable fail ret=%d", ret);
          }
          FILL_TRACE_BUF(session_ctx.get_tlog_buffer(), "set to memtable ret=%d", ret);
          //int64_t ts = tbsys::CTimeUtil::getTime();
          //char fname[1024];
          //snprintf(fname, 1024, "./%ld", ts);
          //TBSYS_LOG(INFO, "wait for file %s", fname);
          //struct stat st;
          //if (0 == stat("./start.wait", &st))
          //{
          //  while (true)
          //  {
          //    if (0 == stat(fname, &st))
          //    {
          //      break;
          //    }
          //  }
          //}
        }
        table_mgr_.revert_active_memtable(table_item);
      }
      log_memtable_memory_info();
      return ret;
    }

    int ObUpsTableMgr :: add_memtable_uncommited_checksum(uint64_t *checksum)
    {
      int err = OB_SUCCESS;
      TableItem *table_item = NULL;
      if (NULL == checksum)
      {
        TBSYS_LOG(WARN, "invalid param, checksum null pointer");
        err = OB_INVALID_ARGUMENT;
      }
      else if (NULL == (table_item = table_mgr_.get_active_memtable()))
      {
        TBSYS_LOG(WARN, "failed to acquire active memtable");
        err = OB_ERROR;
      }
      else
      {
        *checksum = table_item->get_memtable().calc_uncommited_checksum(*checksum);
        table_item->get_memtable().update_uncommited_checksum(*checksum);
        table_mgr_.revert_active_memtable(table_item);
      }
      return err;
    }

    int ObUpsTableMgr :: check_checksum(const uint64_t checksum2check,
                                        const uint64_t checksum_before_mutate,
                                        const uint64_t checksum_after_mutate)
    {
      int err = OB_SUCCESS;
      TableItem *table_item = NULL;
      if (!check_checksum_)
      {}
      else if (NULL == (table_item = table_mgr_.get_active_memtable()))
      {
        TBSYS_LOG(WARN, "failed to acquire active memtable");
        err = OB_ERROR;
      }
      else
      {
        err = table_item->get_memtable().check_checksum(checksum2check, checksum_before_mutate, checksum_after_mutate);
        table_mgr_.revert_active_memtable(table_item);
      }
      return err;
    }

    void ObUpsTableMgr :: log_memtable_memory_info()
    {
      static volatile uint64_t counter = 0;
      static const int64_t mod = 400000;
      static int64_t last_report_ts = 0;
      if (1 == (ATOMIC_ADD(&counter, 1) % mod))
      {
        int64_t cur_ts = tbsys::CTimeUtil::getTime();
        TBSYS_LOG(INFO, "DML total=%lu, TPS=%ld", counter, 1000000 * mod/(cur_ts - last_report_ts));
        last_report_ts = cur_ts;
        log_table_info();
      }
    }

    void ObUpsTableMgr :: get_memtable_memory_info(TableMemInfo &mem_info)
    {
      TableItem *table_item = table_mgr_.get_active_memtable();
      if (NULL != table_item)
      {
        mem_info.memtable_used = table_item->get_memtable().used() + table_mgr_.get_frozen_memused();
        mem_info.memtable_total = table_item->get_memtable().total() + table_mgr_.get_frozen_memtotal();
        MemTableAttr memtable_attr;
        table_item->get_memtable().get_attr(memtable_attr);
        mem_info.memtable_limit = memtable_attr.total_memlimit;
        table_mgr_.revert_active_memtable(table_item);
      }
    }

    void ObUpsTableMgr::set_memtable_attr(const MemTableAttr &memtable_attr)
    {
      table_mgr_.set_memtable_attr(memtable_attr);
    }

    int ObUpsTableMgr::get_memtable_attr(MemTableAttr &memtable_attr)
    {
      return table_mgr_.get_memtable_attr(memtable_attr);
    }

    void ObUpsTableMgr :: update_memtable_stat_info()
    {
      TableItem *table_item = table_mgr_.get_active_memtable();
      if (NULL != table_item)
      {
        MemTableAttr memtable_attr;
        table_mgr_.get_memtable_attr(memtable_attr);
        int64_t active_limit = memtable_attr.total_memlimit;
        int64_t frozen_limit = memtable_attr.total_memlimit;
        int64_t active_total = table_item->get_memtable().total();
        int64_t frozen_total = table_mgr_.get_frozen_memtotal();
        int64_t active_used = table_item->get_memtable().used();
        int64_t frozen_used = table_mgr_.get_frozen_memused();
        int64_t active_row_count = table_item->get_memtable().size();
        int64_t frozen_row_count = table_mgr_.get_frozen_rowcount();

        OB_STAT_SET(UPDATESERVER, UPS_STAT_MEMTABLE_TOTAL, active_total + frozen_total);
        OB_STAT_SET(UPDATESERVER, UPS_STAT_MEMTABLE_USED, active_used + frozen_used);
        OB_STAT_SET(UPDATESERVER, UPS_STAT_TOTAL_LINE, active_row_count + frozen_row_count);

        OB_STAT_SET(UPDATESERVER, UPS_STAT_ACTIVE_MEMTABLE_LIMIT, active_limit);
        OB_STAT_SET(UPDATESERVER, UPS_STAT_ACTICE_MEMTABLE_TOTAL, active_total);
        OB_STAT_SET(UPDATESERVER, UPS_STAT_ACTIVE_MEMTABLE_USED, active_used);
        OB_STAT_SET(UPDATESERVER, UPS_STAT_ACTIVE_TOTAL_LINE, active_row_count);

        OB_STAT_SET(UPDATESERVER, UPS_STAT_FROZEN_MEMTABLE_LIMIT, frozen_limit);
        OB_STAT_SET(UPDATESERVER, UPS_STAT_FROZEN_MEMTABLE_TOTAL, frozen_total);
        OB_STAT_SET(UPDATESERVER, UPS_STAT_FROZEN_MEMTABLE_USED, frozen_used);
        OB_STAT_SET(UPDATESERVER, UPS_STAT_FROZEN_TOTAL_LINE, frozen_row_count);

        table_mgr_.revert_active_memtable(table_item);
      }
    }

    int ObUpsTableMgr :: set_schemas(const CommonSchemaManagerWrapper &schema_manager)
    {
      int ret = OB_SUCCESS;
      //mod zhaoqiong [Schema Manager] 20150327:b
      //ret = schema_mgr_.set_schema_mgr(schema_manager);
      //exp:if schema is not complete, save it to temp schema
      if (!schema_manager.get_impl()->is_completion())
      {
        ObSpinLockGuard guard(tmp_schema_lock_);

        tmp_schema_mgr_ = *(schema_manager.get_impl());
        TBSYS_LOG(INFO, "schema is not completion");

      }
      else
      {
      //mod:e
        ObSpinLockGuard guard(schema_lock_);

        if (schema_manager.get_version() < schema_mgr_.get_version())
        {
          TBSYS_LOG(WARN, "set_schema(new_version=%ld, cur_ups_version=%ld): NO need to switch",
                    schema_manager.get_version(), schema_mgr_.get_version());
        }
        else if (OB_SUCCESS != (ret = schema_mgr_.set_schema_mgr(schema_manager)))
        {
          TBSYS_LOG(ERROR, "set_schemas error, ret=%d schema_version=%ld", ret, schema_manager.get_version());
        }
        else
        {
          TBSYS_LOG(INFO, "set_schemas(version[%ld->%ld])", schema_mgr_.get_version(), schema_manager.get_version());
          TBSYS_LOG(INFO, "==========print schema start==========");
          schema_manager.print_info();
          TBSYS_LOG(INFO, "==========print schema end==========");
        }
      }

      return ret;
    }

    //add zhaoqiong [Schema Manager] 20150327:b

    int ObUpsTableMgr :: replay_mgt_next(const char* buf, const int64_t buf_len, int64_t& pos, const ReplayType replay_type)
    {
      int ret = OB_SUCCESS;

      CurFreezeParam freeze_param;
      ObSpinLockGuard guard(tmp_schema_lock_);
      if (OB_SUCCESS != (ret = freeze_param.deserialize(buf, buf_len, pos)))
      {
        TBSYS_LOG(ERROR, "freeze_param deserialize fail, ret=%d", ret);
      }
      else if (OB_SUCCESS != (ret = tmp_schema_mgr_.deserialize(buf, buf_len, pos)))
      {
        TBSYS_LOG(ERROR, "schema_manager deserialize fail, ret=%d", ret);
      }
      else if (tmp_schema_mgr_.get_version() < schema_mgr_.get_version())
      {
        ret = OB_VERSION_NOT_MATCH;
        TBSYS_LOG(ERROR, "should not be here.set_schema(new_version=%ld, cur_ups_version=%ld): NO need to switch",
                  tmp_schema_mgr_.get_version(), schema_mgr_.get_version());
      }
      else if (tmp_schema_mgr_.get_impl()->is_completion())
      {
        ObSpinLockGuard guard(schema_lock_);
        ret = schema_mgr_.set_schema_mgr(tmp_schema_mgr_);
      }

      bool major_version_changed = false;
      ObUpdateServerMain *ups_main = NULL;
      if (OB_SUCCESS == ret && tmp_schema_mgr_.get_impl()->is_completion())
      {
        ups_main = ObUpdateServerMain::get_instance();
        if (NULL == ups_main)
        {
          TBSYS_LOG(WARN, "get ups main fail");
          ret = OB_ERROR;
        }
      }
      /**
      * exp:if schema is completion, do replay_freeze_memtable
      * else do nothing
      */
      if (OB_SUCCESS == ret && tmp_schema_mgr_.get_impl()->is_completion())
      {
        uint64_t new_version = freeze_param.param.active_version;
        uint64_t frozen_version = freeze_param.param.frozen_version;
        uint64_t new_log_file_id = freeze_param.param.new_log_file_id;
        int64_t time_stamp = freeze_param.param.time_stamp;

        if (RT_LOCAL == replay_type
            && (freeze_param.param.op_flag & FLAG_MINOR_LOAD_BYPASS
                || freeze_param.param.op_flag & FLAG_MAJOR_LOAD_BYPASS))
        {
          int minor_num_limit = (freeze_param.param.op_flag & FLAG_MINOR_LOAD_BYPASS)? SSTableID::MAX_MINOR_VERSION: 0;
          ret = table_mgr_.sstable_scan_finished(minor_num_limit);
        }
        else
        {
          ret = table_mgr_.replay_freeze_memtable(new_version, frozen_version, new_log_file_id, time_stamp);
        }
        if (OB_SUCCESS == ret)
        {
          SSTableID sst_id_new = new_version;
          SSTableID sst_id_frozen = frozen_version;
          if (sst_id_new.major_version != sst_id_frozen.major_version)
          {
            major_version_changed = true;
          }

          if (RT_APPLY == replay_type
              && (freeze_param.param.op_flag & FLAG_MINOR_LOAD_BYPASS
                  || freeze_param.param.op_flag & FLAG_MAJOR_LOAD_BYPASS))
          {
            int64_t loaded_num = 0;
            int tmp_ret = load_sstable_bypass(ups_main->get_update_server().get_sstable_mgr(), loaded_num);
            TBSYS_LOG(INFO, "replay load bypass ret=%d loader_num=%ld", tmp_ret, loaded_num);
          }
        }
      }

      if (OB_SUCCESS == ret && tmp_schema_mgr_.get_impl()->is_completion())
      {
        if (major_version_changed)
        {
          ups_main->get_update_server().submit_report_freeze();
        }
        ups_main->get_update_server().submit_handle_frozen();
      }

      return ret;
    }
	
    int ObUpsTableMgr::set_schema_next(const char* buf, const int64_t data_len, int64_t& pos)
    {
      int ret = OB_SUCCESS;
      //use tmp schema first
      ObSpinLockGuard guard(tmp_schema_lock_);
      if (OB_SUCCESS != (ret = tmp_schema_mgr_.deserialize(buf, data_len, pos)))
      {
        TBSYS_LOG(ERROR, "schema_manager deserialize error, ret=%d", ret);
      }
      else if (!tmp_schema_mgr_.get_impl()->is_completion())
      {
        TBSYS_LOG(INFO, "schema is not completion");
      }
      else
      {
        //assign complete tmp_schema_mgr to schema_mgr
        ObSpinLockGuard guard(schema_lock_);
        if (tmp_schema_mgr_.get_version() <= schema_mgr_.get_version())
        {
          ret = OB_VERSION_NOT_MATCH;
          TBSYS_LOG(ERROR, "set_schema(new_version=%ld, cur_ups_version=%ld): NO need to switch",
                    tmp_schema_mgr_.get_version(), schema_mgr_.get_version());
        }
        else if (OB_SUCCESS != (ret = schema_mgr_.set_schema_mgr(tmp_schema_mgr_)))
        {
          TBSYS_LOG(ERROR, "set_schemas error, ret=%d schema_version=%ld", ret, tmp_schema_mgr_.get_version());
        }
        else
        {
          TBSYS_LOG(INFO, "set_schemas(version[%ld->%ld])", schema_mgr_.get_version(), tmp_schema_mgr_.get_version());
          TBSYS_LOG(INFO, "==========print schema start==========");
          tmp_schema_mgr_.print_info();
          TBSYS_LOG(INFO, "==========print schema end==========");
        }
      }
      return ret;
    }

    int ObUpsTableMgr:: switch_schema_mutator(const ObSchemaMutator &schema_mutator)
    {
      int ret = OB_SUCCESS;
      ObSpinLockGuard guard(schema_lock_);

      if (schema_mutator.get_refresh_schema())
      {
        ret = OB_SUCCESS;
        TBSYS_LOG(WARN, "mutator contain refresh schema, skip this mutator");
      }
      else if (schema_mutator.get_end_version() <= schema_mutator.get_start_version())
      {
        ret = OB_INVALID_ARGUMENT;
        TBSYS_LOG(WARN, "schema mutator error, not a valid mutator, start_version:%ld, end_version:%ld",
                  schema_mutator.get_start_version(), schema_mutator.get_end_version());
      }
      else if (schema_mutator.get_end_version() <= schema_mgr_.get_version())
      {
        TBSYS_LOG(WARN, "set_schema(new_version=%ld, cur_ups_version=%ld): NO need to switch",
                  schema_mutator.get_end_version(), schema_mgr_.get_version());
      }
      else if (OB_SUCCESS != (ret = schema_mgr_.apply_schema_mutator(schema_mutator)))
      {
        TBSYS_LOG(ERROR, "set_schemas error, ret=%d schema_version=%ld", ret, schema_mutator.get_end_version());
      }
      else if (OB_SUCCESS != (ret = write_schema_mutator(schema_mutator)))
      {
        TBSYS_LOG(ERROR, "write_schema_mutator(version=%ld)=>%d", schema_mutator.get_end_version(), ret);
      }
      else
      {
        TBSYS_LOG(INFO, "set_schema mutator(version[%ld->%ld])", schema_mutator.get_start_version(), schema_mutator.get_end_version());
      }
      return ret;
    }

    int ObUpsTableMgr::write_schema_mutator(const ObSchemaMutator &schema_mutator)
    {
      int ret = OB_SUCCESS;
      ObUpdateServerMain *main = ObUpdateServerMain::get_instance();
      int64_t serialize_size = 0;
      if (NULL == main)
      {
        TBSYS_LOG(ERROR, "get updateserver main null pointer");
        ret = OB_ERROR;
      }
      else if (NULL == log_buffer_)
      {
        TBSYS_LOG(WARN, "log buffer malloc fail");
        ret = OB_ERROR;
      }
      else if (OB_SUCCESS != (ret = schema_mutator.serialize(log_buffer_, LOG_BUFFER_SIZE, serialize_size)))
      {
        TBSYS_LOG(WARN, "ups_mutator serialilze fail log_buffer=%p log_buffer_size=%ld serialize_size=%ld ret=%d",
                  log_buffer_, LOG_BUFFER_SIZE, serialize_size, ret);
      }
      else
      {
        ObUpsLogMgr& log_mgr = main->get_update_server().get_log_mgr();
        TBSYS_LOG(INFO, "ups table mgr switch schema mutator.");
        ret = log_mgr.write_and_flush_log(OB_UPS_SWITCH_SCHEMA_MUTATOR, log_buffer_, serialize_size);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "write log fail log_buffer_=%p serialize_size=%ld ret=%d", log_buffer_, serialize_size, ret);
        }
      }

      return ret;
    }
    int ObUpsTableMgr::switch_tmp_schemas()
    {
      int ret = OB_SUCCESS;
      //ObSpinLockGuard guard(tmp_schema_lock_);
      if (lock_tmp_schema())
      {
        ret = OB_EAGAIN;
        TBSYS_LOG(ERROR, "unexpected error, schema should be locked");
      }
      else if (!tmp_schema_mgr_.get_impl()->is_completion())
      {
        ret = OB_INNER_STAT_ERROR;
        TBSYS_LOG(WARN, "tmp schema mgr is not completion");
      }
      else
      {
        TBSYS_LOG(INFO, "switch schemas");
        ret = switch_schemas(tmp_schema_mgr_);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR, "set_schemas failed, ret=%d", ret);
        }
      }

      if (!unlock_tmp_schema())
      {
        TBSYS_LOG(ERROR, "unexpected error");
      }
      return ret;
    }

    //add:e
	
    int ObUpsTableMgr:: write_schema(const CommonSchemaManagerWrapper &schema_manager)
    {
      int ret = OB_SUCCESS;
      ObUpdateServerMain *main = ObUpdateServerMain::get_instance();
      int64_t serialize_size = 0;
      if (NULL == main)
      {
        TBSYS_LOG(ERROR, "get updateserver main null pointer");
        ret = OB_ERROR;
      }
      else
      {
        if (NULL == log_buffer_)
        {
          TBSYS_LOG(WARN, "log buffer malloc fail");
          ret = OB_ERROR;
        }
      }

      if (OB_SUCCESS == ret)
      {
        ret = schema_manager.serialize(log_buffer_, LOG_BUFFER_SIZE, serialize_size);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "ups_mutator serialilze fail log_buffer=%p log_buffer_size=%ld serialize_size=%ld ret=%d",
                    log_buffer_, LOG_BUFFER_SIZE, serialize_size, ret);
        }
        else
        {
          ObUpsLogMgr& log_mgr = main->get_update_server().get_log_mgr();
          TBSYS_LOG(INFO, "ups table mgr switch schemas.");
          ret = log_mgr.write_and_flush_log(OB_UPS_SWITCH_SCHEMA, log_buffer_, serialize_size);
          if (OB_SUCCESS != ret)
          {
            TBSYS_LOG(WARN, "write log fail log_buffer_=%p serialize_size=%ld ret=%d", log_buffer_, serialize_size, ret);
          }
        }
      }
      //add zhaoqiong [Schema Manager] 20150327:b
      //write followed part of schema to log
      ObSchemaManagerV2* schema = const_cast<ObSchemaManagerV2*>(schema_manager.get_impl());
      while(OB_SUCCESS == ret && schema->need_next_serialize())
      {
        serialize_size = 0;
        if (OB_SUCCESS != (ret = schema->prepare_for_next_serialize()))
        {
          TBSYS_LOG(WARN, "prepare_for_next_serialize fail ret=%d", ret);
        }
        else if (OB_SUCCESS != (ret = schema->serialize(log_buffer_, LOG_BUFFER_SIZE, serialize_size)))
        {
          TBSYS_LOG(WARN, "ups_mutator serialilze fail log_buffer=%p log_buffer_size=%ld serialize_size=%ld ret=%d",
                    log_buffer_, LOG_BUFFER_SIZE, serialize_size, ret);
        }
        else
        {
          ObUpsLogMgr& log_mgr = main->get_update_server().get_log_mgr();
          TBSYS_LOG(INFO, "ups table mgr write switch schemas next log.");
          ret = log_mgr.write_and_flush_log(OB_UPS_SWITCH_SCHEMA_NEXT, log_buffer_, serialize_size);
          if (OB_SUCCESS != ret)
          {
            TBSYS_LOG(WARN, "write log for next schema part fail log_buffer_=%p serialize_size=%ld ret=%d", log_buffer_, serialize_size, ret);
          }
        }
      }
      //add:e

      return ret;
    }

    int ObUpsTableMgr :: switch_schemas(const CommonSchemaManagerWrapper &schema_manager)
    {
      int ret = OB_SUCCESS;
      ObSpinLockGuard guard(schema_lock_);
      //mod zhaoqiong [Schema Manager] 20150327:b
      //if (schema_manager.get_version() < schema_mgr_.get_version())
      if (schema_manager.get_version() <= schema_mgr_.get_version())
      //mod:e
      {
        TBSYS_LOG(WARN, "switch_schema(new_version=%ld, cur_ups_version=%ld): NO need to switch",
                  schema_manager.get_version(), schema_mgr_.get_version());
      }
      else if (OB_SUCCESS != (ret = schema_mgr_.set_schema_mgr(schema_manager)))
      {
        TBSYS_LOG(ERROR, "set_schemas error, ret=%d", ret);
      }
      else if (OB_SUCCESS != (ret = write_schema(schema_manager)))
      {
        TBSYS_LOG(ERROR, "write_schema(version=%ld)=>%d", schema_manager.get_version(), ret);
      }
      else
      {
        TBSYS_LOG(INFO, "switch_schemas(version[%ld->%ld])", schema_mgr_.get_version(), schema_manager.get_version());
        TBSYS_LOG(INFO, "==========print schema start==========");
        schema_manager.print_info();
        TBSYS_LOG(INFO, "==========print schema end==========");
      }

      return ret;
    }

    int ObUpsTableMgr :: create_index()
    {
      int ret = OB_SUCCESS;
      // need not create index
      TBSYS_LOG(WARN, "no create index impl now");
      return ret;
    }

    template <class T>
    int ObUpsTableMgr :: get_(const BaseSessionCtx &session_ctx,
                              const ObGetParam &get_param,
                              T &scanner,
                              const int64_t start_time,
                              const int64_t timeout,
                              const sql::ObLockFlag lock_flag/* = sql::LF_NONE*/)
    {
      int ret = OB_SUCCESS;
      TableList *table_list = GET_TSI_MULT(TableList, TSI_UPS_TABLE_LIST_1);
      uint64_t max_valid_version = 0;
      bool is_final_minor = false;
      if (NULL == table_list)
      {
        TBSYS_LOG(WARN, "get tsi table_list fail");
        ret = OB_ERROR;
      }
      else if (OB_SUCCESS != (ret = table_mgr_.acquire_table(get_param.get_version_range(), max_valid_version, *table_list, is_final_minor, get_table_id(get_param)))
          || 0 == table_list->size())
      {
        TBSYS_LOG(WARN, "acquire table fail version_range=%s", range2str(get_param.get_version_range()));
        ret = (OB_SUCCESS == ret) ? OB_INVALID_START_VERSION : ret;
      }
      else
      {
        FILL_TRACE_BUF(session_ctx.get_tlog_buffer(), "version=%s table_num=%ld", range2str(get_param.get_version_range()), table_list->size());
        TableList::iterator iter;
        int64_t index = 0;
        SSTableID sst_id;
        for (iter = table_list->begin(); iter != table_list->end(); iter++, index++)
        {
          ITableEntity *table_entity = *iter;
          ITableUtils *table_utils = NULL;
          if (NULL == table_entity)
          {
            TBSYS_LOG(WARN, "invalid table_entity version_range=%s", range2str(get_param.get_version_range()));
            ret = OB_ERROR;
            break;
          }
          if (NULL == (table_utils = table_entity->get_tsi_tableutils(index)))
          {
            TBSYS_LOG(WARN, "get tsi tableutils fail index=%ld", index);
            ret = OB_TOO_MANY_SSTABLE;
            break;
          }
          sst_id = (NULL == table_entity) ? 0 : table_entity->get_table_item().get_sstable_id();
          FILL_TRACE_BUF(session_ctx.get_tlog_buffer(), "get table %s ret=%d", sst_id.log_str(), ret);
        }

        if (OB_SUCCESS == ret)
        {
          ret = get_(session_ctx, *table_list, get_param, scanner, start_time,
                     timeout, lock_flag);
          if (OB_SUCCESS != ret)
          {
            TBSYS_LOG(WARN, "failed to get_ ret=%d", ret);
          }
          else
          {
            scanner.set_data_version(ObVersion::get_version(SSTableID::get_major_version(max_valid_version),
                                                            SSTableID::get_minor_version_end(max_valid_version),
                                                            is_final_minor));
          }
        }

        for (iter = table_list->begin(), index = 0; iter != table_list->end(); iter++, index++)
        {
          ITableEntity *table_entity = *iter;
          ITableUtils *table_utils = NULL;
          if (NULL != table_entity
              && NULL != (table_utils = table_entity->get_tsi_tableutils(index)))
          {
            table_utils->reset();
          }
        }
        table_mgr_.revert_table(*table_list);
      }
      return ret;
    }

    template <class T>
    int ObUpsTableMgr :: scan_(const BaseSessionCtx &session_ctx,
                              const ObScanParam &scan_param,
                              T &scanner,
                              const int64_t start_time,
                              const int64_t timeout)
    {
      int ret = OB_SUCCESS;
      TableList *table_list = GET_TSI_MULT(TableList, TSI_UPS_TABLE_LIST_1);
      uint64_t max_valid_version = 0;
      const ObNewRange *scan_range = NULL;
      uint64_t table_id = OB_INVALID_ID;//add zhaoqiong [Truncate Table]:20170519
      bool is_final_minor = false;
      if (NULL == table_list)
      {
        TBSYS_LOG(WARN, "get tsi table_list fail");
        ret = OB_ERROR;
      }
      //add zhaoqiong [Truncate Table]:20170519
      else if (OB_INVALID_ID == (table_id = scan_param.get_table_id()))
      {
        TBSYS_LOG(WARN, "invalid scan table_id");
        ret = OB_ERROR;
      }
      //add:e
      else if (NULL == (scan_range = scan_param.get_range()))
      {
        TBSYS_LOG(WARN, "invalid scan range");
        ret = OB_ERROR;
      }
      else if (OB_SUCCESS != (ret = table_mgr_.acquire_table(scan_param.get_version_range(), max_valid_version, *table_list, is_final_minor, get_table_id(scan_param)))
              || 0 == table_list->size())
      {
        TBSYS_LOG(WARN, "acquire table fail version_range=%s", range2str(scan_param.get_version_range()));
        ret = (OB_SUCCESS == ret) ? OB_INVALID_START_VERSION : ret;
      }
      else
      {
        ColumnFilter cf;
        ColumnFilter *pcf = ColumnFilter::build_columnfilter(scan_param, &cf);
        FILL_TRACE_BUF(session_ctx.get_tlog_buffer(), "%s columns=%s direction=%d version=%s table_num=%ld",
                      scan_range2str(*scan_range),
                      (NULL == pcf) ? "nil" : pcf->log_str(),
                      scan_param.get_scan_direction(),
                      range2str(scan_param.get_version_range()),
                      table_list->size());

        do
        {
          ITableEntity::Guard guard(table_mgr_.get_resource_pool());
          ObMerger merger;
          ObIterator *ret_iter = &merger;
          merger.set_asc(scan_param.get_scan_direction() == ScanFlag::FORWARD);

          TableList::iterator iter;
          int64_t index = 0;
          SSTableID sst_id;
          ITableIterator *prev_table_iter = NULL;
          bool truncate_status = false; /*add zhaoqiong [Truncate Table]:20170519*/
          for (iter = table_list->begin(); iter != table_list->end(); iter++, index++)
          {
            ITableEntity *table_entity = *iter;
            ITableUtils *table_utils = NULL;
            ITableIterator *table_iter = NULL;
            truncate_status = false; /*add zhaoqiong [Truncate Table]:20170519*/
            if (NULL == table_entity)
            {
              TBSYS_LOG(WARN, "invalid table_entity version_range=%s", range2str(scan_param.get_version_range()));
              ret = OB_ERROR;
              break;
            }
            if (NULL == (table_utils = table_entity->get_tsi_tableutils(index)))
            {
              TBSYS_LOG(WARN, "get tsi tableutils fail index=%ld", index);
              ret = OB_TOO_MANY_SSTABLE;
              break;
            }
            if (ITableEntity::SSTABLE == table_entity->get_table_type())
            {
              table_iter = prev_table_iter;
            }
            if (NULL == table_iter
                && NULL == (table_iter = table_entity->alloc_iterator(table_mgr_.get_resource_pool(), guard)))
            {
              TBSYS_LOG(WARN, "alloc table iterator fai index=%ld", index);
              ret = OB_MEM_OVERFLOW;
              break;
            }
            prev_table_iter = NULL;
            if (OB_SUCCESS != (ret = table_entity->scan(session_ctx, scan_param, table_iter)))
            {
              TBSYS_LOG(WARN, "table entity scan fail ret=%d scan_range=%s", ret, scan_range2str(*scan_range));
              break;
            }
            sst_id = (NULL == table_entity) ? 0 : table_entity->get_table_item().get_sstable_id();
            FILL_TRACE_BUF(session_ctx.get_tlog_buffer(), "scan table type=%d %s ret=%d iter=%p", table_entity->get_table_type(), sst_id.log_str(), ret, table_iter);
            if (ITableEntity::SSTABLE == table_entity->get_table_type())
            {
              if (OB_ITER_END == table_iter->next_cell())
              {
                table_iter->reset();
                prev_table_iter = table_iter;
                continue;
              }
            }
            if (1 == table_list->size())
            {
              ret_iter = table_iter;
              break;
            }
            //add zhaoqiong [Truncate Table]:20170519
            if (OB_SUCCESS != (ret = table_entity->get_table_truncate_stat(table_id,truncate_status)))
            {
                TBSYS_LOG(WARN, "failed to get_table_truncate_stat=%d", ret);
                break;
            }
            //add:e
            else if (OB_SUCCESS != (ret = merger.add_iterator(table_iter,truncate_status)))
            {
              TBSYS_LOG(WARN, "add iterator to merger fail ret=%d", ret);
              break;
            }
          }

          if (OB_SUCCESS == ret)
          {
            int64_t row_count = 0;
            ret = add_to_scanner_(*ret_iter, scanner, row_count, start_time, timeout, scan_param.get_scan_size());
            FILL_TRACE_BUF(session_ctx.get_tlog_buffer(), "add to scanner scanner_size=%ld row_count=%ld ret=%d", scanner.get_size(), row_count, ret);
            if (OB_SIZE_OVERFLOW == ret)
            {
              if (row_count > 0)
              {
                scanner.set_is_req_fullfilled(false, row_count);
                ret = OB_SUCCESS;
              }
              else
              {
                TBSYS_LOG(WARN, "memory is not enough to add even one row");
                ret = OB_ERROR;
              }
            }
            else if (OB_SUCCESS != ret)
            {
              TBSYS_LOG(WARN, "failed to add data from ups_merger to scanner, ret=%d", ret);
            }
            else
            {
              scanner.set_is_req_fullfilled(true, row_count);
            }
            if (OB_SUCCESS == ret)
            {
              scanner.set_data_version(ObVersion::get_version(SSTableID::get_major_version(max_valid_version),
                                                              SSTableID::get_minor_version_end(max_valid_version),
                                                              is_final_minor));

              ObNewRange range;
              range.table_id_ = scan_param.get_table_id();
              range.set_whole_range();
              ret = scanner.set_range(range);
            }
          }

          for (iter = table_list->begin(), index = 0; iter != table_list->end(); iter++, index++)
          {
            ITableEntity *table_entity = *iter;
            ITableUtils *table_utils = NULL;
            if (NULL != table_entity
                && NULL != (table_utils = table_entity->get_tsi_tableutils(index)))
            {
              table_utils->reset();
            }
          }
        }
        while (false);
        table_mgr_.revert_table(*table_list);
      }
      return ret;
    }

    template <class T>
    int ObUpsTableMgr :: get_(const BaseSessionCtx &session_ctx,
                              TableList &table_list,
                              const ObGetParam& get_param,
                              T& scanner,
                              const int64_t start_time,
                              const int64_t timeout,
                              const sql::ObLockFlag lock_flag)
    {
      int err = OB_SUCCESS;

      int64_t cell_size = get_param.get_cell_size();
      const ObCellInfo* cell = NULL;

      if (cell_size <= 0)
      {
        TBSYS_LOG(WARN, "invalid param, cell_size=%ld", cell_size);
        err = OB_INVALID_ARGUMENT;
      }
      else if (NULL == (cell = get_param[0]))
      {
        TBSYS_LOG(WARN, "invalid param first cellinfo");
        err = OB_ERROR;
      }
      else
      {
        int64_t last_cell_idx = 0;
        ObRowkey last_row_key = cell->row_key_;
        uint64_t last_table_id = cell->table_id_;
        int64_t cell_idx = 1;

        while (OB_SUCCESS == err && cell_idx < cell_size)
        {
          if (NULL == (cell = get_param[cell_idx]))
          {
            TBSYS_LOG(WARN, "cellinfo null pointer idx=%ld", cell_idx);
            err = OB_ERROR;
          }
          else if (cell->row_key_ != last_row_key ||
                  cell->table_id_ != last_table_id)
          {
            err = get_row_(session_ctx, table_list, last_cell_idx, cell_idx - 1, get_param, scanner, start_time, timeout, lock_flag);
            if (OB_SIZE_OVERFLOW == err)
            {
              TBSYS_LOG(WARN, "allocate memory failed, first_idx=%ld, last_idx=%ld",
                  last_cell_idx, cell_idx - 1);
            }
            else if (OB_SUCCESS != err)
            {
              TBSYS_LOG(WARN, "failed to get_row_, first_idx=%ld, last_idx=%ld, err=%d",
                  last_cell_idx, cell_idx - 1, err);
            }
            else
            {
              if ((start_time + timeout) < g_cur_time)
              {
                if (last_cell_idx > 0)
                {
                  // at least get one row
                  TBSYS_LOG(WARN, "get or scan too long time, start_time=%ld timeout=%ld timeu=%ld row_count=%ld",
                      start_time, timeout, tbsys::CTimeUtil::getTime() - start_time, last_cell_idx);
                  err = OB_FORCE_TIME_OUT;
                }
                else
                {
                  TBSYS_LOG(ERROR, "can't get any row, start_time=%ld timeout=%ld timeu=%ld",
                      start_time, timeout, tbsys::CTimeUtil::getTime() - start_time);
                  err = OB_RESPONSE_TIME_OUT;
                }
              }

              if (OB_SUCCESS == err)
              {
                last_cell_idx = cell_idx;
                last_row_key = cell->row_key_;
                last_table_id = cell->table_id_;
              }
            }
          }

          if (OB_SUCCESS == err)
          {
            ++cell_idx;
          }
        }

        if (OB_SUCCESS == err)
        {
          err = get_row_(session_ctx, table_list, last_cell_idx, cell_idx - 1, get_param, scanner, start_time, timeout, lock_flag);
          if (OB_SIZE_OVERFLOW == err)
          {
            TBSYS_LOG(WARN, "allocate memory failed, first_idx=%ld, last_idx=%ld",
                last_cell_idx, cell_idx - 1);
          }
          else if (OB_SUCCESS != err)
          {
            TBSYS_LOG(WARN, "failed to get_row_, first_idx=%ld, last_idx=%ld, err=%d",
                last_cell_idx, cell_idx - 1, err);
          }
        }

        FILL_TRACE_BUF(session_ctx.get_tlog_buffer(), "table_list_size=%ld get_param_cell_size=%ld get_param_row_size=%ld last_cell_idx=%ld cell_idx=%ld scanner_size=%ld ret=%d",
                      table_list.size(), get_param.get_cell_size(), get_param.get_row_size(), last_cell_idx, cell_idx, scanner.get_size(), err);
        if (OB_SIZE_OVERFLOW == err)
        {
          // wrap error
          scanner.set_is_req_fullfilled(false, last_cell_idx);
          err = OB_SUCCESS;
        }
        else if (OB_FORCE_TIME_OUT == err)
        {
          scanner.set_is_req_fullfilled(false, cell_idx);
          err = OB_SUCCESS;
        }
        else if (OB_SUCCESS == err)
        {
          scanner.set_is_req_fullfilled(true, cell_idx);
        }
      }

      return err;
    }

    template <class T>
    int ObUpsTableMgr :: get_row_(const BaseSessionCtx &session_ctx,
                                  TableList &table_list,
                                  const int64_t first_cell_idx,
                                  const int64_t last_cell_idx,
                                  const ObGetParam& get_param,
                                  T& scanner,
                                  const int64_t start_time,
                                  const int64_t timeout,
                                  const sql::ObLockFlag lock_flag)
    {
      int ret = OB_SUCCESS;
      int64_t cell_size = get_param.get_cell_size();
      const ObCellInfo* cell = NULL;
      ColumnFilter *column_filter = NULL;

      if (first_cell_idx > last_cell_idx)
      {
        TBSYS_LOG(WARN, "invalid param, first_cell_idx=%ld, last_cell_idx=%ld",
            first_cell_idx, last_cell_idx);
        ret = OB_INVALID_ARGUMENT;
      }
      else if (cell_size <= last_cell_idx)
      {
        TBSYS_LOG(WARN, "invalid status, cell_size=%ld, last_cell_idx=%ld",
            cell_size, last_cell_idx);
        ret = OB_ERROR;
      }
      else if (NULL == (cell = get_param[first_cell_idx]))
      {
        TBSYS_LOG(WARN, "cellinfo null pointer idx=%ld", first_cell_idx);
        ret = OB_ERROR;
      }
      else if (NULL == (column_filter = ITableEntity::get_tsi_columnfilter()))
      {
        TBSYS_LOG(WARN, "get tsi columnfilter fail");
        ret = OB_ERROR;
      }
      else
      {
        ITableEntity::Guard guard(table_mgr_.get_resource_pool());
        ObMerger merger;
        ObIterator *ret_iter = &merger;

        uint64_t table_id = cell->table_id_;
        ObRowkey row_key = cell->row_key_;
        column_filter->clear();
        for (int64_t i = first_cell_idx; i <= last_cell_idx; ++i)
        {
          if (NULL != get_param[i]) // double check
          {
            column_filter->add_column(get_param[i]->column_id_);
          }
        }
        //add duyr [Delete_Update_Function_isolation] [JHOBv0.1] 20160531:b
        column_filter->set_data_mark_param(get_param.get_data_mark_param());
        //add duyr 20160531:e

        TableList::iterator iter;
        int64_t index = 0;
        bool truncate_status = false; /*add zhaoqiong [Truncate Table]:20170519*/
        ITableIterator *prev_table_iter = NULL;
        for (iter = table_list.begin(), index = 0; iter != table_list.end(); iter++, index++)
        {
          ITableEntity *table_entity = *iter;
          ITableUtils *table_utils = NULL;
          ITableIterator *table_iter = NULL;
          truncate_status = false; /*add zhaoqiong [Truncate Table]:20170519*/
          if (NULL == table_entity)
          {
            TBSYS_LOG(WARN, "invalid table_entity version_range=%s", range2str(get_param.get_version_range()));
            ret = OB_ERROR;
            break;
          }
          if (NULL == (table_utils = table_entity->get_tsi_tableutils(index)))
          {
            TBSYS_LOG(WARN, "get tsi tableutils fail index=%ld", index);
            ret = OB_ERROR;
            break;
          }
          if (ITableEntity::SSTABLE == table_entity->get_table_type())
          {
            table_iter = prev_table_iter;
          }
          if (NULL == table_iter
              && NULL == (table_iter = table_entity->alloc_iterator(table_mgr_.get_resource_pool(), guard)))
          {
            TBSYS_LOG(WARN, "alloc table iterator fai index=%ld", index);
            ret = OB_MEM_OVERFLOW;
            break;
          }
          prev_table_iter = NULL;
          if (OB_SUCCESS != (ret = table_entity->get(session_ctx, table_id, row_key, column_filter, lock_flag, table_iter)))
          {
            TBSYS_LOG(WARN, "table entity get fail ret=%d table_id=%lu row_key=[%s] columns=[%s]",
                      ret, table_id, to_cstring(row_key), column_filter->log_str());
            break;
          }
          TBSYS_LOG(DEBUG, "get row row_key=[%s] row_key_ptr=%p columns=[%s] iter=%p",
                    to_cstring(row_key), row_key.ptr(), column_filter->log_str(), table_iter);
          if (ITableEntity::SSTABLE == table_entity->get_table_type())
          {
            if (OB_ITER_END == table_iter->next_cell())
            {
              table_iter->reset();
              prev_table_iter = table_iter;
              continue;
            }
          }
          if (1 == table_list.size())
          {
            ret_iter = table_iter;
            break;
          }
          //add zhaoqiong [Truncate Table]:20170519
          if (OB_SUCCESS != (ret = table_entity->get_table_truncate_stat(table_id,truncate_status)))
          {
              TBSYS_LOG(WARN, "failed to get_table_truncate_stat=%d", ret);
              break;
          }
          //add:e
          else if (OB_SUCCESS != (ret = merger.add_iterator(table_iter,truncate_status)))
          {
            TBSYS_LOG(WARN, "add iterator to merger fail ret=%d", ret);
            break;
          }
        }

        if (OB_SUCCESS == ret)
        {
          int64_t row_count = 0;
          ret = add_to_scanner_(*ret_iter, scanner, row_count, start_time, timeout);
          if (OB_SIZE_OVERFLOW == ret)
          {
            // return ret
          }
          else if (OB_SUCCESS != ret)
          {
            TBSYS_LOG(WARN, "failed to add data from merger to scanner, ret=%d", ret);
          }
          else
          {
            // TODO (rizhao) add successful cell num to scanner
          }
        }
      }

      TBSYS_LOG(DEBUG, "[op=GET_ROW] [first_cell_idx=%ld] [last_cell_idx=%ld] [ret=%d]", first_cell_idx, last_cell_idx, ret);
      return ret;
    }

    int ObUpsTableMgr :: clear_active_memtable()
    {
      return table_mgr_.clear_active_memtable();
    }

    int ObUpsTableMgr::check_permission_(ObMutator &mutator, const IToken &token)
    {
      int ret = OB_SUCCESS;
      CellInfoProcessor ci_proc;
      ObUpdateServerMain *main = ObUpdateServerMain::get_instance();
      if (NULL == main)
      {
        TBSYS_LOG(ERROR, "get updateserver main null pointer");
        ret = OB_ERROR;
      }
      while (OB_SUCCESS == ret
            && OB_SUCCESS == (ret = mutator.next_cell()))
      {
        ObMutatorCellInfo *mutator_ci = NULL;
        ObCellInfo *ci = NULL;
        if (OB_SUCCESS == mutator.get_cell(&mutator_ci)
            && NULL != mutator_ci)
        {
          const CommonTableSchema *table_schema = NULL;
          UpsSchemaMgr::SchemaHandle schema_handle;
          ci = &(mutator_ci->cell_info);
          if (!ci_proc.analyse_syntax(*mutator_ci))
          {
            ret = OB_SCHEMA_ERROR;
          }
          else if (ci_proc.need_skip())
          {
            continue;
          }
          else if (OB_SUCCESS != (ret = schema_mgr_.get_schema_handle(schema_handle)))
          {
            TBSYS_LOG(WARN, "get_schema_handle fail ret=%d", ret);
          }
          else
          {
            if (NULL == (table_schema = schema_mgr_.get_table_schema(schema_handle, ci->table_name_)))
            {
              TBSYS_LOG(WARN, "get schema fail table_name=%.*s table_name_length=%d",
                        ci->table_name_.length(), ci->table_name_.ptr(), ci->table_name_.length());
              ret = OB_SCHEMA_ERROR;
            }
            else if (OB_SUCCESS != (ret = main->get_update_server().get_perm_table().check_table_writeable(token, table_schema->get_table_id())))
            {
              ObString username;
              token.get_username(username);
              TBSYS_LOG(WARN, "check write permission fail ret=%d table_id=%lu table_name=%.*s user_name=%.*s",
                        ret, table_schema->get_table_id(), ci->table_name_.length(), ci->table_name_.ptr(),
                        username.length(), username.ptr());
            }
            else
            {
              // pass check
            }
            schema_mgr_.revert_schema_handle(schema_handle);
          }
        }
        else
        {
          ret = OB_ERROR;
        }
      }
      ret = (OB_ITER_END == ret) ? OB_SUCCESS : ret;
      mutator.reset_iter();
      return ret;
    }

    int ObUpsTableMgr::trans_name2id_(ObMutator &mutator)
    {
      int ret = OB_SUCCESS;
      CellInfoProcessor ci_proc;
      while (OB_SUCCESS == ret
            && OB_SUCCESS == (ret = mutator.next_cell()))
      {
        ObMutatorCellInfo *mutator_ci = NULL;
        ObCellInfo *ci = NULL;
        if (OB_SUCCESS == mutator.get_cell(&mutator_ci)
            && NULL != mutator_ci)
        {
          const CommonTableSchema *table_schema = NULL;
          const CommonColumnSchema *column_schema = NULL;
          UpsSchemaMgr::SchemaHandle schema_handle;
          ci = &(mutator_ci->cell_info);
          if (!ci_proc.analyse_syntax(*mutator_ci))
          {
            ret = OB_SCHEMA_ERROR;
          }
          else if (ci_proc.need_skip())
          {
            continue;
          }
          else if (!ci_proc.cellinfo_check(*ci))
          {
            ret = OB_SCHEMA_ERROR;
          }
          else if (OB_SUCCESS != (ret = schema_mgr_.get_schema_handle(schema_handle)))
          {
            TBSYS_LOG(WARN, "get_schema_handle fail ret=%d", ret);
          }
          else
          {
            // 调用schema转化name2id
            if (NULL == (table_schema = schema_mgr_.get_table_schema(schema_handle, ci->table_name_)))
            {
              TBSYS_LOG(WARN, "get schema fail table_name=%.*s table_name_length=%d",
                        ci->table_name_.length(), ci->table_name_.ptr(), ci->table_name_.length());
              ret = OB_SCHEMA_ERROR;
            }
            else if (!CellInfoProcessor::cellinfo_check(*ci, *table_schema))
            {
              ret = OB_SCHEMA_ERROR;
            }
            else if (common::ObActionFlag::OP_DEL_ROW == ci_proc.get_op_type())
            {
              // do not trans column name
              ci->table_id_ = table_schema->get_table_id();
              ci->column_id_ = OB_INVALID_ID;
              ci->table_name_.assign_ptr(NULL, 0);
              ci->column_name_.assign_ptr(NULL, 0);
            }
            else if (NULL == (column_schema = schema_mgr_.get_column_schema(schema_handle, ci->table_name_, ci->column_name_)))
            {
              TBSYS_LOG(WARN, "get column schema fail table_name=%.*s table_id=%lu column_name=%.*s column_name_length=%d",
                        ci->table_name_.length(), ci->table_name_.ptr(), table_schema->get_table_id(),
                        ci->column_name_.length(), ci->column_name_.ptr(), ci->column_name_.length());
              ret = OB_SCHEMA_ERROR;
            }
            else if (table_schema->get_rowkey_info().is_rowkey_column(column_schema->get_id()))
            {
              TBSYS_LOG(WARN, "rowkey column cannot be update %s", print_cellinfo(ci));
              ret = OB_SCHEMA_ERROR;
            }
            // 数据类型与合法性检查
            else if (!CellInfoProcessor::cellinfo_check(*ci, *column_schema))
            {
              ret = OB_SCHEMA_ERROR;
            }
            else
            {
              ci->table_id_ = table_schema->get_table_id();
              ci->column_id_ = column_schema->get_id();
              ci->table_name_.assign_ptr(NULL, 0);
              ci->column_name_.assign_ptr(NULL, 0);
            }
            schema_mgr_.revert_schema_handle(schema_handle);
          }
        }
        else
        {
          ret = OB_ERROR;
        }
      }
      ret = (OB_ITER_END == ret) ? OB_SUCCESS : ret;
      mutator.reset_iter();

      return ret;
    };

    int ObUpsTableMgr::fill_commit_log(ObUpsMutator &ups_mutator, TraceLog::LogBuffer &tlog_buffer)
    {
      return fill_commit_log_(ups_mutator, tlog_buffer);
    }

    int ObUpsTableMgr::flush_commit_log(TraceLog::LogBuffer &tlog_buffer)
    {
      return flush_commit_log_(tlog_buffer);
    }
#if 1
    int ObUpsTableMgr::fill_commit_log_(ObUpsMutator &ups_mutator, TraceLog::LogBuffer &tlog_buffer)
    {
      int ret = OB_SUCCESS;
      int64_t serialize_size = 0;
      ObUpdateServerMain *main = ObUpdateServerMain::get_instance();
      if (NULL == main)
      {
        TBSYS_LOG(ERROR, "get updateserver main null pointer");
        ret = OB_ERROR;
      }
      else
      {
        FILL_TRACE_BUF(tlog_buffer, "ups_mutator serialize");
        ObUpsLogMgr& log_mgr = main->get_update_server().get_log_mgr();
        if (OB_BUF_NOT_ENOUGH == (ret = log_mgr.write_log(OB_LOG_UPS_MUTATOR, ups_mutator)))
        {
          TBSYS_LOG(INFO, "log buffer full");
          ret = OB_EAGAIN;
        }
        else if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "write log fail ret=%d, enter FATAL state", ret);
          set_state_as_fatal();
          ret = OB_RESPONSE_TIME_OUT;
        }
      }
      FILL_TRACE_BUF(tlog_buffer, "write log size=%ld ret=%d", serialize_size, ret);
      return ret;
    };
#else
    int ObUpsTableMgr::fill_commit_log_(ObUpsMutator &ups_mutator, TraceLog::LogBuffer &tlog_buffer)
    {
      int ret = OB_SUCCESS;
      int64_t serialize_size = 0;
      ObUpdateServerMain *main = ObUpdateServerMain::get_instance();
      if (NULL == main)
      {
        TBSYS_LOG(ERROR, "get updateserver main null pointer");
        ret = OB_ERROR;
      }
      else if (NULL == log_buffer_)
      {
        TBSYS_LOG(WARN, "log buffer malloc fail");
        ret = OB_ERROR;
      }
      else if (OB_SUCCESS != (ret = ups_mutator.serialize(log_buffer_, LOG_BUFFER_SIZE, serialize_size)))
      {
        TBSYS_LOG(WARN, "ups_mutator serialilze fail log_buffer=%p log_buffer_size=%ld serialize_size=%ld ret=%d",
                  log_buffer_, LOG_BUFFER_SIZE, serialize_size, ret);
      }
      else
      {
        FILL_TRACE_BUF(tlog_buffer, "ups_mutator serialize");
        ObUpsLogMgr& log_mgr = main->get_update_server().get_log_mgr();
        if (OB_BUF_NOT_ENOUGH == (ret = log_mgr.write_log(OB_LOG_UPS_MUTATOR, log_buffer_, serialize_size)))
        {
          TBSYS_LOG(INFO, "log buffer full");
          ret = OB_EAGAIN;
        }
        else if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "write log fail ret=%d, enter FATAL state", ret);
          set_state_as_fatal();
          ret = OB_RESPONSE_TIME_OUT;
        }
      }
      FILL_TRACE_BUF(tlog_buffer, "write log size=%ld ret=%d", serialize_size, ret);
      return ret;
    };
#endif
    int ObUpsTableMgr::flush_commit_log_(TraceLog::LogBuffer &tlog_buffer)
    {
      int ret = OB_SUCCESS;
      ObUpdateServerMain *main = ObUpdateServerMain::get_instance();
      if (NULL == main)
      {
        TBSYS_LOG(ERROR, "get updateserver main null pointer");
        ret = OB_ERROR;
      }
      else
      {
        ObUpsLogMgr& log_mgr = main->get_update_server().get_log_mgr();
        ret = log_mgr.flush_log(tlog_buffer);
      }
      // 只要不能刷新日志都要优雅退出
      //if (OB_SUCCESS != ret)
      //{
      //  TBSYS_LOG(WARN, "flush log fail ret=%d, will kill self", ret);
      //  kill(getpid(), SIGTERM);
      //}
      return ret;
    };

    int ObUpsTableMgr::load_sstable_bypass(SSTableMgr &sstable_mgr, int64_t &loaded_num)
    {
      int ret = OB_SUCCESS;
      table_mgr_.lock_freeze();
      uint64_t major_version = table_mgr_.get_cur_major_version();
      uint64_t minor_version_start = table_mgr_.get_cur_minor_version();
      uint64_t minor_version_end = SSTableID::MAX_MINOR_VERSION - 1;
      uint64_t clog_id = table_mgr_.get_last_clog_id();
      uint64_t checksum = 0;
      ret = sstable_mgr.load_sstable_bypass(major_version, minor_version_start, minor_version_end, clog_id,
                                            table_mgr_.get_table_list2add(), checksum);
      if (OB_SUCCESS == ret)
      {
        int64_t tmp = table_mgr_.get_table_list2add().size();
        ret = table_mgr_.add_table_list();
        if (OB_SUCCESS == ret)
        {
          loaded_num = tmp;
          last_bypass_checksum_ = checksum;
        }
      }
      table_mgr_.unlock_freeze();
      return ret;
    }

    int ObUpsTableMgr::check_cur_version()
    {
      int ret = OB_SUCCESS;
      ObUpsMutator ups_mutator;
      ups_mutator.set_check_cur_version();
      ups_mutator.set_cur_major_version(table_mgr_.get_cur_major_version());
      ups_mutator.set_cur_minor_version(table_mgr_.get_cur_minor_version());
      ups_mutator.set_last_bypass_checksum(last_bypass_checksum_);
      if (OB_SUCCESS != (ret = fill_commit_log_(ups_mutator, TraceLog::get_logbuffer())))
      {
        TBSYS_LOG(WARN, "fill commit log fail ret=%d", ret);
      }
      else if (OB_SUCCESS != (ret = flush_commit_log_(TraceLog::get_logbuffer())))
      {
        TBSYS_LOG(WARN, "flush commit log fail ret=%d", ret);
      }
      else
      {
        TBSYS_LOG(INFO, "write check cur version log succ, cur_major_version=%lu cur_minor_version=%lu",
                  ups_mutator.get_cur_major_version(), ups_mutator.get_cur_minor_version());
      }
      return ret;
    }

    int ObUpsTableMgr::commit_check_sstable_checksum(const uint64_t sstable_id, const uint64_t checksum)
    {
      int ret = OB_SUCCESS;
      SSTableID sst_id = sstable_id;
      ObUpsMutator ups_mutator;
      ups_mutator.set_check_sstable_checksum();
      ups_mutator.set_cur_major_version(sst_id.major_version);
      ups_mutator.set_cur_minor_version(sst_id.minor_version_start);
      ups_mutator.set_sstable_checksum(checksum);
      if (OB_SUCCESS != (ret = fill_commit_log_(ups_mutator, TraceLog::get_logbuffer())))
      {
        TBSYS_LOG(WARN, "fill commit log fail ret=%d", ret);
      }
      else if (OB_SUCCESS != (ret = flush_commit_log_(TraceLog::get_logbuffer())))
      {
        TBSYS_LOG(WARN, "flush commit log fail ret=%d", ret);
      }
      else
      {
        TBSYS_LOG(INFO, "write check sstable checksum log succ, cur_major_version=%lu cur_minor_version=%lu checksum=%lu",
                  ups_mutator.get_cur_major_version(), ups_mutator.get_cur_minor_version(), checksum);
      }
      return ret;
    }

    void ObUpsTableMgr::update_merged_version(ObUpsRpcStub &rpc_stub, const ObServer &root_server, const int64_t timeout_us)
    {
      int64_t oldest_memtable_size = 0;
      uint64_t oldest_memtable_version = SSTableID::MAX_MAJOR_VERSION;
      table_mgr_.get_oldest_memtable_size(oldest_memtable_size, oldest_memtable_version);
      uint64_t oldest_major_version = SSTableID::MAX_MAJOR_VERSION;
      table_mgr_.get_oldest_major_version(oldest_major_version);
      uint64_t last_frozen_version = 0;
      get_last_frozen_memtable_version(last_frozen_version);

      uint64_t merged_version = table_mgr_.get_merged_version();
      if (merged_version < oldest_major_version)
      {
        merged_version = oldest_major_version;
      }

      while (merged_version <= last_frozen_version)
      {
        bool merged = false;
        int tmp_ret = rpc_stub.check_table_merged(root_server, merged_version, merged, timeout_us);
        if (OB_SUCCESS != tmp_ret)
        {
          TBSYS_LOG(WARN, "rpc check_table_merged fail ret=%d", tmp_ret);
          break;
        }
        if (!merged)
        {
          TBSYS_LOG(INFO, "major_version=%lu have not merged done", merged_version);
          break;
        }
        else
        {
          table_mgr_.set_merged_version(merged_version, tbsys::CTimeUtil::getTime());
          TBSYS_LOG(INFO, "major_version=%lu have merged done, done_time=%ld", merged_version, table_mgr_.get_merged_timestamp());
          merged_version += 1;
        }
      }

      if (oldest_memtable_version <= table_mgr_.get_merged_version())
      {
        submit_immediately_drop();
      }
    }

    int ObUpsTableMgr::pre_process(const bool using_id, ObMutator& mutator, const IToken *token)
    {
      int ret = OB_SUCCESS;

      const CommonSchemaManager* common_schema_mgr = NULL;
      UpsSchemaMgrGuard guard;
      common_schema_mgr = schema_mgr_.get_schema_mgr(guard);

      if (OB_SUCCESS == ret
          && NULL != token)
      {
        ret = check_permission_(mutator, *token);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "fail to check permission ret=%d", ret);
        }
      }

      if (OB_SUCCESS == ret
          && !using_id)
      {
        ret = trans_name2id_(mutator);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "failed to trans_name2id, ret=%d", ret);
        }
      }

      return ret;
    }

    void ObUpsTableMgr::dump_memtable(const ObString &dump_dir)
    {
      table_mgr_.dump_memtable2text(dump_dir);
    }

    void ObUpsTableMgr::dump_schemas()
    {
      schema_mgr_.dump2text();
    }

    int ObUpsTableMgr::sstable_scan_finished(const int64_t minor_num_limit)
    {
      return table_mgr_.sstable_scan_finished(minor_num_limit);
    }

    int ObUpsTableMgr::check_sstable_id()
    {
      return table_mgr_.check_sstable_id();
    }

    void ObUpsTableMgr::log_table_info()
    {
      TBSYS_LOG(INFO, "replay checksum flag=%s", STR_BOOL(check_checksum_));
      table_mgr_.log_table_info();
      ObUpdateServerMain *ups_main = ObUpdateServerMain::get_instance();
      if (NULL != ups_main)
      {
        ups_main->get_update_server().get_sstable_mgr().log_sstable_info();
      }
    }

    void ObUpsTableMgr::set_warm_up_percent(const int64_t warm_up_percent)
    {
      table_mgr_.set_warm_up_percent(warm_up_percent);
    }

    int ObUpsTableMgr::get_schema(const uint64_t major_version, CommonSchemaManagerWrapper &sm)
    {
      return table_mgr_.get_schema(major_version, sm);
    }

    int ObUpsTableMgr::get_sstable_range_list(const uint64_t major_version, const uint64_t table_id, TabletInfoList &ti_list)
    {
      return table_mgr_.get_sstable_range_list(major_version, table_id, ti_list);
    }

    bool get_key_prefix(const TEKey &te_key, TEKey &prefix_key)
    {
      bool bret = false;
      TEKey tmp_key = te_key;
      ObUpdateServerMain *main = ObUpdateServerMain::get_instance();
      if (NULL == main)
      {
        TBSYS_LOG(ERROR, "get updateserver main null pointer");
      }
      else
      {
        ObUpsTableMgr &tm = main->get_update_server().get_table_mgr();
        const CommonTableSchema *table_schema = NULL;
        UpsSchemaMgr::SchemaHandle schema_handle;
        int tmp_ret = OB_SUCCESS;
        if (OB_SUCCESS != (tmp_ret = tm.schema_mgr_.get_schema_handle(schema_handle)))
        {
          TBSYS_LOG(WARN, "get schema handle fail ret=%d", tmp_ret);
        }
        else
        {
          if (NULL == (table_schema = tm.schema_mgr_.get_table_schema(schema_handle, te_key.table_id)))
          {
            TBSYS_LOG(ERROR, "get schema fail table_id=%lu", te_key.table_id);
          }
          else
          {
            int32_t split_pos = table_schema->get_split_pos();
            if (split_pos > tmp_key.row_key.length())
            {
              TBSYS_LOG(ERROR, "row key cannot be splited length=%ld split_pos=%d",
                        tmp_key.row_key.length(), split_pos);
            }
            else
            {
              tmp_key.row_key.assign(const_cast<ObObj*>(tmp_key.row_key.ptr()), split_pos);
              prefix_key = tmp_key;
              bret = true;
            }
          }
          tm.schema_mgr_.revert_schema_handle(schema_handle);
        }
      }
      return bret;
    }

    int ObUpsTableMgr :: get(const BaseSessionCtx &session_ctx,
                            const ObGetParam &get_param,
                            ObScanner &scanner,
                            const int64_t start_time,
                            const int64_t timeout)
    {
      return get_(session_ctx, get_param, scanner, start_time, timeout);
    }

    int ObUpsTableMgr :: new_get(BaseSessionCtx &session_ctx,
                                const ObGetParam &get_param,
                                common::ObCellNewScanner &scanner,
                                const int64_t start_time,
                                const int64_t timeout,
                                const sql::ObLockFlag lock_flag/* = sql::LF_NONE*/)
    {
      int ret = OB_SUCCESS;
      if (OB_SUCCESS != (ret = get_(session_ctx, get_param, scanner, start_time,
                                    timeout, lock_flag)))
      {
        TBSYS_LOG(WARN, "ups get fail:ret[%d]", ret);
      }
      else if (OB_SUCCESS != (ret = scanner.finish()))
      {
        TBSYS_LOG(WARN, "finish new scanner fail:ret[%d]", ret);
      }
      return ret;
    }

    int ObUpsTableMgr :: get(const ObGetParam &get_param, ObScanner &scanner, const int64_t start_time, const int64_t timeout)
    {
      return get_(get_param, scanner, start_time, timeout);
    }

    int ObUpsTableMgr :: new_get(const ObGetParam &get_param, common::ObCellNewScanner &scanner, const int64_t start_time, const int64_t timeout)
    {
      int ret = OB_SUCCESS;
      if (OB_SUCCESS != (ret = get_(get_param, scanner, start_time, timeout)))
      {
        TBSYS_LOG(WARN, "ups get fail:ret[%d]", ret);
      }
      else if (OB_SUCCESS != (ret = scanner.finish()))
      {
        TBSYS_LOG(WARN, "finish new scanner fail:ret[%d]", ret);
      }
      return ret;
    }

    template<class T>
    int ObUpsTableMgr :: get_(const ObGetParam &get_param, T &scanner, const int64_t start_time, const int64_t timeout)
    {
      int ret = OB_SUCCESS;
      TableList *table_list = GET_TSI_MULT(TableList, TSI_UPS_TABLE_LIST_1);
      uint64_t max_valid_version = 0;
      bool is_final_minor = false;
      if (NULL == table_list)
      {
        TBSYS_LOG(WARN, "get tsi table_list fail");
        ret = OB_ERROR;
      }
      else if (OB_SUCCESS != (ret = table_mgr_.acquire_table(get_param.get_version_range(), max_valid_version, *table_list, is_final_minor, get_table_id(get_param)))
          || 0 == table_list->size())
      {
        TBSYS_LOG(WARN, "acquire table fail version_range=%s", range2str(get_param.get_version_range()));
        ret = (OB_SUCCESS == ret) ? OB_INVALID_START_VERSION : ret;
      }
      else
      {
        FILL_TRACE_LOG("version=%s table_num=%ld", range2str(get_param.get_version_range()), table_list->size());
        TableList::iterator iter;
        int64_t index = 0;
        SSTableID sst_id;
        int64_t trans_start_counter = 0;
        for (iter = table_list->begin(); iter != table_list->end(); iter++, index++)
        {
          ITableEntity *table_entity = *iter;
          ITableUtils *table_utils = NULL;
          if (NULL == table_entity)
          {
            TBSYS_LOG(WARN, "invalid table_entity version_range=%s", range2str(get_param.get_version_range()));
            ret = OB_ERROR;
            break;
          }
          if (NULL == (table_utils = table_entity->get_tsi_tableutils(index)))
          {
            TBSYS_LOG(WARN, "get tsi tableutils fail index=%ld", index);
            ret = OB_TOO_MANY_SSTABLE;
            break;
          }
          if (OB_SUCCESS != (ret = table_entity->start_transaction(table_utils->get_trans_descriptor())))
          {
            TBSYS_LOG(WARN, "start transaction fail ret=%d", ret);
            break;
          }
          trans_start_counter++;
          sst_id = (NULL == table_entity) ? 0 : table_entity->get_table_item().get_sstable_id();
          FILL_TRACE_LOG("get table %s ret=%d", sst_id.log_str(), ret);
        }

        if (OB_SUCCESS == ret)
        {
          ret = get_(*table_list, get_param, scanner, start_time, timeout);
          if (OB_SUCCESS != ret)
          {
            TBSYS_LOG(WARN, "failed to get_ ret=%d", ret);
          }
          else
          {
            scanner.set_data_version(ObVersion::get_version(SSTableID::get_major_version(max_valid_version),
                                                            SSTableID::get_minor_version_end(max_valid_version),
                                                            is_final_minor));
          }
        }

        for (iter = table_list->begin(), index = 0; iter != table_list->end() && index < trans_start_counter; iter++, index++)
        {
          ITableEntity *table_entity = *iter;
          ITableUtils *table_utils = NULL;
          if (NULL != table_entity
              && NULL != (table_utils = table_entity->get_tsi_tableutils(index)))
          {
            table_entity->end_transaction(table_utils->get_trans_descriptor());
            table_utils->reset();
          }
        }
        table_mgr_.revert_table(*table_list);
      }
      return ret;
    }

    int ObUpsTableMgr :: scan(const BaseSessionCtx &session_ctx,
                              const ObScanParam &scan_param,
                              ObScanner &scanner,
                              const int64_t start_time,
                              const int64_t timeout)
    {
      return scan_(session_ctx, scan_param, scanner, start_time, timeout);
    }

    int ObUpsTableMgr :: new_scan(BaseSessionCtx &session_ctx,
                                  const ObScanParam &scan_param,
                                  common::ObCellNewScanner &scanner,
                                  const int64_t start_time,
                                  const int64_t timeout)
    {
      int ret = OB_SUCCESS;
      if (OB_SUCCESS != (ret = scan_(session_ctx, scan_param, scanner, start_time, timeout)))
      {
        TBSYS_LOG(WARN, "ups scan fail:ret[%d]", ret);
      }
      else if(OB_SUCCESS != (ret = scanner.finish()))
      {
        TBSYS_LOG(WARN, "finish new scanner fail:ret[%d]", ret);
      }
      return ret;
    }

    int ObUpsTableMgr :: scan(const ObScanParam &scan_param, ObScanner &scanner, const int64_t start_time, const int64_t timeout)
    {
      return scan_(scan_param, scanner, start_time, timeout);
    }

    int ObUpsTableMgr :: new_scan(const ObScanParam &scan_param, common::ObCellNewScanner &scanner, const int64_t start_time, const int64_t timeout)
    {
      int ret = OB_SUCCESS;
      if (OB_SUCCESS != (ret = scan_(scan_param, scanner, start_time, timeout)))
      {
        TBSYS_LOG(WARN, "ups scan fail:ret[%d]", ret);
      }
      else if(OB_SUCCESS != (ret = scanner.finish()))
      {
        TBSYS_LOG(WARN, "finish new scanner fail:ret[%d]", ret);
      }
      return ret;
    }

    template<class T>
    int ObUpsTableMgr :: scan_(const ObScanParam &scan_param, T &scanner, const int64_t start_time, const int64_t timeout)
    {
      int ret = OB_SUCCESS;
      TableList *table_list = GET_TSI_MULT(TableList, TSI_UPS_TABLE_LIST_1);
      uint64_t max_valid_version = 0;
      const ObNewRange *scan_range = NULL;
      uint64_t table_id = OB_INVALID_ID;//add zhaoqiong [Truncate Table]:20170519
      bool is_final_minor = false;
      if (NULL == table_list)
      {
        TBSYS_LOG(WARN, "get tsi table_list fail");
        ret = OB_ERROR;
      }
      //add zhaoqiong [Truncate Table]:20170519
      else if (OB_INVALID_ID == (table_id = scan_param.get_table_id()))
      {
        TBSYS_LOG(WARN, "invalid scan table_id");
        ret = OB_ERROR;
      }
      //add:e
      else if (NULL == (scan_range = scan_param.get_range()))
      {
        TBSYS_LOG(WARN, "invalid scan range");
        ret = OB_ERROR;
      }
      else if (OB_SUCCESS != (ret = table_mgr_.acquire_table(scan_param.get_version_range(), max_valid_version, *table_list, is_final_minor, get_table_id(scan_param)))
              || 0 == table_list->size())
      {
        TBSYS_LOG(WARN, "acquire table fail version_range=%s", range2str(scan_param.get_version_range()));
        ret = (OB_SUCCESS == ret) ? OB_INVALID_START_VERSION : ret;
      }
      else
      {
        ColumnFilter cf;
        ColumnFilter *pcf = ColumnFilter::build_columnfilter(scan_param, &cf);
        FILL_TRACE_LOG("%s columns=%s direction=%d version=%s table_num=%ld",
                      scan_range2str(*scan_range),
                      (NULL == pcf) ? "nil" : pcf->log_str(),
                      scan_param.get_scan_direction(),
                      range2str(scan_param.get_version_range()),
                      table_list->size());

        do
        {
          ITableEntity::Guard guard(table_mgr_.get_resource_pool());
          ObMerger merger;
          ObIterator *ret_iter = &merger;
          merger.set_asc(scan_param.get_scan_direction() == ScanFlag::FORWARD);

          TableList::iterator iter;
          int64_t index = 0;
          SSTableID sst_id;
          int64_t trans_start_counter = 0;
          ITableIterator *prev_table_iter = NULL;
          bool truncate_status = false; /*add zhaoqiong [Truncate Table]:20170519*/
          for (iter = table_list->begin(); iter != table_list->end(); iter++, index++)
          {
            ITableEntity *table_entity = *iter;
            ITableUtils *table_utils = NULL;
            ITableIterator *table_iter = NULL;
            truncate_status = false; /*add zhaoqiong [Truncate Table]:20170519*/
            if (NULL == table_entity)
            {
              TBSYS_LOG(WARN, "invalid table_entity version_range=%s", range2str(scan_param.get_version_range()));
              ret = OB_ERROR;
              break;
            }
            if (NULL == (table_utils = table_entity->get_tsi_tableutils(index)))
            {
              TBSYS_LOG(WARN, "get tsi tableutils fail index=%ld", index);
              ret = OB_TOO_MANY_SSTABLE;
              break;
            }
            if (ITableEntity::SSTABLE == table_entity->get_table_type())
            {
              table_iter = prev_table_iter;
            }
            if (NULL == table_iter
                && NULL == (table_iter = table_entity->alloc_iterator(table_mgr_.get_resource_pool(), guard)))
            {
              TBSYS_LOG(WARN, "alloc table iterator fai index=%ld", index);
              ret = OB_MEM_OVERFLOW;
              break;
            }
            prev_table_iter = NULL;
            if (OB_SUCCESS != (ret = table_entity->start_transaction(table_utils->get_trans_descriptor())))
            {
              TBSYS_LOG(WARN, "start transaction fail ret=%d scan_range=%s", ret, scan_range2str(*scan_range));
              break;
            }
            trans_start_counter++;
            if (OB_SUCCESS != (ret = table_entity->scan(table_utils->get_trans_descriptor(), scan_param, table_iter)))
            {
              TBSYS_LOG(WARN, "table entity scan fail ret=%d scan_range=%s", ret, scan_range2str(*scan_range));
              break;
            }
            sst_id = (NULL == table_entity) ? 0 : table_entity->get_table_item().get_sstable_id();
            FILL_TRACE_LOG("scan table type=%d %s ret=%d iter=%p", table_entity->get_table_type(), sst_id.log_str(), ret, table_iter);
            if (ITableEntity::SSTABLE == table_entity->get_table_type())
            {
              if (OB_ITER_END == table_iter->next_cell())
              {
                table_iter->reset();
                prev_table_iter = table_iter;
                continue;
              }
            }
            if (1 == table_list->size())
            {
              ret_iter = table_iter;
              break;
            }
            //add zhaoqiong [Truncate Table]:20170519
            if (OB_SUCCESS != (ret = table_entity->get_table_truncate_stat(table_id,truncate_status)))
            {
                TBSYS_LOG(WARN, "failed to get_table_truncate_stat=%d", ret);
                break;
            }
            //add:e
            else if (OB_SUCCESS != (ret = merger.add_iterator(table_iter,truncate_status)))
            {
              TBSYS_LOG(WARN, "add iterator to merger fail ret=%d", ret);
              break;
            }
          }

          if (OB_SUCCESS == ret)
          {
            int64_t row_count = 0;
            ret = add_to_scanner_(*ret_iter, scanner, row_count, start_time, timeout, scan_param.get_scan_size());
            FILL_TRACE_LOG("add to scanner scanner_size=%ld row_count=%ld ret=%d", scanner.get_size(), row_count, ret);
            if (OB_SIZE_OVERFLOW == ret)
            {
              if (row_count > 0)
              {
                scanner.set_is_req_fullfilled(false, row_count);
                ret = OB_SUCCESS;
              }
              else
              {
                TBSYS_LOG(WARN, "memory is not enough to add even one row");
                ret = OB_ERROR;
              }
            }
            else if (OB_SUCCESS != ret)
            {
              TBSYS_LOG(WARN, "failed to add data from ups_merger to scanner, ret=%d", ret);
            }
            else
            {
              scanner.set_is_req_fullfilled(true, row_count);
            }
            if (OB_SUCCESS == ret)
            {
              scanner.set_data_version(ObVersion::get_version(SSTableID::get_major_version(max_valid_version),
                                                              SSTableID::get_minor_version_end(max_valid_version),
                                                              is_final_minor));

              ObNewRange range;
              range.table_id_ = scan_param.get_table_id();
              range.set_whole_range();
              ret = scanner.set_range(range);
            }
          }

          for (iter = table_list->begin(), index = 0; iter != table_list->end() && index < trans_start_counter; iter++, index++)
          {
            ITableEntity *table_entity = *iter;
            ITableUtils *table_utils = NULL;
            if (NULL != table_entity
                && NULL != (table_utils = table_entity->get_tsi_tableutils(index)))
            {
              table_entity->end_transaction(table_utils->get_trans_descriptor());
              table_utils->reset();
            }
          }
        }
        while (false);
        table_mgr_.revert_table(*table_list);
      }
      return ret;
    }

    template<class T>
    int ObUpsTableMgr :: get_(TableList &table_list, const ObGetParam& get_param, T& scanner,
                              const int64_t start_time, const int64_t timeout)
    {
      int err = OB_SUCCESS;

      int64_t cell_size = get_param.get_cell_size();
      const ObCellInfo* cell = NULL;

      if (cell_size <= 0)
      {
        TBSYS_LOG(WARN, "invalid param, cell_size=%ld", cell_size);
        err = OB_INVALID_ARGUMENT;
      }
      else if (NULL == (cell = get_param[0]))
      {
        TBSYS_LOG(WARN, "invalid param first cellinfo");
        err = OB_ERROR;
      }
      else
      {
        int64_t last_cell_idx = 0;
        ObRowkey last_row_key = cell->row_key_;
        uint64_t last_table_id = cell->table_id_;
        int64_t cell_idx = 1;

        while (OB_SUCCESS == err && cell_idx < cell_size)
        {
          if (NULL == (cell = get_param[cell_idx]))
          {
            TBSYS_LOG(WARN, "cellinfo null pointer idx=%ld", cell_idx);
            err = OB_ERROR;
          }
          else if (cell->row_key_ != last_row_key ||
                  cell->table_id_ != last_table_id)
          {
            err = get_row_(table_list, last_cell_idx, cell_idx - 1, get_param, scanner, start_time, timeout);
            if (OB_SIZE_OVERFLOW == err)
            {
              TBSYS_LOG(WARN, "allocate memory failed, first_idx=%ld, last_idx=%ld",
                  last_cell_idx, cell_idx - 1);
            }
            else if (OB_SUCCESS != err)
            {
              TBSYS_LOG(WARN, "failed to get_row_, first_idx=%ld, last_idx=%ld, err=%d",
                  last_cell_idx, cell_idx - 1, err);
            }
            else
            {
              if ((start_time + timeout) < g_cur_time)
              {
                if (last_cell_idx > 0)
                {
                  // at least get one row
                  TBSYS_LOG(WARN, "get or scan too long time, start_time=%ld timeout=%ld timeu=%ld row_count=%ld",
                      start_time, timeout, tbsys::CTimeUtil::getTime() - start_time, last_cell_idx);
                  err = OB_FORCE_TIME_OUT;
                }
                else
                {
                  TBSYS_LOG(ERROR, "can't get any row, start_time=%ld timeout=%ld timeu=%ld",
                      start_time, timeout, tbsys::CTimeUtil::getTime() - start_time);
                  err = OB_RESPONSE_TIME_OUT;
                }
              }

              if (OB_SUCCESS == err)
              {
                last_cell_idx = cell_idx;
                last_row_key = cell->row_key_;
                last_table_id = cell->table_id_;
              }
            }
          }

          if (OB_SUCCESS == err)
          {
            ++cell_idx;
          }
        }

        if (OB_SUCCESS == err)
        {
          err = get_row_(table_list, last_cell_idx, cell_idx - 1, get_param, scanner, start_time, timeout);
          if (OB_SIZE_OVERFLOW == err)
          {
            TBSYS_LOG(WARN, "allocate memory failed, first_idx=%ld, last_idx=%ld",
                last_cell_idx, cell_idx - 1);
          }
          else if (OB_SUCCESS != err)
          {
            TBSYS_LOG(WARN, "failed to get_row_, first_idx=%ld, last_idx=%ld, err=%d",
                last_cell_idx, cell_idx - 1, err);
          }
        }

        FILL_TRACE_LOG("table_list_size=%ld get_param_cell_size=%ld get_param_row_size=%ld last_cell_idx=%ld cell_idx=%ld scanner_size=%ld ret=%d",
                      table_list.size(), get_param.get_cell_size(), get_param.get_row_size(), last_cell_idx, cell_idx, scanner.get_size(), err);
        if (OB_SIZE_OVERFLOW == err)
        {
          // wrap error
          scanner.set_is_req_fullfilled(false, last_cell_idx);
          err = OB_SUCCESS;
        }
        else if (OB_FORCE_TIME_OUT == err)
        {
          scanner.set_is_req_fullfilled(false, cell_idx);
          err = OB_SUCCESS;
        }
        else if (OB_SUCCESS == err)
        {
          scanner.set_is_req_fullfilled(true, cell_idx);
        }
      }

      return err;
    }

    template<class T>
    int ObUpsTableMgr :: get_row_(TableList &table_list, const int64_t first_cell_idx, const int64_t last_cell_idx,
                                  const ObGetParam& get_param, T& scanner,
                                  const int64_t start_time, const int64_t timeout)
    {
      int ret = OB_SUCCESS;
      int64_t cell_size = get_param.get_cell_size();
      const ObCellInfo* cell = NULL;
      ColumnFilter *column_filter = NULL;

      if (first_cell_idx > last_cell_idx)
      {
        TBSYS_LOG(WARN, "invalid param, first_cell_idx=%ld, last_cell_idx=%ld",
            first_cell_idx, last_cell_idx);
        ret = OB_INVALID_ARGUMENT;
      }
      else if (cell_size <= last_cell_idx)
      {
        TBSYS_LOG(WARN, "invalid status, cell_size=%ld, last_cell_idx=%ld",
            cell_size, last_cell_idx);
        ret = OB_ERROR;
      }
      else if (NULL == (cell = get_param[first_cell_idx]))
      {
        TBSYS_LOG(WARN, "cellinfo null pointer idx=%ld", first_cell_idx);
        ret = OB_ERROR;
      }
      else if (NULL == (column_filter = ITableEntity::get_tsi_columnfilter()))
      {
        TBSYS_LOG(WARN, "get tsi columnfilter fail");
        ret = OB_ERROR;
      }
      else
      {
        ITableEntity::Guard guard(table_mgr_.get_resource_pool());
        ObMerger merger;
        ObIterator *ret_iter = &merger;

        uint64_t table_id = cell->table_id_;
        ObRowkey row_key = cell->row_key_;
        column_filter->clear();
        for (int64_t i = first_cell_idx; i <= last_cell_idx; ++i)
        {
          if (NULL != get_param[i]) // double check
          {
            column_filter->add_column(get_param[i]->column_id_);
          }
        }

        TableList::iterator iter;
        int64_t index = 0;
        ITableIterator *prev_table_iter = NULL;
        bool truncate_status = false; /*add zhaoqiong [Truncate Table]:20170519*/
        for (iter = table_list.begin(), index = 0; iter != table_list.end(); iter++, index++)
        {
          ITableEntity *table_entity = *iter;
          ITableUtils *table_utils = NULL;
          ITableIterator *table_iter = NULL;
          truncate_status = false; /*add zhaoqiong [Truncate Table]:20170519*/
          if (NULL == table_entity)
          {
            TBSYS_LOG(WARN, "invalid table_entity version_range=%s", range2str(get_param.get_version_range()));
            ret = OB_ERROR;
            break;
          }
          if (NULL == (table_utils = table_entity->get_tsi_tableutils(index)))
          {
            TBSYS_LOG(WARN, "get tsi tableutils fail index=%ld", index);
            ret = OB_ERROR;
            break;
          }
          if (ITableEntity::SSTABLE == table_entity->get_table_type())
          {
            table_iter = prev_table_iter;
          }
          if (NULL == table_iter
              && NULL == (table_iter = table_entity->alloc_iterator(table_mgr_.get_resource_pool(), guard)))
          {
            TBSYS_LOG(WARN, "alloc table iterator fai index=%ld", index);
            ret = OB_MEM_OVERFLOW;
            break;
          }
          prev_table_iter = NULL;
          if (OB_SUCCESS != (ret = table_entity->get(table_utils->get_trans_descriptor(),
              table_id, row_key, column_filter, table_iter)))
          {
            TBSYS_LOG(WARN, "table entity get fail ret=%d table_id=%lu row_key=[%s] columns=[%s]",
                      ret, table_id, to_cstring(row_key), column_filter->log_str());
            break;
          }
          TBSYS_LOG(DEBUG, "get row row_key=[%s] row_key_ptr=%p columns=[%s] iter=%p",
                    to_cstring(row_key), row_key.ptr(), column_filter->log_str(), table_iter);
          if (ITableEntity::SSTABLE == table_entity->get_table_type())
          {
            if (OB_ITER_END == table_iter->next_cell())
            {
              table_iter->reset();
              prev_table_iter = table_iter;
              continue;
            }
          }
          if (1 == table_list.size())
          {
            ret_iter = table_iter;
            break;
          }

          //add zhaoqiong [Truncate Table]:20170519
          if (OB_SUCCESS != (ret = table_entity->get_table_truncate_stat(table_id,truncate_status)))
          {
              TBSYS_LOG(WARN, "failed to get_table_truncate_stat=%d", ret);
              break;
          }
          //add:e
          else if (OB_SUCCESS != (ret = merger.add_iterator(table_iter,truncate_status)))
          {
            TBSYS_LOG(WARN, "add iterator to merger fail ret=%d", ret);
            break;
          }
        }

        if (OB_SUCCESS == ret)
        {
          int64_t row_count = 0;
          ret = add_to_scanner_(*ret_iter, scanner, row_count, start_time, timeout);
          if (OB_SIZE_OVERFLOW == ret)
          {
            // return ret
          }
          else if (OB_SUCCESS != ret)
          {
            TBSYS_LOG(WARN, "failed to add data from merger to scanner, ret=%d", ret);
          }
          else
          {
            // TODO (rizhao) add successful cell num to scanner
          }
        }
      }

      TBSYS_LOG(DEBUG, "[op=GET_ROW] [first_cell_idx=%ld] [last_cell_idx=%ld] [ret=%d]", first_cell_idx, last_cell_idx, ret);
      return ret;
    }

    template<class T>
    int ObUpsTableMgr :: add_to_scanner_(ObIterator& ups_merger, T& scanner, int64_t& row_count,
                                        const int64_t start_time, const int64_t timeout, const int64_t result_limit_size/* = UINT64_SIZE*/)
    {
      int err = OB_SUCCESS;

      ObCellInfo* cell = NULL;
      bool is_row_changed = false;
      row_count = 0;
      while (OB_SUCCESS == err && (OB_SUCCESS == (err = ups_merger.next_cell())))
      {
        err = ups_merger.get_cell(&cell, &is_row_changed);
        if (OB_SUCCESS != err || NULL == cell)
        {
          TBSYS_LOG(WARN, "failed to get cell, err=%d", err);
          err = OB_ERROR;
        }
        else
        {
          TBSYS_LOG(DEBUG, "from merger %s is_row_changed=%s", print_cellinfo(cell), STR_BOOL(is_row_changed));
          if (is_row_changed)
          {
            ++row_count;
          }
          err = scanner.add_cell(*cell, false, is_row_changed);
          if (1 < row_count
              && 0 < result_limit_size
              && result_limit_size <= scanner.get_size())
          {
            err = OB_SIZE_OVERFLOW;
          }
          else if (is_row_changed
              && 1 < row_count
              && (start_time + timeout) < g_cur_time)
          {
            TBSYS_LOG(WARN, "get or scan too long time, start_time=%ld timeout=%ld timeu=%ld row_count=%ld",
                      start_time, timeout, tbsys::CTimeUtil::getTime() - start_time, row_count - 1);
            err = OB_SIZE_OVERFLOW;
          }
          if (OB_SUCCESS != err)
          {
            row_count-=1;
            if (1 > row_count)
            {
              TBSYS_LOG(WARN, "invalid row_count=%ld", row_count);
            }
          }
          if (OB_SIZE_OVERFLOW == err)
          {
            TBSYS_LOG(DEBUG, "scanner memory is not enough, rollback last row");
            // rollback last row
            if (OB_SUCCESS != scanner.rollback())
            {
              TBSYS_LOG(WARN, "failed to rollback");
              err = OB_ERROR;
            }
          }
          else if (OB_SUCCESS != err)
          {
            TBSYS_LOG(WARN, "failed to add cell, err=%d", err);
          }
        }
      }

      if (OB_ITER_END == err)
      {
        // wrap error
        err = OB_SUCCESS;
      }

      return err;
    }
  }
}
