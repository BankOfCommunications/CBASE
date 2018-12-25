/**
 * (C) 2007-2013 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Authors:
 *   rongxuan <rongxuan.lc@taobao.com>
 *     - some work details if you want
 *
 */
#include "rootserver/ob_root_server2.h"
#include "common/ob_bypass_task_info.h"
#include "rootserver/ob_root_operation_helper.h"
namespace oceanbase
{
  namespace rootserver
  {
    ObRootOperationHelper::ObRootOperationHelper()
      :process_(INIT), root_server_(NULL),
      start_time_us_(0), delete_index_(0),
      done_count_(0), total_count_(0), config_(NULL),
      rpc_stub_(NULL), server_manager_(NULL)
    {
    }
    ObRootOperationHelper::~ObRootOperationHelper()
    {
    }
    int ObRootOperationHelper::init(const ObRootServer2 *root_server,
        const ObRootServerConfig *config,
        const ObRootRpcStub *rpc_stub,
        const ObChunkServerManager* server_manager)
    {
      int ret = OB_SUCCESS;
      if (NULL == root_server || NULL == config
          || NULL == rpc_stub || NULL == server_manager)
      {
        TBSYS_LOG(WARN, "invalid arugment, root_server=%p, config=%p, rpc_stub=%p, server_manager=%p",
            root_server, config, rpc_stub, server_manager);
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        root_server_ = const_cast<ObRootServer2 *>(root_server);
        config_ = const_cast<ObRootServerConfig *>(config);
        rpc_stub_ = const_cast<ObRootRpcStub *>(rpc_stub);
        server_manager_ = const_cast<ObChunkServerManager*>(server_manager);
        bypass_data_.init(config_);
      }
      return ret;
    }

    int ObRootOperationHelper::start_operation(const common::ObSchemaManagerV2 *schema_mgr,
        const ObBypassTaskInfo &table_name_id,
        const int64_t frozen_version)
    {
      int ret = OB_SUCCESS;
      if (NULL == schema_mgr)
      {
        TBSYS_LOG(WARN, "INVALID ARGUMENT. schema_mgr=%p, table_name_id.count()=%ld", schema_mgr, table_name_id.count());
        ret = OB_INVALID_ARGUMENT;
      }
      OperationType type;
      if (OB_SUCCESS == ret)
      {
        process_ = STARTED;
        type = table_name_id.get_operation_type();
        bypass_data_.set_operation_type(type);
        switch (type)
        {
          case CLEAN_ROOT_TABLE:
            {
              TBSYS_LOG(INFO, "receive order to clean old roottable.");
              ret = clean_root_table(schema_mgr);
              break;
            }
          case IMPORT_ALL:
            {
              TBSYS_LOG(INFO, "start to import all table. for cluster dulplicate.");
              ret = import_all_table(frozen_version, schema_mgr, table_name_id);
              break;
            }
          case IMPORT_TABLE:
            {
              TBSYS_LOG(INFO, "start to import table. for bypass process");
              ret = bypass_process(frozen_version, schema_mgr, table_name_id);
              break;
            }
          default:
            TBSYS_LOG(WARN, "invalid type. type=%d", type);
            ret = OB_ERROR;
            break;
        }
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "fail to start operation. operation_type=%d, ret=%d", type, ret);
          bypass_data_.destroy_data();
          end_bypass_process();
        }
      }
      return ret;
    }

    int ObRootOperationHelper::clean_root_table(const common::ObSchemaManagerV2 *schema_mgr)
    {
      int ret = OB_SUCCESS;
      TBSYS_LOG(INFO, "start to clean root table.");
      if (NULL == schema_mgr)
      {
        TBSYS_LOG(WARN, "invalid argument. schema_mgr=%p", schema_mgr);
        ret = OB_INVALID_ARGUMENT;
      }
      if (OB_SUCCESS == ret)
      {
        ret = bypass_data_.generate_bypass_data(schema_mgr);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "fail to generate new data. ret=%d", ret);
        }
      }
      if (OB_SUCCESS == ret)
      {
        start_time_us_ = tbsys::CTimeUtil::getTime();
        ret = root_server_->request_cs_report_tablet();
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "fail to request cs report tablet. ret=%d", ret);
        }
        else
        {
          TBSYS_LOG(INFO, "root server enter report_tablet process");
          process_ = REPORT_TABLET;
        }
      }
      return ret;
    }
    int ObRootOperationHelper::import_all_table(const int64_t frozen_version,
        const common::ObSchemaManagerV2 *schema_mgr,
        const ObBypassTaskInfo &table_name_id)
    {
      int ret = OB_SUCCESS;
      TBSYS_LOG(INFO, "start to import all table.");
      if (NULL == schema_mgr)
      {
        TBSYS_LOG(WARN, "invalid argument. schema_mgr=%p", schema_mgr);
        ret = OB_INVALID_ARGUMENT;
      }
      if (OB_SUCCESS == ret)
      {
        ret = bypass_data_.generate_bypass_data(schema_mgr);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "fail to generate new data. ret=%d", ret);
        }
      }
      if (OB_SUCCESS == ret)
      {
        int64_t now = tbsys::CTimeUtil::getTime();
        start_time_us_ = now;
        if (OB_SUCCESS != (ret = request_cs_load_bypass_table(frozen_version, table_name_id)))
        {
          TBSYS_LOG(WARN, "fail to request cs load bypass table. ret=%d", ret);
        }
        else
        {
          TBSYS_LOG(INFO, "root server enter load_sstable process");
          process_ = LOAD_SSTABLE;
        }
      }
      return ret;
    }

    int ObRootOperationHelper::bypass_process(const int64_t frozen_version, const common::ObSchemaManagerV2 *schema_mgr,
        const ObBypassTaskInfo &table_name_id)
    {
      int ret = OB_SUCCESS;
      if (NULL == schema_mgr)
      {
        TBSYS_LOG(WARN, "INVALID ARGUMENT. schema_mgr=%p, table_name_id.count()=%ld", schema_mgr, table_name_id.count());
        ret = OB_INVALID_ARGUMENT;
      }
      if (OB_SUCCESS == ret)
      {
        ret = bypass_data_.init_schema_mgr(schema_mgr);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "fail to init schema_mgr while bypass proces. ret=%d", ret);
        }
      }
      if (OB_SUCCESS == ret)
      {
        int64_t now = tbsys::CTimeUtil::getTime();
        start_time_us_ = now;
        for (int64_t i = 0; i < table_name_id.count(); i++)
        {
          char table_name[OB_MAX_TABLE_NAME_LENGTH];
          strncpy(table_name, const_cast<ObBypassTaskInfo*>(&table_name_id)->at(i).first.ptr(),
              const_cast<ObBypassTaskInfo*>(&table_name_id)->at(i).first.length());
          table_name[const_cast<ObBypassTaskInfo*>(&table_name_id)->at(i).first.length()] = '\0';
          //char* table_name = const_cast<ObBypassTaskInfo*>(&table_name_id)->at(i).first.ptr();
          uint64_t new_table_id = const_cast<ObBypassTaskInfo*>(&table_name_id)->at(i).second;
          if (OB_SUCCESS != (ret = bypass_data_.change_table_schema(schema_mgr, table_name, new_table_id)))
          {
            TBSYS_LOG(WARN, "fail to generate new schema. ret=%d", ret);
            break;
          }
          else
          {
            TBSYS_LOG(INFO, "bypass process, table_name=%s, new_table_id=%ld", table_name, new_table_id);
          }
        }
      }
      if (OB_SUCCESS == ret)
      {
        ret = bypass_data_.generate_root_table();
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "fail to generate new roottable. ret=%d", ret);
        }
      }
      if (OB_SUCCESS == ret)
      {
        if (OB_SUCCESS != (ret = request_cs_load_bypass_table(frozen_version, table_name_id)))
        {
          TBSYS_LOG(WARN, "fail to request cs load bypass table. ret=%d", ret);
        }
        else
        {
          TBSYS_LOG(INFO, "root server enter load_sstable process");
          process_ = LOAD_SSTABLE;
        }
      }
      return ret;
    }
    int ObRootOperationHelper::check_delete_tables_process()
    {
      int ret = OB_ERROR;
      common::ObArray<uint64_t>& delete_table = bypass_data_.get_delete_table();
      TBSYS_LOG(INFO, "delete index=%ld, delete table count=%ld", delete_index_, delete_table.count());
      if (delete_index_ >= delete_table.count())
      {
        if (done_count_ >= total_count_)
        {
          ret = OB_SUCCESS;
          TBSYS_LOG(INFO, "delete process is done. done_count=%ld, total_count=%ld", done_count_, total_count_);
          done_count_ = 0;
          total_count_ = 0;
          ret = OB_SUCCESS;
        }
      }
      else
      {
        uint64_t table_id = delete_table.at(delete_index_);
        ObChunkServerManager::iterator it = server_manager_->begin();
        for (; it != server_manager_->end(); ++it)
        {
          if (it->status_ != ObServerStatus::STATUS_DEAD)
          {
            if (OB_SUCCESS != rpc_stub_->request_cs_delete_table(it->server_, table_id, config_->network_timeout))
            {
              TBSYS_LOG(WARN, "fail to request cs to delete table,table_id=%lu. cs_addr=%s", table_id, it->server_.to_cstring());
            }
            else
            {
              total_count_ ++;
            }
          }
        }
        TBSYS_LOG(INFO, "request cs to delete table. table_id=%ld, total_count=%ld",
            table_id, total_count_);
        delete_index_ ++;
      }
      if (OB_SUCCESS != ret)
      {
        int64_t now = tbsys::CTimeUtil::getTime();
        int64_t timeout = config_->delete_table_time;
        if (start_time_us_ + config_->delete_table_time < now)
        {
          TBSYS_LOG(WARN, "cs delete table timeout. start_time=%ld, timeout=%ld, now=%ld",
              start_time_us_, timeout, now);
          ret = OB_BYPASS_TIMEOUT;
        }
      }
      return ret;
    }
    void ObRootOperationHelper::delete_tables_done(const int cs_index,
        const uint64_t table_id, const bool is_succ)
    {
      TBSYS_LOG(INFO, "cs_index=%d  delete table finished. table_id=%lu, is_succ=%s",
          cs_index, table_id, is_succ ? "true" : "false");
      done_count_ ++;
    }
    int ObRootOperationHelper::request_cs_load_bypass_table(const int64_t frozen_version,
        const common::ObBypassTaskInfo &table_name_id,
        const int64_t expect_cs)
    {
      int ret = OB_SUCCESS;
      common::ObTableImportInfoList import_info;
      import_info.tablet_version_ = frozen_version;
      if (OB_SUCCESS == ret)
      {
        if (IMPORT_ALL == table_name_id.get_operation_type())
        {
          import_info.import_all_ = true;
        }
        else
        {
          import_info.import_all_ = false;
          for (int64_t i = 0; i < table_name_id.count(); i++)
          {
            if (OB_SUCCESS != (ret = import_info.add_table(const_cast<ObBypassTaskInfo*>(&table_name_id)->at(i).second)))
            {
              TBSYS_LOG(WARN, "fail to add table. ret=%d, i = %ld", ret, i);
              break;
            }
            else
            {
              TBSYS_LOG(INFO, "bypass proces: request cs load tablet, table_id=%lu", const_cast<ObBypassTaskInfo*>(&table_name_id)->at(i).second);
            }
          }
        }
      }
      if (OB_SUCCESS == ret)
      {
        ObChunkServerManager::iterator it = server_manager_->begin();
        for (; it != server_manager_->end(); ++it)
        {
          if (expect_cs == OB_INVALID_INDEX || expect_cs != it - server_manager_->begin())
          {
            if( it->status_ != ObServerStatus::STATUS_DEAD)
            {
              if (OB_SUCCESS != (ret = rpc_stub_->request_cs_load_bypass_tablet(it->server_, import_info,
                      config_->network_timeout)))
              {
                TBSYS_LOG(WARN, "fail to request cs to report. cs_addr=%s", it->server_.to_cstring());
              }
            }
            else
            {
              it->bypass_process_ = ObServerStatus::INIT;
            }
          }
        }
      }
      return ret;
    }
    int ObRootOperationHelper::cs_load_sstable_done(const int cs_index,
        const common::ObTableImportInfoList &table_list, const bool is_load_succ)
    {
      int ret = OB_SUCCESS;
      UNUSED(table_list);
      if (!is_load_succ)
      {
        TBSYS_LOG(WARN, "cs_index %d fail to load tablet.", cs_index);
      }
      else
      {
        TBSYS_LOG(INFO, "chunkserver %d finish load sstable, table_list=%s", cs_index, to_cstring(table_list));
        ObServerStatus* status = server_manager_->get_server_status(cs_index);
        if (NULL == status)
        {
          ret = OB_ERROR;
          TBSYS_LOG(WARN, "invalid status.  status=%p, cs_index=%d", status, cs_index);
        }
        else
        {
          status->bypass_process_ = ObServerStatus::LOADED;
        }
      }
      return ret;
    }

    int ObRootOperationHelper::report_tablets(const ObTabletReportInfoList& tablets,
        const int32_t server_index, const int64_t frozen_mem_version)
    {
      int ret = OB_SUCCESS;
      UNUSED(frozen_mem_version);
      if (process_ != REPORT_TABLET)
      {
        ret = OB_ERROR;
        TBSYS_LOG(WARN, "bypass not in report process. check the stat. process=%s", print_process());
      }
      else
      {
        ret = bypass_data_.report_tablets(tablets, server_index, frozen_mem_version);
      }
      return ret;
    }
    int ObRootOperationHelper::waiting_job_done(const int cs_index)
    {
      int ret = OB_SUCCESS;
      ObServerStatus* status = server_manager_->get_server_status(cs_index);
      if (NULL == status)
      {
        ret = OB_ERROR;
        TBSYS_LOG(WARN, "invalid status.  status=%p, cs_index=%d", status, cs_index);
      }
      else if (process_ != REPORT_TABLET)
      {
        ret = OB_ERROR;
        TBSYS_LOG(WARN, "bypass not in report process. check the stat. process=%s", print_process());
      }
      else
      {
        TBSYS_LOG(INFO, "cs finish report for new root table. cs_index=%d", cs_index);
        status->bypass_process_ = ObServerStatus::REPORTED;
      }
      return ret;
    }
    int ObRootOperationHelper::end_bypass_process()
    {
      int ret = OB_SUCCESS;
      delete_index_ = 0;
      done_count_ = 0;
      total_count_ = 0;
      start_time_us_ = 0;
      process_ = INIT;
      bypass_data_.reset_data();
      return ret;
    }

    int ObRootOperationHelper::set_delete_table(common::ObArray<uint64_t> &delete_tables)
    {
      int ret = OB_SUCCESS;
      if (delete_tables.count() <= 0)
      {
        ret = OB_INVALID_ARGUMENT;
        TBSYS_LOG(WARN, "invalid argument. table count=%ld", delete_tables.count());
      }
      else
      {
        bypass_data_.set_delete_table(delete_tables);
      }
      return ret;
    }
    int ObRootOperationHelper::check_load_sstable_process()
    {
      int ret = OB_SUCCESS;
      ObChunkServerManager::iterator it = server_manager_->begin();
      for (; it != server_manager_->end(); ++it)
      {
        if (it->status_ != ObServerStatus::STATUS_DEAD
            && it->bypass_process_ != ObServerStatus::LOADED)
        {
          TBSYS_LOG(TRACE, "cs have not finish load. server=%s", to_cstring(it->server_));
          ret = OB_ERROR;
          break;
        }
      }
      if (OB_SUCCESS != ret)
      {
        int64_t now = tbsys::CTimeUtil::getTime();
        int64_t timeout = config_->load_sstable_time;
        if (start_time_us_ + timeout < now)
        {
          TBSYS_LOG(WARN, "cs load sstable timeout. start_time=%ld, max_timeout=%ld, now=%ld",
              start_time_us_, timeout, now);
          ret = OB_BYPASS_TIMEOUT;
        }
      }
      return ret;
    }

    int ObRootOperationHelper::check_report_tablet_process()
    {
      int ret = OB_SUCCESS;
      ObChunkServerManager::iterator it = server_manager_->begin();
      for (; it != server_manager_->end(); ++it)
      {
        if (it->status_ != ObServerStatus::STATUS_DEAD
            && it->bypass_process_ != ObServerStatus::REPORTED)
        {
          TBSYS_LOG(TRACE, "cs have not report tablet. server=%s", to_cstring(it->server_));
          ret = OB_ERROR;
          break;
        }
      }
      if (OB_SUCCESS != ret)
      {
        int64_t now = tbsys::CTimeUtil::getTime();
        int64_t timeout = config_->report_tablet_time;
        if (start_time_us_ + config_->report_tablet_time < now)
        {
          TBSYS_LOG(WARN, "cs report tablet timeout. start_time=%ld, max_timeout=%ld, now=%ld",
              start_time_us_, timeout, now);
          ret = OB_BYPASS_TIMEOUT;
        }
      }
      return ret;
    }

    int ObRootOperationHelper::check_root_table()
    {
      int ret = OB_SUCCESS;
      common::ObTabletReportInfoList delete_list;
      ret = bypass_data_.check_root_table(delete_list);
      if (OB_SUCCESS == ret)
      {
        if (delete_list.get_tablet_size() > 0)
        {
          TBSYS_LOG(WARN, "cs report some useless tablet. please check it");
        }
        TBSYS_LOG(INFO, "check root table success.");
      }
      else
      {
        int64_t now = tbsys::CTimeUtil::getTime();
        int64_t timeout = config_->build_root_table_time;
        if (start_time_us_ + config_->build_root_table_time < now)
        {
          TBSYS_LOG(WARN, "cs report tablet timeout. start_time=%ld, max_timeout=%ld, now=%ld",
              start_time_us_, timeout, now);
          ret = OB_BYPASS_TIMEOUT;
        }
      }
      return ret;
    }

    int ObRootOperationHelper::check_process(OperationType &type)
    {
      int ret = OB_SUCCESS;
      type = bypass_data_.get_operation_type();
      //step1: check load process
      if (process_ == LOAD_SSTABLE)
      {
        ret = check_load_sstable_process();
        if (OB_SUCCESS == ret)
        {
          TBSYS_LOG(INFO, "check cs load sstable success. enter the next process:report tablet");
          process_ = REPORT_TABLET;
          start_time_us_ = tbsys::CTimeUtil::getTime();
          root_server_->request_cs_report_tablet();
        }
        else if (ret == OB_BYPASS_TIMEOUT)
        {
          TBSYS_LOG(WARN, "load sstable timeout. request cs report new tablet. try to build roottable, process:report tablet");
          process_ = REPORT_TABLET;
          start_time_us_ = tbsys::CTimeUtil::getTime();
          root_server_->request_cs_report_tablet();
        }
      }
      //step2: check report process
      if (REPORT_TABLET == process_)
      {
        ret = check_report_tablet_process();
        if (OB_SUCCESS == ret)
        {
          TBSYS_LOG(INFO, "check cs report tablet success.enter the next processi:sort root table");
          process_ = SORT_ROOT_TABLE;
          start_time_us_ = tbsys::CTimeUtil::getTime();
        }
      }
      //step3: check roottable integrity
      if (SORT_ROOT_TABLE == process_
          || (OB_BYPASS_TIMEOUT == ret && REPORT_TABLET == process_))
      {
        ret = check_root_table();
        if (OB_SUCCESS == ret)
        {
          TBSYS_LOG(INFO, "check root table success. notity switch root_table");
          ret = root_server_->bypass_meta_data_finished(type, bypass_data_.get_root_table(),
              bypass_data_.get_tablet_info_manager(), bypass_data_.get_schema_manager());
          if (OB_SUCCESS != ret)
          {
            TBSYS_LOG(WARN, "fail to finish bypass meta data. err=%d", ret);
            bypass_data_.destroy_data();
            end_bypass_process();
          }
          else if (type == IMPORT_TABLE)
          {
            TBSYS_LOG(INFO, "bypass process new root table ok, try to delete old tablets");
            start_time_us_ = tbsys::CTimeUtil::getTime();
            process_ = DELETE_TABLE;
          }
        }
        else if (OB_BYPASS_TIMEOUT == ret)
        {
          TBSYS_LOG(WARN, "check roottable timeout. end the bypass process");
          bypass_data_.destroy_data();
          end_bypass_process();
        }
      }
      //step4: check delete process
      if (DELETE_TABLE == process_)
      {
        ret = check_delete_tables_process();
        if (OB_SUCCESS == ret)
        {
          TBSYS_LOG(INFO, "check delete table success. finish the all process");
        }
      }
      if (OB_SUCCESS == ret)
      {
        TBSYS_LOG(INFO, "operation process is all finished.");
        end_bypass_process();
      }
      return ret;
    }
    ObRootTable2* ObRootOperationHelper::get_root_table()
    {
      return bypass_data_.get_root_table();
    }
    ObSchemaManagerV2* ObRootOperationHelper::get_schema_manager()
    {
      return bypass_data_.get_schema_manager();
    }
    ObRootOperationHelper::OperationProcess ObRootOperationHelper::get_process()
    {
      return process_;
    }
    void ObRootOperationHelper::set_process(OperationProcess process)
    {
      process_ = process;
    }
    const char* ObRootOperationHelper::print_process()
    {
      switch (process_)
      {
        case INIT:
          return "INIT";
        case STARTED:
          return "STARTED";
        case LOAD_SSTABLE:
          return "LOAD_SSTABLE";
        case REPORT_TABLET:
          return "REPORT_TABLET";
        case SORT_ROOT_TABLE:
          return "SORT_ROOT_TABLE";
        case DELETE_TABLE:
          return "DELETE_TABLE";
        default:
          return "error";
      }
    }
  }
}
