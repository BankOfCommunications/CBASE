/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Time-stamp: <2013-11-04 13:55:50 fufeng.syd>
 * Version: $Id$
 * Filename: ob_config_manager.cpp
 *
 * Authors:
 *   Yudi Shi <fufeng.syd@alipay.com>
 *
 */


#include "ob_tablet_backup_manager.h"
#include "common/ob_schema.h"
#include "ob_tablet_backup_runnable.h"
#include "common/location/ob_tablet_location_range_iterator.h"
#include "rootserver/ob_root_rpc_stub.h"

using namespace oceanbase::common;

namespace oceanbase
{
  namespace backupserver
  {
    bool TabletBackupTask::operator ==(const TabletBackupTask & src) const
    {
      bool ret = false;
      if (range_.equal2(src.range_))
      {
          ret = true;
      }
      return ret;
    }
    ObTabletBackupManager::ObTabletBackupManager(const common::ObSchemaManagerV2 & schema)
      :schema_mgr_(schema)
    {
      doing_task_idx = -1;
    }

    ObTabletBackupManager::~ObTabletBackupManager()
    {

    }

    const ObBackupServer * ObTabletBackupManager::get_backup_server() const
    {
      return backup_server_;
    }

    ObBackupServer * ObTabletBackupManager::get_backup_server()
    {
      return backup_server_;
    }

    int ObTabletBackupManager::print()
    {
      int ret = OB_SUCCESS;
      TBSYS_LOG(INFO, "dump the backup progress info start");
      bool do_print = false;
      if (backup_start_)
      {
        int64_t cur_time = tbsys::CTimeUtil::getTime();
        TBSYS_LOG(INFO, "the backup is working, dumping [%ld]tables, start_time[%ld], cost_time [%ld]",task_array_.count(),start_time_,cur_time- start_time_);
        do_print = true;
      }
      else if (start_time_ == 0)
      {
        ret = OB_OP_NOT_ALLOW;
        TBSYS_LOG(INFO, "the backup is not started, start_time");
      }
      else
      {
        ret = OB_ITER_END;
        do_print = true;
        TBSYS_LOG(INFO, "the backup is finished, dumped [%ld]tables, start_time[%ld], end_time[%ld], cost_time[%ld]",task_array_.count(),start_time_, end_time_, end_time_-start_time_);
      }

      if (do_print)
      {
        tbsys::CWLockGuard task_guard(task_thread_rwlock_);
        if (backup_start_)
        {
          TBSYS_LOG(INFO, "the [%ld]-th table is backup", doing_task_idx+1);
        }
        for (int64_t idx =0 ; idx <task_array_.count(); idx++)
        {
          if (task_array_.at(idx).status != BACKUP_WAITING && task_array_.at(idx).status != BACKUP_INIT)
          {
            TBSYS_LOG(INFO,"table_id[%ld],status[%s]",
                  task_array_.at(idx).table_id,
                      parse_backup_status(task_array_.at(idx).status));
            if (task_array_.at(idx).status == BACKUP_DOING)
            {
              TBSYS_LOG(INFO,"start_time[%ld]",task_array_.at(idx).start_time_);
            }
            else
            {
              TBSYS_LOG(INFO,"start_time[%ld], end_time[%ld], cost_time[%ld]",
                      task_array_.at(idx).start_time_,task_array_.at(idx).end_time_,task_array_.at(idx).end_time_-task_array_.at(idx).start_time_);

            }
          }

        }
      }
      TBSYS_LOG(INFO, "dump the backup progress info end");
      return ret;

    }


    int ObTabletBackupManager::initialize(ObBackupServer * server)
    {
      int ret = OB_SUCCESS;
      backup_server_ = server;

      root_rpc_ = new (std::nothrow) ObGeneralRootRpcProxy(backup_server_->get_config().retry_times,
        backup_server_->get_config().network_timeout, backup_server_->get_config().get_root_server());
      if (NULL == root_rpc_)
      {
        ret = OB_ALLOCATE_MEMORY_FAILED;
      }
      else
      {
        ret = root_rpc_->init(&(backup_server_->get_rpc_stub()));
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(WARN, "root rpc proxy init failed:ret[%d]", ret);
        }
      }

      location_cache_ = new(std::nothrow)common::ObTabletLocationCache;
      if (NULL == location_cache_)
      {
        ret = OB_ALLOCATE_MEMORY_FAILED;
      }
      else
      {
        ret = location_cache_->init(backup_server_->get_backup_config().location_cache_size,
          1024, backup_server_->get_backup_config().location_cache_timeout,
          backup_server_->get_backup_config().vtable_location_cache_timeout);
      }

      cache_proxy_ = new(std::nothrow)common::ObTabletLocationCacheProxy(backup_server_->get_self(),
        root_rpc_, location_cache_);
      if (NULL == cache_proxy_)
      {
        ret = OB_ALLOCATE_MEMORY_FAILED;
      }
      else
      {
        // init lock holder for concurrent inner table access
        ret = cache_proxy_->init(MAX_INNER_TABLE_COUNT, MAX_ROOT_SERVER_ACCESS_COUNT);
      }

      inited_ = true;
      return ret;
    }

    void ObTabletBackupManager::clear()
    {
      doing_task_idx = -1;
      start_time_ = 0;
      end_time_ = 0;
      min_done_tablet_task_idx = -1;
      max_doing_tablet_task_idx = -1;
      task_array_.clear();
      tablet_task_array_.clear();
      location_cache_->clear();
    }

    int ObTabletBackupManager::do_start_backup_task( const char * db_name, const char * table_name)
    {
      int ret = OB_SUCCESS;
      int64_t tablet_version = backup_server_->get_tablet_manager().get_serving_data_version();
      if(! inited_)
      {
        TBSYS_LOG(ERROR, "service not initialized, cannot accept any message.");
        ret = OB_NOT_INIT;
      }
      else if (backup_start_ == true)
      {
        TBSYS_LOG(WARN, "backup is already started");
        ret = OB_EAGAIN;
      }
      else if (tablet_version == 0 || tablet_version == backup_server_->get_ups_frozen_version())
      {
        clear();
        ObString dt;
        if ( db_name == NULL && table_name == NULL) //backup the whole database
        {
          dt = ObString::make_string("");
        }
        else if ( 0 == strcmp(db_name,"") && table_name != NULL) //inner table
        {
          char buf [MAX_NAME_LENGTH] = {0};
          dt.assign_buffer(buf,MAX_NAME_LENGTH);
          dt.clone(ObString::make_string(table_name));
        }
        else
        {
          char buf[MAX_NAME_LENGTH] = {0};
          dt.assign_buffer(buf,MAX_NAME_LENGTH);
          dt.concat(db_name,table_name);
        }
        ret = inner_fill_backup_task(dt);
      }
      else
      {
        ret = OB_SSTABLE_VERSION_UNEQUAL;
        TBSYS_LOG(ERROR, "data version is not matched, failed to backup the chunkserver data");

      }
      if (OB_SUCCESS == ret)
      {
        //finally set the backup task start
        //waiting for the backup thread to dispatch backup task

        //this for only one thread modify
        tbsys::CThreadGuard task_mutex_guard(&(task_thread_mutex_));
        tbsys::CWLockGuard task_guard(task_thread_rwlock_);
        start_time_= tbsys::CTimeUtil::getTime();
        backup_start_ = true;
        doing_task_idx ++;
      }
      else
      {
        backup_server_->report_sub_task_finish(OB_CHUNKSERVER,ret);
      }
      backup_thread_->wakeup();
      return ret;
    }
    int ObTabletBackupManager::do_abort_backup_task()
    {
      int ret = OB_SUCCESS;
      if (!inited_)
      {
        TBSYS_LOG(ERROR, "service not initialized, cannot accept any message.");
        ret = OB_NOT_INIT;
      }
      else if ( backup_start_ == false)
      {
        TBSYS_LOG(WARN, "backup service is not started, no backup task is aborted");
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        //this for only one thread modify
        tbsys::CThreadGuard task_mutex_guard(&(task_thread_mutex_));
        tbsys::CWLockGuard task_guard(task_thread_rwlock_);
        for (int64_t idx = doing_task_idx; idx <task_array_.count(); idx ++)
        {
          task_array_.at(idx).status = BACKUP_CANCELLED;
        }
      }
      backup_thread_->wakeup();
      return ret;
    }

    int ObTabletBackupManager::do_tablet_backup_over(int code, ObDataSourceDesc & desc)
    {
      int ret = OB_SUCCESS;
      if (!inited_)
      {
        TBSYS_LOG(ERROR, "service not initialized, cannot accept any message.");
        ret = OB_NOT_INIT;
      }
      tbsys::CThreadGuard task_mutex_guard(&(task_thread_mutex_));
      tbsys::CWLockGuard task_guard(task_thread_rwlock_);
      if (desc.range_.table_id_ < static_cast<uint64_t>(task_array_.at(doing_task_idx).table_id)
          ||task_array_.at(doing_task_idx).status == BACKUP_CANCELLED
          || task_array_.at(doing_task_idx).status == BACKUP_ERROR
          || task_array_.at(doing_task_idx).status == BACKUP_DONE)
      {
        TBSYS_LOG(WARN, "abandon the msg, tablet info [%s]",to_cstring(desc.range_));
      }
      else
      {
        //this for only one thread modify
        tbsys::CThreadGuard mutex_guard(&(tablet_task_thread_mutex_));
        tbsys::CWLockGuard guard(tablet_task_thread_rwlock_);

        int64_t idx = 0;
        TabletBackupTask task;
        task.range_ = desc.range_;
        for (idx = min_done_tablet_task_idx+1; idx <= max_doing_tablet_task_idx; idx++)
        {
          if (tablet_task_array_.at(idx) == task) break;
        }
        if (idx > max_doing_tablet_task_idx)
        {
          TBSYS_LOG(WARN, "abandon the msg, tablet info [%s]",to_cstring(desc.range_));
        }
        else if (OB_SUCCESS == code)
        {
          tablet_task_array_.at(idx).status_ = BACKUP_DONE;
          while(min_done_tablet_task_idx + 1 <= max_doing_tablet_task_idx)
          {
            if (tablet_task_array_.at(min_done_tablet_task_idx + 1).status_ == BACKUP_DOING
                || tablet_task_array_.at(min_done_tablet_task_idx + 1).status_ == BACKUP_WAITING)
              break;
            else
            {
              __sync_fetch_and_add(&min_done_tablet_task_idx,1);
            }
          }
          if (min_done_tablet_task_idx >= tablet_task_array_.count()-1)
          {
            task_array_.at(doing_task_idx).status = BACKUP_DONE;
            task_array_.at(doing_task_idx).end_time_ = tbsys::CTimeUtil::getTime();
            TBSYS_LOG(INFO, "set task_array.at(%ld) backup finish", doing_task_idx);
          }
        }
        else if (OB_EAGAIN == code)
        {
          tablet_task_array_.at(idx).status_ = BACKUP_WAITING;
        }
        else
        {
          TBSYS_LOG(WARN, "failed to migrate tablet, ret[%d]", code);
          tablet_task_array_.at(idx).status_ = BACKUP_WAITING;
          __sync_fetch_and_add(&(tablet_task_array_.at(idx).cur_replica_cs_),1);
        }
      }
      backup_thread_->wakeup();
      return ret;
    }

    int ObTabletBackupManager::do_dispatch_backup_task()
    {
      int ret = OB_SUCCESS;
      if (!inited_)
      {
        TBSYS_LOG(ERROR, "service not initialized, cannot accept any message.");
        ret = OB_NOT_INIT;
      }
      else if ( backup_start_ == false)
      {
        TBSYS_LOG(ERROR, "backup service is not started, cannot dispatch backup task");
        ret = OB_INNER_STAT_ERROR;
      }
      if (OB_SUCCESS == ret)
      {
        //this for only one thread modify
        tbsys::CThreadGuard task_mutex_guard(&(task_thread_mutex_));
        tbsys::CWLockGuard task_guard(task_thread_rwlock_);

        BACKUP_STATUS status =  task_array_.at(doing_task_idx).status;
        if (status == BACKUP_WAITING)// the first round
        {
          task_array_.at(doing_task_idx).start_time_= tbsys::CTimeUtil::getTime();
          ret = generate_tablet_backup_task();
          if (ret == OB_SUCCESS || ret == OB_CANCELED)
          {
            TBSYS_LOG(INFO,"generate the backup task [doing_task_idx:%ld], ret [%d]", doing_task_idx, ret);
          }
          else
          {
            task_array_.at(doing_task_idx).status = BACKUP_ERROR;
            task_array_.at(doing_task_idx).end_time_= tbsys::CTimeUtil::getTime();
            TBSYS_LOG(INFO,"failed to generate the backup task [ret:%d]", ret);
          }
        }
        else if (status == BACKUP_ERROR || status == BACKUP_DONE || status == BACKUP_CANCELLED)// sub tablet_backup is finished
        {
          doing_task_idx ++; //start the table batch-backup task
          if (doing_task_idx == task_array_.count())
          {
            backup_start_ = false; //set the whole backup is finished
            end_time_ = tbsys::CTimeUtil::getTime();
            print();
            TBSYS_LOG(INFO,"the backup task is finished [task number:%ld]", task_array_.count());
            ret = OB_SUCCESS;

            //report the backup info
            int err = OB_SUCCESS;
            int64_t idx = 0;
            while (idx < doing_task_idx)
            {
              if (task_array_.at(idx).status != BACKUP_DONE) break;
              idx ++;
            }
            if (idx < task_array_.count())
            {
              err = OB_ERROR;
            }
            backup_server_->report_sub_task_finish(OB_CHUNKSERVER,err);
          }
          else
          {
            task_array_.at(doing_task_idx).start_time_= tbsys::CTimeUtil::getTime();
            ret = generate_tablet_backup_task();
            if (ret == OB_SUCCESS)
            {
              TBSYS_LOG(INFO,"generate the backup task [doing_task_idx:%ld], ret[%d]", doing_task_idx, ret);
            }
            else if (ret == OB_CANCELED)
            {
              task_array_.at(doing_task_idx).end_time_= tbsys::CTimeUtil::getTime();
              TBSYS_LOG(INFO,"generate the backup task [doing_task_idx:%ld], ret[%d]", doing_task_idx, ret);
            }
            else
            {
              task_array_.at(doing_task_idx).end_time_= tbsys::CTimeUtil::getTime();
              task_array_.at(doing_task_idx).status = BACKUP_ERROR;
              TBSYS_LOG(INFO,"failed to generate the backup task [doing_task_idx:%ld],[ret:%d]",doing_task_idx,ret);
            }
          }
        }
        else if (status == BACKUP_DOING)
        {
          int32_t count;
          ret = async_do_tablet_backup_task(count);
          if (OB_SUCCESS == ret)
          {
            TBSYS_LOG(INFO,"send tablet backup task [count:%d] in this round", count);
          }
          else if(OB_ITER_END == ret)
          {
            TBSYS_LOG(ERROR,"send tablet backup task [ret:%d] in this round", ret);
            task_array_.at(doing_task_idx).status = BACKUP_ERROR;
            task_array_.at(doing_task_idx).end_time_= tbsys::CTimeUtil::getTime();
          }
          else
          {
            TBSYS_LOG(ERROR,"send tablet backup task [ret:%d] in this round", ret);
          }
        }
        else
        {
          TBSYS_LOG(ERROR,"should not be here");
        }
      }
      return ret;
    }

  int ObTabletBackupManager::generate_tablet_backup_task()
  {
    int ret = OB_SUCCESS;
    int64_t table_id = 0;
    int status = BACKUP_INIT;
    {
      //this for only one thread modify
      tbsys::CThreadGuard mutex_guard(&(tablet_task_thread_mutex_));
      tbsys::CWLockGuard guard(tablet_task_thread_rwlock_);

      table_id = task_array_.at(doing_task_idx).table_id;
      status =  task_array_.at(doing_task_idx).status;
      if (status == BACKUP_CANCELLED)
      {
        ret = OB_CANCELED;
        TBSYS_LOG(INFO,"the backup task has been cancelled [table_id:%ld]", table_id);
      }
      else if (status != BACKUP_WAITING)
      {
        ret = OB_INNER_STAT_ERROR;
      }
      else
      {
        tablet_task_array_.clear();
        min_done_tablet_task_idx = -1;
        max_doing_tablet_task_idx = -1;
        task_array_.at(doing_task_idx).status = BACKUP_DOING;
      }
    }

    if (ret == OB_SUCCESS)
    {
      ObNewRange backup_range;
      backup_range.table_id_ = table_id;
      backup_range.set_whole_range();
      ObNewRange query_range;

      int32_t replica_count = ObTabletLocationList::MAX_REPLICA_COUNT;
      ObChunkServerItem replicas[ObTabletLocationList::MAX_REPLICA_COUNT];
      do
      {
        ObTabletLocationRangeIterator range_iter;
        if ((OB_SUCCESS == ret)
            && (OB_SUCCESS != (ret = range_iter.initialize(cache_proxy_,
                                                     &backup_range, ScanFlag::FORWARD,&buffer_pool_))))
        {
          TBSYS_LOG(WARN,"fail to initialize range iterator [ret:%d]", ret);
        }
        if ((OB_SUCCESS == ret)
            && (OB_SUCCESS != (ret = range_iter.next(replicas,replica_count,query_range)))
            && (OB_ITER_END != ret))
        {
          TBSYS_LOG(WARN,"fail to find replicas [ret:%d]", ret);
        }
        if (OB_ITER_END == ret)
        {
          TBSYS_LOG(WARN,"fail to find replicas, while retry [ret:%d]", ret);
          ret = OB_ERR_UNEXPECTED;
        }
        if ((OB_SUCCESS == ret) && (replica_count <= 0))
        {
          TBSYS_LOG(WARN,"fail to find replicas, while retry [ret:%d,replica_count:%d]", ret, replica_count);
          ret = OB_DATA_NOT_SERVE;
        }
        else
        {
          TBSYS_LOG(INFO,"succeed to find replicas, [ret:%d,replica_count:%d]", ret, replica_count);
        }
        backup_range.start_key_ = query_range.end_key_;

        if(OB_SUCCESS == ret && replica_count > 0)
        {
          TabletBackupTask new_task;
          new_task.replica_count_ = replica_count;
          new_task.cur_replica_cs_ = 0;
          new_task.retry_time_ = 0;
          new_task.start_time_ = 0;
          memcpy(new_task.replica_,replicas,sizeof(ObChunkServerItem)*(ObTabletLocationList::MAX_REPLICA_COUNT));
          new_task.range_ = query_range;
          new_task.status_ = BACKUP_WAITING;
          tablet_task_array_.push_back(new_task);
        }

      }while (!backup_range.start_key_.is_max_row() && ret == OB_SUCCESS);

      for (int64_t idx =0 ; idx <tablet_task_array_.count();idx ++)
      {
        TBSYS_LOG(INFO," tablet_task_array_[%ld], rowkey [%s]", idx,to_cstring(tablet_task_array_.at(idx).range_));
        for (int64_t tmp = 0; tmp < tablet_task_array_.at(idx).replica_count_; tmp ++)
        {
          TBSYS_LOG(INFO," replica [%ld], chunkserver info [%s]",
                  tmp,to_cstring(tablet_task_array_.at(idx).replica_[tmp]));
        }
      }
      if (ret != OB_SUCCESS)
      {
        tablet_task_array_.clear();
      }
      if (ret == OB_SCHEMA_ERROR || ret == OB_ERROR_OUT_OF_RANGE)           // maybe table is dropped
      {
        ret = OB_SUCCESS;
      }
    }

    return ret;


  }

  int ObTabletBackupManager::async_do_tablet_backup_task( int32_t & count)
  {
    int ret= OB_SUCCESS;
    int64_t migrate_timeout = backup_server_->get_backup_config().tablet_backup_timeout;
    int64_t retry_time = backup_server_->get_backup_config().tablet_backup_retry_times;
    count = 0;
    {
      //this for only one thread modify
      tbsys::CThreadGuard mutex_guard(&(tablet_task_thread_mutex_));
      tbsys::CWLockGuard guard(tablet_task_thread_rwlock_);
      max_doing_tablet_task_idx = min(min_done_tablet_task_idx + MAX_COCURRENT_BACKUP_TASK,
                                    tablet_task_array_.count()-1);
      TBSYS_LOG(INFO," async do tablet_backup_task min[%ld],max[%ld]",
            min_done_tablet_task_idx , max_doing_tablet_task_idx);
      for (int64_t idx = min_done_tablet_task_idx +1; idx <= max_doing_tablet_task_idx; idx++)
      {
        int64_t cur_time = tbsys::CTimeUtil::getTime();
        TabletBackupTask & task = tablet_task_array_.at(idx);
        if ((task.status_ == BACKUP_WAITING && task.cur_replica_cs_ < task.replica_count_)
            || (task.status_ == BACKUP_DOING && cur_time - task.start_time_ > migrate_timeout))
        {
          if (task.status_ == BACKUP_DOING && task.retry_time_ > retry_time)
          {
            task.status_ = BACKUP_WAITING;
            task.cur_replica_cs_++;
            TBSYS_LOG(INFO," task.cur_replica_cs_[%d]",task.cur_replica_cs_);
          }
          else
          {
            TBSYS_LOG(INFO," async do tablet_backup_task, table_range[%s]",
                to_cstring(task.range_));
            ObDataSourceDesc data_source_desc;
            data_source_desc.type_ = common::ObDataSourceDesc::OCEANBASE_OUT;
            data_source_desc.range_ = task.range_;
            data_source_desc.src_server_ = task.replica_[task.cur_replica_cs_].addr_;
            data_source_desc.sstable_version_ = 0; // cs will fill this version
            data_source_desc.tablet_version_ = backup_server_->get_ups_frozen_version();
            data_source_desc.keep_source_ = true;

            ObServer dest = backup_server_->get_self();
            ret = backup_server_->get_root_rpc_stub().migrate_tablet(
              dest,data_source_desc, backup_server_->get_config().network_timeout);
            if (OB_SUCCESS == ret)
            {
              TBSYS_LOG(INFO, "migrate tablet: type=%s, tablet=%s tablet_version=%ld src=%s dest=%s keep_src=%c",
                    data_source_desc.get_type(), to_cstring(data_source_desc.range_), data_source_desc.tablet_version_,
                    to_cstring(data_source_desc.src_server_), to_cstring(dest), data_source_desc.keep_source_?'Y':'N');
            }
            else
            {
              TBSYS_LOG(WARN, "failed to send migrate tablet: err=%d: type=%s, tablet=%s tablet_version=%ld src=%s dest=%s keep_src=%c",
              ret, data_source_desc.get_type(), to_cstring(data_source_desc.range_), data_source_desc.tablet_version_,
              to_cstring(data_source_desc.src_server_), to_cstring(dest), data_source_desc.keep_source_?'Y':'N');
            }
            if (ret == OB_SUCCESS && task.status_ == BACKUP_WAITING)
            {
              task.start_time_ = tbsys::CTimeUtil::getTime();
              task.status_ = BACKUP_DOING;
              task.retry_time_ = 0;
            }
            else if (ret == OB_SUCCESS && task.status_ == BACKUP_DOING)
            {
              task.start_time_ = tbsys::CTimeUtil::getTime();
              task.retry_time_ ++;
            }
          }
        }
        else if ((task.status_ == BACKUP_DOING && cur_time - task.start_time_ < migrate_timeout)
                 || task.status_ == BACKUP_DONE)
        {

        }
        else
        {
          TBSYS_LOG(INFO," task.retry_time[%ld]",task.retry_time_);
          TBSYS_LOG(INFO," task.cost_time[%ld],timeout[%ld]",cur_time - task.start_time_,migrate_timeout);
          TBSYS_LOG(INFO," task.cur_replicas_cs[%d],task.replicas_count[%d]",task.cur_replica_cs_,task.replica_count_);
          TBSYS_LOG(INFO," task.status[%d]",task.status_);
          task.status_ = BACKUP_ERROR;
          ret = OB_ITER_END;
          break;
        }
        count ++;

      }//end for
    }
    return ret;
  }

  int ObTabletBackupManager::inner_fill_backup_task(ObString & dt)
  {
    int ret= OB_SUCCESS;
    const ObTableSchema * table_schema = NULL;
    //this for only one thread modify
    tbsys::CThreadGuard mutex_guard(&(tablet_task_thread_mutex_));
    tbsys::CWLockGuard guard(tablet_task_thread_rwlock_);
    if ( 0 == dt.size())
    {
      TBSYS_LOG(INFO, "backup the whole database");
      ret = OB_ENTRY_NOT_EXIST;
      for (uint64_t tid= 1; tid <=schema_mgr_.get_max_table_id(); tid++)
      {
        if(tid == OB_ALL_SERVER_STAT_TID || tid == OB_ALL_SERVER_SESSION_TID || tid == OB_ALL_STATEMENT_TID)
          continue;
        if (NULL != (table_schema = schema_mgr_.get_table_schema(tid)))
        {
          //mod zhaoqiong [backup bug fixed for abnormal index table] 20151103:b
          if (table_schema->get_index_helper().tbl_tid == OB_INVALID_ID ||
              ((table_schema->get_index_helper().tbl_tid != OB_INVALID_ID)&&(table_schema->get_index_helper().status == common::AVALIBALE)))
          {
            TableBackupTask task;
            memcpy(task.table_name,table_schema->get_table_name(),MAX_NAME_LENGTH);
            task.table_id = table_schema->get_table_id();
            task.status = BACKUP_WAITING;
            task.start_time_ =0;
            task.end_time_ = 0;
            task_array_.push_back(task);
            TBSYS_LOG(INFO, "the table task [%s] is added ",task.table_name);
            ret = OB_SUCCESS;
          }
          //mod:e
        }
      }
    }
    else if (NULL != (table_schema = schema_mgr_.get_table_schema(dt)))
    {
      //mod zhaoqiong [backup bug fixed for abnormal index table] 20151103:b
      if (table_schema->get_index_helper().tbl_tid == OB_INVALID_ID ||
          ((table_schema->get_index_helper().tbl_tid != OB_INVALID_ID)&&(table_schema->get_index_helper().status == common::AVALIBALE)))
      {
        TableBackupTask task;
        memcpy(task.table_name,dt.ptr(),MAX_NAME_LENGTH);
        task.table_id = table_schema->get_table_id();
        task.status = BACKUP_WAITING;
        task.start_time_ =0;
        task.end_time_ = 0;
        task_array_.push_back(task);
        TBSYS_LOG(INFO, "the table task [%s] is added ",task.table_name);
      }
      //mod:e
    }
    else
    {
      ret = OB_ENTRY_NOT_EXIST;
      TBSYS_LOG(INFO, "the table [%.*s] is not existed ",dt.length(),dt.ptr());
    }

    return ret;

  }
  }
}
