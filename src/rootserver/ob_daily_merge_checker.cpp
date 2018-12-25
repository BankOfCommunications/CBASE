/*
 * Copyright (C) 2007-2012 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * Description here
 *
 * Version: $Id$
 *
 * Authors:
 *   zhidong <xielun.szd@taobao.com>
 *     - some work details here
 */

#include "ob_root_server2.h"
#include "ob_daily_merge_checker.h"

using namespace oceanbase::common;
using namespace oceanbase::rootserver;


DEFINE_SERIALIZE(ObRowChecksum)
{
  int ret = OB_SUCCESS;
  ret = serialization::encode_vi64(buf, buf_len, pos, data_version_);

  if(OB_SUCCESS == ret)
    ret = serialization::encode_vi64(buf, buf_len, pos, rc_array_.count());

  for(int64_t i = 0; i < rc_array_.count() && OB_SUCCESS == ret; i++)
  {
    ret = rc_array_[i].serialize(buf, buf_len, pos);
  }

  return ret;
}

DEFINE_DESERIALIZE(ObRowChecksum)
{
  int ret = OB_SUCCESS;
  int64_t count = 0;
  ret = serialization::decode_vi64(buf, data_len, pos, &data_version_);

  if(OB_SUCCESS == ret)
    ret = serialization::decode_vi64(buf, data_len, pos, &count);


  ObRowChecksumInfo rc;
  for(int64_t i = 0; i < count; i++)
  {
    rc.reset();
    ret = rc.deserialize(buf, data_len, pos);
    if(OB_SUCCESS != ret)
      break;

    rc_array_.push_back(rc);
  }

  return ret;
}


bool ObRowChecksum::is_equal(const ObRowChecksum &slave) const
{
  bool ret = true;
  if(data_version_ != slave.data_version_)
  {
    TBSYS_LOG(ERROR, "unequal data_version, master_data_version:%ld slave_data_version:%ld", data_version_, slave.data_version_);
    ret = false;
  }
  else
  {
    if(rc_array_.count() != slave.rc_array_.count())
    {
      TBSYS_LOG(ERROR, "unequal table num, master_table_num:%ld, slave_table_num:%ld", rc_array_.count(), slave.rc_array_.count());
      ret = false;
    }

    int64_t i = 0;
    int64_t j = 0;
    for(; i < rc_array_.count(); i++)
    {
      for(j = 0; j < slave.rc_array_.count(); j++)
      {
        if(rc_array_[i].table_id_ == slave.rc_array_[j].table_id_)
        {
          if(rc_array_[i].row_checksum_ != slave.rc_array_[j].row_checksum_)
          {
            TBSYS_LOG(ERROR, "unequal table row_checksum, table_id=%ld, master_checksum=%ld, slave_checksum=%ld", rc_array_[i].table_id_, rc_array_[i].row_checksum_, slave.rc_array_[j].row_checksum_);
            ret = false;
          }
          break;
        }
        else
        {
          continue;
        }
      }

      if(j == slave.rc_array_.count())
      {
        TBSYS_LOG(ERROR, "unequal table num, master table_id:%ld not found in slave", rc_array_[i].table_id_);
        ret = false;
      }
    }
  }

  return ret;
}

DEFINE_GET_SERIALIZE_SIZE(ObRowChecksum)
{
  int64_t total_size = 0;
  total_size += serialization::encoded_length_vi64(data_version_);
  total_size += serialization::encoded_length_vi64(rc_array_.count());

  for(int64_t i = 0; i < rc_array_.count(); i++)
  {
    total_size += rc_array_[i].get_serialize_size();
  }
  return total_size;
}

int64_t ObRowChecksum::to_string(char* buffer, const int64_t length) const
{
  int64_t pos = 0;

  for(int64_t i = 0; i < rc_array_.count(); i++)
  {
    databuff_printf(buffer, length, pos, "table_id=%lu, row_checksum=%lu\n", rc_array_[i].table_id_, rc_array_[i].row_checksum_);
  }
  return pos;
}


DEFINE_SERIALIZE(ObRowChecksumInfo)
{
  int ret = OB_SUCCESS;
  ret = serialization::encode_vi64(buf, buf_len, pos, table_id_);

  if(OB_SUCCESS == ret)
    ret = serialization::encode_vi64(buf, buf_len, pos, row_checksum_);

  return ret;
}

DEFINE_DESERIALIZE(ObRowChecksumInfo)
{
  int ret = OB_SUCCESS;
  ret = serialization::decode_vi64(buf, data_len, pos, reinterpret_cast<int64_t*>(&table_id_));

  if(OB_SUCCESS == ret)
    ret = serialization::decode_vi64(buf, data_len, pos, reinterpret_cast<int64_t*>(&row_checksum_));

  return ret;
}

DEFINE_GET_SERIALIZE_SIZE(ObRowChecksumInfo)
{
  int64_t total_size = 0;
  total_size += serialization::encoded_length_vi64(table_id_);
  total_size += serialization::encoded_length_vi64(row_checksum_);

  return total_size;
}

bool ObRowChecksumInfo::operator == (const ObRowChecksumInfo &other) const
{
  return other.table_id_ == table_id_ && other.row_checksum_ == row_checksum_;
}

int64_t ObRowChecksumInfo::to_string(char* buffer, const int64_t length) const
{
  int64_t pos = 0;
  databuff_printf(buffer, length, pos, "table_id=%lu, row_checksum=%lu", table_id_, row_checksum_);

  return pos;
}

ObDailyMergeChecker::ObDailyMergeChecker(ObRootServer2 * root_server) : root_server_(root_server), check_task_(this, root_server)
{
  OB_ASSERT(root_server != NULL);
}

ObDailyMergeChecker::~ObDailyMergeChecker()
{
}

void ObDailyMergeChecker::MonitorRowChecksumTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  ObServer master_rs;
  const ObRootServerConfig& config = root_server_->get_config();
  //mod pangtianze [Paxos rs_election] 20150630:b
  //master_rs.set_ipv4_addr(config.master_root_server_ip, (int32_t)config.master_root_server_port);
  ret = root_server_->get_rs_master(master_rs);
  //mod:e

  master_.reset();
  slave_.reset();

  const ObiRole& obi_role = root_server_->get_obi_role();
  if (obi_role.get_role() == ObiRole::SLAVE &&
      root_server_->get_config().monitor_row_checksum)
  {
    //step1 get frozen version
    int64_t frozen_version = root_server_->get_last_frozen_version();

    //step2 get local row_checksum
    ret = root_server_->get_row_checksum(frozen_version, OB_INVALID_ID, slave_);
    if(OB_SUCCESS != ret && OB_ENTRY_NOT_EXIST != ret)
    {
      TBSYS_LOG(WARN, "slave cluster get row_checksum failed ret:%d", ret);
    }
    else if(OB_ENTRY_NOT_EXIST == ret)
    {
      TBSYS_LOG(INFO, "slave cluster get row_checksum failed, is merging");
    }

    //step3 get row_checksum from master cluster
    if(OB_SUCCESS == ret)
    {
      ret = root_server_->get_rpc_stub().get_row_checksum(master_rs, frozen_version, OB_INVALID_ID, master_, config.monitor_row_checksum_timeout);
      if(OB_SUCCESS != ret && OB_ENTRY_NOT_EXIST != ret)
      {
        TBSYS_LOG(WARN, "master cluster get row_checksum failed, ret:%d", ret);
      }
      else if(OB_ENTRY_NOT_EXIST == ret)
      {
        TBSYS_LOG(INFO, "master cluster get row_checksum failed, is merging");
      }
    }

    if(OB_SUCCESS == ret)
    {
      //step4 compare master cluster and slave cluster row_checksum
      if(master_.is_equal(slave_))
      {
        TBSYS_LOG(INFO, "==========> check row_checksum OKay <==========");
      }
      else
      {
        TBSYS_LOG(WARN, "==========> check row_checksum failed! <==========");
      }

      TBSYS_LOG(INFO, "==========> master row_checksum <==========\n%s", to_cstring(master_));
      TBSYS_LOG(INFO, "==========> slave row_checksum <==========\n%s", to_cstring(slave_));
    }
  }
  else
  {
    TBSYS_LOG(INFO, "do not check row_checksum now, for obi_role:%s or switch:%s", obi_role.get_role_str(), config.monitor_row_checksum.str());
  }


  if(OB_SUCCESS != (ret = merge_checker_->timer_.schedule(*this,
          root_server_->get_config().monitor_row_checksum_interval, false)))
  {
    TBSYS_LOG(ERROR, "timer schedule fail, ret: [%d]", ret);
  }
}

void ObDailyMergeChecker::run(tbsys::CThread * thread, void * arg)
{
  UNUSED(thread);
  UNUSED(arg);
  int err = OB_SUCCESS;
  TBSYS_LOG(INFO, "[NOTICE] merge checker thread start");
  bool did_get_last_frozen_version = true;
  int64_t last_check_timestamp = 0;

  //start the monitor row checksum task
  if(OB_SUCCESS != (err = timer_.init()))
  {
    TBSYS_LOG(ERROR, "init timer fail, ret: [%d]", err);
  }
  else if(OB_SUCCESS != (err = timer_.schedule(check_task_,
          root_server_->get_config().monitor_row_checksum_interval, false)))
  {
    TBSYS_LOG(ERROR, "timer schedule fail, ret: [%d]", err);
  }

  while (!_stop)
  {
    if (root_server_->is_master()) //add pangtianze [Paxos rs_election] 20170629:b
    {
    if (did_get_last_frozen_version)
    {
      ObServer ups = root_server_->get_update_server_info(false);
      if (0 == ups.get_ipv4())
      {
        TBSYS_LOG(INFO, "no ups right now, sleep for next round");
        sleep(1);
        continue;
      }
      else if (OB_SUCCESS != (err = root_server_->get_last_frozen_version_from_ups(false)))
      {
        TBSYS_LOG(WARN, "failed to get frozen version, err=%d ups=%s", err, ups.to_cstring());
      }
      else
      {
        did_get_last_frozen_version = false;
      }
    }
    if (root_server_->is_master() && root_server_->is_daily_merge_tablet_error())
    {
      TBSYS_LOG(ERROR, "daily merge error!!, message info is: %s", root_server_->get_daily_merge_error_msg());
    }
    else
    {
      int64_t now = tbsys::CTimeUtil::getTime();
      int64_t max_merge_duration_us = root_server_->config_.max_merge_duration_time;
      if (root_server_->is_master() && (root_server_->last_frozen_time_ > 0))
      {
        /*//add liumz, [secondary index static_index_build] 20150629:b
        if (root_server_->get_obi_role().get_role() == ObiRole::MASTER)
        {
          if (OB_SUCCESS != (err = root_server_->modify_init_index()))
          {
            TBSYS_LOG(WARN, "fail to modify init index status. ret=%d", err);
          }
        }
        //add:e*/
        // check all tablet merged finish and root table is integrated
        int64_t frozen_version = root_server_->get_last_frozen_version();
        if (!root_server_->check_all_tablet_safe_merged())
        {
          //mod liumz, [secondary_index:don't delete dropped tables when building index]20160328:b
          //if (now > last_check_timestamp + root_server_->get_config().monitor_drop_table_interval)
          if (now > last_check_timestamp + root_server_->get_config().monitor_drop_table_interval && root_server_->check_create_index_over())
          //mod:e
          {
            // delete the dropped tablet from root table
            int64_t count = 0;
            err = root_server_->delete_dropped_tables(count);
            if (count > 0)
            {
              TBSYS_LOG(WARN, "delete dropped tables' tablets in root table:count[%ld], err[%d]", count, err);
            }
            last_check_timestamp = now;
          }
          // the first time not write this error log
          if (root_server_->last_frozen_time_ != 1)
          {
            if (
                //del lbzhong [Paxos Cluster.Balance] 20160704:b
                ///this if branch is not used, so delete
                // !root_server_->is_loading_data() &&
                //del:e
                max_merge_duration_us + root_server_->last_frozen_time_ < now)
            {
              TBSYS_LOG(ERROR, "merge is too slow, start at:%ld, now:%ld, used_time:%ld, max_merge_duration_:%ld",
                  root_server_->last_frozen_time_, now, now - root_server_->last_frozen_time_, max_merge_duration_us);
            }
          }
        }
        else
        {
          // write the log for qa & dba
          TBSYS_LOG(INFO, "build new root table ok:last_version[%ld]", frozen_version);
          root_server_->last_frozen_time_ = 0;
          // checkpointing after done merge
          root_server_->make_checkpointing();

          //add liuxiao [secondary index col checksum]20150526          
          //mod liumz, bugfix: [alloc memory for ObSchemaManagerV2 in heap, not on stack]20150702:b
          //ObSchemaManagerV2 out_schema;
          ObSchemaManagerV2* out_schema = OB_NEW(ObSchemaManagerV2, ObModIds::OB_RS_SCHEMA_MANAGER);
          //mod:e
          int index_stat_ret = OB_SUCCESS;
          ObArray<uint64_t> index_id_list;
          //add liumz, bugfix: [alloc memory for ObSchemaManagerV2 in heap, not on stack]20150702:b
          if (NULL == out_schema)
          {
            err = OB_ALLOCATE_MEMORY_FAILED;
            TBSYS_LOG(WARN, "fail to new schema_manager. ret=%d", err);
          }//add:e
          else if(OB_SUCCESS != (index_stat_ret = root_server_->get_schema(false,false,*out_schema)))
          {
            TBSYS_LOG(WARN,"failed to get schema to get index schema");
          }
          else if(OB_SUCCESS != (index_stat_ret = out_schema->get_all_avaiable_index_list(index_id_list)))
          {
            TBSYS_LOG(WARN,"failed to get index list");
          }
          else if (root_server_->config_.monitor_column_checksum)
          {
            for(int64_t i=0;i < index_id_list.count();i++)
            {
              //mod liumz, [paxos static index]20170607:b
              bool is_cluster_alive[OB_CLUSTER_ARRAY_LEN];
              root_server_->get_alive_cluster_with_cs(is_cluster_alive);
              for (int cluster_idx = 0; cluster_idx < OB_MAX_COPY_COUNT && OB_SUCCESS == err; cluster_idx++)
              {
                if (is_cluster_alive[cluster_idx+1])
                {
                  //mod liumz, [secondary index col checksum bugfix]20160622:b
                  bool is_right = true;
                  TBSYS_LOG(INFO, "check index table tid:%ld", index_id_list.at(i));
                  if(OB_SUCCESS != (err = root_server_->check_column_checksum(cluster_idx+1, index_id_list.at(i), is_right)))
                  {
                    TBSYS_LOG(ERROR, "check column checksum failed, index id[%ld], cluster_id[%d]", index_id_list.at(i), cluster_idx+1);
                  }
                  if (!is_right)
                  {
                    if(OB_SUCCESS != (err = root_server_->modify_index_process_info(cluster_idx+1, index_id_list.at(i), ERROR)))
                    {
                      TBSYS_LOG(WARN, "failed to change idx table status:%ld, cluster_id[%d]", index_id_list.at(i), cluster_idx+1);
                    }
                  }
                  //mod:e
                }
              }
              //add:e
            }
          }
          //add liumz, bugfix: [alloc memory for ObSchemaManagerV2 in heap, not on stack]20150702:b
          if (out_schema != NULL)
          {
            OB_DELETE(ObSchemaManagerV2, ObModIds::OB_RS_SCHEMA_MANAGER, out_schema);
          }
          //add:e
          //add e
          //add liuxiao [secondary index col checksum]20150526
          //清理内部表的旧列校验和
          if(root_server_->get_obi_role().get_role() == ObiRole::MASTER && root_server_->get_last_frozen_version() > 3)
          {
            int clean_ret = OB_SUCCESS;
              if(OB_SUCCESS != (clean_ret = root_server_->clean_old_checksum_v2(root_server_->get_last_frozen_version() - 1)))
              {
                TBSYS_LOG(WARN, "clean __all_cchecksum_info failed  %d", clean_ret);
              }
              else
              {
                TBSYS_LOG(INFO, "clean __all_cchecksum_info success");
              }
          }
          //add e

          //add liumz, [secondary index static_index_build] 20150629:b
          if (root_server_->get_obi_role().get_role() == ObiRole::MASTER)
          {
            if (OB_SUCCESS != root_server_->modify_index_stat_amd())
            {
              TBSYS_LOG(WARN, "fail to modify index status after merge done. ret=%d", err);
            }
          }
          //add:e
        }
      }
      //add liumz, [secondary index static_index_build] 20150529:b
      else if (root_server_->is_master() && (root_server_->last_frozen_time_ == 0))
      {        
        //not_available -> available in __index_process_info, depend on safe copy
        if (OB_SUCCESS != (err = root_server_->modify_staging_index_process_info()))
        {
          TBSYS_LOG(WARN, "fail to modify staging index status in __index_process_info. ret=%d", err);
        }
        if (root_server_->get_obi_role().get_role() == ObiRole::MASTER)
        {
          //init -> not_available or not_available -> available in schema, depend on __index_process_info
          if (OB_SUCCESS != (err = root_server_->modify_init_index()))
          {
            TBSYS_LOG(WARN, "fail to modify init index status. ret=%d", err);
          }
        }
      }
      //add:e
	}
    }
    sleep(CHECK_INTERVAL_SECOND);
  }
  TBSYS_LOG(INFO, "[NOTICE] merge checker thread exit");
}
