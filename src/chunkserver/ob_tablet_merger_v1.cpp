/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 *         ob_tablet_merger.cpp is for what ...
 *
 *  Version: $Id: ob_tablet_merger.cpp 12/25/2012 03:42:46 PM qushan Exp $
 *
 *  Authors:
 *     qushan < qushan@taobao.com >
 *        - some work details if you want
 */


#include "ob_tablet_merger_v1.h"
#include "common/ob_schema.h"
#include "common/ob_schema_manager.h" //add by zhaoqiong [bugfix: create table][schema sync]20160106
#include "sstable/ob_sstable_schema.h"
#include "ob_chunk_server_main.h"
#include "common/file_directory_utils.h"
#include "ob_chunk_merge.h"
#include "ob_tablet_manager.h"
//add liuxiao [secondary index col checksum]20150330
#include "common/ob_column_checksum.h"
//add e

namespace oceanbase
{
  namespace chunkserver
  {
    using namespace tbutil;
    using namespace common;
    using namespace sstable;

    /*-----------------------------------------------------------------------------
     *  ObTabletMerger
     *-----------------------------------------------------------------------------*/

    ObTabletMerger::ObTabletMerger(ObChunkMerge& chunk_merge,ObTabletManager& manager)
      : chunk_merge_(chunk_merge), manager_(manager), old_tablet_(NULL), frozen_version_(0)
    {
      sstable_id_.sstable_file_id_  = 0;
      sstable_id_.sstable_file_offset_ = 0;
      path_[0] = 0;
      tablet_array_.clear();
    }

    ObTabletMerger::~ObTabletMerger()
    {
    }

    int64_t ObTabletMerger::calc_tablet_checksum(const int64_t sstable_checksum)
    {
      int64_t tablet_checksum = 0;
      int64_t checksum_len = sizeof(uint64_t);
      char checksum_buf[checksum_len];
      int64_t pos = 0;
      if (OB_SUCCESS == serialization::encode_i64(checksum_buf,
          checksum_len, pos, sstable_checksum))
      {
        tablet_checksum = ob_crc64(
            tablet_checksum, checksum_buf, checksum_len);
      }
      return tablet_checksum;
    }

    int ObTabletMerger::check_tablet(const ObTablet* tablet)
    {
      int ret = OB_SUCCESS;

      if (NULL == tablet)
      {
        TBSYS_LOG(WARN, "tablet is NULL");
        ret = OB_INVALID_ARGUMENT;
      }
      else if (!manager_.get_disk_manager().is_disk_avail(tablet->get_disk_no()))
      {
        TBSYS_LOG(WARN, "tablet:%s locate on bad disk:%d, sstable_id:%ld",
            to_cstring(tablet->get_range()), tablet->get_disk_no(),
            (tablet->get_sstable_id_list()).count() > 0 ?
            (tablet->get_sstable_id_list()).at(0).sstable_file_id_ : 0);
        ret = OB_ERROR;
      }
      //check sstable path where exist
      else if ((tablet->get_sstable_id_list()).count() > 0)
      {
        int64_t sstable_id = 0;
        char path[OB_MAX_FILE_NAME_LENGTH];
        path[0] = '\0';
        sstable_id = (tablet->get_sstable_id_list()).at(0).sstable_file_id_;

        if ( OB_SUCCESS != (ret = get_sstable_path(sstable_id, path, sizeof(path))) )
        {
          TBSYS_LOG(WARN, "can't get the path of sstable, tablet:%s sstable_id:%ld",
              to_cstring(tablet->get_range()), sstable_id);
        }
        else if (false == FileDirectoryUtils::exists(path))
        {
          TBSYS_LOG(WARN, "tablet:%s sstable file is not exist, path:%s, sstable_id:%ld",
              to_cstring(tablet->get_range()), path, sstable_id);
          ret = OB_FILE_NOT_EXIST;
        }
      }

      return ret;
    }

    int ObTabletMerger::update_meta(ObTablet* old_tablet,
        const common::ObVector<ObTablet*> & tablet_array, const bool sync_meta)
    {
      int ret = OB_SUCCESS;
      ObMultiVersionTabletImage& tablet_image = manager_.get_serving_tablet_image();
      ObTablet *new_tablet_list[ tablet_array.size() ];
      int32_t idx = 0;
      if (OB_SUCCESS == ret)
      {
        ret = check_tablet(old_tablet);
      }

      for(ObVector<ObTablet *>::iterator it = tablet_array.begin();
          OB_SUCCESS == ret && it != tablet_array.end(); ++it)
      {
        if (NULL == (*it)) //in case
        {
          ret = OB_ERROR;
          break;
        }

        if(OB_SUCCESS != (ret = check_tablet((*it))))
        {
          break;
        }

        new_tablet_list[idx++] = (*it);
      }


      if (OB_SUCCESS == ret)
      {
        // in case we have migrated tablets, discard current merge tablet
        if (!old_tablet->is_merged())
        {
          if (OB_SUCCESS != (ret = tablet_image.upgrade_tablet(
                  old_tablet, new_tablet_list, idx, false)))
          {
            TBSYS_LOG(WARN,"upgrade new merged tablets error [%d]",ret);
          }
          else
          {
            if (sync_meta)
            {
              // sync new tablet meta files;
              for(int32_t i = 0; i < idx; ++i)
              {
                if (OB_SUCCESS != (ret = tablet_image.write(
                        new_tablet_list[i]->get_data_version(),
                        new_tablet_list[i]->get_disk_no())))
                {
                  TBSYS_LOG(WARN,"write new meta failed i=%d, version=%ld, disk_no=%d", i ,
                      new_tablet_list[i]->get_data_version(), new_tablet_list[i]->get_disk_no());
                }
              }

              // sync old tablet meta files;
              if (OB_SUCCESS == ret
                  && OB_SUCCESS != (ret = tablet_image.write(
                      old_tablet->get_data_version(), old_tablet->get_disk_no())))
              {
                TBSYS_LOG(WARN,"write old meta failed version=%ld, disk_no=%d",
                    old_tablet->get_data_version(), old_tablet->get_disk_no());
              }
            }

            if (OB_SUCCESS == ret)
            {
              int64_t recycle_version = old_tablet->get_data_version()
                - (ObMultiVersionTabletImage::MAX_RESERVE_VERSION_COUNT - 1);
              if (recycle_version > 0)
              {
                manager_.get_regular_recycler().recycle_tablet(
                    old_tablet->get_range(), recycle_version);
              }
            }
          }
        }
        else
        {
          TBSYS_LOG(INFO, "current tablet covered by migrated tablets, discard.");
        }
      }

      return ret;
    }

    int ObTabletMerger::gen_sstable_file_location(const int32_t disk_no,
        sstable::ObSSTableId& sstable_id, char* path, const int64_t path_size)
    {
      int ret = OB_SUCCESS;
      bool is_sstable_exist = false;
      sstable_id.sstable_file_id_ = manager_.allocate_sstable_file_seq();
      sstable_id.sstable_file_offset_ = 0;
      do
      {
        sstable_id.sstable_file_id_ = (sstable_id_.sstable_file_id_ << 8) | (disk_no & DISK_NO_MASK);

        if ( OB_SUCCESS != (ret = get_sstable_path(sstable_id, path, path_size)) )
        {
          TBSYS_LOG(WARN, "Merge : can't get the path of new sstable, ");
        }
        else if (true == (is_sstable_exist = FileDirectoryUtils::exists(path_)))
        {
          // reallocate new file seq until get file name not exist.
          sstable_id_.sstable_file_id_ = manager_.allocate_sstable_file_seq();
        }
      } while (OB_SUCCESS == ret && is_sstable_exist);

      return ret;
    }

    int ObTabletMerger::init()
    {
      return OB_SUCCESS;
    }

    /*-----------------------------------------------------------------------------
     *  ObTabletMergerV1
     *-----------------------------------------------------------------------------*/

    ObTabletMergerV1::ObTabletMergerV1(ObChunkMerge& chunk_merge, ObTabletManager& manager)
      : ObTabletMerger(chunk_merge, manager),
      cell_(NULL),
      new_table_schema_(NULL),
      current_sstable_size_(0),
      row_num_(0),
      pre_column_group_row_num_(0),
      cs_proxy_(manager),
      ms_wrapper_(*(ObChunkServerMain::get_instance()->get_chunk_server().get_rpc_proxy()),
                  ObChunkServerMain::get_instance()->get_chunk_server().get_config().index_timeout),
      merge_join_agent_(cs_proxy_)
    {
      //add liuxiao [secondary index col checksum]20150330
      //初始化内部column_checksum_对象
      memset(column_checksum_.get_str(),0,common::OB_MAX_COL_CHECKSUM_STR_LEN);
      //add e
      //add liuxiao [secondary index col checksum] 20150629
      rowkey_desc_.reset();
      index_desc_.reset();
      max_data_table_cid_ = OB_INVALID_ID;
      is_static_truncated_ = false; /*add zhaoqiong [Truncate Table]:20160318*/
      //add e
    }

    int ObTabletMergerV1::init()
    {
      int ret = OB_SUCCESS;
      ObChunkServer&  chunk_server = ObChunkServerMain::get_instance()->get_chunk_server();

      if (OB_SUCCESS == ret && chunk_server.get_config().join_cache_size >= 1)
      {
        ms_wrapper_.get_ups_get_cell_stream()->set_cache(manager_.get_join_cache());
      }

      if (OB_SUCCESS == ret)
      {
        bool dio = chunk_server.get_config().write_sstable_use_dio;
        writer_.set_dio(dio);
      }

      return ret;
    }

    int ObTabletMergerV1::check_row_count_in_column_group()
    {
      int ret = OB_SUCCESS;
      if (pre_column_group_row_num_ != 0)
      {
        if (row_num_ != pre_column_group_row_num_)
        {
          TBSYS_LOG(ERROR,"the row num between two column groups is difference,[%ld,%ld]",
              row_num_,pre_column_group_row_num_);
          ret = OB_ERROR;
        }
      }
      else
      {
        pre_column_group_row_num_ = row_num_;
      }
      return ret;
    }

    void ObTabletMergerV1::reset_for_next_column_group()
    {
      row_.clear();
      reset_local_proxy();
      merge_join_agent_.clear();
      scan_param_.reset();
      row_num_ = 0;
    }

    int ObTabletMergerV1::save_current_row(const bool current_row_expired)
    {
      int ret = OB_SUCCESS;
      if (current_row_expired)
      {
        //TBSYS_LOG(DEBUG, "current row expired.");
        //hex_dump(row_.get_row_key().ptr(), row_.get_row_key().length(), false, TBSYS_LOG_LEVEL_DEBUG);
      }
      else if ((ret = writer_.append_row(row_,current_sstable_size_)) != OB_SUCCESS )
      {
        TBSYS_LOG(ERROR, "Merge : append row failed [%d], this row_, obj count:%ld, "
                         "table:%lu, group:%lu",
            ret, row_.get_obj_count(), row_.get_table_id(), row_.get_column_group_id());
        for(int32_t i=0; i<row_.get_obj_count(); ++i)
        {
          row_.get_obj(i)->dump(TBSYS_LOG_LEVEL_ERROR);
        }
      }
      //add liuxiao [secondary index col checksum]20150330
      //每保存一行数据就计算一下该行数据的列校验和，并加到column_checksum_
      else if(ret==OB_SUCCESS)
      {
        common::col_checksum cc;
        cc.reset();
        if(is_data_table_ && has_index_or_is_index_)
        {
          //如果是数据表
          if(OB_SUCCESS != (ret = calc_tablet_col_checksum_index_local_tablet(row_,rowkey_desc_,index_desc_,cc.get_str())))
          {
            TBSYS_LOG(ERROR,"calc column_checksum error,ret[%d]",ret);
          }
          else if(OB_SUCCESS != (ret = column_checksum_.sum(cc)))
          {
            TBSYS_LOG(ERROR,"sum column_checksum error,ret[%d]",ret);
          }
          //TBSYS_LOG(ERROR,"test::liuxiao table_id:%ld  rowkey desc:%s index_desc:%s",row_.get_table_id(),to_cstring(rowkey_desc_),to_cstring(index_desc_));
          //TBSYS_LOG(INFO,"test::lmz, range:%s, current cc:[%s], column_checksum_:[%s]", to_cstring(new_range_), cc.get_str_const(), column_checksum_.get_str_const());
        }
        else if(!is_data_table_ && has_index_or_is_index_)
        {
          //如果是索引表
          if(OB_SUCCESS != (ret = calc_tablet_col_checksum_index_index_tablet(row_,rowkey_desc_,index_desc_,cc.get_str(),max_data_table_cid_)))
          {
            TBSYS_LOG(ERROR,"calc column_checksum error,ret[%d]",ret);
          }
          else if(OB_SUCCESS != (ret = column_checksum_.sum(cc)))
          {
            TBSYS_LOG(ERROR,"sum column_checksum error,ret[%d]",ret);
          }
          //TBSYS_LOG(ERROR,"test::liuxiao table_id:%ld  rowkey desc:%s index_desc:%s",row_.get_table_id(),to_cstring(rowkey_desc_),to_cstring(index_desc_));
        }
      }
      //add e
      return ret;
    }
   
    int ObTabletMergerV1::wait_aio_buffer() const
    {
      int ret = OB_SUCCESS;
      int status = 0;

      ObThreadAIOBufferMgrArray* aio_buf_mgr_array = GET_TSI_MULT(ObThreadAIOBufferMgrArray, TSI_SSTABLE_THREAD_AIO_BUFFER_MGR_ARRAY_1);
      if (NULL == aio_buf_mgr_array)
      {
        ret = OB_ERROR;
      }
      else if (OB_AIO_TIMEOUT == (status =
            aio_buf_mgr_array->wait_all_aio_buf_mgr_free(10 * 1000000)))
      {
        TBSYS_LOG(WARN, "failed to wait all aio buffer manager free, stop current thread");
        ret = OB_ERROR;
      }

      return ret;

    }

    int ObTabletMergerV1::reset_local_proxy() const
    {
      int ret = OB_SUCCESS;

      /**
       * if this function fails, it means that some critical problems
       * happen, don't kill chunk server here, just output some error
       * info, then we can restart this chunk server manualy.
       */
      ret = manager_.end_scan();
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "failed to end scan to release resources");
      }

      ret = reset_query_thread_local_buffer();
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "failed to reset query thread local buffer");
      }

      return ret;
    }

    int ObTabletMergerV1::reset()
    {
      row_num_ = 0;
      frozen_version_ = 0;

      sstable_id_.sstable_file_id_ = 0;
      path_[0] = 0;

      sstable_schema_.reset();
      tablet_array_.clear();
      //add liuxiao [secondary index col checksum]20150330
      //重置column_checksum_
      column_checksum_.reset();
      //add e
      //add liuxiao [secondary index col checksum]20150330
      rowkey_desc_.reset();
      index_desc_.reset();
      max_data_table_cid_ = OB_INVALID_ID;
      //add e

      is_static_truncated_ = false; /*add zhaoqiong [Truncate Table]:20160318*/
      return reset_local_proxy();
    }

    int ObTabletMergerV1::merge_column_group(
        const int64_t column_group_idx,
        const uint64_t column_group_id,
        int64_t& split_row_pos,
        const int64_t max_sstable_size,
        const bool is_need_join,
        bool& is_tablet_splited,
        bool& is_tablet_unchanged)
    {
      int ret = OB_SUCCESS;
      RowStatus row_status = ROW_START;
      ObOperatorMemLimit mem_limit;

      bool is_row_changed = false;
      bool current_row_expired = false;
      bool need_filter = tablet_merge_filter_.need_filter();
      /**
       * there are 4 cases that we cann't do "unmerge_if_unchanged"
       * optimization
       * 1. the config of chunkserver doesn't enable this function
       * 2. need expire some data in this tablet
       * 3. the table need join another tables
       * 4. the sub range of this tablet is splited
       */
      bool unmerge_if_unchanged =
        (THE_CHUNK_SERVER.get_config().unmerge_if_unchanged
         && !need_filter && !is_need_join && !is_tablet_splited);
      int64_t expire_row_num = 0;
      mem_limit.merge_mem_size_ = THE_CHUNK_SERVER.get_config().merge_mem_limit;
      // in order to reuse the internal cell array of merge join agent
      mem_limit.max_merge_mem_size_ = mem_limit.merge_mem_size_ + 1024 * 1024;
      ms_wrapper_.get_ups_get_cell_stream()->set_timeout(THE_CHUNK_SERVER.get_config().merge_timeout);
      ms_wrapper_.get_ups_scan_cell_stream()->set_timeout(THE_CHUNK_SERVER.get_config().merge_timeout);

      is_tablet_splited = false;
      is_tablet_unchanged = false;

      if (OB_SUCCESS != (ret = wait_aio_buffer()))
      {
        TBSYS_LOG(ERROR, "wait aio buffer error, column_group_id = %ld", column_group_id);
      }
      else if (OB_SUCCESS != (ret = fill_scan_param( column_group_id )))
      {
        TBSYS_LOG(ERROR,"prepare scan param failed : [%d]",ret);
      }
      else if (OB_SUCCESS != (ret = tablet_merge_filter_.adjust_scan_param(
        column_group_idx, column_group_id, scan_param_)))
      {
        TBSYS_LOG(ERROR, "tablet merge filter adjust scan param failed : [%d]", ret);
      }
      else if (OB_SUCCESS != (ret = merge_join_agent_.start_agent(scan_param_,
              *ms_wrapper_.get_ups_scan_cell_stream(),
              *ms_wrapper_.get_ups_get_cell_stream(),
              chunk_merge_.current_schema_,
              mem_limit,0, unmerge_if_unchanged, is_static_truncated_))) /*add zhaoqiong [Truncate Table]:20160318: param :is_static_truncate_*/
      {
        TBSYS_LOG(ERROR,"set request param for merge_join_agent failed [%d]",ret);
        merge_join_agent_.clear();
        scan_param_.reset();
      }
      else if (merge_join_agent_.is_unchanged()
          && old_tablet_->get_sstable_id_list().count() > 0)
      {
        is_tablet_unchanged = true;
      }

      if (OB_SUCCESS == ret && 0 == column_group_idx && !is_tablet_unchanged
          && (ret = create_new_sstable()) != OB_SUCCESS)
      {
        TBSYS_LOG(ERROR,"create sstable failed.");
      }

      while(OB_SUCCESS == ret && !is_tablet_unchanged)
      {
        cell_ = NULL; //in case
        if ( manager_.is_stoped() )
        {
          TBSYS_LOG(WARN,"stop in merging column_group_id=%ld", column_group_id);
          ret = OB_CS_MERGE_CANCELED;
        }
        else if ((ret = merge_join_agent_.next_cell()) != OB_SUCCESS ||
            (ret = merge_join_agent_.get_cell(&cell_, &is_row_changed)) != OB_SUCCESS)
        {
          //end or error
          if (OB_ITER_END == ret)
          {
            TBSYS_LOG(DEBUG,"Merge : end of file");
            ret = OB_SUCCESS;
            if (need_filter && ROW_GROWING == row_status)
            {
              ret = tablet_merge_filter_.check_and_trim_row(column_group_idx, row_num_,
                row_, current_row_expired);
              if (OB_SUCCESS != ret)
              {
                TBSYS_LOG(ERROR, "tablet merge filter do check_and_trim_row failed, "
                                 "row_num_=%ld", row_num_);
              }
              else if (OB_SUCCESS == ret && current_row_expired)
              {
                ++expire_row_num;
              }
            }
            if (OB_SUCCESS == ret && ROW_GROWING == row_status
                && OB_SUCCESS == (ret = save_current_row(current_row_expired)))
            {
              ++row_num_;
              row_.clear();
              current_row_expired = false;
              row_status = ROW_END;
            }
          }
          else
          {
            TBSYS_LOG(ERROR,"Merge : get data error,ret : [%d]", ret);
          }

          // end of file
          break;
        }
        else if ( cell_ != NULL )
        {
          /**
           * there are some data in scanner
           */
          if ((ROW_GROWING == row_status && is_row_changed))
          {
            if (0 == (row_num_ % chunk_merge_.merge_pause_row_count_) )
            {
              if (chunk_merge_.merge_pause_sleep_time_ > 0)
              {
                if (0 == (row_num_ % (chunk_merge_.merge_pause_row_count_ * 20)))
                {
                  // print log every sleep 20 times;
                  TBSYS_LOG(INFO,"pause in merging,sleep %ld",
                      chunk_merge_.merge_pause_sleep_time_);
                }
                usleep(static_cast<useconds_t>(chunk_merge_.merge_pause_sleep_time_));
              }

            }

            if (need_filter)
            {
              ret = tablet_merge_filter_.check_and_trim_row(column_group_idx, row_num_,
                row_, current_row_expired);
              if (OB_SUCCESS != ret)
              {
                TBSYS_LOG(ERROR, "tablet merge filter do check_and_trim_row failed, "
                                 "row_num_=%ld", row_num_);
              }
              else if (OB_SUCCESS == ret && current_row_expired)
              {
                ++expire_row_num;
              }
            }

            // we got new row, write last row we build.
            if (OB_SUCCESS == ret
                && OB_SUCCESS == (ret = save_current_row(current_row_expired)) )
            {
              ++row_num_;
              current_row_expired = false;
              // start new row.
              row_status = ROW_START;
            }

            // this split will use current %row_, so we clear row below.
            if (OB_SUCCESS == ret
                && ((split_row_pos > 0 && row_num_ > split_row_pos)
                    || (0 == column_group_idx && 0 == split_row_pos
                        && current_sstable_size_ > max_sstable_size))
                && maybe_change_sstable())
            {
              /**
               * if the merging tablet is no data, we can't set a proper
               * split_row_pos, so we try to merge the first column group, and
               * the first column group split by sstable size, if the first
               * column group size is larger than max_sstable_size, we set the
               * current row count as the split_row_pos, the next clolumn
               * groups of the first splited tablet use this split_row_pos,
               * after merge the first splited tablet, we use the first
               * splited tablet as sampler, re-caculate the average row size
               * of tablet and split_row_pos, the next splited tablet of the
               * tablet will use the new split_row_pos.
               */
              if (0 == column_group_idx && 0 == split_row_pos
                  && current_sstable_size_ > max_sstable_size)
              {
                split_row_pos = row_num_;
                TBSYS_LOG(INFO, "column group %ld splited by sstable size, "
                                "current_sstable_size_=%ld, max_sstable_size=%ld, "
                                "split_row_pos=%ld",
                  column_group_idx, current_sstable_size_,
                  max_sstable_size, split_row_pos);
              }
              /**
               * record the end key
               */
              if ( (ret = update_range_end_key()) != OB_SUCCESS)
              {
                TBSYS_LOG(ERROR,"update end range error [%d]",ret);
              }
              else
              {
                is_tablet_splited = true;
              }
              // entercount split point, skip to merge next column group
              // we reset all status whenever start new column group, so
              // just break it, leave it to reset_for_next_column_group();
              break;
            }

            // before we build new row, must clear last row in use.
            row_.clear();
          }

          if ( OB_SUCCESS == ret && ROW_START == row_status)
          {
            // first cell in current row, set rowkey and table info.
            row_.set_rowkey(cell_->row_key_);
            row_.set_table_id(cell_->table_id_);
            row_.set_column_group_id(column_group_id);
            row_status = ROW_GROWING;
          }

          if (OB_SUCCESS == ret && ROW_GROWING == row_status)
          {
            // no need to add current cell to %row_ when current row expired.
            if ((ret = row_.add_obj(cell_->value_)) != OB_SUCCESS)
            {
              TBSYS_LOG(ERROR,"Merge : add_cell_to_row failed [%d]",ret);
            }
          }
        }
        else
        {
          TBSYS_LOG(ERROR,"get cell return success but cell is null");
          ret = OB_ERROR;
        }
      }

      TBSYS_LOG(INFO, "column group %ld merge completed, ret = %d, version_range=%s, "
          "total row count =%ld, expire row count = %ld, split_row_pos=%ld, "
          "max_sstable_size=%ld, is_tablet_splited=%d, is_tablet_unchanged=%d, range=%s",
          column_group_id, ret, range2str(scan_param_.get_version_range()),
          row_num_, expire_row_num, split_row_pos,
          max_sstable_size, is_tablet_splited, is_tablet_unchanged, to_cstring(new_range_));

      return ret;
    }

    bool ObTabletMergerV1::is_table_need_join(const uint64_t table_id)
    {
      bool ret = false;
      const ObColumnSchemaV2* column = NULL;
      int32_t column_size = 0;

      column = chunk_merge_.current_schema_.get_table_schema(table_id, column_size);
      if (NULL != column && column_size > 0)
      {
        for (int32_t i = 0; i < column_size; ++i)
        {
          if (NULL != column[i].get_join_info())
          {
            ret = true;
            break;
          }
        }
      }

      return ret;
    }

    int ObTabletMergerV1::merge(ObTablet *tablet,int64_t frozen_version)
    {
      //add by zhaoqiong [bugfix: create table][schema sync]20160106:b
      common::ObMergerSchemaManager * schema_manager = THE_CHUNK_SERVER.get_schema_manager();
      const ObSchemaManagerV2 *user_schema = NULL;
      //add:e
      int   ret = OB_SUCCESS;
      int64_t split_row_pos = 0;
      int64_t init_split_row_pos = 0;
      bool    is_tablet_splited = false;
      bool    is_first_column_group_splited = false;
      bool    is_tablet_unchanged = false;
      int64_t max_sstable_size = OB_DEFAULT_MAX_TABLET_SIZE;
      bool    sync_meta = THE_CHUNK_SERVER.get_config().each_tablet_sync_meta;
      ObTablet* new_tablet = NULL;

      FILL_TRACE_LOG("start merge tablet");
      if (NULL == tablet || frozen_version <= 0)
      {
        TBSYS_LOG(ERROR,"merge : interal error ,param invalid frozen_version:[%ld]",frozen_version);
        ret = OB_ERROR;
      }
      else if ( OB_SUCCESS != (ret = reset()) )
      {
        TBSYS_LOG(ERROR, "reset query thread local buffer error.");
      }

      //add by zhaoqiong [bugfix: create table][schema sync]20160128:b
      if (ret == OB_SUCCESS)
      {
        user_schema = schema_manager->get_user_schema(0);
        if (user_schema == NULL)
        {
          TBSYS_LOG(WARN, "get latest but local schema failed:latest[%ld]", schema_manager->get_latest_version());
          ret = OB_INNER_STAT_ERROR;
        }
        else if ( user_schema->get_version() > chunk_merge_.current_schema_.get_version())
        {
           if (NULL == ( new_table_schema_ = chunk_merge_.current_schema_.get_table_schema(tablet->get_range().table_id_))
                && NULL != user_schema->get_table_schema(tablet->get_range().table_id_) && tablet->get_range().is_whole_range())
          {
            TBSYS_LOG(WARN,"table (%lu) schema is not found in frozen schema",tablet->get_range().table_id_);
            //add empty tablet, there is no sstable files in it.
            old_tablet_ = tablet;
            ret = create_empty_tablet_and_upgrade(new_tablet,frozen_version);
            if (OB_SUCCESS == ret)
            {
              ret = OB_CS_MERGE_CANCELED;
            }
          }
        }
        // user_schema can be release after schema checker.
        if (user_schema != NULL)
        {
          schema_manager->release_schema(user_schema);
        }
      }
      //add:e

      //mod by zhaoqiong [bugfix: create table][schema sync]20160106:b
//      else if ( NULL == (new_table_schema_ =
//            chunk_merge_.current_schema_.get_table_schema(tablet->get_range().table_id_)))
      if (ret == OB_SUCCESS)
      {
        if (NULL == (new_table_schema_ =
                          chunk_merge_.current_schema_.get_table_schema(tablet->get_range().table_id_)))
        {
          //This table has been deleted
          TBSYS_LOG(INFO,"table (%lu) has been deleted",tablet->get_range().table_id_);
          tablet->set_merged();
          ret = OB_CS_TABLE_HAS_DELETED;
        }
        else if (OB_SUCCESS != (ret = fill_sstable_schema(*new_table_schema_,sstable_schema_)))
        {
          TBSYS_LOG(ERROR, "convert table schema to sstable schema failed, table=%ld",
                    tablet->get_range().table_id_);
        }
        //add liuxiao [secondary index col checksum] 20150401
        //检查该表是否需要是索引表，或者是有索引表的数据表
        else if(OB_SUCCESS != (ret = set_has_index_or_is_index(tablet->get_range().table_id_)))
        {
          TBSYS_LOG(ERROR,"failed set flag for get old col checksum ret=%d",ret);
        }
        //add liuxiao [seconadry index col checksum] 20150629
        //代码优化，避免重复构建行
        else
        {
          if(is_data_table_)
          {
            if(OB_SUCCESS != (ret = cons_row_desc_local_tablet(rowkey_desc_,index_desc_,tablet->get_range().table_id_)))
            {
              TBSYS_LOG(ERROR,"failed to create row desc for cal col checksum,ret[%d]",ret);
            }
            //TBSYS_LOG(ERROR,"test::liuxiao table_id:%ld  rowkey desc:%s index_desc:%s",tablet->get_range().table_id_,to_cstring(rowkey_desc_),to_cstring(index_desc_));
          }
          else if(!is_data_table_ && has_index_or_is_index_)
          {
            if(OB_SUCCESS != (ret = cons_row_desc_without_virtual(rowkey_desc_,index_desc_,max_data_table_cid_,tablet->get_range().table_id_)))
            {
              TBSYS_LOG(ERROR,"failed to create row desc for cal col checksum,ret[%d]",ret);
            }
            //TBSYS_LOG(ERROR,"test::liuxiao table_id:%ld  rowkey desc:%s index_desc:%s",tablet->get_range().table_id_,to_cstring(rowkey_desc_),to_cstring(index_desc_));
          }
        }
        //add e
      }
      //add e      
      if (OB_SUCCESS == ret)
      {
        // parse compress name from schema, may be NULL;
        const char *compressor_name = new_table_schema_->get_compress_func_name();
        if (NULL == compressor_name || '\0' == *compressor_name )
        {
          TBSYS_LOG(WARN,"no compressor with this sstable.");
        }
        compressor_string_.assign_ptr(const_cast<char *>(compressor_name),static_cast<int32_t>(strlen(compressor_name)));

        // if schema define max sstable size for table, use it
        if (new_table_schema_->get_max_sstable_size() > 0)
        {
          max_sstable_size = new_table_schema_->get_max_sstable_size();
        }
        else
        {
          TBSYS_LOG(INFO, "schema doesn't specify max_sstable_size, "
              "use default value, max_sstable_size=%ld", max_sstable_size);
        }

        // according to sstable size, compute the average row count of each sstable
        split_row_pos = tablet->get_row_count();
        int64_t over_size_percent = THE_CHUNK_SERVER.get_config().over_size_percent_to_split;
        int64_t split_size = (over_size_percent > 0) ? (max_sstable_size * (100 + over_size_percent) / 100) : 0;
        int64_t occupy_size = tablet->get_occupy_size();
        int64_t row_count = tablet->get_row_count();
        if (occupy_size > split_size && row_count > 0)
        {
          int64_t avg_row_size = occupy_size / row_count;
          if (avg_row_size > 0)
          {
            split_row_pos = max_sstable_size / avg_row_size;
          }
        }
        /**
         * if user specify over_size_percent, and the tablet size is
         * greater than 0 and not greater than split_size threshold, we
         * don't split this tablet. if tablet size is 0, use the default
         * split algorithm
         */
        if (over_size_percent > 0 && occupy_size > 0 && occupy_size <= split_size)
        {
          // set split_row_pos = -1, this tablet will not split
          split_row_pos = -1;
        }
        init_split_row_pos = split_row_pos;
      }


      if (OB_SUCCESS == ret)
      {
        new_range_ = tablet->get_range();
        old_tablet_ = tablet;
        frozen_version_ = frozen_version;
        int64_t sstable_id = 0;
        char range_buf[OB_RANGE_STR_BUFSIZ];
        ObSSTableId sst_id;
        char path[OB_MAX_FILE_NAME_LENGTH];
        path[0] = '\0';

        if ((tablet->get_sstable_id_list()).count() > 0)
          sstable_id = (tablet->get_sstable_id_list()).at(0).sstable_file_id_;

        tablet->get_range().to_string(range_buf,sizeof(range_buf));
        if (0 != sstable_id)
        {
          sst_id.sstable_file_id_ = sstable_id;
          get_sstable_path(sst_id, path, sizeof(path));
        }
        TBSYS_LOG(INFO, "start merge sstable_id:%ld, old_version=%ld, "
            "new_version=%ld, split_row_pos=%ld, table_row_count=%ld, "
            "tablet_occupy_size=%ld, compressor=%.*s, path=%s, range=%s",
            sstable_id, tablet->get_data_version(), frozen_version,
            split_row_pos, tablet->get_row_count(),
            tablet->get_occupy_size(), compressor_string_.length(),
            compressor_string_.ptr(), path, range_buf);

        uint64_t column_group_ids[OB_MAX_COLUMN_GROUP_NUMBER];
        int64_t column_group_num = sizeof(column_group_ids) / sizeof(column_group_ids[0]);

        if ( OB_SUCCESS != (ret = sstable_schema_.get_table_column_groups_id(
                new_table_schema_->get_table_id(), column_group_ids,column_group_num))
            || column_group_num <= 0)
        {
          TBSYS_LOG(ERROR,"get column groups failed, column_group_num=%ld, ret=%d",
              column_group_num, ret);
        }
        bool is_need_join = is_table_need_join(new_table_schema_->get_table_id());

        if (OB_SUCCESS == ret &&
            OB_SUCCESS != (ret = tablet_merge_filter_.init(chunk_merge_.current_schema_,
              column_group_num, tablet, frozen_version, chunk_merge_.frozen_timestamp_)))
        {
          TBSYS_LOG(ERROR, "failed to initialize tablet merge filter, table=%ld",
              tablet->get_range().table_id_);
        }

        FILL_TRACE_LOG("after prepare merge tablet, column_group_num=%d, ret=%d",
          column_group_num, ret);
        while(OB_SUCCESS == ret)
        {
          if ( manager_.is_stoped() )
          {
            TBSYS_LOG(WARN,"stop in merging");
            ret = OB_CS_MERGE_CANCELED;
          }

          // clear all bits while start merge new tablet.
          // generally in table split.
          tablet_merge_filter_.clear_expire_bitmap();
          pre_column_group_row_num_ = 0;
          is_first_column_group_splited  = false;

          for(int32_t group_index = 0; (group_index < column_group_num) && (OB_SUCCESS == ret); ++group_index)
          {
            if ( manager_.is_stoped() )
            {
              TBSYS_LOG(WARN,"stop in merging");
              ret = OB_CS_MERGE_CANCELED;
            }
            // %expire_row_filter will be set in first column group,
            // and test in next merge column group.
            else if ( OB_SUCCESS != ( ret = merge_column_group(
                  group_index, column_group_ids[group_index],
                  split_row_pos, max_sstable_size, is_need_join,
                  is_tablet_splited, is_tablet_unchanged)) )
            {
              TBSYS_LOG(ERROR, "merge column group[%d] = %lu , group num = %ld,"
                  "split_row_pos = %ld error.",
                  group_index, column_group_ids[group_index],
                  column_group_num, split_row_pos);
            }
            else if ( OB_SUCCESS != (ret = check_row_count_in_column_group()) )
            {
              TBSYS_LOG(ERROR, "check row count column group[%d] = %lu",
                  group_index, column_group_ids[group_index]);
            }

            // only when merging the first column group, the function merge_column_group()
            // set the is_tablet_splited flag
            if (OB_SUCCESS == ret && 0 == group_index)
            {
              is_first_column_group_splited = is_tablet_splited;
            }

            FILL_TRACE_LOG("finish merge column group %d, is_splited=%d, "
                           "row_num=%ld, ret=%d",
              group_index, is_tablet_splited, row_num_, ret);

            // reset for next column group..
            // page arena reuse avoid memory explosion
            reset_for_next_column_group();

            // if the first column group is unchaged, the tablet is unchanged,
            // break the merge loop
            if (OB_SUCCESS == ret && 0 == group_index && is_tablet_unchanged)
            {
              break;
            }
          }

          // all column group has been written,finish this sstable
          if (OB_SUCCESS == ret
              && (ret = finish_sstable(is_tablet_unchanged, new_tablet))!= OB_SUCCESS)
          {
            TBSYS_LOG(ERROR,"close sstable failed [%d]",ret);
          }
          //add liuxiao [secondary index col checksum] 20150522
          //出现分裂的话需要重置columnchecksum,以供下一个tablet使用
          else
          {
            column_checksum_.reset();
          }
          //add e

          // not split, break
          if (OB_SUCCESS == ret && !is_first_column_group_splited)
          {
            break;
          }

          FILL_TRACE_LOG("start merge new splited tablet");

          /**
           * if the merging tablet is no data, we can't set a proper
           * split_row_pos, so we try to merge the first column group, and
           * the first column group split by sstable size, if the first
           * column group size is larger than max_sstable_size, we set the
           * current row count as the split_row_pos, the next clolumn
           * groups of the first splited tablet use this split_row_pos,
           * after merge the first splited tablet, we use the first
           * splited tablet as sampler, re-caculate the average row size
           * of tablet and split_row_pos, the next splited tablet of the
           * tablet will use the new split_row_pos.
           */
          if (OB_SUCCESS == ret && 0 == init_split_row_pos
              && split_row_pos > 0 && NULL != new_tablet
              && new_tablet->get_occupy_size() > 0
              && new_tablet->get_row_count() > 0)
          {
            int64_t avg_row_size = new_tablet->get_occupy_size() / new_tablet->get_row_count();
            if (avg_row_size > 0)
            {
              split_row_pos = max_sstable_size / avg_row_size;
            }
            init_split_row_pos = split_row_pos;
          }

          if (OB_SUCCESS == ret)
          {
            update_range_start_key();

            //prepare the next request param
            new_range_.end_key_ = tablet->get_range().end_key_;
            if (tablet->get_range().end_key_.is_max_row())
            {
              TBSYS_LOG(INFO,"this tablet has max flag,reset it");
              new_range_.end_key_.set_max_row();
              new_range_.border_flag_.unset_inclusive_end();
            }
          }
        } // while (OB_SUCCESS == ret) //finish one tablet
        if (OB_SUCCESS == ret)
        {
          ret = update_meta(old_tablet_, tablet_array_, sync_meta);
        }
        else
        {
          TBSYS_LOG(ERROR,"merge failed,don't add these tablet");

          int64_t t_off = 0;
          int64_t s_size = 0;
          writer_.close_sstable(t_off,s_size);
          if (sstable_id_.sstable_file_id_ != 0)
          {
            if (strlen(path_) > 0)
            {
              unlink(path_); //delete
              TBSYS_LOG(WARN,"delete %s",path_);
            }
            manager_.get_disk_manager().add_used_space((sstable_id_.sstable_file_id_ & DISK_NO_MASK),0);
          }

          int64_t sstable_id = 0;

          for(ObVector<ObTablet *>::iterator it = tablet_array_.begin(); it != tablet_array_.end(); ++it)
          {
            if ( ((*it) != NULL) && ((*it)->get_sstable_id_list().count() > 0))
            {
              sstable_id = (*it)->get_sstable_id_list().at(0).sstable_file_id_;
              if (OB_SUCCESS == get_sstable_path(sstable_id,path_,sizeof(path_)))
              {
                unlink(path_);
              }
            }
          }
          tablet_array_.clear();
          path_[0] = '\0';
          current_sstable_size_ = 0;
        }
      }
      FILL_TRACE_LOG("finish merge tablet, ret=%d", ret);
      PRINT_TRACE_LOG();
      CLEAR_TRACE_LOG();

      return ret;
    }


    int ObTabletMergerV1::fill_sstable_schema(const ObTableSchema& common_schema,ObSSTableSchema& sstable_schema)
    {
      int ret = OB_SUCCESS;
      bool binary_format = chunk_merge_.write_sstable_version_ < SSTableReader::ROWKEY_SSTABLE_VERSION;
      sstable_schema.reset();
      const ObRowkeyInfo & rowkey_info = chunk_merge_.current_schema_.get_table_schema(
          common_schema.get_table_id())->get_rowkey_info();
      // set rowkey info for write old fashion sstable
      if (binary_format)
      {
        TBSYS_LOG(INFO, "table=%ld, rowkey info=%s, binary length=%ld",
            common_schema.get_table_id(), to_cstring(rowkey_info), rowkey_info.get_binary_rowkey_length());
        row_.set_rowkey_info(&rowkey_info);
      }

      ret = build_sstable_schema(common_schema.get_table_id(), chunk_merge_.current_schema_,
          sstable_schema, binary_format);
      if ( 0 == sstable_schema.get_column_count() && OB_SUCCESS == ret ) //this table has moved to updateserver
      {
        ret = OB_CS_TABLE_HAS_DELETED;
      }

      return ret;
    }

    int ObTabletMergerV1::create_new_sstable()
    {
      int ret                          = OB_SUCCESS;
      sstable_id_.sstable_file_id_     = manager_.allocate_sstable_file_seq();
      sstable_id_.sstable_file_offset_ = 0;
      int32_t disk_no                  = manager_.get_disk_manager().get_dest_disk();
      int64_t sstable_block_size       = OB_DEFAULT_SSTABLE_BLOCK_SIZE;
      bool is_sstable_exist            = false;

      if (disk_no < 0)
      {
        TBSYS_LOG(ERROR,"does't have enough disk space");
        sstable_id_.sstable_file_id_ = 0;
        ret = OB_CS_OUTOF_DISK_SPACE;
      }

      // if schema define sstable block size for table, use it
      // for the schema with version 2, the default block size is 64(KB),
      // we skip this case and use the config of chunkserver
      if (NULL != new_table_schema_ && new_table_schema_->get_block_size() > 0
          && 64 != new_table_schema_->get_block_size())
      {
        sstable_block_size = new_table_schema_->get_block_size();
      }

      if (OB_SUCCESS == ret)
      {
        /**
         * mustn't use the same sstable id in the the same disk, because
         * if tablet isn't changed, we just add a hard link pointer to
         * the old sstable file, so maybe different sstable file pointer
         * the same content in disk. if we cancle daily merge, maybe
         * some tablet meta isn't synced into index file, then we
         * restart chunkserver will do daily merge again, it may reuse
         * the same sstable id, if the sstable id is existent and it
         * pointer to a share disk content, the sstable will be
         * truncated if we create sstable with the sstable id.
         */
        do
        {
          sstable_id_.sstable_file_id_     = (sstable_id_.sstable_file_id_ << 8) | (disk_no & 0xff);

          if ((OB_SUCCESS == ret) && (ret = get_sstable_path(sstable_id_,path_,sizeof(path_))) != OB_SUCCESS )
          {
            TBSYS_LOG(ERROR,"Merge : can't get the path of new sstable");
            ret = OB_ERROR;
          }

          if (OB_SUCCESS == ret)
          {
            is_sstable_exist = FileDirectoryUtils::exists(path_);
            if (is_sstable_exist)
            {
              sstable_id_.sstable_file_id_ = manager_.allocate_sstable_file_seq();
            }
          }
        } while (OB_SUCCESS == ret && is_sstable_exist);

        if (OB_SUCCESS == ret)
        {
          TBSYS_LOG(INFO,"create new sstable, sstable_path:%s, compressor:%.*s, "
                         "version=%ld, block_size=%ld\n",
              path_, compressor_string_.length(), compressor_string_.ptr(),
              frozen_version_, sstable_block_size);
          path_string_.assign_ptr(path_,static_cast<int32_t>(strlen(path_) + 1));

          int64_t element_count = DEFAULT_ESTIMATE_ROW_COUNT;
          if (NULL != old_tablet_ && old_tablet_->get_row_count() != 0)
          {
            element_count = old_tablet_->get_row_count();
          }

          if ((ret = writer_.create_sstable(sstable_schema_,
                  path_string_, compressor_string_, frozen_version_,
                  OB_SSTABLE_STORE_DENSE, sstable_block_size, element_count)) != OB_SUCCESS)
          {
            if (OB_IO_ERROR == ret)
              manager_.get_disk_manager().set_disk_status(disk_no,DISK_ERROR);
            TBSYS_LOG(ERROR,"Merge : create sstable failed : [%d]",ret);
          }
        }
      }
      return ret;
    }

    int ObTabletMergerV1::fill_scan_param(const uint64_t column_group_id)
    {
      int ret = OB_SUCCESS;

      ObString table_name_string;

      //scan_param_.reset();
      scan_param_.set(new_range_.table_id_, table_name_string, new_range_);
      /**
       * in merge,we do not use cache,
       * and just read frozen mem table.
       */
      scan_param_.set_is_result_cached(false);
      scan_param_.set_is_read_consistency(false);

      int64_t preread_mode = THE_CHUNK_SERVER.get_config().merge_scan_use_preread;
      if ( 0 == preread_mode )
      {
        scan_param_.set_read_mode(ScanFlag::SYNCREAD);
      }
      else
      {
        scan_param_.set_read_mode(ScanFlag::ASYNCREAD);
      }

      ObVersionRange version_range;
      //mod zhaoqiong [Truncate Table]:20170519
//      ObVersionRange new_range; /*add zhaoqiong [Truncate Table]:20160318*/
      bool is_truncated = false;
      //mod:e
      version_range.border_flag_.set_inclusive_end();

      version_range.start_version_ =  old_tablet_->get_data_version();
      version_range.end_version_   =  old_tablet_->get_data_version() + 1;

      //add zhaoqiong [Truncate Table]:20160318:b
      ObMergerRpcProxy* rpc_proxy = THE_CHUNK_SERVER.get_rpc_proxy();
      if (NULL != rpc_proxy)
      {
          int64_t retry_times  = THE_CHUNK_SERVER.get_config().retry_times;
          //mod zhaoqiong [Truncate Table]:20170519
  //        RPC_RETRY_WAIT(true, retry_times, ret,
  //          rpc_proxy->check_incremental_data_range(new_range_.table_id_, version_range, new_range));
          RPC_RETRY_WAIT(true, retry_times, ret,
            rpc_proxy->check_incremental_data_range(new_range_.table_id_, version_range, is_truncated));
          //mod:e
      }
      else
      {
        TBSYS_LOG(WARN, "get rpc proxy from chunkserver failed");
        ret = OB_ERROR;
      }

      if (OB_SUCCESS == ret)
      {
          //mod zhaoqiong [Truncate Table]:20170519
//        if (ObVersion::compare(int64_t(new_range.start_version_),int64_t(version_range.start_version_))!=0)
        if (is_truncated)
          //mod:e
        {
          is_static_truncated_ = true;
        }
        else
        {
          is_static_truncated_ = false;
        }

        scan_param_.set_version_range(version_range);
      //add:e

      int64_t size = 0;
      const ObSSTableSchemaColumnDef *col = sstable_schema_.get_group_schema(
          new_table_schema_->get_table_id(), column_group_id, size);

      if (NULL == col || size <= 0)
      {
        TBSYS_LOG(ERROR,"cann't find this column group:%lu, table_id=%ld",
            column_group_id, new_table_schema_->get_table_id());
        sstable_schema_.dump();
        ret = OB_ERROR;
      }
      else
      {
        for(int32_t i = 0; i < size; ++i)
        {
          if ( (ret = scan_param_.add_column( (col + i)->column_name_id_ ) ) != OB_SUCCESS)
          {
            TBSYS_LOG(ERROR,"add column id [%u] to scan_param_ error[%d]",(col + i)->column_name_id_, ret);
            break;
          }
        }
      }
      }//add zhaoqiong [Truncate Table]:20160318
      return ret;
    }

    int ObTabletMergerV1::update_range_start_key()
    {
      int ret = OB_SUCCESS;
      // set split_rowkey_ as start key of next range.
      // must copy split_rowkey_ to last_rowkey_buffer_,
      // next split will modify split_rowkey_;
      ObMemBufAllocatorWrapper allocator(last_rowkey_buffer_, ObModIds::OB_SSTABLE_WRITER);
      if (OB_SUCCESS != (ret = split_rowkey_.deep_copy(new_range_.start_key_, allocator)))
      {
        TBSYS_LOG(WARN, "set split_rowkey_(%s) as start key of new range error.", to_cstring(split_rowkey_));
      }
      else
      {
        new_range_.border_flag_.unset_inclusive_start();
      }
      return ret;
    }

    int ObTabletMergerV1::update_range_end_key()
    {
      int ret = OB_SUCCESS;
      int32_t split_pos = new_table_schema_->get_split_pos();

      int64_t rowkey_column_count = 0;
      sstable_schema_.get_rowkey_column_count(new_range_.table_id_, rowkey_column_count);
      common::ObRowkey rowkey(NULL, rowkey_column_count);
      ObMemBufAllocatorWrapper allocator(split_rowkey_buffer_, ObModIds::OB_SSTABLE_WRITER);

      if (rowkey_column_count <= 0)
      {
        TBSYS_LOG(INFO, "table: %ld has %ld column rowkey",
            new_range_.table_id_, rowkey_column_count);
        ret = OB_ERROR;
      }
      else if (OB_SUCCESS != (ret = row_.get_rowkey(rowkey)))
      {
        TBSYS_LOG(ERROR, "get rowkey for current row error.");
      }
      else if (OB_SUCCESS != (ret = rowkey.deep_copy(split_rowkey_, allocator)))
      {
        TBSYS_LOG(ERROR, "deep copy current row:%s to split_rowkey_ error", to_cstring(rowkey));
      }
      else
      {
        TBSYS_LOG(INFO, "current rowkey:%s,split rowkey (%s), split pos=%d",
            to_cstring(rowkey), to_cstring(split_rowkey_), split_pos);
      }

      if (split_pos > 0 && split_pos < split_rowkey_.get_obj_cnt())
      {
        const_cast<ObObj*>(split_rowkey_.get_obj_ptr()+split_pos)->set_max_value();
      }

      if (OB_SUCCESS == ret)
      {
        new_range_.end_key_ = split_rowkey_;
        new_range_.border_flag_.set_inclusive_end();
      }
      return ret;
    }

    int ObTabletMergerV1::create_hard_link_sstable(int64_t& sstable_size)
    {
      int ret = OB_SUCCESS;
      ObSSTableId old_sstable_id;
      char old_path[OB_MAX_FILE_NAME_LENGTH];
      sstable_size = 0;

      /**
       * when do daily merge, the old tablet is unchanged, there is no
       * dynamic data in update server for this old tablet. we needn't
       * merge this tablet, just create a hard link for the unchanged
       * tablet at the same disk. althrough the hard link only add the
       * reference count of inode, and both sstable names refer to the
       * same sstable file, there is oly one copy on disk.
       *
       * after create a hard link, we also add the disk usage space.
       * so the disk usage space statistic is not very correct, but
       * when the sstable is recycled, the disk space of the sstable
       * will be decreased. so we can ensure the recycle logical is
       * correct.
       */
      if (old_tablet_->get_sstable_id_list().count() > 0)
      {
        int32_t disk_no = old_tablet_->get_disk_no();

        /**
         * mustn't use the same sstable id in the the same disk, because
         * if tablet isn't changed, we just add a hard link pointer to
         * the old sstable file, so maybe different sstable file pointer
         * the same content in disk. if we cancle daily merge, maybe
         * some tablet meta isn't synced into index file, then we
         * restart chunkserver will do daily merge again, it may reuse
         * the same sstable id, if the sstable id is existent and it
         * pointer to a share disk content, the sstable will be
         * truncated if we create sstable with the sstable id.
         */
        do
        {
          sstable_id_.sstable_file_id_ = manager_.allocate_sstable_file_seq();
          sstable_id_.sstable_file_id_ = (sstable_id_.sstable_file_id_ << 8) | (disk_no & 0xff);

          if ((ret = get_sstable_path(sstable_id_,path_,sizeof(path_))) != OB_SUCCESS )
          {
            TBSYS_LOG(ERROR, "create_hard_link_sstable: can't get the path of hard link sstable");
            ret = OB_ERROR;
          }
        } while (OB_SUCCESS == ret && FileDirectoryUtils::exists(path_));

        if (OB_SUCCESS == ret)
        {
          /**
           * FIXME: current we just support one tablet with only one
           * sstable file
           */
          sstable_size = (*old_tablet_->get_sstable_reader_list().at(0)).get_sstable_size();
          old_sstable_id = old_tablet_->get_sstable_id_list().at(0);
          if ((ret = get_sstable_path(old_sstable_id, old_path, sizeof(old_path))) != OB_SUCCESS )
          {
            TBSYS_LOG(ERROR, "create_hard_link_sstable: can't get the path of old sstable");
            ret = OB_ERROR;
          }
          else if (0 != ::link(old_path, path_))
          {
            TBSYS_LOG(ERROR, "failed create hard link for unchanged sstable, "
                             "old_sstable=%s, new_sstable=%s",
              old_path, path_);
            ret = OB_ERROR;
          }
        }
      }

      return ret;
    }

    int ObTabletMergerV1::finish_sstable(const bool is_tablet_unchanged,
      ObTablet*& new_tablet)
    {
      int64_t trailer_offset = 0;
      int64_t sstable_size = 0;
      int ret = OB_SUCCESS;
      uint64_t row_checksum = 0;
      new_tablet = NULL;
      TBSYS_LOG(DEBUG,"Merge : finish_sstable");
      if (!is_tablet_unchanged)
      {
        row_checksum = writer_.get_row_checksum();
        if (OB_SUCCESS != (ret = writer_.set_tablet_range(new_range_)))
        {
          TBSYS_LOG(ERROR, "failed to set tablet range for sstable writer");
        }
        else if (OB_SUCCESS != (ret = writer_.close_sstable(trailer_offset,sstable_size)))
        {
          TBSYS_LOG(ERROR,"Merge : close sstable failed.");
        }
        else if (sstable_size < 0)
        {
          TBSYS_LOG(ERROR, "Merge : close sstable failed, sstable_size=%ld", sstable_size);
            ret = OB_ERROR;
        }
      }

      if (OB_SUCCESS == ret)
      {
        ObMultiVersionTabletImage& tablet_image = manager_.get_serving_tablet_image();
        const ObSSTableTrailer* trailer = NULL;
        ObTablet *tablet = NULL;
        ObTabletExtendInfo extend_info;
        row_num_ = 0;
        pre_column_group_row_num_ = 0;
        if ((ret = tablet_image.alloc_tablet_object(new_range_,frozen_version_,tablet)) != OB_SUCCESS)
        {
          TBSYS_LOG(ERROR,"alloc_tablet_object failed.");
        }
        else
        {
          if (is_tablet_unchanged)
          {
            //add liuxiao [secondary index col checksum]20150522
            if((!is_data_table_ && has_index_or_is_index_) || (has_index_or_is_index_ && is_data_table_))
            {
              //是索引表或有有建索引表的数据表
              //如果没有增量数据则需要从rs取旧的回来(is_tablet_unchanged)
              //如果有增量的话，finish tablet的时候就已经发送
              //左边条件：如果是索引表，任何情况都去拉
              //右边条件：如果是数据表，如果有索引且没有init状态的索引表，那么有旧的列校验和可用

              //modify liuxiao [secondary index bug fix]20150812
              /*ObServer master_master_ups;
              if(OB_SUCCESS != (ret = ObChunkServerMain::get_instance()->get_chunk_server().get_rpc_proxy()->get_master_master_update_server(true,master_master_ups)))
              {
                TBSYS_LOG(ERROR,"failed to get ups for report cchecksum");
              }
              else
              {
                if(OB_SUCCESS != (ret = this->manager_.get_old_tablet_column_checksum(tablet->get_range(),column_checksum_)))
                {
                  TBSYS_LOG(ERROR,"merge v1 start get ccchecksum failed ret=%d  table_id=%ld",ret,tablet->get_range().table_id_);
                }
                else if(OB_SUCCESS != (ret = manager_.send_tablet_column_checksum(column_checksum_,new_range_,frozen_version_)))
                {
                  TBSYS_LOG(ERROR,"merge v1 start send ccchecksum failed ret=%d table_id=%ld",ret,tablet->get_range().table_id_);
                }
              }
              */
              //add liumz, [secondary index col checksum]20160405:b
              if (new_range_.end_key_.is_max_row())
              {
                new_range_.end_key_.set_max_row();
                new_range_.border_flag_.unset_inclusive_end();
              }
              //add:e
              if(OB_SUCCESS != (ret = this->manager_.get_old_tablet_column_checksum(new_range_,column_checksum_)))
              {
                if((is_or_have_init_index_ && !is_data_table_) || (!is_have_available_index_ && is_data_table_))
                {
                  //init index  or  data table without available index
                  ret = OB_SUCCESS;
                  TBSYS_LOG(WARN,"merge v1 start get ccchecksum failed ret=%d  table_id=%ld",ret,tablet->get_range().table_id_);
                }
                else
                {
                  TBSYS_LOG(ERROR,"merge v1 start get ccchecksum failed ret=%d  table_id=%ld",ret,tablet->get_range().table_id_);
                }
              }
              else if(OB_SUCCESS != (ret = manager_.send_tablet_column_checksum(column_checksum_,new_range_,frozen_version_)))
              {
                TBSYS_LOG(ERROR,"merge v1 start send ccchecksum failed ret=%d table_id=%ld",ret,tablet->get_range().table_id_);
              }
              TBSYS_LOG(DEBUG,"test::lmz, range:%s, old cc:[%s]", to_cstring(new_range_), column_checksum_.get_str_const());
              //modify e
            }
            //add e

            if (OB_SUCCESS == ret)//add liumz, [can't ignore col_checksum error]20160712
            {
              tablet->set_disk_no(old_tablet_->get_disk_no());
              row_checksum = old_tablet_->get_row_checksum();
              ret = create_hard_link_sstable(sstable_size);
              if (OB_SUCCESS == ret && sstable_size > 0
                  && old_tablet_->get_sstable_id_list().count() > 0)
              {
                trailer = &(dynamic_cast<ObSSTableReader*>(old_tablet_->get_sstable_reader_list().at(0)))->get_trailer();
              }
            }
          }
          else
          {
            //add liuxiao [secondary index col checksum]20150522
            //如果表有增量，就用新算的列校验和,并发送系统表
            if((!is_data_table_ && has_index_or_is_index_) || (has_index_or_is_index_ && is_data_table_))
            {
              if(NULL == column_checksum_.get_str())
              {
                ret = OB_ERROR;
                TBSYS_LOG(ERROR,"column checksum is null");
              }
              else
              {
                //delete by liuxiao [secondary index bug fix] 20150812
                //ObServer master_master_ups;
                //if(OB_SUCCESS != (ret = ObChunkServerMain::get_instance()->get_chunk_server().get_rpc_proxy()->get_master_master_update_server(true,master_master_ups)))
                //{
                //  TBSYS_LOG(ERROR,"failed to get ups for report cchecksum");
                //}
                //delete e
                //add liumz, [secondary index col checksum]20160405:b
                if (new_range_.end_key_.is_max_row())
                {
                  new_range_.end_key_.set_max_row();
                  new_range_.border_flag_.unset_inclusive_end();
                }
                //add:e
                TBSYS_LOG(DEBUG,"test::lmz, range:%s, new cc:[%s]", to_cstring(new_range_), column_checksum_.get_str_const());
                if(OB_SUCCESS != (ret = manager_.send_tablet_column_checksum(column_checksum_,new_range_,frozen_version_)))
                {
                  TBSYS_LOG(ERROR,"lixuaio merge v1 start get ccchecksum");
                }
              }
            }
            //add e
            trailer = &writer_.get_trailer();
            tablet->set_disk_no( (sstable_id_.sstable_file_id_) & DISK_NO_MASK);
          }

          if (OB_SUCCESS == ret)
          {
            //set new data version
            tablet->set_data_version(frozen_version_);
            if (tablet_merge_filter_.need_filter())
            {
              extend_info.last_do_expire_version_ = frozen_version_;
            }
            else
            {
              extend_info.last_do_expire_version_ = old_tablet_->get_last_do_expire_version();
            }
            if (sstable_size > 0)
            {
              if (NULL == trailer)
              {
                TBSYS_LOG(ERROR, "after close sstable, trailer=NULL, "
                                 "is_tablet_unchanged=%d, sstable_size=%ld",
                  is_tablet_unchanged, sstable_size);
                ret = OB_ERROR;
              }
              else if ((ret = tablet->add_sstable_by_id(sstable_id_)) != OB_SUCCESS)
              {
                TBSYS_LOG(ERROR,"Merge : add sstable to tablet failed.");
              }
              else
              {
                int64_t pos = 0;
                int64_t checksum_len = sizeof(uint64_t);
                char checksum_buf[checksum_len];

                /**
                 * compatible with the old version, before the sstable code
                 * don't support empty sstable, if it's empty, no sstable file
                 * is created. now it support empty sstable, and there is no row
                 * data in sstable, the statistic of tablet remain constant, the
                 * occupy size, row count adn checksum is 0
                 *
                 */
                if (trailer->get_row_count() > 0)
                {
                  extend_info.occupy_size_ = sstable_size;
                  extend_info.row_count_ = trailer->get_row_count();
                  ret = serialization::encode_i64(checksum_buf,
                      checksum_len, pos, trailer->get_sstable_checksum());
                  if (OB_SUCCESS == ret)
                  {
                    extend_info.check_sum_ = ob_crc64(checksum_buf, checksum_len);
                  }
                }
              }
            }
            else if (0 == sstable_size)
            {
              /**
               * the tablet doesn't include sstable, the occupy_size_,
               * row_count_ and check_sum_ in extend_info is 0, needn't set
               * them again
               */
            }

            //inherit sequence number from parent tablet
            extend_info.sequence_num_ = old_tablet_->get_sequence_num();
            extend_info.row_checksum_ = row_checksum;
          }

          /**
           * we set the extent info of tablet here. when we write the
           * index file, we needn't load the sstable of this tablet to get
           * the extend info.
           */
          if (OB_SUCCESS == ret)
          {
            tablet->set_extend_info(extend_info);
          }
        }

        if (OB_SUCCESS == ret)
        {
          ret = tablet_array_.push_back(tablet);
        }

        if (OB_SUCCESS == ret)
        {
          manager_.get_disk_manager().add_used_space(
            (sstable_id_.sstable_file_id_ & DISK_NO_MASK),
            sstable_size, !is_tablet_unchanged);
        }

        if (OB_SUCCESS == ret && NULL != tablet)
        {
          new_tablet = tablet;

          if (is_tablet_unchanged)
          {
            char range_buf[OB_RANGE_STR_BUFSIZ];
            new_range_.to_string(range_buf,sizeof(range_buf));
            TBSYS_LOG(INFO, "finish_sstable, create hard link sstable=%s, range=%s, "
                            "sstable_size=%ld, row_count=%ld",
              path_, range_buf, sstable_size, extend_info.row_count_);
          }
        }
      }
      if (OB_SUCCESS == ret)
      {
        path_[0] = '\0';
        current_sstable_size_ = 0;
      }
      return ret;
    }

    int ObTabletMergerV1::report_tablets(ObTablet* tablet_list[],int32_t tablet_size)
    {
      int  ret = OB_SUCCESS;
      int64_t num = OB_MAX_TABLET_LIST_NUMBER;
      ObTabletReportInfoList *report_info_list =  GET_TSI_MULT(ObTabletReportInfoList, TSI_CS_TABLET_REPORT_INFO_LIST_1);

      if (tablet_size < 0)
      {
        ret = OB_INVALID_ARGUMENT;
      }
      else if (NULL == report_info_list)
      {
        ret = OB_ALLOCATE_MEMORY_FAILED;
      }
      else
      {
        report_info_list->reset();
        ObTabletReportInfo tablet_info;
        for(int32_t i=0; i < tablet_size && num > 0 && OB_SUCCESS == ret; ++i)
        {
          manager_.fill_tablet_info(*tablet_list[i], tablet_info);
          if (OB_SUCCESS != (ret = report_info_list->add_tablet(tablet_info)) )
          {
            TBSYS_LOG(WARN, "failed to add tablet info, num=%ld, err=%d", num, ret);
          }
          else
          {
            --num;
          }
        }
        if (OB_SUCCESS != (ret = manager_.send_tablet_report(*report_info_list,true))) //always has more
        {
          TBSYS_LOG(WARN, "failed to send request report to rootserver err = %d",ret);
        }
      }
      return ret;
    }

    bool ObTabletMergerV1::maybe_change_sstable() const
    {
      bool ret = false;
      int64_t rowkey_cmp_size = new_table_schema_->get_split_pos();
      const uint64_t table_id = row_.get_table_id();
      int64_t rowkey_column_count = 0;
      ObRowkey last_rowkey;

      if (row_.is_binary_rowkey())
      {
        rowkey_column_count = row_.get_rowkey_info()->get_size();
      }
      else
      {
        sstable_schema_.get_rowkey_column_count(table_id, rowkey_column_count);
      }
      last_rowkey.assign(NULL, rowkey_column_count);
      row_.get_rowkey(last_rowkey);

      if (0 < rowkey_cmp_size
          && last_rowkey.get_obj_cnt() >= rowkey_cmp_size
          && cell_->row_key_.get_obj_cnt() >= rowkey_cmp_size)
      {
        for (int i = 0; i < rowkey_cmp_size; ++i)
        {
          if (last_rowkey.get_obj_ptr()[i] != cell_->row_key_.get_obj_ptr()[i])
          {
            ret = true;
          }
        }
      }
      else
      {
        ret = true;
      }
      return ret;
    }



    //add by zhaoqiong [bugfix: create table][schema sync]20160106:b
    int ObTabletMergerV1::create_empty_tablet_and_upgrade(ObTablet *&new_tablet, int64_t frozen_version)
    {
      int ret = OB_SUCCESS;
      bool  sync_meta = THE_CHUNK_SERVER.get_config().each_tablet_sync_meta;
      if (!old_tablet_->get_range().is_whole_range())
      {
        ret = OB_INNER_STAT_ERROR;
        TBSYS_LOG(ERROR,"inner state error.");
      }
      else if (OB_SUCCESS != (ret = manager_.get_serving_tablet_image().alloc_tablet_object(old_tablet_->get_range(),frozen_version,new_tablet)))
      {
        TBSYS_LOG(ERROR,"alloc_tablet_object failed.");
      }
      else
      {
        // in case we have migrated tablets, discard current merge tablet
        if (!old_tablet_->is_merged())
        {
          // add empty tablet, there is no sstable files in it.
          // if scan or get query on this tablet, scan will return empty dataset.
          new_tablet->set_data_version(frozen_version);
          // assign a disk for new tablet.
          int32_t disk_no = manager_.get_disk_manager().get_dest_disk();
          new_tablet->set_disk_no(disk_no);
          manager_.get_disk_manager().add_used_space(disk_no, 0);
          if (OB_SUCCESS != (ret = manager_.get_serving_tablet_image().upgrade_tablet(old_tablet_, &new_tablet, 1, false)))
          {
            TBSYS_LOG(WARN,"upgrade new merged tablets error [%d]",ret);
          }

          if (ret == OB_SUCCESS)
          {
            if(sync_meta)
            {
              // sync new tablet meta files
              if (OB_SUCCESS != (ret = manager_.get_serving_tablet_image().write(frozen_version, new_tablet->get_disk_no())))
              {
                TBSYS_LOG(ERROR, "write new meta failed, version=%ld, disk=%d", frozen_version, new_tablet->get_disk_no());
              }
              // sync old tablet meta files
              else if (OB_SUCCESS != (ret = manager_.get_serving_tablet_image().write(old_tablet_->get_data_version(), old_tablet_->get_disk_no())))
              {
                TBSYS_LOG(ERROR, "write old meta failed, version=%ld, disk=%d", old_tablet_->get_data_version(), old_tablet_->get_disk_no());
              }
            }
          }
        }
        else
        {
          TBSYS_LOG(INFO, "current tablet covered by migrated tablets, discard.");
        }
      }

      return ret;
    }
    //add:e

    //add liuxiao [secondary index col checksum] 20150407
    //判定是否为索引表，或为带索引表的数据表
    int ObTabletMergerV1::set_has_index_or_is_index(const uint64_t table_id)
    {
      int ret = OB_SUCCESS;
      /* del liumz, [use frozen version's schema manager]20160113
      common::ObMergerSchemaManager* merge_schema = get_global_sstable_schema_manager();
      const ObSchemaManagerV2* schemav2 = NULL;
      */
      const ObTableSchema* ts = NULL;
      const ObTableSchema* ts_of_data_table = NULL;
      IndexList index_list;
      /* del liumz, [use frozen version's schema manager]20160113
      if(merge_schema == NULL)
      {
        TBSYS_LOG(ERROR,"get merge schema manager error,ret=%d",ret);
        ret=OB_SCHEMA_ERROR;
      }
      else
      {
        schemav2 = merge_schema->get_schema(table_id);  //获取表的schema
      }
      */
      // add liumz, [use frozen version's schema manager]20160113:b
      const ObSchemaManagerV2* schemav2 = &(chunk_merge_.current_schema_);
      //add:e
      if(schemav2 == NULL)
      {
        TBSYS_LOG(ERROR,"get schema manager v2 error,table_id:%ld,ret=%d",table_id,ret);
        ret = OB_SCHEMA_ERROR;
      }
      else if (NULL == (ts = schemav2->get_table_schema(table_id)))
      {
        TBSYS_LOG(ERROR,"get_table_schema error,table_id:%ld,ret=%d",table_id,ret);
        ret = OB_SCHEMA_ERROR;
      }
      /* del liumz, [use frozen version's schema manager]20160113
      if(ts == NULL)
      {
        TBSYS_LOG(ERROR,"get_table_schema error,table_id:%ld,ret=%d",table_id,ret);
        ret = OB_SCHEMA_ERROR;
      }*/
      else
      {
        if(ts->get_index_helper().tbl_tid != OB_INVALID_ID)
        {
          //有对应的数据表,就是索引表
          has_index_or_is_index_ = true;
          is_data_table_ = false;
          is_or_have_init_index_ = (INDEX_INIT == ts->get_index_helper().status);

          int64_t index_table_rowkey_count = ts->get_rowkey_info().get_size();
          ts_of_data_table = schemav2->get_table_schema(ts->get_index_helper().tbl_tid);
          if(NULL != ts_of_data_table)
          {
            int64_t data_table_rowkey_count = ts_of_data_table->get_rowkey_info().get_size();
            index_column_num = index_table_rowkey_count - (int64_t)data_table_rowkey_count;
            if(index_column_num < 0) //modify liuxiao [secondary index bug.fix] 20150707
            {
              ret = OB_ERROR;
              TBSYS_LOG(WARN,"index_column_num is not right = %ld",index_column_num);
            }
          }
          else
          {
            ret = OB_ERROR;
            TBSYS_LOG(WARN,"data_table is null! table_id:%ld",ts->get_index_helper().tbl_tid);
          }
        }
        else if(OB_SUCCESS != (ret = schemav2->get_index_list(table_id,index_list)))
        {
          TBSYS_LOG(ERROR,"failed to get index list ret = %d",ret);
        }
        else if(index_list.get_count() > 0)
        {
          //是数据表,判断索引表数量，大于0就是有索引表的
          has_index_or_is_index_ = true;
          is_data_table_ = true;
          is_have_available_index_ = schemav2->is_have_available_index(table_id);
          is_or_have_init_index_ = schemav2->is_have_init_index(table_id);
          //TBSYS_LOG(INFO,"test:liuxiao this table need cal column checksum table_id = %ld",table_id);
        }
        else
        {
          //既不是索引表，也不是有索引表的数据表
          has_index_or_is_index_ = false;
          is_data_table_ = false;
          //TBSYS_LOG(INFO,"test:liuxiao this table do not need cal column checksum table_id = %ld",table_id);
        }
      }
      /* del liumz, [use frozen version's schema manager]20160113
      //add wenghaixing [secondary index static_index_build.bug_fix.schema_error]20150509
      if(NULL != schemav2 && NULL != merge_schema )
      {
          int err = merge_schema->release_schema(schemav2);
          if(OB_SUCCESS != err)
          {
            ret = err;
            TBSYS_LOG(WARN, "release schema failed, ret = %d", ret);
          }
      }
      //add e
      */
      return ret;
    }
    //add e
    //add wenghaixing [secondary index static_index_build.merge]20150422
    int ObTabletMergerV1::stop_invalid_index_tablet_merge(ObTablet *tablet, bool &invalid)
    {
      int ret = OB_SUCCESS;
      uint64_t table_id = OB_INVALID_ID;
      ObTableSchema * schema = NULL;
      invalid = false;
      if(NULL == tablet)
      {
        ret = OB_INVALID_ARGUMENT;
        TBSYS_LOG(ERROR, "null pointer of merge tablet");
      }
      if(OB_SUCCESS == ret)
      {
        table_id = tablet->get_range().table_id_;
        schema = chunk_merge_.current_schema_.get_table_schema(table_id);
      }

      if(NULL != schema && OB_INVALID_ID != schema->get_index_helper().tbl_tid && AVALIBALE != schema->get_index_helper().status)
      {
        tablet->set_removed();
        invalid = true;
        TBSYS_LOG(INFO, "unvaliabale index tablet cannot be merged!");
      }
      return ret;
    }
    //add e
    //add liuxiao zhuyanchao[secondary index col checksum]20150409
    int ObTabletMergerV1::cons_row_desc_local_tablet(ObRowDesc &rowkey_desc,ObRowDesc &index_desc,uint64_t tid)
    {
      //该函数只用于处理数据表
      //构造数据表的需要计算列校验和的row desc
      //rowkey_desc保存原表主键
      //index_desc保存除了原表主键的索引表中的列
      int ret = OB_SUCCESS;
      int64_t count = 0;
      //int64_t rowkey_count = 0;
      bool hint_index = false;
      /* del liumz, [use frozen version's schema manager]20160113
      common::ObMergerSchemaManager* merge_schema = get_global_sstable_schema_manager();
      const ObSchemaManagerV2 *schemav2 = NULL;
      */
      const ObTableSchema* schema = NULL;
      const ObSSTableSchemaColumnDef* col_def = NULL;
      /* del liumz, [use frozen version's schema manager]20160113
      if(NULL == merge_schema)
      {
        TBSYS_LOG(ERROR,"get merge_schema error");
        ret = OB_SCHEMA_ERROR;
      }
      else if(NULL == (schemav2 = merge_schema->get_schema(tid)))
      {
        TBSYS_LOG(ERROR,"get schema manager v2 error,ret=%d",ret);
        ret = OB_SCHEMA_ERROR;
      }
      else
      {
        schema = schemav2->get_table_schema(tid);
      }*/
      // add liumz, [use frozen version's schema manager]20160113:b
      const ObSchemaManagerV2* schemav2 = &(chunk_merge_.current_schema_);
      if(schemav2 == NULL)
      {
        TBSYS_LOG(ERROR,"get schema manager v2 error,table_id:%ld,ret=%d",tid,ret);
        ret = OB_SCHEMA_ERROR;
      }
      else if (NULL == (schema = schemav2->get_table_schema(tid)))  //获取表的table schema
      {
        TBSYS_LOG(ERROR,"get table schema error,ret=%d",ret);
        ret = OB_SCHEMA_ERROR;
      }
      //add:e
      /* del liumz, [use frozen version's schema manager]20160113
      if(OB_SUCCESS != ret || schema == NULL)
      {
        TBSYS_LOG(ERROR,"get table schema error,ret=%d",ret);
        ret = OB_SCHEMA_ERROR;
      }*/
      else
      {
        count = sstable_schema_.get_column_count(); //count保存原表列个数
        //rowkey_count = schema->get_rowkey_info().get_size();
        for(int i = 0;i < count;i++)
        {   
          if(NULL == (col_def = sstable_schema_.get_column_def((int)i)))
          {
            ret =OB_ERROR;
            TBSYS_LOG(WARN, "failed to get column def of table[%ld], i[%d],ret[%d]", tid, i, ret);
            break;
          }
          //遍历原表的所有列，并只保留在索引表中有的列
          if(OB_SUCCESS != (ret = schemav2->column_hint_index(tid,col_def->column_name_id_,hint_index)))
          {
            TBSYS_LOG(ERROR,"failed to check if hint index,ret=%d",ret);
            break;
          }
          else if(hint_index)
          {
            //该列在索引表中有
            if(schema->get_rowkey_info().is_rowkey_column(col_def->column_name_id_))
            {
              //如果是主键则单独加入到rowkey_desc中
              if(OB_SUCCESS != (ret = rowkey_desc.add_column_desc(tid, i)))
              {
                TBSYS_LOG(WARN, "failed to set column rowkey_desc table_id[%ld], ret[%d]", tid, ret);
                break;
              }
            }
            else
            {
              //不是主键，就加入到index_desc中
              if(OB_SUCCESS != (ret = index_desc.add_column_desc(tid, i)))
              {
                TBSYS_LOG(WARN, "failed to set column index_desc table_id[%ld], ret[%d]", tid, ret);
                break;
              }
            }
          }
        }
      }
      /* del liumz, [use frozen version's schema manager]20160113
      //add wenghaixing [secondary index static_index_build.bug_fix.schema_error]20150509
      if(NULL != schemav2 && NULL != merge_schema )
      {
          int err = merge_schema->release_schema(schemav2);
          if(OB_SUCCESS != err)
          {
            ret = err;
            TBSYS_LOG(WARN, "release schema failed, ret = %d", ret);
          }
      }
      //add e
      */
      return ret;
    }

    int ObTabletMergerV1::cons_row_desc_without_virtual(ObRowDesc &rowkey_desc,ObRowDesc &index_desc,uint64_t &max_data_table_cid,uint64_t tid)
    {
      //用于处理索引表
      //rowkey_desc保存原表主键
      //index_desc保存除了原表主键的索引表中的列
      //由于是obsstaberow不是obrow，无法用列id定位某一个obj，所以使用i表示，i从0开始
      int ret = OB_SUCCESS;
      int64_t column_count = 0;
      uint64_t data_tid = OB_INVALID_ID;
      const ObTableSchema* index_schema;
      const ObTableSchema* data_schema;
      const ObSSTableSchemaColumnDef* col_def = NULL;
      /* del liumz, [use frozen version's schema manager]20160113
      const ObSchemaManagerV2 *schemav2 = NULL;
      common::ObMergerSchemaManager* merge_schema = get_global_sstable_schema_manager();
      if(NULL == merge_schema)
      {
        TBSYS_LOG(ERROR, "merge_schema is null ");
        ret =OB_ERROR;
      }
      else if(NULL == (schemav2 = merge_schema->get_schema(tid)))
      {
        TBSYS_LOG(ERROR, "schemav2 is null ");
        ret =OB_ERROR;
      }*/
      // add liumz, [use frozen version's schema manager]20160113:b
      const ObSchemaManagerV2* schemav2 = &(chunk_merge_.current_schema_);
      if(schemav2 == NULL)
      {
        TBSYS_LOG(ERROR,"get schema manager v2 error,table_id:%ld,ret=%d",tid,ret);
        ret = OB_SCHEMA_ERROR;
      }
      //add:e
      else
      {
        index_schema = schemav2->get_table_schema(tid);
        if(NULL == index_schema)
        {
          TBSYS_LOG(ERROR, "failed to get index_schema");
          ret = OB_SCHEMA_ERROR;
        }
        else if(OB_INVALID_ID == (data_tid = index_schema->get_index_helper().tbl_tid))
        {
          TBSYS_LOG(ERROR, "failed to get data_table_id");
          ret =OB_ERROR;
        }
        else if(NULL == (data_schema = schemav2->get_table_schema(data_tid)))
        {
          TBSYS_LOG(ERROR, "failed to get data_table_schema");
          ret =OB_ERROR;
        }
        else
        {
          max_data_table_cid = data_schema->get_max_column_id();
          column_count = sstable_schema_.get_column_count();
          for(int64_t i = 0; i< column_count; i++)
          {
            if(NULL == (col_def = sstable_schema_.get_column_def((int)i)))
            {
              TBSYS_LOG(WARN, "failed to get column def of table[%ld], i[%ld],ret[%d]", tid, i, ret);
              ret =OB_ERROR;
              break;
            }
            else if(data_schema->get_rowkey_info().is_rowkey_column(col_def->column_name_id_))
            {
              if(OB_SUCCESS != (ret = rowkey_desc.add_column_desc(tid, i)))
              {
                //这里只能保存下标，不保存列id
                TBSYS_LOG(WARN, "failed to set column rowkey_desc table_id[%ld], ret[%d]", tid, ret);
                break;
              }
            }
            else
            {
              if(OB_SUCCESS != (ret = index_desc.add_column_desc(tid, i)))
              {
                TBSYS_LOG(WARN, "failed to set column rowkey_desc table_id[%ld], ret[%d]", tid, ret);
                break;
              }
            }
          }
        }
      }
      /* del liumz, [use frozen version's schema manager]20160113
      //add wenghaixing [secondary index static_index_build.bug_fix.schema_error]20150509
      if(NULL != schemav2 && NULL != merge_schema )
      {
        int err = merge_schema->release_schema(schemav2);
        if(OB_SUCCESS != err)
        {
          ret = err;
          TBSYS_LOG(WARN, "release schema failed, ret = %d", ret);
        }
      }
      //add e
      */
      return ret;
    }

    int ObTabletMergerV1::calc_tablet_col_checksum_index_local_tablet(const sstable::ObSSTableRow& row, ObRowDesc rowkey_desc, ObRowDesc index_desc, char *column_checksum)
    {
      //用于计算原表的列校验和
      //rowkey_desc保存了主键的信息,index_desc保存了非主键列，但在索引表中的列（index列或storing 列）
      int ret = OB_SUCCESS;
      int64_t index_count = 0;
      int64_t rowkey_count = 0;
      int64_t i = 0;
      const ObRowDesc::Desc *desc_rowkey = rowkey_desc.get_cells_desc_array(rowkey_count);
      const ObRowDesc::Desc *desc_index = index_desc.get_cells_desc_array(index_count);
      int pos = 0,len = 0;
      const ObObj* obj = NULL;
      uint64_t column_id = OB_INVALID_ID;
      ObBatchChecksum bc;
      uint64_t rowkey_checksum = 0;
      const ObSSTableSchemaColumnDef* col_def = NULL;
      if(OB_SUCCESS == ret)
      {
        for(i = 0;i < rowkey_count;i++)
        {
          //先计算所有主键列的校验和
          obj = row.get_obj((int)(desc_rowkey[i].column_id_));
          bc.reset();
          if(obj == NULL)
          {
            ret = OB_ERROR;
            TBSYS_LOG(ERROR, "get sstable row obj error,ret=%d", ret);
            break;
          }
          else
          {
            obj->checksum(bc);
            rowkey_checksum += bc.calc();
          }
        }
        //0 for rowkey column checksum
        len = snprintf(column_checksum + pos,OB_MAX_COL_CHECKSUM_STR_LEN-1-pos,"%ld",(uint64_t)0);
        if(len < 0)
        {
          TBSYS_LOG(ERROR,"write column checksum error");
          ret = OB_ERROR;
        }
        else
        {
          pos += len;
          column_checksum[pos++] = ':';
          if(pos < OB_MAX_COL_CHECKSUM_STR_LEN-1)
          {
            len = snprintf(column_checksum+pos,OB_MAX_COL_CHECKSUM_STR_LEN-1-pos,"%lu",rowkey_checksum);
            pos += len;
          }
          //mod liumz, [secondary index col checksum bugfix]20160406:b
          //if(i != ((rowkey_count+index_count)-1))
          if(0 != index_count)
          //mod:e
          {
            column_checksum[pos++] = ',';
          }
          else
          {
            column_checksum[pos++] = '\0';
          }
        }
      }

      if(OB_SUCCESS == ret)
      {
        //计算所有剩余列
        for(i = 0;i < index_count;i++)
        {
          obj = row.get_obj((int)(desc_index[i].column_id_));
          bc.reset();
          if(obj == NULL)
          {
            ret = OB_ERROR;
            TBSYS_LOG(ERROR, "get sstable row obj error,ret=%d", ret);
            break;
          }
          else if(NULL == (col_def = sstable_schema_.get_column_def((int)desc_index[i].column_id_)))
          {
            ret = OB_ERROR;
            TBSYS_LOG(WARN, "failed to get column def in cal col checksum");
            break;
          }
          else
          {
            obj->checksum(bc);
          }
          column_id = col_def->column_name_id_;
          len = snprintf(column_checksum+pos,OB_MAX_COL_CHECKSUM_STR_LEN-1-pos,"%ld",column_id);
          if(len < 0)
          {
            TBSYS_LOG(ERROR,"write column checksum error");
            ret = OB_ERROR;
            break;
          }
          else
          {
            pos += len;
            column_checksum[pos++] = ':';
            if(pos < OB_MAX_COL_CHECKSUM_STR_LEN-1)
            {
              len = snprintf(column_checksum+pos,OB_MAX_COL_CHECKSUM_STR_LEN-1-pos,"%lu",bc.calc());
              pos += len;
            }
            //mod liumz, [secondary index col checksum bugfix]20160406:b
            //if(i != ((rowkey_count+index_count)-1))
            if(i != (index_count - 1))
            //mod:e
            {
              column_checksum[pos++] = ',';
            }
            else
            {
              column_checksum[pos++] = '\0';
            }
          }
        }
      }
      return ret;
    }



    int ObTabletMergerV1::calc_tablet_col_checksum_index_index_tablet(const sstable::ObSSTableRow& row, ObRowDesc rowkey_desc,ObRowDesc index_desc, char *column_checksum, const uint64_t max_data_table_cid)
    {
      //该方法专门用于处理索引表的列校验和
      int ret = OB_SUCCESS;
      int64_t rowkey_count = rowkey_desc.get_column_num(); //原表主键列
      int64_t index_count = index_desc.get_column_num(); //保存除了原表主键列以外的列（包括storing列）
      const ObRowDesc::Desc *desc_rowkey = rowkey_desc.get_cells_desc_array(rowkey_count);
      const ObRowDesc::Desc *desc_index = index_desc.get_cells_desc_array(index_count);
      int64_t i = 0;
      int pos = 0,len = 0;
      const ObObj* obj = NULL;
      uint64_t column_id = OB_INVALID_ID;
      ObBatchChecksum bc;
      uint64_t rowkey_checksum = 0;
      const ObSSTableSchemaColumnDef* col_def = NULL;
      if(OB_SUCCESS == ret)
      {
        //rowkey_count为原表主键个数
        for(i = 0;i < rowkey_count;i++)
        {
          //先计算主键校验和
          obj = row.get_obj((int)desc_rowkey[i].column_id_);
          bc.reset();
          if(obj == NULL)
          {
            ret = OB_ERROR;
            TBSYS_LOG(ERROR, "get sstable row obj error,ret=%d", ret);
            break;
          }
          else
          {
            obj->checksum(bc);
            rowkey_checksum += bc.calc();
          }
        }
        //0 for rowkey column checksum
        len = snprintf(column_checksum + pos,OB_MAX_COL_CHECKSUM_STR_LEN-1-pos,"%ld",(uint64_t)0);
        if(len < 0)
        {
          TBSYS_LOG(ERROR,"write column checksum error");
          ret = OB_ERROR;
        }
        else
        {
          pos += len;
          column_checksum[pos++] = ':';
          if(pos < OB_MAX_COL_CHECKSUM_STR_LEN-1)
          {
            len = snprintf(column_checksum+pos,OB_MAX_COL_CHECKSUM_STR_LEN-1-pos,"%lu",rowkey_checksum);
            pos += len;
          }
          //mod liumz, [secondary index col checksum bugfix]20160406:b
          //if(i != ((rowkey_count+index_count)-1)
          if(0 != index_count)
          //mod:e
          {
            column_checksum[pos++] = ',';
          }
          else
          {
            column_checksum[pos++] = '\0';
          }
        }
      }

      if(OB_SUCCESS == ret)
      {
        for(i = 0;i < index_count;i++)
        {
          obj = row.get_obj((int)desc_index[i].column_id_);
          bc.reset();
          if(obj == NULL)
          {
            ret = OB_ERROR;
            TBSYS_LOG(ERROR, "get sstable row obj error,ret=%d", ret);
            break;
          }
          else if(NULL == (col_def = sstable_schema_.get_column_def((int)desc_index[i].column_id_)))
          {
            ret = OB_ERROR;
            TBSYS_LOG(WARN, "failed to get column def in cal col checksum");
            break;
          }
          else
          {
            obj->checksum(bc);
          }
          column_id = col_def->column_name_id_;
          if(column_id > max_data_table_cid)
          {
            continue;
          }
          len = snprintf(column_checksum+pos,OB_MAX_COL_CHECKSUM_STR_LEN-1-pos,"%ld",column_id);
          if(len < 0)
          {
            TBSYS_LOG(ERROR,"write column checksum error");
            ret = OB_ERROR;
            break;
          }
          else
          {
            pos += len;
            column_checksum[pos++] = ':';
            if(pos < OB_MAX_COL_CHECKSUM_STR_LEN-1)
            {
              len = snprintf(column_checksum+pos,OB_MAX_COL_CHECKSUM_STR_LEN-1-pos,"%lu",bc.calc());
              pos += len;
            }
            //mod liumz, [secondary index col checksum bugfix]20160406:b
            //if(i != ((rowkey_count+index_column_num)-1))
            if(i != (index_count - 1))
            //mod:e
            {
              column_checksum[pos++] = ',';
            }
            else
            {
              column_checksum[pos++] = '\0';
            }
          }
        }
      }
      return ret;
    }
    //add e

  } /* chunkserver */
} /* oceanbase */
