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


#include "ob_tablet_merger_v2.h"
#include "common/ob_schema.h"
#include "common/file_directory_utils.h"
#include "compactsstablev2/ob_sstable_store_struct.h"
#include "compactsstablev2/ob_sstable_schema.h"
#include "sql/ob_sql_scan_param.h"
#include "ob_chunk_server_main.h"
#include "ob_chunk_merge.h"
#include "ob_tablet_manager.h"

namespace oceanbase
{
  namespace chunkserver
  {
    using namespace tbutil;
    using namespace common;
    using namespace sstable;

    /*-----------------------------------------------------------------------------
     *  ObTabletMergerV2
     *-----------------------------------------------------------------------------*/

    ObTabletMergerV2::ObTabletMergerV2(ObChunkMerge& chunk_merge, ObTabletManager& manager) 
      : ObTabletMerger(chunk_merge, manager)
    {}

    int ObTabletMergerV2::init()
    {
      int ret = OB_SUCCESS;
      ObChunkServer&  chunk_server = ObChunkServerMain::get_instance()->get_chunk_server();

      if (OB_SUCCESS == ret)
      {
        int64_t dio_type = chunk_server.get_config().write_sstable_use_dio;
        bool dio = false;
        if (dio_type > 0)
        {
          dio = true;
        }
      }

      return ret;
    }


    int ObTabletMergerV2::reset()
    {
      frozen_version_ = 0;

      sstable_id_.sstable_file_id_ = 0;
      path_[0] = 0;

      sstable_schema_.reset();
      tablet_array_.clear();

      sql_scan_param_.reset();
      tablet_scan_.reset();
      op_ups_scan_.reset();
      op_ups_multi_get_.reset();
      writer_.reset();
      return OB_SUCCESS;
    }

    int ObTabletMergerV2::init_sstable_writer(const common::ObTableSchema & table_schema, 
        const ObTablet* tablet, const int64_t frozen_version)
    {
      int ret = OB_SUCCESS;
      compactsstablev2::ObFrozenMinorVersionRange version_range;
      version_range.major_version_ = frozen_version;
      ObCompactStoreType store_type = DENSE_DENSE; 
      int64_t table_count = 1;  // chunkserver sstable always 1

      // if schema define sstable block size for table, use it
      // for the schema with version 2, the default block size is 64(KB),
      // we skip this case and use the config of chunkserver
      int64_t sstable_block_size = OB_DEFAULT_SSTABLE_BLOCK_SIZE;
      if (table_schema.get_block_size() > 0
          && 64 != table_schema.get_block_size())
      {
        sstable_block_size = table_schema.get_block_size();
      }

      int64_t max_sstable_size = OB_DEFAULT_MAX_TABLET_SIZE;
      int64_t min_split_sstable_size = 0;
      // calc default sstable max size and min split size.
      // if schema define max sstable size for table, use it
      if (table_schema.get_max_sstable_size() > 0)
      {
        max_sstable_size = table_schema.get_max_sstable_size();
      }

      int64_t over_size_percent = THE_CHUNK_SERVER.get_config().over_size_percent_to_split;
      if (over_size_percent > 0)
      {
        min_split_sstable_size = max_sstable_size * over_size_percent / 100;
      }


      const char *compressor_name = NULL;
      ObString compressor_string;
      if (NULL == (compressor_name = table_schema.get_compress_func_name()) 
          || 0 == *compressor_name)
      {
        TBSYS_LOG(WARN,"no compressor with this sstable. table id = (%lu)", 
            tablet->get_range().table_id_);
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        compressor_string.assign_ptr(const_cast<char *>(compressor_name),
            static_cast<int32_t>(strlen(compressor_name)));
      }



      if (OB_SUCCESS != (ret = writer_.set_sstable_param(version_range, 
              store_type, table_count, sstable_block_size, compressor_string, 
              max_sstable_size, min_split_sstable_size)))
      {
        TBSYS_LOG(WARN, "ret=%d,set_sstable_param error, major_version_=%ld,"
            "store_type=%d, table_count=%ld, sstable_block_size=%ld,"
            "compressor_name=%s, max_sstable_size=%ld, min_split_sstable_size=%ld",
            ret, frozen_version_, store_type, table_count, sstable_block_size,
            compressor_name, max_sstable_size, sstable_block_size);
      }
      else if (OB_SUCCESS != (ret = writer_.set_table_info(tablet->get_range().table_id_, 
              sstable_schema_, tablet->get_range())))
      {
        TBSYS_LOG(WARN, "set_table_info error, ret=%d, range=%s", ret, to_cstring(tablet->get_range()));
      }
      else
      {
        // log merge start info...
        int64_t sstable_id = 0;
        ObSSTableId sst_id;
        char path[OB_MAX_FILE_NAME_LENGTH];
        path[0] = '\0';

        if ((tablet->get_sstable_id_list()).count() > 0)
          sstable_id = (tablet->get_sstable_id_list()).at(0).sstable_file_id_;

        if (0 != sstable_id)
        {
          sst_id.sstable_file_id_ = sstable_id;
          get_sstable_path(sst_id, path, sizeof(path));
        }

        TBSYS_LOG(INFO, "start merge sstable_id:%ld, old_version=%ld, "
            "new_version=%ld, table_row_count=%ld, "
            "tablet_occupy_size=%ld, compressor=%s, path=%s, range=%s,"
            "max_sstable_size=%ld, min_split_sstable_size=%ld, sstable_block_size=%ld",
            sstable_id, tablet->get_data_version(), frozen_version,
            tablet->get_row_count(), tablet->get_occupy_size(), 
            compressor_name, path, to_cstring(tablet->get_range()), 
            max_sstable_size, min_split_sstable_size, sstable_block_size);
      }

      return ret;
    }

    int ObTabletMergerV2::prepare_merge(ObTablet *tablet, int64_t frozen_version)
    {
      int ret = OB_SUCCESS;
      ObTableSchema* table_schema = NULL;
      if (NULL == tablet || frozen_version <= 0)
      {
        TBSYS_LOG(ERROR,"merge : interal error, param invalid tablet[%p], frozen_version:[%ld]",
            tablet, frozen_version);
        ret = OB_INVALID_ARGUMENT;
      }
      else if ( OB_SUCCESS != (ret = reset()) )
      {
        TBSYS_LOG(ERROR, "reset query thread local buffer error.");
      }
      else if ( NULL == (table_schema =
            chunk_merge_.current_schema_.get_table_schema(tablet->get_range().table_id_)) )
      {
        //This table has been deleted
        TBSYS_LOG(INFO,"table (%lu) has been deleted",tablet->get_range().table_id_);
        tablet->set_merged();
        ret = OB_CS_TABLE_HAS_DELETED;
      }
      else if (OB_SUCCESS != (ret = build_sstable_schema(tablet->get_range().table_id_, sstable_schema_)))
      {
        TBSYS_LOG(ERROR, "convert table schema to sstable schema failed, table=%ld",
            tablet->get_range().table_id_);
      }
      else if (OB_SUCCESS != (ret = init_sstable_writer(*table_schema, tablet, frozen_version)))
      {
        TBSYS_LOG(ERROR, "init_sstable_writer failed, ret=%d, table=%ld",
            ret, tablet->get_range().table_id_);
      }
      else
      {
        old_tablet_ = tablet;
        frozen_version_ = frozen_version;

      }
      return ret;
    }

    int ObTabletMergerV2::wait_aio_buffer() const
    {
      int ret = OB_SUCCESS;
      int status = 0;

      ObThreadAIOBufferMgrArray* aio_buf_mgr_array = 
        GET_TSI_MULT(ObThreadAIOBufferMgrArray, TSI_SSTABLE_THREAD_AIO_BUFFER_MGR_ARRAY_1);
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

    int ObTabletMergerV2::open()
    {
      int ret = OB_SUCCESS;
      int64_t network_timeout = 0;
      //ObFilter filter;
      ObChunkServer & chunkserver = THE_CHUNK_SERVER;
      network_timeout = chunkserver.get_config().network_timeout;
      ScanContext scan_context;
      manager_.build_scan_context(scan_context);
      tablet_scan_.set_scan_context(scan_context);

      int64_t now = tbsys::CTimeUtil::getTime();
      tablet_scan_.set_ts_timeout_us(now + network_timeout);

      if (OB_SUCCESS != (ret = build_sql_scan_param(sstable_schema_, sql_scan_param_)))
      {
        TBSYS_LOG(WARN, "build_sql_scan_param: ret[%d]", ret);
      }
      // init sstable operator 
      else if (OB_SUCCESS != (ret = tablet_scan_.set_rpc_proxy(chunkserver.get_rpc_proxy())))
      {
        TBSYS_LOG(WARN, "fail to set rpc proxy:ret[%d]", ret);
      }
      else
      {
        tablet_scan_.set_sql_scan_param( sql_scan_param_ );
        tablet_scan_.set_join_batch_count(chunkserver.get_config().join_batch_count);
        tablet_scan_.set_is_read_consistency(false);

        if (OB_SUCCESS != (ret = tablet_scan_.create_plan(chunk_merge_.current_schema_)))
        {
          TBSYS_LOG(WARN, "fail to create plan:ret[%d]", ret);
        }
        else if(OB_SUCCESS != (ret = tablet_scan_.open()))
        {
          TBSYS_LOG(WARN, "open tablet scan fail:ret[%d]", ret);
        }
      }

      return ret;
    }

    int ObTabletMergerV2::do_merge()
    {
      int ret = OB_SUCCESS;
      bool is_tablet_unchanged = false;
      bool is_sstable_split = false;
      bool need_filter = tablet_merge_filter_.need_filter();
      /**
       * there are 2 cases that we cann't do "unmerge_if_unchanged"
       * optimization
       * 1. the config of chunkserver doesn't enable this function
       * 2. need expire some data in this tablet
       * 3. the table need join another tables (in v2, TabletScan op do this job)
       * 4. the sub range of this tablet is splited(in v2, Writer do this job)
       */
      bool unmerge_if_unchanged =
        (THE_CHUNK_SERVER.get_config().unmerge_if_unchanged && (!need_filter) );

      if (OB_SUCCESS != (ret = wait_aio_buffer()))
      {
        TBSYS_LOG(ERROR, "wait aio buffer error, ret= %d", ret);
      }
      else if (OB_SUCCESS != (ret = open()))
      {
        TBSYS_LOG(ERROR,"set request param for merge_join_agent failed [%d]", ret);
        reset();
      }
      else if (unmerge_if_unchanged && !tablet_scan_.has_incremental_data() 
          && old_tablet_->get_sstable_id_list().count() > 0)
      {
        is_tablet_unchanged = true;
        TBSYS_LOG(INFO, "tablet %s has no incremental data, finish.", to_cstring(old_tablet_->get_range()));
        ret = finish_sstable(false, true);
      }
      else if (OB_SUCCESS != (ret = create_new_sstable()))
      {
        TBSYS_LOG(ERROR,"create sstable failed.");
      }

      const ObRow *cur_row = NULL;
      while (OB_SUCCESS == ret && !is_tablet_unchanged)
      {
        if ( manager_.is_stoped() )
        {
          TBSYS_LOG(WARN, "stop in merging");
          ret = OB_CS_MERGE_CANCELED;
        }
        else
        {
          ret = tablet_scan_.get_next_row(cur_row);
        }

        if (OB_ITER_END == ret)
        {
          // finish the last sstable
          ret = finish_sstable(is_sstable_split, is_tablet_unchanged);
          TBSYS_LOG(INFO, "scan row END, finish current sstable split=%d, unchanged=%d,ret=%d", 
              is_sstable_split, is_tablet_unchanged, ret);
          break;
        }
        else if (OB_SUCCESS == ret && NULL != cur_row)
        {
          if (OB_SUCCESS != (ret = writer_.append_row(*cur_row, is_sstable_split)))
          {
            TBSYS_LOG(WARN, "append row error, ret=%d, row=%s", ret, to_cstring(*cur_row));
          }
          else if (is_sstable_split)
          {
            TBSYS_LOG(INFO, "split tablet, range=%s, cur_row=%s", 
                to_cstring(old_tablet_->get_range()), to_cstring(*cur_row));
            if (OB_SUCCESS != (ret = finish_sstable(is_sstable_split, is_tablet_unchanged)))
            {
              TBSYS_LOG(WARN, "finish_sstable error ret=%d", ret);
            }
            else if (OB_SUCCESS != (ret = create_new_sstable()))
            {
              TBSYS_LOG(WARN, "create_new_sstable error,ret=%d", ret);
            }
            else
            {
              is_sstable_split = false;
            }
          }
        }
        else
        {
          TBSYS_LOG(ERROR, "tablet_scan_ get_next_row error, ret=%d, cur_row=%p", ret, cur_row);
        }
      }

      if (OB_SUCCESS !=  tablet_scan_.close())
      {
        TBSYS_LOG(WARN, "tablet_scan_ close error.");
      }
      CLEAR_TRACE_LOG();

      return ret;
    }

    int ObTabletMergerV2::merge(ObTablet *tablet, int64_t frozen_version)
    {
      int ret = OB_SUCCESS;

      bool sync_meta = THE_CHUNK_SERVER.get_config().each_tablet_sync_meta;
      if (OB_SUCCESS != (ret = prepare_merge(tablet, frozen_version)))
      {
        TBSYS_LOG(WARN, "save merge info failed, ret=%d", ret);
      }
      else if (OB_SUCCESS != (ret = tablet_merge_filter_.init(chunk_merge_.current_schema_,
                0, tablet, frozen_version, chunk_merge_.frozen_timestamp_)))
      {
        TBSYS_LOG(ERROR, "failed to initialize tablet merge filter, table=%ld",
            tablet->get_range().table_id_);
      }
      else if (OB_SUCCESS != (ret = do_merge()))
      {
        TBSYS_LOG(WARN, "do_merge error, ret=%d", ret);

      }
      else if (OB_SUCCESS != (ret = update_meta(old_tablet_, tablet_array_, sync_meta)))
      {
        TBSYS_LOG(WARN, "update_meta error, ret=%d", ret);
      }

      if (OB_SUCCESS != ret)
      {
        // merge failed, cleanup create sstable files;
        cleanup_uncomplete_sstable_files();
      }

      TBSYS_LOG(INFO, "finish merge tablet, ret=%d", ret);

      return ret;
    }

    int ObTabletMergerV2::cleanup_uncomplete_sstable_files()
    {
      int64_t sstable_id = 0;
      char path[OB_MAX_FILE_NAME_LENGTH];

      for(ObVector<ObTablet *>::iterator it = tablet_array_.begin(); it != tablet_array_.end(); ++it)
      {
        if ( ((*it) != NULL) && ((*it)->get_sstable_id_list().count() > 0))
        {
          sstable_id = (*it)->get_sstable_id_list().at(0).sstable_file_id_;
          if (OB_SUCCESS == get_sstable_path(sstable_id,path,sizeof(path)))
          {
            unlink(path);
            TBSYS_LOG(WARN,"cleanup sstable %s",path);
            manager_.get_disk_manager().add_used_space(
                static_cast<int32_t>(get_sstable_disk_no(sstable_id_.sstable_file_id_)) , 0);
          }
        }
      }
      return OB_SUCCESS;
    }

    int ObTabletMergerV2::build_sstable_schema(const uint64_t table_id, compactsstablev2::ObSSTableSchema& sstable_schema)
    {
      int ret = OB_SUCCESS;
      sstable_schema.reset();
      ret = compactsstablev2::build_sstable_schema(table_id, chunk_merge_.current_schema_, sstable_schema);
      return ret;
    }

    int ObTabletMergerV2::create_new_sstable()
    {
      int ret                          = OB_SUCCESS;
      int32_t disk_no                  = manager_.get_disk_manager().get_dest_disk();


      if (disk_no < 0)
      {
        TBSYS_LOG(ERROR,"does't have enough disk space");
        sstable_id_.sstable_file_id_ = 0;
        ret = OB_CS_OUTOF_DISK_SPACE;
      }
      else if (OB_SUCCESS != (ret = gen_sstable_file_location(disk_no, sstable_id_, path_, sizeof(path_))))
      {
        TBSYS_LOG(WARN, "gen_sstable_file_location disk_no=%d failed.", disk_no);
      }
      else 
      {
        ObString path_string;
        path_string.assign_ptr(path_,static_cast<int32_t>(strlen(path_) + 1));
        if (OB_SUCCESS != (ret = writer_.set_sstable_filepath(path_string)) )
        {
          if (OB_IO_ERROR == ret)
            manager_.get_disk_manager().set_disk_status(disk_no,DISK_ERROR);
          TBSYS_LOG(ERROR,"Merge : create sstable failed : [%d]",ret);
        }
        else
        {
          TBSYS_LOG(INFO,"create new sstable, sstable_path:%s ,version=%ld", path_, frozen_version_);
        }
      }


      return ret;
    }

    int ObTabletMergerV2::build_project(const compactsstablev2::ObSSTableSchemaColumnDef* def, 
        const int64_t size, sql::ObProject& project)
    {
      int ret = OB_SUCCESS;
      sql::ObSqlExpression sql_expression;
      sql::ExprItem item;
      for (int64_t i = 0; i < size && OB_SUCCESS == ret; ++i)
      {
        sql_expression.reset();
        item.value_.cell_.tid = def[i].table_id_;
        item.value_.cell_.cid = def[i].column_id_;
        item.type_ = T_REF_COLUMN;
        sql_expression.set_tid_cid(def[i].table_id_, def[i].column_id_);
        if (OB_SUCCESS != (ret = sql_expression.add_expr_item(item)))
        {
          TBSYS_LOG(WARN, "add_expr_item ret=%d, tid=%ld, cid=%ld", ret, def[i].table_id_, def[i].column_id_);
        }
        else if (OB_SUCCESS != (ret = sql_expression.add_expr_item_end()))
        {
          TBSYS_LOG(WARN, "add_expr_item_end ret=%d, tid=%ld, cid=%ld", ret, def[i].table_id_, def[i].column_id_);
        }
        else if (OB_SUCCESS != (ret = project.add_output_column(sql_expression)))
        {
          TBSYS_LOG(WARN, "add_output_column ret=%d, tid=%ld, cid=%ld", ret, def[i].table_id_, def[i].column_id_);
        }
      }
      return ret;
    }

    int ObTabletMergerV2::build_project(const compactsstablev2::ObSSTableSchema& schema, sql::ObProject& project)
    {
      int ret = OB_SUCCESS;
      int64_t rowkey_size = 0;
      int64_t rowvalue_size = 0;
      uint64_t table_id = old_tablet_->get_range().table_id_;
      const compactsstablev2::ObSSTableSchemaColumnDef* def = NULL;

      if (NULL == (def = schema.get_table_schema(table_id, true, rowkey_size)))
      {
        TBSYS_LOG(WARN, "get rowkey schema error, table_id=%ld", table_id);
        ret = OB_INVALID_ARGUMENT;
      }
      else if (OB_SUCCESS != (ret = build_project(def, rowkey_size, project)))
      {
        TBSYS_LOG(WARN, "build_project rowkey error, table_id=%ld", table_id);
      }
      else if (NULL == (def = schema.get_table_schema(table_id, false, rowvalue_size)))
      {
        TBSYS_LOG(WARN, "get rowkey schema error, table_id=%ld", table_id);
        ret = OB_INVALID_ARGUMENT;
      }
      else if (OB_SUCCESS != (ret = build_project(def, rowvalue_size, project)))
      {
        TBSYS_LOG(WARN, "build_project rowvalue error, table_id=%ld", table_id);
      }

      return ret;
    }

    int ObTabletMergerV2::build_sql_scan_param(
        const compactsstablev2::ObSSTableSchema& schema, sql::ObSqlScanParam& scan_param)
    {
      int ret = OB_SUCCESS;
      ObProject project;
      uint64_t table_id = old_tablet_->get_range().table_id_;
      int64_t rowkey_column_count = 0;
      if (OB_SUCCESS != (ret = build_project( schema, project)))
      {
        TBSYS_LOG(ERROR, "prepare scan param failed : [%d]",ret);
      }
      else if (OB_SUCCESS != (ret = schema.get_rowkey_column_count(table_id, rowkey_column_count)))
      {
        TBSYS_LOG(ERROR, "get rowkey_column_count table id[%ld] failed: [%d]", table_id, ret);
      }
      else if (OB_SUCCESS != (ret = scan_param.set_table_id(table_id, table_id)))
      {
        TBSYS_LOG(ERROR, "set table id failed: [%d]",ret);
      }
      else if (OB_SUCCESS != (ret = scan_param.set_range(old_tablet_->get_range())))
      {
        TBSYS_LOG(ERROR, "set range failed:[%d] range:%s", ret, to_cstring(old_tablet_->get_range()));
      }
      else if (OB_SUCCESS != (ret = scan_param.set_project(project)))
      {
        TBSYS_LOG(ERROR, "set project failed:[%d]", ret);
      }
      else if (tablet_merge_filter_.need_filter() 
          && OB_SUCCESS != (ret = tablet_merge_filter_.adjust_scan_param(scan_param)))
      {
        TBSYS_LOG(ERROR, "build filter failed:[%d]", ret);
      }
      else
      {
        scan_param.set_data_version(frozen_version_);
        scan_param.set_is_result_cached(false);
        scan_param.set_daily_merge_scan(true);
        // TODO compare new sstable && old schema;
        scan_param.set_full_row_scan(true);
        scan_param.set_rowkey_column_count(static_cast<int16_t>(rowkey_column_count));
        int64_t async_mode = THE_CHUNK_SERVER.get_config().merge_scan_use_preread;
        if ( 0 == async_mode )
        {
          scan_param.set_read_mode(ScanFlag::SYNCREAD);
        }
        else
        {
          scan_param.set_read_mode(ScanFlag::ASYNCREAD);
        }
      }
      return ret;
    }

    int ObTabletMergerV2::create_hard_link_sstable()
    {
      int ret = OB_SUCCESS;
      ObSSTableId old_sstable_id;
      char old_path[OB_MAX_FILE_NAME_LENGTH];

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
        /**
         * FIXME: current we just support one tablet with only one
         * sstable file
         */
        old_sstable_id = old_tablet_->get_sstable_id_list().at(0);
        if ((ret = get_sstable_path(old_sstable_id, old_path, sizeof(old_path))) != OB_SUCCESS )
        {
          TBSYS_LOG(ERROR, "create_hard_link_sstable: can't get the path of old sstable");
          ret = OB_ERROR;
        }
        else if (OB_SUCCESS != (ret = gen_sstable_file_location(disk_no, sstable_id_, path_, sizeof(path_))))
        {
          TBSYS_LOG(WARN, "gen_sstable_file_location disk_no=%d failed.", disk_no);
        }
        else if (0 != ::link(old_path, path_))
        {
          TBSYS_LOG(ERROR, "failed create hard link for unchanged sstable, disk=%d "
              "old_sstable=%s, new_sstable=%s, error:%s", disk_no, old_path, path_, strerror(errno));
          ret = OB_ERROR;
        }
      }

      return ret;
    }

    int ObTabletMergerV2::build_extend_info(const bool is_tablet_unchanged, ObTabletExtendInfo& extend_info)
    {
      // copy old tablet 's extend info
      extend_info = old_tablet_->get_extend_info();
      if (tablet_merge_filter_.need_filter())
      {
        extend_info.last_do_expire_version_ = frozen_version_;
      }
      else
      {
        extend_info.last_do_expire_version_ = old_tablet_->get_last_do_expire_version();
      }

      if (!is_tablet_unchanged)
      {
        extend_info.check_sum_ = calc_tablet_checksum(writer_.get_sstable_checksum(0));
        extend_info.row_count_ = writer_.get_sstable_row_count(0);
        extend_info.occupy_size_ = writer_.get_sstable_size(0);
        extend_info.sstable_version_ = SSTableReader::COMPACT_SSTABLE_VERSION;
      }
      return OB_SUCCESS;
    }

    int ObTabletMergerV2::build_new_tablet(const bool is_tablet_unchanged, ObTablet* &tablet)
    {
      int ret = OB_SUCCESS;
      ObTabletExtendInfo extend_info ;


      if (is_tablet_unchanged && OB_SUCCESS != (ret = create_hard_link_sstable()))
      {
        TBSYS_LOG(WARN, "cannot create_hard_link_sstable , ret=%d", ret);
      }
      else if (OB_SUCCESS != (ret = build_extend_info(is_tablet_unchanged, extend_info)))
      {
        TBSYS_LOG(WARN, "build_extend_info error. is_tablet_unchanged=%d, ret = %d", 
            is_tablet_unchanged, ret);
      }
      else if (OB_SUCCESS != (ret = tablet->add_sstable_by_id(sstable_id_)) )
      {
        TBSYS_LOG(ERROR,"Merge : add sstable to tablet failed. ret=%d", ret);
      }
      else
      {
        tablet->set_disk_no(static_cast<int32_t>(get_sstable_disk_no(sstable_id_.sstable_file_id_)));
        tablet->set_data_version(frozen_version_);
        tablet->set_extend_info(extend_info);
      }
      return ret;
    }

    int ObTabletMergerV2::finish_sstable(const bool is_sstable_split, const bool is_tablet_unchanged)
    {
      int ret = OB_SUCCESS;
      ObTablet* new_tablet = NULL;
      const ObNewRange *new_range = &old_tablet_->get_range();

      ObMultiVersionTabletImage& tablet_image = manager_.get_serving_tablet_image();
      if (!is_tablet_unchanged)
      {
        // if sstable split, get last split range.
        // else finish the last sstable.
        if (!is_sstable_split && OB_SUCCESS != (ret = writer_.finish()))
        {
          TBSYS_LOG(ERROR,"Merge : finish sstable failed, ret=%d", ret);
        }
        else if (OB_SUCCESS != (ret = writer_.get_table_range(new_range, 0)))
        {
          TBSYS_LOG(ERROR, "Merge : cannot get split range .");
          ret = OB_INVALID_ARGUMENT;
        }
      }

      if (OB_SUCCESS == ret)
      {
        if (OB_SUCCESS != (ret = 
              tablet_image.alloc_tablet_object(*new_range, frozen_version_, new_tablet)))
        {
          TBSYS_LOG(ERROR,"alloc_tablet_object failed, range=%s, version=%ld, ret=%d", 
              to_cstring(*new_range), frozen_version_, ret);
        }
        else if (OB_SUCCESS != (ret = build_new_tablet(is_tablet_unchanged, new_tablet)))
        {
          TBSYS_LOG(ERROR,"build_new_tablet failed, range=%s, is_tablet_unchanged=%d, ret=%d", 
              to_cstring(*new_range), is_tablet_unchanged, ret);
        }
        else if (OB_SUCCESS != (ret = tablet_array_.push_back(new_tablet)))
        {
          TBSYS_LOG(WARN, "cannot push new_tablet=%p", new_tablet);
        }
        else 
        {
          manager_.get_disk_manager().add_used_space(
              static_cast<int32_t>(get_sstable_disk_no(sstable_id_.sstable_file_id_)), 
              new_tablet->get_occupy_size(), !is_tablet_unchanged);
        }
        TBSYS_LOG(INFO, "finish current sstable path=%s, row_count=%ld, "
            "sstable size=%ld, crcsum=%ld, new range:%s", 
            path_, new_tablet->get_row_count(), new_tablet->get_occupy_size(), 
            new_tablet->get_checksum(), to_cstring(*new_range));
      }

      return ret;
    }


  } /* chunkserver */
} /* oceanbase */
