#include "common/ob_schema.h"
#include "sstable/ob_sstable_schema.h"
#include "ob_chunk_server_main.h"
#include "common/file_directory_utils.h"
#include "ob_tablet_manager.h"
#include "ob_index_builder.h"
#include "common/utility.h"
#include "ob_tablet_merger_v1.h"
//add liuxiao [secondary index col checksum]20150401
#include "common/ob_column_checksum.h"
//add e
//add zhuyanchao[secondary index static_data_build.report]20150325
#include "common/ob_tablet_histogram_report_info.h"
#include "common/ob_tablet_histogram.h"
//add e
namespace oceanbase
{
  namespace chunkserver
  {
    using namespace tbutil;
    using namespace common;
    using namespace sstable;
    /*-----------------------------------------------------------------------------
     *  ObIndexBuilder
     *-----------------------------------------------------------------------------*/
    ObIndexBuilder::ObIndexBuilder(ObIndexWorker* worker, ObMergerSchemaManager* manager, ObTabletManager* tablet_manager)
      :worker_(worker),manager_(manager),current_schema_manager_(NULL),tablet_manager_(tablet_manager),current_sstable_size_(0),
       ms_wrapper_(*(ObChunkServerMain::get_instance()->get_chunk_server().get_rpc_proxy()),
                    ObChunkServerMain::get_instance()->get_chunk_server().get_config().merge_timeout)
    {
      sstable_id_.sstable_file_id_ = 0;
      sstable_id_.sstable_file_offset_ = 0;
      path_[0] = 0;
      tablet_arrary_.reset();
      new_table_schema_ = NULL;
      table_id_ = OB_INVALID_ID;
      memset(path_, 0, common::OB_MAX_FILE_NAME_LENGTH);
      disk_no_ = 0;
      static_index_hisgram.set_allocator(&allocator_);
    }

    int ObIndexBuilder::init()
    {
      int ret = OB_SUCCESS;
      if(NULL == worker_)
      {
        TBSYS_LOG(ERROR,"null pointer of worker!");
        ret = OB_ERROR;
      }
      else if(NULL == (r_s_hash_ = worker_->get_range_info()))
      {
        TBSYS_LOG(ERROR, "null pointer of range_hash_pointer");
        ret = OB_ERROR;
      }
#if 0
      else if(OB_SUCCESS != (ret = sort_.set_run_filename(DEFAULT_SORT_DUMP_FILE)))
      {
        TBSYS_LOG(WARN, "set run file name failed,ret[%d]", ret);
      }
      else
      {
        sort_.set_mem_size_limit(DEFAULT_MEMORY_LIMIT);
      }
#endif

      return ret;
    }

    void ObIndexBuilder::get_sstable_id(ObSSTableId &sstable_id)
    {
      sstable_id = sstable_id_;
    }

    int ObIndexBuilder::start(ObTablet *tablet,int64_t sample_rate)
    {      
      //add zhuyanchao[secondary index static_index_build.report]20150320
      uint64_t tablet_row_count = 0;
      //add:e
      int ret = OB_SUCCESS;
      int32_t disk_no = -1;
      if(NULL == tablet)
      {
        TBSYS_LOG(ERROR, "tablet pointer is null");
        ret = OB_INVALID_ARGUMENT;
      }
      else if(NULL == manager_)
      {
        TBSYS_LOG(ERROR, "MergeSchemaManager pointer is NULL");
        ret = OB_INVALID_ARGUMENT;
      }
      else if(NULL == (current_schema_manager_ = manager_->get_schema(0)))
      {
        TBSYS_LOG(ERROR, "get null schema manager pointer!");
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        //add liumz, [debug for checksum]20160615:b
        ObTabletReportInfo tablet_info_before, tablet_info_after;
        uint64_t crc_before = 0, crc_after = 0, row_before = 0, row_after = 0;
        tablet_manager_->fill_tablet_info(*tablet, tablet_info_before);
        crc_before = tablet_info_before.tablet_info_.crc_sum_;
        row_before = tablet_info_before.tablet_info_.row_checksum_;
        //add:e

        //add zhuyanchao[secondary index static_index_build.report]20150320
        tablet_manager_->fill_tablet_histogram_info(*tablet, *(get_tablet_histogram_report_info()));
        tablet_row_count=tablet->get_row_count();
        //add e
        new_range_ = tablet->get_range();
        disk_no = tablet->get_disk_no();
        TBSYS_LOG(INFO,"get tablet range[%s] to build index", to_cstring(new_range_));
        uint64_t table_id = worker_->get_process_tid();
        //IndexList il;
        /*if(OB_SUCCESS != (ret = gen_index_table_list(table_id, il)))
        {
          TBSYS_LOG(ERROR, "gen index table list error,data_table_id[%ld],ret[%d]",table_id,ret);
        }
        else */
        if(OB_SUCCESS != (ret = write_partitional_index_v1(new_range_, table_id, disk_no, tablet_row_count, sample_rate)))
        {
          TBSYS_LOG(ERROR, "gen static index test error,ret[%d]",ret);
        }
        else if(OB_SUCCESS != (ret = tablet->add_local_index_sstable_by_id(sstable_id_)))
        {
          TBSYS_LOG(ERROR, "add sstable_id[%ld] in tablet error",sstable_id_.sstable_file_id_);
        }
        //add liumz[sync local index meta]20161028
        else if(OB_SUCCESS != (ret = update_local_index_meta(tablet, true)))
        {
          TBSYS_LOG(ERROR, "update local index  error, sstable_file_id[%ld]", sstable_id_.sstable_file_id_);
        }
        //add:e
        if (OB_LIKELY(OB_SUCCESS == ret))
        {
          TBSYS_LOG(INFO, "build local index sstable[%ld] successed ! Add in tablet;",sstable_id_.sstable_file_id_);
        }
        //add liumz, [debug for checksum]20160615:b
        tablet_manager_->fill_tablet_info(*tablet, tablet_info_after);
        crc_after = tablet_info_after.tablet_info_.crc_sum_;
        row_after = tablet_info_after.tablet_info_.row_checksum_;
        if (crc_before != crc_after || row_before != row_after)
        {
          TBSYS_LOG(ERROR,"test::liumz, checksum changed!");
          TBSYS_LOG(WARN, "test::liumz, before tablet <%s>, row count:[%ld], size:[%ld], "
              "crc:[%ld] row_checksum:[%lu] version:[%ld] sequence_num:[%ld] to report list",
              to_cstring(tablet_info_before.tablet_info_.range_),
              tablet_info_before.tablet_info_.row_count_,
              tablet_info_before.tablet_info_.occupy_size_,
              tablet_info_before.tablet_info_.crc_sum_,
              tablet_info_before.tablet_info_.row_checksum_,
              tablet_info_before.tablet_location_.tablet_version_,
              tablet_info_before.tablet_location_.tablet_seq_);
          TBSYS_LOG(WARN, "test::liumz, after tablet <%s>, row count:[%ld], size:[%ld], "
              "crc:[%ld] row_checksum:[%lu] version:[%ld] sequence_num:[%ld] to report list",
              to_cstring(tablet_info_after.tablet_info_.range_),
              tablet_info_after.tablet_info_.row_count_,
              tablet_info_after.tablet_info_.occupy_size_,
              tablet_info_after.tablet_info_.crc_sum_,
              tablet_info_after.tablet_info_.row_checksum_,
              tablet_info_after.tablet_location_.tablet_version_,
              tablet_info_after.tablet_location_.tablet_seq_);
        }
        //add:e
      }
      if(NULL != current_schema_manager_)
      {
        if(OB_LIKELY(OB_SUCCESS == manager_->release_schema(current_schema_manager_)))
        {
          current_schema_manager_ = NULL;
          //TBSYS_LOG(ERROR,"test whx:: release local index schema success!");
        }
      }
      return ret;
    }

    int ObIndexBuilder::start_total_index(ObNewRange *range)
    {
      int ret = OB_SUCCESS;
      const RangeServerHash* range_server = NULL;
      ObScanParam param;
      //int64_t trailer_offset = 0;
      ///判断�?些参数是否正�?
      if(NULL == worker_ || NULL == range || NULL == manager_)
      {
        TBSYS_LOG(ERROR, "some pointer is null,invalid argument");
        ret = OB_ERROR;
      }
      else if(NULL == (current_schema_manager_ = manager_->get_schema(0)))
      {
        TBSYS_LOG(ERROR, "SchemaManagerV2 is NULL pointer!");
        ret = OB_ERROR;
      }
      else
      {
        new_range_ = *range;
        table_id_ = range->table_id_;
      }

      if(OB_SUCCESS == ret)
      {
        //首先创建�?个空的sstable
        int32_t disk_no = tablet_manager_->get_disk_manager().get_dest_disk();
        if(OB_SUCCESS != (ret = create_new_sstable(new_range_.table_id_, disk_no)))
        {
          TBSYS_LOG(WARN, "create new sstable for table[%ld] failed, disk_no[%d]", new_range_.table_id_, disk_no);
        }
        else if(NULL == (range_server = worker_->get_range_info()))
        {
          TBSYS_LOG(WARN, "failed to get range server info");
          ret = OB_ERROR;
        }
        else
        {
          TBSYS_LOG(INFO, "create total index sstable success,table[%ld]", new_range_.table_id_);
          disk_no_ = disk_no;
        }
      }

      if(OB_SUCCESS == ret)
      {
        (*ms_wrapper_.get_cs_scan_cell_stream()).set_self(THE_CHUNK_SERVER.get_self());
        if(OB_SUCCESS != (ret = fill_scan_param(param)))
        {
          TBSYS_LOG(WARN, "fill scan param for index failed,ret[%d]", ret);
        }
        else if(OB_SUCCESS != (ret = query_agent_.start_agent(param, *ms_wrapper_.get_cs_scan_cell_stream(), range_server)))
        {
          /*if(OB_ITER_END == ret)
          {
            query_agent_.set_not_used();
          }*/
          TBSYS_LOG(WARN, "start query agent failed,ret[%d]", ret);
        }
        else
        {
          //TBSYS_LOG(WARN, "test::whx scan_param = %s", to_cstring(param));
        }
        //TBSYS_LOG(ERROR,"test::whx param = [%s] new_range_[%s],fake_range[%s]", to_cstring(*(param.get_range())),to_cstring(new_range_),to_cstring(*(param.get_fake_range())));
      }

      if(OB_SUCCESS == ret || OB_ITER_END == ret)
      {
        if(OB_SUCCESS != (ret = write_total_index_v1()))
        {
          TBSYS_LOG(WARN, "write total index failed,ret[%d]", ret);
        }
        else
        {
          query_agent_.stop_agent();
        }
      }

      if(NULL != current_schema_manager_)
      {
        if(OB_SUCCESS != (manager_->release_schema(current_schema_manager_)))
        {
          TBSYS_LOG(WARN, "release schema failed,ret = [%d]",ret);

        }
        else
        {
          current_schema_manager_ = NULL;
          //TBSYS_LOG(ERROR,"test whx:: release global index schema success!");
        }
      }
      return ret;
    }

    int ObIndexBuilder::get_failed_fake_range(ObNewRange &range)
    {
      return query_agent_.get_failed_fake_range(range);
    }

    int ObIndexBuilder::trans_row_to_sstrow(ObRowDesc &desc, const ObRow &row, ObSSTableRow &sst_row)
    {
      int ret = OB_SUCCESS;
      uint64_t tid = OB_INVALID_ID;
      uint64_t cid = OB_INVALID_ID;
      const ObObj *obj = NULL;
      //TBSYS_LOG(INFO, "test::whx row = %s",to_cstring(row));
      if(NULL == new_table_schema_)
      {
        TBSYS_LOG(WARN, "null pointer of table schema");
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        row_.clear();
        row_.set_rowkey_obj_count(desc.get_rowkey_cell_count());
      }
      for(int64_t i = 0; OB_SUCCESS == ret && i < desc.get_column_num();i++)
      {
        if(OB_SUCCESS != (ret = desc.get_tid_cid(i,tid,cid)))
        {
          TBSYS_LOG(WARN,"failed in get tid_cid from row desc,ret[%d]",ret);
          break;
        }
        else if(OB_INDEX_VIRTUAL_COLUMN_ID == cid)
        {
          ObObj vi_obj;
          vi_obj.set_null();
          ret = row_.add_obj(vi_obj);
        }
        else if(OB_SUCCESS != (ret = row.get_cell(tid,cid,obj)))
        {
          TBSYS_LOG(WARN, "get cell from obrow failed!tid[%ld],cid[%ld],ret[%d]",tid,cid,ret);
          break;
        }
        if(OB_SUCCESS == ret && OB_INDEX_VIRTUAL_COLUMN_ID != cid)
        {
          if(NULL == obj)
          {
            TBSYS_LOG(WARN, "obj pointer cannot be NULL!");
            ret = OB_INVALID_ARGUMENT;
            break;
          }
          else if(OB_SUCCESS != (ret = sst_row.add_obj(*obj)))
          {
            TBSYS_LOG(WARN, "set cell for index row failed,ret = %d",ret);
            break;
          }
          else
          {
            //TBSYS_LOG(INFO,"test::whx add obj[%s] success",to_cstring(*obj));
          }
        }
      }
      return ret;
    }

    int ObIndexBuilder::write_total_index_v1()
    {
      int ret = OB_SUCCESS;
      const ObRow *row = NULL;
     // ObRow* row_ptr = &row;
      ObRowDesc desc;
      ObAgentScan ascan;
      //ObSort sort;
      ret = sort_.set_child(0,ascan);
      int64_t trailer_offset;
      //add zhuyanchao[secondary index static_index_build.columnchecksum]20150407
      ObRowDesc index_desc;
      cc.reset();
      //add e
      if(OB_SUCCESS != ret|| NULL == worker_ || NULL == worker_->get_tablet_manager())
      {
        ret = OB_ERR_UNEXPECTED;
        TBSYS_LOG(WARN, "worker_ should not be null!");
      }
      else if(OB_SUCCESS != (ret = row_.set_table_id(table_id_)))
      {
        TBSYS_LOG(WARN,"failed to set sstable row tid[%d]", ret);
      }
      else if(OB_SUCCESS != (ret = row_.set_column_group_id(0)))
      {
        TBSYS_LOG(WARN,"failed to set sstable row column_group_id[%d]", ret);
      }
      else if(OB_SUCCESS != (ret = cons_row_desc(desc)))
      {
        TBSYS_LOG(WARN,"failed to construct row desc[%d]", ret);
      }
      else if(OB_SUCCESS != (ret = cons_row_desc_without_virtual(index_desc)))
      {
        TBSYS_LOG(WARN,"failed to construct index row desc[%d]", ret);
      }
      else
      {
        //row.set_row_desc(desc);
      }

      //TBSYS_LOG(ERROR,"test::liuxiao total cons_row_desc desc:%s",to_cstring(desc));

      if(OB_SUCCESS == ret)
      {
        ScanContext sc;
        ascan.set_row_desc(desc);
        ascan.set_query_agent(&query_agent_);
        ascan.set_server(THE_CHUNK_SERVER.get_self());
        worker_->get_tablet_manager()->build_scan_context(sc);
        ascan.set_scan_context(sc);
      }

      if(OB_SUCCESS == ret)
      {
        for(int64_t i = 0; i < desc.get_rowkey_cell_count();i++)
        {
          uint64_t tid = OB_INVALID_ID;
          uint64_t cid = OB_INVALID_ID;
          if(OB_SUCCESS != (ret = desc.get_tid_cid(i, tid, cid)))
          {
            TBSYS_LOG(WARN, "get tid cid from row desc failed,i[%ld],ret[%d]",i, ret);
            break;
          }
          else if(OB_SUCCESS != (ret = sort_.add_sort_column(tid, cid, true)))
          {
            TBSYS_LOG(WARN, "set sort column failed,tid[%ld], cid[%ld],ret[%d]",tid, cid, ret);
            break;
          }
        }
      }

      if(OB_SUCCESS == ret)
      {
        ret = sort_.open();
      }

      if(OB_SUCCESS == ret)
      {
        while(OB_SUCCESS == (ret = sort_.get_next_row(row)))
        {
          //add zhuyanchao[secondary index static_index_build.columnchecksum]20150407
          if(OB_SUCCESS != (ret = calc_tablet_col_checksum_index(*row, index_desc, cc.get_str(), table_id_)))
          {
            TBSYS_LOG(ERROR,"fail to calculate tablet column checksum index =%d",ret);
            break;
          }
          //add liuxiao [secondary index col checksum] 20150525
          else if(OB_SUCCESS != (ret = column_checksum_.sum(cc)))
          {
            TBSYS_LOG(ERROR,"checksum sum error =%d",ret);
            break;
          }
          cc.reset();
          //add e
          //add e
          if(OB_SUCCESS != (ret = trans_row_to_sstrow(desc, *row, row_)))
          {
            TBSYS_LOG(WARN, "failed to trans row to sstable_row,ret = %d", ret);
            break;
          }
          else if(OB_SUCCESS != (ret = save_current_row()))
          {
            TBSYS_LOG(WARN, "failed to save current row,ret = %d", ret);
            break;
          }
        }
        if(OB_ITER_END == ret)
        {
          {
            ObMultiVersionTabletImage& tablet_image = tablet_manager_->get_serving_tablet_image();//todo 和下面重复了
            frozen_version_ = tablet_image.get_serving_version();
            //delete by liuxiao [secondary index bug.fix]20150812
            //ObServer master_master_ups;
            //if(OB_SUCCESS != (ret = ObChunkServerMain::get_instance()->get_chunk_server().get_rpc_proxy()->get_master_master_update_server(true,master_master_ups)))
            //{
            //  TBSYS_LOG(ERROR,"failed to get ups for report index cchecksum");
            //}
            //delete e
            //add liumz, [secondary index col checksum]20160405:b
            if (new_range_.end_key_.is_max_row())
            {
              new_range_.end_key_.set_max_row();
              new_range_.border_flag_.unset_inclusive_end();
            }
            //add:e
            if(OB_SUCCESS != (ret = tablet_manager_->send_tablet_column_checksum(column_checksum_, new_range_, frozen_version_)))
            {
              TBSYS_LOG(ERROR,"send tablet column checksum failed =%d",ret);
            }
            else
            {
              //ret = OB_SUCCESS;
            }
            //add liuxiao [secondary index col checksum] 20150525
            column_checksum_.reset();
            //add e
            index_extend_info_.row_checksum_=sstable_writer_.get_row_checksum();
          }
        }
      }
      //add zhuyanchao[secondary index static_data_build.report]20150401
      if(OB_SUCCESS == ret)
      {
          ObTablet* index_image_tablet;
          ObMultiVersionTabletImage& tablet_image = tablet_manager_->get_serving_tablet_image();
          bool  sync_meta = THE_CHUNK_SERVER.get_config().each_tablet_sync_meta;
          if ((ret = tablet_image.alloc_tablet_object(new_range_,frozen_version_,index_image_tablet)) != OB_SUCCESS)
          {
            TBSYS_LOG(ERROR,"alloc_index_image_tablet_object failed.");
          }
          else
          {
            if((ret = construct_index_tablet_info(index_image_tablet)) == OB_SUCCESS)
            {
              if(OB_SUCCESS != (ret = update_meta_index(index_image_tablet,sync_meta)))
              {
                TBSYS_LOG(WARN,"upgrade new index tablets error [%d]",ret);
              }
            }
          }
      }
      //add e

      //if(OB_SUCCESS == ret)
      TBSYS_LOG(INFO,"write total index go to end!");
      sort_.reset();
      current_sstable_size_ = 0;
      //add wenghaixing [secondary index static_index_build]20150804
      /*when we stop to read sstable,we should reset tmi*/
      ObThreadAIOBufferMgrArray* aio_buf_mgr_array = GET_TSI_MULT(ObThreadAIOBufferMgrArray, TSI_SSTABLE_THREAD_AIO_BUFFER_MGR_ARRAY_1);
      if(NULL != aio_buf_mgr_array)
      {
        aio_buf_mgr_array->reset();
      }
      //add e
      sstable_writer_.close_sstable(trailer_offset,current_sstable_size_);
      return ret;
    }

    int ObIndexBuilder::cons_row_desc(ObRowDesc &desc)
    {
      int ret = OB_SUCCESS;
      int64_t key_count = 0;
      int64_t column_count = 0;
      const ObSSTableSchemaColumnDef* col_def = NULL;
      if(OB_SUCCESS != (ret = sstable_schema_.get_rowkey_column_count(table_id_, key_count)))
      {
        TBSYS_LOG(WARN, "get rowkey count failed,tid[%ld],ret[%d]", table_id_, ret);
      }
      else
      {
        column_count = sstable_schema_.get_column_count();
      }
      desc.set_rowkey_cell_count(key_count);
      for(int64_t i = 0;i < column_count; i++)
      {
        if(NULL == (col_def = sstable_schema_.get_column_def((int)i)))
        {
          TBSYS_LOG(WARN, "failed to get column def of table[%ld], i[%ld],ret[%d]", table_id_, i, ret);
          break;
        }
        else if(OB_SUCCESS != (desc.add_column_desc(table_id_, col_def->column_name_id_)))
        {
          TBSYS_LOG(WARN, "failed to set column desc table_id[%ld], ret[%d]", table_id_, ret);
          break;
        }
      }

      return ret;
    }
    //add zhuyanchao[secondary index static_index_build.columnchecksum]20150407
    int ObIndexBuilder::cons_row_desc_local_tablet(ObRowDesc &index_data_desc,ObRowDesc &data_rowkey_desc,ObRowDesc &index_column_desc,
                                                   uint64_t tid ,uint64_t index_tid)
    {
      int ret = OB_SUCCESS;
      int64_t count;
      //int64_t index_count;
      int64_t size;
      int64_t data_table_rowkey_count;
      //int64_t frozen_version;
      int64_t rowkey_count;
      ObRowkeyInfo index_rowkey;
      ObRowkeyInfo data_rowkey;
      uint64_t tmp_coulumn_id = OB_INVALID_ID;
      //ObRowkeyColumn rc;
      bool hint_index;
      if(NULL == current_schema_manager_)
      {
        TBSYS_LOG(ERROR,"get schema manager v2 error,ret=%d",ret);
        ret=OB_SCHEMA_ERROR;
      }
      const ObTableSchema *index_schema = current_schema_manager_->get_table_schema(index_tid);
      const ObTableSchema *schema = current_schema_manager_->get_table_schema(tid);
      if(NULL == schema)
      {
        TBSYS_LOG(ERROR,"get data table schema error,ret=%d",ret);
        ret=OB_SCHEMA_ERROR;
      }
      else if(NULL == index_schema)
      {
        TBSYS_LOG(ERROR,"get index table schema error,ret=%d",ret);
      }
      else
      {
        count = schema->get_max_column_id();
        //index_count = index_schema->get_max_column_id();
        index_rowkey = index_schema->get_rowkey_info();
        size = index_rowkey.get_size();
        data_rowkey = schema->get_rowkey_info();
        rowkey_count = index_schema->get_rowkey_info().get_size();
        data_table_rowkey_count = schema->get_rowkey_info().get_size();
        index_data_desc.set_rowkey_cell_count(rowkey_count);
        //construct index data desc
        for(int64_t i = 0;i < size;i++)
        {
          if(OB_SUCCESS != (ret = index_rowkey.get_column_id(i,tmp_coulumn_id)))
          {
            TBSYS_LOG(ERROR,"failed to get rowkey column id from index info,ret=%d",ret);
            break;
          }
          else if(OB_SUCCESS != (ret = index_data_desc.add_column_desc(index_tid,tmp_coulumn_id)))
          {
            break;
          }
        }
        if(OB_SUCCESS ==  ret)
        {
          for(int i = 0;i < data_table_rowkey_count;i++)
          {
            if(OB_SUCCESS != (ret = data_rowkey.get_column_id(i,tmp_coulumn_id)))
            {
              TBSYS_LOG(ERROR,"failed to get rowkey column id from index info,ret=%d",ret);
              break;
            }
            else if(!index_rowkey.is_rowkey_column(tmp_coulumn_id))
            {
              ret = index_data_desc.add_column_desc(index_tid,tmp_coulumn_id);
            }
            if(OB_SUCCESS != ret)
            {
              break;
            }
          }
        }
        if(OB_SUCCESS ==  ret)
        {
          for(int i = 0;i <= count;i++)
          {
            if(!index_rowkey.is_rowkey_column(i) && !data_rowkey.is_rowkey_column(i))
            {
                if(OB_SUCCESS!=(ret= current_schema_manager_->column_hint_index(tid,i,hint_index)))
                {
                  TBSYS_LOG(ERROR,"failed to check if hint index,ret=%d",ret);
                  break;
                }
                else if(hint_index)
                {
                  ret = index_data_desc.add_column_desc(index_tid,i);
                }
            }
            if(OB_SUCCESS != ret)
            {
              break;
            }
          }
          //add wenghaixing [secondary index alter_table_debug]20150611
          if(OB_SUCCESS == ret &&current_schema_manager_->is_index_has_storing(index_tid))
          {
            ret = index_data_desc.add_column_desc(index_tid, OB_INDEX_VIRTUAL_COLUMN_ID);
          }
          //add e
        }


        /*
        for(int64_t i = 0;i < size;i++)
        {
          if(OB_SUCCESS != (ret = index_rowkey.get_column_id(i,tmp_coulumn_id)))
          {
            TBSYS_LOG(ERROR,"failed to get rowkey column id from index info,ret=%d",ret);
            break;
          }
          else if(!data_rowkey.is_rowkey_column(tmp_coulumn_id))
          {
            ret = index_data_desc.add_column_desc(index_tid,tmp_coulumn_id);
          }
          if(OB_SUCCESS != ret)
          {
            break;
          }
        }

        if(OB_SUCCESS ==  ret)
        {
          for(int i = 0;i < data_table_rowkey_count;i++)
          {
            if(OB_SUCCESS != (ret = data_rowkey.get_column_id(i,tmp_coulumn_id)))
            {
              TBSYS_LOG(ERROR,"failed to get rowkey column id from index info,ret=%d",ret);
              break;
            }
            else
            {
              ret = index_data_desc.add_column_desc(index_tid,tmp_coulumn_id);
            }
            if(OB_SUCCESS != ret)
            {
              break;
            }
          }
        }
        if(OB_SUCCESS ==  ret)
        {
          for(int i = OB_APP_MIN_COLUMN_ID;i <= (count+1);i++)
          {
            if(!index_rowkey.is_rowkey_column(i) && !data_rowkey.is_rowkey_column(i))
            {
                if(OB_SUCCESS!=(ret= current_schema_manager_->column_hint_index(tid,i,hint_index)))
                {
                  TBSYS_LOG(ERROR,"failed to check if hint index,ret=%d",ret);
                  break;
                }
                else if(hint_index)
                {
                  ret = index_data_desc.add_column_desc(index_tid,i);
                }
            }
            if(OB_SUCCESS != ret)
            {
              break;
            }
          }
        }
        */
        //TBSYS_LOG(ERROR,"test::liuxiao index desc %s",to_cstring(index_data_desc));
        //construct e
        if(OB_SUCCESS == ret)
        {
          for(int i = 0; i<=count;i++)
          {
            if(data_rowkey.is_rowkey_column(i))
            {
              data_rowkey_desc.add_column_desc(index_tid,i);
            }
          }
        }
        if(OB_SUCCESS == ret)
        {
          for(int i = 0;i <= count;i++)
          {
            if(OB_SUCCESS!=(ret= current_schema_manager_->column_hint_index(tid,i,hint_index)))
            {
              TBSYS_LOG(ERROR,"failed to check if hint index,ret=%d",ret);
              break;
            }
            else if(!data_rowkey.is_rowkey_column(i) && hint_index)
            {
              index_column_desc.add_column_desc(tid,i);
            }
          }
        }
      }
      return ret;
    }
    int ObIndexBuilder::cons_row_desc_without_virtual(ObRowDesc &desc)
    {
      int ret = OB_SUCCESS;
      int64_t key_count = 0;
      int64_t column_count = 0;
      const ObTableSchema* index_schema;
      const ObTableSchema* schema;
      const ObSSTableSchemaColumnDef* col_def = NULL;
      //const ObSchemaManagerV2 *schemav2 = manager_->get_user_schema(frozen_version_);
      uint64_t  max_data_table_cid = OB_INVALID_ID;
      index_schema = current_schema_manager_->get_table_schema(table_id_);
      uint64_t data_tid = index_schema->get_index_helper().tbl_tid;
      schema = current_schema_manager_->get_table_schema(data_tid);
      max_data_table_cid=current_schema_manager_->get_table_schema(data_tid)->get_max_column_id();
      if(OB_SUCCESS != (ret = sstable_schema_.get_rowkey_column_count(table_id_, key_count)))
      {
        TBSYS_LOG(WARN, "get rowkey count failed,tid[%ld],ret[%d]", table_id_, ret);
      }
      else
      {
        column_count = sstable_schema_.get_column_count();
      }
      desc.set_rowkey_cell_count(schema->get_rowkey_info().get_size());
      for(int64_t i = 0; i< column_count; i++)
      {
        if(NULL == (col_def = sstable_schema_.get_column_def((int)i)))
        {
          TBSYS_LOG(WARN, "failed to get column def of table[%ld], i[%ld],ret[%d]", table_id_, i, ret);
          break;
        }
        else if(schema->get_rowkey_info().is_rowkey_column(col_def->column_name_id_))
        {
          if(OB_SUCCESS != (desc.add_column_desc(table_id_, col_def->column_name_id_)))
          {
            TBSYS_LOG(WARN, "failed to set column desc table_id[%ld], ret[%d]", table_id_, ret);
            break;
          }
        }
      }
      for(int64_t i = 0; i< column_count; i++)
      {
        if(NULL == (col_def = sstable_schema_.get_column_def((int)i)))
        {
          TBSYS_LOG(WARN, "failed to get column def of table[%ld], i[%ld],ret[%d]", table_id_, i, ret);
          break;
        }
        else if(!schema->get_rowkey_info().is_rowkey_column(col_def->column_name_id_) && col_def->column_name_id_<=max_data_table_cid)
        {
          if(OB_SUCCESS != (desc.add_column_desc(table_id_, col_def->column_name_id_)))
          {
            TBSYS_LOG(WARN, "failed to set column desc table_id[%ld], ret[%d]", table_id_, ret);
            break;
          }
        }
      }
      return ret;
    }
    //add e

    int ObIndexBuilder::gen_index_table_list(uint64_t data_table_id, IndexList &list)
    {
      int ret = OB_SUCCESS;

     // int64_t version = manager_->get_latest_version();
      //const ObSchemaManagerV2 * schema = manager_->get_user_schema(version);
      const ObTableSchema* table_schema = NULL;
      uint64_t idx_tid = OB_INVALID_ID;
      IndexList il;
      if(NULL == current_schema_manager_)
      {
        ret = OB_SCHEMA_ERROR;
        TBSYS_LOG(ERROR,"get user schema error!");
      }
      else
      {
        const hash::ObHashMap<uint64_t,IndexList,hash::NoPthreadDefendMode>* hash = current_schema_manager_->get_index_hash();
        //hash::ObHashMap<uint64_t,index_list,hash::NoPthreadDefendMode>::const_iterator iter=hash->begin();
        if(NULL == hash)
        {
          ret = OB_ERROR;
          TBSYS_LOG(ERROR,"hash pointer is null");
        }
        else if(hash::HASH_EXIST == hash->get(data_table_id,il))
        {
          //index_list il=iter->second;
          for(int64_t i = 0;OB_SUCCESS == ret && i<il.get_count(); i++)
          {
            il.get_idx_id(i,idx_tid);
            table_schema = current_schema_manager_->get_table_schema(idx_tid);
            if(NULL == table_schema)
            {
              ret = OB_SCHEMA_ERROR;
              TBSYS_LOG(ERROR,"failed to get table schema");
            }
            else if(table_schema->get_index_helper().status == NOT_AVALIBALE)
            {
              ret = list.add_index(table_schema->get_table_id());
            }
           }
         }
       }
       return ret;
    }

    int ObIndexBuilder::write_partitional_index_v1(ObNewRange range, uint64_t index_tid, int32_t disk, int64_t tablet_row_count, int64_t sample_rate)
    {
      int ret = OB_SUCCESS;
      ObRowDesc index_data_desc;
      ObRowDesc data_rowkey_desc;
      ObRowDesc cc_index_column_desc;
      ObNewRange construct_sample_range;//构�? range
      ObTabletSample index_sample;
      common::ObObj objs[OB_MAX_ROWKEY_COLUMN_NUMBER];
      int64_t row_count = 1;//当row_count等于sample_count的时候将此时的obindexsample� 入onindexhistogram中，并且将count和sample初始�?
      int64_t temp_tablet_row_count =1;//记录当前的�?�的rowcount当与tablet的rowcount相等的时候则是最后一行这个时候要将range� 入到sample
      uint64_t data_tid = range.table_id_;
      ObTabletScan tmp_table_scan;
      ObSSTableScan ob_sstable_scan;
      ObSqlScanParam ob_sql_scan_param;
      sstable::ObSSTableScanParam sstable_scan_param;
      ObArray<uint64_t> basic_columns;
      ObTabletManager *tablet_mgr = worker_->get_tablet_manager();
      ScanContext sc;
      tablet_mgr->build_scan_context(sc);
      const ObRow *cur_row = NULL;
      sort_.set_child(0,ob_sstable_scan);
      ObRowDesc desc;
      int64_t trailer_offset;
      cal_sample_rate(tablet_row_count, sample_rate);
	  //add weixing [improve sec_index]20161104
      bool store = true;
      //add e
      if(OB_SUCCESS != (ret = cons_row_desc_local_tablet(index_data_desc, data_rowkey_desc, cc_index_column_desc, data_tid, index_tid)))
      {
        TBSYS_LOG(WARN, "construct row desc failed,ret = [%d]", ret);
      }
      if(OB_SUCCESS == ret)
      {
        if(OB_SUCCESS != (ret = create_new_sstable(index_tid, disk)))
        {
          TBSYS_LOG(WARN, "failed to create new sstable for index[%ld], disk[%d], ret = %d", index_tid, disk, ret);
        }
        else
        {
          basic_columns.clear();
          row_.set_column_group_id(0);
          row_.set_table_id(index_tid);
        }
      }
      if(OB_SUCCESS == ret)
      {
        const ObTableSchema* table_schema = current_schema_manager_->get_table_schema(index_tid);
        const ObRowkeyInfo* rowkey_info = &(table_schema->get_rowkey_info());
        if(NULL == table_schema || NULL == new_table_schema_)
        {
          TBSYS_LOG(WARN, "table schema or new_table_schema_ error, null pointer!");
          ret = OB_SCHEMA_ERROR;
        }
        else if(OB_SUCCESS != (ret = ob_sql_scan_param.set_range(range)))
        {
          TBSYS_LOG(WARN, "set ob_sql_param_range failed,ret= %d,range = %s", ret, to_cstring(range));
        }
        else
        {
          ob_sql_scan_param.set_is_result_cached(false);
          construct_sample_range.table_id_=index_tid;
          construct_sample_range.border_flag_.set_inclusive_start();
          construct_sample_range.border_flag_.set_inclusive_end();
        }
        if(OB_SUCCESS == ret)
        {
          if(OB_SUCCESS != (ret = push_cid_in_desc_and_ophy(index_tid, data_tid, index_data_desc, basic_columns, desc)))
          {
            TBSYS_LOG(WARN, "push cid indesc failed,data_tid[%ld], index_data_desc = %s, desc = %s, ret = %d", data_tid, to_cstring(index_data_desc), to_cstring(desc), ret );
          }
          else if(OB_SUCCESS != (ret = tmp_table_scan.build_sstable_scan_param_pub(basic_columns,ob_sql_scan_param,sstable_scan_param)))
          {
            TBSYS_LOG(WARN,"failed to build sstable scan param");
          }
          else if(OB_SUCCESS != (ret = ob_sstable_scan.open_scan_context(sstable_scan_param,sc)))
          {
            TBSYS_LOG(WARN,"failed to open scan context");
          }
          else if(OB_SUCCESS != (ret = sort_.open()))
          {
            TBSYS_LOG(WARN,"failed to open ObSsTableScan!");
          }
          else
          {
            while(OB_SUCCESS == ret && OB_SUCCESS == (ret = sort_.get_next_row(cur_row)))
            {
              if(NULL != cur_row)
              {
                //TBSYS_LOG(ERROR, "test::whx desc = [%s]",to_cstring(desc));
                if(OB_SUCCESS != (ret = trans_row_to_sstrow(desc, *cur_row, row_)))
                {
                  TBSYS_LOG(WARN,"failed to trans row to sstable row, ret = %d", ret);
                  break;
                }
                else
                {
                  if(1 == row_count || temp_tablet_row_count == tablet_row_count)//construct start key
                  {
                    if(1 == row_count)
                    {
                      for(int i = 0; i < rowkey_info->get_size(); i++)
                      {
                        if(OB_SUCCESS != (ret = ob_write_obj_v2(allocator_, row_.get_row_obj(i),objs[i] )))
                        {
                          TBSYS_LOG(ERROR,"write obj faield,ret [%d]",ret);
                          break;
                        }
                      }
                      construct_sample_range.start_key_.assign(objs,rowkey_info->get_size());
                    }
                    if(temp_tablet_row_count == tablet_row_count)
                    {
                      construct_sample_range.end_key_.assign(row_.get_obj_v1(0),rowkey_info->get_size());
                      index_sample.range_=construct_sample_range;
                      index_sample.row_count_=row_count;
                      if(OB_SUCCESS !=(ret = get_tablet_histogram()->add_sample_with_deep_copy(index_sample)))
                      {
                        TBSYS_LOG(WARN,"fail to add sample to histogram =%d",ret);
                        break;
                      }
                      //TBSYS_LOG(ERROR,"test:whx output2 tablet_row_count[%ld],temp_tablet_row_count = %ld", tablet_row_count, temp_tablet_row_count);
                    }
                    temp_tablet_row_count ++;
                    row_count ++;

                  }
                  else if(row_count == sample_rate && temp_tablet_row_count != tablet_row_count)
                  {
                    temp_tablet_row_count++;
                    construct_sample_range.end_key_.assign(row_.get_obj_v1(0),rowkey_info->get_size());
                    index_sample.range_ = construct_sample_range;
                    index_sample.row_count_ = row_count;
                    if(store == true)//add wexing [improve sec_index]
                    {
                      if(OB_SUCCESS !=(ret = get_tablet_histogram()->add_sample_with_deep_copy(index_sample)))
                      {
                        TBSYS_LOG(WARN,"fail to add sample to histogram =%d",ret);
                      }
                      store =false;
                    }
                    else if(store == false)
                    {
                      store = true;
                    }
                    row_count = 1;
                  }
                  else
                  {
                    temp_tablet_row_count ++;
                    row_count ++;
                  }
                }
                if(OB_SUCCESS == ret)
                {
                  if(OB_SUCCESS != (ret = calc_tablet_col_checksum_index_local_tablet(*cur_row,data_rowkey_desc,cc_index_column_desc,cc.get_str(),data_tid)))
                  {
                    TBSYS_LOG(ERROR,"fail to calculate data table column checksum index =%d",ret);
                    break;
                  }
                  else if(OB_SUCCESS != (ret = column_checksum_.sum(cc)))
                  {
                    TBSYS_LOG(ERROR,"checksum sum error =%d",ret);
                    break;
                  }
                  cc.reset();
                }
                if(OB_SUCCESS == ret)
                {
                  if(OB_SUCCESS != (ret = save_current_row()))
                  {
                    TBSYS_LOG(ERROR,"write row error,ret=%d",ret);
                    break;
                  }
                }
              }
            }//end while
            if(OB_ITER_END == ret)
            {
              ret = OB_SUCCESS;
              if(0 == tablet_row_count)
              {
                index_sample.range_.table_id_ = index_tid;
                index_sample.row_count_ = 0;
                index_sample.range_.start_key_.set_min_row();
                index_sample.range_.end_key_.set_max_row();
                if(OB_SUCCESS != (ret = get_tablet_histogram()->add_sample_with_deep_copy(index_sample)))
                {
                  TBSYS_LOG(WARN,"fail to add sample to histogram =%d",ret);
                }
              }
              if(OB_SUCCESS == ret)
              {
                get_tablet_histogram_report_info()->static_index_histogram_ = *(get_tablet_histogram());
                ObMultiVersionTabletImage& tablet_image = tablet_manager_->get_serving_tablet_image();
                frozen_version_ = tablet_image.get_serving_version();
                //delete by liuxiao [secondary index bug fix]20150812
                //ObServer master_master_ups;
                //if(OB_SUCCESS != (ret = ObChunkServerMain::get_instance()->get_chunk_server().get_rpc_proxy()->get_master_master_update_server(true,master_master_ups)))
                //{
                //  TBSYS_LOG(ERROR,"failed to get ups for report index cchecksum");
                //}
                //delete e
                //add liumz, [secondary index col checksum]20160405:b
                if (new_range_.end_key_.is_max_row())
                {
                  new_range_.end_key_.set_max_row();
                  new_range_.border_flag_.unset_inclusive_end();
                }
                //add:e
                if(OB_SUCCESS != (ret = tablet_manager_->send_tablet_column_checksum(column_checksum_, new_range_, frozen_version_)))
                {
                  TBSYS_LOG(ERROR,"send tablet column checksum failed =%d",ret);
                }
                column_checksum_.reset();
              }
            }
          }
        }
        if(OB_SUCCESS != ob_sstable_scan.reset_scanner())
        {
          TBSYS_LOG(WARN, "close sstable scan failed!");
        }
      }
      if(true)
      {
        sstable_writer_.close_sstable(trailer_offset,current_sstable_size_);
        sort_.reset();
      }
      return ret;
    }

    int ObIndexBuilder::fill_sstable_schema(const uint64_t table_id, const ObSchemaManagerV2 &common_schema, ObSSTableSchema &sstable_schema)
    {
      return build_sstable_schema(table_id,common_schema,
                                        sstable_schema, false);
    }

    int ObIndexBuilder::save_current_row()
    {
      int ret = OB_SUCCESS;
      /*if (true)
      {
        //TBSYS_LOG(DEBUG, "current row expired.");
        //hex_dump(row_.get_row_key().ptr(), row_.get_row_key().length(), false, TBSYS_LOG_LEVEL_DEBUG);
      }
      else
      */
      if ((ret = sstable_writer_.append_row(row_,current_sstable_size_)) != OB_SUCCESS )
      {
        TBSYS_LOG(ERROR, "write_index : append row failed [%d], this row_, obj count:%ld, "
                         "table:%lu, group:%lu",
            ret, row_.get_obj_count(), row_.get_table_id(), row_.get_column_group_id());
        for(int32_t i=0; i<row_.get_obj_count(); ++i)
        {
          row_.get_obj(i)->dump(TBSYS_LOG_LEVEL_ERROR);
        }
      }
      return ret;
    }

    int ObIndexBuilder::fill_scan_param(ObScanParam &param)
    {
      int ret = OB_SUCCESS;
      const ObSSTableSchemaColumnDef* def = NULL;
#if 0
      RangeServerHash* range_server = NULL;
      if(NULL == (range_server = worker_->get_range_info()))
      {
        TBSYS_LOG(WARN, "failed to get range server info");
        ret = OB_ERROR;
      }
      else
#endif
      {
        param.set_fake(true);
        param.set_copy_args(true);

        //param.add_column()
        ret = param.set_range(new_range_);
      }

      if(OB_SUCCESS == ret)
      {
        for(int64_t i = 0; i < sstable_schema_.get_column_count(); i++)
        {
          if(NULL == (def = sstable_schema_.get_column_def((int)i)))
          {
            TBSYS_LOG(WARN, "get column define failed");
            ret = OB_ERROR;
            break;
          }
          else
          {
            param.add_column(def->column_name_id_);
            //TBSYS_LOG(INFO, "test::whx add column %d",(int)def->column_name_id_);
          }
        }
      }
      if(OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "fill scan param failed[%d]", ret);
      }
      return ret;
    }

    int ObIndexBuilder::create_new_sstable(uint64_t table_id, int32_t disk)
    {
      int ret = OB_SUCCESS;
      sstable_id_.sstable_file_id_ = tablet_manager_->allocate_sstable_file_seq();
      sstable_id_.sstable_file_offset_ = 0;
      int32_t disk_no = disk;
      int64_t sstable_block_size = OB_DEFAULT_SSTABLE_BLOCK_SIZE;
      bool is_sstable_exist = false;
      sstable_schema_.reset();
      //sstable_writer_.reset();
      if(disk_no < 0)
      {
        TBSYS_LOG(ERROR,"does't have enough disk space for static index");
        sstable_id_.sstable_file_id_ = 0;
        ret = OB_CS_OUTOF_DISK_SPACE;
      }
      //const ObSchemaManagerV2* mgrv2 = manager_->get_user_schema(0);
      else if(NULL == current_schema_manager_)
      {
        TBSYS_LOG(ERROR,"faild to get schema manager!");
        ret = OB_SCHEMA_ERROR;
      }
      else if(NULL == (new_table_schema_ = current_schema_manager_->get_table_schema(table_id)))
      {
        TBSYS_LOG(ERROR, "failed to get table[%ld] schema",table_id);
        ret = OB_SCHEMA_ERROR;
      }
      else if(OB_SUCCESS != (ret = fill_sstable_schema(table_id,*current_schema_manager_,sstable_schema_)))
      {
        TBSYS_LOG(ERROR,"fill sstable schema error");
      }
      else
      {
        if (NULL != new_table_schema_ && new_table_schema_->get_block_size() > 0
            && 64 != new_table_schema_->get_block_size())
        {
          sstable_block_size = new_table_schema_->get_block_size();
        }
      }
      if (OB_SUCCESS == ret)
      {
        do
        {
          sstable_id_.sstable_file_id_     = (sstable_id_.sstable_file_id_ << 8) | (disk_no & 0xff);

          if ((OB_SUCCESS == ret) && (ret = get_sstable_path(sstable_id_,path_,sizeof(path_))) != OB_SUCCESS )
          {
            TBSYS_LOG(ERROR,"Create Index : can't get the path of new sstable");
            ret = OB_ERROR;
          }
          TBSYS_LOG(DEBUG,"test::whx create new sstable, sstable_path:%s", path_);
          if (OB_SUCCESS == ret)
          {
            is_sstable_exist = FileDirectoryUtils::exists(path_);
            if (is_sstable_exist)
            {
              sstable_id_.sstable_file_id_ = tablet_manager_->allocate_sstable_file_seq();
            }
          }
        } while (OB_SUCCESS == ret && is_sstable_exist);

        if (OB_SUCCESS == ret)
        {
          TBSYS_LOG(DEBUG,"test::whx create new sstable, sstable_path:%s, version=%ld, block_size=%ld,sstable_id=%ld\n",
                                              path_, frozen_version_, sstable_block_size,sstable_id_.sstable_file_id_);
          path_string_.assign_ptr(path_,static_cast<int32_t>(strlen(path_) + 1));
          TBSYS_LOG(DEBUG,"test::whx create new sstable, path_string_:%.*s", path_string_.length(), path_string_.ptr());
          compressor_string_.assign_ptr(const_cast<char*>(new_table_schema_->get_compress_func_name()),static_cast<int32_t>(sizeof(new_table_schema_->get_compress_func_name())+1));
          int64_t element_count = DEFAULT_ESTIMATE_ROW_COUNT;
          if ((ret = sstable_writer_.create_sstable(sstable_schema_,
                  path_string_, compressor_string_, frozen_version_,
                  OB_SSTABLE_STORE_DENSE, sstable_block_size, element_count)) != OB_SUCCESS)
          {
            if (OB_IO_ERROR == ret)
              tablet_manager_->get_disk_manager().set_disk_status(disk_no,DISK_ERROR);
            TBSYS_LOG(ERROR,"Merge : create sstable failed : [%d]",ret);
          }
        }
      }
      return ret;
    }

    //add zhuyanchao[secondary index static_data_build]20150401
    int ObIndexBuilder::construct_index_tablet_info(ObTablet* tablet)
    {
     // TBSYS_LOG(WARN,"test::zhuyanchao test get into construct index tablet info");
      int ret = OB_SUCCESS;
      if(NULL != tablet && tablet_manager_ != NULL)
      {
       // TBSYS_LOG(WARN,"test::zhuyanchao test get into construct index tablet info");
        const ObSSTableTrailer* trailer = NULL;
        trailer =&sstable_writer_.get_trailer();
        tablet->set_disk_no(disk_no_);
        tablet->set_data_version(frozen_version_);
        //tablet->set_range(new_range_);
          if(NULL == trailer)
          {
          TBSYS_LOG(ERROR, "null pointor of construct index tablet trailer");
            ret = OB_ERROR;
          }
        else if(OB_SUCCESS != (ret = tablet->add_sstable_by_id(sstable_id_)))
          {
            TBSYS_LOG(ERROR,"STATIC INDEX : add sstable to tablet failed.");
          }
          else
          {
              index_extend_info_.occupy_size_=current_sstable_size_;
              index_extend_info_.row_count_=trailer->get_row_count();
              //index_extend_info_.row_checksum_=sstable_writer_.get_row_checksum();
              index_extend_info_.check_sum_=sstable_writer_.get_trailer().get_sstable_checksum();
              index_extend_info_.last_do_expire_version_ = tablet_manager_->get_serving_data_version();//todo @haixing same as data table
              index_extend_info_.sequence_num_ =0;//todo checksum
              //delete liuxiao [secondary_index] 20150522
              //memcpy(index_extend_info_.column_checksum_, &cc, sizeof(col_checksum));//todo column_checksum
              //delete end
        }
        if (OB_SUCCESS == ret)
        {
          tablet->set_extend_info(index_extend_info_);
        }
        if (OB_SUCCESS == ret)
        {
          tablet_manager_->get_disk_manager().add_used_space(
          (sstable_id_.sstable_file_id_ & DISK_NO_MASK),
          current_sstable_size_, false);
        }
      }
      else
      {
        TBSYS_LOG(ERROR,"index image tablet is null");
        ret = OB_ERROR;
      }
      return ret;
    }
    //add liumz[secondary index static_data_build.report]20161028
    int ObIndexBuilder::update_local_index_meta(ObTablet* tablet,const bool sync_meta)
    {
      int ret = OB_SUCCESS;
      if(tablet == NULL)
      {
          TBSYS_LOG(WARN,"update meta index error, null pointor of tablet");
          ret = OB_ERROR;
      }
      if(tablet_manager_ == NULL)
      {
          TBSYS_LOG(WARN,"update meta index error, null pointor of tabletmanager");
          ret = OB_ERROR;
      }
      if(OB_SUCCESS == ret)
      {
        ObMultiVersionTabletImage& tablet_image = tablet_manager_->get_serving_tablet_image();
        if (OB_SUCCESS == ret)
        {
          ret = check_tablet(tablet);//todo zhuyanchao check tablet
        }

        if(OB_SUCCESS == ret)
        {
          if (sync_meta)
          {
            // sync new index tablet meta files;
            if (OB_SUCCESS != (ret = tablet_image.write(
                                 tablet->get_data_version(),
                                 tablet->get_disk_no())))
            {
              TBSYS_LOG(ERROR,"write local index meta failed , version=%ld, disk_no=%d",
                        tablet->get_data_version(), tablet->get_disk_no());
            }
            else
            {
              TBSYS_LOG(INFO,"write local index meta success!");
            }
          }
        }
      }
      return ret;
    }
    //add e
    //add zhuyanchao[secondary index static_data_build.report]20150401
    int ObIndexBuilder::update_meta_index(ObTablet* tablet,const bool sync_meta)
    {
      int ret = OB_SUCCESS;
      if(tablet == NULL)
      {
          TBSYS_LOG(WARN,"update meta index error, null pointor of tablet");
          ret = OB_ERROR;
      }
      if(tablet_manager_ == NULL)
      {
          TBSYS_LOG(WARN,"update meta index error, null pointor of tabletmanager");
          ret = OB_ERROR;
      }
      if(OB_SUCCESS == ret)
      {
      ObMultiVersionTabletImage& tablet_image = tablet_manager_->get_serving_tablet_image();
      if (OB_SUCCESS == ret)
      {
        ret = check_tablet(tablet);//todo zhuyanchao check tablet
      }

      if(OB_SUCCESS == ret)
      {
          if (OB_SUCCESS != (ret = tablet_image.upgrade_index_tablet(
                  tablet, false)))
          {
            TBSYS_LOG(WARN,"upgrade new index tablets error [%d]",ret);
          }
          else
          {
            if (sync_meta)
            {
              // sync new index tablet meta files;
              if (OB_SUCCESS != (ret = tablet_image.write(
                      tablet->get_data_version(),
                      tablet->get_disk_no())))
              {
                TBSYS_LOG(WARN,"write new index meta failed , version=%ld, disk_no=%d",
                    tablet->get_data_version(), tablet->get_disk_no());
                //add zhuaynchao [secondary bug write tablet image fail 20151224]
                int32_t disk_no = 0;
                if(OB_SUCCESS != (ret = tablet_image.remove_tablet(tablet->get_range(),tablet->get_data_version(),disk_no)))
                {
                  TBSYS_LOG(WARN, "failed to remove tablet from tablet image, "
                                  "version=%ld, disk=%d, range=%s",
                            tablet->get_data_version(), tablet->get_disk_no(),
                            to_cstring(tablet->get_range()));
                }
                //add e
              }
             }
           }
          }
      }
      return ret;
    }
    //add e
    //add zhuyanchao[secondary index static_index_build]20150403
    int ObIndexBuilder::calc_tablet_col_checksum_index(const ObRow& row, ObRowDesc desc, char *column_checksum, int64_t tid)
    {
       //TBSYS_LOG(WARN,"test::zhuyanchao begin to calc checksum");
       int ret=OB_SUCCESS;
       int64_t count = 0;
       const ObRowDesc::Desc *desc_index = desc.get_cells_desc_array(count);
       // TBSYS_LOG(WARN,"test::zhuyanchao begin to calc checksum = %ld",count);
       if(OB_SUCCESS==ret)
       {
           int pos=0,len=0;
           const ObObj* obj=NULL;
           uint64_t column_id=OB_INVALID_ID;
           ObBatchChecksum bc;
           uint64_t rowkey_checksum=0;
           int64_t rowkey_count=0;
           for(int64_t i=0;i < count;i++)
           {
             row.get_cell(tid,desc_index[i].column_id_,obj);
             bc.reset();
             if(obj==NULL)
             {
               ret=OB_ERROR;
               TBSYS_LOG(ERROR,"get sstable row obj error,ret=%d",ret);
               break;
             }
             else
             {
               obj->checksum(bc);
               if(rowkey_count < desc.get_rowkey_cell_count())
               {
                 rowkey_count++;
                 rowkey_checksum+=bc.calc();
                 if(rowkey_count==desc.get_rowkey_cell_count())
                 {
                   //0 for rowkey column checksum
                   len=snprintf(column_checksum+pos,OB_MAX_COL_CHECKSUM_STR_LEN-1-pos,"%ld",(uint64_t)0);
                   if(len<0)
                   {
                     TBSYS_LOG(ERROR,"write column checksum error");
                     ret=OB_ERROR;
                   }
                   else
                   {
                     pos+=len;
                     column_checksum[pos++]=':';
                     if(pos<OB_MAX_COL_CHECKSUM_STR_LEN-1)
                     {
                       len=snprintf(column_checksum+pos,OB_MAX_COL_CHECKSUM_STR_LEN-1-pos,"%lu",rowkey_checksum);
                       pos+=len;
                     }
                     if(i!=count-1)
                     {
                       column_checksum[pos++]=',';
                     }
                     else
                     {
                       column_checksum[pos++]='\0';
                     }
                   }
                 }
               }
               else
               {
                 column_id = desc_index[i].column_id_;
                 len=snprintf(column_checksum+pos,OB_MAX_COL_CHECKSUM_STR_LEN-1-pos,"%ld",column_id);
                 if(len<0)
                 {
                   TBSYS_LOG(ERROR,"write column checksum error");
                   ret=OB_ERROR;
                 }
                 else
                 {
                   pos+=len;
                   column_checksum[pos++]=':';
                   if(pos<OB_MAX_COL_CHECKSUM_STR_LEN-1)
                   {
                     len=snprintf(column_checksum+pos,OB_MAX_COL_CHECKSUM_STR_LEN-1-pos,"%lu",bc.calc());
                     pos+=len;
                   }
                   if(i!=count-1)
                   {
                     column_checksum[pos++]=',';
                   }
                   else
                   {
                     column_checksum[pos++]='\0';
                   }
                 }
               }
             }
           }

       }
       //add e
        return ret;
    }

    //add liuxiao [secondary index col checksum]20150529
    int ObIndexBuilder::calc_tablet_col_checksum_index_local_tablet(const ObRow& row, ObRowDesc &rowkey_desc, ObRowDesc &index_desc, char *column_checksum, int64_t tid)
    {
      //用于计算原表的列校验�?
      //rowkey_desc保存了主键的信息,index_desc保存了非主键列，但在索引表中的列（index列或storing 列）
      //TBSYS_LOG(ERROR, "test::liuxiao cal cc in indexbuilder");
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

      if(OB_SUCCESS == ret)
      {
        for(i = 0;i < rowkey_count;i++)
        {
          //先计算所有主键列的校验和
          if(OB_SUCCESS != (ret = row.get_cell(tid, desc_rowkey[i].column_id_, obj)))
          {
            ret = OB_ERROR;
            TBSYS_LOG(ERROR, "get sstable row obj error,ret=%d", ret);
            break;
          }
          else if(obj == NULL)
          {
            ret = OB_ERROR;
            TBSYS_LOG(ERROR, "row obj error NULL");
            break;
          }
          else
          {
            bc.reset();
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
          if(i != ((rowkey_count+index_count)))
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
        //计算�?有剩余列
        for(i = 0;i < index_count;i++)
        {
          if(OB_SUCCESS != (ret = row.get_cell(tid, desc_index[i].column_id_, obj)))
          {
            ret = OB_ERROR;
            TBSYS_LOG(ERROR, "get sstable row obj error,ret=%d", ret);
            break;
          }
          else if(obj == NULL)
          {
            ret = OB_ERROR;
            TBSYS_LOG(ERROR, "get sstable row obj error,ret=%d", ret);
            break;
          }
          else
          {
            bc.reset();
            obj->checksum(bc);
          }
          column_id = desc_index[i].column_id_;
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
            if(i != (index_count-1))
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

    void ObIndexBuilder::cal_sample_rate(const int64_t tablet_row_count, int64_t &sample_rate)
    {
      if(0 == tablet_row_count % sample_rate)
      {
        sample_rate = tablet_row_count/sample_rate;
      }
      else
      {
        sample_rate = tablet_row_count/sample_rate +1;
      }
    }

    int ObIndexBuilder::push_cid_in_desc_and_ophy(uint64_t index_tid, uint64_t data_tid, const ObRowDesc index_data_desc, ObArray<uint64_t> &basic_columns, ObRowDesc &desc)
    {
      int ret = OB_SUCCESS;
      uint64_t tid = OB_INVALID_ID;
      uint64_t cid = OB_INVALID_ID;
      for(int64_t j = 0; j < index_data_desc.get_rowkey_cell_count(); j++)
      {
        if(OB_SUCCESS != (ret = index_data_desc.get_tid_cid(j, tid, cid)))
        {
          TBSYS_LOG(WARN,"get column schema failed,idx[%ld] ret[%d]",j,ret);
          break;
        }
        else
        {
          basic_columns.push_back(cid);
          sort_.add_sort_column(data_tid,cid,true);
          desc.add_column_desc(data_tid,cid);
        }
      }
      if(OB_SUCCESS == ret)
      {
        for(int64_t j = index_data_desc.get_rowkey_cell_count(); j < index_data_desc.get_column_num();j++)
        {
          if(OB_SUCCESS != (ret = index_data_desc.get_tid_cid(j, tid, cid)))
          {
            TBSYS_LOG(WARN,"get column schema failed,cid[%ld]",cid);
            ret = OB_ERROR;
            break;
          }
          else if(OB_SUCCESS != (ret = basic_columns.push_back(cid)))
          {
            TBSYS_LOG(WARN, "push back basic columns failed, ret = %d", ret);
            break;
          }
          else if(OB_SUCCESS != (ret = sort_.add_sort_column(data_tid,cid,true)))
          {
            TBSYS_LOG(WARN, "add sort column failed ,data_tid[%ld],cid[%ld], ret = %d", data_tid, cid, ret);
            break;
          }
          else if(NULL != (current_schema_manager_->get_column_schema(index_tid, cid)))
          {
            desc.add_column_desc(data_tid,cid);
          }
        }
      }

      if(OB_SUCCESS == ret)
      {
        desc.set_rowkey_cell_count(index_data_desc.get_rowkey_cell_count());
      }

      return ret;
    }
    //add e
    //add e
    //add zhuyanchao[secondary index static_index_build]20150530
    int ObIndexBuilder::check_tablet(const ObTablet* tablet)
    {
      int ret = OB_SUCCESS;
      if (NULL == tablet)
      {
        TBSYS_LOG(WARN, "tablet is NULL");
        ret = OB_INVALID_ARGUMENT;
      }
      else if (!tablet_manager_->get_disk_manager().is_disk_avail(tablet->get_disk_no()))
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
    //add e
  }//end chunkserver

}//end oceanbase
