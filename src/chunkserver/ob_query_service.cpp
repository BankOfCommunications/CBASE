/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * ob_query_service.cpp for query(get or scan), do merge, join,
 * group by, order by, limit, topn operation and so on.
 *
 * Authors:
 *   huating <huating.zmq@taobao.com>
 *
 */
#include "common/ob_action_flag.h"
#include "common/ob_trace_log.h"
#include "ob_query_service.h"

namespace oceanbase
{
  namespace chunkserver
  {
    using namespace oceanbase::common;

    ObQueryService::ObQueryService(ObChunkServer& chunk_server)
    : chunk_server_(chunk_server),
      get_scan_proxy_(chunk_server.get_tablet_manager()),
      rpc_proxy_(*chunk_server.get_rpc_proxy()),
      query_agent_(get_scan_proxy_),
      ups_get_cell_stream_(&rpc_proxy_, MERGE_SERVER),
      ups_scan_cell_stream_(&rpc_proxy_, MERGE_SERVER),
      schema_mgr_(NULL), row_cells_cnt_(0), got_rows_cnt_(0),
      max_scan_rows_(0)
    {

    }

    ObQueryService::~ObQueryService()
    {

    }

    int ObQueryService::get(const ObGetParam& get_param, ObScanner& scanner,
                            const int64_t timeout_time)
    {
      int ret = OB_SUCCESS;
      int64_t i = 0;
      bool need_merge_dynamic_data = false;
      uint64_t table_id = OB_INVALID_ID;
      const ObTableSchema* table_schema = NULL;
      int64_t row_count = get_param.get_row_size();
      const ObGetParam::ObRowIndex* row_index = get_param.get_row_index();
      ObOperatorMemLimit mem_limit;
      ObVersionRange version_range;

      if(0 >= get_param.get_cell_size() || NULL == row_index)
      {
        TBSYS_LOG(WARN,"get param cell size error, cell_size=%ld, row_index=%p",
                  get_param.get_cell_size(), row_index);
        ret = OB_INVALID_ARGUMENT;
      }

      //get local newest schema
      if (OB_SUCCESS == ret)
      {
        ret = rpc_proxy_.get_schema(get_param[0]->table_id_, ObMergerRpcProxy::LOCAL_NEWEST, &schema_mgr_);
        if (OB_SUCCESS != ret || NULL == schema_mgr_)
        {
          TBSYS_LOG(WARN, "fail to get the latest schema, unexpected error");
          ret = OB_SCHEMA_ERROR;
        }
        else
        {
          for (i = 0; i < row_count && OB_SUCCESS == ret; ++i)
          {
            if (table_id != get_param[row_index[i].offset_]->table_id_)
            {
              table_id = get_param[row_index[i].offset_]->table_id_;
              table_schema = schema_mgr_->get_table_schema(table_id);
              if (NULL != table_schema && table_schema->get_consistency_level() != STATIC)
              {
                need_merge_dynamic_data = true;
                break;
              }
              else if (NULL == table_schema)
              {
                TBSYS_LOG(WARN, "fail to get table schema, table_id=%lu", table_id);
                ret = OB_SCHEMA_ERROR;
              }
            }
          }

          if (OB_SUCCESS == ret && i == row_count && !need_merge_dynamic_data)
          {
            /**
             * if all the tables in get parameter needn't merge the dynamic
             * data from udpate server, modify the version range of scan
             * param, version range with 0 start version and end version,
             * chunkserver will not read dynamic data from udpateserver.
             */
            version_range.start_version_ = 0;
            version_range.end_version_ = 0;
            version_range.border_flag_.inclusive_start();
            version_range.border_flag_.inclusive_end();
            const_cast<ObGetParam&>(get_param).set_version_range(version_range);
          }
        }
      }

      // do request
      if (OB_SUCCESS == ret)
      {
        mem_limit.max_merge_mem_size_ = chunk_server_.get_config().max_merge_mem_size;
        ret = query_agent_.start_agent(
              get_param, ups_get_cell_stream_,
              ups_get_cell_stream_, *schema_mgr_,
              mem_limit, timeout_time);
        if (OB_CS_TABLET_NOT_EXIST == ret)
        {
          scanner.set_is_req_fullfilled(true, 0);
        }
      }

      if (OB_SUCCESS == ret)
      {
        ret = fill_get_data(get_param, scanner);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "failed to store cell array to scanner, ret=%d", ret);
        }
      }

      return ret;
    }

    void ObQueryService::end_get()
    {
      if (NULL != schema_mgr_)
      {
        rpc_proxy_.release_schema(schema_mgr_);
        schema_mgr_ = NULL;
      }
      query_agent_.clear();
      row_cells_cnt_ = 0;
      got_rows_cnt_ = 0;
      max_scan_rows_ = 0;
    }

    int ObQueryService::fill_get_data(const ObGetParam& get_param,
                                      ObScanner& scanner)
    {
      int ret = OB_SUCCESS;
      ObInnerCellInfo *cur_cell = NULL;
      int64_t fullfilled_item_num = 0;
      bool is_row_changed = false;
      ObCellInfo temp_cell;
      scanner.set_data_version(ups_get_cell_stream_.get_data_version());
      if (row_cells_cnt_ > 0)
      {
        for (int64_t i = 0; i < row_cells_cnt_; ++i)
        {
          //TODO: optimize it soon
          temp_cell.table_id_ = (*row_cells_[i]).table_id_;
          temp_cell.column_id_ = (*row_cells_[i]).column_id_;
          temp_cell.row_key_ = (*row_cells_[i]).row_key_;
          temp_cell.value_ = (*row_cells_[i]).value_;
          ret = scanner.add_cell(temp_cell, false, (0 == i));
          if (OB_SUCCESS != ret)
          {
            TBSYS_LOG(WARN, "failed to add cell to scanner, ret=%d", ret);
            break;
          }
        }
      }

      while (OB_SUCCESS == ret && OB_SUCCESS == (ret = query_agent_.next_cell()))
      {
        ret = query_agent_.get_cell(&cur_cell, &is_row_changed);
        if (OB_SUCCESS == ret)
        {
          if (row_cells_cnt_ >= MAX_ROW_COLUMN_NUMBER)
          {
            TBSYS_LOG(WARN, "row cells count is too much, row_cells_cnt_=%ld, "
                            "max_row_cells_cnt=%ld", row_cells_cnt_, MAX_ROW_COLUMN_NUMBER);
            is_row_changed = true;
          }
          if (is_row_changed)
          {
            row_cells_cnt_ = 0;
          }
          {
            row_cells_[row_cells_cnt_++] = cur_cell;
            //TODO: optimize it soon
            temp_cell.table_id_ = cur_cell->table_id_;
            temp_cell.column_id_ = cur_cell->column_id_;
            temp_cell.row_key_ = cur_cell->row_key_;
            temp_cell.value_ = cur_cell->value_;
          }
          ret = scanner.add_cell(temp_cell, false, is_row_changed);
          if (OB_SIZE_OVERFLOW == ret)
          {
            int64_t roll_cell_idx = fullfilled_item_num - 1;
            while ((get_param[roll_cell_idx]->table_id_
                    == get_param[fullfilled_item_num - 1]->table_id_)
                   &&(get_param[roll_cell_idx]->row_key_
                      == get_param[fullfilled_item_num - 1]->row_key_))
            {
              roll_cell_idx --;
            }
            fullfilled_item_num = roll_cell_idx + 1;
            ret = scanner.rollback();
            if (OB_SUCCESS == ret)
            {
              scanner.set_whole_result_row_num(query_agent_.get_total_result_row_count());
              ret = scanner.set_is_req_fullfilled(false, fullfilled_item_num);
            }
            break;
          }
          else if (OB_SUCCESS != ret)
          {
            TBSYS_LOG(WARN, "failed to add cell to scanner, ret=%d", ret);
          }
        }

        if (OB_SUCCESS == ret)
        {
          fullfilled_item_num ++;
        }
      }

      if (OB_ITER_END == ret)
      {
        row_cells_cnt_ = 0;
        scanner.set_whole_result_row_num(query_agent_.get_total_result_row_count());
        scanner.set_data_version(query_agent_.get_data_version());
        ret = scanner.set_is_req_fullfilled(true, fullfilled_item_num);
      }
      else if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "error occurs while fill get data, ret=%d", ret);
      }

      FILL_TRACE_LOG("ret=%d, size=%ld, cell_num=%ld, row_num=%ld, "
                     "fullfilled_item_num=%ld,",
        ret, scanner.get_size(), scanner.get_cell_num(), scanner.get_row_num(),
        fullfilled_item_num);

      return ret;
    }

    int ObQueryService::scan(const ObScanParam& scan_param, ObScanner& scanner,
                             const int64_t timeout_time)
    {
      int ret = OB_SUCCESS;
      bool is_full_dump = false;
      const ObTableSchema* table_schema = NULL;
      ObOperatorMemLimit mem_limit;
      ObVersionRange version_range;
      ObVersionRange org_version_range;

      // get local newest schema
      ret = rpc_proxy_.get_schema(scan_param.get_table_id(), ObMergerRpcProxy::LOCAL_NEWEST, &schema_mgr_);
      if (OB_SUCCESS != ret || NULL == schema_mgr_)
      {
        TBSYS_LOG(WARN, "fail to get the latest schema, unexpected error");
        ret = OB_SCHEMA_ERROR;
      }
      else
      {
        table_schema = schema_mgr_->get_table_schema(scan_param.get_table_id());
        if (NULL != table_schema)
        {
          /**
           * if the table needn't merge the dynamic data from udpate
           * server and it isn't full dump, modify the version range of
           * scan param, version range with 0 start version and end
           * version, chunkserver will not read dynamic data from
           * udpateserver.
           */
          //add wenghaixing [secondary index static_index_build.cs_scan] 20150326
          if(scan_param.if_need_fake())
          {
            version_range.start_version_ = 0;
            version_range.end_version_ = 0;
            version_range.border_flag_.inclusive_start();
            version_range.border_flag_.inclusive_end();
            const_cast<ObScanParam&>(scan_param).set_version_range(version_range);
          }
          //add e
          //modify wenghaixing [secondary index static_index_build.cs_scan]20150326
          //if (table_schema->get_consistency_level() == STATIC) //old code
          ///这里添加了如果scan_param包含了局部索引的range的情况，这个时候是不需要ups的数据的
          else if (table_schema->get_consistency_level() == STATIC)
          //modify e
          {
            org_version_range = scan_param.get_version_range();
            is_full_dump =
              (!org_version_range.border_flag_.is_max_value()
              && !org_version_range.border_flag_.is_min_value()
              && ((org_version_range.start_version_ == org_version_range.end_version_
                  && org_version_range.border_flag_.inclusive_start()
                  && org_version_range.border_flag_.inclusive_end())
                || (org_version_range.start_version_ + 1 == org_version_range.end_version_
                  && !org_version_range.border_flag_.inclusive_start()
                  && org_version_range.border_flag_.inclusive_end())
                || (org_version_range.start_version_ + 2 == org_version_range.end_version_
                  && !org_version_range.border_flag_.inclusive_start()
                  && !org_version_range.border_flag_.inclusive_end())));
            if (!is_full_dump)
            {
              version_range.start_version_ = 0;
              version_range.end_version_ = 0;
              version_range.border_flag_.inclusive_start();
              version_range.border_flag_.inclusive_end();
              const_cast<ObScanParam&>(scan_param).set_version_range(version_range);
            }
          }

          /**
           * if each scan operation of the table is too big, it consume
           * too much resource, we could limit the resource usage of the
           * table, so we limit the scan rows per tablet of the table,
           * maybe the result is not correctly, but it save some resource
           * for another necessary scan operation. this is a degradation
           * step for high load. here we use the scan_size_ member to
           * store the max scan rows per tablet temporarily. the scan size
           * is splited to 2 parts, the high 32 bits store
           * max_scan_rows_per_tabelt, the low 32 bits store
           * internal_ups_scan_size.
           */
          int64_t ups_scan_size = table_schema->get_internal_ups_scan_size();
          max_scan_rows_ = table_schema->get_max_scan_rows_per_tablet();
          if (max_scan_rows_ > 0 || ups_scan_size > 0)
          {
            if (ups_scan_size > 0 && 0 == max_scan_rows_
                && scan_param.get_is_result_cached())
            {
              const_cast<ObScanParam&>(scan_param).set_read_mode(ScanFlag::SYNCREAD);
            }
            int64_t scan_size = MAKE_SCAN_SIZE(ups_scan_size,max_scan_rows_);
            const_cast<ObScanParam&>(scan_param).set_scan_size(scan_size);
          }
        }
        else
        {
          TBSYS_LOG(WARN, "fail to get table schema, table_id=%lu",
            scan_param.get_table_id());
          ret = OB_SCHEMA_ERROR;
        }
      }

      // do request
      if (OB_SUCCESS == ret)
      {
        mem_limit.merge_mem_size_ = chunk_server_.get_config().merge_mem_size;
        mem_limit.max_merge_mem_size_ = chunk_server_.get_config().max_merge_mem_size;
        mem_limit.groupby_mem_size_ = chunk_server_.get_config().groupby_mem_size;
        mem_limit.max_groupby_mem_size_ = chunk_server_.get_config().max_groupby_mem_size;
        ret = query_agent_.start_agent(
              scan_param, ups_scan_cell_stream_,
              ups_get_cell_stream_, *schema_mgr_,
              mem_limit, timeout_time);
      }

      if (OB_SUCCESS == ret)
      {
        ret = fill_scan_data(scanner);
        if (OB_SUCCESS != ret && OB_ITER_END != ret)
        {
          TBSYS_LOG(WARN, "failed to store cell array to scanner, ret=%d", ret);
        }
        // dump scanner for debug only
        // scan_param.dump();
        // scanner.dump_all(TBSYS_LOG_LEVEL_DEBUG);
      }

      return ret;
    }

    void ObQueryService::end_scan()
    {
      if (NULL != schema_mgr_)
      {
        rpc_proxy_.release_schema(schema_mgr_);
        schema_mgr_ = NULL;
      }
      query_agent_.clear();
      row_cells_cnt_ = 0;
      got_rows_cnt_ = 0;
      max_scan_rows_ = 0;
    }

    int ObQueryService::fill_scan_data(ObScanner& scanner)
    {
      int ret = OB_SUCCESS;
      ObInnerCellInfo *cur_cell = NULL;
      bool is_fullfilled = false;
      bool is_row_changed = false;
      bool need_groupby = query_agent_.need_groupby();
      ObCellInfo temp_cell;
      if (row_cells_cnt_ > 0)
      {
        for (int64_t i = 0; i < row_cells_cnt_; ++i)
        {
          //TODO: optimize it soon
          temp_cell.table_id_ = (*row_cells_[i]).table_id_;
          temp_cell.column_id_ = (*row_cells_[i]).column_id_;
          temp_cell.row_key_ = (*row_cells_[i]).row_key_;
          temp_cell.value_ = (*row_cells_[i]).value_;
          ret = scanner.add_cell(temp_cell, false, (0 == i));
          if (OB_SUCCESS != ret)
          {
            TBSYS_LOG(WARN, "failed to add cell to scanner, ret=%d", ret);
            break;
          }
        }
      }

      if (OB_SUCCESS == ret)
      {
        scanner.set_data_version(ups_scan_cell_stream_.get_data_version());
        ret = scanner.set_range(get_scan_proxy_.get_tablet_range());
      }

      while (OB_SUCCESS == ret && OB_SUCCESS == (ret = query_agent_.next_cell()))
      {
        ret = query_agent_.get_cell(&cur_cell, &is_row_changed);
        if (OB_SUCCESS == ret)
        {
          if (is_row_changed)
          {
            row_cells_cnt_ = 0;
          }
          if (row_cells_cnt_ >= MAX_ROW_COLUMN_NUMBER)
          {
            TBSYS_LOG(WARN, "row cells count is too much, row_cells_cnt_=%ld, "
                            "max_row_cells_cnt=%ld",
              row_cells_cnt_, MAX_ROW_COLUMN_NUMBER);
            ret = OB_SIZE_OVERFLOW;
            break;
          }
          else
          {
            row_cells_[row_cells_cnt_++] = cur_cell;
            //TODO: optimize it soon
            temp_cell.table_id_ = cur_cell->table_id_;
            temp_cell.column_id_ = cur_cell->column_id_;
            temp_cell.row_key_ = cur_cell->row_key_;
            temp_cell.value_ = cur_cell->value_;
          }

          ret = scanner.add_cell(temp_cell, false, is_row_changed);
          if (OB_SUCCESS == ret && max_scan_rows_ > 0 && !need_groupby)
          {
            if (is_row_changed)
            {
              got_rows_cnt_++;
            }

            if (got_rows_cnt_ >= max_scan_rows_ && scanner.get_row_num() > 0
                && 0 == scanner.get_cell_num() % scanner.get_row_num())
            {
              ret = OB_ITER_END;
              break;
            }
          }

          if (OB_SIZE_OVERFLOW == ret)
          {
            ret = scanner.rollback();
            if (OB_SUCCESS == ret)
            {

              scanner.set_whole_result_row_num(query_agent_.get_total_result_row_count());
              ret = scanner.set_is_req_fullfilled(false, 1);
            }
            break;
          }
          else if (OB_SUCCESS != ret)
          {
            TBSYS_LOG(WARN, "failed to add cell to scanner, ret=%d", ret);
          }
        }
      }

      if (OB_ITER_END == ret)
      {
        row_cells_cnt_ = 0;
        scanner.set_whole_result_row_num(query_agent_.get_total_result_row_count());
        scanner.set_data_version(query_agent_.get_data_version());
        is_fullfilled = query_agent_.is_request_fullfilled();
        ret = scanner.set_is_req_fullfilled(is_fullfilled, 1);
        if (OB_SUCCESS == ret)
        {
          ret = OB_ITER_END;
        }
      }
      else if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "error occurs while fill scan data, ret=%d", ret);
      }

      FILL_TRACE_LOG("ret=%d, size=%ld, cell_num=%ld, row_num=%ld, "
                     "whole_row_num=%ld, got_rows=%ld, max_scan_rows=%ld, "
                     "is_fullfilled=%d,",
        ret, scanner.get_size(), scanner.get_cell_num(), scanner.get_row_num(),
        scanner.get_whole_result_row_num(), got_rows_cnt_, max_scan_rows_,
        is_fullfilled);

      return ret;
    }
  } // end namespace chunkserver
} // end namespace oceanbase
