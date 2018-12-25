/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or 
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *  
 * ob_get_scan_proxy.cpp for get scan proxy of merge and join 
 * agent. 
 *
 * Authors:
 *   huating <huating.zmq@taobao.com>
 *
 */
#include "ob_get_scan_proxy.h"
#include "ob_tablet_manager.h"
#include "common/ob_scan_param.h"
#include "common/ob_column_filter.h"

namespace oceanbase 
{
  namespace chunkserver 
  {
    using namespace oceanbase::common;
    using namespace oceanbase::sstable;
    using namespace oceanbase::compactsstable;

    ObGetScanProxy::ObGetScanProxy(ObTabletManager& manager)
      : ObMergerRpcProxy(), tablet_manager_(manager)
    {
    }
        
    ObGetScanProxy::~ObGetScanProxy() 
    {
    }

    int ObGetScanProxy::cs_get(const ObGetParam& get_param, ObScanner& scanner, 
                               ObIterator* it_out[],int64_t& it_size)
    {
      int ret = OB_SUCCESS;
      int64_t idx = 0;
      if (NULL == it_out || it_size <= 0)
      {
        ret = OB_INVALID_ARGUMENT;
      }

      if (OB_SUCCESS == ret)
      {
        ret = tablet_manager_.get(get_param, scanner);
      }

      //ms depend on OB_CS_TABLET_NOT_EXIST return value
      if (OB_SUCCESS != ret && OB_CS_TABLET_NOT_EXIST != ret)
      {
        TBSYS_LOG(WARN, "failed to get data from local chunk server, ret=%d", ret);
      }
      else
      {
        it_out[idx++] = &scanner;
      }

      if (OB_SUCCESS == ret)
      {
        ObScanner* compact_scanner = GET_TSI_MULT(ObScanner,TSI_CS_COMPACTSSTABLE_GET_SCANEER_1);
        if (NULL == compact_scanner)
        {
          TBSYS_LOG(WARN,"alloc scanner failed"); //just warn
        }
        else
        {
          compact_scanner->reset();
          if ((ret = get_compact_scanner(get_param,scanner,*compact_scanner)) != OB_SUCCESS )
          {
            TBSYS_LOG(WARN,"get data from compact sstable failed,err=%d",ret);
          }
          else if (idx < it_size)
          {
            if (!compact_scanner->is_empty())
            {
              it_out[idx++] = compact_scanner;
            }
            int64_t compact_version = compact_scanner->get_data_version();
            if (compact_version > 0)
            {
              scanner.set_data_version(compact_version);
            }            
          }
        }
      }

      it_size = idx;

      return ret;
    }

    int ObGetScanProxy::cs_scan(const ObScanParam& scan_param, ObScanner& scanner, 
                                ObIterator* it_out[],int64_t& it_size)
    {
      int ret = OB_SUCCESS;
      int64_t it_num = 0;

      if ((NULL == it_out) || it_size <= 0)
      {
        ret = OB_INVALID_ARGUMENT;
      }

      if ((OB_SUCCESS == ret) && ((ret = tablet_manager_.scan(scan_param, scanner)) != OB_SUCCESS))
      {
        TBSYS_LOG(WARN, "failed to scan data from local chunk server, ret=%d", ret);        
      }

      if (OB_SUCCESS == ret)
      {
        ObSSTableScanner *sstable_scanner = 
          GET_TSI_MULT(ObSSTableScanner, TSI_CS_SSTABLE_SCANNER_1);
        if (NULL == sstable_scanner)
        {
          TBSYS_LOG(ERROR, "failed to get thread local sstable scanner, "
                           "sstable_scanner=%p", 
                    sstable_scanner);
          ret = OB_ALLOCATE_MEMORY_FAILED;
        }
        else
        {
          tablet_range_.reset();
          ret = scanner.get_range(tablet_range_);
          if (OB_SUCCESS == ret)
          {
            it_out[it_num++] = sstable_scanner;
          }
          else 
          {
            TBSYS_LOG(WARN, "failed to get tablet range from scanner, ret=%d", ret);
          }
        }
      }

      if (OB_SUCCESS == ret)
      {
        int64_t query_version = 0;
        const ObVersionRange version_range = scan_param.get_version_range();
        if (!version_range.border_flag_.is_max_value() && version_range.end_version_ != 0)
        {
          query_version = version_range.end_version_;
        }
        ObTablet*& scan_tablet            = tablet_manager_.get_cur_thread_scan_tablet();
        int32_t compactsstable_num        = scan_tablet->get_compactsstable_num();

        if (compactsstable_num > 0)
        {
          ObCompactSSTableMemNode* mem_node = scan_tablet->get_compactsstable_list();
          ObCompactMemIteratorArray *its    = GET_TSI_MULT(ObCompactMemIteratorArray,TSI_CS_COMPACTSSTABLE_ITERATOR_1);
          ColumnFilter* cf                  = GET_TSI_MULT(ColumnFilter,TSI_CS_COLUMNFILTER_1);          
          ObCompactSSTableMem* mem          = NULL;          
          int64_t major_version             = 0;
          int32_t i                         = 0;          

          if ((NULL == mem_node) || (NULL == cf) || (NULL == its))
          {
            TBSYS_LOG(WARN,"unexpect error,mem_node=%p,cf=%p,its=%p",mem_node,cf,its);
            ret = OB_ALLOCATE_MEMORY_FAILED;
          }
          else
          {
            cf->clear();
          }

          if ((OB_SUCCESS == ret) && (NULL == (cf = ColumnFilter::build_columnfilter(scan_param, cf))))
          {
            TBSYS_LOG(WARN,"build columnfilter failed");
            ret = OB_ERROR;
          }

          for(i=0; (mem_node != NULL) && (i < compactsstable_num) && (OB_SUCCESS == ret) && (it_num < it_size);++i)
          {
            mem = &mem_node->mem_;
            if (NULL == mem)
            {
              TBSYS_LOG(WARN,"unexpect error,compact mem is null");
              ret = OB_ERROR;
            }
            else
            {
              //query version just have major version
              major_version = mem->get_version_range().major_version_;
              if ((0 == query_version) || (query_version > 0 && major_version <= query_version))
              {
                if ((ret = its->iters_[i].init(mem)) != OB_SUCCESS)
                {
                  TBSYS_LOG(WARN,"init iterator of compact mem failed,ret=%d",ret);
                }
                else if ((ret = its->iters_[i].set_scan_param(*scan_param.get_range(),cf,
                                                              scan_param.get_scan_direction() == ScanFlag::BACKWARD))
                         != OB_SUCCESS)
                {
                  TBSYS_LOG(WARN,"set scan param failed,ret=%d",ret);
                }
                else
                {
                  it_out[it_num++] = &its->iters_[i];
                  //set data version to the last compact sstable version
                  scanner.set_data_version(mem->get_data_version());
                  FILL_TRACE_LOG("add compact iterator to merger,it_num:%ld,version:%ld",it_num,mem->get_data_version());
                }
              }
              else
              {
                break; //got enough data
              }
            }
            mem_node = mem_node->next_;
          } //end for
        } //end compactsstable_num > 0
      }

      if (OB_SUCCESS == ret)
      {
        it_size = it_num;
      }

      return ret;
    }

    int ObGetScanProxy::get_compact_scanner(const ObGetParam& get_param,
                                            ObScanner& orig_scanner,ObScanner& compact_scanner)
    {
      ObTabletManager::ObGetThreadContext*& get_context = tablet_manager_.get_cur_thread_get_contex();
      const ObGetParam::ObRowIndex* row_index           = get_param.get_row_index();      
      int64_t row_num                                   = orig_scanner.get_row_num();
      ColumnFilter* cf                                  = GET_TSI_MULT(ColumnFilter,TSI_CS_COLUMNFILTER_1);      
      ObTablet* tablet                                  = NULL;
      int ret                                           = OB_SUCCESS;      
      ObRowkey rowkey;

      if ((NULL == get_context) || (NULL == row_index) || (NULL == cf))
      {
        TBSYS_LOG(WARN,"get thread context of get failed");
        ret = OB_ERROR;
      }
      else
      {
        int64_t compactsstable_version = get_context->min_compactsstable_version_;
        if (ObVersion::compare(compactsstable_version,orig_scanner.get_data_version()) > 0)
        {
          compact_scanner.set_data_version(compactsstable_version);
          for(int64_t i=0; (i < row_num) && (OB_SUCCESS == ret); ++i)
          {
            tablet = get_context->tablets_[i];
            rowkey = get_param[row_index[i].offset_]->row_key_;

            build_get_column_filter(get_param,row_index[i].offset_,row_index[i].size_,*cf);

            if ((tablet != NULL) &&
                (ret = get_compact_row(*tablet,rowkey,compactsstable_version,cf,compact_scanner)) != OB_SUCCESS)
            {
              TBSYS_LOG(WARN,"get data from compactsstable failed,ret=%d",ret);
            }
          }
        }
      }
      return ret;
    }

    int ObGetScanProxy::get_compact_row(ObTablet& tablet,ObRowkey& rowkey,const int64_t compactsstable_version,
                                        const ColumnFilter *cf,
                                        ObScanner& compact_scanner)
    {

      int32_t compactsstable_num        = tablet.get_compactsstable_num();
      ObCompactSSTableMemNode* mem_node = tablet.get_compactsstable_list();
      ObCompactMemIteratorArray *its    = GET_TSI_MULT(ObCompactMemIteratorArray,TSI_CS_COMPACTSSTABLE_ITERATOR_1);
      ObCompactSSTableMem* mem          = NULL;
      int ret                           = OB_SUCCESS;
      int32_t m                         = 0;
      uint64_t table_id                 = OB_INVALID_ID;
      bool add_row_not_exist            = false;
      ObMerger merger;

      for(m=0; (OB_SUCCESS == ret) &&
            (mem_node != NULL) && (m < compactsstable_num); )
      {
        mem = &mem_node->mem_;
        if (NULL == mem)
        {
          TBSYS_LOG(WARN,"unexpect error,compactsstable is null");
          ret = OB_ERROR;
        }
        else if (ObVersion::compare(mem->get_data_version(),compactsstable_version) <= 0)
        {
          if (OB_INVALID_ID == table_id)
          {
            table_id = mem->get_table_id();
          }
          if (!mem->is_row_exist(rowkey))
          {
            //row not exists,do nothing            
            TBSYS_LOG(DEBUG,"row not exist,%s", to_cstring(rowkey));
            add_row_not_exist = true;
          }
          else if ((ret = its->iters_[m].init(mem)) != OB_SUCCESS)
          {
            TBSYS_LOG(WARN,"init iterator failed,ret=%d",ret);
          }
          else if ((ret = its->iters_[m].set_get_param(rowkey,cf)) != OB_SUCCESS)
          {
            TBSYS_LOG(WARN,"set get param failed,ret=%d",ret);
          }
          else if ((ret = merger.add_iterator(&(its->iters_[m]))) != OB_SUCCESS)
          {
            TBSYS_LOG(WARN,"add iterator to merger failed,ret=%d",ret);
          }          
          else
          {
            ++m;
            //success
          }
        }
        else
        {
          break; //got enough data
        }
        mem_node = mem_node->next_;
      }

      if ((m > 0) && (OB_SUCCESS == ret))
      {
        if ((ret = fill_compact_data(merger,compact_scanner)) != OB_SUCCESS)
        {
          TBSYS_LOG(WARN,"fill compact data failed,ret=%d",ret);
        }
      }
      else if ((OB_SUCCESS == ret) && add_row_not_exist)
      {
        //not exist
        ObCellInfo cell;
        cell.table_id_ = table_id;
        cell.column_id_ = OB_INVALID_ID;
        cell.value_.set_ext(ObActionFlag::OP_ROW_DOES_NOT_EXIST);
        cell.row_key_ = rowkey;
        if ((ret = compact_scanner.add_cell(cell,false,true)) != OB_SUCCESS)
        {
          TBSYS_LOG(WARN,"add not exist cell failed,ret=%d",ret);
        }
      }
      return ret;
    }
    
    int ObGetScanProxy::fill_compact_data(ObIterator& iterator,ObScanner& scanner)
    {
      int ret = OB_SUCCESS;

      ObCellInfo* cell = NULL;
      bool is_row_changed = false;

      while (OB_SUCCESS == (ret = iterator.next_cell()))
      {
        ret = iterator.get_cell(&cell,&is_row_changed);
        if (OB_SUCCESS != ret || NULL == cell)
        {
          TBSYS_LOG(WARN, "failed to get cell, cell=%p, err=%d", cell, ret);
        }
        else
        {
          ret = scanner.add_cell(*cell, false, is_row_changed);
          if (OB_SIZE_OVERFLOW == ret)
          {
            //TODO
            TBSYS_LOG(INFO, "ObScanner size full, cannot add any cell.");
            scanner.rollback();
            ret = OB_SUCCESS;
            break;
          }
          else if (OB_SUCCESS != ret)
          {
            TBSYS_LOG(WARN, "failed to add cell to scanner, ret=%d", ret);
          }
        }

        if (OB_SUCCESS != ret)
        {
          break;
        }
      }

      if (OB_ITER_END == ret)
      {
        ret = OB_SUCCESS;
      }
      else if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "retor occurs while iterating, ret=%d", ret);
      }
      return ret;
    }

    int ObGetScanProxy::build_get_column_filter(const ObGetParam& get_param,
                                                const int32_t offset,const int32_t size,
                                                ColumnFilter& cf)
    {
      int ret = OB_SUCCESS;
      
      cf.clear();
      
      for(int32_t i=0; (OB_SUCCESS == ret) && (i < size); ++i)
      {
        ret = cf.add_column(get_param[offset + i]->column_id_);
      }
      return ret;
    }
    
    const ObNewRange& ObGetScanProxy::get_tablet_range() const
    {
      return tablet_range_;
    }
  } // end namespace chunkserver
} // end namespace oceanbase
