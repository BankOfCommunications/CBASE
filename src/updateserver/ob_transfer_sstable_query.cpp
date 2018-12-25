/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *  
 * ob_transfer_sstable_query.cpp for get or scan columns from
 * transfer sstables. 
 *
 * Authors:
 *   huating <huating.zmq@taobao.com>
 *
 */
#include <tbsys.h>
#include "common/ob_get_param.h"
#include "common/ob_scan_param.h"
#include "sstable/ob_sstable_scanner.h"
#include "sstable/ob_sstable_getter.h"
#include "sstable/ob_disk_path.h"
#include "ob_transfer_sstable_query.h"

namespace oceanbase
{
  namespace updateserver
  {
    using namespace tbsys;
    using namespace common;
    using namespace sstable;

    ObTransferSSTableQuery::ObTransferSSTableQuery(common::IFileInfoMgr &fileinfo_mgr)
    : inited_(false), block_cache_(fileinfo_mgr),
      block_index_cache_(fileinfo_mgr), sstable_row_cache_(NULL)
    {

    }

    ObTransferSSTableQuery::~ObTransferSSTableQuery()
    {
      if (NULL != sstable_row_cache_)
      {
        sstable_row_cache_->destroy();
        delete sstable_row_cache_;
        sstable_row_cache_ = NULL;
      }
    }

    int ObTransferSSTableQuery::init(const int64_t block_cache_size,
                                     const int64_t block_index_cache_size,
                                     const int64_t sstable_row_cache_size)
    {
      int ret = OB_SUCCESS;

      //ret = fileinfo_cache_.init(bc_conf.ficache_max_num);
      if (OB_SUCCESS == ret)
      {
        ret = block_cache_.init(block_cache_size);
      }

      if (OB_SUCCESS == ret)
      {
        ret = block_index_cache_.init(block_index_cache_size);
      }

      if (OB_SUCCESS == ret)
      {
        if (sstable_row_cache_size > 0)
        {
          sstable_row_cache_ = new (std::nothrow) ObSSTableRowCache();
          if (NULL != sstable_row_cache_)
          {
            if (OB_SUCCESS != (ret = sstable_row_cache_->init(sstable_row_cache_size)))
            {
              TBSYS_LOG(ERROR, "init sstable row cache failed");
            }
          }
          else 
          {
            TBSYS_LOG(WARN, "failed to new sstable row cache");
            ret = OB_ERROR;
          }
        }
      }

      if (OB_SUCCESS == ret)
      {
        inited_ = true;
      }

      return ret;
    }

    int ObTransferSSTableQuery::enlarge_cache_size(
       const int64_t block_cache_size, 
       const int64_t block_index_cache_size,
       const int64_t sstable_row_cache_size)
    {
      int ret = OB_SUCCESS;
      if (!inited_)
      {
        TBSYS_LOG(INFO, "not inited");
        ret = OB_NOT_INIT;
      }
      else
      {
        block_cache_.enlarg_cache_size(block_cache_size);
        block_index_cache_.enlarg_cache_size(block_index_cache_size);
        if (NULL != sstable_row_cache_ && sstable_row_cache_size > 0)
        {
          sstable_row_cache_->enlarg_cache_size(sstable_row_cache_size);
        }
      }
      return ret;
    }

    int ObTransferSSTableQuery::get(const ObGetParam& get_param, 
                                    ObSSTableReader& reader,
                                    ObIterator* iterator)
    {
      int ret                         = OB_SUCCESS;
      int64_t cell_size               = get_param.get_cell_size();
      int64_t row_size                = get_param.get_row_size();
      ObSSTableGetter* sstable_getter = dynamic_cast<ObSSTableGetter*>(iterator);
      const ObGetParam::ObRowIndex* row_index = get_param.get_row_index();
      static __thread const ObSSTableReader* sstable_reader = NULL;

      if (!inited_)
      {
        TBSYS_LOG(WARN, "transfer sstable query has not inited");
        ret = OB_ERROR;
      }
      else if (cell_size <= 0 || NULL == row_index || row_size <= 0
               || row_size > OB_MAX_GET_ROW_NUMBER)
      {
        TBSYS_LOG(WARN, "invalid get param, cell_size=%ld, row_index=%p, row_size=%ld",
                  cell_size, row_index, row_size);
        ret = OB_INVALID_ARGUMENT;
      }
      else if (NULL == sstable_getter)
      {
        TBSYS_LOG(WARN, "get thread local instance of sstable getter failed");
        ret = OB_ERROR;
      }

      if (OB_SUCCESS == ret)
      {
        if (!reader.empty())
        {
          sstable_reader = &reader;
        }
        else
        {
          sstable_reader = NULL;
        }

        ret = sstable_getter->init(block_cache_, block_index_cache_, 
                                   get_param, &sstable_reader, 1, true,
                                   sstable_row_cache_);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "failed to init sstable getter, ret=%d", ret);
        }
        else
        {
          // do nothing
        }
      }

      return ret;
    }

    int ObTransferSSTableQuery::scan(const ObScanParam& scan_param, ObSSTableReader& reader,
                                     ObIterator* iterator)
    {
      int ret                           = OB_SUCCESS;
      ObSSTableScanner *sstable_scanner = dynamic_cast<ObSSTableScanner*>(iterator);

      if (!inited_)
      {
        TBSYS_LOG(WARN, "transfer sstable query has not inited");
        ret = OB_ERROR;
      }
      else if (NULL == sstable_scanner)
      {
        TBSYS_LOG(WARN, "failed to dynamic cast iterator to sstable scanner, sstable_scanner=%p", 
                  sstable_scanner);
        ret = OB_ALLOCATE_MEMORY_FAILED;
      }
      else
      {
        if (!reader.empty())
        {
        ret = sstable_scanner->set_scan_param(scan_param, &reader, block_cache_, block_index_cache_, true);
        }
        else 
        {
          ret = sstable_scanner->set_scan_param(scan_param, NULL, block_cache_, block_index_cache_, true);
      }
      }

      return ret;
    }

    int ObTransferSSTableQuery::get_sstable_end_key(
      const ObSSTableReader& reader, const uint64_t table_id, ObRowkey& row_key)
    {
      int ret = OB_SUCCESS;
      ObBlockIndexPositionInfo info;

      if (!inited_)
      {
        TBSYS_LOG(WARN, "transfer sstable query has not inited");
        ret = OB_ERROR;
      }
      else if (OB_INVALID_ID == table_id)
      {
        TBSYS_LOG(WARN, "invalid table id, table_id=%lu", table_id);
        ret = OB_INVALID_ARGUMENT;
      }
      else 
      {
        info.sstable_file_id_ = reader.get_sstable_id().sstable_file_id_;
        info.offset_ = reader.get_trailer().get_block_index_record_offset();
        info.size_   = reader.get_trailer().get_block_index_record_size();

        ret = block_index_cache_.get_end_key(info, table_id, row_key);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "failed to get sstable end key, table_id=%lu", table_id);
        }
      }

      return ret;
    }
  }//end namespace updateserver

  namespace sstable
  {
    //mod zhaoqiong [fixed for Backup]:20150811:b
    //uint64_t get_sstable_disk_no(const uint64_t sstable_id)
    uint64_t get_ups_sstable_disk_no(const uint64_t sstable_id)
    //mod:e
    {
      return sstable_id; 
    }
  }
}//end namespace oceanbase
