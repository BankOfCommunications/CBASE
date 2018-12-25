/*
* (C) 2007-2011 TaoBao Inc.
*
* This program is free software; you can redistribute it and/or modify
* it under the terms of the GNU General Public License version 2 as
* published by the Free Software Foundation.
*
* ob_compactsstable_cache.cpp is for what ...
*
* Version: $id$
*
* Authors:
*   MaoQi maoqi@taobao.com
*
*/

#include "ob_compactsstable_cache.h"
#include "ob_chunk_server_main.h"

namespace oceanbase
{
  using namespace compactsstable;
  using namespace tbsys;
  using namespace common;
  namespace chunkserver
  {
    ObCompacSSTableMemGetter::ObCompacSSTableMemGetter(ObMergerRpcProxy& proxy):rpc_proxy_(proxy),
                                                                                cell_stream_(&rpc_proxy_)
    {
    }

    ObCompacSSTableMemGetter::~ObCompacSSTableMemGetter()
    {}

    int ObCompacSSTableMemGetter::get(ObTablet* tablet,int64_t data_version)
    {
      int ret = OB_SUCCESS;
      if (NULL == tablet)
      {
        TBSYS_LOG(WARN,"tablet is null");
        ret = OB_INVALID_ARGUMENT;
      }

      if ((OB_SUCCESS == ret) && ((ret = fill_scan_param(*tablet,data_version)) != OB_SUCCESS))
      {
        TBSYS_LOG(WARN,"fill scan param failed,ret=%d",ret);
      }

      if (OB_SUCCESS == ret)
      {
        ret = cell_stream_.scan(scan_param_);
        if ((ret != OB_SUCCESS) && (ret != OB_ITER_END))
        {
          TBSYS_LOG(WARN,"scan from ups failed,ret=%d",ret);
        }
        else if (OB_SUCCESS == ret)
        {
          ObCompactSSTableMemNode *cache = new(std::nothrow)ObCompactSSTableMemNode();
          if (NULL == cache)
          {
            ret = OB_ALLOCATE_MEMORY_FAILED;
          }
          else
          {
            int64_t ups_data_version = cell_stream_.get_data_version();
            ObFrozenVersionRange frozen_version;
            frozen_version.major_version_ = ObVersion::get_major(ups_data_version);
            frozen_version.minor_version_start_ = static_cast<int32_t>(ObVersion::get_minor(ups_data_version));
            frozen_version.minor_version_end_ = static_cast<int32_t>(ObVersion::get_minor(ups_data_version));
            frozen_version.is_final_minor_version_ = ObVersion::is_final_minor(ups_data_version);
            if ((ret = cache->mem_.init(frozen_version,0)) != OB_SUCCESS)
            {
              TBSYS_LOG(WARN,"init compact sstable failed,ret=%d",ret);
            }
            else if ((ret = fill_compact_sstable(cell_stream_,cache->mem_)) != OB_SUCCESS)
            {
              TBSYS_LOG(WARN,"write compact sstable failed,ret=%d",ret);
            }
            else
            {
              //success
            }

            if ((ret != OB_SUCCESS) && (cache != NULL))
            {
              delete cache;
              cache = NULL;
            }
          }

          if (OB_SUCCESS == ret)
          {
            ObVersion mv = cache->mem_.get_data_version();
            //mod zhaoqiong [Truncate Table]:20160318:b
            //TBSYS_LOG(INFO,"add compact cache to tablet,tablet:[%p,%s],cache:[%p,row_num:%ld,version:%d-%hd-%hd]",
            TBSYS_LOG(INFO,"add compact cache to tablet,tablet:[%p,%s],cache:[%p,row_num:%ld,version:%ld-%ld-%hd]",
                      tablet,scan_range2str(tablet->get_range()),
                      cache,cache->mem_.get_row_count(),mv.major_,mv.minor_,mv.is_final_minor_);
            //mod:e
            tablet->add_compactsstable(cache);
          }
        }
        else //OB_ITER_END == ret
        {
          //do nothing
          //TODO change version
          ret = OB_SUCCESS;
        }
      }
      return ret;
    }

    int ObCompacSSTableMemGetter::fill_scan_param(ObTablet& tablet,int64_t data_version)
    {
      int ret = OB_SUCCESS;
      ObString table_name_string;
      const ObNewRange& range = tablet.get_range();
      uint64_t table_id = range.table_id_;
      ObVersionRange version_range;

      scan_param_.reset();
      scan_param_.set(table_id, table_name_string, range);
      scan_param_.set_is_result_cached(false);
      scan_param_.set_is_read_consistency(false);
      fill_version_range(tablet,data_version,version_range);
      scan_param_.set_version_range(version_range);

      if ( (ret = scan_param_.add_column(0UL)) != OB_SUCCESS)
      {
        TBSYS_LOG(ERROR,"add column id to scan_param_ error[%d]",ret);
      }
      return ret;
    }

    void ObCompacSSTableMemGetter::fill_version_range(const ObTablet& tablet,const int64_t ups_data_version,
                                                      ObVersionRange& version_range) const
    {
      int64_t                     cs_data_version = tablet.get_cache_data_version();
      int64_t                     cs_major        = ObVersion::get_major(cs_data_version);
      int64_t                     ups_major       = ObVersion::get_major(ups_data_version);
      int64_t                     ups_minor       = ObVersion::get_minor(ups_data_version);
      int64_t                     cs_minor        = ObVersion::get_minor(cs_data_version);
      bool                        is_final_minor  = ObVersion::is_final_minor(cs_data_version);

      version_range.start_version_ = cs_data_version;
      version_range.end_version_ = ups_data_version;
      version_range.border_flag_.unset_inclusive_start();
      version_range.border_flag_.unset_inclusive_end();

      if (0 == cs_minor)
      {
        //minor is 0,there is no compactsstable
        version_range.start_version_ = cs_major + 1;
        version_range.border_flag_.set_inclusive_start();

        if (ups_major > (cs_major + 1))
        {
          version_range.end_version_ = cs_major + 2;
          version_range.border_flag_.unset_inclusive_end();
        }
      }
      else
      {
        //cs has minor version
        if ((cs_major == ups_major) && ((ups_minor - cs_minor) > 1))
        {

        }
        else if (ups_major > cs_major)
        {
          if (is_final_minor)
          {
            TBSYS_LOG(WARN,"there is some new frozen table,but we can hold only one major version");
          }
          else
          {
            version_range.end_version_ = cs_major;
            version_range.border_flag_.set_inclusive_end();
          }
        }
        else if (ups_major < cs_major)
        {
          //ups_major < cs_major ,impossible
          TBSYS_LOG(WARN,"unexpect error,ups_major:%ld,cs_major:%ld",ups_major,cs_major);
        }
        else
        {
          //should not be here,ups_major == cs_major && ups_minor - cs_minor equal to 1
          TBSYS_LOG(WARN,"unexpect error:major(%ld,%ld),minor:(%ld,%ld)",cs_major,ups_major,cs_minor,ups_minor);
        }
      }
    }

    int ObCompacSSTableMemGetter::fill_compact_sstable(ObScanCellStream& cell_stream,ObCompactSSTableMem& sstable)
    {
      int ret              = OB_SUCCESS;
      bool is_row_changed  = false;
      ObCellInfo *cur_cell = NULL;
      int64_t usage_size   = 0;
      int64_t compactsstable_cache_size = THE_CHUNK_SERVER.get_config().compactsstable_cache_size;
      ObObj rowkey_obj;

      while((OB_SUCCESS == ret) && (OB_SUCCESS == (ret = cell_stream.next_cell())))
      {
        usage_size  = ob_get_mod_memory_usage(ObModIds::OB_COMPACTSSTABLE_WRITER);
        if (usage_size > compactsstable_cache_size)
        {
          TBSYS_LOG(WARN,"compactsstable cache size overflow,usage:%ld,size limit:%ld",
                    usage_size,compactsstable_cache_size);
          ret = OB_SIZE_OVERFLOW;
        }

        if (OB_SUCCESS == (ret = cell_stream.get_cell(&cur_cell,&is_row_changed)))
        {
          if (is_row_changed && row_cell_num_ > 0)
          {
            if ((ret = save_row(cur_cell->table_id_,sstable)) != OB_SUCCESS)
            {
              TBSYS_LOG(WARN,"append row to sstable failed,ret=%d",ret);
            }
          }

          if (OB_SUCCESS == ret)
          {
            if (0 == row_cell_num_)
            {
              //TODO add rowkey
              //rowkey_obj.set_varchar(cur_cell->row_key_);
              if ((ret = row_.add_col(ObCompactRow::ROWKEY_COLUMN_ID,rowkey_obj)) != OB_SUCCESS)
              {
                TBSYS_LOG(WARN,"add column to row failed,ret=%d",ret);
              }
              else
              {
                ++row_cell_num_;
              }
            }

            if (cur_cell->value_.get_type() == ObExtendType &&
                cur_cell->value_.get_ext() == ObActionFlag::OP_NOP)
            {
              // we do not save nop
            }
            else if ((ret = row_.add_col(cur_cell->column_id_,cur_cell->value_)) != OB_SUCCESS)
            {
              TBSYS_LOG(WARN,"add column to row failed,ret=%d",ret);
            }
            else
            {
              ++row_cell_num_;
            }
          }
        }
      }

      if ((OB_ITER_END == ret) && (row_cell_num_ > 0))
      {
        if ((ret = save_row(cur_cell->table_id_,sstable)) != OB_SUCCESS)
        {
          TBSYS_LOG(WARN,"append row to sstable failed,ret=%d",ret);
        }
      }
      return ret;
    }

    int ObCompacSSTableMemGetter::save_row(uint64_t table_id,ObCompactSSTableMem& sstable)
    {
      int ret = OB_SUCCESS;
      int64_t size = 0;
      if (row_.get_col_num() > 1) //do not save row that have only rowkey column
      {
        if ((ret = row_.set_end()) != OB_SUCCESS)
        {
          TBSYS_LOG(WARN,"set end of row_ failed,ret=%d",ret);
        }

        if ((OB_SUCCESS == ret) && ((ret = sstable.append_row(table_id,row_,size)) != OB_SUCCESS))
        {
          TBSYS_LOG(WARN,"add row_ to sstable failed,ret=%d",ret);
        }
      }

      row_.reset();
      row_cell_num_ = 0;

      return ret;
    }


    ObCompactSSTableMemThread::ObCompactSSTableMemThread() : manager_(NULL),
                                                             inited_(false),
                                                             read_idx_(0),
                                                             write_idx_(0),
                                                             tablets_num_(0),
                                                             data_version_(0)
    {}

    ObCompactSSTableMemThread::~ObCompactSSTableMemThread()
    {
      destroy();
    }

    int ObCompactSSTableMemThread::init(ObTabletManager* manager)
    {
      int ret = OB_SUCCESS;
      int64_t thread_count = THE_CHUNK_SERVER.get_config().compactsstable_cache_thread_num;
      if ((NULL == manager))
      {
        TBSYS_LOG(WARN,"invalid argument,manager is null");
        ret = OB_INVALID_ARGUMENT;
      }

      if (thread_count <=0)
      {
        thread_count = 1;
      }
      else if (thread_count > MAX_THREAD_COUNT)
      {
        thread_count = MAX_THREAD_COUNT;
      }

      manager_ = manager;
      setThreadCount(static_cast<int32_t>(thread_count));
      start();
      inited_  = true;
      return ret;
    }

    void ObCompactSSTableMemThread::destroy()
    {
      if (inited_)
      {
        inited_ = false;
        //stop the thread
        stop();
        //signal
        mutex_.broadcast();
        //join
        wait();
        //release remain tablet
        ObTablet* tablet = NULL;
        while(NULL != (tablet = pop()))
        {
          manager_->get_serving_tablet_image().release_tablet(tablet);
        }

        manager_      = NULL;
        read_idx_     = 0;
        write_idx_    = 0;
        tablets_num_  = 0;
        data_version_ = 0;
      }
    }

    int ObCompactSSTableMemThread::push(ObTablet* tablet,int64_t data_version)
    {
      int ret = OB_SUCCESS;

      if (NULL == tablet || data_version <= 0)
      {
        TBSYS_LOG(WARN,"tablet is null");
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        mutex_.lock();
        if (ObVersion::compare(data_version,data_version_) > 0)
        {
          data_version_ = data_version;
        }

        if (write_idx_ >= MAX_TABLETS_NUM)
        {
          write_idx_ = 0;
        }

        if (tablets_num_ < MAX_TABLETS_NUM)
        {
          tablets_[write_idx_++] = tablet;
          ++tablets_num_;
          mutex_.signal();
        }
        else
        {
          TBSYS_LOG(WARN,"no left space to hold this tablet,read_idx_:%d,write_idx_:%d",read_idx_,write_idx_);
          ret = OB_SIZE_OVERFLOW;
        }
        mutex_.unlock();
      }
      return ret;
    }

    void ObCompactSSTableMemThread::run(CThread *thread, void *arg)
    {
      UNUSED(thread);
      UNUSED(arg);
      ObTablet* tablet = NULL;
      ObCompacSSTableMemGetter* getter = new(std::nothrow) ObCompacSSTableMemGetter(*THE_CHUNK_SERVER.get_rpc_proxy());
      if (NULL == getter)
      {
        TBSYS_LOG(WARN,"alloc getter failed");
      }
      else
      {
        while(!_stop)
        {
          mutex_.lock();
          while (!_stop && (NULL == (tablet = pop())))
          {
            mutex_.wait();
          }

          if (_stop)
          {
            mutex_.broadcast();
            mutex_.unlock();
            break;
          }

          mutex_.unlock();
          if (NULL != tablet) //just in case
          {
            if (getter->get(tablet,data_version_) != OB_SUCCESS)
            {
              TBSYS_LOG(WARN,"get data from ups failed");
            }
            //whether success or not,always release it.
            tablet->clear_compactsstable_flag();
            if (manager_->get_serving_tablet_image().release_tablet(tablet) != OB_SUCCESS)
            {
              TBSYS_LOG(WARN,"release tablet failed");
            }
          }
        } //end while

        delete getter;
        getter = NULL;
      }
    }

    //get lock first
    ObTablet* ObCompactSSTableMemThread::pop()
    {
      ObTablet* tablet = NULL;
      if ((write_idx_ == read_idx_))
      {
        // no tablet
      }
      else
      {
        if (read_idx_ >= MAX_TABLETS_NUM)
        {
          read_idx_ = 0;
        }

        if ((tablets_num_ > 0) && (read_idx_ != write_idx_))
        {
          tablet = tablets_[read_idx_++];
          --tablets_num_;
        }
      }
      return tablet;
    }
  } //end namespace chunkserver
} //end namespace oceanbase
