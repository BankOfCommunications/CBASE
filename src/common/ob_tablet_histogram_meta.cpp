/*===============================================================
*   (C) 2015-~ BankComm Inc.
*
*
*   Version: 0.1 2015-03-21
*
*   Authors:
*          liumengzhan(liumengzhan@bankcomm.com)
*
*
================================================================*/
#include "ob_tablet_histogram_meta.h"

namespace oceanbase {
  namespace common {


    void ObTabletHistogramMeta::dump() const
    {
      for (int32_t i = 0; i < common::OB_MAX_COPY_COUNT; i++)
      {
        TBSYS_LOG(INFO, "server_info_index = %d, timestamp = %ld", report_cs_info_[i].server_info_index_, report_cs_info_[i].report_timestamp_);
      }
    }

    void ObTabletHistogramTable::dump() const
    {
      TBSYS_LOG(INFO, "begin dump hist table. meta_table_.size():%d", (int32_t)meta_table_.get_array_index());
      if (NULL != tablet_hist_manager_ && NULL != root_server_)
      {
        for (int32_t i = 0; i < meta_table_.get_array_index(); i++)
        {
          TBSYS_LOG(INFO, "dump the %d-th tablet info", i);
          root_server_->dump_root_table(data_holder_[i].root_meta_index_);
          TBSYS_LOG(INFO, "dump the %d-th tablet info end", i);

          TBSYS_LOG(INFO, "dump the %d-th histogram info", i);
          data_holder_[i].dump();
          tablet_hist_manager_->dump(data_holder_[i].hist_index_);          
          TBSYS_LOG(INFO, "dump the %d-th histogram info end", i);
        }
      }
      TBSYS_LOG(INFO, "dump hist table complete");
    }

    ObTabletHistogramTable::ObTabletSampleRangeWrapper* ObTabletHistogramTable::alloc_srw_object()
    {
      ObTabletSampleRangeWrapper* srw = NULL;
      char* ptr = module_arena_.alloc_aligned(sizeof(ObTabletSampleRangeWrapper));
      if (NULL != ptr)
      {
        srw = new (ptr) ObTabletSampleRangeWrapper();
      }
      return srw;
    }

    int ObTabletHistogramTable::alloc_srw_object(const int64_t count)
    {
      int ret = OB_SUCCESS;
      if (NULL != srw_ptr_)
      {
        ob_free(srw_ptr_);
        srw_ptr_ = NULL;
      }
      srw_ptr_ = reinterpret_cast<ObTabletSampleRangeWrapper*>(ob_malloc(sizeof(ObTabletSampleRangeWrapper)*count, ObModIds::OB_STATIC_INDEX_HISTOGRAM));
      if (NULL != srw_ptr_)
      {
        for (int64_t i=0; i<count; i++)
        {
          new(srw_ptr_ + i) ObTabletSampleRangeWrapper();
        }
      }
      else
      {
        ret = OB_MEM_OVERFLOW;
      }
      return ret;
    }

    int ObTabletHistogramTable::add_meta(const ObTabletHistogramMeta &meta)
    {
      int ret = OB_SUCCESS;
      if (meta_table_.get_array_index() >= ObTabletHistogramManager::MAX_TABLET_COUNT_PER_TABLE)
      {
        TBSYS_LOG(WARN, "too many histogram, max is %ld now is %ld", ObTabletHistogramManager::MAX_TABLET_COUNT_PER_TABLE, meta_table_.get_array_index());
        ret = OB_ARRAY_OUT_OF_RANGE;
      }
      else if (!meta_table_.push_back(meta)) {
        TBSYS_LOG(WARN, "failed to push hist meta into meta_table_");
        ret = OB_ARRAY_OUT_OF_RANGE;
      }
      return ret;
    }

    /*
    int ObTabletHistogramTable::add_hist_meta(const ObTabletInfo &tablet_info, const ObTabletHistogram &hist, const int32_t server_index)
    {
      int ret = OB_SUCCESS;
      int32_t hist_index = OB_INVALID_INDEX;
      if (NULL == tablet_hist_manager_ || NULL == root_server_)
      {
        ret = OB_NOT_INIT;
        TBSYS_LOG(WARN, "root_server_ or tablet_hist_manager_ is null.");
      }
      else if(OB_SUCCESS != (ret = tablet_hist_manager_->add_histogram(hist, hist_index)))
      {
        TBSYS_LOG(WARN, "add histogram to manager failed.");
      }    
      if (OB_SUCCESS == ret)
      {
        ObTabletHistogramMeta hist_meta;
        hist_meta.hist_index_ = hist_index;
        hist_meta.server_info_indexes_[0] = server_index;
        if (OB_SUCCESS != (ret = root_server_->get_root_meta(tablet_info, hist_meta.root_meta_)))
        {
          TBSYS_LOG(WARN, "can not get hist_meta.root_meta_ from root table.");
        }
        else if (OB_SUCCESS != (ret = add_meta(hist_meta)))
        {
          TBSYS_LOG(WARN, "add histogram meta to table failed.");
        }

      }
      return ret;
    }*/

    int ObTabletHistogramTable::add_hist_meta(const ObTabletInfo &tablet_info, const int32_t hist_index, const int32_t server_index, const uint64_t idx_tid)
    {
      int ret = OB_SUCCESS;
      iterator find_pos = NULL;
      ObTabletHistogramMeta hist_meta;
      hist_meta.hist_index_ = hist_index;
//      hist_meta.server_info_indexes_[0] = server_index;
      if (NULL == root_server_)
      {
        ret = OB_NOT_INIT;
        TBSYS_LOG(WARN, "root_server_ is null.");
      }
      //add liumz, [check index tid consistency between hist table and reported sample]20151217:b
      else if (index_tid_ != idx_tid)
      {
        ret = OB_INVALID_DATA;
        TBSYS_LOG(WARN, "index tid is not consistent. index_tid_[%lu], sample's range tid[%lu]"
                  , index_tid_, idx_tid);
      }
      //add:e
      else if (NULL == (find_pos = find_tablet_pos(tablet_info)))
      {
        ret = OB_ERROR;
        TBSYS_LOG(WARN, "find_tablet_pos failed.");
      }
      //新的tablet_info, 该tablet的static index统计信息尚未汇报
      else if (end() == find_pos)
      {
        //mod liumz, bugfix: 汇报一个tablet的CS数超过OB_MAX_COPY_COUNT时, 新的CS汇报信息不会写入hist table, 20150505:b
        //hist_meta.server_info_indexes_[0] = server_index;
        hist_meta.report_cs_info_[0].server_info_index_ = server_index;
        hist_meta.report_cs_info_[0].report_timestamp_ = tbsys::CTimeUtil::getTime();
        //mod:e
        if (OB_SUCCESS != (ret = root_server_->get_root_meta_index(tablet_info, hist_meta.root_meta_index_)))
        {
          TBSYS_LOG(WARN, "can not get hist_meta.root_meta_ from root table.");
        }
        else if (OB_SUCCESS != (ret = add_meta(hist_meta)))
        {
          TBSYS_LOG(WARN, "add histogram meta to table failed.");
        }
      }
      //相同的tablet_info, 该tablet的static index统计信息之前已汇报过
      else
      {
        //find_pos->hist_index_要更新吗？
        int32_t server_index_pos = find_suitable_pos(find_pos, server_index);
        //mod liumz, 20150504:b
        //if (server_index_pos > 0 && server_index_pos < common::OB_MAX_COPY_COUNT)
        if (server_index_pos >= 0 && server_index_pos < common::OB_MAX_COPY_COUNT)
        //mod:e
        {
          //mod liumz, bugfix: 汇报一个tablet的CS数超过OB_MAX_COPY_COUNT时, 新的CS汇报信息不会写入hist table, 20150505:b
          //find_pos->server_info_indexes_[server_index_pos] = server_index;
          find_pos->report_cs_info_[server_index_pos].server_info_index_ = server_index;
          find_pos->report_cs_info_[server_index_pos].report_timestamp_ = tbsys::CTimeUtil::getTime();
          //mod:e
        }
      }
      return ret;
    }

    //add liumz, 20150323:b
    int ObTabletHistogramTable::check_reported_hist_info(const ObTabletInfo *compare_tablet, bool &is_finished) const
    {
      is_finished = false;
      int ret = OB_SUCCESS;
      if (NULL == compare_tablet)
      {
        ret = OB_INVALID_DATA;
        TBSYS_LOG(WARN, "compared tablet info is null.");
      }
      else if (NULL == root_server_)
      {
        ret = OB_NOT_INIT;
        TBSYS_LOG(WARN, "root_server_ is null.");
      }
      else if (meta_table_.get_array_index() > 0)
      {
        //first set ret = OB_NO_MONITOR_DATA, otherwise, if meta_table_'s size() == 0, return OB_SUCCESS anyway
        //check pass even if cs has not reported!!!
        for (int64_t i = 0; i < meta_table_.get_array_index(); i++)
        {
          const ObTabletInfo * tablet_info = NULL;
          if (OB_SUCCESS != (ret = root_server_->get_rt_tablet_info(data_holder_[i].root_meta_index_, tablet_info)))
          {
            TBSYS_LOG(WARN, "get_rt_tablet_info failed. ret=%d", ret);
            break;
          }
          else if (NULL == tablet_info)
          {
            ret = OB_INVALID_DATA;
            TBSYS_LOG(WARN, "tablet info is null. ret=%d", ret);
            break;
          }
          else if (tablet_info->equal(*compare_tablet))
          {
            is_finished = true;
            break;
          }
        }
      }
      return ret;
    }
    //add:e

    ObTabletHistogramTable::iterator ObTabletHistogramTable::find_tablet_pos(const ObTabletInfo &tablet_info)
    {
      iterator find_pos = NULL;
      if (NULL == root_server_)
      {
        TBSYS_LOG(WARN, "root_server_ is null.");
      }
      else
      {
        for (find_pos = begin(); find_pos != end(); find_pos++)
        {
          const ObTabletInfo * compare_tablet = NULL;
          if (OB_SUCCESS != root_server_->get_rt_tablet_info(find_pos->root_meta_index_, compare_tablet))
          {
            find_pos = NULL;
            TBSYS_LOG(WARN, "get_rt_tablet_info failed.");
            break;
          }
          else if (NULL == compare_tablet)
          {
            find_pos = NULL;
            TBSYS_LOG(WARN, "tablet info is null.");
            break;
          }
          else if (compare_tablet->equal(tablet_info))
          {
            break;
          }
        }
      }
      return find_pos;
    }

    int32_t ObTabletHistogramTable::find_suitable_pos(const_iterator it, const int32_t server_index) const
    {
      int32_t found_it_index = OB_INVALID_INDEX;
      //add liumz, bugfix: chekc null pointer, 20150505:b
      if (NULL == it)
      {
        TBSYS_LOG(WARN, "invalid parameter");
      }
      else
      {
      //add:e
        for (int32_t i = 0; i < common::OB_MAX_COPY_COUNT; i++)
        {
          //mod liumz, bugfix: 汇报一个tablet的CS数超过OB_MAX_COPY_COUNT时, 新的CS汇报信息不会写入hist table, 20150505:b
          //if (it->server_info_indexes_[i] == server_index)
          if (it->report_cs_info_[i].server_info_index_ == server_index)
          //mod:e
          {
            found_it_index = i;
            break;
          }
        }
        if (OB_INVALID_INDEX == found_it_index)
        {
          for (int32_t i = 0; i < common::OB_MAX_COPY_COUNT; i++)
          {
            //mod liumz, bugfix: 汇报一个tablet的CS数超过OB_MAX_COPY_COUNT时, 新的CS汇报信息不会写入hist table, 20150505:b
            //if (it->server_info_indexes_[i] == server_index)
            if (it->report_cs_info_[i].server_info_index_ == server_index)
            //mod:e
            {
              found_it_index = i;
              break;
            }
          }
        }
        //add liumz, bugfix: 汇报一个tablet的CS数超过OB_MAX_COPY_COUNT时, 新的CS汇报信息不会写入hist table, 20150505:b
        if (OB_INVALID_INDEX == found_it_index)
        {
          int64_t old_timestamp = ((((uint64_t)1) << 63) - 1);
          for (int32_t i = 0; i < common::OB_MAX_COPY_COUNT; i++)
          {
            if (it->report_cs_info_[i].report_timestamp_ < old_timestamp)
            {
              old_timestamp = it->report_cs_info_[i].report_timestamp_;
              found_it_index = i;
            }
          }
        }
        //add:e
      }

      return found_it_index;
    }


    bool compare_srw(const ObTabletHistogramTable::ObTabletSampleRangeWrapper *l_srw, const ObTabletHistogramTable::ObTabletSampleRangeWrapper *r_srw)
    {
      bool cmp = false;
      cmp = (l_srw->rowkey_.compare(r_srw->rowkey_) < 0);
      return cmp;
    }

    bool unique_srw(const ObTabletHistogramTable::ObTabletSampleRangeWrapper *l_srw, const ObTabletHistogramTable::ObTabletSampleRangeWrapper *r_srw)
    {
      bool cmp = false;
      cmp = (l_srw->rowkey_.compare(r_srw->rowkey_) == 0);
      return cmp;
    }

    int ObTabletHistogramTable::fill_all_ranges()
    {
      int ret = OB_SUCCESS;
      if (NULL == tablet_hist_manager_){
        ret = OB_NOT_INIT;
        TBSYS_LOG(WARN, "tablet_hist_manager_ is null");
      }
      if (OB_SUCCESS == ret)
      {
        TBSYS_LOG(INFO, "before alloc srw object, count[%ld]", tablet_hist_manager_->get_histogram_size()*(ObTabletHistogram::MAX_SAMPLE_BUCKET + 2));

        int ret = alloc_srw_object(tablet_hist_manager_->get_histogram_size()*(ObTabletHistogram::MAX_SAMPLE_BUCKET + 2));
//        ObTabletSampleRangeWrapper* l_srw = NULL;
//        ObTabletSampleRangeWrapper* r_srw = NULL;
        ObTabletHistogram* p_hist = NULL;
        ObTabletSample* p_sample =NULL;
        const_iterator it = begin();
        int64_t srw_idx = 0;
        for (; it != end() && OB_LIKELY(OB_SUCCESS == ret); it++)
        {
          if (OB_SUCCESS != (ret = tablet_hist_manager_->get_histogram(it->hist_index_, p_hist)))
          {
            TBSYS_LOG(WARN, "get histogram failed. hist index = %d", it->hist_index_);
          }
          else
          {
            for (int64_t i = 0; i < p_hist->get_sample_count() && OB_LIKELY(OB_SUCCESS == ret); i++)
            {
              if (OB_SUCCESS != (ret = p_hist->get_sample(i, p_sample)))
              {
                TBSYS_LOG(WARN, "get sample from histogram failed. sample index = %ld", i);
              }
              //add liumz, check if p_sample is null, 20150428:b
              else if (NULL == p_sample)
              {
                ret = OB_ERROR;
                TBSYS_LOG(WARN, "p_sample is NULL");
              }
              //add:e
//              else if (NULL == (l_srw = alloc_srw_object()) || NULL == (r_srw = alloc_srw_object()))
//              {
//                ret = OB_MEM_OVERFLOW;
//                TBSYS_LOG(ERROR, "alloc memory for srw failed.");
//              }
              else
              {
                (srw_ptr_ + srw_idx)->hist_meta_ = it;
                (srw_ptr_ + srw_idx + 1)->hist_meta_ = it;
                (srw_ptr_ + srw_idx)->rowkey_ = p_sample->get_range().start_key_;
                (srw_ptr_ + srw_idx + 1)->rowkey_ = p_sample->get_range().end_key_;
                srw_idx += 2;
//                int64_t count = srw_array_->count();
                  /*
                  if (count + 2 >= ObTabletHistogramManager::MAX_SAMPLE_COUNT_PER_TABLE)
                  {
                    ret = OB_ARRAY_OUT_OF_RANGE;
                    TBSYS_LOG(WARN, "too many samples in one table, fill all ranges failed.");
                  }
                  else if (OB_SUCCESS != (ret = srw_array_.push_back(l_srw)) || OB_SUCCESS != (ret = srw_array_.push_back(r_srw)))
                  */
//                TBSYS_LOG(DEBUG, "l_srw.rowkey_[%s], r_srw.rowkey_[%s]", to_cstring(l_srw.rowkey_), to_cstring(r_srw.rowkey_));
//                if (OB_SUCCESS != (ret = srw_array_->push_back(l_srw)) || OB_SUCCESS != (ret = srw_array_->push_back(r_srw)))
//                {
//                  ret = OB_ARRAY_OUT_OF_RANGE;
//                  TBSYS_LOG(WARN, "too many samples in one table, fill all ranges failed.");
//                }
//                else
//                {

//                  ObTabletSampleRangeWrapper &l_srw_2 = srw_array_->at(count);
//                  ObTabletSampleRangeWrapper &r_srw_2 = srw_array_->at(count + 1);
//                common::ObSortedVector<ObTabletSampleRangeWrapper *>::iterator insert_pos = NULL;
//                  TBSYS_LOG(DEBUG, "l_srw_2.rowkey_[%s], r_srw_2.rowkey_[%s]", to_cstring(l_srw_2.rowkey_), to_cstring(r_srw_2.rowkey_));
//                if (OB_SUCCESS != (ret = sorted_srw_list_.insert_unique(l_srw, insert_pos, compare_srw, unique_srw)))
//                {
//                  TBSYS_LOG(DEBUG, "can not insert left srw into sorted_srws_");
//                }
//                if (OB_CONFLICT_VALUE == ret)
//                {
//                  ret = OB_SUCCESS;
//                  TBSYS_LOG(DEBUG, "duplicate sample range.");
//                }
//                if (OB_SUCCESS == ret && OB_SUCCESS != (ret = sorted_srw_list_.insert_unique(r_srw, insert_pos, compare_srw, unique_srw)))
//                {
//                  TBSYS_LOG(DEBUG, "can not insert right srw into sorted_srws_");
//                }
//                if (OB_CONFLICT_VALUE == ret)
//                {
//                  ret = OB_SUCCESS;
//                  TBSYS_LOG(DEBUG, "duplicate sample range.");
//                }
              }
            }//end for
          }
        }//end for

        TBSYS_LOG(INFO, "before insert sorted list, srw_idx[%ld]", srw_idx);
        common::ObSortedVector<ObTabletSampleRangeWrapper *>::iterator insert_pos = NULL;
        for (int64_t i=0; i<srw_idx; i++)
        {
          if (OB_SUCCESS != (ret = sorted_srw_list_.insert_unique(srw_ptr_+i, insert_pos, compare_srw, unique_srw)))
          {
            TBSYS_LOG(DEBUG, "can not insert left srw into sorted_srws_");
          }
          if (OB_CONFLICT_VALUE == ret)
          {
            ret = OB_SUCCESS;
            TBSYS_LOG(DEBUG, "duplicate sample range.");
          }
        }
        TBSYS_LOG(INFO, "after insert sorted list, size[%d]", sorted_srw_list_.size());

      }
#if 0
      //add liumz, [dump sorted_srw_list_]20151207:b
      if (OB_SUCCESS == ret)
      {
        ObTabletSampleRangeWrapper * srw = NULL;
        for (int32_t idx = 0; idx < sorted_srw_list_.size(); idx++)
        {
          if (NULL != (srw = sorted_srw_list_.at(idx)))
          {
            TBSYS_LOG(INFO, "rowkey[%s], idx[%d]", to_cstring(srw->rowkey_), idx);
            srw->hist_meta_->dump();
          }
        }
      }
      //add:e
#endif
      return ret;
    }

    //mod liumz, [paxos static index]20170626:b
    //int ObTabletHistogramTable::get_next_tablet_info(const int64_t sample_num, ObTabletInfo &tablet_info, int32_t *server_index, const int32_t copy_count)
    int ObTabletHistogramTable::get_next_tablet_info(const int64_t sample_num, ObTabletInfo &tablet_info, int32_t *server_index, const int32_t copy_count, const int32_t cluster_id)
    //mod:e
    {
      int ret = OB_SUCCESS;
      if (sample_num <=0 || sample_num > ObTabletHistogram::MAX_SAMPLE_BUCKET || NULL == server_index)
      {
        ret = OB_ERROR;
        TBSYS_LOG(WARN, "wrong parameters, sample_num=%ld, server_index=%p", sample_num, server_index);
      }
      else if (sorted_srw_list_.size() <= cur_srw_index_ || 0 > cur_srw_index_)
      {
        ret = OB_ARRAY_OUT_OF_RANGE;
        TBSYS_LOG(WARN, "index out of range, cur_srw_index_=%d, sorted_srw_list_.size()=%d", cur_srw_index_, sorted_srw_list_.size());
      }

      if (OB_SUCCESS == ret)
      {
        //int64_t interval = static_cast<int64_t>(sorted_srw_list_.size())/sample_num;
        int64_t interval = sample_num;//mod liumz, [bugfix]20161028 //TODO lmz
        //add liumz, bugfix: validate interval, 20150507:b
        interval = interval <= 0 ? 1 : interval;
        //add:e
        int32_t begin_index = cur_srw_index_;
        tablet_info.range_.border_flag_.unset_inclusive_start();
        if (0 == cur_srw_index_)
        {
          tablet_info.range_.start_key_ = common::ObRowkey::MIN_ROWKEY;
        }
        else if (NULL != sorted_srw_list_.at(cur_srw_index_))
        {
          tablet_info.range_.start_key_ = sorted_srw_list_.at(cur_srw_index_)->rowkey_;
        }
        else
        {
          ret = OB_ERROR;
          TBSYS_LOG(WARN, "null srw, cur_srw_index_=%d", cur_srw_index_);
        }

        //add liumz, bugfix: validate ret, 20150507:b
        if (OB_SUCCESS == ret)
        {
        //add:e
          cur_srw_index_ += static_cast<int32_t>(interval);
          cur_srw_index_ = (cur_srw_index_ == (sorted_srw_list_.size() - 1)) ? sorted_srw_list_.size() : cur_srw_index_;
          if (cur_srw_index_ < sorted_srw_list_.size())
          {
            if (NULL != sorted_srw_list_.at(cur_srw_index_))
            {
              tablet_info.range_.end_key_ = sorted_srw_list_.at(cur_srw_index_)->rowkey_;
              tablet_info.range_.border_flag_.set_inclusive_end();
            }
            else
            {
              ret = OB_ERROR;
              TBSYS_LOG(WARN, "null srw, cur_srw_index_=%d", cur_srw_index_);
            }
          }
          else
          {
            tablet_info.range_.end_key_ = common::ObRowkey::MAX_ROWKEY;
            tablet_info.range_.border_flag_.unset_inclusive_end();
          }
        }

        //add liumz, bugfix: validate ret, 20150507:b
        if (OB_SUCCESS == ret)
        {
        //add:e
          tablet_info.range_.table_id_ = get_index_tid();
          int32_t end_index = cur_srw_index_ < sorted_srw_list_.size() ? cur_srw_index_ : sorted_srw_list_.size();
          TBSYS_LOG(INFO, "test::liumz, begin_index=%d, end_index=%d, sorted_srw_list_.size()=%d", begin_index, end_index, sorted_srw_list_.size());
          //mod liumz, [paxos static index]20170626:b
          //ret = fill_server_index(server_index, copy_count, begin_index, end_index);
          ret = fill_server_index(server_index, copy_count, begin_index, end_index, cluster_id);
          //mod:e
        }
      }
      return ret;
    }

    //mod liumz, [paxos static index]20170626:b
    //int ObTabletHistogramTable::fill_server_index(int32_t *server_index, const int32_t copy_count, const int32_t begin_index, const int32_t end_index) const
    int ObTabletHistogramTable::fill_server_index(int32_t *server_index, const int32_t copy_count, const int32_t begin_index, const int32_t end_index, const int32_t cluster_id) const
    //mod:e
    {
      int ret = OB_SUCCESS;
      if (NULL == server_index || begin_index > end_index || begin_index < 0 || end_index > sorted_srw_list_.size())
      {
        ret = OB_ERROR;
        TBSYS_LOG(WARN, "wrong parameters, begin_index=%d, end_index=%d, server_index=%p.", begin_index, end_index, server_index);
      }
      if (OB_SUCCESS == ret)
      {
        int32_t server_index_idx = 0;
        for (int32_t i = begin_index; i < end_index && server_index_idx < copy_count && OB_LIKELY(OB_SUCCESS == ret); i++)
        {
          if (NULL == sorted_srw_list_.at(i))
          {
            ret = OB_ERROR;
            TBSYS_LOG(WARN, "null srw, index=%d", i);
          }
          else if (NULL == sorted_srw_list_.at(i)->hist_meta_)
          {
            ret = OB_ERROR;
            TBSYS_LOG(WARN, "srw's hist_meta_ is null, srw index=%d", i);
          }
          else
          {
            int32_t server_idx = OB_INVALID_INDEX;
            //TODO, any better way to select CS
            for (int32_t idx = 0; idx < common::OB_MAX_COPY_COUNT && server_index_idx < copy_count; idx++)
            {
              //if (OB_INVALID_INDEX != (server_idx = sorted_srw_list_.at(i)->hist_meta_->server_info_indexes_[idx]))
              if (OB_INVALID_INDEX != (server_idx = sorted_srw_list_.at(i)->hist_meta_->report_cs_info_[idx].server_info_index_))
              {
                //add liumz, bugfix: may add the same server more than once, 20150509:b
                bool exist = false;
                //check if this cs is already added in server_index[]
                for (int32_t found_index = 0; found_index < server_index_idx; found_index++)
                {
                  if (server_index[found_index] == server_idx)
                  {
                    exist = true;
                    break;
                  }
                }
                if (!exist)
                {
                //add:e
                  server_index[server_index_idx++] = server_idx;
                }
              }
            }//end for
          }
        }//end for

        //add liumz, [if index replicas is not enough, fill with data table's server info index]20150723:b
        for (int32_t i = begin_index; i < end_index && server_index_idx < copy_count && OB_LIKELY(OB_SUCCESS == ret); i++)
        {
          if (NULL == root_server_)
          {
            ret = OB_NOT_INIT;
            TBSYS_LOG(WARN, "root_server_ is null.");
          }
          else if (NULL == sorted_srw_list_.at(i))
          {
            ret = OB_ERROR;
            TBSYS_LOG(WARN, "null srw, index=%d", i);
          }
          else if (NULL == sorted_srw_list_.at(i)->hist_meta_)
          {
            ret = OB_ERROR;
            TBSYS_LOG(WARN, "srw's hist_meta_ is null, srw index=%d", i);
          }
          else
          {
            rootserver::ObRootTable2::const_iterator meta_data = NULL;
            if (OB_SUCCESS != (ret = root_server_->get_root_meta(sorted_srw_list_.at(i)->hist_meta_->root_meta_index_, meta_data)))
            {
              TBSYS_LOG(WARN, "get root meta by index[%d] failed, ret=%d", sorted_srw_list_.at(i)->hist_meta_->root_meta_index_, ret);
            }
            else if (NULL == meta_data)
            {
              TBSYS_LOG(WARN, "root meta is NULL.");
            }
            else
            {
              int32_t server_idx = OB_INVALID_INDEX;
              for (int32_t idx = 0; idx < common::OB_MAX_COPY_COUNT && server_index_idx < copy_count; idx++)
              {
                //mod liumz, [paxos static index]20170626:b
                //if (OB_INVALID_INDEX != (server_idx = meta_data->server_info_indexes_[idx]))
                if (OB_INVALID_INDEX != (server_idx = meta_data->server_info_indexes_[idx]) && cluster_id == root_server_->get_server_manager().get_cs(server_idx).cluster_id_)
                //mod:e
                {
                  bool exist = false;
                  //check if this cs is already added in server_index[]
                  for (int32_t found_index = 0; found_index < server_index_idx; found_index++)
                  {
                    if (server_index[found_index] == server_idx)
                    {
                      exist = true;
                      break;
                    }
                  }
                  if (!exist)
                  {
                    server_index[server_index_idx++] = server_idx;
                  }
                }
              }//end for
            }
          }
        }//end for
        //add:e
      }
      return ret;
    }

  }

}
