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
#include "ob_tablet_histogram.h"
#include "ob_tablet_info.h"

namespace oceanbase {
  namespace common {

    void ObTabletSample::dump() const
    {
//      static char row_key_dump_buff[OB_MAX_ROW_KEY_LENGTH * 2];
//      range_.to_string(row_key_dump_buff, OB_MAX_ROW_KEY_LENGTH * 2);
      TBSYS_LOG(INFO, "sample range = %s", to_cstring(range_));
      TBSYS_LOG(INFO, "row_count = %ld", row_count_);
    }

    void ObTabletHistogram::dump() const
    {
      for (int32_t i = 0; i < sample_helper_.get_array_index(); i++)
      {
        TBSYS_LOG(DEBUG, "dump %d-th sample.", i);
        samples_[i].dump();
      }
    }

    int ObTabletHistogram::get_sample(const int64_t sample_index, ObTabletSample *&return_sample)
    {
      int ret = OB_SUCCESS;
      if (sample_index < 0 || sample_index >= sample_helper_.get_array_index())
      {
        TBSYS_LOG(WARN, "array index out of range.");
        ret = OB_ARRAY_OUT_OF_RANGE;
      }
      else {
        return_sample = samples_ + sample_index;
      }
      return ret;
    }

    int ObTabletHistogram::get_sample(const int64_t sample_index, const ObTabletSample *&return_sample) const
    {
      int ret = OB_SUCCESS;
      if (sample_index < 0 || sample_index >= sample_helper_.get_array_index())
      {
        TBSYS_LOG(WARN, "array index out of range.");
        ret = OB_ARRAY_OUT_OF_RANGE;
      }
      else {
        return_sample = samples_ + sample_index;
      }
      return ret;
    }

    /*
     * 浅拷贝
     */
    int ObTabletHistogram::add_sample(const ObTabletSample &index_sample)
    {
      int ret = OB_SUCCESS;
      if (sample_helper_.get_array_index() >= MAX_SAMPLE_BUCKET)
      {
        ret = OB_ARRAY_OUT_OF_RANGE;
        TBSYS_LOG(WARN, "too many sample in one histogram, max is %ld, now is %ld.", MAX_SAMPLE_BUCKET, sample_helper_.get_array_index());
      }
      else if (!sample_helper_.push_back(index_sample)) {
        ret = OB_ARRAY_OUT_OF_RANGE;
        TBSYS_LOG(WARN, "can not push index_sample into sample_list_");
      }
      return ret;
    }

    /*
     * deep copy add sample with internal allocator_
     */
    int ObTabletHistogram::add_sample_with_deep_copy(const ObTabletSample &index_sample)
    {
      int ret = OB_SUCCESS;
      ObTabletSample inner_sample;

      if (NULL == allocator_)
      {
        ret = OB_NOT_INIT;
        TBSYS_LOG(WARN, "allocator_ is null.");
      }
      else if (sample_helper_.get_array_index() >= MAX_SAMPLE_BUCKET){
        ret = OB_ARRAY_OUT_OF_RANGE;
        TBSYS_LOG(WARN, "too many sample in one histogram, max is %ld, now is %ld.", MAX_SAMPLE_BUCKET, sample_helper_.get_array_index());
      }
      else if(OB_SUCCESS != (ret = inner_sample.deep_copy(*allocator_, index_sample)))
      {
        TBSYS_LOG(WARN, "deep copy index sample failed!");
      }
      else if (!sample_helper_.push_back(inner_sample)) {
        ret = OB_ARRAY_OUT_OF_RANGE;
        TBSYS_LOG(WARN, "can not push index_sample into sample_list_");
      }
      return ret;
    }

    /*
     * deep copy with internal allocator_
     */
    int ObTabletHistogram::deep_copy(const ObTabletHistogram &other)
    {
      int ret = OB_SUCCESS;
      this->server_info_index_ = other.server_info_index_;
      int64_t size = other.get_sample_count();
      for (int64_t i = 0; i < size; i++)
      {
        const ObTabletSample *sample = NULL;
        if (OB_SUCCESS != (ret = other.get_sample(i, sample)))
        {
          TBSYS_LOG(WARN, "get sample failed");
          break;
        }
        //add liumz, check if sample is null, 20150428:b
        else if (NULL == sample)
        {
          ret = OB_ERROR;
          TBSYS_LOG(WARN, "sample is NULL");
          break;
        }
        //add:e
        else if (OB_SUCCESS != (ret = add_sample_with_deep_copy(*sample))) {
          TBSYS_LOG(WARN, "add sample failed");
          break;
        }
      }

      return ret;
    }

    /*
     * deep copy with specific allocator
     */
    template <typename Allocator>
    int ObTabletHistogram::deep_copy(Allocator &allocator, const ObTabletHistogram &other)
    {
      int ret = OB_SUCCESS;
      this->server_info_index_ = other.server_info_index_;
      int64_t size = other.get_sample_count();
      for (int64_t i = 0; i < size; i++)
      {
        const ObTabletSample *sample = NULL;
        if (OB_SUCCESS != (ret = other.get_sample(i, sample)))
        {
          TBSYS_LOG(WARN, "get sample failed");
          break;
        }
        //add liumz, check if sample is null, 20150428:b
        else if (NULL == sample)
        {
          ret = OB_ERROR;
          TBSYS_LOG(WARN, "sample is NULL");
          break;
        }
        //add:e
        else if (OB_SUCCESS != (ret = add_sample_with_deep_copy(allocator, *sample))) {
          TBSYS_LOG(WARN, "add sample failed");
          break;
        }
      }

      return ret;
    }

    void ObTabletHistogramManager::reuse()
    {
      tbsys::CThreadGuard mutex_gard(&alloc_mutex_);
      module_arena_.reuse();
      histogram_helper_.clear();
      /*
      sp_array_.clear();
      sp_sorted_list_.clear();
      cur_wrapper_ = sp_sorted_list_.begin();
      */
      table_id_ = OB_INVALID_ID;
      index_tid_ = OB_INVALID_ID;
    }

    /*
    bool compare_sample(const ObTabletSampleWrapper *lhs, const ObTabletSampleWrapper *rhs)
    {
      bool cmp = false;
      if (lhs->border_ == LEFT && rhs->border_ == LEFT)
      {
        cmp =lhs->sp_->get_range().compare_with_startkey(rhs->sp_->get_range()) < 0;
      }
      else if (lhs->border_ == RIGHT && rhs->border_ == RIGHT) {
        cmp = lhs->sp_->get_range().compare_with_endkey(rhs->sp_->get_range()) < 0;
      }
      else {
        ObNewRange tmp_range;
        if (lhs->border_ == LEFT)
        {
          tmp_range.end_key_ = lhs->sp_->get_range().start_key_;
          cmp = tmp_range.compare_with_endkey(rhs->sp_->get_range());
        }
        else {
          tmp_range.end_key_ = rhs->sp_->get_range().start_key_;
          cmp = lhs->sp_->get_range().compare_with_endkey(tmp_range);
        }
      }
      return cmp;
    }

    bool unique_sample(const ObTabletSampleWrapper *lhs, const ObTabletSampleWrapper *rhs)
    {
      UNUSED(lhs);
      UNUSED(rhs);
      return false;
    }

    int ObTabletHistogramManager::fill_all_samples()
    {
      int ret = OB_SUCCESS;
      ObTabletHistogram* histogram = NULL;

      for (int64_t hist_index = 0; hist_index < histogram_helper_.get_array_index() && OB_LIKELY(OB_SUCCESS == ret); hist_index++)
      {
        if (sp_array_.count() >= MAX_SAMPLE_COUNT_PER_TABLE)
        {
          ret = OB_ARRAY_OUT_OF_RANGE;
          TBSYS_LOG(WARN, "too many samples in one table, fill all samples failed.");
        }
        else
        {
          histogram = histograms_[hist_index];
          for(int64_t i = 0; i < histogram->get_sample_count() && OB_LIKELY(OB_SUCCESS == ret); i++)
          {
            ObTabletSample *sp = NULL;
            ObTabletSampleWrapper left_sample(NULL, histogram, LEFT);
            ObTabletSampleWrapper right_sample(NULL, histogram, RIGHT);

            if (OB_SUCCESS != (ret = histogram->get_sample(i, sp)))
            {
              TBSYS_LOG(WARN, "get sample from histogram failed. sample index = %ld", i);
            }
            else
            {
              left_sample.sp_ = sp;
              right_sample.sp_ = sp;
              int64_t count = sp_array_.count();
              if (count + 2 >= MAX_SAMPLE_COUNT_PER_TABLE)
              {
                ret = OB_ARRAY_OUT_OF_RANGE;
                TBSYS_LOG(WARN, "too many samples in one table, fill all samples failed.");
              }
              else if (OB_SUCCESS == (ret = sp_array_.push_back(left_sample)) && OB_SUCCESS == (ret = sp_array_.push_back(right_sample)))
              {
                  ObTabletSampleWrapper &lsp = sp_array_.at(count);
                  ObTabletSampleWrapper &rsp = sp_array_.at(count + 1);
                  ObSortedVector<ObTabletSampleWrapper *>::iterator it = sp_sorted_list_.end();
                  //从前向后查找第一个不小于left_sample的位置
                  if (OB_SUCCESS != (ret = sp_sorted_list_.insert_unique(&lsp, it, compare_sample, unique_sample)))
                  {
                    TBSYS_LOG(WARN, "can not insert left_sample into sorted sample list");
                  }
                  else if (OB_SUCCESS != (ret = sp_sorted_list_.insert_unique(&rsp, it, compare_sample, unique_sample))) {
                    TBSYS_LOG(WARN, "can not insert right_sample into sorted sample list");
                  }
              }
              else
              {
                ret = OB_ARRAY_OUT_OF_RANGE;
                TBSYS_LOG(WARN, "too many samples in one table, fill all samples failed.");
              }
            }
          }//end for
        }
      }//end for

      return ret;
    }
    */

    int ObTabletHistogramManager::get_histogram(const int32_t hist_index, ObTabletHistogram *&out_hist) const
    {
      int ret = OB_SUCCESS;
      //add liumz, 20150429:b
      out_hist = NULL;
      //add:e
      if (hist_index < 0 || hist_index >= histogram_helper_.get_array_index())
      {
        ret = OB_ARRAY_OUT_OF_RANGE;
        TBSYS_LOG(WARN, "index out of range, index=%d, histogram_helper_.size()=%ld", hist_index, histogram_helper_.get_array_index());
      }
      //mod liumz, 20150429:b
      //if (OB_SUCCESS == ret)
      else
      //mod:e
      {
        out_hist = histograms_[hist_index];
      }
      return ret;
    }

    int ObTabletHistogramManager::add_histogram(const ObTabletHistogram &histogram, int32_t &out_index, uint64_t &idx_tid)
    {
//      tbsys::CThreadGuard mutex_gard(&alloc_mutex_);
      int ret = OB_SUCCESS;
      out_index = OB_INVALID_INDEX;
      idx_tid = OB_INVALID_ID;
      const ObTabletSample *first_sample = NULL;
      //20150409, histogram must contains at least one sample
      if (histogram.get_sample_count() < 1)
      {
        ret = OB_ERROR;
        TBSYS_LOG(WARN, "added histogram is null.");
      }
      //20150512, tid in histogram must be consistent with index_tid_.
      else if (OB_SUCCESS != (ret = histogram.get_sample(0, first_sample)))
      {
        TBSYS_LOG(WARN, "get first sample failed.");
      }
      else if (NULL == first_sample)
      {
        ret = OB_ERROR;
        TBSYS_LOG(WARN, "first sample is NULL.");
      }
      else if (index_tid_ != (idx_tid = first_sample->range_.table_id_))
      {
        ret = OB_INVALID_DATA;
        TBSYS_LOG(WARN, "index tid is not consistent. index_tid_[%lu], sample's range tid[%lu]"
                  , index_tid_, first_sample->range_.table_id_);
      }
      else if (histogram_helper_.get_array_index() >= MAX_TABLET_COUNT_PER_TABLE)
      {
        ret = OB_ARRAY_OUT_OF_RANGE;
        TBSYS_LOG(WARN, "too many histogram in hist manager, max is %ld, now is %ld.", MAX_TABLET_COUNT_PER_TABLE, histogram_helper_.get_array_index());
      }
      else
      {
        ObTabletHistogram* inner_hist = alloc_hist_object();
//        ObTabletHistogram* inner_hist = alloc_hist_object();
//        inner_hist->set_allocator(module_arena_);
//        if (OB_SUCCESS != (ret = inner_hist->deep_copy(histogram)))
        if (NULL == inner_hist)
        {
          ret = OB_ERROR ;
          TBSYS_LOG(WARN, "alloc hist object failed.");
        }
        else if (OB_SUCCESS != (ret = inner_hist->deep_copy(module_arena_, histogram)))
        {
          ret = OB_ARRAY_OUT_OF_RANGE;
          TBSYS_LOG(WARN, "deep copy histogram with specific allocator failed.");
        }
        else if (!histogram_helper_.push_back(inner_hist))
        {
          ret = OB_ARRAY_OUT_OF_RANGE;
          TBSYS_LOG(WARN, "failed to push hist into hist manager.");
        }
      }
      if (OB_SUCCESS == ret)
      {
        out_index = static_cast<int32_t>(histogram_helper_.get_array_index() - 1);
      }
      return ret;
    }

    ObTabletHistogram* ObTabletHistogramManager::alloc_hist_object()
    {
      tbsys::CThreadGuard mutex_gard(&alloc_mutex_);
      ObTabletHistogram* hist = NULL;
      char* ptr = module_arena_.alloc_aligned(sizeof(ObTabletHistogram));
      if (NULL != ptr)
      {
        hist = new (ptr) ObTabletHistogram();
      }
      return hist;
    }

    /*
    //add liumz, 20150323:b
    int ObTabletHistogramManager::get_one_tablet_info(const int64_t sample_num, ObTabletInfo &tablet_info, int32_t *server_index, const int32_t copy_count)
    {
      int ret = OB_SUCCESS;
      if (NULL == cur_wrapper_)
      {
        cur_wrapper_ = sp_sorted_list_.begin();
      }
//      ObTabletSampleWrapper *sw = NULL;
//      TBSYS_LOG(ERROR, "test::liumz, sp_sorted_list_.size()=%d", sp_sorted_list_.size());
//      for (int32_t i = 0; i < sp_sorted_list_.size(); i++)
//      {
//        TBSYS_LOG(ERROR, "test::liumz, dump %d-th sample wrapper of sp_sorted_list_", i);
//        if (NULL != (sw = sp_sorted_list_.at(i)))
//        {
//          sw->sp_->dump();
//        }
//      }
      TBSYS_LOG(ERROR, "test::liumz, get_one_tablet_info cur_wrapper_=%p", cur_wrapper_);

      if (NULL == cur_wrapper_)
      {
        ret = OB_ERROR;//important
        TBSYS_LOG(WARN, "sp_sorted_list_ is empty");
      }
      else
      {
        int64_t interval = static_cast<int64_t>(sp_sorted_list_.size())/sample_num;
        const_iterator start = cur_wrapper_;
        if (cur_wrapper_ < end() - 1)//cur_wrapper_ used last time
        {
          tablet_info.range_ = (*cur_wrapper_)->sp_->range_;
          //set range_'s left border flag
          tablet_info.range_.border_flag_.unset_inclusive_start();
          if (sp_sorted_list_.begin() == cur_wrapper_)//构造(MIN_ROWKEY, XXX]
          {
            tablet_info.range_.start_key_ = common::ObRowkey::MIN_ROWKEY;
          }

          //move cur_wrapper_ forward
          if (cur_wrapper_ + interval < end())
          {
            cur_wrapper_ += interval;
          }
          else
          {
            cur_wrapper_ = end() - 1;
          }

          if (sp_sorted_list_.end() - 1 == cur_wrapper_)//构造(XXX, MAX_ROWKEY]
          {
            tablet_info.range_.end_key_ = common::ObRowkey::MAX_ROWKEY;
            tablet_info.range_.border_flag_.unset_inclusive_end();
          }
          else
          {
            tablet_info.range_.end_key_ = (*cur_wrapper_)->sp_->range_.end_key_;
            tablet_info.range_.border_flag_.set_inclusive_end();
          }

          //modify tid to index_tid_
          tablet_info.range_.table_id_ = index_tid_;

          for(int32_t i = 0; i < copy_count && start <= cur_wrapper_; start++)
          {
            server_index[i] = (*start)->hp_->get_server_index();
            bool same_server_index = false;
            for (int32_t j = 0; j < i; j++)
            {
              if (server_index[i] == server_index[j])
              {
                same_server_index = true;
                server_index[i] = OB_INVALID_INDEX;
                break;
              }
            }
            if (!same_server_index)
            {
              i++;
            }
          }
        }
        else
        {
          ret = OB_ITER_END;
        }
      }
      return ret;
    }
    //add:e
    */

    void ObTabletHistogramManager::dump(const int32_t hist_index) const
    {
      if (hist_index >=0 && hist_index < histogram_helper_.get_array_index())
      {
        histograms_[hist_index]->dump();
      }
    }

    DEFINE_SERIALIZE(ObTabletSample)
    {
      int ret = OB_SUCCESS;
      ret = serialization::encode_vi64(buf, buf_len, pos, row_count_);

      if (ret == OB_SUCCESS)
        ret = range_.serialize(buf, buf_len, pos);

      return ret;
    }


    DEFINE_DESERIALIZE(ObTabletSample)
    {
      int ret = OB_SUCCESS;
      ret = serialization::decode_vi64(buf, data_len, pos, &row_count_);

      if (OB_SUCCESS == ret)
      {
        ret = range_.deserialize(buf, data_len, pos);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "fail to deserialize range, ret=%d, buf=%p, data_len=%ld, pos=%ld",
              ret, buf, data_len, pos);
        }
      }

      return ret;
    }

    DEFINE_GET_SERIALIZE_SIZE(ObTabletSample)
    {
      int64_t total_size = 0;

      total_size += serialization::encoded_length_vi64(row_count_);
      total_size += range_.get_serialize_size();

      return total_size;
    }

    DEFINE_SERIALIZE(ObTabletHistogram)
    {
      int ret = OB_SUCCESS;
      int64_t size = sample_helper_.get_array_index();

      ret = serialization::encode_vi32(buf, buf_len, pos, server_info_index_);
      if (ret == OB_SUCCESS)
      {
        ret = serialization::encode_vi64(buf, buf_len, pos, size);
      }

      if (ret == OB_SUCCESS)
      {
        for (int64_t i = 0; i < size; ++i)
        {
          ret = samples_[i].serialize(buf, buf_len, pos);
          if (ret != OB_SUCCESS)
            break;
        }
      }

      return ret;
    }

    DEFINE_DESERIALIZE(ObTabletHistogram)
    {
      int ret = OB_SUCCESS;
      int64_t size = 0;
      ObObj* ptr = NULL;

      if (NULL == allocator_)
      {
        ret = OB_NOT_INIT;
        TBSYS_LOG(WARN, "allocator_ not set, deseriablize failed.");
      }
      //mod liumz, bugfix: add else branch, 20150428:b
      else
      {
        ret = serialization::decode_vi32(buf, data_len, pos, &server_info_index_);
        if (ret == OB_SUCCESS)
        {
          ret = serialization::decode_vi64(buf, data_len, pos, &size);
        }
      }
      //mod:e
      if (ret == OB_SUCCESS && size > 0)
      {
        for (int64_t i=0; i<size; ++i)
        {
          ObTabletSample sample;
          ptr = reinterpret_cast<ObObj*>(allocator_->alloc(sizeof(ObObj) * OB_MAX_ROWKEY_COLUMN_NUMBER * 2));
          sample.range_.start_key_.assign(ptr, OB_MAX_ROWKEY_COLUMN_NUMBER);
          sample.range_.end_key_.assign(ptr + OB_MAX_ROWKEY_COLUMN_NUMBER, OB_MAX_ROWKEY_COLUMN_NUMBER);
          ret = sample.deserialize(buf, data_len, pos);
          if (OB_SUCCESS != ret)
          {
            TBSYS_LOG(WARN, "fail to deserialize ObStaticIndexSample.");
            break;
          }
          else if (OB_SUCCESS != (ret = add_sample(sample)))//浅拷贝
          {
            TBSYS_LOG(WARN, "fail to add sample into histogram.");
            break;
          }
        }
      }

      return ret;
    }

    DEFINE_GET_SERIALIZE_SIZE(ObTabletHistogram)
    {
      int64_t total_size = 0;

      total_size += serialization::encoded_length_vi32(server_info_index_);

      int64_t size = sample_helper_.get_array_index();
      total_size += serialization::encoded_length_vi64(size);

      if (size > 0)
      {
        for (int64_t i=0; i<size; ++i)
          total_size += samples_[i].get_serialize_size();
      }

      return total_size;
    }
  }

}
