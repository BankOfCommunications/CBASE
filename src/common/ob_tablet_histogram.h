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
#ifndef OCEANBASE_COMMON_OB_TABLET_HISTOGRAM_H_
#define OCEANBASE_COMMON_OB_TABLET_HISTOGRAM_H_

#include <tblog.h>
#include "ob_define.h"
#include "ob_server.h"
#include "ob_array_helper.h"
#include "page_arena.h"
#include "ob_range2.h"
#include "ob_vector.h"
#include "ob_se_array.h"

namespace oceanbase {
  namespace common {

    class ObTabletInfo;

    enum SampleBorder{
      BOTH,
      LEFT,
      RIGHT,
    };

    struct ObTabletSample
    {
        ObNewRange range_;
        int64_t row_count_;

        ObTabletSample():row_count_(0)
        {
        }
        inline void set_range(const ObNewRange &range)
        {
          range_ = range;
        }
        inline const ObNewRange& get_range(void) const
        {
          return range_;
        }
        inline void set_row_count(const int64_t row_count)
        {
          row_count_ = row_count;
        }
        inline int64_t get_row_count(void) const
        {
          return row_count_;
        }

        void dump() const;

        template <typename Allocator>
        int deep_copy(Allocator &allocator, const ObTabletSample &other);
        NEED_SERIALIZE_AND_DESERIALIZE;
    };

    template <typename Allocator>
    inline int ObTabletSample::deep_copy(Allocator &allocator, const ObTabletSample &other)
    {
      int ret = OB_SUCCESS;
      this->row_count_ = other.row_count_;

      ret = deep_copy_range(allocator, other.range_, this->range_);
      return ret;
    }


    /*
     * 存储一个tablet的直方图信息
     */
    class ObTabletHistogram
    {
      public:
        ObTabletHistogram()
          :allocator_(NULL), server_info_index_(OB_INVALID_INDEX)
        {
          init();
        }
        ObTabletHistogram(common::ModuleArena *allocator)
          :allocator_(allocator), server_info_index_(OB_INVALID_INDEX)
        {
          init();
        }
        ~ ObTabletHistogram()
        {
          allocator_ = NULL;
          server_info_index_ = OB_INVALID_INDEX;
        }

        void reset()
        {
          sample_helper_.clear();
        }

        void set_allocator(common::ModuleArena *allocator){
          allocator_ = allocator;
        }

        void init()
        {
          sample_helper_.init(MAX_SAMPLE_BUCKET, samples_);
        }

        int64_t get_sample_count() const
        {
          return sample_helper_.get_array_index();
        }

        void set_server_index(const int32_t server_index)
        {
          server_info_index_ = server_index;
        }
        int32_t get_server_index() const
        {
          return server_info_index_;
        }

        void dump() const;

        int get_sample(const int64_t sample_index, ObTabletSample *&return_sample);
        int get_sample(const int64_t sample_index, const ObTabletSample *&return_sample) const;
        int add_sample(const ObTabletSample &index_sample);

        int add_sample_with_deep_copy(const ObTabletSample &index_sample);
        template <typename Allocator>
        int add_sample_with_deep_copy(Allocator &allocator, const ObTabletSample &index_sample);

        int deep_copy(const ObTabletHistogram &other);
        template <typename Allocator>
        int deep_copy(Allocator &allocator, const ObTabletHistogram &other);

        NEED_SERIALIZE_AND_DESERIALIZE;//反序列化时必须给allocator_赋值!!!

      public:
        static const int64_t MAX_SAMPLE_BUCKET = 256;

      private:
        common::ModuleArena *allocator_;//add_sample_with_deep_copy或者反序列化时深拷贝用的内存分配器
        ObTabletSample samples_[MAX_SAMPLE_BUCKET];
        common::ObArrayHelper<ObTabletSample> sample_helper_;
        int32_t server_info_index_;//CS index, maybe deleted later

    };

    /*
     * deep copy add sample with specific allocator
     */
    template <typename Allocator>
    int ObTabletHistogram::add_sample_with_deep_copy(Allocator &allocator, const ObTabletSample &index_sample)
    {
      int ret = OB_SUCCESS;
      ObTabletSample inner_sample;

      if (sample_helper_.get_array_index() >= MAX_SAMPLE_BUCKET){
        ret = OB_ARRAY_OUT_OF_RANGE;
        TBSYS_LOG(WARN, "too many sample in one histogram, max is %ld, now is %ld.", MAX_SAMPLE_BUCKET, sample_helper_.get_array_index());
      }
      else if(OB_SUCCESS != (ret = inner_sample.deep_copy(allocator, index_sample)))
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
    struct ObTabletSampleWrapper
    {
        const ObTabletSample *sp_;
        const ObTabletHistogram *hp_;
        SampleBorder border_;

        ObTabletSampleWrapper():sp_(NULL), hp_(NULL), border_(BOTH)
        {}
        ObTabletSampleWrapper(ObTabletSample *sp, ObTabletHistogram *hp, SampleBorder border)
          :sp_(sp), hp_(hp), border_(border)
        {}
    };
    */

    /*
     * 存储一个table所有CS上的tablet的直方图信息
     */
    class ObTabletHistogramManager
    {
      /*
      public:
        typedef ObSortedVector<ObTabletSampleWrapper *>::const_iterator const_iterator;
      */
      public:
        ObTabletHistogramManager();
        ~ObTabletHistogramManager()
        {
          reuse();
        }

        ObTabletHistogram* alloc_hist_object();
        int add_histogram(const ObTabletHistogram &histogram, int32_t &out_index, uint64_t &idx_tid);
        int get_histogram(const int32_t hist_index, ObTabletHistogram *&out_hist) const;
        int64_t get_histogram_size() {return histogram_helper_.get_array_index();}
        /*
        int fill_all_samples();
        int get_one_tablet_info(const int64_t sample_num, common::ObTabletInfo &tablet_info, int32_t *server_index, const int32_t copy_count);
        */

        void dump(const int32_t hist_index) const;

        int set_table_id(const uint64_t tid, const uint64_t index_tid)
        {
          int ret = OB_SUCCESS;
          if (OB_INVALID_ID == tid || OB_INVALID_ID == index_tid)
          {
            ret = OB_INVALID_ARGUMENT;
            TBSYS_LOG(WARN, "invalid tid, tid=%lu, index_tid=%lu", tid, index_tid);
          }
          else
          {
            table_id_ = tid;
            index_tid_ = index_tid;
          }
          return ret;
        }
        uint64_t get_table_id() const
        {
          return table_id_;
        }
        uint64_t get_index_tid() const
        {
          return index_tid_;
        }
        void reuse();

      /*
      public:
        inline const_iterator begin()
        {
          return sp_sorted_list_.begin();
        }
        inline const_iterator end()
        {
          return sp_sorted_list_.end();
        }
        inline const_iterator next()
        {
          return cur_wrapper_;
        }
      */

      public:
        static const int64_t MAX_TABLET_COUNT_PER_TABLE = OB_MAX_TABLET_NUMBER_PER_TABLE;
        //static const int64_t MAX_SAMPLE_COUNT_PER_TABLE = 2 * 256 * 4 * 1024;
        common::ModulePageAllocator mod_;
        common::ModuleArena module_arena_;

      private:
        ObTabletHistogram* histograms_[MAX_TABLET_COUNT_PER_TABLE];
        common::ObArrayHelper<ObTabletHistogram*> histogram_helper_;

        /*
        common::ObSEArray<ObTabletSampleWrapper, MAX_SAMPLE_COUNT_PER_TABLE> sp_array_;
        ObSortedVector<ObTabletSampleWrapper *> sp_sorted_list_;//sorted sp_array_
        const_iterator cur_wrapper_;
        */

        uint64_t table_id_;
        uint64_t index_tid_;//add_histogram前检查index_tid_是否一致
        mutable tbsys::CThreadMutex alloc_mutex_;//线程锁
    };

    inline ObTabletHistogramManager::ObTabletHistogramManager()
      :mod_(ObModIds::OB_STATIC_INDEX_HISTOGRAM),
      module_arena_(ModuleArena::DEFAULT_PAGE_SIZE, mod_),
      table_id_(OB_INVALID_ID), index_tid_(OB_INVALID_ID)
    {
      histogram_helper_.init(MAX_TABLET_COUNT_PER_TABLE, histograms_);
      /*
      cur_wrapper_ = sp_sorted_list_.begin();
      TBSYS_LOG(ERROR, "test::liumz, init cur_wrapper_=%p", cur_wrapper_);
      */
    }

  }
}

#endif // OCEANBASE_COMMON_OB_TABLET_HISTOGRAM_H_
