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
#ifndef OCEANBASE_COMMON_OB_TABLET_HISTOGRAM_META_H
#define OCEANBASE_COMMON_OB_TABLET_HISTOGRAM_META_H

#include "rootserver/ob_root_meta2.h"
#include "rootserver/ob_root_table2.h"
#include "rootserver/ob_root_server2.h"
//#include "rootserver/ob_tablet_info_manager.h"
#include "common/ob_tablet_histogram.h"
#include "common/ob_tablet_info.h"

#ifdef GTEST
#define private public
#define protected public
#endif

namespace oceanbase {
  namespace common{

    //add liumz, bugfix: 汇报一个tablet的CS数超过OB_MAX_COPY_COUNT时, 新的CS汇报信息不会写入hist table, 20150505:b
    struct ReportCSInfo
    {
        int32_t server_info_index_;
        int64_t report_timestamp_;

        ReportCSInfo()
        {
          server_info_index_ = OB_INVALID_INDEX;
          report_timestamp_ = 0;
        }
    };
    //add:e

    struct ObTabletHistogramMeta
    {
        //rootserver::ObRootTable2::const_iterator root_meta_;//use root_meta_'s info except server_info_indexes_
        int32_t root_meta_index_;
        //int32_t server_info_indexes_[common::OB_MAX_COPY_COUNT];//self server_info_indexes_
        ReportCSInfo report_cs_info_[common::OB_MAX_COPY_COUNT];
        int32_t hist_index_;//histogram's index in hist_manager_

        ObTabletHistogramMeta(): root_meta_index_(OB_INVALID_INDEX), hist_index_(OB_INVALID_INDEX)
        {
          /*
          for (int32_t i = 0; i < common::OB_MAX_COPY_COUNT; i++)
          {
            server_info_indexes_[i] = OB_INVALID_INDEX;
          }*/
        }
        ~ObTabletHistogramMeta()
        {
          //root_meta_ = NULL;
        }

        void dump() const;

    };

    class ObTabletHistogramTable
    {
      public:
        typedef ObTabletHistogramMeta* iterator;
        typedef const ObTabletHistogramMeta* const_iterator;

        struct ObTabletSampleRangeWrapper
        {
            common::ObRowkey rowkey_;
            const_iterator hist_meta_;
        };

      public:
        explicit ObTabletHistogramTable(rootserver::ObRootServer2 *rs, ObTabletHistogramManager *hist_manager)
          :tablet_hist_manager_(hist_manager), root_server_(rs),
           table_id_(OB_INVALID_ID), index_tid_(OB_INVALID_ID),
           cur_srw_index_(0), sorted_srw_list_(OB_MAX_HISTOGRAM_SAMPLE_PER_TABLE),
           mod_(ObModIds::OB_STATIC_INDEX_HISTOGRAM), module_arena_(ModuleArena::DEFAULT_PAGE_SIZE, mod_),
           srw_ptr_(NULL)
        {
          meta_table_.init(ObTabletHistogramManager::MAX_TABLET_COUNT_PER_TABLE, data_holder_);
        }

        virtual ~ObTabletHistogramTable()
        {
          root_server_ = NULL;
          tablet_hist_manager_ = NULL;
          reset();
        }

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
        void reset()
        {
          meta_table_.clear();                    
          sorted_srw_list_.clear();
          table_id_ = OB_INVALID_ID;
          index_tid_ = OB_INVALID_ID;
          cur_srw_index_ = 0;
          module_arena_.reuse();
          if (NULL != srw_ptr_)
          {
            ob_free(srw_ptr_);
            srw_ptr_ = NULL;
          }
        }

        bool next_srw()
        {
          return (sorted_srw_list_.size() > cur_srw_index_) && (0 <= cur_srw_index_);
        }

        void dump() const;

        ObTabletSampleRangeWrapper* alloc_srw_object();
        int alloc_srw_object(const int64_t count);

        /*
         * 将hist加入ObTabletHistogramManager中, 得到histogram_index_,
         * 构造ObHistogramMeta, 添加到meta_table_中
         */
        //don't use this func
        //int add_hist_meta(const ObTabletInfo &tablet_info, const ObTabletHistogram &hist, const int32_t server_index);

        /*
         * hist已经加入ObTabletHistogramManager中, 已经得到其在hist_manager的hist_index, 只需要构造ObHistogramMeta, 添加到meta_table_中
         * 因为写hist table时需要加锁, 把add_histogram()和add_hist_meta()分开, 降低锁粒度
         */
        int add_hist_meta(const ObTabletInfo &tablet_info, const int32_t hist_index, const int32_t server_index, const uint64_t idx_tid);
        int check_reported_hist_info(const ObTabletInfo *compare_tablet, bool &is_finished) const;

        /*
         * find tablet's pos in hist_table_
         */
        iterator find_tablet_pos(const ObTabletInfo &tablet_info);
        /*
         * if reported tablet exists, find suitable index in server_info_indexes_ for server_index
         */
        int32_t find_suitable_pos(const_iterator it, const int32_t server_index) const;
        /*
         * insert all reported range's rowkey into sorted list: sorted_srw_list_
         */
        int fill_all_ranges();
        /*
         * split global index's range, get one tablet of global index from sorted_srw_list_ everytime
         */
        //mod liumz, [paxos static index]20170626:b
        //int get_next_tablet_info(const int64_t sample_num, ObTabletInfo &tablet_info, int32_t *server_index, const int32_t copy_count);
        //int fill_server_index(int32_t *server_index, const int32_t copy_count, const int32_t begin_index, const int32_t end_index) const;
        int get_next_tablet_info(const int64_t sample_num, ObTabletInfo &tablet_info, int32_t *server_index, const int32_t copy_count, const int32_t cluster_id);
        int fill_server_index(int32_t *server_index, const int32_t copy_count, const int32_t begin_index, const int32_t end_index, const int32_t cluster_id) const;
        //mod:e

      public:
        inline iterator begin()
        {
          return &(data_holder_[0]);
        }
        inline iterator end()
        {
          return begin() + meta_table_.get_array_index();
        }
        inline const_iterator begin() const
        {
          return &(data_holder_[0]);
        }
        inline const_iterator end() const
        {
          return begin() + meta_table_.get_array_index();
        }

      private:
        int add_meta(const ObTabletHistogramMeta &meta);

      private:
        ObTabletHistogramMeta data_holder_[ObTabletHistogramManager::MAX_TABLET_COUNT_PER_TABLE];
        common::ObArrayHelper<ObTabletHistogramMeta> meta_table_;
        ObTabletHistogramManager* tablet_hist_manager_;
        rootserver::ObRootServer2* root_server_;
        uint64_t table_id_;
        uint64_t index_tid_;
        int32_t cur_srw_index_;
        common::ObSortedVector<ObTabletSampleRangeWrapper *> sorted_srw_list_;
        common::ModulePageAllocator mod_;
        common::ModuleArena module_arena_;
        ObTabletSampleRangeWrapper* srw_ptr_;
    };


  }
}


#endif // OCEANBASE_COMMON_OB_TABLET_HISTOGRAM_META_H
