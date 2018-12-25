/*add by wenghaixing [secondary index static_index_build] 20150228
 *
 * */
#ifndef OB_CHUNK_INDEX_WORKER_H
#define OB_CHUNK_INDEX_WORKER_H
#include <tbsys.h>
#include <Mutex.h>
#include <Monitor.h>
#include "common/ob_define.h"
#include "common/ob_schema.h"
#include "common/ob_vector.h"
#include "common/thread_buffer.h"
#include "common/ob_rowkey.h"
#include "common/ob_range2.h"
#include "common/location/ob_tablet_location_list.h"
#include "common/ob_schema_manager.h"
//add zhuyanchao[secondary index static_data_build]20130326
#include "common/ob_mod_define.h"
#include "common/ob_tablet_histogram_report_info.h"
#include "common/ob_tablet_histogram.h"
//add e
//add wenghaixing [secondary index static_data_build.exceptional_handle]20150403
#include "common/ob_black_list.h"
//add e
namespace oceanbase
{
  namespace chunkserver
  {
    class ObTabletManager;
    class ObChunkServer;
    class ObIndexBuilder;
    class ObTablet;

    struct FailRecord
    {
      int8_t if_process_;
      int8_t fail_count_;
    };

    struct TabletRecord
    {
      ObTablet* tablet_;
      int8_t fail_count_;
      int8_t if_process_;// -1 means finish success or whipping success, 0 means unprocessed, 1 means processing or whipping failed finally!
      int8_t wok_send_;
      TabletRecord()
      {
        tablet_ = NULL;
        fail_count_ = 0;
        if_process_ = 0;
        wok_send_ = 0;

      }
    };

    struct RangeRecord
    {
      ObNewRange range_;
      int64_t fail_count_;
      int8_t if_process_;
      int8_t wok_send_;
      RangeRecord()
      {
        fail_count_ = 0;
        if_process_ = 0;
        wok_send_ = 0;
      }
    };

    enum ErrNo
    {
      LOCAL_INDEX_SST_BUILD_FAILED = 0,
      LOCAL_INDEX_SST_NOT_FOUND,
      GLOBAL_INDEX_BUILD_FAILED,
    };

    enum ReportStat
    {
      REPORT_FALSE = 0,
      REPORT_FAILED,
      REPORT_SUCCESS,
    };

    class ObIndexWorker : public tbsys::CDefaultRunnable
    {
      public:
        ObIndexWorker();
        ~ObIndexWorker(){}

        int init(ObTabletManager *manager);
        int schedule();
        int start_round();
        int finish_round();
        void destroy();
        int set_config_param();
        int create_work_thread(const int64_t max_work_thread_num);

        int fetch_tablet_info(bool is_local_index = true, bool need_other_cs = false);
        int prepare_build_total_index();
        //int is_need_static_index_tablet(ObTablet* tablet,bool &is_need_index);
        int is_tablet_need_build_static_index(ObTablet* tablet, ObTabletLocationList &list, bool &is_need_index);
        void construct_tablet_item(const uint64_t table_id,
                                   const common::ObRowkey &start_key, const common::ObRowkey &end_rowkey, common::ObNewRange &range,
                                   ObTabletLocationList &list);

        bool can_launch_next_round();
        int parse_location_from_scanner(ObScanner &scanner, ObRowkey &row_key, uint64_t table_id, bool need_other_cs = false);

        //add wenghaixing [secondary index static_sstable_build.report]20150316
        int add_tablet_info(ObTabletReportInfo* tablet);
        int send_local_index_info();
        //add e
        //add wenghaixing[secondary index static_index_build]20150321
        int check_self();
        int push_wok(BlackList &list);
        void reset();
        int is_tablet_handle(ObTablet* tablet, bool &is_handle);
        //add e
        //add wenghaixing[secondary index static_index_build.fix]20150728
        void try_stop_mission(uint64_t index_tid);
        //add e
        inline bool is_work_stoped() const
        {
          return 0 == active_thread_num_ || OB_INVALID_ID == process_idx_tid_;
        }

        inline ObTabletManager* get_tablet_manager()
        {
          return tablet_manager_;
        }

        inline bool get_round_end()
        {
          return round_end_ == TABLET_RELEASE;
        }

        inline int set_schedule_idx_tid(uint64_t index_tid)
        {
          int ret = OB_SUCCESS;
          if(schedule_idx_tid_ != index_tid)
          {
            schedule_idx_tid_ = index_tid;
          }
          else
          {
            ret = OB_ALREADY_DONE;
          }
          return ret;
        }

        inline int set_schedule_time(uint64_t tid, int64_t start_time)
        {
          int ret = OB_SUCCESS;
          if (schedule_idx_tid_ == tid && OB_INVALID_ID != tid && 0 != start_time)
          {
            if(schedule_time_1_ == 0 && schedule_time_2_ == 0)
            {
              schedule_time_1_ = start_time;
              schedule_time_2_ = start_time;
            }
            else if (schedule_time_2_ != start_time)
            {             
              schedule_time_2_ = start_time;
              TBSYS_LOG(WARN, "rs reboot or switch! schedule_time_1_=[%ld], schedule_time_2_=[%ld]", schedule_time_1_, schedule_time_2_);
            }
          }
          return ret;
        }

        inline uint64_t get_process_tid()
        {
          return process_idx_tid_;
        }

        inline void set_hist_width(int64_t hist_width)
        {
          hist_width_ = hist_width;
        }

        inline uint64_t get_schedule_idx_tid()
        {
          return schedule_idx_tid_;
        }
        //add liuxiao [secondary index static index bug_fix] 20150626
        //判断是否有在建索引
        inline bool if_is_building_index()
        {
          //如果process_idx_tid_的值没有有效索引表id，则此时没有建索引的工作
          if(process_idx_tid_ == OB_INVALID_ID)
          {
            return false;
          }
          else
          {
            return true;
          }
        }

        //add e

        //add zhuyanchao [secondary index static_sstable_build.report]20150325
        ObTabletHistogramReportInfoList* get_ob_tablet_histogram_report_info_list()
        {
          return static_index_report_infolist;
        }
      public:
        inline hash::ObHashMap<ObNewRange,ObTabletLocationList,hash::NoPthreadDefendMode>* get_range_info()
        {
          return &data_multcs_range_hash_;
        }
      private:
        const static int64_t MAX_WORK_THREAD = 32;
        const static int32_t TABLET_COUNT_PER_WORK = 10240;
        const static uint32_t MAX_WORK_PER_DISK = 2;
        const static int64_t SLEEP_INTERVAL = 5000000;
        const static int8_t MAX_FAILE_COUNT = 5;
        //add zhuyanchao test
        const static int64_t tablets_num=200;
        const static int sample_rate=200;
        //add e
        const static int8_t ROUND_TRUE = 1;
        const static int8_t ROUND_FALSE = 0;
        const static int8_t TABLET_RELEASE = 2;
      private:
        int create_all_index_workers();
        int create_index_builders(ObIndexBuilder** builder, const int64_t size);
        int destroy_index_builders(ObIndexBuilder** builder, const int64_t size);
        virtual void run(tbsys::CThread* thread, void *arg);
        void construct_index(const int64_t thread_no);
        //void construct_total_index(const int64_t thread_no);
        int get_tablets_ranges(TabletRecord* &tablet, RangeRecord* &range, int &err);
        void finish_phase1();
        int finish_phase2(bool &total_reported);
        int get_index_builder(const int64_t thread_no, ObIndexBuilder* &builder);
        bool check_if_tablet_range_failed(bool is_local_index, TabletRecord* &tablet, RangeRecord* &range);
        //add wenghaixing [secondary index static_index_build cluster.p2]20150630
        bool is_current_index_complete(const int64_t status);
        bool is_current_index_failed(const int64_t status);
        //add e
        int add_tablet(ObTablet* tablet);
        int add_range(ObNewRange* range);
        //add wenghaixing [secondary index static_index.]20150408
        int whipping_wok(ErrNo level, const ObTablet* tablet, ObNewRange range);
        //add e
        //add wenghaixing [secondary index static_index.exceptional_handle]201504232
        bool is_phase_one_need_end();
        bool is_phase_two_need_end();
        int release_tablet_array();
        void inc_get_tablet_count();
        void inc_get_range_count();
        //add e
      private:
        //add zhuyanchao [secondary index static_data_build]20150320
        common::ObTabletHistogramReportInfoList *static_index_report_infolist;//collect histogram report info
        //add e
        volatile bool inited_;
        int64_t thread_num_;
        uint64_t process_idx_tid_;//当前处理的索引表id
        uint64_t schedule_idx_tid_;//计划即将处理的索引表tid
        int64_t schedule_time_1_;
        int64_t schedule_time_2_;//schedule_time_1_ == schedule_time_2_ unless rootserver reboot or switch
        volatile int local_reported_;//means local histograms reported only in current round
        int64_t tablet_index_;
        int64_t range_index_;
        int64_t hist_width_;
        volatile int64_t tablets_have_got_;
        volatile int64_t range_have_got_;
        volatile int64_t active_thread_num_;
        int64_t min_work_thread_num_;
        volatile int8_t round_start_; //1 round  is start, 0 round is not start
        volatile int8_t round_end_;   //1 round  is end, 0 round is not start, 2 round is end and all tablet is release
        ObTabletManager *tablet_manager_;
        //SpinRWLock migrate_lock_;
        common::ObMergerSchemaManager *schema_mgr_;
        volatile int64_t local_work_start_time_;    //this version start time
        volatile int64_t local_work_last_end_time_;
        volatile int64_t total_work_start_time_;    //this version start time
        volatile int64_t total_work_last_end_time_; //if root server mean to stop mission, change it not to be zero
        //uint64_t tablet_list_[common::OB_MAX_TABLE_NUMBER];
        pthread_cond_t cond_;
        pthread_mutex_t mutex_;
        pthread_mutex_t phase_mutex_;
        pthread_mutex_t tablet_range_mutex_;
        ObArray<TabletRecord> tablet_array_;
        //add wenghaixing [secondary index static_sstable_build]20150320
        ObArray<RangeRecord> range_array_;
        //add e
        ObIndexBuilder *builder_[MAX_WORK_THREAD];
        hash::ObHashMap<ObNewRange,ObTabletLocationList,hash::NoPthreadDefendMode> data_multcs_range_hash_;
        hash::ObHashMap<ObNewRange,ObTabletLocationList,hash::NoPthreadDefendMode> range_hash_;
        //add wenghaixing [secondary index static_sstable_build.report]20150316
        ObTabletReportInfoList report_info_;
        common::ModuleArena report_allocator_;
        //add e
        //add wenghaixing [secondary index static_sstable_build]20150320
        common::ModulePageAllocator mod_;
        ModuleArena allocator_;
        ModuleArena temp_allocator_;
        //add e
        //add wenghaixing [secondary index static_data_build.exceptional_handle]20150403
        BlackListArray black_list_array_;
        //add e


    };

  }

}

#endif // OB_CHUNK_INDEX_WORKER_H
