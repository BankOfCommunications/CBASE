#ifndef OB_INDEX_MONITOR_H
#define OB_INDEX_MONITOR_H
#include "common/ob_define.h"
#include "common/ob_array.h"
//#include "common/ob_se_array.h"
#include "common/ob_vector.h"
#include "ob_root_server2.h"
#include "common/utility.h"
//#include "ob_root_worker.h"
/*
 * OB_INDEX_MONITOR_H
 * 在rs端启动和监控静态索引的创建流程
 */

//using namespace oceanbase::common;
namespace oceanbase
{
  namespace rootserver
  {

    enum MonitorPhase {
      MONITOR_INIT = 0,
      MONITOR_LOCAL,
      MONITOR_GLOABL,
    };

    class ObRootWorker;
    class ObIndexMonitor
    {
      public:
        static const int INDEX_RETRY_TIME = 1;

      public:

        ObIndexMonitor();
        ~ObIndexMonitor();
        void init(ObRootWorker* worker);
        void start();
        void stop();
        bool is_start();
        int start_mission(const int64_t& start_version);
        int schedule();
        int gen_index_list();
        //add liuxiao [secondary index migrate index tablet] 20150410
        bool check_create_index_over();
        //add e
        //add wenghaixing [secondary index.cluster]20150629
        void set_start_version(const int64_t& start_version){start_version_ = start_version;}
        //add e
        //add liumz, [secondary index version management]20160413:b
        int64_t get_start_version() {return start_version_;}
        bool is_last_finished() {return last_finished_;}
        //add:e
        MonitorPhase get_monitor_phase() {return monitor_phase_;}
      private:
        int cs_build_index_signal();
        void monitor();
        int monitor_singal_idx(uint64_t idx_id);
        void clear_up_mess(const uint64_t idx_id, const bool need_delete_rt);
        int commit_index_command();
        //add wenghaixing [secodnary index static_index_build]20150324
        int calc_hist_width(uint64_t index_id, int64_t& width);
        int fetch_tablet_info(const uint64_t table_id,
                              const ObRowkey & row_key, ObScanner & scanner);
        int calc_tablet_num_from_scanner(ObScanner &scanner, ObRowkey &row_key, uint64_t table_id, int64_t &tablet_num);
        //copy from ob_index_worker
        int construct_tablet_item(const uint64_t table_id,
                                  const common::ObRowkey &start_key, const common::ObRowkey &end_rowkey, common::ObNewRange &range,
                                  ObTabletLocationList &list);
        //add e
      private:
        tbsys::CThreadMutex monitor_mutex_;
        tbsys::CThreadMutex mission_mutex_;
        ObRootWorker* worker_;
        bool is_monitor_start_;
        bool running_;
        bool mission_compelete_;
        volatile int64_t mission_start_time_;
        volatile int64_t mission_end_time_;
        uint64_t process_idx_tid_;
        int64_t process_index_num_;
        int64_t success_index_num_;
        ObArray<uint64_t> index_list_;
        ObSortedVector<uint64_t*> sorted_index_list_;
        ObArray<uint64_t> failed_index_;
        int64_t hist_width_;//sample num
        //add wenghaixing [secondary index.cluster]20150629
        int64_t start_version_;
        //add e
        //ObRootServer2 root_server_;      
        MonitorPhase monitor_phase_;
        bool last_finished_;
    };

    //add liuxiao [secondary index migrate index tablet] 20150410
    inline bool ObIndexMonitor::check_create_index_over()
    {
      return !running_;
    }

    //add e

  }//end rootserver
}//end oceanbase


#endif // OB_INDEX_MONITOR_H
