/*===============================================================
*   (C) 2007-2010 Taobao Inc.
*
*
*   Version: 0.1 2010-09-19
*
*   Authors:
*          daoan(daoan@taobao.com)
*
*
================================================================*/
#ifndef OCEANBASE_ROOTSERVER_OB_CHUNK_SERVER_MANAGER_H_
#define OCEANBASE_ROOTSERVER_OB_CHUNK_SERVER_MANAGER_H_
#include "common/ob_array_helper.h"
#include "common/ob_server.h"
#include "common/ob_array.h"
#include "common/ob_range2.h"
#include "common/ob_data_source_desc.h"
#include "ob_server_balance_info.h"
#include "ob_migrate_info.h"
#include "ob_root_server_config.h"

namespace oceanbase
{
  namespace rootserver
  {
    // shutdown具体执行的操作:shutdown, restart
    enum ShutdownOperation
    {
      SHUTDOWN = 0,
      RESTART,
    };

    struct ObBalanceInfo
    {
      /// total size of all sstables for one particular table in this CS
      int64_t table_sstable_total_size_;
      /// total count of all sstables for one particular table in this CS
      int64_t table_sstable_count_;
      /// the count of currently migrate-in tablets
      int32_t cur_in_;
      /// the count of currently migrate-out tablets
      int32_t cur_out_;

      ObBalanceInfo();
      ~ObBalanceInfo();
      void reset();
      void reset_for_table();
      void inc_in();
      void inc_out();
      void dec_in();
      void dec_out();
    };

    const int64_t CHUNK_LEASE_DURATION = 1000000;
    const int16_t CHUNK_SERVER_MAGIC = static_cast<int16_t>(0xCDFF);
    struct ObServerStatus
    {
      enum CsOperationProcess
      {
        FAILED = -1,
        INIT = 0,
        LOADED = 1,
        REPORTED = 2,
        START_SAMPLE = 3,
        SAMPLED = 4,
        START_INDEX = 5,
        INDEXED = 6,
      };
      enum EStatus {
        STATUS_DEAD = 0,
        STATUS_WAITING_REPORT,
        STATUS_SERVING ,
        STATUS_REPORTING,
        STATUS_REPORTED,
        STATUS_SHUTDOWN,         // will be shut down
      };
      ObServerStatus();
      NEED_SERIALIZE_AND_DESERIALIZE;
      void set_hb_time(int64_t hb_t);
      void set_hb_time_ms(int64_t hb_t);
      void set_lms(bool lms);
      bool is_alive(int64_t now, int64_t lease) const;
      bool is_ms_alive(int64_t now, int64_t lease) const;
      bool is_lms() const;
      void dump(const int32_t index) const;
      const char* get_cs_stat_str() const;

      common::ObServer server_;
      volatile int64_t last_hb_time_;
      volatile int64_t last_hb_time_ms_;  //the last hb time of mergeserver,for compatible,we don't serialize this field
      EStatus ms_status_;
      EStatus status_;

      int32_t port_cs_; //chunk server port
      int32_t port_ms_; //merger server port
      int32_t port_ms_sql_;   /* for ms sql port */

      int32_t hb_retry_times_;        //no need serialize

      ObServerDiskInfo disk_info_; //chunk server disk info
      int64_t register_time_;       // no need serialize
      //used in the new rebalance algorithm, don't serialize
      ObBalanceInfo balance_info_;
      bool wait_restart_;
      bool can_restart_; //all the tablet in this chunkserver has safe copy num replicas, means that this server can be restarted;
      bool lms_;         /* listen mergeserver flag */
      //add pangtianze [Paxos rs_election] 20170420:b
      bool is_valid_;  // when switching to master, rs first rebuild old ms/cs list, only alive ms/cs re-regist succ, they can be valid.
      //add:e
      CsOperationProcess bypass_process_;
    };
    class ObChunkServerManager
    {
      public:
        enum {
          MAX_SERVER_COUNT = 1000,
        };

        typedef ObServerStatus* iterator;
        typedef const ObServerStatus* const_iterator;

        ObChunkServerManager();
        virtual ~ObChunkServerManager();

        //add chujiajia[Paxos rs_election]20151016:b
        void get_servers(common::ObArrayHelper<ObServerStatus> &servers_temp);
        inline bool is_exist_serving_ms()
        {
          bool ret = false;
          const_iterator it = end();
          for (it = begin(); end() != it; ++it)
          {
            if (it->ms_status_ == ObServerStatus::STATUS_SERVING
                && it->port_ms_ != 0)
            {
              TBSYS_LOG(INFO, "exist serving ms port: [%d], server: [%s]",
                        it->port_ms_, to_cstring(it->server_));
              ret = true;
              break;
            }
          }
          return ret;
        }
        inline void reset_server_info()
        {
          servers_.clear();
        }
        //add:e

        iterator begin();
        const_iterator begin() const;
        iterator end();
        const_iterator end() const;
        int64_t size() const;
        iterator find_by_ip(const common::ObServer& server);
        const_iterator find_by_ip(const common::ObServer& server) const;
        const_iterator get_serving_ms() const;
        // regist or receive heartbeat
        int receive_hb(const common::ObServer& server, const int64_t time_stamp, const bool is_merge_server = false,
            const bool is_listen_ms = false, const int32_t sql_port = 0, const bool is_regist = false
                //add pangtianze [Paxos rs_election] 20170420:b
                ,const bool valid_flag = true
                //add:e
                       );
        int update_disk_info(const common::ObServer& server, const ObServerDiskInfo& disk_info);
        int get_ms_port(const common::ObServer &server, int32_t &sql_port)const;
        int get_array_length() const;
        ObServerStatus* get_server_status(const int32_t index);
        const ObServerStatus* get_server_status(const int32_t index) const;
        common::ObServer get_cs(const int32_t index) const;
        int get_server_index(const common::ObServer &server, int32_t &index) const;
        int32_t get_alive_server_count(const bool chunkserver
                                       //add lbzhong [Paxos Cluster.Flow.MS] 201607026:b
                                       , const int32_t cluster_id = 0
                                       //add:e
                                       ) const;
        int get_next_alive_ms(int32_t & index, common::ObServer & server
                              //add lbzhong [Paxos Cluster.Flow.MS] 201607026:b
                              , const int32_t cluster_id
                              //add:e
                              ) const;

        void set_server_down(iterator& it);
        void set_server_down_ms(iterator& it);
        void reset_balance_info();
        void reset_balance_info_for_table(int32_t &cs_num, int32_t &shutdown_num
                                          //add lbzhong [Paxos Cluster.Balance] 20160705:b
                                          , const int64_t cluster_id
                                          //add:e
                                          );
        bool is_migrate_infos_full() const;
        int add_migrate_info(const common::ObServer& src_server, const common::ObServer& dest_server,
            const ObDataSourceDesc& data_source_desc);
        int free_migrate_info(const common::ObNewRange& range, const common::ObServer& src_server,
            const common::ObServer& dest_server);
        void print_migrate_info();
        bool check_migrate_info_timeout(int64_t timeout, common::ObServer& src_server,
            common::ObServer& dest_server, common::ObDataSourceDesc::ObDataSourceType& type);

        int64_t get_migrate_num() const;
        int64_t get_max_migrate_num() const;
        void set_max_migrate_num(int64_t size);

        int shutdown_cs(const common::ObArray<common::ObServer> &servers, enum ShutdownOperation op);
        void restart_all_cs();
        void cancel_restart_all_cs();
        int cancel_shutdown_cs(const common::ObArray<common::ObServer> &servers, enum ShutdownOperation op);
        bool has_shutting_down_server() const;

        //add lbzhong [Paxos Cluster.Balance] 20160706:b
        void is_cluster_alive_with_ms_cs(bool* is_cluster_alive) const;
        bool is_cluster_alive_with_ms_cs(const int32_t cluster_id) const;
        bool is_cluster_alive_with_cs(const int32_t cluster_id) const;
        int32_t get_alive_ms_count(const int32_t cluster_id) const;
        //add:e
        //add bingo [Paxos Cluster.Balance] 20161118:b
        void is_cluster_alive_with_cs(bool* is_cluster_alive_with_cs) const;
        //add:e
        //add bingo [Paxos Cluster.Balance] 20170407:b
        bool is_cluster_alive_with_ms(const int32_t cluster_id) const;
        void is_cluster_alive_with_ms_and_cs(bool* is_cluster_alive) const;
        bool is_cluster_alive_with_ms_and_cs(const int32_t cluster_id) const;
        //add:e

        NEED_SERIALIZE_AND_DESERIALIZE;
        ObChunkServerManager& operator= (const ObChunkServerManager& other);
        int serialize_cs(const ObServerStatus *it, char* buf, const int64_t buf_len, int64_t& pos) const;
        int serialize_ms(const ObServerStatus *it, char* buf, const int64_t buf_len, int64_t& pos) const;
        int serialize_cs_list(char* buf, const int64_t buf_len, int64_t& pos) const;
        int serialize_ms_list(char* buf, const int64_t buf_len, int64_t& pos
                              //add lbzhong [Paxos Cluster.Flow.MS] 201607027:b
                              , const int32_t cluster_id = 0
                              //add:e
                              ) const;

      public:
        int write_to_file(const char* filename);
        int read_from_file(const char* filename, int32_t &cs_num, int32_t &ms_num);
      private:
        mutable tbsys::CRWLock migrate_infos_lock_;
        ObServerStatus data_holder_[MAX_SERVER_COUNT];
        common::ObArrayHelper<ObServerStatus> servers_;
        ObMigrateInfos migrate_infos_;
    };
  }
}
#endif
