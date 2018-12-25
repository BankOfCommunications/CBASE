/*
 * =====================================================================================
 *
 *       Filename:  OceanbaseDb.h
 *
 *        Version:  1.0
 *        Created:  04/12/2011 04:48:39 PM
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  yushun.swh (DBA Group), yushun.swh@taobao.com
 *        Company:  taobao
 *
 * =====================================================================================
 */
#ifndef OB_API_OCEANBASEDB_H
#define  OB_API_OCEANBASEDB_H

#include "common/ob_packet_factory.h"
#include "common/ob_client_manager.h"
#include "common/ob_server.h"
#include "common/ob_string.h"
#include "common/ob_scanner.h"
#include "common/ob_result.h"
#include "common/utility.h"
#include "common/ob_schema.h"
#include "common/ob_mutator.h"
#include "common/ob_object.h"
#include "common/ob_base_client.h"
#include "common/thread_buffer.h"
#include "sql/ob_sql_result_set.h"
#include "common/ob_general_rpc_stub.h" //add by zhuxh:20160712 [New way of fetching schema]
#include "db_table_info.h"
#include <string>
#include <map>
#include <vector>

//add by zhuxh:20160120
#define MAX_CLUSTER_NUM 32

#define RPC_WITH_RETIRES(RPC, retries, RET) {                                                       \
  int __RPC_LOCAL_INDEX__ = 0;                                                                      \
  while (__RPC_LOCAL_INDEX__++ < retries) {                                                         \
    (RET) = (RPC);                                                                                  \
    if ((RET) != 0) {                                                                               \
      TBSYS_LOG(WARN, "call {%s} failed, ret = %d, retry = %d", #RPC, (RET), __RPC_LOCAL_INDEX__);  \
    } else {                                                                                        \
      break;                                                                                        \
    }                                                                                               \
  }                                                                                                 \
}
//add by liyongfeng:20141020 每次重试都先sleep(5)
#define RPC_WITH_RETRIES_SLEEP(RPC, retries, RET) {															\
	int __RPC_LOCAL_INDEX__ = 0;																			\
	while (__RPC_LOCAL_INDEX__++ < retries) {																\
		(RET) = (RPC);																						\
		if ((RET) != 0) {																					\
			TBSYS_LOG(WARN, "call {%s} failed, ret = %d, retry = %d", #RPC, (RET), __RPC_LOCAL_INDEX__);	\
			sleep(5);																						\
		} else {																							\
			break;																							\
		}																									\
	}																										\
}

extern const int kDefaultResultBufferSize;
extern const int64_t MAX_TIMEOUT_US;
extern const int64_t SPAN_TIMEOUT_US;


namespace oceanbase {
  namespace api {
    const int TSI_GET_ID = 1001;
    const int TSI_SCAN_ID = 1002;
    const int TSI_MBUF_ID = 1003;

    class DbRecordSet;
    using namespace common;

    const int64_t kDefaultTimeout = 2000000;

    class DbTranscation;
    class OceanbaseDb;

    class RowMutator {
      public:
        friend class DbTranscation;
      public:

        int delete_row();
        int add(const char *column_name, const ObObj &val);
        int add(const std::string &column_name, const ObObj &val);

        int32_t op() const { return op_; }

        RowMutator();
        RowMutator(const std::string &table_name, const ObRowkey &rowkey, 
            DbTranscation *tnx);

      private:
        
        void set_op(int32_t op) { op_ = op; }
        void set(const std::string &table_name, const ObRowkey &rowkey, 
            DbTranscation *tnx);

        std::string table_name_;
        ObRowkey rowkey_;
        DbTranscation *tnx_;
        int32_t op_;
    };

    class DbTranscation {
      public:
        friend class RowMutator;
        friend class OceanbaseDb;
      public:
        DbTranscation();
        DbTranscation(OceanbaseDb *db);

        int insert_mutator(const char* table_name, const ObRowkey &rowkey, RowMutator *mutator);
        int update_mutator(const char* table_name, const ObRowkey &rowkey, RowMutator *mutator);

        int free_row_mutator(RowMutator *&mutator);

        int commit();
        int abort();
        int reset();
        inline void set_db(OceanbaseDb *db)
        {
          db_ = db;
          assert(db_ != NULL);
        }

      private:
        int add_cell(ObMutatorCellInfo &cell);

        OceanbaseDb *db_;
        ObMutator mutator_;
    };

    struct DbMutiGetRow {
      DbMutiGetRow() :columns(NULL) { }

      ObRowkey rowkey;
      const std::vector<std::string> *columns;
      std::string table;
    };

    class OceanbaseDb {
      private:
        typedef std::map<ObRowkey, TabletInfo> CacheRow;
        typedef std::map<std::string, CacheRow > CacheSet;
      public:
        struct DbStats {
          int64_t total_succ_gets;
          int64_t total_fail_gets;
          int64_t total_send_bytes;
          int64_t total_recv_bytes;
          int64_t total_succ_apply;
          int64_t total_fail_apply;

          DbStats() {
            memset(this, 0, sizeof(DbStats));
          }
        };

      // add by zcd, only used in init_execute_sql(), get_result() :b
        // 这个结构是为了用于多线程使用init_execute_sql(), get_result()函数
        // 每个线程都有自己的私有单例ExecuteSqlParam对象来作运行标记
        struct ExecuteSqlParam
        {
          ObServer ms;
          std::string sql;
          int64_t session_id;
          int64_t client_timeout_timestamp;
          int64_t ms_timeout;
          bool is_first;
        };
      // :end
      public:
        static const uint64_t kTabletTimeout = 10;
        friend class DbTranscation;
      public:
        OceanbaseDb(const char *ip, unsigned short port, int64_t timeout = kDefaultTimeout, 
            uint64_t tablet_timeout = kTabletTimeout);
        ~OceanbaseDb();

        int get(const std::string &table, const std::vector<std::string> &columns,
            const ObRowkey &rowkey, DbRecordSet &rs);

        int get(const std::string &table, const std::vector<std::string> &columns,
            const std::vector<ObRowkey> &rowkeys, DbRecordSet &rs);

        int get(const std::vector<DbMutiGetRow> &rows, DbRecordSet &rs);

        int init();
        static int global_init(const char*log_dir, const char *level);
        int get_ms_location(const ObRowkey &row_key, const std::string &table_name);

        int get_tablet_location(const std::string &table, const ObRowkey &rowkey, 
            common::ObServer &server);

        int fetch_schema(common::ObSchemaManagerV2& schema_manager);

        int search_tablet_cache(const std::string &table, const ObRowkey &rowkey, TabletInfo &loc);
        void insert_tablet_cache(const std::string &table, const ObRowkey &rowkey, TabletInfo &tablet);

        DbStats db_stats() const { return db_stats_; }

        int start_tnx(DbTranscation *&tnx);
        void end_tnx(DbTranscation *&tnx);

        int get_update_server(ObServer &server);

        void set_consistency(bool consistency) { consistency_ = consistency; }

        bool get_consistency() const { return consistency_; }

        int scan(const TabletInfo &tablets, const std::string &table, const std::vector<std::string> &columns, 
            const ObRowkey &start_key, const ObRowkey &end_key, DbRecordSet &rs, int64_t version = 0, 
            bool inclusive_start = false, bool inclusive_end = true);

        int scan(const std::string &table, const std::vector<std::string> &columns, 
            const ObRowkey &start_key, const ObRowkey &end_key, DbRecordSet &rs, int64_t version = 0, 
            bool inclusive_start = false, bool inclusive_end = true);

        int get_memtable_version(int64_t &version);

        int set_limit_info(const int64_t offset, const int64_t count);

        // add by liyongfeng, 20141014, get state of daily merge from rootserver
		int get_daily_merge_state(int32_t &state);//获取每日合并的状态
		int get_latest_update_server(int32_t &ups_switch);//ob_import结束,获取最新主UPS,并与最初获取的主UPS进行比较,如果不一致,表示ob_import过程中有UPS切换
		//bool compare_update_server(ObServer server);//将最新主UPS,并与最初获取的主UPS进行比较,如果不一致,表示ob_import过程中有UPS切换
      // add:end
        // add by zcd:b
        // 获取当前集群中可以作为导出工具使用的ms集合，选取算法见实现
        int fetch_ms_list(const int master_percent, const int slave_percent, std::vector<ObServer> &array);

        // add by zhangcd 20150814:b
        int fetch_mysql_ms_list(const int master_percent, std::vector<ObServer> &array);
        // add:e

        // 每次开始sql查询时需要调用此函数初始化查询参数
        int init_execute_sql(const ObServer& ms, const std::string& sql, int64_t ms_timeout, int64_t client_timeout);

        // 得到下一个数据包
        int get_result(sql::ObSQLResultSet &rs, ObDataBuffer &out_buffer, bool &has_next);

        // 由rootserver来获得一个ms地址
        int get_a_ms_from_rs(ObServer &ms);

        // 获取当前OceanbaseDb对象中存储的超时时间
        int64_t get_timeout_us(){ return timeout_; }

        // 发送终止接收数据包的命令
        int post_end_next(ObServer server, int64_t session_id);

        // 获取当前线程中正在执行的sql所对应的server和session_id
        int get_thread_server_session(ObServer &server, int64_t& session_id);
        //:end

        int execute_dml_sql(const ObString& ob_sql, const ObServer &ms, int64_t& affected_row);

	// add by lyf :b
        // 获取一个table的所有tabletinfo信息
        int get_tablet_info(const std::string &table, const ObRowkey &rowkey, ObDataBuffer &data_buff, ObScanner &scanner);
    //:end
        //add by zhuxh:20160603 [Sequence] :b
        int sequence(bool create, std::string seq_name, int64_t start = 0, int64_t increment = 1, bool default_start = true, bool default_increment = true); //add by zhuxh:20160907 [cb Sequence]
        //add :e

      private:
        int init_get_param(ObGetParam *&param, const std::vector<DbMutiGetRow> &rows);

        int do_server_get(common::ObServer &server, 
            const ObRowkey& row_key, common::ObScanner &scanner, 
            common::ObDataBuffer& data_buff, const std::string &table_name, 
            const std::vector<std::string> &columns);

        int do_muti_get(common::ObServer &server, const std::vector<DbMutiGetRow>& rows, 
            ObScanner &scanner, ObDataBuffer& data_buff);

        common::ObScanParam *get_scan_param(const std::string &table, const std::vector<std::string>& columns,
            const ObRowkey &start_key, const ObRowkey &end_key,
            bool inclusive_start = false, bool inclusive_end = true,
            int64_t data_version = 0);

        void free_tablet_cache();
        void mark_ms_failure(ObServer &server, const std::string &table, const ObRowkey &rowkey);
        void try_mark_server_fail(TabletInfo &tablet_info, ObServer &server, bool &do_erase_tablet);

        int do_server_cmd(const ObServer &server, const int32_t opcode, 
            ObDataBuffer &inout_buffer, int64_t &pos);

        //reference count of db handle
        void unref() { __sync_fetch_and_sub(&db_ref_, 1); }
        void ref() { __sync_fetch_and_add (&db_ref_, 1); }

        // add by zcd :b
        // 发送一个请求包到ms上，并得到结果集中的第一个数据包返回
        int send_execute_sql(const ObServer& ms, const std::string& sql, const int64_t timeout, int64_t& session_id, sql::ObSQLResultSet &rs, common::ObDataBuffer &out_buffer, bool &has_next);

        // 获得结果集中后续的数据包返回
        int get_next_result(const ObServer& ms, const int64_t timeout, sql::ObSQLResultSet& rs, common::ObDataBuffer &out_buffer, const int64_t session_id, bool &has_next);

        // 获取线程私有的ObDataBuffer单例存储在data_buffer中
        int get_thread_buffer(common::ObDataBuffer& data_buffer);

        // 获取线程私有的ExecuteSqlParam单例存储在param中
        int get_ts_execute_sql_param(struct ExecuteSqlParam *&param);

        // 删除线程私有单列，在线程结束的时候会被调用到
        static void destroy_ts_execute_sql_param_key(void *ptr);
        // :end


        // add by zhuxh:20160118 :b
        // to init has_slave_cluster, slave_root_server_ and slave_client_
        int init_slave_cluster_info();
        // add :e

        common::ObServer root_server_;
        common::ObServer update_server_;
        common::ObBaseClient client_;

        //add by zhuxh:20160118 :b //check merge of slave cluster
        int arr_slave_client_length;
        common::ObBaseClient arr_slave_client_[MAX_CLUSTER_NUM];
        //add :e

        CacheSet cache_set_;
        tbsys::CThreadMutex cache_lock_;
       
        uint64_t tablet_timeout_;
        int64_t timeout_;
        bool inited_;

        DbStats db_stats_;

        int64_t db_ref_;
        bool consistency_;

        int64_t limit_offset_;
        /// 0 means not limit
        int64_t limit_count_;

        // add by zcd :b
        pthread_key_t send_sql_param_key_;
        // :end

        // add by zcd :b
        common::ThreadSpecificBuffer tsbuffer_;
        // :end

        common::ThreadSpecificBuffer rpc_buffer_; //add by zhuxh:20160712 [New way of fetching schema]
    };
  }
}

#endif
