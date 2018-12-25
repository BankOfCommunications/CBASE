/*
 * =====================================================================================
 *
 *       Filename:  ob_export_producer.h
 *
 *    Description:  
 *
 *        Version:  1.0
 *        Created:  2014年11月17日 14时44分52秒
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  zhangcd
 *   Organization:  
 *
 * =====================================================================================
 */

#ifndef OB_EXPORT_PRODUCER_H
#define OB_EXPORT_PRODUCER_H

#include <vector>
#include <queue>
#include "ob_export_queue.h"
#include "tbsys.h"
#include "ob_export_param.h"
#include "ob_export_extra_param.h"
#include "db_tablet_info.h"
#include "oceanbase_db.h"
#include "db_row_formator.h"
#include "file_writer.h"//add qianzm [export_by_tablets] 20160415

#define OB_EXPORT_SUCCESS 0
#define OB_EXPORT_NOT_EXIST 1
#define OB_EXPORT_EXIST 2
#define OB_EXPORT_EMPTY 3

using namespace oceanbase::api;


/*
 * =====================================================================================
 *        Class:  ActiveServerSession
 *  Description:  当前客户端所有有效的查询session和所查询的server
 * =====================================================================================
 */
class ActiveServerSession
{
  struct ServerSession
  {
    ObServer server;
    int64_t session_id;
  };

private:
  std::map<int, ServerSession> map;//producer id为key，ServerSession对象为value
  tbsys::CThreadMutex map_lock_;

public:
  int pop_by_producer_id(int producer_id, ObServer& server, int64_t& session_id);
  int push_by_id(int producer_id, ObServer server, int64_t session_id);
  int pop_one(ObServer& server, int64_t& session_id);
};

class ExportProducer : public QueueProducer<TabletBlock*>
{
private:
  typedef std::map<ObRowkey, ObExportTabletInfo> TabletCache;

public:
  ExportProducer(oceanbase::api::OceanbaseDb *db, const ObSchemaManagerV2 *schema_mgr, const char* table_name, 
                 ErrorCode *code, const ObExportTableParam &table_param, DbRowFormator *rowformator,
                 std::vector<oceanbase::common::ObServer> ms_vec, int64_t max_limittime,
                 /*add qianzm [export_by_tablets] 20160524*/bool is_multi_writers,
                 int producer_concurrency); //add by zhuxh [producer concurrency set] 20170518
  int init();
  int produce(TabletBlock* &block);
  int produce_v2(TabletBlock* &block);
  //add qianzm [export_by_tablets] 20160415
  int print_tablet_info_cache();
  int get_tablets_count(){return tablet_index_;}
  //add 20160415:e

private:
  int parse_rowkey(const ObRowkey& rowkey, char *out_buffer, int64_t capacity);
  int add_escape_char(const char *str, const int str_len, char *out_buffer, int64_t capacity);
  int parse_key_to_sql(const ObRowkey& startkey, const ObRowkey& endkey, string& out_range_str);
  ObServer get_one_ms();

  //add by liyongfeng:20141120
  void insert_tablet_cache(const ObRowkey &rowkey, ObExportTabletInfo &tablet);
  int get_tablet_info();
  int parse_scanner(ObScanner &scanner);

  int get_execute_sql(std::string &exe_sql);
  int parse_rowkey_into_sql();
  //add:e

  int init_rowkey_idx(const ObArray<oceanbase::sql::ObResultSet::Field>& fields);
  int create_row_key(const ObRow& row, ObRowkey &rowkey);
  int get_rowkey_column_name(const char* table_name, const ObSchemaManagerV2 *schema, vector<string> &rowkey_column_names);

  int thread_post_end_session();
  int thread_add_server_session();
  int thread_remove_server_session();

public:
  static ActiveServerSession ac_server_session;

private:
  //多线程访问produce函数时使每个线程都有自己的线程私有单例，用来标记状态转移
  struct ProduceMark
  {
    bool next_packet;
    bool next_sql_request;
    int64_t last_timeout;
    bool is_last_timeout;
    ObRowkey start_rowkey;
    ObRowkey last_rowkey;
    int producer_index;
    ProduceMark();
    static int static_index;
    tbsys::CThreadMutex s_index_lock_;
    //add qianzm [export_by_tablets] 20160415
    string err_sql_;
    int tablet_index_;
    //add 20160415:e

    //add by zhuxh [timeout mechanism] 20170512 :b
    int64_t producer_run_times; // How many times producer() has run for, used in the future
    int64_t deadline;           // the thread timeout
    //add by zhuxh [timeout mechanism] 20170512 :e
  };
  
private:
  oceanbase::api::OceanbaseDb *db_;
  ObExportTableParam table_param_;
  int64_t timeout_us_;

  //add by liyongfeng:20141120
  tbsys::CThreadMutex allocator_lock_;
  CharArena allocator_;
  TabletCache tablet_cache_;
  ObRowkey last_end_key_;//用于获取tabletInfo时,记录上一个tablet的end_key,初始值ObRowkey::MIX_ROWKEY
  tbsys::CThreadMutex tablet_cache_lock_;
  //add:e

  std::vector<oceanbase::common::ObServer> ms_vec_;
  ErrorCode *err_code_;

  tbsys::CThreadMutex rowkey_idx_inited_lock_;
  bool is_rowkey_idx_inited_;
  std::vector<int> rowkey_idx_;
  const char* table_name_;
  const ObSchemaManagerV2 *schema_;

  bool is_rowformator_result_field_inited_;
  tbsys::CThreadMutex rowformator_result_field_lock_;
  DbRowFormator *rowformator_;

  int64_t max_limittime_;
  //add qianzm [export_by_tablets] 20160415:b
  int tablet_index_;
  vector<string> failed_sql_;
  vector<int> failed_tablets_index_;
  //add 20160415:e
  bool is_multi_writers_;//add qianzm [export_by_tablets] 20160524

  int producer_concurrency; //add by zhuxh [producer concurrency set] 20170512
};

#endif     /* -----  OB_EXPORT_PRODUCER_H  ----- */
