/*
 * =====================================================================================
 *
 *       Filename:  db_dump_config.h
 *
 *    Description:  
 *
 *        Version:  1.0
 *        Created:  04/14/2011 07:44:04 PM
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  yushun.swh (DBA Group), yushun.swh@taobao.com
 *        Company:  taobao
 *
 * =====================================================================================
 */

#ifndef  OB_API_DB_SCHEMA_INC
#define  OB_API_DB_SCHEMA_INC
#include <string>
#include <vector>
#include <map>
#include "common/ob_schema.h"
#include "db_record_filter.h"

#define DUMP_CONFIG (DbDumpConfigMgr::get_instance())

using namespace oceanbase::common;

class DbTableConfig {
  public:
    struct RowkeyItem {
      std::string key_name;
      int info_idx;

      RowkeyItem() {
        info_idx = -1;
      }

      RowkeyItem(const std::string &key_name) {
        this->key_name = key_name;
      }

      RowkeyItem(const char *key_name, int idx) {
        this->key_name = key_name;
        info_idx = idx;
      }

      bool operator==(const RowkeyItem &rhs) const {
        return key_name == rhs.key_name;
      }
    };
  public:
    DbTableConfig(const std::string &tab, const std::vector<const char *> &columns, const ObSchemaManagerV2 *schema_mgr) { 
      table_ = tab;
      filter_ = NULL;

      std::vector<const char *>::const_iterator itr = columns.begin();
      while(itr != columns.end()) {
        columns_.push_back(std::string(*itr));
        itr++;
      }
      schema_mgr_ = schema_mgr;
    }

    DbTableConfig() {
      table_id_ = 0;
      filter_ = NULL;
      schema_mgr_ = NULL;
    }

    ~DbTableConfig() {
    }

    DbTableConfig(const DbTableConfig &rhs)
    {
      table_id_ = rhs.table_id_;
      table_= rhs.table_;
      columns_ = rhs.columns_;
      filter_ = rhs.filter_;
      rowkey_columns_ = rhs.rowkey_columns_;
      output_columns_ = rhs.output_columns_;
      revise_columns_ = rhs.revise_columns_;
      schema_mgr_ = rhs.schema_mgr_;
    }

    const std::string &get_table() { return table_; }

    inline std::vector<std::string>& get_columns() { 
      return columns_; 
    }

    inline const std::vector<std::string>& get_columns() const { 
      return columns_;
    }

    bool check_valid() const;

    std::vector<RowkeyItem>& rowkey_columns() { return rowkey_columns_; }
    std::vector<std::string>& output_columns() { return output_columns_; }
    const std::vector<std::string>& output_columns() const { return output_columns_; }

    const std::string &table() const { return table_; }
    uint64_t table_id() const { return table_id_; }

    void parse_rowkey_item();
    void parse_output_columns(const std::string &out_columns);

    std::vector<const char *> & get_revise_columns() { return revise_columns_; }

    void set_revise_columns(std::vector<const char *> &revise_columns) {
      revise_columns_ = revise_columns;
    }

    bool is_revise_column(const std::string &column) const;

    bool get_is_rowkey_column(RowkeyItem &item) const;

    void set_filter(DbRowFilter *filter) { filter_ = filter; }

    const DbRowFilter *filter() const { return filter_; }
  private:
    std::string table_;
    std::vector<std::string> columns_;
    std::vector<RowkeyItem> rowkey_columns_;
    std::vector<std::string> output_columns_;

    std::vector<const char *> revise_columns_;
    const ObSchemaManagerV2 *schema_mgr_;
    uint64_t table_id_;
    DbRowFilter *filter_;
};

class DbDumpConfigMgr {
  public:
    ~DbDumpConfigMgr();

    int load(const char *file);
    int load_sys_param();

    static DbDumpConfigMgr *get_instance(); 
    std::vector<DbTableConfig>& get_configs() { return configs_; }

    int get_table_config(const std::string &table, const DbTableConfig* &cfg);
    int get_table_config(uint64_t table_id, const DbTableConfig* &cfg);

    const std::string &get_output_dir() const { return output_dir_; }

    const std::string &get_log_dir() { return log_dir_; }
    const std::string &get_log_level() { return log_level_; }
    const std::string &get_ob_log() const { return ob_log_dir_; }
    const std::string &app_name() const { return app_name_; }

    const char *get_host() const { return host_.c_str(); }
    unsigned short get_port() const { return port_; }
    int64_t get_network_timeout() const { return network_timeout_; }

    int64_t get_worker_thread_nr() const { return worker_thread_nr_; }
    int64_t get_parser_thd_nr() const { return parser_thread_nr_; }

    int gen_table_id_map();

    int64_t max_file_size() { return max_file_size_; }
    int64_t rotate_file_interval() { return rotate_file_interval_; }

    const std::string &get_tmp_log_path() const { return tmp_log_path_; }
    int64_t get_monitor_interval() { return nas_check_interval_; }

    int64_t get_init_log() { return init_log_id_; }

    char get_header_delima() { return header_delima_; }
    char get_body_delima() { return body_delima_; }

    const std::string &pid_file() { return pid_file_; }
    void destory();

    int64_t max_nolog_interval() { return max_nolog_interval_; }

    bool get_consistency() const { return consistency_; }

    const char *init_date() const { return init_date_;}

    int muti_get_nr() const { return muti_get_nr_; }

    void add_table_config(const std::string &table_name);
  private:
    DbDumpConfigMgr() { 
      network_timeout_ = 1000000;
      worker_thread_nr_ = 10;
      parser_thread_nr_ = 1;
      max_file_size_ = 100 * 1024 * 1024; //100M
      rotate_file_interval_ = 10; //10S
      nas_check_interval_ = 1; //1s
      body_delima_ = '\001';
      header_delima_ = '\002';
      pid_file_ = "obdump.pid";
      max_nolog_interval_ = 0;
      consistency_ = true;
      init_date_ = NULL;
      muti_get_nr_ = 0;
    }

    std::vector<DbRowFilter *> filter_set_;

    std::vector<DbTableConfig> configs_;
    std::map<uint64_t, DbTableConfig> config_set_;

    static DbDumpConfigMgr *instance_;

    int64_t network_timeout_;

    std::string pid_file_;
    std::string log_dir_;
    std::string log_level_;

    std::string ob_log_dir_;

    //output data path
    std::string output_dir_;

    std::string host_;
    unsigned short port_;
    std::string app_name_;

    //thread nr def
    int64_t worker_thread_nr_;
    int64_t parser_thread_nr_;

    //output file
    int64_t max_file_size_;
    int64_t rotate_file_interval_;

    std::string tmp_log_path_;

    int64_t nas_check_interval_;
    int64_t init_log_id_;

    char header_delima_;
    char body_delima_;

    int64_t monitor_interval_;
    int64_t max_nolog_interval_;                /* seconds */

    bool consistency_;

    const char *init_date_;
    int muti_get_nr_;

    ObSchemaManagerV2 schema_mgr_;
};

#endif   /* ----- #ifndef ob_api_db_schema_INC  ----- */
