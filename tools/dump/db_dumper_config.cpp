/*
 * =====================================================================================
 *
 *       Filename:  DbDumpConfig.cpp
 *
 *    Description:  
 *
 *        Version:  1.0
 *        Created:  04/14/2011 07:50:34 PM
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  yushun.swh (DBA Group), yushun.swh@taobao.com
 *        Company:  taobao
 *
 * =====================================================================================
 */
#include "db_dumper_config.h"
#include "db_record_filter.h"
#include "common/utility.h"
#include "common/ob_string.h"
#include "oceanbase_db.h"
#include <map>
#include <vector>
#include <algorithm>

using namespace oceanbase::common;
using namespace oceanbase::api;

const char *kConfigObLog="ob_log_dir";
const char *kConfigColumn= "column_info";
const char *kConfigSys = "dumper_config";

const char *kOutputDir = "output_dir";

const char *kSysParserNr = "parser_thread_nr";
const char *kSysWorkerThreadNr = "worker_thread_nr";

const char *kSysLogDir="log_dir";
const char *kSysLogLevel="log_level";

const char *kSysHost = "host";
const char *kSysPort = "port";
const char *kSysNetworkTimeout = "network_timeout";
const char *kSysAppName = "app_name";


const char *kSysMaxFileSize = "max_file_size";
const char *kSysRotateFileTime = "rotate_file_interval";
const char *kConfigTableId = "table_id";
const char *kConfigReviseColumn = "revise_column";
const char *kSysConsistency = "consistency";
const char *kSysMutiGetNr = "muti_get_nr";

//columns should be dumped in rowkey
//rowkey_column=name,start_pos,end_pos,type,endian
const char *kConfigRowkeyColumn = "rowkey_column";
//columns filter, [column name], [ column type] ,[min], [max]
const char *kConfigColumnFilter = "column_filter";

const char *kSysMonitorInterval = "monitor_interval";
const char *kSysTmpLogDir = "tmp_log_dir";
const char *kSysInitLogId = "init_log_id";
const char *kOutputFormat = "output_format";

const char *kSysHeaderDelima = "header_delima";
const char *kSysBodyDelima = "body_delima";
const char *kSysPidFile = "obdump.pid";
const char *kSysMaxNologInvertal = "max_nolog_interval";
const char *kSysInitDate = "init_date";


bool DbTableConfig::is_revise_column(const std::string &column) const
{
  bool ret = false;

  for(size_t i = 0;i < revise_columns_.size(); i++) {
    if (column.compare(revise_columns_[i]) == 0) {
      ret = true;
      break;
    }
  }

  return ret;
}

void DbTableConfig::parse_rowkey_item()
{
  assert(schema_mgr_ != NULL);

  table_id_ = schema_mgr_->get_table_schema(table_.c_str())->get_table_id();
  const ObRowkeyInfo& rowkey_info = schema_mgr_->get_table_schema(table_.c_str())->get_rowkey_info();

  for (int64_t i = 0;i < rowkey_info.get_size();i++) {
    ObRowkeyColumn rowkey_column;
    rowkey_info.get_column(i, rowkey_column);
    int32_t idx = 0;
    const ObColumnSchemaV2* col_schema = schema_mgr_->get_column_schema(table_id_, rowkey_column.column_id_, &idx);

    RowkeyItem item(col_schema->get_name(), i);
    rowkey_columns_.push_back(item);
    TBSYS_LOG(INFO, "table rowkey item%ld: %s", i, item.key_name.c_str());
  }
}

void DbTableConfig::parse_output_columns(const std::string &out_columns)
{
  std::string::size_type delima_pos;
  std::string::size_type base_pos = 0;
  std::string column;

  while ((delima_pos = out_columns.find(",", base_pos)) != std::string::npos) {
    column = out_columns.substr(base_pos, delima_pos - base_pos);
    output_columns_.push_back(column);
    TBSYS_LOG(DEBUG, "COLUMN: %s", column.c_str());
    base_pos = delima_pos + 1;
  }
  
  column = out_columns.substr(base_pos);
  output_columns_.push_back(column);
  TBSYS_LOG(DEBUG, "COLUMN: %s", column.c_str());
}

bool DbTableConfig::get_is_rowkey_column(RowkeyItem &item) const
{
  std::vector<RowkeyItem>::const_iterator rowkey_itr = 
    find(rowkey_columns_.begin(), rowkey_columns_.end(), item);

  //if rowkey contains this column
  bool is_rowkey = false;
  if (rowkey_itr != rowkey_columns_.end()) {
    is_rowkey = true;
    item = *rowkey_itr;
  }
  return is_rowkey;
}

bool DbTableConfig::check_valid() const
{
  bool valid = true;
  const char *table_name = table_.c_str();
  for(size_t i = 0;i < output_columns_.size(); i++) {
    if (schema_mgr_->get_column_schema(table_name, output_columns_[i].c_str()) == NULL) {
      TBSYS_LOG(ERROR, "schema does not contain such column[%s]", output_columns_[i].c_str());
      valid = false;
      break;
    }
  }

  return valid;
}

DbDumpConfigMgr* DbDumpConfigMgr::instance_ = NULL;

DbDumpConfigMgr* DbDumpConfigMgr::get_instance()
{
  if (instance_ == NULL)
    instance_ = new(std::nothrow) DbDumpConfigMgr();

  if (instance_ == NULL) {
    TBSYS_LOG(ERROR, "error when get instance");
  }

  return instance_;
}

void DbDumpConfigMgr::add_table_config(const std::string &table_name)
{
  //table config
  std::vector<const char *> columns;
  columns = TBSYS_CONFIG.getStringList(table_name.c_str(), kConfigColumn);
  DbTableConfig config(table_name, columns, &schema_mgr_);

  //parse rowkey item
  config.parse_rowkey_item();

  //set revise column
  std::vector<const char*> revise_column = TBSYS_CONFIG.getStringList(table_name.c_str(), kConfigReviseColumn);
  config.set_revise_columns(revise_column);
  TBSYS_LOG(INFO, "revise column size=%zu", config.get_revise_columns().size());

  const char *output_format_str = TBSYS_CONFIG.getString(table_name.c_str(), std::string(kOutputFormat));
  if (output_format_str != NULL) {
    std::string str = output_format_str;
    config.parse_output_columns(str);
  }

  const char *filter_str = TBSYS_CONFIG.getString(table_name.c_str(), std::string(kConfigColumnFilter));
  if (filter_str) {
    std::string str = filter_str;
    DbRowFilter *filter = create_filter_from_str(str.substr(str.find('=')));
    config.set_filter(filter);
    filter_set_.push_back(filter);
  }

  //add table 
  if (config.check_valid() == true) {
    configs_.push_back(config);
  } else {
    TBSYS_LOG(ERROR, "check config valid failed, table[%s]", table_name.c_str());
  }
}

int DbDumpConfigMgr::load_sys_param()
{
  parser_thread_nr_ = TBSYS_CONFIG.getInt(kConfigSys, std::string(kSysParserNr), 1);
  output_dir_ = TBSYS_CONFIG.getString(kConfigSys, std::string(kOutputDir), "data");
  worker_thread_nr_ = TBSYS_CONFIG.getInt(kConfigSys, std::string(kSysWorkerThreadNr), 6);
  if (worker_thread_nr_ <= 0) {
    TBSYS_LOG(WARN, "[%ld] is not a valid worker thread number, use 6 instead", worker_thread_nr_);
    worker_thread_nr_ = 6;
  }

  log_dir_ = TBSYS_CONFIG.getString(kConfigSys, std::string(kSysLogDir), "obdump.log");
  log_level_ = TBSYS_CONFIG.getString(kConfigSys, std::string(kSysLogLevel), "DEBUG");
  ob_log_dir_ = TBSYS_CONFIG.getString(kConfigSys, std::string(kConfigObLog));
  if (ob_log_dir_.empty()) {
    TBSYS_LOG(ERROR, "please specify path of commlitlog");
    return OB_ERROR;
  }

  host_ = TBSYS_CONFIG.getString(kConfigSys, std::string(kSysHost));
  port_ = TBSYS_CONFIG.getInt(kConfigSys, std::string(kSysPort), 2500);
  if (host_.empty() || port_ <= 0) {
    TBSYS_LOG(ERROR, "oceabase %s:%d config error", host_.c_str(), port_);
    return OB_ERROR;
  }

  consistency_ = (bool)TBSYS_CONFIG.getInt(kConfigSys, std::string(kSysConsistency), 1);
  network_timeout_ = TBSYS_CONFIG.getInt(kConfigSys, std::string(kSysNetworkTimeout), 8 * kDefaultTimeout);
  max_file_size_ = TBSYS_CONFIG.getInt(kConfigSys, std::string(kSysMaxFileSize), 50 * 1024 * 1024);
  rotate_file_interval_ = TBSYS_CONFIG.getInt(kConfigSys, std::string(kSysRotateFileTime), 60);
  monitor_interval_ = TBSYS_CONFIG.getInt(kConfigSys, std::string(kSysMonitorInterval), 10);
  tmp_log_path_= TBSYS_CONFIG.getString(kConfigSys, std::string(kSysTmpLogDir), "tmp_log");
  init_log_id_ = TBSYS_CONFIG.getInt(kConfigSys, std::string(kSysInitLogId), 0);
  header_delima_ = TBSYS_CONFIG.getInt(kConfigSys, std::string(kSysHeaderDelima), 2);
  body_delima_ = TBSYS_CONFIG.getInt(kConfigSys, std::string(kSysBodyDelima), 1);
  app_name_ = TBSYS_CONFIG.getString(kConfigSys, std::string(kSysAppName), "obdump");
  pid_file_ = TBSYS_CONFIG.getString(kConfigSys, std::string(kSysPidFile), "obdump.pid");
  max_nolog_interval_ = TBSYS_CONFIG.getInt(kConfigSys, std::string(kSysMaxNologInvertal), 3600);
  init_date_ = TBSYS_CONFIG.getString(kConfigSys, std::string(kSysInitDate));
  muti_get_nr_ = TBSYS_CONFIG.getInt(kConfigSys, std::string(kSysMutiGetNr), 0);
  return OB_SUCCESS;
}

int DbDumpConfigMgr::load(const char *path)
{
  int ret = TBSYS_CONFIG.load(path);
  if (ret != OB_SUCCESS) {
    TBSYS_LOG(ERROR, "Can't Load TBSYS config");
    return ret;
  }

  if ((ret = load_sys_param()) != OB_SUCCESS) {
    TBSYS_LOG(ERROR, "load sys param failed, quiting");
    return ret;
  }

  OceanbaseDb db(host_.c_str(), port_);
  if ((ret = db.init()) != OB_SUCCESS) {
    TBSYS_LOG(ERROR, "connect to %s:%d failed", host_.c_str(), port_);
    return ret;
  }

  RPC_WITH_RETIRES(db.fetch_schema(schema_mgr_), 5, ret);
  if (ret != OB_SUCCESS) {
    TBSYS_LOG(ERROR, "dumper config fetch_schema failed, ret = %d", ret);
    return ret;
  }

  std::vector<std::string> sections;
  TBSYS_CONFIG.getSectionName(sections);
  std::vector<std::string>::iterator itr = sections.begin();
  while(itr != sections.end()) {
    if (itr->compare(kConfigSys)) {
      add_table_config(*itr);
    }
    itr++;
  }

  ret = gen_table_id_map();
  if (ret != OB_SUCCESS) {
    TBSYS_LOG(ERROR, "gen table id map failed");
  }

  return ret;
}


int DbDumpConfigMgr::get_table_config(const std::string &table, const DbTableConfig* &cfg)
{
  std::vector<DbTableConfig>::iterator itr = configs_.begin();
  while(itr != configs_.end()) {
    if (table == itr->table()) {
      cfg = &(*itr);
      return OB_SUCCESS;
    }
    itr++;
  }

  return OB_ERROR;
}

int DbDumpConfigMgr::gen_table_id_map()
{
  int ret = OB_SUCCESS;
  std::vector<DbTableConfig>::iterator itr = configs_.begin();

  while(itr != configs_.end()) {
    config_set_[itr->table_id()] = *itr;
    itr++;
  }
  if (configs_.size() != config_set_.size()) {
    TBSYS_LOG(ERROR, "duplicate table id found");
    ret = OB_ERROR;
  }

  return ret;
}

int DbDumpConfigMgr::get_table_config(uint64_t table_id, const DbTableConfig* &cfg)
{
  int ret = OB_SUCCESS;

  std::map<uint64_t, DbTableConfig>::iterator itr = 
    config_set_.find(table_id);

  if (itr == config_set_.end()) {
    ret = OB_ERROR;
  } else {
    cfg = &(itr->second);
  }

  return ret;
}

DbDumpConfigMgr::~DbDumpConfigMgr()
{
  for(size_t i = 0;i < filter_set_.size(); i++) {
    if (filter_set_[i] != NULL)
      delete filter_set_[i];
  }
}

void DbDumpConfigMgr::destory()
{
  if (instance_ != NULL)
    delete instance_;
}
