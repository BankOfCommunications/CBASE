/* * ===================================================================================== 
 * * *       Filename:  db_dumper.cpp 
 * * *    Description:  
 *
 *        Version:  1.0
 *        Created:  04/14/2011 08:26:52 PM
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  yushun.swh (DBA Group), yushun.swh@taobao.com
 *        Company:  taobao
 *
 * =====================================================================================
 */
#include "db_dumper.h"
#include "db_thread_mgr.h"
#include "db_dumper_mgr.h"
#include "db_dumper_config.h"
#include "db_log_monitor.h"
#include <sstream>

DbDumperMgr *DbDumperMgr::instance_ = NULL;

DbDumperMgr::DbDumperMgr() { 
  parser_thread_nr_ = 1; 
  //default threads nr
  worker_thread_nr_ = 10;
  dumper_ = NULL;
  monitor_ = NULL;
  parser_ = NULL;
  started_ = false;

  std::string log_tmp_path = DUMP_CONFIG->get_tmp_log_path();
  std::string ckp_name = "DUMPER.CHECKPOINT";
  ckp_.init(ckp_name, log_tmp_path);
}

DbDumperMgr *DbDumperMgr::instance()
{
  if (instance_ == NULL)
    instance_ = new DbDumperMgr();

  return instance_;
}

void DbDumperMgr::signal_handler(int sig)
{
    UNUSED(sig);
    instance_->stop();
}

DbDumperMgr::~DbDumperMgr()
{
  if (started_ == true) {
    stop();
  }
}

int64_t DbDumperMgr::get_start_log()
{
  int log_id = static_cast<int32_t>(DUMP_CONFIG->get_init_log());

  TBSYS_LOG(INFO, "init log id = %d", log_id);
  int ret = ckp_.load_checkpoint();
  if (ret == OB_SUCCESS) {
    log_id = static_cast<int32_t>(ckp_.id());
  } else {
    TBSYS_LOG(INFO, "no checkpoint info, using init_log_id=%d", log_id);
  }

  return log_id;
}

int DbDumperMgr::initialize(OceanbaseDb *db, std::string datapath, std::string log_dir, 
                            int64_t parser_thread_nr, int64_t worker_thread_nr)
{
  int ret = OB_SUCCESS;
  db_ = db;
  datapath_ = datapath;
  log_dir_ = log_dir;

  parser_thread_nr_ = parser_thread_nr;
  worker_thread_nr_ = worker_thread_nr;

  if (db_ == NULL) {
    TBSYS_LOG(ERROR, "oceanbase database can't be NULL");
    ret = OB_ERROR;
  }

  if (log_dir == "" || worker_thread_nr == 0) {
    TBSYS_LOG(ERROR, "log dir is not specified");
    ret = OB_ERROR;
  }

//  if (ret == OB_SUCCESS)
//    ret = db_->fetch_schema(schema_mgr_);

  TBSYS_LOG(INFO, "UPS commitlog path is %s", log_dir_.c_str());
  return ret;
}

int DbDumperMgr::start()
{
  int ret = OB_SUCCESS;
  if (started_)
    return ret;

  //create dumper
  dumper_ = new(std::nothrow) DbDumper();
  if (dumper_ == NULL) {
    ret = OB_ERROR;
    TBSYS_LOG(ERROR, "can't allocate DbDumper");
  }

  if (ret == OB_SUCCESS) {
    ret = dumper_->init(db_);
    if (ret != OB_SUCCESS) {
      TBSYS_LOG(ERROR, "can't init dumper");
    }
  }

  std::string log_tmp_path = DUMP_CONFIG->get_tmp_log_path();
  if (ret == OB_SUCCESS) {
    //create log monitor
    int64_t check_interval = DUMP_CONFIG->get_monitor_interval();
    int64_t start_log = get_start_log();

    monitor_ = new(std::nothrow) DbLogMonitor(log_dir_, log_tmp_path, check_interval);
    if (monitor_ != NULL) {
      monitor_->start_servic(start_log);
    } else {
      TBSYS_LOG(ERROR, "can't create monitor");
      ret = OB_ERROR;
    }
  }

  if (ret == OB_SUCCESS) {
    sleep(1);                                   /* wait log monitor start */
    parser_ = new(std::nothrow) DbLogParser(log_tmp_path, monitor_, &ckp_);
    if (parser_ == NULL) {
      TBSYS_LOG(ERROR, "can't allocate parser");
      ret = OB_ERROR;
    }
    ret = parser_->start_parse(dumper_);
  }

  if (ret == OB_SUCCESS) {
    DbThreadMgr::get_instance()->init(dumper_, DUMP_CONFIG->muti_get_nr());
    ret = DbThreadMgr::get_instance()->start(static_cast<int32_t>(worker_thread_nr_));
  } 

  if (ret == OB_SUCCESS) {
    signal(SIGTERM, DbDumperMgr::signal_handler);
  }

  if (ret != OB_SUCCESS) {
    TBSYS_LOG(INFO, "error ocurrs, cleaning dumper mgr stuff");

    if (parser_ != NULL) {
      delete parser_;
      parser_ = NULL;
    }
    if (monitor_ != NULL) {
      delete monitor_;
      monitor_ = NULL;
    }
    if (dumper_ != NULL) {
      delete dumper_;
      dumper_ = NULL;
    }
    DbThreadMgr::get_instance()->destroy();
  } else {
    started_ = true;
    TBSYS_LOG(INFO, "log serveice started");
  }

  return ret;
}

void DbDumperMgr::stop()
{
  if (started_ == true) {
    started_ = false;
    TBSYS_LOG(INFO, "stop dumper process");
    monitor_->stop_service();

    TBSYS_LOG(INFO, "stop parser");
    parser_->stop_parse();

    DbThreadMgr::get_instance()->stop();
    DbThreadMgr::get_instance()->destroy();

  }
}

void DbDumperMgr::wait()
{
  if (started_)
    monitor_->wait();

  delete parser_;
  parser_ = NULL;

  delete monitor_;
  monitor_ = NULL;

  delete dumper_;
  dumper_ = NULL;
}
