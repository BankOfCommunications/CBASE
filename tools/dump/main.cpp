/*
 * =====================================================================================
 *
 *       Filename:  main.cpp
 *
 *        Version:  1.0
 *        Created:  04/13/2011 02:15:56 PM
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  yushun.swh@taobao.com
 *        Company:  taobao
 *
 * =====================================================================================
 */

#include "oceanbase_db.h"
#include "db_table_info.h"
#include "db_record_set.h"
#include "db_dumper.h"
#include "db_dumper_config.h"
#include "db_dumper_mgr.h"
#include <iostream>
#include <vector>
#include <string>
#include <algorithm>

using namespace oceanbase::api;
using namespace oceanbase::common;
using namespace std;
using namespace tbsys;

void usage(const char *program)
{
  cout << program << " "
    << "-c [CONFIG FILE] "
    << "-s [START LOG ID]"
    << endl;
}

int deamonize()
{
  int ret = OB_SUCCESS;
  string pid_file = DUMP_CONFIG->pid_file();
  string log_file = DUMP_CONFIG->get_log_dir();

  int pid = 0;
  if ((pid = CProcess::existPid(pid_file.c_str()))) {
    cerr << "program has already started" << endl;
    ret = OB_ERROR;
  } else {
    cout << "log file" << log_file << endl;
    if (CProcess::startDaemon(pid_file.c_str(), log_file.c_str()) != 0)
      ret = OB_ERROR;
  }

  return ret;
}

int setup_db_env(const char *config_file, bool start_deamon)
{
  OceanbaseDb::global_init(DUMP_CONFIG->get_log_dir().c_str(), 
      DUMP_CONFIG->get_log_level().c_str());

  int ret = OB_SUCCESS;
  ret = DUMP_CONFIG->load(config_file);
  if (ret == OB_SUCCESS) {
    if (start_deamon) {
      ret = deamonize();
    }
  } else {
    TBSYS_LOG(ERROR, "initialize config file error, quiting");
  }

  return ret;
}

void destory_db_env()
{
  DUMP_CONFIG->destory();
}

int do_dump_work()
{
  int ret = OB_SUCCESS;
  OceanbaseDb db(DUMP_CONFIG->get_host(), DUMP_CONFIG->get_port(), DUMP_CONFIG->get_network_timeout());
  db.init();

  db.set_consistency(DUMP_CONFIG->get_consistency());

  string output_dir = DUMP_CONFIG->get_output_dir();
  string log_dir = DUMP_CONFIG->get_ob_log();
  int64_t parser_nr = DUMP_CONFIG->get_parser_thd_nr();
  int64_t worker_nr = DUMP_CONFIG->get_worker_thread_nr();
  string tmp_log_path = DUMP_CONFIG->get_tmp_log_path();

  DbDumperMgr *dumper_mgr = DbDumperMgr::instance();
  if (dumper_mgr == NULL) {
    TBSYS_LOG(ERROR, "can't get instance");
    ret = OB_ERROR;
  } else {
    ret = dumper_mgr->initialize(&db, output_dir, log_dir, parser_nr, worker_nr);
    if (ret != OB_SUCCESS) {
      TBSYS_LOG(ERROR, "can't initialize DbDumperMgr, due to [%d]", ret);
    }
  }

  if (ret == OB_SUCCESS) {
    ret = dumper_mgr->start();
    if (ret == OB_SUCCESS)
      dumper_mgr->wait();
    else 
      TBSYS_LOG(ERROR, "can't start dumper mgr");
//    dumper_mgr.stop();
  }

  return ret;
}

int main(int argc, char *argv[])
{
  const char *config_file = NULL;

  int ret = 0;
  bool start_deamon = false;

  while ((ret = getopt(argc, argv, "c:d")) != -1) {
    switch (ret) {
     case 'c':
       config_file = optarg;
       break;
     case 'd':
       start_deamon = true;
       break;
    }
  }

  if (config_file == NULL) {
    usage(argv[0]);
    return 0;
  }
  
  ret = setup_db_env(config_file, start_deamon);
  if (ret == OB_SUCCESS) {
    ret = do_dump_work();
  }

  destory_db_env();

  return ret;
}
