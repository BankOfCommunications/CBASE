/*
 * =====================================================================================
 *
 *       Filename:  db_dumper.h
 *
 *        Version:  1.0
 *        Created:  04/14/2011 07:26:41 PM
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  yushun.swh (DBA Group), yushun.swh@taobao.com
 *        Company:  taobao
 *
 * =====================================================================================
 */

#ifndef  OB_API_DB_DUMPER_MGR_INC
#define  OB_API_DB_DUMPER_MGR_INC
#include "oceanbase_db.h"
#include "db_record_set.h"
#include "db_parse_log.h"
#include "db_dumper_config.h"
#include "common/file_utils.h"
#include "common/page_arena.h"
#include "common/thread_buffer.h"
#include "db_dumper_checkpoint.h"
#include <string>

using namespace oceanbase::common;
using namespace oceanbase::api;
using namespace tbsys;

class DbDumperMgr;
class RowKeyMemStorage;
class DbDumpMemWriter;
class DbDumperWriteHandler;
class DbDumperWriter;
class DbLogMonitor;

class DbDumperMgr {
  public:
    ~DbDumperMgr();
    int initialize(OceanbaseDb *db, std::string datapath, std::string log_dir, 
                   int64_t parser_thread_nr, int64_t worker_thread_nr);
    int start();
    void stop();
    void wait();

    static void signal_handler(int sig);
    static DbDumperMgr *instance();
  private:
    DbDumperMgr();
    static DbDumperMgr *instance_;

    int64_t get_start_log();

//    ObSchemaManager schema_mgr_;
    OceanbaseDb *db_;
    CRWLock mutex_;

    DbDumper *dumper_;
    DbLogMonitor *monitor_;
    DbLogParser * parser_;

    std::string datapath_;
    std::string log_dir_;

    int64_t parser_thread_nr_;
    int64_t worker_thread_nr_;

    bool started_;
    DbCheckpoint ckp_;
};

#endif   /* ----- #ifndef db_dumper_INC  ----- */
