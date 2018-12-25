/*
 * =====================================================================================
 *
 *       Filename:  db_parse_log.h
 *
 *    Description:  
 *
 *        Version:  1.0
 *        Created:  04/19/2011 03:51:58 PM
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  yushun.swh (DBA Group), yushun.swh@taobao.com
 *        Company:  taobao
 *
 * =====================================================================================
 */

#ifndef  db_parse_log_INC
#define  db_parse_log_INC
#include "common/utility.h"
#include "common/ob_string.h"
#include "common/ob_action_flag.h"
#include "common/ob_common_param.h"
//#include "db_dumper.h"
#include <string>
#include <map>

using namespace oceanbase::common;

class DbLogMonitor;
class DbCheckpoint;

class RowkeyHandler {
  public:
      virtual ~RowkeyHandler() { }

      virtual bool need_seq() { return false; }
      virtual int64_t get_next_seq() { return 0; }

      virtual int process_rowkey(const ObCellInfo &cell, int op, uint64_t timestamp) 
      { 
        UNUSED(cell);
        UNUSED(op);
        UNUSED(timestamp);
        return 0; 
      }

      virtual int process_rowkey(const ObCellInfo &cell, int op, uint64_t timestamp, int64_t seq) 
      { 
        UNUSED(cell);
        UNUSED(op);
        UNUSED(timestamp);
        UNUSED(seq);
        return 0; 
      }

      virtual int start_process() { return OB_SUCCESS; }
      virtual int end_process() { return OB_SUCCESS; }
      virtual int get_id() { return 0; }
      virtual void wait_completion(bool) = 0;
};

class DbLogParser : public tbsys::Runnable {
  public:
    DbLogParser(std::string &path, DbLogMonitor *monitor, DbCheckpoint *ckp);
    ~DbLogParser();

    int start_parse(RowkeyHandler *handler);
    void stop_parse();

    void run(tbsys::CThread *thread, void *arg);
    int parse_log(int64_t log_id);
  private:
    int remove_log_file(int64_t log_id);

    RowkeyHandler *handler_;

    std::string log_path_;
    tbsys::CThread *thr_;
    bool running_;
    DbLogMonitor *monitor_;
    DbCheckpoint *ckp_;
};

int search_logs(const char *log_dir_path, const char *time_start, const char *time_end, long &log_start, long &log_end);

#endif   /* ----- #ifndef db_parse_log_INC  ----- */
