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

#ifndef  OB_API_DB_DUMPER_WRITER_INC
#define  OB_API_DB_DUMPER_WRITER_INC
#include "oceanbase_db.h"
#include "db_record_set.h"
#include "db_parse_log.h"
#include "db_dumper_config.h"
#include "db_dumper_checkpoint.h"
#include "common/file_utils.h"
#include "common/page_arena.h"
#include "common/thread_buffer.h"
#include "file_appender.h"

#include <set>
#include <list>
#include <string>
#include <vector>
#include <algorithm>
#include <pthread.h>
#include <semaphore.h>

using namespace oceanbase::common;
using namespace oceanbase::api;
using namespace tbsys;

class DbDumperMgr;
class RowKeyMemStorage;

class DbDumperWriteHandler {
  public:
    virtual ~DbDumperWriteHandler() { }
    virtual int push_record(const char *data, int len) 
    { 
      UNUSED(data);
      UNUSED(len);
      return OB_SUCCESS; 
    }
    virtual int start() { return OB_SUCCESS; }
    virtual void stop() {  }
    virtual uint64_t get_id() { return 0; }
    virtual void wait_completion(bool rotate_date) = 0;
    virtual int rotate_date_dir() = 0;
};

class DbOutputFiles {
  public:
    DbOutputFiles() { }

    inline int output_done(std::string &path) { 
      FileUtils file;
      std::string tmp_path = path + "DONE";

      int ret = file.open(tmp_path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0600);

      if (ret < 0)
        ret = OB_ERROR;
      else {
        file.close();
        ret = OB_SUCCESS;
      }

      return ret;
    }
};

class DbDumperWriter : public Runnable, public DbDumperWriteHandler {
  public:
    //sem_wait will timeout in 2 seconds
    const static int kSemWaitTimeout = 2;
  public:
    struct RecordInfo {
      int length;
      char buf[0];
    };

    DbDumperWriter(uint64_t id, std::string path);

    ~DbDumperWriter();

    void run(tbsys::CThread *thr, void *arg);
    int start();
    void stop();

    int push_record(const char *data, int lenth);
    int write_record(bool flush);

    RecordInfo *alloc_record(const char *data, int len);
    void free_record(RecordInfo *rec);

    uint64_t get_id() { return id_; }
    int64_t log_id() { return log_id_; }

    void wait_completion(bool rotate_date);

    //rotate dir name
    virtual int rotate_date_dir();

    //statitics info
    int pushed_lines_;
    int writen_lines_;

  private:
    int init_output_files();
    int rotate_output_file();
    bool need_rotate_output_file();
    int mv_output_file();
    int64_t get_next_file_id();

    int64_t log_id_;
    DbOutputFiles output_files_;
    time_t last_rotate_time_;
    std::string current_date_;

    uint64_t id_;
    std::string path_;
    AppendableFile *file_;

    std::list<RecordInfo *> records_;

    bool running_;
    sem_t sem_empty_;
    tbsys::CThread *thr_;
    CThreadMutex records_lock_;
};

#endif   /* ----- #ifndef db_dumper_INC  ----- */
