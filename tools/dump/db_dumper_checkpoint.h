/*
 * =====================================================================================
 *
 *       Filename:  db_dumper_checkpoint.h
 *
 *    Description:  
 *
 *        Version:  1.0
 *        Created:  06/15/2011 03:54:29 PM
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  yushun.swh (), yushun.swh@taobao.com
 *        Company:  taobao
 *
 * =====================================================================================
 */

#ifndef  DB_DUMPER_CHECKPOINT_INC
#define  DB_DUMPER_CHECKPOINT_INC
#include "common/utility.h"
#include <string>
#include <set>

using namespace tbsys;
using namespace oceanbase::common;

class DbCheckpoint : CDefaultRunnable {
  public:
    DbCheckpoint(std::string name, std::string path) 
      :id_(0), path_(path + "/" + name), ckp_name_(name) { }

    DbCheckpoint()
      :id_(0) { }

    ~DbCheckpoint();

    int64_t id() { return id_; }
    int64_t inc_id() { return ++id_; }

    int load_checkpoint();
    int write_checkpoint(int64_t log_id);

    void run(CThread *thr, void *arg);
    void init(std::string name, std::string path);

  private:
    int do_checkpoint_local(int64_t log_id);
    int do_checkpoint_remote(int64_t log_id);

    int64_t id_;
    std::string path_;
    std::string ckp_name_;
};

#endif   /* ----- #ifndef db_dumper_checkpoint_INC  ----- */
