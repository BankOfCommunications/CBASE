#ifndef __DB_THREAD_MGR_H__
#define  __DB_THREAD_MGR_H__
#include "common/utility.h"
#include "db_dumper.h"
#include "common/ob_string.h"
#include <pthread.h>
#include <semaphore.h>
#include <set>
#include <list>
#include <algorithm>

using namespace oceanbase::common;
using namespace tbsys;

const int kMaxRowkeysInMem = 1000000;
const int kMaxUsedBytes = 1024 * 1024 * 1;
const int kMutiGetKeyNr = 15;

struct TableRowkeyComparor {
  int operator()(const TableRowkey &lhs, const TableRowkey &rhs) {
    if (lhs.table_id != rhs.table_id)
      return lhs.table_id < rhs.table_id;
    return lhs.rowkey < rhs.rowkey;
  }
};

struct MallocAllocator {
  inline char* alloc(int64_t sz) {
    return new(std::nothrow) char[sz];
  }

  inline void free(char *ptr) {
    delete []ptr;
  }
};

class DbThreadMgr : public CDefaultRunnable {
  public:

    typedef std::set<TableRowkey, TableRowkeyComparor>::iterator KeyIterator;
    static DbThreadMgr *get_instance() {
      if (instance_ == NULL)
        instance_ = new(std::nothrow) DbThreadMgr();

      return instance_;
    }

    ~DbThreadMgr() {
      TBSYS_LOG(INFO, "Inserted Lines = %ld, Writen Lines = %ld", inserted_lines_, writen_lines_);
      TBSYS_LOG(INFO, "all keys size %zu", all_keys_.size());
    }

    int destroy() {
      if (running_ == true) {
        stop();
      }

      delete this;
      return OB_SUCCESS;
    }


    int insert_key(const ObRowkey& key, uint64_t table_id, int op, uint64_t timestamp, int64_t seq) {
      int ret = OB_SUCCESS;

      ObRowkey new_key;
      if (clone_rowkey(key, new_key) != OB_SUCCESS) {
        TBSYS_LOG(ERROR, "clone rowkey failed, ret = %d", ret);
        ret = OB_ERROR;
      } else {
        TableRowkey tab_key;
        tab_key.rowkey = new_key;
        tab_key.table_id = table_id;
        tab_key.op = op;
        tab_key.seq = seq;
        tab_key.timestamp = timestamp;

        cond_.lock();
        all_keys_.push_back(tab_key);
        inserted_lines_++;
        
        cond_.unlock();
        cond_.signal();

        ret = OB_SUCCESS;
      }
      
      return ret;
    }

    int clone_rowkey(const ObRowkey &rowkey, ObRowkey &out)
    {
      int ret = OB_SUCCESS;
      ret = rowkey.deep_copy(out, allocator_);
      return ret;
    }

    void free_rowkey(ObRowkey &rowkey)
    {
      allocator_.free((char *)const_cast<ObObj *>(rowkey.ptr()));
    }

    void free_table_keys(TableRowkey *keys, int64_t key_nr) {
      for(int64_t i = 0;i < key_nr; i++) {
        free_rowkey(keys[i].rowkey);
      }
    }

    void dump_keys(TableRowkey *keys, int64_t key_nr, const char *msg) {
      static __thread char buffer[OB_MAX_ROW_KEY_LENGTH];

      for(int64_t i = 0;i < key_nr; i++) {
        int64_t sz = keys[i].rowkey.to_string(buffer, OB_MAX_ROW_KEY_LENGTH);
        buffer[sz] = 0;

        TBSYS_LOG(INFO, "[%s]:rowkey=%s, op=%d, table_id=%ld", msg, buffer, keys[i].op, keys[i].table_id);
      }
    }

    int init(DbDumper *dumper, int muti_get_nr = kMutiGetKeyNr) {
      int ret = OB_SUCCESS;

      if (muti_get_nr > 0 && muti_get_nr <= kMutiGetKeyNr) {
        muti_get_nr_ = muti_get_nr;
      }

      TBSYS_LOG(INFO, "[DbThreadMgr]:muti_get_nr=%d", muti_get_nr_);

      dumper_ = dumper;
      if (dumper_ == NULL) {
        ret = OB_ERROR;
      }

      return ret;
    }

    void run(CThread *thd, void *arg) {
      TableRowkey key;
      int muti_key_nr = 0;
      TableRowkey muti_keys[kMutiGetKeyNr];
      UNUSED(thd);
      UNUSED(arg);

      DbRecordSet rs;
      rs.init();

      while(true) {
        muti_key_nr = 0;
        bool do_work = false;

        cond_.lock();
        while(!_stop && all_keys_.empty())
          cond_.wait();

        if (!all_keys_.empty()) {

          while (!all_keys_.empty() && muti_key_nr < muti_get_nr_) {
            key = all_keys_.front();
            all_keys_.pop_front();
            muti_keys[muti_key_nr++] = key;
          }

          if (muti_key_nr != 0)
            do_work = true;
        } else if (_stop) {
          //all work done
          cond_.unlock();
          break;
        }
        cond_.unlock();

        if (do_work) {
          int max_retries = 20;
          int ret = OB_SUCCESS;
          int back_off = 1;

          while(max_retries-- > 0) {
            //TODO:ret = do dump key, when not success
            ret = dumper_->do_dump_rowkey(muti_keys, muti_key_nr, rs);

            if (ret == OB_SUCCESS) {
              __sync_fetch_and_add(&writen_lines_, muti_key_nr);
              break;
            }

            TBSYS_LOG(INFO, "db_thread_mgr backoff sleeping, time=[%d]", back_off);
            sleep(back_off);

            if ((back_off << 1) < 5) {
              back_off = back_off << 2;
            } else {
              back_off = 5;
            }
          }

          if (ret != OB_SUCCESS) {
            //TODO:dump rowkey
            TBSYS_LOG(ERROR, "Max Retries, skipping records, num=%d", muti_key_nr);
            dump_keys(muti_keys, muti_key_nr, "DUMP FAILED");
          }

          free_table_keys(muti_keys, muti_key_nr);
        }
      }

    }

    void stop() {
      cond_.lock();
      _stop = true;
      running_ = false;
      cond_.unlock();
      cond_.broadcast();

      CDefaultRunnable::wait();
      CDefaultRunnable::stop();
    }

    int start(int max_thread) {
      thread_max_ = max_thread;
      if (thread_max_ == 0)
        return OB_ERROR;

      TBSYS_LOG(INFO, "start thread mgr with thread nr [%d]", max_thread);
      setThreadCount(max_thread);
      CDefaultRunnable::start();
      running_ = true;

      return OB_SUCCESS;
    }

    void wait_completion() {
      bool empty = false;
      TBSYS_LOG(INFO, "start waiting thread mgr");

      while (true) {
        cond_.lock();
        empty = all_keys_.empty();
        cond_.unlock();

        if (empty) {
          TBSYS_LOG(INFO, "finish waiting thread mgr");
          break;
        }
        usleep(10000);
      }
    }

    int64_t inserted_lines() { return inserted_lines_; }
    int64_t writen_lines() { return writen_lines_; }
    int64_t duplicate_lines() { return duplicate_lines_; }

  private:
    DbThreadMgr() {
      running_ = false;
      duplicate_lines_ = inserted_lines_ = writen_lines_ = 0;
      writer_waiting_ = false;
      muti_get_nr_ = kMutiGetKeyNr;
    }

  private:
    DbDumper *dumper_;
    static DbThreadMgr *instance_;
    CThreadMutex mutex_;
    std::list<TableRowkey> all_keys_;

    int thread_max_;
    CThreadCond cond_;
    bool running_;

    int64_t inserted_lines_;
    int64_t writen_lines_;
    int64_t duplicate_lines_;

    bool writer_waiting_;
    CThreadCond insert_cond_;

    int muti_get_nr_;
    MallocAllocator allocator_;
};

#endif
