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

#ifndef  OB_API_DB_DUMPER_INC
#define  OB_API_DB_DUMPER_INC
#include "oceanbase_db.h"
#include "db_record_set.h"
#include "db_parse_log.h"
#include "db_dumper_config.h"
#include "common/file_utils.h"
#include "common/thread_buffer.h"
#include "db_dumper_writer.h"

#include <pthread.h>
#include <semaphore.h>
#include <vector>
#include <list>
#include <deque>
#include <string>
#include <set>
#include <algorithm>

using namespace oceanbase::common;
using namespace oceanbase::api;
using namespace tbsys;

class DbDumperMgr;

const int kRecordSize = 2 * 1024 * 1024;
const int kWriterBuffSize = 16 * 1024 * 1024;
const int kRowkeyPerThd = 5000;

class DbDumperWriteHandler;

struct TableRowkey {
  ObRowkey rowkey;
  uint64_t table_id;
  uint64_t timestamp;
  int64_t seq;
  int op;
};

class DbDumper : public RowkeyHandler {
  public:
    struct DbTableDumpWriter {
      uint64_t table_id;
      DbDumperWriteHandler *dumper;
    };

  public:
    DbDumper();

    ~DbDumper(); 

    int init(OceanbaseDb *db);

    virtual bool need_seq() { return true; }

    virtual int process_rowkey(const ObCellInfo &cell, int op, uint64_t timestamp, int64_t seq);

    virtual int do_dump_rowkey(const TableRowkey *rowkeys, const int64_t size,
                               DbRecordSet &rs); 

    virtual int db_dump_rowkey(const TableRowkey *rowkeys, const int64_t size,
                               DbRecordSet &rs); 

    int pack_record(const TableRowkey *rowkeys, const int64_t size,
                    DbRecordSet &rs);

    int64_t merge_get_req(const TableRowkey *rowkeys, const int64_t size, TableRowkey *merged);

    virtual int64_t get_next_seq();

    void wait_completion(bool rotate_date);

    int dump_del_key(const TableRowkey &key);

  private:
    int handle_del_row(const DbTableConfig *cfg,const ObRowkey &rowkey, int op, uint64_t timestamp, int64_t seq);
    int push_record(uint64_t table_id, const char *rec, int len);

    int find_table_key(const TableRowkey *rowkeys, const int64_t size,
                       const ObRowkey&rowkey, TableRowkey &table_key);

    int append_del_keys(const TableRowkey *req_keys, const int64_t req_key_size,
                        const TableRowkey *res_keys, const int64_t res_key_size);

    int setup_dumpers();
    void destroy_dumpers();
    OceanbaseDb *db_;
    DbRecordSet rs_;

    std::vector<DbTableDumpWriter> dump_writers_;

    CThreadMutex rowkeys_mutex_;
    int64_t total_keys_;

    ThreadSpecificBuffer record_buffer_;
    pthread_spinlock_t seq_lock_;
    int64_t seq_;
};

#endif   /* ----- #ifndef db_dumper_INC  ----- */
