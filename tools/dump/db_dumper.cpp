/* * ===================================================================================== * *       
 * Filename:  db_dumper.cpp * *    Description:  
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
#include "db_utils.h"
#include "db_thread_mgr.h"
#include "db_record_formator.h"
#include "common/ob_action_flag.h"
#include "updateserver/ob_ups_utils.h"
#include <sstream>

static int64_t del_lines = 0;
static int64_t insert_lines = 0;

int DbDumper::process_rowkey(const ObCellInfo &cell, int op, uint64_t timestamp, int64_t seq) 
{
  int ret = OB_SUCCESS;

  const DbTableConfig *cfg = NULL;

  ret = DUMP_CONFIG->get_table_config(cell.table_id_, cfg);
  if (ret != OB_SUCCESS) {
    TBSYS_LOG(DEBUG, "no such table, table_id=%ld", cell.table_id_);
  } else {
    DbThreadMgr::get_instance()->insert_key(cell.row_key_, cell.table_id_, op, timestamp, seq);
    __sync_add_and_fetch(&total_keys_, 1);
  }

  return ret;
}

void DbDumper::wait_completion(bool rotate_date)
{
  //wait worker stop
  DbThreadMgr::get_instance()->wait_completion();

  //wait writer stop
  DbTableDumpWriter writer;
  for(size_t i = 0;i < dump_writers_.size(); i++) {
    writer = dump_writers_[i];
    if (writer.dumper) {
      writer.dumper->wait_completion(rotate_date);
    }
  }
}

int DbDumper::setup_dumpers()
{
  int ret = OB_SUCCESS;
  std::vector<DbTableConfig> &cfgs = DUMP_CONFIG->get_configs();
  const std::string &output = DUMP_CONFIG->get_output_dir();

  for(size_t i = 0; i < cfgs.size(); i++) {
    DbTableDumpWriter writer;

    std::string table_dir = output + "/";
    table_dir += cfgs[i].table();

    writer.table_id = cfgs[i].table_id();
    writer.dumper = new(std::nothrow) DbDumperWriter(writer.table_id, table_dir);
    if (writer.dumper == NULL) {
      TBSYS_LOG(ERROR, "Can't create dumper, allocate memory failed");
      ret = OB_ERROR;
      break;
    }

    dump_writers_.push_back(writer);
  }

  if (ret == OB_SUCCESS) {
    for(size_t i = 0;i < dump_writers_.size(); i++) {
      ret = dump_writers_[i].dumper->start();
      if (ret != OB_SUCCESS) {
        TBSYS_LOG(ERROR, "start up dumper error, ret=%d", ret);
        break;
      }
    }
  }
  return ret;
}

int DbDumper::init(OceanbaseDb *db)
{
  int ret = OB_SUCCESS;
  db_ = db;

  if (db_ == NULL) {
    TBSYS_LOG(ERROR, "database or schema is NULL");
    ret = OB_ERROR;
  } else {
    ret = rs_.init();
  }

  if (ret == OB_SUCCESS) {
    if (pthread_spin_init(&seq_lock_, PTHREAD_PROCESS_PRIVATE) != 0) {
      TBSYS_LOG(ERROR, "can't init spin lock");
      ret = OB_ERROR;
    }
  }

  if (ret == OB_SUCCESS) {
    ret = setup_dumpers();
  } else {
    TBSYS_LOG(ERROR, "failed when init DbDumper, [retcode=%d]", ret);
    ret= OB_ERROR;
  }

  return ret;
}

DbDumper::DbDumper() {
  total_keys_ = 0;
  seq_ = 0;
}

DbDumper::~DbDumper() 
{
  TBSYS_LOG(INFO, "waiting dump writer thread stop");
  destroy_dumpers();
  pthread_spin_destroy(&seq_lock_);
  TBSYS_LOG(INFO, "Total rowkyes = %ld", total_keys_);
  TBSYS_LOG(INFO, "del_lines= %ld, insert_lines=%ld", del_lines, insert_lines);
}

void DbDumper::destroy_dumpers()
{
  for(size_t i = 0;i < dump_writers_.size(); i++) {
    if (dump_writers_[i].dumper != NULL)
      delete dump_writers_[i].dumper;
  }
  dump_writers_.clear();
}

int64_t DbDumper::merge_get_req(const TableRowkey *rowkeys, const int64_t size, TableRowkey *merged)
{
  memcpy(merged, rowkeys, sizeof(TableRowkey) * size);

  if (size < 2)
    return size;

  int64_t new_size = 0;
  merged[new_size++] = rowkeys[size - 1];

  for (int64_t i = size - 2;i >= 0; i--) {
    if (rowkeys[i].rowkey != rowkeys[i + 1].rowkey || 
        rowkeys[i].table_id != rowkeys[i + 1].table_id) {
      merged[new_size++] = rowkeys[i];
    } else {
      if (rowkeys[i].timestamp > rowkeys[i + 1].timestamp) {
        TBSYS_LOG(WARN, "currupted log files, mutations are not in order, time1=%ld, time=%ld",
                  rowkeys[i].timestamp, rowkeys[i + 1].timestamp);
      }
    }
  }

  return new_size;
}

int DbDumper::db_dump_rowkey(const TableRowkey *rowkeys, const int64_t size, DbRecordSet &rs)
{

  int ret = OB_SUCCESS;
  const DbTableConfig *cfg = NULL;

  std::vector<DbMutiGetRow> rows;
  TableRowkey merged_keys[kMutiGetKeyNr];

  int64_t new_size = merge_get_req(rowkeys, size, merged_keys);
  for(int64_t i = 0;i < new_size; i++) {
    DbMutiGetRow row;

    ret = DUMP_CONFIG->get_table_config(merged_keys[i].table_id, cfg);
    if (ret != OB_SUCCESS) {
      TBSYS_LOG(WARN, "no such table");
      break;
    }

    row.table = cfg->table();
    row.rowkey = merged_keys[i].rowkey;
    row.columns = &(cfg->get_columns());

    rows.push_back(row);
  }

  if (ret == OB_SUCCESS) {
    ret = db_->get(rows, rs);
    if (ret == OB_SUCCESS) {
      //wrap the record, push it in the dumper queue
      ret = pack_record(merged_keys, new_size, rs);
      if (ret != OB_SUCCESS) {
        TBSYS_LOG(ERROR, "pack record error");
      }
    } else {
      TBSYS_LOG(ERROR, "can't get data from database");
    }
  }

  return ret;
}

int DbDumper::dump_del_key(const TableRowkey &key)
{
  int ret = OB_SUCCESS;
  const DbTableConfig *cfg = NULL;

  ret = DUMP_CONFIG->get_table_config(key.table_id, cfg);
  if (ret != OB_SUCCESS) {
    TBSYS_LOG(ERROR, "no table find, table_id=%lu", key.table_id);
  } else {
    __sync_add_and_fetch(&del_lines, 1);
    ret = handle_del_row(cfg, key.rowkey, ObActionFlag::OP_DEL_ROW, key.timestamp, key.seq);
  }

  return ret;
}

int DbDumper::handle_del_row(const DbTableConfig *cfg,const ObRowkey &rowkey, int op, uint64_t timestamp, int64_t seq)
{
  int ret = OB_SUCCESS;
  ThreadSpecificBuffer::Buffer *buffer = record_buffer_.get_buffer();
  buffer->reset();
  ObDataBuffer data_buff(buffer->current(), buffer->remain());

  //deleted record,just append header
  UniqFormatorHeader header;
  ret = header.append_header(rowkey, op, timestamp, seq, DUMP_CONFIG->app_name(), cfg->table(), data_buff);
  if (ret != OB_SUCCESS) {
    TBSYS_LOG(ERROR, "Unable seiralize header, due to [%d], skip this line", ret);
  } 

  if (ret == OB_SUCCESS) {
    int len = append_end_rec(data_buff);
    if (len > 0) {
      ret = OB_SUCCESS;
    } else {
      ret = OB_ERROR;
    }
  }

  if (ret == OB_SUCCESS) {
    char *data = data_buff.get_data();
    int64_t len = data_buff.get_position();

    ret = push_record(cfg->table_id(), data, static_cast<int32_t>(len));
    if (ret != OB_SUCCESS) {
      TBSYS_LOG(ERROR, "can't push record to dumper writer queue, rec len=%ld", len);
    }
  }

  return ret;
}

int DbDumper::do_dump_rowkey(const TableRowkey *rowkeys, const int64_t size, DbRecordSet &rs) 
{
  int ret = OB_SUCCESS;

  ret = db_dump_rowkey(rowkeys, size, rs);

  return ret;
}

int DbDumper::push_record(uint64_t table_id, const char *rec, int len)
{
  int ret = OB_SUCCESS;
  for(size_t i = 0;i < dump_writers_.size(); i++) {
    if (dump_writers_[i].table_id == table_id) {
      ret = dump_writers_[i].dumper->push_record(rec, len);
      break;
    }
  }

  return ret;
}

#if 1
void dump_scanner(ObScanner &scanner);
#endif

int DbDumper::pack_record(const TableRowkey *rowkeys, const int64_t size,
                          DbRecordSet &rs) 
{
  int ret = OB_SUCCESS;
  TableRowkey exist_keys[kMutiGetKeyNr];
  int64_t exist_key_nr = 0;

  ThreadSpecificBuffer::Buffer *buffer = record_buffer_.get_buffer();
  buffer->reset();
  ObDataBuffer data_buff(buffer->current(), buffer->remain());

  const DbTableConfig *cfg = NULL;
  TableRowkey table_key;
  ObRowkey rowkey;

#if 0
  dump_scanner(rs.get_scanner());
  TBSYS_LOG(INFO, "DUMP STARTING KEYS");
  for(int64_t i = 0;i < size; i++) {
    char buf[128];
    int len = hex_to_str(rowkeys[i].rowkey.ptr(), rowkeys[i].rowkey.length(), buf, 128);
    buf[2 * len] = 0;
    TBSYS_LOG(INFO, "KEY%d:%s", i, buf);
  }
#endif

  DbRecordSet::Iterator itr = rs.begin();
  while(ret == OB_SUCCESS && itr != rs.end()) {
    data_buff.get_position() = 0;

    DbRecord *recp;
    ret = itr.get_record(&recp);
    if (ret != OB_SUCCESS || recp == NULL) {
      TBSYS_LOG(ERROR, "can't get record, due to[%d]", ret);
      break;
    }

    if (recp->empty()) {                      /* a deleted record call handle del */
      itr++;                                      /* step to next row */
      continue;
    }

    if (recp->get_rowkey(rowkey) != OB_SUCCESS) {
      TBSYS_LOG(ERROR, "muti thread processing same DbRecordSet?");
      break;
    }

#if 0
    {
      char buf[128];

      int len = hex_to_str(rowkey.ptr(), rowkey.length(), buf, 128);
      buf[2 * len] = 0;
      TBSYS_LOG(INFO, "resp, key=%s", buf);
    }
#endif

    ret = find_table_key(rowkeys, size, rowkey, table_key);
    if (ret != OB_SUCCESS) {
      TBSYS_LOG(ERROR, "can't find ds rowkey in req rowkeys, currupted memory?");
      break;
    }

    assert(exist_key_nr < kMutiGetKeyNr);
    exist_keys[exist_key_nr++] = table_key;     /* setup exist array */

    ret = DUMP_CONFIG->get_table_config(table_key.table_id, cfg);
    //1.filter useless column
    if (ret == OB_SUCCESS && cfg->filter()) {
      bool skip = (*cfg->filter())(recp);
      if (skip) {
        itr++;                                      /* step to next row */
        continue;
      }
    }

    //2.append output header
    if (ret == OB_SUCCESS) {
      UniqFormatorHeader header;
      ret = header.append_header(table_key.rowkey, table_key.op, table_key.timestamp,
                                 table_key.seq, DUMP_CONFIG->app_name(), 
                                 cfg->table(), data_buff);
      if (ret != OB_SUCCESS) {
        TBSYS_LOG(ERROR, "Unable seiralize header, due to [%d], skip this line", ret);
      } else {
        ret = append_header_delima(data_buff);
      }
    }

    //3.append record
    if (ret == OB_SUCCESS) {
      DbRecordFormator formator;
      ret = formator.format_record(table_key.table_id, recp, table_key.rowkey, data_buff);
      if (ret != OB_SUCCESS) {
        TBSYS_LOG(ERROR, "Unable seiralize record, due to [%d], skip this line", ret);
      }
#if 1
      if (ret != OB_SUCCESS) {
        DbRecordSet::Iterator itr1 = rs.begin();
        while (itr1 != rs.end()) {
          TBSYS_LOG(INFO, "New Record Start");
          DbRecord *rec1;
          ret = itr1.get_record(&rec1);
          if (ret != OB_SUCCESS) {
            TBSYS_LOG(ERROR, "DBG:can't get record");
          } else {
            rec1->dump();
          }

          itr1++;
        }
      }
#endif

    }

    if (ret == OB_SUCCESS) {
      char *data = data_buff.get_data();
      int64_t len = data_buff.get_position();

      //4.push to dumper writer queue
      ret = push_record(table_key.table_id, data, static_cast<int32_t>(len));
      if (ret != OB_SUCCESS) {
        TBSYS_LOG(ERROR, "can't push record to dumper writer queue, rec len=%ld", len);
      }
    }

    itr++;                                      /* step to next row */
  }

  if (ret == OB_SUCCESS) {
    ret = append_del_keys(rowkeys, size, exist_keys, exist_key_nr);
  }

  return ret;
}

int DbDumper::find_table_key(const TableRowkey *rowkeys, const int64_t size,
                             const ObRowkey &rowkey, TableRowkey &table_key)
{
  int ret = OB_SUCCESS;

  int64_t i = 0;
  for (;i < size;i++) {
    if (rowkeys[i].rowkey == rowkey) {
      table_key = rowkeys[i];
      break;
    }
  }

  if (i == size) {
    ret = OB_ERROR;
  }

  return ret;
}

int DbDumper::append_del_keys(const TableRowkey *req_keys, const int64_t req_key_size,
                              const TableRowkey *res_keys, const int64_t res_key_size)
{
  int ret = OB_SUCCESS;

  if (req_key_size == res_key_size)
    return ret;

  for(int64_t i = 0;i < req_key_size; i++) {
    int idx = 0;
    for (;idx < res_key_size && req_keys[i].rowkey != res_keys[idx].rowkey; idx++);

    if (idx == res_key_size) {
#if 0
      char buf[128];

      int len = hex_to_str(req_keys[i].rowkey.ptr(), req_keys[i].rowkey.length(), buf, 128);
      buf[2 * len] = 0;
      TBSYS_LOG(INFO, "meet deleted key, key=%s", buf);
#endif
      ret = dump_del_key(req_keys[i]);
      if (ret != OB_SUCCESS) {
        TBSYS_LOG(WARN, "can't dump_del_key");
        break;
      }
    }
  }

  return ret;
}


int64_t DbDumper::get_next_seq()
{
  __sync_add_and_fetch(&seq_, 1);
  return seq_;
}
