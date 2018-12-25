#include <sys/types.h>
#include <sys/stat.h>
#include <math.h>
#include <dirent.h>
#include <iostream>
#include <pthread.h>
#include "common/ob_repeated_log_reader.h"
#include "common/utility.h"
#include "common/hash/ob_hashmap.h"
#include "updateserver/ob_ups_mutator.h"
#include "updateserver/ob_ups_utils.h"
#include "db_utils.h"
#include "db_parse_log.h"
#include "db_log_monitor.h"
#include "db_msg_report.h"
#include "db_dumper_checkpoint.h"

#include <string>
#include <sstream>
#include <map>

using namespace std;
using namespace oceanbase::common;
using namespace oceanbase::common::hash;
using namespace oceanbase::updateserver;

#define DATE_MAX 31
#define THREAD_MAX 4
#define FILENUM_PER_THREAD 1024

DbLogParser::DbLogParser(std::string &path, DbLogMonitor *monitor, DbCheckpoint *ckp) 
{
  log_path_ = path;
  thr_ = NULL;  
  running_ = false;
  monitor_ = monitor;
  handler_ = NULL;
  ckp_ = ckp;
  if (ckp_ == NULL) {
    TBSYS_LOG(WARN, "checkpoint disabled");
  }
}

DbLogParser::~DbLogParser()
{
  if (thr_) {
    stop_parse();
  }
}

int DbLogParser::remove_log_file(int64_t log_id)
{
  char buf[1024];
  int ret = OB_SUCCESS;

  int len = snprintf(buf, 1024, "%s/%ld", log_path_.c_str(), log_id);
  if (len < 0) {
    ret = OB_ERROR;
  } else {
    if (unlink(buf) != 0) {
      ret = OB_ERROR;
    }
  }

  return ret;
}

int DbLogParser::parse_log(int64_t log_id)
{
  LogCommand cmd;

  int ret = OB_SUCCESS;
  uint64_t log_seq;
  int64_t data_len;
  char* log_data;

  ObRepeatedLogReader reader;
  long count = 0;
  int64_t start_time = gen_usec();

  ret = reader.init(log_path_.c_str());
  if (OB_SUCCESS != ret) {
    TBSYS_LOG(ERROR, "can't open directory, due to %d", ret);
    ret = OB_ERROR;
  }

  if (ret == OB_SUCCESS) {
    TBSYS_LOG(INFO, "Using LOG:%ld", log_id);
    ret = reader.open(log_id);
    if (OB_SUCCESS != ret) {
      TBSYS_LOG(ERROR, "can't open log %ld, due to %d", log_id, ret);
    }
  }


  if (ret == OB_SUCCESS && handler_) {
    handler_->start_process();
  }

  if (ret == OB_SUCCESS) {
    ret = reader.read_log(cmd, log_seq, log_data, data_len);
    if (ret != OB_SUCCESS) {
      TBSYS_LOG(ERROR, "read log failed");
    }
  }

  while (OB_SUCCESS == ret) {
    int64_t pos = 0;

    //TODO: when meet ups switch commitlog, refresh schema 
    if (OB_LOG_UPS_MUTATOR == cmd) {
      ObUpsMutator mut;

      ret = mut.deserialize(log_data, data_len, pos);
      if (OB_SUCCESS != ret) {
        TBSYS_LOG(ERROR, "deserializing ObUpsMutator failed");
        break;
      }

      ObMutatorCellInfo* cell = NULL;
      bool first_rowkey= true;
      int last_ext = 0;
      ObCellInfo last_cell;

      while (OB_SUCCESS == (ret = mut.next_cell())) {
        ret = mut.get_cell(&cell);
        if (OB_SUCCESS != ret || NULL == cell) {
          TBSYS_LOG(ERROR, "ObUpsMutator get cell failed");
          break;
        }

        if (mut.is_freeze_memtable()) {
          //skip
        } else if (mut.is_drop_memtable()) {
          //skip
        } else {
          if (ObActionFlag::OP_USE_OB_SEM == cell->cell_info.value_.get_ext()) {
            //skip
          } else if (ObActionFlag::OP_USE_DB_SEM == cell->cell_info.value_.get_ext()) {
            //skip
          } else {
            int64_t ext = cell->cell_info.value_.get_ext();

            if (ext != ObActionFlag::OP_DEL_ROW)
              ext = cell->op_type.get_ext();

            /* if this is a a valid operation */
            if (ext == ObActionFlag::OP_UPDATE || ext == ObActionFlag::OP_INSERT ||
                ext == ObActionFlag::OP_DEL_ROW) {
              if (first_rowkey || last_cell.row_key_ != cell->cell_info.row_key_ ||
                  last_cell.table_id_ != cell->cell_info.table_id_ ||
                  last_cell.table_name_ != cell->cell_info.table_name_ || ext != last_ext) {
//                uint64_t timestamp = mut.get_mutate_timestamp();

                count++;
                first_rowkey = false;
                last_cell = cell->cell_info;
                last_ext = static_cast<int32_t>(ext);

                if (handler_) {
                  if (handler_->need_seq())
                    handler_->process_rowkey(cell->cell_info, static_cast<int32_t>(ext), gen_usec(), handler_->get_next_seq());
                  else
                    handler_->process_rowkey(cell->cell_info, static_cast<int32_t>(ext), gen_usec());
                }
              }
            } else {
              TBSYS_LOG(ERROR, "OB/DB operation deserialize error, op_type=%ld, cell_type=%d", 
                        cell->op_type.get_ext(), cell->cell_info.value_.get_type());
              ret = OB_ERROR;
            }
          }
        } 
      }
    }

    ret = reader.read_log(cmd, log_seq, log_data, data_len);
  }

  TBSYS_LOG(INFO, "LOG_ID:%ld, ROWKEY COUNT:%ld, TIME ELAPSED(us):%ld", 
            log_id, count, gen_usec() - start_time);

  if (handler_)
    handler_->end_process();

  if (OB_READ_NOTHING != ret) {
    TBSYS_LOG(ERROR, "parsing log[:%ld:] failed, [retcode = %d]", log_id, ret);
    report_msg(MSG_ERROR, "parsing log [%d], please revise it", log_id);
  } else {
    ret = OB_SUCCESS;
  }

  reader.close();

  return ret;
}

void DbLogParser::run(tbsys::CThread *thread, void *args)
{
  int64_t log_id;
  int ret = OB_SUCCESS;
  UNUSED(thread);
  UNUSED(args);

  TBSYS_LOG(INFO, "starting log parser thread");
  while (running_ == true && (ret = monitor_->fetch_log(log_id)) == OB_SUCCESS) {
    TBSYS_LOG(INFO, "succeed fetch a log, id=%ld", log_id);
    ret = parse_log(log_id);
    if (ret != OB_SUCCESS) {
      //get log file from nas directly
      ret = monitor_->get_nas_log(log_id);
      if (ret == OB_SUCCESS) {
        //parse it again
        ret = parse_log(log_id);
        if (ret != OB_SUCCESS) {
          TBSYS_LOG(ERROR, "can't parse log file, id=%ld", log_id);
          report_msg(MSG_ERROR, "can't parse log file, id=%ld", log_id);
        }
      } else {
        TBSYS_LOG(ERROR, "can't get log[%ld] from nas", log_id);
      }
    } else {                                    /* parse log success */
      if (remove_log_file(log_id) != OB_SUCCESS) {
        TBSYS_LOG(ERROR, "can't remove log id");
      }
    }

    if (ret == OB_SUCCESS) {
      bool need_rotate_date = monitor_->is_lastlog_of_day(log_id);
      handler_->wait_completion(need_rotate_date);
    }

    //even if failed parse log_id, 
    //we still write the checkpoint to record that log_id has been parsed
    if (ret == OB_SUCCESS && ckp_ != NULL) {
      ret = ckp_->write_checkpoint(log_id);
      if (ret != OB_SUCCESS) {
        TBSYS_LOG(ERROR, "can't write checkpoint");
        report_msg(MSG_ERROR, "can't write checkpoint");
      }
    }
  }

  TBSYS_LOG(INFO, "parser thread quiting");
}

int DbLogParser::start_parse(RowkeyHandler *handler)
{
  int ret = OB_SUCCESS;
  if (thr_ != NULL) {
    thr_->join();
    delete thr_;
  }

  handler_ = handler;

  thr_ = new(std::nothrow) tbsys::CThread();
  if (thr_ != NULL) {
    thr_->start(this, NULL);
    running_ = true;
  } else {
    TBSYS_LOG(ERROR, "can't alloc thread");
    ret = OB_ERROR;
  }

  return ret;
}


void DbLogParser::stop_parse()
{
  running_ = false;
  if (thr_ != NULL) {
    thr_->join();
    delete thr_;
    thr_ = NULL;
  }
}
