/* * ===================================================================================== * *       
 * Filename:  db_dumper_writer.cpp 
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
#include "db_dumper_writer.h"
#include "db_thread_mgr.h"
#include "db_utils.h"
#include "common/file_directory_utils.h"
#include "db_file_utils.h"
#include "db_msg_report.h"
#include <time.h>
#include <sstream>

#define MAX_PATH_LEN 1024
#define MAX_DATE_LEN 128

DbDumperWriter::DbDumperWriter(uint64_t id, string path)
{
  id_ = id;
  path_ = path;
  thr_ = NULL;

  running_ = false;
  pushed_lines_ = 0;
  writen_lines_ = 0;

  TBSYS_LOG(INFO, "DbDumperWriter %s", path_.c_str());
  log_id_ = 0;
  file_ = NULL;
}

DbDumperWriter::~DbDumperWriter() 
{
  TBSYS_LOG(INFO, "FILEID:%ld, PUSHEDLEN:%d, WRITENLEN:%d", id_, pushed_lines_,  writen_lines_);
  stop();
  if (file_ != NULL) {
    delete file_;
    file_ = NULL;
  }
  sem_destroy(&sem_empty_);
}

int DbDumperWriter::mv_output_file()
{
  int ret = OB_SUCCESS;
  char src_buf[MAX_PATH_LEN];
  char dst_buf[MAX_PATH_LEN];

  int64_t id = log_id_;
  int len = snprintf(src_buf, MAX_PATH_LEN, "%s/%s/%ld.TMP", 
                     path_.c_str(), current_date_.c_str(), id);
  if (len <= 0) {
    TBSYS_LOG(ERROR, "mv_out_file:src_buf size too short");
    ret = OB_ERROR;
  }

  if (ret == OB_SUCCESS) {
    len = snprintf(dst_buf, MAX_PATH_LEN, "%s/%s/%ld",
                   path_.c_str(), current_date_.c_str(), id);
    if (len <= 0) {
      TBSYS_LOG(ERROR, "mv_out_file:dst_buf size too small");
      ret = OB_ERROR;
    }
  }

  if (ret == OB_SUCCESS) {
    FileDirectoryUtils dir_utils;

    if (!dir_utils.rename(src_buf, dst_buf)) {
      TBSYS_LOG(ERROR, "can't rename %s to %s", src_buf, dst_buf);
      ret = OB_ERROR;
    }
  }

  return ret;
}

//not thread safe
int DbDumperWriter::init_output_files()
{
  int ret = OB_SUCCESS; 

  FileDirectoryUtils dir_utils;
  std::string dir_path = path_ + "/" + current_date_;
  dir_path += "/";

  bool exists = dir_utils.exists(dir_path.c_str());
  if (exists) {
    log_id_ = get_next_file_id();
  } else {
    if (!dir_utils.create_full_path(dir_path.c_str())) {
      TBSYS_LOG(ERROR, "create directory %s failed", dir_path.c_str());
      ret = OB_ERROR;
    } else {
      log_id_ = 0;
    }
  }

  if (ret == OB_SUCCESS) {
    char path[MAX_PATH_LEN];
    int len = snprintf(path, MAX_PATH_LEN, "%s/%s/%ld.TMP", path_.c_str(),
                       current_date_.c_str(), ++log_id_);

    if (len < 0 || len > MAX_PATH_LEN) {
      TBSYS_LOG(ERROR, "when format path error");
      ret = OB_ERROR;
    } else {                                    /* create output file */
      ret = AppendableFile::NewAppendableFile(path, file_);
      if (ret != OB_SUCCESS) {
        TBSYS_LOG(ERROR, "when open file %s", path);
      }
    }
  }

  return ret;
}

void DbDumperWriter::wait_completion(bool rotate_date)
{
  while(true) {
    {
      CThreadGuard guard(&records_lock_);
      if (records_.empty()) {
        TBSYS_LOG(INFO, "waiting tableid:%lu complete writing records", id_);
        break;
      }
    }

    TBSYS_LOG(INFO, "DumperWrite Go to sleep");
    usleep(5000);
  }

  int ret = OB_SUCCESS;

  if (rotate_date)
    ret= rotate_date_dir();
  else 
    ret = rotate_output_file();

  if (ret != OB_SUCCESS) {
    TBSYS_LOG(ERROR, "can't rotate next out file, wait completion failed, quiting");
    stop();
  } else {
    TBSYS_LOG(INFO, "wait complete success");
  }
}

int64_t DbDumperWriter::get_next_file_id()
{
  DIR *log_dir = NULL;
  dirent *file_in_dir = NULL;
  int64_t log_id = 0;

  std::string log_dir_path = path_ + "/" + current_date_;
  log_dir = opendir(log_dir_path.c_str());
  if( NULL == log_dir ) {
    TBSYS_LOG(ERROR, "Can't open path, %s", log_dir_path.c_str());
  } else {
    while((file_in_dir=readdir(log_dir)) && NULL != file_in_dir) {
      const char *file_name = file_in_dir->d_name;

      if (is_number(file_name)) {
        int64_t file_id = atol(file_name);

        if (file_id > log_id)
          log_id = file_id;
      }
    }

    if (log_dir != NULL)
      closedir(log_dir);
  }

  return log_id;
}

//this function is called when writer thread is sleeping
int DbDumperWriter::rotate_date_dir()
{
  char datebuf[MAX_DATE_LEN];
  int ret = OB_SUCCESS;
  get_current_date(datebuf, MAX_DATE_LEN);

  CThreadGuard guard(&records_lock_); 
  //need to change dir
  if (current_date_.compare(datebuf)) {
    std::string tmp_path = path_ + "/" + current_date_ ;
    tmp_path += "/";

    ret = output_files_.output_done(tmp_path);
    if (ret != OB_SUCCESS) {
      TBSYS_LOG(ERROR, "can't write done file, path:%s, %d", tmp_path.c_str(), ret);
      report_msg(MSG_ERROR, "can't write done file, path:%s", tmp_path.c_str());
    }
    TBSYS_LOG(INFO, "max writen files number=[%ld]", log_id_);

    //finish all tmp file
    if (ret == OB_SUCCESS) {
      if (file_ != NULL) {
        file_->Sync();
        delete file_;
        file_ = NULL;
      }

      ret = mv_output_file();
      if (ret != OB_SUCCESS) {
        TBSYS_LOG(ERROR, "can't mv_out_file ");
      }
    }

    if (ret == OB_SUCCESS) {
      //date has changed, rotate to new dir
      current_date_ = datebuf;
      TBSYS_LOG(INFO, "change to new date, %s", current_date_.c_str());
      ret = init_output_files();
      if (ret != OB_SUCCESS) {
        TBSYS_LOG(ERROR, "can't change directory");
      }
    }
  } else {
    TBSYS_LOG(WARN, "can't roate date dir,because date hasn't changed");
  }
  
  return ret;
}

int DbDumperWriter::rotate_output_file()
{
  int len = 0;
  int ret = OB_SUCCESS;
  char file_buf[MAX_PATH_LEN];

  //protected file from write
  CThreadGuard guard(&records_lock_);

  if (file_ != NULL) {
    file_->Sync();
    delete file_;
    file_ = NULL;
  }

  ret = mv_output_file();
  if (ret == OB_SUCCESS) {
    //rotate to next file
    last_rotate_time_ = time(NULL);
    len = snprintf(file_buf, MAX_DATE_LEN, "%s/%s/%ld.TMP", path_.c_str(), 
                   current_date_.c_str(), ++log_id_);
    if (len <= 0) {
      TBSYS_LOG(ERROR, "file buffer is not enough");
      ret = OB_ERROR;
    } else {
      ret = AppendableFile::NewAppendableFile(file_buf, file_);
      if (ret != OB_SUCCESS) {
        TBSYS_LOG(ERROR, "when open file %s", file_buf);
      }
    }
  }

  return ret;
}

bool DbDumperWriter::need_rotate_output_file()
{
  int64_t interval = DUMP_CONFIG->rotate_file_interval();
  time_t now = time(NULL);
  return (now - last_rotate_time_ > interval);
}

int DbDumperWriter::start()
{
  int ret = OB_SUCCESS;

  if (sem_init(&sem_empty_, 0, 0) != 0) {
    TBSYS_LOG(ERROR, "error when calling sem_init");
    ret = OB_ERROR;
  }

  if (DUMP_CONFIG->init_date() == NULL) {
    char datebuf[MAX_DATE_LEN];
    get_current_date(datebuf, MAX_DATE_LEN);

    std::string dir_path = path_ + "/" + datebuf;
    current_date_ = datebuf;
  } else {
    current_date_ = DUMP_CONFIG->init_date();
  }

  TBSYS_LOG(INFO, "[dump writer]:init date=%s", current_date_.c_str());
  if (ret == OB_SUCCESS) {
    ret = init_output_files();
    if (ret != OB_SUCCESS) {
      TBSYS_LOG(ERROR, "when init output files");
    }
  }

  if (ret == OB_SUCCESS) {
    thr_ = new(std::nothrow)  tbsys::CThread();
    if (thr_ == NULL)
      ret = OB_ERROR;
  }

  if (ret == OB_SUCCESS) {
    running_ = true;
    thr_->start(this, NULL);
  }

  return ret;
}

void DbDumperWriter::stop()
{
  running_ = false;
  sem_post(&sem_empty_);
  if (thr_ != NULL) {
    thr_->join();
    delete thr_;
    thr_ = NULL;
  }
}

void DbDumperWriter::run(tbsys::CThread *thr_, void *arg)
{
  last_rotate_time_ = time(NULL);
  UNUSED(thr_);
  UNUSED(arg);

  int ret = OB_SUCCESS;

  while(running_ || !records_.empty()) {
    ret = OB_SUCCESS;
    bool flush = !running_;

    if (need_rotate_output_file()) {
      ret = rotate_output_file();
      if (ret != OB_SUCCESS) {
        TBSYS_LOG(ERROR, "can't rotate file");
        break;
      }
    }

    if (ret == OB_SUCCESS) {
      ret = write_record(flush); 

      if (ret != OB_SUCCESS) {
        TBSYS_LOG(ERROR, "write record failed, %d", ret);
        sleep(1);
      }
    }
  }

  //mv last_file.TMP to last_file
  if (ret == OB_SUCCESS)
    mv_output_file();
}

void DbDumperWriter::free_record(RecordInfo *rec)
{
  char *p = (char *)rec;
  delete [] p;
}

int DbDumperWriter::push_record(const char *data, int lenth)
{
  int ret = OB_SUCCESS;

  RecordInfo *rec = alloc_record(data, lenth);
  if (rec == NULL) {
    TBSYS_LOG(ERROR, "Can't allocate records, len:%d", lenth);
    ret = OB_ERROR;
  } else {
    CThreadGuard gard(&records_lock_);
    pushed_lines_++;
    records_.push_back(rec);
    sem_post(&sem_empty_);
  }

  return ret;
}

int DbDumperWriter::write_record(bool flush)
{
  int ret = OB_SUCCESS;
  RecordInfo * rec = NULL; 
  struct timespec timeout;
  UNUSED(flush);

  timeout.tv_sec = time(NULL) + kSemWaitTimeout;
  timeout.tv_nsec = 0;

  if (!records_.empty()) {
    CThreadGuard gard(&records_lock_);
    //no problem doing this, because only one thread is waiting on the queue
    rec = records_.front();
  } else if (running_) {
    sem_timedwait(&sem_empty_, &timeout);
  }

  if (rec != NULL) {
    CThreadGuard gard(&records_lock_);

    if (file_ == NULL || (ret = file_->Append(rec->buf, rec->length)) != OB_SUCCESS) {
      TBSYS_LOG(ERROR, "Write record failed, ret:%d, path:%s, len:%d", ret, path_.c_str(), rec->length);
    } else {
      writen_lines_++;
      records_.pop_front();
      free_record(rec);
    }
  }

  return ret;
}

DbDumperWriter::RecordInfo *DbDumperWriter::alloc_record(const char *data, int len)
{
  char *p = new(std::nothrow) char[len + sizeof(RecordInfo)];
  if (p != NULL) {
    memcpy(((RecordInfo *)p)->buf, data, len);
    ((RecordInfo *)p)->length = len;
  }
  return (RecordInfo *)p;
}
