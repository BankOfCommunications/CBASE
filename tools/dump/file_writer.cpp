// =====================================================================================
//
//       Filename:  file_writer.cpp
//
//    Description:  
//
//        Version:  1.0
//        Created:  2014年11月14日 09时56分58秒
//       Revision:  none
//       Compiler:  g++
//
//         Author:  liyongfeng
//   Organization:  ecnu DaSE
//
// =====================================================================================


#include <time.h>
#include "file_writer.h"
#include "tbsys.h"
#include "common/ob_define.h"

int64_t kDefaultMaxFileSize = 1 * 1024 * 1024 * 1024;//1024MB
int64_t kDefaultMaxRecordNum = 1 * 10 * 100 * 100 * 100;//10Million

FileWriter::FileWriter()
{
  filename_ = NULL;
  max_file_size_ = DEFAULT_MAX_FILE_SIZE;
  max_record_num_ = DEFAULT_MAX_REOCRD_NUM;
  cur_file_size_ = 0;
  cur_record_num_ = 0;

  flag_ = 1;
  file_ = NULL;
}

FileWriter::~FileWriter()
{
  if (file_ != NULL) {
    fclose(file_);
  }

  free(filename_);
}

void FileWriter::set_max_file_size(const int64_t &max_file_size)
{
  if (max_file_size > 0) {
    max_file_size_ = max_file_size;
  }

  flag_ = 1;
}

void FileWriter::set_max_record_count(const int64_t &max_record_num)
{
  if (max_record_num > 0) {
    max_record_num_ = max_record_num;
  }

  flag_ = 0;
}

int FileWriter::init_file_name(const char *filename)
{
  int ret = 0;
  if (NULL == filename) {
    TBSYS_LOG(ERROR, "input filename is NULL pointer");
    return -1;
  }

  if (file_ != NULL){
    fclose(file_);
  }

  free(filename_);
  filename_ = NULL;
  cur_file_size_ = 0;
  cur_record_num_ = 0;

  filename_ = strdup(filename);
  file_ = fopen(filename_, "w+");
  if (NULL == file_) {
    ret = -1;
    TBSYS_LOG(ERROR, "can't open file, name=%s", filename_);
  }

  return ret;
}

int FileWriter::rotate_data_file(const char *filename, const char *fmt)
{
  //sleep(10);
  int ret = 0;
  if (NULL == filename && filename_ != NULL) {
    filename = filename_;
  }

  if (0 == access(filename, R_OK)) {
    char old_data_file[256];
    time_t t;
    time(&t);
    struct tm tm;
    localtime_r((const time_t*)&t, &tm);
    if (fmt != NULL) {
      char tmptime[256];
      strftime(tmptime, sizeof(tmptime), fmt, &tm);
      sprintf(old_data_file, "%s.%s", filename, tmptime);
    } else {
      sprintf(old_data_file, "%s.%04d%02d%02d%02d%02d%02d", 
          filename, tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday, tm.tm_hour, tm.tm_min, tm.tm_sec);
    }
    rename(filename, old_data_file);
  }

  fclose(file_);
  file_ = NULL;
  
  file_ = fopen(filename, "w+");
  if (NULL == file_) {
    ret = -1;
    TBSYS_LOG(ERROR, "can't open file, name=%s", filename_);
  }

  return ret;
}

int FileWriter::write_records(const char *buffer, const int64_t &length, const int64_t &num)
{
  tbsys::CThreadGuard guard(&file_lock_);
  int ret = 0;

  if (NULL == buffer || length <= 0) {
    TBSYS_LOG(ERROR, "records buffer is NULL or empty");
    return -1;
  }
  //首先判断是否要分割新文件
  if(flag_) {//如果falg_ = 1,采用文件大小进行分割
    if (cur_file_size_ + length > max_file_size_) {
      //添加rotate_data_file
      rotate_data_file(filename_);
      cur_file_size_ = 0;
    }
  } else {//否则,采用文件记录数进行分割
    if (cur_record_num_ + num > max_record_num_) {
      //添加rotate_data_file
      rotate_data_file(filename_);
      cur_record_num_ = 0;
    }
  }

  size_t count = fwrite(buffer, length, 1, file_);
  if (count != 1) {
    TBSYS_LOG(DEBUG, "write records buffer into file[%s] failed", filename_);
    ret = -1;
  } else {
    cur_file_size_ += length;
    cur_record_num_ += num;
    TBSYS_LOG(DEBUG, "write reocrds buffer into file, buffer length=[%ld], record num=[%ld], cur file size=[%ld], cur record num=[%ld]", length, num, cur_file_size_, cur_record_num_);
  }
  return ret;
}
