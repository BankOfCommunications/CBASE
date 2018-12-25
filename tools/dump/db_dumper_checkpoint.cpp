/*
 * =====================================================================================
 *
 *       Filename:  db_dumper_checkpoint.cpp
 *
 *    Description:  
 *
 *        Version:  1.0
 *        Created:  06/15/2011 03:57:36 PM
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  yushun.swh (), yushun.swh@taobao.com
 *        Company:  taobao
 *
 * =====================================================================================
 */
#include "db_dumper_checkpoint.h"
#include "common/file_utils.h"
#include "common/ob_define.h"
#include <string>

using namespace oceanbase::common;

const int kCheckpointLen = 32;

DbCheckpoint::~DbCheckpoint()
{
}

void DbCheckpoint::init(std::string name, std::string path)
{
  ckp_name_ = name;
  path_ = path + "/" + name;
}

int DbCheckpoint::load_checkpoint()
{
  int ret = OB_SUCCESS;
  FileUtils file;
  bool get_from_rpc = false;

  int fd = file.open(path_.c_str(), O_RDONLY);
  if (fd < 0) {
    TBSYS_LOG(WARN, "open %s error", path_.c_str());
    TBSYS_LOG(INFO, "get checkpoint from rpc");
    ret = OB_ERROR;
    get_from_rpc = true;
  } else {
    char buff[kCheckpointLen];
    int len = static_cast<int32_t>(file.read(buff, kCheckpointLen));
    if (len <= 0) {
      TBSYS_LOG(ERROR, "read %s error", ckp_name_.c_str());
      ret = OB_ERROR;
    } else {
      buff[len] = '\0';
      id_ = atol(buff);
      ret = OB_SUCCESS;
    }
  }

  if (get_from_rpc) {
    //TODO:get from rpc. then write checkpoint on disk
  }

  return ret;
}

void DbCheckpoint::run(CThread *thr, void *arg)
{
  UNUSED(thr);
  UNUSED(arg);
}

int DbCheckpoint::do_checkpoint_local(int64_t log_id)
{
  FileUtils file;
  int ret = OB_SUCCESS;

  int fd = file.open(path_.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 
                      S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH);

  if (fd < 0) {
    TBSYS_LOG(ERROR, "error when open %s", path_.c_str());
    ret = OB_ERROR;
  } else {
    char buff[kCheckpointLen];

    int len = snprintf(buff, kCheckpointLen, "%ld", log_id);
    if (len < 0) {
      TBSYS_LOG(ERROR, "sprintf error");
      ret = OB_ERROR;
    } else if (len > kCheckpointLen) {
      TBSYS_LOG(ERROR, "id_ is too long for the buffer");
      ret = OB_ERROR;
    } else {
      if (file.write(buff, len, true) == len) {
        id_ = log_id;
        TBSYS_LOG(INFO, "write checkpoint successfully");
        ret = OB_SUCCESS;
      } else {
        TBSYS_LOG(ERROR, "write checkpoint failed");
        ret = OB_ERROR;
      }
    }
  }

  return ret;
}


int DbCheckpoint::do_checkpoint_remote(int64_t log_id)
{
  UNUSED(log_id);
  int ret = OB_SUCCESS;
  //TODO:add thrift support
  return ret;
}

//write log_id increase order
int DbCheckpoint::write_checkpoint(int64_t log_id)
{
  int ret = OB_SUCCESS;

  //write checkpoint
  ret = do_checkpoint_remote(log_id);
  if (ret != OB_SUCCESS) {
    TBSYS_LOG(ERROR, "can't write remote checkpoint");
  } else {
    ret = do_checkpoint_local(log_id);
    if (ret != OB_SUCCESS)
      TBSYS_LOG(ERROR, "can't write local checkpoint");
  }

  return ret;
}
