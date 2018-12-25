#include "hdfs_env.h"
#include "common/utility.h"
#include <cassert>

using namespace oceanbase::tools;
using namespace std;

HdfsWritableFile::~HdfsWritableFile()
{
  if (fd_ != NULL)
    close();
  env_->unref();
}

int HdfsWritableFile::append(const common::ObDataBuffer &data)
{
  int remain = data.get_position();
  const char *outp = data.get_data();
  int len = 0;

  TBSYS_LOG(DEBUG, "HDFS fs handle = %ld, fd = %ld", (long)env_->fs(), (long)fd_);
  while (remain > 0)
  {
    int nwrite = hdfsWrite(env_->fs(), fd_, outp, remain);
    if (nwrite < 0)
    {
      TBSYS_LOG(ERROR, "hdfs write error already writen = %d", len);
      len = -1;
      break;
    }

    remain -= nwrite;
    outp += nwrite;
    len += nwrite;
  }

  return len;
}

// 0 -- success, -1 -- failed
int HdfsWritableFile::flush()
{
  return hdfsFlush(env_->fs(), fd_);
}

int HdfsWritableFile::sync()
{
  return HdfsWritableFile::flush();
}

int HdfsWritableFile::close()
{
  int ret = 0;

  if (fd_ != NULL)
  {
    ret = hdfsCloseFile(env_->fs(), fd_);
    if (ret != 0)
    {
      TBSYS_LOG(WARN, "hdfsCloseFile error");
    }
  }
  fd_ = NULL;

  return ret;
}

HdfsEnv::~HdfsEnv()
{
  if (fs_ != NULL)
  {
    assert(fs_counter_ == 1);
    hdfsDisconnect(fs_);
    TBSYS_LOG(DEBUG, "call hdfs env destructor");
  }
}

int HdfsEnv::rmfile(const char *path)
{
  return hdfsDelete(fs_, path);
}

int HdfsEnv::rename(const char *src, const char *target)
{
  return hdfsRename(fs_, src, target);
}

int HdfsEnv::new_writable_file(const std::string &file_name, WritableFile *&file)
{
  int ret = 0;
  file = NULL;

  hdfsFile fd = hdfsOpenFile(fs_, file_name.c_str(), O_WRONLY, buffersize_, replica_, blocksize_);
  TBSYS_LOG(DEBUG, "file name = %s, buffersize=%ld, replica=%ld,bs=%ld",
            file_name.c_str(), buffersize_, replica_, blocksize_);

  if (fd == NULL)
  {
    TBSYS_LOG(ERROR, "can't open hdfs file, name=%s", file_name.c_str());
    ret = -1;
  }
  else
  {
    file = new(std::nothrow) HdfsWritableFile(this, fd);
    if (file == NULL)
    {
      TBSYS_LOG(ERROR, "can't allocate memory for HdfsWritableFile");
      ret = -1;
    }
  }

  return ret;
}

int HdfsEnv::setup(const Env::Param *param)
{
  int ret = 0;
  const HdfsParam *hparam = reinterpret_cast<const HdfsParam*>(param);
  blocksize_ = hparam->blocksize;
  buffersize_ = hparam->io_buffer_size;
  replica_ = hparam->replication;
  TBSYS_LOG(INFO, "host=%s,user=%s, port=%d, group_size=%d", hparam->host, hparam->user, hparam->port, hparam->group_size);

  fs_ = hdfsConnectAsUser("hdpnn", hparam->port, hparam->user,
                          hparam->group, hparam->group_size);
  if (fs_ == NULL)
  {
    ret = -1;
    TBSYS_LOG(ERROR, "calling hdfsConnectAsUser failed");
  }
  else
  {
    ref();
    valid_ = true;
    TBSYS_LOG(DEBUG, "Hdfs FS handle = %ld", (long)fs_);
  }

  return ret;
}

static HdfsEnv *__hdfs_env = NULL;

Env *Env::Hadoop()
{
  if (__hdfs_env == NULL)
  {
    __hdfs_env = new HdfsEnv();
  }

  return __hdfs_env;
}
