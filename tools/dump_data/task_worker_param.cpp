#include "task_worker_param.h"
#include "common/utility.h"
#include "common/ob_define.h"
#include "hdfs_env.h"
#include "hdfs_ext.h"
#include <vector>
#include <string>

using namespace oceanbase::tools;
using namespace oceanbase::common;
using namespace std;

static const char *kSysConfig = "sys";
static const char *kOutputFileType = "output.file.type";

//section name for hdfs config
static const char *kHdfsConfig = "hdfs";
static const char *kHdfsGroup = "group";
static const char *kHdfsHost = "host";
static const char *kHdfsPort = "port";
static const char *kHdfsUser = "user";
static const char *kHdfsIoBufferSize = "io.buffer.size";
static const char *kHdfsReplication = "replication";
static const char *kHdfsBlockSize = "block.size";
static const char *kHadoopAuth = "hadoop.auth";

TaskWorkerParam::TaskWorkerParam()
{
  task_file_type_ = POSIX_FILE;                          /* 0 -- POSIX_FILE */
  memset(hdfs_group_buffer_, 0, sizeof(hdfs_group_buffer_));
  memset(hdfs_group_ptr_, 0, sizeof(hdfs_group_ptr_));
}

TaskWorkerParam::~TaskWorkerParam()
{

}

int TaskWorkerParam::load(const char *file)
{
  int ret = OB_SUCCESS;
  if (file == NULL)
  {
    TBSYS_LOG(ERROR, "no configuration file is specified");
    ret = OB_ERROR;
  }

  if (ret == OB_SUCCESS)
  {
    ret = TBSYS_CONFIG.load(file);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(ERROR, "load conf failed:file[%s], ret[%d]", file, ret);
    }

    if (ret == OB_SUCCESS)
    {
      //defalut file type is POSIX_FILE
      task_file_type_ = TBSYS_CONFIG.getInt(kSysConfig, kOutputFileType, 1);
      if (task_file_type_ == HDFS_FILE)
      {
        ret = load_hdfs_config_env();
      }
    }
  }


  if (ret == OB_SUCCESS)                        /* parse table confs */
  {
    vector<string> sections;
    TBSYS_CONFIG.getSectionName(sections);

    for(size_t i = 0;i < sections.size(); i++) 
    {
      if (sections[i] != kSysConfig && sections[i] != kHdfsConfig) 
      {
        TableConf conf;
        ret = TableConf::loadConf(sections[i].c_str(), conf);
        if (ret != OB_SUCCESS) 
        {
          TBSYS_LOG(ERROR, "can't load conf table_name = %s", sections[i].c_str());
          break;
        }
        table_confs_.push_back(conf);
      }
    }
  }

  return ret;
}

int TaskWorkerParam::get_table_conf(const ObString &table_name, const TableConf *&conf) const
{
  int ret = OB_ERROR;

  for(size_t i = 0;i < table_confs_.size(); i++) 
  {
    if (table_name.compare(table_confs_[i].table_name().c_str()) == 0) 
    {
      conf = &table_confs_[i];
      ret = OB_SUCCESS;
      break;
    }
  }

  return ret;
}

int TaskWorkerParam::get_table_conf(int64_t table_id, const TableConf *&conf) const
{
  int ret = OB_ERROR;

  for(size_t i = 0;i < table_confs_.size(); i++) 
  {
    if (table_confs_[i].table_id() == table_id) 
    {
      conf = &table_confs_[i];
      ret = OB_SUCCESS;
      break;
    }
  }

  return ret;
}

int TaskWorkerParam::load_hdfs_config_env()
{
  hdfs_param_.blocksize = 1048576;
  hdfs_param_.io_buffer_size = 4096;

  hdfs_param_.group_size = kHdfsMaxGroupNr;
  int ret = hdfsGetConf(hdfs_param_.host, hdfs_param_.port, hdfs_param_.user, hdfs_group_ptr_, hdfs_param_.group_size);
  hdfs_param_.group = (const char **)hdfs_group_ptr_;

  return ret;
}

int TaskWorkerParam::load_hdfs_config()
{
  int ret = OB_SUCCESS;
  int need_auth = 0;

  hdfs_param_.replication = TBSYS_CONFIG.getInt(kHdfsConfig, kHdfsReplication, 0);
  hdfs_param_.port = TBSYS_CONFIG.getInt(kHdfsConfig, kHdfsPort, 9000);
  hdfs_param_.host = TBSYS_CONFIG.getString(kHdfsConfig, kHdfsHost);
  hdfs_param_.user = TBSYS_CONFIG.getString(kHdfsConfig, kHdfsUser);

  /* default 4K io buffer */
  hdfs_param_.io_buffer_size = TBSYS_CONFIG.getInt(kHdfsConfig, kHdfsIoBufferSize, 4096); 
  hdfs_param_.blocksize = TBSYS_CONFIG.getInt(kHdfsConfig, kHdfsBlockSize, 0);
  need_auth = TBSYS_CONFIG.getInt(kHdfsConfig, kHadoopAuth, 0); /* default--do not need auth */

  const char *group_str = TBSYS_CONFIG.getString(kHdfsConfig, kHdfsGroup);
  if (group_str == NULL) 
  {
    TBSYS_LOG(WARN, "group is null");
    hdfs_param_.group = NULL;
    hdfs_param_.group_size = 0;
  }
  else
  {
    char *str = NULL;
    int group_idx = 0;

    if (strlen(group_str) + 1 < (size_t)kHdfsGroupBuffSize) /* extra byte for '#' */
    {
      strcpy(hdfs_group_buffer_, group_str);
      str = strtok(hdfs_group_buffer_, ",");
    }
    else
    {
      TBSYS_LOG(ERROR, "group string is too len, max availabe length is %d", kHdfsGroupBuffSize);
      ret = OB_ERROR;
    }

    while (str != NULL && ret == OB_SUCCESS)
    {
      if (group_idx >= kHdfsMaxGroupNr) 
      {
        TBSYS_LOG(ERROR, "too much nr group, max is %d", kHdfsMaxGroupNr);
        ret = OB_ERROR;
        break;
      }

      hdfs_group_ptr_[group_idx++] = str;
      str = strtok(NULL, ",");
    }

    if (ret == OB_SUCCESS)
    {
      hdfs_param_.group = (const char **)hdfs_group_ptr_;
      hdfs_param_.group_size = group_idx;

      if (need_auth)
      {
        //the last elemnt of group is user passwd, start with a '#'
        char *passwd = const_cast<char *>(hdfs_param_.group[group_idx - 1]);
        for(int i = strlen(passwd); i >= 0; i--) 
        {
          passwd[i + 1] = passwd[i];
        }

        passwd[0] = '#';
      }
    }
  }

  return ret;
}

void TaskWorkerParam::DebugString()
{
  fprintf(stderr, "[sys section]\n");
  fprintf(stderr, "output.file.type=%d\n", task_file_type());
  fprintf(stderr, "[hdfs section]\n");
  fprintf(stderr, "host=%s\n", hdfs_param().host);
  fprintf(stderr, "port=%d\n", hdfs_param().port);
  fprintf(stderr, "user=%s\n", hdfs_param().user);
  fprintf(stderr, "io.buffer.size=%ld\n", hdfs_param().io_buffer_size);
  fprintf(stderr, "block_size=%ld\n", hdfs_param().blocksize);
  fprintf(stderr, "replication=%ld\n", hdfs_param().replication);

  for(int i = 0;i < hdfs_param().group_size; i++) {
    const char *group = hdfs_param().group[i];
    fprintf(stderr, "group%d=%s\n", i, group);
  }

  for(size_t i = 0;i < table_confs_.size(); i++) {
    fprintf(stderr, "%s\n", table_confs_[i].DebugString().c_str());
  }
}
