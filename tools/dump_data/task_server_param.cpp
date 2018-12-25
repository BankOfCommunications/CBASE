#include "config.h"
#include "task_server_param.h"
#include <string>

using namespace oceanbase::common;
using namespace oceanbase::tools;

static const char * OBTS_SECTION = "task_server";
static const char * OBTS_PORT = "port";
static const char * OBTS_DEVNAME = "dev_name";
static const char * OBTS_RESULTFILE = "result_file";
static const char * OBTS_LOGFILE = "log_file";
static const char * OBTS_LOGLEVEL = "log_level";
static const char * OBTS_MAX_LOGSIZE = "max_log_file_size";
static const char * OBTS_VISIT_COUNT = "max_visit_count";
static const char * OBTS_TIMEOUT_TIMES = "max_timeout_times";
static const char * OBTS_TASK_QUEUE_SIZE = "task_queue_size";
static const char * OBTS_TASK_THREAD_COUNT = "task_thread_count";
static const char * OBTS_NETWORK_TIMEOUT = "network_timeout_us";
static const char * OBTS_TALETS_FILE = "tablet_list_file";

static const char * OBTS_RS_SECTION = "root_server";
static const char * OBTS_RS_VIP = "vip";
static const char * OBTS_RS_PORT = "port";

TaskServerParam::TaskServerParam()
{
  memset(this, 0, sizeof(TaskServerParam));
  root_server_port_ = 0;
  server_listen_port_ = 10234;
  max_visit_count_ = 10;
  max_timeout_times_ = 2;
  task_queue_size_ = 512;
  task_thread_count_ = 20;
  network_timeout_ = 1000 * 1000;
}

TaskServerParam::~TaskServerParam()
{
}

int TaskServerParam::load_from_config(const char * file)
{
  int ret = OB_SUCCESS;
  if (NULL == file)
  {
    TBSYS_LOG(ERROR, "check conf file failed:file[%s]", file);
    ret = OB_ERROR;
  }
  else
  {
    ret = TBSYS_CONFIG.load(file);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(ERROR, "load conf failed:file[%s], ret[%d]", file, ret);
    }
  }

  // task server conf
  if (OB_SUCCESS == ret)
  {
    ret = load_string(dev_name_, OB_MAX_IP_SIZE, OBTS_SECTION, OBTS_DEVNAME);
    if (OB_SUCCESS == ret)
    {
      server_listen_port_ = TBSYS_CONFIG.getInt(OBTS_SECTION, OBTS_PORT, 0);
      if (server_listen_port_ < 1024)
      {
        ret = OB_ERROR;
        TBSYS_LOG(ERROR, "check task server conf failed:port[%d]", server_listen_port_);
      }
    }
  }

  // task finish result conf
  if (OB_SUCCESS == ret)
  {
    ret = load_string(result_file_, OB_MAX_FILE_NAME, OBTS_SECTION, OBTS_RESULTFILE);
  }

  if (OB_SUCCESS == ret)
  {
    ret = load_string(tablet_list_file_, OB_MAX_FILE_NAME, OBTS_SECTION, OBTS_TALETS_FILE);
  }

  // log conf
  if (OB_SUCCESS == ret)
  {
    log_name_ = TBSYS_CONFIG.getString(OBTS_SECTION, OBTS_LOGFILE);

    ret = load_string(log_level_, OB_MAX_LOG_LEVEL, OBTS_SECTION, OBTS_LOGLEVEL);
    if (OB_SUCCESS == ret)
    {
      max_log_size_ = TBSYS_CONFIG.getInt(OBTS_SECTION, OBTS_MAX_LOGSIZE, 1024);
    }
  }

  // other conf
  if (OB_SUCCESS == ret)
  {
    max_visit_count_ = TBSYS_CONFIG.getInt(OBTS_SECTION, OBTS_VISIT_COUNT, 20);
    max_timeout_times_ = TBSYS_CONFIG.getInt(OBTS_SECTION, OBTS_TIMEOUT_TIMES, 3);
    task_queue_size_ = TBSYS_CONFIG.getInt(OBTS_SECTION, OBTS_TASK_QUEUE_SIZE, 512);
    task_thread_count_ = TBSYS_CONFIG.getInt(OBTS_SECTION, OBTS_TASK_THREAD_COUNT, 20);
    network_timeout_ = TBSYS_CONFIG.getInt(OBTS_SECTION, OBTS_NETWORK_TIMEOUT, 1000 * 1000);
    if ((max_visit_count_ <= 0) || (max_timeout_times_ <= 0) || (task_queue_size_ <= 0)
      || (task_thread_count_ <= 0) || (network_timeout_ <= 0))
    {
      ret = OB_ERROR;
      TBSYS_LOG(ERROR, "check task server conf failed:count[%d], times[%d], queue[%d], "
          "thread[%d], timeout[%ld]", max_visit_count_, max_timeout_times_, task_queue_size_,
          task_thread_count_, network_timeout_);
    }
  }

  // root server conf
  if (OB_SUCCESS == ret)
  {
    ret = load_string(root_server_ip_, OB_MAX_IP_SIZE, OBTS_RS_SECTION, OBTS_RS_VIP);
    if (OB_SUCCESS == ret)
    {
      root_server_port_ = TBSYS_CONFIG.getInt(OBTS_RS_SECTION, OBTS_RS_PORT, 0);
      if (root_server_port_ < 1024)
      {
        ret = OB_ERROR;
        TBSYS_LOG(ERROR, "check root server conf failed:port[%d]", root_server_port_);
      }
    }
  }

  if (ret == OB_SUCCESS)
  {
    std::vector<std::string> sections;
    TBSYS_CONFIG.getSectionName(sections);
    for(size_t i = 0;i < sections.size(); i++)
    {
      if (sections[i] != OBTS_SECTION && sections[i] != OBTS_RS_SECTION)
      {
        TableConf conf;
        TBSYS_LOG(DEBUG, "section=%s", sections[i].c_str());
        ret = TableConf::loadConf(sections[i].c_str(), conf);
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(ERROR, "can't load conf table_name = %s", sections[i].c_str());
          break;
        }
        confs_.push_back(conf);
      }
    }
  }
  return ret;
}


int TaskServerParam::load_string(char * dest, const int32_t size, const char * section,
    const char * name, bool require)
{
  int ret = OB_SUCCESS;
  if (NULL == dest || 0 >= size || NULL == section || NULL == name)
  {
    ret = OB_ERROR;
  }

  const char * value = NULL;
  if (OB_SUCCESS == ret)
  {
    value = TBSYS_CONFIG.getString(section, name);
    if (require && (NULL == value || 0 >= strlen(value)))
    {
      TBSYS_LOG(ERROR, "%s.%s has not been set.", section, name);
      ret = OB_ERROR;
    }
  }

  if (OB_SUCCESS == ret && NULL != value)
  {
    if ((int32_t)strlen(value) >= size)
    {
      TBSYS_LOG(ERROR, "%s.%s too long, length (%d) > %d", section, name, int(strlen(value)), size);
      ret = OB_SIZE_OVERFLOW;
    }
    else
    {
      strncpy(dest, value, strlen(value));
    }
  }
  return ret;
}


