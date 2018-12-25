#include <sys/types.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include "olap_conf.h"
#include "common/ob_define.h"
#include "common/ob_schema.h"
using namespace oceanbase;
using namespace oceanbase::common;
using namespace std;
namespace 
{
  static const char * OLAP_SECTION = "olap";
}

int OlapConf::parse_merge_server(char* str)
{
  int ret           = OB_SUCCESS;
  char *str_ptr     = NULL;
  int32_t port      = 0;
  char *begin_ptr   = NULL;
  char *end_ptr     = str + strlen(str);
  char *ptr         = NULL;
  int64_t length    = 0;
  char buffer[64];

  if (NULL == str)
  {
    TBSYS_LOG(WARN, "invalid param, str=%p", str);
    ret = OB_ERROR;
  }

  /**
   * Servers list format is like this. For examle:
   * "127.0.0.1:11108,127.0.0.1:11109"
   */
  for (begin_ptr = str; begin_ptr != end_ptr && OB_SUCCESS == ret;)
  {
    port = 0;
    str_ptr = index(begin_ptr, ',');

    if (NULL != str_ptr)
    {
      memcpy(buffer, begin_ptr, str_ptr - begin_ptr);
      buffer[str_ptr - begin_ptr]= '\0';
      begin_ptr = str_ptr + 1;
    }
    else
    {
      length= strlen(begin_ptr);
      memcpy(buffer, begin_ptr, length);
      buffer[length] = '\0';
      begin_ptr = end_ptr;
    }

    ptr = index(buffer, ':');
    if (NULL != ptr)
    {
      ptr[0] = '\0';
      ptr++;
      port = static_cast<int32_t>(strtol(ptr, (char**)NULL, 10));
    }

    server_info_t ms;
    if (inet_pton(AF_INET, buffer, &(ms.ip_)) <= 0)
    {
      TBSYS_LOG(WARN,"unrecogonizable ip addr:%s", buffer);
      ret = OB_INVALID_ARGUMENT;
    }
    else
    {
      ms.port_ = port;
      snprintf(ms.ip_str_,sizeof(ms.ip_str_), "%s", buffer);
      ms_server_vec_.push_back(ms);
    }

    if (isspace(*begin_ptr))
    {
      begin_ptr++;
    }
  }

  return ret;
}

int OlapConf::init(const char *conf_file)
{
  static char ms_list_buf[4096];
  int err = OB_SUCCESS;
  tbsys::CConfig config;
  if (config.load(conf_file) != EXIT_SUCCESS)
  {
    TBSYS_LOG(WARN,"fail to load configuration");
    err = OB_INVALID_ARGUMENT;
  }
  if (OB_SUCCESS == err)
  {
    const char *ms_list = NULL;
    ms_list = config.getString(OLAP_SECTION, "ms_list");
    if (NULL == ms_list)
    {
      TBSYS_LOG(WARN,"fail to get ms_list from configuration");
      err  = OB_INVALID_ARGUMENT;
    }
    else
    {
      snprintf(ms_list_buf,sizeof(ms_list_buf),"%s", ms_list);
      if (OB_SUCCESS != parse_merge_server(ms_list_buf))
      {
        TBSYS_LOG(WARN,"fail to parse merge server list:%s", ms_list_buf);
      }
    }
  }

  if (OB_SUCCESS == err)
  {
    const char * log_path = NULL;
    log_path = config.getString(OLAP_SECTION, "log_path");
    if (NULL == log_path)
    {
      TBSYS_LOG(WARN,"fail to get log_path");
      err  = OB_INVALID_ARGUMENT;
    }
    else
    {
      log_path_.append(log_path,log_path + strlen(log_path));
    }
  }

  if (OB_SUCCESS == err)
  {
    const char *log_level = NULL;
    log_level = config.getString(OLAP_SECTION, "log_level");
    if (NULL == log_level)
    {
      TBSYS_LOG(WARN,"fail to get log_level");
      err  = OB_INVALID_ARGUMENT;
    }
    else
    {
      log_level_.append(log_level,log_level + strlen(log_level));
      for (string::iterator it = log_level_.begin(); it != log_level_.end(); it++)
      {
        *it = static_cast<char>(toupper(*it));
      }
    }
  }
  if (OB_SUCCESS == err)
  {
    TBSYS_LOGGER.setFileName(log_path_.c_str());
    TBSYS_LOGGER.setLogLevel(log_level_.c_str());
  }

  if (OB_SUCCESS == err)
  {
    read_thread_count_ = config.getInt(OLAP_SECTION, "read_thread_count", 10);
    if (read_thread_count_ <= 0)
    {
      TBSYS_LOG(WARN,"read_thread_count should bigger than 0 [read_thread_count:%ld]", read_thread_count_);
      err  = OB_INVALID_ARGUMENT;
    }
  }

  if (OB_SUCCESS == err)
  {
    network_timeout_us_ = config.getInt(OLAP_SECTION, "network_timeout_us", 10);
    if (network_timeout_us_ <= 0)
    {
      TBSYS_LOG(WARN,"network_timeout_us should bigger than 0 [network_timeout_us:%ld]", network_timeout_us_);
      err  = OB_INVALID_ARGUMENT;
    }
  }

  if (OB_SUCCESS == err)
  {
    int64_t val;
    val = config.getInt(OLAP_SECTION, "start_key", 0);
    if (val <= 0)
    {
      TBSYS_LOG(WARN,"start_key should bigger than 0 [start_key:%ld]", val);
      err  = OB_INVALID_ARGUMENT;
    }
    else
    {
      start_key_ = static_cast<uint32_t>(val);
    }
  }

  if (OB_SUCCESS == err)
  {
    int64_t val;
    val = config.getInt(OLAP_SECTION, "end_key", 0);
    if (val <= 0)
    {
      TBSYS_LOG(WARN,"end_key should bigger than 0 [end_key:%ld]", val);
      err  = OB_INVALID_ARGUMENT;
    }
    else
    {
      end_key_ = static_cast<uint32_t>(val);
    }
    if ((OB_SUCCESS == err) && (end_key_ <= start_key_))
    {
      TBSYS_LOG(WARN,"end_key must bigger than start_key [start_key:%u,end_key:%u]", start_key_, end_key_);
      err  = OB_INVALID_ARGUMENT;
    }
  }

  if (OB_SUCCESS == err)
  {
    int64_t val;
    val = config.getInt(OLAP_SECTION, "scan_count_min", 0);
    if (val <= 0)
    {
      TBSYS_LOG(WARN,"scan_count_min should bigger than 0 [scan_count_min:%ld]", val);
      err  = OB_INVALID_ARGUMENT;
    }
    else
    {
      scan_count_min_ = static_cast<uint32_t>(val);
    }
  }

  if (OB_SUCCESS == err)
  {
    int64_t val;
    val = config.getInt(OLAP_SECTION, "scan_count_max", 0);
    if (val <= 0)
    {
      TBSYS_LOG(WARN,"scan_count_max should bigger than 0 [scan_count_max:%ld]", val);
      err  = OB_INVALID_ARGUMENT;
    }
    else
    {
      scan_count_max_ = static_cast<uint32_t>(val);
    }
    if ((OB_SUCCESS == err) && (scan_count_max_ <= scan_count_min_))
    {
      TBSYS_LOG(WARN,"scan_count_max must bigger than scan_count_min [scan_count_min:%u,scan_count_max:%u]", 
        scan_count_min_, scan_count_max_);
      err  = OB_INVALID_ARGUMENT;
    }
  }
  if (OB_SUCCESS == err)
  {
    TBSYS_LOG(INFO,"merge_server_count:%zu", ms_server_vec_.size());
    for (size_t i = 0; i < ms_server_vec_.size(); i++)
    {
      TBSYS_LOG(INFO, "ms_:%zu,ip:%s,port:%u", i, ms_server_vec_[i].ip_str_, ms_server_vec_[i].port_);
    }

    TBSYS_LOG(INFO, "log_path:%s", log_path_.c_str());
    TBSYS_LOG(INFO, "log_level:%s", log_level_.c_str());

    TBSYS_LOG(INFO, "read_thread_count:%ld", read_thread_count_);
    assert(end_key_ > start_key_);
    TBSYS_LOG(INFO, "start_key:%u", start_key_);
    TBSYS_LOG(INFO, "end_key:%u", end_key_);

    TBSYS_LOG(INFO, "scan_count_min:%u", scan_count_min_);
    TBSYS_LOG(INFO, "scan_count_max:%u", scan_count_max_);
    assert(scan_count_max_ > scan_count_min_);


    TBSYS_LOG(INFO, "network_timeout_us:%ld", network_timeout_us_);
  }
  return err;
}
