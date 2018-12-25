#ifndef OLAP_OLAP_CONF_H_ 
#define OLAP_OLAP_CONF_H_
#include <vector> 
#include <string> 
class OlapConf
{
public:
  struct server_info_t
  {
    uint32_t ip_;
    uint32_t port_;
    char     ip_str_[64];
  };
  int init(const char *conf_file);

  const std::vector<server_info_t> & get_ms_servers()const
  {
    return ms_server_vec_;
  };

  const std::string& get_log_path()const
  {
    return log_path_;
  }

  const std::string&  get_log_level()const
  {
    return log_level_;
  }

  int64_t  get_read_thread_count()const
  {
    return read_thread_count_;
  }

  uint32_t get_start_key()const
  {
    return start_key_;
  }

  uint32_t get_end_key()const
  {
    return end_key_;
  }

  uint32_t get_min_scan_count()const
  {
    return scan_count_min_;
  }

  uint32_t get_max_scan_count()const
  {
    return scan_count_max_;
  }

  int64_t get_network_timeout()const
  {
    return network_timeout_us_;
  }
private:
  int parse_merge_server(char* str);
  std::vector<server_info_t> ms_server_vec_;
  std::string log_path_;
  std::string log_level_;
  int64_t     read_thread_count_;
  uint32_t    start_key_;
  uint32_t    end_key_;

  uint32_t    scan_count_min_;
  uint32_t    scan_count_max_;

  int64_t     network_timeout_us_;
};

#endif /* OLAP_OLAP_CONF_H_ */
