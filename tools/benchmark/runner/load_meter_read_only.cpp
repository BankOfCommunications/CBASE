#include <getopt.h>
#include <string.h>
#include "tbsys.h"
#include "common/ob_malloc.h"
#include "common/ob_define.h"
#include "common/file_directory_utils.h"
#include "load_define.h"
#include "load_param.h"
#include "load_runner.h"

using namespace std;
using namespace oceanbase::tools;
using namespace oceanbase::common;

void print_usage()
{
  fprintf(stderr, "ob_meter [OPTION]\n");
  fprintf(stderr, "   -a|--addr oceanbase server addr\n");
  fprintf(stderr, "   -P|--port oceanbase server port\n");
  fprintf(stderr, "   -u|--user_name oceanbase user name\n");
  fprintf(stderr, "   -p|--password oceanbase password\n");
  fprintf(stderr, "   -f|--file input load file\n");
  fprintf(stderr, "   -t|--thread load runner thread count\n");
  fprintf(stderr, "   -l|--log file name\n");
  fprintf(stderr, "   -i|--loop times\n");
  fprintf(stderr, "   -d|--is debug mode\n");
}

int main(int argc, char ** argv)
{
  int ret = OB_SUCCESS;
  ob_init_memory_pool();
  MeterParam param;
  LoadRunner runner;
  const char * opt_string = "a:P:u:p:f:w:q:i:l:t:m:edh";
  struct option longopts[] =
  {
    {"addr", 1, NULL, 'a'},
    {"port", 1, NULL, 'P'},
    {"port", 1, NULL, 'u'},
    {"port", 1, NULL, 'p'},
    {"file", 1, NULL, 'f'},
    {"thread", 1, NULL, 't'},
    {"log", 1, NULL, 'f'},
    {"qps", 1, NULL, 'q'},
    {"max", 1, NULL, 'm'},
    {"iter", 1, NULL, 'i'},
    {"watch", 1, NULL, 'w'},
    //{"check", 0, NULL, 'c'},
    {"enable", 0, NULL, 'e'},
    {"debug", 0, NULL, 'd'},
    {"help", 0, NULL, 'h'},
    {0, 0, 0, 0}
  };

  int opt = 0;
  const char * file_path = NULL;
  const char * hostname = NULL;
  int32_t port = 0;
  bool is_debug = false;
  const char* log_file = NULL;
  while ((opt = getopt_long(argc, argv, opt_string, longopts, NULL)) != -1)
  {
    switch (opt)
    {
      case 'a':
        hostname = optarg;
        break;
      case 'P':
        port = atoi(optarg);
        break;
      case 'u':
        strncpy(param.ctrl_info_.user_name_, optarg, OB_MAX_USERNAME_LENGTH);
        break;
      case 'p':
        strncpy(param.ctrl_info_.password_, optarg, OB_MAX_PASSWORD_LENGTH);
        break;
      case 'f':
        file_path = optarg;
        break;
      case 'l':
        log_file = optarg;
        break;
      case 'd':
        is_debug = true;
        break;
      case 'i':
        param.load_source_.iter_times_  = atoi(optarg);
        break;
      case 'q':
        param.load_source_.query_per_second_  = atoi(optarg);
        break;
      case 'm':
        param.load_source_.max_query_count_  = atoi(optarg);
        break;
      case 't':
        param.ctrl_info_.thread_count_ = atoi(optarg);
        break;
      case 'w':
        param.ctrl_info_.status_interval_ = 1000 * 1000 * atoi(optarg);
        break;
      case 'e':
        param.ctrl_info_.enable_read_master_ = true;
        break;
      case 'h':
      default:
        print_usage();
        exit(1);
    }
  }
  if (is_debug)
  {
    TBSYS_LOGGER.setLogLevel("DEBUG");
  }
  else
  {
    TBSYS_LOGGER.setLogLevel("INFO");
  }
  if (NULL != file_path)
  {
    if (NULL != log_file)
    {
      char buf[MAX_FILE_LEN];
      strncpy(buf, log_file, MAX_FILE_LEN);
      TBSYS_LOGGER.setFileName(buf);
    }
    //TBSYS_LOGGER.setFileName("playback.log");
    // source load file
    strncpy(param.load_source_.file_, file_path, MAX_FILE_LEN);
    if (!FileDirectoryUtils::exists(param.load_source_.file_))
    {
      ret = OB_ERROR;
      TBSYS_LOG(WARN, "input load source file not exist:file[%s]", param.load_source_.file_);
    }
    else
    {
      param.ctrl_info_.new_server_.set_ipv4_addr(hostname, port);
      ret = runner.start(param, true);
    }
  }
  else
  {
    ret = OB_ERROR;
    print_usage();
  }
  if (OB_SUCCESS == ret)
  {
    runner.wait();
  }
  return ret;
}

