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
  fprintf(stderr, "   -u|-- user name\n");
  fprintf(stderr, "   -p|-- password\n");
  fprintf(stderr, "   -a|--addr oceanbase server addr\n");
  fprintf(stderr, "   -P|--port oceanbase server port\n");
  fprintf(stderr, "   -f|--file input load file\n");
  fprintf(stderr, "   -t|--thread load runner thread count\n");
  fprintf(stderr, "   -i|--loop times\n");
  fprintf(stderr, "   -d|--is debug info? \n");
}

int main(int argc, char ** argv)
{
  int ret = OB_SUCCESS;
  ob_init_memory_pool();
  MeterParam param;
  LoadRunner runner;
  const char * opt_string = "a:P:u:p:f:w:q:i:t:m:eh";
  struct option longopts[] =
  {
    {"addr", 1, NULL, 'a'},
    {"Port", 1, NULL, 'P'},
    {"user", 1, NULL, 'u'},
    {"password", 1, NULL, 'p'},
    {"file", 1, NULL, 'f'},
    {"thread", 1, NULL, 't'},
    {"qps", 1, NULL, 'q'},
    {"max", 1, NULL, 'm'},
    {"iter", 1, NULL, 'i'},
    {"watch", 1, NULL, 'w'},
    //{"check", 0, NULL, 'c'},
    {"enable", 0, NULL, 'e'},
    {"help", 0, NULL, 'h'},
    {0, 0, 0, 0}
  };

  int opt = 0;
  const char * file_path = NULL;
  const char * hostname = "10.232.36.30";
  int32_t port = 2801;
  while ((opt = getopt_long(argc, argv, opt_string, longopts, NULL)) != -1)
  {
    switch (opt)
    {
      case 'a':
        hostname = optarg;
        break;
      case 'p':
        port = atoi(optarg);
        break;
      case 'f':
        file_path = optarg;
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
      //case 'c':
      //  param.ctrl_info_.compare_result_ = true;
      //  break;
      case 'e':
        param.ctrl_info_.enable_read_master_ = true;
        break;
      case 'h':
      default:
        print_usage();
        exit(1);
    }
  }
  TBSYS_LOGGER.setFileName("playback.log");
  //TBSYS_LOGGER.setLogLevel("INFO");
  TBSYS_LOGGER.setLogLevel("DEBUG");
  if (NULL != file_path)
  {
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
      ret = runner.start(param);
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

