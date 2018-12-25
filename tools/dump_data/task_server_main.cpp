#include <set>
#include <getopt.h>
#include <string.h>
#include "tblog.h"
#include "task_server.h"
#include "task_server_param.h"

using namespace std;
using namespace oceanbase::common;
using namespace oceanbase::tools;

static const int64_t SLEEP_TIME = 4000; // 4s
static const int64_t TIMEOUT = 1000 * 1000 * 10; // 10s

void print_usage()
{
  fprintf(stderr, "task_server [OPTION]\n");
  fprintf(stderr, "   -f|--conf server conf file\n");
  fprintf(stderr, "   -t|--table dump table name\n");
  fprintf(stderr, "   -h|--help usage help\n");
}

int main_routine(const set<string> & tables, const TaskServerParam & param);

int main(int argc, char ** argv)
{
  int ret = OB_SUCCESS;
  const char * opt_string = "f:t:h";
  struct option longopts[] =
  {
    {"conf", 1, NULL, 'f'},
    {"table", 1, NULL, 't'},
    {"help", 0, NULL, 'h'},
    {0, 0, 0, 0}
  };

  int opt = 0;
  const char * conf = NULL;
  TaskServerParam param;
  std::set<std::string> tables;
  while ((opt = getopt_long(argc, argv, opt_string, longopts, NULL)) != -1)
  {
    switch (opt)
    {
      case 'f':
        conf = optarg;
        break;
      case 't':
        tables.insert(std::string(optarg));
        break;
      case 'h':
      default:
        print_usage();
        exit(1);
    }
  }

  if (NULL == conf)
  {
    conf = "task_server.conf";
  }

  ret = param.load_from_config(conf);
  if (ret != OB_SUCCESS)
  {
    TBSYS_LOG(ERROR, "load conf failed:conf[%s], ret[%d]", conf, ret);
  }
  else
  {
    if (param.get_log_name() != NULL)
    {
      TBSYS_LOGGER.setFileName(param.get_log_name());
    }
    TBSYS_LOGGER.setLogLevel(param.get_log_level());
    TBSYS_LOGGER.setMaxFileSize(param.get_max_log_size() * 1024L * 1024L);
    signal(SIGPIPE, SIG_IGN);
    ob_init_memory_pool();
    ret = main_routine(tables, param);
  }
  return ret;
}


int main_routine(const set<string> & tables, const TaskServerParam & param)
{
  int ret = OB_SUCCESS;
  ObServer root_server(ObServer::IPV4, param.get_root_server_ip(), param.get_root_server_port());
  TaskServer server(param.get_result_file(), param.get_timeout_times(), param.get_max_visit_count(),
      param.get_network_timeout(), root_server);
  RpcStub rpc(server.get_client(), server.get_buffer());
  rpc.set_base_server(&server);
  TaskFactory & task_factory = server.get_task_factory();
  task_factory.add_table_confs(&param.get_all_conf());

  set<string>::const_iterator it;
  for (it = tables.begin();it != tables.end(); ++it)
  {
    ret = task_factory.add_table((*it).c_str());
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(ERROR, "add table failed:table[%s], ret[%d]", (*it).c_str(), ret);
    }
  }

  // init run
  BaseServerRunner server_run(server);
  tbsys::CThread server_thread;
  if (OB_SUCCESS == ret)
  {
    ret = server.init(param.get_thread_count(), param.get_queue_size(), &rpc,
        param.get_dev_name(), param.get_task_server_port());
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(ERROR, "check server init failed:ret[%d]", ret);
    }
    else
    {
      server_thread.start(&server_run, NULL);
    }
  }

  if (OB_SUCCESS == ret)
  {
    /// sleep for server thread init
    usleep(SLEEP_TIME);
    server.dump_task_info(param.get_tablet_list_file());
    if (0 == server.get_task_manager().get_count())
    {
      TBSYS_LOG(INFO, "%s", "check no task for doing");
    }
    else
    {
      server_thread.join();
    }
  }

  return ret;
}

