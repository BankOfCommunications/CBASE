#include <getopt.h>
#include <set>
#include <string.h>

#include "task_env.h"
#include "task_worker.h"
#include "task_worker_param.h"
#include "hdfs_env.h"

using namespace oceanbase::tools;
using namespace oceanbase::common;

static const int64_t TIMEOUT = 1000 * 1000 * 20;  // 20s
static const int64_t RETRY_TIME = 2000 * 1000;    // 2s
static const int64_t MAX_PATH_LEN = 1024;         // 1024B

void print_usage()
{
  fprintf(stderr, "task_worker [OPTION]\n");
  fprintf(stderr, "   -a|--addr task server addr\n");
  fprintf(stderr, "   -p|--port task server port\n");
  fprintf(stderr, "   -f|--file output file path\n");
  fprintf(stderr, "   -l|--log  output log file\n");
  fprintf(stderr, "   -c|--config config file path\n");
  fprintf(stderr, "   -h|--help usage help\n");
}

int main_routine(Env *env, const ObServer & task_server, const char * file_path, const TaskWorkerParam *param);

int setup_env(const TaskWorkerParam &param, Env **envp);
void destroy_env(Env **envp);

int main(int argc, char ** argv)
{
  int ret = OB_SUCCESS;
  const char * opt_string = "a:p:f:l:h:c:";
  struct option longopts[] =
  {
    {"addr", 1, NULL, 'a'},
    {"port", 1, NULL, 'p'},
    {"file", 1, NULL, 'f'},
    {"log", 1, NULL, 'l'},
    {"help", 0, NULL, 'h'},
    {"config", 1, NULL, 'c'},
    {0, 0, 0, 0}
  };

  int opt = 0;
  const char * log_file = NULL;
  const char * file_path = NULL;
  const char * hostname = NULL;
  const char *config = NULL;
  int32_t port = 0;
  std::set<std::string> dump_tables;
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
      case 'l':
        log_file = optarg;
        break;
      case 'c':
        config = optarg;
        break;
      case 'h':
      default:
        print_usage();
        exit(1);
    }
  }

  if ((NULL != hostname) && (NULL != log_file) && (0 != port) && (config != NULL))
  {
    ob_init_memory_pool();
    TBSYS_LOGGER.setFileName(log_file, true);
    TBSYS_LOGGER.setLogLevel("DEBUG");
    TBSYS_LOGGER.setMaxFileSize(1024L * 1024L);

    TaskWorkerParam param;
    ret = param.load(config);
    if (ret == OB_SUCCESS)
    {
      Env *env = NULL;

      param.DebugString();
      ret = setup_env(param, &env);
      if (ret != OB_SUCCESS)
      {
        TBSYS_LOG(ERROR, "setup env failed");
      }
      else
      {
        ObServer task_server(ObServer::IPV4, hostname, port);
        ret = main_routine(env, task_server, file_path, &param);
      }
      destroy_env(&env);
    }
    else
    {
      TBSYS_LOG(ERROR, "can't load config file, path=%s", config);
    }
  }
  else
  {
    ret = OB_ERROR;
    print_usage();
  }
  return ret;
}

int main_routine(Env *env, const ObServer & task_server, const char * file_path, const TaskWorkerParam *param)
{
  // task server addr
  uint64_t times = 3;
  TaskWorker client(env, task_server, TIMEOUT, times, param);
  RpcStub rpc(client.get_client(), client.get_buffer());
  // output data path
  int ret = client.init(&rpc, file_path);
  if (ret != OB_SUCCESS)
  {
    TBSYS_LOG(ERROR, "client init failed:ret[%d]", ret);
  }
  else
  {
    TaskInfo task;
    TaskCounter count;
    char output_file[MAX_PATH_LEN] = "";
    while (true)
    {
      // step 1. fetch a new task
      ret = client.fetch_task(count, task);
      if (ret != OB_SUCCESS)
      {
        // timeout because of server not exist or server exit
        if (OB_RESPONSE_TIME_OUT == ret)
        {
          ret = OB_SUCCESS;
          break;
        }
        TBSYS_LOG(WARN, "fetch new task failed:ret[%d]", ret);
        usleep(RETRY_TIME);
      }
      else if (count.finish_count_ < count.total_count_)
      {
        // step 2. do the task
        ret = client.do_task(task, output_file, sizeof(output_file));
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(ERROR, "finish task failed:task[%ld], ret[%d]", task.get_id(), ret);
        }
        else
        {
          TBSYS_LOG(INFO, "finish task succ:task[%ld], file[%s], table_id=%ld",
                    task.get_id(), output_file,
                    task.get_table_id());
        }
        // step 3. report the task status
        ret = client.report_task(ret, output_file, task);
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(ERROR, "report task failed:task[%ld], ret[%d]", task.get_id(), ret);
        }
      }
      else
      {
        ret = OB_SUCCESS;
        TBSYS_LOG(INFO, "finish all the task");
        break;
      }
    }
  }
  return ret;
}

int setup_env(const TaskWorkerParam &param, Env **envp)
{
  int ret = OB_SUCCESS;
  *envp = NULL;

  if (param.task_file_type() == TaskWorkerParam::POSIX_FILE)
  {
    *envp = Env::Posix();
    ret = (*envp)->setup(NULL);
  }
  else
  {
    *envp = Env::Hadoop();
    if ((*envp)->setup(reinterpret_cast<const Env::Param *>(&param.hdfs_param())) != 0)
    {
      TBSYS_LOG(ERROR, "can't setup hdfs env");
      ret = OB_ERROR;
    }
  }

  return ret;
}

void destroy_env(Env **envp)
{
  delete (*envp);
  *envp = NULL;
}
