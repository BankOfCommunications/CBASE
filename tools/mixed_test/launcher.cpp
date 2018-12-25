#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>
#include <getopt.h>
#include <signal.h>
#include "tbsys.h"
#include "test_utils.h"
#include "utils.h"

struct CmdLine
{
  char *test_type;
  char *cluster_addr;
  int64_t cluster_port;
  int64_t thread_num;
  int64_t sign;
  bool check;

  CmdLine()
  {
    test_type = NULL;
    cluster_addr = NULL;
    cluster_port = 0;
    thread_num = 0;
    sign = 0;
    check = false;
  };

  void log_all()
  {
    TBSYS_LOG(INFO, "test_type=%s cluster_addr=%s cluster_port=%ld thread_num=%ld sign=%ld check=%s",
              test_type, cluster_addr, cluster_port, thread_num, sign, STR_BOOL(check));
  };

  bool is_valid()
  {
    bool bret = false;
    if (NULL != test_type
        && NULL != cluster_addr
        && 0 < cluster_port
        && 0 < thread_num)
    {
      bret = true;
    }
    return bret;
  };
};

void print_usage()
{
  fprintf(stderr, "\n");
  fprintf(stderr, "launcher [OPTION]\n");
  fprintf(stderr, "   -t|--test_type [mixed write random_mget random_scan total_scan]\n");
  fprintf(stderr, "   -a|--cluster_addr oceanbase cluster address\n");
  fprintf(stderr, "   -p|--cluster_port oceanbase clulster port\n");
  fprintf(stderr, "   -n|--thread_num number of thread to launch\n");
  fprintf(stderr, "   -s|--sign sign of this batch launch [default 0]\n");
  fprintf(stderr, "   -k|--check check the read result or not [default not]\n");
  fprintf(stderr, "\n");
}

void parse_cmd_line(int argc, char **argv, CmdLine &clp)
{
  int opt = 0;
  const char* opt_string = "t:a:p:n:s:kh";
  struct option longopts[] =
  {
    {"test_type", 1, NULL, 't'},
    {"cluster_addr", 1, NULL, 'a'},
    {"cluster_port", 1, NULL, 'p'},
    {"thread_num", 1, NULL, 'n'},
    {"sign", 1, NULL, 's'},
    {"check", 1, NULL, 'k'},
    {"help", 0, NULL, 'h'},
    {0, 0, 0, 0}
  };
  while ((opt = getopt_long(argc, argv, opt_string, longopts, NULL)) != -1)
  {
    switch (opt)
    {
    case 't':
      clp.test_type = optarg;
      break;
    case 'a':
      clp.cluster_addr = optarg;
      break;
    case 'p':
      clp.cluster_port = atoi(optarg);
      break;
    case 'n':
      clp.thread_num = atoll(optarg);
      break;
    case 's':
      clp.sign = atoll(optarg);
      break;
    case 'k':
      clp.check = true;
      break;
    case 'h':
    default:
      print_usage();
      exit(1);
    }
  }
  if (!clp.is_valid())
  {
    print_usage();
    exit(-1);
  }
}

int join_path(char* result_path, const int64_t len_limit, const char* base, const char* path)
{
  int err = OB_SUCCESS;
  int64_t len = 0;
  if (NULL == result_path || 0 >= len_limit || NULL == path)
  {
    err = OB_INVALID_ARGUMENT;
    TBSYS_LOG(ERROR, "join_path(result_path=%s, limit=%ld, base=%s, path=%s)=>%d",
              result_path, len_limit, base, path, err);
  }
  else if ('/' == path[0] || NULL == base)
  {
    if (0 >= (len = snprintf(result_path, len_limit, "%s", path)) || len > len_limit)
    {
      err = OB_BUF_NOT_ENOUGH;
      TBSYS_LOG(ERROR, "snprintf(path=%s)=>%ld", path, len);
    }
  }
  else
  {
    if(0 >= (len = snprintf(result_path, len_limit, "%s/%s", base,  path)) || len > len_limit)
    {
      err = OB_BUF_NOT_ENOUGH;
      TBSYS_LOG(ERROR, "snprintf(base=%s, path=%s)=>%d", base, path, err);
    }
  }
  return err;
}
    
int get_absolute_path(char* abs_path, const int64_t len_limit, const char* path)
{
  int err = OB_SUCCESS;
  char cwd[OB_MAX_FILE_NAME_LENGTH];
  if (NULL == abs_path || 0 >= len_limit || NULL == path)
  {
    err = OB_INVALID_ARGUMENT;
    TBSYS_LOG(ERROR, "get_absolute_path(abs_path=%p, limit=%ld, path=%s)=>%d", abs_path, len_limit, path, err);
  }
  else if (NULL == getcwd(cwd, sizeof(cwd)))
  {
    err = OB_BUF_NOT_ENOUGH;
    TBSYS_LOG(ERROR, "getcwd error[%s]", strerror(errno));
  }
  else if (OB_SUCCESS != (err = join_path(abs_path, len_limit, cwd, path)))
  {
    TBSYS_LOG(ERROR, "join_path(len_limit=%ld, base=%s, path=%s)=>%d", len_limit, cwd, path, err);
  }
  return err;
}

const char* get_absolute_path(const char* path)
{
  int err = OB_SUCCESS;
  static char abs_path[1<<10];
  if (OB_SUCCESS != (err = get_absolute_path(abs_path, sizeof(abs_path), path)))
  {
    TBSYS_LOG(ERROR, "get_absolute_path(path=%s)=>%d", path, err);
  }
  return OB_SUCCESS == err? abs_path: NULL;
}

int64_t get_cur_sign(const int64_t sign)
{
  int64_t local_ip = abs(tbsys::CNetUtil::getLocalAddr(NULL));
  int64_t ret = local_ip | sign;
  fprintf(stderr, "local_ip=%ld cur_sign=%ld calc_sign=%ld\n", local_ip, sign, ret);
  return ret;
}

void run_write(const int64_t sign, const int64_t num, const CmdLine &clp)
{
  int64_t prefix_start = INT32_MAX / clp.thread_num * num + 1;
  int64_t prefix_end = INT32_MAX / clp.thread_num * (num + 1);
  prefix_start = (sign << 32) | prefix_start;
  prefix_end = (sign << 32) | prefix_end;
  char str_port[32];
  char str_prefix_start[32];
  char str_prefix_end[32];
  sprintf(str_port, "%ld", clp.cluster_port);
  sprintf(str_prefix_start, "%ld", prefix_start);
  sprintf(str_prefix_end, "%ld", prefix_end);
  if (-1 == execl("./multi_write",
                  get_absolute_path("multi_write"),
                  "-a", clp.cluster_addr,
                  "-p", str_port,
                  "-d", clp.cluster_addr,
                  "-g", str_port,
                  "-r", clp.cluster_addr,
                  "-o", str_port,
                  "-s", str_prefix_start,
                  "-e", str_prefix_end,
                  "-n", "MAX",
                  NULL))
  {
    fprintf(stderr, "execl fail errno=%u\n", errno);
  }
}

void run_random_mget(const int64_t sign, const int64_t num, const CmdLine &clp)
{
  int64_t prefix_start = INT32_MAX / clp.thread_num * num + 1;
  prefix_start = (sign << 32) | prefix_start;
  char str_port[32];
  char str_prefix_start[32];
  sprintf(str_port, "%ld", clp.cluster_port);
  sprintf(str_prefix_start, "%ld", prefix_start);
  if (-1 == execl("./random_read",
                  get_absolute_path("random_read"),
                  "-a", clp.cluster_addr,
                  "-p", str_port,
                  "-r", clp.cluster_addr,
                  "-o", str_port,
                  "-s", str_prefix_start,
                  clp.check ? "-k" : "",
                  "-d",
                  "-m", "MAX",
                  NULL))
  {
    fprintf(stderr, "execl fail errno=%u\n", errno);
  }
}

void run_random_scan(const int64_t sign, const int64_t num, const CmdLine &clp)
{
  int64_t prefix_start = INT32_MAX / clp.thread_num * num + 1;
  prefix_start = (sign << 32) | prefix_start;
  char str_port[32];
  char str_prefix_start[32];
  sprintf(str_port, "%ld", clp.cluster_port);
  sprintf(str_prefix_start, "%ld", prefix_start);
  if (-1 == execl("./random_read",
                  get_absolute_path("random_read"),
                  "-a", clp.cluster_addr,
                  "-p", str_port,
                  "-r", clp.cluster_addr,
                  "-o", str_port,
                  "-s", str_prefix_start,
                  clp.check ? "-k" : "",
                  "-m", "MAX",
                  NULL))
  {
    fprintf(stderr, "execl fail errno=%u\n", errno);
  }
}

void run_total_scan(const int64_t sign, const int64_t num, const CmdLine &clp)
{
  int64_t prefix_start = INT32_MAX / clp.thread_num * num + 1;
  prefix_start = (sign << 32) | prefix_start;
  char str_port[32];
  char str_prefix_start[32];
  sprintf(str_port, "%ld", clp.cluster_port);
  sprintf(str_prefix_start, "%ld", prefix_start);
  if (-1 == execl("./total_scan",
                  get_absolute_path("total_scan"),
                  "-a", clp.cluster_addr,
                  "-p", str_port,
                  "-r", clp.cluster_addr,
                  "-o", str_port,
                  "-s", str_prefix_start,
                  clp.check ? "-k" : "",
                  "-m", "MAX",
                  NULL))
  {
    fprintf(stderr, "execl fail errno=%u\n", errno);
  }
}

void signal_handler_SIGTERM(int sn)
{
  UNUSED(sn);
  if (0 != kill(0, SIGKILL))
  {
    fprintf(stderr, "send kill sig to process group failed!\n");
  }
}

typedef void (*run_test_tp)(const int64_t, const int64_t, const CmdLine&);

int main(int argc, char **argv)
{
  signal(SIGABRT, signal_handler_SIGTERM);
  signal(SIGTERM, signal_handler_SIGTERM);
  signal(SIGINT, signal_handler_SIGTERM);
  CmdLine clp;
  parse_cmd_line(argc, argv, clp);

  int64_t n_test_handlers = 0;
  run_test_tp test_list[] = {run_write, run_random_mget, run_random_scan, run_total_scan};
  if (0 == strcmp("mixed", clp.test_type))
  {
    n_test_handlers = 3;
  }
  else if (0 == strcmp("write", clp.test_type))
  {
    test_list[0] = run_write;
    n_test_handlers = 1;
  }
  else if (0 == strcmp("random_mget", clp.test_type))
  {
    test_list[0] = run_random_mget;
    n_test_handlers = 1;
  }
  else if (0 == strcmp("random_scan", clp.test_type))
  {
    test_list[0] = run_random_scan;
    n_test_handlers = 1;
  }
  else if (0 == strcmp("total_scan", clp.test_type))
  {
    test_list[0] = run_total_scan;
    n_test_handlers = 1;
  }
  else
  {
    fprintf(stderr, "Invalid test_type=%s\n", clp.test_type);
    print_usage();
    exit(-1);
  }

  int64_t sign = get_cur_sign(clp.sign);
  for (int64_t i = 0; i < clp.thread_num * n_test_handlers; i++)
  {
    usleep(1000*1000);
    pid_t ret = fork();
    if (0 < ret)
    {
      fprintf(stderr, "launch child_process[%ld] success pid=%d\n", i, ret);
      continue;
    }
    else if (0 == ret)
    {
      test_list[i/clp.thread_num](sign, i%clp.thread_num, clp);
      break;
    }
    else
    {
      fprintf(stderr, "launch child_process[%ld] errno=%u\n", i, errno);
      break;
    }
  }
  int err = 0;
  int status = 0;
  pid_t pid = 0;
  if (0 > (pid = wait(&status)))
  {
    fprintf(stderr, "wait child termination failed!\n");
  }
  else if (0 != (err = kill(0, SIGKILL)))
  {
    fprintf(stderr, "send kill sig to process group failed!\n");
  }
  return err;
}

