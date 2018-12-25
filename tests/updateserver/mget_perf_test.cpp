#include <stdio.h>
#include <sys/types.h>
#include <sys/wait.h>
#include "mock_client.h"
#include "test_utils.h"

using namespace oceanbase;
using namespace common;
using namespace updateserver;

static const int64_t TIMEOUT = 10000000L;

void init_mock_client(const char *addr, int32_t port, MockClient &client)
{
  ObServer dst_host;
  dst_host.set_ipv4_addr(addr, port);
  client.init(dst_host);
}

char *addr = NULL;
int port = 0;
uint64_t table_id = 0;
ObVersionRange version_range;
char *rfile = NULL;
int64_t row_count = 0;
int64_t rne_percent = 0;
int64_t proc_count = 0;
int64_t thread_count = 0;
int64_t total_get_count = 0;
int64_t rowkey_length = 0;
int64_t rowkey_count = 0;
char *rowkey_array = NULL;
char *rne_array = NULL;

void prepare_rowkey_array()
{
  struct stat st;
  stat(rfile, &st);
  rowkey_count = st.st_size / rowkey_length;
  rowkey_array = new char[rowkey_count * rowkey_length];
  rne_array = new char[rowkey_count * rowkey_length];
  FILE *fd = fopen(rfile, "r");
  fread(rowkey_array, rowkey_length, rowkey_count, fd);
  memcpy(rne_array, rowkey_array, rowkey_count * rowkey_length);
  fclose(fd);
  for (int64_t i = 0; i < rowkey_count; i++)
  {
    int8_t *v = (int8_t*)&rne_array[i * rowkey_length + rowkey_length - sizeof(int8_t)];
    *v += 1;
  }
}

void print_scanner(ObScanner &scanner)
{
  int64_t row_counter = 0;
  while (OB_SUCCESS == scanner.next_cell())
  {
    ObCellInfo *ci = NULL;
    bool is_row_changed = false;
    scanner.get_cell(&ci, &is_row_changed);
    fprintf(stderr, "%s\n", common::print_cellinfo(ci, "CLI_SCAN"));
    if (is_row_changed)
    {
      row_counter++;
    }
  }
  bool is_fullfilled = false;
  int64_t fullfilled_num = 0;
  ObString last_rk;
  scanner.get_last_row_key(last_rk);
  scanner.get_is_req_fullfilled(is_fullfilled, fullfilled_num);
  fprintf(stderr, "is_fullfilled=%s fullfilled_num=%ld row_counter=%ld last_row_key=[%s]\n",
          STR_BOOL(is_fullfilled), fullfilled_num, row_counter, print_string(last_rk));
}

void build_get_param(struct drand48_data &seed, ObGetParam &get_param)
{
  get_param.reset();
  ObCellInfo ci;
  ci.table_id_ = table_id;
  ci.column_id_ = OB_FULL_ROW_COLUMN_ID;
  for (int64_t i = 0; i < row_count; i++)
  {
    int64_t rand = 0;
    lrand48_r(&seed, &rand);
    int64_t index = rand % rowkey_count;
    if (rne_percent > (rand % 100))
    {
      ci.row_key_.assign_ptr(&rne_array[index * rowkey_length], rowkey_length);
    }
    else
    {
      ci.row_key_.assign_ptr(&rowkey_array[index * rowkey_length], rowkey_length);
    }
    get_param.add_cell(ci);
  }
  get_param.set_version_range(version_range);
}

void *thread_func(void *data)
{
  MockClient *client = (MockClient*)data;
  int local_get_count = total_get_count / proc_count / thread_count;
  ObGetParam get_param;
  ObScanner scanner;
  struct drand48_data seed;
  srand48_r(pthread_self(), &seed);
  int64_t total_timeu = 0;
  for (int64_t i = 0; i < local_get_count; i++)
  {
    build_get_param(seed, get_param);
    int64_t timeu = tbsys::CTimeUtil::getTime();
    int ret = client->ups_get(get_param, scanner, TIMEOUT);
    total_timeu += (tbsys::CTimeUtil::getTime() - timeu);
    if (OB_SUCCESS != ret)
    {
      fprintf(stdout, "ups get fail ret=%d\n", ret);
      break;
    }
    print_scanner(scanner);
  }
  fprintf(stdout, "timeu arg=%0.2f\n", 1.0 * total_timeu / local_get_count);
  return NULL;
}

void proc_func()
{
  MockClient client;
  init_mock_client(addr, port, client);

  pthread_t *pd = new pthread_t[thread_count];
  for (int64_t i = 0; i < thread_count; i++)
  {
    pthread_create(&pd[i], NULL, thread_func, &client);
  }
  for (int64_t i = 0; i < thread_count; i++)
  {
    pthread_join(pd[i], NULL);
  }
  delete[] pd;
}

int main(int argc, char **argv)
{
  if (11 > argc)
  {
    fprintf(stdout, "./mget_perf_test [addr] [port] [table_id] [version_range] [rfile] \n"
                    "                 [row_count] [rne_percent] [proc_count] [thread_count] [total_get] [rowkey_length]\n");
    exit(1);
  }

  TBSYS_LOGGER.setLogLevel("ERROR");
  UNUSED(argc);
  addr = argv[1];
  port = atoi(argv[2]);
  table_id = atoll(argv[3]);
  version_range = str2range(argv[4]);
  rfile = argv[5];

  row_count = atoll(argv[6]);
  rne_percent = atoll(argv[7]);
  if (0 > rne_percent
      || 100 < rne_percent)
  {
    fprintf(stdout, "invalid rne_percent=%ld\n", rne_percent);
    exit(1);
  }
  proc_count = atoll(argv[8]);
  thread_count = atoll(argv[9]);
  total_get_count = atoll(argv[10]);
  if ((proc_count * thread_count) > total_get_count)
  {
    fprintf(stdout, "invalid total_get_count=%ld\n", total_get_count);
    exit(1);
  }
  rowkey_length = atoll(argv[11]);

  prepare_rowkey_array();

  int64_t timeu = tbsys::CTimeUtil::getTime();
  for (int64_t i = 0; i < proc_count; i++)
  {
    pid_t pid = fork();
    if (0 < pid)
    {
      fprintf(stdout, "launch child_process[%ld] success pid=%d\n", i, pid);
      continue;
    }
    else if (0 == pid)
    {
      proc_func();
      exit(0);
    }
    else
    {
      fprintf(stdout, "launch child_process[%ld] errno=%u\n", i, errno);
      break;
    }
  }
  for (int64_t i = 0; i < proc_count; i++)
  {
    int status = 0;
    wait(&status);
  }
  fprintf(stdout, "timeu=%ld\n", tbsys::CTimeUtil::getTime() - timeu);

  delete[] rowkey_array;
  delete[] rne_array;
}

