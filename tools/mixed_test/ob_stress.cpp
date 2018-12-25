#include <getopt.h>
#include <sys/syscall.h>
#include "mock_client.h"
#include "thread_store.h"
#include "utils.h"

using namespace oceanbase;
using namespace common;
using namespace updateserver;

#define STR_SIZE(value) static_cast<ObString::obstr_size_t>(value)

class Random
{
  public:
    uint64_t rand()
    {   
      return (*seed_.get() = ((*seed_.get() * 214013L + 2531011L) >> 16));
    }   
    void srand(uint64_t seed)
    {   
      *seed_.get() = seed;
    }   
  private:
    thread_store<uint64_t> seed_;
};

struct ThreadParam
{
  int64_t rk_start;
  int64_t rk_end;
  int64_t rk_length;

  int64_t mutator_rc;
  int64_t mutator_num;

  uint32_t col_num;
  const ObColumnSchemaV2 *table_schema;
  ObString table_name;

  MockClient *client;
  int64_t timeout;
};

struct MainParam
{
  char *serv_addr;
  int32_t serv_port;

  int64_t process_num;
  int64_t thread_num;
  int64_t mutator_rc;
  int64_t mutator_num;

  int64_t rk_start;
  int64_t rk_end;
  int64_t rk_length;

  uint64_t table_id;
  char *schema_file;

  MainParam()
  {
    serv_addr = NULL;
    serv_port = -1;
    process_num = 0;
    thread_num = 0;
    mutator_rc = 0;
    mutator_num = 0;
    rk_start = 0;
    rk_end = 0;
    rk_length = 0;
    table_id = 0;
    schema_file = NULL;
  };

  bool is_valid() const
  {
    bool bret = false;
    if (NULL != serv_addr
        && -1 != serv_port
        && 0 < process_num
        && 0 < thread_num
        && 0 < mutator_rc
        && 0 < mutator_num
        && ((rk_end - rk_start) >= thread_num)
        && 0 < rk_length
        && 0 < table_id
        && NULL != schema_file)
    {
      bret = true;
    }
    return bret;
  };
};

pid_t gettid()
{
  return static_cast<pid_t>(syscall(SYS_gettid));
}

const ObColumnSchemaV2 *get_table_schema(const char *file, const uint64_t table_id, int32_t &col_num, ObString &table_name)
{
  static ObSchemaManagerV2 schema;
  tbsys::CConfig config;
  schema.parse_from_file(file, config);
  const ObTableSchema *table_schema = schema.get_table_schema(table_id);
  table_name.assign_ptr(const_cast<char*>(table_schema->get_table_name()), STR_SIZE(strlen(table_schema->get_table_name())));
  return schema.get_table_schema(table_id, col_num);
}

void build_rk(uint64_t p, uint64_t cur_rk, ObString &rk_str)
{
  char format[16];
  int prefix = rk_str.length() < 8 ? rk_str.length() : 8;
  sprintf(format, "%c0%dlu", '%', prefix);
  snprintf(rk_str.ptr(), prefix + 1, format, p);
  sprintf(format, "%c0%dlu", '%', rk_str.length() - prefix);
  snprintf(rk_str.ptr() + prefix, rk_str.length() - prefix + 1, format, cur_rk);
}

void fill_mutator(const ObString &table_name, const ObString &rk_str, const ObColumnSchemaV2 &col_schema, char *vc_buf,
                  ObMutator &mutator, PageArena<char> &allocer)
{
  ObObj value;
  ObString col_name;
  col_name.assign_ptr(const_cast<char*>(col_schema.get_name()), STR_SIZE(strlen(col_schema.get_name())));
  switch (col_schema.get_type())
  {
    case ObIntType:
      value.set_int(atoll(rk_str.ptr()));
      break;
    case ObVarcharType:
      memcpy(vc_buf, rk_str.ptr(), rk_str.length());
      value.set_varchar(ObString(STR_SIZE(col_schema.get_size()), STR_SIZE(col_schema.get_size()), vc_buf));
      break;
    default:
      value.set_null();
      break;
  }
  mutator.update(table_name, TestRowkeyHelper(rk_str, &allocer), col_name, value);
}

void *thread_func(void *data)
{
  ThreadParam *tp = (ThreadParam*)data;
  int64_t cur_rk = tp->rk_start;
  char *rk_buf = new char[tp->rk_length + 1];
  ObString rk_str(STR_SIZE(tp->rk_length), STR_SIZE(tp->rk_length), rk_buf);
  ObMutator mutator;
  int64_t total_timeu = 0;
  char *vc_buf = new char[2 * 1024 * 1024];
  Random rand;
  memset(vc_buf, '0', 2 * 1024 * 1024);
  rand.srand((uint64_t)vc_buf);
  //int64_t rk_size = tp->rk_end - tp->rk_start;
  for (int64_t m = 0; m < tp->mutator_num; m++)
  {
    mutator.reset();
    PageArena<char> allocer;
    allocer.reuse();
    for (int64_t r = 0; r < tp->mutator_rc; r++)
    {
      //build_rk(cur_rk, rk_str);
      build_rk(rand.rand(), rand.rand(), rk_str);
      for (int64_t c = 0; c < tp->col_num; c++)
      {
        fill_mutator(tp->table_name, rk_str, tp->table_schema[c], vc_buf, mutator, allocer);
      }
      if ((++cur_rk) >= tp->rk_end)
      {
        cur_rk = tp->rk_start;
      }
    }
    int64_t timeu = tbsys::CTimeUtil::getTime();
    int ret = tp->client->ups_apply(mutator, tp->timeout);
    timeu = tbsys::CTimeUtil::getTime() - timeu;
    TBSYS_LOG(INFO, "[%d] apply ret=%d timeu=%ld", gettid(), ret, timeu);
    total_timeu += timeu;
  }
  TBSYS_LOG(INFO, "[%d] total timeu=%ld", gettid(), total_timeu);
  delete[] vc_buf;
  delete[] rk_buf;
  return NULL;
}

void run(const pid_t pid, MainParam &param)
{
  char log_file_buffer[32];
  sprintf(log_file_buffer, "ob_stress.log.p%d.c%d", pid, getpid());
  TBSYS_LOGGER.rotateLog(log_file_buffer);
  TBSYS_LOGGER.setFileName(log_file_buffer);
  TBSYS_LOGGER.setLogLevel("info");

  pthread_t *pd = new pthread_t[param.thread_num];
  ThreadParam *tp = new ThreadParam[param.thread_num];

  ObServer dst_host;
  dst_host.set_ipv4_addr(param.serv_addr, param.serv_port);
  MockClient client;
  client.init(dst_host);

  int32_t col_num = 0;
  ObString table_name;
  const ObColumnSchemaV2 *table_schema = get_table_schema(param.schema_file, param.table_id, col_num, table_name);

  int64_t rk_step = (param.rk_end - param.rk_start) / param.thread_num;

  for (int64_t i = 0; i < param.thread_num; i++)
  {
    tp[i].rk_start = param.rk_start + i * rk_step;
    tp[i].rk_end = param.rk_start + (i + 1) * rk_step;
    tp[i].rk_length = param.rk_length;
    tp[i].mutator_rc = param.mutator_rc;
    tp[i].mutator_num = param.mutator_num;
    tp[i].col_num = col_num;
    tp[i].table_schema = table_schema;
    tp[i].table_name = table_name;
    tp[i].client = &client;
    tp[i].timeout = 10000000;

    pthread_create(&pd[i], NULL, thread_func, &tp[i]);
  }

  for (int64_t i = 0; i < param.thread_num; i++)
  {
    pthread_join(pd[i], NULL);
  }

  delete[] tp;
  delete[] pd;
}

void print_usage()
{
  fprintf(stderr, "\n");
  fprintf(stderr, "ob_stress [OPTION]\n");
  fprintf(stderr, "   -a|--serv_addr updateserver/mergeserver address\n");
  fprintf(stderr, "   -p|--serv_port updateserver/mergeserver port\n\n");

  fprintf(stderr, "   -o|--process_num \n");
  fprintf(stderr, "   -t|--thread_num \n");
  fprintf(stderr, "   -r|--mutator_rc row count per mutator\n");
  fprintf(stderr, "   -n|--mutator_num mutator num per thread\n\n");

  fprintf(stderr, "   -s|--rk_start rowkey start number\n");
  fprintf(stderr, "   -e|--rk_end rowkey end number\n");
  fprintf(stderr, "   -l|--rk_length rowkey lenght\n\n");

  fprintf(stderr, "   -i|--table_id \n");
  fprintf(stderr, "   -f|--schema_file \n\n");

  fprintf(stderr, "   -h|--help print help info\n\n");
}

void parse_cmd_line(int argc, char **argv, MainParam &param)
{
  int opt = 0;
  const char* opt_string = "a:p:o:t:r:n:s:e:l:i:f:h";
  struct option longopts[] =
  {
    {"serv_addr", 1, NULL, 'a'},
    {"serv_port", 1, NULL, 'p'},
    {"process_num", 1, NULL, 'o'},
    {"thread_num", 1, NULL, 't'},
    {"mutator_rc", 1, NULL, 'r'},
    {"mutator_num", 1, NULL, 'n'},
    {"rk_start", 1, NULL, 's'},
    {"rk_end", 1, NULL, 'e'},
    {"rk_length", 1, NULL, 'l'},
    {"table_id", 1, NULL, 'i'},
    {"schema_file", 1, NULL, 'f'},
    {"help", 0, NULL, 'h'},
    {0, 0, 0, 0}
  };
  while ((opt = getopt_long(argc, argv, opt_string, longopts, NULL)) != -1)
  {
    switch (opt)
    {
    case 'a':
      param.serv_addr = optarg;
      break;
    case 'p':
      param.serv_port = atoi(optarg);
      break;
    case 'o':
      param.process_num = atoll(optarg);
      break;
    case 't':
      param.thread_num = atoll(optarg); 
      break;
    case 'r':
      param.mutator_rc = atoll(optarg);
      break;
    case 'n':
      if (0 == strcmp(optarg, "MAX"))
      {
        param.mutator_num = INT64_MAX;
      }
      else
      {
        param.mutator_num = atoll(optarg);
      }
      break;
    case 's':
      param.rk_start = atoll(optarg);
      break;
    case 'e':
      if (0 == strcmp(optarg, "MAX"))
      {
        param.rk_end = INT64_MAX;
      }
      else
      {
        param.rk_end = atoll(optarg);
      }
      break;
    case 'l':
      param.rk_length = atoll(optarg);
      break;
    case 'i':
      param.table_id = atoll(optarg);
      break;
    case 'f':
      param.schema_file = optarg;
      break;
    case 'h':
    default:
      print_usage();
      exit(1);
    }
  }
  if (!param.is_valid())
  {
    print_usage();
    exit(-1);
  }
}

int main(int argc, char **argv)
{
  MainParam param;
  parse_cmd_line(argc, argv, param);

  for (int64_t i = 0; i < param.process_num; i++)
  {
    pid_t pid = getpid();
    pid_t cid = fork();
    if (0 == cid)
    {
      run(pid, param);
      exit(0);
    }
    else
    {
      TBSYS_LOG(INFO, "fork child process succ cid=%d", cid);
    }
  }
}

