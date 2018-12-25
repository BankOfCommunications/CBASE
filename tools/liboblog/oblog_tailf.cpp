#include <sys/time.h>
#include <stdio.h>
#include "liboblog.h"
#include <getopt.h>         // getopt_long
#include <stdlib.h>         // strtoull
#include <string.h>         // memset
#include <map>              // map
#include <string>           // string
#include "ob_log_spec.h"    // ObLogSpec

using namespace oceanbase::common;
using namespace oceanbase::liboblog;

// Macro defination
//#define USE_MAP_CONFIG

//#define TEST_FOR_REENTRANT

#define LOG_ERR(str, ...) \
  do { \
    fprintf(stderr, "ERROR: %s(), line=%d: " str, \
        __FUNCTION__, __LINE__, ##__VA_ARGS__); \
  } while (0)

#define LOG(str, ...) \
  do { \
    fprintf(stderr, str, ##__VA_ARGS__); \
  } while (0)

#define LOG_ITER(index, str, ...) \
  do { \
    fprintf(stderr, "[ITER %d] " str, index, ##__VA_ARGS__); \
  } while (0)

#define MAX_ITERATOR_NUM 10
#define CONFIG_FILE_MAX_SIZE (1 << 20)

// Typedefs
typedef int64_t (*str_to_int_pt) (const char *str);
typedef std::pair<std::string, std::string>  MapPair;

// Global variables
int g_iterator_num = 1;
uint64_t g_checkpoint = 0;                // checkpoint
bool g_print_result = false;
const char *g_config = NULL;              // configure file
std::vector<uint64_t> g_total_partitions; // total partitions

ObLogSpecFactory g_log_spec_factory;
IObLogSpec *g_log_spec = NULL;
IObLog *g_log[MAX_ITERATOR_NUM];
std::vector<uint64_t> g_partitions[MAX_ITERATOR_NUM];
int64_t g_run_time_usec = -1;

// Static functions
static int init();
static int start();
static void destroy();
static int init_ob_log();
static int init_log_spec();
static int64_t my_atoll(const char *str);
static int parse_args(int argc, char **argv);
static void print_usage(const char *prog_name);
static const char *print_record_type(int type);
static const char *print_src_category(int src_category);
static inline int64_t get_cur_microseconds_time(void);
static inline int64_t tv_to_microseconds(const timeval & tp);
static void print_binlog_record(IBinlogRecord *br, int index);
void init_config_map_(std::map<std::string, std::string> &config_map);
static int64_t split_string(const char *str, int64_t *values, str_to_int_pt *callbacks, const int64_t size, const char d);

int main(int argc, char **argv)
{
  int ret = OB_SUCCESS;

  if (OB_SUCCESS != (ret = parse_args(argc, argv)))
  {
    LOG_ERR("failed to parse arguments\n");
  }
  else if (OB_SUCCESS != (ret = init()))
  {
    LOG_ERR("failed to initialize liboblog\n");
  }
  else if (OB_SUCCESS != (ret = start()))
  {
    LOG_ERR("failed to start liboblog\n");
  }

  destroy();

#ifdef TEST_FOR_REENTRANT

  LOG("TEST FOR REENTRANT\n");

  if (OB_SUCCESS != (ret = init()))
  {
    LOG_ERR("failed to initialize liboblog\n");
  }
  else if (OB_SUCCESS != (ret = start()))
  {
    LOG_ERR("failed to start liboblog\n");
  }

  destroy();
#endif

  return 0;
}

int64_t my_atoll(const char *str)
{
  return ::atoll(str);
}

int64_t split_string(const char *str, int64_t *values, str_to_int_pt *callbacks, const int64_t size, const char d)
{
  int64_t idx = 0;
  if (NULL != str)
  {
    static const int64_t BUFFER_SIZE = 1024;
    char buffer[BUFFER_SIZE];
    snprintf(buffer, BUFFER_SIZE, "%s", str);
    char *iter = buffer;
    char *deli = NULL;
    char *end = iter + strlen(iter);

    while (iter < end
          && idx < size)
    {
      deli = strchr(iter, d);
      if (NULL != deli)
      {
        *deli = '\0';
      }

      values[idx] = callbacks[idx](iter);
      if (0 > values[idx])
      {
        break;
      }

      idx += 1;
      if (NULL == deli)
      {
        break;
      }
      iter = deli + 1;
    }
  }
  return idx;
}

inline int64_t tv_to_microseconds(const timeval & tp)
{
  return (((int64_t) tp.tv_sec) * 1000000 + (int64_t) tp.tv_usec);
}

inline int64_t get_cur_microseconds_time(void)
{
  struct timeval tp;
  gettimeofday(&tp, NULL);
  return tv_to_microseconds(tp);
}

void print_binlog_record(IBinlogRecord *br, int index)
{
  if (NULL != br && 0 < index)
  {
    if (EBEGIN == br->recordType())
    {
      LOG_ITER(index, "BEGIN    TM=[%ld]  CP=[%s]  SRC_CATEGORY=[%s]\n",
          br->getTimestamp(), br->getCheckpoint(), print_src_category(br->getSrcCategory()));
      LOG_ITER(index, "\n");
    }
    else if (ECOMMIT == br->recordType())
    {
      LOG_ITER(index, "COMMIT   TM=[%ld]  CP=[%s]  SRC_CATEGORY=[%s]\n\n",
          br->getTimestamp(), br->getCheckpoint(), print_src_category(br->getSrcCategory()));
    }
    else if (HEARTBEAT == br->recordType())
    {
      LOG_ITER(index, "HEARTBEAT  TM=[%ld]  SRC_CATEGORY=[%s]\n\n",
          br->getTimestamp(), print_src_category(br->getSrcCategory()));
    }
    else
    {
      br->toString();
      LOG_ITER(index, "  [%s] DB=[%s] TB=[%s] CP=[%s] LOG_EVENT=[%s]\n",
              print_record_type(br->recordType()),
              br->dbname(), br->getTableMeta()->getName(),
              br->getCheckpoint(),
              br->firstInLogevent() ? "true" : "false");
      std::vector<std::string*>::const_iterator ic = br->newCols().begin();
      std::vector<std::string*>::const_iterator io = br->oldCols().begin();
      LOG_ITER(index, "    NewCols  ");
      while (true)
      {
        if (ic == br->newCols().end())
        {
          break;
        }
        LOG("  [%s]", *ic ? (*ic)->c_str() : NULL);
        ic++;
      }
      LOG("\n");
      LOG_ITER(index, "    OldCols  ");
      while (true)
      {
        if (io == br->oldCols().end())
        {
          break;
        }
        LOG("  [%s]", *io ? (*io)->c_str() : NULL);
        io++;
      }
      LOG("\n");
      LOG_ITER(index, "\n");
    }
  }
}

const char *print_record_type(int type)
{
  static const char *str = "UNKNOWN";

  switch (type)
  {
    case EDELETE:
      str = "DELETE";
      break;

    case EINSERT:
      str = "INSERT";
      break;

    case EREPLACE:
      str = "REPLACE";
      break;

    case EUPDATE:
      str = "UPDATE";
      break;

    case HEARTBEAT:
      str = "HEARTBEAT";
      break;

    case CONSISTENCY_TEST:
      str = "CONSISTENCY_TEST";
      break;

    case EBEGIN:
      str = "EBEGIN";
      break;

    case ECOMMIT:
      str = "ECOMMIT";
      break;

    case EDDL:
      str = "EDDL";
      break;

    case EROLLBACK:
      str = "EROLLBACK";
      break;

    case EDML:
      str = "EDML";
      break;

    default:
      str = "UNKNOWN";
      break;
  }

  return str;
}

const char *print_src_category(int src_category)
{
  static const char *sc_name = "UNKNOWN";

  switch (src_category)
  {
    case SRC_FULL_RECORDED:
      sc_name = "SRC_FULL_RECORDED";
      break;

    case SRC_FULL_RETRIEVED:
      sc_name = "SRC_FULL_RETRIEVED";
      break;

    case SRC_FULL_FAKED:
      sc_name = "SRC_FULL_FAKED";
      break;

    case SRC_PART_RECORDED:
      sc_name = "SRC_PART_RECORDED";
      break;

    default:
      sc_name = "UNKNOWN";
      break;
  }

  return sc_name;
}

void print_usage(const char *prog_name)
{
  LOG("USAGE: %s -f config_file_path -c valid_checkpoint -p partition_list [-o] [-n iterator_num]\n\n"
      "   -f, --config_file     configuration file\n"
      "   -c, --checkpoint      checkpoint\n"
      "   -p, --partition       database partition subscribed, eg. 0,1,2,3 \n"
      "   -o, --output_result   output result\n"
      "   -n, --iterator_num    number of iterators [1, %d], default 1\n"
      "   -t, --run_time_usec   run time in micro seconds, default -1: means forever\n"
      "   -h, --help            display this help and exit\n"
      "eg: %s -f liboblog.conf -c 1000000000 -p 0,1,2,3 -o\n",
      prog_name, MAX_ITERATOR_NUM, prog_name);
}

int parse_args(int argc, char **argv)
{
  int ret = OB_SUCCESS;

  // option variables
  int opt = -1;
  const char *opt_string = "hof:c:p:n:t:";
  struct option long_opts[] =
  {
    {"config_file",1,NULL,'f'},
    {"checkpoint", 1, NULL, 'c'},
    {"partitoin", 1, NULL, 'p'},
    {"output_result",0,NULL, 'o'},
    {"help", 0, NULL, 'h'},
    {"iterator_num", 1, NULL, 'n'},
    {"run_time_usec", 1, NULL, 't'},
    {0, 0, 0, 0}
  };

  // Parse command line
  while (OB_SUCCESS == ret &&
      (opt = getopt_long(argc, argv, opt_string, long_opts, NULL)) != -1)
  {
    switch (opt)
    {
      case 'h':
        print_usage(argv[0]);
        ret = OB_IN_STOP_STATE;
        break;

      case 'f':
        g_config = optarg;
        break;

      case 'c':
        g_checkpoint = strtoull(optarg, NULL, 10);
        break;

      case 'p':
        {
          const int64_t ARRAY_SIZE = 1024;
          int64_t values[ARRAY_SIZE];
          str_to_int_pt callbacks[ARRAY_SIZE];
          for (int64_t i = 0; i < ARRAY_SIZE; i++)
          {
            callbacks[i] = my_atoll;
          }
          int64_t ret_size = split_string(optarg, values, callbacks, ARRAY_SIZE, ',');
          if (0 >= ret_size)
          {
            LOG_ERR("invalid checkpoint: %s\n", optarg);
            ret = OB_INVALID_ARGUMENT;
          }
          else
          {
            for (int64_t i = 0; i < ret_size; i++)
            {
              g_total_partitions.push_back(values[i]);
            }
          }
        }
        break;

      case 'o':
        g_print_result = true;
        break;

      case 'n':
        g_iterator_num = atoi(optarg);
        if (0 >= g_iterator_num || MAX_ITERATOR_NUM < g_iterator_num)
        {
          LOG_ERR("invalid argument: iterator_num = %d, which should be in [1, %d]\n",
              g_iterator_num, MAX_ITERATOR_NUM);

          ret = OB_INVALID_ARGUMENT;
        }
        break;

      case 't':
        g_run_time_usec = my_atoll(optarg);
        break;

      default:
        ret = OB_ERROR;
        LOG_ERR("unknown parameters\n");
        break;
    } // end switch
  } // end while

  if (OB_SUCCESS == ret)
  {
    if (NULL == g_config)
    {
      ret = OB_INVALID_ARGUMENT;
      LOG_ERR("configuration file is missing\n");
      print_usage(argv[0]);
    }
    else if (0 >= g_total_partitions.size())
    {
      LOG_ERR("database partition subcribed is missing\n");
      print_usage(argv[0]);
    }
    else if (g_iterator_num > static_cast<int>(g_total_partitions.size()))
    {
      LOG_ERR("iterator number %d is bigger than database partition number %d\n",
          g_iterator_num, (int)g_total_partitions.size());
    }
  }

  return ret;
}

int init()
{
  int ret = OB_SUCCESS;

  if (OB_SUCCESS == (ret = init_log_spec()))
  {
    ret = init_ob_log();
  }

  return ret;
}

int init_log_spec()
{
  int ret = OB_SUCCESS;

  // Contruct liboblog instance
  g_log_spec = g_log_spec_factory.construct_log_spec();
  if (NULL == g_log_spec)
  {
    LOG_ERR("failed to contruct liboblog instance\n");
    ret = OB_ERR_UNEXPECTED;
  }

  if (OB_SUCCESS == ret)
  {
#ifdef USE_MAP_CONFIG
    std::map<std::string, std::string> config_map;

    init_config_map_(config_map);
    g_log_spec->init(config_map, g_checkpoint);
    if (OB_SUCCESS != ret)
    {
      LOG_ERR("failed to initialize liboblog instance, g_checkpoint = %lu, ret=%d\n",
          g_checkpoint, ret);
    }
#else
    // Initialize liboblog instance
    ret = g_log_spec->init(g_config, g_checkpoint);
    if (OB_SUCCESS != ret)
    {
      LOG_ERR("failed to initialize liboblog instance, g_config=%s, g_checkpoint=%lu, ret=%d\n",
          g_config, g_checkpoint, ret);
    }
#endif
  }

  if (OB_SUCCESS == ret)
  {
    // Print perf information to stderr
    dynamic_cast<ObLogSpec *>(g_log_spec)->set_perf_print_to_stderr(true);
  }

  return ret;
}

void init_config_map_(std::map<std::string, std::string> &config_map)
{
  config_map.insert(MapPair("cluster_url","\"http://10.232.4.35:9000/method=get&key=obtest_wanhong.wwh_10.235.162.5\""));
  config_map.insert(MapPair("cluster_user","admin"));
  config_map.insert(MapPair("cluster_password","admin"));
//  config_map.insert(MapPair("clusterAddress","10.235.162.5:2828"));
  config_map.insert(MapPair("router_thread_num","16"));
  config_map.insert(MapPair("log_fpath","./ss.log"));
  config_map.insert(MapPair("query_back", "0"));
  config_map.insert(MapPair("query_back_read_consistency", "strong"));
  config_map.insert(MapPair("skip_dirty_data", "1"));
//  config_map.insert(MapPair("tb_select", "test,ipm_inventory"));
  config_map.insert(MapPair("tb_select", "test"));
  config_map.insert(MapPair("test_dbn_format", "test_%04lu"));
  config_map.insert(MapPair("test_tbn_format", "test_%04lu"));
  config_map.insert(MapPair("test_db_partition_lua", "return arg[1] % 1"));
  config_map.insert(MapPair("test_tb_partition_lua", "return arg[1] % 32"));
  config_map.insert(MapPair("ipm_inventory_dbn_format", "ipm_inventory_%04lu"));
  config_map.insert(MapPair("ipm_inventory_tbn_format", "ipm_inventory_%04lu"));
  config_map.insert(MapPair("ipm_inventory_db_partition_lua", "return arg[1] % 16"));
  config_map.insert(MapPair("ipm_inventory_tb_partition_lua", "return arg[1] % 32"));
}

int init_ob_log()
{
  int ret = OB_SUCCESS;

  (void)memset(g_log, 0, sizeof(g_log) * sizeof(IObLog *));

  int partition_num = static_cast<int>(g_total_partitions.size());

  int index = 0;
  for (int i = 0; i < g_iterator_num - 1; i++)
  {
    LOG("%dth iterator's DB partitions: ", i + 1);
    for (int j = 0; j < partition_num / g_iterator_num; j++)
    {
      uint64_t part = g_total_partitions[index++];
      g_partitions[i].push_back(part);
      LOG("%lu ", part);
    }

    LOG("\n");
  }

  if (index < partition_num)
  {
    LOG("%dth iterator's DB partitions: ", g_iterator_num);
    while (index < partition_num)
    {
      uint64_t part = g_total_partitions[index++];
      g_partitions[g_iterator_num - 1].push_back(part);
      LOG("%lu ", part);
    }
    LOG("\n");
  }

  // Construct and initialize iterators
  for (int i = 0; i < g_iterator_num; i++)
  {
    g_log[i] = g_log_spec_factory.construct_log();
    if (NULL == g_log[i])
    {
      LOG_ERR("failed to contruct %dth IObLog, ret=%d\n", i, ret);
      ret = OB_ERR_UNEXPECTED;
    }
    else
    {
      ret = g_log[i]->init(g_log_spec, g_partitions[i]);
      if (OB_SUCCESS != ret)
      {
        LOG_ERR("failed to initialize %dth iterator, ret=%d\n", i, ret);
      }
    }
  }

  return ret;
}

int start()
{
  int ret = OB_SUCCESS;

  // Launch liboblog instance
  ret = g_log_spec->launch();
  if (OB_SUCCESS != ret)
  {
    LOG_ERR("failed to launch liboblog instance, ret=%d\n", ret);
  }

  IBinlogRecord *bin_log_record = NULL;
  int64_t timeout = g_run_time_usec >= 0 ? g_run_time_usec : (1000000L * 1000000L);
  int64_t start_time = tbsys::CTimeUtil::getTime();
  int64_t end_time = g_run_time_usec + start_time;

  while (OB_SUCCESS == ret)
  {
    for (int i = 0; OB_SUCCESS == ret && i < g_iterator_num; i++)
    {
      bool  is_commit_or_heartbeat = false;
      do
      {
        is_commit_or_heartbeat = false;
        g_log[i]->next_record(&bin_log_record, timeout);
        if (NULL == bin_log_record)
        {
          ret = OB_IN_STOP_STATE;
          break;
        }
        else
        {
          is_commit_or_heartbeat = ((ECOMMIT == bin_log_record->recordType())
              || (HEARTBEAT == bin_log_record->recordType()));
        }

        if (g_print_result) print_binlog_record(bin_log_record, i + 1);

        g_log[i]->release_record(bin_log_record);
        bin_log_record = NULL;
      } while (! is_commit_or_heartbeat);
    }

    if (g_run_time_usec >= 0 && tbsys::CTimeUtil::getTime() > end_time)
    {
      break;
    }
  }

  return ret;
}

void destroy()
{
  g_log_spec->stop();

  for (int i = 0; i < g_iterator_num; i++)
  {
    if (NULL != g_log[i])
    {
      g_log[i]->destroy();
      g_log_spec_factory.deconstruct(g_log[i]);
      g_log[i] = NULL;
    }
  }

  if (NULL != g_log_spec)
  {
    g_log_spec->destroy();
    g_log_spec_factory.deconstruct(g_log_spec);
    g_log_spec = NULL;
  }
}
