#include <getopt.h>
#include "common/ob_malloc.h"
#include "mutator_builder.h"
#include "test_utils.h"
#include "utils.h"

using namespace oceanbase;
using namespace common;

struct CmdLine
{
  static const int64_t DEFAULT_TABLE_START_VERSION = 2;
  static const int64_t DEFAULT_PREFIX_START = 1;
  static const int64_t DEFAULT_PREFIX_END = 1024;
  static const int64_t DEFAULT_MUTATOR_NUM = 1024;
  static const int64_t DEFAULT_SUFFIX_LENGTH = 20;
  static const int64_t DEFAULT_MAX_ROW = 128;
  static const int64_t DEFAULT_MAX_SUFFIX = DEFAULT_SUFFIX_NUM_PER_PREFIX;
  static const int64_t DEFAULT_MAX_CELL = 32;
  static const char *DEFAULT_LOG_LEVEL;

  char *write_addr;
  int64_t write_port;
  char *schema_addr;
  int64_t schema_port;
  char *read_addr;
  int64_t read_port;
  bool using_id;
  int64_t table_start_version;
  char *schema_file;
  int64_t prefix_start;
  int64_t prefix_end;
  int64_t mutator_num;
  int64_t suffix_length;
  int64_t max_suffix;

  int64_t max_row;
  int64_t max_cell;

  const char *log_level;

  void log_all()
  {
    TBSYS_LOG(INFO, "write_addr=%s write_port=%ld schema_addr=%s schema_port=%ld read_addr=%s read_port=%ld using_id=%s "
              "table_start_version=%ld schema_file=%s prefix_start=%ld prefix_end=%ld "
              "mutator_num=%ld suffix_length=%ld "
              "max_row=%ld max_suffix=%ld max_cell=%ld log_level=%s",
              write_addr, write_port, schema_addr, schema_port, read_addr, read_port, STR_BOOL(using_id),
              table_start_version, schema_file, prefix_start, prefix_end,
              mutator_num, suffix_length,
              max_row, max_suffix, max_cell, log_level);
  };

  CmdLine()
  {
    write_addr = NULL;
    write_port = -1;
    schema_addr = NULL;
    schema_port = -1;
    read_addr = NULL;
    read_port = -1;
    using_id = false;
    schema_file = NULL;
    table_start_version = DEFAULT_TABLE_START_VERSION; 
    prefix_start = DEFAULT_PREFIX_START;
    prefix_end = DEFAULT_PREFIX_END;
    mutator_num = DEFAULT_MUTATOR_NUM;
    suffix_length = DEFAULT_SUFFIX_LENGTH;
    max_row = DEFAULT_MAX_ROW;
    max_suffix = DEFAULT_MAX_SUFFIX;
    max_cell = DEFAULT_MAX_CELL;
    log_level = DEFAULT_LOG_LEVEL;
  };

  bool is_valid()
  {
    bool bret = false;
    if (NULL != write_addr
        && 0 < write_port
        && ((NULL != schema_addr
              && 0 < schema_port)
            || NULL != schema_file)
        && NULL != read_addr
        && 0 < read_port)
    {
      bret = true;
    }
    return bret;
  };
};

const char *CmdLine::DEFAULT_LOG_LEVEL = "info";

void print_usage()
{
  fprintf(stderr, "\n");
  fprintf(stderr, "multi_write [OPTION]\n");
  fprintf(stderr, "   -a|--write_addr write server/cluster address, end with '@' to direct access server, otherwise using API\n");
  fprintf(stderr, "   -p|--write_port write server/cluster port\n");
  fprintf(stderr, "   -d|--read_addr read server/cluster address, end with '@' to direct access server, otherwise using API\n");
  fprintf(stderr, "   -g|--read_port read server/cluster port\n");
  fprintf(stderr, "   -r|--schema_addr schema server address\n");
  fprintf(stderr, "   -o|--schema_port schema server port\n");
  fprintf(stderr, "   -f|--schema_file if schema_file set, will not fetch from schema server\n");
  fprintf(stderr, "   -i|--using_id using table_id and column_id to scan [if not set, default using name]\n");
  fprintf(stderr, "   -w|--log_level [default %s]\n\n", CmdLine::DEFAULT_LOG_LEVEL);

  fprintf(stderr, "   -t|--table_start_version to read from updateserver [default %ld]\n", CmdLine::DEFAULT_TABLE_START_VERSION);
  fprintf(stderr, "   -s|--prefix_start rowkey prefix start number [default %ld]\n", CmdLine::DEFAULT_PREFIX_START);
  fprintf(stderr, "   -e|--prefix_end rowkey prefix start number [default %ld], support MAX\n", CmdLine::DEFAULT_PREFIX_END);
  fprintf(stderr, "   -n|--mutator_num total mutator number to apply [default %ld], support MAX\n", CmdLine::DEFAULT_MUTATOR_NUM);
  fprintf(stderr, "   -l|--suffix_length rowkey suffix length [default %ld]\n", CmdLine::DEFAULT_SUFFIX_LENGTH);
  fprintf(stderr, "   -u|--max_suffix max suffix number per prefix [default %ld]\n\n", CmdLine::DEFAULT_MAX_SUFFIX);

  fprintf(stderr, "   -m|--max_row max row number per mutator [default %ld]\n", CmdLine::DEFAULT_MAX_ROW);
  fprintf(stderr, "   -c|--max_cell max cell number per row [default %ld]\n", CmdLine::DEFAULT_MAX_CELL);

  fprintf(stderr, "\n");
}

void parse_cmd_line(int argc, char **argv, CmdLine &clp)
{
  int opt = 0;
  const char* opt_string = "a:p:r:o:d:g:it:f:s:e:n:m:u:c:w:h";
  struct option longopts[] =
  {
    {"write_addr", 1, NULL, 'a'},
    {"write_port", 1, NULL, 'p'},
    {"schema_addr", 1, NULL, 'r'},
    {"schema_port", 1, NULL, 'o'},
    {"read_addr", 1, NULL, 'd'},
    {"read_port", 1, NULL, 'g'},
    {"using_id", 0, NULL, 'i'},
    {"table_start_version", 1, NULL, 't'},
    {"schema_file", 1, NULL, 'f'},
    {"prefix_start", 1, NULL, 's'},
    {"prefix_end", 1, NULL, 'e'},
    {"mutator_num", 1, NULL, 'n'},
    {"max_row", 1, NULL, 'm'},
    {"max_suffix", 1, NULL, 'u'},
    {"max_cell", 1, NULL, 'c'},
    {"log_level", 1, NULL, 'w'},
    {"help", 0, NULL, 'h'},
    {0, 0, 0, 0}
  };
  while ((opt = getopt_long(argc, argv, opt_string, longopts, NULL)) != -1)
  {
    switch (opt)
    {
    case 'a':
      clp.write_addr = optarg;
      break;
    case 'p':
      clp.write_port = atoi(optarg);
      break;
    case 'r':
      clp.schema_addr = optarg;
      break;
    case 'o':
      clp.schema_port = atoi(optarg);
      break;
    case 'd':
      clp.read_addr = optarg;
      break;
    case 'g':
      clp.read_port = atoi(optarg);
      break;
    case 'i':
      clp.using_id = true;
      break;
    case 't':
      clp.table_start_version = atoll(optarg);
      break;
    case 'f':
      clp.schema_file = optarg;
      break;
    case 's':
      clp.prefix_start = atoll(optarg);
      break;
    case 'e':
      if (0 == strcmp("MAX", optarg))
      {
        clp.prefix_end = INT64_MAX;
      }
      else
      {
        clp.prefix_end = atoll(optarg);
      }
      break;
    case 'n':
      if (0 == strcmp("MAX", optarg))
      {
        clp.mutator_num = INT64_MAX;
      }
      else
      {
        clp.mutator_num = atoll(optarg);
      }
      break;
    case 'l':
      clp.suffix_length = atoll(optarg);
      break;
    case 'm':
      clp.max_row = atoll(optarg);
      break;
    case 'u':
      clp.max_suffix = atoll(optarg);
      break;
    case 'w':
      clp.log_level = optarg;
      break;
    case 'c':
      clp.max_cell = atoll(optarg);
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

int main(int argc, char **argv)
{
  CmdLine clp;
  parse_cmd_line(argc, argv, clp);

  char buffer[1024];
  sprintf(buffer, "multi_write.%ld.log", clp.prefix_start);
  TBSYS_LOGGER.rotateLog(buffer);
  TBSYS_LOGGER.setFileName(buffer);
  TBSYS_LOGGER.setLogLevel(getenv("log_level")? getenv("log_level"): clp.log_level);
  clp.log_all();
  TBSYS_LOG(INFO, "cmdline: %s", my_str_join(argc, argv));
  TBSYS_LOG(INFO, "getpid()=%d", getpid());

  ob_init_memory_pool();

  ObSchemaManager schema_mgr;
  if (NULL != clp.schema_file)
  {
    tbsys::CConfig config;
    if (!schema_mgr.parse_from_file(clp.schema_file, config))
    {
      TBSYS_LOG(WARN, "parse schema fail");
      exit(-1);
    }
  }
  else if (OB_SUCCESS != fetch_schema(clp.schema_addr, clp.schema_port, schema_mgr))
  {
    TBSYS_LOG(WARN, "fetch schema fail");
    exit(-1);
  }
  schema_mgr.print_info();

  ClientWrapper client;
  client.init(clp.write_addr, static_cast<int32_t>(clp.write_port));

  MutatorBuilder mb;
  mb.init4write(schema_mgr, clp.prefix_start,
                clp.read_addr, static_cast<int32_t>(clp.read_port),
                clp.table_start_version, clp.using_id,
                clp.suffix_length, clp.max_suffix, clp.prefix_end, clp.max_row, &client);

  for (int64_t i = 0; i < clp.mutator_num; i++)
  {
    ObMutator mutator;
    PageArena<char> allocer;
    int ret = OB_SUCCESS;
    if (OB_SUCCESS != (ret = mb.build_mutator(mutator, allocer, clp.using_id, clp.max_cell)))
    {
      TBSYS_LOG(WARN, "build_mutator fail ret=%d\n", ret);
      break;
    }
    while (true)
    {
      int64_t timeu = tbsys::CTimeUtil::getTime();
      ret = client.apply(mutator);
      TBSYS_LOG(INFO, "apply ret=%d timeu=%ld\n", ret, tbsys::CTimeUtil::getTime() - timeu);
      if (OB_SUCCESS == ret)
      {
        break;
      }
    }
  }
  client.destroy();
}

