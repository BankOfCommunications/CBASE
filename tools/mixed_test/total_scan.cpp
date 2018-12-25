#include <getopt.h>
#include "common/ob_malloc.h"
#include "mutator_builder.h"
#include "test_utils.h"
#include "utils.h"
#include "row_checker.h"

using namespace oceanbase;
using namespace common;

struct CmdLine
{
  static const int64_t DEFAULT_TABLE_START_VERSION = 2;
  static const int64_t DEFAULT_PREFIX_START = 1;
  static const int64_t DEFAULT_TEST_TIMES = 10240;
  static const char *DEFAULT_LOG_LEVEL;

  char *serv_addr;
  int64_t serv_port;
  char *schema_addr;
  int64_t schema_port;
  bool using_id;
  int64_t table_start_version;
  char *schema_file;
  int64_t prefix_start;
  int64_t test_times;
  bool check;
  const char *log_level;

  CmdLine()
  {
    serv_addr = NULL;
    serv_port = -1;
    schema_addr = NULL;
    schema_port = -1;
    using_id = false;
    table_start_version = DEFAULT_TABLE_START_VERSION;
    schema_file = NULL;
    prefix_start = DEFAULT_PREFIX_START;
    test_times = DEFAULT_TEST_TIMES;
    check = false;
    log_level = DEFAULT_LOG_LEVEL;
  };

  void log_all()
  {
    TBSYS_LOG(INFO, "serv_addr=%s serv_port=%ld schema_addr=%s schema_port=%ld using_id=%s table_start_version=%ld "
              "schema_file=%s prefix_start=%ld test_times=%ld check=%s log_level=%s",
              serv_addr, serv_port, schema_addr, schema_port, STR_BOOL(using_id), table_start_version,
              schema_file, prefix_start, test_times, STR_BOOL(check), log_level);
  };

  bool is_valid()
  {
    bool bret = false;
    if (NULL != serv_addr
        && 0 < serv_port
        && ((NULL != schema_addr
              && 0 < schema_port)
            || NULL != schema_file))
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
  fprintf(stderr, "total_scan [OPTION]\n");
  fprintf(stderr, "   -a|--serv_addr updateserver/cluster address, end with '@' to direct access server, otherwise using API\n");
  fprintf(stderr, "   -p|--serv_port updateserver/cluster port\n");
  fprintf(stderr, "   -r|--schema_addr schema server address\n");
  fprintf(stderr, "   -o|--schema_port schema server port\n");
  fprintf(stderr, "   -f|--schema_file if schema_file set, will not fetch from schema server\n");
  fprintf(stderr, "   -i|--using_id using table_id and column_id to scan [if not set, default using name]\n");
  fprintf(stderr, "   -w|--log_level [default %s]\n\n", CmdLine::DEFAULT_LOG_LEVEL);

  fprintf(stderr, "   -t|--table_start_version to read from updateserver [default %ld]\n", CmdLine::DEFAULT_TABLE_START_VERSION);
  fprintf(stderr, "   -s|--prefix_start rowkey prefix start number, must be same as multi_write [default %ld]\n", CmdLine::DEFAULT_PREFIX_START);
  fprintf(stderr, "   -k|--check check the read result or not [default not]\n");

  fprintf(stderr, "   -m|--test_times random get test times [default %ld], support MAX\n", CmdLine::DEFAULT_TEST_TIMES);

  fprintf(stderr, "\n");
}

void parse_cmd_line(int argc, char **argv, CmdLine &clp)
{
  int opt = 0;
  const char* opt_string = "a:p:r:o:it:f:s:m:w:kh";
  struct option longopts[] =
  {
    {"serv_addr", 1, NULL, 'a'},
    {"serv_port", 1, NULL, 'p'},
    {"schema_addr", 1, NULL, 'r'},
    {"schema_port", 1, NULL, 'o'},
    {"using_id", 0, NULL, 'i'},
    {"table_start_version", 1, NULL, 't'},
    {"schema_file", 1, NULL, 'f'},
    {"prefix_start", 1, NULL, 's'},
    {"test_times", 1, NULL, 'm'},
    {"log_level", 1, NULL, 'w'},
    {"check", 1, NULL, 'k'},
    {"help", 0, NULL, 'h'},
    {0, 0, 0, 0}
  };
  while ((opt = getopt_long(argc, argv, opt_string, longopts, NULL)) != -1)
  {
    switch (opt)
    {
    case 'a':
      clp.serv_addr = optarg;
      break;
    case 'p':
      clp.serv_port = atoi(optarg);
      break;
    case 'r':
      clp.schema_addr = optarg;
      break;
    case 'o':
      clp.schema_port = atoi(optarg);
      break;
    case 'i':
      clp.using_id = true;
      break;
    case 't':
      clp.table_start_version = atol(optarg);
      break;
    case 'f':
      clp.schema_file = optarg;
      break;
    case 's':
      clp.prefix_start = atoll(optarg);
      break;
    case 'm':
      if (0 == strcmp("MAX", optarg))
      {
        clp.test_times = INT64_MAX;
      }
      else
      {
        clp.test_times = atoll(optarg);
      }
      break;
    case 'w':
      clp.log_level = optarg;
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

void scan_check_all(MutatorBuilder &mb, ClientWrapper &client, const int64_t table_start_version, const bool using_id, const bool check)
{
  for (int64_t i = 0; i < mb.get_schema_num(); ++i)
  {
    ObScanParam scan_param;
    PageArena<char> allocer;
    mb.build_total_scan_param(scan_param, allocer, table_start_version, using_id, i);
    int ret = OB_SUCCESS;
    bool is_fullfilled = false;
    int64_t fullfilled_item_num = 0;
    int64_t row_counter = 0;
    ObScanner scanner;
    while (!is_fullfilled
          && OB_SUCCESS == ret)
    {
      RowChecker rc;
      int64_t timeu = tbsys::CTimeUtil::getTime();
      scan_param.set_limit_info(0, 2048);
      ret = client.scan(scan_param, scanner);
      timeu = tbsys::CTimeUtil::getTime() - timeu;
      int64_t cur_prefix_row_counter = 0;
      int64_t cur_row_counter = 0;
      if (OB_SUCCESS == ret)
      {
        while (OB_SUCCESS == scanner.next_cell())
        {
          ObCellInfo *ci = NULL;
          bool is_row_changed = false;
          if (OB_SUCCESS == scanner.get_cell(&ci, &is_row_changed))
          {
            if (is_row_changed)
            {
              if (rc.is_prefix_changed(ci->row_key_))
              {
                cur_prefix_row_counter = 0;
              }
              cur_prefix_row_counter++;
              cur_row_counter++;
            }
            if (!check)
            {
              if (is_row_changed)
              {
                rc.add_rowkey(ci->row_key_);
              }
              continue;
            }
            if (!using_id)
            {
              trans_name2id(*ci, mb.get_schema(i));
            }
            if (is_row_changed && 0 != rc.cell_num())
            {
              std::string row_key_str(to_obstr(TestRowkeyHelper(rc.get_cur_rowkey())).ptr(), 0, to_obstr(TestRowkeyHelper(rc.get_cur_rowkey())).length());

              bool get_row_bret = get_check_row(mb.get_schema(i), rc.get_cur_rowkey(), mb.get_cellinfo_builder(i), client, table_start_version, using_id);

              bool cell_info_bret = rc.check_row(mb.get_cellinfo_builder(i), mb.get_schema(i));
              if (!get_row_bret
                  || !cell_info_bret)
              {
                TBSYS_LOG(ERROR, "[%s] [get_row check_ret=%s] [cell_info check_ret=%s]",
                          row_key_str.c_str(), STR_BOOL(get_row_bret), STR_BOOL(cell_info_bret));
              }
            }
            if (is_row_changed
                && 0 != rc.rowkey_num()
                && rc.is_prefix_changed(ci->row_key_))
            {
              std::string row_key_str(to_obstr(TestRowkeyHelper(rc.get_last_rowkey())).ptr(), 0, to_obstr(TestRowkeyHelper(rc.get_last_rowkey())).length());

              bool bret = rc.check_rowkey(mb.get_rowkey_builder(i));
              if (!bret)
              {
                TBSYS_LOG(ERROR, "[%s] [row_key check_ret=%s]", row_key_str.c_str(), STR_BOOL(bret));
              }
            }
            rc.add_cell(ci);
            if (is_row_changed)
            {
              rc.add_rowkey(ci->row_key_);
            }
          }
        }
        if (0 != rc.cell_num())
        {
          std::string row_key_str(to_obstr(TestRowkeyHelper(rc.get_cur_rowkey())).ptr(), 0, to_obstr(TestRowkeyHelper(rc.get_cur_rowkey())).length());

          bool get_row_bret = get_check_row(mb.get_schema(i), rc.get_cur_rowkey(), mb.get_cellinfo_builder(i), client, table_start_version, using_id);

          bool cell_info_bret = rc.check_row(mb.get_cellinfo_builder(i), mb.get_schema(i));
          if (!get_row_bret
              || !cell_info_bret)
          {
            TBSYS_LOG(ERROR, "[%s] [get_row check_ret=%s] [cell_info check_ret=%s]",
                     row_key_str.c_str(), STR_BOOL(get_row_bret), STR_BOOL(cell_info_bret));
          }
        }
      }
      TBSYS_LOG(INFO, "total_scan ret=%d timeu=%ld row_counter=%ld last_prefix_row_counter=%ld",
                ret, timeu, cur_row_counter, cur_prefix_row_counter);
      if (OB_SUCCESS != ret)
      {
        ret = OB_SUCCESS;
        continue;
      }
      scanner.get_is_req_fullfilled(is_fullfilled, fullfilled_item_num);
      ObNewRange *range = const_cast<ObNewRange*>(scan_param.get_range());
      scanner.get_last_row_key(range->start_key_);
      ObString start_key_str;
      range->start_key_.get_obj_ptr()[0].get_varchar(start_key_str);
      memset(start_key_str.ptr() + RowkeyBuilder::I64_STR_LENGTH, 0, mb.get_rowkey_builder(i).get_suffix_length());
      if (!is_fullfilled)
      {
        cur_row_counter -= cur_prefix_row_counter;
      }
      row_counter += cur_row_counter;
    }
    TBSYS_LOG(INFO, "table_id=%lu total_row_counter=%ld", mb.get_schema(i).get_table_id(), row_counter);
  }
}

int main(int argc, char **argv)
{
  CmdLine clp;
  parse_cmd_line(argc, argv, clp);

  char buffer[1024];
  sprintf(buffer, "total_scan.%ld.log", clp.prefix_start);
  TBSYS_LOGGER.rotateLog(buffer);
  TBSYS_LOGGER.setFileName(buffer);
  TBSYS_LOGGER.setLogLevel(clp.log_level);
  clp.log_all();

  ob_init_memory_pool();
  TBSYS_LOG(INFO, "getpid()=%d", getpid());

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
  client.init(clp.serv_addr, static_cast<int32_t>(clp.serv_port));
  for (int64_t i = 0; i < clp.test_times; i++)
  {
    MutatorBuilder mb;
    mb.init4read(schema_mgr, clp.prefix_start, clp.serv_addr, static_cast<int32_t>(clp.serv_port), clp.table_start_version, clp.using_id);
    scan_check_all(mb, client, clp.table_start_version, clp.using_id, clp.check);
  }
  client.destroy();
}

