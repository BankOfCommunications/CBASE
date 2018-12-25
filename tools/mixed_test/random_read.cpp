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
  static const int64_t DEFAULT_ROW_NUM = DEFAULT_ROW_NUM_PER_MGET;
  static const int64_t DEFAULT_CELL_NUM = 10;
  static const char *DEFAULT_LOG_LEVEL;

  char *serv_addr;
  int64_t serv_port;
  char *schema_addr;
  int64_t schema_port;
  bool using_id;
  bool get2read;
  int64_t table_start_version;
  char *schema_file;
  int64_t prefix_start;
  int64_t test_times;
  int64_t row_num;
  int64_t cell_num;
  bool check;
  const char *log_level;
  const char *rowkey2check;

  CmdLine()
  {
    serv_addr = NULL;
    serv_port = -1;
    schema_addr = NULL;
    schema_port = -1;
    using_id = false;
    get2read = false;
    table_start_version = DEFAULT_TABLE_START_VERSION;
    schema_file = NULL;
    prefix_start = DEFAULT_PREFIX_START;
    test_times = DEFAULT_TEST_TIMES;
    row_num = DEFAULT_ROW_NUM;
    cell_num = DEFAULT_CELL_NUM;
    check = false;
    log_level = DEFAULT_LOG_LEVEL;
    rowkey2check = NULL;;
  };

  void log_all()
  {
    TBSYS_LOG(INFO, "serv_addr=%s serv_port=%ld schema_addr=%s schema_port=%ld using_id=%s get2read=%s table_start_version=%ld "
              "schema_file=%s prefix_start=%ld "
              "test_times=%ld row_num=%ld cell_num=%ld check=%s log_level=%s",
              serv_addr, serv_port, schema_addr, schema_port, STR_BOOL(using_id), STR_BOOL(get2read), table_start_version,
              schema_file, prefix_start, 
              test_times, row_num, cell_num, STR_BOOL(check), log_level);
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
  fprintf(stderr, "random_read [OPTION]\n");
  fprintf(stderr, "   -a|--serv_addr updateserver/cluster address, end with '@' to direct access server, otherwise using API\n");
  fprintf(stderr, "   -p|--serv_port updateserver/cluster port\n");
  fprintf(stderr, "   -r|--schema_addr schema server address\n");
  fprintf(stderr, "   -o|--schema_port schema server port\n");
  fprintf(stderr, "   -f|--schema_file if schema_file set, will not fetch from schema server\n");
  fprintf(stderr, "   -i|--using_id using table_id and column_id to scan [if not set, default using name]\n");
  fprintf(stderr, "   -d|--get2read using get protocol to read [if not set, default using scan protocol to read]\n");
  fprintf(stderr, "   -w|--log_level [default %s]\n\n", CmdLine::DEFAULT_LOG_LEVEL);

  fprintf(stderr, "   -t|--table_start_version to read from updateserver [default %ld]\n", CmdLine::DEFAULT_TABLE_START_VERSION);
  fprintf(stderr, "   -s|--prefix_start rowkey prefix start number, must be same as mutil_write [default %ld]\n", CmdLine::DEFAULT_PREFIX_START);
  fprintf(stderr, "   -k|--check check the read result or not [default not]\n\n");

  fprintf(stderr, "   -m|--test_times random get test times [default %ld], support MAX\n", CmdLine::DEFAULT_TEST_TIMES);
  fprintf(stderr, "   -u|--row_num row number of each random get test [default %ld]\n", CmdLine::DEFAULT_ROW_NUM);
  fprintf(stderr, "   -n|--cell_num max cell number to get of each row [default %ld]\n", CmdLine::DEFAULT_CELL_NUM);

  fprintf(stderr, "   -R|--rowkey2check rowkey to run check [default NULL]\n");

  fprintf(stderr, "\n");
}

void parse_cmd_line(int argc, char **argv, CmdLine &clp)
{
  int opt = 0;
  const char* opt_string = "a:p:r:o:idt:f:s:m:u:n:R:w:kh";
  struct option longopts[] =
  {
    {"serv_addr", 1, NULL, 'a'},
    {"serv_port", 1, NULL, 'p'},
    {"schema_addr", 1, NULL, 'r'},
    {"schema_port", 1, NULL, 'o'},
    {"using_id", 0, NULL, 'i'},
    {"get2read", 0, NULL, 'd'},
    {"table_start_version", 1, NULL, 't'},
    {"schema_file", 1, NULL, 'f'},
    {"prefix_start", 1, NULL, 's'},
    {"test_times", 1, NULL, 'm'},
    {"row_num", 1, NULL, 'u'},
    {"cell_num", 1, NULL, 'n'},
    {"rowkey2check", 1, NULL, 'R'},
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
    case 'd':
      clp.get2read = true;
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
    case 'u':
      clp.row_num = atoll(optarg);
      break;
    case 'n':
      clp.cell_num = atoll(optarg);
      break;
    case 'R':
      clp.rowkey2check = optarg;
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

int random_scan_check(MutatorBuilder &mb, ClientWrapper &client, const int64_t table_start_version, const bool using_id, const bool check)
{
  int ret = OB_SUCCESS;
  int64_t i = 0;
  int64_t prefix = 0;
  ObScanParam scan_param;
  PageArena<char> allocer;
  ret = mb.build_scan_param(scan_param, allocer, table_start_version, using_id, i, prefix);
  bool is_fullfilled = false;
  int64_t fullfilled_item_num = 0;
  RowChecker rc;
  int64_t row_counter = 0;
  ObScanner scanner;
  while (!is_fullfilled
        && OB_SUCCESS == ret)
  {
    int64_t timeu = tbsys::CTimeUtil::getTime();
    ret = client.scan(scan_param, scanner);
    timeu = tbsys::CTimeUtil::getTime() - timeu;
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
            cur_row_counter++;
            row_counter++;
          }
          if (!check)
          {
            continue;
          }
          if (!using_id)
          {
            trans_name2id(*ci, mb.get_schema(i));
          }
          if (is_row_changed && 0 != rc.cell_num())
          {
            std::string row_key_str(to_obstr(TestRowkeyHelper(rc.get_cur_rowkey())).ptr(), 0, to_obstr(TestRowkeyHelper(rc.get_cur_rowkey())).length());

            bool get_row_bret = true;
            get_row_bret = get_check_row(mb.get_schema(i), rc.get_cur_rowkey(), mb.get_cellinfo_builder(i), client, table_start_version, using_id);

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

            bool bret = rc.check_rowkey(mb.get_rowkey_builder(i), &prefix);
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
      if (check
          && 0 != rc.cell_num())
      {
        std::string row_key_str(to_obstr(TestRowkeyHelper(rc.get_cur_rowkey())).ptr(), 0, to_obstr(TestRowkeyHelper(rc.get_cur_rowkey())).length());

        bool get_row_bret = true;
        get_row_bret = get_check_row(mb.get_schema(i), rc.get_cur_rowkey(), mb.get_cellinfo_builder(i), client, table_start_version, using_id);

        bool cell_info_bret = rc.check_row(mb.get_cellinfo_builder(i), mb.get_schema(i));
        if (!get_row_bret
            || !cell_info_bret)
        {
          TBSYS_LOG(ERROR, "[%s] [get_row check_ret=%s] [cell_info check_ret=%s]",
                   row_key_str.c_str(), STR_BOOL(get_row_bret), STR_BOOL(cell_info_bret));
        }
      }
    }
    TBSYS_LOG(INFO, "random_scan ret=%d timeu=%ld row_counter=%ld", ret, timeu, cur_row_counter);
    scanner.get_is_req_fullfilled(is_fullfilled, fullfilled_item_num);
    ObNewRange *range = const_cast<ObNewRange*>(scan_param.get_range());
    scanner.get_last_row_key(range->start_key_);
    range->border_flag_.unset_inclusive_start();
  }
  TBSYS_LOG(DEBUG, "table_id=%lu row_counter=%ld", mb.get_schema(i).get_table_id(), row_counter);
  return ret;
}

int random_get_check(MutatorBuilder &mb, ClientWrapper &client, const int64_t table_start_version, const bool using_id,
                      const int64_t row_num, const int64_t cell_num_per_row, const bool check)
{
  int ret = OB_SUCCESS;
  ObScanner scanner;
  ObGetParam get_param;
  PageArena<char> allocer;
  mb.build_get_param(get_param, allocer, table_start_version, using_id, row_num, cell_num_per_row);
  int64_t timeu = tbsys::CTimeUtil::getTime();
  ret = client.get(get_param, scanner);
  timeu= tbsys::CTimeUtil::getTime() - timeu;
  int64_t row_counter = 0;
  if (OB_SUCCESS == ret)
  {
    RowChecker rc;
    int64_t schema_pos = -1;
    RowExistChecker rec;
    //ObCellInfo cc;
    while (OB_SUCCESS == scanner.next_cell())
    {
      ObCellInfo *ci = NULL;
      bool is_row_changed = false;
      if (OB_SUCCESS == scanner.get_cell(&ci, &is_row_changed))
      {
        if (is_row_changed)
        {
          row_counter++;
        }
        if (!check)
        {
          continue;
        }
        if (is_row_changed
            && ObActionFlag::OP_ROW_DOES_NOT_EXIST == ci->value_.get_ext())
        {
          if (false && rec.check_row_not_exist(client, ci, using_id, table_start_version, timeu))
          {
            TBSYS_LOG(ERROR, "[%s] maybe un-exist but not, timeu=%ld", to_cstring(ci->row_key_), timeu);
          }
          continue;
        }
        int64_t tmp_schema_pos = -1;
        if (!using_id)
        {
          tmp_schema_pos = mb.get_schema_pos(ci->table_name_);
          trans_name2id(*ci, mb.get_schema(tmp_schema_pos));
        }
        else
        {
          tmp_schema_pos = mb.get_schema_pos(ci->table_id_);
        }
        if (is_row_changed && 0 != rc.cell_num())
        {
          std::string row_key_str(to_obstr(TestRowkeyHelper(rc.get_cur_rowkey())).ptr(), 0, to_obstr(TestRowkeyHelper(rc.get_cur_rowkey())).length());

          bool get_row_bret = true;
          get_row_bret = get_check_row(mb.get_schema(schema_pos), rc.get_cur_rowkey(), mb.get_cellinfo_builder(schema_pos), client, table_start_version, using_id);

          bool cell_info_bret = rc.check_row(mb.get_cellinfo_builder(schema_pos), mb.get_schema(schema_pos));
          if (!get_row_bret
              || !cell_info_bret)
          {
            TBSYS_LOG(ERROR, "[%s] [get_row check_ret=%s] [cell_info check_ret=%s]",
                     row_key_str.c_str(), STR_BOOL(get_row_bret), STR_BOOL(cell_info_bret));
          }
        }
        if (ObActionFlag::OP_NOP != ci->value_.get_ext())
        {
          schema_pos = tmp_schema_pos;
          rc.add_cell(ci);
          //cc = *ci;
        }
      }
    }
    if (check
        && 0 != rc.cell_num())
    {
      std::string row_key_str(to_obstr(TestRowkeyHelper(rc.get_cur_rowkey())).ptr(), 0, to_obstr(TestRowkeyHelper(rc.get_cur_rowkey())).length());

      bool get_row_bret = true;
      get_row_bret = get_check_row(mb.get_schema(schema_pos), rc.get_cur_rowkey(), mb.get_cellinfo_builder(schema_pos), client, table_start_version, using_id);

      bool cell_info_bret = rc.check_row(mb.get_cellinfo_builder(schema_pos), mb.get_schema(schema_pos));
      if (!get_row_bret
          || !cell_info_bret)
      {
        TBSYS_LOG(ERROR, "[%s] [get_row check_ret=%s] [cell_info check_ret=%s]",
                 row_key_str.c_str(), STR_BOOL(get_row_bret), STR_BOOL(cell_info_bret));
      }
    }
  }
  TBSYS_LOG(INFO, "random_mget ret=%d timeu=%ld row_counter=%ld", ret, timeu, row_counter);

  bool is_fullfilled = false;
  int64_t fullfilled_item_num = 0;
  scanner.get_is_req_fullfilled(is_fullfilled, fullfilled_item_num);
  TBSYS_LOG(DEBUG, "row_counter=%ld get_param_row_size=%ld fullfilled_item_num=%ld get_param_cell_size=%ld "
            "is_fullfilled=%s row_is_equal=%s cell_is_equal=%s",
            row_counter, get_param.get_row_size(), fullfilled_item_num, get_param.get_cell_size(),
            STR_BOOL(is_fullfilled),
            STR_BOOL(!is_fullfilled || row_counter == get_param.get_row_size()),
            STR_BOOL(!is_fullfilled || fullfilled_item_num == get_param.get_cell_size()));
  return ret;
}

int main(int argc, char **argv)
{
  CmdLine clp;
  parse_cmd_line(argc, argv, clp);

  if (clp.get2read)
  {
    char buffer[1024];
    sprintf(buffer, "random_read.mget.%ld.log", clp.prefix_start);
    TBSYS_LOGGER.rotateLog(buffer);
    TBSYS_LOGGER.setFileName(buffer);
  }
  else
  {
    char buffer[1024];
    sprintf(buffer, "random_read.scan.%ld.log", clp.prefix_start);
    TBSYS_LOGGER.rotateLog(buffer);
    TBSYS_LOGGER.setFileName(buffer);
  }

  TBSYS_LOGGER.setLogLevel(getenv("log_level")? getenv("log_level"): clp.log_level);
  clp.log_all();

  ob_init_memory_pool();
  TBSYS_LOG(INFO, "cmdline: %s", my_str_join(argc, argv));
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

  MutatorBuilder mb;
  mb.init4read(schema_mgr, clp.prefix_start, clp.serv_addr, static_cast<int32_t>(clp.serv_port), clp.table_start_version, clp.using_id);

  ClientWrapper client;
  client.init(clp.serv_addr, static_cast<int32_t>(clp.serv_port));
  if (NULL != clp.rowkey2check)
  {
    CharArena allocer;
    get_check_row(mb.get_schema(0), make_rowkey(clp.rowkey2check, &allocer), mb.get_cellinfo_builder(0), client, clp.table_start_version, clp.using_id);
  }
  else
  {
    for (int64_t i = 0; i < clp.test_times; i++)
    {
      if (clp.get2read)
      {
        random_get_check(mb, client, clp.table_start_version, clp.using_id, clp.row_num, clp.cell_num, clp.check);
      }
      else
      {
        random_scan_check(mb, client, clp.table_start_version, clp.using_id, clp.check);
      }
    }
  }
  client.destroy();
}

