/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *
 *         search_sstable.cpp is for what ...
 *
 *  Version: $Id: search_sstable.cpp 01/30/2013 04:05:56 PM qushan Exp $
 *
 *  Authors:
 *     qushan < qushan@taobao.com >
 *        - some work details if you want
 */

#include <getopt.h>
#include <dirent.h>
#include <ctype.h>
#include <tbsys.h>
#include <dirent.h>
#include <string.h>
#include "search_sstable.h"
#include "common/ob_define.h"
#include "common/ob_range.h"
#include "common/ob_object.h"
#include "common/utility.h"
#include "sql/ob_sstable_scanner.h"
#include "common/ob_schema_manager.h"

using namespace oceanbase;
using namespace common;



const char *g_sstable_directory = NULL;
ObSchemaManagerV2 g_schema;
ObMergerSchemaManager g_sm;



// --------------------------------------------------------------
// CacheFactory
// --------------------------------------------------------------
CacheFactory CacheFactory::instance_;

void CacheFactory::init()
{
  int64_t block_cache_size = 1024 * 1024 * 2;
  int64_t ficache_max_num = 1024;
  int64_t block_index_cache_size = 1024*1024*2;

  fic_.init(ficache_max_num);
  int ret = block_cache_.init(block_cache_size);
  if (ret) return;
  ret = block_index_cache_.init(block_index_cache_size);
  if (ret) return;
  ret = compact_block_cache_.init(block_cache_size);
  if (ret) return;
  ret = compact_block_index_cache_.init(block_index_cache_size);
}

void CacheFactory::destroy()
{
  block_cache_.destroy();
  block_index_cache_.clear();
  compact_block_cache_.destroy();
  compact_block_index_cache_.destroy();
}

// --------------------------------------------------------------
// RowScanOp
// --------------------------------------------------------------
  RowScanOp::RowScanOp()
: reader1_(arena_, CacheFactory::get_instance().fic_),
  reader2_(CacheFactory::get_instance().fic_)
{
  memset(&scan_context_, 0, sizeof(scan_context_));
  scan_context_.sstable_reader_ = &reader1_;
  scan_context_.compact_context_.sstable_reader_ = &reader2_;
}

RowScanOp::~RowScanOp()
{
}

int RowScanOp::open(const int64_t sstable_file_id, const int64_t sstable_version, const ObSSTableScanParam & param)
{
  int ret = OB_SUCCESS;
  CacheFactory::get_instance().build_scan_context(scan_context_);

  if (sstable_version  < SSTableReader::COMPACT_SSTABLE_VERSION)
  {
    if (OB_SUCCESS != (reader1_.open(sstable_file_id, 0)))
    {
      TBSYS_LOG(ERROR, "cannot find sstable with scan range: %s, sstable version: %ld",
          to_cstring(param.get_range()), sstable_version);
    }
    else if (OB_SUCCESS != (ret = scanner_.set_scan_param(param, &scan_context_)))
    {
      TBSYS_LOG(ERROR, "set_scan_param to scanner error, range: %s, sstable version: %ld",
          to_cstring(param.get_range()), sstable_version);
    }
    else
    {
      iterator_ = &scanner_;
    }
  }
  else
  {
    if (OB_SUCCESS != (reader2_.open(sstable_file_id, 0)))
    {
      TBSYS_LOG(ERROR, "cannot find sstable with scan range: %s, sstable version: %ld",
          to_cstring(param.get_range()), sstable_version);
    }
    else if (OB_SUCCESS != (ret = compact_scanner_.set_scan_param(&param, &scan_context_.compact_context_)))
    {
      TBSYS_LOG(ERROR, "set_scan_param to scanner error, range: %s, sstable version: %ld",
          to_cstring(param.get_range()), sstable_version);
    }
    else
    {
      iterator_ = &compact_scanner_;
    }
  }

  return ret;
}

int RowScanOp::get_next_row(const ObRowkey* &row_key, const ObRow *&row_value)
{
  int ret = OB_SUCCESS;
  if (NULL == iterator_)
  {
    TBSYS_LOG(ERROR, "internal error, iterator_ not set, call init_sstable_scanner");
    ret = OB_NOT_INIT;
  }
  else 
  {
    ret = iterator_->get_next_row(row_key, row_value);
  }

  return ret;
}

// --------------------------------------------------------------
// CellScanOp
// --------------------------------------------------------------
  CellScanOp::CellScanOp()
: reader1_(arena_, CacheFactory::get_instance().fic_), row_iter_(false)
{
}

CellScanOp::~CellScanOp()
{
}

int CellScanOp::build_row_desc(const ObScanParam& param, const ObSSTableReader& reader)
{
  int ret = OB_SUCCESS;
  row_desc_.reset();
  uint64_t table_id = param.get_table_id();
  const uint64_t * const cid_array = param.get_column_id();
  const ObSSTableSchema* schema = reader.get_schema();
  ObRowkeyInfo rowkey_info_ ;
  row_desc_.add_column_desc(OB_INVALID_ID, OB_ACTION_FLAG_COLUMN_ID);

  if (param.get_column_id_size() == 1 && cid_array[0] == 0)
  {
    // whole row, query whole column group;
    int64_t column_size = 0;
    const ObSSTableSchemaColumnDef* def_array = NULL; 

    // add rowkey columns in first column group;
    if (schema->is_binary_rowkey_format(table_id))
    {
      ret = get_global_schema_rowkey_info(table_id, rowkey_info_);
      for (int64_t i = 0; i < rowkey_info_.get_size() && OB_SUCCESS == ret; ++i)
      {
        ret = row_desc_.add_column_desc(table_id, rowkey_info_.get_column(i)->column_id_);
      }
    }
    else if ( NULL == ( def_array = schema->get_group_schema(table_id, 
            ObSSTableSchemaColumnDef::ROWKEY_COLUMN_GROUP_ID, column_size)))
    {
      fprintf(stderr, "rowkey column group not exist:table:%ld", table_id);
    }
    else
    {
      for (int64_t i = 0; i < column_size && OB_SUCCESS == ret; ++i)
      {
        ret = row_desc_.add_column_desc(table_id, def_array[i].column_name_id_);
      }
    }

    def_array = schema->get_group_schema(table_id, 0, column_size);
    if (NULL == def_array)
    {
      ret = OB_ERROR;
      fprintf(stderr, "find column group def array error table=%ld", table_id);
    }
    else
    {
      // add every column id in this column group.
      for (int64_t i = 0; i < column_size && OB_SUCCESS == ret; ++i)
      {
        /**
         * if one column belongs to several column group, only the first 
         * column group will handle it. except there is only one column 
         * group. 
         */
        ret = row_desc_.add_column_desc(table_id, def_array[i].column_name_id_);
      }
    }
  }
  else
  {
    for (int64_t i = 0; OB_SUCCESS == ret && i < param.get_column_id_size(); ++i)
    {
      ret = row_desc_.add_column_desc(table_id, cid_array[i]);
    }
  }
  return ret;
}

int CellScanOp::open(const int64_t sstable_file_id, const int64_t sstable_version, const ObScanParam& param)
{
  int ret = OB_SUCCESS;

  if (OB_SUCCESS != (reader1_.open(sstable_file_id, 0)))
  {
    TBSYS_LOG(ERROR, "cannot find sstable with scan range: %s, sstable version: %ld",
        to_cstring(*param.get_range()), sstable_version);
  }
  else if (OB_SUCCESS != (ret = build_row_desc(param, reader1_)))
  {
    TBSYS_LOG(ERROR, "build_row_desc error, range: %s, sstable version: %ld",
        to_cstring(*param.get_range()), sstable_version);
  }
  else if (OB_SUCCESS != (ret = scanner_.set_scan_param(param, &reader1_, 
          CacheFactory::get_instance().block_cache_,
          CacheFactory::get_instance().block_index_cache_,
          false)))
  {
    TBSYS_LOG(ERROR, "set_scan_param to scanner error, range: %s, sstable version: %ld",
        to_cstring(*param.get_range()), sstable_version);
  }
  else
  {
    row_iter_.set_cell_iter(&scanner_, row_desc_, true);
  }

  return ret;
}

int CellScanOp::get_next_row(const ObRowkey* &row_key, const ObRow *&row_value)
{
  int ret = OB_SUCCESS;
  ret = row_iter_.get_next_row(row_key, row_value);

  return ret;
}

int scan_sstable_output_cell(const CmdLineParam &clp)
{
  int ret = OB_SUCCESS;
  ObScanParam param;
  CellScanOp scan_op;
  ObCellInfo* cell_info = NULL;
  bool is_row_changed = false;

  int64_t row_count = 0;
  int64_t start_time = tbsys::CTimeUtil::getTime();
  if (OB_SUCCESS != (ret = build_scan_param(clp, param)))
  {
    fprintf(stderr, "build param range=%s, query columns=%s, err=%d\n", 
        clp.scan_range, clp.query_columns, ret);
  }
  else if (OB_SUCCESS != (ret = scan_op.open(clp.sstable_file_id, clp.sstable_version, param)))
  {
    fprintf(stderr, "open scan op range=%s, query columns=%s, err=%d\n", 
        clp.scan_range, clp.query_columns, ret);
  }
  else
  {
    while (OB_SUCCESS == (ret = scan_op.next_cell()))
    {
      ret = scan_op.get_cell(&cell_info, &is_row_changed);
      if (OB_SUCCESS != ret) break;
      if (clp.output) 
      {
        if (is_row_changed)
        {
          ++row_count;
          fprintf(stderr, "get row:%s\n", to_cstring(cell_info->row_key_));
        }
        fprintf(stderr, "\tcell:%s\n", print_cellinfo(cell_info));
      }
    }
    int64_t end_time = tbsys::CTimeUtil::getTime();
    fprintf(stderr, "scan row count = %ld consume time =%ld\n", row_count, end_time - start_time);
  }
  return 0;
}

// --------------------------------------------------------------
// utility functions;
// --------------------------------------------------------------
void usage()
{
  printf("\n");
  printf("Usage: search_sstable [OPTION]\n");
  printf("   -a| --cmd_type command type [row_scan|row_get|cell_scan|cell_get] \n");
  printf("   -p| --sstable_path search sstable file path \n");
  printf("   -s| --compatible_schema compatible schema file for old binary format sstable.\n");
  printf("   -v| --sstable_version sstable version [1|2|3]\n");
  printf("   -t| --table_id scan or get table id \n");
  printf("   -r| --scan_range scan range in sstable\n");
  printf("   -g| --get_rowkey get rowkey in sstable\n");
  printf("   -c| --query_columns (scan or get) columns, 0 means full row.\n");
  printf("   -y| --is_async_read use async read mode\n");
  printf("   -h| --help print this help info\n");
  printf("   -q| --quiet do not print DEBUG log \n");
  printf("   samples:\n");
  printf("   ./search_sstable -a scan -p sstable_file_path -t 1001 -r \"[int:2;int:10]\" -c 1001,1002\n");
  printf("\n");
  exit(0);
}

int parse_cmd(const char* cmd)
{
  int ret = -1;
  if(0 == strcmp("row_scan", cmd))
  {
    ret = ROW_SCAN;
  }
  else if(0 == strcmp("row_get", cmd))
  {
    ret = ROW_GET;
  }
  else if(0 == strcmp("cell_scan", cmd))
  {
    ret = CELL_SCAN;
  }
  else if(0 == strcmp("cell_get", cmd))
  {
    ret = CELL_GET;
  }
  else if(0 == strcmp("scan_cell", cmd))
  {
    ret = SCAN_CELL;
  }
  return ret;
}

void parse_cmd_line(int argc, char **argv, CmdLineParam &clp)
{
  int opt = 0;
  const char* opt_string = "p:s:a:v:t:r:g:c:neyqh";
  const char* file_path = NULL;

  struct option longopts[] =
  {
    {"sstable_file_path",1,NULL,'p'},
    {"compatible_schema",1,NULL,'s'},
    {"cmd_type",1,NULL,'a'},
    {"sstable_version",1,NULL,'v'},
    {"table_id",1,NULL,'t'},
    {"scan_range",1,NULL,'r'},
    {"get_rowkey",1,NULL,'g'},
    {"query_columns",1,NULL,'c'},
    {"disable_output",0,NULL,'n'},
    {"is_result_cached",0,NULL, 'e'},
    {"is_async_read",0,NULL, 'y'},
    {"quiet",0,NULL,'q'},
    {"help",0,NULL,'h'},
    {0,0,0,0}
  };

  memset(&clp,0,sizeof(clp));
  clp.output = true;
  //init clp
  while((opt = getopt_long(argc, argv, opt_string, longopts,NULL)) != -1)
  {
    switch (opt)
    {
      case 'p':
        file_path = optarg;
        break;
      case 's':
        clp.schema_file = optarg;
        break;
      case 'a':
        clp.cmd_type = parse_cmd(optarg);
        break;
      case 'v':
        clp.sstable_version = atoi(optarg);
        break;
      case 't':
        clp.table_id = atoi(optarg);
        break;
      case 'r':
        clp.scan_range = optarg;
        break;
      case 'g':
        clp.get_rowkey = optarg;
        break;
      case 'c':
        clp.query_columns = optarg;
        break;
      case 'n':
        clp.output = false;
        break;
      case 'e':
        clp.is_result_cached = true;
        break;
      case 'y':
        clp.is_async_read = true;
        break;
      case 'q':
        clp.quiet = 1;
        break;
      case 'h':
        usage();
        break;
      default:
        usage();
        exit(1);
    }
  }

  if (clp.cmd_type < 0)
  {
    fprintf(stderr, "cmd_type not valid.\n");
    usage();
    exit(-1);
  }


  // parse g_sstable_directory & file id from sstable_file_path
  char * fp = new char[OB_MAX_FILE_NAME_LENGTH];
  memset(fp, 0, OB_MAX_FILE_NAME_LENGTH);
  const char * directory = NULL;
  const char * name = NULL;
  if (NULL != file_path)
  {
    strcpy(fp, file_path);
    char * p = strrchr(fp, '/');
    if (NULL != p)
    {
      *p = 0;
      directory = fp;
      name = p + 1;
    }
  }

  if (NULL != name) clp.sstable_file_id = strtoll(name, NULL, 10);
  if (NULL != directory) g_sstable_directory = directory;

  if (NULL == g_sstable_directory || 0 >= clp.sstable_file_id)
  {
    fprintf(stderr, "sstable file path not set");
    usage();
    exit(-1);
  }

  if (NULL == clp.query_columns) clp.query_columns = "0";

}

int main(const int argc, char **argv)
{
  // char* sst_file = NULL;
  // ObSSTableReader reader;
  CmdLineParam clp;
  parse_cmd_line(argc,argv,clp);
  if (clp.quiet) TBSYS_LOGGER.setLogLevel("ERROR");

  ob_init_crc64_table(OB_DEFAULT_CRC64_POLYNOM);
  ob_init_memory_pool();
  CacheFactory::get_instance().init();
  tbsys::CConfig config;

  if (NULL != clp.schema_file)
  {
    g_schema.parse_from_file(clp.schema_file, config);
    g_sm.init(false, g_schema);
    set_global_sstable_schema_manager(&g_sm);
  }

  switch (clp.cmd_type)
  {
    case ROW_SCAN:
      scan_sstable_output_row<RowScanOp, ObSSTableScanParam>(clp);
      break;
    case CELL_SCAN:
      scan_sstable_output_row<CellScanOp, ObScanParam>(clp);
      break;
    case SCAN_CELL:
      scan_sstable_output_cell(clp);
      break;
    default:
      fprintf(stderr, "unsupport cmd:%ld\n", clp.cmd_type);
      break;
  }

  return 0;
}


