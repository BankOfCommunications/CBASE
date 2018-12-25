#include <iostream>
#include <cstdlib>
#include <getopt.h>
#include <cstdio>
#include <arpa/inet.h>
#include "common/ob_define.h"
#include "common/ob_schema.h"
#include "common/ob_malloc.h"
#include "sstable/ob_sstable_writer.h"
#include "../../common/test_rowkey_helper.h"
#include "olap.h"
using namespace std;
using namespace oceanbase;
using namespace oceanbase::sstable;
using namespace oceanbase::common;
struct gen_param
{
  uint32_t start_include_;
  uint32_t end_include_;
  const char *sst_name_;
  const char *schema_;
};

static CharArena allocator_;

void usage(char **argv)
{
  fprintf(stderr, "%s -s row_key_start[uint32_t] -e row_key_end[uint32_t] -p sst_path[char*] -m schema_path[char*]\n", argv[0]);
  exit(0);
}

void parse_cmd_line(int argc, char **argv, gen_param &param)
{
  int opt = 0;
  const char *opt_string = "s:e:p:m:h";
  struct option longopts[] =
  {
    {"help", 0, NULL, 'h'},
    {"row_key_start", 1, NULL, 's'},
    {"row_key_end", 1, NULL, 'e'},
    {"sst_path", 1, NULL, 'p'},
    {"schema_path", 1, NULL, 'm'}
  };
  memset(&param, 0x00, sizeof(param));
  if (argc <= 1)
  {
    usage(argv);
  }
  while ((opt = getopt_long(argc,argv, opt_string,longopts,NULL)) != -1)
  {
    switch (opt)
    {
    case 'h':
      usage(argv);
      break;
    case 's':
      param.start_include_ = static_cast<uint32_t>(strtoul(optarg,NULL,10));
      break;
    case 'e':
      param.end_include_ = static_cast<uint32_t>(strtoul(optarg,NULL,10));
      break;
    case 'p':
      param.sst_name_ = optarg;
      break;
    case 'm':
      param.schema_ = optarg;
      break;
    default:
      usage(argv);
      break;
    }
  }
  if ((NULL == param.sst_name_) || (NULL == param.schema_))
  {
    usage(argv);
  }
}

int fill_sstable_schema(ObSchemaManagerV2 &mgr, const uint64_t table_id, ObSSTableSchema& sstable_schema)
{
  int ret = OB_SUCCESS;
  int32_t cols = 0;
  int32_t size = 0;
  ObSSTableSchemaColumnDef column_def;

  sstable_schema.reset();

  const ObColumnSchemaV2 *col = mgr.get_table_schema( table_id,size);

  if (NULL == col || size <= 0)
  {
    TBSYS_LOG(ERROR,"cann't find this table:%lu",table_id);
    ret = OB_ERROR;
  }
  else
  {
    for (int col_index = 0; col_index < size && OB_SUCCESS == ret; ++col_index)
    {
      memset(&column_def,0,sizeof(column_def));
      column_def.table_id_ = static_cast<uint32_t>(table_id);
      column_def.column_group_id_ = static_cast<uint16_t>((col + col_index)->get_column_group_id());
      column_def.column_name_id_ = static_cast<uint16_t>((col + col_index)->get_id());
      column_def.column_value_type_ = (col + col_index)->get_type();
      if ( (ret = sstable_schema.add_column_def(column_def)) != OB_SUCCESS )
      {
        TBSYS_LOG(ERROR,"add column_def(%u,%u,%u) failed col_index : %d",column_def.table_id_,
          column_def.column_group_id_,column_def.column_name_id_,col_index);
      }
      ++cols;
    }
  }

  if ( 0 == cols && OB_SUCCESS == ret ) //this table has moved to updateserver
  {
    ret = OB_CS_TABLE_HAS_DELETED;
  }
  return ret;
}

int gen_sst(const gen_param &param)
{
  ObSSTableSchema *sst_schema = new(std::nothrow)ObSSTableSchema;
  ObSSTableWriter sst_writer;
  ObSSTableRow    sst_row;
  tbsys::CConfig config;
  ObSchemaManagerV2 *mgr = new(std::nothrow) ObSchemaManagerV2;
  int err = OB_SUCCESS;
  if ((NULL == mgr) || (NULL == sst_schema))
  {
    err = OB_ALLOCATE_MEMORY_FAILED;
    TBSYS_LOG(WARN,"fail to allocate memory for ObSchemaManagerV2");
  }
  if ((OB_SUCCESS == err) && (!mgr->parse_from_file(param.schema_,config)))
  {
    TBSYS_LOG(WARN,"fail to load schema from file [err:%d,schema.ini:%s]", err, param.schema_);
    err = OB_INVALID_ARGUMENT;
  }
  if ((OB_SUCCESS == err) && (OB_SUCCESS != (err = fill_sstable_schema(*mgr,msolap::target_table_id,*sst_schema))))
  {
    TBSYS_LOG(WARN,"fail to fill sst schema [err:%d, table_id:%lu]", err, msolap::target_table_id);
  }
  ObTrailerParam trailer_param;
  trailer_param.block_size_ = msolap::ups_sst_block_size;
  trailer_param.compressor_name_.assign((char*)msolap::ups_sst_compressor_name, static_cast<int32_t>(strlen(msolap::ups_sst_compressor_name)));
  trailer_param.frozen_time_ = tbsys::CTimeUtil::getTime();
  trailer_param.store_type_ = msolap::ups_sst_store_type;
  trailer_param.table_version_ = msolap::ups_sst_version;
  ObString sst_path(static_cast<int32_t>(strlen(param.sst_name_)), static_cast<int32_t>(strlen(param.sst_name_)),(char*)param.sst_name_);
  if ((OB_SUCCESS == err) && (OB_SUCCESS != sst_writer.create_sstable(*sst_schema,sst_path, trailer_param)))
  {
    TBSYS_LOG(WARN,"fail to create sst [err:%d,sst_path:%s]", err, param.sst_name_);
  }
  uint32_t big_endian_row_key = 0;
  int64_t space_usage = 0;
  ObString row_key_str(sizeof(big_endian_row_key), sizeof(big_endian_row_key), (char*)&big_endian_row_key);
  if((OB_SUCCESS == err) && (OB_SUCCESS != (err = sst_row.set_obj_count(msolap::max_column_id - msolap::min_column_id + 1))))
  {
    TBSYS_LOG(WARN,"fail to call sst_row.set_obj_count [err:%d]", err);
  }
  for (uint32_t row_key = param.start_include_; (OB_SUCCESS == err) && (row_key <= param.end_include_); row_key++)
  {
    big_endian_row_key = htonl(row_key);
    sst_row.clear();
    if ((OB_SUCCESS == err) &&(OB_SUCCESS != (err =  sst_row.set_rowkey(TestRowkeyHelper(row_key_str, &allocator_)))))
    {
      TBSYS_LOG(WARN,"fail to set sst row rowkey [err:%d]", err);
    }
    if((OB_SUCCESS == err) && (OB_SUCCESS !=(err = sst_row.set_table_id(msolap::target_table_id))))
    {
      TBSYS_LOG(WARN,"fail to set sst row table id [err:%d]", err);
    }
    if((OB_SUCCESS == err) && (OB_SUCCESS !=(err = sst_row.set_column_group_id(msolap::ups_sst_column_group_id))))
    {
      TBSYS_LOG(WARN,"fail to set sst row group id [err:%d]", err);
    }
    for (uint64_t column_id = msolap::min_column_id; (OB_SUCCESS == err)&&(column_id <= msolap::max_column_id); column_id ++)
    {
      ObObj val;
      val.set_int(msolap::olap_get_column_val(column_id,big_endian_row_key));
      if (OB_SUCCESS != (err = sst_row.add_obj(val)))
      {
        TBSYS_LOG(WARN,"fail to add obj to sst row [err:%d]", err);
      }
    }
    assert(OB_SUCCESS == sst_row.check_schema(*sst_schema));
    if((OB_SUCCESS == err) && (OB_SUCCESS != (err = sst_writer.append_row(sst_row,space_usage))))
    {
      TBSYS_LOG(WARN,"fail to append row to sst writer [err:%d]", err);
    }
  }
  int64_t trailer_offset = 0;
  int64_t sst_size = 0;
  if((OB_SUCCESS == err) && (OB_SUCCESS != (err = sst_writer.close_sstable(trailer_offset, sst_size))))
  {
    TBSYS_LOG(WARN,"fail to close sst [err:%d]", err);
  }
  else
  {
    TBSYS_LOG(INFO, "sst write success [trailer_offset:%ld,sst_size:%ld]", trailer_offset, sst_size);
  }
  delete sst_schema;
  delete mgr;
  return err;
}

int main(int argc, char **argv)
{
  oceanbase::common::ob_init_memory_pool();
  gen_param param;
  parse_cmd_line(argc,argv, param);
  return gen_sst(param);
}
