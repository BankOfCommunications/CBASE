/*
 *  (C) 2007-2010 Taobao Inc.
 *  
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License version 2 as
 *  published by the Free Software Foundation.
 *
 *         ????.cpp is for what ...
 *
 *  Version: $Id: ipvsadm.c,v 1.27 2005/12/10 16:00:07 wensong Exp $
 *
 *  Authors:
 *     Author Name <email address>
 *        - some work details if you want
 */

#include <ctype.h>
#include <tbsys.h>
#include "common/ob_define.h"
#include "common/ob_malloc.h"
#include "common/serialization.h"
#include "common/ob_schema.h"
#include "common/ob_scanner.h"
#include "chunkserver/ob_tablet_image.h"
#include "chunkserver/ob_tablet_manager.h"
#include "sstable/ob_sstable_reader.h"
#include "sstable/ob_sstable_trailer.h"

using namespace oceanbase::common;
using namespace oceanbase::sstable;
using namespace oceanbase::chunkserver;
using namespace oceanbase::common::serialization;

ObTabletManager tablet_mgr;

int local_cs_scan( const ObScanParam& scan_param, ObScanner& scanner) 
{
  int ret = tablet_mgr.scan(scan_param, scanner);
  if (ret != OB_SUCCESS) 
  {
    fprintf(stderr, "scan failed=%d\n", ret);
    return ret;
  }

  return ret;
}

int scan_test_case(
    const int64_t user_id,
    const int8_t item_type,
    const char** columns,
    const int32_t column_size
    )
{
  ObScanParam input;

  const int32_t key_size = 17;
  char start_key[key_size] ;
  char end_key[key_size] ;

  int64_t pos = 0;
  encode_i64(start_key, key_size, pos, user_id);
  if (item_type <= 1)
    encode_i8(start_key, key_size, pos, item_type);
  else
    encode_i8(start_key, key_size, pos, 0x0);
  memset(start_key + pos, 0x0, key_size - pos);

  pos = 0;
  encode_i64(end_key, key_size, pos, user_id);
  if ( item_type <= 1)
    encode_i8(end_key, key_size, pos, item_type);
  else
    encode_i8(end_key, key_size, pos, 0xFF);

  memset(end_key + pos, 0xFF, key_size - pos);

  hex_dump(start_key, key_size);
  hex_dump(end_key, key_size);

  //const char* query_column1 = "user_nick";
  const char* table_name = "collect_info";

  ObString ob_table_name(0, strlen(table_name), (char*)table_name);

  ObRange range;
  range.table_id_= 0;
  range.start_key_.assign_ptr(start_key, key_size);
  range.end_key_.assign_ptr(end_key, key_size);

  range.border_flag_.set_inclusive_start();
  range.border_flag_.set_inclusive_end();

  input.set(0, ob_table_name, range);
  input.set_scan_size(100);

  for (int i = 0; i < column_size; ++i)
  {
    const char* column_name = columns[i];
    ObString ob_query_col(0, strlen(column_name), (char*)column_name);
    input.add_column(ob_query_col);
  }


  ObScanner scanner;
  int64_t start = tbsys::CTimeUtil::getTime();
  int ret = local_cs_scan(input, scanner);
  int64_t end = tbsys::CTimeUtil::getTime();
  fprintf(stderr,"rpc_cs_scan time consume:%ld\n", end - start);
  if (OB_SUCCESS != ret)
  {
    fprintf(stderr,"ret:%d\n", ret);
  }
  else
  {
    ObScannerIterator iter;
    int i = 0;
    for (iter = scanner.begin(); iter != scanner.end(); iter++)
    {
      ObCellInfo cell_info;
      iter.get_cell(cell_info);
      fprintf(stderr,"---------------------%d----------------------\n", i);
      fprintf(stderr,"table_name:%.*s\n", cell_info.table_name_.length(), cell_info.table_name_.ptr());
      hex_dump(cell_info.row_key_.ptr(), cell_info.row_key_.length());
      fprintf(stderr,"column_name:%.*s\n", cell_info.column_name_.length(), cell_info.column_name_.ptr());
      fprintf(stderr,"---------------------%d----------------------\n", i);
      ++i;
    }
  }
  return ret;
}

int init_mgr()
{
  int err = OB_SUCCESS;

  ObBlockCacheConf bc_conf;
  bc_conf.block_cache_memsize_mb = 128;
  bc_conf.ficache_max_num = 1024;

  ObBlockIndexCacheConf bic_conf;
  bic_conf.cache_mem_size = 128 * 1024 * 1024;

  err = tablet_mgr.init(bc_conf, bic_conf);

  return err;
}

int main (int argc, char* argv[])
{
  int i = 0;
  int64_t user_id = 0;
  int8_t item_type = 0;
  const char* schema_file = NULL;
  while ((i = getopt(argc, argv, "s:u:t:")) != EOF) 
  {
    switch (i) 
    {
      case 'u':
        user_id = strtoll(optarg, NULL, 10);
        break;
      case 's':
        schema_file = optarg;
        break;
      case 't':
        item_type = atoi(optarg);
        break;
      case 'h':
        fprintf(stderr, "Usage: %s \n", argv[0]);
        return OB_ERROR;
    }
  }

  int ret = OB_SUCCESS;
  ret = init_mgr();
  if (ret != OB_SUCCESS) 
  {
    fprintf(stderr, "init failed=%d\n", ret);
    return ret;
  }
  int32_t disk_no_array[] = { 1 };
  int32_t size = 1;
  ret = tablet_mgr.load_tablets(disk_no_array, size);
  if (ret != OB_SUCCESS) 
  {
    fprintf(stderr, "load tablets failed=%d\n", ret);
    return ret;
  }

  ObSchemaManager schema_mgr;
  if (!schema_mgr.parse_from_file(schema_file, TBSYS_CONFIG)) 
  {
    fprintf(stderr, "load schema failed=%d\n", ret);
    return ret;
  }

  /*
  ret = tablet_mgr.set_serving_schema(schema_mgr);
  if (ret != OB_SUCCESS) 
  {
    fprintf(stderr, "set_serving_schema failed=%d\n", ret);
    return ret;
  }
  */
  const char* columns[] = { "user_nick" };
  scan_test_case(user_id, item_type, columns, 1);
}
