/*
 *  (C) 2007-2010 Taobao Inc.
 *  
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License version 2 as
 *  published by the Free Software Foundation.
 *
 *         scan_sstable.cpp is for what ...
 *
 *  Version: $Id: scan_sstable.cpp 2010年12月24日 14时27分34秒 qushan Exp $
 *
 *  Authors:
 *     qushan < qushan@taobao.com >
 *        - some work details if you want
 */



#include "profiler.h"
#include "common/ob_define.h"
#include "common/ob_malloc.h"
#include "common/ob_object.h"
#include "common/utility.h"
#include "common/ob_record_header.h"
#include "chunkserver/ob_blockcache.h"
#include "chunkserver/ob_block_index_cache.h"
#include "chunkserver/ob_sstable_writer.h"
#include "chunkserver/ob_sstable_reader.h"
#include "chunkserver/ob_sstable_block_reader.h"
#include "chunkserver/ob_sstable_block_index.h"
#include "chunkserver/ob_sstable_scanner.h"

using namespace oceanbase;
using namespace common;
using namespace chunkserver;
using namespace tbsys;

class GFactory
{
  public:
    static const int64_t THREAD_BUFFER_SIZE = 2*1024*1024;
  public:
    GFactory() :
      compressed_buffer_(THREAD_BUFFER_SIZE), 
      uncompressed_buffer_(THREAD_BUFFER_SIZE) { init(); }
    ~GFactory() { destroy(); }
    static GFactory& get_instance() { return instance_; }
    ObBlockCache& get_block_cache() { return block_cache_; }
    ObBlockIndexCache& get_block_index_cache() { return block_index_cache_; }
    const ThreadSpecificBuffer::Buffer& get_compressed_buffer() { return *compressed_buffer_.get_buffer();}
    const ThreadSpecificBuffer::Buffer& get_uncompressed_buffer() { return *uncompressed_buffer_.get_buffer();}
  private:
    void init();
    void destroy();
    static GFactory instance_;
    ObBlockCache block_cache_;
    ObBlockIndexCache block_index_cache_;
    ThreadSpecificBuffer compressed_buffer_;
    ThreadSpecificBuffer uncompressed_buffer_;
};

GFactory GFactory::instance_;

void GFactory::init()
{
  ob_init_memory_pool(64 * 1024 * 1024);
  int ret = block_cache_.init(10);
  //ASSERT_EQ(0, ret);
  ret = block_index_cache_.init(1024*1024*2, -1, 1024);
  //ASSERT_EQ(0, ret);
}

void GFactory::destroy()
{
  block_cache_.destroy();
  block_index_cache_.clear();
}

int main (int argc, char* argv[])
{
  const char* file_name = 0;
  ObSSTableId sstable_id;
  int i = 0;
  int slient = 0;
  while ((i = getopt(argc, argv, "i:q")) != EOF) 
  {
    switch (i) 
    {
      case 'i':
        //file_name = optarg;
        sstable_id.sstable_file_id_ = strtoll(optarg, NULL, 10);
        sstable_id.sstable_file_offset_ = 0;
        break;
      case 'q':
        slient = 1;
        break;
      case 'h':
        fprintf(stderr, "Usage: %s "
            "-f file_path\n", argv[0]);
        return OB_ERROR;
    }
  }

  if (slient) TBSYS_LOGGER.setLogLevel("INFO");

  ob_init_memory_pool();


  int ret = OB_SUCCESS;
  ObSSTableReader reader;
  ret = reader.open(sstable_id);
  if (ret)
  {
    fprintf(stderr, "open src file %s failure ret = %d\n", file_name, ret);
    return ret;
  }

  int64_t table_id = reader.get_trailer().get_table_id(0);

  ObSSTableScanner scanner (
      GFactory::get_instance().get_block_index_cache(),
      GFactory::get_instance().get_block_cache(),
      GFactory::get_instance().get_compressed_buffer(),
      GFactory::get_instance().get_uncompressed_buffer()
      );

  ObBorderFlag border_flag;
  border_flag.set_inclusive_start();
  border_flag.set_inclusive_end();
  ObRange range;
  range.start_key_ = reader.get_start_key(table_id);
  range.end_key_ = reader.get_end_key(table_id);
  range.border_flag_ = border_flag;

  ObScanParam scan_param;
  ObString table_name;
  scan_param.set(table_id, table_name, range);
  scan_param.add_column(0);
  scan_param.set_is_result_cached(0);

  //PROFILER_START("scan_profile_start");
  ret = scanner.set_scan_param(scan_param, &reader);
  if (OB_SUCCESS != ret)
  {
    fprintf(stderr, "set_scan_param failed , ret =%d\n", ret);
    return ret;
  }

  int64_t row_index = 0;
  int64_t col_index = 0;
  //int64_t column_count = SSTableBuilder::COL_NUM;
  int64_t total = 0;
  ObCellInfo* cur_cell = NULL;
  bool is_row_changed = false;
  bool first_row = true;
  int64_t start = tbsys::CTimeUtil::getTime();
  int64_t last = tbsys::CTimeUtil::getTime();
  while (OB_SUCCESS == scanner.next_cell())
  {
    ret = scanner.get_cell(&cur_cell, &is_row_changed);
    //EXPECT_EQ(0, err);
    //EXPECT_NE((ObCellInfo*) NULL, cur_cell); 
    if (is_row_changed)
    {
      // if (col_index > 0) EXPECT_EQ(column_count, col_index+1);
      // reset column
      col_index = 0;
      if (!first_row) ++row_index;
      if (first_row) first_row = false;
    }
    else
    {
      ++col_index;
    }
    //check_cell(cell_infos[row_index + row_start_index][col_index], *cur_cell);
    ++total;
    if (row_index % 100000 == 0) 
    {
      int64_t now = tbsys::CTimeUtil::getTime();
      fprintf(stderr, "scan 100000 rows, consume=%ld\n", now - last);
      last = now;
    }
  }
  //PROFILER_DUMP();
  //PROFILER_STOP();

  int64_t end = tbsys::CTimeUtil::getTime();
  fprintf(stderr, "scan %ld rows, consume=%ld\n", row_index, end - start);
}
