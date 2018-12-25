/**
 * (C) 2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * test_helper.h
 *
 * Authors:
 *   chuanhui <rizhao.ych@taobao.com>
 *   qushan<qushan@taobao.com>
 *   huating <huating.zmq@taobao.com>
 *
 */
#ifndef __OCEANBASE_CHUNKSERVER_TEST_HELPER_H__
#define __OCEANBASE_CHUNKSERVER_TEST_HELPER_H__

#include <tblog.h>
#include <gtest/gtest.h>
#include "common/ob_define.h"
#include "common/ob_iterator.h"
#include "common/thread_buffer.h"
#include "sstable/ob_blockcache.h"
#include "sstable/ob_block_index_cache.h"
#include "sstable/ob_sstable_reader.h"
#include "sstable/ob_sstable_writer.h"
#include "chunkserver/ob_tablet_image.h"
#include "chunkserver/ob_tablet_manager.h"

using namespace std;
using namespace oceanbase::common;
using namespace oceanbase::chunkserver;
using namespace oceanbase::sstable;


void check_string(const ObString& expected, const ObString& real);

void check_obj(const ObObj& expected, const ObObj& real);

void check_cell(const ObCellInfo& expected, const ObCellInfo& real);

void check_cell_with_name(const ObCellInfo& expected, const ObCellInfo& real);

int init_sstable(ObSSTableReader& sstable, const ObCellInfo** cell_infos,
    const int64_t row_num, const int64_t col_num, const int64_t sst_id = 0,
    const char* sstable_file_path = NULL);
int prepare_sstable_directroy(const int64_t disk_num);

class GFactory
{
  public:
    static const int64_t THREAD_BUFFER_SIZE = 2*1024*1024;
  public:
    GFactory() :
      block_cache_(fic_), block_index_cache_(fic_),
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
    FileInfoCache fic_;
    ObBlockCache block_cache_;
    ObBlockIndexCache block_index_cache_;
    ThreadSpecificBuffer compressed_buffer_;
    ThreadSpecificBuffer uncompressed_buffer_;
};

int write_cg_sstable(const ObCellInfo** cell_infos,
    const int64_t row_num, const int64_t col_num, const char* sstable_file_path );
int write_sstable(const ObCellInfo** cell_infos,
    const int64_t row_num, const int64_t col_num, const char* sstable_file_path );
int write_empty_sstable(const ObCellInfo** cell_infos,
    const int64_t row_num, const int64_t col_num, const char* sstable_file_path );

class SSTableBuilder
{
  public:
    SSTableBuilder();
    ~SSTableBuilder();
    typedef int (WRITE_SSTABLE_FUNC)(const ObCellInfo** cell_infos,
        const int64_t row_num, const int64_t col_num, const char* sstable_file_path );
    int generate_sstable_file(WRITE_SSTABLE_FUNC write_func, const ObSSTableId& sstable_id,
                              const int64_t start_index = 0);
    ObCellInfo** const get_cell_infos() const { return cell_infos; }
  public:
    static const int64_t table_id = 100;
    static const int64_t ROW_NUM = 1000;
    static const int64_t COL_NUM = 5;

  private:
    ObCellInfo** cell_infos;
    char* row_key_strs[ROW_NUM][COL_NUM];
};

class MultSSTableBuilder
{
  public:
    MultSSTableBuilder()
    {

    }

    ~MultSSTableBuilder()
    {

    }

    int generate_sstable_files(bool empty_sstable = false);

    int get_sstable_id(ObSSTableId & sstable_id, const int64_t index) const
    {
      int ret = OB_SUCCESS;

      if (index < 0 || index >= SSTABLE_NUM)
      {
        ret = OB_ERROR;
      }
      else
      {
        sstable_id.sstable_file_id_ = index % DISK_NUM
                                      + 256 * (index / DISK_NUM + 1) + 1;
        sstable_id.sstable_file_offset_ = 0;
      }

      return ret;
    }

    ObCellInfo** const get_cell_infos(const int64_t index) const
    {
      return builder_[index].get_cell_infos();
    }

  public:
    static const int64_t DISK_NUM = 12;
    static const int64_t SSTABLE_NUM = DISK_NUM * 2;

  private:
    SSTableBuilder builder_[SSTABLE_NUM];
};

class TabletManagerIniter
{
  public:
    TabletManagerIniter(ObTabletManager& tablet_mgr)
    : inited_(false), tablet_mgr_(tablet_mgr)
    {

    }

    ~TabletManagerIniter()
    {

    }

    int init(bool create = false, bool empty_tablet = false);

    const MultSSTableBuilder& get_mult_sstable_builder() const
    {
      return builder_;
    }

  private:
    int create_tablet(const ObNewRange& range, const ObSSTableId& sst_id, bool serving, bool add_sst_id = true, const int64_t version = 1);
    int create_tablets(bool empty_tablet = false);

  private:
    bool inited_;
    ObTabletManager& tablet_mgr_;
    MultSSTableBuilder builder_;
};

#endif //__TEST_HELPER_H__

