/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *  
 * test_aio_buffer_mgr.cpp for test aio buffer manager 
 *
 * Authors: 
 *   huating <huating.zmq@taobao.com>
 *
 */
#include <tblog.h>
#include <gtest/gtest.h>
#include "common/ob_malloc.h"
#include "common/ob_fileinfo_manager.h"
#include "chunkserver/ob_fileinfo_cache.h"
#include "sstable/ob_aio_buffer_mgr.h"
#include "sstable/ob_blockcache.h"
#include "sstable/ob_block_index_cache.h"
#include "sstable/ob_disk_path.h"

using namespace std;
using namespace oceanbase::common;
using namespace oceanbase::chunkserver;
using namespace oceanbase::sstable;

namespace oceanbase
{
  namespace tests
  {
    namespace sstable 
    {
      static char file_name[256];
      static const int64_t file_size = 1024 * 1024 * 5;
      static const uint64_t sstable_id = 0;
      static const int64_t timeout = 10 * 1000 * 1000;
      static FileInfoCache fic; 
      static ObBlockCache bc(fic);
      static ObAIOBufferMgr aio_buf_mgr;

      class TestObAIOBufferMgr : public ::testing::Test
      {
      public:
        static void SetUpTestCase()
        {
          ObSSTableId ob_sstable_id;
          ob_sstable_id.sstable_file_id_ = sstable_id;
          get_sstable_path(ob_sstable_id, file_name, 256);

          remove(file_name);
          char cmd[512];
          sprintf(cmd, "mkdir -p %s; rm -rf %s; dd if=/dev/zero of=%s bs=1M count=%ld", 
                  file_name, file_name, file_name, file_size / (1024 * 1024));
          system(cmd);
          
          int64_t block_cache_size = 64 * 1024 * 1024;
          int64_t ficache_max_num = 1024;
        
          fic.init(ficache_max_num);
          bc.init(block_cache_size);
        }
      
        static void TearDownTestCase()
        {
          fic.destroy();
          bc.destroy();
        }
      
        virtual void SetUp()
        {
  
        }
      
        virtual void TearDown()
        {
      
        }

      public:
        int test_get_block_eof(const int64_t block_size, const int64_t block_count,
                               const int64_t start_offset, const int64_t file_size,
                               const bool reverse_scan = false, const bool copy2cache = false)
        {
          int ret = OB_SUCCESS;
          ObBlockPositionInfos pos_infos;
          char* buffer = NULL;
          bool from_cache = false;
  
          pos_infos.block_count_ = block_count;
          for (int64_t i = 0; i < block_count; ++i)
          {
            pos_infos.position_info_[i].offset_ = start_offset + i * block_size;
            pos_infos.position_info_[i].size_ = block_size;
          }
  
          ret = aio_buf_mgr.advise(bc, sstable_id, pos_infos, copy2cache, reverse_scan);
          EXPECT_EQ(OB_SUCCESS, ret);
  
          if (!reverse_scan)
          {
            for (int64_t i = 0; i < block_count; ++i)
            {
              ret = aio_buf_mgr.get_block(bc, sstable_id, pos_infos.position_info_[i].offset_, 
                                          block_size, timeout, buffer, from_cache, false);
              if (pos_infos.position_info_[i].offset_ + pos_infos.position_info_[i].size_ 
                  > file_size)
              {
                EXPECT_TRUE(OB_AIO_EOF == ret);
                if (OB_AIO_EOF == ret)
                {
                  break;
                }
              }
              else
              {
                EXPECT_EQ(OB_SUCCESS, ret);
                EXPECT_TRUE(NULL != buffer);
              }
            }    
          }
          else
          {
            for (int64_t i = block_count - 1; i >= 0; --i)
            {
              ret = aio_buf_mgr.get_block(bc, sstable_id, pos_infos.position_info_[i].offset_, 
                                          block_size, timeout, buffer, from_cache, false);
              if (pos_infos.position_info_[block_count - 1].offset_ 
                  + pos_infos.position_info_[block_count - 1].size_ > file_size)
              {
                EXPECT_TRUE(OB_AIO_EOF == ret);
                if (OB_AIO_EOF == ret)
                {
                  break;
                }
              }
              else
              {
                EXPECT_EQ(OB_SUCCESS, ret);
                EXPECT_TRUE(NULL != buffer);
              }
            }  
          }
          
          return ret;      
        }

        int test_get_block(const int64_t block_size, const int64_t block_count,
                           const int64_t step = 0, const bool continuous = true,
                           const bool reverse_scan = false, const bool copy2cache = false)
        {
          int ret = OB_SUCCESS;
          ObBlockPositionInfos pos_infos;
          char* buffer = NULL;
          bool from_cache = false;
  
          pos_infos.block_count_ = block_count;
          for (int64_t i = 0; i < block_count; ++i)
          {
            if (0 == i)
            {
              pos_infos.position_info_[i].offset_ = i * block_size;
              pos_infos.position_info_[i].size_ = block_size + i * step;              
            }
            else
            {
              pos_infos.position_info_[i].offset_ = pos_infos.position_info_[i - 1].offset_ 
                + pos_infos.position_info_[i - 1].size_;
              pos_infos.position_info_[i].size_ = block_size + i * step;
            }
          }
  
          ret = aio_buf_mgr.advise(bc, sstable_id, pos_infos, copy2cache, reverse_scan);
          EXPECT_EQ(OB_SUCCESS, ret);
  
          if (!reverse_scan)
          {
            for (int64_t i = 0; i < block_count; ++i)
            {
              if (continuous)
              {
                ret = aio_buf_mgr.get_block(bc, sstable_id, pos_infos.position_info_[i].offset_, 
                                            pos_infos.position_info_[i].size_, timeout, buffer, 
                                            from_cache, false);
                EXPECT_EQ(OB_SUCCESS, ret);
                EXPECT_TRUE(NULL != buffer);
              }
              else
              {
                ret = aio_buf_mgr.get_block(bc, sstable_id, pos_infos.position_info_[i].offset_ + 1, 
                                            pos_infos.position_info_[i].size_, timeout, buffer, 
                                            from_cache, false);
                EXPECT_TRUE(OB_SUCCESS != ret);
              }
            }    
          }
          else
          {
            for (int64_t i = block_count - 1; i >= 0; --i)
            {
              if (continuous)
              {
                ret = aio_buf_mgr.get_block(bc, sstable_id, pos_infos.position_info_[i].offset_, 
                                            pos_infos.position_info_[i].size_, timeout, buffer,
                                            from_cache, false);
                EXPECT_EQ(OB_SUCCESS, ret);
                EXPECT_TRUE(NULL != buffer);
              }
              else
              {
                ret = aio_buf_mgr.get_block(bc, sstable_id, pos_infos.position_info_[i].offset_ + 1, 
                                            pos_infos.position_info_[i].size_, timeout, buffer,
                                            from_cache, false);
                EXPECT_TRUE(OB_SUCCESS != ret);
              }
            }
          }
          
          return ret;      
        }

        void test_get_one_block(const bool copy2cache = false, 
                                const bool reverse_scan = false)
        {
          bc.clear();
          test_get_block(1, 1, 0, true, reverse_scan, copy2cache);
          test_get_block(512, 1, 0, true, reverse_scan, copy2cache);
          test_get_block(1023, 1, 0, true, reverse_scan, copy2cache);
          test_get_block(1024, 1, 0, true, reverse_scan, copy2cache);
          test_get_block(1024 * 128, 1, 0, true, reverse_scan, copy2cache);
          test_get_block(1024 * 512, 1, 0, true, reverse_scan, copy2cache);
          test_get_block(1024 * 1023, 1, 0, true, reverse_scan, copy2cache);
        }

        void test_get_ten_blocks(const bool copy2cache = false, const bool reverse_scan = false)
        {
          bc.clear();
          test_get_block(1, 10, 0, true, reverse_scan, copy2cache);
          test_get_block(512, 10, 0, true, reverse_scan, copy2cache);
          test_get_block(1023, 10, 0, true, reverse_scan, copy2cache);
          test_get_block(1024, 10, 0, true, reverse_scan, copy2cache);
          test_get_block(1024 * 128, 10, 0, true, reverse_scan, copy2cache);
          test_get_block(1024 * 512, 10, 0, true, reverse_scan, copy2cache);
        }

        void test_get_blocks_with_one_aio_buf(const bool copy2cache = false, 
                                              const bool reverse_scan = false)
        {
          bc.clear();
          test_get_block(1024 * 255, 2, 0, true, reverse_scan, copy2cache);
          test_get_block(1024 * 40, 10, 0, true, reverse_scan, copy2cache);
          test_get_block(1024 * 4, 100, 0, true, reverse_scan, copy2cache);
          test_get_block(1024, 200, 0, true, reverse_scan, copy2cache);
          test_get_block(500, 1000, 0, true, reverse_scan, copy2cache);
        }

        void test_get_blocks_with_two_aio_buf(const bool copy2cache = false, 
                                              const bool reverse_scan = false)
        {
          bc.clear();
          test_get_block(1024 * 512, 2, 0, true, reverse_scan, copy2cache);
          test_get_block(1024 * 1023, 2, 0, true, reverse_scan, copy2cache);
          test_get_block(1024 * 100, 10, 0, true, reverse_scan, copy2cache);
          test_get_block(1024 * 100, 20, 0, true, reverse_scan, copy2cache);
          test_get_block(1024 * 10, 100, 0, true, reverse_scan, copy2cache);
          test_get_block(1024 * 10, 200, 0, true, reverse_scan, copy2cache);
          test_get_block(1024, 1000, 0, true, reverse_scan, copy2cache);
          test_get_block(1024, 2000, 0, true, reverse_scan, copy2cache);
        }

        void test_get_blocks_with_preread(const bool copy2cache = false, 
                                          const bool reverse_scan = false)
        {
          bc.clear();
          test_get_block(1024 * 1023, 3, 0, true, reverse_scan, copy2cache);
          test_get_block(1024 * 1023, 5, 0, true, reverse_scan, copy2cache);
          test_get_block(1024 * 400, 10, 0, true, reverse_scan, copy2cache);
          test_get_block(1024 * 100, 50, 0, true, reverse_scan, copy2cache);
          test_get_block(1024 * 10, 500, 0, true, reverse_scan, copy2cache);
          test_get_block(1024, 5000, 0, true, reverse_scan, copy2cache);
        }

        void test_get_block_eof(const bool copy2cache = false, 
                                const bool reverse_scan = false)
        {
          bc.clear();
          test_get_block_eof(1024, 10, file_size - 1024 * 10, file_size, reverse_scan, copy2cache);
          test_get_block_eof(1024, 10, file_size - 1024, file_size, reverse_scan, copy2cache);
          test_get_block_eof(1024, 10, file_size, file_size, reverse_scan, copy2cache);
          test_get_block_eof(1024, 10, file_size + 1024 * 10, file_size, reverse_scan, copy2cache);
          test_get_block_eof(1024 * 900, 10, 0, file_size, reverse_scan, copy2cache);
        }

        void test_get_block_with_diff_block_size(const bool copy2cache = false, 
                                                 const bool reverse_scan = false)
        {
          bc.clear();
          test_get_block(1024, 10, 10, true, reverse_scan, copy2cache);
          test_get_block(1024, 10, -10, true, reverse_scan, copy2cache);
          test_get_block(1024, 100, 10, true, reverse_scan, copy2cache);
          test_get_block(1024 * 100, 10, 1000, true, reverse_scan, copy2cache);
          test_get_block(1024 * 100, 10, -1000, true, reverse_scan, copy2cache);
          test_get_block(1024 * 100, 30, 100, true, reverse_scan, copy2cache);
          test_get_block(1024 * 100, 30, -100, true, reverse_scan, copy2cache);
        }

        void test_get_block_incontinuous(const bool copy2cache = false, 
                                         const bool reverse_scan = false)
        {
          bc.clear();
          test_get_block(1024, 10, 10, false, reverse_scan, copy2cache);
          test_get_block(1024, 10, -10, false, reverse_scan, copy2cache);
          test_get_block(1024, 100, 10, false, reverse_scan, copy2cache);
          test_get_block(1024 * 100, 10, 1000, false, reverse_scan, copy2cache);
          test_get_block(1024 * 100, 10, -1000, false, reverse_scan, copy2cache);
          test_get_block(1024 * 100, 30, 100, false, reverse_scan, copy2cache);
          test_get_block(1024 * 100, 30, -100, false, reverse_scan, copy2cache);
        }
      };

      TEST_F(TestObAIOBufferMgr, test_get_block_non_init)
      {
        int ret = OB_SUCCESS;
        char* buffer = NULL;
        bool from_cache =false;

        ret = aio_buf_mgr.get_block(bc, 0, 0, 1, timeout, buffer, from_cache);
        EXPECT_TRUE(OB_SUCCESS != ret);
      }

      TEST_F(TestObAIOBufferMgr, test_init)
      {
        int ret = OB_SUCCESS;

        ret = aio_buf_mgr.init();
        EXPECT_EQ(OB_SUCCESS, ret);
      }

      TEST_F(TestObAIOBufferMgr, test_get_block_invalid_param)
      {
        int ret = OB_SUCCESS;
        char* buffer = NULL;
        bool from_cache = false;

        ret = aio_buf_mgr.get_block(bc, 0, 0, 0, 0, buffer, from_cache);
        EXPECT_TRUE(OB_SUCCESS != ret);  
        ret = aio_buf_mgr.get_block(bc, 0, 0, 1024 * 1024, 0, buffer, from_cache);
        EXPECT_TRUE(OB_SUCCESS != ret); 
        ret = aio_buf_mgr.get_block(bc, OB_INVALID_ID, 0, 1, 1, buffer, from_cache);
        EXPECT_TRUE(OB_SUCCESS != ret);   
        ret = aio_buf_mgr.get_block(bc, OB_INVALID_ID, -1, 1, 1, buffer, from_cache);
        EXPECT_TRUE(OB_SUCCESS != ret);  
        ret = aio_buf_mgr.get_block(bc, OB_INVALID_ID, -1, -1, 1, buffer, from_cache);
        EXPECT_TRUE(OB_SUCCESS != ret);
        ret = aio_buf_mgr.get_block(bc, OB_INVALID_ID, -1, -1, -1, buffer, from_cache);
        EXPECT_TRUE(OB_SUCCESS != ret);
        ret = aio_buf_mgr.get_block(bc, 0, -1, 1, 1, buffer, from_cache);
        EXPECT_TRUE(OB_SUCCESS != ret); 
        ret = aio_buf_mgr.get_block(bc, 0, -1, -1, 1, buffer, from_cache);
        EXPECT_TRUE(OB_SUCCESS != ret); 
        ret = aio_buf_mgr.get_block(bc, 0, -1, -1, -1, buffer, from_cache);
        EXPECT_TRUE(OB_SUCCESS != ret); 
        ret = aio_buf_mgr.get_block(bc, 0, 0, -1, 1, buffer, from_cache);
        EXPECT_TRUE(OB_SUCCESS != ret); 
        ret = aio_buf_mgr.get_block(bc, 0, 0, -1, -1, buffer, from_cache);
        EXPECT_TRUE(OB_SUCCESS != ret); 
        ret = aio_buf_mgr.get_block(bc, OB_INVALID_ID, 0, -1, 0, buffer, from_cache);
        EXPECT_TRUE(OB_SUCCESS != ret); 
      }

      TEST_F(TestObAIOBufferMgr, test_get_block_non_advise)
      {
        int ret = OB_SUCCESS;
        char* buffer = NULL;
        bool from_cache = false;

        ret = aio_buf_mgr.get_block(bc, 0, 0, 1, 1, buffer, from_cache);
        EXPECT_TRUE(OB_SUCCESS != ret); 
        ret = aio_buf_mgr.get_block(bc, 10, 0, 1, 1, buffer, from_cache);
        EXPECT_TRUE(OB_SUCCESS != ret); 
        ret = aio_buf_mgr.get_block(bc, 10, 2, 1, 1, buffer, from_cache);
        EXPECT_TRUE(OB_SUCCESS != ret); 
        ret = aio_buf_mgr.get_block(bc, 10, 512, 1024, 1, buffer, from_cache);
        EXPECT_TRUE(OB_SUCCESS != ret); 
      }

      TEST_F(TestObAIOBufferMgr, test_advise_with_wrong_param)
      {
        int ret = OB_SUCCESS;
        ObBlockPositionInfos pos_infos;
        int64_t block_count = 1;
        int64_t block_size = 1024 * 1024;

        ret = aio_buf_mgr.advise(bc, OB_INVALID_ID, pos_infos, false);
        EXPECT_TRUE(OB_SUCCESS != ret);
        ret = aio_buf_mgr.advise(bc, sstable_id, pos_infos, false);
        EXPECT_TRUE(OB_SUCCESS != ret);

        //block size too big
        pos_infos.block_count_ = block_count;
        for (int64_t i = 0; i < block_count; ++i)
        {
          pos_infos.position_info_[i].offset_ = i * block_size;
          pos_infos.position_info_[i].size_ = block_size;
        }
        ret = aio_buf_mgr.advise(bc, sstable_id, pos_infos, false);
        EXPECT_TRUE(OB_SUCCESS != ret);

        //blocks aren't continuous
        block_count = 10;
        block_size = 1024;
        pos_infos.block_count_ = block_count;
        for (int64_t i = 0; i < block_count; ++i)
        {
          pos_infos.position_info_[i].offset_ = i * block_size;
          pos_infos.position_info_[i].size_ = block_size - 1;
        }
        ret = aio_buf_mgr.advise(bc, sstable_id, pos_infos, false);
        EXPECT_TRUE(OB_SUCCESS != ret);    
        
        //blocks are overlap
        block_count = 10;
        block_size = 1024;
        pos_infos.block_count_ = block_count;
        for (int64_t i = 0; i < block_count; ++i)
        {
          pos_infos.position_info_[i].offset_ = i * block_size;
          pos_infos.position_info_[i].size_ = block_size + 1;
        }
        ret = aio_buf_mgr.advise(bc, sstable_id, pos_infos, false);
        EXPECT_TRUE(OB_SUCCESS != ret); 
        
        //block offset or block size is less than 0 
        block_count = 10;
        block_size = -1;
        pos_infos.block_count_ = block_count;
        for (int64_t i = 0; i < block_count; ++i)
        {
          pos_infos.position_info_[i].offset_ = i * block_size;
          pos_infos.position_info_[i].size_ = block_size + 1;
        }
        ret = aio_buf_mgr.advise(bc, sstable_id, pos_infos, false);
        EXPECT_TRUE(OB_SUCCESS != ret); 
      }

      TEST_F(TestObAIOBufferMgr, test_advise_success)
      {
        int ret = OB_SUCCESS;
        ObBlockPositionInfos pos_infos;
        int64_t block_count = 10;
        int64_t block_size = 1024;

        pos_infos.block_count_ = block_count;
        for (int64_t i = 0; i < block_count; ++i)
        {
          pos_infos.position_info_[i].offset_ = i * block_size;
          pos_infos.position_info_[i].size_ = block_size;
        }
        ret = aio_buf_mgr.advise(bc, sstable_id, pos_infos, false);
        EXPECT_TRUE(OB_SUCCESS == ret); 

        //advise again
        ret = aio_buf_mgr.advise(bc, sstable_id, pos_infos, false, true);
        EXPECT_TRUE(OB_SUCCESS == ret); 

        //thounsands of blocks
        block_count = 4000;
        block_size = 1024;
        pos_infos.block_count_ = block_count;
        for (int64_t i = 0; i < block_count; ++i)
        {
          pos_infos.position_info_[i].offset_ = i * block_size;
          pos_infos.position_info_[i].size_ = block_size;
        }
        ret = aio_buf_mgr.advise(bc, sstable_id, pos_infos, false);
        EXPECT_TRUE(OB_SUCCESS == ret);

        ret = aio_buf_mgr.advise(bc, sstable_id, pos_infos, true, true);
        EXPECT_TRUE(OB_SUCCESS == ret);
      }

      TEST_F(TestObAIOBufferMgr, test_get_one_block)
      {
        test_get_one_block(false, false);
        test_get_one_block(false, true);
        test_get_one_block(true, false);
        test_get_one_block(true, true);
      }

      TEST_F(TestObAIOBufferMgr, test_get_ten_blocks)
      {
        test_get_ten_blocks(false, false);
        test_get_ten_blocks(false, true);
        test_get_ten_blocks(true, false);
        test_get_ten_blocks(true, true);
      }

      TEST_F(TestObAIOBufferMgr, test_get_blocks_with_one_aio_buf)
      {
        //total size <= 512K
        test_get_blocks_with_one_aio_buf(false, false);
        test_get_blocks_with_one_aio_buf(false, true);
        test_get_blocks_with_one_aio_buf(true, false);
        test_get_blocks_with_one_aio_buf(true, true);
      }

      TEST_F(TestObAIOBufferMgr, test_get_blocks_with_two_aio_buf)
      {
        //512K < total_size <= 2M
        test_get_blocks_with_two_aio_buf(false, false);
        test_get_blocks_with_two_aio_buf(false, true);
        test_get_blocks_with_two_aio_buf(true, false);
        test_get_blocks_with_two_aio_buf(true, true);
      }

      TEST_F(TestObAIOBufferMgr, test_get_blocks_with_preread)
      {
        //total_size > 2M
        test_get_blocks_with_preread(false, false);
        test_get_blocks_with_preread(false, true);
        test_get_blocks_with_preread(true, false);
        test_get_blocks_with_preread(true, true);
      }

      TEST_F(TestObAIOBufferMgr, test_get_block_eof)
      {
        test_get_block_eof(false, false);
        test_get_block_eof(false, true);
        test_get_block_eof(true, false);
        test_get_block_eof(true, true);
      }

      TEST_F(TestObAIOBufferMgr, test_get_block_with_diff_block_size)
      {
        test_get_block_with_diff_block_size(false, false);
        test_get_block_with_diff_block_size(false, true);
        test_get_block_with_diff_block_size(true, false);
        test_get_block_with_diff_block_size(true, true);
      }

      TEST_F(TestObAIOBufferMgr, test_get_block_incontinuous)
      {
        test_get_block_incontinuous(false, false);
        test_get_block_incontinuous(false, true);
        test_get_block_incontinuous(true, false);
        test_get_block_incontinuous(true, true);
      }

    }//end namespace chunkserver
  }//end namespace tests
}//end namespace oceanbase

int main(int argc, char** argv)
{
  ob_init_memory_pool();
  TBSYS_LOGGER.setLogLevel("ERROR");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
