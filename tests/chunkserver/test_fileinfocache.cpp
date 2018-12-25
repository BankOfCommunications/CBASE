/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *  
 * test_fileinfocache.cpp for test file info cache
 *
 * Authors: 
 *   yubai< yubai.lk@taobao.com>
 *   huating <huating.zmq@taobao.com>
 *
 */

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <new>
#include "ob_fileinfo_cache.h"
#include "gtest/gtest.h"

using namespace oceanbase;
using namespace chunkserver;
using namespace sstable;
using namespace common;

namespace oceanbase
{
  namespace sstable 
  {
    bool &get_succ()
    {
      static bool succ = true;
      return succ;
    }

    int get_sstable_path(const ObSSTableId& sstable_id,char *path,int64_t path_len)
    {
      if (get_succ())
      {
        snprintf(path, path_len, "./data/sst_%lu", sstable_id.sstable_file_id_);
        return OB_SUCCESS;
      }
      else
      {
        get_succ() = true;
        return OB_ERROR;
      }
    }
  }
}

TEST(TestFileInfoCache, get_fileinfo)
{
  FileInfoCache fic;
  fic.init(1024);
  uint64_t handle = 1048576;
  const IFileInfo *ret = NULL;
  const IFileInfo *nil = NULL;
  int invalid_fd = -1;

  get_succ() = false;
  EXPECT_EQ(nil, fic.get_fileinfo(handle));
  fic.destroy();

  fic.init(1024);
  EXPECT_NE(nil, (ret = fic.get_fileinfo(handle)));
  EXPECT_NE(invalid_fd, ret->get_fd());
  EXPECT_EQ(ret, fic.get_fileinfo(handle));

  EXPECT_EQ(nil, fic.get_fileinfo(handle + 1));

  fic.revert_fileinfo(ret);
  fic.revert_fileinfo(ret);

  fic.destroy();
}

TEST(TestFileInfoCache, revert_fileinfo)
{
  FileInfoCache fic;
  fic.init(1024);
  uint64_t handle = 1048576;
  const IFileInfo* fileinfo = NULL;

  EXPECT_EQ(OB_ERROR, fic.revert_fileinfo(NULL));
  fileinfo = fic.get_fileinfo(handle);
  EXPECT_EQ(OB_SUCCESS, fic.revert_fileinfo(fileinfo));

  fic.destroy();
}

TEST(TestFileInfoCache, clear)
{
  FileInfoCache fic;
  
  EXPECT_EQ(OB_SUCCESS, fic.clear());
  fic.init(1024);  
  EXPECT_EQ(OB_SUCCESS, fic.clear());
  uint64_t handle = 1048576;
  const IFileInfo *ret = NULL;
  ret = fic.get_fileinfo(handle);
  EXPECT_EQ(OB_ERROR, fic.clear());
  fic.revert_fileinfo(ret);
  EXPECT_EQ(OB_SUCCESS, fic.clear());
}

int main(int argc, char **argv)
{
  ob_init_memory_pool();
  TBSYS_LOGGER.setLogLevel("ERROR");
  testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
