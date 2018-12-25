////===================================================================
 //
 // ob_multi_file_utils.h / common / Oceanbase
 //
 // Copyright (C) 2010 Taobao.com, Inc.
 //
 // Created on 2011-04-28 by Yubai (yubai.lk@taobao.com) 
 //
 // -------------------------------------------------------------------
 //
 // Description
 //
 //
 // -------------------------------------------------------------------
 // 
 // Change Log
 //
////====================================================================

#ifndef  OCEANBASE_UPDATESERVER_MULTI_FILE_UTILS_H_
#define  OCEANBASE_UPDATESERVER_MULTI_FILE_UTILS_H_
#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <pthread.h>
#include <new>
#include <algorithm>
#include "common/ob_define.h"
#include "common/ob_file.h"
#include "common/ob_list.h"
#include "common/hash/ob_hashutils.h"

namespace oceanbase
{
  namespace updateserver
  {
    class MultiFileUtils : public common::ObIFileAppender
    {
      typedef common::ObFileAppender FileAppenderImpl;
      typedef common::hash::SimpleAllocer<FileAppenderImpl> FileAlloc;
      static const bool IS_DIRECT = true;
      static const bool IS_CREATE = true;
      static const bool IS_TRUNC = false;
      public:
        MultiFileUtils();
        ~MultiFileUtils();
      public:
        // 多个文件名使用'\0'分隔 最后一个文件名后使用两个'\0'结尾
        int open(const common::ObString &fname, const bool dio, const bool is_create, const bool is_trunc, const int64_t align_size);
        int create(const common::ObString &fname, const bool dio, const int64_t align_size);
        void close();
        int64_t get_file_pos() const;
        int append(const void *buf, const int64_t count, bool is_fsync);
        int fsync();
      private:
        common::ObList<common::ObIFileAppender*> flist_;
        FileAlloc file_alloc_;
    };
  }
}

#endif //OCEANBASE_UPDATESERVER_MULTI_FILE_UTILS_H_

