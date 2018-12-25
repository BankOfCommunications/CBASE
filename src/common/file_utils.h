/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * 
 *
 * Version: $Id: file_utils.h,v 0.1 2010/07/22 16:57:07 duanfei Exp $
 *
 * Authors:
 *   duanfei <duanfei@taobao.com>
 *     - some work details if you want
 *   Author Name ...
 *
 */

#ifndef OCEANBASE_COMMON_FILE_UTILS_H_
#define OCEANBASE_COMMON_FILE_UTILS_H_

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

namespace oceanbase
{
  namespace common
  {
    class FileUtils
    {
      public:
        FileUtils ();
        explicit FileUtils(int fd);
        virtual ~ FileUtils ();

        virtual int32_t open (const char *filepath, const int64_t flags, mode_t mode);
        int32_t open (const char* filepath, const int64_t flags);
        virtual void close ();
        int64_t lseek (const int64_t offset, const int64_t whence);
        int64_t read (char *data, const int64_t size);
        virtual int64_t write (const char *data, const int64_t size, bool sync = false);
        int64_t pread(char *data, const int64_t size, const int64_t offset);
        int64_t pwrite(const char *data, const int64_t size, const int64_t offfset, 
                       bool sync = false);
        int64_t get_size();
        virtual int ftruncate(int64_t length);
      private:
        bool own_fd_;
        int32_t fd_;
    };
  }       //end namespace common
}       //end namespace oceanbase
#endif
