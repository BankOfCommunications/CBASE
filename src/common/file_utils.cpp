/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * 
 *
 * Version: $Id: file_utils.cpp,v 0.1 2010/07/22 16:58:07 duanfei Exp $
 *
 * Authors:
 *   duanfei <duanfei@taobao.com>
 *     - some work details if you want
 *   Author Name ...
 *
 */

#include <errno.h>
#include "ob_define.h"
#include "file_utils.h"

namespace oceanbase
{
  namespace common
  {

    FileUtils::FileUtils ():own_fd_(true), fd_ (-1)
    {

    }

    FileUtils::FileUtils (int fd) : own_fd_(false), fd_(fd)
    {

    }

    FileUtils::~FileUtils ()
    {
      if (own_fd_)
      {
        this->close ();
      }
    }

    int32_t FileUtils::open (const char *pathname, const int64_t flags, mode_t mode)
    {
      if (pathname == NULL || fd_ >= 0)
      {
        return -1;
      }

      fd_ =::open (pathname, static_cast<int32_t>(flags), mode);
      return fd_ < 0  ? -1 : fd_;
    }

    int32_t FileUtils::open (const char* pathname, const int64_t flags)
    {
      if (pathname == NULL || fd_ >= 0)
      {
        return -1;
      }

      fd_ =::open (pathname, static_cast<int32_t>(flags));
      return fd_ < 0  ? -1 : fd_;
    }

    void FileUtils::close ()
    {
      if (fd_ >= 0)
      {
        ::close (fd_);
        fd_ = -1;
      }
    }

    int64_t FileUtils::lseek (const int64_t offset, const int64_t whence)
    {
      return::lseek (fd_, offset, static_cast<int32_t>(whence));
    }

    int64_t FileUtils::read (char *data, const int64_t size)
    {
      if (fd_ < 0 || data == NULL || size <= 0)
        return -1;

      int64_t iOffset = 0;
      int64_t iRet = 0;
      int64_t length = size;
      do
      {
        iRet =::read (fd_, (data + iOffset), length);
        if (iRet < 0)
        {
          if (errno == EINTR)//and again call read
          {
            continue;
          }

          if (iOffset > 0)
          {
            ::lseek (fd_, 0 - iOffset, SEEK_CUR);
          }
          iOffset = -1;
          break;
        }
        else if (iRet == 0)
        {
          break;
        }
        length  -= iRet;
        iOffset += iRet;
      }
      while (length > 0);
      
      return iOffset;
    }

    int64_t FileUtils::write (const char *data, const int64_t size, bool sync)
    {
      if (data == NULL || size <= 0 || fd_ < 0)
        return -1;
          
      int64_t iOffset = 0;
      int64_t iRet = 0;
      int64_t length = size;
      do
      {
        iRet =::write (fd_, (data + iOffset), length);
        if (iRet < 0)
        {
          if (errno == EINTR)//and again call write
          {
            continue;
          }

          if (iOffset > 0)
          {
            ::lseek (fd_, 0 - iOffset, SEEK_CUR);
          }

          iOffset = -1;
          break;
        }
        length -= iRet;
        iOffset += iRet;
      }
      while (length > 0);

      if (sync)
      {
        ::sync();
      }

      return iOffset;
    }

    int64_t FileUtils::pread(char *data, const int64_t size, const int64_t offset)
    {
      if (NULL == data || size <= 0 || offset < 0)
        return -1;
      return ::pread (fd_, data, size, offset);
    }

    int64_t FileUtils::pwrite(const char *data, const int64_t size, 
                              const int64_t offset, bool sync)
    {
      if (NULL == data || size <= 0 || offset < 0)
        return -1;

      int64_t iRet = ::pwrite (fd_, data, size, offset);
      if (sync && iRet > 0)
      {
        ::sync();
      }
      return iRet;
    }

    int FileUtils::ftruncate(int64_t length)
    {
      return (::ftruncate(fd_, length));
    }

    int64_t FileUtils::get_size()
    {
      int64_t size =0;
      if (fd_ < 0)
      {
        size = -1;
      }
      else
      {
        struct stat st;
        if (0 != (::fstat(fd_, &st)))
        {
          size = -1;
        }
        else
        {
          size = st.st_size;
        }
      }
      return size;
    }

  }//end namespace common
}//end namespace oceanbase
