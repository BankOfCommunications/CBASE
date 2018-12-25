////===================================================================
 //
 // ob_serialization.cpp / hash / common / Oceanbase
 //
 // Copyright (C) 2010 Taobao.com, Inc.
 //
 // Created on 2010-07-23 by Yubai (yubai.lk@taobao.com) 
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

#ifndef  OCEANBASE_COMMON_HASH_SERIALIZATION_H_
#define  OCEANBASE_COMMON_HASH_SERIALIZATION_H_
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include "ob_hashutils.h"

namespace oceanbase
{
  namespace common
  {
    namespace hash
    {
      template <class _archive, class _value>
      int serialization(_archive &ar, const _value &value)
      {
        return (const_cast<_value&>(value)).serialization(ar);
      }
      template <class _archive, class _value>
      int deserialization(_archive &ar, _value &value)
      {
        return value.deserialization(ar);
      }

      class SimpleArchive
      {
        public:
          static const int FILE_OPEN_RFLAG = O_CREAT | O_RDONLY;
          static const int FILE_OPEN_WFLAG = O_CREAT | O_TRUNC | O_WRONLY;
        private:
          static const mode_t FILE_OPEN_MODE = S_IRWXU | S_IRGRP | S_IXGRP | S_IROTH | S_IXOTH;
        public:
          SimpleArchive() : fd_(-1)
          {
          };
          ~SimpleArchive()
          {
            if (-1 != fd_)
            {
              destroy();
            }
          };
        private:
          SimpleArchive(const SimpleArchive &);
          SimpleArchive operator= (const SimpleArchive &);
        public:
          int init(const char *filename, int flag)
          {
            int ret = 0;
            if (NULL == filename
                || (FILE_OPEN_RFLAG != flag && FILE_OPEN_WFLAG != flag))
            {
              HASH_WRITE_LOG(HASH_WARNING, "invalid param filename=%p flag=%x", filename, flag);
              ret = -1;
            }
            else if (-1 == (fd_ = open(filename, flag, FILE_OPEN_MODE)))
            {
              HASH_WRITE_LOG(HASH_WARNING, "open file fail, filename=[%s] flag=%x errno=%u", filename, flag, errno);
              ret = -1;
            }
            else
            {
              // do nothing
            }
            return ret;
          }
          int destroy()
          {
            int ret = 0;
            if (-1 == fd_)
            {
              HASH_WRITE_LOG(HASH_WARNING, "have not inited");
              ret = -1;
            }
            else
            {
              close(fd_);
              fd_ = -1;
            }
            return ret;
          };
          int push(const void *data, int64_t size)
          {
            int ret = 0;
            ssize_t write_ret = 0;
            if (-1 == fd_)
            {
              HASH_WRITE_LOG(HASH_WARNING, "have not inited");
              ret = -1;
            }
            else if (NULL == data || 0 == size)
            {
              HASH_WRITE_LOG(HASH_WARNING, "invalid param data=%p size=%ld", data, size);
              ret = -1;
            }
            else if (size != (int64_t)(write_ret = write(fd_, data, size)))
            {
              HASH_WRITE_LOG(HASH_WARNING, "write fail errno=%u fd_=%d data=%p size=%ld write_ret=%ld",
                  errno, fd_, data, size, write_ret);
              ret = -1;
            }
            else
            {
              // do nothing
            }
            return ret;
          };
          int pop(void *data, int64_t size)
          {
            int ret = 0;
            ssize_t read_ret = 0;
            if (-1 == fd_)
            {
              HASH_WRITE_LOG(HASH_WARNING, "have not inited");
              ret = -1;
            }
            else if (NULL == data || 0 == size)
            {
              HASH_WRITE_LOG(HASH_WARNING, "invalid param data=%p size=%ld", data, size);
              ret = -1;
            }
            else if (size != (int64_t)(read_ret = read(fd_, data, size)))
            {
              HASH_WRITE_LOG(HASH_WARNING, "read fail errno=%u fd_=%d data=%p size=%ld read_ret=%ld",
                  errno, fd_, data, size, read_ret);
              ret = -1;
            }
            else
            {
              // do nothing
            }
            return ret;
          };
        private:
          int fd_;
      };

      // 基本类型偏特化宏
      #define _SERIALIZATION_SPEC(type) \
      template <class _archive> \
      int serialization(_archive &ar, type &value) \
      { \
        return ar.push(&value, sizeof(value)); \
      }
      #define _DESERIALIZATION_SPEC(type) \
      template <class _archive> \
      int deserialization(_archive &ar, type &value) \
      { \
        return ar.pop(&value, sizeof(value)); \
      }
      _SERIALIZATION_SPEC(int8_t);
      _SERIALIZATION_SPEC(uint8_t);
      _SERIALIZATION_SPEC(const int8_t);
      _SERIALIZATION_SPEC(const uint8_t);
      _SERIALIZATION_SPEC(int16_t);
      _SERIALIZATION_SPEC(uint16_t);
      _SERIALIZATION_SPEC(const int16_t);
      _SERIALIZATION_SPEC(const uint16_t);
      _SERIALIZATION_SPEC(int32_t);
      _SERIALIZATION_SPEC(uint32_t);
      _SERIALIZATION_SPEC(const int32_t);
      _SERIALIZATION_SPEC(const uint32_t);
      _SERIALIZATION_SPEC(int64_t);
      _SERIALIZATION_SPEC(uint64_t);
      _SERIALIZATION_SPEC(const int64_t);
      _SERIALIZATION_SPEC(const uint64_t);
      _SERIALIZATION_SPEC(float);
      _SERIALIZATION_SPEC(const float);
      _SERIALIZATION_SPEC(double);
      _SERIALIZATION_SPEC(const double);

      _DESERIALIZATION_SPEC(int8_t);
      _DESERIALIZATION_SPEC(uint8_t);
      _DESERIALIZATION_SPEC(int16_t);
      _DESERIALIZATION_SPEC(uint16_t);
      _DESERIALIZATION_SPEC(int32_t);
      _DESERIALIZATION_SPEC(uint32_t);
      _DESERIALIZATION_SPEC(int64_t);
      _DESERIALIZATION_SPEC(uint64_t);
      _DESERIALIZATION_SPEC(float);
      _DESERIALIZATION_SPEC(double);

      template <class _archive>
      int serialization(_archive &ar, const HashNullObj &value)
      {
        return 0;
      }

      template <class _archive>
      int deserialization(_archive &ar, HashNullObj &value)
      {
        return 0;
      }

      template <class _archive, typename _T1, typename _T2>
      int serialization(_archive &ar, const HashMapPair<_T1, _T2> &pair)
      {
        int ret = 0;
        if (0 != serialization(ar, pair.first)
            || 0 != serialization(ar, pair.second))
        {
          ret = -1;
        }
        return ret;
      }

      template <class _archive, typename _T1, typename _T2>
      int deserialization(_archive &ar, HashMapPair<_T1, _T2> &pair)
      {
        int ret = 0;
        if (0 != deserialization(ar, pair.first)
            || 0 != deserialization(ar, pair.second))
        {
          ret = -1;
        }
        return ret;
      }
    }
  }
}

#endif //OCEANBASE_COMMON_HASH_SERIALIZATION_H_

