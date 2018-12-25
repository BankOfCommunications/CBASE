////===================================================================
 //
 // ob_multi_file_utils.cpp / common / Oceanbase
 //
 // Copyright (C) 2010, 2013 Taobao.com, Inc.
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

#include "ob_multi_file_utils.h"

namespace oceanbase
{
  using namespace common;
  namespace updateserver
  {
    MultiFileUtils::MultiFileUtils()
    {
    }

    MultiFileUtils::~MultiFileUtils()
    {
      close();
    }

    int MultiFileUtils::open(const ObString &fname, const bool dio, const bool is_create, const bool is_trunc, const int64_t align_size)
    {
      int32_t ret = OB_SUCCESS;
      if (NULL == fname.ptr())
      {
        TBSYS_LOG(WARN, "fname null pointer");
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        close();
        const char *iter = fname.ptr();
        while (NULL != iter)
        {
          ObString fname;
          fname.assign(const_cast<char*>(iter), static_cast<int32_t>(strlen(iter)));
          ObIFileAppender *file = file_alloc_.alloc();
          if (NULL == file)
          {
            TBSYS_LOG(WARN, "allocate file appender fail [%s]", iter);
            ret = OB_ERROR;
            break;
          }
          else if (OB_SUCCESS != (ret = file->open(fname, dio, is_create, is_trunc, align_size)))
          {
            TBSYS_LOG(WARN, "open [%s] fail ret=%d", iter, ret);
            break;
          }
          else if (OB_SUCCESS != (ret = flist_.push_back(file)))
          {
            TBSYS_LOG(WARN, "push fileinfo to list fail [%s] ret=%d", iter, ret);
            file->close();
            break;
          }
          TBSYS_LOG(INFO, "open [%s] pos=[%ld] succ", iter, flist_.size() - 1);
          while ('\0' != *iter)
          {
            iter++;
          }
          iter++;
          if ('\0' == *iter)
          {
            break;
          }
        }
        if (OB_SUCCESS != ret)
        {
          close();
        }
      }
      return ret;
    }

    int MultiFileUtils::create(const ObString &fname, const bool dio, const int64_t align_size)
    {
      int32_t ret = OB_SUCCESS;
      if (NULL == fname.ptr())
      {
        TBSYS_LOG(WARN, "fname null pointer");
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        close();
        const char *iter = fname.ptr();
        while (NULL != iter)
        {
          ObString fname;
          fname.assign(const_cast<char*>(iter), static_cast<int32_t>(strlen(iter)));
          ObIFileAppender *file = file_alloc_.alloc();
          if (NULL == file)
          {
            TBSYS_LOG(WARN, "allocate file appender fail [%s]", iter);
            ret = OB_ERROR;
            break;
          }
          else if (OB_SUCCESS != (ret = file->create(fname, dio, align_size)))
          {
            TBSYS_LOG(WARN, "open [%s] fail ret=%d", iter, ret);
            break;
          }
          else if (OB_SUCCESS != (ret = flist_.push_back(file)))
          {
            TBSYS_LOG(WARN, "push fileinfo to list fail [%s] ret=%d", iter, ret);
            file->close();
            break;
          }
          TBSYS_LOG(INFO, "open [%s] pos=[%ld] succ", iter, flist_.size() - 1);
          while ('\0' != *iter)
          {
            iter++;
          }
          iter++;
          if ('\0' == *iter)
          {
            break;
          }
        }
        if (OB_SUCCESS != ret)
        {
          close();
        }
      }
      return ret;
    }

    void MultiFileUtils::close()
    {
      ObList<ObIFileAppender*>::iterator iter;
      for (iter = flist_.begin(); iter != flist_.end(); iter++)
      {
        ObIFileAppender *file = *iter;
        if (NULL != file)
        {
          file->close();
          file_alloc_.free(dynamic_cast<FileAppenderImpl*>(file));
          file = NULL;
        }
      }
      flist_.clear();
    }

    int64_t MultiFileUtils::get_file_pos() const
    {
      int64_t ret = -1;
      ObList<ObIFileAppender*>::const_iterator iter;
      ObIFileAppender *file = NULL;
      if (0 == flist_.size())
      {
        TBSYS_LOG(WARN, "no file open");
      }
      else if (flist_.end() == (iter = flist_.begin()))
      {
        TBSYS_LOG(WARN, "flist begin equal end");
      }
      else if (NULL == (file = *iter))
      {
        TBSYS_LOG(WARN, "invalid file pos=[0]");
      }
      else
      {
        ret = file->get_file_pos();
      }
      return ret;
    }

    int MultiFileUtils::append(const void *buf, const int64_t count, bool is_fsync)
    {
      int ret = OB_SUCCESS;
      if (0 == flist_.size())
      {
        TBSYS_LOG(WARN, "no file open");
        ret = OB_ERROR;
      }
      else
      {
        ObList<ObIFileAppender*>::iterator iter;
        int64_t pos = 0;
        for (iter = flist_.begin(); iter != flist_.end(); iter++, pos++)
        {
          ObIFileAppender *file = *iter;
          if (NULL == file)
          {
            TBSYS_LOG(WARN, "invalid file pos=[%ld] buf=%p count=%ld", pos, buf, count);
            ret = OB_ERROR;
            break;
          }
          ret = file->append(buf, count, is_fsync);
          if (OB_SUCCESS != ret)
          {
            TBSYS_LOG(WARN, "append file fail pos=[%ld] ret=%d buf=%p count=%ld", pos, ret, buf, count);
            break;
          }
        }
      }
      return ret;

    }

    int MultiFileUtils::fsync()
    {
      int ret = OB_SUCCESS;
      if (0 == flist_.size())
      {
        TBSYS_LOG(WARN, "no file open");
        ret = OB_ERROR;
      }
      else
      {
        ObList<ObIFileAppender*>::iterator iter;
        int64_t pos = 0;
        for (iter = flist_.begin(); iter != flist_.end(); iter++, pos++)
        {
          ObIFileAppender *file = *iter;
          if (NULL == file)
          {
            TBSYS_LOG(WARN, "invalid file pos=[%ld]", pos);
            ret = OB_ERROR;
            break;
          }
          ret = file->fsync();
          if (OB_SUCCESS != ret)
          {
            TBSYS_LOG(WARN, "fsync fail pos=[%ld] ret=%d", pos, ret);
            break;
          }
        }
      }
      return ret;
    }
  }
}
