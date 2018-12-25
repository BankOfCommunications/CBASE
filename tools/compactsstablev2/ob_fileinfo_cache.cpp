#include "ob_fileinfo_cache.h"

namespace oceanbase
{
  namespace chunkserver
  {
    using namespace common;
    using namespace compactsstablev2;

    FileInfo::FileInfo() : handle_()
    {
      fd_ = -1;
    }

    FileInfo::~FileInfo()
    {
      destroy();
    }

    FileInfo::FileInfo(const FileInfo &other) : FileInfoBase(), handle_()
    {
      fd_ = -1;
      if (-1 != other.fd_)
      {
        fd_ = dup(other.fd_);
      }
    }

    int FileInfo::init(const uint64_t sstable_id, const bool use_dio)
    {
      int ret             = OB_SUCCESS;
      char *sstable_fname = NULL;

      destroy();
      if (NULL == (sstable_fname = get_sstable_fname_(sstable_id)))
      {
        TBSYS_LOG(WARN, "get sstable filename fail sstable_id=%lu", sstable_id);
        ret = OB_ERROR;
      }
      else if (!use_dio && -1 == (fd_ = open(sstable_fname, FILE_OPEN_NORMAL_RFLAG)))
      {
        TBSYS_LOG(WARN, "normal open sstable file fail errno=%u error_str=%s "
                        "filename=%s sstable_id=%lu", 
                  errno, strerror(errno), sstable_fname, sstable_id);
        ret = OB_ERROR;
      }
      else if (use_dio && -1 == (fd_ = open(sstable_fname, FILE_OPEN_DIRECT_RFLAG)))
      {
        TBSYS_LOG(WARN, "direct open sstable file fail errno=%u error_str=%s "
                        "filename=%s sstable_id=%lu", 
                  errno, strerror(errno), sstable_fname, sstable_id);
        ret = OB_ERROR;
      }
      else
      {
        TBSYS_LOG(INFO, "open file succ fd=%d sstable_filename=%s "
                        "sstable_id=%lu", 
                  fd_, sstable_fname, sstable_id);
      }

      return ret;
    }

    void FileInfo::destroy()
    {
      if (-1 != fd_)
      {
        close(fd_);
        fd_ = -1;
      }
    }

    void FileInfo::set_cache_handle(const Handle &handle)
    {
      handle_ = handle;
    }

    const FileInfo::Handle &FileInfo::get_cache_handle() const
    {
      return handle_;
    }

    int FileInfo::get_fd() const
    {
      return fd_;
    }

    char *FileInfo::get_sstable_fname_(uint64_t sstable_id)
    {
      static const int64_t fname_buf_size = 2048;
      static __thread char fname_buf[fname_buf_size];
      char *ret = fname_buf;

      if (OB_SUCCESS != get_sstable_path(sstable_id, fname_buf, fname_buf_size))
      {
        ret = NULL;
      }

      return ret;
    }

    FileInfoCache::FileInfoCache()
    {
    }

    FileInfoCache::~FileInfoCache()
    {
    }

    int FileInfoCache::init(const int64_t max_cache_num)
    {
      return cache_.init(max_cache_num * KVCACHE_BLOCK_SIZE);
    };

    int FileInfoCache::destroy()
    {
      return cache_.destroy();
    };

    const FileInfo *FileInfoCache::get_cache_fileinfo(const uint64_t sstable_id)
    {
      FileInfo *ret = NULL;
      Handle tmp_handle;

      if (OB_SUCCESS == cache_.get(sstable_id, ret, tmp_handle) && NULL != ret)
      {
        if (ret->get_fd() >= 0)
        {
          ret->set_cache_handle(tmp_handle);
        }
        else
        {
          /**
           * got a file_info with invalid fd_, just skip it, and return 
           * NULL file_info pointer 
           */
          cache_.revert(tmp_handle);
          ret = NULL;
        }      
      }

      return ret;
    }

    const IFileInfo *FileInfoCache::get_fileinfo(const uint64_t sstable_id)
    {
      FileInfo *ret = NULL;
      int tmp_ret   = OB_SUCCESS;
      Handle tmp_handle;

      if (OB_SUCCESS != (tmp_ret = cache_.get(sstable_id, ret, tmp_handle, false)))
      {
        FileInfo file_info;
        if (OB_SUCCESS != (tmp_ret = file_info.init(sstable_id)))
        {
          TBSYS_LOG(WARN, "init file info fail sstable_id=%lu", sstable_id);
          /**
           * add a invalid file_info into cache, and kvcache can wash it 
           * out correctly 
           */
          tmp_ret = cache_.put(sstable_id, &file_info, false);
          if (OB_SUCCESS != tmp_ret)
          {
            TBSYS_LOG(WARN, "failed to put fake file info, sstable_id=%lu, fd_=%d"
                            "err=%d",
                      sstable_id, file_info.get_fd(), tmp_ret);
          }
          ret = NULL;
        }
        else
        {
          tmp_ret = cache_.put_and_fetch(sstable_id, &file_info, ret, 
                                         tmp_handle, false, false);
          if (OB_SUCCESS != tmp_ret && NULL == ret)
          {
            TBSYS_LOG(WARN, "failed to put and fetch file info, sstable_id=%lu, "
                            "err=%d, ret=%p",
                      sstable_id, tmp_ret, ret);
          }
        }
      }

      if (OB_SUCCESS == tmp_ret && NULL != ret)
      {
        if (ret->get_fd() >= 0)
        {
          ret->set_cache_handle(tmp_handle);
        }
        else
        {
          /**
           * got a file_info with invalid fd_, just skip it, and return 
           * NULL file_info pointer 
           */
          cache_.revert(tmp_handle);
          ret = NULL;
        }
      }

      return ret;
    }

    int FileInfoCache::revert_fileinfo(const IFileInfo *file_info)
    {
      int ret = OB_SUCCESS;

      if (NULL == file_info)
      {
        ret = OB_ERROR;
      }
      else
      {
        const FileInfo *file_info_sub = dynamic_cast<const FileInfo*>(file_info);
        Handle handle2revert = file_info_sub->get_cache_handle();
        cache_.revert(handle2revert);
      }

      return ret;
    }

    int FileInfoCache::clear()
    {
      return cache_.clear();
    }
  }
}
