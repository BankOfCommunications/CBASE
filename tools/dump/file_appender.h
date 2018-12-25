#ifndef __FILE_APPENDER_H__
#define __FILE_APPENDER_H__

#include <deque>
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>
#include "common/utility.h"
#include "common/ob_define.h"

// We preallocate up to an extra megabyte and use memcpy to append new
// data to the file.  This is safe since we either properly close the
// file before reading from it, or for log files, the reading code
// knows enough to skip zero suffixes.
using namespace oceanbase::common;
using namespace tbsys;

class AppendableFile {
  private:
    std::string filename_;
    int fd_;
    size_t page_size_;
    size_t map_size_;       // How much extra memory to map at a time
    char* base_;            // The mapped region
    char* limit_;           // Limit of the mapped region
    char* dst_;             // Where to write next  (in range [base_,limit_])
    char* last_sync_;       // Where have we synced up to
    uint64_t file_offset_;  // Offset of base_ in file
    CThreadMutex append_lock_;

    // Have we done an munmap of unsynced data?
    bool pending_sync_;

    // Roundup x to a multiple of y
    static size_t Roundup(size_t x, size_t y) {
      return ((x + y - 1) / y) * y;
    }

    size_t TruncateToPageBoundary(size_t s) {
      s -= (s & (page_size_ - 1));
      assert((s % page_size_) == 0);
      return s;
    }

    void UnmapCurrentRegion() {
      if (base_ != NULL) {
        if (last_sync_ < limit_) {
          // Defer syncing this data until next Sync() call, if any
          pending_sync_ = true;
        }
        munmap(base_, limit_ - base_);
        file_offset_ += limit_ - base_;
        base_ = NULL;
        limit_ = NULL;
        last_sync_ = NULL;
        dst_ = NULL;

        // Increase the amount we map the next time, but capped at 1MB
        if (map_size_ < (1<<20)) {
          map_size_ *= 2;
        }
      }
    }

    bool MapNewRegion() {
      assert(base_ == NULL);
      if (ftruncate(fd_, file_offset_ + map_size_) < 0) {
        return false;
      }
      void* ptr = mmap(NULL, map_size_, PROT_READ | PROT_WRITE, MAP_SHARED,
                       fd_, file_offset_);
      if (ptr == MAP_FAILED) {
        return false;
      }
      base_ = reinterpret_cast<char*>(ptr);
      limit_ = base_ + map_size_;
      dst_ = base_;
      last_sync_ = base_;
      return true;
    }

  public:
    AppendableFile(const std::string& fname, int fd, size_t page_size)
      : filename_(fname),
      fd_(fd),
      page_size_(page_size),
      map_size_(Roundup(65536, page_size)),
      base_(NULL),
      limit_(NULL),
      dst_(NULL),
      last_sync_(NULL),
      file_offset_(0),
      pending_sync_(false) {
        assert((page_size & (page_size - 1)) == 0);
      }

    static int NewAppendableFile(const std::string &name, AppendableFile *&reslut);

    static int NewAppendableFile(const char *fname, AppendableFile *&reslut);

    virtual ~AppendableFile() {
      if (fd_ >= 0) {
        AppendableFile::Close();
      }
    }

    virtual int AppendNoLock(const char *src, size_t left) {
      while (left > 0) {
        assert(base_ <= dst_);
        assert(dst_ <= limit_);
        size_t avail = limit_ - dst_;
        if (avail == 0) {
          UnmapCurrentRegion();
          MapNewRegion();
        }

        size_t n = (left <= avail) ? left : avail;
        memcpy(dst_, src, n);
        dst_ += n;
        src += n;
        left -= n;
      }
      return OB_SUCCESS;
    }

    inline CThreadMutex &get_append_lock()
    {
      return append_lock_;
    }

    virtual int Append(const char *src, size_t left) {
      CThreadGuard guard(&append_lock_);
      return AppendNoLock(src, left);
    }

    virtual int Close() {
      int ret = OB_SUCCESS;
      size_t unused = limit_ - dst_;
      UnmapCurrentRegion();
      if (unused > 0) {
        // Trim the extra space at the end of the file
        if (ftruncate(fd_, file_offset_ - unused) < 0) {
          ret = OB_ERROR;
          TBSYS_LOG(ERROR, "error ocurrs when ftruncate %s, due to %s", 
                    filename_.c_str(), strerror(errno));
        }
      }

      if (close(fd_) < 0) {
        if (ret == OB_SUCCESS) {
          ret = OB_ERROR;
          TBSYS_LOG(ERROR, "error ocurrs when close %s, due to %s", 
                    filename_.c_str(), strerror(errno));
        }
      }

      fd_ = -1;
      base_ = NULL;
      limit_ = NULL;
      return ret;
    }

    virtual int Flush() {
      return OB_SUCCESS;
    }

    virtual int Sync() {
      int ret = OB_SUCCESS;

      if (pending_sync_) {
        // Some unmapped data was not synced
        pending_sync_ = false;
        if (fdatasync(fd_) < 0) {
          ret = OB_ERROR;
          TBSYS_LOG(ERROR, "error ocurrs when fdatasync %s, due to %s", 
                    filename_.c_str(), strerror(errno));
        }
      }

      if (dst_ > last_sync_) {
        // Find the beginnings of the pages that contain the first and last
        // bytes to be synced.
        size_t p1 = TruncateToPageBoundary(last_sync_ - base_);
        size_t p2 = TruncateToPageBoundary(dst_ - base_ - 1);
        last_sync_ = dst_;
        if (msync(base_ + p1, p2 - p1 + page_size_, MS_SYNC) < 0) {
          ret = OB_ERROR;
          TBSYS_LOG(ERROR, "error ocurrs when msync%s, due to %s", 
                    filename_.c_str(), strerror(errno));
        }
      }

      return ret;
    }
};

#endif
