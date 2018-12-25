/**
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * Authors:
 *   yuanqi <yuanqi.xhf@taobao.com>
 *     - some work details if you want
 */
#include "ob_log_data_writer.h"
#include "ob_log_dir_scanner.h"
#include "ob_log_cursor.h"
#include "ob_log_generator.h"
#include "ob_file.h"
#include "sys/vfs.h"

namespace oceanbase
{
  namespace common
  {
    ObLogDataWriter::AppendBuffer::AppendBuffer(): file_pos_(-1), buf_(NULL), buf_end_(0), buf_limit_(DEFAULT_BUF_SIZE)
    {}

    ObLogDataWriter::AppendBuffer::~AppendBuffer()
    {
      if (NULL != buf_)
      {
        free(buf_);
        buf_ = NULL;
      }
    }

    int ObLogDataWriter::AppendBuffer::write(const char* buf, int64_t len, int64_t pos)
    {
      int err = OB_SUCCESS;
      int sys_err = 0;
      int64_t file_pos = 0;
      if (NULL == buf || len < 0 || pos < 0)
      {
        err = OB_INVALID_ARGUMENT;
      }
      else if (NULL == buf_
               && 0 != (sys_err = posix_memalign((void**)&buf_, ObLogGenerator::LOG_FILE_ALIGN_SIZE, buf_limit_)))
      {
        err = OB_ALLOCATE_MEMORY_FAILED;
        TBSYS_LOG(ERROR, "posix_memalign(align=%ld, size=%ld): %s",
                  ObLogGenerator::LOG_FILE_ALIGN_SIZE, buf_limit_, strerror(sys_err));
      }
      else if (len > buf_limit_)
      {
        err = OB_INVALID_ARGUMENT;
        TBSYS_LOG(ERROR, "write_size[%ld] overflow: buf_limit=%ld", len, buf_limit_);
      }
      else
      {
        file_pos = file_pos_ >= 0? file_pos_: pos;
      }
      if (OB_SUCCESS != err)
      {}
      else if (pos < file_pos || pos > file_pos + buf_end_)
      {
        err = OB_DISCONTINUOUS_LOG;
        TBSYS_LOG(ERROR, "write(bufferd=[%ld,+%ld], append=[%ld,+%ld])", file_pos, buf_end_, pos, len);
      }
      else if (file_pos + buf_limit_ < pos + len)
      {
        err = OB_BUF_NOT_ENOUGH;
        TBSYS_LOG(DEBUG, "AppendBuf not ENOUGH: file_pos[%ld] + buf_limit[%ld] < pos[%ld] + len[%ld]",
                  file_pos, buf_limit_, pos, len);
      }
      else
      {
        memcpy(buf_ + (pos - file_pos), buf, len);
        buf_end_ = pos + len - file_pos;
        file_pos_ = file_pos;
      }
      return err;
    }

    int ObLogDataWriter::AppendBuffer::flush(int fd)
    {
      int err = OB_SUCCESS;
      if (fd < 0)
      {
        err = OB_INVALID_ARGUMENT;
      }
      else if (buf_end_ <= 0)
      {} // do nothing
      else if (unintr_pwrite(fd, buf_, buf_end_, file_pos_) != buf_end_)
      {
        err = OB_IO_ERROR;
        TBSYS_LOG(ERROR, "uniter_pwrite(fd=%d, buf=%p[%ld], pos=%ld):%s",
                  fd, buf_, buf_end_, file_pos_, strerror(errno));
      }
      else
      {
        file_pos_ = -1;
        buf_end_ = 0;
      }
      return err;
    }

    ObLogDataWriter::ObLogDataWriter():
      log_dir_(NULL),
      file_size_(0),
      end_cursor_(),
      log_sync_type_(OB_LOG_SYNC),
      fd_(-1), cur_file_id_(-1),
      num_file_to_add_(-1), min_file_id_(0),
      min_avail_file_id_(-1),
      min_avail_file_id_getter_(NULL)
    {
    }

    ObLogDataWriter::~ObLogDataWriter()
    {
      int err = OB_SUCCESS;
      if (NULL != log_dir_)
      {
        free((void*)log_dir_);
        log_dir_ = NULL;
      }
      if (fd_ > 0)
      {
        if (OB_LOG_NOSYNC == log_sync_type_ && OB_SUCCESS != (err = write_buffer_.flush(fd_)))
        {
          TBSYS_LOG(ERROR, "write_buffer_.flush(fd=%d)=>%d", fd_, err);
        }
        if (0 != close(fd_))
        {
          TBSYS_LOG(ERROR, "close(fd=%d):%s", fd_, strerror(errno));
        }
      }
    }

    int64_t ObLogDataWriter::to_string(char* buf, const int64_t len) const
    {
      int64_t pos = 0;
      databuff_printf(buf, len, pos, "DataWriter(%s, sync_type=%ld)", to_cstring(end_cursor_), log_sync_type_);
      return pos;
    }

    int ObLogDataWriter::init(const char* log_dir, const int64_t file_size, int64_t du_percent,
                              const int64_t log_sync_type,
                              MinAvailFileIdGetter* min_avail_file_id_getter)
    {
      int err = OB_SUCCESS;
      ObLogDirScanner scanner;
      struct statfs fsst;
      if (NULL != log_dir_)
      {
        err = OB_INIT_TWICE;
      }
      else if (NULL == log_dir || file_size <= 0 || du_percent < 0 || du_percent > 100)
      {
        err = OB_INVALID_ARGUMENT;
        TBSYS_LOG(ERROR, "log_dir == %p, file_size=%ld, du_percent=%ld, min_avail_fid_getter=%p",
                  log_dir, file_size, du_percent, min_avail_file_id_getter);
      }
      else if (OB_SUCCESS != (err = scanner.init(log_dir))
               && OB_DISCONTINUOUS_LOG != err)
      {
        TBSYS_LOG(ERROR, "scanner.init(log_dir=%s)=>%d", log_dir, err);
      }
      else if (OB_SUCCESS != (err = scanner.get_min_log_id((uint64_t&)min_file_id_)) && OB_ENTRY_NOT_EXIST != err)
      {
        TBSYS_LOG(ERROR, "scanner.get_min_log_file_id()=>%d", err);
      }
      else
      {
        err = OB_SUCCESS;
      }
      if (OB_SUCCESS != err)
      {}
      else if(0 != statfs(log_dir, &fsst))
      {
        err = OB_IO_ERROR;
        TBSYS_LOG(ERROR, "statfs(dir=%s)=>%s", log_dir, strerror(errno));
      }
      else
      {
        log_dir_ = strdup(log_dir);
        file_size_ = file_size;
        log_sync_type_ = log_sync_type;
        num_file_to_add_ = (int64_t)fsst.f_bsize * ((int64_t)fsst.f_blocks * du_percent /100LL - (int64_t)(fsst.f_blocks - fsst.f_bavail))/file_size;
        min_avail_file_id_getter_ = min_avail_file_id_getter;
        TBSYS_LOG(INFO, "log_data_writer.init(log_dir=%s, fsize=%ld, dsize=%ld/%ld, du_limit=%ld%%, new_file_count=%ld)",
                  log_dir, file_size,
                  fsst.f_bsize * fsst.f_bavail, fsst.f_bsize * fsst.f_blocks,
                  du_percent, num_file_to_add_);
      }
      return err;
    }

    int myfallocate_by_append(int fd, int mode, off_t offset, off_t len)
    {
      int err = 0;
      static char buf[1<<20] __attribute__ ((aligned(DIO_ALIGN_SIZE)));
      int64_t count = 0;
      UNUSED(mode);
      if (offset < 0 || len <= 0)
      {
        errno = -EINVAL;
        err = -1;
      }
      for(int64_t pos = offset; 0 == err && pos < offset + len; pos += count)
      {
        count = min(offset + len - pos, sizeof(buf));
        if (unintr_pwrite(fd, buf, count, pos) != count)
        {
          err = -1;
          TBSYS_LOG(ERROR, "uniter_pwrite(pos=%ld, count=%ld) fail", pos, count);
          break;
        }
      }
      return err;
    }
# if __linux && (__GLIBC__ > 2 || (__GLIBC__ == 2 && __GLIBC_MINOR__ >= 10))
    int myfallocate(int fd, int mode, off_t offset, off_t len)
    {
      int err = 0;
      static bool syscall_supported = true;
      if (syscall_supported && 0 != (err = fallocate(fd, mode, offset, len)))
      {
        syscall_supported = false;
        TBSYS_LOG(WARN, "glibc support fallocate(), but fallocate still fail with %s, fallback to call myfallocate_by_append()", strerror(errno));
      }
      if (!syscall_supported)
      {
        err = myfallocate_by_append(fd, mode, offset, len);
      }
      return err;
    }
#else
    int myfallocate(int fd, int mode, off_t offset, off_t len)
    {
      return myfallocate_by_append(fd, mode, offset, len);
    }
#endif
    int file_expand_by_fallocate(const int fd, const int64_t file_size)
    {
      int err = OB_SUCCESS;
      struct stat st;
      if (fd < 0 || file_size <= 0)
      {
        err = OB_INVALID_ARGUMENT;
      }
      else if (0 != fstat(fd, &st))
      {
        err = OB_IO_ERROR;
        TBSYS_LOG(ERROR, "fstat(%d):%s", fd, strerror(errno));
      }
      else if (0 != (st.st_size & ObLogGenerator::LOG_FILE_ALIGN_MASK)
               || 0 != (file_size & ObLogGenerator::LOG_FILE_ALIGN_MASK))
      {
        err = OB_LOG_NOT_ALIGN;
        TBSYS_LOG(ERROR, "file_size[%ld] or file_size[%ld] not align by %lx",
                  st.st_size, file_size, ObLogGenerator::LOG_FILE_ALIGN_MASK);
      }
      else if (st.st_size >= file_size)
      {} //do nothing
      else if (0 != myfallocate(fd, 0, st.st_size, file_size - st.st_size))
      {
        err = OB_IO_ERROR;
        TBSYS_LOG(ERROR, "fallocate(fd=%d):%s", fd, strerror(errno));
      }
      else if (0 != fsync(fd))
      {
        err = OB_IO_ERROR;
        TBSYS_LOG(ERROR, "fsync(fd=%d):%s", fd, strerror(errno));
      }
      return err;
    }

    // 文件原来的大小需要512字节对齐
    int file_expand_by_append(const int fd, const int64_t file_size)
    {
      int err = OB_SUCCESS;
      struct stat st;
      static char buf[1<<20] __attribute__ ((aligned(DIO_ALIGN_SIZE)));
      int64_t count = 0;
      bool need_fsync = false;
      if (fd < 0 || file_size <= 0)
      {
        err = OB_INVALID_ARGUMENT;
      }
      else if (0 != fstat(fd, &st))
      {
        err = OB_IO_ERROR;
        TBSYS_LOG(ERROR, "fstat(%d):%s", fd, strerror(errno));
      }
      else if (0 != (st.st_size & ObLogGenerator::LOG_FILE_ALIGN_MASK)
               || 0 != (file_size & ObLogGenerator::LOG_FILE_ALIGN_MASK))
      {
        err = OB_LOG_NOT_ALIGN;
        TBSYS_LOG(ERROR, "file_size[%ld] or file_size[%ld] not align by %lx",
                  st.st_size, file_size, ObLogGenerator::LOG_FILE_ALIGN_MASK);
      }
      for(int64_t pos = st.st_size; OB_SUCCESS == err && pos < file_size; pos += count)
      {
        need_fsync = true;
        count = min(file_size - pos, sizeof(buf));
        if (unintr_pwrite(fd, buf, count, pos) != count)
        {
          err = OB_IO_ERROR;
          TBSYS_LOG(ERROR, "uniter_pwrite(pos=%ld, count=%ld)", pos, count);
          break;
        }
      }
      if (OB_SUCCESS == err && need_fsync && 0 != fsync(fd))
      {
        err = OB_IO_ERROR;
        TBSYS_LOG(ERROR, "fsync(fd=%d):%s", fd, strerror(errno));
      }
      return err;
    }

    int ObLogDataWriter::write(const ObLogCursor& start_cursor, const ObLogCursor& end_cursor,
                               const char* data, const int64_t len)
    {
      int err = OB_SUCCESS;
      if (NULL == log_dir_)
      {
        err = OB_NOT_INIT;
      }
      else if (NULL == data || len < 0 || !start_cursor.is_valid() || !end_cursor.is_valid())
      {
        err = OB_INVALID_ARGUMENT;
        TBSYS_LOG(ERROR, "write(cursor=[%s,%s], data=%p[%ld])",
                  to_cstring(start_cursor), to_cstring(end_cursor), data, len);
      }
      else if (0 != (((uint64_t)data) & ObLogGenerator::LOG_FILE_ALIGN_MASK)
               || 0 != (len & ObLogGenerator::LOG_FILE_ALIGN_MASK))
      {
        err = OB_LOG_NOT_ALIGN;
        TBSYS_LOG(ERROR, "write_log(data=%p, len=%ld): NOT_ALIGN", data, len);
      }
      else if (!start_cursor.equal(end_cursor_))
      {
        err = OB_DISCONTINUOUS_LOG;
      }
      else if (OB_LOG_NOT_PERSISTENT == log_sync_type_)
      {
        for(int64_t i = 0; i < 4000; i++)
        {
          PAUSE();
        }
      }
      else if (OB_SUCCESS != (err = prepare_fd(start_cursor.file_id_)))
      {
        TBSYS_LOG(ERROR, "prepare_fd(%ld)=>%d", start_cursor.file_id_, err);
      }
      else if (OB_LOG_NOSYNC == log_sync_type_)
      {
        if (OB_SUCCESS != (err = write_buffer_.write(data, len, start_cursor.offset_))
          && OB_BUF_NOT_ENOUGH != err)
        {
          TBSYS_LOG(ERROR, "writer_buffer_.write(%p[%ld], pos=%ld)=>%d", data, len, start_cursor.offset_, err);
        }
        else if (OB_SUCCESS == err)
        {}
        else if (OB_SUCCESS != (err = write_buffer_.flush(fd_)))
        {
          TBSYS_LOG(ERROR, "write_buffer_.flush(fd_=%d)=>%d", fd_, err);
        }
        else if (OB_SUCCESS != (err = write_buffer_.write(data, len, start_cursor.offset_)))
        {
          TBSYS_LOG(ERROR, "send write fail: start_cursor=%s, data=%p[%ld]", to_cstring(start_cursor), data, len);
        }
      }
      else
      {
        if (unintr_pwrite(fd_, data, len, start_cursor.offset_) != len)
        {
          err = OB_IO_ERROR;
          TBSYS_LOG(ERROR, "pwrite(fd=%d, buf=%p[%ld]), cursor=%s): %s",
                    fd_, data, len, to_cstring(start_cursor), strerror(errno));
        }
        else if (0 != fdatasync(fd_))
        {
          err = OB_IO_ERROR;
          TBSYS_LOG(ERROR, "fdatasync(%d):%s", fd_, strerror(errno));
        }
      }
      if (OB_SUCCESS == err)
      {
        end_cursor_ = end_cursor;
      }
      return err;
    }

    int ObLogDataWriter::prepare_fd(const int64_t file_id)
    {
      int err = OB_SUCCESS;
      char fname[OB_MAX_FILE_NAME_LENGTH];
      char pool_file[OB_MAX_FILE_NAME_LENGTH];
      int64_t count = 0;
      if (cur_file_id_ == file_id)
      {
        if (fd_ < 0)
        {
          cur_file_id_ = -1;
          err = OB_ERR_UNEXPECTED;
          TBSYS_LOG(ERROR, "fd[%d] < 0, file_id=%ld", fd_, file_id);
        }
      }
      else
      {
        cur_file_id_ = -1;
        if (fd_ >= 0)
        {
          if (OB_LOG_NOSYNC == log_sync_type_ && OB_SUCCESS != (err = write_buffer_.flush(fd_)))
          {
            TBSYS_LOG(ERROR, "write_buffer_.flush(fd=%d)=>%d", fd_, err);
          }
          else if (NULL != min_avail_file_id_getter_ && OB_SUCCESS != (err = file_expand_by_fallocate(fd_, file_size_)))
          {
            TBSYS_LOG(ERROR, "file_expand_to(fd=%d, size=%ld):%s", fd_, file_size_, strerror(errno));
          }
          if (0 != close(fd_))
          {
            err = OB_IO_ERROR;
            TBSYS_LOG(ERROR, "close(fd=%d):%s", fd_, strerror(errno));
          }
        }
        fd_ = -1;
      }
      if (OB_SUCCESS != err || fd_ >= 0)
      {}
      else if ((count = snprintf(fname, sizeof(fname), "%s/%ld", log_dir_, file_id)) < 0
               || count >= (int64_t)sizeof(fname))
      {
        err = OB_BUF_NOT_ENOUGH;
        TBSYS_LOG(ERROR, "file name too long, log_dir=%s, log_id=%ld, buf_size=%ld",
                  log_dir_, file_id, sizeof(fname));
      }
      else if ((fd_ = open(fname, OPEN_FLAG, OPEN_MODE)) >= 0)
      {
        TBSYS_LOG(INFO, "old %s exist, append clog to it", fname);
      }
      else if ((NULL == select_pool_file(pool_file, sizeof(pool_file))
                || (fd_ = reuse(pool_file, fname)) < 0)
               && (fd_ = open(fname, CREATE_FLAG, OPEN_MODE)) < 0)
      {
        err = OB_IO_ERROR;
        TBSYS_LOG(ERROR, "open(%s): %s", fname, strerror(errno));
      }
      if (OB_SUCCESS == err)
      {
        cur_file_id_ = file_id;
        if (0 == min_file_id_)
        {
          min_file_id_ = cur_file_id_;
        }
      }
      return err;
    }

    int ObLogDataWriter::reuse(const char* pool_file, const char* fname)
    {
      int err = OB_SUCCESS;
      char tmp_pool_file[OB_MAX_FILE_NAME_LENGTH];
      int64_t len = 0;
      int fd = -1;
      // 允许open和rename失败，但是不允许pwrite()失败.
      if (NULL == pool_file || NULL == fname)
      {
        err = OB_INVALID_ARGUMENT;
      }
      else if ((len = snprintf(tmp_pool_file, sizeof(tmp_pool_file), "%s.tmp", pool_file)) < 0
               || len >= (int64_t)sizeof(tmp_pool_file))
      {
        err = OB_BUF_NOT_ENOUGH;
      }
      else if (0 != rename(pool_file, tmp_pool_file))
      {
        err = OB_IO_ERROR;
        TBSYS_LOG(WARN, "rename(%s,%s):%s", pool_file, tmp_pool_file, strerror(errno));
      }
      else if ((fd = open(tmp_pool_file, OPEN_FLAG, OPEN_MODE)) < 0)
      {
        err = OB_IO_ERROR;
        TBSYS_LOG(ERROR, "open(%s) failed: %s", tmp_pool_file, strerror(errno));
      }
      else if (unintr_pwrite(fd, ObLogGenerator::eof_flag_buf_,
                             sizeof(ObLogGenerator::eof_flag_buf_), 0) != sizeof(ObLogGenerator::eof_flag_buf_))
      {
        err = OB_IO_ERROR;
        TBSYS_LOG(ERROR, "write_eof_flag fail(%s): %s", tmp_pool_file, strerror(errno));
      }
      else if (0 != fsync(fd))
      {
        err = OB_IO_ERROR;
        TBSYS_LOG(ERROR, "fdatasync(%s):%s", tmp_pool_file, strerror(errno));
      }
      else if (0 != rename(tmp_pool_file, fname))
      {
        err = OB_IO_ERROR;
        TBSYS_LOG(ERROR, "rename(%s,%s):%s", pool_file, fname, strerror(errno));
      }
      if (OB_SUCCESS != err && fd > 0)
      {
        close(fd);
        fd = -1;
      }
      return fd;
    }

    const char* ObLogDataWriter::select_pool_file(char* buf, const int64_t buf_len)
    {
      char* result = NULL;
      int64_t len = 0;
      if (NULL == min_avail_file_id_getter_)
      {}
      else if (0 == min_file_id_)
      {
        num_file_to_add_--;
        TBSYS_LOG(INFO, "min_file_id has not inited, can not reuse file, will create new file num_file_to_add=%ld", num_file_to_add_);
      }
      else if (min_file_id_ < 0)
      {
        TBSYS_LOG(ERROR, "min_file_id[%ld] < 0", min_file_id_);
      }
      else if (num_file_to_add_ > 0)
      {
        num_file_to_add_--;
        TBSYS_LOG(INFO, "num_file_to_add[%ld] >= 0 will create new file.", num_file_to_add_);
      }
      else if (min_file_id_ > min_avail_file_id_
               && min_file_id_ > (min_avail_file_id_ = min_avail_file_id_getter_->get()))
      {
        TBSYS_LOG(WARN, "can not select pool_file: min_file_id=%ld, min_avail_file_id=%ld",
                  min_file_id_, min_avail_file_id_);
      }
      else if ((len = snprintf(buf, buf_len, "%s/%ld", log_dir_, min_file_id_)) < 0
               || len >= buf_len)
      {
        TBSYS_LOG(ERROR, "gen fname fail: buf_len=%ld, fname=%s/%ld", buf_len, log_dir_, min_file_id_);
      }
      else
      {
        result = buf;
        min_file_id_++;
      }
      TBSYS_LOG(INFO, "select_pool_file(num_file_to_add=%ld, min_file_id=%ld, max_reusable_fid=%ld)=>%s",
                num_file_to_add_, min_file_id_, min_avail_file_id_, result);
      return result;
    }

    int ObLogDataWriter::check_eof_after_log_cursor(const ObLogCursor& cursor)
    {
      int err = OB_SUCCESS;
      int sys_err = 0;
      char fname[OB_MAX_FILE_NAME_LENGTH];
      int64_t fname_len = 0;
      int fd = -1;
      char* buf = NULL;
      int64_t len = ObLogGenerator::LOG_FILE_ALIGN_SIZE;
      int64_t read_count = 0;
      if (0 != (sys_err = posix_memalign((void**)&buf, ObLogGenerator::LOG_FILE_ALIGN_SIZE, len)))
      {
        err = OB_ALLOCATE_MEMORY_FAILED;
        TBSYS_LOG(ERROR, "posix_memalign(align=%ld, size=%ld): %s",
                  ObLogGenerator::LOG_FILE_ALIGN_SIZE, len, strerror(sys_err));
      }
      else if ((fname_len = snprintf(fname, sizeof(fname), "%s/%ld", log_dir_, cursor.file_id_)) < 0
               || fname_len >= (int64_t)sizeof(fname))
      {
        err = OB_BUF_NOT_ENOUGH;
        TBSYS_LOG(ERROR, "gen fname fail: buf_len=%ld, fname=%s/%ld", sizeof(fname), log_dir_, cursor.file_id_);
      }
      else if ((fd = open(fname, O_RDONLY | O_DIRECT | O_CREAT, OPEN_MODE)) < 0)
      {
        err = OB_IO_ERROR;
        TBSYS_LOG(ERROR, "open %s fail: %s", fname, strerror(errno));
      }
      else if (::get_file_size(fd) == cursor.offset_)
      {
        TBSYS_LOG(WARN, "no eof mark after log_cursor[%s], but this is the end of file, maybe replay old version log_file", to_cstring(cursor));
      }
      else if (0 > (read_count = unintr_pread(fd, buf, len, cursor.offset_)))
      {
        err = OB_IO_ERROR;
        TBSYS_LOG(ERROR, "pread(file=%s, fd=%d, buf=%p[%ld], cursor.offset=%ld)=>%s", fname, fd, buf, len, cursor.offset_, strerror(errno));
      }
      else if (!ObLogGenerator::is_eof(buf, read_count))
      {
        err = OB_LAST_LOG_RUINNED;
        TBSYS_LOG(ERROR, "%s not follow by eof, read_count=%ld", to_cstring(cursor), read_count);
      }

      if (fd >= 0)
      {
        close(fd);
      }
      if (NULL != buf)
      {
        free(buf);
      }
      return err;
    }

    int ObLogDataWriter::start_log(const ObLogCursor& cursor)
    {
      int err = OB_SUCCESS;
      if (!cursor.is_valid())
      {
        err = OB_INVALID_ARGUMENT;
        TBSYS_LOG(ERROR, "cursor[%s].is_invalid", to_cstring(cursor));
      }
	   //del wangdonghui [Paxos ups_replication_tmplog] 20170315 :b
      /*
      else if (end_cursor_.is_valid())
      {
        err = OB_INIT_TWICE;
        TBSYS_LOG(ERROR, "ObLogWriter is init already init, cursor_=%s", to_cstring(end_cursor_));
      }
      */
	  //del
      else if (0 != (cursor.offset_ & ObLogGenerator::LOG_FILE_ALIGN_MASK))
      {
        err = OB_LOG_NOT_ALIGN;
        TBSYS_LOG(WARN, "start_log(%s): LOG_NOT_ALIGNED", to_cstring(cursor));
      }
      //mod wangjiahao [Paxos ups_replication_tmplog] 20150715 :b  TODO_SHOULDNT_BE_MODIFYED_NEED_TO_RESTORE
      /*
      else if (OB_SUCCESS != (err = check_eof_after_log_cursor(cursor)))
      {
        TBSYS_LOG(ERROR, "no eof after %s, maybe commitlog corrupt, err=%d", to_cstring(cursor), err);
        err = OB_START_LOG_CURSOR_INVALID;
      }
      */
      //mod :e
      else
      {
        end_cursor_ = cursor;
      }
      return err;
    }

    int ObLogDataWriter::reset()
    {
      int err = OB_SUCCESS;
      end_cursor_.reset();
      return err;
    }

    int ObLogDataWriter::get_cursor(ObLogCursor& cursor) const
    {
      int err = OB_SUCCESS;
      cursor = end_cursor_;
      return err;
    }
  }; // end namespace common
}; // end namespace oceanbase
