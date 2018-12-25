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

#include "ob_located_log_reader.h"
#include "common/ob_file.h"
#include "ob_ups_log_utils.h"

using namespace oceanbase::common;

namespace oceanbase
{
  namespace updateserver
  {
    int read_log_file_by_location(const char* log_dir, const int64_t file_id, const int64_t offset,
                         char* buf, const int64_t len, int64_t& read_count, const bool dio = true)
    {
      int err = OB_SUCCESS;
      char path[OB_MAX_FILE_NAME_LENGTH];
      int64_t path_len = 0;
      ObString fname;
      ObFileReader file_reader;
      read_count = 0;
      if (NULL == log_dir || 0 >= file_id || 0 > offset || NULL == buf || 0 >= len)
      {
        err = OB_INVALID_ARGUMENT;
      }
      else if (0 >= (path_len = snprintf(path, sizeof(path), "%s/%ld", log_dir, file_id))
               || path_len > (int64_t)sizeof(path))
      {
        err = OB_ERROR;
      }
      else
      {
        fname.assign_ptr(path, static_cast<int32_t>(path_len));
      }

      if (OB_SUCCESS != err)
      {}
      else if (OB_SUCCESS != (err = file_reader.open(fname, dio)))
      {
        if (OB_FILE_NOT_EXIST != err)
        {
          TBSYS_LOG(WARN, "file_reader.open(%s/%ld)=>%d", path, file_id, err);
        }
        else
        {
          err = OB_SUCCESS;
        }
      }
      else if (OB_SUCCESS != (err = file_reader.pread(buf, len, offset, read_count)))
      {
        TBSYS_LOG(ERROR, "file_reader.pread(buf=%p[%ld], offset=%ld)=>%d", buf, len, offset, err);
      }

      if (file_reader.is_opened())
      {
        file_reader.close();
      }
      return err;
    }

    bool ObLocatedLogReader::is_inited() const
    {
      return NULL != log_dir_;
    }

    int ObLocatedLogReader::init(const char* log_dir, const bool dio /*=true*/)
    {
      int err = OB_SUCCESS;
      if (is_inited())
      {
        err = OB_INIT_TWICE;
      }
      else if (NULL == log_dir)
      {
        err = OB_INVALID_ARGUMENT;
      }
      else
      {
        log_dir_ = log_dir;
        dio_ = dio;
      }
      return err;
    }

    int ObLocatedLogReader::read_log(const int64_t file_id, const int64_t offset,
                                     int64_t& start_id, int64_t& end_id,
                                     char* buf, const int64_t len, int64_t& read_count, bool& is_file_end)
    {
      int err = OB_SUCCESS;
      if (!is_inited())
      {
        err = OB_NOT_INIT;
      }
      else if (OB_SUCCESS != (err = read_log_file_by_location(log_dir_, file_id, offset, buf, len, read_count, dio_)))
      {
        TBSYS_LOG(ERROR, "read_log_file_by_location(%s/%ld, offset=%ld)=>%d", log_dir_, file_id, offset, err);
      }
      else if (OB_SUCCESS != (err = trim_log_buffer(offset, OB_DIRECT_IO_ALIGN_BITS,
                                                    buf, read_count, read_count, start_id, end_id, is_file_end)))
      {
        TBSYS_LOG(ERROR, "trim_log_buffer()=>%d", err);
      }
      return err;
    }
  }; // end namespace updateserver
}; // end namespace oceanbase
