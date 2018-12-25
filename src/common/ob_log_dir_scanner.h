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
 *   yanran <yanran.hfs@taobao.com>
 *     - some work details if you want
 */

#ifndef OCEANBASE_COMMON_OB_LOG_DIR_SCANNER_H_
#define OCEANBASE_COMMON_OB_LOG_DIR_SCANNER_H_

#include "ob_vector.h"

namespace oceanbase
{
  namespace common
  {
    struct ObLogFile
    {
      char name[OB_MAX_FILE_NAME_LENGTH];
      uint64_t id;

      enum FileType
      {
        UNKNOWN = 1,
        LOG = 2,
        CKPT = 3
      };

      int assign(const char* filename, FileType &type);
      bool isLogId(const char* str) const ;
      uint64_t strToUint64(const char* str) const;
      bool operator< (const ObLogFile& r) const;
    };

    template <>
    struct ob_vector_traits<ObLogFile>
    {
    public:
      typedef ObLogFile& pointee_type;
      typedef ObLogFile value_type;
      typedef const ObLogFile const_value_type;
      typedef value_type* iterator;
      typedef const value_type* const_iterator;
      typedef int32_t difference_type;
    };

    class ObLogDirScanner
    {
    public:
      ObLogDirScanner();
      virtual ~ObLogDirScanner();

      /**
       * 初始化传入日志目录, ObLogScanner类会扫描整个目录
       * 对日志文件按照id进行排序, 得到最大日志号和最小日志号
       * 同时获得最大的checkpoint号
       *
       * !!! 扫描过程中有一种异常情况, 即日志目录内的文件不连续
       * 此时, 最小的连续日志被认为是有效日志
       * 出现这种情况时, init返回值是OB_DISCONTINUOUS_LOG
       *
       * @param [in] log_dir 日志目录
       * @return OB_SUCCESS 成功
       * @return OB_DISCONTINUOUS_LOG 扫描成功, 但是日志文件不连续
       * @return otherwise 失败
       */
      int init(const char* log_dir);

      /// @brief get minimum commit log id
      /// @retval OB_SUCCESS
      /// @retval OB_ENTRY_NOT_EXIST no commit log in directory
      int get_min_log_id(uint64_t &log_id) const;

      /// @brief get maximum commit log id
      /// @retval OB_SUCCESS
      /// @retval OB_ENTRY_NOT_EXIST no commit log in directory
      int get_max_log_id(uint64_t &log_id) const;

      /// @brief get maximum checkpoint id
      /// @retval OB_SUCCESS
      /// @retval OB_ENTRY_NOT_EXIST no commit log in directory
      int get_max_ckpt_id(uint64_t &ckpt_id) const;

      bool has_log() const;
      bool has_ckpt() const;

    private:
      /**
       * 遍历日志目录下的所有文件, 找到所有日志文件
       */
      int search_log_dir_(const char* log_dir);

      /**
       * 检查日志文件是否连续
       * 同时获取最小和最大日志文件号
       */
      int check_continuity_(const ObVector<ObLogFile> &files, uint64_t &min_file_id, uint64_t &max_file_id);

      inline int check_inner_stat() const
      {
        int ret = OB_SUCCESS;
        if (!is_initialized_)
        {
          TBSYS_LOG(ERROR, "ObLogDirScanner has not been initialized.");
          ret = OB_NOT_INIT;
        }
        return ret;
      }

    private:
      uint64_t min_log_id_;
      uint64_t max_log_id_;
      uint64_t max_ckpt_id_;
      bool has_log_;
      bool has_ckpt_;
      bool is_initialized_;
    };
  } // end namespace common
} // end namespace oceanbase

#endif // OCEANBASE_COMMON_OB_LOG_DIR_SCANNER_H_
