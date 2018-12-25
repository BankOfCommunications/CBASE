
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

#ifndef OCEANBASE_COMMON_OB_REPEATED_LOG_READER_H_
#define OCEANBASE_COMMON_OB_REPEATED_LOG_READER_H_

#include "ob_single_log_reader.h"

namespace oceanbase
{
  namespace common
  {
    class ObRepeatedLogReader : public ObSingleLogReader
    {
    public:
      ObRepeatedLogReader();
      virtual ~ObRepeatedLogReader();

      /**
       * @brief 从操作日志中读取一个更新操作
       * @param [out] cmd 日志类型
       * @param [out] log_seq 日志序号
       * @param [out] log_data 日志内容
       * @param [out] data_len 缓冲区长度
       * @return OB_SUCCESS: 如果成功;
       *         OB_READ_NOTHING: 从文件中没有读到数据
       *         others: 发生了错误.
       */
      int read_log(LogCommand &cmd, uint64_t &log_seq, char *&log_data, int64_t &data_len);
    };
  } // end namespace common
} // end namespace oceanbase

#endif // OCEANBASE_COMMON_OB_REPEATED_LOG_READER_H_
