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
#ifndef OCEANBASE_UPDATESERVER_OB_LOCATED_LOG_READER_H_
#define OCEANBASE_UPDATESERVER_OB_LOCATED_LOG_READER_H_
#include "common/ob_define.h"
using namespace oceanbase::common;
namespace oceanbase
{
  namespace updateserver
  {
    class IObLocatedLogReader
    {
      public:
        IObLocatedLogReader(){}
        virtual ~IObLocatedLogReader(){}
        virtual int read_log(const int64_t file_id, const int64_t offset,
                            int64_t& start_id, int64_t& end_id,
                             char* buf, const int64_t len, int64_t& read_count, bool& is_file_end) = 0;
    };

// 实现为每次重新打开指定的文件，定位到给定的偏移，读取日志，读取的日志拷贝到给定的缓冲区内.
//  读取的日志不超过调用者给定的缓冲区长度，也不能越过文件末尾跳到下一个文件。
//  读取文件成功后，接着解析一遍读到的日志，去掉缓冲区最后可能剩余的半个日志，并同时得到开始和结束的日志ID。
//  start_id 在调用时作为常量给出，返回之前会检查实际读到的第一条日志ID是否真的等于 start_id 。

// 返回的日志是由 [start_id,end_id) 组成的左闭右开的区间。
// 读取不到日志(到了文件末尾)不认为是错误，这时 read_count=0, end_id=start_id;
// 如果遇到switch_log, is_file_end被设置为true;
// 调用者一般每次都会请求读取约2M的日志, 所以每次读取磁盘应该也不会有性能问题。
    class ObLocatedLogReader : public IObLocatedLogReader
    {
      public:
        ObLocatedLogReader() : log_dir_(NULL), dio_(true) {}
        virtual ~ObLocatedLogReader(){}
        int init(const char* log_dir, const bool dio = true);
        virtual int read_log(const int64_t file_id, const int64_t offset,
                            int64_t& start_id, int64_t& end_id,
                             char* buf, const int64_t len, int64_t& read_count, bool& is_file_end);
      protected:
        bool is_inited() const;
      private:
        const char* log_dir_;
        bool dio_; // 是否使用DirectIO
    };
  }; // end namespace updateserver
}; // end namespace oceanbase
#endif // OCEANBASE_UPDATESERVER_OB_LOCATED_LOG_READER_H_
