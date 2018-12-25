////===================================================================
 //
 // liboblog.h liboblog / Oceanbase
 //
 // Copyright (C) 2013 Alipay.com, Inc.
 //
 // Created on 2013-05-23 by Yubai (yubai.lk@alipay.com) 
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

#ifndef  OCEANBASE_LIBOBLOG_H_
#define  OCEANBASE_LIBOBLOG_H_

#ifndef __STDC_LIMIT_MACROS
#define __STDC_LIMIT_MACROS
#endif

#ifndef __STDC_CONSTANT_MACROS
#define __STDC_CONSTANT_MACROS
#endif

#include <stdint.h>
#include <vector>
#include <map>
#include "ob_define.h"
#include "MD.h"
#include "BR.h"

namespace oceanbase
{
  namespace liboblog
  {
    struct ObLogError
    {
      enum ErrLevel
      {
        ERR_WARN = 0,
        ERR_ABORT,
      } level_;             ///< error level
      int errno_;           ///< error number
      const char *errmsg_;  ///< error message
    };

    /// 日志位置类型
    enum ObLogPositionType
    {
      POS_TYPE_UNKNOWN = 0,
      POS_TYPE_CHECKPOINT = 1,         // 位点
      POS_TYPE_TIMESTAMP_SECOND = 2,   // 时间戳 单位：秒
      POS_TYPE_NUMBER
    };

    /// 特殊的日志位置值
    enum ObLogSpecificPositionValue
    {
      POS_VALUE_MAX_SERVED = 0,   // 最大可服务的日志位置
      POS_VALUE_MIN_SERVED = 1,   // 最小可服务的日志位置
    };

    typedef void (* ERROR_CALLBACK) (const ObLogError &err);

    class IObLogSpec
    {
      public:
        virtual ~IObLogSpec() {};
      public:
        /*
         * 初始化
         * @param config            配置文件名
         * @param start_position    日志启动位置，值可以为: ObLogSpecificPositionValue
         * @param err_cb            错误回调函数指针
         * @param pos_type          日志启动位置类型 (默认是位点)
         */
        virtual int init(const char *config,
            const uint64_t start_position,
            ERROR_CALLBACK err_cb = NULL,
            ObLogPositionType pos_type = POS_TYPE_CHECKPOINT) = 0;

        /*
         * 初始化
         * @param configs         配置项map结构
         * @param start_position  日志启动位置，值可以为: ObLogSpecificPositionValue
         * @param err_cb          错误回调函数指针
         * @param pos_type        日志启动位置类型 (默认是位点)
         */
        virtual int init(const std::map<std::string, std::string>& configs,
            const uint64_t start_position,
            ERROR_CALLBACK err_cb = NULL,
            ObLogPositionType pos_type = POS_TYPE_CHECKPOINT) = 0;

        virtual void destroy() = 0;

        /*
         * Launch liboblog
         * @retval OB_SUCCESS on success
         * @retval ! OB_SUCCESS on fail
         */
        virtual int launch() = 0;

        /*
         * Stop liboblog
         */
        virtual void stop() = 0;
    };

    class IObLog;
    class ObLogSpecFactory
    {
      public:
        ObLogSpecFactory();
        ~ObLogSpecFactory();
      public:
        IObLogSpec *construct_log_spec();
        IObLog *construct_log();
        void deconstruct(IObLogSpec *log_spec);
        void deconstruct(IObLog *log);
    };

    class IObLog
    {
      public:
        virtual ~IObLog() {};

      public:
        /*
         * 初始化
         * @param log_spec    全局使用一个log_spec
         * @param partition   抓取指定分区的数据
         * @return 0:成功
         */
        virtual int init(IObLogSpec *log_spec, const std::vector<uint64_t> &partitions) = 0;

        /*
         * 销毁
         */
        virtual void destroy() = 0;

        /*
         * 迭代一个数据库statement
         * @param record      迭代record，地址由内部分配，允许多次调用record之后再调用release
         * @param 0:成功
         */
        virtual int next_record(IBinlogRecord **record, const int64_t timeout_us) = 0;

        /*
         * 释放record
         * @param record
         */
        virtual void release_record(IBinlogRecord *record) = 0;
    };
  }
}

#endif //OCEANBASE_LIBOBLOG_H_

