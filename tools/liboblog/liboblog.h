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

    /// ��־λ������
    enum ObLogPositionType
    {
      POS_TYPE_UNKNOWN = 0,
      POS_TYPE_CHECKPOINT = 1,         // λ��
      POS_TYPE_TIMESTAMP_SECOND = 2,   // ʱ��� ��λ����
      POS_TYPE_NUMBER
    };

    /// �������־λ��ֵ
    enum ObLogSpecificPositionValue
    {
      POS_VALUE_MAX_SERVED = 0,   // ���ɷ������־λ��
      POS_VALUE_MIN_SERVED = 1,   // ��С�ɷ������־λ��
    };

    typedef void (* ERROR_CALLBACK) (const ObLogError &err);

    class IObLogSpec
    {
      public:
        virtual ~IObLogSpec() {};
      public:
        /*
         * ��ʼ��
         * @param config            �����ļ���
         * @param start_position    ��־����λ�ã�ֵ����Ϊ: ObLogSpecificPositionValue
         * @param err_cb            ����ص�����ָ��
         * @param pos_type          ��־����λ������ (Ĭ����λ��)
         */
        virtual int init(const char *config,
            const uint64_t start_position,
            ERROR_CALLBACK err_cb = NULL,
            ObLogPositionType pos_type = POS_TYPE_CHECKPOINT) = 0;

        /*
         * ��ʼ��
         * @param configs         ������map�ṹ
         * @param start_position  ��־����λ�ã�ֵ����Ϊ: ObLogSpecificPositionValue
         * @param err_cb          ����ص�����ָ��
         * @param pos_type        ��־����λ������ (Ĭ����λ��)
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
         * ��ʼ��
         * @param log_spec    ȫ��ʹ��һ��log_spec
         * @param partition   ץȡָ������������
         * @return 0:�ɹ�
         */
        virtual int init(IObLogSpec *log_spec, const std::vector<uint64_t> &partitions) = 0;

        /*
         * ����
         */
        virtual void destroy() = 0;

        /*
         * ����һ�����ݿ�statement
         * @param record      ����record����ַ���ڲ����䣬�����ε���record֮���ٵ���release
         * @param 0:�ɹ�
         */
        virtual int next_record(IBinlogRecord **record, const int64_t timeout_us) = 0;

        /*
         * �ͷ�record
         * @param record
         */
        virtual void release_record(IBinlogRecord *record) = 0;
    };
  }
}

#endif //OCEANBASE_LIBOBLOG_H_

