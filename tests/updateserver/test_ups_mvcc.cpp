/**
 * (C) 2013-2013 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 *
 * Authors:
 *   yanran.hfs <yanran.hfs@alipya.com>
 *
 */

#include "gtest/gtest.h"
#include "common/ob_malloc.h"
#include "common/ob_schema.h"
#include "common/ob_new_scanner_helper.h"
#include "updateserver/ob_ups_table_mgr.h"
#include "updateserver/ob_update_server_main.h"
//#include "test_utils.h"
#include <execinfo.h>

using namespace oceanbase::common;
using namespace oceanbase::updateserver;
using namespace oceanbase::sql;

//error code
const int ERROR_CODE_TRY_FAILED = 100;

namespace oceanbase
{
  namespace test
  {
    class CONST
    {
      public:
        static const uint64_t  CheckInfoTID              = 9001;
        static const uint64_t  CheckDataTID              = 9002;
        static const uint64_t  InfoRowkeyCID             = 16;
        static const uint64_t  RowStatCID                = 17;
        static const uint64_t  WriteThreadIDCID          = 18;
        static const uint64_t  CheckDataValueCID         = 19;
        static const uint64_t  DataRowkeyCID             = 16;
        static const uint64_t  SelfCheck1CID             = 17;
        static const uint64_t  SelfCheck2CID             = 18;
        static const uint64_t  MultiCheckBaseCID         = 19;
        static const uint64_t  MAX_CID                   = 100;
        static const ObObjType InfoRowkeyCTYPE           = ObIntType;
        static const ObObjType RowStatCTYPE              = ObIntType;
        static const ObObjType WriteThreadIDCTYPE        = ObIntType;
        static const ObObjType CheckDataValueCTYPE       = ObVarcharType;
        static const ObObjType DataRowkeyCTYPE           = ObIntType;
        static const ObObjType SelfCheck1CTYPE           = ObIntType;
        static const ObObjType SelfCheck2CTYPE           = ObIntType;
        static const ObObjType MultiCheckBaseCTYPE       = ObIntType;
        static const char *    CheckInfoTNAME;
        static const char *    CheckDataTNAME;
        static const char *    InfoRowkeyCNAME;
        static const char *    RowStatCNAME;
        static const char *    WriteThreadIDCNAME;
        static const char *    CheckDataValueCNAME;
        static const char *    DataRowkeyCNAME;
        static const char *    SelfCheck1CNAME;
        static const char *    SelfCheck2CNAME;
        static const char *    MultiCheckBaseCNAME;

        //static const int64_t   InfoMaxRowkeyBase         = 10000;
        //static const int64_t   DataMaxRowkeyBase         = 100000;
        static const int64_t   InfoMaxRowkeyBase         = 100;
        static const int64_t   DataMaxRowkeyBase         = 1000;
        static const int64_t   DataLineNumMax            = 20;
        static const int64_t   InfoMaxRowkeyMultiplier   = 100000;
        static const int64_t   DataMaxRowkeyMultiplier   = 100000;
        static volatile int64_t InfoRowkeyMultiplier;
        static volatile int64_t DataRowkeyMultiplier;

        static       int64_t   CoreNum;
        static       int64_t   MaxLineNum;
        static       int64_t   RunningTime;
        static const int64_t   QueueSize                 = 1L << 20;
        static const int64_t   TransactionTimeout        = 1000000;
        static const int64_t   TransactionIdleTime       = 500000;
        static const int64_t   MinorVersionLimit         = 100;
        static const int64_t   CommitPercentage          = 100;
        static const int64_t   PointMultiplier           = 10;


        static int64_t GetUniqTime()
        {
          static int64_t LastTime = 0;
          while (true)
          {
            int64_t newv = tbsys::CTimeUtil::getTime();
            int64_t oldv = LastTime;
            if (newv > oldv)
            {
              if (oldv == __sync_val_compare_and_swap(&LastTime, oldv, newv))
              {
                return newv;
              }
            }
            asm("pause");
          }
        }
        static int64_t GetCoreNum()
        {
          if (0 != CoreNum)
          {
            return CoreNum;
          }
          else
          {
            static int64_t siCoreNum = sysconf(_SC_NPROCESSORS_ONLN);
            return siCoreNum;
          }
        }
        static int64_t GetApplierNum()
        {
          return GetCoreNum() > 1 ? GetCoreNum() / 2 : 1;
        }
        static int64_t GetCheckerNum()
        {
          return GetCoreNum() - GetApplierNum();
        }
        static int64_t GetInfoMaxRowkey()
        {
          return InfoMaxRowkeyBase * GetApplierNum();
        }
        static int64_t GetDataMaxRowkey()
        {
          return DataMaxRowkeyBase * GetApplierNum();
        }
        static int64_t GetInfoRowkeyMultiplier()
        {
          return InfoRowkeyMultiplier;
        }
        static int64_t GetDataRowkeyMultiplier()
        {
          return DataRowkeyMultiplier;
        }
        static int64_t GetRunningTime()
        {
          return RunningTime;
        }
        static int64_t getMaxLineNum()
        {
          return MaxLineNum;
        }

        static int InitSchema(ObSchemaManagerV2 & schema)
        {
          int ret = OB_SUCCESS;
#define DEFINE_TABLE_SCHEMA(TableName, Rowkey) \
          ObTableSchema TableName##Schema;\
          TableName##Schema.set_table_id(TableName##TID);\
          TableName##Schema.set_max_column_id(MAX_CID);\
          TableName##Schema.set_table_name(TableName##TNAME);\
          ObRowkeyColumn Rowkey##Column;\
          Rowkey##Column.length_ = 8;\
          Rowkey##Column.column_id_ = Rowkey##CID;\
          Rowkey##Column.type_ = ObIntType;\
          ObRowkeyInfo Rowkey##Info;\
          if (OB_SUCCESS == ret \
              && OB_SUCCESS != (ret = Rowkey##Info.add_column(Rowkey##Column))) {\
          } else {\
            TableName##Schema.set_rowkey_info(Rowkey##Info);\
            if (OB_SUCCESS == ret \
              && OB_SUCCESS != (ret = schema.add_table(TableName##Schema))) {\
              TBSYS_LOG(ERROR, "SchemaMgr add_table " #TableName " error");\
            } else {\
              TBSYS_LOG(INFO, "Schema New Table: " #TableName "(ID=%ld)",\
                  TableName##TID);\
            }\
          }
#define DEFILE_COLUMN_SCHEMA(TableName, ColumnName) \
          ObColumnSchemaV2 ColumnName##Schema;\
          ColumnName##Schema.set_table_id(TableName##TID);\
          ColumnName##Schema.set_column_id(ColumnName##CID);\
          ColumnName##Schema.set_column_name(ColumnName##CNAME);\
          ColumnName##Schema.set_column_type(ColumnName##CTYPE);\
          if (OB_SUCCESS == ret \
              && OB_SUCCESS != (ret = schema.add_column(ColumnName##Schema))) {\
            TBSYS_LOG(ERROR, "SchemaMgr add_column " #TableName "." #ColumnName " error");\
          } else {\
            TBSYS_LOG(INFO, "Schema New Column: " #TableName "." #ColumnName \
                "(ID=%ld)", ColumnName##CID);\
          }
#define DEFILE_COLUMN_SCHEMA_V(TableName, ColumnNameV, ColumnIDV, ColumnTypeV) \
          ObColumnSchemaV2 ColumnNameV##Schema;\
          ColumnNameV##Schema.set_table_id(TableName##TID);\
          ColumnNameV##Schema.set_column_id(ColumnIDV);\
          ColumnNameV##Schema.set_column_name(ColumnNameV);\
          ColumnNameV##Schema.set_column_type(ColumnTypeV);\
          if (OB_SUCCESS == ret \
              && OB_SUCCESS != (ret = schema.add_column(ColumnNameV##Schema))) {\
            TBSYS_LOG(ERROR, "SchemaMgr add_column " #TableName ".%s error", ColumnNameV);\
          } else {\
            TBSYS_LOG(INFO, "Schema New Column: " #TableName ".%s(ID=%ld)", \
                ColumnNameV, ColumnIDV);\
          }

          DEFINE_TABLE_SCHEMA(CheckInfo, InfoRowkey)
          DEFINE_TABLE_SCHEMA(CheckData, DataRowkey)

          DEFILE_COLUMN_SCHEMA(CheckInfo, InfoRowkey)
          DEFILE_COLUMN_SCHEMA(CheckInfo, RowStat)
          DEFILE_COLUMN_SCHEMA(CheckInfo, WriteThreadID)
          DEFILE_COLUMN_SCHEMA(CheckInfo, CheckDataValue)
          DEFILE_COLUMN_SCHEMA(CheckData, DataRowkey)
          DEFILE_COLUMN_SCHEMA(CheckData, SelfCheck1)
          DEFILE_COLUMN_SCHEMA(CheckData, SelfCheck2)

          int64_t ApplierNum = GetApplierNum();
          for (int64_t i = 0; i < ApplierNum; i++)
          {
            char column_name[BUFSIZ];
            snprintf(column_name, BUFSIZ, "%s%ld", MultiCheckBaseCNAME, i);
            uint64_t column_id = MultiCheckBaseCID + i;
            ObObjType column_type = ObIntType;
            DEFILE_COLUMN_SCHEMA_V(CheckData, column_name, column_id, column_type)
          }

          return ret;
        }
    };

    const char * CONST::CheckInfoTNAME             = "CheckInfo";
    const char * CONST::CheckDataTNAME             = "CheckData";
    const char * CONST::InfoRowkeyCNAME            = "Rowkey";
    const char * CONST::RowStatCNAME               = "RowStat";
    const char * CONST::WriteThreadIDCNAME         = "WriteThreadID";
    const char * CONST::CheckDataValueCNAME        = "CheckDataValue";
    const char * CONST::DataRowkeyCNAME            = "Rowkey";
    const char * CONST::SelfCheck1CNAME            = "SelfCheck1";
    const char * CONST::SelfCheck2CNAME            = "SelfCheck2";
    const char * CONST::MultiCheckBaseCNAME        = "MultiCheck";

    int64_t      CONST::CoreNum                    = 0;
    int64_t      CONST::MaxLineNum                 = 100000000;
    int64_t      CONST::RunningTime                = 2;
    volatile int64_t CONST::InfoRowkeyMultiplier   = 0;
    volatile int64_t CONST::DataRowkeyMultiplier   = 0;

    class ErrMsgException
    {
      public:
        ErrMsgException(const char * pszFile, int iLine, const char * pszFunction,
                        pthread_t iTID, int iErrCode, const char* pszMsg, ...)
          : pszFile_(pszFile), iLine_(iLine), pszFunction_(pszFunction),
            iTID_(iTID), iErrCode_(iErrCode)
        {
          gettimeofday(&tTime_, NULL);
          va_list arg;
          va_start(arg, pszMsg);
          vsnprintf(pszMsg_, BUFSIZ, pszMsg, arg);
          va_end(arg);
        }
        ErrMsgException(const ErrMsgException & e)
          : tTime_(e.tTime_), pszFile_(e.pszFile_), iLine_(e.iLine_),
            pszFunction_(e.pszFunction_), iTID_(e.iTID_), iErrCode_(e.iErrCode_)
        {
          strcpy(pszMsg_, e.pszMsg_);
        }
        ~ErrMsgException()
        {
        }
        const char * what() const
        {
          return pszMsg_;
        }
        void logException() const
        {
          struct tm tm;
          localtime_r((const time_t*)&tTime_.tv_sec, &tm);
          TBSYS_LOG(ERROR, "Exception: %04d%02d%02d%02d%02d%02d%06ld "
              "%ld %s:%d %s ErrCode: %d ErrMsg: %s",
              tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday,
              tm.tm_hour, tm.tm_min, tm.tm_sec, tTime_.tv_usec,
              iTID_, pszFile_, iLine_, pszFunction_,
              iErrCode_, pszMsg_);
        }
        int getErrorCode() const
        {
          return iErrCode_;
        }

      private:
        struct timeval       tTime_;
        const char *         pszFile_;
        int                  iLine_;
        const char *         pszFunction_;
        pthread_t            iTID_;
        int                  iErrCode_;
        char                 pszMsg_[BUFSIZ];
    };
#define THROW_EXCEPTION(iErrCode, pszMsg, ...) \
    throw ErrMsgException(__FILE__, __LINE__, __FUNCTION__, pthread_self(), \
                          iErrCode, pszMsg, ##__VA_ARGS__)
#define THROW_EXCEPTION_AND_LOG(iErrCode, pszMsg, ...) \
    TBSYS_LOG(ERROR, "Throw Exception: ErrCode: %d ErrMsg: " pszMsg, \
        iErrCode, ##__VA_ARGS__); \
    throw ErrMsgException(__FILE__, __LINE__, __FUNCTION__, pthread_self(), \
                          iErrCode, pszMsg, ##__VA_ARGS__)
#define RETHROW(ex) \
    TBSYS_LOG(ERROR, "Rethrow Exception: ErrCode: %d ErrMsg: %s", \
        ex.getErrorCode(), ex.what()); \
    throw

    class DummyLogWriter : public ObILogWriter
    {
      public:
        virtual int switch_log_file(uint64_t &new_log_file_id)
        {
          UNUSED(new_log_file_id);
          return 0;
        }
        virtual int write_replay_point(uint64_t replay_point)
        {
          UNUSED(replay_point);
          return 0;
        }
    };

    class Random
    {
      public:
        uint64_t rand()
        {
          return seed_ = ((seed_ * 214013L + 2531011L) >> 16);
        }
        uint64_t rand(uint64_t max)
        {
          return rand() % max;
        }
        void     srand(uint64_t seed)
        {
          seed_ = seed;
        }
      private:
        //thread_store<uint64_t> seed_;
        uint64_t seed_;
    };

    template <class T>
    class AutoArray
    {
      private:
        T* p_;

      public:
        explicit AutoArray(T* p = 0) throw() : p_(p) { }

        //AutoString(AutoString& __a) throw() : _M_ptr(__a.release()) { }
        AutoArray(const AutoArray& a) throw() : p_(a.p_)
        {
          const_cast<AutoArray&>(a).p_ = 0;
        }

        ~AutoArray();

        T* operator->() const throw()
        {
          _GLIBCXX_DEBUG_ASSERT(p_ != 0);
          return p_;
        }

        const T operator[](int64_t index) const throw()
        {
          _GLIBCXX_DEBUG_ASSERT(p_ != 0);
          return p_[index];
        }

        T & operator[](int64_t index) throw()
        {
          _GLIBCXX_DEBUG_ASSERT(p_ != 0);
          return p_[index];
        }

        T & operator*() throw()
        {
          _GLIBCXX_DEBUG_ASSERT(p_ != 0);
          return *p_;
        }

        T* get() const throw() { return p_; }

        T* release() throw()
        {
          T* tmp = p_;
          p_ = 0;
          return tmp;
        }

        void reset(T* p = 0) throw()
        {
          if (p != p_)
          {
            this->~AutoArray();
            p_ = p;
          }
        }
    };
    typedef AutoArray<char> AutoString;

    template<class A = FIFOAllocator>
    class CopyA
    {
      public:
        CopyA()
          : oA_(oDefaultA_)
        {
          const int64_t TOTAL_LIMIT = 1L << 30;
          const int64_t HOLD_LIMIT  = 100L << 20;
          const int64_t PAGE_SIZE   = 4L << 20;
          int err = oA_.init(TOTAL_LIMIT, HOLD_LIMIT, PAGE_SIZE);
          if (OB_SUCCESS != err)
          {
            TBSYS_LOG(ERROR, "FIFOAllocator init error");
          }
        }
        CopyA(A & oA)
          : oA_(oA)
        {
        }
        A &                  getA()
        {
          return oA_;
        }
        AutoString           copyCstr(ObString & str)
        {
          char * sNew = new (oA_.alloc(str.length() + 1)) char[str.length() + 1];
          memcpy(sNew, str.ptr(), str.length());
          sNew[str.length()] = '\0';
          return AutoString(sNew);
        }
        AutoString           copyCstr(char * str)
        {
          return copyCstr(str, static_cast<int32_t>(strlen(str) + 1));
        }
        AutoString           copyCstr(char * str, int len)
        {
          char * sNew = new (oA_.alloc(len)) char[len];
          memcpy(sNew, str, len);
          return AutoString(sNew);
        }
        template<class T>
        AutoArray<T>         allocArray(int64_t iSize)
        {
          T * ptP = new (oA_.alloc(sizeof(T) * iSize)) T[iSize];
          return AutoArray<T>(ptP);
        }
      protected:
        A & oA_;
        FIFOAllocator oDefaultA_;
    };
    typedef CopyA<> Copy;

    class Singleton
    {
      public:
        static Singleton &  getInstance()
        {
          static Singleton soInstance;
          return soInstance;
        }
        ObFixedQueue<int64_t> & getQueue()
        {
          return oQueue_;
        }
        DummyLogWriter &    getLogWriter()
        {
          return oLogWriter_;
        }
        SessionCtxFactory & getCtxFactory()
        {
          return oCtxFactory_;
        }
        SessionMgr &        getSessionMgr()
        {
          return oSessionMgr_;
        }
        LockMgr &           getLockMgr()
        {
          return oLockMgr_;
        }
        ObUpsTableMgr &     getStorage()
        {
          return oStorage_;
        }
        Copy &              getCopy()
        {
          return oCopy_;
        }
        pthread_spinlock_t & getCommitMutex()
        {
          return lCommitMutex_;
        }
        Random &            getRandom()
        {
          return oRand_;
        }
        int                 endSession(SessionMgr & oSessionMgr,
                                       BaseSessionCtx * poBaseSessionCtx,
                                       uint32_t uDescriptor,
                                       bool bIsRollBack)
        {
          //int64_t tid = poBaseSessionCtx->get_trans_id();
          int ret = OB_SUCCESS;
          int64_t iCurTimestamp = tbsys::CTimeUtil::getTime();
          pthread_spin_lock(&lCommitMutex_);
          iCurTimestamp = (iCurTimestamp <= iLastTimestamp_) ?
                  (iLastTimestamp_ + 1) : iCurTimestamp;
          iCurTimestamp = (iCurTimestamp <= oSessionMgr.get_commited_trans_id()) ?
                  (oSessionMgr.get_commited_trans_id() + 1) : iCurTimestamp;
          iLastTimestamp_ = iCurTimestamp;
          poBaseSessionCtx->set_trans_id(iCurTimestamp);
          oSessionMgr.revert_ctx(uDescriptor);
          ret = oSessionMgr.end_session(uDescriptor, bIsRollBack);
          pthread_spin_unlock(&lCommitMutex_);
          if (OB_SUCCESS != ret)
          {
            TBSYS_LOG(ERROR, "end_session error");
          }
          else
          {
            //TBSYS_LOG(INFO, "[END] trans_id=%ld", tid);
          }
          return ret;
        }
      protected:
        Singleton()
          : oStorage_(oLogWriter_), iLastTimestamp_(0)
        {
          int ret = OB_SUCCESS;
          const uint32_t MAX_RO_NUM = 1000;
          const uint32_t MAX_RP_NUM = 1000;
          const uint32_t MAX_RW_NUM = 1000;
          if (OB_SUCCESS != (ret = oQueue_.init(CONST::QueueSize)))
          {
            TBSYS_LOG(ERROR, "ObFixedQueue init error");
          }
          else if (OB_SUCCESS != (ret = oSessionMgr_.init(MAX_RO_NUM, MAX_RP_NUM,
                  MAX_RW_NUM, &oCtxFactory_)))
          {
            TBSYS_LOG(ERROR, "SessionMgr init error");
          }
          else if (OB_SUCCESS != (ret = oStorage_.init()))
          {
            TBSYS_LOG(ERROR, "ObUpsTableMgr init error");
          }
          else if (OB_SUCCESS != (ret = oStorage_.sstable_scan_finished(
                  CONST::MinorVersionLimit)))
          {
            TBSYS_LOG(ERROR, "sstable_scan_finished error");
          }
          else if (0 != (ret = pthread_spin_init(&lCommitMutex_, 0)))
          {
            TBSYS_LOG(ERROR, "pthread_spin_init error");
          }
          else
          {
            ObSchemaManagerV2 schema;
            if (OB_SUCCESS != (ret = CONST::InitSchema(schema)))
            {
              TBSYS_LOG(ERROR, "SchemaGen error");
            }
            else
            {
              CommonSchemaManagerWrapper cs(schema);
              oStorage_.set_schemas(cs);
              ObUpdateServerMain *ups_main = ObUpdateServerMain::get_instance();
              ObUpsTableMgr &table_mgr = ups_main->get_update_server().get_table_mgr();
              table_mgr.set_schemas(cs);
            }
          }
        }
        ~Singleton()
        {
          pthread_spin_destroy(&lCommitMutex_);
        }
        DISALLOW_COPY_AND_ASSIGN(Singleton) ;
      protected:
        ObFixedQueue<int64_t> oQueue_;
        DummyLogWriter      oLogWriter_;
        SessionCtxFactory   oCtxFactory_;
        SessionMgr          oSessionMgr_;
        LockMgr             oLockMgr_;
        ObUpsTableMgr       oStorage_;
        Copy                oCopy_;
        pthread_spinlock_t  lCommitMutex_;
        Random              oRand_;
        int64_t             iLastTimestamp_;
    };
#define S Singleton::getInstance()

    template<class T>
    AutoArray<T>::~AutoArray()
    {
      if (p_ != 0)
      {
        Singleton::getInstance().getCopy().getA().free(p_);
      }
    }

    class Transaction
    {
      public:
        class Guard
        {
          public:
            Guard(Transaction & oTran) throw(ErrMsgException)
              : oTran_(oTran), bHasEnded_(false)
            {
            }
            virtual ~Guard()
            {
              if (!bHasEnded_)
              {
                try
                {
                  rollback();
                }
                catch (ErrMsgException & ex)
                {
                  ex.logException();
                }
              }
            }
            virtual BaseSessionCtx * getSessionCtx() = 0;
            void commit() throw(ErrMsgException)
            {
              oTran_.commit();
              bHasEnded_ = true;
            }
            void rollback() throw(ErrMsgException)
            {
              oTran_.rollback();
              bHasEnded_ = true;
            }
            virtual SessionType getSessionType()
            {
              return oTran_.getSessionType();
            }
          private:
            Guard(const Guard & r);
            void operator=(const Guard & r);
          protected:
            Transaction & oTran_;
            bool bHasEnded_;
        };
      public:
        Transaction(SessionMgr & oSessionMgr)
          : oSessionMgr_(oSessionMgr), uDescriptor_(0)
        {
        }
        virtual ~Transaction()
        {
        }
        void beginTransaction()
        {
          int err = oSessionMgr_.begin_session(getSessionType(),
              tbsys::CTimeUtil::getTime(), CONST::TransactionTimeout,
              CONST::TransactionIdleTime, uDescriptor_);
          if (OB_SUCCESS != err)
          {
            THROW_EXCEPTION_AND_LOG(err, "begin_session error, err: %d", err);
          }
        }
        void commit()
        {
          endTransaction_(false);
        }
        void rollback()
        {
          endTransaction_(true);
        }
        virtual SessionType        getSessionType() = 0;
        virtual BaseSessionCtx *   getSessionCtx() = 0;
      protected:
        void endTransaction_(bool isRollback)
        {
          int err = OB_SUCCESS;
          if (NULL != getSessionCtx())
          {
            if (OB_SUCCESS != (err = S.endSession(oSessionMgr_, getSessionCtx(),
                    uDescriptor_, isRollback)))
            {
              THROW_EXCEPTION_AND_LOG(err, "endSession error, err: %d", err);
            }
            else
            {
              uDescriptor_ = 0;
            }
          }
        }
      protected:
        SessionMgr &         oSessionMgr_;
        uint32_t             uDescriptor_;
    };

    class ROTransaction : public Transaction
    {
      public:
        typedef BaseSessionCtx     SessionCtxType;
      public:
        class ROGuard : public Guard
        {
          public:
            ROGuard(ROTransaction & oROTran) throw(ErrMsgException)
              : Guard(oROTran)
            {
              beginTransaction();
            }
            void beginTransaction() throw(ErrMsgException)
            {
              reinterpret_cast<ROTransaction &>(oTran_).beginTransaction();
              bHasEnded_ = false;
            }
            virtual BaseSessionCtx * getSessionCtx()
            {
              return (reinterpret_cast<ROTransaction &>(oTran_)).getSessionCtx();
            }
        };
        ROTransaction(SessionMgr & oSessionMgr)
          : Transaction(oSessionMgr)
        {
        }
        void beginTransaction()
        {
          Transaction::beginTransaction();
          poBaseSessionCtx_ = oSessionMgr_.fetch_ctx<BaseSessionCtx>(uDescriptor_);
          if (poBaseSessionCtx_ == NULL)
          {
            THROW_EXCEPTION_AND_LOG(OB_ERROR, "fetch_ctx error");
          }
        }
        virtual SessionType        getSessionType()
        {
          return ST_READ_ONLY;
        }
        virtual BaseSessionCtx *   getSessionCtx()
        {
          return poBaseSessionCtx_;
        }
      protected:
        BaseSessionCtx *     poBaseSessionCtx_;
    };

    class RWTransaction : public Transaction
    {
      public:
        typedef RWSessionCtx       SessionCtxType;
      public:
        class RWGuard : public Guard
        {
          public:
            RWGuard(RWTransaction & oRWTran)
              : Guard(oRWTran)
            {
              beginTransaction();
            }
            void beginTransaction() throw(ErrMsgException)
            {
              reinterpret_cast<RWTransaction &>(oTran_).beginTransaction();
              bHasEnded_ = false;
            }
            virtual RWSessionCtx * getSessionCtx()
            {
              return reinterpret_cast<RWTransaction &>(oTran_).getSessionCtx();
            }
        };
        RWTransaction(SessionMgr & oSessionMgr, LockMgr & oLockMgr)
          : Transaction(oSessionMgr), oLockMgr_(oLockMgr)
        {
        }
        void beginTransaction()
        {
          ILockInfo * poLockInfo = NULL;
          Transaction::beginTransaction();
          poRWSessionCtx_ = oSessionMgr_.fetch_ctx<RWSessionCtx>(uDescriptor_);
          if (poRWSessionCtx_ == NULL)
          {
            THROW_EXCEPTION_AND_LOG(OB_ERROR, "fetch_ctx error");
          }
          else if (NULL == (poLockInfo = oLockMgr_.assign(READ_COMMITED,
                *poRWSessionCtx_)))
          {
            THROW_EXCEPTION_AND_LOG(OB_ERROR, "assign lock error");
          }
          else
          {
            poLockInfo->on_trans_begin();
          }
        }
        virtual SessionType      getSessionType()
        {
          return ST_READ_WRITE;
        }
        virtual RWSessionCtx *   getSessionCtx()
        {
          return poRWSessionCtx_;
        }
      protected:
        LockMgr &            oLockMgr_;
        RWSessionCtx *       poRWSessionCtx_;
    };

    class DAO
    {
      protected:
        ObUpsTableMgr &      oStorage_;
        Copy &               oCopy_;
        ObGetParam           oGetParam_;
        ObCellNewScanner     oScanner_;
        ObUpsRow             oUpsRow_;
        ObRowDesc            oRowDesc_;
        ObObj                oRow1_;
        ObCellInfo           oCellInfo_;
        ObMutator            oMutator_;
        char *               pTempBuffer_;
        static const int64_t icTempBufferSize_ = 2L << 20;
      public:
        DAO(ObUpsTableMgr & oStorage, Copy & oCopy)
          : oStorage_(oStorage), oCopy_(oCopy)
        {
          pTempBuffer_ = new char[icTempBufferSize_];
        }
        virtual ~DAO()
        {
          delete[] pTempBuffer_;
          pTempBuffer_ = NULL;
        }
        int getInfoLine(BaseSessionCtx * poSessionCtx,
            int64_t iRowkey, int64_t & iRowStat, int64_t & iWriteThreadID,
            AutoString & pszCheckDataValue) throw(ErrMsgException)
        {
          return getInfoLine_(poSessionCtx, LF_NONE, iRowkey, iRowStat,
              iWriteThreadID, pszCheckDataValue);
        }
        int getInfoLineLocked(BaseSessionCtx * poSessionCtx,
            int64_t iRowkey, int64_t & iRowStat, int64_t & iWriteThreadID,
            AutoString & pszCheckDataValue) throw(ErrMsgException)
        {
          return getInfoLine_(poSessionCtx, LF_WRITE, iRowkey, iRowStat,
              iWriteThreadID, pszCheckDataValue);
        }
        void getDataLine(BaseSessionCtx * poSessionCtx, int64_t iRowkey,
            int64_t & iSelfCheck2, AutoArray<int64_t> & aiMultiChecks,
            int64_t & iMultiCheckNum) throw(ErrMsgException)
        {
          int err = OB_SUCCESS;
          ObObj *    poCell;
          iMultiCheckNum = CONST::GetApplierNum();
          uint64_t * aiColumnIDs = reinterpret_cast<uint64_t*>(
                  alloca(sizeof(uint64_t) * (iMultiCheckNum + 1)));
          aiColumnIDs[0] = CONST::SelfCheck2CID;
          for (int64_t i = 0; i < iMultiCheckNum; i++)
          {
            aiColumnIDs[i + 1] = CONST::MultiCheckBaseCID + i;
          }
          int iColumnNum = static_cast<int32_t>(iMultiCheckNum + 1);
          prepareGetParam_(CONST::CheckDataTID, iRowkey, aiColumnIDs, iColumnNum);
          execGet_(poSessionCtx, LF_NONE);
          if (OB_SUCCESS != (err = oUpsRow_.get_cell(CONST::CheckDataTID,
                    CONST::SelfCheck2CID, poCell)))
          {
            THROW_EXCEPTION_AND_LOG(err, "get_cell error");
          }
          else if (OB_SUCCESS != (err = poCell->get_int(iSelfCheck2)))
          {
            THROW_EXCEPTION_AND_LOG(err, "get_int error. CellType: %d", poCell->get_type());
          }
          else
          {
            aiMultiChecks.reset(oCopy_.allocArray<int64_t>(iMultiCheckNum).release());
            for (int64_t i = 0; i < iMultiCheckNum; i++)
            {
              if (OB_SUCCESS != (err = oUpsRow_.get_cell(CONST::CheckDataTID,
                    CONST::MultiCheckBaseCID + i, poCell)))
              {
                THROW_EXCEPTION_AND_LOG(err, "get_cell error");
              }
              else if (poCell->get_type() == ObIntType)
              {
                if (OB_SUCCESS != (err = poCell->get_int(aiMultiChecks[i])))
                {
                  THROW_EXCEPTION_AND_LOG(err, "get_int error. CellType: %d", poCell->get_type());
                }
              }
              else if (poCell->get_type() == ObExtendType)
              {
                if (poCell->get_ext() != ObActionFlag::OP_NOP)
                {
                  err = OB_ERROR;
                  THROW_EXCEPTION_AND_LOG(err, "ext error. ext: %ld", poCell->get_ext());
                }
                else
                {
                  aiMultiChecks[i] = 0;
                }
              }
            }
          }
        }
        void getDataLine(BaseSessionCtx * poSessionCtx, int64_t iRowkey,
            int64_t iWriteThreadID, int64_t & iSelfCheck1,
            int64_t & iMultiCheck) throw(ErrMsgException)
        {
          int err = OB_SUCCESS;
          ObObj *    poCell;
          uint64_t   aiColumnIDs[] = {
                  CONST::SelfCheck1CID,
                  CONST::MultiCheckBaseCID + iWriteThreadID
          };
          int iColumnNum = sizeof(aiColumnIDs) / sizeof(aiColumnIDs[0]);
          prepareGetParam_(CONST::CheckDataTID,
                  iRowkey, aiColumnIDs, iColumnNum);
          execGet_(poSessionCtx, LF_NONE);
          if (OB_SUCCESS != (err = oUpsRow_.get_cell(CONST::CheckDataTID,
                  CONST::SelfCheck1CID, poCell)))
          {
            THROW_EXCEPTION_AND_LOG(err, "get_cell error");
          }
          else if (OB_SUCCESS != (err = poCell->get_int(iSelfCheck1)))
          {
            THROW_EXCEPTION_AND_LOG(err, "get_int error. CellType: %d", poCell->get_type());
          }
          else if (OB_SUCCESS != (err = oUpsRow_.get_cell(CONST::CheckDataTID,
                  CONST::MultiCheckBaseCID + iWriteThreadID, poCell)))
          {
            THROW_EXCEPTION_AND_LOG(err, "get_cell error");
          }
          else if (OB_SUCCESS != (err = poCell->get_int(iMultiCheck)))
          {
            THROW_EXCEPTION_AND_LOG(err, "get_int error. CellType: %d", poCell->get_type());
          }
        }
        void applyInfoLine(RWSessionCtx * oSessionCtx, int64_t iRowkey,
            int64_t iRowStat, int64_t iWriteThreadID, char * pszCheckDataValue)
            throw(ErrMsgException)
        {
          uint64_t aiColumnIDs[] = {
                  CONST::RowStatCID,
                  CONST::WriteThreadIDCID,
                  };
          int64_t aiValues[] = {
                  iRowStat,
                  iWriteThreadID,
                  };
          int iColumnNum = sizeof(aiColumnIDs) / sizeof(aiColumnIDs[0]);
          prepareMutator_(CONST::CheckInfoTID, iRowkey, aiColumnIDs, aiValues,
              iColumnNum);
          ObObj    oRow1(ObIntType, 0, 0, iRowkey);
          ObRowkey oRowkey(&oRow1, 1);
          ObObj    oVal;
          int err = oMutator_.insert(CONST::CheckInfoTID, oRowkey,
                  CONST::CheckDataValueCID,
                                     ObObj(ObVarcharType, 0, static_cast<int32_t>(strlen(pszCheckDataValue)),
                    reinterpret_cast<int64_t>(pszCheckDataValue)));
          if (OB_SUCCESS != err)
          {
            THROW_EXCEPTION_AND_LOG(err, "insert error");
          }
          execApply(oSessionCtx);
        }
        void applyInfoLine(RWSessionCtx * oSessionCtx, int64_t iRowkey,
            int64_t iRowStat) throw(ErrMsgException)
        {
          uint64_t aiColumnIDs[] = {
                  CONST::RowStatCID,
                  };
          int64_t aiValues[] = {
                  iRowStat,
                  };
          int iColumnNum = sizeof(aiColumnIDs) / sizeof(aiColumnIDs[0]);
          prepareMutator_(CONST::CheckInfoTID, iRowkey, aiColumnIDs, aiValues,
              iColumnNum);
          execApply(oSessionCtx);
        }
        void applyDataLine(RWSessionCtx * oSessionCtx, int64_t iRowkey,
            int iThreadID, int64_t iSelfCheck1, int64_t iSelfCheck2,
            int64_t iMultiCheck) throw(ErrMsgException)
        {
          uint64_t aiColumnIDs[] = {
                  CONST::SelfCheck1CID,
                  CONST::SelfCheck2CID,
                  CONST::MultiCheckBaseCID + iThreadID,
                  };
          int64_t aiValues[] = {
                  iSelfCheck1,
                  iSelfCheck2,
                  iMultiCheck
                  };
          int iColumnNum = sizeof(aiColumnIDs) / sizeof(aiColumnIDs[0]);
          prepareMutator_(CONST::CheckDataTID, iRowkey, aiColumnIDs, aiValues,
              iColumnNum);
          execApply(oSessionCtx);
        }
        void applyDataLine(RWSessionCtx * oSessionCtx, int64_t iRowkey,
            int64_t iSelfCheck2) throw(ErrMsgException)
        {
          uint64_t aiColumnIDs[] = {
                  CONST::SelfCheck2CID,
                  };
          int64_t aiValues[] = {
                  iSelfCheck2,
                  };
          int iColumnNum = sizeof(aiColumnIDs) / sizeof(aiColumnIDs[0]);
          prepareMutator_(CONST::CheckDataTID, iRowkey, aiColumnIDs, aiValues,
              iColumnNum);
          execApply(oSessionCtx);
        }
      protected:
        void prepareGetParam_(uint64_t iTableID, int64_t iRowkey,
            uint64_t aiColumnIDs[], int iColumnNum) throw(ErrMsgException)
        {
          int err = OB_SUCCESS;
          oGetParam_.reset();
          oScanner_.reuse();
          //oUpsRow_.reset();
          oRowDesc_.reset();
          oCellInfo_.reset();

          ObVersionRange oVersionRange;
          oVersionRange.start_version_.major_ = 2;
          oVersionRange.start_version_.minor_ = 0;
          oVersionRange.start_version_.is_final_minor_ = 0;
          oVersionRange.border_flag_.set_inclusive_start();
          oVersionRange.border_flag_.set_max_value();
          oGetParam_.set_version_range(oVersionRange);

          oRow1_.set_int(iRowkey);
          oCellInfo_.table_id_  = iTableID;
          oCellInfo_.row_key_.assign(&oRow1_, 1);
          for (int i = 0; i < iColumnNum; i++)
          {
            oCellInfo_.column_id_ = aiColumnIDs[i];
            if (OB_SUCCESS != (err = oGetParam_.add_cell(oCellInfo_)))
            {
              THROW_EXCEPTION_AND_LOG(err, "add_cell error");
            }
          }
        }
        void execGet_(BaseSessionCtx * poSessionCtx, ObLockFlag iLockFlag)
           throw(ErrMsgException)
        {
          int err = OB_SUCCESS;
          const ObRowkey * cpoRowkey;
          int64_t iStartTime = tbsys::CTimeUtil::getTime();
          int64_t iTimeout = 1000000;
          if(OB_SUCCESS !=
              (err = ObNewScannerHelper::get_row_desc(oGetParam_, false, oRowDesc_)))
          {
            THROW_EXCEPTION_AND_LOG(err, "get_row_desc error");
          }
          else
          {
            oScanner_.set_row_desc(oRowDesc_);
            if (OB_SUCCESS != (err = oStorage_.new_get(*poSessionCtx,
                    oGetParam_, oScanner_, iStartTime, iTimeout, iLockFlag)))
            {
              THROW_EXCEPTION(err, "new_get error");
            }
            else
            {
              oUpsRow_.set_row_desc(oRowDesc_);
              if (OB_SUCCESS != (err = oScanner_.get_next_row(cpoRowkey, oUpsRow_)))
              {
                THROW_EXCEPTION_AND_LOG(err, "get_next_row error");
              }
            }
          }
        }
        int getInfoLine_(BaseSessionCtx * poSessionCtx, ObLockFlag iLockFlag,
            int64_t iRowkey, int64_t & iRowStat, int64_t & iWriteThreadID,
            AutoString & pszCheckDataValue) throw(ErrMsgException)
        {
          int ret = OB_SUCCESS;
          int err = OB_SUCCESS;
          ObObj *    poCell;
          ObString   oString;
          uint64_t   aiColumnIDs[] = {
                  CONST::RowStatCID,
                  CONST::WriteThreadIDCID,
                  CONST::CheckDataValueCID
                  };
          int iColumnNum = sizeof(aiColumnIDs) / sizeof(aiColumnIDs[0]);
          prepareGetParam_(CONST::CheckInfoTID, iRowkey, aiColumnIDs, iColumnNum);
          execGet_(poSessionCtx, iLockFlag);
          if (oUpsRow_.get_is_all_nop())
          {
            ret = OB_ENTRY_NOT_EXIST;
          }
          else if (OB_SUCCESS != (err = oUpsRow_.get_cell(CONST::CheckInfoTID,
                    CONST::RowStatCID, poCell)))
          {
            THROW_EXCEPTION_AND_LOG(err, "get_cell error");
          }
          else if (OB_SUCCESS != (err = poCell->get_int(iRowStat)))
          {
            THROW_EXCEPTION_AND_LOG(err, "get_int error. CellType: %d", poCell->get_type());
          }
          else if (OB_SUCCESS != (err = oUpsRow_.get_cell(CONST::CheckInfoTID,
                  CONST::WriteThreadIDCID, poCell)))
          {
            THROW_EXCEPTION_AND_LOG(err, "get_cell error");
          }
          else if (OB_SUCCESS != (err = poCell->get_int(iWriteThreadID)))
          {
            THROW_EXCEPTION_AND_LOG(err, "get_int error. CellType: %d", poCell->get_type());
          }
          else if (OB_SUCCESS != (err = oUpsRow_.get_cell(CONST::CheckInfoTID,
                  CONST::CheckDataValueCID, poCell)))
          {
            THROW_EXCEPTION_AND_LOG(err, "get_cell error");
          }
          else if (OB_SUCCESS != (err = poCell->get_varchar(oString)))
          {
            THROW_EXCEPTION_AND_LOG(err, "get_varchar error. CellType: %d", poCell->get_type());
          }
          else
          {
            pszCheckDataValue.reset(oCopy_.copyCstr(oString).release());
            //TBSYS_LOG(INFO, "Rowkey: %ld  RowStat: %ld  WriteThreadID: %ld  "
            //    "CheckDataValue: %s", iRowkey, iRowStat, iWriteThreadID,
            //    pszCheckDataValue.get());
          }
          return ret;
        }
        void prepareMutator_(uint64_t iTableID, int64_t iRowkey,
            uint64_t aiColumnIDs[], int64_t aiValues[], int iColumnNum)
            throw(ErrMsgException)
        {
          int err = OB_SUCCESS;
          oMutator_.reset();
          ObObj    oRow1(ObIntType, 0, 0, iRowkey);
          ObRowkey oRowkey(&oRow1, 1);
          for (int i = 0; i < iColumnNum; i++)
          {
            if (OB_SUCCESS != (err = oMutator_.insert(iTableID, oRowkey,
                    aiColumnIDs[i], ObObj(ObIntType, 0, 0, aiValues[i]))))
            {
              THROW_EXCEPTION_AND_LOG(err, "insert error");
            }
          }
        }
        void sedesMutator_()
        {
          int64_t pos = 0;
          oMutator_.serialize(pTempBuffer_, icTempBufferSize_, pos);
          pos = 0;
          oMutator_.deserialize(pTempBuffer_, icTempBufferSize_, pos);
        }
        void execApply(RWSessionCtx * poSessionCtx) throw(ErrMsgException)
        {
          int err = OB_SUCCESS;
          bool bUsingId = true;
          sedesMutator_();
          if (OB_SUCCESS != (err = oStorage_.apply(bUsingId, *poSessionCtx,
              *poSessionCtx->get_lock_info(), oMutator_)))
          {
            THROW_EXCEPTION(err, "apply error");
          }
        }
    };

    class ApplyThread : public tbsys::CDefaultRunnable
    {
      protected:
        class Applier
        {
          protected:
            RWSessionCtx *   poRWSessionCtx_;
            BaseSessionCtx * poBaseSessionCtx_;
            uint32_t         uDescriptor_;
            ApplyThread &    roThread_;
            int64_t          iInfoRowkey_;
            int64_t          iRowStat_;
            int64_t          iWriteThreadID_;
            AutoString       pszCheckDataValue_;
            bool             bCommit_;
            int64_t          iChangedRowCounter_;
            RWTransaction    oTran_;
            DAO              oDao_;
          public:
            Applier(ApplyThread & roThread)
              : roThread_(roThread), iChangedRowCounter_(0),
                oTran_(roThread.session_, roThread.lock_mgr_),
                oDao_(roThread.storage_, roThread.oCopy_)
            {
            }
            int applyRandom()
            {
              int ret = OB_SUCCESS;
              try
              {
                RWTransaction::RWGuard oGuard(oTran_);
                randomInfoRowkey_(oGuard);
                randomData_();
                writeInfo_();
                commitOrRollback_(oGuard);
              }
              catch (ErrMsgException & ex)
              {
                ex.logException();
                ret = ex.getErrorCode();
              }
              return ret;
            }
            int64_t getInfoRowkey()
            {
              return iInfoRowkey_;
            }
            bool    getCommit()
            {
              return bCommit_;
            }
            int64_t getChangedRowCounter()
            {
              return iChangedRowCounter_;
            }
          protected:
            void randomInfoRowkey_(RWTransaction::RWGuard & oGuard) throw(ErrMsgException)
            {
              int err = OB_SUCCESS;
              int64_t iErrorCounter = 0;
              bool bFirstTime = true;
              do
              {
                if (bFirstTime)
                {
                  bFirstTime = false;
                }
                else
                {
                  oGuard.rollback();
                  oGuard.beginTransaction();
                }
                iInfoRowkey_ = CONST::GetInfoRowkeyMultiplier() * CONST::GetInfoMaxRowkey()
                        + roThread_.oRand_.rand(CONST::GetInfoMaxRowkey());
                try
                {
                  err = oDao_.getInfoLineLocked(oGuard.getSessionCtx(),
                      iInfoRowkey_, iRowStat_, iWriteThreadID_,
                      pszCheckDataValue_);
                }
                catch (ErrMsgException & ex)
                {
                  if (ex.getErrorCode() == OB_ERR_SHARED_LOCK_CONFLICT)
                  {
                    err = ex.getErrorCode();
                    //TBSYS_LOG(INFO, "1: iInfoRowkey_: %ld iRowStat_: %ld err: %d",
                    //    iInfoRowkey_, iRowStat_, err);
                  }
                  else
                  {
                    RETHROW(ex);
                  }
                }
                //TBSYS_LOG(ERROR, "1: iInfoRowkey_: %ld iRowStat_: %ld err: %d",
                //    iInfoRowkey_, iRowStat_, err);
              }
              while (!(OB_ENTRY_NOT_EXIST == err
                     || (OB_SUCCESS == err && iRowStat_ == 0)
                     || (++iErrorCounter >= CONST::GetInfoMaxRowkey())));
              //TBSYS_LOG(ERROR, "2: iInfoRowkey_: %ld iRowStat_: %ld err: %d",
              //    iInfoRowkey_, iRowStat_, err);
              if (iErrorCounter >= CONST::GetInfoMaxRowkey())
              {
                THROW_EXCEPTION(ERROR_CODE_TRY_FAILED, "Have tried all InfoRowkey: "
                    "counter = %ld", iErrorCounter);
              }
              //if (iErrorCounter > 0)
              //{
              //  TBSYS_LOG(INFO, "2: iInfoRowkey_: %ld iRowStat_: %ld err: %d",
              //      iInfoRowkey_, iRowStat_, err);
              //}
            }
            void randomData_() throw(ErrMsgException)
            {
              int err = OB_SUCCESS;
              char pszData[BUFSIZ];
              int  iDataPos = 0;
              int64_t iGenTimestamp = CONST::GetUniqTime();
              int64_t iDataRowkeyRand = roThread_.oRand_.rand(CONST::GetDataMaxRowkey());
              int64_t iDataRowkeyStart = CONST::GetDataRowkeyMultiplier()
                      * CONST::GetDataMaxRowkey() + iDataRowkeyRand;
              int64_t iDataApplyNum = roThread_.oRand_.rand(
                           std::min(CONST::GetDataMaxRowkey() - iDataRowkeyRand,
                                    +CONST::DataLineNumMax));
              iDataPos += snprintf(pszData + iDataPos, BUFSIZ, "%ld", iGenTimestamp);
              iDataPos += snprintf(pszData + iDataPos, BUFSIZ, " %ld", iDataRowkeyStart);
              iDataPos += snprintf(pszData + iDataPos, BUFSIZ, " %ld", iDataApplyNum);
              int64_t iSelfCheck2;
              AutoArray<int64_t> aiMultiChecks;
              int64_t iMultiCheckNum;
              int64_t iChecksum;
              for (int64_t i = iDataRowkeyStart;
                   i < iDataRowkeyStart + iDataApplyNum;
                   i++)
              {
                int64_t iMultiCheck = roThread_.oRand_.rand();
                while (true)
                {
                  try
                  {
                    oDao_.applyDataLine(oTran_.getSessionCtx(), i,
                        roThread_.iThreadID_, iGenTimestamp, 0, iMultiCheck);
                    break;
                  }
                  catch (ErrMsgException & ex)
                  {
                    if (ex.getErrorCode() != OB_ERR_EXCLUSIVE_LOCK_CONFLICT)
                    {
                      RETHROW(ex);
                    }
                  }
                }
                oDao_.getDataLine(oTran_.getSessionCtx(), i, iSelfCheck2,
                    aiMultiChecks, iMultiCheckNum);
                calcChecksum_(aiMultiChecks, iMultiCheckNum, iChecksum);

                try
                {
                  oDao_.applyDataLine(oTran_.getSessionCtx(), i, iChecksum);
                }
                catch (ErrMsgException & ex)
                {
                }
                iDataPos += snprintf(pszData + iDataPos, BUFSIZ, " %ld",
                    iMultiCheck);
                iChangedRowCounter_++;
              }
              if (OB_SUCCESS == err)
              {
                pszCheckDataValue_.reset(roThread_.oCopy_.copyCstr(pszData).release());
              }
            }
            void calcChecksum_(AutoArray<int64_t> & aiMultiChecks,
                int64_t iMultiCheckNum, int64_t & iChecksum) throw(ErrMsgException)
            {
              iChecksum = 0;
              for (int i = 0; i < iMultiCheckNum; i++)
              {
                iChecksum ^= aiMultiChecks[i];
              }
            }
            void writeInfo_() throw(ErrMsgException)
            {
              iRowStat_ = 1;
              iWriteThreadID_ = roThread_.iThreadID_;
              oDao_.applyInfoLine(oTran_.getSessionCtx(), iInfoRowkey_,
                  iRowStat_, iWriteThreadID_, pszCheckDataValue_.get());
              iChangedRowCounter_++;
            }
            void commitOrRollback_(RWTransaction::RWGuard & oGuard) throw(ErrMsgException)
            {
              if (static_cast<int64_t>(roThread_.oRand_.rand(100))
                      < CONST::CommitPercentage)
              {
                commit_(oGuard);
                bCommit_ = true;
              }
              else
              {
                rollback_(oGuard);
                bCommit_ = false;
              }
            }
            void commit_(RWTransaction::RWGuard & oGuard) throw(ErrMsgException)
            {
              oGuard.commit();
            }
            void rollback_(RWTransaction::RWGuard & oGuard) throw(ErrMsgException)
            {
              oGuard.rollback();
            }
        };
      protected:
        ObFixedQueue<int64_t> & oQueue_;
        SessionMgr &         session_;
        LockMgr &            lock_mgr_;
        ObUpsTableMgr &      storage_;
        Copy &               oCopy_;
        int                  iThreadID_;
        pthread_spinlock_t & lCommitMutex_;
        Random &             oRand_;
        Applier              oApplier_;
        int                  iErr_;
      public:
        ApplyThread(ObFixedQueue<int64_t> & oQueue, SessionMgr & session,
            LockMgr & lock_mgr, ObUpsTableMgr & storage, Copy & oCopy,
            int iThreadID, pthread_spinlock_t & lCommitMutex, Random & oRand)
          : oQueue_(oQueue), session_(session), lock_mgr_(lock_mgr),
            storage_(storage), oCopy_(oCopy), iThreadID_(iThreadID),
            lCommitMutex_(lCommitMutex), oRand_(oRand), oApplier_(*this)
        {
        }
        ApplyThread(const ApplyThread & r)
          : CDefaultRunnable(), oQueue_(r.oQueue_), session_(r.session_),
            lock_mgr_(r.lock_mgr_), storage_(r.storage_), oCopy_(r.oCopy_),
            iThreadID_(r.iThreadID_), lCommitMutex_(r.lCommitMutex_),
            oRand_(r.oRand_), oApplier_(*this)
        {
        }
        ~ApplyThread()
        {
        }
        virtual void run(tbsys::CThread *thread, void *arg)
        {
          UNUSED(thread);
          UNUSED(arg);
          int err = OB_SUCCESS;
          int64_t counter = 0;
          while (!_stop && OB_SUCCESS == err)
          {
            if (OB_SUCCESS != (err = oApplier_.applyRandom()))
            {
              if (ERROR_CODE_TRY_FAILED == err)
              {
                err = OB_SUCCESS;
              }
              else
              {
                TBSYS_LOG(ERROR, "applyRandom error");
              }
            }
            else if (oApplier_.getCommit())
            {
              int64_t iInfoRowkey = oApplier_.getInfoRowkey();
              if (iInfoRowkey != 0)
              {
                if (OB_SUCCESS != (err = oQueue_.push((int64_t *)iInfoRowkey)))
                {
                  TBSYS_LOG(ERROR, "push error");
                }
                else
                {
                  if (counter++ % CONST::PointMultiplier == 0)
                  {
                    fprintf(stderr, ".");
                  }
                }
              }
            }
          }
          fprintf(stderr, "\n");
          TBSYS_LOG(INFO, "===========");
          TBSYS_LOG(INFO, "Thread(%d) did %ld tests changed %ld rows",
              iThreadID_, counter, oApplier_.getChangedRowCounter());
          TBSYS_LOG(INFO, "===========");
          iErr_ = err;
        }
        void setThreadID(int iThreadID)
        {
          iThreadID_ = iThreadID;
        }
        int getErr()
        {
          return iErr_;
        }
    };
    typedef std::vector<ApplyThread> ApplierVector;

    class CheckThread : public tbsys::CDefaultRunnable
    {
      protected:
        class Checker
        {
          protected:
            BaseSessionCtx * poBaseSessionCtx_;
            CheckThread &    oThread_;
            uint32_t         uDescriptor_;
            int64_t          iInfoRowkey_;
            int64_t          iRowStat_;
            int64_t          iWriteThreadID_;
            AutoString       pszCheckDataValue_;
            RWTransaction    oRWTran_;
            ROTransaction    oROTran_;
            DAO              oDao_;
          public:
            Checker(CheckThread & oThread)
              : oThread_(oThread),
                oRWTran_(oThread.session_, oThread.lock_mgr_),
                oROTran_(oThread.session_),
                oDao_(oThread.storage_, oThread.oCopy_)
            {
            }
            int check(int64_t iInfoRowkey)
            {
              UNUSED(iInfoRowkey);
              int ret = OB_SUCCESS;
              iInfoRowkey_ = iInfoRowkey;
              try
              {
                RWTransaction::RWGuard oRWGuard(oRWTran_);
                ROTransaction::ROGuard oROGuard(oROTran_);
                getInfo_();
                checkData_();
                writeRowStat_(oRWGuard);
                endSession_(oRWGuard, oROGuard);
              }
              catch (ErrMsgException & ex)
              {
                TBSYS_LOG(ERROR, "check error: iInfoRowkey_: %ld  "
                    "iRowStat_: %ld  iWriteThreadID_: %ld  pszCheckDataValue_: %s",
                    iInfoRowkey_, iRowStat_, iWriteThreadID_, pszCheckDataValue_.get());
                ret = ex.getErrorCode();
                abort();
              }
              return ret;
            }
          protected:
            void getInfo_() throw(ErrMsgException)
            {
              oDao_.getInfoLine(oROTran_.getSessionCtx(), iInfoRowkey_,
                  iRowStat_, iWriteThreadID_, pszCheckDataValue_);
              if (iRowStat_ != 1)
              {
                THROW_EXCEPTION_AND_LOG(OB_ERROR, "check error, InfoRowkey: %ld "
                    "iRowStat_: %ld", iInfoRowkey_, iRowStat_);
              }
            }
            void checkData_() throw(ErrMsgException)
            {
              int64_t iPos = 0;
              int64_t iGenTimestamp = 0;
              int64_t iDataStartRowkey = 0;
              int64_t iDataRowkeyNum = 0;
              getNextInt_(pszCheckDataValue_, iPos, iGenTimestamp);
              getNextInt_(pszCheckDataValue_, iPos, iDataStartRowkey);
              getNextInt_(pszCheckDataValue_, iPos, iDataRowkeyNum);
              for (int64_t i = iDataStartRowkey;
                   i < iDataStartRowkey + iDataRowkeyNum;
                   i++)
              {
                int64_t iCheckValue = 0;
                int64_t iSelfCheck1 = 0;
                int64_t iSelfCheck2 = 0;
                int64_t iMultiCheck = 0;
                AutoArray<int64_t> aiMultiChecks;
                int64_t iMultiCheckNum = 0;
                int64_t iChecksum = 0;
                getNextInt_(pszCheckDataValue_, iPos, iCheckValue);
                oDao_.getDataLine( oROTran_.getSessionCtx(), i, iWriteThreadID_,
                        iSelfCheck1, iMultiCheck);
                if (iGenTimestamp == iSelfCheck1 && iCheckValue != iMultiCheck)
                {
                  THROW_EXCEPTION_AND_LOG(OB_ERROR, "check error, DataRowkey: %ld "
                      "iWriteThreadID_: %ld iCheckValue: %ld iMultiCheck: %ld",
                      i, iWriteThreadID_, iCheckValue, iMultiCheck);
                }
                else
                {
                  oDao_.getDataLine(oROTran_.getSessionCtx(), i, iSelfCheck2,
                        aiMultiChecks, iMultiCheckNum);
                  calcChecksum_(aiMultiChecks, iMultiCheckNum, iChecksum);
                  if (iChecksum != iSelfCheck2)
                  {
                    THROW_EXCEPTION_AND_LOG(OB_ERROR, "checksum error, DataRowkey: %ld "
                        "iChecksum: %ld iSelfCheck2: %ld", i, iChecksum,
                        iSelfCheck2);
                  }
                }
              }
            }
            void calcChecksum_(AutoArray<int64_t> & aiMultiChecks,
                int64_t iMultiCheckNum, int64_t & iChecksum)
            {
              iChecksum = 0;
              for (int i = 0; i < iMultiCheckNum; i++)
              {
                iChecksum ^= aiMultiChecks[i];
              }
            }
            void writeRowStat_(RWTransaction::RWGuard & oRWGuard) throw(ErrMsgException)
            {
              iRowStat_ = 0;
              while (true)
              {
                try
                {
                  oDao_.applyInfoLine(oRWGuard.getSessionCtx(), iInfoRowkey_,
                      iRowStat_);
                  break;
                }
                catch (ErrMsgException & ex)
                {
                  if (ex.getErrorCode() != OB_ERR_EXCLUSIVE_LOCK_CONFLICT)
                  {
                    RETHROW(ex);
                  }
                }
              }
            }
            void endSession_(RWTransaction::RWGuard & oRWGuard,
                ROTransaction::ROGuard & oROGuard) throw(ErrMsgException)
            {
              oRWGuard.commit();
              oROGuard.commit();
            }
            void getNextInt_(const AutoString & pszS, int64_t & pos,
                int64_t & iInt) throw(ErrMsgException)
            {
              if (pszS[pos] == '\0')
              {
                THROW_EXCEPTION_AND_LOG(OB_ITER_END, "no more integer in string");
              }
              else
              {
                while (pszS[pos] == ' ')
                {
                  pos++;
                }
                char * endp;
                iInt = strtol(pszS.get() + pos, &endp, 10);
                pos = endp - pszS.get();
              }
            }
        };
      protected:
        ObFixedQueue<int64_t> & oQueue_;
        SessionMgr &         session_;
        LockMgr &            lock_mgr_;
        ObUpsTableMgr &      storage_;
        Copy &               oCopy_;
        int                  iThreadID_;
        Random &             oRand_;
        Checker              oChecker_;
        int                  iErr_;
      public:
        CheckThread(ObFixedQueue<int64_t> & oQueue, SessionMgr & session,
            LockMgr & lock_mgr, ObUpsTableMgr & storage, Copy & oCopy,
            int iThreadID, Random & oRand)
          : oQueue_(oQueue), session_(session), lock_mgr_(lock_mgr),
            storage_(storage), oCopy_(oCopy), iThreadID_(iThreadID),
            oRand_(oRand), oChecker_(*this)
        {
        }
        CheckThread(const CheckThread & r)
          : CDefaultRunnable(), oQueue_(r.oQueue_), session_(r.session_),
            lock_mgr_(r.lock_mgr_), storage_(r.storage_), oCopy_(r.oCopy_),
            iThreadID_(r.iThreadID_), oRand_(r.oRand_), oChecker_(*this)
        {
        }
        virtual void run(tbsys::CThread *thread, void *arg)
        {
          UNUSED(thread);
          UNUSED(arg);
          int err = OB_SUCCESS;
          int64_t iSuccCounter = 0;
          while (!_stop && OB_SUCCESS == err)
          {
            int64_t * piTemp;
            if (OB_SUCCESS == oQueue_.pop(piTemp))
            {
              if (NULL == piTemp)
              {
                TBSYS_LOG(ERROR, "piTemp error");
                abort();
                err = OB_ERROR;
              }
              else
              {
                int64_t iInfoRowkey = (int64_t)piTemp;
                if (OB_SUCCESS != (err = oChecker_.check(iInfoRowkey)))
                {
                  TBSYS_LOG(ERROR, "check error: iInfoRowkey_: %ld", iInfoRowkey);
                }
                else
                {
                  iSuccCounter++;
                }
              }
            }
          }
          iErr_ = err;
          TBSYS_LOG(INFO, "STAT: SuccCounter: %ld", iSuccCounter);
        }
        void setThreadID(int iThreadID)
        {
          iThreadID_ = iThreadID;
        }
        int getErr()
        {
          return iErr_;
        }
    };
    typedef std::vector<CheckThread> CheckerVector;

    int TestInsert(SessionMgr & session_,
        LockMgr & lock_mgr_, ObUpsTableMgr & storage_)
    {
      RWTransaction rw(session_, lock_mgr_);
      DAO oDao(storage_, S.getCopy());

      rw.beginTransaction();

      oDao.applyInfoLine(rw.getSessionCtx(), 1, 2, 3, const_cast<char*>("123"));
      int64_t iRowkey = 1;
      int64_t iRowStat;
      int64_t iWriteThreadID;
      AutoString pszCheckDataValue;
      oDao.getInfoLine(rw.getSessionCtx(),
          iRowkey, iRowStat, iWriteThreadID, pszCheckDataValue);

      TBSYS_LOG(INFO, "Rowkey: %ld  RowStat: %ld  WriteThreadID: %ld  "
          "CheckDataValue: %s", iRowkey, iRowStat, iWriteThreadID,
          pszCheckDataValue.get());
      EXPECT_EQ(2, iRowStat);
      EXPECT_EQ(3, iWriteThreadID);
      EXPECT_STREQ("123", pszCheckDataValue.get());

      rw.commit();

      ROTransaction ro(session_);
      ro.beginTransaction();

      oDao.getInfoLine(ro.getSessionCtx(), 1, iRowStat, iWriteThreadID,
          pszCheckDataValue);
      TBSYS_LOG(INFO, "Rowkey: %ld  RowStat: %ld  WriteThreadID: %ld  "
          "CheckDataValue: %s", 1L, iRowStat, iWriteThreadID,
          pszCheckDataValue.get());
      EXPECT_EQ(2, iRowStat);
      EXPECT_EQ(3, iWriteThreadID);
      EXPECT_STREQ("123", pszCheckDataValue.get());

      ro.commit();

      rw.beginTransaction();

      oDao.applyInfoLine(rw.getSessionCtx(), 2, 3, 4, const_cast<char*>("234"));
      oDao.getInfoLine(rw.getSessionCtx(), 2, iRowStat,
          iWriteThreadID, pszCheckDataValue);
      TBSYS_LOG(INFO, "Rowkey: %ld  RowStat: %ld  WriteThreadID: %ld  "
          "CheckDataValue: %s", 2L, iRowStat, iWriteThreadID,
          pszCheckDataValue.get());
      EXPECT_EQ(3, iRowStat);
      EXPECT_EQ(4, iWriteThreadID);
      EXPECT_STREQ("234", pszCheckDataValue.get());

      rw.commit();

      rw.beginTransaction();

      int iThreadID = 0;
      oDao.applyDataLine(rw.getSessionCtx(), 10, iThreadID, 11, 12, 13);
      iThreadID = 1;
      oDao.applyDataLine(rw.getSessionCtx(), 10, iThreadID, 21, 22, 23);

      rw.commit();

      ro.beginTransaction();

      iThreadID = 0;
      int64_t iSelfCheck1;
      int64_t iSelfCheck2;
      int64_t iMultiCheck;

      oDao.getDataLine(ro.getSessionCtx(), 10, iThreadID, iSelfCheck1,
          iMultiCheck);
      TBSYS_LOG(INFO, "iSelfCheck1: %ld  iSelfCheck2: %ld  iMultiCheck: %ld",
              iSelfCheck1, iSelfCheck2, iMultiCheck);
      EXPECT_EQ(21, iSelfCheck1);
      EXPECT_EQ(13, iMultiCheck);

      AutoArray<int64_t> aiMultiChecks;
      int64_t iMultiCheckNum;
      oDao.getDataLine(ro.getSessionCtx(), 10, iSelfCheck2, aiMultiChecks,
          iMultiCheckNum);
      TBSYS_LOG(INFO, "iSelfCheck2: %ld  iMultiCheckNum: %ld  iMultiCheck0: %ld  "
              "iMultiCheck1: %ld", iSelfCheck2, iMultiCheckNum,
              aiMultiChecks[0], aiMultiChecks[1]);
      EXPECT_EQ(22, iSelfCheck2);
      EXPECT_EQ(13, aiMultiChecks[0]);
      EXPECT_EQ(23, aiMultiChecks[1]);

      ro.commit();

      rw.beginTransaction();

      oDao.applyDataLine(rw.getSessionCtx(), 10, 101);
      oDao.getDataLine(rw.getSessionCtx(), 10, iSelfCheck2, aiMultiChecks,
          iMultiCheckNum);
      TBSYS_LOG(INFO, "iSelfCheck2: %ld  iMultiCheckNum: %ld  iMultiCheck0: %ld  "
              "iMultiCheck1: %ld", iSelfCheck2, iMultiCheckNum,
              aiMultiChecks[0], aiMultiChecks[1]);
      EXPECT_EQ(101, iSelfCheck2);
      EXPECT_EQ(13, aiMultiChecks[0]);
      EXPECT_EQ(23, aiMultiChecks[1]);

      rw.commit();

      return 0;
    }

    void TestStart(ApplierVector & appliers, CheckerVector & checkers)
    {
      int iThreadID = 0;
      for (ApplierVector::iterator iter = appliers.begin();
          iter != appliers.end(); iter++)
      {
        iter->setThreadID(iThreadID++);
        iter->start();
      }
      iThreadID = 0;
      for (CheckerVector::iterator iter = checkers.begin();
          iter != checkers.end(); iter++)
      {
        iter->setThreadID(iThreadID++);
        iter->start();
      }
    }

    bool TestCheckRunning(CheckerVector & checkers)
    {
      bool bRunning = false;
      for (CheckerVector::iterator iter = checkers.begin();
          iter != checkers.end(); iter++)
      {
        if (OB_SUCCESS == iter->getErr())
        {
          bRunning = true;
        }
      }
      return bRunning;
    }

    void TestStop(ApplierVector & appliers, CheckerVector & checkers)
    {
      for (ApplierVector::iterator iter = appliers.begin();
          iter != appliers.end(); iter++)
      {
        iter->stop();
      }
      for (CheckerVector::iterator iter = checkers.begin();
          iter != checkers.end(); iter++)
      {
        iter->stop();
      }
      for (ApplierVector::iterator iter = appliers.begin();
          iter != appliers.end(); iter++)
      {
        iter->wait();
        EXPECT_EQ(OB_SUCCESS, iter->getErr());
      }
      for (CheckerVector::iterator iter = checkers.begin();
          iter != checkers.end(); iter++)
      {
        iter->wait();
        EXPECT_EQ(OB_SUCCESS, iter->getErr());
      }
    }

    void TestChangeMultiplier()
    {
      CONST::InfoRowkeyMultiplier = S.getRandom().rand(CONST::InfoMaxRowkeyMultiplier);
      CONST::DataRowkeyMultiplier = S.getRandom().rand(CONST::DataMaxRowkeyMultiplier);
      //CONST::InfoRowkeyMultiplier = 1;
      //CONST::DataRowkeyMultiplier = 1;
    }

    TEST(UPS_MVCC, apply) {
      //TestInsert(S.getSessionMgr(), S.getLockMgr(), S.getStorage());
      ApplierVector appliers(CONST::GetApplierNum(),
          ApplyThread(S.getQueue(), S.getSessionMgr(), S.getLockMgr(),
                      S.getStorage(), S.getCopy(), 0, S.getCommitMutex(),
                      S.getRandom()));
      CheckerVector checkers(CONST::GetCheckerNum(),
          CheckThread(S.getQueue(), S.getSessionMgr(), S.getLockMgr(),
                      S.getStorage(), S.getCopy(), S.getCommitMutex(),
                      S.getRandom()));
      TestStart(appliers, checkers);
      int64_t iStartTimestamp = tbsys::CTimeUtil::getTime();
      MemTable & oMemTable = S.getStorage().get_table_mgr()->get_active_memtable()->get_memtable();
      while ((CONST::GetRunningTime() == 0
             || tbsys::CTimeUtil::getTime() - iStartTimestamp
                 < CONST::GetRunningTime() * 1000000)
          && oMemTable.btree_size() < CONST::getMaxLineNum())
      {
        TBSYS_LOG(INFO, "=================");
        int64_t total = oMemTable.btree_alloc_memory();
        int64_t reserved = oMemTable.btree_reserved_memory();
        TBSYS_LOG(INFO, "TEST IS RUNNING. BTREE LINE NUMBER: %-10ld  MEM: %-12ld  "
            "RESV: %-12ld  %%: %ld",
            oMemTable.btree_size(), total, reserved, reserved * 100 / total);
        TBSYS_LOG(INFO, "=================");
        if (!TestCheckRunning(checkers))
        {
          break;
        }
        sleep(1);
        TestChangeMultiplier();
      }
      TestStop(appliers, checkers);
      TBSYS_LOG(INFO, "=================");
      int64_t total = oMemTable.btree_alloc_memory();
      int64_t reserved = oMemTable.btree_reserved_memory();
      TBSYS_LOG(INFO, "TEST IS FINISHED. BTREE LINE NUMBER: %-10ld  MEM: %-12ld  "
          "RESV: %-12ld  %%: %ld",
          oMemTable.btree_size(), total, reserved, reserved * 100 / total);
      TBSYS_LOG(INFO, "=================");
    }

  } // end namespace updateserver
} // end namespace oceanbase

using namespace oceanbase::test;

void printUsage(char * pszProgName)
{
  fprintf(stderr, "\nUsage: %s [-c Core_Num]\n\n", pszProgName);
}

void parseArgs(int argc, char* argv[])
{
  int opt = 0;
  while (-1 != (opt = getopt(argc, argv, "c:n:t:h")))
  {
    switch (opt)
    {
      case 'c':
        CONST::CoreNum = atoll(optarg);
        break;
      case 'n':
        CONST::MaxLineNum = atoll(optarg);
        break;
      case 't':
        CONST::RunningTime = atoll(optarg);
        break;
      case 'h':
        printUsage(argv[0]);
        exit(0);
      default:
        fprintf(stderr, "unknown argument");
        printUsage(argv[0]);
        THROW_EXCEPTION_AND_LOG(OB_ERROR, "unknown argument");
    }
  }
}

int main(int argc, char* argv[])
{
  TBSYS_LOG(INFO, "start main");
  ::testing::InitGoogleTest(&argc, argv);
  int err = OB_SUCCESS;
  //TBSYS_LOGGER.setFileName("test_ups_mvcc.log");
  TBSYS_LOGGER.setLogLevel("info");
  if (OB_SUCCESS != (err = ob_init_memory_pool())) return err;
  try
  {
    parseArgs(argc, argv);
    return RUN_ALL_TESTS();
  }
  catch (ErrMsgException & ex)
  {
    fprintf(stderr, "Exception: %s\n", ex.what());
    ex.logException();
    return 1;
  }
}
