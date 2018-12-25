////===================================================================
 //
 // ob_lock_mgr.h updateserver / Oceanbase
 //
 // Copyright (C) 2010 Taobao.com, Inc.
 //
 // Created on 2012-08-30 by Yubai (yubai.lk@taobao.com) 
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

#ifndef  OCEANBASE_UPDATESERVER_LOCK_MGR_H_
#define  OCEANBASE_UPDATESERVER_LOCK_MGR_H_
#include "common/ob_define.h"
#include "common/ob_transaction.h" 
#include "ob_table_engine.h"

namespace oceanbase
{
  namespace updateserver
  {
    class ILockInfo : public ISessionCallback
    {
      public:
        ILockInfo(const common::IsolationLevel il) : il_(il) {};
        virtual ~ILockInfo() {};
      public:
        virtual int on_trans_begin() = 0;
        virtual int on_read_begin(const TEKey &key, TEValue &value) = 0;
        virtual int on_write_begin(const TEKey &key, TEValue &value) = 0;
        //add dyr [NPU-2015009-cursor] [NPU-OB-009] 20150319:b
        virtual int force_unlock_row(const TEKey &key, TEValue &value) = 0;
        //add dyr 20150319:e
        virtual void on_trans_end() = 0;
        virtual void on_precommit_end() = 0;
        common::IsolationLevel get_isolation_level() const {return il_;};
      protected:
        const common::IsolationLevel il_;
    };

    class IRowUnlocker : public ISessionCallback
    {
      public:
        virtual ~IRowUnlocker() {};
      public:
        int cb_func(const bool rollback, void *data, BaseSessionCtx &session);
      public:
        virtual int unlock(TEValue *value, BaseSessionCtx &session) = 0;
    };

    class RowExclusiveUnlocker : public IRowUnlocker
    {
      public:
        RowExclusiveUnlocker();
        ~RowExclusiveUnlocker();
      public:
        int unlock(TEValue *value, BaseSessionCtx &session);
    };

    class LockMgr;
    class RWSessionCtx;
    class RPSessionCtx;

    class RPLockInfo : public ILockInfo // Replay lock info
    {
      public:
        RPLockInfo(RPSessionCtx &session_ctx);
        ~RPLockInfo();
      public:
        int on_trans_begin();
        int on_read_begin(const TEKey &key, TEValue &value);
        //add dyr [NPU-2015009-cursor] [NPU-OB-009] 20150319:b
        int force_unlock_row(const TEKey &key, TEValue &value);
        //add dyr 20150319:e
        int on_write_begin(const TEKey &key, TEValue &value);
        void on_trans_end();
        void on_precommit_end();
      public:
        int cb_func(const bool rollback, void *data, BaseSessionCtx &session);
      private:
        RPSessionCtx &session_ctx_;
        RowExclusiveUnlocker row_exclusive_unlocker_;
        CallbackMgr callback_mgr_;
    };

    class RCLockInfo : public ILockInfo // Read commited lock info
    {
      static const int64_t LOCK_WAIT_TIME = 10L * 1000L; // 10ms
      public:
        RCLockInfo(RWSessionCtx &session_ctx);
        ~RCLockInfo();
      public:
        int on_trans_begin();
        int on_read_begin(const TEKey &key, TEValue &value);
        //add dyr [NPU-2015009-cursor] [NPU-OB-009] 20150319:b
        int force_unlock_row(const TEKey &key, TEValue &value);
        //add dyr 20150319:e
        int on_write_begin(const TEKey &key, TEValue &value);
        void on_trans_end();
        void on_precommit_end();
      public:
        int cb_func(const bool rollback, void *data, BaseSessionCtx &session);
      private:
        RWSessionCtx &session_ctx_;
        RowExclusiveUnlocker row_exclusive_unlocker_;
        CallbackMgr callback_mgr_;
    };

    class LockMgr
    {
      public:
        LockMgr();
        ~LockMgr();
      public:
        ILockInfo *assign(const common::IsolationLevel level, BaseSessionCtx &session_ctx);
    };
  }
}

#endif //OCEANBASE_UPDATESERVER_LOCK_MGR_H_

