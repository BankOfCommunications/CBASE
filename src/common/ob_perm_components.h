////===================================================================
 //
 // ob_perm_components.h common / Oceanbase
 //
 // Copyright (C) 2010 Taobao.com, Inc.
 //
 // Created on 2011-08-16 by Yubai (yubai.lk@taobao.com) 
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
#ifndef  OCEANBASE_COMMON_PERM_COMPONENTS_H_
#define  OCEANBASE_COMMON_PERM_COMPONENTS_H_
#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <pthread.h>
#include <new>
#include <typeinfo>
#include "common/ob_define.h"
#include "common/page_arena.h"
#include "common/ob_malloc.h"
#include "common/ob_mod_define.h"
#include "common/ob_string.h"
#include "common/ob_meta_cache.h"
#include "common/ob_mutator.h"

namespace oceanbase
{
  namespace common
  {
    static const int64_t SKEY_UPDATE_PERIOD = 3L * 60L * 1000L * 1000L; // 3min
    static const int64_t PERM_CACHE_TIMEOUT = 1L * 60L * 1000L * 1000L; // 1min
    static const int64_t PERM_CACHE_MAX_ITEM_NUM = 1024;

    class ISKeyTable
    {
      public:
        ISKeyTable() {};
        virtual ~ISKeyTable() {};
      public:
        virtual int get_skey(const int64_t version, int64_t &skey) const = 0;
        virtual int get_cur_skey(int64_t &version, int64_t &skey) const = 0;
    };

    class IUserTable
    {
      public:
        IUserTable() {};
        virtual ~IUserTable() {};
      public:
        virtual int userinfo_check(const ObString &username, const ObString &password) const = 0;
    };

    class IToken
    {
      public:
        IToken() {};
        virtual ~IToken() {};
      public:
        virtual int deserialize(const char* buf, const int64_t data_len, int64_t& pos) = 0;
      public:
        virtual int set_username(const ObString &username) = 0;
        virtual int set_timestamp(const int64_t timestamp) = 0;
        virtual int get_username(ObString &username) const = 0;
        virtual int get_timestamp(int64_t &timestamp) const = 0;
      public:
        virtual int64_t get_skey_version() const = 0;
        virtual int encrypt(const int64_t skey_version, const int64_t skey) = 0;
        virtual int decrypt(const int64_t skey) = 0;
    };

    extern int supper_account_check(const ObString &username, const ObString &password);
    extern bool is_supper_account(const ObString &username);
    extern bool is_supper_table(const uint64_t table_id);

    class ObUserTable : public IUserTable
    {
      typedef ObMetaCache<ObUserInfoKey, ObUserInfoValue> ObUserTableCache;
      typedef RefGuard<ObUserInfoKey, ObUserInfoValue> ObUserTableRefGuard;
      static const int64_t RPC_TIMEOUT = 1 * 1000 * 1000; // 1s
      public:
        ObUserTable();
        ~ObUserTable();
      public:
        int init(IRpcStub *rpc_stub,
                const int64_t cache_timeout = PERM_CACHE_TIMEOUT,
                const int64_t max_cache_num = PERM_CACHE_MAX_ITEM_NUM);
        void destroy();
        int userinfo_check(const ObString &username, const ObString &password) const;
      private:
        ObUserTableCache cache_;
    };

    class ObSKeyTable : public ISKeyTable
    {
      struct SKeyInfo
      {
        int64_t skey;
      };
      struct CurSKeyInfo
      {
        int64_t version;
        int64_t skey;
      };
      typedef ObMetaCache<ObSKeyInfoKey, ObSKeyInfoValue> ObSKeyTableCache;
      typedef RefGuard<ObSKeyInfoKey, ObSKeyInfoValue> ObSKeyTableRefGuard;
      static const int64_t RPC_TIMEOUT = 1 * 1000 * 1000; // 1s
      static const int64_t CUR_VERSION_KEY = INT64_MAX;
      static const int32_t SKEY_INFO_LENGTH = sizeof(SKeyInfo);
      static const int32_t CUR_SKEY_INFO_LENGTH = sizeof(CurSKeyInfo);
      public:
        ObSKeyTable();
        ~ObSKeyTable();
      public:
        int init(IRpcStub *rpc_stub,
                const int64_t cache_timeout = PERM_CACHE_TIMEOUT,
                const int64_t max_cache_num = PERM_CACHE_MAX_ITEM_NUM);
        void destroy();
        int get_skey(const int64_t version, int64_t &skey) const;
        int get_cur_skey(int64_t &version, int64_t &skey) const;
        int update_cur_skey(ObMutator &mutator) const;
      private:
        ObSKeyTableCache cache_;
    };

    class ObPermTable
    {
      typedef ObMetaCache<ObPermInfoKey, ObPermInfoValue> ObPermTableCache;
      typedef RefGuard<ObPermInfoKey, ObPermInfoValue> ObPermTableRefGuard;
      static const int64_t RPC_TIMEOUT = 1 * 1000 * 1000; // 1s
      public:
        ObPermTable();
        ~ObPermTable();
      public:
        int init(IRpcStub *rpc_stub,
                const int64_t cache_timeout = PERM_CACHE_TIMEOUT,
                const int64_t max_cache_num = PERM_CACHE_MAX_ITEM_NUM);
        void destroy();
        int check_table_writeable(const IToken &token, const uint64_t table_id);
      private:
        ObPermTableCache cache_;
    };
  }
}

#endif //OCEANBASE_COMMON_PERM_COMPONENTS_H_

