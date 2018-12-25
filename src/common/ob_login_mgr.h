////===================================================================
 //
 // ob_login_mgr.h common / Oceanbase
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
#ifndef  OCEANBASE_COMMON_LOGIN_MGR_H_
#define  OCEANBASE_COMMON_LOGIN_MGR_H_
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
#include "common/ob_perm_components.h"

namespace oceanbase
{
  namespace common
  {
    class ObLoginInfo
    {
      static const int64_t CUR_VERSION = 1;
      static const int64_t MAGIC_NUM = 0x6d675f6c6f67696e; // "mg_login"
      public:
        ObLoginInfo();
        ~ObLoginInfo();
      public:
        int serialize(char* buf, const int64_t buf_len, int64_t& pos) const;
        int deserialize(const char* buf, const int64_t data_len, int64_t& pos);
        int64_t get_serialize_size(void) const;
      public:
        int set_username(const ObString &username);
        int set_password(const ObString &password);
        const ObString &get_username() const;
        const ObString &get_password() const;
      public:
        const char *log_str() const;
      private:
        char username_buf_[OB_MAX_USERNAME_LENGTH];
        char password_buf_[OB_MAX_CIPHER_LENGTH];
        ObString username_str_;
        ObString password_str_;
    };

    class ObLoginMgr
    {
      static const uint64_t LOW_INT32_MASK = 0x00000000ffffffff;
      static const int64_t TOKEN_EXPIRED_TIME = 1 * 60; // 1 minute
      public:
        ObLoginMgr();
        ~ObLoginMgr();
      public:
        int init(ISKeyTable *skey_table, IUserTable *user_table);
        void destroy();
      public:
        int handle_login(const ObLoginInfo &login_info, IToken &token);
        int check_token(char *buf, const int64_t data_len, int64_t &pos, IToken &token);
      private:
        int set_timestamp(IToken &token, int64_t &timestamp);
        int check_timestamp(IToken &token);
      private:
        bool inited_;
        ISKeyTable *skey_table_;
        IUserTable *user_table_;
    };
  }
}

#endif //OCEANBASE_COMMON_LOGIN_MGR_H_

