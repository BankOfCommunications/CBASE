////===================================================================
 //
 // ob_token.h common / Oceanbase
 //
 // Copyright (C) 2010 Taobao.com, Inc.
 //
 // Created on 2011-08-16 by rongxuan (rongxuan.lc@taobao.com) 
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
#ifndef  OCEANBASE_COMMON_TOKEN_H_
#define  OCEANBASE_COMMON_TOKEN_H_
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
#include "common/ob_perm_components.h"

namespace oceanbase
{
  namespace common
  {
    class ObToken : public IToken
    {
      public:
        ObToken();
        ~ObToken();
      public:
        int serialize(char* buf, const int64_t buf_len, int64_t& pos) const;
        int deserialize(const char* buf, const int64_t buf_len, int64_t& pos);
        int64_t get_serialize_size(void) const;
      public:
        // 设置用户名串 要求深拷贝
        int set_username(const ObString &username);
        // 设置token时间戳
        int set_timestamp(const int64_t timestamp);
        // 获取用户名串 反序列化后必须解密才能获取username
        int get_username(ObString &username) const;
        // 获取token时间戳 反序列化后必须解密才能获取token时间戳
        int get_timestamp(int64_t &timestamp) const;
      public:
        // 获取密钥的版本号
        int64_t get_skey_version() const;
        // 加密数据 加密后才能执行序列化
        int encrypt(const int64_t skey_version, const int64_t skey);
        // 解密数据 反序列化后才可以解密
        int decrypt(const int64_t skey);
      public:
        const static int64_t OB_MAGIC_NUM = 201108;
        const static int64_t OB_CELL_NUM = 2;
      private:
        ObString user_name_;
        char name_[OB_MAX_USERNAME_LENGTH];
        int64_t timestamp_;
        int64_t skey_version_;
        char buffer_[OB_MAX_TOKEN_BUFFER_LENGTH];
        int64_t buffer_length_;
    };
  }
}

#endif //OCEANBASE_COMMON_TOKEN_H_

