////===================================================================
 //
 // ob_cipher.h common / Oceanbase
 //
 // Copyright (C) 2010 Taobao.com, Inc.
 //
 // Created on 2011-01-14 by Yubai (yubai.lk@taobao.com) 
 //
 // -------------------------------------------------------------------
 //
 // Description
 //
 // -------------------------------------------------------------------
 // 
 // Change Log
 //
////====================================================================
#ifndef  OCEANBASE_COMMON_CIPHER_H_
#define  OCEANBASE_COMMON_CIPHER_H_
#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <openssl/md5.h>
#include "common/ob_define.h"
#include "common/ob_string.h"

namespace oceanbase
{
  namespace common
  {
    class ObCipher
    {
      public:
        ObCipher() {};
        ~ObCipher() {};
      public:
        inline ObString get_cipher(const ObString &input)
        {
          ObString ret;
          char binary[MD5_DIGEST_LENGTH];
          if (NULL != input.ptr()
              && 0 <= input.length()
              && NULL != MD5((unsigned char*)input.ptr(), input.length(), (unsigned char*)binary))
          {
            hex_to_str(binary, MD5_DIGEST_LENGTH, buffer_, OB_MAX_CIPHER_LENGTH);
            ret.assign_ptr(buffer_, OB_MAX_CIPHER_LENGTH);
          }
          return ret;
        };
        inline ObString get_cipher(const char *input)
        {
          ObString ret;
          char binary[MD5_DIGEST_LENGTH];
          if (NULL != input
              && NULL != MD5((unsigned char*)input, strlen(input), (unsigned char*)binary))
          {
            hex_to_str(binary, MD5_DIGEST_LENGTH, buffer_, OB_MAX_CIPHER_LENGTH);
            ret.assign_ptr(buffer_, OB_MAX_CIPHER_LENGTH);
          }
          return ret;
        };
      private:
        char buffer_[OB_MAX_CIPHER_LENGTH + 1];
    };
  }
}

#endif //OCEANBASE_COMMON_CIPHER_H_

