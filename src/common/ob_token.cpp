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
#include "ob_token.h"
#include "ob_define.h"
#include "ob_encrypt.h"
namespace oceanbase
{
  namespace common
  {
    ObToken::ObToken()
    {
      timestamp_ = 0;
      skey_version_ = 0;
      buffer_length_ = OB_MAX_TOKEN_BUFFER_LENGTH;
    }

    ObToken::~ObToken()
    {
    }

    int ObToken::set_username(const ObString &username)
    {
      int err = OB_SUCCESS;
      if(username.ptr() == NULL)
      {
        err = OB_INVALID_ARGUMENT;
        TBSYS_LOG(WARN, "invlid username. err=%d", err);
      }
      if(OB_SUCCESS == err)
      {
        int32_t length = username.length();
        user_name_.assign_buffer(name_, OB_MAX_USERNAME_LENGTH);
        int32_t size = user_name_.write(username.ptr(), length);
        if(size != length)
        {
          err = OB_ERROR;
          TBSYS_LOG(WARN, "fail to write data to username");
        }
      }
      return err;
    }

    /*    int ObToken::set_password(const ObString &password)
          {
          int err = OB_SUCCESS;
          if(password.ptr() == NULL)
          {
          err = OB_INVALID_ARGUMENT;
          TBSYS_LOG(WARN, "invlid password. err=%d", err);
          }
          if(OB_SUCCESS == err)
          {
          int32_t length = strlen(password.ptr()) + 1;
          char * buf = new char (length) ;
          if(buf == NULL)
          {
          err = OB_ALLOCATE_MEMORY_FAILED;
          TBSYS_LOG(WARN, "fail to allocate memory. err=%d", err);
          }
          else
          {
          password_.assign_buffer(buf, length);
          int32_t size = password_.write(password.ptr(), length);
          if(size != length)
          {
          err = OB_ERROR;
          TBSYS_LOG(WARN, "fail to write data to password");
          }
          }
          }
          return err;
          }
     */
    int ObToken::set_timestamp(const int64_t timestamp)
    {
      int err = OB_SUCCESS;
      if(timestamp <= 0)
      {
        err = OB_INVALID_ARGUMENT;
        TBSYS_LOG(WARN, "invalid argument. err=%d", err);
      }
      if(err == OB_SUCCESS)
      {
        timestamp_ = timestamp;
      }
      return err;
    }

    int ObToken::get_username(ObString &username) const
    {
      int err = OB_SUCCESS;
      username.assign_ptr(const_cast<char*>(user_name_.ptr()), user_name_.length());
      return err;
    }
    /*
       int ObToken::get_password(ObString &password) const
       {
       int err = OB_SUCCESS;
       password.assign_ptr(const_cast<char *>(password_.ptr()), password_.length());
       return err;
       }
     */
    int ObToken::get_timestamp(int64_t &timestamp) const
    {
      int err = OB_SUCCESS;
      timestamp = timestamp_;
      return err;
    }

    int64_t ObToken::get_skey_version() const
    {
      return skey_version_;
    }

    int ObToken::encrypt(const int64_t skey_version, const int64_t skey)
    {
      int err = OB_SUCCESS;
      DES_crypt crypt;
      if(err == OB_SUCCESS)
      {
        skey_version_ = skey_version;
        err = crypt.des_encrypt(user_name_, timestamp_, skey, buffer_, buffer_length_);
        if(err != OB_SUCCESS)
        {
          TBSYS_LOG(WARN, "fail to encrypt. err=%d", err);
        }
      }
      return err;	  
    }
    int ObToken::serialize(char* buf, const int64_t buf_len, int64_t& pos) const
    {
      int err = OB_SUCCESS;
      if(buf == NULL || buf_len <= 0 || pos >= buf_len)
      {
        err = OB_INVALID_ARGUMENT;
        TBSYS_LOG(WARN, "invalid argument. err=%d", err);
      }
      if(OB_SUCCESS == err)
      {
        if(buffer_ == NULL)
        {
          err = OB_INNER_STAT_ERROR;
          TBSYS_LOG(WARN, "data have't encrypt. err=%d", err);
        }
      }
      if(OB_SUCCESS == err)
      {
        if((pos + 4 * static_cast<int64_t>(sizeof(int64_t)) + buffer_length_) >= buf_len)
        {
          err = OB_ERROR;
          TBSYS_LOG(WARN, "buf is not enough, pos=%ld, buf_len=%ld", pos, buf_len);
        }
      }
      if(OB_SUCCESS == err)
      {
        *(reinterpret_cast<int64_t*>(buf + pos)) = OB_MAGIC_NUM;
        pos += sizeof(int64_t);
        *(reinterpret_cast<int64_t*>(buf + pos)) = OB_CELL_NUM;
        pos += sizeof(int64_t);
        *(reinterpret_cast<int64_t*>(buf + pos)) = skey_version_;
        pos += sizeof(int64_t);
        *(reinterpret_cast<int64_t*>(buf + pos)) = buffer_length_;
        pos += sizeof(int64_t);
        memcpy(buf + pos, buffer_, buffer_length_);	
        pos += buffer_length_;	   
      }
      return err;
    }

    int ObToken::deserialize(const char* buf, const int64_t buf_len, int64_t& pos)
    {
      int err = OB_SUCCESS;
      if(buf == NULL || buf_len <= 0 || pos >= buf_len)
      {
        err = OB_INVALID_ARGUMENT;
        TBSYS_LOG(WARN, "invalid argument. err=%d", err);
      }
      if(OB_SUCCESS == err)
      {
        int64_t magic_num = *(reinterpret_cast<const int64_t*>(buf + pos));
        if(magic_num != OB_MAGIC_NUM)
        {
          err = OB_NOT_A_TOKEN;
          //TBSYS_LOG(INFO, "not have teken, err=%d", err);
        }
      }
      if(OB_SUCCESS == err)
      {
        pos += sizeof(int64_t);
        int64_t cell_num = *(reinterpret_cast<const int64_t*>(buf + pos));
        if(cell_num != OB_CELL_NUM)
        {
          err = OB_TOKEN_EXPIRED;
          TBSYS_LOG(INFO, "token timeout, should renew it");
        }
      }
      if(OB_SUCCESS == err)
      {
        pos += sizeof(int64_t);
        skey_version_ = *(reinterpret_cast<const int64_t*>(buf + pos));
        pos += sizeof(int64_t);
        buffer_length_ = *(reinterpret_cast<const int64_t*>(buf + pos));
        pos += sizeof(int64_t);
      }
      if(OB_SUCCESS == err)
      {
        memcpy(buffer_, buf + pos, buffer_length_);
        pos += buffer_length_;
      }
      return err;
    }
    int ObToken::decrypt(const int64_t skey)
    {
      int err = OB_SUCCESS;
      if(buffer_ == NULL)
      {
        err = OB_INNER_STAT_ERROR;
        TBSYS_LOG(WARN, "inner stat error.");
      }
      else
      {
        DES_crypt crypt;
        err = crypt.des_decrypt(user_name_, timestamp_, skey, buffer_, buffer_length_);
        if(err != OB_SUCCESS)
        {
          TBSYS_LOG(WARN, "decrypt fail .err=%d", err);
        }
      }
      return err;
    }

    int64_t ObToken::get_serialize_size(void) const
    {
      return 4 * static_cast<int64_t>(sizeof(int64_t)) + buffer_length_; 
    }
  }
}

