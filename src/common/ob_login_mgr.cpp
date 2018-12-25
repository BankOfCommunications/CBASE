////===================================================================
 //
 // ob_login_mgr.cpp common / Oceanbase
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
#include "common/ob_login_mgr.h"

namespace oceanbase
{
  namespace common
  {
    ObLoginInfo::ObLoginInfo() : username_str_(OB_MAX_USERNAME_LENGTH, 0, username_buf_),
                                 password_str_(OB_MAX_CIPHER_LENGTH, 0, password_buf_)
    {
    }

    ObLoginInfo::~ObLoginInfo()
    {
    }

    int ObLoginInfo::set_username(const ObString &username)
    {
      int ret = OB_SUCCESS;
      if (NULL == username.ptr()
          || 0 == username.length()
          || username_str_.size() < username.length())
      {
        TBSYS_LOG(WARN, "invalid param ptr=%p length=%d", username.ptr(), username.length());
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        username_str_.assign_buffer(username_str_.ptr(), username_str_.size());
        username_str_.write(username.ptr(), username.length());
      }
      return ret;
    }

    int ObLoginInfo::set_password(const ObString &password)
    {
      int ret = OB_SUCCESS;
      if (NULL == password.ptr()
          || 0 == password.length()
          || password_str_.size() < password.length())
      {
        TBSYS_LOG(WARN, "invalid param ptr=%p length=%d", password.ptr(), password.length());
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        password_str_.assign_buffer(password_str_.ptr(), password_str_.size());
        password_str_.write(password.ptr(), password.length());
      }
      return ret;
    }

    const ObString &ObLoginInfo::get_username() const
    {
      return username_str_;
    }

    const ObString &ObLoginInfo::get_password() const
    {
      return password_str_;
    }

    int ObLoginInfo::serialize(char* buf, const int64_t buf_len, int64_t& pos) const
    {
      int ret = OB_SUCCESS;
      int64_t cur_pos = pos;
      if (NULL == buf
          || 0 >= buf_len
          || 0 > pos
          || (pos + this->get_serialize_size()) > buf_len)
      {
        ret = OB_BUF_NOT_ENOUGH;
      }
      else if (OB_SUCCESS != (ret = serialization::encode_i64(buf, buf_len, cur_pos, MAGIC_NUM)))
      {
        TBSYS_LOG(WARN, "encode MAGIC_NUM fail ret=%d buf=%p buf_len=%ld cur_pos=%ld", ret, buf, buf_len, cur_pos);
      }
      else if (OB_SUCCESS != (ret = serialization::encode_i64(buf, buf_len, cur_pos, CUR_VERSION)))
      {
        TBSYS_LOG(WARN, "encode CUR_VERSION fail ret=%d buf=%p buf_len=%ld cur_pos=%ld", ret, buf, buf_len, cur_pos);
      }
      else if (OB_SUCCESS != (ret = username_str_.serialize(buf, buf_len, cur_pos)))
      {
        TBSYS_LOG(WARN, "encode username fail ret=%d buf=%p buf_len=%ld cur_pos=%ld", ret, buf, buf_len, cur_pos);
      }
      else if (OB_SUCCESS != (ret = password_str_.serialize(buf, buf_len, cur_pos)))
      {
        TBSYS_LOG(WARN, "encode password fail ret=%d buf=%p buf_len=%ld cur_pos=%ld", ret, buf, buf_len, cur_pos);
      }
      else
      {
        pos = cur_pos;
      }
      return ret;
    }

    int ObLoginInfo::deserialize(const char* buf, const int64_t data_len, int64_t& pos)
    {
      int ret = OB_SUCCESS;
      int64_t cur_pos = pos;
      int64_t magic_num = 0;
      int64_t version = 0;
      username_str_.assign_buffer(username_str_.ptr(), username_str_.size());
      password_str_.assign_buffer(password_str_.ptr(), password_str_.size());
      if (NULL == buf
          || 0 >= data_len
          || 0 > pos
          || pos >= data_len)
      {
        ret = OB_BUF_NOT_ENOUGH;
      }
      else if (OB_SUCCESS != (ret = serialization::decode_i64(buf, data_len, cur_pos, &magic_num)))
      {
        TBSYS_LOG(WARN, "decode magic_num fail ret=%d buf=%p data_len=%ld cur_pos=%ld", ret, buf, data_len, cur_pos);
      }
      else if (MAGIC_NUM != magic_num)
      {
        ret = OB_ENTRY_NOT_EXIST;
      }
      else if (OB_SUCCESS != (ret = serialization::decode_i64(buf, data_len, cur_pos, &version)))
      {
        TBSYS_LOG(WARN, "decode version fail ret=%d buf=%p data_len=%ld cur_pos=%ld", ret, buf, data_len, cur_pos);
      }
      else if (CUR_VERSION != version)
      {
        TBSYS_LOG(WARN, "invalid version=%ld buf=%p data_len=%ld cur_pos=%ld", version, buf, data_len, cur_pos);
        ret = OB_ERROR;
      }
      else if (OB_SUCCESS != (ret = username_str_.deserialize(buf, data_len, cur_pos)))
      {
        TBSYS_LOG(WARN, "decode username fail ret=%d buf=%p data_len=%ld cur_pos=%ld", ret, buf, data_len, cur_pos);
      }
      else if (OB_SUCCESS != (ret = password_str_.deserialize(buf, data_len, cur_pos)))
      {
        TBSYS_LOG(WARN, "decode password fail ret=%d buf=%p data_len=%ld cur_pos=%ld", ret, buf, data_len, cur_pos);
      }
      else
      {
        pos = cur_pos;
      }
      return ret;
    }

    int64_t ObLoginInfo::get_serialize_size(void) const
    {
      int64_t ret = 0;
      ret += serialization::encoded_length_i64(MAGIC_NUM);
      ret += serialization::encoded_length_i64(CUR_VERSION);
      ret += username_str_.get_serialize_size();
      ret += password_str_.get_serialize_size();
      return ret;
    }

    const char *ObLoginInfo::log_str() const
    {
      static const int64_t BUFFER_SIZE = 128;
      static __thread char buffers[2][BUFFER_SIZE];
      static __thread int64_t i = 0;
      char *buffer = buffers[i++ % 2];
      buffer[0] = '\0';
      snprintf(buffer, BUFFER_SIZE, "version=%ld username=[%.*s] password=[%.*s]",
              CUR_VERSION, username_str_.length(), username_str_.ptr(), password_str_.length(), password_str_.ptr());
      return buffer;
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////

    ObLoginMgr::ObLoginMgr() : inited_(false),
                               skey_table_(NULL),
                               user_table_(NULL)
    {
    }

    ObLoginMgr::~ObLoginMgr()
    {
      destroy();
    }

    int ObLoginMgr::init(ISKeyTable *skey_table, IUserTable *user_table)
    {
      int ret = OB_SUCCESS;
      if (inited_)
      {
        TBSYS_LOG(WARN, "have already inited this=%p", this);
        ret = OB_INIT_TWICE;
      }
      else if (NULL == skey_table
              || NULL == user_table)
      {
        TBSYS_LOG(WARN, "invalid param skey_table=%p user_table=%p", skey_table, user_table);
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        skey_table_ = skey_table;
        user_table_ = user_table;
        inited_ = true;
      }
      return ret;
    }

    void ObLoginMgr::destroy()
    {
      if (inited_)
      {
        // do nothing
      }
    }

    int ObLoginMgr::handle_login(const ObLoginInfo &login_info, IToken &token)
    {
      int ret = OB_SUCCESS;
      ObString username = login_info.get_username();
      ObString password = login_info.get_password();
      int64_t skey_version = 0;
      int64_t skey = 0;
      int64_t timestamp = 0;
      if (!inited_)
      {
        TBSYS_LOG(WARN, "have not inited this=%p", this);
        ret = OB_NOT_INIT;
      }
      else if (OB_SUCCESS != (ret = user_table_->userinfo_check(username, password)))
      {
        TBSYS_LOG(WARN, "user info check fail ret=%d %s", ret, login_info.log_str());
      }
      else if (OB_SUCCESS != (ret = skey_table_->get_cur_skey(skey_version, skey)))
      {
        TBSYS_LOG(WARN, "get cur skey fail ret=%d", ret);
      }
      else if (OB_SUCCESS != (ret = token.set_username(username)))
      {
        TBSYS_LOG(WARN, "set username to token fail ret=%d username=%.*s", ret, username.length(), username.ptr());
      }
      else if (OB_SUCCESS != (ret = set_timestamp(token, timestamp)))
      {
        TBSYS_LOG(WARN, "set timestamp to token fail ret=%d", ret);
      }
      else if (OB_SUCCESS != (ret = token.encrypt(skey_version, skey)))
      {
        TBSYS_LOG(WARN, "encrypt token fail ret=%d skey_version=%ld", ret, skey_version);
      }
      else
      {
        TBSYS_LOG(INFO, "login succ %s skey_version=%ld timstamp=%ld", login_info.log_str(), skey_version, timestamp);
      }
      return ret;
    }

    int ObLoginMgr::check_token(char *buf, const int64_t data_len, int64_t &pos, IToken &token)
    {
      int ret = OB_SUCCESS;
      int64_t skey = 0;
      if (!inited_)
      {
        TBSYS_LOG(WARN, "have not inited this=%p", this);
        ret = OB_NOT_INIT;
      }
      else if (OB_SUCCESS != (ret = token.deserialize(buf, data_len, pos)))
      {
        //TBSYS_LOG(WARN, "deserialize token fail ret=%d", ret);
      }
      else if (OB_SUCCESS != (ret = skey_table_->get_skey(token.get_skey_version(), skey)))
      {
        TBSYS_LOG(WARN, "get skey fail ret=%d skey_version=%ld", ret, token.get_skey_version());
      }
      else if (OB_SUCCESS != (ret = token.decrypt(skey)))
      {
        TBSYS_LOG(WARN, "decrypt token fail ret=%d", ret);
      }
      else if (OB_SUCCESS != (ret = check_timestamp(token)))
      {
        TBSYS_LOG(WARN, "check timestamp fail ret=%d", ret);
      }
      else
      {
        // check succ
      }
      return ret;
    }

    int ObLoginMgr::set_timestamp(IToken &token, int64_t &timestamp)
    {
      int ret = OB_SUCCESS;
      uint64_t local_ip = tbsys::CNetUtil::getLocalAddr(NULL);
      int64_t time_sec = tbsys::CTimeUtil::getTime() / 1000000;
      timestamp = (time_sec << 32) | local_ip;
      ret = token.set_timestamp(timestamp);
      return ret;
    }

    int ObLoginMgr::check_timestamp(IToken &token)
    {
      int ret = OB_SUCCESS;
      int64_t timestamp = 0;
      if (OB_SUCCESS != (ret = token.get_timestamp(timestamp)))
      {
        TBSYS_LOG(WARN, "get timestamp from token fail ret=%d", ret);
      }
      else
      {
        uint64_t builder_ip = ((uint64_t)timestamp) & LOW_INT32_MASK;
        UNUSED(builder_ip);
        int64_t time_sec = ((uint64_t)timestamp >> 32) & LOW_INT32_MASK;
        if (((time_sec + TOKEN_EXPIRED_TIME) * 1000000) < tbsys::CTimeUtil::getTime())
        {
          TBSYS_LOG(WARN, "timestamp=%ld time_sec=%ld cur_time=%ld timeout=%ld",
                    timestamp, time_sec, tbsys::CTimeUtil::getTime(), TOKEN_EXPIRED_TIME);
          ret = OB_TOKEN_EXPIRED;
        }
      }
      return ret;
    }
  }
}

