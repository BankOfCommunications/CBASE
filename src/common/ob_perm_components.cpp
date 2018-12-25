////===================================================================
 //
 // ob_perm_components.cpp common / Oceanbase
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
#include <algorithm>
#include "common/ob_define.h"
#include "common/page_arena.h"
#include "common/ob_malloc.h"
#include "common/ob_mod_define.h"
#include "common/ob_string.h"
#include "common/ob_perm_components.h"
#include "common/ob_meta_cache.h"
#include "common/ob_schema.h"
#include "common/utility.h"
#include "common/ob_cipher.h"

namespace oceanbase
{
  namespace common
  {
    static const char *SUPPER_ACCOUNT = "root";
    static const char *SUPPER_PASSWORD = "hello1234";

    int supper_account_check(const ObString &username, const ObString &password)
    {
      int ret = OB_SUCCESS;
      ObString supper_account;
      supper_account.assign_ptr(const_cast<char*>(SUPPER_ACCOUNT), static_cast<int32_t>(strlen(SUPPER_ACCOUNT)));
      if (username != supper_account)
      {
        ret = OB_USER_NOT_EXIST;
      }
      else
      {
        ObCipher cipher;
        ObString supper_password = cipher.get_cipher(SUPPER_PASSWORD);
        if (password != supper_password)
        {
          ret = OB_PASSWORD_WRONG;
        }
      }
      return ret;
    }

    bool is_supper_account(const ObString &username)
    {
      bool bret = false;
      ObString supper_account;
      supper_account.assign_ptr(const_cast<char*>(SUPPER_ACCOUNT), static_cast<int32_t>(strlen(SUPPER_ACCOUNT)));
      if (username == supper_account)
      {
        bret = true;
      }
      else
      {
        //TBSYS_LOG(WARN, "[%.*s] not supper account [%.*s]",
        //          username.length(), username.ptr(), supper_account.length(), supper_account.ptr());
      }
      return bret;
    }
    
    bool is_supper_table(const uint64_t table_id)
    {
      bool bret = false;
      if (PERM_TABLE_ID == table_id
          || USER_TABLE_ID == table_id
          || SKEY_TABLE_ID == table_id)
      {
        bret = true;
      }
      return bret;
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////

    ObUserTable::ObUserTable() : cache_()
    {
    }

    ObUserTable::~ObUserTable()
    {
    }

    int ObUserTable::init(IRpcStub *rpc_stub,
                          const int64_t cache_timeout,
                          const int64_t max_cache_num)
    {
      int ret = OB_SUCCESS;
      ret = cache_.init(cache_timeout, rpc_stub, max_cache_num);
      return ret;
    }

    void ObUserTable::destroy()
    {
      cache_.destroy();
    }

    int ObUserTable::userinfo_check(const ObString &username, const ObString &password) const
    {
      int ret = OB_SUCCESS;
      ObUserTableRefGuard ref_guard;
      ObUserInfoKey key;
      ObUserInfoValue value;
      key.set_user_name(username);
      if (OB_SUCCESS != (ret = const_cast<ObUserTableCache&>(cache_).get(key, value, RPC_TIMEOUT, ref_guard)))
      {
        if (OB_ENTRY_NOT_EXIST == ret)
        {
          ret = supper_account_check(username, password);
        }
        else
        {
          TBSYS_LOG(WARN, "get from cache fail ret=%d username=[%.*s]", ret, username.length(), username.ptr());
          ret = (OB_ENTRY_NOT_EXIST == ret) ? OB_USER_NOT_EXIST : ret;
        }
      }
      else if (password != value.get_passwd())
      {
        //TBSYS_LOG(WARN, "password wrong [%.*s] [%.*s]", password.length(), password.ptr(), value.get_passwd().length(), value.get_passwd().ptr());
        ret = OB_PASSWORD_WRONG;
      }
      else
      {
        // pass check
      }
      return ret;
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////

    ObSKeyTable::ObSKeyTable() : cache_()
    {
    }

    ObSKeyTable::~ObSKeyTable()
    {
    }

    int ObSKeyTable::init(IRpcStub *rpc_stub,
                          const int64_t cache_timeout,
                          const int64_t max_cache_num)
    {
      int ret = OB_SUCCESS;
      ret = cache_.init(cache_timeout, rpc_stub, max_cache_num);
      return ret;
    }

    void ObSKeyTable::destroy()
    {
      cache_.destroy();
    }

    int ObSKeyTable::get_skey(const int64_t version, int64_t &skey) const
    {
      int ret = OB_SUCCESS;
      ObSKeyTableRefGuard ref_guard;
      ObSKeyInfoKey key;
      ObSKeyInfoValue value;
      key.set_skey_version(version);
      if (OB_SUCCESS != (ret = const_cast<ObSKeyTableCache&>(cache_).get(key, value, RPC_TIMEOUT, ref_guard)))
      {
        TBSYS_LOG(WARN, "get from cache fail ret=%d version=%ld", ret, version);
        ret = (OB_ENTRY_NOT_EXIST == ret) ? OB_SKEY_VERSION_WRONG : ret;
      }
      else if (NULL == value.get_skey().ptr()
              || SKEY_INFO_LENGTH > value.get_skey().length())
      {
        TBSYS_LOG(WARN, "invalid skey_info ptr=%p length=%d", value.get_skey().ptr(), value.get_skey().length());
        ret = OB_ERROR;
      }
      else
      {
        SKeyInfo skey_info;
        memcpy(&skey_info, value.get_skey().ptr(), SKEY_INFO_LENGTH);
        skey = skey_info.skey;
      }
      return ret;
    }

    int ObSKeyTable::get_cur_skey(int64_t &version, int64_t &skey) const
    {
      int ret = OB_SUCCESS;
      ObSKeyTableRefGuard ref_guard;
      ObSKeyInfoKey key;
      ObSKeyInfoValue value;
      key.set_skey_version(CUR_VERSION_KEY);
      if (OB_SUCCESS != (ret = const_cast<ObSKeyTableCache&>(cache_).get(key, value, RPC_TIMEOUT, ref_guard)))
      {
        TBSYS_LOG(WARN, "get cur_version from cache fail ret=%d", ret);
      }
      else if (NULL == value.get_skey().ptr()
              || CUR_SKEY_INFO_LENGTH > value.get_skey().length())
      {
        TBSYS_LOG(WARN, "invalid cur_skey_info ptr=%p length=%d %.*s",
                  value.get_skey().ptr(), value.get_skey().length(),
                  value.get_skey().length(), value.get_skey().ptr());
        ret = OB_ERROR;
      }
      else
      {
        CurSKeyInfo cur_skey_info;
        memcpy(&cur_skey_info, value.get_skey().ptr(), CUR_SKEY_INFO_LENGTH);
        version = cur_skey_info.version;
        skey = cur_skey_info.skey;
      }
      return ret;
    }

    int ObSKeyTable::update_cur_skey(ObMutator &mutator) const
    {
      int ret = OB_SUCCESS;

      int64_t timestamp = tbsys::CTimeUtil::getTime();
      CurSKeyInfo cur_skey_info;
      cur_skey_info.version = timestamp;
      cur_skey_info.skey = ~timestamp;
      SKeyInfo skey_info;
      skey_info.skey = cur_skey_info.skey;

      ObString table_name;
      ObString column_name;
      ObRowkey row_key;
      ObObj rowkey_obj;
      ObString value;
      ObObj obj;
      table_name.assign_ptr(const_cast<char*>(SKEY_TABLE_NAME), static_cast<int32_t>(strlen(SKEY_TABLE_NAME)));
      column_name.assign_ptr(const_cast<char*>(SKEY_COL_NAME), static_cast<int32_t>(strlen(SKEY_COL_NAME)));

      uint64_t version = htonll(timestamp);
      rowkey_obj.set_varchar(ObString(0, sizeof(version), (char*)&version));
      row_key.assign(&rowkey_obj, 1);
      value.assign_ptr((char*)&skey_info, sizeof(skey_info));
      obj.set_varchar(value);

      if (OB_SUCCESS != (ret = mutator.insert(table_name, row_key, column_name, obj)))
      {
        TBSYS_LOG(WARN, "mutator insert fail ret=%d", ret);
      }
      else
      {
        uint64_t cur_version_key = htonll(CUR_VERSION_KEY);
        rowkey_obj.set_varchar(ObString(0, sizeof(CUR_VERSION_KEY), (char*)&cur_version_key));
        row_key.assign(&rowkey_obj, 1);
        value.assign_ptr((char*)&cur_skey_info, sizeof(cur_skey_info));
        obj.set_varchar(value);
        if (OB_SUCCESS != (ret = mutator.update(table_name, row_key, column_name, obj)))
        {
          TBSYS_LOG(WARN, "mutator update fail ret=%d", ret);
        }
        else
        {
          TBSYS_LOG(INFO, "build mutator for skey switch succ, new version=%ld", timestamp);
        }
      }
      return ret;
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////

    ObPermTable::ObPermTable() : cache_()
    {
    }

    ObPermTable::~ObPermTable()
    {
    }

    int ObPermTable::init(IRpcStub *rpc_stub,
                          const int64_t cache_timeout,
                          const int64_t max_cache_num)
    {
      int ret = OB_SUCCESS;
      ret = cache_.init(cache_timeout, rpc_stub, max_cache_num);
      return ret;
    }

    void ObPermTable::destroy()
    {
      cache_.destroy();
    }

    int ObPermTable::check_table_writeable(const IToken &token, const uint64_t table_id)
    {
      int ret = OB_SUCCESS;
      ObString username;
      if (OB_SUCCESS != (ret = token.get_username(username)))
      {
        TBSYS_LOG(WARN, "get username from token fail ret=%d", ret);
      }
      else if (is_supper_table(table_id))
      {
        if (!is_supper_account(username))
        {
          ret = OB_NO_PERMISSION;
        }
      }
      else
      {
        ObPermTableRefGuard ref_guard;
        ObPermInfoKey key;
        ObPermInfoValue value;
        key.set_table_id(table_id);
        key.set_user_name(username);
        if (OB_SUCCESS != (ret = cache_.get(key, value, RPC_TIMEOUT, ref_guard)))
        {
          ret = (OB_ENTRY_NOT_EXIST == ret) ? OB_NO_PERMISSION : ret;
        }
        else if (0 == (value.get_perm_mask() & OB_WRITEABLE))
        {
          TBSYS_LOG(WARN, "table_id=%lu username=[%.*s] perm_mask=%ld",
                    table_id, username.length(), username.ptr(), value.get_perm_mask());
          ret = OB_NO_PERMISSION;
        }
        else
        {
          // pass check
        }
      }
      return ret;
    }
  }
}

