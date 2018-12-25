////===================================================================
 //
 // ob_trans_buffer.cpp / hash / common / Oceanbase
 //
 // Copyright (C) 2010 Taobao.com, Inc.
 //
 // Created on 2010-09-16 by Yubai (yubai.lk@taobao.com) 
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

#include "ob_trans_buffer.h"

namespace oceanbase
{
  namespace updateserver
  {
    using namespace common;
    using namespace hash;

    TransBuffer::TransBuffer() : trans_map_(), patch_map_()
    {
      if (0 != trans_map_.create(DEFAULT_SIZE))
      {
        TBSYS_LOG(WARN, "create trans map fail size=%ld", DEFAULT_SIZE);
      }
      if (0 != patch_map_.create(DEFAULT_SIZE))
      {
        TBSYS_LOG(WARN, "create patch map fail size=%ld", DEFAULT_SIZE);
      }
    }

    TransBuffer::~TransBuffer()
    {
    }

    int TransBuffer::push(const TEKey &key, const TEValue &value)
    {
      int ret = OB_SUCCESS;
      int hash_ret = patch_map_.set(key, value, 1);
      if (HASH_INSERT_SUCC != hash_ret
          && HASH_OVERWRITE_SUCC != hash_ret)
      {
        TBSYS_LOG(WARN, "set to patch map fail %s %s", key.log_str(), value.log_str());
        ret = OB_ERROR;
      }
      return ret;
    }

    int TransBuffer::get(const TEKey &key, TEValue &value) const
    {
      int ret = OB_SUCCESS;
      if (HASH_EXIST != patch_map_.get(key, value)
          && HASH_EXIST != trans_map_.get(key, value))
      {
        ret = OB_ENTRY_NOT_EXIST;
      }
      return ret;
    }

    int TransBuffer::commit_()
    {
      int ret = OB_SUCCESS;
      for (iterator iter = patch_map_.begin(); iter != patch_map_.end(); iter++)
      {
        int hash_ret = trans_map_.set(iter->first, iter->second, 1);
        if (HASH_INSERT_SUCC != hash_ret
            && HASH_OVERWRITE_SUCC != hash_ret)
        {
          TBSYS_LOG(ERROR, "set to trans map fail %s %s", iter->first.log_str(), iter->second.log_str());
          ret = OB_ERROR;
          break;
        }
      }
      patch_map_.clear();
      return ret;
    }
    
    int TransBuffer::start_mutation()
    {
      int ret = OB_SUCCESS;
      ret = commit_();
      return ret;
    }

    int TransBuffer::end_mutation(bool rollback)
    {
      int ret = OB_SUCCESS;
      if (!rollback)
      {
        ret = commit_();
      }
      else
      {
        patch_map_.clear();
      }
      return ret;
    }

    int TransBuffer::clear()
    {
      int ret = OB_SUCCESS;
      trans_map_.clear();
      patch_map_.clear();
      return ret;
    }

    TransBuffer::iterator TransBuffer::begin() const
    {
      return trans_map_.begin();
    }

    TransBuffer::iterator TransBuffer::end() const
    {
      return trans_map_.end();;
    }
  }
}

