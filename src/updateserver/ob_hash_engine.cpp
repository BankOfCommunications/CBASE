////===================================================================
 //
 // ob_hash_engine.cpp / hash / common / Oceanbase
 //
 // Copyright (C) 2010 Taobao.com, Inc.
 //
 // Created on 2010-09-09 by Yubai (yubai.lk@taobao.com) 
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

#include "common/hash/ob_hashutils.h"
#include "common/ob_malloc.h"
#include "common/ob_atomic.h"
#include "ob_hash_engine.h"
#include "ob_ups_utils.h"

namespace oceanbase
{
  namespace updateserver
  {
    using namespace common;
    using namespace hash;

    HashEngine::HashEngine() : inited_(false), created_index_(false), sorted_index_(NULL), sorted_index_size_(0),
                               hashmap_(), indexmap_(), write_transaction_started_(false),
                               mutation_started_(false), trans_buffer_()
    {
    }

    HashEngine::~HashEngine()
    {
      if (inited_)
      {
        destroy();
      }
    }

    int HashEngine::init(MemTank *allocer, int64_t hash_item_num, int64_t index_item_num)
    {
      UNUSED(allocer);
      int ret = OB_SUCCESS;
      if (inited_)
      {
        TBSYS_LOG(WARN, "have already inited");
        ret = OB_ERROR;
      }
      else if (0 >= hash_item_num || 0 >= index_item_num)
      {
        TBSYS_LOG(WARN, "invalid param hash_item_num=%ld index_item_num=%ld",
                  hash_item_num, index_item_num);
        ret = OB_ERROR;
      }
      else if (0 != hashmap_.create(hash_item_num))
      {
        TBSYS_LOG(WARN, "create hashmap fail hash_item_num=%ld", hash_item_num);
        ret = OB_ERROR;
      }
      else if (0 != indexmap_.create(index_item_num))
      {
        TBSYS_LOG(WARN, "create indexmap fail index_item_num=%ld", index_item_num);
        ret = OB_ERROR;
        hashmap_.destroy();
      }
      else
      {
        inited_ = true;
      }
      return ret;
    }

    int HashEngine::destroy()
    {
      int ret = OB_SUCCESS;
      if (!inited_)
      {
        TBSYS_LOG(WARN, "have not inited");
      }
      else if (OB_SUCCESS != clear())
      {
        TBSYS_LOG(WARN, "clear fail");
        ret = OB_ERROR;
      }
      else
      {
        indexmap_.destroy();
        hashmap_.destroy();
        inited_ = false;
      }
      return ret;
    }

    int HashEngine::clear()
    {
      int ret = OB_SUCCESS;
      if (!inited_)
      {
        TBSYS_LOG(WARN, "have not inited");
        ret = OB_ERROR;
      }
      else if (write_transaction_started_)
      {
        TBSYS_LOG(WARN, "there is one transaction started");
        ret = OB_ERROR;
      }
      else if (mutation_started_)
      {
        TBSYS_LOG(WARN, "there is one mutation started");
        ret = OB_ERROR;
      }
      else
      {
        created_index_ = false;
        if (NULL != sorted_index_)
        {
          //delete[] sorted_index_;
          ob_free(sorted_index_);
          sorted_index_ = NULL;
          sorted_index_size_ = 0;
        }
        trans_buffer_.clear();
        indexmap_.clear();
        hashmap_.clear();
      }
      return ret;
    }

    HashEngine::HashKV *HashEngine::get_hashkv_ptr_(const HashKey &key)
    {
      HashAtomicFunc callback;
      hashmap_.atomic(key, callback);
      return callback.pkv;
    }

    bool HashEngine::update_map_(const TEKey &key, const TEValue &value)
    {
      bool bret = false;
      
      HashKey hash_key = key;
      HashValue hash_value = value;
      
      IndexKey index_key;
      IndexValue index_value;
      
      if (!get_key_prefix(key, index_key.key))
      {
        TBSYS_LOG(WARN, "get key prefix fail %s", key.log_str());
      }
      else
      {
        int hash_ret = hashmap_.set(hash_key, hash_value, 1);
        int index_ret = indexmap_.get(index_key, index_value);
        
        if (HASH_OVERWRITE_SUCC == hash_ret && HASH_EXIST == index_ret)
        {
          // 仅更新hash value即可
          bret = true;
        }
        else if (HASH_INSERT_SUCC == hash_ret && HASH_EXIST == index_ret)
        {
          // 插入hash value, 还要更新index value
          HashKV *cur_kv = NULL;
          if (NULL != (cur_kv = get_hashkv_ptr_(hash_key)))
          {
            // 保证同一个索引内的rowkey是顺序的
            HashKV *iter = index_value.list_head;
            HashKV *prev = NULL;
            bool need_update_indexmap = false;
            while (NULL != iter)
            {
              if (0 < (iter->first - hash_key))
              {
                cur_kv->second.next = iter;
                if (iter == index_value.list_head)
                {
                  index_value.list_head = cur_kv;
                  need_update_indexmap = true;
                }
                else if (NULL != prev)
                {
                  // 原子操作
                  atomic_exchange_pointer((pvoid*)&(prev->second.next), cur_kv);
                }
                break;
              }
              prev = iter;
              iter = iter->second.next;
            }
            if (NULL == iter
                && NULL != prev)
            {
              atomic_exchange_pointer((pvoid*)&(prev->second.next), cur_kv);
            }
            if (!need_update_indexmap
                || HASH_OVERWRITE_SUCC == indexmap_.set(index_key, index_value, 1))
            {
              bret = true;
            }
          }
          if (!bret)
          {
            hashmap_.erase(hash_key);
          }
        }
        else if (HASH_INSERT_SUCC == hash_ret && HASH_NOT_EXIST == index_ret)
        {
          // 插入hash value, 还要插入新的index value
          if (NULL != (index_value.list_head = get_hashkv_ptr_(hash_key))
              && HASH_INSERT_SUCC == indexmap_.set(index_key, index_value, 0))
          {
            TBSYS_LOG(DEBUG, "set to indexmap succ %s", index_key.log_str());
            bret = true;
          }
          if (!bret)
          {
            hashmap_.erase(hash_key);
          }
        }
        else
        {
          TBSYS_LOG(WARN, "updatemap fail hash_ret=%d index_ret=%d %s %s",
                    hash_ret, index_ret, key.log_str(), value.log_str());
        }
      }
      return bret;
    }

    int HashEngine::set(HashEngineTransHandle &handle,
                        const TEKey &key,
                        const TEValue &value)
    {
      int ret = OB_SUCCESS;
      if (!inited_)
      {
        TBSYS_LOG(WARN, "have not inited");
        ret = OB_ERROR;
      }
      else if (!handle.is_valid_(this, WRITE_TRANSACTION))
      {
        ret = OB_ERROR;
      }
      else if (OB_SUCCESS != trans_buffer_.push(key, value))
      {
        TBSYS_LOG(WARN, "fill write trans buffer fail %s %s", key.log_str(), value.log_str());
        ret = OB_ERROR;
      }
      else
      {
        //TBSYS_LOG(INFO, "fill write trans buffer succ %s %s", key.log_str(), value.log_str());
      }
      return ret;
    }

    int HashEngine::set(const TEKey &key, const TEValue &value)
    {
      int ret = OB_SUCCESS;
      if (!inited_)
      {
        TBSYS_LOG(WARN, "have not inited");
        ret = OB_ERROR;
      }
      else if (!update_map_(key, value))
      {
        TBSYS_LOG(WARN, "update map fail %s %s", key.log_str(), value.log_str());
        ret = OB_ERROR;
      }
      else
      {
        //TBSYS_LOG(INFO, "update map succ %s %s", key.log_str(), value.log_str());
      }
      return ret;
    }

    int HashEngine::query_map_(TEKey &key, TEValue &value)
    {
      int ret = OB_SUCCESS;
      HashKey hash_key = key;
      HashValue hash_value;
      //int hash_ret = hashmap_.get(hash_key, hash_value);
      HashAtomicFunc callback;
      int hash_ret = hashmap_.atomic(hash_key, callback);
      if (HASH_EXIST == hash_ret
          && NULL != callback.pkv)
      {
        key = callback.pkv->first.key;
        value = callback.pkv->second.value;
      }
      else if (HASH_NOT_EXIST == hash_ret)
      {
        //TBSYS_LOG(INFO, "get from hashmap fail hash_ret=HASH_NOT_EXIST %s", key.log_str());
        ret = OB_ENTRY_NOT_EXIST;
      }
      else
      {
        TBSYS_LOG(WARN, "get from hashmap fail hash_ret=%d %s", hash_ret, key.log_str());
        ret = OB_ERROR;
      }
      return ret;
    }

    int HashEngine::get(const HashEngineTransHandle &handle,
                        TEKey &key,
                        TEValue &value)
    {
      int ret = OB_SUCCESS;
      if (!inited_)
      {
        TBSYS_LOG(WARN, "have not inited");
        ret = OB_ERROR;
      }
      else if (handle.is_valid_(this, READ_TRANSACTION))
      {
        ret = query_map_(key, value);
      }
      else if (handle.is_valid_(this, WRITE_TRANSACTION))
      {
        if (OB_SUCCESS != trans_buffer_.get(key, value))
        {
          ret = query_map_(key, value);
        }
      }
      else
      {
        ret = OB_ERROR;
      }
      return ret;
    }

    int HashEngine::get(TEKey &key, TEValue &value)
    {
      int ret = OB_SUCCESS;
      if (!inited_)
      {
        TBSYS_LOG(WARN, "have not inited");
        ret = OB_ERROR;
      }
      else
      {
        ret = query_map_(key, value);
      }
      return ret;
    }

    bool HashEngine::search_index_(const IndexItem &start_key, const int start_exclude, const int min_key,
                                  const IndexItem &end_key, const int end_exclude, const int max_key,
                                  const IndexItem *&index_head, const IndexItem *&index_tail)
    {
      bool bret = false;

      index_head = NULL;
      index_tail = NULL;

      IndexItem *start_iter = NULL;
      IndexItem *end_iter = NULL;
      if (0 < sorted_index_size_)
      {
        // lower bound返回第一个大于等于key的位置
        start_iter = std::lower_bound(sorted_index_, sorted_index_ + sorted_index_size_, start_key);
        end_iter = std::upper_bound(start_iter, sorted_index_ + sorted_index_size_, end_key);
      }
      if (NULL != start_iter
          && (sorted_index_ + sorted_index_size_) != start_iter)
      {
        if (0 == start_exclude
            || 0 != min_key)
        {
          // 如果是闭区间 则不需要判断key与当前位置的比较结果是不是等于 直接返回当前位置
          index_head = start_iter;
        }
        else if (*start_iter == start_key)
        {
          // 如果是开区间 并且key与当前位置的比较结果为等于 需要再跳一个位置
          index_head = start_iter + 1;
        }
        else
        {
          // 如果是开区间 并且key与当前位置的比较结果不为等于 直接返回当前位置
          index_head = start_iter;
        }
      }
      if (NULL != end_iter
          && sorted_index_ != end_iter)
      {
        end_iter -= 1;
        if (0 == end_exclude
            || 0 != max_key)
        {
          index_tail = end_iter;
        }
        else if (*end_iter == end_key)
        {
          index_tail = end_iter - 1;
        }
        else
        {
          index_tail = end_iter;
        }
      }
      if ((sorted_index_ + sorted_index_size_) <= index_head
          || sorted_index_ > index_head)
      {
        index_head = NULL;
      }
      if ((sorted_index_ + sorted_index_size_) <= index_tail
          || sorted_index_ > index_tail)
      {
        index_tail = NULL;
      }
      if (NULL != index_head
          && NULL != index_tail
          && 0 <= (index_tail - index_head))
      {
        bret = true;
      }
      return bret;
    }

    TEKey &HashEngine::get_min_key_() const
    {
      return sorted_index_[0].hash_key_ptr->key;
    }

    TEKey &HashEngine::get_max_key_() const
    {
      return sorted_index_[sorted_index_size_ - 1].hash_key_ptr->key;
    }

    int HashEngine::sorted_scan_(const TEKey &start_key,
                                const int min_key,
                                const int start_exclude,
                                const TEKey &end_key,
                                const int max_key,
                                const int end_exclude,
                                HashEngineIterator &iter)
    {
      int ret = OB_SUCCESS;
      
      if (0 >= sorted_index_size_)
      {
        //ret = OB_ENTRY_NOT_EXIST;
      }
      else
      {
        HashKey hash_start_key = (0 == min_key) ? start_key : get_min_key_();
        HashKey hash_end_key = (0 == max_key) ? end_key : get_max_key_();
        IndexItem index_start_key;
        IndexItem index_end_key;
        index_start_key.hash_key_ptr = &hash_start_key;
        index_end_key.hash_key_ptr = &hash_end_key;

        const IndexItem *index_head = NULL;
        const IndexItem *index_tail = NULL;

        if (0 < (hash_start_key - hash_end_key))
        {
          TBSYS_LOG(WARN, "start_key must larger than or equal end_key start_key[%s], end_key[%s]",
                    hash_start_key.log_str(), hash_end_key.log_str());
          ret = OB_ERROR;
        }
        else if (!search_index_(index_start_key, start_exclude, min_key, index_end_key, end_exclude, max_key, index_head, index_tail))
        {
          TBSYS_LOG(WARN, "search index fail start_key[%s] min_key=%d start_exclude=%d end_key[%s] max_key=%d end_exclude=%d",
                    start_key.log_str(), min_key, start_exclude, end_key.log_str(), max_key, end_exclude);
          //ret = OB_ENTRY_NOT_EXIST;
        }
        else
        {
          iter.sorted_set_(index_head, index_tail);
        }
      }
      return ret;
    }

    bool HashEngine::unsorted_result_filt_(const IndexValue &index_value,
                                          const TEKey &start_key, const int start_exclude,
                                          const TEKey &end_key, const int end_exclude,
                                          const HashKV *&list_head, const HashKV *&list_tail)
    {
      bool bret = false;
      list_head = NULL;
      list_tail = NULL;
      HashKV *list_iter = index_value.list_head;
      HashKV *start_iter = NULL;
      HashKV *end_iter = NULL;
      int start_cmp_ret = 0;
      int end_cmp_ret = 0;
      HashKV *prev_end_iter = NULL;
      // 找到第一个大于等于start_key的位置 并记录比较结果
      // 如果都大于start_key 那么start_iter就是链表头
      while (NULL != list_iter)
      {
        start_cmp_ret = start_key - list_iter->first.key;
        start_iter = list_iter;
        if (0 >= start_cmp_ret)
        {
          break;
        }
        prev_end_iter = list_iter;
        list_iter = list_iter->second.next;
      }
      // 如果找start_iter的时候已经将list遍历完,那么不进这个循环,end_iter自然为空
      // 找到第一个大于等于end_key的位置 并记录比较结果
      while (NULL != list_iter)
      {
        end_cmp_ret = end_key - list_iter->first.key;
        if (0 <= end_cmp_ret)
        {
          end_iter = list_iter;
        }
        if (0 >= end_cmp_ret)
        {
          break;
        }
        prev_end_iter = list_iter;
        list_iter = list_iter->second.next;
      }
      if (NULL != start_iter)
      {
        if (0 == start_exclude)
        {
          // 如果是闭区间 则不需要判断与当前位置的比较结果是不是等于 list_head直接是当前位置
          list_head = start_iter;
        }
        else if (0 == start_cmp_ret)
        {
          // 如果是开区间 并且与当前位置的比较结果为等于 需要再跳一个位置
          list_head = start_iter->second.next;
        }
        else
        {
          // 如果是开区间 并且与当前位置的比较结果不为等于 则list_head就为当前位置
          list_head = start_iter;
        }
      }
      if (NULL != end_iter)
      {
        if (0 == end_exclude)
        {
          list_tail = end_iter;
        }
        else if (0 == end_cmp_ret)
        {
          list_tail = prev_end_iter;
        }
        else
        {
          list_tail = end_iter;
        }
      }
      if (NULL != list_head
          && NULL != list_tail
          && 0 <= (list_tail->first - list_head->first))
      {
        bret = true;
      }
      return bret;
    }

    int HashEngine::nosorted_scan_(const TEKey &start_key,
                                const int min_key,
                                const int start_exclude,
                                const TEKey &end_key,
                                const int max_key,
                                const int end_exclude,
                                HashEngineIterator &iter)
    {
      int ret = OB_SUCCESS;
      TEKey start_key_prefix;
      TEKey end_key_prefix;
      if (0 != min_key || 0 != max_key)
      {
        TBSYS_LOG(WARN, "cannot scan by min_key or max_key for unindexed hash engine start_key[%s]",
                  start_key.log_str());
        ret = OB_NOT_SUPPORTED;
      }
      else if (0 < (start_key - end_key))
      {
        TBSYS_LOG(WARN, "start_key must larger than or equal end_key start_key[%s], end_key[%s]",
                  start_key.log_str(), end_key.log_str());
        ret = OB_ERROR;
      }
      else if (!get_key_prefix(start_key, start_key_prefix)
              || !get_key_prefix(end_key, end_key_prefix))
      {
        TBSYS_LOG(WARN, "get key prefix fail start_key[%s], end_key[%s]",
                  start_key.log_str(), end_key.log_str());
        ret = OB_ERROR;
      }
      else if (start_key_prefix != end_key_prefix)
      {
        TBSYS_LOG(WARN, "start_key_prefix must equal to end_key_prefix for unindexed hash engine start_key[%s], end_key[%s]",
                  start_key.log_str(), end_key.log_str());
        ret = OB_NOT_SUPPORTED;
      }
      else
      {
        IndexKey index_key = start_key_prefix;
        IndexValue index_value;
        int hash_ret = indexmap_.get(index_key, index_value);
        const HashKV *list_head = NULL;
        const HashKV *list_tail = NULL;
        bool filt_ret = unsorted_result_filt_(index_value, start_key, start_exclude, end_key, end_exclude, list_head, list_tail);
        if (HASH_EXIST == hash_ret
            && filt_ret)
        {
          iter.unsorted_set_(list_head, list_tail);
        }
        else if (HASH_NOT_EXIST == hash_ret
                || !filt_ret)
        {
          TBSYS_LOG(INFO, "get from indexmap fail hash_ret=%d filt_ret=%s %s", hash_ret, STR_BOOL(filt_ret), start_key_prefix.log_str());
          //ret = OB_ENTRY_NOT_EXIST;
        }
        else
        {
          TBSYS_LOG(WARN, "get from indexmap fail hash_ret=%d %s", hash_ret, start_key_prefix.log_str());
          ret = OB_ERROR;
        }
      }
      return ret;
    }

    int HashEngine::scan(const HashEngineTransHandle &handle,
                        const TEKey &start_key,
                        const int min_key,
                        const int start_exclude,
                        const TEKey &end_key,
                        const int max_key,
                        const int end_exclude,
                        HashEngineIterator &iter)
    {
      int ret = OB_SUCCESS;
      iter.reset();
      if (!inited_)
      {
        TBSYS_LOG(WARN, "have not inited");
        ret = OB_ERROR;
      }
      else if (!handle.is_valid_(this, READ_TRANSACTION))
      {
        ret = OB_ERROR;
      }
      else if (!created_index_)
      {
        ret = nosorted_scan_(start_key, min_key, start_exclude, end_key, max_key, end_exclude, iter);
      }
      else
      {
        ret = sorted_scan_(start_key, min_key, start_exclude, end_key, max_key, end_exclude, iter);
      }
      return ret;
    }
    
    int HashEngine::create_index()
    {
      int ret = OB_SUCCESS;
      IndexItem *tmp_index = NULL;
      if (!inited_)
      {
        TBSYS_LOG(WARN, "have not inited");
        ret = OB_ERROR;
      }
      else if (created_index_)
      {
        TBSYS_LOG(INFO, "have already created index");
        ret = OB_SUCCESS;
      }
      else if (0 == hashmap_.size())
      {
        TBSYS_LOG(INFO, "not data, need not create index");
        ret = OB_SUCCESS;
      }
      //else if (NULL == (tmp_index = new(std::nothrow) IndexItem[hashmap_.size()]))
      else if (NULL == (tmp_index = (IndexItem*)ob_malloc(sizeof(IndexItem) * hashmap_.size())))
      {
        TBSYS_LOG(WARN, "new tmp index fail size=%ld", hashmap_.size());
        ret = OB_ERROR;
      }
      else
      {
        hashmap_iterator_t iter;
        int64_t index_pos = 0;
        for (iter = hashmap_.begin(); iter != hashmap_.end(); iter++, index_pos++)
        {
          TBSYS_LOG(DEBUG, "hashmap_size=%ld index_pos=%ld hash_key=%s ptr=%p", hashmap_.size(), index_pos, iter->first.log_str(), &(iter->first));
          tmp_index[index_pos].hash_key_ptr = &(iter->first);
          tmp_index[index_pos].hash_value_ptr = &(iter->second);
        }
        std::sort(tmp_index, tmp_index + index_pos);
        sorted_index_ = tmp_index;
        sorted_index_size_ = index_pos;
      }
      if (OB_SUCCESS == ret)
      {
        created_index_ = true;
      }
      return ret;
    }

    int HashEngine::start_transaction(const TETransType type, HashEngineTransHandle &handle)
    {
      int ret = OB_SUCCESS;
      if (!inited_)
      {
        TBSYS_LOG(WARN, "have not inited");
        ret = OB_ERROR;
      }
      else
      {
        switch (type)
        {
          case READ_TRANSACTION:
            handle.host_ = this;
            handle.trans_type_ = type;
            break;
          case WRITE_TRANSACTION:
            if (write_transaction_started_)
            {
              TBSYS_LOG(WARN, "there is one transaction have been started");
              ret = OB_UPS_TRANS_RUNNING;
            }
            else if (OB_SUCCESS != trans_buffer_.start_mutation())
            {
              TBSYS_LOG(WARN, "start mutation fail");
              ret = OB_ERROR;
            }
            else
            {
              write_transaction_started_ = true;
              handle.host_ = this;
              handle.trans_type_ = type;
            }
            break;
          default:
            handle.host_ = NULL;
            handle.trans_type_ = INVALID_TRANSACTION;
            ret = OB_ERROR;
            break;
        }
      }
      return ret;
    };
    
    int HashEngine::end_transaction(HashEngineTransHandle &handle, bool rollback)
    {
      int ret = OB_SUCCESS;
      if (!inited_)
      {
        TBSYS_LOG(WARN, "have not inited");
        ret = OB_ERROR;
      }
      else if (!handle.is_valid_(this))
      {
        ret = OB_ERROR;
      }
      else
      {
        switch (handle.trans_type_)
        {
          case WRITE_TRANSACTION:
            if (!write_transaction_started_)
            {
              TBSYS_LOG(WARN, "there is no transaction started");
              ret = OB_ERROR;
            }
            else if (mutation_started_)
            {
              TBSYS_LOG(WARN, "mutation have not end");
              ret = OB_ERROR;
            }
            else if (OB_SUCCESS != trans_buffer_.end_mutation(rollback))
            {
              TBSYS_LOG(WARN, "end transaction fail");
              ret = OB_ERROR;
            }
            else
            {  
              if (!rollback)
              {
                TransBuffer::iterator iter;
                for (iter = trans_buffer_.begin(); iter != trans_buffer_.end(); ++iter)
                {
                  if (OB_SUCCESS != set(iter->first, iter->second))
                  {
                    ret = OB_ERROR;
                    break;
                  }
                  else
                  {
                    TBSYS_LOG_US(DEBUG, "commit trans succ %s %s", iter->first.log_str(), iter->second.log_str());
                  }
                }
              }
              trans_buffer_.clear();
              write_transaction_started_ = false;
            }
            break;
          case READ_TRANSACTION:
            break;
          default:
            ret = OB_ERROR;
            break;
        }
        if (OB_SUCCESS == ret)
        {
          handle.reset();
        }
      }
      return ret;
    };

    int HashEngine::start_mutation(HashEngineTransHandle &handle)
    {
      int ret = OB_SUCCESS;
      if (!inited_)
      {
        TBSYS_LOG(WARN, "have not inited");
        ret = OB_ERROR;
      }
      else if (!write_transaction_started_)
      {
        TBSYS_LOG(WARN, "there is no transaction started");
        ret = OB_ERROR;
      }
      else if (mutation_started_)
      {
        TBSYS_LOG(WARN, "there is one mutation have been started");
        ret = OB_ERROR;
      }
      else if (!handle.is_valid_(this, WRITE_TRANSACTION))
      {
        ret = OB_ERROR;
      }
      else if (OB_SUCCESS != (ret = trans_buffer_.start_mutation()))
      {
        TBSYS_LOG(WARN, "trans buffer start mutation fail");
      }
      else
      {
        mutation_started_ = true;
      }
      return ret;
    }

    int HashEngine::end_mutation(HashEngineTransHandle &handle, bool rollback)
    {
      int ret = OB_SUCCESS;
      if (!inited_)
      {
        TBSYS_LOG(WARN, "have not inited");
        ret = OB_ERROR;
      }
      else if (!mutation_started_)
      {
        TBSYS_LOG(WARN, "no mutation have been started");
        ret = OB_ERROR;
      }
      else if (!handle.is_valid_(this, WRITE_TRANSACTION))
      {
        ret = OB_ERROR;
      }
      else
      {
        ret = trans_buffer_.end_mutation(rollback);
        mutation_started_ = false;
      }
      return ret;
    }

    void HashEngine::dump2text(const char *fname) const
    {
      const int64_t BUFFER_SIZE = 1024;
      char buffer[BUFFER_SIZE];
      snprintf(buffer, BUFFER_SIZE, "%s.hash.main", fname);
      FILE *fd = fopen(buffer, "w");
      if (NULL != fd)
      {
        int64_t num = 0;
        hashmap_const_iterator_t hash_iter;
        for (hash_iter = hashmap_.begin(); hash_iter != hashmap_.end(); ++hash_iter, ++num)
        {
          fprintf(fd, "[ROW_INFO][%ld] hash_key=[%s] ptr=%p hash_value=[%s] ptr=%p\n",
                  num, hash_iter->first.log_str(), &(hash_iter->first),
                  hash_iter->second.log_str(), &(hash_iter->second));
          int64_t pos = 0;
          ObCellInfoNode *iter = hash_iter->second.value.list_head;
          while (NULL != iter)
          {
            fprintf(fd, "          [CELL_INFO][%ld][%ld] column_id=%lu value=[%s] ptr=%p\n",
                    num, pos++, iter->cell_info.column_id_,
                    print_obj(iter->cell_info.value_), iter);
            if (hash_iter->second.value.list_tail == iter)
            {
              break;
            }
            iter = iter->next;;
          }
        }
        fclose(fd);
      }
      snprintf(buffer, BUFFER_SIZE, "%s.hash.index", fname);
      fd = fopen(buffer, "w");
      if (NULL != fd)
      {
        int64_t num = 0;
        indexmap_const_iterator_t index_iter;
        for (index_iter = indexmap_.begin(); index_iter != indexmap_.end(); ++index_iter, ++num)
        {
          fprintf(fd, "[INDEX_INFO][%ld] index_key=[%s] ptr=%p\n",
                  num, index_iter->first.log_str(), &(index_iter->first));
          int64_t pos = 0;
          HashKV *iter = index_iter->second.list_head;
          while (NULL != iter)
          {
            fprintf(fd, "            [ROW_INFO][%ld][%ld] hash_key=[%s] ptr=%p hash_value=[%s] ptr=%p\n",
                    num, pos++, iter->first.log_str(), &(iter->first),
                    iter->second.log_str(), &(iter->second));
            iter = iter->second.next;
          }
        }
        fclose(fd);
      }
    }

    int64_t HashEngine::size() const
    {
      return hashmap_.size();
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////

    SortedIterator::SortedIterator() : start_index_item_(NULL),
                                       cur_index_item_(NULL),
                                       end_index_item_(NULL)
    {
    }

    SortedIterator::~SortedIterator()
    {
    }

    int SortedIterator::next()
    {
      int ret = OB_SUCCESS;
      if (end_index_item_ == cur_index_item_)
      {
        ret = OB_ITER_END;
      }
      else if (NULL == cur_index_item_
              || end_index_item_ < (cur_index_item_ += 1))
      {
        TBSYS_LOG(WARN, "invalid internal pointer cur_hash_kv=%p", cur_index_item_);
        ret = OB_ITER_END;
      }
      return ret;
    }

    int SortedIterator::get_key(TEKey &key) const
    {
      int ret = OB_SUCCESS;
      if (NULL == cur_index_item_
          || NULL == cur_index_item_->hash_value_ptr)
      {
        ret = OB_ERROR;
      }
      else
      {
        key = cur_index_item_->hash_key_ptr->key;
      }
      return ret;
    }

    int SortedIterator::get_value(TEValue &value) const
    {
      int ret = OB_SUCCESS;
      if (NULL == cur_index_item_
          || NULL == cur_index_item_->hash_value_ptr)
      {
        ret = OB_ERROR;
      }
      else
      {
        value = cur_index_item_->hash_value_ptr->value;
      }
      return ret;
    }

    void SortedIterator::restart()
    {
      cur_index_item_ = start_index_item_;
    }

    void SortedIterator::set(const HashEngine::IndexItem *start_index_item,
                            const HashEngine::IndexItem *end_index_item)
    {
      start_index_item_ = start_index_item;
      cur_index_item_ = start_index_item;
      end_index_item_ = end_index_item;
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////

    UnsortedIterator::UnsortedIterator() : start_hash_kv_(NULL),
                                           cur_hash_kv_(NULL),
                                           end_hash_kv_(NULL)
    {
    }

    UnsortedIterator::~UnsortedIterator()
    {
    }

    int UnsortedIterator::next()
    {
      int ret = OB_SUCCESS;
      if (end_hash_kv_ == cur_hash_kv_)
      {
        ret = OB_ITER_END;
      }
      else if (NULL == cur_hash_kv_
              || NULL == (cur_hash_kv_ = cur_hash_kv_->second.next))
      {
        TBSYS_LOG(WARN, "invalid internal pointer cur_hash_kv=%p", cur_hash_kv_);
        ret = OB_ITER_END;
      }
      return ret;
    }

    int UnsortedIterator::get_key(TEKey &key) const
    {
      int ret = OB_SUCCESS;
      if (NULL == cur_hash_kv_)
      {
        ret = OB_ERROR;
      }
      else
      {
        key = cur_hash_kv_->first.key;
      }
      return ret;
    }

    int UnsortedIterator::get_value(TEValue &value) const
    {
      int ret = OB_SUCCESS;
      if (NULL == cur_hash_kv_)
      {
        ret = OB_ERROR;
      }
      else
      {
        value = cur_hash_kv_->second.value;
      }
      return ret;
    }

    void UnsortedIterator::restart()
    {
      cur_hash_kv_ = start_hash_kv_;
    }

    void UnsortedIterator::set(const HashEngine::HashKV *start_hash_kv,
                              const HashEngine::HashKV *end_hash_kv)
    {
      start_hash_kv_ = start_hash_kv;
      cur_hash_kv_ = start_hash_kv;
      end_hash_kv_ = end_hash_kv;
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////

    HashEngineIterator::HashEngineIterator() : sorted_iter_(), unsorted_iter_(), iter_(NULL)
    {
    }

    HashEngineIterator::~HashEngineIterator()
    {
    }

    HashEngineIterator::HashEngineIterator(const HashEngineIterator &other)
    {
      *this = other;
    }

    HashEngineIterator &HashEngineIterator::operator = (const HashEngineIterator &other)
    {
      unsorted_iter_ = other.unsorted_iter_;
      sorted_iter_ = other.sorted_iter_;
      if (other.iter_ == &(other.unsorted_iter_))
      {
        iter_ = &(unsorted_iter_);
      }
      else if (other.iter_ == &(other.sorted_iter_))
      {
        iter_ = &(sorted_iter_);
      }
      else
      {
        iter_ = NULL;
      }
      return *this;
    }

    void HashEngineIterator::unsorted_set_(const HashEngine::HashKV *start_hash_kv,
                                          const HashEngine::HashKV *end_hash_kv)
    {
      unsorted_iter_.set(start_hash_kv, end_hash_kv);
      iter_ = &unsorted_iter_;
    }

    void HashEngineIterator::sorted_set_(const HashEngine::IndexItem *start_index_item,
                                        const HashEngine::IndexItem *end_index_item)
    {
      sorted_iter_.set(start_index_item, end_index_item);
      iter_ = &sorted_iter_;
    }

    int HashEngineIterator::next()
    {
      int ret = OB_SUCCESS;
      if (NULL == iter_)
      {
        ret = OB_ITER_END;
      }
      else
      {
        ret = iter_->next();
      }
      return ret;
    }

    int HashEngineIterator::get_key(TEKey &key) const
    {
      int ret = OB_SUCCESS;
      if (NULL == iter_)
      {
        ret = OB_ERROR;
      }
      else
      {
        ret = iter_->get_key(key);
      }
      return ret;
    }

    int HashEngineIterator::get_value(TEValue &value) const
    {
      int ret = OB_SUCCESS;
      if (NULL == iter_)
      {
        ret = OB_ERROR;
      }
      else
      {
        ret = iter_->get_value(value);
      }
      return ret;
    }

    void HashEngineIterator::restart()
    {
      if (NULL != iter_)
      {
        iter_->restart();
      }
    }

    void HashEngineIterator::reset()
    {
      iter_ = NULL;
    }
  }
}

