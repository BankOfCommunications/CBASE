////===================================================================
 //
 // ob_query_engine.cpp / hash / common / Oceanbase
 //
 // Copyright (C) 2010 Taobao.com, Inc.
 //
 // Created on 2012-03-28 by Yubai (yubai.lk@taobao.com) 
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
#include "ob_query_engine.h"
#include "ob_memtable.h"

namespace oceanbase
{
  namespace updateserver
  {
    using namespace common;

    int64_t QueryEngine::HASH_SIZE = 50000000;
    QueryEngine::QueryEngine(MemTank &allocer) : inited_(false),
                                                 btree_alloc_(allocer),
                                                 table_btree_alloc_(allocer), /*add zhaoqiong [Truncate Table]:20160318*/
                                                 hash_alloc_(allocer),
                                                 keybtree_(btree_alloc_),
                                                 table_keybtree_(table_btree_alloc_), /*add zhaoqiong [Truncate Table]:20160318*/
                                                 keyhash_(hash_alloc_, hash_alloc_)
    {
    }

    QueryEngine::~QueryEngine()
    {
      if (inited_)
      {
        destroy();
      }
    }

    int QueryEngine::init(const int64_t hash_size)
    {
      int ret = OB_SUCCESS;
      if (inited_)
      {
        TBSYS_LOG(WARN, "have already inited");
        ret = OB_ERROR;
      }
      else
      {
        if (OB_SUCCESS != (ret = keyhash_.create(hash::cal_next_prime(hash_size?: HASH_SIZE))))
        {
          TBSYS_LOG(WARN, "keyhash create fail");
        }
        else if (ERROR_CODE_OK != (ret = keybtree_.init()))
        {
          TBSYS_LOG(WARN, "keybtree init fail");
        }
        //add zhaoqiong [Truncate Table]:20160318:b
        else if (ERROR_CODE_OK != (ret = table_keybtree_.init()))
        {
          TBSYS_LOG(WARN, "table_keybtree init fail");
        }
        //add:e
        else
        {
          inited_ = true;
        }
      }
      return ret;
    }

    int QueryEngine::destroy()
    {
      int ret = OB_SUCCESS;
      if (!inited_)
      {
        TBSYS_LOG(WARN, "have not inited");
      }
      else
      {
        keybtree_.destroy();
        keyhash_.destroy();
        table_keybtree_.destroy(); /*add zhaoqiong [Truncate Table]:20160318*/
        inited_ = false;
      }
      return ret;
    }

    int QueryEngine::clear()
    {
      int ret = OB_SUCCESS;
      if (!inited_)
      {
        TBSYS_LOG(WARN, "have not inited");
      }
      else
      {
        keybtree_.clear();
        keyhash_.clear();
        table_keybtree_.clear(); /*add zhaoqiong [Truncate Table]:20160318*/
      }
      return ret;
    }

    int QueryEngine::set(const TEKey &key, TEValue *value)
    {
      int ret = OB_SUCCESS;
      int btree_ret = ERROR_CODE_OK;
      int hash_ret = OB_SUCCESS;
      TEHashKey hash_key;
      hash_key.table_id = static_cast<uint32_t>(key.table_id);
      hash_key.rk_length = static_cast<int32_t>(key.row_key.length());
      hash_key.row_key = key.row_key.ptr();
      if (!inited_)
      {
        TBSYS_LOG(WARN, "have not inited");
        ret = OB_ERROR;
      }
      else if (NULL == value)
      {
        TBSYS_LOG(INFO, "value null pointer");
        ret = OB_ERROR;
      }
      //add zhaoqiong [Truncate Table]:20160318:b
      else if (0 == key.row_key.length())
      {
        value->index_stat = IST_NO_INDEX;
        if (ERROR_CODE_OK != (btree_ret = table_keybtree_.put(key, value, true)))
        {
          TBSYS_LOG(WARN, "put to keybtree fail btree_ret=%d [%s] [%s]",
                    btree_ret, key.log_str(), value->log_str());
          ret = (ERROR_CODE_ALLOC_FAIL == btree_ret) ? OB_MEM_OVERFLOW : OB_ERROR;
        }
        else
        {
          value->index_stat |= IST_BTREE_INDEX;
          TBSYS_LOG(DEBUG, "insert to btree succ %s %s", key.log_str(), value->log_str());
        }
      }
      //add:e
      else if (OB_SUCCESS != (hash_ret = keyhash_.insert(hash_key, value)))
      {
        if (OB_ENTRY_EXIST != hash_ret)
        {
          TBSYS_LOG(WARN, "put to keyhash fail hash_ret=%d value=%p [%s] [%s]",
                    hash_ret, value, key.log_str(), value->log_str());
        }
        ret = hash_ret;
      }
      else
      {
        value->index_stat |= IST_HASH_INDEX;
        if (ERROR_CODE_OK != (btree_ret = keybtree_.put(key, value, true)))
        {
          TBSYS_LOG(WARN, "put to keybtree fail btree_ret=%d [%s] [%s]",
                    btree_ret, key.log_str(), value->log_str());
          ret = (ERROR_CODE_ALLOC_FAIL == btree_ret) ? OB_MEM_OVERFLOW : OB_ERROR;
          keyhash_.erase(hash_key);
          value->index_stat = IST_NO_INDEX;
        }
        else
        {
          value->index_stat |= IST_BTREE_INDEX;
          TBSYS_LOG(DEBUG, "insert to hash and btree succ %s %s", key.log_str(), value->log_str());
        }
      }
      return ret;
    }

    TEValue *QueryEngine::get(const TEKey &key)
    {
      TEValue *ret = NULL;
      if (!inited_)
      {
        TBSYS_LOG(WARN, "have not inited");
      }
      //add zhaoqiong [Truncate Table]:20160318
      else if (0 == key.row_key.length())
      {
        int btree_ret = ERROR_CODE_OK;
        if (ERROR_CODE_OK != (btree_ret = table_keybtree_.get(key, ret))
            || NULL == ret)
        {
          if (ERROR_CODE_NOT_FOUND != btree_ret)
          {
            TBSYS_LOG(WARN, "get from table_keybtree fail btree_ret=%d %s", btree_ret, key.log_str());
          }
        }
      }
      //add:e
      else if (g_conf.using_hash_index)
      {
        TEHashKey hash_key;
        hash_key.table_id = static_cast<uint32_t>(key.table_id);
        hash_key.rk_length = static_cast<int32_t>(key.row_key.length());
        hash_key.row_key = key.row_key.ptr();
        int hash_ret = OB_SUCCESS;
        while (true)
        {
          if (OB_SUCCESS != (hash_ret = keyhash_.get(hash_key, ret))
              || NULL == ret)
          {
            if (OB_ENTRY_NOT_EXIST != hash_ret)
            {
              TBSYS_LOG(WARN, "get from keyhash fail hash_ret=%d %s", hash_ret, key.log_str());
            }
            break;
          }
          else if ((ret->index_stat & IST_HASH_INDEX)
                  && (ret->index_stat & IST_BTREE_INDEX))
          {
            break;
          }
          else
          {
            asm("pause");
            ret = NULL;
            continue;
          }
        }
      }
      else
      {
        int btree_ret = ERROR_CODE_OK;
        if (ERROR_CODE_OK != (btree_ret = keybtree_.get(key, ret))
            || NULL == ret)
        {
          if (ERROR_CODE_NOT_FOUND != btree_ret)
          {
            TBSYS_LOG(WARN, "get from keybtree fail btree_ret=%d %s", btree_ret, key.log_str());
          }
        }
      }
      return ret;
    }

    int QueryEngine::scan(const TEKey &start_key,
                          const int min_key,
                          const int start_exclude,
                          const TEKey &end_key,
                          const int max_key,
                          const int end_exclude,
                          const bool reverse,
                          QueryEngineIterator &iter)
    {
      int ret = OB_SUCCESS;
      int btree_ret = ERROR_CODE_OK;
      if (!inited_)
      {
        TBSYS_LOG(WARN, "have not inited");
        ret = OB_ERROR;
      }
      else if (ERROR_CODE_OK != (btree_ret = keybtree_.get_scan_handle(iter.get_read_handle_())))
      {
        TBSYS_LOG(WARN, "btree get scan handle fail btree_ret=%d", btree_ret);
        ret = OB_ERROR;
      }
      else
      {
        btree_ret = ERROR_CODE_OK;
        const TEKey *scan_start_key_ptr = NULL;
        const TEKey *scan_end_key_ptr = NULL;
        int start_exclude_ = start_exclude;
        int end_exclude_ = end_exclude;
        TEKey btree_min_key;
        TEKey btree_max_key;
        if (0 != min_key)
        {
          btree_ret = keybtree_.get_min_key(btree_min_key);
          if (ERROR_CODE_OK != btree_ret)
          {
            TBSYS_LOG(WARN, "get min key from btree fail btree_ret=%d", btree_ret);
            ret = OB_ERR_UNEXPECTED;
          }
          else
          {
            scan_start_key_ptr = &btree_min_key;
            start_exclude_ = 0;
          }
        }
        else
        {
          scan_start_key_ptr = &start_key;
        }
        if (0 != max_key)
        {
          btree_ret = keybtree_.get_max_key(btree_max_key);
          if (ERROR_CODE_OK != btree_ret)
          {
            TBSYS_LOG(WARN, "get max key from btree fail btree_ret=%d", btree_ret);
            ret = OB_ERR_UNEXPECTED;
          }
          else
          {
            scan_end_key_ptr = &btree_max_key;
            end_exclude_ = 0;
          }
        }
        else
        {
          scan_end_key_ptr = &end_key;
        }
        if (reverse)
        {
          std::swap(scan_start_key_ptr, scan_end_key_ptr);
          std::swap(start_exclude_, end_exclude_);
        }
        if (OB_SUCCESS == ret)
        {
          btree_ret = keybtree_.set_key_range(iter.get_read_handle_(),
                                              *scan_start_key_ptr, start_exclude_,
                                              *scan_end_key_ptr, end_exclude_);
          TBSYS_LOG(DEBUG, "[BTREE_SCAN_PARAM] [%s] %d [%s] %d",
                    scan_start_key_ptr->log_str(), start_exclude_, scan_end_key_ptr->log_str(), end_exclude_);
          if (ERROR_CODE_OK != btree_ret)
          {
            TBSYS_LOG(WARN, "set key range to btree scan handle fail btree_ret=%d", btree_ret);
            ret = OB_ERR_UNEXPECTED;
          }
          else
          {
            iter.set_(&keybtree_);
          }
        }
      }
      return ret;
    }

    //add zhaoqiong [Truncate Table]:20160318:b
    int QueryEngine::scan_table(const TEKey &start_key,
                          const int min_key,
                          const int start_exclude,
                          const TEKey &end_key,
                          const int max_key,
                          const int end_exclude,
                          const bool reverse,
                          QueryEngineIterator &iter)
    {
      int ret = OB_SUCCESS;
      int btree_ret = ERROR_CODE_OK;
      if (!inited_)
      {
        TBSYS_LOG(WARN, "have not inited");
        ret = OB_ERROR;
      }
      else if (ERROR_CODE_OK != (btree_ret = table_keybtree_.get_scan_handle(iter.get_read_handle_())))
      {
        TBSYS_LOG(WARN, "btree get scan handle fail btree_ret=%d", btree_ret);
        ret = OB_ERROR;
      }
      else
      {
        btree_ret = ERROR_CODE_OK;
        const TEKey *scan_start_key_ptr = NULL;
        const TEKey *scan_end_key_ptr = NULL;
        int start_exclude_ = start_exclude;
        int end_exclude_ = end_exclude;
        TEKey btree_min_key;
        TEKey btree_max_key;
        if (0 != min_key)
        {
          btree_ret = table_keybtree_.get_min_key(btree_min_key);
          if (ERROR_CODE_OK != btree_ret)
          {
            TBSYS_LOG(WARN, "get min key from btree fail btree_ret=%d", btree_ret);
            ret = OB_ERR_UNEXPECTED;
          }
          else
          {
            scan_start_key_ptr = &btree_min_key;
            start_exclude_ = 0;
          }
        }
        else
        {
          scan_start_key_ptr = &start_key;
        }
        if (0 != max_key)
        {
          btree_ret = table_keybtree_.get_max_key(btree_max_key);
          if (ERROR_CODE_OK != btree_ret)
          {
            TBSYS_LOG(WARN, "get max key from btree fail btree_ret=%d", btree_ret);
            ret = OB_ERR_UNEXPECTED;
          }
          else
          {
            scan_end_key_ptr = &btree_max_key;
            end_exclude_ = 0;
          }
        }
        else
        {
          scan_end_key_ptr = &end_key;
        }
        if (reverse)
        {
          std::swap(scan_start_key_ptr, scan_end_key_ptr);
          std::swap(start_exclude_, end_exclude_);
        }
        if (OB_SUCCESS == ret)
        {
          btree_ret = table_keybtree_.set_key_range(iter.get_read_handle_(),
                                              *scan_start_key_ptr, start_exclude_,
                                              *scan_end_key_ptr, end_exclude_);
          TBSYS_LOG(DEBUG, "[BTREE_SCAN_PARAM] [%s] %d [%s] %d",
                    scan_start_key_ptr->log_str(), start_exclude_, scan_end_key_ptr->log_str(), end_exclude_);
          if (ERROR_CODE_OK != btree_ret)
          {
            TBSYS_LOG(WARN, "set key range to btree scan handle fail btree_ret=%d", btree_ret);
            ret = OB_ERR_UNEXPECTED;
          }
          else
          {
            iter.set_(&table_keybtree_);
          }
        }
      }
      return ret;
    }
    //add:e
    void QueryEngine::dump2text(const char *fname)
    {
      const int64_t BUFFER_SIZE = 1024;
      char buffer[BUFFER_SIZE];
      snprintf(buffer, BUFFER_SIZE, "%s.btree.main", fname);
      FILE *fd = fopen(buffer, "w");
      if (NULL != fd)
      {
        keybtree_t::TScanHandle handle;
        TEKey btree_min_key;
        TEKey btree_max_key;
        if (ERROR_CODE_OK == keybtree_.get_scan_handle(handle)
            && ERROR_CODE_OK == keybtree_.get_min_key(btree_min_key)
            && ERROR_CODE_OK == keybtree_.get_max_key(btree_max_key))
        {
          keybtree_.set_key_range(handle, btree_min_key, 0, btree_max_key, 0);
          TEKey key;
          TEValue *value = NULL;
          MemTableGetIter get_iter;
          int64_t num = 0;
          while (ERROR_CODE_OK == keybtree_.get_next(handle, key, value)
                && NULL != value)
          {
            fprintf(fd, "[ROW_INFO][%ld] btree_key=[%s] btree_value=[%s] ptr=%p\n",
                    num, key.log_str(), value->log_str(), value);
            int64_t pos = 0;
            ObCellInfo *ci = NULL;
            get_iter.set_(key, value, NULL, true, NULL);
            while (OB_SUCCESS == get_iter.next_cell()
                  && OB_SUCCESS == get_iter.get_cell(&ci))
            {
              fprintf(fd, "          [CELL_INFO][%ld][%ld] column_id=%lu value=[%s]\n",
                      num, pos++, ci->column_id_, print_obj(ci->value_));
            }
            ++num;
          }
        }
        fclose(fd);
      }
    }

    int64_t QueryEngine::btree_size()
    {
      return keybtree_.get_object_count();
    }

    int64_t QueryEngine::btree_alloc_memory()
    {
      return keybtree_.get_alloc_memory();
    }

    int64_t QueryEngine::btree_reserved_memory()
    {
      return keybtree_.get_reserved_memory();
    }

    void QueryEngine::btree_dump_mem_info()
    {
      keybtree_.dump_mem_info();
    }

    int64_t QueryEngine::hash_size() const
    {
      return keyhash_.size();
    }

    int64_t QueryEngine::hash_bucket_using() const
    {
      return keyhash_.bucket_using();
    }

    int64_t QueryEngine::hash_uninit_unit_num() const
    {
      return keyhash_.uninit_unit_num();
    }
    //add zhaoqiong [Truncate Table]:20160318:b
    int QueryEngine::get_table_truncate_stat(uint64_t table_id, bool & is_truncated)
    {
      is_truncated = false;
      int ret = OB_SUCCESS;
      TEKey key;
      TEValue * value = NULL;
      if (table_id == OB_INVALID_ID )
      {
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        key.table_id = table_id;
        int btree_ret = ERROR_CODE_OK;
        if (ERROR_CODE_OK != (btree_ret = table_keybtree_.get(key, value))
            || NULL == value)
        {
          if (ERROR_CODE_NOT_FOUND != btree_ret)
          {
            TBSYS_LOG(WARN, "get from table_keybtree fail btree_ret=%d %s", btree_ret, key.log_str());
          }
        }
        else if (value->cell_info_cnt > 0)
        {
          is_truncated = true;
        }
      }
      return ret;
    }
    //add:e
    ////////////////////////////////////////////////////////////////////////////////////////////////////

    QueryEngineIterator::QueryEngineIterator() : keybtree_(NULL), read_handle_(), key_(), pvalue_(NULL)
    {
    }

    QueryEngineIterator::~QueryEngineIterator()
    {
    }

    int QueryEngineIterator::next()
    {
      int ret = OB_SUCCESS;
      int btree_ret = ERROR_CODE_OK;
      if (NULL == keybtree_)
      {
        ret = OB_ITER_END;
      }
      else
      {
        while (true)
        {
          if (ERROR_CODE_OK != (btree_ret = keybtree_->get_next(read_handle_, key_, pvalue_))
              || NULL == pvalue_)
          {
            if (ERROR_CODE_NOT_FOUND != btree_ret)
            {
              TBSYS_LOG(WARN, "get_next from keybtree fail btree_ret=%d pvalue_=%p", btree_ret, pvalue_);
            }
            ret = OB_ITER_END;
            break;
          }
          if (pvalue_->is_empty())
          {
            // 数据链表为空时 直接跳过这记录
            //add zhujun [transaction read uncommit]2016/3/31
            if(pvalue_->row_lock.get_uid() != 0) {
                break;
            }
            else {
                continue;
            }
            //add:e
          }
          else
          {
            break;
          }
        }
      }
      return ret;
    }

    TEValue *QueryEngineIterator::get_value()
    {
      return pvalue_;
    }

    const TEKey &QueryEngineIterator::get_key() const
    {
      return key_;
    }

    void QueryEngineIterator::reset()
    {
      keybtree_ = NULL;
      read_handle_.reset();
      pvalue_ = NULL;
    }

    void QueryEngineIterator::set_(QueryEngine::keybtree_t *keybtree)
    {
      keybtree_ = keybtree;
      pvalue_ = NULL;
    }

    QueryEngine::keybtree_t::TScanHandle &QueryEngineIterator::get_read_handle_()
    {
      return read_handle_;
    }

  }
}

