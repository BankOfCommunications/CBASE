////===================================================================
 //
 // ob_btree_engine.cpp / hash / common / Oceanbase
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

#include "ob_btree_engine.h"

namespace oceanbase
{
  namespace updateserver
  {
    using namespace common;

    BtreeEngine::BtreeEngine() : inited_(false), keybtree_(sizeof(TEKey), &key_alloc_, &node_alloc_), allocer_(NULL),
                                 transaction_started_(false), mutation_started_(false)
    {
    }

    BtreeEngine::~BtreeEngine()
    {
      if (inited_)
      {
        destroy();
      }
    }

    int BtreeEngine::init(MemTank *allocer)
    {
      int ret = OB_SUCCESS;
      if (inited_)
      {
        TBSYS_LOG(WARN, "have already inited");
        ret = OB_ERROR;
      }
      else if (NULL == allocer)
      {
        TBSYS_LOG(WARN, "allocer null pointer");
        ret = OB_ERROR;
      }
      else
      {
        key_alloc_.set_mem_tank(allocer);
        node_alloc_.set_mem_tank(allocer);
        allocer_ = allocer;
        inited_ = true;
      }
      return ret;
    }

    int BtreeEngine::destroy()
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
        inited_ = false;
      }
      return ret;
    }

    int BtreeEngine::clear()
    {
      int ret = OB_SUCCESS;
      if (!inited_)
      {
        TBSYS_LOG(WARN, "have not inited");
        ret = OB_ERROR;
      }
      else if (transaction_started_)
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
        keybtree_.destroy();
      }
      return ret;
    }

    int BtreeEngine::set(const TEKey &key, const TEValue &value)
    {
      int ret = OB_SUCCESS;
      TEValue *pvalue = NULL;
      if (!inited_)
      {
        TBSYS_LOG(WARN, "have not inited");
        ret = OB_ERROR;
      }
      else if (NULL == (pvalue = (TEValue*)allocer_->indep_alloc(sizeof(value))))
      {
        TBSYS_LOG(WARN, "alloc value memory fail");
        ret = OB_ERROR;
      }
      else
      {
        int btree_ret = ERROR_CODE_OK;
        *pvalue = value;
        if (ERROR_CODE_OK != (btree_ret = keybtree_.put(key, pvalue, true)))
        {
          TBSYS_LOG(WARN, "put to keybtree fail btree_ret=%d %s %s", btree_ret, key.log_str(), value.log_str());
          ret = (ERROR_CODE_ALLOC_FAIL == btree_ret) ? OB_MEM_OVERFLOW : OB_ERROR;
        }
        else
        {
          // do nothing
        }
      }
      return ret;
    }

    int BtreeEngine::set(BtreeEngineTransHandle &handle,
                        const TEKey &key,
                        const TEValue &value)
    {
      int ret = OB_SUCCESS;
      TEValue *pvalue = NULL;
      if (!inited_)
      {
        TBSYS_LOG(WARN, "have not inited");
        ret = OB_ERROR;
      }
      else if (NULL == handle.cur_write_handle_ptr_)
      {
        TBSYS_LOG(WARN, "cur write handle null pointer");
        ret = OB_ERROR;
      }
      else if (NULL == (pvalue = (TEValue*)allocer_->indep_alloc(sizeof(value))))
      {
        TBSYS_LOG(WARN, "alloc value memory fail");
        ret = OB_MEM_OVERFLOW;
      }
      else
      {
        int btree_ret = ERROR_CODE_OK;
        *pvalue = value;
        if (ERROR_CODE_OK != (btree_ret = keybtree_.put(*handle.cur_write_handle_ptr_, key, pvalue, true)))
        {
          TBSYS_LOG(WARN, "put to keybtree fail btree_ret=%d %s %s", btree_ret, key.log_str(), value.log_str());
          ret = OB_ERROR;
        }
        else
        {
          // do nothing
        }
      }
      return ret;
    }

    int BtreeEngine::get(const TEKey &key, TEValue &value)
    {
      int ret = OB_SUCCESS;
      int btree_ret = ERROR_CODE_OK;
      TEValue *pvalue = NULL;
      if (!inited_)
      {
        TBSYS_LOG(WARN, "have not inited");
        ret = OB_ERROR;
      }
      else if (ERROR_CODE_OK != (btree_ret = keybtree_.get(key, pvalue))
              || NULL == pvalue)
      {
        if (ERROR_CODE_NOT_FOUND != btree_ret)
        {
          TBSYS_LOG(WARN, "get from keybtree fail btree_ret=%d pvalue=%p %s", btree_ret, pvalue, key.log_str());
        }
        ret = OB_ENTRY_NOT_EXIST;
      }
      else
      {
        value = *pvalue;
      }
      return ret;
    }

    int BtreeEngine::get(BtreeEngineTransHandle &handle,
                        const TEKey &key, TEValue &value)
    {
      int ret = OB_SUCCESS;
      TEValue *pvalue = NULL;
      if (!inited_)
      {
        TBSYS_LOG(WARN, "have not inited");
        ret = OB_ERROR;
      }
      else
      {
        int btree_ret = ERROR_CODE_OK;
        switch (handle.trans_type_)
        {
          case READ_TRANSACTION:
            if (ERROR_CODE_OK != (btree_ret = keybtree_.get(handle.read_handle_, key, pvalue))
                || NULL == pvalue)
            {
              if (ERROR_CODE_NOT_FOUND != btree_ret)
              {
                TBSYS_LOG(WARN, "get from keybtree fail btree_ret=%d pvalue=%p %s", btree_ret, pvalue, key.log_str());
              }
              ret = OB_ENTRY_NOT_EXIST;
            }
            else
            {
              value = *pvalue;
            }
            break;
          case WRITE_TRANSACTION:
            if (NULL == handle.cur_write_handle_ptr_)
            {
              TBSYS_LOG(WARN, "cur write handle null pointer");
              ret = OB_ERROR;
            }
            else if (ERROR_CODE_OK != (btree_ret = keybtree_.get(*handle.cur_write_handle_ptr_, key, pvalue))
                    || NULL == pvalue)
            {
              if (ERROR_CODE_NOT_FOUND != btree_ret)
              {
                TBSYS_LOG(WARN, "get from keybtree fail btree_ret=%d pvalue=%p %s", btree_ret, pvalue, key.log_str());
              }
              ret = OB_ENTRY_NOT_EXIST;
            }
            else
            {
              value = *pvalue;
            }
            break;
          default:
            TBSYS_LOG(WARN, "invalid trans_type=%d", handle.trans_type_);
            ret = OB_ERROR;
            break;
        }
      }
      return ret;
    }

    int BtreeEngine::scan(BtreeEngineTransHandle &handle,
                          const TEKey &start_key,
                          const int min_key,
                          const int start_exclude,
                          const TEKey &end_key,
                          const int max_key,
                          const int end_exclude,
                          const bool reverse,
                          BtreeEngineIterator &iter)
    {
      int ret = OB_SUCCESS;
      if (!inited_)
      {
        TBSYS_LOG(WARN, "have not inited");
        ret = OB_ERROR;
      }
      else
      {
        const TEKey *btree_start_key_ptr = NULL;
        const TEKey *btree_end_key_ptr = NULL;
        int start_exclude_ = start_exclude;
        int end_exclude_ = end_exclude;
        if (0 != min_key)
        {
          btree_start_key_ptr = keybtree_.get_min_key();
        }
        else
        {
          btree_start_key_ptr = &start_key;
        }
        if (0 != max_key)
        {
          btree_end_key_ptr = keybtree_.get_max_key();
        }
        else
        {
          btree_end_key_ptr = &end_key;
        }
        if (reverse)
        {
          std::swap(btree_start_key_ptr, btree_end_key_ptr);
          std::swap(start_exclude_, end_exclude_);
        }
        keybtree_.set_key_range(handle.read_handle_,
                                btree_start_key_ptr, start_exclude_,
                                btree_end_key_ptr, end_exclude_);
        iter.set_(&keybtree_, &(handle.read_handle_));
      }
      return ret;
    }

    int BtreeEngine::start_transaction(const TETransType type,
                                      BtreeEngineTransHandle &handle)
    {
      int ret = OB_SUCCESS;
      if (!inited_)
      {
        TBSYS_LOG(WARN, "have not inited");
        ret = OB_ERROR;
      }
      else
      {
        int btree_ret = ERROR_CODE_OK;
        switch (type)
        {
          case READ_TRANSACTION:
            if(ERROR_CODE_OK != (btree_ret = keybtree_.get_read_handle(handle.read_handle_)))
            {
              TBSYS_LOG(WARN, "btree get_read_handle fail ret=%d", btree_ret);
              ret = OB_ERROR;
            }
            else
            {
              handle.trans_type_ = READ_TRANSACTION;
            }
            break;
          case WRITE_TRANSACTION:
            if (transaction_started_)
            {
              TBSYS_LOG(WARN, "there is one transaction have been started");
              ret = OB_UPS_TRANS_RUNNING;
            }
            else if(ERROR_CODE_OK != (btree_ret = keybtree_.get_write_handle(handle.write_handle_)))
            {
              TBSYS_LOG(WARN, "btree get_write_handle fail ret=%d", btree_ret);
              ret = OB_ERROR;
            }
            else
            {
              transaction_started_ = true;
              handle.trans_type_ = WRITE_TRANSACTION;
              handle.cur_write_handle_ptr_ = &handle.write_handle_;
            }
            break;
          default:
            ret = OB_ERROR;
            break;
        }
      }
      return ret;
    }

    int BtreeEngine::end_transaction(BtreeEngineTransHandle &handle, bool rollback)
    {
      int ret = OB_SUCCESS;
      if (!inited_)
      {
        TBSYS_LOG(WARN, "have not inited");
        ret = OB_ERROR;
      }
      else
      {
        switch (handle.trans_type_)
        {
          case READ_TRANSACTION:
            handle.read_handle_.clear();
            break;
          case WRITE_TRANSACTION:
            if (!transaction_started_)
            {
              TBSYS_LOG(WARN, "there is no transaction started");
              ret = OB_ERROR;
            }
            else if (mutation_started_)
            {
              TBSYS_LOG(WARN, "mutation have not end");
              ret = OB_ERROR;
            }
            else
            {  
              if (!rollback)
              {
                handle.write_handle_.end();
              }
              else
              {
                handle.write_handle_.rollback();
                handle.write_handle_.end();
              }
              handle.cur_write_handle_ptr_ = NULL;
              transaction_started_ = false;
            }
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
    }

    int BtreeEngine::start_mutation(BtreeEngineTransHandle &handle)
    {
      int ret = OB_SUCCESS;
      if (!inited_)
      {
        TBSYS_LOG(WARN, "have not inited");
        ret = OB_ERROR;
      }
      else if (!transaction_started_)
      {
        TBSYS_LOG(WARN, "there is no transaction started");
        ret = OB_ERROR;
      }
      else if (mutation_started_)
      {
        TBSYS_LOG(WARN, "there is one mutation have been started");
        ret = OB_ERROR;
      }
      else if (NULL == (handle.cur_write_handle_ptr_ = (handle.write_handle_.get_write_handle())))
      {
        TBSYS_LOG(WARN, "get write handle fail");
        ret = OB_ERROR;
        handle.cur_write_handle_ptr_ = &handle.write_handle_;
      }
      else
      {
        mutation_started_ = true;
      }
      return ret;
    }

    int BtreeEngine::end_mutation(BtreeEngineTransHandle &handle, bool rollback)
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
      else if (NULL == handle.cur_write_handle_ptr_)
      {
        TBSYS_LOG(WARN, "cur write handle null pointer");
        ret = OB_ERROR;
      }
      else
      {
        if (rollback)
        {
          handle.cur_write_handle_ptr_->rollback();
        }
        else
        {
          handle.cur_write_handle_ptr_->end();
        }
        handle.cur_write_handle_ptr_ = &handle.write_handle_;
        mutation_started_ = false;
      }
      return ret;
    }

    int BtreeEngine::create_index()
    {
      int ret = OB_SUCCESS;
      if (!inited_)
      {
        TBSYS_LOG(WARN, "have not inited");
        ret = OB_ERROR;
      }
      return ret;
    }

    void BtreeEngine::dump2text(const char *fname)
    {
      const int64_t BUFFER_SIZE = 1024;
      char buffer[BUFFER_SIZE];
      snprintf(buffer, BUFFER_SIZE, "%s.btree.main", fname);
      FILE *fd = fopen(buffer, "w");
      if (NULL != fd)
      {
        BtreeReadHandle handle;
        if (ERROR_CODE_OK == keybtree_.get_read_handle(handle))
        {
          keybtree_.set_key_range(handle, keybtree_.get_min_key(), 0, keybtree_.get_max_key(), 0);
          TEKey key;
          TEValue *value = NULL;
          int64_t num = 0;
          while (ERROR_CODE_OK == keybtree_.get_next(handle, key, value)
                && NULL != value)
          {
            fprintf(fd, "[ROW_INFO][%ld] btree_key=[%s] btree_value=[%s] ptr=%p\n",
                    num, key.log_str(), value->log_str(), value);
            int64_t pos = 0;
            ObCellInfoNode *iter = value->list_head;
            while (NULL != iter)
            {
              fprintf(fd, "          [CELL_INFO][%ld][%ld] column_id=%lu value=[%s] ptr=%p\n",
                      num, pos++, iter->cell_info.column_id_,
                      print_obj(iter->cell_info.value_), iter);
              if (value->list_tail == iter)
              {
                break;
              }
              iter = iter->next;;
            }
            ++num;
          }
        }
        fclose(fd);
      }
    }

    int64_t BtreeEngine::size()
    {
      return keybtree_.get_object_count();
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////

    BtreeEngineIterator::BtreeEngineIterator() : keybtree_(NULL), read_handle_ptr_(NULL), key_(), pvalue_(NULL)
    {
    }

    BtreeEngineIterator::~BtreeEngineIterator()
    {
    }

    int BtreeEngineIterator::next()
    {
      int ret = OB_SUCCESS;
      int btree_ret = ERROR_CODE_OK;
      if (NULL == keybtree_
          || NULL == read_handle_ptr_)
      {
        //TBSYS_LOG(WARN, "invalid internal pointer keybtree=%p read_handle_ptr=%p", keybtree_, read_handle_ptr_);
        ret = OB_ITER_END;
      }
      else if (ERROR_CODE_OK != (btree_ret = keybtree_->get_next(*read_handle_ptr_, key_, pvalue_))
              || NULL == pvalue_)
      {
        if (ERROR_CODE_NOT_FOUND != btree_ret)
        {
          TBSYS_LOG(WARN, "get_next from keybtree fail btree_ret=%d pvalue_=%p", btree_ret, pvalue_);
        }
        ret = OB_ITER_END;
      }
      else
      {
        // do nothing
      }
      return ret;
    }

    int BtreeEngineIterator::get_value(TEValue &value) const
    {
      int ret = OB_SUCCESS;
      if (NULL == pvalue_)
      {
        ret = OB_ERROR;
      }
      else
      {
        value = *pvalue_;
      }
      return ret;
    }

    int BtreeEngineIterator::get_key(TEKey &key) const
    {
      int ret = OB_SUCCESS;
      if (NULL == keybtree_
          || NULL == read_handle_ptr_)
      {
        ret = OB_ERROR;
      }
      else
      {
        key = key_;
      }
      return ret;
    }

    void BtreeEngineIterator::reset()
    {
      keybtree_ = NULL;
      read_handle_ptr_ = NULL;
      pvalue_ = NULL;
    }

    void BtreeEngineIterator::set_(BtreeEngine::keybtree_t *keybtree, BtreeReadHandle *read_handle_ptr)
    {
      keybtree_ = keybtree;
      read_handle_ptr_ = read_handle_ptr;
      pvalue_ = NULL;
      next();
    }
  }
}

