#ifndef OCEANBASE_COMMON_BTREE_KEY_BTREE_H_
#define OCEANBASE_COMMON_BTREE_KEY_BTREE_H_

#include "btree_base.h"

namespace oceanbase
{
  namespace common
  {
    /**
     * 以不同类型的key的BTree
     */

    template<class K, class V>
    class KeyBtree : public BtreeBase
    {
    public:
      /**
       * 构造函数
       *
       * @param key_size key最大长度
       */
      KeyBtree(int32_t key_size, BtreeAlloc *key_allocator = NULL, BtreeAlloc *node_allocator = NULL);

      /**
       * 析构函数
       */
      ~KeyBtree();

      /**
       * 根据key, 找到相应value, 放在value里
       *
       * @param   key     搜索的主键
       * @param   value   搜索到的值
       * @return  OK:     找到了, NOT_FOUND: 没找到
       */
      int32_t get(const K &key, V &value);

      /*
       * 需要先 get_read_handle 然后用这个函数
       * @param   handle  读的handle
       * @param   value   搜索到的值
       * @return  OK:     找到了, NOT_FOUND: 没找到
       */
      int32_t get(BtreeBaseHandle &handle, const K &key, V &value);

      /**
       * 把key, value加到btree中
       *
       * @param   key     put的主键
       * @param   value   put的值
       * @param   overwrite 是否覆盖
       * @return  OK:     成功,  KEY_EXIST: key已经存在了
       */
      int32_t put(const K &key, const V &value, const bool overwrite = false, K **stored_key = NULL);
      int32_t put(const K &key, const V &value, V &old_value, const bool key_overwrite = false, const bool value_overwrite = true);

      /**
       * 根据key, 从btree中删除掉
       *
       * @param   key     remove的主键
       * @return  OK:     成功,  NOT_FOUND: key不存在
       */
      int32_t remove(const K &key);
      int32_t remove(const K &key, V &old_value);

      /**
       * 设置key的查询范围
       *
       * @param handle 把key设置到handle里面
       * @param from_key 设置开始的key
       * @param from_exclude 设置与开始的key相等是否排除掉
       * @param to_key 设置结束的key
       * @param to_exclude 设置与结束的key相等是否排除掉
       */
      void set_key_range(BtreeReadHandle &handle, const K *from_key, int32_t from_exclude,
                         const K *to_key, int32_t to_exclude);

      /**
       * 取下一个值放到value上
       *
       * 例子:
       *     KeyBtree<string, int32_t> tree;
       *     BtreeReadHandle handle;
       *     if (tree.get_read_handle(handle) == ERROR_CODE_OK) {
       *         tree.set_key_range(handle, fromkey, 0, tokey, 0);
       *         while(tree.get_next(handle, value) == ERROR_CODE_OK) {
       *              ....
       *         }
       *     }
       *
       * @param   handle   上面的search_range过的参数
       * @param   value   搜索到的值
       * @return  OK:     找到了, NOT_FOUND: 没找到
       */
      int32_t get_next(BtreeReadHandle &handle, V &value);
      int32_t get_next(BtreeReadHandle &handle, K &key, V &value);

      /**
       * 把key, value加到btree中
       *
       * @param   key     put的主键
       * @param   value   put的值
       * @param   overwrite 是否覆盖
       * @return  OK:     成功,  KEY_EXIST: key已经存在了
       */
      int32_t put(BtreeWriteHandle &handle, const K &key, const V &value, const bool overwrite = false);
      int32_t put(BtreeWriteHandle &handle, const K &key, const V &value, V &old_value);

      /**
       * 根据key, 从btree中删除掉
       *
       * @param   key     remove的主键
       * @return  OK:     成功,  NOT_FOUND: key不存在
       */
      int32_t remove(BtreeWriteHandle &handle, const K &key);
      int32_t remove(BtreeWriteHandle &handle, const K &key, V &old_value);

      /**
       * 得到最小Key
       */
      K* get_min_key() const;

      /**
       * 得到最在Key
       */
      K* get_max_key() const;

    private:

      /**
       * key比较函数, 被基类(BtreeBase)调用
       */
      static int64_t key_compare_func(const char *a, const char *b);
      static int64_t nokey_compare_func(const char *a, const char *b);
      /**
       * 把K类型的key写到alloc出来内存里
       */
      inline char *set_key_to_buf(char *pkey, const K& key);

    private:
      int32_t key_size_;  // key的大小
      int32_t type_size_;  // key的大小
      BtreeDefaultAlloc key_default_allocator_;
    };

    ///////////////////////////////////////////////////////////////////////////////////////////////////
    /**
     * 以不同类型的key的BTree
     *
     */
    template<class K, class V>
    KeyBtree<K, V>::KeyBtree(int32_t key_size, BtreeAlloc *key_allocator,
                             BtreeAlloc *node_allocator) : BtreeBase(node_allocator)
    {
      tree_count_ = 1;
      // 引用计数一个int32_t
      key_size_ = static_cast<int32_t>(key_size + sizeof(int32_t));
      if (key_size_ > CONST_KEY_MAX_LENGTH)
        key_size_ = CONST_KEY_MAX_LENGTH;
      type_size_ = 0;
      key_allocator_ = (key_allocator ? key_allocator : &key_default_allocator_);
      if (key_allocator_ && key_size_ > static_cast<int32_t>(sizeof(char *) + sizeof(int32_t)))
      {
        key_allocator_->init(key_size_);
        key_compare_[0] = key_compare_func;
      }
      else
      {
        key_allocator_ = NULL;
        key_compare_[0] = nokey_compare_func;
        type_size_ = static_cast<int32_t>(key_size_ - sizeof(int32_t));
        key_size_ = 0;
      }
      set_write_lock_enable(true);
    }

    /**
     * 析构函数
     */
    template<class K, class V>
    KeyBtree<K, V>::~KeyBtree()
    {
    }

    /**
     * 根据key, 找到相应value, 放在value里
     *
     * @param   key     搜索的主键
     * @param   value   搜索到的值
     * @return  OK:     找到了, NOT_FOUND: 没找到
     */
    template<class K, class V>
    int32_t KeyBtree<K, V>::get(const K &key, V &value)
    {
      BtreeBaseHandle handle;
      int32_t ret = get_read_handle(handle);
      // 得到handle
      if (ERROR_CODE_OK == ret)
        ret = get(handle, key, value);

      return ret;
    }

    /*
     * 需要先 get_read_handle 然后用这个函数
     * @param   handle  读的handle
     * @param   value   搜索到的值
     * @return  OK:     找到了, NOT_FOUND: 没找到
     */
    template<class K, class V>
    int32_t KeyBtree<K, V>::get(BtreeBaseHandle &handle, const K &key, V &value)
    {
      int32_t ret = ERROR_CODE_OK;
      // 是否取到调用过get_read_handle
      if (!handle.is_null_pointer())
      {
        char tmpkey[key_size_];
        char *pkey = set_key_to_buf(tmpkey, key);
        // 搜索
        BtreeKeyValuePair *pair = get_one_pair(handle, 0, pkey);
        if (NULL == pair)
        {
          ret = ERROR_CODE_NOT_FOUND;
        }
        else
        {
          // 设到value上返回
          value = (V)(long)pair->value_;
        }
      }
      else
      {
        ret = ERROR_CODE_NOT_FOUND;
      }
      return ret;
    }

    /**
     * 把key, value加到btree中
     *
     * @param   key     put的主键
     * @param   value   put的值
     * @param   overwrite 是否覆盖
     * @return  true:   成功, false: 失败
     */
    template<class K, class V>
    int32_t KeyBtree<K, V>::put(const K &key, const V &value, const bool overwrite, K **stored_key)
    {
      BtreeWriteHandle handle;
      int32_t ret = get_write_handle(handle);
      if (ERROR_CODE_OK != ret)
      {
        TBSYS_LOG(ERROR, "get_write_handle()=>%d", ret);
      }
      if (ERROR_CODE_OK == ret)
      {
        // 分配内存
        char *pkey = set_key_to_buf(NULL, key);
        if (NULL == pkey) 
        {
          TBSYS_LOG(ERROR, "alloc memory fail");
          ret = ERROR_CODE_ALLOC_FAIL;
        }
        else
        {
          // 插入一个key
          ret = put_pair(handle, pkey, reinterpret_cast<char*>(value), overwrite);
          if (ERROR_CODE_OK != ret)
          {
            if (ERROR_CODE_KEY_REPEAT != ret)
            {
              TBSYS_LOG(ERROR, "put_pair()=>%d", ret);
            }
          }
          if (key_allocator_ && ret != ERROR_CODE_OK)
          {
            key_allocator_->release(pkey);
          }
          if (ERROR_CODE_OK == ret
              && NULL != stored_key)
          {
            *stored_key = reinterpret_cast<K*>(pkey + sizeof(int32_t));
          }
        }
      }
      return ret;
    }

    template<class K, class V>
    int32_t KeyBtree<K, V>::put(const K &key, const V &value, V &old_value, const bool key_overwrite, const bool value_overwrite)
    {
      BtreeWriteHandle handle;
      int32_t ret = get_write_handle(handle);
      if (ERROR_CODE_OK == ret)
      {
        // 分配内存
        char *pkey = set_key_to_buf(NULL, key);
        if (NULL == pkey) 
        {
          TBSYS_LOG(ERROR, "alloc memory fail");
          ret = ERROR_CODE_ALLOC_FAIL;
        }
        else
        {
          // 插入一个key
          ret = put_pair(handle, pkey, reinterpret_cast<char*>(value), value_overwrite);
          old_value = (V)(long)handle.get_old_value();
          if (ERROR_CODE_OK == ret)
          {
            // 覆盖旧Key
            char *ptr = handle.get_old_key();

            if (key_overwrite && ptr)
            {
              if (key_allocator_)
              {
                K *pk = reinterpret_cast<K *>(const_cast<char *>(ptr + sizeof(int32_t)));
                *pk = key;
              }
              else
              {
                memcpy(&ptr, &key, type_size_);
              }
            }
          }
          else if (key_allocator_)
          {
            key_allocator_->release(pkey);
          }
        }
      }
      return ret;
    }

    /**
     * 根据key, 从btree中删除掉
     *
     * @param   key     remove的主键
     * @return  true:   成功, false: 失败
     */
    template<class K, class V>
    int32_t KeyBtree<K, V>::remove(const K &key)
    {
      BtreeWriteHandle handle;
      int32_t ret = get_write_handle(handle);
      if (ERROR_CODE_OK == ret)
      {
        char tmpkey[key_size_];
        char *pkey = set_key_to_buf(tmpkey, key);
        // 删除一个key
        ret = remove_pair(handle, 0, pkey);
      }
      return ret;
    }

    template<class K, class V>
    int32_t KeyBtree<K, V>::remove(const K &key, V &value)
    {
      BtreeWriteHandle handle;
      int32_t ret = get_write_handle(handle);
      if (ERROR_CODE_OK == ret)
      {
        char tmpkey[key_size_];
        char *pkey = set_key_to_buf(tmpkey, key);
        // 删除一个key
        ret = remove_pair(handle, 0, pkey);
        value = (V)(long)handle.get_old_value();
      }
      return ret;
    }

    /**
     * key比较函数, 注册到BTreeBase上
     */
    template<class K, class V>
    int64_t KeyBtree<K, V>::key_compare_func(const char *a, const char *b)
    {
      const K* pa = reinterpret_cast<const K*>(a + sizeof(int32_t));
      const K* pb = reinterpret_cast<const K*>(b + sizeof(int32_t));
      return ((*pa) - (*pb));
    }

    template<class K, class V>
    int64_t KeyBtree<K, V>::nokey_compare_func(const char *a, const char *b)
    {
      return (a - b);
    }

    /**
     * 设置key的查询范围
     *
     * @param handle 把key设置到handle里面
     * @param from_key 设置开始的key
     * @param from_exclude 设置与开始的key相等是否排除掉
     * @param to_key 设置结束的key
     * @param to_exclude 设置与结束的key相等是否排除掉
     */
    template<class K, class V>
    void KeyBtree<K, V>::set_key_range(BtreeReadHandle &handle, const K *from_key,
                                       int32_t from_exclude, const K *to_key, int32_t to_exclude)
    {
      // 设置from_key
      if (from_key != get_min_key() && from_key != get_max_key())
      {
        char from_tmpkey[key_size_];
        char *from_pkey = set_key_to_buf(from_tmpkey, *from_key);
        handle.set_from_key_range(from_pkey, key_size_, from_exclude);
      }
      else
      {
        char *from_pkey = reinterpret_cast<char*>(const_cast<K*>(from_key));
        handle.set_from_key_range(from_pkey, 0, from_exclude);
      }
      // 设置to_key
      if (to_key != get_min_key() && to_key != get_max_key())
      {
        char to_tmpkey[key_size_];
        char *to_pkey = set_key_to_buf(to_tmpkey, *to_key);
        handle.set_to_key_range(to_pkey, key_size_, to_exclude);
      }
      else
      {
        char *to_pkey = reinterpret_cast<char*>(const_cast<K*>(to_key));
        handle.set_to_key_range(to_pkey, 0, to_exclude);
      }
      handle.set_tree_id(0);
    }

    template<class K, class V>
    int32_t KeyBtree<K, V>::get_next(BtreeReadHandle &handle, V &value)
    {
      int32_t ret = ERROR_CODE_OK;
      // 下一个value
      BtreeKeyValuePair *pair = get_range_pair(handle);
      if (NULL == pair)
      {
        ret = ERROR_CODE_NOT_FOUND;
      }
      else
      {
        // 设到value上返回
        value = (V)(long)pair->value_;
      }
      return ret;
    }

    template<class K, class V>
    int32_t KeyBtree<K, V>::get_next(BtreeReadHandle &handle, K &key, V &value)
    {
      int32_t ret = ERROR_CODE_OK;
      // 下一个value
      BtreeKeyValuePair *pair = get_range_pair(handle);
      if (NULL == pair)
      {
        ret = ERROR_CODE_NOT_FOUND;
      }
      else
      {
        // 设到key上返回
        if (key_allocator_)
        {
          K* pk = reinterpret_cast<K*>(const_cast<char*>(pair->key_ + sizeof(int32_t)));
          key = *pk;
        }
        else
        {
          memcpy(&key, &pair->key_, type_size_);
        }
        // 设到value上返回
        value = (V)(long)pair->value_;
      }
      return ret;
    }

    template<class K, class V>
    int32_t KeyBtree<K, V>::put(BtreeWriteHandle &handle, const K &key, const V &value, const bool overwrite)
    {
      int32_t ret = ERROR_CODE_OK;
      // 分配内存
      char *pkey = set_key_to_buf(NULL, key);
      if (NULL == pkey) 
      {
        TBSYS_LOG(ERROR, "alloc memory fail");
        ret = ERROR_CODE_ALLOC_FAIL;
      }
      else
      {
        // 插入一个key
        ret = put_pair(handle, pkey, reinterpret_cast<char*>(value), overwrite);
        // 如果失败, 释放掉key
        if (key_allocator_ && ret != ERROR_CODE_OK)
        {
          key_allocator_->release(pkey);
        }
      }
      return ret;
    }

    template<class K, class V>
    int32_t KeyBtree<K, V>::put(BtreeWriteHandle &handle, const K &key, const V &value, V &old_value)
    {
      int32_t ret = ERROR_CODE_OK;
      // 分配内存
      char *pkey = set_key_to_buf(NULL, key);
      if (NULL == pkey) 
      {
        TBSYS_LOG(ERROR, "alloc memory fail");
        ret = ERROR_CODE_ALLOC_FAIL;
      }
      else
      {
        // 插入一个key
        ret = put_pair(handle, pkey, reinterpret_cast<char*>(value), true);
        old_value = (V)(long)handle.get_old_value();
        // 如果失败, 释放掉key
        if (key_allocator_ && ret != ERROR_CODE_OK)
        {
          key_allocator_->release(pkey);
        }
      }
      return ret;
    }

    template<class K, class V>
    int32_t KeyBtree<K, V>::remove(BtreeWriteHandle &handle, const K &key)
    {
      char tmpkey[key_size_];
      char *pkey = set_key_to_buf(tmpkey, key);
      // 删除key
      return remove_pair(handle, 0, pkey);
    }

    template<class K, class V>
    int32_t KeyBtree<K, V>::remove(BtreeWriteHandle &handle, const K &key, V &value)
    {
      char tmpbuf[key_size_];
      char *pkey = set_key_to_buf(tmpbuf, key);
      // 删除key
      int ret = remove_pair(handle, 0, pkey);
      value = (V)(long)handle.get_old_value();
      return ret;
    }

    /**
     * 得到最小Key
     */
    template<class K, class V>
    K* KeyBtree<K, V>::get_min_key() const
    {
      return reinterpret_cast<K*>(const_cast<char*>(BtreeBase::MIN_KEY));
    }

    /**
     * 得到最在Key
     */
    template<class K, class V>
    K* KeyBtree<K, V>::get_max_key() const
    {
      return reinterpret_cast<K*>(const_cast<char*>(BtreeBase::MAX_KEY));
    }

    /**
     * 把K类型的key写到alloc出来内存里
     */
    template<class K, class V>
    char *KeyBtree<K, V>::set_key_to_buf(char *tmpkey, const K& key)
    {
      char *pkey = tmpkey;
      if (key_allocator_ == NULL)
      {
        pkey = NULL;
        memcpy(&pkey, &key, type_size_);
      }
      else
      {
        if (pkey == NULL)
        {
          pkey = key_allocator_->alloc();
        }
        if (NULL == pkey)
        {
          TBSYS_LOG(ERROR, "alloc memory fail");
        }
        else
        {
          *(reinterpret_cast<int32_t*>(pkey)) = 1;
          K* pk = reinterpret_cast<K*>(pkey + sizeof(int32_t));
          (*pk) = key;
        }
      }
      return pkey;
    }

  } // end namespace common
} // end namespace oceanbase

#endif
