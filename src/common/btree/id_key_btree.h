#ifndef OCEANBASE_COMMON_BTREE_ID_KEY_BTREE_H_
#define OCEANBASE_COMMON_BTREE_ID_KEY_BTREE_H_

#include "btree_base.h"

namespace oceanbase
{
  namespace common
  {
    /**
     * 以不同类型的key的BTree
     */

    template<class K, class V>
    class IdKeyBtree : public BtreeBase
    {
    public:
      /**
       * 构造函数
       *
       * @param key_size key最大长度
       */
      IdKeyBtree(int32_t key_size, BtreeAlloc *key_allocator = NULL, BtreeAlloc *node_allocator = NULL);

      /**
       * 析构函数
       */
      ~IdKeyBtree();

      /**
       * 根据ID, 找到相应value, 放在value里
       *
       * @param   key     搜索的ID
       * @param   value   搜索到的值
       * @return  OK:     找到了, NOT_FOUND: 没找到
       */
      int32_t get(const uint64_t id, V &value);

      /**
       * 在BtreeReadHandle上多次查
       * 根据ID, 找到相应value, 放在value里
       *
       * @param   key     搜索的ID
       * @param   value   搜索到的值
       * @return  OK:     找到了, NOT_FOUND: 没找到
       */
      int32_t get(BtreeBaseHandle &handle, const uint64_t id, V &value);

      /**
       * 根据key, 找到相应value, 放在value里
       *
       * @param   key     搜索的主键
       * @param   value   搜索到的值
       * @return  OK:     找到了, NOT_FOUND: 没找到
       */
      int32_t get(const K &key, V &value);

      /**
       * 在BtreeReadHandle上多次查
       * 根据key, 找到相应value, 放在value里
       *
       * @param   key     搜索的主键
       * @param   value   搜索到的值
       * @return  OK:     找到了, NOT_FOUND: 没找到
       */
      int32_t get(BtreeBaseHandle &handle, const K &key, V &value);

      /**
       * 把key, value加到btree中
       *
       * @param   key     put的主键
       * @param   value   put的值
       * @param   id      内部生成的id值, 作返回用
       * @param   overwrite 是否覆盖
       * @return  OK:     成功,  KEY_EXIST: key已经存在了
       */
      int32_t put(const K &key, const V &value, uint64_t &id, const bool overwrite = false);

      /**
       * 根据ID, 从btree中删除掉
       *
       * @param   key     remove的id
       * @return  OK:     成功,  NOT_FOUND: key不存在
       */
      int32_t remove(const uint64_t id);

      /**
       * 根据key, 从btree中删除掉
       *
       * @param   key     remove的主键
       * @return  OK:     成功,  NOT_FOUND: key不存在
       */
      int32_t remove(const K &key);

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
       * 设置ID的查询范围
       *
       * @param handle 把id设置到handle里面
       * @param from_key 设置开始的id
       * @param from_exclude 设置与开始的id相等是否排除掉
       * @param to_key 设置结束的id
       * @param to_exclude 设置与结束的id相等是否排除掉
       */
      void set_key_range(BtreeReadHandle &handle, const uint64_t from_id, int32_t from_exclude,
                         const uint64_t to_id, int32_t to_exclude);

      /**
       * 取下一个值放到value上
       *
       * 例子:
       *     IdKeyBtree<string, int32_t> tree;
       *     BtreeReadHandle handle;
       *     if (tree.get_read_handle(handle) == ERROR_CODE_OK) {
       *         tree.set_key_range(handle, fromkey, 0, tokey, 0);
       *         while(tree.get_next(handle, value) == ERROR_CODE_OK) {
       *              ....
       *         }
       *     }
       *
       * @param   param   上面的search_range过的参数
       * @param   value   搜索到的值
       * @return  OK:     找到了, NOT_FOUND: 没找到
       */
      int32_t get_next(BtreeReadHandle &handle, V &value);
      int32_t get_next(BtreeReadHandle &handle, K &key, uint64_t &id, V &value);

      /**
       * 把key, value加到btree中
       *
       * @param   key     put的主键
       * @param   value   put的值
       * @param   overwrite 是否覆盖
       * @return  OK:     成功,  KEY_EXIST: key已经存在了
       */
      int32_t put(BtreeWriteHandle &handle, const K &key, const V &value, uint64_t &id, const bool overwrite = false);

      /**
       * 根据id, 从btree中删除掉
       *
       * @param   key     remove的ID
       * @return  OK:     成功,  NOT_FOUND: key不存在
       */
      int32_t remove(BtreeWriteHandle &handle, const uint64_t id);

      /**
       * 根据key, 从btree中删除掉
       *
       * @param   key     remove的主键
       * @return  OK:     成功,  NOT_FOUND: key不存在
       */
      int32_t remove(BtreeWriteHandle &handle, const K &key);

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
      /**
       * key比较函数, 被基类(BtreeBase)调用
       */
      static int64_t id_compare_func(const char *a, const char *b);
      /**
       * 把K类型的key写到alloc出来内存里
       */
      inline void set_key_to_buf(const char *p, const uint64_t id, const K *key);

    private:
      uint64_t id_;
      int32_t key_size_;  // key的大小
      BtreeDefaultAlloc key_default_allocator_;
    };

    ///////////////////////////////////////////////////////////////////////////////////////////////////
    /**
     * 以不同类型的key的BTree
     *
     */
    template<class K, class V>
    IdKeyBtree<K, V>::IdKeyBtree(int32_t key_size, BtreeAlloc *key_allocator, BtreeAlloc *node_allocator) : BtreeBase(node_allocator)
    {
      tree_count_ = 2;
      key_compare_[0] = key_compare_func;
      key_compare_[1] = id_compare_func;
      // 引用计数一个int32_t
      key_size_ = key_size + sizeof(int32_t) + sizeof(uint64_t);
      if (key_size_ > CONST_KEY_MAX_LENGTH)
        key_size_ = CONST_KEY_MAX_LENGTH;
      key_allocator_ = (key_allocator ? key_allocator : &key_default_allocator_);
      key_allocator_->init(key_size_);
      id_ = 0;
    }

    /**
     * 析构函数
     */
    template<class K, class V>
    IdKeyBtree<K, V>::~IdKeyBtree()
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
    int32_t IdKeyBtree<K, V>::get(const K &key, V &value)
    {
      BtreeBaseHandle handle;
      int32_t ret = get_read_handle(handle);
      // 得到handle
      if (ERROR_CODE_OK == ret)
        ret = get(handle, key, value);

      return ret;
    }

    /**
     * 根据BtreeBaseHandle多次取
     * 根据key, 找到相应value, 放在value里
     *
     * @param   handle  读的handle
     * @param   key     搜索的主键
     * @param   value   搜索到的值
     * @return  OK:     找到了, NOT_FOUND: 没找到
     */
    template<class K, class V>
    int32_t IdKeyBtree<K, V>::get(BtreeBaseHandle &handle, const K &key, V &value)
    {
      int32_t ret = ERROR_CODE_OK;
      // 是否取到调用过get_read_handle
      if (!handle.is_null_pointer())
      {
        char pkey[key_size_];
        set_key_to_buf(pkey, 0, &key);
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
     * 根据ID, 找到相应value, 放在value里
     *
     * @param   key     搜索的ID
     * @param   value   搜索到的值
     * @return  OK:     找到了, NOT_FOUND: 没找到
     */
    template<class K, class V>
    int32_t IdKeyBtree<K, V>::get(const uint64_t id, V &value)
    {
      BtreeBaseHandle handle;
      int32_t ret = get_read_handle(handle);
      // 得到handle
      if (ERROR_CODE_OK == ret)
        get(handle, id, value);

      return ret;
    }

    /**
     * 根据ID, 找到相应value, 放在value里
     *
     * @param   key     搜索的ID
     * @param   value   搜索到的值
     * @return  OK:     找到了, NOT_FOUND: 没找到
     */
    template<class K, class V>
    int32_t IdKeyBtree<K, V>::get(BtreeBaseHandle &handle, const uint64_t id, V &value)
    {
      int32_t ret = ERROR_CODE_OK;
      // 是否取到调用过get_read_handle
      if (!handle.is_null_pointer())
      {
        char pkey[key_size_];
        set_key_to_buf(pkey, id, NULL);
        // 搜索, 从id树搜索
        BtreeKeyValuePair *pair = get_one_pair(handle, 1, pkey);
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
    int32_t IdKeyBtree<K, V>::put(const K &key, const V &value, uint64_t &id, const bool overwrite)
    {
      BtreeWriteHandle handle;
      int32_t ret = get_write_handle(handle);
      if (ERROR_CODE_OK == ret)
      {
        // 分配内存
        char *pkey = key_allocator_->alloc();
        *(reinterpret_cast<int32_t*>(pkey)) = tree_count_;
        id = id_ ++;
        set_key_to_buf(pkey, id, &key);
        // 插入一个key
        ret = put_pair(handle, pkey, reinterpret_cast<char*>(value), overwrite);
        if (ret != ERROR_CODE_OK)
        {
          key_allocator_->release(pkey);
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
    int32_t IdKeyBtree<K, V>::remove(const K &key)
    {
      BtreeWriteHandle handle;
      int32_t ret = get_write_handle(handle);
      if (ERROR_CODE_OK == ret)
      {
        char pkey[key_size_];
        set_key_to_buf(pkey, 0, &key);
        // 删除一个key
        ret = remove_pair(handle, 0, pkey);
      }
      return ret;
    }

    /**
     * 根据ID, 从btree中删除掉
     *
     * @param   key     remove的ID
     * @return  true:   成功, false: 失败
     */
    template<class K, class V>
    int32_t IdKeyBtree<K, V>::remove(const uint64_t id)
    {
      BtreeWriteHandle handle;
      int32_t ret = get_write_handle(handle);
      if (ERROR_CODE_OK == ret)
      {
        char pkey[key_size_];
        set_key_to_buf(pkey, id, NULL);
        // 删除一个key
        ret = remove_pair(handle, 1, pkey);
      }
      return ret;
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
    void IdKeyBtree<K, V>::set_key_range(BtreeReadHandle &handle, const K *from_key,
                                         int32_t from_exclude, const K *to_key, int32_t to_exclude)
    {
      // 设置from_key
      if (from_key != get_min_key() && from_key != get_max_key())
      {
        char from_pkey[key_size_];
        set_key_to_buf(from_pkey, 0, from_key);
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
        char to_pkey[key_size_];
        set_key_to_buf(to_pkey, 0, to_key);
        handle.set_to_key_range(to_pkey, key_size_, to_exclude);
      }
      else
      {
        char *to_pkey = reinterpret_cast<char*>(const_cast<K*>(to_key));
        handle.set_to_key_range(to_pkey, 0, to_exclude);
      }
      handle.set_tree_id(0);
    }

    /**
     * 设置id的查询范围
     */
    template<class K, class V>
    void IdKeyBtree<K, V>::set_key_range(BtreeReadHandle &handle, const uint64_t from_key, int32_t from_exclude,
                                         const uint64_t to_key, int32_t to_exclude)
    {
      // 设置from_key
      handle.set_from_key_range(reinterpret_cast<char*>(from_key), 0, from_exclude);
      handle.set_to_key_range(reinterpret_cast<char*>(to_key), 0, to_exclude);
      handle.set_tree_id(1);
    }

    /**
     * 根据查询范围, 取下一个值
     */
    template<class K, class V>
    int32_t IdKeyBtree<K, V>::get_next(BtreeReadHandle &handle, V &value)
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
    int32_t IdKeyBtree<K, V>::get_next(BtreeReadHandle &handle, K &key, uint64_t &id, V &value)
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
        uint64_t* pid = reinterpret_cast<uint64_t*>(const_cast<char*>(pair->key_ + sizeof(int32_t)));
        id = (*pid);
        K* pk = reinterpret_cast<K*>(const_cast<char*>(pair->key_ + sizeof(int32_t) + sizeof(int64_t)));
        key = (*pk);
        // 设到value上返回
        value = (V)(long)pair->value_;
      }
      return ret;
    }

    /**
     * put一key, 用于批量删除
     */
    template<class K, class V>
    int32_t IdKeyBtree<K, V>::put(BtreeWriteHandle &handle, const K &key, const V &value, uint64_t &id, const bool overwrite)
    {
      int32_t ret = ERROR_CODE_OK;
      // 分配内存
      char *pkey = key_allocator_->alloc();
      *(reinterpret_cast<int32_t*>(pkey)) = tree_count_;
      id = id_ ++;
      set_key_to_buf(pkey, id, &key);
      // 插入一个key
      ret = put_pair(handle, pkey, reinterpret_cast<char*>(value), overwrite);
      // 如果失败, 释放掉key
      if (ret != ERROR_CODE_OK)
      {
        key_allocator_->release(pkey);
      }
      return ret;
    }

    /**
     * 根据key删除
     */
    template<class K, class V>
    int32_t IdKeyBtree<K, V>::remove(BtreeWriteHandle &handle, const K &key)
    {
      char pkey[key_size_];
      set_key_to_buf(pkey, 0, &key);
      // 删除key
      return remove_pair(handle, 0, pkey);
    }

    /**
     * 根据id删除
     */
    template<class K, class V>
    int32_t IdKeyBtree<K, V>::remove(BtreeWriteHandle &handle, uint64_t id)
    {
      char pkey[key_size_];
      set_key_to_buf(pkey, id, NULL);
      // 删除key
      return remove_pair(handle, 1, pkey);
    }

    /**
     * 得到最小Key
     */
    template<class K, class V>
    K* IdKeyBtree<K, V>::get_min_key() const
    {
      return reinterpret_cast<K*>(const_cast<char*>(BtreeBase::MIN_KEY));
    }

    /**
     * 得到最在Key
     */
    template<class K, class V>
    K* IdKeyBtree<K, V>::get_max_key() const
    {
      return reinterpret_cast<K*>(const_cast<char*>(BtreeBase::MAX_KEY));
    }

    /**
     * 把K类型的key写到alloc出来内存里
     */
    template<class K, class V>
    void IdKeyBtree<K, V>::set_key_to_buf(const char *p, const uint64_t id, const K *key)
    {
      uint64_t* pid = reinterpret_cast<uint64_t*>(const_cast<char*>(p + sizeof(int32_t)));
      (*pid) = id;
      if (key != NULL)
      {
        K* pk = reinterpret_cast<K*>(const_cast<char*>(p + sizeof(int32_t) + sizeof(int64_t)));
        (*pk) = (*key);
      }
    }

    /**
     * key比较函数, 注册到BTreeBase上
     */
    template<class K, class V>
    int64_t IdKeyBtree<K, V>::key_compare_func(const char *a, const char *b)
    {
      const K* pa = reinterpret_cast<const K*>(a + sizeof(int32_t) + sizeof(int64_t));
      const K* pb = reinterpret_cast<const K*>(b + sizeof(int32_t) + sizeof(int64_t));
      return ((*pa) - (*pb));
    }

    /**
     * id比较函数, 注册到BTreeBase上
     */
    template<class K, class V>
    int64_t IdKeyBtree<K, V>::id_compare_func(const char *a, const char *b)
    {
      const uint64_t *pa = reinterpret_cast<const uint64_t*>(a + sizeof(int32_t));
      const uint64_t *pb = reinterpret_cast<const uint64_t*>(b + sizeof(int32_t));
      int32_t ret = 0;
      if ((*pa) < (*pb))
        ret = -1;
      else if ((*pa) > (*pb))
        ret = 1;
      return ret;
    }

  } // end namespace common
} // end namespace oceanbase

#endif
