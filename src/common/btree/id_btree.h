#ifndef OCEANBASE_COMMON_BTREE_ID_BTREE_H_
#define OCEANBASE_COMMON_BTREE_ID_BTREE_H_

#include "btree_base.h"

namespace oceanbase
{
  namespace common
  {
    /**
     * 以内部生成序号为ID做为key
     */

    template<class V>
    class IdBtree : public BtreeBase
    {
    public:
      /**
       * 构造函数
       */
      IdBtree();

      /**
       * 析构函数
       */
      ~IdBtree();

      /**
       * 根据key, 找到相应value, 放在value里
       *
       * @param   key     搜索的主键
       * @param   value   搜索到的值
       * @return  OK:     找到了, NOT_FOUND: 没找到
       */
      int32_t get(const uint64_t key, V &value);

      /**
       * 在BtreeReadHandle上查
       * 根据key, 找到相应value, 放在value里
       *
       * @param   handle  读的handle
       * @param   key     搜索的主键
       * @param   value   搜索到的值
       * @return  OK:     找到了, NOT_FOUND: 没找到
       */
      int32_t get(BtreeBaseHandle &handle, const uint64_t key, V &value);

      /**
       * 把key, value加到btree中
       *
       * @param   key     put的主键, 由idtree自动产生返回
       * @param   value   put的值
       * @param   overwrite 是否覆盖
       * @return  OK:     成功,  KEY_EXIST: key已经存在了
       */
      int32_t put(uint64_t &key, const V &value, const bool overwrite = false);

      /**
       * 根据key, 从btree中删除掉
       *
       * @param   key     remove的主键
       * @return  OK:     成功,  NOT_FOUND: key不存在
       */
      int32_t remove(const uint64_t key);

      /**
       * 设置key的查询范围
       *
       * @param handle 把key设置到handle里面
       * @param from_key 设置开始的key
       * @param from_exclude 设置与开始的key相等是否排除掉
       * @param to_key 设置结束的key
       * @param to_exclude 设置与结束的key相等是否排除掉
       */
      void set_key_range(BtreeReadHandle &handle, const uint64_t from_key, int32_t from_exclude,
                         const uint64_t to_key, int32_t to_exclude);

      /**
       * 取下一个值放到value上
       *
       * 例子:
       *     IdBtree tree;
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
      int32_t get_next(BtreeReadHandle &handle, uint64_t &key, V &value);

      /**
       * 把key, value加到btree中
       *
       * @param   key     put的主键, 由idtree自动产生返回
       * @param   value   put的值
       * @param   overwrite 是否覆盖
       * @return  OK:     成功,  KEY_EXIST: key已经存在了
       */
      int32_t put(BtreeWriteHandle &handle, uint64_t &key, const V &value, const bool overwrite = false);

      /**
       * 根据key, 从btree中删除掉
       *
       * @param   key     remove的主键
       * @return  OK:     成功,  NOT_FOUND: key不存在
       */
      int32_t remove(BtreeWriteHandle &handle, const uint64_t key);

      /**
       * 得到最小Key
       */
      uint64_t get_min_key() const;

      /**
       * 得到最在Key
       */
      uint64_t get_max_key() const;

    private:

      /**
       * key比较函数, 被基类(BtreeBase)调用
       */
      static int64_t key_compare_func(const char *a, const char *b);

    private:
      uint64_t id_;
    };

    ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
     * 构造函数
     */
    template<class V>
    IdBtree<V>::IdBtree()
    {
      tree_count_ = 1;
      key_compare_[0] = key_compare_func;
      id_ = 0;
    }

    /**
     * 析构函数
     */
    template<class V>
    IdBtree<V>::~IdBtree()
    {
    }

    /**
     * 根据key, 找到相应value, 放在value里
     *
     * @param   key     搜索的主键
     * @param   value   搜索到的值
     * @return  OK:     找到了, NOT_FOUND: 没找到
     */
    template<class V>
    int32_t IdBtree<V>::get(const uint64_t key, V &value)
    {
      BtreeBaseHandle handle;
      int32_t ret = get_read_handle(handle);
      // 得到handle
      if (ERROR_CODE_OK == ret)
        ret = get(handle, key, value);

      return ret;
    }

    /**
     * 根据key, 找到相应value, 放在value里
     *
     * @param   handle  读的handle
     * @param   key     搜索的主键
     * @param   value   搜索到的值
     * @return  OK:     找到了, NOT_FOUND: 没找到
     */
    template<class V>
    int32_t IdBtree<V>::get(BtreeBaseHandle &handle, const uint64_t key, V &value)
    {
      int32_t ret = ERROR_CODE_OK;
      // 是否取到调用过get_read_handle
      if (!handle.is_null_pointer())
      {
        // 搜索
        BtreeKeyValuePair *pair = get_one_pair(handle, 0, reinterpret_cast<char*>(key));
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
     * 内部产生key 及 value加到btree中
     *
     * @param   key     put的主键, 由内部产生
     * @param   value   put的值
     * @param   overwrite 是否覆盖
     * @return  OK:     成功,  KEY_EXIST: key已经存在了
     */
    template<class V>
    int32_t IdBtree<V>::put(uint64_t &key, const V &value, const bool overwrite)
    {
      BtreeWriteHandle handle;
      int32_t ret = get_write_handle(handle);
      if (ERROR_CODE_OK == ret)
      {
        // 插入一个key
        key = id_ ++;
        ret = put_pair(handle, reinterpret_cast<char*>(key), reinterpret_cast<char*>(value), overwrite);
      }
      return ret;
    }

    /**
     * 根据key, 从btree中删除掉
     *
     * @param   key     remove的主键
     * @return  OK:     成功,  NOT_FOUND: key不存在
     */
    template<class V>
    int32_t IdBtree<V>::remove(const uint64_t key)
    {
      BtreeWriteHandle handle;
      int32_t ret = get_write_handle(handle);
      if (ERROR_CODE_OK == ret)
      {
        // 删除一个key
        ret = remove_pair(handle, 0, reinterpret_cast<char*>(key));
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
    template<class V>
    void IdBtree<V>::set_key_range(BtreeReadHandle &handle, const uint64_t from_key, int32_t from_exclude,
                                   const uint64_t to_key, int32_t to_exclude)
    {
      // 设置from_key
      handle.set_from_key_range(reinterpret_cast<char*>(from_key), 0, from_exclude);
      handle.set_to_key_range(reinterpret_cast<char*>(to_key), 0, to_exclude);
      handle.set_tree_id(0);
    }

    /**
     * 取下一个值放到value上
     *
     * 例子:
     *     IdBtree tree;
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
    template<class V>
    int32_t IdBtree<V>::get_next(BtreeReadHandle &handle, V &value)
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

    template<class V>
    int32_t IdBtree<V>::get_next(BtreeReadHandle &handle, uint64_t &key, V &value)
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
        key = reinterpret_cast<uint64_t>(pair->key_);
        value = (V)(long)pair->value_;
      }
      return ret;
    }

    /**
     * 把key, value加到btree中
     *
     * @param   key     put的主键, 由内部产生
     * @param   value   put的值
     * @param   overwrite 是否覆盖
     * @return  OK:     成功,  KEY_EXIST: key已经存在了
     */
    template<class V>
    int32_t IdBtree<V>::put(BtreeWriteHandle &handle, uint64_t &key, const V &value, const bool overwrite)
    {
      key = id_ ++;
      return put_pair(handle, reinterpret_cast<char*>(key), reinterpret_cast<char*>(value), overwrite);
    }

    /**
     * 根据key, 从btree中删除掉
     *
     * @param   key     remove的主键
     * @return  OK:     成功,  NOT_FOUND: key不存在
     */
    template<class V>
    int32_t IdBtree<V>::remove(BtreeWriteHandle &handle, const uint64_t key)
    {
      return remove_pair(handle, 0, reinterpret_cast<char*>(key));
    }


    /**
     * 得到最小Key
     */
    template<class V>
    uint64_t IdBtree<V>::get_min_key() const
    {
      return reinterpret_cast<uint64_t>(BtreeBase::MIN_KEY);
    }

    /**
     * 得到最在Key
     */
    template<class V>
    uint64_t IdBtree<V>::get_max_key() const
    {
      return reinterpret_cast<uint64_t>(BtreeBase::MAX_KEY);
    }


    /**
     * key比较函数, 被基类(BtreeBase)调用
     */
    template<class V>
    int64_t IdBtree<V>::key_compare_func(const char *a, const char *b)
    {
      const uint64_t id1 = reinterpret_cast<const uint64_t>(a);
      const uint64_t id2 = reinterpret_cast<const uint64_t>(b);
      int32_t ret = 0;
      if (id1 < id2)
        ret = -1;
      else if (id1 > id2)
        ret = 1;
      return ret;
    }

  } // end namespace common
} // end namespace oceanbase

#endif
