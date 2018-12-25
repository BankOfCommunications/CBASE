/*
 * Copyright (C) 2012-2013 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * Description here
 *
 * Version: $Id$
 *
 * Authors:
 *   Fusheng Han <yanran.hfs@taobao.com>
 *     - Node of CMBtree
 */
#ifndef OCEANBASE_COMMON_CMBTREE_BTREE_NODE_H_
#define OCEANBASE_COMMON_CMBTREE_BTREE_NODE_H_

#include "btree_define.h"
#include "btree_tid.h"
#include "qlock.h"

namespace oceanbase
{
  namespace common
  {
    namespace cmbtree
    {
      /**
       * @brief Btree的节点
       *
       * Btree的数据直接存储在节点中，包括Key和Value。
       * 所有的节点存储的Key类型是相同的。
       * 叶子节点里存储的范型Value的值，非叶子节点存储的指向其他节点的指针。
       * 所以叶子节点和非叶子节点使用两种不同类型的结构
       * BtreeLeafNode和BtreeInterNode。
       *
       * BtreeInterNode和BtreeLeafNode同时继承自BtreeBaseNode
       *
       *          BtreeBaseNode
       *               |
       *       -------------------
       *       |                 |
       * BtreeInterNode     BtreeLeafNode
       *
       * BtreeInterNode里最多时存储CONST_NODE_OBJECT_COUNT - 1个Key，
       * 存储在keys_结构中，和CONST_NODE_OBJECT_COUNT个指针，
       * 存储在ptrs_结构中。以ptrs_[0]为根的子树中的所有Key均小于keys_[0]；
       * 以ptrs_[1]为根的子树中的所有Key均大于等于keys_[0]，小于keys_[1]；
       * 以此类推，以ptrs_[k]为根的子树中的所有Key均大于等于keys_[k-1]，
       * 小于keys_[k]。以最右则的ptrs_[size_-1]为根的子树的所有节点大于
       * keys_[size_-2]。
       *
       * BtreeLeafNode里存储的是<Key, Value>对，
       * 最多存储CONST_NODE_OBJECT_COUNT对。所有的Key存储在keys_结构中，
       * 所有的Value存储在values_结构中。
       */
      template<class K, class V>
      class BtreeBaseNode
      {
        public:
          typedef BtreeBaseNode<K, V>    TBaseNode;
        public:
          BtreeBaseNode                ();

          /**
           * @brief 搜索Pair的位置
           *
           * 对于非叶子节点，寻找key所处的下一层节点的位置；
           * 如果下层节点发生分裂，这个位置也是新的key所处的位置。
           * 对于叶子节点，找到对应的key的位置；
           * 当key不存在时，找到插入key时，应该插入的位置。
           *
           * 函数结果pos符合下面的条件：
           * <ul>
           *   <li><b>pos==-1</b>：key < keys_[0]</li>
           *   <li><b>pos==size_-1</b>：key >= keys_[size_-1]</li>
           *   <li><b>0<=pos<size_-1</b>：keys_[pos] <= key < keys_[pos+1]</li>
           * </ul>
           * 对于BtreeInterNode，ptrs_[pos+1]是包含key的子树的根节点；
           * 对于BtreeLeafNode，flag=0时，keys_[pos]==key；
           * flag!=0时，pos+1是key所应该插入的位置。
           */
          int32_t  find_pos            (const K & key, int16_t & pos,
                                        int64_t & flag) const;

          /**
           * @brief 得到当前个数
           */
          int16_t  get_size            () const;

          /**
           * @brief 得到在pos位置的key
           */
          int32_t  get_key             (int16_t pos, K & key) const;

          const K &get_key             (int16_t pos) const;

          /**
           * @brief 设置顺序号
           */
          void     set_sequence        (int64_t value);

          /**
           * @brief 得到顺序号
           */
          int64_t  get_sequence        () const;

          /**
           * @brief 是否叶子
           */
          int32_t  is_leaf             () const;

          /**
           * @brief 设置叶子
           */
          void     set_leaf            (int32_t value);

          /**
           * @brief 加共享锁
           *
           *   多个共享锁是相容的；
           *   如果已经有互斥锁存在，
           *   那么函数一直等到共享锁加成功后才返回
           */
          int32_t  shared_lock         ();

          int32_t  try_shared_lock     ();

          /**
           * @brief 释放共享锁
           */
          int32_t  shared_unlock       ();

          /**
           * @brief 加互斥锁
           *
           *   互斥锁是独占的
           *   如果已经有互斥锁存在，
           *   那么函数一直等到新的互斥锁加成功后才返回
           */
          int32_t  exclusive_lock      ();

          /**
           * @brief 释放互斥锁
           */
          int32_t  exclusive_unlock    ();

          /**
           * @brief 共享锁转互斥锁
           *
           *   在已经有共享锁的情况下，将共享锁转换成互斥锁。
           *   如果已经有互斥锁或者有另一个线程也在试图将共享锁转换成互斥锁，
           *   那么函数失败返回，返回码是<b>ERROR_CODE_FAIL</b>。
           */
          int32_t  shared2exclusive_lock();

          int32_t  try_shared2exclusive_lock();

          /**
           * @brief 互斥锁转共享锁
           *
           *   在已经有互斥锁的情况下，将互斥锁转换成共享锁。
           *   不会失败。
           */
          int32_t  exclusive2shared_lock();

          /**
           * @brief 设置next指向的位置
           */
          int32_t  set_next            (TBaseNode * next);

          /**
           * @brief 得到next值
           */
          BtreeBaseNode<K, V> * get_next();

#ifdef CMBTREE_DEBUG
          /**
           * 打印出来
           */
          int32_t  print(int32_t level) const;
#endif

        protected:
          /**
           * @brief 从当前节点拷贝一些key到new_node
           * 拷贝的范围是从当前节点的src_begin位置开始，
           * 拷贝number个key到new_node的dst_begin位置
           */
          int32_t  copy_keys_          (TBaseNode * new_node, int16_t src_begin,
                                        int16_t dst_begin, int16_t number);

          /**
           * @brief 复制除keys_之外的内容到new_node节点里
           */
          int32_t  clone_aux_          (TBaseNode * new_node);

          /**
           * @brief 复制节点并添加一个新key
           */
          int32_t  clone_and_add_key_  (TBaseNode * new_node, int16_t pos,
                                        const K & key);

          /**
           * @brief 复制节点并覆盖一个key
           */
          int32_t  clone_and_set_key_  (TBaseNode * new_node, int16_t pos,
                                        const K & key);

          /**
           * @brief 复制节点一部分
           */
          int32_t  clone_base_range_   (TBaseNode * new_node,
                                        int16_t range_start,
                                        int16_t range_end);

          /**
           * @brief 复制节点一部分并添加一个新key
           */
          int32_t  clone_range_and_add_key_(TBaseNode * new_node,
                                            int16_t range_start,
                                            int16_t range_end,
                                            int16_t pos,
                                            const K & key);

          /**
           * @brief 复制节点一部分并覆盖一个key
           */
          int32_t  clone_range_and_set_key_(TBaseNode * new_node,
                                            int16_t range_start,
                                            int16_t range_end,
                                            int16_t pos,
                                            const K & key);

        protected:
        public:
          int16_t size_;                            /// 当前pair个数
          uint8_t is_leaf_: 1;                      /// 是否叶节点
          int64_t sequence_;
          TBaseNode * next_;                        /// 指向其他节点的指针
          QLock   lock_;                            /// 节点的锁
          K       keys_[CONST_NODE_OBJECT_COUNT];   /// key的内容
      };

      template<class K, class V>
      class BtreeInterNode : public BtreeBaseNode<K, V>
      {
        public:
          typedef BtreeBaseNode<K, V>    TBaseNode;
          typedef BtreeInterNode<K, V>   TInterNode;
        public:
          ~BtreeInterNode              ();

          /**
           * @brief 执行put key操作
           *
           * 函数进行如下赋值操作：
  \verbatim
    keys_[pos] = key
    ptrs_[pos] = ptr1
    ptrs_[pos+1] = ptr2
    if (pos == size_) size_++
  \endverbatim
           */
          int32_t  put_key             (int16_t pos, const K & key,
                                        const TBaseNode * ptr1,
                                        const TBaseNode * ptr2);

          int32_t  get_ptr             (int16_t pos, TBaseNode * & ptr);

          TBaseNode * get_ptr          (int16_t pos) const;

          int32_t  set_ptr             (int16_t pos, const TBaseNode * ptr);

          int32_t  clone_and_add_tri   (TInterNode * new_inter, int16_t pos,
                                        const K & key, const TBaseNode * ptr1,
                                        const TBaseNode * ptr2);

          int32_t  get_middle_key_after_adding(int16_t pos, const K & added_key,
                                               K & out_key);

          int32_t  copy_left_part_and_add_tri(TInterNode * new_inter, int16_t pos,
                                              const K & key,
                                              const TBaseNode * ptr1,
                                              const TBaseNode * ptr2);

          int32_t  copy_right_part_and_add_tri(TInterNode * new_inter,
                                               int16_t pos,
                                               const K & key,
                                               const TBaseNode * ptr1,
                                               const TBaseNode * ptr2);

#ifdef CMBTREE_DEBUG
          /**
           * 打印出来
           */
          int32_t  print(int32_t level) const;
#endif

        protected:
          /**
           * @brief 从当前节点拷贝一些ptr到new_node
           * 拷贝的范围是从当前节点的src_begin位置开始，
           * 拷贝number个ptr到new_node的dst_begin位置
           */
          int32_t  copy_ptrs_          (TInterNode * new_node, int16_t src_begin,
                                        int16_t dst_begin, int16_t number);

          int32_t  clone_range_        (TInterNode * new_inter,
                                        int16_t range_start, int16_t range_end);

          /**
           * @brief 拷贝当前节点[range_start, range_end)范围的内容到new_inter
           */
          int32_t  clone_range_and_add_tri_(TInterNode * new_inter,
                                            int16_t range_start,
                                            int16_t range_end,
                                            int16_t pos,
                                            const K & key,
                                            const TBaseNode * ptr1,
                                            const TBaseNode * ptr2);

        protected:
          const TBaseNode * volatile ptrs_[CONST_NODE_OBJECT_COUNT + 1];/// 指向子节点的指针
      };

      template<class K, class V>
      class BtreeLeafNode : public BtreeBaseNode<K, V>
      {
        public:
          typedef BtreeBaseNode<K, V>    TBaseNode;
          typedef BtreeLeafNode<K, V>    TLeafNode;
        public:

          BtreeLeafNode                ();

          ~BtreeLeafNode               ();

          int32_t  get_value           (int16_t pos, V & value);

          /**
           * @brief 在pos位置增加一个key, value对
           */
          int32_t  add_pair            (int16_t pos, const K & key,
                                        const V & value);

          int32_t  clone_and_set_pair  (TLeafNode * new_leaf, int16_t pos,
                                        const K & key, const V & value);

          int32_t  clone_and_add_pair  (TLeafNode * new_leaf, int16_t pos,
                                        const K & key, const V & value);

          int32_t  copy_left_part_and_set_pair(TLeafNode * new_leaf, int16_t pos,
                                               const K & key, const V & value);

          int32_t  copy_left_part_and_add_pair(TLeafNode * new_leaf, int16_t pos,
                                               const K & key, const V & value);

          int32_t  copy_right_part_and_set_pair(TLeafNode * new_leaf, int16_t pos,
                                                const K & key, const V & value);

          int32_t  copy_right_part_and_add_pair(TLeafNode * new_leaf, int16_t pos,
                                                const K & key, const V & value);

#ifdef CMBTREE_DEBUG
          /**
           * 打印出来
           */
          int32_t  print(int32_t level) const;
#endif

        protected:
          /**
           * @brief 从当前节点拷贝一些ptr到new_node
           * 拷贝的范围是从当前节点的src_begin位置开始，
           * 拷贝number个ptr到new_node的dst_begin位置
           */
          int32_t  copy_values_        (TLeafNode * new_node, int16_t src_begin,
                                        int16_t dst_begin, int16_t number);

          int32_t  clone_range_        (TLeafNode * new_leaf,
                                        int16_t range_start, int16_t range_end);

          /**
           * @brief 拷贝当前节点[range_start, range_end)范围的内容到new_inter
           */
          int32_t  clone_range_and_add_pair_(TLeafNode * new_leaf,
                                             int16_t range_start,
                                             int16_t range_end,
                                             int16_t pos,
                                             const K & key,
                                             const V & value);

          /**
           * @brief 拷贝当前节点[range_start, range_end)范围的内容到new_inter
           */
          int32_t  clone_range_and_set_pair_(TLeafNode * new_leaf,
                                             int16_t range_start,
                                             int16_t range_end,
                                             int16_t pos,
                                             const K & key,
                                             const V & value);

          /**
           * @brief 获取左边部分需要拷贝的个数
           * 
           * @param pos 表示新插入的key的位置
           * @return 左边部分需要拷贝的个数
           */
          int16_t  get_left_part_length_   (int16_t pos);

          /**
           * @brief 获取右边部分需要拷贝的个数
           *
           * @param pos 表示新插入的key的位置
           * @return 右边部分需要拷贝的个数
           */
          int16_t  get_right_part_length_  (int16_t pos);

        protected:
          V       values_[CONST_NODE_OBJECT_COUNT]; /// value的内容
      };

      template<class K, class V>
      BtreeBaseNode<K, V>::BtreeBaseNode()
          : size_(0), is_leaf_(0), sequence_(0), next_(0)
      {
      }

      template<class K, class V>
      int32_t BtreeBaseNode<K, V>::find_pos(const K & key,
          int16_t & pos, int64_t & flag) const
      {
        int32_t ret = ERROR_CODE_OK;
        CMBTREE_CHECK_FALSE(size_ <= 0 || size_ > CONST_NODE_OBJECT_COUNT,
                                "size: %d CONST_NODE_OBJECT_COUNT: %d",
                                size_, CONST_NODE_OBJECT_COUNT);

        flag = -1;
        if (UNLIKELY(size_ < 0 || size_ > CONST_NODE_OBJECT_COUNT))
        {
           ret = ERROR_CODE_NOT_EXPECTED;
           pos = CONST_NODE_OBJECT_COUNT;
        }

        if (UNLIKELY(0 == size_))
        {
          ret = ERROR_CODE_NOT_EXPECTED;
          pos = 0;
        }

        // 有数据
        int16_t start = 0;
        int16_t mid = 1;
        int16_t end = static_cast<int16_t>(size_ - 1);

        while (start != end)
        {
          mid = static_cast<int16_t>((start + end) >> 1);
          flag = keys_[mid] - key;
          //if (flag > 0)
          //{
          //  end = mid;
          //}
          end = flag > 0 ? mid : end;
          //else if (flag < 0)
          //{
          //  start = mid + 1;
          //}
          start = flag < 0 ? static_cast<int16_t>(mid + 1) : start;
          //else
          //{
          //  start = mid;
          //  break;
          //}
          start = flag == 0 ? mid : start;
          end   = flag == 0 ? start : end;
        }
        if (LIKELY(flag != 0))
        {
          if (mid != start)
          {
            flag = keys_[start] - key;
          }
          if (flag > 0)
          {
            start--;
            flag = -1;
          }
        }

        pos = start;
        return ret;
      }

      template<class K, class V>
      int16_t BtreeBaseNode<K, V>::get_size() const
      {
        return size_;
      }

      template<class K, class V>
      int32_t BtreeBaseNode<K, V>::get_key(int16_t pos, K & key) const
      {
        int32_t ret = ERROR_CODE_OK;
        if (UNLIKELY(pos < 0 || pos >= this->size_))
        {
          CMBTREE_LOG(ERROR, "get_key error, size_ = %d, pos = %d",
              this->size_, pos);
          ret = ERROR_CODE_NOT_EXPECTED;
        }
        else
        {
          key = this->keys_[pos];
        }
        return ret;
      }

      template<class K, class V>
      void BtreeBaseNode<K, V>::set_sequence(int64_t value)
      {
        sequence_ = value;
      }

      template<class K, class V>
      int64_t BtreeBaseNode<K, V>::get_sequence() const
      {
        return sequence_;
      }

      template<class K, class V>
      int32_t BtreeBaseNode<K, V>::is_leaf() const
      {
        return is_leaf_;
      }

      template<class K, class V>
      void BtreeBaseNode<K, V>::set_leaf(int32_t value)
      {
        is_leaf_ = (value ? 1 : 0);
      }

      template<class K, class V>
      int32_t BtreeBaseNode<K, V>::shared_lock()
      {
        int32_t ret = ERROR_CODE_OK;
        int32_t err = lock_.shared_lock(BtreeTID::gettid());
        if (err != OB_SUCCESS)
        {
          ret = ERROR_CODE_FAIL;
        }
        return ret;
      }

      template<class K, class V>
      int32_t BtreeBaseNode<K, V>::try_shared_lock()
      {
        int32_t ret = ERROR_CODE_OK;
        int32_t err = lock_.try_shared_lock(BtreeTID::gettid());
        if (err != OB_SUCCESS)
        {
          ret = ERROR_CODE_FAIL;
        }
        return ret;
      }

      template<class K, class V>
      int32_t BtreeBaseNode<K, V>::shared_unlock()
      {
        int32_t ret = ERROR_CODE_OK;
        int32_t err = lock_.shared_unlock(BtreeTID::gettid());
        if (err != OB_SUCCESS)
        {
          ret = ERROR_CODE_FAIL;
        }
        return ret;
      }

      template<class K, class V>
      int32_t BtreeBaseNode<K, V>::exclusive_lock()
      {
        int32_t ret = ERROR_CODE_OK;
        int32_t err = lock_.exclusive_lock(BtreeTID::gettid());
        if (err != OB_SUCCESS)
        {
          ret = ERROR_CODE_FAIL;
        }
        return ret;
      }

      template<class K, class V>
      int32_t BtreeBaseNode<K, V>::exclusive_unlock()
      {
        int32_t ret = ERROR_CODE_OK;
        int32_t err = lock_.exclusive_unlock(BtreeTID::gettid());
        if (err != OB_SUCCESS)
        {
          ret = ERROR_CODE_FAIL;
        }
        return ret;
      }

      template<class K, class V>
      int32_t BtreeBaseNode<K, V>::shared2exclusive_lock()
      {
        int32_t ret = ERROR_CODE_OK;
        int32_t err = lock_.share2exclusive_lock(BtreeTID::gettid());
        if (err != OB_SUCCESS)
        {
          ret = ERROR_CODE_FAIL;
        }
        return ret;
      }

      template<class K, class V>
      int32_t BtreeBaseNode<K, V>::try_shared2exclusive_lock()
      {
        int32_t ret = ERROR_CODE_OK;
        int32_t err = lock_.try_shared2exclusive_lock(BtreeTID::gettid());
        if (err != OB_SUCCESS)
        {
          ret = ERROR_CODE_FAIL;
        }
        return ret;
      }

      template<class K, class V>
      int32_t BtreeBaseNode<K, V>::exclusive2shared_lock()
      {
        int32_t ret = ERROR_CODE_OK;
        int32_t err = lock_.exclusive2shared_lock(BtreeTID::gettid());
        if (err != OB_SUCCESS)
        {
          ret = ERROR_CODE_FAIL;
        }
        return ret;
      }

      template<class K, class V>
      int32_t BtreeBaseNode<K, V>::set_next(TBaseNode * next)
      {
        int32_t ret = ERROR_CODE_OK;
        //if (UNLIKELY(NULL == next))
        //{
        //  CMBTREE_LOG(ERROR, "set_next next is null");
        //  ret = ERROR_CODE_NOT_EXPECTED;
        //}
        //else
        {
          //CMBTREE_ATOMIC_STORE(&next_, next, __ATOMIC_ACQ_REL);
          next_ = next;
        }
        return ret;
      }

      template<class K, class V>
      BtreeBaseNode<K, V> * BtreeBaseNode<K, V>::get_next()
      {
        return next_;
      }

#ifdef CMBTREE_DEBUG
      template<class K, class V>
      int32_t BtreeBaseNode<K, V>::print(int32_t level) const
      {
        if (is_leaf())
        {
          return reinterpret_cast<const BtreeLeafNode<K, V> *>(this)
              ->print(level);
        }
        else
        {
          return reinterpret_cast<const BtreeInterNode<K, V> *>(this)
              ->print(level);
        }
      }
#endif // CMBTREE_DEBUG

      template<class K, class V>
      int32_t BtreeBaseNode<K, V>::copy_keys_(TBaseNode * new_node,
          int16_t src_begin, int16_t dst_begin, int16_t number)
      {
        int32_t ret = ERROR_CODE_OK;
        if (UNLIKELY(NULL == new_node
                     || src_begin + number > size_
                     || dst_begin + number > CONST_NODE_OBJECT_COUNT))
        {
          CMBTREE_LOG(ERROR, "copy_keys_ parameters are invalid, "
              "new_node = %p src_begin = %d dst_begin = %d number = %d "
              "size_ = %d CONST_NODE_OBJECT_COUNT = %d",
              new_node, src_begin, dst_begin, number, size_,
              CONST_NODE_OBJECT_COUNT);
          ret = ERROR_CODE_NOT_EXPECTED;
        }
        else
        {
          for (int16_t i = 0; i < number; i++)
          {
            new_node->keys_[dst_begin + i] = keys_[src_begin + i];
          }
        }
        return ret;
      }

      template<class K, class V>
      int32_t BtreeBaseNode<K, V>::clone_aux_(TBaseNode * new_node)
      {
        int32_t ret = ERROR_CODE_OK;
        if (UNLIKELY(NULL == new_node))
        {
          CMBTREE_LOG(ERROR, "clone_aux_ new_node is null");
          ret = ERROR_CODE_NOT_EXPECTED;
        }
        else
        {
          new_node->size_     = size_;
          new_node->is_leaf_  = is_leaf_;
          new_node->sequence_ = sequence_;
          new_node->next_     = next_;
        }
        return ret;
      }

      template<class K, class V>
      int32_t BtreeBaseNode<K, V>::clone_and_add_key_(
          TBaseNode * new_node, int16_t pos, const K & key)
      {
        return clone_range_and_add_key_(new_node, 0, size_, pos, key);
      }

      template<class K, class V>
      int32_t BtreeBaseNode<K, V>::clone_and_set_key_(
          TBaseNode * new_node, int16_t pos, const K & key)
      {
        return clone_range_and_set_key_(new_node, 0, size_, pos, key);
      }

      template<class K, class V>
      int32_t BtreeBaseNode<K, V>::clone_base_range_(TBaseNode * new_node,
          int16_t range_start, int16_t range_end)
      {
        int32_t ret = ERROR_CODE_OK;
        int32_t err = ERROR_CODE_OK;
        if (UNLIKELY(NULL == new_node
                     || range_start > range_end
                     || range_end > size_))
        {
          CMBTREE_LOG(ERROR, "clone_base_range_ parameters are invalid, "
              "new_node = %p range_start = %d range_end = %d size_ = %d",
              new_node, range_start, range_end, size_);
          ret = ERROR_CODE_NOT_EXPECTED;
        }

        if (LIKELY(ERROR_CODE_OK == ret))
        {
          err = this->clone_aux_(new_node);
          if (UNLIKELY(ERROR_CODE_OK != err))
          {
            CMBTREE_LOG(ERROR, "clone_aux_ error, err = %d new_node = %p",
                err, new_node);
            ret = ERROR_CODE_NOT_EXPECTED;
          }
          else
          {
            new_node->size_ = static_cast<int16_t>(range_end - range_start);
          }
        }

        // copy keys and set key
        if (LIKELY(ERROR_CODE_OK == ret))
        {
          err = this->copy_keys_(new_node, range_start, 0,
              static_cast<int16_t>(range_end - range_start));
          if (UNLIKELY(ERROR_CODE_OK != err))
          {
            CMBTREE_LOG(ERROR, "copy_keys_ error, err = %d new_node = %p "
                "range_start = %d number = %d", err, new_node, range_start,
                range_end - range_start);
            ret = ERROR_CODE_NOT_EXPECTED;
          }
        }

        return ret;
      }

      template<class K, class V>
      int32_t BtreeBaseNode<K, V>::clone_range_and_add_key_(
          TBaseNode * new_node, int16_t range_start, int16_t range_end,
          int16_t pos, const K & key)
      {
        int32_t ret = ERROR_CODE_OK;
        int32_t err = ERROR_CODE_OK;

        if (UNLIKELY(NULL == new_node
                     || range_start > range_end
                     || pos < 0
                     || pos > range_end - range_start
                     || range_end > size_))
        {
          CMBTREE_LOG(ERROR, "clone_range_and_add_key_ parameters are "
              "invalid, new_node = %p range_start = %d range_end = %d "
              "pos = %d size_ = %d", new_node, range_start, range_end, pos,
              size_);
          ret = ERROR_CODE_NOT_EXPECTED;
        }

        if (LIKELY(ERROR_CODE_OK == ret))
        {
          err = this->clone_aux_(new_node);
          if (UNLIKELY(ERROR_CODE_OK != err))
          {
            CMBTREE_LOG(ERROR, "clone_aux_ error, err = %d new_node = %p",
                err, new_node);
            ret = ERROR_CODE_NOT_EXPECTED;
          }
          else
          {
            new_node->size_ = static_cast<int16_t>(range_end - range_start + 1);
          }
        }

        // copy left part
        if (LIKELY(ERROR_CODE_OK == ret))
        {
          err = this->copy_keys_(new_node, range_start, 0, pos);
          if (UNLIKELY(ERROR_CODE_OK != err))
          {
            CMBTREE_LOG(ERROR, "copy_keys_ error, err = %d new_node = %p "
                "range_start = %d pos = %d", err, new_node, range_start, pos);
            ret = ERROR_CODE_NOT_EXPECTED;
          }
        }

        // add pair and copy right part
        if (LIKELY(ERROR_CODE_OK == err))
        {
          new_node->keys_[pos] = key;
          err = this->copy_keys_(new_node,
              static_cast<int16_t>(range_start + pos),
              static_cast<int16_t>(pos + 1),
              static_cast<int16_t>(new_node->size_ - pos - 1));
          if (UNLIKELY(ERROR_CODE_OK != err))
          {
            CMBTREE_LOG(ERROR, "copy_keys_ error, err = %d new_node = %p "
                "range_start = %d new_node->size_ = %d pos = %d", err, new_node,
                range_start, new_node->size_, pos);
            ret = ERROR_CODE_NOT_EXPECTED;
          }
        }

        return ret;
      }

      template<class K, class V>
      int32_t BtreeBaseNode<K, V>::clone_range_and_set_key_(
          TBaseNode * new_node, int16_t range_start, int16_t range_end,
          int16_t pos, const K & key)
      {
        int32_t ret = ERROR_CODE_OK;
        int32_t err = ERROR_CODE_OK;

        if (UNLIKELY(NULL == new_node
                     || range_start > range_end
                     || pos < 0
                     || pos >= range_end - range_start
                     || range_end > size_))
        {
          CMBTREE_LOG(ERROR, "clone_range_and_add_key_ parameters are "
              "invalid, new_node = %p range_start = %d range_end = %d "
              "pos = %d size_ = %d", new_node, range_start, range_end, pos,
              size_);
          ret = ERROR_CODE_NOT_EXPECTED;
        }

        // copy keys and set key
        if (LIKELY(ERROR_CODE_OK == ret))
        {
          err = this->clone_base_range_(new_node, range_start, 0,
              range_end - range_start);
          if (UNLIKELY(ERROR_CODE_OK != err))
          {
            CMBTREE_LOG(ERROR, "clone_range_ error, err = %d new_node = %p "
                "range_start = %d range_end = %d", err, new_node, range_start,
                range_end);
            ret = ERROR_CODE_NOT_EXPECTED;
          }
          else
          {
            new_node->keys_[pos] = key;
          }
        }

        return ret;
      }

      template<class K, class V>
      BtreeInterNode<K, V>::~BtreeInterNode()
      {
        CMBTREE_CHECK_TRUE(
            this->size_ > 0 && this->size_ <= CONST_NODE_OBJECT_COUNT
            && this->is_leaf_ == 0
            && this->lock_.n_ref_ == 0 && this->lock_.uid_ == 0,
            "size_ = %d is_leaf = %d n_ref_ = %d uid_ = %d",
            this->size_, this->is_leaf_,
            this->lock_.n_ref_, this->lock_.uid_);
      }

      template<class K, class V>
      int32_t BtreeInterNode<K, V>::put_key(int16_t pos, const K & key,
          const TBaseNode * ptr1, const TBaseNode * ptr2)
      {
        int32_t ret = ERROR_CODE_OK;
        if (UNLIKELY(pos < 0 || pos > this->size_))
        {
          CMBTREE_LOG(ERROR, "put_key error, size_ = %d, pos = %d",
              this->size_, pos);
          ret = ERROR_CODE_NOT_EXPECTED;
        }
        else
        {
          this->keys_[pos] = key;
          ptrs_[pos] = ptr1;
          ptrs_[pos + 1] = ptr2;
          if (pos == this->size_)
          {
            this->size_++;
          }
        }
        return ret;
      }

      template<class K, class V>
      int32_t BtreeInterNode<K, V>::get_ptr(int16_t pos, TBaseNode * & ptr)
      {
        int32_t ret = ERROR_CODE_OK;
        if (UNLIKELY(pos < 0 || pos > this->size_))
        {
          CMBTREE_LOG(ERROR, "get_ptr error, size_ = %d, pos = %d",
              this->size_, pos);
          ret = ERROR_CODE_NOT_EXPECTED;
        }
        else
        {
          ptr = const_cast<TBaseNode *>(ptrs_[pos]);
        }
        return ret;
      }

      template<class K, class V>
      int32_t BtreeInterNode<K, V>::set_ptr(int16_t pos,
          const TBaseNode * ptr)
      {
        int32_t ret = ERROR_CODE_OK;
        if (UNLIKELY(pos < 0 || pos > this->size_))
        {
          CMBTREE_LOG(ERROR, "get_ptr error, size_ = %d, pos = %d",
              this->size_, pos);
          ret = ERROR_CODE_NOT_EXPECTED;
        }
        else
        {
          //CMBTREE_ATOMIC_STORE(ptrs_ + pos, ptr, __ATOMIC_ACQ_REL);
          ptrs_[pos] = ptr;
        }
        return ret;
      }

      template<class K, class V>
      int32_t BtreeInterNode<K, V>::clone_and_add_tri(
          TInterNode * new_inter, int16_t pos, const K & key,
          const TBaseNode * ptr1, const TBaseNode * ptr2)
      {
        return clone_range_and_add_tri_(new_inter, 0, this->size_, pos,
            key, ptr1, ptr2);
      }

      template<class K, class V>
      int32_t BtreeInterNode<K, V>::get_middle_key_after_adding(
          int16_t pos, const K & added_key, K & out_key)
      {
        int32_t ret = ERROR_CODE_OK;
        int64_t left_number = this->size_ / 2;
        if (pos < left_number)
        {
          out_key = this->keys_[left_number - 1];
        }
        else if (pos == left_number)
        {
          out_key = added_key;
        }
        else
        {
          out_key = this->keys_[left_number];
        }
        return ret;
      }

      template<class K, class V>
      int32_t BtreeInterNode<K, V>::copy_left_part_and_add_tri(
          TInterNode * new_inter, int16_t pos,
          const K & key, const TBaseNode * ptr1, const TBaseNode * ptr2)
      {
        int32_t ret = ERROR_CODE_OK;
        int32_t err = ERROR_CODE_OK;
        int16_t left_number = this->size_ / 2;
        if (pos < left_number)
        {
          err = clone_range_and_add_tri_(new_inter, 0,
              static_cast<int16_t>(left_number - 1), pos, key, ptr1, ptr2);
          if (UNLIKELY(ERROR_CODE_OK != err))
          {
            CMBTREE_LOG(ERROR, "clone_range_and_add_tri_ error, err = %d "
                "new_inter = %p left_number = %d pos = %d ptr1 = %p "
                "ptr2 = %p", err, new_inter, left_number, pos, ptr1, ptr2);
            ret = ERROR_CODE_NOT_EXPECTED;
          }
        }
        else
        {
          err = clone_range_(new_inter, 0, left_number);
          if (UNLIKELY(ERROR_CODE_OK != err))
          {
            CMBTREE_LOG(ERROR, "clone_range_ error, err = %d new_inter = %p "
                "left_number = %d", err, new_inter, left_number);
            ret = ERROR_CODE_NOT_EXPECTED;
          }
          else
          {
            if (pos == left_number)
            {
              new_inter->ptrs_[pos] = ptr1;
            }
          }
        }
        return ret;
      }

      template<class K, class V>
      int32_t BtreeInterNode<K, V>::copy_right_part_and_add_tri(
          TInterNode * new_inter, int16_t pos,
          const K & key, const TBaseNode * ptr1, const TBaseNode * ptr2)
      {
        int32_t ret = ERROR_CODE_OK;
        int32_t err = ERROR_CODE_OK;
        int16_t left_number = this->size_ / 2;
        if (pos <= left_number)
        {
          err = clone_range_(new_inter, left_number, this->size_);
          if (UNLIKELY(ERROR_CODE_OK != err))
          {
            CMBTREE_LOG(ERROR, "clone_range_ error, err = %d new_inter = %p "
                "left_number = %d size_ = %d", err, new_inter, left_number,
                this->size_);
            ret = ERROR_CODE_NOT_EXPECTED;
          }
          else
          {
            if (pos == left_number)
            {
              new_inter->ptrs_[0] = ptr2;
            }
          }
        }
        else
        {
          err = clone_range_and_add_tri_(new_inter,
              static_cast<int16_t>(left_number + 1), this->size_,
              static_cast<int16_t>(pos - left_number - 1), key, ptr1, ptr2);
          if (UNLIKELY(ERROR_CODE_OK != err))
          {
            CMBTREE_LOG(ERROR, "clone_range_and_add_tri_ error, err = %d "
                "new_inter = %p left_number = %d size_ = %d pos = %d ptr1 = %p"
                "ptr2 = %p",
                err, new_inter, left_number, this->size_, pos, ptr1, ptr2);
            ret = ERROR_CODE_NOT_EXPECTED;
          }
        }
        return ret;
      }

#ifdef CMBTREE_DEBUG
      template<class K, class V>
      int32_t BtreeInterNode<K, V>::print(int32_t level) const
      {
        int32_t ret = ERROR_CODE_OK;
        for (int32_t i = 0; i < level; i++)
        {
          fprintf(stderr, "    ");
        }
        fprintf(stderr, "%d ----size: %d ----   ----address: %p ----",
            level, this->size_, this);
        fprintf(stderr, "  [ ");
        for(int32_t i = 0; i < this->size_; i++)
        {
          fprintf(stderr, "%s ", this->keys_[i].to_cstring());
        }
        fprintf(stderr, "]");
        fprintf(stderr, "  [ ");
        for (int32_t i = 0; i < this->size_ + 1; i++)
        {
          fprintf(stderr, "%p ", ptrs_[i]);
        }
        fprintf(stderr, "]\n");
        for(int32_t i = 0; i < this->size_ + 1; i++)
        {
          ((BtreeBaseNode<K, V>*)ptrs_[i])->print(level + 1);
        }
        return ret;
      }
#endif // CMBTREE_DEBUG

      template<class K, class V>
      int32_t BtreeInterNode<K, V>::copy_ptrs_(TInterNode * new_node,
          int16_t src_begin, int16_t dst_begin, int16_t number)
      {
        int32_t ret = ERROR_CODE_OK;
        if (UNLIKELY(NULL == new_node
                     || src_begin + number > this->size_ + 1
                     || dst_begin + number > CONST_NODE_OBJECT_COUNT + 1))
        {
          CMBTREE_LOG(ERROR, "copy_ptrs_ parameters are invalid, "
              "new_node = %p src_begin = %d dst_begin = %d number = %d "
              "size_ = %d CONST_NODE_OBJECT_COUNT = %d",
              new_node, src_begin, dst_begin, number, this->size_,
              CONST_NODE_OBJECT_COUNT);
          ret = ERROR_CODE_NOT_EXPECTED;
        }
        else
        {
          for (int16_t i = 0; i < number; i++)
          {
            new_node->ptrs_[dst_begin + i] = ptrs_[src_begin + i];
          }
        }
        return ret;
      }

      template<class K, class V>
      int32_t BtreeInterNode<K, V>::clone_range_(
          TInterNode * new_inter, int16_t range_start, int16_t range_end)
      {
        int32_t ret = ERROR_CODE_OK;
        int32_t err = ERROR_CODE_OK;

        if (UNLIKELY(NULL == new_inter
                     || range_start > range_end))
        {
          CMBTREE_LOG(ERROR, "clone_range_ parameters are invalid, "
              "new_inter = %p range_start = %d range_end = %d ",
              new_inter, range_start, range_end);
          ret = ERROR_CODE_NOT_EXPECTED;
        }

        if (LIKELY(ERROR_CODE_OK == ret))
        {
          err = this->clone_base_range_(new_inter, range_start, range_end);
          if (UNLIKELY(ERROR_CODE_OK != err))
          {
            CMBTREE_LOG(ERROR, "clone_base_range_ error, err = %d "
                "new_inter = %p range_start = %d range_end = %d",
                err, new_inter, range_start, range_end);
            ret = ERROR_CODE_NOT_EXPECTED;
          }
          else
          {
            err = copy_ptrs_(new_inter, range_start, 0,
                static_cast<int16_t>(range_end - range_start + 1));
            if (UNLIKELY(ERROR_CODE_OK != err))
            {
              CMBTREE_LOG(ERROR, "copy_ptrs_ error, err = %d new_inter = %p "
                  "range_start = %d range_end = %d", err, new_inter, range_start,
                  range_end);
              ret = ERROR_CODE_NOT_EXPECTED;
            }
          }
        }

        return ret;
      }

      template<class K, class V>
      int32_t BtreeInterNode<K, V>::clone_range_and_add_tri_(
          TInterNode * new_inter, int16_t range_start, int16_t range_end,
          int16_t pos, const K & key, const TBaseNode * ptr1,
          const TBaseNode * ptr2)
      {
        int32_t ret = ERROR_CODE_OK;
        int32_t err = ERROR_CODE_OK;

        if (UNLIKELY(NULL == new_inter
                     || range_start > range_end
                     || pos < 0
                     || pos > range_end - range_start
                     || range_end > this->size_
                     || NULL == ptr1
                     || NULL == ptr2))
        {
          CMBTREE_LOG(ERROR, "clone_range_and_add_tri_ parameters are "
              "invalid, new_inter = %p range_start = %d range_end = %d "
              "pos = %d ptr1 = %p ptr2 = %p size_ = %d", new_inter, range_start,
              range_end, pos, ptr1, ptr2, this->size_);
          ret = ERROR_CODE_NOT_EXPECTED;
        }

        if (LIKELY(ERROR_CODE_OK == ret))
        {
          err = this->clone_range_and_add_key_(new_inter,
              range_start, range_end, pos, key);
          if (UNLIKELY(ERROR_CODE_OK != err))
          {
            CMBTREE_LOG(ERROR, "clone_range_and_add_key_ error, err = %d "
                "new_inter = %p range_start = %d range_end = %d pos = %d",
                err, new_inter, range_start, range_end, pos);
            ret = ERROR_CODE_NOT_EXPECTED;
          }
        }

        // copy ptrs
        if (LIKELY(ERROR_CODE_OK == ret))
        {
          err = this->copy_ptrs_(new_inter, range_start, 0, pos);
          if (UNLIKELY(ERROR_CODE_OK != err))
          {
            CMBTREE_LOG(ERROR, "copy_ptrs_ error, err = %d new_inter = %p "
                "range_start = %d pos = %d", err, new_inter, range_start, pos);
            ret = ERROR_CODE_NOT_EXPECTED;
          }
          else
          {
            err = this->copy_ptrs_(new_inter,
                static_cast<int16_t>(range_start + pos + 1),
                static_cast<int16_t>(pos + 2),
                static_cast<int16_t>(new_inter->size_ - pos - 1));
            if (UNLIKELY(ERROR_CODE_OK != err))
            {
              CMBTREE_LOG(ERROR, "copy_ptrs_ error, err = %d new_inter = %p "
                  "range_start = %d new_inter->size_ = %d pos = %d", err,
                  new_inter, range_start, new_inter->size_, pos);
              ret = ERROR_CODE_NOT_EXPECTED;
            }
            else
            {
              new_inter->ptrs_[pos] = ptr1;
              new_inter->ptrs_[pos + 1] = ptr2;
            }
          }
        }

        return ret;
      }

      template<class K, class V>
      BtreeLeafNode<K, V>::BtreeLeafNode()
      {
        this->is_leaf_ = 1;
      }

      template<class K, class V>
      BtreeLeafNode<K, V>::~BtreeLeafNode()
      {
        CMBTREE_CHECK_TRUE(
            this->size_ > 0 && this->size_ <= CONST_NODE_OBJECT_COUNT
            && this->is_leaf_ == 1
            && this->lock_.n_ref_ == 0 && this->lock_.uid_ == 0,
            "size_ = %d is_leaf = %d n_ref_ = %d uid_ = %d",
            this->size_, this->is_leaf_,
            this->lock_.n_ref_, this->lock_.uid_);
      }

      template<class K, class V>
      int32_t BtreeLeafNode<K, V>::get_value(int16_t pos, V & value)
      {
        int32_t ret = ERROR_CODE_OK;
        if (UNLIKELY(pos < 0 || pos > this->size_))
        {
          CMBTREE_LOG(ERROR, "get_value error, size_ = %d, pos = %d",
              this->size_, pos);
          ret = ERROR_CODE_NOT_EXPECTED;
        }
        else
        {
          value = values_[pos];
        }
        return ret;
      }

      template<class K, class V>
      int32_t BtreeLeafNode<K, V>::add_pair(int16_t pos, const K & key,
                                                   const V & value)
      {
        int32_t ret = ERROR_CODE_OK;

        for (int32_t i = this->size_; i > pos; i--)
        {
          this->keys_[i] = this->keys_[i - 1];
          values_[i] = values_[i - 1];
        }
        this->keys_[pos] = key;
        values_[pos] = value;
        this->size_ ++;

        return ret;
      }


      template<class K, class V>
      int32_t BtreeLeafNode<K, V>::clone_and_set_pair(
          TLeafNode * new_leaf, int16_t pos, const K & key, const V & value)
      {
        int32_t ret = ERROR_CODE_OK;
        int32_t err = ERROR_CODE_OK;

        if (UNLIKELY(NULL == new_leaf || pos < 0 || pos >= this->size_))
        {
          CMBTREE_LOG(ERROR, "clone_and_set_pair_ parameters are invalid, "
              "new_leaf = %p pos = %d", new_leaf, pos);
          ret = ERROR_CODE_NOT_EXPECTED;
        }
        else
        {
          err = clone_range_and_set_pair_(new_leaf, 0, this->size_,
              pos, key, value);
          if (UNLIKELY(ERROR_CODE_OK != err))
          {
            CMBTREE_LOG(ERROR, "clone_range_and_set_pair_ error, err = %d "
                "new_leaf = %p size_ = %d pos = %d", err, new_leaf, this->size_,
                pos);
            ret = ERROR_CODE_NOT_EXPECTED;
          }
        }

        return ret;
      }

      template<class K, class V>
      int32_t BtreeLeafNode<K, V>::clone_and_add_pair(
          TLeafNode * new_leaf, int16_t pos, const K & key, const V & value)
      {
        int32_t ret = ERROR_CODE_OK;
        int32_t err = ERROR_CODE_OK;

        if (UNLIKELY(NULL == new_leaf || pos < 0 || pos > this->size_))
        {
          CMBTREE_LOG(ERROR, "clone_and_add_pair_ parameters are invalid, "
              "new_leaf = %p pos = %d", new_leaf, pos);
          ret = ERROR_CODE_NOT_EXPECTED;
        }
        else
        {
          err = clone_range_and_add_pair_(new_leaf, 0, this->size_,
              pos, key, value);
          if (UNLIKELY(ERROR_CODE_OK != err))
          {
            CMBTREE_LOG(ERROR, "clone_range_and_add_pair_ error, err = %d "
                "new_leaf = %p size_ = %d pos = %d", err, new_leaf, this->size_,
                pos);
            ret = ERROR_CODE_NOT_EXPECTED;
          }
        }

        return ret;
      }

      template<class K, class V>
      int32_t BtreeLeafNode<K, V>::copy_left_part_and_set_pair(
          TLeafNode * new_leaf, int16_t pos, const K & key, const V & value)
      {
        int32_t ret = ERROR_CODE_OK;
        int32_t err = ERROR_CODE_OK;
        if (UNLIKELY(NULL == new_leaf || pos < 0 || pos >= this->size_))
        {
          CMBTREE_LOG(ERROR, "copy_left_part_and_add_pair parameters are "
              "invalid, new_leaf = %p pos = %d", new_leaf, pos);
          ret = ERROR_CODE_NOT_EXPECTED;
        }
        else
        {
          int16_t copied_number = this->size_ / 2;
          if (pos < copied_number)
          {
            err = clone_range_and_set_pair_(new_leaf, 0, copied_number,
                pos, key, value);
            if (UNLIKELY(ERROR_CODE_OK != err))
            {
              CMBTREE_LOG(ERROR, "clone_range_and_set_pair_ error, err = %d "
                  "new_leaf = %p copied_number = %d pos = %d",
                  err, new_leaf, copied_number, pos);
              ret = ERROR_CODE_NOT_EXPECTED;
            }
          }
          else
          {
            err = clone_range_(new_leaf, 0, copied_number);
            if (UNLIKELY(ERROR_CODE_OK != err))
            {
              CMBTREE_LOG(ERROR, "clone_range_ error, err = %d new_leaf = %p "
                  "copied_number = %d", err, new_leaf, copied_number);
              ret = ERROR_CODE_NOT_EXPECTED;
            }
          }
        }
        return ret;
      }

      template<class K, class V>
      int32_t BtreeLeafNode<K, V>::copy_left_part_and_add_pair(
          TLeafNode * new_leaf, int16_t pos, const K & key, const V & value)
      {
        int32_t ret = ERROR_CODE_OK;
        int32_t err = ERROR_CODE_OK;
        if (UNLIKELY(NULL == new_leaf || pos < 0 || pos > this->size_))
        {
          CMBTREE_LOG(ERROR, "copy_left_part_and_add_pair parameters are "
              "invalid, new_leaf = %p pos = %d", new_leaf, pos);
          ret = ERROR_CODE_NOT_EXPECTED;
        }
        else
        {
          int16_t copied_number = this->get_left_part_length_(pos);
          if (pos < copied_number)
          {
            err = clone_range_and_add_pair_(new_leaf, 0, copied_number,
                pos, key, value);
            if (UNLIKELY(ERROR_CODE_OK != err))
            {
              CMBTREE_LOG(ERROR, "clone_range_and_add_pair_ error, err = %d "
                  "new_leaf = %p copied_number = %d pos = %d",
                  err, new_leaf, copied_number, pos);
              ret = ERROR_CODE_NOT_EXPECTED;
            }
          }
          else
          {
            err = clone_range_(new_leaf, 0, copied_number);
            if (UNLIKELY(ERROR_CODE_OK != err))
            {
              CMBTREE_LOG(ERROR, "clone_range_ error, err = %d new_leaf = %p "
                  "copied_number = %d", err, new_leaf, copied_number);
              ret = ERROR_CODE_NOT_EXPECTED;
            }
          }
        }
        return ret;
      }

      template<class K, class V>
      int32_t BtreeLeafNode<K, V>::copy_right_part_and_set_pair(
          TLeafNode * new_leaf, int16_t pos, const K & key, const V & value)
      {
        int32_t ret = ERROR_CODE_OK;
        int32_t err = ERROR_CODE_OK;
        if (UNLIKELY(NULL == new_leaf || pos < 0 || pos >= this->size_))
        {
          CMBTREE_LOG(ERROR, "copy_right_part_and_add_pair parameters are "
              "invalid, new_leaf = %p pos = %d", new_leaf, pos);
          ret = ERROR_CODE_NOT_EXPECTED;
        }
        else
        {
          int16_t left_number = this->size_ / 2;
          //int16_t copied_number = this->size_ - left_number;
          if (pos < left_number)
          {
            err = clone_range_(new_leaf, left_number, this->size_);
            if (UNLIKELY(ERROR_CODE_OK != err))
            {
              CMBTREE_LOG(ERROR, "clone_range_ error, err = %d new_leaf = %p "
                  "left_number = %d size_ = %d", err, new_leaf, left_number,
                  this->size_);
              ret = ERROR_CODE_NOT_EXPECTED;
            }
          }
          else
          {
            err = clone_range_and_set_pair_(new_leaf, left_number, this->size_,
                static_cast<int16_t>(pos - left_number), key, value);
            if (UNLIKELY(ERROR_CODE_OK != err))
            {
              CMBTREE_LOG(ERROR, "clone_range_and_set_pair_ error, err = %d "
                  "new_leaf = %p left_number = %d size_ = %d pos = %d",
                  err, new_leaf, left_number, this->size_, pos);
              ret = ERROR_CODE_NOT_EXPECTED;
            }
          }
        }
        return ret;
      }

      template<class K, class V>
      int32_t BtreeLeafNode<K, V>::copy_right_part_and_add_pair(
          TLeafNode * new_leaf, int16_t pos, const K & key, const V & value)
      {
        int32_t ret = ERROR_CODE_OK;
        int32_t err = ERROR_CODE_OK;
        if (UNLIKELY(NULL == new_leaf || pos < 0 || pos > this->size_))
        {
          CMBTREE_LOG(ERROR, "copy_right_part_and_add_pair parameters are "
              "invalid, new_leaf = %p pos = %d", new_leaf, pos);
          ret = ERROR_CODE_NOT_EXPECTED;
        }
        else
        {
          int16_t copied_number = this->get_right_part_length_(pos);
          int16_t left_number = static_cast<int16_t>(this->size_ - copied_number);
          if (pos < left_number)
          {
            err = clone_range_(new_leaf, left_number, this->size_);
            if (UNLIKELY(ERROR_CODE_OK != err))
            {
              CMBTREE_LOG(ERROR, "clone_range_ error, err = %d new_leaf = %p "
                  "left_number = %d size_ = %d", err, new_leaf, left_number,
                  this->size_);
              ret = ERROR_CODE_NOT_EXPECTED;
            }
          }
          else
          {
            err = clone_range_and_add_pair_(new_leaf, left_number, this->size_,
                static_cast<int16_t>(pos - left_number), key, value);
            if (UNLIKELY(ERROR_CODE_OK != err))
            {
              CMBTREE_LOG(ERROR, "clone_range_and_add_pair_ error, err = %d "
                  "new_leaf = %p left_number = %d size_ = %d pos = %d",
                  err, new_leaf, left_number, this->size_, pos);
              ret = ERROR_CODE_NOT_EXPECTED;
            }
          }
        }
        return ret;
      }

#ifdef CMBTREE_DEBUG
      template<class K, class V>
      int32_t BtreeLeafNode<K, V>::print(int32_t level) const
      {
        int32_t ret = ERROR_CODE_OK;
        for (int32_t i = 0; i < level; i++)
        {
          fprintf(stderr, "    ");
        }
        fprintf(stderr, "%d ----size: %d ----   ----address: %p ----",
            level, this->size_, this);
        fprintf(stderr, "  [ ");
        for(int32_t i = 0; i < this->size_; i++)
        {
          fprintf(stderr, "%s ", this->keys_[i].to_cstring());
        }
        fprintf(stderr, "]");
        fprintf(stderr, "  [ ");
        //for (int32_t i = 0; i < this->size_; i++)
        //{
        //  fprintf(stderr, "%s ", values_[i].to_cstring());
        //}
        fprintf(stderr, "]\n");
        return ret;
      }
#endif // CMBTREE_DEBUG

      template<class K, class V>
      int32_t BtreeLeafNode<K, V>::copy_values_(TLeafNode * new_node,
          int16_t src_begin, int16_t dst_begin, int16_t number)
      {
        int32_t ret = ERROR_CODE_OK;
        if (UNLIKELY(NULL == new_node
                     || src_begin + number > this->size_
                     || dst_begin + number > CONST_NODE_OBJECT_COUNT))
        {
          CMBTREE_LOG(ERROR, "copy_values_ parameters are invalid, "
              "new_node = %p src_begin = %d dst_begin = %d number = %d "
              "size_ = %d CONST_NODE_OBJECT_COUNT = %d",
              new_node, src_begin, dst_begin, number, this->size_,
              CONST_NODE_OBJECT_COUNT);
          ret = ERROR_CODE_NOT_EXPECTED;
        }
        else
        {
          for (int16_t i = 0; i < number; i++)
          {
            new_node->values_[dst_begin + i] = values_[src_begin + i];
          }
        }
        return ret;
      }

      template<class K, class V>
      int32_t BtreeLeafNode<K, V>::clone_range_(TLeafNode * new_leaf,
          int16_t range_start, int16_t range_end)
      {
        int32_t ret = ERROR_CODE_OK;
        int32_t err = ERROR_CODE_OK;

        if (UNLIKELY(NULL == new_leaf
                     || range_start > range_end))
        {
          CMBTREE_LOG(ERROR, "clone_range_ parameters are invalid, "
              "new_leaf = %p range_start = %d range_end = %d ",
              new_leaf, range_start, range_end);
          ret = ERROR_CODE_NOT_EXPECTED;
        }

        if (LIKELY(ERROR_CODE_OK == ret))
        {
          err = this->clone_base_range_(new_leaf, range_start, range_end);
          if (UNLIKELY(ERROR_CODE_OK != err))
          {
            CMBTREE_LOG(ERROR, "clone_base_range_ error, err = %d "
                "new_leaf = %p range_start = %d range_end = %d ",
                err, new_leaf, range_start, range_end);
            ret = ERROR_CODE_NOT_EXPECTED;
          }
          else
          {
            err = copy_values_(new_leaf, range_start, 0,
					static_cast<int16_t>(range_end - range_start));
            if (UNLIKELY(ERROR_CODE_OK != err))
            {
              CMBTREE_LOG(ERROR, "copy_values_ error, err = %d new_leaf = %p "
                  "range_start = %d range_end = %d", err, new_leaf, range_start,
                  range_end);
              ret = ERROR_CODE_NOT_EXPECTED;
            }
          }
        }

        return ret;
      }

      template<class K, class V>
      int32_t BtreeLeafNode<K, V>::clone_range_and_add_pair_(
          TLeafNode * new_leaf, int16_t range_start, int16_t range_end,
          int16_t pos, const K & key, const V & value)
      {
        int32_t ret = ERROR_CODE_OK;
        int32_t err = ERROR_CODE_OK;

        if (UNLIKELY(NULL == new_leaf
                     || range_start > range_end
                     || pos < 0
                     || pos > range_end - range_start
                     || range_end > this->size_))
        {
          CMBTREE_LOG(ERROR, "clone_range_and_add_pair_ parameters are "
              "invalid, new_leaf = %p range_start = %d range_end = %d "
              "pos = %d size_ = %d", new_leaf, range_start, range_end,
              pos, this->size_);
          ret = ERROR_CODE_NOT_EXPECTED;
        }

        if (LIKELY(ERROR_CODE_OK == ret))
        {
          err = this->clone_range_and_add_key_(new_leaf,
              range_start, range_end, pos, key);
          if (UNLIKELY(ERROR_CODE_OK != err))
          {
            CMBTREE_LOG(ERROR, "clone_range_and_add_key_ error, err = %d "
                "new_leaf = %p range_start = %d range_end = %d pos = %d",
                err, new_leaf, range_start, range_end, pos);
            ret = ERROR_CODE_NOT_EXPECTED;
          }
        }

        // copy values
        if (LIKELY(ERROR_CODE_OK == ret))
        {
          err = this->copy_values_(new_leaf, range_start, 0, pos);
          if (UNLIKELY(ERROR_CODE_OK != err))
          {
            CMBTREE_LOG(ERROR, "copy_values_ error, err = %d new_leaf = %p "
                "range_start = %d pos = %d", err, new_leaf, range_start, pos);
            ret = ERROR_CODE_NOT_EXPECTED;
          }
          else
          {
            new_leaf->values_[pos] = value;
            err = this->copy_values_(new_leaf,
                static_cast<int16_t>(range_start + pos),
                static_cast<int16_t>(pos + 1),
                static_cast<int16_t>(new_leaf->size_ - pos - 1));
            if (UNLIKELY(ERROR_CODE_OK != err))
            {
              CMBTREE_LOG(ERROR, "copy_values_ error, err = %d new_leaf = %p "
                  "range_start = %d new_leaf->size_ = %d pos = %d", err, new_leaf,
                  range_start, new_leaf->size_, pos);
              ret = ERROR_CODE_NOT_EXPECTED;
            }
          }
        }

        return ret;
      }

      template<class K, class V>
      int32_t BtreeLeafNode<K, V>::clone_range_and_set_pair_(
          TLeafNode * new_leaf, int16_t range_start, int16_t range_end,
          int16_t pos, const K & key, const V & value)
      {
        int32_t ret = ERROR_CODE_OK;
        int32_t err = ERROR_CODE_OK;

        if (UNLIKELY(NULL == new_leaf
                     || range_start > range_end
                     || pos < 0
                     || pos >= range_end - range_start
                     || range_end > this->size_))
        {
          CMBTREE_LOG(ERROR, "clone_range_and_add_pair_ parameters are "
              "invalid, new_leaf = %p range_start = %d range_end = %d "
              "pos = %d size_ = %d", new_leaf, range_start, range_end,
              pos, this->size_);
          ret = ERROR_CODE_NOT_EXPECTED;
        }

        if (LIKELY(ERROR_CODE_OK == ret))
        {
          err = this->clone_range_(new_leaf, range_start, range_end);
          if (UNLIKELY(ERROR_CODE_OK != err))
          {
            CMBTREE_LOG(ERROR, "clone_range_ error, err = %d "
                "new_leaf = %p range_start = %d range_end = %d",
                err, new_leaf, range_start, range_end);
            ret = ERROR_CODE_NOT_EXPECTED;
          }
          else
          {
            new_leaf->keys_[pos] = key;
            new_leaf->values_[pos] = value;
          }
        }

        return ret;
      }

      template<class K, class V>
      int16_t BtreeLeafNode<K, V>::get_left_part_length_(int16_t pos)
      {
        int16_t half_size = static_cast<int16_t>((this->size_ + 1) / 2);
        int16_t length = half_size;
        if (pos < half_size)
        {
          length--;
        }
        return length;
      }

      template<class K, class V>
      int16_t BtreeLeafNode<K, V>::get_right_part_length_(int16_t pos)
      {
        int16_t half_size = static_cast<int16_t>((this->size_ + 1) / 2);
        int16_t length = static_cast<int16_t>(this->size_ - half_size);
        if (pos < half_size)
        {
          length++;
        }
        return length;
      }

    } // end namespace cmbtree
  } // end namespace common
} // end namespace oceanbase

#endif

