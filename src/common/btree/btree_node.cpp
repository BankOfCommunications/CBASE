#include "btree_alloc.h"
#include "btree_node.h"

namespace oceanbase
{
  namespace common
  {
    /**
     * BTree节点
     */

    BtreeNode::BtreeNode()
    {
      max_size_ = 0;
      size_ = 0;
      read_only_ = 0;
      deleted_ = 0;
      sequence_ = 0;
      is_leaf_ = 0;
    }

    /**
     * 析构函数
     */
    BtreeNode::~BtreeNode()
    {
    }

    /**
     * 初始化
     */
    int32_t BtreeNode::init(int16_t max_size)
    {
      OCEAN_BTREE_CHECK_TRUE(max_size > 0, "max_size: %d", max_size);

      int32_t ret = ERROR_CODE_FAIL;
      if (max_size > 0)
      {
        max_size_ = max_size;
        ret = ERROR_CODE_OK;
      }
      return ret;
    }

    /**
     * 是否只读的node
     */
    int32_t BtreeNode::is_read_only()
    {
      return read_only_;
    }

    /**
     * 设置node为只读
     */
    void BtreeNode::set_read_only(int32_t value)
    {
      read_only_ = (value ? 1 : 0);
    }

    /**
     * 是否deleted
     */
    int32_t BtreeNode::is_deleted()
    {
      return deleted_;
    }

    /**
     * 设置deleted
     */
    void BtreeNode::set_deleted(int32_t value)
    {
      deleted_ = (value ? 1 : 0);
    }

    /**
     * 在pos位置增加一个key
     */
    int32_t BtreeNode::add_pair(int16_t pos, const char *key, const char *value, BtreeKeyCompare *
#ifdef OCEAN_BTREE_CHECK
                                compr
#endif
                               )
    {
      OCEAN_BTREE_CHECK_TRUE(0 == read_only_, "read_only: %d", read_only_);
      OCEAN_BTREE_CHECK_TRUE(pos >= 0 && pos <= size_, "pos: %d, size: %d", pos, size_);
      OCEAN_BTREE_CHECK_TRUE(size_ >= 0 && size_ < max_size_ && max_size_ == CONST_NODE_OBJECT_COUNT,
                             "size: %d, max_size: %d, cnt: %d", size_, max_size_, CONST_NODE_OBJECT_COUNT);

      int32_t ret = ERROR_CODE_FAIL;
      if (0 == read_only_ && pos >= 0 && pos <= size_ && size_ < max_size_)
      {

        // 向后移出一个pair
        for(int32_t i = size_; i > pos; i--)
        {
          pairs_[i].key_ = pairs_[i-1].key_;
          pairs_[i].value_ = pairs_[i-1].value_;
        }

        // 增加
        pairs_[pos].key_ = const_cast<char*>(key);
        pairs_[pos].value_ = const_cast<char*>(value);
        size_ ++;
        ret = ERROR_CODE_OK;
      }

#ifdef OCEAN_BTREE_CHECK
      int64_t f = 0;
      if (compr && pos > 0 && (f = (*compr)(pairs_[pos-1].key_, key)) >= 0)
      {
        OCEAN_BTREE_CHECK_TRUE(0, "smaller than the left key, pos=%d, flag=%ld", pos, f);
      }
      if (compr && pos + 1 < size_ && (f = (*compr)(key, pairs_[pos+1].key_)) >= 0)
      {
        OCEAN_BTREE_CHECK_TRUE(0, "bigger than the right key, pos=%d, flag=%ld", pos, f);
      }
#endif

      return ret;
    }

    /**
     * 把pos位置key删除掉
     */
    int32_t BtreeNode::remove_pair(int16_t pos, BtreeKeyValuePair &pair)
    {
      OCEAN_BTREE_CHECK_TRUE(0 == read_only_ && pos >= 0 && pos < size_,
                             "read_only: %d, pos: %d, size: %d", read_only_, pos, size_);
      OCEAN_BTREE_CHECK_TRUE(max_size_ == CONST_NODE_OBJECT_COUNT, "max_size: %d", max_size_);

      int32_t ret = ERROR_CODE_FAIL;
      if (0 == read_only_ && pos >= 0 && pos < size_ && size_ <= CONST_NODE_OBJECT_COUNT)
      {
        pair.key_ = pairs_[pos].key_;
        pair.value_ = pairs_[pos].value_;
        // 向前移一个pair
        size_ --;
        for(int32_t i = pos; i < size_; i++)
        {
          pairs_[i].key_ = pairs_[i+1].key_;
          pairs_[i].value_ = pairs_[i+1].value_;
        }
        ret = ERROR_CODE_OK;
      }


      return ret;
    }

    /**
     * 搜索位置
     */
    int32_t BtreeNode::find_pos(const char *key, BtreeKeyCompare *compr, int64_t &flag)
    {
      flag = -1;
      OCEAN_BTREE_CHECK_FALSE(size_ < 0 || max_size_ != CONST_NODE_OBJECT_COUNT
                              || size_ > max_size_ , "size: %d max_size: %d", size_, max_size_);
      if (size_ < 0 || max_size_ != CONST_NODE_OBJECT_COUNT || size_ > max_size_ )
        return CONST_NODE_OBJECT_COUNT;

      if (0 == size_)
        return 0;

      // 有数据
      int32_t start = 0;
      int32_t mid = 1;
      int32_t end = size_ - 1;

      while (start != end)
      {
        mid = ((start + end) >> 1);
        flag = (*compr)(pairs_[mid].key_, key);
        if (flag > 0)
          end = mid;
        else if (flag < 0)
          start = mid + 1;
        else
        {
          start = mid;
          break;
        }
      }
      if (flag)
      {
        if (mid != start)
          flag = (*compr)(pairs_[start].key_, key);

        if (flag > 0)
        {
          start--;
          flag = -1;
        }
      }

      return start;
    }

    /**
     * 根据pos, 得到key,value对
     */
    BtreeKeyValuePair *BtreeNode::get_pair(int16_t pos)
    {
      if (pos >= 0 && pos < size_ && size_ <= CONST_NODE_OBJECT_COUNT)
        return &pairs_[pos];
      else
        return NULL;
    }

    /**
     * 根据pos, 得到下次key,value对
     */
    BtreeKeyValuePair *BtreeNode::get_next_pair(int16_t pos)
    {
      if (pos >= 0 && pos + 1 < size_ && size_ <= CONST_NODE_OBJECT_COUNT)
        return &pairs_[pos+1];
      else
        return NULL;
    }

    /**
     * 设置key,value对, 如果key不更新设为null, 如果value不更新设为null
     */
    int32_t BtreeNode::set_pair(int16_t pos, const char *key, const char *value, const int32_t flag, BtreeKeyCompare *
#ifdef OCEAN_BTREE_CHECK
                                compr
#endif
                               )
    {
      OCEAN_BTREE_CHECK_TRUE(0 == read_only_ && pos >= 0 && pos < size_,
                             "read_only: %d, pos: %d, size: %d", read_only_, pos, size_);
      OCEAN_BTREE_CHECK_TRUE(max_size_ == CONST_NODE_OBJECT_COUNT, "max_size: %d", max_size_);

      int32_t ret = ERROR_CODE_FAIL;
      if (0 == read_only_ && pos >= 0 && pos < size_ && size_ <= CONST_NODE_OBJECT_COUNT)
      {
        if ((flag & UPDATE_KEY)) pairs_[pos].key_ = const_cast<char*>(key);
        if ((flag & UPDATE_VALUE)) pairs_[pos].value_ = const_cast<char*>(value);
        ret = ERROR_CODE_OK;
      }

#ifdef OCEAN_BTREE_CHECK
      int64_t f = 0;
      if (compr && (flag & UPDATE_KEY) && pos > 0 && (f = (*compr)(pairs_[pos-1].key_, key)) >= 0)
      {
        OCEAN_BTREE_CHECK_TRUE(0, "smaller than the left key, pos=%d, flag=%ld", pos, f);
      }
      if (compr && (flag & UPDATE_KEY) && pos + 1 < size_ && (f = (*compr)(key, pairs_[pos+1].key_)) >= 0)
      {
        OCEAN_BTREE_CHECK_TRUE(0, "bigger than the right key, pos=%d, flag=%ld", pos, f);
      }
#endif
      return ret;
    }

    /**
     * 得到当前个数
     */
    int32_t BtreeNode::get_size()
    {
      return size_;
    }

    /**
     * 移出size个pair到next_node上
     */
    int32_t BtreeNode::move_pair_out(BtreeNode *next_node, int32_t size)
    {
      OCEAN_BTREE_CHECK_TRUE(next_node, "next_node is null.");
      if (next_node)
      {
        OCEAN_BTREE_CHECK_TRUE((0 == read_only_ && 0 == next_node->read_only_ &&
                                size > 0 && size <= size_ && ((size + next_node->size_) <= next_node->max_size_)),
                               "read_only: %d, next_read_only: %d, size: %d, size_:%d, next_size: %d max_size: %d",
                               read_only_, next_node->read_only_, size, size_, next_node->size_, next_node->max_size_);
      }
      OCEAN_BTREE_CHECK_TRUE(max_size_ == CONST_NODE_OBJECT_COUNT, "max_size: %d", max_size_);

      int32_t ret = ERROR_CODE_FAIL;
      if (next_node && 0 == read_only_ && 0 == next_node->read_only_ &&
          size > 0 && size <= size_ && size_ <= CONST_NODE_OBJECT_COUNT &&
          ((size + next_node->size_) <= next_node->max_size_) &&
          next_node->max_size_ == CONST_NODE_OBJECT_COUNT)
      {

        // 把next_node向后移size个位置
        if (next_node->size_ > 0)
          next_node->pair_move_backward(static_cast<int16_t>(size), 0, next_node->size_);

        // copy to next_node
        BtreeKeyValuePair *dst = next_node->pairs_;
        BtreeKeyValuePair *src = pairs_ + size_ - size;
        for(int32_t i = 0; i < size; i++)
        {
          dst->key_ = src->key_;
          dst->value_ = src->value_;
          dst ++;
          src ++;
        }
        next_node->size_ = static_cast<int16_t>(next_node->size_ + size);
        size_ = static_cast<int16_t>(size_ - size);
        ret = ERROR_CODE_OK;
      }
      return ret;
    }

    /**
     * 从next_node上移入size个pair到此node上
     */
    int32_t BtreeNode::move_pair_in(BtreeNode *next_node, int32_t size)
    {
      OCEAN_BTREE_CHECK_TRUE(next_node, "next_node is null.");
      if (next_node)
      {
        OCEAN_BTREE_CHECK_TRUE((0 == read_only_ && 0 == next_node->read_only_ &&
                                size > 0 && size <= next_node->size_ && size + size_ <= max_size_),
                               "read_only: %d, next_read_only: %d, size: %d, size_:%d, next_size: %d max_size: %d",
                               read_only_, next_node->read_only_, size, size_, next_node->size_, max_size_);
      }
      OCEAN_BTREE_CHECK_TRUE(max_size_ == CONST_NODE_OBJECT_COUNT, "max_size: %d", max_size_);

      int32_t ret = ERROR_CODE_FAIL;
      if (next_node && 0 == read_only_ && 0 == next_node->read_only_ &&
          size > 0 && size <= next_node->size_ && next_node->size_ <= CONST_NODE_OBJECT_COUNT &&
          size + size_ <= max_size_ && max_size_ == CONST_NODE_OBJECT_COUNT)
      {

        // 复制到当前node上
        BtreeKeyValuePair *dst = pairs_ + size_;
        BtreeKeyValuePair *src = next_node->pairs_;
        for(int32_t i = 0; i < size; i++)
        {
          dst->key_ = src->key_;
          dst->value_ = src->value_;
          dst ++;
          src ++;
        }
        next_node->size_ = static_cast<int16_t>(next_node->size_ - size);
        size_ = static_cast<int16_t>(size_ + size);

        // 把next_node向前移size个位置
        next_node->pair_move_forward(0, static_cast<int16_t>(size), next_node->size_);
        ret = ERROR_CODE_OK;
      }
      return ret;
    }

    /**
     * 把next_node全部搬到当前节点上, 不改变next_node上的内容.
     */
    int32_t BtreeNode::merge_pair_in(BtreeNode *next_node)
    {
      int32_t ret = ERROR_CODE_FAIL;
      OCEAN_BTREE_CHECK_TRUE(next_node, "next_node is null.");
      if (next_node)
      {
        int32_t size = next_node->size_;

        OCEAN_BTREE_CHECK_TRUE((0 == read_only_ && ((size + size_) <= max_size_)),
                               "read_only: %d, size: %d, size_:%d, max_size: %d",
                               read_only_, size, size_, max_size_);
        OCEAN_BTREE_CHECK_TRUE(max_size_ == CONST_NODE_OBJECT_COUNT, "max_size: %d", max_size_);

        if (0 == read_only_ && size >= 0 &&
            ((size + size_) <= max_size_) &&
            max_size_ == CONST_NODE_OBJECT_COUNT)
        {

          // 复制到当前node上
          BtreeKeyValuePair *dst = pairs_ + size_;
          BtreeKeyValuePair *src = next_node->pairs_;
          for(int32_t i = 0; i < size; i++)
          {
            dst->key_ = src->key_;
            dst->value_ = src->value_;
            dst ++;
            src ++;
          }
          size_ = static_cast<int16_t>(size_ + size);
          ret = ERROR_CODE_OK;
        }
      }
      return ret;
    }

    /**
     * 移动pair对, 向前移
     */
    void BtreeNode::pair_move_forward(int16_t dst, int16_t src, int32_t n)
    {
      OCEAN_BTREE_CHECK_TRUE(dst >= 0 && dst < max_size_, "dst: %d, max_size: %d", dst, max_size_);
      OCEAN_BTREE_CHECK_TRUE(src >= 0 && src < max_size_, "src: %d, max_size: %d", src, max_size_);
      OCEAN_BTREE_CHECK_TRUE(n > 0 && n + src <= max_size_, "n: %d src: %d, max_size: %d", n, src, max_size_);
      OCEAN_BTREE_CHECK_TRUE(max_size_ == CONST_NODE_OBJECT_COUNT, "max_size: %d", max_size_);
      OCEAN_BTREE_CHECK_TRUE(dst < src, "dst: %d, src: %d", dst, src);
      if (n > 0 && n + src <= max_size_)
      {
        while(n-- > 0)
        {
          pairs_[dst].key_ = pairs_[src].key_;
          pairs_[dst++].value_ = pairs_[src++].value_;
        }
      }
    }

    /**
     * 移动pair对, 向后移
     */
    void BtreeNode::pair_move_backward(int16_t dst, int16_t src, int32_t n)
    {
      OCEAN_BTREE_CHECK_TRUE(dst >= 0 && dst < max_size_, "dst: %d, max_size: %d", dst, max_size_);
      OCEAN_BTREE_CHECK_TRUE(src >= 0 && src < max_size_, "src: %d, max_size: %d", src, max_size_);
      OCEAN_BTREE_CHECK_TRUE(n > 0 && n + dst <= max_size_, "n: %d dst: %d, max_size: %d", n, dst, max_size_);

      OCEAN_BTREE_CHECK_TRUE(max_size_ == CONST_NODE_OBJECT_COUNT, "max_size: %d", max_size_);
      OCEAN_BTREE_CHECK_TRUE(dst > src, "dst: %d, src: %d", dst, src);

      if (n > 0 && n + dst <= max_size_)
      {
        dst = static_cast<int16_t>(dst + n);
        src = static_cast<int16_t>(src + n);
        while(n-- > 0)
        {
          pairs_[--dst].key_ = pairs_[--src].key_;
          pairs_[dst].value_ = pairs_[src].value_;
        }
      }
    }

    /**
     * 设置顺序号
     */
    void BtreeNode::set_sequence(int64_t value)
    {
      sequence_ = value;
    }

    /**
     * 得到顺序号
     */
    int64_t BtreeNode::get_sequence()
    {
      return sequence_;
    }

    /**
     * 把key的引用计数加1
     */
    void BtreeNode::inc_key_refcount()
    {
      if (is_leaf_)
      {
        OCEAN_BTREE_CHECK_TRUE(size_ <= max_size_ && max_size_ == CONST_NODE_OBJECT_COUNT,
                               "size: %d, max_size: %d", size_, max_size_);

        int32_t *addr;
        OCEAN_BTREE_CHECK_TRUE(read_only_ == 0, "is read only.");
        for(int32_t i = 0; i < size_; i++)
        {
          addr = reinterpret_cast<int32_t*>(pairs_[i].key_);
          OCEAN_BTREE_CHECK_TRUE(addr, "addr is null.");
          if (addr)
          {
            OCEAN_BTREE_CHECK_TRUE((*addr) >= 0, "addr:%p pos:%d value:%d", addr, i, *addr);
            (*addr) ++;
          }
        }
      }
    }

    /**
     * 把key的引用计数减1
     */
    void BtreeNode::dec_key_refcount(BtreeAlloc *allocator)
    {
      if (is_leaf_)
      {
        OCEAN_BTREE_CHECK_TRUE(size_ <= max_size_ && max_size_ == CONST_NODE_OBJECT_COUNT,
                               "size: %d, max_size: %d", size_, max_size_);

        int32_t *addr;
        for(int32_t i = 0; i < size_; i++)
        {
          addr = reinterpret_cast<int32_t*>(pairs_[i].key_);
          OCEAN_BTREE_CHECK_TRUE(addr, "addr is null.");
          if (addr)
          {
            OCEAN_BTREE_CHECK_TRUE((*addr) > 0, "addr:%p pos:%d value:%d", addr, i, *addr);
            (*addr) --;
            if (0 == (*addr))
            {
              allocator->release(pairs_[i].key_);
            }
          }
        }
      }
    }

    /**
    * 是否叶子节点
    */
    int32_t BtreeNode::is_leaf()
    {
      return is_leaf_;
    }

    /**
     * 设置叶子节点
     */
    void BtreeNode::set_leaf(int32_t value)
    {
      is_leaf_ = (value ? 1 : 0);
    }

#ifdef OCEAN_BASE_BTREE_DEBUG
    int32_t BtreeNode::print(int32_t level, int32_t max_level, char *pbuf)
    {
      fprintf(stderr, "%d ----size: %d, max_size: %d node: %p ----\n", level, size_, max_size_, this);
      int32_t ret = 0;
      for(int32_t i = 0; i < size_; i++)
      {
        fprintf(stderr, "  %d %p => %p\n", *(reinterpret_cast<int32_t*>(pairs_[i].key_)), pairs_[i].key_ + 4, pairs_[i].value_);
      }
      if (level < max_level)
      {
        BtreeNode *node;
        for(int32_t i = 0; i < size_; i++)
        {
          node = reinterpret_cast<BtreeNode*>(pairs_[i].value_);
          if (node)
            ret |= node->print(level + 1, max_level, pbuf);
          else
            ret = 1;
          if (ret) break;
        }
      }
      return ret;
    }
#endif

  } // end namespace common
} // end namespace oceanbase
