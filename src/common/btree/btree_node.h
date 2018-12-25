#ifndef OCEANBASE_COMMON_BTREE_BTREE_NODE_H_
#define OCEANBASE_COMMON_BTREE_BTREE_NODE_H_

#include "btree_define.h"

namespace oceanbase
{
  namespace common
  {
    /**
     * BTree每个节点
     */
    class BtreeAlloc;
    class BtreeNode
    {
    public:
      enum
      {
        UPDATE_KEY = 1,
        UPDATE_VALUE = 2
      };

      /**
       * 构造函数
       */
      BtreeNode();

      /**
       * 析构函数
       */
      ~BtreeNode();

      /**
       * 初始化
       */
      int32_t init(int16_t max_size);

      /**
       * 在pos位置增加一个key
       */
      int32_t add_pair(int16_t pos, const char *key, const char *value, BtreeKeyCompare *compr);

      /**
       * 把pos位置key删除掉, 并且返回删除的BtreeKeyValuePair
       */
      int32_t remove_pair(int16_t pos, BtreeKeyValuePair &pair);

      /**
       * 搜索Pair的位置
       */
      int32_t find_pos(const char *key, BtreeKeyCompare *compr, int64_t &equal_flag);

      /**
       * 根据pos, 得到key,value对
       */
      BtreeKeyValuePair *get_pair(int16_t pos);

      /**
       * 根据pos, 得到下次key,value对
       */
      BtreeKeyValuePair *get_next_pair(int16_t pos);

      /**
       * 设置key,value对
       */
      int32_t set_pair(int16_t pos, const char *key, const char *value, const int32_t flag, BtreeKeyCompare *compr);

      /**
       * 得到当前个数
       */
      int32_t get_size();

      /**
       * 移出size个pair到next_node上
       */
      int32_t move_pair_out(BtreeNode *next_node, int32_t size);

      /**
       * 从next_node上移入size个pair到此node上
       */
      int32_t move_pair_in(BtreeNode *next_node, int32_t size);

      /**
       * 把next_node全部搬到当前节点上, 不改变next_node上的内容.
       */
      int32_t merge_pair_in(BtreeNode *next_node);

      /**
       * 是否只读的node
       */
      int32_t is_read_only();

      /**
       * 设置node为只读
       */
      void set_read_only(int32_t value);

      /**
       * 是否deleted
       */
      int32_t is_deleted();

      /**
       * 设置deleted
       */
      void set_deleted(int32_t value);

      /**
       * 设置顺序号
       */
      void set_sequence(int64_t value);

      /**
       * 得到顺序号
       */
      int64_t get_sequence();

      /**
       * 把key的引用计数加1
       */
      void inc_key_refcount();

      /**
       * 把key的引用计数减1
       */
      void dec_key_refcount(BtreeAlloc *allocator);

      /**
      * 是否叶子
      */
      int32_t is_leaf();

      /**
       * 设置叶子
       */
      void set_leaf(int32_t value);

#ifdef OCEAN_BASE_BTREE_DEBUG
      /**
       * 打印出来
       */
      int32_t print(int32_t level, int32_t max_level, char *pbuf);
#endif

    private:
      /**
       * 移动pair对, 向后移
       */
      void pair_move_backward(int16_t dst, int16_t src, int32_t n);
      /**
       * 移动pair对, 向前移
       */
      void pair_move_forward(int16_t dst, int16_t src, int32_t n);

    private:
      int16_t max_size_;                 // 最大object数
      int16_t size_;                     // 当前pair个数
      uint8_t read_only_: 1;             // 是否只读
      uint8_t deleted_: 1;               // 设置为删除
      uint8_t is_leaf_: 1;               // 是否叶节点
      int64_t sequence_;                 // 是在哪个RootPointer里产生的
      BtreeKeyValuePair pairs_[0];       // key value pair对
    };
  } // end namespace common
} // end namespace oceanbase

#endif

