#ifndef OCEANBASE_COMMON_BTREE_BTREE_ROOT_POINTER_H_
#define OCEANBASE_COMMON_BTREE_BTREE_ROOT_POINTER_H_

#include "btree_node.h"
#include "btree_define.h"
#include "btree_array_list.h"

namespace oceanbase
{
  namespace common
  {
    /**
     * 用于指向root节点的指针对象
     */
    class BtreeRootPointer
    {
      friend class BtreeBase;
      friend class BtreeWriteHandle;
      friend class BtreeRootPointerList;
    public:
      /**
       * 初始化
       */
      void init(BtreeAlloc *allocator);

      /**
       * 增加到copy_node_中
       */
      void add_node_list(BtreeNode *node);

      /**
       * 释放掉所有copy_node_
       */
      int32_t release_node_list(int32_t flag);

      /**
       * new出一个新节点
       */
      BtreeNode *new_node(int32_t count);

      /**
       * copy出一个节点来
       */
      BtreeNode *copy_node(BtreeNode *node, BtreeAlloc *key_has_ref);

      /**
       * 把proot加在些rootpointer后面
       */
      void add_tail(BtreeRootPointer *proot, BtreeRootPointer* &tail);

      /**
       * 从list中把自己移出来
       */
      void remove_from_list(BtreeRootPointer* &tail);

      /**
       * 把所有node设成只读, 并且把is_deleted的node释放掉
       */
      void set_read_only(BtreeAlloc *key_allocator, bool remove);

      /**
       * 清空copy_list, 不对list上node进行释放, CLEAR_LIST_ONLY
       */
      void clear_list_only();

      /**
       * 释放所有copy_node_
       * @param sequence == 0 释放所有的node
       */
      int32_t release_node_list(int64_t sequence, BtreeAlloc *key_has_ref);

      /**
       * 引用计数CAS
       */
      static unsigned char refcas(volatile int64_t *ref, int64_t old_value, int64_t new_value);

      /**
       * copy node数量
       */
      int32_t get_copy_node_count();

      /**
       * reset copy list
       */
      void node_list_reset();

      /**
       * 对后面的空的list进行回收
       */
      void shrink_list();

      /**
       * 得到初始值
       */
      void node_list_first(BtreeNodeList &list);

      /**
       * 得到node_list中的next
       */
      BtreeNode *node_list_next(BtreeNodeList &list);

      /**
       * 得到分配器
       */
      BtreeAlloc *get_node_allocator();

      /**
       * 释放掉自己
       */
      void destroy();

      /**
       * 需要设成only
       */
      int8_t need_set_only();
    private:
      // 下一节点的指针
      BtreeRootPointer *next_pointer_;
      BtreeRootPointer *prev_pointer_;
      // 顺序号
      int64_t sequence_;
      // 引用计数
      volatile int64_t ref_count_;
      // node分配器
      BtreeAlloc *node_allocator_;
      // 存放树的root节点
      BtreeNode *root_[CONST_MAX_BTREE_COUNT];
      // 树的深度
      int8_t tree_depth_[CONST_MAX_BTREE_COUNT];
      // 统计
      int8_t new_node_create_;
      int64_t total_object_count_;
      int32_t node_count_;

      // 存放复制过的节点
      BtreeArrayList node_list_;
      // 下面不要定义变量, 保证copy_node_放在最后一个
    };


    class BtreeRootPointerList
    {
    public:
      /*
       * 构造函数
       */
      BtreeRootPointerList();

      /*
       * 析构函数
       */
      ~BtreeRootPointerList();

      /*
       * 出链
       */
      BtreeRootPointer *pop();

      /*
       * 清空
       */
      void clear();

      /*
       * 入链
       */
      void push(BtreeRootPointer *BtreeRootPointer);

      /*
       * 长度
       */
      int size();

      /*
       * 是否为空
       */
      bool empty();

    protected:
      BtreeRootPointer *_head;  // 链头
      BtreeRootPointer *_tail;  // 链尾
      int _size;      // 元素数量
    };


  } // end namespace common
} // end namespace oceanbase

#endif

