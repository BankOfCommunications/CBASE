#ifndef OCEANBASE_COMMON_BTREE_BTREE_BASE_H_
#define OCEANBASE_COMMON_BTREE_BTREE_BASE_H_

#include "btree_define.h"
#include "btree_default_alloc.h"
#include "btree_root_pointer.h"
#include "btree_read_handle_new.h"
#include "btree_write_handle.h"

namespace oceanbase
{
  namespace common
  {

    /**
     * BTree 基础类, 对BTree结构的创建, 搜索, 以及copy on write
     */

    class BtreeReadParam;
    class BtreeWriteParam;
    class BtreeRootPointer;
    class BtreeNode;
    class BtreeBase : public BtreeCallback
    {
      friend class BtreeWriteHandle;
    public:
      // 最小的key
      static const char *MIN_KEY;
      // 最大的key
      static const char *MAX_KEY;
      // 每次最大回收的rootpointer数量
      static const int32_t MAX_RECYCLE_ROOT_COUNT;
      // RECYCLE的Mask
      static const int64_t RECYCLE_MASK;
      static const int64_t REFCOUNT_MASK;

    public:
      /**
       * 构造函数
       */
      BtreeBase(BtreeAlloc *allocator = NULL);

      /**
       * 析构函数
       */
      virtual ~BtreeBase();

      /**
       * 得到读的句柄
       */
      int32_t get_read_handle(BtreeBaseHandle &handle);

      /**
       * 得到写的句柄
       */
      int32_t get_write_handle(BtreeWriteHandle &handle);

      /**
       * 得到object数量
       */
      int64_t get_object_count();

      /**
       * 得到node数量
       */
      int32_t get_node_count();

      /**
       * 分配出多少node
       */
      int64_t get_alloc_count();
      /**
       * 分配出多少M内存
       */
      int32_t get_alloc_memory();
      /**
       * root pointer有多少放在freelist中
       */
      int32_t get_freelist_size();
      /**
       * 把树清空
       */
      void clear();
      /**
       * 设置写锁, enable = 0 不要设置锁
       */
      void set_write_lock_enable(int32_t enable);
      /**
       * 把树直接销毁
       */
      void destroy();

#ifdef OCEAN_BASE_BTREE_DEBUG
      /**
       * 打印出来.
       */
      void print(BtreeBaseHandle *handle);
#endif
    protected:
      /**
       * 搜索一个key
       */
      BtreeKeyValuePair *get_one_pair(BtreeBaseHandle &handle, const int32_t tree_id, const char *key);

      /**
       * 搜索一个key, 用于范围
       */
      BtreeKeyValuePair *get_range_pair(BtreeReadHandle &handle);

      /**
       * 插入一key, value对
       */
      int32_t put_pair(BtreeWriteHandle &handle, const char *key, const char *value, const bool overwrite);

      /**
       * 删除一个key
       */
      int32_t remove_pair(BtreeWriteHandle &handle, const int32_t tree_id, const char *key);

    protected:
      /**
       * 内部有几棵树
       */
      int32_t tree_count_;
      /**
       * 树的比较函数
       */
      BtreeKeyCompare *key_compare_[CONST_MAX_BTREE_COUNT];
      /**
       * Key有引用计数
       */
      BtreeAlloc *key_allocator_;

    private:
      /**
       * 搜索下面节点
       */
      int32_t get_children(BtreeRootPointer *proot, const int32_t tree_id, const char *key, BtreeReadParam &param);
      /**
       * 把搜索到的node,复制出一份来
       */
      int32_t copy_node_for_write(BtreeWriteHandle &handle, BtreeWriteParam &param);
      /**
       * 把key,value写到node上
       */
      int32_t add_pair_to_node(BtreeWriteHandle &handle, BtreeWriteParam& param, int32_t level, const char *key, const char *value);
      /**
       * 需要兄弟节点的时候, 把他copy出来
       */
      BtreeNode* copy_next_node_for_write(BtreeWriteHandle &handle, BtreeWriteParam& param, int32_t level);
      /**
       * 从node里删除掉key
       */
      int32_t remove_pair_from_node(BtreeWriteHandle &handle, BtreeWriteParam& param, int32_t level, int32_t next_pos);
      /**
       * 更新parent的first key
       */
      void update_parent_first_key(BtreeWriteHandle &handle, BtreeWriteParam& param, int32_t level);
      /**
       * 移动位置
       */
      int32_t pos_move_next(BtreeReadParam *param, int32_t direction);
      /**
       * 更新key上的引用计数
       */
      void update_key_ref_count(BtreeWriteHandle &handle, char *old_key, char *new_key);
      /**
       * 切换RootPointer并且回归以前的RootPointer
       */
      int callback_done(void *data);
      /**
       * 把BTree所有的node放入node_list等待删除
       */
      void remove_allnode(BtreeWriteHandle &handle, BtreeNode *root, int32_t depth, int32_t level);

    private:
      // 空的btree rootpointer, 为了保证root_pointer不设成null.
      BtreeRootPointer empty_root_pointer_;
      // root_pointer list的头节点
      BtreeRootPointer *root_pointer_;
      // root_pointer list的尾节点
      BtreeRootPointer *root_tail_;
      // root_pointer 回收节点
      BtreeRootPointer *root_recycle_;
      // 写的锁
      pthread_mutex_t mutex_;
      pthread_mutex_t *wl_mutex_;
      // 节点分配器
      BtreeAlloc *node_allocator_;
      BtreeDefaultAlloc node_default_allocator_;
      // root_pointer 序列
      int64_t proot_sequence_;
      // root_pointer freelist
      BtreeRootPointerList proot_freelist_;
    };

  } // end namespace common
} // end namespace oceanbase

#endif

