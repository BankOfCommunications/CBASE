////===================================================================
 //
 // ob_table_mgr.h updateserver / Oceanbase
 //
 // Copyright (C) 2010, 2011 Taobao.com, Inc.
 //
 // Created on 2011-03-23 by Yubai (yubai.lk@taobao.com) 
 //
 // -------------------------------------------------------------------
 //
 // Description
 //
 // table文件管理器
 // 管理活跃表和冻结表(包括memtable和sstable)
 //
 // -------------------------------------------------------------------
 // 
 // Change Log
 //
////====================================================================

#ifndef  OCEANBASE_UPDATESERVER_TABLE_MGR_H_
#define  OCEANBASE_UPDATESERVER_TABLE_MGR_H_
#include <sys/types.h>
#include <dirent.h>
#include <sys/vfs.h>
#include <unistd.h>
#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <pthread.h>
#include <new>
#include <algorithm>
#include "common/ob_atomic.h"
#include "common/ob_define.h"
#include "common/ob_vector.h"
#include "common/page_arena.h"
#include "common/hash/ob_hashmap.h"
#include "common/ob_list.h"
#include "common/ob_regex.h"
#include "common/ob_fileinfo_manager.h"
#include "common/ob_tsi_factory.h"
#include "common/ob_spin_rwlock.h"
#include "common/ob_timer.h"
#include "common/ob_row_compaction.h"
#include "common/ob_iterator_adaptor.h"
#include "common/ob_resource_pool.h"
#include "common/ob_log_writer.h"
#include "common/btree/key_btree.h"
#include "sstable/ob_sstable_row.h"
#include "sstable/ob_sstable_block_builder.h"
#include "sstable/ob_sstable_trailer.h"
#include "sstable/ob_sstable_reader.h"
#include "sstable/ob_sstable_scanner.h"
#include "sstable/ob_sstable_getter.h"
#include "sstable/ob_sstable_schema.h" /*add zhaoqiong [Truncate Table]:20160318*/
#include "ob_ups_utils.h"
#include "ob_sstable_mgr.h"
#include "common/ob_column_filter.h"
#include "ob_memtable.h"
#include "ob_memtable_rowiter.h"
#include "ob_inc_seq.h"

namespace oceanbase
{
  namespace updateserver
  {
    typedef MemTableTransDescriptor TableTransDescriptor;

    class ITableIterator : public common::ObIterator
    {
      public:
        virtual ~ITableIterator() {};
        virtual void reset() = 0;
    };

    class MemTableEntityIterator : public ITableIterator
    {
      public:
        MemTableEntityIterator();
        ~MemTableEntityIterator();
      public:
        virtual int next_cell();
        virtual int get_cell(common::ObCellInfo **cell_info);
        virtual int get_cell(common::ObCellInfo **cell_info, bool *is_row_changed);
        virtual int is_row_finished(bool *is_row_finished);
        virtual void reset();
        MemTableIterator &get_memtable_iter();
      private:
        MemTableIterator memtable_iter_;
        common::ObRowCompaction rc_;
    };

    class SSTableEntityIterator : public ITableIterator
    {
      public:
        SSTableEntityIterator();
        ~SSTableEntityIterator();
      public:
        virtual int next_cell();
        virtual int get_cell(common::ObCellInfo **cell_info);
        virtual int get_cell(common::ObCellInfo **cell_info, bool *is_row_changed);
        virtual int is_row_finished(bool *is_row_finished);
        virtual void reset();
        inline common::ObGetParam &get_get_param();
        void set_sstable_iter(common::ObIterator *sstable_iter);
        void set_column_filter(common::ColumnFilter *column_filter);
        sstable::ObSSTableGetter &get_sstable_getter();
        sstable::ObSSTableScanner &get_sstable_scanner();
      private:
        common::ObGetParam get_param_;
        common::ObIterator *sstable_iter_;
        sstable::ObSSTableGetter sst_getter_;
        sstable::ObSSTableScanner sst_scanner_;
        common::ColumnFilter *column_filter_;
        //common::ObCellInfo cell_info_;
        common::ObStringBuf string_buf_;
        common::ObCellInfo *cur_ci_ptr_;
        common::ObCellInfo nop_cell_;
        //bool is_iter_end_;
        bool is_row_changed_;
        bool row_has_changed_;
        //bool row_has_expected_column_;
        //bool row_has_returned_column_;
        //bool need_not_next_;
        //bool is_sstable_iter_end_;
        int64_t iter_counter_;
    };

    class ITableUtils
    {
      public:
        virtual ~ITableUtils() {};
      public:
        virtual TableTransDescriptor &get_trans_descriptor() = 0;
        virtual void reset() = 0;
    };

    class MemTableUtils : public ITableUtils
    {
      public:
        MemTableUtils();
        ~MemTableUtils();
      public:
        virtual TableTransDescriptor &get_trans_descriptor();
        virtual void reset();
      private:
        TableTransDescriptor trans_descriptor_;
    };

    class SSTableUtils : public ITableUtils
    {
      public:
        SSTableUtils();
        ~SSTableUtils();
      public:
        virtual TableTransDescriptor &get_trans_descriptor();
        virtual void reset();
      private:
        TableTransDescriptor trans_descriptor_;
    };

    class TableItem;
    class ITableEntity
    {
      public:
        typedef ObResourcePool<SSTableEntityIterator> SSTableResourcePool;
        typedef ObResourcePool<MemTableEntityIterator> MemTableResourcePool;
        typedef ObResourcePool<ObRowIterAdaptor> RowIterAdaptorResourcePool;
        typedef SSTableResourcePool::Guard SSTableGuard;
        typedef MemTableResourcePool::Guard MemTableGuard;
        typedef RowIterAdaptorResourcePool::Guard RowIterAdaptorGuard;
        class ResourcePool
        {
          public:
            SSTableResourcePool &get_sstable_rp() {return sstable_rp_;};
            MemTableResourcePool &get_memtable_rp() {return memtable_rp_;};
            RowIterAdaptorResourcePool &get_row_iter_adaptor_rp() { return row_iter_adaptor_rp_; }
          private:
            SSTableResourcePool sstable_rp_;
            MemTableResourcePool memtable_rp_;
            RowIterAdaptorResourcePool row_iter_adaptor_rp_;
        };
        class Guard
        {
          public:
            Guard(ResourcePool &rp) : sstable_guard_(rp.get_sstable_rp()),
                                      memtable_guard_(rp.get_memtable_rp()),
                                      row_iter_adaptor_guard_(rp.get_row_iter_adaptor_rp())
            {
            };
            void reset()
            {
              sstable_guard_.reset();
              memtable_guard_.reset();
              row_iter_adaptor_guard_.reset();
            };
          public:
            SSTableGuard &get_sstable_guard() {return sstable_guard_;};
            MemTableGuard &get_memtable_guard() {return memtable_guard_;};
            RowIterAdaptorGuard &get_row_iter_adaptor_guard() {return row_iter_adaptor_guard_;};
          private:
            SSTableGuard sstable_guard_;
            MemTableGuard memtable_guard_;
            RowIterAdaptorGuard row_iter_adaptor_guard_;
        };
        enum TableType
        {
          MEMTABLE = 0,
          SSTABLE = 1,
        };
        struct SSTableMeta
        {
          int64_t time_stamp;
          int64_t sstable_loaded_time;
        };
      public:
        ITableEntity(TableItem &table_item) : table_item_(table_item)
        {
        };
      public:
        virtual ~ITableEntity() {};
      public:
        virtual ITableUtils *get_tsi_tableutils(const int64_t index) = 0;
        virtual ITableIterator *alloc_iterator(ResourcePool &rp, Guard &guard) = 0;
        virtual ObRowIterAdaptor *alloc_row_iter_adaptor(ResourcePool &rp, Guard &guard);
        virtual int get(TableTransDescriptor &trans_handle,
                        const uint64_t table_id,
                        const common::ObRowkey &row_key,
                        common::ColumnFilter *column_filter,
                        ITableIterator *iter) = 0;
        virtual int get(const BaseSessionCtx &session_ctx,
                        const uint64_t table_id,
                        const common::ObRowkey &row_key,
                        common::ColumnFilter *column_filter,
                        ITableIterator *iter) = 0;
        virtual int get(const BaseSessionCtx &session_ctx,
                        const uint64_t table_id,
                        const common::ObRowkey &row_key,
                        common::ColumnFilter *column_filter,
                        const sql::ObLockFlag lock_flag,
                        ITableIterator *iter) = 0;
        virtual int scan(TableTransDescriptor &trans_handle,
                        const common::ObScanParam &scan_param,
                        ITableIterator *iter) = 0;
        virtual int scan(const BaseSessionCtx &session_ctx,
                        const common::ObScanParam &scan_param,
                        ITableIterator *iter) = 0;
        virtual int start_transaction(TableTransDescriptor &trans_descriptor) = 0;
        virtual int end_transaction(TableTransDescriptor &trans_descriptor) = 0;
        virtual void ref() = 0;
        virtual void deref() = 0;
        virtual TableType get_table_type() = 0;
        virtual int get_table_truncate_stat(uint64_t table_id, bool &is_truncated) = 0; /*add zhaoqiong [Truncate Table]:20170519*/
      public:
        inline TableItem &get_table_item()
        {
          return table_item_;
        };
        inline static common::ColumnFilter *get_tsi_columnfilter()
        {
          using namespace common;
          return GET_TSI_MULT(common::ColumnFilter, TSI_UPS_COLUMN_FILTER_1);
        };
        inline static common::ObScanParam *get_tsi_scanparam()
        {
          using namespace common;
          return GET_TSI_MULT(common::ObScanParam, TSI_UPS_SCAN_PARAM_2);
        };
      protected:
        static const int64_t MAX_TABLE_UTILS_NUM = common::OB_UPS_MAX_MINOR_VERSION_NUM;
        TableItem &table_item_;
    };

    class MemTableEntity : public ITableEntity
    {
      struct TableUtilsSet
      {
        MemTableUtils data[MAX_TABLE_UTILS_NUM];
      };
      public:
        MemTableEntity(TableItem &table_item);
        virtual ~MemTableEntity();
      public:
        virtual ITableUtils *get_tsi_tableutils(const int64_t index);
        virtual ITableIterator *alloc_iterator(ResourcePool &rp, Guard &guard);
        virtual int get(TableTransDescriptor &trans_handle,
                        const uint64_t table_id,
                        const common::ObRowkey &row_key,
                        common::ColumnFilter *column_filter,
                        ITableIterator *iter);
        virtual int get(const BaseSessionCtx &session_ctx,
                        const uint64_t table_id,
                        const common::ObRowkey &row_key,
                        common::ColumnFilter *column_filter,
                        ITableIterator *iter);
        virtual int get(const BaseSessionCtx &session_ctx,
                        const uint64_t table_id,
                        const common::ObRowkey &row_key,
                        common::ColumnFilter *column_filter,
                        const sql::ObLockFlag lock_flag,
                        ITableIterator *iter);
        virtual int scan(TableTransDescriptor &trans_handle,
                        const common::ObScanParam &scan_param,
                        ITableIterator *iter);
        virtual int scan(const BaseSessionCtx &session_ctx,
                        const common::ObScanParam &scan_param,
                        ITableIterator *iter);
        virtual int start_transaction(TableTransDescriptor &trans_descriptor);
        virtual int end_transaction(TableTransDescriptor &trans_descriptor);
        virtual void ref();
        virtual void deref();
        virtual TableType get_table_type();
        MemTable &get_memtable();
        virtual int get_table_truncate_stat(uint64_t table_id, bool &is_truncated); /*add zhaoqiong [Truncate Table]:20170519*/
      private:
        MemTable memtable_;
    };

    class SSTableEntity : public ITableEntity
    {
      struct TableUtilsSet
      {
        SSTableUtils data[MAX_TABLE_UTILS_NUM];
      };
      public:
        SSTableEntity(TableItem &table_item);
        virtual ~SSTableEntity();
      public:
        virtual ITableUtils *get_tsi_tableutils(const int64_t index);
        virtual ITableIterator *alloc_iterator(ResourcePool &rp, Guard &guard);
        virtual int get(TableTransDescriptor &trans_handle,
                        const uint64_t table_id,
                        const common::ObRowkey &row_key,
                        common::ColumnFilter *column_filter,
                        ITableIterator *iter);
        virtual int get(const BaseSessionCtx &session_ctx,
                        const uint64_t table_id,
                        const common::ObRowkey &row_key,
                        common::ColumnFilter *column_filter,
                        ITableIterator *iter);
        virtual int get(const BaseSessionCtx &session_ctx,
                        const uint64_t table_id,
                        const common::ObRowkey &row_key,
                        common::ColumnFilter *column_filter,
                        const sql::ObLockFlag lock_flag,
                        ITableIterator *iter);
        virtual int scan(TableTransDescriptor &trans_handle,
                        const common::ObScanParam &scan_param,
                        ITableIterator *iter);
        virtual int scan(const BaseSessionCtx &session_ctx,
                        const common::ObScanParam &scan_param,
                        ITableIterator *iter);
        virtual int start_transaction(TableTransDescriptor &trans_descriptor);
        virtual int end_transaction(TableTransDescriptor &trans_descriptor);
        virtual void ref();
        virtual void deref();
        virtual TableType get_table_type();
        uint64_t get_sstable_id() const;
        void set_sstable_id(const uint64_t sstable_id);
        int init_sstable_meta(SSTableMeta &sst_meta);
        void destroy_sstable_meta();
        void pre_load_sstable_block_index();
        int get_endkey(const uint64_t table_id, common::ObTabletInfo &ci);
        bool check_sstable_checksum(const uint64_t remote_sstable_checksum);
        virtual int get_table_truncate_stat(uint64_t table_id, bool &is_truncated); /*add zhaoqiong [Truncate Table]:20170519*/
      private:
        uint64_t sstable_id_;
        common::ModulePageAllocator mod_;
        common::ModuleArena allocator_;
        sstable::ObSSTableReader *sstable_reader_;
        uint64_t remote_sstable_checksum_;
        common::ObSpinLock sstable_reader_lock_;
    };

    typedef MemTableEntity InMemTableEntity;
    class TableMgr;
    class TableItem
    {
      public:
        enum Stat
        {
          UNKNOW = 0,
          ACTIVE = 1,   // 是活跃表 可作为起始状态
          FREEZING = 2, // 正在冻结
          FROZEN = 3,   // 已经冻结
          DUMPING = 4,  // 正在转储
          DUMPED = 5,   // 已经转储sstable
          DROPING = 6,  // 正在释放
          DROPED = 7,   // 内存表已释放 可作为起始状态
        };
        const static int64_t DEFAULT_INMEMTABLE_HASH_SIZE = 1000000;
      public:
        TableItem();
        ~TableItem();
      public:
        ITableEntity *get_table_entity(int64_t &sstable_percent, uint64_t table_id=OB_INVALID_ID);
        int get_table_truncate_stat(uint64_t table_id, bool &is_truncated); /*add zhaoqiong [Truncate Table]:20160318*/
        MemTable &get_memtable();
        MemTable &get_inmemtable();
        int init_sstable_meta();
        Stat get_stat() const;
        void set_stat(const Stat stat);
        uint64_t get_sstable_id() const;
        void set_sstable_id(const uint64_t sstable_id);
        int64_t get_time_stamp() const;
        int64_t get_sstable_loaded_time() const;
        int do_freeze(const uint64_t clog_id, const int64_t time_stamp);
        int do_dump(common::ObILogWriter &log_writer);
        int do_drop();
        int freeze_memtable();
        bool dump_memtable();
        bool drop_memtable();
        bool erase_sstable();
        int get_schema(CommonSchemaManagerWrapper &sm) const;
        int get_endkey(const uint64_t table_id, common::ObTabletInfo &ci);
        bool check_sstable_checksum(const uint64_t remote_sstable_checksum)
        {return sstable_entity_.check_sstable_checksum(remote_sstable_checksum);};
      protected:
        int load_inmemtable(ObUpsTableMgr& table_mgr, SessionMgr& session_mgr, LockMgr& lock_mgr);
      public:
        int64_t inc_ref_cnt();
        int64_t dec_ref_cnt();
      private:
        InMemTableEntity inmemtable_entity_;
        MemTableEntity memtable_entity_;
        SSTableEntity sstable_entity_;
        Stat stat_;
        bool is_inmemtable_ready_;
        volatile int64_t ref_cnt_;
        uint64_t clog_id_;
        MemTableRowIterator row_iter_;
        int64_t time_stamp_;
        int64_t sstable_loaded_time_;
        CommonSchemaManager *schema_;
    };

    typedef common::ObList<ITableEntity*> TableList;

    class TableMgr : public ISSTableObserver, public IExternMemTotal
    {
      struct TableItemKey
      {
        SSTableID sst_id;
        uint64_t reserver;
        TableItemKey()
        {
        };
        TableItemKey(const SSTableID &other_sst_id)
        {
          sst_id = other_sst_id;
        };
        inline int operator- (const TableItemKey &other) const
        {
          return SSTableID::compare(sst_id, other.sst_id);
        };
      };
      typedef common::KeyBtree<TableItemKey, TableItem*> TableItemMap;
      typedef common::hash::SimpleAllocer<TableItem> TableItemAllocator;
      static const int64_t MIN_MAJOR_VERSION_KEEP = 2;
      public:
        enum FreezeType
        {
          AUTO_TRIG = 0,
          FORCE_MINOR = 1,
          FORCE_MAJOR = 2,
          MINOR_LOAD_BYPASS = 3,
          MAJOR_LOAD_BYPASS = 4,
        };
      public:
        TableMgr(common::ObILogWriter &log_writer);
        ~TableMgr();
      public:
        virtual int add_sstable(const uint64_t sstable_id);
        virtual int erase_sstable(const uint64_t sstable_id);
      public:
        int init();
        void destroy();

        // 在播放日志完成后调用
        int sstable_scan_finished(const int64_t minor_num_limit);

//        // 根据truncate info修正版本号范围
//        int check_table_range(const common::ObVersionRange &version_range,
//                          bool is_truncated,
//                          uint64_t table_id = OB_INVALID_ID); /*add zhaoqiong [Truncate Table]:20160318*/
        // 仅判断版本号范围内有无truncate操作,
        int check_table_range(const common::ObVersionRange &version_range,
                          bool &is_truncated,
                          uint64_t table_id = OB_INVALID_ID); /*add zhaoqiong [Truncate Table]:20170519*/

        // 根据版本号范围获取一组table entity
        int acquire_table(const common::ObVersionRange &version_range,
                          uint64_t &max_version,
                          TableList &table_list,
                          bool &is_final_minor,
                          uint64_t table_id=OB_INVALID_ID);
        // 释放一组table entity
        void revert_table(const TableList &table_list);

        // 获取当前的活跃内存表
        TableItem *get_active_memtable();
        // 归还活跃表
        void revert_active_memtable(TableItem *table_item);

        // 只有slave或回放日志时调用
        // 冻结当前活跃表 frozen version为传入参数
        int replay_freeze_memtable(const uint64_t new_version,
                                  const uint64_t frozen_version,
                                  const uint64_t clog_id,
                                  const int64_t time_stamp = 0);
        // 只有master调用
        // 主线程定期调用 内存占用超过mem_limit情况下冻结活跃表
        // 当前版本冻结表个数超过num_limit情况下执行major free 由定时线程触发异步任务
        // 执行major free的时候要与slave同步日志
        int try_freeze_memtable(const int64_t mem_limit, const int64_t num_limit,
                                const int64_t min_major_freeze_interval,
                                uint64_t &new_version, uint64_t &frozen_version,
                                uint64_t &clog_id, int64_t &time_stamp,
                                bool &major_version_changed);
        int try_freeze_memtable(const FreezeType freeze_type,
                                uint64_t &new_version, uint64_t &frozen_version,
                                uint64_t &clog_id, int64_t &time_stamp,
                                bool &major_version_changed);
        bool need_auto_freeze() const;
        // 工作线程调用 将冻结表转储为sstable
        // 返回false表示没有冻结表需要转储了
        bool try_dump_memtable();
        // 主线程定期调用 内存占用超过mem_limit情况下释放冻结表 由定时线程触发异步任务
        bool try_drop_memtable(const int64_t mem_limit);
        bool try_drop_memtable(const bool force);
        // 工作线程调用 卸载并删除生成时间到现在超过time_limit的sstable文件
        void try_erase_sstable(const int64_t time_limit);
        void try_erase_sstable(const bool force);

        int check_sstable_id();

        void log_table_info();

        void set_memtable_attr(const MemTableAttr &memtable_attr);
        int get_memtable_attr(MemTableAttr &memtable_attr);

        int64_t get_frozen_memused() const;
        int64_t get_frozen_memtotal() const;
        int64_t get_frozen_rowcount() const;
        uint64_t get_active_version();
        int get_table_time_stamp(const uint64_t major_version, int64_t &time_stamp);
        int get_oldest_memtable_size(int64_t &size, uint64_t &major_version);
        int get_oldest_major_version(uint64_t &oldest_major_version);
        int get_schema(const uint64_t major_version, CommonSchemaManagerWrapper &sm);
        int get_sstable_range_list(const uint64_t major_version, const uint64_t table_id,
                                  TabletInfoList &ti_list);
        TransMgr& get_trans_mgr();
        void dump_memtable2text(const common::ObString &dump_dir);
        int clear_active_memtable();

        virtual int64_t get_extern_mem_total();

        void set_warm_up_percent(const int64_t warm_up_percent);

        void lock_freeze();
        void unlock_freeze();
        uint64_t get_cur_major_version() const;
        uint64_t get_cur_minor_version() const;
        uint64_t get_last_clog_id() const;
        common::ObList<uint64_t> &get_table_list2add();
        int add_table_list();

        uint64_t get_merged_version() const;
        int64_t get_merged_timestamp() const;
        void set_merged_version(const uint64_t merged_version, const int64_t merged_timestamp);

        ITableEntity::ResourcePool &get_resource_pool();

        int check_sstable_checksum(const uint64_t version, const uint64_t remote_sstable_checksum);
      private:
        TableItem *freeze_active_(const uint64_t new_version);
        void revert_memtable_(TableItem *table_item);
        int64_t get_warm_up_percent_();
        int modify_active_version_(common::BtreeWriteHandle &write_handle,
                                  const int64_t major_version,
                                  const int64_t minor_version);
        bool less_than(const TableItemKey *v, const TableItemKey *t, int exclusive_equal);
      private:
        common::ObILogWriter &log_writer_;
        bool inited_;
        bool sstable_scan_finished_;
        TableItemAllocator table_allocator_;
        //common::SpinRWLock map_lock_;
        RWLock map_lock_;
        TableItemMap table_map_;

        uint64_t cur_major_version_;
        uint64_t cur_minor_version_;
        TableItem *active_table_item_;

        int64_t frozen_memused_;
        int64_t frozen_memtotal_;
        int64_t frozen_rowcount_;
        int64_t last_major_freeze_time_;

        MemTableAttr memtable_attr_;

        int64_t cur_warm_up_percent_;

        common::SpinRWLock freeze_lock_;
        common::ObList<uint64_t> table_list2add_;
        uint64_t last_clog_id_;

        uint64_t merged_version_;
        int64_t merged_timestamp_;

        ITableEntity::ResourcePool resource_pool_;
        TransMgr trans_mgr_;
    };

    extern void thread_read_prepare();
    extern void thread_read_complete();

    class MajorFreezeDuty : public common::ObTimerTask
    {
      public:
        static const int64_t SCHEDULE_PERIOD = 1000000L;
      public:
        MajorFreezeDuty() : same_minute_flag_(false) {};
        virtual ~MajorFreezeDuty() {};
        virtual void runTimerTask();
      private:
        bool same_minute_flag_;
    };

    class HandleFrozenDuty : public common::ObTimerTask
    {
      public:
        static const int64_t SCHEDULE_PERIOD = 1L * 60L * 1000000L;
      public:
        HandleFrozenDuty() {};
        virtual ~HandleFrozenDuty() {};
        virtual void runTimerTask();
    };
  }
}

#endif //OCEANBASE_UPDATESERVER_TABLE_MGR_H_

