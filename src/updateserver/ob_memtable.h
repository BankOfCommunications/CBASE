////===================================================================
 //
 // ob_memtable.h updateserver / Oceanbase
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

#ifndef  OCEANBASE_UPDATESERVER_MEMTABLE_H_
#define  OCEANBASE_UPDATESERVER_MEMTABLE_H_
#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <pthread.h>
#include <new>
#include <bitset>
#include <algorithm>
#include "common/ob_atomic.h"
#include "common/ob_define.h"
#include "common/ob_string_buf.h"
#include "common/ob_iterator.h"
#include "common/page_arena.h"
#include "common/ob_mutator.h"
#include "common/ob_read_common_data.h"
#include "common/ob_scanner.h"
#include "common/bloom_filter.h"
#include "common/ob_range2.h"
#include "common/ob_cell_meta.h"
#include "common/ob_column_filter.h"
#include "common/ob_cellinfo_processor.h"
#include "sql/ob_husk_filter.h"
#include "ob_table_engine.h"
#include "ob_ups_mutator.h"
#include "ob_trans_mgr.h"
#include "ob_query_engine.h"
#include "ob_ups_compact_cell_writer.h"
#include "ob_sessionctx_factory.h"
#include "ob_lock_mgr.h"

namespace oceanbase
{
  namespace updateserver
  {
    class MemTable;

    typedef QueryEngine TableEngine;
    typedef QueryEngineIterator TableEngineIterator;

    typedef TETransType MemTableTransType;
    typedef uint64_t MemTableTransDescriptor;

    template <int64_t BUF_SIZE>
    class FixedSizeBuffer
    {
      public:
        char *get_buffer() {return buffer_;};
        int64_t get_size() {return BUF_SIZE;};
      private:
        char buffer_[BUF_SIZE];
    };

    class TransNodeWrapper4Merge : public ITransNode
    {
      public:
        TransNodeWrapper4Merge(const TransNode &tn) : trans_node_(tn)
        {
        };
        virtual ~TransNodeWrapper4Merge()
        {
        };
      public:
        int64_t get_trans_id() const
        {
          return trans_node_.get_min_flying_trans_id();
        };
      private:
        const TransNode &trans_node_;
    };

    //memtable 迭代器重构 如下四个需求全部使用MemTableGetIter迭代
    //  1. [常规get/scan] 需要构造RNE的cell，和根据create_time构造mtime/ctime的cell，如果有列过滤还会构造NOP的cell
    //  2. [QueryEngine的dump2text] 没有列过滤和事务id过滤，不会构造NOP，但是会构造RNE/mtime/ctime
    //  3. [转储] 没有列过滤和事务id过滤，会在QueryEngine跳过空的行，不会构造RNE和NOP，但会构造mtime/ctime
    //  4. [单行merge] merge前需要判断事务id过滤后是否还有数据，如果没有就不调用GetIter迭代，防止构造出RNE写回memtable；此外还需要RowCompaction保证不调整顺序，防止表示事务ID的mtime被调整到普通列的后面
    //  5. [update and returm] 与常规get/scan类似，但没有事务id过滤
    class MemTableScanIter;
    class MemTableRowIterator;
    class MemTableGetIter : public common::ObIterator
    {
      friend class MemTable;
      friend class MemTableScanIter;
      friend class MemTableRowIterator;
      friend class QueryEngine;
      static const int64_t COMPACT_BUFFER_SIZE = 2 * sizeof(common::ObCellMeta) + sizeof(common::ObObj);
      public:
        MemTableGetIter();
        ~MemTableGetIter();
      public:
        int next_cell();
        int get_cell(common::ObCellInfo **cell_info);
        int get_cell(common::ObCellInfo **cell_info, bool *is_row_changed);
      public:
        void reset();
      private:
        void set_(const TEKey &te_key,
                  const TEValue *te_value,
                  const common::ColumnFilter *column_filter,
                  const ITransNode *trans_node);
        void set_(const TEKey &te_key,
                  const TEValue *te_value,
                  const common::ColumnFilter *column_filter,
                  const bool return_rowkey_column,
                  const BaseSessionCtx *session_ctx,
                  const TEValue * table_value = NULL /*add zhaoqiong [Truncate Table]:20170519 */);
        inline const ObCellInfoNode *get_cur_node_iter_() const;
        inline bool trans_end_(const common::ObObj &value);
        inline bool read_uncommited_data_();
        inline ObCellInfoNode *get_list_head_();
      private:
        TEKey te_key_;
        const TEValue *te_value_;
        const TEValue *table_value_; //add zhaoqiong [Truncate Table]:20170519
        const common::ColumnFilter *column_filter_;
        //add duyr [Delete_Update_Function_isolation] [JHOBv0.1] 20160531:b
        ObDataMarkParam data_mark_param_;
        //add duyr 20160531:e
        bool return_rowkey_column_;

        const BaseSessionCtx *session_ctx_;
        bool is_iter_end_;
        const ObCellInfoNode *node_iter_;
        ObCellInfoNodeIterableWithCTime cell_iter_;
        int64_t iter_counter_;
        common::ObCellInfo ci_;

        ObCellInfoNode nop_v_node_;
        char nop_v_buf_[COMPACT_BUFFER_SIZE];
        ObCellInfoNode rne_v_node_;
        char rne_v_buf_[COMPACT_BUFFER_SIZE];
        ObCellInfoNode mctime_v_node_;
        char mctime_v_buf_[COMPACT_BUFFER_SIZE * 2];
    };

    class MemTableScanIter : public common::ObIterator
    {
      friend class MemTable;
      public:
        MemTableScanIter();
        ~MemTableScanIter();
      public:
        int next_cell();
        int get_cell(common::ObCellInfo **cell_info);
        int get_cell(common::ObCellInfo **cell_info, bool *is_row_changed);
      public:
        void reset();
      private:
        void set_(const uint64_t table_id,
                  common::ColumnFilter *column_filter,
                  const TransNode *trans_node);
        void set_(const uint64_t table_id,
                  common::ColumnFilter *column_filter,
                  const bool return_rowkey_column,
                  const BaseSessionCtx *session_ctx,
                  TEValue * value = NULL); /*add zhaoqiong [Truncate Table]:20160318*/
        inline TableEngineIterator &get_te_iter_();
        inline static bool is_row_not_exist_(MemTableGetIter &get_iter);
      private:
        TableEngineIterator te_iter_;
        uint64_t table_id_;
        common::ColumnFilter *column_filter_;
        bool return_rowkey_column_;
        const BaseSessionCtx *session_ctx_;
        bool is_iter_end_;
        TEValue * table_value_; /*add zhaoqiong [Truncate Table]:20160318*/
        MemTableGetIter get_iter_;
    };

    class MemTableIterator : public common::ObIterator
    {
      friend class MemTable;
      public:
        MemTableIterator();
        ~MemTableIterator();
      public:
        int next_cell();
        int get_cell(common::ObCellInfo **cell_info);
        int get_cell(common::ObCellInfo **cell_info, bool *is_row_changed);
      public:
        void reset();
      private:
        MemTableScanIter &get_scan_iter_();
        MemTableGetIter &get_get_iter_();
      private:
        MemTableScanIter scan_iter_;
        MemTableGetIter get_iter_;
        common::ObIterator *iter_;
    };

    struct MemTableAttr
    {
      int64_t total_memlimit;
      //int64_t drop_page_num_once;
      //int64_t drop_sleep_interval_us;
      IExternMemTotal *extern_mem_total;
      MemTableAttr() : total_memlimit(0),
                       //drop_page_num_once(0),
                       //drop_sleep_interval_us(0),
                       extern_mem_total(NULL)
      {
      };
    };

    class ObUpsTableMgr;
    class MemTable : public ITableEngine
    {
      friend class ObCellInfoNode;
      struct RollbackInfo
      {
        TEKey key;
        TEValue *dest;
        TEValue src;
      };
      struct CommitInfo
      {
        int64_t row_counter;
      };
      static const char *MIN_STR;
      static const int64_t MAX_ROW_CELLINFO = 128;
      static const int64_t MAX_ROW_SIZE = common::OB_MAX_ROW_LENGTH / CELL_INFO_SIZE_UNIT;
      static const int64_t BLOOM_FILTER_NHASH = 1;
      static const int64_t BLOOM_FILTER_NBYTE = common::OB_MAX_PACKET_LENGTH - 1 * 1024;
      static const int64_t MAX_TRANS_NUM = 64;
      public:
        MemTable();
        ~MemTable();
      public:
        int init(const int64_t hash_size = 0);
        int destroy();
        bool is_inited() const { return inited_; }
      public:
        int rollback(void *data);
        int commit(void *data);
      public:
        // 插入key-value对，如果已存在则覆盖
        // @param [in] key 待插入的key
        // @param [in] value 待插入的value
        int set(const MemTableTransDescriptor td, ObUpsMutator &mutator, const bool check_checksum = false,
                ObUpsTableMgr *ups_table_mgr = NULL, common::ObScanner *scanner = NULL);
        int set(RWSessionCtx &session_ctx, ILockInfo &lock_info, common::ObMutator &mutator);
        int set(RWSessionCtx &session_ctx, common::ObIterator &iter, const common::ObDmlType dml_type);

        int ensure_cur_row(const TEKey &key, TEValue*& value);
		//add zhaoqiong [Truncate Table]:20170704:b
        int query_cur_row_without_lock(const TEKey &key, TEValue*& value);
		//add:e
        int rdlock_row(ILockInfo* lock_info, const TEKey &key, TEValue*& value, const sql::ObLockFlag lock_flag);
        // 获取指定key的value
        // @param [in] key 要查询的key
        // @param [out] value 查询返回的value
        int get(const MemTableTransDescriptor td,
                const uint64_t table_id, const common::ObRowkey &row_key,
                MemTableIterator &iterator,
                common::ColumnFilter *column_filter = NULL);
        int get(const BaseSessionCtx &session_ctx,
                const uint64_t table_id, const common::ObRowkey &row_key,
                MemTableIterator &iterator,
                common::ColumnFilter *column_filter = NULL, const sql::ObLockFlag lock_flag = sql::LF_NONE);
        // 范围查询，返回一个iterator
        // @param [in] 查询范围的start key
        // @param [in] 查询范围是否包含start key本身, 0为包含, 非0为不包含; 在hash实现时必须为0
        // @param [in] 查询范围的end key
        // @param [in] 查询范围是否包含end key本身, 0为包含, 非0为不包含; 在hash实现时必须为0
        // @param [out] iter 查询结果的迭代器
        int scan(const MemTableTransDescriptor td,
                const common::ObNewRange &range,
                const bool reverse,
                MemTableIterator &iter,
                common::ColumnFilter *column_filter = NULL);
        int scan(const BaseSessionCtx &session_ctx,
                const common::ObNewRange &range,
                const bool reverse,
                MemTableIterator &iter,
                common::ColumnFilter *column_filter = NULL);

        int start_transaction(const TETransType trans_type, MemTableTransDescriptor &td, const int64_t trans_id = -1);

        int end_transaction(const MemTableTransDescriptor td, bool rollback = false);

        // 开始一次事务性的更新
        // 一次mutation结束之前不能开启新的mutation
        int start_mutation(const MemTableTransDescriptor td);

        // 结束一次mutation
        // @param[in] rollback 是否回滚
        int end_mutation(const MemTableTransDescriptor td, bool rollback);

        inline int64_t get_version() const
        {
          return version_;
        };
        inline void set_version(int64_t new_version)
        {
          common::atomic_exchange((uint64_t*)&version_, (uint64_t)new_version);
        };

        inline int64_t get_ref_cnt() const
        {
          return ref_cnt_;
        };
        inline int64_t inc_ref_cnt()
        {
          return common::atomic_inc((uint64_t*)&ref_cnt_);
        };
        inline int64_t dec_ref_cnt()
        {
          return common::atomic_dec((uint64_t*)&ref_cnt_);
        };

        int clear();

        inline int64_t total() const
        {
          return mem_tank_.total();
        };

        inline int64_t used() const
        {
          return mem_tank_.used();
        };

        inline void set_attr(const MemTableAttr &attr)
        {
          mem_tank_.set_total_limit(attr.total_memlimit);
          mem_tank_.set_extern_mem_total(attr.extern_mem_total);
        };

        inline void get_attr(MemTableAttr &attr)
        {
          attr.total_memlimit = mem_tank_.get_total_limit();
          attr.extern_mem_total = mem_tank_.get_extern_mem_total();
        };

        inline void log_memory_info() const
        {
          mem_tank_.log_info();
        };

        void dump2text(const common::ObString &dump_dir)
        {
          const int64_t BUFFER_SIZE = 1024;
          char buffer[BUFFER_SIZE];
          snprintf(buffer, BUFFER_SIZE, "%.*s/ups_memtable.ref_%ld.ver_%ld.pid_%d.tim_%ld",
                  dump_dir.length(), dump_dir.ptr(),
                  ref_cnt_, version_, getpid(), tbsys::CTimeUtil::getTime());
          table_engine_.dump2text(buffer);
        };

        inline TEValueSessionCallback &get_tevalue_cb()
        {
          return tevalue_cb_;
        }

        inline TransSessionCallback &get_trans_cb()
        {
          return trans_cb_;
        }

        int check_checksum(const uint64_t checksum2check,
                           const uint64_t checksum_before_mutate,
                           const uint64_t checksum_after_mutate);

        inline uint64_t get_checksum() const
        {
          return checksum_;
        }

        inline void update_checksum(const uint64_t checksum)
        {
          checksum_ = checksum;
          TBSYS_LOG(DEBUG, "checksum update to %lu", checksum_);
        }

        inline uint64_t get_uncommited_checksum() const
        {
          return uncommited_checksum_;
        }

        inline void update_uncommited_checksum(const uint64_t checksum)
        {
          uncommited_checksum_ = checksum;
          TBSYS_LOG(DEBUG, "uncommited_checksum update to %lu", uncommited_checksum_);
        }

        inline uint64_t calc_uncommited_checksum(const uint64_t checksum)
        {
          return common::ob_crc64(uncommited_checksum_, &checksum, sizeof(checksum));
        }

        inline int64_t get_last_trans_id() const
        {
          return last_trans_id_;
        }

        inline void update_last_trans_id(const int64_t trans_id)
        {
          last_trans_id_ = trans_id;
          TBSYS_LOG(DEBUG, "last_trans_id update to %lu", last_trans_id_);
        }

        inline void add_row_counter(const int64_t row_counter)
        {
          row_counter_ += row_counter;
        }

        inline int64_t size() const
        {
          return row_counter_;
        };

        inline int64_t btree_size()
        {
          return table_engine_.btree_size();
        };

        inline int64_t btree_alloc_memory()
        {
          return table_engine_.btree_alloc_memory();
        };

        inline int64_t btree_reserved_memory()
        {
          return table_engine_.btree_reserved_memory();
        };

        inline void    btree_dump_mem_info()
        {
          table_engine_.btree_dump_mem_info();
        };

        inline int64_t hash_size() const
        {
          return table_engine_.hash_size();
        };

        inline int64_t hash_bucket_using() const
        {
          return table_engine_.hash_bucket_using();
        };

        inline int64_t hash_uninit_unit_num() const
        {
          return table_engine_.hash_uninit_unit_num();
        };

        int get_table_truncate_stat(uint64_t table_id, bool & is_truncated); /*add zhaoqiong [Truncate Table]:20160318*/

        int get_bloomfilter(common::TableBloomFilter &table_bf) const;

        int scan_all(TableEngineIterator &iter);

        int scan_all_table(TableEngineIterator &iter); /*add zhaoqiong [Truncate Table]:20160318*/

        int scan_one_table(TableEngineIterator &iter,int64_t table_id); //add zhaoqiong [Truncate Table]:20170519

      private:
        inline int copy_cells_(TransNode &tn,
                              TEValue &value,
                              ObUpsCompactCellWriter &ccw);
        inline int copy_cells_(TEValueUCInfo &uci,
                              ObUpsCompactCellWriter &ccw);
        inline int build_mtime_cell_(const int64_t mtime,
                                    const uint64_t table_id,
                                    ObUpsCompactCellWriter &ccw);
        inline int ob_sem_handler_(TransNode &tn,
                                  ObCellInfo &cell_info,
                                  TEKey &cur_key,
                                  TEValue *&cur_value,
                                  const TEKey &prev_key,
                                  TEValue *prev_value,
                                  bool is_row_changed,
                                  bool is_row_finished,
                                  ObUpsCompactCellWriter &ccw,
                                  int64_t &total_row_counter,
                                  int64_t &new_row_counter,
                                  common::ObBatchChecksum &bc);
        inline int ob_sem_handler_(RWSessionCtx &session,
                                  ILockInfo &lock_info,
                                  const ObCellInfo &cell_info,
                                  TEKey &cur_key, //del const lbzhong[Update rowkey] 20151221:b:e
                                  TEValue *&cur_value,
                                  const bool is_row_changed,
                                  const bool is_row_finished,
                                  ObUpsCompactCellWriter &ccw,
                                  int64_t &total_row_counter,
                                  int64_t &new_row_counter,
                                  common::ObBatchChecksum &bc
                                  //add lbzhong[Update rowkey] 20151221:b
                                  ,const bool is_create_new_row = false
                                  //add:e
                                  );
        inline int get_cur_value_(TransNode &tn,
                                  TEKey &cur_key,
                                  const TEKey &prev_key,
                                  TEValue *prev_value,
                                  TEValue *&cur_value,
                                  bool is_row_changed,
                                  int64_t &total_row_counter,
                                  int64_t &new_row_counter,
                                  common::ObBatchChecksum &bc);
        inline int get_cur_value_(RWSessionCtx &session,
                                  ILockInfo &lock_info,
                                  TEKey &cur_key, //del const lbzhong[Update rowkey] 20151221:b:e
                                  TEValue *&cur_value,
                                  TEValueUCInfo *&cur_uci,
                                  const bool is_row_changed,
                                  int64_t &total_row_counter,
                                  int64_t &new_row_counter,
                                  common::ObBatchChecksum &bc
                                  //add lbzhong[Update rowkey] 20151221:b
                                  , const ObRowkey *new_rowkey = NULL,
                                  const bool is_create_new_row = false
                                  //add:e
                                  );
        inline int update_value_(const TransNode &tn,
                                const uint64_t table_id,
                                ObCellInfo &cell_info,
                                TEValue &value,
                                ObUpsCompactCellWriter &ccw,
                                common::ObBatchChecksum &bc);
        inline int update_value_(RWSessionCtx &session,
                                const uint64_t table_id,
                                const ObCellInfo &cell_info,
                                TEValueUCInfo &uci,
                                ObUpsCompactCellWriter &ccw,
                                common::ObBatchChecksum &bc);
        inline int merge_(const TransNode &tn,
                          const TEKey &te_key,
                          TEValue &te_value);
        inline int merge_(RWSessionCtx &session,
                          const TEKey &te_key,
                          TEValue &te_value,
                          TEValue * table_value = NULL /*add zhaoqiong [Truncate Table]:20170519*/);

        inline static bool is_row_too_long_(const RWSessionCtx &session, const TEKey &te_key, const TEValue &te_value);
        inline static int16_t get_varchar_length_kb_(const common::ObObj &value)
        {
          int16_t ret = 0;
          if (ObVarcharType == value.get_type())
          {
            ObString vc;
            value.get_varchar(vc);
            int64_t length = vc.length();
            length = (length + CELL_INFO_SIZE_UNIT - 1) & ~(CELL_INFO_SIZE_UNIT- 1);
            length = length / CELL_INFO_SIZE_UNIT;
            ret = (int16_t)length;
          }
          //add fanqiushi DECIMAL OceanBase_BankCommV0.3 2014_7_19:b
          else if(ObDecimalType == value.get_type())
          {
            ObString vc;
            value.get_decimal(vc);
            int64_t length = vc.length();
            length = (length + CELL_INFO_SIZE_UNIT - 1) & ~(CELL_INFO_SIZE_UNIT- 1);
            length = length / CELL_INFO_SIZE_UNIT;
            ret = (int16_t)length;
          }
          //add:e
          return ret;
        };
        inline static bool is_row_not_exist_(const common::ObObj &value)
        {
          return (value.get_ext() == common::ObActionFlag::OP_ROW_DOES_NOT_EXIST);
        };
        inline static bool is_delete_row_(const common::ObObj &value)
        {
          return (value.get_ext() == common::ObActionFlag::OP_DEL_ROW);
        };
        //add zhaoqiong [Truncate Table]:20160318:b
        inline static bool is_trun_tab_(const common::ObObj &value)
        {
          return (value.get_ext() == common::ObActionFlag::OP_TRUN_TAB);
        };
        //add:e
        inline static bool is_nop_(const common::ObObj &value)
        {
          return (value.get_ext() == common::ObActionFlag::OP_NOP);
        };
        inline static bool is_insert_(const int64_t op_type)
        {
          return (op_type == common::ObActionFlag::OP_INSERT);
        };
        inline static bool is_update_(const int64_t op_type)
        {
          return (op_type == common::ObActionFlag::OP_UPDATE);
        };

        inline static const common::ObRowkey &get_start_key(const common::ObNewRange &range)
        {
          return range.start_key_;
        };
        inline static const common::ObRowkey &get_end_key(const common::ObNewRange &range)
        {
          return range.end_key_;
        };
        inline static uint64_t get_table_id(const common::ObNewRange &range)
        {
          return range.table_id_;
        };
        inline static int get_start_exclude(const common::ObNewRange &range)
        {
          return range.border_flag_.inclusive_start() ? 0 : 1;
        };
        inline static int get_end_exclude(const common::ObNewRange &range)
        {
          return range.border_flag_.inclusive_end() ? 0 : 1;
        };
        inline static int get_min_key(const common::ObNewRange &range)
        {
          return range.start_key_.is_min_row() ? 1 : 0;
        };
        inline static int get_max_key(const common::ObNewRange &range)
        {
          return range.end_key_.is_max_row() ? 1 : 0;
        };

        void handle_checksum_error(ObUpsMutator &mutator);
        //add lbzhong [Update rowkey] 20160510:b
        int update_rowkey(RWSessionCtx &session,
                                    ILockInfo &lock_info,
                                    TEKey &cur_key,
                                    TEValue *&cur_value,
                                    bool is_row_changed,
                                    int64_t &new_row_counter,
                                    const ObRowkey *new_rowkey,
                                    ObBatchChecksum &bc);
        int delete_old_row(RWSessionCtx &session,
                                     ILockInfo &lock_info,
                                     TEKey &old_key,
                                     bool is_row_changed,
                                     ObBatchChecksum &bc);
        //add:e
      private:
        bool inited_;
        MemTank mem_tank_;
        TableEngine table_engine_;
        common::TableBloomFilter table_bf_;

        int64_t version_;
        int64_t ref_cnt_;

        int64_t checksum_before_mutate_;
        int64_t checksum_after_mutate_;
        uint64_t checksum_;
        uint64_t uncommited_checksum_;
        int64_t last_trans_id_;

        common::ObObj MIN_OBJ;
        common::ObObj MAX_OBJ;
        TransMgr trans_mgr_;
        int64_t row_counter_;

        TEValueSessionCallback tevalue_cb_;
        TEValueStmtCallback tevalue_stmt_cb_;
        TransSessionCallback trans_cb_;
    };
  }
}

#endif //OCEANBASE_UPDATESERVER_MEMTABLE_H_

