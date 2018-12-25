/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 *
 * Version: 0.1: ob_mutator.h,v 0.1 2010/09/15 11:10:37 chuanhui Exp $
 *
 * Authors:
 *   chuanhui <rizhao.ych@taobao.com>
 *     - some work details if you want
 *
 */

#ifndef __OCEANBASE_CHUNKSERVER_OB_MUTATOR_H__
#define __OCEANBASE_CHUNKSERVER_OB_MUTATOR_H__

#include "ob_read_common_data.h"
#include "ob_string_buf.h"
#include "ob_action_flag.h"
#include "page_arena.h"
#include "ob_schema.h"
#include "ob_trace_log.h"

namespace oceanbase
{
  namespace tests
  {
    namespace common
    {
      class TestMutatorHelper;
    }
  }
}

namespace oceanbase
{
  namespace common
  {
    struct ObMutatorCellInfo
    {
      // all ext type
      ObObj op_type;
      ObCellInfo cell_info;

      bool is_return(void) const
      {
        return (op_type.get_ext() & ObActionFlag::OP_RETURN_UPDATE_RESULT);
      }
    };

    // ObMutator represents a list of cell mutation.
    // 使用mutator的请注意 从mutator中迭代数据 必须保证经过了反序列化才能正常获取is_row_changed和is_row_finished标记 否则会返回not support
    class ObMutator
    {
      friend class oceanbase::tests::common::TestMutatorHelper;
      static const int64_t ALLOCATOR_PAGE_SIZE = 2L * 1024L * 1024L;
      public:
        enum
        {
          RETURN_NO_RESULT = 0,
          RETURN_UPDATE_RESULT = 1,
        };

        enum MUTATOR_TYPE
        {
          NORMAL_UPDATE = 10,    // normal update type
          CORRECTION_UPDATE = 11,// correction data type
        };

        enum RowChangedStat
        {
          CHANGED_UNKNOW = 0,
          NOCHANGED = 1,
          CHANGED = 2,
        };

        enum RowFinishedStat
        {
          FINISHED_UNKNOW = 0,
          NOFINISHED = 1,
          FINISHED = 2,
        };

        enum BarrierFlag // Bit Flag
        {
          NO_BARRIER = 0,
          ROWKEY_BARRIER = 1,
          ROW_DML_TYPE = 0x2,
        };

      public:
        ObMutator();
        ObMutator(ModuleArena &arena);
        virtual ~ObMutator();
        int clear();
        int reset();
        int reuse() { return clear(); }
        void mark()
        {
          mark_list_head_         = list_head_;       
          mark_list_tail_         = list_tail_;
          mark_cell_count_        = cell_count_;
          mark_first_table_name_  = first_table_name_;
          mark_last_row_key_      = last_row_key_;
          mark_last_table_name_   = last_table_name_;
          mark_last_table_id_     = last_table_id_;
          mark_cell_store_size_   = cell_store_size_;
        };
        void rollback()
        {
          list_head_         = mark_list_head_;       
          list_tail_         = mark_list_tail_;
          if (NULL != list_tail_)
          {
            list_tail_->next = NULL;
          }
          cell_count_        = mark_cell_count_;
          first_table_name_  = mark_first_table_name_;
          last_row_key_      = mark_last_row_key_;
          last_table_name_   = mark_last_table_name_;
          last_table_id_     = mark_last_table_id_;
          cell_store_size_   = mark_cell_store_size_;
          TBSYS_TRACE_LOG("rollback list_head=%p list_tail=%p cell_count=%ld cell_store_size=%ld", list_head_, list_tail_, cell_count_, cell_store_size_);
        };
      public:
        // Uses ob&db semantic, ob semantic is used by default.
        int use_ob_sem();
        int use_db_sem();

        // check mutator type:normal or data correction
        void set_mutator_type(const MUTATOR_TYPE type);
        ObMutator::MUTATOR_TYPE get_mutator_type(void) const;

        // Adds update mutation to list
        int update(const ObString& table_name, const ObRowkey& row_key,
            const ObString& column_name, const ObObj& value, const int return_flag = RETURN_NO_RESULT);
        int update(const uint64_t table_id, const ObRowkey& row_key,
            const uint64_t column_id, const ObObj& value, const int return_flag = RETURN_NO_RESULT);
        // Adds insert mutation to list
        int insert(const ObString& table_name, const ObRowkey& row_key,
            const ObString& column_name, const ObObj& value, const int return_flag = RETURN_NO_RESULT);
        int insert(const uint64_t table_id, const ObRowkey& row_key,
            const uint64_t column_id, const ObObj& value, const int return_flag = RETURN_NO_RESULT);
        // Adds add mutation to list
        int add(const ObString& table_name, const ObRowkey& row_key,
            const ObString& column_name, const int64_t value, const int return_flag = RETURN_NO_RESULT);
        int add(const ObString& table_name, const ObRowkey& row_key,
            const ObString& column_name, const float value, const int return_flag = RETURN_NO_RESULT);
        int add(const ObString& table_name, const ObRowkey& row_key,
            const ObString& column_name, const double value, const int return_flag = RETURN_NO_RESULT);
        int add_datetime(const ObString& table_name, const ObRowkey& row_key,
            const ObString& column_name, const ObDateTime& value, const int return_flag = RETURN_NO_RESULT);
        int add_precise_datetime(const ObString& table_name, const ObRowkey& row_key,
            const ObString& column_name, const ObPreciseDateTime& value, const int return_flag = RETURN_NO_RESULT);
        // Adds del_row mutation to list
        int del_row(const ObString& table_name, const ObRowkey& row_key);
        int del_row(const uint64_t table_id, const ObRowkey& row_key);

        //add zhaoqiong [Truncate Table]:20160318:b
        int trun_tab(const uint64_t table_id, const common::ObString & table_name, const common::ObString & user_name, const common::ObString & comment);
        int ups_trun_tab(const uint64_t table_id, const bool force_finish = false);
        //add:e
        int add_row_barrier();
        int set_dml_type(const ObDmlType dml_type);
        int add_cell(const ObMutatorCellInfo& cell);
        const ObString & get_first_table_name(void) const;
        int64_t size(void) const;
      public:
        //inline void set_compatible_rowkey_info(const ObRowkeyInfo* rowkey_info) {rowkey_info_ = rowkey_info;};
        inline void set_compatible_schema(const ObSchemaManagerV2* sm) {schema_manager_ = sm;}

      public:
      public:
        virtual void reset_iter();
        virtual int next_cell();
        virtual int get_cell(ObMutatorCellInfo** cell);
        virtual int get_cell(ObMutatorCellInfo** cell, bool* is_row_changed, bool* is_row_finished, ObDmlType *dml_type = NULL);

      public:
        int pre_serialize();
        int serialize(char* buf, const int64_t buf_len, int64_t& pos) const;
        int deserialize(const char* buf, const int64_t buf_len, int64_t& pos);
        int64_t get_serialize_size(void) const;

      private:
        int next_cell_();
        int add_cell(const ObMutatorCellInfo& cell,
                    const RowChangedStat row_changed_stat,
                    const BarrierFlag barrier_flag,
                    const ObDmlType dml_type);


      private:
        struct CellInfoNode
        {
          ObMutatorCellInfo cell;
          RowChangedStat row_changed_stat;
          RowFinishedStat row_finished_stat;
          int32_t barrier_flag;
          ObDmlType dml_type;
          CellInfoNode* next;
        };

        enum IdNameType
        {
          UNSURE = 0,
          USE_ID = 1,
          USE_NAME = 2,
        };

      private:
        int copy_cell_(const ObMutatorCellInfo& src_cell, ObMutatorCellInfo& dst_cell,
                      RowChangedStat row_changed_stat, int64_t& store_size);
        int add_node_(CellInfoNode* cur_node);

      private:
        int serialize_flag_(char* buf, const int64_t buf_len, int64_t& pos, const int64_t flag) const;
        int64_t get_obj_serialize_size_(const int64_t value, bool is_ext) const;
        int64_t get_obj_serialize_size_(const ObString& str) const;
        int64_t get_obj_serialize_size_(const ObRowkey& rowkey) const;

      private:
        CellInfoNode* list_head_;
        CellInfoNode* list_tail_;
        ModulePageAllocator mod_;
        ModuleArena local_page_arena_;
        ObStringBuf str_buf_;
        ModuleArena &page_arena_;
        int64_t cell_count_;
        ObString first_table_name_;
        ObRowkey last_row_key_;
        ObString last_table_name_;
        uint64_t last_table_id_;
        IdNameType id_name_type_;
        int64_t  cell_store_size_;
        CellInfoNode* cur_iter_node_;
        MUTATOR_TYPE type_;
        bool has_begin_;
        ObRowkeyInfo rowkey_info_;
        const ObSchemaManagerV2* schema_manager_;
        char* mutator_buf_;

        CellInfoNode *mark_list_head_;
        CellInfoNode *mark_list_tail_;
        int64_t       mark_cell_count_;
        ObString      mark_first_table_name_;
        ObRowkey      mark_last_row_key_;
        ObString      mark_last_table_name_;
        uint64_t      mark_last_table_id_;
        int64_t       mark_cell_store_size_;
    };
  }
}

#endif //__OB_MUTATOR_H__

