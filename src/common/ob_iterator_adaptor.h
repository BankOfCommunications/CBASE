////===================================================================
 //
 // ob_iterator_adaptor.h common / Oceanbase
 //
 // Copyright (C) 2010 Taobao.com, Inc.
 //
 // Created on 2012-11-21 by Yubai (yubai.lk@taobao.com)
 //
 // -------------------------------------------------------------------
 //
 // Description
 //
 // -------------------------------------------------------------------
 //
 // Change Log
 //
////====================================================================
#ifndef  OCEANBASE_COMMON_ITERATOR_ADAPTOR_H_
#define  OCEANBASE_COMMON_ITERATOR_ADAPTOR_H_
#include "common/ob_iterator.h"
#include "common/ob_row.h"
#include "common/ob_schema.h"
#include "common/ob_ups_row.h"
#include "sql/ob_rowkey_phy_operator.h"
//add fanqiushi_index
#include "ob_row_store.h"
//add:e
namespace oceanbase
{
  namespace common
  {
    class ObObjCastHelper
    {
      public:
        ObObjCastHelper();
        ~ObObjCastHelper();
      public:
        void set_need_cast(const bool need_cast);
        //add wenghaixing [secondary index static_index_build.consistency]20150424
        //int reset(const ObRowDesc &row_desc, const ObSchemaManagerV2 &schema_mgr);
        int reset(const ObRowDesc &row_desc, const ObSchemaManagerV2 &schema_mgr,int64_t index_num = 0);
        //add e
      public:
        int cast_cell(const int64_t idx, ObObj &cell) const;
        int cast_rowkey_cell(const int64_t idx, ObObj &cell) const;
        //add fanqiushi DECIMAL OceanBase_BankCommV0.3 2014_7_19:b
        /*
         *声明自己写的两个函数
         */
        uint32_t get_precision(const int64_t idx);
        uint32_t get_scale(const int64_t idx);
        //add:e
      private:
        static ObString get_tsi_buffer_();
        static ObString get_tsi_buffer_(const int64_t idx);
      private:
        bool need_cast_;
        ObObjMeta col_types_[OB_ROW_MAX_COLUMNS_COUNT];
        int64_t col_num_;
    };

    class ObRowkeyCastHelper
    {
      public:
        static int cast_rowkey(const ObRowkeyInfo &rki, ObRowkey &rowkey);
      private:
        static ObString get_tsi_buffer_(const int64_t idx);
    };

    class ObCellAdaptor : public ObIterator
    {
      public:
        ObCellAdaptor();
        ~ObCellAdaptor();
      public:
        int next_cell();
        int get_cell(ObCellInfo** cell);
        int get_cell(ObCellInfo** cell, bool* is_row_changed);
        int is_row_finished(bool* is_row_finished);
      public:
        void set_row(const ObRow *row, const int64_t rk_size);
        int cast_rowkey_(const ObRow &row, const int64_t rk_size);
        void reset();
        ObObjCastHelper &get_och();
      //add lbzhong [Update rowkey] 20160509:b
      private:
        int get_new_rowkey(const ObRow *&row, ObRowkey *&new_rowkey, const int64_t rk_size);
      //add:e
      private:
        const ObRow *row_;
        int64_t rk_size_;
        int64_t cur_idx_;
        bool is_iter_end_;
        ObCellInfo cell_;
        bool need_nop_cell_;
        ObObjCastHelper och_;
    };

    class ObCellIterAdaptor : public ObIterator
    {
      public:
        ObCellIterAdaptor();
        ~ObCellIterAdaptor();
      public:
        int next_cell();
        int get_cell(ObCellInfo** cell);
        int get_cell(ObCellInfo** cell, bool* is_row_changed);
        int is_row_finished(bool* is_row_finished);
      public:
        //modify wenghaixing [secondary index static_index_build.consistency]20150424
        //void set_row_iter(sql::ObPhyOperator *row_iter, const int64_t rk_size, const ObSchemaManagerV2 *schema_mgr);
        void set_row_iter(sql::ObPhyOperator *row_iter, const int64_t rk_size, const ObSchemaManagerV2 *schema_mgr, int64_t index_num = 0);
        //modify e
        void reset();
      private:
        sql::ObPhyOperator *row_iter_;
        int64_t rk_size_;
        ObCellAdaptor single_row_iter_;
        bool is_iter_end_;
        int set_row_iter_ret_;
    };
    //add fanqiushi_index
    class ObIndexCellIterAdaptor : public ObIterator
    {
      public:
        ObIndexCellIterAdaptor();
        ~ObIndexCellIterAdaptor();
      public:
        int next_cell();
        int get_cell(ObCellInfo** cell);
        int get_cell(ObCellInfo** cell, bool* is_row_changed);
        int is_row_finished(bool* is_row_finished);
      public:
        void set_row_iter(ObRowStore *row_iter, const int64_t rk_size, const ObSchemaManagerV2 *schema_mgr,ObRowDesc row_desc, int64_t index_num = 0);
        void reset();
      private:
        ObRowStore *row_iter_;
        ObRowDesc row_desc_;
        ObRow index_row_tmp_;
        int64_t rk_size_;
        ObCellAdaptor single_row_iter_;
        bool is_iter_end_;
        int set_row_iter_ret_;
    };
    //add:e
    class ObRowIterAdaptor : public sql::ObRowkeyPhyOperator
    {
      static const int64_t ALLOCATOR_PAGE_SIZE = 65536;
      public:
        ObRowIterAdaptor();
        ObRowIterAdaptor(bool is_ups_row);
        ~ObRowIterAdaptor();
      public:
        int set_child(int32_t child_idx, ObPhyOperator &child_operator);
      public:
        int open();
        int close();
        virtual sql::ObPhyOperatorType get_type() const { return sql::PHY_ROW_ITER_ADAPTOR; }

        int get_next_row(const ObRow *&row);
        int get_next_row(const ObRowkey *&rowkey, const ObRow *&row);
        int get_row_desc(const ObRowDesc *&row_desc) const;
        int64_t to_string(char* buf, const int64_t buf_len) const;
      public:
        void set_cell_iter(ObIterator *cell_iter, const ObRowDesc &row_desc, const bool deep_copy);
        virtual void reset();
        virtual void reuse();
      private:
        ModulePageAllocator mod_;
        ModuleArena allocator_;
        ObIterator *cell_iter_;
        ObRow cur_row_;
        bool deep_copy_;
        bool is_ups_row_;
    };
  }
}

#endif //OCEANBASE_COMMON_ITERATOR_ADAPTOR_H_

