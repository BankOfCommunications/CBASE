/**
 * liumengzhan_delete_index
 * Version: $Id$
 *
 * ob_delete_index.h
 *
 * Authors:
 *   liumengzhan<liumengzhan@bankcomm.com>
 *
 */
#ifndef OB_DELETE_INDEX_H
#define OB_DELETE_INDEX_H 1

#include "ob_single_child_phy_operator.h"
#include "sql/ob_ups_modify.h"
#include "common/ob_iterator.h"
#include "common/ob_iterator_adaptor.h"
#include "updateserver/ob_sessionctx_factory.h"
#include "updateserver/ob_ups_table_mgr.h"
#include "updateserver/ob_ups_utils.h"
namespace oceanbase
{
  using namespace common;
  namespace sql
  {
    class ObDeleteIndex : public ObSingleChildPhyOperator
    {
    public:
      ObDeleteIndex();
      ~ObDeleteIndex();
      virtual int open();
      virtual int close();
      virtual void reset();
      virtual void reuse();
      virtual int get_next_row(const ObRow *&row);
      virtual int get_row_desc(const ObRowDesc *&row_desc) const;
      //add wenghaixing [secondary index static_index_build.consistency]20150424
      inline virtual int64_t get_index_num()const{return index_num_;}
      //add e
      int get_index_row_desc(int64_t index, const ObRowDesc *&row_desc) const;
      //replace liumz, [UT_bugfix] 20150420:b
      //int get_index_tid(int64_t index, uint64_t table_id) const;
      int get_index_tid(int64_t index, uint64_t &table_id) const;
      //replace:e

      void set_main_tid(uint64_t table_id);
      uint64_t get_main_tid() const;
      int set_index_num(int64_t index_num);
      //int64_t get_index_num() const;
      int set_input_values(uint64_t table_id, int64_t index_num);
      void set_row_desc(const ObRowDesc &row_desc);
      const ObRowDesc& get_row_desc() const;
      int add_index_tid(int64_t index, const uint64_t index_tid);
      int add_index_row_desc(int64_t index, const ObRowDesc &row_desc);

      virtual int64_t to_string(char* buf, const int64_t buf_len) const;

      void reset_iterator();
      int handle_trigger(const ObSchemaManagerV2 *schema_mgr, updateserver::ObIUpsTableMgr* host,
                         updateserver::RWSessionCtx &session_ctx, const ObDmlType dml_type
                         , ObRowStore *store);
      int handle_one_index_table(const ObRowDesc &row_dec, const ObSchemaManagerV2 *schema_mgr,
                                 updateserver::ObIUpsTableMgr* host, updateserver::RWSessionCtx &session_ctx,
                                 const ObDmlType dml_type, ObRowStore *store);
      //        int cons_index_row_desc(uint64_t index_tid,const ObSchemaManagerV2 *schema_mgr,ObRowDesc &row_desc);
      //        bool is_cid_in_index_table(uint64_t tid,uint64_t cid,ObRowDesc row_dec);

      enum ObPhyOperatorType get_type() const{return PHY_DELETE_INDEX;}
      typedef ObArray<ObRowDesc > ObRowDescArray;
      DECLARE_PHY_OPERATOR_ASSIGN;
      NEED_SERIALIZE_AND_DESERIALIZE;
    private:
      uint64_t main_table_id_;
      int64_t index_num_;
      uint64_t index_table_id_[OB_MAX_INDEX_NUMS];
      common::ObRowDesc row_desc_;
      common::ObRowDesc index_row_desc_[OB_MAX_INDEX_NUMS];
      ObRow cur_row_;
    };
  }
}

#endif // OB_DELETE_INDEX_H
