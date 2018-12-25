/**
 * fanqiushi_index
 * Version: $Id$
 *
 * ob_index_trigger.h
 *
 * Authors:
 *   fanqiushi <fanqiushiecnu@gmail.com>
 *
 */
#ifndef OB_INDEX_TRIGGER_H
#define OB_INDEX_TRIGGER_H 1

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
    class ObIndexTrigger : public ObSingleChildPhyOperator
    {
      public:
        ObIndexTrigger();
        ~ObIndexTrigger();
        int open();
        int close();
        virtual void reset();
        virtual void reuse();
        //add wenghaixing [secondary index static_index_build.consistency]20150424
        inline virtual int64_t get_index_num()const{return index_num_;}
        //add e
        int get_next_row(const ObRow *&row);
        int get_row_desc(const ObRowDesc *&row_desc) const;
        int64_t to_string(char* buf, const int64_t buf_len) const;
        int set_i64_values(int64_t m_id,int64_t index_num,int64_t cur_index);

        int handle_trigger(const ObRowDesc*row_dec, const ObSchemaManagerV2 *schema_mgr,
                           const int64_t rk_size, updateserver::ObIUpsTableMgr* host,
                           updateserver::RWSessionCtx &session_ctx, const ObDmlType dml_type, ObRowStore *store);
        int handle_one_index_table(uint64_t index_tid, const ObRowDesc*row_dec,
                                   const ObSchemaManagerV2 *schema_mgr, const int64_t rk_size,
                                   updateserver::ObIUpsTableMgr* host, updateserver::RWSessionCtx &session_ctx,
                                   const ObDmlType dml_type , ObRowStore *store);
        int cons_index_row_desc(uint64_t index_tid,const ObSchemaManagerV2 *schema_mgr,ObRowDesc &row_desc);
        bool is_cid_in_index_table(uint64_t tid,uint64_t cid,ObRowDesc row_dec);

        enum ObPhyOperatorType get_type() const{return PHY_INDEX_TRIGGER;}
        typedef ObArray<ObRowDesc > ObRowDescArray;
        DECLARE_PHY_OPERATOR_ASSIGN;
        NEED_SERIALIZE_AND_DESERIALIZE;
      private:
        ObRowDesc cur_row_desc_;
        ObRowDesc index_row_desc_[OB_MAX_INDEX_NUMS];
       // ObRow cur_row_;
        int64_t main_table_id_;
        int64_t index_num_;
        //ObRowDescArray RowDescArray_;
        int64_t cur_index_;

    };
  }
}

#endif // OB_INDEX_TRIGGER_H
