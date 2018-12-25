/**
 * ob_multi_bind.h
 *
 * Authors:
 *   tianz
 * used to get sub_query's result and direct bind result to main query 
 * or build hashmap and bloomfilter, and check filter conditions
 */
#ifndef _OB_MULTI_BIND_H_
#define _OB_MULTI_BIND_H_

#include "sql/ob_phy_operator.h"
#include "sql/ob_multi_children_phy_operator.h"
#include "common/ob_row.h"
#include "common/ob_row_store.h"
#include "common/hash/ob_hashmap.h"
#include "common/ob_rowkey.h"
#include "common/ob_array.h"
#include "common/ob_custom_allocator.h"
#include "ob_filter.h"



namespace oceanbase
{
  namespace sql
  {
    class ObMultiBind: public ObMultiChildrenPhyOperator
    {
      public:
        ObMultiBind();
        ~ObMultiBind();
        virtual int open();
        virtual int close();
        virtual void reset();
        virtual void reuse();

        /**
         * 获得下一行的引用
         * @note 在下次调用get_next或者close前，返回的row有效
         * @pre 调用open()
         * @param row [out]
         *
         * @return OB_SUCCESS或OB_ITER_END或错误码
         */
        int get_next_row(const common::ObRow *&row);
        int get_row_desc(const common::ObRowDesc *&row_desc) const;
        int64_t to_string(char* buf, const int64_t buf_len) const;
        int build_hash_map();
        //add zhaoqiong [TableMemScan_Subquery_BUGFIX] 20151118:b
        int build_mem_hash_map();
        //end:e
        //add peiouya [IN_Subquery_prepare_BUGFIX] 20141013:b
        enum ObPhyOperatorType get_type() const{return PHY_MULTI_BIND;}
        DECLARE_PHY_OPERATOR_ASSIGN;
        //add 20141013:e
      private:
        DISALLOW_COPY_AND_ASSIGN(ObMultiBind);
    
      private:
        static const int MAX_SUB_QUERY_NUM = 5;//restrict num of sub_query which use HASHMAP
        static const int MAX_USE_BLOOMFILTER_NUM = 2;//restrict num of sub_query which use BLOOMFILTER
        static const int64_t HASH_BUCKET_NUM = 100000;
        static const int64_t BLOOMFILTER_ELEMENT_NUM = 1000000;
        static const int BIG_RESULTSET_THRESHOLD = 1000; //大数据集阈值
        common::ObRow curr_row_;
        ObFilter sub_query_filter_ ;
        int hashmap_num_ ;
        common::CustomAllocator arena_;
        common::ObArray<common::ObRowkey>  sub_result_;
        common::hash::ObHashMap<common::ObRowkey,common::ObRowkey,common::hash::NoPthreadDefendMode> sub_query_map_[MAX_SUB_QUERY_NUM];
        common::ObArray<ObObjType> sub_query_map_and_bloomfilter_column_type[MAX_SUB_QUERY_NUM];  //add peiouya [IN_TYPEBUG_FIX] 20151225
        bool is_subquery_result_contain_null[MAX_SUB_QUERY_NUM];  //add peiouya [IN_AND NOT_IN_WITH_NULL_BUG_FIX] 20160518
    };
  } // end namespace sql
} // end namespace oceanbase

#endif /* _OB_MULTI_BIND_H_*/

