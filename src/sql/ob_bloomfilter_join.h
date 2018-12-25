/**
 * ob_bloomfilter_bind.h
 *
 * Authors:
 *   steven.h.d
 * used to get hash_join's and nested_loop_join's result and direct bind result to main query
 * or build hashmap and bloomfilter, and check filter conditions
 */

#ifndef OB_BLOOMFILTER_JOIN_H
#define OB_BLOOMFILTER_JOIN_H

#include "ob_join.h"
#include "sql/ob_phy_operator.h"
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
    class ObBloomFilterJoin: public ObJoin
    {
      public:
        ObBloomFilterJoin();
        ~ObBloomFilterJoin();
        virtual int open();
        virtual int close();
        virtual void reset();
        virtual void reuse();
        virtual int set_join_type(const ObJoin::JoinType join_type);
        virtual ObPhyOperatorType get_type() const{return PHY_BLOOMFILTER_JOIN;}
        DECLARE_PHY_OPERATOR_ASSIGN;
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
      private:
        int inner_hash_get_next_row(const common::ObRow *&row);
        int left_hash_outer_get_next_row(const common::ObRow *&row);
        int get_next_right_row(const ObRow *&row);
        int normal_get_next_row(const common::ObRow *&row);
        int inner_get_next_row(const common::ObRow *&row);
        int full_outer_get_next_row(const common::ObRow *&row);
        int right_outer_get_next_row(const common::ObRow *&row);
        int left_outer_get_next_row(const common::ObRow *&row);
        int compare_equijoin_cond(const ObRow& r1, const ObRow& r2, int &cmp) const;
        int left_row_compare_equijoin_cond(const ObRow& r1, const ObRow& r2, int &cmp) const;
        int right_row_compare_equijoin_cond(const ObRow& r1, const ObRow& r2, int &cmp) const;
        int compare_hash_equijoin(const ObRow *&r1, const ObRow& r2, int &cmp,bool left_hash_id_cache_valid,int &last_hash_id_);
        int curr_row_is_qualified(bool &is_qualified);
        int cons_row_desc(const ObRowDesc &rd1, const ObRowDesc &rd2);
        int join_rows(const ObRow& r1, const ObRow& r2);
        int left_join_rows(const ObRow& r1);
        int left_rows(const common::ObRow *&row,int &last_left_hash_id_);
        int right_join_rows(const ObRow& r2);
        DISALLOW_COPY_AND_ASSIGN(ObBloomFilterJoin);

      private:
        static const int64_t MAX_SINGLE_ROW_SIZE = common::OB_ROW_MAX_COLUMNS_COUNT*(common::OB_MAX_VARCHAR_LENGTH+4);
        typedef int (ObBloomFilterJoin::*get_next_row_func_type)(const common::ObRow *&row);
        get_next_row_func_type get_next_row_func_;
        const common::ObRow *last_left_row_;
        const common::ObRow *last_right_row_;
        ObSqlExpression *table_filter_expr_;
        common::ObRow last_join_left_row_;
        common::ObString last_join_left_row_store_;
        common::ObRowStore right_cache_;
        common::ObRow curr_cached_right_row_;
        common::ObRowDesc row_desc_;
        bool right_cache_is_valid_;
        bool is_right_iter_end_;
        int process_sub_query();
        bool last_left_row_has_printed_;
        bool last_right_row_has_printed_;
        bool left_cache_is_valid_;
        bool left_hash_id_cache_valid_;
        common::ObRowStore left_cache_;
        common::ObRowStore left_cache_for_right_join_;
        common::ObRow last_join_right_row_;
        common::ObString last_join_right_row_store_;
        common::ObRow curr_cached_left_row_;
        bool is_left_iter_end_;
        bool use_bloomfilter_;
        int last_hash_id_;
        int last_left_hash_id_;
        ObFilter sub_query_filter_ ;


        static const int MAX_SUB_QUERY_NUM = 1;//num of HASHMAP
        static const int MAX_USE_BLOOMFILTER_NUM = 1;//num of BLOOMFILTER
        static const int64_t HASH_BUCKET_NUM = 100000;
        static const int64_t BLOOMFILTER_ELEMENT_NUM = 1000000;
        common::ObRow curr_row_;
        int hashmap_num_ ;
        int cur_hashmap_num_ ;
        common::CustomAllocator arena_;
        common::hash::ObHashMap<common::ObRowkey,common::ObRowkey,common::hash::NoPthreadDefendMode> bloomfilter_map_[MAX_SUB_QUERY_NUM];
        struct hash_row_store
        {
          common::ObRowStore row_store;
          int32_t id_no_;
          ObArray<int8_t> hash_iterators;
          hash_row_store():id_no_(0) {}
        }hash_table_row_store[HASH_BUCKET_NUM];
        //add by steven.h.d  2015.6.4
        int64_t con_length_;
        int numi;
    };
  } // end namespace sql
} // end namespace oceanbase

#endif // OB_BLOOMFILTER_JOIN_H
