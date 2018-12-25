#ifndef OCEANBASE_SQL_SELECTSTMT_H_
#define OCEANBASE_SQL_SELECTSTMT_H_

#include "ob_stmt.h"
#include "ob_raw_expr.h"
#include "common/ob_string.h"
#include "common/ob_string_buf.h"
#include "common/ob_array.h"
#include "common/ob_vector.h"
#include "ob_sequence_stmt.h"//add lijianqiang [sequence select] 20150623

namespace oceanbase
{
  namespace sql
  {
    struct SelectItem
    {
      uint64_t   expr_id_;
      bool       is_real_alias_;
      common::ObString alias_name_;
      common::ObString expr_name_;
      common::ObObjType type_;
    };

    struct OrderItem
    {
      enum OrderType
      {
        ASC,
        DESC
      };

      uint64_t   expr_id_;
      OrderType  order_type_;
    };

    struct JoinedTable
    {
      enum JoinType
      {
        T_FULL,
        T_LEFT,
        T_RIGHT,
        T_INNER,
          //add by wl
          T_SEMI,
          T_SEMI_LEFT,
          T_SEMI_RIGHT,
      };

      int add_table_id(uint64_t tid) { return table_ids_.push_back(tid); }
      int add_join_type(JoinType type) { return join_types_.push_back(type); }
      int add_expr_id(uint64_t eid) { return expr_ids_.push_back(eid); }
      int add_join_exprs(ObVector<uint64_t>& exprs);
      void set_joined_tid(uint64_t tid) { joined_table_id_ = tid; }

      uint64_t   joined_table_id_;
      common::ObArray<uint64_t>  table_ids_;
      common::ObArray<uint64_t>  join_types_;
      common::ObArray<uint64_t>  expr_ids_;
      common::ObArray<int64_t>   expr_nums_per_join_;
    };

    struct FromItem
    {
      uint64_t   table_id_;
      // false: it is the real table id
      // true: it is the joined table id
      bool      is_joined_;
    };
  }

  namespace common
  {
    template <>
      struct ob_vector_traits<oceanbase::sql::SelectItem>
      {
        typedef oceanbase::sql::SelectItem* pointee_type;
        typedef oceanbase::sql::SelectItem value_type;
        typedef const oceanbase::sql::SelectItem const_value_type;
        typedef value_type* iterator;
        typedef const value_type* const_iterator;
        typedef int32_t difference_type;
      };

    template <>
      struct ob_vector_traits<oceanbase::sql::OrderItem>
      {
        typedef oceanbase::sql::OrderItem* pointee_type;
        typedef oceanbase::sql::OrderItem value_type;
        typedef const oceanbase::sql::OrderItem const_value_type;
        typedef value_type* iterator;
        typedef const value_type* const_iterator;
        typedef int32_t difference_type;
      };

    template <>
      struct ob_vector_traits<oceanbase::sql::FromItem>
      {
        typedef oceanbase::sql::FromItem* pointee_type;
        typedef oceanbase::sql::FromItem value_type;
        typedef const oceanbase::sql::FromItem const_value_type;
        typedef value_type* iterator;
        typedef const value_type* const_iterator;
        typedef int32_t difference_type;
      };
  }

  namespace sql
  {
    class ObSelectStmt : public ObStmt
        //add lijianqiang [sequence select] 20150623:b
        /*Exp:将ObSequenceStmt独立出来，让使用它的继承*/
        ,public ObSequenceStmt
        //add 20150623:e
    {
    public:
      enum SetOperator
      {
        UNION,
        INTERSECT,
        EXCEPT,
        NONE,
      };

      ObSelectStmt(common::ObStringBuf* name_pool);
      virtual ~ObSelectStmt();

      int32_t get_select_item_size() const { return select_items_.size(); }
      int32_t get_from_item_size() const { return from_items_.size(); }
      int32_t get_joined_table_size() const { return joined_tables_.size(); }
      int32_t get_group_expr_size() const { return group_expr_ids_.size(); }
      int32_t get_agg_fun_size() const { return agg_func_ids_.size(); }
      int32_t get_having_expr_size() const { return having_expr_ids_.size(); }
      int32_t get_order_item_size() const { return order_items_.size(); }
      //add liumz, [ROW_NUMBER]20150824
      int32_t get_partition_expr_size() const { return partition_expr_ids_.size(); }
      int32_t get_order_item_for_rownum_size() const { return order_items_for_rownum_.size(); }
      int32_t get_anal_fun_size() const { return anal_func_ids_.size(); }
      //add:e
      void assign_distinct() { is_distinct_ = true; }
      void assign_all() { is_distinct_ = false; }
      void assign_set_op(SetOperator op) { set_op_ = op; }
      void assign_set_distinct() { is_set_distinct_ = true; }
      void assign_set_all() { is_set_distinct_ = false; }
      void assign_left_query_id(uint64_t lid) { left_query_id_ = lid; }
      void assign_right_query_id(uint64_t rid) { right_query_id_ = rid; }
      int check_alias_name(ResultPlan& result_plan, const common::ObString& sAlias) const;
      uint64_t get_alias_expr_id(common::ObString& alias_name);
      uint64_t generate_joined_tid() { return gen_joined_tid_--; }
      uint64_t get_left_query_id() { return left_query_id_; }
      uint64_t get_right_query_id() { return right_query_id_; }
      uint64_t get_limit_expr_id() const { return limit_count_id_; }
      uint64_t get_offset_expr_id() const { return limit_offset_id_; }
      bool is_distinct() const { return is_distinct_; }
      bool is_set_distinct() { return is_set_distinct_; }
      bool is_for_update() { return for_update_; }
      bool has_limit()
      {
        return (limit_count_id_ != common::OB_INVALID_ID || limit_offset_id_ != common::OB_INVALID_ID);
      }
      SetOperator get_set_op() { return set_op_; }
      JoinedTable* get_joined_table(uint64_t table_id);

      const SelectItem& get_select_item(int32_t index) const
      {
        OB_ASSERT(0 <= index && index < select_items_.size());
        return select_items_[index];
      }

      const FromItem& get_from_item(int32_t index) const
      {
        OB_ASSERT(0 <= index && index < from_items_.size());
        return from_items_[index];
      }

      const OrderItem& get_order_item(int32_t index) const
      {
        OB_ASSERT(0 <= index && index < order_items_.size());
        return order_items_[index];
      }
	  //add liumz, [optimize group_order by index]20170419:b
      int64_t& get_order_item_flag(int32_t index) const
      {
        OB_ASSERT(0 <= index && index < order_items_indexed_flags_.size());
        return order_items_indexed_flags_[index];
      }
	  //add:e
      uint64_t get_group_expr_id(int32_t index) const
      {
        OB_ASSERT(0 <= index && index < group_expr_ids_.size());
        return group_expr_ids_[index];
      }
	  //add liumz, [optimize group_order by index]20170419:b
      int64_t& get_group_expr_flag(int32_t index) const
      {
        OB_ASSERT(0 <= index && index < group_expr_indexed_flags_.size());
        return group_expr_indexed_flags_[index];
      }
	  //add:e
      uint64_t get_agg_expr_id(int32_t index) const
      {
        OB_ASSERT(0 <= index && index < agg_func_ids_.size());
        return agg_func_ids_[index];
      }

      uint64_t get_having_expr_id(int32_t index) const
      {
        OB_ASSERT(0 <= index && index < having_expr_ids_.size());
        return having_expr_ids_[index];
      }

      common::ObVector<uint64_t>& get_having_exprs()
      {
        return having_expr_ids_;
      }

      int add_group_expr(uint64_t expr_id)
      {
        group_expr_indexed_flags_.push_back(0);//add liumz, [optimize group_order by index]20170419
        return group_expr_ids_.push_back(expr_id);
      }

      int add_agg_func(uint64_t expr_id)
      {
        return agg_func_ids_.push_back(expr_id);
      }

      int add_from_item(uint64_t tid, bool is_joined = false)
      {
        int ret = OB_SUCCESS;
        if (tid != OB_INVALID_ID)
        {
          FromItem item;
          item.table_id_ = tid;
          item.is_joined_ = is_joined;
          ret = from_items_.push_back(item);
        }
        else
        {
          ret = OB_ERR_ILLEGAL_ID;
        }
        return ret;
      }

      int add_order_item(OrderItem& order_item)
      {
        order_items_indexed_flags_.push_back(0);//add liumz, [optimize group_order by index]20170419
        return order_items_.push_back(order_item);
      }

      //add liumz, [ROW_NUMBER]20150824
      int add_anal_func(uint64_t expr_id)
      {
        return anal_func_ids_.push_back(expr_id);
      }

      uint64_t get_anal_expr_id(int32_t index) const
      {
        OB_ASSERT(0 <= index && index < anal_func_ids_.size());
        return anal_func_ids_[index];
      }

      int add_partition_expr(uint64_t expr_id)
      {
        return partition_expr_ids_.push_back(expr_id);
      }

      uint64_t get_partition_expr_id(int32_t index) const
      {
        OB_ASSERT(0 <= index && index < partition_expr_ids_.size());
        return partition_expr_ids_[index];
      }

      int add_order_item_for_rownum(OrderItem& order_item)
      {
        return order_items_for_rownum_.push_back(order_item);
      }

      const OrderItem& get_order_item_for_rownum(int32_t index) const
      {
        OB_ASSERT(0 <= index && index < order_items_for_rownum_.size());
        return order_items_for_rownum_[index];
      }
      //add:e

      //add gaojt [ListAgg][JHOBv0.1]20150104:b
      int add_order_item_for_listagg(OrderItem& order_item)
      {
        return order_items_for_listagg_.push_back(order_item);
      }
      const OrderItem& get_order_item_for_listagg(int32_t index) const
      {
        OB_ASSERT(0 <= index && index < order_items_for_listagg_.size());
        return order_items_for_listagg_[index];
      }
      int32_t get_order_item_for_listagg_size() const { return order_items_for_listagg_.size(); }
      //add 20150104:e
      int add_joined_table(JoinedTable* pJoinedTable)
      {
        return joined_tables_.push_back(pJoinedTable);
      }

      int add_having_expr(uint64_t expr_id)
      {
        return having_expr_ids_.push_back(expr_id);
      }

      void set_limit_offset(const uint64_t& limit, const uint64_t& offset)
      {
        limit_count_id_ = limit;
        limit_offset_id_ = offset;
      }

      void set_for_update(bool for_update)
      {
        for_update_ = for_update;
      }

      int check_having_ident(
          ResultPlan& result_plan,
          ObString& column_name,
          TableItem* table_item,
          ObRawExpr*& ret_expr) const;

      int add_select_item(
          uint64_t eid,
          bool is_real_alias,
          const common::ObString& alias_name,
          const common::ObString& expr_name,
          const common::ObObjType& type);

      int copy_select_items(ObSelectStmt* select_stmt);
      int copy_select_items_v2(ObSelectStmt* select_stmt, ObLogicalPlan* logical_plan);
      //add peiouya  [IN_TYPEBUG_FIX] 20151225 :b
      int add_dsttype_for_output_columns(common::ObArray<common::ObObjType>& columns_dsttype);
      common::ObArray<common::ObObjType> & get_output_columns_dsttype() {return output_columns_dsttype_;}
      //add 20151225:e
      void print(FILE* fp, int32_t level, int32_t index = 0);
      //add tianz [EXPORT_TOOL] 20141120:b
      inline int add_value_row(oceanbase::common::ObArray<uint64_t>& value_row)
      {
        return range_vectors_.push_back(value_row);
      }
      inline void set_values_size(int64_t size)
      {
        return range_vectors_.reserve(size);
      }
      inline const oceanbase::common::ObArray<uint64_t>& get_value_row(int64_t idx) const
      {
        OB_ASSERT(idx >= 0 && idx < range_vectors_.count());
        return range_vectors_.at(idx);
      }
      inline int64_t get_range_vector_count() {return range_vectors_.count();}
      inline void set_start_is_min() { start_is_min_ = true;}
      inline bool start_is_min(){ return start_is_min_;}
      inline void set_end_is_max(){ end_is_max_ = true;}
      inline bool end_is_max(){ return end_is_max_;}
      inline void set_has_range(bool has_range){has_range_ = has_range;}
      inline bool get_has_range()const {return has_range_;}
      inline const uint64_t get_table_id() const
      {
        OB_ASSERT(from_items_.size()>0);
        return from_items_[0].table_id_;
      }
      //add 20141120:e
	  //add liumz, [optimize group_order by index]20170419:b
      int32_t get_unindexed_order_item_size() const
      {
        int32_t unindexed = 0;
        for (int32_t i = 0; i < order_items_indexed_flags_.size(); i ++)
        {
          if (0 == order_items_indexed_flags_[i])
            unindexed++;
        }
        return unindexed;
      }

      bool is_indexed_group() const
      {
        int32_t indexed = 0;
        for (int32_t i = 0; i < group_expr_indexed_flags_.size(); i ++)
        {
          if (1 == group_expr_indexed_flags_[i])
            indexed++;
        }
        return indexed > 0;
      }
	  //add:e
      //add liuzy [sequence select] 20150525:b
      bool select_clause_has_sequence(){return select_clause_has_sequence_;}
      void set_select_clause_has_sequence(bool is_has_seq){select_clause_has_sequence_ = is_has_seq;}
      //pair
      common::ObArray<common::ObArray<std::pair<common::ObString,uint64_t> > > &get_sequence_all_name_type_pairs();
      common::ObArray<std::pair<common::ObString,uint64_t> > &get_sequence_single_name_type_pair(int64_t idx);
      void     reset_sequence_single_name_type_pair();
      int      cons_sequence_single_name_type_pair(common::ObString &sequence_name);
      int      add_sequence_single_name_type_pairs();
      //store info of where expr
      common::ObArray<uint64_t> &get_sequence_where_clause_expr_ids();
      int      add_sequence_where_clause_expr(uint64_t expr_id);
      //store use condition of sequence in select clause
      common::ObArray<bool> &get_sequence_select_clause_has_sequence();
      int      set_sequence_select_clause_has_sequence(bool is_has_sequence);
      //record count of column has sequence
      void generate_column_has_sequene_count();
      int64_t get_column_has_sequene_count();
      //add 20150525:e
      //delete by xionghui [subquery_final] 20160216 :b
      /*
      //add zhujun [fix equal-subquery bug] 20151013:b     
      inline bool get_is_equal_subquery()
      {
        return is_equal_subquery_;
      }

      inline void set_is_equal_subquery(bool flag)
      {
        is_equal_subquery_=flag;
      }
      //add 20151013:e
      //add xionghui [fix like_subquery bug] 20151015:
      inline bool get_is_like_subquery()
      {
          return is_like_subquery_;
      }
      inline void set_is_like_subquery(bool flag)
      {
          is_like_subquery_=flag;
      }
      //add e:
      */
      //delete e:
      //add qianzm [set_operation] 20160107:b
      common::ObArray<ObObjType>& get_result_type_array()
      {
        return result_type_array_for_setop_;
      }
      //add 20160107:e
      int add_result_type_array_for_setop(common::ObArray<common::ObObjType>& result_columns_type); //add qianzm [set_operation] 20160115
    private:
      /* These fields are only used by normal select */
      bool    is_distinct_;
      common::ObVector<SelectItem>   select_items_;
      common::ObVector<FromItem>     from_items_;
      common::ObVector<JoinedTable*> joined_tables_;
      common::ObVector<uint64_t>     group_expr_ids_;
      common::ObVector<int64_t>      group_expr_indexed_flags_;//indexed flag, 0 false : 1 true, default false //add liumz, [optimize group_order by index]20170419
      common::ObVector<uint64_t>     having_expr_ids_;
      common::ObVector<uint64_t>     agg_func_ids_;
      common::ObVector<uint64_t>     anal_func_ids_;//add liumz, [ROW_NUMBER]20150824
      common::ObVector<uint64_t>     partition_expr_ids_;//add liumz, [ROW_NUMBER]20150824
      common::ObArray<common::ObObjType> output_columns_dsttype_;  //add peiouya [IN_TYPEBUG_FIX] 20151225

      /* These fields are only used by set select */
      SetOperator set_op_;
      bool        is_set_distinct_;
      uint64_t    left_query_id_;
      uint64_t    right_query_id_;

      /* These fields are used by both normal select and set select */
      common::ObVector<OrderItem>  order_items_;
      common::ObVector<int64_t>    order_items_indexed_flags_;//indexed flag, 0 false : 1 true, default false //add liumz, [optimize group_order by index]20170419
      common::ObVector<OrderItem>  order_items_for_listagg_;//add gaojt [ListAgg][JHOBv0.1]20150104
      common::ObVector<OrderItem>  order_items_for_rownum_;//add liumz, [ROW_NUMBER]20150824

      /* -1 means no limit */
      uint64_t    limit_count_id_;
      uint64_t    limit_offset_id_;

      /* FOR UPDATE clause */
      bool      for_update_;

      uint64_t    gen_joined_tid_;
	  //add tianz [EXPORT_TOOL] 20141120:b
      bool has_range_;
      oceanbase::common::ObArray<oceanbase::common::ObArray<uint64_t> > range_vectors_;
	  bool start_is_min_;
	  bool end_is_max_;
	  //add 20141120:e
	  //add liuzy [sequence select] 20150401:b
      common::ObArray<common::ObArray<std::pair<common::ObString,uint64_t> > > sequence_name_type_pairs_;//存放所有列的sequence信息
      common::ObArray<std::pair<common::ObString,uint64_t> >                   single_name_type_pair_;  //存放一列的sequence信息
      common::ObArray<uint64_t>  sequence_where_clause_expr_id_;            //存储where条件中的sequence的expr_id，有push：expr_id，没有push：0
      common::ObArray<bool>      sequence_select_clause_has_sequence_;      //存储select查询列中是否有sequence，有push：true，没有push：false
      bool                       select_clause_has_sequence_;               //用于标记select clause中是否使用过sequence
      int64_t                    column_has_sequence_count_;                //用于记录select clause中有多少列包含sequence
      //add 20150525:e
      //delete by xionghui [subquery_final] 20160216 :b
      /*
      //add zhujun [fix equal-subquery bug] 20151013:b
      bool is_equal_subquery_;
      //add 20151013:e
      //add xionghui[fix like-subquery bug] 20151015:b
      bool is_like_subquery_;
      //add 20151015:e
      */
      //delete e:
      //add qianzm [set_operation] 20160107:b
      common::ObArray<ObObjType> result_type_array_for_setop_;
      //add 20160107:e
    };
    //add liuzy [sequence select] 20150525:b
    /*Expr: handle name_type pair*/
    inline common::ObArray<common::ObArray<std::pair<common::ObString,uint64_t> > > &ObSelectStmt::get_sequence_all_name_type_pairs()
    {
      return sequence_name_type_pairs_;
    }
    inline int ObSelectStmt::cons_sequence_single_name_type_pair(common::ObString &sequence_name)
    {
      int ret = common::OB_SUCCESS;
      std::pair<common::ObString,uint64_t> name_type;
      if (common::OB_SUCCESS != (ret = add_sequence_name_no_dup(sequence_name)))
      {
        TBSYS_LOG(ERROR,"add sequence name no dup error!");
      }
      if (common::OB_SUCCESS == ret)
      {
        name_type.first.assign_ptr(sequence_name.ptr(),sequence_name.length());
        TBSYS_LOG(DEBUG,"clone name is[%.*s]",name_type.first.length(),name_type.first.ptr());
        if (is_next_type())//ture nextval
        {
          name_type.second = common::NEXT_TYPE;
          TBSYS_LOG(DEBUG,"push next");
        }
        else//false  prevval
        {
          name_type.second = common::PREV_TYPE;
          TBSYS_LOG(DEBUG, "push preval");
        }
        if (OB_SUCCESS != (ret = single_name_type_pair_.push_back(name_type)))
        {
          TBSYS_LOG(ERROR,"push back single_name_type_pair_ failed:[%.*s]", sequence_name.length(), sequence_name.ptr());
        }
      }
      return ret;
    }
    inline int ObSelectStmt::add_sequence_single_name_type_pairs()
    {
      int ret = common::OB_SUCCESS;
      ret = sequence_name_type_pairs_.push_back(single_name_type_pair_);
      if (common::OB_SUCCESS != ret)
      {
        ret = common::OB_ERROR;
      }
      return ret;
    }
    inline void ObSelectStmt::reset_sequence_single_name_type_pair()
    {
      single_name_type_pair_.clear();
    }
    inline common::ObArray<std::pair<common::ObString,uint64_t> > &ObSelectStmt::get_sequence_single_name_type_pair(int64_t idx)
    {
      OB_ASSERT(idx >= 0 && idx < sequence_name_type_pairs_.count());
      return sequence_name_type_pairs_.at(idx);
    }
    /*Exp: handle sequence info in where expr*/
    inline common::ObArray<uint64_t> &ObSelectStmt::get_sequence_where_clause_expr_ids()
    {
      return sequence_where_clause_expr_id_;
    }
    inline int ObSelectStmt::add_sequence_where_clause_expr(uint64_t expr_id)
    {
      return sequence_where_clause_expr_id_.push_back(expr_id);
    }
    /*Exp: handle sequence info in select clause*/
    inline int ObSelectStmt::set_sequence_select_clause_has_sequence(bool is_has_sequence)
    {
      return sequence_select_clause_has_sequence_.push_back(is_has_sequence);
    }
    inline common::ObArray<bool> &ObSelectStmt::get_sequence_select_clause_has_sequence()
    {
      return sequence_select_clause_has_sequence_;
    }
    /*Exp: For every column contains sequence, column_has_sequence_count_ is increment by 1*/
    inline void ObSelectStmt::generate_column_has_sequene_count()
    {
      column_has_sequence_count_++;
    }
    inline int64_t ObSelectStmt::get_column_has_sequene_count()
    {
      return column_has_sequence_count_;
    }
    //add 20150525:e
  }
}

#endif //OCEANBASE_SQL_SELECTSTMT_H_
