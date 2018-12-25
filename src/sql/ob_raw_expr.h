#ifndef OCEANBASE_SQL_RAWEXPR_H_
#define OCEANBASE_SQL_RAWEXPR_H_
#include "common/ob_bit_set.h"
#include "ob_sql_expression.h"
#include "common/ob_define.h"
#include "common/ob_vector.h"
#include "common/ob_string.h"
#include "common/ob_string_buf.h"

namespace oceanbase
{
  namespace sql
  {
    class ObTransformer;
    class ObLogicalPlan;
    class ObPhysicalPlan;

    class ObRawExpr
    {
    public:
      explicit ObRawExpr(ObItemType expr_type = T_INVALID)
          :type_(expr_type)
      {
        result_type_ = ObMinType;
        is_listagg_ = false;//add gaojt [ListAgg][JHOBv0.1]20150104
      }
      virtual ~ObRawExpr() {}
      //virtual void trace(FILE *stream, int indentNum = 0);
      const ObItemType get_expr_type() const { return type_; }
      const common::ObObjType & get_result_type() const { return result_type_; }
      void set_expr_type(ObItemType type) { type_ = type; }
      void set_result_type(const common::ObObjType & type) { result_type_ = type; }

      bool is_const() const;
      bool is_column() const;
      // Format like "C1 = 5"
      bool is_equal_filter() const;
      // Format like "C1 between 5 and 10"
      bool is_range_filter() const;

      //add dyr [HugeTable_Join_Filter] [JHOBv0.1] 20140603:b
      // Format like "A.c1 = 5" or "A.c1 between 5 and 10"
      bool is_column_range_filter() const;
      //add 20140603:e

      // Only format like "T1.c1 = T2.c1", not "T1.c1 - T2.c1 = 0"
      bool is_join_cond() const;
      bool is_aggr_fun() const;
      void set_is_listagg(bool is_listagg);//add gaojt [ListAgg][JHOBv0.1]20140104
      bool get_is_listagg()const;//add gaojt [ListAgg][JHOBv0.1]20140104
      // c1 =5 c1 = ?
      bool is_equal_filter_expend() const; // add by maosy [Delete_Update_Function][prepare question mark ] 20170717
      bool is_question_mark()const ;// add by maosy [Delete_Update_Function][prepare question mark ] 20170717
      virtual int fill_sql_expression(
          ObSqlExpression& inter_expr,
          ObTransformer *transformer = NULL,
          ObLogicalPlan *logical_plan = NULL,
          ObPhysicalPlan *physical_plan = NULL) const = 0;
      virtual void print(FILE* fp, int32_t level) const = 0;

    private:
      //add wanglei [to date optimization] 20160329:b
      bool is_optimization_;
      //add wanglei :e
      ObItemType  type_;
      common::ObObjType result_type_;
      bool is_listagg_;//add gaojt [ListAgg][JHOBv0.1]20150104
    };

    class ObConstRawExpr : public ObRawExpr
    {
    public:
      ObConstRawExpr()
      {
      }
      ObConstRawExpr(oceanbase::common::ObObj& val, ObItemType expr_type = T_INVALID)
          : ObRawExpr(expr_type), value_(val)
      {
      }
      virtual ~ObConstRawExpr() {}
      const oceanbase::common::ObObj& get_value() const { return value_; }
      void set_value(const oceanbase::common::ObObj& val) { value_ = val; }
      int set_value_and_type(const common::ObObj& val);
      virtual int fill_sql_expression(
          ObSqlExpression& inter_expr,
          ObTransformer *transformer = NULL,
          ObLogicalPlan *logical_plan = NULL,
          ObPhysicalPlan *physical_plan = NULL) const;
      void print(FILE* fp, int32_t level) const;
    private:
      oceanbase::common::ObObj value_;
    };

    class ObCurTimeExpr : public ObRawExpr
    {
      public:
        explicit ObCurTimeExpr():ObRawExpr(T_CUR_TIME) {}
        virtual ~ObCurTimeExpr() {}
        virtual int fill_sql_expression(
            ObSqlExpression& inter_expr,
            ObTransformer *transformer = NULL,
            ObLogicalPlan *logical_plan = NULL,
            ObPhysicalPlan *physical_plan = NULL) const;
        void print(FILE* fp, int32_t level) const;
    };

    class ObUnaryRefRawExpr : public ObRawExpr
    {
    public:
      ObUnaryRefRawExpr()
      {
        id_ = OB_INVALID_ID;
      }
      ObUnaryRefRawExpr(uint64_t id, ObItemType expr_type = T_INVALID)
          : ObRawExpr(expr_type), id_(id)
      {
      }
      virtual ~ObUnaryRefRawExpr() {}
      uint64_t get_ref_id() const { return id_; }
      void set_ref_id(uint64_t id) { id_ = id; }
      virtual int fill_sql_expression(
          ObSqlExpression& inter_expr,
          ObTransformer *transformer = NULL,
          ObLogicalPlan *logical_plan = NULL,
          ObPhysicalPlan *physical_plan = NULL) const;
      void print(FILE* fp, int32_t level) const;
      int get_name(common::ObString& name) const;

    private:
      uint64_t id_;
    };

    class ObBinaryRefRawExpr : public ObRawExpr
    {
    public:
      ObBinaryRefRawExpr()
      {
        first_id_ = OB_INVALID_ID;
        second_id_ = OB_INVALID_ID;
      }
      ObBinaryRefRawExpr(uint64_t first_id, uint64_t second_id, ObItemType expr_type = T_INVALID)
          : ObRawExpr(expr_type), first_id_(first_id), second_id_(second_id)
      {
      }
      virtual ~ObBinaryRefRawExpr() {}
      uint64_t get_first_ref_id() const { return first_id_; }
      uint64_t get_second_ref_id() const { return second_id_; }
      void set_first_ref_id(uint64_t id) { first_id_ = id; }
      void set_second_ref_id(uint64_t id) { second_id_ = id; }
      virtual int fill_sql_expression(
          ObSqlExpression& inter_expr,
          ObTransformer *transformer = NULL,
          ObLogicalPlan *logical_plan = NULL,
          ObPhysicalPlan *physical_plan = NULL) const;
      void print(FILE* fp, int32_t level) const;

    private:
      uint64_t first_id_;
      uint64_t second_id_;
    };

    class ObUnaryOpRawExpr : public ObRawExpr
    {
    public:
      ObUnaryOpRawExpr()
      {
        expr_ = NULL;
      }
      ObUnaryOpRawExpr(ObRawExpr *expr, ObItemType expr_type = T_INVALID)
          : ObRawExpr(expr_type), expr_(expr)
      {
      }
      virtual ~ObUnaryOpRawExpr() {}
      ObRawExpr* get_op_expr() const { return expr_; }
      void set_op_expr(ObRawExpr *expr) { expr_ = expr; }
      virtual int fill_sql_expression(
          ObSqlExpression& inter_expr,
          ObTransformer *transformer = NULL,
          ObLogicalPlan *logical_plan = NULL,
          ObPhysicalPlan *physical_plan = NULL) const;
      void print(FILE* fp, int32_t level) const;

    private:
      ObRawExpr *expr_;
    };

    class ObBinaryOpRawExpr : public ObRawExpr
    {
    public:
      ObBinaryOpRawExpr()
      {
      }
      ObBinaryOpRawExpr(
          ObRawExpr *first_expr, ObRawExpr *second_expr, ObItemType expr_type = T_INVALID)
          : ObRawExpr(expr_type), first_expr_(first_expr), second_expr_(second_expr)
      {
      }
      virtual ~ObBinaryOpRawExpr(){}
      ObRawExpr* get_first_op_expr() const { return first_expr_; }
      ObRawExpr* get_second_op_expr() const { return second_expr_; }
      void set_op_exprs(ObRawExpr *first_expr, ObRawExpr *second_expr);
      virtual int fill_sql_expression(
          ObSqlExpression& inter_expr,
          ObTransformer *transformer = NULL,
          ObLogicalPlan *logical_plan = NULL,
          ObPhysicalPlan *physical_plan = NULL) const;
      void print(FILE* fp, int32_t level) const;

    private:
      ObRawExpr *first_expr_;
      ObRawExpr *second_expr_;
    };

    class ObTripleOpRawExpr : public ObRawExpr
    {
    public:
      ObTripleOpRawExpr()
      {
        first_expr_ = NULL;
        second_expr_ = NULL;
        third_expr_ = NULL;
      }
      ObTripleOpRawExpr(
          ObRawExpr *first_expr, ObRawExpr *second_expr,
          ObRawExpr *third_expr, ObItemType expr_type = T_INVALID)
          : ObRawExpr(expr_type),
          first_expr_(first_expr), second_expr_(second_expr),
          third_expr_(third_expr)
      {
      }
      virtual ~ObTripleOpRawExpr(){}
      ObRawExpr* get_first_op_expr() const { return first_expr_; }
      ObRawExpr* get_second_op_expr() const { return second_expr_; }
      ObRawExpr* get_third_op_expr() const { return third_expr_; }
      void set_op_exprs(ObRawExpr *first_expr, ObRawExpr *second_expr, ObRawExpr *third_expr);
      virtual int fill_sql_expression(
          ObSqlExpression& inter_expr,
          ObTransformer *transformer = NULL,
          ObLogicalPlan *logical_plan = NULL,
          ObPhysicalPlan *physical_plan = NULL) const;
      void print(FILE* fp, int32_t level) const;

    private:
      ObRawExpr *first_expr_;
      ObRawExpr *second_expr_;
      ObRawExpr *third_expr_;
    };

    class ObMultiOpRawExpr : public ObRawExpr
    {
    public:
      ObMultiOpRawExpr()
      {
      }
      virtual ~ObMultiOpRawExpr(){}
      ObRawExpr* get_op_expr(int32_t index) const
      {
        ObRawExpr* expr = NULL;
        if (index >= 0 && index < exprs_.size())
          expr = exprs_.at(index);
        return expr;
      }
      int add_op_expr(ObRawExpr *expr) { return exprs_.push_back(expr); }
      int32_t get_expr_size() const { return exprs_.size(); }
      virtual int fill_sql_expression(
          ObSqlExpression& inter_expr,
          ObTransformer *transformer = NULL,
          ObLogicalPlan *logical_plan = NULL,
          ObPhysicalPlan *physical_plan = NULL) const;
      void print(FILE* fp, int32_t level) const;

    private:
      oceanbase::common::ObVector<ObRawExpr*> exprs_;
    };

    class ObCaseOpRawExpr : public ObRawExpr
    {
    public:
      ObCaseOpRawExpr()
      {
        arg_expr_ = NULL;
        default_expr_ = NULL;
      }
      virtual ~ObCaseOpRawExpr(){}
      ObRawExpr* get_arg_op_expr() const { return arg_expr_; }
      ObRawExpr* get_default_op_expr() const { return default_expr_; }
      ObRawExpr* get_when_op_expr(int32_t index) const
      {
        ObRawExpr* expr = NULL;
        if (index >= 0 && index < when_exprs_.size())
          expr = when_exprs_[index];
        return expr;
      }
      ObRawExpr* get_then_op_expr(int32_t index) const
      {
        ObRawExpr* expr = NULL;
        if (index >= 0 || index < then_exprs_.size())
          expr = then_exprs_[index];
        return expr;
      }
      void set_arg_op_expr(ObRawExpr *expr) { arg_expr_ = expr; }
      void set_default_op_expr(ObRawExpr *expr) { default_expr_ = expr; }
      int add_when_op_expr(ObRawExpr *expr) { return when_exprs_.push_back(expr); }
      int add_then_op_expr(ObRawExpr *expr) { return then_exprs_.push_back(expr); }
      int32_t get_when_expr_size() const { return when_exprs_.size(); }
      int32_t get_then_expr_size() const { return then_exprs_.size(); }
      virtual int fill_sql_expression(
          ObSqlExpression& inter_expr,
          ObTransformer *transformer = NULL,
          ObLogicalPlan *logical_plan = NULL,
          ObPhysicalPlan *physical_plan = NULL) const;
      void print(FILE* fp, int32_t level) const;

    private:
      ObRawExpr *arg_expr_;
      oceanbase::common::ObVector<ObRawExpr*> when_exprs_;
      oceanbase::common::ObVector<ObRawExpr*> then_exprs_;
      ObRawExpr *default_expr_;
    };

    class ObAggFunRawExpr : public ObRawExpr
    {
    public:
      ObAggFunRawExpr()
      {
        param_expr_ = NULL;
        listagg_param_delimeter_ = NULL;//add gaojt [ListAgg][JHOBv0.1]20140104
        distinct_ = false;
      }
      //mod gaojt [ListAgg][JHOBv0.1]20150104:b
      /*
      ObAggFunRawExpr(ObRawExpr *param_expr,ObRawExpr* listagg_param_delimeter, bool is_distinct, ObItemType expr_type = T_INVALID)
          : ObRawExpr(expr_type), param_expr_(param_expr),listagg_param_delimeter_(listagg_param_delimeter), distinct_(is_distinct)
      {
      }*/
      ObAggFunRawExpr(ObRawExpr *param_expr, bool is_distinct, ObItemType expr_type = T_INVALID)
          : ObRawExpr(expr_type), param_expr_(param_expr), distinct_(is_distinct),listagg_param_delimeter_(NULL)
      {
      }
      // mod 20150104:e
      virtual ~ObAggFunRawExpr() {}
      ObRawExpr* get_param_expr() const { return param_expr_; }
      void set_param_expr(ObRawExpr *expr) { param_expr_ = expr; }
      void set_listagg_param_delimeter(ObRawExpr *expr) { listagg_param_delimeter_ = expr; }//add gaojt [ListAgg][JHOBv0.1]20150104
      bool is_param_distinct() const { return distinct_; }
      void set_param_distinct() { distinct_ = true; }
      virtual int fill_sql_expression(
          ObSqlExpression& inter_expr,
          ObTransformer *transformer = NULL,
          ObLogicalPlan *logical_plan = NULL,
          ObPhysicalPlan *physical_plan = NULL) const;
      void print(FILE* fp, int32_t level) const;

    private:
      // NULL means '*'
      ObRawExpr* param_expr_;
      bool     distinct_;
      ObRawExpr* listagg_param_delimeter_;//add gaojt [ListAgg][JHOBv0.1]20150104
    };

    class ObSysFunRawExpr : public ObRawExpr
    {
    public:
      ObSysFunRawExpr() {}
      virtual ~ObSysFunRawExpr() {}
      ObRawExpr* get_param_expr(int32_t index) const
      {
        ObRawExpr* expr = NULL;
        if (index >= 0 || index < exprs_.size())
          expr = exprs_[index];
        return expr;
      }
      int add_param_expr(ObRawExpr *expr) { return exprs_.push_back(expr); }
      void set_func_name(const common::ObString& name) { func_name_ = name; }
      const common::ObString& get_func_name() { return func_name_; }
      int32_t get_param_size() const { return exprs_.size(); }
      virtual int fill_sql_expression(
          ObSqlExpression& inter_expr,
          ObTransformer *transformer = NULL,
          ObLogicalPlan *logical_plan = NULL,
          ObPhysicalPlan *physical_plan = NULL) const;
      void print(FILE* fp, int32_t level) const;

    private:
      common::ObString func_name_;
      common::ObVector<ObRawExpr*> exprs_;
    };

    class ObSqlRawExpr
    {
    public:
      ObSqlRawExpr();
      ObSqlRawExpr(
          uint64_t expr_id,
          uint64_t table_id = oceanbase::common::OB_INVALID_ID,
          uint64_t column_id = oceanbase::common::OB_INVALID_ID,
          ObRawExpr* expr = NULL);
      virtual ~ObSqlRawExpr() {}
      uint64_t get_expr_id() const { return expr_id_; }
      uint64_t get_column_id() const { return column_id_; }
      uint64_t get_table_id() const { return table_id_; }
      void set_expr_id(uint64_t expr_id) { expr_id_ = expr_id; }
      void set_column_id(uint64_t column_id) { column_id_ = column_id; }
      void set_table_id(uint64_t table_id) { table_id_ = table_id; }
      void set_expr(ObRawExpr* expr) { expr_ = expr; }
      void set_tables_set(const common::ObBitSet<> tables_set) { tables_set_ = tables_set; }
      void set_applied(bool is_applied) { is_apply_ = is_applied; }
      void set_contain_aggr(bool contain_aggr) { contain_aggr_ = contain_aggr; }
      void set_contain_alias(bool contain_alias) { contain_alias_ = contain_alias; }
      void set_columnlized(bool is_columnlized) { is_columnlized_ = is_columnlized; }
      bool is_apply() const { return is_apply_; }
      bool is_contain_aggr() const { return contain_aggr_; }
      bool is_contain_alias() const { return contain_alias_; }
      bool is_columnlized() const { return is_columnlized_; }
      ObRawExpr* get_expr() const { return expr_; }
      const common::ObBitSet<>& get_tables_set() const { return tables_set_; }
      common::ObBitSet<>& get_tables_set() { return tables_set_; }
      const common::ObObjType get_result_type() const { return expr_->get_result_type(); }
      int fill_sql_expression(
          ObSqlExpression& inter_expr,
          ObTransformer *transformer = NULL,
          ObLogicalPlan *logical_plan = NULL,
          ObPhysicalPlan *physical_plan = NULL);
      void print(FILE* fp, int32_t level, int32_t index = 0) const;

      //add dyr [HugeTable_Join_Filter] [JHOBv0.1] 20140417:b
      void set_join_optimized(bool is_join_optimize) { is_join_optimized_ = is_join_optimize; }
      //返回true表示当前表达式已经在某个JOIN优化中被使用过了
      bool is_join_optimized() const { return is_join_optimized_; }
      //add 20140417:e

      //add duyr [join_without_pushdown_is_null] 20151214:b
      void set_push_down_with_outerjoin(bool val) {can_push_down_with_outerjoin_ = val;}
      bool can_push_down_with_outerjoin() const {return can_push_down_with_outerjoin_;}
      //add duyr 20151214:e

      int set_result_type(ObObjType &result_type);//add qianzm [set_operation] 20151222

    private:
      uint64_t  expr_id_;
      uint64_t  table_id_;
      uint64_t  column_id_;
      bool      is_apply_;
      bool      contain_aggr_;
      bool      contain_alias_;
      bool      is_columnlized_;
      common::ObBitSet<>  tables_set_;
      ObRawExpr*  expr_;

      //add dyr [HugeTable_Join_Filter] [JHOBv0.1] 20140417:b
      bool      is_join_optimized_; //true表示当前表达式已经在优化某个join操作时被使用过了
      //add 20140417:e
      //add duyr [join_without_pushdown_is_null] 20151214:b
      bool      can_push_down_with_outerjoin_;//true表示此表达式在join语句中时，也可以下压给cs；false表示不能下压
      //add 20151214:e
    };

  }

}

#endif //OCEANBASE_SQL_RAWEXPR_H_
