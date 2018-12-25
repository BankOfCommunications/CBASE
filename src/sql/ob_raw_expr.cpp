#include "ob_raw_expr.h"
#include "ob_transformer.h"
#include "type_name.c"
#include "ob_prepare.h"
#include "ob_result_set.h"

using namespace oceanbase::sql;
using namespace oceanbase::common;

bool ObRawExpr::is_const() const
{
  return (type_ >= T_INT && type_ <= T_NULL);
}

bool ObRawExpr::is_column() const
{
  return (type_ == T_REF_COLUMN);
}

bool ObRawExpr::is_equal_filter() const
{
  bool ret = false;
  if (type_ == T_OP_EQ || type_ == T_OP_IS)
  {
    ObBinaryOpRawExpr *binary_expr = dynamic_cast<ObBinaryOpRawExpr *>(const_cast<ObRawExpr *>(this));
    if (binary_expr->get_first_op_expr()->is_const()
      || binary_expr->get_second_op_expr()->is_const())
      ret = true;
  }
  return ret;
}
// add by maosy [Delete_Update_Function][prepare question mark ] 20170717 b:
bool ObRawExpr::is_question_mark()const
{
    return (type_ ==T_QUESTIONMARK) ;
}
bool ObRawExpr::is_equal_filter_expend() const
{
  bool ret = false;
  if (type_ == T_OP_EQ || type_ == T_OP_IS)
  {
    ObBinaryOpRawExpr *binary_expr = dynamic_cast<ObBinaryOpRawExpr *>(const_cast<ObRawExpr *>(this));
    if (binary_expr->get_first_op_expr()->is_const()
      || binary_expr->get_second_op_expr()->is_const())
    {
      ret = true;
    }
    else if ((binary_expr->get_first_op_expr()->is_column()
              || binary_expr->get_second_op_expr()->is_column())
             &&(binary_expr->get_first_op_expr()->is_question_mark()
             || binary_expr->get_second_op_expr()->is_question_mark()))
    {
        ret = true ;
    }

  }
  return ret;
}
// add by maosy e

bool ObRawExpr::is_range_filter() const
{
  bool ret = false;
  if (type_ >= T_OP_LE && type_ <= T_OP_GT)
  {
    ObBinaryOpRawExpr *binary_expr = dynamic_cast<ObBinaryOpRawExpr *>(const_cast<ObRawExpr *>(this));
    if (binary_expr->get_first_op_expr()->is_const()
      || binary_expr->get_second_op_expr()->is_const())
      ret = true;
  }
  else if (type_ == T_OP_BTW)
  {
    ObTripleOpRawExpr *triple_expr = dynamic_cast<ObTripleOpRawExpr *>(const_cast<ObRawExpr *>(this));
    if (triple_expr->get_second_op_expr()->is_const()
      && triple_expr->get_third_op_expr()->is_const())
      ret = true;
  }
  return ret;
}

//add dyr [HugeTable_Join_Filter] [JHOBv0.1] 20140603:b
bool ObRawExpr::is_column_range_filter() const
{
    bool ret = false;
    if (type_ == T_OP_EQ || type_ == T_OP_IS)
    {
      ObBinaryOpRawExpr *binary_expr = dynamic_cast<ObBinaryOpRawExpr *>(const_cast<ObRawExpr *>(this));
      if ( ( binary_expr->get_first_op_expr()->is_const()
             && binary_expr->get_second_op_expr()->get_expr_type() == T_REF_COLUMN )
        || ( binary_expr->get_second_op_expr()->is_const()
             && binary_expr->get_first_op_expr()->get_expr_type() == T_REF_COLUMN)
         )
        ret = true;
    }
    else if (type_ >= T_OP_LE && type_ <= T_OP_GT)
    {
      ObBinaryOpRawExpr *binary_expr = dynamic_cast<ObBinaryOpRawExpr *>(const_cast<ObRawExpr *>(this));
      if ( ( binary_expr->get_first_op_expr()->is_const()
             && binary_expr->get_second_op_expr()->get_expr_type() == T_REF_COLUMN )
        || ( binary_expr->get_second_op_expr()->is_const()
             && binary_expr->get_first_op_expr()->get_expr_type()  == T_REF_COLUMN )
         )
        ret = true;
    }
    else if (type_ == T_OP_BTW)
    {
      ObTripleOpRawExpr *triple_expr = dynamic_cast<ObTripleOpRawExpr *>(const_cast<ObRawExpr *>(this));
      if (triple_expr->get_first_op_expr()->get_expr_type() == T_REF_COLUMN
        && triple_expr->get_second_op_expr()->is_const()
        && triple_expr->get_third_op_expr()->is_const())
        ret = true;
    }
    return ret;
}
//add 20140603:e

bool ObRawExpr::is_join_cond() const
{
  bool ret = false;
  if (type_ == T_OP_EQ)
  {
    ObBinaryOpRawExpr *binary_expr = dynamic_cast<ObBinaryOpRawExpr *>(const_cast<ObRawExpr *>(this));
    if (binary_expr->get_first_op_expr()->get_expr_type() == T_REF_COLUMN
      && binary_expr->get_second_op_expr()->get_expr_type() == T_REF_COLUMN)
      ret = true;
  }
  return ret;
}
//add gaojt [ListAgg][JHOBv0.1]20150104:b
void ObRawExpr::set_is_listagg(bool is_listagg)
{
    is_listagg_ = is_listagg;
}
bool ObRawExpr::get_is_listagg() const
{
    return is_listagg_;
}
/*add 20150104:e */
bool ObRawExpr::is_aggr_fun() const
{
  bool ret = false;
  if (type_ >= T_FUN_MAX && type_ <= T_FUN_AVG)
    ret = true;
  return ret;
}

int ObConstRawExpr::set_value_and_type(const common::ObObj& val)
{
  int ret = OB_SUCCESS;
  switch(val.get_type())
  {
    case ObNullType:   // 空类型
      this->set_expr_type(T_NULL);
      this->set_result_type(ObNullType);
      break;
    case ObIntType:
      this->set_expr_type(T_INT);
      this->set_result_type(ObIntType);
      break;
    //add lijianqiang [INT_32] 20151008:b
    case ObInt32Type:
      this->set_expr_type(T_INT32);
      this->set_result_type(ObInt32Type);
      break;
    //add 20151008:e
    case ObFloatType:              // @deprecated
      this->set_expr_type(T_FLOAT);
      this->set_result_type(ObFloatType);
      break;
    case ObDoubleType:             // @deprecated
      this->set_expr_type(T_DOUBLE);
      this->set_result_type(ObDoubleType);
      break;
    //add peiouya [DATE_TIME] 20150906:b
    case ObDateType:
    this->set_expr_type(T_DATE_NEW);
      this->set_result_type(ObDateType);
      break;
    case ObTimeType:
    this->set_expr_type(T_TIME);
      this->set_result_type(ObTimeType);
      break;
    //add 20150906:e
    //add peiouya [DATE_TIME] 20150908:b
    case ObIntervalType:
      this->set_expr_type(T_DATE);
      this->set_result_type(ObIntervalType);
      break;
    //add 20150908:e
    case ObPreciseDateTimeType:    // =5
    case ObCreateTimeType:
    case ObModifyTimeType:
      this->set_expr_type(T_DATE);
      this->set_result_type(ObPreciseDateTimeType);
      break;
    case ObVarcharType:
      this->set_expr_type(T_STRING);
      this->set_result_type(ObVarcharType);
      break;
    case ObBoolType:
      this->set_expr_type(T_BOOL);
      this->set_result_type(ObBoolType);
      break;
    //add fanqiushi DECIMAL OceanBase_BankCommV0.3 2014_7_19:b
    case ObDecimalType:
      this->set_expr_type(T_DECIMAL);
      this->set_result_type(ObDecimalType);
      break;
      //add:e
    default:
      ret = OB_NOT_SUPPORTED;
      TBSYS_LOG(WARN, "obj type not support, type=%d", val.get_type());
      break;
  }
  if (OB_LIKELY(OB_SUCCESS == ret))
  {
    if (ObExtendType != val.get_type())
    {
      value_ = val;
    }
  }
  return ret;
}

void ObConstRawExpr::print(FILE* fp, int32_t level) const
{
  for(int i = 0; i < level; ++i) fprintf(fp, "    ");
  fprintf(fp, "%s : ", get_type_name(get_expr_type()));
  switch(get_expr_type())
  {
    case T_INT:
    {
      int64_t i = 0;
      value_.get_int(i);
      fprintf(fp, "%ld\n", i);
      break;
    }
    //add lijianqiang [INT_32] 20151008:b
    case T_INT32:
    {
      int32_t i = 0;
      value_.get_int32(i);
      fprintf(fp, "%d\n", i);
      break;
    }
    //add 20151008:e
    case T_STRING:
    case T_BINARY:
    {
      ObString str;
      value_.get_varchar(str);
      fprintf(fp, "'%.*s' length=%d\n", str.length(), str.ptr(), str.length());
      break;
    }
    case T_DATE:
    {
      ObDateTime d = static_cast<ObDateTime>(0L);
      value_.get_datetime(d);
      fprintf(fp, "%ld\n", d);
      break;
    }
    //add peiouya [DATE_TIME] 20150912:b
    case T_DATE_NEW:
    {
      ObDate d = static_cast<ObDate>(0L);
      value_.get_date(d);
      fprintf(fp, "%ld\n", d);
      break;
    }
    case T_TIME:
    {
      ObTime d = static_cast<ObTime>(0L);
      value_.get_time(d);
      fprintf(fp, "%ld\n", d);
      break;
    }
    //add 20150912:e
    case T_FLOAT:
    {
      float f = 0.0f;
      value_.get_float(f);
      fprintf(fp, "%f\n", f);
      break;
    }
    case T_DOUBLE:
    {
      double d = 0.0f;
      value_.get_double(d);
      fprintf(fp, "%lf\n", d);
      break;
    }
    case T_DECIMAL:
    {
      ObString str;
      value_.get_varchar(str);
      fprintf(fp, "%.*s\n", str.length(), str.ptr());
      break;
    }
    case T_BOOL:
    {
      bool b = false;
      value_.get_bool(b);
      fprintf(fp, "%s\n", b ? "TRUE" : "FALSE");
      break;
    }
    case T_NULL:
    {
      fprintf(fp, "NULL\n");
      break;
    }
    case T_UNKNOWN:
    {
      fprintf(fp, "UNKNOWN\n");
      break;
    }
    default:
      fprintf(fp, "error type!\n");
      break;
  }
}

int ObConstRawExpr::fill_sql_expression(
    ObSqlExpression& inter_expr,
    ObTransformer *transformer,
    ObLogicalPlan *logical_plan,
    ObPhysicalPlan *physical_plan) const
{
  int ret = OB_SUCCESS;
  UNUSED(logical_plan);
  UNUSED(physical_plan);
  UNUSED(transformer);
  float f = 0.0f;
  double d = 0.0;
  ExprItem item;
  item.type_ = get_expr_type();
  item.data_type_ = get_result_type();
  switch (item.type_)
  {
    case T_STRING:
    case T_BINARY:
      ret = value_.get_varchar(item.string_);
      break;
    case T_FLOAT:
      ret = value_.get_float(f);
      item.value_.float_ = f;
      break;
    case T_DOUBLE:
      ret = value_.get_double(d);
      item.value_.double_ = d;
      break;
    case T_DECIMAL:
      //modify fanqiushi DECIMAL OceanBase_BankCommV0.3 2014_7_19:b
      //ret = value_.get_varchar(item.string_);
      ret = value_.get_decimal(item.string_);
      //modify:e
      break;
    case T_INT:
      ret = value_.get_int(item.value_.int_);
      break;
    //add lijianqiang [INT_32] 20151008:b
    case T_INT32:
      ret = value_.get_int32(item.value_.int32_);
      break;
    //add 20151008:e
    case T_BOOL:
      ret = value_.get_bool(item.value_.bool_);
      break;
    case T_DATE:
      ret = value_.get_precise_datetime(item.value_.datetime_);
      break;
    //add peiouya [DATE_TIME] 20150912:b
    case T_DATE_NEW:
      ret = value_.get_date(item.value_.datetime_);
      break;
    case T_TIME:
      ret = value_.get_time(item.value_.datetime_);
      break;
    //add 20150912:e
    case T_QUESTIONMARK:
      ret = value_.get_int(item.value_.int_);
      //add gaojt [Delete_Update_Function] [JHOBv0.1] 20160520:b
      if (inter_expr.get_is_update())
      {
         inter_expr.add_question_num(1/*step*/);
      }
      //add gaojt 20160520:e
      break;
    case T_SYSTEM_VARIABLE:
    case T_TEMP_VARIABLE:
      ret = value_.get_varchar(item.string_);
      break;
    case T_NULL:
      break;
    default:
      TBSYS_LOG(WARN, "unexpected expression type %d", item.type_);
      ret = OB_ERR_EXPR_UNKNOWN;
      break;
  }
  if (OB_SUCCESS == ret)
  {
    //add gaojt [ListAgg][JHOBv0.1]20150104:b
    if(get_is_listagg())
    {
      inter_expr.set_listagg_delimeter(item.string_);
    }
    else /* add 20150104:e */
      ret = inter_expr.add_expr_item(item);
  }
  return ret;
}

void ObCurTimeExpr::print(FILE* fp, int32_t level) const
{
  for(int i = 0; i < level; ++i) fprintf(fp, "    ");
  fprintf(fp, "%s\n", get_type_name(get_expr_type()));
}

int ObCurTimeExpr::fill_sql_expression(
    ObSqlExpression& inter_expr,
    ObTransformer *transformer,
    ObLogicalPlan *logical_plan,
    ObPhysicalPlan *physical_plan) const
{
  UNUSED(physical_plan);
  UNUSED(transformer);

  int ret = OB_SUCCESS;
  //add liuzy [datetime func] 20150909:b
  int64_t idx = logical_plan->get_cur_time_fun_type_idx();
  if (logical_plan->get_cur_time_fun_type_size() == idx)
  {
    idx = 0;
    logical_plan->reset_cur_time_fun_type_idx();
  }
  //add 20150909:e
  ExprItem item;
  item.type_ = get_expr_type(); //T_CUR_TIME,T_CUR_TIME_HMS,T_CUR_DATE
  //add liuzy [datetime func] 20150909:b
  if (T_CUR_DATE == item.type_)
  {
    item.data_type_ = ObDateType;
  }
  else if (T_CUR_TIME_HMS == item.type_)
  {
    item.data_type_ = ObTimeType;
  }
  else
  {
    //add 20150909:e
    item.data_type_ = ObPreciseDateTimeType;
  }
  //mod liuzy [datetime func] 20150909:b
  /*Exp: 修改get_cur_time_fun_type()接口,返回ObArray的引用*/
  //item.value_.int_ = logical_plan->get_cur_time_fun_type();
  item.value_.int_ = logical_plan->get_cur_time_fun_type().at(idx); //just place holder
  TBSYS_LOG(DEBUG, "fill_sql_expression: item.type_ = [%d], item.data_type_ = [%d], idx = [%ld]", item.type_, item.data_type_, idx);
  //mod 20150909:e
  ret = inter_expr.add_expr_item(item);

  return ret;
}

void ObUnaryRefRawExpr::print(FILE* fp, int32_t level) const
{
  for(int i = 0; i < level; ++i) fprintf(fp, "    ");
  fprintf(fp, "%s : %lu\n", get_type_name(get_expr_type()), id_);
}

int ObUnaryRefRawExpr::fill_sql_expression(
    ObSqlExpression& inter_expr,
    ObTransformer *transformer,
    ObLogicalPlan *logical_plan,
    ObPhysicalPlan *physical_plan) const
{
  int ret = OB_SUCCESS;
  ExprItem item;
  item.type_ = get_expr_type();
  if (transformer == NULL || logical_plan == NULL || physical_plan == NULL)
  {
    TBSYS_LOG(ERROR, "transformer error");
    ret = OB_ERROR;
  }
  else
  {
    ErrStat err_stat;
    int32_t index = OB_INVALID_INDEX;
    ret = transformer->gen_physical_select(logical_plan, physical_plan, err_stat, id_, &index);
	//add tianz [SubQuery_for_Instmt] [JHOBv0.1] 20140404:b
	/*Exp: add current expression's sub_query_num,
	* and store sub_query index to current expression
	*/
    inter_expr.add_sub_query_num();
    if(OB_SUCCESS != (ret = inter_expr.set_sub_query_idx(inter_expr.get_sub_query_num(),index)))
    {
      TBSYS_LOG(ERROR, "set sub query idx error");
    }
    else
    {
      ObSelectStmt *sub_query = dynamic_cast<ObSelectStmt*>(logical_plan->get_query(id_));
      item.value_.int_ = sub_query->get_select_item_size();//sub select column num
    }
	//add 20140404:e

	//del tianz [SubQuery_for_Instmt] [JHOBv0.1] 20140504:b
	//item.value_.int_ = index;
	//del 20140504:e

	//add tianz [SubQuery_for_Instmt] [JHOBv0.1] 20140605:b
    if (ret == OB_SUCCESS && OB_INVALID_INDEX == index)
    {
      TBSYS_LOG(ERROR, "generating physical plan for sub-query error");
      ret = OB_ERROR;
    }
	//add 20140605:e
  }
  //del tianz [SubQuery_for_Instmt] [JHOBv0.1] 20140605:b
  //if (ret == OB_SUCCESS && OB_INVALID_INDEX == item.value_.int_)
  //{
  //  TBSYS_LOG(ERROR, "generating physical plan for sub-query error");
  //  ret = OB_ERROR;
 // }
  //del 20140605:e
  if (ret == OB_SUCCESS)
    ret = inter_expr.add_expr_item(item);
  return ret;
}

void ObBinaryRefRawExpr::print(FILE* fp, int32_t level) const
{
  for(int i = 0; i < level; ++i) fprintf(fp, "    ");
  if (first_id_ == OB_INVALID_ID)
    fprintf(fp, "%s : [table_id, column_id] = [NULL, %lu]\n",
            get_type_name(get_expr_type()), second_id_);
  else
    fprintf(fp, "%s : [table_id, column_id] = [%lu, %lu]\n",
            get_type_name(get_expr_type()), first_id_, second_id_);
}

int ObBinaryRefRawExpr::fill_sql_expression(
    ObSqlExpression& inter_expr,
    ObTransformer *transformer,
    ObLogicalPlan *logical_plan,
    ObPhysicalPlan *physical_plan) const
{
  int ret = OB_SUCCESS;
  UNUSED(transformer);
  UNUSED(logical_plan);
  UNUSED(physical_plan);
  ExprItem item;
  item.type_ = get_expr_type();
  item.data_type_ = get_result_type();

  if (ret == OB_SUCCESS && get_expr_type() == T_REF_COLUMN)
  {
    item.value_.cell_.tid = first_id_;
    item.value_.cell_.cid = second_id_;
  }
  else
  {
    // No other type
    ret = OB_ERROR;
  }
  if (ret == OB_SUCCESS)
    ret = inter_expr.add_expr_item(item);
  return ret;
}

void ObUnaryOpRawExpr::print(FILE* fp, int32_t level) const
{
  for(int i = 0; i < level; ++i) fprintf(fp, "    ");
  fprintf(fp, "%s\n", get_type_name(get_expr_type()));
  expr_->print(fp, level + 1);
}

int ObUnaryOpRawExpr::fill_sql_expression(
    ObSqlExpression& inter_expr,
    ObTransformer *transformer,
    ObLogicalPlan *logical_plan,
    ObPhysicalPlan *physical_plan) const
{
  int ret = OB_SUCCESS;
  ExprItem item;
  item.type_ = get_expr_type();
  item.data_type_ = get_result_type();
  item.value_.int_ = 1; /* One operator */

  ret = expr_->fill_sql_expression(inter_expr, transformer, logical_plan, physical_plan);
  if (ret == OB_SUCCESS)
    ret = inter_expr.add_expr_item(item);
  return ret;
}

void ObBinaryOpRawExpr::print(FILE* fp, int32_t level) const
{
  for(int i = 0; i < level; ++i) fprintf(fp, "    ");
  fprintf(fp, "%s\n", get_type_name(get_expr_type()));
  first_expr_->print(fp, level + 1);
  second_expr_->print(fp, level + 1);
}

void ObBinaryOpRawExpr::set_op_exprs(ObRawExpr *first_expr, ObRawExpr *second_expr)
{
  ObItemType exchange_type = T_MIN_OP;
  switch (get_expr_type())
  {
    case T_OP_LE:
      exchange_type = T_OP_GE;
      break;
    case T_OP_LT:
      exchange_type = T_OP_GT;
      break;
    case T_OP_GE:
      exchange_type = T_OP_LE;
      break;
    case T_OP_GT:
      exchange_type = T_OP_LT;
      break;
    case T_OP_EQ:
    case T_OP_NE:
      exchange_type = get_expr_type();
      break;
    default:
      exchange_type = T_MIN_OP;
      break;
  }
  if (exchange_type != T_MIN_OP
    && first_expr && first_expr->is_const()
    && second_expr && second_expr->is_column())
  {
    set_expr_type(exchange_type);
    first_expr_ = second_expr;
    second_expr_ = first_expr;
  }
  else
  {
    first_expr_ = first_expr;
    second_expr_ = second_expr;
  }
}

int ObBinaryOpRawExpr::fill_sql_expression(
    ObSqlExpression& inter_expr,
    ObTransformer *transformer,
    ObLogicalPlan *logical_plan,
    ObPhysicalPlan *physical_plan) const
{
  int ret = OB_SUCCESS;
  bool dem_1_to_2 = false;
  ExprItem item;
  item.type_ = get_expr_type();
  item.data_type_ = get_result_type();
  item.value_.int_ = 2; /* Two operators */

  // all form with 1 dimension and without sub-select will be changed to 2 dimensions
  // c1 in (1, 2, 3) ==> (c1) in ((1), (2), (3))
  if ((ret = first_expr_->fill_sql_expression(
                             inter_expr,
                             transformer,
                             logical_plan,
                             physical_plan)) == OB_SUCCESS
    && (get_expr_type() == T_OP_IN || get_expr_type() == T_OP_NOT_IN))
  {
    if (!first_expr_ || !second_expr_)
    {
      ret = OB_ERR_EXPR_UNKNOWN;
    }
    else if (first_expr_->get_expr_type() != T_OP_ROW
      && first_expr_->get_expr_type() != T_REF_QUERY
	  //mod tianz [SubQuery_for_Instmt] [JHOBv0.1] 20140404:b
	  /*Exp: make in sub_query and in true value have same format*/
	  //&& second_expr_->get_expr_type() == T_OP_ROW)
      && (second_expr_->get_expr_type() == T_OP_ROW ||second_expr_->get_expr_type() == T_REF_QUERY))
	  //mod 20140404:e
    {
      dem_1_to_2 = true;
      ExprItem dem2;
      dem2.type_ = T_OP_ROW;
      dem2.data_type_ = ObIntType;
      dem2.value_.int_ = 1;
      ret = inter_expr.add_expr_item(dem2);
    }
    if (OB_LIKELY(ret == OB_SUCCESS))
    {
      ExprItem left_item;
      left_item.type_ = T_OP_LEFT_PARAM_END;
      left_item.data_type_ = ObIntType;
      switch (first_expr_->get_expr_type())
      {
        case T_OP_ROW:
        case T_REF_QUERY:
          left_item.value_.int_ = 2;
          break;
        default:
          left_item.value_.int_ = 1;
          break;
      }
      if (dem_1_to_2)
        left_item.value_.int_ = 2;
      ret = inter_expr.add_expr_item(left_item);
    }
  }
  if (ret == OB_SUCCESS)
  {
  	//mod tianz [SubQuery_for_Instmt] [JHOBv0.1] 20140404:b
	/*Exp: make in sub_query and in true value have same format*/
	//if (!dem_1_to_2)
    if ((!dem_1_to_2)||second_expr_->get_expr_type() == T_REF_QUERY )
	//mod 20140404:e
    {
      ret = second_expr_->fill_sql_expression(inter_expr, transformer, logical_plan, physical_plan);
      //add peiouya [IN_TYPEBUG_FIX] 20151225:b
      if (OB_SUCCESS == ret
         && (get_expr_type() == T_OP_IN || get_expr_type() == T_OP_NOT_IN)
         && second_expr_->get_expr_type() == T_REF_QUERY)
      {
        /* 1. get the the column result type of left operand */
        common::ObArray<common::ObObjType> left_output_columns_dsttype;  //save left column result_type
        switch (first_expr_->get_expr_type ())
        {
          case T_OP_ROW:
          {
            ObMultiOpRawExpr *left_expr = dynamic_cast<ObMultiOpRawExpr *>(first_expr_);
            int32_t num_left_param = left_expr->get_expr_size();
            for (int32_t idx = 0; idx < num_left_param; idx++)
            {
              left_output_columns_dsttype.push_back (left_expr->get_op_expr(idx)->get_result_type ());
            }
            break;
          }
          case T_REF_QUERY:
          {
             TBSYS_LOG(USER_ERROR,"Not support sql like : subquery in (...)");
             ret =OB_NOT_SUPPORTED;
             break;
           }
           default:
           {
             //this case left column num must be only 1
             left_output_columns_dsttype.push_back (first_expr_->get_result_type ());
             break;
           }
         }

         //* 2 get get the the column result type of right operand
         ObUnaryRefRawExpr *right_expr = dynamic_cast<ObUnaryRefRawExpr *>(second_expr_);
         ObSelectStmt* right_select = NULL;
         common::ObArray<common::ObObjType> right_output_columns_dsttype;  //save right column result_type
         if (OB_SUCCESS != ret)
         {
           //nothing todo
         }
         else if (NULL != right_expr && NULL != (right_select = logical_plan->get_select_query (right_expr->get_ref_id ())))
         {
           right_output_columns_dsttype= right_select->get_output_columns_dsttype ();
         }
         else
         {
           TBSYS_LOG(WARN,"Fail to acquire subquery plan");
           ret = OB_ERR_ILLEGAL_ID;
         }

         //* 3 determin result data type
         common::ObArray<common::ObObjType> last_output_columns_dsttype;  //save promote type ,accoring to right_data_type and left_data_type
         if (OB_SUCCESS != ret)
         {
           //nothing todo
         }
         else if (right_output_columns_dsttype.count () == left_output_columns_dsttype.count ())
         {
           ObObjType res_type = ObMaxType;
           for (int64_t idx = 0; idx < right_output_columns_dsttype.count (); idx ++)
           {
             res_type = ObExprObj::type_promotion_for_in_op (left_output_columns_dsttype.at (idx), right_output_columns_dsttype.at(idx));
             if (ObMaxType == res_type)
             {
              TBSYS_LOG(USER_ERROR,"%s can not compare with %s ",
                              oceanbase::common::ObObj::get_sql_type(left_output_columns_dsttype.at (idx)),
                              oceanbase::common::ObObj::get_sql_type(right_output_columns_dsttype.at(idx)));
              ret = OB_NOT_SUPPORTED;
              break;
             }
             else
             {
               last_output_columns_dsttype.push_back (res_type);
             }
           }
         }
         else
         {
           TBSYS_LOG(USER_ERROR,"In operands contain different column(s)");
           ret = OB_ERR_COLUMN_SIZE;
         }

         //* 4. get subquery physical plan in  physical_plan
         ObPhyOperator* sub_query_physical_plan = NULL;
         if (OB_SUCCESS == ret)
         {
           int64_t index = OB_INVALID_INDEX;
           index = inter_expr.get_sub_query_num ();
           sub_query_physical_plan = physical_plan->get_phy_query (inter_expr.get_sub_query_idx (static_cast<int32_t>(--index)));
         }

         //* 5. set last_output_columns_dsttype only if subquery's first operator is in
         //      (ObProject, ObSequenceSelect, ObSetOperator's children)
         if (OB_SUCCESS != ret)
         {
           //nothing todo
         }
         else if (NULL != sub_query_physical_plan
                    && (PHY_PROJECT == sub_query_physical_plan->get_type ()
                        || PHY_SEQUENCE_SELECT == sub_query_physical_plan->get_type ()
                        || PHY_MERGE_UNION == sub_query_physical_plan->get_type ()
                        || PHY_MERGE_EXCEPT == sub_query_physical_plan->get_type ()
                        || PHY_MERGE_INTERSECT == sub_query_physical_plan->get_type ()))
         {
           sub_query_physical_plan->add_dsttype_for_output_columns (last_output_columns_dsttype);
         }
         else
         {
           TBSYS_LOG(USER_ERROR,"Not support the sql");
           ret = OB_NOT_SUPPORTED;
         }
       }
     //add 20151225:e
    }
    else
    {
      ExprItem dem2;
      dem2.type_ = T_OP_ROW;
      dem2.data_type_ = ObIntType;
      dem2.value_.int_ = 1;
      ObMultiOpRawExpr *row_expr = dynamic_cast<ObMultiOpRawExpr*>(second_expr_);
      if (row_expr != NULL)
      {
        ExprItem row_item;
        row_item.type_ = row_expr->get_expr_type();
        row_item.data_type_ = row_expr->get_result_type();
        row_item.value_.int_ = row_expr->get_expr_size();
        for (int32_t i = 0; ret == OB_SUCCESS && i < row_expr->get_expr_size(); i++)
        {
          if ((ret = row_expr->get_op_expr(i)->fill_sql_expression(
                                                   inter_expr,
                                                   transformer,
                                                   logical_plan,
                                                   physical_plan)) != OB_SUCCESS
            || (ret = inter_expr.add_expr_item(dem2)) != OB_SUCCESS)
          {
            break;
          }
        }
        if (ret == OB_SUCCESS)
          ret = inter_expr.add_expr_item(row_item);
      }
      else
      {
        ret = OB_ERR_EXPR_UNKNOWN;
      }
    }
  }
  if (ret == OB_SUCCESS)
    ret = inter_expr.add_expr_item(item);
  return ret;
}

void ObTripleOpRawExpr::print(FILE* fp, int32_t level) const
{
  for(int i = 0; i < level; ++i) fprintf(fp, "    ");
  fprintf(fp, "%s\n", get_type_name(get_expr_type()));
  first_expr_->print(fp, level + 1);
  second_expr_->print(fp, level + 1);
  third_expr_->print(fp, level + 1);
}

void ObTripleOpRawExpr::set_op_exprs(
    ObRawExpr *first_expr,
    ObRawExpr *second_expr,
    ObRawExpr *third_expr)
{
  first_expr_ = first_expr;
  second_expr_ = second_expr;
  third_expr_ = third_expr;
}

int ObTripleOpRawExpr::fill_sql_expression(
    ObSqlExpression& inter_expr,
    ObTransformer *transformer,
    ObLogicalPlan *logical_plan,
    ObPhysicalPlan *physical_plan) const
{
  int ret = OB_SUCCESS;
  ExprItem item;
  item.type_ = get_expr_type();
  item.data_type_ = get_result_type();
  item.value_.int_ = 3; /* thress operators */

  if (ret == OB_SUCCESS)
    ret = first_expr_->fill_sql_expression(inter_expr, transformer, logical_plan, physical_plan);
  if (ret == OB_SUCCESS)
    ret = second_expr_->fill_sql_expression(inter_expr, transformer, logical_plan, physical_plan);
  if (ret == OB_SUCCESS)
    ret = third_expr_->fill_sql_expression(inter_expr, transformer, logical_plan, physical_plan);
  if (ret == OB_SUCCESS)
    ret = inter_expr.add_expr_item(item);
  return ret;
}

void ObMultiOpRawExpr::print(FILE* fp, int32_t level) const
{
  for(int i = 0; i < level; ++i) fprintf(fp, "    ");
  fprintf(fp, "%s\n", get_type_name(get_expr_type()));
  for (int32_t i = 0; i < exprs_.size(); i++)
  {
    exprs_[i]->print(fp, level + 1);
  }
}

int ObMultiOpRawExpr::fill_sql_expression(
    ObSqlExpression& inter_expr,
    ObTransformer *transformer,
    ObLogicalPlan *logical_plan,
    ObPhysicalPlan *physical_plan) const
{
  int ret = OB_SUCCESS;
  ExprItem item;
  item.type_ = get_expr_type();
  item.data_type_ = get_result_type();
  item.value_.int_ = exprs_.size();

  for (int32_t i = 0; ret == OB_SUCCESS && i < exprs_.size(); i++)
  {
    ret = exprs_[i]->fill_sql_expression(inter_expr, transformer, logical_plan, physical_plan);
  }
  if (ret == OB_SUCCESS)
    ret = inter_expr.add_expr_item(item);
  return ret;
}

void ObCaseOpRawExpr::print(FILE* fp, int32_t level) const
{
  for(int i = 0; i < level; ++i) fprintf(fp, "    ");
  fprintf(fp, "%s\n", get_type_name(get_expr_type()));
  if (arg_expr_)
    arg_expr_->print(fp, level + 1);
  for (int32_t i = 0; i < when_exprs_.size() && i < then_exprs_.size(); i++)
  {
    when_exprs_[i]->print(fp, level + 1);
    then_exprs_[i]->print(fp, level + 1);
  }
  if (default_expr_)
  {
    default_expr_->print(fp, level + 1);
  }
  else
  {
    for(int i = 0; i < level; ++i) fprintf(fp, "    ");
    fprintf(fp, "DEFAULT : NULL\n");
  }
}

int ObCaseOpRawExpr::fill_sql_expression(
    ObSqlExpression& inter_expr,
    ObTransformer *transformer,
    ObLogicalPlan *logical_plan,
    ObPhysicalPlan *physical_plan) const
{
  int ret = OB_SUCCESS;
  ExprItem item;
  if (arg_expr_ == NULL)
    item.type_ = T_OP_CASE;
  else
    item.type_ = T_OP_ARG_CASE;
  item.data_type_ = get_result_type();
  item.value_.int_ = (arg_expr_ == NULL ? 0 : 1) + when_exprs_.size() + then_exprs_.size();
  item.value_.int_ += (default_expr_ == NULL ? 0 : 1);

  //add wanglei [case when fix] 20160.23:b
  ExprItem item_case_begin,item_case_when,item_case_then,item_case_else,item_case_break,item_case_end,equal;
  item_case_begin.type_ = T_CASE_BEGIN;
  item_case_begin.data_type_ = ObIntType;
  item_case_begin.value_.int_ = 1;
  item_case_when.type_ = T_CASE_WHEN;
  item_case_when.data_type_ = ObIntType;
  item_case_when.value_.int_ = 1;
  item_case_then.type_ = T_CASE_THEN;
  item_case_then.data_type_ = ObIntType;
  item_case_then.value_.int_ = 1;
  item_case_else.type_ = T_CASE_ELSE;
  item_case_else.data_type_ = ObIntType;
  item_case_else.value_.int_ = 1;
  item_case_break.type_ = T_CASE_BREAK;
  item_case_break.data_type_ = ObIntType;
  item_case_break.value_.int_ = 1;
  item_case_end.type_ = T_CASE_END;
  item_case_end.data_type_ = ObIntType;
  item_case_end.value_.int_ = 1;
  equal.type_ = T_OP_EQ;
  equal.data_type_ = ObIntType;
  equal.value_.int_ = 2;
  //add wanglei :e
  if(arg_expr_ ==NULL)
  {
      ret = inter_expr.add_expr_item (item_case_begin);
      if(OB_SUCCESS != ret)
      {}
      else
      {
          for (int32_t i = 0; ret == OB_SUCCESS && i < when_exprs_.size() && i < then_exprs_.size(); i++)
          {
               //add wanglei [case when fix] 20160.23:b
              if(OB_SUCCESS !=(ret = inter_expr.add_expr_item (item_case_when)))
              {
                  TBSYS_LOG(ERROR,"add when flag failure,ret=%d",ret);
                  break;
              }else if(OB_SUCCESS !=(ret =when_exprs_[i]->fill_sql_expression(inter_expr, transformer, logical_plan, physical_plan)))
              {
                  TBSYS_LOG(ERROR,"add when exprs failure,ret=%d",ret);
                  break;
              }else if(OB_SUCCESS !=(ret = inter_expr.add_expr_item (item_case_then)))
              {
                  TBSYS_LOG(ERROR,"add then flag failure,ret=%d",ret);
                  break;
              }else if(OB_SUCCESS !=(ret = then_exprs_[i]->fill_sql_expression(inter_expr, transformer, logical_plan, physical_plan)))
              {
                  TBSYS_LOG(ERROR,"add then exprs failure,ret=%d",ret);
                  break;
              }else if(OB_SUCCESS !=(ret =inter_expr.add_expr_item (item_case_break)))
              {
                  TBSYS_LOG(ERROR,"add then exprs failure,ret=%d",ret);
                  break;
              }
              //add wanglei:e
          }
          ret = inter_expr.add_expr_item (item_case_else);
          if (ret == OB_SUCCESS && default_expr_ != NULL)
              ret = default_expr_->fill_sql_expression(inter_expr, transformer, logical_plan, physical_plan);
          if (ret == OB_SUCCESS)
          {
              if(OB_SUCCESS !=(ret =inter_expr.add_expr_item (item_case_break)))
              {
                  TBSYS_LOG(ERROR,"add then exprs failure,ret=%d",ret);
              }else if(OB_SUCCESS !=(ret =inter_expr.add_expr_item (item_case_end)))
              {
                  TBSYS_LOG(ERROR,"add then exprs failure,ret=%d",ret);
              }
          }
          if (ret == OB_SUCCESS)
              ret = inter_expr.add_expr_item(item);
          //add dolphin[case when return type]@20151231:b
          inter_expr.set_expr_type(get_result_type());
      }
  }
  else
  {
      ret = inter_expr.add_expr_item (item_case_begin);
      for (int32_t i = 0; ret == OB_SUCCESS && i < when_exprs_.size() && i < then_exprs_.size(); i++)
      {
           //add wanglei [case when fix] 20160.23:b
          if(OB_SUCCESS !=(ret = inter_expr.add_expr_item (item_case_when)))
          {
              TBSYS_LOG(ERROR,"add when flag failure,ret=%d",ret);
              break;
          }else if(OB_SUCCESS !=(ret = arg_expr_->fill_sql_expression(inter_expr, transformer, logical_plan, physical_plan)))
          {
              TBSYS_LOG(ERROR,"add arg_expr_ failure,ret=%d",ret);
              break;
          }
          else if(OB_SUCCESS !=(ret =when_exprs_[i]->fill_sql_expression(inter_expr, transformer, logical_plan, physical_plan)))
          {
              TBSYS_LOG(ERROR,"add when exprs failure,ret=%d",ret);
              break;
          }else if(OB_SUCCESS !=(ret = inter_expr.add_expr_item (equal)))
          {
              TBSYS_LOG(ERROR,"add equal failure,ret=%d",ret);
              break;
          }
          else if(OB_SUCCESS !=(ret = inter_expr.add_expr_item (item_case_then)))
          {
              TBSYS_LOG(ERROR,"add then flag failure,ret=%d",ret);
              break;
          }else if(OB_SUCCESS !=(ret = then_exprs_[i]->fill_sql_expression(inter_expr, transformer, logical_plan, physical_plan)))
          {
              TBSYS_LOG(ERROR,"add then exprs failure,ret=%d",ret);
              break;
          }else if(OB_SUCCESS !=(ret =inter_expr.add_expr_item (item_case_break)))
          {
              TBSYS_LOG(ERROR,"add then exprs failure,ret=%d",ret);
              break;
          }
          //add wanglei:e
      }
      ret = inter_expr.add_expr_item (item_case_else);
      if (ret == OB_SUCCESS && default_expr_ != NULL)
          ret = default_expr_->fill_sql_expression(inter_expr, transformer, logical_plan, physical_plan);
      if (ret == OB_SUCCESS)
      {
          if(OB_SUCCESS !=(ret =inter_expr.add_expr_item (item_case_break)))
          {
              TBSYS_LOG(ERROR,"add then exprs failure,ret=%d",ret);
          }else if(OB_SUCCESS !=(ret =inter_expr.add_expr_item (item_case_end)))
          {
              TBSYS_LOG(ERROR,"add then exprs failure,ret=%d",ret);
          }
      }
      if (ret == OB_SUCCESS)
          ret = inter_expr.add_expr_item(item);
      //add dolphin[case when return type]@20151231:b
      inter_expr.set_expr_type(get_result_type());
  }
  return ret;
}

void ObAggFunRawExpr::print(FILE* fp, int32_t level) const
{
  for(int i = 0; i < level; ++i) fprintf(fp, "    ");
  fprintf(fp, "%s\n", get_type_name(get_expr_type()));
  if (distinct_)
  {
    for(int i = 0; i < level; ++i) fprintf(fp, "    ");
    fprintf(fp, "DISTINCT\n");
  }
  if (param_expr_)
    param_expr_->print(fp, level + 1);
}

int ObAggFunRawExpr::fill_sql_expression(
    ObSqlExpression& inter_expr,
    ObTransformer *transformer,
    ObLogicalPlan *logical_plan,
    ObPhysicalPlan *physical_plan) const
{
  int ret = OB_SUCCESS;
  inter_expr.set_aggr_func(get_expr_type(), distinct_);
  if (param_expr_)
    ret = param_expr_->fill_sql_expression(inter_expr, transformer, logical_plan, physical_plan);
  //add gaojt [ListAgg][JHOBv0.1]20150104:b
  if(OB_SUCCESS == ret && listagg_param_delimeter_)
  {
     ret = listagg_param_delimeter_->fill_sql_expression(inter_expr, transformer, logical_plan, physical_plan);
  }
  /* add 20150104:e */
  return ret;
}

void ObSysFunRawExpr::print(FILE* fp, int32_t level) const
{
  for(int i = 0; i < level; ++i) fprintf(fp, "    ");
  fprintf(fp, "%s : %.*s\n", get_type_name(get_expr_type()), func_name_.length(), func_name_.ptr());
  for (int32_t i = 0; i < exprs_.size(); i++)
  {
    exprs_[i]->print(fp, level + 1);
  }
}

int ObSysFunRawExpr::fill_sql_expression(
    ObSqlExpression& inter_expr,
    ObTransformer *transformer,
    ObLogicalPlan *logical_plan,
    ObPhysicalPlan *physical_plan) const
{
  int ret = OB_SUCCESS;
  ExprItem item;
  item.type_ = T_FUN_SYS;
  item.string_ = func_name_;
  item.value_.int_ = exprs_.size();
  //add wanglei [to date optimization] 20160328:b
  bool is_can_optimization = false;
  char function_name[20];
  memset(function_name,'\0',sizeof(function_name));
  snprintf(function_name, 20, "%.*s", func_name_.length(), func_name_.ptr ());
  ObSqlExpression *se_temp= ObSqlExpression::alloc();
  if(strcmp(function_name,"to_date")==0)
  {
      const ObObj *result_temp = NULL;
      const ObRow *row = NULL;
      ObExprObj result;
      result.set_null ();
      if(exprs_.size() == 2&&!exprs_[0]->is_column())
      {
          ObSysFunRawExpr *ob_sys_fun_expr = NULL;
          if(exprs_[0]->get_expr_type() == T_FUN_SYS)
              ob_sys_fun_expr = dynamic_cast<ObSysFunRawExpr *> (exprs_[0]);
          if(OB_SUCCESS != (ret = exprs_[0]->fill_sql_expression(*se_temp, transformer, logical_plan, physical_plan)))
          {
              TBSYS_LOG(WARN,"[to date optimization] fill first expr faild!");
          }
          else if((ob_sys_fun_expr != NULL && se_temp->is_optimization())||
                  (ob_sys_fun_expr == NULL))
          {
              if(OB_SUCCESS != (ret = exprs_[1]->fill_sql_expression(*se_temp, transformer, logical_plan, physical_plan)))
              {
                  TBSYS_LOG(WARN,"[to date optimization] fill second expr faild!");
              }
              else
              {
                  if(se_temp!=NULL)
                  {
                      if (OB_SUCCESS != (ret =  se_temp->add_expr_item(item)))
                      {
                          TBSYS_LOG(WARN, "[to date optimization] faild to add expr to sql expression");
                      }
                      else if(OB_SUCCESS != (ret = se_temp->add_expr_item_end ()))
                      {
                          TBSYS_LOG(WARN, "[to date optimization] faild to add expr to sql expression");
                      }
                      else if(OB_SUCCESS != (ret = se_temp->calc(*row,result_temp)))
                      {
                          ret = OB_SUCCESS;
                          TBSYS_LOG(WARN, "[to date optimization] faild to calc the sql expression");
                      }
                      else
                      {
                          se_temp->get_result (result);
                          if(result.get_type() == ObDateType)
                          {
                              ExprItem item_temp;
                              item_temp.type_ = T_DATE_NEW;
                              item_temp.value_.datetime_ = result.get_datetime ();
                              inter_expr.add_expr_item(item_temp);
                              inter_expr.set_is_optimization(true);
                              is_can_optimization = true;
                          }
                          else
                          {
                              TBSYS_LOG(WARN,"[to date optimization]can not optimization for to date!");
                          }
                      }
                  }
                  else
                  {
                      ret = OB_ERR_POINTER_IS_NULL;
                      TBSYS_LOG(WARN,"[to date optimization] no pointer of ObSqlExpression!");
                  }
              }
          }
          else
          {
               TBSYS_LOG(WARN,"[to date optimization]can not optimization for to date!");
          }

      }
  }
  else if(strcmp(function_name,"to_timestamp")==0)
  {
      const ObObj *result_temp = NULL;
      const ObRow *row = NULL;
      ObExprObj result;
      result.set_null ();
      if(exprs_.size() == 2&&!exprs_[0]->is_column())
      {
          ObSysFunRawExpr *ob_sys_fun_expr = NULL;
          if(exprs_[0]->get_expr_type() == T_FUN_SYS)
              ob_sys_fun_expr = dynamic_cast<ObSysFunRawExpr *> (exprs_[0]);
          if(OB_SUCCESS != (ret = exprs_[0]->fill_sql_expression(*se_temp, transformer, logical_plan, physical_plan)))
          {
              TBSYS_LOG(WARN,"[to timestamp optimization] fill first expr faild!");
          }
          else if((ob_sys_fun_expr != NULL && se_temp->is_optimization())||
                  (ob_sys_fun_expr == NULL))
          {
              if(OB_SUCCESS != (ret = exprs_[1]->fill_sql_expression(*se_temp, transformer, logical_plan, physical_plan)))
              {
                  TBSYS_LOG(WARN,"[to timestamp optimization] fill second expr faild!");
              }
              else
              {
                  if(se_temp!=NULL)
                  {
                      if (OB_SUCCESS != (ret =  se_temp->add_expr_item(item)))
                      {
                          TBSYS_LOG(WARN, "[to timestamp optimization] faild to add expr to sql expression");
                      }
                      else if(OB_SUCCESS != (ret = se_temp->add_expr_item_end ()))
                      {
                          TBSYS_LOG(WARN, "[to timestamp optimization] faild to add expr to sql expression");
                      }
                      else if(OB_SUCCESS != (ret = se_temp->calc(*row,result_temp)))
                      {
                          ret = OB_SUCCESS;
                          TBSYS_LOG(WARN, "[to timestamp optimization] faild to calc the sql expression");
                      }
                      else
                      {
                          se_temp->get_result (result);
                          if(result.get_type() == ObPreciseDateTimeType)
                          {
                              ExprItem item_temp;
                              item_temp.type_ = T_DATE;
                              item_temp.value_.datetime_ = result.get_datetime ();
                              inter_expr.add_expr_item(item_temp);
                              inter_expr.set_is_optimization(true);
                              is_can_optimization = true;
                          }
                          else
                          {
                              TBSYS_LOG(WARN,"[to timestamp optimization]can not optimization for to date!");
                          }
                      }
                  }
                  else
                  {
                      ret = OB_ERR_POINTER_IS_NULL;
                      TBSYS_LOG(WARN,"[to timestamp optimization] no pointer of ObSqlExpression!");
                  }
              }
          }
          else
          {
              TBSYS_LOG(WARN,"[to timestamp optimization]can not optimization for to date!");
          }

      }
  }
  else if(strcmp(function_name,"to_char")==0)
  {
      const ObObj *result_temp = NULL;
      const ObRow *row = NULL;
      ObExprObj result;
      result.set_null ();
      if(exprs_.size() == 1&&!exprs_[0]->is_column())
      {
          ObSysFunRawExpr *ob_sys_fun_expr = NULL;
          if(exprs_[0]->get_expr_type() == T_FUN_SYS)
              ob_sys_fun_expr = dynamic_cast<ObSysFunRawExpr *>( exprs_[0]);
          if(OB_SUCCESS != (ret = exprs_[0]->fill_sql_expression(*se_temp, transformer, logical_plan, physical_plan)))
          {
              TBSYS_LOG(WARN,"[to date optimization] fill first expr faild!");
          }else if((ob_sys_fun_expr != NULL && se_temp->is_optimization())||
                   (ob_sys_fun_expr == NULL))
          {
              if(se_temp!=NULL)
              {
                  if (OB_SUCCESS != (ret =  se_temp->add_expr_item(item)))
                  {
                      TBSYS_LOG(WARN, "[to date optimization] faild to add expr to sql expression");
                  }
                  else if(OB_SUCCESS != (ret = se_temp->add_expr_item_end ()))
                  {
                      TBSYS_LOG(WARN, "[to date optimization] faild to add expr to sql expression");
                  }
                  else if(OB_SUCCESS != (ret = se_temp->calc(*row,result_temp)))
                  {
                      ret = OB_SUCCESS;
                      TBSYS_LOG(WARN, "[to date optimization] faild to calc the sql expression");
                  }
                  else
                  {
                      se_temp->get_result (result);
                      if(result.get_type() == ObVarcharType)
                      {
                          ExprItem item_temp;
                          item_temp.type_ = T_STRING;
                          item_temp.string_ = result.get_varchar ();
                          inter_expr.add_expr_item(item_temp);
                          inter_expr.set_is_optimization(true);
                          is_can_optimization = true;
                      }
                      else
                      {
                          TBSYS_LOG(WARN,"[to date optimization]can not optimization for to date!");
                      }
                  }
              }
              else
              {
                  ret = OB_ERR_POINTER_IS_NULL;
                  TBSYS_LOG(WARN,"[to date optimization] no pointer of ObSqlExpression!");
              }
          }
          else
          {
               TBSYS_LOG(WARN,"[to date optimization]can not optimization for to date!");
          }
      }
  }else if(strcmp(function_name,"last_day")==0)
  {
      const ObObj *result_temp = NULL;
      const ObRow *row = NULL;
      ObExprObj result;
      result.set_null ();
      if(exprs_.size() == 1&&!exprs_[0]->is_column())
      {
          ObSysFunRawExpr *ob_sys_fun_expr = NULL;
          if(exprs_[0]->get_expr_type() == T_FUN_SYS)
              ob_sys_fun_expr = dynamic_cast<ObSysFunRawExpr *>( exprs_[0]);
          if(OB_SUCCESS != (ret = exprs_[0]->fill_sql_expression(*se_temp, transformer, logical_plan, physical_plan)))
          {
              TBSYS_LOG(WARN,"[to date optimization] fill first expr faild!");
          }
          else if((ob_sys_fun_expr != NULL && se_temp->is_optimization())||
                  (ob_sys_fun_expr == NULL))
          {
              if(se_temp!=NULL)
              {
                  if (OB_SUCCESS != (ret =  se_temp->add_expr_item(item)))
                  {
                      TBSYS_LOG(WARN, "[to date optimization] faild to add expr to sql expression");
                  }
                  else if(OB_SUCCESS != (ret = se_temp->add_expr_item_end ()))
                  {
                      TBSYS_LOG(WARN, "[to date optimization] faild to add expr to sql expression");
                  }
                  else if(OB_SUCCESS != (ret = se_temp->calc(*row,result_temp)))
                  {
                      ret = OB_SUCCESS;
                      TBSYS_LOG(WARN, "[to date optimization] faild to calc the sql expression");
                  }
                  else
                  {
                      se_temp->get_result (result);
                      if(result.get_type() == ObDateType)
                      {
                          ExprItem item_temp;
                          item_temp.type_ = T_DATE_NEW;
                          item_temp.value_.datetime_ = result.get_datetime ();
                          inter_expr.add_expr_item(item_temp);
                          inter_expr.set_is_optimization(true);
                          is_can_optimization = true;
                      }
                      else
                      {
                          TBSYS_LOG(WARN,"[to date optimization]can not optimization for to date!");
                      }
                  }
              }
              else
              {
                  ret = OB_ERR_POINTER_IS_NULL;
                  TBSYS_LOG(WARN,"[to date optimization] no pointer of ObSqlExpression!");
              }
          }
          else
          {
              TBSYS_LOG(WARN,"[to date optimization]can not optimization for to date!");
          }
      }
  }else if(strcmp(function_name,"date_add_day")==0)
  {
      const ObObj *result_temp = NULL;
      const ObRow *row = NULL;
      ObExprObj result;
      result.set_null();
      if(!exprs_[0]->is_column())
      {
          ObSysFunRawExpr *ob_sys_fun_expr = NULL;
          if(exprs_[0]->get_expr_type() == T_FUN_SYS)
              ob_sys_fun_expr = dynamic_cast<ObSysFunRawExpr *>( exprs_[0]);
          if(OB_SUCCESS != (ret = exprs_[0]->fill_sql_expression(*se_temp, transformer, logical_plan, physical_plan)))
          {
              TBSYS_LOG(WARN,"[to date optimization] fill first expr faild!");
          }
          else if((ob_sys_fun_expr != NULL && se_temp->is_optimization())||
                  (ob_sys_fun_expr == NULL))
          {
              if(OB_SUCCESS != (ret = exprs_[1]->fill_sql_expression(*se_temp, transformer, logical_plan, physical_plan)))
              {
                  TBSYS_LOG(WARN,"[to date optimization] fill second expr faild!");
              }
              else
              {
                  if(se_temp!=NULL)
                  {
                      if (OB_SUCCESS != (ret =  se_temp->add_expr_item(item)))
                      {
                          TBSYS_LOG(WARN, "[to date optimization] faild to add expr to sql expression");
                      }
                      else if(OB_SUCCESS != (ret = se_temp->add_expr_item_end ()))
                      {
                          TBSYS_LOG(WARN, "[to date optimization] faild to add expr to sql expression");
                      }
                      else if(OB_SUCCESS != (ret = se_temp->calc(*row,result_temp)))
                      {
                          ret = OB_SUCCESS;
                          TBSYS_LOG(WARN, "[to date optimization] faild to calc the sql expression");
                      }
                      else
                      {
                          se_temp->get_result (result);
                          if(result.get_type() == ObDateType)
                          {
                              ExprItem item_temp;
                              item_temp.type_ = T_DATE_NEW;
                              item_temp.value_.datetime_ = result.get_datetime ();
                              inter_expr.add_expr_item(item_temp);
                              inter_expr.set_is_optimization(true);
                              is_can_optimization = true;
                          }
                          else
                          {
                              TBSYS_LOG(WARN,"[to date optimization]can not optimization for to date!");
                          }
                      }
                  }
                  else
                  {
                      ret = OB_ERR_POINTER_IS_NULL;
                      TBSYS_LOG(WARN,"[to date optimization] no pointer of ObSqlExpression!");
                  }
              }

          }
          else
          {
              TBSYS_LOG(WARN,"[to date optimization]can not optimization for to date!");
          }
      }
  }
  if(!is_can_optimization)
  {
      inter_expr.set_is_optimization(false);
      // 原流程：b
      for (int32_t i = 0; ret == OB_SUCCESS && i < exprs_.size(); i++)
      {
          ret = exprs_[i]->fill_sql_expression(inter_expr, transformer, logical_plan, physical_plan);
          if (ret != OB_SUCCESS)
          {
              TBSYS_LOG(WARN, "Add parameters of system function failed, param %d", i + 1);
              break;
          }
      }
      if (ret == OB_SUCCESS && (ret = inter_expr.add_expr_item(item)) != OB_SUCCESS)
      {
          TBSYS_LOG(WARN, "Add system function %.*s failed", func_name_.length(), func_name_.ptr());
      }
      //add dolphin[coalesce return type]@20151126:b
      inter_expr.set_expr_type(get_result_type());
      //TBSYS_LOG(ERROR,"dolphin::test ObSysFunRawExpr return type is:%d",get_result_type());
      // 原流程：e
  }
  ObSqlExpression::free(se_temp);
  //add wanglei :e
  return ret;
}

ObSqlRawExpr::ObSqlRawExpr()
{
  expr_id_ = OB_INVALID_ID;
  table_id_ = OB_INVALID_ID;
  column_id_ = OB_INVALID_ID;
  is_apply_ = false;
  contain_aggr_ = false;
  contain_alias_ = false;
  is_columnlized_ = false;
  expr_ = NULL;

  //add dyr [HugeTable_Join_Filter] [JHOBv0.1] 20140417:b
  is_join_optimized_ = false;
  //add 20140417:e

  //add duyr [join_without_pushdown_is_null] 20151214:b
  can_push_down_with_outerjoin_ = true;
  //add duyr 20151214:e
}

ObSqlRawExpr::ObSqlRawExpr(
    uint64_t expr_id, uint64_t table_id, uint64_t column_id, ObRawExpr* expr)
{
  table_id_ = table_id;
  expr_id_ = expr_id;
  column_id_ = column_id;
  is_apply_ = false;
  contain_aggr_ = false;
  contain_alias_ = false;
  is_columnlized_ = false;
  expr_ = expr;
  //add dyr [HugeTable_Join_Filter] [JHOBv0.1] 20140417:b
  is_join_optimized_ = false;
  //add 20140417:e

  //add duyr [join_without_pushdown_is_null] 20151214:b
  can_push_down_with_outerjoin_ = true;
  //add duyr 20151214:e
}

int ObSqlRawExpr::fill_sql_expression(
    ObSqlExpression& inter_expr,
    ObTransformer *transformer,
    ObLogicalPlan *logical_plan,
    ObPhysicalPlan *physical_plan)
{
  int ret = OB_SUCCESS;
  if (!(transformer == NULL && logical_plan == NULL && physical_plan == NULL)
    && !(transformer != NULL && logical_plan != NULL && physical_plan != NULL))
  {
    TBSYS_LOG(WARN,"(ObTransformer, ObLogicalPlan, ObPhysicalPlan) should be set together");
  }

  inter_expr.set_tid_cid(table_id_, column_id_);
  if (ret == OB_SUCCESS)
    ret = expr_->fill_sql_expression(inter_expr, transformer, logical_plan, physical_plan);
  if (ret == OB_SUCCESS)
    ret = inter_expr.add_expr_item_end();
  return ret;
}

//add qianzm [set_operation] 20151222 :b
int ObSqlRawExpr::set_result_type(common::ObObjType &result_type)
{
  int ret = OB_SUCCESS;
  expr_->set_result_type(result_type);
  return ret;
}
//add e

void ObSqlRawExpr::print(FILE* fp, int32_t level, int32_t index) const
{
  for(int i = 0; i < level; ++i) fprintf(fp, "    ");
  fprintf(fp, "<ObSqlRawExpr %d Begin>\n", index);
  for(int i = 0; i < level; ++i) fprintf(fp, "    ");
  fprintf(fp, "expr_id = %lu\n", expr_id_);
  for(int i = 0; i < level; ++i) fprintf(fp, "    ");
  if (table_id_ == OB_INVALID_ID)
    fprintf(fp, "(table_id : column_id) = (NULL : %lu)\n", column_id_);
  else
    fprintf(fp, "(table_id : column_id) = (%lu : %lu)\n", table_id_, column_id_);
  expr_->print(fp, level);
  for(int i = 0; i < level; ++i) fprintf(fp, "    ");
  fprintf(fp, "<ObSqlRawExpr %d End>\n", index);
}
