/*
 * (C) 2007-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * ob_postfix_expression.cpp is for what ...
 *
 * Version: $id: ob_postfix_expression.cpp, v 0.1 7/29/2011 14:39 xiaochu Exp $
 *
 * Authors:
 *   xiaochu <xiaochu.yh@taobao.com>
 *     - some work details if you want
 *
 * last update:
 * 2012/5/30 change name space oceanbase::common to oceanbase::sql
 *           modify calc interface to adapt ExprItem
 *
 */
//add wanglei [to_date] 20160314:b
#include "common/ob_obj_cast.h"
#include <ctype.h>
//add wanglei:e
#include "ob_postfix_expression.h"
#include "ob_type_convertor.h"
#include "common/utility.h"
#include "common/ob_tsi_factory.h"
#include "common/ob_obj_cast.h"
#include "common/ob_schema.h"
#include "sql/ob_item_type_str.h"
#include "ob_result_set.h"
#include "ob_sql_session_info.h"

using namespace oceanbase::sql;

namespace oceanbase
{
namespace sql
{
//add fanqiushi_index
ObObj& ObPostfixExpression::get_expr_by_index(int64_t index){
    //int64_t index;
    if(expr_.count()<=index){
        index=expr_.count()-1;
    }
    return expr_.at(index);
}
//add:e
//add wenghaixing&fanqiushi for fix insert bug decimal key 2014/10/11
ObObj& ObPostfixExpression::get_expr(){
    int64_t index;
    if(expr_.count()>0){
        index=expr_.count()-1;
    }
    else index=0;
    return expr_.at(index);
}

int ObPostfixExpression::fix_varchar_and_decimal(uint32_t p,uint32_t s)
{
    int ret=OB_SUCCESS;
    ObObj& obj=get_expr();
    ObObjType type=obj.get_type();

    if(type==ObDecimalType)
    {
        ObString os;
        ObDecimal od;
        obj.get_decimal(os);

        if(OB_SUCCESS!=(ret=od.from(os.ptr(),os.length())))
        {}
        else if(od.get_scale()>s)
        {
            if(OB_SUCCESS!=(ret=od.modify_value(p,s)))
            {}
            else{
                char buf[MAX_PRINTABLE_SIZE];
                memset(buf, 0, MAX_PRINTABLE_SIZE);
                int64_t length=0;
                length=od.to_string(buf, MAX_PRINTABLE_SIZE);

                ObString os2;
                os2.assign_ptr(buf,static_cast<int32_t>(length));
                ObObj obj2;
                obj2.set_decimal(os2);
                str_buf_.write_obj(obj2, &obj);
            }
        }
    }
    else if(type==ObVarcharType){
        ObString os;
        ObDecimal od;
        obj.get_varchar(os);

        if(OB_SUCCESS!=(ret=od.from(os.ptr(),os.length())))
        {}
        else if(od.get_scale()>s)
        {
            if(OB_SUCCESS!=(ret=od.modify_value(p,s)))
            {}
            else{
                char buf[MAX_PRINTABLE_SIZE];
                memset(buf, 0, MAX_PRINTABLE_SIZE);
                int64_t length=0;
                length=od.to_string(buf, MAX_PRINTABLE_SIZE);

                ObString os2;
                os2.assign_ptr(buf,static_cast<int32_t>(length));
                ObObj obj2;
                obj2.set_varchar(os2);
                str_buf_.write_obj(obj2, &obj);
            }
        }
    }
    return ret;
}
//add e
//add wenghaixing [secondary index expire info]20141229
ObObj& ObPostfixExpression::find_expire_expr()
{
    //ObObj not_find;
    for(int64_t i=0;i<expr_.count();i++)
    {
        if(expr_.at(i).get_type()==ObVarcharType)
        {
            return expr_.at(i);
        }
    }
    return not_find_;
}
//add e
bool ObPostfixExpression::ExprUtil::is_column_idx(const ObObj &obj)
{
    int64_t val = 0;
    bool ret = true;
    if (OB_SUCCESS != obj.get_int(val) || COLUMN_IDX != val)
    {
        ret = false;
    }
    return ret;
}
bool ObPostfixExpression::ExprUtil::is_const_obj(const ObObj &obj)
{
    int64_t val = 0;
    bool ret = true;
    if (OB_SUCCESS != obj.get_int(val) || CONST_OBJ != val)
    {
        ret = false;
    }
    return ret;
}
bool ObPostfixExpression::ExprUtil::is_op(const ObObj &obj)
{
    int64_t val = 0;
    bool ret = true;
    if (OB_SUCCESS != obj.get_int(val) || OP != val)
    {
        ret = false;
    }
    return ret;
}
//add gaojt [Insert_Subquery_Function] [JHOBv0.1] 20140715:b
/*Exp:check whether the type is QUERY_ID*/
bool ObPostfixExpression::ExprUtil::is_sub_query(const ObObj &obj)
{
    int64_t val = 0;
    bool ret = true;
    if (OB_SUCCESS != obj.get_int(val) || QUERY_ID != val)
    {
        ret = false;
    }
    return ret;
}
//add 20140715:e
bool ObPostfixExpression::ExprUtil::is_end(const ObObj &obj)
{
    int64_t val = 0;
    bool ret = true;
    if (OB_SUCCESS != obj.get_int(val) || END != val)
    {
        ret = false;
    }
    return ret;
}
bool ObPostfixExpression::ExprUtil::is_op_of_type(const ObObj &obj, ObItemType type)
{
    int64_t val = 0;
    bool ret = true;
    if (OB_SUCCESS != obj.get_int(val) || type != val)
    {
        ret = false;
    }
    return ret;
}

bool ObPostfixExpression::ExprUtil::is_value(const ObObj &obj, int64_t value)
{
    int64_t val = 0;
    bool ret = true;
    if (OB_SUCCESS != obj.get_int(val) || value != val)
    {
        ret = false;
    }
    return ret;
}


int ExprItem::assign(const common::ObObj &obj)
{
    int ret = OB_SUCCESS;
    switch(obj.get_type())
    {
    case ObNullType:
        type_ = T_NULL;
        break;
    case ObIntType:
        type_ = T_INT;
        obj.get_int(value_.int_);
        break;
        //add lijianqiang [INT_32] 20151008:b
    case ObInt32Type:
        type_ = T_INT32;
        obj.get_int32(value_.int32_);
        break;
        //add 20151008:e
    case ObFloatType:
        type_ = T_FLOAT;
        obj.get_float(value_.float_);
        break;
    case ObDoubleType:
        type_ = T_DOUBLE;
        obj.get_double(value_.double_);
        break;
        //add peiouya [DATE_TIME] 20150906:b
    case ObDateType:
        type_ = T_DATE_NEW;
        obj.get_date(value_.datetime_);
        break;
    case ObTimeType:
        type_ = T_TIME;
        obj.get_time(value_.datetime_);
        break;
        //add 20150906:e
        //add peiouya [DATE_TIME] 20150908:b
    case ObIntervalType:
        type_ = T_DATE;
        obj.get_interval(value_.datetime_);
        break;
        //add 20150908:e
    case ObPreciseDateTimeType:
        type_ = T_DATE;
        obj.get_precise_datetime(value_.datetime_);
        break;
    case ObVarcharType:
        type_ = T_STRING;
        obj.get_varchar(string_);
        break;
    case ObBoolType:
        type_ = T_BOOL;
        obj.get_bool(value_.bool_);
        break;
    case ObDecimalType:
        //modify fanqiushi DECIMAL OceanBase_BankCommV0.3 2014_7_9:b
        /*
           *该函数是将参数Obj里面的值存到ExprItem里面
           *参数obj是中缀表达式里面的值，ExprItem里面的值最后会写到后缀表达式里�?         *
           *因为中缀表达式里我把decimal的值按照varchar存的，所以在这里存到ExprItem里面的值也是按照varcahr存的
           *
           *get_varchar_d函数是我们自己写的，主要功能跟ObObj的get_varchar函数相同。不同之处是get_varchar必须是type�?           *ObVarcharType的obj才能调用。而调用get_varchar_d函数的ObObj可以是其他类�?           */
        //ret = OB_NOT_SUPPORTED;      //old code
        type_ = T_DECIMAL;
        obj.get_varchar_d(string_);
        //modify:e
        break;
    default:
        ret = OB_ERR_UNEXPECTED;
        break;
    }
    return ret;
}

/*     初始化数学运算操作调用表 */
op_call_func_t ObPostfixExpression::call_func[T_MAX_OP - T_MIN_OP - 1] = {
    /*   WARNING: 下面的顺序不可以调换�?           *   需要与(ob_item_tpye.h) ExprType enum定义对应
           */
    ObPostfixExpression::minus_func, /* T_OP_NEG */
    ObPostfixExpression::plus_func, /* T_OP_POS */
    ObPostfixExpression::add_func, /* T_OP_ADD */
    ObPostfixExpression::sub_func, /* T_OP_MINUS */
    ObPostfixExpression::mul_func, /* T_OP_MUL */
    ObPostfixExpression::div_func, /* T_OP_DIV */
    ObPostfixExpression::nop_func, /* TODO: T_OP_POW */
    ObPostfixExpression::mod_func, /* T_OP_REM */
    ObPostfixExpression::mod_func, /* T_OP_MOD */
    ObPostfixExpression::eq_func,  /* T_OP_EQ */
    ObPostfixExpression::le_func,  /* T_OP_LE */
    ObPostfixExpression::lt_func,  /* T_OP_LT */
    ObPostfixExpression::ge_func,  /* T_OP_GE */
    ObPostfixExpression::gt_func,  /* T_OP_GT */
    ObPostfixExpression::neq_func, /* T_OP_NE */
    ObPostfixExpression::is_func,  /* T_OP_IS */
    ObPostfixExpression::is_not_func,/* T_OP_IS_NOT */
    ObPostfixExpression::btw_func, /* T_OP_BTW */
    ObPostfixExpression::not_btw_func, /* T_OP_NOT_BTW */
    ObPostfixExpression::like_func,/* T_OP_LIKE */
    ObPostfixExpression::not_like_func, /* T_OP_NOT_LIKE */
    ObPostfixExpression::not_func, /* T_OP_NOT */
    ObPostfixExpression::and_func, /* T_OP_AND */
    ObPostfixExpression::or_func,  /* T_OP_OR */
    ObPostfixExpression::in_func, /*  T_OP_IN */
    ObPostfixExpression::not_in_func, /* T_OP_NOT_IN */
    ObPostfixExpression::arg_case_func, /* T_OP_AGR_CASE */
    ObPostfixExpression::case_func, /* T_OP_CASE */
    ObPostfixExpression::row_func, /* T_OP_ROW */
    ObPostfixExpression::nop_func, /* TODO: T_OP_EXISTS */
    ObPostfixExpression::concat_func, /* T_OP_CNN */
    ObPostfixExpression::nop_func, // T_FUN_SYS
    ObPostfixExpression::left_param_end_func, // T_OP_LEFT_PARAM_END
    ObPostfixExpression::cast_thin_func,  //add  peiouya [EXPIRE_INFO_NOT_ARITHMETIC_FIX] 20160608
};

// system function table
op_call_func_t ObPostfixExpression::SYS_FUNCS_TAB[SYS_FUNC_NUM] =
{
    ObPostfixExpression::sys_func_length, // SYS_FUNC_LENGTH
    ObPostfixExpression::sys_func_substr, // SYS_FUNC_SUBSTR
    ObPostfixExpression::sys_func_cast,             // SYS_FUNC_CAST
    ObPostfixExpression::sys_func_cur_user,         // SYS_FUNC_CUR_USER
    ObPostfixExpression::sys_func_trim,             // SYS_FUNC_TRIM
    ObPostfixExpression::sys_func_lower,            // SYS_FUNC_LOWER
    ObPostfixExpression::sys_func_upper,            // SYS_FUNC_UPPER
    ObPostfixExpression::sys_func_coalesce,         // SYS_FUNC_COALESCE
    ObPostfixExpression::sys_func_hex,         // SYS_FUNC_HEX
    ObPostfixExpression::sys_func_unhex,         // SYS_FUNC_UNHEX
    ObPostfixExpression::sys_func_ip_to_int,         // SYS_FUNC_IP_TO_INT
    ObPostfixExpression::sys_func_int_to_ip,         // SYS_FUNC_INT_TO_IP
    ObPostfixExpression::sys_func_greatest,         // SYS_FUNC_GREATEST
    ObPostfixExpression::sys_func_least, // SYS_FUNC_LEAST
    //add wanglei [last_day] 20160311:b
    ObPostfixExpression::sys_func_last_day,
    //add wanglei [to_char] 20160311:b
    ObPostfixExpression::sys_func_to_char,
    //add wanglei [to_date] 20160311:b
    ObPostfixExpression::sys_func_to_date,
    //add wanglei [to_timestamp] 20160311:b
    ObPostfixExpression::sys_func_to_timestamp,
    //add qianzhaoming [decode] 20151029:b
    ObPostfixExpression::sys_func_decode,         // SYS_FUNC_DECODE
    //add :e
    //add xionghui [floor and ceil] 20151214:b
    ObPostfixExpression::sys_func_floor,         // SYS_FUNC_FLOOR
    ObPostfixExpression::sys_func_ceil,          // SYS_FUNC_CEIL
    //add 20151214:e
    //add xionghui [round] 20150126:b
    ObPostfixExpression::sys_func_round,         // SYS_FUNC_ROUND
    //add 20150126 e:
    //add wuna [datetime func] 20150828:b
    ObPostfixExpression::sys_func_year,
    ObPostfixExpression::sys_func_month,
    ObPostfixExpression::sys_func_day,
    ObPostfixExpression::sys_func_days,
    ObPostfixExpression::sys_func_date_add_year,
    ObPostfixExpression::sys_func_date_sub_year,
    ObPostfixExpression::sys_func_date_add_month,
    ObPostfixExpression::sys_func_date_sub_month,
    ObPostfixExpression::sys_func_date_add_day,
    ObPostfixExpression::sys_func_date_sub_day,
    ObPostfixExpression::sys_func_date_add_hour,
    ObPostfixExpression::sys_func_date_sub_hour,
    ObPostfixExpression::sys_func_date_add_minute,
    ObPostfixExpression::sys_func_date_sub_minute,
    ObPostfixExpression::sys_func_date_add_second,
    ObPostfixExpression::sys_func_date_sub_second,
    ObPostfixExpression::sys_func_date_add_microsecond,
    ObPostfixExpression::sys_func_date_sub_microsecond,
    //add 20150828:e
    //add liuzy [datetime func] 20150901:b
    ObPostfixExpression::sys_func_hour,
    ObPostfixExpression::sys_func_minute,
    ObPostfixExpression::sys_func_second,
    //add 20150901:e
    //add lijianqiang [replace_fuction] 20151102:b
    ObPostfixExpression::sys_func_replace,
    //add 20151102:e
};

const char* const ObPostfixExpression::SYS_FUNCS_NAME[SYS_FUNC_NUM] =
{
    "length",
    "substr",
    "cast",
    "current_user",
    "trim",
    "lower",
    "upper",
    "coalesce",
    "hex",
    "unhex",
    "ip2int",
    "int2ip",
    "greatest",
    "least",
    //add wanglei [last_day] 20160311:b
    "last_day",
    //add wanglei [to_char] 20160311:b
    "to_char",
    //add wanglei [to_date] 20160311:b
    "to_date",
    //add wanglei:e
    //add wanglei [to_timestamp] 20160311:b
    "to_timestamp",
    //add wanglei:e
    //add qianzhaoming [decode] 20151029:b
    "decode",
    //add :e
    //add xionghui [floor and ceil] 20151214:b
    "floor",
    "ceil",
    //add 20151214:e
    //add xionghui [round] 20150126:b
    "round",
    //add 20150126 e:
    //add wuna [datetime func] 20150828:b
    "year",
    "month",
    "day",
    "days",
    "date_add_year",
    "date_sub_year",
    "date_add_month",
    "date_sub_month",
    "date_add_day",
    "date_sub_day",
    "date_add_hour",
    "date_sub_hour",
    "date_add_minute",
    "date_sub_minute",
    "date_add_second",
    "date_sub_second",
    "date_add_microsecond",
    "date_sub_microsecond",
    //add 20150828:e
    //add liuzy [datetime func] 20150901:b
    "hour",
    "minute",
    "second",
    //add 20150901:e
    "replace" //add lijianqiang [replace_function] 20151102
};

int32_t ObPostfixExpression::SYS_FUNCS_ARGS_NUM[SYS_FUNC_NUM] =
{
    1,
    TWO_OR_THREE,
    //modify fanqiushi DECIMAL OceanBase_BankCommV0.3 2014_7_9:b
    //2,   //old code
    TWO_OR_FOUR,
    //modify:e
    0,
    3,/*trim*/
    1,
    1,
    MORE_THAN_ZERO,
    1,
    1,
    1,
    1,
    MORE_THAN_ZERO,
    MORE_THAN_ZERO,
    //add wanglei [last_day] 20160314:b
    1,
    //add wanglei:e
    //add wanglei [to_char] 20160314:b
    1,
    //add wanglei:e
    //add wanglei [to_date] 20160314:b
    2,
    //add wanglei:e
    //add wanglei [to_timestamp] 20160314:b
    2,
    //add wanglei:e
    //add qianzhaoming 20151029:b
    MORE_THAN_ZERO,/*decode*/
    //add xionghui [floor and ceil] 20151231:b
    1,/*floor*/
    1,/*ceil*/
    //add :e
    2,/*round*/ //add xionghui [round] 20150126:b
    //add wuna [datetime func] 20150828:b
    1,
    1,
    1,
    1,
    2,
    2,
    2,
    2,
    2,
    2,
    2,
    2,
    2,
    2,
    2,
    2,
    2,
    2,
    //add 20150828:e
    //add liuzy [datetime func] 20150901:b
    1,
    1,
    1,
    //add 20150901:e
    TWO_OR_THREE, //add lijianqiang [replace_function] 20151102
};

ObPostfixExpression::ObPostfixExpression()
    :expr_(64*1024, ModulePageAllocator(ObModIds::OB_SQL_EXPR)),
      stack_(NULL),
      did_int_div_as_double_(false),
      str_buf_(ObModIds::OB_SQL_EXPR, DEF_STRING_BUF_SIZE),
      owner_op_(NULL),
      calc_buf_(ObModIds::OB_SQL_EXPR_CALC, DEF_STRING_BUF_SIZE)
    ,temp_expr_(64*1024, ModulePageAllocator(ObModIds::OB_SQL_EXPR)),//add tianz [SubQuery_for_Instmt] [JHOBv0.1] 20140408
      bloom_filter_(NULL), //add tianz [SubQuery_for_Instmt] [JHOBv0.1] 20140408
      post_in_ada_(NULL)//add dolphin[in post-expression optimization]@20150915
{
}

ObPostfixExpression::~ObPostfixExpression()
{
    //add dolphin[in post-expression optimization]@20150915
    //right_hash_.destroy();
    if(NULL != post_in_ada_)
    {
        post_in_ada_->~ObPostfixInAdaptor();
        ob_free(post_in_ada_);
        post_in_ada_ = NULL;
    }

}

ObPostfixExpression& ObPostfixExpression::operator=(const ObPostfixExpression &other)
{
    int ret = OB_SUCCESS;
    int i = 0;
    this->expr_.clear();
    this->calc_buf_.clear();
  //add dolphin[in post-expression optimization]@20150925:b
  if(NULL != other.post_in_ada_)
  {
    other.post_in_ada_->~ObPostfixInAdaptor();
    ob_free(other.post_in_ada_);
    //other.post_in_ada_ = NULL;
  }
  //other.post_buf_.clear();
 free_post_in();
  //add:e
    if (OB_SUCCESS != (ret = str_buf_.reset()))
    {
        TBSYS_LOG(WARN, "fail to reset string buffer");
    }
    else if (&other != this)
    {
        ObObj obj;
        this->set_owner_op(other.owner_op_);
        expr_.reserve(other.expr_.count());
        for (i = 0; i < other.expr_.count(); i++)
        {
            if (ObVarcharType == other.expr_[i].get_type())
            {
                if(OB_SUCCESS != (ret = str_buf_.write_obj(other.expr_[i], &obj)))
                {
                    TBSYS_LOG(ERROR, "fail to write object to string buffer. ret=%d", ret);
                }
                else if (OB_SUCCESS != (ret = expr_.push_back(obj)))
                {
                    TBSYS_LOG(WARN, "failed to add item, err=%d", ret);
                }
            }
            //add fanqiushi DECIMAL OceanBase_BankCommV0.3 2014_7_19:b
            else if(ObDecimalType==other.expr_[i].get_type())
            {
                if(OB_SUCCESS != (ret = str_buf_.write_obj_varchar_to_decimal(other.expr_[i], &obj)))
                {
                    TBSYS_LOG(ERROR, "fail to write object to string buffer. ret=%d", ret);
                }
                else
                {
                    //add fanqiushi DECIMAL OceanBase_BankCommV0.3 2014_7_19:b
                    obj.set_precision(other.expr_[i].get_precision());
                    obj.set_scale(other.expr_[i].get_scale());
                    //add:e
                    if (OB_SUCCESS != (ret = expr_.push_back(obj)))
                    {
                        TBSYS_LOG(WARN, "failed to add item, err=%d", ret);
                    }
                }

            }
            //add:e
            else
            {
                if (OB_SUCCESS != (ret = expr_.push_back(other.expr_[i])))
                {
                    TBSYS_LOG(WARN, "failed to add item, err=%d", ret);
                }
            }
        }
        //add tianz [SubQuery_for_Instmt] [JHOBv0.1] 20140408:b
        this->bloom_filter_ = other.bloom_filter_;
        //add 20140408 :e
        //add dolphin[coalesce return type]@20151126:b
        expr_type_ = other.expr_type_;
    }
    return *this;
}

int ObPostfixExpression::get_sys_func(const common::ObString &sys_func, ObSqlSysFunc &func_type) const
{
#define OB_POSTFIX_EXPRESSION_GET_SYS_FUNC(SYS_FUNC_ID) \
    else if (sys_func.length() == static_cast<int64_t>(strlen(SYS_FUNCS_NAME[SYS_FUNC_ID])) \
    && 0 == strncasecmp(SYS_FUNCS_NAME[SYS_FUNC_ID], sys_func.ptr(), sys_func.length())) \
    { \
    func_type = SYS_FUNC_ID; \
}

    int ret = OB_SUCCESS;
    if (0)
    {
        // if (0) so that the macro works
    }
    OB_POSTFIX_EXPRESSION_GET_SYS_FUNC(SYS_FUNC_LENGTH)
            OB_POSTFIX_EXPRESSION_GET_SYS_FUNC(SYS_FUNC_SUBSTR)
            OB_POSTFIX_EXPRESSION_GET_SYS_FUNC(SYS_FUNC_CAST)
            OB_POSTFIX_EXPRESSION_GET_SYS_FUNC(SYS_FUNC_CUR_USER)
            OB_POSTFIX_EXPRESSION_GET_SYS_FUNC(SYS_FUNC_TRIM)
            OB_POSTFIX_EXPRESSION_GET_SYS_FUNC(SYS_FUNC_LOWER)
            OB_POSTFIX_EXPRESSION_GET_SYS_FUNC(SYS_FUNC_UPPER)
            OB_POSTFIX_EXPRESSION_GET_SYS_FUNC(SYS_FUNC_COALESCE)
            OB_POSTFIX_EXPRESSION_GET_SYS_FUNC(SYS_FUNC_HEX)
            OB_POSTFIX_EXPRESSION_GET_SYS_FUNC(SYS_FUNC_UNHEX)
            OB_POSTFIX_EXPRESSION_GET_SYS_FUNC(SYS_FUNC_IP_TO_INT)
            OB_POSTFIX_EXPRESSION_GET_SYS_FUNC(SYS_FUNC_INT_TO_IP)
            OB_POSTFIX_EXPRESSION_GET_SYS_FUNC(SYS_FUNC_GREATEST)
            OB_POSTFIX_EXPRESSION_GET_SYS_FUNC(SYS_FUNC_LEAST)
            //add wanglei [last_day] 20160311:b
            OB_POSTFIX_EXPRESSION_GET_SYS_FUNC(SYS_FUNC_LAST_DAY)
            //add wanglei:e
            //add wanglei [to_char] 20160311:b
            OB_POSTFIX_EXPRESSION_GET_SYS_FUNC(SYS_FUNC_TO_CHAR)
            //add wanglei:e
            //add wanglei [to_date] 20160311:b
            OB_POSTFIX_EXPRESSION_GET_SYS_FUNC(SYS_FUNC_TO_DATE)
            //add wanglei:e
            //add wanglei [to_timestamp] 20160311:b
            OB_POSTFIX_EXPRESSION_GET_SYS_FUNC(SYS_FUNC_TO_TIMESTAMP)
            //add wanglei:e
            //add qianzhaoming [decode] 20151029:b
            OB_POSTFIX_EXPRESSION_GET_SYS_FUNC(SYS_FUNC_DECODE)
            //add :e
            //add xionghui [floor and ceil] 20151214:b
            OB_POSTFIX_EXPRESSION_GET_SYS_FUNC(SYS_FUNC_FLOOR)
            OB_POSTFIX_EXPRESSION_GET_SYS_FUNC(SYS_FUNC_CEIL)
            //add 20151214:e
            //add xionghui [round] 20150126:b
            OB_POSTFIX_EXPRESSION_GET_SYS_FUNC(SYS_FUNC_ROUND)
            //add e:
            //add wuna [datetime func] 20150828:b
            OB_POSTFIX_EXPRESSION_GET_SYS_FUNC(SYS_FUNC_YEAR)
            OB_POSTFIX_EXPRESSION_GET_SYS_FUNC(SYS_FUNC_MONTH)
            OB_POSTFIX_EXPRESSION_GET_SYS_FUNC(SYS_FUNC_DAY)
            OB_POSTFIX_EXPRESSION_GET_SYS_FUNC(SYS_FUNC_DAYS)
            OB_POSTFIX_EXPRESSION_GET_SYS_FUNC(SYS_FUNC_DATE_ADD_YEAR)
            OB_POSTFIX_EXPRESSION_GET_SYS_FUNC(SYS_FUNC_DATE_SUB_YEAR)
            OB_POSTFIX_EXPRESSION_GET_SYS_FUNC(SYS_FUNC_DATE_ADD_MONTH)
            OB_POSTFIX_EXPRESSION_GET_SYS_FUNC(SYS_FUNC_DATE_SUB_MONTH)
            OB_POSTFIX_EXPRESSION_GET_SYS_FUNC(SYS_FUNC_DATE_ADD_DAY)
            OB_POSTFIX_EXPRESSION_GET_SYS_FUNC(SYS_FUNC_DATE_SUB_DAY)
            OB_POSTFIX_EXPRESSION_GET_SYS_FUNC(SYS_FUNC_DATE_ADD_HOUR)
            OB_POSTFIX_EXPRESSION_GET_SYS_FUNC(SYS_FUNC_DATE_SUB_HOUR)
            OB_POSTFIX_EXPRESSION_GET_SYS_FUNC(SYS_FUNC_DATE_ADD_MINUTE)
            OB_POSTFIX_EXPRESSION_GET_SYS_FUNC(SYS_FUNC_DATE_SUB_MINUTE)
            OB_POSTFIX_EXPRESSION_GET_SYS_FUNC(SYS_FUNC_DATE_ADD_SECOND)
            OB_POSTFIX_EXPRESSION_GET_SYS_FUNC(SYS_FUNC_DATE_SUB_SECOND)
            OB_POSTFIX_EXPRESSION_GET_SYS_FUNC(SYS_FUNC_DATE_ADD_MICROSECOND)
            OB_POSTFIX_EXPRESSION_GET_SYS_FUNC(SYS_FUNC_DATE_SUB_MICROSECOND)
            //add 20150828:e
            //add liuzy [datetime func] 20150901:b
            OB_POSTFIX_EXPRESSION_GET_SYS_FUNC(SYS_FUNC_HOUR)
            OB_POSTFIX_EXPRESSION_GET_SYS_FUNC(SYS_FUNC_MINUTE)
            OB_POSTFIX_EXPRESSION_GET_SYS_FUNC(SYS_FUNC_SECOND)
            //add 20150901:e
            //add lijianqiang [replace_fuction] 20151102:b
            OB_POSTFIX_EXPRESSION_GET_SYS_FUNC(SYS_FUNC_REPLACE)
            //add 20151102:e
            else
    {
        ret = OB_ERR_UNKNOWN_SYS_FUNC;
    }
    return ret;
#undef OB_POSTFIX_EXPRESSION_GET_SYS_FUNC
}

int ObPostfixExpression::add_expr_obj(const ObObj &obj)
{
    int ret = OB_SUCCESS;
    ObObj obj2;
    if (obj.get_type() == ObVarcharType)
    {
        if (OB_SUCCESS != (ret = str_buf_.write_obj(obj, &obj2)))
        {
            TBSYS_LOG(WARN, "fail to write object to string buffer. err=%d", ret);
        }
        else if (OB_SUCCESS != (ret = expr_.push_back(obj2)))
        {
            TBSYS_LOG(WARN, "fail to push object to expr array. err=%d", ret);
        }
    }
    else if (OB_SUCCESS != (ret = expr_.push_back(obj)))
    {
        TBSYS_LOG(WARN, "fail to push object to expr array. err=%d", ret);
    }
    return ret;
}
//add dolphin[coalesce return type]@20151126:b
void ObPostfixExpression::set_expr_type(const ObObjType& type)
{
    expr_type_ = type;
}

ObObjType ObPostfixExpression::get_expr_type() const
{
    return expr_type_;
}
//add:e

int ObPostfixExpression::add_expr_item(const ExprItem &item)
{
    int ret = OB_SUCCESS;
    ObObj item_type;
    ObObj obj, obj2;
    ObSqlSysFunc sys_func;
    switch(item.type_)
    {
    case T_STRING:
    case T_BINARY:
        item_type.set_int(CONST_OBJ);
        obj.set_varchar(item.string_);
        if (OB_SUCCESS != (ret = str_buf_.write_obj(obj, &obj2)))
        {
            TBSYS_LOG(WARN, "fail to write object to string buffer. err=%d", ret);
        }
        else if (OB_SUCCESS != (ret = expr_.push_back(item_type)))
        {}
        else if (OB_SUCCESS != (ret = expr_.push_back(obj2)))
        {}
        break;
    case T_FLOAT:
        item_type.set_int(CONST_OBJ);
        obj.set_float(item.value_.float_);
        if (OB_SUCCESS != (ret = expr_.push_back(item_type)))
        {}
        else if (OB_SUCCESS != (ret = expr_.push_back(obj)))
        {}
        break;
    case T_DOUBLE:
        item_type.set_int(CONST_OBJ);
        obj.set_double(item.value_.double_);
        if (OB_SUCCESS != (ret = expr_.push_back(item_type)))
        {}
        else if (OB_SUCCESS != (ret = expr_.push_back(obj)))
        {}
        break;
    case T_DECIMAL:
        //modify fanqiushi DECIMAL OceanBase_BankCommV0.3 2014_7_9:b
        /*
          *     生成后缀表达式，将值存到obj2里面，再把obj2放到后缀表达式的数组expr_�?          *     这里obj2的类型是ObDecimalType.
          */
        item_type.set_int(CONST_OBJ);
        obj.set_varchar(item.string_);
        if (OB_SUCCESS != (ret = str_buf_.write_obj_varchar_to_decimal(obj, &obj2)))
        {
            TBSYS_LOG(WARN, "fail to write object to string buffer. err=%d", ret);
        }
        else if (OB_SUCCESS != (ret = expr_.push_back(item_type)))
        {}
        else if (OB_SUCCESS != (ret = expr_.push_back(obj2)))
        {}
        //modify:e
        break;
    case T_INT:
        item_type.set_int(CONST_OBJ);
        obj.set_int(item.value_.int_);
        if (OB_SUCCESS != (ret = expr_.push_back(item_type)))
        {}
        else if (OB_SUCCESS != (ret = expr_.push_back(obj)))
        {}
        break;
        //add lijianqiang [INT_32] 20151008:b
    case T_INT32:
        item_type.set_int(CONST_OBJ);
        obj.set_int32(item.value_.int32_);
        if (OB_SUCCESS != (ret = expr_.push_back(item_type)))
        {}
        else if (OB_SUCCESS != (ret = expr_.push_back(obj)))
        {}
        break;
        //add 20151008:e
    case T_BOOL:
        item_type.set_int(CONST_OBJ);
        obj.set_bool(item.value_.bool_);
        if (OB_SUCCESS != (ret = expr_.push_back(item_type)))
        {}
        else if (OB_SUCCESS != (ret = expr_.push_back(obj)))
        {}
        break;
    case T_DATE:
        item_type.set_int(CONST_OBJ);
        obj.set_precise_datetime(item.value_.datetime_);
        if (OB_SUCCESS != (ret = expr_.push_back(item_type)))
        {}
        else if (OB_SUCCESS != (ret = expr_.push_back(obj)))
        {}
        break;
        //add peiouya [DATE_TIME] 20150912:b
    case T_DATE_NEW:
        item_type.set_int(CONST_OBJ);
        obj.set_date(item.value_.datetime_);
        if (OB_SUCCESS != (ret = expr_.push_back(item_type)))
        {}
        else if (OB_SUCCESS != (ret = expr_.push_back(obj)))
        {}
        break;
    case T_TIME:
        item_type.set_int(CONST_OBJ);
        obj.set_time(item.value_.datetime_);
        if (OB_SUCCESS != (ret = expr_.push_back(item_type)))
        {}
        else if (OB_SUCCESS != (ret = expr_.push_back(obj)))
        {}
        break;
        //add 20150912:e
    case T_CUR_TIME:
        item_type.set_int(CUR_TIME_OP);
        obj.set_int(item.value_.int_);
        if (OB_SUCCESS != (ret = expr_.push_back(item_type)))
        {}
        else if (OB_SUCCESS != (ret = expr_.push_back(obj)))
        {}
        break;
        //add liuzy [datetime func] 20150910:b
    case T_CUR_DATE:
        item_type.set_int(CUR_DATE_OP);
        obj.set_int(item.value_.int_);
        if (OB_SUCCESS != (ret = expr_.push_back(item_type)))
        {}
        else if (OB_SUCCESS != (ret = expr_.push_back(obj)))
        {}
        break;
    case T_CUR_TIME_HMS:
        item_type.set_int(CUR_TIME_HMS_OP);
        obj.set_int(item.value_.int_);
        if (OB_SUCCESS != (ret = expr_.push_back(item_type)))
        {}
        else if (OB_SUCCESS != (ret = expr_.push_back(obj)))
        {}
        break;
        //add 20150910:e
    case T_QUESTIONMARK:
        item_type.set_int(PARAM_IDX);
        obj.set_int(item.value_.int_);
        if (OB_SUCCESS != (ret = expr_.push_back(item_type)))
        {}
        else if (OB_SUCCESS != (ret = expr_.push_back(obj)))
        {}
        break;
    case T_SYSTEM_VARIABLE:
    case T_TEMP_VARIABLE:
        item_type.set_int(item.type_ == T_SYSTEM_VARIABLE ? SYSTEM_VAR : TEMP_VAR);
        obj.set_varchar(item.string_);
        if (OB_SUCCESS != (ret = str_buf_.write_obj(obj, &obj2)))
        {
            TBSYS_LOG(WARN, "fail to write variable name to string buffer. err=%d", ret);
        }
        else if (OB_SUCCESS != (ret = expr_.push_back(item_type)))
        {}
        else if (OB_SUCCESS != (ret = expr_.push_back(obj2)))
        {}
        break;
    case T_NULL:
        item_type.set_int(CONST_OBJ);
        obj.set_null();
        if (OB_SUCCESS != (ret = expr_.push_back(item_type)))
        {}
        else if (OB_SUCCESS != (ret = expr_.push_back(obj)))
        {}
        break;
    case T_REF_COLUMN:
        item_type.set_int(COLUMN_IDX);
        obj.set_int(item.value_.cell_.tid);
        obj2.set_int(item.value_.cell_.cid);
        if (OB_SUCCESS != (ret = expr_.push_back(item_type)))
        {}
        else if (OB_SUCCESS != (ret = expr_.push_back(obj)))
        {}
        else if (OB_SUCCESS != (ret = expr_.push_back(obj2)))
        {}
        break;
    case T_REF_QUERY:
        //mod tianz [sub select] 20140331:b
        //  TBSYS_LOG(ERROR, "TODO... not implement yet");
        //  ret = OB_NOT_SUPPORTED;
        item_type.set_int(QUERY_ID);
        obj.set_int(item.value_.int_);//sub select column num
        if(OB_SUCCESS != (ret = expr_.push_back(item_type)))
        {}
        else if (OB_SUCCESS != (ret = expr_.push_back(obj)))
        {}
        //mod 20140331:e
        break;
    case T_OP_EXISTS:
    case T_OP_POS:
    case T_OP_NEG:
    case T_OP_ADD:
    case T_OP_MINUS:
    case T_OP_MUL:
    case T_OP_DIV:
    case T_OP_REM:
    case T_OP_POW:
    case T_OP_MOD:
    case T_OP_LE:
    case T_OP_LT:
    case T_OP_EQ:
    case T_OP_GE:
    case T_OP_GT:
    case T_OP_NE:
    case T_OP_LIKE:
    case T_OP_NOT_LIKE:
    case T_OP_AND:
    case T_OP_OR:
    case T_OP_NOT:
    case T_OP_IS:
    case T_OP_IS_NOT:
    case T_OP_BTW:
    case T_OP_NOT_BTW:
    case T_OP_CNN:
    case T_OP_IN:
    case T_OP_NOT_IN:
    case T_OP_ARG_CASE:
    case T_OP_CASE:
    case T_OP_ROW:
    case T_OP_LEFT_PARAM_END:
    case T_OP_CAST_THIN:  //add  peiouya [EXPIRE_INFO_NOT_ARITHMETIC_FIX] 20160608
        item_type.set_int(OP);
        obj.set_int(item.type_); // op type
        obj2.set_int(item.value_.int_); // operands count
        if (OB_SUCCESS != (ret = expr_.push_back(item_type)))
        {}
        else if (OB_SUCCESS != (ret = expr_.push_back(obj)))
        {}
        else if (OB_SUCCESS != (ret = expr_.push_back(obj2)))
        {}
        break;
    case T_CUR_TIME_OP:
        item_type.set_int(UPS_TIME_OP);
        if (OB_SUCCESS != (ret = expr_.push_back(item_type)))
        {
            TBSYS_LOG(WARN, "failed to push item_type, ret=%d", ret);
        }
        break;
    case T_FUN_SYS:
        item_type.set_int(OP);
        obj.set_int(item.type_); // system function
        obj2.set_int(item.value_.int_); // operands count
        if (OB_SUCCESS != (ret = get_sys_func(item.string_, sys_func)))
        {
            TBSYS_LOG(WARN, "unknown system function=%.*s", item.string_.length(), item.string_.ptr());
        }
        else if (OB_SUCCESS != (ret = expr_.push_back(item_type)))
        {}
        else if (OB_SUCCESS != (ret = expr_.push_back(obj)))
        {}
        else if (OB_SUCCESS != (ret = expr_.push_back(obj2)))
        {}
        else
        {
            obj2.set_int(sys_func);
            ret = expr_.push_back(obj2);
        }
        break;
        //add wanglei [case when fix] 20160323:b
    case T_CASE_BEGIN:
    case T_CASE_WHEN:
    case T_CASE_THEN:
    case T_CASE_ELSE:
    case T_CASE_BREAK:
    case T_CASE_END:
        item_type.set_int(OP);
        obj.set_int(item.type_); // op type
        obj2.set_int(1); // operands count
        if (OB_SUCCESS != (ret = expr_.push_back(item_type)))
        {}
        else if (OB_SUCCESS != (ret = expr_.push_back(obj)))
        {}
        else if (OB_SUCCESS != (ret = expr_.push_back(obj2)))
        {}
        break;
        //add wanglei :e
    default:
        TBSYS_LOG(WARN, "unknown expr item type=%d", item.type_);
        ret = OB_INVALID_ARGUMENT;
        break;
    }
    return ret;
}

int ObPostfixExpression::add_expr_item_end()
{
    int ret = OB_SUCCESS;
    ObObj obj;
    obj.set_int(END);
    if (OB_SUCCESS != (ret = expr_.push_back(obj)))
    {
        TBSYS_LOG(WARN, "failed to add END, err=%d", ret);
    }
    return ret;
}

int ObPostfixExpression::merge_expr(const ObPostfixExpression &expr1, const ObPostfixExpression &expr2, const ExprItem &op)
{
    int ret = OB_SUCCESS;
    for(int64_t i = 0; ret == OB_SUCCESS && i < expr1.expr_.count() - 1; i++)
    {
        ret = this->expr_.push_back(expr1.expr_[i]);

    }
    for(int64_t i = 0; ret == OB_SUCCESS && i < expr2.expr_.count() - 1; i++)
    {
        ret = this->expr_.push_back(expr2.expr_[i]);

    }
    if (ret == OB_SUCCESS)
    {
        ret = add_expr_item(op);
    }
    if (ret == OB_SUCCESS)
    {
        ret = add_expr_item_end();
    }
    if (ret != OB_SUCCESS)
    {
        TBSYS_LOG(ERROR, "Generate new expression faild, ret=%d", ret);
    }
    return ret;
}

//mod peiouya peiouya [IN_TYPEBUG_FIX] 20151225:b
////mod tianz [SubQuery_for_Instmt] [JHOBv0.1] 20140408:b
////int ObPostfixExpression::calc(const common::ObRow &row, const ObObj *&composite_val)
//int ObPostfixExpression::calc(const common::ObRow &row, const ObObj *&composite_val,hash::ObHashMap<common::ObRowkey,common::ObRowkey,common::hash::NoPthreadDefendMode>* hash_map,bool second_check )
////mod 20140408:e
int ObPostfixExpression::calc(const common::ObRow &row, const ObObj *&composite_val,
            hash::ObHashMap<common::ObRowkey,common::ObRowkey,common::hash::NoPthreadDefendMode>* hash_map,
            bool is_hashmap_contain_null,  //add peiouya [IN_AND NOT_IN_WITH_NULL_BUG_FIX] 20160518
            common::ObArray<ObObjType> * hash_map_pair_type_desc,
            bool second_check )
{
    int ret = OB_SUCCESS;
    int64_t type = 0;
    int64_t value = 0;
    int64_t value2 = 0;
    int64_t sys_func = 0;
    int idx = 0;
    ObExprObj result;
    int idx_i = 0;
    int sub_query_idx = 0;//add tianz [SubQuery_for_Instmt] [JHOBv0.1] 20140408:b /*Exp:for or,sub query index*/
    ObPostExprExtraParams *extra_params = GET_TSI_MULT(ObPostExprExtraParams, TSI_SQL_EXPR_EXTRA_PARAMS_1);
    // get the stack for calculation
    ObPostfixExpressionCalcStack *stack = GET_TSI_MULT(ObPostfixExpressionCalcStack, TSI_SQL_EXPR_STACK_1);
    if (NULL == stack || NULL == extra_params)
    {
        TBSYS_LOG(ERROR, "no memory for postfix expression extra params and stack");
        ret = OB_ALLOCATE_MEMORY_FAILED;
    }
    else
    {
        stack_ = stack->stack_;
        extra_params->did_int_div_as_double_ = did_int_div_as_double_;
        //add dolphin[coalesce return type]@20151126:b
        extra_params->expr_type_ =  get_expr_type();
        calc_buf_.reuse();
    }
    bool quick_path = false;
    const ObObj *var = NULL;
    //add wanglei [case when fix] 20160323:b
    int start = 0;
    bool is_case_when = false;
    bool when_is_true = false;
    bool else_is_null = true;
    ObExprObj null_result;
    null_result.set_null ();
  if( NULL != post_in_ada_) //add dolphin [in optimization] @20160608
      post_in_ada_->map_index = 0;
    //add wanglei :e
    while (OB_SUCCESS == ret)
    {
       // if(stack_[idx]!=NULL)
       // TBSYS_LOG(ERROR,"wanglei case when test:expr[%d]={%s}",idx,to_cstring(expr_[idx]));
        // 获得数据类型:列id、数字、操作符、结束标识符
      if (OB_SUCCESS != (ret = expr_[idx++].get_int(type)))
        {
            TBSYS_LOG(WARN, "fail to get int value. unexpected! ret=%d idx=%d", ret, idx-1);
            ret = OB_ERR_UNEXPECTED;
            break;
        }
      else if(type <= BEGIN_TYPE || type >= END_TYPE)
        {
            TBSYS_LOG(WARN, "unsupported operand type [type:%ld],idx=%d", type,idx-1);
            ret = OB_ERR_UNEXPECTED;
            break;
        }
        else if (END == type)   // expr_中以END符号表示结束
        {
            if (idx_i != 1)
            {
                TBSYS_LOG(WARN, "calculation stack must be empty. check the code for bugs. idx_i=%d,idx=%d,type=%ld", idx_i,idx-1,type);
                ret = OB_ERR_UNEXPECTED;
            }
            else
            {
                //modify fanqiushi DECIMAL OceanBase_BankCommV0.3 2014_7_9:b
                ObExprObj E_obj=stack_[idx_i-1];
                if(E_obj.get_type()==ObDecimalType)
                {
                    ObDecimal od=E_obj.get_decimal();

                    //add huangcc [Update rowkey] bug#1216 20160919:b
                    if(row.is_update_rowkey())
                    {
                        if(composite_val!=NULL)
                        {
                            uint32_t p=composite_val->get_precision();
                            uint32_t s=composite_val->get_scale();
                            if(s!=0 && od.get_vscale()>s)
                            {
                                od.modify_value(p,s);
                            }
                            
                        }
                        common::ObRow *rrow=const_cast<common::ObRow *>(&row);
                        rrow->set_is_update_rowkey(false); 
                    }
                    //add:e
                    
                    char buf[MAX_PRINTABLE_SIZE];
                    memset(buf, 0, MAX_PRINTABLE_SIZE);
                    int64_t length=0;
                    length=od.to_string(buf, MAX_PRINTABLE_SIZE);
                    ObString os;
                    os.assign_ptr(buf,static_cast<int32_t>(length));
                    ObObj obj;
                    obj.set_varchar(os);
                    if(OB_SUCCESS != (ret = str_buf_.write_obj(obj, &result_)))
                    {
                        TBSYS_LOG(ERROR, "fail to write object to string buffer. ret=%d", ret);
                    }
                    ObString os2;
                    result_.get_varchar(os2);
                    result_.set_decimal(os2);
                    composite_val = &result_;
                    idx_i--;
                }
                else
                {
                    if (OB_SUCCESS != (ret = stack_[--idx_i].to(result_)))
                    {
                        TBSYS_LOG(WARN, "failed to convert exprobj to obj, err=%d", ret);
                    }
                    else
                    {
                        composite_val = &result_;
                    }
                }
                //modify:e

            }
            break;
        }
        else if (idx_i < 0
                 || idx_i >= ObPostfixExpressionCalcStack::STACK_SIZE
                 || idx > expr_.count())
        {
            TBSYS_LOG(WARN,"calculation stack overflow [stack.index:%d] "
                      "or run out of operand [operand.used:%d,operand.avaliable:%ld]", idx_i, idx, expr_.count());
            ret = OB_ERR_UNEXPECTED;
            break;
        }
        else
        {
            // do calculation
            switch(type)
            {
            case COLUMN_IDX:

                if (OB_SUCCESS != (ret = expr_[idx++].get_int(value)))
                {
                    TBSYS_LOG(WARN,"get_int error [err:%d]", ret);
                }
                else if (OB_SUCCESS  != (ret = expr_[idx++].get_int(value2)))
                {
                    TBSYS_LOG(WARN,"get_int error [err:%d]", ret);
                }
                else
                {
                    if (4 == expr_.count())
                    {
                        // quick path
                        if (OB_SUCCESS != (ret = row.get_cell(static_cast<uint64_t>(value),
                                                              static_cast<uint64_t>(value2), composite_val)))
                        {
                            TBSYS_LOG(WARN, "fail to get cell from row. err=%d tid=%ld cid=%ld",
                                      ret, value, value2);
                        }
                        quick_path = true;
                    }
                    else
                    {
                        const ObObj *cell = NULL;
                        if (OB_SUCCESS != (ret = row.get_cell(static_cast<uint64_t>(value),
                                                              static_cast<uint64_t>(value2), cell)))
                        {
                            TBSYS_LOG(WARN, "fail to get cell from row. err=%d tid=%ld cid=%ld",
                                      ret, value, value2);
                        }
                        else
                        {
                            stack_[idx_i++].assign(*cell);
                        }
                    }
                }
                break;
            case CONST_OBJ:
                stack_[idx_i++].assign(expr_[idx++]);
                break;
            case PARAM_IDX:
            case SYSTEM_VAR:
            case TEMP_VAR:
            case CUR_TIME_OP:
                //add liuzy [datetime func] 20150910:b
            case CUR_DATE_OP:
            case CUR_TIME_HMS_OP:
                //add 20150910:e
                if (OB_SUCCESS == (ret = get_var_obj(static_cast<ObPostExprNodeType>(type), expr_[idx++], var)))
                {
                    stack_[idx_i++].assign(*var);
                }
                else
                {
                    TBSYS_LOG(WARN, "Can not get value ObObj. err=%d", ret);
                }
                break;
            case OP:

                // 根据OP的类型，从堆栈中弹出1个或多个操作数，进行计算
                if (OB_SUCCESS != (ret = expr_[idx++].get_int(value)))
                {
                    TBSYS_LOG(WARN,"get operator type failed [err:%d]", ret);
                }
                else if (OB_UNLIKELY(value <= T_MIN_OP || value >= T_MAX_OP))
                {
                    TBSYS_LOG(WARN,"unsupported operator type [type:%ld]", value);
                    ret = OB_ERR_UNEXPECTED;
                }
                else if(OB_SUCCESS != (ret = expr_[idx++].get_int(value2 /*param_count*/)))
                {
                    TBSYS_LOG(WARN,"get_int error [err:%d]", ret);
                }
                else
                {
                    extra_params->operand_count_ = static_cast<int32_t>(value2);
                    extra_params->str_buf_ = &calc_buf_;
                    if (OB_UNLIKELY(T_FUN_SYS == value))
                    {
                        if(OB_SUCCESS != (ret = expr_[idx++].get_int(sys_func)))
                        {
                            TBSYS_LOG(WARN, "failed to get sys func, err=%d", ret);
                        }
                        else if (0 > sys_func || sys_func >= SYS_FUNC_NUM)
                        {
                            TBSYS_LOG(WARN, "invalid sys function type=%ld", sys_func);
                            ret = OB_ERR_UNEXPECTED;
                        }
                        else if (OB_SUCCESS != (ret = SYS_FUNCS_TAB[sys_func](stack_, idx_i, result, *extra_params)))
                        {
                            TBSYS_LOG(WARN, "failed to call sys func, err=%d func=%ld", ret, sys_func);
                            break;
                        }
                        else
                        {
                            stack_[idx_i++] = result;
                        }
                    }
                    else
                    {
                        //add tianz [SubQuery_for_Instmt] [JHOBv0.1] 20140508:b
                        /*Exp: IN or NOT_IN expression,check its special mark--right_elem_count,
                  * if -1 == right_elem_count, indicate this expression hsa sub_query,
                  * need to do special process
                  */
                        bool special_process = false;
                        if ((T_OP_IN == value ||T_OP_NOT_IN == value) && (NULL != bloom_filter_ || NULL != hash_map))
                        {
                            int64_t right_elem_count = 0;
                            if (OB_SUCCESS != (ret = stack_[idx_i-1].get_int(right_elem_count)))
                            {
                                TBSYS_LOG(WARN, "fail to get_int from stack. top=%d, ret=%d", idx_i, ret);
                            }
                            else if(-1 == right_elem_count)
                            {
                                special_process = true;
                                //mod peiouya [IN_AND NOT_IN_WITH_NULL_BUG_FIX] 20160518:b
                                ////mod peiouya peiouya [IN_TYPEBUG_FIX] 20151225:b
                                ////if (OB_LIKELY(OB_SUCCESS == (ret = this->in_sub_query_func(stack_, idx_i, result, *extra_params,bloom_filter_,hash_map, sub_query_idx,second_check))))
                                //if (OB_LIKELY(OB_SUCCESS == (ret = this->in_sub_query_func(stack_, idx_i, result, *extra_params, bloom_filter_, hash_map, hash_map_pair_type_desc ,sub_query_idx, second_check))))
                                if (OB_LIKELY(OB_SUCCESS == (ret = this->in_sub_query_func(stack_, idx_i, result, *extra_params, bloom_filter_, hash_map,is_hashmap_contain_null,  hash_map_pair_type_desc ,sub_query_idx, second_check))))
                                ////mod 20151225:e
                                //mod 20160518:e
                                {
                                  //mod peiouya [IN_AND NOT_IN_WITH_NULL_BUG_FIX] 20160518:b
                                  if(T_OP_NOT_IN == value)
                                  {
                                    if (result.is_null())
                                    {
                                      result.set_bool(false);
                                    }
                                    else
                                    {
                                        result.lnot(result);
                                    }
                                  }
                                  else
                                  {
                                    if (result.is_null())
                                    {
                                      result.set_bool(false);
                                    }
                                  }
                                  //mod peiouya [IN_AND NOT_IN_WITH_NULL_BUG_FIX] 20160518:e
                                  stack_[idx_i++] = result;
                                }
                                else if (OB_NO_RESULT == ret)
                                {
                                    ret = OB_SUCCESS;
                                }
                                else
                                {
                                    TBSYS_LOG(WARN, "call calculation function error [value:%ld, idx_i:%d, err:%d]",
                                              value, idx_i, ret);
                                }
                            }

                        }
                        if(!special_process)
                        {
                            //add 20140508:e
                            //add dolphin[in post-expression optimization]@20150920:b
                            if (value == T_OP_LEFT_PARAM_END)
                            {
                                if(NULL == post_in_ada_)
                                {
                                    char *bf;
                                    if (NULL == (bf = (char*)ob_malloc(sizeof(ObPostfixInAdaptor), ObModIds::OB_IN_POSTFIX_EXPRESSION)))
                                    {
                                      ret = OB_ALLOCATE_MEMORY_FAILED;
                                        TBSYS_LOG(WARN,"alloc memory for in post-expression in adaptor failed");
                                        break;
                                    }
                                    else
                                    {
                                      post_in_ada_ = new(bf) ObPostfixInAdaptor();
                                      if(NULL == post_in_ada_)
                                        {
                                            ret = OB_ALLOCATE_MEMORY_FAILED;
                                            TBSYS_LOG(WARN,"alloc memory for new in post-expression in adaptor object failed");
                                            break;
                                        }
                                    }
                                }
                                if(OB_SUCCESS != (ret = expr_[idx-4].get_int(post_in_ada_->cdim)))
                                {
                                    TBSYS_LOG(WARN, "failed to get cdim, err=%d", ret);
                                    break;
                                }

                                int64_t tp =  extra_params->in_row_operator_.vec_.count() -1 ;
                                int64_t kk = tp;
                                while (kk > tp - post_in_ada_->cdim)
                                {
                                    extra_params->in_row_operator_.vec_.at(kk).to(post_in_ada_->temp_for_left[tp-kk]);
                                    if(post_in_ada_->temp_for_left[tp-kk].get_type() == ObNullType)
                                    {
                                        post_in_ada_->has_null_left_ = true;
                                    }
                                    --kk;
                                }

                                post_in_ada_->left_param_.assign(post_in_ada_->temp_for_left, post_in_ada_->cdim);
                                if (OB_LIKELY(OB_SUCCESS == (ret = (this->call_func[value - T_MIN_OP - 1])(stack_, idx_i, result, *extra_params))))
                                {
                                    stack_[idx_i++] = result;

                                }
                                else if (OB_NO_RESULT == ret)
                                {
                                    // nop
                                    ret = OB_SUCCESS;
                                }
                                else
                                {
                                    TBSYS_LOG(WARN, "call calculation function error [value:%ld, idx_i:%d, err:%d]",
                                              value, idx_i, ret);
                                }
                                int64_t v =0;
                                expr_[idx].get_int(v);
                                if(v == QUERY_ID )
                                    break;
                                if(NULL != post_in_ada_ && post_in_ada_->map_index + 1 <= post_in_ada_->rhm.count())
                                {
                                    if(0 <=  post_in_ada_->rhm.at(post_in_ada_->map_index))
                                    {
                                        int64_t tt =  post_in_ada_->rhm.at(post_in_ada_->map_index);
                                        if(OB_SUCCESS != (ret=check_in_hash(result, static_cast<int>(tt), post_in_ada_->Optype_[post_in_ada_->map_index])))
                                        {
                                            TBSYS_LOG(ERROR,"check in hash error,ret =%d",ret);
                                            return ret;
                                        }
                                        else
                                        {
                                            idx_i--;
                                            stack_[idx_i++] = result;
                                            ++post_in_ada_->map_index;
                                            post_in_ada_->cdim = 0;
                                            post_in_ada_->right_count = 0;
                                            post_in_ada_->inhash = false;
                                            post_in_ada_->has_null_left_ = false;
                                            idx = post_in_ada_->rhv.at(tt)->pre_idx_ + 2;
                                            int64_t top = extra_params->in_row_operator_.vec_.count();
                                            for (int l = 0; l < top; l++)
                                            {
                                                extra_params->in_row_operator_.vec_.pop_back();
                                            }

                                            break;
                                        }

                                    }
                                    else
                                    {

                                        break;
                                    }
                                }
                                else if(!post_in_ada_->has_null_left_)
                                {
                                    int k = idx+1;
                                    for(;k<expr_.count();++k)
                                    {
                                        if(OB_OBJ_TYPE_ERROR == (ret = expr_[k].get_int(post_in_ada_->right_count)))
                                        {
                                            continue;
                                        }
                                        else if(OB_SUCCESS == ret)
                                        {
                                            if(T_OP_IN == post_in_ada_->right_count || T_OP_NOT_IN == post_in_ada_->right_count)
                                            {
                                                if(T_OP_NOT_IN == post_in_ada_->right_count)
                                                {
                                                    post_in_ada_->is_not = true;
                                                }
                                                else
                                                {
                                                    post_in_ada_->is_not = false;
                                                }
                                                if(OB_SUCCESS != (ret = expr_[k-1].get_int(post_in_ada_->right_count)))
                                                {
                                                    TBSYS_LOG(WARN, "failed to get sys right_count, err=%d,k=%d,right_count=%ld", ret,k,post_in_ada_->right_count);
                                                    break;
                                                }
                                                else
                                                {
                                                    if(post_in_ada_->right_count != OP)
                                                        continue;
                                                }
                                                if(OB_SUCCESS != (ret = expr_[k-2].get_int(post_in_ada_->right_count)))
                                                {
                                                    TBSYS_LOG(WARN, "failed to get sys right_count, err=%d,k=%d,right_count=%ld", ret,k,post_in_ada_->right_count);
                                                    break;
                                                }
                                                else
                                                {
                                                    //TBSYS_LOG(ERROR,"right_count is %ld",right_count);
                                                    if(post_in_ada_->right_count >= USE_HASH_NUM)
                                                    {
                                                        post_in_ada_->inhash = true;
                                                    }
                                                    else
                                                    {
                                                      post_in_ada_->right_count = 0;
                                                    }
                                                    break;

                                                }
                                            }
                                        }
                                        else
                                        {
                                            TBSYS_LOG(WARN, "failed to get sys right_count, err=%d,k=%d,right_count=%ld", ret,k,post_in_ada_->right_count);
                                           break;
                                        }
                                    }

                                    if(post_in_ada_->inhash)
                                    {
                                            idx_i--;
                                            ObInRowHashOperator *inop = NULL;
                                            void *inbuf = ob_malloc(sizeof(ObInRowHashOperator),ObModIds::OB_IN_POSTFIX_EXPRESSION);

                                      if(NULL != inbuf)
                                            {
                                                inop = new(inbuf) ObInRowHashOperator();
                                            }
                                            else
                                            {
                                                ret = OB_ALLOCATE_MEMORY_FAILED;
                                                TBSYS_LOG(ERROR,"alloc memory for in post-expression hash map failed");
                                                break;
                                            }
                                            char *var_cast = (char *) ob_malloc(post_in_ada_->right_count * post_in_ada_->cdim * MAX_PRINTABLE_SIZE, ObModIds::OB_IN_POSTFIX_EXPRESSION);
                                      if(NULL == var_cast)
                                            {
                                                ret = OB_ALLOCATE_MEMORY_FAILED;
                                                TBSYS_LOG(WARN,"alloc memory for in post-expression hash values failed");
                                                break;
                                            }
                                            inop->dim_ = post_in_ada_->cdim;
                                            inop->right_count_ = post_in_ada_->right_count;
                                            post_in_ada_->t_dim = 0;
                                            inop->pre_idx_ = k;
                                            inop->var_cast_ = var_cast;
                                            if(NULL != post_in_ada_)
                                            {
                                                post_in_ada_->rhm.push_back(-1);
                                                post_in_ada_->rhv.push_back(inop);
                                                post_in_ada_->rhm.at(
                                                        post_in_ada_->map_index) = static_cast<int32_t>(post_in_ada_->rhv.count() - 1);
                                              break;
                                            }
                                            else
                                            {
                                                ret = OB_ALLOCATE_MEMORY_FAILED;
                                                TBSYS_LOG(WARN,"alloc memory for in post-expression pointer post_ind_ada_  failed");
                                                break;
                                            }

                                    }
                                }

                            }
                            else if (value == T_OP_ROW )
                            {
                                //TBSYS_LOG(ERROR,"T_OP_ROW,inhash=%d,t_dim+1=%d,right_count=%ld,idx=%d,idx_i=%d",inhash,t_dim+1,right_count,idx,idx_i);
                                if(NULL != post_in_ada_ && post_in_ada_->inhash && ++post_in_ada_->t_dim <= post_in_ada_->right_count )
                                {
                                  if (OB_SUCCESS !=
                                            (ret = extra_params->in_row_operator_.push_right_hash(stack_,
                                                                                                  post_in_ada_->temp_for_left,
                                                                                                  post_in_ada_->rhv.at(
                                                                                                          post_in_ada_->map_index), idx_i,
                                                                                                  post_in_ada_->cdim, post_in_ada_->rhv.at(
                                                            post_in_ada_->map_index)->var_cast_ + (post_in_ada_->t_dim - 1) * post_in_ada_->cdim * MAX_PRINTABLE_SIZE, post_in_ada_->is_not
                                                                                                  )))
                                    {
                                        TBSYS_LOG(WARN, "fail to push row into push_right_hash. ret=%d,idx_i=%d", ret,idx_i);
                                        break;
                                    }
                                    else
                                    {
                                        break;
                                    }
                                }
                                if(NULL == post_in_ada_ || (NULL != post_in_ada_ && !post_in_ada_->inhash))
                                {
                                    if (OB_LIKELY(OB_SUCCESS ==
                                                  (ret = (this->call_func[value - T_MIN_OP - 1])(stack_, idx_i, result,
                                                                                                 *extra_params))))
                                    {
                                        stack_[idx_i++] = result;
                                        ret = OB_SUCCESS;
                                    }
                                    else if (OB_NO_RESULT == ret)
                                    {
                                        // nop
                                        ret = OB_SUCCESS;
                                    }
                                    else
                                    {
                                        ret = OB_ALLOCATE_MEMORY_FAILED;
                                        TBSYS_LOG(WARN, "call calculation function error [value:%ld, idx_i:%d, err:%d]",value, idx_i, ret);
                                        break;
                                    }
                                }
                            }
                            else if ((value == T_OP_IN || value == T_OP_NOT_IN ) && NULL != post_in_ada_ && post_in_ada_->inhash )
                            {
                                int64_t tt = post_in_ada_->rhm.at(post_in_ada_->map_index);
                                post_in_ada_->Optype_[post_in_ada_->map_index] = value;
                               // print_in_hash_param(t);
                                if(OB_SUCCESS != (ret=check_in_hash(result, static_cast<int>(tt), post_in_ada_->Optype_[post_in_ada_->map_index])))
                                {
                                    TBSYS_LOG(ERROR,"check in hash error,ret =%d",ret);
                                    return ret;
                                }
                                else
                                {
                                    stack_[idx_i++] = result;

                                    ++post_in_ada_->map_index;
                                    post_in_ada_->cdim = 0;
                                    post_in_ada_->right_count = 0;
                                    post_in_ada_->inhash = false;
                                    post_in_ada_->has_null_left_ = false;
                                    int64_t top = extra_params->in_row_operator_.vec_.count();
                                    for (int l = 0; l < top; l++)
                                    {
                                        extra_params->in_row_operator_.vec_.pop_back();
                                    }
                                    break;
                                }

                            }
                            //处理一般函�?
                          if (value != T_OP_ROW && value != T_OP_LEFT_PARAM_END)
                            {
                                //add wanglei [case when fix] 20160323:b
                                if(T_CASE_BEGIN == value)
                                {
                                    if(is_case_when == false)
                                    {
                                        start = idx_i;
                                        is_case_when = true;
                                    }
                                    else
                                    {
                                        idx = idx -3;
                                        //mod peiouya [IN_AND NOT_IN_WITH_NULL_BUG_FIX] 20160518:b
                                        //ret = calc(row,composite_val,hash_map,hash_map_pair_type_desc,second_check,idx,idx_i);
                                        ret = calc(row,composite_val,hash_map,is_hashmap_contain_null,hash_map_pair_type_desc,second_check,idx,idx_i);
                                        //mod peiouya [IN_AND NOT_IN_WITH_NULL_BUG_FIX] 20160518:e
                                    }
                                    continue;
                                }
                                if(is_case_when && ret == OB_SUCCESS)
                                {
                                    switch (value)
                                    {
                                    case T_OP_IN:
                                    case T_OP_NOT_IN:
                                    {
                                        if(!post_in_ada_->inhash)
                                        {
                                            if (OB_LIKELY(OB_SUCCESS ==
                                                          (ret = (this->call_func[value - T_MIN_OP - 1])(stack_, idx_i, result,
                                                                                                         *extra_params))))
                                            {
                                                stack_[idx_i++] = result;
                                                //TBSYS_LOG(ERROR,"dolphin::  call_func idx_i=%d,value=%ld",idx_i,value);
                                                ret = OB_SUCCESS;
                                            }
                                            else if (OB_NO_RESULT == ret)
                                            {
                                                // nop
                                                ret = OB_SUCCESS;
                                            }
                                            else
                                            {
                                                ret = OB_ALLOCATE_MEMORY_FAILED;
                                                TBSYS_LOG(WARN, "call calculation function error [value:%ld, idx_i:%d, err:%d]",value, idx_i, ret);
                                            }
                                        }
                                    }
                                        break;
                                    case T_OP_LEFT_PARAM_END:
                                    case T_OP_ROW:
                                        break;
                                    case T_CASE_BEGIN:
                                    {
                                        continue;
                                    }
                                        break;
                                    case T_CASE_WHEN:
                                        idx_i = start;
                                        continue;
                                        break;
                                    case T_CASE_THEN:
                                    {
                                        when_is_true = stack_[idx_i-1].is_true();
                                        if(when_is_true)
                                        {
                                            continue;
                                        }
                                        else
                                        {
                                            stack_[idx_i++] = null_result;
                                            int64_t type_temp = 0;
                                            int64_t op_type_temp =0;
                                            int t_case_begin_count =0;
                                            int t_op_case_count =0;
                                            for(int i= idx ;i<expr_.count ();i++)
                                            {
                                                if(op_type_temp == T_CASE_BEGIN)
                                                {
                                                    t_case_begin_count ++;
                                                }
                                                if(op_type_temp == T_OP_CASE)
                                                {
                                                    t_op_case_count ++;
                                                }
                                                if(op_type_temp == T_CASE_ELSE||
                                                        op_type_temp == T_CASE_WHEN)
                                                {
                                                    if(t_case_begin_count == 0 ||
                                                            t_case_begin_count == t_op_case_count)
                                                    {
                                                        idx = idx -3;
                                                        break;
                                                    }
                                                }
                                                if (OB_SUCCESS != (ret = expr_[idx++].get_int(type_temp)))
                                                {
                                                    break;
                                                }
                                                else
                                                {
                                                    switch(type_temp)
                                                    {
                                                    case COLUMN_IDX:
                                                        idx++;idx++;op_type_temp = 0;
                                                        break;
                                                    case CONST_OBJ:
                                                    case PARAM_IDX:
                                                    case SYSTEM_VAR:
                                                    case TEMP_VAR:
                                                    case CUR_TIME_OP:
                                                    case CUR_DATE_OP:
                                                    case CUR_TIME_HMS_OP:
                                                        idx++;op_type_temp = 0;
                                                        break;
                                                    case OP:
                                                    {
                                                        if (OB_SUCCESS != (ret = expr_[idx].get_int(op_type_temp)))
                                                        {
                                                            break;
                                                        }
                                                        else
                                                        {
                                                            switch(op_type_temp)
                                                            {
                                                            case T_OP_EXISTS:case T_OP_POS:case T_OP_NEG:case T_OP_ADD: case T_OP_MINUS:
                                                            case T_OP_MUL:case T_OP_DIV:case T_OP_REM:case T_OP_POW:case T_OP_MOD:
                                                            case T_OP_LE:case T_OP_LT:case T_OP_EQ:case T_OP_GE:case T_OP_GT:case T_OP_NE:
                                                            case T_OP_LIKE:case T_OP_NOT_LIKE:case T_OP_AND:case T_OP_OR:case T_OP_NOT:
                                                            case T_OP_IS:case T_OP_IS_NOT:case T_OP_BTW:case T_OP_NOT_BTW:case T_OP_CNN:
                                                            case T_OP_IN:case T_OP_NOT_IN:case T_OP_ARG_CASE:case T_OP_CASE:case T_OP_ROW:
                                                            case T_OP_LEFT_PARAM_END:
                                                            case T_OP_CAST_THIN:  //add  peiouya [EXPIRE_INFO_NOT_ARITHMETIC_FIX] 20160608
                                                                idx++;idx++;
                                                                break;
                                                            case T_CUR_TIME_OP:
                                                                break;
                                                            case T_FUN_SYS:
                                                                idx++;idx++;idx++;
                                                                break;
                                                            case T_CASE_BEGIN:
                                                            case T_CASE_WHEN:
                                                            case T_CASE_THEN:
                                                            case T_CASE_ELSE:
                                                            case T_CASE_BREAK:
                                                            case T_CASE_END:
                                                                idx++;idx++;
                                                                break;
                                                            default:
                                                                ret = OB_INVALID_ARGUMENT;
                                                                break;
                                                            }
                                                        }
                                                    }
                                                        break;
                                                    default: ret = OB_INVALID_ARGUMENT;op_type_temp = 0;
                                                        break;
                                                    }
                                                }
                                            }
                                        }
                                    }
                                        break;
                                    case T_CASE_ELSE:
                                        else_is_null = false;
                                        continue;
                                        break;
                                    case T_CASE_BREAK:
                                    {
                                        int64_t type_temp = 0;
                                        int64_t op_type_temp =0;
                                        int t_case_begin_count =0;
                                        int t_op_case_end_count =0;
                                        bool can_break = false;
                                        for(int i= idx ;i<expr_.count ();i++)
                                        {
                                            if((op_type_temp == T_CASE_END && can_break)
                                                    ||(op_type_temp == T_CASE_END&&t_case_begin_count == 0))
                                            {
                                                idx = idx -3;
                                                break;
                                            }
                                            if(op_type_temp == T_CASE_BEGIN)
                                            {
                                                t_case_begin_count ++;
                                            }
                                            if(op_type_temp == T_CASE_END)
                                            {
                                                t_op_case_end_count ++;
                                            }
                                            if(op_type_temp == T_CASE_END)
                                            {
                                                if(t_case_begin_count == t_op_case_end_count)
                                                {
                                                   can_break = true;
                                                }
                                            }
//                                            if(op_type_temp == T_CASE_END)
//                                            {
//                                                idx = idx -3;
//                                                break;
//                                            }
                                            if (OB_SUCCESS != (ret = expr_[idx++].get_int(type_temp)))
                                            {
                                                break;
                                            }
                                            else
                                            {
                                                switch(type_temp)
                                                {
                                                case COLUMN_IDX:
                                                    idx++;idx++;
                                                    break;
                                                case CONST_OBJ:
                                                case PARAM_IDX:
                                                case SYSTEM_VAR:
                                                case TEMP_VAR:
                                                case CUR_TIME_OP:
                                                case CUR_DATE_OP:
                                                case CUR_TIME_HMS_OP:
                                                    idx++;
                                                    break;
                                                case OP:
                                                {
                                                    if (OB_SUCCESS != (ret = expr_[idx].get_int(op_type_temp)))
                                                    {
                                                        break;
                                                    }
                                                    else
                                                    {
                                                        switch(op_type_temp)
                                                        {

                                                        case T_OP_EXISTS:
                                                        case T_OP_POS:
                                                        case T_OP_NEG:
                                                        case T_OP_ADD:
                                                        case T_OP_MINUS:
                                                        case T_OP_MUL:
                                                        case T_OP_DIV:
                                                        case T_OP_REM:
                                                        case T_OP_POW:
                                                        case T_OP_MOD:
                                                        case T_OP_LE:
                                                        case T_OP_LT:
                                                        case T_OP_EQ:
                                                        case T_OP_GE:
                                                        case T_OP_GT:
                                                        case T_OP_NE:
                                                        case T_OP_LIKE:
                                                        case T_OP_NOT_LIKE:
                                                        case T_OP_AND:
                                                        case T_OP_OR:
                                                        case T_OP_NOT:
                                                        case T_OP_IS:
                                                        case T_OP_IS_NOT:
                                                        case T_OP_BTW:
                                                        case T_OP_NOT_BTW:
                                                        case T_OP_CNN:
                                                        case T_OP_IN:
                                                        case T_OP_NOT_IN:
                                                        case T_OP_ARG_CASE:
                                                        case T_OP_CASE:
                                                        case T_OP_ROW:
                                                        case T_OP_LEFT_PARAM_END:
                                                        case T_OP_CAST_THIN:  //add  peiouya [EXPIRE_INFO_NOT_ARITHMETIC_FIX] 20160608
                                                            idx++;idx++;
                                                            break;
                                                        case T_CUR_TIME_OP:

                                                            break;
                                                        case T_FUN_SYS:
                                                            idx++;idx++;idx++;
                                                            break;//add wanglei [case when fix] 20160323:b
                                                        case T_CASE_BEGIN:
                                                        case T_CASE_WHEN:
                                                        case T_CASE_THEN:
                                                        case T_CASE_ELSE:
                                                        case T_CASE_BREAK:
                                                        case T_CASE_END:
                                                            idx ++;idx++;
                                                            break;
                                                        default:
                                                            ret = OB_INVALID_ARGUMENT;
                                                            break;
                                                        }
                                                    }
                                                } break;
                                                default: ret = OB_INVALID_ARGUMENT;
                                                    break;
                                                }
                                            }
                                        }
                                    }
                                        break;
                                    case T_CASE_END:
                                    {
                                        if(else_is_null)
                                            stack_[idx_i++] = null_result;
                                        continue;
                                    }
                                        break;
                                    case T_OP_ARG_CASE:
                                    case T_OP_CASE:
                                    {
                                        if (OB_LIKELY(OB_SUCCESS ==
                                                      (ret = (this->call_func[value - T_MIN_OP - 1])(stack_, idx_i, result,
                                                                                                     *extra_params))))
                                        {
                                            stack_[idx_i++] = result;
                                            ret = OB_SUCCESS;
                                        }
                                        else if (OB_NO_RESULT == ret)
                                        {
                                            // nop
                                            ret = OB_SUCCESS;
                                        }
                                        else
                                        {
                                            TBSYS_LOG(WARN, "call calculation function error [value:%ld, idx_i:%d, err:%d]",value, idx_i, ret);
                                        }
                                    }
                                        break;
                                    default:
                                    {
                                        if (OB_LIKELY(OB_SUCCESS ==
                                                      (ret = (this->call_func[value - T_MIN_OP - 1])(stack_, idx_i, result,
                                                                                                     *extra_params))))
                                        {
                                            stack_[idx_i++] = result;
                                            ret = OB_SUCCESS;
                                        }
                                        else if (OB_NO_RESULT == ret)
                                        {
                                            // nop
                                            ret = OB_SUCCESS;
                                        }
                                        else
                                        {
                                            TBSYS_LOG(WARN, "call calculation function error [value:%ld, idx_i:%d, err:%d]",value, idx_i, ret);
                                        }
                                    }
                                        break;
                                    }
                                }
                                else if (ret == OB_SUCCESS && OB_LIKELY(OB_SUCCESS ==
                                                   (ret = (this->call_func[value - T_MIN_OP - 1])(stack_, idx_i, result,
                                                                                                  *extra_params))))
                                {
                                    /*if(ObDecimalType == result.get_type() && result.get_varchar().ptr())
                                                                                {
                                                                                    ObDecimal od=result.get_decimal();
                                                                                    char buf[MAX_PRINTABLE_SIZE];
                                                                                    memset(buf, 0, MAX_PRINTABLE_SIZE);
                                                                                    int64_t length=0;
                                                                                    length=od.to_string(buf, MAX_PRINTABLE_SIZE);
                                                                                    ObString os;
                                                                                    os.assign_ptr(buf,static_cast<int32_t>(length));
                                                                                    ObString ns;
                                                                                    if(OB_SUCCESS != (ret = str_buf_.write_string(os, &ns)))
                                                                                    {
                                                                                        TBSYS_LOG(ERROR, "fail to write object to string buffer. ret=%d", ret);
                                                                                        break;
                                                                                    }
                                                                                    TBSYS_LOG(ERROR,"dolphin::test ns is %.*s",ns.length(),ns.ptr());
                                                                                    result.set_varchar(ns);


                                                                                }*/
                                    stack_[idx_i++] = result;
                                    ret = OB_SUCCESS;
                                }
                                else if (OB_NO_RESULT == ret)
                                {
                                    // nop
                                    ret = OB_SUCCESS;
                                }
                                else
                                {
                                    TBSYS_LOG(WARN, "call calculation function error [value:%ld, idx_i:%d, err:%d]",value, idx_i, ret);
                                }
                                //add wanglei :e
                            }//add dolphin[in post-expression optimization]@20150921
                        }
                    }
                }//add tianz [SubQuery_for_Instmt] [JHOBv0.1] 20140408 /*Exp:match the new else branch*/
                break;
            case UPS_TIME_OP:
                if (OB_SUCCESS != (ret = sys_func_current_timestamp(stack_, idx_i, result, *extra_params)))
                {
                    TBSYS_LOG(WARN, "failed to call sys_func_current_timestamp, err=%d", ret);
                }
                else
                {
                    stack_[idx_i++] = result;
                }
                break;
                //add tianz [SubQuery_for_Instmt] [JHOBv0.1] 20140408:b
                /*Exp:for QUERY_ID,set special mark :-1  */
            case QUERY_ID:
                if(OB_SUCCESS != (ret = expr_[idx++].get_int(value)))
                {
                    TBSYS_LOG(WARN,"get_int error [err:%d]", ret);
                }
                else
                {
                    int64_t temp_value = -1;
                    result.set_int(temp_value);
                    stack_[idx_i++] = result;
                }
                break;
                //add 20140408:e
            default:
                ret = OB_ERR_UNEXPECTED;
                TBSYS_LOG(WARN,"unexpected [type:%ld]", type);
                break;
            } // end switch
        }   // end else
        if (quick_path)
            break;
    } // end while

    return ret;
}
//add wanglei [case when fix] 20160516:b
int ObPostfixExpression::calc(const common::ObRow &row, const ObObj *&composite_val,
hash::ObHashMap<common::ObRowkey,common::ObRowkey,common::hash::NoPthreadDefendMode>* hash_map,
bool is_hashmap_contain_null,  //add peiouya [IN_AND NOT_IN_WITH_NULL_BUG_FIX] 20160518
common::ObArray<ObObjType> * hash_map_pair_type_desc,
bool second_check ,int &idx_, int &idx_i_)
//mod 20151225:e
{
    int ret = OB_SUCCESS;
    int64_t type = 0;
    int64_t value = 0;
    int64_t value2 = 0;
    int64_t sys_func = 0;
    int &idx = idx_;
    ObExprObj result;
    int idx_i_flag = idx_i_;
    int &idx_i = idx_i_;
    int sub_query_idx = 0;//add tianz [SubQuery_for_Instmt] [JHOBv0.1] 20140408:b /*Exp:for or,sub query index*/
    ObPostExprExtraParams *extra_params = GET_TSI_MULT(ObPostExprExtraParams, TSI_SQL_EXPR_EXTRA_PARAMS_1);
    // get the stack for calculation
    ObPostfixExpressionCalcStack *stack = GET_TSI_MULT(ObPostfixExpressionCalcStack, TSI_SQL_EXPR_STACK_1);
    if (NULL == stack || NULL == extra_params)
    {
        TBSYS_LOG(ERROR, "no memory for postfix expression extra params and stack");
        ret = OB_ALLOCATE_MEMORY_FAILED;
    }
    else
    {
        //TBSYS_LOG(ERROR,"dolphin::test extra_params->in_row_operator_.vec_.count() is %ld",extra_params->in_row_operator_.vec_.count());
        stack_ = stack->stack_;
        extra_params->did_int_div_as_double_ = did_int_div_as_double_;
        //add dolphin[coalesce return type]@20151126:b
        extra_params->expr_type_ =  get_expr_type();
        //TBSYS_LOG(ERROR,"dolphin::test post expression return type is:%d",get_expr_type());
        calc_buf_.reuse();
    }
    bool quick_path = false;
    const ObObj *var = NULL;
    if( NULL != post_in_ada_) //add dolphin [in optimization] @20160608
        post_in_ada_->map_index = 0;
    //add wanglei [case when fix] 20160323:b
    int start = 0;
    bool is_case_when = false;
    bool when_is_true = false;
    bool else_is_null = true;
    ObExprObj null_result;
    null_result.set_null ();
    //add wanglei :e
    while (OB_SUCCESS == ret)
    {
       // if(stack_[idx]!=NULL)
       // TBSYS_LOG(ERROR,"wanglei case when test:expr[%d]={%s}",idx,to_cstring(expr_[idx]));
        // 获得数据类型:列id、数字、操作符、结束标�?
      if (OB_SUCCESS != (ret = expr_[idx++].get_int(type)))
        {
            TBSYS_LOG(WARN, "fail to get int value. unexpected! ret=%d idx=%d", ret, idx-1);
            ret = OB_ERR_UNEXPECTED;
            break;
        }
        else if(type <= BEGIN_TYPE || type >= END_TYPE)
        {
            TBSYS_LOG(WARN, "unsupported operand type [type:%ld],idx=%d", type,idx-1);
            ret = OB_ERR_UNEXPECTED;
            break;
        }
        else if (END == type)   // expr_中以END符号表示结束
        {
            if (idx_i != idx_i_flag)
            {
                for(int i = 0;i <idx_i;++i)
                {
                    ObExprObj o = stack_[i];
                    char buf[256];
                    o.to_string(buf,256);
                    TBSYS_LOG(ERROR,"stack[%d] is:%s",i,buf);
                }
                TBSYS_LOG(WARN, "calculation stack must be empty. check the code for bugs. idx_i=%d", idx_i);
                ret = OB_ERR_UNEXPECTED;
            }
            else
            {
                //modify fanqiushi DECIMAL OceanBase_BankCommV0.3 2014_7_9:b
                ObExprObj E_obj=stack_[idx_i-1];
                if(E_obj.get_type()==ObDecimalType)
                {
                    ObDecimal od=E_obj.get_decimal();
                    char buf[MAX_PRINTABLE_SIZE];
                    memset(buf, 0, MAX_PRINTABLE_SIZE);
                    int64_t length=0;
                    length=od.to_string(buf, MAX_PRINTABLE_SIZE);
                    ObString os;
                    os.assign_ptr(buf,static_cast<int32_t>(length));
                    ObObj obj;
                    obj.set_varchar(os);
                    if(OB_SUCCESS != (ret = str_buf_.write_obj(obj, &result_)))
                    {
                        TBSYS_LOG(ERROR, "fail to write object to string buffer. ret=%d", ret);
                    }
                    ObString os2;
                    result_.get_varchar(os2);
                    result_.set_decimal(os2);
                    composite_val = &result_;
                    idx_i--;
                }
                else
                {
                    if (OB_SUCCESS != (ret = stack_[--idx_i].to(result_)))
                    {
                        TBSYS_LOG(WARN, "failed to convert exprobj to obj, err=%d", ret);
                    }
                    else
                    {
                        composite_val = &result_;
                    }
                }
                //modify:e

            }
            break;
        }
        else if (idx_i < 0
                 || idx_i >= ObPostfixExpressionCalcStack::STACK_SIZE
                 || idx > expr_.count())
        {
            TBSYS_LOG(WARN,"calculation stack overflow [stack.index:%d] "
                      "or run out of operand [operand.used:%d,operand.avaliable:%ld]", idx_i, idx, expr_.count());
            ret = OB_ERR_UNEXPECTED;
            break;
        }
        else
        {
            // do calculation
            switch(type)
            {
            case COLUMN_IDX:

                if (OB_SUCCESS != (ret = expr_[idx++].get_int(value)))
                {
                    TBSYS_LOG(WARN,"get_int error [err:%d]", ret);
                }
                else if (OB_SUCCESS  != (ret = expr_[idx++].get_int(value2)))
                {
                    TBSYS_LOG(WARN,"get_int error [err:%d]", ret);
                }
                else
                {
                    if (4 == expr_.count())
                    {
                        // quick path
                        if (OB_SUCCESS != (ret = row.get_cell(static_cast<uint64_t>(value),
                                                              static_cast<uint64_t>(value2), composite_val)))
                        {
                            TBSYS_LOG(WARN, "fail to get cell from row. err=%d tid=%ld cid=%ld",
                                      ret, value, value2);
                        }
                        quick_path = true;
                    }
                    else
                    {
                        const ObObj *cell = NULL;
                        if (OB_SUCCESS != (ret = row.get_cell(static_cast<uint64_t>(value),
                                                              static_cast<uint64_t>(value2), cell)))
                        {
                            TBSYS_LOG(WARN, "fail to get cell from row. err=%d tid=%ld cid=%ld",
                                      ret, value, value2);
                        }
                        else
                        {
                            stack_[idx_i++].assign(*cell);
                        }
                    }
                }
                break;
            case CONST_OBJ:
                     stack_[idx_i++].assign(expr_[idx++]);
                break;
            case PARAM_IDX:
            case SYSTEM_VAR:
            case TEMP_VAR:
            case CUR_TIME_OP:
                //add liuzy [datetime func] 20150910:b
            case CUR_DATE_OP:
            case CUR_TIME_HMS_OP:
                //add 20150910:e
                if (OB_SUCCESS == (ret = get_var_obj(static_cast<ObPostExprNodeType>(type), expr_[idx++], var)))
                {
                    stack_[idx_i++].assign(*var);
                }
                else
                {
                    TBSYS_LOG(WARN, "Can not get value ObObj. err=%d", ret);
                }
                break;
            case OP:

                // 根据OP的类型，从堆栈中弹出1个或多个操作数，进行计算
                if (OB_SUCCESS != (ret = expr_[idx++].get_int(value)))
                {
                    TBSYS_LOG(WARN,"get operator type failed [err:%d]", ret);
                }
                else if (OB_UNLIKELY(value <= T_MIN_OP || value >= T_MAX_OP))
                {
                    TBSYS_LOG(WARN,"unsupported operator type [type:%ld]", value);
                    ret = OB_ERR_UNEXPECTED;
                }
                else if(OB_SUCCESS != (ret = expr_[idx++].get_int(value2 /*param_count*/)))
                {
                    TBSYS_LOG(WARN,"get_int error [err:%d]", ret);
                }
                else
                {
                    extra_params->operand_count_ = static_cast<int32_t>(value2);
                    extra_params->str_buf_ = &calc_buf_;
                    if (OB_UNLIKELY(T_FUN_SYS == value))
                    {
                        if(OB_SUCCESS != (ret = expr_[idx++].get_int(sys_func)))
                        {
                            TBSYS_LOG(WARN, "failed to get sys func, err=%d", ret);
                        }
                        else if (0 > sys_func || sys_func >= SYS_FUNC_NUM)
                        {
                            TBSYS_LOG(WARN, "invalid sys function type=%ld", sys_func);
                            ret = OB_ERR_UNEXPECTED;
                        }
                        else if (OB_SUCCESS != (ret = SYS_FUNCS_TAB[sys_func](stack_, idx_i, result, *extra_params)))
                        {
                            TBSYS_LOG(WARN, "failed to call sys func, err=%d func=%ld", ret, sys_func);
                            break;
                        }
                        else
                        {
                            stack_[idx_i++] = result;
                        }
                    }
                    else
                    {
                        //add tianz [SubQuery_for_Instmt] [JHOBv0.1] 20140508:b
                        /*Exp: IN or NOT_IN expression,check its special mark--right_elem_count,
                  * if -1 == right_elem_count, indicate this expression hsa sub_query,
                  * need to do special process
                  */
                        bool special_process = false;
                        if ((T_OP_IN == value ||T_OP_NOT_IN == value) && (NULL != bloom_filter_ || NULL != hash_map))
                        {
                            int64_t right_elem_count = 0;
                            if (OB_SUCCESS != (ret = stack_[idx_i-1].get_int(right_elem_count)))
                            {
                                TBSYS_LOG(WARN, "fail to get_int from stack. top=%d, ret=%d", idx_i, ret);
                            }
                            else if(-1 == right_elem_count)
                            {
                                special_process = true;
                                //mod peiouya [IN_AND NOT_IN_WITH_NULL_BUG_FIX] 20160518:b
                                ////mod peiouya peiouya [IN_TYPEBUG_FIX] 20151225:b
                                ////if (OB_LIKELY(OB_SUCCESS == (ret = this->in_sub_query_func(stack_, idx_i, result, *extra_params,bloom_filter_,hash_map, sub_query_idx,second_check))))
                                //if (OB_LIKELY(OB_SUCCESS == (ret = this->in_sub_query_func(stack_, idx_i, result, *extra_params, bloom_filter_, hash_map, hash_map_pair_type_desc ,sub_query_idx, second_check))))
                                if (OB_LIKELY(OB_SUCCESS == (ret = this->in_sub_query_func(stack_, idx_i, result, *extra_params, bloom_filter_, hash_map,is_hashmap_contain_null, hash_map_pair_type_desc ,sub_query_idx, second_check))))
                                    //mod 20151225:e
                                {
                                  if(T_OP_NOT_IN == value)
                                  {
                                    if (result.is_null())
                                    {
                                      result.set_bool(false);
                                    }
                                    else
                                    {
                                        result.lnot(result);
                                    }
                                  }
                                  else
                                  {
                                    if (result.is_null())
                                    {
                                      result.set_bool(false);
                                    }
                                  }

                                  //mod peiouya [IN_AND NOT_IN_WITH_NULL_BUG_FIX] 20160518:e
                                  stack_[idx_i++] = result;
                                }
                                else if (OB_NO_RESULT == ret)
                                {
                                    ret = OB_SUCCESS;
                                }
                                else
                                {
                                    TBSYS_LOG(WARN, "call calculation function error [value:%ld, idx_i:%d, err:%d]",
                                              value, idx_i, ret);
                                }
                            }

                        }
                        if(!special_process)
                        {
                            //add 20140508:e
                            //TBSYS_LOG(ERROR,"dolphin:: enter not sub query process flow");
                            //add dolphin[in post-expression optimization]@20150920:b
                            if (value == T_OP_LEFT_PARAM_END)
                            {
                                //print_expr();
                              if(NULL == post_in_ada_)
                              {
                                char *bf;
                                if (NULL == (bf = (char*)ob_malloc(sizeof(ObPostfixInAdaptor), ObModIds::OB_IN_POSTFIX_EXPRESSION)))
                                {
                                  ret = OB_ALLOCATE_MEMORY_FAILED;
                                  TBSYS_LOG(WARN,"alloc memory for in post-expression in adaptor failed");
                                  break;
                                }
                                else
                                {
                                  post_in_ada_ = new(bf) ObPostfixInAdaptor();
                                  if(NULL == post_in_ada_)
                                  {
                                    ret = OB_ALLOCATE_MEMORY_FAILED;
                                    TBSYS_LOG(WARN,"alloc memory for new in post-expression in adaptor object failed");
                                    break;
                                  }
                                }
                              }
                              if(OB_SUCCESS != (ret = expr_[idx-4].get_int(post_in_ada_->cdim)))
                              {
                                TBSYS_LOG(WARN, "failed to get cdim, err=%d", ret);
                                break;
                              }

                              int64_t tp =  extra_params->in_row_operator_.vec_.count() -1 ;
                              int64_t kk = tp;
                              while (kk > tp - post_in_ada_->cdim)
                              {
                                extra_params->in_row_operator_.vec_.at(kk).to(post_in_ada_->temp_for_left[tp-kk]);
                                if(post_in_ada_->temp_for_left[tp-kk].get_type() == ObNullType)
                                {
                                  post_in_ada_->has_null_left_ = true;
                                }
                                --kk;
                              }

                              post_in_ada_->left_param_.assign(post_in_ada_->temp_for_left, post_in_ada_->cdim);
                              if (OB_LIKELY(OB_SUCCESS == (ret = (this->call_func[value - T_MIN_OP - 1])(stack_, idx_i, result, *extra_params))))
                              {
                                stack_[idx_i++] = result;

                              }
                              else if (OB_NO_RESULT == ret)
                              {
                                // nop
                                ret = OB_SUCCESS;
                              }
                              else
                              {
                                TBSYS_LOG(WARN, "call calculation function error [value:%ld, idx_i:%d, err:%d]",
                                          value, idx_i, ret);
                              }
                              int64_t v =0;
                              expr_[idx].get_int(v);
                              if(v == QUERY_ID )
                                break;
                              if(NULL != post_in_ada_ && post_in_ada_->map_index + 1 <= post_in_ada_->rhm.count())
                              {
                                if(0 <=  post_in_ada_->rhm.at(post_in_ada_->map_index))
                                {
                                  int64_t tt =  post_in_ada_->rhm.at(post_in_ada_->map_index);
                                  if(OB_SUCCESS != (ret=check_in_hash(result, static_cast<int>(tt), post_in_ada_->Optype_[post_in_ada_->map_index])))
                                  {
                                    TBSYS_LOG(ERROR,"check in hash error,ret =%d",ret);
                                    return ret;
                                  }
                                  else
                                  {
                                    idx_i--;
                                    stack_[idx_i++] = result;
                                    ++post_in_ada_->map_index;
                                    post_in_ada_->cdim = 0;
                                    post_in_ada_->right_count = 0;
                                    post_in_ada_->inhash = false;
                                    post_in_ada_->has_null_left_ = false;
                                    idx = post_in_ada_->rhv.at(tt)->pre_idx_ + 2;
                                    int64_t top = extra_params->in_row_operator_.vec_.count();
                                    for (int l = 0; l < top; l++)
                                    {
                                      extra_params->in_row_operator_.vec_.pop_back();
                                    }

                                    break;
                                  }

                                }
                                else
                                {
                                  break;
                                }
                              }
                                else if(!post_in_ada_->has_null_left_)
                                {
                                  int k = idx+1;
                                  for(;k<expr_.count();++k)
                                  {
                                    if(OB_OBJ_TYPE_ERROR == (ret = expr_[k].get_int(post_in_ada_->right_count)))
                                    {
                                      continue;
                                    }
                                    else if(OB_SUCCESS == ret)
                                    {
                                      if(T_OP_IN == post_in_ada_->right_count || T_OP_NOT_IN == post_in_ada_->right_count)
                                      {
                                        if(T_OP_NOT_IN == post_in_ada_->right_count)
                                        {
                                          post_in_ada_->is_not = true;
                                        }
                                        else
                                        {
                                          post_in_ada_->is_not = false;
                                        }
                                        if(OB_SUCCESS != (ret = expr_[k-1].get_int(post_in_ada_->right_count)))
                                        {
                                          TBSYS_LOG(WARN, "failed to get sys right_count, err=%d,k=%d,right_count=%ld", ret,k,post_in_ada_->right_count);
                                          break;
                                        }
                                        else
                                        {
                                          if(post_in_ada_->right_count != OP)
                                            continue;
                                        }
                                        if(OB_SUCCESS != (ret = expr_[k-2].get_int(post_in_ada_->right_count)))
                                        {
                                          TBSYS_LOG(WARN, "failed to get sys right_count, err=%d,k=%d,right_count=%ld", ret,k,post_in_ada_->right_count);
                                          break;
                                        }
                                        else
                                        {
                                          //TBSYS_LOG(ERROR,"right_count is %ld",right_count);
                                          if(post_in_ada_->right_count >= USE_HASH_NUM)
                                          {
                                            post_in_ada_->inhash = true;
                                          }
                                          break;

                                        }
                                      }
                                    }
                                    else
                                    {
                                      TBSYS_LOG(WARN, "failed to get sys right_count, err=%d,k=%d,right_count=%ld", ret,k,post_in_ada_->right_count);
                                      break;
                                    }
                                  }

                                  if(post_in_ada_->inhash)
                                  {
                                    idx_i--;
                                    ObInRowHashOperator *inop = NULL;
                                    void *inbuf = ob_malloc(sizeof(ObInRowHashOperator),ObModIds::OB_IN_POSTFIX_EXPRESSION);
                                    if(NULL != inbuf)
                                    {
                                      inop = new(inbuf) ObInRowHashOperator();
                                    }
                                    else
                                    {
                                      ret = OB_ALLOCATE_MEMORY_FAILED;
                                      TBSYS_LOG(WARN,"alloc memory for in post-expression hash map failed");
                                      break;
                                    }
                                    char *var_cast = (char *) ob_malloc(post_in_ada_->right_count * post_in_ada_->cdim * MAX_PRINTABLE_SIZE, ObModIds::OB_IN_POSTFIX_EXPRESSION);
                                    if(NULL == var_cast)
                                    {
                                      ret = OB_ALLOCATE_MEMORY_FAILED;
                                      TBSYS_LOG(WARN,"alloc memory for in post-expression hash values failed");
                                      break;
                                    }
                                    inop->dim_ = post_in_ada_->cdim;
                                    inop->right_count_ = post_in_ada_->right_count;
                                    post_in_ada_->t_dim = 0;
                                    inop->pre_idx_ = k;
                                    inop->var_cast_ = var_cast;
                                    if(NULL != post_in_ada_)
                                    {
                                      post_in_ada_->rhm.push_back(-1);
                                      post_in_ada_->rhv.push_back(inop);
                                      post_in_ada_->rhm.at(
                                          post_in_ada_->map_index) = static_cast<int32_t>(post_in_ada_->rhv.count() - 1);
                                      break;
                                    }
                                    else
                                    {
                                      ret = OB_ALLOCATE_MEMORY_FAILED;
                                      TBSYS_LOG(WARN,"alloc memory for in post-expression pointer post_ind_ada_  failed");
                                      break;
                                    }
                                  }

                                }

                            }
                            else if (value == T_OP_ROW )
                            {
                              //TBSYS_LOG(ERROR,"T_OP_ROW,inhash=%d,t_dim+1=%d,right_count=%ld,idx=%d,idx_i=%d",inhash,t_dim+1,right_count,idx,idx_i);
                              if(NULL != post_in_ada_ && post_in_ada_->inhash && ++post_in_ada_->t_dim <= post_in_ada_->right_count )
                              {
                                if (OB_SUCCESS !=
                                    (ret = extra_params->in_row_operator_.push_right_hash(stack_,
                                                                                          post_in_ada_->temp_for_left,
                                                                                          post_in_ada_->rhv.at(
                                                                                              post_in_ada_->map_index), idx_i,
                                                                                          post_in_ada_->cdim, post_in_ada_->rhv.at(
                                            post_in_ada_->map_index)->var_cast_ + (post_in_ada_->t_dim - 1) * post_in_ada_->cdim * MAX_PRINTABLE_SIZE, post_in_ada_->is_not
                                    )))
                                {
                                  TBSYS_LOG(WARN, "fail to push row into push_right_hash. ret=%d,idx_i=%d", ret,idx_i);
                                  break;
                                }
                                else
                                {

                                  break;
                                }
                              }
                              if(NULL == post_in_ada_ || (NULL != post_in_ada_ && !post_in_ada_->inhash))
                              {
                                if (OB_LIKELY(OB_SUCCESS ==
                                    (ret = (this->call_func[value - T_MIN_OP - 1])(stack_, idx_i, result,
                                                                                   *extra_params))))
                                {
                                  stack_[idx_i++] = result;
                                  //TBSYS_LOG(ERROR,"dolphin::  call_func idx_i=%d,value=%ld",idx_i,value);
                                  ret = OB_SUCCESS;
                                }
                                else if (OB_NO_RESULT == ret)
                                {
                                  // nop
                                  ret = OB_SUCCESS;
                                }
                                else
                                {
                                  ret = OB_ALLOCATE_MEMORY_FAILED;
                                  TBSYS_LOG(WARN, "call calculation function error [value:%ld, idx_i:%d, err:%d]",value, idx_i, ret);
                                  break;
                                }
                              }
                            }
                            else if ((value == T_OP_IN || value == T_OP_NOT_IN ) && NULL != post_in_ada_ && post_in_ada_->inhash )
                            {
                              int64_t tt = post_in_ada_->rhm.at(post_in_ada_->map_index);
                              post_in_ada_->Optype_[post_in_ada_->map_index] = value;
                              // print_in_hash_param(t);
                              if(OB_SUCCESS != (ret=check_in_hash(result, static_cast<int>(tt), post_in_ada_->Optype_[post_in_ada_->map_index])))
                              {
                                TBSYS_LOG(ERROR,"check in hash error,ret =%d",ret);
                                return ret;
                              }
                              else
                              {
                                stack_[idx_i++] = result;
                                ++post_in_ada_->map_index;
                                post_in_ada_->cdim = 0;
                                post_in_ada_->right_count = 0;
                                post_in_ada_->inhash = false;
                                post_in_ada_->has_null_left_ = false;
                                int64_t top = extra_params->in_row_operator_.vec_.count();
                                for (int l = 0; l < top; l++)
                                {
                                  extra_params->in_row_operator_.vec_.pop_back();
                                }
                                break;
                              }
                            }
                            //处理一般函�?
                          if (value != T_OP_ROW && value != T_OP_LEFT_PARAM_END)
                            {
                                //add wanglei [case when fix] 20160323:b
                                if(T_CASE_BEGIN == value)
                                {
                                    if(is_case_when == false)
                                    {
                                        start = idx_i;
                                        is_case_when = true;
                                    }
                                    else
                                    {
                                        idx = idx -3;
                                        //mod peiouya [IN_AND NOT_IN_WITH_NULL_BUG_FIX] 20160518:b
                                        //ret = calc(row,composite_val,hash_map,hash_map_pair_type_desc,second_check,idx,idx_i);
                                        ret = calc(row,composite_val,hash_map,is_hashmap_contain_null,hash_map_pair_type_desc,second_check,idx,idx_i);
                                        //mod peiouya [IN_AND NOT_IN_WITH_NULL_BUG_FIX] 20160518:e
                                    }
                                    continue;
                                }
                                if(is_case_when && ret == OB_SUCCESS)
                                {
                                    switch (value)
                                    {
                                    case T_OP_IN:
                                    case T_OP_NOT_IN:
                                    {
                                        if(!post_in_ada_->inhash)
                                        {
                                            if (OB_LIKELY(OB_SUCCESS ==
                                                          (ret = (this->call_func[value - T_MIN_OP - 1])(stack_, idx_i, result,
                                                                                                         *extra_params))))
                                            {
                                                stack_[idx_i++] = result;
                                                //TBSYS_LOG(ERROR,"dolphin::  call_func idx_i=%d,value=%ld",idx_i,value);
                                                ret = OB_SUCCESS;
                                            }
                                            else if (OB_NO_RESULT == ret)
                                            {
                                                // nop
                                                ret = OB_SUCCESS;
                                            }
                                            else
                                            {
                                                ret = OB_ALLOCATE_MEMORY_FAILED;
                                                TBSYS_LOG(WARN, "call calculation function error [value:%ld, idx_i:%d, err:%d]",value, idx_i, ret);
                                            }
                                        }
                                    }
                                        break;
                                    case T_CASE_BEGIN:
                                    {
                                        continue;
                                    }
                                        break;
                                    case T_CASE_WHEN:
                                        idx_i = start;
                                        continue;
                                        break;
                                    case T_CASE_THEN:
                                    {
                                        when_is_true = stack_[idx_i-1].is_true();
                                        if(when_is_true)
                                        {
                                            continue;
                                        }
                                        else
                                        {
                                            stack_[idx_i++] = null_result;
                                            int64_t type_temp = 0;
                                            int64_t op_type_temp =0;
                                            int t_case_begin_count =0;
                                            int t_op_case_count =0;
                                            for(int i= idx ;i<expr_.count ();i++)
                                            {
                                                if(op_type_temp == T_CASE_BEGIN)
                                                {
                                                    t_case_begin_count ++;
                                                }
                                                if(op_type_temp == T_OP_CASE)
                                                {
                                                    t_op_case_count ++;
                                                }
                                                if(op_type_temp == T_CASE_ELSE||
                                                        op_type_temp == T_CASE_WHEN)
                                                {
                                                    if(t_case_begin_count == 0 ||
                                                            t_case_begin_count == t_op_case_count)
                                                    {
                                                        idx = idx -3;
                                                        break;
                                                    }
                                                }
                                                if (OB_SUCCESS != (ret = expr_[idx++].get_int(type_temp)))
                                                {
                                                    break;
                                                }
                                                else
                                                {
                                                    switch(type_temp)
                                                    {
                                                    case COLUMN_IDX:
                                                        idx++;idx++;op_type_temp = 0;
                                                        break;
                                                    case CONST_OBJ:
                                                    case PARAM_IDX:
                                                    case SYSTEM_VAR:
                                                    case TEMP_VAR:
                                                    case CUR_TIME_OP:
                                                    case CUR_DATE_OP:
                                                    case CUR_TIME_HMS_OP:
                                                        idx++;op_type_temp = 0;
                                                        break;
                                                    case OP:
                                                    {
                                                        if (OB_SUCCESS != (ret = expr_[idx].get_int(op_type_temp)))
                                                        {
                                                            break;
                                                        }
                                                        else
                                                        {
                                                            switch(op_type_temp)
                                                            {

                                                            case T_OP_EXISTS:case T_OP_POS:case T_OP_NEG:case T_OP_ADD: case T_OP_MINUS:
                                                            case T_OP_MUL:case T_OP_DIV:case T_OP_REM:case T_OP_POW:case T_OP_MOD:
                                                            case T_OP_LE:case T_OP_LT:case T_OP_EQ:case T_OP_GE:case T_OP_GT:case T_OP_NE:
                                                            case T_OP_LIKE:case T_OP_NOT_LIKE:case T_OP_AND:case T_OP_OR:case T_OP_NOT:
                                                            case T_OP_IS:
                                                            case T_OP_IS_NOT:
                                                            case T_OP_BTW:
                                                            case T_OP_NOT_BTW:
                                                            case T_OP_CNN:
                                                            case T_OP_IN:
                                                            case T_OP_NOT_IN:
                                                            case T_OP_ARG_CASE:
                                                            case T_OP_CASE:
                                                            case T_OP_ROW:
                                                            case T_OP_LEFT_PARAM_END:
                                                            case T_OP_CAST_THIN:  //add  peiouya [EXPIRE_INFO_NOT_ARITHMETIC_FIX] 20160608
                                                                idx++;idx++;
                                                                break;
                                                            case T_CUR_TIME_OP:

                                                                break;
                                                            case T_FUN_SYS:
                                                                idx++;idx++;idx++;
                                                                break;
                                                            case T_CASE_BEGIN:
                                                            case T_CASE_WHEN:
                                                            case T_CASE_THEN:
                                                            case T_CASE_ELSE:
                                                            case T_CASE_BREAK:
                                                            case T_CASE_END:
                                                                idx++;idx++;
                                                                break;
                                                            default:
                                                                ret = OB_INVALID_ARGUMENT;
                                                                break;
                                                            }
                                                        }
                                                    }
                                                        break;
                                                    default: ret = OB_INVALID_ARGUMENT;op_type_temp = 0;
                                                        break;
                                                    }
                                                }
                                            }
                                        }
                                    }
                                        break;
                                    case T_CASE_ELSE:
                                        else_is_null = false;
                                        continue;
                                        break;
                                    case T_CASE_BREAK:
                                    {
                                        int64_t type_temp = 0;
                                        int64_t op_type_temp =0;
                                        int t_case_begin_count =0;
                                        int t_op_case_end_count =0;
                                        bool can_break = false;
                                        for(int i= idx ;i<expr_.count ();i++)
                                        {
                                            if((op_type_temp == T_CASE_END && can_break)
                                                    ||(op_type_temp == T_CASE_END&&t_case_begin_count == 0))
                                            {
                                                idx = idx -3;
                                                break;
                                            }
                                            if(op_type_temp == T_CASE_BEGIN)
                                            {
                                                t_case_begin_count ++;
                                            }
                                            if(op_type_temp == T_CASE_END)
                                            {
                                                t_op_case_end_count ++;
                                            }
                                            if(op_type_temp == T_CASE_END)
                                            {
                                                if(t_case_begin_count == t_op_case_end_count)
                                                {
                                                   can_break = true;
                                                }
                                            }
//                                            if(op_type_temp == T_CASE_END)
//                                            {
//                                                idx = idx -3;
//                                                break;
//                                            }
                                            if (OB_SUCCESS != (ret = expr_[idx++].get_int(type_temp)))
                                            {
                                                break;
                                            }
                                            else
                                            {
                                                switch(type_temp)
                                                {
                                                case COLUMN_IDX:
                                                    idx++;idx++;
                                                    break;
                                                case CONST_OBJ:
                                                case PARAM_IDX:
                                                case SYSTEM_VAR:
                                                case TEMP_VAR:
                                                case CUR_TIME_OP:
                                                case CUR_DATE_OP:
                                                case CUR_TIME_HMS_OP:
                                                    idx++;
                                                    break;
                                                case OP:
                                                {
                                                    if (OB_SUCCESS != (ret = expr_[idx].get_int(op_type_temp)))
                                                    {
                                                        break;
                                                    }
                                                    else
                                                    {
                                                        switch(op_type_temp)
                                                        {

                                                        case T_OP_EXISTS:
                                                        case T_OP_POS:
                                                        case T_OP_NEG:
                                                        case T_OP_ADD:
                                                        case T_OP_MINUS:
                                                        case T_OP_MUL:
                                                        case T_OP_DIV:
                                                        case T_OP_REM:
                                                        case T_OP_POW:
                                                        case T_OP_MOD:
                                                        case T_OP_LE:
                                                        case T_OP_LT:
                                                        case T_OP_EQ:
                                                        case T_OP_GE:
                                                        case T_OP_GT:
                                                        case T_OP_NE:
                                                        case T_OP_LIKE:
                                                        case T_OP_NOT_LIKE:
                                                        case T_OP_AND:
                                                        case T_OP_OR:
                                                        case T_OP_NOT:
                                                        case T_OP_IS:
                                                        case T_OP_IS_NOT:
                                                        case T_OP_BTW:
                                                        case T_OP_NOT_BTW:
                                                        case T_OP_CNN:
                                                        case T_OP_IN:
                                                        case T_OP_NOT_IN:
                                                        case T_OP_ARG_CASE:
                                                        case T_OP_CASE:
                                                        case T_OP_ROW:
                                                        case T_OP_LEFT_PARAM_END:
                                                        case T_OP_CAST_THIN:  //add  peiouya [EXPIRE_INFO_NOT_ARITHMETIC_FIX] 20160608
                                                            idx++;idx++;
                                                            break;
                                                        case T_CUR_TIME_OP:

                                                            break;
                                                        case T_FUN_SYS:
                                                            idx++;idx++;idx++;
                                                            break;//add wanglei [case when fix] 20160323:b
                                                        case T_CASE_BEGIN:
                                                        case T_CASE_WHEN:
                                                        case T_CASE_THEN:
                                                        case T_CASE_ELSE:
                                                        case T_CASE_BREAK:
                                                        case T_CASE_END:
                                                            idx ++;idx++;
                                                            break;
                                                        default:
                                                            ret = OB_INVALID_ARGUMENT;
                                                            break;
                                                        }
                                                    }
                                                } break;
                                                default: ret = OB_INVALID_ARGUMENT;
                                                    break;
                                                }
                                            }
                                        }
                                    }
                                        break;
                                    case T_CASE_END:
                                    {
                                        if(else_is_null)
                                            stack_[idx_i++] = null_result;
                                        continue;
                                    }
                                        break;
                                    case  T_OP_ARG_CASE:
                                    case  T_OP_CASE:
                                    {
                                        if (OB_LIKELY(OB_SUCCESS ==
                                                      (ret = (this->call_func[value - T_MIN_OP - 1])(stack_, idx_i, result,
                                                                                                     *extra_params))))
                                        {
                                            stack_[idx_i++] = result;
                                            ret = OB_SUCCESS;
                                        }
                                        else if (OB_NO_RESULT == ret)
                                        {
                                            // nop
                                            ret = OB_SUCCESS;
                                        }
                                        else
                                        {
                                            TBSYS_LOG(WARN, "call calculation function error [value:%ld, idx_i:%d, err:%d]",value, idx_i, ret);
                                        }
                                        return ret;
                                    }
                                        break;
                                    default:
                                    {
                                        if (OB_LIKELY(OB_SUCCESS ==
                                                      (ret = (this->call_func[value - T_MIN_OP - 1])(stack_, idx_i, result,
                                                                                                     *extra_params))))
                                        {
                                            stack_[idx_i++] = result;
                                            ret = OB_SUCCESS;
                                        }
                                        else if (OB_NO_RESULT == ret)
                                        {
                                            // nop
                                            ret = OB_SUCCESS;
                                        }
                                        else
                                        {
                                            TBSYS_LOG(WARN, "call calculation function error [value:%ld, idx_i:%d, err:%d]",value, idx_i, ret);
                                        }
                                    }
                                        break;
                                    }

                                }
                                else if (ret == OB_SUCCESS && OB_LIKELY(OB_SUCCESS ==
                                                   (ret = (this->call_func[value - T_MIN_OP - 1])(stack_, idx_i, result,
                                                                                                  *extra_params))))
                                {
                                    stack_[idx_i++] = result;
                                    ret = OB_SUCCESS;
                                }
                                else if (OB_NO_RESULT == ret)
                                {
                                    // nop
                                    ret = OB_SUCCESS;
                                }
                                else
                                {
                                    TBSYS_LOG(WARN, "call calculation function error [value:%ld, idx_i:%d, err:%d]",value, idx_i, ret);
                                }

                            }//add dolphin[in post-expression optimization]@20150921
                        }
                    }
                }//add tianz [SubQuery_for_Instmt] [JHOBv0.1] 20140408 /*Exp:match the new else branch*/
                break;
            case UPS_TIME_OP:
                if (OB_SUCCESS != (ret = sys_func_current_timestamp(stack_, idx_i, result, *extra_params)))
                {
                    TBSYS_LOG(WARN, "failed to call sys_func_current_timestamp, err=%d", ret);
                }
                else
                {
                    stack_[idx_i++] = result;
                }
                break;
                //add tianz [SubQuery_for_Instmt] [JHOBv0.1] 20140408:b
                /*Exp:for QUERY_ID,set special mark :-1  */
            case QUERY_ID:
                if(OB_SUCCESS != (ret = expr_[idx++].get_int(value)))
                {
                    TBSYS_LOG(WARN,"get_int error [err:%d]", ret);
                }
                else
                {
                    int64_t temp_value = -1;
                    result.set_int(temp_value);
                    stack_[idx_i++] = result;
                }
                break;
                //add 20140408:e
            default:
                ret = OB_ERR_UNEXPECTED;
                TBSYS_LOG(WARN,"unexpected [type:%ld]", type);
                break;
            } // end switch
        }   // end else
        if (quick_path)
            break;
    } // end while
    return ret;
}
//add wanglei [case when fix] 20160516:e
int ObPostfixExpression::get_var_obj(
        ObPostExprNodeType type,
        const ObObj& expr_node,
        const ObObj*& val) const
{
    int ret = OB_SUCCESS;
    ObResultSet *result_set = NULL;
    if (type != PARAM_IDX && type != SYSTEM_VAR && type != TEMP_VAR && type != CUR_TIME_OP
            //add liuzy [datetime func] 20150910:b
            && type != CUR_TIME_HMS_OP && type != CUR_DATE_OP
            //add 20150910:e
            )
    {
        val = &expr_node;
    }
    else
    {
        if (!owner_op_ || !owner_op_->get_phy_plan()
                || !(result_set = owner_op_->get_phy_plan()->get_result_set()))
        {
            ret = OB_ERR_UNEXPECTED;
            TBSYS_LOG(WARN, "Can not get result set.err=%d", ret);
        }
        else if (type == PARAM_IDX)
        {
            int64_t param_idx = OB_INVALID_INDEX;
            if ((ret = expr_node.get_int(param_idx)) != OB_SUCCESS)
            {
                TBSYS_LOG(ERROR, "Can not get param index, ret=%d", ret);
            }
            else if (param_idx < 0 || param_idx >= result_set->get_params().count())
            {
                ret = OB_ERR_ILLEGAL_INDEX;
                TBSYS_LOG(ERROR, "Wrong index of question mark position, pos = %ld\n", param_idx);
            }
            else
            {
                val = result_set->get_params().at(param_idx);
            }
        }
        else if (type == SYSTEM_VAR || type == TEMP_VAR)
        {
            ObString var_name;
            ObSQLSessionInfo *session_info = result_set->get_session();
            if (!session_info)
            {
                ret = OB_ERR_UNEXPECTED;
                TBSYS_LOG(WARN, "Can not get session info.err=%d", ret);
            }
            else if ((ret = expr_node.get_varchar(var_name)) != OB_SUCCESS)
            {
                TBSYS_LOG(ERROR, "Can not get variable name");
            }
            else if (type == SYSTEM_VAR
                     && (val = session_info->get_sys_variable_value(var_name)) == NULL)
            {
                ret = OB_ERR_VARIABLE_UNKNOWN;
                TBSYS_LOG(USER_ERROR, "System variable %.*s does not exists", var_name.length(), var_name.ptr());
            }
            else if (type == TEMP_VAR
                     && (val = session_info->get_variable_value(var_name)) == NULL)
            {
                ret = OB_ERR_VARIABLE_UNKNOWN;
                TBSYS_LOG(USER_ERROR, "Variable %.*s does not exists", var_name.length(), var_name.ptr());
            }
        }
        else if (type == CUR_TIME_OP)
        {
            if ((val = result_set->get_cur_time_place()) == NULL)
            {
                ret = OB_ERR_UNEXPECTED;
                TBSYS_LOG(WARN, "Can not get current time. err=%d", ret);
            }
        }
        //add liuzy [datetime func] 20150910:b
        else if (type == CUR_DATE_OP)
        {
            if ((val = result_set->get_cur_time_place(type)) == NULL)
            {
                ret = OB_ERR_UNEXPECTED;
                TBSYS_LOG(WARN, "Can not get current date. err=%d", ret);
            }
        }
        else if (type == CUR_TIME_HMS_OP)
        {
            if ((val = result_set->get_cur_time_place(type)) == NULL)
            {
                ret = OB_ERR_UNEXPECTED;
                TBSYS_LOG(WARN, "Can not get current time for HMS. err=%d", ret);
            }
        }
        //add 20150910:e
    }
    return ret;
}

int ObPostfixExpression::is_const_expr(bool &is_type) const
{
    return check_expr_type((int64_t)CONST_OBJ, is_type, 3);
}

int ObPostfixExpression::is_column_index_expr(bool &is_type) const
{
    return ObPostfixExpression::check_expr_type((int64_t)COLUMN_IDX, is_type, 4);
}


int ObPostfixExpression::check_expr_type(const int64_t type_val, bool &is_type, const int64_t stack_size) const
{
    int err = OB_SUCCESS;
    int64_t expr_type = -1;
    is_type = false;
    if (expr_.count() == stack_size)
    {
        if (ObIntType == expr_[0].get_type())
        {
            if (OB_SUCCESS != (err = expr_[0].get_int(expr_type)))
            {
                TBSYS_LOG(WARN, "fail to get int value.err=%d", err);
            }
            else if (type_val == expr_type)
            {
                is_type = true;
            }
        }
    }
    return err;
}

int ObPostfixExpression::get_column_index_expr(uint64_t &tid, uint64_t &cid, bool &is_type) const
{
    int err = OB_SUCCESS;
    int64_t expr_type = -1;
    is_type = false;
    if (4 == expr_.count())
    {
        if (ObIntType == expr_[0].get_type())
        {
            if (OB_SUCCESS != (err = expr_[0].get_int(expr_type)))
            {
                TBSYS_LOG(WARN, "fail to get int value.err=%d", err);
            }
            else if ((int64_t)COLUMN_IDX == expr_type)
            {
                if (OB_SUCCESS != (err = expr_[1].get_int((int64_t&)tid)))
                {
                    TBSYS_LOG(WARN, "fail to get int value.err=%d", err);
                }
                else if (OB_SUCCESS != (err = expr_[2].get_int((int64_t&)cid)))
                {
                    TBSYS_LOG(WARN, "fail to get int value.err=%d", err);
                }
                else
                {
                    is_type = true;
                }
            }
        }
    }
    return err;
}
//add wanglei [second index fix] 20160425:b
 int ObPostfixExpression::get_tid_indexs( ObArray<uint64_t> & index_list)
 {
     int ret = OB_SUCCESS;
     int64_t type = 0;
     int64_t count = expr_.count();
     int64_t idx = 0;
     int32_t column_count=0;
     int64_t tmp_index = OB_INVALID_ID;
     while(idx<count)
     {
         if (OB_SUCCESS != (ret = expr_[idx].get_int(type)))
         {
             TBSYS_LOG(WARN, "Fail to get op type. unexpected! ret=%d", ret);
             break;
         }
         else if(type == COLUMN_IDX)
         {
             tmp_index=idx+1;
             index_list.push_back (tmp_index);
             column_count++;
             idx = idx + get_type_num(idx,COLUMN_IDX);
         }
         //add fanqiushi_index_prepare
         //mod dragon 2017-1-12 b
         //代码格式调整，并补充一些后来添加的类型
         else if(type == OP
                 || type == CONST_OBJ
                 || type == QUERY_ID
                 || type == END
                 || type == UPS_TIME_OP
                 || type == PARAM_IDX
                 || type == SYSTEM_VAR
                 || type == TEMP_VAR
                 || type == CUR_TIME_OP
                 || type == CUR_TIME_HMS_OP
                 || type == CUR_DATE_OP
                 || type == END_TYPE)
         {
           idx = idx + get_type_num(idx,type);
         }
         /*---old---
         else if(type == OP ||type == COLUMN_IDX||type == T_OP_ROW||type == CONST_OBJ||type == QUERY_ID||type == END||type == UPS_TIME_OP)
         {
             idx = idx + get_type_num(idx,type);
         }
         ---old---*/
         //mod dragon e
         else
         {
             ret=OB_ERROR;
             TBSYS_LOG(WARN,"wrong expr type: %ld",type);
             break;
         }
         //add:e
     }
     return ret;
 }
//add wanglei [second index fix] 20160425:e
//add fanqiushi_index
int ObPostfixExpression::change_tid(uint64_t& array_index) //获得表达式中记录列的tid的ObObj在ObObj数组里的下标
{

    int ret = OB_SUCCESS;
    int64_t type = 0;
    int64_t count = expr_.count();
    int64_t idx = 0;
    int32_t column_count=0;
    int64_t tmp_index = OB_INVALID_ID;
    while(idx<count)
    {
        if (OB_SUCCESS != (ret = expr_[idx].get_int(type)))
        {
            TBSYS_LOG(WARN, "Fail to get op type. unexpected! ret=%d", ret);
            break;
        }
        else if(type == COLUMN_IDX)
        {
            tmp_index=idx+1;
            column_count++;
            idx = idx + get_type_num(idx,COLUMN_IDX);
        }
        //add fanqiushi_index_prepare
        //mod dragon 2017-1-12 b
        //代码格式调整，并补充一些后来添加的类型
        else if(type == OP
                || type == CONST_OBJ
                || type == QUERY_ID
                || type == END
                || type == UPS_TIME_OP
                || type == PARAM_IDX
                || type == SYSTEM_VAR
                || type == TEMP_VAR
                || type == CUR_TIME_OP
                || type == CUR_TIME_HMS_OP
                || type == CUR_DATE_OP
                || type == END_TYPE)
        {
          idx = idx + get_type_num(idx,type);
        }
        /*---old---
        else if(type == OP ||type == COLUMN_IDX||type == T_OP_ROW||type == CONST_OBJ||type == QUERY_ID||type == END||type == UPS_TIME_OP)
        {
            idx = idx + get_type_num(idx,type);
        }
        ---old---*/
        //mod dragon e
        else
        {
            ret=OB_ERROR;
            TBSYS_LOG(WARN,"wrong expr type: %ld",type);
            break;
        }
        //add:e
    }
    if(column_count==1&& ret == OB_SUCCESS)
        array_index=(uint64_t)tmp_index;
    else
        ret=OB_ERROR;
    return ret;
}

int ObPostfixExpression::get_cid(uint64_t& cid)  //获得表达式中列的cid，如果表达式中有多个列，则报�?
 {
    int ret = OB_SUCCESS;
    int64_t type = 0;
    int64_t count = expr_.count();
    int64_t idx = 0;
    int32_t column_count=0;
    int64_t tmp_cid = OB_INVALID_ID;
    int64_t tmp_tid = OB_INVALID_ID;
    while(idx<count)
    {
        if (OB_SUCCESS != (ret = expr_[idx].get_int(type)))
        {
            TBSYS_LOG(WARN, "Fail to get op type. unexpected! ret=%d", ret);
            break;
        }
        else if(type == COLUMN_IDX)
        {
            if (OB_SUCCESS != (ret = expr_[idx+1].get_int(tmp_tid)))
            {
                //return_ret=false;
                TBSYS_LOG(WARN, "Fail to get op type. unexpected! ret=%d", ret);
                break;
            }
            else if (OB_SUCCESS != (ret = expr_[idx+2].get_int(tmp_cid)))
            {
                //return_ret=false;
                TBSYS_LOG(WARN, "Fail to get op type. unexpected! ret=%d", ret);
                break;
            }
            else
            {
                column_count++;
                idx = idx + get_type_num(idx,COLUMN_IDX);
            }
        }
        //add fanqiushi_index_prepare
        //mod dragon 2017-1-12 b
        //代码格式调整，并补充一些后来添加的类型
        else if(type == OP
                || type == CONST_OBJ
                || type == QUERY_ID
                || type == END
                || type == UPS_TIME_OP
                || type == PARAM_IDX
                || type == SYSTEM_VAR
                || type == TEMP_VAR
                || type == CUR_TIME_OP
                || type == CUR_TIME_HMS_OP
                || type == CUR_DATE_OP
                || type == END_TYPE)
        {
          idx = idx + get_type_num(idx,type);
        }
        /*---old---
        else if(type == OP ||type == COLUMN_IDX||type == T_OP_ROW||type == CONST_OBJ||type == QUERY_ID||type == END||type == UPS_TIME_OP)
        {
            idx = idx + get_type_num(idx,type);
        }
        ---old---*/
        //mod dragon e
        else
        {
            ret=OB_ERROR;
            TBSYS_LOG(WARN,"wrong expr type: %ld",type);
            break;
        }
        //add:e
    }
    if(column_count==1&&ret==OB_SUCCESS)
        cid=(uint64_t)tmp_cid;
    else
        ret=OB_ERROR;
    return ret;
}

//add:e

//add tianz [SubQuery_for_Instmt] [JHOBv0.1] 20140408:b

/*Exp:used before specific value replace the mark of sub_query,
    * pop other information of expression out to temp expression ,
    * then remove the mark of sub_query
    * @param sub_query_index[in] sub_query position in current expr
    */
int ObPostfixExpression::delete_sub_query_info(int sub_query_index)
{
    int ret= OB_SUCCESS;
    int64_t idx = 0;
    int64_t type = BEGIN_TYPE;
    //find sub_query mark in expr_
    while(OB_SUCCESS == ret && sub_query_index>0)
    {
        idx = idx + get_type_num(idx,type);
        if (OB_SUCCESS != (ret = expr_[idx].get_int(type)))
        {
            TBSYS_LOG(WARN, "Fail to get type. unexpected! ret=%d", ret);
            ret = OB_ERR_UNEXPECTED;
            break;
        }
        else if(QUERY_ID == type)
        {
            sub_query_index--;
        }
    }
    //push other information to temp_expr_
    ObObj temp_obj;
    if(OB_SUCCESS == ret)
    {
        idx = idx + 2;//sub select's two obj
        while(idx < expr_.count())
        {
            expr_.pop_back(temp_obj);
            temp_expr_.push_back(temp_obj);
        }
    }
    //remove sub_query's two elements
    expr_.pop_back();
    expr_.pop_back();
    return ret;
}

/*Exp:get sub_query selected column number
    * @param sub_query_index[in] ,sub_query's number in current expression
    * @param num[out],sub_query selected column number
    * @param special_sub_query[out],sub_query is a separate expression, not belong to in or not in expression
    */
int ObPostfixExpression::get_sub_query_column_num(int sub_query_index,int64_t &num,bool &special_sub_query)
{
    int ret = OB_SUCCESS;
    int64_t idx = 0;
    int64_t type = BEGIN_TYPE;
    //find sub_query mark in expr_
    while(OB_SUCCESS == ret && sub_query_index>0)
    {
        idx = idx + get_type_num(idx,type);
        if (OB_SUCCESS != (ret = expr_[idx].get_int(type)))
        {
            TBSYS_LOG(WARN, "Fail to get type. unexpected! ret=%d", ret);
            ret = OB_ERR_UNEXPECTED;
            break;
        }
        else if(QUERY_ID == type)
        {
            sub_query_index--;
        }
    }
    //check is special sub_query (sub_query is a independent expression,nto belong to IN or NOT_IN expression)
    if(OB_SUCCESS != (ret = expr_[++idx].get_int(num)))
    {
        TBSYS_LOG(WARN, "Fail to get sub query column num. unexpected! ret=%d", ret);
        ret = OB_ERR_UNEXPECTED;
    }
    else if(OB_SUCCESS != (ret = expr_[++idx].get_int(type)))
    {
        TBSYS_LOG(WARN, "Fail to get type. unexpected! ret=%d", ret);
        ret = OB_ERR_UNEXPECTED;
    }
    else if(OP == type)
    {
        if(OB_SUCCESS != (ret = expr_[++idx].get_int(type)))
        {
            TBSYS_LOG(WARN, "Fail to get operator type. unexpected! ret=%d", ret);
            ret = OB_ERR_UNEXPECTED;
        }
        else if(T_OP_IN == type ||T_OP_NOT_IN == type)
        {
            special_sub_query = false;
        }
        else
        {
            special_sub_query = true;
        }
    }
    else
    {
        special_sub_query = true;
    }
    return ret;
}
/*Exp:get sub_query selected column number
    * @param sub_query_index[in] ,sub_query's number in current expression
    * @param num[out],sub_query selected column number
    * @param special_sub_query[out],sub_query is a separate expression, not belong to in or not in expression
    * @param special_case:T_OP_EQ || T_OP_LE ||T_OP_LT || T_OP_GE || T_OP_GT || T_OP_NE
    */
//add xionghui [fix equal subquery bug] 20150122 b:
int ObPostfixExpression::get_sub_query_column_num_new(int sub_query_index,int64_t &num,bool &special_sub_query,bool &special_case)
{
    int ret = OB_SUCCESS;
    int64_t idx = 0;
    int64_t type = BEGIN_TYPE;
    //find sub_query mark in expr_
    while(OB_SUCCESS == ret && sub_query_index>0)
    {
        idx = idx + get_type_num(idx,type);
        if (OB_SUCCESS != (ret = expr_[idx].get_int(type)))
        {
            TBSYS_LOG(WARN, "Fail to get type. unexpected! ret=%d", ret);
            ret = OB_ERR_UNEXPECTED;
            break;
        }
        else if(QUERY_ID == type)
        {
            sub_query_index--;
        }
    }
    //check is special sub_query (sub_query is a independent expression,nto belong to IN or NOT_IN expression)
    if(OB_SUCCESS != (ret = expr_[++idx].get_int(num)))
    {
        TBSYS_LOG(WARN, "Fail to get sub query column num. unexpected! ret=%d", ret);
        ret = OB_ERR_UNEXPECTED;
    }
    else if(OB_SUCCESS != (ret = expr_[++idx].get_int(type)))
    {
        TBSYS_LOG(WARN, "Fail to get type. unexpected! ret=%d", ret);
        ret = OB_ERR_UNEXPECTED;
    }
    else if(OP == type)
    {
        if(OB_SUCCESS != (ret = expr_[++idx].get_int(type)))
        {
            TBSYS_LOG(WARN, "Fail to get operator type. unexpected! ret=%d", ret);
            ret = OB_ERR_UNEXPECTED;
        }
        else if(T_OP_IN == type ||T_OP_NOT_IN == type)
        {
            special_sub_query = false;
        }
        else if(type == T_OP_EQ || type == T_OP_LE || type ==T_OP_LT ||
                type == T_OP_GE || type == T_OP_GT || type == T_OP_NE
                //add by xionghui [subquery_final] 20160216 b:
                || type == T_OP_LIKE || type == T_OP_NOT_LIKE
                //add e:
                )
        {
            special_case = true;
            special_sub_query = true;
        }
        else
        {
            special_sub_query = true;
        }
    }
    else
    {
        special_sub_query = true;
    }
    return ret;
}
//add e:

/*Exp:used after specific value replace the mark of sub_query,
    * push other information of expression back to modified expression
    */
int ObPostfixExpression::complete_sub_query_info()
{
    int ret = OB_SUCCESS;
    int64_t count = temp_expr_.count();//int64_t count = 4;
    ObObj temp_obj;
    while(count>0)
    {
        temp_expr_.pop_back(temp_obj);
        expr_.push_back(temp_obj);
        count--;
    }
    return ret;
}

/*Exp:used for specific value replace the mark of sub_query,
    * push specific value back to current expression
    * @param obj_value[in] specific value which will replace the mark of sub_query
    */
int ObPostfixExpression::add_expr_in_obj(ObObj &obj_value)
{
    int ret = OB_SUCCESS;
    ObObj item_type;
    // type mark;
    item_type.set_int(CONST_OBJ);
    ObObj obj2_value;
    if (obj_value.get_type() == ObVarcharType)
    {
      if (OB_SUCCESS != (ret = str_buf_.write_obj(obj_value,&obj2_value)))
      {
        TBSYS_LOG(WARN,"fail to write object to string buffer.ret=%d",ret);
      }
      else if (OB_SUCCESS != (ret = expr_.push_back(item_type)))
      {
        TBSYS_LOG(WARN,"fail to push back item value.ret=%d",ret);
      }
      else if (OB_SUCCESS != (ret = expr_.push_back(obj2_value)))
      {
        TBSYS_LOG(WARN,"fail to push object to expr array.ret=%d",ret);
      }
    }
    else if (obj_value.get_type() == ObDecimalType)
    {
      if (OB_SUCCESS != (ret = str_buf_.write_obj_varchar_to_decimal(obj_value, &obj2_value)))
      {
          TBSYS_LOG(WARN, "fail to write object to string buffer. ret=%d", ret);
      }
      else
      {
        obj2_value.set_precision(obj_value.get_precision());
        obj2_value.set_scale(obj_value.get_scale());
      }
      if (OB_SUCCESS != ret)
      {
      }
      else if (OB_SUCCESS != (ret = expr_.push_back(item_type)))
      {
        TBSYS_LOG(WARN,"fail to push back item value.ret=%d",ret);
      }
      else if (OB_SUCCESS != (ret = expr_.push_back(obj2_value)))
      {
        TBSYS_LOG(WARN,"fail to push object to expr array.ret=%d",ret);
      }
    }
    else
    {
      if (OB_SUCCESS != (ret = expr_.push_back(item_type)))
      {
        TBSYS_LOG(WARN,"fail to push back item value.ret=%d",ret);
      }
      else if (OB_SUCCESS != (ret = expr_.push_back(obj_value)))
      {
        TBSYS_LOG(WARN,"fail to push object to expr array.ret=%d",ret);
      }
    }
    return ret;
}

/*Exp: get different ObPostExprNodeType's params number
    * @param idx[in] current position in expr
    * @param type[in] current ObPostExprNodeType
    * @retrun current ObPostExprNodeType's params number
    */
int64_t ObPostfixExpression::get_type_num(int64_t idx,int64_t type)
{
    int64_t num = 0;
    int ret = OB_SUCCESS;
    if(type == BEGIN_TYPE)
    {
        num = 0;
    }
    else if (type == OP)
    {
        num = 3;
        int64_t op_type = 0;
        if (OB_SUCCESS != (ret = expr_[idx+1].get_int(op_type)))
        {
            TBSYS_LOG(WARN, "Fail to get op type. unexpected! ret=%d", ret);
        }
        else if (T_FUN_SYS == op_type)
        {
            ++num;
        }
    }
    else if (type == COLUMN_IDX || type == T_OP_ROW)
    {
        num = 3;
    }
    else if (type == CONST_OBJ ||type == QUERY_ID||type == PARAM_IDX||type==TEMP_VAR)//add wanglei TEMP_VAR [second index fix] 20160513
    {
        num = 2;
    }
    //mod dragon 2017-1-12 b
    //有问题的代码见old部分
    else if(type == CUR_TIME_OP || type == CUR_TIME_HMS_OP || type == CUR_DATE_OP)
    {
      num = 2;
    }
    else if (type == END || type == UPS_TIME_OP)
    {
      num = 1;
    }
    /*---old---
    else if (type == END || type == UPS_TIME_OP||CUR_TIME_OP
             ||CUR_TIME_HMS_OP
             ||CUR_DATE_OP
             ||UPS_TIME_OP)
    {
        num = 1;
    }
    ---old---*/
    //mod dragon e
    else
    {
        TBSYS_LOG(WARN, "Unkown type %ld", type);
        return -1;
    }
    return num;
}
// add 20140408:e

//add tianz [SubQuery_for_Instmt] [JHOBv0.1] 20140523:b

/*Exp:check whether current expression satisfy the condition of use bloomfilter
    * OR and NOT_IN expression do not use bloomfilter
    */
bool ObPostfixExpression::is_satisfy_bloomfilter()
{
    int ret = OB_SUCCESS;
    bool is_satisfy_bloomfilter = true;
    int64_t type = 0;
    int64_t count = expr_.count();
    int64_t idx = 0;
    while(idx<count)
    {
        if (OB_SUCCESS != (ret = expr_[idx].get_int(type)))
        {
            TBSYS_LOG(WARN, "Fail to get op type. unexpected! ret=%d", ret);
        }
        else if(type == OP)
        {
            if (OB_SUCCESS != (ret = expr_[idx+1].get_int(type)))
            {
                TBSYS_LOG(WARN, "Fail to get op type. unexpected! ret=%d", ret);
            }
            else if (T_OP_OR == type || T_OP_NOT_IN == type)
            {
                is_satisfy_bloomfilter = false;
                break;
            }
            else
            {
                idx = idx + get_type_num(idx,OP);
            }
        }
        else
        {
            idx = idx + get_type_num(idx,type);
        }
    }
    return is_satisfy_bloomfilter;
}
//add 20140523:e
inline static bool test_expr_int(const ObPostfixExpression::ExprArray &expr, int i, int64_t expected)
{
    int64_t val = 0;
    return ObIntType == expr[i].get_type()
            && OB_SUCCESS == expr[i].get_int(val)
            && expected == val;
}
/*add by wanglei [semi join] 20151106*/
bool ObPostfixExpression::is_LT_OR_LE_cond() const
{
    bool ret = false;
    if (test_expr_int(expr_, 7, T_OP_LT)||test_expr_int(expr_, 7, T_OP_LE))
    {
        ret=true;
    }
    return ret;
}
/*add by wanglei [semi join] 20151106*/
bool ObPostfixExpression::is_GT_OR_GE_cond() const
{
    bool ret = false;
    if (test_expr_int(expr_, 7, T_OP_GT)||test_expr_int(expr_, 7, T_OP_GE))
    {
        ret=true;
    }
    return ret;
}
/*add by wanglei [semi join] 20151106*/
bool ObPostfixExpression::is_otherjoin_cond(ExprItem::SqlCellInfo &c1, ExprItem::SqlCellInfo &c2) const
{
    bool ret = false;
    // COL_IDX|tid|cid|COL_IDX|tid|cid|OP|LT|2|END
    if (expr_.count() == 10)
    {
        if (test_expr_int(expr_, 0, COLUMN_IDX)
                && test_expr_int(expr_, 3, COLUMN_IDX)
                && test_expr_int(expr_, 6, OP)
                && test_expr_int(expr_, 7, T_OP_LT)
                && test_expr_int(expr_, 8, 2))
        {
            ret = true;
            int64_t val = 0;
            if (ObIntType == expr_[1].get_type()
                    && OB_SUCCESS == expr_[1].get_int(val))
            {
                c1.tid = val;
            }
            else
            {
                ret = false;
            }
            if (ObIntType == expr_[2].get_type()
                    && OB_SUCCESS == expr_[2].get_int(val))
            {
                c1.cid = val;
            }
            else
            {
                ret = false;
            }
            if (ObIntType == expr_[4].get_type()
                    && OB_SUCCESS == expr_[4].get_int(val))
            {
                c2.tid = val;
            }
            else
            {
                ret = false;
            }
            if (ObIntType == expr_[5].get_type()
                    && OB_SUCCESS == expr_[5].get_int(val))
            {
                c2.cid = val;
            }
            else
            {
                ret = false;
            }
        }
        if (test_expr_int(expr_, 0, COLUMN_IDX)
                && test_expr_int(expr_, 3, COLUMN_IDX)
                && test_expr_int(expr_, 6, OP)
                && test_expr_int(expr_, 7, T_OP_GT)
                && test_expr_int(expr_, 8, 2))
        {
            ret = true;
            int64_t val = 0;
            if (ObIntType == expr_[1].get_type()
                    && OB_SUCCESS == expr_[1].get_int(val))
            {
                c1.tid = val;
            }
            else
            {
                ret = false;
            }
            if (ObIntType == expr_[2].get_type()
                    && OB_SUCCESS == expr_[2].get_int(val))
            {
                c1.cid = val;
            }
            else
            {
                ret = false;
            }
            if (ObIntType == expr_[4].get_type()
                    && OB_SUCCESS == expr_[4].get_int(val))
            {
                c2.tid = val;
            }
            else
            {
                ret = false;
            }
            if (ObIntType == expr_[5].get_type()
                    && OB_SUCCESS == expr_[5].get_int(val))
            {
                c2.cid = val;
            }
            else
            {
                ret = false;
            }
        }
        if (test_expr_int(expr_, 0, COLUMN_IDX)
                && test_expr_int(expr_, 3, COLUMN_IDX)
                && test_expr_int(expr_, 6, OP)
                && test_expr_int(expr_, 7, T_OP_LE)
                && test_expr_int(expr_, 8, 2))
        {
            ret = true;
            int64_t val = 0;
            if (ObIntType == expr_[1].get_type()
                    && OB_SUCCESS == expr_[1].get_int(val))
            {
                c1.tid = val;
            }
            else
            {
                ret = false;
            }
            if (ObIntType == expr_[2].get_type()
                    && OB_SUCCESS == expr_[2].get_int(val))
            {
                c1.cid = val;
            }
            else
            {
                ret = false;
            }
            if (ObIntType == expr_[4].get_type()
                    && OB_SUCCESS == expr_[4].get_int(val))
            {
                c2.tid = val;
            }
            else
            {
                ret = false;
            }
            if (ObIntType == expr_[5].get_type()
                    && OB_SUCCESS == expr_[5].get_int(val))
            {
                c2.cid = val;
            }
            else
            {
                ret = false;
            }
        }
        if (test_expr_int(expr_, 0, COLUMN_IDX)
                && test_expr_int(expr_, 3, COLUMN_IDX)
                && test_expr_int(expr_, 6, OP)
                && test_expr_int(expr_, 7, T_OP_GE)
                && test_expr_int(expr_, 8, 2))
        {
            ret = true;
            int64_t val = 0;
            if (ObIntType == expr_[1].get_type()
                    && OB_SUCCESS == expr_[1].get_int(val))
            {
                c1.tid = val;
            }
            else
            {
                ret = false;
            }
            if (ObIntType == expr_[2].get_type()
                    && OB_SUCCESS == expr_[2].get_int(val))
            {
                c1.cid = val;
            }
            else
            {
                ret = false;
            }
            if (ObIntType == expr_[4].get_type()
                    && OB_SUCCESS == expr_[4].get_int(val))
            {
                c2.tid = val;
            }
            else
            {
                ret = false;
            }
            if (ObIntType == expr_[5].get_type()
                    && OB_SUCCESS == expr_[5].get_int(val))
            {
                c2.cid = val;
            }
            else
            {
                ret = false;
            }
        }
    }
    return ret;
}
bool ObPostfixExpression::is_equijoin_cond(ExprItem::SqlCellInfo &c1, ExprItem::SqlCellInfo &c2) const
{
    bool ret = false;
    // COL_IDX|tid|cid|COL_IDX|tid|cid|OP|EQ|2|END
    if (expr_.count() == 10)
    {
        if (test_expr_int(expr_, 0, COLUMN_IDX)
                && test_expr_int(expr_, 3, COLUMN_IDX)
                && test_expr_int(expr_, 6, OP)
                && test_expr_int(expr_, 7, T_OP_EQ)
                && test_expr_int(expr_, 8, 2))
        {
            ret = true;
            int64_t val = 0;
            if (ObIntType == expr_[1].get_type()
                    && OB_SUCCESS == expr_[1].get_int(val))
            {
                c1.tid = val;
            }
            else
            {
                ret = false;
            }
            if (ObIntType == expr_[2].get_type()
                    && OB_SUCCESS == expr_[2].get_int(val))
            {
                c1.cid = val;
            }
            else
            {
                ret = false;
            }
            if (ObIntType == expr_[4].get_type()
                    && OB_SUCCESS == expr_[4].get_int(val))
            {
                c2.tid = val;
            }
            else
            {
                ret = false;
            }
            if (ObIntType == expr_[5].get_type()
                    && OB_SUCCESS == expr_[5].get_int(val))
            {
                c2.cid = val;
            }
            else
            {
                ret = false;
            }
        }
    }
    return ret;
}

bool ObPostfixExpression::is_simple_condition(bool real_val, uint64_t &column_id, int64_t &cond_op,
                                              ObObj &const_val, ObPostExprNodeType *val_type) const
{
    int err = OB_SUCCESS;
    int64_t type_val = -1;
    int64_t v_type = -1;
    int64_t cid = OB_INVALID_ID;
    bool is_simple_cond_type = false;

    do{
        if (expr_.count() == (3+2+3+1)) /*cid(3) + const_operand(2) + operator(3) + end(1) */
        {
            /* (1) cid */
            if (ObIntType != expr_[0].get_type())
            {
                // not int val, pass
                break;
            }
            else if(OB_SUCCESS != (err = expr_[0].get_int(type_val)))
            {
                TBSYS_LOG(WARN, "fail to get int value.err=%d", err);
                break;
            }
            else if (type_val != COLUMN_IDX)
            {
                break;
            }
            else if (OB_SUCCESS != (err = expr_[2].get_int(cid)))
            {
                TBSYS_LOG(WARN, "fail to get int value.err=%d", err);
                break;
            }
            /* (2) const_opr */
            else if (ObIntType != expr_[3].get_type())
            {
                // not int val, pass
                break;
            }
            else if (OB_SUCCESS != (err = expr_[3].get_int(v_type)))
            {
                TBSYS_LOG(WARN, "fail to get int value.err=%d", err);
                break;
            }
            else if (v_type != CONST_OBJ && v_type != PARAM_IDX
                     && v_type != SYSTEM_VAR && v_type != TEMP_VAR && v_type != CUR_TIME_OP
                     //add liuzy [datetime func] 20150910:b
                     && v_type != CUR_DATE_OP && v_type != CUR_TIME_HMS_OP
                     //add 20150910:e
                     )
            {
                break;
            }
            /* (3) op */
            else if (ObIntType != expr_[5].get_type())
            {
                // not int val, pass
                break;
            }
            else if (OB_SUCCESS != (err = expr_[5].get_int(type_val)))
            {
                TBSYS_LOG(WARN, "fail to get int value.err=%d", err);
                break;
            }
            else if (type_val != OP)
            {
                break;
            }
            else if (ObIntType != expr_[6].get_type())
            {
                // not int val, pass
                break;
            }
            else if (OB_SUCCESS != (err = expr_[6].get_int(type_val)))
            {
                TBSYS_LOG(WARN, "fail to get int value.err=%d", err);
                break;
            }
            else if ((type_val < T_OP_EQ || type_val >= T_OP_NE) && (type_val != T_OP_IS))
            {
                break;
            }
            /* (4) result */
            else
            {
                if (real_val)
                {
                    const ObObj *var_ptr = NULL;
                    if ((err = get_var_obj(static_cast<ObPostExprNodeType>(v_type), expr_[4], var_ptr)) == OB_SUCCESS)
                    {
                        const_val = *var_ptr;
                    }
                    else
                    {
                        TBSYS_LOG(WARN, "Fail to get real value.err=%d", err);
                        break;
                    }
                }
                else
                {
                    const_val = expr_[4];
                }
                if (val_type)
                {
                    *val_type = static_cast<ObPostExprNodeType>(v_type);
                }
                column_id = (uint64_t)cid;
                cond_op = type_val;
                is_simple_cond_type = true;
            }
        }
    } while(0);
    return is_simple_cond_type;
}
//add fanqiushi_index
int ObPostfixExpression::get_all_cloumn(ObArray<uint64_t> &column_index)  //获得表达式中所有列的存tid的ObObj在ObObj数组里的下标
{
    //bool return_ret=true;
    int ret = OB_SUCCESS;
    //bool return_ret = false;
    int64_t type = 0;
    int64_t count = expr_.count();
    int64_t idx = 0;
    //int32_t column_count=0;
    //int64_t tmp_cid = OB_INVALID_ID;
    // int64_t tid = OB_INVALID_ID;
    while(idx<count)
    {
        if (OB_SUCCESS != (ret = expr_[idx].get_int(type)))
        {
            //return_ret=false;
            TBSYS_LOG(WARN, "Fail to get op type. unexpected! ret=%d", ret);
            break;
        }
        else if(type == COLUMN_IDX)
        {
            column_index.push_back(idx+1);
            idx = idx + get_type_num(idx,COLUMN_IDX);
        }
        //add fanqiushi_index_prepare
        //mod dragon 2017-1-12 b
        //代码格式调整，并补充一些后来添加的类型
        else if(type == OP
                || type == CONST_OBJ
                || type == QUERY_ID
                || type == END
                || type == UPS_TIME_OP
                || type == PARAM_IDX
                || type == SYSTEM_VAR
                || type == TEMP_VAR
                || type == CUR_TIME_OP
                || type == CUR_TIME_HMS_OP
                || type == CUR_DATE_OP
                || type == END_TYPE)
        {
          idx = idx + get_type_num(idx,type);
        }
        /*---old---
        else if(type == OP ||type == COLUMN_IDX||type == T_OP_ROW||type == CONST_OBJ||type == QUERY_ID||type == END||type == UPS_TIME_OP)
        {
            idx = idx + get_type_num(idx,type);
        }
        ---old---*/
        //mod dragon e
        else
        {
            ret=OB_ERROR;
            TBSYS_LOG(WARN,"wrong expr type: %ld",type);
            break;
        }
        //add:e
    }
    return ret;
}

bool ObPostfixExpression::is_this_expr_can_use_index(uint64_t &index_tid,uint64_t main_tid,const ObSchemaManagerV2 *sm_v2)
{
    //判断该表达式是否能够使用索引。如果该表达式只有一列，并且是个等值或in表达式，并且该表达式的列的cid是主表main_tid的某一张索引表的第一主键，则该表达式能够使用索引
    int ret = OB_SUCCESS;
    bool return_ret = false;
    int64_t type = 0;
    int64_t tmp_type = 0;
    int64_t count = expr_.count();
    int64_t idx = 0;
    int32_t column_count=0;
    int32_t EQ_count=0;
    int32_t IN_count=0;
    int64_t cid = OB_INVALID_ID;
    int64_t tid = OB_INVALID_ID;
    //uint64_t tmp_index_tid= OB_INVALID_ID;
    while(idx<count)
    {
        if (OB_SUCCESS != (ret = expr_[idx].get_int(type)))
        {
            return_ret=false;
            TBSYS_LOG(WARN, "Fail to get op type. unexpected! ret=%d", ret);
            break;
        }
        else if(type == COLUMN_IDX)
        {
            if (OB_SUCCESS != (ret = expr_[idx+1].get_int(tid)))
            {
                return_ret=false;
                TBSYS_LOG(WARN, "Fail to get op type. unexpected! ret=%d", ret);
                break;
            }
            else if (OB_SUCCESS != (ret = expr_[idx+2].get_int(cid)))
            {
                return_ret=false;
                TBSYS_LOG(WARN, "Fail to get op type. unexpected! ret=%d", ret);
                break;
            }
            else
            {
                column_count++;
                idx = idx + get_type_num(idx,COLUMN_IDX);
            }
        }
        else if(type == OP)
        {
            if (ObIntType != expr_[idx+1].get_type())
            {
                return_ret=false;
                ret=OB_ERROR;
                break;
            }
            else if (OB_SUCCESS != (ret = expr_[idx+1].get_int(tmp_type)))
            {
                TBSYS_LOG(WARN, "fail to get int value.err=%d", ret);
                return_ret=false;
                break;
            }
            //mod liumz, [se_index support_range_scan]20160314:b
            //else if (tmp_type == T_OP_EQ )
            else if ((tmp_type >= T_OP_EQ && tmp_type <=T_OP_GT) || tmp_type == T_OP_BTW)
            //mod:e
            {
                EQ_count++;
                idx = idx + get_type_num(idx,OP);
            }
            else if(tmp_type == T_OP_IN)
            {
                IN_count++;
                idx = idx + get_type_num(idx,OP);
            }
            else
            {
                idx = idx + get_type_num(idx,OP);
            }
        }
        //add fanqiushi_index_prepare
        //mod dragon 2017-1-12 b
        //代码格式调整，并补充一些后来添加的类型
        else if(type == OP
                || type == CONST_OBJ
                || type == QUERY_ID
                || type == END
                || type == UPS_TIME_OP
                || type == PARAM_IDX
                || type == SYSTEM_VAR
                || type == TEMP_VAR
                || type == CUR_TIME_OP
                || type == CUR_TIME_HMS_OP
                || type == CUR_DATE_OP
                || type == END_TYPE)
        {
          idx = idx + get_type_num(idx,type);
        }
        /*---old---
        else if(type == OP ||type == COLUMN_IDX||type == T_OP_ROW||type == CONST_OBJ||type == QUERY_ID||type == END||type == UPS_TIME_OP)
        {
            idx = idx + get_type_num(idx,type);
        }
        ---old---*/
        //mod dragon e
        else
        {
            return_ret=false;
            TBSYS_LOG(WARN,"wrong expr type: %ld",type);
            break;
        }
        //add:e
    }
    //TBSYS_LOG(ERROR,"test::fanqs,column_count=%d,EQ_count=%d",column_count,EQ_count);
    //mod liumz, [optimize group_order by index]20170419:b
    //if((column_count==1&&EQ_count==1)||(column_count==1&&IN_count==1))
    if((column_count==1&&EQ_count==1)||(column_count==1&&IN_count==1)||(column_count==1&&idx==4))
    //mod:e
    {
        uint64_t tmp_index_tid[OB_MAX_INDEX_NUMS];
        for(int32_t m=0;m<OB_MAX_INDEX_NUMS;m++)
        {
            tmp_index_tid[m]=OB_INVALID_ID;
        }
        if(sm_v2->is_cid_in_index(cid,main_tid,tmp_index_tid))
        {
            index_tid=tmp_index_tid[0];
            return_ret=true;
            //TBSYS_LOG(ERROR,"test::fanqs,column_count=%d,EQ_count=%d",column_count,EQ_count);
        }
    }
    return return_ret;
}

bool ObPostfixExpression::is_all_expr_cid_in_indextable(uint64_t index_tid,const ObSchemaManagerV2 *sm_v2)  //判断该表达式的所有列是否都在索引表index_tid�?
{
    bool return_ret=true;
    int ret = OB_SUCCESS;
    //bool return_ret = false;
    int64_t type = 0;
    int64_t count = expr_.count();
    int64_t idx = 0;
    //int32_t column_count=0;
    int64_t tmp_cid = OB_INVALID_ID;
    int64_t tid = OB_INVALID_ID;
    while(idx<count)
    {
        if (OB_SUCCESS != (ret = expr_[idx].get_int(type)))
        {
            return_ret=false;
            TBSYS_LOG(WARN, "Fail to get op type. unexpected! ret=%d", ret);
            break;
        }
        else if(type == COLUMN_IDX)
        {
            if (OB_SUCCESS != (ret = expr_[idx+1].get_int(tid)))
            {
                return_ret=false;
                TBSYS_LOG(WARN, "Fail to get op type. unexpected! ret=%d", ret);
                break;
            }
            else if (OB_SUCCESS != (ret = expr_[idx+2].get_int(tmp_cid)))
            {
                return_ret=false;
                TBSYS_LOG(WARN, "Fail to get op type. unexpected! ret=%d", ret);
                break;
            }
            else
            {
                const ObColumnSchemaV2 *column_schema = NULL;
                if(NULL == (column_schema = sm_v2->get_column_schema(index_tid, tmp_cid)))
                {
                    return_ret=false;
                    break;
                }
                else
                    idx = idx + get_type_num(idx,COLUMN_IDX);
            }
        }
        //add fanqiushi_index_prepare
        //mod dragon 2017-1-12 b
        //代码格式调整，并补充一些后来添加的类型
        else if(type == OP
                || type == CONST_OBJ
                || type == QUERY_ID
                || type == END
                || type == UPS_TIME_OP
                || type == PARAM_IDX
                || type == SYSTEM_VAR
                || type == TEMP_VAR
                || type == CUR_TIME_OP
                || type == CUR_TIME_HMS_OP
                || type == CUR_DATE_OP
                || type == END_TYPE)
        {
          idx = idx + get_type_num(idx,type);
        }
        /*---old---
        else if(type == OP ||type == COLUMN_IDX||type == T_OP_ROW||type == CONST_OBJ||type == QUERY_ID||type == END||type == UPS_TIME_OP)
        {
            idx = idx + get_type_num(idx,type);
        }
        ---old---*/
        //mod dragon e
        else
        {
            return_ret=false;
            TBSYS_LOG(WARN,"wrong expr type: %ld",type);
            break;
        }
        //add:e
    }
    return return_ret;
}

int ObPostfixExpression::find_cid(uint64_t &cid)  //获得表达式中列的cid
{
    int ret = OB_SUCCESS;
    //bool return_ret = false;
    int64_t type = 0;
    int64_t count = expr_.count();
    int64_t idx = 0;
    int32_t column_count=0;
    int64_t tmp_cid = OB_INVALID_ID;
    int64_t tid = OB_INVALID_ID;
    while(idx<count)
    {
        if (OB_SUCCESS != (ret = expr_[idx].get_int(type)))
        {
            //return_ret=true;
            TBSYS_LOG(WARN, "Fail to get op type. unexpected! ret=%d", ret);
            break;
        }
        else if(type == COLUMN_IDX)
        {
            if (OB_SUCCESS != (ret = expr_[idx+1].get_int(tid)))
            {
                //return_ret=true;
                TBSYS_LOG(WARN, "Fail to get op type. unexpected! ret=%d", ret);
                break;
            }
            else if (OB_SUCCESS != (ret = expr_[idx+2].get_int(tmp_cid)))
            {
                //return_ret=true;
                TBSYS_LOG(WARN, "Fail to get op type. unexpected! ret=%d", ret);
                break;
            }
            else
            {
                column_count++;
                idx = idx + get_type_num(idx,COLUMN_IDX);
            }
        }
        //add fanqiushi_index_prepare
        //mod dragon 2017-1-12 b
        //代码格式调整，并补充一些后来添加的类型
        else if(type == OP
                || type == CONST_OBJ
                || type == QUERY_ID
                || type == END
                || type == UPS_TIME_OP
                || type == PARAM_IDX
                || type == SYSTEM_VAR
                || type == TEMP_VAR
                || type == CUR_TIME_OP
                || type == CUR_TIME_HMS_OP
                || type == CUR_DATE_OP
                || type == END_TYPE)
        {
          idx = idx + get_type_num(idx,type);
        }
        /*---old---
        else if(type == OP ||type == COLUMN_IDX||type == T_OP_ROW||type == CONST_OBJ||type == QUERY_ID||type == END||type == UPS_TIME_OP)
        {
            idx = idx + get_type_num(idx,type);
        }
        ---old---*/
        //mod dragon e
        else
        {
            ret=OB_ERROR;
            TBSYS_LOG(WARN,"wrong expr type: %ld",type);
            break;
        }
        //add:e
    }
    if(column_count==1&&ret==OB_SUCCESS)
    {
        cid=tmp_cid;
    }
    else{
        ret=OB_ERROR;
        TBSYS_LOG(WARN,"too many columns in one expr");
    }
    return ret;
}
//add wanglei [second index fix] 20160425:b
bool ObPostfixExpression::is_expr_has_more_than_two_columns()
{
//    int ret = OB_SUCCESS;
//    int64_t type = 0;
//    int64_t count = expr_.count();
//    int64_t idx = 0;
//    int32_t column_count=0;
//    int64_t cid = OB_INVALID_ID;
//    int64_t tid = OB_INVALID_ID;
//    while(idx<count)
//    {
//        if (OB_SUCCESS != (ret = expr_[idx].get_int(type)))
//        {
//            TBSYS_LOG(WARN, "Fail to get op type. unexpected! ret=%d", ret);
//            break;
//        }
//        else if(type == COLUMN_IDX)
//        {
//            if (OB_SUCCESS != (ret = expr_[idx+1].get_int(tid)))
//            {
//                TBSYS_LOG(WARN, "Fail to get op type. unexpected! ret=%d", ret);
//                break;
//            }
//            else if (OB_SUCCESS != (ret = expr_[idx+2].get_int(cid)))
//            {
//                TBSYS_LOG(WARN, "Fail to get op type. unexpected! ret=%d", ret);
//                break;
//            }
//            else
//            {
//                column_count++;
//                idx = idx + get_type_num(idx,COLUMN_IDX);
//            }
//        }
//        //add fanqiushi_index_prepare
//        else if(type == OP ||type == COLUMN_IDX||type == T_OP_ROW||type == CONST_OBJ||type == QUERY_ID||type == END||type == UPS_TIME_OP)
//        {
//            idx = idx + get_type_num(idx,type);
//        }
//    }
//    if(column_count>1)
//        return true;
//    else
//        return false;
    int ret = OB_SUCCESS;
    bool return_ret = false;
    int64_t type = 0;
    int64_t count = expr_.count();
    int64_t idx = 0;
    int32_t column_count=0;
    int64_t cid = OB_INVALID_ID;
    int64_t tid = OB_INVALID_ID;
    while(idx<count)
    {
        if (OB_SUCCESS != (ret = expr_[idx].get_int(type)))
        {
            return_ret=false;
            TBSYS_LOG(WARN, "Fail to get op type. unexpected! ret=%d", ret);
            break;
        }
        else if(type == COLUMN_IDX)
        {
            if (OB_SUCCESS != (ret = expr_[idx+1].get_int(tid)))
            {
                return_ret=false;
                TBSYS_LOG(WARN, "Fail to get op type. unexpected! ret=%d", ret);
                break;
            }
            else if (OB_SUCCESS != (ret = expr_[idx+2].get_int(cid)))
            {
                return_ret=false;
                TBSYS_LOG(WARN, "Fail to get op type. unexpected! ret=%d", ret);
                break;
            }
            else
            {
                column_count++;
                if(column_count>1)
                    return_ret = true;
                else
                    return_ret = false;
                idx = idx + get_type_num(idx,COLUMN_IDX);
            }
        }
        //add fanqiushi_index_prepare
        else if(type == OP
                ||type == COLUMN_IDX
                ||type == T_OP_ROW
                ||type == CONST_OBJ
                ||type == QUERY_ID
                ||type == END
                ||type == UPS_TIME_OP
                ||type == PARAM_IDX
                ||type == SYSTEM_VAR
                ||type == TEMP_VAR
                ||type == CUR_TIME_OP
                ||type ==CUR_TIME_HMS_OP
                ||type ==CUR_DATE_OP
                ||type ==END_TYPE
                )
        {
            int64_t tmp = get_type_num(idx,type);
            idx = idx + tmp;
            if(tmp == -1)
            {
                if(column_count>1)
                    return_ret = true;
                else
                    return_ret = false;
                break;
            }
        }
        else
        {
            return_ret=false;
            TBSYS_LOG(WARN,"wrong expr type: %ld",type);
            break;
        }
        //add:e
    }
    return return_ret;
}
//add wanglei [second index fix] 20160425:e
bool ObPostfixExpression::is_have_main_cid(uint64_t main_column_id)
{ //如果表达式中有主表的第一主键，或者表达式中有超过两列的，返回true
    int ret = OB_SUCCESS;
    bool return_ret = false;
    int64_t type = 0;
    int64_t count = expr_.count();
    int64_t idx = 0;
    int32_t column_count=0;
    int64_t cid = OB_INVALID_ID;
    int64_t tid = OB_INVALID_ID;
    while(idx<count)
    {
        if (OB_SUCCESS != (ret = expr_[idx].get_int(type)))
        {
            return_ret=true;
            TBSYS_LOG(WARN, "Fail to get op type. unexpected! ret=%d", ret);
            break;
        }
        else if(type == COLUMN_IDX)
        {
            if (OB_SUCCESS != (ret = expr_[idx+1].get_int(tid)))
            {
                return_ret=true;
                TBSYS_LOG(WARN, "Fail to get op type. unexpected! ret=%d", ret);
                break;
            }
            else if (OB_SUCCESS != (ret = expr_[idx+2].get_int(cid)))
            {
                return_ret=true;
                TBSYS_LOG(WARN, "Fail to get op type. unexpected! ret=%d", ret);
                break;
            }
            else
            {
                if(cid==(int64_t)main_column_id)
                {
                    return_ret=true;
                    break;
                }
                else
                {
                    column_count++;
                    idx = idx + get_type_num(idx,COLUMN_IDX);
                }
            }
        }
        //add fanqiushi_index_prepare
        //mod dragon 2017-1-12 b
        //代码格式调整，并补充一些后来添加的类型
        else if(type == OP
                || type == CONST_OBJ
                || type == QUERY_ID
                || type == END
                || type == UPS_TIME_OP
                || type == PARAM_IDX
                || type == SYSTEM_VAR
                || type == TEMP_VAR
                || type == CUR_TIME_OP
                || type == CUR_TIME_HMS_OP
                || type == CUR_DATE_OP
                || type == END_TYPE)
        {
          idx = idx + get_type_num(idx,type);
        }
        /*---old---
        else if(type == OP ||type == COLUMN_IDX||type == T_OP_ROW||type == CONST_OBJ||type == QUERY_ID||type == END||type == UPS_TIME_OP)
        {
            idx = idx + get_type_num(idx,type);
        }
        ---old---*/
        //mod dragon e
        else
        {
            return_ret=true;
            TBSYS_LOG(WARN,"wrong expr type: %ld",type);
            break;
        }
        //add:e
    }
    //add wanglei [second index fix] 20160425:b
    //must
//    if(column_count>1)
//        return_ret=true;
    //add wanglei [second index fix] 20160425:e

    return return_ret;
}

//add:e

bool ObPostfixExpression::is_simple_between(bool real_val, uint64_t &column_id, int64_t &cond_op,
                                            ObObj &cond_start, ObObj &cond_end) const
{
    int err = OB_ERROR;
    int64_t type_val1 = -1;
    int64_t type_val2 = -1;
    int64_t cid = OB_INVALID_ID;
    bool is_simple_cond_type = false;
    do{
        if (expr_.count() == (3+2+2+3+1)) /*cid(3) + const_opr(2) + const_opr(2) + operator(3) + end(1) */
        {
            /* (1) cid */
            if (ObIntType != expr_[0].get_type())
            {
                // not int val, pass
                break;
            }
            else if(OB_SUCCESS != (err = expr_[0].get_int(type_val1)))
            {
                TBSYS_LOG(WARN, "fail to get int value.err=%d", err);
                break;
            }
            else if (type_val1 != COLUMN_IDX)
            {
                break;
            }
            else if (OB_SUCCESS != (err = expr_[2].get_int(cid)))
            {
                TBSYS_LOG(WARN, "fail to get int value.err=%d", err);
                break;
            }
            /* (2) const_opr 1 and const_opr 2 */
            else if (ObIntType != expr_[3].get_type())
            {
                // not int val, pass
                break;
            }
            else if (OB_SUCCESS != (err = expr_[3].get_int(type_val1)))
            {
                TBSYS_LOG(WARN, "fail to get int value.err=%d", err);
                break;
            }
            else if (type_val1 != CONST_OBJ && type_val1 != PARAM_IDX
                     && type_val1 != SYSTEM_VAR && type_val1 != TEMP_VAR && type_val1 != CUR_TIME_OP
                     //add liuzy [datetime func] 20150910:b
                     && type_val1 != CUR_DATE_OP && type_val1 != CUR_TIME_HMS_OP
                     //add 20150910:e
                     )
            {
                break;
            }
            else if (ObIntType != expr_[5].get_type())
            {
                // not int val, pass
                break;
            }
            else if (OB_SUCCESS != (err = expr_[5].get_int(type_val2)))
            {
                TBSYS_LOG(WARN, "fail to get int value.err=%d", err);
                break;
            }
            else if (type_val2 != CONST_OBJ && type_val2 != PARAM_IDX
                     && type_val2 != SYSTEM_VAR && type_val2 != TEMP_VAR && type_val2 != CUR_TIME_OP
                     //add liuzy [datetime func] 20150910:b
                     && type_val2 != CUR_DATE_OP && type_val2 != CUR_TIME_HMS_OP
                     //add 20150910:e
                     )
            {
                break;
            }
            else
            {
                if (real_val)
                {
                    const ObObj *val = NULL;
                    if (OB_SUCCESS == (err = get_var_obj(static_cast<ObPostExprNodeType>(type_val1), expr_[4], val)))
                    {
                        cond_start = *val;
                    }
                    else
                    {
                        break;
                    }
                    if (OB_SUCCESS == (err = get_var_obj(static_cast<ObPostExprNodeType>(type_val2), expr_[6], val)))
                    {
                        cond_end = *val;
                    }
                    else
                    {
                        break;
                    }
                }
                else
                {
                    cond_start = expr_[4];
                    cond_end = expr_[6];
                }
            }
            /* (3) op */
            if (OB_SUCCESS != err)
            {
                // pass
                break;
            }
            else if (ObIntType != expr_[7].get_type())
            {
                // not int val, pass
                break;
            }
            else if (OB_SUCCESS != (err = expr_[7].get_int(type_val2)))
            {
                TBSYS_LOG(WARN, "fail to get int value.err=%d", err);
                break;
            }
            else if (type_val2 != OP)
            {
                break;
            }
            else if (ObIntType != expr_[8].get_type())
            {
                // not int val, pass
                break;
            }
            else if (OB_SUCCESS != (err = expr_[8].get_int(type_val2)))
            {
                TBSYS_LOG(WARN, "fail to get int value.err=%d", err);
                break;
            }
            else if (type_val2 != T_OP_BTW)
            {
                break;
            }
            else
            {
                /* result */
                column_id = (uint64_t)cid;
                cond_op = type_val2;
                is_simple_cond_type = true;
                err = OB_SUCCESS;
            }
        }
        else
        {
            TBSYS_LOG(DEBUG, "not a valid simple between function");
            err = OB_INVALID_ARGUMENT;
        }
    } while(0);
    return is_simple_cond_type;
}


DEFINE_SERIALIZE(ObPostfixExpression)
{
    int ret = OB_SUCCESS;
    int i = 0;
    ObObj obj;
    obj.set_int(expr_.count());
    //add dolphin[coalesce return type]@20151126:b
    ObObj ty;
    ty.set_int(expr_type_);
    //add:e
    if (OB_SUCCESS != (ret = obj.serialize(buf, buf_len, pos)))
    {
        TBSYS_LOG(WARN, "fail to serialize postfix expression size. ret=%d", ret);
    }
    else
    {
        for (i = 0; i < expr_.count(); i++)
        {
            int64_t type = BEGIN_TYPE;
            int64_t num = 0;
            if (OB_SUCCESS != (ret = expr_[i].get_int(type)))
            {
                TBSYS_LOG(WARN, "Fail to get type. unexpected! ret=%d idx=%d", ret, i);
                ret = OB_ERR_UNEXPECTED;
                break;
            }
            else if (type == PARAM_IDX || type == SYSTEM_VAR || type == TEMP_VAR || type == CUR_TIME_OP
                     //add liuzy [datetime func] 20150914:b
                     || type == CUR_TIME_HMS_OP || type == CUR_DATE_OP
                     //add 20150914:e
                     )
            {
                ObObj new_type;
                new_type.set_int(CONST_OBJ);
                const ObObj *val = NULL;
                if (OB_SUCCESS != (ret = new_type.serialize(buf, buf_len, pos)))
                {
                    TBSYS_LOG(WARN, "Fail to serialize type CONST_OBJ");
                    break;
                }
                else if (i >= expr_.count() - 1
                         || OB_SUCCESS != (ret = get_var_obj(static_cast<ObPostExprNodeType>(type), expr_[++i], val)))
                {
                    ret = OB_ERR_UNEXPECTED;
                    TBSYS_LOG(WARN,"Get value ObObj failed [err:%d]", ret);
                    break;
                }
                else if (OB_SUCCESS != (ret = val->serialize(buf, buf_len, pos)))
                {
                    TBSYS_LOG(WARN, "fail to serialize expr[%d]. ret=%d", i, ret);
                    break;
                }
                continue;
            }
            else if (type == OP)
            {
                num = 3;
                int64_t op_type = 0;
                if (OB_SUCCESS != (ret = expr_[i+1].get_int(op_type)))
                {
                    TBSYS_LOG(WARN, "Fail to get op type. unexpected! ret=%d idx=%d", ret, i+1);
                    ret = OB_ERR_UNEXPECTED;
                    break;
                }
                else if (T_FUN_SYS == op_type)
                {
                    ++num;
                }
            }
            else if (type == COLUMN_IDX)
            {
                num = 3;
            }
            //mod tianz [SubQuery_for_Instmt] [JHOBv0.1] 20140408:b
            //else if (type == CONST_OBJ)
            else if (type == CONST_OBJ||type == QUERY_ID)
                //mod 20140408:e
            {
                num = 2;
            }
            else if (type == END || type == UPS_TIME_OP)
            {
                num = 1;
            }
            else
            {
                TBSYS_LOG(WARN, "Unkown type %ld", type);
                ret = OB_ERR_UNEXPECTED;
                break;
            }
            for (int64_t j = 0; j < num; j++)
            {
                if (i >= expr_.count()
                        || OB_SUCCESS != (ret = expr_[i].serialize(buf, buf_len, pos)))
                {
                    ret = OB_ERR_UNEXPECTED;
                    TBSYS_LOG(WARN, "Fail to serialize expr[%d]. ret=%d count=%ld", i, ret, expr_.count());
                    break;
                }
                else if (j < num - 1)
                {
                    i++;
                }
            }
        }
    }
    //add dolphin[coalesce return type]@20151126:b
    if (OB_SUCCESS != (ret = ty.serialize(buf, buf_len, pos)))
    {
        TBSYS_LOG(WARN, "fail to serialize postfix expression type. ret=%d", ret);
    }
    //add:e
    return ret;
}

// single row expression checker
// ONLY: (x,x,x) in ((x,x,x))
//mod fyd IN_EXPR [PrefixKeyQuery_for_INstmt]  2014.4.14
// fix bug:if the in expr rowkey column requence does not match schema ,this funciton will ignore the rowkey,and and  do not considers it as rowkey  --fyd
// 该函数目前实现的功能是：如果包含全部主键，则返回true
bool ObPostfixExpression::is_simple_in_expr(bool real_val, const ObRowkeyInfo &info, ObIArray<ObRowkey> &rowkey_array,
                                            common::PageArena<ObObj,common::ModulePageAllocator> &rowkey_objs_allocator) const
{
    //del:b
    //      int err = OB_SUCCESS;
    //      int64_t rowkey_column_count = 0;
    //      int64_t index = 0;
    //      int64_t len = expr_.count();
    //      int64_t dim = 0, dim2 = 0;
    //      int64_t row_count = 0;
    //      int64_t row = 0;
    //      int64_t val_idx = 0;
    //      bool is_simple_expr = false;
    //      ObRowkey rowkey;
    //      ObObj *rowkey_objs = NULL;
    //      ObRowkeyColumn rowkey_column;
    //      int64_t cid = OB_INVALID_ID;
    //      if (len > 16)
    //      {
    //        // check 1: (OP, T_OP_IN, 2), (END)
    //        if (!ExprUtil::is_end(expr_.at(len-1)) || !ExprUtil::is_value(expr_.at(len-2), 2L) ||
    //            !ExprUtil::is_op_of_type(expr_.at(len-3), T_OP_IN) || !ExprUtil::is_op(expr_.at(len-4)))
    //        {
    //          // TBSYS_LOG(DEBUG, "not simple in expr. len=%ld. %d, %d, %d, %d", len,
    //          //    ExprUtil::is_end(expr_.at(len-1)), ExprUtil::is_value(expr_.at(len-2), 2L),
    //          //    ExprUtil::is_op_of_type(expr_.at(len-3), T_OP_IN), ExprUtil::is_op(expr_.at(len-4)));
    //        }
    //        // check 2: (OP, T_OP_ROW, row_count)
    //        // support getting multi row,
    //        // for example, (a,b) in ((1,2), (3,4), (5,6)), row_count = 3, dim = 2
    //        else if (OB_SUCCESS != expr_.at(len-5).get_int(row_count) || !ExprUtil::is_op_of_type(expr_.at(len-6), T_OP_ROW) ||
    //            !ExprUtil::is_op(expr_.at(len-7)))
    //        {
    //          // TBSYS_LOG(DEBUG, "not simple in expr. len=%ld", len);
    //        }
    //        // check 3: (OP, T_OP_ROW, dim)
    //        else if (OB_SUCCESS != expr_.at(len-8).get_int(dim) || !ExprUtil::is_op_of_type(expr_.at(len-9), T_OP_ROW) ||
    //            !ExprUtil::is_op(expr_.at(len-10)))
    //        {
    //          // TBSYS_LOG(DEBUG, "not simple in expr. len=%ld", len);
    //        }
    //        else
    //        {
    //          const int64_t single_row_len = dim * 2 + 3;
    //          len = (len - 7) - (row_count * single_row_len);  // support multi row, 'dim' columns, each column takes two objects
    //          val_idx = len;
    //          OB_ASSERT(dim > 0);
    //          OB_ASSERT(len > 6);
    //          if (dim <= 0 || len <= 6)
    //          {
    //            // TBSYS_LOG(DEBUG, "not simple in expr. len=%ld", len);
    //          }
    //          // check 4: (OP, T_OP_LEFT_PARAM_END, 2)
    //          else if (!ExprUtil::is_value(expr_.at(len-1), 2L) || !ExprUtil::is_op_of_type(expr_.at(len-2), T_OP_LEFT_PARAM_END) ||
    //              !ExprUtil::is_op(expr_.at(len-3)))
    //          {
    //            // TBSYS_LOG(DEBUG, "not simple in expr. len=%ld", len);
    //          }
    //          // check 5: (OP, T_OP_ROW, ?)
    //          else if (OB_SUCCESS != expr_.at(len-4).get_int(dim2) || !ExprUtil::is_op_of_type(expr_.at(len-5), T_OP_ROW) ||
    //              !ExprUtil::is_op(expr_.at(len-6)))
    //          {
    //            // TBSYS_LOG(DEBUG, "not simple in expr. len=%ld.%ld, %d, %d", len,
    //            //    dim2, ExprUtil::is_op_of_type(expr_.at(len-5), T_OP_ROW), ExprUtil::is_op(expr_.at(len-6)));
    //          }
    //          else if (dim != dim2)
    //          {
    //            // TBSYS_LOG(DEBUG, "not simple in expr. len=%ld. dim=%ld", len, dim);
    //          }
    //          else if (dim >= info.get_size())
    //          {
    //            len = len - (dim * 3 + 6); // 3 = COLUMN_IDX, TID, CID
    //            if (len == 0)
    //            {
    //              // extra values from expression
    //              // extra 1: rowkey columns
    //              int64_t size = info.get_size();
    //              for (index = 0; index < size && index < dim && OB_SUCCESS == err; index++)
    //              {
    //                if (OB_SUCCESS != (err = info.get_column(index, rowkey_column)))
    //                {
    //                  TBSYS_LOG(ERROR, "get rowkey column fail. index=%ld, size=%ld", index, size);
    //                }
    //                else
    //                {
    //                  if (OB_SUCCESS != expr_.at(index * 3 + 2).get_int(cid)) // 3=COLUMN_REF, TID, CID
    //                  {
    //                    TBSYS_LOG(ERROR, "fail to get int value from expr_.at(2)");
    //                  }
    //                  if (rowkey_column.column_id_ == static_cast<uint64_t>(cid))
    //                  {
    //                    rowkey_column_count++;
    //                  }
    //                  else
    //                  {
    //                    break;
    //                  }
    //                }
    //              }
    //              if ((OB_SUCCESS == err) && (rowkey_column_count == size))
    //              {
    //                if (NULL != (rowkey_objs = rowkey_objs_allocator.alloc(row_count * rowkey_column_count * sizeof(ObObj))))
    //                {
    //                  for (row = 0; row < row_count; row++)
    //                  {
    //                    // extra 2: values
    //                    for (index = 0; index < rowkey_column_count; index++)
    //                    {
    //                      // TODO: check every T_OP_ROW dim, all must be equal. currently skipped this step
    //                      const int64_t type_offset = val_idx + row * single_row_len + (index * 2);
    //                      const int64_t val_offset = val_idx + row * single_row_len + (index * 2 + 1); // 2=CONST,VALUE
    //                      int64_t type = 0;
    //                      const ObObj *val = NULL;
    //                      if (OB_SUCCESS != (err = expr_.at(type_offset).get_int(type)))
    //                      {
    //                        TBSYS_LOG(ERROR, "Can not get value type. err=%d", err);
    //                      }
    //                      else if (real_val)
    //                      {
    //                        if (OB_SUCCESS != (err = get_var_obj(static_cast<ObPostExprNodeType>(type), expr_.at(val_offset), val)))
    //                        {
    //                          TBSYS_LOG(ERROR, "Can not get value. err=%d", err);
    //                        }
    //                        else
    //                        {
    //                          rowkey_objs[index] = *val;
    //                        }
    //                      }
    //                      else
    //                      {
    //                        rowkey_objs[index] = expr_.at(val_offset);
    //                      }
    //                      // TBSYS_LOG(DEBUG, "index=%ld, at=%ld, val=%s", index, offset, to_cstring(rowkey_objs[index]));
    //                    }
    //                    rowkey.assign(rowkey_objs, rowkey_column_count);
    //                    if (OB_SUCCESS!= (err = rowkey_array.push_back(rowkey)))
    //                    {
    //                      TBSYS_LOG(ERROR, "fail to push rowkey to array. err=%d", err);
    //                    }
    //                    rowkey_objs += rowkey_column_count;
    //                  }
    //                }
    //                else
    //                {
    //                  TBSYS_LOG(ERROR, "fail to alloc memory");
    //                  err = OB_ALLOCATE_MEMORY_FAILED;
    //                }
    //                if (OB_SUCCESS == err)
    //                {
    //                  is_simple_expr = true;
    //                }
    //              }
    //            }
    //            else
    //            {
    //              // TBSYS_LOG(DEBUG, "not simple in expr. len=%ld. dim=%ld", len, dim);
    //            }
    //          }
    //        }
    //      }
    //      return is_simple_expr;
    //del:e
    // only if it is ex rowkey ,we need to check  wethter  the ex rowkey  is rowkey  or not.
    bool is_simple_expr = true;
    if ( this ->is_in_expr_with_ex_rowkey(real_val, info, rowkey_array, rowkey_objs_allocator )  )
    {
        for ( int i = 0 ; i < rowkey_array.count() ; i++ )
        {
            if ( ( (ObRowkey) rowkey_array.at(i) ).length() != info.get_size() )
            {
                is_simple_expr = false;
                rowkey_array.clear();
                break;
            }
        }
    }
    else
    {
        is_simple_expr = false ;
        rowkey_array.clear();
    }

    return is_simple_expr ;
}
//mod:e

//add lijianqiang [sequence insert] 20150414:b
ObSEArray<ObObj, 64> & ObPostfixExpression::get_expr_array()
{
    return expr_;
}
int64_t ObPostfixExpression::get_expr_obj_size()
{
    return expr_.count();
}
//add 20150414:e
//add fyd IN_EXPR [PrefixKeyQuery_for_INstmt]   2014.4.14:b
// 注意此时 rowkey 返回的�?仅代表的是主键前缀中的数据，而且是按照主键顺序的  fyd
//  此函数，如果包含主键前缀或是全主键，都会返回true
bool ObPostfixExpression::is_in_expr_with_ex_rowkey(bool real_val, const ObRowkeyInfo &info, ObIArray<ObRowkey> &rowkey_array, common::PageArena<ObObj,common::ModulePageAllocator> &rowkey_objs_allocator) const
{
    int err = OB_SUCCESS;
    int64_t rowkey_column_count = 0;
    int64_t index = 0;
    int64_t len = expr_.count();
    int64_t dim = 0, dim2 = 0;
    int64_t row_count = 0;
    int64_t row = 0;
    int64_t val_idx = 0;
    bool is_simple_expr = false;
    ObRowkey rowkey;
    ObObj *rowkey_objs = NULL;
    //int64_t *rowkey_types = NULL ; //added
    ObRowkeyColumn rowkey_column;
    int64_t cid = OB_INVALID_ID;
    int64_t rowkey_index_map[OB_MAX_ROWKEY_COLUMN_NUMBER]={0};//用于保存按顺序的主键列在行输入表达式中的位置
    if (len > 16)
    {
        // check 1: (OP, T_OP_IN, 2), (END)
        if (!ExprUtil::is_end(expr_.at(len-1)) || !ExprUtil::is_value(expr_.at(len-2), 2L) ||
                !ExprUtil::is_op_of_type(expr_.at(len-3), T_OP_IN) || !ExprUtil::is_op(expr_.at(len-4)))
        {
            //    ExprUtil::is_end(expr_.at(len-1)), ExprUtil::is_value(expr_.at(len-2), 2L),
            //    ExprUtil::is_op_of_type(expr_.at(len-3), T_OP_IN), ExprUtil::is_op(expr_.at(len-4)));
        }
        // check 2: (OP, T_OP_ROW, row_count)
        // support getting multi row,
        // for example, (a,b) in ((1,2), (3,4), (5,6)), row_count = 3, dim = 2
        else if (OB_SUCCESS != expr_.at(len-5).get_int(row_count) || !ExprUtil::is_op_of_type(expr_.at(len-6), T_OP_ROW) ||
                 !ExprUtil::is_op(expr_.at(len-7)))//exp1-3  fyd
        {
            // TBSYS_LOG(DEBUG, "not simple in expr. len=%ld", len);
        }
        // check 3: (OP, T_OP_ROW, dim)
        else if (OB_SUCCESS != expr_.at(len-8).get_int(dim) || !ExprUtil::is_op_of_type(expr_.at(len-9), T_OP_ROW) ||
                 !ExprUtil::is_op(expr_.at(len-10)))//  exp7?
        {
            // TBSYS_LOG(DEBUG, "not simple in expr. len=%ld", len);
        }
        else
        {
            const int64_t single_row_len = dim * 2 + 3;
            // dim 表示右边具体一个（1,2,3,...）表达式中一共有几个具体数据�?表示这个数据占两个expr�?表示跟着这个数据有一个占3个expr的OP子句  fyd  2014.3.20
            len = (len - 7) - (row_count * single_row_len);  // support multi row, 'dim' columns, each column takes two objects
            // len -7 因为7个expr 完全�?(OP, T_OP_ROW, row_count) (OP, T_OP_IN, 2), (END)  这里将这些信息去�? fyd 2014.3.20
            //  row_count * single_row_len 代表�?右边子句�?有关行具体信息加上描述信息的长度  fyd  2014.3.20
            val_idx = len;//val_idx 表示 倒序中，跳过in子句右边数据数据，expr1-1开始的地方  fyd
            OB_ASSERT(dim > 0);
            OB_ASSERT(len > 6);
            if (dim <= 0 || len <= 6)
            {
                // TBSYS_LOG(DEBUG, "not simple in expr. len=%ld", len);
            }
            // check 4: (OP, T_OP_LEFT_PARAM_END, 2)
            else if (!ExprUtil::is_value(expr_.at(len-1), 2L) || !ExprUtil::is_op_of_type(expr_.at(len-2), T_OP_LEFT_PARAM_END) ||
                     !ExprUtil::is_op(expr_.at(len-3)))
            {
                // TBSYS_LOG(DEBUG, "not simple in expr. len=%ld", len);
            }
            // check 5: (OP, T_OP_ROW, ?)
            else if (OB_SUCCESS != expr_.at(len-4).get_int(dim2) || !ExprUtil::is_op_of_type(expr_.at(len-5), T_OP_ROW) ||
                     !ExprUtil::is_op(expr_.at(len-6)))//dim2 表示 in 左侧列的�? fyd  2014.3.20
            {
                // TBSYS_LOG(DEBUG, "not simple in expr. len=%ld.%ld, %d, %d", len,
                //    dim2, ExprUtil::is_op_of_type(expr_.at(len-5), T_OP_ROW), ExprUtil::is_op(expr_.at(len-6)));
            }
            else if (dim != dim2)
            {
                // TBSYS_LOG(DEBUG, "not simple in expr. len=%ld. dim=%ld", len, dim);
            }
            else if (/*dim >= info.get_size()*/dim>0)// 表示in子句列个数大�?  肯定是是 true
            {
                len = len - (dim * 3 + 6); // 3 = COLUMN_IDX, TID, CID
                // 6表示上面�?OP, T_OP_LEFT_PARAM_END, 2)(OP, T_OP_ROW, ?) 这六个表达式的�?fyd  2014.3.20
                if (len == 0)// len 等于 0 表示 这个表达式序列是正确�? fyd  2014.3.20
                {
                    // extra values from expression
                    // extra 1: rowkey columns
                    //  从输入列中遍历出所有的主键 ，从第一主键开�?这里计算出的rowkey_column_count  是属于主键前缀的列数目  fyd
                    int64_t size = info.get_size();
                    for (index = 0; index < size  && OB_SUCCESS == err; index++)
                    {
                        if (OB_SUCCESS != (err = info.get_column(index, rowkey_column)))
                        {
                            TBSYS_LOG(ERROR, "get rowkey column fail. index=%ld, size=%ld", index, size);
                        }
                        else
                        {
                            int64_t  tmp_index = 0;
                            for( tmp_index=0; tmp_index < dim; tmp_index++ )
                            {

                                if (OB_SUCCESS != expr_.at(tmp_index * 3 + 2).get_int(cid)) // 3=COLUMN_REF, TID, CID
                                {
                                    TBSYS_LOG(ERROR, "fail to get int value from expr_.at(2)");
                                }
                                if (rowkey_column.column_id_ == static_cast<uint64_t>(cid))
                                {
                                    rowkey_index_map[index] = tmp_index;// 记录了每个主键列的存储顺�?
                                    rowkey_column_count++;
                                    break;
                                }
                            }
                            if ( tmp_index == dim )
                            {
                                break;
                            }
                        }
                    }//end for (index = 0; index < size && index < dim && OB_SUCCESS == err; index++)  fyd
                    if ((OB_SUCCESS == err) && (rowkey_column_count >0 ))
                    {
                        if (NULL != (rowkey_objs = rowkey_objs_allocator.alloc(row_count * rowkey_column_count * sizeof(ObObj)))
                                /*&&NULL != (rowkey_types = rowkey_objs_allocator.alloc(row_count * rowkey_column_count * sizeof(int64_t)))*/)
                        {
                            for (row = 0; row < row_count; row++)
                            {
                                // extra 2: values
                                for (index = 0; index < rowkey_column_count; index++)
                                {
                                    // TODO: check every T_OP_ROW dim, all must be equal. currently skipped this step
                                    const int64_t type_offset = val_idx + row * single_row_len + (rowkey_index_map[index] * 2);
                                    const int64_t val_offset = val_idx + row * single_row_len + (rowkey_index_map[index] * 2 + 1); // 2=CONST,VALUE
                                    int64_t type = 0;
                                    const ObObj *val = NULL;
                                    ObObj type_obj;
                                    if (OB_SUCCESS != (err = expr_.at(type_offset).get_int(type)))
                                    {
                                        TBSYS_LOG(ERROR, "Can not get value type. err=%d", err);
                                    }
                                    else if (real_val)
                                    {
                                        if (OB_SUCCESS != (err = get_var_obj(static_cast<ObPostExprNodeType>(type), expr_.at(val_offset), val)))
                                        {
                                            TBSYS_LOG(ERROR, "Can not get value. err=%d", err);
                                        }
                                        else
                                        {
                                            rowkey_objs[index] = *val;
                                            //rowkey_types[index] = type;
                                        }
                                    }
                                    else
                                    {
                                        rowkey_objs[index] = expr_.at(val_offset);
                                        // rowkey_types[index] =  type;
                                    }
                                    // TBSYS_LOG(DEBUG, "index=%ld, at=%ld, val=%s", index, offset, to_cstring(rowkey_objs[index]));
                                }// for (index = 0; index < rowkey_column_count; index++)
                                rowkey.assign(rowkey_objs, rowkey_column_count);
                                if (OB_SUCCESS!= (err = rowkey_array.push_back(rowkey)))
                                {
                                    TBSYS_LOG(ERROR, "fail to push rowkey to array. err=%d", err);
                                }
                                /* if (rowkey_obj_type_array == NULL || OB_SUCCESS!= (err = rowkey_obj_type_array.push_back(rowkey_types)))
          {
            if(OB_SUCCESS != err)
              TBSYS_LOG(WARN, "fail to push rowkey_types  to array. err=%d", err);
          }*/
                                rowkey_objs += rowkey_column_count;
                            }//
                        }
                        else
                        {
                            TBSYS_LOG(ERROR, "fail to alloc memory");
                            err = OB_ALLOCATE_MEMORY_FAILED;
                        }
                        if (OB_SUCCESS == err)
                        {
                            is_simple_expr = true;
                        }
                    }//end  if ((OB_SUCCESS == err) && (rowkey_column_count == size))  fyd
                }//end  (len == 0)  fyd
                else
                {
                    TBSYS_LOG(DEBUG, "not simple in expr. len=%ld. dim=%ld", len, dim);
                }//end
            }
        }
    }
    TBSYS_LOG(DEBUG, "current simple in expr is simple with ex rowkey. ret=%d. isSmple=%d", err, is_simple_expr);
    return is_simple_expr;
}
//add:e
DEFINE_DESERIALIZE(ObPostfixExpression)
{
    int ret = OB_SUCCESS;
    int i = 0;
    ObObj obj;
    ObObj obj2;
    int64_t val = 0;
    reset();
    if (OB_SUCCESS != (ret = obj.deserialize(buf, data_len, pos)))
    {
        TBSYS_LOG(WARN, "fail to deserialize obj. ret=%d. buf=%p, data_len=%ld, pos=%ld",
                  ret, buf, data_len, pos);
    }
    else if (ObIntType != obj.get_type())
    {
        TBSYS_LOG(WARN, "unexpected obj type. actual type:%d, expected:%d", obj.get_type(), ObIntType);
        ret = OB_ERR_UNEXPECTED;
    }
    else
    {
        if ((OB_SUCCESS != (ret = obj.get_int(val))) || (val <= 0))
        {
            TBSYS_LOG(WARN, "fail to get int value. ret=%d, expr_.count()%ld", ret, val);
        }
        else
        {
            int expr_count = static_cast<int32_t>(val);
            for (i = 0; i < expr_count; i++)
            {
                if (OB_SUCCESS != (ret = obj.deserialize(buf, data_len, pos)))
                {
                    TBSYS_LOG(WARN, "fail to deserialize obj. ret=%d. buf=%p, data_len=%ld, pos=%ld",
                              ret, buf, data_len, pos);
                    break;
                }
                else if (ObVarcharType == obj.get_type())
                {
                    if (OB_SUCCESS != (ret = str_buf_.write_obj(obj, &obj2)))
                    {
                        TBSYS_LOG(WARN, "fail to write object to string buffer. ret=%d", ret);
                    }
                    else if (OB_SUCCESS != (ret = expr_.push_back(obj2)))
                    {
                        TBSYS_LOG(WARN, "failed to add item, err=%d", ret);
                    }
                }
                //add wenghaixing DECIMAL OceanBase_BankCommV0.3 2014_7_10:b
                else if(ObDecimalType==obj.get_type()){
                    if (OB_SUCCESS != (ret = str_buf_.write_obj_varchar_to_decimal(obj, &obj2)))
                    {
                        TBSYS_LOG(WARN, "fail to write object to string buffer. ret=%d", ret);
                    }
                    else
                    {
                        // TBSYS_LOG(WARN, "test::fan2=,,,,,p=%d,,,s=%d",obj.get_precision() ,obj.get_scale());
                        obj2.set_precision(obj.get_precision());
                        obj2.set_scale(obj.get_scale());
                        if (OB_SUCCESS != (ret = expr_.push_back(obj2)))
                        {
                            TBSYS_LOG(WARN, "failed to add item, err=%d", ret);
                        }
                    }
                    //TBSYS_LOG(ERROR,"DEFINE_DESERIALIZE");
                }
                //add e
                else
                {
                    if (OB_SUCCESS != (ret = expr_.push_back(obj)))
                    {
                        TBSYS_LOG(WARN, "failed to add item, err=%d", ret);
                    }
                }
            }
            //add dolphin[coalesce return type]@20151128:b
            if (OB_SUCCESS != (ret = obj.deserialize(buf, data_len, pos)))
            {
                TBSYS_LOG(WARN, "fail to deserialize obj expr_type_. ret=%d. buf=%p, data_len=%ld, pos=%ld",
                          ret, buf, data_len, pos);
            }
            else
            {
                int64_t tt = 0;
                obj.get_int(tt);
                expr_type_ = (ObObjType) tt;
            }
            //add:e
        }
    }
    return ret;
}

DEFINE_GET_SERIALIZE_SIZE(ObPostfixExpression)
{
    int64_t size = 0;
    ObObj obj;
    obj.set_int(expr_.count());
    size += obj.get_serialize_size();
    for (int i = 0; i < expr_.count(); i++)
    {
        int64_t type = BEGIN_TYPE;
        int64_t num = 0;
        if (OB_SUCCESS != expr_[i].get_int(type))
        {
            TBSYS_LOG(WARN, "Fail to get type. unexpected! idx=%d", i);
            break;
        }
        else if (type == PARAM_IDX || type == SYSTEM_VAR || type == TEMP_VAR || type == CUR_TIME_OP
                 //add liuzy [datetime func] 20150914:b
                 || type == CUR_TIME_HMS_OP || type == CUR_DATE_OP
                 //add 20150914:e
                 )
        {
            ObObj new_type;
            new_type.set_int(CONST_OBJ);
            const ObObj *val = NULL;
            size += new_type.get_serialize_size();
            if (i >= expr_.count() - 1
                    || OB_SUCCESS != get_var_obj(static_cast<ObPostExprNodeType>(type), expr_[++i], val))
            {
                TBSYS_LOG(WARN,"Get value ObObj failed ");
                break;
            }
            size += val->get_serialize_size();
            continue;
        }
        else if (type == OP)
        {
            num = 3;
            int64_t op_type = 0;
            if (OB_SUCCESS != expr_[i + 1].get_int(op_type))
            {
                TBSYS_LOG(WARN, "Fail to get op type. unexpected! idx=%d", i+1);
                break;
            }
            else if (T_FUN_SYS == op_type)
            {
                ++num;
            }
        }
        else if (type == COLUMN_IDX)
        {
            num = 3;
        }
        else if (type == CONST_OBJ)
        {
            num = 2;
        }
        else if (type == END || type == UPS_TIME_OP)
        {
            num = 1;
        }
        else
        {
            TBSYS_LOG(WARN, "Unkown type %ld", type);
            break;
        }
        for (int64_t j = 0; j < num; j++)
        {
            if (i >= expr_.count())
            {
                TBSYS_LOG(WARN, "Fail to serialize expr[%d]. count=%ld", i, expr_.count());
                break;
            }
            else
            {
                size += expr_[i].get_serialize_size();
            }
            if (j < num - 1)
            {
                i++;
            }
        }
    }
    //add dolphin[coalesce return type]@20151128:b
    ObObj type;
    type.set_int(expr_type_);
    size += type.get_serialize_size();
    //add:e
    return size;
}

/************************************************************************/
/*****************   function implementation     ************************/
/************************************************************************/
inline int ObPostfixExpression::nop_func(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params)
{
    UNUSED(params);
    UNUSED(stack_i);
    UNUSED(idx_i);
    UNUSED(result);
    TBSYS_LOG(WARN, "function not implemented!");
    return OB_NOT_IMPLEMENT;
}

inline int ObPostfixExpression::reserved_func(const ObExprObj &obj1, const ObExprObj &obj2, ObExprObj &result)
{
    int err = OB_INVALID_ARGUMENT;
    UNUSED(obj1);
    UNUSED(obj2);
    UNUSED(result);
    return err;
}


/* compare function list:
     * >   gt_func
     * >=  ge_func
     * <=  le_func
     * <   lt_func
     * ==  eq_func
     * !=  neq_func
     */
inline int ObPostfixExpression::gt_func(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params)
{
    int err = OB_SUCCESS;
    UNUSED(params);
    if (NULL == stack_i)
    {
        TBSYS_LOG(WARN, "stack pointer is NULL.");
        err = OB_INVALID_ARGUMENT;
    }
    else if (idx_i < 2)
    {
        TBSYS_LOG(WARN, "no enough operand in the stack. current size:%d", idx_i);
        err = OB_INVALID_ARGUMENT;
    }
    else
    {
        //mod peiouya [DATE_TIME] 20150929:b
        //stack_i[idx_i-2].gt(stack_i[idx_i-1], result);
        err = stack_i[idx_i-2].gt(stack_i[idx_i-1], result);
        //mod 20150929:e
        idx_i -= 2;
    }
    return err;
}

inline int ObPostfixExpression::ge_func(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params)
{
    int err = OB_SUCCESS;
    UNUSED(params);
    if (NULL == stack_i)
    {
        TBSYS_LOG(WARN, "stack pointer is NULL.");
        err = OB_INVALID_ARGUMENT;
    }
    else if (idx_i < 2)
    {
        TBSYS_LOG(WARN, "no enough operand in the stack. current size:%d", idx_i);
        err = OB_INVALID_ARGUMENT;
    }
    else
    {
        //mod peiouya [DATE_TIME] 20150929:b
        //stack_i[idx_i-2].ge(stack_i[idx_i-1], result);
        err = stack_i[idx_i-2].ge(stack_i[idx_i-1], result);
        //mod 20150929:e
        idx_i -= 2;
    }
    return err;
}

inline int ObPostfixExpression::le_func(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params)
{
    int err = OB_SUCCESS;
    UNUSED(params);
    if (NULL == stack_i)
    {
        TBSYS_LOG(WARN, "stack pointer is NULL.");
        err = OB_INVALID_ARGUMENT;
    }
    else if (idx_i < 2)
    {
        TBSYS_LOG(WARN, "no enough operand in the stack. current size:%d", idx_i);
        err = OB_INVALID_ARGUMENT;
    }
    else
    {
        //mod peiouya [DATE_TIME] 20150929:b
        //stack_i[idx_i-2].le(stack_i[idx_i-1], result);
        err = stack_i[idx_i-2].le(stack_i[idx_i-1], result);
        //mod 20150929:e
        idx_i -= 2;
    }
    return err;
}

inline int ObPostfixExpression::lt_func(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params)
{
    int err = OB_SUCCESS;
    UNUSED(params);
    if (NULL == stack_i)
    {
        TBSYS_LOG(WARN, "stack pointer is NULL.");
        err = OB_INVALID_ARGUMENT;
    }
    else if (idx_i < 2)
    {
        TBSYS_LOG(WARN, "no enough operand in the stack. current size:%d", idx_i);
        err = OB_INVALID_ARGUMENT;
    }
    else
    {
        //mod peiouya [DATE_TIME] 20150929:b
        //stack_i[idx_i-2].lt(stack_i[idx_i-1], result);
        err = stack_i[idx_i-2].lt(stack_i[idx_i-1], result);
        //mod 20150929:e
        idx_i -= 2;
    }
    return err;
}

inline int ObPostfixExpression::eq_func(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params)
{
    int err = OB_SUCCESS;
    UNUSED(params);
    if (NULL == stack_i)
    {
        TBSYS_LOG(WARN, "stack pointer is NULL.");
        err = OB_INVALID_ARGUMENT;
    }
    else if (idx_i < 2)
    {
        TBSYS_LOG(WARN, "no enough operand in the stack. current size:%d", idx_i);
        err = OB_INVALID_ARGUMENT;
    }
    else
    {
        //mod peiouya [DATE_TIME] 20150929:b
        //stack_i[idx_i-2].eq(stack_i[idx_i-1], result);
        err = stack_i[idx_i-2].eq(stack_i[idx_i-1], result);
        //mod 20150929:e
        idx_i -= 2;
    }
    return err;
}

inline int ObPostfixExpression::neq_func(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params)
{
    int err = OB_SUCCESS;
    UNUSED(params);
    if (NULL == stack_i)
    {
        TBSYS_LOG(WARN, "stack pointer is NULL.");
        err = OB_INVALID_ARGUMENT;
    }
    else if (idx_i < 2)
    {
        TBSYS_LOG(WARN, "no enough operand in the stack. current size:%d", idx_i);
        err = OB_INVALID_ARGUMENT;
    }
    else
    {
        //mod peiouya [DATE_TIME] 20150929:b
        //stack_i[idx_i-2].ne(stack_i[idx_i-1], result);
        err = stack_i[idx_i-2].ne(stack_i[idx_i-1], result);
        //mod 20150929:e
        idx_i -= 2;
    }
    return err;
}

inline int ObPostfixExpression::is_not_func(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params)
{
    int err = OB_SUCCESS;
    UNUSED(params);
    if (NULL == stack_i)
    {
        TBSYS_LOG(WARN, "stack pointer is NULL.");
        err = OB_INVALID_ARGUMENT;
    }
    else if (idx_i < 2)
    {
        TBSYS_LOG(WARN, "no enough operand in the stack. current size:%d", idx_i);
        err = OB_INVALID_ARGUMENT;
    }
    else
    {
        stack_i[idx_i-2].is_not(stack_i[idx_i-1], result);
        idx_i -= 2;
    }
    return err;
}

inline int ObPostfixExpression::is_func(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params)
{
    int err = OB_SUCCESS;
    UNUSED(params);
    if (NULL == stack_i)
    {
        TBSYS_LOG(WARN, "stack pointer is NULL.");
        err = OB_INVALID_ARGUMENT;
    }
    else if (idx_i < 2)
    {
        TBSYS_LOG(WARN, "no enough operand in the stack. current size:%d", idx_i);
        err = OB_INVALID_ARGUMENT;
    }
    else
    {
        stack_i[idx_i-2].is(stack_i[idx_i-1], result);
        idx_i -= 2;
    }
    return err;
}


inline int ObPostfixExpression::add_func(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params)
{
    int err = OB_SUCCESS;
    UNUSED(params);
    if (NULL == stack_i)
    {
        TBSYS_LOG(WARN, "stack pointer is NULL.");
        err = OB_INVALID_ARGUMENT;
    }
    else if (idx_i < 2)
    {
        TBSYS_LOG(WARN, "no enough operand in the stack. current size:%d", idx_i);
        err = OB_INVALID_ARGUMENT;
    }
    else
    {
        //mod peiouya [DATE_TIME] 20150906:b
        //stack_i[idx_i-2].add(stack_i[idx_i-1], result);
        err = stack_i[idx_i-2].add(stack_i[idx_i-1], result);
        //mod 20150907:e
        idx_i -= 2;
    }
    return err;
}


inline int ObPostfixExpression::sub_func(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params)
{
    int err = OB_SUCCESS;
    UNUSED(params);
    if (NULL == stack_i)
    {
        TBSYS_LOG(WARN, "stack pointer is NULL.");
        err = OB_INVALID_ARGUMENT;
    }
    else if (idx_i < 2)
    {
        TBSYS_LOG(WARN, "no enough operand in the stack. current size:%d", idx_i);
        err = OB_INVALID_ARGUMENT;
    }
    else
    {
        //mod peiouya [DATE_TIME] 20150906:b
        //stack_i[idx_i-2].sub(stack_i[idx_i-1], result);
        err = stack_i[idx_i-2].sub(stack_i[idx_i-1], result);
        //mod 20150906:e
        idx_i -= 2;
    }
    return err;
}


inline int ObPostfixExpression::mul_func(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params)
{
    int err = OB_SUCCESS;
    UNUSED(params);
    if (NULL == stack_i)
    {
        TBSYS_LOG(WARN, "stack pointer is NULL.");
        err = OB_INVALID_ARGUMENT;
    }
    else if (idx_i < 2)
    {
        TBSYS_LOG(WARN, "no enough operand in the stack. current size:%d", idx_i);
        err = OB_INVALID_ARGUMENT;
    }
    else
    {
        //mod peiouya [DATE_TIME] 20150906:b
        //stack_i[idx_i-2].mul(stack_i[idx_i-1], result);
        err = stack_i[idx_i-2].mul(stack_i[idx_i-1], result);
        //mod 20150906:e
        idx_i -= 2;
    }
    return err;
}


inline int ObPostfixExpression::div_func(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params)
{
    int err = OB_SUCCESS;
    if (NULL == stack_i)
    {
        TBSYS_LOG(WARN, "stack pointer is NULL.");
        err = OB_INVALID_ARGUMENT;
    }
    else if (idx_i < 2)
    {
        TBSYS_LOG(WARN, "no enough operand in the stack. current size:%d", idx_i);
        err = OB_INVALID_ARGUMENT;
    }
    else
    {
        //mod peiouya [DATE_TIME] 20150906:b
        //stack_i[idx_i-2].div(stack_i[idx_i-1], result, params.did_int_div_as_double_);
        err = stack_i[idx_i-2].div(stack_i[idx_i-1], result, params.did_int_div_as_double_);
        //mod 20150907:e
        idx_i -= 2;
    }
    return err;
}

inline int ObPostfixExpression::mod_func(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params)
{
    int err = OB_SUCCESS;
    UNUSED(params);
    if (NULL == stack_i)
    {
        TBSYS_LOG(WARN, "stack pointer is NULL.");
        err = OB_INVALID_ARGUMENT;
    }
    else if (idx_i < 2)
    {
        TBSYS_LOG(WARN, "no enough operand in the stack. current size:%d", idx_i);
        err = OB_INVALID_ARGUMENT;
    }
    else
    {
        //mod peiouya [DATE_TIME] 20150906:b
        //stack_i[idx_i-2].mod(stack_i[idx_i-1], result);
        err = stack_i[idx_i-2].mod(stack_i[idx_i-1], result);
        //mod 20150906:e
        idx_i -= 2;
    }
    return err;
}


inline int ObPostfixExpression::and_func(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params)
{
    int err = OB_SUCCESS;
    UNUSED(params);
    if (NULL == stack_i)
    {
        TBSYS_LOG(WARN, "stack pointer is NULL.");
        err = OB_INVALID_ARGUMENT;
    }
    else if (idx_i < 2)
    {
        TBSYS_LOG(WARN, "no enough operand in the stack. current size:%d", idx_i);
        err = OB_INVALID_ARGUMENT;
    }
    else
    {
        stack_i[idx_i-2].land(stack_i[idx_i-1], result);
        idx_i -= 2;
    }
    return err;
}

inline int ObPostfixExpression::or_func(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params)
{
    int err = OB_SUCCESS;
    UNUSED(params);
    if (NULL == stack_i)
    {
        TBSYS_LOG(WARN, "stack pointer is NULL.");
        err = OB_INVALID_ARGUMENT;
    }
    else if (idx_i < 2)
    {
        TBSYS_LOG(WARN, "no enough operand in the stack. current size:%d", idx_i);
        err = OB_INVALID_ARGUMENT;
    }
    else
    {
        stack_i[idx_i-2].lor(stack_i[idx_i-1], result);
        idx_i -= 2;
    }
    return err;
}

inline int ObPostfixExpression::minus_func(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params)
{
    int err = OB_SUCCESS;
    UNUSED(params);
    if (NULL == stack_i)
    {
        TBSYS_LOG(WARN, "stack pointer is NULL.");
        err = OB_INVALID_ARGUMENT;
    }
    else if (idx_i < 1)
    {
        TBSYS_LOG(WARN, "no enough operand in the stack. current size:%d", idx_i);
        err = OB_INVALID_ARGUMENT;
    }
    else
    {
        stack_i[idx_i-1].negate(result);
        idx_i -= 1;
    }
    return err;
}


inline int ObPostfixExpression::plus_func(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params)
{
    int err = OB_SUCCESS;
    UNUSED(params);
    if (NULL == stack_i)
    {
        TBSYS_LOG(WARN, "stack pointer is NULL.");
        err = OB_INVALID_ARGUMENT;
    }
    else if (idx_i < 1)
    {
        TBSYS_LOG(WARN, "no enough operand in the stack. current size:%d", idx_i);
        err = OB_INVALID_ARGUMENT;
    }
    else
    {
        // don't touch it whatever the type is
        result = stack_i[idx_i-1];
        idx_i -= 1;
    }
    return err;
}



inline int ObPostfixExpression::not_func(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params)
{
    int err = OB_SUCCESS;
    UNUSED(params);
    if (NULL == stack_i)
    {
        TBSYS_LOG(WARN, "stack pointer is NULL.");
        err = OB_INVALID_ARGUMENT;
    }
    else if (idx_i < 1)
    {
        TBSYS_LOG(WARN, "no enough operand in the stack. current size:%d", idx_i);
        err = OB_INVALID_ARGUMENT;
    }
    else
    {
        stack_i[idx_i-1].lnot(result);
        idx_i -= 1;
    }
    return err;
}

inline int ObPostfixExpression::sys_func_substr(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params)
{
    int err = OB_SUCCESS;
    ObString varchar;
    ObString res_varchar;

    if (NULL == stack_i)
    {
        TBSYS_LOG(WARN, "stack pointer is NULL.");
        err = OB_INVALID_ARGUMENT;
    }
    else if (idx_i < params.operand_count_)
    {
        TBSYS_LOG(WARN, "no enough operand in the stack. current size:%d", idx_i);
        err = OB_INVALID_ARGUMENT;
    }
    else
    {
        //TBSYS_LOG(INFO, "params.operand_count_ = %d, [3]=%s, [2]=%s, [1]=%s",
        //    params.operand_count_, to_cstring(stack_i[idx_i-3]), to_cstring(stack_i[idx_i-2]), to_cstring(stack_i[idx_i-1]));

        if (params.operand_count_ == 2)
        {
            err = stack_i[idx_i-2].substr(stack_i[idx_i-1], result, *params.str_buf_);
        }
        else if (params.operand_count_ == 3)
        {
            err = stack_i[idx_i-3].substr(stack_i[idx_i-2], stack_i[idx_i-1], result, *params.str_buf_);
        }
        else
        {
            err = OB_ERR_UNEXPECTED;
            TBSYS_LOG(WARN, " unexpected operand count %d", params.operand_count_);
        }
        idx_i -= params.operand_count_;
    }
    return err;

}
inline int ObPostfixExpression::like_func(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params)
{
    int err = OB_SUCCESS;
    UNUSED(params);
    if (NULL == stack_i)
    {
        TBSYS_LOG(WARN, "stack pointer is NULL.");
        err = OB_INVALID_ARGUMENT;
    }
    else if (idx_i < 2)
    {
        TBSYS_LOG(WARN, "no enough operand in the stack. current size:%d", idx_i);
        err = OB_INVALID_ARGUMENT;
    }
    else
    {
        err = stack_i[idx_i-2].like(stack_i[idx_i-1], result);
        idx_i -= 2;
    }
    return err;
}

inline int ObPostfixExpression::left_param_end_func(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params)
{
    int ret = OB_SUCCESS;
    int dim = params.operand_count_;
    if (OB_SUCCESS != (ret = const_cast<ObPostExprExtraParams &>(params).in_row_operator_.push_row(stack_i, idx_i, 1)))
    {
        TBSYS_LOG(WARN, "fail to push row into in_row_operator_. ret=%d", ret);
    }
    else
    {
        result.set_int((int64_t)dim);
    }
    return ret;
}

inline int ObPostfixExpression::row_func(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params)
{
    int ret = OB_SUCCESS;
    int row_count = params.operand_count_;
    if (NULL == stack_i)
    {
        TBSYS_LOG(WARN, "stack pointer is NULL.");
        ret = OB_INVALID_ARGUMENT;
    }
    else if (idx_i < row_count)
    {
        TBSYS_LOG(WARN, "no enough operand in the stack. current size:%d, row_count=%d", idx_i, row_count);
        ret = OB_INVALID_ARGUMENT;
    }
    //add tianz [SubQuery_for_Instmt] [JHOBv0.1] 20140508:b
    else if (-1 == row_count)
    {
        result.set_int((int64_t)row_count);
    }
    //add 20140508:e
    else
    {
        if (OB_SUCCESS != (ret = const_cast<ObPostExprExtraParams &>(params).in_row_operator_.push_row(stack_i, idx_i, row_count)))
        {
            TBSYS_LOG(WARN, "fail to push row to row operator. ret=%d", ret);
        }
        else
        {
            //TBSYS_LOG(INFO, "idx_i=%d, row_count=%d", idx_i, row_count);
            result.set_int((int64_t)row_count);
        }
    }
    return ret;
}

//mod peiouya peiouya [IN_TYPEBUG_FIX] 20151225:b
////add tianz [SubQuery_for_Instmt] [JHOBv0.1] 20140508:b

/*Exp: process in sub_query prepare check
    * @param bloom_filter[in]
    * @param hash_map[in]
    * @param sub_query_idx[in,out] ,used for or expression which may have more than one sub_query
    * @param second_check[in] , used for mark hashmap check
    */
//inline int ObPostfixExpression::in_sub_query_func(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params,ObBloomFilterV1* bloom_filter,hash::ObHashMap<common::ObRowkey,common::ObRowkey,common::hash::NoPthreadDefendMode>* hash_map, int &sub_query_idx ,bool second_check)
inline int ObPostfixExpression::in_sub_query_func(ObExprObj *stack_i, int &idx_i, ObExprObj &result,
                                                  const ObPostExprExtraParams &params,
                                                  ObBloomFilterV1* bloom_filter,
                                                  hash::ObHashMap<common::ObRowkey,common::ObRowkey,common::hash::NoPthreadDefendMode>* hash_map,
                                                  bool is_hashmap_contain_null,  //add peiouya [IN_AND NOT_IN_WITH_NULL_BUG_FIX] 20160518
                                                  common::ObArray<ObObjType> * hash_map_pair_type_desc,
                                                  int &sub_query_idx,
                                                  bool second_check)
{
    int ret = OB_SUCCESS;
    if (NULL == stack_i)
    {
        TBSYS_LOG(WARN, "stack pointer is NULL.");
        ret = OB_INVALID_ARGUMENT;
    }
    else if (idx_i < 2)
    {
        TBSYS_LOG(WARN, "no enough operand in the stack. current size:%d", idx_i);
        ret = OB_INVALID_ARGUMENT;
    }
    else
    {
        int64_t right_elem_count = 0;
        if (OB_SUCCESS != (ret = stack_i[idx_i-1].get_int(right_elem_count)))
        {
            TBSYS_LOG(WARN, "fail to get_int from stack. top=%d, ret=%d", idx_i, ret);
        }
        else if(-1 == right_elem_count)
        {
            //mod peiouya [IN_AND NOT_IN_WITH_NULL_BUG_FIX] 20160518:b
            ////mod peiouya [IN_TYPEBUG_FIX] 20151225:b
            ////if(OB_SUCCESS !=(ret = get_result(stack_i,idx_i,result,params,bloom_filter,hash_map,sub_query_idx,second_check)))
            //if(OB_SUCCESS !=(ret = get_result(stack_i,idx_i, result, params, bloom_filter, hash_map, hash_map_pair_type_desc, sub_query_idx, second_check)))
            //    //mod 20151225:e
            if(OB_SUCCESS !=(ret = get_result(stack_i,idx_i, result, params, bloom_filter, hash_map,is_hashmap_contain_null, hash_map_pair_type_desc, sub_query_idx, second_check)))
            //mod peiouya [IN_AND NOT_IN_WITH_NULL_BUG_FIX] 20160518:e
            {
                TBSYS_LOG(WARN, "fail to get IN operation result. ret=%d", ret);
            }

        }
        else
        {
            //should not be here
            TBSYS_LOG(WARN, "invialid argument");
            ret = OB_INVALID_ARGUMENT;
        }
    }
    return ret;
}
//add wanglei [to date optimization] 20160328
int ObPostfixExpression::get_result (ObExprObj & result)
{
    int ret = OB_SUCCESS;
    result = stack_[0];
    return ret;
}

bool ObPostfixExpression::is_op_is_null()
{
  //COLUMN_IDX | NULL | OP | T_OP_IS | 2
  bool res = false;
  int ret = OB_SUCCESS;
  int64_t idx = 0;
  int64_t type = 0;
  idx = idx + get_type_num(idx, COLUMN_IDX);
  if(OB_SUCCESS != (ret = expr_[idx].get_int(type)))
  {
    TBSYS_LOG(WARN, "Fail to get type. unexpected! ret=%d", ret);
  }
  else if(CONST_OBJ == type)
  {
    if(ObNullType == expr_[idx+1].get_type())
    {
      idx = idx + get_type_num(idx, CONST_OBJ);
      if(OB_SUCCESS != (ret = expr_[idx].get_int(type)))
      {
        TBSYS_LOG(WARN, "Fail to get type. unexpected! ret=%d", ret);
      }
      else if(OP == type)
      {
        int64_t op_type = 0;
        if(OB_SUCCESS != (ret = expr_[idx+1].get_int(op_type)))
        {
          TBSYS_LOG(WARN, "Fail to get op type. unexpected! ret=%d", ret);
        }
        else if(T_OP_IS == op_type)
        {
          res = true;
        }
      }
    }
  }
  return res;
}
/*Exp: process in sub_query, do real work
    * @param bloom_filter[in]
    * @param hash_map[in]
    * @param sub_query_idx[in,out] ,used for or expression which may have more than one sub_query
    * @param second_check[in] , used for mark hashmap check
    */
//int ObPostfixExpression::get_result(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params,
//common::ObBloomFilterV1 * bloom_filter,
//hash::ObHashMap<common::ObRowkey,
//common::ObRowkey,
//common::hash::NoPthreadDefendMode>* hash_map, int &sub_query_idx,bool second_check )
int ObPostfixExpression::get_result(ObExprObj *stack_i, int &idx_i, ObExprObj &result,
                                    const ObPostExprExtraParams &params,
                                    common::ObBloomFilterV1 * bloom_filter,
                                    hash::ObHashMap<common::ObRowkey,common::ObRowkey,common::hash::NoPthreadDefendMode>* hash_map,
                                    bool is_hashmap_contain_null,  //add peiouya [IN_AND NOT_IN_WITH_NULL_BUG_FIX] 20160518
                                    common::ObArray<ObObjType> * hash_map_pair_type_desc,
                                    int &sub_query_idx,
                                    bool second_check)
{
    int ret = OB_SUCCESS;
    ObArray<ObExprObj> &vec_ = const_cast<ObPostExprExtraParams &>(params).in_row_operator_.vec_;
    int64_t left_start_idx = 0;
    int64_t right_elem_count = 0;
    int64_t width = 0;
    int64_t dim = 0;
    int64_t vec_top = 0;
    ObExprObj width_obj;

    OB_ASSERT(NULL != stack_i);
    OB_ASSERT(idx_i >= 2);

    if (OB_SUCCESS != (ret = stack_i[--idx_i].get_int(right_elem_count)))
    {
        TBSYS_LOG(WARN, "fail to get_int from stack. top=%d, ret=%d", idx_i, ret);
    }
    else if (OB_SUCCESS != (ret = stack_i[--idx_i].get_int(dim)))
    {
        TBSYS_LOG(WARN, "fail to get_int from stack. top=%d, ret=%d", idx_i, ret);
    }
    else
    {
        vec_top = vec_.count();
        if (OB_SUCCESS != (ret = vec_.at(vec_top - 1, width_obj)))
        {
            TBSYS_LOG(WARN, "fail to get width_obj from array. vec_top=%ld, ret=%d", vec_top, ret);
        }
        else if (OB_SUCCESS != (ret = width_obj.get_int(width)))
        {
            TBSYS_LOG(WARN, "fail to get_int from stack. top=%d, ret=%d", idx_i, ret);
        }
        else
        {
            left_start_idx  = vec_top - 1 - width;
            //Reverse order output
            ObObj *value = new ObObj[width];
            //mod peiouya [IN_TYPEBUG_FIX] 20151225:b
            char *varchar_buff = NULL;
            alloc_small_mem_return_ptr(varchar_buff, width, ret);
            //for(int i =0;i<width;i++)
            for(int i =0; OB_SUCCESS == ret && i<width;i++)
                //mod 20151225:e
            {
                ObExprObj temp_obj;
                ObObj temp_value;
                if (OB_SUCCESS != (ret = vec_.at(left_start_idx+i, temp_obj)))
                {
                    TBSYS_LOG(WARN, "fail to get width_obj from array. vec_top=%ld, ret=%d", vec_top, ret);
                }
                //add peiouya [IN_TYPEBUG_FIX] 20151225:b
                else if (ObDecimalType == temp_obj.get_type ())
                {
                    ObDecimal od = temp_obj.get_decimal ();
                    int64_t length = od.to_string (varchar_buff + OB_MAX_VARCHAR_LENGTH * i, OB_MAX_VARCHAR_LENGTH);
                    ObString os;
                    os.assign_ptr (varchar_buff + OB_MAX_VARCHAR_LENGTH * i, static_cast<int32_t>(length));
                    temp_value.set_decimal (os, od.get_precision (),od.get_scale (),od.get_vscale ());
                }
                //add 20151225:e
                else if(OB_SUCCESS != (ret = temp_obj.to(temp_value)))
                {
                    TBSYS_LOG(WARN, "fail to chang ObExprObj to ObObj. ");
                }
                value[width-1-i] = temp_value;
            }

            //add peiouya [IN_TYPEBUG_FIX] 20151225:b
            char *cast_varchar_buff = NULL;
            ObObj casted_cells[width];
            ObObj dst_value[width];
            alloc_small_mem_return_ptr(cast_varchar_buff, width, ret);
            bind_memptr_for_casted_cells(casted_cells, width, cast_varchar_buff);
            //add 20151225:e

            if(OB_SUCCESS!=ret )
            {
                TBSYS_LOG(WARN,"get result ,get data error");
            }
            else if(second_check)
            {
                ObRowkey p;
                p.assign(value,width);
                //mod peiouya [IN_TYPEBUG_FIX] 20151225:b
                if (NULL != hash_map_pair_type_desc && OB_SUCCESS != (ret = ob_cast_rowkey(hash_map_pair_type_desc, p, casted_cells, dst_value)))
                {
                    //nothing todo
                }
                else
                {
                    ObRowkey tmp_rowkey;
                    tmp_rowkey.assign (dst_value, width);
                    //mod peiouya [IN_AND NOT_IN_WITH_NULL_BUG_FIX] 20160518:b
                    if(0 >= hash_map[sub_query_idx ].size())
                    {
                      result.set_bool(false);
                    }
                    else if(is_arr_contain_null_value(dst_value, width))
                    {
                      result.set_null();
                    }
                    //if(NULL != hash_map[sub_query_idx ].get(p) )
                    else if(NULL != hash_map[sub_query_idx ].get(tmp_rowkey) )
                    {
                        result.set_bool(true);
                    }
                    else if (is_hashmap_contain_null)
                    {
                        result.set_null();
                    }
                    //mod peiouya [IN_AND NOT_IN_WITH_NULL_BUG_FIX] 20160518:e
                    else
                    {
                        result.set_bool(false);
                    }
                    //mod 20151225:e
                }
                sub_query_idx ++;
            }
            else if(NULL == bloom_filter)
            {
                TBSYS_LOG(WARN,"bloom filter should not be null");
            }
            else
            {
                //mod peiouya [IN_TYPEBUG_FIX] 20151225:b
                ObRowkey p;
                p.assign(value,width);
                hash_map_pair_type_desc = bloom_filter->get_rowkey_type_desc_ptr ();
                //mod peiouya [IN_TYPEBUG_FIX] 20151225:b
                //this branch is responsed for case excepet for 'IN （subquery�?  20160118:b
                //this process cannot ensure that result is ok.
                //user must ensure that corresponding columns' type is same.
                if (NULL != hash_map_pair_type_desc && 0 >= hash_map_pair_type_desc->count ())
                {
                    //mod peiouya [IN_AND NOT_IN_WITH_NULL_BUG_FIX] 20160518:b
                    if(is_arr_contain_null_value(value, width))
                    {
                      result.set_null();
                    }
                    else if(bloom_filter->may_contain(value,width) )
                    //if(bloom_filter->may_contain(value,width) )
                    //mod peiouya [IN_AND NOT_IN_WITH_NULL_BUG_FIX] 20160518:e
                    {
                        result.set_bool(true);
                    }
                    else
                    {
                        result.set_bool(false);
                    }
                }
                //20160118:e
                else if (NULL != hash_map_pair_type_desc && OB_SUCCESS != (ret = ob_cast_rowkey(hash_map_pair_type_desc, p, casted_cells, dst_value)))
                {
                    //nothing todo
                }
                else
                {
                    //mod peiouya [IN_AND NOT_IN_WITH_NULL_BUG_FIX] 20160518:b
                    if(is_arr_contain_null_value(dst_value, width))
                    {
                      result.set_null();
                    }
                    else if(bloom_filter->may_contain(dst_value,width) )
                    //if(bloom_filter->may_contain(dst_value,width) )
                    //mod peiouya [IN_AND NOT_IN_WITH_NULL_BUG_FIX] 20160518:e
                    {
                        result.set_bool(true);
                    }
                    else
                    {
                        result.set_bool(false);
                    }
                }
                //mod 20151225:e
            }

            delete [] value;
            vec_.pop_back();//left param num

            for(int i =0;i<width;i++)
            {
                vec_.pop_back();//left param
            }
            ob_free(varchar_buff); //add peiouya [IN_TYPEBUG_FIX] 20151225
            ob_free(cast_varchar_buff); //add peiouya [IN_TYPEBUG_FIX] 20151225
        }
    }
    return ret;

}
//add20140508:e
inline int ObPostfixExpression::in_func(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params)
{
    // in的算�?    //
    // �?    // 2 IN (3, 4) 的后缀表达式为
    // [栈顶] 2, 3, 4, Row(2), IN(2)
    // in_row_operator_中数据的layout为：
    // 2 3 4
    // width = 1
    //
    // �?    // (1, 3) IN ((3, 4), (1, 2))的后缀表达式为
    // [栈顶] 1, 3, Row(2), 3, 4, Row(2), 1, 2, Row(2), Row(2), IN(2)
    // in_row_operator_中数据的layout为：
    // 1 3 3 4 1 2
    // width = 2
    //
    // 显然，根据width，取出前width个数，逐个往后比较即可计算得到in的结�?    // note: 1. 该后缀表达式计算中，左操作数先出栈 2. 最后一个T_OP_ROW操作被实际上忽略
    //
    int ret = OB_SUCCESS;
    if (NULL == stack_i)
    {
        TBSYS_LOG(WARN, "stack pointer is NULL.");
        ret = OB_INVALID_ARGUMENT;
    }
    else if (idx_i < 2)
    {
        TBSYS_LOG(WARN, "no enough operand in the stack. current size:%d", idx_i);
        ret = OB_INVALID_ARGUMENT;
    }
    else
    {
        if (OB_SUCCESS != (ret = const_cast<ObPostExprExtraParams &>(params).in_row_operator_.get_result(stack_i, idx_i, result,true)))
        {
            TBSYS_LOG(WARN, "fail to get IN operation result. ret=%d", ret);
        }
    }
    return ret;
}

inline int ObPostfixExpression::not_in_func(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params)
{
    int ret = OB_SUCCESS;
    if (NULL == stack_i)
    {
        TBSYS_LOG(WARN, "stack pointer is NULL.");
        ret = OB_INVALID_ARGUMENT;
    }
    else if (idx_i < 2)
    {
        TBSYS_LOG(WARN, "no enough operand in the stack. current size:%d", idx_i);
        ret = OB_INVALID_ARGUMENT;
    }
    else
    {
        if (OB_SUCCESS != (ret = const_cast<ObPostExprExtraParams &>(params).in_row_operator_.get_result(stack_i, idx_i, result,false)))
        {
            TBSYS_LOG(WARN, "fail to get IN operation result. ret=%d", ret);
        }
    }
    return ret;
}

int ObPostfixExpression::not_like_func(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params)
{
    int err = OB_SUCCESS;
    UNUSED(params);
    if (NULL == stack_i)
    {
        TBSYS_LOG(WARN, "stack pointer is NULL.");
        err = OB_INVALID_ARGUMENT;
    }
    else if (idx_i < 2)
    {
        TBSYS_LOG(WARN, "no enough operand in the stack. current size:%d", idx_i);
        err = OB_INVALID_ARGUMENT;
    }
    else
    {
        stack_i[idx_i-2].not_like(stack_i[idx_i-1], result);
        idx_i -= 2;
    }
    return err;
}

int ObPostfixExpression::btw_func(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params)
{
    int ret = OB_SUCCESS;
    UNUSED(params);
    if (NULL == stack_i)
    {
        TBSYS_LOG(WARN, "stack pointer is NULL.");
        ret = OB_INVALID_ARGUMENT;
    }
    else if (idx_i < 3)
    {
        TBSYS_LOG(WARN, "no enough operand in the stack. current size:%d", idx_i);
        ret = OB_INVALID_ARGUMENT;
    }
    else
    {
        //mod peiouya [DATE_TIME] 20150929:b
        //stack_i[idx_i-3].btw(stack_i[idx_i-2], stack_i[idx_i-1], result);
        ret = stack_i[idx_i-3].btw(stack_i[idx_i-2], stack_i[idx_i-1], result);
        //mod 20150929:e
        idx_i -= 3;
    }
    return ret;
}

int ObPostfixExpression::not_btw_func(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params)
{
    int ret = OB_SUCCESS;
    UNUSED(params);
    if (NULL == stack_i)
    {
        TBSYS_LOG(WARN, "stack pointer is NULL.");
        ret = OB_INVALID_ARGUMENT;
    }
    else if (idx_i < 3)
    {
        TBSYS_LOG(WARN, "no enough operand in the stack. current size:%d", idx_i);
        ret = OB_INVALID_ARGUMENT;
    }
    else
    {
        //mod peiouya [DATE_TIME] 20150929:b
        //stack_i[idx_i-3].not_btw(stack_i[idx_i-2], stack_i[idx_i-1], result);
        ret = stack_i[idx_i-3].not_btw(stack_i[idx_i-2], stack_i[idx_i-1], result);
        //mod 20150929:e
        idx_i -= 3;
    }
    return ret;
}

const char * ObPostfixExpression::get_sys_func_name(enum ObSqlSysFunc func_id)
{
    const char *ret;
    if (func_id >= SYS_FUNC_LENGTH && func_id < SYS_FUNC_NUM)
    {
        ret = SYS_FUNCS_NAME[func_id];
    }
    else
    {
        ret = "";
        TBSYS_LOG(ERROR, "unexpected sql sys func type %d", func_id);
    }
    return ret;
}

int ObPostfixExpression::get_sys_func_param_num(const ObString& name, int32_t& param_num)
{
    int ret = OB_SUCCESS;
    int32_t i = 0;
    for (i = SYS_FUNC_LENGTH; i < SYS_FUNC_NUM; i++)
    {
        if (static_cast<int32_t>(strlen(SYS_FUNCS_NAME[i])) == name.length()
                && strncasecmp(SYS_FUNCS_NAME[i], name.ptr(), name.length()) == 0)
            break;
    }
    if (i >= SYS_FUNC_LENGTH && i < SYS_FUNC_NUM)
    {
        param_num = SYS_FUNCS_ARGS_NUM[i];
    }
    else
    {
        ret = OB_ERR_FUNCTION_UNKNOWN;
        TBSYS_LOG(WARN, "Unknown function '%.*s', ret=%d", name.length(), name.ptr(), ret);
    }
    return ret;
}

inline int ObPostfixExpression::sys_func_ip_to_int(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params)
{
    int err = OB_SUCCESS;
    UNUSED(params);
    if (OB_UNLIKELY(NULL == stack_i))
    {
        TBSYS_LOG(WARN, "stack pointer is NULL.");
        err = OB_INVALID_ARGUMENT;
    }
    else if (OB_UNLIKELY(idx_i < 1))
    {
        TBSYS_LOG(WARN, "no enough operand in the stack. current size:%d", idx_i);
        err = OB_INVALID_ARGUMENT;
    }
    else
    {
        stack_i[idx_i - 1].ip_to_int(result);
        idx_i -= 1;
    }
    return err;
}
inline int ObPostfixExpression::sys_func_int_to_ip(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params)
{
    int err = OB_SUCCESS;
    UNUSED(params);
    if (OB_UNLIKELY(NULL == stack_i))
    {
        TBSYS_LOG(WARN, "stack pointer is NULL.");
        err = OB_INVALID_ARGUMENT;
    }
    else if (OB_UNLIKELY(idx_i < 1))
    {
        TBSYS_LOG(WARN, "no enough operand in the stack. current size:%d", idx_i);
        err = OB_INVALID_ARGUMENT;
    }
    else
    {
        stack_i[idx_i - 1].int_to_ip(result, *(params.str_buf_));
        idx_i -= 1;
    }
    return err;
}
inline int ObPostfixExpression::sys_func_unhex(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params)
{
    int err = OB_SUCCESS;
    UNUSED(params);
    if (OB_UNLIKELY(NULL == stack_i))
    {
        TBSYS_LOG(WARN, "stack pointer is NULL.");
        err = OB_INVALID_ARGUMENT;
    }
    else if (OB_UNLIKELY(idx_i < 1))
    {
        TBSYS_LOG(WARN, "no enough operand in the stack. current size:%d", idx_i);
        err = OB_INVALID_ARGUMENT;
    }
    else
    {
        stack_i[idx_i - 1].unhex(result, *(params.str_buf_));
        idx_i -= 1;
    }
    return err;
}
inline int ObPostfixExpression::sys_func_hex(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params)
{
    int err = OB_SUCCESS;
    UNUSED(params);
    if (OB_UNLIKELY(NULL == stack_i))
    {
        TBSYS_LOG(WARN, "stack pointer is NULL.");
        err = OB_INVALID_ARGUMENT;
    }
    else if (OB_UNLIKELY(idx_i < 1))
    {
        TBSYS_LOG(WARN, "no enough operand in the stack. current size:%d", idx_i);
        err = OB_INVALID_ARGUMENT;
    }
    else
    {
        stack_i[idx_i - 1].hex(result, *(params.str_buf_));
        idx_i -= 1;
    }
    return err;
}
inline int ObPostfixExpression::sys_func_length(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params)
{
    int err = OB_SUCCESS;
    UNUSED(params);
    if (OB_UNLIKELY(NULL == stack_i))
    {
        TBSYS_LOG(WARN, "stack pointer is NULL.");
        err = OB_INVALID_ARGUMENT;
    }
    else if (OB_UNLIKELY(idx_i < 1))
    {
        TBSYS_LOG(WARN, "no enough operand in the stack. current size:%d", idx_i);
        err = OB_INVALID_ARGUMENT;
    }
    else
    {
        stack_i[idx_i-1].varchar_length(result);
        idx_i -= 1;
    }
    return err;
}

inline int ObPostfixExpression::sys_func_cast(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params)
{
    //modify fanqiushi DECIMAL OceanBase_BankCommV0.3 2014_7_19:b
    /*int err = OB_SUCCESS;
      if (OB_UNLIKELY(NULL == stack_i || NULL == params.str_buf_))
      {
        TBSYS_LOG(WARN, "stack_i=%p, str_buf_=%p.", stack_i, params.str_buf_);
        err = OB_INVALID_ARGUMENT;
      }
      else if (OB_UNLIKELY(idx_i < 2))
      {
        TBSYS_LOG(WARN, "no enough operand in the stack. current size:%d", idx_i);
        err = OB_INVALID_ARGUMENT;
      }
      else
      {
        int64_t literal_type = 0;
        int32_t dest_type = 0;
        if (OB_SUCCESS != (err = stack_i[idx_i-1].get_int(literal_type)))
        {
          TBSYS_LOG(WARN, "fail to get int value. actual type = %d. err=%d", stack_i[idx_i-1].get_type(), err);
        }
        else
        {
          // convert literal data type to inner data type
          dest_type = convert_item_type_to_obj_type(static_cast<ObItemType>(literal_type));
          if (OB_SUCCESS == err)
          {
            if (OB_SUCCESS != (err = stack_i[idx_i-2].cast_to(dest_type, result, *params.str_buf_)))
            {
              TBSYS_LOG(WARN, "fail to cast data from type %d to type %d. err=%d", stack_i[idx_i-2].get_type(), dest_type, err);
            }
          }
        }
        idx_i -= 2;
      }
      return err;*/          //old code
    int err = OB_SUCCESS;
    if (OB_UNLIKELY(NULL == stack_i || NULL == params.str_buf_))
    {
        TBSYS_LOG(WARN, "stack_i=%p, str_buf_=%p.", stack_i, params.str_buf_);
        err = OB_INVALID_ARGUMENT;
    }
    else if (OB_UNLIKELY(idx_i < 2))
    {
        TBSYS_LOG(WARN, "no enough operand in the stack. current size:%d", idx_i);
        err = OB_INVALID_ARGUMENT;
    }
    else
    {
        int64_t literal_type = 0;
        int32_t dest_type = 0;
        if(params.operand_count_==4)
        {
            if (OB_SUCCESS != (err = stack_i[idx_i-3].get_int(literal_type)))
            {
                TBSYS_LOG(WARN, "fail to get int value. actual type = %d. err=%d", stack_i[idx_i-3].get_type(), err);
            }
            else
            {
                // convert literal data type to inner data type
                dest_type = convert_item_type_to_obj_type(static_cast<ObItemType>(literal_type));
                int64_t pre=0;
                int64_t scale=0;
                stack_i[idx_i-2].get_int(pre);
                stack_i[idx_i-1].get_int(scale);
                if (OB_SUCCESS == err)
                {
                    if (OB_SUCCESS != (err = stack_i[idx_i-4].cast_toV2(dest_type, result, *params.str_buf_,(uint32_t)(pre),(uint32_t)(scale))))
                    {
                        TBSYS_LOG(WARN, "fail to cast data from type %d to type %d. err=%d", stack_i[idx_i-4].get_type(), dest_type, err);
                    }
                }
            }
            idx_i -= 4;
        }
        else
        {

            if (OB_SUCCESS != (err = stack_i[idx_i-1].get_int(literal_type)))
            {
                TBSYS_LOG(WARN, "fail to get int value. actual type = %d. err=%d", stack_i[idx_i-1].get_type(), err);
            }
            else
            {
                // convert literal data type to inner data type
                dest_type = convert_item_type_to_obj_type(static_cast<ObItemType>(literal_type));
                if (OB_SUCCESS == err)
                {
                    if (OB_SUCCESS != (err = stack_i[idx_i-2].cast_to(dest_type, result, *params.str_buf_)))
                    {
                        TBSYS_LOG(WARN, "fail to cast data from type %d to type %d. err=%d", stack_i[idx_i-2].get_type(), dest_type, err);
                    }
                }
            }
            idx_i -= 2;
        }
    }
    return err;
    //modify:e
}

inline int ObPostfixExpression::sys_func_current_timestamp(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params)
{
    UNUSED(stack_i);
    UNUSED(idx_i);
    UNUSED(params);

    result.set_precise_datetime(tbsys::CTimeUtil::getTime());
    return OB_SUCCESS;
}

inline int ObPostfixExpression::sys_func_cur_user(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params)
{
    UNUSED(stack_i);
    UNUSED(idx_i);
    UNUSED(result);
    UNUSED(params);
    return OB_NOT_SUPPORTED;
}

inline int ObPostfixExpression::sys_func_trim(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params)
{
    int err = OB_SUCCESS;
    if (NULL == stack_i || NULL == params.str_buf_)
    {
        TBSYS_LOG(WARN, "stack_i=%p, str_buf_=%p.", stack_i, params.str_buf_);
        err = OB_INVALID_ARGUMENT;
    }
    else if (idx_i < 3)
    {
        TBSYS_LOG(WARN, "no enough operand in the stack. current size:%d", idx_i);
        err = OB_INVALID_ARGUMENT;
    }
    else
    {
        if (OB_SUCCESS != (err = stack_i[idx_i-1].trim(stack_i[idx_i-3], stack_i[idx_i-2], result, *params.str_buf_)))
        {
            TBSYS_LOG(WARN, "fail to trim value. err=%d", err);
        }
        idx_i -= 3;
    }
    return err;
}

inline int ObPostfixExpression::sys_func_lower(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params)
{
    int err = OB_SUCCESS;
    if (NULL == stack_i || NULL == params.str_buf_)
    {
        TBSYS_LOG(WARN, "stack_i=%p, str_buf_=%p.", stack_i, params.str_buf_);
        err = OB_INVALID_ARGUMENT;
    }
    else if (idx_i < 1)
    {
        TBSYS_LOG(WARN, "no enough operand in the stack. current size:%d", idx_i);
        err = OB_INVALID_ARGUMENT;
    }
    else
    {
        if (OB_SUCCESS != (err = stack_i[idx_i-1].lower_case(result, *params.str_buf_)))
        {
            TBSYS_LOG(WARN, "fail to get lower value. err=%d", err);
        }
        idx_i -= 1;
    }
    return err;
}

inline int ObPostfixExpression::sys_func_upper(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params)
{
    int err = OB_SUCCESS;
    if (NULL == stack_i || NULL == params.str_buf_)
    {
        TBSYS_LOG(WARN, "stack_i=%p, str_buf_=%p.", stack_i, params.str_buf_);
        err = OB_INVALID_ARGUMENT;
    }
    else if (idx_i < 1)
    {
        TBSYS_LOG(WARN, "no enough operand in the stack. current size:%d", idx_i);
        err = OB_INVALID_ARGUMENT;
    }
    else
    {
        if (OB_SUCCESS != (err = stack_i[idx_i-1].upper_case(result, *params.str_buf_)))
        {
            TBSYS_LOG(WARN, "fail to get lower value. err=%d", err);
        }
        idx_i -= 1;
    }
    return err;
}

inline int ObPostfixExpression::sys_func_coalesce(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params)
{
    int err = OB_SUCCESS;
    ObObj obj;
    if (NULL == stack_i)
    {
        TBSYS_LOG(WARN, "stack pointer is NULL.");
        err = OB_INVALID_ARGUMENT;
    }
    else if (idx_i < params.operand_count_)
    {
        TBSYS_LOG(WARN, "no enough operand in the stack. current size:%d", idx_i);
        err = OB_INVALID_ARGUMENT;
    }
    else
    {
        result.set_null();
        for (int i = params.operand_count_; i > 0; i--)
        {
            if (!stack_i[idx_i - i].is_null())
            {
                //add dolphin[coalesce return type]@20151126:b
                ObExprObj E_obj = stack_i[idx_i - i];
                if(E_obj.get_type()==ObDecimalType)
                {
                    if(ObDecimalType != params.expr_type_)
                    {
                        err = OB_INVALID_ARGUMENT;
                        TBSYS_LOG(WARN, "invalid cast specification for coalesce, type=%d but expr_type is %d,ret=%d", E_obj.get_type(), params.expr_type_, err);
                        break;
                    }
                    else
                    {
                        result.set_decimal(E_obj.get_decimal());
                        break;
                    }

                }
                else
                {
                    if (OB_SUCCESS != (err = E_obj.to(obj)))
                    {
                        TBSYS_LOG(WARN, "fail to copy object. err=%d", err);
                        break;
                    }
                    ObObj ex_type;
                    ex_type.set_type(params.expr_type_);
                    ObObj res;
                    char buf[MAX_PRINTABLE_SIZE];
                    res.set_varchar(ObString(MAX_PRINTABLE_SIZE,MAX_PRINTABLE_SIZE,buf));
                    const ObObj *temp;
                    if(OB_SUCCESS != (err = obj_cast(obj,ex_type,res,temp)))
                    {
                        char bf[256];
                        obj.to_string(bf,255);
                        TBSYS_LOG(WARN,"failed to cast %s to type %d,ret=%d",bf,params.expr_type_,err);
                        break;
                    }
                    else
                    {
                        result.assign(*temp);
                        break;
                    }

                }
                //add:e

                //del dolphin[coalesce return type]@20151126:b
                /*            if (OB_SUCCESS != (err = stack_i[idx_i - i].to(obj)))
            {
              TBSYS_LOG(WARN, "fail to copy object. err=%d", err);
            }
            else
            {
              result.assign(obj);
            }
            break;*/
                //del:e
            }
        }
        idx_i -= params.operand_count_;
    }
    return err;
}

inline int ObPostfixExpression::sys_func_greatest(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params)
{
    int err = OB_SUCCESS;
    ObObj obj;
    if (NULL == stack_i)
    {
        TBSYS_LOG(WARN, "stack pointer is NULL.");
        err = OB_INVALID_ARGUMENT;
    }
    else if (idx_i < params.operand_count_)
    {
        TBSYS_LOG(WARN, "no enough operand in the stack. current size:%d", idx_i);
        err = OB_INVALID_ARGUMENT;
    }
    else
    {
        int i = params.operand_count_;
        int cmp = 0;
        ObExprObj *greatest = NULL;
        result.set_null();

        if (i > 0)
        {
            greatest = &stack_i[idx_i - i];
            i--;
        }

        for (; i > 0; i--)
        {
            if (OB_SUCCESS == (err = stack_i[idx_i-i].compare(*greatest, cmp)))
            {
                if (0 < cmp)
                {
                    greatest = &stack_i[idx_i-i];
                }
            }
            else
            {
                break;
            }
        }
        if (OB_SUCCESS == err)
        {
            if (NULL != greatest)
            {
                greatest->to(obj);
                result.assign(obj);
                idx_i -= params.operand_count_;
            }
            else
            {
                err = OB_INVALID_ARGUMENT;
            }
        }
        else if (OB_RESULT_UNKNOWN == err)
        {
            result.set_null(); // default is null
            idx_i -= params.operand_count_;
            err = OB_SUCCESS;
        }
    }
    return err;
}

inline int ObPostfixExpression::sys_func_least(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params)
{
    int err = OB_SUCCESS;
    ObObj obj;
    if (NULL == stack_i)
    {
        TBSYS_LOG(WARN, "stack pointer is NULL.");
        err = OB_INVALID_ARGUMENT;
    }
    else if (idx_i < params.operand_count_)
    {
        TBSYS_LOG(WARN, "no enough operand in the stack. current size:%d", idx_i);
        err = OB_INVALID_ARGUMENT;
    }
    else
    {
        int i = params.operand_count_;
        int cmp = 0;
        ObExprObj *greatest = NULL;
        result.set_null();

        if (i > 0)
        {
            greatest = &stack_i[idx_i - i];
            i--;
        }

        for (; i > 0; i--)
        {
            if (OB_SUCCESS == (err = stack_i[idx_i-i].compare(*greatest, cmp)))
            {
                if (0 > cmp)
                {
                    greatest = &stack_i[idx_i-i];
                }
            }
            else
            {
                break;
            }
        }
        if (OB_SUCCESS == err)
        {
            if (NULL != greatest)
            {
                greatest->to(obj);
                result.assign(obj);
                idx_i -= params.operand_count_;
            }
            else
            {
                err = OB_INVALID_ARGUMENT;
            }
        }
        else if (OB_RESULT_UNKNOWN == err)
        {
            result.set_null(); // default is null
            idx_i -= params.operand_count_;
            err = OB_SUCCESS;
        }
    }
    return err;
}
// add wuna [datetime func] 20150828:b
inline int ObPostfixExpression::sys_func_year(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params)
{
    int err = OB_SUCCESS;
    UNUSED(params);
    if (OB_UNLIKELY(NULL == stack_i))
    {
        TBSYS_LOG(WARN, "stack pointer is NULL.");
        err = OB_INVALID_ARGUMENT;
    }
    else if (OB_UNLIKELY(idx_i < 1))
    {
        TBSYS_LOG(WARN, "no enough operand in the stack. current size:%d", idx_i);
        err = OB_INVALID_ARGUMENT;
    }
    else
    {
        err = stack_i[idx_i - 1].year(result);
        if(err!=OB_SUCCESS)
        {
            TBSYS_LOG(USER_ERROR, "the year func param is not corrected");
        }
        idx_i -= 1;
    }
    return err;
}
inline int ObPostfixExpression::sys_func_month(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params)
{
    int err = OB_SUCCESS;
    UNUSED(params);
    if (OB_UNLIKELY(NULL == stack_i))
    {
        TBSYS_LOG(WARN, "stack pointer is NULL.");
        err = OB_INVALID_ARGUMENT;
    }
    else if (OB_UNLIKELY(idx_i < 1))
    {
        TBSYS_LOG(WARN, "no enough operand in the stack. current size:%d", idx_i);
        err = OB_INVALID_ARGUMENT;
    }
    else
    {
        err = stack_i[idx_i - 1].month(result);
        if(err!=OB_SUCCESS)
        {
            TBSYS_LOG(USER_ERROR, "the month func param is not corrected");
        }
        idx_i -= 1;
    }
    return err;
}
inline int ObPostfixExpression::sys_func_day(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params)
{
    int err = OB_SUCCESS;
    UNUSED(params);
    if (OB_UNLIKELY(NULL == stack_i))
    {
        TBSYS_LOG(WARN, "stack pointer is NULL.");
        err = OB_INVALID_ARGUMENT;
    }
    else if (OB_UNLIKELY(idx_i < 1))
    {
        TBSYS_LOG(WARN, "no enough operand in the stack. current size:%d", idx_i);
        err = OB_INVALID_ARGUMENT;
    }
    else
    {
        err = stack_i[idx_i - 1].day(result);
        if(err!=OB_SUCCESS)
        {
            TBSYS_LOG(USER_ERROR, "the day func param is not corrected");
        }
        idx_i -= 1;
    }
    return err;
}
inline int ObPostfixExpression::sys_func_days(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params)
{
    int err = OB_SUCCESS;
    UNUSED(params);
    if (OB_UNLIKELY(NULL == stack_i))
    {
        TBSYS_LOG(WARN, "stack pointer is NULL.");
        err = OB_INVALID_ARGUMENT;
    }
    else if (OB_UNLIKELY(idx_i < 1))
    {
        TBSYS_LOG(WARN, "no enough operand in the stack. current size:%d", idx_i);
        err = OB_INVALID_ARGUMENT;
    }
    else
    {
        err = stack_i[idx_i - 1].days(result);
        if(err!=OB_SUCCESS)
        {
            TBSYS_LOG(USER_ERROR, "the days func param is not corrected");
        }
        idx_i -= 1;
    }
    return err;
}
/*add 20150828:e*/
/*add peiouy [datetime func] 20150912:b*/
static int days_in_month[]= {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31, 0};
static bool is_leap_year(int year)
{
    bool leap_year = false;
    if ((year & 3) == 0 && (year%100 || (year%400 == 0 && year)))
    {
        leap_year = true;
    }
    return leap_year;
}

static void calc_precisetime_except_microseconds(int64_t &src_time, ObInterval value, time_unit tm_unit, int64_t op_type)
{
    OB_ASSERT(days == tm_unit || hours == tm_unit || minutes == tm_unit || seconds == tm_unit);
    //mod hongchen [USEC_BUG_FIX] 20161118:b
    //int64_t microseconds = labs(src_time % 1000000);
    //int64_t src_time_without_microseconds = (src_time / 1000000) * 1000000;
    //int64_t middle_result = 0;
    //if (T_OP_ADD == op_type)
    //{
    //    middle_result = src_time_without_microseconds + value;
    //}
    //else
    //{
    //    middle_result = src_time_without_microseconds - value;
    //}
    if (T_OP_ADD == op_type)
    {
        src_time = src_time + value;
    }
    else
    {
        src_time = src_time - value;
    }

    /*
    if (0 <= middle_result)
    {
        src_time = middle_result + microseconds;
    }
    else
    {
        src_time = middle_result - microseconds;
    }
    */
    //mod hongchen [USEC_BUG_FIX] 20161118:e
}
static void calc_precisetime_with_microseconds(int64_t &src_time, ObInterval value, time_unit tm_unit, int64_t op_type)
{
    OB_ASSERT(microseconds == tm_unit);
    //mod hongchen [USEC_BUG_FIX] 20161118:b
    if (T_OP_ADD == op_type)
    {
        src_time = src_time + value;
    }
    else
    {
        src_time = src_time - value;
    }
    /*
    int64_t microseconds = labs(src_time % 1000000);
    int64_t src_time_without_microseconds = (src_time / 1000000) * 1000000;
    if (0 > src_time)
    {
        if ((0 <= value && T_OP_ADD == op_type)
                ||(0 >= value && T_OP_MINUS == op_type))
        {
            //not carry in
            if (1000000 > microseconds + labs(value))
            {
                src_time = src_time_without_microseconds - (labs(value) + microseconds);
            }
            //carry in
            else
            {
                int64_t middle_result = src_time_without_microseconds + ((labs(value) + microseconds) / 1000000) * 1000000;
                //result_time's sign and src_time's sign are not same
                if (0 <= middle_result)
                {
                    src_time = middle_result + (labs(value) + microseconds) % 1000000;
                }
                //result_time's sign and src_time's sign are same
                else
                {
                    src_time = middle_result - (labs(value) + microseconds) % 1000000;
                }
            }
        }
        else
        {
            //not carry out
            if (0 <= microseconds - labs(value))
            {
                src_time = src_time_without_microseconds - (microseconds - labs(value));
            }
            //carry out
            else
            {
                src_time = src_time_without_microseconds - 1000000 - (labs(value) / 1000000) * 1000000
                        + ((1000000 + microseconds - labs(value % 1000000)) / 1000000) * 1000000
                        - (1000000 + microseconds - labs(value % 1000000)) % 1000000;
            }
        }
    }
    else
    {
        if ((0 <= value && T_OP_ADD == op_type)
                ||(0 >= value && T_OP_MINUS == op_type))
        {
            //carry in , not carry in, same process
            src_time = src_time_without_microseconds + microseconds + labs(value);
        }
        else
        {
            //not carry out
            if (0 <= microseconds - labs(value))
            {
                src_time = src_time_without_microseconds + microseconds - labs(value);
            }
            //carry out
            else
            {
                int64_t middle_result = src_time_without_microseconds - 1000000 -(labs(value) / 1000000) * 1000000
                        + ((1000000 + microseconds - labs(value % 1000000))  / 1000000) * 1000000;
                //result_time's sign and src_time's sign are same
                if (0 <= middle_result)
                {
                    src_time = middle_result + (1000000 + microseconds - labs(value % 1000000)) % 1000000;
                }
                //result_time's sign and src_time's sign are not same
                else
                {
                    src_time = middle_result - (1000000 + microseconds - labs(value % 1000000)) % 1000000;
                }
            }
        }
    }
    */
    //mod hongchen [USEC_BUG_FIX] 20161118:e
}

//add peiouya [DATE_TIME] 20150914:b
static int type_skew_by_time_unit(ObExprObj& obj, ObInterval& value, time_unit tm_unit, int64_t op_type)
{
    int ret = OB_SUCCESS;
    switch(tm_unit)
    {
    case days:
        value -= value % (86400L * 1000000L);
        break;
    case hours:
        value -= value % (3600L * 1000000L);
        break;
    case minutes:
        value -= value % (60L * 1000000L);
        break;
    case seconds:
        value -= value % 1000000L;
        break;
    case microseconds:
        break;
    default:
        TBSYS_LOG(WARN, "time unit out of range.");
        ret = OB_ERROR;
    }
    if (OB_SUCCESS != ret)
    {
    }
    else if (ObNullType == obj.get_type ())
    {
    }
    else if (ObTimeType == obj.get_type ())
    {
        switch (tm_unit)
        {
        case hours:
        case minutes:
        case seconds:
            ret = OB_SUCCESS;
            break;
        default:
            TBSYS_LOG(WARN, "time artimatic only support hours, minutes, seconds.");
            ret = OB_OBJ_TYPE_ERROR;
        }
        if (OB_SUCCESS == ret)
        {
            value = value % (86400L * 1000000L);
        }
    }
    else if (ObDateType == obj.get_type ())
    {
        if (days == tm_unit)
        {
            ret = OB_SUCCESS;
        }
        else
        {
            TBSYS_LOG(WARN, "date artimatic not support hours, minutes, seconds, microseconds.");
            ret = OB_OBJ_TYPE_ERROR;
        }
    }
    else if (ObDateTimeType == obj.get_type ())
    {
        if (microseconds == tm_unit)
        {
            TBSYS_LOG(WARN, "datetime artimatic not support microseconds.");
            ret = OB_OBJ_TYPE_ERROR;
        }
    }
    else if (ObPreciseDateTimeType == obj.get_type ()
             || ObCreateTimeType == obj.get_type ()
             || ObModifyTimeType == obj.get_type ())
    {
        int64_t time = 0;
        switch (obj.get_type())
        {
        case ObPreciseDateTimeType:
            time = obj.get_precise_datetime();
            break;
        case ObCreateTimeType:
            time = obj.get_ctime();
            break;
        case ObModifyTimeType:
            time = obj.get_mtime();
            break;
        default:
            break;
        }
        //calc real value and set second add_para value with 0
        switch(tm_unit)
        {
        case days:
        case hours:
        case minutes:
        case seconds:
            calc_precisetime_except_microseconds(time, value, tm_unit, op_type);
            value = 0;
            break;
        case microseconds:
            calc_precisetime_with_microseconds(time, value, tm_unit, op_type);
            value = 0;
            break;
        default:
            break;
        }
        //set obj with real value
        switch (obj.get_type())
        {
        case ObPreciseDateTimeType:
            obj.set_precise_datetime(time);
            break;
        case ObModifyTimeType:
            obj.set_mtime(time);
            break;
        case ObCreateTimeType:
            obj.set_ctime(time);
            break;
        default:
            break;
        }
        ret = OB_SUCCESS;
    }
    else
    {
        TBSYS_LOG(WARN, "obj type is %d.", obj.get_type ());
        ret = OB_OBJ_TYPE_ERROR;
    }
    return ret;
}
//add 20150914:e
static int type_skew(ObExprObj& obj, int64_t& value, bool is_year)
{
    int ret = OB_SUCCESS;
    time_t time = 0;
    int microseconds = 0;  //add fix_calc_bug peiouya 20151026
    struct tm gtm;
    switch (obj.get_type())
    {
    case ObDateTimeType:
        time = static_cast<time_t>(obj.get_datetime());
        time *= 1000 * 1000L;
        break;
    case ObDateType:   //add peiouya [DATE_TIME] 20150914
    case ObPreciseDateTimeType:
        time = static_cast<time_t>(obj.get_precise_datetime());
        break;
    case ObModifyTimeType:
        time = static_cast<time_t>(obj.get_mtime());
        break;
    case ObCreateTimeType:
        time = static_cast<time_t>(obj.get_ctime());
        break;
    default:
        TBSYS_LOG(ERROR, "unexpected branch");
        ret = OB_OBJ_TYPE_ERROR;
    }
    if (OB_SUCCESS == ret)
    {
        microseconds = static_cast<int>(time % 1000000);  //add fix_calc_bug peiouya 20151026
        time = time / 1000000;
        //add hongchen [USEC_BUG_FIX] 20161118:b
        if (0 > microseconds) 
        {
          time = time - 1;
          microseconds = 1000000 + microseconds;
        }
        //add hongchen [USEC_BUG_FIX] 20161118:e
        localtime_r(&time, &gtm);
        if (is_year)
        {
            //year
            if ((1 <= gtm.tm_year + value + 1900) && (9999 >= gtm.tm_year + value + 1900))
            {
                gtm.tm_year += static_cast<int>(value);
                if ((2 == (gtm.tm_mon + 1)) && (29 == gtm.tm_mday) && !is_leap_year(static_cast<int>(gtm.tm_year + 1900)))
                {
                    gtm.tm_mday = 28;
                }
            }
            else
            {
                obj.set_null();
                ret = OB_VALUE_OUT_OF_DATES_RANGE; //add peiouya [DATE_TIME] 20150914
            }
        }
        else
        {
            //month
            int64_t period = (gtm.tm_year + 1900) * 12 + gtm.tm_mon + value;
            if (120000L <= period || 12L > period)
            {
                obj.set_null();
                ret = OB_VALUE_OUT_OF_DATES_RANGE; //add peiouya [DATE_TIME] 20150914
            }
            else
            {
                gtm.tm_year = static_cast<int>(period / 12) - 1900;
                gtm.tm_mon  = static_cast<int>(period % 12);
                if ( gtm.tm_mday > days_in_month[gtm.tm_mon])
                {
                    gtm.tm_mday = days_in_month[gtm.tm_mon];
                    if ((2 == (gtm.tm_mon + 1)) && is_leap_year(static_cast<int>(gtm.tm_year + 1900)))
                    {
                        gtm.tm_mday += 1;
                    }
                }
            }
        }
    }
    if (OB_SUCCESS == ret)
    {
        //mod fix_calc_bug peiouya 20151026:b
        //value = static_cast<int64_t>(mktime(&gtm) - time) * 1000000;
        int64_t skew_time = mktime(&gtm);
        //mod hongchen [USEC_BUG_FIX] 20161118:b
        /*
        if (0 <= skew_time)
        {
            skew_time = skew_time * 1000000 + abs(microseconds);
        }
        else
        {
            skew_time = skew_time * 1000000 - abs(microseconds);
        }
        */
        skew_time = skew_time * 1000000 + abs(microseconds);
        //mod hongchen [USEC_BUG_FIX] 20161118:e
        switch (obj.get_type())
        {
        case ObDateTimeType:
            obj.set_datetime(skew_time);
            break;
        case ObDateType:   //add peiouya [DATE_TIME] 20150914
            obj.set_date(skew_time);
            break;
        case ObPreciseDateTimeType:
            obj.set_precise_datetime(skew_time);
            break;
        case ObModifyTimeType:
            obj.set_mtime(skew_time);
            break;
        case ObCreateTimeType:
            obj.set_ctime(skew_time);
            break;
        default:
            break;
        }
        value = 0;
        //mod 20151026:end
    }
    return ret;
}
/* add 20150912:e*/
/* add wuna [datetime func] 20150909:b*/
int ObPostfixExpression::datetime_calc(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params,int64_t op_type)
{
    int err = OB_SUCCESS;
    if (OB_UNLIKELY(idx_i < 1))
    {
        TBSYS_LOG(WARN, "no enough operand in the stack. current size:%d", idx_i);
        err = OB_INVALID_ARGUMENT;
    }
    else if(stack_i[idx_i-2].get_type() != ObNullType
            && !stack_i[idx_i-2].is_datetime_for_out())//change
    {
        err = OB_ERROR;
        TBSYS_LOG(USER_ERROR,"param is invalid,must be date,time,timestamp.");
    }
    else
    {
        if (OB_LIKELY(OB_SUCCESS == (err = (call_func[op_type - T_MIN_OP - 1])(stack_i, idx_i, result, params))))
        {
        }
        else if (OB_NO_RESULT == err)
        {
            // nop
            err = OB_SUCCESS;
        }
        else
        {
            TBSYS_LOG(WARN, "call calculation datetime function error [value:%ld, idx_i:%d, err:%d]",
                      op_type, idx_i, err);
        }
    }
    return err;
}

int ObPostfixExpression::calc_year_month(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params, int64_t value ,bool is_year)
{
    int err = OB_SUCCESS;
    int64_t interval_value;
    if(OB_SUCCESS != (err = stack_i[idx_i-1].get_int(interval_value)))
    {
        if(stack_i[idx_i-1].get_type() == ObNullType)
        {
            err = OB_SUCCESS;
        }
        else if(stack_i[idx_i-1].get_type() == ObDecimalType)
        {
            err = OB_SUCCESS;
            int64_t cast_value;
            err = stack_i[idx_i-1].get_decimal().cast_to_int64(cast_value);
            if(err!= OB_SUCCESS)
            {
                TBSYS_LOG(WARN, "decimal cast_to_int64 failed.err=%d",err);
            }
            else
            {
                interval_value=cast_value;
            }
        }
        else
        {
            TBSYS_LOG(WARN, "get_int failed.err = %d.", err);
        }
    }
    if(OB_SUCCESS == err)
    {
        if(stack_i[idx_i-2].get_type() == ObNullType || stack_i[idx_i-1].get_type() == ObNullType)
        {
            result.set_null();
            idx_i -= 2;
        }
        else
        {
            if ( T_OP_MINUS == value)
            {
                interval_value = -interval_value;
            }
            if(OB_SUCCESS != (err = type_skew(stack_i[idx_i-2],interval_value,is_year)))
            {
                TBSYS_LOG(WARN, " type_skew failed.err = %d.", err);
            }
            else
            {
                if ( T_OP_MINUS == value)
                {
                    interval_value = -interval_value;
                }
                stack_i[idx_i-1].set_interval(static_cast<ObInterval>(interval_value));
                if(OB_SUCCESS != (err = datetime_calc(stack_i,idx_i,result,params,value)))
                {
                    TBSYS_LOG(ERROR,"calc calc_year_month func failed.ret = %d",err);
                }
            }
        }
    }
    return err;
}

int ObPostfixExpression::calc_day_hour_minute_second_microsecond(ObExprObj *stack_i, int &idx_i, ObExprObj &result,
                                                                 const ObPostExprExtraParams &params, int64_t value,time_unit tm_unit)
{
    int err=OB_SUCCESS;
    if(stack_i[idx_i-1].get_type() == ObNullType)
    {
    }
    else if(stack_i[idx_i-1].get_type() == ObDecimalType)
    {
        int64_t cast_value;
        err=stack_i[idx_i-1].get_decimal().cast_to_int64(cast_value);
        if(err!= OB_SUCCESS)
        {
            TBSYS_LOG(WARN, "decimal cast_to_int64 failed.err=%d",err);
        }
        else
        {
            stack_i[idx_i-1].set_interval(cast_value);
        }
    }
    else
    {
        stack_i[idx_i-1].set_interval(static_cast<ObInterval>(stack_i[idx_i-1].get_int()));
    }
    if(OB_SUCCESS == err)
    {
        ObInterval interval_value = static_cast<ObInterval>(stack_i[idx_i-1].get_precise_datetime());
        if(OB_SUCCESS != (err = type_skew_by_time_unit(stack_i[idx_i-2],interval_value,tm_unit, value)))
        {
            TBSYS_LOG(ERROR,"calc type_skew_by_time_unit failed.err=%d.",err);
        }
        else if(stack_i[idx_i-2].get_type() == ObNullType || stack_i[idx_i-1].get_type() == ObNullType)
        {
            result.set_null();
            idx_i -= 2;
        }
        else
        {
            stack_i[idx_i-1].set_interval(interval_value);
            if(OB_SUCCESS != (err = datetime_calc(stack_i,idx_i,result,params,value)))
            {
                TBSYS_LOG(ERROR,"calc sys_func_adddate func failed.ret = %d",err);
            }
        }
    }
    return err;
}

inline int ObPostfixExpression::sys_func_date_add_day(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params)
{
    int err = OB_SUCCESS;
    int64_t value = T_OP_ADD;
    if (OB_UNLIKELY(NULL == stack_i))
    {
        TBSYS_LOG(WARN, "stack pointer is NULL.");
        err = OB_INVALID_ARGUMENT;
    }
    else
    {
        if(OB_SUCCESS != (err = calc_day_hour_minute_second_microsecond(stack_i,idx_i,result,params,value,days)))
        {
            TBSYS_LOG(ERROR,"calc sys_func_date_add_day failed,err = %d",err);
        }

    }
    return err;
}

inline int ObPostfixExpression::sys_func_date_sub_day(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params)
{
    int err = OB_SUCCESS;
    int64_t value = T_OP_MINUS;
    if (OB_UNLIKELY(NULL == stack_i))
    {
        TBSYS_LOG(WARN, "stack pointer is NULL.");
        err = OB_INVALID_ARGUMENT;
    }
    else
    {
        if(OB_SUCCESS != (err = calc_day_hour_minute_second_microsecond(stack_i,idx_i,result,params,value,days)))
        {
            TBSYS_LOG(ERROR,"calc sys_func_date_sub_day failed,err = %d",err);
        }

    }
    return err;
}

inline int ObPostfixExpression::sys_func_date_add_hour(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params)
{
    int err = OB_SUCCESS;
    int64_t value = T_OP_ADD;
    if (OB_UNLIKELY(NULL == stack_i))
    {
        TBSYS_LOG(WARN, "stack pointer is NULL.");
        err = OB_INVALID_ARGUMENT;
    }
    else
    {
        if(OB_SUCCESS != (err = calc_day_hour_minute_second_microsecond(stack_i,idx_i,result,params,value,hours)))
        {
            TBSYS_LOG(ERROR,"calc sys_func_date_add_hour failed,err = %d",err);
        }

    }
    return err;
}

inline int ObPostfixExpression::sys_func_date_sub_hour(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params)
{
    int err = OB_SUCCESS;
    int64_t value = T_OP_MINUS;
    if (OB_UNLIKELY(NULL == stack_i))
    {
        TBSYS_LOG(WARN, "stack pointer is NULL.");
        err = OB_INVALID_ARGUMENT;
    }
    else
    {
        if(OB_SUCCESS != (err = calc_day_hour_minute_second_microsecond(stack_i,idx_i,result,params,value,hours)))
        {
            TBSYS_LOG(ERROR,"calc sys_func_date_sub_hour failed,err = %d",err);
        }

    }
    return err;
}

inline int ObPostfixExpression::sys_func_date_add_minute(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params)
{
    int err = OB_SUCCESS;
    int64_t value = T_OP_ADD;
    if (OB_UNLIKELY(NULL == stack_i))
    {
        TBSYS_LOG(WARN, "stack pointer is NULL.");
        err = OB_INVALID_ARGUMENT;
    }
    else
    {
        if(OB_SUCCESS != (err = calc_day_hour_minute_second_microsecond(stack_i,idx_i,result,params,value,minutes)))
        {
            TBSYS_LOG(ERROR,"calc sys_func_date_add_minute failed,err = %d",err);
        }

    }
    return err;
}

inline int ObPostfixExpression::sys_func_date_sub_minute(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params)
{
    int err = OB_SUCCESS;
    int64_t value = T_OP_MINUS;
    if (OB_UNLIKELY(NULL == stack_i))
    {
        TBSYS_LOG(WARN, "stack pointer is NULL.");
        err = OB_INVALID_ARGUMENT;
    }
    else
    {
        if(OB_SUCCESS != (err = calc_day_hour_minute_second_microsecond(stack_i,idx_i,result,params,value,minutes)))
        {
            TBSYS_LOG(ERROR,"calc sys_func_date_sub_minute failed,err = %d",err);
        }

    }
    return err;
}

inline int ObPostfixExpression::sys_func_date_add_second(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params)
{
    int err = OB_SUCCESS;
    int64_t value = T_OP_ADD;
    if (OB_UNLIKELY(NULL == stack_i))
    {
        TBSYS_LOG(WARN, "stack pointer is NULL.");
        err = OB_INVALID_ARGUMENT;
    }
    else
    {
        if(OB_SUCCESS != (err = calc_day_hour_minute_second_microsecond(stack_i,idx_i,result,params,value,seconds)))
        {
            TBSYS_LOG(ERROR,"calc sys_func_date_add_second failed,err = %d",err);
        }

    }
    return err;
}

inline int ObPostfixExpression::sys_func_date_sub_second(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params)
{
    int err = OB_SUCCESS;
    int64_t value = T_OP_MINUS;
    if (OB_UNLIKELY(NULL == stack_i))
    {
        TBSYS_LOG(WARN, "stack pointer is NULL.");
        err = OB_INVALID_ARGUMENT;
    }
    else
    {
        if(OB_SUCCESS != (err = calc_day_hour_minute_second_microsecond(stack_i,idx_i,result,params,value,seconds)))
        {
            TBSYS_LOG(ERROR,"calc sys_func_date_sub_second failed,err = %d",err);
        }

    }
    return err;
}

inline int ObPostfixExpression::sys_func_date_add_microsecond(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params)
{
    int err = OB_SUCCESS;
    int64_t value = T_OP_ADD;
    if (OB_UNLIKELY(NULL == stack_i))
    {
        TBSYS_LOG(WARN, "stack pointer is NULL.");
        err = OB_INVALID_ARGUMENT;
    }
    else
    {
        if(OB_SUCCESS != (err = calc_day_hour_minute_second_microsecond(stack_i,idx_i,result,params,value,microseconds)))
        {
            TBSYS_LOG(ERROR,"calc sys_func_date_add_microsecond failed,err = %d",err);
        }

    }
    return err;
}

inline int ObPostfixExpression::sys_func_date_sub_microsecond(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params)
{
    int err = OB_SUCCESS;
    int64_t value = T_OP_MINUS;
    if (OB_UNLIKELY(NULL == stack_i))
    {
        TBSYS_LOG(WARN, "stack pointer is NULL.");
        err = OB_INVALID_ARGUMENT;
    }
    else
    {
        if(OB_SUCCESS != (err = calc_day_hour_minute_second_microsecond(stack_i,idx_i,result,params,value,microseconds)))
        {
            TBSYS_LOG(ERROR,"calc sys_func_date_sub_microsecond failed,err = %d",err);
        }
    }
    return err;
}
inline int ObPostfixExpression::sys_func_date_add_year(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params)
{
    int err = OB_SUCCESS;
    int64_t value = T_OP_ADD;
    if (OB_UNLIKELY(NULL == stack_i))
    {
        TBSYS_LOG(WARN, "stack pointer is NULL.");
        err = OB_INVALID_ARGUMENT;
    }
    else
    {
        if(OB_SUCCESS != (err = calc_year_month(stack_i,idx_i,result,params,value,true)))
        {
            TBSYS_LOG(WARN, "sys_func_date_add_year failed.err = %d",err);
        }
    }
    return err;
}
inline int ObPostfixExpression::sys_func_date_sub_year(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params)
{
    int err = OB_SUCCESS;
    int64_t value = T_OP_MINUS;
    if (OB_UNLIKELY(NULL == stack_i))
    {
        TBSYS_LOG(WARN, "stack pointer is NULL.");
        err = OB_INVALID_ARGUMENT;
    }
    else
    {
        if(OB_SUCCESS != (err = calc_year_month(stack_i,idx_i,result,params,value,true)))
        {
            TBSYS_LOG(WARN, "sys_func_date_sub_year failed.err = %d",err);
        }
    }
    return err;
}
inline int ObPostfixExpression::sys_func_date_add_month(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params)
{
    int err = OB_SUCCESS;
    int64_t value = T_OP_ADD;
    if (OB_UNLIKELY(NULL == stack_i))
    {
        TBSYS_LOG(WARN, "stack pointer is NULL.");
        err = OB_INVALID_ARGUMENT;
    }
    else
    {
        if(OB_SUCCESS != (err = calc_year_month(stack_i,idx_i,result,params,value,false)))
        {
            TBSYS_LOG(WARN, "sys_func_date_add_month failed.err = %d",err);
        }
    }
    return err;
}
//add xionghui [floor and ceil] 20151214:b
inline int ObPostfixExpression::sys_func_floor(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params)
{
    int err = OB_SUCCESS;
    if (NULL == stack_i || NULL == params.str_buf_)
    {
        TBSYS_LOG(WARN, "stack_i=%p, str_buf_=%p.", stack_i, params.str_buf_);
        err = OB_INVALID_ARGUMENT;
    }
    else if (idx_i < 1)
    {
        TBSYS_LOG(WARN, "no enough operand in the stack. current size:%d", idx_i);
        err = OB_INVALID_ARGUMENT;
    }
    else
    {
        ObObjType type = stack_i[idx_i-1].get_type();
        int64_t tmp_value = 0;
        int32_t tmp_value_32 = 0;
        ObDecimal tmp_decimal,out;
        ObExprObj expr_to;

        float result_float=0.0f ,whole_float=0.0f;
        double whole_double = 0, result_double = 0;

        ObStringBuf mem_buf;
        switch(type)
        {
        case ObIntType:
            tmp_value = static_cast<int64_t>(stack_i[idx_i-1].get_int());
            break;
        case ObInt32Type:
            tmp_value_32 = static_cast<int32_t>(stack_i[idx_i-1].get_int32());
            break;
        case ObDecimalType:
            tmp_decimal= stack_i[idx_i-1].get_decimal();
            tmp_decimal.floor(out);
            break;
        case ObFloatType:
            whole_float = stack_i[idx_i-1].get_float();
            break;
        case ObDoubleType:
            whole_double = stack_i[idx_i-1].get_double();
            break;
        case ObVarcharType:
            if((err = stack_i[idx_i-1].cast_to(ObDecimalType, expr_to, mem_buf)) != OB_SUCCESS)
            {
                TBSYS_LOG(ERROR, "varchar cast failed ret is %d",err);
            }
            else if(expr_to.is_null())
            {
                TBSYS_LOG(USER_ERROR, "Invalid character found in a character string argument");
                err = OB_INVALID_ARGUMENT;
            }else
            {
                tmp_decimal = expr_to.get_decimal();
                tmp_decimal.floor(out);
            }
            break;
            //xionghui [floor ceil round NULL bug] 20160222 b:
           case ObNullType:
               break;
            //xionghui e:
        default:
            TBSYS_LOG(USER_ERROR, "No authorized routine named FLOOR of type FUNCTION having compatible arguments was found type is %d", type);
            err = OB_INVALID_ARGUMENT;
            break;
        }
        if(err == OB_SUCCESS )
        {
            if(type == ObNullType)
            {
                result.set_null();
            }
            else if(type == ObVarcharType || type == ObDecimalType)
            {
                result.set_decimal(out);
            }
            else if(type == ObDoubleType)
            {
                result_double = floor(whole_double);
                result.set_double(result_double);

            }else if(type == ObFloatType)
            {
                result_float = (float)floor((double)whole_float);
                result.set_float(result_float);

            }else if(type == ObInt32Type)
            {
                result.set_int32(tmp_value_32);
            }
            else
            {
                result.set_int(tmp_value);
            }
        }
        idx_i -= 1;
        mem_buf.clear();
    }

    return err;
}

inline int ObPostfixExpression::sys_func_ceil(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params)
{
    int err = OB_SUCCESS;
    if (NULL == stack_i || NULL == params.str_buf_)
    {
        TBSYS_LOG(WARN, "stack_i=%p, str_buf_=%p.", stack_i, params.str_buf_);
        err = OB_INVALID_ARGUMENT;
    }
    else if (idx_i < 1)
    {
        TBSYS_LOG(WARN, "no enough operand in the stack. current size:%d", idx_i);
        err = OB_INVALID_ARGUMENT;
    }
    else
    {
        ObObjType type = stack_i[idx_i-1].get_type();
        int64_t tmp_value = 0;
        int32_t tmp_value_32 = 0;
        ObDecimal tmp_decimal,out;
        ObExprObj expr_to;

        float result_float=0.0f ,whole_float=0.0f ;
        double result_double = 0 ,whole_double = 0 ;

        ObStringBuf mem_buf;
        switch(type)
        {
        case ObIntType:
            tmp_value = static_cast<int64_t>(stack_i[idx_i-1].get_int());
            break;
        case ObInt32Type:
            tmp_value_32 = static_cast<int32_t>(stack_i[idx_i-1].get_int32());
            break;
        case ObDecimalType:
            tmp_decimal= stack_i[idx_i-1].get_decimal();
            tmp_decimal.ceil(out);
            break;
        case ObFloatType:
            whole_float = stack_i[idx_i-1].get_float();
            break;
        case ObDoubleType:
            whole_double = stack_i[idx_i-1].get_double();
            break;
        case ObVarcharType:

            if((err = stack_i[idx_i-1].cast_to(ObDecimalType, expr_to, mem_buf)) != OB_SUCCESS)
            {
                TBSYS_LOG(ERROR, "varchar cast failed ret is %d",err);
            }
            else if(expr_to.is_null())
            {
                TBSYS_LOG(USER_ERROR, "Invalid character found in a character string argument");
                err = OB_INVALID_ARGUMENT;
            }else
            {
                tmp_decimal = expr_to.get_decimal();
                tmp_decimal.ceil(out);
            }
            break;
            //xionghui [floor ceil round NULL bug] 20160222 b:
           case ObNullType:
               break;
            //xionghui e:
        default:
            TBSYS_LOG(USER_ERROR, "No authorized routine named CEIL of type FUNCTION having compatible arguments was found type is %d", type);
            err = OB_INVALID_ARGUMENT;
            break;
        }
        if(err == OB_SUCCESS )
        {
            if(type == ObNullType)
            {
                result.set_null();
            }
            else if(type == ObVarcharType || type == ObDecimalType)
            {
                result.set_decimal(out);
            }
            else if(type == ObDoubleType)
            {
                result_double = ceil(whole_double);
                result.set_double(result_double);

            }else if(type == ObFloatType)
            {
                result_float = (float)ceil((double)whole_float);
                result.set_float(result_float);

            }else if(type == ObInt32Type)
            {
                result.set_int32(tmp_value_32);
            }
            else
            {
                result.set_int(tmp_value);
            }
        }
        idx_i -= 1;
        mem_buf.clear();
    }
    return err;
}
//add 20151214:e
//add xionghui [round] 20150126:b
inline int ObPostfixExpression::sys_func_round(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params)
{
    int err = OB_SUCCESS;
    if (NULL == stack_i || NULL == params.str_buf_)
    {
        TBSYS_LOG(WARN, "stack_i=%p, str_buf_=%p.", stack_i, params.str_buf_);
        err = OB_INVALID_ARGUMENT;
    }
    else if (idx_i < 2)
    {
        TBSYS_LOG(WARN, "no enough operand in the stack. current size:%d", idx_i);
        err = OB_INVALID_ARGUMENT;
    }
    else
    {
        //make sure the num_digits is int
        int64_t num_digits = stack_i[idx_i-1].get_int();
        ObObjType type1 = stack_i[idx_i-1].get_type();
        if(type1 == ObIntType || type1 == ObInt32Type)
        {
            ObObjType type2 = stack_i[idx_i-2].get_type();
            ObDecimal tmp_decimal,out;
            ObExprObj expr_to;
            ObExprObj expr_tmp;
            ObStringBuf mem_buf;
            switch(type2)
            {
            case ObIntType:
            case ObInt32Type:
            case ObFloatType:
            case ObDoubleType:
                if (OB_SUCCESS != (err = stack_i[idx_i-2].cast_to(ObVarcharType, expr_tmp, mem_buf)))
                {
                    err = OB_INVALID_ARGUMENT;
                    TBSYS_LOG(ERROR, "varchar cast failed ret is %d",err);
                }
                if((err = expr_tmp.cast_to(ObDecimalType, expr_to, mem_buf)) != OB_SUCCESS)
                {
                    TBSYS_LOG(ERROR, "varchar cast failed ret is %d",err);
                }
                else if(expr_to.is_null())
                {
                    TBSYS_LOG(USER_ERROR, "Invalid character found in a character string argument");
                    err = OB_INVALID_ARGUMENT;
                }
                else
                {
                    tmp_decimal = expr_to.get_decimal();
                    tmp_decimal.round(out,num_digits);
                }
                break;
            case ObDecimalType:
                tmp_decimal= stack_i[idx_i-2].get_decimal();
                tmp_decimal.round(out,num_digits);
                break;
             //xionghui [floor ceil round NULL bug] 20160222 b:
            case ObNullType:
                break;
             //xionghui e:
            default:
                TBSYS_LOG(USER_ERROR, "the first argument is invalid type is %d", type2);
                err = OB_INVALID_ARGUMENT;
                break;
            }
            mem_buf.clear();
            if(err == OB_SUCCESS)
            {
                 //mod xionghui [floor ceil round NULL bug] 20160222 b:
                if(type2 == ObNullType)
                {
                    result.set_null();
                }
                else
                {
                    result.set_decimal(out);
                }
                //xionghui e:
            }
        }
        else
        {
            TBSYS_LOG(USER_ERROR, "the second argument is invalid type is %d", type1);
            err = OB_INVALID_ARGUMENT;
        }
        idx_i -= 2;
    }
    return err;
}
//add 20150126 e:


inline int ObPostfixExpression::sys_func_date_sub_month(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params)
{
    int err = OB_SUCCESS;
    int64_t value = T_OP_MINUS;
    if (OB_UNLIKELY(NULL == stack_i))
    {
        TBSYS_LOG(WARN, "stack pointer is NULL.");
        err = OB_INVALID_ARGUMENT;
    }
    else
    {
        if(OB_SUCCESS != (err = calc_year_month(stack_i,idx_i,result,params,value,false)))
        {
            TBSYS_LOG(WARN, "sys_func_date_sub_month failed.err = %d",err);
        }
    }
    return err;
}
/* add 20150909:e*/
//add liuzy [datetime func] 20150901:b
/*Exp: add system function "hour()"*/
inline int ObPostfixExpression::sys_func_hour(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params)
{
    int err = OB_SUCCESS;
    UNUSED(params);
    if (OB_UNLIKELY(NULL == stack_i))
    {
        TBSYS_LOG(WARN, "stack pointer is NULL.");
        err = OB_INVALID_ARGUMENT;
    }
    else if (OB_UNLIKELY(idx_i < 1))
    {
        TBSYS_LOG(WARN, "no enough operand in the stack. current size:%d", idx_i);
        err = OB_INVALID_ARGUMENT;
    }
    else
    {
        err = stack_i[idx_i - 1].hour(result);
        if(err != OB_SUCCESS)
        {
            TBSYS_LOG(USER_ERROR, "the hour func param is not corrected");
        }
        idx_i -= 1;
    }
    return err;
}
/*Exp: add system function "minute()"*/
inline int ObPostfixExpression::sys_func_minute(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params)
{
    int err = OB_SUCCESS;
    UNUSED(params);
    if (OB_UNLIKELY(NULL == stack_i))
    {
        TBSYS_LOG(WARN, "stack pointer is NULL.");
        err = OB_INVALID_ARGUMENT;
    }
    else if (OB_UNLIKELY(idx_i < 1))
    {
        TBSYS_LOG(WARN, "no enough operand in the stack. current size:%d", idx_i);
        err = OB_INVALID_ARGUMENT;
    }
    else
    {
        err = stack_i[idx_i - 1].minute(result);
        if(err != OB_SUCCESS)
        {
            TBSYS_LOG(USER_ERROR, "the minute func param is not corrected");
        }
        idx_i -= 1;
    }
    return err;
}
/*Exp: add system function "second()"*/
inline int ObPostfixExpression::sys_func_second(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params)
{
    int err = OB_SUCCESS;
    UNUSED(params);
    if (OB_UNLIKELY(NULL == stack_i))
    {
        TBSYS_LOG(WARN, "stack pointer is NULL.");
        err = OB_INVALID_ARGUMENT;
    }
    else if (OB_UNLIKELY(idx_i < 1))
    {
        TBSYS_LOG(WARN, "no enough operand in the stack. current size:%d", idx_i);
        err = OB_INVALID_ARGUMENT;
    }
    else
    {
        err = stack_i[idx_i - 1].second(result);
        if(err != OB_SUCCESS)
        {
            TBSYS_LOG(USER_ERROR, "the second func param is not corrected");
        }
        idx_i -= 1;
    }
    return err;
}
//add 20150901:e
//add lijianqiang [replace_fuction] 20151102:b
/*Exp: function for "REPLACE(-source_str-,-searching_str-,-replace_str-)"*/
inline int ObPostfixExpression::sys_func_replace(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params)
{
    int err = OB_SUCCESS;
    if (OB_UNLIKELY(NULL == stack_i || NULL == params.str_buf_))
    {
        TBSYS_LOG(WARN, "stack pointer=%p, str_buf_=%p", stack_i, params.str_buf_);
        err = OB_INVALID_ARGUMENT;
    }
    else if (idx_i < 2 || params.operand_count_ < 2)
    {
        TBSYS_LOG(WARN, "no enough operand in the stack. current size:%d, current op count:%d", idx_i, params.operand_count_);
        err = OB_INVALID_ARGUMENT;
    }
    else
    {
        ObExprObj source_obj, search_obj, replace_obj;
        ObString source_str, search_str, replace_str;
        const char *ptr, *end, *strend, *search, *search_end;
        int32_t from_length,to_length;
        int32_t offset;
        ObObj temp_result;
        bool return_source_no_change = false;
        //cast to varchar
        if (2 == params.operand_count_)
        {
            if (OB_SUCCESS != (err = stack_i[idx_i-2].cast_to(ObVarcharType, source_obj, *params.str_buf_)))
            {
                TBSYS_LOG(WARN, "cast source string to varchar failed, err=[%d]",err);
            }
            else if (OB_SUCCESS != (err = stack_i[idx_i-1].cast_to(ObVarcharType, search_obj, *params.str_buf_)))
            {
                TBSYS_LOG(WARN, "cast search string to varchar failed, err=[%d]",err);
            }
        }
        else if (3 == params.operand_count_)
        {
            if (OB_SUCCESS != (err = stack_i[idx_i-3].cast_to(ObVarcharType, source_obj, *params.str_buf_)))
            {
                TBSYS_LOG(WARN, "cast source string to varchar failed, err=[%d]",err);
            }
            else if (OB_SUCCESS != (err = stack_i[idx_i-2].cast_to(ObVarcharType, search_obj, *params.str_buf_)))
            {
                TBSYS_LOG(WARN, "cast search string to varchar failed, err=[%d]",err);
            }
            else if (OB_SUCCESS != (err = stack_i[idx_i-1].cast_to(ObVarcharType, replace_obj, *params.str_buf_)))
            {
                TBSYS_LOG(WARN, "cast repalce string to varchar failed, err=[%d]",err);
            }
        }
        else//other
        {
            err = OB_ERR_PARAM_SIZE;
            TBSYS_LOG(USER_ERROR, "the replace func param is not corrected, ret=[%d]",err);
        }
        if (OB_SUCCESS == err && ( source_obj.is_null() || search_obj.is_null() || (replace_obj.is_null() && (3 == params.operand_count_))))
        {
            if (search_obj.is_null() || (replace_obj.is_null() && (3 == params.operand_count_)))
            {
                err = OB_ERROR;
                TBSYS_LOG(USER_ERROR, "last two params of repalce function can't null");
            }
            else if (source_obj.is_null())
            {
                err = OB_ERR_VARIABLE_NULL;
            }
        }
        else
        {
            source_str = source_obj.get_varchar();
            search_str = search_obj.get_varchar();
            if (2 == params.operand_count_)
            {
                replace_str = ObString::make_string("");
            }
            else
            {
                replace_str = replace_obj.get_varchar();
            }
            //        TBSYS_LOG(ERROR,"the soruce is[%.*s],the search is[%.*s], the replace is[%.*s]",
            //                  source_str.length(),source_str.ptr(),
            //                  search_str.length(),search_str.ptr(),
            //                  replace_str.length(),replace_str.ptr());
            if (search_str.length() == 0)
            {
                //return the source str
                return_source_no_change = true;
            }
            temp_result.set_varchar(source_str);//str or the original obj
            std::string res,res1,res2;
            res.assign(source_str.ptr(),source_str.length());
            res1.assign(search_str.ptr(),search_str.length());
            res2.assign(replace_str.ptr(),replace_str.length());
            if (err == OB_SUCCESS && !return_source_no_change)
            {
                if (-1 == (offset = (int32_t)res.find(res1.data())))//-1 if not find
                {
                    //return the source str
                    return_source_no_change = true;
                }
                else//find
                {
                    from_length = (int32_t)res1.length();//search string length
                    to_length = (int32_t)res2.length();// replace string length
                    search = res1.c_str();
                    search_end = search + from_length;
                    strend = res.c_str() + res.length();
                    end = strend ? strend - from_length + 1 : NULL;//the source str may be null,check here
                    ptr = res.c_str() + offset;
                    while (true)
                    {
                        while (ptr < end)
                        {
                            if (*ptr == *search)
                            {
                                char *i,*j;
                                i = (char*) ptr + 1; j = (char*) search + 1;
								int32_t match_length = 1;
                                while (j != search_end)
                                {
                                    TBSYS_LOG(ERROR,"not search end");
                                    if (*i++ != *j++)
                                    {
                                        ptr ++;
                                        break;//for next char
                                    }
									match_length++;
                                }
                                //if (j == search_end)//maching success,can repalce current search string
                                if (match_length == from_length)
								{
                                    offset = (int32_t)(ptr - res.c_str());
                                    res.replace(offset, from_length, res2);
                                    offset += to_length;
                                    ptr = res.c_str() + offset;
                                    end = res.c_str() + res.length() - from_length + 1;
                                    break;//for next sub str
                                }
                            }
                            else
                            {
                                ptr ++;
                            }
                        }//end while
                        if (ptr >= end)
                        {
                            break;
                        }
                    }//end while (true)
                }//end else
            }
            //replace over,return result need cast?
            if (!return_source_no_change)
            {
                ObString assign_string;
                assign_string.assign(const_cast<char*>(res.c_str()),static_cast<int32_t>(res.length()));
                if (OB_UNLIKELY(OB_SUCCESS != (err = params.str_buf_->write_string(assign_string, &source_str))))
                {
                    TBSYS_LOG(WARN, "fail to allocate memory for string. ret=%d", err);
                }
                temp_result.set_varchar(source_str);
                //          TBSYS_LOG(ERROR,"the final result is[%.*s]",source_str.length(),source_str.ptr());
            }
            result.assign(temp_result);
        }
        if (OB_ERR_VARIABLE_NULL == err)
        {
            result.set_null();
            err = OB_SUCCESS;
        }
        idx_i -= params.operand_count_;
    }
    return err;
}
//add 20151102:e

//add  peiouya [EXPIRE_INFO_NOT_ARITHMETIC_FIX] 20160608:b
inline int ObPostfixExpression::cast_thin_func(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params)
{
  UNUSED(params);
  int err = OB_SUCCESS;
  int64_t dest_type = 0;
  if (OB_UNLIKELY(NULL == stack_i))
  {
      TBSYS_LOG(WARN, "stack_i=%p.", stack_i);
      err = OB_INVALID_ARGUMENT;
  }
  else if (OB_UNLIKELY(idx_i < 2))
  {
      TBSYS_LOG(WARN, "no enough operand in the stack. current size:%d", idx_i);
      err = OB_INVALID_ARGUMENT;
  }
  else if (OB_SUCCESS != (err = stack_i[idx_i-1].get_int(dest_type)))
  {
      TBSYS_LOG(WARN, "fail to get int value. actual type = %d. err=%d", stack_i[idx_i-1].get_type(), err);
  }
  else if (ObIntType  != dest_type)
  {
    err = OB_NOT_SUPPORTED;
    TBSYS_LOG(ERROR,"cast_thin_func only support cast to ObIntType. but cur cast to type:%ld",dest_type);
  }
  else // if (ObIntType  == dest_type)
  {
    ObStringBuf tmp_str_buf;  //only for input-para, actually not to use

    if(ObVarcharType == stack_i[idx_i-2].get_type ())
    {
       err = stack_i[idx_i-2].cast_to(ObPreciseDateTimeType, result, tmp_str_buf);
    }

    if(OB_SUCCESS == err && ObVarcharType == stack_i[idx_i-2].get_type ())
    {
      result.set_int (result.get_precise_datetime ());
    }
    else if (OB_SUCCESS != (err = stack_i[idx_i-2].cast_to(ObIntType, result, tmp_str_buf)))
    {
      TBSYS_LOG(DEBUG,"fail to cast type:%d--->ObIntType, but we ignore it, along with set result with null!",
                stack_i[idx_i-2].get_type ());
      result.set_null ();
      err = OB_SUCCESS;
    }
    else
    {
      //nothing todo
    }
    idx_i -= 2;
  }

  return err;
}
//add  peiouya [EXPIRE_INFO_NOT_ARITHMETIC_FIX] 20160608:e

inline int ObPostfixExpression::concat_func(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params)
{
    int err = OB_SUCCESS;
    if (OB_UNLIKELY(NULL == stack_i || NULL == params.str_buf_))
    {
        TBSYS_LOG(WARN, "stack pointer=%p, str_buf_=%p", stack_i, params.str_buf_);
        err = OB_INVALID_ARGUMENT;
    }
    else if (idx_i < 2)
    {
        TBSYS_LOG(WARN, "no enough operand in the stack. current size:%d", idx_i);
        err = OB_INVALID_ARGUMENT;
    }
    else
    {
        if (OB_SUCCESS != (err = stack_i[idx_i-2].concat(stack_i[idx_i-1], result, *params.str_buf_)))
        {
            err = OB_SUCCESS;
            result.set_null();
        }
        idx_i -= 2;
    }
    return err;
}
//add wanglei [last_day] 20160311:b
inline int ObPostfixExpression::sys_func_last_day(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params)
{
    int err = OB_SUCCESS;
    UNUSED(params);
    ObExprObj expr_tmp;
    ObStringBuf mem_buf;
    if (OB_UNLIKELY(NULL == stack_i))
    {
        TBSYS_LOG(WARN, "stack pointer is NULL.");
        err = OB_INVALID_ARGUMENT;
    }
    else if (OB_UNLIKELY(idx_i < 1))
    {
        TBSYS_LOG(WARN, "no enough operand in the stack. current size:%d", idx_i);
        err = OB_INVALID_ARGUMENT;
    }
    else if ( stack_i[idx_i - 1].get_type() == ObDateType||
              stack_i[idx_i - 1].get_type() == ObDateTimeType||
              stack_i[idx_i - 1].get_type() == ObTimeType||
              stack_i[idx_i - 1].get_type() == ObCreateTimeType||
              stack_i[idx_i - 1].get_type() == ObModifyTimeType||
              stack_i[idx_i - 1].get_type() == ObPreciseDateTimeType)
    {
        expr_tmp = stack_i[idx_i - 1];
    }
    else
    {
        err = stack_i[idx_i - 1].cast_to(ObDateType, expr_tmp, mem_buf);
    }
    if(err != OB_SUCCESS)
    {
        TBSYS_LOG(WARN, "no support type!");
    }
    else
    {
        struct tm gtm;
        memset(&gtm,0,sizeof(gtm));
        time_t t = static_cast<time_t>(expr_tmp.get_precise_datetime() / 1000000L);
        localtime_r(&t, &gtm);
        if(gtm.tm_mon==0)
        {
            gtm.tm_year = gtm.tm_year - 1;
            gtm.tm_mon = 11;
        }
        else
        {
            gtm.tm_mon =  gtm.tm_mon -1;
        }
        //leap year skew
        if (is_leap_year(static_cast<int>(gtm.tm_year + 1900)) && (2 == ( gtm.tm_mon + 1)))
        {
            gtm.tm_mday = 29;
        }
        else
        {
            gtm.tm_mday = days_in_month[gtm.tm_mon];
        }
        //calculate result and set out_result
        time_t t_t = mktime(&gtm);
        result.set_date(static_cast<ObDate>(static_cast<int64_t>(t_t) * 1000000L));
        idx_i -= 1;
    }
    return err;
}
//add wanglei:e
//add wanglei [to_char] 20160311:b
inline int ObPostfixExpression::sys_func_to_char(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params)
{
    int ret = OB_SUCCESS;
    char *date_user_temp = (char*)malloc(sizeof(char)*40);
    if (NULL == stack_i)
    {
        TBSYS_LOG(WARN, "stack pointer is NULL.");
        ret = OB_INVALID_ARGUMENT;
    }
    else if (params.operand_count_ < 1 || idx_i < params.operand_count_)
    {
        TBSYS_LOG(USER_ERROR, "no enough operand in the stack. current size:%d. expect more than:%d", idx_i, params.operand_count_);
        ret = OB_INVALID_ARGUMENT;
    }
    else
    {
        ObObjType obj_type = stack_i[idx_i-1].get_type ();
        ObExprObj promoted_obj;
        ObString obstr_temp;
        obstr_temp.assign_ptr (date_user_temp,40);
        promoted_obj.set_varchar (obstr_temp);
        if(obj_type== ObPreciseDateTimeType)
        {
            ObObjCastParams params_temp;
            if (OB_SUCCESS != (ret = OB_OBJ_CAST[ObPreciseDateTimeType][ObVarcharType](params_temp, stack_i[idx_i-1], promoted_obj)))
            {
                TBSYS_LOG(WARN, "failed to cast object");
            }
            if(ret == OB_SUCCESS)
                result = promoted_obj;
        }else if(obj_type== ObDateType )
        {
            ObObjCastParams params_temp;
            if (OB_SUCCESS != (ret = OB_OBJ_CAST[ObDateType][ObVarcharType](params_temp, stack_i[idx_i-1], promoted_obj)))
            {
                TBSYS_LOG(WARN, "failed to cast object");
            }
            if(ret == OB_SUCCESS)
                result = promoted_obj;
        }
        else if(obj_type== ObDateTimeType )
        {
            ObObjCastParams params_temp;
            if (OB_SUCCESS != (ret = OB_OBJ_CAST[ObDateTimeType][ObVarcharType](params_temp, stack_i[idx_i-1], promoted_obj)))
            {
                TBSYS_LOG(WARN, "failed to cast object");
            }
            if(ret == OB_SUCCESS)
                result = promoted_obj;

        }else if(obj_type== ObCreateTimeType)
        {
            ObObjCastParams params_temp;
            if (OB_SUCCESS != (ret = OB_OBJ_CAST[ObCreateTimeType][ObVarcharType](params_temp, stack_i[idx_i-1], promoted_obj)))
            {
                TBSYS_LOG(WARN, "failed to cast object");
            }
            if(ret == OB_SUCCESS)
                result = promoted_obj;
        }
        else if(obj_type== ObModifyTimeType)
        {
            ObObjCastParams params_temp;
            if (OB_SUCCESS != (ret = OB_OBJ_CAST[ObModifyTimeType][ObVarcharType](params_temp, stack_i[idx_i-1], promoted_obj)))
            {
                TBSYS_LOG(WARN, "failed to cast object");
            }
            if(ret == OB_SUCCESS)
                result = promoted_obj;
        }
        else if(obj_type == ObVarcharType)
        {

            result = stack_i[idx_i-1];
        }
        else
        {
            TBSYS_LOG(USER_ERROR, "no support type!");
            ret = OB_INVALID_ARGUMENT;
        }
    }
    if (OB_SUCCESS == ret)
    {
        idx_i -= params.operand_count_;
    }
    return ret;
}
//add wanglei:e
//add wanglei [to_date] 20160311:b
inline int ObPostfixExpression::date_printf(char * date_buff, const char* format, ...)
{
    int length = 0;
    va_list args;
    va_start(args, format);
    length = vsnprintf(date_buff, 11, format, args);
    va_end(args);
    return length;
}
int ObPostfixExpression::parse_date(char * date,char * date_str,int date_length,int date_str_length,ObExprObj &ob_date)
{
    UNUSED(date_length);
    UNUSED(date_str_length);
    int ret = OB_SUCCESS;
    int unexpected_type = TIME_CHECK;
    struct tm gtm,gtm_check;
    memset(&gtm,0,sizeof(gtm));
    const int REGEX_NUM = 8;
    const char *regex_list[REGEX_NUM] = {
        "yyyy-mm-dd",
        "dd-mon-yy",
        "yyyy-mm-dd hh24:mi:ss",
        "dd-mm-yy",
        "dd-mm-yyyy hh24:mi:ss",
        "dd-mon-rr",
        "dd/mm/yyyy hh24:mi:ss",
        "DD-MON-RR"
    };
    int index = -1;
    char year[10];
    char month[10];
    char day[10];
    memset(year,'\0',sizeof(year));
    memset(month,'\0',sizeof(month));
    memset(day,'\0',sizeof(day));
    for(int i=0;i<date_str_length;i++)
    {
        date_str[i] = (char)tolower(date_str[i]);
    }
    for(int i=0;i<REGEX_NUM;i++)
    {
        if(strcmp(regex_list[i],date_str)==0)
        {
            index = i;
            break;
        }
    }
    int split[2] ={0,0};
    for(int i=0,j=0;i<date_length&&j<2;i++){if(date[i] == '-' || date[i] == '/'){split[j]= i;j++;}}
    switch(index)
    {
    case -1: {
        ret = OB_INVALID_ARGUMENT;
        TBSYS_LOG(USER_ERROR,"not suport format type!");
    }break;
    case 2:
    {
        for(int i=0;i<split[0];i++){year[i] = date[i];}
        for(int i=split[0]+1,j=0;i<split[1];i++,j++){ month[j] = date[i];}
        for(int i=split[1]+1,j=0;i<date_length;i++,j++){ day[j] = date[i];}
      }break;
    case 3:{
        for(int i=0;i<split[0];i++){day[i] = date[i];}
        for(int i=split[0]+1,j=0;i<split[1];i++,j++){ month[j] = date[i];}
        for(int i=split[1]+1,j=0;i<date_length;i++,j++){ year[j] = date[i];}
       }break;
    case 6:
    case 4:
    {
        for(int i=0;i<split[0];i++){day[i] = date[i];}
        for(int i=split[0]+1,j=0;i<split[1];i++,j++){ month[j] = date[i];}
        for(int i=split[1]+1,j=0;i<date_length;i++,j++){ year[j] = date[i];}
       }break;
    case 0:{
        for(int i=0;i<split[0];i++){year[i] = date[i];}
        for(int i=split[0]+1,j=0;i<split[1];i++,j++){ month[j] = date[i];}
        for(int i=split[1]+1,j=0;i<date_length;i++,j++){ day[j] = date[i];}
       }break;
    case 7:
    case 5:
    case 1:{
        int index_month = 0;
        bool is_has_month = false;
        for(int j=0;j<date_length;j++)
        {
            char temp[4];
            memset(temp,0,sizeof(temp));
            temp[0] = date[j];
            temp[1] = date[++j];
            temp[2] = date[++j];
            if(strcmp(temp,"月")==0)
            {
                is_has_month = true;
                index_month = j-2;
            }
            j=j-2;
        }
        if(is_has_month)
        {
            for(int i=0;i<split[0];i++)
            {
                day[i] = date[i];
            }
            for(int i=split[0]+1,j=0;i<index_month;i++,j++)
            {
                month[j] = date[i];
            }
            for(int i=split[1]+1,j=0;i<date_length;i++,j++)
            {
                year[j] = date[i];
            }
        }
        else
        {
            for(int i=0;i<split[0];i++)
            {
                day[i] = date[i];
            }
            for(int i=split[0]+1,j=0;i<split[1];i++,j++)
            {
                month[j] = (char)tolower(date[i]);
            }
            for(int i=split[1]+1,j=0;i<date_length;i++,j++)
            {
                year[j] = date[i];
            }
        }
    }break;
    default:
    {
        ret = OB_INVALID_ARGUMENT;
        TBSYS_LOG(USER_ERROR,"not suport format type!");
    }break;
    }
    if(ret == OB_SUCCESS)
    {
        //int length_t = 0;
        int year_int = 0;
        if(atoi(year)<1000)
            year_int = atoi(year) + 2000;
        else
            year_int = atoi(year);

        gtm.tm_year = year_int-1900;
        if(index == 1 || index == 5 || index == 7)
        {
            if(strcmp(month,"jan")==0)
            {
                gtm.tm_mon = 1-1;
            }
            else if(strcmp(month,"feb")==0)
            {
                gtm.tm_mon = 2-1;
            }
            else if(strcmp(month,"mar")==0)
            {
                gtm.tm_mon = 3-1;
            }
            else if(strcmp(month,"apr")==0)
            {
                gtm.tm_mon = 4-1;
            }
            else if(strcmp(month,"may")==0)
            {
                gtm.tm_mon = 5-1;
            }
            else if(strcmp(month,"jun")==0)
            {
                gtm.tm_mon = 6-1;
            }
            else if(strcmp(month,"jul")==0)
            {
                gtm.tm_mon = 7-1;
            }
            else if(strcmp(month,"aug")==0)
            {
                gtm.tm_mon = 8-1;
            }
            else if(strcmp(month,"sep")==0)
            {
                gtm.tm_mon = 9-1;
            }
            else if(strcmp(month,"oct")==0)
            {
                gtm.tm_mon = 10-1;
            }
            else if(strcmp(month,"nov")==0)
            {
                gtm.tm_mon = 11-1;
            }
            else if(strcmp(month,"dec")==0)
            {
                gtm.tm_mon = 12-1;
            }
            else
            {
                ret = OB_INVALID_ARGUMENT;
                TBSYS_LOG(USER_ERROR,"the date param is not corrected");
            }
        }
        else
          gtm.tm_mon = atoi(month) -1;
        if(ret == OB_SUCCESS)
        {
            gtm.tm_mday = atoi(day);
            time_t t_t = mktime(&gtm);
            ob_date.set_date(static_cast<ObDate>(static_cast<int64_t>(t_t) * 1000000L));
            if(OB_SUCCESS != (ret = ob_date.get_gtm(gtm_check, unexpected_type)))
            {
                TBSYS_LOG(USER_ERROR,"the date param is not corrected");
            }
        }
    }
    return ret;
}
inline int ObPostfixExpression::sys_func_to_date(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params)
{
    int ret = OB_SUCCESS;
    int regex_length = 0;
    int date_length = 0;
    char date_regex[40] ;
    char date_user[40] ;
    memset(date_regex,'\0',sizeof(date_regex));
    memset(date_user,'\0',sizeof(date_user));
    if (NULL == stack_i)
    {

        TBSYS_LOG(WARN, "stack pointer is NULL.");
        ret = OB_INVALID_ARGUMENT;
    }
    else if (params.operand_count_ < 1 || idx_i < params.operand_count_)
    {

        TBSYS_LOG(USER_ERROR, "no enough operand in the stack. current size:%d. expect more than:%d", idx_i, params.operand_count_);
        ret = OB_INVALID_ARGUMENT;
    }
    else
    {
        if(stack_i[idx_i-1].get_type ()== ObVarcharType )
        {
            ObObjType obj_type = stack_i[idx_i-2].get_type ();
            if(obj_type== ObDateType)
            {
                result = stack_i[idx_i - 2];
            }
            else if(obj_type == ObPreciseDateTimeType||
                    obj_type == ObDateTimeType ||
                    obj_type == ObCreateTimeType||
                    obj_type == ObModifyTimeType
                    )
            {
                int unexpected_type = TIME_CHECK;
                struct tm gtm;
                if(OB_SUCCESS!=(ret = stack_i[idx_i - 2].get_gtm(gtm, unexpected_type)))
                {
                    TBSYS_LOG(WARN,"wrong type");
                }
                else
                {
                    time_t t_t = mktime(&gtm);
                    result.set_date(static_cast<ObDate>(static_cast<int64_t>(t_t) * 1000000L));
                }
            }
            else if(obj_type == ObVarcharType)
            {
                date_length = stack_i[idx_i-2].get_varchar().length();
                regex_length = stack_i[idx_i-1].get_varchar ().length ();
                snprintf(date_user, 40, "%.*s", stack_i[idx_i-2].get_varchar().length(), stack_i[idx_i-2].get_varchar().ptr ());
                snprintf(date_regex, 40, "%.*s", stack_i[idx_i-1].get_varchar ().length(), stack_i[idx_i-1].get_varchar ().ptr());
                ret = parse_date(date_user,date_regex,date_length,regex_length,result);
            }
            else
            {
                ret = OB_INVALID_ARGUMENT;
                TBSYS_LOG(WARN, "no support type!");
            }
        }
        else
        {
            TBSYS_LOG(USER_ERROR, "fomart must be varchar type");
            ret = OB_INVALID_ARGUMENT;
        }

    }
    if (OB_SUCCESS == ret)
    {
        idx_i -= params.operand_count_;
    }
    return ret;
}
//add wanglei:e
//add wanglei [to_timestamp] 20160311:b
inline int ObPostfixExpression::parse_timestamp(char * date,char * date_str,int date_length,int date_str_length,ObExprObj &ob_date)
{
    UNUSED(date_length);
    UNUSED(date_str_length);
    int ret = OB_SUCCESS;
    int unexpected_type = TIME_CHECK;
    struct tm gtm,gtm_check;
    memset(&gtm,0,sizeof(gtm));
    const int REGEX_NUM = 3;
    const char *regex_list[REGEX_NUM] = {
        "dd-mon-yy hh.mm.ss.ssssss pp",
        "yyyy-mm-dd hh.mm.ss.ssssss",
        "yyyy-mm-dd hh:mm:ss"
    };
    int index = -1;
    char year[10];
    char month[10];
    char day[10];
    char minutes[10];
    char hour[10];
    char second[10];
    char microseconds[10];
    char date_t[20];
    char time_tmp[30];
    char PM[10];
    memset(year,'\0',sizeof(year));
    memset(month,'\0',sizeof(month));
    memset(day,'\0',sizeof(day));
    memset(minutes,'\0',sizeof(minutes));
    memset(hour,'\0',sizeof(hour));
    memset(second,'\0',sizeof(second));
    memset(microseconds,'\0',sizeof(microseconds));
    memset(date_t,'\0',sizeof(date_t));
    memset(time_tmp,'\0',sizeof(time_tmp));
    memset(PM,'\0',sizeof(PM));
    for(int i=0;i<date_str_length;i++)
    {
        date_str[i] = (char)tolower(date_str[i]);
    }
    for(int i=0;i<REGEX_NUM;i++)
    {
        if(strcmp(regex_list[i],date_str)==0)
        {
            index = i;
            break;
        }
    }
    int k_count=1;
    int j_t=0;
    int date_t_length =0;
    int time_t_length =0;
    for(int i=0;i<date_length;i++)
    {
        if(k_count==1)
        {
            if(date[i]!=' ')
            {
                date_t[j_t++] = date[i];
                date_t_length++;
            }
            else
            {
                date_t[j_t] ='\0';
                k_count++;
                j_t = 0;
            }
        }else if(k_count ==2)
        {
            if(date[i]!=' ')
            {
                time_tmp[j_t++] = date[i];
                time_t_length++;
            }
            else
            {
                time_tmp[j_t] ='\0';
                k_count++;
                j_t = 0;
            }
        }else if(k_count ==3 && index ==0)
        {
            if(date[i]!=' ')
                PM[j_t++] = date[i];
            else
            {
                PM[j_t] ='\0';
                k_count++;
                j_t = 0;
            }
        }
    }
    int split_g[2] ={0,0};
    int split_d[3] ={0,0,0};
    for(int i=0,j=0;i<date_t_length&&j<2;i++){if(date_t[i] == '-' || date_t[i] == '/'){split_g[j]= i;j++;}}
    for(int i=0,j=0;i<time_t_length&&j<3;i++){if(time_tmp[i] == '.' || time_tmp[i] == ':'){split_d[j]= i;j++;}}
    switch(index)
    {
    case -1: {
        ret = OB_INVALID_ARGUMENT;
        TBSYS_LOG(USER_ERROR,"not suport format type!");
    }break;
    case 0:{
        int index_month = 0;
        bool is_has_month = false;
        for(int j=0;j<date_length;j++)
        {
            char temp[4];
            memset(temp,0,sizeof(temp));
            temp[0] = date_t[j];
            temp[1] = date_t[++j];
            temp[2] = date_t[++j];
            if(strcmp(temp,"月")==0)
            {
                is_has_month = true;
                index_month = j-2;
            }
            j=j-2;
        }
        if(is_has_month)
        {
            for(int i=0;i<split_g[0];i++)
            {
                day[i] = date_t[i];
            }
            for(int i=split_g[0]+1,j=0;i<index_month;i++,j++)
            {
                month[j] = date_t[i];
            }
            for(int i=split_g[1]+1,j=0;i<date_length;i++,j++)
            {
                year[j] = date_t[i];
            }
        }
        else
        {
            for(int i=0;i<split_g[0];i++)
            {
                day[i] = date_t[i];
            }
            for(int i=split_g[0]+1,j=0;i<split_g[1];i++,j++)
            {
                month[j] = (char)tolower(date_t[i]);
            }
            for(int i=split_g[1]+1,j=0;i<date_t_length;i++,j++)
            {
                year[j] = date_t[i];
            }

            for(int i=0;i<split_d[0];i++)
            {
                hour[i] = time_tmp[i];
            }
            for(int i=split_d[0]+1,j=0;i<split_d[1];i++,j++)
            {
                minutes[j] = time_tmp[i];
            }
            for(int i=split_d[1]+1,j=0;i<split_d[2];i++,j++)
            {
                second[j] = time_tmp[i];
            }
            for(int i=split_d[2]+1,j=0;i<time_t_length;i++,j++)
            {
                microseconds[j] = time_tmp[i];
            }
        }
    }break;
    case 1:
    {
        for(int i=0;i<split_g[0];i++)
        {
            year[i] = date_t[i];
        }
        for(int i=split_g[0]+1,j=0;i<split_g[1];i++,j++)
        {
            month[j] = date_t[i];
        }
        for(int i=split_g[1]+1,j=0;i<date_t_length;i++,j++)
        {
            day[j] = date_t[i];
        }

        for(int i=0;i<split_d[0];i++)
        {
            hour[i] = time_tmp[i];
        }
        for(int i=split_d[0]+1,j=0;i<split_d[1];i++,j++)
        {
            minutes[j] = time_tmp[i];
        }
        for(int i=split_d[1]+1,j=0;i<split_d[2];i++,j++)
        {
            second[j] = time_tmp[i];
        }
        for(int i=split_d[2]+1,j=0;i<time_t_length;i++,j++)
        {
            microseconds[j] = time_tmp[i];
        }
    }break;
    case 2:
    {
        for(int i=0;i<split_g[0];i++)
        {
            year[i] = date_t[i];
        }
        for(int i=split_g[0]+1,j=0;i<split_g[1];i++,j++)
        {
            month[j] = date_t[i];
        }
        for(int i=split_g[1]+1,j=0;i<date_t_length;i++,j++)
        {
            day[j] = date_t[i];
        }

        for(int i=0;i<split_d[0];i++)
        {
            hour[i] = time_tmp[i];
        }
        for(int i=split_d[0]+1,j=0;i<split_d[1];i++,j++)
        {
            minutes[j] = time_tmp[i];
        }
        for(int i=split_d[1]+1,j=0;i<time_t_length;i++,j++)
        {
            second[j] = time_tmp[i];
        }
    }break;
    default:
    {
        ret = OB_INVALID_ARGUMENT;
        TBSYS_LOG(USER_ERROR,"not suport format type!");
    }break;
    }
    if(ret == OB_SUCCESS)
    {
        int year_int = 0;
        if(atoi(year)<1000)
            year_int = atoi(year) + 2000;
        else
            year_int = atoi(year);

        gtm.tm_year = year_int-1900;
        if(index == 0)
        {
            if(strcmp(month,"jan")==0)
            {
                gtm.tm_mon = 1-1;
            }
            else if(strcmp(month,"feb")==0)
            {
                gtm.tm_mon = 2-1;
            }
            else if(strcmp(month,"mar")==0)
            {
                gtm.tm_mon = 3-1;
            }
            else if(strcmp(month,"apr")==0)
            {
                gtm.tm_mon = 4-1;
            }
            else if(strcmp(month,"may")==0)
            {
                gtm.tm_mon = 5-1;
            }
            else if(strcmp(month,"jun")==0)
            {
                gtm.tm_mon = 6-1;
            }
            else if(strcmp(month,"jul")==0)
            {
                gtm.tm_mon = 7-1;
            }
            else if(strcmp(month,"aug")==0)
            {
                gtm.tm_mon = 8-1;
            }
            else if(strcmp(month,"sep")==0)
            {
                gtm.tm_mon = 9-1;
            }
            else if(strcmp(month,"oct")==0)
            {
                gtm.tm_mon = 10-1;
            }
            else if(strcmp(month,"nov")==0)
            {
                gtm.tm_mon = 11-1;
            }
            else if(strcmp(month,"dec")==0)
            {
                gtm.tm_mon = 12-1;
            }
            else
            {
                ret = OB_INVALID_ARGUMENT;
                TBSYS_LOG(USER_ERROR,"the date param is not corrected");
            }
        }
        else
          gtm.tm_mon = atoi(month) -1;
        if(ret == OB_SUCCESS)
        {
            gtm.tm_mday = atoi(day);
            if(strcmp(PM,"PM") ==0 ||strcmp(PM,"pm") ==0)
                gtm.tm_hour =(atoi(hour)+12);
            else
                gtm.tm_hour =(atoi(hour));
            gtm.tm_min = atoi(minutes);
            gtm.tm_sec = atoi(second);
            gtm.tm_isdst = -1;
            time_t t_t = mktime(&gtm);
            int64_t timestamp =0;
            timestamp = static_cast<ObPreciseDateTime>(static_cast<int64_t>(t_t) * 1000000L);
            if (timestamp < 0)
            {
                timestamp -= atoi(microseconds);
            }
            else
            {
                timestamp += atoi(microseconds);
            }
            ob_date.set_precise_datetime (timestamp);
            if(OB_SUCCESS != (ret = ob_date.get_gtm(gtm_check, unexpected_type)))
            {
                TBSYS_LOG(USER_ERROR,"the date param is not corrected");
            }
        }
    }
    return ret;
}
inline int ObPostfixExpression::sys_func_to_timestamp(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params)
{
    int ret = OB_SUCCESS;
    int regex_length = 0;
    int date_length = 0;
    char date_regex[40] ;
    char date_user[40] ;
    memset(date_regex,'\0',sizeof(date_regex));
    memset(date_user,'\0',sizeof(date_user));
    if (OB_UNLIKELY(NULL == stack_i))
    {
        TBSYS_LOG(WARN, "stack pointer is NULL.");
        ret = OB_INVALID_ARGUMENT;
    }
    else if (OB_UNLIKELY(idx_i < 2))
    {
        TBSYS_LOG(WARN, "no enough operand in the stack. current size:%d", idx_i);
        ret = OB_INVALID_ARGUMENT;
    }
    else
    {
        if(stack_i[idx_i-1].get_type ()== ObVarcharType)
        {
            date_length = stack_i[idx_i-2].get_varchar().length();
            regex_length = stack_i[idx_i-1].get_varchar().length();;
            snprintf(date_user, 40, "%.*s", stack_i[idx_i-2].get_varchar().length(), stack_i[idx_i-2].get_varchar().ptr ());
            snprintf(date_regex, 40, "%.*s", stack_i[idx_i-1].get_varchar().length(), stack_i[idx_i-1].get_varchar().ptr ());
            ret = parse_timestamp (date_user,date_regex,date_length,regex_length,result);
        }
        else
        {
            TBSYS_LOG(USER_ERROR, "fomart must be varchar type");
            ret = OB_INVALID_ARGUMENT;
        }

    }
    if (OB_SUCCESS == ret)
    {
        idx_i -= params.operand_count_;
    }
    return ret;
}
//add wanglei:e
//add qianzhaoming [decode] 20151029:b
inline int ObPostfixExpression::sys_func_decode(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params)
{
    int ret = OB_SUCCESS;
    int i = 0;
    int first = 0;
    bool match_when = false;
    if (NULL == stack_i)
    {

        TBSYS_LOG(WARN, "stack pointer is NULL.");
        ret = OB_INVALID_ARGUMENT;
    }
    else if (params.operand_count_ < 3 || idx_i < params.operand_count_)
    {

        TBSYS_LOG(WARN, "no enough operand in the stack. current size:%d. expect more than:%d", idx_i, params.operand_count_);
        ret = OB_INVALID_ARGUMENT;
    }
    else
    {
        first = idx_i - params.operand_count_;
        ObExprObj cmp;
        for (i = first + 1; i < idx_i - 1; i += 2)
        {
            if (stack_i[first].get_type() == ObNullType && stack_i[i].get_type() == ObNullType)
            {
                result = stack_i[i+1];
                match_when = true;
                break;
            }
            stack_i[first].eq(stack_i[i], cmp);
            if(cmp.is_true())
            {
                result = stack_i[i+1];
                match_when = true;
                break;
            }
        }
        if (false == match_when)
        {
            if (params.operand_count_ % 2 == 0)
            {
                result = stack_i[idx_i - 1]; // match else
            }
            else
            {
                result.set_null(); // no else in expression, then match nothing
            }
        }
        if (OB_SUCCESS == ret)
        {
            idx_i -= params.operand_count_;
        }
    }
    return ret;
}
//add :e

int ObPostfixExpression::arg_case_func(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params)
{
    int ret = OB_SUCCESS;
    int i = 0;
    bool match_when = false;
    if (NULL == stack_i)
    {
        TBSYS_LOG(WARN, "stack pointer is NULL.");
        ret = OB_INVALID_ARGUMENT;
    }
    else
    {
        for (i = idx_i-3; i < idx_i; i += 2)
        {
            if (stack_i[i].is_true())
            {
                result = stack_i[i+1];
                match_when = true;
                break;
            }
        }
        if (false == match_when)
        {
            if (3% 2 != 0)
            {
                result = stack_i[idx_i - 1]; // match else
            }
            else
            {
                result.set_null(); // no else in expression, then match nothing
            }
        }
        if (OB_SUCCESS == ret)
        {
            idx_i -= 3;//params.operand_count_;
        }
        //add dolphin[case when return type]@20151231:b
        if(result.get_type() != params.expr_type_)
        {
            ObObj ex_type;
            ObObj obj1;
            result.to(obj1);
            ex_type.set_type(params.expr_type_);
            ObObj res;
            char buf[MAX_PRINTABLE_SIZE];
            res.set_varchar(ObString(MAX_PRINTABLE_SIZE,MAX_PRINTABLE_SIZE,buf));
            const ObObj *temp;
            if(OB_SUCCESS != (ret = obj_cast(obj1,ex_type,res,temp)))
            {
                char bf[256];
                obj1.to_string(bf,255);
                TBSYS_LOG(WARN,"failed to cast %s to type %d,ret=%d",bf,params.expr_type_,ret);
            }
            else
            {
                result.assign(*temp);
            }
        }
        //add:e
    }
    return ret;
    //注释掉的原流�?b
//    int ret = OB_SUCCESS;
//    int arg_pos = idx_i - params.operand_count_;
//    ObExprObj cmp;
//    int i = 0;
//    bool match_when = false;
//    if (NULL == stack_i)
//    {
//        TBSYS_LOG(WARN, "stack pointer is NULL.");
//        ret = OB_INVALID_ARGUMENT;
//    }
//    else if (params.operand_count_ < 3 || idx_i < params.operand_count_)
//    {
//        TBSYS_LOG(WARN, "no enough operand in the stack. current size:%d. expect more than:%d", idx_i, params.operand_count_);
//        ret = OB_INVALID_ARGUMENT;
//    }
//    else
//    {
//        // case 1:
//        // case c1 when 10 then expr1 else expr2  ==postfix stack==>
//        // c1(idx_i-4), 10(idx_i-3), expr1(idx_i-2), expr2(idx_i-1), T_OP_ARG_CASE
//        // params.operand_count_ = 4
//        //
//        // case 2:
//        // case c1 when 10 then expr1  ==postfix stack==>
//        // c1(idx_i-3), 10(idx_i-2), expr1(idx_i-1), T_OP_ARG_CASE
//        // params.operand_count_ = 3
//        //
//        for (i = idx_i - (params.operand_count_ - 1); i < idx_i - 1; i += 2)
//        {
//            stack_i[arg_pos].eq(stack_i[i], cmp);
//            //TBSYS_LOG(DEBUG, "i=%d,arg_pos=%d,cmp_case_when(%s,%s)", i, arg_pos, to_cstring(stack_i[arg_pos]), to_cstring(stack_i[i]));
//            if (cmp.is_true()) // match when
//            {
//                result = stack_i[i+1];
//                match_when = true;
//                break;
//            }
//        }
//        if (false == match_when)
//        {
//            if (params.operand_count_ % 2 == 0)
//            {
//                result = stack_i[idx_i - 1]; // match else
//            }
//            else
//            {
//                result.set_null(); // no else in expression, then match nothing
//            }
//        }
//        if (OB_SUCCESS == ret)
//        {
//            idx_i -= params.operand_count_;
//        }
//        //add dolphin[case when return type]@20151231:b
//        if(result.get_type() != params.expr_type_)
//        {
//            ObObj ex_type;
//            ObObj obj1;
//            result.to(obj1);
//            ex_type.set_type(params.expr_type_);
//            ObObj res;
//            char buf[MAX_PRINTABLE_SIZE];
//            res.set_varchar(ObString(MAX_PRINTABLE_SIZE,MAX_PRINTABLE_SIZE,buf));
//            const ObObj *temp;
//            if(OB_SUCCESS != (ret = obj_cast(obj1,ex_type,res,temp)))
//            {
//                char bf[256];
//                obj1.to_string(bf,255);
//                TBSYS_LOG(WARN,"failed to cast %s to type %d,ret=%d",bf,params.expr_type_,ret);
//            }
//            else
//            {
//                result.assign(*temp);
//            }
//        }
//        //add:e
//    }
//    return ret;
    //注释掉的原流�?e
}

int ObPostfixExpression::case_func(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params)
{
    int ret = OB_SUCCESS;
    int i = 0;
    bool match_when = false;
    if (NULL == stack_i)
    {
        TBSYS_LOG(WARN, "stack pointer is NULL.");
        ret = OB_INVALID_ARGUMENT;
    }
    else
    {
        for (i = idx_i - params.operand_count_; i < idx_i; i += 2)
        {
            if (stack_i[i].is_true())
            {
                result = stack_i[i+1];
                match_when = true;
                break;
            }
        }
        if (false == match_when)
        {
            if (params.operand_count_ % 2 != 0)
            {
                result = stack_i[idx_i - 1]; // match else
            }
            else
            {
                result.set_null(); // no else in expression, then match nothing
            }
        }
        if (OB_SUCCESS == ret)
        {
            idx_i -= 3;//params.operand_count_;
        }
        //add dolphin[case when return type]@20151231:b
        if(result.get_type() != params.expr_type_)
        {
            ObObj ex_type;
            ObObj obj1;
            result.to(obj1);
            ex_type.set_type(params.expr_type_);
            ObObj res;
            char buf[MAX_PRINTABLE_SIZE];
            res.set_varchar(ObString(MAX_PRINTABLE_SIZE,MAX_PRINTABLE_SIZE,buf));
            const ObObj *temp;
            if(OB_SUCCESS != (ret = obj_cast(obj1,ex_type,res,temp)))
            {
                char bf[256];
                obj1.to_string(bf,255);
                TBSYS_LOG(WARN,"failed to cast %s to type %d,ret=%d",bf,params.expr_type_,ret);
            }
            else
            {
                result.assign(*temp);
            }
        }
        //add:e
    }
    return ret;
    //注释掉的原流�?b
//    int ret = OB_SUCCESS;
//    int i = 0;
//    bool match_when = false;
//    if (NULL == stack_i)
//    {
//        TBSYS_LOG(WARN, "stack pointer is NULL.");
//        ret = OB_INVALID_ARGUMENT;
//    }
//    else if (params.operand_count_ < 3 || idx_i < params.operand_count_)
//    {
//        TBSYS_LOG(WARN, "no enough operand in the stack. current size:%d. expect more than:%d", idx_i, params.operand_count_);
//        ret = OB_INVALID_ARGUMENT;
//    }
//    else
//    {
//        // case 1:
//        // case when false then expr1 else expr2  ==postfix stack==>
//        // false(idx_i-3), expr1(idx_i-2), expr2(idx_i-1), T_OP_ARG_CASE
//        // params.operand_count_ = 3
//        //
//        // case 2:
//        // case when false then expr1  ==postfix stack==>
//        // false(idx_i-2), expr1(idx_i-1), T_OP_ARG_CASE
//        // params.operand_count_ = 2
//        //
//        for (i = idx_i - params.operand_count_; i < idx_i; i += 2)
//        {
//            if (stack_i[i].is_true())
//            {
//                result = stack_i[i+1];
//                match_when = true;
//                break;
//            }
//        }
//        if (false == match_when)
//        {
//            if (params.operand_count_ % 2 != 0)
//            {
//                result = stack_i[idx_i - 1]; // match else
//            }
//            else
//            {
//                result.set_null(); // no else in expression, then match nothing
//            }
//        }
//        if (OB_SUCCESS == ret)
//        {
//            idx_i -= params.operand_count_;
//        }
//        //add dolphin[case when return type]@20151231:b
//        if(result.get_type() != params.expr_type_)
//        {
//            ObObj ex_type;
//            ObObj obj1;
//            result.to(obj1);
//            ex_type.set_type(params.expr_type_);
//            ObObj res;
//            char buf[MAX_PRINTABLE_SIZE];
//            res.set_varchar(ObString(MAX_PRINTABLE_SIZE,MAX_PRINTABLE_SIZE,buf));
//            const ObObj *temp;
//            if(OB_SUCCESS != (ret = obj_cast(obj1,ex_type,res,temp)))
//            {
//                char bf[256];
//                obj1.to_string(bf,255);
//                TBSYS_LOG(WARN,"failed to cast %s to type %d,ret=%d",bf,params.expr_type_,ret);
//            }
//            else
//            {
//                result.assign(*temp);
//            }
//        }
//        //add:e
//    }
//    return ret;
    //注释掉的原流�?e
}
// dolphin[in post-expression optimization]@20150915
int ObPostfixExpression::check_in_hash(ObExprObj& result,int t,int64_t v)
{
    int ret = OB_SUCCESS;
    if(NULL == post_in_ada_)
    {
        ret = OB_ERR_POINTER_IS_NULL;
        TBSYS_LOG(WARN,"post expression check in hash but post_in_ada_ ptr is NULL");
        return ret;
    }
    int64_t cnt = post_in_ada_->left_param_.get_obj_cnt();
    for(int i = 0;i < cnt;++i)
    {
        if(ObNullType == post_in_ada_->left_param_.get_obj_ptr()[i].get_type())
        {
            result.set_bool(false);
            return ret;
        }

    }
    const ObRowkey *value = post_in_ada_->rhv.at(t)->right_hash_.get(post_in_ada_->left_param_);
    if( v == T_OP_IN)
    {
        if(value != NULL)
        {
            const ObObj *obj1 = post_in_ada_->left_param_.get_obj_ptr();
            const ObObj *obj2 = value->get_obj_ptr();
            ObExprObj exp1,exp2;
            int val;
            result.set_bool(true);
            for(int j = 0;j<cnt;++j)
            {
                exp1.assign(obj1[j]);
                exp2.assign(obj2[j]);
                if(OB_SUCCESS != (ret = exp1.compare(exp2,val)))
                {
                    char buf1[256];
                    char buf2[256];
                    exp1.to_string(buf1,256);
                    exp2.to_string(buf2,256);
                    TBSYS_LOG(WARN,"ObExprObj %s and %s compare error",buf1,buf2);
                    break;
                }
                else
                {
                    if(val != 0)
                    {
                        result.set_bool(false);
                        break;
                    }

                }
            }
        }
        else
        {
            result.set_bool(false);
        }
    }
    else if( v == T_OP_NOT_IN)
    {
        if(value != NULL)
        {
            const ObObj *obj1 = post_in_ada_->left_param_.get_obj_ptr();
            const ObObj *obj2 = value->get_obj_ptr();
            ObExprObj exp1,exp2;
            int val;
            result.set_bool(true);
            for(int j = 0;j<cnt;++j)
            {
                exp1.assign(obj1[j]);
                exp2.assign(obj2[j]);
                if(OB_SUCCESS != (ret = exp1.compare(exp2,val)))
                {
                    char buf1[256];
                    char buf2[256];
                    exp1.to_string(buf1,256);
                    exp2.to_string(buf2,256);
                    TBSYS_LOG(WARN,"ObExprObj %s and %s compare error",buf1,buf2);
                    break;
                }
                else
                {
                    if(val == 0)
                    {
                        result.set_bool(false);
                        break;
                    }
                }
            }
        }
        else
        {
            result.set_bool(true);
        }
        //if not in is true,then we will next iterator not_null branch
        if(result.get_bool() && post_in_ada_->rhv.at(t)->in_null_.size() > 0)
        {
            ObVector<ObRowkey*>::iterator it  = post_in_ada_->rhv.at(t)->in_null_.begin();
            bool r1=false,r2=false;
            for(;it != post_in_ada_->rhv.at(t)->in_null_.end();++it)
            {
                const ObObj *obj1 = (*it)->get_obj_ptr();
                ObExprObj exp1,exp2;
                int val;
                const ObObj *obj2 = post_in_ada_->left_param_.get_obj_ptr();
                for(int j = 0;j<cnt;++j)
                {
                    exp1.assign(obj1[j]);
                    exp2.assign(obj2[j]);
                    char buf1[256];
                    char buf2[256];
                    exp1.to_string(buf1,256);
                    exp2.to_string(buf2,256);
                    if(ObNullType != obj1[j].get_type())
                    {
                        exp1.assign(obj1[j]);
                        exp2.assign(obj2[j]);
                        if(OB_SUCCESS != (ret = exp1.compare(exp2,val)))
                        {
                            char buf1[256];
                            char buf2[256];
                            exp1.to_string(buf1,256);
                            exp2.to_string(buf2,256);
                            TBSYS_LOG(WARN,"ObExprObj %s and %s compare error",buf1,buf2);
                            break;
                        }
                        else
                        {
                            r1 = val != 0;
                        }
                    }
                    else if(cnt > 1 )
                    {
                        r1 = false;
                    }
                    else if(cnt == 1)
                    {
                        return ret;
                    }
                    r2 = r1||r2;
                    if(r2)
                        break;
                    else
                        continue;
                }
                if(!r2)
                {
                    result.set_bool(false);
                    return ret;
                }
                else
                {
                    result.set_bool(true);
                }
            }

        }
    }
    return ret;
}

/*int ObPostfixExpression:: print_in_hash_param(int t)
{
    int ret = OB_SUCCESS;
    if(NULL != post_in_ada_)
    {
        int j = 0;
        int bufsize = 0;
        int len = 0;
        char buf[256];
        ObRowkey rowkey;
        ObObj obj;
        char objstring[64];

        int64_t dim = post_in_ada_->left_param_.get_obj_cnt();
        for(int i = 0;i < dim;++i)
        {
            post_in_ada_->left_param_.get_obj_ptr()[i].to_string(objstring,64);
            len = snprintf(buf+bufsize,256,"(%s",objstring);
            bufsize += len;
        }
        TBSYS_LOG(ERROR,"dolphin::test left_param_ is %s",buf);
        hash::ObHashMap<ObRowkey,ObRowkey,hash::NoPthreadDefendMode>::const_iterator iter = post_in_ada_->rhv.at(t)->right_hash_.begin();
        bufsize = 0;
        len = 0;
        for(;iter != post_in_ada_->rhv.at(t)->right_hash_.end();++iter)
        {
            memset(buf,0,256);
            j++;
            rowkey = iter->first;
            bufsize = 0;
            len = 0;
            for(int i = 0;i < dim;++i)
            {
                obj=rowkey.get_obj_ptr()[i];
                obj.to_string(objstring,64);
                len = snprintf(buf+bufsize,256,"%s)",objstring);
                bufsize += len;


            }
            TBSYS_LOG(ERROR,"dolphin::test hash value is %s",buf);


        }
        ObVector<ObRowkey*>::iterator it  = post_in_ada_->rhv.at(t)->in_null_.begin();
        j = 0;
        for(;it != post_in_ada_->rhv.at(t)->in_null_.end();++it)
        {
            memset(buf,0,256);
            ++j;
            const ObObj *obj1 = (*it)->get_obj_ptr();
            bufsize = 0;
            len = 0;
            for(int i = 0;i < dim;++i)
            {
                obj1[i].to_string(objstring,64);
                len = snprintf(buf+bufsize,256,"%s)",objstring);
                bufsize += len;


            }
            TBSYS_LOG(ERROR,"dolphin::test hash null value is %s",buf);


        }


    }

    return ret;
}*/

/*int ObPostfixExpression::print_expr(int j)
{
    int ret = OB_SUCCESS;
    for(int i = j;i < expr_.count();++i)
    {
        char b[256];
        expr_[i].to_string(b,255);
        TBSYS_LOG(WARN,"expr_[%d] is %s:",i,b);

    }
    return ret;
}*/
//add:e


int64_t ObPostfixExpression::to_string(char* buf, const int64_t buf_len) const
{
    int64_t pos = 0;
    int err = OB_SUCCESS;
    int idx = 0;
    int64_t type = 0;
    int64_t value = 0;
    int64_t value2 = 0;
    int64_t sys_func = 0;
    while(idx < expr_.count() && OB_SUCCESS == err)
    {
        expr_[idx++].get_int(type);
        if (END == type)
        {
            break;
        }
        else if(type <= BEGIN_TYPE || type >= END_TYPE)
        {
            break;
        }
        switch(type)
        {
        case COLUMN_IDX:
            if (OB_SUCCESS  != (err = expr_[idx++].get_int(value)))
            {
                TBSYS_LOG(WARN,"get_int error [err:%d]", err);
            }
            else if (OB_SUCCESS  != (err = expr_[idx++].get_int(value2)))
            {
                TBSYS_LOG(WARN,"get_int error [err:%d]", err);
            }
            else
            {
                uint64_t tid = static_cast<uint64_t>(value);
                if (OB_INVALID_ID == tid)
                {
                    databuff_printf(buf, buf_len, pos, "COL<NULL,%lu>|", static_cast<uint64_t>(value2));
                }
                else
                {
                    databuff_printf(buf, buf_len, pos, "COL<%lu,%lu>|", tid, static_cast<uint64_t>(value2));
                }
            }
            break;
        case CONST_OBJ:
            pos += expr_[idx].to_string(buf+pos, buf_len-pos);
            databuff_printf(buf, buf_len, pos, "|");
            idx++;
            break;
        case CUR_TIME_OP:
            databuff_printf(buf, buf_len, pos, "current_timestamp()");
            idx++;  // skip place holder
            break;
            //add liuzy [datetime func] 20150910:b
        case CUR_DATE_OP:
            databuff_printf(buf, buf_len, pos, "current_date()");
            idx++;  // skip place holder
            break;
        case CUR_TIME_HMS_OP:
            databuff_printf(buf, buf_len, pos, "current_time()");
            idx++;  // skip place holder
            break;
            //add 20150910:e
        case PARAM_IDX:
            if (OB_SUCCESS  != (err = expr_[idx++].get_int(value)))
            {
                TBSYS_LOG(WARN,"get_int error [err:%d]", err);
            }
            else
            {
                databuff_printf(buf, buf_len, pos, "PARAM<%ld>|", value);
            }
            break;
        case SYSTEM_VAR:
            databuff_printf(buf, buf_len, pos, "@@");
            pos += expr_[idx++].to_string(buf+pos, buf_len-pos);
            break;
        case TEMP_VAR:
            databuff_printf(buf, buf_len, pos, "@");
            pos += expr_[idx++].to_string(buf+pos, buf_len-pos);
            break;
        case OP:
            // 根据OP的类型，从堆栈中弹出1个或多个操作数，进行计算
            if (OB_SUCCESS != (err = expr_[idx++].get_int(value)))
            {
                TBSYS_LOG(WARN,"get_int error [err:%d]", err);
            }
            else if (value <= T_MIN_OP || value >= T_MAX_OP)
            {
                TBSYS_LOG(WARN,"unsupported operator type [type:%ld]", value);
                err = OB_INVALID_ARGUMENT;
            }
            else if(OB_SUCCESS != (err = expr_[idx++].get_int(value2 /*param_count*/)))
            {
                TBSYS_LOG(WARN,"get_int error [err:%d]", err);
            }
            else
            {
                if (OB_UNLIKELY(T_FUN_SYS == value))
                {
                    if(OB_SUCCESS != (err = expr_[idx++].get_int(sys_func)))
                    {
                        TBSYS_LOG(WARN, "failed to get sys func, err=%d", err);
                    }
                    else if (0 > sys_func || sys_func >= SYS_FUNC_NUM)
                    {
                        TBSYS_LOG(WARN, "invalid sys function type=%ld", sys_func);
                        err = OB_ERR_UNEXPECTED;
                    }
                    else
                    {
                        databuff_printf(buf, buf_len, pos, "%s<%ld>|", SYS_FUNCS_NAME[sys_func], value2);
                    }
                }
                else
                {
                    switch (static_cast<ObItemType>(value)) {
                    case T_CASE_BEGIN:
                    case T_CASE_WHEN:
                    case T_CASE_THEN:
                    case T_CASE_ELSE:
                    case T_CASE_BREAK:
                    case T_CASE_END:
                    {
                         databuff_printf(buf, buf_len, pos, "%s|", ob_op_func_str(static_cast<ObItemType>(value)));
                    }
                        break;
                    default:
                         databuff_printf(buf, buf_len, pos, "%s<%ld>|", ob_op_func_str(static_cast<ObItemType>(value)), value2);
                        break;
                    }
                }
            }
            break;
            //add tianz [SubQuery_for_Instmt] [JHOBv0.1] 20140403:b
        case QUERY_ID:
            if (OB_SUCCESS  != (err = expr_[idx++].get_int(value)))
            {
                TBSYS_LOG(WARN,"get_int error [err:%d]", err);
            }
            else
            {
                databuff_printf(buf, buf_len, pos, "sub_query_select_num<%ld>|", value);
            }
            break;
            //add 20140403:e
        default:
            databuff_printf(buf, buf_len, pos, "unexpected [type:%ld]", type);
            err = OB_ERR_UNEXPECTED;
            TBSYS_LOG(WARN,"unexpected [type:%ld]", type);
            break;
        }
    } // end while
    return pos;
}
//add gaojt [Insert_Subquery_Function] [JHOBv0.1] 20140715:b

/*Exp:determin whether the expr is sub_query for insert,
    * used for rpc_scan get filter
    */
bool ObPostfixExpression::is_rows_filter()
{
    int err = OB_SUCCESS;
    bool ret = false;
    int64_t mark = 0;
    int64_t idx = 0;
    int64_t type = BEGIN_TYPE;
    while(OB_SUCCESS == err && END != type)
    {
        idx = idx + get_type_num(idx,type);
        if (OB_SUCCESS != (err = expr_[idx].get_int(type)))
        {
            TBSYS_LOG(WARN, "Fail to get type. unexpected! ret=%d", err);
            err = OB_ERR_UNEXPECTED;
            break;
        }
        else if(QUERY_ID == type)
        {
            if(OB_SUCCESS != (err = expr_[++idx].get_int(mark)))
            {
                TBSYS_LOG(WARN, "Fail to get sub query column num. unexpected! ret=%d", err);
                err = OB_ERR_UNEXPECTED;
            }
            else if(-1 == mark)
            {
                ret = true;
                break;
            }
        }
    }
    return ret;
}

/*Exp:used before specific value replace the mark of sub_query,
    * pop other information of expression out to temp expression ,
    * then remove the mark of sub_query
    */
int ObPostfixExpression::delete_sub_query_info()
{
    int ret= OB_SUCCESS;
    int64_t idx = 0;
    int64_t type = BEGIN_TYPE;
    int64_t mark = 0;
    //find sub_query mark in expr_
    while(OB_SUCCESS == ret && END != type)
    {
        idx = idx + get_type_num(idx,type);
        if (OB_SUCCESS != (ret = expr_[idx].get_int(type)))
        {
            TBSYS_LOG(WARN, "Fail to get type. unexpected! ret=%d", ret);
            ret = OB_ERR_UNEXPECTED;
            break;
        }
        else if(QUERY_ID == type)
        {
            if(OB_SUCCESS != (ret = expr_[idx+1].get_int(mark)))
            {
                TBSYS_LOG(WARN, "Fail to get sub query column num. unexpected! ret=%d", ret);
                ret = OB_ERR_UNEXPECTED;
            }
            else if(-1 == mark)
            {
                break;
            }
        }
    }
    //push other information to temp_expr_
    ObObj temp_obj;
    if(OB_SUCCESS == ret && -1 == mark)
    {
        idx = idx + 2;//sub select's two obj
        while(idx < expr_.count())
        {
            expr_.pop_back(temp_obj);
            temp_expr_.push_back(temp_obj);
        }
    }
    //remove sub_query's two elements
    expr_.pop_back();
    expr_.pop_back();
    return ret;
}

/*Exp:determin whether the expr is sub_query for insert,
    * used for strategy store expr
    */
bool ObPostfixExpression::is_in_sub_query_expr(const ObRowkeyInfo &info) const
{
    int err = OB_SUCCESS;
    bool is_sub_query_expr = false;
    int64_t len = expr_.count();;
    int64_t  dim2 = 0;
    int64_t rowkey_column_count = 0;
    int64_t index = 0;
    ObRowkeyColumn rowkey_column;
    int64_t cid = OB_INVALID_ID;
    int64_t row_count = 0;
    if(len<0)
    {
        TBSYS_LOG(INFO, "should not be here. len=%ld", len);
    }
    else if(len>14)
    {
        if (!ExprUtil::is_end(expr_.at(len-1)) || !ExprUtil::is_value(expr_.at(len-2), 2L) ||
                !ExprUtil::is_op_of_type(expr_.at(len-3), T_OP_IN) || !ExprUtil::is_op(expr_.at(len-4)))
        {
            TBSYS_LOG(DEBUG, "not simple in expr. len=%ld. %d, %d, %d, %d", len,
                      ExprUtil::is_end(expr_.at(len-1)), ExprUtil::is_value(expr_.at(len-2), 2L),
                      ExprUtil::is_op_of_type(expr_.at(len-3), T_OP_IN), ExprUtil::is_op(expr_.at(len-4)));
        }

        else if (OB_SUCCESS != expr_.at(len-5).get_int(row_count) || !ExprUtil::is_sub_query(expr_.at(len-6)))
        {
            //modify by wanglei [semi join]:b 20160106
            //TBSYS_LOG(INFO, "not simple in expr. len=%ld", len);
            //add :e
        }
        else if (!ExprUtil::is_value(expr_.at(len-7), 2L) || !ExprUtil::is_op_of_type(expr_.at(len-8), T_OP_LEFT_PARAM_END) ||
                 !ExprUtil::is_op(expr_.at(len-9)))
        {
            TBSYS_LOG(DEBUG, "not simple in expr. len=%ld", len);
        }
        else
        {
            OB_ASSERT(len > 10);
            if(OB_SUCCESS !=(err= expr_.at(len-10).get_int(dim2)))
            {
                TBSYS_LOG(ERROR, "fail to get left value of in expr. err=%d", err);
            }
            else if(dim2 >= info.get_size())
            {
                len = len -(dim2*3+12);
                if(0 == len)
                {
                    int64_t size = info.get_size();
                    for (index = 0; index < size && index < dim2&& OB_SUCCESS == err; index++)
                    {
                        if (OB_SUCCESS != (err = info.get_column(index, rowkey_column)))
                        {
                            TBSYS_LOG(ERROR, "get rowkey column fail. index=%ld, size=%ld", index, size);
                        }
                        else
                        {
                            OB_ASSERT(index * 3 + 2 < expr_.count());
                            if (OB_SUCCESS != expr_.at(index * 3 + 2).get_int(cid)) // 3=COLUMN_REF, TID, CID
                            {
                                TBSYS_LOG(ERROR, "fail to get int value from expr_.at(2)");
                            }
                            if (rowkey_column.column_id_ == static_cast<uint64_t>(cid))
                            {
                                rowkey_column_count++;
                            }
                            else
                            {
                                break;
                            }
                        }

                    }
                    if ((OB_SUCCESS == err) && (rowkey_column_count == size))
                    {
                        is_sub_query_expr = true;
                    }
                }

            }
        }
    }
    return is_sub_query_expr;
}

//add lbzhong[Update rowkey] 20151221:b
int ObPostfixExpression::convert_to_equal_expr(const int64_t table_id, const int64_t column_id, const ObObj *& value)
{
    int ret = OB_SUCCESS;
    ExprItem expr_item_column; // COL<tid, cid>
    expr_item_column.type_ = T_REF_COLUMN;
    expr_item_column.value_.cell_.tid = table_id;
    expr_item_column.value_.cell_.cid = column_id;
    if(OB_SUCCESS != (ret = add_expr_item(expr_item_column)))
    {
        TBSYS_LOG(ERROR, "fail to add_expr_item, ret=%d", ret);
    }
    else
    {
        ExprItem expr_item_value; // value
        expr_item_value.assign(*value);
        if(OB_SUCCESS != (ret = add_expr_item(expr_item_value)))
        {
            TBSYS_LOG(ERROR, "fail to add_expr_item, ret=%d", ret);
        }
        else
        {
            ExprItem expr_item_eq; // EQ<2>
            expr_item_eq.type_ = T_OP_EQ;
            expr_item_eq.value_.int_ = 2;
            if(OB_SUCCESS != (ret = add_expr_item(expr_item_eq)))
            {
                TBSYS_LOG(ERROR, "fail to add_expr_item, ret=%d", ret);
            }
            else if(OB_SUCCESS != (ret = add_expr_item_end())) // <END>
            {
                TBSYS_LOG(ERROR, "fail to add_expr_item_end, ret=%d", ret);
            }
        }
    }
    return ret;
}
//add:e
} /* sql */
} /* namespace */
