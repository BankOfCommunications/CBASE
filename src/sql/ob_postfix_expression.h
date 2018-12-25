/*
 * (C) 2007-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * ob_postfix_expression.h is for what ...
 *
 * Version: $id: ob_postfix_expression.h, v 0.1 7/29/2011 14:39 xiaochu Exp $
 *
 * Authors:
 *   xiaochu <xiaochu.yh@taobao.com>
 *     - 后缀表达式求值，可用于复合列等需要支持复杂求值的场合
 *
 */



#ifndef OCEANBASE_SQL_OB_POSTFIX_EXPRESSION_H_
#define OCEANBASE_SQL_OB_POSTFIX_EXPRESSION_H_

#include <common/ob_custom_allocator.h>
#include <common/ob_vector.h>
#include <common/ob_obj_cast.h>
#include "ob_item_type.h"
#include "common/ob_string.h"
#include "common/ob_string_buf.h"
#include "common/ob_string_search.h"
#include "common/ob_array.h"
#include "common/ob_object.h"
#include "common/ob_result.h"
#include "common/ob_row.h"
#include "common/ob_expr_obj.h"
#include "common/ob_se_array.h"
#include "ob_phy_operator.h"
//add tianz [SubQuery_for_Instmt] [JHOBv0.1] 20140408:b
#include "common/bloom_filter.h"
#include "common/hash/ob_hashmap.h"
#include "common/ob_rowkey.h"
//add 20140408:e
using namespace oceanbase::common;

namespace oceanbase
{
  namespace common
  {
    class ObRowkeyInfo;
  };
  namespace sql
  {
  //add peiouya [DATE_TIME] 20150914:b
  enum time_unit
  {
    ObTimeMinUnit = -1,
    days,
    hours,
    minutes,
    seconds,
    microseconds,
    ObTimeMaxUnit
  };//add 20150914:e
    struct ExprItem
    {
      struct SqlCellInfo{
        uint64_t tid;
        uint64_t cid;
      };

      ObItemType  type_;
      common::ObObjType data_type_;
      /* for:
               * 1. INTNUM
               * 2. BOOL
               * 3. DATE_VALUE
               * 4. query reference
               * 5. column reference
               * 6. expression reference
               * 7. num of operands
               */
      union{
        bool      bool_;
        int64_t   datetime_;
        int64_t   int_;
        //add lijianqiang [INT_32] 20151008:b
        int32_t   int32_;
        //add 20151008:e
        float float_;
        double    double_;
        struct SqlCellInfo cell_;  // table_id, column_id
      }value_;
      // due to compile restriction, cant put string_ into union.
      // reason: ObString default constructor has parameters
      ObString  string_;        // const varchar obj or system function name
      public:
        int assign(const common::ObObj &obj);
    };

    enum ObSqlSysFunc
    {
      SYS_FUNC_LENGTH = 0,
      SYS_FUNC_SUBSTR,
      SYS_FUNC_CAST,
      SYS_FUNC_CUR_USER,
      SYS_FUNC_TRIM,
      SYS_FUNC_LOWER,
      SYS_FUNC_UPPER,
      SYS_FUNC_COALESCE,
      SYS_FUNC_HEX,
      SYS_FUNC_UNHEX,
      SYS_FUNC_IP_TO_INT,
      SYS_FUNC_INT_TO_IP,
      SYS_FUNC_GREATEST,
      SYS_FUNC_LEAST,
      //add wanglei [last_day] 20160311:b
      SYS_FUNC_LAST_DAY,
      //add :e
      //add wanglei [to_char] 20160311:b
      SYS_FUNC_TO_CHAR,
      //add :e
      //add wanglei [to_date] 20160311:b
      SYS_FUNC_TO_DATE,
      //add :e
      //add wanglei [to_timestamp] 20160311:b
      SYS_FUNC_TO_TIMESTAMP,
      //add :e
      //add qianzhaoming [decode] 201151029:b
      SYS_FUNC_DECODE,
      //add :e
      //add xionghui [floor and ceil] 20151214:b
      SYS_FUNC_FLOOR,
      SYS_FUNC_CEIL,
      //add 20151214:e
      //add xionghui [ROUND] 20150126:b
      SYS_FUNC_ROUND,
      //add 20160126 e:
      //add wuna [datetime func] 20150828:b
      SYS_FUNC_YEAR,
      SYS_FUNC_MONTH,
      SYS_FUNC_DAY,
      SYS_FUNC_DAYS,
      SYS_FUNC_DATE_ADD_YEAR,
      SYS_FUNC_DATE_SUB_YEAR,
      SYS_FUNC_DATE_ADD_MONTH,
      SYS_FUNC_DATE_SUB_MONTH,
      SYS_FUNC_DATE_ADD_DAY,
      SYS_FUNC_DATE_SUB_DAY,
      SYS_FUNC_DATE_ADD_HOUR,
      SYS_FUNC_DATE_SUB_HOUR,
      SYS_FUNC_DATE_ADD_MINUTE,
      SYS_FUNC_DATE_SUB_MINUTE,
      SYS_FUNC_DATE_ADD_SECOND,
      SYS_FUNC_DATE_SUB_SECOND,
      SYS_FUNC_DATE_ADD_MICROSECOND,
      SYS_FUNC_DATE_SUB_MICROSECOND,
      //add 20150828:e
      //add liuzy [datetime func] 20150901:b
      SYS_FUNC_HOUR,
      SYS_FUNC_MINUTE,
      SYS_FUNC_SECOND,
      //add 20150901:e
      SYS_FUNC_REPLACE,//add lijianqiang [replace_function] 20151102
      /* SYS_FUNC_NUM is always in the tail */
      SYS_FUNC_NUM
    };

    enum ObSqlParamNumFlag
    {
      TWO_OR_THREE = -3,
      OCCUR_AS_PAIR = -2,
      MORE_THAN_ZERO = -1,
      //add fanqiushi DECIMAL OceanBase_BankCommV0.3 2014_7_9:b
        /*
        * for cast函数
        */
        TWO_OR_FOUR=-4,
        //add:e
    };

    //add dolphin[in post-expression optimization]@20150922:b
    struct ObInRowHashOperator
    {
        typedef hash::ObHashMap<ObRowkey,ObRowkey,hash::NoPthreadDefendMode> InRowHashMap;
        InRowHashMap right_hash_;
        int64_t dim_;
        int64_t right_count_;
        int pre_idx_;
        char *var_cast_;
        common::ObVector<common::ObRowkey*>  in_null_;
        ObStringBuf post_buf_;
        ObInRowHashOperator():dim_(0),right_count_(0),pre_idx_(0),in_null_(3),post_buf_(ObModIds::OB_IN_POSTFIX_EXPRESSION, 64 * 1024)
        {
            right_hash_.set_local_allocer(ObModIds::OB_IN_POSTFIX_EXPRESSION_1);
            right_hash_.create(1000);
        }
        void reset()
        {
          in_null_.clear();
          right_hash_.size();
          right_hash_.destroy();
          post_buf_.clear();
          ob_free(var_cast_,ObModIds::OB_IN_POSTFIX_EXPRESSION_2);
        }
    };
  template<typename T>
  class ObPostExpressionCallBack
  {
   public:
    void operator () (ObInRowHashOperator** ptr)
    {
      (*ptr)->reset();


    }
  };
    struct ObPostfixInAdaptor
    {
        int64_t Optype_[32];
        int64_t cdim;
        int t_dim;
        int map_index;
        int64_t right_count;
        bool is_not;
        bool inhash;
        bool has_null_left_;
        ObRowkey left_param_;
        ObObj temp_for_left[256];
        ObArray<ObInRowHashOperator*,ModulePageAllocator> rhv;
        ObArray<int32_t,ModulePageAllocator> rhm;
        ObPostfixInAdaptor():cdim(0),t_dim(0),map_index(0),right_count(0),is_not(false),inhash(false),has_null_left_(false),rhv(2048,ModulePageAllocator(ObModIds::OB_IN_POSTFIX_EXPRESSION)),rhm(1024,ModulePageAllocator(ObModIds::OB_IN_POSTFIX_EXPRESSION))
        {
        }

        ~ObPostfixInAdaptor()
        {
            //rhv.clear();
            for(int64_t i=0;i<rhv.count();++i)
            {
              ObInRowHashOperator* ir =  rhv.at(i);
              ir->reset();
              ob_free(ir,ObModIds::OB_IN_POSTFIX_EXPRESSION);
            }
            rhm.clear();
        }
    };
    //add:e
    struct ObInRowOperator
    {
      ObArray<ObExprObj> vec_;
      ObInRowOperator()
      {
      }
       //add dolphin[in post-expression optimization]@20150919:b
       int push_right_hash(ObExprObj *stack,ObObj * left,ObInRowHashOperator *ip, int& top, const int64_t count,char *ptr,bool is_not = false)
        {
          int ret = OB_SUCCESS;
          int64_t i = count;
         // static int f = 0;
          if (top < count)
          {
            ret = OB_INVALID_ARGUMENT;
          }
          else
          {
            const ObObj *temp;
            ObObj res[count];
            ObObj colum_obj[count];
            ObObj colum_val[count];
            ObObj obj;
            ObObj ex_type;
            ObObjType type;
            char buf[count][MAX_PRINTABLE_SIZE];
            memset(buf,0,count * MAX_PRINTABLE_SIZE);
            while (i > 0)
            {
              i--;
              ObExprObj E_obj = stack[--top];
              if(E_obj.get_type()==ObDecimalType)
              {
                ObDecimal od=E_obj.get_decimal();
                int64_t length=0;
                length=od.to_string(buf[i], MAX_PRINTABLE_SIZE);
                ObString os;
                os.assign_ptr(buf[i],static_cast<int32_t>(length));
                obj.set_decimal(os);
                obj.obj_copy(colum_val[i]);
                type = left[i].get_type();
                if(type != ObNullType )
                {
                  ex_type.set_type(type);
                  if(type == ObDecimalType)
                  {
                    if((obj.get_precision()-obj.get_vscale())>(left[i].get_precision()-left[i].get_scale()))
                    {
                      ex_type.set_scale(obj.get_vscale());
                      ex_type.set_precision(obj.get_precision());
                    }
                    else
                    {
                      ex_type.set_scale(left[i].get_scale());
                      ex_type.set_precision(left[i].get_precision());
                    }
                  }

                  res[i].set_varchar(ObString(MAX_PRINTABLE_SIZE,MAX_PRINTABLE_SIZE,ptr+(count-i-1)*MAX_PRINTABLE_SIZE));
                  if(OB_SUCCESS != (ret = obj_cast(obj,ex_type,res[i],temp)))
                  {
                    char b[256];
                    obj.to_string(b,255);
                    TBSYS_LOG(WARN,"failed to cast %s to type %d",b,type);
                  }
                  else
                  {
                    temp->obj_copy(colum_obj[i]);
                  }
                }
                else
                {
                  E_obj.to(colum_obj[i]);
                }
              }
              else
              {
                  type = left[i].get_type();
                  E_obj.to(obj);
                  E_obj.to(colum_val[i]);
                  if(type != ObNullType )
                  {
                      ex_type.set_type(type);
                      if(type == ObDecimalType)
                      {
                          ex_type.set_scale(left[i].get_scale());
                          ex_type.set_precision(left[i].get_precision());

                      }

                      res[i].set_varchar(ObString(MAX_PRINTABLE_SIZE,MAX_PRINTABLE_SIZE,ptr+(count-i-1)*MAX_PRINTABLE_SIZE));
                      if(OB_SUCCESS != (ret = obj_cast(obj,ex_type,res[i],temp)))
                      {
                          char b[256];
                          obj.to_string(b,255);
                          TBSYS_LOG(WARN,"failed to cast %s to type %d",b,type);
                      }
                      else
                      {
                         if(OB_SUCCESS != (ret = temp->obj_copy(colum_obj[i])))
                         {
                             TBSYS_LOG(WARN,"failed to copy obobj ret=%d",ret);
                             return ret;
                         }
                      }
                  }
                  else
                  {
                      if(OB_SUCCESS != (ret = obj.obj_copy(colum_obj[i])))
                      {
                          TBSYS_LOG(WARN,"failed to copy obobj ret=%d",ret);
                          return ret;
                      }
                  }
              }
            }
            ObRowkey columns;
            ObRowkey columnv;
            columns.assign(colum_obj,count);
            columnv.assign(colum_val,count);
              if(!is_not)
              {
                  for(int l = 0; l < count ; ++l)
                  {
                      if(colum_val[l].get_type() == ObNullType)
                          return ret;
                  }
              }
              else
              {
                  bool has_null = false;
                  for(int l = 0; l < count ; ++l)
                  {
                      if(colum_val[l].get_type() == ObNullType)
                      {
                          has_null = true;
                          break;
                      }
                  }
                  if(has_null)
                  {
                      bool ne = true;
                      if(1 == count || 2 == count)
                      {
                          if(ip->in_null_.size() >= (2 * count) -1 )
                              ne = false;
                      }
                      if(ne)
                      {
                          ObRowkey *inr = (ObRowkey *) ob_malloc(sizeof(ObRowkey),ObModIds::OB_IN_POSTFIX_EXPRESSION);
                          if(NULL != inr)
                          {
                              inr = new(inr) ObRowkey();
                          }
                          else
                          {
                              TBSYS_LOG(WARN,"alloc memory for in post-expression hash map failed");
                          }
                          if(OB_SUCCESS != (ret = columnv.deep_copy(*inr,ip->post_buf_)))
                          {
                              TBSYS_LOG(WARN, "fail to deep copy column,%d",ret);
                              return ret;
                          }
                          else
                          {
                              if(OB_SUCCESS != (ret = ip->in_null_.push_back(inr)))
                                  TBSYS_LOG(WARN,"failed to push null value into in_null vector for not in postexprssion");
                              return ret;

                          }
                      }

                  }

              }
            ObRowkey columns2;
            ObRowkey cvals;
            int hash_err = 0;

              if(OB_SUCCESS != (ret = columns.deep_copy(columns2,ip->post_buf_)))
              {
                TBSYS_LOG(WARN, "fail to deep copy column,%d",ret);
                return ret;
              }
              if(OB_SUCCESS != (ret = columnv.deep_copy(cvals,ip->post_buf_)))
              {
                  TBSYS_LOG(WARN, "fail to deep copy value,%d",ret);
                  return ret;
              }
               hash_err = ip->right_hash_.set(columns2,cvals,1);
                if (hash::HASH_INSERT_SUCC != hash_err && hash::HASH_OVERWRITE_SUCC != hash_err)
                {
                  ret = OB_ERROR;
                  TBSYS_LOG(WARN,"fail to set hash value of current group [err:%d]", hash_err);
                }



          }
          //TBSYS_LOG(ERROR,"dolphin:: exit out push_right_hash idx_i:%d,dim:%ld,f=%d",top,count,f);

          return ret;
        }
      //add:e
      int push_row(const ObExprObj *stack, int &top, const int count)
      {
        int i = 0;
        int ret = OB_SUCCESS;
        if (top < count)
        {
          TBSYS_LOG(WARN, "not enough row elements in stack. top=%d, count=%d", top, count);
          ret = OB_INVALID_ARGUMENT;
        }
        else
        {
          for (i = 0; i < count; i++)
          {

            if (OB_SUCCESS != (ret = vec_.push_back(stack[--top])))
            {
              TBSYS_LOG(WARN, "fail to push element to IN vector. ret=%d", ret);
            }

          }
        }
        return ret;
      }

      int get_result(ObExprObj *stack, int &top, ObExprObj &result,bool is_in)
      {
        int ret = OB_SUCCESS;
        int64_t left_start_idx = 0;
        int64_t right_start_idx = 0;
        int64_t right_elem_count = 0;
        int64_t width = 0;
        int64_t dim = 0;
        int64_t vec_top = 0;
        int64_t i = 0;
        ObExprObj cmp;
        ObExprObj width_obj;

        OB_ASSERT(NULL != stack);
        OB_ASSERT(top >= 2);

        if (OB_SUCCESS != (ret = stack[--top].get_int(right_elem_count)))
        {
          TBSYS_LOG(WARN, "fail to get_int from stack. top=%d, ret=%d", top, ret);
        }
        else if (OB_SUCCESS != (ret = stack[--top].get_int(dim)))
        {
          TBSYS_LOG(WARN, "fail to get_int from stack. top=%d, ret=%d", top, ret);
        }
        else
        {
          vec_top = vec_.count();
          switch (dim)
          {
            case 1:
              right_start_idx = vec_top - right_elem_count;
              left_start_idx  = right_start_idx - 1; // only 1 element
              if (OB_SUCCESS != (ret = check_is_in_row(result, left_start_idx, right_start_idx, right_elem_count, 1,is_in)))
              {
                TBSYS_LOG(WARN, "fail to check element in row. left_start_idx=%ld, right_start_idx=%ld, elem_count=%ld, width=%ld",
                    left_start_idx, right_start_idx, right_elem_count, 1L);
              }
              break;
            case 2:
              if (OB_SUCCESS != (ret = vec_.at(vec_top - 1, width_obj)))
              {
                TBSYS_LOG(WARN, "fail to get width_obj from array. vec_top=%ld, ret=%d", vec_top, ret);
              }
              else if (OB_SUCCESS != (ret = width_obj.get_int(width)))
              {
                TBSYS_LOG(WARN, "fail to get_int from stack. top=%d, ret=%d", top, ret);
              }
              else
              {
                right_start_idx = vec_top - right_elem_count - right_elem_count * width;
                left_start_idx  = right_start_idx - 1 - width;
                if (OB_SUCCESS != (ret = check_is_in_row(result, left_start_idx, right_start_idx, right_elem_count, width,is_in)))
                {
                  TBSYS_LOG(WARN, "fail to check element in row. left_start_idx=%ld, right_start_idx=%ld, elem_count=%ld, width=%ld",
                      left_start_idx, right_start_idx, right_elem_count, width);
                }
              }
              break;
            default:
              TBSYS_LOG(WARN, "invalid dim. dim=%ld", dim);
              ret = OB_ERR_UNEXPECTED;
              break;
          }
          if (OB_SUCCESS == ret)
          {
            for (i = left_start_idx; i < vec_top; i++)
            {
              vec_.pop_back();
            }
          }
        }
        return ret;
      }

      // consider cases: 10 in (null, 10) = true;
      // 10 in (null, 20) = null;
      // null in (10, 20) = null
      int check_is_in_row(ObExprObj &result, const int64_t left_start_idx,
          const int64_t right_start_idx, const int64_t right_elem_count, const int64_t width,bool in)
      {
        int64_t right_idx = 0;
        int64_t i = 0;
        int ret = OB_SUCCESS;
        ObExprObj left, right, cmp;
        bool r1 = false, r2 = false;
        //del dolphin[in post-expression optimization]@20151221
        /*bool is_in = false;
        bool has_null = false;*/
        //del:e
        for (right_idx = right_start_idx;
            OB_SUCCESS == ret && right_idx < right_start_idx + right_elem_count * width;
            right_idx += width)
        {
          //del dolphin[in post-expression optimization]@20151221
          //is_in = true;
          if(in)
          {
              r2 = true;
          }
          else
          {
              r2 = false;
          }
          for (i = 0; OB_SUCCESS == ret && i < width; i++)
          {
            if (OB_SUCCESS == (ret = vec_.at(left_start_idx + i,left)) && OB_SUCCESS == (ret= vec_.at(right_idx + i, right)))
            {
              //modify dolphin[in post-expression optimization]@20151221
              /*
              if (cmp.is_true())
              {
                // go forward, try to match next in current row
              }
              else // is_false or is_null
              {
                if (left.is_null() && right.is_null())
                {
                  // skip. null in null = true (special case)
                }
                else
                {
                  is_in = false;
                  if (cmp.is_null())
                  {
                    has_null = true;
                  }
                  break;
                }
              }*/
              if(left.is_null())
              {
                  result.set_bool(false);
                  return ret;
              }
              left.eq(right, cmp);
              if(in)
              {
                  if(right.is_null())
                  {
                      r2 = false;
                      break;
                  }
                  else
                  {
                      if(cmp.is_null())
                      {
                          r2 = false;
                          break;
                      }
                      else if(cmp.is_true())
                      {
                          r1 = true;

                      }
                      else if(!cmp.is_true())
                      {
                          r1 = false;
                      }
                      r2 = r1 && r2;
                      if(!r2)
                          break;
                      else
                          continue;
                  }

              }
              else
              {
                  if(right.is_null())
                  {
                      r1 = false;

                  }
                  else
                  {
                      if(cmp.is_null())
                      {
                          r2 = false;
                          break;
                      }
                      else if(cmp.is_true())
                      {
                          r1 = false;
                      }
                      else if(!cmp.is_true())
                      {
                          r1 = true;
                      }
                  }
                  r2 = r1 || r2;
                  if(r2)
                      break;
                  else
                      continue;
              }
            }
            else
            {
              TBSYS_LOG(WARN, "fail to get element from array. ret=%d. vec.count=%ld, width[%ld], i[%ld], left_idx[%ld], right_idx[%ld]" ,
                  ret, vec_.count(), width, i, left_start_idx + i, right_idx + i);
            }
          }
          if(in)
          {
              if(r2)
              {
                  result.set_bool(true);
                  return ret;
              }
              else
              {
                  result.set_bool(false);
              }
          }
          else
          {
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
          //del dolphin[in post-expression optimization]@20151221
        /*  if (true == is_in)
          {
            break; // no need to search more elements
          }*/
          //del:e
        }
        //del dolphin[in post-expression optimization]@20151221
        /*if (is_in)
          {
              result.set_bool(true);
          }
          else if (has_null)
          {
              result.set_null();
          }
          else
          {
              result.set_bool(false);
          }*/
        //del:e
        return ret;
      }
    };

    struct ObPostExprExtraParams
    {
      bool did_int_div_as_double_;
      int32_t operand_count_;
      ObInRowOperator in_row_operator_;
      char varchar_buf_[OB_MAX_VARCHAR_LENGTH];
      ObStringBuf *str_buf_;
      //add dolphin[coalesce return type]@20151126
      ObObjType expr_type_;
      ObPostExprExtraParams()
        :did_int_div_as_double_(false), operand_count_(0), in_row_operator_(), str_buf_(NULL)
      {
      }
    };
    typedef int(*op_call_func_t)(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params);

    struct ObPostfixExpressionCalcStack
    {
      // should be larger than MAX_SQL_EXPRESSION_SYMBOL_COUNT(=256)
	  //mod peiouya [ENLARGE_STACK_SIZE] 20150311:b
      //static const int64_t STACK_SIZE = 512;
	  static const int64_t STACK_SIZE = 5120;
	  //mod end 20150311:e
      ObExprObj stack_[STACK_SIZE];
    };

    class ObPostfixExpression
    {
      public:
        enum ObPostExprNodeType {
          BEGIN_TYPE = 0,
          COLUMN_IDX,
          //del  peiouya [EXPIRE_INFO_NOT_ARITHMETIC_FIX] keep order as common::ObExpression 20160608:b
          //QUERY_ID,//add tianz [SubQuery_for_Instmt] [JHOBv0.1] 20140331 /*Exp: mark for sub_query*/
          //del  peiouya [EXPIRE_INFO_NOT_ARITHMETIC_FIX] keep order as common::ObExpression 20160608:e
          CONST_OBJ,
          PARAM_IDX,
          SYSTEM_VAR,
          TEMP_VAR,
          OP,
          CUR_TIME_OP,
          CUR_TIME_HMS_OP,//add liuzy [datetime func] 20150910
          CUR_DATE_OP,    //add liuzy [datetime func] 20150910
          UPS_TIME_OP,
          //add  peiouya [EXPIRE_INFO_NOT_ARITHMETIC_FIX] keep order as common::ObExpression 20160608:b
          QUERY_ID,//add tianz [SubQuery_for_Instmt] [JHOBv0.1] 20140331 /*Exp: mark for sub_query*/
          //del  peiouya [EXPIRE_INFO_NOT_ARITHMETIC_FIX] keep order as common::ObExpression 20160608:e
          END, /* postfix expression terminator */
          END_TYPE
        };

     public:
        ObPostfixExpression();
        ~ObPostfixExpression();
        ObPostfixExpression& operator=(const ObPostfixExpression &other);
        void set_int_div_as_double(bool did);

        // add expression object into array directly,
        // user assure objects of postfix expr sequence.
        int add_expr_obj(const common::ObObj &obj);
        //add dolphin[coalesce return type]@20151126:b
        void set_expr_type(const ObObjType& type);
        ObObjType get_expr_type() const;
        //add:e
        int add_expr_item(const ExprItem &item);
        int add_expr_item_end();
        //add fanqiushi_index
        int change_tid(uint64_t& array_index);
        int get_cid(uint64_t& cid);
        ObObj& get_expr_by_index(int64_t index);
        //add:e
        //add wanglei [second index fix] 20160425:b
         int get_tid_indexs( ObArray<uint64_t> & index_list);
        //add wanglei [second index fix] 20160425:e
        void reset(void);

        /* 将row中的值代入到expr计算结果 */
        //mod peiouya peiouya [IN_TYPEBUG_FIX] 20151225:b
        ////mod tianz [SubQuery_for_Instmt] [JHOBv0.1] 20140508:b
        ///*Exp: calc will call static function to do real work,
        // *other params need transfer to static function as input parameters
        // */
        ////int calc(const common::ObRow &row, const ObObj *&result);
        //int calc(const common::ObRow &row, const ObObj *&result,hash::ObHashMap<common::ObRowkey,common::ObRowkey,common::hash::NoPthreadDefendMode>* hash_map,bool second_check );
        ////mod 20140508:e
        //add wanglei [case when fix] 20160516:b
        int calc(const common::ObRow &row, const ObObj *&composite_val,
                 hash::ObHashMap<common::ObRowkey,common::ObRowkey,common::hash::NoPthreadDefendMode>* hash_map,
                 bool is_hashmap_contain_null,  //add peiouya [IN_AND NOT_IN_WITH_NULL_BUG_FIX] 20160518
                 common::ObArray<ObObjType> * hash_map_pair_type_desc,
                 bool second_check ,int &idx_, int &idx_i_);
        //add wanglei [case when fix] 20160516:e
        int calc(const common::ObRow &row, const ObObj *&composite_val,
                    hash::ObHashMap<common::ObRowkey,common::ObRowkey,common::hash::NoPthreadDefendMode>* hash_map,
                    bool is_hashmap_contain_null,  //add peiouya [IN_AND NOT_IN_WITH_NULL_BUG_FIX] 20160518
                    common::ObArray<ObObjType> * hash_map_pair_type_desc,
                    bool second_check );
        //mod 20151225:e
        /*
         * 判断表达式类型：是否是const, column_index, etc
         * 如果表达式类型为column_index,则返回index值
         */
        //add wanglei [to date optimization] 20160328
        int get_result (ObExprObj & result);
        //add wanglei:e
        //add dragon [Bugfix lmz-2017-2-6] 2017-2-7 b
        /**
         * @brief is_op_is_null
         * 判断是否是 A.a IS NULL表达式 2是参数个数
         * COLUMN_IDX | NULL | OP | T_OP_IS | 2
         * @return
         */
        bool is_op_is_null();
        //add dragon e
        int is_const_expr(bool &is_type) const;
        int is_column_index_expr(bool &is_type) const;
        int get_column_index_expr(uint64_t &tid, uint64_t &cid, bool &is_type) const;
        int merge_expr(const ObPostfixExpression &expr1, const ObPostfixExpression &expr2, const ExprItem &op);
        bool is_empty() const;
        /*add by wanglei [semi join] 20151106*/
        bool is_otherjoin_cond(ExprItem::SqlCellInfo &c1, ExprItem::SqlCellInfo &c2) const;
        bool is_LT_OR_LE_cond() const;
        bool is_GT_OR_GE_cond() const;
        bool is_equijoin_cond(ExprItem::SqlCellInfo &c1, ExprItem::SqlCellInfo &c2) const;
        // NB: Ugly interface, wish it will not exist in futher. In fact, this interfaces should not appears in post-expression
        // Since it so ugly, we do not change is_simple_between() and is_simple_in_expr() because their out values are not used so far.
        // val_type: 0 - const, 1 - system variable,
        bool is_simple_condition(bool real_val, uint64_t &column_id, int64_t &cond_op, ObObj &const_val, ObPostExprNodeType *val_type = NULL) const;
        //add fanqiushi_index
        bool is_have_main_cid(uint64_t main_column_id);
        int find_cid(uint64_t &cid);
        bool is_all_expr_cid_in_indextable(uint64_t index_tid,const ObSchemaManagerV2 *sm_v2);
        bool is_this_expr_can_use_index(uint64_t &index_tid,uint64_t main_tid,const ObSchemaManagerV2 *sm_v2);
        int get_all_cloumn(ObArray<uint64_t> &column_index);
        //add:e
        //add wanglei [second index fix] 20160425:b
        bool is_expr_has_more_than_two_columns();
        //add wanglei [second index fix] 20160425:e
        bool is_simple_between(bool real_val, uint64_t &column_id, int64_t &cond_op, ObObj &cond_start, ObObj &cond_end) const;
        bool is_simple_in_expr(bool real_val, const ObRowkeyInfo &info, ObIArray<ObRowkey> &rowkey_array,
            common::PageArena<ObObj,common::ModulePageAllocator> &allocator) const;
        //add lijianqiang [sequence] 20150414:b
        ObSEArray<ObObj, 64>& get_expr_array();
        int64_t get_expr_obj_size();
        //add 20150414:e
        // add  by  fyd  [PrefixKeyQuery_for_INstmt] 2014.3.24:b
        // 用于判断当前的 IN 表达式中书否有主键前缀，如果有，返回true，同时将包含的主键前缀信息以及相关主键的具体指，
        //保存到 rowkey_array,因此注意这里的rowkey_array可能不包含所有的主键信息返回的信息可以当做构建新的 rowkey range 的参数
        bool is_in_expr_with_ex_rowkey(bool real_val, const ObRowkeyInfo &info, ObIArray<ObRowkey> &rowkey_array,
                  common::PageArena<ObObj,common::ModulePageAllocator> &rowkey_objs_allocator) const;
        //add:end
        inline void set_owner_op(ObPhyOperator *owner_op);
        inline ObPhyOperator* get_owner_op();
        static const char *get_sys_func_name(enum ObSqlSysFunc func_id);
        static int get_sys_func_param_num(const common::ObString& name, int32_t& param_num);
        // print the postfix expression
        int64_t to_string(char* buf, const int64_t buf_len) const;

        //add dolphin[in post-expression optimization]@20150915
        //int print_expr(int i = 0);
        //int print_in_hash_param(int i = 0);
        void free_post_in();
       bool post_in_ada_is_null();

      int check_in_hash(ObExprObj& result,int t = 0,int64_t v = T_OP_IN);
        //add:e
		//add tianz [SubQuery_for_Instmt] [JHOBv0.1] 20140508:b

        /*Exp:used before specific value replace the mark of sub_query,
		* pop other information of expression out to temp expression ,
		* then remove the mark of sub_query
		* @param sub_query_index[in] sub_query position in current expr
		*/
        int delete_sub_query_info(int sub_query_index);

		/*Exp:used for specific value replace the mark of sub_query,
		* push specific value back to current expression
		* @param obj_value[in] specific value which will replace the mark of sub_query
		*/
        int add_expr_in_obj(ObObj &obj_value);

		/*Exp:used after specific value replace the mark of sub_query,
		* push other information of expression back to modified expression
		*/
        int complete_sub_query_info();

        /*Exp: get different ObPostExprNodeType's params number
		* @param idx[in] current position in expr
		* @param type[in] current ObPostExprNodeType
		* @retrun current ObPostExprNodeType's params number
		*/
        int64_t get_type_num(int64_t idx,int64_t type);

		/*Exp:get sub_query selected column number
		* @param sub_query_index[in] ,sub_query's number in current expression
		* @param num[out],sub_query selected column number
		* @param special_sub_query[out],sub_query is a separate expression, not belong to in or not in expression
		*/
        int get_sub_query_column_num(int sub_query_index,int64_t &num,bool &special_sub_query);
        //add xionghui [fix equal subquery bug] 20150122 b:
        int get_sub_query_column_num_new(int sub_query_index,int64_t &num,bool &special_sub_query,bool &special_case);
        //add e:
		/*Exp:pass bloomfilter address to bloomfilter pointer
		* @param bloom_filter[in]
		*/
        void set_has_bloomfilter(ObBloomFilterV1 * bloom_filter){bloom_filter_ = bloom_filter;};

		/*Exp:check whether current expression satisfy the condition of use bloomfilter */
        bool is_satisfy_bloomfilter();// add 20140523
		//add 20140508:e
		//add gaojt [Insert_Subquery_Function] [JHOBv0.1] 20140715:b
		
		/*Exp:determin whether the expr is sub_query for insert,
		* used for rpc_scan get filter
		*/
		bool is_rows_filter();
		
		/*Exp:used before specific value replace the mark of sub_query, 
		* pop other information of expression out to temp expression ,
		* then remove the mark of sub_query
		*/
		int delete_sub_query_info();
		
		/*Exp:determin whether the expr is sub_query for insert,
		* used for strategy store expr 
		*/
		bool is_in_sub_query_expr(const ObRowkeyInfo &info) const;
		//add 20140715:e
        NEED_SERIALIZE_AND_DESERIALIZE;
      private:
        class ExprUtil
        {
          public:
          static inline bool is_column_idx(const ObObj &obj);
          static inline bool is_const_obj(const ObObj &obj);
          static inline bool is_op(const ObObj &obj);
          static inline bool is_end(const ObObj &obj);

          static inline bool is_value(const ObObj &obj, int64_t value);
          static inline bool is_op_of_type(const ObObj &obj, ObItemType type);
		  static inline bool is_sub_query(const ObObj &obj);   //add gaojt [Insert_Subquery_Function] [JHOBv0.1] 20140715
        };
      private:
        ObPostfixExpression(const ObPostfixExpression &other);
        static inline int nop_func(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params);
        static inline int reserved_func(const ObExprObj &obj1, const ObExprObj &obj2, ObExprObj &result);
        /* compare function list:
         * >   gt_func
         * >=  ge_func
         * <=  le_func
         * <   lt_func
         * ==  eq_func
         * !=  neq_func
         */
        static inline int do_gt_func(const ObExprObj &obj1, const ObExprObj &obj2, ObExprObj &result);
        static inline int gt_func(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params);
        static inline int do_ge_func(const ObExprObj &obj1, const ObExprObj &obj2, ObExprObj &result);
        static inline int ge_func(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params);
        static inline int do_lt_func(const ObExprObj &obj1, const ObExprObj &obj2, ObExprObj &result);

        static inline int do_le_func(const ObExprObj &obj1, const ObExprObj &obj2, ObExprObj &result);
        static inline int le_func(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params);
        static inline int lt_func(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params);
        static inline int do_eq_func(const ObExprObj &obj1, const ObExprObj &obj2, ObExprObj &result);
        static inline int eq_func(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params);
        static inline int do_neq_func(const ObExprObj &obj1, const ObExprObj &obj2, ObExprObj &result);
        static inline int neq_func(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params);
        static inline int do_is_not_func(const ObExprObj &obj1, const ObExprObj &obj2, ObExprObj &result);
        static inline int is_not_func(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params);
        static inline int do_is_func(const ObExprObj &obj1, const ObExprObj &obj2, ObExprObj &result);
        static inline int is_func(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params);
        static inline int add_func(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params);
        static inline int sub_func(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params);
        static inline int mul_func(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params);
        static inline int div_func(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params);
        static inline int mod_func(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params);
        static inline int and_func(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params);
        static inline int or_func(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params);
        static inline int minus_func(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params);
        static inline int plus_func(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params);
        static inline int not_func(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params);
        static inline int like_func(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params);
        static inline int in_func(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params);
        static inline int not_in_func(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params);
        static inline int not_like_func(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params);
        static inline int btw_func(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params);
        static inline int not_btw_func(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params);
        static inline int sys_func_length(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params);
        static inline int sys_func_substr(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params);
        static inline int sys_func_cast(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params);
        static inline int sys_func_current_timestamp(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params);
        static inline int sys_func_cur_user(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params);
        static inline int sys_func_trim(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params);
        static inline int sys_func_lower(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params);
        static inline int sys_func_upper(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params);
        static inline int sys_func_coalesce(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params);
        static inline int sys_func_greatest(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params);
        static inline int sys_func_least(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params);
        static inline int concat_func(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params);
        static inline int left_param_end_func(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params);
        //add  peiouya [EXPIRE_INFO_NOT_ARITHMETIC_FIX] 20160608:b
        static inline int cast_thin_func(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params);
        //add  peiouya [EXPIRE_INFO_NOT_ARITHMETIC_FIX] 20160608:e
        static inline int row_func(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params);
        static inline int arg_case_func(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params);
        static inline int case_func(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params);
        static inline int sys_func_hex(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params);
        static inline int sys_func_unhex(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params);
        static inline int sys_func_ip_to_int(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params);
        static inline int sys_func_int_to_ip(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params);
        //add wanglei [last_day] 20160311:b
        static inline int sys_func_last_day(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params);
        //add wanglei [to_char] 20160311:b
        static inline int sys_func_to_char(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params);
        //add wanglei [to_date] 20160311:b
        static inline int date_printf(char * date_buff, const char* format, ...);
        static int parse_date(char * date,char * date_str,int date_length,int date_str_length,ObExprObj &ob_date);
        static inline int sys_func_to_date(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params);
        //add wanglei:e
        //add wanglei [to_timestamp] 20160311:b
        static int parse_timestamp(char * date,char * date_str,int date_length,int date_str_length,ObExprObj &ob_date);
        static inline int sys_func_to_timestamp(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params);
        //add qianzhaoming [decode] 20151029:e
        static inline int sys_func_decode(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params);
        //add :e
        //add xionghui [floor and ceil] 20151214:b
        static inline int sys_func_floor(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params);
        static inline int sys_func_ceil(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params);
        //add 20151214:e
        //add xionghui [round] 20150126:b
        static inline int sys_func_round(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params);
        //add 20150126 e:
        //add wuna [datetime func] 20150828:b
        static inline int sys_func_year(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params);
        static inline int sys_func_month(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params);
        static inline int sys_func_day(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params);
        static inline int sys_func_days(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params);
        static int datetime_calc(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params,int64_t op_type);
        static int calc_year_month(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params, int64_t value ,bool is_year);
        static int calc_day_hour_minute_second_microsecond(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params, int64_t value ,time_unit tm_unit);
        static inline int sys_func_date_add_year(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params);
        static inline int sys_func_date_sub_year(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params);
        static inline int sys_func_date_add_month(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params);
        static inline int sys_func_date_sub_month(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params);
        static inline int sys_func_date_add_day(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params);
        static inline int sys_func_date_sub_day(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params);
        static inline int sys_func_date_add_hour(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params);
        static inline int sys_func_date_sub_hour(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params);
        static inline int sys_func_date_add_minute(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params);
        static inline int sys_func_date_sub_minute(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params);
        static inline int sys_func_date_add_second(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params);
        static inline int sys_func_date_sub_second(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params);
        static inline int sys_func_date_add_microsecond(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params);
        static inline int sys_func_date_sub_microsecond(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params);
        //add 20150828:e
        //add liuzy [datetime func] 20150901:b
        static inline int sys_func_hour(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params);
        static inline int sys_func_minute(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params);
        static inline int sys_func_second(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params);
        //add 20150901:e
        //add lijianqiang [replace_function]20151102:b
        static inline int sys_func_replace(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params);
        //add 20151102:e
        //mod peiouya peiouya [IN_TYPEBUG_FIX] 20151225:b
        ////add tianz [SubQuery_for_Instmt] [JHOBv0.1] 20140508:b

        /*Exp: process in sub_query prepare check
         * @param bloom_filter[in]
         * @param hash_map[in]
         * @param sub_query_idx[in,out] ,used for or expression which may have more than one sub_query
         * @param second_check[in] , used for mark hashmap check
         */
        //static inline int in_sub_query_func(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params,ObBloomFilterV1* bloom_filter,hash::ObHashMap<common::ObRowkey,common::ObRowkey,common::hash::NoPthreadDefendMode>* hash_map, int &sub_query_idx,bool second_check  );
        static inline int in_sub_query_func(ObExprObj *stack_i, int &idx_i, ObExprObj &result,
                                                               const ObPostExprExtraParams &params,
                                                               ObBloomFilterV1* bloom_filter,
                                                               hash::ObHashMap<common::ObRowkey,common::ObRowkey,common::hash::NoPthreadDefendMode>* hash_map,
                                                               bool is_hashmap_contain_null,  //add peiouya [IN_AND NOT_IN_WITH_NULL_BUG_FIX] 20160518
                                                               common::ObArray<ObObjType> * hash_map_pair_type_desc,
                                                               int &sub_query_idx,
                                                               bool second_check);


        /*Exp: process in sub_query, do real work
         * @param bloom_filter[in]
         * @param hash_map[in]
         * @param sub_query_idx[in,out] ,used for or expression which may have more than one sub_query
         * @param second_check[in] , used for mark hashmap check
         */
        //static int get_result(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params,common::ObBloomFilterV1 * bloom_filter,hash::ObHashMap<common::ObRowkey,common::ObRowkey,common::hash::NoPthreadDefendMode>* hash_map, int &sub_query_idx,bool second_check );
        static int get_result(ObExprObj *stack_i, int &idx_i, ObExprObj &result,
                                      const ObPostExprExtraParams &params,
                                      common::ObBloomFilterV1 * bloom_filter,
                                      hash::ObHashMap<common::ObRowkey,common::ObRowkey,common::hash::NoPthreadDefendMode>* hash_map,
                                      bool is_hashmap_contain_null,  //add peiouya [IN_AND NOT_IN_WITH_NULL_BUG_FIX] 20160518
                                      common::ObArray<ObObjType> * hash_map_pair_type_desc,
                                      int &sub_query_idx,
                                      bool second_check);
        ////add 20140508:e
        //mod 20151225:e
        // 辅助函数，检查表达式是否表示const或者column index
        int check_expr_type(const int64_t type_val, bool &is_type, const int64_t stack_len) const;
        int get_sys_func(const common::ObString &sys_func, ObSqlSysFunc &func_type) const;
        int get_var_obj(ObPostExprNodeType type, const ObObj& expr_node, const ObObj*& val) const;

      private:
        static const int64_t DEF_STRING_BUF_SIZE = 64 * 1024L;
        static const int64_t BASIC_SYMBOL_COUNT = 64;
        static op_call_func_t call_func[T_MAX_OP - T_MIN_OP - 1];
        static op_call_func_t SYS_FUNCS_TAB[SYS_FUNC_NUM];
        static const char* const SYS_FUNCS_NAME[SYS_FUNC_NUM];
        static int32_t SYS_FUNCS_ARGS_NUM[SYS_FUNC_NUM];
        static const int32_t USE_HASH_NUM = 1;  //add dolphin[in post-expression optimization]@20160519:b
    public:
        typedef ObSEArray<ObObj, BASIC_SYMBOL_COUNT> ExprArray;
        //add wenghaixing for fix delete bug decimal key 2014/10/11
         ObObj& get_expr() ;
         int fix_varchar_and_decimal(uint32_t p,uint32_t s);
        //add e
         //add wenghaixing [secondary index expire info]20141229
         ObObj& find_expire_expr();
         //add e
         //add lbzhong[Update rowkey] 20151221:b
         int convert_to_equal_expr(const int64_t table_id, const int64_t column_id, const ObObj *&value);
         inline const ExprArray & get_expr_array() const
         {
           return expr_;
         }

         //add:e

      private:
        ExprArray expr_;
        ObExprObj *stack_;
        bool did_int_div_as_double_;
        ObObj result_;
        ObStringBuf str_buf_;
        ObPhyOperator *owner_op_;
        ObStringBuf calc_buf_;
		//add tianz [SubQuery_for_Instmt] [JHOBv0.1] 20140508:b
		//when delete sub_query mark ,temp_expr_ store other inforemation after this mark
		ExprArray temp_expr_;
        common::ObBloomFilterV1 *bloom_filter_;		   //bloom filter
		//add 20140508:e
		//add wenghaixing [secondary index expire info]20141229
		ObObj not_find_;
		//add e
        //add dolphin[in post-expression optimization]@20150915:b
         ObPostfixInAdaptor *post_in_ada_ ;
        //add dolphin[coalesce return type]@20151126
        ObObjType expr_type_;

    }; // class ObPostfixExpression
    //add dolphin[in post-expression optimization]@20150919:b
    inline bool ObPostfixExpression::post_in_ada_is_null()
  {
        return post_in_ada_ == NULL;
  };

    inline void ObPostfixExpression::free_post_in()
    {
      if(NULL != post_in_ada_)
      {
        post_in_ada_->~ObPostfixInAdaptor();
        ob_free(post_in_ada_);
        post_in_ada_ = NULL;
      }
    }
    //add:e
    inline void ObPostfixExpression::set_int_div_as_double(bool did)
    {
      did_int_div_as_double_ = did;
    }
    inline bool ObPostfixExpression::is_empty() const
    {
      int64_t type = 0;
      return 0 == expr_.count()
        || (1 == expr_.count()
            && common::OB_SUCCESS == expr_[0].get_int(type)
            && END == type);
    }
    inline void ObPostfixExpression::reset(void)
    {

      //str_buf_.reset();
      str_buf_.clear();
      expr_.clear();
      owner_op_ = NULL;
      calc_buf_.clear();
	  //add tianz [SubQuery_for_Instmt] [JHOBv0.1] 20140508:b
      temp_expr_.clear();
      bloom_filter_ = NULL;
	  //add 20140508:e
      //add dolphin[in post-expression optimization]@20150925:b
      if(NULL != post_in_ada_)
      {
        post_in_ada_->~ObPostfixInAdaptor();
        ob_free(post_in_ada_);
        post_in_ada_ = NULL;
      }
      //add:e
    }
    inline void ObPostfixExpression::set_owner_op(ObPhyOperator *owner_op)
    {
      owner_op_ = owner_op;
    }
    inline ObPhyOperator* ObPostfixExpression::get_owner_op()
    {
      return owner_op_;
    }
  } // namespace commom
}// namespace oceanbae

#endif
