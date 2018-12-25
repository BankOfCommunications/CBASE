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
 *     - 后缀表达式求值，可用于复合列等需要支持复杂求值的场合
 *
 */



#ifndef OCEANBASE_COMMON_OB_POSTFIX_EXPRESSION_H_
#define OCEANBASE_COMMON_OB_POSTFIX_EXPRESSION_H_
#include "ob_string.h"
#include "ob_expression.h"
#include "ob_array_helper.h"
#include "ob_object.h"
#include "hash/ob_hashmap.h"
#include "ob_result.h"
#include "common/ob_string_buf.h"
#include "ob_expr_obj.h"
namespace oceanbase
{
  namespace common
  {

     /*
     *  中缀表达式=>后缀表达式 解析器
     *  reference: http://totty.iteye.com/blog/123252
     */
    class ObScanParam;
    class ObCellArray;
    class ObExpressionParser
    {
      class ObExpressionGrammaChecker
      {
        /* TODO: 类型检查 */
      };

      public:
      ObExpressionParser()
      {
        symbols_.init(OB_MAX_COMPOSITE_SYMBOL_COUNT, sym_list_);
        stack_.init(OB_MAX_COMPOSITE_SYMBOL_COUNT, sym_stack);
      }

      ~ObExpressionParser(){}

      //add peiouya [Expire_condition_modify] 20140909:b
      /**expr:目的是检查列明的前后衔接字符是否合法
        */
      int check_sys_day(const int code)
      {
          int err = OB_SUCCESS;
          switch(code)
          {
          case peCodeLessThan:
          case peCodeLessOrEqual:
          case peCodeGreaterThan:
          case peCodeGreaterOrEqual:
            err = OB_SUCCESS;
            break;
          default:
            err = OB_ERROR;
            TBSYS_LOG(ERROR, "invalid expire_info-----$SYS_DAY, include invalid param!");
            break;
          }
          return err;
      }
      //add 20140909:b
      //mod  peiouya [EXPIRE_INFO_NOT_ARITHMETIC_FIX] 20160608:b
      //int parse(const ObString &infix_string, ObArrayHelper<ObObj> &postfix_objs)
      int parse(const ObString &infix_string, ObArrayHelper<ObObj> &postfix_objs, bool is_expire_info = false)
      //mod  peiouya [EXPIRE_INFO_NOT_ARITHMETIC_FIX] 20160608:e
      {
        reset();
        is_expire_info_ = is_expire_info;  //add  peiouya [EXPIRE_INFO_NOT_ARITHMETIC_FIX] 20160608

        int err = OB_SUCCESS;
        int pos = 0;  // char position in infix_str
        const char *infix_str = infix_string.ptr();
        infix_max_len_ = infix_string.length();
        result_ = &postfix_objs;
        //TBSYS_LOG(INFO, "decode infix_str=%.*s", infix_max_len_, infix_str);
        //add peiouya [Expire_condition_modify] 20140909:b
        bool is_sys_day = false;
        //add 20140909:e

        // 分词
        while(pos < infix_max_len_ && symbols_.get_array_index() < symbols_.get_array_size() && OB_SUCCESS == err)
        {
          Symbol sym;
          if(OB_SUCCESS != (err = get_sym(infix_str, pos, sym)))
          {
            if (OB_ITER_END != err)
            {
              TBSYS_LOG(WARN, "get sym err");
            }
            else //if (err == OB_ITER_END)
            {
              err = OB_SUCCESS;
              break;
            }
          }
          else
          {
            if (symbols_.push_back(sym))
            {
              err = OB_SUCCESS;
              TBSYS_LOG(DEBUG, "sym %ld=[type:%d][%.*s]",
                  symbols_.get_array_index(),
                  sym.type,
                  sym.length,
                  sym.value);
            }
            else
            {
              TBSYS_LOG(WARN, "error adding symbol to symbol list");
              err = OB_ERROR;
            }
            //add peiouya [Expire_condition_modify] 20140909:b
            if (DATETIME == sym.type)
            {
              char tmp_buf[127+1];
              if (sym.length > 127 || sym.length <= 0)
              {
                err = OB_ERROR;
                TBSYS_LOG(WARN, "invalid param. length=%d, str=%s", sym.length, sym.value);
              }
              else
              {
                strncpy(tmp_buf, sym.value, sym.length);
                tmp_buf[sym.length] = '\0';

                if((NULL != strchr(tmp_buf, '-')) && (NULL == strchr(tmp_buf, ' ')) )
                {
                  is_sys_day = true;
                }
              }
            }
            //add 20140909:e
          }
        }

        //add peiouya [Expire_condition_modify] 20140909:b
        //精度为天的数据类型检查
        //格式：列名 (<|<=|>|>=) [+|-]数字 +|- DATE (AND 列名 (<|<=|>|>=) [+|-]数字 +|- DATE)
        //表达式内只能出现不超过1个AND
        //例如：name < +9 + DATE 或 name < DATE + 9 或 +9 + DATE > name
        int size = 0;
        const Symbol *symbol = NULL;
        if (is_sys_day && OB_SUCCESS == err)
        {
          for (size = 0; size < symbols_.get_array_index() && OB_SUCCESS == err; size++)
          {
              symbol = symbols_.at(size);
              switch(symbol->type)
              {
                case OPERATOR:
                  if (0 == strncmp("+", symbol->value, symbol->length) || 0 == strncmp("-", symbol->value, symbol->length)
                      || 0 == strncmp("<", symbol->value, symbol->length) || 0 == strncmp("<=", symbol->value, symbol->length)
                      || 0 == strncmp(">", symbol->value, symbol->length) || 0 == strncmp(">=", symbol->value, symbol->length))
                  {
                    err = OB_SUCCESS;
                  }
                  else
                  {
                    err = OB_ERROR;
                    TBSYS_LOG(ERROR, "expire_info--$SYS_DAY,only support operator [+|-|<|<=|>|>=]!");
                  }
                  break;
                case COLUMN_NAME:
                case DATETIME:
                case NUMBER:
                case HEX_NUMBER:
                  err = OB_SUCCESS;
                  break;
                case KEYWORD:
                  if ((strlen("NOT") == (size_t)symbol->length && 0 == strncasecmp("NOT", symbol->value, symbol->length))
                      ||(strlen("IS") == (size_t)symbol->length && 0 == strncasecmp("IS", symbol->value, symbol->length))
                      ||(strlen("LIKE") == (size_t)symbol->length && 0 == strncasecmp("LIKE", symbol->value, symbol->length))
                      ||(strlen("NULL") == (size_t)symbol->length && 0 == strncasecmp("NULL", symbol->value, symbol->length)))
                  {
                    err = OB_ERROR;
                    TBSYS_LOG(ERROR, "expire_info--$SYS_DAY can only include [AND|OR]!, but sym %d =[type:%d][%.*s]", size,
                        symbol->type,
                        symbol->length,
                        symbol->value);
                  }
                  else
                  {
                    err = OB_SUCCESS;
                  }
                  break;
                default:
                  err = OB_ERROR;
                  TBSYS_LOG(ERROR, "invalid expire_info-----$SYS_DAY, include invalid param! sym %d =[type:%d][%.*s]", size,
                      symbol->type,
                      symbol->length,
                      symbol->value);
                  break;
              }
          }
        }
        //add 20140909:e
        // 语义
        // (1) 字符串转内码
        // (2) 错误检查
        int i = 0;
        const Symbol *prev_symbol = NULL;
        for (i = 0; i < symbols_.get_array_index() && OB_SUCCESS == err; i++)
        {
          prev_symbol = (i == 0) ? NULL : symbols_.at(i - 1);

          //modify peiouya [Expire_condition_modify] 20140909:b
          //if (OB_SUCCESS != (err = encode_sym(*symbols_.at(i), prev_symbol)))
          if (OB_SUCCESS != (err = encode_sym(*symbols_.at(i), prev_symbol, is_sys_day)))
          //modify 20140909:e
          {
            TBSYS_LOG(WARN, "encode symbol failed. sym %d =[type:%d][%.*s]", i,
                symbols_.at(i)->type,
                symbols_.at(i)->length,
                symbols_.at(i)->value);
            break;
          }
          else
          {

            TBSYS_LOG(DEBUG, "encode symbol OK. sym %d =[type:%d][%.*s]", i,
                symbols_.at(i)->type,
                symbols_.at(i)->length,
                symbols_.at(i)->value);
          }
        }

        // 语义Fixup
        // 负号


        //add peiouya [Expire_condition_modify] 20140909:b
        //精度为天的格式检查，只检查COLUMN_NAME的前后数据类型
        //仅当COLUMN_NAME在<、<=、>、>=一侧单独出现才认为合法，即name不能用括号括起来，也不能进行运算，name 前后只能跟<、<=、>、>=
        //例如 name代表列名：name < **  合法，(name) < ** 非法，name < ** and name > ** 合法
        if (is_sys_day && OB_SUCCESS == err)
        {
          for (size = 0; size < symbols_.get_array_index() && OB_SUCCESS == err; size++)
          {
            symbol = symbols_.at(size);
            switch(symbol->code)
            {
              case peCodeAdd:
              case peCodeSub:
              case peCodeLessThan:
              case peCodeLessOrEqual:
              case peCodeGreaterThan:
              case peCodeGreaterOrEqual:
              case peCodeAnd:
              case peCodeOr:
              case peCodeColumnName:
              case peCodeDateTime:
                 err = OB_SUCCESS;
                 break;
              default:
                 err = OB_ERROR;
                 TBSYS_LOG(ERROR, "invalid expire_info-----$SYS_DAY, include invalid param!");
                 break;
            };
            if (COLUMN_NAME == symbol->type && OB_SUCCESS == err)
            {
              if (0 == size)
              {
                if (OPERATOR == symbols_.at(size + 1)->type)
                {
                  err = check_sys_day(symbols_.at(size + 1)->code);
                }
                else
                {
                  err = OB_ERROR;
                  TBSYS_LOG(ERROR, "invalid expire_info-----$SYS_DAY, include invalid param!");
                }
              }
              else if (size + 1 >= symbols_.get_array_index())
              {
                if (OPERATOR == symbols_.at(size - 1)->type)
                {
                  err = check_sys_day(symbols_.at(size - 1)->code);
                }
                else
                {
                  err = OB_ERROR;
                  TBSYS_LOG(ERROR, "invalid expire_info-----$SYS_DAY, include invalid param!");
                }
              }
              else
              {
                if((OB_SUCCESS == check_sys_day(symbols_.at(size - 1)->code) && (peCodeAnd == symbols_.at(size + 1)->code ||peCodeOr == symbols_.at(size + 1)->code))
                   ||(OB_SUCCESS == check_sys_day(symbols_.at(size + 1)->code) && (peCodeAnd == symbols_.at(size - 1)->code || peCodeOr == symbols_.at(size - 1)->code)))
                {
                  err = OB_SUCCESS;
                }
                else
                {
                  err = OB_ERROR;
                  TBSYS_LOG(ERROR, "invalid expire_info-----$SYS_DAY, include invalid param!");
                }
              }
            }
          }
        }
        //add peiouya [Expire_condition_modify] 20140913:b
        if (is_sys_day && OB_SUCCESS == err)
        {
          Symbol *cur_symbol = NULL;
          Symbol *next_symbol = NULL;
          Symbol *next_next_symbol = NULL;
          for (size = 0; size < symbols_.get_array_index() && OB_SUCCESS == err; size++)
          {
            cur_symbol = symbols_.at(size);
            if(peCodeDateTime == cur_symbol->code)
            {
              if(size + 1 >= symbols_.get_array_index())
              {
                next_symbol = NULL;
              }
              else
              {
                next_symbol = symbols_.at(size + 1);
              }
              if(NULL != next_symbol && (peCodeAdd == next_symbol->code || peCodeSub == next_symbol->code))
              {
                if(size + 2 >= symbols_.get_array_index())
                {
                  next_next_symbol = NULL;
                }
                else
                {
                  next_next_symbol = symbols_.at(size + 2);
                }
                if(NULL != next_next_symbol && peCodeDateTime == next_next_symbol->code)
                {
                  ObPreciseDateTime cur_value = 0;
                  ObPreciseDateTime next_value = 0;
                  cur_symbol->encode.get_precise_datetime(cur_value);
                  next_next_symbol->encode.get_precise_datetime(next_value);
                  if(peCodeAdd == next_symbol->code)
                  {
                    next_value += cur_value;
                    next_next_symbol->encode.set_precise_datetime(next_value);
                    cur_symbol->code = INVALID;
                    next_symbol->code = INVALID;
                  }
                  else
                  {
                    next_value = cur_value - next_value;
                    next_next_symbol->encode.set_precise_datetime(next_value);
                    cur_symbol->code = INVALID;
                    next_symbol->code = INVALID;
                  }
                }
                else
                {
                  err = OB_ERROR;
                  TBSYS_LOG(ERROR, "invalid expire_info-----$SYS_DAY!");
                }
              }
              else if(NULL != next_symbol &&(peCodeColumnName == next_symbol->code || peCodeDateTime == next_symbol->code))
              {
                err = OB_ERROR;
                TBSYS_LOG(ERROR, "invalid expire_info-----$SYS_DAY!");
              }
            }
          }
        }
        //add 20140913:e
        // 转换
        if(OB_SUCCESS == err && OB_SUCCESS == (err = infix2postfix()))
        {
          //TBSYS_LOG(INFO, "successful in converting infix expression to postfix expression");
        }
        else
        {
          TBSYS_LOG(WARN, "fail to convert infix expression to postfix expression");
        }

        return err;
      }

    //add wenghaixing [secondary index create fix]20141226
      int64_t get_sym_type(int64_t i)
      {
          Symbol* sym= symbols_.at(i);
          if(sym==NULL)
          return 0;
          else
          return sym->type;
      }

      //add e

      private:

      enum Type{
        OPERATOR = 1,
        STRING,
        HEX_STRING,
        DATETIME,
        COLUMN_NAME,
        NUMBER,
        KEYWORD,
        HEX_NUMBER
      };

      enum peCode{
        peCodeLeftBracket = 1,
        peCodeRightBracket = 2,
        peCodeMul = 3,
        peCodeDiv = 4,
        peCodeAdd = 5,
        peCodeSub = 6,
        peCodeLessThan = 7,
        peCodeLessOrEqual = 8,
        peCodeEqual = 9,
        peCodeNotEqual = 10,
        peCodeGreaterThan = 11,
        peCodeGreaterOrEqual = 12,
        peCodeIs = 13,
        peCodeLike = 14,
        peCodeAnd = 15,
        peCodeOr = 16,
        peCodeNot = 17,
        peCodeMod = 20,
        peCodeColumnName = 50,
        peCodeString = 51,
        peCodeInt = 52,
        peCodeFloat = 53,
        peCodeDateTime = 54,
        peCodeNull = 55,
        peCodeTrue = 56,
        peCodeFalse = 57,
        peCodeMinus = 58,
        peCodePlus = 59,
        INVALID
      };


      enum pePriority{
        pePrioLeftBracket = 100,  /* highest */
        pePrioRightBracket = 0,   /* lowest */
        pePrioMinus = 10,
        pePrioPlus = 10,
        pePrioNot = 10,
        pePrioMul = 9,
        pePrioDiv = 9,
        pePrioMod = 9,
        pePrioAdd = 8,
        pePrioSub = 8,
        pePrioLessThan = 7,
        pePrioLessOrEqual = 7,
        pePrioEqual = 7,
        pePrioNotEqual = 7,
        pePrioGreaterThan = 7,
        pePrioGreaterOrEqual = 7,
        pePrioIs = 7,
        pePrioLike = 7,
        pePrioAnd = 6,
        pePrioOr = 5
          /*,
            pePrioString = -1,
            pePrioInt = -1,
            pePrioFloat = -1,
            pePrioDateTime = -1,
            pePrioNull = -1,
            pePrioTrue = -1,
            pePrioFalse = -1
            */
      };


      struct Symbol{
        Symbol():length(0), type(0){};
        int length;
        int type;
        char value[16 * 1024];  // FIXME: should use a predefined Macro
        int push(char c)
        {
          int err = OB_SUCCESS;

          if (length < 16 * 1024)
          {
            //TBSYS_LOG(DEBUG, "push:[lenght:%d]", length);
            value[length] = c;
            length++;
          }
          else
          {
            err = OB_ERROR;
            TBSYS_LOG(WARN, "expression parsing error. Token is too long! Exceeds 16KB");
          }
          return err;
        }
        ObObj encode;
        int code; //内码
        int prio; //优先级

        int toInt()
        {
          int err = OB_SUCCESS;
          int64_t d = 0;
          if (length >= 128)
          {
            err = OB_ERROR;
          }
          else
          {
            value[length] = '\0';
            // FIXME: 错误检查
            d = (int64_t)atol(value);
            code = peCodeInt;
            encode.set_int(d);
          }
          return err;
        }

        int hexToInt()
        {
          int err = OB_SUCCESS;
          int64_t d = 0;
          if (length >= 128)
          {
            err = OB_ERROR;
          }
          else
          {
            value[length] = '\0';
            // FIXME: 错误检查
            d = (int64_t)strtol(value, NULL, 16);
            code = peCodeInt;
            encode.set_int(d);
            //TBSYS_LOG(INFO, "hex2int, d=%ld", d);
          }
          return err;
        }

        int toDouble()
        {
          int err = OB_SUCCESS;
          double d = 0.0;
          if (length >= 128)
          {
            err = OB_ERROR;
          }
          else
          {
            value[length] = '\0';
            // FIXME: 错误检查
            d = atof(value);
            code = peCodeFloat;
            encode.set_double(d);
          }
          return err;
        }
      };


      inline void skip_blank(const char *p, int &pos)
      {
        while(pos < infix_max_len_ && p[pos] == ' ')
        {
          pos++;
        }
      }

      inline int get_column_name(const char *p, int &pos, Symbol &sym)
      {
        char c;
        int err = OB_SUCCESS;

        skip_blank(p, pos);

        if (p[pos] == '`')
        {
          pos++;
          while(p[pos] != '`')
          {
            if (pos >= infix_max_len_)
            {
              err = OB_SIZE_OVERFLOW;
              TBSYS_LOG(WARN, "unexpected end of input");
              break;
            }

            if(OB_SUCCESS != (err = sym.push(p[pos])))
            {
              break;
            }
            pos++;
          }
          pos++;  // skip last `
        }
        else
        {
          do{
            if (pos >= infix_max_len_)
            {
              break;
            }
            c = p[pos];

            if (c == '(' || c == ')' || c == '>' || c == '<' || c == '!' || c == '=' ||
                c == '+' || c == '-' || c == '*' || c == '/' || c == '%' || c == ' ')
            {
              break;
            }
            if(OB_SUCCESS != (err = sym.push(c)))
            {
              break;
            }
            pos++;
          }while(true);
        }
        return err;
      }


      inline int get_number(const char*p, int &pos, Symbol &sym)
      {
        int err = OB_SUCCESS;
        char c, c2;
        bool first_time = true;
        skip_blank(p, pos);

        if(pos + 1 < infix_max_len_)
        {
          c = p[pos];
          c2 = p[pos+1];
          if (c == '0' && (c2=='x' || c2=='X'))
          {
            sym.type = HEX_NUMBER;
            pos+=2;
            while(pos < infix_max_len_)
            {
              c = p[pos];
              if((c >= '0' && c <= '9') ||
                 (c >= 'a' && c <= 'f') ||
                 (c >= 'A' && c <= 'F'))
              {
                if (OB_SUCCESS != (err = sym.push(c)))
                {
                  break;
                }
                pos++;
                first_time = false;
              }
              else
              {
                // no data followed 0x
                if (true == first_time)
                {
                  err = OB_ERROR;
                }
                break;
              }
            }
          }
        }

        if (OB_SUCCESS == err && NUMBER == sym.type)
        {
          while(pos < infix_max_len_)
          {
            c = p[pos];
            if(c == '.')
            {
              if(first_time)
              {
                first_time = false;
                if (OB_SUCCESS != (err = sym.push(c)))
                {
                  break;
                }
                pos++;
              }
              else
              {
                // 2 dots is not right ;)
                err = OB_ERROR;
                break;
              }
            }
            else if(c >= '0' && c <= '9')
            {
              if (OB_SUCCESS != (err = sym.push(c)))
              {
                break;
              }
              pos++;
            }
            else
            {
              break;
            }
          }
        }
        return err;
      }

      inline int get_left_parenthese(const char*p, int &pos, Symbol &sym)
      {
        int err = OB_SUCCESS;
        char c;

        skip_blank(p, pos);

        c = p[pos];
        if (c == '(')
        {
          if (OB_SUCCESS == (err = sym.push(c)))
          {
            //TBSYS_LOG(DEBUG, "push c=%c", c);
            pos++;
          }
        }
        else
        {
          TBSYS_LOG(WARN, "unexpected char %c", c);
        }
        return err;
      }

      inline int get_right_parenthese(const char*p, int &pos, Symbol &sym)
      {
        int err = OB_SUCCESS;
        char c;

        skip_blank(p, pos);

        c = p[pos];
        if (c == ')')
        {
          if (OB_SUCCESS == (err = sym.push(c)))
          {
            //TBSYS_LOG(DEBUG, "push c=%c", c);
            pos++;
          }
        }
        else
        {
          TBSYS_LOG(WARN, "unexpected char %c", c);
        }
        return err;
      }



      inline int get_operator(const char*p, int &pos, Symbol &sym)
      {
        int err = OB_SUCCESS;
        char c = 0;

        skip_blank(p, pos);

        if (pos < infix_max_len_)
        {
          c = p[pos];
        }

        if ( c == '+' || c == '-' || c == '*' || c == '/' || c == '%' || c == '=')
        {
          //TBSYS_LOG(DEBUG, "push c=%c", c);
          if (OB_SUCCESS == (err = sym.push(c)))
          {
            pos++;
          }
        }
        else if(c == '>' || c == '<' || c == '!')
        {
          //TBSYS_LOG(DEBUG, "push c=%c", c);
          if (OB_SUCCESS == (err = sym.push(c)))
          {
            pos++;
            // >=, <=, !=
            if (pos < infix_max_len_)
            {
              c = p[pos];
              if (c == '=')
              {
                //TBSYS_LOG(DEBUG, "push c=%c", c);
                if (OB_SUCCESS == (err = sym.push(c)))
                {
                  pos++;
                }
              }
            }
          }
        }

        return err;
      }


      inline int get_binary(const char*p, int &pos, Symbol &sym)
      {
        int err = OB_SUCCESS;

        skip_blank(p, pos);

        if ((pos < infix_max_len_ - 3) && (p[pos] == 'b' || p[pos] == 'B' ) && p[pos+1] == '\'')
        {
          pos++; pos++;
          while(p[pos] != '\'')
          {
            if (pos >= infix_max_len_)
            {
              err = OB_SIZE_OVERFLOW;
              TBSYS_LOG(WARN, "unexpected end of input");
              break;
            }
            if (OB_SUCCESS != (err = sym.push(p[pos])))
            {
              break;
            }
            pos++;
          }
          pos++;  // skip last '
          /* FIXME: 将十六进制字符串转换成binary ObString */
          // code here?
          //
        }
        else
        {
          TBSYS_LOG(WARN, "unexpected string format");
        }

        return err;
      }


      inline int get_string(const char*p, int &pos, Symbol &sym)
      {
        int err = OB_SUCCESS;

        skip_blank(p, pos);

        if (p[pos] == '\'')
        {
          pos++;
          while(p[pos] != '\'')
          {
            if (pos >= infix_max_len_)
            {
              err = OB_SIZE_OVERFLOW;
              TBSYS_LOG(WARN, "unexpected end of input");
              break;
            }
            // string中支持转义字符(\)
            if (pos < infix_max_len_ - 1 && p[pos] == '\\' && p[pos+1] == '\'')
            {
              pos++;  // skip back slash(\)
            }

            if (OB_SUCCESS != (err = sym.push(p[pos])))
            {
              break;
            }
            pos++;
          }
          pos++;  // skip last '
        }
        else
        {
          TBSYS_LOG(WARN, "unexpected string format");
        }

        return err;
      }

      inline int get_keyword(const char*p, int &pos, Symbol &sym)
      {
        int err = OB_SUCCESS;
        char c;

        skip_blank(p, pos);
        do{
          if (pos >= infix_max_len_)
          {
            break;
          }
          c = p[pos];
          //TBSYS_LOG(DEBUG, "c=%c", c);
          if (c == '(' || c == ')' || c == '>' || c == '<' || c == '!' || c == '=' ||
              c == '+' || c == '-' || c == '*' || c == '/' || c == '%' || c == ' ')
          {
            break;
          }
          if (OB_SUCCESS != (err = sym.push(c)))
          {
            break;
          }
          pos++;
        }while(true);

        return err;
      }


      inline int get_datetime(const char*p, int &pos, Symbol &sym, char sep)
      {
        int err = OB_SUCCESS;

        skip_blank(p, pos);

        if (p[pos] == sep)
        {
          pos++;
          while(p[pos] != sep)
          {
            if (pos >= infix_max_len_)
            {
              err = OB_SIZE_OVERFLOW;
              TBSYS_LOG(WARN, "unexpected end of input");
              break;
            }
            if (OB_SUCCESS != (err = sym.push(p[pos])))
            {
              break;
            }
            pos++;
          }
          pos++;  // skip last sep
        }
        else
        {
          err = OB_ERR_UNEXPECTED;
          TBSYS_LOG(WARN, "unexpected datetime format");
        }
        return err;
      }


      inline int get_datetime_v1(const char*p, int &pos, Symbol &sym)
      {
        return get_datetime(p, pos, sym, '#');
      }


      inline int get_datetime_v2(const char*p, int &pos, Symbol &sym)
      {
        return get_datetime(p, pos, sym, '\'');
      }

      inline int decode_datetime(const char *str, int length, int64_t &decoded_val)
      {
        int err = OB_SUCCESS;
        char tmp_buf[127+1];  // zero terminator
        char *tmp_ptr = NULL;
        const char *format = NULL;
        //char *str = "2010-01-02 02:32:2";
        struct tm tm;
        char *ret;

        /// @note: must set to zero. strptime won't clear this when format is "%Y-%m-%d".
        memset(&tm, 0x00, sizeof(struct tm));

        if (length > 127 || length <= 0 || NULL == str)
        {
          err = OB_ERROR;
          TBSYS_LOG(WARN, "invalid param. length=%d, str=%p", length, str);
        }
        else
        {
          strncpy(tmp_buf, str, length);
          tmp_buf[length] = '\0';

          if (NULL != strchr(tmp_buf, ' '))
          {
            format = "%Y-%m-%d %H:%M:%S";
          }
          else if(NULL != strchr(tmp_buf, '-'))
          {
            format = "%Y-%m-%d";
          }
          else
          {
            // timestamp表达方式
            // FIXME: 不支持负数表示。一般来说这种需求不存在，故不支持
            format = NULL;
          }

          if (NULL != format)
          {
            ret = strptime(tmp_buf, format, &tm);
            if (ret != (tmp_buf + length))
            {
              decoded_val = -1;
              err = OB_ERROR;
              TBSYS_LOG(WARN, "decode datetime error. [input:%.*s][format=%s][ret=%p][tmp_buf=%p][length=%d]",
                  length, tmp_buf, format, ret, tmp_buf, length);
            }
            else
            {
              tm.tm_isdst = -1;
              if(-1 == (decoded_val = (int64_t)mktime(&tm)))
              {
                err = OB_ERROR;
                TBSYS_LOG(WARN, "convert time to timestamp format error");
              }
              else
              {
                /// bugfix: set to precise date time
                decoded_val *= 1000L * 1000; // convert sec to micro-sec
              }
            }
          }
          else  // NULL == format, direct timestamp format like #12312423# in micro-sec format
          {
            decoded_val = strtol(tmp_buf, &tmp_ptr, 10);
            if ('\0' != *tmp_ptr) // entire string is not valid
            {
              err = OB_ERROR;
              TBSYS_LOG(WARN, "convert timestamp error");
            }
          }
        }
        return err;
      }

      int get_sym(const char*p, int &pos, Symbol &sym)
      {
        int err = OB_SUCCESS;
        char c, c2;

        skip_blank(p, pos); // 跳过前导空格

        if (pos < infix_max_len_ - 1)
        {
          c = p[pos];
          c2 = p[pos+1];
        }
        else if (pos < infix_max_len_)
        {
          c = p[pos];
          c2 = 0;
        }
        else
        {
          err = OB_ITER_END;
        }


        if(OB_SUCCESS == err && c != ' ')
        {
          if(c == '>' || c == '<' || c == '!' || c == '=' ||
              c == '+' || c == '-' || c == '*' || c == '/' || c == '%')
          {
            //TBSYS_LOG(DEBUG, "get operator");
            sym.type = OPERATOR;
            err = get_operator(p, pos, sym);
          }
          else if(c == '(')
          {
            //TBSYS_LOG(DEBUG, "get left parenthese");
            sym.type = OPERATOR;
            err = get_left_parenthese(p, pos, sym);
          }
          else if(c == ')')
          {
            //TBSYS_LOG(DEBUG, "get left parenthese");
            sym.type = OPERATOR;
            err = get_right_parenthese(p, pos, sym);
          }
          else if(c == '\'')
          {
            //TBSYS_LOG(DEBUG, "get string");
            sym.type = STRING;
            err = get_string(p, pos, sym);
          }
          else if(c == '#')
          {
            //TBSYS_LOG(DEBUG, "get datetime");
            sym.type = DATETIME;
            err = get_datetime_v1(p, pos, sym);
          }
          else if(c == 'd'|| c == 'D') // d... possiblely a datetime type, try it!
          {
            bool is_datetime = true;
            if (pos < infix_max_len_ - 8)
            {
              if (0 == strncasecmp("datetime'", &p[pos], 9))
              {
                // datetime'...
                pos += 8;
              }
              else if (0 == strncasecmp("date'", &p[pos], 5))
              {
                // date'...
                pos += 4;
              }
              else if (c2 == '\'')
              {
                // D'
                pos += 1;
              }
              else
              {
                is_datetime = false;
              }
            }
            else if (pos < infix_max_len_ - 4)
            {
              if (0 == strncasecmp("date'", &p[pos], 5))
              {
                // date'...
                pos += 4;
              }
              else if (c2 == '\'')
              {
                // D'
                pos += 1;
              }
              else
              {
                is_datetime = false;
              }
            }
            else if (c2 == '\'')
            {
              // D'
              pos += 1;
            }
            else
            {
              is_datetime = false;
            }

            if (true == is_datetime)
            {
              //TBSYS_LOG(DEBUG, "get datetime");
              sym.type = DATETIME;
              err = get_datetime_v2(p, pos, sym);
            }
            else
            {
              //TBSYS_LOG(DEBUG, "get keyword");
              sym.type = KEYWORD;
              err = get_keyword(p, pos, sym);
            }
          }
          else if(c == '`')
          {
            //TBSYS_LOG(DEBUG, "get column name");
            sym.type = COLUMN_NAME;
            err = get_column_name(p, pos, sym);
          }
          else if(c == '.' || (c >= '0' && c <= '9'))
          {
            //TBSYS_LOG(DEBUG, "get number");
            sym.type = NUMBER;
            err = get_number(p, pos, sym);
          }
          else if((c == 'b' || c == 'B') && c2 == '\'')
          {
            //TBSYS_LOG(DEBUG, "get string");
            sym.type = HEX_STRING;
            err = get_binary(p, pos, sym);
          }
          else
          {
            //TBSYS_LOG(DEBUG, "get keyword");
            sym.type = KEYWORD;
            err = get_keyword(p, pos, sym);
          }
        }
        else
        {
          //TBSYS_LOG(DEBUG, "end of input!!!!!!!!!!!!!!!!!!!!!!!");
        }

        return err;
      }

      int hex_to_binary(Symbol &sym)
      {
        int err = OB_SUCCESS;
        int i = 0;
        int out = 0;
        int hi, lo;

        // 将字符型 0~f 转化成数字型 0~15
        for(i = 0; i < sym.length; i++)
        {
          if (sym.value[i] >= 'A' && sym.value[i] <= 'F')
          {
            sym.value[i] = static_cast<char>(sym.value[i] - 'A' + 10);
          }
          else if (sym.value[i] >= 'a' && sym.value[i] <= 'f')
          {
            sym.value[i] = static_cast<char>(sym.value[i] - 'a' + 10);
          }
          else if(sym.value[i] >= '0' && sym.value[i] <= '9')
          {
            sym.value[i] = static_cast<char>(sym.value[i] - '0');
          }
          else
          {
            TBSYS_LOG(WARN, "Invalid binary format. sym[%d] = 0x%X", i, sym.value[i]);
            err = OB_ERROR;
          }
        }

        if (OB_SUCCESS == err)
        {
          i = 0;
          if (sym.length % 2 != 0)
          {
            hi = 0;
            lo = sym.value[i++];
            sym.value[out++] = static_cast<char>(lo);
          }
          while(i < sym.length)
          {
            hi = sym.value[i++];
            lo = sym.value[i++];
            sym.value[out++] = static_cast<char>(lo + (hi << 4));
            //TBSYS_LOG(DEBUG, "c=%c", sym.value[out-1]);
          }
          sym.length = out;
        }

        return err;

      }

      bool symbol_is_operand(const Symbol &sym)
      {
        bool ret = false;
        switch(sym.code)
        {
          case peCodeColumnName:
          case peCodeString:
          case peCodeInt:
          case peCodeFloat:
          case peCodeDateTime:
          case peCodeNull:
          case peCodeTrue:
          case peCodeFalse:
          // 这个情况比较特殊，需要注意 (1-3)-5,()作为一个整体可以看做是个operand
          case peCodeRightBracket:
            ret = true;
            break;
          default:
            ret = false;
            break;
        }
        return ret;
      }

      //modify peiouya [Expire_condition_modify] 20140909:b
      //int encode_sym(Symbol &sym, const Symbol *prev_symbol)
      int encode_sym(Symbol &sym, const Symbol *prev_symbol, const bool is_sys_day)
      //modify 20140909:e
      {
        int err = OB_SUCCESS;
        int64_t tmp = 0;
        ObString s;
        bool dig_found = false;
        int i = 0;
        //TBSYS_LOG(DEBUG, "sym type:%d,value:%.*s", sym.type, sym.length, sym.value);
        switch(sym.type)
        {
          case OPERATOR:
            if (0 == strncmp("+", sym.value, sym.length))
            {
              // 如果+号前面不是操作数，而是一个操作符，那么可以断定是正号，而不是加号
              if ((prev_symbol == NULL) || ((prev_symbol != NULL) && (true != symbol_is_operand(*prev_symbol))))
              {
                sym.code = peCodePlus;
                sym.prio = pePrioPlus;
                sym.encode.set_int(ObExpression::PLUS_FUNC);
              }
              else
              {
                sym.code = peCodeAdd;
                sym.prio = pePrioAdd;
                sym.encode.set_int(ObExpression::ADD_FUNC);
              }
            }
            else if(0 == strncmp("-", sym.value, sym.length))
            {
              // 如果-号前面不是操作数，而是一个操作符，那么可以断定是负号，而不是减号
              if ((prev_symbol == NULL) || ((prev_symbol != NULL) && (true != symbol_is_operand(*prev_symbol))))
              {
                sym.code = peCodeMinus;
                sym.prio = pePrioMinus;
                sym.encode.set_int(ObExpression::MINUS_FUNC);
              }
              else
              {
                sym.code = peCodeSub;
                sym.prio = pePrioSub;
                sym.encode.set_int(ObExpression::SUB_FUNC);
              }
            }
            else if(0 == strncmp("*", sym.value, sym.length))
            {
              sym.code = peCodeMul;
              sym.prio = pePrioMul;
              sym.encode.set_int(ObExpression::MUL_FUNC);
            }
            else if(0 == strncmp("/", sym.value, sym.length))
            {
              sym.code = peCodeDiv;
              sym.prio = pePrioDiv;
              sym.encode.set_int(ObExpression::DIV_FUNC);
            }
            else if(0 == strncmp("%", sym.value, sym.length))
            {
              sym.code = peCodeMod;
              sym.prio = pePrioMod;
              sym.encode.set_int(ObExpression::MOD_FUNC);
            }
            else if(0 == strncmp("<", sym.value, sym.length))
            {
              sym.code = peCodeLessThan;
              sym.prio = pePrioLessThan;
              sym.encode.set_int(ObExpression::LT_FUNC);
            }
            else if(0 == strncmp("<=", sym.value, sym.length))
            {
              sym.code = peCodeLessOrEqual;
              sym.prio = pePrioLessOrEqual;
              sym.encode.set_int(ObExpression::LE_FUNC);
            }
            else if(0 == strncmp("=", sym.value, sym.length))
            {
              sym.code = peCodeEqual;
              sym.prio = pePrioEqual;
              sym.encode.set_int(ObExpression::EQ_FUNC);
            }
            else if(0 == strncmp("!=", sym.value, sym.length))
            {
              sym.code = peCodeNotEqual;
              sym.prio = pePrioNotEqual;
              sym.encode.set_int(ObExpression::NE_FUNC);
            }
            else if(0 == strncmp(">", sym.value, sym.length))
            {
              sym.code = peCodeGreaterThan;
              sym.prio = pePrioGreaterThan;
              sym.encode.set_int(ObExpression::GT_FUNC);
            }
            else if(0 == strncmp(">=", sym.value, sym.length))
            {
              sym.code = peCodeGreaterOrEqual;
              sym.prio = pePrioGreaterOrEqual;
              sym.encode.set_int(ObExpression::GE_FUNC);
            }
            else if(0 == strncmp("(", sym.value, sym.length))
            {
              sym.code = peCodeLeftBracket;
              sym.prio = pePrioLeftBracket;
              sym.encode.set_int(ObExpression::LEFT_PARENTHESE);
            }
            else if(0 == strncmp(")", sym.value, sym.length))
            {
              sym.code = peCodeRightBracket;
              sym.prio = pePrioRightBracket;
              sym.encode.set_int(ObExpression::RIGHT_PARENTHESE);
            }
            else
            {
              TBSYS_LOG(WARN, "unexpected symbol value. [type:%d][value:%.*s]",
                  sym.type,
                  sym.length,
                  sym.value);
              err = OB_ERR_UNEXPECTED;
            }
            break;
          case DATETIME:
            //TODO:将字符串时间转化成时间戳
            sym.code = peCodeDateTime;
            if(OB_SUCCESS != (err = decode_datetime(sym.value, sym.length, tmp)))
            {
              TBSYS_LOG(WARN, "fail to decode datetime.");
            }
            else
            {
              sym.encode.set_precise_datetime(tmp);
            }
            break;
          case NUMBER:
            for(i = 0; i < sym.length; i++)
            {
              if (sym.value[i] == '.')
              {
                if (dig_found == false)
                {
                  dig_found = true;
                }
                else
                {
                  TBSYS_LOG(WARN, "invalid number format! [type:%d][value:%.*s]",
                      sym.type,
                      sym.length,
                      sym.value);
                  err = OB_ERROR;
                  break;
                }
              }
            }
            if ( OB_SUCCESS == err )
            {
              if (dig_found)
              {
                //FIXME: 当前版本不支持0x, 10.0f等表示法
                //       仅支持十进制整数和浮点数（双精度）
                // TODO: 浮点转换
                sym.toDouble();
                //double d = 10.0;
                //sym.code = peCodeFloat;
                //sym.encode.set_double(d);
              }
              else
              {
                sym.toInt();
                //int64_t i = 10;
                //sym.code = peCodeInt;
                //sym.encode.set_int(i);
              }
            }
            //add peiouya [Expire_condition_modify] 20140909:b
            if (is_sys_day)
            {
              int64_t value = 0;
              sym.encode.get_int(value);
              value *= 24 * 60 * 60 * 1000000L;
              sym.type = DATETIME;
              sym.code = peCodeDateTime;
              sym.encode.set_precise_datetime(value);
            }
            //add 20140909:e
            break;
          case HEX_NUMBER:
                sym.hexToInt();
                //int64_t i = 10;
                //sym.code = peCodeInt;
                //sym.encode.set_int(i);
                //add peiouya [Expire_condition_modify] 20140909:b
                if (is_sys_day)
                {
                  int64_t value = 0;
                  sym.encode.get_int(value);
                  value *= 24 * 60 * 60 * 1000000L;
                  sym.type = DATETIME;
                  sym.code = peCodeDateTime;
                  sym.encode.set_precise_datetime(value);
                }
                //add 20140909:e
            break;
          case COLUMN_NAME:
          #if 1
            /// 正式代码
            sym.code = peCodeColumnName;
            s.assign(sym.value, sym.length);
            sym.encode.set_varchar(s);
          #else
            /// 专为测试而写
            sym.toInt();
            sym.code = peCodeColumnName;  // fixup code type
          #endif
            break;
          case STRING:
            sym.code = peCodeString;
            s.assign(sym.value, sym.length);
            sym.encode.set_varchar(s);
            break;
          case HEX_STRING:
            sym.code = peCodeString;
            /*TODO: 将4141这种串转化成AA这样的串，然后给ObString
            */
            if(OB_SUCCESS == (err = hex_to_binary(sym)))
            {
              s.assign(sym.value, sym.length);
              sym.encode.set_varchar(s);
            }
            break;
          case KEYWORD:
            if (strlen("AND") == (size_t)sym.length && 0 == strncasecmp("AND", sym.value, sym.length))
            {
              sym.type = OPERATOR;
              sym.code = peCodeAnd;
              sym.prio = pePrioAnd;
              sym.encode.set_int(ObExpression::AND_FUNC);
            }
            else if (strlen("OR") == (size_t)sym.length && 0 == strncasecmp("OR", sym.value, sym.length))
            {
              sym.type = OPERATOR;
              sym.code = peCodeOr;
              sym.prio = pePrioOr;
              sym.encode.set_int(ObExpression::OR_FUNC);
            }
            else if (strlen("NOT") == (size_t)sym.length && 0 == strncasecmp("NOT", sym.value, sym.length))
            {
              sym.type = OPERATOR;
              sym.code = peCodeNot;
              sym.prio = pePrioNot;
              sym.encode.set_int(ObExpression::NOT_FUNC);
            }
            else if (strlen("IS") == (size_t)sym.length && 0 == strncasecmp("IS", sym.value, sym.length))
            {
              sym.type = OPERATOR;
              sym.code = peCodeIs;
              sym.prio = pePrioIs;
              sym.encode.set_int(ObExpression::IS_FUNC);
            }
            else if (strlen("LIKE") == (size_t)sym.length && 0 == strncasecmp("LIKE", sym.value, sym.length))
            {
              sym.type = OPERATOR;
              sym.code = peCodeLike;
              sym.prio = pePrioLike;
              sym.encode.set_int(ObExpression::LIKE_FUNC);
            }
            else if (strlen("NULL") == (size_t)sym.length && 0 == strncasecmp("NULL", sym.value, sym.length))
            {
              sym.type = NUMBER;
              sym.code = peCodeNull;
              sym.encode.set_null();
            }
            else
            {
              sym.type = COLUMN_NAME;
              #if 1
                /// 正式代码
                sym.code = peCodeColumnName;
                s.assign(sym.value, sym.length);
                sym.encode.set_varchar(s);
              #else
                /// 专为测试而写
                sym.toInt();
                sym.code = peCodeColumnName;  // fixup code type
              #endif
              //TBSYS_LOG(INFO, "column name with ` (e.g `my_col_name`) is recommended!");
              break;
            }
            break;
          default:
            TBSYS_LOG(WARN, "unexpected sym type. [type:%d][value:%.*s]",
                sym.type,
                sym.length,
                sym.value);
            err = OB_ERR_UNEXPECTED;
            break;
        }
        TBSYS_LOG(DEBUG, "sym type:%d,value:%.*s", sym.type, sym.length, sym.value);
        //sym.encode.dump();
        return err;
      }

      int parse_expression()
      {
        int err = OB_SUCCESS;
        int i = 0;
        int code = 0;
        Symbol *sym = NULL;
        //Symbol *next_sym = NULL;
        Symbol zero_sym;

        for (i = 0; i < symbols_.get_array_index(); i++)
        {
          sym = symbols_.at(i);
          code = sym->code;
          switch(code)
          {
            case peCodeLeftBracket:
              if (!stack_.push_back(sym))
              {
                err = OB_ERROR;
              }
#if 0
              // 往后看一个符号，如果是“+/-”，则插入一个0到symbol列表中
              // 优化：插入0到symbol列表后，马上会被output。
              //       直接output可以避免一次symbol列表的向后shuffle
              if (i < symbols_.get_array_index() - 1 )
              {
                next_sym = symbols_.at(i+1);
                if (peCodeSub == next_sym->code || peCodeAdd == next_sym->code)
                {
                  zero_sym.push('0');
                  zero_sym.toInt();
                  output(&zero_sym);
                }
              }
#endif
              break;
            case peCodeRightBracket:
              err = do_oper2();
              break;
            case peCodeMul:
            case peCodeDiv:
            case peCodeMod:
              //break;
            case peCodeAdd:
            case peCodeSub:
              // break;
            case peCodeLessThan:
            case peCodeLessOrEqual:
            case peCodeEqual:
            case peCodeNotEqual:
            case peCodeGreaterThan:
            case peCodeGreaterOrEqual:
            case peCodeIs:
            case peCodeLike:
              // break;
            case peCodeAnd:
              // break;
            case peCodeOr:
            case peCodeNot:
            case peCodeMinus:
            case peCodePlus:
              err = do_oper1(sym);
              break;
            case peCodeString:
            case peCodeInt:
            case peCodeFloat:
            case peCodeDateTime:
            case peCodeNull:
            case peCodeTrue:
            case peCodeFalse:
              err = output(sym);
              break;
            //add peiouya [Expire_condition_modify] 20140913:b
            case INVALID:
              //NOTHING TODO, SKIP CURRENT invalid sym
              break;
            //add 20140913:e
            default:
              err = output(sym);
              /*
                 err = OB_ERR_UNEXPECTED;
                 TBSYS_LOG(WARN, "unexpected symbol found");
                 */
              break;
          }
          if (OB_SUCCESS != err)
          {
            TBSYS_LOG(WARN, "fail. err = %d", err);
            break;
          }
        }

        while((OB_SUCCESS == err) && (0 < stack_.get_array_index()))
        {
          //TBSYS_LOG(INFO, "pop");
          err = output(*stack_.pop());
          //TBSYS_LOG(INFO, "pop end");
        }

        if (OB_SUCCESS == err)
        {
          err = output_terminator();
        }

        if (OB_SUCCESS == err && 0 == stack_.get_array_index())
        {
          err = OB_ITER_END;
        }
        return err;
      }

      int do_oper1(Symbol *new_sym)
      {
        int err = OB_SUCCESS;
        Symbol *old_sym;
        int code = 0;
        int new_prio = 0;
        int old_prio = 0;

        if (NULL != new_sym)
        {
          while((OB_SUCCESS == err) && (0 < stack_.get_array_index()))
          {
            old_sym = *stack_.pop();
            if (NULL  == old_sym)
            {
              TBSYS_LOG(WARN, "unexpect null pointer");
              err = OB_ERROR;
              break;
            }
            code = old_sym->code;
            if (peCodeLeftBracket == code)
            {
              if (!stack_.push_back(old_sym))
              {
                err = OB_ERROR;
                TBSYS_LOG(WARN, "fail to push symbol into stack. stack is full!");
              }
              break;
            }
            else
            {
              new_prio = new_sym->prio;
              old_prio = old_sym->prio;
              if (new_prio > old_prio)
              {
                if (!stack_.push_back(old_sym))
                {
                  err = OB_ERROR;
                  TBSYS_LOG(WARN, "fail to push symbol into stack. stack is full!");
                }
                break;
              }
              // bugfix code: 正负号属于右结合，靠近右边操作数的先计算
              else if (new_prio == old_prio && (new_sym->code == peCodeMinus || new_sym->code == peCodePlus))
              {
                if (!stack_.push_back(old_sym))
                {
                  err = OB_ERROR;
                  TBSYS_LOG(WARN, "fail to push symbol into stack. stack is full!");
                }
                break;
              }
              else
              {
                err = output(old_sym);
              }
           }
          } /* while */

          if (OB_SUCCESS == err)
          {
            if(!stack_.push_back(new_sym))
            {
              err = OB_ERROR;
            }
          }
        }
        else
        {
          TBSYS_LOG(WARN, "symbol pointer is null");
          err = OB_ERROR;
        }
        return err;
      }



      int do_oper2()
      {
        int err = OB_SUCCESS;
        Symbol *sym;
        int code = 0;

        while(0 < stack_.get_array_index())
        {
          sym = *stack_.pop();
          if (NULL == sym)
          {
            TBSYS_LOG(WARN, "fail to pop symbol from stack. stack is empty!");
            err = OB_ERROR;
            break;
          }
          else
          {
            code = sym->code;
            if (peCodeLeftBracket == code)
            {
              break;
            }
            else
            {
              err = output(sym);
              if (OB_SUCCESS != err)
              {
                TBSYS_LOG(WARN, "unexpected error");
                break;
              }
            }
          }
        }
        return err;
      }

      //add  peiouya [EXPIRE_INFO_NOT_ARITHMETIC_FIX] 20160608:b
      int conplete_for_expire_info(Symbol *s)
      {
        OB_ASSERT(is_expire_info_);

        int err = OB_SUCCESS;
        ObObj tmp_obj[4];  //only for expire_info

        switch (s->code)
        {
          case peCodeColumnName:
          case peCodeString:
          case peCodeDateTime:
                  tmp_obj[0].set_int(ObExpression::CONST_OBJ);
                  tmp_obj[1].set_int(common::ObIntType);
                  tmp_obj[2].set_int(ObExpression::OP);
                  tmp_obj[3].set_int(ObExpression::CAST_THIN_FUNC);
                  for(int64_t index = 0; index < 4; ++index)
                  {
                    if(!result_->push_back(tmp_obj[index]))
                    {
                      err = OB_ERROR;
                      TBSYS_LOG(WARN, "push obj failed. array_index=%ld array_size=%ld",
                          result_->get_array_index(), result_->get_array_size());
                      break;
                    }
                  }
                  break;
           default:
                  break;
        }

       return err;
      }
      //add  peiouya [EXPIRE_INFO_NOT_ARITHMETIC_FIX] 20160608:e

      int output(Symbol *s)
      {
        int err = OB_SUCCESS;
        ObObj type_obj;
        /*
        TBSYS_LOG(INFO, ">>>>>>>>output===>>>>: [code:%d, value:%.*s]",
            s->code,
            s->length,
            s->value);
        s->encode.dump();
        */
        if (NULL == result_)
        {
          err = OB_ERROR;
          TBSYS_LOG(WARN, "add symbol to result stack failed. result=%p", result_);
        }
        else
        {
          /* 中缀表达式到后缀表达式的协议转换 */
          switch(s->code)
          {
            case peCodeMul:
            case peCodeDiv:
            case peCodeMod:
            case peCodeAdd:
            case peCodeSub:
            case peCodeLessThan:
            case peCodeLessOrEqual:
            case peCodeEqual:
            case peCodeNotEqual:
            case peCodeGreaterThan:
            case peCodeGreaterOrEqual:
            case peCodeIs:
            case peCodeLike:
            case peCodeAnd:
            case peCodeOr:
            case peCodeNot:
            case peCodeMinus:
            case peCodePlus:
              type_obj.set_int(ObExpression::OP);
              break;
            case peCodeColumnName:
              type_obj.set_int(ObExpression::COLUMN_IDX);
              break;
            case peCodeString:
            case peCodeInt:
            case peCodeFloat:
            case peCodeDateTime:
            case peCodeNull:
            case peCodeTrue:
            case peCodeFalse:
              type_obj.set_int(ObExpression::CONST_OBJ);
              break;
            default:
              TBSYS_LOG(WARN, "unexpected symbol. code=%d", s->code);
              err = OB_ERROR;
              break;
          }
          if ( (OB_SUCCESS != err) || (!result_->push_back(type_obj)) )
          {
            err = OB_ERROR;
            TBSYS_LOG(WARN, "push obj failed. array_index=%ld array_size=%ld",
                result_->get_array_index(), result_->get_array_size());
          }
          else if (!result_->push_back(s->encode))
          {
            err = OB_ERROR;
            TBSYS_LOG(WARN, "push obj failed. array_index=%ld array_size=%ld",
                result_->get_array_index(), result_->get_array_size());
          }
          //add  peiouya [EXPIRE_INFO_NOT_ARITHMETIC_FIX] 20160608:b
          else if (is_expire_info_)
          {
              err = conplete_for_expire_info(s);
          }
          //add  peiouya [EXPIRE_INFO_NOT_ARITHMETIC_FIX] 20160608:e
        }
        return err;
      }

      int output_terminator()
      {
        int err = OB_SUCCESS;
        ObObj type_obj;

        type_obj.set_int(ObExpression::END);  // terminator symbol
        if (!result_->push_back(type_obj))
        {
          err = OB_ERROR;
          TBSYS_LOG(WARN, "push obj failed. array_index=%ld array_size=%ld",
              result_->get_array_index(), result_->get_array_size());
        }
        return err;
      }

      int infix2postfix()
      {
        int err = OB_SUCCESS;
        if (symbols_.get_array_index() > 0 && NULL != result_)
        {
          err = parse_expression();
        }
        else
        {
          err = OB_ERROR;
          TBSYS_LOG(WARN, "param error in parsing expression. size=%ld, result=%p",
              symbols_.get_array_index(), result_);
        }

        if (OB_ITER_END != err)
        {
          err = OB_ERROR;
        }
        else
        {
          err = OB_SUCCESS;
        }

        return err;
      }


      /* 重置对象 */
      void reset()
      {
        this->symbols_.clear();
        this->stack_.clear();
        this->result_ = NULL;
        this->infix_max_len_ = 0;  // input expr length
        is_expire_info_ = false;  //add  peiouya [EXPIRE_INFO_NOT_ARITHMETIC_FIX] 20160608
      }

      private:
      // internal stack for parsing expression
      Symbol *sym_stack[OB_MAX_COMPOSITE_SYMBOL_COUNT];
      ObArrayHelper<Symbol *> stack_;
      // internal buffer for parsing symbols
      Symbol sym_list_[OB_MAX_COMPOSITE_SYMBOL_COUNT];
      ObArrayHelper<Symbol> symbols_;
      // result pointer. point to result buffer
      ObArrayHelper<ObObj> *result_;
      // 被解析字符串的长度，用于防止越界访问和判断终止
      int infix_max_len_;
      bool is_expire_info_;  //add  peiouya [EXPIRE_INFO_NOT_ARITHMETIC_FIX] 20160608
    };










    class ObScanParam;
    typedef int(*op_call_func_t)(ObExprObj *a, int &b, ObExprObj &c);
    class ObPostfixExpression
    {
      public:
        ObPostfixExpression(){};
        ~ObPostfixExpression(){};
         ObPostfixExpression& operator=(const ObPostfixExpression &other);

        /* @param expr:已经解析好了的后缀表达式数组 */
        int set_expression(const ObObj *expr, ObStringBuf  & data_buf);

        /* @absolete interface
         *
         * @param expr: 中缀表达式
         * @param scan_param: 包含name to index的映射，用于解析
         */
        int set_expression(const ObString &expr, const ObScanParam &scan_param);

        /* @param expr: 中缀表达式
         * @param cname_to_idx_map: 包含name to index的映射，用于解析
         * @param parser: 解析器，用于解析expr
         */
        //mod  peiouya [EXPIRE_INFO_NOT_ARITHMETIC_FIX] 20160608:b
        //int set_expression(const ObString &expr,
        //    const hash::ObHashMap<ObString,int64_t,hash::NoPthreadDefendMode> & cname_to_idx_map,
        //    ObExpressionParser & parser, common::ObResultCode *rc = NULL);
        int set_expression(const ObString&                                                         expr,
                           const hash::ObHashMap<ObString,int64_t,hash::NoPthreadDefendMode> &     cname_to_idx_map,
                           ObExpressionParser &                                                    parser,
                           common::ObResultCode                                                    *rc  =  NULL,
                           bool                                                                    is_expire_info = false);
        //mod  peiouya [EXPIRE_INFO_NOT_ARITHMETIC_FIX] 20160608:b

        /* 将org_cell中的值代入到expr计算结果 */
        int calc(ObObj &composite_val,
            const ObCellArray & org_cells,
            const int64_t org_row_beg,
            const int64_t org_row_end
            );

        const ObObj * get_expression()const
        {
          return expr_;
        }
        /* 序列化 和 反序列化 */
        NEED_SERIALIZE_AND_DESERIALIZE;
      private:
      private:
        static op_call_func_t call_func[ObExpression::MAX_FUNC];
        ObObj expr_[OB_MAX_COMPOSITE_SYMBOL_COUNT];
        // ObExpressionParser parser_;
        // TODO: 修改成指针引用，减少数据拷贝
        ObExprObj stack_i[OB_MAX_COMPOSITE_SYMBOL_COUNT];
        int postfix_size_;
    }; // class ObPostfixExpression

  } // namespace commom
}// namespace oceanbae

#endif
