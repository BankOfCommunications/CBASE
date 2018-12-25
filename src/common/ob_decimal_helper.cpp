#include "ob_decimal_helper.h"

namespace oceanbase
{
  namespace common
  {
    ///@fn 求出宽度为width的最大整数，比如width = 4，那么返回值是9999
    ///@param[out] 返回宽度为width的最大整数
    int ObDecimalHelper::get_max_express_number(const int64_t width, int64_t &value)
    {
      int err = OB_SUCCESS;
      if (width >= 0 && width <= OB_MAX_DECIMAL_WIDTH - OB_MAX_PRECISION_WIDTH)
      {
        value = 0;
        for (int64_t i = 1; i <= width; i++)
        {
          value = value * OB_DECIMAL_MULTIPLICATOR + OB_MAX_DIGIT;
        }
      }
      else
      {
        err = OB_INVALID_ARGUMENT;
        TBSYS_LOG(WARN, "invalid argument, width = %ld", width);
      }
      return err;
    }

    ///@fn 求出宽度为width的最小整数,比如width = 4,那么返回值是1000
    int ObDecimalHelper::get_min_express_number(const int64_t width, int64_t &number)
    {
      int err = OB_SUCCESS;
      if (0 == width)
      {
        number = 0;
      }
      else if (width > 0 && width <= OB_MAX_DECIMAL_WIDTH - OB_MAX_PRECISION_WIDTH)
      {
        number = 1;
        for (int64_t i = 1; i < width; i++)
        {
          number *= OB_DECIMAL_MULTIPLICATOR;
        }
      }
      else
      {
        err = OB_INVALID_ARGUMENT;
        TBSYS_LOG(WARN, "invalid argument.width= %ld", width);
      }
      return err;
    }

    //直接用整数运算实现四舍五入
    int ObDecimalHelper::round_off(int64_t &fractional, bool &is_carry, const int64_t precision)
    {
      int err = OB_SUCCESS;
      is_carry = false;
      if (precision < 0 || fractional < 0)
      {
        err = OB_INVALID_ARGUMENT;
        TBSYS_LOG(WARN, "invalid argument, fractional = %ld, precision = %ld", fractional, precision);
      }
      int64_t tmp_val = 0;
      if (OB_SUCCESS == err)
      {
        err = get_max_express_number(precision, tmp_val);
        if (err != OB_SUCCESS)
        {
          TBSYS_LOG(WARN, "fail to get max express number, precision = %ld, err = %d", precision, err);
        }
      }
      if (OB_SUCCESS == err)
      {
        if (fractional < tmp_val)
        {
          TBSYS_LOG(WARN, "fractional needn't be round. fractional =%ld, precision = %ld", fractional, precision);
        }
        else
        {
          int64_t remainder = 0;
          while (fractional > tmp_val)
          {
            remainder = fractional % OB_DECIMAL_MULTIPLICATOR;
            fractional /= OB_DECIMAL_MULTIPLICATOR;
          }
          //需要进位
          if (remainder >= 5)
          {
            if (fractional == tmp_val)
            {
              fractional = 0;
              is_carry = true;
            }
            else
            {
              fractional += 1;
            }
          }
        }
      }
      if (OB_SUCCESS != err)
      {
        TBSYS_LOG(WARN, "fail to round off ,err = %d", err);
      }
      return err;
    }

    int ObDecimalHelper::round_off(int64_t &fractional, bool &is_carry, const int64_t src_pre, const int64_t dest_pre)
    {
      int err = OB_SUCCESS;
      if (fractional < 0 || src_pre < 0 || dest_pre < 0
          || src_pre > OB_MAX_PRECISION_WIDTH
          || dest_pre > OB_MAX_PRECISION_WIDTH)
      {
        err = OB_INVALID_ARGUMENT;
        TBSYS_LOG(WARN, "invalid argument. fractional = %ld, src_pre = %ld, dest_pre = %ld", fractional, src_pre, dest_pre);
      }
      is_carry = false;
      if (fractional != 0)
      {
        if (OB_SUCCESS == err && dest_pre > src_pre)
        {
          err = promote_fractional(fractional, dest_pre - src_pre);
        }
        if (OB_SUCCESS == err && dest_pre == src_pre)
        {
          err = round_off(fractional, is_carry, src_pre);
        }
        if (OB_SUCCESS == err && dest_pre < src_pre)
        {
          int64_t min_number = 0;
          int64_t tmp_fra = fractional;
          bool tmp_carry = false;
          err = get_min_express_number(src_pre, min_number);
          if (OB_SUCCESS == err)
          {
            if (tmp_fra > min_number)
            {
              err = round_off(fractional, tmp_carry, dest_pre);
              if (OB_SUCCESS == err)
              {
                is_carry = tmp_carry;
              }
            }
            else
            {
              int64_t i = src_pre;
              while (fractional < min_number)
              {
                i--;
                get_min_express_number(i, min_number);
              }
              if (i - src_pre + dest_pre >= 0)
              {
                err = round_off(fractional, tmp_carry, i - src_pre + dest_pre);
                if (OB_SUCCESS == err)
                {
                  if (0 == fractional && true == tmp_carry)
                  {
                    err = get_min_express_number(dest_pre + i - src_pre + 1, min_number);
                    if (OB_SUCCESS == err)
                    {
                      fractional += min_number;
                    }
                  }
                }
              }
              else
              {
                fractional = 0;
              }
            }
          }
        }
      }
      if (OB_SUCCESS != err)
      {
        TBSYS_LOG(WARN, "fail to round off. err =%d", err);
      }
      return err;
    }

    ///@fn 检查字符串表示的定点数是否合法
    /*如果合法，则返回定点数是否带符号位，符号位是正还是负、是否含有小数部分
      如果不合法，则返回错误码*/
    int ObDecimalHelper::check_decimal_str(const char *value, const int64_t length, bool &has_fractional,  bool &has_sign, bool &sign)
    {
      int err = OB_SUCCESS;
      bool ret = false;
      has_fractional = false;
      sign = false;
      has_sign = false;
      if (value == NULL || length < 1
          || length > OB_MAX_DECIMAL_WIDTH + 2
          ||static_cast<int64_t>(strlen(value)) < length)
      {
        err = OB_INVALID_ARGUMENT;
        ret = true;
        TBSYS_LOG(WARN, "invalid argument. value=%p, length = %ld", value, length);
      }
      char *pos = NULL;
      char *end = NULL;
      char *tmp = NULL;
      //检查首位是否为+ - 或者数字
      if (OB_SUCCESS == err && false == ret)
      {
        pos = const_cast<char*>(value);
        end = pos + length - 1;
        tmp = pos;
        if ((*pos != '-') && (*pos != '+') && !isdigit(*pos))
        {
          err = OB_NOT_A_DECIMAL;
          ret = true;
          TBSYS_LOG(WARN, "input value string not legal. the first char =%c", *pos);
        }
        else
        {
          sign = (*pos == '-') ? false : true;
          if (!isdigit(*pos))
          {
            has_sign = true;
            if (tmp == end)
            {
              err = OB_NOT_A_DECIMAL;
              ret = true;
              TBSYS_LOG(WARN, "value isn't a decimal, there is only a sign, without digit");
            }
            else
            {
              tmp ++;
            }
          }
        }
      }

      //tmp指向符号位后的第一个字符 或者是 不含符号位的字符串首位
      //pos指向首地址

      char tmp_value[length + 1];
      int64_t tmp_len = length;
      char *stringp = NULL;

      //利用strsep分割字符串成整数部分和小数部分
      if (OB_SUCCESS == err && false == ret)    
      {     
        if (tmp != pos)
        {
          tmp_len = length - 1;
        }
        memcpy(tmp_value, tmp, tmp_len);
        pos = tmp_value;
        tmp = tmp_value;
        end = &tmp_value[tmp_len - 1];
        tmp_value[tmp_len] = '\0';
        tmp_value[length] = '\0';
        stringp = strsep (&tmp, "."); 
      }   

      if (OB_SUCCESS == err && false == ret)
      {     
        //判断整数部分
        if (stringp == NULL)
        {
          ret = true;
          err = OB_NOT_A_DECIMAL;
          TBSYS_LOG(WARN, "value begin with '.'");
        }
        if (false == ret && static_cast<int64_t>(strlen(stringp)) > OB_MAX_DECIMAL_WIDTH - OB_MAX_PRECISION_WIDTH)
        {
          err = OB_NOT_A_DECIMAL;
          ret = true;
          TBSYS_LOG(WARN, "value integer too long, len = %ld, max_len = %ld", 
            tmp - pos, OB_MAX_DECIMAL_WIDTH - OB_MAX_PRECISION_WIDTH);
        }
        //如果字符串最后一个是'.'
        if (false == ret && '.' == value[length - 1])
        {
          err = OB_NOT_A_DECIMAL;
          ret = true;
          TBSYS_LOG(WARN, "value should not end with char '.'");
        }
        if (false == ret)
        {
          while ('\0' != *pos)
          {
            if (!isdigit(*pos))
            {
              err = OB_NOT_A_DECIMAL;
              ret = true;
              TBSYS_LOG(WARN, "value isn't a decimal. the first char after sign is %c", *pos);
              break;
            }
            pos ++;
          }
        }
        if (false == ret && NULL == tmp)
        {
          has_fractional = false;
          ret = true;
        }
        else if (false == ret)
        {
          has_fractional = true;
        }
      }

      //判断小数部分是否满足要求

      if (OB_SUCCESS == err && false == ret)
      {     
        if (static_cast<int64_t>(strlen(tmp)) > OB_MAX_PRECISION_WIDTH)
        {
          err = OB_NOT_A_DECIMAL;
          ret = true;
          TBSYS_LOG(WARN, "value fractional too long, len = %ld, max_len = %ld", tmp - pos, OB_MAX_PRECISION_WIDTH);
        }
        pos = tmp;
        if (false == ret)
        {
          while ('\0' != *pos)
          {
            if (!isdigit(*pos))
            {
              err = OB_NOT_A_DECIMAL;
              ret = true;
              TBSYS_LOG(WARN, "value isn't a decimal. the char is %c, not a digit", *pos);
              break;
            }
            pos ++;
          }
        }
      }
      if (OB_SUCCESS != err)
      {
        TBSYS_LOG(WARN, "fail to check decimal. err = %d", err);
      }
      return err;
    } 

    int ObDecimalHelper::string_to_int(const char* value, const int64_t length, int64_t &integer, int64_t &fractional, int64_t &precision, bool &sign)
    {
      int err = OB_SUCCESS;
      if (NULL == value || length <= 0
          || length > OB_MAX_DECIMAL_WIDTH + 2
          || static_cast<int64_t>(strlen(value)) < length)
      {
        err = OB_INVALID_ARGUMENT;
        TBSYS_LOG(WARN, "invalid argument, value = %p, length = %ld", value, length);
      }
      bool has_fractional = false;
      bool has_sign = false;
      if (OB_SUCCESS == err)
      {
        err = check_decimal_str(value, length, has_fractional, has_sign, sign);
        if (OB_SUCCESS != err)
        {
          TBSYS_LOG(WARN, "fail to check decimal str. err = %d", err);
        }
      }
      char str[OB_ARRAY_SIZE];
      char *pos = str;
      char *tmp = str;
      if (OB_SUCCESS == err)
      {
        memcpy(str, value, length);
        str[length] = '\0';
        if(has_sign)
        {
          pos ++;
        }
        if (!has_fractional)
        {
          precision = 0;
          fractional = 0;
          integer = strtol(pos, &tmp, 10);
        }
        else
        {
          integer = strtol(pos, &tmp, 10);
          tmp ++;
          fractional = strtol(tmp, &pos, 10);
          precision = pos - tmp;
        }
      }
      if (OB_SUCCESS != err)
      {
        TBSYS_LOG(WARN, "fail to do string to int. err= %d", err);
      }
      return err;
    }

    //将小数部分提升precision个精度,
    //为了小数部分比较的方便
    //比如，原来精度为3，小数位为2，现在再提升2个精度，那么小数应该变为200
    int ObDecimalHelper::promote_fractional(int64_t &fractional, const int64_t precision)
    {
      int err = OB_SUCCESS;
      if (fractional < 0 || precision < 0 || precision > OB_MAX_PRECISION_WIDTH)
      {
        err = OB_INVALID_ARGUMENT;
        TBSYS_LOG(WARN, "invalid argument. fractional = %ld, precision =%ld", fractional, precision);
      }
      if (OB_SUCCESS == err)
      {
        for (int64_t i = 0; i < precision; i++)
        {
          fractional *= OB_DECIMAL_MULTIPLICATOR;
        }
      }
      return err;
    }

    /*未经过测试的保留函数 ，可能以后要用
      int  ObDecimalHelper::MULT(const int64_t multiplier, const int64_t multiplicand, int32_t *product, const int64_t array_size)
      {
      int err = OB_SUCCESS;
      bool end = false;
      if(product == NULL || array_size < 4)
      {
      err = OB_INVALID_ARGUMENT;TBSYS_LOG(WARN, "invalid argument.product = %p, array_size = %d", product, array_size);
      }
      if(err == OB_SUCCESS)
      {
      if(multiplier == 0 || multiplicand == 0)
      {
      for(int64_t i = 0; i < 4; i++)
      {
     *(product + i * sizeof(int32_t)) = 0;
     }
     end = true;
     }
     }

     if(end == false && err == OB_SUCCESS)
     {
     if(multiplier == 1)
     {
     *product = 0;
     *(product + sizeof(int64_t)) = multiplicand;
     end = true;
     }
     }

     if(end == false && err == OB_SUCCESS)
     {
     if(multiplicand == 1)
     {
     *product = 0;
     *(product + sizeof(int64_t)) = multiplier;
     end = true;
     }
     }

     if(end == false && err == OB_SUCCESS)
     {
     int64_t sign = 1;
     if((multiplier < 0 && multiplicand > 0)  || (multiplier > 0 && multiplicand < 0))
     {
     sign = -1;
     }
     else
     {
     sign = 1;
     }
     int64_t l_tmp_1 = (multiplier & OB_LEFT_PART_MAST_WITH_SIGN) >> 32;
     int64_t r_tmp_1 = multiplier & OB_RIGHT_PART_MAST;
     int64_t l_tmp_2 = (multiplicand & OB_LEFT_PART_MAST_WITH_SIGN) >> 32;
     int64_t r_tmp_2 = multiplicand & OB_RIGHT_PART_MAST;
     int64_t result_part_1 = l_tmp_1 * l_tmp_2;
     int64_t result_part_2 = r_tmp_1 * r_tmp_2;
     int64_t result_part_3 = (l_tmp_1 - r_tmp_1) * (r_tmp_2 - l_tmp_2) + result_part_1 + result_part_2;
    //最后一个int32_t的值
    r_tmp_1 = result_part_2 & OB_RIGHT_PART_MAST;
     *(product + 3 * sizeof(int32_t)) = static_cast<int32_t>(r_tmp_1);

    //第3个int32_t的值等于part_2的左半部分 + part_3的右半部分的和，取和的右半部分，和的左半部分是进位
    l_tmp_1 = (result_part_2 & OB_LEFT_PART_MAST_WITHOUT_SIGN) >> 32;
    r_tmp_1 = result_part_3 & OB_RIGHT_PART_MAST;
    //结果
    l_tmp_1 += r_tmp_1;
    r_tmp_2 = l_tmp_1 & OB_RIGHT_PART_MAST;
     *(product + 2 * sizeof(int32_t)) = static_cast<int32_t>(r_tmp_2);
    //进位
    l_tmp_2 = (l_tmp_1 & OB_LEFT_PART_MAST_WITHOUT_SIGN) >> 32;
    //第2个int32_t的值等于上面的进位 + part3的左半部分+ part1的右半部分，取右半部分，左半部分是进位
    l_tmp_1 = (result_part_3 & OB_LEFT_PART_MAST_WITHOUT_SIGN) >> 32;
    r_tmp_1 = result_part_1 & OB_RIGHT_PART_MAST;
    //结果
    r_tmp_2 = l_tmp_2 + l_tmp_1 + r_tmp_1;
    r_tmp_1 = r_tmp_2 & OB_RIGHT_PART_MAST;
    *(product + sizeof(int32_t)) = static_cast<int32_t>(r_tmp_1);
    //进位
    l_tmp_1 = (r_tmp_2 & OB_LEFT_PART_MAST_WITHOUT_SIGN) >> 32;

    //第一个int32_t的值等于part_1的左半部分+进位
    l_tmp_2 = (result_part_1 & OB_LEFT_PART_MAST_WITHOUT_SIGN) >> 32;
    *product = static_cast<int32_t>(l_tmp_2 + l_tmp_1);
    if(sign == -1)
    {
      for(int64_t i = 0; i < 4; i++)
      {
        if(*(product + i * sizeof(int32_t)) != 0)
        {
          *(product + i * sizeof(int32_t)) = (*(product + i * sizeof(int32_t))) * sign;
          break;
        }
      }
    }
    end = true;
  }
  return err;
  }
  */

    int ObDecimalHelper::ADD(ObDecimal &decimal, const ObDecimal &add_decimal,
        const ObTableProperty &gb_table_property)
    {
      int err = OB_SUCCESS;
      err = check_decimal(decimal);
      if (err != OB_SUCCESS)
      {
        err = OB_INVALID_ARGUMENT;
      }
      if (OB_SUCCESS == err)
      {
        err = check_decimal(add_decimal);
        if (err != OB_SUCCESS)
        {
          err = OB_INVALID_ARGUMENT;
        }
      }
      if (OB_SUCCESS == err)
      {
        if ((decimal.meta.value_.width_ != add_decimal.meta.value_.width_)
            || (decimal.meta.value_.precision_ != add_decimal.meta.value_.precision_))
        {
          err = OB_DECIMAL_PRECISION_NOT_EQUAL;
          TBSYS_LOG(WARN, "decimal precision not equal. one_meta.width = %d, one_meta.precision = %d, other_meta.width = %d, other_meta.precision = %d", decimal.meta.value_.width_, decimal.meta.value_.precision_, add_decimal.meta.value_.width_, add_decimal.meta.value_.precision_);
        }
      }
      if (OB_SUCCESS == err && (decimal.meta.value_.sign_ == add_decimal.meta.value_.sign_))
      {
        //如果符号位相同
        decimal.fractional += add_decimal.fractional;
        decimal.integer += add_decimal.integer;
        err = is_over_flow(decimal.meta.value_.width_, decimal.meta.value_.precision_, decimal.integer, decimal.fractional, gb_table_property);
        if (err != OB_SUCCESS && err != OB_DECIMAL_OVERFLOW_WARN)
        {
          TBSYS_LOG(WARN, "fail to judge is_over_flow.err= %d", err);
        }
        else
        {
          err = OB_SUCCESS;
        }
      }
      if (OB_SUCCESS == err && decimal.meta.value_.sign_ != add_decimal.meta.value_.sign_)
      {
        //符号位不相同,则判断大小
        if (decimal.integer < add_decimal.integer)
        {
          if (decimal.fractional < add_decimal.fractional)
          {
            decimal.integer = add_decimal.integer - decimal.integer;
            decimal.fractional = add_decimal.fractional - decimal.fractional;
            decimal.meta = add_decimal.meta;
          }
          else
          {
            decimal.integer = add_decimal.integer - decimal.integer -1;
            int64_t value = 0;
            err = get_min_express_number(decimal.meta.value_.precision_, value);
            if (OB_SUCCESS == err)
            {
              decimal.fractional = add_decimal.fractional + value - decimal.fractional;
              decimal.meta = add_decimal.meta;
            }
          }
        }
        else
        {
          if (decimal.integer == add_decimal.integer)
          {
            if (decimal.fractional >= add_decimal.fractional)
            {
              decimal.integer = 0;
              decimal.fractional = decimal.fractional - add_decimal.fractional;
            }
            else
            {
              decimal.integer = 0;
              decimal.fractional = add_decimal.fractional - decimal.fractional;
              decimal.meta = add_decimal.meta;
            }
          }
          else
          {
            if (decimal.fractional < add_decimal.fractional)
            {
              decimal.integer = decimal.integer - add_decimal.integer -1;
              int64_t tmp_val = 0;
              err = get_min_express_number(decimal.meta.value_.precision_ + 1, tmp_val);
              if (OB_SUCCESS == err)
              {
                decimal.fractional = decimal.fractional + tmp_val - add_decimal.fractional;
              }
            }
            else
            {
              decimal.integer = decimal.integer - add_decimal.integer;
              decimal.fractional = decimal.fractional - add_decimal.fractional;
            }
          }
        }
      }
      if (OB_SUCCESS != err)
      {
        TBSYS_LOG(WARN, "fail to do add. err= %d", err);
      }
      return err;
    }

  int ObDecimalHelper::MUL(const ObDecimal &decimal, int64_t multiplicand,
      int64_t &product_int, int64_t &product_fra, bool &sign,
      const ObTableProperty &gb_table_property)
  {
    int err = OB_SUCCESS;
    if (multiplicand > INT32_MAX)
    {
      err = OB_INVALID_ARGUMENT;
      TBSYS_LOG(WARN, "invalid argument, mulitplicand = %ld", multiplicand);
    }
    if (OB_SUCCESS == err)
    {
      err = check_decimal(decimal);
      if (err != OB_SUCCESS)
      {
        err = OB_INVALID_ARGUMENT;
      }
    }
    bool end = false;
    if (OB_SUCCESS == err)
    {
      if (0 == multiplicand || (0 == decimal.integer && 0 == decimal.fractional))
      {
        end = true;
        product_int = 0;
        product_fra = 0;
        sign = true;
      }
    }
    if (OB_SUCCESS == err && false == end)
    {
      int8_t str_product[OB_ARRAY_SIZE];
      sign = true;
      if ((decimal.meta.value_.sign_ == -1  && multiplicand > 0)
          || (decimal.meta.value_.sign_ == 0 && multiplicand < 0))
      {
        sign = false;
      }
      if (multiplicand < 0)
      {
        multiplicand = labs(multiplicand);
      }
      err = array_mul(decimal.integer, decimal.fractional, decimal.meta.value_.precision_,  multiplicand, str_product, OB_ARRAY_SIZE);
      if (OB_SUCCESS != err)
      {
        TBSYS_LOG(WARN, "fail to do MULT. err = %d", err);
      }
      else
      {
        //此处需要判断整数部分是否过长。
        //去掉前面的零以后判断
        int64_t i = 0;
        while (0 == str_product[i])
        {
          i ++;
        }
        if (OB_ARRAY_SIZE - i > decimal.meta.value_.width_)
        {
          if (SQL_DEFAULT == gb_table_property.mode)
          {
            err = OB_DECIMAL_OVERFLOW_WARN;
            TBSYS_LOG(WARN, "integer_number overflow, shortted to the max decimal");
            err = get_max_express_number(decimal.meta.value_.width_ - decimal.meta.value_.precision_, product_int);
            if (OB_SUCCESS == err)
            {
              err = get_max_express_number(decimal.meta.value_.precision_, product_fra);
            }
          }
          else
          {
            err = OB_DECIMAL_UNLEGAL_ERROR;
            TBSYS_LOG(WARN, "integer_number overflow.");
          }
        }
        else
        {
          err = byte_to_int(product_fra, &str_product[OB_ARRAY_SIZE - decimal.meta.value_.precision_], decimal.meta.value_.precision_);
          if (OB_SUCCESS == err && i < OB_ARRAY_SIZE - decimal.meta.value_.precision_)
          {
            err = byte_to_int(product_int, &str_product[i], OB_ARRAY_SIZE - decimal.meta.value_.precision_ - i);
          }
          else
          {
            product_int = 0;
          }
        }
      }
    }
    if (OB_SUCCESS != err)
    {
      TBSYS_LOG(WARN, "fail to do mul. err= %d", err);
    }
    return err;
  }

  int ObDecimalHelper::int_to_byte(int8_t* array, const int64_t size, int64_t &value_len, const int64_t value, const bool in_head)
  {
    int err = OB_SUCCESS;
    if (NULL == array || size != OB_ARRAY_SIZE || value < 0)
    {
      err = OB_INVALID_ARGUMENT;
      TBSYS_LOG(WARN, "invalid argument. array=%p, size=%ld, should be %ld, value=%ld", array, size, OB_ARRAY_SIZE, value);
    }
    int64_t i = 0;
    int64_t tmp_value = value;
    if (OB_SUCCESS == err)
    {
      if (0 == tmp_value)
      {
        value_len = 1;
      }
      else
      {
        while (0 < tmp_value)
        {
          array[size - 1 - i] = static_cast<int8_t>(tmp_value % OB_DECIMAL_MULTIPLICATOR);
          i ++;
          tmp_value = tmp_value / OB_DECIMAL_MULTIPLICATOR;   
        }
        value_len = i;
      }
    }
    if (OB_SUCCESS == err && in_head == true)
    {
      for(int64_t i = 0; i < value_len; i++)
      {
        array[i] = array[size - value_len + i];
        array[size - value_len + i] = 0;
      }
    }
    return err;
  }

  int ObDecimalHelper::byte_to_int(int64_t &value, const int8_t *array, const int64_t value_len)
  {
    int err = OB_SUCCESS;
    if (0 >= value_len || NULL == array || OB_MAX_DECIMAL_WIDTH - OB_MAX_PRECISION_WIDTH < value_len)
    {
      err = OB_INVALID_ARGUMENT;
      TBSYS_LOG(WARN, "invalid argument. array=%p, value_len=%ld", array, value_len);
    }
    int64_t tmp_value = 0;
    if (OB_SUCCESS == err)
    {
      for (int64_t i = 0; i < value_len; i++)
      {
        if (!isdigit(array[i] + '0'))
        {
          err = OB_INVALID_ARGUMENT;
          TBSYS_LOG(WARN, "array have some char[%d] here.", array[i]);
          break;
        }
        else
        {
          tmp_value = tmp_value * OB_DECIMAL_MULTIPLICATOR + array[i];
        }
      }
      value = tmp_value;
    }
    if (OB_SUCCESS != err)
    {
      TBSYS_LOG(WARN, "fail to do byte to int. err= %d", err);
    }
    return err;
  }

  int ObDecimalHelper::get_digit_number(const int64_t value, int64_t &number)
  {
    int err = OB_SUCCESS;
    if (0 > value)
    {
      err = OB_INVALID_ARGUMENT;
      TBSYS_LOG(WARN, "invalid argument. value=%ld, should be no less than 0", value);
    }
    if (OB_SUCCESS == err)
    {
      if (0 == value)
      {
        number = 1;
      }
      else
      {
        int64_t i = 0;
        int64_t tmp_value = 1;
        while (value >= tmp_value)
        {
          tmp_value *= OB_DECIMAL_MULTIPLICATOR;
          i ++;
        }
        number = i;
      }
    }
    return err;
  } 

  //算法描述：用被乘数的每一位去乘以乘数，将结果移位相加后保留在数组中，
  int ObDecimalHelper::array_mul(const int64_t integer, const int64_t fractional,
      const int64_t precision, const int64_t multiplicand,
      int8_t *product, const int64_t length)
  {
    int err = OB_SUCCESS;
    if (product == NULL || length != OB_ARRAY_SIZE || integer < 0
        || fractional < 0 || precision < 0 || fractional > INT32_MAX
        || multiplicand < 0 || multiplicand > INT32_MAX
        || precision > OB_MAX_PRECISION_WIDTH)
    {
      err = OB_INVALID_ARGUMENT;
      TBSYS_LOG(WARN, "invalid argument, product = %p, length = %ld, the min length should be 64, integer = %ld, fractional = %ld, precision = %ld, multiplicand = %ld", product, length, integer, fractional, precision, multiplicand);
    } 

    //记录每次相乘的结果，存入int8_t数组，并与已有结果进行以为相加
    int64_t int_result = 0;
    int8_t array_result[OB_ARRAY_SIZE];
    int8_t array_integer[OB_ARRAY_SIZE];  
    int8_t array_fractional[OB_ARRAY_SIZE];
    int64_t num_integer = 0;
    int64_t num_fractional = 0;
    if (OB_SUCCESS == err)
    {
      memset(product, 0, length);
      memset(array_result, 0, OB_ARRAY_SIZE);
      err = int_to_byte(array_integer, OB_ARRAY_SIZE, num_integer, integer, true);
      if (OB_SUCCESS == err)
      {
        err = int_to_byte(array_fractional, OB_ARRAY_SIZE, num_fractional, fractional, true);
      }
      if (OB_SUCCESS != err)
      {
        TBSYS_LOG(WARN, "fail to tranlate to int8_t array. err = %d", err);
      }
    }
    //先用小数部分的每一位来乘以乘数
    int64_t pos = 0;
    if (OB_SUCCESS == err && fractional != 0)
    {
      int64_t i = num_fractional - 1;
      int64_t tmp = 0;
      while (i >= 0 && OB_SUCCESS == err)
      {
        int_result = multiplicand * array_fractional[i];
        err = int_to_byte(array_result, OB_ARRAY_SIZE, tmp, int_result, false);
        if (OB_SUCCESS == err)
        {
          err = byte_add(product, array_result, OB_ARRAY_SIZE, pos);
          pos ++;
          memset(array_result, 0, OB_ARRAY_SIZE * sizeof(int8_t));
        }
        else
        {
          break;
        }
        i --;
      }
    }
    //如果小数部分以0开始，那么需要跳过0的乘法，但需要进行乘积的移位
    if (OB_SUCCESS == err)
    {
      int i = 0;
      int64_t tmp = 0;
      err = get_min_express_number(precision, tmp);
      if (OB_SUCCESS == err)
      {
        while (fractional < tmp)
        {
          tmp = tmp / OB_DECIMAL_MULTIPLICATOR;
          i ++;
        }
        pos += i;
      }
    }
    //进行整数部分的乘法
    if (OB_SUCCESS == err)
    {
      int64_t i = num_integer - 1;
      int64_t tmp = 0;
      while (i >= 0 && OB_SUCCESS == err)
      {
        int_result = multiplicand * array_integer[i];
        err = int_to_byte(array_result, OB_ARRAY_SIZE, tmp, int_result, false);
        if (OB_SUCCESS == err)
        {
          err = byte_add(product, array_result, OB_ARRAY_SIZE, pos);
          pos ++;
          memset(array_result, 0, sizeof(int8_t) * OB_ARRAY_SIZE);
        }
        else
        {
          break;
        }
        i --;
      }
    }
    if (OB_SUCCESS != err)
    {
      TBSYS_LOG(WARN, "fail to do array_mul. err= %d", err);
    }
    return err;
  }

  int ObDecimalHelper::byte_add(int8_t *add, const int8_t *adder, const int64_t size, const int64_t pos)
  {
    int err = OB_SUCCESS;
    if (NULL == add || NULL == adder || size != OB_ARRAY_SIZE || pos < 0 || pos > OB_ARRAY_SIZE)
    {
      err = OB_INVALID_ARGUMENT;
      TBSYS_LOG(WARN, "invalid argument. add =%p, adder=%p, size=%ld, pos=%ld", add, adder, size, pos);
    }
    if (OB_SUCCESS == err)
    {
      int64_t is_carry = 0;
      for (int64_t i = size - 1; i >= pos; i--)
      {
        if (isdigit(add[i - pos] + '0') && isdigit(adder[i] + '0') && isdigit(add[i] + '0'))
        {
          add[i - pos] = static_cast<int8_t>(add[i - pos] + adder[i] + is_carry);
          is_carry = 0;
          if (add[i - pos] >= 10)
          {
            add[i - pos] = static_cast<int8_t>(add[i - pos] - 10);
            is_carry = 1;
          }
        }   
        else
        {
          err = OB_INVALID_ARGUMENT;
          TBSYS_LOG(WARN, "no_digit here, *add = %d, *adder = %d", add[i + pos], adder[i]);
          break;
        }
      }
    }
    return err;
  }

  //算法描述:模拟笔算的过程，从高位到低位，采用试商法获的每位的商和余数，直到满足精度
  //或者除法结束,计算过程中，是从被除数的首位开始试商，所以保存商的字符数组可能以0开始
  int ObDecimalHelper::array_div(const int64_t integer, const int64_t fractional,
      const int64_t precision, const int64_t divisor,
      int8_t *consult, const int64_t consult_len)
  {
    int err = OB_SUCCESS;
    if (NULL == consult || integer < 0
        || fractional < 0 || precision < 0
        || divisor <= 0 || consult_len != OB_ARRAY_SIZE
        || fractional > INT32_MAX
        || precision > OB_MAX_PRECISION_WIDTH)
    {
      err = OB_INVALID_ARGUMENT;
      TBSYS_LOG(WARN, "invalid arugment. integer = %ld, fractioanl = %ld, precision = %ld, divisor = %ld, consult = %p, consult_len = %ld", integer, fractional, precision, divisor, consult, consult_len);
    }
    int8_t div[OB_ARRAY_SIZE];
    int64_t len_int = 0;
    int64_t len_fra = 0;
    int64_t len_zero = 0;

    //计算小数前面0的个数    
    if (OB_SUCCESS == err)
    {
      int64_t tmp = 0;
      err = get_min_express_number(precision, tmp);
      if (OB_SUCCESS == err)
      {
        while (fractional < tmp)
        {
          tmp = tmp / OB_DECIMAL_MULTIPLICATOR;
          len_zero ++;
        }
      }
    }
    //被除数的长度由3部分组成，len_int, len_zero, len_fra
    //将被除数写入数组
    if (OB_SUCCESS == err)
    {
      err = int_to_byte(div, OB_ARRAY_SIZE, len_int, integer, true);
      if (OB_SUCCESS == err)
      {
        memset(div + len_int, 0, len_zero * sizeof(int8_t));
        int64_t tmp = len_int + len_zero;
        if (fractional == 0)
        {
          len_fra = 0;
        }
        else
        {
          err = int_to_byte(&div[tmp], OB_ARRAY_SIZE, len_fra, fractional, true);
        }
        div[tmp + len_fra] = 0;
      }
    }
    if (OB_SUCCESS  == err)
    {
      int64_t start_pos = 0;
      int64_t end_pos = 0;
      int64_t consult_pos = 0;
      int64_t number = 0;
      int64_t remainder = 0;
      int64_t rem_len = 0;
      int64_t tmp = 0;
      //商要多取一位
      while (consult_pos < len_int + len_zero + len_fra + 1)
      {
        err = byte_to_int(number, div + start_pos, end_pos - start_pos + 1);
        if (OB_SUCCESS == err)
        {
          if (number < divisor)
          {
            end_pos ++;
            consult[consult_pos ++] = 0;
          }
          else
          {
            consult[consult_pos] = static_cast<int8_t>(number / divisor);
            remainder = number % divisor;
            if (0 != remainder)
            {
              get_digit_number(remainder, rem_len);
              int_to_byte(&div[end_pos - rem_len + 1], OB_ARRAY_SIZE, tmp, remainder, true);
              start_pos = end_pos - rem_len + 1;
            }
            else
            {
              start_pos = end_pos + 1;
            }
            consult_pos ++;
            end_pos++;
          }
        }
        else
        {
          break;
        }
      }
    }
    if (OB_SUCCESS != err)
    {
      TBSYS_LOG(WARN, "fail to do array_div. err= %d", err);
    }
    return err;
  }

  int ObDecimalHelper::DIV(const ObDecimal &decimal,
      int64_t divisor, int64_t &consult_int, int64_t &consult_fra, bool &sign,
      const ObTableProperty &gb_table_property)
  {
    int err = OB_SUCCESS;
    err = check_decimal(decimal);
    if (OB_SUCCESS != err)
    {
      err = OB_INVALID_ARGUMENT;
    }
    if (OB_SUCCESS == err)
    {
      if (0 == divisor)
      {
        if (true == gb_table_property.error_division_by_zero  && SQL_DEFAULT == gb_table_property.mode)
        {
          err = OB_OBJ_DIVIDE_BY_ZERO;
          consult_int = 0;
          consult_fra = 0;
          TBSYS_LOG(WARN, "divide by zero warn.");
        }
        else if (true == gb_table_property.error_division_by_zero && SQL_STRICT == gb_table_property.mode)
        {
          err = OB_OBJ_DIVIDE_ERROR;
          TBSYS_LOG(ERROR, "divide by zero error.");
        }
        else
        {
          consult_int = 0;
          consult_fra = 0;
        }
      }
      else
      {
        if (0 == decimal.integer && 0 == decimal.fractional)
        {
          consult_int = 0;
          consult_fra = 0;
          sign = true;
        }
        else
        {
          int8_t result[OB_ARRAY_SIZE];
          sign = true;
          if ((decimal.meta.value_.sign_ == -1  && divisor > 0)
              || (decimal.meta.value_.sign_ == 0 && divisor < 0))
          {
            sign = false;
          }
          if (divisor < 0)
          {
            divisor = labs(divisor);
          }
          err = array_div(decimal.integer, decimal.fractional, decimal.meta.value_.precision_, divisor, result, OB_ARRAY_SIZE);
          if (err != OB_SUCCESS)
          {
            TBSYS_LOG(WARN, "fail to do div. err = %d", err);
          }
          else
          {
            int64_t int_len = 0;
            err = get_digit_number(decimal.integer, int_len);
            if (OB_SUCCESS == err)
            {
              err = byte_to_int(consult_int, result, int_len);
            }
            if (OB_SUCCESS == err)
            {
              err = byte_to_int(consult_fra, &result[int_len], decimal.meta.value_.precision_ + 1);
            }
            int64_t max_fra = 0;
            //除法运算，不会出现整数部分的溢出
            if (OB_SUCCESS == err)
            {
              err = get_max_express_number(decimal.meta.value_.precision_, max_fra);
            }
            if (OB_SUCCESS == err)
            {
              consult_fra /= OB_DECIMAL_MULTIPLICATOR;      
              if (result[int_len + decimal.meta.value_.precision_] > 4)
              {         
                if (consult_fra == max_fra)
                {
                  consult_fra = 0;
                  consult_int ++;          
                }
                else
                {
                  consult_fra ++;
                }
              }
            }
          }    
        } 
      }
    }
    if (OB_SUCCESS != err)
    {
      TBSYS_LOG(WARN, "fail to do div. err= %d", err);
    }
    return err;
  }

  int ObDecimalHelper::is_over_flow(const int64_t width, const int64_t precision,
      int64_t &integer, int64_t &fractional, const ObTableProperty &gb_table_property)
  {
    int err = OB_SUCCESS;
    if (width <= 0 || precision <= 0
        || integer < 0 || fractional < 0
        || width < precision || precision > OB_MAX_PRECISION_WIDTH
        || width > OB_MAX_DECIMAL_WIDTH || fractional > INT32_MAX)
    {
      err = OB_INVALID_ARGUMENT;
      TBSYS_LOG(WARN, "invalid argument. width = %ld, precision = %ld, integer = %ld, fractional = %ld", width, precision, integer, fractional);
    }
    int64_t max_integer = 0;
    int64_t max_fractional = 0;
    if (OB_SUCCESS == err)
    {
      err = get_max_express_number(width - precision, max_integer);
      if (err != OB_SUCCESS)
      {
        TBSYS_LOG(WARN, "fail to get max express number. width = %ld, err = %d", width - precision, err);
      }
    }
    if (OB_SUCCESS == err)
    {
      err = get_max_express_number(precision, max_fractional);
      if (err != OB_SUCCESS)
      {
        TBSYS_LOG(WARN, "fail to get max express number. width = %ld, err = %d", precision, err);
      }
    }
    if (OB_SUCCESS == err)
    {
      if (fractional > max_fractional)
      {
        fractional -= (max_fractional + 1);
        integer ++;
      }
      if (integer > max_integer)
      {
        if (SQL_DEFAULT == gb_table_property.mode)
        {
          err = OB_DECIMAL_OVERFLOW_WARN;
          TBSYS_LOG(WARN, "integer_number overflow, shortted to the max decimal");
          integer = max_integer;
          fractional = max_fractional;
        }
        else
        {
          err = OB_DECIMAL_UNLEGAL_ERROR;
          TBSYS_LOG(WARN, "integer_number overflow.");
        }
      }
    }
    if (OB_SUCCESS != err)
    {
      TBSYS_LOG(WARN, "fail to do is_over_flow. err= %d", err);
    }
    return err;
  }

  int ObDecimalHelper::decimal_format(const int64_t width, const int64_t precision, int64_t &integer,
      int64_t &fractional, const ObTableProperty &gb_table_property)
  {
    int err = OB_SUCCESS;
    if (width <= 0 || precision <= 0
        || integer < 0 || fractional < 0
        || width < precision || precision > OB_MAX_PRECISION_WIDTH
        || width > OB_MAX_DECIMAL_WIDTH || fractional > INT32_MAX)
    {
      err = OB_INVALID_ARGUMENT;
      TBSYS_LOG(WARN, "invalid argument. width = %ld, precision = %ld, integer = %ld, fractional = %ld", width, precision, integer, fractional);
    }
    int64_t max_integer = 0;
    int64_t max_fractional = 0;
    if (OB_SUCCESS == err)
    {
      err = get_max_express_number(width - precision, max_integer);
      if (err != OB_SUCCESS)
      {
        TBSYS_LOG(WARN, "fail to get max express number. width = %ld, err = %d", width - precision, err);
      }
    }
    if (OB_SUCCESS == err)
    {
      err = get_max_express_number(precision, max_fractional);
      if (err != OB_SUCCESS)
      {
        TBSYS_LOG(WARN, "fail to get max express number. width = %ld, err = %d", width - precision, err);
      }
    }
    if (OB_SUCCESS == err)
    {
      bool is_carry = false;
      err = round_off(fractional, is_carry, precision);
      if (OB_SUCCESS == err && is_carry)
      {
        integer ++;
      }
      if (integer > max_integer && OB_SUCCESS == err)
      {
        if (SQL_DEFAULT == gb_table_property.mode)
        {
          err = OB_DECIMAL_OVERFLOW_WARN;
          TBSYS_LOG(WARN, "integer_number overflow, shortted to the max decimal");
          integer = max_integer;
          fractional = max_fractional;
        }
        else
        {
          err = OB_DECIMAL_UNLEGAL_ERROR;
          TBSYS_LOG(WARN, "integer_number overflow.");
        }
      }
    }
    return err;
  }

  int ObDecimalHelper::decimal_compare(ObDecimal decimal, ObDecimal other_decimal)
  {
    int err = OB_SUCCESS;
    err = check_decimal(decimal);
    if (OB_SUCCESS != err)
    {
      err = OB_INVALID_ARGUMENT;
    }
    if (OB_SUCCESS == err)
    {
      err = check_decimal(other_decimal);
      if (OB_SUCCESS != err)
      {
        err = OB_INVALID_ARGUMENT;
      }
    }
    //如果小数的精度不相同，则提升小数的精度到相同
    if (OB_SUCCESS == err)
    {
      //提升精度较小者的小数部分的值
      if (decimal.meta.value_.precision_ != other_decimal.meta.value_.precision_)
      {
        if (decimal.meta.value_.precision_ > other_decimal.meta.value_.precision_)
        {
          err = promote_fractional(other_decimal.fractional, decimal.meta.value_.precision_ - other_decimal.meta.value_.precision_);
        }
        else
        {
          err = promote_fractional(decimal.fractional, other_decimal.meta.value_.precision_ - decimal.meta.value_.precision_);
        }
      }
      //如果符号位相同
      if (OB_SUCCESS == err && decimal.meta.value_.sign_ == other_decimal.meta.value_.sign_)
      {
        if (decimal.integer == other_decimal.integer && decimal.fractional == other_decimal.fractional)
        {
          err = OB_DECIMAL_EQUAL;
        }
        else
        {
          if (decimal.meta.value_.sign_ == 0)
          {
            if (decimal.integer < other_decimal.integer || (decimal.integer == other_decimal.integer  && decimal.fractional < other_decimal.fractional))
            {
              err = OB_DECIMAL_LESS;
            }
            else
            {
              err = OB_DECIMAL_BIG;;
            }
          }
          else
          {
            if (decimal.integer < other_decimal.integer || (decimal.integer == other_decimal.integer && decimal.fractional < other_decimal.fractional))
            {
              err = OB_DECIMAL_BIG;
            }
            else
            {
              err = OB_DECIMAL_LESS;
            }
          }
        }
      }
      else if (OB_SUCCESS == err)
      {
        err = static_cast<int>((decimal.meta.value_.sign_ == 0) ? OB_DECIMAL_BIG : OB_DECIMAL_LESS);
      }
    }
    return err;
  }

  int ObDecimalHelper::check_decimal(const ObDecimal &decimal)
  {
    int err = OB_SUCCESS;
    if (decimal.integer < 0 || decimal.fractional < 0
        || decimal.meta.value_.width_ < decimal.meta.value_.precision_
        || decimal.meta.value_.precision_ < 0
        || decimal.meta.value_.width_ > OB_MAX_DECIMAL_WIDTH
        || decimal.meta.value_.precision_ > OB_MAX_PRECISION_WIDTH)
    {
      TBSYS_LOG(WARN, "invalid_argument, integer = %ld, fractional = %ld, "
                      "width = %d, precision = %d", 
        decimal.integer, decimal.fractional, decimal.meta.value_.width_, 
        decimal.meta.value_.precision_);
      err = OB_INVALID_ARGUMENT;
    }
    int64_t max_integer = 0;
    int64_t max_fractional = 0;
    if (OB_SUCCESS == err)
    {
      err = get_max_express_number(decimal.meta.value_.width_ - decimal.meta.value_.precision_, max_integer);
      if (OB_SUCCESS == err && max_integer < decimal.integer)
      {
        err = OB_INVALID_ARGUMENT;
      }
    }
    if (OB_SUCCESS == err)
    {
      err = get_max_express_number(decimal.meta.value_.precision_, max_fractional);
      if (OB_SUCCESS == err && max_fractional < decimal.fractional)
      {
        err = OB_INVALID_ARGUMENT;
      }
    }
    return err;
  }
  }
}

