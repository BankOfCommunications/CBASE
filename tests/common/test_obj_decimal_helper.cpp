#include "common/ob_object.h"
#include "common/ob_decimal_helper.h"
#include <gtest/gtest.h>

using namespace oceanbase::common;

namespace oceanbase
{
  namespace tests
  {
    namespace common
    {
      class TestObjDecimal : public ::testing::Test
      {
        public:
          virtual void SetUp()
          {
          }
          virtual void TearDown()
          {
          }
      };
      class TestObjDecimalHelper : public ObDecimalHelper
      {
        public:
          static int promote_fractional(int64_t &fractional, const int64_t precision)
          {
            return ObDecimalHelper::promote_fractional(fractional, precision);
          }
          static int array_mul(const int64_t integer,
              const int64_t fractional,
              const int64_t precision,
              const int64_t multiplicand,
              int8_t *product,
              const int64_t length)
          {
            return ObDecimalHelper::array_mul(integer, fractional, precision, multiplicand, product, length);
          }
          static int array_div(const int64_t integer,
              const int64_t fractional,
              const int64_t precision,
              const int64_t divisor,
              int8_t *consult,
              const int64_t consult_len)
          {
            return ObDecimalHelper::array_div(integer, fractional, precision, divisor, consult, consult_len);
          }

          static int is_over_flow(const int64_t width,
              const int64_t precision,
              int64_t &integer,
              int64_t &fractional,
              const ObTableProperty &gb_table_property)
          {
            return ObDecimalHelper::is_over_flow(width, precision, integer, fractional, gb_table_property);
          }
          static int decimal_format(const int64_t width,
              const int64_t precision,
              int64_t &integer,
              int64_t &fractional,
              const ObTableProperty &gb_table_property)
          {
            return ObDecimalHelper::decimal_format(width, precision, integer, fractional, gb_table_property);
          }
          static int check_decimal_str(const char *value,
              const int64_t length,
              bool &has_fractional,
              bool &has_sign,
              bool &sign)
          {
            return ObDecimalHelper::check_decimal_str(value, length, has_fractional, has_sign, sign);
          }
          static int int_to_byte(int8_t* array, const int64_t size, int64_t &value_len, const int64_t value, const bool in_head)
          {
            return ObDecimalHelper::int_to_byte(array, size, value_len, value, in_head);
          }
          static int get_digit_number(const int64_t value, int64_t &number)
          {
            return ObDecimalHelper::get_digit_number(value, number);
          }
          static int byte_to_int(int64_t &value, const int8_t *array, const int64_t value_len)
          {
            return ObDecimalHelper::byte_to_int(value, array, value_len);
          }
          static int byte_add(int8_t *add, const int8_t *adder, const int64_t size, const int64_t pos)
          {
            return ObDecimalHelper::byte_add(add, adder, size, pos);
          }
      };

      //对小数的精度进行提升
      //int promote_fractional(int64_t &fractional, const int64_t precision)
      TEST_F(TestObjDecimal, promote_fractional)
      {
        int64_t fractional = 0;
        int64_t precision = 0;
        //输入小数部分为负数，或者精度为负数
        {
          fractional = -1;
          EXPECT_EQ(OB_INVALID_ARGUMENT, TestObjDecimalHelper::promote_fractional(fractional, precision));
        }
        {
          precision = -1;
          fractional = 1;
          EXPECT_EQ(OB_INVALID_ARGUMENT, TestObjDecimalHelper::promote_fractional(fractional, precision));
        }
        {
          precision = 1;
          fractional = 0;
          EXPECT_EQ(OB_SUCCESS, TestObjDecimalHelper::promote_fractional(fractional, precision));
          EXPECT_EQ(0, fractional);
        }
        {
          precision = 0;
          fractional = 1;
          EXPECT_EQ(0, TestObjDecimalHelper::promote_fractional(fractional, precision));
        }
        //精度小于小数的宽度
        {
          precision = 3;
          fractional = 99312;
          EXPECT_EQ(OB_SUCCESS, TestObjDecimalHelper::promote_fractional(fractional, precision));
          EXPECT_EQ(99312000, fractional);
        }
        //精度大于宽度
        {
          precision = 4;
          fractional = 12;
          EXPECT_EQ(OB_SUCCESS, TestObjDecimalHelper::promote_fractional(fractional, precision));
          EXPECT_EQ(120000, fractional);
        }
      }

      // static int array_mul(const int64_t integer,const int64_t fractional,const int64_t precision,
      //     const int64_t multiplicand, char *product, const int64_t length);
      TEST_F(TestObjDecimal, array_mul)
      {
        int64_t integer = 12;
        int64_t fractional = 3;
        int64_t precision = 1;
        int64_t mul = 25;
        int8_t product[64];
        int64_t length = 64;

        //====================
        //输入参数不对
        //====================
        {
          integer = -2;
          EXPECT_EQ(OB_INVALID_ARGUMENT, TestObjDecimalHelper::array_mul(integer, fractional, precision, mul, product, length));
        }
        {
          integer = 12;
          fractional = -1;
          EXPECT_EQ(OB_INVALID_ARGUMENT, TestObjDecimalHelper::array_mul(integer, fractional, precision, mul, product, length));
        }
        {
          precision = -1;
          fractional = 12;
          EXPECT_EQ(OB_INVALID_ARGUMENT, TestObjDecimalHelper::array_mul(integer, fractional, precision, mul, product, length));
        }
        {
          mul = -3;
          precision = 12;
          EXPECT_EQ(OB_INVALID_ARGUMENT, TestObjDecimalHelper::array_mul(integer, fractional, precision, mul, product, length));
        }
        {
          length = -1;
          mul = 12;
          EXPECT_EQ(OB_INVALID_ARGUMENT, TestObjDecimalHelper::array_mul(integer, fractional, precision, mul, product, length));
        }
        {
          length = 53;
          mul = 12;
          EXPECT_EQ(OB_INVALID_ARGUMENT, TestObjDecimalHelper::array_mul(integer, fractional, precision, mul, product, length));
        }
        {
          length = 64;
          int8_t *buf = NULL;
          EXPECT_EQ(OB_INVALID_ARGUMENT, TestObjDecimalHelper::array_mul(integer, fractional, precision, mul, buf, length));
        }

        //===================
        //各种类型数据的计算
        //===================
        integer = 12;
        fractional = 3;
        precision = 1;
        mul = 25;
        length = 64;
        //整数部分为0 0.3 * 25 = 7.5
        {
          int64_t val = 0;
          EXPECT_EQ(0, TestObjDecimalHelper::array_mul(val, fractional, precision, mul, product, length));
          EXPECT_EQ(5, product[length - 1]);
          EXPECT_EQ(7, product[length - 2]);
        }
        //小数部分为0 12.0 * 25 = 300.0
        {
          int64_t val = 0;
          EXPECT_EQ(0, TestObjDecimalHelper::array_mul(integer, val, precision, mul, product, length));
          EXPECT_EQ(0, product[length - 1]);
          EXPECT_EQ(0, product[length - 2]);
          EXPECT_EQ(0, product[length - 3]);
          EXPECT_EQ(3, product[length - 4]);
        }
        //整数部分和小数部分都为0， 0.0 * 25 = 0.0；
        {
          int64_t val = 0;
          EXPECT_EQ(0, TestObjDecimalHelper::array_mul(val, val, precision, mul, product, length));
          EXPECT_EQ(0, product[length - 1]);
          EXPECT_EQ(0, product[length - 2]);
        }
        //被乘数为0 12.3 * 0 = 0.0
        {
          int64_t val = 0;
          EXPECT_EQ(0, TestObjDecimalHelper::array_mul(integer, fractional, precision, val, product, length));
          EXPECT_EQ(0, product[length - 1]);
          EXPECT_EQ(0, product[length - 2]);
        }
        //小数点后为0的情况 12.005 * 25 = 300.125
        {
          int64_t val = 5;
          int64_t pre = 3;
          EXPECT_EQ(0, TestObjDecimalHelper::array_mul(integer, val, pre, mul, product, length));
          EXPECT_EQ(5, product[length - 1]);
          EXPECT_EQ(2, product[length - 2]);
          EXPECT_EQ(1, product[length - 3]);
          EXPECT_EQ(0, product[length - 4]);
          EXPECT_EQ(0, product[length - 5]);
          EXPECT_EQ(3, product[length - 6]);
        }
        //整数位0， 小数点后位为0 0.003 * 25 = 0.075
        {
          int inte = 0;
          int64_t val = 3;
          int64_t pre = 3;
          EXPECT_EQ(0, TestObjDecimalHelper::array_mul(inte, val, pre, mul, product, length));
          EXPECT_EQ(5, product[length - 1]);
          EXPECT_EQ(7, product[length - 2]);
          EXPECT_EQ(0, product[length - 3]);
          EXPECT_EQ(0, product[length - 4]);      
        }
        //小数位0， 但是精度不为1 12.000 * 25 = 300.000
        {
          int64_t val = 0;
          int64_t pre = 3;
          EXPECT_EQ(0, TestObjDecimalHelper::array_mul(integer, val, pre, mul, product, length));
          EXPECT_EQ(0, product[length - 1]);
          EXPECT_EQ(0, product[length - 2]);
          EXPECT_EQ(0, product[length - 3]);
          EXPECT_EQ(0, product[length - 4]);      
          EXPECT_EQ(0, product[length - 5]);
          EXPECT_EQ(3, product[length - 6]);
        }
        //小数位0，精度为1 12.0 * 25 = 300.0
        {
          int64_t val = 0;
          int64_t pre = 1;
          EXPECT_EQ(0, TestObjDecimalHelper::array_mul(integer, val, pre, mul, product, length));
          EXPECT_EQ(0, product[length - 1]);
          EXPECT_EQ(0, product[length - 2]);
          EXPECT_EQ(0, product[length - 3]);
          EXPECT_EQ(3, product[length - 4]);      
        }
        //被乘数很大,但没有超过范围
        {
          int64_t val = 999999999;
          EXPECT_EQ(0, TestObjDecimalHelper::array_mul(integer, fractional, precision, val, product, length));
          TBSYS_LOG(WARN, "12.3 * 999999999 = %s\n", product);
          EXPECT_EQ(7, product[length - 1]);
          EXPECT_EQ(7, product[length - 2]);
          EXPECT_EQ(8, product[length - 3]);
          EXPECT_EQ(9, product[length - 4]);      
          EXPECT_EQ(9, product[length - 5]);
          EXPECT_EQ(9, product[length - 6]);
          EXPECT_EQ(9, product[length - 7]);
          EXPECT_EQ(9, product[length - 8]);
          EXPECT_EQ(9, product[length - 9]);
          EXPECT_EQ(2, product[length - 10]);
          EXPECT_EQ(2, product[length - 11]);
          EXPECT_EQ(1, product[length - 12]);
        }

        //被乘数超过范围
        {
          int64_t val = 9999999999;
          printf("max integer = %d\n", INT32_MAX);
          EXPECT_EQ(OB_INVALID_ARGUMENT, TestObjDecimalHelper::array_mul(integer, fractional, precision, val, product, length));
        }
        //定点数很大
        {
          integer = 9999999999;
          fractional = 999999999;
          int64_t val = 999999999;
          int64_t precision = 9;
          EXPECT_EQ(0, TestObjDecimalHelper::array_mul(integer, fractional, precision, val, product, length));
          TBSYS_LOG(WARN, "\n9999999999.999999999 * 999999999 = %s\n", product);
          fractional = 0;
          EXPECT_EQ(0, TestObjDecimalHelper::array_mul(integer, fractional, precision, val, product, length));
          TBSYS_LOG(WARN, "\n9999999999.000000000 * 999999999 = %s\n", product);
          fractional = 999999999;
          integer = 0;
          EXPECT_EQ(0, TestObjDecimalHelper::array_mul(integer, fractional, precision, val, product, length));
          TBSYS_LOG(WARN, "\n0.999999999 * 999999999 = %s\n", product);
        }
      }

      //static int array_div(const int64_t integer, const int64_t fractional, const int64_t precision,
      //     const int64_t divisor,  char *consult, const int64_t consult_len);
      TEST_F(TestObjDecimal, array_div)
      {
        //=====================
        //输入参数不正确
        //=====================

        {
          int64_t integer = -1;
          int64_t fractional = 4;
          int64_t precision = 5;
          int64_t divisor = 5;
          int8_t consult[64];
          int64_t consult_len = 64;
          EXPECT_EQ(OB_INVALID_ARGUMENT, TestObjDecimalHelper::array_div(integer, fractional, precision, divisor, consult, consult_len));
        }
        {
          int64_t integer = 4;
          int64_t fractional = -4;
          int64_t precision = 5;
          int64_t divisor = 5;
          int8_t consult[64];
          int64_t consult_len = 64;
          EXPECT_EQ(OB_INVALID_ARGUMENT, TestObjDecimalHelper::array_div(integer, fractional, precision, divisor, consult, consult_len));
        }
        {
          int64_t integer = 4;
          int64_t fractional = 4;
          int64_t precision = -5;
          int64_t divisor = 5;
          int8_t consult[64];
          int64_t consult_len = 64;
          EXPECT_EQ(OB_INVALID_ARGUMENT, TestObjDecimalHelper::array_div(integer, fractional, precision, divisor, consult, consult_len));
        }
        {
          int64_t integer = 5;
          int64_t fractional = 4;
          int64_t precision = 5;
          int64_t divisor = -5;
          int8_t consult[64];
          int64_t consult_len = 64;
          EXPECT_EQ(OB_INVALID_ARGUMENT, TestObjDecimalHelper::array_div(integer, fractional, precision, divisor, consult, consult_len));
        }
        {
          int64_t integer = 5;
          int64_t fractional = 4;
          int64_t precision = 5;
          int64_t divisor = 5;
          int8_t *consult = NULL;
          int64_t consult_len = 25;
          EXPECT_EQ(OB_INVALID_ARGUMENT, TestObjDecimalHelper::array_div(integer, fractional, precision, divisor, consult, consult_len));
        }
        {
          int64_t integer = 5;
          int64_t fractional = 4;
          int64_t precision = 5;
          int64_t divisor = 5;
          int8_t consult[64];
          int64_t consult_len = -2;
          EXPECT_EQ(OB_INVALID_ARGUMENT, TestObjDecimalHelper::array_div(integer, fractional, precision, divisor, consult, consult_len));
        }
        //除数为0
        {
          int64_t integer = 5;
          int64_t fractional = 4;
          int64_t precision = 5;
          int64_t divisor = 0;
          int8_t consult[64];
          int64_t consult_len = 64;
          EXPECT_EQ(OB_INVALID_ARGUMENT, TestObjDecimalHelper::array_div(integer, fractional, precision, divisor, consult, consult_len));
        }
        //consult的长度不够
        {
          int64_t integer = 5;
          int64_t fractional = 4;
          int64_t precision = 5;
          int64_t divisor = 5;
          int8_t consult[63];
          int64_t consult_len = 63;
          EXPECT_EQ(OB_INVALID_ARGUMENT, TestObjDecimalHelper::array_div(integer, fractional, precision, divisor, consult, consult_len));
        }

        //=======================
        //正常计算
        //=======================

        // 没有余数 5.4 / 2 = 2.70
        {
          int64_t integer = 5;
          int64_t fractional = 4;
          int64_t precision = 1;
          int64_t divisor = 2;
          int8_t consult[64];
          int64_t consult_len = 64;
          EXPECT_EQ(0, TestObjDecimalHelper::array_div(integer, fractional, precision, divisor, consult, consult_len));
          //EXPECT_EQ(0, strcmp(consult, "270"));
          EXPECT_EQ(consult[0], 2);
          EXPECT_EQ(consult[1], 7);
          EXPECT_EQ(consult[2], 0);

        }
        //有余数 5.3 / 2 = 2.65
        {
          int64_t integer = 5;
          int64_t fractional = 3;
          int64_t precision = 1;
          int64_t divisor = 2;
          int8_t consult[64];
          int64_t consult_len = 64;
          EXPECT_EQ(0, TestObjDecimalHelper::array_div(integer, fractional, precision, divisor, consult, consult_len));
          EXPECT_EQ(consult[0], 2);
          EXPECT_EQ(consult[1], 6);
          EXPECT_EQ(consult[2], 5);
        }
        //整数位为0, 没有余数  0.32 / 2 = 0.160
        {
          int64_t integer = 0;
          int64_t fractional = 32;
          int64_t precision = 2;
          int64_t divisor = 2;
          int8_t consult[64];
          int64_t consult_len = 64;
          EXPECT_EQ(0, TestObjDecimalHelper::array_div(integer, fractional, precision, divisor, consult, consult_len));
          //EXPECT_EQ(0, strcmp(consult, "0160"));
          EXPECT_EQ(consult[0], 0);
          EXPECT_EQ(consult[1], 1);
          EXPECT_EQ(consult[2], 6);
          EXPECT_EQ(consult[3], 0);
        }
        //整数位为0, 小数起始位置为0  0.03 / 2 = 0.015
        {
          int64_t integer = 0;
          int64_t fractional = 3;
          int64_t precision = 2;
          int64_t divisor = 2;
          int8_t consult[64];
          int64_t consult_len = 64;
          EXPECT_EQ(0, TestObjDecimalHelper::array_div(integer, fractional, precision, divisor, consult, consult_len));
          EXPECT_EQ(consult[0], 0);
          EXPECT_EQ(consult[1], 0);
          EXPECT_EQ(consult[2], 1);     
          EXPECT_EQ(consult[3], 5);
        }

        //743.003 / 89 = 8.3483
        {
          int64_t integer = 743;
          int64_t fractional = 3;
          int64_t precision = 3;
          int64_t divisor = 89;
          int8_t consult[64];
          int64_t consult_len = 64;
          EXPECT_EQ(0, TestObjDecimalHelper::array_div(integer, fractional, precision, divisor, consult, consult_len));
          EXPECT_EQ(consult[0], 0);
          EXPECT_EQ(consult[1], 0);
          EXPECT_EQ(consult[2], 8);     
          EXPECT_EQ(consult[3], 3);
          EXPECT_EQ(consult[4], 4);
          EXPECT_EQ(consult[5], 8);
          EXPECT_EQ(consult[6], 3);     
        }

        //整数位为0  0.01 / 99 = 0.000
        {
          int64_t integer = 0;
          int64_t fractional = 1;
          int64_t precision = 2;
          int64_t divisor = 99;
          int8_t consult[64];
          int64_t consult_len = 64;
          EXPECT_EQ(0, TestObjDecimalHelper::array_div(integer, fractional, precision, divisor, consult, consult_len));
          EXPECT_EQ(consult[0], 0);
          EXPECT_EQ(consult[1], 0);
          EXPECT_EQ(consult[2], 0);     
          EXPECT_EQ(consult[3], 0);
        }
        //0.98 / 99 = 0.009
        {
          int64_t integer = 0;
          int64_t fractional = 98;
          int64_t precision = 2;
          int64_t divisor = 99;
          int8_t consult[64];
          int64_t consult_len = 64;
          EXPECT_EQ(0, TestObjDecimalHelper::array_div(integer, fractional, precision, divisor, consult, consult_len));
          EXPECT_EQ(consult[0], 0);
          EXPECT_EQ(consult[1], 0);
          EXPECT_EQ(consult[2], 0);     
          EXPECT_EQ(consult[3], 9);
        }

        //123.45 / 432 = 000.285
        {
          int64_t integer = 123;
          int64_t fractional = 45;
          int64_t precision = 2;
          int64_t divisor = 432;
          int8_t consult[64];
          int64_t consult_len = 64;
          EXPECT_EQ(0, TestObjDecimalHelper::array_div(integer, fractional, precision, divisor, consult, consult_len));
          //EXPECT_EQ(0, strcmp(consult, "000285"));
          EXPECT_EQ(consult[0], 0);
          EXPECT_EQ(consult[1], 0);
          EXPECT_EQ(consult[2], 0);     
          EXPECT_EQ(consult[3], 2);
          EXPECT_EQ(consult[4], 8);
          EXPECT_EQ(consult[5], 5);
        }

        //763.90 / 563 = 001.356
        {
          int64_t integer = 763;
          int64_t fractional = 90;
          int64_t precision = 2;
          int64_t divisor = 563;
          int8_t consult[64];
          int64_t consult_len = 64;
          EXPECT_EQ(0, TestObjDecimalHelper::array_div(integer, fractional, precision, divisor, consult, consult_len));
          //EXPECT_EQ(0, strcmp(consult, "001356"));
          EXPECT_EQ(consult[0], 0);
          EXPECT_EQ(consult[1], 0);
          EXPECT_EQ(consult[2], 1);     
          EXPECT_EQ(consult[3], 3);
          EXPECT_EQ(consult[4], 5);
          EXPECT_EQ(consult[5], 6);
        }

        //整数和小数都为0 0.00 / 32 = 0.000
        {
          int64_t integer = 0;
          int64_t fractional = 0;
          int64_t precision = 2;
          int64_t divisor = 32;
          int8_t consult[64];
          int64_t consult_len = 64;
          EXPECT_EQ(0, TestObjDecimalHelper::array_div(integer, fractional, precision, divisor, consult, consult_len));
          //EXPECT_EQ(0, strcmp(consult, "0000"));
          EXPECT_EQ(consult[0], 0);
          EXPECT_EQ(consult[1], 0);
          EXPECT_EQ(consult[2], 0);     
          EXPECT_EQ(consult[3], 0);
        }

        //除数为2位数  5432.567 / 39 = 0139.2965
        {
          int64_t integer = 5432;
          int64_t fractional = 567;
          int64_t precision = 3;
          int64_t divisor = 39;
          int8_t consult[64];
          int64_t consult_len = 64;
          EXPECT_EQ(0, TestObjDecimalHelper::array_div(integer, fractional, precision, divisor, consult, consult_len));
          //EXPECT_EQ(0, strcmp(consult, "01392965"));
          EXPECT_EQ(consult[0], 0);
          EXPECT_EQ(consult[1], 1);
          EXPECT_EQ(consult[2], 3);     
          EXPECT_EQ(consult[3], 9);
          EXPECT_EQ(consult[4], 2);
          EXPECT_EQ(consult[5], 9);
          EXPECT_EQ(consult[6], 6); 
          EXPECT_EQ(consult[7], 5); 
        }

        //小数为0 54986 / 3456 = 00015.910
        {
          int64_t integer = 54986;
          int64_t fractional = 0;
          int64_t precision = 2;
          int64_t divisor = 3456;
          int8_t consult[64];
          int64_t consult_len = 64;
          EXPECT_EQ(0, TestObjDecimalHelper::array_div(integer, fractional, precision, divisor, consult, consult_len));
          //EXPECT_EQ(0, strcmp(consult, "00015910"));
          EXPECT_EQ(consult[0], 0);
          EXPECT_EQ(consult[1], 0);
          EXPECT_EQ(consult[2], 0);     
          EXPECT_EQ(consult[3], 1);
          EXPECT_EQ(consult[4], 5);
          EXPECT_EQ(consult[5], 9);
          EXPECT_EQ(consult[6], 1); 
          EXPECT_EQ(consult[7], 0); 
        }
        //54985.45 / 342 = 00160.776
        {
          int64_t integer = 54985;
          int64_t fractional = 45;
          int64_t precision = 2;
          int64_t divisor = 342;
          int8_t consult[64];
          int64_t consult_len = 64;
          EXPECT_EQ(0, TestObjDecimalHelper::array_div(integer, fractional, precision, divisor, consult, consult_len));
          //EXPECT_EQ(0, strcmp(consult, "00160776"));
          EXPECT_EQ(consult[0], 0);
          EXPECT_EQ(consult[1], 0);
          EXPECT_EQ(consult[2], 1);     
          EXPECT_EQ(consult[3], 6);
          EXPECT_EQ(consult[4], 0);
          EXPECT_EQ(consult[5], 7);
          EXPECT_EQ(consult[6], 7); 
          EXPECT_EQ(consult[7], 6); 
        }
        // 3456.78 / 543 = 0006.366
        {
          int64_t integer = 3456;
          int64_t fractional = 78;
          int64_t precision = 2;
          int64_t divisor = 543;
          int8_t consult[64];
          int64_t consult_len = 64;
          EXPECT_EQ(0, TestObjDecimalHelper::array_div(integer, fractional, precision, divisor, consult, consult_len));
          //EXPECT_EQ(0, strcmp(consult, "0006366"));
          EXPECT_EQ(consult[0], 0);
          EXPECT_EQ(consult[1], 0);
          EXPECT_EQ(consult[2], 0);     
          EXPECT_EQ(consult[3], 6);
          EXPECT_EQ(consult[4], 3);
          EXPECT_EQ(consult[5], 6);
          EXPECT_EQ(consult[6], 6); 
        }
        //854932.67 / 953 = 000897.096
        {
          int64_t integer = 854932;
          int64_t fractional = 67;
          int64_t precision = 2;
          int64_t divisor = 953;
          int8_t consult[64];
          int64_t consult_len = 64;
          EXPECT_EQ(0, TestObjDecimalHelper::array_div(integer, fractional, precision, divisor, consult, consult_len));
          //EXPECT_EQ(0, strcmp(consult, "000897096"));
          EXPECT_EQ(consult[0], 0);
          EXPECT_EQ(consult[1], 0);
          EXPECT_EQ(consult[2], 0);     
          EXPECT_EQ(consult[3], 8);
          EXPECT_EQ(consult[4], 9);
          EXPECT_EQ(consult[5], 7);
          EXPECT_EQ(consult[6], 0); 
          EXPECT_EQ(consult[7], 9); 
          EXPECT_EQ(consult[8], 6); 

        }
      }

      // static int is_over_flow(const int64_t width, const int64_t precision, int64_t &integer,
      //  int64_t &fractional, const ObTableProperty &gb_table_property);
      TEST_F(TestObjDecimal, is_over_flow)
      {
        int64_t width = 6;
        int64_t precision = 3;
        int64_t integer =  32;
        int64_t fractional = 34;
        ObTableProperty property;

        //输入参数不正确
        {
          width = -1;
          precision = 3;
          integer = 43;
          fractional = 432;
          EXPECT_EQ(OB_INVALID_ARGUMENT, TestObjDecimalHelper::is_over_flow(width, precision, integer, fractional, property));
        }
        {
          width = 1;
          precision = 3;
          integer = 43;
          fractional = 432;
          EXPECT_EQ(OB_INVALID_ARGUMENT, TestObjDecimalHelper::is_over_flow(width, precision, integer, fractional, property));
        }
        {
          width = 5;
          precision = -3;
          integer = 43;
          fractional = 432;
          EXPECT_EQ(OB_INVALID_ARGUMENT, TestObjDecimalHelper::is_over_flow(width, precision, integer, fractional, property));
        }
        {
          width = 67;
          precision = 3;
          integer = 43;
          fractional = 432;
          EXPECT_EQ(OB_INVALID_ARGUMENT, TestObjDecimalHelper::is_over_flow(width, precision, integer, fractional, property));
        }
        {
          width = 6;
          precision = 34;
          integer = 43;
          fractional = 432;
          EXPECT_EQ(OB_INVALID_ARGUMENT, TestObjDecimalHelper::is_over_flow(width, precision, integer, fractional, property));
        }
        {
          width = 6;
          precision = 3;
          integer = -1;
          fractional = 432;
          EXPECT_EQ(OB_INVALID_ARGUMENT, TestObjDecimalHelper::is_over_flow(width, precision, integer, fractional, property));
        }
        {
          width = 6;
          precision = 3;
          integer = 1;
          fractional = -432;
          EXPECT_EQ(OB_INVALID_ARGUMENT, TestObjDecimalHelper::is_over_flow(width, precision, integer, fractional, property));
        }
        //正常处理,无需处理
        {
          width = 6;
          precision = 3;
          integer = 23;
          fractional = 432;
          EXPECT_EQ(0,  TestObjDecimalHelper::is_over_flow(width, precision, integer, fractional, property));
          EXPECT_EQ(integer, 23);
          EXPECT_EQ(fractional, 432);
        }
        //整数有溢出
        {
          width = 6;
          precision = 3;
          integer = 3452;
          fractional = 432;
          EXPECT_EQ(OB_DECIMAL_OVERFLOW_WARN, TestObjDecimalHelper::is_over_flow(width, precision, integer, fractional, property));
          EXPECT_EQ(integer, 999);
          EXPECT_EQ(fractional, 999);
        }
        //小数有进位
        {
          width = 6;
          precision = 3;
          integer = 45;
          fractional = 1432;
          EXPECT_EQ(0, TestObjDecimalHelper::is_over_flow(width, precision, integer, fractional, property));
          EXPECT_EQ(integer, 46);
          EXPECT_EQ(fractional, 432);
        }
        //小数进位后，整数溢出
        {
          width = 6;
          precision = 3;
          integer = 999;
          fractional = 1432;
          EXPECT_EQ(OB_DECIMAL_OVERFLOW_WARN, TestObjDecimalHelper::is_over_flow(width, precision, integer, fractional, property));
          EXPECT_EQ(integer, 999);
          EXPECT_EQ(fractional, 999);
        }
        property.mode = SQL_STRICT;
        {
          width = 6;
          precision = 3;
          integer = 1999;
          fractional = 432;
          EXPECT_EQ(OB_DECIMAL_UNLEGAL_ERROR, TestObjDecimalHelper::is_over_flow(width, precision, integer, fractional, property));
        }
      }

      //static int decimal_format(const int64_t width,  const int64_t precision, int64_t &integer,
      //   int64_t &fractional, const ObTableProperty &gb_table_property)
      TEST_F(TestObjDecimal, decimal_format)
      {
        int64_t width = 6;
        int64_t precision = 3;
        int64_t integer =  32;
        int64_t fractional = 34;
        ObTableProperty property;

        //输入参数不正确
        {
          width = -1;
          precision = 3;
          integer = 43;
          fractional = 432;
          EXPECT_EQ(OB_INVALID_ARGUMENT, TestObjDecimalHelper::decimal_format(width, precision, integer, fractional, property));
        }
        {
          width = 1;
          precision = 3;
          integer = 43;
          fractional = 432;
          EXPECT_EQ(OB_INVALID_ARGUMENT, TestObjDecimalHelper::decimal_format(width, precision, integer, fractional, property));
        }
        {
          width = 5;
          precision = -3;
          integer = 43;
          fractional = 432;
          EXPECT_EQ(OB_INVALID_ARGUMENT, TestObjDecimalHelper::decimal_format(width, precision, integer, fractional, property));
        }
        {
          width = 67;
          precision = 3;
          integer = 43;
          fractional = 432;
          EXPECT_EQ(OB_INVALID_ARGUMENT, TestObjDecimalHelper::decimal_format(width, precision, integer, fractional, property));
        }
        {
          width = 6;
          precision = 34;
          integer = 43;
          fractional = 432;
          EXPECT_EQ(OB_INVALID_ARGUMENT, TestObjDecimalHelper::decimal_format(width, precision, integer, fractional, property));
        }
        {
          width = 6;
          precision = 3;
          integer = -1;
          fractional = 432;
          EXPECT_EQ(OB_INVALID_ARGUMENT, TestObjDecimalHelper::decimal_format(width, precision, integer, fractional, property));
        }
        {
          width = 6;
          precision = 3;
          integer = 1;
          fractional = -432;
          EXPECT_EQ(OB_INVALID_ARGUMENT, TestObjDecimalHelper::decimal_format(width, precision, integer, fractional, property));
        }
        //正常处理,无需处理
        {
          width = 6;
          precision = 3;
          integer = 23;
          fractional = 432;
          EXPECT_EQ(0, TestObjDecimalHelper::decimal_format(width, precision, integer, fractional, property));
          EXPECT_EQ(integer, 23);
          EXPECT_EQ(fractional, 432);
        }
        //整数有溢出
        {
          width = 6;
          precision = 3;
          integer = 3452;
          fractional = 432;
          EXPECT_EQ(OB_DECIMAL_OVERFLOW_WARN, TestObjDecimalHelper::decimal_format(width, precision, integer, fractional, property));
          EXPECT_EQ(integer, 999);
          EXPECT_EQ(fractional, 999);
        }
        //小数溢出，四舍五入
        {
          width = 6;
          precision = 3;
          integer = 45;
          fractional = 1432;
          EXPECT_EQ(0, TestObjDecimalHelper::decimal_format(width, precision, integer, fractional, property));
          EXPECT_EQ(integer, 45);
          EXPECT_EQ(fractional, 143);
        }
        //小数有入位
        {
          width = 6;
          precision = 3;
          integer = 45;
          fractional = 1436;
          EXPECT_EQ(0, TestObjDecimalHelper::decimal_format(width, precision, integer, fractional, property));
          EXPECT_EQ(integer, 45);
          EXPECT_EQ(fractional, 144);
        }
        //有向整数的进位
        {
          width = 6;
          precision = 3;
          integer = 45;
          fractional = 9998;
          EXPECT_EQ(0, TestObjDecimalHelper::decimal_format(width, precision, integer, fractional, property));
          EXPECT_EQ(integer, 46);
          EXPECT_EQ(fractional, 0);
        }
        //小数进位后，整数溢出
        {
          width = 6;
          precision = 3;
          integer = 999;
          fractional = 9999;
          EXPECT_EQ(OB_DECIMAL_OVERFLOW_WARN, TestObjDecimalHelper::decimal_format(width, precision, integer, fractional, property));
          EXPECT_EQ(integer, 999);
          EXPECT_EQ(fractional, 999);
        }
        property.mode = SQL_STRICT;
        {
          width = 6;
          precision = 3;
          integer = 1999;
          fractional = 432;
          EXPECT_EQ(OB_DECIMAL_UNLEGAL_ERROR, TestObjDecimalHelper::decimal_format(width, precision, integer, fractional, property));
        }
      }

      //static int check_decimal_str(const char *value, const int64_t length, bool &has_fractional,
      //   bool &has_sign, bool &sign);
      TEST_F(TestObjDecimal, check_decimal_str)
      {
        char *value = NULL;
        int64_t length = 0;
        bool has_fractional = false;
        bool has_sign = false;
        bool sign = false;

        //输入value为空
        {
          length = 16;
          EXPECT_EQ(OB_INVALID_ARGUMENT, TestObjDecimalHelper::check_decimal_str(value, length, has_fractional, has_sign, sign));
        }

        //输入length 为0
        {
          value = new char[16];
          length = 0;
          EXPECT_EQ(OB_INVALID_ARGUMENT, TestObjDecimalHelper::check_decimal_str(value, length, has_fractional, has_sign, sign));
          delete []value;
        }

        //输入参数正确，字符内容不正确
        {
          char value[16] = "jingsong";
          length = strlen(value);
          EXPECT_EQ(OB_NOT_A_DECIMAL, TestObjDecimalHelper::check_decimal_str(value, length, has_fractional, has_sign, sign));
        }

        //第一个字符为符号位-，第二字符不对
        {
          char value[16] = "+jinsong";
          length = strlen(value);
          EXPECT_EQ(OB_NOT_A_DECIMAL, TestObjDecimalHelper::check_decimal_str(value, length, has_fractional, has_sign, sign));
        }

        //首字符为+
        {
          char value[16] = "-jinsong";
          length = strlen(value);
          EXPECT_EQ(OB_NOT_A_DECIMAL, TestObjDecimalHelper::check_decimal_str(value, length, has_fractional, has_sign, sign));
        }
        //首字符为数字
        {
          char value[16] = "9jinsong";
          length = strlen(value);
          EXPECT_EQ(OB_NOT_A_DECIMAL, TestObjDecimalHelper::check_decimal_str(value, length, has_fractional, has_sign, sign));
        }
        //首字符为数字
        {
          char value[16] = "9jinsong";
          length = 1;
          EXPECT_EQ(0, TestObjDecimalHelper::check_decimal_str(value, length, has_fractional, has_sign, sign));
          EXPECT_EQ(true, sign);
          EXPECT_EQ(false, has_sign);
          EXPECT_EQ(false, has_fractional);
        }
        //只有整数部分
        {
          char value[16] = "98663";
          length = strlen(value);
          EXPECT_EQ(OB_SUCCESS, TestObjDecimalHelper::check_decimal_str(value, length, has_fractional, has_sign, sign));
          EXPECT_EQ(true, sign);
          EXPECT_EQ(false, has_sign);
          EXPECT_EQ(false, has_fractional);
        }
        {
          char value[16] = "+98663";
          length = strlen(value);
          EXPECT_EQ(OB_SUCCESS, TestObjDecimalHelper::check_decimal_str(value, length, has_fractional, has_sign, sign));
          EXPECT_EQ(true, sign);
          EXPECT_EQ(true, has_sign);
          EXPECT_EQ(false, has_fractional);
        }
        {
          char value[16] = "-98663";
          length = strlen(value);
          EXPECT_EQ(OB_SUCCESS, TestObjDecimalHelper::check_decimal_str(value, length, has_fractional, has_sign, sign));
          EXPECT_EQ(false, sign);
          EXPECT_EQ(true, has_sign);
          EXPECT_EQ(false, has_fractional);
        }
        {
          char value[16] = "9jinsong";
          length = 2;
          EXPECT_EQ(OB_NOT_A_DECIMAL, TestObjDecimalHelper::check_decimal_str(value, length, has_fractional, has_sign, sign));
        }
        {
          char value[16] = "-9jinsong";
          length = 4;
          EXPECT_EQ(OB_NOT_A_DECIMAL, TestObjDecimalHelper::check_decimal_str(value, length, has_fractional, has_sign, sign));
        }
        {
          char value[16] = "+9jinsong";
          length =5;
          EXPECT_EQ(OB_NOT_A_DECIMAL, TestObjDecimalHelper::check_decimal_str(value, length, has_fractional, has_sign, sign));
        }
        //带小数点
        {
          char value[16] = "98663.";
          length = strlen(value);
          EXPECT_EQ(OB_NOT_A_DECIMAL, TestObjDecimalHelper::check_decimal_str(value, length, has_fractional, has_sign, sign));
        }
        {
          char value[16] = "+98663.";
          length = strlen(value);
          EXPECT_EQ(OB_NOT_A_DECIMAL, TestObjDecimalHelper::check_decimal_str(value, length, has_fractional, has_sign, sign));
        }
        {
          char value[16] = "-98663.";
          length = strlen(value);
          EXPECT_EQ(OB_NOT_A_DECIMAL, TestObjDecimalHelper::check_decimal_str(value, length, has_fractional, has_sign, sign));
        }
        //长度以小数点结束，但字符串并没有结束
        {
          char value[16] = "98663.4";
          length = strlen(value) -1;
          EXPECT_EQ(OB_NOT_A_DECIMAL, TestObjDecimalHelper::check_decimal_str(value, length, has_fractional, has_sign, sign));
        }
        {
          char value[16] = "+98663.4";
          length = strlen(value)-1;
          EXPECT_EQ(OB_NOT_A_DECIMAL, TestObjDecimalHelper::check_decimal_str(value, length, has_fractional, has_sign, sign));
        }
        {
          char value[16] = "-98663.4";
          length = strlen(value) - 1;
          EXPECT_EQ(OB_NOT_A_DECIMAL, TestObjDecimalHelper::check_decimal_str(value, length, has_fractional, has_sign, sign));
        }
        //长度以小数点结束，但字符串并没有结束
        {
          char value[16] = "98663.te";
          length = strlen(value) - 2;
          EXPECT_EQ(OB_NOT_A_DECIMAL, TestObjDecimalHelper::check_decimal_str(value, length, has_fractional, has_sign, sign));
        }
        {
          char value[16] = "+98663.jt3";
          length = strlen(value)-3;
          EXPECT_EQ(OB_NOT_A_DECIMAL, TestObjDecimalHelper::check_decimal_str(value, length, has_fractional, has_sign, sign));
        }
        {
          char value[16] = "-98663.hj5";
          length = strlen(value) - 3;
          EXPECT_EQ(OB_NOT_A_DECIMAL, TestObjDecimalHelper::check_decimal_str(value, length, has_fractional, has_sign, sign));
        }

        //小数点后面不是数字
        {
          char value[16] = "98663.r";
          length = strlen(value);
          EXPECT_EQ(OB_NOT_A_DECIMAL, TestObjDecimalHelper::check_decimal_str(value, length, has_fractional, has_sign, sign));
        }
        {
          char value[16] = "+98663.e";
          length = strlen(value);
          EXPECT_EQ(OB_NOT_A_DECIMAL, TestObjDecimalHelper::check_decimal_str(value, length, has_fractional, has_sign, sign));
        }
        {
          char value[16] = "-98663.h";
          length = strlen(value);
          EXPECT_EQ(OB_NOT_A_DECIMAL, TestObjDecimalHelper::check_decimal_str(value, length, has_fractional, has_sign, sign));
        }

        //小数点后面是数字
        {
          char value[16] = "98663.54";
          length = strlen(value);
          EXPECT_EQ(OB_SUCCESS, TestObjDecimalHelper::check_decimal_str(value, length, has_fractional, has_sign, sign));
          EXPECT_EQ(true, sign);
          EXPECT_EQ(false, has_sign);
          EXPECT_EQ(true, has_fractional);
        }
        {
          char value[16] = "+98663.43";
          length = strlen(value);
          EXPECT_EQ(OB_SUCCESS, TestObjDecimalHelper::check_decimal_str(value, length, has_fractional, has_sign, sign));
          EXPECT_EQ(true, sign);
          EXPECT_EQ(true, has_sign);
          EXPECT_EQ(true, has_fractional);
        }
        {
          char value[16] = "-98663.23";
          length = strlen(value);
          EXPECT_EQ(OB_SUCCESS, TestObjDecimalHelper::check_decimal_str(value, length, has_fractional, has_sign,  sign));
          EXPECT_EQ(false, sign);
          EXPECT_EQ(true, has_sign);
          EXPECT_EQ(true, has_fractional);
        }
        {
          char value[16] = "98663.54ge";
          length = strlen(value);
          EXPECT_EQ(OB_NOT_A_DECIMAL, TestObjDecimalHelper::check_decimal_str(value, length, has_fractional, has_sign, sign));
        }
        {
          char value[16] = "-98663.54ge";
          length = strlen(value);
          EXPECT_EQ(OB_NOT_A_DECIMAL, TestObjDecimalHelper::check_decimal_str(value, length, has_fractional, has_sign, sign));
        }
        {
          char value[16] = "+98663.54ge";
          length = strlen(value);
          EXPECT_EQ(OB_NOT_A_DECIMAL, TestObjDecimalHelper::check_decimal_str(value, length, has_fractional, has_sign, sign));
        }
        {
          char value[16] = "98663.54ge";
          length = strlen(value) - 2;
          EXPECT_EQ(0, TestObjDecimalHelper::check_decimal_str(value, length, has_fractional, has_sign, sign));
          EXPECT_EQ(true, sign);
          EXPECT_EQ(false, has_sign);
          EXPECT_EQ(true, has_fractional);
        }
        {
          char value[16] = "-98663.54fge";
          length = strlen(value) - 3;
          EXPECT_EQ(0, TestObjDecimalHelper::check_decimal_str(value, length, has_fractional, has_sign, sign));
          EXPECT_EQ(false, sign);
          EXPECT_EQ(true, has_sign);
          EXPECT_EQ(true, has_fractional);
        }
        {
          char value[16] = "+98663.54g5he";
          length = strlen(value) - 4;
          EXPECT_EQ(0, TestObjDecimalHelper::check_decimal_str(value, length, has_fractional, has_sign, sign));
          EXPECT_EQ(true, sign);
          EXPECT_EQ(true, has_sign);
          EXPECT_EQ(true, has_fractional);
        }
        //整数位太长
        {
          char value[28] = "+123456789123456789123.54";
          int64_t length = strlen(value);
          EXPECT_EQ(OB_NOT_A_DECIMAL, TestObjDecimalHelper::check_decimal_str(value, length, has_fractional, has_sign, sign));
        }
        //小数位太长
        {
          char value[28] = "+12346789123.12345678912";
          int64_t length = strlen(value);
          EXPECT_EQ(OB_NOT_A_DECIMAL, TestObjDecimalHelper::check_decimal_str(value, length, has_fractional, has_sign, sign));
        }
      }
      // static int get_max_express_number(const int64_t width, int64_t &value);
      TEST_F(TestObjDecimal, get_max_express_number)
      {
        int64_t width = 1;
        int64_t value = 0;
        EXPECT_EQ(OB_SUCCESS, ObDecimalHelper::get_max_express_number(width, value));
        EXPECT_EQ(9, value);
        width = 2;
        EXPECT_EQ(0, ObDecimalHelper::get_max_express_number(width, value));
        EXPECT_EQ(99,value);
        width = 9;
        EXPECT_EQ(0, ObDecimalHelper::get_max_express_number(width, value));
        EXPECT_EQ(999999999, value);
        width = -1;
        EXPECT_EQ(OB_INVALID_ARGUMENT, ObDecimalHelper::get_max_express_number(width, value));
        width = 0;
        EXPECT_EQ(0, ObDecimalHelper::get_max_express_number(width, value));
        EXPECT_EQ(0, value);
      }
      //int64_t get_min_express_number(const int64_t precision)
      TEST_F(TestObjDecimal, get_min_express_number)
      {
        int64_t width = 1;
        int64_t value = 0;
        EXPECT_EQ(0, ObDecimalHelper::get_min_express_number(width, value));
        EXPECT_EQ(1, value);
        width = 2;
        EXPECT_EQ(0, ObDecimalHelper::get_min_express_number(width, value));
        EXPECT_EQ(10, value);
        width = 9;
        EXPECT_EQ(0, ObDecimalHelper::get_min_express_number(width, value));
        EXPECT_EQ(100000000, value);
        width = -1;
        EXPECT_EQ(OB_INVALID_ARGUMENT, ObDecimalHelper::get_min_express_number(width, value));
        width = 0;
        EXPECT_EQ(0, ObDecimalHelper::get_max_express_number(width, value));
        EXPECT_EQ(0, value);
      }

      //测试四舍五入函数是否正确
      //int64_t round_off(int64_t &fractional, bool &is_carry, const int64_t precision)
      TEST_F(TestObjDecimal, round_off)
      {
        //正常功能测试,无进位，
        {
          int64_t fractional = 12345;
          bool is_carry = false;
          int64_t precision = 3;
          EXPECT_EQ(0, ObDecimalHelper::round_off(fractional, is_carry, precision));
          EXPECT_EQ(123, fractional);
          EXPECT_EQ(false, is_carry);
        }
        //带1次进位的测试
        {
          int64_t fractional = 12356;
          bool is_carry = false;
          int64_t precision = 3;
          EXPECT_EQ(0, ObDecimalHelper::round_off(fractional, is_carry, precision));
          EXPECT_EQ(124, fractional);
          EXPECT_EQ(false, is_carry);
        }
        //带2次进位的测试
        {
          int64_t fractional = 15965;
          bool is_carry = false;
          int64_t precision = 3;
          EXPECT_EQ(0, ObDecimalHelper::round_off(fractional, is_carry, precision));
          EXPECT_EQ(160, fractional);
          EXPECT_EQ(false, is_carry);
        }
        //带向整数位进行的测试
        {
          int64_t fractional = 99965;
          bool is_carry = false;
          int64_t precision = 3;
          EXPECT_EQ(0, ObDecimalHelper::round_off(fractional, is_carry, precision));
          EXPECT_EQ(0, fractional);
          EXPECT_EQ(true, is_carry);
        }
        //输入参数不正确
        {
          int64_t fractional = -1;
          bool is_carry = false;
          int64_t precision = 3;
          EXPECT_EQ(OB_INVALID_ARGUMENT, ObDecimalHelper::round_off(fractional, is_carry, precision));
        }
        {
          int64_t fractional = 99965;
          bool is_carry = false;
          int64_t precision = -3;
          EXPECT_EQ(OB_INVALID_ARGUMENT, ObDecimalHelper::round_off(fractional, is_carry, precision));
        }
        {
          int64_t fractional = 99965;
          bool is_carry = false;
          int64_t precision = 19;
          EXPECT_EQ(OB_INVALID_ARGUMENT, ObDecimalHelper::round_off(fractional, is_carry, precision));
        }
      }

      //static int string_to_int(const char* value, const int64_t length,int64_t &integer,
      //   int64_t &fractional, int64_t &precision, bool &sign);
      TEST_F(TestObjDecimal, string_to_int)
      {
        char *value = NULL;
        int64_t length = 0;
        bool has_fractional = false;
        bool has_sign = false;
        bool sign = false;
        int64_t integer = 0;
        int64_t fractional = 0;
        int64_t precision = 0;

        //输入value为空
        {
          length = 16;
          EXPECT_EQ(OB_INVALID_ARGUMENT, TestObjDecimalHelper::check_decimal_str(value, length, has_fractional, has_sign, sign));
          EXPECT_EQ(OB_INVALID_ARGUMENT, ObDecimalHelper::string_to_int(value, length, integer, fractional, precision, sign));
        }

        //输入length 为0
        {
          value = new char[16];
          length = 0;
          EXPECT_EQ(OB_INVALID_ARGUMENT, TestObjDecimalHelper::check_decimal_str(value, length, has_fractional, has_sign, sign));
          EXPECT_EQ(OB_INVALID_ARGUMENT, ObDecimalHelper::string_to_int(value, length, integer, fractional, precision, sign));
          delete []value;
        }

        //输入参数正确，字符内容不正确
        {
          char value[16] = "jingsong";
          length = strlen(value);
          EXPECT_EQ(OB_NOT_A_DECIMAL, TestObjDecimalHelper::check_decimal_str(value, length, has_fractional, has_sign, sign));
          EXPECT_EQ(OB_NOT_A_DECIMAL, ObDecimalHelper::string_to_int(value, length, integer, fractional, precision, sign));
        }

        //第一个字符为符号位-，第二字符不对
        {
          char value[16] = "+jinsong";
          length = strlen(value);
          EXPECT_EQ(OB_NOT_A_DECIMAL, TestObjDecimalHelper::check_decimal_str(value, length, has_fractional, has_sign, sign));
          EXPECT_EQ(OB_NOT_A_DECIMAL, ObDecimalHelper::string_to_int(value, length, integer, fractional, precision, sign));
        }

        //首字符为+
        {
          char value[16] = "-jinsong";
          length = strlen(value);
          EXPECT_EQ(OB_NOT_A_DECIMAL, TestObjDecimalHelper::check_decimal_str(value, length, has_fractional, has_sign, sign));
          EXPECT_EQ(OB_NOT_A_DECIMAL, ObDecimalHelper::string_to_int(value, length, integer, fractional, precision, sign));
        }
        //首字符为数字
        {
          char value[16] = "9jinsong";
          length = strlen(value);
          EXPECT_EQ(OB_NOT_A_DECIMAL, TestObjDecimalHelper::check_decimal_str(value, length, has_fractional, has_sign, sign));
          EXPECT_EQ(OB_NOT_A_DECIMAL, ObDecimalHelper::string_to_int(value, length, integer, fractional, precision, sign));
        }
        //首字符为数字
        {
          char value[16] = "9jinsong";
          length = 1;
          EXPECT_EQ(0, TestObjDecimalHelper::check_decimal_str(value, length, has_fractional, has_sign, sign));
          EXPECT_EQ(0, ObDecimalHelper::string_to_int(value, length, integer, fractional, precision, sign));
          EXPECT_EQ(integer, 9);
          EXPECT_EQ(fractional, 0);
          EXPECT_EQ(precision, 0);
          EXPECT_EQ(sign, true);
        }
        //只有整数部分
        {
          char value[16] = "98663";
          length = strlen(value);
          EXPECT_EQ(OB_SUCCESS, TestObjDecimalHelper::check_decimal_str(value, length, has_fractional, has_sign, sign));
          EXPECT_EQ(0, ObDecimalHelper::string_to_int(value, length, integer, fractional, precision, sign));
          EXPECT_EQ(integer, 98663);
          EXPECT_EQ(fractional, 0);
          EXPECT_EQ(precision, 0);
          EXPECT_EQ(sign, true);
        }
        {
          char value[16] = "+98663";
          length = strlen(value);
          EXPECT_EQ(OB_SUCCESS, TestObjDecimalHelper::check_decimal_str(value, length, has_fractional, has_sign, sign));
          EXPECT_EQ(0, ObDecimalHelper::string_to_int(value, length, integer, fractional, precision, sign));
          EXPECT_EQ(integer, 98663);
          EXPECT_EQ(fractional, 0);
          EXPECT_EQ(precision, 0);
          EXPECT_EQ(sign, true);
        }
        {
          char value[16] = "-98663";
          length = strlen(value);
          EXPECT_EQ(OB_SUCCESS, TestObjDecimalHelper::check_decimal_str(value, length, has_fractional, has_sign, sign));
          EXPECT_EQ(0, ObDecimalHelper::string_to_int(value, length, integer, fractional, precision, sign));
          EXPECT_EQ(integer, 98663);
          EXPECT_EQ(fractional, 0);
          EXPECT_EQ(precision, 0);
          EXPECT_EQ(sign, false);
        }
        {
          char value[16] = "9jinsong";
          length = 2;
          EXPECT_EQ(OB_NOT_A_DECIMAL, TestObjDecimalHelper::check_decimal_str(value, length, has_fractional, has_sign, sign));
          EXPECT_EQ(OB_NOT_A_DECIMAL, ObDecimalHelper::string_to_int(value, length, integer, fractional, precision, sign));

        }
        {
          char value[16] = "-9jinsong";
          length = 4;
          EXPECT_EQ(OB_NOT_A_DECIMAL, TestObjDecimalHelper::check_decimal_str(value, length, has_fractional, has_sign, sign));
          EXPECT_EQ(OB_NOT_A_DECIMAL, ObDecimalHelper::string_to_int(value, length, integer, fractional, precision, sign));
        }
        {
          char value[16] = "+9jinsong";
          length =5;
          EXPECT_EQ(OB_NOT_A_DECIMAL, TestObjDecimalHelper::check_decimal_str(value, length, has_fractional, has_sign, sign));
          EXPECT_EQ(OB_NOT_A_DECIMAL, ObDecimalHelper::string_to_int(value, length, integer, fractional, precision, sign));
        }
        //带小数点
        {
          char value[16] = "98663.";
          length = strlen(value);
          EXPECT_EQ(OB_NOT_A_DECIMAL, TestObjDecimalHelper::check_decimal_str(value, length, has_fractional, has_sign, sign));
          EXPECT_EQ(OB_NOT_A_DECIMAL, ObDecimalHelper::string_to_int(value, length, integer, fractional, precision, sign));
        }
        {
          char value[16] = "+98663.";
          length = strlen(value);
          EXPECT_EQ(OB_NOT_A_DECIMAL, TestObjDecimalHelper::check_decimal_str(value, length, has_fractional, has_sign, sign));
          EXPECT_EQ(OB_NOT_A_DECIMAL, ObDecimalHelper::string_to_int(value, length, integer, fractional, precision, sign));
        }
        {
          char value[16] = "-98663.";
          length = strlen(value);
          EXPECT_EQ(OB_NOT_A_DECIMAL, TestObjDecimalHelper::check_decimal_str(value, length, has_fractional, has_sign, sign));
          EXPECT_EQ(OB_NOT_A_DECIMAL, ObDecimalHelper::string_to_int(value, length, integer, fractional, precision, sign));
        }
        //长度以小数点结束，但字符串并没有结束
        {
          char value[16] = "98663.4";
          length = strlen(value) -1;
          EXPECT_EQ(OB_NOT_A_DECIMAL, TestObjDecimalHelper::check_decimal_str(value, length, has_fractional, has_sign, sign));
          EXPECT_EQ(OB_NOT_A_DECIMAL, ObDecimalHelper::string_to_int(value, length, integer, fractional, precision, sign));
        }
        {
          char value[16] = "+98663.4";
          length = strlen(value)-1;
          EXPECT_EQ(OB_NOT_A_DECIMAL, TestObjDecimalHelper::check_decimal_str(value, length, has_fractional, has_sign, sign));
          EXPECT_EQ(OB_NOT_A_DECIMAL, ObDecimalHelper::string_to_int(value, length, integer, fractional, precision, sign));
        }
        {
          char value[16] = "-98663.4";
          length = strlen(value) - 1;
          EXPECT_EQ(OB_NOT_A_DECIMAL, TestObjDecimalHelper::check_decimal_str(value, length, has_fractional, has_sign, sign));
          EXPECT_EQ(OB_NOT_A_DECIMAL, ObDecimalHelper::string_to_int(value, length, integer, fractional, precision, sign));
        }
        //长度以小数点结束，但字符串并没有结束
        {
          char value[16] = "98663.te";
          length = strlen(value) - 2;
          EXPECT_EQ(OB_NOT_A_DECIMAL, TestObjDecimalHelper::check_decimal_str(value, length, has_fractional, has_sign, sign));
          EXPECT_EQ(OB_NOT_A_DECIMAL, ObDecimalHelper::string_to_int(value, length, integer, fractional, precision, sign));
        }
        {
          char value[16] = "+98663.jt3";
          length = strlen(value)-3;
          EXPECT_EQ(OB_NOT_A_DECIMAL, TestObjDecimalHelper::check_decimal_str(value, length, has_fractional, has_sign, sign));
          EXPECT_EQ(OB_NOT_A_DECIMAL, ObDecimalHelper::string_to_int(value, length, integer, fractional, precision, sign));
        }
        {
          char value[16] = "-98663.hj5";
          length = strlen(value) - 3;
          EXPECT_EQ(OB_NOT_A_DECIMAL, TestObjDecimalHelper::check_decimal_str(value, length, has_fractional, has_sign, sign));
          EXPECT_EQ(OB_NOT_A_DECIMAL, ObDecimalHelper::string_to_int(value, length, integer, fractional, precision, sign));
        }

        //小数点后面不是数字
        {
          char value[16] = "98663.r";
          length = strlen(value);
          EXPECT_EQ(OB_NOT_A_DECIMAL, TestObjDecimalHelper::check_decimal_str(value, length, has_fractional, has_sign, sign));
          EXPECT_EQ(OB_NOT_A_DECIMAL, ObDecimalHelper::string_to_int(value, length, integer, fractional, precision, sign));
        }
        {
          char value[16] = "+98663.e";
          length = strlen(value);
          EXPECT_EQ(OB_NOT_A_DECIMAL, TestObjDecimalHelper::check_decimal_str(value, length, has_fractional, has_sign, sign));
          EXPECT_EQ(OB_NOT_A_DECIMAL, ObDecimalHelper::string_to_int(value, length, integer, fractional, precision, sign));
        }
        {
          char value[16] = "-98663.h";
          length = strlen(value);
          EXPECT_EQ(OB_NOT_A_DECIMAL, TestObjDecimalHelper::check_decimal_str(value, length, has_fractional, has_sign, sign));
          EXPECT_EQ(OB_NOT_A_DECIMAL, ObDecimalHelper::string_to_int(value, length, integer, fractional, precision, sign));
        }

        //小数点后面是数字
        {
          char value[16] = "98663.54";
          length = strlen(value);
          EXPECT_EQ(OB_SUCCESS, TestObjDecimalHelper::check_decimal_str(value, length, has_fractional, has_sign, sign));
          EXPECT_EQ(0, ObDecimalHelper::string_to_int(value, length, integer, fractional, precision, sign));
          EXPECT_EQ(integer, 98663);
          EXPECT_EQ(fractional, 54);
          EXPECT_EQ(precision, 2);
          EXPECT_EQ(sign, true);
        }
        {
          char value[16] = "+98663.43";
          length = strlen(value);
          EXPECT_EQ(OB_SUCCESS, TestObjDecimalHelper::check_decimal_str(value, length, has_fractional, has_sign, sign));
          EXPECT_EQ(0, ObDecimalHelper::string_to_int(value, length, integer, fractional, precision, sign));
          EXPECT_EQ(integer, 98663);
          EXPECT_EQ(fractional, 43);
          EXPECT_EQ(precision, 2);
          EXPECT_EQ(sign, true);
        }
        {
          char value[16] = "-98663.23";
          length = strlen(value);
          EXPECT_EQ(OB_SUCCESS, TestObjDecimalHelper::check_decimal_str(value, length, has_fractional, has_sign,  sign));
          EXPECT_EQ(0, ObDecimalHelper::string_to_int(value, length, integer, fractional, precision, sign));
          EXPECT_EQ(integer, 98663);
          EXPECT_EQ(fractional, 23);
          EXPECT_EQ(precision, 2);
          EXPECT_EQ(sign, false);
        }
        {
          char value[16] = "98663.54ge";
          length = strlen(value);
          EXPECT_EQ(OB_NOT_A_DECIMAL, TestObjDecimalHelper::check_decimal_str(value, length, has_fractional, has_sign, sign));
          EXPECT_EQ(OB_NOT_A_DECIMAL, ObDecimalHelper::string_to_int(value, length, integer, fractional, precision, sign));
        }
        {
          char value[16] = "-98663.54ge";
          length = strlen(value);
          EXPECT_EQ(OB_NOT_A_DECIMAL, TestObjDecimalHelper::check_decimal_str(value, length, has_fractional, has_sign, sign));
          EXPECT_EQ(OB_NOT_A_DECIMAL, ObDecimalHelper::string_to_int(value, length, integer, fractional, precision, sign));
        }
        {
          char value[16] = "+98663.54ge";
          length = strlen(value);
          EXPECT_EQ(OB_NOT_A_DECIMAL, TestObjDecimalHelper::check_decimal_str(value, length, has_fractional, has_sign, sign));
          EXPECT_EQ(OB_NOT_A_DECIMAL, ObDecimalHelper::string_to_int(value, length, integer, fractional, precision, sign));
        }
        {
          char value[16] = "98663.54ge";
          length = strlen(value) - 2;
          EXPECT_EQ(0, TestObjDecimalHelper::check_decimal_str(value, length, has_fractional, has_sign, sign));
          EXPECT_EQ(0, ObDecimalHelper::string_to_int(value, length, integer, fractional, precision, sign));
          EXPECT_EQ(integer, 98663);
          EXPECT_EQ(fractional, 54);
          EXPECT_EQ(precision, 2);
          EXPECT_EQ(sign, true);
        }
        {
          char value[16] = "-98663.54fge";
          length = strlen(value) - 3;
          EXPECT_EQ(0, TestObjDecimalHelper::check_decimal_str(value, length, has_fractional, has_sign, sign));
          EXPECT_EQ(0, ObDecimalHelper::string_to_int(value, length, integer, fractional, precision, sign));
          EXPECT_EQ(integer, 98663);
          EXPECT_EQ(fractional, 54);
          EXPECT_EQ(precision, 2);
          EXPECT_EQ(sign, false);
        }
        {
          char value[16] = "+98663.54g5he";
          length = strlen(value) - 4;
          EXPECT_EQ(0, TestObjDecimalHelper::check_decimal_str(value, length, has_fractional, has_sign, sign));
          EXPECT_EQ(0, ObDecimalHelper::string_to_int(value, length, integer, fractional, precision, sign));
          EXPECT_EQ(integer, 98663);
          EXPECT_EQ(fractional, 54);
          EXPECT_EQ(precision, 2);
          EXPECT_EQ(sign, true);
        }
        {
          char value[16] = "98663.5456ge";
          length = strlen(value) - 4;
          EXPECT_EQ(0, TestObjDecimalHelper::check_decimal_str(value, length, has_fractional, has_sign, sign));
          EXPECT_EQ(0, ObDecimalHelper::string_to_int(value, length, integer, fractional, precision, sign));
          EXPECT_EQ(integer, 98663);
          EXPECT_EQ(fractional, 54);
          EXPECT_EQ(precision, 2);
          EXPECT_EQ(sign, true);
        }
        {
          char value[16] = "-98663.5454fge";
          length = strlen(value) - 5;
          EXPECT_EQ(0, TestObjDecimalHelper::check_decimal_str(value, length, has_fractional, has_sign, sign));
          EXPECT_EQ(0, ObDecimalHelper::string_to_int(value, length, integer, fractional, precision, sign));
          EXPECT_EQ(integer, 98663);
          EXPECT_EQ(fractional, 54);
          EXPECT_EQ(precision, 2);
          EXPECT_EQ(sign, false);
        }
        {
          char value[16] = "+98663.545g5he";
          length = strlen(value) - 5;
          EXPECT_EQ(0, TestObjDecimalHelper::check_decimal_str(value, length, has_fractional, has_sign, sign));
          EXPECT_EQ(0, ObDecimalHelper::string_to_int(value, length, integer, fractional, precision, sign));
          EXPECT_EQ(integer, 98663);
          EXPECT_EQ(fractional, 54);
          EXPECT_EQ(precision, 2);
          EXPECT_EQ(sign, true);
        }

        //整数位0
        {
          char value[16] = "0.43dd";
          length = strlen(value) - 2;
          EXPECT_EQ(0, ObDecimalHelper::string_to_int(value, length, integer, fractional, precision, sign));
          EXPECT_EQ(integer, 0);
          EXPECT_EQ(fractional, 43);
          EXPECT_EQ(precision, 2);
          EXPECT_EQ(sign, true);
        }
        //小数以0开头
        {
          char value[16] = "0.043dd";
          length = strlen(value) - 2;
          EXPECT_EQ(0, ObDecimalHelper::string_to_int(value, length, integer, fractional, precision, sign));
          EXPECT_EQ(integer, 0);
          EXPECT_EQ(fractional, 43);
          EXPECT_EQ(precision, 3);
          EXPECT_EQ(sign, true);
        }
        //整数非0，小数为0
        {
          char value[16] = "5.0043ff";
          length = strlen(value) - 2;
          EXPECT_EQ(0, ObDecimalHelper::string_to_int(value, length, integer, fractional, precision, sign));
          EXPECT_EQ(integer, 5);
          EXPECT_EQ(fractional, 43);
          EXPECT_EQ(precision, 4);
          EXPECT_EQ(sign, true);
        }
      }

      //static int ADD(ObDecimal &decimal,  const ObDecimal &add_decimal, const ObTableProperty &gb_table_property);
      TEST_F(TestObjDecimal, add)
      {
        ObDecimalHelper::meta_union meta;
        ObDecimalHelper::ObDecimal decimal;
        ObDecimalHelper::meta_union add_meta;
        ObDecimalHelper::ObDecimal add_decimal;
        ObTableProperty gb_table_property;
        //两个不同精度的obj
        {
          meta.value_.width_ = 4;
          meta.value_.precision_ = 2;
          meta.value_.sign_ = 0;
          decimal.integer = 1;
          decimal.fractional = 4;
          decimal.meta = meta;

          add_meta.value_.width_ = 4;
          add_meta.value_.precision_ = 3;
          add_meta.value_.sign_ = 0;
          add_decimal.integer = 1;
          add_decimal.fractional = 4;
          add_decimal.meta = add_meta;

          EXPECT_EQ(OB_DECIMAL_PRECISION_NOT_EQUAL, ObDecimalHelper::ADD(decimal, add_decimal, gb_table_property));
        }
        //参数为值为负
        {
          meta.value_.width_ = 4;
          meta.value_.precision_ = 2;
          meta.value_.sign_ = -1;
          decimal.integer = -63;
          decimal.fractional = 23;
          decimal.meta = meta;

          add_meta.value_.width_ = 4;
          add_meta.value_.precision_ = 2;
          add_meta.value_.sign_ = -1;
          add_decimal.integer = 42;
          add_decimal.fractional = 12;
          add_decimal.meta = add_meta;

          EXPECT_EQ(OB_INVALID_ARGUMENT, ObDecimalHelper::ADD(decimal, add_decimal, gb_table_property));
        }
        {
          meta.value_.width_ = 4;
          meta.value_.precision_ = 2;
          meta.value_.sign_ = -1;
          decimal.integer = 68;
          decimal.fractional = -23;
          decimal.meta = meta;

          add_meta.value_.width_ = 4;
          add_meta.value_.precision_ = 2;
          add_meta.value_.sign_ = -1;
          add_decimal.integer = 42;
          add_decimal.fractional = 12;
          add_decimal.meta = add_meta;

          EXPECT_EQ(OB_INVALID_ARGUMENT, ObDecimalHelper::ADD(decimal, add_decimal, gb_table_property));
        }
        {
          meta.value_.width_ = 4;
          meta.value_.precision_ = 2;
          meta.value_.sign_ = -1;
          decimal.integer = 63;
          decimal.fractional = 23;
          decimal.meta = meta;

          add_meta.value_.width_ = 4;
          add_meta.value_.precision_ = 2;
          add_meta.value_.sign_ = -1;
          add_decimal.integer = -42;
          add_decimal.fractional = 12;
          add_decimal.meta = add_meta;

          EXPECT_EQ(OB_INVALID_ARGUMENT, ObDecimalHelper::ADD(decimal, add_decimal, gb_table_property));
        }
        {
          meta.value_.width_ = 4;
          meta.value_.precision_ = 2;
          meta.value_.sign_ = -1;
          decimal.integer = 83;
          decimal.fractional = 23;
          decimal.meta = meta;

          add_meta.value_.width_ = 4;
          add_meta.value_.precision_ = 2;
          add_meta.value_.sign_ = -1;
          add_decimal.integer = 42;
          add_decimal.fractional = -12;
          add_decimal.meta = add_meta;

          EXPECT_EQ(OB_INVALID_ARGUMENT, ObDecimalHelper::ADD(decimal, add_decimal, gb_table_property));
        }
        //整数过大
        {
          meta.value_.width_ = 4;
          meta.value_.precision_ = 2;
          meta.value_.sign_ = -1;
          decimal.integer = 683;
          decimal.fractional = 23;
          decimal.meta = meta;

          add_meta.value_.width_ = 4;
          add_meta.value_.precision_ = 2;
          add_meta.value_.sign_ = -1;
          add_decimal.integer = 42;
          add_decimal.fractional = 12;
          add_decimal.meta = add_meta;

          EXPECT_EQ(OB_INVALID_ARGUMENT, ObDecimalHelper::ADD(decimal, add_decimal, gb_table_property));
        }

        {
          meta.value_.width_ = 4;
          meta.value_.precision_ = 2;
          meta.value_.sign_ = -1;
          decimal.integer = 83;
          decimal.fractional = 23;
          decimal.meta = meta;

          add_meta.value_.width_ = 4;
          add_meta.value_.precision_ = 2;
          add_meta.value_.sign_ = -1;
          add_decimal.integer = 432;
          add_decimal.fractional = 12;
          add_decimal.meta = add_meta;

          EXPECT_EQ(OB_INVALID_ARGUMENT, ObDecimalHelper::ADD(decimal, add_decimal, gb_table_property));
        }
        //小数过大
        {
          {
            meta.value_.width_ = 4;
            meta.value_.precision_ = 2;
            meta.value_.sign_ = -1;
            decimal.integer = 68;
            decimal.fractional = 323;
            decimal.meta = meta;

            add_meta.value_.width_ = 4;
            add_meta.value_.precision_ = 2;
            add_meta.value_.sign_ = -1;
            add_decimal.integer = 42;
            add_decimal.fractional = 12;
            add_decimal.meta = add_meta;

            EXPECT_EQ(OB_INVALID_ARGUMENT, ObDecimalHelper::ADD(decimal, add_decimal, gb_table_property));
          }

          {
            meta.value_.width_ = 4;
            meta.value_.precision_ = 2;
            meta.value_.sign_ = -1;
            decimal.integer = 68;
            decimal.fractional = 23;
            decimal.meta = meta;

            add_meta.value_.width_ = 4;
            add_meta.value_.precision_ = 2;
            add_meta.value_.sign_ = -1;
            add_decimal.integer = 42;
            add_decimal.fractional = 132;
            add_decimal.meta = add_meta;

            EXPECT_EQ(OB_INVALID_ARGUMENT, ObDecimalHelper::ADD(decimal, add_decimal, gb_table_property));
          }
          //精度相同，符号相同的定点数加法,无溢出，无进位 1.04 + 1.04 = 2.08
          {
            meta.value_.width_ = 4;
            meta.value_.precision_ = 2;
            meta.value_.sign_ = 0;
            decimal.integer = 1;
            decimal.fractional = 4;
            decimal.meta = meta;

            add_meta.value_.width_ = 4;
            add_meta.value_.precision_ = 2;
            add_meta.value_.sign_ = 0;
            add_decimal.integer = 1;
            add_decimal.fractional = 4;
            add_decimal.meta = add_meta;

            EXPECT_EQ(0, ObDecimalHelper::ADD(decimal, add_decimal, gb_table_property));
            EXPECT_EQ(decimal.integer, 2);
            EXPECT_EQ(decimal.fractional, 8);
            EXPECT_EQ(decimal.meta.value_.sign_, 0);
          }
          //符号位相同的定点数加入，整数部分有溢出，无进位 68.23 + 42.12
          {
            meta.value_.width_ = 4;
            meta.value_.precision_ = 2;
            meta.value_.sign_ = 0;
            decimal.integer = 68;
            decimal.fractional = 23;
            decimal.meta = meta;

            add_meta.value_.width_ = 4;
            add_meta.value_.precision_ = 2;
            add_meta.value_.sign_ = 0;
            add_decimal.integer = 42;
            add_decimal.fractional = 12;
            add_decimal.meta = add_meta;

            EXPECT_EQ(0, ObDecimalHelper::ADD(decimal, add_decimal, gb_table_property));
            EXPECT_EQ(decimal.integer,99);
            EXPECT_EQ(decimal.fractional, 99);
            EXPECT_EQ(decimal.meta.value_.sign_, 0);
          }
          //符号位相同的定点数加法，整数部分无溢出，小数部分有进位 18.83 + 42.62 = 61.45
          {
            meta.value_.width_ = 4;
            meta.value_.precision_ = 2;
            meta.value_.sign_ = 0;
            decimal.integer = 18;
            decimal.fractional = 83;
            decimal.meta = meta;

            add_meta.value_.width_ = 4;
            add_meta.value_.precision_ = 2;
            add_meta.value_.sign_ = 0;
            add_decimal.integer = 42;
            add_decimal.fractional = 62;
            add_decimal.meta = add_meta;

            EXPECT_EQ(0, ObDecimalHelper::ADD(decimal, add_decimal, gb_table_property));
            EXPECT_EQ(decimal.integer,61);
            EXPECT_EQ(decimal.fractional, 45);
            EXPECT_EQ(decimal.meta.value_.sign_, 0);
          }

          //相同符号位的加法，小数有进位，导致整数有溢出 57.83 + 42.62 = 99.99
          {
            meta.value_.width_ = 4;
            meta.value_.precision_ = 2;
            meta.value_.sign_ = 0;
            decimal.integer = 57;
            decimal.fractional = 83;
            decimal.meta = meta;

            add_meta.value_.width_ = 4;
            add_meta.value_.precision_ = 2;
            add_meta.value_.sign_ = 0;
            add_decimal.integer = 42;
            add_decimal.fractional = 62;
            add_decimal.meta = add_meta;

            EXPECT_EQ(0, ObDecimalHelper::ADD(decimal, add_decimal, gb_table_property));
            EXPECT_EQ(decimal.integer,99);
            EXPECT_EQ(decimal.fractional, 99);
            EXPECT_EQ(decimal.meta.value_.sign_, 0);
          }
          //=======================
          //以下的相同符号位为负号
          //=======================
          //精度相同，符号相同的定点数加法,无溢出，无进位 -1.04 + -1.05 = -2.08
          {
            meta.value_.width_ = 4;
            meta.value_.precision_ = 2;
            meta.value_.sign_ = -1;
            decimal.integer = 1;
            decimal.fractional = 4;
            decimal.meta = meta;

            add_meta.value_.width_ = 4;
            add_meta.value_.precision_ = 2;
            add_meta.value_.sign_ = -1;
            add_decimal.integer = 1;
            add_decimal.fractional = 4;
            add_decimal.meta = add_meta;

            EXPECT_EQ(0, ObDecimalHelper::ADD(decimal, add_decimal, gb_table_property));
            EXPECT_EQ(decimal.integer,2);
            EXPECT_EQ(decimal.fractional, 8);
            EXPECT_EQ(decimal.meta.value_.sign_, -1);
          }
          //符号位相同的定点数加入，整数部分有溢出，无进位 -68.23 - 42.12 = -99.99
          {
            meta.value_.width_ = 4;
            meta.value_.precision_ = 2;
            meta.value_.sign_ = -1;
            decimal.integer = 68;
            decimal.fractional = 23;
            decimal.meta = meta;

            add_meta.value_.width_ = 4;
            add_meta.value_.precision_ = 2;
            add_meta.value_.sign_ = -1;
            add_decimal.integer = 42;
            add_decimal.fractional = 12;
            add_decimal.meta = add_meta;

            EXPECT_EQ(0, ObDecimalHelper::ADD(decimal, add_decimal, gb_table_property));
            EXPECT_EQ(decimal.integer,99);
            EXPECT_EQ(decimal.fractional, 99);
            EXPECT_EQ(decimal.meta.value_.sign_, -1);
          }

          //符号位相同的定点数加法，整数部分无溢出，小数部分有进位 -18.83 - 42.62 = -61.46
          {
            meta.value_.width_ = 4;
            meta.value_.precision_ = 2;
            meta.value_.sign_ = -1;
            decimal.integer = 18;
            decimal.fractional = 83;
            decimal.meta = meta;

            add_meta.value_.width_ = 4;
            add_meta.value_.precision_ = 2;
            add_meta.value_.sign_ = -1;
            add_decimal.integer = 42;
            add_decimal.fractional = 62;
            add_decimal.meta = add_meta;

            EXPECT_EQ(0, ObDecimalHelper::ADD(decimal, add_decimal, gb_table_property));
            EXPECT_EQ(decimal.integer, 61);
            EXPECT_EQ(decimal.fractional, 45);
            EXPECT_EQ(decimal.meta.value_.sign_, -1);
          }

          //相同符号位的加法，小数有进位，导致整数有溢出 -57.83 - 42.62 = -99.99
          {
            meta.value_.width_ = 4;
            meta.value_.precision_ = 2;
            meta.value_.sign_ = -1;
            decimal.integer = 57;
            decimal.fractional = 83;
            decimal.meta = meta;

            add_meta.value_.width_ = 4;
            add_meta.value_.precision_ = 2;
            add_meta.value_.sign_ = -1;
            add_decimal.integer = 42;
            add_decimal.fractional = 62;
            add_decimal.meta = add_meta;

            EXPECT_EQ(0, ObDecimalHelper::ADD(decimal, add_decimal, gb_table_property));
            EXPECT_EQ(decimal.integer,99);
            EXPECT_EQ(decimal.fractional, 99);
            EXPECT_EQ(decimal.meta.value_.sign_, -1);
          }

          //==========================
          //以下为不同符号位定点数的相加
          //==========================

          //==========================
          //正数大于负数的加法
          //==========================

          //整数部分和小数部分均大于 57.84 - 42.62 = 15.22
          {
            meta.value_.width_ = 4;
            meta.value_.precision_ = 2;
            meta.value_.sign_ = 0;
            decimal.integer = 57;
            decimal.fractional = 84;
            decimal.meta = meta;

            add_meta.value_.width_ = 4;
            add_meta.value_.precision_ = 2;
            add_meta.value_.sign_ = -1;
            add_decimal.integer = 42;
            add_decimal.fractional = 62;
            add_decimal.meta = add_meta;

            EXPECT_EQ(0, ObDecimalHelper::ADD(decimal, add_decimal, gb_table_property));
            EXPECT_EQ(decimal.integer, 15);
            EXPECT_EQ(decimal.fractional, 22);
            EXPECT_EQ(decimal.meta.value_.sign_, 0);
          }
          //整数大于，小数等于 57.84 - 42.84= 15.0
          {
            meta.value_.width_ = 4;
            meta.value_.precision_ = 2;
            meta.value_.sign_ = 0;
            decimal.integer = 57;
            decimal.fractional = 84;
            decimal.meta = meta;

            add_meta.value_.width_ = 4;
            add_meta.value_.precision_ = 2;
            add_meta.value_.sign_ = -1;
            add_decimal.integer = 42;
            add_decimal.fractional = 84;
            add_decimal.meta = add_meta;

            EXPECT_EQ(0, ObDecimalHelper::ADD(decimal, add_decimal, gb_table_property));
            EXPECT_EQ(decimal.integer, 15);
            EXPECT_EQ(decimal.fractional, 0);
            EXPECT_EQ(decimal.meta.value_.sign_, 0);
          }
          //整数大于，小数小于 57.43 - 42.62 = 14.81
          {
            meta.value_.width_ = 4;
            meta.value_.precision_ = 2;
            meta.value_.sign_ = 0;
            decimal.integer = 57;
            decimal.fractional = 43;
            decimal.meta = meta;

            add_meta.value_.width_ = 4;
            add_meta.value_.precision_ = 2;
            add_meta.value_.sign_ = -1;
            add_decimal.integer = 42;
            add_decimal.fractional = 62;
            add_decimal.meta = add_meta;

            EXPECT_EQ(0, ObDecimalHelper::ADD(decimal, add_decimal, gb_table_property));
            EXPECT_EQ(decimal.integer, 14);
            EXPECT_EQ(decimal.fractional, 81);
            EXPECT_EQ(decimal.meta.value_.sign_, 0);
          }

          //==========================
          //负数大于正数的加法
          //==========================

          //负数的整数部分和小数部分均大于 -57.84 + 42.62 = - 15.22
          {
            meta.value_.width_ = 4;
            meta.value_.precision_ = 2;
            meta.value_.sign_ = -1;
            decimal.integer = 57;
            decimal.fractional = 84;
            decimal.meta = meta;

            add_meta.value_.width_ = 4;
            add_meta.value_.precision_ = 2;
            add_meta.value_.sign_ = 0;
            add_decimal.integer = 42;
            add_decimal.fractional = 62;
            add_decimal.meta = add_meta;

            EXPECT_EQ(0, ObDecimalHelper::ADD(decimal, add_decimal, gb_table_property));
            EXPECT_EQ(decimal.integer, 15);
            EXPECT_EQ(decimal.fractional, 22);
            EXPECT_EQ(decimal.meta.value_.sign_, -1);
          }
          //负数的整数大于，小数等于 -57.84 + 42.84
          {
            meta.value_.width_ = 4;
            meta.value_.precision_ = 2;
            meta.value_.sign_ = -1;
            decimal.integer = 57;
            decimal.fractional = 84;
            decimal.meta = meta;

            add_meta.value_.width_ = 4;
            add_meta.value_.precision_ = 2;
            add_meta.value_.sign_ = 0;
            add_decimal.integer = 42;
            add_decimal.fractional = 84;
            add_decimal.meta = add_meta;

            EXPECT_EQ(0, ObDecimalHelper::ADD(decimal, add_decimal, gb_table_property));
            EXPECT_EQ(decimal.integer, 15);
            EXPECT_EQ(decimal.fractional, 0);
            EXPECT_EQ(decimal.meta.value_.sign_, -1);
          }
          //负数的整数大于，小数小于 -57.44 + 42.62 = -14.82
          {
            meta.value_.width_ = 4;
            meta.value_.precision_ = 2;
            meta.value_.sign_ = -1;
            decimal.integer = 57;
            decimal.fractional = 44;
            decimal.meta = meta;

            add_meta.value_.width_ = 4;
            add_meta.value_.precision_ = 2;
            add_meta.value_.sign_ = 0;
            add_decimal.integer = 42;
            add_decimal.fractional = 62;
            add_decimal.meta = add_meta;

            EXPECT_EQ(0, ObDecimalHelper::ADD(decimal, add_decimal, gb_table_property));
            EXPECT_EQ(decimal.integer, 14);
            EXPECT_EQ(decimal.fractional, 82);
            EXPECT_EQ(decimal.meta.value_.sign_, -1);
          }
        }

      }

      // static int MUL(const ObDecimal &decimal, int64_t multiplicand, int64_t &product_int,
      //  int64_t &product_fra,   bool &sign, const ObTableProperty &gb_table_property)
      TEST_F(TestObjDecimal, mul)
      {
        //输入参数不正确
        int64_t mul = 64;
        int64_t product_int = 0;
        int64_t product_fra = 0;
        bool sign = false;
        ObTableProperty property;
        ObDecimalHelper::meta_union meta;
        ObDecimalHelper::ObDecimal decimal;
        meta.value_.width_ = 5;
        meta.value_.precision_ = 2;
        meta.value_.sign_ = 0;
        decimal.meta = meta;
        {
          decimal.integer = -1;
          decimal.fractional = 4;
          EXPECT_EQ(OB_INVALID_ARGUMENT, ObDecimalHelper::MUL(decimal, mul, product_int, product_fra, sign, property));
        }
        {
          decimal.integer = 8786;
          decimal.fractional = 4;
          EXPECT_EQ(OB_INVALID_ARGUMENT, ObDecimalHelper::MUL(decimal, mul, product_int, product_fra, sign, property));
        }
        {
          decimal.integer = 1;
          decimal.fractional = 342;
          EXPECT_EQ(OB_INVALID_ARGUMENT, ObDecimalHelper::MUL(decimal, mul, product_int, product_fra, sign, property));
        }
        {
          mul = 9999999999;
          decimal.integer = 1;
          decimal.fractional = 42;
          EXPECT_EQ(OB_INVALID_ARGUMENT, ObDecimalHelper::MUL(decimal, mul, product_int, product_fra, sign, property));
        }
        {
          decimal.integer = -1;
          decimal.fractional = 4;
          EXPECT_EQ(OB_INVALID_ARGUMENT, ObDecimalHelper::MUL(decimal, mul, product_int, product_fra, sign, property));
        }
        {
          decimal.integer = 1;
          decimal.fractional = -4;
          EXPECT_EQ(OB_INVALID_ARGUMENT, ObDecimalHelper::MUL(decimal, mul, product_int, product_fra, sign, property));
        }

        //被乘数为正常数 5.5 * 25 = 137.5 == 99.99
        {
          mul = 25;
          decimal.integer = 5;
          decimal.fractional = 50;
          EXPECT_EQ(0, ObDecimalHelper::MUL(decimal, mul, product_int, product_fra, sign, property));
          EXPECT_EQ(product_int, 137);
          EXPECT_EQ(product_fra, 50);
          EXPECT_EQ(sign, true);
        }
        //乘数为0
        {
          mul = 0;
          decimal.integer = 5;
          decimal.fractional = 50;
          EXPECT_EQ(0, ObDecimalHelper::MUL(decimal, mul, product_int, product_fra, sign, property));
          EXPECT_EQ(product_int, 0);
          EXPECT_EQ(product_fra, 0);
          EXPECT_EQ(sign, true);
        }
        //整数部分为0 0.5 * 25 = 12.5
        {
          mul = 25;
          decimal.integer = 0;
          decimal.fractional = 50;
          EXPECT_EQ(0, ObDecimalHelper::MUL(decimal, mul, product_int, product_fra, sign, property));
          EXPECT_EQ(product_int, 12);
          EXPECT_EQ(product_fra, 50);
          EXPECT_EQ(sign, true);
        }
        //小数部分为0 5.0 * 25 = 125.0
        {
          mul = 25;
          decimal.integer = 5;
          decimal.fractional = 0;
          EXPECT_EQ(0, ObDecimalHelper::MUL(decimal, mul, product_int, product_fra, sign, property));
          EXPECT_EQ(product_int, 125);
          EXPECT_EQ(product_fra, 0);
          EXPECT_EQ(sign, true);
        }
        //小数点后面有0 5.05 * 25 = 126.25
        {
          mul = 25;
          decimal.integer = 5;
          decimal.fractional = 5;
          EXPECT_EQ(0, ObDecimalHelper::MUL(decimal, mul, product_int, product_fra, sign, property));
          EXPECT_EQ(product_int, 126);
          EXPECT_EQ(product_fra, 25);
          EXPECT_EQ(sign, true);
        }

        //整数为0，小数点后也为0  0.05 * 1 = 0.05
        {
          mul = 1;
          decimal.integer = 0;
          decimal.fractional = 5;
          EXPECT_EQ(0, ObDecimalHelper::MUL(decimal, mul, product_int, product_fra, sign, property));
          EXPECT_EQ(product_int, 0);
          EXPECT_EQ(product_fra, 5);
          EXPECT_EQ(sign, true);
        }

        //同上  0.05 * 125 = 6.25
        {
          mul = 125;
          decimal.integer = 0;
          decimal.fractional = 5;
          EXPECT_EQ(0, ObDecimalHelper::MUL(decimal, mul, product_int, product_fra, sign, property));
          EXPECT_EQ(product_int, 6);
          EXPECT_EQ(product_fra, 25);
          EXPECT_EQ(sign, true);
        }

        //乘法以后，整数溢出 125.58 * 25 = 3139,50
        {
          mul = 25;
          decimal.integer = 125;
          decimal.fractional = 58;
          decimal.meta.value_.width_ = 5;
          decimal.meta.value_.precision_ = 2;
          EXPECT_EQ(0, ObDecimalHelper::MUL(decimal, mul, product_int, product_fra, sign, property));
          EXPECT_EQ(product_int, 999);
          EXPECT_EQ(product_fra, 99);
          EXPECT_EQ(sign, true);
        }

        //=========================
        //整数为负数
        //=========================
        //乘法以后，整数溢出 25.58 * -25 = 639,50
        {
          mul = -25;
          decimal.integer = 25;
          decimal.fractional = 58;
          EXPECT_EQ(0, ObDecimalHelper::MUL(decimal, mul, product_int, product_fra, sign, property));
          EXPECT_EQ(product_int, 639);
          EXPECT_EQ(product_fra, 50);
          EXPECT_EQ(sign, false);
        }

        //====================
        //都为负数
        //====================
        //乘法以后，整数溢出 -25.58 * -25 = 639,50
        {
          mul = -25;
          decimal.integer = 25;
          decimal.fractional = 58;
          decimal.meta.value_.sign_ = -1;
          EXPECT_EQ(0, ObDecimalHelper::MUL(decimal, mul, product_int, product_fra, sign, property));
          EXPECT_EQ(product_int, 639);
          EXPECT_EQ(product_fra, 50);
          EXPECT_EQ(sign, true);
        }
      }

      // static int DIV(const ObDecimal &decimal, int64_t divisor, int64_t &consult_int,
      //int64_t &consult_fra,  bool &sign,  const ObTableProperty &gb_table_property);
      TEST_F(TestObjDecimal, div)
      {
        //输入参数不正确
        int64_t mul = 64;
        int64_t product_int = 0;
        int64_t product_fra = 0;
        bool sign = false;
        ObTableProperty property;
        ObDecimalHelper::meta_union meta;
        ObDecimalHelper::ObDecimal decimal;
        meta.value_.width_ = 5;
        meta.value_.precision_ = 2;
        meta.value_.sign_ = 0;
        decimal.meta = meta;
        {
          decimal.integer = -1;
          decimal.fractional = 4;
          EXPECT_EQ(OB_INVALID_ARGUMENT, ObDecimalHelper::DIV(decimal, mul, product_int, product_fra, sign, property));
        }
        {
          decimal.integer = 8976;
          decimal.fractional = 4;
          EXPECT_EQ(OB_INVALID_ARGUMENT, ObDecimalHelper::DIV(decimal, mul, product_int, product_fra, sign, property));
        }
        {
          decimal.integer = 1;
          decimal.fractional = 342;
          EXPECT_EQ(OB_INVALID_ARGUMENT, ObDecimalHelper::DIV(decimal, mul, product_int, product_fra, sign, property));
        }
        {
          mul = 9999999999;
          decimal.integer = 1;
          decimal.fractional = 42;
          EXPECT_EQ(0, ObDecimalHelper::DIV(decimal, mul, product_int, product_fra, sign, property));
        }
        {
          decimal.integer = -1;
          decimal.fractional = 4;
          EXPECT_EQ(OB_INVALID_ARGUMENT, ObDecimalHelper::DIV(decimal, mul, product_int, product_fra, sign, property));
        }
        {
          decimal.integer = 1;
          decimal.fractional = -4;
          EXPECT_EQ(OB_INVALID_ARGUMENT, ObDecimalHelper::DIV(decimal, mul, product_int, product_fra, sign, property));
        }

        //正常计算
        //=====================
        //正常计算 先正数
        //=====================

        //整数部分为0 0.43 / 24 = 0.017
        {
          mul = 24;
          decimal.integer = 0;
          decimal.fractional = 43;
          EXPECT_EQ(0, ObDecimalHelper::DIV(decimal, mul, product_int, product_fra, sign, property));
          EXPECT_EQ(product_int, 0);
          EXPECT_EQ(product_fra, 2);
          EXPECT_EQ(sign, true);
        }
        //需要后移一位 0.45 / 67 =  0.006
        {
          mul = 67;
          decimal.integer = 0;
          decimal.fractional = 45;
          EXPECT_EQ(0, ObDecimalHelper::DIV(decimal, mul, product_int, product_fra, sign, property));
          EXPECT_EQ(product_int, 0);
          EXPECT_EQ(product_fra, 1);
          EXPECT_EQ(sign, true);
        }
        //小数部分为0  234 / 12 = 19.5
        {

          mul = 12;
          decimal.integer = 234;
          decimal.fractional = 0;
          EXPECT_EQ(0, ObDecimalHelper::DIV(decimal, mul, product_int, product_fra, sign, property));
          EXPECT_EQ(product_int, 19);
          EXPECT_EQ(product_fra, 50);
          EXPECT_EQ(sign, true);
        }
        //需要后移一位  234 / 54 = 4.333
        {

          mul = 54;
          decimal.integer = 234;
          decimal.fractional = 0;
          EXPECT_EQ(0, ObDecimalHelper::DIV(decimal, mul, product_int, product_fra, sign, property));
          EXPECT_EQ(product_int, 4);
          EXPECT_EQ(product_fra, 33);
          EXPECT_EQ(sign, true);
        }
        //小数四舍五入后有向高位的进位  456.89 / 14 = 32.635
        {

          mul = 14;
          decimal.integer = 456;
          decimal.fractional = 89;
          EXPECT_EQ(0, ObDecimalHelper::DIV(decimal, mul, product_int, product_fra, sign, property));
          EXPECT_EQ(product_int, 32);
          EXPECT_EQ(product_fra, 64);
          EXPECT_EQ(sign, true);
        }
        //小数有向整数的进位   3573.1 / 65 = 54.97
        {
          mul = 65;
          decimal.integer = 3573;
          decimal.fractional = 10;
          decimal.meta.value_.width_ = 7;
          EXPECT_EQ(0, ObDecimalHelper::DIV(decimal, mul, product_int, product_fra, sign, property));
          EXPECT_EQ(product_int, 54);
          EXPECT_EQ(product_fra, 97);
          EXPECT_EQ(sign, true);
        }
        //被除数为0 0 / 65 = 0.00
        {
          mul = 65;
          decimal.integer = 0;
          decimal.fractional = 0;
          EXPECT_EQ(0, ObDecimalHelper::DIV(decimal, mul, product_int, product_fra, sign, property));
          EXPECT_EQ(product_int, 0);
          EXPECT_EQ(product_fra, 0);
          EXPECT_EQ(sign, true);
        }
        //被除数为0  54.45 / 0  warn
        {
          mul = 0;
          decimal.integer = 234;
          decimal.fractional = 0;
          EXPECT_EQ(OB_OBJ_DIVIDE_BY_ZERO, ObDecimalHelper::DIV(decimal, mul, product_int, product_fra, sign, property));
        }

        //=================
        // 被除数为负数
        //=================

        //整数部分为0 -0.43 / 24 = 0.017
        {
          mul = 24;
          decimal.integer = 0;
          decimal.fractional = 43;
          decimal.meta.value_.sign_ = -1;
          EXPECT_EQ(0, ObDecimalHelper::DIV(decimal, mul, product_int, product_fra, sign, property));
          EXPECT_EQ(product_int, 0);
          EXPECT_EQ(product_fra, 2);
          EXPECT_EQ(sign, false);
        }
        //需要后移一位 -0.45 / 67 =  0.006
        {
          mul = 67;
          decimal.integer = 0;
          decimal.fractional = 45;
          decimal.meta.value_.sign_ = -1;
          EXPECT_EQ(0, ObDecimalHelper::DIV(decimal, mul, product_int, product_fra, sign, property));
          EXPECT_EQ(product_int, 0);
          EXPECT_EQ(product_fra, 1);
          EXPECT_EQ(sign, false);
        }
        //小数部分为0  -234 / 12 = -19.5
        {
          mul = 12;
          decimal.integer = 234;
          decimal.fractional = 0;
          decimal.meta.value_.sign_ = -1;
          EXPECT_EQ(0, ObDecimalHelper::DIV(decimal, mul, product_int, product_fra, sign, property));
          EXPECT_EQ(product_int, 19);
          EXPECT_EQ(product_fra, 50);
          EXPECT_EQ(sign, false);
        }
        //需要后移一位  -234 / 54 = -4.333
        {
          mul = 54;
          decimal.integer = 234;
          decimal.fractional = 0;
          decimal.meta.value_.sign_ = -1;
          EXPECT_EQ(0, ObDecimalHelper::DIV(decimal, mul, product_int, product_fra, sign, property));
          EXPECT_EQ(product_int, 4);
          EXPECT_EQ(product_fra, 33);
          EXPECT_EQ(sign, false);
        }
        //小数四舍五入后有向高位的进位  456.89 / 14 = 32.635
        {
          mul = 14;
          decimal.integer = 456;
          decimal.fractional = 89;
          EXPECT_EQ(0, ObDecimalHelper::DIV(decimal, mul, product_int, product_fra, sign, property));
          EXPECT_EQ(product_int, 32);
          EXPECT_EQ(product_fra, 64);
          EXPECT_EQ(sign, false);
        }
        //小数有向整数的进位   3573.1 / 65 = 54.97
        {
          mul = 65;
          decimal.integer = 3573;
          decimal.fractional = 10;
          EXPECT_EQ(0, ObDecimalHelper::DIV(decimal, mul, product_int, product_fra, sign, property));
          EXPECT_EQ(product_int, 54);
          EXPECT_EQ(product_fra, 97);
          EXPECT_EQ(sign, false);
        }
        //被除数为0 0 / 65 = 0.00
        {
          mul = 14;
          decimal.integer = 0;
          decimal.fractional = 0;
          EXPECT_EQ(0, ObDecimalHelper::DIV(decimal, mul, product_int, product_fra, sign, property));
          EXPECT_EQ(product_int, 0);
          EXPECT_EQ(product_fra, 0);
          EXPECT_EQ(sign, true);
        }
        property.error_division_by_zero = true;
        property.mode = SQL_STRICT;
        //被除数为0  54.45 / 0  error
        {
          mul = 0;
          decimal.integer = 456;
          decimal.fractional = 89;
          EXPECT_EQ(OB_OBJ_DIVIDE_ERROR, ObDecimalHelper::DIV(decimal, mul, product_int, product_fra, sign, property));
        }

        //=====================
        //两者均未负数
        //=====================

        //整数部分为0 -0.43 / -24 = 0.017
        {
          mul = -24;
          decimal.integer = 0;
          decimal.fractional = 43;
          decimal.meta.value_.sign_ = -1;
          EXPECT_EQ(0, ObDecimalHelper::DIV(decimal, mul, product_int, product_fra, sign, property));
          EXPECT_EQ(product_int, 0);
          EXPECT_EQ(product_fra, 2);
          EXPECT_EQ(sign, true);
        }
        //需要后移一位 -0.45 / 67 =  0.006
        {
          mul = -67;
          decimal.integer = 0;
          decimal.fractional = 45;
          decimal.meta.value_.sign_ = -1;
          EXPECT_EQ(0, ObDecimalHelper::DIV(decimal, mul, product_int, product_fra, sign, property));
          EXPECT_EQ(product_int, 0);
          EXPECT_EQ(product_fra, 1);
          EXPECT_EQ(sign, true);
        }
        //小数部分为0  -234 / -12 = -19.5
        {
          mul = -12;
          decimal.integer = 234;
          decimal.fractional = 0;
          decimal.meta.value_.sign_ = -1;
          EXPECT_EQ(0, ObDecimalHelper::DIV(decimal, mul, product_int, product_fra, sign, property));
          EXPECT_EQ(product_int, 19);
          EXPECT_EQ(product_fra, 50);
          EXPECT_EQ(sign, true);
        }
        //需要后移一位  -234 / -54 = 4.333
        {
          mul = -54;
          decimal.integer = 234;
          decimal.fractional = 0;
          decimal.meta.value_.sign_ = -1;
          EXPECT_EQ(0, ObDecimalHelper::DIV(decimal, mul, product_int, product_fra, sign, property));
          EXPECT_EQ(product_int, 4);
          EXPECT_EQ(product_fra, 33);
          EXPECT_EQ(sign, true);
        }
        //小数四舍五入后有向高位的进位  456.89 / 14 = 32.635
        {
          mul = 14;
          decimal.integer = 456;
          decimal.fractional = 89;
          decimal.meta.value_.sign_ = 0;
          EXPECT_EQ(0, ObDecimalHelper::DIV(decimal, mul, product_int, product_fra, sign, property));
          EXPECT_EQ(product_int, 32);
          EXPECT_EQ(product_fra, 64);
          EXPECT_EQ(sign, true);
        }
        //小数有向整数的进位   3573.1 / 65 = 54.97
        {
          mul = 65;
          decimal.integer = 3573;
          decimal.fractional = 10;
          EXPECT_EQ(0, ObDecimalHelper::DIV(decimal, mul, product_int, product_fra, sign, property));
          EXPECT_EQ(product_int, 54);
          EXPECT_EQ(product_fra, 97);
          EXPECT_EQ(sign, true);
        }
      }

      //static int decimal_compare(ObDecimal decimal, ObDecimal other_decimal);
      TEST_F(TestObjDecimal, decimal_compare)
      {
        ObDecimalHelper::meta_union meta;
        ObDecimalHelper::ObDecimal decimal;
        ObDecimalHelper::meta_union add_meta;
        ObDecimalHelper::ObDecimal add_decimal;

        //输入参数不对
        {
          meta.value_.width_ = -5;
          meta.value_.precision_ = 2;
          meta.value_.sign_ = 0;
          decimal.integer = 1;
          decimal.fractional = 4;
          decimal.meta = meta;

          add_meta.value_.width_ = 5;
          add_meta.value_.precision_ = 3;
          add_meta.value_.sign_ = 0;
          add_decimal.integer = 1;
          add_decimal.fractional = 4;
          add_decimal.meta = add_meta;

          EXPECT_EQ(OB_INVALID_ARGUMENT, ObDecimalHelper::decimal_compare(decimal, add_decimal));
        }
        {
          meta.value_.width_ = 5;
          meta.value_.precision_ = -2;
          meta.value_.sign_ = 0;
          decimal.integer = 1;
          decimal.fractional = 4;
          decimal.meta = meta;

          add_meta.value_.width_ = 5;
          add_meta.value_.precision_ = 3;
          add_meta.value_.sign_ = 0;
          add_decimal.integer = 1;
          add_decimal.fractional = 4;
          add_decimal.meta = add_meta;

          EXPECT_EQ(OB_INVALID_ARGUMENT, ObDecimalHelper::decimal_compare(decimal, add_decimal));
        }
        {
          meta.value_.width_ = 35;
          meta.value_.precision_ = 2;
          meta.value_.sign_ = 0;
          decimal.integer = 1;
          decimal.fractional = 4;
          decimal.meta = meta;

          add_meta.value_.width_ = 5;
          add_meta.value_.precision_ = 3;
          add_meta.value_.sign_ = 0;
          add_decimal.integer = 1;
          add_decimal.fractional = 4;
          add_decimal.meta = add_meta;

          EXPECT_EQ(OB_INVALID_ARGUMENT, ObDecimalHelper::decimal_compare(decimal, add_decimal));
        }

        //精度不同

        //两个不同精度的obj
        //正数
        {
          meta.value_.width_ = 5;
          meta.value_.precision_ = 2;
          meta.value_.sign_ = 0;
          decimal.integer = 1;
          decimal.fractional = 4;
          decimal.meta = meta;

          add_meta.value_.width_ = 5;
          add_meta.value_.precision_ = 3;
          add_meta.value_.sign_ = 0;
          add_decimal.integer = 1;
          add_decimal.fractional = 4;
          add_decimal.meta = add_meta;

          EXPECT_EQ(1, ObDecimalHelper::decimal_compare(decimal, add_decimal));
          EXPECT_EQ(-1, ObDecimalHelper::decimal_compare(add_decimal, decimal));
        }
        {
          meta.value_.width_ = 5;
          meta.value_.precision_ = 2;
          meta.value_.sign_ = 0;
          decimal.integer = 1;
          decimal.fractional = 4;
          decimal.meta = meta;

          add_meta.value_.width_ = 5;
          add_meta.value_.precision_ = 3;
          add_meta.value_.sign_ = 0;
          add_decimal.integer = 1;
          add_decimal.fractional = 40;
          add_decimal.meta = add_meta;

          EXPECT_EQ(0, ObDecimalHelper::decimal_compare(decimal, add_decimal));
          EXPECT_EQ(0, ObDecimalHelper::decimal_compare(add_decimal, decimal));
        }
        {
          meta.value_.width_ = 5;
          meta.value_.precision_ = 2;
          meta.value_.sign_ = 0;
          decimal.integer = 1;
          decimal.fractional = 4;
          decimal.meta = meta;

          add_meta.value_.width_ = 5;
          add_meta.value_.precision_ = 3;
          add_meta.value_.sign_ = 0;
          add_decimal.integer = 1;
          add_decimal.fractional = 42;
          add_decimal.meta = add_meta;

          EXPECT_EQ(-1, ObDecimalHelper::decimal_compare(decimal, add_decimal));
          EXPECT_EQ(1, ObDecimalHelper::decimal_compare(add_decimal, decimal));
        }


        //负数
        {
          meta.value_.width_ = 5;
          meta.value_.precision_ = 2;
          meta.value_.sign_ = -1;
          decimal.integer = 1;
          decimal.fractional = 4;
          decimal.meta = meta;

          add_meta.value_.width_ = 5;
          add_meta.value_.precision_ = 3;
          add_meta.value_.sign_ = -1;
          add_decimal.integer = 1;
          add_decimal.fractional = 4;
          add_decimal.meta = add_meta;

          EXPECT_EQ(-1, ObDecimalHelper::decimal_compare(decimal, add_decimal));
          EXPECT_EQ(1, ObDecimalHelper::decimal_compare(add_decimal, decimal));
        }
        {
          meta.value_.width_ = 5;
          meta.value_.precision_ = 2;
          meta.value_.sign_ = -1;
          decimal.integer = 1;
          decimal.fractional = 4;
          decimal.meta = meta;

          add_meta.value_.width_ = 5;
          add_meta.value_.precision_ = 3;
          add_meta.value_.sign_ = -1;
          add_decimal.integer = 1;
          add_decimal.fractional = 40;
          add_decimal.meta = add_meta;

          EXPECT_EQ(0, ObDecimalHelper::decimal_compare(decimal, add_decimal));
          EXPECT_EQ(0, ObDecimalHelper::decimal_compare(add_decimal, decimal));
        }
        {
          meta.value_.width_ = 5;
          meta.value_.precision_ = 2;
          meta.value_.sign_ = -1;
          decimal.integer = 1;
          decimal.fractional = 4;
          decimal.meta = meta;

          add_meta.value_.width_ = 5;
          add_meta.value_.precision_ = 3;
          add_meta.value_.sign_ = -1;
          add_decimal.integer = 1;
          add_decimal.fractional = 42;
          add_decimal.meta = add_meta;

          EXPECT_EQ(1, ObDecimalHelper::decimal_compare(decimal, add_decimal));
          EXPECT_EQ(-1, ObDecimalHelper::decimal_compare(add_decimal, decimal));
        }

        //精度相同
        //整数
        {
          meta.value_.width_ = 5;
          meta.value_.precision_ = 3;
          meta.value_.sign_ = 0;
          decimal.integer = 3;
          decimal.fractional = 4;
          decimal.meta = meta;

          add_meta.value_.width_ = 5;
          add_meta.value_.precision_ = 3;
          add_meta.value_.sign_ = 0;
          add_decimal.integer = 1;
          add_decimal.fractional = 42;
          add_decimal.meta = add_meta;

          EXPECT_EQ(1, ObDecimalHelper::decimal_compare(decimal, add_decimal));
          EXPECT_EQ(-1, ObDecimalHelper::decimal_compare(add_decimal, decimal));
        }
        {
          meta.value_.width_ = 5;
          meta.value_.precision_ = 3;
          meta.value_.sign_ = 0;
          decimal.integer = 1;
          decimal.fractional = 44;
          decimal.meta = meta;

          add_meta.value_.width_ = 5;
          add_meta.value_.precision_ = 3;
          add_meta.value_.sign_ = 0;
          add_decimal.integer = 1;
          add_decimal.fractional = 42;
          add_decimal.meta = add_meta;

          EXPECT_EQ(1, ObDecimalHelper::decimal_compare(decimal, add_decimal));
          EXPECT_EQ(-1, ObDecimalHelper::decimal_compare(add_decimal, decimal));
        }
        {
          meta.value_.width_ = 5;
          meta.value_.precision_ = 3;
          meta.value_.sign_ = 0;
          decimal.integer = 3;
          decimal.fractional = 42;
          decimal.meta = meta;

          add_meta.value_.width_ = 5;
          add_meta.value_.precision_ = 3;
          add_meta.value_.sign_ = 0;
          add_decimal.integer = 1;
          add_decimal.fractional = 420;
          add_decimal.meta = add_meta;

          EXPECT_EQ(1, ObDecimalHelper::decimal_compare(decimal, add_decimal));
          EXPECT_EQ(-1, ObDecimalHelper::decimal_compare(add_decimal, decimal));
        }
        {
          meta.value_.width_ = 5;
          meta.value_.precision_ = 2;
          meta.value_.sign_ = 0;
          decimal.integer = 3;
          decimal.fractional = 42;
          decimal.meta = meta;

          add_meta.value_.width_ = 5;
          add_meta.value_.precision_ = 3;
          add_meta.value_.sign_ = 0;
          add_decimal.integer = 3;
          add_decimal.fractional = 420;
          add_decimal.meta = add_meta;

          EXPECT_EQ(0, ObDecimalHelper::decimal_compare(decimal, add_decimal));
          EXPECT_EQ(0, ObDecimalHelper::decimal_compare(add_decimal, decimal));
        }
        //负数
        {
          meta.value_.width_ = 5;
          meta.value_.precision_ = 3;
          meta.value_.sign_ = -1;
          decimal.integer = 3;
          decimal.fractional = 4;
          decimal.meta = meta;

          add_meta.value_.width_ = 5;
          add_meta.value_.precision_ = 3;
          add_meta.value_.sign_ = -1;
          add_decimal.integer = 1;
          add_decimal.fractional = 42;
          add_decimal.meta = add_meta;

          EXPECT_EQ(-1, ObDecimalHelper::decimal_compare(decimal, add_decimal));
          EXPECT_EQ(1, ObDecimalHelper::decimal_compare(add_decimal, decimal));
        }
        {
          meta.value_.width_ = 5;
          meta.value_.precision_ = 3;
          meta.value_.sign_ = -1;
          decimal.integer = 1;
          decimal.fractional = 44;
          decimal.meta = meta;

          add_meta.value_.width_ = 5;
          add_meta.value_.precision_ = 3;
          add_meta.value_.sign_ = -1;
          add_decimal.integer = 1;
          add_decimal.fractional = 42;
          add_decimal.meta = add_meta;

          EXPECT_EQ(-1, ObDecimalHelper::decimal_compare(decimal, add_decimal));
          EXPECT_EQ(1, ObDecimalHelper::decimal_compare(add_decimal, decimal));
        }
        {
          meta.value_.width_ = 5;
          meta.value_.precision_ = 3;
          meta.value_.sign_ = -1;
          decimal.integer = 3;
          decimal.fractional = 42;
          decimal.meta = meta;

          add_meta.value_.width_ = 5;
          add_meta.value_.precision_ = 3;
          add_meta.value_.sign_ = -1;
          add_decimal.integer = 3;
          add_decimal.fractional = 42;
          add_decimal.meta = add_meta;

          EXPECT_EQ(0, ObDecimalHelper::decimal_compare(decimal, add_decimal));
          EXPECT_EQ(0, ObDecimalHelper::decimal_compare(add_decimal, decimal));
        }

        //符号不同
        {
          meta.value_.width_ = 5;
          meta.value_.precision_ = 3;
          meta.value_.sign_ = 0;
          decimal.integer = 3;
          decimal.fractional = 4;
          decimal.meta = meta;

          add_meta.value_.width_ = 5;
          add_meta.value_.precision_ = 3;
          add_meta.value_.sign_ = 0;
          add_decimal.integer = 1;
          add_decimal.fractional = 42;
          add_decimal.meta = add_meta;

          EXPECT_EQ(1, ObDecimalHelper::decimal_compare(decimal, add_decimal));
          EXPECT_EQ(-1, ObDecimalHelper::decimal_compare(add_decimal, decimal));
        }
        {
          meta.value_.width_ = 5;
          meta.value_.precision_ = 3;
          meta.value_.sign_ = 0;
          decimal.integer = 1;
          decimal.fractional = 44;
          decimal.meta = meta;

          add_meta.value_.width_ = 5;
          add_meta.value_.precision_ = 3;
          add_meta.value_.sign_ = 0;
          add_decimal.integer = 1;
          add_decimal.fractional = 42;
          add_decimal.meta = add_meta;

          EXPECT_EQ(1, ObDecimalHelper::decimal_compare(decimal, add_decimal));
          EXPECT_EQ(-1, ObDecimalHelper::decimal_compare(add_decimal, decimal));
        }
        {
          meta.value_.width_ = 5;
          meta.value_.precision_ = 3;
          meta.value_.sign_ = 0;
          decimal.integer = 3;
          decimal.fractional = 42;
          decimal.meta = meta;

          add_meta.value_.width_ = 5;
          add_meta.value_.precision_ = 3;
          add_meta.value_.sign_ = -1;
          add_decimal.integer = 1;
          add_decimal.fractional = 420;
          add_decimal.meta = add_meta;

          EXPECT_EQ(1, ObDecimalHelper::decimal_compare(decimal, add_decimal));
          EXPECT_EQ(-1, ObDecimalHelper::decimal_compare(add_decimal, decimal));
        }
      }

      //int round_off(int64_t &fractional, bool &is_carry, const int64_t src_pre, const int64_t dest_pre)
      TEST_F(TestObjDecimal, round_off_2)
      {
        {
          int64_t fractional = 170;
          bool is_carry = false;
          int64_t src_pre = 5;
          int64_t dest_pre = 3;
          EXPECT_EQ(0, ObDecimalHelper::round_off(fractional, is_carry, src_pre, dest_pre));
          EXPECT_EQ(fractional, 2);
          EXPECT_EQ(is_carry, false);
        }
        {
          int64_t fractional = 170;
          bool is_carry = false;
          int64_t src_pre = 5;
          int64_t dest_pre = 7;
          EXPECT_EQ(0, ObDecimalHelper::round_off(fractional, is_carry, src_pre, dest_pre));
          EXPECT_EQ(fractional, 17000);
          EXPECT_EQ(is_carry, false);
        }
        {
          int64_t fractional = 176;
          bool is_carry = false;
          int64_t src_pre = 2;
          int64_t dest_pre = 2;
          EXPECT_EQ(0, ObDecimalHelper::round_off(fractional, is_carry, src_pre, dest_pre));
          EXPECT_EQ(fractional, 18);
          EXPECT_EQ(is_carry, false);
        }
        {
          int64_t fractional = 997;
          bool is_carry = false;
          int64_t src_pre = 5;
          int64_t dest_pre = 3;
          EXPECT_EQ(0, ObDecimalHelper::round_off(fractional, is_carry, src_pre, dest_pre));
          EXPECT_EQ(fractional, 10);
          EXPECT_EQ(is_carry, false);
        }
        {
          int64_t fractional = 95;
          bool is_carry = false;
          int64_t src_pre = 5;
          int64_t dest_pre = 3;
          EXPECT_EQ(0, ObDecimalHelper::round_off(fractional, is_carry, src_pre, dest_pre));
          EXPECT_EQ(fractional, 1);
          EXPECT_EQ(is_carry, false);
        }
        {
          int64_t fractional = 9995;
          bool is_carry = false;
          int64_t src_pre = 5;
          int64_t dest_pre = 3;
          EXPECT_EQ(0, ObDecimalHelper::round_off(fractional, is_carry, src_pre, dest_pre));
          EXPECT_EQ(fractional, 100);
          EXPECT_EQ(is_carry, false);
        }
        {
          int64_t fractional = 99995;
          bool is_carry = false;
          int64_t src_pre = 5;
          int64_t dest_pre = 3;
          EXPECT_EQ(0, ObDecimalHelper::round_off(fractional, is_carry, src_pre, dest_pre));
          EXPECT_EQ(fractional, 0);
          EXPECT_EQ(is_carry, true);
        }
        {
          int64_t fractional = 6;
          bool is_carry = false;
          int64_t src_pre = 7;
          int64_t dest_pre = 3;
          EXPECT_EQ(0, ObDecimalHelper::round_off(fractional, is_carry, src_pre, dest_pre));
          EXPECT_EQ(fractional, 0);
          EXPECT_EQ(is_carry, false);
        }
        {
          int64_t fractional = 65;
          bool is_carry = false;
          int64_t src_pre = 5;
          int64_t dest_pre = 3;
          EXPECT_EQ(0, ObDecimalHelper::round_off(fractional, is_carry, src_pre, dest_pre));
          EXPECT_EQ(fractional, 1);
          EXPECT_EQ(is_carry, false);
        }
        {
          int64_t fractional = 45;
          bool is_carry = false;
          int64_t src_pre = 5;
          int64_t dest_pre = 3;
          EXPECT_EQ(0, ObDecimalHelper::round_off(fractional, is_carry, src_pre, dest_pre));
          EXPECT_EQ(fractional, 0);
          EXPECT_EQ(is_carry, false);
        }
        {
          int64_t fractional = 5;
          bool is_carry = false;
          int64_t src_pre = 5;
          int64_t dest_pre = 3;
          EXPECT_EQ(0, ObDecimalHelper::round_off(fractional, is_carry, src_pre, dest_pre));
          EXPECT_EQ(fractional, 0);
          EXPECT_EQ(is_carry, false);
        }



      }

      //int int_to_byte(int8_t* array, const int64_t size, int64_t &value_len, const int64_t value)
      TEST_F(TestObjDecimal, int_to_byte)
      {
        //输入参数不正确
        {
          int8_t *array = NULL;
          int64_t size = OB_ARRAY_SIZE;
          int64_t value = 1;
          int64_t value_len = 0;
          EXPECT_EQ(OB_INVALID_ARGUMENT, TestObjDecimalHelper::int_to_byte(array, size, value_len ,value, true));
        }
        {
          int8_t array[54];
          int64_t size = 54;
          int64_t value = 1;
          int64_t value_len = 0;
          EXPECT_EQ(OB_INVALID_ARGUMENT, TestObjDecimalHelper::int_to_byte(array, size, value_len ,value, true));
        }
        {
          int8_t array[OB_ARRAY_SIZE];
          int64_t size = OB_ARRAY_SIZE;
          int64_t value = -1;
          int64_t value_len = 0;
          EXPECT_EQ(OB_INVALID_ARGUMENT, TestObjDecimalHelper::int_to_byte(array, size, value_len ,value, true));
        }
        //正确处理
        //1位数
        {
          int8_t array[OB_ARRAY_SIZE];
          int64_t size = OB_ARRAY_SIZE;
          int64_t value = 1;
          int64_t value_len = 0;
          EXPECT_EQ(0, TestObjDecimalHelper::int_to_byte(array, size, value_len ,value, true));
          EXPECT_EQ(value_len, 1);
          EXPECT_EQ(array[0], 1);
        }
        //2位数
        {
          int8_t array[OB_ARRAY_SIZE];
          int64_t size = OB_ARRAY_SIZE;
          int64_t value = 14;
          int64_t value_len = 0;
          EXPECT_EQ(0, TestObjDecimalHelper::int_to_byte(array, size, value_len ,value, true));
          EXPECT_EQ(value_len, 2);
          EXPECT_EQ(array[0], 1);
          EXPECT_EQ(array[1], 4);
        }
        //4位数
        {
          int8_t array[OB_ARRAY_SIZE];
          int64_t size = OB_ARRAY_SIZE;
          int64_t value = 1000;
          int64_t value_len = 0;
          EXPECT_EQ(0, TestObjDecimalHelper::int_to_byte(array, size, value_len ,value, true));
          EXPECT_EQ(value_len, 4);
          EXPECT_EQ(array[0], 1);
          EXPECT_EQ(array[1], 0);
          EXPECT_EQ(array[2], 0);
          EXPECT_EQ(array[3], 0);
        }
        //8位数
        {
          int8_t array[OB_ARRAY_SIZE];
          int64_t size = OB_ARRAY_SIZE;
          int64_t value = 12340678;
          int64_t value_len = 0;
          EXPECT_EQ(0, TestObjDecimalHelper::int_to_byte(array, size, value_len ,value, true));
          EXPECT_EQ(value_len, 8);
          EXPECT_EQ(array[0], 1);
          EXPECT_EQ(array[1], 2);
          EXPECT_EQ(array[2], 3);
          EXPECT_EQ(array[3], 4);
          EXPECT_EQ(array[4], 0);
          EXPECT_EQ(array[5], 6);
          EXPECT_EQ(array[6], 7);
          EXPECT_EQ(array[7], 8);
        }		
      }

      //int get_digit_number(const int64_t value, int64_t &number)
      TEST_F(TestObjDecimal, get_digit_number)
      {
        //输入参数不正确
        {
          int64_t value = -1;
          int64_t number = 0;
          EXPECT_EQ(OB_INVALID_ARGUMENT, TestObjDecimalHelper::get_digit_number(value, number));
        }
        //正确输入
        //1位数
        {
          int64_t value = 0;
          int64_t number = 0;
          EXPECT_EQ(0, TestObjDecimalHelper::get_digit_number(value, number));
          EXPECT_EQ(1, number);
        }
        {
          int64_t value = 1;
          int64_t number = 0;
          EXPECT_EQ(0, TestObjDecimalHelper::get_digit_number(value, number));
          EXPECT_EQ(1, number);
        }
        //2位数
        {
          int64_t value = 18;
          int64_t number = 0;
          EXPECT_EQ(0, TestObjDecimalHelper::get_digit_number(value, number));
          EXPECT_EQ(2, number);
        }
        //4位数
        {
          int64_t value = 1020;
          int64_t number = 0;
          EXPECT_EQ(0, TestObjDecimalHelper::get_digit_number(value, number));
          EXPECT_EQ(4, number);
        }
        //7位数
        {
          int64_t value = 1234567;
          int64_t number = 0;
          EXPECT_EQ(0, TestObjDecimalHelper::get_digit_number(value, number));
          EXPECT_EQ(7, number);
        }
      }

      //int byte_to_int(int64_t &value, const int8_t *array, const int64_t value_len)
      TEST_F(TestObjDecimal, byte_to_int)
      {
        //输入不正确
        {
          int8_t *array = NULL;
          int64_t value = 0;
          int64_t value_len = 2;
          EXPECT_EQ(OB_INVALID_ARGUMENT, TestObjDecimalHelper::byte_to_int(value, array, value_len));
        }
        {
          int8_t array[12];
          int64_t value = 0;
          int64_t value_len = 0;
          EXPECT_EQ(OB_INVALID_ARGUMENT, TestObjDecimalHelper::byte_to_int(value, array, value_len));
        }
        {
          int8_t array[12];
          int64_t value = 0;
          int64_t value_len = -3;
          EXPECT_EQ(OB_INVALID_ARGUMENT, TestObjDecimalHelper::byte_to_int(value, array, value_len));
        }
        //正常输入
        {
          int8_t array[12] = {1, 2};
          int64_t value = 0;
          int64_t value_len = 2;
          EXPECT_EQ(0, TestObjDecimalHelper::byte_to_int(value, array, value_len));
          EXPECT_EQ(value, 12);
        }
        //含有非数字
        {
          int8_t array[12] = {1, 2, 'j', 4};
          int64_t value = 0;
          int64_t value_len = 4;
          EXPECT_EQ(OB_INVALID_ARGUMENT, TestObjDecimalHelper::byte_to_int(value, array, value_len));
        }
        //较长数字
        {
          int8_t array[12] = {1, 2, 3, 4, 5, 6};
          int64_t value = 0;
          int64_t value_len = 6;
          EXPECT_EQ(0, TestObjDecimalHelper::byte_to_int(value, array, value_len));
          EXPECT_EQ(value, 123456);
          value_len = 5;
          EXPECT_EQ(0, TestObjDecimalHelper::byte_to_int(value, array, value_len));
          EXPECT_EQ(value, 12345);
        }
        //太长的数字
        {
          int8_t array[19] = {1,2,3,4,5,6,7,8,9,1,2,3,4,5,6,7,8,9,0};
          int64_t value = 0;
          int64_t value_len = 19;
          EXPECT_EQ(OB_INVALID_ARGUMENT, TestObjDecimalHelper::byte_to_int(value, array, value_len));
          value_len = 18;
          EXPECT_EQ(0, TestObjDecimalHelper::byte_to_int(value, array, value_len));
          EXPECT_EQ(value, 123456789123456789);
          array[4] = 'j';
          EXPECT_EQ(OB_INVALID_ARGUMENT, TestObjDecimalHelper::byte_to_int(value, array, value_len));
        }		
      }

      //int byte_add(int8_t *add, const int8_t *adder, const int64_t size, const int64_t pos)
      TEST_F(TestObjDecimal, byte_add)
      {
        //输入不正确
        {
          int64_t size = 64;
          int8_t *add = NULL;
          int8_t adder[size];
          int64_t pos = 1;
          EXPECT_EQ(OB_INVALID_ARGUMENT, TestObjDecimalHelper::byte_add(add, adder, size, pos));
        }		
        {
          int64_t size = 32;
          int8_t add[size];
          int8_t adder[size];
          int64_t pos = 1;
          EXPECT_EQ(OB_INVALID_ARGUMENT, TestObjDecimalHelper::byte_add(add, adder, size, pos));
        }		
        {
          int64_t size = 64;
          int8_t add[size];
          int8_t *adder = NULL;
          int64_t pos = 1;
          EXPECT_EQ(OB_INVALID_ARGUMENT, TestObjDecimalHelper::byte_add(add, adder, size, pos));
        }
        {
          int64_t size = 64;
          int8_t add[size];
          int8_t adder[size];
          int64_t pos = -1;
          EXPECT_EQ(OB_INVALID_ARGUMENT, TestObjDecimalHelper::byte_add(add, adder, size, pos));
        }
        {
          int64_t size = 64;
          int8_t add[size];
          int8_t adder[size];
          int64_t pos = 64;
          EXPECT_EQ(0, TestObjDecimalHelper::byte_add(add, adder, size, pos));
        }
        //正常输入
        //pos = 0, 无进位
        {
          int64_t size = 64;
          int8_t add[size];
          for(int64_t i = 0; i < size; i++)
          {
            add[i] = 0;
          }
          add[size - 1 - 0] = 1;
          add[size - 1 -1] = 1;
          add[size - 1 -2] = 1;
          add[size - 1 -3] = 1;
          int8_t adder[size];
          for(int64_t i = 0; i < size; i++)
          {
            adder[i] = 0;
          }
          adder[size - 1 -0] = 1;
          adder[size - 1 -1] = 1;
          adder[size - 1 -2] = 1;
          adder[size - 1 -3] = 1;
          adder[size - 1 -4] = 1;		  
          int64_t pos = 0;
          EXPECT_EQ(0, TestObjDecimalHelper::byte_add(add, adder, size, pos));
          EXPECT_EQ(add[size - 1 -0], 2);
          EXPECT_EQ(add[size - 1 -1], 2);
          EXPECT_EQ(add[size - 1 -2], 2);
          EXPECT_EQ(add[size - 1 -3], 2);
          EXPECT_EQ(add[size - 1 -4], 1);
          EXPECT_EQ(add[size - 1 -5], 0);
        }
        //pos = 0, 有进位
        {
          int64_t size = 64;
          int8_t add[size];
          for(int64_t i = 0; i < size; i++)
          {
            add[i] = 0;
          }
          add[size - 1 -0] = 9;
          add[size - 1 -1] = 9;
          add[size - 1 -2] = 9;
          add[size - 1 -3] = 9;
          int8_t adder[size];
          for(int64_t i = 0; i < size; i++)
          {
            adder[i] = 0;
          }
          adder[size - 1 -0] = 1;
          adder[size - 1 -1] = 1;
          adder[size - 1 -2] = 1;
          adder[size - 1 -3] = 1;
          adder[size - 1 -4] = 1;		  
          int64_t pos = 0;
          EXPECT_EQ(0, TestObjDecimalHelper::byte_add(add, adder, size, pos));
          EXPECT_EQ(add[size - 1 -0], 0);
          EXPECT_EQ(add[size - 1 -1], 1);
          EXPECT_EQ(add[size - 1 -2], 1);
          EXPECT_EQ(add[size - 1 -3], 1);
          EXPECT_EQ(add[size - 1 -4], 2);
          EXPECT_EQ(add[size - 1 -5], 0);		  
        }
        //pos = 1, 无进位
        {
          int64_t size = 64;
          int8_t add[size];
          for(int64_t i = 0; i < size; i++)
          {
            add[i] = 0;
          }
          add[size - 1 -0] = 1;
          add[size - 1 -1] = 1;
          add[size - 1 -2] = 1;
          add[size - 1 -3] = 1;
          int8_t adder[size];
          for(int64_t i = 0; i < size; i++)
          {
            adder[i] = 0;
          }
          adder[size - 1 -0] = 1;
          adder[size - 1 -1] = 1;
          adder[size - 1 -2] = 1;
          adder[size - 1 -3] = 1;
          adder[size - 1 -4] = 1;		  
          int64_t pos = 1;
          EXPECT_EQ(0, TestObjDecimalHelper::byte_add(add, adder, size, pos));
          EXPECT_EQ(add[size - 1 -0], 1);
          EXPECT_EQ(add[size - 1 -1], 2);
          EXPECT_EQ(add[size - 1 -2], 2);
          EXPECT_EQ(add[size - 1 -3], 2);
          EXPECT_EQ(add[size - 1 -4], 1);
          EXPECT_EQ(add[size - 1 -5], 1);
          EXPECT_EQ(add[size - 1 -6], 0);
        }
        //pos = 2，有进位
        {
          int64_t size = 64;
          int8_t add[size];
          for(int64_t i = 0; i < size; i++)
          {
            add[i] = 0;
          }
          add[size - 1 -0] = 9;
          add[size - 1 -1] = 9;
          add[size - 1 -2] = 9;
          add[size - 1 -3] = 9;
          int8_t adder[size];
          for(int64_t i = 0; i < size; i++)
          {
            adder[i] = 0;
          }
          adder[size - 1 -0] = 1;
          adder[size - 1 -1] = 1;
          adder[size - 1 -2] = 1;
          adder[size - 1 -3] = 1;
          adder[size - 1 -4] = 1;		  
          int64_t pos = 2;
          EXPECT_EQ(0, TestObjDecimalHelper::byte_add(add, adder, size, pos));
          EXPECT_EQ(add[size - 1 -0], 9);
          EXPECT_EQ(add[size - 1 -1], 9);
          EXPECT_EQ(add[size - 1 -2], 0);
          EXPECT_EQ(add[size - 1 -3], 1);
          EXPECT_EQ(add[size - 1 -4], 2);
          EXPECT_EQ(add[size - 1 -5], 1);	
          EXPECT_EQ(add[size - 1 -6], 1);	
          EXPECT_EQ(add[size - 1 -7], 0);	
        }
        //中间含非数字
        {
          int64_t size = 64;
          int8_t add[size];
          for(int64_t i = 0; i < size; i++)
          {
            add[i] = 0;
          }
          add[size - 1 -0] = 1;
          add[size - 1 -1] = 'f';
          add[size - 1 -2] = 1;
          add[size - 1 -3] = 1;
          int8_t adder[size];
          for(int64_t i = 0; i < size; i++)
          {
            adder[i] = 0;
          }
          adder[size - 1 -0] = 1;
          adder[size - 1 -1] = 1;
          adder[size - 1 -2] = 1;
          adder[size - 1 -3] = 1;
          adder[size - 1 -4] = 1;		  
          int64_t pos = 0;
          EXPECT_EQ(OB_INVALID_ARGUMENT, TestObjDecimalHelper::byte_add(add, adder, size, pos));
          EXPECT_EQ(OB_INVALID_ARGUMENT, TestObjDecimalHelper::byte_add(adder, add, size, pos));
        }
      }

    }
  }
}


int main(int argc, char** argv)
{
  TBSYS_LOGGER.setLogLevel("DEBUG");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
