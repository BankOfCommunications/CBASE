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
      class ObjTest : public ObObj
      {
        public:
          int plus(const ObObj other_obj, const bool is_add)
          {
            return ObObj::decimal_apply_plus(other_obj, is_add);
          }
      };
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


      /* int ObObj::set_decimal(int64_t integer_number,int64_t fractional_number,                                                 const int64_t width,const int64_t precision, cosnt bool sign, const bool is_add)*/
      TEST_F(TestObjDecimal, set_decimal)
      {
        int64_t integer_number = 0;
        int64_t fractional_number = 0;
        //bool sign = false;
        int64_t width = 0;
        int64_t precision = 0;
        //obobj中保存的值
        int64_t integer = 0;
        int64_t fractional = 0;
        bool sign_to = false;
        //整数大于宽度
        {
          ObObj obobj;
          width = 4;
          precision = 2;
          integer_number = 880;
          fractional_number = 0;
          EXPECT_EQ(OB_INVALID_ARGUMENT, obobj.set_decimal(integer_number, fractional_number, width, precision));
        }
        //测试meta是否正确
        {
          ObObj obobj;
          width = 7;
          precision = 4;
          integer_number = 880;
          fractional_number = 80;
          EXPECT_EQ(0, obobj.set_decimal(integer_number, fractional_number, width, precision));
          obobj.get_decimal_integer(integer);
          EXPECT_EQ(880,integer);
          obobj.get_decimal_fractional(fractional);
          EXPECT_EQ(80, fractional);
          obobj.get_decimal_sign(sign_to);
          EXPECT_EQ(true, sign_to);
          ObDecimalHelper::meta_union meta;
          obobj.get_decimal(integer, fractional, meta);
          EXPECT_EQ(880,integer);
          EXPECT_EQ(80, fractional);
          EXPECT_EQ(7, meta.value_.width_);
          EXPECT_EQ(4, meta.value_.precision_);
          EXPECT_EQ(0, meta.value_.sign_);
        }
        //整数刚好等于宽度的最大值
        {
          ObObj obobj;
          width = 4;
          precision = 2;
          integer_number = 99;
          fractional_number = 0;
          EXPECT_EQ(0, obobj.set_decimal(integer_number, fractional_number, width, precision));
          obobj.get_decimal_integer(integer);
          EXPECT_EQ(99,integer);
          obobj.get_decimal_fractional(fractional);
          EXPECT_EQ(0, fractional);
          obobj.get_decimal_sign(sign_to);
          EXPECT_EQ(true, sign_to);
          ObDecimalHelper::meta_union meta;
          obobj.get_decimal(integer, fractional, meta);
          EXPECT_EQ(99,integer);
          EXPECT_EQ(0, fractional);
          EXPECT_EQ(4, meta.value_.width_);
          EXPECT_EQ(2, meta.value_.precision_);
          EXPECT_EQ(0, meta.value_.sign_);
        }
        //整数小于宽度
        {
          ObObj obobj;
          width = 5;
          precision = 2;
          integer_number = 880;
          fractional_number = 0;
          EXPECT_EQ(0, obobj.set_decimal(integer_number, fractional_number, width, precision));
          obobj.get_decimal_integer(integer);
          EXPECT_EQ(880,integer);
          obobj.get_decimal_fractional(fractional);
          EXPECT_EQ(0, fractional);
          obobj.get_decimal_sign(sign_to);
          EXPECT_EQ(true, sign_to);
          ObDecimalHelper::meta_union meta;
          obobj.get_decimal(integer, fractional, meta);
          EXPECT_EQ(880,integer);
          EXPECT_EQ(0, fractional);
          EXPECT_EQ(5, meta.value_.width_);
          EXPECT_EQ(2, meta.value_.precision_);
          EXPECT_EQ(0, meta.value_.sign_);
        }
        //小数位数等于精度

        {
          ObObj obobj;
          width = 4;
          precision = 2;
          integer_number = 88;
          fractional_number = 99;
          EXPECT_EQ(0, obobj.set_decimal(integer_number, fractional_number, width, precision));
          obobj.get_decimal_integer(integer);
          EXPECT_EQ(88,integer);
          obobj.get_decimal_fractional(fractional);
          EXPECT_EQ(99, fractional);
          obobj.get_decimal_sign(sign_to);
          EXPECT_EQ(true, sign_to);
        }

        //小数位数大于精度，但没有进位
        {
          ObObj obobj;
          width = 4;
          precision = 2;
          integer_number = 99;
          fractional_number = 123;
          EXPECT_EQ(OB_INVALID_ARGUMENT, obobj.set_decimal(integer_number, fractional_number, width, precision));
        }

        //因为符号位是单独存放的，所以不对带负号的定点数进行详细测试。
        //带负号的进位后导致溢出
        {
          ObObj obobj;
          width = 4;
          precision = 2;
          integer_number = 880;
          fractional_number = 0;
          EXPECT_EQ(OB_INVALID_ARGUMENT, obobj.set_decimal(integer_number, fractional_number, width, precision, false));
        }
        //宽度小于精度
        {
          ObObj obobj;
          width = 5;
          precision = 6;
          EXPECT_EQ(OB_INVALID_ARGUMENT, obobj.set_decimal(integer_number, fractional_number, width, precision, false));
        }
        //宽度小于0
        {
          ObObj obobj;
          width = -1;
          precision = 7;
          EXPECT_EQ(OB_INVALID_ARGUMENT, obobj.set_decimal(integer_number, fractional_number, width, precision, false));
        }

        //精度小于0
        {
          ObObj obobj;
          width = 4;
          precision = -2;
          EXPECT_EQ(OB_INVALID_ARGUMENT, obobj.set_decimal(integer_number, fractional_number, width, precision, false));
        }
        //宽度大于最大值
        {
          ObObj obobj;
          width = 67;
          precision = 2;
          EXPECT_EQ(OB_INVALID_ARGUMENT, obobj.set_decimal(integer_number, fractional_number, width, precision, false));
        }
        //精度大于最大
        {
          ObObj obobj;
          width = 4;
          precision = 31;
          EXPECT_EQ(OB_INVALID_ARGUMENT, obobj.set_decimal(integer_number, fractional_number, width, precision, false));
        }
      }

      /*set_decimal (const char *value, const int64_t length, const int64_t width,
        int64_t precision, const bool is_add) */
      TEST_F(TestObjDecimal, set_decimal_varchar)
      {
        char *value = NULL;
        int64_t length = 0;
        bool sign = false;
        int64_t width = 8;
        int64_t precision = 3;
        int64_t integer = 0;
        int64_t fractional = 0;

        //输入value为空
        {
          ObObj obj;
          length = 16;
          EXPECT_EQ(OB_INVALID_ARGUMENT, obj.set_decimal(value, length, width, precision));
        }
        //输入length 为0
        {
          ObObj obj;
          value = new char[16];
          length = 0;
          EXPECT_EQ(OB_INVALID_ARGUMENT, obj.set_decimal(value, length, width, precision));
          delete []value;
        }

        //输入参数正确，字符内容不正确
        {
          ObObj obj;
          char value[16] = "jingsong";
          length = strlen(value);
          EXPECT_EQ(OB_NOT_A_DECIMAL, obj.set_decimal(value, length, width, precision));
        }

        //第一个字符为符号位-，第二字符不对
        {
          ObObj obj;
          char value[16] = "+jinsong";
          length = strlen(value);
          EXPECT_EQ(OB_NOT_A_DECIMAL, obj.set_decimal(value, length, width, precision));
        }

        //首字符为+
        {
          ObObj obj;
          char value[16] = "-jinsong";
          length = strlen(value);
          EXPECT_EQ(OB_NOT_A_DECIMAL, obj.set_decimal(value, length, width, precision));
        }
        //首字符为数字
        {
          ObObj obj;
          char value[16] = "9jinsong";
          length = 1;
          EXPECT_EQ(OB_SUCCESS, obj.set_decimal(value, length, width, precision));
          obj.get_decimal_integer(integer);
          obj.get_decimal_fractional(fractional);
          obj.get_decimal_sign(sign);
          EXPECT_EQ(9, integer);
          EXPECT_EQ(0, fractional);
          EXPECT_EQ(true, sign);

        }
        //只有整数部分
        {
          ObObj obj;
          char value[16] = "98663";
          length = strlen(value);
          EXPECT_EQ(OB_SUCCESS, obj.set_decimal(value, length, width, precision));
          obj.get_decimal_integer(integer);
          obj.get_decimal_fractional(fractional);
          obj.get_decimal_sign(sign);
          EXPECT_EQ(98663, integer);
          EXPECT_EQ(0, fractional);
          EXPECT_EQ(true, sign);
        }
        {
          ObObj obj;
          char value[16] = "+98663";
          length = strlen(value);
          EXPECT_EQ(OB_SUCCESS, obj.set_decimal(value, length, width, precision));
          obj.get_decimal_integer(integer);
          obj.get_decimal_fractional(fractional);
          obj.get_decimal_sign(sign);
          EXPECT_EQ(98663, integer);
          EXPECT_EQ(0, fractional);
          EXPECT_EQ(true, sign);
        }
        {
          ObObj obj;
          char value[16] = "+98";
          length = strlen(value);
          EXPECT_EQ(OB_SUCCESS, obj.set_decimal(value, length, width, precision));
          obj.get_decimal_integer(integer);
          obj.get_decimal_fractional(fractional);
          obj.get_decimal_sign(sign);
          EXPECT_EQ(98, integer);
          EXPECT_EQ(0, fractional);
          EXPECT_EQ(true, sign);
        }
        {
          ObObj obj;
          char value[16] = "-98663";
          length = strlen(value);
          EXPECT_EQ(OB_SUCCESS, obj.set_decimal(value, length, width, precision));
          obj.get_decimal_integer(integer);
          obj.get_decimal_fractional(fractional);
          obj.get_decimal_sign(sign);
          EXPECT_EQ(98663, integer);
          EXPECT_EQ(0, fractional);
          EXPECT_EQ(false, sign);
        }
        //带小数点
        {
          ObObj obj;
          char value[16] = "98663.";
          length = strlen(value);
          EXPECT_EQ(OB_NOT_A_DECIMAL, obj.set_decimal(value, length, width, precision));
        }
        {
          ObObj obj;
          char value[16] = "+98663.";
          length = strlen(value);
          EXPECT_EQ(OB_NOT_A_DECIMAL, obj.set_decimal(value, length, width, precision));
        }
        {
          ObObj obj;
          char value[16] = "-98663.";
          length = strlen(value);
          EXPECT_EQ(OB_NOT_A_DECIMAL, obj.set_decimal(value, length, width, precision));
        }

        //小数点后面不是数字
        {
          ObObj obj;
          char value[16] = "98663.r";
          length = strlen(value);
          EXPECT_EQ(OB_NOT_A_DECIMAL, obj.set_decimal(value, length, width, precision));
        }
        {
          ObObj obj;
          char value[16] = "+98663.e";
          length = strlen(value);
          EXPECT_EQ(OB_NOT_A_DECIMAL, obj.set_decimal(value, length, width, precision));
        }
        {
          ObObj obj;
          char value[16] = "-98663.h";
          length = strlen(value);
          EXPECT_EQ(OB_NOT_A_DECIMAL, obj.set_decimal(value, length, width, precision));
        }
        //小数点后面是数字
        {
          ObObj obj;
          char value[16] = "98663.54";
          length = strlen(value);
          EXPECT_EQ(OB_SUCCESS, obj.set_decimal(value, length, width, precision));
          obj.get_decimal_integer(integer);
          obj.get_decimal_fractional(fractional);
          obj.get_decimal_sign(sign);
          EXPECT_EQ(98663, integer);
          EXPECT_EQ(540, fractional);
          EXPECT_EQ(true, sign);
        }
        {
          ObObj obj;
          char value[16] = "+63.43";
          length = strlen(value);
          EXPECT_EQ(OB_SUCCESS, obj.set_decimal(value, length, width, precision));
          obj.get_decimal_integer(integer);
          obj.get_decimal_fractional(fractional);
          obj.get_decimal_sign(sign);
          EXPECT_EQ(63, integer);
          EXPECT_EQ(430, fractional);
          EXPECT_EQ(true, sign);
        }
        {
          ObObj obj;
          char value[16] = "-98663.23";
          length = strlen(value);
          EXPECT_EQ(OB_SUCCESS, obj.set_decimal(value, length, width, precision));
          obj.get_decimal_integer(integer);
          obj.get_decimal_fractional(fractional);
          obj.get_decimal_sign(sign);
          EXPECT_EQ(98663, integer);
          EXPECT_EQ(230, fractional);
          EXPECT_EQ(false, sign);

        }
        //无溢出
        {
          ObObj obj;
          char value[16] = "+63.439";
          length = strlen(value);
          EXPECT_EQ(OB_SUCCESS, obj.set_decimal(value, length, width, precision));
          obj.get_decimal_integer(integer);
          obj.get_decimal_fractional(fractional);
          obj.get_decimal_sign(sign);
          EXPECT_EQ(63, integer);
          EXPECT_EQ(439, fractional);
          EXPECT_EQ(true, sign);
        }
        //小数位数不足精度

        {
          ObObj obj;
          char value[16] = "+63.43";
          length = strlen(value);
          EXPECT_EQ(OB_SUCCESS, obj.set_decimal(value, length, width, precision));
          obj.get_decimal_integer(integer);
          obj.get_decimal_fractional(fractional);
          obj.get_decimal_sign(sign);
          EXPECT_EQ(63, integer);
          EXPECT_EQ(430, fractional);
          EXPECT_EQ(true, sign);
        }
        {
          ObObj obj;
          char value[16] = "+0.43";
          length = strlen(value);
          EXPECT_EQ(OB_SUCCESS, obj.set_decimal(value, length, width, precision));
          obj.get_decimal_integer(integer);
          obj.get_decimal_fractional(fractional);
          obj.get_decimal_sign(sign);
          EXPECT_EQ(0, integer);
          EXPECT_EQ(430, fractional);
          EXPECT_EQ(true, sign);
        }

        //整数以0开头
        {
          ObObj obj;
          char value[16] = "+063.009";
          length = strlen(value);
          EXPECT_EQ(OB_SUCCESS, obj.set_decimal(value, length, width, precision));
          obj.get_decimal_integer(integer);
          obj.get_decimal_fractional(fractional);
          obj.get_decimal_sign(sign);
          EXPECT_EQ(63, integer);
          EXPECT_EQ(9, fractional);
          EXPECT_EQ(true, sign);
        }
        //整数以0开头，小数也以0开头
        {
          ObObj obj;
          char value[16] = "+0.009";
          length = strlen(value);
          int64_t p = 2;
          EXPECT_EQ(OB_SUCCESS, obj.set_decimal(value, length, width, p));
          obj.get_decimal_integer(integer);
          obj.get_decimal_fractional(fractional);
          obj.get_decimal_sign(sign);
          EXPECT_EQ(0, integer);
          EXPECT_EQ(1, fractional);
          EXPECT_EQ(true, sign);
        }
        {
          ObObj obj;
          char value[16] = "+0.017";
          length = strlen(value);
          int64_t p = 2;
          EXPECT_EQ(OB_SUCCESS, obj.set_decimal(value, length, width, p));
          obj.get_decimal_integer(integer);
          obj.get_decimal_fractional(fractional);
          obj.get_decimal_sign(sign);
          EXPECT_EQ(0, integer);
          EXPECT_EQ(2, fractional);
          EXPECT_EQ(true, sign);
        }
        //小数以0开头

        {
          ObObj obj;
          char value[16] = "+63.009";
          length = strlen(value);
          EXPECT_EQ(OB_SUCCESS, obj.set_decimal(value, length, width, precision));
          obj.get_decimal_integer(integer);
          obj.get_decimal_fractional(fractional);
          obj.get_decimal_sign(sign);
          EXPECT_EQ(63, integer);
          EXPECT_EQ(9, fractional);
          EXPECT_EQ(true, sign);
        }

        //小数以0开头，很多0
        {
          ObObj obj;
          char value[16] = "+63.00009";
          length = strlen(value);
          EXPECT_EQ(OB_SUCCESS, obj.set_decimal(value, length, width, precision));
          obj.get_decimal_integer(integer);
          obj.get_decimal_fractional(fractional);
          obj.get_decimal_sign(sign);
          EXPECT_EQ(63, integer);
          EXPECT_EQ(0, fractional);
          EXPECT_EQ(true, sign);
        }
        //小数以0开头，部分0
        {
          ObObj obj;
          char value[16] = "+63.0009";
          length = strlen(value);
          EXPECT_EQ(OB_SUCCESS, obj.set_decimal(value, length, width, precision));
          obj.get_decimal_integer(integer);
          obj.get_decimal_fractional(fractional);
          obj.get_decimal_sign(sign);
          EXPECT_EQ(63, integer);
          EXPECT_EQ(1, fractional);
          EXPECT_EQ(true, sign);
        }

        //小数部分带进位
        {
          ObObj obj;
          char value[16] = "+63.9999";
          length = strlen(value);
          EXPECT_EQ(OB_SUCCESS, obj.set_decimal(value, length, width, precision));
          obj.get_decimal_integer(integer);
          obj.get_decimal_fractional(fractional);
          obj.get_decimal_sign(sign);
          EXPECT_EQ(64, integer);
          EXPECT_EQ(0, fractional);
          EXPECT_EQ(true, sign);
        }

        //进位后导致整数位溢出
        {
          ObObj obj;
          char value[16] = "+99.9999";
          length = strlen(value);
          EXPECT_EQ(OB_SUCCESS, obj.set_decimal(value, length, width, precision));
          obj.get_decimal_integer(integer);
          obj.get_decimal_fractional(fractional);
          obj.get_decimal_sign(sign);
          EXPECT_EQ(100, integer);
          EXPECT_EQ(0, fractional);
          EXPECT_EQ(true, sign);
        }
        //小数位有进位，高位有溢出
        {
          ObObj obj;
          char value[16] = "+363.9999";
          length = strlen(value);
          EXPECT_EQ(OB_SUCCESS, obj.set_decimal(value, length, width, precision));
          obj.get_decimal_integer(integer);
          obj.get_decimal_fractional(fractional);
          obj.get_decimal_sign(sign);
          EXPECT_EQ(364, integer);
          EXPECT_EQ(0, fractional);
          EXPECT_EQ(true, sign);
        }
        //验证float

        {
          float data = 1.4;
          char value[16];
          sprintf(value, "%f", data);
          length = 16;
          ObObj obj;

          EXPECT_EQ(OB_SUCCESS, obj.set_decimal(value, strlen(value), width, precision));
          obj.get_decimal_integer(integer);
          obj.get_decimal_fractional(fractional);
          obj.get_decimal_sign(sign);
          EXPECT_EQ(1, integer);
          EXPECT_EQ(400, fractional);
          EXPECT_EQ(true, sign);
        }
      }
      // int ObObj::set_decimal(const float value, const int64_t width,
      //        const int64_t precision, const bool is_add)
      TEST_F(TestObjDecimal, set_decimal_float)
      {
        int64_t width = 0;
        int64_t precision = 0;
        bool is_add = true;
        float value = 0.4;
        int64_t integer = 0;
        int64_t fractional = 0;
        //输入参数不正确
        {
          width = -1;
          precision = 1;
          ObObj obj;
          EXPECT_EQ(OB_INVALID_ARGUMENT, obj.set_decimal(value, width, precision, is_add));
        }
        {
          width = 2;
          precision = -1;
          ObObj obj;
          EXPECT_EQ(OB_INVALID_ARGUMENT, obj.set_decimal(value, width, precision, is_add));
        }
        {
          width = 2;
          precision = 3;
          ObObj obj;
          EXPECT_EQ(OB_INVALID_ARGUMENT, obj.set_decimal(value, width, precision, is_add));
        }

        //参数正确,但是整数的宽度为0，导致整数部分溢出，最大值为0.0
        {
          width = 3;
          precision = 3;
          value = 1.4;
          ObObj obj;
          EXPECT_EQ(OB_INVALID_ARGUMENT, obj.set_decimal(value, width, precision, is_add));
        }
        //正常情况
        {
          width = 4;
          precision = 3;
          value = 1.4;
          ObObj obj;
          EXPECT_EQ(0, obj.set_decimal(value, width, precision, is_add));
          obj.get_decimal_integer(integer);
          EXPECT_EQ(1, integer);
          obj.get_decimal_fractional(fractional);
          EXPECT_EQ(400, fractional);
        }
        //很长的浮点数
        {
          width = 15;
          precision = 8;
          value = 1.1166611111111111111;
          ObObj obj;
          EXPECT_EQ(0, obj.set_decimal(value, width, precision, is_add));
          obj.get_decimal_integer(integer);
          printf("integer = %ld", integer);
          obj.get_decimal_fractional(fractional);
          printf("fractional = %ld", fractional);
        }
      }

      TEST_F(TestObjDecimal, set_decimal_double)
      {
        int64_t width = 0;
        int64_t precision = 0;
        bool is_add = true;
        double value = 0.4;
        int64_t integer = 0;
        int64_t fractional = 0;
        //输入参数不正确
        {
          width = -1;
          precision = 1;
          ObObj obj;
          EXPECT_EQ(OB_INVALID_ARGUMENT, obj.set_decimal(value, width, precision, is_add));
        }
        {
          width = 2;
          precision = -1;
          ObObj obj;
          EXPECT_EQ(OB_INVALID_ARGUMENT, obj.set_decimal(value, width, precision, is_add));
        }
        {
          width = 2;
          precision = 3;
          ObObj obj;
          EXPECT_EQ(OB_INVALID_ARGUMENT, obj.set_decimal(value, width, precision, is_add));
        }

        //参数正确,但是整数的宽度为0，导致整数部分溢出，最大值为0.0
        {
          width = 3;
          precision = 3;
          value = 1.4;
          ObObj obj;
          EXPECT_EQ(OB_INVALID_ARGUMENT, obj.set_decimal(value, width, precision, is_add));
        }
        //正常情况
        {
          width = 4;
          precision = 3;
          value = 1.4;
          ObObj obj;
          EXPECT_EQ(0, obj.set_decimal(value, width, precision, is_add));
          obj.get_decimal_integer(integer);
          EXPECT_EQ(1, integer);
          obj.get_decimal_fractional(fractional);
          EXPECT_EQ(400, fractional);
        }
        //很长的浮点数
        {
          width = 19;
          precision = 9;
          value = 1.1166611111111111111;
          ObObj obj;
          EXPECT_EQ(0, obj.set_decimal(value, width, precision, is_add));
          obj.get_decimal_integer(integer);
          printf("integer = %ld", integer);
          obj.get_decimal_fractional(fractional);
          printf("fractional = %ld", fractional);
        }
      }

      //int ObObj::decimal_apply_plus(ObObj& one, const ObObj other, const bool is_add)
      //实现两个定点数的加法，不进行参数的判断
      TEST_F(TestObjDecimal, plus)
      {
        char a1[16] = "1.4";
        char b1[16] = "1.5";
        int64_t width = 5;
        int64_t precision = 3;
        int64_t precision_1 = 4;
        bool is_add = true;
        int64_t integer = 0;
        int64_t fractional = 0;
        ObDecimalHelper::meta_union meta;
        bool sign = false;
        //两个不同精度的obj
        {
          ObjTest a;
          EXPECT_EQ(0, a.set_decimal(a1, strlen(a1), width, precision, is_add));
          ObObj b;
          EXPECT_EQ(0, b.set_decimal(a1, strlen(b1), width, precision_1, is_add));

          EXPECT_EQ(OB_DECIMAL_PRECISION_NOT_EQUAL, a.plus(b, is_add));
        }
        //精度相同，符号相同的定点数加法,无溢出，无进位
        {
          ObjTest a;
          EXPECT_EQ(0, a.set_decimal(a1, strlen(a1),  width, precision, is_add));
          ObObj b;
          EXPECT_EQ(0, b.set_decimal(b1, strlen(b1), width, precision, is_add));
          EXPECT_EQ(0, a.plus(b, true));
          a.get_decimal_integer(integer);
          EXPECT_EQ(2, integer);
          a.get_decimal_fractional(fractional);
          EXPECT_EQ(900, fractional);
          a.get_decimal_sign(sign);
          EXPECT_EQ(true, sign);
        }
        //符号位相同的定点数加入，整数部分有溢出，无进位
        {
          char a1[16] = "68.23";
          char b1[16] = "42.12";
          ObjTest a;
          EXPECT_EQ(0, a.set_decimal(a1, strlen(a1),  width, precision, is_add));
          ObObj b;
          EXPECT_EQ(0, b.set_decimal(b1, strlen(b1), width, precision, is_add));
          EXPECT_EQ(0, a.plus(b, true));
          a.get_decimal(integer, fractional, meta, is_add);
          EXPECT_EQ(99, integer);
          EXPECT_EQ(999, fractional);
           a.get_decimal_sign(sign);
          EXPECT_EQ(true, sign);
          EXPECT_EQ(true, is_add);
        }
        //符号位相同的定点数加法，整数部分无溢出，小数部分有进位
        {
          char a1[16] = "18.839";
          char b1[16] = "42.624";
          ObjTest a;
          EXPECT_EQ(0, a.set_decimal(a1, strlen(a1), width, precision, is_add));
          a.get_decimal(integer, fractional, meta, is_add);
          TBSYS_LOG(INFO, "integer = %d, fractional = %d", integer, fractional);
          ObObj b;
          EXPECT_EQ(0, b.set_decimal(b1, strlen(b1), width, precision, is_add));
          b.get_decimal(integer, fractional, meta, is_add);
          TBSYS_LOG(INFO, "integer = %d, fractional = %d", integer, fractional);
          EXPECT_EQ(0, a.plus(b, true));
          a.get_decimal(integer, fractional, meta, is_add);
          EXPECT_EQ(61, integer);
          EXPECT_EQ(463, fractional);
           a.get_decimal_sign(sign);
          EXPECT_EQ(true, sign);
          EXPECT_EQ(true, is_add);
        }
        //更新值上的加法
        {
          char a1[16] = "18.839";
          char b1[16] = "42.624";
          ObjTest a;
          EXPECT_EQ(0, a.set_decimal(a1, strlen(a1),  width, precision, false));
          ObObj b;
          EXPECT_EQ(0, b.set_decimal(b1, strlen(b1), width, precision, true));
          EXPECT_EQ(0, a.plus(b, true));
          a.get_decimal(integer, fractional, meta, is_add);
          EXPECT_EQ(61, integer);
          EXPECT_EQ(463, fractional);
           a.get_decimal_sign(sign);
          EXPECT_EQ(true, sign);
          EXPECT_EQ(true, is_add);
        }
        //相同符号位的加法，小数有进位，导致整数有溢出
        {
          char a1[16] = "57.839";
          char b1[16] = "42.624";
          ObjTest a;
          EXPECT_EQ(0, a.set_decimal(a1, strlen(a1),  width, precision, true));
          ObObj b;
          EXPECT_EQ(0, b.set_decimal(b1, strlen(b1), width, precision, true));
          EXPECT_EQ(0, a.plus(b, true));
          a.get_decimal(integer, fractional, meta, is_add);
          EXPECT_EQ(99, integer);
          EXPECT_EQ(999, fractional);
           a.get_decimal_sign(sign);
          EXPECT_EQ(true, sign);
          EXPECT_EQ(true, is_add);
        }
        //=======================
        //以下的相同符号位为负号
        //=======================
        //精度相同，符号相同的定点数加法,无溢出，无进位
        {
          char a1[16] = "-1.4";
          char b1[16] = "-1.5";
          ObjTest a;
          EXPECT_EQ(0, a.set_decimal(a1, strlen(a1),  width, precision, is_add));
          ObObj b;
          EXPECT_EQ(0, b.set_decimal(b1, strlen(b1), width, precision, is_add));
          EXPECT_EQ(0, a.plus(b, true));
          a.get_decimal_integer(integer);
          EXPECT_EQ(2, integer);
          a.get_decimal_fractional(fractional);
          EXPECT_EQ(900, fractional);
          a.get_decimal_sign(sign);
          EXPECT_EQ(false, sign);
        }
        //符号位相同的定点数加入，整数部分有溢出，无进位
        {
          char a1[16] = "-68.23";
          char b1[16] = "-42.12";
          ObjTest a;
          EXPECT_EQ(0, a.set_decimal(a1, strlen(a1),  width, precision, is_add));
          ObObj b;
          EXPECT_EQ(0, b.set_decimal(b1, strlen(b1), width, precision, is_add));
          EXPECT_EQ(0, a.plus(b, true));
          a.get_decimal(integer, fractional, meta, is_add);
          EXPECT_EQ(99, integer);
          EXPECT_EQ(999, fractional);
           a.get_decimal_sign(sign);
          EXPECT_EQ(false, sign);
          EXPECT_EQ(true, is_add);
        }
        //符号位相同的定点数加法，整数部分无溢出，小数部分有进位
        {
          char a1[16] = "-18.839";
          char b1[16] = "-42.624";
          ObjTest a;
          EXPECT_EQ(0, a.set_decimal(a1, strlen(a1), width, precision, is_add));
          ObObj b;
          EXPECT_EQ(0, b.set_decimal(b1, strlen(b1), width, precision, is_add));
          EXPECT_EQ(0, a.plus(b, true));
          a.get_decimal(integer, fractional, meta, is_add);
          EXPECT_EQ(61, integer);
          EXPECT_EQ(463, fractional);
           a.get_decimal_sign(sign);
          EXPECT_EQ(false, sign);
          EXPECT_EQ(true, is_add);
        }
        //更新值上的加法
        {
          char a1[16] = "-18.839";
          char b1[16] = "-42.624";
          ObjTest a;
          EXPECT_EQ(0, a.set_decimal(a1, strlen(a1),  width, precision, false));
          ObObj b;
          EXPECT_EQ(0, b.set_decimal(b1, strlen(b1), width, precision, true));
          EXPECT_EQ(0, a.plus(b, false));
          a.get_decimal(integer, fractional, meta, is_add);
          EXPECT_EQ(61, integer);
          EXPECT_EQ(463, fractional);
           a.get_decimal_sign(sign);
          EXPECT_EQ(false, sign);
          EXPECT_EQ(false, is_add);
        }
        //相同符号位的加法，小数有进位，导致整数有溢出
        {
          char a1[16] = "-57.839";
          char b1[16] = "-42.624";
          ObjTest a;
          EXPECT_EQ(0, a.set_decimal(a1, strlen(a1),  width, precision, true));
          ObObj b;
          EXPECT_EQ(0, b.set_decimal(b1, strlen(b1), width, precision, true));
          EXPECT_EQ(0, a.plus(b, true));
          a.get_decimal(integer, fractional, meta, is_add);
          EXPECT_EQ(99, integer);
          EXPECT_EQ(999, fractional);
           a.get_decimal_sign(sign);
          EXPECT_EQ(false, sign);
          EXPECT_EQ(true, is_add);
        }

        //==========================
        //以下为不同符号位定点数的相加
        //==========================

        //==========================
        //正数大于负数的加法
        //==========================

        //整数部分和小数部分均大于
        {
          char a1[16] = "57.839";
          char b1[16] = "-42.624";
          ObjTest a;
          EXPECT_EQ(0, a.set_decimal(a1, strlen(a1),  width, precision, true));
          ObObj b;
          EXPECT_EQ(0, b.set_decimal(b1, strlen(b1), width, precision, true));
          EXPECT_EQ(0, a.plus(b, true));
          a.get_decimal(integer, fractional, meta, is_add);
          EXPECT_EQ(15, integer);
          EXPECT_EQ(215, fractional);
           a.get_decimal_sign(sign);
          EXPECT_EQ(true, sign);
          EXPECT_EQ(true, is_add);
        }
        //整数大于，小数等于
        {
          char a1[16] = "57.839";
          char b1[16] = "-42.839";
          ObjTest a;
          EXPECT_EQ(0, a.set_decimal(a1, strlen(a1),  width, precision, true));
          ObObj b;
          EXPECT_EQ(0, b.set_decimal(b1, strlen(b1), width, precision, true));
          EXPECT_EQ(0, a.plus(b, true));
          a.get_decimal(integer, fractional, meta, is_add);
          EXPECT_EQ(15, integer);
          EXPECT_EQ(0, fractional);
           a.get_decimal_sign(sign);
          EXPECT_EQ(true, sign);
          EXPECT_EQ(true, is_add);
        }
        //整数大于，小数小于
        {
          char a1[16] = "57.439";
          char b1[16] = "-42.624";
          ObjTest a;
          EXPECT_EQ(0, a.set_decimal(a1, strlen(a1),  width, precision, true));
          ObObj b;
          EXPECT_EQ(0, b.set_decimal(b1, strlen(b1), width, precision, true));
          EXPECT_EQ(0, a.plus(b, true));
          a.get_decimal(integer, fractional, meta, is_add);
          EXPECT_EQ(14, integer);
           a.get_decimal_sign(sign);
          EXPECT_EQ(815, fractional);
          EXPECT_EQ(true, sign);
          EXPECT_EQ(true, is_add);
        }

        //==========================
        //负数大于正数的加法
        //==========================

        //负数的整数部分和小数部分均大于
        {
          char a1[16] = "-57.839";
          char b1[16] = "42.624";
          ObjTest a;
          EXPECT_EQ(0, a.set_decimal(a1, strlen(a1),  width, precision, true));
          ObObj b;
          EXPECT_EQ(0, b.set_decimal(b1, strlen(b1), width, precision, true));
          EXPECT_EQ(0, a.plus(b, true));
          a.get_decimal(integer, fractional, meta, is_add);
          EXPECT_EQ(15, integer);
          EXPECT_EQ(215, fractional);
           a.get_decimal_sign(sign);
          EXPECT_EQ(false, sign);
          EXPECT_EQ(true, is_add);
        }
        //负数的整数大于，小数等于
        {
          char a1[16] = "-57.839";
          char b1[16] = "42.839";
          ObjTest a;
          EXPECT_EQ(0, a.set_decimal(a1, strlen(a1),  width, precision, true));
          ObObj b;
          EXPECT_EQ(0, b.set_decimal(b1, strlen(b1), width, precision, true));
          EXPECT_EQ(0, a.plus(b, true));
          a.get_decimal(integer, fractional, meta, is_add);
          EXPECT_EQ(15, integer);
          EXPECT_EQ(0, fractional);
           a.get_decimal_sign(sign);
          EXPECT_EQ(false, sign);
          EXPECT_EQ(true, is_add);
        }
        //负数的整数大于，小数小于
        {
          char a1[16] = "-57.439";
          char b1[16] = "42.624";
          ObjTest a;
          EXPECT_EQ(0, a.set_decimal(a1, strlen(a1),  width, precision, true));
          ObObj b;
          EXPECT_EQ(0, b.set_decimal(b1, strlen(b1), width, precision, true));
          EXPECT_EQ(0, a.plus(b, true));
          a.get_decimal(integer, fractional, meta, is_add);
          EXPECT_EQ(14, integer);
          EXPECT_EQ(815, fractional);
           a.get_decimal_sign(sign);
          EXPECT_EQ(false, sign);
          EXPECT_EQ(true, is_add);
        }
      }

      //int ObObj::do_decimal_mul(const ObObj &multiplicand, ObObj &product)
      TEST_F(TestObjDecimal, do_decimal_mul)
      {
        ObObj a;
        ObObj b;
        ObObj c;
        int64_t integer = 12;
        int64_t fractional = 5;
        int64_t precision = 3;
        int64_t width = 5;
        bool sign = true;
        //乘数不是ObDecimalType
        {
          EXPECT_EQ(OB_INVALID_ARGUMENT, a.do_decimal_mul(b, c));
        }
        //被乘数不是可以运算的类型
        {
          ObObj d;
          EXPECT_EQ(0, a.set_decimal(integer, fractional, width, precision, sign));
          d.set_seq();
          EXPECT_EQ(OB_INVALID_ARGUMENT, a.do_decimal_mul(d, c));
        }
        // 被乘数不是整形
        {
          ObObj d;
          float val = 5.3;
          d.set_float(val);
          EXPECT_EQ(OB_OBJ_TYPE_ERROR, a.do_decimal_mul(d, c));
        }
        ObDecimalHelper::meta_union meta;
        //被乘数为空类型
        {
          EXPECT_EQ(OB_INVALID_ARGUMENT, a.do_decimal_mul(b, c));
          /*EXPECT_EQ(ObDecimalType, c.get_type());
          EXPECT_EQ(0, c.get_decimal(integer, fractional, meta));
          EXPECT_EQ(0, integer);
          EXPECT_EQ(0, fractional);
          EXPECT_EQ(0, meta.value_.sign_);
          EXPECT_EQ(3, meta.value_.precision_);
          EXPECT_EQ(5, meta.value_.width_);
        */}

          //被乘数大于INT32_MAX
          {
            int64_t int_val= 5;
            int64_t fra_val = 5;
            int64_t pre = 1;
            int64_t val = 4294967299;;
            EXPECT_EQ(0, a.set_decimal(int_val, fra_val, width, pre));
            b.set_int(val);
            EXPECT_EQ(OB_INVALID_ARGUMENT, a.do_decimal_mul(b, c));
            val = INT32_MAX;
            b.set_int(val);
            EXPECT_EQ(0, a.do_decimal_mul(b, c));
          }
        //被乘数为正常数 5.5 * 25
        {
          int64_t int_val= 5;
          int64_t fra_val = 5;
          int64_t pre = 1;
          int64_t val = 25;
          EXPECT_EQ(0, a.set_decimal(int_val, fra_val, width, pre));
          b.set_int(val);
          EXPECT_EQ(0, a.do_decimal_mul(b, c));
          EXPECT_EQ(0, c.get_decimal(integer, fractional, meta));
          EXPECT_EQ(137, integer);
          EXPECT_EQ(5, fractional);
          EXPECT_EQ(0, meta.value_.sign_);
          EXPECT_EQ(1, meta.value_.precision_);
          EXPECT_EQ(5, meta.value_.width_);
        }
        //被乘数为0
        {
          int64_t int_val= 5;
          int64_t fra_val = 5;
          int64_t pre = 1;
          int64_t val = 0;
          EXPECT_EQ(0, a.set_decimal(int_val, fra_val, width, pre));
          b.set_int(val);
          EXPECT_EQ(0, a.do_decimal_mul(b, c));
          EXPECT_EQ(0, c.get_decimal(integer, fractional, meta));
          EXPECT_EQ(0, integer);
          EXPECT_EQ(0, fractional);
          EXPECT_EQ(0, meta.value_.sign_);
          EXPECT_EQ(1, meta.value_.precision_);
          EXPECT_EQ(5, meta.value_.width_);
        }
        //整数部分为0 0.5 * 25 = 12.5
        {
          int64_t int_val= 0;
          int64_t fra_val = 5;
          int64_t pre = 1;
          int64_t val = 25;
          EXPECT_EQ(0, a.set_decimal(int_val, fra_val, width, pre));
          b.set_int(val);
          EXPECT_EQ(0, a.do_decimal_mul(b, c));
          EXPECT_EQ(0, c.get_decimal(integer, fractional, meta));
          EXPECT_EQ(12, integer);
          EXPECT_EQ(5, fractional);
          EXPECT_EQ(0, meta.value_.sign_);
          EXPECT_EQ(1, meta.value_.precision_);
          EXPECT_EQ(5, meta.value_.width_);
        }
        //小数部分为0 5.0 * 25 = 125.0
        {
          int64_t int_val= 5;
          int64_t fra_val = 0;
          int64_t pre = 1;
          int64_t val = 25;
          EXPECT_EQ(0, a.set_decimal(int_val, fra_val, width, pre));
          b.set_int(val);
          EXPECT_EQ(0, a.do_decimal_mul(b, c));
          EXPECT_EQ(0, c.get_decimal(integer, fractional, meta));
          EXPECT_EQ(125, integer);
          EXPECT_EQ(0, fractional);
          EXPECT_EQ(0, meta.value_.sign_);
          EXPECT_EQ(1, meta.value_.precision_);
          EXPECT_EQ(5, meta.value_.width_);
        }
        //小数点后面有0 5.05 * 25 = 126.25
        {
          int64_t int_val= 5;
          int64_t fra_val = 5;
          int64_t pre = 2;
          int64_t val = 25;
          EXPECT_EQ(0, a.set_decimal(int_val, fra_val, width, pre));
          b.set_int(val);
          EXPECT_EQ(0, a.do_decimal_mul(b, c));
          EXPECT_EQ(0, c.get_decimal(integer, fractional, meta));
          EXPECT_EQ(126, integer);
          EXPECT_EQ(25, fractional);
          EXPECT_EQ(0, meta.value_.sign_);
          EXPECT_EQ(2, meta.value_.precision_);
          EXPECT_EQ(5, meta.value_.width_);
        }

        //小数部分为0， 精度不为1 5.000 * 25 = 125.000
        {
          int64_t int_val= 5;
          int64_t fra_val = 0;
          int64_t pre = 3;
          int64_t val = 25;
          EXPECT_EQ(0, a.set_decimal(int_val, fra_val, width, pre));
          b.set_int(val);
          EXPECT_EQ(0, a.do_decimal_mul(b, c));
          EXPECT_EQ(0, c.get_decimal(integer, fractional, meta));
          EXPECT_EQ(99, integer);
          EXPECT_EQ(999, fractional);
          EXPECT_EQ(0, meta.value_.sign_);
          EXPECT_EQ(3, meta.value_.precision_);
          EXPECT_EQ(5, meta.value_.width_);
        }

        //整数为0，小数点后也为0  0.05 * 1 = 0.05
        {
          int64_t int_val= 0;
          int64_t fra_val = 5;
          int64_t pre = 2;
          int64_t val = 1;
          EXPECT_EQ(0, a.set_decimal(int_val, fra_val, width, pre));
          b.set_int(val);
          EXPECT_EQ(0, a.do_decimal_mul(b, c));
          EXPECT_EQ(0, c.get_decimal(integer, fractional, meta));
          EXPECT_EQ(0, integer);
          EXPECT_EQ(5, fractional);
          EXPECT_EQ(0, meta.value_.sign_);
          EXPECT_EQ(2, meta.value_.precision_);
          EXPECT_EQ(5, meta.value_.width_);
        }

        //同上  0.05 * 125 = 6.25
        {
          int64_t int_val= 0;
          int64_t fra_val = 5;
          int64_t pre = 2;
          int64_t val = 125;
          EXPECT_EQ(0, a.set_decimal(int_val, fra_val, width, pre));
          b.set_int(val);
          EXPECT_EQ(0, a.do_decimal_mul(b, c));
          EXPECT_EQ(0, c.get_decimal(integer, fractional, meta));
          EXPECT_EQ(6, integer);
          EXPECT_EQ(25, fractional);
          EXPECT_EQ(0, meta.value_.sign_);
          EXPECT_EQ(2, meta.value_.precision_);
          EXPECT_EQ(5, meta.value_.width_);
        }

        //乘法以后，整数溢出 25.58 * 25 = 639,50
        {
          int64_t int_val= 25;
          int64_t fra_val = 58;
          int64_t pre = 2;
          int64_t wid = 4;
          int64_t val = 25;
          EXPECT_EQ(0, a.set_decimal(int_val, fra_val, wid, pre));
          b.set_int(val);
          EXPECT_EQ(0, a.do_decimal_mul(b, c));
          EXPECT_EQ(0, c.get_decimal(integer, fractional, meta));
          EXPECT_EQ(99, integer);
          EXPECT_EQ(99, fractional);
          EXPECT_EQ(0, meta.value_.sign_);
          EXPECT_EQ(2, meta.value_.precision_);
          EXPECT_EQ(4, meta.value_.width_);
        }

        //=========================
        //整数为负数
        //=========================
        //乘法以后，整数溢出 25.58 * -25 = 639,50
        {
          int64_t int_val= 25;
          int64_t fra_val = 58;
          int64_t pre = 2;
          int64_t wid = 5;
          int64_t val = -25;
          EXPECT_EQ(0, a.set_decimal(int_val, fra_val, wid, pre));
          b.set_int(val);
          EXPECT_EQ(0, a.do_decimal_mul(b, c));
          EXPECT_EQ(0, c.get_decimal(integer, fractional, meta));
          EXPECT_EQ(639, integer);
          EXPECT_EQ(50, fractional);
          EXPECT_EQ(false, meta.value_.sign_ == 0 ? true : false);
          EXPECT_EQ(2, meta.value_.precision_);
          EXPECT_EQ(5, meta.value_.width_);
        }

        //====================
        //都为负数
        //====================
        //乘法以后，整数溢出 -25.58 * -25 = 639,50
        {
          int64_t int_val= 25;
          int64_t fra_val = 58;
          int64_t pre = 2;
          int64_t wid = 5;
          int64_t val = -25;
          bool sign = false;
          ObDecimalHelper::meta_union m;
          EXPECT_EQ(0, a.set_decimal(int_val, fra_val, wid, pre, sign));
          b.set_int(val);
          EXPECT_EQ(0, a.do_decimal_mul(b, c));
          EXPECT_EQ(0, c.get_decimal(integer, fractional, m));
          EXPECT_EQ(639, integer);
          EXPECT_EQ(50, fractional);
          EXPECT_EQ(true, m.value_.sign_ == 0 ? true: false);
          EXPECT_EQ(2, m.value_.precision_);
          EXPECT_EQ(5, m.value_.width_);
        }
      }

	 // int ObObj::do_decimal_div(const ObObj &dividend, ObObj consult)
      TEST_F(TestObjDecimal, do_decimal_div)
      {
        //===============
        //输入类型不匹配
        //===============

        //org_type不是定点数型
        {
          ObObj one;
          ObObj other;
          ObObj consult;
          int64_t val = 64;
          other.set_int(val);
          EXPECT_EQ(OB_INVALID_ARGUMENT, one.do_decimal_div(other, consult));
        }
        //other的类型不对
        {
          ObObj one;
          ObObj other;
          ObObj consult;
          int64_t integer = 64;
          int64_t fractional = 34;
          int64_t width = 6;
          int64_t precision = 4;
          EXPECT_EQ(0, one.set_decimal(integer, fractional, width, precision));
          other.set_seq();
          EXPECT_EQ(OB_INVALID_ARGUMENT, one.do_decimal_div(other, consult));
        }
        //other为空类型
        {
          ObObj one;
          ObObj other;
          ObObj consult;
          int64_t integer = 64;
          int64_t fractional = 34;
          int64_t width = 6;
          int64_t precision = 4;
          EXPECT_EQ(0, one.set_decimal(integer, fractional, width, precision));
          other.set_seq();
          EXPECT_EQ(OB_INVALID_ARGUMENT, one.do_decimal_div(other, consult));
        }
        //类型正确
        {
          ObObj one;
          ObObj other;
          ObObj consult;
          int64_t integer = 64;
          int64_t fractional = 34;
          int64_t width = 6;
          int64_t precision = 4;
          EXPECT_EQ(0, one.set_decimal(integer, fractional, width, precision));
          other.set_int(integer);
          EXPECT_EQ(0, one.do_decimal_div(other, consult));
        }

        //=====================
        //正常计算 先正数
        //=====================

        //整数部分为0 0.43 / 24 = 0.017
        {
          ObObj one;
          ObObj other;
          ObObj consult;
          int64_t integer = 0;
          int64_t fractional = 43;
          int64_t width = 6;
          int64_t precision = 2;
          EXPECT_EQ(0, one.set_decimal(integer, fractional, width, precision));
          int64_t val = 24;
          other.set_int(val);
          EXPECT_EQ(0, one.do_decimal_div(other, consult));
          ObDecimalHelper::meta_union meta;
          EXPECT_EQ(0, consult.get_decimal(integer, fractional, meta));
          EXPECT_EQ(0, integer);
          EXPECT_EQ(2, fractional);
          EXPECT_EQ(width, meta.value_.width_);
          EXPECT_EQ(precision, meta.value_.precision_);
          EXPECT_EQ(true, (meta.value_.sign_ == 0 ? true : false));
        }
        //需要后移一位 0.45 / 67 =  0.006
        {
          ObObj one;
          ObObj other;
          ObObj consult;
          int64_t integer = 0;
          int64_t fractional = 45;
          int64_t width = 6;
          int64_t precision = 2;
          EXPECT_EQ(0, one.set_decimal(integer, fractional, width, precision));
          int64_t val = 67;
          other.set_int(val);
          EXPECT_EQ(0, one.do_decimal_div(other, consult));
          ObDecimalHelper::meta_union meta;
          EXPECT_EQ(0, consult.get_decimal(integer, fractional, meta));
          EXPECT_EQ(0, integer);
          EXPECT_EQ(1, fractional);
          EXPECT_EQ(width, meta.value_.width_);
          EXPECT_EQ(precision, meta.value_.precision_);
          EXPECT_EQ(true, (meta.value_.sign_ == 0 ? true : false));
        }
        //小数部分为0  234 / 12 = 19.5
        {
          ObObj one;
          ObObj other;
          ObObj consult;
          int64_t integer = 234;
          int64_t fractional = 0;
          int64_t width = 6;
          int64_t precision = 2;
          EXPECT_EQ(0, one.set_decimal(integer, fractional, width, precision));
          int64_t val = 12;
          other.set_int(val);
          EXPECT_EQ(0, one.do_decimal_div(other, consult));
          ObDecimalHelper::meta_union meta;
          EXPECT_EQ(0, consult.get_decimal(integer, fractional, meta));
          EXPECT_EQ(19, integer);
          EXPECT_EQ(50, fractional);
          EXPECT_EQ(width, meta.value_.width_);
          EXPECT_EQ(precision, meta.value_.precision_);
          EXPECT_EQ(true, (meta.value_.sign_ == 0 ? true : false));
        }
        //需要后移一位  234 / 54 = 4.333
        {
          ObObj one;
          ObObj other;
          ObObj consult;
          int64_t integer = 234;
          int64_t fractional = 0;
          int64_t width = 6;
          int64_t precision = 2;
          EXPECT_EQ(0, one.set_decimal(integer, fractional, width, precision));
          int64_t val = 54;
          other.set_int(val);
          EXPECT_EQ(0, one.do_decimal_div(other, consult));
          ObDecimalHelper::meta_union meta;
          EXPECT_EQ(0, consult.get_decimal(integer, fractional, meta));
          EXPECT_EQ(4, integer);
          EXPECT_EQ(33, fractional);
          EXPECT_EQ(width, meta.value_.width_);
          EXPECT_EQ(precision, meta.value_.precision_);
          EXPECT_EQ(true, (meta.value_.sign_ == 0 ? true : false));
        }
        //小数四舍五入后有向高位的进位  456.89 / 14 = 32.635
        {
          ObObj one;
          ObObj other;
          ObObj consult;
          int64_t integer = 456;
          int64_t fractional = 89;
          int64_t width = 6;
          int64_t precision = 2;
          EXPECT_EQ(0, one.set_decimal(integer, fractional, width, precision));
          int64_t val = 14;
          other.set_int(val);
          EXPECT_EQ(0, one.do_decimal_div(other, consult));
          ObDecimalHelper::meta_union meta;
          EXPECT_EQ(0, consult.get_decimal(integer, fractional, meta));
          EXPECT_EQ(32, integer);
          EXPECT_EQ(64, fractional);
          EXPECT_EQ(width, meta.value_.width_);
          EXPECT_EQ(precision, meta.value_.precision_);
          EXPECT_EQ(true, (meta.value_.sign_ == 0 ? true : false));
        }
        //小数有向整数的进位   3573.1 / 65 = 54.97
        {
          ObObj one;
          ObObj other;
          ObObj consult;
          int64_t integer = 3573;
          int64_t fractional = 1;
          int64_t width = 6;
          int64_t precision = 1;
          EXPECT_EQ(0, one.set_decimal(integer, fractional, width, precision));
          int64_t val = 65;
          other.set_int(val);
          EXPECT_EQ(0, one.do_decimal_div(other, consult));
          ObDecimalHelper::meta_union meta;
          EXPECT_EQ(0, consult.get_decimal(integer, fractional, meta));
          EXPECT_EQ(55, integer);
          EXPECT_EQ(0, fractional);
          EXPECT_EQ(width, meta.value_.width_);
          EXPECT_EQ(precision, meta.value_.precision_);
          EXPECT_EQ(true, (meta.value_.sign_ == 0 ? true : false));
        }
        //被除数为0 0 / 65 = 0.00
        {
          ObObj one;
          ObObj other;
          ObObj consult;
          int64_t integer = 0;
          int64_t fractional = 0;
          int64_t width = 6;
          int64_t precision = 1;
          EXPECT_EQ(0, one.set_decimal(integer, fractional, width, precision));
          int64_t val = 65;
          other.set_int(val);
          EXPECT_EQ(0, one.do_decimal_div(other, consult));
          ObDecimalHelper::meta_union meta;
          EXPECT_EQ(0, consult.get_decimal(integer, fractional, meta));
          EXPECT_EQ(0, integer);
          EXPECT_EQ(0, fractional);
          EXPECT_EQ(width, meta.value_.width_);
          EXPECT_EQ(precision, meta.value_.precision_);
          EXPECT_EQ(true, (meta.value_.sign_ == 0 ? true : false));
        }
        //被除数为0  54.45 / 0  warn
        {
          ObObj one;
          ObObj other;
          ObObj consult;
          int64_t integer = 54;
          int64_t fractional = 45;
          int64_t width = 6;
          int64_t precision = 2;
          EXPECT_EQ(0, one.set_decimal(integer, fractional, width, precision));
          int64_t val = 0;
          other.set_int(val);
          EXPECT_EQ(OB_OBJ_DIVIDE_BY_ZERO, one.do_decimal_div(other, consult));
        }

        //=================
        // 被除数为负数
        //=================

        //整数部分为0 -0.43 / 24 = 0.017
        {
          ObObj one;
          ObObj other;
          ObObj consult;
          int64_t integer = 0;
          int64_t fractional = 43;
          int64_t width = 6;
          int64_t precision = 2;
          EXPECT_EQ(0, one.set_decimal(integer, fractional, width, precision, false));
          int64_t val = 24;
          other.set_int(val);
          EXPECT_EQ(0, one.do_decimal_div(other, consult));
          ObDecimalHelper::meta_union meta;
          EXPECT_EQ(0, consult.get_decimal(integer, fractional, meta));
          EXPECT_EQ(0, integer);
          EXPECT_EQ(2, fractional);
          EXPECT_EQ(width, meta.value_.width_);
          EXPECT_EQ(precision, meta.value_.precision_);
          EXPECT_EQ(false, (meta.value_.sign_ == 0 ? true : false));
        }
        //需要后移一位 -0.45 / 67 =  0.006
        {
          ObObj one;
          ObObj other;
          ObObj consult;
          int64_t integer = 0;
          int64_t fractional = 45;
          int64_t width = 6;
          int64_t precision = 2;
          EXPECT_EQ(0, one.set_decimal(integer, fractional, width, precision, false));
          int64_t val = 67;
          other.set_int(val);
          EXPECT_EQ(0, one.do_decimal_div(other, consult));
          ObDecimalHelper::meta_union meta;
          EXPECT_EQ(0, consult.get_decimal(integer, fractional, meta));
          EXPECT_EQ(0, integer);
          EXPECT_EQ(1, fractional);
          EXPECT_EQ(width, meta.value_.width_);
          EXPECT_EQ(precision, meta.value_.precision_);
          EXPECT_EQ(false, (meta.value_.sign_ == 0 ? true : false));
        }
        //小数部分为0  -234 / 12 = -19.5
        {
          ObObj one;
          ObObj other;
          ObObj consult;
          int64_t integer = 234;
          int64_t fractional = 0;
          int64_t width = 6;
          int64_t precision = 2;
          EXPECT_EQ(0, one.set_decimal(integer, fractional, width, precision, false));
          int64_t val = 12;
          other.set_int(val);
          EXPECT_EQ(0, one.do_decimal_div(other, consult));
          ObDecimalHelper::meta_union meta;
          EXPECT_EQ(0, consult.get_decimal(integer, fractional, meta));
          EXPECT_EQ(19, integer);
          EXPECT_EQ(50, fractional);
          EXPECT_EQ(width, meta.value_.width_);
          EXPECT_EQ(precision, meta.value_.precision_);
          EXPECT_EQ(false, (meta.value_.sign_ == 0 ? true : false));
        }
        //需要后移一位  -234 / 54 = -4.333
        {
          ObObj one;
          ObObj other;
          ObObj consult;
          int64_t integer = 234;
          int64_t fractional = 0;
          int64_t width = 6;
          int64_t precision = 2;
          EXPECT_EQ(0, one.set_decimal(integer, fractional, width, precision, false));
          int64_t val = 54;
          other.set_int(val);
          EXPECT_EQ(0, one.do_decimal_div(other, consult));
          ObDecimalHelper::meta_union meta;
          EXPECT_EQ(0, consult.get_decimal(integer, fractional, meta));
          EXPECT_EQ(4, integer);
          EXPECT_EQ(33, fractional);
          EXPECT_EQ(width, meta.value_.width_);
          EXPECT_EQ(precision, meta.value_.precision_);
          EXPECT_EQ(false, (meta.value_.sign_ == 0 ? true : false));
        }
        //小数四舍五入后有向高位的进位  456.89 / 14 = 32.635
        {
          ObObj one;
          ObObj other;
          ObObj consult;
          int64_t integer = 456;
          int64_t fractional = 89;
          int64_t width = 6;
          int64_t precision = 2;
          EXPECT_EQ(0, one.set_decimal(integer, fractional, width, precision, false));
          int64_t val = 14;
          other.set_int(val);
          EXPECT_EQ(0, one.do_decimal_div(other, consult));
          ObDecimalHelper::meta_union meta;
          EXPECT_EQ(0, consult.get_decimal(integer, fractional, meta));
          EXPECT_EQ(32, integer);
          EXPECT_EQ(64, fractional);
          EXPECT_EQ(width, meta.value_.width_);
          EXPECT_EQ(precision, meta.value_.precision_);
          EXPECT_EQ(false, (meta.value_.sign_ == 0 ? true : false));
        }
        //小数有向整数的进位   3573.1 / 65 = 54.97
        {
          ObObj one;
          ObObj other;
          ObObj consult;
          int64_t integer = 3573;
          int64_t fractional = 1;
          int64_t width = 6;
          int64_t precision = 1;
          EXPECT_EQ(0, one.set_decimal(integer, fractional, width, precision, false));
          int64_t val = 65;
          other.set_int(val);
          EXPECT_EQ(0, one.do_decimal_div(other, consult));
          ObDecimalHelper::meta_union meta;
          EXPECT_EQ(0, consult.get_decimal(integer, fractional, meta));
          EXPECT_EQ(55, integer);
          EXPECT_EQ(0, fractional);
          EXPECT_EQ(width, meta.value_.width_);
          EXPECT_EQ(precision, meta.value_.precision_);
          EXPECT_EQ(false, (meta.value_.sign_ == 0 ? true : false));
        }
        //被除数为0 0 / 65 = 0.00
        {
          ObObj one;
          ObObj other;
          ObObj consult;
          int64_t integer = 0;
          int64_t fractional = 0;
          int64_t width = 6;
          int64_t precision = 1;
          EXPECT_EQ(0, one.set_decimal(integer, fractional, width, precision, false));
          int64_t val = 65;
          other.set_int(val);
          EXPECT_EQ(0, one.do_decimal_div(other, consult));
          ObDecimalHelper::meta_union meta;
          EXPECT_EQ(0, consult.get_decimal(integer, fractional, meta));
          EXPECT_EQ(0, integer);
          EXPECT_EQ(0, fractional);
          EXPECT_EQ(width, meta.value_.width_);
          EXPECT_EQ(precision, meta.value_.precision_);
          EXPECT_EQ(true, (meta.value_.sign_ == 0 ? true : false));
        }
        //被除数为0  54.45 / 0  warn
        {
          ObObj one;
          ObObj other;
          ObObj consult;
          int64_t integer = 54;
          int64_t fractional = 45;
          int64_t width = 6;
          int64_t precision = 2;
          EXPECT_EQ(0, one.set_decimal(integer, fractional, width, precision, false));
          int64_t val = 0;
          other.set_int(val);
          EXPECT_EQ(OB_OBJ_DIVIDE_BY_ZERO, one.do_decimal_div(other, consult));
        }

        //=====================
        //两者均未负数
        //=====================

        //整数部分为0 -0.43 / 24 = 0.017
        {
          ObObj one;
          ObObj other;
          ObObj consult;
          int64_t integer = 0;
          int64_t fractional = 43;
          int64_t width = 6;
          int64_t precision = 2;
          EXPECT_EQ(0, one.set_decimal(integer, fractional, width, precision, false));
          int64_t val = -24;
          other.set_int(val);
          EXPECT_EQ(0, one.do_decimal_div(other, consult));
          ObDecimalHelper::meta_union meta;
          EXPECT_EQ(0, consult.get_decimal(integer, fractional, meta));
          EXPECT_EQ(0, integer);
          EXPECT_EQ(2, fractional);
          EXPECT_EQ(width, meta.value_.width_);
          EXPECT_EQ(precision, meta.value_.precision_);
          EXPECT_EQ(true, (meta.value_.sign_ == 0 ? true : false));
        }
        //需要后移一位 -0.45 / 67 =  0.006
        {
          ObObj one;
          ObObj other;
          ObObj consult;
          int64_t integer = 0;
          int64_t fractional = 45;
          int64_t width = 6;
          int64_t precision = 2;
          EXPECT_EQ(0, one.set_decimal(integer, fractional, width, precision, false));
          int64_t val = -67;
          other.set_int(val);
          EXPECT_EQ(0, one.do_decimal_div(other, consult));
          ObDecimalHelper::meta_union meta;
          EXPECT_EQ(0, consult.get_decimal(integer, fractional, meta));
          EXPECT_EQ(0, integer);
          EXPECT_EQ(1, fractional);
          EXPECT_EQ(width, meta.value_.width_);
          EXPECT_EQ(precision, meta.value_.precision_);
          EXPECT_EQ(true, (meta.value_.sign_ == 0 ? true : false));
        }
        //小数部分为0  -234 / 12 = -19.5
        {
          ObObj one;
          ObObj other;
          ObObj consult;
          int64_t integer = 234;
          int64_t fractional = 0;
          int64_t width = 6;
          int64_t precision = 2;
          EXPECT_EQ(0, one.set_decimal(integer, fractional, width, precision, false));
          int64_t val = -12;
          other.set_int(val);
          EXPECT_EQ(0, one.do_decimal_div(other, consult));
          ObDecimalHelper::meta_union meta;
          EXPECT_EQ(0, consult.get_decimal(integer, fractional, meta));
          EXPECT_EQ(19, integer);
          EXPECT_EQ(50, fractional);
          EXPECT_EQ(width, meta.value_.width_);
          EXPECT_EQ(precision, meta.value_.precision_);
          EXPECT_EQ(true, (meta.value_.sign_ == 0 ? true : false));
        }
        //需要后移一位  -234 / 54 = -4.333
        {
          ObObj one;
          ObObj other;
          ObObj consult;
          int64_t integer = 234;
          int64_t fractional = 0;
          int64_t width = 6;
          int64_t precision = 2;
          EXPECT_EQ(0, one.set_decimal(integer, fractional, width, precision, false));
          int64_t val = -54;
          other.set_int(val);
          EXPECT_EQ(0, one.do_decimal_div(other, consult));
          ObDecimalHelper::meta_union meta;
          EXPECT_EQ(0, consult.get_decimal(integer, fractional, meta));
          EXPECT_EQ(4, integer);
          EXPECT_EQ(33, fractional);
          EXPECT_EQ(width, meta.value_.width_);
          EXPECT_EQ(precision, meta.value_.precision_);
          EXPECT_EQ(true, (meta.value_.sign_ == 0 ? true : false));
        }
        //小数四舍五入后有向高位的进位  456.89 / 14 = 32.635
        {
          ObObj one;
          ObObj other;
          ObObj consult;
          int64_t integer = 456;
          int64_t fractional = 89;
          int64_t width = 6;
          int64_t precision = 2;
          EXPECT_EQ(0, one.set_decimal(integer, fractional, width, precision, false));
          int64_t val = -14;
          other.set_int(val);
          EXPECT_EQ(0, one.do_decimal_div(other, consult));
          ObDecimalHelper::meta_union meta;
          EXPECT_EQ(0, consult.get_decimal(integer, fractional, meta));
          EXPECT_EQ(32, integer);
          EXPECT_EQ(64, fractional);
          EXPECT_EQ(width, meta.value_.width_);
          EXPECT_EQ(precision, meta.value_.precision_);
          EXPECT_EQ(true, (meta.value_.sign_ == 0 ? true : false));
        }
        //小数有向整数的进位   3573.1 / 65 = 54.97
        {
          ObObj one;
          ObObj other;
          ObObj consult;
          int64_t integer = 3573;
          int64_t fractional = 1;
          int64_t width = 6;
          int64_t precision = 1;
          EXPECT_EQ(0, one.set_decimal(integer, fractional, width, precision, false));
          int64_t val = -65;
          other.set_int(val);
          EXPECT_EQ(0, one.do_decimal_div(other, consult));
          ObDecimalHelper::meta_union meta;
          EXPECT_EQ(0, consult.get_decimal(integer, fractional, meta));
          EXPECT_EQ(55, integer);
          EXPECT_EQ(0, fractional);
          EXPECT_EQ(width, meta.value_.width_);
          EXPECT_EQ(precision, meta.value_.precision_);
          EXPECT_EQ(true, (meta.value_.sign_ == 0 ? true : false));
        }
        //被除数为0 0 / 65 = 0.00
        {
          ObObj one;
          ObObj other;
          ObObj consult;
          int64_t integer = 0;
          int64_t fractional = 0;
          int64_t width = 6;
          int64_t precision = 1;
          EXPECT_EQ(0, one.set_decimal(integer, fractional, width, precision, false));
          int64_t val = -65;
          other.set_int(val);
          EXPECT_EQ(0, one.do_decimal_div(other, consult));
          ObDecimalHelper::meta_union meta;
          EXPECT_EQ(0, consult.get_decimal(integer, fractional, meta));
          EXPECT_EQ(0, integer);
          EXPECT_EQ(0, fractional);
          EXPECT_EQ(width, meta.value_.width_);
          EXPECT_EQ(precision, meta.value_.precision_);
          EXPECT_EQ(true, (meta.value_.sign_ == 0 ? true : false));
        }
        //被除数为0  54.45 / 0  warn
        {
          ObObj one;
          ObObj other;
          ObObj consult;
          int64_t integer = 54;
          int64_t fractional = 45;
          int64_t width = 6;
          int64_t precision = 2;
          EXPECT_EQ(0, one.set_decimal(integer, fractional, width, precision, false));
          int64_t val = 0;
          other.set_int(val);
          EXPECT_EQ(OB_OBJ_DIVIDE_BY_ZERO, one.do_decimal_div(other, consult));
        }

      }

      //bool ObObj::operator < (const ObObj &that_obj) const
      TEST_F(TestObjDecimal, less)
      {
        //================
        //两个正定点数
        //================

        //======精度相同

        //整数大,小数大
        {
          ObObj obj1;
          int64_t int1 = 34;
          int64_t fra1 = 65;
          int64_t p1 = 3;
          int64_t w1 = 6;
          EXPECT_EQ(0, obj1.set_decimal(int1, fra1, w1, p1));

          ObObj obj2;
          int64_t int2 = 33;
          int64_t fra2 = 5;
          int64_t p2 = 3;
          int64_t w2 = 6;
          EXPECT_EQ(0, obj1.set_decimal(int2, fra2, w2, p2));

          EXPECT_EQ(false, obj1 < obj2);

        }

        //整数大,小数小
        {
          ObObj obj1;
          int64_t int1 = 34;
          int64_t fra1 = 65;
          int64_t p1 = 3;
          int64_t w1 = 6;
          EXPECT_EQ(0, obj1.set_decimal(int1, fra1, w1, p1));

          ObObj obj2;
          int64_t int2 = 33;
          int64_t fra2 = 85;
          int64_t p2 = 3;
          int64_t w2 = 6;
          EXPECT_EQ(0, obj1.set_decimal(int2, fra2, w2, p2));

          EXPECT_EQ(false, obj1 < obj2);
        }

        //整数小,小数大
        {
          ObObj obj1;
          int64_t int1 = 34;
          int64_t fra1 = 95;
          int64_t p1 = 3;
          int64_t w1 = 6;
          EXPECT_EQ(0, obj1.set_decimal(int1, fra1, w1, p1));

          ObObj obj2;
          int64_t int2 = 53;
          int64_t fra2 = 85;
          int64_t p2 = 3;
          int64_t w2 = 6;
          EXPECT_EQ(0, obj2.set_decimal(int2, fra2, w2, p2));

          EXPECT_EQ(true, obj1 < obj2);
        }

        //整数小,小数小
        {
          ObObj obj1;
          int64_t int1 = 34;
          int64_t fra1 = 65;
          int64_t p1 = 3;
          int64_t w1 = 6;
          EXPECT_EQ(0, obj1.set_decimal(int1, fra1, w1, p1));

          ObObj obj2;
          int64_t int2 = 83;
          int64_t fra2 = 85;
          int64_t p2 = 3;
          int64_t w2 = 6;
          EXPECT_EQ(0, obj2.set_decimal(int2, fra2, w2, p2));

          EXPECT_EQ(true, obj1 < obj2);
        }

        //相等的定点数
        {
          ObObj obj1;
          int64_t int1 = 34;
          int64_t fra1 = 65;
          int64_t p1 = 3;
          int64_t w1 = 6;
          EXPECT_EQ(0, obj1.set_decimal(int1, fra1, w1, p1));

          ObObj obj2;
          int64_t int2 = 34;
          int64_t fra2 = 65;
          int64_t p2 = 3;
          int64_t w2 = 6;
          EXPECT_EQ(0, obj2.set_decimal(int2, fra2, w2, p2));

          EXPECT_EQ(false, obj1 < obj2);
        }

        //整数为0  小数为0
        {
          ObObj obj1;
          int64_t int1 = 0;
          int64_t fra1 = 0;
          int64_t p1 = 3;
          int64_t w1 = 6;
          EXPECT_EQ(0, obj1.set_decimal(int1, fra1, w1, p1));

          ObObj obj2;
          int64_t int2 = 0;
          int64_t fra2 = 0;
          int64_t p2 = 3;
          int64_t w2 = 6;
          EXPECT_EQ(0, obj2.set_decimal(int2, fra2, w2, p2));

          EXPECT_EQ(false, obj1 < obj2);
        }

        //========精度不同

        //精度大的值小
        {
          ObObj obj1;
          int64_t int1 = 34;
          int64_t fra1 = 9;
          int64_t p1 = 2;
          int64_t w1 = 6;
          EXPECT_EQ(0, obj1.set_decimal(int1, fra1, w1, p1));

          ObObj obj2;
          int64_t int2 = 34;
          int64_t fra2 = 85;
          int64_t p2 = 3;
          int64_t w2 = 6;
          EXPECT_EQ(0, obj2.set_decimal(int2, fra2, w2, p2));

          EXPECT_EQ(false, obj1 < obj2);
        }

        //精度大的值大
        {
          ObObj obj1;
          int64_t int1 = 34;
          int64_t fra1 = 7;
          int64_t p1 = 2;
          int64_t w1 = 6;
          EXPECT_EQ(0, obj1.set_decimal(int1, fra1, w1, p1));

          ObObj obj2;
          int64_t int2 = 34;
          int64_t fra2 = 85;
          int64_t p2 = 3;
          int64_t w2 = 6;
          EXPECT_EQ(0, obj2.set_decimal(int2, fra2, w2, p2));

          EXPECT_EQ(true, obj1 < obj2);
        }

        //小数相同,精度不同
        {
          ObObj obj1;
          int64_t int1 = 34;
          int64_t fra1 = 9;
          int64_t p1 = 2;
          int64_t w1 = 6;
          EXPECT_EQ(0, obj1.set_decimal(int1, fra1, w1, p1));

          ObObj obj2;
          int64_t int2 = 34;
          int64_t fra2 = 90;
          int64_t p2 = 3;
          int64_t w2 = 6;
          EXPECT_EQ(0, obj2.set_decimal(int2, fra2, w2, p2));

          EXPECT_EQ(false, obj1 < obj2);
        }

        //整数大,小精度,值大
        {
          ObObj obj1;
          int64_t int1 = 35;
          int64_t fra1 = 93;
          int64_t p1 = 2;
          int64_t w1 = 6;
          EXPECT_EQ(0, obj1.set_decimal(int1, fra1, w1, p1));

          ObObj obj2;
          int64_t int2 = 34;
          int64_t fra2 = 85;
          int64_t p2 = 3;
          int64_t w2 = 6;
          EXPECT_EQ(0, obj2.set_decimal(int2, fra2, w2, p2));

          EXPECT_EQ(false, obj1 < obj2);
        }

        //整数小,大精度,值小
        {
          ObObj obj1;
          int64_t int1 = 33;
          int64_t fra1 = 9;
          int64_t p1 = 4;
          int64_t w1 = 6;
          EXPECT_EQ(0, obj1.set_decimal(int1, fra1, w1, p1));

          ObObj obj2;
          int64_t int2 = 34;
          int64_t fra2 = 85;
          int64_t p2 = 3;
          int64_t w2 = 6;
          EXPECT_EQ(0, obj2.set_decimal(int2, fra2, w2, p2));

          EXPECT_EQ(true, obj1 < obj2);
        }

        //小数为0,精度不同
        {
          ObObj obj1;
          int64_t int1 = 34;
          int64_t fra1 = 0;
          int64_t p1 = 2;
          int64_t w1 = 6;
          EXPECT_EQ(0, obj1.set_decimal(int1, fra1, w1, p1));

          ObObj obj2;
          int64_t int2 = 34;
          int64_t fra2 = 0;
          int64_t p2 = 3;
          int64_t w2 = 6;
          EXPECT_EQ(0, obj2.set_decimal(int2, fra2, w2, p2));

          EXPECT_EQ(false, obj1 < obj2);
        }

        //=================
        // 一个正定点数 一个负定点数
        //=================

        //=========精度相同

        //整数大,小数小
        {
          ObObj obj1;
          int64_t int1 = 45;
          int64_t fra1 = 3;
          int64_t p1 = 3;
          int64_t w1 = 6;
          EXPECT_EQ(0, obj1.set_decimal(int1, fra1, w1, p1, false));

          ObObj obj2;
          int64_t int2 = 24;
          int64_t fra2 = 75;
          int64_t p2 = 3;
          int64_t w2 = 6;
          EXPECT_EQ(0, obj2.set_decimal(int2, fra2, w2, p2));

          EXPECT_EQ(true, obj1 < obj2);
          EXPECT_EQ(false, obj2 < obj1);
        }

        //整数小,小数大
        {
          ObObj obj1;
          int64_t int1 = 5;
          int64_t fra1 = 36;
          int64_t p1 = 3;
          int64_t w1 = 6;
          EXPECT_EQ(0, obj1.set_decimal(int1, fra1, w1, p1, false));

          ObObj obj2;
          int64_t int2 = 24;
          int64_t fra2 = 25;
          int64_t p2 = 3;
          int64_t w2 = 6;
          EXPECT_EQ(0, obj2.set_decimal(int2, fra2, w2, p2));

          EXPECT_EQ(true, obj1 < obj2);
          EXPECT_EQ(false, obj2 < obj1);
        }

        //整数小,小数大
        {
          ObObj obj1;
          int64_t int1 = 45;
          int64_t fra1 = 83;
          int64_t p1 = 3;
          int64_t w1 = 6;
          EXPECT_EQ(0, obj1.set_decimal(int1, fra1, w1, p1, false));

          ObObj obj2;
          int64_t int2 = 74;
          int64_t fra2 = 75;
          int64_t p2 = 3;
          int64_t w2 = 6;
          EXPECT_EQ(0, obj2.set_decimal(int2, fra2, w2, p2));

          EXPECT_EQ(true, obj1 < obj2);
          EXPECT_EQ(false, obj2 < obj1);
        }

        //相等的定点数
        {
          ObObj obj1;
          int64_t int1 = 45;
          int64_t fra1 = 3;
          int64_t p1 = 3;
          int64_t w1 = 6;
          EXPECT_EQ(0, obj1.set_decimal(int1, fra1, w1, p1, false));

          ObObj obj2;
          int64_t int2 = 45;
          int64_t fra2 = 3;
          int64_t p2 = 3;
          int64_t w2 = 6;
          EXPECT_EQ(0, obj2.set_decimal(int2, fra2, w2, p2));

          EXPECT_EQ(true, obj1 < obj2);
          EXPECT_EQ(false, obj2 < obj1);
        }

        //整数为0  小数为0
        {
          ObObj obj1;
          int64_t int1 = 0;
          int64_t fra1 = 0;
          int64_t p1 = 3;
          int64_t w1 = 6;
          EXPECT_EQ(0, obj1.set_decimal(int1, fra1, w1, p1));

          ObObj obj2;
          int64_t int2 = 0;
          int64_t fra2 = 0;
          int64_t p2 = 3;
          int64_t w2 = 6;
          EXPECT_EQ(0, obj2.set_decimal(int2, fra2, w2, p2));

          EXPECT_EQ(false, obj1 < obj2);
          EXPECT_EQ(false, obj2 < obj1);
        }

        //=============精度不同

        //精度大的值小
        {
          ObObj obj1;
          int64_t int1 = 45;
          int64_t fra1 = 3;
          int64_t p1 = 2;
          int64_t w1 = 6;
          EXPECT_EQ(0, obj1.set_decimal(int1, fra1, w1, p1, false));

          ObObj obj2;
          int64_t int2 = 24;
          int64_t fra2 = 2;
          int64_t p2 = 3;
          int64_t w2 = 6;
          EXPECT_EQ(0, obj2.set_decimal(int2, fra2, w2, p2));

          EXPECT_EQ(true, obj1 < obj2);
          EXPECT_EQ(false, obj2 < obj1);
        }

        //精度大的值大
        {
          ObObj obj1;
          int64_t int1 = 45;
          int64_t fra1 = 3;
          int64_t p1 = 2;
          int64_t w1 = 6;
          EXPECT_EQ(0, obj1.set_decimal(int1, fra1, w1, p1, false));

          ObObj obj2;
          int64_t int2 = 78;
          int64_t fra2 = 75;
          int64_t p2 = 3;
          int64_t w2 = 6;
          EXPECT_EQ(0, obj2.set_decimal(int2, fra2, w2, p2));

          EXPECT_EQ(true, obj1 < obj2);
          EXPECT_EQ(false, obj2 < obj1);
        }

        //小数相同,精度不同
        {
          ObObj obj1;
          int64_t int1 = 45;
          int64_t fra1 = 3;
          int64_t p1 = 2;
          int64_t w1 = 6;
          EXPECT_EQ(0, obj1.set_decimal(int1, fra1, w1, p1));

          ObObj obj2;
          int64_t int2 = 45;
          int64_t fra2 = 30;
          int64_t p2 = 3;
          int64_t w2 = 6;
          EXPECT_EQ(0, obj2.set_decimal(int2, fra2, w2, p2, false));

          EXPECT_EQ(false, obj1 < obj2);
          EXPECT_EQ(true, obj2 < obj1);
        }

        //整数大,小精度,值大
        {
          ObObj obj1;
          int64_t int1 = 45;
          int64_t fra1 = 3;
          int64_t p1 = 2;
          int64_t w1 = 6;
          EXPECT_EQ(0, obj1.set_decimal(int1, fra1, w1, p1));

          ObObj obj2;
          int64_t int2 = 41;
          int64_t fra2 = 30;
          int64_t p2 = 3;
          int64_t w2 = 6;
          EXPECT_EQ(0, obj2.set_decimal(int2, fra2, w2, p2, false));

          EXPECT_EQ(false, obj1 < obj2);
        }

        //整数小,大精度,值小
        {
          ObObj obj1;
          int64_t int1 = 45;
          int64_t fra1 = 3;
          int64_t p1 = 2;
          int64_t w1 = 6;
          EXPECT_EQ(0, obj1.set_decimal(int1, fra1, w1, p1));

          ObObj obj2;
          int64_t int2 = 48;
          int64_t fra2 = 30;
          int64_t p2 = 3;
          int64_t w2 = 6;
          EXPECT_EQ(0, obj2.set_decimal(int2, fra2, w2, p2, false));

          EXPECT_EQ(false, obj1 < obj2);
        }

        //小数为0,精度不同
        {
          ObObj obj1;
          int64_t int1 = 45;
          int64_t fra1 = 0;
          int64_t p1 = 2;
          int64_t w1 = 6;
          EXPECT_EQ(0, obj1.set_decimal(int1, fra1, w1, p1));

          ObObj obj2;
          int64_t int2 = 45;
          int64_t fra2 = 0;
          int64_t p2 = 3;
          int64_t w2 = 6;
          EXPECT_EQ(0, obj2.set_decimal(int2, fra2, w2, p2, false));

          EXPECT_EQ(false, obj1 < obj2);
        }

        //====================
        //两个负定点数
        //====================

        //=========精度相同

        //整数大,小数小
        {
          ObObj obj1;
          int64_t int1 = 49;
          int64_t fra1 = 3;
          int64_t p1 = 3;
          int64_t w1 = 6;
          EXPECT_EQ(0, obj1.set_decimal(int1, fra1, w1, p1, false));

          ObObj obj2;
          int64_t int2 = 45;
          int64_t fra2 = 30;
          int64_t p2 = 3;
          int64_t w2 = 6;
          EXPECT_EQ(0, obj2.set_decimal(int2, fra2, w2, p2, false));

          EXPECT_EQ(true, obj1 < obj2);
          EXPECT_EQ(false, obj2 < obj1);
        }

        //整数小,小数大
        {
          ObObj obj1;
          int64_t int1 = 34;
          int64_t fra1 = 35;
          int64_t p1 = 3;
          int64_t w1 = 6;
          EXPECT_EQ(0, obj1.set_decimal(int1, fra1, w1, p1, false));

          ObObj obj2;
          int64_t int2 = 45;
          int64_t fra2 = 30;
          int64_t p2 = 3;
          int64_t w2 = 6;
          EXPECT_EQ(0, obj2.set_decimal(int2, fra2, w2, p2, false));

          EXPECT_EQ(false, obj1 < obj2);
        }

        //整数小,小数大
        {
          ObObj obj1;
          int64_t int1 = 49;
          int64_t fra1 = 37;
          int64_t p1 = 3;
          int64_t w1 = 6;
          EXPECT_EQ(0, obj1.set_decimal(int1, fra1, w1, p1, false));

          ObObj obj2;
          int64_t int2 = 74;
          int64_t fra2 = 30;
          int64_t p2 = 3;
          int64_t w2 = 6;
          EXPECT_EQ(0, obj2.set_decimal(int2, fra2, w2, p2, false));

          EXPECT_EQ(false, obj1 < obj2);
        }

        //整数相同,小数不同
        {
          ObObj obj1;
          int64_t int1 = 45;
          int64_t fra1 = 3;
          int64_t p1 = 3;
          int64_t w1 = 6;
          EXPECT_EQ(0, obj1.set_decimal(int1, fra1, w1, p1, false));

          ObObj obj2;
          int64_t int2 = 45;
          int64_t fra2 = 30;
          int64_t p2 = 3;
          int64_t w2 = 6;
          EXPECT_EQ(0, obj2.set_decimal(int2, fra2, w2, p2, false));

          EXPECT_EQ(false, obj1 < obj2);
        }

        //相等的定点数
        {
          ObObj obj1;
          int64_t int1 = 49;
          int64_t fra1 = 30;
          int64_t p1 = 3;
          int64_t w1 = 6;
          EXPECT_EQ(0, obj1.set_decimal(int1, fra1, w1, p1, false));

          ObObj obj2;
          int64_t int2 = 49;
          int64_t fra2 = 30;
          int64_t p2 = 3;
          int64_t w2 = 6;
          EXPECT_EQ(0, obj2.set_decimal(int2, fra2, w2, p2, false));

          EXPECT_EQ(false, obj1 < obj2);
        }

        //整数为0  小数为0
        {
          ObObj obj1;
          int64_t int1 = 0;
          int64_t fra1 = 0;
          int64_t p1 = 3;
          int64_t w1 = 6;
          EXPECT_EQ(0, obj1.set_decimal(int1, fra1, w1, p1, false));

          ObObj obj2;
          int64_t int2 = 0;
          int64_t fra2 = 0;
          int64_t p2 = 3;
          int64_t w2 = 6;
          EXPECT_EQ(0, obj2.set_decimal(int2, fra2, w2, p2, false));

          EXPECT_EQ(false, obj1 < obj2);
        }

        //=============精度不同

        //精度大的值小
        {
          ObObj obj1;
          int64_t int1 = 49;
          int64_t fra1 = 3;
          int64_t p1 = 2;
          int64_t w1 = 6;
          EXPECT_EQ(0, obj1.set_decimal(int1, fra1, w1, p1, false));

          ObObj obj2;
          int64_t int2 = 54;
          int64_t fra2 = 30;
          int64_t p2 = 2;
          int64_t w2 = 6;
          EXPECT_EQ(0, obj2.set_decimal(int2, fra2, w2, p2, false));

          EXPECT_EQ(false, obj1 < obj2);
        }

        //精度大的值大
        {
          ObObj obj1;
          int64_t int1 = 49;
          int64_t fra1 = 3;
          int64_t p1 = 2;
          int64_t w1 = 6;
          EXPECT_EQ(0, obj1.set_decimal(int1, fra1, w1, p1, false));

          ObObj obj2;
          int64_t int2 = 45;
          int64_t fra2 = 32;
          int64_t p2 = 3;
          int64_t w2 = 6;
          EXPECT_EQ(0, obj2.set_decimal(int2, fra2, w2, p2, false));

          EXPECT_EQ(true, obj1 < obj2);
        }

        //小数相同,精度不同
        {
          ObObj obj1;
          int64_t int1 = 45;
          int64_t fra1 = 3;
          int64_t p1 = 2;
          int64_t w1 = 6;
          EXPECT_EQ(0, obj1.set_decimal(int1, fra1, w1, p1, false));

          ObObj obj2;
          int64_t int2 = 45;
          int64_t fra2 = 30;
          int64_t p2 = 3;
          int64_t w2 = 6;
          EXPECT_EQ(0, obj2.set_decimal(int2, fra2, w2, p2, false));

          EXPECT_EQ(false, obj1 < obj2);
        }

        //整数大,小精度,值大
        {
          ObObj obj1;
          int64_t int1 = 49;
          int64_t fra1 = 3;
          int64_t p1 = 2;
          int64_t w1 = 6;
          EXPECT_EQ(0, obj1.set_decimal(int1, fra1, w1, p1, false));

          ObObj obj2;
          int64_t int2 = 45;
          int64_t fra2 = 39;
          int64_t p2 = 3;
          int64_t w2 = 6;
          EXPECT_EQ(0, obj2.set_decimal(int2, fra2, w2, p2, false));

          EXPECT_EQ(true, obj1 < obj2);
        }

        //整数小,大精度,值小
        {
          ObObj obj1;
          int64_t int1 = 43;
          int64_t fra1 = 32;
          int64_t p1 = 2;
          int64_t w1 = 6;
          EXPECT_EQ(0, obj1.set_decimal(int1, fra1, w1, p1, false));

          ObObj obj2;
          int64_t int2 = 45;
          int64_t fra2 = 30;
          int64_t p2 = 3;
          int64_t w2 = 6;
          EXPECT_EQ(0, obj2.set_decimal(int2, fra2, w2, p2, false));

          EXPECT_EQ(false, obj1 < obj2);
        }

        //小数为0,精度不同
        {
          ObObj obj1;
          int64_t int1 = 49;
          int64_t fra1 = 0;
          int64_t p1 = 2;
          int64_t w1 = 6;
          EXPECT_EQ(0, obj1.set_decimal(int1, fra1, w1, p1, false));

          ObObj obj2;
          int64_t int2 = 49;
          int64_t fra2 = 0;
          int64_t p2 = 3;
          int64_t w2 = 6;
          EXPECT_EQ(0, obj2.set_decimal(int2, fra2, w2, p2, false));

          EXPECT_EQ(false, obj1 < obj2);
        }
      }

	 //bool ObObj::operator>(const ObObj &that_obj) const
      TEST_F(TestObjDecimal, big)
      {
        //================
        //两个正定点数
        //================

        //======精度相同

        //整数大，小数大
        {
          ObObj obj1;
          int64_t int1 = 34;
          int64_t fra1 = 65;
          int64_t p1 = 3;
          int64_t w1 = 6;
          EXPECT_EQ(0, obj1.set_decimal(int1, fra1, w1, p1));

          ObObj obj2;
          int64_t int2 = 33;
          int64_t fra2 = 5;
          int64_t p2 = 3;
          int64_t w2 = 6;
          EXPECT_EQ(0, obj1.set_decimal(int2, fra2, w2, p2));

          EXPECT_EQ(true, obj1 > obj2);

        }

        //整数大，小数小
        {
          ObObj obj1;
          int64_t int1 = 34;
          int64_t fra1 = 65;
          int64_t p1 = 3;
          int64_t w1 = 6;
          EXPECT_EQ(0, obj1.set_decimal(int1, fra1, w1, p1));

          ObObj obj2;
          int64_t int2 = 33;
          int64_t fra2 = 85;
          int64_t p2 = 3;
          int64_t w2 = 6;
          EXPECT_EQ(0, obj1.set_decimal(int2, fra2, w2, p2));

          EXPECT_EQ(true, obj1 > obj2);
        }

        //整数小，小数大
        {
          ObObj obj1;
          int64_t int1 = 34;
          int64_t fra1 = 95;
          int64_t p1 = 3;
          int64_t w1 = 6;
          EXPECT_EQ(0, obj1.set_decimal(int1, fra1, w1, p1));

          ObObj obj2;
          int64_t int2 = 53;
          int64_t fra2 = 85;
          int64_t p2 = 3;
          int64_t w2 = 6;
          EXPECT_EQ(0, obj2.set_decimal(int2, fra2, w2, p2));

          EXPECT_EQ(false, obj1 > obj2);
        }

        //整数小，小数小
        {
          ObObj obj1;
          int64_t int1 = 34;
          int64_t fra1 = 65;
          int64_t p1 = 3;
          int64_t w1 = 6;
          EXPECT_EQ(0, obj1.set_decimal(int1, fra1, w1, p1));

          ObObj obj2;
          int64_t int2 = 83;
          int64_t fra2 = 85;
          int64_t p2 = 3;
          int64_t w2 = 6;
          EXPECT_EQ(0, obj2.set_decimal(int2, fra2, w2, p2));

          EXPECT_EQ(false, obj1 > obj2);
        }

        //相等的定点数
        {
          ObObj obj1;
          int64_t int1 = 34;
          int64_t fra1 = 65;
          int64_t p1 = 3;
          int64_t w1 = 6;
          EXPECT_EQ(0, obj1.set_decimal(int1, fra1, w1, p1));

          ObObj obj2;
          int64_t int2 = 34;
          int64_t fra2 = 65;
          int64_t p2 = 3;
          int64_t w2 = 6;
          EXPECT_EQ(0, obj2.set_decimal(int2, fra2, w2, p2));

          EXPECT_EQ(false, obj1 > obj2);
        }

        //整数为0  小数为0
        {
          ObObj obj1;
          int64_t int1 = 0;
          int64_t fra1 = 0;
          int64_t p1 = 3;
          int64_t w1 = 6;
          EXPECT_EQ(0, obj1.set_decimal(int1, fra1, w1, p1));

          ObObj obj2;
          int64_t int2 = 0;
          int64_t fra2 = 0;
          int64_t p2 = 3;
          int64_t w2 = 6;
          EXPECT_EQ(0, obj2.set_decimal(int2, fra2, w2, p2));

          EXPECT_EQ(false, obj1 > obj2);
        }

        //========精度不同

        //精度大的值小
        {
          ObObj obj1;
          int64_t int1 = 34;
          int64_t fra1 = 9;
          int64_t p1 = 2;
          int64_t w1 = 6;
          EXPECT_EQ(0, obj1.set_decimal(int1, fra1, w1, p1));

          ObObj obj2;
          int64_t int2 = 34;
          int64_t fra2 = 85;
          int64_t p2 = 3;
          int64_t w2 = 6;
          EXPECT_EQ(0, obj2.set_decimal(int2, fra2, w2, p2));

          EXPECT_EQ(true, obj1 > obj2);
        }

        //精度大的值大
        {
          ObObj obj1;
          int64_t int1 = 34;
          int64_t fra1 = 7;
          int64_t p1 = 2;
          int64_t w1 = 6;
          EXPECT_EQ(0, obj1.set_decimal(int1, fra1, w1, p1));

          ObObj obj2;
          int64_t int2 = 34;
          int64_t fra2 = 85;
          int64_t p2 = 3;
          int64_t w2 = 6;
          EXPECT_EQ(0, obj2.set_decimal(int2, fra2, w2, p2));

          EXPECT_EQ(false, obj1 > obj2);
        }

        //小数相同，精度不同
        {
          ObObj obj1;
          int64_t int1 = 34;
          int64_t fra1 = 9;
          int64_t p1 = 2;
          int64_t w1 = 6;
          EXPECT_EQ(0, obj1.set_decimal(int1, fra1, w1, p1));

          ObObj obj2;
          int64_t int2 = 34;
          int64_t fra2 = 90;
          int64_t p2 = 3;
          int64_t w2 = 6;
          EXPECT_EQ(0, obj2.set_decimal(int2, fra2, w2, p2));

          EXPECT_EQ(false, obj1 > obj2);
        }

        //整数大，小精度，值大
        {
          ObObj obj1;
          int64_t int1 = 35;
          int64_t fra1 = 93;
          int64_t p1 = 2;
          int64_t w1 = 6;
          EXPECT_EQ(0, obj1.set_decimal(int1, fra1, w1, p1));

          ObObj obj2;
          int64_t int2 = 34;
          int64_t fra2 = 85;
          int64_t p2 = 3;
          int64_t w2 = 6;
          EXPECT_EQ(0, obj2.set_decimal(int2, fra2, w2, p2));

          EXPECT_EQ(true, obj1 > obj2);
        }

        //整数小，大精度，值小
        {
          ObObj obj1;
          int64_t int1 = 33;
          int64_t fra1 = 9;
          int64_t p1 = 4;
          int64_t w1 = 6;
          EXPECT_EQ(0, obj1.set_decimal(int1, fra1, w1, p1));

          ObObj obj2;
          int64_t int2 = 34;
          int64_t fra2 = 85;
          int64_t p2 = 3;
          int64_t w2 = 6;
          EXPECT_EQ(0, obj2.set_decimal(int2, fra2, w2, p2));

          EXPECT_EQ(false, obj1 > obj2);
        }

        //小数为0,精度不同
        {
          ObObj obj1;
          int64_t int1 = 34;
          int64_t fra1 = 0;
          int64_t p1 = 2;
          int64_t w1 = 6;
          EXPECT_EQ(0, obj1.set_decimal(int1, fra1, w1, p1));

          ObObj obj2;
          int64_t int2 = 34;
          int64_t fra2 = 0;
          int64_t p2 = 3;
          int64_t w2 = 6;
          EXPECT_EQ(0, obj2.set_decimal(int2, fra2, w2, p2));

          EXPECT_EQ(false, obj1 > obj2);
        }

        //=================
        // 一个正定点数 一个负定点数
        //=================

        //=========精度相同

        //整数大，小数小
        {
          ObObj obj1;
          int64_t int1 = 45;
          int64_t fra1 = 3;
          int64_t p1 = 3;
          int64_t w1 = 6;
          EXPECT_EQ(0, obj1.set_decimal(int1, fra1, w1, p1, false));

          ObObj obj2;
          int64_t int2 = 24;
          int64_t fra2 = 75;
          int64_t p2 = 3;
          int64_t w2 = 6;
          EXPECT_EQ(0, obj2.set_decimal(int2, fra2, w2, p2));

          EXPECT_EQ(false, obj1 > obj2);
          EXPECT_EQ(true, obj2 > obj1);
        }

        //整数小，小数大
        {
          ObObj obj1;
          int64_t int1 = 5;
          int64_t fra1 = 36;
          int64_t p1 = 3;
          int64_t w1 = 6;
          EXPECT_EQ(0, obj1.set_decimal(int1, fra1, w1, p1, false));

          ObObj obj2;
          int64_t int2 = 24;
          int64_t fra2 = 25;
          int64_t p2 = 3;
          int64_t w2 = 6;
          EXPECT_EQ(0, obj2.set_decimal(int2, fra2, w2, p2));

          EXPECT_EQ(false, obj1 > obj2);
          EXPECT_EQ(true, obj2 > obj1);
        }

        //整数小，小数大
        {
          ObObj obj1;
          int64_t int1 = 45;
          int64_t fra1 = 83;
          int64_t p1 = 3;
          int64_t w1 = 6;
          EXPECT_EQ(0, obj1.set_decimal(int1, fra1, w1, p1, false));

          ObObj obj2;
          int64_t int2 = 74;
          int64_t fra2 = 75;
          int64_t p2 = 3;
          int64_t w2 = 6;
          EXPECT_EQ(0, obj2.set_decimal(int2, fra2, w2, p2));

          EXPECT_EQ(false, obj1 > obj2);
          EXPECT_EQ(true, obj2 > obj1);
        }

        //相等的定点数
        {
          ObObj obj1;
          int64_t int1 = 45;
          int64_t fra1 = 3;
          int64_t p1 = 3;
          int64_t w1 = 6;
          EXPECT_EQ(0, obj1.set_decimal(int1, fra1, w1, p1, false));

          ObObj obj2;
          int64_t int2 = 45;
          int64_t fra2 = 3;
          int64_t p2 = 3;
          int64_t w2 = 6;
          EXPECT_EQ(0, obj2.set_decimal(int2, fra2, w2, p2));

          EXPECT_EQ(false, obj1 > obj2);
          EXPECT_EQ(true, obj2 > obj1);
        }

        //整数为0  小数为0
        {
          ObObj obj1;
          int64_t int1 = 0;
          int64_t fra1 = 0;
          int64_t p1 = 3;
          int64_t w1 = 6;
          EXPECT_EQ(0, obj1.set_decimal(int1, fra1, w1, p1, false));

          ObObj obj2;
          int64_t int2 = 0;
          int64_t fra2 = 0;
          int64_t p2 = 3;
          int64_t w2 = 6;
          EXPECT_EQ(0, obj2.set_decimal(int2, fra2, w2, p2));

          EXPECT_EQ(false, obj1 > obj2);
          EXPECT_EQ(true, obj2 > obj1);
        }

        //=============精度不同

        //精度大的值小
        {
          ObObj obj1;
          int64_t int1 = 45;
          int64_t fra1 = 3;
          int64_t p1 = 2;
          int64_t w1 = 6;
          EXPECT_EQ(0, obj1.set_decimal(int1, fra1, w1, p1, false));

          ObObj obj2;
          int64_t int2 = 24;
          int64_t fra2 = 2;
          int64_t p2 = 3;
          int64_t w2 = 6;
          EXPECT_EQ(0, obj2.set_decimal(int2, fra2, w2, p2));

          EXPECT_EQ(false, obj1 > obj2);
        }

        //精度大的值大
        {
          ObObj obj1;
          int64_t int1 = 45;
          int64_t fra1 = 3;
          int64_t p1 = 2;
          int64_t w1 = 6;
          EXPECT_EQ(0, obj1.set_decimal(int1, fra1, w1, p1, false));

          ObObj obj2;
          int64_t int2 = 78;
          int64_t fra2 = 75;
          int64_t p2 = 3;
          int64_t w2 = 6;
          EXPECT_EQ(0, obj2.set_decimal(int2, fra2, w2, p2));

          EXPECT_EQ(false, obj1 > obj2);
        }

        //小数相同，精度不同
        {
          ObObj obj1;
          int64_t int1 = 45;
          int64_t fra1 = 3;
          int64_t p1 = 2;
          int64_t w1 = 6;
          EXPECT_EQ(0, obj1.set_decimal(int1, fra1, w1, p1));

          ObObj obj2;
          int64_t int2 = 45;
          int64_t fra2 = 30;
          int64_t p2 = 3;
          int64_t w2 = 6;
          EXPECT_EQ(0, obj2.set_decimal(int2, fra2, w2, p2, false));

          EXPECT_EQ(true, obj1 > obj2);
        }

        //整数大，小精度，值大
        {
          ObObj obj1;
          int64_t int1 = 45;
          int64_t fra1 = 3;
          int64_t p1 = 2;
          int64_t w1 = 6;
          EXPECT_EQ(0, obj1.set_decimal(int1, fra1, w1, p1));

          ObObj obj2;
          int64_t int2 = 41;
          int64_t fra2 = 30;
          int64_t p2 = 3;
          int64_t w2 = 6;
          EXPECT_EQ(0, obj2.set_decimal(int2, fra2, w2, p2, false));

          EXPECT_EQ(true, obj1 > obj2);
        }

        //整数小，大精度，值小
        {
          ObObj obj1;
          int64_t int1 = 45;
          int64_t fra1 = 3;
          int64_t p1 = 2;
          int64_t w1 = 6;
          EXPECT_EQ(0, obj1.set_decimal(int1, fra1, w1, p1));

          ObObj obj2;
          int64_t int2 = 48;
          int64_t fra2 = 30;
          int64_t p2 = 3;
          int64_t w2 = 6;
          EXPECT_EQ(0, obj2.set_decimal(int2, fra2, w2, p2, false));

          EXPECT_EQ(true, obj1 > obj2);;
        }

        //小数为0,精度不同
        {
          ObObj obj1;
          int64_t int1 = 45;
          int64_t fra1 = 0;
          int64_t p1 = 2;
          int64_t w1 = 6;
          EXPECT_EQ(0, obj1.set_decimal(int1, fra1, w1, p1));

          ObObj obj2;
          int64_t int2 = 45;
          int64_t fra2 = 0;
          int64_t p2 = 3;
          int64_t w2 = 6;
          EXPECT_EQ(0, obj2.set_decimal(int2, fra2, w2, p2, false));

          EXPECT_EQ(true, obj1 > obj2);
        }

        //====================
        //两个负定点数
        //====================

        //=========精度相同

        //整数大，小数小
        {
          ObObj obj1;
          int64_t int1 = 49;
          int64_t fra1 = 3;
          int64_t p1 = 3;
          int64_t w1 = 6;
          EXPECT_EQ(0, obj1.set_decimal(int1, fra1, w1, p1, false));

          ObObj obj2;
          int64_t int2 = 45;
          int64_t fra2 = 30;
          int64_t p2 = 3;
          int64_t w2 = 6;
          EXPECT_EQ(0, obj2.set_decimal(int2, fra2, w2, p2, false));

          EXPECT_EQ(false, obj1 > obj2);
        }

        //整数小，小数大
        {
          ObObj obj1;
          int64_t int1 = 34;
          int64_t fra1 = 35;
          int64_t p1 = 3;
          int64_t w1 = 6;
          EXPECT_EQ(0, obj1.set_decimal(int1, fra1, w1, p1, false));

          ObObj obj2;
          int64_t int2 = 45;
          int64_t fra2 = 30;
          int64_t p2 = 3;
          int64_t w2 = 6;
          EXPECT_EQ(0, obj2.set_decimal(int2, fra2, w2, p2, false));

          EXPECT_EQ(true, obj1 > obj2);
        }

        //整数小，小数大
        {
          ObObj obj1;
          int64_t int1 = 49;
          int64_t fra1 = 37;
          int64_t p1 = 3;
          int64_t w1 = 6;
          EXPECT_EQ(0, obj1.set_decimal(int1, fra1, w1, p1, false));

          ObObj obj2;
          int64_t int2 = 74;
          int64_t fra2 = 30;
          int64_t p2 = 3;
          int64_t w2 = 6;
          EXPECT_EQ(0, obj2.set_decimal(int2, fra2, w2, p2, false));

          EXPECT_EQ(true, obj1 > obj2);
        }

        //整数相同，小数不同
        {
          ObObj obj1;
          int64_t int1 = 45;
          int64_t fra1 = 3;
          int64_t p1 = 3;
          int64_t w1 = 6;
          EXPECT_EQ(0, obj1.set_decimal(int1, fra1, w1, p1, false));

          ObObj obj2;
          int64_t int2 = 45;
          int64_t fra2 = 30;
          int64_t p2 = 3;
          int64_t w2 = 6;
          EXPECT_EQ(0, obj2.set_decimal(int2, fra2, w2, p2, false));

          EXPECT_EQ(true, obj1 > obj2);
        }

        //相等的定点数
        {
          ObObj obj1;
          int64_t int1 = 49;
          int64_t fra1 = 30;
          int64_t p1 = 3;
          int64_t w1 = 6;
          EXPECT_EQ(0, obj1.set_decimal(int1, fra1, w1, p1, false));

          ObObj obj2;
          int64_t int2 = 49;
          int64_t fra2 = 30;
          int64_t p2 = 3;
          int64_t w2 = 6;
          EXPECT_EQ(0, obj2.set_decimal(int2, fra2, w2, p2, false));

          EXPECT_EQ(false, obj1 > obj2);
        }

        //整数为0  小数为0
        {
          ObObj obj1;
          int64_t int1 = 0;
          int64_t fra1 = 0;
          int64_t p1 = 3;
          int64_t w1 = 6;
          EXPECT_EQ(0, obj1.set_decimal(int1, fra1, w1, p1, false));

          ObObj obj2;
          int64_t int2 = 0;
          int64_t fra2 = 0;
          int64_t p2 = 3;
          int64_t w2 = 6;
          EXPECT_EQ(0, obj2.set_decimal(int2, fra2, w2, p2, false));

          EXPECT_EQ(false, obj1 > obj2);
        }

        //=============精度不同

        //精度大的值小
        {
          ObObj obj1;
          int64_t int1 = 49;
          int64_t fra1 = 3;
          int64_t p1 = 2;
          int64_t w1 = 6;
          EXPECT_EQ(0, obj1.set_decimal(int1, fra1, w1, p1, false));

          ObObj obj2;
          int64_t int2 = 54;
          int64_t fra2 = 30;
          int64_t p2 = 2;
          int64_t w2 = 6;
          EXPECT_EQ(0, obj2.set_decimal(int2, fra2, w2, p2, false));

          EXPECT_EQ(true, obj1 > obj2);
        }

        //精度大的值大
        {
          ObObj obj1;
          int64_t int1 = 49;
          int64_t fra1 = 3;
          int64_t p1 = 2;
          int64_t w1 = 6;
          EXPECT_EQ(0, obj1.set_decimal(int1, fra1, w1, p1, false));

          ObObj obj2;
          int64_t int2 = 45;
          int64_t fra2 = 32;
          int64_t p2 = 3;
          int64_t w2 = 6;
          EXPECT_EQ(0, obj2.set_decimal(int2, fra2, w2, p2, false));

          EXPECT_EQ(false, obj1 > obj2);
        }

        //小数相同，精度不同
        {
          ObObj obj1;
          int64_t int1 = 45;
          int64_t fra1 = 3;
          int64_t p1 = 2;
          int64_t w1 = 6;
          EXPECT_EQ(0, obj1.set_decimal(int1, fra1, w1, p1, false));

          ObObj obj2;
          int64_t int2 = 45;
          int64_t fra2 = 30;
          int64_t p2 = 3;
          int64_t w2 = 6;
          EXPECT_EQ(0, obj2.set_decimal(int2, fra2, w2, p2, false));

          EXPECT_EQ(false, obj1 > obj2);
        }

        //整数大，小精度，值大
        {
          ObObj obj1;
          int64_t int1 = 49;
          int64_t fra1 = 3;
          int64_t p1 = 2;
          int64_t w1 = 6;
          EXPECT_EQ(0, obj1.set_decimal(int1, fra1, w1, p1, false));

          ObObj obj2;
          int64_t int2 = 45;
          int64_t fra2 = 39;
          int64_t p2 = 3;
          int64_t w2 = 6;
          EXPECT_EQ(0, obj2.set_decimal(int2, fra2, w2, p2, false));

          EXPECT_EQ(false, obj1 > obj2);
        }

        //整数小，大精度，值小
        {
          ObObj obj1;
          int64_t int1 = 43;
          int64_t fra1 = 32;
          int64_t p1 = 2;
          int64_t w1 = 6;
          EXPECT_EQ(0, obj1.set_decimal(int1, fra1, w1, p1, false));

          ObObj obj2;
          int64_t int2 = 45;
          int64_t fra2 = 30;
          int64_t p2 = 3;
          int64_t w2 = 6;
          EXPECT_EQ(0, obj2.set_decimal(int2, fra2, w2, p2, false));

          EXPECT_EQ(true, obj1 > obj2);
        }

        //小数为0,精度不同
        {
          ObObj obj1;
          int64_t int1 = 49;
          int64_t fra1 = 0;
          int64_t p1 = 2;
          int64_t w1 = 6;
          EXPECT_EQ(0, obj1.set_decimal(int1, fra1, w1, p1, false));

          ObObj obj2;
          int64_t int2 = 49;
          int64_t fra2 = 0;
          int64_t p2 = 3;
          int64_t w2 = 6;
          EXPECT_EQ(0, obj2.set_decimal(int2, fra2, w2, p2, false));

          EXPECT_EQ(false, obj1 > obj2);
        }
      }

      //bool ObObj::operator<=(const ObObj &that_obj) const
      TEST_F(TestObjDecimal, le)
      {
        //=================
        //符号相同，精度相同
        //==================

        //正号
        //整数大于，小数小于
        {
          ObObj obj1;
          int64_t int1 = 49;
          int64_t fra1 = 23;
          int64_t p1 = 3;
          int64_t w1 = 6;
          EXPECT_EQ(0, obj1.set_decimal(int1, fra1, w1, p1));

          ObObj obj2;
          int64_t int2 = 43;
          int64_t fra2 = 34;
          int64_t p2 = 3;
          int64_t w2 = 6;
          EXPECT_EQ(0, obj2.set_decimal(int2, fra2, w2, p2));

          EXPECT_EQ(false, obj1 <= obj2);
        }
        //整数大于，小数大于
        {
          ObObj obj1;
          int64_t int1 = 49;
          int64_t fra1 = 43;
          int64_t p1 = 3;
          int64_t w1 = 6;
          EXPECT_EQ(0, obj1.set_decimal(int1, fra1, w1, p1));

          ObObj obj2;
          int64_t int2 = 43;
          int64_t fra2 = 34;
          int64_t p2 = 3;
          int64_t w2 = 6;
          EXPECT_EQ(0, obj2.set_decimal(int2, fra2, w2, p2));

          EXPECT_EQ(false, obj1 <= obj2);
        }
        //整数小于，小数小于
        {
          ObObj obj1;
          int64_t int1 = 49;
          int64_t fra1 = 43;
          int64_t p1 = 3;
          int64_t w1 = 6;
          EXPECT_EQ(0, obj1.set_decimal(int1, fra1, w1, p1));

          ObObj obj2;
          int64_t int2 = 654;
          int64_t fra2 = 54;
          int64_t p2 = 3;
          int64_t w2 = 6;
          EXPECT_EQ(0, obj2.set_decimal(int2, fra2, w2, p2));

          EXPECT_EQ(true, obj1 <= obj2);
        }
        //整数小于，小数小于
        {
          ObObj obj1;
          int64_t int1 = 49;
          int64_t fra1 = 43;
          int64_t p1 = 3;
          int64_t w1 = 6;
          EXPECT_EQ(0, obj1.set_decimal(int1, fra1, w1, p1));

          ObObj obj2;
          int64_t int2 = 76;
          int64_t fra2 = 74;
          int64_t p2 = 3;
          int64_t w2 = 6;
          EXPECT_EQ(0, obj2.set_decimal(int2, fra2, w2, p2));

          EXPECT_EQ(true, obj1 <= obj2);
        }
        //整数等于，小数小于
        {
          ObObj obj1;
          int64_t int1 = 49;
          int64_t fra1 = 23;
          int64_t p1 = 3;
          int64_t w1 = 6;
          EXPECT_EQ(0, obj1.set_decimal(int1, fra1, w1, p1));

          ObObj obj2;
          int64_t int2 = 49;
          int64_t fra2 = 34;
          int64_t p2 = 3;
          int64_t w2 = 6;
          EXPECT_EQ(0, obj2.set_decimal(int2, fra2, w2, p2));

          EXPECT_EQ(true, obj1 <= obj2);
        }
        //整数等于，小数大于
        {
          ObObj obj1;
          int64_t int1 = 49;
          int64_t fra1 = 76;
          int64_t p1 = 3;
          int64_t w1 = 6;
          EXPECT_EQ(0, obj1.set_decimal(int1, fra1, w1, p1));

          ObObj obj2;
          int64_t int2 = 49;
          int64_t fra2 = 34;
          int64_t p2 = 3;
          int64_t w2 = 6;
          EXPECT_EQ(0, obj2.set_decimal(int2, fra2, w2, p2));

          EXPECT_EQ(false, obj1 <= obj2);
        }
        //整数等于，小数等于
        {
          ObObj obj1;
          int64_t int1 = 49;
          int64_t fra1 = 43;
          int64_t p1 = 3;
          int64_t w1 = 6;
          EXPECT_EQ(0, obj1.set_decimal(int1, fra1, w1, p1));

          ObObj obj2;
          int64_t int2 = 49;
          int64_t fra2 = 43;
          int64_t p2 = 3;
          int64_t w2 = 6;
          EXPECT_EQ(0, obj2.set_decimal(int2, fra2, w2, p2));

          EXPECT_EQ(true, obj1 <= obj2);
        }

        // 负号
        //整数大于，小数小于
        {
          ObObj obj1;
          int64_t int1 = 76;
          int64_t fra1 = 42;
          int64_t p1 = 3;
          int64_t w1 = 6;
          EXPECT_EQ(0, obj1.set_decimal(int1, fra1, w1, p1, false));

          ObObj obj2;
          int64_t int2 = 49;
          int64_t fra2 = 43;
          int64_t p2 = 3;
          int64_t w2 = 6;
          EXPECT_EQ(0, obj2.set_decimal(int2, fra2, w2, p2, false));

          EXPECT_EQ(true, obj1 <= obj2);
        }
        //整数大于，小数大于
        {
          ObObj obj1;
          int64_t int1 = 56;
          int64_t fra1 = 47;
          int64_t p1 = 3;
          int64_t w1 = 6;
          EXPECT_EQ(0, obj1.set_decimal(int1, fra1, w1, p1, false));

          ObObj obj2;
          int64_t int2 = 49;
          int64_t fra2 = 43;
          int64_t p2 = 3;
          int64_t w2 = 6;
          EXPECT_EQ(0, obj2.set_decimal(int2, fra2, w2, p2, false));

          EXPECT_EQ(true, obj1 <= obj2);
        }
        //整数小于，小数大于
        {
          ObObj obj1;
          int64_t int1 = 34;
          int64_t fra1 = 76;
          int64_t p1 = 3;
          int64_t w1 = 6;
          EXPECT_EQ(0, obj1.set_decimal(int1, fra1, w1, p1, false));

          ObObj obj2;
          int64_t int2 = 49;
          int64_t fra2 = 43;
          int64_t p2 = 3;
          int64_t w2 = 6;
          EXPECT_EQ(0, obj2.set_decimal(int2, fra2, w2, p2, false));

          EXPECT_EQ(false, obj1 <= obj2);
        }
        //整数小于，小数小于
        {
          ObObj obj1;
          int64_t int1 = 34;
          int64_t fra1 = 32;
          int64_t p1 = 3;
          int64_t w1 = 6;
          EXPECT_EQ(0, obj1.set_decimal(int1, fra1, w1, p1, false));

          ObObj obj2;
          int64_t int2 = 49;
          int64_t fra2 = 43;
          int64_t p2 = 3;
          int64_t w2 = 6;
          EXPECT_EQ(0, obj2.set_decimal(int2, fra2, w2, p2, false));

          EXPECT_EQ(false, obj1 <= obj2);
        }
        //整数等于，小数大于
        {
          ObObj obj1;
          int64_t int1 = 49;
          int64_t fra1 = 87;
          int64_t p1 = 3;
          int64_t w1 = 6;
          EXPECT_EQ(0, obj1.set_decimal(int1, fra1, w1, p1, false));

          ObObj obj2;
          int64_t int2 = 49;
          int64_t fra2 = 43;
          int64_t p2 = 3;
          int64_t w2 = 6;
          EXPECT_EQ(0, obj2.set_decimal(int2, fra2, w2, p2, false));

          EXPECT_EQ(true, obj1 <= obj2);
        }
        //整数等于，小数小于
        {
          ObObj obj1;
          int64_t int1 = 49;
          int64_t fra1 = 2;
          int64_t p1 = 3;
          int64_t w1 = 6;
          EXPECT_EQ(0, obj1.set_decimal(int1, fra1, w1, p1, false));

          ObObj obj2;
          int64_t int2 = 49;
          int64_t fra2 = 43;
          int64_t p2 = 3;
          int64_t w2 = 6;
          EXPECT_EQ(0, obj2.set_decimal(int2, fra2, w2, p2, false));

          EXPECT_EQ(false, obj1 <= obj2);
        }
        //整数等于，小数等于
        {
          ObObj obj1;
          int64_t int1 = 49;
          int64_t fra1 = 43;
          int64_t p1 = 3;
          int64_t w1 = 6;
          EXPECT_EQ(0, obj1.set_decimal(int1, fra1, w1, p1, false));

          ObObj obj2;
          int64_t int2 = 49;
          int64_t fra2 = 43;
          int64_t p2 = 3;
          int64_t w2 = 6;
          EXPECT_EQ(0, obj2.set_decimal(int2, fra2, w2, p2, false));

          EXPECT_EQ(true, obj1 <= obj2);
        }

        //=================
        //符号不相同，精度相同
        //=================
        //前正后负
        //整数大于，小数小于
        {
          ObObj obj1;
          int64_t int1 = 49;
          int64_t fra1 = 23;
          int64_t p1 = 3;
          int64_t w1 = 6;
          EXPECT_EQ(0, obj1.set_decimal(int1, fra1, w1, p1));

          ObObj obj2;
          int64_t int2 = 23;
          int64_t fra2 = 43;
          int64_t p2 = 3;
          int64_t w2 = 6;
          EXPECT_EQ(0, obj2.set_decimal(int2, fra2, w2, p2, false));

          EXPECT_EQ(false, obj1 <= obj2);
        }
        //整数大于，小数大于
        {
          ObObj obj1;
          int64_t int1 = 49;
          int64_t fra1 = 65;
          int64_t p1 = 3;
          int64_t w1 = 6;
          EXPECT_EQ(0, obj1.set_decimal(int1, fra1, w1, p1));

          ObObj obj2;
          int64_t int2 = 23;
          int64_t fra2 = 43;
          int64_t p2 = 3;
          int64_t w2 = 6;
          EXPECT_EQ(0, obj2.set_decimal(int2, fra2, w2, p2, false));

          EXPECT_EQ(false, obj1 <= obj2);
        }
        //整数小于，小数小于
        {
          ObObj obj1;
          int64_t int1 = 49;
          int64_t fra1 = 23;
          int64_t p1 = 3;
          int64_t w1 = 6;
          EXPECT_EQ(0, obj1.set_decimal(int1, fra1, w1, p1));

          ObObj obj2;
          int64_t int2 = 76;
          int64_t fra2 = 43;
          int64_t p2 = 3;
          int64_t w2 = 6;
          EXPECT_EQ(0, obj2.set_decimal(int2, fra2, w2, p2, false));

          EXPECT_EQ(false, obj1 <= obj2);
        }
        //整数小于，小数小于
        {
          ObObj obj1;
          int64_t int1 = 49;
          int64_t fra1 = 23;
          int64_t p1 = 3;
          int64_t w1 = 6;
          EXPECT_EQ(0, obj1.set_decimal(int1, fra1, w1, p1));

          ObObj obj2;
          int64_t int2 = 87;
          int64_t fra2 = 87;
          int64_t p2 = 3;
          int64_t w2 = 6;
          EXPECT_EQ(0, obj2.set_decimal(int2, fra2, w2, p2, false));

          EXPECT_EQ(false, obj1 <= obj2);
        }
        //整数等于，小数等于
        {
          ObObj obj1;
          int64_t int1 = 49;
          int64_t fra1 = 23;
          int64_t p1 = 3;
          int64_t w1 = 6;
          EXPECT_EQ(0, obj1.set_decimal(int1, fra1, w1, p1));

          ObObj obj2;
          int64_t int2 = 49;
          int64_t fra2 = 23;
          int64_t p2 = 3;
          int64_t w2 = 6;
          EXPECT_EQ(0, obj2.set_decimal(int2, fra2, w2, p2, false));

          EXPECT_EQ(false, obj1 <= obj2);
        }

        //前负后正
        //整数大于，小数小于
        {
          ObObj obj1;
          int64_t int1 = 49;
          int64_t fra1 = 23;
          int64_t p1 = 3;
          int64_t w1 = 6;
          EXPECT_EQ(0, obj1.set_decimal(int1, fra1, w1, p1, false));

          ObObj obj2;
          int64_t int2 = 23;
          int64_t fra2 = 43;
          int64_t p2 = 3;
          int64_t w2 = 6;
          EXPECT_EQ(0, obj2.set_decimal(int2, fra2, w2, p2));

          EXPECT_EQ(true, obj1 <= obj2);
        }
        //整数大于，小数大于
        {
          ObObj obj1;
          int64_t int1 = 49;
          int64_t fra1 = 76;
          int64_t p1 = 3;
          int64_t w1 = 6;
          EXPECT_EQ(0, obj1.set_decimal(int1, fra1, w1, p1, false));

          ObObj obj2;
          int64_t int2 = 23;
          int64_t fra2 = 43;
          int64_t p2 = 3;
          int64_t w2 = 6;
          EXPECT_EQ(0, obj2.set_decimal(int2, fra2, w2, p2));

          EXPECT_EQ(true, obj1 <= obj2);
        }
        //整数小于，小数小于
        {
          ObObj obj1;
          int64_t int1 = 49;
          int64_t fra1 = 23;
          int64_t p1 = 3;
          int64_t w1 = 6;
          EXPECT_EQ(0, obj1.set_decimal(int1, fra1, w1, p1, false));

          ObObj obj2;
          int64_t int2 = 87;
          int64_t fra2 = 43;
          int64_t p2 = 3;
          int64_t w2 = 6;
          EXPECT_EQ(0, obj2.set_decimal(int2, fra2, w2, p2));

          EXPECT_EQ(true, obj1 <= obj2);
        }
        //整数小于，小数小于
        {
          ObObj obj1;
          int64_t int1 = 49;
          int64_t fra1 = 23;
          int64_t p1 = 3;
          int64_t w1 = 6;
          EXPECT_EQ(0, obj1.set_decimal(int1, fra1, w1, p1, false));

          ObObj obj2;
          int64_t int2 = 78;
          int64_t fra2 = 43;
          int64_t p2 = 3;
          int64_t w2 = 6;
          EXPECT_EQ(0, obj2.set_decimal(int2, fra2, w2, p2));

          EXPECT_EQ(true, obj1 <= obj2);
        }
        //整数等于，小数等于
        {
          ObObj obj1;
          int64_t int1 = 49;
          int64_t fra1 = 23;
          int64_t p1 = 3;
          int64_t w1 = 6;
          EXPECT_EQ(0, obj1.set_decimal(int1, fra1, w1, p1, false));

          ObObj obj2;
          int64_t int2 = 49;
          int64_t fra2 = 23;
          int64_t p2 = 3;
          int64_t w2 = 6;
          EXPECT_EQ(0, obj2.set_decimal(int2, fra2, w2, p2));

          EXPECT_EQ(true, obj1 <= obj2);
        }
        //整数等于，小数小于
        {
          ObObj obj1;
          int64_t int1 = 49;
          int64_t fra1 = 23;
          int64_t p1 = 3;
          int64_t w1 = 6;
          EXPECT_EQ(0, obj1.set_decimal(int1, fra1, w1, p1, false));

          ObObj obj2;
          int64_t int2 = 49;
          int64_t fra2 = 43;
          int64_t p2 = 3;
          int64_t w2 = 6;
          EXPECT_EQ(0, obj2.set_decimal(int2, fra2, w2, p2));

          EXPECT_EQ(true, obj1 <= obj2);
        }
        //整数等于，小数大于
        {
          ObObj obj1;
          int64_t int1 = 23;
          int64_t fra1 = 76;
          int64_t p1 = 3;
          int64_t w1 = 6;
          EXPECT_EQ(0, obj1.set_decimal(int1, fra1, w1, p1, false));

          ObObj obj2;
          int64_t int2 = 23;
          int64_t fra2 = 43;
          int64_t p2 = 3;
          int64_t w2 = 6;
          EXPECT_EQ(0, obj2.set_decimal(int2, fra2, w2, p2));

          EXPECT_EQ(true, obj1 <= obj2);
        }
        //===============
        //精度不相同
        //==============

        //精度大，值小
        {
          ObObj obj1;
          int64_t int1 = 23;
          int64_t fra1 = 23;
          int64_t p1 = 2;
          int64_t w1 = 6;
          EXPECT_EQ(0, obj1.set_decimal(int1, fra1, w1, p1));

          ObObj obj2;
          int64_t int2 = 23;
          int64_t fra2 = 43;
          int64_t p2 = 3;
          int64_t w2 = 6;
          EXPECT_EQ(0, obj2.set_decimal(int2, fra2, w2, p2));

          EXPECT_EQ(false, obj1 <= obj2);
        }
        //精度大，值大
        {
          ObObj obj1;
          int64_t int1 = 49;
          int64_t fra1 = 3;
          int64_t p1 = 2;
          int64_t w1 = 6;
          EXPECT_EQ(0, obj1.set_decimal(int1, fra1, w1, p1));

          ObObj obj2;
          int64_t int2 =49;
          int64_t fra2 = 28;
          int64_t p2 = 3;
          int64_t w2 = 6;
          EXPECT_EQ(0, obj2.set_decimal(int2, fra2, w2, p2));

          EXPECT_EQ(false, obj1 <= obj2);
        }
        {
          ObObj obj1;
          int64_t int1 = 49;
          int64_t fra1 = 3;
          int64_t p1 = 2;
          int64_t w1 = 6;
          EXPECT_EQ(0, obj1.set_decimal(int1, fra1, w1, p1));

          ObObj obj2;
          int64_t int2 =49;
          int64_t fra2 = 31;
          int64_t p2 = 3;
          int64_t w2 = 6;
          EXPECT_EQ(0, obj2.set_decimal(int2, fra2, w2, p2));

          EXPECT_EQ(true, obj1 <= obj2);
        }
        //======
        //精度大，值小
        {
          ObObj obj1;
          int64_t int1 = 23;
          int64_t fra1 = 23;
          int64_t p1 = 2;
          int64_t w1 = 6;
          EXPECT_EQ(0, obj1.set_decimal(int1, fra1, w1, p1, false));

          ObObj obj2;
          int64_t int2 = 23;
          int64_t fra2 = 43;
          int64_t p2 = 3;
          int64_t w2 = 6;
          EXPECT_EQ(0, obj2.set_decimal(int2, fra2, w2, p2, false));

          EXPECT_EQ(true, obj1 <= obj2);
        }
        //精度大，值大
        {
          ObObj obj1;
          int64_t int1 = 49;
          int64_t fra1 = 3;
          int64_t p1 = 2;
          int64_t w1 = 6;
          EXPECT_EQ(0, obj1.set_decimal(int1, fra1, w1, p1, false));

          ObObj obj2;
          int64_t int2 =49;
          int64_t fra2 = 28;
          int64_t p2 = 3;
          int64_t w2 = 6;
          EXPECT_EQ(0, obj2.set_decimal(int2, fra2, w2, p2, false));

          EXPECT_EQ(true, obj1 <= obj2);
        }
        {
          ObObj obj1;
          int64_t int1 = 49;
          int64_t fra1 = 3;
          int64_t p1 = 2;
          int64_t w1 = 6;
          EXPECT_EQ(0, obj1.set_decimal(int1, fra1, w1, p1, false));

          ObObj obj2;
          int64_t int2 =49;
          int64_t fra2 = 31;
          int64_t p2 = 3;
          int64_t w2 = 6;
          EXPECT_EQ(0, obj2.set_decimal(int2, fra2, w2, p2, false));

          EXPECT_EQ(false, obj1 <= obj2);
        }
      }

      TEST_F(TestObjDecimal, ge)
      {
        //=================
        //符号相同，精度相同
        //==================

        //正号
        //整数大于，小数小于
        {
          ObObj obj1;
          int64_t int1 = 49;
          int64_t fra1 = 23;
          int64_t p1 = 3;
          int64_t w1 = 6;
          EXPECT_EQ(0, obj1.set_decimal(int1, fra1, w1, p1));

          ObObj obj2;
          int64_t int2 = 43;
          int64_t fra2 = 34;
          int64_t p2 = 3;
          int64_t w2 = 6;
          EXPECT_EQ(0, obj2.set_decimal(int2, fra2, w2, p2));

          EXPECT_EQ(true, obj1 >= obj2);
        }
        //整数大于，小数大于
        {
          ObObj obj1;
          int64_t int1 = 49;
          int64_t fra1 = 43;
          int64_t p1 = 3;
          int64_t w1 = 6;
          EXPECT_EQ(0, obj1.set_decimal(int1, fra1, w1, p1));

          ObObj obj2;
          int64_t int2 = 43;
          int64_t fra2 = 34;
          int64_t p2 = 3;
          int64_t w2 = 6;
          EXPECT_EQ(0, obj2.set_decimal(int2, fra2, w2, p2));

          EXPECT_EQ(true, obj1 >= obj2);
        }
        //整数小于，小数小于
        {
          ObObj obj1;
          int64_t int1 = 49;
          int64_t fra1 = 43;
          int64_t p1 = 3;
          int64_t w1 = 6;
          EXPECT_EQ(0, obj1.set_decimal(int1, fra1, w1, p1));

          ObObj obj2;
          int64_t int2 = 654;
          int64_t fra2 = 54;
          int64_t p2 = 3;
          int64_t w2 = 6;
          EXPECT_EQ(0, obj2.set_decimal(int2, fra2, w2, p2));

          EXPECT_EQ(false, obj1 >= obj2);
        }
        //整数小于，小数小于
        {
          ObObj obj1;
          int64_t int1 = 49;
          int64_t fra1 = 43;
          int64_t p1 = 3;
          int64_t w1 = 6;
          EXPECT_EQ(0, obj1.set_decimal(int1, fra1, w1, p1));

          ObObj obj2;
          int64_t int2 = 76;
          int64_t fra2 = 74;
          int64_t p2 = 3;
          int64_t w2 = 6;
          EXPECT_EQ(0, obj2.set_decimal(int2, fra2, w2, p2));

          EXPECT_EQ(false, obj1 >= obj2);
        }
        //整数等于，小数小于
        {
          ObObj obj1;
          int64_t int1 = 49;
          int64_t fra1 = 23;
          int64_t p1 = 3;
          int64_t w1 = 6;
          EXPECT_EQ(0, obj1.set_decimal(int1, fra1, w1, p1));

          ObObj obj2;
          int64_t int2 = 49;
          int64_t fra2 = 34;
          int64_t p2 = 3;
          int64_t w2 = 6;
          EXPECT_EQ(0, obj2.set_decimal(int2, fra2, w2, p2));

          EXPECT_EQ(false, obj1 >= obj2);
        }
        //整数等于，小数大于
        {
          ObObj obj1;
          int64_t int1 = 49;
          int64_t fra1 = 76;
          int64_t p1 = 3;
          int64_t w1 = 6;
          EXPECT_EQ(0, obj1.set_decimal(int1, fra1, w1, p1));

          ObObj obj2;
          int64_t int2 = 49;
          int64_t fra2 = 34;
          int64_t p2 = 3;
          int64_t w2 = 6;
          EXPECT_EQ(0, obj2.set_decimal(int2, fra2, w2, p2));

          EXPECT_EQ(true, obj1 >= obj2);
        }
        //整数等于，小数等于
        {
          ObObj obj1;
          int64_t int1 = 49;
          int64_t fra1 = 43;
          int64_t p1 = 3;
          int64_t w1 = 6;
          EXPECT_EQ(0, obj1.set_decimal(int1, fra1, w1, p1));

          ObObj obj2;
          int64_t int2 = 49;
          int64_t fra2 = 43;
          int64_t p2 = 3;
          int64_t w2 = 6;
          EXPECT_EQ(0, obj2.set_decimal(int2, fra2, w2, p2));

          EXPECT_EQ(true, obj1 >= obj2);
        }

        // 负号
        //整数大于，小数小于
        {
          ObObj obj1;
          int64_t int1 = 76;
          int64_t fra1 = 42;
          int64_t p1 = 3;
          int64_t w1 = 6;
          EXPECT_EQ(0, obj1.set_decimal(int1, fra1, w1, p1, false));

          ObObj obj2;
          int64_t int2 = 49;
          int64_t fra2 = 43;
          int64_t p2 = 3;
          int64_t w2 = 6;
          EXPECT_EQ(0, obj2.set_decimal(int2, fra2, w2, p2, false));

          EXPECT_EQ(false, obj1 >= obj2);
        }
        //整数大于，小数大于
        {
          ObObj obj1;
          int64_t int1 = 56;
          int64_t fra1 = 47;
          int64_t p1 = 3;
          int64_t w1 = 6;
          EXPECT_EQ(0, obj1.set_decimal(int1, fra1, w1, p1, false));

          ObObj obj2;
          int64_t int2 = 49;
          int64_t fra2 = 43;
          int64_t p2 = 3;
          int64_t w2 = 6;
          EXPECT_EQ(0, obj2.set_decimal(int2, fra2, w2, p2, false));

          EXPECT_EQ(false, obj1 >= obj2);
        }
        //整数小于，小数大于
        {
          ObObj obj1;
          int64_t int1 = 34;
          int64_t fra1 = 76;
          int64_t p1 = 3;
          int64_t w1 = 6;
          EXPECT_EQ(0, obj1.set_decimal(int1, fra1, w1, p1, false));

          ObObj obj2;
          int64_t int2 = 49;
          int64_t fra2 = 43;
          int64_t p2 = 3;
          int64_t w2 = 6;
          EXPECT_EQ(0, obj2.set_decimal(int2, fra2, w2, p2, false));

          EXPECT_EQ(true, obj1 >= obj2);
        }
        //整数小于，小数小于
        {
          ObObj obj1;
          int64_t int1 = 34;
          int64_t fra1 = 32;
          int64_t p1 = 3;
          int64_t w1 = 6;
          EXPECT_EQ(0, obj1.set_decimal(int1, fra1, w1, p1, false));

          ObObj obj2;
          int64_t int2 = 49;
          int64_t fra2 = 43;
          int64_t p2 = 3;
          int64_t w2 = 6;
          EXPECT_EQ(0, obj2.set_decimal(int2, fra2, w2, p2, false));

          EXPECT_EQ(true, obj1 >= obj2);
        }
        //整数等于，小数大于
        {
          ObObj obj1;
          int64_t int1 = 49;
          int64_t fra1 = 87;
          int64_t p1 = 3;
          int64_t w1 = 6;
          EXPECT_EQ(0, obj1.set_decimal(int1, fra1, w1, p1, false));

          ObObj obj2;
          int64_t int2 = 49;
          int64_t fra2 = 43;
          int64_t p2 = 3;
          int64_t w2 = 6;
          EXPECT_EQ(0, obj2.set_decimal(int2, fra2, w2, p2, false));

          EXPECT_EQ(false, obj1 >= obj2);
        }
        //整数等于，小数小于
        {
          ObObj obj1;
          int64_t int1 = 49;
          int64_t fra1 = 2;
          int64_t p1 = 3;
          int64_t w1 = 6;
          EXPECT_EQ(0, obj1.set_decimal(int1, fra1, w1, p1, false));

          ObObj obj2;
          int64_t int2 = 49;
          int64_t fra2 = 43;
          int64_t p2 = 3;
          int64_t w2 = 6;
          EXPECT_EQ(0, obj2.set_decimal(int2, fra2, w2, p2, false));

          EXPECT_EQ(true, obj1 >= obj2);
        }
        //整数等于，小数等于
        {
          ObObj obj1;
          int64_t int1 = 49;
          int64_t fra1 = 43;
          int64_t p1 = 3;
          int64_t w1 = 6;
          EXPECT_EQ(0, obj1.set_decimal(int1, fra1, w1, p1, false));

          ObObj obj2;
          int64_t int2 = 49;
          int64_t fra2 = 43;
          int64_t p2 = 3;
          int64_t w2 = 6;
          EXPECT_EQ(0, obj2.set_decimal(int2, fra2, w2, p2, false));

          EXPECT_EQ(true, obj1 >= obj2);
        }

        //=================
        //符号不相同，精度相同
        //=================
        //前正后负
        //整数大于，小数小于
        {
          ObObj obj1;
          int64_t int1 = 49;
          int64_t fra1 = 23;
          int64_t p1 = 3;
          int64_t w1 = 6;
          EXPECT_EQ(0, obj1.set_decimal(int1, fra1, w1, p1));

          ObObj obj2;
          int64_t int2 = 23;
          int64_t fra2 = 43;
          int64_t p2 = 3;
          int64_t w2 = 6;
          EXPECT_EQ(0, obj2.set_decimal(int2, fra2, w2, p2, false));

          EXPECT_EQ(true, obj1 >= obj2);
        }
        //整数大于，小数大于
        {
          ObObj obj1;
          int64_t int1 = 49;
          int64_t fra1 = 65;
          int64_t p1 = 3;
          int64_t w1 = 6;
          EXPECT_EQ(0, obj1.set_decimal(int1, fra1, w1, p1));

          ObObj obj2;
          int64_t int2 = 23;
          int64_t fra2 = 43;
          int64_t p2 = 3;
          int64_t w2 = 6;
          EXPECT_EQ(0, obj2.set_decimal(int2, fra2, w2, p2, false));

          EXPECT_EQ(true, obj1 >= obj2);
        }
        //整数小于，小数小于
        {
          ObObj obj1;
          int64_t int1 = 49;
          int64_t fra1 = 23;
          int64_t p1 = 3;
          int64_t w1 = 6;
          EXPECT_EQ(0, obj1.set_decimal(int1, fra1, w1, p1));

          ObObj obj2;
          int64_t int2 = 76;
          int64_t fra2 = 43;
          int64_t p2 = 3;
          int64_t w2 = 6;
          EXPECT_EQ(0, obj2.set_decimal(int2, fra2, w2, p2, false));

          EXPECT_EQ(true, obj1 >= obj2);
        }
        //整数小于，小数小于
        {
          ObObj obj1;
          int64_t int1 = 49;
          int64_t fra1 = 23;
          int64_t p1 = 3;
          int64_t w1 = 6;
          EXPECT_EQ(0, obj1.set_decimal(int1, fra1, w1, p1));

          ObObj obj2;
          int64_t int2 = 87;
          int64_t fra2 = 87;
          int64_t p2 = 3;
          int64_t w2 = 6;
          EXPECT_EQ(0, obj2.set_decimal(int2, fra2, w2, p2, false));

          EXPECT_EQ(true, obj1 >= obj2);
        }
        //整数等于，小数等于
        {
          ObObj obj1;
          int64_t int1 = 49;
          int64_t fra1 = 23;
          int64_t p1 = 3;
          int64_t w1 = 6;
          EXPECT_EQ(0, obj1.set_decimal(int1, fra1, w1, p1));

          ObObj obj2;
          int64_t int2 = 49;
          int64_t fra2 = 23;
          int64_t p2 = 3;
          int64_t w2 = 6;
          EXPECT_EQ(0, obj2.set_decimal(int2, fra2, w2, p2, false));

          EXPECT_EQ(true, obj1 >= obj2);
        }

        //前负后正
        //整数大于，小数小于
        {
          ObObj obj1;
          int64_t int1 = 49;
          int64_t fra1 = 23;
          int64_t p1 = 3;
          int64_t w1 = 6;
          EXPECT_EQ(0, obj1.set_decimal(int1, fra1, w1, p1, false));

          ObObj obj2;
          int64_t int2 = 23;
          int64_t fra2 = 43;
          int64_t p2 = 3;
          int64_t w2 = 6;
          EXPECT_EQ(0, obj2.set_decimal(int2, fra2, w2, p2));

          EXPECT_EQ(false, obj1 >= obj2);
        }
        //整数大于，小数大于
        {
          ObObj obj1;
          int64_t int1 = 49;
          int64_t fra1 = 76;
          int64_t p1 = 3;
          int64_t w1 = 6;
          EXPECT_EQ(0, obj1.set_decimal(int1, fra1, w1, p1, false));

          ObObj obj2;
          int64_t int2 = 23;
          int64_t fra2 = 43;
          int64_t p2 = 3;
          int64_t w2 = 6;
          EXPECT_EQ(0, obj2.set_decimal(int2, fra2, w2, p2));

          EXPECT_EQ(false, obj1 >= obj2);
        }
        //整数小于，小数小于
        {
          ObObj obj1;
          int64_t int1 = 49;
          int64_t fra1 = 23;
          int64_t p1 = 3;
          int64_t w1 = 6;
          EXPECT_EQ(0, obj1.set_decimal(int1, fra1, w1, p1, false));

          ObObj obj2;
          int64_t int2 = 87;
          int64_t fra2 = 43;
          int64_t p2 = 3;
          int64_t w2 = 6;
          EXPECT_EQ(0, obj2.set_decimal(int2, fra2, w2, p2));

          EXPECT_EQ(false, obj1 >= obj2);
        }
        //整数小于，小数小于
        {
          ObObj obj1;
          int64_t int1 = 49;
          int64_t fra1 = 23;
          int64_t p1 = 3;
          int64_t w1 = 6;
          EXPECT_EQ(0, obj1.set_decimal(int1, fra1, w1, p1, false));

          ObObj obj2;
          int64_t int2 = 78;
          int64_t fra2 = 43;
          int64_t p2 = 3;
          int64_t w2 = 6;
          EXPECT_EQ(0, obj2.set_decimal(int2, fra2, w2, p2));

          EXPECT_EQ(false, obj1 >= obj2);
        }
        //整数等于，小数等于
        {
          ObObj obj1;
          int64_t int1 = 49;
          int64_t fra1 = 23;
          int64_t p1 = 3;
          int64_t w1 = 6;
          EXPECT_EQ(0, obj1.set_decimal(int1, fra1, w1, p1, false));

          ObObj obj2;
          int64_t int2 = 49;
          int64_t fra2 = 23;
          int64_t p2 = 3;
          int64_t w2 = 6;
          EXPECT_EQ(0, obj2.set_decimal(int2, fra2, w2, p2));

          EXPECT_EQ(false, obj1 >= obj2);
        }
        //整数等于，小数小于
        {
          ObObj obj1;
          int64_t int1 = 49;
          int64_t fra1 = 23;
          int64_t p1 = 3;
          int64_t w1 = 6;
          EXPECT_EQ(0, obj1.set_decimal(int1, fra1, w1, p1, false));

          ObObj obj2;
          int64_t int2 = 49;
          int64_t fra2 = 43;
          int64_t p2 = 3;
          int64_t w2 = 6;
          EXPECT_EQ(0, obj2.set_decimal(int2, fra2, w2, p2));

          EXPECT_EQ(false, obj1 >= obj2);
        }
        //整数等于，小数大于
        {
          ObObj obj1;
          int64_t int1 = 23;
          int64_t fra1 = 76;
          int64_t p1 = 3;
          int64_t w1 = 6;
          EXPECT_EQ(0, obj1.set_decimal(int1, fra1, w1, p1, false));

          ObObj obj2;
          int64_t int2 = 23;
          int64_t fra2 = 43;
          int64_t p2 = 3;
          int64_t w2 = 6;
          EXPECT_EQ(0, obj2.set_decimal(int2, fra2, w2, p2));

          EXPECT_EQ(false, obj1 >= obj2);
        }
        //===============
        //精度不相同
        //==============

        //精度大，值小
        {
          ObObj obj1;
          int64_t int1 = 23;
          int64_t fra1 = 23;
          int64_t p1 = 2;
          int64_t w1 = 6;
          EXPECT_EQ(0, obj1.set_decimal(int1, fra1, w1, p1));

          ObObj obj2;
          int64_t int2 = 23;
          int64_t fra2 = 43;
          int64_t p2 = 3;
          int64_t w2 = 6;
          EXPECT_EQ(0, obj2.set_decimal(int2, fra2, w2, p2));

          EXPECT_EQ(true, obj1 >= obj2);
        }
        //精度大，值大
        {
          ObObj obj1;
          int64_t int1 = 49;
          int64_t fra1 = 3;
          int64_t p1 = 2;
          int64_t w1 = 6;
          EXPECT_EQ(0, obj1.set_decimal(int1, fra1, w1, p1));

          ObObj obj2;
          int64_t int2 =49;
          int64_t fra2 = 28;
          int64_t p2 = 3;
          int64_t w2 = 6;
          EXPECT_EQ(0, obj2.set_decimal(int2, fra2, w2, p2));

          EXPECT_EQ(true, obj1 >= obj2);
        }
        {
          ObObj obj1;
          int64_t int1 = 49;
          int64_t fra1 = 3;
          int64_t p1 = 2;
          int64_t w1 = 6;
          EXPECT_EQ(0, obj1.set_decimal(int1, fra1, w1, p1));

          ObObj obj2;
          int64_t int2 =49;
          int64_t fra2 = 31;
          int64_t p2 = 3;
          int64_t w2 = 6;
          EXPECT_EQ(0, obj2.set_decimal(int2, fra2, w2, p2));

          EXPECT_EQ(false, obj1 >= obj2);
        }
        //======
        //精度大，值小
        {
          ObObj obj1;
          int64_t int1 = 23;
          int64_t fra1 = 23;
          int64_t p1 = 2;
          int64_t w1 = 6;
          EXPECT_EQ(0, obj1.set_decimal(int1, fra1, w1, p1, false));

          ObObj obj2;
          int64_t int2 = 23;
          int64_t fra2 = 43;
          int64_t p2 = 3;
          int64_t w2 = 6;
          EXPECT_EQ(0, obj2.set_decimal(int2, fra2, w2, p2, false));

          EXPECT_EQ(false, obj1 >= obj2);
        }
        //精度大，值大
        {
          ObObj obj1;
          int64_t int1 = 49;
          int64_t fra1 = 3;
          int64_t p1 = 2;
          int64_t w1 = 6;
          EXPECT_EQ(0, obj1.set_decimal(int1, fra1, w1, p1, false));

          ObObj obj2;
          int64_t int2 =49;
          int64_t fra2 = 28;
          int64_t p2 = 3;
          int64_t w2 = 6;
          EXPECT_EQ(0, obj2.set_decimal(int2, fra2, w2, p2, false));

          EXPECT_EQ(false, obj1 >= obj2);
        }
        {
          ObObj obj1;
          int64_t int1 = 49;
          int64_t fra1 = 3;
          int64_t p1 = 2;
          int64_t w1 = 6;
          EXPECT_EQ(0, obj1.set_decimal(int1, fra1, w1, p1, false));

          ObObj obj2;
          int64_t int2 =49;
          int64_t fra2 = 31;
          int64_t p2 = 3;
          int64_t w2 = 6;
          EXPECT_EQ(0, obj2.set_decimal(int2, fra2, w2, p2, false));

          EXPECT_EQ(true, obj1 >= obj2);
        }
      }

      TEST_F(TestObjDecimal, equal)
      {
        //精度相同
        {
          ObObj obj1;
          int64_t int1 = 49;
          int64_t fra1 = 31;
          int64_t p1 = 3;
          int64_t w1 = 6;
          EXPECT_EQ(0, obj1.set_decimal(int1, fra1, w1, p1, false));

          ObObj obj2;
          int64_t int2 =49;
          int64_t fra2 = 31;
          int64_t p2 = 3;
          int64_t w2 = 6;
          EXPECT_EQ(0, obj2.set_decimal(int2, fra2, w2, p2, false));

          EXPECT_EQ(true, obj1 == obj2);
        }

        {
          ObObj obj1;
          int64_t int1 = 43;
          int64_t fra1 = 31;
          int64_t p1 = 3;
          int64_t w1 = 6;
          EXPECT_EQ(0, obj1.set_decimal(int1, fra1, w1, p1, false));

          ObObj obj2;
          int64_t int2 =49;
          int64_t fra2 = 31;
          int64_t p2 = 3;
          int64_t w2 = 6;
          EXPECT_EQ(0, obj2.set_decimal(int2, fra2, w2, p2, false));

          EXPECT_EQ(false, obj1 == obj2);
        }

        {
          ObObj obj1;
          int64_t int1 = 49;
          int64_t fra1 = 45;
          int64_t p1 = 3;
          int64_t w1 = 6;
          EXPECT_EQ(0, obj1.set_decimal(int1, fra1, w1, p1, false));

          ObObj obj2;
          int64_t int2 =49;
          int64_t fra2 = 31;
          int64_t p2 = 3;
          int64_t w2 = 6;
          EXPECT_EQ(0, obj2.set_decimal(int2, fra2, w2, p2, false));

          EXPECT_EQ(false, obj1 == obj2);
        }

        {
          ObObj obj1;
          int64_t int1 = 49;
          int64_t fra1 = 45;
          int64_t p1 = 3;
          int64_t w1 = 6;
          EXPECT_EQ(0, obj1.set_decimal(int1, fra1, w1, p1));

          ObObj obj2;
          int64_t int2 =49;
          int64_t fra2 = 31;
          int64_t p2 = 3;
          int64_t w2 = 6;
          EXPECT_EQ(0, obj2.set_decimal(int2, fra2, w2, p2));

          EXPECT_EQ(false, obj1 == obj2);
        }
        {
          ObObj obj1;
          int64_t int1 = 49;
          int64_t fra1 = 45;
          int64_t p1 = 3;
          int64_t w1 = 6;
          EXPECT_EQ(0, obj1.set_decimal(int1, fra1, w1, p1));

          ObObj obj2;
          int64_t int2 = 49;
          int64_t fra2 = 45;
          int64_t p2 = 3;
          int64_t w2 = 6;
          EXPECT_EQ(0, obj2.set_decimal(int2, fra2, w2, p2));

          EXPECT_EQ(true, obj1 == obj2);
        }

        {
          ObObj obj1;
          int64_t int1 = 49;
          int64_t fra1 = 45;
          int64_t p1 = 3;
          int64_t w1 = 6;
          EXPECT_EQ(0, obj1.set_decimal(int1, fra1, w1, p1, false));

          ObObj obj2;
          int64_t int2 = 49;
          int64_t fra2 = 45;
          int64_t p2 = 3;
          int64_t w2 = 6;
          EXPECT_EQ(0, obj2.set_decimal(int2, fra2, w2, p2));

          EXPECT_EQ(false, obj1 == obj2);
        }

        //精度不同，但值相同

        {
          ObObj obj1;
          int64_t int1 = 49;
          int64_t fra1 = 4;
          int64_t p1 = 2;
          int64_t w1 = 6;
          EXPECT_EQ(0, obj1.set_decimal(int1, fra1, w1, p1));

          ObObj obj2;
          int64_t int2 =49;
          int64_t fra2 = 40;
          int64_t p2 = 3;
          int64_t w2 = 6;
          EXPECT_EQ(0, obj2.set_decimal(int2, fra2, w2, p2));

          EXPECT_EQ(true, obj1 == obj2);
        }

        {
          ObObj obj1;
          int64_t int1 = 49;
          int64_t fra1 = 4;
          int64_t p1 = 2;
          int64_t w1 = 6;
          EXPECT_EQ(0, obj1.set_decimal(int1, fra1, w1, p1, false));

          ObObj obj2;
          int64_t int2 =49;
          int64_t fra2 = 40;
          int64_t p2 = 3;
          int64_t w2 = 6;
          EXPECT_EQ(0, obj2.set_decimal(int2, fra2, w2, p2, false));

          EXPECT_EQ(true, obj1 == obj2);
        }

        {
          ObObj obj1;
          int64_t int1 = 49;
          int64_t fra1 = 4;
          int64_t p1 = 4;
          int64_t w1 = 6;
          EXPECT_EQ(0, obj1.set_decimal(int1, fra1, w1, p1, false));

          ObObj obj2;
          int64_t int2 =49;
          int64_t fra2 = 40;
          int64_t p2 = 3;
          int64_t w2 = 6;
          EXPECT_EQ(0, obj2.set_decimal(int2, fra2, w2, p2, false));

          EXPECT_EQ(false, obj1 == obj2);
        }
        {
          ObObj obj1;
          int64_t int1 = 49;
          int64_t fra1 = 40;
          int64_t p1 = 4;
          int64_t w1 = 6;
          EXPECT_EQ(0, obj1.set_decimal(int1, fra1, w1, p1, false));

          ObObj obj2;
          int64_t int2 =49;
          int64_t fra2 = 4;
          int64_t p2 = 3;
          int64_t w2 = 6;
          EXPECT_EQ(0, obj2.set_decimal(int2, fra2, w2, p2, false));

          EXPECT_EQ(true, obj1 == obj2);
        }

        {
          ObObj obj1;
          int64_t int1 = 49;
          int64_t fra1 = 4;
          int64_t p1 = 2;
          int64_t w1 = 6;
          EXPECT_EQ(0, obj1.set_decimal(int1, fra1, w1, p1, false));

          ObObj obj2;
          int64_t int2 =49;
          int64_t fra2 = 40;
          int64_t p2 = 3;
          int64_t w2 = 6;
          EXPECT_EQ(0, obj2.set_decimal(int2, fra2, w2, p2));

          EXPECT_EQ(false, obj1 == obj2);
        }

        {
          ObObj obj1;
          int64_t int1 = 49;
          int64_t fra1 = 40;
          int64_t p1 = 2;
          int64_t w1 = 6;
          EXPECT_EQ(0, obj1.set_decimal(int1, fra1, w1, p1, false));

          ObObj obj2;
          int64_t int2 = 49;
          int64_t fra2 = 40;
          int64_t p2 = 3;
          int64_t w2 = 6;
          EXPECT_EQ(0, obj2.set_decimal(int2, fra2, w2, p2));

          EXPECT_EQ(false, obj1 == obj2);
        }
        {
          ObObj obj1;
          int64_t int1 = 49;
          int64_t fra1 = 40;
          int64_t p1 = 2;
          int64_t w1 = 6;
          EXPECT_EQ(0, obj1.set_decimal(int1, fra1, w1, p1));

          ObObj obj2;
          int64_t int2 = 49;
          int64_t fra2 = 40;
          int64_t p2 = 3;
          int64_t w2 = 6;
          EXPECT_EQ(0, obj2.set_decimal(int2, fra2, w2, p2));

          EXPECT_EQ(false, obj1 == obj2);
        }
      }

      TEST_F(TestObjDecimal, ne)
      {
        //精度相同
        {
          ObObj obj1;
          int64_t int1 = 49;
          int64_t fra1 = 31;
          int64_t p1 = 3;
          int64_t w1 = 6;
          EXPECT_EQ(0, obj1.set_decimal(int1, fra1, w1, p1, false));

          ObObj obj2;
          int64_t int2 =49;
          int64_t fra2 = 31;
          int64_t p2 = 3;
          int64_t w2 = 6;
          EXPECT_EQ(0, obj2.set_decimal(int2, fra2, w2, p2, false));

          EXPECT_EQ(false, obj1 != obj2);
        }

        {
          ObObj obj1;
          int64_t int1 = 43;
          int64_t fra1 = 31;
          int64_t p1 = 3;
          int64_t w1 = 6;
          EXPECT_EQ(0, obj1.set_decimal(int1, fra1, w1, p1, false));

          ObObj obj2;
          int64_t int2 =49;
          int64_t fra2 = 31;
          int64_t p2 = 3;
          int64_t w2 = 6;
          EXPECT_EQ(0, obj2.set_decimal(int2, fra2, w2, p2, false));

          EXPECT_EQ(true, obj1 != obj2);
        }

        {
          ObObj obj1;
          int64_t int1 = 49;
          int64_t fra1 = 45;
          int64_t p1 = 3;
          int64_t w1 = 6;
          EXPECT_EQ(0, obj1.set_decimal(int1, fra1, w1, p1, false));

          ObObj obj2;
          int64_t int2 =49;
          int64_t fra2 = 31;
          int64_t p2 = 3;
          int64_t w2 = 6;
          EXPECT_EQ(0, obj2.set_decimal(int2, fra2, w2, p2, false));

          EXPECT_EQ(true, obj1 != obj2);
        }

        {
          ObObj obj1;
          int64_t int1 = 49;
          int64_t fra1 = 45;
          int64_t p1 = 3;
          int64_t w1 = 6;
          EXPECT_EQ(0, obj1.set_decimal(int1, fra1, w1, p1));

          ObObj obj2;
          int64_t int2 =49;
          int64_t fra2 = 31;
          int64_t p2 = 3;
          int64_t w2 = 6;
          EXPECT_EQ(0, obj2.set_decimal(int2, fra2, w2, p2));

          EXPECT_EQ(true, obj1 != obj2);
        }
        {
          ObObj obj1;
          int64_t int1 = 49;
          int64_t fra1 = 45;
          int64_t p1 = 3;
          int64_t w1 = 6;
          EXPECT_EQ(0, obj1.set_decimal(int1, fra1, w1, p1));

          ObObj obj2;
          int64_t int2 = 49;
          int64_t fra2 = 45;
          int64_t p2 = 3;
          int64_t w2 = 6;
          EXPECT_EQ(0, obj2.set_decimal(int2, fra2, w2, p2));

          EXPECT_EQ(false, obj1 != obj2);
        }

        {
          ObObj obj1;
          int64_t int1 = 49;
          int64_t fra1 = 45;
          int64_t p1 = 3;
          int64_t w1 = 6;
          EXPECT_EQ(0, obj1.set_decimal(int1, fra1, w1, p1, false));

          ObObj obj2;
          int64_t int2 = 49;
          int64_t fra2 = 45;
          int64_t p2 = 3;
          int64_t w2 = 6;
          EXPECT_EQ(0, obj2.set_decimal(int2, fra2, w2, p2));

          EXPECT_EQ(true, obj1 != obj2);
          EXPECT_EQ(false, obj1 == obj2);
        }

        //精度不同，但值相同

        {
          ObObj obj1;
          int64_t int1 = 49;
          int64_t fra1 = 4;
          int64_t p1 = 2;
          int64_t w1 = 6;
          EXPECT_EQ(0, obj1.set_decimal(int1, fra1, w1, p1));

          ObObj obj2;
          int64_t int2 =49;
          int64_t fra2 = 40;
          int64_t p2 = 3;
          int64_t w2 = 6;
          EXPECT_EQ(0, obj2.set_decimal(int2, fra2, w2, p2));

          EXPECT_EQ(false, obj1 != obj2);
          EXPECT_EQ(true, obj1 == obj2);
        }

        {
          ObObj obj1;
          int64_t int1 = 49;
          int64_t fra1 = 4;
          int64_t p1 = 2;
          int64_t w1 = 6;
          EXPECT_EQ(0, obj1.set_decimal(int1, fra1, w1, p1, false));

          ObObj obj2;
          int64_t int2 =49;
          int64_t fra2 = 40;
          int64_t p2 = 3;
          int64_t w2 = 6;
          EXPECT_EQ(0, obj2.set_decimal(int2, fra2, w2, p2, false));

          EXPECT_EQ(false, obj1 != obj2);
          EXPECT_EQ(true, obj1 == obj2);
        }

        {
          ObObj obj1;
          int64_t int1 = 49;
          int64_t fra1 = 4;
          int64_t p1 = 4;
          int64_t w1 = 6;
          EXPECT_EQ(0, obj1.set_decimal(int1, fra1, w1, p1, false));

          ObObj obj2;
          int64_t int2 =49;
          int64_t fra2 = 40;
          int64_t p2 = 3;
          int64_t w2 = 6;
          EXPECT_EQ(0, obj2.set_decimal(int2, fra2, w2, p2, false));

          EXPECT_EQ(true, obj1 != obj2);
        }

        {
          ObObj obj1;
          int64_t int1 = 49;
          int64_t fra1 = 4;
          int64_t p1 = 2;
          int64_t w1 = 6;
          EXPECT_EQ(0, obj1.set_decimal(int1, fra1, w1, p1, false));

          ObObj obj2;
          int64_t int2 =49;
          int64_t fra2 = 40;
          int64_t p2 = 3;
          int64_t w2 = 6;
          EXPECT_EQ(0, obj2.set_decimal(int2, fra2, w2, p2));

          EXPECT_EQ(true, obj1 != obj2);
        }

        {
          ObObj obj1;
          int64_t int1 = 49;
          int64_t fra1 = 40;
          int64_t p1 = 2;
          int64_t w1 = 6;
          EXPECT_EQ(0, obj1.set_decimal(int1, fra1, w1, p1, false));

          ObObj obj2;
          int64_t int2 = 49;
          int64_t fra2 = 40;
          int64_t p2 = 3;
          int64_t w2 = 6;
          EXPECT_EQ(0, obj2.set_decimal(int2, fra2, w2, p2));

          EXPECT_EQ(true, obj1 != obj2);
        }
        {
          ObObj obj1;
          int64_t int1 = 49;
          int64_t fra1 = 40;
          int64_t p1 = 2;
          int64_t w1 = 6;
          EXPECT_EQ(0, obj1.set_decimal(int1, fra1, w1, p1));

          ObObj obj2;
          int64_t int2 = 49;
          int64_t fra2 = 40;
          int64_t p2 = 3;
          int64_t w2 = 6;
          EXPECT_EQ(0, obj2.set_decimal(int2, fra2, w2, p2));

          EXPECT_EQ(true, obj1 != obj2);
        }
      }

	 // void ObObj::dump(const int32_t log_level /*=TBSYS_LOG_LEVEL_DEBUG*/) const
      TEST_F(TestObjDecimal, dump)
      {
        ObObj obj;
        int64_t integer = 43;
        int64_t fractional = 54;
        bool sign = true;
        int64_t width = 6;
        int64_t precision = 3;
        bool is_add = true;
        obj.set_decimal(integer, fractional, width, precision, sign, is_add);
        obj.dump();
        obj.set_decimal(integer, fractional, width, precision, false, false);
        obj.dump();
      }

      //int encode_decimal_type(char *buf, const int64_t buf_len, int64_t &pos,
      //const int64_t integer, const int32_t fractional, int16_t meta, const bool is_add)
      TEST_F(TestObjDecimal, serialize)
      {
        //整数的序列化 正数 无is_add
        {
          ObObj obj;
          ObDecimalHelper::meta_union meta;
          TBSYS_LOG(WARN, "meta size = %d", sizeof(meta));
          int64_t integer = 43;
          int64_t fractional = 54;
          bool sign = true;
          int64_t width = 6;
          int64_t precision = 3;
          bool is_add = false;
          obj.set_decimal(integer, fractional, width, precision, sign, is_add);
          char buf[1024];
          int64_t buf_len = 1024;
          int64_t pos = 0;
          obj.serialize(buf, buf_len, pos);
          ObObj objto;
          pos = 0;
          objto.deserialize(buf, buf_len, pos);
          int64_t i = 0;
          int64_t f = 0;
          bool a = false;
          objto.get_decimal(i, f, meta, a);
          EXPECT_EQ(integer, i);
          EXPECT_EQ(fractional, f);
          EXPECT_EQ(width, meta.value_.width_);
          EXPECT_EQ(precision, meta.value_.precision_);
          EXPECT_EQ(sign, meta.value_.sign_ == 0 ? true : false);
          EXPECT_EQ(is_add, a);
        }

        //负数的序列化  负数 有is_add
        {
          ObObj obj;
          int64_t integer = 45323;
          int64_t fractional = 545;
          bool sign = false;
          int64_t width = 8;
          int64_t precision = 3;
          bool is_add = true;
          obj.set_decimal(integer, fractional, width, precision, sign, is_add);
          char buf[1024];
          int64_t buf_len = 1024;
          int64_t pos = 0;
          obj.serialize(buf, buf_len, pos);
          ObObj objto;
          pos = 0;
          objto.deserialize(buf, buf_len, pos);
          int64_t i = 0;
          int64_t f = 0;
          ObDecimalHelper::meta_union meta;
          bool a = false;
          objto.get_decimal(i, f, meta, a);
          EXPECT_EQ(integer, i);
          EXPECT_EQ(fractional, f);
          EXPECT_EQ(width, meta.value_.width_);
          EXPECT_EQ(precision, meta.value_.precision_);
          EXPECT_EQ(sign, meta.value_.sign_ == 0 ? true : false);
          EXPECT_EQ(is_add, a);
        }

        //is_add的序列化 正数 + 有 is_add
        {
          ObObj obj;
          int64_t integer = 4723;
          int64_t fractional = 54;
          bool sign = true;
          int64_t width = 7;
          int64_t precision = 3;
          bool is_add = true;
          obj.set_decimal(integer, fractional, width, precision, sign, is_add);
          char buf[1024];
          int64_t buf_len = 1024;
          int64_t pos = 0;
          obj.serialize(buf, buf_len, pos);
          ObObj objto;
          pos = 0;
          objto.deserialize(buf, buf_len, pos);
          int64_t i = 0;
          int64_t f = 0;
          ObDecimalHelper::meta_union meta;
          bool a = false;
          objto.get_decimal(i, f, meta, a);
          EXPECT_EQ(integer, i);
          EXPECT_EQ(fractional, f);
          EXPECT_EQ(width, meta.value_.width_);
          EXPECT_EQ(precision, meta.value_.precision_);
          EXPECT_EQ(sign, meta.value_.sign_ == 0 ? true : false);
          EXPECT_EQ(is_add, a);
        }

        //负数无 is_add
        {
          ObObj obj;
          int64_t integer = 4723;
          int64_t fractional = 54;
          bool sign = false;
          int64_t width = 7;
          int64_t precision = 3;
          bool is_add = true;
          obj.set_decimal(integer, fractional, width, precision, sign, is_add);
          char buf[1024];
          int64_t buf_len = 1024;
          int64_t pos = 0;
          obj.serialize(buf, buf_len, pos);
          ObObj objto;
          pos = 0;
          objto.deserialize(buf, buf_len, pos);
          int64_t i = 0;
          int64_t f = 0;
          ObDecimalHelper::meta_union meta;
          bool a = false;
          objto.get_decimal(i, f, meta, a);
          EXPECT_EQ(integer, i);
          EXPECT_EQ(fractional, f);
          EXPECT_EQ(width, meta.value_.width_);
          EXPECT_EQ(precision, meta.value_.precision_);
          EXPECT_EQ(sign, meta.value_.sign_ == 0 ? true : false);
          EXPECT_EQ(is_add, a);
        }
      }

      //int ObObj::apply(const ObObj &mutation)

      TEST_F(TestObjDecimal, apply)
      {
        //如果obj为空值, 两者都没有 is_add
        {
          ObObj obj;
          obj.set_null();
          ObObj mutation;
          int64_t integer = 12;
          int64_t fractional = 32;
          int64_t width = 6;
          int64_t precision = 3;
          EXPECT_EQ(0, mutation.set_decimal(integer, fractional, width, precision));
          EXPECT_EQ(0, obj.apply(mutation));
          int64_t int_val = 0;
          int64_t fra_val = 0;
          ObDecimalHelper::meta_union meta;;
          bool is_add = false;
          obj.get_decimal(int_val, fra_val, meta, is_add);
          EXPECT_EQ(int_val, integer);
          EXPECT_EQ(fra_val, fractional);
          EXPECT_EQ(width, meta.value_.width_);
          EXPECT_EQ(precision, meta.value_.precision_);
          EXPECT_EQ(true, (meta.value_.sign_ == 0 ? true : false));
          EXPECT_EQ(is_add, false);
        }

        //如果obj为行不存在
        {
          ObObj obj;
          int64_t val = 11;
          obj.set_ext(val);
          ObObj mutation;
          int64_t integer = 12;
          int64_t fractional = 32;
          int64_t width = 6;
          int64_t precision = 3;
          mutation.set_decimal(integer, fractional, width, precision);
          EXPECT_EQ(0, obj.apply(mutation));
          int64_t int_val;
          int64_t fra_val;
          ObDecimalHelper::meta_union meta;;
          bool is_add = false;
          obj.get_decimal(int_val, fra_val, meta, is_add);
          EXPECT_EQ(int_val, integer);
          EXPECT_EQ(fra_val, fractional);
          EXPECT_EQ(width, meta.value_.width_);
          EXPECT_EQ(precision, meta.value_.precision_);
          EXPECT_EQ(true, (meta.value_.sign_ == 0 ? true : false));
          EXPECT_EQ(is_add, false);
        }

        //两个正定点数，前面有is_add 后面没有
        {
          ObObj obj;
          int64_t i = 34;
          int64_t f = 34;
          int64_t w = 6;
          int64_t p = 3;
          obj.set_decimal(i, f, w, p, true, true);
          ObObj mutation;
          int64_t integer = 12;
          int64_t fractional = 32;
          int64_t width = 6;
          int64_t precision = 3;
          mutation.set_decimal(integer, fractional, width, precision);
          EXPECT_EQ(0, obj.apply(mutation));
          int64_t int_val;
          int64_t fra_val;
          ObDecimalHelper::meta_union meta;;
          bool is_add = false;
          obj.get_decimal(int_val, fra_val, meta, is_add);
          EXPECT_EQ(int_val, integer);
          EXPECT_EQ(fra_val, fractional);
          EXPECT_EQ(width, meta.value_.width_);
          EXPECT_EQ(precision, meta.value_.precision_);
          EXPECT_EQ(true, (meta.value_.sign_ == 0 ? true : false));
          EXPECT_EQ(is_add, false);
        }
        //两个正定点数，前面有有is_add ，后面也有
        {
          ObObj obj;
          int64_t i = 34;
          int64_t f = 34;
          int64_t w = 6;
          int64_t p = 3;
          obj.set_decimal(i, f, w, p, true, true);
          ObObj mutation;
          int64_t integer = 12;
          int64_t fractional = 32;
          int64_t width = 6;
          int64_t precision = 3;
          mutation.set_decimal(integer, fractional, width, precision, true, true);
          EXPECT_EQ(0, obj.apply(mutation));
          int64_t int_val;
          int64_t fra_val;
          ObDecimalHelper::meta_union meta;;
          bool is_add = false;
          obj.get_decimal(int_val, fra_val, meta, is_add);
          EXPECT_EQ(int_val, integer + i);
          EXPECT_EQ(fra_val, fractional + f);
          EXPECT_EQ(width, meta.value_.width_);
          EXPECT_EQ(precision, meta.value_.precision_);
          EXPECT_EQ(true, (meta.value_.sign_ == 0 ? true : false));
          EXPECT_EQ(is_add, true);
        }
        //一正一负两个定点数，前面无is_add 后面有
        {
          ObObj obj;
          int64_t i = 34;
          int64_t f = 34;
          int64_t w = 6;
          int64_t p = 3;
          obj.set_decimal(i, f, w, p, false);
          ObObj mutation;
          int64_t integer = 12;
          int64_t fractional = 32;
          int64_t width = 6;
          int64_t precision = 3;
          mutation.set_decimal(integer, fractional, width, precision, true, true);
          EXPECT_EQ(0, obj.apply(mutation));
          int64_t int_val;
          int64_t fra_val;
          ObDecimalHelper::meta_union meta;;
          bool is_add = false;
          obj.get_decimal(int_val, fra_val, meta, is_add);
          EXPECT_EQ(int_val, i - integer);
          EXPECT_EQ(fra_val, f - fractional);
          EXPECT_EQ(width, meta.value_.width_);
          EXPECT_EQ(precision, meta.value_.precision_);
          EXPECT_EQ(false, (meta.value_.sign_ == 0 ? true : false));
          EXPECT_EQ(is_add, false);
        }

        //一正一负两个定点数，前面无，后面也无
        {
          ObObj obj;
          int64_t i = 34;
          int64_t f = 34;
          int64_t w = 6;
          int64_t p = 3;
          obj.set_decimal(i, f, w, p, false);
          ObObj mutation;
          int64_t integer = 12;
          int64_t fractional = 32;
          int64_t width = 6;
          int64_t precision = 3;
          mutation.set_decimal(integer, fractional, width, precision);
          EXPECT_EQ(0, obj.apply(mutation));
          int64_t int_val;
          int64_t fra_val;
          ObDecimalHelper::meta_union meta;;
          bool is_add = false;
          obj.get_decimal(int_val, fra_val, meta, is_add);
          EXPECT_EQ(int_val, integer);
          EXPECT_EQ(fra_val, fractional);
          EXPECT_EQ(width, meta.value_.width_);
          EXPECT_EQ(precision, meta.value_.precision_);
          EXPECT_EQ(true, (meta.value_.sign_ == 0 ? true : false));
          EXPECT_EQ(is_add, false);
        }

        //一正一负两个定点数，前面有，后面无
        {
          ObObj obj;
          int64_t i = 34;
          int64_t f = 34;
          int64_t w = 6;
          int64_t p = 3;
          obj.set_decimal(i, f, w, p, false, true);
          ObObj mutation;
          int64_t integer = 12;
          int64_t fractional = 32;
          int64_t width = 6;
          int64_t precision = 3;
          mutation.set_decimal(integer, fractional, width, precision);
          EXPECT_EQ(0, obj.apply(mutation));
          int64_t int_val;
          int64_t fra_val;
          ObDecimalHelper::meta_union meta;;
          bool is_add = false;
          obj.get_decimal(int_val, fra_val, meta, is_add);
          EXPECT_EQ(int_val, integer);
          EXPECT_EQ(fra_val, fractional);
          EXPECT_EQ(width, meta.value_.width_);
          EXPECT_EQ(precision, meta.value_.precision_);
          EXPECT_EQ(true, (meta.value_.sign_ == 0 ? true : false));
          EXPECT_EQ(is_add, false);
        }

        //两个定点数精度不同
        {
          ObObj obj;
          int64_t i = 34;
          int64_t f = 34;
          int64_t w = 6;
          int64_t p = 4;
          obj.set_decimal(i, f, w, p, false, true);
          ObObj mutation;
          int64_t integer = 12;
          int64_t fractional = 32;
          int64_t width = 6;
          int64_t precision = 3;
          mutation.set_decimal(integer, fractional, width, precision);
          EXPECT_EQ(OB_DECIMAL_PRECISION_NOT_EQUAL, obj.apply(mutation));
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
