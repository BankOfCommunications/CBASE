#include "ob_define.h"
#include <tblog.h>
#ifndef _OCEANBASE_COMMON_OB_DECIMAL_HELPER_
#define _OCEANBASE_COMMON_OB_DECIMAL_HELPER_
namespace oceanbase
{
  namespace tests
  {
    namespace common
    {
      class TestObjDecimalHelper;
    }
  }
  namespace common
  {
    const static int64_t OB_MAX_DECIMAL_WIDTH = 27;
    const static int64_t OB_MAX_PRECISION_WIDTH = 9;
    const static int64_t OB_MAX_PRECISION_NUMBER = 999999999L;
    const static int64_t OB_DECIMAL_MULTIPLICATOR = 10;
    const static int64_t OB_MAX_DIGIT = 9;
    const static int64_t OB_MAX_DIGIT_ASC = '9';
    const static int64_t OB_INT_BINARY_LENGTH = 64;
    const static int64_t OB_INT_HALF_LENGTH = 32;
    const static int64_t OB_LEFT_PART_MAST_WITH_SIGN = 0xefff0000;
    const static int64_t OB_RIGHT_PART_MAST = 0x0000ffff;
    const static int64_t OB_LEFT_PART_MAST_WITHOUT_SIGN = 0xffff0000;
    const static int64_t OB_ARRAY_SIZE = 64;
    const static int64_t OB_DECIMAL_EQUAL = 0;
    const static int64_t OB_DECIMAL_LESS = -1;
    const static int64_t OB_DECIMAL_BIG = 1;
    //在处理溢出是使用
    /*如果是DEFAULT，那么溢出时，给出警告，并将数据截断为该宽度下的最大定点数
      如果是STRICT,那么会直接返回错误，不允许插入 */
    enum ObSqlMode
    {
      SQL_DEFAULT = 0,
      SQL_STRICT,
    };

    //在处理溢出或者除0的时候使用
    /*error_division_by_zero是指当除数为0时的处理方案，
      情况一：如果error_division_by_zero为真，并且sql_mode为strict时，会直接返回错误。
      情况二：如果error_division_by_zero为真，而sql_mode为default时，会返回警告，并且计算结果为NULL。
      其他情况下，不会返回警告，并且计算结果为NULL。*/
    struct ObTableProperty
    {
      ObSqlMode mode;
      bool error_division_by_zero;

      static ObTableProperty& getInstance()
      {
        static ObTableProperty instance;
        return instance;
      }

    //private:
      ObTableProperty()
      {
        mode = SQL_DEFAULT;
        error_division_by_zero = true;
      }
    };

    class ObObj;
    class ObDecimalHelper
    {
      public:
        friend class ObObj;
        friend class tests::common::TestObjDecimalHelper;
        struct meta
        {
          int8_t sign_:1;
          int8_t precision_:7;
          int8_t width_:8;
        };

        union meta_union
        {
          int16_t reserved_;
          meta value_;
        };

        struct ObDecimal
        {
          int64_t integer;
          int64_t fractional;
          meta_union meta;
        };

      public:
        ///@fn 求出宽度为width的最大整数，比如width = 4，那么返回值是9999
        ///@param[out] 返回宽度为width的最大整数
        static int get_max_express_number(const int64_t width, int64_t &value);

        ///@fn 求出宽度为precision的最小整数,比如width = 4,那么返回值是1000
        static int get_min_express_number(const int64_t width, int64_t &number);

        ///@fn实现小数部分的四舍五入
        ///@param[in/out]fractional 小数部分的数值
        ///@param[out]四舍五入是否有向整数的进位
        ///@param[in]precision 四舍五入后需要满足的精度
        static int round_off(int64_t &fractional, bool &is_carry, const int64_t precision);

        ///@fn 将精度为src_pre的小数部分按照dest_pre的精度来进行四舍五入
        ///@param fractional[in/out] 小数部分的值
        ///@param is_carry[out] 是否有向整数位的进位
        ///@param src_pre[in] 输入小数的精度
        ///@param dest_pre[src] 转换以后小数的精度
        static int round_off(int64_t &fractional, bool &is_carry, const int64_t src_pre, const int64_t dest_pre);

        ///@fn 将字符串转换成定点数的整数部分和小数部分
        ///@param value[in] 字符串首地址
        ///@param length[in] 定点数字符串的长度
        ///@param integer[out] 从字符串中解析出来的整数部分
        ///@param fractional[out] 从字符串中解析出来的小数部分
        ///@param precision[out] 从字符串中解析出来的小数的长度
        ///@param sign[out] 解析出来定点数的符号，true为正号，false为负号
        static int string_to_int(const char* value,
            const int64_t length,
            int64_t &integer,
            int64_t &fractional,
            int64_t &precision,
            bool &sign);

        ///@fn 实现两个定点数整数值的相加运算
        ///@param decimal[in/out] 被加数
        ///@param add_decimal[in] 加数
        ///@param gb_table_property[in] 溢出的处理规则
        static int ADD(ObDecimal &decimal,
            const ObDecimal &add_decimal,
            const ObTableProperty &gb_table_property);

        ///@fn 实现两个int64_t值的乘法
        ///@param product 乘积存放的地址
        ///@param array_size produce乘积数组的大小，至少要位4
        // static int MULT(const int64_t multiplier, const int64_t multiplicand, int32_t *product, const int64_t array_size);

        ///@fn 定点数与整数的乘法
        ///@param decimal[in] 被乘数
        ///@param multiplicand [in] 乘数
        ///@param product_int [out] 乘积的整数
        ///@param product_fra [out] 乘积的小数部分
        ///@param sign [out] 乘积的符号
        ///@param gb_table_property[in] 溢出的处理规则
        static int MUL(const ObDecimal &decimal,
            int64_t multiplicand,
            int64_t &product_int,
            int64_t &product_fra,
            bool &sign,
            const ObTableProperty &gb_table_property);

        ///@fn 实现定点数与整数的除法
        ///@param decimal[in] 定点数的被除数
        ///@param divisor[in] 除数
        ///@param consult_int[out] 商的整数部分
        ///@param consult_fra[out] 商的小数部分
        ///@param sign[out] 商的符号
        ///@param gb_table_property[in] 溢出的处理规则
        static int DIV(const ObDecimal &decimal,
            int64_t divisor,
            int64_t &consult_int,
            int64_t &consult_fra,
            bool &sign,
            const ObTableProperty &gb_table_property);

        ///@fn比较两个定点数的大小,
        ///@返回值有4个,0表示相当，1表示大于，-1表示小于,-2表示输入参数错误
        static int decimal_compare(ObDecimal decimal, ObDecimal other_decimal);

      private:

        //@fn 为了进行小数的比较,将小数的精度今天提升
        ///@param fractional[in/out] 小数部分的值
        ///@param precision[in] 应该提升的精度
        static int promote_fractional(int64_t &fractional, const int64_t precision);

        ///@fn实现定点数与整数的乘法,结果保存在数组中
        ///@param integer[in] 定点数的整数部分
        ///@param fractional[in]定点数的小数部分
        ///@param precision[in] 定点数的精度
        ///@param multiplicand[in] 被乘数
        ///@param length[in] 数组的长度，不能小于64
        static int array_mul(const int64_t integer,
            const int64_t fractional,
            const int64_t precision,
            const int64_t multiplicand,
            int8_t *product,
            const int64_t length);

        ///@fn实现定点数与整数的除法,结果存入数组中
        ///@param integer[in] 定点数的整数部分
        ///@param fractional[in] 定点数的小数部分
        ///@param precision[in] 小数的精度
        ///@param divisor[in] 除数，为非负数
        ///@param consult[out] 结果的存放地方
        ///@param consult_len[in] 传入consult的大小
        static int array_div(const int64_t integer,
            const int64_t fractional,
            const int64_t precision,
            const int64_t divisor,
            int8_t *consult,
            const int64_t consult_len);

        ///@fn用于判断加法操作时，和是否溢出,如果溢出，则按定点数的模式进行转换，小数如果溢出，则进位
        static int is_over_flow(const int64_t width,
            const int64_t precision,
            int64_t &integer,
            int64_t &fractional,
            const ObTableProperty &gb_table_property);

        ///@fn当定点数不符合模式时，将其进行规格化，小数如果溢出，则四舍五入
        static int decimal_format(const int64_t width,
            const int64_t precision,
            int64_t &integer,
            int64_t &fractional,
            const ObTableProperty &gb_table_property);

        ///@fn 检查字符串表示的定点数是否合法
        ///如果合法，则返回字符串中是否带符号位，符号位是正还是负，是否含有小数部分
        ///@param value[in] 带定点数的字符串
        ///@param length[in] 将value的前length个字符为定点数字符
        ///@param has_fractional[out] 返回是否带有小数部分
        ///@param has_sign[out] 返回是否带符号位
        ///@param sign[out] 符号位是正还是负
        static int check_decimal_str(const char *value,
            const int64_t length,
            bool &has_fractional,
            bool &has_sign,
            bool &sign);
        static int check_decimal(const ObDecimal &decimal);
		
		///@fn 将一个位数为value_len的整数写入一个大小为size的int8_t数组中
		///@param array[out] 写入的int8_t数组
		///@param size[in] 数组的大小
		///@param value_len[out] 要写入整数的位数
		///@param value[in] 要写入的整数
		static int int_to_byte(int8_t* array, const int64_t size, int64_t &value_len, const int64_t value, const bool in_head);
		///@fn 统计整数value总由几位数字组成
		static int get_digit_number(const int64_t value, int64_t &number);
	    
		///@fn 将一个位数为value_len的整数写入一个大小为size的int8_t数组中
		///@param value[out] 要取出的整数
		///@param value_len[in] 整数的位数
		///@param array[in] 存储整数的int8_t数组
		static int byte_to_int(int64_t &value, const int8_t *array, const int64_t value_len);
		
		///@fn 实现两个int8_t数组的加法，其中，adder数组需要左移pos位以后，进行相加，移位补0，实现乘法中的移位相加
		///@param size[in] 两个数组的大小都为size,size 的大小OB_ARRAY_SIZE，保存不会溢出
		static int byte_add(int8_t *add, const int8_t *adder, const int64_t size, const int64_t pos);
    };
  }
}

#endif
