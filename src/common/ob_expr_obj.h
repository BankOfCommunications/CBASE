/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_expr_obj.h
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#ifndef _OB_EXPR_OBJ_H
#define _OB_EXPR_OBJ_H 1
#include "ob_object.h"
#include "ob_string_buf.h"

class ObExprObj_Math_Test;
class ObExprObj_negate_test_Test;
class ObExprObjTest_get_bool_Test;
class ObExprObjTest_compare_Test;
class ObExprObjTest_like_Test;
class ObExprObjTest_others_Test;
namespace oceanbase
{
  namespace common
  {

    class ObExprObj
    {
      public:
        ObExprObj();
        ~ObExprObj();

        void assign(const ObObj &obj);
        int to(ObObj &obj) const;
        int cast_to(int32_t dest_type, ObExprObj &result, ObStringBuf &mem_buf) const;
        //add fanqiushi DECIMAL OceanBase_BankCommV0.3 2014_7_19:b
        void set_decimal(const ObDecimal &value);
        int cast_toV2(int32_t dest_type, ObExprObj &result, ObStringBuf &mem_buf,uint32_t precision,uint32_t scale) ;
        //add:e
        // setters
        void set_null();
        void set_int(const int64_t value);
        //add lijianqiang [INT_32] 20150930:b
        void set_int32(const int32_t value);
        //add 20150930:e
        void set_float(const float value);
        void set_double(const double value);
        void set_datetime(const ObDateTime &value);
        void set_precise_datetime(const ObPreciseDateTime &value);
        //add peiouya [DATE_TIME] 20150831:b
        void set_date(const ObDate &value);
        void set_time(const ObTime &value);
        //add 20150831:e
        void set_interval(const ObInterval &value);  //add peiouya [DATE_TIME] 20150909
        void set_varchar(const ObString& value);
        void set_ctime(const ObCreateTime &value);
        void set_mtime(const ObModifyTime &value);
        void set_bool(const bool value);
        void set_decimal(const ObNumber &value);
        void set_ext(const int64_t value);

        // getters
        bool is_null() const {return ObNullType == type_;}
        int64_t get_int() const {return v_.int_;}
        //add lijianqiang [INT_32] 20150930:b
        int32_t get_int32()const {return v_.int32_;}
        //add 20150930:e
        float get_float() const {return v_.float_;}
        double get_double() const {return v_.double_;}
        const ObDateTime& get_datetime() const {return v_.datetime_;}
        const ObPreciseDateTime& get_precise_datetime() const {return v_.precisedatetime_;}
        const ObString& get_varchar() const {return varchar_;}
        const ObCreateTime& get_ctime() const {return v_.createtime_;}
        const ObModifyTime& get_mtime() const {return v_.modifytime_;}
        bool get_bool() const {return v_.bool_;}
        //modify fanqiushi DECIMAL OceanBase_BankCommV0.3 2014_7_19:b
        //const ObNumber& get_decimal() const {return num_;}      old code
        const ObDecimal& get_decimal() const {return decimal_;}
        //modify:e
        const int64_t get_ext() const {return v_.ext_;}
        ObObjType get_type() const;
        bool is_zero() const;
        bool is_true() const;
        bool is_false() const;
        int get_int(int64_t& value) const;
        //add yushengjuan [INT_32] 20151012:b
        int get_int32(int32_t& value) const;
        //add 20151012:e
        int get_varchar(ObString& value) const;
        int do_type_promotion(ObObjType l, ObObjType r, ObObjType &res_type);//add qianzm [set_operation] 20151222


        /// compare
        int compare(const ObExprObj &other, int &cmp) const;

        // compare operators
        ///@note the return code of these functions can be ignored
        int lt(const ObExprObj &other, ObExprObj &res) const;
        int gt(const ObExprObj &other, ObExprObj &res) const;
        int le(const ObExprObj &other, ObExprObj &res) const;
        int ge(const ObExprObj &other, ObExprObj &res) const;
        int eq(const ObExprObj &other, ObExprObj &res) const;
        int ne(const ObExprObj &other, ObExprObj &res) const;
        int btw(const ObExprObj &v1, const ObExprObj &v2, ObExprObj &res) const;
        int not_btw(const ObExprObj &v1, const ObExprObj &v2, ObExprObj &res) const;
        /// is true/false/null(unknown)
        int is(const ObExprObj &other, ObExprObj &res) const;
        /// is not true/false/null(unknown)
        int is_not(const ObExprObj &other, ObExprObj &res) const;

        /// logical and
        int land(const ObExprObj &other, ObExprObj &res) const;
        /// logical or
        int lor(const ObExprObj &other, ObExprObj &res) const;
        /// logical not
        int lnot(ObExprObj &res) const;

        // numeric arithmetic operators
        int add(ObExprObj &other, ObExprObj &res);
        int sub(ObExprObj &other, ObExprObj &res);
        int mul(ObExprObj &other, ObExprObj &res);
        int div(ObExprObj &other, ObExprObj &res, bool int_div_as_double);
        int mod(const ObExprObj &other, ObExprObj &res) const;
        int negate(ObExprObj &res) const;

        /// string like
        int old_like(const ObExprObj &other, ObExprObj &result) const; // compatible with oceanbase 0.3
        int like(const ObExprObj &other, ObExprObj &result) const;
        int substr(const ObExprObj &start_pos, ObExprObj &result, ObStringBuf &mem_buf) const;
        int substr(const ObExprObj &start_pos_obj, const ObExprObj &expect_length_obj, ObExprObj &result, ObStringBuf &mem_buf) const;
        int not_like(const ObExprObj &other, ObExprObj &result) const;
        int varchar_length(ObExprObj &res) const;
        int hex(ObExprObj &res, ObStringBuf &mem_buf);
        int unhex(ObExprObj &res, ObStringBuf &mem_buf);
        //add wuna [datetime func] 20150831:b
        int get_gtm(struct tm &gtm, int unexpected_type) const;
        int year(ObExprObj &res);
        int month(ObExprObj &res);
        int day(ObExprObj &res);
        int days(ObExprObj &res);
        //add 20150831:e
        //add liuzy [datetime func] 20150901:b
        int hour(ObExprObj &res);
        int minute(ObExprObj &res);
        int second(ObExprObj &res);
        //add 20150901:e
        //add liuzy [datetime func] 20151019:b
        int check_real_type(const ObString &in) const;
        //add 20151019:e
        int ip_to_int(ObExprObj &res);
        int int_to_ip(ObExprObj &res, ObStringBuf &mem_buf);
        int upper_case(ObExprObj &result, ObStringBuf &mem_buf) const;
        int lower_case(ObExprObj &result, ObStringBuf &mem_buf) const;
        int concat(const ObExprObj &other, ObExprObj &result, ObStringBuf &mem_buf) const;
        int trim(const ObExprObj &trimType, const ObExprObj &trimPattern, ObExprObj &result, ObStringBuf &mem_buf) const;
        // result type of calculations
        // all supported operations not listed here return ObBoolType
        static ObObj type_add(const ObObj& t1, const ObObj& t2);
        static ObObj type_sub(const ObObj& t1, const ObObj& t2);
        static ObObj type_mul(const ObObj& t1, const ObObj& t2);
        static ObObj type_div(const ObObj& t1, const ObObj& t2, bool int_div_as_double);
        static ObObj type_mod(const ObObj& t1, const ObObj& t2);
        static ObObj type_negate(const ObObj& t1);
        static ObObj type_varchar_length(const ObObj& t1);
        //add dolphin[coalesce return type] 20151126:b
        static int coalesce_type_compatible( ObObjType &t1, const ObObjType &t2);
        static bool can_compare(const ObObjType &t1, const ObObjType &t2) ;
        static bool is_numeric(const ObObjType &t) ;
        static bool is_datetime_compare(const ObObjType type) ;
        //add:e
        int64_t to_string(char* buffer, const int64_t length) const;
        int64_t to_string_v2(std::string& s) const;//add gaojt [ListAgg][JHOBv0.1]20150104
        //add wenghaixing DECIMAL OceanBase_BankCommV0.3 2014_7_10:b
        uint32_t get_precision();
        uint32_t get_scale();
        uint32_t get_vscale();
        static ObObj type_add_v2(const ObObj& t1, const ObObj& t2);
        static ObObj type_sub_v2(const ObObj& t1, const ObObj& t2);
        static ObObj type_mul_v2(const ObObj& t1, const ObObj& t2);
        static ObObj type_div_v2(const ObObj& t1, const ObObj& t2, bool int_div_as_double);
        //add e
        bool is_datetime_for_out() const;//add peiouy[datetime func]20150912
        //add peiouya [IN_TYPEBUG_FIX] 20151225:b
        static ObObjType type_promotion_for_in_op (const ObObjType left_type, const ObObjType right_type);
        //add 20151225:e
      private:
        // function members
        static int type_promotion(ObObjType type_promote_map[ObMaxType][ObMaxType],
                                  const ObExprObj &this_obj, const ObExprObj &other,
                                  ObExprObj &promoted_obj1, ObExprObj &promoted_obj2,
                                  const ObExprObj *&p_this, const ObExprObj *&p_other);

        static int compare_type_promotion(const ObExprObj &this_obj, const ObExprObj &other,
                                          ObExprObj &promoted_obj1, ObExprObj &promoted_obj2,
                                          const ObExprObj *&p_this, const ObExprObj *&p_other);
        int cast_to_int(int64_t &val) const;
        int cast_to_varchar(ObString &varchar, ObStringBuf &mem_buf) const;
        //add fanqiushi DECIMAL OceanBase_BankCommV0.3 2014_7_19:b
        inline int cast_to_decimal(ObString &varchar, ObStringBuf &mem_buf) const;
        //add:e
        int get_bool(bool &value) const;
        int get_timestamp(int64_t & timestamp) const;
        bool is_datetime() const;
        bool is_datetime(const ObObjType type) const;  //add peiouya [DATE_TIME] 20150906
        bool is_numeric() const;
        bool can_compare(const ObExprObj & other) const;
        int compare_same_type(const ObExprObj &other) const;
        static int arith_type_promotion(const ObExprObj &this_obj, const ObExprObj &other,
                                  ObExprObj &promoted_obj1, ObExprObj &promoted_obj2,
                                  const ObExprObj *&p_this_obj, const ObExprObj *&p_other);
        //add [DATE_TIME] peiouya 20150831:b
        void check_and_set_res_type(const ObObjType first_type, const ObObjType second_type,
                                    ObExprObj &res) const;
        //add 20150831:e
        int time_skew(ObExprObj &res) const; //add [DATE_TIME] peiouya 20150914
        int add_same_type(const ObExprObj &other, ObExprObj &res) const;
        int sub_same_type(const ObExprObj &other, ObExprObj &res) const;
        int mul_same_type(const ObExprObj &other, ObExprObj &res) const;
        static int div_type_promotion(const ObExprObj &this_obj, const ObExprObj &other,
                                      ObExprObj &promoted_obj1, ObExprObj &promoted_obj2,
                                      const ObExprObj *&p_this, const ObExprObj *&p_other,
                                      bool int_div_as_double);
        int div_same_type(const ObExprObj &other, ObExprObj &res) const;
        static int mod_type_promotion(const ObExprObj &this_obj, const ObExprObj &other,
                                      ObExprObj &promoted_obj1, ObExprObj &promoted_obj2,
                                      const ObExprObj *&p_this, const ObExprObj *&p_other);

        static ObObj type_arithmetic(const ObObj& t1, const ObObj& t2);
        int substr(const int32_t start_pos, const int32_t expect_length_of_str,
                  ObExprObj &result, ObStringBuf &mem_buf) const;
        int lrtrim(const ObString src, const ObString pattern, int32_t &start, int32_t &end) const;
        int ltrim(const ObString src, const ObString pattern, int32_t &start) const;
        int rtrim(const ObString src, const ObString pattern, int32_t &end) const;
        // functions for testing only
        friend class ::ObExprObj_Math_Test;
        friend class ::ObExprObj_negate_test_Test;
        friend class ::ObExprObjTest_get_bool_Test;
        friend class ::ObExprObjTest_compare_Test;
        friend class ::ObExprObjTest_like_Test;
        friend class ::ObExprObjTest_others_Test;
        void set_null(int null); // for testing only
        void set_varchar(const char* value);
        int set_decimal(const char* dec_str);
        int get_datetime(ObDateTime& value) const;
        int get_precise_datetime(ObPreciseDateTime& value) const;
        int get_decimal(char * buf, const int64_t buf_len) const;
        int get_float(float &f) const;
        int get_double(double &d) const;
      private:
        // data members
        int8_t type_;           // ObObjType
        ObNumber num_;
        //add fanqiushi DECIMAL OceanBase_BankCommV0.3 2014_7_19:b
        ObDecimal decimal_;
        //add:e
        ObString varchar_;
        union
        {
          int64_t int_;
          //add lijianqiang [INT_32] 20150930:b
          int32_t int32_;
          //add 20150930:e
          int64_t ext_;
          ObDateTime datetime_;
          ObPreciseDateTime precisedatetime_;
          ObCreateTime createtime_;
          ObModifyTime modifytime_;
          bool bool_;
          float float_;
          double double_;
        } v_;

    };

    inline ObExprObj::ObExprObj()
      :type_(ObNullType)
    {
      v_.int_ = 0;
    }

    inline ObExprObj::~ObExprObj()
    {
    }

    inline void ObExprObj::set_null()
    {
      type_ = ObNullType;
    }

    inline void ObExprObj::set_null(int ignore)
    {
      UNUSED(ignore);
      type_ = ObNullType;
    }

    inline void ObExprObj::set_int(const int64_t value)
    {
      type_ = ObIntType;
      v_.int_ = value;
    }
    //add lijianqiang [INT_32] 20150930:b
    inline void ObExprObj::set_int32(const int32_t value)
    {
      type_ = ObInt32Type;
      v_.int32_ = value;
    }
    //add 20150930:e
    inline void ObExprObj::set_float(const float value)
    {
      type_ = ObFloatType;
      v_.float_ = value;
    }

    inline void ObExprObj::set_double(const double value)
    {
      type_ = ObDoubleType;
      v_.double_ = value;
    }

    inline void ObExprObj::set_datetime(const ObDateTime &value)
    {
      type_ = ObDateTimeType;
      v_.datetime_ = value;
    }

    inline void ObExprObj::set_precise_datetime(const ObPreciseDateTime &value)
    {
      type_ = ObPreciseDateTimeType;
      v_.precisedatetime_ = value;
    }

    //add peiouya [DATE_TIME] 20150831:b
    inline void ObExprObj::set_date(const ObDate &value)
    {
      type_ = ObDateType;
      v_.precisedatetime_ = static_cast<ObPreciseDateTime>(value);
    }

    inline void ObExprObj::set_time(const ObTime &value)
    {
      type_ = ObTimeType;
      v_.precisedatetime_ = static_cast<ObPreciseDateTime>(value);
    }
    //add 20150831:e

    //add peiouya [DATE_TIME] 20150909:b
    inline void ObExprObj::set_interval(const ObInterval &value)
    {
      type_ = ObIntervalType;
      v_.precisedatetime_ = static_cast<ObPreciseDateTime>(value);
    }
    //add 20150909:e

    inline void ObExprObj::set_varchar(const ObString& value)
    {
      type_ = ObVarcharType;
      varchar_ = value;
    }

    inline void ObExprObj::set_ctime(const ObCreateTime &value)
    {
      type_ = ObCreateTimeType;
      v_.createtime_ = value;
    }

    inline void ObExprObj::set_mtime(const ObModifyTime &value)
    {
      type_ = ObModifyTimeType;
      v_.modifytime_ = value;
    }

    inline void ObExprObj::set_bool(const bool value)
    {
      type_ = ObBoolType;
      v_.bool_ = value;
    }

    inline void ObExprObj::set_decimal(const ObNumber &value)
    {
      type_ = ObDecimalType;
      num_ = value;
    }

    //modify fanqiushi DECIMAL OceanBase_BankCommV0.3 2014_7_19:b
    inline void ObExprObj::set_decimal(const ObDecimal &value)
    {
      type_ = ObDecimalType;
      decimal_ = value;
    }
   //modify:e

    inline void ObExprObj::set_ext(const int64_t value)
    {
      type_ = ObExtendType;
      v_.ext_ = value;
    }

    inline bool ObExprObj::is_zero() const
    {
      bool result = false;
      //modify fanqiushi DECIMAL OceanBase_BankCommV0.3 2014_7_9:b
      // change num_ to decimal_
      if (((type_ == ObIntType) && (v_.int_ == 0))
          || ((type_ == ObDecimalType) && decimal_.is_zero())
          || (type_ == ObFloatType && v_.float_ == 0.0f)
          || (type_ == ObDoubleType && v_.double_ == 0.0)
          || ((type_ == ObInt32Type)&& (v_.int32_ == 0)))//add lijianqiang [INT_32] bug_fix for div 0
      {
        result = true;
      }
      return result;
      //modify:e
    }

    inline ObObjType ObExprObj::get_type() const
    {
      return static_cast<ObObjType>(type_);
    }

    inline bool ObExprObj::is_true() const
    {
      return type_ == ObBoolType && v_.bool_;
    }

    inline bool ObExprObj::is_false() const
    {
      return type_ == ObBoolType && !v_.bool_;
    }
    //add wenghaixing DECIMAL OceanBase_BankCommV0.3 2014_7_10:b
    inline uint32_t ObExprObj::get_precision(){

      return decimal_.get_precision();
    }
    inline uint32_t ObExprObj::get_scale(){

      return decimal_.get_scale();
    }
    inline uint32_t ObExprObj::get_vscale(){

      return decimal_.get_vscale();
    }
//add :e
  } // end namespace common
} // end namespace oceanbase

#endif /* _OB_EXPR_OBJ_H */
