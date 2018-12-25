/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_number.h
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#ifndef _OB_NUMBER_H
#define _OB_NUMBER_H 1
#include <stdint.h>
#include <iostream>
namespace oceanbase
{
  namespace common
  {
    class ObNumber
    {
      public:
        // init as 0
        ObNumber();
        ~ObNumber();

        // copy
        ObNumber(const ObNumber &other);
        ObNumber& operator=(const ObNumber &other);

        // type convertors
        void from(int8_t vscale, int8_t nwords, const uint32_t *words);
        int from(const char* buf, int64_t buf_len);
        int from(const char* str);
        void from(int64_t i64);
        static const int64_t MAX_PRINTABLE_SIZE = 128;
        int64_t to_string(char* buf, const int64_t buf_len) const;
        int to_int64(int64_t &i64) const;
        bool can_convert_to_int64() const;
        int round_to(int8_t precision, int8_t scale, int8_t &nwords, int8_t &vscale, uint32_t *words) const;
        int cast_to_int64(int64_t &i64) const;

        int8_t get_vscale() const;
        int8_t get_nwords() const;
        const uint32_t *get_words() const;

        // arithmetic operators
        int add(const ObNumber &other, ObNumber &res) const;
        int sub(const ObNumber &other, ObNumber &res) const;
        int mul(const ObNumber &other, ObNumber &res) const;
        int div(const ObNumber &other, ObNumber &res) const;
        int negate(ObNumber &res) const;

        int compare(const ObNumber &other) const;
        bool operator<(const ObNumber &other) const;
        bool operator<=(const ObNumber &other) const;
        bool operator>(const ObNumber &other) const;
        bool operator>=(const ObNumber &other) const;
        bool operator==(const ObNumber &other) const;
        bool operator!=(const ObNumber &other) const;

        void set_zero();
        bool is_negative() const;
        bool is_zero() const;

        static const int8_t MAX_NWORDS = 9;
        static int8_t QUOTIENT_SCALE;
      private:
        // types and constants
        static const uint32_t SIGN_MASK = 0x80000000;
        static const int64_t BASE = static_cast<int64_t>(UINT32_MAX) + 1;
        static const int64_t HALF_BASE = BASE / 2;
        static const uint64_t UBASE = static_cast<uint64_t>(BASE);
        static const int8_t HALF_NWORDS = 4;
        static const int8_t SINGLE_PRECISION_NDIGITS = 38;
        static const int8_t DOUBLE_PRECISION_NDIGITS = 2 * SINGLE_PRECISION_NDIGITS;
      private:
        // function members
        int left_shift(int8_t i, bool did_carry);
        void right_shift(int8_t i);
        void extend_words(int8_t nwords);
        void remove_leading_zeroes();
        void remove_leading_zeroes_unsigned();
        void round_fraction_part(int8_t scale);
        // @note n and res can refer to the same object
        static void negate(const ObNumber &n, ObNumber &res);
        // @note n1 and res can refer to the same object
        static int add_uint32(const ObNumber &n1, uint32_t n2, ObNumber &res);
        // @note multiplicand and res can refer to the same object
        static int mul_uint32(const ObNumber &multiplicand, uint32_t multiplier, ObNumber &res, bool did_carry);
        // @note dividend and quotient(or remainder) can refer to the same object
        static void div_uint32(const ObNumber &dividend, uint32_t divisor, ObNumber &quotient,
                              ObNumber &remainder);
        static void add_words(const ObNumber &n1, const ObNumber &n2, ObNumber &res);
        static int mul_words(const ObNumber &n1, const ObNumber &n2, ObNumber &res);
        static void div_words(const ObNumber &dividend, const ObNumber &divisor,
                             ObNumber &quotient, ObNumber &remainder);
        static void knuth_div_unsigned(const uint32_t *dividend, const ObNumber &divisor,
                              ObNumber &quotient, int8_t i, ObNumber &remainder);
        // @note n1(or n2) and res can refer to the same object
        static void sub_words_unsigned(const ObNumber &n1, const ObNumber &n2, ObNumber &res);
        static int compare_words_unsigned(const ObNumber &n1, const ObNumber &n2);
      private:
        // data members
        int8_t reserved1_;
        int8_t reserved2_;
        int8_t vscale_;         // scale of the value
        int8_t nwords_;         // valid range [1, MAX_NWORDS]
        uint32_t words_[MAX_NWORDS];
    };

    inline void ObNumber::set_zero()
    {
      nwords_ = 1;
      words_[0] = 0;
    }

    inline bool ObNumber::is_zero() const
    {
      return 1 == nwords_ && 0 == words_[0];
    }

    inline bool ObNumber::is_negative() const
    {
      return nwords_ > 0 && (words_[nwords_-1] & SIGN_MASK) != 0;
    }

    inline int8_t ObNumber::get_vscale() const
    {
      return vscale_;
    }

    inline int8_t ObNumber::get_nwords() const
    {
      return nwords_;
    }

    inline const uint32_t *ObNumber::get_words() const
    {
      return words_;
    }

    std::ostream & operator<<(std::ostream &os, const ObNumber& num); // for google test
  } // end namespace common
} // end namespace oceanbase

#endif /* _OB_NUMBER_H */
