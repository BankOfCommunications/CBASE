/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_number.cpp
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#include "ob_number.h"
#include "utility.h"

using namespace oceanbase::common;
int8_t ObNumber::QUOTIENT_SCALE = 38;

static const uint32_t POWER10[] = {
  1, 10, 100, 1000, 10000, 100000, 1000000, 10000000, 100000000, 1000000000
};

ObNumber::ObNumber()
  :reserved1_(0), reserved2_(0), vscale_(0), nwords_(1)
{
  words_[0] = 0;
}

void ObNumber::from(int8_t vscale, int8_t nwords, const uint32_t *words)
{
  OB_ASSERT(0 <= vscale);
  OB_ASSERT(0 <= nwords);
  OB_ASSERT(MAX_NWORDS >= nwords);
  OB_ASSERT(words);
  vscale_ = vscale;
  nwords_ = nwords;
  for (int8_t i = 0; i < nwords_; ++i)
  {
    words_[i] = words[i];
  }
}

ObNumber::~ObNumber()
{
}

ObNumber::ObNumber(const ObNumber &other)
{
  *this = other;
}

ObNumber& ObNumber::operator=(const ObNumber &other)
{
  if (OB_LIKELY(this != &other))
  {
    this->vscale_ = other.vscale_;
    this->nwords_ = other.nwords_;
    for (int8_t i = 0; i < other.nwords_; ++i)
    {
      this->words_[i] = other.words_[i];
    }
  }
  return *this;
}

inline int ObNumber::mul_uint32(const ObNumber &multiplicand, uint32_t multiplier, ObNumber &res, bool did_carry)
{
  int ret = OB_SUCCESS;
  int64_t carry = 0;
  int64_t new_carry = 0;
  OB_ASSERT(multiplicand.nwords_ <= MAX_NWORDS);
  OB_ASSERT(multiplicand.nwords_ > 0);
  for (int8_t i = 0; i < multiplicand.nwords_; ++i)
  {
    carry += static_cast<int64_t>(multiplier) * multiplicand.words_[i];
    if (carry >= BASE)
    {
      new_carry = carry / BASE;
      res.words_[i] = static_cast<uint32_t>(carry - (new_carry*BASE));
      carry = new_carry;
    }
    else
    {
      res.words_[i] = static_cast<uint32_t>(carry);
      carry = 0;
    }
  }
  res.nwords_ = multiplicand.nwords_;
  if (did_carry)
  {
    if (carry > 0)
    {
      if (OB_LIKELY(res.nwords_ < MAX_NWORDS - 1))
      {
        res.words_[res.nwords_++] = static_cast<uint32_t>(carry);
      }
      else
      {
        TBSYS_LOG(WARN, "multiply overflow, carry=%ld", carry);
        ret = OB_VALUE_OUT_OF_RANGE;
      }
    }
  }
  return ret;
}

inline void ObNumber::negate(const ObNumber &n, ObNumber &res)
{
  OB_ASSERT(n.nwords_ <= MAX_NWORDS);
  OB_ASSERT(n.nwords_ > 0);
  // 1. inverse
  res.nwords_ = n.nwords_;
  res.vscale_ = n.vscale_;
  for (int8_t i = 0; i < n.nwords_; ++i)
  {
    res.words_[i] = ~n.words_[i];
  }
  // 2. add one and ignore the final carry
  int64_t carry = 1;
  for (int8_t i = 0; i < res.nwords_; ++i)
  {
    carry += static_cast<int64_t>(res.words_[i]);
    if (carry >= BASE)
    {
      res.words_[i] = static_cast<uint32_t>(carry - BASE);
      carry = 1;
    }
    else
    {
      res.words_[i] = static_cast<uint32_t>(carry);
      carry = 0;
    }
  }
}

inline int ObNumber::add_uint32(const ObNumber &n1, uint32_t n2, ObNumber &res)
{
  int ret = OB_SUCCESS;
  OB_ASSERT(n1.nwords_ > 0);
  OB_ASSERT(n1.nwords_ <= MAX_NWORDS);
  int64_t carry = n2;
  for (int8_t i = 0; i < n1.nwords_; ++i)
  {
    carry += n1.words_[i];
    if (carry >= BASE)
    {
      res.words_[i] = static_cast<uint32_t>(carry - BASE);
      carry = 1;
    }
    else
    {
      res.words_[i] = static_cast<uint32_t>(carry);
      carry = 0;
      break;
    }
  }
  res.nwords_ = n1.nwords_;
  if (carry > 0)
  {
    if (OB_LIKELY(res.nwords_ < MAX_NWORDS - 1))
    {
      res.words_[res.nwords_++] = static_cast<uint32_t>(carry);
    }
    else
    {
      TBSYS_LOG(WARN, "add overflow, carry=%ld", carry);
      ret = OB_VALUE_OUT_OF_RANGE;
    }
  }
  return ret;
}

inline void ObNumber::div_uint32(const ObNumber &dividend, uint32_t divisor, ObNumber &quotient, ObNumber &remainder)
{
  OB_ASSERT(0 < dividend.nwords_);
  OB_ASSERT(MAX_NWORDS >= dividend.nwords_);
  OB_ASSERT(0 < divisor);
  int64_t carry = 0;
  for (int i = dividend.nwords_ - 1; i >= 0; --i)
  {
    carry = carry * BASE + dividend.words_[i];
    quotient.words_[i] = static_cast<uint32_t>(carry / divisor);
    carry = carry % divisor;
  }
  quotient.nwords_ = dividend.nwords_;
  quotient.remove_leading_zeroes();

  if (0 == carry)
  {
    remainder.set_zero();      // remainder is zero
  }
  else
  {
    remainder.words_[0] = static_cast<uint32_t>(carry);
    remainder.nwords_ = 1;
  }
}

inline int ObNumber::left_shift(int8_t i, bool did_carry)
{
  int ret = OB_SUCCESS;
  OB_ASSERT(0 < i && i < DOUBLE_PRECISION_NDIGITS);
  vscale_ = static_cast<int8_t>(vscale_ + i);
  while (9 < i)
  {
    if (OB_SUCCESS != (ret = mul_uint32(*this, POWER10[9], *this, did_carry)))
    {
      break;
    }
    i = static_cast<int8_t>(i - 9);
  }
  if (OB_SUCCESS == ret)
  {
    ret = mul_uint32(*this, POWER10[i], *this, did_carry);
  }
  return ret;
}

inline void ObNumber::right_shift(int8_t i)
{
  OB_ASSERT(0 < i && i <= vscale_);
  vscale_ = static_cast<int8_t>(vscale_ - i);
  ObNumber remainder;
  while (9 < i)
  {
    div_uint32(*this, POWER10[9], *this, remainder);
    i = static_cast<int8_t>(i - 9);
  }
  div_uint32(*this, POWER10[i], *this, remainder);
}

int64_t ObNumber::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  OB_ASSERT(nwords_ <= MAX_NWORDS);
  OB_ASSERT(nwords_ > 0);
  char inner_buf[MAX_PRINTABLE_SIZE];
  ObNumber clone;
  bool is_neg = is_negative();
  if (is_neg)
  {
    negate(*this, clone);
  }
  else
  {
    clone = *this;
  }
  ObNumber remainder;
  int i = MAX_PRINTABLE_SIZE;
  while (!clone.is_zero())
  {
    div_uint32(clone, 10, clone, remainder);
    if (remainder.is_zero())
    {
      inner_buf[--i] = '0';
    }
    else
    {
      OB_ASSERT(1 == remainder.nwords_);
      OB_ASSERT(remainder.words_[0] < 10 && remainder.words_[0] > 0);
      inner_buf[--i] = static_cast<char>('0' + remainder.words_[0]);
    }
    OB_ASSERT(0 < i);
  } // end while
  int64_t dec_digits_num = MAX_PRINTABLE_SIZE - i;
  if (0 == dec_digits_num)
  {
    databuff_printf(buf, buf_len, pos, "0");
  }
  else
  {
    if (is_neg)
    {
      databuff_printf(buf, buf_len, pos, "-");
    }
    if (clone.vscale_ >= dec_digits_num)
    {
      // 0.0...0xxx
      databuff_printf(buf, buf_len, pos, "0.");
      for (int j = 0; j < clone.vscale_ - dec_digits_num; ++j)
      {
        databuff_printf(buf, buf_len, pos, "0");
      }
      if (NULL != buf && 0 <= pos && dec_digits_num < buf_len - pos)
      {
        memcpy(buf+pos, inner_buf+i, dec_digits_num);
        pos += dec_digits_num;
        buf[pos] = '\0';
      }
    }
    else
    {
      if (NULL != buf && 0 <= pos && dec_digits_num-clone.vscale_ < buf_len - pos)
      {
        memcpy(buf+pos, inner_buf+i, dec_digits_num-clone.vscale_); // digits before the point
        pos += dec_digits_num-clone.vscale_;
      }
      if (clone.vscale_ > 0)
      {
        if (NULL != buf && 0 <= pos && 1 + clone.vscale_ < buf_len - pos)
        {
          buf[pos++] = '.';
          memcpy(buf+pos, inner_buf+i+dec_digits_num-clone.vscale_, clone.vscale_); // digits after the point
          pos += clone.vscale_;
        }
      }
      if (NULL != buf && 0 <= pos && 0 < buf_len - pos)
      {
        buf[pos] = '\0';
      }
    }
  }
  OB_ASSERT(pos <= buf_len);
  return pos;
}

int ObNumber::from(const char* str)
{
  return from(str, strlen(str));
}

/// @note This implementation is not correct generally, and we assume that the input string has passed the SQL parser.
int ObNumber::from(const char* str, int64_t buf_len)
{
  int ret = OB_SUCCESS;
  nwords_ = 4;
  memset(words_, 0, sizeof(words_));
  vscale_ = 0;
  bool is_negative = false;
  int8_t vscale = -1;
  for (int64_t i = 0; i < buf_len; ++i)
  {
    switch(*str)
    {
      case '0':
      case '1':
      case '2':
      case '3':
      case '4':
      case '5':
      case '6':
      case '7':
      case '8':
      case '9':
        if (OB_SUCCESS != (ret = this->mul_uint32(*this, 10, *this, true)))
        {
          break;
        }
        else if (OB_SUCCESS != (ret = add_uint32(*this, *str - '0', *this)))
        {
          break;
        }
        if (0 <= vscale)
        {
          ++vscale;
        }
        break;
      case '-':
        is_negative = true;
        break;
      case '.':
        vscale = 0;
        break;
      default:
        TBSYS_LOG(WARN, "invalid numeric char=%c", *str);
        ret = OB_ERR_UNEXPECTED;
        break;
    }
    if (OB_SUCCESS != ret)
    {
      break;
    }
    ++str;
  }
  if (SINGLE_PRECISION_NDIGITS < vscale)
  {
    TBSYS_LOG(WARN, "too many fractional digits, vscale=%hhd", vscale);
    ret = OB_VALUE_OUT_OF_RANGE;
  }
  else if (OB_SUCCESS == ret)
  {
    if (0 < vscale)
    {
      vscale_ = vscale;
    }
    if (is_negative)
    {
      negate(*this, *this);
    }
    remove_leading_zeroes();
  }
  if (OB_SUCCESS != ret)
  {
    set_zero();
  }
  return ret;
}

void ObNumber::from(int64_t i64)
{
  vscale_ = 0;
  nwords_ = 0;
  bool is_neg = i64 < 0;
  if (is_neg)
  {
    i64 = -i64;
  }
  int64_t new_i64 = 0;
  while(i64)
  {
    new_i64 = i64 / BASE;
    words_[nwords_++] = static_cast<uint32_t>(i64 - (BASE*new_i64)); // i64 % BASE
    i64 = new_i64;
  }
  if (0 == nwords_)
  {
    set_zero();
  }
  if (is_neg)
  {
    negate(*this, *this);
  }
}

bool ObNumber::can_convert_to_int64() const
{
  return vscale_ == 0 && 0 <= nwords_ && nwords_ <= 2;
}

int ObNumber::to_int64(int64_t &i64) const
{
  int ret = OB_SUCCESS;
  if (!can_convert_to_int64())
  {
    TBSYS_LOG(WARN, "the number cannot be converted to int64, vscale=%hhd nwords=%hhd",
              vscale_, nwords_);
    ret = OB_VALUE_OUT_OF_RANGE;
  }
  else
  {
    i64 = 0;
    bool is_neg = is_negative();
    const ObNumber *pos_num = this;
    ObNumber pos_this;
    if (is_neg)
    {
      negate(*this, pos_this);
      pos_num = &pos_this;
    }
    OB_ASSERT(pos_num->nwords_ <= 2);
    for (int i = pos_num->nwords_ - 1; i >= 0; --i)
    {
      i64 *= BASE;
      i64 += pos_num->words_[i];
    }
    if (is_neg)
    {
      i64 = -i64;
    }
  }
  return ret;
}

int ObNumber::cast_to_int64(int64_t &i64) const
{
  int ret = OB_SUCCESS;
  i64 = 0;
  bool is_neg = is_negative();
  ObNumber pos_clone = *this;
  if (is_neg)
  {
    negate(*this, pos_clone);
  }

  ObNumber remainder;
  int vscale = vscale_;
  int digits[DOUBLE_PRECISION_NDIGITS];
  int digit_idx = DOUBLE_PRECISION_NDIGITS - 1;
  while (!pos_clone.is_zero())
  {
    div_uint32(pos_clone, 10, pos_clone, remainder);
    if (vscale > 0)
    {
      vscale--;
    }
    else
    {
      OB_ASSERT(digit_idx >= 0);
      if (remainder.is_zero())
      {
        digits[digit_idx--] = 0;
      }
      else
      {
        OB_ASSERT(1 == remainder.nwords_);
        OB_ASSERT(remainder.words_[0] < 10 && remainder.words_[0] > 0);
        digits[digit_idx--] = remainder.words_[0];
      }
    } // end while
  }
  OB_ASSERT(digit_idx >= -1);
  for (int i = digit_idx+1; i < DOUBLE_PRECISION_NDIGITS; ++i)
  {
    i64 = i64 * 10 + digits[i];
  }
  if (is_neg)
  {
    i64 = -i64;
  }
  return ret;
}

inline void ObNumber::extend_words(int8_t nwords)
{
  OB_ASSERT(nwords > nwords_);
  OB_ASSERT(nwords <= MAX_NWORDS);
  if (is_negative())
  {
    for (int8_t i = nwords_; i < nwords; ++i)
    {
      words_[nwords_++] = UINT32_MAX;
    }
  }
  else
  {
    for (int8_t i = nwords_; i < nwords; ++i)
    {
      words_[nwords_++] = 0;
    }
  }
}

int ObNumber::round_to(int8_t precision, int8_t scale, int8_t &nwords, int8_t &vscale, uint32_t *words) const
{
  OB_ASSERT(precision >= scale && 0 <= scale && NULL != words);
  int ret = OB_SUCCESS;
  ObNumber clone = *this;
  bool is_neg = is_negative();
  if (is_neg)
  {
    negate(clone, clone);
  }
  if (clone.vscale_ > scale)
  {
    clone.right_shift(static_cast<int8_t>(clone.vscale_ - scale));
    clone.remove_leading_zeroes();
  }
  ObNumber clone_clone = clone;
  int8_t vprec = 0;
  ObNumber remainder;
  while (!clone_clone.is_zero())
  {
    div_uint32(clone_clone, 10, clone_clone, remainder);
    clone_clone.remove_leading_zeroes();
    ++vprec;
  }
  if (vprec > precision)
  {
    ret = OB_VALUE_OUT_OF_RANGE;
    TBSYS_LOG(WARN, "value is not representable with the precision and scale, p=%hhd s=%hhd vp=%hhd vs=%hhd",
              precision, scale, vprec, this->vscale_);
  }
  else
  {
    if (is_neg)
    {
      negate(clone, clone);
    }
    nwords = clone.nwords_;
    vscale = clone.vscale_;
    for (int8_t i = 0; i < clone.nwords_; ++i)
    {
      words[i] = clone.words_[i];
    }
  }
  return ret;
}

void ObNumber::round_fraction_part(int8_t scale)
{
  ObNumber *clone = this;
  ObNumber neg_this;
  bool is_neg = is_negative();
  if (is_neg)
  {
    negate(*this, neg_this);
    clone = &neg_this;
  }
  if (vscale_ > scale)
  {
    clone->right_shift(static_cast<int8_t>(vscale_ - scale));
  }
  if (is_neg)
  {
    negate(*clone, *this);
  }
  remove_leading_zeroes();
}

int ObNumber::add(const ObNumber &other, ObNumber &res) const
{
  int ret = OB_SUCCESS;
  res.set_zero();
  ObNumber n1 = *this;
  ObNumber n2 = other;
  if (n1.vscale_ > SINGLE_PRECISION_NDIGITS)
  {
    n1.round_fraction_part(SINGLE_PRECISION_NDIGITS);
  }
  if (n2.vscale_ > SINGLE_PRECISION_NDIGITS)
  {
    n2.round_fraction_part(SINGLE_PRECISION_NDIGITS);
  }
  int8_t res_nwords = static_cast<int8_t>(std::max(n1.nwords_, n2.nwords_) + 1);
  if (res_nwords > MAX_NWORDS)
  {
    TBSYS_LOG(WARN, "number out of range");
    ret = OB_VALUE_OUT_OF_RANGE;
  }
  else
  {
    n1.extend_words(res_nwords);
    n2.extend_words(res_nwords);
  }
  if (n1.vscale_ > n2.vscale_)
  {
    ret = n2.left_shift(static_cast<int8_t>(n1.vscale_ - n2.vscale_), false);
  }
  else if (n1.vscale_ < n2.vscale_)
  {
    ret = n1.left_shift(static_cast<int8_t>(n2.vscale_ - n1.vscale_), false);
  }
  if (OB_SUCCESS == ret)
  {
    add_words(n1, n2, res);
    res.remove_leading_zeroes();
  }
  return ret;
}

void ObNumber::add_words(const ObNumber &n1, const ObNumber &n2, ObNumber &res)
{
  OB_ASSERT(n1.nwords_ > 0);
  OB_ASSERT(n1.nwords_ <= MAX_NWORDS);
  OB_ASSERT(n1.nwords_ == n2.nwords_);
  OB_ASSERT(n1.vscale_ == n2.vscale_);
  res.nwords_ = n1.nwords_;
  res.vscale_ = n1.vscale_;
  int64_t carry = 0;
  for (int8_t i = 0; i < n1.nwords_; ++i)
  {
    carry += static_cast<int64_t>(n1.words_[i]) + n2.words_[i];
    if (carry >= BASE)
    {
      res.words_[i] = static_cast<uint32_t>(carry - BASE);
      carry = 1;
    }
    else
    {
      res.words_[i] = static_cast<uint32_t>(carry);
      carry = 0;
    }
  }
}

void ObNumber::remove_leading_zeroes()
{
  for (int i = nwords_ - 1; i > 0; --i)
  {
    if (UINT32_MAX == words_[i])
    {
      if ((words_[i-1] & SIGN_MASK) != 0)
      {
        --nwords_;             // remove zero
      }
      else
      {
        break;
      }
    }
    else if (0 == words_[i])
    {
      if ((words_[i-1] & SIGN_MASK) == 0)
      {
        --nwords_;             // remove zero
      }
      else
      {
        break;
      }
    }
    else
    {
      break;
    }
  } // end for
}

int ObNumber::sub(const ObNumber &other, ObNumber &res) const
{
  int ret = OB_SUCCESS;
  ObNumber neg_other = other;
  if (neg_other.nwords_ >= MAX_NWORDS)
  {
    TBSYS_LOG(WARN, "value out of range for sub");
    ret = OB_VALUE_OUT_OF_RANGE;
  }
  else
  {
    neg_other.extend_words(static_cast<int8_t>(neg_other.nwords_ + 1));
    negate(neg_other, neg_other);
    neg_other.remove_leading_zeroes();
    ret = add(neg_other, res);
  }
  return ret;
}

int ObNumber::compare(const ObNumber &other) const
{
  int ret = 0;
  ObNumber res;
  if (OB_SUCCESS != this->sub(other, res))
  {
    // return 0 even if error occur
  }
  else if (res.is_negative())
  {
    ret = -1;
  }
  else if (!res.is_zero())
  {
    ret = 1;
  }
  return ret;
}

bool ObNumber::operator<(const ObNumber &other) const
{
  int cmp = this->compare(other);
  return cmp < 0;
}

bool ObNumber::operator<=(const ObNumber &other) const
{
  int cmp = this->compare(other);
  return cmp <= 0;
}

bool ObNumber::operator>(const ObNumber &other) const
{
  int cmp = this->compare(other);
  return cmp > 0;
}

bool ObNumber::operator>=(const ObNumber &other) const
{
  int cmp = this->compare(other);
  return cmp >= 0;
}

bool ObNumber::operator==(const ObNumber &other) const
{
  int cmp = this->compare(other);
  return cmp == 0;
}

bool ObNumber::operator!=(const ObNumber &other) const
{
  int cmp = this->compare(other);
  return cmp != 0;
}

int ObNumber::mul_words(const ObNumber &n1, const ObNumber &n2, ObNumber &res)
{
  int ret = OB_SUCCESS;
  OB_ASSERT(n1.nwords_ > 0);
  OB_ASSERT(n2.nwords_ > 0);
  res.nwords_ = static_cast<int8_t>(n1.nwords_ + n2.nwords_ + 1);
  if (OB_UNLIKELY(res.nwords_ > MAX_NWORDS))
  {
    TBSYS_LOG(WARN, "multiply precision overflow, res_nwords=%hhd", res.nwords_);
    ret = OB_VALUE_OUT_OF_RANGE;
  }
  else
  {
    // naive multiply algorithm
    for (int8_t i = 0; i < res.nwords_; ++i)
    {
      res.words_[i] = 0;
    }
    uint64_t carry = 0;
    uint64_t new_carry = 0;
    for (int8_t i = 0; i < n2.nwords_; ++i) // n2 as multiplier
    {
      carry = 0;
      for (int8_t j = 0; j < n1.nwords_; ++j) // n1 as multiplicant
      {
        carry += static_cast<uint64_t>(n1.words_[j]) * n2.words_[i];
        carry += res.words_[i+j];
        if (carry >= UBASE)
        {
          new_carry = carry / BASE;
          res.words_[i+j] = static_cast<uint32_t>(carry - new_carry * BASE);
          carry = new_carry;
        }
        else
        {
          res.words_[i+j] = static_cast<uint32_t>(carry);
          carry = 0;
        }
      } // end for
      if (carry > 0)
      {
        res.words_[i+n1.nwords_] = static_cast<uint32_t>(carry);
        carry = 0;
      }
    }   // end for
    OB_ASSERT(0 == carry);
  }
  return ret;
}

int ObNumber::mul(const ObNumber &other, ObNumber &res) const
{
  int ret = OB_SUCCESS;
  res.set_zero();
  if (!this->is_zero() && !other.is_zero())
  {
    ObNumber multiplicand = *this;
    ObNumber multiplier = other;
    bool res_is_neg = false;
    if (multiplicand.is_negative())
    {
      negate(multiplicand, multiplicand);
      res_is_neg = true;
    }
    if (multiplier.is_negative())
    {
      negate(multiplier, multiplier);
      res_is_neg = !res_is_neg;
    }
    if (multiplicand.vscale_ > SINGLE_PRECISION_NDIGITS)
    {
      multiplicand.round_fraction_part(SINGLE_PRECISION_NDIGITS);
    }
    if (multiplier.vscale_ > SINGLE_PRECISION_NDIGITS)
    {
      multiplier.round_fraction_part(SINGLE_PRECISION_NDIGITS);
    }
    res.vscale_ = static_cast<int8_t>(multiplicand.vscale_ + multiplier.vscale_);
    ret = mul_words(multiplicand, multiplier, res);
    res.remove_leading_zeroes();
    if(res_is_neg)
    {
      negate(res, res);
    }
  }
  return ret;
}

inline int ObNumber::compare_words_unsigned(const ObNumber &n1, const ObNumber &n2)
{
  int ret = 0;
  OB_ASSERT(n1.nwords_ <= MAX_NWORDS);
  OB_ASSERT(n2.nwords_ <= MAX_NWORDS);
  int8_t n = std::max(n1.nwords_, n2.nwords_);
  for (int i = n-1; i >= 0; --i)
  {
    if (i < n1.nwords_ && i >= n2.nwords_)
    {
      if (n1.words_[i] > 0)
      {
        ret = 1;
        break;
      }
    }
    else if (i < n2.nwords_ && i >= n1.nwords_)
    {
      if (n2.words_[i] > 0)
      {
        ret = -1;
        break;
      }
    }
    else
    {
      if (n1.words_[i] > n2.words_[i])
      {
        ret = 1;
        break;
      }
      else if (n1.words_[i] < n2.words_[i])
      {
        ret = -1;
        break;
      }
    }
  }
  return ret;
}

inline void ObNumber::sub_words_unsigned(const ObNumber &n1, const ObNumber &n2, ObNumber &res)
{
  OB_ASSERT(n1.nwords_ <= MAX_NWORDS);
  OB_ASSERT(n2.nwords_ <= MAX_NWORDS);
  res.nwords_ = std::max(n1.nwords_, n2.nwords_);
  int64_t borrow = 0;
  for (int8_t i = 0; i < res.nwords_; ++i)
  {
    if (i < n1.nwords_)
    {
      borrow += n1.words_[i];
    }
    if (i < n2.nwords_)
    {
      borrow -= n2.words_[i];
    }
    if (0 > borrow)
    {
      res.words_[i] = static_cast<uint32_t>(borrow + BASE);
      borrow = -1;
    }
    else
    {
      res.words_[i] = static_cast<uint32_t>(borrow);
      borrow = 0;
    }
  } // end for
}

inline void ObNumber::remove_leading_zeroes_unsigned()
{
  for (int i = nwords_ - 1; i > 0; --i)
  {
    if (0 == words_[i])
    {
      --nwords_;             // remove zero
    }
    else
    {
      break;
    }
  } // end for
}

void ObNumber::knuth_div_unsigned(const uint32_t *dividend,
                        const ObNumber &divisor,
                        ObNumber &quotient, int8_t qidx, ObNumber &remainder)
{
  OB_ASSERT(divisor.words_[divisor.nwords_-1] >= HALF_BASE); // (BASE ^ divisor_n)/2 <= divisor
  OB_ASSERT(0 <= qidx && qidx < MAX_NWORDS);
  ObNumber cdividend;
  cdividend.nwords_ = static_cast<int8_t>(divisor.nwords_ + 1);
  for (int8_t i = 0; i < cdividend.nwords_; ++i)
  {
    cdividend.words_[i] = dividend[i];
  }
  ObNumber base;
  base.from(BASE);
  ObNumber p;
  mul_words(base, divisor, p);
  p.remove_leading_zeroes_unsigned();

  int cmp = compare_words_unsigned(cdividend, p);
  if (cmp >= 0)
  {
    sub_words_unsigned(cdividend, p, cdividend);
    knuth_div_unsigned(cdividend.words_, divisor, quotient, qidx, remainder); // recursively called at most once
    quotient.words_[qidx+1] = 1;
  }
  else
  {
    int8_t n = divisor.nwords_;
    uint64_t q = (UBASE * dividend[n] + dividend[n-1]) / divisor.words_[n-1];
    if (q >= UBASE)
    {
      q = UBASE - 1;
    }
    ObNumber Q;
    Q.from(q);
    ObNumber T;
    mul_words(Q, divisor, T);
    T.remove_leading_zeroes_unsigned();
    for (int i = 0; i < 2; ++i)
    {
      cmp = compare_words_unsigned(T, cdividend);
      if (cmp > 0)
      {
        Q.from(--q);
        sub_words_unsigned(T, divisor, T);
      }
      else
      {
        break;
      }
    } // end for
    if (Q.nwords_ == 1)
    {
      quotient.words_[qidx] = Q.words_[0];
    }
    else
    {
      OB_ASSERT(Q.is_zero());
      quotient.words_[qidx] = 0;
    }
    sub_words_unsigned(cdividend, T, remainder);
  }
}

inline void ObNumber::div_words(const ObNumber &dividend, const ObNumber &divisor, ObNumber &quotient, ObNumber &remainder)
{
  OB_ASSERT(!dividend.is_zero());
  OB_ASSERT(!divisor.is_zero());
  OB_ASSERT(0 < dividend.nwords_ && dividend.nwords_ <= MAX_NWORDS);
  OB_ASSERT(0 < divisor.nwords_ && divisor.nwords_ <= MAX_NWORDS);

  if (1 == divisor.nwords_)
  {
    // fast algorithm
    div_uint32(dividend, divisor.words_[0], quotient, remainder);
  }
  else
  {
    // Knuth's algorithm, Volumn 2, Section 4.3, Page 235
    ObNumber cdivisor;
    ObNumber cdividend;
    if (divisor.words_[divisor.nwords_-1] < HALF_BASE)
    {
      // make sure (BASE ^ n)/2 <= divisor
      uint32_t factor = static_cast<uint32_t>(BASE / (divisor.words_[divisor.nwords_-1] + 1));
      mul_uint32(divisor, factor, cdivisor, true);
      mul_uint32(dividend, factor, cdividend, true);
      OB_ASSERT(cdivisor.words_[cdivisor.nwords_-1] >= HALF_BASE);
    }
    else
    {
      cdividend = dividend;
      cdivisor = divisor;
    }

    if (cdividend.nwords_ < cdivisor.nwords_) // include the case when dividend is zero
    {
      quotient.set_zero();
      remainder = dividend;
    }
    else if (cdividend.nwords_ == cdivisor.nwords_)
    {
      int cmp = compare_words_unsigned(cdividend, cdivisor);
      if (cmp < 0)
      {
        quotient.set_zero();
        remainder = dividend;
      }
      else
      {
        quotient.nwords_ = 1;
        quotient.words_[0] = 1;
        sub_words_unsigned(cdividend, cdivisor, remainder);
      }
    }
    else                        // dividend.nwords_ >= 1 + divosor.nwords_
    {
      quotient.nwords_ = static_cast<int8_t>(cdividend.nwords_ - cdivisor.nwords_ + 1);
      memset(quotient.words_, 0, sizeof(quotient.words_));
      for (int8_t i = static_cast<int8_t>(cdividend.nwords_ - cdivisor.nwords_ - 1); i >= 0; --i)
      {
        knuth_div_unsigned(&cdividend.words_[i], cdivisor, quotient, i, remainder);
        for (int8_t j = 0; j < cdivisor.nwords_; ++j)
        {
          cdividend.words_[i+j] = remainder.words_[j];
        }
      }
    }
  }
  if (quotient.nwords_ > 0
      && static_cast<int64_t>(quotient.words_[quotient.nwords_-1]) >= HALF_BASE)
  {
    quotient.words_[quotient.nwords_++] = 0; // quotient is always positive
  }
  if (remainder.nwords_ > 0
      && static_cast<int64_t>(remainder.words_[remainder.nwords_-1]) >= HALF_BASE)
  {
    remainder.words_[remainder.nwords_++] = 0; // remainder is always positive
  }
}

int ObNumber::div(const ObNumber &other, ObNumber &res) const
{
  int ret = OB_SUCCESS;
  res.set_zero();
  if (other.is_zero())
  {
    TBSYS_LOG(WARN, "divisor is zero");
    ret = OB_DIVISION_BY_ZERO;
  }
  else if (!this->is_zero())
  {
    res.vscale_ = QUOTIENT_SCALE;
    ObNumber dividend = *this;
    ObNumber divisor = other;
    ObNumber remainder;
    bool res_is_neg = false;
    if (dividend.is_negative())
    {
      negate(dividend, dividend);
      res_is_neg = true;
    }
    if (dividend.vscale_ > DOUBLE_PRECISION_NDIGITS)
    {
      dividend.round_fraction_part(DOUBLE_PRECISION_NDIGITS);
    }
    if (divisor.vscale_ > SINGLE_PRECISION_NDIGITS)
    {
      divisor.round_fraction_part(SINGLE_PRECISION_NDIGITS);
    }
    if (dividend.vscale_ < divisor.vscale_
        || res.vscale_ > dividend.vscale_ - divisor.vscale_)
    {
      if (OB_SUCCESS != (ret = dividend.left_shift(static_cast<int8_t>(res.vscale_ + divisor.vscale_ - dividend.vscale_), true)))
      {
        TBSYS_LOG(WARN, "left shift overflow, err=%d res_vscale=%hhd other_vscale=%hhd this_vscale=%hhd",
                  ret, res.vscale_, divisor.vscale_, dividend.vscale_);
      }
    }
    if (OB_LIKELY(OB_SUCCESS == ret))
    {
      if (divisor.is_negative())
      {
        negate(divisor, divisor);
        res_is_neg = !res_is_neg;
      }
      divisor.remove_leading_zeroes_unsigned();
      div_words(dividend, divisor, res, remainder);
      if (OB_UNLIKELY(res_is_neg))
      {
        negate(res, res);
      }
      res.remove_leading_zeroes();
    }
  }
  return ret;
}

int ObNumber::negate(ObNumber &res) const
{
  int ret = OB_SUCCESS;
  if (this->nwords_ >= MAX_NWORDS)
  {
    TBSYS_LOG(WARN, "value out of range to do negate");
    ret = OB_VALUE_OUT_OF_RANGE;
  }
  else
  {
    res = *this;
    res.extend_words(static_cast<int8_t>(this->nwords_ + 1));
    negate(res, res);
    res.remove_leading_zeroes();
  }
  return ret;
}

std::ostream & oceanbase::common::operator<<(std::ostream &os, const ObNumber& num)
{
  char buff[ObNumber::MAX_PRINTABLE_SIZE];
  num.to_string(buff, ObNumber::MAX_PRINTABLE_SIZE);
  os << buff;
  return os;
}
