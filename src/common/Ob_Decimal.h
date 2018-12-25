#ifndef OB_DECIMAL_H
#define OB_DECIMAL_H
/*
 *  (C) 2014 OceanBase_Comm
 *
 *  This is head file for Ob_Decimal.
 *  It contain some const variable and tools function for decimal data type.
 *  To use it ,you should add it to oceanbase code file ,in src/common,
 *  and you should add this and .cpp file into makefile.am
 *
 *  To acomplish this,you should use it with ttmathint.h ttmathmisc.h and so on.
 *
 *         ????.cpp is for what ...
 *
 *  Version: $Id: ipvsadm.c,v 1.27 2014/5/22 16:00:07 wenghaixing Exp $
 *
 *  Authors:
 *     wenghaixing <kindaichweng@gmail.com>
 *        - some work details if you want
 *  Modify:
 *     wenghaixing <kindaichweng@gmail.com>
 */

#include <iostream>
#include "ttmathint.h"
#include <stdint.h>
namespace oceanbase {

namespace common {
//add by kindaich,use ttmath as TTInt to be word in ObDecimal
/*
 * in 64-bit machine,if size =2,that value can be -2^(64*2-1)to2^(64*2-1)
 * that would be 38 digit in string
 *
 * so if size=4
 *
 * that would be 76 digit in string
 */

//add wenghaixing DECIMAL OceanBase_BankCommV0.1 2014_5_22:b
typedef ttmath::Int<2> TTInt;
typedef ttmath::Int<4> TTCInt;
typedef ttmath::Int<8> TTLInt;
//static const int64_t kMaxScaleFactor = 1000000000000;
static const char _str_kMaxScaleFactor_38[40] = "9999999999"
		"9999999999"
		"9999999999"
		"99999999";
static const char _str_kMinScaleFactor_38[41] = "-9999999999"
		"9999999999"
		"9999999999"
		"99999999";
static const char _str_kMaxScaleFactor[40] = "10000000000"
		"0000000000"
		"0000000000"
		"0000000";
static const char _str_kMaxScaleFactor_29[32] = "10000000000"
		"0000000000"
		"000000000";
static const char _str_kMaxScaleFactor_16[18] = "10000000000"
		"00000";
static const TTCInt kMaxScaleFactor(_str_kMaxScaleFactor);
static const TTCInt kMaxScaleFactor_29(_str_kMaxScaleFactor_29);
static const TTInt  kMAX(_str_kMaxScaleFactor_38);
static const TTInt  kMin(_str_kMinScaleFactor_38);
static const int8_t MAX_DECIMAL_DIGIT = 38;
static const int8_t MAX_DECIMAL_SCALE = 37;
static const uint64_t MAX_PRINTABLE_SIZE = 128;
static const int8_t NWORDS = 1;
static const int8_t NTABLES = 2;
//static const char MIN_INT=-9223372036854775808;
//static const char MAX_INT=9223372036854775807;

class ObDecimal {

public:

	//for test OB_SUCCESS

	ObDecimal();
	~ObDecimal();

	ObDecimal(const ObDecimal &other);
	ObDecimal& operator=(const ObDecimal &other);

	void set_zero();
	bool is_zero() const;

	void from(uint32_t precision, uint32_t scale, uint32_t vscale,
			const TTInt words);
	int from(const char* buf, int64_t buf_len);
	int from(const char* str);
	int from(int64_t i64);


	int64_t to_string(char* buf, const int64_t buf_len) const;
	int to_int64(int64_t &i64) const;
	//to_int64 now is not to be used
	//int can_convert_to_int64() const;
	int cast_to_int64(int64_t &i64) const;
	//cast_to_int64 is not to be used;
	//add yushengjuan [INT_32] 20151009:b
	int cast_to_int32(int32_t &i32) const;
	//add 20151009:e

    //add xionghui [floor and ceil] 20151217:b
    int floor(ObDecimal &out) const;
    int ceil(ObDecimal &out) const;
    //add 20151217:e

    //add xionghui [round] 20150126:b
    int round(ObDecimal &out,int64_t num_digits) const;
    //add e:

	int round_to(uint32_t &vscale, TTInt &words) const;
	//round_to is not to be used;
	//int8_t get_vscale();

	TTInt *get_words();
	const TTInt * get_words_to_print() const;
	uint32_t get_precision() const;
	uint32_t get_scale() const;
	uint32_t get_vscale() const;
	void set_scale(uint32_t t);
	void set_precision(uint32_t t);
	void set_vscale(uint32_t t);
	void set_word(int i, uint64_t t);
	void reset();
	int modify_value(uint32_t p, uint32_t s);

	//add by wenghaixing 20140701
	char* get_ptr();
	char* get_ptr_()const;
	void set_ptr(char* in);
	//add e

	/**
	 * add by herilyn chou 20140425
	 */
	void print() const {
		std::cout << vscale_ << " " << precision_ << " " << scale_ << std::endl;
	}
	//arithmetic operators
	int add(const ObDecimal &other, ObDecimal &res) const;
	int sub(const ObDecimal &other, ObDecimal &res) const;
	int mul(const ObDecimal &other, ObDecimal &res) const;
	int div(const ObDecimal &other, ObDecimal &res) const;
	int negate(ObDecimal &res) const;
	int operator+(const ObDecimal &other) const;

	int compare(const ObDecimal &other) const;
	bool operator<(const ObDecimal &other) const;
	bool operator<=(const ObDecimal &other) const;
	bool operator>(const ObDecimal &other) const;
	bool operator>=(const ObDecimal &other) const;
	bool operator==(const ObDecimal &other) const;
	bool operator!=(const ObDecimal &other) const;

private:

	uint32_t vscale_;
	uint32_t precision_;
	uint32_t scale_;
	TTInt word[NWORDS];
	char* val_ptr;

};

inline char* ObDecimal::get_ptr_()const{

   return val_ptr;
}

inline char* ObDecimal::get_ptr(){

   return val_ptr;
}

inline void ObDecimal::set_ptr(char* in){

   val_ptr=in;
}

inline const TTInt *ObDecimal::get_words_to_print() const {

	return word;
}
;

inline uint32_t ObDecimal::get_vscale() const {
	return vscale_;
}

inline TTInt *ObDecimal::get_words() {

	return word;
}

inline uint32_t ObDecimal::get_precision() const {

	return precision_;
}

inline uint32_t ObDecimal::get_scale() const {

	return scale_;
}

inline void ObDecimal::set_zero() {

	word[0] = 0;

}

inline void ObDecimal::set_precision(uint32_t t) {

	precision_ = t;
}

inline void ObDecimal::set_scale(uint32_t t) {

	scale_ = t;
}

inline void ObDecimal::set_vscale(uint32_t t) {

	vscale_ = t;
}
//     inline bool ObDecimal::is_zero()const{
//
//         if(0==word[0])return true;
//         else return false;
//     }
inline bool ObDecimal::is_zero() const {
	TTInt zero = 0;
	return zero == word[0];
}

inline void ObDecimal::set_word(int i, uint64_t t) {

	word[0].table[i] = t;
}
std::ostream & operator<<(std::ostream &os, const ObDecimal& num);

}

}
//add :e
#endif // OB_DECIMAL_H
