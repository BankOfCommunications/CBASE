#ifndef OB_SEQUENCE_DEFINE_H
#define OB_SEQUENCE_DEFINE_H

#include <string>
#include "common/ob_object.h"
namespace oceanbase
{
  namespace common
  {
    const int OB_MAX_SEQUENCE_USE_IN_SINGLE_STATEMENT = 100;//一个语句中最多出现的不同sequence的个数
    //sequence columns define
    const char* const OB_SEQUENCE_NAME = "sequence_name";
    const char* const OB_SEQUENCE_DATA_TYPE = "data_type";
    const char* const OB_SEQUENCE_CURRENT_VALUE = "current_value";
    const char* const OB_SEQUENCE_INCREMENT_BY = "increment_by";
    const char* const OB_SEQUENCE_MIN_VALUE = "min_value";
    const char* const OB_SEQUENCE_MAX_VALUE = "max_value";
    const char* const OB_SEQUENCE_IS_CYCLE = "is_cycle";
    const char* const OB_SEQUENCE_CACHE_NUM = "cache_num";
    const char* const OB_SEQUENCE_IS_ORDER = "is_order";
    const char* const OB_SEQUENCE_IS_VALID = "is_valid";
    const char* const OB_SEQUENCE_CONST_START_WITH = "const_start_with";
    const char* const OB_SEQUENCE_CAN_USE_PREVVAL = "can_use_prevval";
    const char* const OB_SEQUENCE_USE_QUICK_PATH = "use_quick_path";
    //add liuzy 20150710:b
    const char* const OB_SEQUENCE_START_WITH = "start_with";
    const char* const OB_SEQUENCE_RESTART_WITH = "restart_with";
    const char* const OB_SEQUENCE_DEFAULT_MAX_VALUE = "2147483647";
    const char* const OB_SEQUENCE_DEFAULT_MIN_VALUE = "-2147483648";
    const char* const OB_SEQUENCE_DEFAULT_MAX_VALUE_FOR_INT64 = "9223372036854775807";
    const char* const OB_SEQUENCE_DEFAULT_MIN_VALUE_FOR_INT64 = "-9223372036854775808";
    const char* const OB_SEQUENCE_DEFAULT_NO_USE_CHAR = "0";
    const char* const OB_SEQUENCE_DEFAULT_START_CHAR = "1";
    const char* const OB_SEQUENCE_NEGATIVE_START_CHAR = "-1";
    const uint8_t OB_SEQUENCE_DEFAULT_DATA_TYPE = 0;//0: integer
    const uint8_t OB_SEQUENCE_DEFAULT_DATA_TYPE_FOR_INT64 = 64;//64: int64
    const uint64_t OB_SEQUENCE_DEFAULT_CACHE = 20;
    const int64_t OB_SEQUENCE_DEFAULT_NO_USE_VALUE = 0;
    const int64_t OB_SEQUENCE_DEFAULT_START_VALUE = 1;
    const int64_t OB_SEQUENCE_DEFAULT_VAILD = 1;
    //add 20150710:e
    //for sequence update
    const std::string SINGLE_QUOTATION = "\'";
    const std::string SEMICOLON =";";
    //for sequence transaction
    const std::string START_TRANS = "start transaction;";
    const std::string END_TRANS = "commit;";

    enum SequecneColumnId//the order can't be modified
    {
      NAME_ID = OB_APP_MIN_COLUMN_ID,
      DATA_TYPE_ID ,// =17,
      CURRENT_VALUE_ID ,//= 18,
      INC_BY_ID ,//= 19,
      MIN_VALUE_ID ,//= 20,
      MAX_VALUE_ID ,//= 21,
      CYCLE_ID ,//= 22,
      CACHE_NUM_ID ,//= 23,
      ORDER_ID ,//= 24,
      VALID_ID ,//= 25,
      CONST_START_WITH_ID ,//= 26,
      CAN_USE_PREVVAL_ID, //= 27,
      USE_QUICK_PATH_ID, //=28
      END_ID //=29
    };

    //used for hash map
    struct SequenceInfo
    {
        ObObj current_value_;//the newes value for current sequence
        int64_t data_type_;
        ObObj inc_by_;
        ObObj min_value_;
        ObObj max_value_;
        int64_t cycle_;
        int64_t cache_;
        int64_t order_;
        int64_t valid_;//current info is valid or not
        int64_t can_use_prevval_;//can't be used befor the next value used at least once
        int64_t use_quick_path_;//if true,we use the quick path to complete the sequence mission
        ObObj prevval_;
        SequenceInfo():current_value_(),data_type_(0),inc_by_(),min_value_(),max_value_(),
          cycle_(0),cache_(0),order_(0),valid_(1),can_use_prevval_(0),use_quick_path_(0),prevval_(){}
    };
  }//end namespace common
}
#endif // OB_SEQUENCE_DEFINE_H
