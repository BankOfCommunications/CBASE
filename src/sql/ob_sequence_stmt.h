//add lijianqiang [sequence] 20150608:b
/**
 * ob_sequence_stmt.h
 *
 * used for sequence of INSERT DELETE UPDATE SELECT
 *
 * Authors:
 * lijianqiang <lijianqiang@mail.nwpu.edu.cn>
 *
 */
#ifndef OB_SEQUENCE_STMT_H
#define OB_SEQUENCE_STMT_H

#include "common/ob_array.h"
#include "common/ob_string.h"
#include "ob_sequence_define.h"
#include "ob_item_type.h"
#include "parse_node.h"
using namespace oceanbase::common;

namespace oceanbase
{
  namespace common
  {
    enum SequenceType// the order can't be modified
    {
      NEXT_TYPE = 0,
      PREV_TYPE
    };
  }
  namespace sql
  {
    class ObSequenceStmt
    {
      public:
        ObSequenceStmt();
        virtual ~ObSequenceStmt();
        /*Exp:this enum corresponds columns of sequence system table*/
        enum SequenceColumn
        {
          DataType = 0,
          CurrentValue,
          IncrementBy,
          MinValue,
          MaxValue,
          Cycle,
          Cache,
          Order,
          Valid,
          ConstStartWith,
          CanUsePrevval,
          UseQuickPath,
          OptionFlag  /*this tag must be in the end*/
        };

        struct SequenceOption
        {
          enum OptionState
          {
            NoSet = 0,
            Set = 1
          };
          int is_set_;
          uint64_t expr_id_;
        };

        int init_sequence_option();
        //是否重置
        void set_replace(bool is_replace);
        bool is_replace();
        void set_sequence_expr(int column, uint64_t expr_id);
        uint64_t get_sequence_expr(int8_t idx);
        //判断重复选项
        int is_set_option(int column);
        int sequence_option_validity(int64_t dml_type);
        int sequence_cons_decimal_value(ObDecimal &res, ParseNode* node, const char* const node_name, uint8_t datatype = -1, int option = -1);
        void set_sequence_datatype(uint8_t datatype);
        uint8_t get_sequence_datatype();
        void set_sequence_startwith(ObDecimal startwith);
        void set_sequence_startwith(ObString startwith);
        ObDecimal get_sequence_startwith();
        void set_sequence_incrementby(ObDecimal incrementby);
        void set_sequence_incrementby(ObString incrementby);
        ObDecimal get_sequence_incrementby();
        void set_sequence_minvalue(ObDecimal minvalue);
        void set_sequence_minvalue(ObString minvalue);
        ObDecimal get_sequence_minvalue();
        void set_sequence_maxvalue(ObDecimal maxvalue);
        void set_sequence_maxvalue(ObString maxvalue);
        ObDecimal get_sequence_maxvalue();
        void set_sequence_cache(uint64_t cache);
        uint64_t get_sequence_cache();
        int generate_decimal_value(uint8_t datatype, ObDecimal &res, int option = -1);//calculate decimal boundary value
        //add 20150623:e
        bool has_sequence(){return (sequence_names_no_dup_.count() > 0);}
        bool is_next_type(){return is_next_type_;}
		void set_is_next_type(bool is_next_type){is_next_type_ = is_next_type;}
        void set_current_expr_has_sequence(bool is_has_sequence){current_expr_has_sequence_ = is_has_sequence;}
        bool current_expr_has_sequence(){return current_expr_has_sequence_;}
        int add_sequence_name_no_dup(common::ObString &sequence_name);
		int64_t get_sequence_names_no_dup_size();
        common::ObArray<common::ObString> & get_sequence_names_no_dup();
        const common::ObString & get_sequence_name_by_idx(int64_t idx)const;
        void print(FILE* fp, int32_t level, int32_t index);
      private:
        common::ObArray<common::ObString> sequence_names_no_dup_;//存储去重的所有sequence名字，物理计划判断sequence是否创建使用
        bool current_expr_has_sequence_;                         //逻辑计划解析sequence表达式的时候表示当前的表达式/列有无seuquence
        bool is_next_type_;                                      //当前sequence类型的标记，在select,update和insert中有用，delete中没有使用
      private:
        uint64_t cache_;
        bool is_replace_;
        SequenceOption seq_option_[OptionFlag];
        uint8_t data_type_; //0 : integer; n(1~31) : decimal(n,0)
        ObDecimal start_with_,increment_by_,
        min_value_,max_value_;
    };

    //add liuzy [sequence] 20150623:b
    inline int ObSequenceStmt::init_sequence_option()
    {
      int ret = OB_SUCCESS;
      for(int i = 0; i < OptionFlag; ++i)
      {
        seq_option_[i].is_set_ = SequenceOption::NoSet;
        seq_option_[i].expr_id_ = 0;
      }
      cache_ = OB_SEQUENCE_DEFAULT_CACHE;
      data_type_ = OB_SEQUENCE_DEFAULT_DATA_TYPE;
      if (OB_SUCCESS != (ret = min_value_.from(OB_SEQUENCE_DEFAULT_START_CHAR)))
      {
        TBSYS_LOG(ERROR, "Default value of minvalue covert to decimal failed.");
      }
      else if (OB_SUCCESS != (ret = max_value_.from(OB_SEQUENCE_DEFAULT_MAX_VALUE)))
      {
        TBSYS_LOG(ERROR, "Default value of maxvalue covert to decimal failed.");
      }
      else if (OB_SUCCESS != (ret = start_with_.from(OB_SEQUENCE_DEFAULT_START_CHAR)))
      {
        TBSYS_LOG(ERROR, "Default value of start with covert to decimal failed.");
      }
      else if(OB_SUCCESS != (ret = increment_by_.from(OB_SEQUENCE_DEFAULT_START_CHAR)))
      {
        TBSYS_LOG(ERROR, "Default value of increment by covert to decimal failed.");
      }
      return ret;
    }
    inline void ObSequenceStmt::set_replace(bool is_replace)
    {
      //在ob_insert_stmt.h中需要调用set_stmt_type此处先注释
      is_replace_ = is_replace;
    }
    inline bool ObSequenceStmt::is_replace()
    {
      return is_replace_;
    }
    inline void ObSequenceStmt::set_sequence_expr(int column, uint64_t expr_id)
    {
      seq_option_[column].expr_id_ = expr_id;
      seq_option_[column].is_set_ = SequenceOption::Set;
    }
    inline uint64_t ObSequenceStmt::get_sequence_expr(int8_t idx)
    {
      return seq_option_[idx].expr_id_;
    }
    inline int ObSequenceStmt::is_set_option(int column)
    {
      int ret = SequenceOption::Set;
      if (SequenceOption::NoSet ==  seq_option_[column].is_set_)
        ret = SequenceOption::NoSet;
      return ret;
    }
    //DataType: get(), set()
    inline void ObSequenceStmt::set_sequence_datatype(uint8_t datatype)
    {
      data_type_ = datatype;
    }
    inline uint8_t ObSequenceStmt::get_sequence_datatype()
    {
      return data_type_;
    }
    //StartWith: get(), set()
    inline void ObSequenceStmt::set_sequence_startwith(ObDecimal startwith)
    {
      start_with_ = startwith;
    }
    inline void ObSequenceStmt::set_sequence_startwith(ObString startwith)
    {
      start_with_.from(startwith.ptr(), startwith.length());
    }
    inline ObDecimal ObSequenceStmt::get_sequence_startwith()
    {
      return start_with_;
    }
    //IncrementBy: get(), set()
    inline void ObSequenceStmt::set_sequence_incrementby(ObDecimal incrementby)
    {
      increment_by_ = incrementby;
    }
    inline void ObSequenceStmt::set_sequence_incrementby(ObString incrementby)
    {
      increment_by_.from(incrementby.ptr(), incrementby.length());
    }
    inline ObDecimal ObSequenceStmt::get_sequence_incrementby()
    {
      return increment_by_;
    }
    //Minvalue: get(), set()
    inline void ObSequenceStmt::set_sequence_minvalue(ObDecimal minvalue)
    {
      min_value_ = minvalue;
    }
    inline void ObSequenceStmt::set_sequence_minvalue(ObString minvalue)
    {
      min_value_.from(minvalue.ptr(), minvalue.length());
    }
    inline ObDecimal ObSequenceStmt::get_sequence_minvalue()
    {
      return min_value_;
    }
    //Maxvalue: get(), set()
    inline void ObSequenceStmt::set_sequence_maxvalue(ObDecimal maxvalue)
    {
      max_value_ = maxvalue;
    }
    inline void ObSequenceStmt::set_sequence_maxvalue(ObString maxvalue)
    {
      max_value_.from(maxvalue.ptr(), maxvalue.length());
    }
    inline ObDecimal ObSequenceStmt::get_sequence_maxvalue()
    {
      return max_value_;
    }
    //Cache: get(), set()
    inline void ObSequenceStmt::set_sequence_cache(uint64_t cache)
    {
      cache_ = cache;
    }
    inline uint64_t ObSequenceStmt::get_sequence_cache()
    {
      return cache_;
    }
    inline int ObSequenceStmt::generate_decimal_value(uint8_t datatype, ObDecimal &res, int option)
    {
      int ret = OB_SUCCESS;
      if(0 == datatype)
      {
        if (MinValue == option)
        {
          if (OB_SUCCESS != (ret = res.from(OB_SEQUENCE_DEFAULT_MIN_VALUE)))
          {
            TBSYS_LOG(ERROR, "Convert default min value to deciaml failed.");
          }
        }
        else if(OB_SUCCESS != (ret = res.from(OB_SEQUENCE_DEFAULT_MAX_VALUE)))
        {
          TBSYS_LOG(ERROR, "Convert default max value to deciaml failed.");
        }
      }
      else
      {
        std::string sign("-");
        std::string temp(datatype, '9');
        if (MinValue == option)
        {
          temp = sign + temp;
        }
        const char* result = temp.c_str();
        if (OB_SUCCESS != (ret = res.from(result)))
        {
          TBSYS_LOG(ERROR, "Convert bound value to deciaml failed.");
        }
      }
      return ret;
    }
    //add 20150623:e
    inline int ObSequenceStmt::add_sequence_name_no_dup(common::ObString &sequence_name)
    {
      int ret = OB_SUCCESS;
      bool can_push = true;
      for (int64_t i = 0; i < sequence_names_no_dup_.count(); i++)
      {
        if (sequence_names_no_dup_.at(i) == sequence_name)
        {
          can_push = false;
          break;
        }
      }
      if (can_push)
      {
        ret = sequence_names_no_dup_.push_back(sequence_name);
      }
      return ret;
    }
    inline common::ObArray<common::ObString> & ObSequenceStmt::get_sequence_names_no_dup()
    {
      return sequence_names_no_dup_;
    }
    inline const common::ObString & ObSequenceStmt::get_sequence_name_by_idx(int64_t idx)const
    {
      OB_ASSERT(idx >= 0 && idx < sequence_names_no_dup_.count());
      return sequence_names_no_dup_.at(idx);
    }
    inline int64_t ObSequenceStmt::get_sequence_names_no_dup_size()
    {
      return sequence_names_no_dup_.count();
    }
  }
}


#endif // OB_SEQUENCE_STMT_H
//add  20150608:e
