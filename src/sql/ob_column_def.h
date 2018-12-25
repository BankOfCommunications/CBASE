/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_column_def.h
 *
 * Authors:
 *   Guibin Du <tianguan.dgb@taobao.com>
 *
 */
#ifndef OCEANBASE_SQL_OB_COLUMN_DEF_H_
#define OCEANBASE_SQL_OB_COLUMN_DEF_H_
#include "common/ob_object.h"
#include "common/ob_string.h"
#include "common/ob_define.h"

namespace oceanbase
{
  namespace sql
  {
    enum AlterAction
    {
      NONE_ACTION,
      ADD_ACTION,
      DROP_ACTION,
      ALTER_ACTION,
	  //add fyd [NotNULL_check] [JHOBv0.1] 20140108:b
	  ALTER_ACTION_NULL,
	  //add 20140108:e
      RENAME_ACTION,
    };

    enum DropBehavior
  	{
  	  NONE_BEHAVIOR,
  	  CASCADE_BEHAVIOR,
  	  RESTRICT_BEHAVIOR,
  	};

    struct ObColumnDef
    {
      ObColumnDef()
      {
        column_id_ = common::OB_INVALID_ID;
        action_ = NONE_ACTION;
        drop_behavior_ = NONE_BEHAVIOR;
        data_type_ = common::ObMinType;
        type_length_ = -1;
        precision_ = -1;
        //modify fanqiushi DECIMAL OceanBase_BankCommV0.3 2014_7_19:b
        /*
         *如果用户建表时decimal只有一个参数，则scale默认为0
         */
        //scale_ = -1;    //old code
        scale_=0;
        //modify:e
        not_null_ = false;
        atuo_increment_ = false;
        primary_key_id_ = 0;
        default_value_.set_type(common::ObMinType);
      }

      void print_indentation(FILE* fp, int32_t level) const;
      void print(FILE* fp, int32_t level);

      uint64_t            column_id_;
      common::ObString    column_name_;
      common::ObString    new_column_name_;
      AlterAction         action_;
      DropBehavior        drop_behavior_; // for alter column drop
      common::ObObjType   data_type_;
      int64_t             type_length_;   // for some type only
      int64_t             precision_;     // for some type only
      int64_t             scale_;         // for some type only
      int64_t             primary_key_id_;
      bool                not_null_;
      bool                atuo_increment_;
      common::ObObj       default_value_;
    };

    inline void ObColumnDef::print_indentation(FILE* fp, int32_t level) const
    {
      for(int i = 0; i < level; ++i)
        fprintf(fp, "    ");
    }
    inline void ObColumnDef::print(FILE* fp, int32_t level)
    {
      print_indentation(fp, level);
      fprintf(fp, "column_id : %ld\n", column_id_);
      print_indentation(fp, level);
      fprintf(fp, "column_id : %ld\n", column_id_);
      print_indentation(fp, level);
      fprintf(fp, "column_name : %.*s\n", column_name_.length(), column_name_.ptr());
      switch (action_)
      {
        case ADD_ACTION:
          print_indentation(fp, level);
          fprintf(fp, "Alter Action = ADD_ACTION\n");
          break;
        case DROP_ACTION:
          print_indentation(fp, level);
          fprintf(fp, "Alter Action = DROP_ACTION\n");
          break;
        case ALTER_ACTION:
		//add fyd [NotNULL_check] [JHOBv0.1] 20140108:b
		case ALTER_ACTION_NULL:
		//add 20140108:e
          print_indentation(fp, level);
          fprintf(fp, "Alter Action = ALTER_ACTION\n");
          break;
        case RENAME_ACTION:
          print_indentation(fp, level);
          fprintf(fp, "Alter Action = RENAME_ACTION\n");
          print_indentation(fp, level);
          fprintf(fp, "column_name : %.*s\n", new_column_name_.length(), new_column_name_.ptr());
          break;
        default:
          break;
      }

      if (data_type_ == common::ObVarcharType)
      {
        print_indentation(fp, level);
        fprintf(fp, "type_length : %ld\n", type_length_);
      }
      if (data_type_ == common::ObDecimalType
        || data_type_ == common::ObFloatType
        || data_type_ == common::ObPreciseDateTimeType
        //add peiouya [DATE_TIME] 20150906:b
        || data_type_ == common::ObTimeType
        || data_type_ == common::ObDateType
        //add 20150906:e
        )
      {
        print_indentation(fp, level);
        fprintf(fp, "precision : %ld\n", precision_);
      }
      if (data_type_ == common::ObDecimalType)
      {
        print_indentation(fp, level);
        fprintf(fp, "scale : %ld\n", scale_);
      }
      if (primary_key_id_ > 0)
      {
        print_indentation(fp, level);
        fprintf(fp, "primary_key_id : %ld\n", primary_key_id_);
      }
      print_indentation(fp, level);
      fprintf(fp, "not null : %s\n", not_null_ ? "TRUE" : "FALSE");
      if (atuo_increment_)
      {
        print_indentation(fp, level);
        fprintf(fp, "atuo_increment : TRUE\n");
      }
      switch(default_value_.get_type())
      {
        case common::ObNullType:
          print_indentation(fp, level);
          fprintf(fp, "default value : NULL\n");
          break;
        case common::ObIntType:
        {
          int64_t val = 0;
          default_value_.get_int(val);
          print_indentation(fp, level);
          fprintf(fp, "default value : %ld\n", val);
          break;
        }
        //add lijianqiang [INT_32] 20151008:b
        case common::ObInt32Type:
        {
          int32_t val32 = 0;
          default_value_.get_int32(val32);
          print_indentation(fp, level);
          fprintf(fp, "default value : %d\n", val32);
          break;
        }
        //add 20151008:e
        case common::ObFloatType:
        {
          float val = 0;
          default_value_.get_float(val);
          print_indentation(fp, level);
          fprintf(fp, "default value : %f\n", val);
          break;
        }
        case common::ObDoubleType:
        {
          double val = 0;
          default_value_.get_double(val);
          print_indentation(fp, level);
          fprintf(fp, "default value : %lf\n", val);
          break;
        }
        case common::ObDateTimeType:
        case common::ObPreciseDateTimeType:
        {
          common::ObPreciseDateTime val = 0;
          default_value_.get_precise_datetime(val);
          print_indentation(fp, level);
          fprintf(fp, "default value : %ld\n", val);
          break;
        }
        //add peiouya [DATE_TIME] 20150906:b
        case common::ObDateType:
        {
          common::ObDate val = 0;
          default_value_.get_date (val);
          print_indentation(fp, level);
          fprintf(fp, "default value : %ld\n", val);
          break;
        }
        case common::ObTimeType:
        {
          common::ObTime val = 0;
          default_value_.get_time (val);
          print_indentation(fp, level);
          fprintf(fp, "default value : %ld\n", val);
          break;
        }
        //add 20150906:e
        case common::ObVarcharType:
        {
          common::ObString val;
          default_value_.get_varchar(val);
          print_indentation(fp, level);
          fprintf(fp, "default value : %.*s\n", val.length(), val.ptr());
          break;
        }
        case common::ObCreateTimeType:
        {
          common::ObModifyTime val = 0;
          default_value_.get_modifytime(val);
          print_indentation(fp, level);
          fprintf(fp, "default value : %ld\n", val);
          break;
        }
        case common::ObModifyTimeType:
        {
          common::ObCreateTime val = 0;
          default_value_.get_createtime(val);
          print_indentation(fp, level);
          fprintf(fp, "default value : %ld\n", val);
          break;
        }
        case common::ObBoolType:
        {
          bool val = false;
          default_value_.get_bool(val);
          print_indentation(fp, level);
          fprintf(fp, "default value : %s\n", val ? "true" : "false");
          break;
        }
        case common::ObDecimalType:
        {
          /*
          common::ObNumber val;
          default_value_.get_decimal(val);
          char buf[common::OB_MAX_TOKEN_BUFFER_LENGTH]; // just find a long enough macro
          int64_t len = val.to_string(buf, common::OB_MAX_TOKEN_BUFFER_LENGTH);
          print_indentation(fp, level);
          fprintf(fp, "default value : %.*s\n", static_cast<int32_t>(len), buf);
          break;
          */
          //modify wenghaixing DECIMAL OceanBase_BankCommV0.3 2014_7_19:b
            common::ObString val;
            default_value_.get_decimal(val);
            print_indentation(fp, level);
            fprintf(fp, "default value : %.*s\n", static_cast<int32_t>(val.length()), val.ptr());
          //modify e
          break;
        }
        default:
          /* do nothing */
          break;
      }
    }
  }
}

#endif /* OCEANBASE_SQL_OB_COLUMN_DEF_H_ */

