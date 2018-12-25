////===================================================================
 //
 // ob_cellinfo_processor.h / hash / common / Oceanbase
 //
 // Copyright (C) 2010 Taobao.com, Inc.
 //
 // Created on 2010-09-27 by Yubai (yubai.lk@taobao.com) 
 //
 // -------------------------------------------------------------------
 //
 // Description
 //
 // ObCellInfo的预处理器
 // 由MemTable内部使用
 //
 // -------------------------------------------------------------------
 // 
 // Change Log
 //
////====================================================================

#ifndef  OCEANBASE_UPDATESERVER_CELLINFO_PROCESSOR_H_
#define  OCEANBASE_UPDATESERVER_CELLINFO_PROCESSOR_H_

#include "ob_define.h"
#include "ob_schema.h"
#include "ob_read_common_data.h"
#include "ob_object.h"
#include "ob_action_flag.h"

namespace oceanbase
{
  namespace common 
  {
    class CellInfoProcessor
    {
      public:
        inline CellInfoProcessor() : is_db_sem_(false), need_skip_(false), need_return_(false), cur_op_type_(0)
        {
        };
        inline ~CellInfoProcessor()
        {
        };
      public:
        // 返回语法是否合法
        inline bool analyse_syntax(ObMutatorCellInfo &mutator_cell_info)
        {
          bool bret = true;
          const ObCellInfo &cell_info = mutator_cell_info.cell_info;
          const ObObj &value = cell_info.value_;
          const ObObj &op_type = mutator_cell_info.op_type;
          if (ObExtendType == value.get_type())
          {
            switch (value.get_ext())
            {
              case ObActionFlag::OP_USE_DB_SEM:
                need_skip_ = true;
                is_db_sem_ = true;
                break;
              case ObActionFlag::OP_USE_OB_SEM:
                need_skip_ = true;
                is_db_sem_ = false;
                break;
              //add zhaoqiong [Truncate Table]:20160318:b
              case ObActionFlag::OP_TRUN_TAB:
                need_skip_ = false;
                cur_op_type_ = ObActionFlag::OP_TRUN_TAB;
                break;
              //add:e
              case ObActionFlag::OP_DEL_ROW:
                need_skip_ = false;
                cur_op_type_ = ObActionFlag::OP_DEL_ROW;
                break;
              case ObActionFlag::OP_NOP:
                need_skip_ = false;
                cur_op_type_ = ObActionFlag::OP_NOP;
                break;
              default:
                //TBSYS_LOG(WARN, "value invalid extend info %ld %s", value.get_ext(), print_cellinfo(&cell_info));
                bret = false;
                break;
            }
          }
          else if (ObExtendType == op_type.get_type()
                  && value.is_valid_type())
          {
            // op_type仅表示update和insert两种操作
            switch (op_type.get_ext() & ObActionFlag::OP_ACTION_FLAG_LOW_MASK)
            {
              case ObActionFlag::OP_UPDATE:
                need_skip_ = false;
                cur_op_type_ = ObActionFlag::OP_UPDATE;
                need_return_ = ObActionFlag::OP_RETURN_UPDATE_RESULT & op_type.get_ext() ? true : false;
                //mutator_cell_info.op_type.set_ext(ObActionFlag::OP_UPDATE);
                break;
              case ObActionFlag::OP_INSERT:
                need_skip_ = false;
                cur_op_type_ = ObActionFlag::OP_INSERT;
                need_return_ = ObActionFlag::OP_RETURN_UPDATE_RESULT & op_type.get_ext() ? true : false;
                //mutator_cell_info.op_type.set_ext(ObActionFlag::OP_UPDATE);
                break;
              default:
                //TBSYS_LOG(WARN, "op_type invalid extend info %ld %s", op_type.get_ext(), print_cellinfo(&cell_info));
                bret = false;
                break;
            }
          }
          else
          {
            //TBSYS_LOG(WARN, "no extend info in op_type or value op_type=%d value_type=%d %s",
            //          op_type.get_type(), value.get_type(), print_cellinfo(&cell_info));
            bret = false;
          }

          return bret;
        };

        // 基本的表明 列名 rowkey检查
        inline bool cellinfo_check(const ObCellInfo &cell_info)
        {
          bool bret = true;
          if (NULL == cell_info.row_key_.ptr() || 0 == cell_info.row_key_.length())
          {
            //TBSYS_LOG(WARN, "invalid rowkey %s", to_cstring(cell_info.row_key_));
            bret = false;
          }
          else if (NULL == cell_info.table_name_.ptr() || 0 == cell_info.table_name_.length())
          {
            TBSYS_LOG(WARN, "invalid table name ptr=%p length=%d",
                      cell_info.table_name_.ptr(), cell_info.table_name_.length());
            bret = false;
          }
          else if (ObActionFlag::OP_DEL_ROW != cur_op_type_
                  && ObActionFlag::OP_NOP != cur_op_type_
                  && (NULL == cell_info.column_name_.ptr() || 0 == cell_info.column_name_.length()))
          {
            TBSYS_LOG(WARN, "invalid column name ptr=%p length=%d op_type=%ld",
                      cell_info.column_name_.ptr(), cell_info.column_name_.length(), cur_op_type_);
            bret = false;
          }
          else
          {
            // do nothing
          }

          if (!bret)
          {
            //TBSYS_LOG(WARN, "invalid cell_info %s", print_cellinfo(&cell_info));
          }
          return bret;
        }

        // rowkey合法性与schema的匹配检查
        inline static bool cellinfo_check(const ObCellInfo &cell_info,
                                          const ObTableSchema &table_schema)
        {
          bool bret = true;
          const ObObj *objs = cell_info.row_key_.get_obj_ptr();
          if (cell_info.row_key_.get_obj_cnt() != table_schema.get_rowkey_info().get_size())
          {
            TBSYS_LOG(WARN, "length=%ld not match schema, schema=%ld",
                      cell_info.row_key_.get_obj_cnt(), table_schema.get_rowkey_info().get_size());
            bret = false;
          }
          else if (NULL == objs)
          {
            TBSYS_LOG(WARN, "rowkey obj null pointer");
            bret = false;
          }
          else
          {
            for (int32_t i = 0; i < cell_info.row_key_.get_obj_cnt(); i++)
            {
              const ObRowkeyColumn *rowkey_column = NULL;
              if (NULL == (rowkey_column = table_schema.get_rowkey_info().get_column(i)))
              {
                bret = false;
                break;
              }
			  //modify liyongfeng bank_comm 20140822:b
              //if (objs[i].get_type() != rowkey_column->type_) old code
			  if (objs[i].get_type() != ObNullType && objs[i].get_type() != rowkey_column->type_)
			  //modify:e
              {
                TBSYS_LOG(WARN, "rowkey column type does not match index=%d schema_type=%d rowkey_type=%d",
                          i, rowkey_column->type_, objs[i].get_type());
                bret = false;
                break;
              }
              if (ObVarcharType == rowkey_column->type_
                  && objs[i].get_val_len() > rowkey_column->length_)
              {
                TBSYS_LOG(WARN, "varchar column length too long index=%d schema_length=%ld rowkey_length=%d",
                          i, rowkey_column->length_, objs[i].get_val_len());
                bret = false;
                break;
              }
            }
          }

          if (!bret)
          {
            //TBSYS_LOG(WARN, "invalid cell_info %s", print_cellinfo(&cell_info));
          }
          return bret;
        }

        // column信息合法性与schema的匹配检查
        inline static bool cellinfo_check(const ObCellInfo &cell_info,
                                          const ObColumnSchemaV2 &column_schema)
        {
          bool bret = true;
          const ObObj &value = cell_info.value_;

          // 数据类型与schema匹配检查
          if (column_schema.get_type() != value.get_type()
              && ObNullType != value.get_type())
          {
            TBSYS_LOG(WARN, "schema_type=%d value_type=%d not match", column_schema.get_type(), value.get_type());
            bret = false;
          }
          else if (NULL != column_schema.get_join_info())
          {
            TBSYS_LOG(WARN, "column_id=%lu only depend by join table, cannot update", column_schema.get_id());
            bret = false;
          }
          else
          {
            if (ObVarcharType == value.get_type())
            {
              ObString tmp;
              if (OB_SUCCESS != value.get_varchar(tmp)
                  || tmp.length() > column_schema.get_size())
              {
                TBSYS_LOG(WARN, "invalid varchar schema_max_length=%ld ptr=%p length=%d",
                          column_schema.get_size(), tmp.ptr(), tmp.length());
                bret = false;
              }
            }
          }

          // 数据类型合法性检查
          if (bret)
          {
            switch (column_schema.get_type())
            {
              case ObCreateTimeType:
              case ObModifyTimeType:
                TBSYS_LOG(WARN, "cannot update value_type=%d", column_schema.get_type());
                bret = false;
                break;
              case ObSeqType:
                TBSYS_LOG(WARN, "not support seq_type now");
                bret = false;
                break;
              default:
                break;
            }
          }

          if (!bret)
          {
            //TBSYS_LOG(WARN, "invalid cell_info %s", print_cellinfo(&cell_info));
          }
          return bret;
        }

        // 当前是否执行db语义
        // true 表示执行db语义
        // false 表示执行ob语义
        inline bool is_db_sem() const
        {
          return is_db_sem_;
        };

        inline int64_t get_op_type() const
        {
          return cur_op_type_;
        };

        inline int64_t need_skip() const
        {
          return need_skip_;
        };

        inline bool need_return() const
        {
          return need_return_;
        };

      private:
        bool is_db_sem_;
        bool need_skip_;
        bool need_return_;
        int64_t cur_op_type_;
    };
  }
}

#endif //OCEANBASE_UPDATESERVER_CELLINFO_PREPROCESSOR_H_

