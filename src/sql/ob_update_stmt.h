#ifndef OCEANBASE_SQL_UPDATESTMT_H_
#define OCEANBASE_SQL_UPDATESTMT_H_
#include "ob_stmt.h"
#include <stdio.h>
#include "common/ob_array.h"
#include "common/ob_string.h"
#include "common/ob_string_buf.h"
#include "ob_sequence_stmt.h"//add lijianqiang [sequence update] 20150608
namespace oceanbase
{
  namespace sql
  {
    class ObUpdateStmt : public ObStmt
        //add lijianqiang [sequence update] 20150615:b
        /*Exp:将ObSequenceStmt独立出来，让使用它的继承*/
        ,public ObSequenceStmt
        //add 20150615:e
    {
    public:
      ObUpdateStmt(oceanbase::common::ObStringBuf* name_pool);
      virtual ~ObUpdateStmt();

      uint64_t set_update_table(uint64_t id)
      {
        if (id == oceanbase::common::OB_INVALID_ID)
          return oceanbase::common::OB_INVALID_ID;
        table_id_ = id;
        return id;
      }
      uint64_t get_update_table_id(void)
      {
        return table_id_;
      }      
      
      int add_update_column(uint64_t column_id)
      {
        int ret = common::OB_SUCCESS;
        if (column_id != oceanbase::common::OB_INVALID_ID)
          ret = update_columns_.push_back(column_id);
        return ret;
      }
      
      int get_update_column_id(int64_t idx, uint64_t &column_id)
      {
        int ret = common::OB_SUCCESS;
        if (idx < update_columns_.count())
        {
          column_id = update_columns_.at(idx);
        }
        else
        {
          ret = common::OB_INVALID_ARGUMENT;
        }
        return ret;
      }

      int add_update_expr(uint64_t expr_id)
      {
        int ret = common::OB_SUCCESS;
        if (expr_id == oceanbase::common::OB_INVALID_ID)
          ret = common::OB_ERROR;
        else
          ret = update_exprs_.push_back(expr_id);
        return ret;
      }

      int get_update_expr_id(int64_t idx, uint64_t &expr_id)
      {
        int ret = common::OB_SUCCESS;
        if (idx < update_exprs_.count())
        {
          expr_id = update_exprs_.at(idx);
        }
        else
        {
          ret = common::OB_INVALID_ARGUMENT;
        }
        return ret;
      }
     
      int64_t get_update_column_count(void)
      {
        return update_columns_.count();
      }

      void print(FILE* fp, int32_t level, int32_t index);
      //add lijianqiang [sequence update] 20150519:b
      void set_set_where_boundary(int64_t boundary);//the boundary between set clacuse and where clause
      int64_t get_set_where_boundary();
      int add_sequence_expr_id(uint64_t expr_id);
      int add_sequence_name_and_type(common::ObString &sequence_name);
      int add_sequence_expr_names_and_types();
      void reset_expr_types_and_names();
      int64_t get_sequence_expr_ids_size();
      uint64_t get_sequence_expr_id(int64_t idx);
      common::ObArray<std::pair<common::ObString,uint64_t> > &get_sequence_types_and_names_array(int64_t idx);
      int64_t get_sequence_types_and_names_array_size();
      //add 20150519:e
	   //add gaojt [Delete_Update_Function] [JHOBv0.1] 20160302:b
      void set_is_update_multi_batch(bool is_update_multi_batch){is_update_multi_batch_ = is_update_multi_batch;};
      bool get_is_update_multi_batch(){return is_update_multi_batch_;};
      void set_num_of_questionmark_in_assignlist(int question_num){questionmark_num_ = question_num; };
      int64_t get_questionmark_num_of_assignlist(){return questionmark_num_; };
      //add gaojt 20160302:e
      //add gaojt [Delete_Update_Function] [JHOBv0.1] 20160418:b
      void set_is_update_all_rowkey(bool is_update_all_rowkey){is_update_all_rowkey_ = is_update_all_rowkey;};
      bool get_is_update_all_rowkey(){return is_update_all_rowkey_;};
      //add gaojt 20160418:e
      //add lbzhong [Update rowkey] 20151221:b
      inline bool exist_update_column(const uint64_t column_id) const
      {
        for(int64_t i = 0; i < update_columns_.count(); i++)
        {
          if(update_columns_.at(i) == column_id)
          {
            return true;
          }
        }
        return false;
      }

      int get_update_expr_id(const uint64_t column_id, uint64_t &expr_id) const
      {
        int ret = OB_SUCCESS;
        int64_t idx = -1;
        for(int64_t i = 0; i < update_columns_.count(); i++)
        {
          if(update_columns_.at(i) == column_id)
          {
            idx = i;
            break;
          }
        }
        if(idx < 0 || idx >= update_exprs_.count())
        {
          ret = OB_ERR_COLUMN_NOT_FOUND;
        }
        else
        {
          expr_id = update_exprs_.at(idx);
        }
        return ret;
      }
      inline void get_update_columns(oceanbase::common::ObArray<uint64_t>& update_columns) const
      {
        update_columns = update_columns_;
      }
      //add:e
    private:
      uint64_t   table_id_;
      oceanbase::common::ObArray<uint64_t> update_columns_;
      oceanbase::common::ObArray<uint64_t> update_exprs_;
      //add lijianqiang [sequence update] 20150519:b
      int64_t set_clause_boundary_;//the boundary of set clause and where clause in "sequecne_expr_ids_"
      common::ObArray<uint64_t> sequecne_expr_ids_;//if current expr has sequence ,push expr_id,else push 0
      common::ObArray<common::ObArray<std::pair<common::ObString,uint64_t> > > sequence_name_type_pair_;//for sequence_names_ and sequence_types_
      common::ObArray<std::pair<common::ObString,uint64_t> > single_name_type_pair_;//for construct the "sequence_name_type_pair_"
      //add 20150519:e
      //add gaojt [Delete_Update_Function] [JHOBv0.1] 20160319:b
      bool is_update_multi_batch_;
      int64_t questionmark_num_;
      //add gaojt 20160319:e
      //add gaojt [Delete_Update_Function] [JHOBv0.1] 20160418:b
      bool is_update_all_rowkey_;
      //add gaojt 20160418:e
    };
    //add lijianqiang [sequence update] 20150519:b
    inline void ObUpdateStmt::set_set_where_boundary(int64_t boundary)
    {
      set_clause_boundary_ = boundary;
    }
    inline int64_t ObUpdateStmt::get_set_where_boundary()
    {
      return set_clause_boundary_;
    }
    inline int ObUpdateStmt::add_sequence_expr_id(uint64_t expr_id)
    {
      return sequecne_expr_ids_.push_back(expr_id);
    }
    inline int ObUpdateStmt::add_sequence_name_and_type(common::ObString &sequence_name)
    {
      int ret = common::OB_SUCCESS;
      std::pair<common::ObString,uint64_t> name_type;
      if (common::OB_SUCCESS != (ret = add_sequence_name_no_dup(sequence_name)))
      {
        TBSYS_LOG(ERROR,"add sequence name no dup error!");
      }
      if (common::OB_SUCCESS == ret)
      {
        name_type.first.assign_ptr(sequence_name.ptr(),sequence_name.length());
      }
      if (common::OB_SUCCESS == ret)
      {
        if (is_next_type())//ture nextval
        {
            name_type.second = common::NEXT_TYPE;
  //          TBSYS_LOG(ERROR,"push next");
        }
        else//false  prevval
        {
            name_type.second = common::PREV_TYPE;
  //          TBSYS_LOG(ERROR, "push preval");
        }
        ret = single_name_type_pair_.push_back(name_type);
      }
      return ret;
    }
    inline int ObUpdateStmt::add_sequence_expr_names_and_types()
    {
      int ret = common::OB_SUCCESS;
      ret = sequence_name_type_pair_.push_back(single_name_type_pair_);
      if (common::OB_SUCCESS != ret)
      {
        ret = common::OB_ERROR;
      }
      return ret;
    }
    inline void ObUpdateStmt::reset_expr_types_and_names()
    {
      single_name_type_pair_.clear();
    }
    inline int64_t ObUpdateStmt::get_sequence_expr_ids_size()
    {
      return sequecne_expr_ids_.count();
    }
    inline uint64_t ObUpdateStmt::get_sequence_expr_id(int64_t idx)
    {
     return sequecne_expr_ids_.at(idx);
    }
    inline common::ObArray<std::pair<common::ObString,uint64_t> > &ObUpdateStmt::get_sequence_types_and_names_array(int64_t idx)
    {
      OB_ASSERT(idx >= 0 && idx < sequence_name_type_pair_.count());
      return sequence_name_type_pair_.at(idx);
    }
    inline int64_t ObUpdateStmt::get_sequence_types_and_names_array_size()
    {
      return sequence_name_type_pair_.count();
    }
    //add 20150519:e
  }
}

#endif //OCEANBASE_SQL_UPDATESTMT_H_

