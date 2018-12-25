//add lijianqiang [sequence] 20150423:b
/**
 * ob_sequence.h
 *
 * used for "sequence"
 *
 * Authors:
 * lijianqiang <lijianqiang@mail.nwpu.edu.cn>
 *
 */

#ifndef OB_OCEANBASE_SQL_SEQUENCE_H
#define OB_OCEANBASE_SQL_SEQUENCE_H

#include "common/hash/ob_hashmap.h"
#include "common/ob_string_buf.h"
#include "sql/ob_sequence_stmt.h"
#include "obmysql/ob_mysql_result_set.h"
#include "ob_sequence_define.h"

#define OB_SEQUENCE_SELECT_LIMIT 1
#define OB_SEQUENCE_INSERT_LIMIT 2
#define OB_SEQUENCE_SELECT_WHERE_LIMIT 3
#define OB_SEQUENCE_UPDATE_LIMIT 4

namespace oceanbase
{

  namespace sql
  {
    class ObSequence : public ObMultiChildrenPhyOperator
    {
      public:
        ObSequence();
        virtual ~ObSequence();
        virtual int open();
        virtual int close();
        virtual void reset();
        virtual void reuse();
        virtual bool is_sequence_cond_id(uint64_t cond_id) ;
        virtual int fill_the_sequence_info_to_cond_expr(ObSqlExpression *filter, uint64_t cond_expr_id) ;
        int get_next_row(const common::ObRow *&row);
        int get_row_desc(const common::ObRowDesc *&row_desc) const;
        int64_t to_string(char* buf, const int64_t buf_len) const;
        int64_t get_sequence_name_no_dup_size();
        ObString & get_sequence_name_no_dup(int64_t index);
        int add_sequence_info_to_map(ObString &sequence_name, SequenceInfo &s_info);//for gtest
        void set_stmt(ObSequenceStmt* stmt){sequence_stmt_ = stmt;}
        void set_use_lock_row_result_set(bool flag){use_lock_row_result_set_ = flag;}
        void set_sql_context(ObSqlContext *sql_context){sql_context_ = sql_context;}
        void set_result_set( ObResultSet * rs){this->out_result_set_ = rs;}
        void set_out_row_desc_and_row_desc_ext(ObRowDesc &row_desc, ObRowDescExt& row_desc_ext);//for column obj_cast check
        bool is_quick_path_sequence(ObString &sequence_name);
        bool can_fill_sequence_info(){return can_fill_;}
        int cons_sequence_row_desc(ObRowDesc& row_desc, const ObRowkeyInfo *&rowkey_info);
        int add_sequence_names_no_dup();
        int add_single_row_sequence_name(ObString &sequence_name);
        void reset_single_row_sequence_name();

        int get_only_update_nextval_sequence_info(const ObRow *&sequence_row, SequenceInfo &s_info);
        int get_sequence_data_type_info(const ObRow *&sequence_row, SequenceInfo &s_info, bool only_update_prevval);
        int get_sequence_current_value_info(const ObRow *&sequence_row, SequenceInfo &s_info, bool only_update_prevval, ObString &obj_current_value);
        int get_sequence_inc_by_value_info(const ObRow *&sequence_row, SequenceInfo &s_info, bool only_update_prevval, ObString &obj_increment_by);
        int get_sequence_min_value_info(const ObRow *&sequence_row, SequenceInfo &s_info, bool only_update_prevval);
        int get_sequence_max_value_info(const ObRow *&sequence_row, SequenceInfo &s_info, bool only_update_prevval);
        int get_sequence_cycle_value_info(const ObRow *&sequence_row, SequenceInfo &s_info, bool only_update_prevval);
        int get_sequence_valid_value_info(const ObRow *&sequence_row, SequenceInfo &s_info,bool only_update_prevval);
        int get_sequence_can_use_prev_value_info(const ObRow *&sequence_row, SequenceInfo &s_info, bool update_prevval, ObString &obj_current_value);
        int get_sequence_use_quick_path_value_info(const ObRow *&sequence_row, SequenceInfo &s_info, bool only_update_prevval);

        int update_sequence_info_map(bool update_prevval = true, bool update_nextval = true);
        int update_sequence_info_map_for_prevval(ObString &sequence_name, bool update_prevval = true);
        int get_prevval_by_name(ObString *sequence_name, ObString &prevval, bool &can_use_prevval);
        int get_nextval_by_name(ObString *sequence_name, ObString &nextval, bool &can_use_nextval);
        int init_sequence_info_map();
        int cast_sequence_info_to_decimal(SequenceInfo &s_value,
                                          ObDecimal &dec_current_value,
                                          ObDecimal &dec_min_value,
                                          ObDecimal &dec_max_value,
                                          ObDecimal &dec_inc_by);
        int calc_nextval_for_sequence(SequenceInfo &s_value);
        int calc_prevval_for_sequence(SequenceInfo &s_value);
        int build_next_value_for_sequence(ObString &sequence_name,  SequenceInfo &s_value);
        int update_by_dml(ObString &sequence_name, SequenceInfo &s_info);
        int prepare_lock_row(ObString &sequence_name);//for update and delete
        int prepare_sequence_prevval();
        int check_lock_condition();
        int start_sequence_trans();
        int end_sequence_trans();

        //do_fill for insert, select,batch update
        int do_simple_fill(ObSEArray<ObObj,64> &post_expr_array, int64_t idx, int dml_type, bool is_const = false);
        int do_complex_fill(ObSEArray<ObObj,64> &post_expr_array, int dml_type);
        void recover_the_sequence_expr(ObSqlExpression &expr, ObArray<std::pair<ObString,uint64_t> >&names_types_array);
        //insert
        void init_global_prevval_state_and_lock_sequence_map();
        void reset_nextval_state_map();
        void need_to_update_prevval(ObString &sequence_name, bool &update_prevval);
        void need_to_update_nextval(ObString &sequence_name, bool &update_nextval);
        bool can_use_current_nextval(ObString &sequence_name);
        bool can_lock_current_sequence(ObString &sequence_name);
        void update_nextval_state_map(ObString &sequence_name);
        void update_prevval_state_map(ObString &sequence_name);
        void update_lock_sequence_map(ObString &sequence_name);
        int get_nextval_prevval_map_idx(ObString & sequence_name);
        bool get_cur_seuqnece_type_for_insert();
        bool get_cur_seuqnece_type_for_select(int dml_type, int &ret);
        //update
        bool get_cur_sequence_type_for_update();
        void set_project_for_update(ObProject * project){project_for_update_ = project;}
        int add_nextval_to_project_op(ObString& nextval);
        void reset_col_sequence_types_idx();
        int add_idx_of_next_value_to_project_op(int64_t idx);
        int add_expr_idx_of_out_put_columns_to_project_op(int64_t idx);
        //insert update
        int check_column_cast_validity(ObSqlExpression& expr, const int64_t &column_idx);

      private:
        // disallow copy
        ObSequence(const ObSequence &other);
        ObSequence& operator=(const ObSequence &other);
      protected:
        //for sequence column obj_cast
        ObRowDesc out_row_desc_;//check if the column with current sequence is over flow or cast failed
        ObRowDescExt out_row_desc_ext_;//for type
        //for sequence info get
        ObSequenceStmt * sequence_stmt_;
        //for sequence mapping
        bool sequence_info_map_inited_;
        common::hash::ObHashMap<common::ObString, common::SequenceInfo> sequence_info_map_;//stote column info from "__all_sequence",will be updated after each row filling
        //for lock
        ObSqlContext *sql_context_;//for schema lock and rpc proxy
        ObResultSet *out_result_set_;//out result,
        ObRow lock_row_;//for lock use,only has one cell(rowkey cell)
        ObRowDesc lock_row_desc_;//for lock use,only has one cell(rowkey cell)
        bool has_cons_lock_row_desc_;
        common::ObStringBuf name_pool_;//for map
        ObMySQLResultSet my_result_set_;//for lock row use
        bool has_client_trans_;//if U start trancaction in client,this mark will be true
        ObArray<ObString> sequence_name_no_dup_;//copy from sequence_stmt.h,for check and fill use
        //for quick path
        ObArray<ObString> single_row_sequence_names_;//for quick path,will be cleared after eache row was filled withe the "sequence value"

        //insert
        ObArray<ObArray<uint64_t> > sequence_columns_mark_;//store if the column has sequence or not,"1" marks exist, "0" marks not exist
        ObArray<ObArray<uint64_t> > col_sequence_types_;//store the the column sequences where sequence_columns_mark_ marks "1",one column may have one or more sequence(s) with different types
        int64_t col_sequence_types_idx_;//recond current index of col_sequence_types_, start with "0",the first dimension of "col_sequence_types_"
        int64_t col_sequence_types_idx_idx_;//recond the second dimension index of "col_sequence_types_"
        //select
        int64_t  sequence_names_idx_for_select_;
        int64_t  sequence_pair_idx_for_where_;
        ObArray<int64_t > sequence_idx_in_expr_;
        //for select and update
        common::ObArray<common::ObArray<std::pair<common::ObString,uint64_t> > > sequence_name_type_pairs_;
        //for update
        ObProject * project_for_update_;//this object is only used for update,store the sequence nextval into project for batch update
        int64_t out_put_columns_idx_;//this idx is used to construct the assist info if update to project op,mark the idx of out_put columns sequence which has nextval
    private:
        bool use_lock_row_result_set_;
        bool can_fill_;
        bool has_checked_;//we check the sequence info you input before the sequence replace


        //insert, select (实现update, delete多条时，下面三个map可供参考使用)
        int single_row_nextval_state_map[OB_MAX_SEQUENCE_USE_IN_SINGLE_STATEMENT];//标记每个sequence的nextval使用状态，单行内有效
        int global_prevval_state_map[OB_MAX_SEQUENCE_USE_IN_SINGLE_STATEMENT];//标记每个sequence的prevval使用状态，所有行内有效
        int lock_sequence_map[OB_MAX_SEQUENCE_USE_IN_SINGLE_STATEMENT];

        //for using of filling the sequence into column
        const static int OB_NOT_VARCHAR_OBJ = 100;
        const static int OB_NOT_VALID_SEQUENCE_VARCHAR = 101;

    };
    inline int ObSequence::add_sequence_names_no_dup()
    {
      int ret = OB_SUCCESS;
      if (NULL == sequence_stmt_)
      {
        ret = OB_NOT_INIT;
        TBSYS_LOG(ERROR,"the sequence_stmt_ is not init!");
      }
      else
      {
        int64_t row_size = sequence_stmt_->get_sequence_names_no_dup_size();
        for (int64_t i = 0; OB_SUCCESS == ret && i < row_size; i++)
        {
//          TBSYS_LOG(ERROR,"add sequence[%.*s]",sequence_stmt_->get_sequence_no_dup_name(i).length(),sequence_stmt_->get_sequence_no_dup_name(i).ptr());
          ret = sequence_name_no_dup_.push_back(sequence_stmt_->get_sequence_name_by_idx(i));
        }
      }
      return ret;
    }
    inline ObString & ObSequence::get_sequence_name_no_dup(int64_t index)
    {
      OB_ASSERT(0 <= index && sequence_name_no_dup_.count() > index);
      return sequence_name_no_dup_.at(index);
    }
    inline int64_t ObSequence::get_sequence_name_no_dup_size()
    {
      return sequence_name_no_dup_.count();
    }
    inline int ObSequence::check_lock_condition()
    {
      int ret = OB_SUCCESS;
      if (NULL == sql_context_)
      {
        ret = OB_NOT_INIT;
        TBSYS_LOG(WARN, "the lock conditon not inited!");
      }
      return ret;
    }
    inline void ObSequence::set_out_row_desc_and_row_desc_ext(ObRowDesc &row_desc, ObRowDescExt& row_desc_ext)
    {
      this->out_row_desc_ = row_desc;
      this->out_row_desc_ext_ = row_desc_ext;
    }
    inline bool ObSequence::is_quick_path_sequence(ObString &sequence_name)
    {
      bool ret = false;
      int err = OB_SUCCESS;
      SequenceInfo s_info;
      if (!sequence_info_map_inited_)
      {
        TBSYS_LOG(WARN, "sequence map not init!");
      }
      else
      {
        if (-1 == (err = sequence_info_map_.get(sequence_name,s_info)))
        {
          TBSYS_LOG(ERROR,"get (%.*s)  map failed!",sequence_name.length(),sequence_name.ptr());
        }
        else if (hash::HASH_EXIST == err)//
        {
          if (1 == s_info.use_quick_path_)//"0" no quick path ,"1" quick path here
          {
            ret = true;
          }
        }
      }
      return ret;
    }
    //do_fill for insert, select, batch update
    inline void ObSequence::init_global_prevval_state_and_lock_sequence_map()
    {
      //每个sequence一个标记量
      for (int64_t i = 0; i < OB_MAX_SEQUENCE_USE_IN_SINGLE_STATEMENT; i++)
      {
        //标记“0”:该sequence在目前为止的表达式中还未使用
        //标记“1”:该sequence使用过next型，没有使用过prevval型
        global_prevval_state_map[i] = 0;
        //标记“0”:该sequence 还未加过锁，可以加锁
        //标记“1“:该sequence 已经加过锁了
        lock_sequence_map[i] = 0;
      }
    }
    inline  int ObSequence::get_nextval_prevval_map_idx(ObString & sequence_name)
    {
      int ret = -1;
      int64_t num = sequence_name_no_dup_.count();
      for (int64_t i = 0; i < num; i++)
      {
        if (sequence_name == sequence_name_no_dup_.at(i))//find
        {
          ret = (int)(i);
          break;
        }
      }
      return ret;
    }

   inline int ObSequence::add_single_row_sequence_name(ObString &sequence_name)
   {
     bool can_push = true;
     int ret = OB_SUCCESS;
     for (int64_t i = 0; i < single_row_sequence_names_.count(); i++)//for each sequence,just push once
     {
       if (sequence_name == single_row_sequence_names_.at(i))
       {
         can_push = false;
       }
     }
     if (can_push)
     {
       ret =  single_row_sequence_names_.push_back(sequence_name);
     }
     return ret;
   }
   inline void ObSequence::reset_single_row_sequence_name()
   {
     single_row_sequence_names_.clear();
   }
   inline int ObSequence::add_nextval_to_project_op(ObString& nextval)
   {
     int ret = OB_SUCCESS;
     if (NULL != project_for_update_)
     {
       ret = project_for_update_->add_nextval_for_update(nextval);
     }
     else
     {
       ret = OB_NOT_INIT;
       TBSYS_LOG(ERROR,"the project is not init!ret=[%d]",ret);
     }
     return ret;
   }
   inline int ObSequence::add_idx_of_next_value_to_project_op(int64_t idx)
   {
     int ret = OB_SUCCESS;
     if (NULL != project_for_update_)
     {
       ret = project_for_update_->add_idx_of_next_value(idx);
     }
     else
     {
       ret = OB_NOT_INIT;
       TBSYS_LOG(ERROR,"the project is not init!ret=[%d]",ret);
     }
     return ret;
   }
   inline int ObSequence::add_expr_idx_of_out_put_columns_to_project_op(int64_t idx)
   {
     int ret = OB_SUCCESS;
     if (NULL != project_for_update_)
     {
       ret = project_for_update_->add_expr_idx_of_out_put_columns(idx);
     }
     else
     {
       ret = OB_NOT_INIT;
       TBSYS_LOG(ERROR,"the project is not init!ret=[%d]",ret);
     }
     return ret;
   }

   inline void ObSequence::reset_col_sequence_types_idx()
   {
     col_sequence_types_idx_ = 0;
     col_sequence_types_idx_idx_ =0;
   }

  }//end namespace sql

}//end namespace oceanbase


#endif // OB_SEQUENCE_H
//add 20150423:e
