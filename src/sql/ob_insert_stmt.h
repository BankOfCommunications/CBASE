#ifndef OCEANBASE_SQL_INSERTSTMT_H_
#define OCEANBASE_SQL_INSERTSTMT_H_
#include "ob_stmt.h"
#include <stdio.h>
#include "common/ob_array.h"
#include "common/ob_string.h"
#include "common/ob_string_buf.h"
#include "ob_sequence_stmt.h"//add lijianqiang [sequence insert] 20150608

namespace oceanbase
{
  namespace sql
  {
    class ObInsertStmt : public ObStmt
        //add lijianqiang [sequence insert] 20150615:b
        /*Exp:将ObSequenceStmt独立出来，让使用它的继承*/
        ,public ObSequenceStmt
        //add 20150615:e
    {
    public:
      ObInsertStmt(oceanbase::common::ObStringBuf* name_pool);
      virtual ~ObInsertStmt();

      void set_insert_table(uint64_t id);
      void set_insert_query(uint64_t id);
      void set_replace(bool is_replace);
      void set_values_size(int64_t size);
      int add_value_row(oceanbase::common::ObArray<uint64_t>& value_row);
      bool is_replace() const;
      uint64_t get_table_id() const;
      uint64_t get_insert_query_id() const;
      int64_t get_value_row_size() const;
      const oceanbase::common::ObArray<uint64_t>& get_value_row(int64_t idx) const;

      //add lijianqiang [sequence] 20150402:b
      void set_column_num_sum();
      inline uint64_t& get_column_num_sum();
      uint64_t get_col_sequence_map_flag(int64_t idx) const;
      inline void resolve_sequence_id_vectors();
      void complete_the_sequence_id_vectors();
      void set_sequence_nums();
      void reset_column_sequence_types();
      int add_next_prev_ids();
      int64_t get_next_prev_ids_size();
      common::ObArray<uint64_t>& get_next_prev_ids_array(int64_t idx);

      //add 20150402:e

      void print(FILE* fp, int32_t level, int32_t index);

      //add gaojt [Insert_Subquery_Function] [JHOBv0.1] 20150507:b
      void set_is_insert_multi_batch(bool is_insert_multi_batch){is_insert_multi_batch_ = is_insert_multi_batch;}
      bool get_is_insert_multi_batch(){return is_insert_multi_batch_;}
      //add gaojt 20150507:e

      //add dragon [varchar limit] 2016-8-9 14:54:46
      /**
       * @brief do_var_len_check: 检查varchar列的长度,满足schema的定义
       * @param result_plan 通过result_plan获取schema_check指针
       * @param cid 要进行检测的列的id
       * @param length insert...values..后变量的长度
       * @return 满足schema要求，返回成功；否则，返回相应错误码
       */
      int do_var_len_check(ResultPlan *result_plan, ObColumnInfo &cinfo, int64_t length);
      //add e
    private:
      uint64_t   table_id_;
      uint64_t   sub_query_id_; //用在insert...select...,对应于select子句的query_id
      bool       is_replace_;  // replace semantic
      //value_vectors_ 存储将要插入数据库的值
      oceanbase::common::ObArray<oceanbase::common::ObArray<uint64_t> > value_vectors_;
      //add lijianqiang [sequence] 20150401:b
      /*Exp:因为插入列可能有多行，每行可能有多个sequence（可能相同，可能不同），每列可能有多个sequence（可能相同，可能不同）
        多行之间也可能有多个不同的sequence
        例如：insert into test values
            (nextval for sequence_1,prevval for sequence_2+1*2,...),
            (nextval for sequence_1,prevval for sequence_1 * 5 + c1,...),
            (nextval for sequence_1 ,prevval for sequence_4 + c2,...)...
        增加一个 col_sequence_map_ 来与 next_prev_ids_ 相对应存储某行的某列的
        sequence ID：0为没有sequence，1对应有sequence*/
      oceanbase::common::ObArray<oceanbase::common::ObArray<uint64_t> >next_prev_ids_;//记录只出现sequence的列出现的sequence的类型和个数
      oceanbase::common::ObArray<uint64_t> col_sequence_types_;//构建next_prev_ids_ 辅助工具
      uint64_t column_num_sum_;//构造 col_sequence_map_ 使用
      oceanbase::common::ObArray<uint64_t> col_sequence_map_;//对应存储某行的某列是否有sequence，有 push 1 ，没有  push 0
      //add 20150401:e
	  bool is_insert_multi_batch_;//add gaojt [Insert_Subquery_Function] [JHOBv0.1] 20150507
    };

    inline void ObInsertStmt::set_insert_table(uint64_t id)
    {
      table_id_ = id;
    }

    inline void ObInsertStmt::set_insert_query(uint64_t id)
    {
      sub_query_id_ = id;
    }

    inline void ObInsertStmt::set_replace(bool is_replace)
    {
      is_replace_ = is_replace;
      if (is_replace_)
      {
        set_stmt_type(ObBasicStmt::T_REPLACE);
      }
      else
      {
        set_stmt_type(ObBasicStmt::T_INSERT);
      }
    }

    inline void ObInsertStmt::set_values_size(int64_t size)
    {
      return value_vectors_.reserve(size);
    }

    inline bool ObInsertStmt::is_replace() const
    {
      return is_replace_;
    }

    inline int ObInsertStmt::add_value_row(oceanbase::common::ObArray<uint64_t>& value_row)
    {
      return value_vectors_.push_back(value_row);
    }

    inline uint64_t ObInsertStmt::get_table_id() const
    {
      return table_id_;
    }

    inline uint64_t ObInsertStmt::get_insert_query_id() const
    {
      return sub_query_id_;
    }

    inline int64_t ObInsertStmt::get_value_row_size() const
    {
      return value_vectors_.count();
    }

    inline const oceanbase::common::ObArray<uint64_t>& ObInsertStmt::get_value_row(int64_t idx) const
    {
      OB_ASSERT(idx >= 0 && idx < value_vectors_.count());
      return value_vectors_.at(idx);
    }

    //add lijianqiang [sequence] 20150402:b
    inline void ObInsertStmt::set_column_num_sum()
    {
      column_num_sum_++;
    }
    inline uint64_t & ObInsertStmt::get_column_num_sum()
    {
      return column_num_sum_;
    }
    inline uint64_t ObInsertStmt::get_col_sequence_map_flag(int64_t idx) const
    {
      OB_ASSERT(idx >= 0 && idx < col_sequence_map_.count());
      return col_sequence_map_.at(idx);
    }

    inline void ObInsertStmt::resolve_sequence_id_vectors()//此接口只在解析每一列的表达式的时候遇到列中有sequence时用，标记对应列有无sequence
    {
      int64_t num = col_sequence_map_.count();
      for (int64_t i = num; i < static_cast<int64_t>(column_num_sum_) - 1; i++)
      {
        col_sequence_map_.push_back(0);//没有sequence的列
      }
      if (col_sequence_map_.count() == static_cast<int64_t>(column_num_sum_) - 1)
      {
        col_sequence_map_.push_back(1);//当前列有sequence,只push一次
      }

      if (is_next_type())//ture 为nextval
      {
          col_sequence_types_.push_back(NEXT_TYPE);
//          TBSYS_LOG(ERROR,"push next");
      }
      else//false为prevval
      {
          col_sequence_types_.push_back(PREV_TYPE);
//          TBSYS_LOG(ERROR, "push preval");
      }
    }
    inline void ObInsertStmt::complete_the_sequence_id_vectors()//处理最后一个使用sequence列之后所有的sequence列，全部填充 0
    {
      int64_t num = col_sequence_map_.count();
      for (int64_t i = num; i < static_cast<int64_t>(column_num_sum_); i++)
      {
        col_sequence_map_.push_back(0);//没有sequence的列
      }
    }
    inline void ObInsertStmt::reset_column_sequence_types()
    {
      col_sequence_types_.clear();
    }
    inline int ObInsertStmt::add_next_prev_ids()
    {
      return next_prev_ids_.push_back(col_sequence_types_);
    }
    inline int64_t ObInsertStmt::get_next_prev_ids_size()
    {
      return next_prev_ids_.count();
    }
    inline common::ObArray<uint64_t> & ObInsertStmt::get_next_prev_ids_array(int64_t idx)
    {
      OB_ASSERT(idx >= 0 && idx < next_prev_ids_.count());
      return next_prev_ids_.at(idx);
    }
    //add 20150402:e
  }
}

#endif //OCEANBASE_SQL_INSERTSTMT_H_
