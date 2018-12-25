//add lijianqiang [sequence] 20150407:b
/**
 * ob_sequence_values.h
 *
 * used for "insert into ...values(nextval for sequence_name...)..."
 *
 * Authors:
 * lijianqiang <lijianqiang@mail.nwpu.edu.cn>
 *
 */

#ifndef OB_OCEANBASE_SQL_SEQUENCE_INSERT_H
#define OB_OCEANBASE_SQL_SEQUENCE_INSERT_H


#include "ob_sequence.h"
namespace oceanbase
{
  namespace sql
  {
    class ObSequenceInsert: public ObSequence
    {
      public:
        ObSequenceInsert();
        virtual ~ObSequenceInsert();
        virtual int open();
        virtual int close();
        virtual void reset();
        virtual void reuse();
        bool is_sequence_cond_id(uint64_t cond_id);
        int fill_the_sequence_info_to_cond_expr(ObSqlExpression *filter, uint64_t cond_expr_id);
        int64_t to_string(char* buf, const int64_t buf_len) const;
        enum ObPhyOperatorType get_type() const { return PHY_SEQUENCE_INSERT; }
        int copy_sequence_info_from_insert_stmt(const ObSEArray<int64_t, 64>& row_desc_map);
        int fill_sequence_info_to_input_values(ObExprValues* input_values, ObValues *sequence_values);
        int gen_static_for_duplication_check(ObValues *tem_table,ObExprValues* input_values);
        int do_fill(ObExprValues *input_values);

      private:
        // disallow copy
        ObSequenceInsert(const ObSequenceInsert &other);
        ObSequenceInsert& operator=(const ObSequenceInsert &other);
      private:
        uint64_t table_id_;//the table id witch you insert into,for static data construct
    };
  } // end namespace sql
}
#endif // OB_OCEANBASE_SQL_SEQUENCE_INSERT_H
//add 20150407:e
