//add lijianqiang [sequence] 20150512:b
/**
 * ob_sequence_delete.h
 *
 * used for "DELETE FROM  table_name WHERE C1 = PREVVAL FOR sequence_name AND ..."
 *
 * Authors:
 * lijianqiang <lijianqiang@mail.nwpu.edu.cn>
 *
 */

#ifndef OB_OCEANBASE_SQL_SEQUENCE_DELETE_H
#define OB_OCEANBASE_SQL_SEQUENCE_DELETE_H

#include "ob_delete_stmt.h"
#include "ob_sequence.h"
namespace oceanbase
{
  namespace sql
  {
    class ObSequenceDelete: public ObSequence
    {
      public:
        ObSequenceDelete();
        virtual ~ObSequenceDelete();
        virtual int open();
        virtual int close();
        virtual void reset();
        virtual void reuse();
        int64_t to_string(char* buf, const int64_t buf_len) const;
        enum ObPhyOperatorType get_type() const { return PHY_SEQUENCE_DELETE; }
        int copy_sequence_info_from_delete_stmt();
        bool is_sequence_cond_id(uint64_t cond_id);
        int fill_the_sequence_info_to_cond_expr(ObSqlExpression *filter, uint64_t cond_expr_id);

      private:
        // disallow copy
        ObSequenceDelete(const ObSequenceDelete &other);
        ObSequenceDelete& operator=(const ObSequenceDelete &other);
      private:
        ObArray<uint64_t> sequence_expr_ids_;//the ids of exprs that has sequence
        ObArray<ObString> input_sequence_names_;//all sequences u input from the terminal
    };
    inline bool ObSequenceDelete::is_sequence_cond_id(uint64_t cond_id)
    {
      bool ret = false;
      for (int64_t i = 0; i < sequence_expr_ids_.count(); i++)
      {
        if (cond_id == sequence_expr_ids_.at(i))
        {
          ret = true;
          break;
        }
      }
      return ret;
    }
  } // end namespace sql
}
#endif // OB_SEQUENCE_DELETE_H
//add  20150512:e
