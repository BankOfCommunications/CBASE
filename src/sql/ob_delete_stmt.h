#ifndef OCEANBASE_SQL_DELETESTMT_H_
#define OCEANBASE_SQL_DELETESTMT_H_
#include "ob_stmt.h"
#include "common/ob_string.h"
#include "common/ob_string_buf.h"
//add lijianqiang [sequence delete] 20150514 b:
#include "common/ob_array.h"
#include "ob_sequence_stmt.h"
//add 20150514:e

namespace oceanbase
{
  namespace sql
  {
    class ObDeleteStmt : public ObStmt
        //add lijianqiang [sequence delete] 20150615:b
        /*Exp:将ObSequenceStmt独立出来，让使用它的继承*/
        ,public ObSequenceStmt
        //add 20150615:e
    {
    public:
      ObDeleteStmt(oceanbase::common::ObStringBuf* name_pool);
      virtual ~ObDeleteStmt();

      uint64_t set_delete_table(uint64_t id);
      uint64_t get_delete_table_id(void);
      //add lijianqiang [sequence delete] 20150514:b
      int add_sequence_name(common::ObString & sequence_name);
      int add_sequence_expr_id(uint64_t & sequence_expr_id);
      common::ObArray<common::ObString> & get_sequence_name_array();
      common::ObArray<uint64_t> & get_sequence_expr_ids_array();
      //add 20150514:e
      //add gaojt [Delete_Update_Function] [JHOBv0.1] 20160307:b
      void set_is_delete_multi_batch(bool is_delete_multi_batch){is_delete_multi_batch_ = is_delete_multi_batch;};
      bool get_is_detete_multi_batch(){return is_delete_multi_batch_;};
      //add gaojt 20160307:e
      //add gaojt [Delete_Update_Function] [JHOBv0.1] 20160418:b
      void set_is_delete_all_rowkey(bool is_delete_all_rowkey){is_delete_all_rowkey_ = is_delete_all_rowkey;};
      bool get_is_delete_all_rowkey(){return is_delete_all_rowkey_;};
      //add gaojt 20160418:e
      void print(FILE* fp, int32_t level, int32_t index);

    private:
      uint64_t table_id_;
      //add lijianqiang [sequence delete] 20150514:b
      common::ObArray<common::ObString> sequence_names_;//不去重，只要出现就增加
      common::ObArray<uint64_t> sequence_expr_ids_;
      //add 20150514:e
      //add gaojt [Delete_Update_Function] [JHOBv0.1] 20151229:b
      /*expr: fix the bug of when while there is none delete row */
      common::ObVector<uint64_t> self_construct_rowkey_ids_;
      //add gaojt 20151229:e
      bool is_delete_multi_batch_;//add gaojt [Delete_Update_Function] [JHOBv0.1] 20160307
      bool is_delete_all_rowkey_;//add gaojt [Delete_Update_Function] [JHOBv0.1] 20160418
    };

    inline uint64_t ObDeleteStmt::set_delete_table(uint64_t id)
    {
      table_id_ = id;
      return id;
    }

    inline uint64_t ObDeleteStmt::get_delete_table_id(void)
    {
      return table_id_;
    }

    //add lijianqiang [sequence delete] 20150514:b
    inline int ObDeleteStmt::add_sequence_name(common::ObString & sequence_name)
    {
      int ret = -1;
      if (common::OB_SUCCESS != (ret = add_sequence_name_no_dup(sequence_name)))
      {
        TBSYS_LOG(ERROR,"add sequence name no dup error!");
      }
      else
      {
        ret = sequence_names_.push_back(sequence_name);
      }
      return ret;
    }

    inline common::ObArray<common::ObString> & ObDeleteStmt::get_sequence_name_array()
    {
      return sequence_names_;
    }
    inline int ObDeleteStmt::add_sequence_expr_id(uint64_t & sequence_expr_id)
    {
      return sequence_expr_ids_.push_back(sequence_expr_id);
    }
    inline common::ObArray<uint64_t> & ObDeleteStmt::get_sequence_expr_ids_array()
    {
      return sequence_expr_ids_;
    }
    //add 20150514:e
  }
}

#endif //OCEANBASE_SQL_DELETESTMT_H_

