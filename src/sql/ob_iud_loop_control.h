//add gaojt [Insert_Subquery_Function] [JHOBv0.1] 20150507:b
/**
 * ob_bind_values.h
 *
 * Authors:
 *   gaojt
 * function:Insert_Subquery_Function
 *
 * used for insert ... select
 */
#ifndef _OB_IUD_LOOP_CONTROL_H_
#define _OB_IUD_LOOP_CONTROL_H_
#include "ob_single_child_phy_operator.h"
#include "common/ob_row.h"
// mod by maosy  [Delete_Update_Function]
#include "ob_fill_values.h"
#include "ob_ups_executor.h"
#include "ob_bind_values.h"
//extern bool is_multi_batch_over;
// mod e
namespace oceanbase
{
namespace sql
{
class ObIudLoopControl: public ObSingleChildPhyOperator
{
public:
  ObIudLoopControl();
  ~ObIudLoopControl();
  virtual int open();
  virtual int close();
  virtual void reset();
  virtual void reuse();

  int get_next_row(const common::ObRow *&row);
  int get_row_desc(const common::ObRowDesc *&row_desc) const;
  int64_t to_string(char* buf, const int64_t buf_len) const;
  // mod by maosy [Delete_Update_Function_isolation_RC] 20161218
//  void set_is_multi_batch(bool is_multi_batch){is_multi_batch_ = is_multi_batch;}
  void set_need_start_trans(bool need_start){need_start_trans_ = need_start ; }
  //mod e
  void get_affect_rows(bool& is_ud_original,
                       int64_t &threshhold,
                       ObFillValues *fill_values,
                       ObBindValues *bind_values);
  //del by maosy [Delete_Update_Function]
//  int64_t get_insert_affect_rows(){return affected_row_;};
  //del e
  //add  by maosy [Delete_Update_Function] 20160613:b
  void set_affect_row();
  bool is_batch_over(ObFillValues*& fill_values, ObBindValues *&bind_values);
  void get_fill_bind_values(ObFillValues*& fill_values,ObBindValues*& bind_values);
  // add e
  // mod by maosy [Delete_Update_Function_isolation_RC] 20161218
  //add by gaojt [Delete_Update_Function-Transaction] 201601206:b
//  void set_is_ud(bool is_ud){is_ud_ = is_ud;};
// mod e
  int execute_stmt_no_return_rows(const ObString &stmt);
  int start_transaction();
  int commit();
  int rollback();
  void set_sql_context(ObSqlContext sql_context){sql_context_ = sql_context;};
  //add by gaojt 201601206:e
  enum ObPhyOperatorType get_type() const{return PHY_IUD_LOOP_CONTROL;}
  DECLARE_PHY_OPERATOR_ASSIGN;
private:
  DISALLOW_COPY_AND_ASSIGN(ObIudLoopControl);
private:
  int64_t batch_num_;
  // mod by maosy  [Delete_Update_Function]
 // int64_t inserted_row_num_;
  int64_t affected_row_;
  // mod e
    int64_t threshold_;
    // mod by maosy [Delete_Update_Function_isolation_RC] 20161218
//  bool is_multi_batch_;
  bool need_start_trans_;//是否需要开始事务，只有在非hint的批量时在为true

  //add by gaojt [Delete_Update_Function-Transaction] 201601206:b
//  bool is_ud_;
//mod e
  ObSqlContext sql_context_;
  //add by gaojt 201601206:e
};
} // end namespace sql
} // end namespace oceanbase

#endif /* _OB_IUD_LOOP_CONTROL_H_*/
//add gaojt 20150507:e
