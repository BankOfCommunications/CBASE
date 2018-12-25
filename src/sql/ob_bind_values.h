/**
 * ob_bind_values.h
 *
 * Authors:
 *   gaojt
 * function:
 * Insert_Subquery_Function
 *
 * used for insert ... select
 */
#ifndef _OB_BIND_VALUES_H_
#define _OB_BIND_VALUES_H_
#include "sql/ob_multi_children_phy_operator.h"
#include "common/ob_row.h"
#include "common/ob_rowkey.h"
#include "common/ob_array.h"
#include "common/ob_se_array.h"
#include "common/ob_schema.h"
#include "sql/ob_table_rpc_scan.h"
#include "sql/ob_sql_context.h"

namespace oceanbase
{
namespace sql
{
class ObBindValues: public ObMultiChildrenPhyOperator
{
public:
  ObBindValues();
  ~ObBindValues();
  virtual int open();
  virtual int close();
  virtual void reset();
  virtual void reuse();
  int get_next_row(const common::ObRow *&row);
  int get_row_desc(const common::ObRowDesc *&row_desc) const;
  int64_t to_string(char* buf, const int64_t buf_len) const;
  int set_row_desc_map(common::ObSEArray<int64_t, 64> &row_desc_map);
  int set_row_desc(const common::ObRowDesc &row_desc);
  int set_rowkey_info(const common::ObRowkeyInfo &rowkey_info);
  void set_table_id(uint64_t table_id);
  uint64_t get_table_id_();
  int generate_one_batch();
  int construct_in_left_part(ObSqlExpression *&rows_filter,
                             ExprItem& expr_item,
                             ObTableRpcScan *table_scan);
  int construct_in_right_part(const ObRow *row,
                              ObSqlExpression *&rows_filter,
                              ExprItem &expr_item,
                              oceanbase::sql::ObExprValues *expr_values,
                              ObValues *tmp_table,
                              int64_t &total_row_size);
  int construct_in_end_part(ObTableRpcScan *&table_scan,
                            ObSqlExpression *&rows_filter,
                            int64_t row_num,
                            ExprItem &expr_item);
  int generate_in_operator(ObPhyOperator *sub_query,
                           ObTableRpcScan *table_scan,
                           oceanbase::sql::ObExprValues *expr_values,
                           ObValues *tmp_table,
                           int64_t &row_num,
                           bool &is_close_sub_query);
  //add gaojt [Insert_Subquery_Function] [JHOBv0.1] 20141027
  int64_t get_inserted_row_num(){return row_num_;};
  void set_is_multi_batch(bool is_multi_batch){is_multi_batch_ = is_multi_batch;};
  // add by maosy [Delete_Update_Function]
  bool is_batch_over(){return is_batch_over_;}
  void set_max_row_value_size(int64_t max_row_value_size)
  {
      max_insert_value_size_ = max_row_value_size;
  }
  // add e
  //add gaojt [Insert_Subquery_Function_multi_key] 20160619:b
  int add_column_item(const ColumnItem& column_item);
  //add gaojt 20160619:e
  void get_threshhold(int64_t& threshhold){threshhold = max_rowsize_capacity_;};
  enum ObPhyOperatorType get_type() const{return PHY_BIND_VALUES;}
  DECLARE_PHY_OPERATOR_ASSIGN;
private:
  DISALLOW_COPY_AND_ASSIGN(ObBindValues);
private:
  static const int64_t MAX_INSERT_VALUE_SIZE = static_cast<int64_t>(0.8*2*1024L*1024L);
  static const int64_t MAX_INSERT_ROW_NUM = 20*256;
private:
  common::ObRowDesc row_desc_;
  common::ObRowkeyInfo rowkey_info_;
  common::ObSEArray<int64_t, 64> row_desc_map_;
  int is_reset_;/*Exp: whether reset the environment*/
  uint64_t table_id_;
  int64_t row_num_;
  bool is_multi_batch_;
  bool is_close_sub_query_;
  // add by maosy [Delete_Update_Function]
  bool is_batch_over_;
  // add e
  //add gaojt [Insert_Subquery_Function_multi_key] 20160619:b
  common::ObVector<ColumnItem>   column_items_;
  //add gaojt 20160619:e
  // add by maosy [Delete_Update_Function]20160619:b
  int64_t max_rowsize_capacity_ ;
  int64_t max_insert_value_size_;
  // add maosy 20160619:e
};
} // end namespace sql
} // end namespace oceanbase

#endif /* _OB_BIND_VALUES_H_*/
