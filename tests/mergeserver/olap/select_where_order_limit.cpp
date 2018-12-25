#include <arpa/inet.h>
#include "common/ob_define.h"
#include "select_where_order_limit.h"
#include "olap.h"
#include "../../common/test_rowkey_helper.h"
using namespace oceanbase;
using namespace oceanbase::common;
SelectWhereOrderLimit SelectWhereOrderLimit::static_case_;
static CharArena allocator_;
namespace 
{
  static const char * where_cond = "d = 255";
  static const uint64_t where_cond_id = 'd'-'a' + msolap::min_column_id;
  static const uint32_t where_right_operand = 255;
  static const int64_t expect_row_width  = msolap::max_column_id - msolap::min_column_id + 1;
  static const char * orderby_column = "z";
  static const int32_t orderby_desc = ObScanParam::DESC;
  static int64_t limit_offset = 0;
  static int64_t limit_count = 1024;
}

bool SelectWhereOrderLimit::check_result(const uint32_t min_key_include, const uint32_t max_key_include, ObScanner &result,
  void *arg)
{
  UNUSED(arg);
  bool res = true;
  int err = OB_SUCCESS;
  ObScanner::RowIterator it = result.row_begin();
  ObCellInfo *row_cells = NULL;
  int64_t row_width = 0;
  int64_t hit_row_count = 0;
  /// int64_t expect_hit_row_count = std::min<int64_t>((max_key_include - min_key_include + 1 + 255)/256,
  ///    limit_count);
  int64_t expect_hit_row_count = 0;
  uint32_t mask = 0xff;
  if (((max_key_include & (~mask))>>8) > ((min_key_include & (~mask))>>8))
  {
    expect_hit_row_count = ((max_key_include & (~mask))>>8) - ((min_key_include & (~mask))>>8) - 1;
    if ((max_key_include & mask) >= where_right_operand)
    {
      expect_hit_row_count ++;
    }
    if ((min_key_include & mask) <= where_right_operand)
    {
      expect_hit_row_count ++;
    }
  }
  else
  {
    if (((min_key_include & mask) <= where_right_operand) 
      &&((max_key_include & mask) >= where_right_operand))
    {
      expect_hit_row_count = 1;
    }
  }
  int64_t whole_row_count = expect_hit_row_count;
  expect_hit_row_count = std::min<int64_t>(expect_hit_row_count, limit_count);
  if (whole_row_count != result.get_whole_result_row_num())
  {
    TBSYS_LOG(WARN,"whole row count not correct [expect:%ld,got:%ld]", whole_row_count, 
      result.get_whole_result_row_num());
    err = OB_ERR_UNEXPECTED;
  }
  for (uint32_t row_key = max_key_include;
    (OB_SUCCESS == err) && (row_key >= min_key_include) && it != result.row_end(); 
    row_key --)
  {
    uint32_t big_endian_key = htonl(row_key);
    char key[32];
    snprintf(key, 32, "%d", big_endian_key);
    ObRowkey row_key_str;
    row_key_str = make_rowkey(key, &allocator_);
    uint32_t where_c_val = msolap::olap_get_column_val(where_cond_id, big_endian_key);
    if (where_c_val == where_right_operand)
    {
      if (OB_SUCCESS != (err = it.get_row(&row_cells,&row_width)))
      {
        TBSYS_LOG(WARN,"fail to get row from scanner [err:%d]", err);
      }
      else if (row_width != expect_row_width)
      {
        TBSYS_LOG(WARN,"row width not correct [row_width:%ld, expect_row_width:%ld]", row_width, expect_row_width);
        err = OB_ERR_UNEXPECTED;
      }
      else
      {
        for (uint64_t cid = msolap::min_column_id;(cid <= msolap::max_column_id) && (OB_SUCCESS == err); cid ++)
        {
          int64_t intval = 0;
          if (OB_SUCCESS != (err = row_cells[cid - msolap::min_column_id].value_.get_int(intval)))
          {
            TBSYS_LOG(WARN,"fail to get int val from obj [err:%d, cid:%lu]", err, cid);
          }
          else if (intval != msolap::olap_get_column_val(cid,big_endian_key))
          {
            TBSYS_LOG(WARN,"column value not correct [cid:%lu,expect_val:%u,real_val:%ld]", cid, 
              msolap::olap_get_column_val(cid,big_endian_key), intval);
            err = OB_ERR_UNEXPECTED;
          }
          if ((OB_SUCCESS == err) 
            && ((row_cells[cid - msolap::min_column_id].column_name_.length() != 1) 
            || (row_cells[cid - msolap::min_column_id].column_name_.ptr()[0] != msolap::olap_get_column_name(cid))) )
          {
            TBSYS_LOG(WARN,"unexpect column_name [cid:%lu,cname:%.*s]", cid, 
              row_cells[cid - msolap::min_column_id].column_name_.length(), 
              row_cells[cid - msolap::min_column_id].column_name_.ptr());
            err = OB_ERR_UNEXPECTED;
          }
          if ((OB_SUCCESS == err) && (row_cells[cid - msolap::min_column_id].table_name_ != msolap::target_table_name))
          {
            TBSYS_LOG(WARN,"unexpect table name [table_name:%.*s]", row_cells[cid - msolap::min_column_id].table_name_.length(), 
              row_cells[cid - msolap::min_column_id].table_name_.ptr());
            err = OB_ERR_UNEXPECTED;
          }
          if((OB_SUCCESS == err) && (row_cells[cid - msolap::min_column_id].row_key_ != row_key_str))
          {
            TBSYS_LOG(WARN,"unexpect rowkey");
            err = OB_ERR_UNEXPECTED;
          }
        }
        if (OB_SUCCESS == err)
        {
          it++;
          hit_row_count ++;
        }
      }
    }
  }
  if (OB_SUCCESS != err)
  {
    res = false;
  }
  else if (expect_hit_row_count != hit_row_count)
  {
    TBSYS_LOG(WARN,"row count not correct [hit_row_count:%ld,expect_hit_row_count:%ld]", hit_row_count, expect_hit_row_count);
    res = false;
  }
  return res;
}

int SelectWhereOrderLimit::form_scan_param(ObScanParam &param, const uint32_t min_key_include, const uint32_t max_key_include,
  void *&arg)
{
  arg = NULL;
  int err = OB_SUCCESS;
  if ((OB_SUCCESS == err) && (OB_SUCCESS != (err =  msolap::init_scan_param(param,min_key_include,max_key_include))))
  {
    TBSYS_LOG(WARN,"fail to init scan param [err:%d]", err);
  }
  ObString column_name;
  ObString expr;
  expr.assign((char*)where_cond, static_cast<int32_t>(strlen(where_cond)));
  if ((OB_SUCCESS == err) && (OB_SUCCESS != (err = param.add_where_cond(expr))))
  {
    TBSYS_LOG(WARN,"fail to add where condition [err:%d]", err);
  }
  column_name.assign((char*)orderby_column, static_cast<int32_t>(strlen(orderby_column)));
  if ((OB_SUCCESS == err) && (OB_SUCCESS != (err = param.add_orderby_column(column_name,(ObScanParam::Order)orderby_desc))))
  {
    TBSYS_LOG(WARN,"fail to add orderby column [err:%d]", err);
  }
  if (OB_SUCCESS == err)
  {
    param.set_limit_info(limit_offset, limit_count);
  }
  return err;
}


