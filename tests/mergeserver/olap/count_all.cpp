#include <arpa/inet.h>
#include "common/ob_define.h"
#include "count_all.h"
#include "olap.h"
using namespace oceanbase;
using namespace oceanbase::common;
CountALL CountALL::static_case_;
bool CountALL::check_result(const uint32_t min_key_include, const uint32_t max_key_include, ObScanner &result,
  void *arg)
{
  UNUSED(arg);
  bool res = true;
  int err = OB_SUCCESS;
  ObCellInfo *cell = NULL;
  int64_t intval = 0;
  if (1 != result.get_cell_num())
  {
    TBSYS_LOG(WARN,"expect there is just one cell in the result, but got [%ld]", result.get_cell_num());
    err = OB_ERR_UNEXPECTED;
  }
  if ((OB_SUCCESS == err) && (OB_SUCCESS != (err = result.next_cell())))
  {
    TBSYS_LOG(WARN,"fail to get first cell [err:%d]", err);
  }
  if ((OB_SUCCESS == err) && (OB_SUCCESS != (err = result.get_cell(&cell))))
  {
    TBSYS_LOG(WARN,"fail to get first cell [err:%d]", err);
  }
  if ((OB_SUCCESS == err) && (OB_SUCCESS != (err = cell->value_.get_int(intval))))
  {
    TBSYS_LOG(WARN,"unexpected value type [err:%d,type:%d]",err, cell->value_.get_type());
  }
  if ((OB_SUCCESS == err) && (intval != max_key_include - min_key_include + 1))
  {
    TBSYS_LOG(WARN,"count result error [expected:%d,got:%ld]", max_key_include - min_key_include + 1, intval);
    err = OB_ERR_UNEXPECTED;
  }
  if (OB_SUCCESS != err)
  {
    res = false;
  }
  return res;
}

int CountALL::form_scan_param(ObScanParam &param, const uint32_t min_key_include, const uint32_t max_key_include,
  void *&arg)
{
  arg = NULL;
  int err = OB_SUCCESS;
  if ((OB_SUCCESS == err) && (OB_SUCCESS != (err =  msolap::init_scan_param(param,min_key_include,max_key_include))))
  {
    TBSYS_LOG(WARN,"fail to init scan param [err:%d]", err);
  }
  if ((OB_SUCCESS == err) && (OB_SUCCESS != (err = param.add_column(ObGroupByParam::COUNT_ROWS_COLUMN_NAME))))
  {
    TBSYS_LOG(WARN,"fail to add * column [err:%d]", err);
  }
  const char *cname_cptr = "count";
  ObString as_name;
  as_name.assign((char*)cname_cptr,static_cast<int32_t>(strlen(cname_cptr)));
  if ((OB_SUCCESS == err) 
    && (OB_SUCCESS != (err = param.get_group_by_param().add_aggregate_column(ObGroupByParam::COUNT_ROWS_COLUMN_NAME, as_name,COUNT, true))))
  {
    TBSYS_LOG(WARN,"fail to add aggregate column [err:%d]", err);
  }
  return err;
}


