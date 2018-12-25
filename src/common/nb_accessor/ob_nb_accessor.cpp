
#include "ob_nb_accessor.h"
#include "utility.h"

using namespace oceanbase;
using namespace common;
using namespace nb_accessor;

ObNbAccessor::ObNbAccessor()
  :client_proxy_(NULL),
  is_read_consistency_(true)
{
}

ObNbAccessor::~ObNbAccessor()
{
}

int ObNbAccessor::get(QueryRes*& res, const char* table_name, const KV& rowkey_kv, const SC& select_columns)
{
  int ret = OB_SUCCESS;

  ObRowkey rowkey;
  ObObj rowkey_obj[rowkey_kv.count()];

  ret = common::kv_to_rowkey(rowkey_kv, rowkey, rowkey_obj, static_cast<int32_t>(rowkey_kv.count()));

  if(OB_SUCCESS == ret)
  {
    ret = get(res, table_name, rowkey, select_columns);
  }

  return ret;
}

int ObNbAccessor::get(QueryRes*& res, const char* table_name, const ObRowkey& rowkey, const SC& select_columns)
{
  int ret = OB_SUCCESS;
  ObScanParam* param = NULL;
  ObVersionRange version_range;
  ObCellInfo cell_info;
  if(NULL == table_name && NULL != res)
  {
    ret = OB_INVALID_ARGUMENT;
    TBSYS_LOG(WARN, "table_name[%p], res[%p]", table_name, res);
  }

  if(OB_SUCCESS == ret && OB_SUCCESS != select_columns.get_exec_status())
  {
    ret = select_columns.get_exec_status();
    TBSYS_LOG(WARN, "select columns error:ret[%d]", ret);
  }

  if(OB_SUCCESS == ret && !check_inner_stat())
  {
    ret = OB_ERROR;
    TBSYS_LOG(WARN, "check inner status fail");
  }

  if(OB_SUCCESS == ret)
  {
    param = GET_TSI_MULT(ObScanParam, TSI_COMMON_SCAN_PARAM_1);
    if(NULL == param)
    {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      TBSYS_LOG(WARN, "get thread specific get param fail");
    }
    else
    {
      param->reset();
      param->set_is_read_consistency(is_read_consistency_);
    }
  }

  if (OB_SUCCESS == ret)
  {
    version_range.border_flag_.set_min_value();
    version_range.border_flag_.set_max_value();
    param->set_version_range(version_range);
  }
  if(OB_SUCCESS == ret)
  {
    uint64_t tid = OB_INVALID_ID;
    ObNewRange single_row;
    single_row.border_flag_.set_inclusive_start();
    single_row.border_flag_.set_inclusive_end();
    single_row.start_key_ = rowkey;
    single_row.end_key_ = rowkey;
    param->set(tid, ObString::make_string(table_name), single_row, true);

    const char* column = NULL;
    for(int32_t i=0;i<select_columns.count() && OB_SUCCESS == ret;i++)
    {
      ret = select_columns.at(i, column);
      if(OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "get select column element fail:ret[%d]", ret);
      }

      if(OB_SUCCESS == ret)
      {
        ret = param->add_column(ObString::make_string(column));
        if(OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "get param add cell fail:ret[%d]", ret);
        }
      }
    }
  }

  if(OB_SUCCESS == ret)
  {
    void* tmp = ob_malloc(sizeof(QueryRes), ObModIds::OB_NB_ACCESSOR);
    if(NULL == tmp)
    {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      TBSYS_LOG(WARN, "allocate QueryRes fail");
    }
    else
    {
      res = new(tmp)QueryRes;
    }
  }

  if(OB_SUCCESS == ret)
  {
    ret = client_proxy_->scan(*param, *(res->get_scanner()));
    if(OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN,"get from ms failed,ret = %d",ret);
    }
    else
    {
      TBSYS_LOG(DEBUG, "got %ld cells", res->get_scanner()->get_size());
    }
  }

  bool is_fullfilled = false;
  int64_t fullfilled_item_num = 0;

  if(OB_SUCCESS == ret)
  {
    ret = res->get_scanner()->get_is_req_fullfilled(is_fullfilled, fullfilled_item_num);
    if(OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "get is req fullfilled fail:ret[%d]", ret);
    }
  }

  if(OB_SUCCESS == ret)
  {
    if(!is_fullfilled)
    {
      ret = OB_ERROR;
      TBSYS_LOG(WARN, "scanner is not fullfilled, the result is too large, which is unsupported now");
    }
  }

  if(OB_SUCCESS == ret)
  {
    ret = res->init(select_columns);
    if(OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "create QueryRes fail:ret[%d]", ret);
    }
  }

  if(OB_SUCCESS != ret)
  {
    if(NULL != res)
    {
      res->~QueryRes();
      ob_free(res, ObModIds::OB_NB_ACCESSOR);
      res = NULL;
    }
  }

  return ret;
}

bool ObNbAccessor::check_inner_stat()
{
  return client_proxy_ != NULL;
}

int ObNbAccessor::scan(QueryRes*& res, const char* table_name, const ObNewRange& range, const SC& select_columns)
{
  ScanConds scan_conds;
  return scan(res, table_name, range, select_columns, scan_conds);
}

//add hongchen [UNLIMIT_TABLE] 20161031:b
int ObNbAccessor::get_next_scanner(QueryRes*& res)
{
  int ret = OB_SUCCESS;

  res->get_scanner()->reset();
  if (NULL == res)
  {
    ret = OB_INVALID_ARGUMENT;
  }
  else if (OB_SUCCESS != (ret = client_proxy_->scan_for_next_packet(res->get_serversession(), *(res->get_scanner()))))
  {
    TBSYS_LOG(WARN, "ObNbAccessor failed to receive next packet! ret = %d", ret);
  }
  else
  {
    res->reinit();
  }

    return ret;
}
//add hongchen [UNLIMIT_TABLE] 20161031:e

int ObNbAccessor::scan(QueryRes*& res, const char* table_name, const ObNewRange& range, const SC& select_columns, const ScanConds& scan_conds)
{
  int ret = OB_SUCCESS;
  ObScanParam* param = NULL;
  ObVersionRange version_range;
  ObString ob_table_name;

  if(NULL == table_name || NULL != res)
  {
    ret = OB_INVALID_ARGUMENT;
    TBSYS_LOG(WARN, "table_name[%p], res[%p]", table_name, res);
  }

  if(OB_SUCCESS == ret && OB_SUCCESS != select_columns.get_exec_status())
  {
    ret = select_columns.get_exec_status();
    TBSYS_LOG(WARN, "select columns error:ret[%d]", ret);
  }

  if(OB_SUCCESS == ret && OB_SUCCESS != scan_conds.get_exec_status())
  {
    ret = scan_conds.get_exec_status();
    TBSYS_LOG(WARN, "scan conds error:ret[%d]", ret);
  }

  if(OB_SUCCESS == ret && !check_inner_stat())
  {
    ret = OB_ERROR;
    TBSYS_LOG(WARN, "check inner status fail");
  }

  if(OB_SUCCESS == ret)
  {
    ob_table_name.assign_ptr(const_cast<char*>(table_name), static_cast<int32_t>(strlen(table_name)));
  }

  if(OB_SUCCESS == ret)
  {
    param = GET_TSI_MULT(ObScanParam, TSI_COMMON_SCAN_PARAM_1);
    if(NULL == param)
    {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      TBSYS_LOG(WARN, "get thread specific scan param fail");
    }
    else
    {
      param->reset();
      param->set_is_read_consistency(is_read_consistency_);
    }
  }

  if(OB_SUCCESS == ret)
  {
    version_range.border_flag_.set_min_value();
    version_range.border_flag_.set_max_value();
    param->set_version_range(version_range);

    param->set(OB_INVALID_ID, ob_table_name, range, true);
  }

  const char* select_column = NULL;
  ObString ob_select_column;
  for(int32_t i=0;i<select_columns.count() && OB_SUCCESS == ret;i++)
  {
    ret = select_columns.at(i, select_column);
    if(OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "get sc array element fail:i[%d], ret[%d]", i, ret);
    }

    if(OB_SUCCESS == ret)
    {
      ob_select_column.assign_ptr(const_cast<char*>(select_column), static_cast<int32_t>(strlen(select_column)));
      ret = param->add_column(ob_select_column);
      if(OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "add column to scan param fail:ret[%d]", ret);
      }
    }
  }

  ObSimpleCond scan_cond;
  for(int32_t i=0;i<scan_conds.count() && OB_SUCCESS == ret;i++)
  {
    ret = scan_conds.at(i, scan_cond);
    if(OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "get scan cond array element fail:i[%d], ret[%d]", i, ret);
    }

    if(OB_SUCCESS == ret)
    {
      ret = param->add_where_cond(scan_cond.get_column_name(), scan_cond.get_logic_operator(), scan_cond.get_right_operand());
      if(OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "add cond to scan param fail:ret[%d]", ret);
      }
    }
  }

  if(OB_SUCCESS == ret)
  {
    void* tmp = ob_malloc(sizeof(QueryRes), ObModIds::OB_NB_ACCESSOR);
    if(NULL == tmp)
    {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      TBSYS_LOG(WARN, "allocate QueryRes fail");
    }
    else
    {
      res = new(tmp)QueryRes;
    }
  }

  //add hongchen [UNLIMIT_TABLE] 20161031:b
  //save the session for future communicating
  common::ServerSession ssession;
  //add hongchen [UNLIMIT_TABLE] 20161031:e
  if(OB_SUCCESS == ret)
  {
    //mod hongchen [UNLIMIT_TABLE] 20161031:b
    //ret = client_proxy_->scan(*param, *(res->get_scanner()));
    ret = client_proxy_->scan(*param, *(res->get_scanner()), &ssession);
    //mod hongchen [UNLIMIT_TABLE] 20161031:e
    if(OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN,"scan from ms failed,ret = %d",ret);
    }
    else
    {
      TBSYS_LOG(DEBUG, "got %ld size, cell num:%ld, row num:%ld",
          res->get_scanner()->get_size(), res->get_scanner()->get_cell_num(),
          res->get_scanner()->get_row_num());
      dump_scanner(*(res->get_scanner()));
    }
  }

  bool is_fullfilled = false;
  int64_t fullfilled_item_num = 0;
  if(OB_SUCCESS == ret)
  {
    ret = res->get_scanner()->get_is_req_fullfilled(is_fullfilled, fullfilled_item_num);
    if(OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "get is req fullfilled fail:ret[%d]", ret);
    }
  }

  if(OB_SUCCESS == ret)
  {
    if(!is_fullfilled)
    {
      //mod hongchen [UNLIMIT_TABLE] 20161031:b
      res->set_serversession(ssession);
      //ret = OB_ERROR;
      //TBSYS_LOG(WARN, "scanner is not fullfilled, the result is too large, which is unsupported now");
      //mod hongchen [UNLIMIT_TABLE] 20161031:b
    }
  }

  if(OB_SUCCESS == ret)
  {
    ret = res->init(select_columns);
    if(OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "create QueryRes fail:ret[%d]", ret);
    }
  }

  if(OB_SUCCESS != ret)
  {
    if(NULL != res)
    {
      res->~QueryRes();
      ob_free(res, ObModIds::OB_NB_ACCESSOR);
      res = NULL;
    }
  }

  return ret;
}

int ObNbAccessor::delete_row(const char* table_name, const KV& rowkey_kv)
{
  int ret = OB_SUCCESS;

  ObRowkey rowkey;
  ObObj rowkey_obj[rowkey_kv.count()];

  ret = common::kv_to_rowkey(rowkey_kv, rowkey, rowkey_obj, static_cast<int32_t>(rowkey_kv.count()));

  if(OB_SUCCESS == ret)
  {
    ret = delete_row(table_name, rowkey);
  }

  return ret;
}

int ObNbAccessor::delete_row(const char* table_name, const ObRowkey& rowkey)
{
  int ret = OB_SUCCESS;

  ObString ob_table_name;
  ob_table_name.assign_ptr(const_cast<char*>(table_name), static_cast<int32_t>(strlen(table_name)));

  ObMutator* mutator = NULL;
  if(OB_SUCCESS == ret)
  {
    mutator = GET_TSI_MULT(ObMutator, TSI_COMMON_MUTATOR_1);
    if(NULL == mutator)
    {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      TBSYS_LOG(WARN, "get thread specific ObMutator fail");
    }
  }

  if(OB_SUCCESS == ret)
  {
    ret = mutator->reset();
    if(OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "reset ob mutator fail:ret[%d]", ret);
    }
  }

  if(OB_SUCCESS == ret)
  {
    ret = mutator->del_row(ob_table_name, rowkey);
    if(OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "add del row info to mutator fail:ret[%d]", ret);
    }
  }

  if(OB_SUCCESS == ret)
  {
    ret = client_proxy_->mutate(*mutator);
    if(OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "apply mutator to update server fail:ret[%d]", ret);
    }
  }

  return ret;
}

int ObNbAccessor::delete_row(const char* table_name, const SC& rowkey_columns, const ScanConds& scan_conds)
{
  int ret = OB_SUCCESS;

  if(NULL == table_name)
  {
    ret = OB_INVALID_ARGUMENT;
    TBSYS_LOG(WARN, "table_name is null");
  }

  ObString ob_table_name;
  ob_table_name.assign_ptr(const_cast<char*>(table_name), static_cast<int32_t>(strlen(table_name)));

  ObNewRange range;
  range.set_whole_range();

  ObMutator* mutator = NULL;
  if(OB_SUCCESS == ret)
  {
    mutator = GET_TSI_MULT(ObMutator, TSI_COMMON_MUTATOR_1);
    if(NULL == mutator)
    {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      TBSYS_LOG(WARN, "get thread specific ObMutator fail");
    }
  }

  if(OB_SUCCESS == ret)
  {
    ret = mutator->reset();
    if(OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "reset ob mutator fail:ret[%d]", ret);
    }
  }

  QueryRes* res = NULL;
  if(OB_SUCCESS == ret)
  {
    ret = this->scan(res, table_name, range, rowkey_columns, scan_conds);
    if(OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "scan fail:ret[%d]", ret);
    }
  }

  ObObj* rowkey_obj = NULL;
  if(OB_SUCCESS == ret)
  {
    rowkey_obj = (ObObj*)ob_malloc(rowkey_columns.count() * sizeof(ObObj), ObModIds::OB_NB_ACCESSOR);
    if(NULL == rowkey_obj)
    {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      TBSYS_LOG(WARN, "allocate memory for rowkey fail:ret[%d]", ret);
    }
  }

  ObRowkey rowkey;
  if(OB_SUCCESS == ret)
  {
    rowkey.assign(rowkey_obj, rowkey_columns.count());
  }

  TableRow* row = NULL;
  ObCellInfo* cell_info = NULL;
  while (OB_SUCCESS == ret)
  {
    ret = res->next_row();
    if (OB_ITER_END == ret)
    {
      ret = OB_SUCCESS;
      break;
    }
    else if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "fail to get next row[%d]", ret);
    }

    if(OB_SUCCESS == ret)
    {
      ret = res->get_row(&row);
      if(OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "get row fail:ret[%d]", ret);
      }
      else if(row->count() != rowkey_columns.count())
      {
        ret = OB_ERROR;
        TBSYS_LOG(WARN, "unexpected error:row->count[%ld], rowkey_columns.count[%ld]", row->count(), rowkey_columns.count());
      }
    }

    for(int32_t i=0;(OB_SUCCESS == ret) && (i<row->count());i++)
    {
      cell_info = row->get_cell_info(i);
      if(NULL != cell_info)
      {
        rowkey_obj[i] = cell_info->value_;
      }
      else
      {
        ret = OB_ERROR;
        TBSYS_LOG(WARN, "get cell info fail");
      }
    }

    if(OB_SUCCESS == ret)
    {
      ret = mutator->del_row(ob_table_name, rowkey);
      if(OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "add del row info to mutator fail:ret[%d]", ret);
      }
    }
  } /* end while */

  if(NULL != rowkey_obj)
  {
    ob_free(rowkey_obj, ObModIds::OB_NB_ACCESSOR);
    rowkey_obj = NULL;
  }

  if(OB_SUCCESS == ret)
  {
    ret = client_proxy_->mutate(*mutator);
    if(OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "apply mutator to update server fail:ret[%d]", ret);
    }
  }

  //需要最后释放，mutator里面的字符串都存储在res结构中
  release_query_res(res);

  return ret;
}

int ObNbAccessor::update(const char* table_name, const ObRowkey& rowkey, const KV& kv)
{
  return insert(table_name, rowkey, kv);
}

int ObNbAccessor::update(const char* table_name, const KV& rowkey_kv, const KV& kv)
{
  return insert(table_name, rowkey_kv, kv);
}

int ObNbAccessor::insert(const char* table_name, const KV& rowkey_kv, const KV& kv)
{
  int ret = OB_SUCCESS;

  ObRowkey rowkey;
  ObObj rowkey_obj[rowkey_kv.count()];

  ret = common::kv_to_rowkey(rowkey_kv, rowkey, rowkey_obj, static_cast<int32_t>(rowkey_kv.count()));

  if(OB_SUCCESS == ret)
  {
    ret = insert(table_name, rowkey, kv);
  }

  return ret;
}

int ObNbAccessor::insert(const char* table_name, const ObRowkey& rowkey, const KV& kv)
{
  int ret = OB_SUCCESS;

  ObMutator* mutator = NULL;

  if(OB_SUCCESS == ret)
  {
    mutator = GET_TSI_MULT(ObMutator, TSI_COMMON_MUTATOR_1);
    if(NULL == mutator)
    {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      TBSYS_LOG(WARN, "get thread specific ObMutator fail");
    }
  }

  if(OB_SUCCESS == ret)
  {
    ret = mutator->reset();
    if(OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "reset ob mutator fail:ret[%d]", ret);
    }
  }

  ObMutatorHelper mutator_helper;

  if(OB_SUCCESS == ret)
  {
    ret = mutator_helper.init(mutator);
    if(OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "init mutator helper fail:ret[%d]", ret);
    }
  }

  if(OB_SUCCESS == ret)
  {
    ret = mutator_helper.insert(table_name, rowkey, kv);
    if(OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "insert fail:ret[%d]", ret);
    }
  }

  if(OB_SUCCESS == ret)
  {
    ret = client_proxy_->mutate(*mutator);
    if(OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "apply mutator to update server fail:ret[%d]", ret);
    }
  }

  return ret;
}

int ObNbAccessor::init(ObScanHelper* client_proxy)
{
  int ret = OB_SUCCESS;
  if(NULL == client_proxy)
  {
    ret = OB_INVALID_ARGUMENT;
    TBSYS_LOG(WARN, "client_proxy is null");
  }
  else
  {
    this->client_proxy_ = client_proxy;
  }
  return ret;
}

void ObNbAccessor::release_query_res(QueryRes* res)
{
  if(NULL != res)
  {
    res->~QueryRes();
    ob_free(res, ObModIds::OB_NB_ACCESSOR);
  }
}

int ObNbAccessor::batch_begin(BatchHandle& handle)
{
  int ret = OB_SUCCESS;

  if(OB_SUCCESS == ret)
  {
    handle.mutator_ = GET_TSI_MULT(ObMutator, TSI_COMMON_MUTATOR_1);
    if(NULL == handle.mutator_)
    {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      TBSYS_LOG(WARN, "get thread specific ObMutator fail");
    }
  }

  if(OB_SUCCESS == ret)
  {
    ret = handle.mutator_->reset();
    if(OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "reset ob mutator fail:ret[%d]", ret);
    }
  }

  if(OB_SUCCESS == ret)
  {
    ret = handle.mutator_helper_.init(handle.mutator_);
    if(OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "init mutator helper fail:ret[%d]", ret);
    }
  }

  if(OB_SUCCESS == ret)
  {
    handle.modified = false;
  }
  return ret;
}

int ObNbAccessor::batch_update(BatchHandle& handle, const char* table_name, const ObRowkey& rowkey, const KV& kv)
{
  return batch_insert(handle, table_name, rowkey, kv);
}

int ObNbAccessor::batch_insert(BatchHandle& handle, const char* table_name, const ObRowkey& rowkey, const KV& kv)
{
  int ret = OB_SUCCESS;
  if(OB_SUCCESS == ret)
  {
    ret = handle.mutator_helper_.insert(table_name, rowkey, kv);
    if(OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "insert fail:ret[%d]", ret);
    }
    else
    {
      handle.modified = true;
    }
  }
  return ret;
}

int ObNbAccessor::batch_end(BatchHandle& handle)
{
  int ret = OB_SUCCESS;
  if(OB_SUCCESS == ret && handle.modified)
  {
    ret = client_proxy_->mutate(*(handle.mutator_));
    if(OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "apply mutator to update server fail:ret[%d]", ret);
    }
  }
  return ret;
}


