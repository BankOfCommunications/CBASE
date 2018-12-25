#include "ob_table_id_name.h"
#include "utility.h"

using namespace oceanbase;
using namespace common;
using namespace nb_accessor;

ObTableIdNameIterator::ObTableIdNameIterator()
   :need_scan_(false), only_core_tables_(true),
     table_idx_(-1), client_proxy_(NULL), res_(NULL)
   ,base_version_(0),end_version_(0),ddl_res_(NULL)//add zhaoqiong [Schema Manager] 20150327
{
}

ObTableIdNameIterator::~ObTableIdNameIterator()
{
  this->destroy();
  need_scan_ = true;
}

bool ObTableIdNameIterator::check_inner_stat()
{
  bool ret = true;
  if (NULL == client_proxy_)
  {
    ret = false;
    TBSYS_LOG(ERROR, "not init");
  }
  else
  {
    if (need_scan_)
    {
      ObNewRange range;
      //mod zhaoqiong [Schema Manager] 20150327:b
//      range.border_flag_.unset_inclusive_start();
//      range.border_flag_.set_inclusive_end();
//      range.set_whole_range();
//      int err = OB_SUCCESS;
//      if(OB_SUCCESS != (err = nb_accessor_.scan(res_, FIRST_TABLET_TABLE_NAME, range, SC("table_name")("table_id"))))
//      {
//        TBSYS_LOG(WARN, "nb accessor scan fail:ret[%d]", err);
//        ret = false;
//      }
//      else
//      {
//        TBSYS_LOG(DEBUG, "scan first_tablet_table success. scanner row count =%ld", res_->get_scanner()->get_row_num());
//        need_scan_ = false;
//      }
      int err = OB_SUCCESS;

      if (end_version_ > 0)
      {
        range.border_flag_.unset_inclusive_start();
        range.border_flag_.set_inclusive_end();

        ObObj start_version;
        start_version.set_int(base_version_);
        range.start_key_.assign(&start_version,1);

        ObObj end_version;
        end_version.set_int(end_version_);
        range.end_key_.assign(&end_version,1);
        if(OB_SUCCESS != (err = nb_accessor_.scan(ddl_res_, OB_ALL_DDL_OPERATION_NAME, range, SC("table_id")("operation_type"))))
        {
          TBSYS_LOG(WARN, "nb accessor scan fail:ret[%d]", err);
          ret = false;
        }
      }

      if (err != OB_SUCCESS)
      {
        TBSYS_LOG(WARN, "nb accessor scan fail:ret[%d]", err);
        ret = false;
      }
      else
      {
        range.set_whole_range();
        if(OB_SUCCESS != (err = nb_accessor_.scan(res_, FIRST_TABLET_TABLE_NAME, range, SC("table_name")("db_name")("table_id"))))
        {
          TBSYS_LOG(WARN, "nb accessor scan fail:ret[%d]", err);
          ret = false;
        }
        else
        {
          TBSYS_LOG(DEBUG, "scan first_tablet_table success. scanner row count =%ld", res_->get_scanner()->get_row_num());
          need_scan_ = false;
        }
      }
    }
    //need add core_schema to res_
    //mod:e
  }
  return ret;
}
//mod zhaoqiong [Schema Manager] 20150327:b
//int ObTableIdNameIterator::init(ObScanHelper* client_proxy, bool only_core_tables)
int ObTableIdNameIterator::init(ObScanHelper* client_proxy, bool only_core_tables, int64_t base_version , int64_t end_version)
//mod:e
{
  int ret = OB_SUCCESS;
  if(NULL == client_proxy)
  {
    ret = OB_INVALID_ARGUMENT;
    TBSYS_LOG(WARN, "client_proxy is null");
  }

  if(OB_SUCCESS == ret)
  {
    this->only_core_tables_ = only_core_tables;
    this->client_proxy_ = client_proxy;
    //add zhaoqiong [Schema Manager] 20150327:b
    base_version_ = base_version;
    end_version_ = end_version;
    //add:e
    table_idx_ = -1;
    if (only_core_tables == false)
    {
      need_scan_ = true;
      ret = nb_accessor_.init(client_proxy);
      if(OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "init nb_accessor fail:ret[%d]", ret);
      }
      else
      {
        nb_accessor_.set_is_read_consistency(true);
      }
    }
  }
  return ret;
}

//add zhaoqiong [Schema Manager] 20150327:b
int ObTableIdNameIterator::get_table_list(ObArray<int64_t> & create_table_id, ObArray<int64_t> & drop_table_id, bool &refresh_schema)
{
  int ret = OB_SUCCESS;
  if(!check_inner_stat())
  {
    ret = OB_ERROR;
    TBSYS_LOG(WARN, "check inner stat fail");
  }
  refresh_schema = false;
  while(OB_SUCCESS == ret)
  {
    TableRow* table_row = NULL;
    int64_t type = 0;
    if (NULL == ddl_res_)
    {
      ret = OB_ERR_UNEXPECTED;
      TBSYS_LOG(ERROR, "results is NULL");
    }
    else
    {
      ret = ddl_res_->next_row();
    }

    if(OB_SUCCESS != ret && OB_ITER_END != ret)
    {
      TBSYS_LOG(WARN, "next row fail:ret[%d]", ret);
    }

    if(OB_SUCCESS == ret)
    {
      if(OB_SUCCESS != (ret = ddl_res_->get_row(&table_row)))
      {
        TBSYS_LOG(WARN, "get row fail:ret[%d]", ret);
      }
    }

    ObCellInfo* cell_info = NULL;
    if(OB_SUCCESS == ret)
    {
      cell_info = table_row->get_cell_info("operation_type");
      if(NULL != cell_info)
      {
        if (OB_SUCCESS != (ret = cell_info->value_.get_int(type)))
        {
          TBSYS_LOG(ERROR, "get operation_type fail, ret = %d",ret);
        }
        else if (REFRESH_SCHEMA == type)
        {
          refresh_schema = true;
          TBSYS_LOG(INFO, "refresh schema");
          break;
        }
      }
      else
      {
        ret = OB_ERROR;
        TBSYS_LOG(WARN, "get operation_type cell info fail");
      }
    }

    if(OB_SUCCESS == ret && !refresh_schema)
    {
      cell_info = table_row->get_cell_info("table_id");
      if(NULL != cell_info)
      {
        int64_t table_id = 0;
        ret = cell_info->value_.get_int(table_id);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR, "get table_id fail, ret = %d",ret);
        }
        else
        {
          switch(type)
          {
          case CREATE_TABLE:
            if (OB_SUCCESS != (ret = create_table_id.push_back(table_id)))
            {
              TBSYS_LOG(ERROR, "memory overflow");
            }
            break;
          case DROP_TABLE:
            if (OB_SUCCESS != (ret = drop_table_id.push_back(table_id)))
            {
              TBSYS_LOG(ERROR, "memory overflow");
            }
            break;
          case ALTER_TABLE:
            if (OB_SUCCESS != (ret = drop_table_id.push_back(table_id)))
            {
              TBSYS_LOG(ERROR, "memory overflow");
            }
            else if (OB_SUCCESS != (ret = create_table_id.push_back(table_id)))
            {
              TBSYS_LOG(ERROR, "memory overflow");
            }
            break;
          default:
            ret = OB_ERROR;
            TBSYS_LOG(ERROR, "unknow type:%ld",type);
            break;
          }
        }
      }
      else
      {
        ret = OB_ERROR;
        TBSYS_LOG(WARN, "get table_id cell info fail");
      }
    }
  }

  if (OB_ITER_END == ret)
  {
    ret = OB_SUCCESS;
  }

  return ret;
}

int ObTableIdNameIterator::get_latest_schema_version(int64_t& latest_schema_version)
{
  int ret = OB_SUCCESS;

  ObNewRange range;
  range.set_whole_range();
  if(OB_SUCCESS != (ret = nb_accessor_.scan(ddl_res_, OB_ALL_DDL_OPERATION_NAME, range, SC("schema_version"))))
  {
    TBSYS_LOG(WARN, "nb accessor scan fail:ret[%d]", ret);
  }

  while(OB_SUCCESS == ret)
  {
    TableRow* table_row = NULL;
    int64_t version = 0;
    if (NULL == ddl_res_)
    {
      ret = OB_ERR_UNEXPECTED;
      TBSYS_LOG(ERROR, "results is NULL");
    }
    else
    {
      ret = ddl_res_->next_row();
    }

    if(OB_SUCCESS != ret && OB_ITER_END != ret)
    {
      TBSYS_LOG(WARN, "next row fail:ret[%d]", ret);
    }

    if(OB_SUCCESS == ret)
    {
      if(OB_SUCCESS != (ret = ddl_res_->get_row(&table_row)))
      {
        TBSYS_LOG(WARN, "get row fail:ret[%d]", ret);
      }
    }

    ObCellInfo* cell_info = NULL;
    if(OB_SUCCESS == ret)
    {
      cell_info = table_row->get_cell_info("schema_version");
      if(NULL != cell_info)
      {
        if (OB_SUCCESS != (ret = cell_info->value_.get_int(version)))
        {
          TBSYS_LOG(ERROR, "get schema_version fail, ret = %d",ret);
        }
        else if (version > latest_schema_version)
        {
          latest_schema_version = version;
        }
      }
      else
      {
        ret = OB_ERROR;
        TBSYS_LOG(WARN, "get table_id cell info fail");
      }
    }
  }

  if (OB_ITER_END == ret && latest_schema_version > 0)
  {
    ret = OB_SUCCESS;
  }
  else if (OB_ITER_END == ret && 0 == latest_schema_version)
  {
    //for upgrade
    latest_schema_version = tbsys::CTimeUtil::getTime();
    ret = OB_SUCCESS;
  }

  return ret;
}
//add:e
int ObTableIdNameIterator::next()
{
  int ret = OB_SUCCESS;
  if(!check_inner_stat())
  {
    ret = OB_ERROR;
    TBSYS_LOG(WARN, "check inner stat fail");
  }
  if(OB_SUCCESS == ret)
  {
    if (only_core_tables_ == true)
    {
      ++table_idx_;
      TBSYS_LOG(DEBUG, "table_idx=%d", table_idx_);
      //mod zhaoqiong [Schema Manager] 20150327:b
      //if (table_idx_ < 3)
      if (table_idx_ < CORE_TABLE_COUNT)
      //mod:e
      {
        // we have three basic tables: __first_tablet_entry, __all_all_column, __all_all_join
      }
      else
      {
        ret = OB_ITER_END;
      }
    }
    else
    {
      ++table_idx_;
      TBSYS_LOG(DEBUG, "table_idx=%d", table_idx_);
      //mod zhaoqiong [Schema Manager] 20150327:b
      //if (table_idx_ < 3)
      if (table_idx_ < CORE_TABLE_COUNT)
      //mod:e
      {
        // we have three basic tables: __first_tablet_entry, __all_all_column, __all_all_join
      }
      else if (NULL == res_)
      {
        TBSYS_LOG(ERROR, "results is NULL");
        ret = OB_ERR_UNEXPECTED;
      }
      else
      {
        ret = res_->next_row();
        if(OB_SUCCESS != ret && OB_ITER_END != ret)
        {
          TBSYS_LOG(WARN, "next row fail:ret[%d]", ret);
        }
      }
    }
  }
  return ret;
}

int ObTableIdNameIterator::get(ObTableIdName** table_info)
{
  int ret = OB_SUCCESS;
  if (0 > table_idx_)
  {
    ret = OB_ERR_UNEXPECTED;
    TBSYS_LOG(ERROR, "get failed");
  }
  //mod zhaoqiong [Schema Manager] 20150327:b
  //else if (table_idx_ < 3)
  else if (table_idx_ < CORE_TABLE_COUNT)
  //mod:e
  {
    ret = internal_get(table_info);
  }
  else
  {
    if (only_core_tables_ == true)
    {
      ret = OB_INNER_STAT_ERROR;
      TBSYS_LOG(WARN, "get core tables but table_idx[%d]_ >= 3", table_idx_);
    }
    else
    {
      ret = normal_get(table_info);
    }
  }
  if (OB_SUCCESS == ret)
  {
    TBSYS_LOG(DEBUG, "table_name: [%.*s], table_id: [%ld]",
        (*table_info)->table_name_.length(),
        (*table_info)->table_name_.ptr(), (*table_info)->table_id_);
  }
  return ret;
}

int ObTableIdNameIterator::internal_get(ObTableIdName** table_info)
{
  int ret = OB_SUCCESS;
  switch(table_idx_)
  {
    case 0:
      table_id_name_.table_name_.assign_ptr(const_cast<char*>(FIRST_TABLET_TABLE_NAME),
          static_cast<int32_t>(strlen(FIRST_TABLET_TABLE_NAME)));
      table_id_name_.table_id_ = OB_FIRST_TABLET_ENTRY_TID;
      //add dolphin [database manager]@20150613:b
      table_id_name_.dbname_.assign(const_cast<char*>(OB_DEFAULT_DATABASE_NAME), static_cast<int32_t>(strlen(OB_DEFAULT_DATABASE_NAME)));
      //add:e
      *table_info = &table_id_name_;
      break;
    case 1:
      table_id_name_.table_name_.assign_ptr(const_cast<char*>(OB_ALL_COLUMN_TABLE_NAME),
          static_cast<int32_t>(strlen(OB_ALL_COLUMN_TABLE_NAME)));
      table_id_name_.table_id_ = OB_ALL_ALL_COLUMN_TID;
      //add dolphin [database manager]@20150613:b
      table_id_name_.dbname_.assign(const_cast<char*>(OB_DEFAULT_DATABASE_NAME), static_cast<int32_t>(strlen(OB_DEFAULT_DATABASE_NAME)));
      //add:e
      *table_info = &table_id_name_;
      break;
    case 2:
      table_id_name_.table_name_.assign_ptr(const_cast<char*>(OB_ALL_JOININFO_TABLE_NAME),
          static_cast<int32_t>(strlen(OB_ALL_JOININFO_TABLE_NAME)));
      table_id_name_.table_id_ = OB_ALL_JOIN_INFO_TID;
      //add dolphin [database manager]@20150613:b
      table_id_name_.dbname_.assign(const_cast<char*>(OB_DEFAULT_DATABASE_NAME), static_cast<int32_t>(strlen(OB_DEFAULT_DATABASE_NAME)));
      //add:e
      *table_info = &table_id_name_;
      break;
    //add zhaoqiong [Schema Manager] 20150327:b
    case 3:
      table_id_name_.table_name_.assign_ptr(const_cast<char*>(OB_ALL_DDL_OPERATION_NAME),
          static_cast<int32_t>(strlen(OB_ALL_DDL_OPERATION_NAME)));
      table_id_name_.table_id_ = OB_DDL_OPERATION_TID;
      //add dolphin [database manager]@20150613:b
      table_id_name_.dbname_.assign(const_cast<char*>(OB_DEFAULT_DATABASE_NAME), static_cast<int32_t>(strlen(OB_DEFAULT_DATABASE_NAME)));
      //add:e
      *table_info = &table_id_name_;
      break;
    //add:e
    default:
      ret = OB_ERR_UNEXPECTED;
      TBSYS_LOG(ERROR, "unexpected branch");
      break;
  }
  return ret;
}

int ObTableIdNameIterator::normal_get(ObTableIdName** table_id_name)
{
  int ret = OB_SUCCESS;

  TableRow* table_row = NULL;
  if (NULL == res_)
  {
    ret = OB_ERR_UNEXPECTED;
    TBSYS_LOG(ERROR, "results is NULL");
  }

  if(OB_SUCCESS == ret)
  {
    ret = res_->get_row(&table_row);
    if(OB_SUCCESS != ret && OB_ITER_END != ret)
    {
      TBSYS_LOG(WARN, "get row fail:ret[%d]", ret);
    }
  }

  ObCellInfo* cell_info = NULL;

  if(OB_SUCCESS == ret)
  {
    cell_info = table_row->get_cell_info("table_name");

    if(NULL != cell_info)
    {
      if (cell_info->value_.get_type() == ObNullType
          && cell_info->row_key_.get_obj_cnt() > 0
          && cell_info->row_key_.get_obj_ptr()[0].get_type() == ObVarcharType)
      {
        //add dolphin [database manager]@20150611
        ret = OB_INVALID_ARGUMENT;
        TBSYS_LOG(WARN, "value is null,  get table name from cell rowkey:%s", print_cellinfo(cell_info));
        //modify dolphin [database manager]@20150611
        //cell_info->row_key_.get_obj_ptr()[0].get_varchar(table_id_name_.table_name_);
      }
      else
      {
        cell_info->value_.get_varchar(table_id_name_.table_name_);
      }
    }
    else
    {
      ret = OB_ERROR;
      TBSYS_LOG(WARN, "get table_name cell info fail");
    }
  }
  //add dolphin [database manager]@20150611
  if(OB_SUCCESS == ret)
  {
    cell_info = table_row->get_cell_info("db_name");
    if(NULL != cell_info)
    {
      if (cell_info->value_.get_type() == ObNullType
          && cell_info->row_key_.get_obj_cnt() > 1
          && cell_info->row_key_.get_obj_ptr()[1].get_type() == ObVarcharType)
      {
        ret = OB_INVALID_ARGUMENT;
        TBSYS_LOG(WARN, "db_name value is null,  get database name from cell rowkey:%s", print_cellinfo(cell_info));
      }
      else
      {
        cell_info->value_.get_varchar(table_id_name_.dbname_);
      }
    }
    else
    {
      ret = OB_ERROR;
      TBSYS_LOG(WARN, "get db_name cell info fail");
    }
  }
  //add:e

  if(OB_SUCCESS == ret)
  {
    cell_info = table_row->get_cell_info("table_id");
    if(NULL != cell_info)
    {
      int64_t tmp = 0;
      cell_info->value_.get_int(tmp);
      table_id_name_.table_id_ = static_cast<uint64_t>(tmp);
      if (tmp == 0)
      {
        TBSYS_LOG(INFO, "get table_id = 0");
      }
    }
    else
    {
      ret = OB_ERROR;
      TBSYS_LOG(WARN, "get table_id cell info fail");
    }
  }

  if(OB_SUCCESS == ret)
  {
    *table_id_name = &table_id_name_;
  }
  return ret;
}

void ObTableIdNameIterator::destroy()
{
  if(NULL != res_)
  {
    nb_accessor_.release_query_res(res_);
    res_ = NULL;
  }
  //add zhaoqiong [Schema Manager] 20150327:b
  if(NULL != ddl_res_)
  {
    nb_accessor_.release_query_res(ddl_res_);
    ddl_res_ = NULL;
  }
  //add:e
}

