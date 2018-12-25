#include "ob_mutator_helper.h"

using namespace oceanbase;
using namespace common;

kv_pair::kv_pair()
  :key_(NULL)
{
}

kv_pair::kv_pair(const char* key, ObString val)
{
  this->key_ = key;
  this->obj_.set_varchar(val);
}

kv_pair::kv_pair(const char* key, int64_t val)
{
  this->key_ = key;
  this->obj_.set_int(val);
}

KV::KV(const char* key, ObString val)
  :EasyArray<kv_pair>::EasyArray(kv_pair(key, val))
{
}

KV::KV(const char* key, int64_t val)
  :EasyArray<kv_pair>::EasyArray(kv_pair(key, val))
{
}

KV& KV::operator()(const char* key, ObString val)
{
  EasyArray<kv_pair>::operator()(kv_pair(key, val));
  return *this;
}

KV& KV::operator()(const char* key, int64_t val)
{
  EasyArray<kv_pair>::operator()(kv_pair(key, val));
  return *this;
}

ObMutatorHelper::ObMutatorHelper()
  :mutator_(NULL)
{
}

bool ObMutatorHelper::check_inner_stat() const
{
  return (NULL != mutator_);
}

int ObMutatorHelper::add_column(const char* table_name, const ObRowkey& rowkey, const char* column_name, const ObObj& value, ObMutator* mutator)
{
  int ret = OB_SUCCESS;
  ObString tname;
  ObString column;

  if(NULL == table_name)
  {
    ret = OB_INVALID_ARGUMENT;
    TBSYS_LOG(WARN, "table_name is null");
  }

  if(OB_SUCCESS == ret && NULL == column_name)
  {
    ret = OB_INVALID_ARGUMENT;
    TBSYS_LOG(WARN, "column_name is null");
  }

  if(OB_SUCCESS == ret && NULL == mutator)
  {
    ret = OB_INVALID_ARGUMENT;
    TBSYS_LOG(WARN, "mutator is null");
  }

  if(OB_SUCCESS == ret)
  {
    tname.assign_ptr(const_cast<char*>(table_name), static_cast<int32_t>(strlen(table_name)));
    column.assign_ptr(const_cast<char*>(column_name), static_cast<int32_t>(strlen(column_name)));

    ret = mutator->update(tname, rowkey, column, value);
    if(OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "add value to mutator fail:table_name[%s], column_name[%s]", 
        table_name, column_name);
    }
  }
  return ret;
}

int ObMutatorHelper::init(ObMutator* mutator)
{
  int ret = OB_SUCCESS;
  if(NULL == mutator)
  {
    ret = OB_INVALID_ARGUMENT;
    TBSYS_LOG(WARN, "mutator is null");
  }
  else
  {
    mutator_ = mutator;
  }
  return ret;
}

int ObMutatorHelper::update(const char* table_name, const ObRowkey& rowkey, const KV& kv)
{
  return insert(table_name, rowkey, kv);
}

int common::kv_to_rowkey(const KV& kv, ObRowkey& rowkey, ObObj* rowkey_obj, int32_t obj_buf_count)
{
  int ret = OB_SUCCESS;

  if(OB_SUCCESS != kv.get_exec_status())
  {
    ret = kv.get_exec_status();
    TBSYS_LOG(WARN, "kv.get_exec_status[%d]", kv.get_exec_status());
  }

  if(OB_SUCCESS == ret && kv.count() > obj_buf_count)
  {
    ret = OB_INVALID_ARGUMENT;
    TBSYS_LOG(WARN, "kv.count[%ld] > obj_buf_count[%d]", kv.count(), obj_buf_count);
  }

  kv_pair element;
  for(int32_t i=0;i<kv.count() && OB_SUCCESS == ret;i++)
  {
    ret = kv.at(i, element);
    if(OB_SUCCESS != ret)
    {
      ret = OB_ERROR;
      TBSYS_LOG(WARN, "get element from kv array fail");
    }
    if(OB_SUCCESS == ret)
    {
      rowkey_obj[i] = element.obj_;
    }
  }

  if(OB_SUCCESS == ret)
  {
    rowkey.assign(rowkey_obj, kv.count());
  }
  return ret;
}

int ObMutatorHelper::update(const char* table_name, const KV& rowkey_kv, const KV& kv)
{
  return insert(table_name, rowkey_kv, kv);
}

int ObMutatorHelper::insert(const char* table_name, const KV& rowkey_kv, const KV& kv)
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

int ObMutatorHelper::del_row(const char* table_name, const KV& rowkey_kv)
{
  int ret = OB_SUCCESS;

  ObRowkey rowkey;
  ObObj rowkey_obj[rowkey_kv.count()];

  ret = common::kv_to_rowkey(rowkey_kv, rowkey, rowkey_obj, static_cast<int32_t>(rowkey_kv.count())); 

  if(OB_SUCCESS == ret)
  {
    ret = del_row(table_name, rowkey);
  }

  return ret;
}

int ObMutatorHelper::insert(const char* table_name, const ObRowkey& rowkey, const KV& kv)
{
  int ret = OB_SUCCESS;

  if(!check_inner_stat())
  {
    ret = OB_ERROR;
    TBSYS_LOG(WARN, "check inner stat fail");
  }

  if(OB_SUCCESS == ret && NULL == table_name)
  {
    ret = OB_INVALID_ARGUMENT;
    TBSYS_LOG(WARN, "table_name is null");
  }

  if(OB_SUCCESS == ret && OB_SUCCESS != kv.get_exec_status())
  {
    ret = kv.get_exec_status();
    TBSYS_LOG(WARN, "kv array error:ret[%d]", ret);
  }

  if(OB_SUCCESS == ret && !check_inner_stat())
  {
    ret = OB_ERROR;
    TBSYS_LOG(WARN, "check inner stat fail");
  }

  kv_pair element;
  for(int32_t i=0;i<kv.count() && OB_SUCCESS == ret;i++)
  {
    ret = kv.at(i, element);
    if(OB_SUCCESS != ret)
    {
      ret = OB_ERROR;
      TBSYS_LOG(WARN, "get element from kv array fail");
    }
    if(OB_SUCCESS == ret)
    {
      ret = add_column(table_name, rowkey, element.key_, element.obj_, mutator_);
      if(OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "add column fail:ret[%d]", ret);
      }
    }
  }
  return ret;
}

int ObMutatorHelper::del_row(const char* table_name, const ObRowkey& rowkey)
{
  int ret = OB_SUCCESS;

  ObString ob_table_name;
  ob_table_name.assign_ptr(const_cast<char*>(table_name), static_cast<int32_t>(strlen(table_name)));

  if(!check_inner_stat())
  {
    ret = OB_ERROR;
    TBSYS_LOG(WARN, "check inner stat fail");
  }

  if(OB_SUCCESS == ret)
  {
    ret = mutator_->del_row(ob_table_name, rowkey);
    if(OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "add del row info to mutator_ fail:ret[%d]", ret);
    }
  }
  return ret;
}


