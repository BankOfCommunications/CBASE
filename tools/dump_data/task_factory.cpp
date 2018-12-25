#include "common/ob_rowkey.h"
#include "task_factory.h"
#include <set>

using namespace std;
using namespace oceanbase::common;
using namespace oceanbase::tools;

TaskFactory::TaskFactory()
{
  memtable_version_ = 0;
  timeout_ = -1;
  schema_ = NULL;
  rpc_ = NULL;
  task_manager_ = NULL;
  TBSYS_LOG(DEBUG, "in constructor task factory");
  confs_ = NULL;
}

int TaskFactory::init(const int64_t version, const int64_t timeout, const ObServer & root_server,
  const ObSchemaManagerV2 * schema, RpcStub * rpc, TaskManager * manager)
{
  int ret = OB_SUCCESS;
  if ((NULL == rpc) || (NULL == manager) || (NULL == schema))
  {
    TBSYS_LOG(ERROR, "check rpc or task manager failed:rpc[%p], task[%p], schema[%p]",
      rpc, manager, schema);
    ret = OB_ERROR;
  }
  else
  {
    memtable_version_ = version;
    timeout_ = timeout;
    root_server_ = root_server;
    rpc_ = rpc;
    schema_ = schema;
    task_manager_ = manager;
  }
  return ret;
}

TaskFactory::~TaskFactory()
{
}

void TaskFactory::add_table_confs(const std::vector<TableConf> *confs)
{
  confs_ = confs;
}

int TaskFactory::add_table(const char * table_name)
{
  int ret = OB_SUCCESS;
  if (NULL == table_name)
  {
    TBSYS_LOG(ERROR, "check table name failed:name[%s]", table_name);
    ret = OB_ERROR;
  }
  else
  {
    set<string>::iterator it = dump_tables_.find(string(table_name));
    if (it != dump_tables_.end())
    {
      TBSYS_LOG(ERROR, "table already exist:table[%s]", table_name);
      ret = OB_ERROR;
    }
    else
    {
      dump_tables_.insert(string(table_name));
    }
  }
  return ret;
}

bool TaskFactory::check_string(const ObString & name)
{
  return ((NULL != name.ptr()) && (0 != name.length()));
}

// checking is the last tablet
bool TaskFactory::is_max_rowkey(const int64_t max_len, const char rowkey[], const int64_t key_len)
{
  bool ret = true;
  if ((max_len < 0) || (key_len > max_len))
  {
    ret = false;
    TBSYS_LOG(ERROR, "check table max_len or rowkey failed:max[%ld], key[%s], len[%ld]",
        max_len, rowkey, key_len);
  }
  else if (key_len == max_len)
  {
    for (int i = 0; i < key_len; ++i)
    {
      if ((unsigned char)*(rowkey + i) < 0xFF)
      {
        ret = false;
        break;
      }
    }
  }
  else
  {
    ret = false;
  }
  return ret;
}

int TaskFactory::get_all_tablets(uint64_t & tablet_count)
{
  int ret = OB_SUCCESS;
  if (false == check_inner_stat())
  {
    TBSYS_LOG(ERROR, "%s", "check inner stat failed");
    ret = OB_ERROR;
  }
  else
  {
    const ObTableSchema * table = NULL;
    set<string>::const_iterator it;
    for (it = dump_tables_.begin();it != dump_tables_.end(); ++it)
    {
      // check table exist
      table = schema_->get_table_schema((*it).c_str());
      if (NULL == table)
      {
        ret = OB_ERROR;
        TBSYS_LOG(ERROR, "check table schema failed:table[%s]", (*it).c_str());
        break;
      }
      uint64_t count = 0;
      ret = get_table_tablet((*it).c_str(), table->get_table_id(), count);
      if (ret != OB_SUCCESS)
      {
        TBSYS_LOG(ERROR, "get table's tablet failed:table[%s], id[%lu]", (*it).c_str(),
            table->get_table_id());
        break;
      }
      else
      {
        TBSYS_LOG(DEBUG, "get table tablet succ:table[%s], count[%lu]", (*it).c_str(), count);
        tablet_count += count;
      }
    }
  }
  return ret;
}

int TaskFactory::init_scan_param(const char *table_name, const uint64_t table_id, uint64_t & max_len, ObScanParam & param)
{
  // init version range
  ObVersionRange version_range;
  //version_range.start_version_ = 0;
  version_range.border_flag_.set_min_value();
  version_range.end_version_ = memtable_version_;
  version_range.border_flag_.set_inclusive_end();
  //version_range.border_flag_.set_max_value();
  param.set_version_range(version_range);
  param.set_is_read_consistency(false);
  param.set_is_result_cached(false);
  param.set_is_read_consistency(false);
  int ret = get_max_len(table_id, max_len);
  if (ret != OB_SUCCESS)
  {
    TBSYS_LOG(ERROR, "get table max rowkey len failed:table[%lu], ret[%d]", table_id, ret);
  }
  else
  {
//    ret = add_all_columns(table_id, param);
    ret = add_columns(table_name, table_id, param);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(ERROR, "add all columns failed:table[%lu], ret[%d]", table_id, ret);
    }
  }
  return ret;
}

int TaskFactory::get_max_len(const uint64_t table_id, uint64_t & max_len)
{
  int ret = OB_SUCCESS;
  const ObTableSchema * schema = schema_->get_table_schema(table_id);
  if (NULL == schema)
  {
    TBSYS_LOG(ERROR, "get table schema failed:table[%lu]", table_id);
    ret = OB_ERROR;
  }
  else
  {
    max_len = schema->get_rowkey_max_length();
  }
  return ret;
}

bool TaskFactory::get_table_conf(const char *table_name, const TableConf *&conf)
{
  bool ret = false;

  for(size_t i = 0;confs_ != NULL && i < confs_->size(); i++)
  {
    if (confs_->at(i).table_name() == table_name)
    {
      conf = &confs_->at(i);
      ret = true;
      break;
    }
  }

  return ret;
}

int TaskFactory::add_columns_conf(const uint64_t table_id, common::ObScanParam & param, const TableConf *conf)
{
  int ret = OB_SUCCESS;
  TableConf::ColumnIterator itr = conf->column_begin();

  ObString column;
  while (itr != conf->column_end())
  {
    const char *column_name = *itr;
    if (conf->is_null_column(column_name) || conf->is_rowkey(column_name))
    {
      itr++;
      continue;
    }

    TBSYS_LOG(DEBUG, "columns with conf, column = %s", column_name);
    column.assign(const_cast<char *>(*itr), (int32_t)strlen(column_name));
    ret = param.add_column(column);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(ERROR, "add column failed:table[%lu], column[%s], ret[%d]",
                table_id, column_name, ret);
      ret = OB_ERROR;
      break;
    }

    itr++;
  }

  return ret;
}

int TaskFactory::add_columns(const char *table_name, const uint64_t table_id, common::ObScanParam & param)
{
  int ret = OB_SUCCESS;
  const TableConf *conf = NULL;

  if (get_table_conf(table_name, conf))
  {
    TBSYS_LOG(DEBUG, "add columns, using conf %s", conf->DebugString().c_str());
    ret = add_columns_conf(table_id, param, conf);
  }
  else
  {
    ret = add_all_columns(table_id, param);
  }

  return ret;
}

int TaskFactory::add_all_columns(const uint64_t table_id, ObScanParam & param)
{
  int ret = OB_SUCCESS;
  int32_t count = 0;
  const ObColumnSchemaV2 * column_schema = schema_->get_table_schema(table_id, count);
  if ((NULL == column_schema) || (0 == count))
  {
    ret = OB_ERROR;
    TBSYS_LOG(ERROR, "check column schema or schema count failed:column[%p], count[%d]",
        column_schema, count);
  }
  else
  {
    ObString column;
    const char * column_name = NULL;
    for (int32_t i = 0; i < count; ++i)
    {
      column_name = column_schema->get_name();
      if (column_name != NULL)
      {
        TBSYS_LOG(DEBUG, "TableId:%ld, COLUMN=%s", table_id, column_name);

        column.assign(const_cast<char *>(column_name), (int32_t)strlen(column_name));
        ret = param.add_column(column);
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(ERROR, "add column failed:table[%lu], column[%s], ret[%d]",
              table_id, column_name, ret);
          ret = OB_ERROR;
          break;
        }
      }
      else
      {
        ret = OB_ERROR;
        TBSYS_LOG(ERROR, "check column name failed:table[%lu], name[%s]", table_id, column_name);
        break;
      }
      ++column_schema;
    }
  }
  return ret;
}


int TaskFactory::setup_tablets_version()
{
  int ret = OB_SUCCESS;
  int64_t version = 0;

  bool find = task_manager_->get_tablet_version(memtable_version_, version);
  if (true == find)
  {
    task_manager_->setup_all_tasks_vesion(version);
  }
  else
  {
    ret = OB_ERROR;
    TBSYS_LOG(WARN, "Oceanbase Merging in process, Please dump in another time");
  }

  return ret;
}

// maybe not find a merge server
int TaskFactory::get_table_tablet(const char * table_name, const uint64_t table_id, uint64_t & count)
{
  int ret = OB_SUCCESS;
  ObScanParam scan_param;
  uint64_t max_len = 0;
  if (NULL == table_name)
  {
    TBSYS_LOG(ERROR, "check table name failed:table[%s], id[%lu]", table_name, table_id);
    ret = OB_ERROR;
  }
  else
  {
    ret = init_scan_param(table_name, table_id, max_len, scan_param);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(ERROR, "init scan param failed:table[%s], ret[%d]", table_name, ret);
    }
  }

  if (OB_SUCCESS == ret)
  {
    // for the first table tablet
    ObRowkey row_key;
    ObObj temp_buffer[common::OB_MAX_ROWKEY_COLUMN_NUMBER];
    row_key.assign(temp_buffer, common::OB_MAX_ROWKEY_COLUMN_NUMBER);
    ObGetParam param;
    ObScanner scanner;
    ObServer server;
    ObRowkey start_key;
    ObRowkey end_key;
    ObCellInfo * cell = NULL;
    ObScannerIterator iter;
    bool row_change = false;
    ObString name;
    name.assign(const_cast<char*>(table_name), (int32_t)strlen(table_name));
    ObCellInfo temp_cell;
    temp_cell.table_id_ = table_id;
    temp_cell.column_id_ = 0;
    common::ModuleArena allocator;
    while ((OB_SUCCESS == ret) && (!row_key.is_max_row()))
    {
      param.reset();
      param.set_is_read_consistency(false);
      temp_cell.row_key_ = row_key;
      ret = param.add_cell(temp_cell);
      if (ret != OB_SUCCESS)
      {
        TBSYS_LOG(ERROR, "add cell failed:ret[%d]", ret);
        break;
      }
      ret = rpc_->get(root_server_, timeout_, param, scanner);
      if (ret != OB_SUCCESS)
      {
        TBSYS_LOG(ERROR, "get root table for tablet failed:table[%lu], ret[%d]", table_id, ret);
        break;
      }
      else
      {
        // skip the first row
        iter = scanner.begin();
        ++iter;
        while ((iter != scanner.end())
            && (OB_SUCCESS == (ret = iter.get_cell(&cell, &row_change))) && !row_change)
        {
          if (NULL == cell)
          {
            TBSYS_LOG(ERROR, "%s", "check cell failed");
            ret = OB_INNER_STAT_ERROR;
            break;
          }
          ret = cell->row_key_.deep_copy(start_key, allocator);
          if (ret != OB_SUCCESS)
          {
            TBSYS_LOG(WARN, "deep copy the rowkey failed:ret[%d]", ret);
          }
          ++iter;
        }
      }
      // scanner iter end
      if (ret == OB_SUCCESS)
      {
        int64_t ip = 0;
        int64_t port = 0;
        int64_t version = 0;
        TabletLocation list;
        for (++iter; iter != scanner.end(); ++iter)
        {
          ret = iter.get_cell(&cell, &row_change);
          if (ret != OB_SUCCESS)
          {
            TBSYS_LOG(ERROR, "get cell from scanner iterator failed:ret[%d]", ret);
            break;
          }
          else if (row_change) // && (iter != last_iter))
          {
            TaskInfo task;
            task.set_table_id(table_id);
            task.set_table_name(table_name);
            ret = init_new_task(name, start_key, end_key, scan_param, task);
            if (ret != OB_SUCCESS)
            {
              TBSYS_LOG(ERROR, "init new task failed:ret[%d]", ret);
              break;
            }
            else
            {
              ret = insert_new_task(list, task);
              if (ret != OB_SUCCESS)
              {
                TBSYS_LOG(ERROR, "insert new task failed:ret[%d]", ret);
                break;
              }
              ++count;
            }
            list.clear();
            start_key = end_key;
          }
          else
          {
            cell->row_key_.deep_copy(end_key, allocator);
            if ((cell->column_name_.compare("1_ms_port") == 0)
                || (cell->column_name_.compare("2_ms_port") == 0)
                || (cell->column_name_.compare("3_ms_port") == 0))
            {
              ret = cell->value_.get_int(port);
            }
            else if ((cell->column_name_.compare("1_ipv4") == 0)
                || (cell->column_name_.compare("2_ipv4") == 0)
                || (cell->column_name_.compare("3_ipv4") == 0))
            {
              ret = cell->value_.get_int(ip);
            }
            else if ((cell->column_name_.compare("1_tablet_version") == 0)
                || (cell->column_name_.compare("2_tablet_version") == 0)
                || (cell->column_name_.compare("3_tablet_version") == 0))
            {
              ret = cell->value_.get_int(version);
              if (OB_SUCCESS == ret)
              {
                if (0 == port || port == 0)
                {
                  TBSYS_LOG(WARN, "%s", "check port or ip failed");
                }
                else
                {
                  server.set_ipv4_addr(int32_t(ip), int32_t(port));
                  ObTabletLocation addr(version, server);
                  if (OB_SUCCESS != (ret = list.add(addr)))
                  {
                    TBSYS_LOG(ERROR, "add addr failed:server[%d], port[%d], ret[%d]",
                        server.get_ipv4(), server.get_port(), ret);
                  }
                  else
                  {
                    TBSYS_LOG(DEBUG, "add addr succ:server[%s], version:%ld", server.to_cstring(), version);
                  }
                }
                ip = port = version = 0;
              }

            }

            if (ret != OB_SUCCESS)
            {
              TBSYS_LOG(ERROR, "check get value failed:ret[%d]", ret);
              break;
            }
          }
        }

        // for the last row
        if ((OB_SUCCESS == ret) && (start_key != end_key))
        {
          TaskInfo task;
          task.set_table_id(table_id);
          task.set_table_name(table_name);
          ret = init_new_task(name, start_key, end_key, scan_param, task);
          if (ret != OB_SUCCESS)
          {
            TBSYS_LOG(ERROR, "init new task failed:ret[%d]", ret);
          }
          else
          {
            ret = insert_new_task(list, task);
            if (ret != OB_SUCCESS)
            {
              TBSYS_LOG(ERROR, "insert new task failed:ret[%d]", ret);
            }
            ++count;
          }

          if (OB_SUCCESS == ret)
          {
            // copy all the end key objs to row_key
            for (int64_t i = 0; i < end_key.length(); ++i)
            {
              const_cast<ObObj *> (row_key.ptr())[i] = end_key.ptr()[i];
              row_key.assign(const_cast<ObObj *> (row_key.ptr()), end_key.length());
            }
            // modify last row key for next get root table
            if (!end_key.is_max_row())
            {
              const_cast<ObObj *>(row_key.ptr())[end_key.length()].set_max_value();
              row_key.assign(const_cast<ObObj *> (row_key.ptr()), end_key.length() + 1);
            }
          }
        }
        list.clear();
      }
    }
  }
  return ret;
}

int TaskFactory::init_new_task(const ObString & table_name, const ObRowkey & start_key,
    const ObRowkey & end_key, ObScanParam & scan_param, TaskInfo & task) const
{
  int ret = OB_SUCCESS;
  if (!check_string(table_name))
  {
    TBSYS_LOG(WARN, "check table name or end key failed:name[%.*s]",
      table_name.length(), table_name.ptr());
    ret = OB_ERROR;
  }
  else
  {
    ObNewRange range;
    range.border_flag_.unset_inclusive_start();
    range.border_flag_.set_inclusive_end();
    if (NULL == start_key.ptr())
    {
      range.border_flag_.set_min_value();
    }
    else
    {
      range.start_key_ = start_key;
      range.border_flag_.unset_min_value();
    }
    range.border_flag_.unset_max_value();
    range.end_key_ = end_key;
    scan_param.set(OB_INVALID_ID, table_name, range);
    // deep copy the param to task info param
    ret = task.set_param(scan_param);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(ERROR, "set param failed:ret[%d]", ret);
    }
  }
  return ret;
}

int TaskFactory::insert_new_task(const TabletLocation & list, TaskInfo & task)
{
  int ret = OB_SUCCESS;
  // alarm instantly
  if (0 == list.size())
  {
    TBSYS_LOG(ERROR, "check task server list count failed:task[%lu], count[%lu]",
        task.get_id(), list.size());
  }
  // remain do insert task
  ret = task_manager_->insert_task(list, task);
  if (ret != OB_SUCCESS)
  {
    TBSYS_LOG(ERROR, "insert a new task failed:ret[%d]", ret);
  }
  else
  {
    TBSYS_LOG(TRACE, "insert a new task succ:task[%lu], table_name[%.*s], tablet[%s]",
        task.get_id(), task.get_param().get_table_name().length(), task.get_param().get_table_name().ptr(),
        to_cstring(*task.get_param().get_range()));
  }
  return ret;
}

