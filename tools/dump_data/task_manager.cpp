#include <vector>
#include "common/utility.h"
#include "task_manager.h"

using namespace std;
using namespace oceanbase::common;
using namespace oceanbase::tools;

static const int CS_HIST_VERSION = 2;

TaskManager::TaskManager(const int64_t avg_times, const int64_t max_count)
{
  TBSYS_LOG(DEBUG, "max times[%ld], max_count[%ld]", avg_times, max_count);
  if ((avg_times <= 0) || max_count <= 0)
  {
    TBSYS_LOG(WARN, "check max count failed:times[%ld], count[%ld]", avg_times, max_count);
    avg_times_ = DEFAULT_TIMES;
    max_count_ = DEFAULT_COUNT;
  }
  else
  {
    avg_times_ = avg_times;
    max_count_ = max_count;
  }

  task_id_alloc_ = 0;
  total_task_count_ = 0;
  task_token_ = tbsys::CTimeUtil::getTime();
  total_finish_time_ = MAX_WAIT_TIME;
  total_finish_count_ = 1;
  tablet_version_ = 0;
  TBSYS_LOG(INFO, "task token is created:token[%ld]", task_token_);
}

TaskManager::~TaskManager()
{
}

bool TaskManager::check_tasks(const uint64_t tablet_count)
{
  bool ret = true;
  if (tablet_count != wait_queue_.size())
  {
    ret = false;
    TBSYS_LOG(ERROR, "check tablet count failed:tablet[%lu], task[%lu]",
        tablet_count, wait_queue_.size());
  }
  else
  {
    TaskInfo cur_task;
    TaskInfo last_task;
    const ObNewRange * cur_range = NULL;
    const ObNewRange * last_range = NULL;
    map<uint64_t, TaskInfo>::const_iterator it;
    for (it = wait_queue_.begin(); it != wait_queue_.end(); ++it)
    {
      cur_task = it->second;
      if ((NULL != last_range ) && (last_task.get_param().get_table_name() == cur_task.get_param().get_table_name()))
      {
        last_range = last_task.get_param().get_range();
        cur_range = cur_task.get_param().get_range();
        if ((NULL == last_range) || (NULL == cur_range))
        {
          ret = false;
          TBSYS_LOG(ERROR, "check range failed:last[%p], cur[%p]", last_range, cur_range);
          break;
        }
        if (last_range->end_key_ != cur_range->start_key_)
        {
          TBSYS_LOG(ERROR, "check last tablet end_key not equal with cur tablet start_key:last[%s], cur[%s]",
              to_cstring(*last_range), to_cstring(*cur_range));
          ret = false;
          break;
        }
        else
        {
          last_task = cur_task;
          last_range = last_task.get_param().get_range();
          TBSYS_LOG(DEBUG, "check last tablet end_key equal with cur tablet start_key:last[%s], cur[%s]",
              to_cstring(*last_range), to_cstring(*cur_range));
          continue;
        }
      }
      // table changed
      cur_range = cur_task.get_param().get_range();
      if (!cur_range->start_key_.is_min_row())
      {
        ret = false;
        TBSYS_LOG(WARN, "check table frist tablet start_key is not min value:range[%s]", to_cstring(*cur_range));
        break;
      }
      if (last_range != NULL)
      {
        if (!last_range->end_key_.is_max_row())
        {
          ret = false;
          TBSYS_LOG(WARN, "check table last tablet start_key is not max value:range[%s]", to_cstring(*last_range));
          break;
        }
      }
      last_task = cur_task;
      last_range = last_task.get_param().get_range();
    }
  }

  if (true == ret)
  {
    int64_t count = 0;
    if (repair_all_tasks(count) != OB_SUCCESS)
    {
      TBSYS_LOG(ERROR, "repair task server list failed");
      ret = false;
    }
  }
  return ret;
}

bool TaskManager::get_tablet_version(int64_t memtable_version, int64_t &version)
{
  bool find = false;
  for (int64_t ver = memtable_version; ver > memtable_version - CS_HIST_VERSION; --ver)
  {
    map<uint64_t, TaskInfo>::iterator it = wait_queue_.begin();
    while (it != wait_queue_.end())
    {
      const TabletLocation &loc = it->second.get_location();
      int64_t i = 0;
      for(i = 0; i < loc.size(); i++)
      {
        TBSYS_LOG(DEBUG, "task_id=%ld, task_param=[table:%.*s, range:%s], version=%ld",
            it->first, it->second.get_param().get_table_name().length(), it->second.get_param().get_table_name().ptr(),
            to_cstring(*it->second.get_param().get_range()), loc[i].tablet_version_);
        /* if tablet version == version or tablet version == version + 1
         * we can dump data from this tablet
         */
        if ((ver == loc[i].tablet_version_) || ((ver + 1) == loc[i].tablet_version_))
        {
          break;
        }
      }
      if (i == loc.size())
      {
        TBSYS_LOG(DEBUG, "version:%ld can't be dumped from task[%ld]", ver, it->first);
        break;
      }
      it++;
    }
    if (it == wait_queue_.end())
    {
      tablet_version_ = version = ver;
      find = true;
      TBSYS_LOG(INFO, "version:%ld is selected", version);
      break;
    }
  }

  return find;
}

void TaskManager::setup_all_tasks_vesion(int64_t version)
{
  map<uint64_t, TaskInfo>::iterator it = wait_queue_.begin();
  while (it != wait_queue_.end())
  {
    ObScanParam *param = const_cast<ObScanParam *>(&it->second.get_param());
    ObVersionRange version_range;
    version_range.border_flag_.set_min_value();
    version_range.end_version_ = version;
    version_range.border_flag_.set_inclusive_end();
    param->set_version_range(version_range);
    it++;
  }
}

int TaskManager::repair_all_tasks(int64_t & count)
{
  bool find_valid_server = false;
  bool find_invalid_server = false;
  TaskInfo cur_task;
  map<uint64_t, TaskInfo>::iterator it;
  vector<TabletLocation> server_lists;
  for (it = wait_queue_.begin(); it != wait_queue_.end(); ++it)
  {
    cur_task = it->second;
    if (cur_task.get_location().size() > 0)
    {
      server_lists.push_back(cur_task.get_location());
      find_valid_server = true;
    }

    if (!find_invalid_server && (0 == cur_task.get_location().size()))
    {
      find_invalid_server = true;
    }
  }

  int ret = OB_SUCCESS;
  if (find_invalid_server)
  {
    if (find_valid_server)
    {
      int64_t i = 0;
      for (it = wait_queue_.begin(); it != wait_queue_.end(); ++it)
      {
        if (0 == it->second.get_location().size())
        {
          ++count;
          it->second.set_location(server_lists[(++i) % server_lists.size()]);
        }
      }
    }
    else
    {
      TBSYS_LOG(ERROR, "find invalid server list but not find valid list");
      ret = OB_ERROR;
    }
  }
  TBSYS_LOG(INFO, "repair task count:count[%lu]", count);
  return ret;
}

// add a new task
int TaskManager::insert_task(const TabletLocation & location, TaskInfo & task)
{
  int ret = OB_SUCCESS;
  task.set_location(location);
  task.set_token(task_token_);
  int64_t timestamp = tbsys::CTimeUtil::getTime();
  task.set_timestamp(timestamp);
  std::map<ObServer, int64_t>::const_iterator it;
  tbsys::CThreadGuard lock(&lock_);
  task.set_id(++task_id_alloc_);
  ++total_task_count_;
  wait_queue_.insert(pair<uint64_t, TaskInfo>(task.get_id(), task));
  for (int64_t i = 0; i < location.size(); ++i)
  {
    // TODO server counter for select task
    it = server_manager_.find(location[i].chunkserver_);
    if (it != server_manager_.end())
    {
      server_manager_[location[i].chunkserver_] = it->second + 1;
    }
    else
    {
      server_manager_.insert(pair<ObServer, int64_t>(location[i].chunkserver_, 1));
    }
  }
  TBSYS_LOG(DEBUG, "insert task succ:id[%lu], table_name[%.*s], range[%s], count[%lu]", task_id_alloc_,
      task.get_param().get_table_name().length(), task.get_param().get_table_name().ptr(),
      to_cstring(*task.get_param().get_range()), total_task_count_);
#if false
  std::map<uint64_t, TaskInfo>::iterator temp_it;
  for (temp_it = wait_queue_.begin(); temp_it != wait_queue_.end(); ++temp_it)
  {
    TBSYS_LOG(TRACE, "dump task range:task[%ld], range[%p:%s], key_obj[%p:%p]", temp_it->first,
        temp_it->second.get_param().get_range(), to_cstring(*temp_it->second.get_param().get_range()),
        temp_it->second.get_param().get_range()->start_key_.ptr(), temp_it->second.get_param().get_range()->end_key_.ptr());
  }
#endif
  return ret;
}

int TaskManager::fetch_task(TaskCounter & counter, TaskInfo & task)
{
  int ret = OB_SUCCESS;
  int64_t task_count = 0;
  bool find_task = false;
  map<uint64_t, TaskInfo>::iterator it;
  tbsys::CThreadGuard lock(&lock_);
  // step 1. check waiting task
  for (it = wait_queue_.begin(); it != wait_queue_.end(); ++it)
  {
    for (int64_t i = 0; i < it->second.get_location().size(); ++i)
    {

      if (it->second.get_location()[i].tablet_version_ != tablet_version_ &&
          it->second.get_location()[i].tablet_version_ != (tablet_version_ + 1))
      {
#if 1
        it->second.get_location()[i].dump(it->second.get_location()[i]);
        TBSYS_LOG(DEBUG, "skip task[%ld], due to version compatiablility", it->first);
#endif
        continue;
      }

      TBSYS_LOG(DEBUG, "server:%s is selected, task_count = %ld",
                it->second.get_location()[i].chunkserver_.to_cstring(), task_count);

      task_count = get_server_task_count(it->second.get_location()[i].chunkserver_);
      if (task_count >= max_count_)
      {
        continue;
      }
      else
      {
        task = it->second;
        task.set_index(i);
        find_task = true;
        task.set_timestamp(tbsys::CTimeUtil::getTime());
        // remove this item to doing_queue_
        wait_queue_.erase(it);
        doing_queue_.insert(pair<uint64_t, TaskInfo>(task.get_id(), task));
        break;
      }
    }
    if (find_task)
    {
      break;
    }
  }

  // step 2. check doing timeout task
  if ((false == find_task) && (total_finish_count_ != 0))
  {
    int64_t timestamp = tbsys::CTimeUtil::getTime();
    int64_t avg_finish_time = total_finish_time_/total_finish_count_;
    for (it = doing_queue_.begin(); it != doing_queue_.end(); ++it)
    {
      if ((timestamp - it->second.get_timestamp()) > (avg_times_ * avg_finish_time))
      {
        int64_t last_index = task.get_index();  /* last used mergeserver index */

        // timeout so reset the visit count
        for (int64_t i = 0; i < it->second.get_location().size(); ++i)
        {
          if (i == last_index) {
            continue;                           /* do not allocate same task to same server */
          }

          task_count = get_server_task_count(it->second.get_location()[i].chunkserver_);
          // must > not include equal with
          if (task_count > max_count_)
          {
            continue;
          }
          else
          {
            TBSYS_LOG(INFO, "check task timeout:task[%lu], avg_time[%ld], timeout_times[%ld], "
                "total_time[%ld], total_finish[%ld], finish[%ld], now[%ld], add_time[%ld], old_idx=%lu, new_idx=%lu",
                it->second.get_id(), avg_finish_time, avg_times_, total_finish_time_, total_finish_count_,
                complete_queue_.size(), timestamp, it->second.get_timestamp(),
                last_index, i);
            // update timestamp
            it->second.set_timestamp(timestamp);
            task = it->second;
            task.set_index(i);
            find_task = true;
            break;
          }
        }

        if (find_task)
        {
          break;
        }
      }
    }
  }

  // set task start timestamp
  task.set_timestamp(tbsys::CTimeUtil::getTime());
  counter.total_count_ = total_task_count_;
  counter.wait_count_ = wait_queue_.size();
  counter.doing_count_ = doing_queue_.size();
  counter.finish_count_ = complete_queue_.size();
  if (false == find_task)
  {
    TBSYS_LOG(DEBUG, "not find suitable task");
    ret = OB_ERROR;
  }
  else
  {
    // update working queue for first merge server
    working_queue_[task.get_location()[task.get_index()].chunkserver_] = ++task_count;
    //print_access_server();
  }
  return ret;
}

void TaskManager::print_access_server()
{
  map<common::ObServer, int64_t>::const_iterator it = working_queue_.begin();
  for (it = working_queue_.begin(); it != working_queue_.end(); ++it)
  {
    TBSYS_LOG(INFO, "server info:ip[%s], count[%ld]", it->first.to_cstring(), it->second);
  }
}

int64_t TaskManager::get_server_task_count(const ObServer & server) const
{
  int64_t task_count = 0;
  map<common::ObServer, int64_t>::const_iterator it = working_queue_.find(server);
  if (it != working_queue_.end())
  {
    task_count = it->second;
  }
  else
  {
    task_count = 0;
  }
  return task_count;
}

bool TaskManager::check_finish(void) const
{
  return (total_task_count_ == complete_queue_.size());
}

int TaskManager::print_info(void) const
{
  TBSYS_LOG(INFO, "task token:%ld", task_token_);
  TBSYS_LOG(INFO, "max task id:%lu", task_id_alloc_);
  TBSYS_LOG(INFO, "total count:%lu", total_task_count_);
  TBSYS_LOG(INFO, "wait task count:%ld", wait_queue_.size());
  TBSYS_LOG(INFO, "doing task count:%ld", doing_queue_.size());
  TBSYS_LOG(INFO, "finish task count:%ld", complete_queue_.size());
  TBSYS_LOG(INFO, "visit server count:%ld", working_queue_.size());
  TBSYS_LOG(INFO, "merge server count:%ld", server_manager_.size());
  return OB_SUCCESS;
}

int TaskManager::finish_task(const bool result, const TaskInfo & task)
{
  int ret = OB_SUCCESS;
  if (task.get_token() != task_token_)
  {
    TBSYS_LOG(ERROR, "check task token failed:token[%ld], task[%ld]",
        task_token_, task.get_token());
    ret = OB_ERROR;
  }
  else
  {
    map<uint64_t, TaskInfo>::iterator it;
    int64_t timestamp = tbsys::CTimeUtil::getTime();
    tbsys::CThreadGuard lock(&lock_);
    int64_t task_count = get_server_task_count(task.get_location()[task.get_index()].chunkserver_);
    if (task_count < 1)
    {
      TBSYS_LOG(WARN, "check server task count failed:task[%lu], count[%ld]", task.get_id(), task_count);
    }
    else
    {
      TBSYS_LOG(DEBUG, "server ip = %s, task_count = %ld",
                task.get_location()[task.get_index()].chunkserver_.to_cstring(), task_count);
      // wait timeout for next dispatch
      working_queue_[task.get_location()[task.get_index()].chunkserver_] = --task_count;
      // print_access_server();
    }

    it = doing_queue_.find(task.get_id());
    if (it != doing_queue_.end())
    {
      if (true == result)
      {
        ++total_finish_count_;
        total_finish_time_ += timestamp - task.get_timestamp();
        complete_queue_.insert(pair<uint64_t, TaskInfo>(task.get_id(), task));
        doing_queue_.erase(it);
      }
      // WARN: not insert into wait queue for timeout if result != true
    }
    else
    {
      it = complete_queue_.find(task.get_id());
      if (it != complete_queue_.end())
      {
        if (true == result)
        {
          // for compute average finish time
          ++total_finish_count_;
          total_finish_time_ += timestamp - task.get_timestamp();
        }
        TBSYS_LOG(WARN, "find the task already finished:task[%lu]", task.get_id());
      }
      else
      {
        TBSYS_LOG(ERROR, "not find this task in doing and complete queue:task[%lu]", task.get_id());
        ret = OB_ERROR;
      }
    }
  }
  TBSYS_LOG(INFO, "finish monitor task [id=%lu] stat:wait[%lu], doing[%lu], finish[%lu], avg_time[%ld]", task.get_id(),
      wait_queue_.size(), doing_queue_.size(), complete_queue_.size(), total_finish_time_ / total_finish_count_);
  return ret;
}

void TaskManager::dump_tablets(const char *file)
{
  FILE * f = fopen(file, "w");
  char *key_buf = NULL;

  if (NULL == f)
  {
    TBSYS_LOG(ERROR, "can't write tablets list file, %s", file);
    return;
  }

  key_buf = new (std::nothrow) char[2 * OB_MAX_ROW_KEY_LENGTH];
  if (NULL == key_buf)
  {
    TBSYS_LOG(ERROR, "can't allocate memory");
    fclose(f);
    return;
  }

  map<uint64_t, TaskInfo>::iterator itr = wait_queue_.begin();
  while (itr != wait_queue_.end())
  {
    TabletLocation loc = itr->second.get_location();
    fprintf(f, "TaskId:%ld, ReplicaSize=%ld, TableId:%ld\n",
        itr->first, loc.size(), itr->second.get_table_id());

    const ObNewRange * range = itr->second.get_param().get_range();
    fprintf(f, "S:%s\n", to_cstring(range->start_key_));
    fprintf(f, "E:%s\n", to_cstring(range->end_key_));

    for (int i = 0;i < loc.size();i++)
    {
      loc[i].chunkserver_.to_string(key_buf, 2 * OB_MAX_ROW_KEY_LENGTH);
      fprintf(f, "MergeServer=%s\n", key_buf);
    }

    fprintf(f, "\n");

    itr++;
  }

  fclose(f);
  delete [] key_buf;
}

