#include <new>
#include <stdio.h>
#include "common/ob_define.h"
#include "common/ob_result.h"
#include "common/ob_malloc.h"
#include "common/utility.h"
#include "task_packet.h"
#include "task_worker.h"
#include <string>

using namespace oceanbase::common;
using namespace oceanbase::tools;
using namespace std;
using namespace tbsys;

#define HOST_NAME_LEN 256
#define RESET_STAT() stat_.reset()
#define INC_STAT(type, value) stat_.inc_stat(type, value)
#define DUMP_STAT(x) stat_.dump((x))

TaskWorker::TaskWorker(Env *env, const ObServer & server, const int64_t timeout,
    const uint64_t retry_times, const TaskWorkerParam *param)
{
  server_ = server;
  timeout_ = timeout;
  retry_times_ = retry_times;
  temp_result_ = NULL;
  output_path_ = "/tmp/";
  env_ = env;
  param_ = param;
  assert(param_ != NULL);
}

TaskWorker::~TaskWorker()
{
  if (NULL != temp_result_)
  {
    delete []temp_result_;
    temp_result_ = NULL;
  }
}

int TaskWorker::init(RpcStub * rpc, const char * data_path)
{
  int ret = OB_SUCCESS;
  if ((NULL == rpc) || (temp_result_ != NULL))
  {
    TBSYS_LOG(ERROR, "check data path or rpc stub failed:path[%s], temp_result[%p], stub[%p]",
        data_path, temp_result_, rpc);
    ret = OB_INPUT_PARAM_ERROR;
  }
  else
  {
    ret = BaseClient::init(server_);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(WARN, "init rpc failed:server[%s], ret[%d]", server_.to_cstring(), ret);
    }
  }
  if (OB_SUCCESS == ret)
  {
    rpc_ = rpc;
    temp_result_ = new(std::nothrow) char[OB_MAX_PACKET_LENGTH];
    if (NULL == temp_result_)
    {
      TBSYS_LOG(ERROR, "check new memory failed:mem[%p]", temp_result_);
      ret = OB_ERROR;
    }
    output_path_ = data_path;
  }

  if (ret == OB_SUCCESS)
  {
    if (env_ == NULL || !env_->valid())
    {
      TBSYS_LOG(ERROR, "Env is invalid");
      ret = OB_ERROR;
    }
  }
  return ret;
}

int TaskWorker::fetch_task(TaskCounter & count, TaskInfo & task)
{
  int ret = OB_SUCCESS;

  RESET_STAT();

  if (check_inner_stat())
  {
    for (uint64_t i = 0; i <= MAX_RETRY_TIMES; ++i)
    {
      ret = rpc_->fetch_task(server_, timeout_, count, task);
      if (ret != OB_SUCCESS)
      {
        TBSYS_LOG(WARN, "check fetch task failed:ret[%d]", ret);
        usleep(__useconds_t(RETRY_INTERVAL * (i + 1)));
      }
      else
      {
        TBSYS_LOG(INFO, "fetch task succ:task[%lu]", task.get_id());
        // the param is from local rpc buffer so deserialize to its own buffer
        ret = task.set_param(task.get_param());
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(ERROR, "set task its own param failed:ret[%d]", ret);
        }
        break;
      }
    }
  }
  else
  {
    ret = OB_ERROR;
    TBSYS_LOG(ERROR, "%s", "check inner stat failed");
  }
  return ret;
}


int TaskWorker::report_task(const int64_t result_code, const char * file_name, const TaskInfo & task)
{
  int ret = OB_SUCCESS;
  DUMP_STAT(task.get_id());
  if ((NULL != file_name) && check_inner_stat())
  {
    for (uint64_t i = 0; i <= MAX_RETRY_TIMES; ++i)
    {
      ret = rpc_->report_task(server_, timeout_, result_code, file_name, task);
      if (ret != OB_SUCCESS)
      {
        TBSYS_LOG(WARN, "check report task failed:ret[%d]", ret);
        usleep(RETRY_INTERVAL);
      }
      else
      {
        break;
      }
    }
  }
  else
  {
    ret = OB_ERROR;
    TBSYS_LOG(ERROR, "check inner stat or file_name failed:file[%s]", file_name);
  }
  return ret;
}

int TaskWorker::scan(const int64_t index, const TabletLocation & list, const ObScanParam & param,
    ObScanner & result)
{
  int ret = OB_SUCCESS;
  if (check_inner_stat())
  {
    ret = rpc_->scan(index, list, timeout_, param, result);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(WARN, "check scan task failed:ret[%d]", ret);
    }
  }
  else
  {
    ret = OB_ERROR;
    TBSYS_LOG(ERROR, "%s", "check inner stat failed");
  }
  return ret;
}

int TaskWorker::do_task(TaskOutputFile * file, const TaskInfo & task)
{
  ObScanner scanner;
  ObScanParam param;
  int ret = param.safe_copy(task.get_param());
  if (OB_SUCCESS == ret)
  {
    int64_t table_id = task.get_table_id();
    const TableConf *conf = NULL;
    param_->get_table_conf(task.get_table_name(), conf);
    if (conf != NULL)
    {
      TBSYS_LOG(DEBUG, "using table conf, table_id=%ld", table_id);
      file->set_table_conf(conf);
    }
    else
    {
      TBSYS_LOG(DEBUG, "do not use table conf, table_id=%ld", table_id);
    }
  }
  else
  {
    TBSYS_LOG(WARN, "safe copy scan param failed:ret[%d]", ret);
  }
  int64_t scan_times = 0;
  int64_t total_scan_time = 0;
  int64_t total_output_time = 0;

  common::ObNewRange new_range;
  common::ModuleArena rowkey_allocator;
  while (ret == OB_SUCCESS)
  {
    int64_t scan_start_time = CTimeUtil::getTime();
    ret = scan(task.get_index(), task.get_location(), param, scanner);
    if (ret != OB_SUCCESS)
    {
      INC_STAT(TASK_FAILED_SCAN_COUNT, 1);
      param.get_range()->hex_dump();
      TBSYS_LOG(ERROR, "scan failed:task[%lu], ret[%d]", task.get_id(), ret);
      break;
    }
    else
    {
      total_scan_time += CTimeUtil::getTime() - scan_start_time;
      scan_times++;
      //update stats
      INC_STAT(TASK_SUCC_SCAN_COUNT, 1);
      INC_STAT(TASK_TOTAL_REPONSE_BYTES, scanner.get_serialize_size());

      int64_t start_output_time = CTimeUtil::getTime();
      ret = output(file, scanner);
      if (ret != OB_SUCCESS)
      {
        TBSYS_LOG(ERROR, "output result failed:task[%lu], ret[%d]", task.get_id(), ret);
        break;
      }
      total_output_time += CTimeUtil::getTime() - start_output_time;
      bool finish = false;
      ret = check_finish(param, scanner, finish);
      if (ret != OB_SUCCESS)
      {
        TBSYS_LOG(ERROR, "check result finish failed:task[%lu], ret[%d]", task.get_id(), ret);
        break;
      }
      else if (!finish)
      {
        rowkey_allocator.reuse();
        ret = modify_param(scanner, rowkey_allocator, new_range, param);
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(ERROR, "modify param for next query failed:task[%lu], ret[%d]",
              task.get_id(), ret);
          break;
        }
      }
      else
      {
        TBSYS_LOG(INFO, "do the task succ:task[%lu]", task.get_id());
        TBSYS_LOG(INFO, "Average Scan Time: [%ld]", total_scan_time / scan_times);
        TBSYS_LOG(INFO, "Average Output Time: [%ld]", total_output_time / scan_times);
        break;
      }
    }
  }
  return ret;
}

int TaskWorker::do_task(const char * file_name, const TaskInfo & task)
{
  int ret = OB_SUCCESS;
  char temp_name[MAX_PATH_LEN] = "";
  snprintf(temp_name, sizeof(temp_name), "%s.temp", file_name);
  TaskOutputFile file(temp_name, env_);

  ret = file.open();
  if (ret != OB_SUCCESS)
  {
    TBSYS_LOG(ERROR, "check fopen failed:file[%s]", temp_name);
  }
  else
  {
    ret = do_task(&file, task);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(ERROR, "check do task failed:task[%lu], ret[%d]", task.get_id(), ret);
    }
  }

  int err = OB_SUCCESS;
  if (ret == OB_SUCCESS)
  {
    err = file.close();
    if (OB_SUCCESS != err)
    {
      TBSYS_LOG(ERROR, "fclose failed:file[%s], err[%d], do_task ret[%d]", temp_name, err, ret);
      err = OB_ERROR;
    }
    else if (OB_SUCCESS == ret)
    {
      err = env_->rename(temp_name, file_name);
      if (OB_SUCCESS != err)
      {
        TBSYS_LOG(ERROR, "rename failed from temp to dest:temp[%s], dest[%s], err[%d]",
            temp_name, file_name, err);
        err = OB_ERROR;
      }
    }

    // remove failed temp file
    if (ret != OB_SUCCESS)
    {
      err = env_->rmfile(temp_name);
      if (OB_SUCCESS != err)
      {
        TBSYS_LOG(ERROR, "remove temp file failed:temp[%s], err[%d]", temp_name, err);
        err = OB_ERROR;
      }
    }
  }
  return (err | ret);
}

int TaskWorker::make_file_name(const TaskInfo &task, char file_name[], const int64_t len)
{
  int ret = OB_SUCCESS;
  char host_name[HOST_NAME_LEN];
  if (gethostname(host_name, HOST_NAME_LEN) != 0)
  {
    TBSYS_LOG(ERROR, "can't get host name, due to [%d]", errno);
    ret = OB_ERROR;
  }
  else
  {
    const TableConf *conf = NULL;
    string output_path = output_path_;
    int has_conf = param_->get_table_conf(task.get_table_name(), conf);
    if (has_conf != OB_SUCCESS)
    {
      TBSYS_LOG(DEBUG, "no table conf is specified");
    }
    else
    {
      TBSYS_LOG(DEBUG, "get table_conf ,conf=%s", conf->DebugString().c_str());

      if (!conf->output_path().empty())
      {
        output_path += "/";
        output_path += conf->output_path();
      }
    }

    int blen = snprintf(file_name, len, "%s/%ld_%.*s_%ld_%ld%s", output_path.c_str(), (int64_t)getpid(),
                       task.get_param().get_table_name().length(), task.get_param().get_table_name().ptr(),
                       task.get_token(), task.get_id(), host_name);
    if (blen <= 0)
    {
      TBSYS_LOG(ERROR, "file name too long");
      ret = OB_ERROR;
    }
  }

  return ret;
}

int TaskWorker::do_task(const TaskInfo & task, char file_name[], const int64_t len)
{
  int ret = OB_SUCCESS;
  if (NULL != output_path_)
  {
    ret = make_file_name(task, file_name, len);
    if (ret == OB_SUCCESS)
    {
      ret = do_task(file_name, task);
    }
  }
  else
  {
    TaskOutputFile console;
    ret = console.open();
    if (ret == OB_SUCCESS)
    {
      ret = do_task(&console, task);
    }
    else
    {
      TBSYS_LOG(ERROR, "can't open concole for output");
    }
  }
  return ret;
}

int TaskWorker::check_finish(const ObScanParam & param, const ObScanner & result, bool & finish)
{
  int64_t item_count = 0;
  int ret = result.get_is_req_fullfilled(finish, item_count);
  if (ret != OB_SUCCESS)
  {
    TBSYS_LOG(ERROR, "get is fullfilled failed:ret[%d]", ret);
  }
  else
  {
    TBSYS_LOG(INFO, "%s, Finished count=%ld", finish ? "Finished" : "Still has", item_count);
    INC_STAT(TASK_TOTAL_RECORD_COUNT, item_count); /* update row counts */
    ret = check_result(param, result);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(ERROR, "check result failed:ret[%d]", ret);
    }
  }
  return ret;
}

int TaskWorker::modify_param(const ObScanner & result, ModuleArena & allocator, ObNewRange & new_range, ObScanParam & param)
{
  ObRowkey last_rowkey;
  int ret = result.get_last_row_key(last_rowkey);
  if (ret != OB_SUCCESS)
  {
    TBSYS_LOG(ERROR, "get last row key failed:ret[%d]", ret);
  }
  else
  {
    const ObNewRange * range = param.get_range();
    if (NULL == range)
    {
      ret = OB_ERROR;
      TBSYS_LOG(ERROR, "get range failed:range[%p]", range);
    }
    else
    {
      ret = deep_copy_range(allocator, *range, new_range);
      if (ret != OB_SUCCESS)
      {
        TBSYS_LOG(WARN, "deep copy new range failed:ret[%d]", ret);
      }
    }
    if (OB_SUCCESS == ret)
    {
      new_range.border_flag_.unset_min_value();
      new_range.border_flag_.unset_inclusive_start();
      ret = new_range.end_key_.deep_copy(last_rowkey, allocator);
      if (ret != OB_SUCCESS)
      {
        TBSYS_LOG(WARN, "deep copy last rowkey failed:ret[%d]", ret);
      }
      else
      {
        TBSYS_LOG(INFO, "construct the next scan range:%s", to_cstring(new_range));
        param.set(param.get_table_id(), param.get_table_name(), new_range);
      }
    }
  }
  return ret;
}

int TaskWorker::check_result(const ObScanParam & param, const ObScanner & result)
{
  UNUSED(param);
  UNUSED(result);
  int ret = OB_SUCCESS;
  return ret;
}

int TaskWorker::output(TaskOutputFile * file, ObScanner & result) const
{
  int ret = OB_SUCCESS;
  if (NULL == file)
  {
    TBSYS_LOG(ERROR, "check output file failed:file[%p]", file);
    ret = OB_ERROR;
  }
  else if (!check_inner_stat())
  {
    TBSYS_LOG(ERROR, "%s", "check inner stat failed");
    ret = OB_ERROR;
  }
  else
  {
    TBSYS_LOG(INFO, "output the result");
    UNUSED(result);
    // TODO
    // ret = file->append(result);
  }
  return ret;
}


