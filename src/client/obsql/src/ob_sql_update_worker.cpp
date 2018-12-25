#include "common/ob_define.h"   // UNUSED
#include "ob_sql_update_worker.h"
#include "tblog.h"
#include "ob_sql_util.h"
#include "ob_sql_global.h"
#include "ob_sql_ms_select.h"
#include "ob_sql_cluster_config.h"
#include "ob_sql_conn_acquire.h"    // print_connection_info
#include "ob_sql_util.h"
#include <mysql/mysql.h>
#include <string.h>
#include <tbsys.h>                  // CTimeUtil

// Mutex and condition variable used to wake up update thread
pthread_mutex_t g_update_mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t g_update_cond = PTHREAD_COND_INITIALIZER;
bool g_update_flag = false;

pthread_t update_task;
bool update_work_started = false;

void wakeup_update_worker()
{
  TBSYS_LOG(DEBUG, "wakeup update worker begin, g_update_flag=%s", g_update_flag ? "true" : "false");
  pthread_mutex_lock(&g_update_mutex);
  g_update_flag = true;
  pthread_cond_signal(&g_update_cond);
  pthread_mutex_unlock(&g_update_mutex);
  TBSYS_LOG(INFO, "wakeup update worker, g_update_flag=%s", g_update_flag ? "true" : "false");
}

void clear_update_flag()
{
  TBSYS_LOG(DEBUG, "clear update flag begin, g_update_flag=%s", g_update_flag ? "true" : "false");
  pthread_mutex_lock(&g_update_mutex);
  g_update_flag = false;
  pthread_mutex_unlock(&g_update_mutex);
  TBSYS_LOG(INFO, "clear update flag, g_update_flag=%s", g_update_flag ? "true" : "false");
}

static void *update_global_config(void *arg)
{
  UNUSED(arg);

  int ret = OB_SQL_SUCCESS;
  struct timespec wait_time;
  int64_t last_update_time_us = 0;

  while (g_inited)
  {
    last_update_time_us = tbsys::CTimeUtil::getTime();

    int64_t wait_time_usec = (OB_SQL_SUCCESS != ret) ? OB_SQL_UPDATE_FAIL_WAIT_INTERVAL_USEC
                                                     : OB_SQL_UPDATE_INTERVAL_USEC;

    int64_t end_time = wait_time_usec + last_update_time_us;

    // Compute wait time
    wait_time.tv_sec = wait_time_usec / 1000000L;
    wait_time.tv_nsec = (wait_time_usec % 1000000L) * 1000L;

    pthread_mutex_lock(&g_update_mutex);
    while (! g_update_flag && tbsys::CTimeUtil::getTime() < end_time)
    {
      pthread_cond_timedwait(&g_update_cond, &g_update_mutex, &wait_time);
    }
    pthread_mutex_unlock(&g_update_mutex);

    if (! g_inited)
    {
      OB_SQL_LOG(INFO, "update worker stop on destroying");
      break;
    }

    // NOTE: 当有黑名单时，强制打印连接信息
    if (0 < g_ms_black_list.size_)
    {
      print_connection_info(&g_group_ds);
    }

    OB_SQL_LOG(INFO, "update worker startup, g_update_flag=%s, update interval=%ld us",
        g_update_flag ? "true" : "false",
        tbsys::CTimeUtil::getTime() - last_update_time_us);

    ret = get_ob_config();
    if (OB_SQL_SUCCESS == ret)
    {
      ret = do_update();
      if (OB_SQL_SUCCESS != ret)
      {
        OB_SQL_LOG(WARN, "update global config failed");
        dump_config(g_config_update, "UPDATE");
      }
    }
    else
    {
      OB_SQL_LOG(WARN, "get_ob_config fail, can not update group data source");
    }

    // NOTE: Print connection information after updating
    print_connection_info(&g_group_ds);

    // 每次更新完，无论成功与否，都强制清除g_update_flag，避免大量重复无效工作
    clear_update_flag();

    OB_SQL_LOG(INFO, "update worker end, g_update_flag=%s, ret=%d", g_update_flag ? "true" : "false", ret);
  }

  if (NULL != g_func_set.real_mysql_thread_end)
  {
    OB_SQL_LOG(INFO, "update work call mysql_thread_end before exit");
    g_func_set.real_mysql_thread_end();
  }

  OB_SQL_LOG(INFO, "update work exit, g_inited=%s", g_inited ? "Y" : "N");
  pthread_exit(NULL);
  return NULL;
}

int start_update_worker()
{
  int ret = OB_SQL_SUCCESS;

  ret = get_ob_config();
  if (OB_SQL_SUCCESS == ret)
  {
    ret = do_update();
    if (OB_SQL_SUCCESS == ret)
    {
      ret = pthread_create(&update_task, NULL, update_global_config, NULL);
      if (OB_SQL_SUCCESS != ret)
      {
        OB_SQL_LOG(ERROR, "start config update worker failed");
        ret = OB_SQL_ERROR;
      }
      else
      {
        OB_SQL_LOG(INFO, "start config update worker");
        update_work_started = true;
      }
    }
    else
    {
      OB_SQL_LOG(ERROR, "update global config failed");
      dump_config(g_config_update, "UPDATE");
    }
  }
  else
  {
    OB_SQL_LOG(ERROR, "get config from oceanbse failed");
  }

  return ret;
}

void stop_update_worker()
{
  if (update_work_started)
  {
    wakeup_update_worker();
    pthread_join(update_task, NULL);
    update_work_started = false;

    OB_SQL_LOG(INFO, "stop update work success");
  }
}
