#include <sys/time.h>
#include <stdio.h>
#include <unistd.h>
#include <sys/syscall.h>
#include "liboblog/liboblog.h"

#define GETTID() syscall(__NR_gettid)
using namespace oceanbase::liboblog;

inline int64_t tv_to_microseconds(const timeval & tp)
{
  return (((int64_t) tp.tv_sec) * 1000000 + (int64_t) tp.tv_usec);
}

inline int64_t get_cur_microseconds_time(void)
{
  struct timeval tp;
  gettimeofday(&tp, NULL);
  return tv_to_microseconds(tp);
}

volatile int64_t t_cnt = 0;
volatile int64_t r_cnt = 0;

void print_binlog_record(IBinlogRecord *br)
{
  if (NULL != br)
  {
    if (EBEGIN == br->recordType())
    {
      ATOMIC_ADD(&t_cnt, 1);
      return;
      fprintf(stdout, "Begin\n");
    }
    else if (ECOMMIT == br->recordType())
    {
      return;
      fprintf(stdout, "Commit\n");
    }
    else
    {
      ATOMIC_ADD(&r_cnt, 1);
      return;
      fprintf(stdout, "   DML[%d] db_name=[%s] table_name=[%s]\n",
              br->recordType(), br->dbname(), br->getTableMeta()->getName());
      //std::vector<std::string*>::const_iterator ip = br->colPKs().begin();
      std::vector<std::string*>::const_iterator ic = br->newCols().begin();
      //std::vector<std::string*>::const_iterator in = br->colNames().begin();
      //for (int64_t i = 0; ip != br->colPKs().end(); ip++, i++)
      //{
      //  fprintf(stdout, "      pk[%ld]=[%s]\n", i, (*ip)->c_str());
      //}
      if (EDELETE == br->recordType())
      {
        fprintf(stdout, "      Delete Row\n");
      }
      else
      {
        while (true)
        {
          //fprintf(stdout, "      %s=[%s]\n", (*in)->c_str(), (*ic)->c_str());
          fprintf(stdout, "      %s\n", (*ic)->c_str());
          ic++;
          //in++;
          if (ic == br->newCols().end())
          {
            break;
          }
        }
      }
    }
  }
}

void *thread_func(void *data)
{
  IObLog *log = (IObLog*)data;

  IBinlogRecord *bin_log_record = NULL;
  int64_t timeout = 10 * 1000000; //1s

  while (true)
  {
    log->next_record(&bin_log_record, timeout);
    if (NULL == bin_log_record)
    {
      fprintf(stderr, "Total End\n");
      break;
    }
    print_binlog_record(bin_log_record);
    log->release_record(bin_log_record);
  }

  log->destroy();

  return NULL;
}

int main()
{
  ObLogSpecFactory g_log_spec_factory;
  IObLogSpec *g_log_spec = g_log_spec_factory.construct_log_spec();

  const char *config = "./liboblog.conf";
  //uint64_t last_checkpoint = 42663523;
  //uint64_t last_checkpoint = 42536290;
  uint64_t last_checkpoint = 1227618;
  g_log_spec->init(config, last_checkpoint);

  static const int64_t THREAD_NUM = 8;
  IObLog *log[THREAD_NUM];
  for (int64_t i = 0; i < THREAD_NUM; i++)
  {
    std::vector<uint64_t> partitions;
    for (int64_t j = 0; j < 16 / THREAD_NUM; j++)
    partitions.push_back(i * 16 / THREAD_NUM + j);
    log[i] = g_log_spec_factory.construct_log();
    log[i]->init(g_log_spec, partitions);
  }

  g_log_spec->launch();
  pthread_t pd[THREAD_NUM];
  for (int64_t i = 0; i < THREAD_NUM; i++)
  {
    pthread_create(&pd[i], NULL, thread_func, log[i]);
  }
  int64_t last_t_cnt = 0;
  int64_t last_r_cnt = 0;
  int64_t last_time = 0;
  while (true)
  {
    usleep(1000000);
    int64_t cur_time = get_cur_microseconds_time();
    double tps = (float)(t_cnt - last_t_cnt) / (float)(cur_time - last_time) * 1.0 * 1000000;
    double rps = (float)(r_cnt - last_r_cnt) / (float)(cur_time - last_time) * 1.0 * 1000000;
    last_t_cnt = t_cnt;
    last_r_cnt = r_cnt;
    last_time = cur_time;
    fprintf(stdout, "tps=%0.2lf rps=%0.2lf\n", tps, rps);
  }
  for (int64_t i = 0; i < THREAD_NUM; i++)
  {
    pthread_join(pd[i], NULL);
  }

  for (int64_t i = 0; i < THREAD_NUM; i++)
  {
    g_log_spec_factory.deconstruct(log[i]);
  }
  g_log_spec->destroy();
  g_log_spec_factory.deconstruct(g_log_spec);

  return 0;
}

