#include <sys/time.h>
#include <stdio.h>
#include "liboblog/liboblog.h"

using namespace oceanbase::liboblog;

typedef int64_t (*str_to_int_pt) (const char *str);

int64_t my_atoll(const char *str)
{
  return ::atoll(str);
}

int64_t split_string(const char *str, int64_t *values, str_to_int_pt *callbacks, const int64_t size, const char d)
{
  int64_t idx = 0;
  if (NULL != str)
  {
    static const int64_t BUFFER_SIZE = 1024;
    char buffer[BUFFER_SIZE];
    snprintf(buffer, BUFFER_SIZE, "%s", str);
    char *iter = buffer;
    char *deli = NULL;
    char *end = iter + strlen(iter);

    while (iter < end
          && idx < size)
    {
      deli = strchr(iter, d);
      if (NULL != deli)
      {
        *deli = '\0';
      }

      values[idx] = callbacks[idx](iter);
      if (0 > values[idx])
      {
        break;
      }

      idx += 1;
      if (NULL == deli)
      {
        break;
      }
      iter = deli + 1;
    }
  }
  return idx;
}

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

int64_t t_cnt = 0;
int64_t r_cnt = 0;
int64_t last_t_cnt = 0;
int64_t last_r_cnt = 0;
int64_t last_time = 0;

void print_binlog_record(IBinlogRecord *br, const bool print)
{
  //uint64_t ck1 = br->getCheckpoint1();
  //uint64_t ck2 = br->getCheckpoint2();
  //uint64_t ck = ((ck1 << 32) & 0xffffffff00000000) + (ck2 & 0x00000000ffffffff);
  //fprintf(stderr, "%lu\n", ck);
  if (NULL != br)
  {
    if (EBEGIN == br->recordType())
    {
      t_cnt += 1;
if (!print) return;
      fprintf(stderr, "Begin\n");
    }
    else if (ECOMMIT == br->recordType())
    {
      if (0 == t_cnt % 10000)
      {
        int64_t cur_time = get_cur_microseconds_time();
        double tps = (double)(t_cnt - last_t_cnt) * 1000000.0 / (double)(cur_time - last_time);
        double rps = (double)(r_cnt - last_r_cnt) * 1000000.0 / (double)(cur_time - last_time);
        last_t_cnt = t_cnt;
        last_r_cnt = r_cnt;
        last_time = cur_time;
        fprintf(stderr, "tps=%0.2lf rps=%0.2lf\n", tps, rps);
      }
if (!print) return;
      fprintf(stderr, "Commit\n");
    }
    else
    {
      r_cnt += 1;
//fprintf(stderr, "%ld\n", br->getTimestamp());
if (!print) return;
      br->toString();
      fprintf(stderr, "   DML[%d] db_name=[%s] table_name=[%s] binlog_record=[%p]",
              br->recordType(), br->dbname(), br->getTableMeta()->getName(), br);
      std::vector<std::string*>::const_iterator ic = br->newCols().begin();
      std::vector<std::string*>::const_iterator io = br->oldCols().begin();
      if (EDELETE == br->recordType())
      {
        fprintf(stderr, "      Delete Row");
      }
      fprintf(stderr, "\n");
      fprintf(stderr, "   New");
      while (true)
      {
        if (ic == br->newCols().end())
        {
          break;
        }
        fprintf(stderr, "      [%s]", *ic ? (*ic)->c_str() : NULL);
        ic++;
      }
      fprintf(stderr, "\n");
      fprintf(stderr, "   Old");
      while (true)
      {
        if (io == br->oldCols().end())
        {
          break;
        }
        fprintf(stderr, "      [%s]", *io ? (*io)->c_str() : NULL);
        io++;
      }
      fprintf(stderr, "\n");
    }
  }
}

int main(int argc, char **argv)
{
  ObLogSpecFactory g_log_spec_factory;
  IObLogSpec *g_log_spec = g_log_spec_factory.construct_log_spec();

  if (4 > argc)
  {
    fprintf(stderr, "sample [config_file] [checkpoint] [partitions] [print_result]\n\n");
    fprintf(stderr, "eg. sample ./liboblog.conf 1 0,1,2,3 yes\n\n");
    exit(-1);
  }

  // config
  const char *config = argv[1];

  // checkpoint
  uint64_t last_checkpoint = atoll(argv[2]);
  g_log_spec->init(config, last_checkpoint);

  // partitions
  IObLog *log = g_log_spec_factory.construct_log();
  std::vector<uint64_t> partitions;
  const int64_t ARRAY_SIZE = 1024;
  int64_t values[ARRAY_SIZE];
  str_to_int_pt callbacks[ARRAY_SIZE];
  for (int64_t i = 0; i < ARRAY_SIZE; i++)
  {
    callbacks[i] = my_atoll;
  }
  int64_t ret_size = split_string(argv[3], values, callbacks, ARRAY_SIZE, ',');
  for (int64_t i = 0; i < ret_size; i++)
  {
    partitions.push_back(values[i]);
    fprintf(stdout, "Observe %ld\n", values[i]);
  }
  log->init(g_log_spec, partitions);

  // print result
  bool print = (0 == strcmp(argv[4], "yes"));

  ////////////////////////////////////////////////////////////////////////////////////////////////////

  g_log_spec->launch();

  IBinlogRecord *bin_log_record = NULL;
  int64_t timeout = 10000L * 1000000L; //10s

  last_time = get_cur_microseconds_time();
  while (true)
  {
    log->next_record(&bin_log_record, timeout);
    if (NULL == bin_log_record)
    {
      fprintf(stderr, "Total End\n");
      break;
    }
    print_binlog_record(bin_log_record, print);
    log->release_record(bin_log_record);
  }

  log->destroy();
  g_log_spec_factory.deconstruct(log);
  g_log_spec->destroy();
  g_log_spec_factory.deconstruct(g_log_spec);

  return 0;
}

