
#include "client_simulator.h"

using namespace oceanbase::common;

void sig_handler(int)
{
  if (NULL != GI::instance().runnable())
    GI::instance().runnable()->stop();
}

int main(int argc, char* argv[])
{
  int ret = 0;

  ob_init_memory_pool();

  signal(SIGTERM, sig_handler);
  signal(SIGINT, sig_handler);

  Param& param = GI::instance().param();
  int64_t elapsed_time = 0;

  ret = param.parse_cmd_line(argc, argv);

  if (0 == ret)
  {
    TBSYS_LOGGER.setFileName(param.log_name.ptr(), true);
    TBSYS_LOGGER.setLogLevel(param.log_level);
  }

  if (0 == ret)
  {
    ret = GI::instance().init();
  }

  if (0 == ret)
  {
    int64_t start_time = tbsys::CTimeUtil::getTime();
    if (NULL != GI::instance().runnable())
    {
      GI::instance().runnable()->start();
      GI::instance().runnable()->wait();
    }
    else
    {
      TBSYS_LOG(ERROR, "Runnable is NULL");
    }
    elapsed_time = tbsys::CTimeUtil::getTime() - start_time;
  }

  if (0 == ret)
  {
    StatCollector &stat = GI::instance().stat();

#define STAT_HEAD "\n--- %s request %s:%d statistics ---\n"
#define STAT_FORMAT "elapsed time %lds, " \
                    "%ld request transmitted, " \
                    "%ld successful, " \
                    "%%%f request failed\n" \
                    "average response time %ldus, " \
                    "maximum response time %ldus, " \
                    "minimum response time %ldus\n\n"

    double failed_percent = 0;
    int64_t average_response_time = 0;
    if (0 != stat.get_sum_num())
      failed_percent = static_cast<double>(stat.get_sum_failed_num())
        / (stat.get_sum_num() + stat.get_sum_retry_num()) * 100;
    if (0 != stat.get_sum_succ_num())
      average_response_time = stat.get_sum_total_time() / stat.get_sum_succ_num();
    fprintf(stderr, STAT_HEAD, param.str_tr_type(param.tr_type), param.ip, param.port);
    fprintf(stderr, STAT_FORMAT, elapsed_time / 1000000,
                                 stat.get_sum_num() + stat.get_sum_retry_num(),
                                 stat.get_sum_succ_num(),
                                 failed_percent,
                                 average_response_time,
                                 stat.get_max_rt(),
                                 stat.get_min_rt());
    TBSYS_LOG(INFO, STAT_HEAD, param.str_tr_type(param.tr_type), param.ip, param.port);
    TBSYS_LOG(INFO, STAT_FORMAT, elapsed_time / 1000000,
                                 stat.get_sum_num() + stat.get_sum_retry_num(),
                                 stat.get_sum_succ_num(),
                                 failed_percent,
                                 average_response_time,
                                 stat.get_max_rt(),
                                 stat.get_min_rt());
  }

  return ret;
}

