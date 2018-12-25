#ifndef __TASK_STATS__
#define __TASK_STATS__

#include <vector>
#include "common/ob_define.h"
#include "common/utility.h"

struct TaskStatItem {
  int8_t stat_type;
  int64_t stat_value;
};

enum {
  TASK_SUCC_SCAN_COUNT,
  TASK_TOTAL_REQUEST_BYTES,
  TASK_TOTAL_REPONSE_BYTES,
  TASK_TOTAL_WRITE_BYTES,
  TASK_TOTAL_RECORD_COUNT,
  TASK_FAILED_SCAN_COUNT
};

class TaskStats {
  public:
    TaskStats() { }

    inline void inc_stat(int8_t stat_type, int64_t value) 
    {
      TaskStatItem *stat = get_stat(stat_type);
      if (stat == NULL) 
      {
        TaskStatItem new_stat;
        new_stat.stat_type = stat_type;
        new_stat.stat_value = value;
        stats_.push_back(new_stat);
      }
      else
      {
        stat->stat_value += value;
      }
    }

    inline void reset()
    {
      stats_.clear();
    }

    inline TaskStatItem *get_stat(int8_t stat_type) 
    {
      TaskStatItem *item = NULL;

      for(size_t i = 0;i < stats_.size(); i++) 
      {
        if (stats_[i].stat_type == stat_type) 
        {
          item = &(stats_[i]);
          break;
        }
      }

      return item;
    }

    void dump(int64_t task_id);

    NEED_SERIALIZE_AND_DESERIALIZE;
  private:
    std::vector<TaskStatItem> stats_;
};

#endif
