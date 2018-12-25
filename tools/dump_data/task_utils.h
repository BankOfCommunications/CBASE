#ifndef __TASK_UTILS_H__
#define  __TASK_UTILS_H__

#include "common/ob_object.h"
#include "common/utility.h"
#include "common/data_buffer.h"
#include "common/ob_string.h"
#include "common/ob_common_param.h"
#include "common/ob_action_flag.h"
#include "task_table_conf.h"

#include <time.h>

namespace oceanbase {
  namespace tools {
    using namespace common;

    int ObDateTime2MySQLDate(int64_t ob_time, int time_type, char *outp, int size);

    int serialize_cell(oceanbase::common::ObCellInfo *cell, oceanbase::common::ObDataBuffer &buff);

    int append_delima(oceanbase::common::ObDataBuffer &buff, const RecordDelima &delima);

    int append_end_rec(oceanbase::common::ObDataBuffer &buff, const RecordDelima &rec_delima);

    int task_utils_init();

    int append_int64(int64_t value, ObDataBuffer &buffer);

    int append_double(double value, ObDataBuffer &buffer);

    int append_int8(int8_t value, ObDataBuffer &buffer);

    int append_str(const char *str, int64_t len, ObDataBuffer &buffer);
  }
}

#endif   /* ----- #ifndef db_utils_INC  ----- */
