#ifndef OCEANBASE_COMMON_SCAN_PARAM_LOADER_H_
#define OCEANBASE_COMMON_SCAN_PARAM_LOADER_H_
#include <stdint.h>
#include "common/ob_scan_param.h"
#include "common/ob_string_buf.h"
#include "common/ob_string.h"
#include "common/ob_scan_param.h"

using namespace oceanbase::common;

namespace oceanbase
{
  namespace mergeserver 
  {
    namespace test
    {
      class ObScanParamLoader
      {
        public:
          static const int32_t OB_MAX_IP_SIZE = 64;
          ObScanParamLoader();

        public:
          int load(const char *config_file_name, const char *section_name);
          int get_org_scan_param(ObScanParam & param);
          int decoded_scan_param(ObScanParam &org_param, ObScanParam & param);

          void dump_config();

        private:
          int load_from_config(const char *section_name);
          int load_string(char* dest, const int32_t size,
              const char* section, const char* name, bool require=true);

          void dump_param(ObScanParam &param);
        private:
          ObStringBuf strbuf;
          int table_id_;
          ObString table_name_;
          ObRowkey range_start_;
          ObRowkey range_end_;
          ObString having_cond_;
          ObString where_cond_;
          bool range_start_inclusive_;
          bool range_start_min_;
          bool range_end_inclusive_;
          bool range_end_max_;
          int scan_direction_;
          int limit_offset_;
          int limit_count_;
          ObString column_id_[OB_MAX_COLUMN_NUMBER];
          int column_is_return_[OB_MAX_COLUMN_NUMBER];
          int column_id_count_;
          ObString complex_column_id_[OB_MAX_COLUMN_NUMBER];
          int complex_column_is_return_[OB_MAX_COLUMN_NUMBER];
          int complex_column_id_count_;
          ObString groupby_column_id_[OB_MAX_COLUMN_NUMBER];
          int groupby_column_is_return_[OB_MAX_COLUMN_NUMBER];
          int groupby_column_id_count_;
          ObString agg_column_id_[OB_MAX_COLUMN_NUMBER];
          int agg_column_is_return_[OB_MAX_COLUMN_NUMBER];
          int agg_column_op_[OB_MAX_COLUMN_NUMBER];
          ObString agg_column_as_name_[OB_MAX_COLUMN_NUMBER];
          int agg_column_id_count_;
          ObString groupby_complex_column_id_[OB_MAX_COLUMN_NUMBER];
          int groupby_complex_column_is_return_[OB_MAX_COLUMN_NUMBER];
          int groupby_complex_column_id_count_;
          ObString orderby_column_id_[OB_MAX_COLUMN_NUMBER];
          int orderby_column_order_[OB_MAX_COLUMN_NUMBER];
          int orderby_column_id_count_;
          // if new config item added, don't forget add it into dump_config

          bool config_loaded_;
          tbsys::CConfig config_;
      };
    } /* test */ 
  } /* mergeserver */
} /* oceanbase */

#endif /* end of include guard: OCEANBASE_MERGESERVER_MERGESERVER_PARAMS_H_ */
