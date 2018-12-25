#ifndef OCEANBASE_COMMON_SCANNER_LOADER_H_
#define OCEANBASE_COMMON_SCANNER_LOADER_H_
#include <stdint.h>
#include "common/ob_scanner.h"
#include "common/ob_string_buf.h"
#include "common/ob_string.h"
#include "common/ob_scan_param.h"

using namespace oceanbase::common;
//using namespace oceanbase::mergeserver;

namespace oceanbase
{
  namespace mergeserver 
  {
    namespace test
    {
      class ObScannerLoader
      {
        public:
          static const int32_t OB_MAX_IP_SIZE = 64;
          ObScannerLoader();

        public:
          int load(const char *config_file_name, const char *section_name);
          int get_decoded_scanner(ObScanner & param);

          void dump_config();

        private:
          int load_from_config(const char *section_name);
          int load_string(char* dest, const int32_t size,
              const char* section, const char* name, bool require=true);
          int build_cell_info(int table_id, const ObRowkey &row_key, char *buf, ObScanner &param);
          void auto_set_val(ObObj &val, char *buf);
          void dump_param(ObScanner &param);
        private:
          ObStringBuf strbuf;
          int table_id_;
          ObString table_name_;
          ObRowkey range_start_;
          ObRowkey range_end_;
          bool range_start_inclusive_;
          bool range_start_min_;
          bool range_end_inclusive_;
          bool range_end_max_;
          bool is_fullfilled_;
          int64_t fullfilled_item_num_;
          int64_t data_version_;
          char data_file_name_[4096];
          // if new config item added, don't forget add it into dump_config

          int64_t actual_fullfilled_item_num_;
          bool config_loaded_;
          tbsys::CConfig config_;
      };
    } /* test */ 
  } /* mergeserver */
} /* oceanbase */

#endif /* end of include guard: OCEANBASE_MERGESERVER_MERGESERVER_PARAMS_H_ */
