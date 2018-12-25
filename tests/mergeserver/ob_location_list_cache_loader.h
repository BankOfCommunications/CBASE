#ifndef OCEANBASE_COMMON_LOCATION_LIST_CACHE_LOADER_H_
#define OCEANBASE_COMMON_LOCATION_LIST_CACHE_LOADER_H_
#include <stdint.h>
#include "common/ob_new_scanner.h"
#include "common/ob_string_buf.h"
#include "common/ob_string.h"
#include "common/location/ob_tablet_location_list.h"

using namespace oceanbase::common;

namespace oceanbase
{
  namespace common
  {
    class ObTabletLocationCache;
  };
  namespace mergeserver 
  {
    class ObLocationListCacheLoader
    {
      public:
        static const int32_t OB_MAX_IP_SIZE = 64;
        ObLocationListCacheLoader();
 
      public:
        int load(const char *config_file_name, const char *section_name);
        int get_decoded_location_list_cache(ObTabletLocationCache & cache);

        void dump_config();

      private:
        int load_from_config(const char *section_name);
        int load_string(char* dest, const int32_t size,
            const char* section, const char* name, bool require=true);
      private:
        ObStringBuf strbuf;
        static const int64_t max_count = 10240;
        int table_id_[max_count];
        ObObj range_start_obj_[max_count];
        ObObj range_end_obj_[max_count];
        ObString range_start_[max_count];
        ObString range_end_[max_count];
        ObString server_list_[max_count];
        // if new config item added, don't forget add it into dump_config
        int64_t range_num_;
        bool config_loaded_;
        tbsys::CConfig config_;
        char   fake_max_row_key_buf_[256];
        ObString fake_max_row_key_;
    };
  } /* mergeserver */
} /* oceanbase */

#endif /* end of include guard: OCEANBASE_MERGESERVER_MERGESERVER_PARAMS_H_ */
