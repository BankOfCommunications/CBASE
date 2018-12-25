#ifndef __OB_DATA_SET__
#define  __OB_DATA_SET__

#include "oceanbase_db.h"
#include "db_record_set.h"
#include <string>
#include <vector>

namespace oceanbase {
  namespace api {
    using namespace common;

    class ObDataSet {
      public:
        enum {
          DATASET_TABLET,
          DATASET_RANGE
        };

      public:
        ObDataSet(OceanbaseDb *db);

        int data_boundry_init(const ObRowkey &start_key, const ObRowkey &end_key, int64_t version = 0);

        int set_data_source(const TabletInfo &info, const std::string table,
                            const std::vector<std::string> &cols, const ObRowkey &start_key, 
                            const ObRowkey &end_key, int64_t version_ = 0);

        int set_data_source(const std::string table,
                            const std::vector<std::string> &cols, const ObRowkey &start_key, 
                            const ObRowkey &end_key, int64_t version_ = 0);
        void set_scan_limit(int64_t count) { db_->set_limit_info(0, count); }

        bool has_next();
        int get_record(DbRecord *&recp);

        int next();

        void set_inclusie_start(bool start) { inclusive_start_ = start; }

      private:
        int read_more(const ObRowkey &start_key, const ObRowkey &end_key, int64_t version);

      private:
        OceanbaseDb *db_;

        ObRowkey start_key_;
        ObRowkey end_key_;
        ObRowkey last_end_key_;

        DbRecordSet ds_;
        DbRecordSet::Iterator ds_itr_;

        TabletInfo tablet_info_;
        std::vector<std::string> columns_;

        std::string table_;
        bool inited_;
        bool read_data_;
        bool fullfilled_;
        bool has_more_;
        bool inclusive_start_;

        bool has_tablet_info_;

        int64_t version_;
    };
  }
}

#endif
