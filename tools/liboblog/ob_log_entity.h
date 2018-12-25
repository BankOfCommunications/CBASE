////===================================================================
 //
 // ob_log_entity.h liboblog / Oceanbase
 //
 // Copyright (C) 2013 Alipay.com, Inc.
 //
 // Created on 2013-05-23 by Yubai (yubai.lk@alipay.com) 
 //
 // -------------------------------------------------------------------
 //
 // Description
 // 
 //
 // -------------------------------------------------------------------
 // 
 // Change Log
 //
////====================================================================

#ifndef  OCEANBASE_LIBOBLOG_ENTITY_H_
#define  OCEANBASE_LIBOBLOG_ENTITY_H_

#include "liboblog.h"
#include "ob_log_formator.h"
#include "ob_log_utils.h"

namespace oceanbase
{
  namespace liboblog
  {
    class ObLogEntity : public IObLog
    {
      static uint64_t total_observer_num;
      static uint64_t total_db_partition_num;
      static const uint64_t SEQ_MAP_SIZE = 65536;
      public:
        ObLogEntity();
        ~ObLogEntity();
      public:
        int init(IObLogSpec *log_spec, const std::vector<uint64_t> &partitions);
        void destroy();
        int next_record(IBinlogRecord **record, const int64_t timeout_us);
        void release_record(IBinlogRecord *record);

      private:
        void print_partition_info_(const IObLogFormator *formator, const std::vector<uint64_t> &partitions) const;

      private:
        bool inited_;
        ObLogSpec *log_spec_;
        IObLogFormator *log_formator_;
        std::vector<uint64_t> partitions_;
        ObLogSeqMap seq_map_;
    };
  }
}

#endif //OCEANBASE_LIBOBLOG_ENTITY_H_

