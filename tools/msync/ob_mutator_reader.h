/** (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * Authors:
 *   yuanqi <yuanqi.xhf@taobao.com>
 *     - some work details if you want
 */

#ifndef _OCEANBASE_MSYNC_OB_MUTATOR_READER_H_
#define _OCEANBASE_MSYNC_OB_MUTATOR_READER_H_
#include "common/ob_log_entry.h"
#include "common/ob_log_reader.h"
#include "common/ob_repeated_log_reader.h"
#include "common/ob_schema.h"
#include "updateserver/ob_ups_mutator.h"

using namespace oceanbase::common;
using namespace oceanbase::updateserver;

namespace oceanbase
{
  namespace msync
  {
    class ObMutatorReader
    {
    public:
    ObMutatorReader(): stop_(false), last_seq_(0), last_mutator_(NULL){}
      ~ObMutatorReader(){}
      int initialize(const char* schema, const char* log_dir, uint64_t file_start_id, uint64_t last_seq_id, int64_t wait_time);
      void stop()
      {
        stop_ = true;
      }
      int read(ObDataBuffer& buf, uint64_t& seq);
      int get_mutator(ObMutator*& mutator, uint64_t& seq, int64_t timeout);
      int consume_mutator();
    private:
      int read_(ObMutator& mutator, uint64_t& seq, int64_t size_limit);
      int get_mutator_(ObMutator*& mutator, uint64_t& seq, int64_t timeout);
      int read_single_mutator_(ObMutator*& mutator, uint64_t& seq, int64_t timeout);
      bool stop_;
      int64_t timeout_;
      int64_t wait_time_;
      ObRepeatedLogReader repeated_reader_;
      ObLogReader reader_;
      ObSchemaManagerV2 sch_mgr_;
      char mutator_buffer_[OB_MAX_LOG_BUFFER_SIZE];
      ObMutator accumulated_mutator_;
      uint64_t last_seq_;
      ObMutator* last_mutator_;
      ObMutator cur_mutator_; // use to calculate serialize size
      ObUpsMutator cur_ups_mutator_;
    };
  } // end namespace msync
} // end namespace oceanbase
#endif //_OCEANBASE_MSYNC_OB_MUTATOR_READER_H_

