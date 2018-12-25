/**
 * (C) 2007-2010 Taobao Inc.
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

#ifndef OCEANBASE_UPDATESERVER_OB_POS_LOG_READER_H_
#define OCEANBASE_UPDATESERVER_OB_POS_LOG_READER_H_
#include "ob_log_locator.h"
#include "ob_on_disk_log_locator.h"
#include "ob_located_log_reader.h"

namespace oceanbase
{
  namespace updateserver
  {
    class ObPosLogReader
    {
      public:
        ObPosLogReader();
        virtual ~ObPosLogReader();
        int init(const char* log_dir, const bool dio = true);
        // start_location的log_id_必须是有效的, file_id_和offset_如果无效，会被填充为正确值。
        virtual int get_log(const int64_t start_id, ObLogLocation& start_location, ObLogLocation& end_location,
                            char* buf, const int64_t len, int64_t& read_count);
      protected:
        bool is_inited() const;
	    //del wangjiahao [Paxos ups_replication_tmplog] 20150630 :b
        //private:
	    //del:e
        char log_dir_[OB_MAX_FILE_NAME_LENGTH];
        ObOnDiskLogLocator on_disk_log_locator_;
        ObLocatedLogReader located_log_reader_;
    };
	 //add wangjiahao [Paxos ups_replication_tmplog] 20150630 :b
    class ObRegionLogReader: public ObPosLogReader
    {
      public:
        ObRegionLogReader();
        ~ObRegionLogReader();
        //read log region on disk from start_id to end_id must be existed
        int get_log(const int64_t start_id, const int64_t end_id, char* buf, int64_t& read_count, int64_t max_data_len);
    };
	//add:e
  }; // end namespace updateserver
}; // end namespace oceanbase
#endif // OCEANBASE_UPDATESERVER_OB_POS_LOG_READER_H_
