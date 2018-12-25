/**
 * (C) 2010-2011 Taobao Inc.
 *
 *  This program is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU General Public License
 *  version 2 as published by the Free Software Foundation.
 *
 *  ob_tablet_writer.h  
 *
 *  Authors:
 *    zian <yunliang.shi@alipay.com>
 *
 */

#ifndef OB_CHUNKSERVER_OB_TABLET_WRITER_H_
#define OB_CHUNKSERVER_OB_TABLET_WRITER_H_
#include "ob_tablet_manager.h"
#include "common/file_directory_utils.h"
#include "ob_chunk_server_main.h"
#include "common/ob_server.h"
#include "ob_tablet_reader.h"

namespace oceanbase
{
  namespace chunkserver
  {
    class ObSSTableWriterUtil 
    {
      public:
        ObSSTableWriterUtil(ObTabletManager& manager) : manager_(manager) {}
        int gen_sstable_file_location(const int32_t disk_no, 
            sstable::ObSSTableId& sstable_id, char* path, const int64_t path_size);
        int cleanup_uncomplete_sstable_files(const sstable::ObSSTableId& sstable_id);

      private:
        ObTabletManager &manager_;
    };

    class ObTabletWriter
    {
      public:
        virtual ~ObTabletWriter() {}
        virtual int open() = 0;
        virtual int close() = 0;
        virtual int write(const common::ObDataBuffer& buffer) = 0;
        virtual int write_row(const common::ObRow& row) = 0;
    };

    /**
     * direct file copy
     */
    class ObTabletFileWriter : public ObTabletWriter
    {
      public:
        ObTabletFileWriter();
        ~ObTabletFileWriter(){}
        void reset(){is_empty_ = true; sstable_id_.sstable_file_id_ = 0; add_used_space_ = false; disk_no_ = 0;}
        int prepare(const ObNewRange& range, const int64_t tablet_version, const int64_t tablet_seq_num, const uint64_t row_checksum, ObTabletManager* manager);
        int cleanup();
        virtual int open();
        virtual int close();
        virtual int write(const common::ObDataBuffer& buffer);
        virtual int write_row(const common::ObRow& row){ UNUSED(row); return OB_NOT_SUPPORTED;}
      private:
        int build_new_tablet(ObTablet* &tablet);
        int finish_sstable();

        common::ObFileAppender appender_;
        sstable::ObSSTableId sstable_id_;
        ObNewRange range_;
        ObTabletExtendInfo extend_info_;
        int64_t tablet_version_;
        bool is_empty_;
        bool add_used_space_;
        int32_t disk_no_;
        ObTabletManager* manager_;
    };

    int pipe_row(ObTabletManager& manager, ObTabletReader& reader, ObTabletWriter & writer);
    int pipe_buffer(ObTabletManager& manager, ObTabletReader& reader, ObTabletWriter & writer);
  }
}
#endif
