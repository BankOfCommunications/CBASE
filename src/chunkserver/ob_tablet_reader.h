/**
 * (C) 2010-2011 Taobao Inc.
 *
 *  This program is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU General Public License
 *  version 2 as published by the Free Software Foundation.
 *
 *  ob_tablet_reader.h for migrate
 *
 *  Authors:
 *    zian <yunliang.shi@alipay.com>
 *
 */
#ifndef OB_CHUNKSERVER_OB_TABLET_READER_H_
#define OB_CHUNKSERVER_OB_TABLET_READER_H_
#include "ob_tablet_manager.h"
#include "common/file_directory_utils.h"
#include "ob_chunk_server_main.h"
#include "common/ob_server.h"
#include "common/ob_data_source_desc.h"

namespace oceanbase
{
  namespace chunkserver
  {

    class ObTabletReader
    {
      public:
        virtual ~ObTabletReader() {}
        virtual int open() = 0;
        virtual int close() = 0;
        virtual int read_next(common::ObDataBuffer& buffer) = 0;
        virtual int read_next_row(const common::ObRow* &row) = 0;
        virtual bool has_new_data() const = 0;
    };


    /**
     * buffer reader for remote reader 
     */
    class ObTabletBufferReader : public ObTabletReader
    {
      public:
        ObTabletBufferReader(common::ObDataBuffer &buffer, int64_t &pos);
        void reset();
        void reuse() {has_new_buffer_ = true; has_new_data_ = true;}

        virtual int open() {return OB_SUCCESS;}
        virtual int close() {return OB_SUCCESS;}
        virtual int read_next(common::ObDataBuffer& buffer);
        virtual int read_next_row(const common::ObRow* &row){UNUSED(row); return OB_NOT_SUPPORTED;}
        virtual bool has_new_data() const{return has_new_data_;};

      private:
        common::ObDataBuffer& buffer_;
        int64_t &pos_;

        bool has_new_data_;
        bool has_new_buffer_;
    };


    /**
     * remote reader of the sstable
     */
    class ObTabletRemoteReader : public ObTabletReader
    {
      public:
        ObTabletRemoteReader(common::ObDataBuffer &out_buffer, ObTabletManager &manager);

        int prepare(
            ObDataSourceDesc &desc,
            int16_t &remote_sstable_version,
            int64_t &sequence_num,
            int64_t &row_checksum,
            int64_t &sstable_type);

        virtual int open();
        virtual int close();
        virtual int read_next(common::ObDataBuffer& buffer);
        virtual int read_next_row(const common::ObRow* &row){UNUSED(row); return OB_NOT_SUPPORTED;}
        virtual bool has_new_data() const{return has_new_data_;};

      private:
        common::ObDataBuffer& out_buffer_;
        ObTabletManager &manager_;
        int64_t pos_;
        ObTabletBufferReader buffer_reader_;
        bool has_new_data_;
        int64_t session_id_;
        ObServer src_server_;
    };


    /**
     * scan the sstable or direct copy file
     */
    class ObTabletFileReader : public ObTabletReader
    {
      public:
        ObTabletFileReader(){reset();}
        ~ObTabletFileReader(){}
        void reset();

        int prepare(const ObNewRange& range, const int64_t tablet_version, const int16_t remote_sstable_version);
        virtual int open();
        virtual int read_next(common::ObDataBuffer& buffer);
        virtual int read_next_row(const common::ObRow* &row){UNUSED(row); return OB_NOT_SUPPORTED;}
        virtual bool has_new_data()const {return has_new_data_;}
        virtual int close();

        inline void set_tablet_manager(ObTabletManager* manager){manager_ = manager;}
        int get_sstable_version(int16_t &sstable_version);
        int get_sequence_num(int64_t &sequence_num);
        int get_sstable_type(int64_t &sstable_type);
        int get_row_checksum(int64_t &row_checksum);

      private:
        int read_scanner_data(common::ObDataBuffer& buffer);
        int read_file_data(common::ObDataBuffer& buffer);

      private:
        int64_t file_offset_;
        ObTablet* tablet_;
        FileComponent::DirectFileReader reader_;
        int64_t req_tablet_version_;
        int16_t remote_sstable_version_;
        bool has_new_data_;
        ObTabletManager* manager_;
    };
  }
}
#endif
