/**
 * (C) 2010-2011 Taobao Inc.
 *
 *  This program is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU General Public License
 *  version 2 as published by the Free Software Foundation.
 * 
 *  ob_proxy_reader.h
 *
 *  Authors:
 *    zian <yunliang.shi@alipay.com>
 *
 */
#include "common/data_buffer.h"
#include "ob_yunti_proxy.h"
#include "common/ob_range2.h"
#ifndef OCEANBASE_PROXYREADER_H_
#define OCEANBASE_PROXYREADER_H_

namespace oceanbase
{
  namespace proxyserver
  {
    class ProxyReader
    {
      public:
        ProxyReader(){}
        virtual void reset();
        virtual ~ProxyReader();
        virtual int read_next(common::ObDataBuffer& buffer) = 0;
        virtual bool has_new_data(){return !(internal_buf_.get_remain() > 0 && !has_remain_data());}
        virtual int close() = 0;
      protected:
        int prepare_buffer();
        bool has_remain_data(){return internal_buf_.get_position() - pos_ > 0;}
        int fill_data(common::ObDataBuffer& buffer);

        common::ObDataBuffer internal_buf_;
        int32_t pos_;
        int64_t open_file_time_us_;
        int64_t total_read_size_;
        int64_t total_read_time_us_;
    };

    class YuntiFileReader : public ProxyReader
    {
      public:
        YuntiFileReader(){}
        ~YuntiFileReader(){}

        int prepare(YuntiProxy* proxy, common::ObNewRange &range,
            int64_t req_tablet_version, int16_t remote_sstable_version, const char* sstable_dir);
        int read_next(common::ObDataBuffer& buffer);
        int get_sstable_version(const char* sstable_dir, int16_t &sstable_version);
        int get_tablet_version(int64_t &tablet_version);
        int get_sstable_type(int64_t &sstable_type);
        int close();

      private:
        YuntiProxy* proxy_;
        FILE* fp_;
    };

    class YuntiRangeReader : public ProxyReader
    {
      public:
        YuntiRangeReader(){}
        ~YuntiRangeReader(){}

        void reset();
        int prepare(YuntiProxy* proxy, const char* table_name, const char* sstable_dir); 
        bool has_new_data();
        int read_next(common::ObDataBuffer& buffer);
        int close(){return OB_SUCCESS;}

      private:
        common::ObArray<common::ObNewRange*> range_list_;
        int64_t offset_;
    };
  }
}
#endif
