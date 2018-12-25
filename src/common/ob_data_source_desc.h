/*
 * (C) 2007-2013 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 *
 * Authors:
 *  yongle.xh@alipay.com
 *
 */

#ifndef OCEANBASE_DATA_SOURCE_DESC_H_
#define OCEANBASE_DATA_SOURCE_DESC_H_

#include "ob_range2.h"
#include "ob_server.h"
#include "ob_string.h"
namespace oceanbase
{
  namespace common
  {

    static const char* const proxy_uri_prefix = "proxy://";
    static const char* const oceanbase_uri_prefix = "oceanbase://";

    struct ObDataSourceDesc
    {
      enum ObDataSourceType
      {
        OCEANBASE_INTERNAL = 0, // from same cluster
        OCEANBASE_OUT = 1, // from other cluster, uri=oceanbase://rs_ip:rs_port
        DATA_SOURCE_PROXY = 2, // from data source proxy, uri=proxy://data_source_name://path/to/sstable/dir
        UNKNOWN = 3, // UNKNOWN value
      };

      ObDataSourceDesc();

      template <typename Allocator>
        int deserialize(Allocator& allocator, const char* buf, const int64_t data_len, int64_t& pos);

      void reset();
      const char* get_type() const;
      int64_t to_string(char* buffer, const int64_t length) const;

      //data
      ObDataSourceType type_;
      ObNewRange range_;
      ObServer src_server_; // cs server or proxy server
      ObServer dst_server_;
      int64_t sstable_version_;
      int64_t tablet_version_;
      bool keep_source_; // only used in OCEANBASE_INTERNAL
      ObString uri_; // rs_ip:rs_port or data_source_name://path/to/tablet/dir

      ObObj start_key_objs_[OB_MAX_ROWKEY_COLUMN_NUMBER];
      ObObj end_key_objs_[OB_MAX_ROWKEY_COLUMN_NUMBER];
      NEED_SERIALIZE_AND_DESERIALIZE;
    };

    template <typename Allocator>
      int ObDataSourceDesc::deserialize(Allocator& allocator, const char* buf, const int64_t data_len, int64_t& pos)
      {
        int ret = OB_SUCCESS;
        int32_t type = -1;
        ObString tmp_uri;

        if (NULL == buf)
        {
          ret = OB_DESERIALIZE_ERROR;
        }

        if (OB_SUCCESS == ret
            && OB_SUCCESS == (ret = serialization::decode_i32(buf, data_len, pos, &type))
            && OB_SUCCESS == (ret = range_.deserialize(allocator, buf, data_len, pos))
            && OB_SUCCESS == (ret = src_server_.deserialize(buf, data_len, pos))
            && OB_SUCCESS == (ret = dst_server_.deserialize(buf, data_len, pos))
            && OB_SUCCESS == (ret = serialization::decode_i64(buf, data_len, pos, &sstable_version_))
            && OB_SUCCESS == (ret = serialization::decode_i64(buf, data_len, pos, &tablet_version_))
            && OB_SUCCESS == (ret = serialization::decode_bool(buf, data_len, pos, &keep_source_))
            && OB_SUCCESS == (ret = tmp_uri.deserialize(buf, data_len, pos)))
        {
          // success
          type_ = static_cast<ObDataSourceType>(type);
        }
        else
        {
          TBSYS_LOG(WARN, "failed to deserialize data source desc, ret=%d", ret);
        }

        if (OB_SUCCESS == ret && ret != ob_write_string(allocator, tmp_uri, uri_))
        {
          TBSYS_LOG(WARN, "failed to copy uri, ret=%d", ret);
        }

        return ret;
      }
  }
}
#endif // OCEANBASE_DATA_SOURCE_DESC_H_
