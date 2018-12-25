
/*
 *   (C) 2007-2010 Taobao Inc.
 *
 *
 *   Version: 0.1
 *
 *   Authors:
 *      qushan <qushan@taobao.com>
 *        - ob range class,
 *
 */
#ifndef OCEANBASE_COMMON_OB_RANGE_H_
#define OCEANBASE_COMMON_OB_RANGE_H_

#include <tbsys.h>
#include "ob_define.h"
#include "ob_string.h"
#include "ob_string_buf.h"

namespace oceanbase
{
  namespace common
  {
    class ObBorderFlag
    {
      public:
        NEED_SERIALIZE_AND_DESERIALIZE; //add zhaoqiong [Truncate Table]:20160318:b
      public:
        static const int8_t INCLUSIVE_START = 0x1;
        static const int8_t INCLUSIVE_END = 0x2;
        static const int8_t MIN_VALUE = 0x4;
        static const int8_t MAX_VALUE = 0x8;

      public:
        ObBorderFlag() : data_(0) {}
        ObBorderFlag(int8_t data) : data_(data) {} //add zhaoqiong [Truncate Table]:20160318:b
        virtual ~ObBorderFlag() {}

        inline void set_inclusive_start() { data_ |= INCLUSIVE_START; }

        inline void unset_inclusive_start() { data_ &= (~INCLUSIVE_START); }

        inline bool inclusive_start() const { return (data_ & INCLUSIVE_START) == INCLUSIVE_START; }

        inline void set_inclusive_end() { data_ |= INCLUSIVE_END; }

        inline void unset_inclusive_end() { data_ &= (~INCLUSIVE_END); }

        inline bool inclusive_end() const { return (data_ & INCLUSIVE_END) == INCLUSIVE_END; }

        inline void set_min_value() { data_ |= MIN_VALUE; }
        inline void unset_min_value() { data_ &= (~MIN_VALUE); }
        inline bool is_min_value() const { return (data_ & MIN_VALUE) == MIN_VALUE; }

        inline void set_max_value() { data_ |= MAX_VALUE; }
        inline void unset_max_value() { data_ &= (~MAX_VALUE); }
        inline bool is_max_value() const { return (data_ & MAX_VALUE) == MAX_VALUE; }

        inline void set_data(const int8_t data) { data_ = data; }
        inline int8_t get_data() const { return data_; }
      private:
        int8_t data_;
    };

    struct ObVersion
    {
      NEED_SERIALIZE_AND_DESERIALIZE; //add zhaoqiong [Truncate Table]:20160318:b
      ObVersion() : version_(0) {}
      ObVersion(int64_t version) : version_(version) {}
      union
      {
        int64_t version_;
        struct
        {
          int64_t major_           : 32; //int32_t mod zhaoqiong [Truncate Table]:20160318
          int64_t minor_           : 16; //int16_t mod zhaoqiong [Truncate Table]:20160318
          int16_t is_final_minor_  : 16;
        };
      };

      int64_t operator=(int64_t version)
      {
        version_ = version;
        return version_;
      }

      operator int64_t() const
      {
        return version_;
      }

      static int64_t get_version(int64_t major,int64_t minor,bool is_final_minor)
      {
        ObVersion v;
        v.major_          = static_cast<int32_t>(major);
        v.minor_          = static_cast<int16_t>(minor);
        v.is_final_minor_ = is_final_minor ? 1 : 0;
        return v.version_;
      }

      static int64_t get_major(int64_t version)
      {
        ObVersion v;
        v.version_ = version;
        return v.major_;
      }

      static int64_t get_minor(int64_t version)
      {
        ObVersion v;
        v.version_ = version;
        return v.minor_;
      }

      static bool is_final_minor(int64_t version)
      {
        ObVersion v;
        v.version_ = version;
        return v.is_final_minor_ != 0;
      }

      static int compare(int64_t l,int64_t r)
      {
        int ret = 0;
        ObVersion lv = l;
        ObVersion rv = r;

        //ignore is_final_minor
        if ((lv.major_ == rv.major_) && (lv.minor_ == rv.minor_))
        {
          ret = 0;
        }
        else if ((lv.major_ < rv.major_) ||
                 ((lv.major_ == rv.major_) && lv.minor_ < rv.minor_))
        {
          ret = -1;
        }
        else
        {
          ret = 1;
        }
        return ret;
      }

      int64_t to_string(char * buf, const int64_t buf_len) const
      {
        //mod zhaoqiong [Truncate Table]:20160318:b
        //int64_t pos = snprintf(buf, buf_len, "%d-%hd-%hd",
        int64_t pos = snprintf(buf, buf_len, "%ld-%ld-%hd",
                               major_, minor_, is_final_minor_);
        //mod:e
        return pos;
      }
    };

    class ObVersionProvider
    {
      public:
        virtual ~ObVersionProvider(){}
        virtual const ObVersion get_frozen_version() const = 0;
    };

    struct ObVersionRange
    {
      NEED_SERIALIZE_AND_DESERIALIZE; //add zhaoqiong [Truncate Table]:20160318:b
      ObBorderFlag border_flag_;
      ObVersion start_version_;
      ObVersion end_version_;

      // from MIN to MAX, complete set.
      inline bool is_whole_range() const
      {
        return (border_flag_.is_min_value()) && (border_flag_.is_max_value());
      }

      inline int64_t get_query_version() const
      {
        int64_t query_version = 0;

        /**
         * query_version = 0 means read the data of serving version
         * query_version > 0 means read the data of specified version
         * query_version = -1 means invalid version
         * start_version_ and end_version_ is 0, only read data of
         * serving version, not merge dynamic data
         */
        if (!border_flag_.is_max_value() && end_version_.major_ > 0)
        {
          if (border_flag_.inclusive_end())
          {
            query_version = end_version_.major_;
          }
          else
          {
            if (end_version_.major_ > start_version_.major_ + 1)
            {
              query_version = end_version_.major_;
            }
            else if (end_version_.major_ == start_version_.major_ + 1
                     && border_flag_.inclusive_start())
            {
              query_version = start_version_.major_;
            }
            else
            {
              query_version = -1;
            }
          }
        }

        return query_version;
      }

      int to_string(char* buf,int64_t buf_len) const
      {
        int ret        = OB_SUCCESS;
        const char* lb = NULL;
        const char* rb = NULL;
        if (buf != NULL && buf_len > 0)
        {
          if (border_flag_.is_min_value())
          {
            lb = "(MIN";
          }
          else if (border_flag_.inclusive_start())
          {
            lb = "[";
          }
          else
          {
            lb = "(";
          }

          if (border_flag_.is_max_value())
          {
            rb = "MAX)";
          }
          else if (border_flag_.inclusive_end())
          {
            rb = "]";
          }
          else
          {
            rb = ")";
          }

          int64_t len = 0;

          if (is_whole_range())
          {
            len = snprintf(buf,buf_len,"%s,%s",lb,rb);
          }
          else if (border_flag_.is_min_value())
          {
            //mod zhaoqiong [Truncate Table]:20160318:b
            //len = snprintf(buf,buf_len,"%s,%d-%hd-%hd%s",lb,
            len = snprintf(buf,buf_len,"%s,%ld-%ld-%hd%s",lb,
                           end_version_.major_,end_version_.minor_,end_version_.is_final_minor_,rb);
            //mod:e
          }
          else if (border_flag_.is_max_value())
          {
            //mod zhaoqiong [Truncate Table]:20160318:b
            //len = snprintf(buf,buf_len,"%s%d-%hd-%hd, %s",lb,
            len = snprintf(buf,buf_len,"%s%ld-%ld-%hd, %s",lb,
                           start_version_.major_,start_version_.minor_,start_version_.is_final_minor_,rb);
            //mod:e
          }
          else
          {
            //mod zhaoqiong [Truncate Table]:20160318:b
            //len = snprintf(buf,buf_len,"%s %d-%hd-%hd,%d-%hd-%hd %s",lb,
            len = snprintf(buf,buf_len,"%s %ld-%ld-%hd,%ld-%ld-%hd %s",lb,
                           start_version_.major_,start_version_.minor_,start_version_.is_final_minor_,
                           end_version_.major_,end_version_.minor_,end_version_.is_final_minor_,rb);
            //mod:e
          }
          if (len < 0 || len > buf_len)
          {
            ret = OB_SIZE_OVERFLOW;
          }
        }
        return ret;
      }
    };
  } // end namespace common
} // end namespace oceanbase

#endif //OCEANBASE_COMMON_OB_RANGE_H_
