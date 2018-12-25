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
#include "common/ob_define.h"
#include "common/utility.h"

namespace oceanbase
{
  namespace updateserver
  {
    struct ObWarnStat
    {
      int64_t max_value_limit_;
      int64_t stb_value_limit_;

      int64_t gt_stb_count_limit_; // greater than stable count limit
      int64_t window_count_;

      int64_t total_count_;
      int64_t gt_stb_count_;

      ObWarnStat()
      {
        max_value_limit_ = 0;
        stb_value_limit_ = 0;
        gt_stb_count_limit_ = 0;
        window_count_ = 0;
        total_count_ = 0;
        gt_stb_count_ = 0;
      };

#define DEF_SET_FUNC(name) \
      void set_##name(const int64_t name) \
      { \
        TBSYS_LOG(INFO, "%p set %s from %ld to %ld", this, #name, name##_, name); \
        name##_ = name;\
      };

      DEF_SET_FUNC(max_value_limit);
      DEF_SET_FUNC(stb_value_limit);
      DEF_SET_FUNC(gt_stb_count_limit);
      DEF_SET_FUNC(window_count);

      bool warn(const int64_t value)
      {
        bool bret = false;
        if (0 >= window_count_
            || value <= stb_value_limit_)
        {
          bret = false;
        }
        else if (value > max_value_limit_)
        {
          bret = true;
        }
        else
        {
          if (total_count_ >= window_count_)
          {
            total_count_ = 0;
            gt_stb_count_ = 0;
          }

          total_count_ += 1;
          if (value > stb_value_limit_)
          {
            gt_stb_count_ += 1;
          }

          if (gt_stb_count_ > gt_stb_count_limit_)
          {
            bret = true;
          }
        }
        return bret;
      };

      int64_t to_string(char* buffer, const int64_t length) const
      {
        int64_t pos = 0;
        common::databuff_printf(buffer, length, pos, "max_value_limit=%ld stb_value_limit=%ld "
                                "gt_stb_count_limit=%ld window_count=%ld "
                                "total_count=%ld gt_stb_count=%ld",
                                max_value_limit_, stb_value_limit_,
                                gt_stb_count_limit_, window_count_,
                                total_count_, gt_stb_count_);
        return pos;
      };
    };

    struct ObBatchEventStat
    {
      int64_t batch_count_;
      int64_t cur_end_;
      int64_t mvalue_;
      int64_t mstart_;
      int64_t mend_;
      void clear_mvalue();
      void add(const int64_t start_id, const int64_t end_id, const int64_t value);
      int serialize(char* buf, int64_t len, int64_t& pos) const;
      int deserialize(const char* buf, int64_t len, int64_t& pos);
    };

    struct ObClogStat
    {
      static const int64_t STRUCT_VERSION = 0;
      static const int64_t DEFAULT_DISK_WARN_US = 5000;
      static const int64_t DEFAULT_NET_WARN_US = 5000;
      ObClogStat(): disk_warn_us_(DEFAULT_DISK_WARN_US), net_warn_us_(DEFAULT_NET_WARN_US) {}
      ~ObClogStat() {}
      void set_disk_warn_us(int64_t us) { disk_warn_us_ = us; }
      void set_net_warn_us(int64_t us) { net_warn_us_ = us; }
      void add_disk_us(int64_t start_id, int64_t end_id, int64_t time);
      void add_net_us(int64_t start_id, int64_t end_id, int64_t time);
      void clear();
      int serialize(char* buf, int64_t len, int64_t& pos) const;
      int deserialize(const char* buf, int64_t len, int64_t& pos);
      int64_t to_string(char* buf, const int64_t len) const;
      int64_t disk_warn_us_;
      int64_t net_warn_us_;
      ObBatchEventStat disk_stat_;
      ObBatchEventStat net_stat_;
      ObBatchEventStat batch_stat_;
      ObWarnStat disk_warn_stat_;
      ObWarnStat net_warn_stat_;
    };
  }; // end namespace updateserver
}; // end namespace oceanbase
