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
#ifndef __OB_COMMON_OB_SWITCH_H__
#define __OB_COMMON_OB_SWITCH_H__
#include "ob_define.h"
#include "serialization.h"

namespace oceanbase
{
  namespace common
  {
    class ObSwitch
    {
      public:
        // switch有三种状态，on/off/wait_off, 使用场景如下:
        //  1. 一个工作线程在循环中调用is_wait_off()/check_off(can_stop)函数检查自己是否允许工作
        //  2. 另一个线程调用wait_on()/wait_off()等待工作线程开始和结束(实际上只有结束需要等待，wait_on()调用不用等待)。
        enum {
          SWITCH_ON = 0,
          SWITCH_REQ_OFF = 1,
          SWITCH_OFF = 3,
        };
        enum {
          STATE_MASK = 3,
        };
        ObSwitch(): seq_(0) {}
        ~ObSwitch() {}
        int64_t get_seq() const {return seq_; }
        int serialize(char* buf, const int64_t len, int64_t& pos) const {
          return serialization::encode_i64(buf, len, pos, seq_);
        }
        int deserialize(const char* buf, const int64_t len, int64_t& pos) {
          return serialization::decode_i64(buf, len, pos, (int64_t*)&seq_);
        }
      private:
        // seq_>>2 表示turn on的次数
        volatile int64_t seq_;
      public:
        // wait_on()/wait_off()的调用需要保证是串行的。
        bool is_req_off()
        {
          return SWITCH_REQ_OFF == get_state();
        }
        // 工作线程在每次循环最开始调用check_off(), 如果返回false，就表明需要停下工作
        bool check_off(const bool can_stop_now)
        {
          if (is_req_off() && can_stop_now)
            turn_off();
          return is_off();
        }
        // 控制线程要确保工作线程停下，只需要反复调用wait_off()，直到返回true
        bool wait_off()
        {
          if (!is_off())
            req_off();
          return is_off();
        }
        // 控制线程要确保工作线程开始，调用wait_on()即可
        bool wait_on()
        {
          if (!is_on())
            turn_on();
          return is_on();
        }
      protected:
        int64_t get_state()
        {
          return seq_ & STATE_MASK;
        }

        bool is_on()
        {
          return SWITCH_ON == get_state();
        }
        bool is_off()
        {
          return SWITCH_OFF == get_state();
        }
        bool turn_on_()
        {
          int64_t seq = seq_;
          return __sync_bool_compare_and_swap(&seq_, seq, (seq&~STATE_MASK) + STATE_MASK + 1);
        }
        bool turn_off_()
        {
          int64_t seq = seq_;
          return __sync_bool_compare_and_swap(&seq_, seq, seq | SWITCH_OFF);
        }
        bool req_off_()
        {
          int64_t seq = seq_;
          return __sync_bool_compare_and_swap(&seq_, seq, seq | SWITCH_REQ_OFF);
        }
        bool turn_on()
        {
          while(!turn_on_())
            ;
          return true;
        }
        bool turn_off()
        {
          while(!turn_off_())
            ;
          return true;
        }
        bool req_off()
        {
          while(!req_off_())
            ;
          return true;
        }
    };
  }; // end namespace common
}; // end namespace oceanbase

#endif /* __OB_COMMON_OB_SWITCH_H__ */
