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
namespace oceanbase
{
  namespace common
  {
    typedef int64_t v4si __attribute__ ((vector_size(32)));
    inline int64_t v4si_max(v4si x_) __attribute__((always_inline));
    inline int64_t v4si_max(v4si x_)
    {
      int64_t* x = (int64_t*)&x_;
      int64_t idx1 = x[0] > x[1]? 0: 1;
      int64_t idx2 = x[2] > x[3]? 2: 3;
      return x[idx1] > x[idx2]? idx1: idx2;
    }
    inline int64_t v4si_sum(v4si x_) __attribute__((always_inline));
    inline int64_t v4si_sum(v4si x_)
    {
      int64_t* x = (int64_t*)&x_;
      return x[0] + x[1] + x[2] + x[3];
    }
    inline v4si v4si_gt0(v4si x_) __attribute__((always_inline));
    inline v4si v4si_gt0(v4si x_)
    {
      int64_t* x = (int64_t*)&x_;
      v4si is_gt0 = {x[0] > 0? -1 : 0, x[1] > 0? -1: 0, x[2] > 0? -1: 0, x[3] > 0? -1: 0};
      return is_gt0;
    }
    class ObPriorityScheduler
    {
      public:
        ObPriorityScheduler(){
          v4si quota = {1, 1, 1, 1};
          v4si debt = {0, 0, 0, 0};
          quota_ = quota;
          debt_ = debt;
          last_selected_idx_ = -1;
        }
        ~ObPriorityScheduler(){}
      public:
        void set_quota(v4si quota) __attribute__((always_inline)) { quota_ = quota; }
        int64_t get() {
          last_selected_idx_ = v4si_max(debt_);
          return last_selected_idx_;
        }
        void reset() { v4si zero = {0, 0, 0, 0}; debt_ = zero; }
        void update(int64_t idx, int64_t consume, v4si queue_len) __attribute__((always_inline)) {
          v4si ratio = quota_ * v4si_gt0(queue_len);  // ratio is negative
          int64_t ratio_sum = v4si_sum(ratio);
          if (idx >= 0 && last_selected_idx_ != idx)
          {
            reset();
          }
          if (ratio_sum < 0)
          {
            v4si ratio_sumv = {ratio_sum, ratio_sum, ratio_sum, ratio_sum};
            v4si consumev = {consume, consume, consume, consume};
            debt_ += ratio * consumev/ratio_sumv;
          }
          if (idx >= 0)
          {
            ((int64_t*)&debt_)[idx] -= consume;
          }
        }
        v4si quota_;
        v4si debt_;
        int64_t last_selected_idx_;
    };
  }; // end namespace common
}; // end namespace oceanbase
