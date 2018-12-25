#ifndef OB_MERGER_SERVER_COUNTER_H_
#define OB_MERGER_SERVER_COUNTER_H_

#include "common/ob_server.h"
#include "common/hash/ob_hashmap.h"

namespace oceanbase
{
  namespace mergeserver
  {
    class ObMergerServerCounter
    {
    public:
      ObMergerServerCounter();
      virtual ~ObMergerServerCounter();
    public:
      int init(const int64_t bucket, const int64_t reset_interval);
      int inc(const common::ObServer & server, const int64_t num);
      int64_t get(const common::ObServer & server) const;
      common::hash::ObHashMap<int32_t, int64_t> & get_counter_map();
      void dump(void) const;
    protected:
      int reset(void);
    private:
      void check_reset(void);
    public:
      static const int64_t INIT_SERVER_COUNT = 128;
      static const int64_t DEFAULT_INTERVAL = 10 * 60 * 1000 * 1000L;
    private:
      static const int64_t MAX_ADDR_LEN = 128;
      int64_t reset_interval_;
      int64_t last_reset_timestamp_;
      common::hash::ObHashMap<int32_t, int64_t> counter_map_;
    };
  }
}

#endif //OB_MERGER_SERVER_COUNTER_H_
