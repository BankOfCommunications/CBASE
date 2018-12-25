#ifndef OCEANBASE_ACCESS_COUNTER_H_
#define OCEANBASE_ACCESS_COUNTER_H_

namespace oceanbase
{
  namespace common
  {
    // no need thread safe using LRU for Washout
    class ObAccessCounter
    {
    public:
      ObAccessCounter(const int64_t timeout);
      virtual ~ObAccessCounter();
    protected:
      // access info struct
      struct ObAccessInfo
      {
        int64_t server_id_; // server id
        int64_t timestamp_; // last access timestamp
        int64_t counter_;   // counter for every access
      };
    public:
      // inc the counter info of server_id if timeout clear the data
      int inc(const int64_t server_id);

      // print the info about all peer acess counter info
      void print(void);
    private:
      // find the suitable place for update the peer_id counter
      // if not find right place washout the oldest item like lru
      int64_t find(const int64_t server_id, int64_t & old_server);
      // clear all the data
      void clear(const int64_t timestamp);
    private:
      const static int64_t MAX_COUNT = 256;
      int64_t timeout_;
      int64_t cur_count_;
      int64_t clear_timestamp_;
      //TODO using lru list will be faster by half
      ObAccessInfo data_[MAX_COUNT];
    };
  }
}


#endif // OCEANBASE_ACCESS_COUNTER_H_
