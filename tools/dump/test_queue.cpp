#include "db_queue.h"
#include <string>
#include <set>
#include <algorithm>
#include <gtest/gtest.h>
#include <vector>
#include "common/utility.h"

using namespace oceanbase::common;
using namespace tbsys;

struct QueueMsg {
  int msg_;
};

class TestProducer : public QueueProducer<QueueMsg> {
  public:
    TestProducer() { id_ = 0; }

    virtual int produce(QueueMsg &obj) {
      int ret = 0;

      if (id_ < size_)
        obj.msg_ = id_++;
      else
        ret = ComsumerQueue<QueueMsg>::QUEUE_QUITING;
      /* 
      int ret = 0;
      if (!elements.empty()) {
        obj = elements.front();
        elements.erase(elements.begin());
      } else {
        ret = ComsumerQueue<QueueMsg>::QUEUE_QUITING;
        TBSYS_LOG(DEBUG, "producer is quited");
      }
      */

      return ret;
    }

    std::vector<QueueMsg> elements;

    void init(size_t size) {
      size_ = size;

      return;
      for(size_t i = 0;i < size; i++) {
        QueueMsg msg;
        msg.msg_ = i;
        elements.push_back(msg);
      }

      //shuffle
      for(size_t i = 0;i < size / 2; i++) {
        int64_t idx1 = rand() % size;
        int64_t idx2 = rand() % size;

        if (idx2 != idx1) {
          QueueMsg tmp;

          tmp = elements[idx1];
          elements[idx1] = elements[idx2];
          elements[idx2] = tmp;
        }
      }
    }

  private:
    size_t id_;
    size_t size_;
};

//template <class T>
class ConcurrentVector {
  public:
//    void push_back(T &obj) {
    void push_back(QueueMsg &obj) {
      tbsys::CThreadGuard guard(&mutex_);
      vec_.insert(obj.msg_);
    }

    std::set<int> vec_;
//    std::vector<T> vec_;
  private:
    tbsys::CThreadMutex mutex_;
};

class TestComsumer : public QueueComsumer<QueueMsg> {
  public:
    TestComsumer(ConcurrentVector *vec) {
      vec_ = vec;
    }

    virtual int comsume(QueueMsg &obj) {
//      TBSYS_LOG(INFO, "queue ms is %d", obj.msg_);
      vec_->push_back(obj);
      return 0;
    }

  private:
//    ConcurrentVector<QueueMsg> *vec_;
    ConcurrentVector *vec_;
};

TEST(DB,test_queue)
{
  TestProducer producer;
  producer.init(1000000);

//  ConcurrentVector<QueueMsg> vec;
  ConcurrentVector vec;
  TestComsumer comsumer(&vec);

  int64_t start_time = CTimeUtil::getTime();
  ComsumerQueue<QueueMsg> queue(&producer, &comsumer);
  queue.produce_and_comsume(1, 10);
  TBSYS_LOG(INFO, "Total time elapsed = %ld", CTimeUtil::getTime() - start_time);
  queue.dispose();

//  std::sort(vec.vec_.begin(), vec.vec_.end());
  ASSERT_EQ(vec.vec_.size(), 1000000);

  int i = 0;
  for (std::set<int>::iterator itr = vec.vec_.begin(); 
       itr != vec.vec_.end(); itr++, i++) {
    EXPECT_EQ(i, *itr);
  }
}

int main(int argc, char *argv[])
{
  ::testing::InitGoogleTest(&argc,argv);  
  return RUN_ALL_TESTS();
}

