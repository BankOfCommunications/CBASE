/**
  * add wenghaixing [secondary index static_index_build.exceptional_handle] 20150403
  * @This is a black list for building partitional or global Index
  * when a cs build a index failed,then push this tablet / range in BlackList and send list to other cs
  * another cs recieved the list to redo index mission, if failed, send list to third cs
  * in the end of phase, cs found that all replication are failed,then the index create failed.
  *
  * WARNING:
  * Now index building will only handle 3 replication of data;
  * that is failed in 3 replicate, this index build failed;
  */
#ifndef OB_BLACK_LIST_H
#define OB_BLACK_LIST_H

#include "common/ob_range2.h"
#include "common/ob_server.h"
#include "common/ob_define.h"
#include "common/ob_array.h"
#include "common/page_arena.h"
#include "location/ob_tablet_location_list.h"
#include <tbsys.h>
#include <Mutex.h>
#include <Monitor.h>

//using namespace oceanbase::common;
namespace oceanbase
{
  namespace common
  {
    //mod liumz, [replica_num 3->6]20150105:b
    //static const int64_t OB_INDEX_HANDLE_REP = 3;
    static const int64_t OB_INDEX_HANDLE_REP = OB_MAX_COPY_COUNT;
    //mod:e
    class BlackList
    {
      public:
        BlackList();
        ~BlackList();
        int init();
        int add_in_black_list(ObNewRange &range, ObServer &server);
        bool is_all_repli_failed();
        void get_range(ObNewRange &range);
        void set_range(ObNewRange &range);
        int get_server(int i, ObServer &server);
        int8_t get_server_count();
        void set_server_unserved(ObServer server);
        bool is_server_unserved(const ObServer &server);//add liumz
        int write_list(ObNewRange &range, ObTabletLocationList &list);
        int next_replic_server(ObServer &server);
        void set_wok_send(int8_t value = 1){wok_send_ = value;}
        int8_t get_wok_send(){return wok_send_;}
        NEED_SERIALIZE_AND_DESERIALIZE;
      private:
        int set_rowkey_obj_array(char* buf, const int64_t buf_len, int64_t & pos, const ObObj* array, const int64_t size);
        int set_int_obj_value(char * buf, const int64_t buf_len, int64_t & pos, const int64_t value);
        int get_rowkey_compatible(const char* buf, const int64_t buf_len, int64_t & pos,
            ObObj* array, int64_t& size) ;
      private:
        ObNewRange              range_;
        ObObj                   array_[OB_MAX_ROWKEY_COLUMN_NUMBER * 2];
        int8_t                  server_count_;
      //int8_t                  phase1_;
        ObServer                server_[OB_INDEX_HANDLE_REP];
        int8_t                  unserved_[OB_INDEX_HANDLE_REP];
        int8_t                  wok_send_;//local server flag
    };

    class BlackListArray
    {
      public:
        BlackListArray();
        ~BlackListArray();
        int init();
        int push(BlackList &list);
        int64_t get_list_count();
        /*only provide an interface,not used until now*/
        BlackList& get_black_list(int64_t i, int &err);
        int get_next_black_list(BlackList &list, const ObServer &server);
        int check_range_in_list(ObNewRange &range, bool &in, int64_t &index);
        bool is_list_array_bleach();
        void reset();

      private:
        pthread_mutex_t mutex_;
        ObArray<BlackList>      list_array_;
        ObArray<int8_t>         list_bleach_;
        CharArena               arena_;
        BlackList               error_;

    };

  } //end chunkserver
} //end oceanbase

#endif // OB_BLACK_LIST_H
