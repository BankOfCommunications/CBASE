#ifndef OCEANBASE_OBSQL_SERVER_STATS_H
#define OCEANBASE_OBSQL_SERVER_STATS_H

#include <set>
#include <vector>
#include <string>
#include "common/ob_define.h"
#include "common/ob_statistics.h"
#include "stats.h"
#include "client_rpc.h"


namespace oceanbase 
{ 
  namespace obsql
  {
    struct Present
    {
      public:
        enum 
        {
          PerSecond = 1,
          Ratio,
          ShowCurrent,
        };
        static const int32_t SERVER_COUNT = 5;
        struct Item
        {
          std::string name;
          int32_t width; // width of showing console
          int32_t calc_type; // need divide by interval?
          Item(const std::string &n, const int32_t w, const int32_t stat)
            : name(n), width(w), calc_type(stat) {}
        };

        typedef std::vector<Item> ServerInfo;
        const ServerInfo & get_server_info(const int32_t server_type) const;

      public:
        Present();
        void init();

      private:
        ServerInfo server_info_[SERVER_COUNT];
    };

    // dataserver Access Stat. Info
    class ObServerStats : public Stats
    {
      public:
        struct Store
        {
          oceanbase::common::ObStatManager current;
          oceanbase::common::ObStatManager prev;
          oceanbase::common::ObStatManager diff;
        };

      public:
        explicit ObServerStats(ObClientServerStub &stub, const int64_t server_type) 
          : rpc_stub_(stub), show_date_(1)
        {
          store_.current.set_server_type(server_type);
          store_.prev.set_server_type(server_type);
          store_.diff.set_server_type(server_type);
        }
        virtual ~ObServerStats() {}
        void add_index_filter(const uint32_t index) { index_filter_.push_back(index); }
        void add_table_filter(const uint64_t table) { table_filter_.insert(table); }
        void set_show_date(const int32_t date) { show_date_ = date; }
        void clear_table_filter() { table_filter_.clear(); }
        void clear_index_filter() { index_filter_.clear(); }

      public:
        virtual int32_t output(const int32_t interval);
        virtual int32_t init();

      protected:
        virtual int32_t calc() ;
        virtual int32_t save() ;
        virtual int32_t refresh(const oceanbase::common::ObServer *remote_server) ;

      private:
        void output_header();
        int  calc_hit_ratio(oceanbase::common::ObStat &stat_item, 
            const int ratio, const int hit, const int miss);
        int calc_div_value(oceanbase::common::ObStat &stat_item, 
            const int div, const int time, const int count);
        int32_t print_item(const Present::ServerInfo & server_info, 
                           oceanbase::common::ObStatManager::const_iterator it, 
                           const uint32_t index, 
                           const int32_t interval) const;
      protected:
        ObClientServerStub &rpc_stub_;
        std::vector<uint32_t> index_filter_;
        std::set<uint64_t> table_filter_;
        int32_t show_date_;
        Store store_;
        Present present_;
    };


  }
}


#endif //OCEANBASE_OBSQL_SERVER_STATS_H

