/*
 * =====================================================================================
 *
 *       Filename:  tablet_stats.cpp
 *
 *    Description:  
 *
 *        Version:  1.0
 *        Created:  07/18/2011 10:51:22 AM
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  yushun.swh
 *        Company:  
 *
 * =====================================================================================
 */
#include <iostream>
#include <string>
#include <set>
#include <vector>
#include "db_utils.h"
#include "oceanbase_db.h"
#include "db_log_monitor.h"
#include "db_dumper_config.h"
#include "common/utility.h"
#include "common/file_utils.h"
#include "common/ob_string.h"
#include "db_parse_log.h"

using namespace oceanbase::api;
using namespace oceanbase::common;
using namespace std;
using namespace tbsys;


class tablet_stats {
  public:
    tablet_stats(): hit_times(0) { }

    string end_key_buff;
    string table;
    int64_t record_count;
    int64_t size;
    int64_t hit_times;

    ObString get_end_key() {
      ObString end_key;
      end_key.assign_ptr(const_cast<char*>(end_key_buff.data()), static_cast<int32_t>(end_key_buff.length()));
      return end_key;
    }

    bool operator()(const tablet_stats lfs, const tablet_stats &rhs) {
      if (lfs.table == rhs.table)
        return lfs.end_key_buff < rhs.end_key_buff;
      else 
        return lfs.table < rhs.table;
    }
};

struct TabletKey {
  string table;
  string rowkey;
};

struct TabletValue {
  int64_t hit_times;
  TabletInfo tablet;
};

struct TabletKeyLesser {
  bool operator()(const TabletKey &l, const TabletKey &r) {
    if (l.table == r.table)
      return l.rowkey < r.rowkey;
    else
      return l.table < r.table;
  }
};

void print_rowkey(const ObString &rowkey, string &str, string table, int64_t times)
{
  char tablet_buf[256];
  char rowkey_buf[256];

  int len = hex_to_str(str.data(), static_cast<int32_t>(str.length()), tablet_buf, 256);
  tablet_buf[2 * len] = 0;

  len = hex_to_str(rowkey.ptr(), rowkey.length(), rowkey_buf, 256);
  rowkey_buf[2 * len] = 0;

  TBSYS_LOG(INFO, "table=%s, tablet.endkey=%s, times=%ld, rowkey=%s", table.c_str(), tablet_buf, times, rowkey_buf);
}

class TabletStatser : public RowkeyHandler {
  public:
    TabletStatser() :count_(0), rotate_per_log_(false), count_update_(false), db(NULL), schema_(NULL) { }
    ~TabletStatser() { if (db != NULL) delete db; if (schema_ != NULL) delete schema_; }

    virtual int process_rowkey(const ObCellInfo &cell, int op, uint64_t timestamp) {
      int ret = OB_SUCCESS;
      if (count_update_) {
        map<int64_t, int64_t>::iterator itr = tab_counts_.find(cell.table_id_);
        if (itr != tab_counts_.end()) {
          tab_counts_[cell.table_id_]++;
        } else {
          tab_counts_[cell.table_id_] = 1;
        }
      } else {
        ret = process_stats(cell, op, timestamp);
      }

      return ret;
    }

    int process_stats(const ObCellInfo &cell, int op, uint64_t timestamp) {
      int ret = OB_SUCCESS;
      DbTableConfig *cfg = NULL;
      UNUSED(op);
      UNUSED(timestamp);

      ret = DUMP_CONFIG->get_table_config(cell.table_id_, cfg);
      if (ret == OB_SUCCESS) {
        string &table = cfg->table();
        string rowkey;

        rowkey.assign(cell.row_key_.ptr(), cell.row_key_.length());
        if (force_get_) {
          do_force_search(table, cell.row_key_);
        } else {
          search_stats(table, rowkey);
        }
      }

      return OB_SUCCESS;
    }

    void do_force_search(std::string &table, const ObString &rowkey) {
      TabletInfo tablet;
      TabletKey key;

      key.table = table;
      int ret = fetch_tablet_info(table, rowkey, tablet);
      if (ret != OB_SUCCESS) {
        TBSYS_LOG(ERROR, "can't fetch_tablet_info, table:%s", table.c_str());
        return;
      }

      key.rowkey.assign(tablet.get_end_key().ptr(), tablet.get_end_key().length());
      int times = 0;

      map<TabletKey, TabletValue>::iterator itr = stats.find(key);
      if (itr != stats.end()) {
        itr->second.hit_times++;

        times = static_cast<int32_t>(itr->second.hit_times);
      } else {
        TabletValue value;
        value.tablet = tablet;
        value.hit_times = 1;
        stats[key] = value;

        times = 1;
        TBSYS_LOG(ERROR, "should not goes here");
      }
    }

    bool is_virtual_tablet(const TabletKey &key) {
       map<TabletKey, TabletValue, TabletKeyLesser>::iterator itr = stats_virtual.find(key);
       return itr != stats_virtual.end();
    }

    void print_update_count() {
      char file_name[1024];
      char date_buf[MAX_TIME_LEN];
      bool use_schema = false;
      const char *tab_name = NULL;

      int ret = update_schema();
      if (ret == OB_SUCCESS) {
        use_schema = true;
      } else {
        TBSYS_LOG(ERROR, "exiting");
        exit(1);
      }

      get_current_date(date_buf, MAX_TIME_LEN);
      snprintf(file_name, 1024, "STATS_%s.txt", date_buf);
      FILE *file = fopen(file_name, "w");
      if (file == NULL) {
        TBSYS_LOG(ERROR, "can't create output file, name:%s", file_name);
        return;
      }
      fprintf(file, "%-25s%s\n", "[table id]", "[accesses]");
      map<int64_t, int64_t>::iterator itr = tab_counts_.begin();
      while (itr != tab_counts_.end()) {
        if (use_schema) {
          tab_name = schema_->get_table_schema(itr->first)->get_table_name();
          fprintf(file, "%-25s\t%ld\n", tab_name, itr->second);
        } else {
          fprintf(file, "%ld\t%ld\n", itr->first, itr->second);
        }

        itr++;
      }

      fclose(file);
    }

    void print_result() {
      char file_name[1024];
      char date_buf[MAX_TIME_LEN];
      FileUtils file;

      get_current_date(date_buf, MAX_TIME_LEN);
      snprintf(file_name, 1024, "STATS_%s.txt", date_buf);
      int fd = file.open(file_name, O_TRUNC | O_RDWR | O_CREAT, 0660);
      if (fd < 0) {
        TBSYS_LOG(ERROR, "can't create output file, name:%s, ret=%d", file_name, fd);
        return;
      }

      char *buf = file_name;
      map<TabletKey, TabletValue, TabletKeyLesser>::iterator itr = stats.begin();
      while (itr != stats.end()) {
        char rowkey_buf[1024];
        int len = hex_to_str(itr->first.rowkey.data(), static_cast<int32_t>(itr->first.rowkey.length()), rowkey_buf, 1024);
        rowkey_buf[2 * len] = '\0';

        string tablet_type;
        bool virtual_tablet = is_virtual_tablet(itr->first);
        if (virtual_tablet) {
          tablet_type = "virtual";
        } else {
          tablet_type = "real";
        }

        //table rowkey record_count size hit_times
        len = snprintf(buf, 1024, "%s\t%s\t%ld\t%ld\t%ld\t%s\n", itr->first.table.c_str(), rowkey_buf,
                       itr->second.tablet.record_count_, itr->second.tablet.ocuppy_size_,  
                       itr->second.hit_times, tablet_type.c_str());
        if (file.write(buf, len, true) < 0) {
          TBSYS_LOG(ERROR, "can't write file,%d", len);
        }

        itr++;
      }

      close(fd);
    }

    int fetch_tablet_info(string &table, const ObString &rowkey, TabletInfo &tablet_info)
    {
      int ret = OB_SUCCESS;
      TabletSliceLocation loc;

      ret = db->search_tablet_cache(table, rowkey, tablet_info);
      if (ret != OB_SUCCESS) {
        TBSYS_LOG(INFO, "Table %s not find in tablet cache, do root server get", table.c_str());

        ret = db->get_ms_location(rowkey, table);
        if (ret == OB_SUCCESS)
          ret = db->search_tablet_cache(table, rowkey, tablet_info);
        else {
          TBSYS_LOG(ERROR, "get_ms_location faild , retcode:[%d]", ret);
        }
      }

      if (ret == OB_SUCCESS) {
        ret = tablet_info.get_one_avail_slice(loc, rowkey);
        if (ret != OB_SUCCESS)
          TBSYS_LOG(ERROR, "can't get avail_sice");
      }

      return ret;
    }

    void search_stats(string &table, std::string &rowkey) {
      TabletKey key;
      key.table = table;
      key.rowkey = rowkey;
      map<TabletKey, TabletValue, TabletKeyLesser>::iterator itr = stats.lower_bound(key);

      if (itr != stats.end()) {
        itr->second.hit_times++;
      } else {
        TBSYS_LOG(ERROR, "strange rowkey");
      }
    }

    void calc_table_rows(const char *ip, unsigned port, const char *table_name, bool consistency, bool compatible) {
      db = new (std::nothrow)OceanbaseDb(ip, static_cast<int16_t>(port), kDefaultTimeout, 10000);
      if (!db || db->init()) {
        TBSYS_LOG(ERROR, "can't initialize database handle");
        return;
      }

      db->set_consistency(consistency);
      int ret = get_all_tablets(table_name);
      if (ret != OB_SUCCESS) {
        TBSYS_LOG(ERROR, "No such table");
        return;
      }

      dump_tablets(stats);
//      print_result();

      map<TabletKey, TabletValue, TabletKeyLesser>::iterator itr = stats.begin();
      uint64_t row_counts = 0;
      while (itr != stats.end()) {
        assert (itr->first.table == table_name);
        if (compatible) {
          row_counts += itr->second.tablet.ocuppy_size_;
        } else {
          row_counts += itr->second.tablet.record_count_;
        }

        itr++;
      }

      fprintf(stderr, "[%s]:%ld Rows\n", table_name, row_counts);
      fprintf(stderr, "Finish Count Successfully\n");
    }


    int show_merge_process(const char *ip, unsigned port, int64_t dest_version, bool consistency) {
      db = new (std::nothrow)OceanbaseDb(ip, static_cast<int16_t>(port), kDefaultTimeout, 10000);

      if (!db || db->init()) {
        TBSYS_LOG(ERROR, "can't initialize database handle");
        return -1;
      }

      db->set_consistency(consistency);
      schema_ = new(std::nothrow) ObSchemaManagerV2();
      if (schema_ == NULL) {
        TBSYS_LOG(ERROR, "can't allocate ObSchemaManagerV2");
        return -1;
      }

      int ret = db->fetch_schema(*schema_);
      if (ret != OB_SUCCESS) {
        TBSYS_LOG(ERROR, "can't fetch schema from db");
        return -1;
      }

      const ObTableSchema *tab_schema = schema_->table_begin();
      while (tab_schema != schema_->table_end()) {
        int ret = get_all_tablets(tab_schema->get_table_name());
        if (ret != OB_SUCCESS) {
          TBSYS_LOG(ERROR, "No such table");
          return -1;
        }
        tab_schema++;
      }

      int64_t total_tablets = 0;
      int64_t merge_finished = 0;

      map<TabletKey, TabletValue, TabletKeyLesser>::iterator itr = stats.begin();
      while (itr != stats.end()) {
        for (int i = 0;i < 3;i++) {
          if (itr->second.tablet.slice_[i].server_avail) {
            if (itr->second.tablet.slice_[i].tablet_version == dest_version) {
              merge_finished++;
            }

            total_tablets++;
          }
        }

        itr++;
      }
#if 0
      {
        map<TabletKey, TabletValue, TabletKeyLesser>::iterator itr = stats.begin();
        char buf[4096];

        while (itr != stats.end()) {
          itr->second.tablet.slice_;

          ObString key = itr->second.tablet.get_end_key();
          int len = hex_to_str(key.ptr(), key.length(), buf, 4096);
          buf[2 * len] = 0;

          fprintf(stderr, "Tablet End Key:%s\n", buf);
          for (int i = 0;i < 3;i++) {
            if (itr->second.tablet.slice_[i].server_avail) {
              unsigned char *pip = (unsigned char *)&itr->second.tablet.slice_[i].ip_v4;
              unsigned short port = itr->second.tablet.slice_[i].ms_port;
              fprintf(stderr, "Table=%s,MergeServer:%d.%d.%d.%d:%d\n", itr->first.table.c_str(), 
                      pip[0], pip[1], pip[2], pip[3], port);

              fprintf(stderr, "TabletVersion:%ld\n", itr->second.tablet.slice_[i].tablet_version);
            }
          }
          itr++;
        }
      }
#endif
      fprintf(stdout, "Merged:%ld, Total:%ld\n", merge_finished, total_tablets);

      return 0;
    }


    void list_tablet_list(const char *ip, unsigned port, const char *table_name, bool consistency) {
      db = new (std::nothrow)OceanbaseDb(ip, static_cast<int16_t>(port), kDefaultTimeout, 10000);
      if (!db || db->init()) {
        TBSYS_LOG(ERROR, "can't initialize database handle");
        return;
      }

      db->set_consistency(consistency);
      int ret = get_all_tablets(table_name);
      if (ret != OB_SUCCESS) {
        TBSYS_LOG(ERROR, "can't get all tables");
        return;
      }

      map<TabletKey, TabletValue, TabletKeyLesser>::iterator itr = stats.begin();
      char buf[4096];

      while (itr != stats.end()) {
        itr->second.tablet.slice_;

        ObString key = itr->second.tablet.get_end_key();
        int len = hex_to_str(key.ptr(), key.length(), buf, 4096);
        buf[2 * len] = 0;

        fprintf(stderr, "Tablet End Key:%s\n", buf);
        for (int i = 0;i < 3;i++) {
          if (itr->second.tablet.slice_[i].server_avail) {
            unsigned char *pip = (unsigned char *)&itr->second.tablet.slice_[i].ip_v4;
            unsigned short port = itr->second.tablet.slice_[i].ms_port;
            fprintf(stderr, "MergeServer:%d.%d.%d.%d:%d\n",pip[0], pip[1], pip[2], pip[3], port);
            fprintf(stderr, "TabletVersion:%ld\n", itr->second.tablet.slice_[i].tablet_version);
          }
        }
        itr++;
      }
    }

    int update_schema()
    {
      OceanbaseDb db(DUMP_CONFIG->get_host(), DUMP_CONFIG->get_port(), 8 * kDefaultTimeout);

      int ret = OB_SUCCESS;
      if (schema_ == NULL) ret = OB_ERROR;
      
      if (ret == OB_SUCCESS) {
        ret = db.init();
        if (ret != OB_SUCCESS) {
          TBSYS_LOG(ERROR, "can't init database,%s:%d", DUMP_CONFIG->get_host(), DUMP_CONFIG->get_port());
        }
      }

      if (ret == OB_SUCCESS) {
        ret = db.fetch_schema(*schema_);
        if (ret != OB_SUCCESS) {
          TBSYS_LOG(ERROR, "can't fetch schema from root server [%s:%d]", DUMP_CONFIG->get_host(), DUMP_CONFIG->get_port());
        }
      }

      return ret;
    }

    int init_update_count(bool rotate_per_log)
    {
      int ret = OB_SUCCESS;
      rotate_per_log_ = rotate_per_log;
      count_update_ = true;

      if (ret == OB_SUCCESS) {
        schema_ = new(std::nothrow) ObSchemaManagerV2;

        if (schema_ == NULL) {
          TBSYS_LOG(ERROR, "no enough memory for schema");
          ret = OB_ERROR;
        }
      }

      return ret;
    }

    int init(const char *ip, unsigned short port, int split, bool force_get, bool rotate_per_log) {
      int ret = OB_SUCCESS;
      force_get_ = force_get;
      rotate_per_log_ = rotate_per_log;

      /* disable tablet info timeout, for we are caculating access counts */
      db = new (std::nothrow)OceanbaseDb(ip, port, kDefaultTimeout, 10000);
      if (!db || db->init() != OB_SUCCESS) {
        TBSYS_LOG(ERROR, "can't init oceanbase db");
        ret = OB_ERROR;
      }

      if (ret == OB_SUCCESS) {
        ret = init_all_tablets();
        dump_tablets(stats);

        while (true && split) {
          TBSYS_LOG(INFO, "spliting tablets");
          split_tablets(split);
          split = split / 2;
          if (split <= 1) {
            break;
          }
        }
      }

      return ret;
    }

    bool is_last_tablet(TabletInfo &tablet_info) {
      bool ret = true;
      ObString endkey = tablet_info.get_end_key();
      const unsigned char *p = (const unsigned char *)endkey.ptr();

      {
        char key_buf[256];
        int len = hex_to_str(endkey.ptr(), endkey.length(), key_buf, 256);
        key_buf[ 2 * len] = 0;

        TBSYS_LOG(INFO, "is last_tablet:%s", key_buf);
      }

      for(int i = 0;i < endkey.length(); i++) {
        if (*(p + i) != 0xFF) {
          ret = false;
          break;
        }
      }

      if (endkey.length() == 0)
        ret = false;

      return ret;
    }

    int init_all_tablets() {
      int ret = OB_SUCCESS;
      vector<DbTableConfig> &cfgs = DUMP_CONFIG->get_configs();

      for(size_t i = 0;i < cfgs.size(); i++) {
        ret = get_all_tablets(cfgs[i].table());
        if (ret != OB_SUCCESS) {
          TBSYS_LOG(ERROR, "can't get tables from table:%s", cfgs[i].table().c_str());
          break;
        }
      }

      return ret;
    }

    int get_all_tablets(string table) {
      char key_buf[4096];
      ObString rowkey;
      int ret = OB_SUCCESS;

      key_buf[0] = 0;
      rowkey.assign(key_buf, 1);

      TabletInfo tablet;
      TabletKey key;

      TabletValue value;
      value.hit_times = 0;

      TBSYS_LOG(INFO, "get tablets from table:%s", table.c_str());
      do {
        ret = fetch_tablet_info(table, rowkey, tablet);
        if (ret != OB_SUCCESS) {
          break;
        }

        //update key/value
        key.table = table;
        key.rowkey.assign(tablet.get_end_key().ptr(), tablet.get_end_key().length());
        value.tablet = tablet;
        stats[key] = value;

        //update rowkey
        memcpy(key_buf, tablet.get_end_key().ptr(), tablet.get_end_key().length());
        key_buf[tablet.get_end_key().length()] = 0;
        rowkey.assign_ptr(key_buf, tablet.get_end_key().length() + 1);
      } while (!is_last_tablet(tablet));

      return ret;
    }

    void decode_rowkey_item(DbTableConfig::RowkeyItem &item, int64_t *val, std::string &rowkey) {
      int type = item.type;

      switch(type) {
       case 1:
         *val = int8_t(rowkey[item.start_pos]);
         break;
       case 2:
         memcpy(val, rowkey.data() + item.start_pos, 8);
         reverse((char *)val, (char *)val + 8);
         break;
      }
    }

    void encode_rowkey_item(DbTableConfig::RowkeyItem &item, char *val, string *rowkey) {
      int type = item.type;

      switch(type) {
       case 1:
         TBSYS_LOG(INFO, "append int8");
         rowkey->append(val, 1);
         break;
       case 2:
         int64_t tmp = *(int64_t *)val;
         reverse((char *)&tmp, (char *)&tmp + 8);
         rowkey->append((char *)&tmp,(char *)&tmp + 8);
         break;
      }
    }

    int create_midkey(string &table, std::string &key1, std::string &key2, std::string *midkey) {
      int ret = OB_SUCCESS;
      midkey->clear();

      DbTableConfig *cfg;
      ret = DUMP_CONFIG->get_table_config(table, cfg);
      if (ret != OB_SUCCESS) {
        TBSYS_LOG(ERROR, "No such table");
      } else {

        if (key1.length() != key2.length()) {
          ret = OB_ERROR;
          TBSYS_LOG(ERROR, "key1:%zu and key2:%zu are not the same", key1.length(), key2.length());
        } else {
          vector<DbTableConfig::RowkeyItem> &items = cfg->rowkey_columns();

          for(size_t i = 0;i < items.size(); i++) {
            int64_t key1_item;
            int64_t key2_item; 

            decode_rowkey_item(items[i], &key1_item, key1);
            decode_rowkey_item(items[i], &key2_item, key2);

            int64_t midkey_item = key1_item + key2_item;
            midkey_item /= 2;
            encode_rowkey_item(items[i], (char *)&midkey_item, midkey);
          }
        }
      }

      return ret;
    }

    void print_key(const std::string &table, std::string key) {
      char buf[256];
      int len = hex_to_str(key.data(), static_cast<int32_t>(key.length()), buf,256);
      buf[len * 2] = 0;
      TBSYS_LOG(INFO, "KEY:%s,TABLE:%s, len=%zu", buf, table.c_str(), key.length());
    }

    void dump_tablets(map<TabletKey, TabletValue, TabletKeyLesser> &stat) {
      map<TabletKey, TabletValue, TabletKeyLesser>::iterator itr = stat.begin();
      while (itr != stat.end()) {
        char buf[256];
        int len = hex_to_str(itr->first.rowkey.c_str(), static_cast<int32_t>(itr->first.rowkey.length()), buf, 256);
        buf[2 * len] = 0;
        TBSYS_LOG(INFO, "table=%s, key=%s", itr->first.table.c_str(), buf);
        itr++;
      }
    }

    void split_tablets(int split) {
      int ret = OB_SUCCESS;
      UNUSED(split);
      UNUSED(split);
      map<TabletKey, TabletValue, TabletKeyLesser>::iterator itr = stats.begin();
      bool first_tablet = true;
      std::string key1;
      std::string key2;
      std::string midkey;

      std::string last_table = itr->first.table;
      while (itr != stats.end()) {
        key1.assign(itr->first.rowkey.data(),
                    itr->first.rowkey.length());

        if (is_last_tablet(itr->second.tablet)) {
          first_tablet = true;
          last_table = itr->first.table;
          itr++;
          continue;
        }

        if (last_table != itr->first.table) {
          first_tablet = true;
          last_table = itr->first.table;
        }

        if (first_tablet) {
          first_tablet = false;
          key2.clear();
          key2.append(key1.length(), 0);
        }

        print_key("KEY1", key1);
        print_key("KEY2", key2);

        ret = create_midkey(last_table, key1, key2, &midkey);
        if (ret != OB_SUCCESS) {
          TBSYS_LOG(ERROR, "can't create mid key");
        } else {
          TabletKey key;
          key.table = itr->first.table;
          key.rowkey = midkey;

          print_key("MID", midkey);
          TabletInfo tablet;
          TabletValue value;
          value.hit_times = 0;
          value.tablet = tablet;
          stats_virtual[key] = value;
        }

        key2 = key1;
        itr++;
      }

      stats.insert(stats_virtual.begin(), stats_virtual.end());
      dump_tablets(stats);
    }

    void Destroy() {
      if (db != NULL)
        delete db;
    }

    void wait_completion(bool rotate) { 
      if (rotate || rotate_per_log_) {
        if (count_update_) {
          print_update_count();
          tab_counts_.clear();
        } else {
          print_result(); 
        }
      }
    }

    map<int,string> table_map;
    map<TabletKey, TabletValue, TabletKeyLesser> stats;
    map<TabletKey, TabletValue, TabletKeyLesser> stats_virtual;
    int64_t count_;
    bool force_get_;
    bool rotate_per_log_;
    bool count_update_;
    map<int64_t, int64_t> tab_counts_;
    OceanbaseDb *db;
    ObSchemaManagerV2 *schema_;
};

int deamonize()
{
  int ret = OB_SUCCESS;
  string pid_file = DUMP_CONFIG->pid_file();
  string log_file = DUMP_CONFIG->get_log_dir();

  int pid = 0;
  if ((pid = CProcess::existPid(pid_file.c_str()))) {
    cerr << "program has already started" << endl;
    ret = OB_ERROR;
  } else {
    cout << "log file" << log_file << endl;
    if (CProcess::startDaemon(pid_file.c_str(), log_file.c_str()) != 0)
      ret = OB_ERROR;
  }

  return ret;
}

int setup_db_env(const char *config_file, bool start_deamon)
{
  int ret = OB_SUCCESS;

  ret = DUMP_CONFIG->load(config_file);
  if (ret == OB_SUCCESS && start_deamon) {
    ret = deamonize();
  } else {
    TBSYS_LOG(ERROR, "initialize config file error, quiting");
  }

  if (ret == OB_SUCCESS) {
    OceanbaseDb::global_init(DUMP_CONFIG->get_log_dir().c_str(), 
                             DUMP_CONFIG->get_log_level().c_str());
  }

  return ret;
}

void destory_db_env()
{
  DUMP_CONFIG->destory();
}

void usage(const char *program) 
{
  fprintf(stdout, "Usage:%s -u  -h host -p port -t table [-m when database is slave] [-q version 0.11]\n", program);
  fprintf(stdout, "\tCount total rows in chunck server.\n");
  fprintf(stdout, "\tVersin 0.2 compatiable with ob 0.11\n");
  fprintf(stdout, "\n");
  fprintf(stdout, "Or....");
  fprintf(stdout, "Usage:%s -g  -h host -p port -t table\n", program);
  fprintf(stdout, "\tList all tablets in the oceanbase");
  fprintf(stdout, "\n");
  fprintf(stdout, "Or....");
  fprintf(stdout, "Usage:%s -o  -c [config file ] [-d start as a deamon]\n", program);
  fprintf(stdout, "\tcount all update request during a day\n");
  fprintf(stdout, "Or....");
  fprintf(stdout, "Usage:%s -x  -t table_name -h host -p port\n", program);
  fprintf(stdout, "\tconvert table name to table_id\n");

  fprintf(stdout, "Or....");
  fprintf(stdout, "Usage:%s -l  -h host -p port -v mem_version\n", program);
  fprintf(stdout, "\tshow total merge process\n");
}

int do_stats(int split, bool force_get, bool rotate_per_log, bool count_update)
{
  int ret = OB_SUCCESS;
  TabletStatser statser;
  if (count_update) {
    TBSYS_LOG(INFO, "counting updates during the day");
    statser.init_update_count(rotate_per_log);
  } else {
    statser.init(DUMP_CONFIG->get_host(), DUMP_CONFIG->get_port(), split, force_get, rotate_per_log);
  }

  DbLogMonitor monitor(DUMP_CONFIG->get_ob_log(), DUMP_CONFIG->get_tmp_log_path(), 
                       DUMP_CONFIG->get_monitor_interval());

  monitor.start_servic(DUMP_CONFIG->get_init_log());
  if (ret != OB_SUCCESS) {
    TBSYS_LOG(ERROR, "can't start monitor service");
  } else {
    sleep(1);
    DbLogParser parser(DUMP_CONFIG->get_tmp_log_path(), &monitor, NULL);

    ret = parser.start_parse(&statser);
    if (ret == OB_SUCCESS) {
      monitor.wait();
      parser.stop_parse();
    }

  }

  return ret;
}

int main(int argc, char *argv[])
{
  const char *config_file = NULL;

  int ret = 0;
  bool start_deamon = false;
  int split = 0;
  bool force_get = false;
  bool rotate_per_log = false;
  bool count_table_items = false;
  const char *table_name = NULL;
  const char *host = NULL;
  bool list_tablet = false;
  bool consistency = true;
  bool count_update = true;
  unsigned short port = 0;
  bool compatible = false;
  bool name_to_id = false;
  bool show_merge_process = false;
  int64_t dest_version = 0;

  while ((ret = getopt(argc, argv, "s:c:dqfruot:h:p:gmxv:l")) != -1) {
    switch (ret) {
     case 'x':
       name_to_id = true;
       break;
     case 'c':
       config_file = optarg;
       break;
     case 'd':
       start_deamon = true;
       break;
     case 'q':
       compatible = true;
       break;
     case 's':
       split = atoi(optarg);
       break;
     case 'f':
       force_get = true;
       break;
     case 'r':
       rotate_per_log = true;
       break;
     case 'u':
       count_table_items = true;
       break;
     case 't':
       table_name = optarg;
       break;
     case 'h':
       host = optarg;
       break;
     case 'p':
       port = (unsigned short)atoi(optarg);
       break;
     case 'g':
       list_tablet = true;
       break;
     case 'm':
       consistency = false;
     case 'o':
       count_update = true;
       break;
     case 'l':
       show_merge_process = true;
       break;
     case 'v':
       dest_version = atol(optarg);
       break;
    }
  }

  ret = 0;
  if (list_tablet) {
    if (!host || !port || !table_name) {
      usage(argv[0]);
      return 0;
    }

    OceanbaseDb::global_init(NULL, "ERROR");

    TabletStatser stater;
    stater.list_tablet_list(host, port, table_name, consistency);
    return 0;
  }

  if (show_merge_process) {
    if (!host || !port) {
      usage(argv[0]);
      return 0;
    }

    TBSYS_LOG(DEBUG, "memtable version = %ld", dest_version);
    OceanbaseDb::global_init(NULL, "ERROR");
    TabletStatser stater;
    ret = stater.show_merge_process(host, port, dest_version, consistency);
    return ret;
  }

  if (name_to_id) {
    if (!host || !port || !table_name) {
      usage(argv[0]);
      return 0;
    }

    OceanbaseDb::global_init(NULL, "ERROR");
    OceanbaseDb db(host, port);
    ret = db.init();
    if (ret != OB_SUCCESS) {
      TBSYS_LOG(ERROR, "can't convert name to id, table_name=%s", table_name);
    } else {
      ObSchemaManagerV2 *schema = new ObSchemaManagerV2();
      ret = db.fetch_schema(*schema);
      if (ret == OB_SUCCESS) {
        const ObTableSchema* tab_schema = schema->get_table_schema(table_name);
        if (tab_schema == NULL) {
          ret = OB_ERROR;
        } else {
          fprintf(stdout, "%ld\n", tab_schema->get_table_id());
        }
      }

      if (ret != OB_SUCCESS) {
        TBSYS_LOG(ERROR, "can't fetch schema from server, %s:%d", host, port);
      }

      if (schema != NULL) {
        delete schema;
      }
    }

    return ret;
  }

  /* count table rows, only trun server */
  if (count_table_items == true ) {     
    if (!host || !port || !table_name) {
      usage(argv[0]);
      return 0;
    }

    OceanbaseDb::global_init(NULL, "ERROR");

    TabletStatser stater;
    stater.calc_table_rows(host, port, table_name, consistency, compatible);
  } else {
    if (config_file == NULL || split < 0) {     /* tablet stats */
      usage(argv[0]);
      return 0;
    }

    ret = setup_db_env(config_file, start_deamon);
    if (ret == OB_SUCCESS) {
      ret = do_stats(split, force_get, rotate_per_log, count_update);
    }

    destory_db_env();
  }

  return ret;
}


