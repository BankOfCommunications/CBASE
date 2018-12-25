/*
 *  (C) 2007-2010 Taobao Inc.
 *  
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License version 2 as
 *  published by the Free Software Foundation.
 *
 *         cs_press.h is for what ...
 *
 *  Version: $Id: cs_press.h 2010年11月16日 11时02分09秒 qushan Exp $
 *
 *  Authors:
 *     qushan < qushan@taobao.com >
 *        - some work details if you want
 */



#include <tbsys.h> // for default_runnable
#include "common/ob_string.h"
#include "common/ob_schema.h"
#include "common/ob_atomic.h"
#include "client_rpc.h"
#include "base_client.h"


/**
 * qtype_: query type:
 * 0 scan sequentially
 * 1 scan randomly
 * 2 get  sequentially
 * 3 get  randomly
 */
struct Param
{
  int32_t index_;    // thread index number
  int32_t qtype_;    // thread query type
  int32_t use_cache_;// use cache in query? 0 means not use, 1 means use.
  int32_t read_mode_;


  int32_t step_;     // rowkey increment step
  int64_t seed_;     // rowkey prefix seed number
  int64_t max_seed_;  // max rowkey prefix number
  uint64_t table_id_;  // query table_id 
  int32_t row_count_;     // query row count one time.
  char columns_[128]; // query columns id array
};

class Metrics
{
  public:
    Metrics() : total_consume_(0), total_count_(0) {}
    void reset()
    {
      total_consume_ = 0;
      total_count_ = 0;
    }
    uint64_t tick(uint64_t consume, uint64_t count, bool succeed)
    {
      oceanbase::common::atomic_add(&total_consume_, consume);
      oceanbase::common::atomic_add(&total_count_, count);
      if (succeed) 
      {
        oceanbase::common::atomic_add(&total_succeed_, count);
        oceanbase::common::atomic_add(&succeed_consume_, consume);
      }
      else 
      {
        oceanbase::common::atomic_add(&total_failed_, count);
        oceanbase::common::atomic_add(&failed_consume_, consume);
      }
      return total_count_;
    }

    uint64_t succeed_average() const
    {
      if (total_succeed_ == 0) return 0;
      return succeed_consume_ / total_succeed_;
    }

    uint64_t failed_average() const
    {
      if (total_failed_ == 0) return 0;
      return failed_consume_ / total_failed_;
    }

    uint64_t average() const
    {
      if (total_count_ == 0) return 0;
      return total_consume_ / total_count_;
    }

    void start() { reset(); time_start_ = tbsys::CTimeUtil::getTime(); }
    void stop() { time_stop_ = tbsys::CTimeUtil::getTime(); }
    int64_t duration() const { return time_stop_ - time_start_; }
    void dump() const ;

    uint64_t get_total_consume() const { return total_consume_; }
    uint64_t get_total_count() const { return total_count_; }
    uint64_t get_total_succeed() const { return total_succeed_; }
    uint64_t get_total_failed() const { return total_failed_; }
    uint64_t get_succeed_consume() const { return succeed_consume_; }
    uint64_t get_failed_consume() const { return failed_consume_; }

  private:
    volatile uint64_t total_consume_;
    volatile uint64_t total_count_;
    volatile uint64_t succeed_consume_;
    volatile uint64_t failed_consume_;
    volatile uint64_t total_succeed_;
    volatile uint64_t total_failed_;
    int64_t time_start_;
    int64_t time_stop_;
};

class Worker : public tbsys::CDefaultRunnable
{
  public:
    virtual void run(tbsys::CThread *thread, void *arg);
  public:
    void dump_metrics() const;
    Metrics & get_metrics() { return metrics_; }
  protected:
    int seq_scan_case(const Param &param);
    int random_scan_case(const Param &param);
    int seq_get_case(const Param &param);
    int random_get_case(const Param &param);
    int get_column_array(const Param &param, 
        int64_t *column_id_array, int64_t &column_id_size, bool &is_whole_row);
    /*
     * scan request
     * @param param thread param object
     * @param table_id scan table id
     * @param start_seed scan start key seed
     * @param end_seed scan end key seed
     * @param column_id_array scan column's id
     * @param column_id_size size of %column_id_array
     */
    int unit_scan_case(const Param &param,
        const int64_t start_seed,
        const int64_t end_seed,
        const int64_t *column_id_array, 
        const int64_t column_id_size, 
        const bool is_whole_row);

    /*
     * get request
     */
    int unit_get_case(const Param &param,
        const int64_t *seed_array,
        const int64_t seed_array_size,
        const int64_t *column_id_array, 
        const int64_t column_id_size,
        const bool is_whole_row);

    /*
     * build rowkey from seed
     * @param seed
     * @param rowkey, caller must allocate memory
     */
    int get_rowkey(const int64_t seed, oceanbase::common::ObString& rowkey);
    int get_rowkey_length(const int64_t seed);

    int check_query_result(
        const Param &param,
        const oceanbase::common::ObScanner& scanner,
        const int64_t *seed_array,
        const int64_t seed_array_size,
        const int64_t *column_id_array, 
        const int64_t column_id_size,
        const bool is_whole_row);

    int check_cell(
        const Param &param,
        const oceanbase::common::ObCellInfo &cell_info,
        const uint64_t column_id,
        uint32_t &hash_value);

  private:
    Metrics metrics_;
};

class GFactory
{
  public:
    static const int32_t MAX_THREAD_COUNT = 500;
  public:
    GFactory();
    ~GFactory(); 
    int initialize(const oceanbase::common::ObServer& remote_server, 
        const char* schema_file, const int32_t tc);
    int start();
    int stop();
    int wait();
    static GFactory& get_instance() { return instance_; }
    inline ObClientRpcStub& get_rpc_stub() { return rpc_stub_; }
    inline const Param& get_param(int index) const { return params_[index]; }
    inline Param* get_params() { return params_; }
    inline const oceanbase::common::ObSchemaManager& get_schema_manager() const { return schema_manager_; }
  private:
    static GFactory instance_;
    ObClientRpcStub rpc_stub_;
    BaseClient client_;
    Param params_[MAX_THREAD_COUNT];
    Worker worker_;
    oceanbase::common::ObSchemaManager schema_manager_;
    int32_t thread_count_;
};






