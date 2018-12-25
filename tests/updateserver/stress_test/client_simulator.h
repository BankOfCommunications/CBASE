
#include <common/ob_mutator.h>
#include <common/ob_scanner.h>
#include <common/ob_scan_param.h>
#include <common/ob_packet_factory.h>
#include <common/ob_client_manager.h>
#include <common/ob_define.h>
#include <common/ob_packet.h>
#include <common/ob_result.h>
#include <common/ob_schema.h>
#include <rootserver/ob_chunk_server_manager.h>

#include <common/buffer.h>
#include <common/thread_store.h>

#include <tbnet.h>
#include <tbsys.h>
#include <Mutex.h>

#include <unistd.h>
#include <getopt.h>
#include <math.h>
#include <signal.h>

using namespace oceanbase;

#define ARR_LEN(a) (sizeof(a)/sizeof(a[0]))

#define ID_TO_STR_FUNC(FUNC_NAME, ARRAY, TYPE) \
    static const char* FUNC_NAME(TYPE id) \
    { \
      static common::buffer buff; \
      if (id < static_cast<TYPE>(ARR_LEN(ARRAY)) && 0 != ARRAY[id]) \
      { \
        return ARRAY[id]; \
      } \
      else \
      { \
        buff.assign("UNKNWON").append(id); \
        return buff.ptr(); \
      } \
    }

typedef common::ObVector<const common::ObColumnSchema*> ColumnArr;

namespace oceanbase
{
  namespace common
  {
    template <>
    struct ob_vector_traits<uint64_t>
    {
    public:
      typedef uint64_t& pointee_type;
      typedef uint64_t value_type;
      typedef const uint64_t const_value_type;
      typedef value_type* iterator;
      typedef const value_type* const_iterator;
      typedef int32_t difference_type;
    };
  }
}

class Param
{
  public:
    static const int INVALID_VAL = -1;
    static const int64_t TR_TYPE_WRITE = 1;
    static const int64_t TR_TYPE_USSCAN = 2;
    static const int64_t TR_TYPE_USGET = 3;
    static const int64_t TR_TYPE_MAX = 4;
    static const int64_t LOG_PTYPE_NONE = 0;
    static const int64_t LOG_PTYPE_TR = 1;
    static const int64_t LOG_PTYPE_MAX = 2;
    static const char* TR_TYPE_WRITE_STR;
    static const char* TR_TYPE_USSCAN_STR;
    static const char* TR_TYPE_USGET_STR;
    static const char* LOG_PTYPE_NONE_STR;
    static const char* LOG_PTYPE_TR_STR;

    static const char* TrTypeStr[TR_TYPE_MAX];
    static const char* LogPtypeStr[LOG_PTYPE_MAX];

    static const int DEFAULT_THREAD_NO = 1;
    static const int64_t DEFAULT_TPS_LIMIT = 0;
    static const int64_t DEFAULT_TOTAL_TR_LIMIT = 0;
    static const int64_t DEFAULT_TIMEOUT_US = 1000000;
    static const int64_t DEFAULT_TR_TYPE = TR_TYPE_WRITE;
    static const int64_t DEFAULT_SIZE_P_TR = 0;
    static const int64_t DEFAULT_LOG_PTYPE = LOG_PTYPE_NONE;
    static const char* DEFAULT_LOG_LEVEL;
  public:
    Param();
    ~Param() {}

    ID_TO_STR_FUNC(str_tr_type, TrTypeStr, int64_t)
    ID_TO_STR_FUNC(str_log_ptype, LogPtypeStr, int64_t)
  public:
    const char* ip;
    int port;
    common::buffer schema_file;
    int thread_no;
    int64_t tps_limit;
    int64_t total_tr_limit;
    int64_t timeout_us;
    int64_t tr_type;
    int64_t size_p_tr;
    common::buffer log_name;
    const char* log_level;
    int64_t log_ptype;
  public:
    void print_usage(const char *prog_name);
    int parse_cmd_line(const int argc,  char *const argv[]);
    int fill_default_param(const int argc,  char *const argv[]);
};

class OBAPI
{
  public:
    OBAPI();
    ~OBAPI();
    int init(const common::ObServer &server);
    int apply(const common::ObMutator &mutator, const int64_t timeout);
    int msscan(const common::ObScanParam& scan_param, const int64_t timeout, common::ObScanner& scanner);
    int msget(const common::ObGetParam& get_param, const int64_t timeout, common::ObScanner& scanner);
    int usscan(const common::ObScanParam& scan_param, const int64_t timeout, common::ObScanner& scanner);
    int usget(const common::ObGetParam& get_param, const int64_t timeout, common::ObScanner& scanner);
  protected:
    int init_us_addr_();
    int init_ms_addr_();
    int apply_(const common::ObServer &server, const common::ObMutator &mutator,
        const int64_t timeout);
    int scan_(const common::ObServer &server, const common::ObScanParam& scan_param,
        const int64_t timeout, common::ObScanner& scanner);
    int get_(const common::ObServer &server, const common::ObGetParam& get_param,
        const int64_t timeout, common::ObScanner& scanner);
    const common::ObServer& random_ms_() const;

    tbnet::DefaultPacketStreamer streamer_;
    tbnet::Transport transport_;
    common::ObPacketFactory factory_;
    common::ObClientManager client_;
    common::tbuffer buff_;
    common::ObServer rs_;
    common::ObServer us_;
    std::vector<common::ObServer> ms_arr_;
};

class Random
{
  public:
    int64_t rand64(int64_t min, int64_t max);
    int64_t rand64(int64_t max) {return rand64(0, max - 1);}
    char randchar() { return static_cast<char>(rand64(0, 255)); }
    void randbuffer(common::buffer &buff);
    void init();
  protected:
    unsigned short* xsubi_arr() {return reinterpret_cast<unsigned short*>(xsubi.get()); }
  private:
    common::thread_store<uint64_t> xsubi;
};

class SchemaProxy
{
  public:
    int init(const char* schema_file);
    int random_table(const common::ObSchema* &table) const;
    int random_column(const common::ObSchema* table, ColumnArr &columns) const;
    int64_t get_table_num() const {return table_num_;}
  protected:
    const common::ObSchema* random_schema_() const;
    common::ObSchemaManager schema_;
    int64_t table_num_;
};

class ReqFiller
{
  public:
    ReqFiller() {}
    virtual ~ReqFiller() {}
    virtual int fill_mutator(const common::ObSchema* table,
        const ColumnArr &columns, common::ObMutator &mutator) const;
    virtual int fill_usscan_param(const common::ObSchema* table,
        const ColumnArr &columns, common::ObScanParam &scan_param) const;
    virtual int fill_usget_param(const common::ObSchema* table,
        const ColumnArr &columns, common::ObGetParam &get_param) const {return common::OB_SUCCESS;}
    virtual int check_scanner(const common::ObGetParam &get_param,
        const common::ObScanner &scanner) const {return common::OB_SUCCESS;}
    virtual int check_scanner(const common::ObScanParam &scan_param,
        const common::ObScanner &scanner) const {return common::OB_SUCCESS;}
};

class IdentityReqFiller : public ReqFiller
{
  public:
    IdentityReqFiller() {}
    virtual ~IdentityReqFiller() {}
    virtual int fill_mutator(const common::ObSchema* table,
        const ColumnArr &columns, common::ObMutator &mutator) const;
    virtual int fill_usscan_param(const common::ObSchema* table,
        const ColumnArr &columns, common::ObScanParam &scan_param) const;
    virtual int fill_usget_param(const common::ObSchema* table,
        const ColumnArr &columns, common::ObGetParam &get_param) const;
    virtual int check_scanner(const common::ObScanParam &scan_param,
        const common::ObScanner &scanner) const;
    virtual int check_scanner(const common::ObGetParam &get_param,
        const common::ObScanner &scanner) const;

    static const int64_t BOMB_INT_RESERVED_BITS = 32;
    static const int64_t BOMB_INT_MAX = INT64_MAX >> BOMB_INT_RESERVED_BITS;
    static const int64_t BOMB_INT_MIN = INT_LEAST64_MIN >> BOMB_INT_RESERVED_BITS;
};

class FlowLimiter
{
  public:
    static const int64_t TIME_UNIT = 100000;
    struct Stat
    {
      Stat();
      int64_t last_timestamp_;
      int64_t total_tr_;
      int64_t tr_flow_;
    };
  public:
    FlowLimiter();
    ~FlowLimiter() {}
    void init(int64_t total_tr_limit, int64_t tps_limit);
    int wait();

  protected:
    int64_t total_tr_limit_;
    int64_t tr_unit_limit_;
    common::thread_store<Stat> stat_;
};

class StatCollector
{
  public:
    struct Stat
    {
      Stat();
      int64_t num_;
      int64_t succ_num_;
      int64_t failed_num_;
      int64_t retry_num_;
      int64_t start_timestamp_;
      int64_t total_time_;
      int64_t max_rt_;
      int64_t min_rt_;
    };
  public:
    StatCollector();
    void start();
    void succ();
    void fail(int err_code);
    void retry();
    void add_to_sum();
    void print_stat() const;
    int64_t get_sum_num() const {return sum_num_;}
    int64_t get_sum_succ_num() const {return sum_succ_num_;}
    int64_t get_sum_failed_num() const {return sum_failed_num_;}
    int64_t get_sum_retry_num() const {return sum_retry_num_;}
    int64_t get_sum_total_time() const {return sum_total_time_;}
    int64_t get_max_rt() const {return max_rt_;}
    int64_t get_min_rt() const {return min_rt_;}
  protected:
    common::thread_store<Stat> stat_;
    int64_t sum_num_;
    int64_t sum_succ_num_;
    int64_t sum_failed_num_;
    int64_t sum_retry_num_;
    int64_t sum_total_time_;
    int64_t max_rt_;
    int64_t min_rt_;
};

class ApplyRunnable : public tbsys::CDefaultRunnable
{
  public:
    ApplyRunnable(int thread_no = 1) : tbsys::CDefaultRunnable(thread_no) {}
    ~ApplyRunnable() {}
    virtual void run(tbsys::CThread* thread, void* arg);
  private:
    virtual int gen_mutator_();
    common::thread_store<common::ObMutator> mutator_;
};

class USScanRunnable : public tbsys::CDefaultRunnable
{
  public:
    USScanRunnable(int thread_no = 1) : tbsys::CDefaultRunnable(thread_no) {}
    ~USScanRunnable() {}
    virtual void run(tbsys::CThread* thread, void* arg);
  private:
    virtual int gen_scan_param_();
    common::thread_store<common::ObScanParam> scan_param_;
};

class USGetRunnable : public tbsys::CDefaultRunnable
{
  public:
    USGetRunnable(int thread_no = 1) : tbsys::CDefaultRunnable(thread_no) {}
    ~USGetRunnable() {}
    virtual void run(tbsys::CThread* thread, void* arg);
  private:
    virtual int gen_get_param_();
    common::thread_store<common::ObGetParam> get_param_;
};

class GI
{
  public:
    int init();
    static GI& instance();

    Param& param() {return param_;}
    OBAPI& obapi() {return obapi_;}
    Random& random() {return random_;}
    SchemaProxy& schema() {return schema_;}
    ReqFiller& filler() {return filler_;}
    FlowLimiter& flow() {return flow_;}
    StatCollector& stat() {return stat_;}
    tbsys::CDefaultRunnable* runnable() {return run_;}
  private:
    Param param_;
    OBAPI obapi_;
    Random random_;
    SchemaProxy schema_;
    IdentityReqFiller filler_;
    FlowLimiter flow_;
    StatCollector stat_;
    tbsys::CDefaultRunnable *run_;
  private:
    GI() : param_(), obapi_() {}
    ~GI() {}
    GI(const GI&) {}
};

