#include "gtest/gtest.h"
#include "liboblog/ob_log_fetcher.h"
#include "liboblog/ob_log_partitioner.h"
#include "liboblog/ob_log_router.h"
#include "easy_io.h"
#include "common/ob_tbnet_callback.h"

using namespace oceanbase;
using namespace common;
using namespace updateserver;
using namespace liboblog;

class SS : public IObLogServerSelector
{
  public:
    SS()
    {
    };
    ~SS()
    {
    };
  public:
    int init(const ObLogConfig &config)
    {
      UNUSED(config);
      return OB_SUCCESS;
    };
    void destroy()
    {
    };
    int get_ups(common::ObServer &server, const bool change)
    {
      UNUSED(change);
      server.set_ipv4_addr("10.235.152.32", 41824);
      return OB_SUCCESS;
    };
    int get_ms(common::ObServer &server, const bool change)
    {
      UNUSED(server);
      UNUSED(change);
      return OB_SUCCESS;
    };
    int get_rs(common::ObServer &server, const bool change)
    {
      UNUSED(server);
      UNUSED(change);
      return OB_SUCCESS;
    };
    void refresh()
    {
    };
    int64_t get_ups_num()
    {
      return 0;
    }
};

class RS : public IObLogRpcStub, public ObRpcStub
{
  public:
    RS()
    {
      eio_ = NULL;
      eio_ = easy_eio_create(eio_, 1);
      EXPECT_EQ(true, NULL != eio_);
      memset(&client_handler_, 0, sizeof(easy_io_handler_pt));
      client_handler_.encode = ObTbnetCallback::encode;
      client_handler_.decode = ObTbnetCallback::decode;
      client_handler_.get_packet_id = ObTbnetCallback::get_packet_id;
      EXPECT_EQ(OB_SUCCESS, client_.initialize(eio_, &client_handler_));
      EXPECT_EQ(EASY_OK, easy_eio_start(eio_));
      ObRpcStub::init(&buffer_, &client_);
    };
    ~RS()
    {
      easy_eio_stop(eio_);
      easy_eio_wait(eio_);
      easy_eio_destroy(eio_);
    };
  public:
    int fetch_log(const common::ObServer &ups,
        const ObFetchLogReq& req,
        ObFetchedLog& log,
        const int64_t timeout)
    {
      return send_1_return_1(ups, timeout, OB_FETCH_LOG, DEFAULT_VERSION, req, log);
    };
  private:
    ThreadSpecificBuffer buffer_;
    ObClientManager client_;
    easy_io_t *eio_;
    easy_io_handler_pt client_handler_;
};

class LP : public IObLogPartitioner
{
  public:
    LP()
    {
    };
    ~LP()
    {
    };
  public:
    int init(const ObLogConfig &config, IObLogSchemaGetter *schema_getter)
    {
      UNUSED(config);
      UNUSED(schema_getter);
      return OB_SUCCESS;
    };
    void destroy()
    {
    };
    int partition(const ObMutatorCellInfo *cell, uint64_t *db_partition, uint64_t *tb_partition)
    {
      UNUSED(cell);
      *db_partition = 0;
      *tb_partition = 0;
      return OB_SUCCESS;
    };
    bool is_inited() const
    {
      return true;
    }
};

int dump_ob_mutator_cell(ObMutatorCellInfo& cell,
    const bool irc,
    const bool irf,
    const ObDmlType dml_type)
{
  int err = OB_SUCCESS;
  uint64_t op = cell.op_type.get_ext();
  uint64_t table_id = cell.cell_info.table_id_;
  uint64_t column_id = cell.cell_info.column_id_;
  ObString table_name = cell.cell_info.table_name_;
  ObString column_name = cell.cell_info.column_name_;
  printf("cell: op=%lu, table=%lu[%*s], rowkey=%s, irc=%s, irf=%s, dml_type=%s, column=%lu[%*s], value=%s\n",
      op,
      table_id, table_name.length(), table_name.ptr(), to_cstring(cell.cell_info.row_key_),
      STR_BOOL(irc), STR_BOOL(irf), str_dml_type(dml_type),
      column_id, column_name.length(), column_name.ptr(), print_obj(cell.cell_info.value_));
  return err;
}

int dump_ob_mutator(ObMutator& mut)
{
  int err = OB_SUCCESS;
  TBSYS_LOG(DEBUG, "dump_ob_mutator");
  mut.reset_iter();
  while (OB_SUCCESS == err && OB_SUCCESS == (err = mut.next_cell()))
  {
    ObMutatorCellInfo* cell = NULL;
    bool irc = false;
    bool irf = false;
    ObDmlType dml_type = OB_DML_UNKNOW;
    if (OB_SUCCESS != (err = mut.get_cell(&cell, &irc, &irf, &dml_type)))
    {
      TBSYS_LOG(ERROR, "mut.get_cell()=>%d", err);
    }
    else
    {
      dump_ob_mutator_cell(*cell, irc, irf, dml_type);
    }
  }
  if (OB_ITER_END == err)
  {
    err = OB_SUCCESS;
  }
  return err;
}

bool is_same_row(ObCellInfo& cell_1, ObCellInfo& cell_2)
{
  return cell_1.table_name_ == cell_2.table_name_ && cell_1.table_id_ == cell_2.table_id_ && cell_1.row_key_ == cell_2.row_key_;
}

int mutator_size(ObMutator& src, int64_t& n_cell, int64_t& n_row)
{
  int err = OB_SUCCESS;
  ObMutatorCellInfo* last_cell = NULL;
  n_cell = 0;
  n_row = 0;
  src.reset_iter();
  while ((OB_SUCCESS == err) && (OB_SUCCESS == (err = src.next_cell())))
  {
    ObMutatorCellInfo* cell = NULL;
    if (OB_SUCCESS != (err = src.get_cell(&cell)))
    {
      TBSYS_LOG(ERROR, "mut.get_cell()=>%d", err);
    }
    else if (last_cell != NULL && is_same_row(last_cell->cell_info,cell->cell_info))
    {}
    else
    {
      n_row++;
    }
    if (OB_SUCCESS == err)
    {
      n_cell++;
      last_cell = cell;
    }
  }
  if (OB_ITER_END == err)
  {
    err = OB_SUCCESS;
  }
  return err;
}

const char* format_time(int64_t time_us)
{
  static char time_str[1024];
  const char* format = "%Y-%m-%d %H:%M:%S";
  struct tm time_struct;
  int64_t time_s = time_us / 1000000;
  if(NULL != localtime_r(&time_s, &time_struct))
  {
    strftime(time_str, sizeof(time_str), format, &time_struct);
  }
  time_str[sizeof(time_str)-1] = 0;
  return time_str;
}

const char* get_ups_mutator_type(ObLogMutator& mutator)
{
  if (mutator.is_normal_mutator())
    return "NORMAL";
  if (mutator.is_freeze_memtable())
    return "FREEZE";
  if (mutator.is_drop_memtable())
    return "DROP";
  if (mutator.is_first_start())
    return "START";
  if (mutator.is_check_cur_version())
    return "CHECK_VERSION";
  return "UNKNOWN";
}

int dump_mutator_header(ObLogMutator& mutator, int64_t& n_cell, int64_t& n_row)
{
  int err = OB_SUCCESS;
  if (OB_SUCCESS != (err = mutator_size(mutator.get_mutator(), n_cell, n_row)))
  {
    TBSYS_LOG(ERROR, "mutator_size()=>%d", err);
  }
  else
  {
    printf("%s, size %ld:%ld, MutationTime[%lu]: %s, checksum %lu:%lu\n",
        get_ups_mutator_type(mutator),
        n_cell, n_row,
        mutator.get_mutate_timestamp(),
        format_time(mutator.get_mutate_timestamp()),
        mutator.get_memtable_checksum_before_mutate(),
        mutator.get_memtable_checksum_after_mutate());
    if (mutator.is_check_cur_version())
    {
      printf("cur_version=%lu:%lu\n", mutator.get_cur_major_version(), mutator.get_cur_minor_version());
    }
  }
  return err;
}

int dump_mutator(ObLogMutator& mutator, int64_t& n_cell, int64_t& n_row, bool verbose=true)
{
  int err = OB_SUCCESS;
  if (OB_SUCCESS != (err = dump_mutator_header(mutator, n_cell, n_row)))
  {}
  else if (!verbose)
  {}
  else
  {
    if (OB_SUCCESS != (err = dump_ob_mutator(mutator.get_mutator())))
    {
    }
  }
  return err;
}

class LO : public IObLogObserver
{
  public:
    LO()
    {
    };
    ~LO()
    {
    };
  public:
    int add_mutator(ObLogMutator &mutator, volatile bool &loop_stop)
    {
      if (!loop_stop)
      {
        TBSYS_LOG(INFO, "loop stoped");
      }
      else
      {
        int64_t n_cell = 0;
        int64_t n_row = 0;
        dump_mutator(mutator, n_cell, n_row, false);
      }
      return OB_SUCCESS;
    };
};

TEST(TestLogFetcher, fetch)
{
  ObLogConfig config;

  IObLogServerSelector *ss = new SS();

  IObLogRpcStub *rs = new ObLogRpcStub();
  rs->init();

  IObLogFetcher *lf = new ObLogFetcher();
  lf->init(ss, rs, 1);

  LP *lp = new LP();

  IObLogSchemaGetter *lsg = new ObLogSchemaGetter();
  lsg->init(ss, rs);

  IObLogFilter *filter = new ObLogTableFilter();
  filter->init(lf, config, lsg);

  IObLogRouter *router = new ObLogRouter();
  router->init(config, filter, lsg);

  LO *lo0 = new LO();
  router->register_observer(0, lo0);

  router->launch();

  usleep(500 * 1000000);

  delete lo0;
  delete router;
  delete filter;
  delete lsg;
  delete lp;
  delete lf;
  delete rs;
  delete ss;
}

int main(int argc, char **argv)
{
  ob_init_memory_pool();
  testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}

