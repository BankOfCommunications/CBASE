#include "gtest/gtest.h"
#include "liboblog/ob_log_fetcher.h"
#include "liboblog/ob_log_partitioner.h"
#include "liboblog/ob_log_router.h"
#include "liboblog/ob_log_meta_manager.h"
#include "liboblog/ob_log_formator.h"

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
      server.set_ipv4_addr("10.232.36.34", 41825);
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
      UNUSED(change);
      server.set_ipv4_addr("10.232.36.34", 41815);
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

class DBNB : public IObLogDBNameBuilder
{
  public:
    DBNB()
    {
    };
    ~DBNB()
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
    int get_db_name(const char *src_name,
        const uint64_t partition,
        char *dest_name,
        const int64_t dest_buffer_size)
    {
      snprintf(dest_name, dest_buffer_size, "%s_%04lu", src_name, partition);
      return OB_SUCCESS;
    };
};

class TBNB : public IObLogTBNameBuilder
{
  public:
    TBNB()
    {
    };
    ~TBNB()
    {
    };
  public:
    int init(const ObLogConfig &config)
    {
      UNUSED(config);
      return OB_SUCCESS;
    };
    virtual void destroy()
    {
    };
    int get_tb_name(const char *src_name,
        const uint64_t partition,
        char *dest_name,
        const int64_t dest_buffer_size)
    {
      snprintf(dest_name, dest_buffer_size, "%s_%04lu", src_name, partition);
      return OB_SUCCESS;
    };
};

TEST(TestLogMetaManager, get)
{
  ObLogConfig config;

  IObLogRpcStub *lrs = new ObLogRpcStub();
  lrs->init();

  IObLogServerSelector *lss = new SS();
  lss->init(config);

  IObLogSchemaGetter *lsg = new ObLogSchemaGetter();
  lsg->init(lss, lrs);

  IObLogDBNameBuilder *dbnb = new DBNB();
  dbnb->init(config);

  IObLogTBNameBuilder *tbnb = new TBNB();
  tbnb->init(config);

  IObLogMetaManager *lmm = new ObLogMetaManager();
  lmm->init(lsg, dbnb, tbnb);

  //////////////////////////////////////////////////

  IObLogFetcher *lf = new ObLogFetcher();
  lf->init(lss, lrs, 1);

  IObLogFilter *filter = new ObLogTableFilter();
  filter->init(lf, config, lsg);

  IObLogPartitioner *lp = new LP();

  IObLogRouter *router = new ObLogRouter();
  router->init(config, filter, lsg);

  IObLogFormator *lfm0 = new ObLogDenseFormator();
  lfm0->init(config, lsg, lmm, filter);
  router->register_observer(0, lfm0);

  router->launch();

  while (true)
  {
    IBinlogRecord *br = NULL;
    lfm0->next_record(&br, 1000000);
    if (NULL != br)
    {
      fprintf(stdout, "[%d] db_name=[%s] table_name=[%s]", br->recordType(), br->dbname(), br->getTableMeta()->getName());
      std::vector<std::string*>::const_iterator ic = br->newCols().begin();
      //std::vector<std::string*>::const_iterator in = br->colNames().begin();
      while (true)
      {
        //fprintf(stdout, "%s=[%s] ", (*in)->c_str(), (*ic)->c_str());
        fprintf(stdout, "%s ", (*ic)->c_str());
        ic++;
        //in++;
        if (ic == br->newCols().end())
        {
          break;
        }
      }
      fprintf(stdout, "\n");
      lfm0->release_record(br);
    }
  }

  delete router;
  delete lfm0;
  delete lp;
  delete filter;
  delete lf;
  delete lmm;
  delete tbnb;
  delete dbnb;
  delete lsg;
  delete lss;
  delete lrs;
}

int main(int argc, char **argv)
{
  ob_init_memory_pool();
  testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}

