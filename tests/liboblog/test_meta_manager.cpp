#include "gtest/gtest.h"
#include "liboblog/ob_log_fetcher.h"
#include "liboblog/ob_log_partitioner.h"
#include "liboblog/ob_log_router.h"
#include "liboblog/ob_log_meta_manager.h"
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
      UNUSED(change);
      server.set_ipv4_addr("10.235.152.32", 41814);
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
    int init(ObLogConfig &config)
    {
      UNUSED(config);
      return OB_SUCCESS;
    };
    void destroy()
    {
    };
    int partition(updateserver::ObUpsMutator &mutator, uint64_t *partition)
    {
      UNUSED(mutator);
      *partition = 0;
      return OB_SUCCESS;
    };
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


  ITableMeta *nil_t = NULL;
  ITableMeta *tm = lmm->get_table_meta(3001, 1, 1);
  EXPECT_NE(nil_t, tm);

  EXPECT_EQ(0, strcmp(tm->getName(), "ipm_inventory_0001"));
  EXPECT_EQ(true, tm->hasPK());
  fprintf(stdout, "[PKS]=%s\n", tm->getPKs());
  EXPECT_EQ(0, strcmp(tm->getEncoding(), "UTF-8"));

  IDBMeta *nil_d = NULL;
  IDBMeta *dm = tm->getDBMeta();
  EXPECT_NE(nil_d, dm);
  EXPECT_EQ(0, strcmp(dm->getName(), "ipm_inventory_0001"));
  EXPECT_EQ(0, strcmp(dm->getEncoding(), "UTF-8"));

  IMetaDataCollections *nil_c = NULL;
  IMetaDataCollections *dbc = dm->getMetaDataCollections();
  EXPECT_NE(nil_c, dbc);

  int32_t num_c = tm->getColCount();
  EXPECT_EQ(15, num_c);
  for (int32_t i = 0; i < num_c; i++)
  {
    IColMeta *cm = tm->getCol(i);
    fprintf(stdout, "%s\n", cm->getName());
  }


  delete lmm;
}

int main(int argc, char **argv)
{
  ob_init_memory_pool();
  testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}

