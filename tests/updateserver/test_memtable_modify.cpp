#include <pthread.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <assert.h>
#include "updateserver/ob_memtable_modify.h"
#include "updateserver/ob_trigger_handler.h"
#include "sql/ob_values.h"
#include "gtest/gtest.h"

using namespace oceanbase;
using namespace common;
using namespace updateserver;
using namespace sql;

class MockUpsTableMgr : public ObIUpsTableMgr
{
  public:
    MockUpsTableMgr()
    {
      mock_active_memtable_.init();

      CommonSchemaManager cschema;
      CommonTableSchema table;
      table.set_table_id(1001);
      table.set_table_name("t1");
      table.set_max_column_id(9999);
      ObRowkeyInfo rki;
      ObRowkeyColumn rkc;
      rkc.column_id_ = 16; rkc.type_ = ObIntType; rki.add_column(rkc);
      rkc.column_id_ = 17; rkc.type_ = ObIntType; rki.add_column(rkc);
      table.set_rowkey_info(rki);
      cschema.add_table(table);

      CommonColumnSchema col;
      col.set_table_id(1001); col.set_column_id(16);  col.set_column_name("rk1");  col.set_column_type(ObIntType);
      cschema.add_column(col);
      col.set_table_id(1001); col.set_column_id(17);  col.set_column_name("rk2");  col.set_column_type(ObIntType);
      cschema.add_column(col);
      col.set_table_id(1001); col.set_column_id(101);  col.set_column_name("c1");  col.set_column_type(ObIntType);
      cschema.add_column(col);
      col.set_table_id(1001); col.set_column_id(102);  col.set_column_name("c2");  col.set_column_type(ObIntType);
      cschema.add_column(col);

      CommonSchemaManagerWrapper cschema_wrapper(cschema);
      schema_mgr_.set_schema_mgr(cschema_wrapper);
    };
    ~MockUpsTableMgr()
    {
    };
  public:
    int apply(RWSessionCtx &session_ctx, ObIterator &iter, const ObDmlType dml_type)
    {
      int ret = OB_SUCCESS;
      if (OB_SUCCESS != (ret = session_ctx.add_callback_info(session_ctx,
                                                            &(mock_active_memtable_.get_trans_cb()),
                                                            &(session_ctx.get_uc_info()))))
      {
        TBSYS_LOG(WARN, "add trans cb info to session ctx fail, ret=%d", ret);
      }
      else
      {
        session_ctx.get_ups_mutator().get_mutator().reset();
        if (OB_SUCCESS != (ret = mock_active_memtable_.set(session_ctx, iter, dml_type)))
        {
          TBSYS_LOG(WARN, "set to memtable fail ret=%d", ret);
        }
      }
      return ret;
    };
    UpsSchemaMgr &get_schema_mgr()
    {
      return schema_mgr_;
    };
    MemTable &get_memtable()
    {
      return mock_active_memtable_;
    };
  private:
    MemTable mock_active_memtable_;
    UpsSchemaMgr schema_mgr_;
};

class MockMemTableModify : public MemTableModify
{
  public:
    MockMemTableModify(RWSessionCtx &session, ObIUpsTableMgr &host) : MemTableModify(session, host)
    {
      ObRowkeyColumn rkc;

      rkc.length_ = 0;
      rkc.column_id_ = 16;
      rkc.type_ = ObIntType;
      rki_1001_.add_column(rkc);
      rki_1002_.add_column(rkc);

      rkc.length_ = 0;
      rkc.column_id_ = 17;
      rkc.type_ = ObIntType;
      rki_1001_.add_column(rkc);
      rki_1002_.add_column(rkc);
    };
    ~MockMemTableModify()
    {
    };
  private:
    ObRowkeyInfo *get_rowkey_info_(const uint64_t table_id)
    {
      ObRowkeyInfo *ret = NULL;
      switch (table_id)
      {
        case 1001:
          ret = &rki_1001_;
          break;
        case 1002:
          ret = &rki_1002_;
          break;
        default:
          break;
      }
      return ret;
    };
  private:
    ObRowkeyInfo rki_1001_;
    ObRowkeyInfo rki_1002_;
};

void build_values(const int64_t row_count, ObRowDesc &row_desc, ObValues &values1, ObValues &values2)
{
  values1.set_row_desc(row_desc);
  values2.set_row_desc(row_desc);
  for (int64_t i = 0; i < row_count; i++)
  {
    ObRow row;
    row.set_row_desc(row_desc);
    for (int64_t j = 0; j < row_desc.get_column_num(); j++)
    {
      uint64_t table_id = OB_INVALID_ID;
      uint64_t column_id = OB_INVALID_ID;
      row_desc.get_tid_cid(j, table_id, column_id);
      ObObj obj;
      obj.set_int(i + j);
      row.set_cell(table_id, column_id, obj); 
    }
    values1.add_values(row);
    values2.add_values(row);
  }
}

TEST(TestObInsertDBSemFilter, iter_succ)
{
  ObRowDesc row_desc;
  row_desc.add_column_desc(1001, 16);
  row_desc.add_column_desc(1001, 17);
  row_desc.add_column_desc(1001, 101);
  row_desc.add_column_desc(1001, 102);

  ObValues child;
  ObValues check;
  build_values(10, row_desc, child, check);

  SessionCtxFactory scf;
  SessionMgr sm;
  sm.init(1000, 1000, 1000, &scf);

  MockUpsTableMgr tm;
  uint32_t sd = 0;
  sm.begin_session(ST_READ_WRITE, tbsys::CTimeUtil::getTime(), INT64_MAX, INT64_MAX, sd);
  RWSessionCtx *session = sm.fetch_ctx<RWSessionCtx>(sd);
  LockMgr lm;
  ILockInfo *li = NULL;
  li = lm.assign(READ_COMMITED, *session);

  MockMemTableModify mm(*session, tm);
  mm.set_child(0, child);

  int ret = mm.open();
  EXPECT_EQ(OB_SUCCESS, ret);
  const ObRow *row = NULL;
  ret = mm.get_next_row(row);
  EXPECT_EQ(OB_ITER_END, ret);
  ret = mm.close();
  EXPECT_EQ(OB_SUCCESS, ret);

  session->set_trans_id(tbsys::CTimeUtil::getTime());
  sm.revert_ctx(sd);
  sm.end_session(sd);

  tm.get_memtable().dump2text(ObString(2, 2, "./"));
}

int main(int argc, char **argv)
{
  ob_init_memory_pool();
  testing::InitGoogleTest(&argc,argv); 
  int ret = RUN_ALL_TESTS();
  return ret;
}

