#include "updateserver/ob_memtable_rowiter.h"
#include "common/ob_action_flag.h"
#include "updateserver/ob_update_server_main.h"
#include "test_helper.h"
#include "test_utils.h"
#include "../common/test_rowkey_helper.h"

using namespace oceanbase;
using namespace updateserver;
using namespace common;

static CharArena allocator_;
void  __attribute__((constructor)) init_global_main()
{
}

void  __attribute__((destructor)) destroy_global_main()
{
}

int test_main(int argc, char **argv)
{
  UNUSED(argc);
  UNUSED(argv);
  ObUpdateServerMain *main = ObUpdateServerMain::get_instance();
  ObUpsTableMgr &tm = main->get_update_server().get_table_mgr();
  CommonSchemaManagerWrapper schema_mgr;
  tbsys::CConfig config;
  schema_mgr.parse_from_file("test_schema.ini", config);
  tm.set_schemas(schema_mgr);

  MemTable memtable;
  MemTableRowIterator row_iter;
  ObUpsMutator mutator;
  ObMutatorCellInfo cellinfo;
  uint64_t trans_descriptor = 0;

  int ret = memtable.init();
  assert(OB_SUCCESS == ret);

  cellinfo.op_type.set_ext(ObActionFlag::OP_INSERT);
  cellinfo.cell_info.table_id_ = 1001;
  cellinfo.cell_info.row_key_ = make_rowkey("rk0001_00000000000", &allocator_);
  cellinfo.cell_info.column_id_ = 5;
  cellinfo.cell_info.value_.set_int(1023);
  ret = mutator.get_mutator().add_cell(cellinfo);
  assert(OB_SUCCESS == ret);

  cellinfo.cell_info.value_.set_int(1, true);
  ret = mutator.get_mutator().add_cell(cellinfo);
  assert(OB_SUCCESS == ret);

  cellinfo.cell_info.table_id_ = 1002;
  cellinfo.cell_info.value_.set_int(1024);
  ret = mutator.get_mutator().add_cell(cellinfo);
  assert(OB_SUCCESS == ret);

  prepare_mutator(mutator.get_mutator());

  ret = memtable.start_transaction(WRITE_TRANSACTION, trans_descriptor);
  assert(OB_SUCCESS == ret);
  ret = memtable.start_mutation(trans_descriptor);
  assert(OB_SUCCESS == ret);
  ret = memtable.set(trans_descriptor, mutator);
  assert(OB_SUCCESS == ret);
  ret = memtable.end_mutation(trans_descriptor, false);
  assert(OB_SUCCESS == ret);
  ret = memtable.end_transaction(trans_descriptor, false);
  assert(OB_SUCCESS == ret);

  ret = row_iter.init(&memtable);
  assert(OB_SUCCESS == ret);

  uint64_t check_id = 1001;
  sstable::ObSSTableRow sstable_row;
  ObRowkey rowkey(NULL, 1);
  while (OB_SUCCESS == row_iter.next_row())
  {
    ret = row_iter.get_row(sstable_row);
    assert(OB_SUCCESS == ret);
    assert(check_id++ == sstable_row.get_table_id());
    sstable_row.get_rowkey(rowkey);
    assert(cellinfo.cell_info.row_key_ == rowkey);

    for (int32_t i = 0; i < sstable_row.get_obj_count();)
    {
      int64_t intv = 0;
      sstable_row.get_obj(i++)->get_int(intv);
      fprintf(stderr, "table_id=%lu column_id=%lu ", sstable_row.get_table_id(), intv);
      fprintf(stderr, "value=[%s]\n", common::print_obj(*sstable_row.get_obj(i++)));
    }
  }
  row_iter.reset_iter();
  check_id = 1001;
  while (OB_SUCCESS == row_iter.next_row())
  {
    ret = row_iter.get_row(sstable_row);
    assert(OB_SUCCESS == ret);
    assert(check_id++ == sstable_row.get_table_id());
    sstable_row.get_rowkey(rowkey);
    assert(cellinfo.cell_info.row_key_ == rowkey);

    for (int32_t i = 0; i < sstable_row.get_obj_count();)
    {
      int64_t intv = 0;
      sstable_row.get_obj(i++)->get_int(intv);
      fprintf(stderr, "table_id=%lu column_id=%lu ", sstable_row.get_table_id(), intv);
      fprintf(stderr, "value=[%s]\n", common::print_obj(*sstable_row.get_obj(i++)));
    }
  }
  return 0;
}

int main(int argc, char **argv)
{
  ob_init_memory_pool();
  TBSYS_LOGGER.setLogLevel("info");
  test_main(argc, argv);
  ObUpdateServerMain *ups_main = ObUpdateServerMain::get_instance();
  delete ups_main;
}

