#ifndef _MIXED_TEST_ROW_CHECKER_
#define _MIXED_TEST_ROW_CHECKER_
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include "common/page_arena.h"
#include "common/ob_string.h"
#include "common/hash/ob_hashmap.h"
#include "common/hash/ob_hashset.h"
#include "common/ob_schema.h"
#include "common/ob_read_common_data.h"
#include "common/ob_mutator.h"
#include "updateserver/ob_memtank.h"
#include "rowkey_builder.h"
#include "cellinfo_builder.h"
#include "client_wrapper.h"

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::common::hash;
using namespace oceanbase::updateserver;

class RowChecker
{
  static const int64_t RK_SET_SIZE = 1024;
  public:
    RowChecker();
    ~RowChecker();
  public:
    void add_cell(const ObCellInfo *ci);
    bool check_row(CellinfoBuilder &cb, const ObSchema &schema);
    const ObRowkey &get_cur_rowkey() const;
    int64_t cell_num() const;

    void add_rowkey(const ObRowkey &row_key);
    bool check_rowkey(RowkeyBuilder &rb, const int64_t *prefix_ptr = NULL);
    bool is_prefix_changed(const ObRowkey &row_key);
    const ObRowkey &get_last_rowkey() const;
    int64_t rowkey_num() const;
  public:
    ObRowkey cur_row_key_;
    CellinfoBuilder::result_set_t read_result_;
    MemTank ci_mem_tank_;
    
    bool last_is_del_row_;
    ObRowkey last_row_key_;
    ObHashMap<ObRowkey, uint64_t> rowkey_read_set_;
    MemTank rk_mem_tank_;
    int64_t add_rowkey_times_;
};

class RowExistChecker
{
  public:
    RowExistChecker();
    ~RowExistChecker();
  public:
    bool check_row_not_exist(ClientWrapper &client, const ObCellInfo *ci,
                            const bool using_id, const int64_t table_start_version, const int64_t timestamp);
  private:
    ObHashMap<ObRowkey, int64_t> hash_map_;
    ObHashSet<int64_t> hash_set_;
    MemTank mem_tank_;
};

#endif // _MIXED_TEST_ROW_CHECKER_

