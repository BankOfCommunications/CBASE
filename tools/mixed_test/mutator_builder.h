#ifndef _MIXED_TEST_MUTATOR_BUILDER_
#define _MIXED_TEST_MUTATOR_BUILDER_
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include "common/page_arena.h"
#include "common/ob_string.h"
#include "common/hash/ob_hashmap.h"
#include "common/ob_schema.h"
#include "common/ob_read_common_data.h"
#include "common/ob_mutator.h"
#include "common/hash/ob_hashmap.h"
#include "common/hash/ob_hashutils.h"
#include "updateserver/ob_table_engine.h"
#include "cellinfo_builder.h"
#include "rowkey_builder.h"
#include "client_wrapper.h"

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::common::hash;
using namespace oceanbase::updateserver;

class MutatorBuilder
{
  typedef ObHashMap<TEKey, int64_t> seed_map_t;
  public:
    MutatorBuilder();
    ~MutatorBuilder();
  public:
    int init4read(const ObSchemaManager &schema_mgr,
                  const int64_t prefix_start,
                  const char *serv_addr,
                  const int serv_port,
                  const int64_t table_start_version,
                  const bool using_id);
    int init4write(const ObSchemaManager &schema_mgr,
                  const int64_t prefix_start,
                  const char *serv_addr,
                  const int serv_port,
                  const int64_t table_start_version,
                  const bool using_id,
                  const int64_t suffix_length,          // for write only
                  const int64_t suffix_num,             // for write only
                  const int64_t prefix_end,             // for write only
                  const int64_t max_rownum_per_mutator, // for write only
                  ClientWrapper *client4write);
    void destroy();
    int build_mutator(ObMutator &mutator, PageArena<char> &allocer, const bool using_id, const int64_t cell_num);
    int build_get_param(ObGetParam &get_param, PageArena<char> &allocer,
                        const int64_t table_start_version, const bool using_id,
                        const int64_t row_num, const int64_t cell_num_per_row);
    int build_scan_param(ObScanParam &scan_param, PageArena<char> &allocer,
                        const int64_t table_start_version, const bool using_id, int64_t &table_pos, int64_t &prefix);
    int build_total_scan_param(ObScanParam &scan_param, PageArena<char> &allocer,
                              const int64_t table_start_version, const bool using_id, const int64_t table_pos);
    RowkeyBuilder &get_rowkey_builder(const int64_t schema_pos);
    CellinfoBuilder &get_cellinfo_builder(const int64_t schema_pos);
    const ObSchema &get_schema(const int64_t schema_pos) const;
    int64_t get_schema_num() const;
    int64_t get_schema_pos(const uint64_t table_id) const;
    int64_t get_schema_pos(const ObString &table_name) const;
  private:
    int64_t get_cur_seed_(const seed_map_t &seed_map, const ObSchema &schema, const ObString &row_key, const bool using_id);
    int64_t get_cur_rowkey_info_(const ObSchema &schema, const bool using_id, const int64_t prefix_start);
    int64_t get_suffix_length_(const ObSchema &schema, const bool using_id, const int64_t prefix_start);
    int64_t get_suffix_num_(const ObSchema &schema, const bool using_id, const int64_t prefix_start);
    int64_t get_prefix_end_(const ObSchema &schema, const bool using_id, const int64_t prefix_start);
    int64_t query_prefix_meta_(const ObSchema &schema, const bool using_id, const int64_t prefix_start, const char *column_name);
    static int put_suffix_length_(ClientWrapper &client, const ObSchema &schema, const int64_t prefix_start, const int64_t suffix_length);
    static int put_prefix_end_(ClientWrapper &client, const ObSchema &schema, const int64_t prefix_start, const int64_t prefix_end);
    static int put_suffix_num_(ClientWrapper &client, const ObSchema &schema, const int64_t prefix_start, const int64_t suffix_num);
    static int write_prefix_meta_(ClientWrapper &client, const ObSchema &schema, const int64_t prefix_start,
                                  const int64_t param, const char *column_name);
  public:
    CellinfoBuilder *cb_array_[OB_MAX_TABLE_NUMBER];
    RowkeyBuilder *rb_array_[OB_MAX_TABLE_NUMBER];
    int64_t prefix_end_array_[OB_MAX_TABLE_NUMBER];
    bool need_update_prefix_end_;
    ObSchemaManager schema_mgr_;
    int64_t max_rownum_per_mutator_;
    int64_t table_num_;
    struct drand48_data table_rand_seed_;
    ClientWrapper client_;
    int64_t table_start_version_;
    //seed_map_t seed_map_;
    //PageArena<char> allocer_;
};

#endif // _MIXED_TEST_MUTATOR_BUILDER_

