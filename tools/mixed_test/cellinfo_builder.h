#ifndef _MIXED_TEST_CELLINFO_BUILDER_
#define _MIXED_TEST_CELLINFO_BUILDER_
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include "common/page_arena.h"
#include "common/ob_string.h"
#include "common/hash/ob_hashmap.h"
#include "common/ob_schema.h"
#include "common/ob_read_common_data.h"
#include "common/ob_mutator.h"
#include "schema_compatible.h"

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::common::hash;

class CellinfoBuilder
{
  public:
    enum
    {
      UPDATE = 1,
      INSERT = 2,
      ADD = 3,
      DEL_ROW = 4,
      DEL_CELL = 5,
    };
    class result_set_t : public ObHashMap<uint64_t, ObCellInfo*>
    {
      public:
        result_set_t()
        {
          create(OB_MAX_COLUMN_NUMBER);
        };
    };
    typedef result_set_t::iterator result_iter_t;
  public:
    CellinfoBuilder();
    ~CellinfoBuilder();
  public:
    static int check_schema_mgr(const ObSchemaManager &schema_mgr);
    int get_mutator(const ObString &row_key, const ObSchema &schema, const int64_t cell_num, int64_t &cur_seed,
                    ObMutator &mutator, PageArena<char> &allocer);
    int get_result(const ObString &row_key, const ObSchema &schema,
                  const int64_t begin_seed, const int64_t end_seed, const int64_t cell_num,
                  result_set_t &result, PageArena<char> &allocer);
    static void merge_obj(ObCellInfo *cell_info, result_set_t &result);
  private:
    void build_cell_(struct drand48_data &rand_data, const ObString &row_key, const ObSchema &schema,
                    ObObj &obj, int64_t &column_pos, int &op_type,
                    PageArena<char> &allocer);
    int build_operator_(struct drand48_data &rand_data, const ObString &row_key, const ObSchema &schema,
                        ObMutator &mutator, PageArena<char> &allocer, int &op_type);
    int calc_op_type_(const int64_t rand);
};

#endif // _MIXED_TEST_CELLINFO_BUILDER_

