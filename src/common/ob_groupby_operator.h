/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or 
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *  
 * ob_groupby_operator.h for group by operator. 
 *
 * Authors:
 *   wushi <wushi.ly@taobao.com>
 *   huating <huating.zmq@taobao.com>
 *
 */
#ifndef OCEANBASE_COMMON_OB_GROUPBY_OPERATOR_H_ 
#define OCEANBASE_COMMON_OB_GROUPBY_OPERATOR_H_

#include "ob_cell_array.h"
#include "hash/ob_hashmap.h"
#include "ob_groupby.h"

namespace oceanbase
{
  namespace common
  {
    class ObRowkey;
    class ObGroupByOperator : public ObCellArray
    {
    public:
      ObGroupByOperator();
      ~ObGroupByOperator();

      void clear();

      int init(const ObGroupByParam & param, const int64_t groupby_mem_size, 
        const int64_t max_groupby_mem_size); 

      // add an org row [row_beg,row_end]
      int add_row(const ObCellArray & org_cells, const int64_t row_beg, 
                  const int64_t row_end);
      int copy_topk_row(const ObCellArray& org_cells, const int64_t row_width);

      int init_all_in_one_group_row(const ObRowkey& rowkey, 
                                    const uint64_t table_id);

      int remove_group(const ObGroupKey& key); 
      bool has_group(const ObGroupKey& key);

      int64_t get_group_count()const
      {
        return group_hash_map_.size();
      }

    private:
      int append_fake_composite_column(const ObRowkey& rowkey, 
                                       const uint64_t table_id);
      int copy_row(const ObCellArray & agg_cells, const int64_t row_beg, 
                   const int64_t row_end);

    private:
      static const int64_t HASH_SLOT_NUM = 1024*16;
      const ObGroupByParam *param_;
      hash::ObHashMap<ObGroupKey, int64_t,
        hash::NoPthreadDefendMode> group_hash_map_;
      bool inited_;
      int64_t groupby_mem_size_;
      int64_t max_groupby_mem_size_;
      ObCellInfo empty_cell_;
    };
  }
}  
#endif //OCEANBASE_COMMON_OB_GROUPBY_OPERATOR_H_
