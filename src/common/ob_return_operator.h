/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or 
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *  
 * ob_return_operator.h for implementation of return operator.
 *
 * Authors:
 *   huating <huating.zmq@taobao.com>
 *
 */
#ifndef OCEANBASE_COMMON_RETURN_OPERATOR_H_ 
#define OCEANBASE_COMMON_RETURN_OPERATOR_H_

#include "ob_iterator.h"
#include "ob_array_helper.h"

namespace oceanbase
{
  namespace common
  {
    /**
     * return operator, it does the actual return work, it filters 
     * the unreturned columns.
     */
    class ObReturnOperator : public ObInnerIterator
    {
    public:
      ObReturnOperator();
      ~ObReturnOperator();

    public:
      /**
       * init return operator, just save the param 
       * 
       * @param iter [in] the input iterator
       * @param return_columns_map the return map store the info that 
       *                           which columns are returned
       * @param row_width how mang columns row includes.
       * 
       * @return int if success, return OB_SUCCESS, else return 
       *         OB_ERROR
       */
      int start_return(ObInnerIterator& iter,
                       const ObArrayHelpers<bool>& return_columns_map,
                       const int64_t row_width, 
                       const int64_t limit_offset,
                       const int64_t limit_count);

      // clear all internal status for next new return operation
      void clear(); 

      // iterator vitual functions
      virtual int next_cell();
      virtual int get_cell(ObInnerCellInfo** cell);
      virtual int get_cell(ObInnerCellInfo** cell, bool* is_row_changed);

    private:
      void initialize();
      int jump_limit_offset();

    private:
      common::ObInnerIterator* iter_;
      const ObArrayHelpers<bool>* return_columns_map_;
      int64_t cur_column_idx_in_row_;
      int64_t row_width_;
      int64_t limit_offset_;
      int64_t limit_count_;
      int64_t max_avail_cell_num_;
      bool    row_changed_;
      bool    row_first_cell_got_;
    };
  }
}

#endif // OCEANBASE_COMMON_RETURN_OPERATOR_H_
