/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or 
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *  
 * ob_compose_operator.h for implementation of compose operator.
 *
 * Authors:
 *   huating <huating.zmq@taobao.com>
 *
 */
#ifndef OCEANBASE_COMMON_COMPOSE_OPERATOR_H_ 
#define OCEANBASE_COMMON_COMPOSE_OPERATOR_H_

#include "ob_cell_array.h"
#include "ob_composite_column.h"

namespace oceanbase
{
  namespace common
  {
    /**
     * compose operator, it does the actual compose work, it 
     * traverses result cell array by row and caculate the composite 
     * column value. 
     *  
     * WARNING: in order to avoid memory copy, the operator doesn't 
     * create a new result array to store the result, it caculates 
     * composite column value based on the input result cell array, 
     * and the result cell array has filled fake cell for each 
     * composite column. 
     */
    class ObComposeOperator: public ObInnerIterator
    {
    public:
      ObComposeOperator();
      virtual ~ObComposeOperator();

    public:
      /**
       * init and start compose operator, this function do the first
       * round compose work and store the composed result in result 
       * array, then user can the iterator interface to get the cells 
       * in result array. 
       * 
       * @param result_array [in|out] cell array stored the merged 
       *                     result, after compose, it stores the
       *                     composed result
       * @param composite_columns composite column array param in scan 
       *                          param or groupby param
       * @param row_width how mang columns row includes. row_width = 
       *                  basic_column_size + composite_column_size
       * 
       * @return int if success, return OB_SUCCESS, else return 
       *         OB_ERROR
       */
      int start_compose(ObCellArray& result_array,
                        const ObArrayHelper<ObCompositeColumn>& composite_columns,
                        const int64_t row_width);

      /**
       * after call start_compose() function, and read all the cells 
       * in result array, and there is data to compose, call this 
       * function to continue compose. 
       * 
       * @return int if success, return OB_SUCCESS, else return 
       *         OB_ERROR
       */
      int do_compose();

      // clear all internal status for next new return operation
      void clear(); 

      // iterator vitual functions
      virtual int next_cell();
      virtual int get_cell(ObInnerCellInfo** cell);
      virtual int get_cell(ObInnerCellInfo** cell, bool* is_row_changed);

    private:
      void initialize();
      int compose_one_row(const int64_t row_beg, const int64_t row_end);

    private:
      common::ObCellArray* result_array_;
      const ObArrayHelper<ObCompositeColumn>* composite_columns_;
      int64_t row_width_;
    };
  }
}

#endif // OCEANBASE_COMMON_COMPOSE_OPERATOR_H_
