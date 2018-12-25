/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or 
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *  
 * ob_join_operator.h for implementation of join operator. 
 *
 * Authors:
 *   wushi <wushi.ly@taobao.com>
 *   huating <huating.zmq@taobao.com>
 *
 */
#ifndef OCEANBASE_CHUNKSERVER_JOIN_OPERATOR_H_ 
#define OCEANBASE_CHUNKSERVER_JOIN_OPERATOR_H_

#include "ob_cell_stream.h"
#include "common/ob_vector.h"
#include "common/ob_schema.h"
#include "common/ob_cell_array.h"

namespace oceanbase
{
  namespace chunkserver
  {
    /**
     * join operator, it does the actual join work, it uses
     * joins the merged data with stream from update server, it
     * joins until all the cells need to join in array container are
     * handled. and it stores the status of last round, so user must
     * use this operator to join stream round by round until to
     * completion.
     */
    class ObJoinOperator : public common::ObInnerIterator
    {
    public:
      ObJoinOperator();
      ~ObJoinOperator();

    public:
      /**
       * init and start join operator, this function do the first 
       * round join work and store the join result in result array, 
       * then user can the iterator interface to get the cells in 
       * result array. 
       * 
       * @param result_array [in|out] cell array stored the merged 
       *                     result, after join, it stores the joined
       *                     result
       * @param join_read_param read param used to build get param
       * @param ups_join_stream cell stream used to get join row data 
       *                        from update server
       * @param schema_mgr schema manager
       * 
       * @return int if success, return OB_SUCCESS, else return 
       *         OB_ERROR
       */
      int start_join(common::ObCellArray& result_array,
                     const common::ObReadParam& join_read_param,
                     ObCellStream& ups_join_stream, 
                     const common::ObSchemaManagerV2& schema_mgr);

      /**
       * after call start_join() function, and read all the cells in 
       * result array, and there is data to join, call this function 
       * to continue join.  
       * 
       * @return int if success, return OB_SUCCESS, else return 
       *         OB_ERROR
       */
      int do_join();

      // clear all internal status for next new join operation
      void clear(); 

      // iterator vitual functions
      virtual int next_cell();
      virtual int get_cell(common::ObInnerCellInfo** cell);
      virtual int get_cell(common::ObInnerCellInfo** cell, bool* is_row_changed);

    private:
      void initialize();
      bool is_del_op(const int64_t ext_val);
      int prepare_join_param_();
      int join();
      
      int ob_change_join_value_type(common::ObObj &obj);
      int ob_get_join_value(common::ObObj &res_out, const common::ObObj &left_value_in, 
                            const common::ObObj &right_value_in);

      template<typename IteratorT>
      int join_apply(const common::ObCellInfo & cell, 
                     IteratorT & dst_off_beg, 
                     IteratorT & dst_off_end);

    private:
      static const int64_t DEFAULT_JOIN_ROW_WITD_VEC_SIZE = 10240; //10K
      static const int64_t DEFAULT_JOIN_OFFSET_VEC_SIZE = 102400; //100K

      const common::ObSchemaManagerV2* schema_mgr_;
      ObCellStream*               ups_join_stream_;
      common::ObCellArray*        result_array_;
      common::ObReadParam         cur_join_read_param_;
      common::ObCellArray         join_param_array_;
      common::ObVector<int64_t>   join_row_width_vec_;
      common::ObVector<int64_t>   join_offset_vec_;
      common::ObCellInfo          join_apply_cell_adjusted_;
      common::ObStringBuf         rowkey_buffer_;
    };
  }
}

#endif // OCEANBASE_CHUNKSERVER_JOIN_OPERATOR_H_
