////===================================================================
 //
 // ob_row_compaction.h common / Oceanbase
 //
 // Copyright (C) 2010 Taobao.com, Inc.
 //
 // Created on 2011-10-20 by Yubai (yubai.lk@taobao.com) 
 //
 // -------------------------------------------------------------------
 //
 // Description
 //
 // -------------------------------------------------------------------
 // 
 // Change Log
 //
////====================================================================
#ifndef  OCEANBASE_COMMON_ROW_COMPACTION_H_
#define  OCEANBASE_COMMON_ROW_COMPACTION_H_
#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <pthread.h>
#include <new>
#include <typeinfo>
#include "common/ob_define.h"
#include "common/page_arena.h"
#include "common/ob_malloc.h"
#include "common/ob_mod_define.h"
#include "common/ob_iterator.h"

namespace oceanbase
{
  namespace common
  {
    class ObRowCompaction : public ObIterator
    {
      struct ObjNode
      {
        int64_t version;
        int64_t column_id;
        ObObj value;
        ObjNode *next;
      };
      static const int64_t NOP_FLAG = 0x0000000000000001;
      static const int64_t DEL_ROW_FLAG = 0x0000000000000002;
      static const int64_t ROW_DOES_NOT_EXIST_FLAG = 0x0000000000000004;
      public:
        ObRowCompaction();
        ~ObRowCompaction();
      public:
        // 传入的iterator的get_cell接口必须支持返回正确的is_row_changed
        int set_iterator(ObIterator *iter);
      public:
        int next_cell();
        int get_cell(ObCellInfo **cell_info);
        int get_cell(ObCellInfo **cell_info, bool *is_row_changed);
        int is_row_finished(bool *is_row_finished);
      private:
        int row_compaction_();
        int add_cell_(const ObCellInfo *cell_info, int64_t &row_ext_flag);
      private:
        const uint64_t NODES_NUM_;

        ObjNode *nodes_;
        ObjNode *list_;
        ObjNode *tail_;
        int64_t cur_version_;

        ObIterator *iter_;
        bool is_row_changed_;
        ObjNode row_del_node_;
        ObjNode row_nop_node_;
        ObjNode row_not_exist_node_;
        ObCellInfo *prev_cell_;
        ObCellInfo cur_cell_;
    };
  }
}

#endif //OCEANBASE_COMMON_ROW_COMPACTION_H_

