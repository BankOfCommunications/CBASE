////===================================================================
 //
 // ob_row_compaction.cpp common / Oceanbase
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
#include "common/ob_action_flag.h"
#include "common/ob_row_compaction.h"
#include "common/utility.h"

namespace oceanbase
{
  namespace common
  {
    ObRowCompaction::ObRowCompaction() : NODES_NUM_(OB_ALL_MAX_COLUMN_ID + 1),
                                         nodes_(NULL),
                                         list_(NULL),
                                         tail_(NULL),
                                         cur_version_(0),
                                         iter_(NULL),
                                         is_row_changed_(false),
                                         row_del_node_(),
                                         row_nop_node_(),
                                         row_not_exist_node_(),
                                         prev_cell_(NULL),
                                         cur_cell_()
    {
      if (NULL == (nodes_ = (ObjNode*)ob_malloc(sizeof(ObjNode) * NODES_NUM_, ObModIds::OB_ROW_COMPACTION)))
      {
        TBSYS_LOG(ERROR, "new obj_nodes array fail");
      }
      else
      {
        memset(nodes_, 0, sizeof(ObjNode) * NODES_NUM_);
        row_del_node_.version = 0;
        row_del_node_.column_id = OB_INVALID_ID;
        row_del_node_.value.set_ext(ObActionFlag::OP_DEL_ROW);
        row_del_node_.next = NULL;
        row_nop_node_.version = 0;
        row_nop_node_.column_id = OB_INVALID_ID;
        row_nop_node_.value.set_ext(ObActionFlag::OP_NOP);
        row_nop_node_.next = NULL;
        row_not_exist_node_.version = 0;
        row_not_exist_node_.column_id = OB_INVALID_ID;
        row_not_exist_node_.value.set_ext(ObActionFlag::OP_ROW_DOES_NOT_EXIST);
        row_not_exist_node_.next = NULL;
      }
    }

    ObRowCompaction::~ObRowCompaction()
    {
      if (NULL != nodes_)
      {
        ob_free(nodes_);
      }
    }

    int ObRowCompaction::set_iterator(ObIterator *iter)
    {
      int ret = OB_SUCCESS;
      if (NULL == iter)
      {
        TBSYS_LOG(WARN, "invalid param iter null pointer");
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        list_ = NULL;
        tail_ = NULL;
        iter_ = iter;
        prev_cell_ = NULL;
      }
      return ret;
    }

    int ObRowCompaction::next_cell()
    {
      int ret = OB_SUCCESS;
      if (NULL == nodes_
          || NULL == iter_)
      {
        TBSYS_LOG(WARN, "nodes=%p iter=%p can not work", nodes_, iter_);
        ret = OB_ERROR;
      }
      else if (NULL == list_
              || NULL == (list_ = list_->next))
      {
        cur_version_ += 1;
        is_row_changed_ = true;
        ret = row_compaction_();
      }
      else
      {
        is_row_changed_ = false;
      }
      return ret;
    }

    int ObRowCompaction::get_cell(ObCellInfo **cell_info)
    {
      int ret = OB_SUCCESS;
      if (NULL == nodes_
          || NULL == iter_)
      {
        TBSYS_LOG(WARN, "nodes=%p iter=%p can not work", nodes_, iter_);
        ret = OB_ERROR;
      }
      else if (NULL == cell_info)
      {
        TBSYS_LOG(WARN, "invalid param cell_info=%p", cell_info);
        ret = OB_INVALID_ARGUMENT;
      }
      else if (NULL == list_)
      {
        ret = OB_ITER_END;
      }
      else
      {
        cur_cell_.column_id_ = list_->column_id;
        cur_cell_.value_ = list_->value;
        *cell_info = &cur_cell_;
      }
      return ret;
    }

    int ObRowCompaction::get_cell(ObCellInfo **cell_info, bool *is_row_changed)
    {
      int ret = get_cell(cell_info);
      if (OB_SUCCESS == ret
          && NULL != is_row_changed)
      {
        *is_row_changed = is_row_changed_;
      }
      return ret;
    }

    int ObRowCompaction::is_row_finished(bool *is_row_finished)
    {
      int ret = OB_SUCCESS;
      if (NULL == nodes_
          || NULL == iter_)
      {
        TBSYS_LOG(WARN, "nodes=%p iter=%p can not work", nodes_, iter_);
        ret = OB_ERROR;
      }
      else if (NULL == list_)
      {
        ret = OB_ITER_END;
      }
      else
      {
        if (NULL != is_row_finished)
        {
          *is_row_finished = (NULL == list_->next);
        }
      }
      return ret;
    }

    int ObRowCompaction::row_compaction_()
    {
      int ret = OB_SUCCESS;
      int64_t row_ext_flag = 0;
      if (NULL != prev_cell_)
      {
        ret = add_cell_(prev_cell_, row_ext_flag);
        cur_cell_.table_id_ = prev_cell_->table_id_;
        cur_cell_.row_key_ = prev_cell_->row_key_;
        prev_cell_ = NULL;
      }

      int tmp_ret = OB_SUCCESS;
      ObCellInfo *cell_info = NULL;
      bool is_row_changed = false;
      while (OB_SUCCESS == ret
            && OB_SUCCESS == (tmp_ret = iter_->next_cell()))
      {
        if (OB_SUCCESS == (tmp_ret = iter_->get_cell(&cell_info, &is_row_changed))
            && NULL != cell_info)
        {
          if (!is_row_changed)
          {
            ret = add_cell_(cell_info, row_ext_flag);
          }
          else if (NULL == list_
                  && 0 == row_ext_flag)
          {
            ret = add_cell_(cell_info, row_ext_flag);
            cur_cell_.table_id_ = cell_info->table_id_;
            cur_cell_.row_key_ = cell_info->row_key_;
          }
          else
          {
            prev_cell_ = cell_info;
            break;
          }
        }
        else
        {
          ret = (OB_SUCCESS == tmp_ret) ? OB_ERROR : tmp_ret;
        }
      }
      
      if (OB_SUCCESS == ret
          && 0 != row_ext_flag)
      {
        if (NOP_FLAG & row_ext_flag)
        {
          if (NULL == list_)
          {
            row_nop_node_.next = list_;
            list_ = &row_nop_node_;
          }
        }
        if (ROW_DOES_NOT_EXIST_FLAG & row_ext_flag)
        {
          if (NULL == list_)
          {
            row_not_exist_node_.next = list_;
            list_ = &row_not_exist_node_;
          }
        }
        if (DEL_ROW_FLAG & row_ext_flag)
        {
          row_del_node_.next = list_;
          list_ = &row_del_node_;
        }
      }
      if (OB_SUCCESS == ret
          && NULL == list_)
      {
        ret = (OB_SUCCESS == tmp_ret) ? OB_ITER_END : tmp_ret;
      }
      
      return ret;
    }

    int ObRowCompaction::add_cell_(const ObCellInfo *cell_info, int64_t &row_ext_flag)
    {
      int ret = OB_SUCCESS;
      if (NULL == cell_info)
      {
        TBSYS_LOG(WARN, "invalid param cell_info=%p", cell_info);
        ret = OB_INVALID_ARGUMENT;
      }
      else if (OB_INVALID_ID == cell_info->column_id_
              && ObExtendType == cell_info->value_.get_type())
      {
        if (ObActionFlag::OP_DEL_ROW == cell_info->value_.get_ext())
        {
          row_ext_flag = DEL_ROW_FLAG;
          list_ = NULL;
          tail_ = NULL;
          cur_version_ += 1;
        }
        else if (ObActionFlag::OP_ROW_DOES_NOT_EXIST == cell_info->value_.get_ext())
        {
          row_ext_flag |= ROW_DOES_NOT_EXIST_FLAG;
        }
        else if (ObActionFlag::OP_NOP == cell_info->value_.get_ext())
        {
          row_ext_flag |= NOP_FLAG;
        }
        else
        {
          TBSYS_LOG(ERROR, "invalid extend value=%ld", cell_info->value_.get_ext());
          ret = OB_ERROR;
        }
      }
      else if (NODES_NUM_ <= cell_info->column_id_)
      {
        TBSYS_LOG(ERROR, "invalid column_id=%ld type=%d", cell_info->column_id_, cell_info->value_.get_type());
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        ObjNode *node = nodes_ + cell_info->column_id_;
        if (cur_version_ == node->version)
        {
          ret = node->value.apply(cell_info->value_);
          if (OB_SUCCESS != ret)
          {
            TBSYS_LOG(WARN, "apply fail dest=[%s] src=[%s] column_id=%lu",
                      print_obj(node->value), print_obj(cell_info->value_), cell_info->column_id_);
          }
        }
        else
        {
          node->version = cur_version_;
          node->column_id = cell_info->column_id_;
          node->value = cell_info->value_;
          node->next = NULL;
          if (NULL == list_)
          {
            list_ = node;
          }
          else
          {
            tail_->next = node;
          }
          // 使用tail指针 为了保证数据有序
          tail_ = node;
        }
      }
      return ret;
    }
  }
}

