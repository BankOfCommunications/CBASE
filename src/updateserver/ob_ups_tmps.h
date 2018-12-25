////===================================================================
 //
 // ob_ups_tmps.h / hash / common / Oceanbase
 //
 // Copyright (C) 2010 Taobao.com, Inc.
 //
 // Created on 2010-10-13 by Yubai (yubai.lk@taobao.com) 
 //
 // -------------------------------------------------------------------
 //
 // Description
 //
 //
 // -------------------------------------------------------------------
 // 
 // Change Log
 //
////====================================================================

#ifndef  OCEANBASE_UPDATESERVER_UPS_TMPS_H_
#define  OCEANBASE_UPDATESERVER_UPS_TMPS_H_
#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <pthread.h>
#include <new>
#include <algorithm>
#include "common/ob_define.h"
#include "common/ob_read_common_data.h"
#include "common/ob_object.h"
#include "common/hash/ob_hashmap.h"
#include "common/ob_tsi_factory.h"
#include "common/ob_trace_log.h"
#include "ob_table_engine.h"
#include "ob_ups_stat.h"
#include "ob_update_server_main.h"

namespace oceanbase
{
  namespace updateserver
  {
    using namespace common;

    typedef hash::ObHashMap<uint64_t, ObCellInfoNode*> column_map_t;
    static __thread column_map_t *column_map = NULL;

    template <class Allocator>
    bool merge(const TEKey &te_key, TEValue &te_value, Allocator &allocator, int64_t stop_trans_id = INT64_MAX)
    {
      typedef column_map_t::iterator column_map_iterator_t;
      int64_t timeu = tbsys::CTimeUtil::getTime();

      bool bret = true;
      if (NULL == column_map)
      {
        column_map = GET_TSI_MULT(column_map_t, TSI_UPS_COLUMN_MAP_1);
        if (NULL == column_map)
        {
          TBSYS_LOG(ERROR, "get tsi column map fail");
          bret = false;
        }
        else
        {
          column_map->create(common::OB_MAX_COLUMN_NUMBER);
        }
      }
      else
      {
        column_map->clear();
      }

      TEValue new_value = te_value;
      new_value.list_head = NULL;
      new_value.list_tail = NULL;
      new_value.cell_info_cnt = 0;
      new_value.cell_info_size = 0;
      int16_t cell_info_cnt = te_value.cell_info_cnt;
      int16_t cell_info_size = te_value.cell_info_size;
      ObCellInfoNode *iter = te_value.list_head;
      while (bret
            && te_value.create_time < stop_trans_id)
      {
        bret = false;
        if (NULL == iter)
        {
          TBSYS_LOG(ERROR, "te_value list iter null pointer");
          break;
        }
        if (ObModifyTimeType == iter->cell_info.value_.get_type())
        {
          int64_t v = 0;
          iter->cell_info.value_.get_modifytime(v);
          if (v >= stop_trans_id)
          {
            bret = true;
            break;
          }
        }
        if (common::ObExtendType == iter->cell_info.value_.get_type())
        {
          if (common::ObActionFlag::OP_DEL_ROW == iter->cell_info.value_.get_ext())
          {
            ObCellInfoNode *new_node = (ObCellInfoNode*)allocator.alloc(sizeof(ObCellInfoNode));
            if (NULL != new_node)
            {
              *new_node = *iter;
              new_node->next = NULL;
              new_value.cell_info_cnt = 1;
              new_value.cell_info_size = static_cast<int16_t>(new_node->size());
              new_value.list_head = new_node;
              new_value.list_tail = new_node;
              bret = true;
              cell_info_cnt -= 1;
              cell_info_size -= iter->size();
            }
            column_map->clear();
          }
          else
          {
            TBSYS_LOG(ERROR, "invalid extend type cellinfo %s %s",
                      te_value.log_str(), print_obj(iter->cell_info.value_));
          }
        }
        else
        {
          ObCellInfoNode *cur_node = NULL;
          int hash_ret = column_map->get(iter->cell_info.column_id_, cur_node);
          if (common::hash::HASH_NOT_EXIST == hash_ret)
          {
            ObCellInfoNode *new_node = (ObCellInfoNode*)allocator.alloc(sizeof(ObCellInfoNode));
            if (NULL != new_node)
            {
              if (common::hash::HASH_INSERT_SUCC == column_map->set(iter->cell_info.column_id_, new_node, 0))
              {
                *new_node = *iter;
                bret = true;
                cell_info_cnt -= 1;
                cell_info_size -= iter->size();
              }
              new_node->next = NULL;
            }
          }
          else if (common::hash::HASH_EXIST == hash_ret)
          {
            if (NULL != cur_node)
            {
              if (common::OB_SUCCESS == cur_node->cell_info.value_.apply(iter->cell_info.value_))
              {
                bret = true;
                cell_info_cnt -= 1;
                cell_info_size -= iter->size();
              }
              else
              {
                TBSYS_LOG(ERROR, "apply cellinfo fail %s %s %s",
                          te_value.log_str(), print_obj(cur_node->cell_info.value_), print_obj(iter->cell_info.value_));
              }
            }
          }
          else
          {
            // do nothing
          }
        }

        iter = iter->next;
        if (NULL == iter)
        {
          break;
        }
      } // while

      if (bret)
      {
        column_map_iterator_t map_iter;
        for (map_iter = column_map->begin(); map_iter != column_map->end(); ++map_iter)
        {
          if (NULL != map_iter->second)
          {
            if (NULL == new_value.list_head)
            {
              new_value.list_head = map_iter->second;
              new_value.list_tail = map_iter->second;
              new_value.cell_info_cnt++;
              new_value.cell_info_size = static_cast<int16_t>(new_value.cell_info_size + map_iter->second->size());
            }
            else
            {
              new_value.list_tail->next = map_iter->second;
              new_value.list_tail = map_iter->second;
              new_value.cell_info_cnt++;
              new_value.cell_info_size = static_cast<int16_t>(new_value.cell_info_size + map_iter->second->size());
            }
          }
          else
          {
            bret = false;
          }
        }
      }

      timeu = tbsys::CTimeUtil::getTime() - timeu;
      if (bret)
      {
        // 将不能merge的cell链接道merge过的链表尾部
        if (NULL != new_value.list_head)
        {
          new_value.list_tail->next = iter;
          if (NULL != iter)
          {
            new_value.list_tail = te_value.list_tail;
          }
          new_value.cell_info_cnt += cell_info_cnt;
          new_value.cell_info_size += cell_info_size;
          TBSYS_TRACE_LOG("merge te_value succ [%s] [%s] ==> [%s] cell_info_cnt=%hd cell_info_size=%hdkb timeu=%ld",
                          te_key.log_str(), te_value.log_str(), new_value.log_str(), cell_info_cnt, cell_info_size, timeu);
          te_value = new_value;
        }
      }
      else
      {
        TBSYS_LOG(ERROR, "merge te_value fail %s", te_value.log_str());
      }

      OB_STAT_INC(UPDATESERVER, UPS_STAT_MERGE_COUNT, 1);
      OB_STAT_INC(UPDATESERVER, UPS_STAT_MERGE_TIMEU, timeu);

      return bret;
    }
  }
}

#endif //OCEANBASE_UPDATESERVER_UPS_TMPS_H_

