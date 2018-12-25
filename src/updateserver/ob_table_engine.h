////===================================================================
 //
 // ob_table_engine.h / hash / common / Oceanbase
 //
 // Copyright (C) 2010, 2011 Taobao.com, Inc.
 //
 // Created on 2010-09-09 by Yubai (yubai.lk@taobao.com) 
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

#ifndef  OCEANBASE_UPDATESERVER_TABLE_ENGINE_H_
#define  OCEANBASE_UPDATESERVER_TABLE_ENGINE_H_
#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <pthread.h>
#include <new>
#include <algorithm>
#include "common/ob_define.h"
#include "common/murmur_hash.h"
#include "common/ob_read_common_data.h"
#include "common/utility.h"
#include "common/ob_crc64.h"
#include "common/qlock.h"
#include "ob_ups_utils.h"
#include "ob_ups_compact_cell_iterator.h"
#include "ob_session_mgr.h"

namespace oceanbase
{
  namespace updateserver
  {
    const int64_t CELL_INFO_SIZE_UNIT = 1024L;

    struct ObCellInfoNode
    {
      int64_t modify_time;
      ObCellInfoNode *next;
      char buf[0];

      std::pair<int64_t, int64_t> get_size_and_cnt() const;
    };

    struct ObUndoNode
    {
      ObCellInfoNode *head;
      ObUndoNode *next;
    };

    class ObCellInfoNodeIterable
    {
      protected:
        struct CtrlNode
        {
          uint64_t column_id;
          common::ObObj value;
          CtrlNode *next;
        };
      public:
        ObCellInfoNodeIterable();
        virtual ~ObCellInfoNodeIterable();
      public:
        int next_cell();
        int get_cell(uint64_t &column_id, common::ObObj &value);
        void reset();
      public:
        void set_cell_info_node(const ObCellInfoNode *cell_info_node);
        void set_mtime(const uint64_t column_id, const ObModifyTime value);
        void set_rne();
        void set_head();
      //add duyr [Delete_Update_Function_isolation] [JHOBv0.1] 20160531:b
        void set_data_mark_mtime(const uint64_t column_id,const int64_t value);
        void set_data_mark_major_ver(const uint64_t column_id,const int64_t value);
        void set_data_mark_minor_ver_start(const uint64_t column_id,const int64_t value);
        void set_data_mark_minor_ver_end(const uint64_t column_id,const int64_t value);
        void set_data_mark_data_store_type(const uint64_t column_id,const int64_t value);
      private:
        void reset_data_mark_nodes();
      //add duyr 20160531:e
      private:
        const ObCellInfoNode *cell_info_node_;
        bool is_iter_end_;
        ObUpsCompactCellIterator cci_;
        CtrlNode *ctrl_list_;
        CtrlNode head_node_;
        CtrlNode rne_node_;
        CtrlNode mtime_node_;
        //add duyr [Delete_Update_Function_isolation] [JHOBv0.1] 20160531:b
        CtrlNode data_mark_mtime_node_;
        CtrlNode data_mark_major_version_node_;
        CtrlNode data_mark_minor_ver_start_node_;
        CtrlNode data_mark_minor_ver_end_node_;
        CtrlNode data_mark_data_store_type_node_;
        //add duyr 20160531:e
    };

    class ObCellInfoNodeIterableWithCTime : public ObCellInfoNodeIterable, public RowkeyInfoCache
    {
      public:
        ObCellInfoNodeIterableWithCTime();
        virtual ~ObCellInfoNodeIterableWithCTime();
      public:
        int next_cell();
        int get_cell(uint64_t &column_id, common::ObObj &value);
        void reset();
        bool get_rdel_cell(); //add zhaoqiong [Truncate table] 20170519
        void reset_all(); //add zhaoqiong [Truncate table] 20170519
      public:
        void set_ctime_column_id(const uint64_t column_id);
        void set_rdel(); //add zhaoqiong [Truncate table] 20170519
        void set_rowkey_column(const uint64_t table_id, const common::ObRowkey &row_key);
        void set_nop();
      private:
        int inner_err;
        bool is_iter_end_;
        bool append_trun_; //add zhaoqiong [Truncate table] 20170519
        CtrlNode *ext_list_;
        CtrlNode *trun_node_; //add zhaoqiong [Truncate table] 20170519
        CtrlNode *list_iter_;
        uint64_t column_id_;
        common::ObObj value_;
        CtrlNode ctime_node_;
        CtrlNode rdel_node_; //add zhaoqiong [Truncate table] 20170519
        CtrlNode rowkey_node_[OB_MAX_ROWKEY_COLUMN_NUMBER];
        CtrlNode nop_node_;
    };

#define __USING_FORMED_ROWKEY__
#ifdef __USING_FORMED_ROWKEY__
    typedef common::ObRowkey UpsRowKey;
#else
    typedef common::ObString UpsRowKey;
#endif

    struct TEHashKey
    {
      uint32_t table_id;
      int32_t rk_length;
      const ObObj *row_key;

      TEHashKey() : table_id(UINT32_MAX),
                    rk_length(0),
                    row_key(NULL)
      {
      };
      inline int64_t hash() const
      {
        common::ObRowkey rk(const_cast<ObObj*>(row_key), rk_length);
        //return (rk.murmurhash2(0) + table_id);
        return (rk.murmurhash64A(0) + table_id);
      };
      inline bool operator == (const TEHashKey &other) const
      {
        bool bret = false;
        if (table_id == other.table_id
            && other.rk_length == rk_length)
        {
          bret = true;
          for (int64_t i = 0; i < rk_length && bret; i++)
          {
            if (common::ObNullType == row_key[i].get_type()
                || common::ObNullType == other.row_key[i].get_type())
            {
              bret = (common::ObNullType == row_key[i].get_type()) && (common::ObNullType == other.row_key[i].get_type());
            }
            else
            {
              bret = (0 == row_key[i].compare_same_type(other.row_key[i]));
            }
          }
        }
        return bret;
      };
    };
    
    struct TEKey
    {
      uint64_t table_id;
      UpsRowKey row_key;

      TEKey(const uint64_t table_id, const UpsRowKey& row_key): table_id(table_id), row_key(row_key)
      {}
      //add zhaoqiong [Truncate Table]:20160318
      TEKey(const uint64_t table_id): table_id(table_id), row_key()
      {}
      //add:e
      TEKey() : table_id(common::OB_INVALID_ID), row_key()
      {
      };
      inline int64_t hash() const
      {
        //return row_key.murmurhash2(0) + table_id;
        return row_key.murmurhash64A(0) + table_id;
      };
      inline void checksum(common::ObBatchChecksum &bc) const
      {
        bc.fill(&table_id, sizeof(table_id));
        row_key.checksum(bc);
      };
      inline const char *log_str() const
      {
        static const int32_t BUFFER_SIZE = 2048;
        static __thread char BUFFER[2][BUFFER_SIZE];
        static __thread uint64_t i = 0;
        snprintf(BUFFER[i % 2], BUFFER_SIZE, "table_id=%lu rowkey=[%s] rowkey_ptr=%p rowkey_length=%ld",
                table_id, oceanbase::common::to_cstring(row_key), row_key.ptr(), row_key.length());
        return BUFFER[i++ % 2];
      };
      inline const char * to_cstring() const
      {
        return log_str();
      }
      inline bool operator == (const TEKey &other) const
      {
        return (table_id == other.table_id
                && row_key == other.row_key);
      };
      inline bool operator != (const TEKey &other) const
      {
        return (table_id != other.table_id
                || row_key != other.row_key);
      };
      inline int operator - (const TEKey &other) const
      {
        int ret = 0;
        if (table_id > other.table_id)
        {
          ret = 1;
        }
        else if (table_id < other.table_id)
        {
          ret = -1;
        }
        else
        { 
          if (row_key > other.row_key)
          {
            ret = 1;
          }
          else if (row_key < other.row_key)
          {
            ret = -1;
          }
          else
          {
            ret = 0;
          }
        }
        return ret;
      };
    };

    static const uint8_t IST_NO_INDEX = 0x0;
    static const uint8_t IST_HASH_INDEX = 0x1;
    static const uint8_t IST_BTREE_INDEX = 0x2;

    struct TEValueUCInfo;
    struct TEValue
    {
      //add dyr [NPU-2015009-cursor] [NPU-OB-009] 20150319:b
      enum LockFlag
      {
          UNABLE_FORCE_UNLOCK = 1,
          ABLE_FORCE_UNLOCK   = 2
      };
      //add dyr 20150319:e
      ObUndoNode *undo_list; // tevalue的bit copy必须先复制undolist
      uint8_t index_stat;
      int16_t cell_info_cnt;
      int16_t cell_info_size; // 单位为1K
      ObCellInfoNode *list_head;
      ObCellInfoNode *list_tail;
      TEValueUCInfo *cur_uc_info;
      //add dyr [NPU-2015009-cursor] [NPU-OB-009] 20150319:b
      //标示当前value的互斥锁能否被强制解锁,必须先获得互斥锁再修改此变量
      LockFlag  lock_flag;
      //add dyr 20150319:e
      QLock row_lock;

      TEValue()
      {
        reset();
      };
      inline void reset()
      {
        index_stat = IST_NO_INDEX;
        cell_info_cnt = 0;
        cell_info_size = 0;
        list_head = NULL;
        list_tail = NULL;
        cur_uc_info = NULL;
        row_lock.reset();
        undo_list = NULL;
        //add dyr [NPU-2015009-cursor] [NPU-OB-009] 20150319:b
        lock_flag = UNABLE_FORCE_UNLOCK;
        //add dyr 20150319:e
      };
      inline const char *log_str() const
      {
        static const int32_t BUFFER_SIZE = 2048;
        static __thread char BUFFER[2][BUFFER_SIZE];
        static __thread uint64_t i = 0;
        snprintf(BUFFER[i % 2], BUFFER_SIZE, "index_stat=%hhu cell_info_cnt=%hd cell_info_size=%hdKB "
                "list_head=%p list_tail=%p cur_uc_info=%p lock_uid=%x lock_nref=%u undo_list=%p",
                 index_stat, cell_info_cnt, cell_info_size,
                 list_head, list_tail, cur_uc_info,
                 row_lock.uid_, row_lock.n_ref_, undo_list);
        return BUFFER[i++ % 2];
      };
      inline const char *log_list()
      {
        static const int32_t BUFFER_SIZE = 2048;
        static __thread char BUFFER[2][BUFFER_SIZE];
        static __thread uint64_t i = 0;
        char *buffer = BUFFER[i++ % 2];
        int64_t pos = snprintf(buffer, BUFFER_SIZE, "list_head=%p ", list_head);
        ObCellInfoNode *iter = list_head;
        while (NULL != iter
              && pos < BUFFER_SIZE)
        {
          pos += snprintf(buffer + pos, BUFFER_SIZE - pos, "next=%p ", iter);
          iter = iter->next;
        }
        if (pos < BUFFER_SIZE)
        {
          snprintf(buffer + pos, BUFFER_SIZE - pos, "list_tail=%p", list_tail);
        }
        return buffer;
      };
      inline const char * to_cstring() const
      {
        return log_str();
      }
      inline bool is_empty() const
      {
        return (NULL == list_head);
      };
      int64_t get_cur_version() const
      {
        return list_tail? list_tail->modify_time: -1;
      }

      //add dyr [NPU-2015009-cursor] [NPU-OB-009] 20150319:b
      //必须先获得互斥锁
      int  set_able_force_unlock()
      {
          int ret = OB_SUCCESS;
          if (lock_flag >= UNABLE_FORCE_UNLOCK && lock_flag <= ABLE_FORCE_UNLOCK)
          {
              lock_flag = ABLE_FORCE_UNLOCK;
          }
          else
          {
              TBSYS_LOG(ERROR,"unkown lock flag = %d",lock_flag);
              ret = OB_ERROR;
          }
          return ret;
      }

      //必须先获得互斥锁
      int  set_unable_force_unlock()
      {
          int ret = OB_SUCCESS;
          if (lock_flag >= UNABLE_FORCE_UNLOCK && lock_flag <= ABLE_FORCE_UNLOCK)
          {
              lock_flag = UNABLE_FORCE_UNLOCK;
          }
          else
          {
              TBSYS_LOG(ERROR,"unkown lock flag = %d",lock_flag);
              ret = OB_ERROR;
          }
          return ret;
      }

      //必须先获得互斥锁
      bool is_able_force_unlock()
      {
          return (ABLE_FORCE_UNLOCK == lock_flag);
      }

      //add dyr 20150319:e
    };

    class TEValueStmtCallback : public ISessionCallback
    {
      public:
        TEValueStmtCallback() {};
        ~TEValueStmtCallback() {};
      public:
        int cb_func(const bool rollback, void *data, BaseSessionCtx &session);
    };

    class TEValueSessionCallback : public ISessionCallback
    {
      public:
        TEValueSessionCallback() {};
        ~TEValueSessionCallback() {};
      public:
        int cb_func(const bool rollback, void *data, BaseSessionCtx &session);
    };

    class MemTable;
    class TransSessionCallback : public ISessionCallback
    {
      public:
        TransSessionCallback() {};
        ~TransSessionCallback() {};
      public:
        int cb_func(const bool rollback, void *data, BaseSessionCtx &session);
    };

    enum TETransType
    {
      INVALID_TRANSACTION = 0,
      READ_TRANSACTION = 1,
      WRITE_TRANSACTION = 2,
    };

    extern bool get_key_prefix(const TEKey &te_key, TEKey &prefix_key);
  }
}

#endif //OCEANBASE_UPDATESERVER_TABLE_ENGINE_H_

