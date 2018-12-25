/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 *
 * Version: 0.1: ups_mutator.cpp,v 0.1 2010/09/16 18:51:43 chuanhui Exp $
 *
 * Authors:
 *   chuanhui <rizhao.ych@taobao.com>
 *     - some work details if you want
 *
 */
#include "ob_ups_mutator.h"

namespace oceanbase
{
  namespace updateserver
  {
    using namespace oceanbase::common;


    ObUpsMutator :: ObUpsMutator()
    {
      version_ = 0;
      flag_ = NORMAL_FLAG;
      mutate_timestamp_ = 0;
      memtable_checksum_before_mutate_ = 0;
      memtable_checksum_after_mutate_ = 0;
    }

    ObUpsMutator :: ObUpsMutator(ModuleArena &arena) : mutator_(arena)
    {
      version_ = 0;
      flag_ = NORMAL_FLAG;
      mutate_timestamp_ = 0;
      memtable_checksum_before_mutate_ = 0;
      memtable_checksum_after_mutate_ = 0;
    }

    ObUpsMutator :: ~ObUpsMutator()
    {
      // empty
    }

    ObMutator& ObUpsMutator:: get_mutator()
    {
      return mutator_;
    }

    int ObUpsMutator :: set_freeze_memtable()
    {
      int err = OB_SUCCESS;

      if (NORMAL_FLAG != flag_)
      {
        TBSYS_LOG(WARN, "invalid status, flag_=%d", flag_);
        err = OB_ERROR;
      }
      else
      {
        flag_ = FREEZE_FLAG;
      }

      return err;
    }

    //add zhaoqiong [Truncate Table]:20170519*/
    int ObUpsMutator :: set_trun_flag()
    {
      int err = OB_SUCCESS;

      if (NORMAL_FLAG != flag_)
      {
        TBSYS_LOG(WARN, "invalid status, flag_=%d", flag_);
        err = OB_ERROR;
      }
      else
      {
        flag_ = TRUN_FLAG;
      }

      return err;
    }
    //add:e

    int ObUpsMutator :: set_drop_memtable()
    {
      int err = OB_SUCCESS;

      if (NORMAL_FLAG != flag_)
      {
        TBSYS_LOG(WARN, "invalid status, flag_=%d", flag_);
        err = OB_ERROR;
      }
      else
      {
        flag_ = DROP_FLAG;
      }

      return err;
    }

    int ObUpsMutator :: set_first_start()
    {
      int err = OB_SUCCESS;

      if (NORMAL_FLAG != flag_)
      {
        TBSYS_LOG(WARN, "invalid status, flag_=%d", flag_);
        err = OB_ERROR;
      }
      else
      {
        flag_ = START_FLAG;
      }

      return err;
    }

    int ObUpsMutator :: set_check_cur_version()
    {
      int err = OB_SUCCESS;

      if (NORMAL_FLAG != flag_)
      {
        TBSYS_LOG(WARN, "invalid status, flag_=%d", flag_);
        err = OB_ERROR;
      }
      else
      {
        flag_ = CHECK_CUR_VERSION_FLAG;
      }

      return err;
    }

    int ObUpsMutator :: set_check_sstable_checksum()
    {
      int err = OB_SUCCESS;

      if (NORMAL_FLAG != flag_)
      {
        TBSYS_LOG(WARN, "invalid status, flag_=%d", flag_);
        err = OB_ERROR;
      }
      else
      {
        flag_ = CHECK_SSTABLE_CHECKSUM_FLAG;
      }

      return err;
    }

    bool ObUpsMutator :: is_normal_mutator() const
    {
      return flag_ == NORMAL_FLAG;
    }

    //add zhaoqiong [Truncate Table]:20170519*/
    bool ObUpsMutator :: is_truncate_mutator() const
    {
        return flag_ == TRUN_FLAG;
    }
    //add:e
    
    bool ObUpsMutator :: is_freeze_memtable() const
    {
      return flag_ == FREEZE_FLAG;
    }

    bool ObUpsMutator :: is_drop_memtable() const
    {
      return flag_ == DROP_FLAG;
    }

    bool ObUpsMutator :: is_first_start() const
    {
      return flag_ == START_FLAG;
    }

    bool ObUpsMutator :: is_check_cur_version() const
    {
      return flag_ == CHECK_CUR_VERSION_FLAG;
    }

    bool ObUpsMutator :: is_check_sstable_checksum() const
    {
      return flag_ == CHECK_SSTABLE_CHECKSUM_FLAG;
    }

    int32_t ObUpsMutator :: get_flag() const
    {
      return flag_;
    }


    //add shili [LONG_TRANSACTION_LOG]  20160926:b
    /*@berif 分配ups mutator 大小空间，并序列化到 buf中
     *@param  buf [out]  分配空间 的首地址
     *@parma  size  [in]  分配空间的大小
     */
    int ObUpsMutator :: pre_serialize(char *buf, const int64_t &size)
    {
      int err = OB_SUCCESS;
      int64_t pos = 0;
      if (OB_SUCCESS != (err = serialize(buf, size, pos)))
      {
        TBSYS_LOG(ERROR, "serialize(%p[%ld])=>%d", buf, size, err);
      }
      return err;
    }
    //add e


    void ObUpsMutator :: set_mutate_timestamp(const int64_t timestamp)
    {
      mutate_timestamp_ = timestamp;
    }

    int64_t ObUpsMutator :: get_mutate_timestamp() const
    {
      return mutate_timestamp_;
    }

    void ObUpsMutator :: set_memtable_checksum_before_mutate(const int64_t checksum)
    {
      memtable_checksum_before_mutate_ = checksum;
    }

    int64_t ObUpsMutator :: get_memtable_checksum_before_mutate() const
    {
      return memtable_checksum_before_mutate_;
    }

    void ObUpsMutator :: set_memtable_checksum_after_mutate(const int64_t checksum)
    {
      memtable_checksum_after_mutate_ = checksum;
    }

    int64_t ObUpsMutator :: get_memtable_checksum_after_mutate() const
    {
      return memtable_checksum_after_mutate_;
    }

    void ObUpsMutator :: set_cur_major_version(const uint64_t cur_major_version)
    {
      cur_major_version_ = cur_major_version;
    }

    uint64_t ObUpsMutator :: get_cur_major_version() const
    {
      return cur_major_version_;
    }

    void ObUpsMutator :: set_cur_minor_version(const uint64_t cur_minor_version)
    {
      cur_minor_version_ = cur_minor_version;
    }

    uint64_t ObUpsMutator :: get_cur_minor_version() const
    {
      return cur_minor_version_;
    }

    void ObUpsMutator :: set_last_bypass_checksum(const uint64_t last_bypass_checksum)
    {
      last_bypass_checksum_ = last_bypass_checksum;
    }

    uint64_t ObUpsMutator :: get_last_bypass_checksum() const
    {
      return last_bypass_checksum_;
    }

    void ObUpsMutator :: set_sstable_checksum(const uint64_t sstable_checksum)
    {
      sstable_checksum_ = sstable_checksum;
    }

    uint64_t ObUpsMutator :: get_sstable_checksum() const
    {
      return sstable_checksum_;
    }

    void ObUpsMutator :: reset_iter()
    {
      mutator_.reset_iter();
    }

    int ObUpsMutator :: next_cell()
    {
      return mutator_.next_cell();
    }

    int ObUpsMutator :: get_cell(ObMutatorCellInfo** cell)
    {
      return mutator_.get_cell(cell);
    }

    int ObUpsMutator :: get_cell(ObMutatorCellInfo** cell, bool *is_row_changed)
    {
      return mutator_.get_cell(cell, is_row_changed, NULL);
    }

    int ObUpsMutator :: serialize_header(char* buf, const int64_t buf_len, int64_t& pos) const
    {
      int err = OB_SUCCESS;
      if (NULL == buf || buf_len <= 0 || pos >= buf_len)
      {
        TBSYS_LOG(WARN, "invalid param, buf=%p, buf_len=%ld, pos=%ld", buf, buf_len, pos);
        err = OB_INVALID_ARGUMENT;
      }
      else if ((pos + 2 * (int64_t) sizeof(int32_t) + 2 * (int64_t) sizeof(int64_t)) >= buf_len)
      {
        TBSYS_LOG(WARN, "buf is not enough, pos=%ld, buf_len=%ld", pos, buf_len);
        err = OB_ERROR;
      }
      else
      {
        *(reinterpret_cast<int32_t*> (buf + pos)) = version_;
        pos += sizeof(int32_t);
        *(reinterpret_cast<int32_t*> (buf + pos)) = flag_;
        pos += sizeof(int32_t);
        *(reinterpret_cast<int64_t*> (buf + pos)) = mutate_timestamp_;
        pos += sizeof(int64_t);
        *(reinterpret_cast<int64_t*> (buf + pos)) = memtable_checksum_before_mutate_;
        pos += sizeof(int64_t);
        *(reinterpret_cast<int64_t*> (buf + pos)) = memtable_checksum_after_mutate_;
        pos += sizeof(int64_t);
      }
      return err;
    }

    int ObUpsMutator :: serialize(char* buf, const int64_t buf_len, int64_t& pos) const
    {
      int err = OB_SUCCESS;

      if (NULL == buf || buf_len <= 0 || pos >= buf_len)
      {
        TBSYS_LOG(WARN, "invalid param, buf=%p, buf_len=%ld, pos=%ld", buf, buf_len, pos);
        err = OB_INVALID_ARGUMENT;
      }
      else if (OB_SUCCESS != (err = serialize_header(buf, buf_len, pos)))
      {
        TBSYS_LOG(WARN, "serialize_header(buf=%p[%ld], pos=%ld)=>%d", buf, buf_len, pos, err);
      }
      else if (OB_SUCCESS != (err = mutator_.serialize(buf, buf_len, pos)))
      {
        TBSYS_LOG(WARN, "failed to serialize mutator, err=%d", err);
      }

      return err;
    }

    int ObUpsMutator :: deserialize_header(const char* buf, const int64_t buf_len, int64_t& pos)
    {
      int err = OB_SUCCESS;
      if (NULL == buf || buf_len <= 0 || pos >= buf_len)
      {
        TBSYS_LOG(WARN, "invalid param, buf=%p, buf_len=%ld, pos=%ld", buf, buf_len, pos);
        err = OB_INVALID_ARGUMENT;
      }
      else
      {
        version_ = *(reinterpret_cast<const int32_t*> (buf + pos));
        pos += sizeof(int32_t);
        flag_ = *(reinterpret_cast<const int32_t*> (buf + pos));
        pos += sizeof(int32_t);
        mutate_timestamp_ = *(reinterpret_cast<const int64_t*> (buf + pos));
        pos += sizeof(int64_t);
        memtable_checksum_before_mutate_ = *(reinterpret_cast<const int64_t*> (buf + pos));
        pos += sizeof(int64_t);
        memtable_checksum_after_mutate_ = *(reinterpret_cast<const int64_t*> (buf + pos));
        pos += sizeof(int64_t);
      }
      return err;
    }

    int ObUpsMutator :: deserialize(const char* buf, const int64_t buf_len, int64_t& pos)
    {
      int err = OB_SUCCESS;

      if (NULL == buf || buf_len <= 0 || pos >= buf_len)
      {
        TBSYS_LOG(WARN, "invalid param, buf=%p, buf_len=%ld, pos=%ld", buf, buf_len, pos);
        err = OB_INVALID_ARGUMENT;
      }
      else if (OB_SUCCESS != (err = deserialize_header(buf, buf_len, pos)))
      {
        TBSYS_LOG(ERROR, "deserialize_header(buf=%p[%ld], pos=%ld)=>%d", buf, buf_len, pos, err);
      }
      else if (OB_SUCCESS != (err = mutator_.deserialize(buf, buf_len, pos)))
      {
        TBSYS_LOG(WARN, "failed to deserialize mutator, err=%d", err);
      }

      return err;
    }

    int64_t ObUpsMutator :: get_serialize_size(void) const
    {
      return mutator_.get_serialize_size() + 2 * sizeof(int32_t) + 3 * sizeof(int64_t);
    }

    void ObUpsMutator :: clear()
    {
      version_ = 0;
      flag_ = NORMAL_FLAG;
      mutate_timestamp_ = 0;
      memtable_checksum_before_mutate_ = 0;
      memtable_checksum_after_mutate_ = 0;
      mutator_.clear();
    }

    void ObUpsMutator :: reset()
    {
      version_ = 0;
      flag_ = NORMAL_FLAG;
      mutate_timestamp_ = 0;
      memtable_checksum_before_mutate_ = 0;
      memtable_checksum_after_mutate_ = 0;
      mutator_.reset();
    }
  }
}

