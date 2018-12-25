/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_row_store.cpp
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#include "ob_row_store.h"
#include "ob_row_util.h"
#include "common/ob_compact_cell_writer.h"
#include "utility.h"
using namespace oceanbase::common;

////////////////////////////////////////////////////////////////
struct ObRowStore::BlockInfo
{
  int64_t magic_;
  BlockInfo *next_block_;
  /**
   * cur_data_pos_ must be set when BlockInfo deserialized
   */
  int64_t curr_data_pos_;
  int64_t block_size_;
  char data_[0];

  BlockInfo()
    :magic_(0xabcd4444abcd4444), next_block_(NULL), curr_data_pos_(0)
  {
  }
  inline int64_t get_remain_size() const
  {
    return block_size_ - curr_data_pos_ - sizeof(BlockInfo);
  }
  inline int64_t get_remain_size_for_read(int64_t pos) const
  {
    TBSYS_LOG(DEBUG, "cur=%ld, pos=%ld, remain=%ld", curr_data_pos_, pos, curr_data_pos_ - pos);
    return curr_data_pos_ - pos;
  }
  inline char* get_buffer()
  {
    return data_ + curr_data_pos_;
  }
  inline const char* get_buffer_head() const
  {
    return data_;
  }
  inline void advance(const int64_t length)
  {
    curr_data_pos_ += length;
  }
  inline int64_t get_curr_data_pos()
  {
    return curr_data_pos_;
  }
  inline void set_curr_data_pos(int64_t pos)
  {
    curr_data_pos_ = pos;
  }
  inline bool is_fresh_block()
  {
    return (get_curr_data_pos() == 0);
  }
};

ObRowStore::iterator::iterator()
  : row_store_(NULL),
    cur_iter_pos_(0),
    cur_iter_block_(NULL),
    got_first_next_(false)
{
}

ObRowStore::iterator::iterator(const iterator & r)
  : row_store_(r.row_store_),
    cur_iter_pos_(r.cur_iter_pos_),
    cur_iter_block_(r.cur_iter_block_),
    got_first_next_(r.got_first_next_)
{
}

ObRowStore::iterator::iterator(ObRowStore * row_store, int64_t cur_iter_pos,
    BlockInfo * cur_iter_block, bool got_first_next)
  : row_store_(row_store),
    cur_iter_pos_(cur_iter_pos),
    cur_iter_block_(cur_iter_block),
    got_first_next_(got_first_next)
{
}

int ObRowStore::iterator::get_next_row(const ObRowkey *&rowkey, ObRow &row, common::ObString *compact_row /* = NULL */)
{
  int ret = OB_SUCCESS;
  ret = get_next_row(&cur_rowkey_, cur_rowkey_obj_, row, compact_row);
  if(OB_SUCCESS == ret)
  {
    rowkey = &cur_rowkey_;
  }
  else if(OB_ITER_END != ret)
  {
    TBSYS_LOG(WARN, "get next row fail:ret[%d]", ret);
  }
  return ret;
}

int ObRowStore::iterator::get_next_ups_row(const ObRowkey *&rowkey, ObUpsRow &row, common::ObString *compact_row /* = NULL */)
{
  int ret = OB_SUCCESS;
  ret = get_next_row(rowkey, row, compact_row);
  if(OB_SUCCESS != ret && OB_ITER_END != ret)
  {
    TBSYS_LOG(WARN, "get next ups row fail:ret[%d]", ret);
  }
  return ret;
}

int ObRowStore::iterator::get_next_ups_row(ObUpsRow &row, common::ObString *compact_row /* = NULL */)
{
  return get_next_row(NULL, NULL, row, compact_row);
}

int ObRowStore::iterator::get_next_row(ObRow &row, common::ObString *compact_row /* = NULL */)
{
  return get_next_row(NULL, NULL, row, compact_row);
}

int ObRowStore::iterator::get_next_row(ObRowkey *rowkey, ObObj *rowkey_obj, ObRow &row, common::ObString *compact_row)
{
  int ret = OB_SUCCESS;
  const StoredRow *stored_row = NULL;

  if (NULL == row_store_)
  {
    TBSYS_LOG(ERROR, "row_store_ is NULL, should not reach here");
    ret = OB_ERROR;
  }

  if (OB_SUCCESS == ret && !got_first_next_)
  {
    cur_iter_block_ = row_store_->block_list_tail_;
    cur_iter_pos_ = 0;
    got_first_next_ = true;
  }

  if (OB_SUCCESS == ret && OB_ITER_END == (ret = next_iter_pos(cur_iter_block_, cur_iter_pos_)))
  {
    TBSYS_LOG(DEBUG, "iter end.block=%p, pos=%ld", cur_iter_block_, cur_iter_pos_);
  }
  else if (OB_SUCCESS == ret)
  {
    const char *buffer = cur_iter_block_->get_buffer_head() + cur_iter_pos_;
    stored_row = reinterpret_cast<const StoredRow *>(buffer);
    cur_iter_pos_ += (row_store_->get_reserved_cells_size(stored_row->reserved_cells_count_)
        + stored_row->compact_row_size_);
    //TBSYS_LOG(DEBUG, "stored_row->reserved_cells_count_=%d, stored_row->compact_row_size_=%d, sizeof(ObObj)=%lu, next_pos_=%ld",
    //stored_row->reserved_cells_count_, stored_row->compact_row_size_, sizeof(ObObj), cur_iter_pos_);

    if (OB_SUCCESS == ret && NULL != stored_row)
    {
      ObUpsRow *ups_row = dynamic_cast<ObUpsRow*>(&row);
      if (NULL != ups_row)
      {
        const ObRowDesc *row_desc = row.get_row_desc();
        uint64_t table_id = OB_INVALID_ID;
        uint64_t column_id = OB_INVALID_ID;

        if(NULL == row_desc)
        {
          ret = OB_INVALID_ARGUMENT;
          TBSYS_LOG(WARN, "row should set row desc first");
        }
        else if(OB_SUCCESS != (ret = row_desc->get_tid_cid(0, table_id, column_id)))
        {
          TBSYS_LOG(WARN, "get tid cid fail:ret[%d]", ret);
        }
        else if (OB_SUCCESS != (ret = ObUpsRowUtil::convert(table_id, stored_row->get_compact_row(), *ups_row, rowkey, rowkey_obj)))
        {
          TBSYS_LOG(WARN, "fail to convert compact row to ObUpsRow. ret=%d", ret);
        }
      }
      else
      {
        if(OB_SUCCESS != (ret = ObRowUtil::convert(stored_row->get_compact_row(), row, rowkey, rowkey_obj
              , row_store_->is_update_second_index_ //add lbzhong [Update rowkey] 20151221:b:e
              )))
        {
          TBSYS_LOG(WARN, "fail to convert compact row to ObRow:ret[%d]", ret);
        }
      }
      if (OB_SUCCESS == ret && NULL != compact_row)
      {
        *compact_row = stored_row->get_compact_row();
      }
    }
    else
    {
      TBSYS_LOG(WARN, "fail to get next row. stored_row=%p, ret=%d", stored_row, ret);
    }
  }

  return ret;
}

int ObRowStore::iterator::next_iter_pos(BlockInfo *&iter_block, int64_t &iter_pos)
{
  int ret = OB_SUCCESS;
  if (NULL == iter_block)
  {
    ret = OB_ITER_END;
  }
  else
  {
    while (0 >= iter_block->get_remain_size_for_read(iter_pos))
    {
      iter_block = iter_block->next_block_;
      iter_pos = 0;
      if (NULL == iter_block)
      {
        ret = OB_ITER_END;
        break;
      }
    }
  }
  return ret;
}


////////////////////////////////////////////////////////////////
ObRowStore::ObRowStore(const int32_t mod_id/*=ObModIds::OB_SQL_ROW_STORE*/, ObIAllocator* allocator/*=NULL*/)
  :allocator_(allocator ?: &get_global_tsi_block_allocator()),
   block_list_head_(NULL), block_list_tail_(NULL),
   block_count_(0), cur_size_counter_(0),
   rollback_iter_pos_(-1), rollback_block_list_(NULL),
   mod_id_(mod_id), block_size_(NORMAL_BLOCK_SIZE), is_read_only_(false),
   inner_(this, 0, block_list_head_, false)
   , is_update_second_index_(false) //add lbzhong [Update rowkey] 20151221:b:e
{
}

ObRowStore::~ObRowStore()
{
  clear();
}
ObRowStore *ObRowStore::clone(char *buffer) const
{
  ObRowStore *copy = NULL;
  if (OB_UNLIKELY(NULL == buffer))
  {
    TBSYS_LOG(WARN, "buffer is NULL");
  }
  else
  {
    copy = reinterpret_cast<ObRowStore*>(buffer);
    copy->is_read_only_ = true;
    buffer += get_meta_size();
    BlockInfo *bip = block_list_tail_;
    BlockInfo *prev = NULL;
    BlockInfo *tmp = NULL;
    while(NULL != bip)
    {
      memcpy(buffer, bip, sizeof(BlockInfo) + bip->curr_data_pos_);
      tmp = reinterpret_cast<BlockInfo*>(buffer);
      tmp->curr_data_pos_ = bip->curr_data_pos_;
      tmp->block_size_ = tmp->curr_data_pos_ + sizeof(BlockInfo);
      if (prev != NULL)
      {
        prev->next_block_ = tmp;
      }
      else
      {
        copy->block_list_tail_ = tmp;
      }
      prev = tmp;
      copy->block_list_head_ = tmp;
      buffer += sizeof(BlockInfo) + bip->curr_data_pos_;
      bip = bip->next_block_;
    }
    tmp->next_block_ = NULL;
    // fill meta data
    copy->block_count_ = block_count_;
    copy->cur_size_counter_ = cur_size_counter_;
    copy->mod_id_ = mod_id_;
  }
  return copy;
}
void ObRowStore::set_block_size(int64_t block_size)
{
  block_size_ = block_size;
}
int32_t ObRowStore::get_copy_size() const
{
  return static_cast<int32_t>(cur_size_counter_ + sizeof(BlockInfo) * block_count_ + get_meta_size());
}
int32_t ObRowStore::get_meta_size() const
{
  return static_cast<int32_t>(sizeof(ObRowStore));
}
int ObRowStore::add_reserved_column(uint64_t tid, uint64_t cid)
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS != (ret = reserved_columns_.push_back(std::make_pair(tid, cid))))
  {
    TBSYS_LOG(WARN, "failed to push into array, err=%d", ret);
  }
  return ret;
}

void ObRowStore::reuse()
{
  BlockInfo * block = NULL;
  block_list_head_ = block_list_tail_;
  block = block_list_head_;
  while(NULL != block)
  {
    block->curr_data_pos_ = 0;
    block = block->next_block_;
  }
  cur_size_counter_ = 0;
  rollback_iter_pos_ = -1;
  rollback_block_list_ = NULL;
  reset_iterator();
  reserved_columns_.clear();
  //add lbzhong [Update rowkey] 20151221:b
  is_update_second_index_ = false;
  //add:e
}

void ObRowStore::clear()
{
  clear_rows();
  reserved_columns_.clear();
  //add lbzhong [Update rowkey] 20151221:b
  is_update_second_index_ = false;
  //add:e
}

// method for ObAggregateFunction::prepare()
// prepare need to reuse ObRowStore for WRITE,
// it needs to reuse reserved_columns_ which should not be cleared
void ObRowStore::clear_rows()
{
  while(NULL != block_list_tail_)
  {
    // using block_list_tail_ as the list iterator can prevent
    // memory leakage when there was a rollback
    BlockInfo *tmp = block_list_tail_->next_block_;
    ob_tc_free(block_list_tail_, mod_id_);
    block_list_tail_ = tmp;
  }
  block_list_head_ = NULL;
  block_count_ = 0;
  cur_size_counter_ = 0;
  rollback_iter_pos_ = -1;
  rollback_block_list_ = NULL;
  reset_iterator();
}

int ObRowStore::rollback_last_row()
{
  int ret = OB_SUCCESS;
  // only support add_row->rollback->add_row->rollback->add_row->add_row->rollback
  // NOT support  ..->rollback->rollback->...
  if (rollback_iter_pos_ < 0)
  {
    TBSYS_LOG(WARN, "only one row could be rollback after called add_row() once");
    ret = OB_NOT_SUPPORTED;
  }
  else
  {
    block_list_head_ = rollback_block_list_;
    if (NULL != block_list_head_)
    {
      block_list_head_->set_curr_data_pos(rollback_iter_pos_);
    }
    rollback_iter_pos_ = -1;
  }
  return ret;
}

int ObRowStore::new_block(int64_t block_size)
{
  int ret = OB_SUCCESS;
  // when row store was ever rolled back, block_list_head_->next_block_ may not be NULL
  // that block could be reused
  if ((NULL != block_list_head_) && (NULL != block_list_head_->next_block_))
  {
    // reuse block
    if (block_size <= block_list_head_->next_block_->block_size_)
    {
      block_list_head_ = block_list_head_->next_block_;
      block_list_head_->curr_data_pos_ = 0;
    }
    else
    {
      // create a new block
      BlockInfo *block = static_cast<BlockInfo*>(ob_tc_malloc(block_size, mod_id_));
      if (NULL == block)
      {
        TBSYS_LOG(ERROR, "no memory");
        ret = OB_ALLOCATE_MEMORY_FAILED;
      }
      else
      {
        block = new (block) BlockInfo();
        block->next_block_ = block_list_head_->next_block_;
        block_list_head_->next_block_ = block;
        block->curr_data_pos_ = 0;
        block->block_size_ = block_size;
        block_list_head_ = block;
        ++block_count_;
      }
    }
  }
  else
  {
    // create a new block
    BlockInfo *block = static_cast<BlockInfo*>(ob_tc_malloc(block_size, mod_id_));
    if (NULL == block)
    {
      TBSYS_LOG(ERROR, "no memory");
      ret = OB_ALLOCATE_MEMORY_FAILED;
    }
    else
    {
      block = new (block) BlockInfo();
      block->next_block_ = NULL;
      block->curr_data_pos_ = 0;
      block->block_size_ = block_size;
      if (NULL == block_list_tail_)
      {
        block_list_tail_ = block;  // this is the first block allocated
      }
      if (NULL == block_list_head_)
      {
        block_list_head_ = block;
      }
      else
      {
        block_list_head_->next_block_ = block;
        block_list_head_ = block;
      }
      ++block_count_;
    }
  }
  return ret;
}
int ObRowStore::new_block()
{
  return new_block(block_size_);
}

int ObRowStore::append_row(const ObRowkey *rowkey, const ObRow &row, BlockInfo &block, StoredRow &stored_row)
{
  int ret = OB_SUCCESS;

  const int64_t reserved_columns_count = reserved_columns_.count();
  ObCompactCellWriter cell_writer;

  if(NULL == rowkey)
  {
    cell_writer.init(block.get_buffer(), block.get_remain_size(), SPARSE);
  }
  else
  {
    cell_writer.init(block.get_buffer(), block.get_remain_size(), DENSE_SPARSE);
  }

  const ObObj *cell = NULL;
  uint64_t table_id = OB_INVALID_ID;
  uint64_t column_id = OB_INVALID_ID;
  ObObj cell_clone;

  const ObUpsRow *ups_row = dynamic_cast<const ObUpsRow *>(&row);
  if(OB_SUCCESS == ret && NULL != rowkey)
  {
    if(OB_SUCCESS != (ret = cell_writer.append_rowkey(*rowkey)))
    {
      TBSYS_LOG(WARN, "append rowkey fail:ret[%d]", ret);
    }
  }

  if(OB_SUCCESS == ret && NULL != ups_row)
  {
    if( ups_row->get_is_delete_row() )
    {
      if(OB_SUCCESS != (ret = cell_writer.row_delete()))
      {
        TBSYS_LOG(WARN, "append row delete flag fail:ret[%d]", ret);
      }
    }
  }

  int64_t ext_value = 0;

  for (int64_t i = 0; OB_SUCCESS == ret && i < row.get_column_num(); ++i)
  {
    if (OB_SUCCESS != (ret = row.raw_get_cell(i, cell, table_id, column_id)))
    {
      TBSYS_LOG(WARN, "failed to get cell, err=%d", ret);
      break;
    }
    if (OB_SUCCESS == ret)
    {
      if (ObExtendType == cell->get_type())
      {
        if(OB_SUCCESS != (ret = cell->get_ext(ext_value)))
        {
          TBSYS_LOG(WARN, "get ext value fail:ret[%d]", ret);
        }
        else if (ObActionFlag::OP_VALID == ext_value)
        {
          if (OB_SUCCESS != (ret = cell_writer.append_escape(ObCellMeta::ES_VALID)))
          {
            TBSYS_LOG(WARN, "fail to append escape:ret[%d]", ret);
          }
        }
        else if (ObActionFlag::OP_ROW_DOES_NOT_EXIST == ext_value)
        {
          if (OB_SUCCESS != (ret = cell_writer.append_escape(ObCellMeta::ES_NOT_EXIST_ROW)))
          {
            TBSYS_LOG(WARN, "fail to append escape:ret[%d]", ret);
          }
        }
        else if(ObActionFlag::OP_NOP != ext_value)
        {
          ret = OB_NOT_SUPPORTED;
          TBSYS_LOG(WARN, "not supported ext value:ext[%ld]", ext_value);
        }
        else if(NULL == ups_row)
        {
          ret = OB_NOT_SUPPORTED;
          TBSYS_LOG(WARN, "OP_NOP can only used in ups row");
        }
        else //OP_NOP不需要序列化
        {
          cell_clone = *cell;
        }
      }
      else if (OB_SUCCESS != (ret = cell_writer.append(column_id, *cell, &cell_clone)))
      {
        if (OB_BUF_NOT_ENOUGH != ret)
        {
          TBSYS_LOG(WARN, "failed to append cell, err=%d", ret);
        }
        break;
      }
    }
    if (OB_SUCCESS == ret)
    {
      // whether reserve this cell
      for (int32_t j = 0; j < reserved_columns_count; ++j)
      {
        const std::pair<uint64_t,uint64_t> &tid_cid = reserved_columns_.at(j);
        if (table_id == tid_cid.first && column_id == tid_cid.second)
        {
          stored_row.reserved_cells_[j] = cell_clone;
          break;
        }
      } // end for j
    }
  } // end for i
  if (OB_SUCCESS == ret)
  {
    if (OB_SUCCESS != (ret = cell_writer.row_finish()))
    {
      if (OB_BUF_NOT_ENOUGH != ret)
      {
        TBSYS_LOG(WARN, "failed to append cell, err=%d", ret);
      }
    }
    else
    {
      stored_row.compact_row_size_ = static_cast<int32_t>(cell_writer.size());
      block.advance(cell_writer.size());
      cur_size_counter_ += cell_writer.size();
    }
  }
  return ret;
}


int ObRowStore::add_ups_row(const ObUpsRow &row, const StoredRow *&stored_row)
{
  return add_row(row, stored_row);
}

int ObRowStore::add_ups_row(const ObUpsRow &row, int64_t &cur_size_counter)
{
  return add_row(row, cur_size_counter);
}

int ObRowStore::add_ups_row(const ObRowkey &rowkey, const ObUpsRow &row, int64_t &cur_size_counter)
{
  return add_row(rowkey, row, cur_size_counter);
}

int ObRowStore::add_row(const ObRow &row, const StoredRow *&stored_row)
{
  int64_t cur_size_counter = 0; // value ignored
  return add_row(NULL, row, stored_row, cur_size_counter);
}

int ObRowStore::add_row(const ObRow &row, int64_t &cur_size_counter)
{
  const StoredRow *stored_row = NULL; // value ignored
  return add_row(NULL, row, stored_row, cur_size_counter);
}

int ObRowStore::add_row(const ObRowkey &rowkey, const ObRow &row, int64_t &cur_size_counter)
{
  const StoredRow *stored_row = NULL; // value ignored
  return add_row(&rowkey, row, stored_row, cur_size_counter);
}

int ObRowStore::add_row(const ObRowkey *rowkey, const ObRow &row, const StoredRow *&stored_row, int64_t &cur_size_counter)
{
  int ret = OB_SUCCESS;
  if (is_read_only_)
  {
    ret = OB_NOT_SUPPORTED;
    TBSYS_LOG(WARN, "read only ObRowStore, not allow add row, ret=%d", ret);
  }
  else
  {
    stored_row = NULL;
    const int32_t reserved_columns_count = static_cast<int32_t>(reserved_columns_.count());


    if (NULL == block_list_head_
        || block_list_head_->get_remain_size() <= get_compact_row_min_size(row.get_column_num())
        + get_reserved_cells_size(reserved_columns_count))
    {
      ret = new_block();
    }

    int64_t retry = 0;
    while(OB_SUCCESS == ret && retry < 3)
    {
      // in case this row would be rollback
      rollback_block_list_ = block_list_head_;
      rollback_iter_pos_ = ((block_list_head_==NULL) ? (-1) : block_list_head_->get_curr_data_pos());
      // append OrderByCells
      OB_ASSERT(block_list_head_);
      StoredRow *reserved_cells = reinterpret_cast<StoredRow*>(block_list_head_->get_buffer());
      reserved_cells->reserved_cells_count_ = static_cast<int32_t>(reserved_columns_count);
      block_list_head_->advance(get_reserved_cells_size(reserved_columns_count));
      if (OB_SUCCESS != (ret = append_row(rowkey, row, *block_list_head_, *reserved_cells)))
      {
        if (OB_BUF_NOT_ENOUGH == ret)
        {
          // buffer not enough
          block_list_head_->advance( -get_reserved_cells_size(reserved_columns_count) );
          TBSYS_LOG(DEBUG, "block buffer not enough, buff=%p remain_size=%ld block_count=%ld block_size=%ld",
                    block_list_head_, block_list_head_->get_remain_size(), block_count_, block_list_head_->block_size_);
          ret = OB_SUCCESS;
          ++retry;
          if (block_list_head_->is_fresh_block())
          {
            if (block_list_head_ == block_list_tail_)
            {
              ob_tc_free(block_list_head_, mod_id_);
              --block_count_;
              block_list_head_ = block_list_tail_ = NULL;
              ret = new_block(BIG_BLOCK_SIZE);
            }
            else
            {
              // free the last normal block
              BlockInfo *before_last = NULL;
              BlockInfo *curr = block_list_tail_;
              BlockInfo *next = NULL;
              while (NULL != curr)
              {
                next = curr->next_block_;
                if (next == block_list_head_)
                {
                  before_last = curr;
                  break;
                }
                curr = next;
              }
              if (NULL == before_last)
              {
                TBSYS_LOG(ERROR, "This branch should not be reached, before_last = NULL");
                ret = OB_ERR_UNEXPECTED;
              }
              else
              {
                block_list_head_ = before_last;
                block_list_head_->next_block_ = NULL;
                --block_count_;
                ob_tc_free(next, mod_id_);
                ret = new_block(BIG_BLOCK_SIZE);
              }
            }
          }
          else
          {
            ret = new_block(NORMAL_BLOCK_SIZE);
          }
        }
        else
        {
          TBSYS_LOG(WARN, "failed to append row, err=%d", ret);
        }
      }
      else
      {
        cur_size_counter_ += get_reserved_cells_size(reserved_columns_count);
        stored_row = reserved_cells;
        cur_size_counter = cur_size_counter_;
        break;                  // done
      }
    } // end while
    if (3 <= retry)
    {
      ret = OB_ERR_UNEXPECTED;
      TBSYS_LOG(ERROR, "unexpected branch");
    }
  }
  return ret;
}

int ObRowStore::get_next_row(const ObRowkey *&rowkey, ObRow &row, common::ObString *compact_row /* = NULL */)
{
  return inner_.get_next_row(rowkey, row, compact_row);
}

int ObRowStore::get_next_ups_row(const ObRowkey *&rowkey, ObUpsRow &row, common::ObString *compact_row /* = NULL */)
{
  return inner_.get_next_ups_row(rowkey, row, compact_row);
}

int ObRowStore::get_next_ups_row(ObUpsRow &row, common::ObString *compact_row /* = NULL */)
{
  return inner_.get_next_ups_row(row, compact_row);
}

void ObRowStore::reset_iterator()
{
  inner_.cur_iter_block_ = block_list_tail_;
  inner_.cur_iter_pos_ = 0;
  inner_.got_first_next_ = false;
}

ObRowStore::iterator ObRowStore::begin()
{
  TBSYS_LOG(DEBUG, "ObRowStore::begin block_list_tail_ date_ = %p  curr_data_pos_ = %ld",
      block_list_tail_->data_, block_list_tail_->curr_data_pos_);
  return iterator(this, 0, block_list_tail_, false);
}

int ObRowStore::get_next_row(ObRow &row, common::ObString *compact_row /* = NULL */)
{
  return inner_.get_next_row(row, compact_row);
}

int64_t ObRowStore::get_used_mem_size() const
{
  int64_t used = 0;
  BlockInfo *tmp = block_list_tail_;
  while (tmp != NULL)
  {
    used += tmp->block_size_;
    if (tmp == block_list_head_)
    {
      break;
    }
    tmp = tmp->next_block_;
  }
  return used;
}

bool ObRowStore::is_empty() const
{
  return block_count_ == 0 || block_list_tail_->curr_data_pos_ == 0;
}


DEFINE_SERIALIZE(ObRowStore)
{
  int ret = OB_SUCCESS;
  ObObj obj;
  BlockInfo *block = block_list_tail_;
  while(NULL != block)
  {
    // serialize block size
    obj.set_int(block->curr_data_pos_);
    if (OB_SUCCESS != (ret = obj.serialize(buf, buf_len, pos)))
    {
      TBSYS_LOG(WARN, "fail to serialize block size. ret=%d", ret);
      break;
    }
    // serialize block data
    else
    {
      if (buf_len - pos < block->curr_data_pos_)
      {
        TBSYS_LOG(WARN, "buffer not enough.");
        ret = OB_BUF_NOT_ENOUGH;
      }
      else
      {
        memcpy(buf + pos, block->get_buffer_head(), block->curr_data_pos_);
        pos += block->curr_data_pos_;
      }
    }
    // serialize next block
    if (block == block_list_head_)
    {
      block = NULL; // do not serialize non-used empty block(s)
    }
    else
    {
      block = block->next_block_;
    }
    TBSYS_LOG(DEBUG, "serialize next block");
  }
  if (OB_SUCCESS == ret)
  {
    obj.set_ext(ObActionFlag::OP_END_FLAG);
    if (OB_SUCCESS != (ret = obj.serialize(buf, buf_len, pos)))
    {
      TBSYS_LOG(WARN, "fail to serialize end flag:ret[%d]", ret);
    }
  }
  return ret;
}


DEFINE_DESERIALIZE(ObRowStore)
{
  int ret = OB_SUCCESS;
  int64_t old_pos = pos;
  int64_t block_size = 0;
  ObObj obj;

  reuse();

  while((OB_SUCCESS == (ret = obj.deserialize(buf, data_len, pos)))
              && (ObExtendType != obj.get_type()))
  {
    // get block data size
    if (ObIntType != obj.get_type())
    {
         TBSYS_LOG(WARN, "ObObj deserialize type error, ret=%d buf=%p data_len=%ld pos=%ld",
                       ret, buf, data_len, pos);
         ret = OB_ERROR;
         break;
    }
    if (OB_SUCCESS != (ret = obj.get_int(block_size)))
    {
      TBSYS_LOG(WARN, "ObObj deserialize error, ret=%d buf=%p data_len=%ld pos=%ld",
          ret, buf, data_len, pos);
      break;
    }
    // copy data to store
    if (OB_SUCCESS != (ret = this->new_block(block_size + sizeof(BlockInfo))))
    {
      TBSYS_LOG(WARN, "fail to allocate new block. ret=%d", ret);
      break;
    }
    if (NULL != block_list_head_ && block_size <= block_list_head_->get_remain_size() && block_size  >= 0)
    {
      TBSYS_LOG(DEBUG, "copy data from scanner to row_store. buf=%p, pos=%ld, block_size=%ld",
          buf, pos, block_size);
      memcpy(block_list_head_->get_buffer(), buf + pos, block_size);
      block_list_head_->advance(block_size);
      pos += block_size;
      cur_size_counter_ += block_size;
    }
    else
    {
      TBSYS_LOG(WARN, "fail to deserialize scanner data into new block. block_list_head_=%p, block_size=%ld",
          block_list_head_, block_size);
      ret = OB_ERROR;
      break;
    }
  }

  if (OB_SUCCESS == ret)
  {
    if (ObExtendType != obj.get_type() && ObActionFlag::OP_END_FLAG != obj.get_ext())
    {
      ret = OB_ERR_UNEXPECTED;
      TBSYS_LOG(WARN, "obj should be OP_END_FLAG:obj[%s]", to_cstring(obj));
    }
  }

  if (OB_SUCCESS != ret)
  {
    pos = old_pos;
  }

  return ret;
}

DEFINE_GET_SERIALIZE_SIZE(ObRowStore)
{
  int64_t size = 0;
  ObObj obj;
  BlockInfo *block = block_list_tail_;
  while(NULL != block)
  {
    obj.set_int(block->curr_data_pos_);
    size += obj.get_serialize_size();
    size += block->curr_data_pos_;
    block = block->next_block_;
  }
  obj.set_ext(ObActionFlag::OP_END_FLAG);
  size += obj.get_serialize_size();
  return size;
}


int ObRowStore::add_row(const ObRowkey &rowkey, const ObRow &row, const StoredRow *&stored_row)
{
  int ret = OB_SUCCESS;
  int64_t cur_size_counter = 0;
  if(OB_SUCCESS != (ret = add_row(&rowkey, row, stored_row, cur_size_counter)))
  {
    TBSYS_LOG(WARN, "add row fail:ret[%d]", ret);
  }
  return ret;
}

int ObRowStore::add_ups_row(const ObRowkey &rowkey, const ObUpsRow &row, const StoredRow *&stored_row)
{
  int ret = OB_SUCCESS;
  if(OB_SUCCESS != (ret = add_row(rowkey, row, stored_row)))
  {
    TBSYS_LOG(WARN, "add ups row fail:ret[%d]", ret);
  }
  return ret;
}

int64_t ObRowStore::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  databuff_printf(buf, buf_len, pos, "data_size=%ld block_count=%ld", cur_size_counter_, block_count_);
  return pos;
}
