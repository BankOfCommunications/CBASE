/*
 * =====================================================================================
 *
 *       Filename:  DbRecordSet.cpp
 *
 *        Version:  1.0
 *        Created:  04/12/2011 10:35:22 PM
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  yushun.swh@taobao
 *        Company:  taobao
 *
 * =====================================================================================
 */
#include "db_record_set.h"
#include "db_record.h"

namespace oceanbase {
  namespace api {

    using namespace oceanbase::common;

    int DbRecordSet::Iterator::get_record(DbRecord **recp) 
    { 
      if (last_err_ == OB_SUCCESS)
        *recp = &record_; 
      else
        *recp = NULL;

      return last_err_; 
    }

    DbRecordSet::Iterator::Iterator()
    {
      ds_ = NULL;
      last_err_ = OB_SUCCESS;
    }

    DbRecordSet::Iterator::Iterator(DbRecordSet *ds, common::ObScannerIterator cur_pos)
    {
      ds_ = ds;
      cur_pos_ = rec_end_pos_ = cur_pos;

      last_err_ = OB_SUCCESS;
      bool has_record = false;
      if (fill_record(rec_end_pos_, has_record) == common::OB_ITER_END) {
        if (!has_record)
          cur_pos_ = rec_end_pos_;
      }
    }

    int DbRecordSet::Iterator::fill_record(common::ObScannerIterator &itr, bool &has_record)
    {
      common::ObCellInfo *cell;
      bool row_changed = false;
      bool row_start = false;
      int ret = OB_SUCCESS;
      int64_t value;
      has_record = false;

      record_.reset();

      //skip first row
      while ((ret = itr.get_cell(&cell, &row_changed)) == common::OB_SUCCESS) {
        if (cell->value_.get_ext(value) == common::OB_SUCCESS) {
          itr++;
        } else
          break;
      }

      while((ret = itr.get_cell(&cell, &row_changed)) == common::OB_SUCCESS) {
        has_record = true;

        if (row_changed) {
          if (row_start == false)
            row_start = true;
          else {
            break;
          }
        }

        record_.append_column(*cell);
        itr++;
      }

      if (ret != common::OB_SUCCESS && ret != common::OB_ITER_END) {
        last_err_ = ret;
        TBSYS_LOG(ERROR, "fill record failed, due to %d", ret);
      } else {
        last_err_ = OB_SUCCESS;
      }

      return ret;
    }

    bool DbRecordSet::empty() const
    {
      return scanner_.is_empty();
    }

    bool DbRecordSet::has_more_data() const
    {
      bool fullfilled = false;
      int64_t item_num;

      scanner_.get_is_req_fullfilled(fullfilled, item_num);
      return fullfilled;
    }

    int DbRecordSet::get_last_rowkey(common::ObRowkey& last_key)
    {
      return scanner_.get_last_row_key(last_key);
    }

    DbRecordSet::Iterator& DbRecordSet::Iterator::operator++(int i)
    {
      UNUSED(i);
      cur_pos_ = rec_end_pos_;
      bool has_record = false;
      fill_record(rec_end_pos_, has_record);
      return *this;
    }

    bool DbRecordSet::Iterator::operator==(const DbRecordSet::Iterator &itr) 
    {
      return cur_pos_ == itr.cur_pos_;
    }

    bool DbRecordSet::Iterator::operator!=(const DbRecordSet::Iterator &itr) 
    {
      return cur_pos_ != itr.cur_pos_;
    }


    DbRecordSet::Iterator DbRecordSet::begin() 
    {
      DbRecordSet::Iterator itr = DbRecordSet::Iterator(this, scanner_.begin());
      return itr;
    }

    DbRecordSet::Iterator DbRecordSet::end()
    {
      return DbRecordSet::Iterator(this, scanner_.end());
    }

    DbRecordSet::~DbRecordSet()
    {
      if (p_buffer_ != NULL)
        delete [] p_buffer_;
    }

    int DbRecordSet::init()
    {
      p_buffer_ = new(std::nothrow) char[cap_];
      if (p_buffer_ == NULL)
        return common::OB_MEM_OVERFLOW;

      ob_buffer_.set_data(p_buffer_, cap_);
      inited_ = true;
      return common::OB_SUCCESS;
    }
  }
}
