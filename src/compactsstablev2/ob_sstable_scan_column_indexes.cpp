#include "ob_sstable_scan_column_indexes.h"

using namespace oceanbase::common;

namespace oceanbase
{
  namespace compactsstablev2
  {
    int ObSSTableScanColumnIndexes::add_column_id(const ColumnType type,
        const int64_t index, const uint64_t column_id)
    {
      int ret = OB_SUCCESS;

      if (0 > index || 0 == column_id || common::OB_INVALID_ID == column_id)
      {
        TBSYS_LOG(WARN, "invalid argument:column=%lu", column_id);
        ret = common::OB_INVALID_ARGUMENT;
      }
      else if (column_cnt_ >= column_info_size_)
      {
        TBSYS_LOG(ERROR, "column_cnt_ >= column_info_size_");
        ret = common::OB_SIZE_OVERFLOW;
      }
      else
      {
        if (NULL == column_info_)
        {
          if (NULL == (column_info_ = reinterpret_cast<Column*>(
                         common::ob_malloc(COLUMN_ITEM_SIZE * column_info_size_, ObModIds::OB_SSTABLE_INDEX))))
          {
            TBSYS_LOG(WARN, "failed to alloc memory");
            ret = common::OB_ALLOCATE_MEMORY_FAILED;
          }
        }

        if (common::OB_SUCCESS == ret && NULL != column_info_)
        {
          column_info_[column_cnt_].type_ = type;
          column_info_[column_cnt_].id_ = column_id;
          column_info_[column_cnt_].index_ = index;
          column_cnt_ ++;
        }
      }

      return ret;
    }

    int ObSSTableScanColumnIndexes::get_column(const int64_t index,
        Column& column) const
    {
      int ret = OB_SUCCESS;

      if (0 <= index && column_cnt_ > index)
      {
        column = column_info_[index];
      }
      else
      {
        column.id_ = common::OB_INVALID_ID;
        column.index_ = common::OB_INVALID_INDEX;
        column.type_ = NotExist;
        ret = OB_INVALID_ARGUMENT;
      }

      return ret;
    }

    int64_t ObSSTableScanColumnIndexes::to_string(
        char* buf, const int64_t buf_len) const
    {
      int64_t pos = 0;
      databuff_printf(buf, buf_len,
          pos, "scan_column_indexes:");

      for (int64_t i = 0; i < column_cnt_; i ++)
      {
        databuff_printf(buf, buf_len, pos, "<%ld,%d,%ld,%ld>,",
            i, column_info_[i].type_, column_info_[i].index_,
            column_info_[i].id_);
      }

      databuff_printf(buf, buf_len, pos, "\n");
      return pos;
    }
  }//end namespace compactsstablev2
}//end namespace oceanbase
