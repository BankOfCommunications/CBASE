#ifndef OCEANBASE_COMPACTSSSTABLV2_OB_SSTABLE_BLOCK_SCANNER_H_
#define OCEANBASE_COMPACTSSTABLEV2_OB_SSTABLE_BLOCK_SCANNER_H_

#include "common/ob_define.h"
#include "common/ob_range2.h"
#include "common/ob_compact_cell_iterator.h"
#include "common/utility.h"
#include "ob_sstable_block_reader.h"

namespace oceanbase
{
  namespace compactsstablev2
  {
    class ObSSTableBlockScanner
    {
    public:
      ObSSTableBlockScanner()
        : is_inited_(false),
          is_reverse_scan_(false),
          row_cursor_(NULL),
          row_start_index_(NULL),
          row_last_index_(NULL)
      {
      }

      ~ObSSTableBlockScanner()
      {
      }

      int set_scan_param(const common::ObNewRange& range,
          const bool is_reverse_scan,
          const ObSSTableBlockReader::BlockData& block_data,
          const common::ObCompactStoreType& row_store_type,
          bool& need_looking_forward);

      int get_next_row(common::ObCompactCellIterator*& row);

    private:
      inline int initialize(const bool is_reverse_scan)
      {
        int ret = common::OB_SUCCESS;
        is_reverse_scan_ = is_reverse_scan;
        return ret;
      }

      int locate_start_pos(const common::ObNewRange& range, 
          ObSSTableBlockReader::const_iterator& start_iterator,
          bool& need_looking_forward);

      int locate_end_pos(const common::ObNewRange& range, 
          ObSSTableBlockReader::const_iterator& last_iterator,
          bool& need_looking_forward);

      int load_current_row(
          ObSSTableBlockReader::const_iterator row_index);

      inline int end_of_block()
      {
        bool ret = false;
        if ((!is_reverse_scan_) && row_cursor_ > row_last_index_)
        {
          ret = true;
        }
        else if (is_reverse_scan_ && row_cursor_ < row_start_index_)
        {
          ret = true;
        }
        else
        {
          //do nothing
        }
        return ret;
      }

      inline void next_row()
      {
        if (!is_reverse_scan_)
        {
          row_cursor_ ++;
        }
        else
        {
          row_cursor_ --;
        }
      }

    private:
      bool is_inited_;
      bool is_reverse_scan_;
      ObSSTableBlockReader block_reader_;

      ObSSTableBlockReader::const_iterator row_cursor_;
      ObSSTableBlockReader::const_iterator row_start_index_;
      ObSSTableBlockReader::const_iterator row_last_index_;

      common::ObCompactCellIterator row_;
    };
  }//end namesapce compactsstablev2
}//end namespace oceanbase
#endif
