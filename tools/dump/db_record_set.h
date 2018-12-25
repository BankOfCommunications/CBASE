/*
 * =====================================================================================
 *
 *       Filename:  RecordSet.h
 *
 *        Version:  1.0
 *        Created:  04/12/2011 04:54:10 PM
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  yushun.swh (DBA Group), yushun.swh@taobao.com
 *        Company:  taobao
 *
 * =====================================================================================
 */
#ifndef OB_API_RECORDSET_H
#define  OB_API_RECORDSET_H

#include "common/ob_server.h"
#include "common/ob_string.h"
#include "common/ob_scanner.h"
#include "common/ob_result.h"
#include "common/utility.h"
#include "db_record.h"

namespace oceanbase {
  using common::OB_SUCCESS;
  namespace api {
    const int kDefaultRecordSetSize = 2 * 1024 * 1024;

    class DbRecordSet {
      friend class Iterator;
      friend class OceanbaseDb;

      public:
      class Iterator {
        public:
          explicit Iterator();
          Iterator(DbRecordSet *ds, common::ObScannerIterator cur_pos);
          Iterator& operator++(int);
          bool operator==(const DbRecordSet::Iterator &itr);
          bool operator!=(const DbRecordSet::Iterator &itr);
          int get_record(DbRecord **recordp);
          DbRecord* operator->() { return &record_; }

          int get_last_err() { return last_err_; }
        private:
          int fill_record(common::ObScannerIterator &itr, bool &has_record);

          int last_err_;
          DbRecord record_;
          common::ObScannerIterator cur_pos_;
          common::ObScannerIterator rec_end_pos_;
          DbRecordSet *ds_;
          bool res_start_;
      };

      Iterator begin();
      Iterator end();

      common::ObDataBuffer &get_buffer() { return ob_buffer_; }
      common::ObScanner &get_scanner() { return scanner_; }

      bool has_more_data() const;
      bool empty() const;

      int get_last_rowkey(common::ObRowkey &last_key);

      DbRecordSet(int cap = kDefaultRecordSetSize):cap_(cap), inited_(false) { p_buffer_ = NULL; }
      ~DbRecordSet();
      int init();
      bool inited() { return inited_; }

      private:
      common::ObScanner scanner_;
      char *p_buffer_;
      common::ObDataBuffer ob_buffer_;
      int cap_;
      bool inited_;
    };
  }
}

#endif
