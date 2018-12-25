/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *  
 * ob_read_worker.h for define read worker thread. 
 *
 * Authors:
 *   huating <huating.zmq@taobao.com>
 *
 */
#ifndef OCEANBASE_SYSCHECKER_READ_WORKER_H_
#define OCEANBASE_SYSCHECKER_READ_WORKER_H_

#include <tbsys.h>
#include "client/ob_client.h"
#include "ob_syschecker_rule.h"
#include "ob_syschecker_stat.h"

namespace oceanbase 
{ 
  namespace syschecker
  {
    class ObReadWorker : public tbsys::CDefaultRunnable
    {
    public:
      ObReadWorker(client::ObClient& client, ObSyscheckerRule& rule,
                   ObSyscheckerStat& stat, ObSyscheckerParam& param);
      ~ObReadWorker();

    public:
      virtual void run(tbsys::CThread* thread, void* arg);

    private:
      int init(ObOpParam** read_param);
      int run_get(ObOpParam& read_param, common::ObGetParam& get_param, 
                  common::ObScanner& scanner);
      int run_scan(ObOpParam& read_param, common::ObScanParam& scan_param, 
                   common::ObScanner& scanner);
      int check_scanner_result(const ObOpParam& read_param, 
                               const common::ObScanner& scanner, 
                               const bool is_get);

      int check_cell(const ObOpCellParam& cell_param, 
                     const common::ObCellInfo& cell_info);
      int check_null_cell(const int64_t prefix,
                          const common::ObCellInfo& cell_info, 
                          const common::ObCellInfo& aux_cell_info);
      int check_cell(const ObOpCellParam& cell_param, 
                     const ObOpCellParam& aux_cell_param, 
                     const common::ObCellInfo& cell_info, 
                     const common::ObCellInfo& aux_cell_info);
      int display_scanner(const common::ObScanner& scanner) const;

      int fill_rowkey_project(const int64_t table_id, sql::ObProject &project, common::ObRowDesc& row_desc);
      int fill_row_project(const int64_t table_id, const ObOpRowParam& row_param, 
          sql::ObProject &project, common::ObRowDesc& row_desc);
      int fill_project(int64_t table_id, int64_t column_id, sql::ObProject& project);
      int fill_sql_scan_param(ObOpParam& read_param, sql::ObSqlScanParam& scan_param, common::ObRowDesc& row_desc);
      int run_scan(ObOpParam& read_param, ObSqlScanParam& scan_param, ObNewScanner& scanner);
      int fill_sql_get_param(ObOpParam& read_param, sql::ObSqlGetParam& get_param, common::ObRowDesc& row_desc);
      int run_get(ObOpParam& read_param, ObSqlGetParam& get_param, ObNewScanner& scanner);
      int check_scanner_result(const ObOpParam& read_param, const common::ObRowDesc& desc, 
          const ObNewScanner& scanner, const bool is_get);

    private:
      DISALLOW_COPY_AND_ASSIGN(ObReadWorker);

      client::ObClient& client_;
      ObSyscheckerRule& read_rule_;
      ObSyscheckerStat& stat_;
      ObSyscheckerParam& param_;
    };
  } // end namespace syschecker
} // end namespace oceanbase

#endif //OCEANBASE_SYSCHECKER_READ_WORKER_H_
