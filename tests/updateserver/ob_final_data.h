/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * ob_final_data.h is for what ...
 *
 * Version: $id: ob_final_data.h,v 0.1 11/1/2010 10:15a wushi Exp $
 *
 * Authors:
 *   wushi <wushi.ly@taobao.com>
 *     - some work details if you want
 *
 */
#include <iostream>
#include <vector> 
#include <map>
#include <fstream>
#include "common/ob_cell_array.h"
#include "common/ob_read_common_data.h"
#include "common/ob_string.h"
#include "common/ob_scanner.h"
#include "common/ob_schema.h"
struct ObRowInfo
{
  oceanbase::common::ObString table_name_;
  oceanbase::common::ObString row_key_;

  bool operator <(const ObRowInfo &other)const;
  bool operator ==(const ObRowInfo &other)const;
};

class ObFinalResult
{
public:
  ObFinalResult();
  virtual ~ObFinalResult();

public:
  int parse_from_file(const char *filename, const char *schema_fname);
  int add_cell(const oceanbase::common::ObCellInfo &cell);
  bool row_exist(const oceanbase::common::ObCellInfo & add_cell);

public:
  int get(const oceanbase::common::ObGetParam &get_param, oceanbase::common::ObScanner &scanner,
    bool ret_null = true);
  int scan(const oceanbase::common::ObScanParam &scan_param, oceanbase::common::ObScanner &scanner);

private:
  int apply_join(oceanbase::common::ObSchemaManager &schema_mgr,
                 oceanbase::common::ObCellInfo& cur_cell, 
                 const oceanbase::common::ObJoinInfo &join_info);
  mutable std::map<ObRowInfo, std::vector<oceanbase::common::ObCellInfo> > *row_infos_;
  oceanbase::common::ObCellArray *cell_array_;
};

bool check_result(const oceanbase::common::ObGetParam &get_param, 
                  oceanbase::common::ObScanner &ob_result, ObFinalResult & local_result);

bool check_result(const oceanbase::common::ObScanParam &scan_param, 
                  oceanbase::common::ObScanner &ob_result, ObFinalResult & local_result);
