/*
 * =====================================================================================
 *
 *       Filename:  db_row_formator.h
 *
 *    Description:  
 *
 *        Version:  1.0
 *        Created:  2014年11月27日 13时05分04秒
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  liyongfeng
 *   Organization:  
 *
 * =====================================================================================
 */
#ifndef __DB_ROW_FORMATOR_H__
#define __DB_ROW_FORMATOR_H__

#include <map>
#include "ob_export_param.h"
#include "common/ob_row.h"
#include "sql/ob_result_set.h"
#include "charset.h"//add qianzm [charset] 20160629

class DbRowFormator {
private:
  typedef std::map<std::string, ObExportColumnInfo> ColumnCache;
  class TSIBuffer
  {
    char* line_buffer_;
    int64_t buff_pos_;
    int64_t buff_size_;
  public:
    TSIBuffer()
    {
      line_buffer_ = NULL;
      buff_pos_ = 0;
      buff_size_ = 0;
    }
    int init(int64_t buff_size)
    {
      buff_size_ = buff_size;
      line_buffer_ = (char *)ob_malloc(buff_size, 1);
      if(line_buffer_ == NULL)
      {
        return OB_ERROR;
      }
      buff_pos_ = 0;
      return OB_SUCCESS;
    }
    int64_t& get_pos(){return buff_pos_;}
    char* get_line_buffer(){return line_buffer_;}
    ~TSIBuffer()
    {
      if(line_buffer_ != NULL)
      {
        ob_free(line_buffer_);
        line_buffer_ = NULL;
      }
    }
  };
public:
  DbRowFormator(ObSchemaManagerV2 *schema, const ObExportTableParam &param);
  virtual ~DbRowFormator();
  int init();
  void reset();
  int format(const ObRow &row, int32_t &bad, Charset &charset); //mod by zhuxh:20161018 [add class Charset]

  const char* get_line_buffer();

  int64_t length();

  TSIBuffer *get_tsi_buffer();

  //del zcd export 20150113:b
//  int find_cname_by_idx(int cell_idx, std::string &cname);
  //del:e

  int init_result_fields_map(const ObArray<oceanbase::sql::ObResultSet::Field>& fields);

  //del by zhuxh:20161018 [add class Charset]
  //DbRowFormator needn't know how to convert code. It just converts by object charset.
  /*
  //add qianzm [charset] 20160629:b
  void set_g2u(){ g2u_ = true; }
  void set_u2g(){ u2g_ = true; }
  //add 20160629:e
  */
private:
  int format_column(const ObObj *cell, int cell_idx, int32_t &bad, oceanbase::common::Charset & charset); //mod by zhuxh:20161018 [add class Charset]

private:
  ObSchemaManagerV2 *schema_;
  ObExportTableParam param_;
  
//  char *line_buffer;
//  int64_t buff_pos;
//  int64_t buff_size_;

  bool inited_;

  ColumnCache format_columns_;

  static const char *const user_format_style_[];
  static const char *const sys_format_style1_[];
  static const char *const sys_format_style2_[];

  std::map<int, std::string> result_fields_;

  int format_flags_[OB_ROW_MAX_COLUMNS_COUNT];//标记查询出的各个字段是否需要格式化，1表示需要，0表示不需要
  ObExportColumnInfo export_column_infos_[OB_ROW_MAX_COLUMNS_COUNT];//如果对应上面数组中的元素是1，在对应相同下标位置的元素会存储格式化信息

  //del by zhuxh:20161018 [add class Charset]
  /*
  //add qianzm [charset] 20160629:b
  bool g2u_;
  bool u2g_;
  //add 20160629:e
  */

};
#endif
