/*
 * =====================================================================================
 *
 *       Filename:  ob_export_consumer.h
 *
 *    Description:  
 *
 *        Version:  1.0
 *        Created:  2014年11月17日 15时01分37秒
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  YOUR NAME (), 
 *   Organization:  
 *
 * =====================================================================================
 */


#ifndef __OB_EXPORT_CONSUMER_H__
#define __OB_EXPORT_CONSUMER_H__
#include "ob_export_queue.h"
#include "ob_export_param.h"
#include "ob_export_extra_param.h"
#include "file_writer.h"
#include "file_appender.h"
#include "db_row_formator.h"
#include "charset.h" //add by zhuxh:20161018 [add class Charset]

class ExportConsumer : public QueueComsumer<TabletBlock*>
{
private:
  static const int64_t kLineBufferSize = 2 * 1024 * 1024;//2M
public:
  ExportConsumer(FileWriter *file_writer, DbRowFormator *formator, ErrorCode *errcode, const ObExportTableParam &table_param, AppendableFile *bad_file,
                 /*add qianzm [export_by_tablets] 20160524*/bool is_multi_writers);
  virtual int comsume(TabletBlock* &block);
  virtual int init();
  ~ExportConsumer();
  int get_total_succ_rec_count() { return total_succ_rec_count_; }
  int get_total_failed_rec_count() { return total_failed_rec_count_; }

  int inc_total_succ_rec_count(int increment);

  int inc_total_failed_rec_count(int increment);
  int init_data_file(std::string &filename, int n);//add qianzm [export_by_tablets] 20160415

private:
  int format_column(const ObObj *cell);

private:
  FileWriter* file_writer_;
  DbRowFormator* formator_;
  ObExportTableParam table_param_;

  AppendableFile *bad_file_;

  ErrorCode *err_code_;
  int total_succ_rec_count_;
  int total_failed_rec_count_;

  tbsys::CThreadMutex total_succ_rec_count_lock_;
  tbsys::CThreadMutex total_failed_rec_count_lock_;
  //add qianzm [export_by_tablets] 20160415:b
  FileWriter* tablet_writers_;
  //add 20160415:e
  bool is_multi_writers_;//add qianzm [export_by_tablets] 20160524

  oceanbase::common::CodeConversionFlag code_conversion_flag; //add by zhuxh:20161018 [add class Charset]
};

#endif     /* -----  __OB_EXPORT_CONSUMER_H__  ----- */
