/*
 * =====================================================================================
 *
 *       Filename:  ob_export_consumer.cpp
 *
 *    Description:  
 *
 *        Version:  1.0
 *        Created:  2014年11月19日 09时30分10秒
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  liyongfeng
 *   Organization:  
 *
 * =====================================================================================
 */

#include "ob_export_consumer.h"
#include "common/ob_row.h"
#include "common/ob_new_scanner.h"
#include "sql/ob_sql_result_set.h"

using namespace oceanbase::sql;
using namespace oceanbase::common;

ExportConsumer::ExportConsumer(FileWriter *file_writer, DbRowFormator *formator, ErrorCode *errcode,
                               const ObExportTableParam &table_param, AppendableFile *bad_file,
                               /*add qianzm [export_by_tablets] 20160524*/bool is_multi_writers):
  code_conversion_flag( table_param.code_conversion_flag ) //add by zhuxh:20161018 [add class Charset]
{
  file_writer_ = file_writer;
  formator_ = formator;
  table_param_ = table_param;
  bad_file_ = bad_file;
  assert(NULL != file_writer_);
  assert(NULL != formator_);
  //assert(NULL != bad_file_);
  total_succ_rec_count_ = 0;
  total_failed_rec_count_ = 0;
  err_code_ = errcode;
  is_multi_writers_ = is_multi_writers;//add qianzm [export_by_tablets] 20160524
}

int ExportConsumer::init()
{
  int ret = OB_SUCCESS;
  return ret;
}

ExportConsumer::~ExportConsumer()
{

}

int ExportConsumer::comsume(TabletBlock* &block)
{
  int ret = OB_SUCCESS;
  TBSYS_LOG(INFO, "enter comsume");

  if (NULL == block) {
     TBSYS_LOG(DEBUG, "all tablets have scanned completly");
     return ComsumerQueue<TabletBlock*>::QUEUE_QUITING;
  }

  //add by zhuxh:20161018 [add class Charset] :b
  //Every thread has a charset object.
  Charset charset;
  if( code_conversion_flag != e_non )
  {
    if( code_conversion_flag == e_g2u )
    {
      charset.init(true);
    }
    else if( code_conversion_flag == e_u2g )
    {
      charset.init(false);
    }
    if( charset.fail() )
    {
      TBSYS_LOG(ERROR, "Code conversion init error.");
      err_code_->set_err_code(OB_ERROR);//add dinggh 20161201
      return ComsumerQueue<TabletBlock*>::QUEUE_QUITING;
      //TODO: Export success prints on the screen which is improperly.
    }
  }
  //add by zhuxh:20161018 [add class Charset] :e

  //add qianzm [export_by_tablets] 20160415:b
  int tablet_index = block->get_tablet_index();
  //add e 20160415:e
  int32_t bad_record = 0;
  ObRow row;
  ObSQLResultSet result;
  int success_format_rec_num = 0;
  int failed_format_rec_num = 0;
  block->get_data_buffer().get_position() = 0;
  if (OB_SUCCESS != (ret = result.deserialize(block->get_data_buffer().get_data(), 
                           block->get_data_buffer().get_capacity(), block->get_data_buffer().get_position())))
  {
    TBSYS_LOG(ERROR, "failed to deserialize ObSQLResultSet, ret=[%d]", ret);
    err_code_->set_err_code(OB_ERROR);//add dinggh 20161201
  }
  else
  {
    ObNewScanner& scanner = result.get_new_scanner();
    while (OB_SUCCESS == (ret = scanner.get_next_row(row)))
    {
      formator_->reset();
      if (OB_SUCCESS != (ret = formator_->format(row, bad_record, charset))) //mod by zhuxh:20161018 [add class Charset]
      {
        TBSYS_LOG(ERROR, "failed to format one row, row=[%s], ret=[%d]", to_cstring(row), ret);
        break;
      }
      if (bad_record != 0)//格式化失败,将buffer写入bad_file_
      {
        if (bad_file_ != NULL)
        {
          if (OB_SUCCESS != (ret = bad_file_->Append(formator_->get_line_buffer(), formator_->length())))//此处为多线程,调用加锁的append
          {
            ret = OB_ERROR;
            TBSYS_LOG(ERROR, "can't write to bad file");
            break;
          }
        }
        else
        {
          TBSYS_LOG(DEBUG, "no bad file, no need to write bad file");
        }
        failed_format_rec_num++;
      }
      else//格式化成功,将buffer写入正常数据文件
      {
          //add qianzm [export_by_tablets] 20160415:b
          if (tablet_writers_ != NULL)
          {
              if (OB_SUCCESS != (ret = tablet_writers_[tablet_index].write_records(formator_->get_line_buffer(), formator_->length())))
              {
                  ret = OB_ERROR;
                  TBSYS_LOG(ERROR, "failed to write buffer into data file, ret=[%d]", ret);
                  break;
              }
          }
          else
          //add 20160415:e
          {
              if (OB_SUCCESS != (ret = file_writer_->write_records(formator_->get_line_buffer(), formator_->length())))
              {
                  ret = OB_ERROR;
                  TBSYS_LOG(ERROR, "failed to write buffer into data file, ret=[%d]", ret);
                  break;
              }
          }
          success_format_rec_num++;
      }
    }

    if (OB_ITER_END == ret)
    {
      TBSYS_LOG(DEBUG, "fetch ObRow from ObNewScanner OK!");
      ret = OB_SUCCESS;
      inc_total_succ_rec_count(success_format_rec_num);
      inc_total_failed_rec_count(failed_format_rec_num);
      TBSYS_LOG(INFO, "consumer has consumer %d record, %d successfull, %d failed.", 
                      success_format_rec_num + failed_format_rec_num, 
                      success_format_rec_num, failed_format_rec_num);
    }
    else if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(ERROR, "failed to format ObNewScanner");
      err_code_->set_err_code(OB_ERROR);//add dinggh 20161201
      //需要设置error_code
    }
    else
    {
      TBSYS_LOG(INFO, "format ObNewScanner success");
    }
  }

  delete block;
  block = NULL;

  return ComsumerQueue<TabletBlock*>::QUEUE_SUCCESS;
}

int ExportConsumer::inc_total_succ_rec_count(int increment)
{
  tbsys::CThreadGuard guard(&total_succ_rec_count_lock_);
  total_succ_rec_count_ += increment;
  return total_succ_rec_count_;
}

int ExportConsumer::inc_total_failed_rec_count(int increment)
{
  tbsys::CThreadGuard guard(&total_failed_rec_count_lock_);
  total_failed_rec_count_ += increment;
  return total_failed_rec_count_;
}
//add qianzm [export_by_tablets] 20160415:b
int ExportConsumer::init_data_file(std::string &file_name, int n)
{
    int ret = OB_SUCCESS;
    if (n > 1 && is_multi_writers_)//mod qianzm [export_by_tablets] 20160524
    {
        FileWriter *fw = new FileWriter[n];
        tablet_writers_ = fw;
        for (int i = 0; i < n; i ++)
        {
            char t[128];
            string s;
            sprintf(t, "%d", i);
            s = t;
            string tablet_file_name = file_name + "." + s;

            if (OB_SUCCESS == ret)
            {
                if ((ret = tablet_writers_[i].init_file_name(tablet_file_name.c_str())) != OB_SUCCESS)
                {
                    TBSYS_LOG(ERROR, "FileWrite init failed, ret=[%d]", ret);
                    fprintf(stdout, "init file failed!!!!\n");
                }
                else
                {
					tablet_writers_[i].set_max_record_count(kDefaultMaxRecordNum);
                }
            }
        }
    }
    else{
        tablet_writers_ = NULL;
    }
    return ret;
}
//add 20160415:e
