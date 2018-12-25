/*
 * =====================================================================================
 *
 *       Filename:  ob_export_extra_param.h
 *
 *    Description:  
 *
 *        Version:  1.0
 *        Created:  2014年11月19日 20时58分13秒
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  zhangcd 
 *   Organization:  
 *
 * =====================================================================================
 */
#ifndef OB_EXPORT_EXTRA_H
#define OB_EXPORT_EXTRA_H
#include "common/ob_define.h"
#include "common/ob_schema.h"
#include "tokenizer.h"

const int64_t kDefaultCapacity = 2 * 1024 * 1024;//2M

// 在producer和consumer之间传输的数据单位
class TabletBlock
{
public:
  TabletBlock(int64_t cap = kDefaultCapacity) : cap_(cap), inited_(false) { buffer_ = NULL; }
  virtual ~TabletBlock();
  int init();
  oceanbase::common::ObDataBuffer& get_data_buffer() { return databuff_; }
  //add qianzm [tablets_wtriter] 20160415:b
  void set_tablet_index(int &index){tablet_index_ = index;}
  int get_tablet_index(){return tablet_index_;}
  //add 20160415:e
private:
  char *buffer_;
  oceanbase::common::ObDataBuffer databuff_;
  int64_t cap_;
  bool inited_;
  int tablet_index_;//add qianzm [export_by_tablets] 20160415
};

//此类型是为了作为可以多线程访问的错误类型
class ErrorCode
{
public:
  ErrorCode(){
      ret_ = oceanbase::common::OB_SUCCESS;
      ret_exit_ = oceanbase::common::OB_SUCCESS;//add qianzm [tablet_writers] 20160415
  }
  int set_err_code(int err)
  {
    lock_.lock();
    ret_ = err;
    lock_.unlock();
    return ret_;
  }

  int get_err_code()
  {
    return ret_;
  }
//add qianzm [tablet_writers] 20160415:b
  int set_exit_code(int err)
  {
      lock_.lock();
      ret_exit_ = err;
      lock_.unlock();
      return ret_exit_;
  }

  int get_exit_code()
  {
    return ret_exit_;
  }
//add 20160415:e
private:
  int ret_;
  int ret_exit_;//add qianzm [tablet_writers] 20160415
  tbsys::CThreadMutex lock_;
};
#endif
