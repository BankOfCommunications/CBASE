/*
 * =====================================================================================
 *
 *       Filename:  ob_export_producer.cpp
 *
 *    Description:  
 *
 *        Version:  1.0
 *        Created:  2014年11月19日 09时27分04秒
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  zhangcd 
 *   Organization:  
 *
 * =====================================================================================
 */

#include "ob_export_producer.h"
#include "sql/ob_sql_result_set.h"
#include "common/ob_tsi_factory.h"

using namespace oceanbase::common;
using namespace oceanbase::api;
using namespace oceanbase::sql;
using namespace std;

//static const int kDefaultResultBufferSize = 2 * 1024 * 1024; //一个ob_packet的缓冲区大小
//static const int64_t MAX_TIMEOUT_US = 25 * 60 * 1000 * 1000; //25分钟重试的最大超时时间
//static const int64_t SPAN_TIMEOUT_US = 2 * 60 * 1000 * 1000; //2分钟,每次重试递增的时间

ExportProducer::ExportProducer(oceanbase::api::OceanbaseDb *db, const ObSchemaManagerV2 *schema_mgr, 
                               const char* table_name, ErrorCode *code, const ObExportTableParam &table_param,
                               DbRowFormator *rowformator,
                               std::vector<oceanbase::common::ObServer> ms_vec,
                               int64_t max_limittime,
                               /*add qianzm [export_by_tablets] 20160524*/bool is_multi_writers,
                               int _producer_concurrency):producer_concurrency(_producer_concurrency)  //add by zhuxh [timeout mechanism] 20170518
{
  db_ = db;
  assert(db_ != NULL);
  schema_ = schema_mgr;
  table_name_ = table_name;
  table_param_ = table_param;
  timeout_us_ = 0;
  err_code_ = code;
  ms_vec_ = ms_vec;
  rowformator_ = rowformator;
  is_rowkey_idx_inited_ = false;
  is_rowformator_result_field_inited_ = false;
  max_limittime_ = max_limittime;
  tablet_index_ = 0;//add qianzm [export_by_tablets] 20160415
  is_multi_writers_ = is_multi_writers;//add qianzm [export_by_tablets] 20160524
}

int ExportProducer::ProduceMark::static_index = 0;

//producer_index是在produceMark对象初始化的时候就已经确定的
ExportProducer::ProduceMark::ProduceMark():producer_run_times(0),deadline(0) //add by zhuxh [timeout mechanism] 20170512
{
  next_packet = true;
  next_sql_request = true;
  is_last_timeout = false;
  last_timeout = 0;
  start_rowkey = ObRowkey::MIN_ROWKEY;
  last_rowkey = ObRowkey::MAX_ROWKEY;

  //每个ProduceMark对象获取一个全局独立的序列号
  s_index_lock_.lock();
  producer_index = static_index;
  static_index++;
  s_index_lock_.unlock();
  tablet_index_ = 0;//add qianzm [export_by_tablets] 20160415
}

//当前ob_export与ms之间的所有有效连接的信息
ActiveServerSession ExportProducer::ac_server_session;

//将一个失效的连接信息去除
int ActiveServerSession::pop_by_producer_id(int producer_id, ObServer& server, int64_t& session_id)
{
  int ret = OB_EXPORT_EXIST;
  map_lock_.lock();
  std::map<int, ServerSession>::iterator ss = map.find(producer_id);
  if(ss == map.end())
  {
    ret = OB_EXPORT_NOT_EXIST;
  }
  else
  {
    server = ss->second.server;
    session_id = ss->second.session_id;
    map.erase(ss);
  }
  map_lock_.unlock();
  return ret;
}

//将一个失效的连接信息去除
int ActiveServerSession::pop_one(ObServer& server, int64_t& session_id)
{
  int ret = OB_EXPORT_EXIST;
  map_lock_.lock();
  std::map<int, ServerSession>::iterator ss = map.begin();
  if(ss == map.end())
  {
    ret = OB_EXPORT_EMPTY;
  }
  else
  {
    server = ss->second.server;
    session_id = ss->second.session_id;
    map.erase(ss);
  }
  map_lock_.unlock();
  return ret;
}

//加入一个有效的连接信息
int ActiveServerSession::push_by_id(int producer_id, ObServer server, int64_t session_id)
{
  int ret = OB_EXPORT_SUCCESS;
  map_lock_.lock();
  ServerSession ss;
  ss.server = server;
  ss.session_id = session_id;
  map.insert(make_pair(producer_id, ss));
  map_lock_.unlock();
  return ret;
}

int ExportProducer::produce_v2(TabletBlock* &block)//add qianzm
{
  //获取当前线程私有的单例ProduceMark对象，需要此对象中的标记信息来作判断
  ProduceMark *mark = GET_TSI_MULT(ProduceMark, 1);

  int ret = ComsumerQueue<TabletBlock*>::QUEUE_SUCCESS;
  TBSYS_LOG(DEBUG, "enter produce");
  block = NULL;

  //判断当前的全局err_code_是不是正常状态，如果是异常状态则结束当前线程
  if(OB_SUCCESS != err_code_->get_err_code())
  {
    TBSYS_LOG(INFO, "some other thread exit with fail code! current thread should exit!");
    return ComsumerQueue<TabletBlock*>::QUEUE_QUITING;
  }
  else if(mark->next_sql_request && mark->next_packet)//前一个查询结束，进入下一个查询
  {
    string sql_part1 = "";
    string sql_part2 = "";

    sql_part1 = table_param_.export_sql;

    if(!mark->is_last_timeout)//检查上次查询是否是超时失败，第一次为初始化超时时间
    {
      mark->is_last_timeout = false;
      mark->last_timeout = timeout_us_ + 600000000;//(25min)
      //mark->last_timeout = 180L * 60L * 1000L * 1000L;(3hour)
    }
    else //如果上次查询是超时失败，则退出
    {
      TBSYS_LOG(ERROR, "last get_result is timeout, last_timeout is [%ld]", mark->last_timeout);
      err_code_->set_err_code(OB_ERROR);
      return ComsumerQueue<TabletBlock*>::QUEUE_QUITING;
      /*
	  //将mark中相关的标记量重新赋值
      mark->next_sql_request = false;
      mark->next_packet = false;
      mark->is_last_timeout = false;
      mark->last_timeout += SPAN_TIMEOUT_US;

      //如果调大后的超时时间已经大于最大的超时时间(25min)，则报错
      if(mark->last_timeout > max_limittime_)
      {
        TBSYS_LOG(ERROR, "current timeout[%ld] is exceed the MAX_TIMEOUT_US[%ld], failed!", mark->last_timeout, max_limittime_);
        err_code_->set_err_code(OB_ERROR);
        return ComsumerQueue<TabletBlock*>::QUEUE_QUITING;
      }
	  */
    }
    //将得到的sql进行拼接，第一部分为用户填写的sql语句，第二部分为程序根据mark中的rowkey信息或者tabletinfo中的rowkey信息拼接得到的range后缀信息
	//多表操作无tablet信息，即sql_part2为空
    string sql = sql_part1 + " " + sql_part2;
    TBSYS_LOG(INFO, "query sql: %s", sql.c_str());

    //获取一台可用的ms
    ObServer ms = get_one_ms();
    TBSYS_LOG(INFO, "get one ms: %s, timeout is %ld", to_cstring(ms), mark->last_timeout);

    //发起sql查询
    if(OB_SUCCESS != (ret = db_->init_execute_sql(ms, sql, mark->last_timeout, mark->last_timeout - 5*1000*1000)))
    {
      TBSYS_LOG(ERROR, "init_execute_sql failed! ret=%d", ret);
      err_code_->set_err_code(OB_ERROR);
      return ComsumerQueue<TabletBlock*>::QUEUE_QUITING;
    }

    TBSYS_LOG(DEBUG, "init_execute_sql success!");

    bool has_next = true;
    ObSQLResultSet result_set;

    //新分配一个TabletBlock将databuffer拷贝到该对象中
    TabletBlock *tmp_block = new TabletBlock();
    if(NULL == tmp_block)
    {
      TBSYS_LOG(ERROR, "allocate memory for tmp_block failed! ret=[%d]", ret);
      err_code_->set_err_code(OB_ERROR);
      return ComsumerQueue<TabletBlock*>::QUEUE_QUITING;
    }

    //TabletBlock对象使用之前需要调用init
    if(OB_SUCCESS != (ret = tmp_block->init()))
    {
      TBSYS_LOG(ERROR, "first tmp_block init failed! ret=[%d]", ret);
      err_code_->set_err_code(OB_ERROR);
      delete tmp_block;
      return ComsumerQueue<TabletBlock*>::QUEUE_QUITING;
    }

    TBSYS_LOG(DEBUG, "first tmp_block->init success!");

    ObDataBuffer& out_buffer = tmp_block->get_data_buffer();

    if(OB_SUCCESS != (ret = db_->get_result(result_set, out_buffer, has_next)))
    {
      TBSYS_LOG(ERROR, "get result from mergeserver failed! ret=%d", ret);
      err_code_->set_err_code(OB_ERROR);
      ret = ComsumerQueue<TabletBlock*>::QUEUE_QUITING;
    }
    else
    {
      TBSYS_LOG(DEBUG, "first get a packet success!");

      thread_add_server_session();

      //del by zhuxh:20151230
      /*把初始化主键的这个模块打开，可以让produce_V2也可用于单线程执行单表的操作,
      当以单表的方式导出某个tablet失败时，可以将日志中的记录的失败或还未执行的tablet信息构造成单条sql，
      拿到produce_v2中执行，实现人工续传。

      //初始化rowkey_idx_，进程全局只会初始化一次
      if(!is_rowkey_idx_inited_)
      {
        rowkey_idx_inited_lock_.lock();
        if(!is_rowkey_idx_inited_)
        {
          if(OB_SUCCESS != (ret = init_rowkey_idx(result_set.get_fields())))
          {
            TBSYS_LOG(ERROR, "init_rowkey_idx failed!");
            rowkey_idx_inited_lock_.unlock();
            err_code_->set_err_code(OB_ERROR);
            return ComsumerQueue<TabletBlock*>::QUEUE_QUITING;
          }
          TBSYS_LOG(INFO, "init_rowkey_idx successed!");
          is_rowkey_idx_inited_ = true;
        }
        rowkey_idx_inited_lock_.unlock();
      }
*/

      //初始化rowformator中的成员，这个只能在有结果集时才能调用
      if(!is_rowformator_result_field_inited_)
      {
        rowformator_result_field_lock_.lock();
        if(!is_rowformator_result_field_inited_)
        {
          if(OB_SUCCESS != (ret = rowformator_->init_result_fields_map(result_set.get_fields())))
          {
            TBSYS_LOG(ERROR, "rowformator->init_result_fields_map failed! ret=%d", ret);
            rowformator_result_field_lock_.unlock();
            err_code_->set_err_code(OB_ERROR);
            return ComsumerQueue<TabletBlock*>::QUEUE_QUITING;
          }
          TBSYS_LOG(INFO, "rowformator->init_result_fields_map success!");
          is_rowformator_result_field_inited_ = true;
        }
        rowformator_result_field_lock_.unlock();
      }

     //将新分配的TabletBlock内存地址传回
      block = tmp_block;
      if(has_next)
      {
        mark->next_sql_request = false;
        mark->next_packet = true;
        mark->is_last_timeout = false;
      }
      else
      {
        mark->next_sql_request = false;
        mark->next_packet = false;
        mark->is_last_timeout = false;
        thread_remove_server_session();
      }
      ret = ComsumerQueue<TabletBlock*>::QUEUE_SUCCESS;
    }
  }
  else if(mark->next_packet && !mark->next_sql_request)//当前查询还没有结束，继续获取下一个包
  {
    bool has_next = true;
    ObSQLResultSet result_set;

    //新分配一个TabletBlock将databuffer拷贝到该对象中
    TabletBlock *tmp_block = new TabletBlock();
    if(NULL == tmp_block)
    {
      TBSYS_LOG(ERROR, "allocate memory for tmp_block failed! ret=%d", ret);
      err_code_->set_err_code(OB_ERROR);
      thread_post_end_session();
      return ComsumerQueue<TabletBlock*>::QUEUE_QUITING;
    }

    if(OB_SUCCESS != (ret = tmp_block->init()))
    {
      TBSYS_LOG(ERROR, "next tmp_block init failed! ret=%d", ret);
      err_code_->set_err_code(OB_ERROR);
      delete tmp_block;
      thread_post_end_session();
      return ComsumerQueue<TabletBlock*>::QUEUE_QUITING;
    }

    TBSYS_LOG(DEBUG, "next tmp_block init success!");

    ObDataBuffer& out_buffer = tmp_block->get_data_buffer();

    if(OB_SUCCESS != (ret = db_->get_result(result_set, out_buffer, has_next)))
    {
	/*对于多表操作超时无法实现续传，直接退出
         *================================
      //对于超时这个错误情况，需要重试
      if(OB_RESPONSE_TIME_OUT == ret)
      {
        mark->is_last_timeout = true;
        mark->next_sql_request = true;
        mark->next_packet = true;
        thread_post_end_session();
        TBSYS_LOG(WARN, "current get_reuslt is timeout! timeout_us is %ld", mark->last_timeout);
      }
      else*/
      {
        TBSYS_LOG(ERROR, "get result from mergeserver failed! ret=%d", ret);
        err_code_->set_err_code(OB_ERROR);
        delete tmp_block;
        thread_post_end_session();
        return ComsumerQueue<TabletBlock*>::QUEUE_QUITING;
      }
    }
    else
    {
      TBSYS_LOG(DEBUG, "next get a packet success!");
      //将新分配的TabletBlock内存地址传回
      block = tmp_block;

      ObRow row;
      ObRow last_row;
      ObRowkey start_rowkey;
      bool empty = true;

      //获取最后一行的row，存储到mark中，以备下次调用get_result时发生超时错误时使用
      while(OB_SUCCESS == (ret = result_set.get_new_scanner().get_next_row(row)))
      {
        last_row = row;
        empty = false;
      }

      if(OB_ITER_END == ret && !empty)
      {
        create_row_key(last_row, start_rowkey);
        allocator_lock_.lock();
        start_rowkey.deep_copy(mark->start_rowkey, allocator_);
        allocator_lock_.unlock();
        TBSYS_LOG(TRACE, "mark->start_rowkey = %s", to_cstring(mark->start_rowkey));
        ret = OB_SUCCESS;
      }
      else if(empty)
      {
        TBSYS_LOG(INFO, "current scanner is empty!");
        ret = OB_SUCCESS;
      }
      else
      {
        TBSYS_LOG(ERROR, "get next row failed! ret= [%d]", ret);
      }

      if(OB_SUCCESS != ret)
      {
        TBSYS_LOG(ERROR, "producer occurs failure, exit!");
        err_code_->set_err_code(OB_ERROR);
        thread_post_end_session();
        return ComsumerQueue<TabletBlock*>::QUEUE_QUITING;
      }
      else if(has_next)
      {
        TBSYS_LOG(INFO, "there are left packets should be received!");
        mark->next_sql_request = false;
        mark->next_packet = true;
        mark->is_last_timeout = false;
      }
      else
      {
        TBSYS_LOG(INFO, "there are no left packet should be received!");
        //mark->next_sql_request = true;
        //mark->next_packet = true;
        //只有一个sql 收到所有数据包 就退出
        mark->next_sql_request = false;
        mark->next_packet = false;
        mark->is_last_timeout = false;
        thread_remove_server_session();
      }
      ret = ComsumerQueue<TabletBlock*>::QUEUE_SUCCESS;
    }
  }
  else if(!mark->next_packet && !mark->next_sql_request)
  {
    TBSYS_LOG(INFO, "no execute sql left, work done!");
    ret = ComsumerQueue<TabletBlock*>::QUEUE_QUITING;
  }
  return ret;
}

//数据拉取的执行体，此函数执行一次相当于获取一个packet
int ExportProducer::produce(TabletBlock* &block)
{
  //获取当前线程私有的单例ProduceMark对象，需要此对象中的标记信息来作判断
  ProduceMark *mark = GET_TSI_MULT(ProduceMark, 1);

  int ret = ComsumerQueue<TabletBlock*>::QUEUE_SUCCESS;
  TBSYS_LOG(DEBUG, "enter produce");
  block = NULL;

  //判断当前的全局err_code_是不是正常状态，如果是异常状态则结束当前线程
  //单个tablet的sql执行失败（超时），不使整个进程退出，继续执行其他tablet的sql
  if(OB_SUCCESS != err_code_->get_err_code() && 0)//mod qianzm [export_by_tablets] 20160415
  {
    TBSYS_LOG(INFO, "some other thread exit with fail code! current thread should exit!");
    return ComsumerQueue<TabletBlock*>::QUEUE_QUITING;
  }
  //add qianzm [export_by_tablets] 20160415:b
  //设置一个新的err_code,当使用不同tablet数据写入不同文件功能时，若出现除了超时的其他错误时终止整个程序
  //当未使用不同tablet数据写入不同文件功能时，出现任何错误都终止整个程序
  if (OB_SUCCESS != err_code_->get_exit_code())
  {
    TBSYS_LOG(INFO, "some other thread exit with fail code! current thread should exit!");
    return ComsumerQueue<TabletBlock*>::QUEUE_QUITING;
  }
  //add 20160415:e
  else if(mark->next_sql_request && mark->next_packet)//前一个查询结束，进入下一个查询
  {
    string sql_part1 = "";
    string sql_part2 = "";

    sql_part1 = table_param_.export_sql;

    if(!mark->is_last_timeout)//检查上次查询是否是超时失败，如果不是就执行正常的获取tabletinfo流程来构造sql
    {
      //判断队列是否空，如果空则表示所有任务处理完成，则退出线程，否则从queue中取出一个tabletInfo对象，拼接到sql中
      tablet_cache_lock_.lock();
      if(tablet_cache_.empty())
      {
        TBSYS_LOG(INFO, "tablet_info is empty! thread exit!");

        mark->next_sql_request = false;
        mark->next_packet = false;
        mark->is_last_timeout = false;
        mark->last_timeout = timeout_us_;

        tablet_cache_lock_.unlock();
        return ComsumerQueue<TabletBlock*>::QUEUE_QUITING;
      }

      mark->is_last_timeout = false;
      mark->last_timeout = timeout_us_;
      //TBSYS_LOG(INFO, "qianzm-test timeout_us_ = [%ld]", timeout_us_);
      TabletCache::iterator iter = tablet_cache_.begin();
      ObExportTabletInfo ti = iter->second;
      tablet_cache_.erase(iter);
      TBSYS_LOG(DEBUG, "pop a tabletinfo from tablet_cache_, tabletinfo_.get_start_key_: %s, tabletinfo_.get_end_key_: %s", 
          	to_cstring(ti.get_start_key()), to_cstring(ti.get_end_key()));
      tablet_cache_lock_.unlock();

      //将rowkey信息保存到线程单例mark对象中
      allocator_lock_.lock();
      ti.get_start_key().deep_copy(mark->start_rowkey, allocator_);
      ti.get_end_key().deep_copy(mark->last_rowkey, allocator_);
      allocator_lock_.unlock();

      //根据拿出来的tabletinfo拼接sql
      if(OB_SUCCESS != (ret = parse_key_to_sql(ti.get_start_key(), ti.get_end_key(), sql_part2)))
      {
        TBSYS_LOG(ERROR, "parse_key_to_sql failed! ret=[%d]", ret);
        err_code_->set_err_code(OB_ERROR);
        err_code_->set_exit_code(OB_ERROR);//add qianzm [export_by_tablets] 20160415
        return ComsumerQueue<TabletBlock*>::QUEUE_QUITING;
      }
      else
      {
          //add qianzm [export_by_tablets] 20160415:b
          mark->tablet_index_ = ti.get_tablet_index();
          mark->err_sql_ = sql_part1 + " " + sql_part2;
          //add 20160415:e

          //add by zhuxh [timeout mechanism] 20170512 :b
          /*
          We set the max time limit according to tablet size.
          So max duration is tablet size divided by 6 MB/s, the min velocity I estimate.
          It shouldn't be too small so I set max_limittime_ as the min duration, able to be set by user, 1 hour as default.
          */
          int64_t tablet_timeout = max( ti.occupy_size_ * 1000000 / (6<<20) , max_limittime_ );
          mark->deadline = tbsys::CTimeUtil::getTime() + tablet_timeout;
          TBSYS_LOG(INFO, "Tablet %d, %ld B ( %ld MB ), has %ld us ( %ld min ) as max duration to export", mark->tablet_index_, ti.occupy_size_, ti.occupy_size_ >> 20, tablet_timeout, tablet_timeout / 1000000 / 60);
          //add by zhuxh [timeout mechanism] 20170512 :e
      }
    }
    else //如果上次查询是超时失败，则根据存储在mark中的rowkey信息构造sql，并把超时时间调大
    {
      //TBSYS_LOG(WARN, "last get_result is timeout, start a new query by last rowkey!");
	  TBSYS_LOG(WARN, "last get_result of tablet[%d] is timeout, start a new query by last rowkey!", mark->tablet_index_);
      //将mark中相关的标记量重新赋值
      mark->next_sql_request = false;
      mark->next_packet = false;
      mark->is_last_timeout = false;
      //mark->last_timeout += SPAN_TIMEOUT_US; //Timeout duration shouldn't increase. //mod by zhuxh [timeout mechanism] 20170512

      //如果调大后的超时时间已经大于最大的超时时间，则报错
      
      //mod by zhuxh [timeout mechanism] 20170512 :b
      //if(mark->last_timeout > max_limittime_)
      if( tbsys::CTimeUtil::getTime() >= mark->deadline )
      {
        //TBSYS_LOG(ERROR, "current timeout[%ld] exceeds the MAX_TIMEOUT_US[%ld], failed!", mark->last_timeout, max_limittime_);
        TBSYS_LOG(ERROR, "Tablet export timeout.");
      //mod by zhuxh [timeout mechanism] 20170512 :e
        err_code_->set_err_code(OB_ERROR);
		
        if (!is_multi_writers_) err_code_->set_exit_code(OB_ERROR);//add qianzm 20160705

        //add qianzm [export_by_tablets] 20160415:b
        tablet_cache_lock_.lock();
        /*现在断点续传功能可能存在问题，所以暂时不用断点处的rowkey构造sql，超时的tablet整个重新导出；
         *若断点功能正确，则已导出数据文件是有效的，可根据断点rowkey构造的sql实现续传。
        if (OB_SUCCESS == (parse_key_to_sql(mark->start_rowkey, mark->last_rowkey, sql_part2)))
        {
            string sql = sql_part1 + " " + sql_part2 + ";";
            failed_sql_.push_back(sql);
        }

        The passage above is incorrect.
        It it left part of timeout tablet that will export instead of whole one.
        */
        failed_sql_.push_back(mark->err_sql_);
        failed_tablets_index_.push_back( mark->tablet_index_);

        if ((int)failed_tablets_index_.size() > (int)(tablet_index_ / 2))
        {
            fprintf(stderr,"\nMore than 50 percent data files[%d] is incorrect,you should stop it!!!\n\n", (int)failed_tablets_index_.size());
        }
        tablet_cache_lock_.unlock();
        TBSYS_LOG(ERROR, "tablet[%d] get_result failed!!!", mark->tablet_index_);

        mark->next_sql_request = true;
        mark->next_packet = true;
        mark->is_last_timeout = false;

        //修改这个返回值，当前的tablet导出错误，不导致这个线程退出，而是取下一tablet继续做导出任务
        //因为现在不同的tablet写不同的文件，各个tablet之间数据不会相互影响
        //return ComsumerQueue<TabletBlock*>::QUEUE_ERROR;
        //add 20160415:e

        //return ComsumerQueue<TabletBlock*>::QUEUE_QUITING;
		//mod qianzm 20160705:b
		//当用到不同tablet数据写入不同文件的功能时，此时返回错误码应为QUEUE_ERROR；否则返回QUEUE_QUITING
		if (is_multi_writers_)
		    return ComsumerQueue<TabletBlock*>::QUEUE_ERROR;
		else
		    return ComsumerQueue<TabletBlock*>::QUEUE_QUITING;
	    // mod 20160705:e
      }

      TBSYS_LOG(TRACE, "mark->start_rowkey=%s mark->last_rowkey=%s", to_cstring(mark->start_rowkey), to_cstring(mark->last_rowkey));
      if(OB_SUCCESS != (ret = parse_key_to_sql(mark->start_rowkey, mark->last_rowkey, sql_part2)))
      {
        TBSYS_LOG(ERROR, "parse_key_to_sql failed! ret=[%d]", ret);
        err_code_->set_err_code(OB_ERROR);
        err_code_->set_exit_code(OB_ERROR);//add qianzm [export_by_tablets] 20160415
        return ComsumerQueue<TabletBlock*>::QUEUE_QUITING;
      }
    }

    //将得到的sql进行拼接，第一部分为用户填写的sql语句，第二部分为程序根据mark中的rowkey信息或者tabletinfo中的rowkey信息拼接得到的range后缀信息
    string sql = sql_part1 + " " + sql_part2;
    //TBSYS_LOG(INFO, "query sql: %s", sql.c_str());
	TBSYS_LOG(INFO, "tablet[%d]:query sql: %s", mark->tablet_index_, sql.c_str());
    
    //获取一台可用的ms
    ObServer ms = get_one_ms();
    TBSYS_LOG(INFO, "get one ms: %s, timeout is %ld", to_cstring(ms), mark->last_timeout);

    //发起sql查询
    if(OB_SUCCESS != (ret = db_->init_execute_sql(ms, sql, mark->last_timeout, mark->last_timeout - 5*1000*1000)))
    {
      TBSYS_LOG(ERROR, "init_execute_sql failed! ret=%d", ret);
      err_code_->set_err_code(OB_ERROR);
	  
      if (!is_multi_writers_) err_code_->set_exit_code(OB_ERROR);//add qianzm 20160705

      return ComsumerQueue<TabletBlock*>::QUEUE_QUITING;
    }

    TBSYS_LOG(DEBUG, "init_execute_sql success!");

    bool has_next = true;
    ObSQLResultSet result_set;

    //新分配一个TabletBlock将databuffer拷贝到该对象中
    TabletBlock *tmp_block = new TabletBlock();
    if(NULL == tmp_block)
    {
      TBSYS_LOG(ERROR, "allocate memory for tmp_block failed! ret=[%d]", ret);
      err_code_->set_err_code(OB_ERROR);
	  
      if (!is_multi_writers_) err_code_->set_exit_code(OB_ERROR);//add qianzm 20160705

      return ComsumerQueue<TabletBlock*>::QUEUE_QUITING;
    }

    //TabletBlock对象使用之前需要调用init
    if(OB_SUCCESS != (ret = tmp_block->init()))
    {
      TBSYS_LOG(ERROR, "first tmp_block init failed! ret=[%d]", ret);
      err_code_->set_err_code(OB_ERROR);
	  
      if (!is_multi_writers_) err_code_->set_exit_code(OB_ERROR);//add qianzm 20160705

      delete tmp_block;
      return ComsumerQueue<TabletBlock*>::QUEUE_QUITING;
    }

    TBSYS_LOG(DEBUG, "first tmp_block->init success!");

    ObDataBuffer& out_buffer = tmp_block->get_data_buffer();

    if(OB_SUCCESS != (ret = db_->get_result(result_set, out_buffer, has_next)))
    {
      //add qianzm [export_by_tablets] 20160415:b
      //TBSYS_LOG(ERROR, "get result from mergeserver failed! ret=%d", ret);
	  TBSYS_LOG(ERROR, "get result of tablet[%d] from mergeserver failed! ret=%d", mark->tablet_index_, ret);
      tablet_cache_lock_.lock();
      failed_sql_.push_back(mark->err_sql_);
      failed_tablets_index_.push_back( mark->tablet_index_);
      tablet_cache_lock_.unlock();
      //add 20160415:e

      err_code_->set_err_code(OB_ERROR);
	  
      if (!is_multi_writers_) err_code_->set_exit_code(OB_ERROR);//add qianzm 20160705

      ret = ComsumerQueue<TabletBlock*>::QUEUE_QUITING;
    }
    else
    {
      TBSYS_LOG(DEBUG, "first get a packet success!");

      thread_add_server_session();

      //初始化rowkey_idx_，进程全局只会初始化一次
      if(!is_rowkey_idx_inited_)
      {
        rowkey_idx_inited_lock_.lock();
        if(!is_rowkey_idx_inited_)
        {
          if(OB_SUCCESS != (ret = init_rowkey_idx(result_set.get_fields())))
          {
            TBSYS_LOG(ERROR, "init_rowkey_idx failed!");
            rowkey_idx_inited_lock_.unlock();
            err_code_->set_err_code(OB_ERROR);
            err_code_->set_exit_code(OB_ERROR);//add qianzm [export_by_tablets] 20160415
            return ComsumerQueue<TabletBlock*>::QUEUE_QUITING;
          }
          TBSYS_LOG(INFO, "init_rowkey_idx successed!");
          is_rowkey_idx_inited_ = true;
        }
        rowkey_idx_inited_lock_.unlock();
      }

      //初始化rowformator中的成员，这个只能在有结果集时才能调用
      if(!is_rowformator_result_field_inited_)
      {
        rowformator_result_field_lock_.lock();
        if(!is_rowformator_result_field_inited_)
        {
          if(OB_SUCCESS != (ret = rowformator_->init_result_fields_map(result_set.get_fields())))
          {
            TBSYS_LOG(ERROR, "rowformator->init_result_fields_map failed! ret=%d", ret);
            rowformator_result_field_lock_.unlock();
            err_code_->set_err_code(OB_ERROR);
            err_code_->set_exit_code(OB_ERROR);//add qianzm [export_by_tablets] 20160415
            return ComsumerQueue<TabletBlock*>::QUEUE_QUITING;
          }
          TBSYS_LOG(INFO, "rowformator->init_result_fields_map success!");
          is_rowformator_result_field_inited_ = true;
        }
        rowformator_result_field_lock_.unlock();
      }
	 //设置TabletBlock的index,标记该block应写入哪个文件中
      tmp_block->set_tablet_index(mark->tablet_index_);//add qianzm [export_by_tablets] 20160415

     //将新分配的TabletBlock内存地址传回
      block = tmp_block;

      //add qianzm [export_by_tablets] 20160415:b
      //第一个包也有数据返回，获取最后一行的row，存储到mark中，以备下次调用get_result时发生超时错误时使用
      ObRow row;
      ObRow last_row;
      ObRowkey start_rowkey;
      bool empty = true;

      //获取最后一行的row，存储到mark中，以备下次调用get_result时发生超时错误时使用
      while(OB_SUCCESS == (ret = result_set.get_new_scanner().get_next_row(row)))
      {
        last_row = row;
        empty = false;
      }

      if(OB_ITER_END == ret && !empty)
      {
        create_row_key(last_row, start_rowkey);
        allocator_lock_.lock();
        start_rowkey.deep_copy(mark->start_rowkey, allocator_);
        allocator_lock_.unlock();
        TBSYS_LOG(TRACE, "mark->start_rowkey = %s", to_cstring(mark->start_rowkey));
        ret = OB_SUCCESS;
      }
      else if(empty)
      {
        TBSYS_LOG(INFO, "current scanner is empty!");
        ret = OB_SUCCESS;
      }
      else
      {
        TBSYS_LOG(ERROR, "get next row failed! ret= [%d]", ret);
      }

      if(OB_SUCCESS != ret)
      {
        TBSYS_LOG(ERROR, "producer occurs failure, exit!");
        err_code_->set_err_code(OB_ERROR);
		
        if (!is_multi_writers_) err_code_->set_exit_code(OB_ERROR);//add qianzm 20160705

        thread_post_end_session();
        return ComsumerQueue<TabletBlock*>::QUEUE_QUITING;
      }
      //add 20160415:e

      if(has_next)
      {
        mark->next_sql_request = false;
        mark->next_packet = true;
        mark->is_last_timeout = false;
      }
      else
      {
        mark->next_sql_request = true;
        mark->next_packet = true;
        mark->is_last_timeout = false;
        thread_remove_server_session();
      }
      ret = ComsumerQueue<TabletBlock*>::QUEUE_SUCCESS;
    }
  }
  else if(mark->next_packet && !mark->next_sql_request)//当前查询还没有结束，继续获取下一个包
  {
    bool has_next = true;
    ObSQLResultSet result_set;

    //新分配一个TabletBlock将databuffer拷贝到该对象中
    TabletBlock *tmp_block = new TabletBlock();
    if(NULL == tmp_block)
    {
      TBSYS_LOG(ERROR, "allocate memory for tmp_block failed! ret=%d", ret);
      err_code_->set_err_code(OB_ERROR);
	  
      if (!is_multi_writers_) err_code_->set_exit_code(OB_ERROR);//add qianzm 20160705

      thread_post_end_session();
      return ComsumerQueue<TabletBlock*>::QUEUE_QUITING;
    }

    if(OB_SUCCESS != (ret = tmp_block->init()))
    {
      TBSYS_LOG(ERROR, "next tmp_block init failed! ret=%d", ret);
      err_code_->set_err_code(OB_ERROR);
	  
      if (!is_multi_writers_) err_code_->set_exit_code(OB_ERROR);//add qianzm 20160705

      delete tmp_block;
      thread_post_end_session();
      return ComsumerQueue<TabletBlock*>::QUEUE_QUITING;
    }

    TBSYS_LOG(DEBUG, "next tmp_block init success!");

    ObDataBuffer& out_buffer = tmp_block->get_data_buffer();

    if(OB_SUCCESS != (ret = db_->get_result(result_set, out_buffer, has_next)))
    {
      //对于超时这个错误情况，需要重试
      if(OB_RESPONSE_TIME_OUT == ret)
      {
        mark->is_last_timeout = true;
        mark->next_sql_request = true;
        mark->next_packet = true;
        thread_post_end_session();
        TBSYS_LOG(WARN, "current get_reuslt is timeout! timeout_us is %ld", mark->last_timeout);
      }
      else
      {
        //add qianzm [export_by_tablets] 20160415:b
        //TBSYS_LOG(ERROR, "get result from mergeserver failed! ret=%d", ret);
        TBSYS_LOG(ERROR, "get result of tablet[%d] from mergeserver failed! ret=%d", mark->tablet_index_, ret);
        tablet_cache_lock_.lock();
        failed_sql_.push_back(mark->err_sql_);
        failed_tablets_index_.push_back( mark->tablet_index_);
        tablet_cache_lock_.unlock();
        //add 20160415:e
        err_code_->set_err_code(OB_ERROR);
		
        if (!is_multi_writers_) err_code_->set_exit_code(OB_ERROR);//add qianzm 20160705

        delete tmp_block;
        thread_post_end_session();
        return ComsumerQueue<TabletBlock*>::QUEUE_QUITING;
      }
    }
    else
    {
      TBSYS_LOG(DEBUG, "next get a packet success!");
	  //设置TabletBlock的index,标记该block应写入哪个文件中
      tmp_block->set_tablet_index(mark->tablet_index_);//add qianzm [export_by_tablets] 20160415

      //将新分配的TabletBlock内存地址传回
      block = tmp_block;

      ObRow row;
      ObRow last_row;
      ObRowkey start_rowkey;
      bool empty = true;

      //获取最后一行的row，存储到mark中，以备下次调用get_result时发生超时错误时使用
      while(OB_SUCCESS == (ret = result_set.get_new_scanner().get_next_row(row)))
      {
        last_row = row;
        empty = false;
      }

      if(OB_ITER_END == ret && !empty)
      {
        create_row_key(last_row, start_rowkey);
        allocator_lock_.lock();
        start_rowkey.deep_copy(mark->start_rowkey, allocator_);
        allocator_lock_.unlock();
        TBSYS_LOG(TRACE, "mark->start_rowkey = %s", to_cstring(mark->start_rowkey));
        ret = OB_SUCCESS;
      }
      else if(empty)
      {
        TBSYS_LOG(INFO, "current scanner is empty!");
        ret = OB_SUCCESS;
      }
      else
      {
        TBSYS_LOG(ERROR, "get next row failed! ret= [%d]", ret);
      }

      if(OB_SUCCESS != ret)
      {
        TBSYS_LOG(ERROR, "producer occurs failure, exit!");
        err_code_->set_err_code(OB_ERROR);
		
        if (!is_multi_writers_) err_code_->set_exit_code(OB_ERROR);//add qianzm 20160705

        thread_post_end_session();
        return ComsumerQueue<TabletBlock*>::QUEUE_QUITING;
      }
      else if(has_next)
      {
        TBSYS_LOG(INFO, "there are left packets should be received!");
        mark->next_sql_request = false;
        mark->next_packet = true;
        mark->is_last_timeout = false;
      }
      else
      {
        TBSYS_LOG(INFO, "there are no left packet should be received!");
        mark->next_sql_request = true;
        mark->next_packet = true;
        mark->is_last_timeout = false;
        thread_remove_server_session();
      }
      ret = ComsumerQueue<TabletBlock*>::QUEUE_SUCCESS;
    }
  }
  else if(!mark->next_packet && !mark->next_sql_request)
  {
    TBSYS_LOG(INFO, "no execute sql left, work done!");
    ret = ComsumerQueue<TabletBlock*>::QUEUE_QUITING;
  }
  return ret;
}

int ExportProducer::thread_add_server_session()
{
  int ret = OB_SUCCESS;
  ObServer server;
  int64_t session_id;
  int producer_id;
  ProduceMark *mark = GET_TSI_MULT(ProduceMark, 1);
  if(OB_SUCCESS != (ret = db_->get_thread_server_session(server, session_id)))
  {
    TBSYS_LOG(ERROR, "db_->get_thread_server_session(server, session_id) failed! ret=[%d]", ret);
  }
  producer_id = mark->producer_index;
  if(OB_EXPORT_SUCCESS != ac_server_session.push_by_id(producer_id, server, session_id))
  {
    TBSYS_LOG(ERROR, "ac_server_session.push_by_id(producer_id, server, session_id) failed!");
    ret = OB_ERROR;
  }
  return ret;
}

int ExportProducer::thread_remove_server_session()
{
  int ret = OB_SUCCESS;
  ObServer server;
  int64_t session_id;
  int producer_id;
  ProduceMark *mark = GET_TSI_MULT(ProduceMark, 1);
  producer_id = mark->producer_index;
  if(OB_EXPORT_EXIST != ac_server_session.pop_by_producer_id(producer_id, server, session_id))
  {
    TBSYS_LOG(WARN, "server session not exist, current producer has lost its server_session!");
  }
  return ret;
}

int ExportProducer::thread_post_end_session()
{
  int ret = OB_SUCCESS;
  ObServer server;
  int64_t session_id;
  ProduceMark *mark = GET_TSI_MULT(ProduceMark, 1);
  int producer_id = mark->producer_index;
  if(OB_EXPORT_NOT_EXIST != ac_server_session.pop_by_producer_id(producer_id, server, session_id))
  {
    if(OB_SUCCESS != (ret = db_->post_end_next(server, session_id)))
    {
      TBSYS_LOG(ERROR, "current producer %d post end next failed! ret=[%d]", producer_id, ret);
    }
  }
  else
  {
    TBSYS_LOG(WARN, "server session not exist, current producer has lost its server_session!");
    ret = OB_SUCCESS;
  }
  return ret;
}

// 初始化ExportProducer内部部分对象
int ExportProducer::init()
{
  int ret = OB_SUCCESS;

  last_end_key_.set_min_row();
  //add qianzm [multi-tables] 20151228
  if (table_param_.is_multi_tables_ != 0)
  {
    timeout_us_ = db_->get_timeout_us();
  }
  //add e
  else
  {
    if(OB_SUCCESS != (ret = get_tablet_info()))
    {
      TBSYS_LOG(ERROR, "get_tablet_info failed! please check the table name! ret=[%d]", ret);
    }
    timeout_us_ = db_->get_timeout_us();
  }
  return ret;
}

// 将startrowkey和endkey格式化为一个range()的字符串，作为可以拼接到sql中一部分
int ExportProducer::parse_key_to_sql(const ObRowkey& startkey, const ObRowkey& endkey, string& out_range_str)
{
  int ret = OB_SUCCESS;
  char buffer[2048];
  string range_str = "(";
  string startkey_str;
  string endkey_str;
  if(OB_SUCCESS != (ret = parse_rowkey(startkey, buffer, 2048)))
  {
    TBSYS_LOG(ERROR, "parse rowkey failed! ret=[%d]", ret);
  }
  else
  {
    startkey_str = buffer;
  }

  if(OB_SUCCESS != (ret = parse_rowkey(endkey, buffer, 2048)))
  {
    TBSYS_LOG(ERROR, "parse rowkey failed! ret=[%d]", ret);
  }
  else
  {
    endkey_str = buffer;
  }

  if(OB_SUCCESS != ret)
  {
    TBSYS_LOG(ERROR, "parse tabletinfo failed! ret=[%d]", ret);
  }
  else if(startkey_str == "MIN" && endkey_str == "MAX")
  {
    range_str = "";
  }
  else if(startkey_str == "MIN")
  {
    range_str = "range(," + endkey_str + ")";
  }
  else if(endkey_str == "MAX")
  {
    range_str = "range(" + startkey_str +",)";
  }
  else
  {
    range_str = "range(" + startkey_str + "," + endkey_str + ")";
  }

  if(OB_SUCCESS == ret)
  {
    out_range_str = range_str;
  }

  return ret;
}

//当前获取ms的策略是每个线程单独用一个ms，线程数与ms数相同,线程获取ms时按照当前线程的
//producer_index(对每个producer是唯一的),按照这个producer_index去ms_vec_中定位
ObServer ExportProducer::get_one_ms()
{
  ObServer ms;
  assert(ms_vec_.size() != 0);

  ProduceMark *mark = GET_TSI_MULT(ProduceMark, 1);
  int ms_index = mark->producer_index;
  ms = ms_vec_[ms_index];

  TBSYS_LOG(DEBUG, "ms=%s", to_cstring(ms));
  return ms;
}

/**
 * @author liyongfeng:20141120
 * @brief ExportProducer::get_tablet_info 获取tablet信息
 * @return 成功,返回OB_SUCCESS;否则,返回非OB_SUCCESS
 */
int ExportProducer::get_tablet_info()
{
    int ret = OB_SUCCESS;
    char *p_buffer = new(std::nothrow) char[kDefaultResultBufferSize];
    if (NULL == p_buffer) {
        return OB_MEM_OVERFLOW;
    }   

    ObDataBuffer ob_buffer;
    ob_buffer.set_data(p_buffer, kDefaultResultBufferSize);

    //ObRowkey start_key;
    //start_key.set_min_row();

    ObScanner scanner;
    do {
        ret = db_->get_tablet_info(table_param_.table_name, last_end_key_, ob_buffer, scanner);
        if (ret != OB_SUCCESS) {
            TBSYS_LOG(ERROR, "get_tablet_info failed! ret=[%d]", ret);
            break;
        }   

        ret = parse_scanner(scanner);
        if (ret != OB_SUCCESS) {
            TBSYS_LOG(ERROR, "parse tablet info from ObScanner failed, ret=[%d]", ret);
            break;
        } else if (ObRowkey::MAX_ROWKEY == last_end_key_) {
            TBSYS_LOG(INFO, "get all tablets info from rootserver success!");
            break;
        } else {
            TBSYS_LOG(DEBUG, "need more request to get next tablet info!");
            scanner.reset();
        }   

    } while (true);

    //add qianzm [export_by_tablets]20160415:b
    if (OB_SUCCESS == ret)
    {
        int tablet_cnt = 0;
        TabletCache::iterator iter = tablet_cache_.begin();
        while (iter != tablet_cache_.end())
        {
            tablet_cnt ++;
            iter ++;
        }
        TBSYS_LOG(INFO, "This table has %d tablets", tablet_cnt);
    }
    //add 20160415:e
    if (p_buffer != NULL) {
        delete[] p_buffer;
        p_buffer = NULL;
    }   

    return ret;
}
/**
 * @author liyongfeng:20141120
 * @brief ExportProducer::parse_scanner 解析ObScanner,进而获取所有tablet信息,并将最后一个tablet的end_key记录下来
 * @param scanner 传入参数,存储所有tablet信息
 * @return 成功,返回OB_SUCCESS;否则,返回非OB_SUCCESS
 */
int ExportProducer::parse_scanner(ObScanner &scanner)
{
    int ret = OB_SUCCESS;

    ObRowkey start_key;
    start_key = ObRowkey::MIN_ROWKEY;

    ObCellInfo *cell = NULL;
    bool row_change = false;

    DbRecord record;
    record.reset();

    TBSYS_LOG(DEBUG, "root server get rpc return succeed, cell num=%ld", scanner.get_cell_num());
    oceanbase::common::dump_scanner(scanner, TBSYS_LOG_LEVEL_DEBUG, 0);

    ObScannerIterator iter = scanner.begin();
    ++iter;
    while ((iter != scanner.end()) && (OB_SUCCESS == (ret = iter.get_cell(&cell, &row_change))) && !row_change) {
        if (NULL == cell) {
            ret = OB_INNER_STAT_ERROR;
            break;
        }
        allocator_lock_.lock();
        cell->row_key_.deep_copy(start_key, allocator_);
        allocator_lock_.unlock();
        ++iter;
    }
    record.append_column(*cell);
   
    if (OB_SUCCESS == ret) {
        for (++iter; iter != scanner.end(); ++iter) {
            ret = iter.get_cell(&cell, &row_change);
            if (ret != OB_SUCCESS) {
                TBSYS_LOG(ERROR, "get cell from scanner iterator failed, ret=[%d]", ret);
                break;
            }
            else if (row_change) {
                ObExportTabletInfo tablet_info;
                ret = tablet_info.build_tablet_info(start_key, &record);
                if (ret != OB_SUCCESS) {
                    TBSYS_LOG(ERROR, "build tablet info failed, ret=[%d]", ret);
                    break;
                }
                //add qianzm [export_by_tablets] 20160415:b
                tablet_info.set_tablet_index(tablet_index_);
                tablet_index_ ++;
                //add 20160415:e
                insert_tablet_cache(tablet_info.get_end_key(), tablet_info);
                allocator_lock_.lock();
                tablet_info.get_end_key().deep_copy(start_key, allocator_);
                allocator_lock_.unlock();
                record.reset();

                record.append_column(*cell);
            } else {
                //append one cell into DbRecord
                record.append_column(*cell);
            }
        }
        if (OB_SUCCESS == ret) {
            ObExportTabletInfo tablet_info;
            ret = tablet_info.build_tablet_info(start_key, &record);
            if (ret != OB_SUCCESS) {
                TBSYS_LOG(ERROR, "build tablet info failed, ret=[%d]", ret);
            } else {
                //add qianzm [export_by_tablets] 20160415:b
                tablet_info.set_tablet_index(tablet_index_);
                tablet_index_ ++;
                //add 20160415:e
                insert_tablet_cache(tablet_info.get_end_key(), tablet_info);
                allocator_lock_.lock();
                tablet_info.get_end_key().deep_copy(start_key, allocator_);
                allocator_lock_.unlock();
            }
            record.reset();
        }
        if (OB_SUCCESS == ret) {
            allocator_lock_.lock();
            start_key.deep_copy(last_end_key_, allocator_);
            allocator_lock_.unlock();
        }
    }
    return ret;
}
/**
 * @author liyongfeng:20141120
 * @brief ExportProducer::insert_tablet_cache 将对应的tablet信息放到队列中
 * @param rowkey tablet的end_key
 * @param tablet tablet的所有信息
 * @return
 */
void ExportProducer::insert_tablet_cache(const ObRowkey &rowkey, ObExportTabletInfo &tablet)
{
    TabletCache::iterator iter = tablet_cache_.find(rowkey);
    if (iter != tablet_cache_.end()) {
        tablet_cache_.erase(iter);
        TBSYS_LOG(DEBUG, "delete cache tablet, end rowkey=[%s]", to_cstring(iter->first));
    }

    TBSYS_LOG(DEBUG, "insert cache tablet, end rowkey=[%s]", to_cstring(rowkey));
    tablet_cache_.insert(std::make_pair(rowkey, tablet));
}

//向给定字符串中的特殊字符前加上转义字符
int ExportProducer::add_escape_char(const char *str, const int str_len, char *out_buffer, int64_t capacity)
{
  char *dest_str = out_buffer;
  int i = 0;
  int j = 0;
  int ret = OB_SUCCESS;
  for(; i < str_len; i++)
  {
    if(str[i] == '\\')
    {
      dest_str[j++] = '\\';
      dest_str[j++] = '\\';
    }
    else if(str[i] == '\'')
    {
      dest_str[j++] = '\\';
      dest_str[j++] = '\'';
    }
    else
    {
      dest_str[j++] = str[i];
    }
    if(j > capacity)
    {
      ret = OB_ERROR;
    }
  }
  if(OB_SUCCESS == ret)
    dest_str[j] = '\0';
  return ret;
}

//将给定的rowkey格式化为一个以逗号隔开的字符串
int ExportProducer::parse_rowkey(const ObRowkey& rowkey, char *out_buffer, int64_t capacity)
{
  int ret = OB_SUCCESS;
  int64_t rowkey_num = rowkey.get_obj_cnt();
  const ObObj *ptr = rowkey.get_obj_ptr();
  int64_t reserve_size = capacity;
  int64_t write_size = 0;
  char *pos = out_buffer;
  if(rowkey == ObRowkey::MIN_ROWKEY)
  {
    snprintf(out_buffer, reserve_size, "MIN");
    return OB_SUCCESS;
  }
  if(rowkey == ObRowkey::MAX_ROWKEY)
  {
    snprintf(out_buffer, reserve_size, "MAX");
    return OB_SUCCESS;
  }
  for(int64_t i = 0; i < rowkey_num; i++)
  {
    if(i == 0)
    {
      write_size = snprintf(pos, reserve_size, "(");
      reserve_size -= write_size;
      pos += write_size;
    }

    switch(ptr[i].get_type())
    {
      case ObNullType:   // 空类型
        write_size = snprintf(pos, reserve_size, "NULL");
        reserve_size -= write_size;
        pos += write_size;
        break;
		//add qianzm 20160329:b
	  case ObInt32Type:
        {
          int32_t val = 0;
          ptr[i].get_int32(val);
          write_size = snprintf(pos, reserve_size, "%d", val);
          reserve_size -= write_size;
          pos += write_size;
        } 
        break;
		//add :e
      case ObIntType:
        {
          int64_t val = 0;
          ptr[i].get_int(val);
          write_size = snprintf(pos, reserve_size, "%ld", val);
          reserve_size -= write_size;
          pos += write_size;
        }
        break;
      case ObVarcharType:
        {
          ObString val;
          ptr[i].get_varchar(val);
//          TBSYS_LOG(INFO, "ptr[i] is %s, ptr[i].get_varchar is %s, val.length() = %d", to_cstring(ptr[i]), val.ptr(), val.length());
          char tmp_sql_buffer[2048];
          if(OB_SUCCESS != (ret = add_escape_char(val.ptr(), val.length(), tmp_sql_buffer, 2048)))
          {
            TBSYS_LOG(ERROR, "add_escape_char failed! ret=[%d]", ret);
          }
          else
          {
            write_size = snprintf(pos, reserve_size, "'%.*s'", static_cast<int>(strlen(tmp_sql_buffer)), tmp_sql_buffer);
            reserve_size -= write_size;
            pos += write_size;
          }
        }
        break;
      case ObCreateTimeType:
      case ObDateTimeType:
      case ObPreciseDateTimeType:
      case ObModifyTimeType:
      //add qianzm 20160426:b
      case ObDateType:
      case ObTimeType:
      //add 20160426:e
        {
          int64_t val = 0;
          ptr[i].get_timestamp(val);
          write_size = snprintf(pos, reserve_size, "%ld", val);
          reserve_size -= write_size;
          pos += write_size;
        }
        break;
     case ObDecimalType:            // aka numeric
        {
          ObString val;
          ptr[i].get_decimal(val);
          write_size = snprintf(pos, reserve_size, "%.*s", val.length(),val.ptr());
          reserve_size -= write_size;
          pos += write_size;
        }
        break;
#if 0
      case ObFloatType:              // @deprecated
        ret = OB_ERROR;
        break;
      case ObDoubleType:             // @deprecated
        ret = OB_ERROR;
        break;
      case ObSeqType:
        ret = OB_ERROR;
        break;
      case ObBoolType:
        ret = OB_ERROR;
        break;
#endif
      default:
        ret = OB_ERROR;
        break;
    }

    if ( OB_SUCCESS != ret )
    {
      TBSYS_LOG(ERROR, "parse rowkey error! ret=[%d]", ret);
      break;
    }

    if(i == rowkey_num - 1)
    {
      write_size = snprintf(pos, reserve_size, ")");
      reserve_size -= write_size;
      pos += write_size;
    }
    else
    {
      write_size = snprintf(pos, reserve_size, ",");
      reserve_size -= write_size;
      pos += write_size;
    }
  }
  return ret;
}

//初始化当前表的rowkey在结果集中的下标数组
int ExportProducer::init_rowkey_idx(const ObArray<ObResultSet::Field>& fields)
{
  int ret = OB_SUCCESS;

  vector<string> rowkey_column_names;
  if(OB_SUCCESS != (ret = get_rowkey_column_name(table_name_, schema_, rowkey_column_names)))
  {
    TBSYS_LOG(ERROR, "get_rowkey_column_name error!");
    return ret;
  }

  bool key_find = false;
  for(size_t i = 0; i < rowkey_column_names.size(); i++)
  {
    key_find = false;
    for(int j = 0; j < fields.count(); j++)
    {
      if(0 == strcasecmp(fields.at(j).cname_.ptr(), rowkey_column_names[i].c_str()))//mod by zhuxh:20151230 [strcasecmp]
      {
        rowkey_idx_.push_back(j);
        key_find = true;
        break;
      }
    }
    if(!key_find)
    {
      ret = OB_ERROR;
      TBSYS_LOG(ERROR, "rowkey column %s is not assign in sql!", rowkey_column_names[i].c_str());
      break;
    }
  }

  return ret;
}

//在给定的row中抽取出rowkey字段，构造成一个ObRowkey对象
int ExportProducer::create_row_key(const ObRow& row, ObRowkey &rowkey)
{
  int ret = OB_SUCCESS;
  uint64_t tid = 0;
  uint64_t cid = 0;
  const ObObj *cell = NULL;
  assert(rowkey_idx_.size() > 0);
  ObObj *obj_ptr = new ObObj[rowkey_idx_.size()];

  for(size_t i = 0; i < rowkey_idx_.size(); i++)
  {
    row.raw_get_cell(rowkey_idx_[i], cell, tid, cid);
    allocator_lock_.lock();
    ob_write_obj_v2(allocator_, *cell, obj_ptr[i]);
    allocator_lock_.unlock();
  }

  rowkey.assign(obj_ptr, rowkey_idx_.size());
  return ret;
}

//构造表的rowkey字段的列名数组
int ExportProducer::get_rowkey_column_name(const char* table_name, const ObSchemaManagerV2 *schema, vector<string> &rowkey_column_names)
{
  int ret = OB_SUCCESS;
  const ObRowkeyInfo &rowkey_info = schema->get_table_schema(table_name)->get_rowkey_info();
  const ObTableSchema* tmp_schema = schema->get_table_schema(table_name);
  //add qianzm  20160803:b
  if (tmp_schema == NULL)
  {
    TBSYS_LOG(ERROR,"can not find table schema of %s",table_name);
	ret = OB_ERROR;
  }
  else
  //add 20160803:e
  {
  uint64_t table_id = tmp_schema->get_table_id();
  for(int64_t i = 0; i < rowkey_info.get_size(); i++)
  {
    ObRowkeyColumn rowkey_column;
    rowkey_info.get_column(i, rowkey_column);
    int32_t idx = 0;
    const ObColumnSchemaV2* col_schema = schema->get_column_schema(table_id, rowkey_column.column_id_, &idx);
    rowkey_column_names.push_back(col_schema->get_name());
  }
  }
  return ret;
}

//add qianzm [export_by_tablets] 20160415:b
int ExportProducer::print_tablet_info_cache()
{
    int ret = OB_SUCCESS;
    if (!is_multi_writers_)
        return ret;
    if (failed_sql_.size() != 0 || tablet_cache_.begin() != tablet_cache_.end()){
        FileWriter fw1;
        string tablet_file_name;
        tablet_file_name = table_param_.tablet_info_file + ".failed_tablet_info";
        const char *line_end= "\n";
        if ((ret = fw1.init_file_name(tablet_file_name.c_str())) != OB_SUCCESS)
        {
            TBSYS_LOG(ERROR, "FileWrite init tablet_info_file failed, ret=[%d]", ret);
        }
        else
        {
            if (failed_sql_.size() != failed_tablets_index_.size())
            {
                //failed_sql_和failed_tablets_index_是同时添加的正常情况下大小应该是相等的
                //为了防止有异常情况发生，写了这个分支
                TBSYS_LOG(ERROR, "failed_sql_.size(%d) != failed_tablets_index_.size(%d)",
                          (int)failed_sql_.size(), (int)failed_tablets_index_.size());
                for(size_t i=0;i < failed_sql_.size(); i ++)
                {
                    int64_t len = (int64_t)failed_sql_[i].length();
                    const char *buff = new char[256];
                    buff = (failed_sql_[i].c_str());
                    fw1.write_records(buff, len);
                    fw1.write_records(line_end, 1);
                }
            }
            else
            {
                TBSYS_LOG(ERROR, "failed_sql count is %d ", (int)failed_sql_.size());
                //将所有超时的sql写入一个新的日志文件中
                for(size_t i=0;i < failed_sql_.size(); i ++)
                {
                    TBSYS_LOG(INFO, "The sql of failed_tablet[%d]: %s;", failed_tablets_index_[i], failed_sql_[i].c_str());

                    char buff2[128];
                    sprintf(buff2, "%d", failed_tablets_index_[i]);
                    string s2 = buff2;
                    string s1="#timeout sql of tablet[" + s2 + "]";

                    int64_t len2= static_cast<int64_t>(s1.length());
                    fw1.write_records(s1.c_str(), len2);
                    fw1.write_records(line_end, 1);

                    int64_t len = (int64_t)failed_sql_[i].length();
                    const char *buff = new char[256];
                    buff = (failed_sql_[i].c_str());
                    fw1.write_records(buff, len);
                    fw1.write_records(line_end, 1);
                }
            }
        }
        int index=0;
        if (OB_SUCCESS == ret)
        {
            //fw1.write_records(line_end, 1);
            TabletCache::iterator iter = tablet_cache_.begin();
            if (iter == tablet_cache_.end())
            {
                TBSYS_LOG(INFO, "tablet_cache_ is empty!");
            }

            //若有未执行的tablet的sql也写入新的日志文件中
            while (iter != tablet_cache_.end())
            {
                index++;
                ObExportTabletInfo ti = iter->second;
                string str1,sql;
                if (OB_SUCCESS != (parse_key_to_sql(ti.get_start_key(), ti.get_end_key(), str1)))
                {
                    TBSYS_LOG(ERROR, "parse tablet_cache_ to tablet_info_file failed!");
                    break;
                }
                else
                {
                    char buff2[128];
                    sprintf(buff2, "%d", ti.get_tablet_index());
                    string s2 = buff2;
                    string s1="#not execute sql of tablet[" + s2 + "]";

                    int64_t len2= static_cast<int64_t>(s1.length());
                    fw1.write_records(s1.c_str(), len2);
                    fw1.write_records(line_end, 1);

                    sql = table_param_.export_sql + " " + str1 + ";";
                    TBSYS_LOG(WARN, "Not executed sql of tablet[%d]: %s",
                              ti.get_tablet_index(), sql.c_str());
                    int64_t len = (int64_t)sql.length();
                    const char *buff = new char[256];
                    buff = (sql.c_str());
                    fw1.write_records(buff, len);
                    fw1.write_records(line_end, 1);
                }
                iter ++;
            }
        }
    }
    return ret;
}
//add 20160415:e

//int ExportProducer::get_execute_sql_by_tablet_info(std::string &exe_sql)
//{
//  int ret = OB_SUCCESS;
//
//  tablet_cache_lock_.lock();
//  if (tablet_cache_.empty())
//  {
//    tablet_cache_lock_.unlock();
//    return
//  }
//  else
//  {
//    TabletCache::iterator iter = tablet_cache_.begin();
//    ObExportTabletInfo tablet = iter->second;
//    tablet_cache_.erase(iter);
//    TBSYS_LOG(DEBUG, "pop a tablet from tablet cache, start key[%s], end key[%s]", to_cstring(tablet.get_start_key()), to_cstring(tablet.get_end_key()));
//    tablet_cache_lock_.unlock();
//  }
//
//  return ret;
//}
//
//int ExportProducer::get_execute_sql_by_mark(std::string &exe_sql)
//{
//
//}

//int ExportProducer::new_tablet_block(TabletBlock *&block)
//{
//  int ret = OB_SUCCESS;
//  TabletBlock *tmp_block = new TabletBlock();
//  block = NULL;
//
//  if(NULL == tmp_block)
//  {
//    TBSYS_LOG(ERROR, "allocate memory for tmp_block failed! ret=[%d]", ret);
//    return OB_ERROR;
//  }
//
//  //TabletBlock对象使用之前需要调用init
//  if(OB_SUCCESS != (ret = tmp_block->init()))
//  {
//    TBSYS_LOG(ERROR, "first tmp_block init failed! ret=[%d]", ret);
//    delete tmp_block;
//    return OB_ERROR;
//  }
//  block = tmp_block;
//  return ret;
//}
//

