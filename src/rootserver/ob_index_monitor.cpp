#include "ob_index_monitor.h"
#include "ob_root_worker.h"

namespace oceanbase
{
  namespace rootserver
  {

    ObIndexMonitor::ObIndexMonitor()
      :worker_(NULL),mission_start_time_(0),mission_end_time_(0),
        start_version_(OB_INVALID_VERSION),monitor_phase_(MONITOR_INIT),last_finished_(false)
    {}
    ObIndexMonitor::~ObIndexMonitor()
    {
    }
    void ObIndexMonitor::init(ObRootWorker *worker)
    {

      worker_ = worker;
    }

    void ObIndexMonitor::start()
    {
      tbsys::CThreadGuard lock(&monitor_mutex_);
      if(!is_monitor_start_)
      {
        is_monitor_start_ = true;
        TBSYS_LOG(INFO, "index monitor started!");
      }
      else
      {
        TBSYS_LOG(INFO, "the index monitor is already start");
      }
    }

    void ObIndexMonitor::stop()
    {
      is_monitor_start_ = false;
      running_ = false;
      success_index_num_ = 0;
      index_list_.clear();
      sorted_index_list_.clear();
      failed_index_.clear();//add liumz, [review_bugfix]20151230
      monitor_phase_ = MONITOR_INIT;
      start_version_ = OB_INVALID_VERSION;
      last_finished_ = true;//set here
    }

    bool ObIndexMonitor::is_start()
    {
      return is_monitor_start_;
    }

    int ObIndexMonitor::start_mission(const int64_t& start_version)
    {
      int ret = OB_SUCCESS;
      monitor_mutex_.lock();
      if(running_)
      {
        TBSYS_LOG(INFO, "monitor mission is already begin!");
        monitor_mutex_.unlock();
      }
      else
      {
        set_start_version(start_version);
        last_finished_ = false;//reset here
        running_ = true;
        monitor_mutex_.unlock();
        TBSYS_LOG(INFO, "common merge is complete, and start index mission now!");
        ret = commit_index_command();
      }
      return ret;
    }

    int ObIndexMonitor::commit_index_command()
    {
      int ret = OB_SUCCESS;
      if(NULL == worker_)
      {
        TBSYS_LOG(ERROR, "null pointer of worker!");
        ret = OB_ERROR;
      }
      else
      {
        ret = worker_->submit_index_task();
      }
      return ret;
    }

    void ObIndexMonitor::monitor()
    {
      int err = OB_SUCCESS;
      //如果有些索引创建错误，那么可以skip这些错误的索引，处理下一批
      //失败的索引状态依然为INIT，创建索引失败不该影响数据库
      for(int32_t i = 0;i < sorted_index_list_.size(); i++)
      {
        err = monitor_singal_idx(*(sorted_index_list_.at(i)));
        if(err != OB_SUCCESS)
        {
          TBSYS_LOG(WARN, "index[%ld] build failed.push to failed list", *(sorted_index_list_.at(i)));
          failed_index_.push_back(*(sorted_index_list_.at(i)));
        }
        else
        {
          success_index_num_ ++;
          TBSYS_LOG(INFO, "index[%ld] build success!", *(sorted_index_list_.at(i)));
        }        
      }
      worker_->get_root_server().reset_index_beat();
      //add liumz, [secondary index static_index_build] 20150427:b
      //reuse hist table after finish building all index
      if(OB_SUCCESS != (err = worker_->get_root_server().clean_hist_table()))
      {
        TBSYS_LOG(WARN, "clean hist table error, err=[%d]",err);
      }
      //add:e
      //return ret;
    }

    /*监控索引的创建流程，做的事情是
     *1.唤醒有cs去构建
     *2.等待cs返回的局部索引汇报信息完全，切分range，写入内部表
     *3.查全索引是否创建完全，修改索引表状
     */
    int ObIndexMonitor::monitor_singal_idx(uint64_t idx_id)
    {
      int ret = OB_SUCCESS;
      int try_time = 0;
      TBSYS_LOG(INFO, "Now monitor start index[%ld] constrcut!", idx_id);
      mission_start_time_ = tbsys::CTimeUtil::getTime();
      while(true)
      {
        try_time++;
        if(NULL == worker_)
        {
          TBSYS_LOG(ERROR, "the worker pointer is null");
          ret = OB_ERROR;
        }
        else if(OB_SUCCESS != (ret = calc_hist_width(idx_id, hist_width_)))
        {
          TBSYS_LOG(WARN, "calc hist width failed[%d]", ret);
        }
        else if(OB_SUCCESS != (ret = worker_->get_root_server().reuse_hist_table(idx_id)))
        {
          TBSYS_LOG(WARN, "reuse hist table error, ret=[%d]",ret);
        }
        else if(OB_SUCCESS != (ret = worker_->get_root_server().force_cs_create_index(idx_id, hist_width_, mission_start_time_)))
        {
          TBSYS_LOG(WARN, "send packet error,ret[%d]",ret);
        }
        else
        {
          monitor_phase_ = MONITOR_LOCAL;
          const int64_t sleep_interval = 10000000;
          int64_t now = tbsys::CTimeUtil::getTime();
          bool is_finished1 = false, is_finished2 = false/*, init_changed = false*/;
          bool need_delete_rt = false;
          //step1, build local static index是否超时
          while (!is_finished1)
          {
            now = tbsys::CTimeUtil::getTime();
            if (now > mission_start_time_ + worker_->get_root_server().get_config().monitor_create_index_timeout)
            {
              ret = OB_CS_STATIC_INDEX_TIMEOUT;
              TBSYS_LOG(WARN, "build local static index timeout, mission_start_time_=%ld, now=%ld.", mission_start_time_, now);
              break;
            }
            else if (OB_SUCCESS != (ret = worker_->get_root_server().check_create_local_index_done(idx_id, is_finished1)))
            {
              TBSYS_LOG(WARN, "check create local index[%lu] failed", idx_id);
              break;
            }
            else if (!is_finished1)
            {
              TBSYS_LOG(INFO, "building local static index [%lu], sleep %ldus and check again.", idx_id, sleep_interval);
              usleep(sleep_interval);
            }
          }

          if (OB_SUCCESS == ret)
          {
            monitor_phase_ = MONITOR_GLOABL;
            //step2, if histograms reported success, write global index range into rt
            common::ObArray<uint64_t> delete_table;
            if (OB_SUCCESS != (ret = delete_table.push_back(idx_id)))
            {
              TBSYS_LOG(WARN, "add idx_id to delete table failed. index_tid=%ld", idx_id);
            }
            else if (OB_SUCCESS != (ret = worker_->get_root_server().delete_tables(false, delete_table)))
            {
              TBSYS_LOG(WARN, "fail to delete index table from rt, index_tid=%ld, err=%d", idx_id, ret);
            }
            else if (OB_SUCCESS != (ret = worker_->get_root_server().fill_all_samples()))
            {
              TBSYS_LOG(WARN, "fail to fill all samples. ret=%d", ret);
            }
            else if (OB_SUCCESS != (ret = worker_->get_root_server().write_global_index_range(hist_width_)))
            {
              TBSYS_LOG(WARN, "fail to write global index range into root table, will delete index table from rt, index tid=%ld. ret=%d", idx_id, ret);
              need_delete_rt = true;
            }
            else
            {
              need_delete_rt = true;
              /*
              worker_->get_root_server().request_cs_report_tablet();//case: rs restart
              usleep(sleep_interval);
              worker_->get_root_server().dump_root_table();
              */
              //step3, check build global index finished?
              //查build global static index是否超时
              while (!is_finished2)
              {
                now = tbsys::CTimeUtil::getTime();
                if (now > mission_start_time_ + worker_->get_root_server().get_config().monitor_create_index_timeout)
                {
                  ret = OB_CS_STATIC_INDEX_TIMEOUT;
                  TBSYS_LOG(WARN, "build global static index timeout, mission_start_time_=%ld, now=%ld.", mission_start_time_, now);
                  break;
                }
                else if (OB_SUCCESS != (ret = worker_->get_root_server().check_create_global_index_done(idx_id, is_finished2)))
                {
                  TBSYS_LOG(WARN, "check create global index[%lu] failed", idx_id);
                  break;
                }
                else if (!is_finished2)
                {
                  TBSYS_LOG(INFO, "building global static index [%lu], sleep %ldus and check again.", idx_id, sleep_interval);
                  usleep(sleep_interval);
                }
              }//end while

              if (OB_SUCCESS == ret)
              {
                //mod liumz, [paxos static index]20170607:b
                bool is_cluster_alive[OB_CLUSTER_ARRAY_LEN];
                worker_->get_root_server().get_alive_cluster_with_cs(is_cluster_alive);
                for (int cluster_idx = 0; cluster_idx < OB_MAX_COPY_COUNT && OB_SUCCESS == ret; cluster_idx++)
                {
                  if (is_cluster_alive[cluster_idx+1])
                  {
                    //add liuxiao [secondary index col checksum]
                    //索引生成条件之二，列验和必须符
                    //mod liumz, [secondary index col checksum bugfix]20160622:b
                    bool is_right = true;
                    if(OB_SUCCESS != (ret = worker_->get_root_server().check_column_checksum(cluster_idx+1, idx_id, is_right)) || !is_right)
                    {
                      ret = OB_CHECKSUM_ERROR;
                      if (!is_right)
                      {
                        TBSYS_LOG(ERROR, "col checksum of org_table and index_table do not match index_table_id:%ld, cluster_id[%d]", idx_id, cluster_idx+1);
                      }
                      else
                      {
                        TBSYS_LOG(ERROR, "check column checksum failed, index id[%ld], cluster_id[%d]", idx_id, cluster_idx+1);
                      }
                    }
                    //mod:e
                    //add e
                    //step4.2, if build global index success, modify index table stat.
                    else if (OB_SUCCESS != (ret = worker_->get_root_server().trigger_balance_index(idx_id)))
                    {
                      TBSYS_LOG(WARN, "clean illusive info from rt failed. index_tid=%ld", idx_id);
                    }
                    else if (OB_SUCCESS != (ret = worker_->get_root_server().modify_index_process_info(cluster_idx+1, idx_id, NOT_AVALIBALE)))
                    {
                      TBSYS_LOG(WARN, "fail modify index table's stat to [NOT_AVALIBALE], index_tid=%ld, cluster_id[%d]", idx_id, cluster_idx+1);
                    }
                    else
                    {
                      usleep(sleep_interval);
                      /*while (!init_changed)
                      {
                        now = tbsys::CTimeUtil::getTime();
                        if (now > mission_start_time_ + worker_->get_root_server().get_config().monitor_create_index_timeout)
                        {
                          ret = OB_CS_STATIC_INDEX_TIMEOUT;
                          TBSYS_LOG(WARN, "wait index init stat change timeout, mission_start_time_=%ld, now=%ld.", mission_start_time_, now);
                          break;
                        }
                        else if (OB_SUCCESS != (ret = worker_->get_root_server().check_index_init_changed(idx_id, init_changed)))
                        {
                          TBSYS_LOG(WARN, "check index[%lu] init stat change failed", idx_id);
                          break;
                        }
                        else if (!init_changed)
                        {
                          TBSYS_LOG(INFO, "wait index [%lu] init stat change, sleep %ldus and check again.", idx_id, sleep_interval);
                          usleep(sleep_interval);
                        }
                      }//end while*/
                    }
                  }
                }
                //mod:e
              }
            }
          }
          //clear up the mess
          if (OB_SUCCESS != ret)
          {
            clear_up_mess(idx_id, need_delete_rt);
          }
        }//end else
        if(try_time > 0 || OB_SUCCESS == ret)break;
      }//end while
      monitor_phase_ = MONITOR_INIT;
      return ret;
    }

    void ObIndexMonitor::clear_up_mess(const uint64_t idx_id, const bool need_delete_rt)
    {
      int err = OB_SUCCESS;
      common::ObArray<uint64_t> delete_table;
      //step1. modify index stat to ERROR, if modify failed, does not matter, because index is not avaliable now.
      //mod liumz, [paxos static index]20170626:b
      bool is_cluster_alive[OB_CLUSTER_ARRAY_LEN];
      worker_->get_root_server().get_alive_cluster_with_cs(is_cluster_alive);
      for (int cluster_idx = 0; cluster_idx < OB_MAX_COPY_COUNT && OB_SUCCESS == err; cluster_idx++)
      {
        if (is_cluster_alive[cluster_idx+1])
        {
          if (OB_SUCCESS != (err = worker_->get_root_server().modify_index_process_info(cluster_idx+1, idx_id, ERROR)))
          {
            TBSYS_LOG(WARN, "fail modify index table's stat to [ERROR], index_tid=%ld, cluster_id[%d]", idx_id, cluster_idx+1);
          }
          else
          {
            TBSYS_LOG(INFO, "index table[%ld] stat changed to [ERROR], cluster_id[%d]", idx_id, cluster_idx+1);
          }
        }
      }
      //mod:e


      if (need_delete_rt)
      {
        //step2. delete index table from rt.
        if (OB_SUCCESS != (err = delete_table.push_back(idx_id)))
        {
          TBSYS_LOG(WARN, "add idx_id to delete table failed. index_tid=%ld", idx_id);
        }
        else if (OB_SUCCESS != (err = worker_->get_root_server().delete_tables(false, delete_table)))
        {
          TBSYS_LOG(ERROR, "fail to delete index table from rt, index_tid=%ld, err=%d", idx_id, err);
        }
        else
        {
          TBSYS_LOG(INFO, "delete index table from rt success. index_tid=%ld", idx_id);
        }
      }
    }

    //add wenghaixing [secondary index static_index_build.report_info]20150324
    int ObIndexMonitor::fetch_tablet_info(const uint64_t table_id, const ObRowkey &row_key, ObScanner &scanner)
    {
      int ret = OB_SUCCESS;
      ObCellInfo cell;
      // cell info not root table id
      //UNUSED(root_table_id);
      cell.table_id_ = table_id;
      cell.column_id_ = 0;
      cell.row_key_ = row_key;
      ObGetParam get_param;
      get_param.set_is_result_cached(false);
      get_param.set_is_read_consistency(false);
      ret = get_param.add_cell(cell);
      if(NULL == worker_)
      {
        TBSYS_LOG(WARN, "null pointer of worker_");
        ret = OB_ERROR;
      }
      if(OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "get param add cell failed,ret[%d]",ret);
      }
      else if(OB_SUCCESS != (ret = worker_->get_root_server().find_root_table_key(get_param,scanner)))
      {
        TBSYS_LOG(WARN, "find root table info failed[%d]", ret);
      }
      return ret;
    }

    int ObIndexMonitor::construct_tablet_item(const uint64_t table_id, const ObRowkey &start_key, const ObRowkey &end_rowkey, ObNewRange &range, ObTabletLocationList &list)
    {
      int ret = OB_SUCCESS;
      range.table_id_ = table_id;
      range.border_flag_.unset_inclusive_start();
      range.border_flag_.set_inclusive_end();
      range.start_key_ = start_key;
      range.end_key_ = end_rowkey;
      list.set_timestamp(tbsys::CTimeUtil::getTime());
      list.set_tablet_range(range);
      // list.sort(addr);
      // double check add all range->locationlist to cache
      if (range.start_key_ >= range.end_key_)
      {
        TBSYS_LOG(WARN, "check range invalid:start[%s], end[%s]",
                  to_cstring(range.start_key_), to_cstring(range.end_key_));
      }
      else
      {
        TBSYS_LOG(DEBUG, "got a tablet:%s, with location list:%ld", to_cstring(range), list.size());
      }
      return ret;
    }

    int ObIndexMonitor::calc_tablet_num_from_scanner(ObScanner &scanner, ObRowkey &row_key, uint64_t table_id, int64_t &tablet_num)
    {
      int ret = OB_SUCCESS;
      ObRowkey start_key;
      start_key = ObRowkey::MIN_ROWKEY;
      ObRowkey end_key;
      ObServer server;
      ObCellInfo * cell = NULL;
      bool row_change = false;
      ObTabletLocationList list;
      CharArena allocator;
      ObScannerIterator iter = scanner.begin();
      ObNewRange range;
      ++iter;
      while ((iter != scanner.end())
             && (OB_SUCCESS == (ret = iter.get_cell(&cell, &row_change))) && !row_change)
      {
        if (NULL == cell)
        {
          ret = OB_INNER_STAT_ERROR;
          break;
        }
        cell->row_key_.deep_copy(start_key, allocator);
        ++iter;
      }

      if (ret == OB_SUCCESS)
      {
        int64_t ip = 0;
        int64_t port = 0;
        // next cell
        for(++iter; iter != scanner.end(); ++iter)
        {
          ret = iter.get_cell(&cell,&row_change);
          if(ret != OB_SUCCESS)
          {
            TBSYS_LOG(ERROR, "get cell from scanner iterator failed:ret[%d]", ret);
            break;
          }
          else if (row_change) // && (iter != last_iter))
          {
            construct_tablet_item(table_id,start_key, end_key, range, list);

            tablet_num ++;

            list.clear();
            start_key = end_key;
          }
          else
          {
            cell->row_key_.deep_copy(end_key, allocator);
            if ((cell->column_name_.compare("1_port") == 0)
                //|| (cell->column_name_.compare("2_port") == 0)
                //|| (cell->column_name_.compare("3_port") == 0)
                )
            {
              ret = cell->value_.get_int(port);
            }
            else if ((cell->column_name_.compare("1_ipv4") == 0)
                     //|| (cell->column_name_.compare("2_ipv4") == 0)
                     //|| (cell->column_name_.compare("3_ipv4") == 0)
                     )
            {
              ret = cell->value_.get_int(ip);
              if (OB_SUCCESS == ret)
              {
                if (port == 0)
                {
                  TBSYS_LOG(WARN, "check port failed:ip[%ld], port[%ld]", ip, port);
                }
                server.set_ipv4_addr(static_cast<int32_t>(ip), static_cast<int32_t>(port));
                ObTabletLocation addr(0, server);
                if (OB_SUCCESS != (ret = list.add(addr)))
                {
                  TBSYS_LOG(ERROR, "add addr failed:ip[%ld], port[%ld], ret[%d]",
                            ip, port, ret);
                  break;
                }
                else
                {
                  TBSYS_LOG(DEBUG, "add addr succ:ip[%ld], port[%ld], server:%s", ip, port, to_cstring(server));
                }
                ip = port = 0;
              }
            }
            if (ret != OB_SUCCESS)
            {
              TBSYS_LOG(ERROR, "check get value failed:ret[%d]", ret);
              break;
            }
          }
        }
        // for the last row
        TBSYS_LOG(DEBUG, "get a new tablet start_key[%s], end_key[%s]",
                  to_cstring(start_key), to_cstring(end_key));
        if ((OB_SUCCESS == ret) && (start_key != end_key))
        {
          construct_tablet_item(table_id, start_key, end_key, range, list);
          tablet_num ++;
        }
      }
      else
      {
        TBSYS_LOG(ERROR, "check get first row cell failed:ret[%d]", ret);
      }
      if(OB_SUCCESS == ret)
      {
        row_key = end_key;
      }
      //TBSYS_LOG(INFO,"test::whx range_count(%ld)",range_hash_.size());

      return ret;
    }

    int ObIndexMonitor::calc_hist_width(uint64_t index_id, int64_t &width)
    {
      int ret = OB_SUCCESS;
      width = 0;
      //mod liumz, bugfix: [alloc memory for ObSchemaManagerV2 in heap, not on stack]20150702:b
      //ObSchemaManagerV2 schema_mgr;
      ObSchemaManagerV2* schema_mgr = OB_NEW(ObSchemaManagerV2, ObModIds::OB_RS_SCHEMA_MANAGER);
      //mod:e
      const ObTableSchema* schema = NULL;
      //add liumz, bugfix: [alloc memory for ObSchemaManagerV2 in heap, not on stack]20150702:b
      if (NULL == schema_mgr)
      {
        TBSYS_LOG(WARN, "fail to new schema_manager.");
        ret = OB_ALLOCATE_MEMORY_FAILED;
      }//add:e
      else if(NULL == worker_)
      {
        TBSYS_LOG(ERROR, "null pointer of root worker");
        ret = OB_ERROR;
      }
      else if(OB_SUCCESS != (ret = worker_->get_root_server().get_schema(false,false,*schema_mgr)))
      {
        TBSYS_LOG(ERROR, "get schema manager for monitor failed,ret[%d]", ret);
      }
      else if(NULL == ((schema = schema_mgr->get_table_schema(index_id))))
      {
        TBSYS_LOG(ERROR,  "get index schema failed!");
        ret = OB_ERROR;
      }

      if(OB_SUCCESS == ret)
      {
        uint64_t data_tid = schema->get_index_helper().tbl_tid;
        ObScanner scanner;
        ObRowkey start_key;
        start_key.set_min_row();
        {
          do
          {
            ret = fetch_tablet_info(data_tid, start_key, scanner);
            if(OB_SUCCESS != ret)
            {
              TBSYS_LOG(WARN, "failed to fetch tablet info[%d]", ret);
            }
            else if(OB_SUCCESS != (ret = calc_tablet_num_from_scanner(scanner, start_key, data_tid, width)))
            {
              TBSYS_LOG(WARN, "failed to cal tablet_num,ret [%d]", ret);
            }
            else if (ObRowkey::MAX_ROWKEY == start_key)
            {
              TBSYS_LOG(INFO, "get all tablets info from rootserver success");
              break;
            }
            else
            {
              TBSYS_LOG(DEBUG, "need more request to get next tablet info");
              scanner.reset();
            }
          //}while(true);
          }while(OB_SUCCESS == ret);
        }

      }      
      width = width < ObTabletHistogram::MAX_SAMPLE_BUCKET ? width : ObTabletHistogram::MAX_SAMPLE_BUCKET;
      //add liumz, bugfix: [alloc memory for ObSchemaManagerV2 in heap, not on stack]20150702:b
      if (schema_mgr != NULL)
      {
        OB_DELETE(ObSchemaManagerV2, ObModIds::OB_RS_SCHEMA_MANAGER, schema_mgr);
      }
      //add:e
      return ret;
    }
    //add e

    bool compare_tid(const uint64_t* lhs, const uint64_t* rhs)
    {
      return *lhs < *rhs;
    }

    bool unique_tid(const uint64_t* lhs, const uint64_t* rhs)
    {
      return *lhs == *rhs;
    }

    int ObIndexMonitor::gen_index_list()
    {
      int ret = OB_SUCCESS;
      ObArray<uint64_t> index_list;      
      if(NULL == worker_)
      {
        TBSYS_LOG(ERROR, "the worker pointer is null");
        ret = OB_ERROR;
      }
      else if(OB_SUCCESS != (ret = worker_->get_root_server().fetch_init_index(start_version_, &index_list)))
      {
        TBSYS_LOG(ERROR, "get schema manager for monitor failed,version[%ld],ret[%d]",start_version_, ret);
      }
      else
      {
        uint64_t idx_tid = OB_INVALID_ID;
        //mod liumz, [paxos static index]20170626:b
        //int64_t status = -1;
        //int64_t cluster_id = worker_->get_root_server().get_config().cluster_id;
        IndexStatus status = INDEX_INIT;
        int cluster_num = worker_->get_root_server().get_alive_cluster_num_with_cs();
        //mod:e
        for(int64_t i = 0; i < index_list.count(); i++)
        {
          if(OB_SUCCESS != (ret = index_list.at(i, idx_tid)))
          {
            TBSYS_LOG(WARN, "get from ob array error, ret[%d]", ret);
            break;
          }
          //mod liumz, [paxos static index]20170626:b
          //else if(OB_SUCCESS != (ret = worker_->get_root_server().fetch_index_stat((int64_t)idx_tid, cluster_id, status)))
          else if(OB_SUCCESS != (ret = worker_->get_root_server().get_index_stat(idx_tid, cluster_num, status)))
          {
            //TBSYS_LOG(WARN, "fetch status idx_tid[%ld],cluster_id[%ld] failed", idx_tid, cluster_id);
            TBSYS_LOG(WARN, "fetch status idx_tid[%ld],cluster_num[%d] failed", idx_tid, cluster_num);
            break;
          }
          //else if(-1 == status)
          else if(INDEX_INIT == status)
          //mod:e
          {
            if(OB_SUCCESS != (ret = index_list_.push_back(idx_tid)))
            {
              TBSYS_LOG(WARN, "index list push back failed,tid [%ld]", idx_tid);
              break;
            }
          }
        }
        TBSYS_LOG(INFO, "fetch init index list ok,size[%ld], version[%ld]", index_list_.count(), start_version_);
      }
      /*else if(OB_SUCCESS != (ret = schema_mgr.get_init_index(table_id, size)))
      {
        TBSYS_LOG(ERROR, "get init index list for monitor to build index failed, ret[%d]", ret);
      }
      else
      {
        for(int64_t i = 0; i < size; i++)
        {
          index_list_.push_back(table_id[i]);

        }
        //process_index_num_ = index_list_.count();
      }
      */
      return ret;
    }

    int ObIndexMonitor::schedule()
    {
      int ret = OB_SUCCESS;
      //mod liumz, [bugfix_ups_offline]20150722:b
      const int max_retry_times = 10;
      int retry_times = 0;
      while (retry_times < max_retry_times)
      {
        retry_times++;
        if(OB_SUCCESS != (ret = gen_index_list()))
        {
          index_list_.clear();
          TBSYS_LOG(WARN,"generate index list for index_monitor! sleep and retry!");
          usleep(3000000);
        }
        else
        {
          break;
        }
      }
      ObSortedVector<uint64_t*>::iterator insert_pos = NULL;
      for(int64_t i = 0; i < index_list_.count(); i++)
      {
        uint64_t &idx_tid = index_list_.at(i);
        if(OB_SUCCESS != (ret = sorted_index_list_.insert_unique(&idx_tid, insert_pos, compare_tid, unique_tid)))
        {
          TBSYS_LOG(WARN, "sorted index list push back failed,tid [%ld], ret[%d]", idx_tid, ret);
          break;
        }
      }
      //mod:e
      if(sorted_index_list_.size() == 0)
      {
        TBSYS_LOG(INFO, "there is no index to build,stop monitor!");
        //add liumz, 20150323:b
        stop();
        //add:e
      }
      else
      {
        monitor();
        if(success_index_num_ == sorted_index_list_.size())
        {
          TBSYS_LOG(INFO,"mission complete!All index's static data has been build");
          stop();
        }
        else
        {
          TBSYS_LOG(WARN,"mission complete. There are [%ld] index has not been build",failed_index_.count());
          for(int64_t i = 0; i < failed_index_.count();i++)
          {
            TBSYS_LOG(WARN,"The index[%ld] has not been build ok", failed_index_.at(i));
          }
          stop();
        }
      }
      return ret;
    }
  }//end
}//end oceanbase
