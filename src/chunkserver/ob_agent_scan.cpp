#include "ob_agent_scan.h"
#include "ob_cs_query_agent.h"

//using namespace oceanbase::chunkserver;
namespace oceanbase{
  namespace sql
  {
    //REGISTER_PHY_OPERATOR(chunkserver::ObAgentScan, PHY_AGENT_SCAN);
  }
}
namespace oceanbase
{
  namespace chunkserver
  {
    ObAgentScan::ObAgentScan()
    {
      query_agent_ = NULL;
      local_idx_scan_finishi_ = false;
      local_idx_block_end_ = false;
      first_scan_ = false;
      hash_index_ = 0;
    }

    ObAgentScan::~ObAgentScan()
    {
      query_agent_ = NULL;
    }

    int ObAgentScan::open()
    {
      int ret = OB_SUCCESS;
      if(NULL == query_agent_)
      {
        ret = OB_ERR_UNEXPECTED;
        TBSYS_LOG(WARN, "query_agent cannot be a null pointer!");
      }
      else if(OB_SUCCESS == (ret = build_sst_scan_param()))
      {
        ret = scan();
      }
      cur_row_.set_row_desc(row_desc_);
      if(OB_ITER_END == ret)
      {
        local_idx_scan_finishi_ = true;
        ret = OB_SUCCESS;
      }
      return ret;
    }

    int ObAgentScan::close()
    {
      int ret =OB_SUCCESS;

      return ret;
    }

    void ObAgentScan::reset()
    {
      row_desc_.reset();

      query_agent_ = NULL;
    }

    void ObAgentScan::reuse()
    {

    }

    int64_t ObAgentScan::to_string(char* buf, const int64_t buf_len) const
    {
      int64_t pos = 0;
      UNUSED(buf);
      UNUSED(buf_len);
      return pos;
    }

    int ObAgentScan::scan()
    {
      int ret = OB_ERROR;
      ObNewRange fake_range;
	  //mod whx, [bugfix: free memory]20161030:b
      sst_scan_.reset_scanner();
      //TBSYS_LOG(ERROR,"test::whx in block 0 ");
	  //mod:e
      sst_scan_.reset();
      ret = get_next_local_range(fake_range);
      if(OB_SUCCESS == ret)
      {
        ret = sst_scan_.open_scan_context_local_idx(sst_scan_param_, sc_, fake_range);
        //TBSYS_LOG(ERROR,"test::whx ObAgentScan fake_range[%s]",to_cstring(fake_range));
      }
      if(OB_SUCCESS == ret)
      {
        ret = sst_scan_.init_sstable_scanner_for_local_idx(fake_range);
        if(OB_SUCCESS != ret)
        {
          if(!query_agent_)
          {
            TBSYS_LOG(ERROR, "inner stat error, query agent cannot be null");
          }
          else
          {
            query_agent_->set_failed_fake_range(fake_range);
          }
        }
      }

      if(OB_ITER_END == ret)
      {
        local_idx_block_end_ = true;
        //TBSYS_LOG(ERROR,"test::whx ObAgentScan local_idx_block_end_");
        //ret = OB_SUCCESS;
      }
      return ret;
    }

    int ObAgentScan::get_next_local_row(const ObRow *&row)
    {
      int ret = OB_SUCCESS;
      const ObRowkey *row_key = NULL;
      {
        ret = sst_scan_.get_next_row(row_key,row);
        //TBSYS_LOG(ERROR,"test::whx ObAgentScan row[%s]",to_cstring(*row));
        if(OB_ITER_END == ret)
        {
          do
          {
            ret = scan();
            if(OB_ITER_END == ret)
            {
              break;
            }
            else if(OB_SUCCESS == ret)
            {
              ret = sst_scan_.get_next_row(row_key,row);
            }
            else
            {
              break;
            }
            if(OB_ITER_END == ret)
            {
              continue;
            }
          }while(OB_SUCCESS != ret);
        }
      }
      if(OB_ITER_END == ret && local_idx_block_end_)
      {
        local_idx_scan_finishi_ = true;
		//mod whx, [bugfix: free memory]20161030:b
        //TBSYS_LOG(ERROR,"test::whx in block 1");
          sst_scan_.reset_scanner();
          //TBSYS_LOG(ERROR,"test::whx in block 0 ");
          sst_scan_.reset();
		//mod:e
      }
      if(OB_ITER_END == ret && !query_agent_->has_no_use())
      {
		//mod whx, [bugfix: free memory]20161030:b
        sst_scan_.reset_scanner();
        //TBSYS_LOG(ERROR,"test::whx in block 0 ");
        sst_scan_.reset();
        //TBSYS_LOG(ERROR,"test::whx in block 2");
		//mod:e
      }

      return ret;
    }
    int ObAgentScan::get_next_local_range(ObNewRange &range)
    {
      int ret = OB_SUCCESS;
      bool find = false;
      if(OB_UNLIKELY(NULL == range_server_hash_))
      {
        ret = OB_ERR_UNEXPECTED;
        TBSYS_LOG(WARN,"should not be here");
      }
      if(OB_SUCCESS == ret)
      {
        HashIterator itr = range_server_hash_->begin();
        ObTabletLocationList list;

        int64_t lp = 0;
        for( ; itr != range_server_hash_->end(); ++itr)
        {
          ++lp;
          if(lp <= hash_index_)
          {
            continue;
          }
          else
          {
            list = itr->second;           
            if(list[0].server_.chunkserver_.get_ipv4() == self_.get_ipv4() && list[0].server_.chunkserver_.get_port() == self_.get_port())
            {
              range = (itr->first);
              hash_index_ = lp;
              find = true;
              break;
            }
            if(find)
            {
              //TBSYS_LOG(INFO,"test:whx I find local range,range[%s]",to_cstring(range));
              break;
            }
          }
        }
      }
      if(OB_SUCCESS == ret && !find)
      {
        ret = OB_ITER_END;
      }
      return ret;
    }

    int ObAgentScan::get_next_row(const ObRow *&row)
    {
      int ret = OB_SUCCESS;
      const ObRow* tmp_row = &cur_row_;
      if(OB_UNLIKELY(NULL == query_agent_))
      {
        TBSYS_LOG(WARN,"null pointer of query agent!");
        ret = OB_ERROR;
      }
      else
      {
        if(!local_idx_scan_finishi_)
        {
          if(OB_SUCCESS == (ret = get_next_local_row(tmp_row)))
          {
            row = tmp_row;
          }
        }
        if(local_idx_scan_finishi_)
        {
          if(OB_SUCCESS == (ret = query_agent_->get_next_row(&cur_row_)))
          {
             row = &cur_row_;
          }
        }
      }
      return ret;
    }

    int ObAgentScan::get_row_desc(const ObRowDesc *&row_desc) const
    {
      int ret = OB_SUCCESS;
      row_desc = &row_desc_;
      return ret;
    }

    void ObAgentScan::set_row_desc(const ObRowDesc &desc)
    {
      row_desc_ = desc;
    }

    void ObAgentScan::set_query_agent(chunkserver::ObCSQueryAgent *agent)
    {
      query_agent_ = agent;
      if(NULL != agent)
      {
        range_server_hash_ = agent->get_range_server_hash();

      }
    }

    int ObAgentScan::build_sst_scan_param()
    {
      int ret = OB_SUCCESS;
      ObScanParam *scan_param = NULL;
      ObTabletScan tmp_table_scan;
      ObSqlScanParam ob_sql_scan_param;
      ObArray<uint64_t> basic_columns;
      if(OB_UNLIKELY(NULL == query_agent_))
      {
        TBSYS_LOG(WARN,"null pointer of query agent!");
        ret = OB_ERROR;
      }
      else
      {
        query_agent_->get_scan_param(scan_param);
      }
      if(OB_UNLIKELY(NULL == scan_param))
      {
        TBSYS_LOG(WARN,"null pointer of scan_param!");
        ret = OB_ERROR;
      }

      if(OB_SUCCESS == ret)
      {
        if(OB_SUCCESS != (ret = ob_sql_scan_param.set_range(*(scan_param->get_range()))))
        {
          TBSYS_LOG(WARN,"set range failed!,ret[%d]", ret);
        }
        else
        {
          uint64_t tid = OB_INVALID_ID;
          uint64_t cid = OB_INVALID_ID;
          for(int64_t i = 0; i < row_desc_.get_column_num();i++)
          {
            if(OB_SUCCESS != row_desc_.get_tid_cid(i, tid, cid))
            {
              TBSYS_LOG(WARN, "get tid cid failed! ret [%d]",ret);
              break;
            }
            else if(OB_SUCCESS != (ret = basic_columns.push_back(cid)))
            {
              break;
            }
          }
        }
      }
      if(OB_SUCCESS == ret)
      {
        ret = tmp_table_scan.build_sstable_scan_param_pub(basic_columns, ob_sql_scan_param, sst_scan_param_);
      }
      return ret;
    }

    PHY_OPERATOR_ASSIGN(ObAgentScan)
    {
      int ret = OB_SUCCESS;
      CAST_TO_INHERITANCE(ObAgentScan);
      reset();
      row_desc_ = o_ptr->row_desc_;
      return ret;
    }


  }//end sql
}//end oceanbase
