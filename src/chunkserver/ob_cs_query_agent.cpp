#include "ob_cs_query_agent.h"

namespace oceanbase
{
  namespace chunkserver
  {
    ObCSQueryAgent::ObCSQueryAgent()
                   :pfinal_result_(NULL), scan_param_(NULL), inited_(false),not_used_(false), range_server_hash_(NULL), hash_index_(0),
                    column_count_(0)
    {}
    ObCSQueryAgent::~ObCSQueryAgent()
    {}

    void ObCSQueryAgent::reset()
    {
      hash_index_ = 0;
      pfinal_result_ = NULL;
      scan_param_ = NULL;
      column_count_ = 0;
      inited_ = false;
      hash_index_ = 0;
      range_server_hash_ = NULL;
    }

    //add wenghaixing [secondary index build_static_index.bug_fix]2015_07_24
    void ObCSQueryAgent::set_failed_fake_range(const ObNewRange &range)
    {
      failed_fake_range_.reset();
      failed_fake_range_ = range;
    }
    //add e

    int ObCSQueryAgent::start_agent(ObScanParam &scan_param, ObCSScanCellStream &cs_stream, const RangeServerHash *hash)
    {
      int ret = OB_SUCCESS;
      reset();
      //这么写的话当然不会有空指针的问题了
      pfinal_result_ = &cs_stream;
      scan_param_ = &scan_param;
      range_server_hash_ = hash;
      //ObServer chunkserver;
      if(NULL == hash)
      {
        TBSYS_LOG(WARN, "null pointer of range server hash");
        ret = OB_INNER_STAT_ERROR;
      }
      //在start_agent里面进行第一次scan，获得第一个batch的数据,scan的时候应该传入ChunkServer的地址参数
      if(OB_SUCCESS == ret)
      {
        inited_ = true;
        HashIterator iter = range_server_hash_->begin();
        bool iter_end = true;
        ObTabletLocationList list;
        int loop = 0;
        for(; iter != range_server_hash_->end(); ++ iter)
        {
          list = iter->second;
          if(list[0].server_.chunkserver_ == pfinal_result_->get_self())
          {
            loop++;
            continue;
          }
          else
          {
            /* comment by liumz
             * 如果数据表的第一副本不在本机，设置第一个fake_range和对应的ObTabletLocationList，并标记hash_index
             */
            scan_param_->set_fake(true);
            scan_param_->set_fake_range(iter->first);
            pfinal_result_->set_chunkserver((iter->second));
            //TBSYS_LOG(DEBUG, "test::lmz, out");
            //(iter->second).print_info();//add liumz, debug
            column_count_ = scan_param.get_column_id_size();
            hash_index_ = loop;
            iter_end = false;
            break;
          }
        }
        /* comment by liumz
         * iter_end == true 表示数据表所有tablet的第一副本都在本机
         */
        if(iter_end)
        {
          ret = OB_ITER_END;
        }
        if(OB_SUCCESS == ret)
        {
          TBSYS_LOG(INFO, "fake range:[%s]", to_cstring(*(scan_param_->get_fake_range())));
          if(OB_SUCCESS != (ret = pfinal_result_->scan(*scan_param_)) && OB_ITER_END != ret)
          {
            //add wenghaixing [secondary index bug_fix]20150724
            set_failed_fake_range(*(scan_param_->get_fake_range()));
            //add e
            TBSYS_LOG(WARN, "failed to scan firstly batch data,[%d]", ret);
          }
          else if(OB_SUCCESS == ret || OB_ITER_END ==ret)
          {
            //TBSYS_LOG(ERROR,"test::whx ret start_agent first scan,hash_index_ [%d],fake_range(%s),range(%s) ret= [%d]", hash_index_,to_cstring(*(scan_param_->get_fake_range())),to_cstring(*(scan_param_->get_range())), ret);
            ret = OB_SUCCESS;
          }
        }
      }
      return ret;
    }

    void ObCSQueryAgent::stop_agent()
    {
      inited_ = false;
      pfinal_result_ = NULL;
      scan_param_ = NULL;
    }

    int ObCSQueryAgent::get_failed_fake_range(ObNewRange &range)
    {
      int ret = OB_SUCCESS;
      /*if(NULL == scan_param_)
      {
        TBSYS_LOG(WARN, "scan param is invalid");
        ret = OB_INVALID_ARGUMENT;
      }
      else if(!scan_param_->if_need_fake() || NULL == scan_param_->get_fake_range())
      {
        TBSYS_LOG(WARN, "scan_param is invalid");
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        range = *(scan_param_->get_fake_range());
      }*/
      if(OB_INVALID_ID == failed_fake_range_.table_id_)
      {
        TBSYS_LOG(WARN, "failed fake range is invalid");
        ret = OB_INNER_STAT_ERROR;
      }
      else
      {
        range = failed_fake_range_;
      }
      return ret;
    }

    int ObCSQueryAgent::get_next_row(ObRow *row)
    {
      int ret = OB_SUCCESS;
      //pfinal_result_->get_cell()
      ObCellInfo* cell = NULL;
      //bool is_row_changed = false;
      int64_t column_count = 0;
      if(!not_used_)
      {
        do
        {
          if(OB_SUCCESS == (ret = next_cell()))
          {
            ret = get_cell(&cell);
          }
          else if(OB_ITER_END == ret)
          {
            break;
          }
          else
          {
            TBSYS_LOG(WARN, "get cell failed,ret[%d]", ret);
          }

          if(OB_SUCCESS == ret)
          {
            if(OB_SUCCESS == (ret = row->set_cell((cell)->table_id_, (cell)->column_id_, (cell)->value_)))
            {
             // TBSYS_LOG(INFO,"test::whx::row set cell ok, tid[%ld], cid[%ld],value[%s]", (cell)->table_id_, (cell)->column_id_,to_cstring((cell)->value_));
            }
            else
            {
              TBSYS_LOG(WARN,"row set cell failed, tid[%ld], cid[%ld]", (cell)->table_id_, (cell)->column_id_);
              break;
            }
          }
          else
          {
            break;
          }

        }while(++column_count < column_count_);
        if(OB_SUCCESS != ret && OB_ITER_END != ret)
        {
          TBSYS_LOG(WARN, "get_next_row failed[%d]", ret);
        }
      }
      else
      {
        ret = OB_ITER_END;
      }
      return ret;
    }

    int ObCSQueryAgent::get_cell(ObCellInfo **cell)
    {
      int ret = OB_SUCCESS;
      if(NULL == pfinal_result_)
      {
        TBSYS_LOG(WARN, "null pointer of ObCSScanCellStream");
        ret = OB_ERROR;
      }
      else
      {
        ret = pfinal_result_->get_cell(cell);
      }
      return ret;
    }

    int ObCSQueryAgent::get_cell(ObCellInfo **cell, bool *is_row_changed)
    {
      int ret = OB_SUCCESS;
      if(NULL == pfinal_result_)
      {
        TBSYS_LOG(WARN, "null pointer of ObCSScanCellStream");
        ret = OB_ERROR;
      }
      else
      {
        ret = pfinal_result_->get_cell(cell, is_row_changed);
      }
      return ret;
    }

    int ObCSQueryAgent::next_cell()
    {
      int ret = OB_SUCCESS;
      if(!inited_ || NULL == pfinal_result_ || NULL == range_server_hash_)
      {
        TBSYS_LOG(WARN, "run start_agent() first, inited_=%d, pfinal_result_=%p",
                  inited_, pfinal_result_);
        ret = OB_INVALID_ARGUMENT;
      }
      else if(OB_SUCCESS == (ret = pfinal_result_->next_cell()))
      {

      }
      else if(OB_ITER_END == ret)
      {
        do
        {
          if(hash_index_ < range_server_hash_->size())
          {
            HashIterator iter = range_server_hash_->begin();
            int32_t loop = 0;
            ObTabletLocationList list;
            bool iter_end = true;
            //TBSYS_LOG(ERROR,"test::whx OB_ITER_END hash_index_ [%d],size[%ld]", hash_index_,range_server_hash_->size());
            for(;iter != range_server_hash_->end();iter++)
            {

              list = iter->second;
              if(loop <= hash_index_ || list[0].server_.chunkserver_ == pfinal_result_->get_self())
              {
                loop ++;
                //TBSYS_LOG(ERROR,"test::whx show loop hash_index_[%d]",hash_index_);
                continue;
              }
              else
              {
                scan_param_->set_fake(true);
                scan_param_->set_fake_range(iter->first);
                pfinal_result_->set_chunkserver(iter->second);
                hash_index_ = loop;
                iter_end = false;
                break;
              }
            }
            pfinal_result_->reset_inner_stat();
            //TBSYS_LOG(ERROR,"test::whx ret start_agent next scan,hash_index_ [%d],fake_range(%s),range(%s) ret= [%d]", hash_index_,to_cstring(*(scan_param_->get_fake_range())),to_cstring(*(scan_param_->get_range())), ret);
            if(!iter_end)
            {
              ret = pfinal_result_->scan(*scan_param_);
              if(OB_SUCCESS == ret)
              {
                ret = pfinal_result_->next_cell();
                break;
              }
              else if(OB_ITER_END != ret)
              {
                //add wenghaixing [secondary index bug_fix]20150724
                set_failed_fake_range(*(scan_param_->get_fake_range()));
                //add e
                TBSYS_LOG(WARN, "get next cell failed![%d]",ret);
              }
            }
            else
            {
              break;
            }
          }
          else
          {
            pfinal_result_->reset_inner_stat();
          }
        }while(true);
      }
      return ret;

    }

  }//end chunkserver


}//end oceanbase
