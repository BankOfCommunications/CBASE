#ifndef OB_CS_SCAN_CELL_STREAM_CPP
#define OB_CS_SCAN_CELL_STREAM_CPP

#include "common/utility.h"
#include "ob_read_param_modifier.h"
#include "ob_cs_scan_cell_stream.h"

namespace oceanbase
{
  namespace chunkserver
  {
    using namespace oceanbase::common;

    ObCSScanCellStream::ObCSScanCellStream(ObMergerRpcProxy * rpc_proxy,
      const ObServerType server_type, const int64_t time_out)
    : ObCellStream(rpc_proxy, server_type, time_out)
    {
      finish_ = false;
      cur_rep_index_ = 0;
      reset_inner_stat();
    }

    ObCSScanCellStream::~ObCSScanCellStream()
    {
    }

    int ObCSScanCellStream::next_cell(void)
    {
      int ret = OB_SUCCESS;
      if (NULL == scan_param_)
      {
        TBSYS_LOG(DEBUG, "check scan param is null");
        ret = OB_ITER_END;
      }
      else if (!check_inner_stat())
      {
        TBSYS_LOG(ERROR, "check inner stat failed");
        ret = OB_INNER_STAT_ERROR;
      }
      else
      {
        // do while until get scan data or finished or error occured
        /*if(!cur_result_.is_empty())
        {
          ret = cur_result_.next_cell();
        }*/
        do
        {
          ret = get_next_cell();
          if (OB_ITER_END == ret)
          {
            TBSYS_LOG(DEBUG, "scan cell finish");
            break;
          }
          else if (ret != OB_SUCCESS)
          {
            TBSYS_LOG(WARN, "scan next cell failed:ret[%d]", ret);
            break;
          }
        } while (cur_result_.is_empty() && (OB_SUCCESS == ret));
      }
      return ret;
    }

    int ObCSScanCellStream::scan(const ObScanParam & param)
    {
      int ret = OB_SUCCESS;
      const ObNewRange * range = param.get_range();
      if (NULL == range)
      {
        TBSYS_LOG(WARN, "check scan param failed");
        ret = OB_INPUT_PARAM_ERROR;
      }
      else
      {
        reset_inner_stat();
        scan_param_ = &param;
        TBSYS_LOG(DEBUG, "test::lmz, in");
        //chunkserver_.print_info();//add liumz, debug
        ret = scan_row_data();
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(WARN, "scan server data failed:ret[%d]", ret);
        }
        else
        {
          /**
           * cs return empty scanner, just return OB_ITER_END
           */
          if (cur_result_.is_empty() && !ObCellStream::first_rpc_)
          {
            // check already finish scan
            ret = check_finish_scan(cur_scan_param_);
            if (OB_SUCCESS != ret)
            {
              TBSYS_LOG(WARN, "check finish scan failed:ret[%d]", ret);
            }
            else if (finish_)
            {
              ret = OB_ITER_END;
              //TBSYS_LOG(ERROR,"test::whx here ret = [%d]",ret);
            }
          }
        }
      }
      return ret;
    }

    int ObCSScanCellStream::get_next_cell(void)
    {
      int ret = OB_SUCCESS;
      if (finish_)
      {
        ret = OB_ITER_END;
        TBSYS_LOG(DEBUG, "check next already finish");
      }
      else
      {
        last_cell_ = cur_cell_;
        ret = cur_result_.next_cell();
        // need rpc call for new data
        if (OB_ITER_END == ret)
        {
          // scan the new data only by one rpc call
          ret = scan_row_data();
          if (OB_SUCCESS == ret)
          {
            ret = cur_result_.next_cell();
            if ((ret != OB_SUCCESS) && (ret != OB_ITER_END))
            {
              TBSYS_LOG(WARN, "get next cell failed:ret[%d]", ret);
            }
            else if (OB_ITER_END == ret)
            {
              TBSYS_LOG(DEBUG, "finish the scan");
            }
          }
          else if (OB_ITER_END != ret)
          {
            TBSYS_LOG(WARN, "scan server data failed:ret[%d]", ret);
          }
        }
        else if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(WARN, "next cell failed:ret[%d]", ret);
        }
      }
      return ret;
    }

    int ObCSScanCellStream::scan_row_data()
    {
      int ret = OB_SUCCESS;
      // step 1. modify the scan param for next scan rpc call
      if (!ObCellStream::first_rpc_)
      {
        // check already finish scan
        ret = check_finish_scan(cur_scan_param_);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "check finish scan failed:ret[%d]", ret);
        }
        else if (finish_)
        {
          ret = OB_ITER_END;
        }
      }

      // step 2. construct the next scan param
      if (OB_SUCCESS == ret)
      {
        ret = get_next_param(*scan_param_, cur_result_, &cur_scan_param_, range_buffer_);
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(WARN, "modify scan param failed:ret[%d]", ret);
        }
      }

      // step 3. scan data according the new scan param
      //const int64_t MAX_REP_COUNT = 3;
      //const int64_t MAX_REP_COUNT = OB_MAX_COPY_COUNT;
      if(OB_SUCCESS == ret)
      {
        if(ObCellStream::first_rpc_)
        {
          TBSYS_LOG(DEBUG, "test::lmz, first_rpc_");
          //chunkserver_.print_info();//add liumz, debug
          do
          {
            TBSYS_LOG(DEBUG,"test:lmz cur_rep_index_ = %ld,cs_cs_scan = %s,server = %s", cur_rep_index_, to_cstring(*(cur_scan_param_.get_fake_range())),to_cstring((chunkserver_)[cur_rep_index_].server_.chunkserver_));
            ret = ObCellStream::rpc_scan_row_data(cur_scan_param_, (chunkserver_)[cur_rep_index_].server_.chunkserver_);
            if(OB_SUCCESS != ret)
            {
              cur_rep_index_ ++;

            }
          //} while(OB_TABLET_HAS_NO_LOCAL_SSTABLE == ret && cur_rep_index_ < MAX_REP_COUNT);
          } while(OB_TABLET_HAS_NO_LOCAL_SSTABLE == ret && cur_rep_index_ < chunkserver_.size());
          cur_rep_index_ = 0;//add liumz, [reset cur_rep_index_]20160617
        }
        else
        {
          ret = ObCellStream::rpc_scan_row_data(cur_scan_param_, (chunkserver_)[cur_rep_index_].server_.chunkserver_);
          if (ret != OB_SUCCESS)
          {
            TBSYS_LOG(WARN, "scan row data failed:ret[%d]", ret);
          }
        }
      }
      //if(cur_scan_param_.if_need_fake())
      //TBSYS_LOG(INFO, "test::whx cur_scan_param_.real =%s", to_cstring(*cur_scan_param_.get_range()));

      return ret;
    }

    int ObCSScanCellStream::check_finish_scan(const ObScanParam & param)
    {
      int ret = OB_SUCCESS;
      bool is_fullfill = false;
      if (!finish_)
      {
        int64_t item_count = 0;
        ret = ObCellStream::cur_result_.get_is_req_fullfilled(is_fullfill, item_count);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "get scanner full filled status failed:ret[%d]", ret);
        }
        else if (is_fullfill)
        {
          ObNewRange result_range;
          ret = ObCellStream::cur_result_.get_range(result_range);
          if (OB_SUCCESS != ret)
          {
            TBSYS_LOG(WARN, "get result range failed:ret[%d]", ret);
          }
          else
          {
            finish_ = is_finish_scan(param, result_range);
          }
        }
      }
      return ret;
    }

    int64_t ObCSScanCellStream::get_data_version() const
    {
      return cur_result_.get_data_version();
    }
  } // end namespace chunkserver
} // end namespace oceanbase


#endif // OB_CS_SCAN_CELL_STREAM_CPP
