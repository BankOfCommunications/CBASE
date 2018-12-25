/**
  * add wenghaixing [secondary index static_index_build.cs_scan]20150324
  *
  * 1.CS之间交互的接口，目前暂时只用于读取tablet的局部索引sstable的数据
  * 2.这是一个同步的流式接口，结果放在ObScanner里面
  * 3.ObCSQueryAgent不是线程安全的，所以每个线程应该有自己的agent处理数据
  */
#ifndef OB_CS_QUERY_AGENT_H
#define OB_CS_QUERY_AGENT_H

#include "common/ob_schema.h"
#include "ob_cell_stream.h"
#include "ob_scan_cell_stream.h"
#include "ob_rpc_proxy.h"
#include "common/ob_scan_param.h"
#include "ob_cs_scan_cell_stream.h"

namespace oceanbase
{
  namespace chunkserver
  {
    typedef hash::ObHashMap<ObNewRange,ObTabletLocationList,hash::NoPthreadDefendMode> RangeServerHash;
    typedef hash::ObHashMap<ObNewRange,ObTabletLocationList,hash::NoPthreadDefendMode>::const_iterator HashIterator;
    class ObCSQueryAgent: public common::ObIterator
    {
      public:
        explicit ObCSQueryAgent();
        ~ObCSQueryAgent();

      public:
        virtual int get_cell(common::ObCellInfo** cell);
        virtual int get_cell(common::ObCellInfo** cell, bool* is_row_changed);
        virtual int next_cell();

      public:
        int start_agent(common::ObScanParam& scan_param,  ObCSScanCellStream& cs_stream, const RangeServerHash* hash);
        int get_next_row(ObRow* row);
        void stop_agent();
        int get_failed_fake_range(ObNewRange &range);
        inline void get_scan_param(common::ObScanParam *&param){param = scan_param_;}
        inline void set_not_used(bool not_used = true){not_used_ = not_used;}
        inline bool has_no_use(){return not_used_;}
        const RangeServerHash* get_range_server_hash(){return range_server_hash_;}
        void reset();
        //add wenghaixing [secondary index build_static_index.bug_fix]20150724
        /*add this function so that index builder can return failed fake range*/
        void set_failed_fake_range(const ObNewRange &range);
        //add e
      private:
        ObCSScanCellStream                            *pfinal_result_;
        common::ObScanParam                           *scan_param_;
        bool                                          inited_;
        bool                                          not_used_;
        const RangeServerHash                         *range_server_hash_;
        int32_t                                       hash_index_;          //用来迭代hash的下标
        int64_t                                       column_count_;
        //add wenghaixing [secondary index build_static_index.bug_fix]20150724
        ObNewRange                                    failed_fake_range_;
        //add e
    };

  }//end chunkserver

}//end oceanbase


#endif // OB_CS_QUERY_AGENT_H
